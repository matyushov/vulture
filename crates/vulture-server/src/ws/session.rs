//! One WebSocket session: connect, subscribe, read, keep alive, watch for silence.
//!
//! A session runs in its own task and never blocks on the consumer: frames go to a bounded channel with
//! `try_send`, and a full channel drops the frame and counts it. The supervisor in [`super::manager`] decides
//! what to do when a session ends.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::Gap;

use super::clock::Clock;
use super::connector::{BoxedConnection, ConnectError, WsConnector};
use super::dedup::Dedup;
use super::handler::{Frame, FrameClass, WsHandler};
use super::profile::{PingMessage, WsProfile};
use super::status::WsCounters;

/// A frame delivered to the consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsMessage {
    /// Session that received the frame (increases with every connection).
    pub session: u64,
    /// Receive time, ms since the Unix epoch.
    pub recv_ts_ms: i64,
    /// The frame.
    pub frame: Frame,
}

/// What the manager delivers, in order.
#[derive(Debug, Clone, PartialEq)]
pub enum WsOutput {
    /// A data frame.
    Data(WsMessage),
    /// The connection was lost and has recovered; data in `[from_ts, to_ts]` may be missing. Published
    /// before the first frame after recovery.
    Gap(Gap),
}

/// Why a connection attempt failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectFailure {
    /// No connection within `connect_timeout`.
    Timeout,
    /// HTTP error during the upgrade.
    Http {
        /// Status code.
        status: u16,
        /// `Retry-After`, if sent.
        retry_after: Option<Duration>,
    },
    /// Other connection error.
    Other(String),
}

/// How a session ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Cancelled,
    Connect(ConnectFailure),
    SubscribeRejected,
    /// The liveness stream was silent for `data_silence` (also the "connected but mute" case).
    Silence,
    PongTimeout,
    TransportIdle,
    /// The server closed the connection or the stream ended.
    Closed,
    WriteTimeout,
    Error(String),
    /// The consumer dropped the receiver.
    SinkClosed,
}

#[derive(Debug)]
pub(crate) enum Report {
    /// The liveness stream delivered its first frame on this session.
    Live {
        id: u64,
    },
    Ended {
        id: u64,
        outcome: Outcome,
    },
}

/// State shared by the supervisor and all sessions of one connection.
pub(crate) struct Shared {
    /// Receive time of the latest data frame on any session; 0 if none yet.
    pub last_event_ms: AtomicI64,
    /// Start of the pending gap; 0 if none. Taken by the first session that receives data.
    pub gap_from_ms: AtomicI64,
    pub dedup: Mutex<Dedup>,
    pub counters: Arc<WsCounters>,
}

impl Shared {
    pub fn new(dedup_capacity: usize, counters: Arc<WsCounters>) -> Self {
        Self {
            last_event_ms: AtomicI64::new(0),
            gap_from_ms: AtomicI64::new(0),
            dedup: Mutex::new(Dedup::new(dedup_capacity)),
            counters,
        }
    }

    pub fn dedup(&self) -> std::sync::MutexGuard<'_, Dedup> {
        self.dedup.lock().unwrap_or_else(|e| e.into_inner())
    }
}

pub(crate) struct SessionParams {
    pub id: u64,
    pub profile: Arc<WsProfile>,
    pub handler: Arc<dyn WsHandler>,
    pub connector: Arc<dyn WsConnector>,
    pub clock: Arc<dyn Clock>,
    pub out: mpsc::Sender<WsOutput>,
    pub shared: Arc<Shared>,
    pub cancel: CancellationToken,
    pub report: mpsc::UnboundedSender<Report>,
}

/// Task body: runs the session and reports how it ended.
pub(crate) async fn run_session(p: SessionParams) {
    let (outcome, conn) = session(&p).await;
    if outcome == Outcome::Cancelled {
        // Shutdown or a finished switch: the close handshake is part of the job.
        if let Some(conn) = conn {
            close(conn, p.profile.close_timeout).await;
        }
        let _ = p.report.send(Report::Ended { id: p.id, outcome });
    } else {
        // Report first so the retry is not delayed by a close handshake with a dead peer.
        let _ = p.report.send(Report::Ended { id: p.id, outcome });
        if let Some(conn) = conn {
            close(conn, p.profile.close_timeout).await;
        }
    }
}

async fn at(deadline: Option<Instant>) {
    match deadline {
        Some(d) => sleep_until(d).await,
        None => std::future::pending().await,
    }
}

async fn send(conn: &mut BoxedConnection, msg: Message, limit: Duration) -> Result<(), Outcome> {
    match timeout(limit, conn.send(msg)).await {
        Err(_) => Err(Outcome::WriteTimeout),
        Ok(Err(e)) => Err(Outcome::Error(e.to_string())),
        Ok(Ok(())) => Ok(()),
    }
}

/// Sends a close frame and waits for the peer's close, bounded by `limit`; the socket is dropped after.
async fn close(mut conn: BoxedConnection, limit: Duration) {
    let _ = timeout(limit, async {
        let _ = conn.send(Message::Close(None)).await;
        while let Some(Ok(msg)) = conn.next().await {
            if matches!(msg, Message::Close(_)) {
                break;
            }
        }
    })
    .await;
}

async fn session(p: &SessionParams) -> (Outcome, Option<BoxedConnection>) {
    let prof = &*p.profile;
    let connected = tokio::select! {
        biased;
        _ = p.cancel.cancelled() => return (Outcome::Cancelled, None),
        r = timeout(prof.connect_timeout, p.connector.connect(&prof.url)) => r,
    };
    let mut conn = match connected {
        Err(_) => return (Outcome::Connect(ConnectFailure::Timeout), None),
        Ok(Err(ConnectError::Http {
            status,
            retry_after,
        })) => {
            return (
                Outcome::Connect(ConnectFailure::Http {
                    status,
                    retry_after,
                }),
                None,
            );
        }
        Ok(Err(ConnectError::Other(e))) => {
            return (Outcome::Connect(ConnectFailure::Other(e)), None);
        }
        Ok(Ok(c)) => c,
    };
    let outcome = drive(p, &mut conn).await;
    (outcome, Some(conn))
}

/// Subscribes and runs the read/keepalive loop until the session ends.
async fn drive(p: &SessionParams, conn: &mut BoxedConnection) -> Outcome {
    let prof = &*p.profile;

    for text in p.handler.subscribe_messages() {
        if let Err(o) = send(conn, Message::text(text), prof.write_timeout).await {
            return o;
        }
    }

    let now = Instant::now();
    let idle = prof.idle_timeout();
    let mut liveness_deadline = now + prof.data_silence;
    let mut idle_deadline = idle.map(|d| now + d);
    let mut next_ping = prof.ping.as_ref().map(|c| now + c.interval);
    let mut pong_deadline: Option<Instant> = None;
    let mut live = false;
    let mut gap_done = false;

    loop {
        tokio::select! {
            biased;
            _ = p.cancel.cancelled() => break Outcome::Cancelled,
            incoming = conn.next() => {
                let now = Instant::now();
                if let Some(d) = idle {
                    idle_deadline = Some(now + d);
                }
                let msg = match incoming {
                    None => break Outcome::Closed,
                    Some(Err(e)) => break Outcome::Error(e.to_string()),
                    Some(Ok(m)) => m,
                };
                let frame = match msg {
                    Message::Text(t) => Frame::Text(t.as_str().to_owned()),
                    Message::Binary(b) => Frame::Binary(b.to_vec()),
                    Message::Close(_) => break Outcome::Closed,
                    Message::Pong(_) => {
                        pong_deadline = None;
                        continue;
                    }
                    // Pings are answered by the protocol layer on the next read or write.
                    Message::Ping(_) | Message::Frame(_) => continue,
                };
                let recv_ts = p.clock.now_ms();
                match p.handler.classify(&frame) {
                    FrameClass::Pong => pong_deadline = None,
                    FrameClass::Ack { ok: true } | FrameClass::Ignore => {}
                    FrameClass::Ack { ok: false } => break Outcome::SubscribeRejected,
                    FrameClass::Reply(text) => {
                        if let Err(o) = send(conn, Message::text(text), prof.write_timeout).await {
                            break o;
                        }
                    }
                    FrameClass::Data { liveness } => {
                        if liveness {
                            liveness_deadline = now + prof.data_silence;
                            if !live {
                                live = true;
                                let _ = p.report.send(Report::Live { id: p.id });
                            }
                        }
                        if !gap_done {
                            gap_done = true;
                            if let Err(o) = emit_gap(p, recv_ts).await {
                                break o;
                            }
                        }
                        p.shared.last_event_ms.store(recv_ts, Ordering::Relaxed);
                        if let Err(o) = forward(p, frame, recv_ts) {
                            break o;
                        }
                    }
                }
            }
            _ = sleep_until(liveness_deadline) => break Outcome::Silence,
            _ = at(next_ping) => {
                let cfg = prof.ping.as_ref().expect("ping timer implies ping config");
                let msg = match &cfg.message {
                    PingMessage::Protocol => Message::Ping(Default::default()),
                    PingMessage::Text(t) => Message::text(t.clone()),
                };
                if let Err(o) = send(conn, msg, prof.write_timeout).await {
                    break o;
                }
                let now = Instant::now();
                pong_deadline.get_or_insert(now + cfg.pong_timeout);
                next_ping = Some(now + cfg.interval);
            }
            _ = at(pong_deadline) => break Outcome::PongTimeout,
            _ = at(idle_deadline) => break Outcome::TransportIdle,
        }
    }
}

/// Publishes the pending `Gap`, if any, before the first frame of a recovered connection.
async fn emit_gap(p: &SessionParams, recv_ts: i64) -> Result<(), Outcome> {
    let from = p.shared.gap_from_ms.swap(0, Ordering::AcqRel);
    if from == 0 {
        return Ok(());
    }
    let gap = Gap {
        exchange: p.profile.exchange.clone(),
        stream: p.profile.stream.clone(),
        from_ts: from,
        to_ts: recv_ts.max(from),
    };
    tracing::warn!(
        exchange = %gap.exchange,
        stream = %gap.stream,
        from_ts = gap.from_ts,
        to_ts = gap.to_ts,
        "connection recovered after a gap"
    );
    p.shared.counters.gaps.fetch_add(1, Ordering::Relaxed);
    // A gap must not be lost, so this one waits for room; it is cut short by shutdown.
    tokio::select! {
        biased;
        _ = p.cancel.cancelled() => Err(Outcome::Cancelled),
        r = p.out.send(WsOutput::Gap(gap)) => r.map_err(|_| Outcome::SinkClosed),
    }
}

/// Deduplicates and hands the frame to the consumer without blocking.
fn forward(p: &SessionParams, frame: Frame, recv_ts: i64) -> Result<(), Outcome> {
    let counters = &p.shared.counters;
    if let Some(key) = p.handler.dedup_key(&frame)
        && p.shared.dedup().is_duplicate(key)
    {
        counters.duplicates.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    let item = WsOutput::Data(WsMessage {
        session: p.id,
        recv_ts_ms: recv_ts,
        frame,
    });
    match p.out.try_send(item) {
        Ok(()) => {
            counters.frames_forwarded.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            let total = counters.dropped_overflow.fetch_add(1, Ordering::Relaxed) + 1;
            if total == 1 || total.is_multiple_of(10_000) {
                tracing::warn!(
                    exchange = %p.profile.exchange,
                    stream = %p.profile.stream,
                    dropped_total = total,
                    "consumer is too slow: channel full, frames dropped"
                );
            }
            Ok(())
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(Outcome::SinkClosed),
    }
}
