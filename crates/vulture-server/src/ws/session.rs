//! One WebSocket session: connect, subscribe, read, keep alive, watch for silence.
//!
//! A session runs in its own task and never blocks on the consumer: frames go to a bounded channel with
//! `try_send`, and a full channel drops the frame and counts it. The supervisor in [`super::manager`] decides
//! what to do when a session ends.

use std::collections::{BTreeSet, HashMap};
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
use super::limiter::ConnectionLimiter;
use super::profile::{PingMessage, WsProfile};
use super::status::WsCounters;
use super::topics::{TopicControl, TopicOp, TopicRequest};

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
    /// A runtime topic request got no response within `subscribe_ack_timeout`.
    SubscribeTimeout,
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
    pub topics: Option<Arc<TopicControl>>,
    pub limiter: Option<Arc<ConnectionLimiter>>,
}

/// Task body: runs the session and reports how it ended.
pub(crate) async fn run_session(p: SessionParams) {
    // The permit lives until the socket is closed. Failed attempts use a slot of the window as well.
    let _permit = match &p.limiter {
        Some(limiter) => {
            tokio::select! {
                biased;
                _ = p.cancel.cancelled() => {
                    let _ = p.report.send(Report::Ended { id: p.id, outcome: Outcome::Cancelled });
                    return;
                }
                permit = limiter.acquire() => Some(permit),
            }
        }
        None => None,
    };
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
    let mut tracker = p.topics.as_ref().map(|c| Tracker::new(c.clone()));
    let mut topics_rx = p.topics.as_ref().map(|c| c.watch());
    // Without runtime topics the liveness stream is expected from the moment the subscription is sent. With
    // them, the clock starts when the first topic is subscribed (an empty set has nothing to watch).
    let mut liveness_deadline = if tracker.is_some() {
        None
    } else {
        Some(now + prof.data_silence)
    };
    let mut idle_deadline = idle.map(|d| now + d);
    let mut next_ping = prof.ping.as_ref().map(|c| now + c.interval);
    let mut pong_deadline: Option<Instant> = None;
    let mut live = false;
    let mut gap_done = false;

    if let (Some(tr), Some(rx)) = (tracker.as_mut(), topics_rx.as_mut()) {
        tr.control.clear_rejected();
        let desired = rx.borrow_and_update().clone();
        if let Err(o) = tr.sync(p, conn, &desired).await {
            return o;
        }
        if tr.known.is_empty() {
            // Nothing to listen to: the connection is healthy as it is, and no gap can have happened.
            live = true;
            let _ = p.report.send(Report::Live { id: p.id });
            p.shared.gap_from_ms.store(0, Ordering::Release);
            gap_done = true;
        } else {
            liveness_deadline = Some(Instant::now() + prof.data_silence);
        }
    }

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
                    FrameClass::TopicResponse { id, ok, detail } => {
                        if let Some(tr) = tracker.as_mut() {
                            let was_empty = tr.known.is_empty();
                            if let Err(o) = tr.on_response(p, conn, id, ok, &detail).await {
                                break o;
                            }
                            if tr.known.is_empty() && !was_empty {
                                liveness_deadline = None;
                            }
                        }
                    }
                    FrameClass::TopicRefused { topics, detail } => {
                        if let Some(tr) = tracker.as_mut() {
                            let was_empty = tr.known.is_empty();
                            for topic in topics {
                                if tr.known.contains(&topic) {
                                    tr.reject(p, &topic, &detail);
                                }
                            }
                            if tr.known.is_empty() && !was_empty {
                                liveness_deadline = None;
                            }
                        }
                    }
                    FrameClass::Reply(text) => {
                        if let Err(o) = send(conn, Message::text(text), prof.write_timeout).await {
                            break o;
                        }
                    }
                    FrameClass::Data { liveness } => {
                        if liveness {
                            if liveness_deadline.is_some() || tracker.is_none() {
                                liveness_deadline = Some(now + prof.data_silence);
                            }
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
            changed = topics_changed(&mut topics_rx) => {
                if !changed {
                    topics_rx = None;
                    continue;
                }
                let tr = tracker.as_mut().expect("topic receiver implies tracker");
                let desired = topics_rx
                    .as_mut()
                    .expect("checked above")
                    .borrow_and_update()
                    .clone();
                let was_empty = tr.known.is_empty();
                if let Err(o) = tr.sync(p, conn, &desired).await {
                    break o;
                }
                if tr.known.is_empty() {
                    liveness_deadline = None;
                } else if was_empty || liveness_deadline.is_none() {
                    liveness_deadline = Some(Instant::now() + prof.data_silence);
                }
                if !live && tr.known.is_empty() {
                    live = true;
                    let _ = p.report.send(Report::Live { id: p.id });
                }
            }
            _ = at(liveness_deadline) => break Outcome::Silence,
            _ = at(tracker.as_ref().and_then(|t| t.ack_deadline(prof.subscribe_ack_timeout))) => {
                p.shared.counters.topic_ack_timeouts.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    exchange = %prof.exchange,
                    stream = %prof.stream,
                    "topic request not answered in time; reconnecting"
                );
                break Outcome::SubscribeTimeout;
            }
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

/// Resolves with `true` when the desired topic set changed, `false` when its control is gone; never resolves
/// without a receiver.
async fn topics_changed(
    rx: &mut Option<tokio::sync::watch::Receiver<Arc<BTreeSet<String>>>>,
) -> bool {
    match rx {
        Some(rx) => rx.changed().await.is_ok(),
        None => std::future::pending().await,
    }
}

struct PendingRequest {
    op: TopicOp,
    topics: Vec<String>,
    sent_at: Instant,
}

/// Topic bookkeeping of one session.
struct Tracker {
    control: Arc<TopicControl>,
    /// Subscribed (request sent and not refused) in this session.
    known: BTreeSet<String>,
    /// Refused in this session; not retried until the next one.
    declined: BTreeSet<String>,
    pending: HashMap<String, PendingRequest>,
    next_id: u64,
}

impl Tracker {
    fn new(control: Arc<TopicControl>) -> Self {
        Self {
            control,
            known: BTreeSet::new(),
            declined: BTreeSet::new(),
            pending: HashMap::new(),
            next_id: 1,
        }
    }

    fn ack_deadline(&self, timeout: Duration) -> Option<Instant> {
        self.pending.values().map(|r| r.sent_at + timeout).min()
    }

    /// Brings the session in line with the desired set: unsubscribe what left, subscribe what is new.
    async fn sync(
        &mut self,
        p: &SessionParams,
        conn: &mut BoxedConnection,
        desired: &BTreeSet<String>,
    ) -> Result<(), Outcome> {
        self.declined.retain(|t| desired.contains(t));
        let remove: Vec<String> = self.known.difference(desired).cloned().collect();
        let add: Vec<String> = desired
            .iter()
            .filter(|t| !self.known.contains(*t) && !self.declined.contains(*t))
            .cloned()
            .collect();
        if !remove.is_empty() {
            for t in &remove {
                self.known.remove(t);
            }
            self.send(p, conn, TopicOp::Unsubscribe, &remove).await?;
        }
        if !add.is_empty() {
            self.known.extend(add.iter().cloned());
            self.send(p, conn, TopicOp::Subscribe, &add).await?;
        }
        p.shared
            .counters
            .topics_active
            .store(self.known.len() as u64, Ordering::Relaxed);
        if !remove.is_empty() || !add.is_empty() {
            tracing::debug!(
                exchange = %p.profile.exchange,
                stream = %p.profile.stream,
                added = add.len(),
                removed = remove.len(),
                subscribed = self.known.len(),
                "topics synchronized"
            );
        }
        Ok(())
    }

    async fn send(
        &mut self,
        p: &SessionParams,
        conn: &mut BoxedConnection,
        op: TopicOp,
        topics: &[String],
    ) -> Result<(), Outcome> {
        let requests: Vec<TopicRequest> = p.handler.topic_requests(op, topics, &mut self.next_id);
        for r in requests {
            send(conn, Message::text(r.text), p.profile.write_timeout).await?;
            p.shared
                .counters
                .topic_requests
                .fetch_add(1, Ordering::Relaxed);
            self.pending.insert(
                r.id,
                PendingRequest {
                    op,
                    topics: r.topics,
                    sent_at: Instant::now(),
                },
            );
        }
        Ok(())
    }

    async fn on_response(
        &mut self,
        p: &SessionParams,
        conn: &mut BoxedConnection,
        id: Option<String>,
        ok: bool,
        detail: &str,
    ) -> Result<(), Outcome> {
        let request = id.as_ref().and_then(|i| self.pending.remove(i));
        if ok {
            return Ok(());
        }
        let Some(request) = request else {
            tracing::warn!(
                exchange = %p.profile.exchange,
                stream = %p.profile.stream,
                id = ?id,
                detail,
                "failure response to an unknown topic request"
            );
            return Ok(());
        };
        if request.op == TopicOp::Unsubscribe {
            // The topic keeps arriving; the connector filters it. Nothing else to do.
            tracing::warn!(
                exchange = %p.profile.exchange,
                stream = %p.profile.stream,
                topics = request.topics.len(),
                detail,
                "unsubscribe refused"
            );
            return Ok(());
        }
        let live: Vec<String> = request
            .topics
            .into_iter()
            .filter(|t| self.known.contains(t))
            .collect();
        let offenders: Vec<String> = live
            .iter()
            .filter(|t| names_topic(detail, t))
            .cloned()
            .collect();
        let rest: Vec<String> = live
            .iter()
            .filter(|t| !offenders.contains(t))
            .cloned()
            .collect();
        // A single-topic request cannot be split further: it is the offender even if the message is vague.
        let offenders = if offenders.is_empty() && live.len() == 1 {
            live.clone()
        } else {
            offenders
        };
        for t in &offenders {
            self.reject(p, t, detail);
        }
        let rest: Vec<String> = rest
            .into_iter()
            .filter(|t| self.known.contains(t))
            .collect();
        if rest.is_empty() {
            return Ok(());
        }
        if !offenders.is_empty() {
            // The refusal named the culprit: send the others again as one request.
            return self.send(p, conn, TopicOp::Subscribe, &rest).await;
        }
        // Unknown culprit: halve until it is isolated (at most log2(n) rounds, 2 requests per round).
        let (a, b) = rest.split_at(rest.len() / 2);
        tracing::warn!(
            exchange = %p.profile.exchange,
            stream = %p.profile.stream,
            topics = rest.len(),
            detail,
            "subscribe refused without naming a topic; splitting the request"
        );
        self.send(p, conn, TopicOp::Subscribe, a).await?;
        self.send(p, conn, TopicOp::Subscribe, b).await
    }

    fn reject(&mut self, p: &SessionParams, topic: &str, detail: &str) {
        self.known.remove(topic);
        self.declined.insert(topic.to_string());
        self.control.mark_rejected(topic, detail);
        p.shared
            .counters
            .topics_rejected
            .fetch_add(1, Ordering::Relaxed);
        p.shared
            .counters
            .topics_active
            .store(self.known.len() as u64, Ordering::Relaxed);
        tracing::warn!(
            exchange = %p.profile.exchange,
            stream = %p.profile.stream,
            topic,
            reason = detail,
            "topic refused by the exchange; it stays unsubscribed until the next connection"
        );
    }
}

/// Whether `detail` mentions `topic` as a whole token (not inside a longer name).
fn names_topic(detail: &str, topic: &str) -> bool {
    let is_name_char = |c: char| c.is_alphanumeric() || c == '.' || c == '_';
    let mut from = 0;
    while let Some(i) = detail[from..].find(topic) {
        let start = from + i;
        let end = start + topic.len();
        let before = detail[..start].chars().next_back();
        let after = detail[end..].chars().next();
        if before.is_none_or(|c| !is_name_char(c)) && after.is_none_or(|c| !is_name_char(c)) {
            return true;
        }
        from = end;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::names_topic;

    #[test]
    fn topic_names_match_whole_tokens() {
        assert!(names_topic(
            "error:handler not found,topic:tickers.FOOUSDT",
            "tickers.FOOUSDT"
        ));
        assert!(names_topic(
            "bad topic tickers.FOOUSDT, sorry",
            "tickers.FOOUSDT"
        ));
        assert!(!names_topic("topic:tickers.FOOUSDT2", "tickers.FOOUSDT"));
        assert!(!names_topic("topic:tickers.XFOOUSDT", "FOOUSDT"));
        assert!(!names_topic("handler not found,topic:bad", "c"));
        assert!(!names_topic("nothing here", "tickers.FOOUSDT"));
    }
}
