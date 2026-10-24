//! The supervisor of a managed WebSocket connection.
//!
//! One task per connection runs an event loop over session reports and a handful of deadlines (retry,
//! planned switch, healthy reset, escalation, dedup release). It owns the backoff, decides when to start a
//! session, publishes [`WsStatus`] and counts everything in [`WsCounters`].

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until, timeout};
use tokio_util::sync::CancellationToken;

use super::backoff::Backoff;
use super::clock::Clock;
use super::connector::WsConnector;
use super::handler::WsHandler;
use super::limiter::ConnectionLimiter;
use super::profile::WsProfile;
use super::session::{
    ConnectFailure, Outcome, Report, SessionParams, Shared, WsOutput, run_session,
};
use super::status::{ConnectionState, WsCounters, WsStatus};
use super::topics::TopicControl;

/// Handle to a running managed connection.
pub struct WsHandle {
    /// State for metrics and `Control`.
    pub status: watch::Receiver<WsStatus>,
    /// Counters for metrics.
    pub counters: Arc<WsCounters>,
    task: JoinHandle<()>,
}

impl WsHandle {
    /// Waits for the supervisor to finish (after the shutdown token was cancelled).
    pub async fn join(self) {
        let _ = self.task.await;
    }
}

/// Optional extras of a managed connection.
#[derive(Default, Clone)]
pub struct WsOptions {
    /// Desired topics, changed at runtime. Needs a handler that implements
    /// [`WsHandler::topic_requests`].
    pub topics: Option<Arc<TopicControl>>,
    /// Connection budget shared with the other connections of the exchange.
    pub limiter: Option<Arc<ConnectionLimiter>>,
}

/// Starts a managed connection.
///
/// Returns the handle and the receiver of [`WsOutput`]. The consumer must keep reading: a full channel
/// drops data frames (counted), and only `Gap` events wait for room. Cancelling `shutdown` closes the
/// sessions within the close timeout and moves the state to `Stopped`.
pub fn spawn_ws(
    profile: WsProfile,
    handler: Arc<dyn WsHandler>,
    connector: Arc<dyn WsConnector>,
    clock: Arc<dyn Clock>,
    shutdown: CancellationToken,
) -> Result<(WsHandle, mpsc::Receiver<WsOutput>), String> {
    spawn_ws_with(
        profile,
        handler,
        connector,
        clock,
        shutdown,
        WsOptions::default(),
    )
}

/// [`spawn_ws`] with runtime topics and/or a connection limiter.
pub fn spawn_ws_with(
    profile: WsProfile,
    handler: Arc<dyn WsHandler>,
    connector: Arc<dyn WsConnector>,
    clock: Arc<dyn Clock>,
    shutdown: CancellationToken,
    options: WsOptions,
) -> Result<(WsHandle, mpsc::Receiver<WsOutput>), String> {
    profile.validate()?;
    let (out_tx, out_rx) = mpsc::channel(profile.channel_capacity);
    let counters = Arc::new(WsCounters::default());
    let (status_tx, status_rx) = watch::channel(WsStatus {
        state: ConnectionState::Connecting,
        reconnects: 0,
        consecutive_failures: 0,
    });
    let backoff = Backoff::new(
        profile.backoff_min,
        profile.backoff_max,
        profile.jitter_seed,
    );
    let sup = Supervisor {
        shared: Arc::new(Shared::new(profile.dedup_capacity, counters.clone())),
        profile: Arc::new(profile),
        handler,
        connector,
        clock,
        options,
        out: out_tx,
        shutdown,
        status: status_tx,
        backoff,
        counters: counters.clone(),
        next_id: 1,
        current: None,
        pending: None,
        retiring: None,
        live_ids: HashSet::new(),
        retry_at: None,
        age_at: None,
        healthy_reset_at: None,
        dedup_off_at: None,
        outage_started: None,
        degraded: false,
        ever_live: false,
        sink_closed: false,
    };
    let task = tokio::spawn(sup.run());
    Ok((
        WsHandle {
            status: status_rx,
            counters,
            task,
        },
        out_rx,
    ))
}

struct Active {
    id: u64,
    cancel: CancellationToken,
    live_since: Instant,
}

struct Pending {
    id: u64,
    cancel: CancellationToken,
    /// A make-before-break replacement while the current session is still alive.
    planned: bool,
}

struct Supervisor {
    profile: Arc<WsProfile>,
    handler: Arc<dyn WsHandler>,
    connector: Arc<dyn WsConnector>,
    clock: Arc<dyn Clock>,
    options: WsOptions,
    out: mpsc::Sender<WsOutput>,
    shared: Arc<Shared>,
    shutdown: CancellationToken,
    status: watch::Sender<WsStatus>,
    backoff: Backoff,
    counters: Arc<WsCounters>,

    next_id: u64,
    current: Option<Active>,
    pending: Option<Pending>,
    /// Old session of a switch that is closing.
    retiring: Option<u64>,
    /// Ids of tasks that have not reported `Ended` yet.
    live_ids: HashSet<u64>,

    retry_at: Option<Instant>,
    age_at: Option<Instant>,
    healthy_reset_at: Option<Instant>,
    dedup_off_at: Option<Instant>,
    outage_started: Option<Instant>,
    degraded: bool,
    ever_live: bool,
    sink_closed: bool,
}

async fn at(deadline: Option<Instant>) {
    match deadline {
        Some(d) => sleep_until(d).await,
        None => std::future::pending().await,
    }
}

impl Supervisor {
    async fn run(mut self) {
        let (report_tx, mut reports) = mpsc::unbounded_channel::<Report>();
        let now = Instant::now();
        self.retry_at = Some(now);
        self.outage_started = Some(now);
        self.publish();

        loop {
            let escalate_at = match (self.degraded, self.outage_started) {
                (false, Some(t)) => Some(t + self.profile.degraded_after_gap),
                _ => None,
            };
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => break,
                Some(report) = reports.recv() => self.on_report(report),
                _ = at(self.retry_at) => {
                    self.retry_at = None;
                    self.start_session(false, &report_tx);
                }
                _ = at(self.age_at) => {
                    self.age_at = None;
                    if self.current.is_some() && self.pending.is_none() {
                        tracing::info!(
                            exchange = %self.profile.exchange,
                            stream = %self.profile.stream,
                            "planned reconnect: opening the replacement session"
                        );
                        self.shared.dedup().activate();
                        self.start_session(true, &report_tx);
                    }
                }
                _ = at(self.healthy_reset_at) => {
                    self.healthy_reset_at = None;
                    self.backoff.reset();
                    self.publish();
                }
                _ = at(self.dedup_off_at) => {
                    self.dedup_off_at = None;
                    self.shared().dedup().deactivate();
                }
                _ = at(escalate_at) => self.degrade("outage lasted too long"),
            }
            if self.sink_closed {
                tracing::warn!(
                    exchange = %self.profile.exchange,
                    "consumer channel closed; stopping the connection"
                );
                break;
            }
        }
        self.stop(&mut reports).await;
    }

    fn shared(&self) -> &Shared {
        &self.shared
    }

    fn start_session(&mut self, planned: bool, report_tx: &mpsc::UnboundedSender<Report>) {
        let id = self.next_id;
        self.next_id += 1;
        let cancel = self.shutdown.child_token();
        self.pending = Some(Pending {
            id,
            cancel: cancel.clone(),
            planned,
        });
        self.live_ids.insert(id);
        tokio::spawn(run_session(SessionParams {
            id,
            profile: self.profile.clone(),
            handler: self.handler.clone(),
            connector: self.connector.clone(),
            clock: self.clock.clone(),
            out: self.out.clone(),
            shared: self.shared.clone(),
            cancel,
            report: report_tx.clone(),
            topics: self.options.topics.clone(),
            limiter: self.options.limiter.clone(),
        }));
        self.publish();
    }

    fn on_report(&mut self, report: Report) {
        match report {
            Report::Live { id } => self.on_live(id),
            Report::Ended { id, outcome } => {
                self.live_ids.remove(&id);
                self.on_ended(id, outcome);
            }
        }
        self.publish();
    }

    fn on_live(&mut self, id: u64) {
        let Some(pending) = self.pending.take_if(|p| p.id == id) else {
            return;
        };
        let now = Instant::now();
        if let Some(old) = self.current.take() {
            // Make-before-break: the replacement is live, now the old session may close.
            old.cancel.cancel();
            self.retiring = Some(old.id);
            self.counters
                .planned_reconnects
                .fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                exchange = %self.profile.exchange,
                stream = %self.profile.stream,
                "planned reconnect completed without a gap"
            );
        } else {
            if self.ever_live {
                self.counters.reconnects.fetch_add(1, Ordering::Relaxed);
            }
            self.dedup_off_at = Some(now + self.profile.overlap_grace);
        }
        self.ever_live = true;
        self.current = Some(Active {
            id: pending.id,
            cancel: pending.cancel,
            live_since: now,
        });
        self.outage_started = None;
        self.degraded = false;
        self.healthy_reset_at = Some(now + self.profile.healthy_reset);
        self.age_at = self.next_age_deadline(now);
        tracing::info!(
            exchange = %self.profile.exchange,
            stream = %self.profile.stream,
            session = id,
            "connection healthy"
        );
    }

    fn on_ended(&mut self, id: u64, outcome: Outcome) {
        if outcome == Outcome::SinkClosed {
            self.sink_closed = true;
            return;
        }
        let now = Instant::now();
        if self.retiring == Some(id) {
            self.retiring = None;
            self.dedup_off_at = Some(now + self.profile.overlap_grace);
            return;
        }
        if self.current.as_ref().is_some_and(|c| c.id == id) {
            let current = self.current.take().expect("checked above");
            self.healthy_reset_at = None;
            self.age_at = None;
            if now.duration_since(current.live_since) >= self.profile.healthy_reset {
                self.backoff.reset();
            }
            self.outage_started.get_or_insert(now);
            let last = self.shared.last_event_ms.load(Ordering::Relaxed);
            if last > 0 {
                let _ = self.shared.gap_from_ms.compare_exchange(
                    0,
                    last,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
            tracing::warn!(
                exchange = %self.profile.exchange,
                stream = %self.profile.stream,
                reason = ?outcome,
                "connection lost"
            );
            let pause = self.on_failure(&outcome);
            match self.pending.as_mut() {
                // A replacement is already connecting: it becomes the recovery session.
                Some(p) => p.planned = false,
                None => self.retry_at = Some(now + pause),
            }
            return;
        }
        if self.pending.as_ref().is_some_and(|p| p.id == id) {
            let pending = self.pending.take().expect("checked above");
            if pending.planned && self.current.is_some() {
                self.counters
                    .planned_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.shared.dedup().deactivate();
                self.age_at = Some(now + self.profile.planned_retry_delay);
                tracing::warn!(
                    exchange = %self.profile.exchange,
                    stream = %self.profile.stream,
                    reason = ?outcome,
                    "planned reconnect failed; keeping the current session"
                );
            } else {
                tracing::warn!(
                    exchange = %self.profile.exchange,
                    stream = %self.profile.stream,
                    reason = ?outcome,
                    "connection attempt failed"
                );
                let pause = self.on_failure(&outcome);
                self.retry_at = Some(now + pause);
            }
        }
        // Anything else is a session that was cancelled by us.
    }

    /// Counts a failure and returns the pause before the next attempt.
    fn on_failure(&mut self, outcome: &Outcome) -> Duration {
        self.counters
            .failed_attempts
            .fetch_add(1, Ordering::Relaxed);
        let mut pause = self.backoff.record_failure();
        if let Outcome::Connect(ConnectFailure::Http {
            status,
            retry_after,
        }) = outcome
            && matches!(*status, 403 | 418 | 429)
        {
            self.counters.handshake_bans.fetch_add(1, Ordering::Relaxed);
            pause = pause
                .max(self.profile.ban_pause_min)
                .max(retry_after.unwrap_or_default())
                .min(self.profile.ban_pause_max);
            tracing::warn!(
                exchange = %self.profile.exchange,
                status,
                ?retry_after,
                ?pause,
                "handshake rejected, backing off"
            );
        }
        if self.backoff.failures() >= self.profile.degraded_after_failures {
            self.degrade("too many consecutive failed attempts");
        }
        tracing::debug!(
            exchange = %self.profile.exchange,
            failures = self.backoff.failures(),
            ?pause,
            "next attempt scheduled"
        );
        pause
    }

    fn degrade(&mut self, why: &str) {
        if self.degraded {
            return;
        }
        self.degraded = true;
        self.counters
            .degraded_entered
            .fetch_add(1, Ordering::Relaxed);
        tracing::error!(
            exchange = %self.profile.exchange,
            stream = %self.profile.stream,
            failures = self.backoff.failures(),
            reason = why,
            "connection degraded; retrying indefinitely"
        );
        self.publish();
    }

    fn next_age_deadline(&mut self, now: Instant) -> Option<Instant> {
        let cfg = self.profile.max_connection_age?;
        let offset = self.backoff.symmetric_jitter_ms(cfg.jitter);
        let ms = (cfg.age.as_millis() as i64 + offset).max(1) as u64;
        Some(now + Duration::from_millis(ms))
    }

    fn publish(&self) {
        let state = if self.current.is_some() {
            ConnectionState::Healthy
        } else if self.degraded {
            ConnectionState::Degraded
        } else {
            ConnectionState::Connecting
        };
        self.status.send_replace(WsStatus {
            state,
            reconnects: self.counters.reconnects.load(Ordering::Relaxed),
            consecutive_failures: self.backoff.failures(),
        });
    }

    /// Cancels every session and waits (bounded) for them to close.
    async fn stop(&mut self, reports: &mut mpsc::UnboundedReceiver<Report>) {
        if let Some(c) = self.current.take() {
            c.cancel.cancel();
        }
        if let Some(p) = self.pending.take() {
            p.cancel.cancel();
        }
        let wait = self.profile.close_timeout + Duration::from_secs(1);
        let _ = timeout(wait, async {
            while !self.live_ids.is_empty() {
                match reports.recv().await {
                    Some(Report::Ended { id, .. }) => {
                        self.live_ids.remove(&id);
                    }
                    Some(Report::Live { .. }) => {}
                    None => break,
                }
            }
        })
        .await;
        self.status.send_replace(WsStatus {
            state: ConnectionState::Stopped,
            reconnects: self.counters.reconnects.load(Ordering::Relaxed),
            consecutive_failures: self.backoff.failures(),
        });
        tracing::info!(
            exchange = %self.profile.exchange,
            stream = %self.profile.stream,
            "connection stopped"
        );
    }
}
