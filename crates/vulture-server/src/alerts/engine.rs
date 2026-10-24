//! The engine task: warm-up from the Redis stream, then the live loop.
//!
//! The engine never sits in the writer's path. [`EngineInput::offer`] hands a copy of each event to a bounded
//! queue with `try_send`; when the queue is full the engine's own input is dropped and counted, the writer is
//! unaffected. Signals the engine produces go back into the same pipeline as ordinary events.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::{Event, event::Payload};

use super::core::{Core, Output};
use super::delivery::Delivery;
use super::stats::EngineStats;
use crate::config::AlertsConfig;
use crate::store::{
    RedisStore, StoredEntry, TYPE_GAP, TYPE_LIQUIDATION, TYPE_OI, TYPE_SIGNAL, parse_id,
};
use crate::ws::Clock;

/// How many stream entries one warm-up page reads.
const WARMUP_PAGE: usize = 5000;
/// Signals older than this are not used to restore cooldowns.
const COOLDOWN_RESTORE_MS: i64 = 6 * 3600 * 1000;

/// The tap: the engine's input queue as seen by the pipeline.
#[derive(Clone)]
pub struct EngineInput {
    tx: mpsc::Sender<Event>,
    stats: Arc<EngineStats>,
    last_log_ms: Arc<AtomicI64>,
    clock: Arc<dyn Clock>,
}

impl EngineInput {
    /// A queue of `capacity` events and the receiving end for the engine task.
    pub fn channel(
        capacity: usize,
        stats: Arc<EngineStats>,
        clock: Arc<dyn Clock>,
    ) -> (Self, mpsc::Receiver<Event>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (
            Self {
                tx,
                stats,
                last_log_ms: Arc::new(AtomicI64::new(0)),
                clock,
            },
            rx,
        )
    }

    /// Offers a copy of `event`; never waits. A full queue drops it and counts (logged at most every 10 s).
    pub fn offer(&self, event: &Event) {
        if matches!(event.payload, Some(Payload::Signal(_)) | None) {
            return;
        }
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(event.clone()) {
            let n = self.stats.input_dropped.fetch_add(1, Ordering::Relaxed) + 1;
            let now = self.clock.now_ms();
            let last = self.last_log_ms.load(Ordering::Relaxed);
            if now - last >= 10_000
                && self
                    .last_log_ms
                    .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                tracing::warn!(
                    dropped_total = n,
                    "the analytics engine cannot keep up: its input queue is full; dropping its own input (the Redis writer is unaffected)"
                );
            }
        }
    }
}

/// Everything the engine task needs.
pub struct EngineTask {
    /// The state.
    pub core: Core,
    /// Receiving end of the tap.
    pub rx: mpsc::Receiver<Event>,
    /// Sender into the writer's channel, for signals.
    pub writer: mpsc::Sender<Event>,
    /// Delivery queues.
    pub delivery: Arc<Delivery>,
    /// The Redis stream, for the warm-up.
    pub store: Arc<dyn RedisStore>,
    /// Wall clock.
    pub clock: Arc<dyn Clock>,
    /// Configuration.
    pub cfg: Arc<AlertsConfig>,
    /// Stops the task.
    pub shutdown: CancellationToken,
}

fn successor(id: &str) -> Option<String> {
    let (ms, seq) = parse_id(id)?;
    Some(format!("{ms}-{}", seq + 1))
}

impl EngineTask {
    /// Runs until the input closes or `shutdown` fires.
    pub async fn run(mut self) {
        self.warm_up().await;
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => break,
                ev = self.rx.recv() => {
                    let Some(first) = ev else { break };
                    let mut out = Output::default();
                    self.process_one(&first, &mut out);
                    // Take what is already queued before going back to the select.
                    for _ in 0..255 {
                        match self.rx.try_recv() {
                            Ok(ev) => self.process_one(&ev, &mut out),
                            Err(_) => break,
                        }
                    }
                    self.dispatch(out).await;
                }
                _ = tick.tick() => {
                    let mut out = Output::default();
                    self.core.tick(self.clock.now_ms(), &mut out);
                    self.dispatch(out).await;
                }
            }
        }
        tracing::info!("analytics engine stopped");
    }

    fn process_one(&mut self, ev: &Event, out: &mut Output) {
        let t0 = Instant::now();
        self.core.process(ev, true, out);
        self.core
            .stats()
            .record_eval(t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
    }

    async fn dispatch(&mut self, out: Output) {
        let stats = self.core.stats().clone();
        for hit in out.hits {
            for channel in &hit.channels {
                self.delivery.push(channel, hit.alert.clone());
                stats.alerts_queued.fetch_add(1, Ordering::Relaxed);
            }
        }
        for signal in out.signals {
            tracing::info!(
                kind = ?vulture_proto::v1::SignalType::try_from(signal.r#type).unwrap_or_default(),
                exchange = %signal.exchange,
                symbol = %signal.symbol,
                window_secs = signal.window_secs,
                change_pct = signal.change_pct,
                "signal"
            );
            let event = Event {
                id: String::new(),
                payload: Some(Payload::Signal(signal)),
            };
            // Signals are rare; the writer's channel has room unless Redis is stuck, in which case the engine
            // waits here and its own input queue absorbs (or drops) the events behind it.
            if self.writer.send(event).await.is_err() {
                return;
            }
        }
    }

    /// Replays the last `warmup_secs` of the stream so that a restart does not blind the detectors.
    async fn warm_up(&mut self) {
        let stats = self.core.stats().clone();
        let started = Instant::now();
        let now = self.clock.now_ms();
        let warm_ms = (self.cfg.warmup_secs as i64) * 1000;
        let mut entries_seen = 0u64;
        if warm_ms > 0 {
            let start_ms = now - warm_ms;
            let oi_horizon = now
                - (self
                    .cfg
                    .oi_spike
                    .windows
                    .iter()
                    .map(|w| i64::from(w.secs))
                    .max()
                    .unwrap_or(0)
                    * 1000
                    + 900_000);
            match self
                .replay(start_ms, oi_horizon, now, &mut entries_seen)
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "warm-up from Redis failed; the detectors start with what was read");
                }
            }
        }
        self.core.refresh_now(now);
        let ms = started.elapsed().as_millis() as u64;
        stats.warmup_ms.store(ms, Ordering::Relaxed);
        stats.warmup_events.store(entries_seen, Ordering::Relaxed);
        stats.warmed_up.store(1, Ordering::Relaxed);
        tracing::info!(
            entries = entries_seen,
            elapsed_ms = ms,
            symbols = self.core.symbol_count(),
            "analytics engine warmed up from the Redis stream"
        );
    }

    async fn replay(
        &mut self,
        start_ms: i64,
        oi_horizon: i64,
        now: i64,
        seen: &mut u64,
    ) -> Result<(), String> {
        let bounds = self.store.bounds().await.map_err(|e| e.to_string())?;
        let Some(last) = bounds.last else {
            return Ok(());
        };
        let mut next = format!("{}-0", start_ms.max(0));
        let mut max_liq_ts = i64::MIN;
        let mut failures = 0;
        loop {
            if self.shutdown.is_cancelled() {
                return Ok(());
            }
            let page = match self.store.read_range(&next, &last, Some(WARMUP_PAGE)).await {
                Ok(p) => p,
                Err(e) => {
                    failures += 1;
                    if failures > 5 {
                        return Err(e.to_string());
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            let Some(last_id) = page.last().map(|e| e.id.clone()) else {
                break;
            };
            let full = page.len() >= WARMUP_PAGE;
            for entry in page {
                *seen += 1;
                self.replay_entry(entry, oi_horizon, now, &mut max_liq_ts);
            }
            // Give other tasks a turn between pages.
            tokio::task::yield_now().await;
            if !full {
                break;
            }
            match successor(&last_id) {
                Some(n) => next = n,
                None => break,
            }
        }
        if max_liq_ts != i64::MIN {
            self.core.set_replay_watermark(max_liq_ts);
        }
        Ok(())
    }

    fn replay_entry(
        &mut self,
        entry: StoredEntry,
        oi_horizon: i64,
        now: i64,
        max_liq_ts: &mut i64,
    ) {
        let id_ms = parse_id(&entry.id).map_or(0, |(ms, _)| ms as i64);
        let wanted = match entry.kind.as_str() {
            TYPE_OI | TYPE_GAP => id_ms >= oi_horizon,
            TYPE_LIQUIDATION => true,
            TYPE_SIGNAL => id_ms >= now - COOLDOWN_RESTORE_MS,
            _ => false,
        };
        if !wanted {
            return;
        }
        let Ok(event) = entry.into_event() else {
            return;
        };
        match &event.payload {
            Some(Payload::Signal(s)) => self.core.restore_signal(s),
            Some(Payload::Liquidation(l)) => {
                *max_liq_ts = (*max_liq_ts).max(l.recv_ts);
                self.core.process(&event, false, &mut Output::default());
            }
            _ => self.core.process(&event, false, &mut Output::default()),
        }
    }
}
