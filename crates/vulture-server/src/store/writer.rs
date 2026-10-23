//! The Redis writer: connector channel in, stream entries out.
//!
//! One task owns a FIFO of pending events. It is the batch accumulator in normal operation and the outage
//! buffer when Redis is unreachable, so ordering is preserved by construction.
//!
//! - **Batching:** a pipeline is sent when `batch_size` events are pending or `flush_interval` has passed
//!   since the oldest pending event arrived.
//! - **Outage:** a failed write keeps the events, marks the connection `Disconnected` and schedules the next
//!   attempt after an exponential backoff (1 s to 30 s, equal jitter). Meanwhile new events keep arriving, so
//!   connectors are never blocked.
//! - **Overflow:** above `buffer_limit` pending events the *oldest* are dropped and counted. The first write
//!   after that starts with one [`Gap`] event (`exchange` [`GAP_EXCHANGE`] = `*`, `stream` [`GAP_STREAM`] =
//!   `redis-writer`) whose `from_ts`/`to_ts` span the timestamps of the dropped events, placed before the
//!   events that were kept (they are newer).
//! - **Delivery:** at least once. A pipeline is `MULTI`/`EXEC`, so a batch is applied entirely or not at all,
//!   but if the connection dies after `EXEC` ran and before the reply arrived, the retry duplicates that batch.
//! - **Shutdown:** when the token fires or the channel closes, the writer flushes everything without waiting for
//!   `flush_interval`, for at most `shutdown_timeout`, and then stops.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until, timeout};
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::{Event, Gap, event::Payload};

use super::{EntryFields, RedisStore, StoreError, StoreState, Trim, WriterStatus};
use crate::config::RedisConfig;
use crate::ws::{Backoff, Clock};

/// `Gap.exchange` of gaps produced by the writer itself: not tied to one exchange.
pub const GAP_EXCHANGE: &str = "*";
/// `Gap.stream` of gaps produced by the writer itself.
pub const GAP_STREAM: &str = "redis-writer";

/// Tunables of the writer.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Length of the time window kept in the stream.
    pub window: Duration,
    /// Safeguard for the stream length.
    pub maxlen: u64,
    /// Events per pipeline, at most.
    pub batch_size: usize,
    /// Longest wait before a partial batch is sent.
    pub flush_interval: Duration,
    /// Events held in memory while Redis is slow or down.
    pub buffer_limit: usize,
    /// First retry delay after a failure.
    pub retry_min: Duration,
    /// Longest retry delay.
    pub retry_max: Duration,
    /// How long the final flush may take.
    pub shutdown_timeout: Duration,
    /// Period of the summary log line.
    pub log_interval: Duration,
}

impl WriterConfig {
    /// Takes the writer-relevant fields of `[redis]`; `shutdown_timeout` comes from `[server]`.
    pub fn from_redis(c: &RedisConfig, shutdown_timeout: Duration) -> Self {
        Self {
            window: Duration::from_secs(c.window_secs),
            maxlen: c.maxlen_safeguard,
            batch_size: c.batch_size,
            flush_interval: Duration::from_millis(c.flush_interval_ms),
            buffer_limit: c.buffer_limit,
            retry_min: Duration::from_secs(1),
            retry_max: Duration::from_secs(30),
            shutdown_timeout,
            log_interval: Duration::from_secs(60),
        }
    }
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self::from_redis(&RedisConfig::default(), Duration::from_secs(4))
    }
}

/// Settings the operator should have on the Redis server: `(name, expected value)`; `None` means "any value
/// except the default", used for `maxmemory` (0 is unlimited).
pub const RECOMMENDED_SETTINGS: [(&str, Option<&str>); 4] = [
    ("appendonly", Some("yes")),
    ("appendfsync", Some("everysec")),
    ("maxmemory", None),
    ("maxmemory-policy", Some("noeviction")),
];

/// Result of [`check_settings`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingsReport {
    /// The server answered; the list holds one line per setting that differs (empty means all fine).
    Checked(Vec<String>),
    /// `CONFIG GET` is not allowed on this server.
    Unavailable(String),
    /// The server could not be reached; try again later.
    Unreachable(String),
}

/// Compares the server's settings with [`RECOMMENDED_SETTINGS`]. Never fails.
pub async fn check_settings(store: &dyn RedisStore) -> SettingsReport {
    let names: Vec<&str> = RECOMMENDED_SETTINGS.iter().map(|(n, _)| *n).collect();
    let got = match timeout(Duration::from_secs(5), store.settings(&names)).await {
        Ok(Ok(v)) => v,
        Ok(Err(StoreError::Connection(m))) => return SettingsReport::Unreachable(m),
        Ok(Err(StoreError::Command(m))) => return SettingsReport::Unavailable(m),
        Err(_) => return SettingsReport::Unreachable("timeout".into()),
    };
    let mut diffs = Vec::new();
    for (name, expected) in RECOMMENDED_SETTINGS {
        let value = got.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str());
        match (expected, value) {
            (Some(want), Some(have)) if have.eq_ignore_ascii_case(want) => {}
            (Some(want), Some(have)) => {
                diffs.push(format!("{name} is {have:?}, recommended {want:?}"));
            }
            (None, Some(have)) if have != "0" => {}
            (None, Some(_)) => {
                diffs.push(format!(
                    "{name} is 0 (unlimited), recommended a limit such as 3gb"
                ));
            }
            (_, None) => diffs.push(format!("{name} was not reported by the server")),
        }
    }
    SettingsReport::Checked(diffs)
}

/// Event counts by type, for the periodic log line.
#[derive(Debug, Default, Clone, Copy)]
struct Counts {
    liquidations: u64,
    open_interest: u64,
    gaps: u64,
    signals: u64,
}

impl Counts {
    fn add(&mut self, event: &Event) {
        match &event.payload {
            Some(Payload::Liquidation(_)) => self.liquidations += 1,
            Some(Payload::OpenInterest(_)) => self.open_interest += 1,
            Some(Payload::Gap(_)) => self.gaps += 1,
            Some(Payload::Signal(_)) => self.signals += 1,
            None => {}
        }
    }
}

/// Interval of event timestamps that were dropped.
#[derive(Debug, Clone, Copy)]
struct Lost {
    from_ts: i64,
    to_ts: i64,
    count: u64,
}

/// Best timestamp of an event for describing a lost interval: `(from, to)`.
fn event_span(event: &Event) -> (i64, i64) {
    match &event.payload {
        Some(Payload::Liquidation(l)) => (l.recv_ts, l.recv_ts),
        Some(Payload::OpenInterest(o)) => (o.recv_ts, o.recv_ts),
        Some(Payload::Gap(g)) => (g.from_ts, g.to_ts),
        Some(Payload::Signal(s)) => (s.ts, s.ts),
        None => (0, 0),
    }
}

/// The writer. Build with [`RedisWriter::new`], keep [`RedisWriter::status`], then [`RedisWriter::run`] it.
pub struct RedisWriter {
    store: Arc<dyn RedisStore>,
    cfg: WriterConfig,
    clock: Arc<dyn Clock>,
    status: Arc<WriterStatus>,
}

impl RedisWriter {
    /// New writer.
    pub fn new(store: Arc<dyn RedisStore>, cfg: WriterConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            cfg,
            clock,
            status: Arc::new(WriterStatus::default()),
        }
    }

    /// Counters and state.
    pub fn status(&self) -> Arc<WriterStatus> {
        self.status.clone()
    }

    /// Runs until the channel is closed (all connectors stopped) or `shutdown` fired and the final flush is
    /// done (or `shutdown_timeout` passed).
    pub async fn run(self, mut rx: mpsc::Receiver<Event>, shutdown: CancellationToken) {
        let cfg = self.cfg.clone();
        let batch_size = cfg.batch_size.max(1);
        let mut pending: VecDeque<Event> = VecDeque::new();
        let mut lost: Option<Lost> = None;
        let mut first_pending_at: Option<Instant> = None;
        let mut retry_at: Option<Instant> = None;
        let mut backoff = Backoff::new(cfg.retry_min, cfg.retry_max, None);
        let mut rx_open = true;
        let mut final_deadline: Option<Instant> = None;
        let mut settings_done = false;
        let mut window = Counts::default();
        let mut total = Counts::default();
        let mut tick = tokio::time::interval(cfg.log_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;

        self.check_settings_once(&mut settings_done).await;

        loop {
            let now = Instant::now();
            let have_work = !pending.is_empty() || lost.is_some();
            let due = have_work.then(|| {
                let base = if final_deadline.is_some() || pending.len() >= batch_size {
                    now
                } else {
                    first_pending_at.map_or(now, |t| t + cfg.flush_interval)
                };
                base.max(retry_at.unwrap_or(now))
            });

            if final_deadline.is_some() && !rx_open && !have_work {
                break;
            }
            if let Some(deadline) = final_deadline
                && now >= deadline
            {
                break;
            }
            if let Some(d) = due
                && d <= now
            {
                let ok = self
                    .flush(
                        &mut pending,
                        &mut lost,
                        &mut retry_at,
                        &mut backoff,
                        batch_size,
                        final_deadline,
                    )
                    .await;
                if ok {
                    if pending.is_empty() {
                        first_pending_at = None;
                    }
                    self.status
                        .buffered
                        .store(pending.len() as u64, Ordering::Relaxed);
                    if !settings_done {
                        self.check_settings_once(&mut settings_done).await;
                    }
                }
                continue;
            }

            tokio::select! {
                event = rx.recv(), if rx_open => match event {
                    Some(event) => {
                        self.accept(event, &mut pending, &mut lost, &mut first_pending_at, &mut window, &mut total);
                        // Take what is already queued, up to one batch, before looking at the clock again.
                        while pending.len() < batch_size {
                            match rx.try_recv() {
                                Ok(event) => self.accept(event, &mut pending, &mut lost, &mut first_pending_at, &mut window, &mut total),
                                Err(_) => break,
                            }
                        }
                    }
                    None => {
                        rx_open = false;
                        final_deadline.get_or_insert(Instant::now() + cfg.shutdown_timeout);
                    }
                },
                _ = async {
                    match due {
                        Some(d) => sleep_until(d).await,
                        None => std::future::pending().await,
                    }
                } => {}
                _ = shutdown.cancelled(), if final_deadline.is_none() => {
                    final_deadline = Some(Instant::now() + cfg.shutdown_timeout);
                }
                _ = async {
                    match final_deadline {
                        Some(d) => sleep_until(d).await,
                        None => std::future::pending().await,
                    }
                } => {}
                _ = tick.tick() => {
                    let s = self.status.snapshot();
                    tracing::info!(
                        liquidations = window.liquidations,
                        open_interest = window.open_interest,
                        gaps = window.gaps,
                        signals = window.signals,
                        written = s.written,
                        buffered = s.buffered,
                        dropped = s.dropped,
                        "events in the last minute"
                    );
                    window = Counts::default();
                }
            }
        }

        let left = pending.len() as u64;
        if left > 0 || lost.is_some() {
            let extra = lost.map_or(0, |l| l.count);
            self.status.dropped.fetch_add(left, Ordering::Relaxed);
            tracing::error!(
                lost_events = left + extra,
                "Redis writer stopped with unwritten events"
            );
        }
        self.status.buffered.store(0, Ordering::Relaxed);
        let s = self.status.snapshot();
        tracing::info!(
            liquidations = total.liquidations,
            open_interest = total.open_interest,
            gaps = total.gaps,
            signals = total.signals,
            written = s.written,
            batches = s.batches,
            dropped = s.dropped,
            "Redis writer finished (totals)"
        );
    }

    fn accept(
        &self,
        event: Event,
        pending: &mut VecDeque<Event>,
        lost: &mut Option<Lost>,
        first_pending_at: &mut Option<Instant>,
        window: &mut Counts,
        total: &mut Counts,
    ) {
        window.add(&event);
        total.add(&event);
        if pending.is_empty() {
            *first_pending_at = Some(Instant::now());
        }
        pending.push_back(event);
        while pending.len() > self.cfg.buffer_limit.max(1) {
            if let Some(old) = pending.pop_front() {
                let (from, to) = event_span(&old);
                let l = lost.get_or_insert(Lost {
                    from_ts: from,
                    to_ts: to,
                    count: 0,
                });
                l.from_ts = l.from_ts.min(from);
                l.to_ts = l.to_ts.max(to);
                l.count += 1;
                self.status.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.status
            .buffered
            .store(pending.len() as u64, Ordering::Relaxed);
    }

    /// Sends up to one batch. Returns whether it was written.
    async fn flush(
        &self,
        pending: &mut VecDeque<Event>,
        lost: &mut Option<Lost>,
        retry_at: &mut Option<Instant>,
        backoff: &mut Backoff,
        batch_size: usize,
        final_deadline: Option<Instant>,
    ) -> bool {
        let take = pending.len().min(batch_size);
        let mut entries = Vec::with_capacity(take + 1);
        let gap_event = lost.map(|l| Event {
            id: String::new(),
            payload: Some(Payload::Gap(Gap {
                exchange: GAP_EXCHANGE.into(),
                stream: GAP_STREAM.into(),
                from_ts: l.from_ts,
                to_ts: l.to_ts,
            })),
        });
        if let Some(g) = &gap_event {
            entries.extend(EntryFields::from_event(g));
        }
        entries.extend(
            pending
                .iter()
                .take(take)
                .filter_map(EntryFields::from_event),
        );

        let trim = Trim {
            min_id_ms: self.clock.now_ms() - self.cfg.window.as_millis() as i64,
            maxlen: self.cfg.maxlen,
        };
        let result = if entries.is_empty() {
            Ok(Vec::new())
        } else {
            let call = self.store.append(&entries, trim);
            match final_deadline {
                Some(d) => timeout(d.saturating_duration_since(Instant::now()), call)
                    .await
                    .unwrap_or_else(|_| Err(StoreError::Connection("timeout".into()))),
                None => call.await,
            }
        };
        match result {
            Ok(_) => {
                let was_down = self.status.state() == StoreState::Disconnected;
                pending.drain(..take);
                if let Some(l) = lost.take() {
                    self.status.gaps_written.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        dropped = l.count,
                        from_ts = l.from_ts,
                        to_ts = l.to_ts,
                        "wrote a gap for events dropped while Redis was unavailable"
                    );
                }
                self.status
                    .written
                    .fetch_add(entries.len() as u64, Ordering::Relaxed);
                self.status.batches.fetch_add(1, Ordering::Relaxed);
                self.status
                    .last_write_ms
                    .store(self.clock.now_ms(), Ordering::Relaxed);
                self.status.set_state(StoreState::Connected);
                backoff.reset();
                *retry_at = None;
                if was_down {
                    tracing::info!("Redis writer recovered");
                }
                true
            }
            Err(e) => {
                let first = self.status.state() != StoreState::Disconnected;
                self.status.retries.fetch_add(1, Ordering::Relaxed);
                self.status.set_error(e.to_string());
                self.status.set_state(StoreState::Disconnected);
                let mut delay = backoff.record_failure();
                if let Some(d) = final_deadline {
                    delay = delay.min(d.saturating_duration_since(Instant::now()));
                }
                *retry_at = Some(Instant::now() + delay);
                if first {
                    tracing::warn!(error = %e, ?delay, buffered = pending.len(), "Redis write failed; buffering and retrying");
                } else {
                    tracing::debug!(error = %e, ?delay, buffered = pending.len(), "Redis write failed again");
                }
                false
            }
        }
    }

    async fn check_settings_once(&self, done: &mut bool) {
        match check_settings(self.store.as_ref()).await {
            SettingsReport::Checked(diffs) => {
                *done = true;
                for d in diffs {
                    tracing::warn!("Redis setting differs from the recommended one: {d}");
                }
            }
            SettingsReport::Unavailable(m) => {
                *done = true;
                tracing::debug!(error = %m, "cannot read Redis settings (CONFIG refused); skipping the check");
            }
            SettingsReport::Unreachable(m) => {
                tracing::debug!(error = %m, "Redis settings check postponed: connection problem");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{MemoryStore, StoredEntry};
    use crate::ws::SystemClock;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use vulture_proto::v1::{Liquidation, OpenInterest};

    fn liq(ts: i64) -> Event {
        Event {
            id: String::new(),
            payload: Some(Payload::Liquidation(Liquidation {
                exchange: "binance".into(),
                symbol: "BTC-USDT-PERP".into(),
                recv_ts: ts,
                ..Liquidation::default()
            })),
        }
    }

    fn oi(ts: i64, symbol: &str) -> Event {
        Event {
            id: String::new(),
            payload: Some(Payload::OpenInterest(OpenInterest {
                exchange: "binance".into(),
                symbol: symbol.into(),
                recv_ts: ts,
                oi_base: 1.0,
                ..OpenInterest::default()
            })),
        }
    }

    fn cfg() -> WriterConfig {
        WriterConfig {
            batch_size: 500,
            flush_interval: Duration::from_millis(50),
            buffer_limit: 100_000,
            shutdown_timeout: Duration::from_secs(4),
            log_interval: Duration::from_secs(3600),
            ..WriterConfig::default()
        }
    }

    struct Rig {
        store: Arc<MemoryStore>,
        tx: mpsc::Sender<Event>,
        token: CancellationToken,
        status: Arc<WriterStatus>,
        handle: JoinHandle<()>,
    }

    fn start(cfg: WriterConfig) -> Rig {
        let store = Arc::new(MemoryStore::new());
        let writer = RedisWriter::new(store.clone(), cfg, Arc::new(SystemClock));
        let status = writer.status();
        let (tx, rx) = mpsc::channel(10_000);
        let token = CancellationToken::new();
        let handle = tokio::spawn(writer.run(rx, token.clone()));
        Rig {
            store,
            tx,
            token,
            status,
            handle,
        }
    }

    fn ts_of(entry: &StoredEntry) -> i64 {
        let e = entry.clone().into_event().unwrap();
        event_span(&e).0
    }

    #[tokio::test(start_paused = true)]
    async fn batches_by_size_keep_order() {
        let mut c = cfg();
        c.batch_size = 3;
        c.flush_interval = Duration::from_secs(3600);
        let r = start(c);
        for i in 1..=7 {
            r.tx.send(liq(i)).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
        // Two full batches are out at once; the seventh event waits for the interval.
        assert_eq!(r.store.batch_sizes(), vec![3, 3]);
        assert_eq!(r.status.snapshot().buffered, 1);
        sleep(Duration::from_secs(3601)).await;
        assert_eq!(r.store.batch_sizes(), vec![3, 3, 1]);
        let ts: Vec<i64> = r.store.entries().iter().map(ts_of).collect();
        assert_eq!(ts, (1..=7).collect::<Vec<_>>());
        let s = r.status.snapshot();
        assert_eq!((s.written, s.batches, s.buffered, s.dropped), (7, 3, 0, 0));
        assert_eq!(s.state, StoreState::Connected);
        assert!(s.last_write_ms > 0);
    }

    #[tokio::test(start_paused = true)]
    async fn batches_by_time() {
        let r = start(cfg());
        r.tx.send(liq(1)).await.unwrap();
        r.tx.send(liq(2)).await.unwrap();
        sleep(Duration::from_millis(40)).await;
        assert!(r.store.batch_sizes().is_empty(), "not before the interval");
        sleep(Duration::from_millis(20)).await;
        assert_eq!(r.store.batch_sizes(), vec![2]);
        // The next event starts a new interval.
        r.tx.send(liq(3)).await.unwrap();
        sleep(Duration::from_millis(60)).await;
        assert_eq!(r.store.batch_sizes(), vec![2, 1]);
    }

    #[tokio::test(start_paused = true)]
    async fn entries_carry_type_exchange_symbol_and_the_entry_id() {
        let r = start(cfg());
        r.tx.send(liq(1)).await.unwrap();
        r.tx.send(oi(2, "ETH-USDT-PERP")).await.unwrap();
        let gap = Event {
            id: String::new(),
            payload: Some(Payload::Gap(Gap {
                exchange: "binance".into(),
                stream: "liquidations".into(),
                from_ts: 3,
                to_ts: 4,
            })),
        };
        r.tx.send(gap.clone()).await.unwrap();
        // An event without payload has nothing to store.
        r.tx.send(Event::default()).await.unwrap();
        sleep(Duration::from_millis(100)).await;
        let entries = r.store.entries();
        let fields: Vec<(&str, &str, &str)> = entries
            .iter()
            .map(|e| (e.kind.as_str(), e.exchange.as_str(), e.symbol.as_str()))
            .collect();
        assert_eq!(
            fields,
            vec![
                ("liq", "binance", "BTC-USDT-PERP"),
                ("oi", "binance", "ETH-USDT-PERP"),
                ("gap", "binance", ""),
            ]
        );
        let decoded = entries[2].clone().into_event().unwrap();
        assert_eq!(decoded.id, entries[2].id);
        assert_eq!(decoded.payload, gap.payload);
        let ids: Vec<&String> = entries.iter().map(|e| &e.id).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn an_outage_buffers_then_a_gap_describes_the_dropped_events() {
        let mut c = cfg();
        c.batch_size = 2;
        c.buffer_limit = 5;
        let r = start(c);
        r.store.set_failing(true);
        for i in 1..=12 {
            r.tx.send(liq(i)).await.unwrap();
        }
        sleep(Duration::from_secs(5)).await;
        let s = r.status.snapshot();
        assert_eq!(s.state, StoreState::Disconnected);
        assert_eq!(s.buffered, 5);
        assert_eq!(s.dropped, 7);
        assert!(s.retries >= 1);
        assert!(s.last_error.as_deref().unwrap().contains("injected"));
        assert!(r.store.entries().is_empty());

        r.store.set_failing(false);
        sleep(Duration::from_secs(40)).await;
        let entries = r.store.entries();
        // One gap first (events 1..=7 were dropped), then the five newest events in order.
        let first = entries[0].clone().into_event().unwrap();
        let Some(Payload::Gap(g)) = first.payload else {
            panic!("expected a gap first: {first:?}");
        };
        assert_eq!(
            (g.exchange.as_str(), g.stream.as_str()),
            ("*", "redis-writer")
        );
        assert_eq!((g.from_ts, g.to_ts), (1, 7));
        let rest: Vec<i64> = entries[1..].iter().map(ts_of).collect();
        assert_eq!(rest, vec![8, 9, 10, 11, 12]);
        let s = r.status.snapshot();
        assert_eq!(s.state, StoreState::Connected);
        assert_eq!((s.gaps_written, s.dropped, s.buffered), (1, 7, 0));
        assert_eq!(s.written, 6);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_back_off_up_to_thirty_seconds() {
        let r = start(cfg());
        r.store.set_failing(true);
        r.tx.send(liq(1)).await.unwrap();
        sleep(Duration::from_secs(300)).await;
        // Nominal delays 1, 2, 4, 8, 16, 30, 30, ... with jitter between half and the full delay.
        let attempts = r.store.append_attempts();
        assert!((10..=22).contains(&attempts), "attempts {attempts}");
        r.store.set_failing(false);
        sleep(Duration::from_secs(31)).await;
        assert_eq!(r.store.entries().len(), 1, "nothing is lost after recovery");
        assert_eq!(r.status.snapshot().dropped, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_flushes_what_is_buffered() {
        let mut c = cfg();
        c.flush_interval = Duration::from_secs(3600);
        let r = start(c);
        for i in 1..=5 {
            r.tx.send(liq(i)).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
        assert!(r.store.entries().is_empty());
        r.token.cancel();
        drop(r.tx);
        tokio::time::timeout(Duration::from_secs(1), r.handle)
            .await
            .expect("must finish right after the flush")
            .unwrap();
        assert_eq!(r.store.entries().len(), 5);
        assert_eq!(r.status.snapshot().buffered, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn closing_the_channel_alone_also_flushes() {
        let mut c = cfg();
        c.flush_interval = Duration::from_secs(3600);
        let r = start(c);
        r.tx.send(liq(1)).await.unwrap();
        drop(r.tx);
        tokio::time::timeout(Duration::from_secs(1), r.handle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.store.entries().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_with_redis_down_gives_up_after_the_timeout() {
        let r = start(cfg());
        r.store.set_failing(true);
        r.tx.send(liq(1)).await.unwrap();
        sleep(Duration::from_millis(100)).await;
        r.token.cancel();
        drop(r.tx);
        let started = Instant::now();
        tokio::time::timeout(Duration::from_secs(10), r.handle)
            .await
            .expect("must stop by itself")
            .unwrap();
        let took = started.elapsed();
        assert!(
            took >= Duration::from_secs(3) && took <= Duration::from_secs(5),
            "{took:?}"
        );
        assert_eq!(r.status.snapshot().dropped, 1);
    }

    #[tokio::test]
    async fn settings_report() {
        let store = MemoryStore::new();
        // CONFIG forbidden.
        assert!(matches!(
            check_settings(&store).await,
            SettingsReport::Unavailable(_)
        ));
        let all = |a: &str, f: &str, m: &str, p: &str| {
            Some(
                [
                    ("appendonly", a),
                    ("appendfsync", f),
                    ("maxmemory", m),
                    ("maxmemory-policy", p),
                ]
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .to_vec(),
            )
        };
        store.set_settings(all("yes", "everysec", "3221225472", "noeviction"));
        assert_eq!(
            check_settings(&store).await,
            SettingsReport::Checked(vec![])
        );
        store.set_settings(all("no", "always", "0", "allkeys-lru"));
        let SettingsReport::Checked(diffs) = check_settings(&store).await else {
            panic!("expected a report");
        };
        assert_eq!(diffs.len(), 4, "{diffs:?}");
        store.set_failing(true);
        assert!(matches!(
            check_settings(&store).await,
            SettingsReport::Unreachable(_)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn counters_are_visible_in_the_status() {
        let r = start(cfg());
        r.tx.send(liq(1)).await.unwrap();
        assert_eq!(r.status.snapshot().state, StoreState::Unknown);
        sleep(Duration::from_millis(100)).await;
        let s = r.status.snapshot();
        assert_eq!((s.written, s.batches, s.retries), (1, 1, 0));
        assert_eq!(s.state, StoreState::Connected);
        assert!(s.last_error.is_none());
    }
}
