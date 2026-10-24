//! The flusher: Redis stream in, ClickHouse tables out, without loss and without duplicates.
//!
//! # Protocol
//!
//! State in Redis (next to the stream): `<stream_key>:flusher:watermark` (the last flushed entry ID) and
//! `<stream_key>:flusher:inflight` (`<first> <last>`, the page being inserted). One page is:
//!
//! 1. read up to `batch_max_rows` entries after the watermark;
//! 2. write `inflight = first last` (a write-ahead record of the exact range);
//! 3. insert the rows of each table with the token derived from `(table, first, last)`;
//! 4. in one `MULTI`/`EXEC` set `watermark = last` and delete `inflight`.
//!
//! A crash anywhere between 2 and 4 leaves `inflight` behind. After the restart the same range is read again
//! (entries are only appended, and nothing unflushed is trimmed by time) and inserted with the same tokens, which
//! ClickHouse deduplicates. A page is therefore delivered at least once and stored exactly once. The range is
//! recorded because a plain retry would read a *longer* page when new entries arrived meanwhile, and that block
//! would carry a different token.
//!
//! # Failures
//!
//! Any error aborts the cycle; the watermark stays, so Redis is not trimmed past it, and the next attempt comes
//! after an exponential backoff with jitter (1 s to 60 s). Connectors and the gRPC stream do not depend on the
//! flusher. After recovery the backlog is flushed page by page with a short pause between pages.
//!
//! The one place data can be lost is the `MAXLEN` safeguard of the writer: if the backlog outgrows it, the oldest
//! unflushed entries are trimmed. `vulture_flusher_backlog_maxlen_ratio` warns long before that and
//! `vulture_flusher_possible_loss_total` counts the cases where it happened.
//!
//! Only one flusher may work on a stream: two server processes sharing one Redis would race for the watermark.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::time::{Instant, sleep, sleep_until, timeout_at};
use tokio_util::sync::CancellationToken;

use super::client::{ChError, HistorySink};
use super::rows::{PageRows, Table};
use super::status::{ChState, FlusherStatus};
use super::watermark::{Watermark, dedup_token, next_id};
use crate::config::{ClickHouseConfig, RedisConfig};
use crate::store::{RedisStore, StoreError, parse_id};
use crate::ws::{Backoff, Clock};

/// State key (suffix of the stream key) of the watermark.
pub const WATERMARK_KEY: &str = "flusher:watermark";
/// State key of the page in flight.
pub const INFLIGHT_KEY: &str = "flusher:inflight";

/// Tunables of the flusher.
#[derive(Debug, Clone)]
pub struct FlusherConfig {
    /// Pause between cycles after a success.
    pub interval: Duration,
    /// Entries per page.
    pub batch_max_rows: usize,
    /// First retry delay.
    pub retry_min: Duration,
    /// Longest retry delay.
    pub retry_max: Duration,
    /// Pause between pages of one cycle, so that catching up does not hog Redis or ClickHouse.
    pub page_pause: Duration,
    /// Period of the statistics refresh (backlog estimate, ClickHouse ping).
    pub stats_interval: Duration,
    /// Budget of the final flush at shutdown.
    pub shutdown_timeout: Duration,
}

impl FlusherConfig {
    /// Takes the flusher-relevant fields of the configuration.
    pub fn from_config(ch: &ClickHouseConfig, shutdown_timeout: Duration) -> Self {
        Self {
            interval: Duration::from_secs(ch.flush_interval_secs),
            batch_max_rows: ch.batch_max_rows,
            retry_min: Duration::from_secs(1),
            retry_max: Duration::from_secs(60),
            page_pause: Duration::from_millis(50),
            stats_interval: Duration::from_secs(15),
            shutdown_timeout,
        }
    }
}

/// Why a flush attempt failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FlushError {
    /// Reading or updating Redis failed.
    #[error("redis: {0}")]
    Redis(StoreError),
    /// ClickHouse failed.
    #[error("clickhouse: {0}")]
    ClickHouse(ChError),
    /// The persisted state is unusable.
    #[error("state: {0}")]
    State(String),
    /// The deadline of a best-effort final flush passed.
    #[error("deadline reached")]
    Deadline,
}

/// Estimates the entries after the watermark from the stream length and the time span of its IDs (Redis cannot
/// count by ID cheaply). All IDs in milliseconds.
pub fn estimate_backlog(len: u64, first_ms: u64, last_ms: u64, watermark_ms: u64) -> u64 {
    if len == 0 || watermark_ms >= last_ms {
        return 0;
    }
    if watermark_ms < first_ms || last_ms == first_ms {
        return len;
    }
    let fraction = (last_ms - watermark_ms) as f64 / (last_ms - first_ms) as f64;
    ((len as f64) * fraction).round() as u64
}

/// The flusher. Build with [`Flusher::new`], then [`Flusher::run`] it.
pub struct Flusher {
    store: Arc<dyn RedisStore>,
    sink: Arc<dyn HistorySink>,
    cfg: FlusherConfig,
    watermark: Arc<Watermark>,
    status: Arc<FlusherStatus>,
    clock: Arc<dyn Clock>,
    loaded: AtomicBool,
    migrated: AtomicBool,
    inflight: Mutex<Option<(String, String)>>,
}

impl Flusher {
    /// New flusher; `watermark` is shared with the Redis writer.
    pub fn new(
        store: Arc<dyn RedisStore>,
        sink: Arc<dyn HistorySink>,
        cfg: FlusherConfig,
        watermark: Arc<Watermark>,
        status: Arc<FlusherStatus>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            store,
            sink,
            cfg,
            watermark,
            status,
            clock,
            loaded: AtomicBool::new(false),
            migrated: AtomicBool::new(false),
            inflight: Mutex::new(None),
        }
    }

    /// Counters and gauges.
    pub fn status(&self) -> Arc<FlusherStatus> {
        self.status.clone()
    }

    fn inflight(&self) -> Option<(String, String)> {
        self.inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn set_inflight(&self, v: Option<(String, String)>) {
        *self.inflight.lock().unwrap_or_else(|e| e.into_inner()) = v;
    }

    /// Runs until `stop` fires, then flushes once more (best effort, at most `shutdown_timeout`). Cancel `stop`
    /// only after the Redis writer has written its last events, or they miss that final flush.
    pub async fn run(self, stop: CancellationToken) {
        let mut backoff = Backoff::new(self.cfg.retry_min, self.cfg.retry_max, None);
        let mut next = Instant::now();
        let mut stats = tokio::time::interval(self.cfg.stats_interval);
        stats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut failing = false;
        tracing::info!(
            interval_secs = self.cfg.interval.as_secs(),
            batch_max_rows = self.cfg.batch_max_rows,
            "ClickHouse flusher started"
        );
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = sleep_until(next) => {
                    match self.cycle(None, Some(&stop)).await {
                        Ok(rows) => {
                            if failing {
                                tracing::info!("ClickHouse flusher recovered");
                                failing = false;
                            }
                            backoff.reset();
                            next = Instant::now() + self.cfg.interval;
                            tracing::info!(rows, "flush cycle done");
                        }
                        Err(e) => {
                            let delay = backoff.record_failure();
                            next = Instant::now() + delay;
                            if failing {
                                tracing::debug!(error = %e, ?delay, "flush failed again");
                            } else {
                                failing = true;
                                tracing::warn!(error = %e, ?delay, "flush failed; the watermark stays and Redis keeps the data");
                            }
                        }
                    }
                }
                _ = stats.tick() => self.update_stats().await,
            }
        }
        self.final_flush().await;
    }

    /// One last cycle with a deadline; errors are logged, not retried (the next start continues).
    pub async fn final_flush(&self) {
        let deadline = Instant::now() + self.cfg.shutdown_timeout;
        match self.cycle(Some(deadline), None).await {
            Ok(rows) => tracing::info!(rows, "final flush done"),
            Err(e) => tracing::warn!(
                error = %e,
                "final flush incomplete; the rest stays in Redis and is flushed after the next start"
            ),
        }
    }

    async fn within<T>(
        &self,
        deadline: Option<Instant>,
        fut: impl std::future::Future<Output = T>,
    ) -> Result<T, FlushError> {
        match deadline {
            Some(d) => timeout_at(d, fut).await.map_err(|_| FlushError::Deadline),
            None => Ok(fut.await),
        }
    }

    fn redis_err(&self, e: StoreError) -> FlushError {
        self.status.failures_redis.fetch_add(1, Ordering::Relaxed);
        FlushError::Redis(e)
    }

    fn ch_err(&self, stage: &AtomicCounter, e: ChError) -> FlushError {
        match stage {
            AtomicCounter::Insert => &self.status.failures_insert,
            AtomicCounter::Migrate => &self.status.failures_migrate,
        }
        .fetch_add(1, Ordering::Relaxed);
        if matches!(e, ChError::Connection(_)) {
            self.status.set_ch_state(ChState::Down);
        } else {
            self.status.set_ch_state(ChState::Up);
        }
        FlushError::ClickHouse(e)
    }

    /// Loads the persisted watermark and in-flight record once per process.
    async fn load_state(&self, deadline: Option<Instant>) -> Result<(), FlushError> {
        if self.loaded.load(Ordering::SeqCst) {
            return Ok(());
        }
        let wm = self
            .within(deadline, self.store.state_get(WATERMARK_KEY))
            .await?
            .map_err(|e| self.redis_err(e))?;
        let inflight = self
            .within(deadline, self.store.state_get(INFLIGHT_KEY))
            .await?
            .map_err(|e| self.redis_err(e))?;
        if let Some(v) = &wm {
            if parse_id(v).is_none() {
                return Err(FlushError::State(format!(
                    "the stored watermark {v:?} is not a stream ID"
                )));
            }
            self.watermark.set(v);
        }
        if let Some(v) = inflight {
            let parsed = v
                .split_once(' ')
                .filter(|(a, b)| parse_id(a).is_some() && parse_id(b).is_some());
            match parsed {
                Some((a, b)) => self.set_inflight(Some((a.to_string(), b.to_string()))),
                None => {
                    return Err(FlushError::State(format!(
                        "the stored in-flight record {v:?} is malformed"
                    )));
                }
            }
        }
        tracing::info!(
            watermark = %self.watermark.id(),
            inflight = self.inflight().is_some(),
            "flusher state loaded"
        );
        self.loaded.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn ensure_migrated(&self, deadline: Option<Instant>) -> Result<(), FlushError> {
        if self.migrated.load(Ordering::SeqCst) {
            return Ok(());
        }
        let applied = self
            .within(deadline, self.sink.migrate())
            .await?
            .map_err(|e| self.ch_err(&AtomicCounter::Migrate, e))?;
        self.status.set_ch_state(ChState::Up);
        if !applied.is_empty() {
            tracing::info!(?applied, "ClickHouse schema migrated");
        }
        self.migrated.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Flushes everything after the watermark, page by page. Returns the rows inserted. With `stop`, a cancelled
    /// token ends the cycle early between pages (the final flush follows). With `deadline`, nothing runs past it.
    pub async fn cycle(
        &self,
        deadline: Option<Instant>,
        stop: Option<&CancellationToken>,
    ) -> Result<u64, FlushError> {
        self.load_state(deadline).await?;
        self.ensure_migrated(deadline).await?;
        self.detect_loss(deadline).await?;
        let mut total = 0;
        loop {
            if stop.is_some_and(CancellationToken::is_cancelled) {
                return Ok(total);
            }
            let read_started_ms = self.clock.now_ms();
            let (first, last, entries, full) = match self.inflight() {
                Some((first, last)) => {
                    let entries = self
                        .within(deadline, self.store.read_range(&first, &last, None))
                        .await?
                        .map_err(|e| self.redis_err(e))?;
                    (first, last, entries, true)
                }
                None => {
                    let start = next_id(&self.watermark.id()).ok_or_else(|| {
                        FlushError::State("the watermark has no successor".into())
                    })?;
                    let entries = self
                        .within(
                            deadline,
                            self.store
                                .read_range(&start, "+", Some(self.cfg.batch_max_rows)),
                        )
                        .await?
                        .map_err(|e| self.redis_err(e))?;
                    let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
                        self.status
                            .caught_up_ms
                            .store(read_started_ms, Ordering::Relaxed);
                        self.status.cycles.fetch_add(1, Ordering::Relaxed);
                        return Ok(total);
                    };
                    let (first, last) = (first.id.clone(), last.id.clone());
                    let full = entries.len() >= self.cfg.batch_max_rows;
                    // Write-ahead: remember the exact range before anything is inserted.
                    let record = format!("{first} {last}");
                    self.within(
                        deadline,
                        self.store
                            .state_apply(&[(INFLIGHT_KEY, Some(record.as_str()))]),
                    )
                    .await?
                    .map_err(|e| self.redis_err(e))?;
                    self.set_inflight(Some((first.clone(), last.clone())));
                    (first, last, entries, full)
                }
            };

            let page = PageRows::build(entries);
            for (id, reason) in &page.skipped {
                tracing::error!(entry = %id, reason = reason.label(), "stream entry skipped: it produced no row");
                match reason {
                    super::rows::SkipReason::Undecodable => &self.status.skipped_undecodable,
                    super::rows::SkipReason::Empty => &self.status.skipped_empty,
                }
                .fetch_add(1, Ordering::Relaxed);
            }
            for table in Table::ALL {
                let batch = &page.tables[table.index()];
                if batch.rows == 0 {
                    continue;
                }
                let token = dedup_token(table, &first, &last);
                self.within(
                    deadline,
                    self.sink.insert(table, batch.body.clone(), &token),
                )
                .await?
                .map_err(|e| self.ch_err(&AtomicCounter::Insert, e))?;
                self.status.set_ch_state(ChState::Up);
                self.status.rows[table.index()].fetch_add(batch.rows, Ordering::Relaxed);
                self.status.batches[table.index()].fetch_add(1, Ordering::Relaxed);
            }
            // All tables of the range are acknowledged: advance the watermark and drop the record atomically.
            self.within(
                deadline,
                self.store
                    .state_apply(&[(WATERMARK_KEY, Some(last.as_str())), (INFLIGHT_KEY, None)]),
            )
            .await?
            .map_err(|e| self.redis_err(e))?;
            self.watermark.set(&last);
            self.set_inflight(None);
            total += page.total_rows();
            self.status
                .last_success_ms
                .store(self.clock.now_ms(), Ordering::Relaxed);
            if !full {
                // A short page reached the end of the stream as of the read.
                self.status
                    .caught_up_ms
                    .store(read_started_ms, Ordering::Relaxed);
                self.status.cycles.fetch_add(1, Ordering::Relaxed);
                return Ok(total);
            }
            sleep(self.cfg.page_pause).await;
        }
    }

    /// The oldest retained entry being newer than the watermark means the entry at the watermark is gone, which
    /// only the MAXLEN safeguard can do: unflushed entries were probably trimmed.
    async fn detect_loss(&self, deadline: Option<Instant>) -> Result<(), FlushError> {
        let wm = self.watermark.id();
        if parse_id(&wm) == Some((0, 0)) {
            return Ok(());
        }
        let bounds = self
            .within(deadline, self.store.bounds())
            .await?
            .map_err(|e| self.redis_err(e))?;
        if let (Some(first), Some(w)) = (bounds.first.as_deref().and_then(parse_id), parse_id(&wm))
            && first > w
        {
            self.status.possible_loss.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                watermark = %wm,
                oldest = ?bounds.first,
                "the oldest entry in Redis is newer than the watermark: unflushed entries were probably trimmed by the MAXLEN safeguard"
            );
            // Do not report it again every cycle: move the baseline to what Redis still holds.
            if let Some(f) = &bounds.first
                && self.inflight().is_none()
                && let Some(prev) = prev_id(f)
            {
                self.watermark.set(&prev);
            }
        }
        Ok(())
    }

    /// Refreshes the backlog estimate and pings ClickHouse. Never fails.
    pub async fn update_stats(&self) {
        match self.sink.ping().await {
            Ok(()) => self.status.set_ch_state(ChState::Up),
            Err(ChError::Connection(_)) => self.status.set_ch_state(ChState::Down),
            Err(_) => self.status.set_ch_state(ChState::Up),
        }
        let (len, bounds) = match (self.store.stream_len().await, self.store.bounds().await) {
            (Ok(l), Ok(b)) => (l, b),
            _ => return,
        };
        let ms = |id: &Option<String>| id.as_deref().and_then(parse_id).map_or(0, |(ms, _)| ms);
        let backlog = estimate_backlog(
            len,
            ms(&bounds.first),
            ms(&bounds.last),
            self.watermark.ms(),
        );
        self.status
            .backlog_entries
            .store(backlog, Ordering::Relaxed);
        self.status.backlog_known.store(true, Ordering::Relaxed);
    }
}

/// The step of a ClickHouse failure, for the right counter.
enum AtomicCounter {
    Insert,
    Migrate,
}

/// The largest ID smaller than `id`, or `None` for `0-0`.
fn prev_id(id: &str) -> Option<String> {
    let (ms, seq) = parse_id(id)?;
    match (seq.checked_sub(1), ms.checked_sub(1)) {
        (Some(s), _) => Some(format!("{ms}-{s}")),
        (None, Some(m)) => Some(format!("{m}-{}", u64::MAX)),
        (None, None) => None,
    }
}

/// Convenience for the application: the Redis settings the flusher needs.
pub fn maxlen_of(redis: &RedisConfig) -> u64 {
    redis.maxlen_safeguard
}

#[cfg(test)]
mod tests;
