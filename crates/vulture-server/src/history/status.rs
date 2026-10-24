//! Observable state of the flusher and its metrics source.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, Ordering};

use super::rows::Table;
use super::watermark::Watermark;
use crate::metrics::{Families, MetricSource, unix_now_ms};

/// Reachability of ClickHouse as last observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChState {
    /// Nothing observed yet.
    Unknown,
    /// The last request got an answer.
    Up,
    /// The last request failed to connect or timed out.
    Down,
}

/// Counters and gauges of the flusher. Relaxed ordering: they are statistics.
#[derive(Debug)]
pub struct FlusherStatus {
    /// Rows inserted, per table ([`Table::index`]).
    pub rows: [AtomicU64; 4],
    /// Successful inserts, per table.
    pub batches: [AtomicU64; 4],
    /// Failed attempts at the insert step (ClickHouse).
    pub failures_insert: AtomicU64,
    /// Failed attempts at reading or updating Redis.
    pub failures_redis: AtomicU64,
    /// Failed attempts at the migrations.
    pub failures_migrate: AtomicU64,
    /// Entries skipped because they could not be decoded.
    pub skipped_undecodable: AtomicU64,
    /// Entries skipped because they carried no payload.
    pub skipped_empty: AtomicU64,
    /// Times the flusher noticed that entries after the watermark were probably trimmed by `MAXLEN`.
    pub possible_loss: AtomicU64,
    /// Completed flush cycles.
    pub cycles: AtomicU64,
    /// Wall-clock ms of the last successful page; 0 if none yet.
    pub last_success_ms: AtomicI64,
    /// Wall-clock ms up to which the flusher has verified that every entry is in ClickHouse (the start of its
    /// last read that found nothing newer to flush).
    pub caught_up_ms: AtomicI64,
    /// Estimated entries after the watermark; valid when `backlog_known`.
    pub backlog_entries: AtomicU64,
    /// Whether `backlog_entries` has been estimated.
    pub backlog_known: AtomicBool,
    /// `redis.maxlen_safeguard`, for the headroom gauge.
    pub maxlen: AtomicU64,
    state: AtomicU8,
    watermark: Arc<Watermark>,
}

impl FlusherStatus {
    /// New status; `started_ms` is the process start (the initial "caught up" point).
    pub fn new(watermark: Arc<Watermark>, maxlen: u64, started_ms: i64) -> Self {
        Self {
            rows: Default::default(),
            batches: Default::default(),
            failures_insert: AtomicU64::new(0),
            failures_redis: AtomicU64::new(0),
            failures_migrate: AtomicU64::new(0),
            skipped_undecodable: AtomicU64::new(0),
            skipped_empty: AtomicU64::new(0),
            possible_loss: AtomicU64::new(0),
            cycles: AtomicU64::new(0),
            last_success_ms: AtomicI64::new(0),
            caught_up_ms: AtomicI64::new(started_ms),
            backlog_entries: AtomicU64::new(0),
            backlog_known: AtomicBool::new(false),
            maxlen: AtomicU64::new(maxlen),
            state: AtomicU8::new(0),
            watermark,
        }
    }

    /// Records what was observed about ClickHouse.
    pub fn set_ch_state(&self, state: ChState) {
        let v = match state {
            ChState::Unknown => 0,
            ChState::Up => 1,
            ChState::Down => 2,
        };
        self.state.store(v, Ordering::Relaxed);
    }

    /// Reachability of ClickHouse.
    pub fn ch_state(&self) -> ChState {
        match self.state.load(Ordering::Relaxed) {
            1 => ChState::Up,
            2 => ChState::Down,
            _ => ChState::Unknown,
        }
    }

    /// Total rows inserted into `table`.
    pub fn rows_of(&self, table: Table) -> u64 {
        self.rows[table.index()].load(Ordering::Relaxed)
    }

    /// Seconds the flusher is behind: how long ago it last verified that nothing unflushed was left. It grows
    /// to about one flush interval between cycles and keeps growing while ClickHouse is unavailable.
    pub fn lag_seconds(&self, now_ms: i64) -> f64 {
        (now_ms - self.caught_up_ms.load(Ordering::Relaxed)).max(0) as f64 / 1000.0
    }
}

/// Metrics of the flusher (`vulture_flusher_*`).
pub struct FlusherMetrics(pub Arc<FlusherStatus>);

impl MetricSource for FlusherMetrics {
    fn collect(&self, out: &mut Families) {
        let s = &self.0;
        let now = unix_now_ms();
        for t in Table::ALL {
            out.counter(
                "vulture_flusher_rows_inserted_total",
                "Rows inserted into ClickHouse, per table.",
                &[("table", t.name())],
                s.rows[t.index()].load(Ordering::Relaxed),
            );
            out.counter(
                "vulture_flusher_batches_total",
                "Successful ClickHouse inserts, per table.",
                &[("table", t.name())],
                s.batches[t.index()].load(Ordering::Relaxed),
            );
        }
        for (stage, v) in [
            ("insert", &s.failures_insert),
            ("redis", &s.failures_redis),
            ("migrate", &s.failures_migrate),
        ] {
            out.counter(
                "vulture_flusher_failures_total",
                "Failed flush attempts by stage (each is retried with backoff).",
                &[("stage", stage)],
                v.load(Ordering::Relaxed),
            );
        }
        for (reason, v) in [
            ("undecodable", &s.skipped_undecodable),
            ("empty", &s.skipped_empty),
        ] {
            out.counter(
                "vulture_flusher_skipped_entries_total",
                "Stream entries that produced no row.",
                &[("reason", reason)],
                v.load(Ordering::Relaxed),
            );
        }
        out.counter(
            "vulture_flusher_possible_loss_total",
            "Times entries after the watermark were probably trimmed by the MAXLEN safeguard before they were flushed.",
            &[],
            s.possible_loss.load(Ordering::Relaxed),
        );
        out.counter(
            "vulture_flusher_cycles_total",
            "Completed flush cycles.",
            &[],
            s.cycles.load(Ordering::Relaxed),
        );
        let last = s.last_success_ms.load(Ordering::Relaxed);
        if last > 0 {
            out.gauge(
                "vulture_flusher_last_success_age_seconds",
                "Seconds since the last page was flushed successfully (absent until the first one).",
                &[],
                (now - last).max(0) as f64 / 1000.0,
            );
        }
        out.gauge(
            "vulture_flusher_watermark_lag_seconds",
            "How long ago the flusher last verified that every stream entry is in ClickHouse.",
            &[],
            s.lag_seconds(now),
        );
        let wm = s.watermark.ms();
        if wm > 0 {
            out.gauge(
                "vulture_flusher_watermark_timestamp_seconds",
                "Time part of the watermark (the last flushed entry ID), seconds since the Unix epoch.",
                &[],
                wm as f64 / 1000.0,
            );
        }
        if s.backlog_known.load(Ordering::Relaxed) {
            let backlog = s.backlog_entries.load(Ordering::Relaxed);
            out.gauge(
                "vulture_flusher_backlog_entries",
                "Estimated stream entries after the watermark (not yet in ClickHouse).",
                &[],
                backlog as f64,
            );
            let maxlen = s.maxlen.load(Ordering::Relaxed).max(1);
            out.gauge(
                "vulture_flusher_backlog_maxlen_ratio",
                "Estimated backlog divided by redis.maxlen_safeguard; near 1 the MAXLEN trim starts to drop unflushed entries.",
                &[],
                backlog as f64 / maxlen as f64,
            );
        }
        for (name, state) in [
            ("unknown", ChState::Unknown),
            ("up", ChState::Up),
            ("down", ChState::Down),
        ] {
            out.gauge(
                "vulture_flusher_clickhouse_state",
                "ClickHouse reachability as seen by the flusher; 1 for the current state.",
                &[("state", name)],
                f64::from(u8::from(s.ch_state() == state)),
            );
        }
        out.gauge(
            "vulture_flusher_clickhouse_up",
            "1 when the last request to ClickHouse got an answer, 0 when it failed to connect (absent until known).",
            &[],
            match s.ch_state() {
                ChState::Up => 1.0,
                ChState::Down => 0.0,
                ChState::Unknown => return,
            },
        );
    }
}
