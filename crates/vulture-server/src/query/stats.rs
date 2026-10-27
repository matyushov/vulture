//! Counters of the `History` service for the metrics endpoint (`vulture_history_*`).

use std::sync::atomic::{AtomicU64, Ordering};

use tonic::Code;

use crate::metrics::{Families, MetricSource};

/// The RPCs of the service (and the movers ranking, which is an `Aggregate` mode but a different cost).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// `History.Query`.
    Query,
    /// `History.Aggregate` bins.
    Aggregate,
    /// `History.Aggregate` with `oi_movers`.
    Movers,
}

impl Method {
    const ALL: [Method; 3] = [Method::Query, Method::Aggregate, Method::Movers];

    fn idx(self) -> usize {
        self as usize
    }

    fn label(self) -> &'static str {
        match self {
            Method::Query => "query",
            Method::Aggregate => "aggregate",
            Method::Movers => "movers",
        }
    }
}

/// Result labels of `vulture_history_requests_total{code}`.
const CODES: [&str; 8] = [
    "ok",
    "invalid_argument",
    "out_of_range",
    "resource_exhausted",
    "unavailable",
    "deadline_exceeded",
    "cancelled",
    "internal",
];

fn code_index(code: Code) -> usize {
    match code {
        Code::Ok => 0,
        Code::InvalidArgument => 1,
        Code::OutOfRange => 2,
        Code::ResourceExhausted => 3,
        Code::Unavailable => 4,
        Code::DeadlineExceeded => 5,
        Code::Cancelled => 6,
        _ => 7,
    }
}

/// Why a request was refused before any work.
pub const REJECT_REASONS: [&str; 5] = ["range", "limit", "bins", "rows", "concurrency"];

/// A reason in [`REJECT_REASONS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reject {
    /// Period longer than `max_query_range_secs`.
    Range,
    /// `limit` above `max_rows`, or an unbounded scan.
    Limit,
    /// More bins than `max_bins`.
    Bins,
    /// More rows than the answer may have, or more scanned than allowed.
    Rows,
    /// `max_concurrent` calls already running.
    Concurrency,
}

/// Shared counters.
#[derive(Debug, Default)]
pub struct HistoryStats {
    requests: [[AtomicU64; 8]; 3],
    nanos: [AtomicU64; 3],
    nanos_max: [AtomicU64; 3],
    rows_scanned_ch: AtomicU64,
    rows_scanned_redis: AtomicU64,
    events_returned: AtomicU64,
    bins_returned: AtomicU64,
    movers_returned: AtomicU64,
    rejections: [AtomicU64; 5],
    in_flight: AtomicU64,
    clickhouse_errors: AtomicU64,
    redis_fallbacks: AtomicU64,
    tail_merges: AtomicU64,
}

impl HistoryStats {
    /// Records a finished request.
    pub fn finished(&self, m: Method, code: Code, nanos: u64) {
        self.requests[m.idx()][code_index(code)].fetch_add(1, Ordering::Relaxed);
        self.nanos[m.idx()].fetch_add(nanos, Ordering::Relaxed);
        self.nanos_max[m.idx()].fetch_max(nanos, Ordering::Relaxed);
    }

    /// Records a refusal.
    pub fn rejected(&self, r: Reject) {
        self.rejections[r as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Rows read from ClickHouse.
    pub fn scanned_clickhouse(&self, n: u64) {
        self.rows_scanned_ch.fetch_add(n, Ordering::Relaxed);
    }

    /// Entries read from Redis.
    pub fn scanned_redis(&self, n: u64) {
        self.rows_scanned_redis.fetch_add(n, Ordering::Relaxed);
    }

    /// Events sent by `Query`.
    pub fn events(&self, n: u64) {
        self.events_returned.fetch_add(n, Ordering::Relaxed);
    }

    /// Aggregate rows returned.
    pub fn bins(&self, n: u64) {
        self.bins_returned.fetch_add(n, Ordering::Relaxed);
    }

    /// Movers returned (both lists).
    pub fn movers(&self, n: u64) {
        self.movers_returned.fetch_add(n, Ordering::Relaxed);
    }

    /// A ClickHouse call failed.
    pub fn clickhouse_error(&self) {
        self.clickhouse_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// An answer was produced from Redis because ClickHouse failed.
    pub fn redis_fallback(&self) {
        self.redis_fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    /// An answer included the unflushed Redis tail next to ClickHouse data.
    pub fn tail_merge(&self) {
        self.tail_merges.fetch_add(1, Ordering::Relaxed);
    }

    /// A request started.
    pub fn enter(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    /// A request ended.
    pub fn leave(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Requests that ended with `code`, for tests.
    pub fn count(&self, m: Method, code: Code) -> u64 {
        self.requests[m.idx()][code_index(code)].load(Ordering::Relaxed)
    }

    /// Rejections for a reason, for tests.
    pub fn rejections(&self, r: Reject) -> u64 {
        self.rejections[r as usize].load(Ordering::Relaxed)
    }

    /// Redis fallbacks, for tests.
    pub fn fallbacks(&self) -> u64 {
        self.redis_fallbacks.load(Ordering::Relaxed)
    }

    /// Events returned, for tests.
    pub fn events_total(&self) -> u64 {
        self.events_returned.load(Ordering::Relaxed)
    }

    /// Bins returned, for tests.
    pub fn bins_total(&self) -> u64 {
        self.bins_returned.load(Ordering::Relaxed)
    }
}

impl MetricSource for HistoryStats {
    fn collect(&self, out: &mut Families) {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        for m in Method::ALL {
            for (i, code) in CODES.iter().enumerate() {
                out.counter(
                    "vulture_history_requests_total",
                    "History calls by method and result.",
                    &[("method", m.label()), ("code", code)],
                    g(&self.requests[m.idx()][i]),
                );
            }
            out.counter_f64(
                "vulture_history_request_seconds_total",
                "Time spent answering History calls, seconds (divide by the request count for the mean).",
                &[("method", m.label())],
                g(&self.nanos[m.idx()]) as f64 / 1e9,
            );
            out.gauge(
                "vulture_history_request_seconds_max",
                "Slowest History call since start, seconds.",
                &[("method", m.label())],
                g(&self.nanos_max[m.idx()]) as f64 / 1e9,
            );
        }
        out.counter(
            "vulture_history_rows_scanned_total",
            "Rows read to answer History calls, by store.",
            &[("source", "clickhouse")],
            g(&self.rows_scanned_ch),
        );
        out.counter(
            "vulture_history_rows_scanned_total",
            "Rows read to answer History calls, by store.",
            &[("source", "redis")],
            g(&self.rows_scanned_redis),
        );
        out.counter(
            "vulture_history_events_returned_total",
            "Events sent by History.Query.",
            &[],
            g(&self.events_returned),
        );
        out.counter(
            "vulture_history_bins_returned_total",
            "Aggregate rows (bins times groups) returned by History.Aggregate.",
            &[],
            g(&self.bins_returned),
        );
        out.counter(
            "vulture_history_movers_returned_total",
            "Entries of the OI movers lists returned.",
            &[],
            g(&self.movers_returned),
        );
        for (i, reason) in REJECT_REASONS.iter().enumerate() {
            out.counter(
                "vulture_history_rejections_total",
                "History calls refused for exceeding a limit.",
                &[("reason", reason)],
                g(&self.rejections[i]),
            );
        }
        out.gauge(
            "vulture_history_in_flight",
            "History calls running now.",
            &[],
            g(&self.in_flight) as f64,
        );
        out.counter(
            "vulture_history_clickhouse_errors_total",
            "ClickHouse calls of History that failed.",
            &[],
            g(&self.clickhouse_errors),
        );
        out.counter(
            "vulture_history_redis_fallbacks_total",
            "Answers produced from Redis because ClickHouse was unreachable.",
            &[],
            g(&self.redis_fallbacks),
        );
        out.counter(
            "vulture_history_tail_merges_total",
            "Answers that merged the unflushed Redis tail with ClickHouse data.",
            &[],
            g(&self.tail_merges),
        );
    }
}
