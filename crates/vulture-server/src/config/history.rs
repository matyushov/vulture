//! `[history]`: limits of the `History` service (`Query` and `Aggregate`).

use serde::Deserialize;

/// `[history]` section. The service itself is always on; without `[clickhouse]` it answers from the Redis hot
/// window only.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HistoryApiConfig {
    /// Longest period of one `Query` call, seconds (`OUT_OF_RANGE` above it). `Aggregate` is bounded by
    /// `max_bins` instead.
    pub max_query_range_secs: u64,
    /// Events returned when the request says `limit = 0`.
    pub default_limit: u32,
    /// Most events one `Query` call may return: a `limit` above it is refused with `RESOURCE_EXHAUSTED`.
    pub max_rows: u32,
    /// Most bins per group of one `Aggregate` call (`OUT_OF_RANGE` above it).
    pub max_bins: u32,
    /// Most rows (groups times bins) of one `Aggregate` answer (`RESOURCE_EXHAUSTED` above it).
    pub max_aggregate_rows: u32,
    /// Most (exchange, symbol, bin) cells one `Aggregate` call reads from ClickHouse before it rolls them up.
    pub max_fine_rows: u32,
    /// Most Redis stream entries one call scans for the unflushed tail (or for the whole answer when ClickHouse is
    /// not configured).
    pub tail_max_entries: u32,
    /// Entries per Redis read while scanning the tail.
    pub tail_page: u32,
    /// Deadline of a whole call, seconds; a client deadline can only shorten it.
    pub request_timeout_secs: u64,
    /// Simultaneous History calls; the next one gets `RESOURCE_EXHAUSTED`.
    pub max_concurrent: u32,
    /// Memory ClickHouse may use for one History query, bytes (`max_memory_usage` of the query).
    pub clickhouse_max_memory_bytes: u64,
    /// Entries per list of the OI movers ranking when the request says 0.
    pub movers_default_limit: u32,
    /// Largest `limit` of the OI movers ranking.
    pub movers_max_limit: u32,
    /// A series whose newest sample is older than this (relative to the end of the window) is not a mover, seconds.
    pub movers_max_stale_secs: u64,
    /// Events read from Redis for `Query` are matched to a period by their timestamp; the stream ID is the write
    /// time, so the scan starts this many seconds before `from_ts` and stops this many after `to_ts`.
    pub redis_slack_secs: u64,
}

impl Default for HistoryApiConfig {
    fn default() -> Self {
        Self {
            max_query_range_secs: 31 * 86_400,
            default_limit: 10_000,
            max_rows: 1_000_000,
            max_bins: 5_000,
            max_aggregate_rows: 200_000,
            max_fine_rows: 3_000_000,
            tail_max_entries: 2_000_000,
            tail_page: 5_000,
            request_timeout_secs: 30,
            max_concurrent: 8,
            clickhouse_max_memory_bytes: 2 * 1024 * 1024 * 1024,
            movers_default_limit: 20,
            movers_max_limit: 200,
            movers_max_stale_secs: 900,
            redis_slack_secs: 300,
        }
    }
}

impl HistoryApiConfig {
    /// Adds every problem to `problems`.
    pub fn validate(&self, problems: &mut Vec<String>) {
        for (name, ok) in [
            ("max_query_range_secs", self.max_query_range_secs > 0),
            ("default_limit", self.default_limit > 0),
            ("max_rows", self.max_rows > 0),
            ("max_bins", self.max_bins > 0),
            ("max_aggregate_rows", self.max_aggregate_rows > 0),
            ("max_fine_rows", self.max_fine_rows > 0),
            ("tail_max_entries", self.tail_max_entries > 0),
            ("tail_page", self.tail_page > 0),
            ("request_timeout_secs", self.request_timeout_secs > 0),
            ("max_concurrent", self.max_concurrent > 0),
            (
                "clickhouse_max_memory_bytes",
                self.clickhouse_max_memory_bytes > 0,
            ),
            ("movers_default_limit", self.movers_default_limit > 0),
            ("movers_max_limit", self.movers_max_limit > 0),
            ("movers_max_stale_secs", self.movers_max_stale_secs > 0),
        ] {
            if !ok {
                problems.push(format!("history.{name} must be greater than 0"));
            }
        }
        if self.default_limit > self.max_rows {
            problems.push("history.default_limit must not exceed history.max_rows".into());
        }
        if self.movers_default_limit > self.movers_max_limit {
            problems.push(
                "history.movers_default_limit must not exceed history.movers_max_limit".into(),
            );
        }
    }
}
