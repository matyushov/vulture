//! Counters and state of the Bybit connector, readable from outside (metrics, tests).

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::ws::{ConnectionLimiter, ShardView};

/// All counters are monotonic unless stated otherwise. Relaxed ordering: they are statistics.
#[derive(Debug, Default)]
pub struct BybitStatus {
    /// Data frames taken from the WebSocket manager.
    pub ws_frames: AtomicU64,
    /// Liquidation events published.
    pub liquidations: AtomicU64,
    /// Liquidation records that could not be parsed.
    pub liquidation_parse_errors: AtomicU64,
    /// Liquidations of symbols outside the tracked set.
    pub liquidations_filtered: AtomicU64,
    /// Liquidation records dropped as duplicates.
    pub liquidations_duplicate: AtomicU64,
    /// Ticker frames applied to the state.
    pub ticker_frames: AtomicU64,
    /// Ticker frames that could not be parsed (or fields that were unusable).
    pub ticker_parse_errors: AtomicU64,
    /// Ticker frames of symbols outside the tracked set.
    pub tickers_filtered: AtomicU64,
    /// Frames of an unknown kind.
    pub unknown_frames: AtomicU64,
    /// `Gap` events published (liquidations and open interest streams together).
    pub gaps: AtomicU64,
    /// Open interest events published.
    pub oi_events: AtomicU64,
    /// Sampler slots skipped because the symbol had no open interest data or its connection was down.
    pub oi_skipped: AtomicU64,
    /// Samples without a mark price (`oi_usd` taken from the exchange value or empty).
    pub oi_without_mark: AtomicU64,
    /// Samples that could not be converted.
    pub oi_parse_errors: AtomicU64,
    /// Completed sampler cycles.
    pub oi_cycles: AtomicU64,
    /// Successful instrument list loads.
    pub info_loads: AtomicU64,
    /// Failed instrument list loads.
    pub info_errors: AtomicU64,
    /// HTTP 403 answers to REST calls (wait at least 10 minutes).
    pub http_403: AtomicU64,
    /// HTTP 429 answers to REST calls.
    pub http_429: AtomicU64,
    /// Symbols in the universe (gauge).
    pub symbols: AtomicU64,
    /// Symbols renamed because of an alias collision (gauge of the last load).
    pub alias_collisions: AtomicU64,
    /// Receive time (wall-clock ms) of the last ticker frame; 0 if none yet (gauge).
    pub last_ticker_ms: AtomicI64,
    /// Wall-clock ms until which REST is paused after a 403/429; 0 when not paused (gauge).
    pub paused_until_ms: AtomicI64,
    /// The WebSocket connections, available once the connector runs.
    pub shards: Mutex<Option<Arc<crate::ws::Shards>>>,
    /// The connection budget, available once the connector runs.
    pub limiter: OnceLock<Arc<ConnectionLimiter>>,
}

impl BybitStatus {
    pub(crate) fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Reads a counter.
    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// State of every WebSocket connection (empty until the connector runs).
    pub fn connections(&self) -> Vec<ShardView> {
        self.shards
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| s.views())
            .unwrap_or_default()
    }

    /// The tracked symbols the exchange refused, as `(topic, reason)`, over all connections.
    pub fn rejected_topics(&self) -> Vec<(String, String)> {
        self.shards
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| s.rejected())
            .unwrap_or_default()
    }

    /// `(permits granted, delayed, open)` of the connection budget.
    pub fn limiter_stats(&self) -> Option<(u64, u64, usize)> {
        self.limiter.get().map(|l| {
            (
                l.stats().granted.load(Ordering::Relaxed),
                l.stats().delayed.load(Ordering::Relaxed),
                l.open(),
            )
        })
    }

    /// Adds the connector's samples to a metrics scrape.
    pub fn collect_metrics(&self, out: &mut crate::metrics::Families) {
        super::metrics::collect(self, out);
    }
}
