//! Counters and state of the Binance connector, readable from outside for the future metrics (V1-9).

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::sync::watch;

use crate::ws::{WsCounters, WsStatus};

/// All counters are monotonic unless stated otherwise. Relaxed ordering: they are statistics.
#[derive(Debug, Default)]
pub struct BinanceStatus {
    // --- WebSocket side ---
    /// Data frames taken from the WebSocket manager (liquidations and mark price).
    pub ws_frames: AtomicU64,
    /// Liquidation events published.
    pub liquidations: AtomicU64,
    /// Liquidation frames that could not be parsed.
    pub liquidation_parse_errors: AtomicU64,
    /// Liquidations of symbols outside the collected set (TradFi, settling, not whitelisted).
    pub liquidations_filtered: AtomicU64,
    /// Mark price frames applied.
    pub mark_frames: AtomicU64,
    /// Mark price frames or items that could not be parsed.
    pub mark_parse_errors: AtomicU64,
    /// Frames of an unknown kind.
    pub unknown_frames: AtomicU64,
    /// `Gap` events published.
    pub gaps: AtomicU64,

    // --- symbol list ---
    /// Successful `exchangeInfo` loads.
    pub info_loads: AtomicU64,
    /// Failed `exchangeInfo` loads.
    pub info_errors: AtomicU64,
    /// Symbols in the current set (gauge).
    pub symbols: AtomicU64,
    /// Symbols that share a normalized name with another symbol and were renamed (gauge of the last load).
    pub alias_collisions: AtomicU64,

    // --- REST open interest ---
    /// Requests sent (retries included).
    pub oi_requests: AtomicU64,
    /// Open interest events published.
    pub oi_events: AtomicU64,
    /// Symbols given up on in a cycle after all attempts failed.
    pub oi_failures: AtomicU64,
    /// Responses that could not be parsed or converted.
    pub oi_parse_errors: AtomicU64,
    /// Retries after a network error, timeout, 5xx or 429.
    pub oi_retries: AtomicU64,
    /// Timeouts and network errors.
    pub oi_network_errors: AtomicU64,
    /// HTTP 429 responses (all REST calls).
    pub http_429: AtomicU64,
    /// HTTP 418 responses (IP ban; all REST calls).
    pub http_418: AtomicU64,
    /// Snapshots without a usable mark price (`oi_usd` empty).
    pub oi_without_mark: AtomicU64,
    /// Last `X-MBX-USED-WEIGHT-1M` seen (gauge).
    pub used_weight_1m: AtomicU32,
    /// Whether REST is currently paused after a 418 (gauge).
    pub banned: AtomicBool,
    /// Wall-clock ms until which REST is paused by 429 or 418; 0 when not paused (gauge).
    pub paused_until_ms: AtomicI64,
    /// Current effective polling interval in milliseconds (gauge).
    pub oi_interval_ms: AtomicU64,
    /// `Gap` events published for REST pauses (418 bans and long 429 pauses).
    pub oi_gaps: AtomicU64,
    /// Completed polling cycles.
    pub oi_cycles: AtomicU64,

    /// Counters and state of the WebSocket manager, available once the connector runs.
    pub ws: OnceLock<WsLink>,
}

/// Link to the WebSocket manager's observable state.
#[derive(Debug, Clone)]
pub struct WsLink {
    /// Manager counters (duplicates, reconnects, gaps, overflow).
    pub counters: Arc<WsCounters>,
    /// Manager state.
    pub status: watch::Receiver<WsStatus>,
}

impl BinanceStatus {
    pub(crate) fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Reads a counter.
    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}
