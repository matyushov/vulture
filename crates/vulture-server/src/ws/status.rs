//! Observable state of a managed connection.

use std::sync::atomic::AtomicU64;

/// Connection state exposed to metrics and `Control`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connecting or waiting to retry.
    Connecting,
    /// A session is live: data of the liveness stream is flowing.
    Healthy,
    /// Escalated: 5 failed attempts in a row or a gap of 120 s (defaults). Retries continue forever.
    Degraded,
    /// Shut down.
    Stopped,
}

/// Snapshot published through a `watch` channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WsStatus {
    /// Current state.
    pub state: ConnectionState,
    /// Number of recoveries after a loss (not counting planned switches).
    pub reconnects: u64,
    /// Failed attempts since the last healthy period of `healthy_reset`.
    pub consecutive_failures: u32,
}

/// Monotonic counters, shared with metrics code.
#[derive(Debug, Default)]
pub struct WsCounters {
    /// Failed or lost sessions.
    pub failed_attempts: AtomicU64,
    /// Recoveries after a loss.
    pub reconnects: AtomicU64,
    /// Completed make-before-break switches.
    pub planned_reconnects: AtomicU64,
    /// Planned switches whose new session failed to come up.
    pub planned_failures: AtomicU64,
    /// `Gap` events published.
    pub gaps: AtomicU64,
    /// Frames dropped because the consumer channel was full.
    pub dropped_overflow: AtomicU64,
    /// Frames dropped as duplicates during a switch.
    pub duplicates: AtomicU64,
    /// Frames delivered to the consumer.
    pub frames_forwarded: AtomicU64,
    /// Entries into the `Degraded` state.
    pub degraded_entered: AtomicU64,
    /// Handshake rejections with 403, 429 or 418.
    pub handshake_bans: AtomicU64,
}
