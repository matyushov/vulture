//! Counters of the gRPC layer, for the metrics endpoint (V1-9).

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Live counters; shared as `Arc<GrpcStats>`.
#[derive(Debug, Default)]
pub struct GrpcStats {
    /// Clients subscribed right now (a gauge).
    pub connected_clients: AtomicUsize,
    /// `Subscribe` calls accepted since start.
    pub subscriptions_total: AtomicU64,
    /// Events handed to clients (queued for delivery).
    pub entries_sent: AtomicU64,
    /// Clients disconnected because their queue stayed full.
    pub slow_client_disconnects: AtomicU64,
    /// `Subscribe` calls that carried a `resume_from`.
    pub resume_requests: AtomicU64,
    /// `Subscribe` calls (or running replays) answered with `OUT_OF_RANGE`.
    pub out_of_range: AtomicU64,
    /// `Subscribe` calls refused because `max_clients` was reached.
    pub rejected_max_clients: AtomicU64,
    /// Failed Redis reads while streaming (each is retried).
    pub redis_read_errors: AtomicU64,
}

/// A copy of the counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GrpcSnapshot {
    /// See [`GrpcStats::connected_clients`].
    pub connected_clients: usize,
    /// See [`GrpcStats::subscriptions_total`].
    pub subscriptions_total: u64,
    /// See [`GrpcStats::entries_sent`].
    pub entries_sent: u64,
    /// See [`GrpcStats::slow_client_disconnects`].
    pub slow_client_disconnects: u64,
    /// See [`GrpcStats::resume_requests`].
    pub resume_requests: u64,
    /// See [`GrpcStats::out_of_range`].
    pub out_of_range: u64,
    /// See [`GrpcStats::rejected_max_clients`].
    pub rejected_max_clients: u64,
    /// See [`GrpcStats::redis_read_errors`].
    pub redis_read_errors: u64,
}

impl GrpcStats {
    /// Reads all counters.
    pub fn snapshot(&self) -> GrpcSnapshot {
        GrpcSnapshot {
            connected_clients: self.connected_clients.load(Ordering::Relaxed),
            subscriptions_total: self.subscriptions_total.load(Ordering::Relaxed),
            entries_sent: self.entries_sent.load(Ordering::Relaxed),
            slow_client_disconnects: self.slow_client_disconnects.load(Ordering::Relaxed),
            resume_requests: self.resume_requests.load(Ordering::Relaxed),
            out_of_range: self.out_of_range.load(Ordering::Relaxed),
            rejected_max_clients: self.rejected_max_clients.load(Ordering::Relaxed),
            redis_read_errors: self.redis_read_errors.load(Ordering::Relaxed),
        }
    }
}
