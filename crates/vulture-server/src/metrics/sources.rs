//! Metric sources for the components that live outside the connectors: the Redis writer, the gRPC layer, the
//! interceptor and the periodic Redis stream sampler. Connector metrics are reported by the connectors
//! ([`crate::exchange::Exchange::collect_metrics`]) and TLS metrics by [`crate::tls::TlsReloader`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::{Families, MetricSource, unix_now_ms};
use crate::auth::AuthStats;
use crate::grpc::GrpcStats;
use crate::store::{RedisStore, StoreState, WriterStatus, parse_id};

/// The Redis writer's counters.
pub struct WriterMetrics(pub Arc<WriterStatus>);

impl MetricSource for WriterMetrics {
    fn collect(&self, out: &mut Families) {
        let s = self.0.snapshot();
        out.counter(
            "vulture_redis_written_total",
            "Entries written to the Redis stream.",
            &[],
            s.written,
        );
        out.counter(
            "vulture_redis_batches_total",
            "Successful write pipelines.",
            &[],
            s.batches,
        );
        out.counter(
            "vulture_redis_write_retries_total",
            "Failed write attempts (each is retried).",
            &[],
            s.retries,
        );
        out.gauge(
            "vulture_redis_buffered_events",
            "Events waiting in memory for Redis.",
            &[],
            s.buffered as f64,
        );
        out.counter(
            "vulture_redis_dropped_total",
            "Events dropped because the in-memory buffer was full.",
            &[],
            s.dropped,
        );
        out.counter(
            "vulture_redis_gaps_written_total",
            "Gap events written to describe dropped events.",
            &[],
            s.gaps_written,
        );
        if s.last_write_ms > 0 {
            out.gauge(
                "vulture_redis_last_write_age_seconds",
                "Seconds since the last successful Redis write (absent until the first one).",
                &[],
                ((unix_now_ms() - s.last_write_ms).max(0)) as f64 / 1000.0,
            );
        }
        for (name, state) in [
            ("unknown", StoreState::Unknown),
            ("connected", StoreState::Connected),
            ("disconnected", StoreState::Disconnected),
        ] {
            out.gauge(
                "vulture_redis_connection_state",
                "Redis connection state as seen by the writer; 1 for the current state.",
                &[("state", name)],
                f64::from(u8::from(s.state == state)),
            );
        }
    }
}

/// The gRPC stream service's counters.
pub struct GrpcMetrics(pub Arc<GrpcStats>);

impl MetricSource for GrpcMetrics {
    fn collect(&self, out: &mut Families) {
        let s = self.0.snapshot();
        out.gauge(
            "vulture_grpc_clients",
            "Clients subscribed right now.",
            &[],
            s.connected_clients as f64,
        );
        for (name, help, v) in [
            (
                "vulture_grpc_subscriptions_total",
                "Subscribe calls accepted.",
                s.subscriptions_total,
            ),
            (
                "vulture_grpc_entries_sent_total",
                "Events handed to clients.",
                s.entries_sent,
            ),
            (
                "vulture_grpc_slow_client_disconnects_total",
                "Clients disconnected because their queue stayed full.",
                s.slow_client_disconnects,
            ),
            (
                "vulture_grpc_resume_requests_total",
                "Subscribe calls that carried resume_from.",
                s.resume_requests,
            ),
            (
                "vulture_grpc_out_of_range_total",
                "Subscribe calls or replays answered with OUT_OF_RANGE.",
                s.out_of_range,
            ),
            (
                "vulture_grpc_rejected_max_clients_total",
                "Subscribe calls refused because max_clients was reached.",
                s.rejected_max_clients,
            ),
            (
                "vulture_grpc_redis_read_errors_total",
                "Failed Redis reads while streaming.",
                s.redis_read_errors,
            ),
        ] {
            out.counter(name, help, &[], v);
        }
    }
}

/// The interceptor's counters.
pub struct AuthMetrics(pub Arc<AuthStats>);

impl MetricSource for AuthMetrics {
    fn collect(&self, out: &mut Families) {
        out.counter(
            "vulture_auth_failures_total",
            "Requests rejected for a missing, malformed or wrong token.",
            &[],
            self.0.failures.load(Ordering::Relaxed),
        );
        out.counter(
            "vulture_auth_rate_limited_total",
            "Failed attempts refused because the peer had too many failures.",
            &[],
            self.0.rate_limited.load(Ordering::Relaxed),
        );
    }
}

/// Length and age of the Redis stream, sampled periodically (these need Redis round trips, so they are not
/// done at scrape time).
#[derive(Default)]
pub struct RedisSampler {
    sampled: AtomicBool,
    length: AtomicU64,
    oldest_ms: AtomicI64,
    newest_ms: AtomicI64,
    errors: AtomicU64,
}

impl RedisSampler {
    /// New sampler without a sample.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Takes one sample.
    pub async fn sample(&self, store: &dyn RedisStore) {
        let result = async {
            let len = store.stream_len().await?;
            let bounds = store.bounds().await?;
            Ok::<_, crate::store::StoreError>((len, bounds))
        }
        .await;
        match result {
            Ok((len, bounds)) => {
                let ms = |id: &Option<String>| {
                    id.as_deref()
                        .and_then(parse_id)
                        .map_or(0, |(ms, _)| ms as i64)
                };
                self.length.store(len, Ordering::Relaxed);
                self.oldest_ms.store(ms(&bounds.first), Ordering::Relaxed);
                self.newest_ms.store(ms(&bounds.last), Ordering::Relaxed);
                self.sampled.store(true, Ordering::Relaxed);
            }
            Err(e) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(error = %e, "cannot sample the Redis stream");
            }
        }
    }

    /// Samples every `interval` until `shutdown`.
    pub async fn run(
        self: Arc<Self>,
        store: Arc<dyn RedisStore>,
        interval: Duration,
        shutdown: CancellationToken,
    ) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => self.sample(store.as_ref()).await,
                _ = shutdown.cancelled() => return,
            }
        }
    }
}

impl MetricSource for RedisSampler {
    fn collect(&self, out: &mut Families) {
        out.counter(
            "vulture_redis_stream_sample_errors_total",
            "Failed samplings of the Redis stream length and bounds.",
            &[],
            self.errors.load(Ordering::Relaxed),
        );
        if !self.sampled.load(Ordering::Relaxed) {
            return;
        }
        out.gauge(
            "vulture_redis_stream_length",
            "Entries in the Redis stream (sampled).",
            &[],
            self.length.load(Ordering::Relaxed) as f64,
        );
        let now = unix_now_ms();
        for (name, help, ms) in [
            (
                "vulture_redis_stream_oldest_age_seconds",
                "Age of the oldest entry of the Redis stream (sampled).",
                self.oldest_ms.load(Ordering::Relaxed),
            ),
            (
                "vulture_redis_stream_newest_age_seconds",
                "Age of the newest entry of the Redis stream (sampled); a growing value means no events arrive.",
                self.newest_ms.load(Ordering::Relaxed),
            ),
        ] {
            if ms > 0 {
                out.gauge(name, help, &[], (now - ms).max(0) as f64 / 1000.0);
            }
        }
    }
}
