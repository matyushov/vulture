//! Prometheus metrics (V1-9).
//!
//! Nothing is registered up front. Every component keeps its own counters (atomics in the `*Status` /
//! `*Stats` structs); a [`MetricSource`] turns them into samples at scrape time, and the [`MetricsHub`] renders
//! all registered sources. Labels are low-cardinality on purpose (`exchange`, `type`, `state`, ...): never a
//! symbol, a peer address or a client id.
//!
//! The endpoint ([`serve`]) is a separate listener with `/metrics`, `/healthz` and `/readyz`. It is not
//! authenticated: it listens on loopback by default; exposing it needs a reverse proxy or an internal network.

mod families;
mod server;
mod sources;

use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub use families::Families;
pub use server::{MetricsError, Readiness, bind, healthcheck, serve};
pub use sources::{AuthMetrics, GrpcMetrics, RedisSampler, WriterMetrics};

/// Something that can report its counters.
pub trait MetricSource: Send + Sync {
    /// Adds the current samples to `out`.
    fn collect(&self, out: &mut Families);
}

/// All metric sources of the process.
pub struct MetricsHub {
    started: Instant,
    started_unix: f64,
    sources: Mutex<Vec<Arc<dyn MetricSource>>>,
}

impl Default for MetricsHub {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsHub {
    /// Hub with the process metrics only.
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            started_unix: unix_now_secs(),
            sources: Mutex::new(Vec::new()),
        }
    }

    /// Adds a source.
    pub fn register(&self, source: Arc<dyn MetricSource>) {
        self.sources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(source);
    }

    /// Collects every source.
    pub fn collect(&self) -> Families {
        let mut out = Families::new();
        out.gauge(
            "vulture_build_info",
            "Build information; the value is always 1.",
            &[("version", env!("CARGO_PKG_VERSION"))],
            1.0,
        );
        out.gauge(
            "vulture_process_uptime_seconds",
            "Seconds since the process started.",
            &[],
            self.started.elapsed().as_secs_f64(),
        );
        out.gauge(
            "vulture_process_start_time_seconds",
            "Start time of the process, seconds since the Unix epoch.",
            &[],
            self.started_unix,
        );
        let sources: Vec<_> = self
            .sources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        for source in sources {
            source.collect(&mut out);
        }
        out
    }

    /// Text exposition of every source.
    pub fn render(&self) -> String {
        self.collect().render()
    }
}

/// Seconds since the Unix epoch (wall clock).
pub fn unix_now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Milliseconds since the Unix epoch (wall clock).
pub fn unix_now_ms() -> i64 {
    (unix_now_secs() * 1000.0) as i64
}
