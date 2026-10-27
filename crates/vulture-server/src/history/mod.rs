//! History in ClickHouse (V3a): schema, migrations, the Redis-to-ClickHouse flusher and storage monitoring.
//!
//! - [`rows`]: stream entries to table rows and their `RowBinary` encoding.
//! - [`migrations`]: numbered SQL files embedded in the binary, applied idempotently.
//! - [`client`]: the small HTTP client and the [`HistorySink`] trait the flusher talks to.
//! - [`flusher`]: the loop that moves entries from the Redis stream to the tables.
//! - [`watermark`]: the shared flush position, the trim cutoff and the insert deduplication token.
//! - [`monitor`]: disk and table statistics from `system.*`, exported as gauges.
//! - [`status`]: the flusher's counters and its `vulture_flusher_*` metrics.
//!
//! The server runs the flusher only when `[clickhouse]` is configured and enabled. Without it nothing here is
//! active and the stream is trimmed by time alone.

pub mod client;
pub mod flusher;
pub mod migrations;
pub mod monitor;
pub mod rows;
pub mod status;
pub mod watermark;

pub use client::{ChClient, ChError, HistorySink, QuerySummary, ReadSettings, RowStream};
pub use flusher::{
    FlushError, Flusher, FlusherConfig, INFLIGHT_KEY, WATERMARK_KEY, estimate_backlog,
};
pub use migrations::{MIGRATIONS, Migration};
pub use monitor::ClickHouseMonitor;
pub use rows::{PageRows, Row, SkipReason, Table};
pub use status::{ChState, FlusherMetrics, FlusherStatus};
pub use watermark::{Watermark, dedup_token, next_id, trim_cutoff_ms};

use std::sync::Arc;

use crate::config::{ClickHouseConfig, ConfigError};

/// `true` for a non-empty ASCII identifier (letters, digits, underscore; not starting with a digit): the only
/// names interpolated into SQL.
pub fn is_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !name.as_bytes()[0].is_ascii_digit()
}

/// Everything the application needs to run the flusher next to the writer.
pub struct HistorySetup {
    /// The ClickHouse side.
    pub sink: Arc<dyn HistorySink>,
    /// Flusher tunables.
    pub flusher: FlusherConfig,
    /// Shared with the Redis writer.
    pub watermark: Arc<Watermark>,
    /// Counters (also registered with the metrics hub).
    pub status: Arc<FlusherStatus>,
    /// Client for the monitor; `None` in tests with a fake sink.
    pub monitor: Option<(Arc<ChClient>, std::time::Duration)>,
}

/// Builds the real client from the configuration (resolves `password_file`).
pub fn build_client(cfg: &ClickHouseConfig) -> Result<ChClient, ConfigError> {
    let password = cfg.resolve_password()?;
    ChClient::new(cfg, password).map_err(|e| ConfigError::Invalid(vec![format!("clickhouse: {e}")]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers() {
        assert!(is_identifier("vulture"));
        assert!(is_identifier("vulture_test_2"));
        assert!(!is_identifier(""));
        assert!(!is_identifier("1abc"));
        assert!(!is_identifier("a-b"));
        assert!(!is_identifier("a;drop"));
        assert!(!is_identifier("é"));
    }
}
