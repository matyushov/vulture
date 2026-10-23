//! `tracing` initialization from the `[logging]` section.

use tracing_subscriber::EnvFilter;

use crate::config::{LogFormat, LoggingConfig};

/// Installs the global subscriber. Fails if the filter is invalid or a subscriber is already set.
pub fn init(cfg: &LoggingConfig) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(&cfg.level)
        .map_err(|e| anyhow::anyhow!("invalid logging.level {:?}: {e}", cfg.level))?;
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    let result = match cfg.format {
        LogFormat::Text => builder.try_init(),
        LogFormat::Json => builder.json().try_init(),
    };
    result.map_err(|e| anyhow::anyhow!("cannot install the log subscriber: {e}"))
}
