//! Server configuration: TOML file plus environment overrides.
//!
//! The file is parsed into a generic TOML table first, then every environment variable of the form
//! `VULTURE__SECTION__KEY=value` (double underscore as separator, case-insensitive) is merged into it, and
//! only then is the result deserialized and validated. Array elements are addressed by index, for example
//! `VULTURE__EXCHANGES__0__ENABLED=false`. Secrets (`redis.url`) can come from the environment or from a
//! file (`redis.url_file`) and are wrapped in [`Secret`], which never prints its value.

mod env;
mod secret;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

pub use env::ENV_PREFIX;
pub use secret::Secret;

/// Errors of loading and validating the configuration. The messages are meant to be read by the operator.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The config file could not be read.
    #[error("cannot read config file {path}: {source}")]
    Read {
        /// Path as given on the command line.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// The file is not valid TOML or does not match the schema.
    #[error("invalid config: {0}")]
    Parse(String),
    /// An environment override could not be applied.
    #[error("invalid environment override {var}: {message}")]
    Env {
        /// Name of the variable.
        var: String,
        /// What is wrong with it.
        message: String,
    },
    /// The values are well-formed but not acceptable. Every problem is listed.
    #[error("config validation failed:\n  - {}", .0.join("\n  - "))]
    Invalid(Vec<String>),
    /// A secret file could not be read.
    #[error("cannot read secret file {path}: {source}")]
    SecretFile {
        /// Path of the secret file.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
}

/// Whole server configuration.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// gRPC listener and state directory.
    pub server: ServerConfig,
    /// TLS material; mandatory once the gRPC server exists (V1-8), optional until then.
    pub tls: Option<TlsConfig>,
    /// Redis Streams (hot window).
    pub redis: RedisConfig,
    /// Exchanges to collect from. The list is data, not code.
    pub exchanges: Vec<ExchangeConfig>,
    /// Prometheus endpoint.
    pub metrics: MetricsConfig,
    /// Log level and format.
    pub logging: LoggingConfig,
    /// Normalization data.
    pub normalize: NormalizeConfig,
}

/// `[server]` section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// gRPC listen address.
    pub listen: String,
    /// Directory for the token hash and service state.
    pub data_dir: PathBuf,
    /// Upper bound for graceful shutdown, seconds.
    pub shutdown_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8443".into(),
            data_dir: PathBuf::from("/var/lib/vulture"),
            shutdown_timeout_secs: 4,
        }
    }
}

/// `[tls]` section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Certificate chain (PEM).
    pub cert: PathBuf,
    /// Private key (PEM).
    pub key: PathBuf,
    /// CA for client certificates; enables mTLS.
    pub client_ca: Option<PathBuf>,
}

/// `[redis]` section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RedisConfig {
    /// Connection URL. May contain a password, hence [`Secret`].
    pub url: Option<Secret>,
    /// File holding the URL instead of `url` (for container secrets).
    pub url_file: Option<PathBuf>,
    /// Hot window length, hours.
    pub window_hours: u32,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: None,
            url_file: None,
            window_hours: 24,
        }
    }
}

impl RedisConfig {
    /// Resolves the Redis URL from `url` or `url_file`. Returns `None` if neither is set.
    pub fn resolve_url(&self) -> Result<Option<Secret>, ConfigError> {
        match (&self.url, &self.url_file) {
            (Some(url), _) => Ok(Some(url.clone())),
            (None, Some(path)) => Secret::from_file(path).map(Some),
            (None, None) => Ok(None),
        }
    }
}

/// One element of `[[exchanges]]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeConfig {
    /// Exchange name, lowercase (`binance`); also the `exchange` field of events.
    pub name: String,
    /// Disabled entries are kept in the file but not started.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Normalized symbols to collect (`BTC-USDT-PERP`); empty means every symbol the exchange offers.
    #[serde(default)]
    pub symbols: Vec<String>,
    /// Open interest polling or slicing interval, seconds.
    #[serde(default = "default_oi_interval")]
    pub oi_poll_interval_secs: u64,
}

fn default_true() -> bool {
    true
}

fn default_oi_interval() -> u64 {
    30
}

/// `[metrics]` section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetricsConfig {
    /// Prometheus listen address.
    pub listen: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:9090".into(),
        }
    }
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable lines.
    #[default]
    Text,
    /// One JSON object per line.
    Json,
}

/// `[logging]` section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LoggingConfig {
    /// `tracing` filter directive, for example `info` or `info,vulture_server::ws=debug`.
    pub level: String,
    /// Output format.
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            format: LogFormat::Text,
        }
    }
}

/// `[normalize]` section.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct NormalizeConfig {
    /// Replacement for the built-in base asset alias table (see `data/base_aliases.toml`).
    pub base_aliases_file: Option<PathBuf>,
}

impl Config {
    /// Loads the file and applies the process environment.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        Self::load_with_env(path, std::env::vars())
    }

    /// Loads the file and applies the given environment (injectable for tests).
    pub fn load_with_env(
        path: &Path,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_toml_with_env(&text, env)
    }

    /// Parses TOML text and applies the environment overrides.
    pub fn from_toml_with_env(
        text: &str,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ConfigError> {
        let mut table: toml::Table = text
            .parse()
            .map_err(|e: toml::de::Error| ConfigError::Parse(e.to_string()))?;
        env::apply_overrides(&mut table, env)?;
        let config: Config = table
            .try_into()
            .map_err(|e: toml::de::Error| ConfigError::Parse(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Semantic checks. Collects every problem instead of stopping at the first.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut problems = Vec::new();
        if self.server.listen.parse::<SocketAddr>().is_err() {
            problems.push(format!(
                "server.listen: {:?} is not a socket address (host:port)",
                self.server.listen
            ));
        }
        if self.metrics.listen.parse::<SocketAddr>().is_err() {
            problems.push(format!(
                "metrics.listen: {:?} is not a socket address (host:port)",
                self.metrics.listen
            ));
        }
        if self.server.listen == self.metrics.listen {
            problems.push("metrics.listen must differ from server.listen".into());
        }
        if self.server.shutdown_timeout_secs == 0 {
            problems.push("server.shutdown_timeout_secs must be greater than 0".into());
        }
        if self.redis.window_hours == 0 {
            problems.push("redis.window_hours must be greater than 0".into());
        }
        if self.redis.url.is_some() && self.redis.url_file.is_some() {
            problems.push("redis.url and redis.url_file are mutually exclusive".into());
        }
        if let Some(tls) = &self.tls
            && (tls.cert.as_os_str().is_empty() || tls.key.as_os_str().is_empty())
        {
            problems.push("tls.cert and tls.key must not be empty".into());
        }
        if tracing_subscriber::EnvFilter::try_new(&self.logging.level).is_err() {
            problems.push(format!(
                "logging.level: {:?} is not a valid filter directive",
                self.logging.level
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for (i, ex) in self.exchanges.iter().enumerate() {
            let valid_name = !ex.name.is_empty()
                && ex
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
            if !valid_name {
                problems.push(format!(
                    "exchanges[{i}].name: {:?} must be lowercase ASCII letters, digits or underscore",
                    ex.name
                ));
            } else if !seen.insert(ex.name.as_str()) {
                problems.push(format!("exchanges[{i}].name: duplicate {:?}", ex.name));
            }
            if ex.oi_poll_interval_secs == 0 {
                problems.push(format!(
                    "exchanges[{i}].oi_poll_interval_secs must be greater than 0"
                ));
            }
            for symbol in &ex.symbols {
                if !symbol.ends_with("-PERP") || symbol.matches('-').count() != 2 {
                    problems.push(format!(
                        "exchanges[{i}].symbols: {symbol:?} is not a normalized symbol (BASE-QUOTE-PERP)"
                    ));
                }
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Invalid(problems))
        }
    }

    /// Exchanges that must be started.
    pub fn enabled_exchanges(&self) -> impl Iterator<Item = &ExchangeConfig> {
        self.exchanges.iter().filter(|e| e.enabled)
    }
}

#[cfg(test)]
mod tests;
