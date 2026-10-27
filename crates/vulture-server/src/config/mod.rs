//! Server configuration: TOML file plus environment overrides.
//!
//! The file is parsed into a generic TOML table first, then every environment variable of the form
//! `VULTURE__SECTION__KEY=value` (double underscore as separator, case-insensitive) is merged into it, and
//! only then is the result deserialized and validated. Array elements are addressed by index, for example
//! `VULTURE__EXCHANGES__0__ENABLED=false`. Secrets (`redis.url`) can come from the environment or from a
//! file (`redis.url_file`) and are wrapped in [`Secret`], which never prints its value.

mod alerts;
mod env;
mod history;
mod secret;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

pub use alerts::{
    AlertsConfig, CascadeConfig, DIGEST_SECTIONS, DeliveryConfig, DigestConfig, OiSpikeConfig,
    OiWindow,
};
pub use env::ENV_PREFIX;
pub use history::HistoryApiConfig;
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
    /// TLS for the gRPC listener. When set, the listener serves TLS (HTTP/2 via ALPN) and any bind address is
    /// allowed; without it the listener is plaintext and loopback-only unless `server.allow_plaintext` is set.
    pub tls: Option<TlsConfig>,
    /// Redis Streams (hot window).
    pub redis: RedisConfig,
    /// ClickHouse history and the Redis-to-ClickHouse flusher. Without this section (or with `enabled = false`)
    /// the stream is trimmed by time only and no history is kept.
    pub clickhouse: Option<ClickHouseConfig>,
    /// Exchanges to collect from. The list is data, not code.
    pub exchanges: Vec<ExchangeConfig>,
    /// Prometheus endpoint.
    pub metrics: MetricsConfig,
    /// Log level and format.
    pub logging: LoggingConfig,
    /// Normalization data.
    pub normalize: NormalizeConfig,
    /// Analytics engine, signals, rules and delivery.
    pub alerts: AlertsConfig,
    /// Limits of the `History` service (`Query`, `Aggregate`).
    pub history: HistoryApiConfig,
}

/// `[server]` section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// gRPC listen address. Loopback by default; a non-loopback address needs `[tls]` (or, for an internal
    /// network or a TLS-terminating proxy, `allow_plaintext`).
    pub listen_addr: String,
    /// Allows a non-loopback `listen_addr` without `[tls]` (the bearer token would travel in clear text). A
    /// warning is logged at start. Off by default; meaningless when `[tls]` is set.
    pub allow_plaintext: bool,
    /// Directory for the token (`<data_dir>/token`, mode 0600) and service state. Created with mode 0700.
    pub data_dir: PathBuf,
    /// Upper bound for graceful shutdown, seconds.
    pub shutdown_timeout_secs: u64,
    /// Simultaneous `Stream.Subscribe` clients; the next one gets `RESOURCE_EXHAUSTED`.
    pub max_clients: usize,
    /// Events queued for one client. When the queue stays full for `slow_client_timeout_ms`, the client is
    /// disconnected with `RESOURCE_EXHAUSTED`.
    pub client_buffer: usize,
    /// How long a full client queue is tolerated, milliseconds.
    pub slow_client_timeout_ms: u64,
    /// HTTP/2 keepalive ping interval, seconds.
    pub keepalive_interval_secs: u64,
    /// HTTP/2 keepalive ping timeout, seconds: a client that does not answer is dropped.
    pub keepalive_timeout_secs: u64,
    /// How often the server re-reads the token file, milliseconds, to pick up `token rotate` run by another
    /// process.
    pub token_check_interval_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:50051".into(),
            allow_plaintext: false,
            data_dir: PathBuf::from("/var/lib/vulture"),
            shutdown_timeout_secs: 4,
            max_clients: 16,
            client_buffer: 1024,
            slow_client_timeout_ms: 10_000,
            keepalive_interval_secs: 30,
            keepalive_timeout_secs: 10,
            token_check_interval_ms: 1000,
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
    /// CA bundle (PEM) for client certificates; enables mTLS (a client without a valid certificate is refused).
    /// Read at start only: changing it needs a restart.
    pub client_ca: Option<PathBuf>,
    /// How often the certificate and key files are checked for a change (mtime and size), seconds. 0 disables
    /// polling; SIGHUP always reloads.
    #[serde(default = "default_tls_reload_interval")]
    pub reload_interval_secs: u64,
}

fn default_tls_reload_interval() -> u64 {
    30
}

/// `[redis]` section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RedisConfig {
    /// Connection URL. May contain a password, hence [`Secret`].
    pub url: Option<Secret>,
    /// File holding the URL instead of `url` (for container secrets).
    pub url_file: Option<PathBuf>,
    /// Stream key holding every event of every exchange.
    pub stream_key: String,
    /// Hot window length, seconds (older entries are trimmed on every write).
    pub window_secs: u64,
    /// Upper bound for the stream length: a safeguard that keeps a broken time trimming from filling Redis.
    pub maxlen_safeguard: u64,
    /// Events per write pipeline, at most.
    pub batch_size: usize,
    /// Longest wait before a partial batch is written, milliseconds.
    pub flush_interval_ms: u64,
    /// Events kept in memory while Redis is unavailable; above it the oldest are dropped and a `Gap` is written.
    pub buffer_limit: usize,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: None,
            url_file: None,
            stream_key: "vulture:events".into(),
            window_secs: 86_400,
            maxlen_safeguard: 5_000_000,
            batch_size: 500,
            flush_interval_ms: 50,
            buffer_limit: 100_000,
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

/// `[clickhouse]` section: the cold store and the flusher that feeds it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClickHouseConfig {
    /// The flusher runs only when this is `true` (the default when the section exists).
    pub enabled: bool,
    /// HTTP(S) endpoint of the server, for example `http://127.0.0.1:8123`.
    pub url: String,
    /// Database. Created by the migrations if it does not exist; letters, digits and underscore only.
    pub database: String,
    /// User name.
    pub user: String,
    /// Password. Prefer `VULTURE__CLICKHOUSE__PASSWORD` or `password_file`.
    pub password: Option<Secret>,
    /// File holding the password instead of `password`.
    pub password_file: Option<PathBuf>,
    /// Pause between flush cycles, seconds.
    pub flush_interval_secs: u64,
    /// Stream entries read and inserted per page (one insert per table and page).
    pub batch_max_rows: usize,
    /// Deadline of one insert or migration statement, seconds.
    pub insert_timeout_secs: u64,
    /// How often disk and table statistics are sampled, seconds.
    pub monitor_interval_secs: u64,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            url: "http://127.0.0.1:8123".into(),
            database: "vulture".into(),
            user: "default".into(),
            password: None,
            password_file: None,
            flush_interval_secs: 600,
            batch_max_rows: 100_000,
            insert_timeout_secs: 60,
            monitor_interval_secs: 60,
        }
    }
}

impl ClickHouseConfig {
    /// Resolves the password from `password` or `password_file`; empty when neither is set.
    pub fn resolve_password(&self) -> Result<Secret, ConfigError> {
        match (&self.password, &self.password_file) {
            (Some(p), _) => Ok(p.clone()),
            (None, Some(path)) => Secret::from_file(path),
            (None, None) => Ok(Secret::new("")),
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
    /// WebSocket endpoint override (`ws://` or `wss://`); the connector has a public default. Mainly for tests.
    #[serde(default)]
    pub ws_url: Option<String>,
    /// REST endpoint override (`http://` or `https://`, no trailing path); the connector has a public default.
    #[serde(default)]
    pub rest_url: Option<String>,
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
    /// Listen address of the metrics endpoint (`/metrics`, `/healthz`, `/readyz`). It is not authenticated:
    /// keep it on loopback or an internal network.
    pub listen_addr: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:9100".into(),
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
        match self.server.listen_addr.parse::<SocketAddr>() {
            Err(_) => problems.push(format!(
                "server.listen_addr: {:?} is not a socket address (host:port)",
                self.server.listen_addr
            )),
            Ok(addr) => {
                if let Err(e) = crate::grpc::check_bind_policy(
                    addr,
                    self.server.allow_plaintext,
                    self.tls.is_some(),
                ) {
                    problems.push(format!("server.listen_addr: {e}"));
                }
            }
        }
        for (name, ok) in [
            ("server.max_clients", self.server.max_clients > 0),
            ("server.client_buffer", self.server.client_buffer > 0),
            (
                "server.slow_client_timeout_ms",
                self.server.slow_client_timeout_ms > 0,
            ),
            (
                "server.keepalive_interval_secs",
                self.server.keepalive_interval_secs > 0,
            ),
            (
                "server.keepalive_timeout_secs",
                self.server.keepalive_timeout_secs > 0,
            ),
            (
                "server.token_check_interval_ms",
                self.server.token_check_interval_ms > 0,
            ),
        ] {
            if !ok {
                problems.push(format!("{name} must be greater than 0"));
            }
        }
        if self.metrics.listen_addr.parse::<SocketAddr>().is_err() {
            problems.push(format!(
                "metrics.listen_addr: {:?} is not a socket address (host:port)",
                self.metrics.listen_addr
            ));
        }
        if self.server.listen_addr == self.metrics.listen_addr
            && !self.metrics.listen_addr.ends_with(":0")
        {
            problems.push("metrics.listen_addr must differ from server.listen_addr".into());
        }
        if self.server.shutdown_timeout_secs == 0 {
            problems.push("server.shutdown_timeout_secs must be greater than 0".into());
        }
        let r = &self.redis;
        if r.stream_key.trim().is_empty() {
            problems.push("redis.stream_key must not be empty".into());
        }
        for (name, ok) in [
            ("redis.window_secs", r.window_secs > 0),
            ("redis.maxlen_safeguard", r.maxlen_safeguard > 0),
            ("redis.batch_size", r.batch_size > 0),
            ("redis.flush_interval_ms", r.flush_interval_ms > 0),
            ("redis.buffer_limit", r.buffer_limit > 0),
        ] {
            if !ok {
                problems.push(format!("{name} must be greater than 0"));
            }
        }
        if r.batch_size > r.buffer_limit {
            problems.push("redis.batch_size must not exceed redis.buffer_limit".into());
        }
        if let Some(url) = &r.url
            && let Err(e) = crate::store::validate_url(url.expose())
        {
            problems.push(format!("redis.url: {e}"));
        }
        if self.redis.url.is_some() && self.redis.url_file.is_some() {
            problems.push("redis.url and redis.url_file are mutually exclusive".into());
        }
        if let Some(ch) = &self.clickhouse {
            if !(ch.url.starts_with("http://") || ch.url.starts_with("https://")) {
                problems.push("clickhouse.url must start with http:// or https://".into());
            }
            if !crate::history::is_identifier(&ch.database) {
                problems.push(
                    "clickhouse.database must consist of ASCII letters, digits and underscore"
                        .into(),
                );
            }
            if ch.user.trim().is_empty() {
                problems.push("clickhouse.user must not be empty".into());
            }
            if ch.password.is_some() && ch.password_file.is_some() {
                problems.push(
                    "clickhouse.password and clickhouse.password_file are mutually exclusive"
                        .into(),
                );
            }
            for (name, ok) in [
                ("clickhouse.flush_interval_secs", ch.flush_interval_secs > 0),
                ("clickhouse.batch_max_rows", ch.batch_max_rows > 0),
                ("clickhouse.insert_timeout_secs", ch.insert_timeout_secs > 0),
                (
                    "clickhouse.monitor_interval_secs",
                    ch.monitor_interval_secs > 0,
                ),
            ] {
                if !ok {
                    problems.push(format!("{name} must be greater than 0"));
                }
            }
        }
        if let Some(tls) = &self.tls {
            if tls.cert.as_os_str().is_empty() || tls.key.as_os_str().is_empty() {
                problems.push("tls.cert and tls.key must not be empty".into());
            }
            if tls
                .client_ca
                .as_ref()
                .is_some_and(|p| p.as_os_str().is_empty())
            {
                problems.push("tls.client_ca must not be empty when set".into());
            }
        }
        self.alerts.validate(&mut problems);
        self.history.validate(&mut problems);
        if self.alerts.enabled {
            for (i, ex) in self.exchanges.iter().enumerate() {
                if ex.enabled && ex.oi_poll_interval_secs.saturating_mul(2) > self.alerts.hole_secs
                {
                    problems.push(format!(
                        "alerts.hole_secs ({}) must be at least twice exchanges[{i}].oi_poll_interval_secs ({}), otherwise every window looks like it spans a hole",
                        self.alerts.hole_secs, ex.oi_poll_interval_secs
                    ));
                }
            }
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
            if let Some(url) = &ex.ws_url
                && !(url.starts_with("ws://") || url.starts_with("wss://"))
            {
                problems.push(format!(
                    "exchanges[{i}].ws_url: {url:?} must start with ws:// or wss://"
                ));
            }
            if let Some(url) = &ex.rest_url
                && !(url.starts_with("http://") || url.starts_with("https://"))
            {
                problems.push(format!(
                    "exchanges[{i}].rest_url: {url:?} must start with http:// or https://"
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

    /// The `[clickhouse]` section when the flusher is switched on.
    pub fn clickhouse_enabled(&self) -> Option<&ClickHouseConfig> {
        self.clickhouse.as_ref().filter(|c| c.enabled)
    }

    /// Exchanges that must be started.
    pub fn enabled_exchanges(&self) -> impl Iterator<Item = &ExchangeConfig> {
        self.exchanges.iter().filter(|e| e.enabled)
    }
}

#[cfg(test)]
mod tests;
