//! A small ClickHouse HTTP client: statements, queries returning JSON rows, and `RowBinary` inserts with an
//! insert deduplication token. Credentials travel in request headers, never in the URL or in error messages.

use std::time::Duration;

use futures_util::future::BoxFuture;
use reqwest::header::HeaderValue;

use super::migrations;
use super::rows::Table;
use crate::config::{ClickHouseConfig, Secret};

/// Failure of a ClickHouse request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChError {
    /// Unreachable, connection dropped or timed out.
    #[error("connection: {0}")]
    Connection(String),
    /// The server answered with an error (HTTP status, code and message as ClickHouse reports them).
    #[error("server (HTTP {status}): {message}")]
    Server {
        /// HTTP status.
        status: u16,
        /// Message, trimmed and truncated.
        message: String,
    },
    /// The schema migrations failed or are inconsistent.
    #[error("migration: {0}")]
    Migration(String),
}

impl ChError {
    /// Short class for metric labels.
    pub fn kind(&self) -> &'static str {
        match self {
            ChError::Connection(_) => "connection",
            ChError::Server { .. } => "server",
            ChError::Migration(_) => "migration",
        }
    }
}

/// What the flusher needs from ClickHouse. Implemented by [`ChClient`] and by fakes in tests.
pub trait HistorySink: Send + Sync {
    /// Applies pending schema migrations (idempotent). Returns the versions applied now.
    fn migrate(&self) -> BoxFuture<'_, Result<Vec<u32>, ChError>>;

    /// Inserts a `RowBinary` body into `table`. `token` makes the insert idempotent: the server drops a block it
    /// has already stored under the same token.
    fn insert<'a>(
        &'a self,
        table: Table,
        body: Vec<u8>,
        token: &'a str,
    ) -> BoxFuture<'a, Result<(), ChError>>;

    /// Cheap liveness check.
    fn ping(&self) -> BoxFuture<'_, Result<(), ChError>>;
}

/// The HTTP client.
pub struct ChClient {
    http: reqwest::Client,
    base: reqwest::Url,
    database: String,
    user: String,
    password: Secret,
    timeout: Duration,
}

impl ChClient {
    /// Builds a client from `[clickhouse]`; no connection is made.
    pub fn new(cfg: &ClickHouseConfig, password: Secret) -> Result<Self, String> {
        let base = reqwest::Url::parse(&cfg.url).map_err(|_| "not a valid URL".to_string())?;
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let http = reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .connect_timeout(Duration::from_secs(5))
            .pool_idle_timeout(Duration::from_secs(30))
            .user_agent(concat!("vulture-server/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| e.without_url().to_string())?;
        Ok(Self {
            http,
            base,
            database: cfg.database.clone(),
            user: cfg.user.clone(),
            password,
            timeout: Duration::from_secs(cfg.insert_timeout_secs),
        })
    }

    /// The configured database.
    pub fn database(&self) -> &str {
        &self.database
    }

    async fn post(
        &self,
        params: &[(&str, &str)],
        with_database: bool,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<String, ChError> {
        let mut url = self.base.clone();
        {
            let mut q = url.query_pairs_mut();
            if with_database {
                q.append_pair("database", &self.database);
            }
            for (k, v) in params {
                q.append_pair(k, v);
            }
        }
        let mut request = self
            .http
            .post(url)
            .timeout(timeout)
            .header("X-ClickHouse-User", &self.user);
        if !self.password.expose().is_empty() {
            let mut key = HeaderValue::from_str(self.password.expose()).map_err(|_| {
                ChError::Connection("the password is not a valid header value".into())
            })?;
            key.set_sensitive(true);
            request = request.header("X-ClickHouse-Key", key);
        }
        let response = request
            .body(body)
            .send()
            .await
            .map_err(|e| ChError::Connection(e.without_url().to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| ChError::Connection(e.without_url().to_string()))?;
        if status.is_success() {
            Ok(text)
        } else {
            Err(ChError::Server {
                status: status.as_u16(),
                message: truncate(text.trim(), 600),
            })
        }
    }

    /// Runs a statement (DDL, `INSERT ... VALUES` in the text, ...) in the configured database.
    pub async fn execute(&self, sql: &str) -> Result<(), ChError> {
        self.post(&[], true, sql.as_bytes().to_vec(), self.timeout)
            .await
            .map(|_| ())
    }

    /// Runs a statement without selecting the database (used to create it).
    pub async fn execute_server(&self, sql: &str) -> Result<(), ChError> {
        self.post(&[], false, sql.as_bytes().to_vec(), self.timeout)
            .await
            .map(|_| ())
    }

    /// Runs a query and returns its rows as JSON objects (`FORMAT JSONEachRow`). 64-bit integers arrive as
    /// strings (the server default), see [`json_u64`].
    pub async fn query_rows(&self, sql: &str) -> Result<Vec<serde_json::Value>, ChError> {
        let text = self
            .post(
                &[("default_format", "JSONEachRow")],
                true,
                sql.as_bytes().to_vec(),
                self.timeout,
            )
            .await?;
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                serde_json::from_str(l)
                    .map_err(|e| ChError::Connection(format!("unparsable answer: {e}")))
            })
            .collect()
    }

    /// Inserts a `RowBinary` body. See [`HistorySink::insert`].
    pub async fn insert_row_binary(
        &self,
        table: Table,
        body: Vec<u8>,
        token: &str,
    ) -> Result<(), ChError> {
        let query = format!(
            "INSERT INTO {} ({}) FORMAT RowBinary",
            table.name(),
            table.columns()
        );
        self.post(
            &[
                ("query", &query),
                ("insert_deduplication_token", token),
                // The token only works for synchronous inserts with deduplication on, whatever the user profile says.
                ("insert_deduplicate", "1"),
                ("async_insert", "0"),
            ],
            true,
            body,
            self.timeout,
        )
        .await
        .map(|_| ())
    }
}

impl HistorySink for ChClient {
    fn migrate(&self) -> BoxFuture<'_, Result<Vec<u32>, ChError>> {
        Box::pin(migrations::apply(self))
    }

    fn insert<'a>(
        &'a self,
        table: Table,
        body: Vec<u8>,
        token: &'a str,
    ) -> BoxFuture<'a, Result<(), ChError>> {
        Box::pin(self.insert_row_binary(table, body, token))
    }

    fn ping(&self) -> BoxFuture<'_, Result<(), ChError>> {
        Box::pin(async move {
            self.post(&[], false, b"SELECT 1".to_vec(), Duration::from_secs(5))
                .await
                .map(|_| ())
        })
    }
}

/// A 64-bit unsigned integer from a JSON answer (ClickHouse quotes them) or a plain number.
pub fn json_u64(v: &serde_json::Value) -> u64 {
    match v {
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
        _ => 0,
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_numbers_come_quoted_or_plain() {
        assert_eq!(
            json_u64(&serde_json::json!("18446744073709551615")),
            u64::MAX
        );
        assert_eq!(json_u64(&serde_json::json!(12)), 12);
        assert_eq!(json_u64(&serde_json::json!(null)), 0);
    }

    #[test]
    fn truncation_respects_char_boundaries() {
        assert_eq!(truncate("abc", 10), "abc");
        assert_eq!(truncate("ééé", 3), "é...");
    }

    #[tokio::test]
    async fn an_unreachable_server_is_a_connection_error_without_secrets() {
        let cfg = ClickHouseConfig {
            url: "http://127.0.0.1:1".into(),
            ..ClickHouseConfig::default()
        };
        let client = ChClient::new(&cfg, Secret::new("hunter2")).unwrap();
        let err = client.ping().await.unwrap_err();
        assert!(matches!(err, ChError::Connection(_)), "{err:?}");
        assert!(!err.to_string().contains("hunter2"));
        assert_eq!(err.kind(), "connection");
    }
}
