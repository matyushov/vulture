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
    /// Set once the server refused the resource limits of a read query as a setting change: a read-only user
    /// (`readonly = 1`) cannot change settings, its profile carries the limits instead.
    limits_refused: std::sync::atomic::AtomicBool,
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
            limits_refused: std::sync::atomic::AtomicBool::new(false),
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

    async fn send(
        &self,
        params: &[(&str, &str)],
        with_database: bool,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<reqwest::Response, ChError> {
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
        if status.is_success() {
            return Ok(response);
        }
        let text = response
            .text()
            .await
            .map_err(|e| ChError::Connection(e.without_url().to_string()))?;
        Err(ChError::Server {
            status: status.as_u16(),
            message: truncate(text.trim(), 600),
        })
    }

    async fn post(
        &self,
        params: &[(&str, &str)],
        with_database: bool,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<String, ChError> {
        self.send(params, with_database, body, timeout)
            .await?
            .text()
            .await
            .map_err(|e| ChError::Connection(e.without_url().to_string()))
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

    fn read_params(&self, settings: &ReadSettings, buffered: bool) -> Vec<(String, String)> {
        let mut p = vec![
            ("default_format".to_string(), "JSONEachRow".to_string()),
            (
                "output_format_json_quote_64bit_integers".to_string(),
                "0".to_string(),
            ),
        ];
        if !self
            .limits_refused
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            p.push((
                "max_execution_time".to_string(),
                settings.max_execution_time_secs.to_string(),
            ));
            p.push((
                "max_memory_usage".to_string(),
                settings.max_memory_usage.to_string(),
            ));
        }
        if buffered {
            // The summary header (rows read) is only final when the whole answer is produced before the first byte.
            p.push(("wait_end_of_query".to_string(), "1".to_string()));
        }
        p
    }

    /// Sends a read query. A read-only user cannot change settings: when the server says so, the limits are left
    /// to the user's profile from then on and the query is repeated.
    async fn send_read(
        &self,
        sql: &str,
        settings: &ReadSettings,
        buffered: bool,
    ) -> Result<reqwest::Response, ChError> {
        for attempt in 0..2 {
            let owned = self.read_params(settings, buffered);
            let params: Vec<(&str, &str)> = owned
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            match self
                .send(&params, true, sql.as_bytes().to_vec(), settings.timeout)
                .await
            {
                Err(ChError::Server { message, .. })
                    if attempt == 0
                        && !self
                            .limits_refused
                            .load(std::sync::atomic::Ordering::Relaxed)
                        && (message.contains("READONLY") || message.contains("Cannot modify")) =>
                {
                    self.limits_refused
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    tracing::info!(
                        "the ClickHouse user is read-only: query limits come from its profile, not from the client"
                    );
                }
                other => return other,
            }
        }
        unreachable!("the second attempt always returns")
    }

    /// Runs a read query and returns all rows (`FORMAT JSONEachRow`, 64-bit integers as plain numbers) with the
    /// server's summary of the work done. For answers that fit in memory.
    pub async fn query_rows_with(
        &self,
        sql: &str,
        settings: &ReadSettings,
    ) -> Result<(Vec<serde_json::Value>, QuerySummary), ChError> {
        let response = self.send_read(sql, settings, true).await?;
        let summary = QuerySummary::from_headers(response.headers());
        let text = response
            .text()
            .await
            .map_err(|e| ChError::Connection(e.without_url().to_string()))?;
        let mut rows = Vec::new();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            rows.push(parse_row(line)?);
        }
        Ok((rows, summary))
    }

    /// Runs a read query and hands the rows out as they arrive, so that a large answer never sits in memory.
    pub async fn query_stream(
        &self,
        sql: &str,
        settings: &ReadSettings,
    ) -> Result<RowStream, ChError> {
        let response = self.send_read(sql, settings, false).await?;
        Ok(RowStream {
            response,
            buf: Vec::new(),
            done: false,
        })
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

/// Settings of one read query.
#[derive(Debug, Clone)]
pub struct ReadSettings {
    /// `max_execution_time` of the query, seconds.
    pub max_execution_time_secs: u64,
    /// `max_memory_usage` of the query, bytes.
    pub max_memory_usage: u64,
    /// Deadline of the whole HTTP request (a streamed answer included).
    pub timeout: Duration,
}

/// What ClickHouse reports about the work behind an answer (`X-ClickHouse-Summary`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuerySummary {
    /// Rows read from the tables.
    pub read_rows: u64,
    /// Bytes read from the tables.
    pub read_bytes: u64,
}

impl QuerySummary {
    fn from_headers(headers: &reqwest::header::HeaderMap) -> Self {
        let Some(text) = headers
            .get("x-clickhouse-summary")
            .and_then(|v| v.to_str().ok())
        else {
            return Self::default();
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
            return Self::default();
        };
        Self {
            read_rows: json_u64(&v["read_rows"]),
            read_bytes: json_u64(&v["read_bytes"]),
        }
    }
}

/// The rows of a streamed answer.
pub struct RowStream {
    response: reqwest::Response,
    buf: Vec<u8>,
    done: bool,
}

impl RowStream {
    /// The next row, or `None` at the end of the answer. An error that ClickHouse reports after it has started
    /// to answer (a memory or time limit hit mid-way) arrives here.
    pub async fn next_row(&mut self) -> Result<Option<serde_json::Value>, ChError> {
        loop {
            if let Some(pos) = self.buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).collect();
                let text = String::from_utf8_lossy(&line[..line.len() - 1]);
                if text.trim().is_empty() {
                    continue;
                }
                return parse_row(&text).map(Some);
            }
            if self.done {
                if self.buf.iter().all(u8::is_ascii_whitespace) {
                    return Ok(None);
                }
                let line = std::mem::take(&mut self.buf);
                return parse_row(&String::from_utf8_lossy(&line)).map(Some);
            }
            match self
                .response
                .chunk()
                .await
                .map_err(|e| ChError::Connection(e.without_url().to_string()))?
            {
                Some(chunk) => self.buf.extend_from_slice(&chunk),
                None => self.done = true,
            }
        }
    }
}

fn parse_row(line: &str) -> Result<serde_json::Value, ChError> {
    match serde_json::from_str::<serde_json::Value>(line) {
        Ok(v) => {
            if let Some(e) = v.get("exception").and_then(|e| e.as_str()) {
                return Err(ChError::Server {
                    status: 200,
                    message: truncate(e.trim(), 600),
                });
            }
            Ok(v)
        }
        Err(_) if line.contains("DB::Exception") || line.starts_with("Code:") => {
            Err(ChError::Server {
                status: 200,
                message: truncate(line.trim(), 600),
            })
        }
        Err(e) => Err(ChError::Connection(format!("unparsable answer: {e}"))),
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
    fn rows_parse_and_mid_stream_errors_surface() {
        assert_eq!(parse_row("{\"a\":1.5}").unwrap()["a"], 1.5);
        // Floats survive the round trip exactly.
        let v = parse_row("{\"x\":983.6709816088288}").unwrap();
        assert_eq!(
            v["x"].as_f64().unwrap().to_bits(),
            983.6709816088288f64.to_bits()
        );
        let e =
            parse_row("{\"exception\":\"Code: 241. DB::Exception: Memory limit\"}").unwrap_err();
        assert!(matches!(e, ChError::Server { .. }), "{e:?}");
        let e = parse_row("Code: 159. DB::Exception: Timeout exceeded").unwrap_err();
        assert!(matches!(e, ChError::Server { .. }), "{e:?}");
        assert!(matches!(
            parse_row("garbage").unwrap_err(),
            ChError::Connection(_)
        ));
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
