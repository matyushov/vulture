//! [`RedisStore`] on the `redis` crate: a connection manager that reconnects on its own and connects lazily,
//! so the server starts even when Redis is down.

use std::collections::HashMap;
use std::time::Duration;

use futures_util::future::BoxFuture;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::streams::StreamRangeReply;

use super::{EntryFields, RedisStore, StoreError, StoredEntry, Trim};
use crate::config::Secret;

/// Deadline of one command and of one connection attempt.
const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Checks that `url` is a well-formed Redis URL (`redis://`, `redis+unix://` or `unix://`). The error never
/// repeats the URL because it may contain a password.
pub fn validate_url(url: &str) -> Result<(), String> {
    redis::Client::open(url)
        .map(|_| ())
        .map_err(|_| "not a valid Redis URL".to_string())
}

/// Stream storage on a real Redis server.
pub struct RedisClientStore {
    conn: ConnectionManager,
    key: String,
    /// Substring to hide from error messages.
    secret: String,
}

impl RedisClientStore {
    /// Prepares the client. No connection is made yet.
    pub fn new(url: &Secret, stream_key: &str) -> Result<Self, StoreError> {
        let secret = url.expose().to_string();
        let client = redis::Client::open(secret.as_str())
            .map_err(|_| StoreError::Command("not a valid Redis URL".into()))?;
        // The writer owns the retry policy (backoff, buffer); the manager only needs to reconnect once.
        let config = ConnectionManagerConfig::new()
            .set_number_of_retries(1)
            .set_connection_timeout(Some(IO_TIMEOUT))
            .set_response_timeout(Some(IO_TIMEOUT));
        let conn = ConnectionManager::new_lazy_with_config(client, config)
            .map_err(|e| convert(&e, &secret))?;
        Ok(Self {
            conn,
            key: stream_key.to_string(),
            secret,
        })
    }

    fn err(&self, e: &redis::RedisError) -> StoreError {
        convert(e, &self.secret)
    }
}

fn convert(e: &redis::RedisError, secret: &str) -> StoreError {
    let mut message = e.to_string();
    if !secret.is_empty() {
        message = message.replace(secret, "<redacted>");
    }
    // A rejected login is a problem of the connection, not of one command: retry with backoff and keep buffering.
    if e.is_io_error()
        || e.is_timeout()
        || e.is_connection_dropped()
        || e.is_connection_refusal()
        || e.kind() == redis::ErrorKind::AuthenticationFailed
    {
        StoreError::Connection(message)
    } else {
        StoreError::Command(message)
    }
}

impl RedisStore for RedisClientStore {
    fn append<'a>(
        &'a self,
        entries: &'a [EntryFields],
        trim: Trim,
    ) -> BoxFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(async move {
            let mut pipe = redis::pipe();
            // MULTI/EXEC: a batch is applied entirely or not at all.
            pipe.atomic();
            for e in entries {
                pipe.cmd("XADD")
                    .arg(&self.key)
                    .arg("*")
                    .arg("t")
                    .arg(e.kind)
                    .arg("x")
                    .arg(e.exchange.as_bytes())
                    .arg("s")
                    .arg(e.symbol.as_bytes())
                    .arg("d")
                    .arg(e.data.as_slice());
            }
            pipe.cmd("XTRIM")
                .arg(&self.key)
                .arg("MINID")
                .arg("~")
                .arg(format!("{}-0", trim.min_id_ms.max(0)))
                .ignore();
            pipe.cmd("XTRIM")
                .arg(&self.key)
                .arg("MAXLEN")
                .arg("~")
                .arg(trim.maxlen)
                .ignore();
            let mut conn = self.conn.clone();
            let ids: Vec<String> = pipe
                .query_async(&mut conn)
                .await
                .map_err(|e| self.err(&e))?;
            Ok(ids)
        })
    }

    fn read_range<'a>(
        &'a self,
        start: &'a str,
        end: &'a str,
        count: Option<usize>,
    ) -> BoxFuture<'a, Result<Vec<StoredEntry>, StoreError>> {
        Box::pin(async move {
            let mut cmd = redis::cmd("XRANGE");
            cmd.arg(&self.key).arg(start).arg(end);
            if let Some(n) = count {
                cmd.arg("COUNT").arg(n);
            }
            let mut conn = self.conn.clone();
            let reply: StreamRangeReply =
                cmd.query_async(&mut conn).await.map_err(|e| self.err(&e))?;
            let mut out = Vec::with_capacity(reply.ids.len());
            for item in reply.ids {
                let text = |name: &str| -> String {
                    item.get::<Vec<u8>>(name)
                        .map(|b| String::from_utf8_lossy(&b).into_owned())
                        .unwrap_or_default()
                };
                out.push(StoredEntry {
                    kind: text("t"),
                    exchange: text("x"),
                    symbol: text("s"),
                    data: item.get::<Vec<u8>>("d").unwrap_or_default(),
                    id: item.id,
                });
            }
            Ok(out)
        })
    }

    fn stream_len(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            let mut conn = self.conn.clone();
            redis::cmd("XLEN")
                .arg(&self.key)
                .query_async(&mut conn)
                .await
                .map_err(|e| self.err(&e))
        })
    }

    fn health(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let mut conn = self.conn.clone();
            let _: String = redis::cmd("PING")
                .query_async(&mut conn)
                .await
                .map_err(|e| self.err(&e))?;
            Ok(())
        })
    }

    fn settings<'a>(
        &'a self,
        names: &'a [&'a str],
    ) -> BoxFuture<'a, Result<Vec<(String, String)>, StoreError>> {
        Box::pin(async move {
            let mut conn = self.conn.clone();
            let mut out = Vec::new();
            for name in names {
                let reply: HashMap<String, String> = redis::cmd("CONFIG")
                    .arg("GET")
                    .arg(*name)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| self.err(&e))?;
                out.extend(reply.into_iter().map(|(k, v)| (k.to_lowercase(), v)));
            }
            Ok(out)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_validation_accepts_redis_schemes_and_hides_the_url_in_errors() {
        assert!(validate_url("redis://127.0.0.1:6379").is_ok());
        assert!(validate_url("redis://:pw@host:6379/0").is_ok());
        assert!(validate_url("rediss://host:6380").is_ok());
        let err = validate_url("http://user:hunter2@host").unwrap_err();
        assert!(!err.contains("hunter2"), "{err}");
        assert!(validate_url("not a url").is_err());
    }

    #[tokio::test]
    async fn a_tls_url_connects_lazily_and_fails_cleanly_when_nothing_listens() {
        // Exercises the rustls code path of the client (provider selection) without a TLS server.
        let store =
            RedisClientStore::new(&Secret::new("rediss://:pw123@127.0.0.1:1"), "k").unwrap();
        let err = store.health().await.unwrap_err();
        assert!(matches!(err, StoreError::Connection(_)), "{err:?}");
        assert!(!err.to_string().contains("pw123"));
    }
}
