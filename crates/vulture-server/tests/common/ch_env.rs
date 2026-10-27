//! Real ClickHouse and Redis for the gated `History` tests. They need `VULTURE_TEST_CLICKHOUSE_URL` (user and
//! password from `VULTURE_TEST_CLICKHOUSE_USER` / `VULTURE_TEST_CLICKHOUSE_PASSWORD`) and, for the Redis parts,
//! `VULTURE_TEST_REDIS_URL`; without them the tests print `SKIPPED` and pass. Every test uses its own database and
//! its own Redis key and removes both at the end.
#![allow(dead_code)]

use std::sync::Arc;

use vulture_proto::v1::Event;
use vulture_server::config::{ClickHouseConfig, Secret};
use vulture_server::history::client::json_u64;
use vulture_server::history::{ChClient, HistorySink, PageRows, Table, WATERMARK_KEY, dedup_token};
use vulture_server::store::{EntryFields, RedisClientStore, StoredEntry};

pub fn ch_url() -> Option<String> {
    match std::env::var("VULTURE_TEST_CLICKHOUSE_URL") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!(
                "SKIPPED: VULTURE_TEST_CLICKHOUSE_URL is not set (no ClickHouse server for this test)"
            );
            None
        }
    }
}

pub fn redis_url() -> Option<String> {
    match std::env::var("VULTURE_TEST_REDIS_URL") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!("SKIPPED: VULTURE_TEST_REDIS_URL is not set (no Redis server for this test)");
            None
        }
    }
}

pub fn unique(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{name}_{}_{nanos}", std::process::id())
}

pub struct ChEnv {
    pub cfg: ClickHouseConfig,
    pub password: Secret,
    pub client: Arc<ChClient>,
    pub db: String,
    pub redis: Option<(String, String)>,
}

impl ChEnv {
    /// A fresh migrated database (and a Redis key when Redis is wanted and available).
    pub async fn new(name: &str, with_redis: bool) -> Option<Self> {
        let url = ch_url()?;
        let redis = if with_redis {
            let r = redis_url()?;
            Some((r, format!("vulture:test:{}", unique(name))))
        } else {
            None
        };
        let db = unique(&format!("vulture_test_{name}"));
        let cfg = ClickHouseConfig {
            url,
            database: db.clone(),
            user: std::env::var("VULTURE_TEST_CLICKHOUSE_USER")
                .unwrap_or_else(|_| "default".into()),
            insert_timeout_secs: 60,
            ..ClickHouseConfig::default()
        };
        let password =
            Secret::new(std::env::var("VULTURE_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default());
        let client = Arc::new(ChClient::new(&cfg, password.clone()).unwrap());
        client.migrate().await.unwrap();
        Some(Self {
            cfg,
            password,
            client,
            db,
            redis,
        })
    }

    pub fn redis_store(&self) -> Arc<RedisClientStore> {
        let (url, key) = self.redis.as_ref().unwrap();
        Arc::new(RedisClientStore::new(&Secret::new(url.clone()), key).unwrap())
    }

    /// A client for a ClickHouse that is not there.
    pub fn dead_client(&self) -> Arc<ChClient> {
        let cfg = ClickHouseConfig {
            url: "http://127.0.0.1:1".into(),
            ..self.cfg.clone()
        };
        Arc::new(ChClient::new(&cfg, self.password.clone()).unwrap())
    }

    /// Inserts events (with their IDs) the way the flusher does.
    pub async fn insert(&self, events: &[Event]) {
        for (n, chunk) in events.chunks(20_000).enumerate() {
            let entries: Vec<StoredEntry> = chunk.iter().map(entry_of).collect();
            let first = entries.first().unwrap().id.clone();
            let last = entries.last().unwrap().id.clone();
            let page = PageRows::build(entries);
            for table in Table::ALL {
                let batch = &page.tables[table.index()];
                if batch.rows == 0 {
                    continue;
                }
                let token = format!("{}:{n}", dedup_token(table, &first, &last));
                self.client
                    .insert(table, batch.body.clone(), &token)
                    .await
                    .unwrap();
            }
        }
    }

    /// Writes entries with explicit IDs into the Redis stream (the flusher's watermark is not touched).
    pub async fn redis_fill(&self, events: &[Event]) {
        let (url, key) = self.redis.as_ref().unwrap();
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        for e in events {
            let f = EntryFields::from_event(e).unwrap();
            let _: String = redis::cmd("XADD")
                .arg(key)
                .arg(&e.id)
                .arg("t")
                .arg(f.kind)
                .arg("x")
                .arg(f.exchange.as_bytes())
                .arg("s")
                .arg(f.symbol.as_bytes())
                .arg("d")
                .arg(f.data.as_slice())
                .query_async(&mut conn)
                .await
                .unwrap();
        }
    }

    /// Sets the flusher's watermark in Redis.
    pub async fn redis_set_watermark(&self, id: &str) {
        let (url, key) = self.redis.as_ref().unwrap();
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg(format!("{key}:{WATERMARK_KEY}"))
            .arg(id)
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    pub async fn scalar(&self, sql: &str) -> u64 {
        let rows = self.client.query_rows(sql).await.unwrap();
        json_u64(rows[0].as_object().unwrap().values().next().unwrap())
    }

    pub async fn cleanup(&self) {
        let _ = self
            .client
            .execute_server(&format!("DROP DATABASE IF EXISTS {}", self.db))
            .await;
        if let Some((url, key)) = &self.redis {
            let mut conn = redis::Client::open(url.as_str())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            for k in [key.clone(), format!("{key}:{WATERMARK_KEY}")] {
                let _: i64 = redis::cmd("DEL")
                    .arg(k)
                    .query_async(&mut conn)
                    .await
                    .unwrap();
            }
        }
    }
}

pub fn entry_of(e: &Event) -> StoredEntry {
    let f = EntryFields::from_event(e).expect("payload");
    StoredEntry {
        id: e.id.clone(),
        kind: f.kind.to_string(),
        exchange: f.exchange,
        symbol: f.symbol,
        data: f.data,
    }
}
