//! Tests of the ClickHouse schema, the migrations and the flusher against real servers.
//!
//! They need `VULTURE_TEST_CLICKHOUSE_URL` (for example `http://127.0.0.1:8123`) and skip with a message when it
//! is not set; the user and password come from `VULTURE_TEST_CLICKHOUSE_USER` (default `default`) and
//! `VULTURE_TEST_CLICKHOUSE_PASSWORD` (default empty). The flusher tests also need `VULTURE_TEST_REDIS_URL`.
//! Every test creates its own database and Redis stream key and removes them at the end. Use throwaway
//! containers bound to loopback, for example:
//!
//! ```sh
//! docker run -d --rm --name vulture-test-ch -p 127.0.0.1:8123:8123 -e CLICKHOUSE_PASSWORD=test \
//!     clickhouse/clickhouse-server:25.8
//! docker run -d --rm --name vulture-test-redis -p 127.0.0.1:6379:6379 redis:7
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::{
    Event, Gap, Liquidation, OpenInterest, Side, Signal, SignalType, event::Payload,
};
use vulture_server::app::{AppContext, run_with_history};
use vulture_server::config::{ClickHouseConfig, Config, Secret};
use vulture_server::exchange::{EventSink, Exchange, ExchangeError};
use vulture_server::history::client::json_u64;
use vulture_server::history::{
    ChClient, ChError, ChState, Flusher, FlusherConfig, FlusherMetrics, FlusherStatus,
    HistorySetup, HistorySink, INFLIGHT_KEY, Table, WATERMARK_KEY, Watermark, build_client,
    monitor::ClickHouseMonitor,
};
use vulture_server::metrics::{MetricSource, MetricsHub};
use vulture_server::store::{
    EntryFields, RedisClientStore, RedisStore, RedisWriter, Trim, WriterConfig,
};
use vulture_server::ws::{Clock, SystemClock};

fn ch_url() -> Option<String> {
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

fn redis_url() -> Option<String> {
    match std::env::var("VULTURE_TEST_REDIS_URL") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!("SKIPPED: VULTURE_TEST_REDIS_URL is not set (no Redis server for this test)");
            None
        }
    }
}

fn unique(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{name}_{}_{nanos}", std::process::id())
}

struct Env {
    cfg: ClickHouseConfig,
    password: Secret,
    admin: ChClient,
    db: String,
    redis: Option<(String, String)>,
}

impl Env {
    /// ClickHouse only.
    async fn ch(name: &str) -> Option<Self> {
        let url = ch_url()?;
        let db = unique(&format!("vulture_test_{name}"));
        let cfg = ClickHouseConfig {
            url,
            database: db.clone(),
            user: std::env::var("VULTURE_TEST_CLICKHOUSE_USER")
                .unwrap_or_else(|_| "default".into()),
            insert_timeout_secs: 30,
            ..ClickHouseConfig::default()
        };
        let password =
            Secret::new(std::env::var("VULTURE_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default());
        let admin = ChClient::new(&cfg, password.clone()).unwrap();
        Some(Self {
            cfg,
            password,
            admin,
            db,
            redis: None,
        })
    }

    /// ClickHouse and Redis.
    async fn both(name: &str) -> Option<Self> {
        let mut env = Self::ch(name).await?;
        let url = redis_url()?;
        env.redis = Some((url, format!("vulture:test:{}", unique(name))));
        Some(env)
    }

    fn client(&self) -> Arc<ChClient> {
        Arc::new(ChClient::new(&self.cfg, self.password.clone()).unwrap())
    }

    fn store(&self) -> Arc<RedisClientStore> {
        let (url, key) = self.redis.as_ref().unwrap();
        Arc::new(RedisClientStore::new(&Secret::new(url.clone()), key).unwrap())
    }

    async fn migrate(&self) {
        self.admin.migrate().await.unwrap();
    }

    async fn scalar(&self, sql: &str) -> u64 {
        let rows = self.admin.query_rows(sql).await.unwrap();
        json_u64(rows[0].as_object().unwrap().values().next().unwrap())
    }

    async fn count(&self, table: &str) -> u64 {
        self.scalar(&format!("SELECT count() FROM {table}")).await
    }

    async fn cleanup(&self) {
        let _ = self
            .admin
            .execute_server(&format!("DROP DATABASE IF EXISTS {}", self.db))
            .await;
        if let Some((url, key)) = &self.redis {
            let mut conn = redis::Client::open(url.as_str())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            for k in [
                key.clone(),
                format!("{key}:{WATERMARK_KEY}"),
                format!("{key}:{INFLIGHT_KEY}"),
            ] {
                let _: i64 = redis::cmd("DEL")
                    .arg(k)
                    .query_async(&mut conn)
                    .await
                    .unwrap();
            }
        }
    }
}

fn now_ms() -> i64 {
    SystemClock.now_ms()
}

fn liq(ts: i64, n: usize) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::Liquidation(Liquidation {
            exchange: "binance".into(),
            symbol_raw: "BTCUSDT".into(),
            symbol: "BTC-USDT-PERP".into(),
            exchange_ts: ts - 20,
            recv_ts: ts,
            side: if n.is_multiple_of(2) {
                Side::Long
            } else {
                Side::Short
            } as i32,
            price: 64_321.5 + n as f64,
            qty: 0.153,
            notional_usd: 9_841.25,
        })),
    }
}

fn oi(ts: i64, n: usize) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::OpenInterest(OpenInterest {
            exchange: "binance".into(),
            symbol_raw: format!("SYM{n:03}USDT"),
            symbol: format!("SYM{n:03}-USDT-PERP"),
            exchange_ts: ts - 15,
            recv_ts: ts,
            oi_base: 91_959.654 + n as f64,
            oi_usd: n.is_multiple_of(2).then_some(5_912_345_678.25 + n as f64),
            mark_price: n.is_multiple_of(3).then_some(64_295.1),
            ..OpenInterest::default()
        })),
    }
}

fn gap(n: i64) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::Gap(Gap {
            exchange: "*".into(),
            stream: "redis-writer".into(),
            from_ts: 1_700_000_000_000 + n,
            to_ts: 1_700_000_001_000 + n,
        })),
    }
}

fn signal(ts: i64) -> Event {
    let mut details = HashMap::new();
    details.insert("symbols".to_string(), "12".to_string());
    details.insert("peak_usd_per_min".to_string(), "1500000".to_string());
    Event {
        id: String::new(),
        payload: Some(Payload::Signal(Signal {
            r#type: SignalType::LiqCascade as i32,
            exchange: "*".into(),
            symbol: String::new(),
            window_secs: 300,
            change_pct: 850.5,
            change_abs: 2_500_000.0,
            threshold: 500.0,
            ts,
            details,
        })),
    }
}

fn no_trim() -> Trim {
    Trim {
        min_id_ms: 0,
        maxlen: i64::MAX as u64,
    }
}

async fn append_events(store: &RedisClientStore, events: &[Event]) -> Vec<String> {
    let entries: Vec<EntryFields> = events
        .iter()
        .map(|e| EntryFields::from_event(e).unwrap())
        .collect();
    let mut ids = Vec::new();
    // In chunks, like the writer.
    for chunk in entries.chunks(500) {
        ids.extend(store.append(chunk, no_trim()).await.unwrap());
    }
    ids
}

fn flusher_cfg(rows: usize) -> FlusherConfig {
    FlusherConfig {
        interval: Duration::from_millis(300),
        batch_max_rows: rows,
        retry_min: Duration::from_millis(100),
        retry_max: Duration::from_millis(400),
        page_pause: Duration::from_millis(1),
        stats_interval: Duration::from_millis(200),
        shutdown_timeout: Duration::from_secs(10),
    }
}

fn flusher(
    env: &Env,
    sink: Arc<dyn HistorySink>,
    rows: usize,
    watermark: Arc<Watermark>,
) -> Flusher {
    let status = Arc::new(FlusherStatus::new(watermark.clone(), 5_000_000, now_ms()));
    Flusher::new(
        env.store(),
        sink,
        flusher_cfg(rows),
        watermark,
        status,
        Arc::new(SystemClock),
    )
}

/// A mixed workload: `n` events cycling through the four types.
fn mixed(n: usize) -> Vec<Event> {
    let base = now_ms();
    (0..n)
        .map(|i| match i % 10 {
            0 => liq(base + i as i64, i),
            9 => gap(i as i64),
            8 if i % 100 == 8 => signal(base + i as i64),
            _ => oi(base + i as i64, i),
        })
        .collect()
}

#[tokio::test]
async fn migrations_apply_twice_and_detect_edits() {
    let Some(env) = Env::ch("migrate").await else {
        return;
    };
    let first = env.admin.migrate().await.unwrap();
    assert_eq!(first, vec![1, 2]);
    assert_eq!(env.admin.migrate().await.unwrap(), Vec::<u32>::new());
    assert_eq!(env.count("schema_migrations").await, 2);
    for t in ["liquidations", "open_interest", "gaps", "signals"] {
        let rows = env
            .admin
            .query_rows(&format!(
                "SELECT engine_full FROM system.tables WHERE database = '{}' AND name = '{t}'",
                env.db
            ))
            .await
            .unwrap();
        let engine = rows[0]["engine_full"].as_str().unwrap();
        assert!(engine.contains("MergeTree"), "{engine}");
        assert!(engine.contains("PARTITION BY toYYYYMM"), "{engine}");
        assert!(
            engine.contains("non_replicated_deduplication_window"),
            "{engine}"
        );
        assert!(!engine.contains("TTL"), "{engine}");
    }
    // A migration changed after it was applied is refused.
    env.admin
        .execute("ALTER TABLE schema_migrations UPDATE checksum = 'tampered' WHERE version = 1 SETTINGS mutations_sync = 2")
        .await
        .unwrap();
    let err = env.admin.migrate().await.unwrap_err();
    assert!(matches!(err, ChError::Migration(_)), "{err:?}");
    env.cleanup().await;
}

#[tokio::test]
async fn insert_deduplication_by_token_works_on_the_real_tables() {
    let Some(env) = Env::ch("dedup").await else {
        return;
    };
    env.migrate().await;
    let mut body = Vec::new();
    for e in [gap(1), gap(2)] {
        vulture_server::history::Row::from_event(&e, "1-0")
            .unwrap()
            .encode(&mut body);
    }
    env.admin
        .insert_row_binary(Table::Gaps, body.clone(), "tok-a")
        .await
        .unwrap();
    env.admin
        .insert_row_binary(Table::Gaps, body.clone(), "tok-a")
        .await
        .unwrap();
    assert_eq!(env.count("gaps").await, 2, "same token: dropped");
    env.admin
        .insert_row_binary(Table::Gaps, body, "tok-b")
        .await
        .unwrap();
    assert_eq!(env.count("gaps").await, 4, "other token: stored");
    env.cleanup().await;
}

#[tokio::test]
async fn every_row_type_is_stored_with_the_right_values() {
    let Some(env) = Env::ch("rows").await else {
        return;
    };
    env.migrate().await;
    let events = [
        liq(1_700_000_000_500, 1),
        oi(1_700_000_000_600, 2),
        oi(1_700_000_000_700, 1),
        gap(5),
        signal(1_700_000_000_900),
    ];
    let mut bodies: HashMap<usize, Vec<u8>> = HashMap::new();
    for (i, e) in events.iter().enumerate() {
        let row =
            vulture_server::history::Row::from_event(e, &format!("{}-{i}", 1_700_000_001_000u64))
                .unwrap();
        row.encode(bodies.entry(row.table().index()).or_default());
    }
    for t in Table::ALL {
        env.admin
            .insert_row_binary(
                t,
                bodies.remove(&t.index()).unwrap(),
                &format!("t-{}", t.name()),
            )
            .await
            .unwrap();
    }
    let l = &env
        .admin
        .query_rows("SELECT exchange, symbol, symbol_raw, toUnixTimestamp64Milli(exchange_ts) AS e, toUnixTimestamp64Milli(recv_ts) AS r, toString(side) AS side, price, qty, notional_usd, event_id FROM liquidations")
        .await
        .unwrap()[0];
    assert_eq!(l["exchange"], "binance");
    assert_eq!(l["symbol"], "BTC-USDT-PERP");
    assert_eq!(l["symbol_raw"], "BTCUSDT");
    assert_eq!(json_u64(&l["e"]), 1_700_000_000_480);
    assert_eq!(json_u64(&l["r"]), 1_700_000_000_500);
    assert_eq!(l["side"], "short");
    assert_eq!(l["price"].as_f64().unwrap(), 64_322.5);
    assert_eq!(l["notional_usd"].as_f64().unwrap(), 9_841.25);
    assert_eq!(l["event_id"], "1700000001000-0");

    let o = env
        .admin
        .query_rows("SELECT symbol, oi_usd, mark_price, oi_base FROM open_interest ORDER BY symbol")
        .await
        .unwrap();
    // SYM001 (odd): no oi_usd, no mark price; SYM002 (even): oi_usd, no mark price (2 % 3 != 0).
    assert_eq!(o[0]["symbol"], "SYM001-USDT-PERP");
    assert!(o[0]["oi_usd"].is_null() && o[0]["mark_price"].is_null());
    assert_eq!(o[1]["symbol"], "SYM002-USDT-PERP");
    assert_eq!(o[1]["oi_usd"].as_f64().unwrap(), 5_912_345_680.25);
    assert!(o[1]["mark_price"].is_null());

    let g = &env
        .admin
        .query_rows("SELECT exchange, stream, toUnixTimestamp64Milli(from_ts) AS f, toUnixTimestamp64Milli(to_ts) AS t FROM gaps")
        .await
        .unwrap()[0];
    assert_eq!(
        (g["exchange"].as_str(), g["stream"].as_str()),
        (Some("*"), Some("redis-writer"))
    );
    assert_eq!(json_u64(&g["f"]), 1_700_000_000_005);
    assert_eq!(json_u64(&g["t"]), 1_700_000_001_005);

    let s = &env
        .admin
        .query_rows("SELECT toString(type) AS type, window_secs, change_pct, threshold, toUnixTimestamp64Milli(ts) AS ts, details['symbols'] AS symbols, length(mapKeys(details)) AS n FROM signals")
        .await
        .unwrap()[0];
    assert_eq!(s["type"], "liq_cascade");
    assert_eq!(json_u64(&s["window_secs"]), 300);
    assert_eq!(s["change_pct"].as_f64().unwrap(), 850.5);
    assert_eq!(json_u64(&s["ts"]), 1_700_000_000_900);
    assert_eq!(s["symbols"], "12");
    assert_eq!(json_u64(&s["n"]), 2);
    env.cleanup().await;
}

#[tokio::test]
async fn redis_to_clickhouse_end_to_end_counts_and_values() {
    let Some(env) = Env::both("e2e").await else {
        return;
    };
    let store = env.store();
    let events = mixed(1_300);
    let ids = append_events(&store, &events).await;
    let want = |f: fn(&Event) -> bool| events.iter().filter(|e| f(e)).count() as u64;
    let wm = Arc::new(Watermark::new());
    let f = flusher(&env, env.client(), 400, wm.clone());
    let rows = f.cycle(None, None).await.unwrap();
    assert_eq!(rows, 1_300);
    assert_eq!(
        env.count("liquidations").await,
        want(|e| matches!(e.payload, Some(Payload::Liquidation(_))))
    );
    assert_eq!(
        env.count("open_interest").await,
        want(|e| matches!(e.payload, Some(Payload::OpenInterest(_))))
    );
    assert_eq!(
        env.count("gaps").await,
        want(|e| matches!(e.payload, Some(Payload::Gap(_))))
    );
    assert_eq!(
        env.count("signals").await,
        want(|e| matches!(e.payload, Some(Payload::Signal(_))))
    );
    // event_id is the Redis entry ID and unique.
    let all = env.count("liquidations").await
        + env.count("open_interest").await
        + env.count("gaps").await
        + env.count("signals").await;
    assert_eq!(all, 1_300);
    assert_eq!(
        env.scalar("SELECT uniqExact(event_id) FROM (SELECT event_id FROM liquidations UNION ALL SELECT event_id FROM open_interest UNION ALL SELECT event_id FROM gaps UNION ALL SELECT event_id FROM signals)")
            .await,
        1_300
    );
    // Field-level check of one OI row against the event it came from.
    let (i, e) = events
        .iter()
        .enumerate()
        .find(|(i, e)| matches!(e.payload, Some(Payload::OpenInterest(_))) && i % 6 == 0 && *i > 20)
        .unwrap();
    let Some(Payload::OpenInterest(o)) = &e.payload else {
        unreachable!()
    };
    let rows = env
        .admin
        .query_rows(&format!(
            "SELECT oi_base, oi_usd, mark_price, toUnixTimestamp64Milli(recv_ts) AS r FROM open_interest WHERE event_id = '{}'",
            ids[i]
        ))
        .await
        .unwrap();
    assert_eq!(rows[0]["oi_base"].as_f64().unwrap(), o.oi_base);
    assert_eq!(rows[0]["oi_usd"].as_f64(), o.oi_usd);
    assert_eq!(rows[0]["mark_price"].as_f64(), o.mark_price);
    assert_eq!(json_u64(&rows[0]["r"]) as i64, o.recv_ts);
    // The watermark is in Redis and in memory.
    assert_eq!(
        store.state_get(WATERMARK_KEY).await.unwrap().as_deref(),
        ids.last().map(String::as_str)
    );
    assert_eq!(wm.id(), *ids.last().unwrap());
    assert_eq!(store.state_get(INFLIGHT_KEY).await.unwrap(), None);
    env.cleanup().await;
}

/// Stores the block for real, then reports a failure: the process "died" before it learned about the success.
struct LoseTheAck {
    inner: Arc<ChClient>,
    armed: AtomicBool,
    stored_then_failed: AtomicUsize,
}

impl HistorySink for LoseTheAck {
    fn migrate(&self) -> BoxFuture<'_, Result<Vec<u32>, ChError>> {
        self.inner.migrate()
    }
    fn insert<'a>(
        &'a self,
        table: Table,
        body: Vec<u8>,
        token: &'a str,
    ) -> BoxFuture<'a, Result<(), ChError>> {
        Box::pin(async move {
            self.inner.insert(table, body, token).await?;
            if self.armed.load(Ordering::SeqCst) {
                self.stored_then_failed.fetch_add(1, Ordering::SeqCst);
                return Err(ChError::Connection("ack lost".into()));
            }
            Ok(())
        })
    }
    fn ping(&self) -> BoxFuture<'_, Result<(), ChError>> {
        self.inner.ping()
    }
}

#[tokio::test]
async fn a_crash_between_the_insert_and_the_watermark_leaves_no_duplicates() {
    let Some(env) = Env::both("crash").await else {
        return;
    };
    let store = env.store();
    let first = mixed(700);
    append_events(&store, &first).await;
    let sink = Arc::new(LoseTheAck {
        inner: env.client(),
        armed: AtomicBool::new(true),
        stored_then_failed: AtomicUsize::new(0),
    });
    let f = flusher(&env, sink.clone(), 10_000, Arc::new(Watermark::new()));
    assert!(f.cycle(None, None).await.is_err());
    assert!(sink.stored_then_failed.load(Ordering::SeqCst) >= 1);
    // Tables are inserted in order; the first one (liquidations) was stored before the "crash".
    let stored_before = env.count("liquidations").await;
    assert!(
        stored_before > 0,
        "the block was stored although the flusher never learned"
    );
    assert_eq!(store.state_get(WATERMARK_KEY).await.unwrap(), None);
    drop(f);

    // More events arrive; the process restarts (fresh in-memory state) and continues.
    append_events(&store, &mixed(300)).await;
    sink.armed.store(false, Ordering::SeqCst);
    let f2 = flusher(&env, env.client(), 10_000, Arc::new(Watermark::new()));
    f2.cycle(None, None).await.unwrap();

    let total = env.count("liquidations").await
        + env.count("open_interest").await
        + env.count("gaps").await
        + env.count("signals").await;
    assert_eq!(total, 1_000, "every entry exactly once");
    assert_eq!(
        env.scalar("SELECT uniqExact(event_id) FROM (SELECT event_id FROM liquidations UNION ALL SELECT event_id FROM open_interest UNION ALL SELECT event_id FROM gaps UNION ALL SELECT event_id FROM signals)")
            .await,
        1_000
    );
    env.cleanup().await;
}

/// Delegates to the real client unless the switch is on.
struct Switch {
    inner: Arc<ChClient>,
    down: Arc<AtomicBool>,
}

impl HistorySink for Switch {
    fn migrate(&self) -> BoxFuture<'_, Result<Vec<u32>, ChError>> {
        Box::pin(async move {
            if self.down.load(Ordering::SeqCst) {
                return Err(ChError::Connection("switched off".into()));
            }
            self.inner.migrate().await
        })
    }
    fn insert<'a>(
        &'a self,
        table: Table,
        body: Vec<u8>,
        token: &'a str,
    ) -> BoxFuture<'a, Result<(), ChError>> {
        Box::pin(async move {
            if self.down.load(Ordering::SeqCst) {
                return Err(ChError::Connection("switched off".into()));
            }
            self.inner.insert(table, body, token).await
        })
    }
    fn ping(&self) -> BoxFuture<'_, Result<(), ChError>> {
        Box::pin(async move {
            if self.down.load(Ordering::SeqCst) {
                return Err(ChError::Connection("switched off".into()));
            }
            self.inner.ping().await
        })
    }
}

async fn wait_until(what: &str, mut ok: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ok().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn an_outage_keeps_the_watermark_and_the_catch_up_after_recovery_is_complete() {
    let Some(env) = Env::both("outage").await else {
        return;
    };
    let store = env.store();
    env.migrate().await;
    let down = Arc::new(AtomicBool::new(true));
    let sink = Arc::new(Switch {
        inner: env.client(),
        down: down.clone(),
    });
    let wm = Arc::new(Watermark::new());
    let f = flusher(&env, sink, 250, wm.clone());
    let status = f.status();
    let stop = CancellationToken::new();
    let task = tokio::spawn(f.run(stop.clone()));

    append_events(&store, &mixed(900)).await;
    wait_until("failures to be counted", async || {
        status.failures_insert.load(Ordering::Relaxed)
            + status.failures_migrate.load(Ordering::Relaxed)
            >= 3
    })
    .await;
    assert_eq!(wm.id(), "0-0");
    assert_eq!(env.count("open_interest").await, 0);
    wait_until("the down state", async || {
        status.ch_state() == ChState::Down
    })
    .await;

    // More data piles up during the outage; then ClickHouse returns.
    append_events(&store, &mixed(900)).await;
    down.store(false, Ordering::SeqCst);
    wait_until("the catch-up", async || {
        env.count("liquidations").await
            + env.count("open_interest").await
            + env.count("gaps").await
            + env.count("signals").await
            == 1_800
    })
    .await;
    wait_until("the up state", async || status.ch_state() == ChState::Up).await;
    assert!(status.lag_seconds(now_ms()) < 5.0);
    stop.cancel();
    task.await.unwrap();
    assert_eq!(
        env.count("liquidations").await
            + env.count("open_interest").await
            + env.count("gaps").await
            + env.count("signals").await,
        1_800,
        "nothing repeated"
    );
    env.cleanup().await;
}

#[tokio::test]
async fn a_backlog_larger_than_one_page_and_the_watermark_across_a_restart() {
    let Some(env) = Env::both("pages").await else {
        return;
    };
    let store = env.store();
    let events = mixed(1_050);
    let ids = append_events(&store, &events).await;
    let sink = env.client();
    let f = flusher(&env, sink.clone(), 100, Arc::new(Watermark::new()));
    assert_eq!(f.cycle(None, None).await.unwrap(), 1_050);
    let inserts: u64 = Table::ALL
        .iter()
        .map(|t| f.status().batches[t.index()].load(Ordering::Relaxed))
        .sum();
    assert!(
        inserts >= 11,
        "11 pages, at least one insert each: {inserts}"
    );

    // Restart: a new flusher with a fresh watermark object loads the position from Redis and has nothing to do.
    let wm2 = Arc::new(Watermark::new());
    let f2 = flusher(&env, sink.clone(), 100, wm2.clone());
    assert_eq!(f2.cycle(None, None).await.unwrap(), 0);
    assert_eq!(wm2.id(), *ids.last().unwrap());
    // New entries after the restart are picked up from the persisted position only.
    append_events(&store, &mixed(30)).await;
    assert_eq!(f2.cycle(None, None).await.unwrap(), 30);
    let total = env.count("liquidations").await
        + env.count("open_interest").await
        + env.count("gaps").await
        + env.count("signals").await;
    assert_eq!(total, 1_080);
    env.cleanup().await;
}

#[tokio::test]
async fn redis_is_not_trimmed_past_the_watermark_and_metrics_move() {
    let Some(env) = Env::both("trim").await else {
        return;
    };
    let store = env.store();
    let wm = Arc::new(Watermark::new());
    let mut wcfg = WriterConfig {
        window: Duration::from_millis(200),
        flush_interval: Duration::from_millis(10),
        shutdown_timeout: Duration::from_secs(5),
        log_interval: Duration::from_secs(3600),
        ..WriterConfig::default()
    };
    wcfg.watermark = Some(wm.clone());
    let writer = RedisWriter::new(store.clone(), wcfg, Arc::new(SystemClock));
    let (tx, rx) = mpsc::channel(1_000);
    let shutdown = CancellationToken::new();
    let writer_task = tokio::spawn(writer.run(rx, shutdown.clone()));

    // Entries older than the 200 ms window, never flushed: they must all stay however many writes follow.
    for i in 0..30 {
        tx.send(oi(now_ms(), i)).await.unwrap();
    }
    sleep(Duration::from_millis(600)).await;
    for i in 30..60 {
        tx.send(oi(now_ms(), i)).await.unwrap();
        sleep(Duration::from_millis(15)).await;
    }
    assert_eq!(store.stream_len().await.unwrap(), 60);
    assert_eq!(wm.id(), "0-0");

    // Flush: now the watermark is at the newest entry and the metrics reflect it.
    let status = Arc::new(FlusherStatus::new(wm.clone(), 5_000_000, now_ms()));
    let f = Flusher::new(
        store.clone(),
        env.client(),
        flusher_cfg(1_000),
        wm.clone(),
        status.clone(),
        Arc::new(SystemClock),
    );
    assert_eq!(f.cycle(None, None).await.unwrap(), 60);
    f.update_stats().await;
    let mut fam = vulture_server::metrics::Families::new();
    FlusherMetrics(status.clone()).collect(&mut fam);
    let text = fam.render();
    assert!(
        text.contains("vulture_flusher_rows_inserted_total{table=\"open_interest\"} 60"),
        "{text}"
    );
    assert!(text.contains("vulture_flusher_clickhouse_up 1"), "{text}");
    assert!(
        text.contains("vulture_flusher_last_success_age_seconds"),
        "{text}"
    );
    assert!(
        text.contains("vulture_flusher_watermark_timestamp_seconds"),
        "{text}"
    );
    assert!(text.contains("vulture_flusher_backlog_entries"), "{text}");
    assert!(
        text.contains("vulture_flusher_watermark_lag_seconds"),
        "{text}"
    );
    // The unflushed entries never went below the watermark: the stream is intact.
    assert_eq!(store.stream_len().await.unwrap(), 60);
    shutdown.cancel();
    writer_task.await.unwrap();
    env.cleanup().await;
}

#[tokio::test]
async fn the_monitor_reports_disks_and_tables() {
    let Some(env) = Env::ch("monitor").await else {
        return;
    };
    env.migrate().await;
    let mut body = Vec::new();
    vulture_server::history::Row::from_event(&gap(1), "1-0")
        .unwrap()
        .encode(&mut body);
    env.admin
        .insert_row_binary(Table::Gaps, body, "m1")
        .await
        .unwrap();
    let monitor = ClickHouseMonitor::new();
    monitor.sample(&env.admin).await.unwrap();
    let s = monitor.last().unwrap();
    let disk = s
        .disks
        .iter()
        .find(|d| d.name == "default")
        .expect("default disk");
    assert!(disk.total > 0 && disk.free > 0 && disk.free <= disk.total);
    let gaps = s.tables.iter().find(|t| t.name == "gaps").unwrap();
    assert_eq!(
        (gaps.rows, gaps.parts, gaps.max_parts_per_partition),
        (1, 1, 1)
    );
    assert!(gaps.bytes > 0);
    assert_eq!(s.tables.len(), 4);
    let mut fam = vulture_server::metrics::Families::new();
    monitor.collect(&mut fam);
    let text = fam.render();
    assert!(
        text.contains("vulture_clickhouse_disk_free_bytes{disk=\"default\"}"),
        "{text}"
    );
    assert!(
        text.contains("vulture_clickhouse_table_parts{table=\"gaps\"} 1"),
        "{text}"
    );
    env.cleanup().await;
}

/// Emits a fixed list of events, then waits for shutdown.
struct Scripted(Vec<Event>);

impl Exchange for Scripted {
    fn name(&self) -> &str {
        "binance"
    }

    fn run<'a>(
        &'a self,
        ctx: AppContext,
        sink: EventSink,
    ) -> BoxFuture<'a, Result<(), ExchangeError>> {
        Box::pin(async move {
            for e in &self.0 {
                sink.send(e.clone())
                    .await
                    .map_err(|_| ExchangeError::SinkClosed("binance".into()))?;
            }
            ctx.shutdown.cancelled().await;
            Ok(())
        })
    }
}

#[tokio::test]
async fn the_pipeline_flushes_on_its_own_and_once_more_at_shutdown() {
    let Some(env) = Env::both("pipeline").await else {
        return;
    };
    let mut config = Config::default();
    let (_, key) = env.redis.clone().unwrap();
    config.redis.stream_key = key;
    config.redis.flush_interval_ms = 20;
    let mut ch = env.cfg.clone();
    ch.flush_interval_secs = 1;
    config.clickhouse = Some(ch.clone());
    let token = CancellationToken::new();
    let ctx = AppContext::new(config, token.clone());
    let events = mixed(200);
    let connector: Box<dyn Exchange> = Box::new(Scripted(events.clone()));
    let watermark = Arc::new(Watermark::new());
    let client = Arc::new({
        let mut c = ChClient::new(&ch, env.password.clone()).unwrap();
        let _ = &mut c;
        c
    });
    let hub = Arc::new(MetricsHub::new());
    let status = Arc::new(FlusherStatus::new(watermark.clone(), 5_000_000, now_ms()));
    let setup = HistorySetup {
        sink: client.clone(),
        flusher: FlusherConfig {
            interval: Duration::from_secs(1),
            ..flusher_cfg(10_000)
        },
        watermark,
        status,
        monitor: Some((client, Duration::from_millis(500))),
    };
    let handle = tokio::spawn(run_with_history(
        ctx,
        env.store(),
        vec![connector],
        Some(hub.clone()),
        Some(setup),
    ));
    // Within one interval the rows are in ClickHouse.
    wait_until("the first flush", async || {
        env.admin
            .query_rows("SELECT count() AS c FROM open_interest")
            .await
            .map(|r| json_u64(&r[0]["c"]) > 0)
            .unwrap_or(false)
    })
    .await;
    wait_until("the monitor sample", async || {
        hub.render().contains("vulture_clickhouse_disk_free_bytes")
    })
    .await;
    let text = hub.render();
    assert!(
        text.contains("vulture_flusher_rows_inserted_total"),
        "{text}"
    );

    // Events that arrive just before SIGTERM are flushed by the final flush, not by the next interval.
    let store = env.store();
    let late = append_events(&store, &[liq(now_ms(), 7), liq(now_ms(), 8)]).await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("clean stop")
        .unwrap();
    let total = env.count("liquidations").await
        + env.count("open_interest").await
        + env.count("gaps").await
        + env.count("signals").await;
    assert_eq!(total, 202);
    assert_eq!(
        env.scalar(&format!(
            "SELECT count() FROM liquidations WHERE event_id IN ('{}', '{}')",
            late[0], late[1]
        ))
        .await,
        2
    );
    env.cleanup().await;
}

/// Needs no server: the password never appears in errors or in `Debug`.
#[tokio::test]
async fn client_errors_do_not_leak_the_password() {
    let cfg = ClickHouseConfig {
        url: "http://127.0.0.1:1".into(),
        password: Some(Secret::new("topsecretpw")),
        ..ClickHouseConfig::default()
    };
    let client = build_client(&cfg).unwrap();
    let err = client.ping().await.unwrap_err();
    let text = format!("{err} {err:?} {cfg:?}");
    assert!(!text.contains("topsecretpw"), "{text}");
}
