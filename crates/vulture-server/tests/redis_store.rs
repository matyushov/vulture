//! Tests of the Redis storage and the writer against a real Redis server.
//!
//! They need `VULTURE_TEST_REDIS_URL` (for example `redis://127.0.0.1:6379`) and skip with a message when it
//! is not set. Every test uses its own stream key and deletes it at the end. The tests are serialized because
//! one of them kills every other client connection of the server (`CLIENT KILL`). Use a throwaway Redis
//! instance: `docker run -d --rm -p 127.0.0.1:6379:6379 redis:7`.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::{Event, Gap, Liquidation, OpenInterest, Side, event::Payload};
use vulture_server::app::{AppContext, run_with_connectors};
use vulture_server::config::{Config, Secret};
use vulture_server::exchange::{EventSink, Exchange, ExchangeError};
use vulture_server::store::{
    EntryFields, RedisClientStore, RedisStore, RedisWriter, SettingsReport, StoreError, StoreState,
    Trim, WriterConfig, check_settings,
};
use vulture_server::ws::{Clock, SystemClock};

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn redis_url() -> Option<String> {
    match std::env::var("VULTURE_TEST_REDIS_URL") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!("SKIPPED: VULTURE_TEST_REDIS_URL is not set (no Redis server for this test)");
            None
        }
    }
}

/// A stream key private to one test run.
fn unique_key(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("vulture:test:{name}:{}:{nanos}", std::process::id())
}

struct Env {
    key: String,
    store: Arc<RedisClientStore>,
    admin: redis::aio::MultiplexedConnection,
}

impl Env {
    async fn new(name: &str) -> Option<Self> {
        let url = redis_url()?;
        let key = unique_key(name);
        let store = Arc::new(RedisClientStore::new(&Secret::new(url.clone()), &key).unwrap());
        let admin = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .expect("cannot connect to VULTURE_TEST_REDIS_URL");
        Some(Self { key, store, admin })
    }

    async fn cmd<T: redis::FromRedisValue>(&self, cmd: &mut redis::Cmd) -> T {
        let mut conn = self.admin.clone();
        cmd.query_async(&mut conn).await.unwrap()
    }

    async fn cleanup(&self) {
        let _: i64 = self.cmd(redis::cmd("DEL").arg(&self.key)).await;
    }

    async fn all(&self) -> Vec<vulture_server::store::StoredEntry> {
        self.store.read_range("-", "+", None).await.unwrap()
    }
}

fn now_ms() -> i64 {
    SystemClock.now_ms()
}

fn liq(ts: i64) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::Liquidation(Liquidation {
            exchange: "binance".into(),
            symbol_raw: "BTCUSDT".into(),
            symbol: "BTC-USDT-PERP".into(),
            exchange_ts: ts - 20,
            recv_ts: ts,
            side: Side::Long as i32,
            price: 64_321.5,
            qty: 0.153,
            notional_usd: 9_841.1,
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
            oi_usd: Some(5_912_345_678.25 + n as f64),
            mark_price: Some(64_295.1),
        })),
    }
}

fn fast_cfg(window: Duration) -> WriterConfig {
    WriterConfig {
        window,
        maxlen: 1_000_000,
        batch_size: 500,
        flush_interval: Duration::from_millis(20),
        buffer_limit: 100_000,
        retry_min: Duration::from_millis(200),
        retry_max: Duration::from_secs(1),
        shutdown_timeout: Duration::from_secs(5),
        log_interval: Duration::from_secs(3600),
        watermark: None,
    }
}

fn start_writer(
    store: Arc<RedisClientStore>,
    cfg: WriterConfig,
) -> (
    mpsc::Sender<Event>,
    CancellationToken,
    Arc<vulture_server::store::WriterStatus>,
    tokio::task::JoinHandle<()>,
) {
    let writer = RedisWriter::new(store, cfg, Arc::new(SystemClock));
    let status = writer.status();
    let (tx, rx) = mpsc::channel(10_000);
    let token = CancellationToken::new();
    let handle = tokio::spawn(writer.run(rx, token.clone()));
    (tx, token, status, handle)
}

#[tokio::test]
async fn events_written_by_the_writer_read_back_with_ids_and_fields() {
    let _guard = SERIAL.lock().await;
    let Some(env) = Env::new("roundtrip").await else {
        return;
    };
    let (tx, _token, status, handle) =
        start_writer(env.store.clone(), fast_cfg(Duration::from_secs(86_400)));
    let gap = Event {
        id: String::new(),
        payload: Some(Payload::Gap(Gap {
            exchange: "binance".into(),
            stream: "liquidations".into(),
            from_ts: 5,
            to_ts: 6,
        })),
    };
    let sent = vec![liq(now_ms()), oi(now_ms(), 1), gap, oi(now_ms(), 2)];
    for e in &sent {
        tx.send(e.clone()).await.unwrap();
    }
    drop(tx);
    handle.await.unwrap();

    let entries = env.all().await;
    assert_eq!(entries.len(), 4);
    let kinds: Vec<(&str, &str, &str)> = entries
        .iter()
        .map(|e| (e.kind.as_str(), e.exchange.as_str(), e.symbol.as_str()))
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("liq", "binance", "BTC-USDT-PERP"),
            ("oi", "binance", "SYM001-USDT-PERP"),
            ("gap", "binance", ""),
            ("oi", "binance", "SYM002-USDT-PERP"),
        ]
    );
    for (entry, original) in entries.iter().zip(&sent) {
        assert!(entry.id.contains('-'), "Redis entry ID, got {:?}", entry.id);
        let decoded = entry.clone().into_event().unwrap();
        assert_eq!(decoded.id, entry.id, "Event.id is the Redis entry ID");
        assert_eq!(decoded.payload, original.payload);
    }
    // Range reads: from an ID (exclusive start through "(" is not used; the reader adds 1 itself).
    let tail = env
        .store
        .read_range(&entries[2].id, "+", None)
        .await
        .unwrap();
    assert_eq!(tail.len(), 2);
    assert_eq!(
        env.store.read_range("-", "+", Some(1)).await.unwrap().len(),
        1
    );
    assert_eq!(env.store.stream_len().await.unwrap(), 4);
    env.store.health().await.unwrap();
    let s = status.snapshot();
    assert_eq!((s.written, s.state), (4, StoreState::Connected));
    env.cleanup().await;
}

#[tokio::test]
async fn minid_trimming_drops_entries_older_than_the_window() {
    let _guard = SERIAL.lock().await;
    let Some(env) = Env::new("minid").await else {
        return;
    };
    // Window of one second: after two seconds everything written first is out of the window. Trimming is
    // approximate (whole internal nodes of up to 100 entries), so 300 old entries are three full nodes.
    let (tx, _token, _status, handle) =
        start_writer(env.store.clone(), fast_cfg(Duration::from_secs(1)));
    for i in 0..300 {
        tx.send(oi(now_ms(), i)).await.unwrap();
    }
    sleep(Duration::from_millis(500)).await;
    assert_eq!(env.store.stream_len().await.unwrap(), 300);
    sleep(Duration::from_millis(2000)).await;
    tx.send(liq(now_ms())).await.unwrap();
    drop(tx);
    handle.await.unwrap();
    let left = env.store.stream_len().await.unwrap();
    eprintln!("MINID: 300 old entries + 1 new, window 1 s -> {left} entries left");
    assert!(left <= 101, "old entries must be trimmed, {left} left");
    assert!(left >= 1);
    let entries = env.all().await;
    assert_eq!(
        entries.last().unwrap().kind,
        "liq",
        "the newest entry survives"
    );
    env.cleanup().await;
}

#[tokio::test]
async fn maxlen_safeguard_bounds_the_stream() {
    let _guard = SERIAL.lock().await;
    let Some(env) = Env::new("maxlen").await else {
        return;
    };
    let trim = Trim {
        min_id_ms: 0,
        maxlen: 100,
    };
    for batch in 0..10 {
        let entries: Vec<EntryFields> = (0..100)
            .map(|i| EntryFields::from_event(&oi(now_ms(), batch * 100 + i)).unwrap())
            .collect();
        env.store.append(&entries, trim).await.unwrap();
    }
    let len = env.store.stream_len().await.unwrap();
    eprintln!("MAXLEN ~ 100 after 1000 entries -> {len} entries");
    assert!(
        (100..=200).contains(&len),
        "approximate trimming keeps 100..200, got {len}"
    );
    // The newest entries are the ones kept.
    let last = env.all().await.pop().unwrap().into_event().unwrap();
    let Some(Payload::OpenInterest(o)) = last.payload else {
        panic!("expected open interest");
    };
    assert_eq!(o.symbol_raw, "SYM999USDT");
    env.cleanup().await;
}

#[tokio::test]
async fn the_writer_reconnects_after_its_connection_is_killed() {
    let _guard = SERIAL.lock().await;
    let Some(env) = Env::new("reconnect").await else {
        return;
    };
    let (tx, _token, status, handle) =
        start_writer(env.store.clone(), fast_cfg(Duration::from_secs(86_400)));
    tx.send(liq(1)).await.unwrap();
    wait_for(|| status.snapshot().written == 1).await;

    // Kill every other client connection of the server, including the writer's.
    let killed: i64 = env
        .cmd(
            redis::cmd("CLIENT")
                .arg("KILL")
                .arg("TYPE")
                .arg("normal")
                .arg("SKIPME")
                .arg("yes"),
        )
        .await;
    eprintln!("CLIENT KILL closed {killed} connection(s)");
    assert!(killed >= 1, "the writer must have had a connection");

    for ts in 2..=20 {
        tx.send(liq(ts)).await.unwrap();
        sleep(Duration::from_millis(30)).await;
    }
    drop(tx);
    tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("the writer must finish after recovering")
        .unwrap();
    let s = status.snapshot();
    eprintln!("after the kill: retries {}, state {:?}", s.retries, s.state);
    assert_eq!(s.dropped, 0);
    assert_eq!(s.state, StoreState::Connected);
    // At-least-once: every event is present (a retried batch may repeat).
    let mut seen: Vec<i64> = env
        .all()
        .await
        .into_iter()
        .map(|e| match e.into_event().unwrap().payload {
            Some(Payload::Liquidation(l)) => l.recv_ts,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen, (1..=20).collect::<Vec<_>>());
    env.cleanup().await;
}

async fn wait_for(mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !cond() {
        assert!(Instant::now() < deadline, "condition not met in time");
        sleep(Duration::from_millis(20)).await;
    }
}

/// Fills a stream through the real store and returns `(entries, MEMORY USAGE bytes, INFO delta bytes)`.
async fn fill_and_measure(env: &Env, events: Vec<Event>) -> (usize, u64, i64) {
    let used = |info: String| -> i64 {
        info.lines()
            .find_map(|l| l.strip_prefix("used_memory:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    };
    let before = used(env.cmd(redis::cmd("INFO").arg("memory")).await);
    let trim = Trim {
        min_id_ms: 0,
        maxlen: 10_000_000,
    };
    for chunk in events.chunks(500) {
        let entries: Vec<EntryFields> = chunk
            .iter()
            .map(|e| EntryFields::from_event(e).unwrap())
            .collect();
        env.store.append(&entries, trim).await.unwrap();
    }
    let after = used(env.cmd(redis::cmd("INFO").arg("memory")).await);
    let usage: u64 = env
        .cmd(
            redis::cmd("MEMORY")
                .arg("USAGE")
                .arg(&env.key)
                .arg("SAMPLES")
                .arg(0),
        )
        .await;
    (events.len(), usage, after - before)
}

#[tokio::test]
async fn stored_size_per_entry_is_reported() {
    let _guard = SERIAL.lock().await;
    let Some(env) = Env::new("size").await else {
        return;
    };
    let n = 20_000usize;
    let ts = now_ms();

    let oi_events: Vec<Event> = (0..n).map(|i| oi(ts + i as i64, i % 560)).collect();
    let liq_events: Vec<Event> = (0..n).map(|i| liq(ts + i as i64)).collect();
    // The expected mix: about 20 open interest events per second and about 1 liquidation per second.
    let mix_events: Vec<Event> = (0..n)
        .map(|i| {
            if i % 21 == 20 {
                liq(ts + i as i64)
            } else {
                oi(ts + i as i64, i % 560)
            }
        })
        .collect();

    let mut report = Vec::new();
    for (name, events) in [("oi", oi_events), ("liq", liq_events), ("mix", mix_events)] {
        env.cleanup().await;
        let payload_avg = events
            .iter()
            .map(|e| EntryFields::from_event(e).unwrap().data.len())
            .sum::<usize>() as f64
            / events.len() as f64;
        let (count, usage, delta) = fill_and_measure(&env, events).await;
        let per_usage = usage as f64 / count as f64;
        let per_delta = delta as f64 / count as f64;
        eprintln!(
            "SIZE {name}: {count} entries, protobuf `d` {payload_avg:.1} B avg, \
             MEMORY USAGE {usage} B = {per_usage:.1} B/entry, used_memory delta {delta} B = {per_delta:.1} B/entry"
        );
        assert!((30.0..1000.0).contains(&per_usage), "{name}: {per_usage}");
        report.push((name, per_usage));
    }
    let mix = report.iter().find(|(n, _)| *n == "mix").unwrap().1;
    let per_day = 21.0 * 86_400.0 * mix / (1024.0 * 1024.0);
    eprintln!("SIZE estimate: 21 events/s for 24 h at {mix:.1} B/entry = {per_day:.0} MiB");
    env.cleanup().await;
}

#[tokio::test]
async fn server_settings_can_be_checked() {
    let _guard = SERIAL.lock().await;
    let Some(env) = Env::new("settings").await else {
        return;
    };
    match check_settings(env.store.as_ref()).await {
        SettingsReport::Checked(diffs) => {
            eprintln!("settings that differ from the recommended ones: {diffs:?}");
        }
        other => panic!("CONFIG GET should work on the test server: {other:?}"),
    }
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
async fn the_server_pipeline_writes_connector_events_and_stops_cleanly() {
    let _guard = SERIAL.lock().await;
    let Some(env) = Env::new("pipeline").await else {
        return;
    };
    let mut config = Config::default();
    config.redis.stream_key = env.key.clone();
    config.redis.flush_interval_ms = 20;
    let token = CancellationToken::new();
    let ctx = AppContext::new(config, token.clone());
    let events: Vec<Event> = (0..50)
        .map(|i| {
            if i % 10 == 0 {
                liq(now_ms())
            } else {
                oi(now_ms(), i)
            }
        })
        .collect();
    let connector: Box<dyn Exchange> = Box::new(Scripted(events.clone()));
    let handle = tokio::spawn(run_with_connectors(ctx, env.store.clone(), vec![connector]));
    wait_for_len(&env, 50).await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("clean stop")
        .unwrap();
    let got: Vec<Option<Payload>> = env
        .all()
        .await
        .into_iter()
        .map(|e| e.into_event().unwrap().payload)
        .collect();
    let want: Vec<Option<Payload>> = events.into_iter().map(|e| e.payload).collect();
    assert_eq!(got, want, "same events in the same order");
    env.cleanup().await;
}

async fn wait_for_len(env: &Env, n: u64) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while env.store.stream_len().await.unwrap() < n {
        assert!(
            Instant::now() < deadline,
            "stream did not reach {n} entries"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

/// Needs no server: a closed local port and a password that must never appear in an error.
#[tokio::test]
async fn connection_errors_do_not_leak_the_url() {
    let url = Secret::new("redis://:topsecretpw@127.0.0.1:1/");
    let store = RedisClientStore::new(&url, "k").unwrap();
    let err = store.health().await.unwrap_err();
    assert!(matches!(err, StoreError::Connection(_)), "{err:?}");
    let text = format!("{err} {err:?}");
    assert!(!text.contains("topsecretpw"), "{text}");
    let entry = EntryFields::from_event(&liq(1)).unwrap();
    let err = store
        .append(
            std::slice::from_ref(&entry),
            Trim {
                min_id_ms: 0,
                maxlen: 10,
            },
        )
        .await
        .unwrap_err();
    assert!(!format!("{err}").contains("topsecretpw"));
}

/// Needs no server: the writer keeps running and buffering while Redis is unreachable.
#[tokio::test]
async fn the_writer_survives_an_unreachable_server() {
    let store = Arc::new(RedisClientStore::new(&Secret::new("redis://127.0.0.1:1/"), "k").unwrap());
    let (tx, token, status, handle) = start_writer(store, fast_cfg(Duration::from_secs(60)));
    tx.send(liq(1)).await.unwrap();
    wait_for(|| status.snapshot().retries >= 2).await;
    let s = status.snapshot();
    assert_eq!(s.state, StoreState::Disconnected);
    assert_eq!(s.buffered, 1);
    assert!(s.last_error.is_some());
    token.cancel();
    drop(tx);
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("stops within the shutdown timeout")
        .unwrap();
}
