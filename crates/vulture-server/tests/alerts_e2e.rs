//! End to end on the memory store: events in, a `Signal` out through the gRPC stream, a message at the mock
//! channel; the engine never delays or drops what the writer receives.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::mock_http::{MockHttp, Reply};
use common::*;
use futures_util::future::BoxFuture;
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::event::Payload;
use vulture_proto::v1::{Event, EventType, Liquidation, OpenInterest, Side, SignalType};
use vulture_server::alerts::model::{ChannelDef, ChannelKind, RuleDef, RuleKind, Severity};
use vulture_server::alerts::{Alerts, EngineInput, EngineStats};
use vulture_server::app::{AppContext, run_with_alerts};
use vulture_server::config::{Config, Secret};
use vulture_server::exchange::{EventSink, Exchange, ExchangeError};
use vulture_server::ws::SystemClock;

/// Polls an async condition for up to ten seconds (stores answer over the network).
async fn eventually<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !condition().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

const TOKEN: &str = "123456:SECRETtokenABCdef_ghi-JKL";

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

fn oi(symbol: &str, ts: i64, base: f64) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::OpenInterest(OpenInterest {
            exchange: "binance".into(),
            symbol_raw: symbol.replace('-', ""),
            symbol: symbol.into(),
            exchange_ts: ts,
            recv_ts: ts,
            oi_base: base,
            oi_usd: Some(base * 1000.0),
            mark_price: Some(1000.0),
            ..OpenInterest::default()
        })),
    }
}

fn liq(ts: i64, usd: f64) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::Liquidation(Liquidation {
            exchange: "binance".into(),
            symbol_raw: "ETHUSDT".into(),
            symbol: "ETH-USDT-PERP".into(),
            exchange_ts: ts,
            recv_ts: ts,
            side: Side::Long as i32,
            price: 2500.0,
            qty: usd / 2500.0,
            notional_usd: usd,
        })),
    }
}

fn rules() -> Vec<RuleDef> {
    vec![
        RuleDef {
            id: "big-liq".into(),
            name: "Big liquidations".into(),
            enabled: true,
            kind: RuleKind::Liquidation {
                min_notional_usd: 200_000.0,
                sides: vec![],
            },
            exchanges: vec![],
            symbols: vec![],
            channels: vec!["tg".into()],
            cooldown_secs: 0,
            quiet_hours: Vec::new(),
        },
        RuleDef {
            id: "signals".into(),
            name: "All signals".into(),
            enabled: true,
            kind: RuleKind::Signal {
                types: vec![],
                min_severity: Severity::Info,
            },
            exchanges: vec![],
            symbols: vec![],
            channels: vec!["tg".into()],
            cooldown_secs: 0,
            quiet_hours: Vec::new(),
        },
    ]
}

fn channel() -> ChannelDef {
    ChannelDef {
        id: "tg".into(),
        name: "Telegram".into(),
        kind: ChannelKind::Telegram,
        enabled: true,
        system: false,
        dry_run: false,
        chat_id: "-100".into(),
        secret: Some(Secret::new(TOKEN)),
        secret_file: String::new(),
        secret_env: String::new(),
        min_interval_ms: 0,
        batch_window_ms: 0,
        queue_limit: 0,
        quiet_hours: Vec::new(),
    }
}

fn config(dir: &std::path::Path, mock: &MockHttp) -> Config {
    let mut cfg = Config::default();
    cfg.server.data_dir = dir.to_path_buf();
    cfg.alerts.warmup_secs = 0;
    cfg.alerts.delivery.telegram_base_url = mock.url();
    cfg.alerts.delivery.batch_window_ms = 100;
    cfg.alerts.delivery.telegram_min_interval_ms = 100;
    cfg.alerts.liq_cascade.enabled = false;
    cfg.redis.flush_interval_ms = 10;
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spike_and_liquidation_reach_the_stream_and_the_channel() {
    let mock = MockHttp::start(Reply::ok()).await;
    let data = tempfile::tempdir().unwrap();
    let cfg = config(data.path(), &mock);
    let backend = Backend::memory();
    let store = backend.store();
    let stop = CancellationToken::new();
    let alerts = Alerts::open(&cfg, stop.clone()).unwrap();
    let hub = vulture_server::metrics::MetricsHub::new();
    alerts.register_metrics(&hub);
    alerts
        .manager
        .upsert_channel(channel(), TOKEN, "e2e")
        .await
        .unwrap();
    for r in rules() {
        alerts.manager.upsert_rule(r, "e2e").await.unwrap();
    }
    let server = Server::start_all(
        &backend,
        test_stream_config(),
        Default::default(),
        None,
        Some(alerts.manager.clone()),
    )
    .await;
    let mut stream = server
        .client()
        .await
        .subscribe(filtered(&[], &[], &[EventType::Signal], ""))
        .await
        .unwrap()
        .into_inner();

    // 300 flat OI samples, then +8% twice; one big liquidation.
    let t0 = 1_760_000_000_000;
    let mut events: Vec<Event> = (0..300)
        .map(|i| oi("BTC-USDT-PERP", t0 + i * 30_000, 10_000.0))
        .collect();
    events.push(oi("BTC-USDT-PERP", t0 + 300 * 30_000, 10_800.0));
    events.push(oi("BTC-USDT-PERP", t0 + 301 * 30_000, 10_800.0));
    events.push(liq(t0 + 301 * 30_000 + 1, 450_000.0));
    let total = events.len();
    let ctx = AppContext::new(cfg.clone(), stop.clone());
    let app = tokio::spawn(run_with_alerts(
        ctx,
        store.clone(),
        vec![Box::new(Scripted(events))],
        None,
        None,
        Some(alerts.clone()),
    ));

    let got = recv(&mut stream, 1).await;
    let Some(Payload::Signal(sig)) = &got[0].payload else {
        panic!("not a signal: {:?}", got[0]);
    };
    assert_eq!(sig.r#type, SignalType::OiSpikeUp as i32);
    assert_eq!(sig.exchange, "binance");
    assert_eq!(sig.symbol, "BTC-USDT-PERP");
    assert!(sig.change_pct > 7.9 && sig.change_pct < 8.1, "{sig:?}");
    assert!(sig.details.contains_key("key") && sig.details.contains_key("windows"));
    assert!(
        !got[0].id.is_empty(),
        "the signal went through the writer and has a stream id"
    );

    // The mock channel got the signal and the liquidation (batched into a digest or separately).
    wait_until("messages at the mock channel", || {
        let texts: String = mock
            .requests()
            .iter()
            .map(|r| r.json()["text"].as_str().unwrap_or("").to_string())
            .collect();
        texts.contains("OI spike UP") && texts.contains("Liquidation")
    })
    .await;
    let texts: Vec<String> = mock
        .requests()
        .iter()
        .map(|r| r.json()["text"].as_str().unwrap_or("").to_string())
        .collect();
    assert!(texts.iter().all(|t| !t.contains(TOKEN)));
    assert!(
        texts
            .iter()
            .any(|t| t.contains("BTC-USDT-PERP") && t.contains("+8.00%")),
        "{texts:?}"
    );
    assert!(
        texts
            .iter()
            .any(|t| t.contains("ETH-USDT-PERP") && t.contains("$450K")),
        "{texts:?}"
    );

    // Everything the connector produced reached the stream, plus the signal.
    eventually("the writer wrote everything", async || {
        store.stream_len().await.is_ok_and(|n| n as usize > total)
    })
    .await;
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(10), app)
        .await
        .unwrap()
        .unwrap();
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_starved_engine_never_costs_the_writer_an_event() {
    let mock = MockHttp::start(Reply::ok()).await;
    let data = tempfile::tempdir().unwrap();
    let mut cfg = config(data.path(), &mock);
    cfg.alerts.input_queue = 1; // the engine can hold one event: it must drop its own input, not the writer's
    let backend = Backend::memory();
    let store = backend.store();
    let stop = CancellationToken::new();
    let alerts = Alerts::open(&cfg, stop.clone()).unwrap();
    let n = 30_000usize;
    let t0 = 1_760_000_000_000;
    let events: Vec<Event> = (0..n)
        .map(|i| {
            oi(
                &format!("S{}-USDT-PERP", i % 500),
                t0 + (i / 500) as i64 * 30_000 + (i % 500) as i64,
                1000.0 + i as f64,
            )
        })
        .collect();
    let ctx = AppContext::new(cfg, stop.clone());
    let app = tokio::spawn(run_with_alerts(
        ctx,
        store.clone(),
        vec![Box::new(Scripted(events))],
        None,
        None,
        Some(alerts.clone()),
    ));
    eventually("the writer wrote every event", async || {
        store.stream_len().await.is_ok_and(|len| len as usize >= n)
    })
    .await;
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(10), app)
        .await
        .unwrap()
        .unwrap();
    let len = store.stream_len().await.unwrap() as usize;
    assert!(len >= n, "the writer lost events: {len} < {n}");
}

#[tokio::test]
async fn the_tap_never_blocks_and_counts_what_it_drops() {
    let stats = Arc::new(EngineStats::default());
    let (input, _rx_kept_but_never_read) =
        EngineInput::channel(1, stats.clone(), Arc::new(SystemClock));
    let started = std::time::Instant::now();
    for i in 0..10_000 {
        input.offer(&oi("BTC-USDT-PERP", i, 1.0));
    }
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "offer must not wait"
    );
    assert_eq!(
        stats
            .input_dropped
            .load(std::sync::atomic::Ordering::Relaxed),
        9_999
    );
    // Signals are the engine's own output and are never offered back to it.
    input.offer(&common::signal("binance", "BTC-USDT-PERP", 0));
    assert_eq!(
        stats
            .input_dropped
            .load(std::sync::atomic::Ordering::Relaxed),
        9_999
    );
}

#[tokio::test]
async fn warm_up_restores_the_detectors_from_the_memory_store() {
    warm_up_restores_the_detectors(Backend::memory()).await;
}

/// Gated: needs `VULTURE_TEST_REDIS_URL` (a scratch Redis); skipped with a message otherwise.
#[tokio::test]
async fn warm_up_restores_the_detectors_from_redis() {
    let Some(backend) = Backend::redis("alerts-warmup").await else {
        return;
    };
    warm_up_restores_the_detectors(backend).await;
}

async fn warm_up_restores_the_detectors(backend: Backend) {
    use vulture_server::store::{EntryFields, Trim};
    let mock = MockHttp::start(Reply::ok()).await;
    let data = tempfile::tempdir().unwrap();
    let mut cfg = config(data.path(), &mock);
    cfg.alerts.warmup_secs = 4 * 3600;
    let store = backend.store();
    // The last 4 hours before "now" of a quiet symbol are already in Redis.
    let now = vulture_server::metrics::unix_now_ms();
    let first = now - 200 * 30_000;
    let history: Vec<Event> = (0..200)
        .map(|i| oi("BTC-USDT-PERP", first + i * 30_000, 10_000.0))
        .collect();
    let fields: Vec<EntryFields> = history
        .iter()
        .map(|e| EntryFields::from_event(e).unwrap())
        .collect();
    store
        .append(
            &fields,
            Trim {
                min_id_ms: 0,
                maxlen: 10_000_000,
            },
        )
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let alerts = Alerts::open(&cfg, stop.clone()).unwrap();
    let ctx = AppContext::new(cfg, stop.clone());
    // Two live samples with +8%: only a warmed-up detector can judge them (no 5 minutes of live history).
    let live = vec![
        oi("BTC-USDT-PERP", now + 30_000, 10_800.0),
        oi("BTC-USDT-PERP", now + 60_000, 10_800.0),
    ];
    let app = tokio::spawn(run_with_alerts(
        ctx,
        store.clone(),
        vec![Box::new(Scripted(live))],
        None,
        None,
        Some(alerts.clone()),
    ));
    eventually("the signal in the stream", async || {
        store
            .read_range("-", "+", None)
            .await
            .is_ok_and(|v| v.iter().any(|e| e.kind == "sig"))
    })
    .await;
    assert!(
        alerts
            .stats
            .warmup_events
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 200
    );
    assert_eq!(
        alerts
            .stats
            .warmed_up
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(10), app)
        .await
        .unwrap()
        .unwrap();
    backend.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cascade_is_baselined_from_redis_announced_and_summarized_without_flooding_the_channel() {
    use vulture_server::store::{EntryFields, Trim};
    let mock = MockHttp::start(Reply::ok()).await;
    let data = tempfile::tempdir().unwrap();
    let mut cfg = config(data.path(), &mock);
    cfg.alerts.warmup_secs = 24 * 3600;
    cfg.alerts.liq_cascade.enabled = true;
    cfg.alerts.liq_cascade.quiet_secs = 2;
    cfg.alerts.delivery.batch_window_ms = 400;
    cfg.alerts.delivery.telegram_min_interval_ms = 300;
    let backend = Backend::memory();
    let store = backend.store();
    // A quiet day: one 50k liquidation per minute for the last three hours.
    let now = vulture_server::metrics::unix_now_ms();
    let past: Vec<Event> = (1..=180)
        .rev()
        .map(|m| liq(now - m * 60_000, 50_000.0))
        .collect();
    let fields: Vec<EntryFields> = past
        .iter()
        .map(|e| EntryFields::from_event(e).unwrap())
        .collect();
    store
        .append(
            &fields,
            Trim {
                min_id_ms: 0,
                maxlen: 10_000_000,
            },
        )
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let alerts = Alerts::open(&cfg, stop.clone()).unwrap();
    alerts
        .manager
        .upsert_channel(channel(), TOKEN, "e2e")
        .await
        .unwrap();
    alerts
        .manager
        .upsert_rule(
            RuleDef {
                id: "signals".into(),
                name: "All signals".into(),
                enabled: true,
                kind: RuleKind::Signal {
                    types: vec![],
                    min_severity: Severity::Info,
                },
                exchanges: vec![],
                symbols: vec![],
                channels: vec!["tg".into()],
                cooldown_secs: 0,
                quiet_hours: Vec::new(),
            },
            "e2e",
        )
        .await
        .unwrap();
    let server = Server::start_all(
        &backend,
        test_stream_config(),
        Default::default(),
        None,
        Some(alerts.manager.clone()),
    )
    .await;
    let mut stream = server
        .client()
        .await
        .subscribe(filtered(&[], &[], &[EventType::Signal], ""))
        .await
        .unwrap()
        .into_inner();
    // The burst: 60 liquidations of 100k within about a second, shortly after the start.
    let burst: Vec<Event> = (0..60)
        .map(|i| liq(now + 900 + i * 15, 100_000.0))
        .collect();
    struct Delayed(Vec<Event>);
    impl Exchange for Delayed {
        fn name(&self) -> &str {
            "binance"
        }
        fn run<'a>(
            &'a self,
            ctx: AppContext,
            sink: EventSink,
        ) -> BoxFuture<'a, Result<(), ExchangeError>> {
            Box::pin(async move {
                // Wait for the warm-up to finish, then fire the burst at once.
                tokio::time::sleep(Duration::from_millis(800)).await;
                for e in &self.0 {
                    let _ = sink.send(e.clone()).await;
                }
                ctx.shutdown.cancelled().await;
                Ok(())
            })
        }
    }
    let ctx = AppContext::new(cfg, stop.clone());
    let app = tokio::spawn(run_with_alerts(
        ctx,
        store.clone(),
        vec![Box::new(Delayed(burst))],
        None,
        None,
        Some(alerts.clone()),
    ));
    // Market-wide and binance: a start and an end each.
    let mut phases = Vec::new();
    let mut seen = Vec::new();
    for _ in 0..4 {
        match tokio::time::timeout(Duration::from_secs(8), stream.message()).await {
            Ok(Ok(Some(e))) => seen.push(e),
            other => panic!("after {} signals: {other:?} {:?}", seen.len(), seen),
        }
    }
    for e in seen {
        let Some(Payload::Signal(s)) = e.payload else {
            panic!()
        };
        assert_eq!(s.r#type, SignalType::LiqCascade as i32);
        phases.push((s.exchange.clone(), s.details["phase"].clone()));
        if s.details["phase"] == "end" {
            assert_eq!(s.details["count"], "60");
            assert!(s.details["top_symbols"].starts_with("ETH-USDT-PERP"));
            assert_eq!(s.details["long_share"], "1.000");
        }
    }
    phases.sort();
    assert_eq!(
        phases,
        [
            ("*".to_string(), "end".to_string()),
            ("*".to_string(), "start".to_string()),
            ("binance".to_string(), "end".to_string()),
            ("binance".to_string(), "start".to_string()),
        ]
    );
    wait_until("the summary reached the channel", || {
        mock.requests().iter().any(|r| {
            r.json()["text"]
                .as_str()
                .unwrap_or("")
                .contains("Liquidation cascade ended")
        })
    })
    .await;
    // Four alerts, but the channel saw at most a couple of messages, spaced out, and no 429 was provoked.
    let reqs = mock.requests();
    assert!(reqs.len() <= 3, "{} messages", reqs.len());
    for w in reqs.windows(2) {
        assert!(w[1].at.duration_since(w[0].at) >= Duration::from_millis(280));
    }
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(10), app)
        .await
        .unwrap()
        .unwrap();
    server.stop().await;
}
