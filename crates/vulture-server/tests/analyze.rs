//! The analysis tools (`analyze signals`, `analyze replay`) on real ClickHouse (gated) and without it.

mod common;

use std::collections::BTreeMap;
use std::io::Write;
use std::process::Command;

use common::ch_env::*;
use common::history_data::*;
use vulture_proto::v1::{Event, OpenInterest, Signal, SignalType, event::Payload};
use vulture_server::analyze::cli::{self, ReplayOpts, SignalsOpts};
use vulture_server::analyze::params::Detector;
use vulture_server::analyze::replay::MemoryReplay;
use vulture_server::analyze::study::{SignalKind, SignalRecord, StudyConfig, StudySource};
use vulture_server::config::AlertsConfig;
use vulture_server::query::cold::Scope;
use vulture_server::store::RedisStore;

const BIN: &str = env!("CARGO_BIN_EXE_vulture-server");

/// Ten signals with known outcomes: six followed by +1% after 15 minutes, four by -1%.
async fn known_outcomes(stream: &Stream) -> Vec<Event> {
    let mut events = Vec::new();
    let base = START;
    for i in 0..10i64 {
        let ts = base + i * 90 * MIN;
        let up = i < 6;
        for m in -20..=70i64 {
            let t = ts + m * MIN;
            let price = if m < -5 {
                100.0
            } else if m <= 0 {
                100.0 + (m + 5) as f64 * 0.2
            } else if m < 15 {
                101.0
            } else if up {
                101.0 * 1.012
            } else {
                101.0 * 0.988
            };
            let e = Event {
                id: String::new(),
                payload: Some(Payload::OpenInterest(OpenInterest {
                    exchange: "binance".into(),
                    symbol_raw: "BTCUSDT".into(),
                    symbol: "BTC-USDT-PERP".into(),
                    exchange_ts: t,
                    recv_ts: t + 40,
                    oi_base: 1000.0,
                    oi_usd: Some(1000.0 * price),
                    mark_price: Some(price),
                    ..OpenInterest::default()
                })),
            };
            events.push((t + 40, e));
        }
        let sig = Event {
            id: String::new(),
            payload: Some(Payload::Signal(Signal {
                r#type: SignalType::OiSpikeUp as i32,
                exchange: "binance".into(),
                symbol: "BTC-USDT-PERP".into(),
                window_secs: 300,
                change_pct: 6.0,
                change_abs: 60.0,
                threshold: 3.0,
                ts,
                details: BTreeMap::new().into_iter().collect(),
            })),
        };
        events.push((ts + 100, sig));
    }
    events.sort_by_key(|(t, _)| *t);
    let mut out = Vec::new();
    // Overlapping OI samples of different signals share timestamps only if signals are closer than 90 minutes: no.
    for (t, e) in events {
        out.push(stream.append(&e, t).await);
    }
    out
}

fn opts_signals(from: i64, to: i64) -> SignalsOpts {
    SignalsOpts {
        from,
        to,
        exchanges: vec![],
        study: StudyConfig::default(),
    }
}

#[tokio::test]
async fn the_study_on_clickhouse_finds_the_known_hit_rates() {
    let Some(env) = ChEnv::new("study", false).await else {
        return;
    };
    let stream = Stream::new();
    let events = known_outcomes(&stream).await;
    env.insert(&events).await;
    let (report, rendered) = cli::signals(
        &vulture_server::analyze::source::ChSource::new(env.client.clone()),
        &opts_signals(START - HOUR, START + 20 * HOUR),
    )
    .await
    .unwrap();
    assert_eq!(report.signals, 10);
    assert_eq!(report.excluded_no_price, 0);
    let g = &report.groups[0];
    assert_eq!(
        (g.kind, g.window_secs, g.exchange.as_str()),
        (SignalKind::OiSpikeUp, 300, "binance")
    );
    let h15 = g.horizons.iter().find(|h| h.minutes == 15).unwrap();
    assert_eq!((h15.measured, h15.missing), (10, 0));
    let hit = h15.hits.iter().find(|h| h.threshold_pct == 0.5).unwrap();
    assert_eq!((hit.continuation, hit.reversal), (Some(0.6), Some(0.4)));
    // The return is 1.2% of 101 against the signal price of 101: +/-1.2%.
    assert!(
        (h15.directional_return.q3.unwrap() - 1.2).abs() < 1e-6,
        "{:?}",
        h15.directional_return
    );
    assert!((h15.directional_return.q1.unwrap() + 1.2).abs() < 1e-6);
    assert!(rendered.markdown.contains("oi_spike_up / 300 s / binance"));
    let json: serde_json::Value = serde_json::from_str(&rendered.json).unwrap();
    assert_eq!(json["groups"][0]["signals"], 10);
    // A period without signals says so; an exchange filter removes the group.
    let (empty, md) = cli::signals(
        &vulture_server::analyze::source::ChSource::new(env.client.clone()),
        &opts_signals(0, 1000),
    )
    .await
    .unwrap();
    assert!(empty.groups.is_empty() && md.markdown.contains("No signals"));
    let mut other = opts_signals(START - HOUR, START + 20 * HOUR);
    other.exchanges = vec!["bybit".into()];
    let (none, _) = cli::signals(
        &vulture_server::analyze::source::ChSource::new(env.client.clone()),
        &other,
    )
    .await
    .unwrap();
    assert!(none.groups.is_empty());
    env.cleanup().await;
}

#[tokio::test]
async fn the_cli_prints_the_study_and_writes_files() {
    let Some(env) = ChEnv::new("study_cli", false).await else {
        return;
    };
    let stream = Stream::new();
    env.insert(&known_outcomes(&stream).await).await;
    let dir = tempfile::tempdir().unwrap();
    let cfg_path = dir.path().join("config.toml");
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    write!(
        f,
        "[clickhouse]\nurl = {:?}\ndatabase = {:?}\nuser = {:?}\npassword = {:?}\n",
        env.cfg.url,
        env.db,
        env.cfg.user,
        std::env::var("VULTURE_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default()
    )
    .unwrap();
    drop(f);
    let out_dir = dir.path().join("out");
    let out = Command::new(BIN)
        .args(["analyze", "signals", "--config"])
        .arg(&cfg_path)
        .args([
            "--from",
            &(START - HOUR).to_string(),
            "--to",
            &(START + 20 * HOUR).to_string(),
            "--format",
            "json",
            "--out-dir",
        ])
        .arg(&out_dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["signals"], 10);
    assert!(
        std::fs::read_to_string(out_dir.join("report.md"))
            .unwrap()
            .starts_with("# Signal event study")
    );
    assert!(out_dir.join("report.json").exists());
    // Dates work, bad input fails with a message.
    let bad = Command::new(BIN)
        .args(["analyze", "signals", "--config"])
        .arg(&cfg_path)
        .args(["--from", "yesterday", "--to", "2026-10-27"])
        .output()
        .unwrap();
    assert!(!bad.status.success());
    assert!(
        String::from_utf8_lossy(&bad.stderr).contains("not a time"),
        "{}",
        String::from_utf8_lossy(&bad.stderr)
    );
    env.cleanup().await;
}

fn replay_opts(params: &[&str], from: i64) -> ReplayOpts {
    ReplayOpts {
        detector: Detector::OiSpike,
        params: params.iter().map(|s| s.to_string()).collect(),
        from,
        to: END,
        warmup_secs: 3600,
        chunk_secs: 1200,
        scope: Scope::default(),
        list_max: 20,
        study: StudyConfig::default(),
    }
}

/// A study source with prices from the data set, for the replay in memory.
struct Prices(Vec<Event>);

impl StudySource for Prices {
    fn signals(
        &self,
        _: i64,
        _: i64,
    ) -> futures_util::future::BoxFuture<'_, Result<Vec<SignalRecord>, String>> {
        Box::pin(async { Ok(vec![]) })
    }
    fn marks(
        &self,
        ex: &str,
        sym: &str,
        from: i64,
        to: i64,
    ) -> futures_util::future::BoxFuture<'_, Result<Vec<(i64, f64)>, String>> {
        let (ex, sym) = (ex.to_string(), sym.to_string());
        Box::pin(async move {
            Ok(self
                .0
                .iter()
                .filter_map(|e| match &e.payload {
                    Some(Payload::OpenInterest(o))
                        if o.exchange == ex
                            && o.symbol == sym
                            && o.recv_ts >= from
                            && o.recv_ts <= to =>
                    {
                        o.mark_price.map(|p| (o.recv_ts, p))
                    }
                    _ => None,
                })
                .collect())
        })
    }
    fn liquidated_usd(
        &self,
        _: &str,
        _: &str,
        _: i64,
        _: i64,
    ) -> futures_util::future::BoxFuture<'_, Result<f64, String>> {
        Box::pin(async { Ok(0.0) })
    }
}

const SENSITIVE: &str = "threshold_pct.300=2,min_change_usd.300=1000,windows=300,z_min=2,min_baseline_points=10,min_oi_usd=0";

#[tokio::test]
async fn replay_compares_threshold_sets_without_side_effects() {
    let ds = dataset().await;
    let before = ds.stream.store.stream_len().await.unwrap();
    let res = cli::replay_with(
        &MemoryReplay(ds.events.clone()),
        &Prices(ds.events.clone()),
        &AlertsConfig::default(),
        &replay_opts(&[SENSITIVE, "threshold_pct.300=20,threshold_pct.60=20,threshold_pct.900=20,threshold_pct.3600=20,threshold_pct.14400=20"], START + HOUR),
    )
    .await
    .unwrap();
    assert_eq!(res.runs.len(), 2);
    let sens = &res.runs[0].signals;
    assert!(
        sens.iter().any(|s| s.exchange == "binance"
            && s.symbol == "BTC-USDT-PERP"
            && s.ts >= START + 2 * HOUR
            && s.ts < START + 2 * HOUR + 10 * MIN),
        "the +6% spike of the data set is found: {sens:?}"
    );
    assert!(res.runs[1].signals.is_empty());
    assert!(res.rendered.markdown.contains("## Comparison"));
    assert!(res.rendered.markdown.contains("case 1") && res.rendered.markdown.contains("case 2"));
    let v: serde_json::Value = serde_json::from_str(&res.rendered.json).unwrap();
    assert_eq!(v["cases"][1]["signal_count"], 0);
    assert!(v["cases"][0]["signal_count"].as_u64().unwrap() >= 1);
    // No side effects: the source stream is as it was.
    assert_eq!(ds.stream.store.stream_len().await.unwrap(), before);
    // A bad parameter is refused before anything runs.
    let err = cli::replay_with(
        &MemoryReplay(vec![]),
        &Prices(vec![]),
        &AlertsConfig::default(),
        &replay_opts(&["nonsense=1"], START),
    )
    .await
    .err()
    .unwrap();
    assert!(err.contains("unknown parameter"), "{err}");
}

#[tokio::test]
async fn replay_from_clickhouse_equals_replay_from_memory() {
    let Some(env) = ChEnv::new("replay", false).await else {
        return;
    };
    let ds = dataset().await;
    env.insert(&ds.events).await;
    let opts = replay_opts(&[SENSITIVE], START + HOUR);
    let a = cli::replay_on_clickhouse(env.client.clone(), &AlertsConfig::default(), &opts)
        .await
        .unwrap();
    let b = cli::replay_with(
        &MemoryReplay(ds.events.clone()),
        &Prices(ds.events.clone()),
        &AlertsConfig::default(),
        &opts,
    )
    .await
    .unwrap();
    let key = |r: &cli::ReplayResult| -> Vec<(i64, i32, String, String, u32)> {
        r.runs[0]
            .signals
            .iter()
            .map(|s| {
                (
                    s.ts,
                    s.r#type,
                    s.exchange.clone(),
                    s.symbol.clone(),
                    s.window_secs,
                )
            })
            .collect()
    };
    assert!(!key(&a).is_empty());
    assert_eq!(key(&a), key(&b));
    assert_eq!(a.events_fed, b.events_fed);
    // The replay only read.
    assert_eq!(
        env.scalar("SELECT count() FROM open_interest").await as usize,
        ds.events
            .iter()
            .filter(|e| matches!(e.payload, Some(Payload::OpenInterest(_))))
            .count()
    );
    // Filters.
    let mut one = opts.clone();
    one.scope = Scope {
        exchanges: vec!["bybit".into()],
        ..Scope::default()
    };
    let c = cli::replay_on_clickhouse(env.client.clone(), &AlertsConfig::default(), &one)
        .await
        .unwrap();
    assert!(c.runs[0].signals.iter().all(|s| s.exchange == "bybit"));
    env.cleanup().await;
}
