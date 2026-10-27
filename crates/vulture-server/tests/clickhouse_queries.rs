//! The example queries of `deploy/clickhouse/queries/*.sql` against the real schema (gated like the other
//! ClickHouse tests): every query runs, returns what the data set implies, and every file in the directory is
//! covered here and documented in `deploy/README.md`.

mod common;

use std::path::PathBuf;

use common::ch_env::*;
use common::history_data::*;
use vulture_proto::v1::{Event, Signal, SignalType, event::Payload};

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/clickhouse/queries")
}

/// The query text with the clock pinned: `now()` becomes a fixed time shortly after the data set.
fn pinned(name: &str) -> String {
    let sql = std::fs::read_to_string(dir().join(name)).unwrap_or_else(|e| panic!("{name}: {e}"));
    let now_ms = START + 3 * HOUR + 10 * MIN;
    sql.replace(
        "now()",
        &format!("fromUnixTimestamp64Milli({now_ms}, 'UTC')"),
    )
}

fn num(v: &serde_json::Value) -> f64 {
    match v {
        serde_json::Value::Number(n) => n.as_f64().unwrap(),
        serde_json::Value::String(s) => s.parse().unwrap(),
        other => panic!("not a number: {other}"),
    }
}

async fn loaded(name: &str) -> Option<(ChEnv, Vec<Event>)> {
    let env = ChEnv::new(name, false).await?;
    let ds = dataset().await;
    let mut events = ds.events.clone();
    // A cascade record as the engine writes it at the end of a cascade.
    let stream = Stream::new();
    let end = Event {
        id: String::new(),
        payload: Some(Payload::Signal(Signal {
            r#type: SignalType::LiqCascade as i32,
            exchange: "*".into(),
            symbol: String::new(),
            window_secs: 60,
            change_pct: 900.0,
            change_abs: 12_500_000.0,
            threshold: 500.0,
            ts: START + 100 * MIN,
            details: [
                ("phase", "end"),
                ("start_ts", &(START + 90 * MIN).to_string()),
                ("duration_secs", "540"),
                ("count", "88"),
                ("total_usd", "12500000"),
                ("peak_usd_per_min", "3100000"),
                ("long_share", "0.812"),
                ("symbols", "17"),
                ("top_symbols", "BTC-USDT-PERP $4.0M, ETH-USDT-PERP $2.1M"),
                ("severity", "critical"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        })),
    };
    let mut ev = stream.append(&end, START + 100 * MIN + 500).await;
    // A distinct stream id, later than every id of the data set.
    ev.id = format!("{}-0", START + 3 * HOUR + 5 * MIN);
    events.push(ev);
    env.insert(&events).await;
    Some((env, events))
}

#[tokio::test]
async fn every_example_query_runs_and_answers_sensibly() {
    let Some((env, events)) = loaded("queries").await else {
        return;
    };
    let rows = |sql: String| {
        let c = env.client.clone();
        async move {
            c.query_rows(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}\n{e}"))
        }
    };

    // Top movers: only series above the 1 million USD dust floor (three of the six of the data set), largest
    // change first.
    let r = rows(pinned("top_oi_movers.sql")).await;
    assert_eq!(r.len(), 3, "{r:?}");
    assert!(
        r.iter()
            .all(|x| num(&x["oi_usd_end"]) > 1e6 && num(&x["samples"]) > 10.0)
    );
    assert_eq!(r[0]["symbol"], "SOL-USDT-PERP");
    // The same query without the dust floor finds the Binance BTC spike.
    let loose = pinned("top_oi_movers.sql").replace("oi_usd_end > 1000000", "oi_usd_end > 0");
    let r2 = rows(loose).await;
    let btc = r2
        .iter()
        .find(|x| x["exchange"] == "binance" && x["symbol"] == "BTC-USDT-PERP");
    assert!(btc.is_some_and(|x| num(&x["change_pct"]) > 4.0), "{r2:?}");
    let pcts: Vec<f64> = r.iter().map(|x| num(&x["change_pct"]).abs()).collect();
    assert!(pcts.windows(2).all(|w| w[0] >= w[1]), "sorted by |change|");

    // Liquidation skew: the sums equal the data set's.
    let r = rows(pinned("liquidation_skew_by_hour.sql")).await;
    let total: f64 = r
        .iter()
        .map(|x| num(&x["long_usd"]) + num(&x["short_usd"]))
        .sum();
    let expect: f64 = events
        .iter()
        .filter_map(|e| match &e.payload {
            Some(Payload::Liquidation(l)) => Some(l.notional_usd),
            _ => None,
        })
        .sum();
    assert!((total - expect).abs() < 100.0, "{total} vs {expect}");
    assert!(
        r.iter()
            .all(|x| (0.0..=1.0).contains(&num(&x["long_share"])))
    );

    // Funding extremes: both lists, highest not below lowest.
    let r = rows(pinned("funding_extremes.sql")).await;
    let first = |side: &str| {
        r.iter()
            .find(|x| x["side"] == side)
            .map(|x| num(&x["funding_pct"]))
    };
    assert!(
        first("highest").unwrap() >= first("lowest").unwrap(),
        "{r:?}"
    );
    assert!(
        r.iter().all(|x| x["exchange"] != "okx"),
        "OKX has no funding in the data set"
    );

    // The cascade list shows the inserted record.
    let r = rows(pinned("cascades_list.sql")).await;
    assert_eq!(r.len(), 1, "{r:?}");
    assert_eq!(num(&r[0]["total_usd"]), 12_500_000.0);
    assert_eq!(num(&r[0]["duration_secs"]), 540.0);
    assert_eq!(r[0]["severity"], "critical");

    // Signal outcomes: the one OI spike signal with its price at the signal.
    let r = rows(pinned("signal_outcomes.sql")).await;
    assert_eq!(r.len(), 1, "{r:?}");
    assert_eq!(r[0]["symbol"], "BTC-USDT-PERP");
    assert!(num(&r[0]["price_at_signal"]) > 0.0);
    assert!(!r[0]["return_15m_pct"].is_null());

    // Timeline of Binance BTC: about a sample per 30 s over 3 hours = 180 minutes.
    let r = rows(pinned("symbol_timeline.sql")).await;
    assert!((175..=182).contains(&r.len()), "{}", r.len());
    let liq: f64 = r
        .iter()
        .map(|x| num(&x["liq_long_usd"]) + num(&x["liq_short_usd"]))
        .sum();
    let expect: f64 = events
        .iter()
        .filter_map(|e| match &e.payload {
            Some(Payload::Liquidation(l))
                if l.exchange == "binance" && l.symbol == "BTC-USDT-PERP" =>
            {
                Some(l.notional_usd)
            }
            _ => None,
        })
        .sum();
    assert!((liq - expect).abs() < 10.0, "{liq} vs {expect}");
    assert!(r.iter().any(|x| !x["funding_pct"].is_null()));

    // Completeness: three exchanges, sane coverage, the bybit hole shows as minutes of gap.
    let r = rows(pinned("data_completeness.sql")).await;
    let exchanges: std::collections::BTreeSet<&str> =
        r.iter().filter_map(|x| x["exchange"].as_str()).collect();
    assert_eq!(exchanges.len(), 3, "{exchanges:?}");
    assert!(
        r.iter()
            .all(|x| num(&x["coverage"]) > 0.0 && num(&x["coverage"]) <= 1.2),
        "{r:?}"
    );
    let gap_minutes: f64 = r
        .iter()
        .filter(|x| x["exchange"] == "bybit")
        .map(|x| num(&x["gap_minutes"]))
        .sum();
    assert!((gap_minutes - 30.0).abs() < 0.2, "{gap_minutes}");
    env.cleanup().await;
}

#[test]
fn every_query_file_is_covered_and_documented() {
    let readme = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/README.md"),
    )
    .unwrap();
    let mut names: Vec<String> = std::fs::read_dir(dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".sql"))
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            "cascades_list.sql",
            "data_completeness.sql",
            "funding_extremes.sql",
            "liquidation_skew_by_hour.sql",
            "signal_outcomes.sql",
            "symbol_timeline.sql",
            "top_oi_movers.sql",
        ],
        "a new query needs a test in every_example_query_runs_and_answers_sensibly"
    );
    for n in &names {
        assert!(
            readme.contains(n.as_str()),
            "{n} is not mentioned in deploy/README.md"
        );
        let sql = std::fs::read_to_string(dir().join(n)).unwrap();
        assert!(
            sql.starts_with("--"),
            "{n} starts with a comment saying what it does"
        );
        let code: String = sql
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !code.contains(';'),
            "{n}: one statement without a semicolon, so any client can run it"
        );
        let upper = sql.to_uppercase();
        for forbidden in [
            "INSERT ", "ALTER ", "DROP ", "TRUNCATE", "CREATE ", "DELETE ",
        ] {
            assert!(
                !upper.contains(forbidden),
                "{n} contains {forbidden:?}: the examples are read-only"
            );
        }
    }
}
