//! Latency of the `History` operations on the synthetic history of `synth_history` (task V3b).
//!
//! ```sh
//! VULTURE_SYNTH_CH_PASSWORD=change-me cargo run --release -p vulture-server --example synth_history -- \
//!     --database vulture_synth --drop --days 30 --exchanges 3
//! VULTURE_SYNTH_CH_PASSWORD=change-me cargo run --release -p vulture-server --example history_bench -- \
//!     --database vulture_synth --days 30
//! ```
//!
//! The service runs in this process on an empty in-memory Redis stand-in (so the unflushed tail is empty), with
//! the real ClickHouse client. Each case runs `--runs` times; the median and the best wall time are printed, with
//! the number of rows returned. With `--assert` the process exits non-zero when a target of the task is missed.

use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use futures_util::StreamExt;
use tonic::Request;
use vulture_proto::v1::history_server::History;
use vulture_proto::v1::{AggregateRequest, GroupBy, HistoryRequest, Metric, OiMoversQuery, Step};
use vulture_server::config::{ClickHouseConfig, HistoryApiConfig, Secret};
use vulture_server::history::{ChClient, HistorySink};
use vulture_server::query::cold::ColdStore;
use vulture_server::query::{ChCold, HistoryHandle, HistoryService, SeriesParams};
use vulture_server::store::MemoryStore;
use vulture_server::ws::Clock;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8123")]
    url: String,
    #[arg(long, default_value = "default")]
    user: String,
    #[arg(long, default_value = "vulture_synth")]
    database: String,
    /// Days of history that `synth_history` produced.
    #[arg(long, default_value_t = 30)]
    days: i64,
    /// Start of the history (the default of `synth_history`).
    #[arg(long, default_value_t = 1_790_467_200_000)]
    start_ms: i64,
    #[arg(long, default_value_t = 5)]
    runs: usize,
    /// Fail when a target is missed.
    #[arg(long)]
    assert: bool,
}

struct At(i64);
impl Clock for At {
    fn now_ms(&self) -> i64 {
        self.0
    }
}

const DAY: i64 = 86_400_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cfg = ClickHouseConfig {
        url: args.url.clone(),
        user: args.user.clone(),
        database: args.database.clone(),
        insert_timeout_secs: 300,
        ..ClickHouseConfig::default()
    };
    let client = Arc::new(
        ChClient::new(
            &cfg,
            Secret::new(std::env::var("VULTURE_SYNTH_CH_PASSWORD").unwrap_or_default()),
        )
        .map_err(anyhow::Error::msg)?,
    );
    client.ping().await?;
    let end = args.start_ms + args.days * DAY;
    let hcfg = HistoryApiConfig {
        max_bins: 100_000,
        max_aggregate_rows: 2_000_000,
        max_fine_rows: 20_000_000,
        request_timeout_secs: 120,
        ..HistoryApiConfig::default()
    };
    let service = HistoryService::new(
        Arc::new(MemoryStore::new()),
        Some(Arc::new(ChCold::new(client.clone())) as Arc<dyn ColdStore>),
        None,
        hcfg,
        SeriesParams::default(),
        Arc::new(At(end)),
    );
    let h = HistoryHandle(service);

    let agg = |group_by: GroupBy,
               step: Step,
               from: i64,
               to: i64,
               metrics: &[Metric],
               symbols: &[&str],
               exchanges: &[&str]| AggregateRequest {
        group_by: group_by as i32,
        step: step as i32,
        from_ts: from,
        to_ts: to,
        metrics: metrics.iter().map(|m| *m as i32).collect(),
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
        exchanges: exchanges.iter().map(|s| s.to_string()).collect(),
        ..AggregateRequest::default()
    };
    let default_metrics = vulture_server::query::agg::DEFAULT_METRICS;
    let movers = |window_secs: u32| AggregateRequest {
        to_ts: end,
        oi_movers: Some(OiMoversQuery {
            window_secs,
            limit: 20,
            ..OiMoversQuery::default()
        }),
        ..AggregateRequest::default()
    };

    // (name, target ms, request)
    enum Case {
        Agg(AggregateRequest),
        Query(HistoryRequest),
    }
    let cases: Vec<(&str, Option<f64>, Case)> = vec![
        (
            "one symbol, 1m bins over 24 h, all metrics (binance)",
            Some(200.0),
            Case::Agg(agg(
                GroupBy::Symbol,
                Step::Minute1,
                end - DAY,
                end,
                &default_metrics,
                &["BTC-USDT-PERP"],
                &["binance"],
            )),
        ),
        (
            "one symbol on all exchanges, 1m bins over 24 h, all metrics",
            Some(200.0),
            Case::Agg(agg(
                GroupBy::Symbol,
                Step::Minute1,
                end - DAY,
                end,
                &default_metrics,
                &["BTC-USDT-PERP"],
                &[],
            )),
        ),
        (
            "one symbol, 1d bins over 90 days (data: the days it has), all metrics",
            Some(200.0),
            Case::Agg(agg(
                GroupBy::Symbol,
                Step::Day,
                end - 90 * DAY,
                end,
                &default_metrics,
                &["BTC-USDT-PERP"],
                &["binance"],
            )),
        ),
        (
            "total OI per base asset over 7 days, 1h bins",
            Some(1000.0),
            Case::Agg(agg(
                GroupBy::BaseAsset,
                Step::Hour,
                end - 7 * DAY,
                end,
                &[Metric::OiTotalUsd],
                &[],
                &[],
            )),
        ),
        (
            "total OI per base asset over 7 days, 1d bins",
            Some(1000.0),
            Case::Agg(agg(
                GroupBy::BaseAsset,
                Step::Day,
                end - 7 * DAY,
                end,
                &[Metric::OiTotalUsd],
                &[],
                &[],
            )),
        ),
        (
            "liquidations per exchange over 30 days, 1d bins",
            None,
            Case::Agg(agg(
                GroupBy::Exchange,
                Step::Day,
                end - 30 * DAY,
                end,
                &[Metric::LiqSumUsd, Metric::LiqCount],
                &[],
                &[],
            )),
        ),
        (
            "OI movers over all symbols, window 1 h",
            Some(1000.0),
            Case::Agg(movers(3600)),
        ),
        (
            "OI movers over all symbols, window 5 min",
            Some(1000.0),
            Case::Agg(movers(300)),
        ),
        (
            "OI movers over all symbols, window 24 h",
            None,
            Case::Agg(movers(86_400)),
        ),
        (
            "Query: one symbol's events over 24 h (limit 100000)",
            None,
            Case::Query(HistoryRequest {
                from_ts: end - DAY,
                to_ts: end,
                symbols: vec!["BTC-USDT-PERP".into()],
                exchanges: vec!["binance".into()],
                limit: 100_000,
                ..HistoryRequest::default()
            }),
        ),
        (
            "Query: first 10000 liquidations of the last day (all symbols)",
            None,
            Case::Query(HistoryRequest {
                from_ts: end - DAY,
                to_ts: end,
                types: vec![vulture_proto::v1::EventType::Liquidation as i32],
                limit: 10_000,
                ..HistoryRequest::default()
            }),
        ),
    ];
    let mut missed = Vec::new();
    println!("case | median ms | best ms | rows | target ms");
    for (name, target, case) in cases {
        let mut times = Vec::new();
        let mut rows = 0usize;
        for _ in 0..args.runs {
            let t = Instant::now();
            rows = match &case {
                Case::Agg(r) => {
                    let resp = h
                        .aggregate(Request::new(r.clone()))
                        .await
                        .map_err(|e| anyhow::anyhow!("{name}: {e}"))?
                        .into_inner();
                    match resp.oi_movers {
                        Some(m) => m.top_growth.len() + m.top_decline.len(),
                        None => resp.rows.len(),
                    }
                }
                Case::Query(r) => {
                    let mut s = h.query(Request::new(r.clone())).await?.into_inner();
                    let mut n = 0;
                    while let Some(e) = s.next().await {
                        e.map_err(|e| anyhow::anyhow!("{name}: {e}"))?;
                        n += 1;
                    }
                    n
                }
            };
            times.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        times.sort_by(|a, b| a.total_cmp(b));
        let median = times[times.len() / 2];
        println!(
            "{name} | {median:.1} | {:.1} | {rows} | {}",
            times[0],
            target.map_or("-".to_string(), |t| format!("{t:.0}"))
        );
        if let Some(t) = target
            && median > t
        {
            missed.push(format!("{name}: {median:.1} ms > {t:.0} ms"));
        }
    }
    if args.assert && !missed.is_empty() {
        anyhow::bail!("targets missed:\n  {}", missed.join("\n  "));
    }
    Ok(())
}
