//! Synthetic history for the capacity and query-speed measurements of the ClickHouse schema (task V3a).
//!
//! Generates open interest snapshots for many symbols at a fixed interval plus a bursty liquidation stream, turns
//! them into stream entries and pushes them through the same code path as the flusher (entry to row mapping,
//! `RowBinary` encoding, insert with a deduplication token), then reports rows, bytes on disk per row, insert
//! throughput and the time of a few representative queries. The data is a model, not a recording: how well it
//! compresses depends on how often real values repeat, so treat the numbers as an estimate to be re-measured
//! on the real stream.
//!
//! ```sh
//! docker run -d --rm --name vulture-synth-ch -p 127.0.0.1:8123:8123 -e CLICKHOUSE_PASSWORD=change-me \
//!     clickhouse/clickhouse-server:25.8
//! VULTURE_SYNTH_CH_PASSWORD=change-me cargo run --release -p vulture-server --example synth_history -- \
//!     --url http://127.0.0.1:8123 --user default --database vulture_synth --drop
//! ```
//!
//! The password is read from `VULTURE_SYNTH_CH_PASSWORD` only (not from the command line). Use a scratch
//! ClickHouse: `--drop` removes the database first, and the database is kept at the end unless `--cleanup`.

use std::time::Instant;

use clap::Parser;
use vulture_proto::v1::{Event, Liquidation, OpenInterest, Side, event::Payload};
use vulture_server::config::{ClickHouseConfig, Secret};
use vulture_server::history::client::json_u64;
use vulture_server::history::{ChClient, HistorySink, PageRows, Table, dedup_token};
use vulture_server::store::{EntryFields, StoredEntry};

#[derive(Parser)]
#[command(about = "Fills a scratch ClickHouse with synthetic history and measures it")]
struct Args {
    /// ClickHouse HTTP endpoint.
    #[arg(long, default_value = "http://127.0.0.1:8123")]
    url: String,
    /// User.
    #[arg(long, default_value = "default")]
    user: String,
    /// Database to fill (created by the migrations).
    #[arg(long, default_value = "vulture_synth")]
    database: String,
    /// Drop the database first.
    #[arg(long)]
    drop: bool,
    /// Drop the database at the end.
    #[arg(long)]
    cleanup: bool,
    /// Days of history.
    #[arg(long, default_value_t = 7)]
    days: u32,
    /// Symbols with open interest.
    #[arg(long, default_value_t = 560)]
    symbols: usize,
    /// Exchanges (binance, bybit, okx in this order); each polls every symbol, with its own random walk.
    #[arg(long, default_value_t = 1)]
    exchanges: usize,
    /// Seconds between snapshots of one symbol.
    #[arg(long, default_value_t = 30)]
    interval_secs: u64,
    /// Average liquidations per day (cascades included).
    #[arg(long, default_value_t = 120_000)]
    liquidations_per_day: u64,
    /// Entries per insert page.
    #[arg(long, default_value_t = 100_000)]
    page: usize,
    /// Start of the history, ms since the Unix epoch (default 2026-09-27 00:00 UTC: the week spans two months).
    #[arg(long, default_value_t = 1_790_467_200_000)]
    start_ms: i64,
}

/// xorshift64*: deterministic and dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.unit()
    }
}

struct Sym {
    raw: String,
    norm: String,
    mark: f64,
    oi: f64,
    weight: f64,
    funding: f64,
}

fn symbols(n: usize, rng: &mut Rng) -> Vec<Sym> {
    const MAJORS: [(&str, f64, f64); 10] = [
        ("BTC", 65_000.0, 90_000.0),
        ("ETH", 3_200.0, 1_100_000.0),
        ("SOL", 150.0, 3_000_000.0),
        ("XRP", 0.55, 900_000_000.0),
        ("DOGE", 0.15, 4_000_000_000.0),
        ("BNB", 580.0, 800_000.0),
        ("ADA", 0.45, 700_000_000.0),
        ("AVAX", 35.0, 9_000_000.0),
        ("LINK", 14.0, 20_000_000.0),
        ("SUI", 1.4, 600_000_000.0),
    ];
    (0..n)
        .map(|i| {
            let (base, mark, oi) = if i < MAJORS.len() {
                (MAJORS[i].0.to_string(), MAJORS[i].1, MAJORS[i].2)
            } else {
                let mark = 10f64.powf(rng.range(-4.0, 3.0));
                let usd = 10f64.powf(rng.range(5.5, 8.5));
                (format!("TKN{i:04}"), mark, usd / mark)
            };
            // The last 38 are USDC-margined, like the real universe.
            let quote = if i + 38 >= n { "USDC" } else { "USDT" };
            Sym {
                raw: format!("{base}{quote}"),
                norm: format!("{base}-{quote}-PERP"),
                mark,
                oi,
                // Zipf-like popularity for liquidations.
                weight: 1.0 / (1.0 + i as f64).powf(0.9),
                funding: 0.0001,
            }
        })
        .collect()
}

struct Loader {
    client: ChClient,
    seq: u64,
    pages: [Vec<StoredEntry>; 4],
    rows: [u64; 4],
    build_secs: f64,
    insert_secs: f64,
    page: usize,
}

impl Loader {
    fn push(&mut self, event: Event, recv_ts: i64) -> Option<usize> {
        let fields = EntryFields::from_event(&event)?;
        self.seq += 1;
        let table = match fields.kind {
            "liq" => Table::Liquidations,
            _ => Table::OpenInterest,
        };
        self.pages[table.index()].push(StoredEntry {
            id: format!("{recv_ts}-{}", self.seq),
            kind: fields.kind.into(),
            exchange: fields.exchange,
            symbol: fields.symbol,
            data: fields.data,
        });
        Some(table.index())
    }

    async fn flush(&mut self, index: usize) {
        if self.pages[index].is_empty() {
            return;
        }
        let entries = std::mem::take(&mut self.pages[index]);
        let (first, last) = (
            entries.first().unwrap().id.clone(),
            entries.last().unwrap().id.clone(),
        );
        let started = Instant::now();
        let page = PageRows::build(entries);
        self.build_secs += started.elapsed().as_secs_f64();
        for table in Table::ALL {
            let batch = &page.tables[table.index()];
            if batch.rows == 0 {
                continue;
            }
            let started = Instant::now();
            self.client
                .insert(
                    table,
                    batch.body.clone(),
                    &dedup_token(table, &first, &last),
                )
                .await
                .expect("insert");
            self.insert_secs += started.elapsed().as_secs_f64();
            self.rows[table.index()] += batch.rows;
        }
    }
}

fn percent(a: f64, b: f64) -> f64 {
    if b == 0.0 { 0.0 } else { 100.0 * a / b }
}

async fn print_sizes(client: &ChClient, database: &str, label: &str) {
    let rows = client
        .query_rows(&format!(
            "SELECT table, sum(rows) AS rows, sum(data_compressed_bytes) AS comp, \
             sum(data_uncompressed_bytes) AS raw, count() AS parts FROM system.parts \
             WHERE database = '{database}' AND active AND table IN ('liquidations','open_interest') GROUP BY table ORDER BY table"
        ))
        .await
        .expect("sizes");
    println!("\n== Size {label} ==");
    println!("table | rows | parts | compressed | uncompressed | bytes/row | ratio");
    for r in rows {
        let (n, c, u) = (
            json_u64(&r["rows"]) as f64,
            json_u64(&r["comp"]) as f64,
            json_u64(&r["raw"]) as f64,
        );
        println!(
            "{} | {} | {} | {:.1} MiB | {:.1} MiB | {:.2} | {:.1}x",
            r["table"].as_str().unwrap(),
            n as u64,
            json_u64(&r["parts"]),
            c / 1_048_576.0,
            u / 1_048_576.0,
            c / n,
            u / c
        );
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let password = std::env::var("VULTURE_SYNTH_CH_PASSWORD").unwrap_or_default();
    let cfg = ClickHouseConfig {
        url: args.url.clone(),
        user: args.user.clone(),
        database: args.database.clone(),
        insert_timeout_secs: 300,
        ..ClickHouseConfig::default()
    };
    let client = ChClient::new(&cfg, Secret::new(password.clone())).map_err(anyhow::Error::msg)?;
    if args.drop {
        client
            .execute_server(&format!("DROP DATABASE IF EXISTS {}", args.database))
            .await?;
    }
    let applied = client.migrate().await?;
    println!("migrations applied: {applied:?}");

    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let names = ["binance", "bybit", "okx"];
    let n_ex = args.exchanges.clamp(1, names.len());
    let base_syms = symbols(args.symbols, &mut rng);
    let weight_sum: f64 = base_syms.iter().map(|s| s.weight).sum();
    // One book per exchange: the same symbols, a slightly different level and an independent walk.
    let mut books: Vec<Vec<Sym>> = (0..n_ex)
        .map(|e| {
            base_syms
                .iter()
                .map(|s| Sym {
                    raw: s.raw.clone(),
                    norm: s.norm.clone(),
                    mark: s.mark * (1.0 + 0.0002 * e as f64),
                    oi: s.oi * (1.0 - 0.3 * e as f64),
                    weight: s.weight,
                    funding: s.funding,
                })
                .collect()
        })
        .collect();
    let cycles = u64::from(args.days) * 86_400 / args.interval_secs;
    let liq_base_rate = args.liquidations_per_day as f64 / 86_400.0 * 0.7;
    let cascade_rate = liq_base_rate * 40.0;
    let mut loader = Loader {
        client: ChClient::new(&cfg, Secret::new(password)).map_err(anyhow::Error::msg)?,
        seq: 0,
        pages: Default::default(),
        rows: [0; 4],
        build_secs: 0.0,
        insert_secs: 0.0,
        page: args.page,
    };
    let started = Instant::now();
    let n = args.symbols as u64;
    let day_ms = 86_400_000i64;
    for k in 0..cycles {
        let cycle_start = args.start_ms + (k * args.interval_secs * 1000) as i64;
        // Open interest: each symbol is polled at its own offset within the interval.
        for (ex, syms) in books.iter_mut().enumerate() {
            for (i, s) in syms.iter_mut().enumerate() {
                let offset = (i as u64 * args.interval_secs * 1000 / n) as i64;
                let ts = cycle_start + offset + rng.range(-250.0, 250.0) as i64;
                if rng.unit() < 0.7 {
                    s.oi *= 1.0 + rng.range(-0.0008, 0.0008);
                    s.oi = (s.oi * 1000.0).round() / 1000.0;
                }
                s.mark *= 1.0 + rng.range(-0.0006, 0.0006);
                if rng.unit() < 0.1 {
                    s.funding = (rng.range(-0.0003, 0.0006) * 1e8).round() / 1e8;
                }
                let mark = (s.mark * 1e6).round() / 1e6;
                let recv = ts + rng.range(40.0, 400.0) as i64;
                let event = Event {
                    id: String::new(),
                    payload: Some(Payload::OpenInterest(OpenInterest {
                        exchange: names[ex].into(),
                        symbol_raw: s.raw.clone(),
                        symbol: s.norm.clone(),
                        exchange_ts: ts,
                        recv_ts: recv,
                        oi_base: s.oi,
                        oi_usd: Some(((s.oi * mark) * 100.0).round() / 100.0),
                        mark_price: Some(mark),
                        // Funding moves slowly (changes about every tenth poll, quantized like the exchanges
                        // quote it); the index price follows the mark closely.
                        funding_rate: Some(s.funding),
                        index_price: Some(
                            (mark * (1.0 - 0.0004 + rng.range(-0.0002, 0.0002)) * 1e6).round()
                                / 1e6,
                        ),
                        next_funding_ts: Some((ts / 28_800_000 + 1) * 28_800_000),
                    })),
                };
                if let Some(idx) = loader.push(event, recv)
                    && loader.pages[idx].len() >= loader.page
                {
                    loader.flush(idx).await;
                }
            }
        }
        // Liquidations: a Poisson-like stream, with three five-minute cascades a day at 40x the rate.
        let secs = args.interval_secs;
        for sec in 0..secs {
            let t = cycle_start + (sec * 1000) as i64;
            let in_day = t.rem_euclid(day_ms);
            let cascade = [
                (3 * 3_600_000, 300_000),
                (11 * 3_600_000 + 1_800_000, 300_000),
                (19 * 3_600_000 + 600_000, 300_000),
            ]
            .iter()
            .any(|(from, len)| in_day >= *from && in_day < from + len);
            let rate = if cascade { cascade_rate } else { liq_base_rate };
            let mut count = rate.floor() as u32;
            if rng.unit() < rate.fract() {
                count += 1;
            }
            for _ in 0..count {
                let mut pick = rng.unit() * weight_sum;
                let mut idx = 0;
                let ex = (rng.next() % n_ex as u64) as usize;
                let syms = &books[ex];
                for (i, s) in syms.iter().enumerate() {
                    pick -= s.weight;
                    if pick <= 0.0 {
                        idx = i;
                        break;
                    }
                }
                let s = &syms[idx];
                let ts = t + rng.range(0.0, 999.0) as i64;
                let qty_usd = 10f64.powf(2.0 + 3.5 * rng.unit().powi(2));
                let price = (s.mark * (1.0 + rng.range(-0.001, 0.001)) * 1e6).round() / 1e6;
                let qty = ((qty_usd / price) * 1000.0).round() / 1000.0;
                let event = Event {
                    id: String::new(),
                    payload: Some(Payload::Liquidation(Liquidation {
                        exchange: names[ex].into(),
                        symbol_raw: s.raw.clone(),
                        symbol: s.norm.clone(),
                        exchange_ts: ts,
                        recv_ts: ts + rng.range(20.0, 200.0) as i64,
                        side: if rng.unit() < 0.5 {
                            Side::Long
                        } else {
                            Side::Short
                        } as i32,
                        price,
                        qty,
                        notional_usd: (price * qty * 100.0).round() / 100.0,
                    })),
                };
                if let Some(idx) = loader.push(event, ts)
                    && loader.pages[idx].len() >= loader.page
                {
                    loader.flush(idx).await;
                }
            }
        }
        if k % 2_000 == 0 && k > 0 {
            eprintln!(
                "  {:.0}% generated, {} rows inserted so far",
                percent(k as f64, cycles as f64),
                loader.rows.iter().sum::<u64>()
            );
        }
    }
    for idx in 0..4 {
        loader.flush(idx).await;
    }
    let wall = started.elapsed().as_secs_f64();
    let total_rows: u64 = loader.rows.iter().sum();
    println!("\n== Load ==");
    println!(
        "rows: liquidations {}, open_interest {}; total {total_rows}",
        loader.rows[0], loader.rows[1]
    );
    println!(
        "wall {wall:.1} s (generation+mapping+insert); mapping {:.1} s, insert {:.1} s; \
         insert throughput {:.0} rows/s, end-to-end {:.0} rows/s",
        loader.build_secs,
        loader.insert_secs,
        total_rows as f64 / loader.insert_secs,
        total_rows as f64 / wall
    );

    print_sizes(
        &client,
        &args.database,
        "as inserted (before background merges finish)",
    )
    .await;
    for t in ["liquidations", "open_interest"] {
        client.execute(&format!("OPTIMIZE TABLE {t} FINAL")).await?;
    }
    print_sizes(
        &client,
        &args.database,
        "after OPTIMIZE FINAL (steady state)",
    )
    .await;

    println!("\n== Compression per column (open_interest) ==");
    for r in client
        .query_rows(&format!(
            "SELECT name, data_compressed_bytes AS comp, data_uncompressed_bytes AS raw, compression_codec \
             FROM system.columns WHERE database = '{}' AND table = 'open_interest' ORDER BY comp DESC",
            args.database
        ))
        .await?
    {
        let (c, u) = (json_u64(&r["comp"]), json_u64(&r["raw"]));
        println!(
            "{:<12} {:>10.1} MiB compressed, {:>10.1} MiB raw, {:.2} B/row, codec {}",
            r["name"].as_str().unwrap(),
            c as f64 / 1_048_576.0,
            u as f64 / 1_048_576.0,
            c as f64 / loader.rows[1].max(1) as f64,
            r["compression_codec"].as_str().unwrap_or("")
        );
    }

    let start_ms = args.start_ms;
    let end_ms = start_ms + i64::from(args.days) * day_ms;
    let day_from = end_ms - day_ms;
    let queries: Vec<(&str, String)> = vec![
        (
            "sum of liquidations per symbol per day over the week",
            format!(
                "SELECT symbol, toDate(exchange_ts) AS d, sum(notional_usd) AS usd, count() AS n FROM liquidations WHERE exchange_ts >= toDateTime64({}/1000, 3) AND exchange_ts < toDateTime64({}/1000, 3) GROUP BY symbol, d",
                start_ms, end_ms
            ),
        ),
        (
            "OI of one symbol (BTC) over a day",
            format!(
                "SELECT exchange_ts, oi_base, oi_usd, mark_price FROM open_interest WHERE exchange = 'binance' AND symbol = 'BTC-USDT-PERP' AND exchange_ts >= toDateTime64({}/1000, 3) AND exchange_ts < toDateTime64({}/1000, 3) ORDER BY exchange_ts",
                day_from, end_ms
            ),
        ),
        (
            "OI of one symbol, hourly average over the week",
            format!(
                "SELECT toStartOfHour(exchange_ts) AS h, avg(oi_usd) FROM open_interest WHERE exchange = 'binance' AND symbol = 'ETH-USDT-PERP' AND exchange_ts >= toDateTime64({}/1000, 3) AND exchange_ts < toDateTime64({}/1000, 3) GROUP BY h ORDER BY h",
                start_ms, end_ms
            ),
        ),
        (
            "OI change over the last day for every symbol (first vs last)",
            format!(
                "SELECT symbol, argMin(oi_base, exchange_ts) AS first, argMax(oi_base, exchange_ts) AS last FROM open_interest WHERE exchange_ts >= toDateTime64({}/1000, 3) AND exchange_ts < toDateTime64({}/1000, 3) GROUP BY symbol",
                day_from, end_ms
            ),
        ),
        (
            "top 10 symbols by liquidated USD over the week",
            format!(
                "SELECT symbol, sum(notional_usd) AS usd FROM liquidations WHERE exchange_ts >= toDateTime64({}/1000, 3) GROUP BY symbol ORDER BY usd DESC LIMIT 10",
                start_ms
            ),
        ),
        (
            "row count of the whole open_interest table",
            "SELECT count() FROM open_interest".to_string(),
        ),
        (
            "full scan: average oi_usd over the whole week",
            "SELECT avg(oi_usd) FROM open_interest".to_string(),
        ),
    ];
    println!("\n== Queries (warm cache; 5 runs, median and best, client-side wall time) ==");
    for (name, sql) in queries {
        let mut times = Vec::new();
        let mut returned = 0;
        for _ in 0..5 {
            let t = Instant::now();
            returned = client.query_rows(&sql).await?.len();
            times.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!(
            "{name}: median {:.1} ms, best {:.1} ms ({returned} rows returned)",
            times[times.len() / 2],
            times[0]
        );
    }
    if args.cleanup {
        client
            .execute_server(&format!("DROP DATABASE IF EXISTS {}", args.database))
            .await?;
        println!("\ndatabase {} dropped", args.database);
    }
    Ok(())
}
