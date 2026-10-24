//! Measurements of the analytics engine (task V4a) on synthetic data: memory of the ring buffers, CPU under
//! load, and warm-up time from a stream.
//!
//! ```sh
//! # memory: 2000 symbols x 4 h of 30 s samples
//! cargo run --release -p vulture-server --example synth_alerts -- memory --symbols 2000
//! # CPU: events through the real tap and engine task at a fixed rate (run under /usr/bin/time -l for CPU time)
//! cargo run --release -p vulture-server --example synth_alerts -- load --symbols 2000 --rate 3500 --secs 30
//! # warm-up: fill a stream (the in-memory store, or Redis with --redis-url redis://127.0.0.1:6379) and time it
//! cargo run --release -p vulture-server --example synth_alerts -- warmup --symbols 2000 --hours 4
//! ```
//!
//! The synthetic market is a quiet random walk with a few injected spikes and liquidation bursts. Nothing here
//! talks to an exchange, Telegram or Discord. With `--redis-url` use a scratch Redis: the example writes under a
//! unique stream key and deletes it at the end.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use clap::{Parser, Subcommand};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::{Event, Liquidation, OpenInterest, Side, event::Payload};
use vulture_server::alerts::core::{Core, Output};
use vulture_server::alerts::rules::RuleSet;
use vulture_server::alerts::{Alerts, EngineInput, EngineStats};
use vulture_server::config::{AlertsConfig, Config, Secret};
use vulture_server::store::{EntryFields, MemoryStore, RedisClientStore, RedisStore, Trim};
use vulture_server::ws::SystemClock;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand)]
enum Mode {
    /// Memory of the ring buffers with N symbols x 4 h.
    Memory {
        #[arg(long, default_value_t = 2000)]
        symbols: usize,
    },
    /// CPU under a fixed event rate through the tap and the engine task.
    Load {
        #[arg(long, default_value_t = 2000)]
        symbols: usize,
        /// Events per second (open interest and liquidations).
        #[arg(long, default_value_t = 3500)]
        rate: u64,
        #[arg(long, default_value_t = 30)]
        secs: u64,
        /// Do not run the engine: measures the generator alone, to subtract it.
        #[arg(long)]
        generator_only: bool,
        /// Every symbol drops 8% in the second round (a market-wide crash): the detector's expensive path.
        #[arg(long)]
        crash: bool,
    },
    /// Warm-up time from a stream holding N hours of data.
    Warmup {
        #[arg(long, default_value_t = 2000)]
        symbols: usize,
        #[arg(long, default_value_t = 4)]
        hours: u64,
        /// Use this Redis instead of the in-memory store.
        #[arg(long)]
        redis_url: Option<String>,
    },
}

fn rss_kb() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok();
    out.and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(0)
}

struct Gen {
    symbols: Vec<String>,
    state: Vec<f64>,
    rng: u64,
}

impl Gen {
    fn new(n: usize) -> Self {
        Self {
            symbols: (0..n).map(|i| format!("SYM{i}-USDT-PERP")).collect(),
            state: (0..n).map(|i| 5_000.0 + (i % 97) as f64 * 100.0).collect(),
            rng: 0x9E37_79B9_7F4A_7C15,
        }
    }
    fn next_f64(&mut self) -> f64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        (self.rng >> 11) as f64 / (1u64 << 53) as f64
    }
    fn oi(&mut self, sym: usize, ts: i64) -> Event {
        let step = (self.next_f64() - 0.5) * 0.004;
        self.state[sym] *= 1.0 + step;
        let base = self.state[sym];
        Event {
            id: String::new(),
            payload: Some(Payload::OpenInterest(OpenInterest {
                exchange: "binance".into(),
                symbol_raw: self.symbols[sym].replace('-', ""),
                symbol: self.symbols[sym].clone(),
                exchange_ts: ts,
                recv_ts: ts,
                oi_base: base,
                oi_usd: Some(base * 1000.0),
                mark_price: Some(1000.0),
            })),
        }
    }
    fn liq(&mut self, sym: usize, ts: i64) -> Event {
        let usd = 5_000.0 + self.next_f64() * 60_000.0;
        Event {
            id: String::new(),
            payload: Some(Payload::Liquidation(Liquidation {
                exchange: "binance".into(),
                symbol_raw: self.symbols[sym].replace('-', ""),
                symbol: self.symbols[sym].clone(),
                exchange_ts: ts,
                recv_ts: ts,
                side: if self.next_f64() < 0.5 {
                    Side::Long
                } else {
                    Side::Short
                } as i32,
                price: 1000.0,
                qty: usd / 1000.0,
                notional_usd: usd,
            })),
        }
    }
}

fn core(symbols: usize) -> (Core, Arc<EngineStats>) {
    let cfg = AlertsConfig {
        max_symbols: symbols + 10,
        ..AlertsConfig::default()
    };
    let stats = Arc::new(EngineStats::default());
    (
        Core::new(
            &cfg,
            Arc::new(ArcSwap::from_pointee(RuleSet::default())),
            stats.clone(),
        ),
        stats,
    )
}

fn memory(symbols: usize) {
    let before = rss_kb();
    let (mut core, stats) = core(symbols);
    let mut g = Gen::new(symbols);
    let samples = 4 * 3600 / 30;
    let mut out = Output::default();
    let t0 = 1_760_000_000_000i64;
    let started = Instant::now();
    for k in 0..samples {
        for s in 0..symbols {
            let ev = g.oi(s, t0 + k as i64 * 30_000);
            core.process(&ev, false, &mut out);
        }
    }
    core.refresh_now(t0 + samples as i64 * 30_000);
    let after = rss_kb();
    println!(
        "memory: {symbols} symbols x {samples} samples ({} events replayed in {:.1}s)",
        symbols * samples,
        started.elapsed().as_secs_f64()
    );
    println!(
        "  series={} samples={} buffer_capacity_bytes={} ({:.1} MiB), sample={} bytes",
        stats.series.load(Ordering::Relaxed),
        stats.samples.load(Ordering::Relaxed),
        stats.memory_bytes.load(Ordering::Relaxed),
        stats.memory_bytes.load(Ordering::Relaxed) as f64 / 1048576.0,
        Core::sample_bytes()
    );
    println!(
        "  process RSS before={} MiB after={} MiB delta={:.1} MiB",
        before / 1024,
        after / 1024,
        (after as f64 - before as f64) / 1024.0
    );
}

async fn load(symbols: usize, rate: u64, secs: u64, generator_only: bool, crash: bool) {
    let stats = Arc::new(EngineStats::default());
    let (input, rx) = EngineInput::channel(100_000, stats.clone(), Arc::new(SystemClock));
    let (writer_tx, mut writer_rx) = mpsc::channel::<Event>(10_000);
    let drain = tokio::spawn(async move { while writer_rx.recv().await.is_some() {} });
    let stop = CancellationToken::new();
    let cfg = Arc::new(AlertsConfig {
        max_symbols: symbols + 10,
        warmup_secs: 0,
        ..AlertsConfig::default()
    });
    let rules = Arc::new(ArcSwap::from_pointee(RuleSet::default()));
    let mut g = Gen::new(symbols);
    // Four hours of history first (state only), so the windows are full when the measurement starts. It ends at
    // the present: the engine drops buffers that are stale against the wall clock.
    let t0 = vulture_server::metrics::unix_now_ms() - 4 * 3600 * 1000;
    let mut engine = None;
    let mut core = Core::new(&cfg, rules.clone(), stats.clone());
    let mut out = Output::default();
    let samples = 4 * 3600 / 30;
    for k in 0..samples {
        for s in 0..symbols {
            let ev = g.oi(s, t0 + k as i64 * 30_000);
            core.process(&ev, false, &mut out);
        }
    }
    if !generator_only {
        let delivery =
            vulture_server::alerts::Delivery::new(Default::default(), stop.clone()).unwrap();
        let task = vulture_server::alerts::EngineTask {
            core,
            rx,
            writer: writer_tx.clone(),
            delivery,
            store: Arc::new(MemoryStore::new()),
            clock: Arc::new(SystemClock),
            cfg: cfg.clone(),
            shutdown: stop.clone(),
        };
        engine = Some(tokio::spawn(task.run()));
    }
    // Event time advances so that every symbol gets a sample every 30 s of event time: every OI event is a
    // full evaluation (the worst case for CPU). One event in 20 is a liquidation.
    let per_tick = (rate / 100).max(1);
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut n: u64 = 0;
    let started = Instant::now();
    let total_ticks = secs * 100;
    let before = rss_kb();
    for _ in 0..total_ticks {
        tick.tick().await;
        for _ in 0..per_tick {
            let slot = n as usize % symbols;
            let round = n / symbols as u64;
            let ts = t0 + (samples as i64 + round as i64) * 30_000 + slot as i64;
            if crash && round == 2 && n % symbols as u64 == slot as u64 {
                g.state[slot] *= 0.92;
            }
            let ev = if n % 20 == 19 {
                g.liq(slot, ts)
            } else {
                g.oi(slot, ts)
            };
            if !generator_only {
                input.offer(&ev);
            }
            n += 1;
        }
    }
    let wall = started.elapsed().as_secs_f64();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let evals = stats.eval_count.load(Ordering::Relaxed);
    let ns = stats.eval_ns_total.load(Ordering::Relaxed);
    println!(
        "load: {symbols} symbols, {} events offered in {wall:.1}s ({:.0}/s), engine={}",
        n,
        n as f64 / wall,
        !generator_only
    );
    if !generator_only {
        println!(
            "  evaluated={evals} dropped={} mean={:.2} us/event max={:.2} ms signals(up/down/cascade)={}/{}/{}",
            stats.input_dropped.load(Ordering::Relaxed),
            ns as f64 / evals.max(1) as f64 / 1000.0,
            stats.eval_ns_max.load(Ordering::Relaxed) as f64 / 1e6,
            stats.signals[0].load(Ordering::Relaxed),
            stats.signals[1].load(Ordering::Relaxed),
            stats.signals[2].load(Ordering::Relaxed)
        );
        println!(
            "  engine CPU per second of load ~ {:.1} ms (evaluation only; run under `/usr/bin/time -l` for the whole process)",
            ns as f64 / 1e6 / wall
        );
    }
    println!(
        "  RSS before={} MiB after={} MiB",
        before / 1024,
        rss_kb() / 1024
    );
    stop.cancel();
    if let Some(e) = engine {
        let _ = e.await;
    }
    drop(writer_tx);
    drain.abort();
}

async fn warmup(symbols: usize, hours: u64, redis_url: Option<String>) {
    let key = format!("vulture:synth-alerts:{}", std::process::id());
    let store: Arc<dyn RedisStore> = match &redis_url {
        Some(url) => Arc::new(RedisClientStore::new(&Secret::new(url.clone()), &key).unwrap()),
        None => Arc::new(MemoryStore::new()),
    };
    let mut g = Gen::new(symbols);
    // Entries are appended with stream IDs from "now": the memory store and Redis assign IDs themselves, so the
    // synthetic history is written with fresh IDs and event timestamps in the past; the engine reads by ID, so
    // the replay window is the whole stream.
    let now = vulture_server::metrics::unix_now_ms();
    let samples = hours * 3600 / 30;
    let started = Instant::now();
    let mut batch: Vec<EntryFields> = Vec::with_capacity(5000);
    let mut total = 0u64;
    for k in 0..samples {
        let ts = now - ((samples - k) as i64) * 30_000;
        for s in 0..symbols {
            batch.push(EntryFields::from_event(&g.oi(s, ts + s as i64)).unwrap());
            if s % 25 == 0 {
                batch.push(EntryFields::from_event(&g.liq(s, ts + s as i64)).unwrap());
            }
            if batch.len() >= 5000 {
                total += batch.len() as u64;
                store
                    .append(
                        &batch,
                        Trim {
                            min_id_ms: 0,
                            maxlen: 100_000_000,
                        },
                    )
                    .await
                    .unwrap();
                batch.clear();
            }
        }
    }
    if !batch.is_empty() {
        total += batch.len() as u64;
        store
            .append(
                &batch,
                Trim {
                    min_id_ms: 0,
                    maxlen: 100_000_000,
                },
            )
            .await
            .unwrap();
    }
    println!(
        "filled {total} entries ({hours} h, {symbols} symbols) in {:.1}s into {}",
        started.elapsed().as_secs_f64(),
        if redis_url.is_some() {
            "Redis"
        } else {
            "the memory store"
        }
    );
    // The stream IDs are "now", so the warm-up window must cover the synthetic span: the engine reads from
    // `now - warmup_secs` by ID, which includes everything just written.
    let mut config = Config::default();
    config.alerts.warmup_secs = hours * 3600;
    config.alerts.max_symbols = symbols + 10;
    config.server.data_dir =
        std::env::temp_dir().join(format!("vulture-synth-alerts-{}", std::process::id()));
    std::fs::create_dir_all(&config.server.data_dir).unwrap();
    let stop = CancellationToken::new();
    let alerts = Alerts::open(&config, stop.clone()).unwrap();
    let (writer_tx, _writer_rx) = mpsc::channel::<Event>(10_000);
    let before = rss_kb();
    let (_input, handle) = alerts.spawn_engine(
        store.clone(),
        writer_tx,
        Arc::new(SystemClock),
        stop.clone(),
    );
    while alerts.stats.warmed_up.load(Ordering::Relaxed) == 0 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    println!(
        "warm-up: {} entries replayed in {:.2}s ({:.0} entries/s), series={} samples={}, RSS +{:.1} MiB",
        alerts.stats.warmup_events.load(Ordering::Relaxed),
        alerts.stats.warmup_ms.load(Ordering::Relaxed) as f64 / 1000.0,
        alerts.stats.warmup_events.load(Ordering::Relaxed) as f64
            / (alerts.stats.warmup_ms.load(Ordering::Relaxed).max(1) as f64 / 1000.0),
        alerts.stats.series.load(Ordering::Relaxed),
        alerts.stats.samples.load(Ordering::Relaxed),
        (rss_kb() as f64 - before as f64) / 1024.0
    );
    stop.cancel();
    let _ = handle.await;
    let _ = std::fs::remove_dir_all(&config.server.data_dir);
    if let Some(url) = redis_url {
        let client = redis::Client::open(url.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
    }
}

#[tokio::main]
async fn main() {
    match Args::parse().mode {
        Mode::Memory { symbols } => memory(symbols),
        Mode::Load {
            symbols,
            rate,
            secs,
            generator_only,
            crash,
        } => load(symbols, rate, secs, generator_only, crash).await,
        Mode::Warmup {
            symbols,
            hours,
            redis_url,
        } => warmup(symbols, hours, redis_url).await,
    }
}
