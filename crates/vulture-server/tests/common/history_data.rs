//! A deterministic data set for the `History` tests: six OI series on three exchanges for three hours, liquidations,
//! gaps (one of them a real hole in a series) and a signal, with stream IDs assigned the way Redis would.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use vulture_proto::v1::{
    Event, Gap, Liquidation, OpenInterest, Side, Signal, SignalType, event::Payload,
};
use vulture_server::store::{EntryFields, MemoryStore, RedisStore, Trim};
use vulture_server::ws::Clock;

/// 2026-10-26 00:00 UTC, a Monday.
pub const T0: i64 = 1_792_972_800_000;
pub const MIN: i64 = 60_000;
pub const HOUR: i64 = 3_600_000;
/// The data starts here and lasts three hours.
pub const START: i64 = T0 + HOUR;
pub const END: i64 = START + 3 * HOUR;
/// The test clock stands here.
pub const NOW: i64 = END + 30 * MIN;

pub const SERIES: [(&str, &str); 6] = [
    ("binance", "BTC-USDT-PERP"),
    ("binance", "ETH-USDT-PERP"),
    ("bybit", "BTC-USDT-PERP"),
    ("bybit", "BTC-USDC-PERP"),
    ("bybit", "SOL-USDT-PERP"),
    ("okx", "ETH-USDT-PERP"),
];

/// A clock the test moves by hand.
#[derive(Clone)]
pub struct SetClock(pub Arc<AtomicI64>);

impl SetClock {
    pub fn new(ms: i64) -> Self {
        Self(Arc::new(AtomicI64::new(ms)))
    }
    pub fn set(&self, ms: i64) {
        self.0.store(ms, Ordering::SeqCst);
    }
}

impl Clock for SetClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

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
}

fn raw(symbol: &str) -> String {
    symbol.replace("-PERP", "").replace('-', "")
}

/// The events without IDs, ordered by receive time.
pub fn raw_events() -> Vec<Event> {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut out: Vec<(i64, Event)> = Vec::new();
    for (si, (exchange, symbol)) in SERIES.iter().enumerate() {
        let mut base = 1_000.0 * (si as f64 + 1.0);
        let mut mark = 100.0 * (si as f64 + 1.0);
        let mut t = START + (si as i64) * 1_000;
        let mut n = 0u64;
        while t < END {
            base *= 1.0 + (rng.unit() - 0.5) * 0.002;
            mark *= 1.0 + (rng.unit() - 0.5) * 0.003;
            // A spike of Binance BTC: +6% over four minutes starting two hours in.
            if *exchange == "binance" && *symbol == "BTC-USDT-PERP" {
                let into = t - (START + 2 * HOUR);
                if (0..4 * MIN).contains(&into) {
                    base *= 1.0 + 0.06 / 8.0;
                }
            }
            // The bybit SOL series is silent for half an hour (a real hole next to a Gap record).
            let silent = *exchange == "bybit"
                && *symbol == "SOL-USDT-PERP"
                && (START + 70 * MIN..START + 100 * MIN).contains(&t);
            if !silent {
                let recv = t + 40;
                out.push((
                    recv,
                    Event {
                        id: String::new(),
                        payload: Some(Payload::OpenInterest(OpenInterest {
                            exchange: (*exchange).into(),
                            symbol_raw: raw(symbol),
                            symbol: (*symbol).into(),
                            exchange_ts: t,
                            recv_ts: recv,
                            oi_base: base,
                            oi_usd: if *exchange == "okx" && n % 10 == 3 {
                                None
                            } else {
                                Some(base * mark)
                            },
                            mark_price: if n % 17 == 5 { None } else { Some(mark) },
                            // OKX series carry no funding; the others a rate that steps every 40 samples and
                            // an index price a little below the mark price (a positive basis).
                            funding_rate: (*exchange != "okx")
                                .then_some(0.0001 * (si as f64 + 1.0) + 0.00001 * (n / 40) as f64),
                            index_price: (*exchange != "okx" && n % 23 != 7)
                                .then_some(mark * (1.0 - 0.0003 * (si as f64 + 1.0))),
                            next_funding_ts: (*exchange != "okx")
                                .then(|| (t / 28_800_000 + 1) * 28_800_000),
                        })),
                    },
                ));
            }
            n += 1;
            t += 30_000;
        }
    }
    // Liquidations: roughly one every 20 s over random series.
    let mut t = START + 5_000;
    while t < END {
        let (exchange, symbol) = SERIES[(rng.next() % SERIES.len() as u64) as usize];
        let usd = 1_000.0 + rng.unit() * 500_000.0;
        let side = if rng.unit() < 0.5 {
            Side::Long
        } else {
            Side::Short
        };
        out.push((
            t + 30,
            Event {
                id: String::new(),
                payload: Some(Payload::Liquidation(Liquidation {
                    exchange: exchange.into(),
                    symbol_raw: raw(symbol),
                    symbol: symbol.into(),
                    exchange_ts: t,
                    recv_ts: t + 30,
                    side: side as i32,
                    price: 100.0,
                    qty: usd / 100.0,
                    notional_usd: usd,
                })),
            },
        ));
        t += 10_000 + (rng.next() % 20_000) as i64;
    }
    // Gaps, written at their end like the connectors do.
    for (exchange, stream, from, to) in [
        (
            "bybit",
            "open_interest",
            START + 70 * MIN,
            START + 100 * MIN,
        ),
        (
            "binance",
            "liquidations",
            START + 50 * MIN,
            START + 51 * MIN,
        ),
    ] {
        out.push((
            to + 100,
            Event {
                id: String::new(),
                payload: Some(Payload::Gap(Gap {
                    exchange: exchange.into(),
                    stream: stream.into(),
                    from_ts: from,
                    to_ts: to,
                })),
            },
        ));
    }
    out.push((
        START + 2 * HOUR + 5 * MIN + 500,
        Event {
            id: String::new(),
            payload: Some(Payload::Signal(Signal {
                r#type: SignalType::OiSpikeUp as i32,
                exchange: "binance".into(),
                symbol: "BTC-USDT-PERP".into(),
                window_secs: 300,
                change_pct: 6.0,
                change_abs: 60.0,
                threshold: 3.0,
                ts: START + 2 * HOUR + 5 * MIN,
                details: [("windows".to_string(), "300".to_string())]
                    .into_iter()
                    .collect(),
            })),
        },
    ));
    out.sort_by_key(|(t, _)| *t);
    out.into_iter().map(|(_, e)| e).collect()
}

fn recv_of(e: &Event) -> i64 {
    match e.payload.as_ref() {
        Some(Payload::Liquidation(l)) => l.recv_ts,
        Some(Payload::OpenInterest(o)) => o.recv_ts,
        Some(Payload::Gap(g)) => g.to_ts + 100,
        Some(Payload::Signal(s)) => s.ts + 500,
        None => 0,
    }
}

/// A stream that assigns IDs from a hand-set clock.
pub struct Stream {
    pub store: Arc<MemoryStore>,
    clock: SetClock,
}

impl Stream {
    pub fn new() -> Self {
        let clock = SetClock::new(0);
        Self {
            store: Arc::new(MemoryStore::with_clock(Box::new(clock.clone()))),
            clock,
        }
    }

    /// Appends one event with the ID Redis would give it at `at_ms` and returns it with the ID set.
    pub async fn append(&self, e: &Event, at_ms: i64) -> Event {
        self.clock.set(at_ms);
        let fields = EntryFields::from_event(e).expect("payload");
        let ids = self
            .store
            .append(
                &[fields],
                Trim {
                    min_id_ms: 0,
                    maxlen: 100_000_000,
                },
            )
            .await
            .expect("append");
        Event {
            id: ids[0].clone(),
            payload: e.payload.clone(),
        }
    }
}

impl Default for Stream {
    fn default() -> Self {
        Self::new()
    }
}

/// The whole data set written to a stream.
pub struct Dataset {
    pub stream: Stream,
    /// Every event with its ID, in stream order.
    pub events: Vec<Event>,
}

pub async fn dataset() -> Dataset {
    let stream = Stream::new();
    let mut events = Vec::new();
    for e in raw_events() {
        let at = recv_of(&e);
        events.push(stream.append(&e, at).await);
    }
    Dataset { stream, events }
}

impl Dataset {
    /// The ID after which `fraction` of the events are behind the watermark.
    pub fn watermark_at(&self, fraction: f64) -> (usize, String) {
        let i = ((self.events.len() as f64) * fraction) as usize;
        let i = i.clamp(1, self.events.len() - 1);
        (i, self.events[i - 1].id.clone())
    }
}
