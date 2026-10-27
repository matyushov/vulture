//! Detector replay (B4): the engine's own core over a stretch of stored history, with other thresholds.
//!
//! [`Core`] is the synchronous heart of the live engine; the replay feeds it the stored events in stream order
//! exactly as the engine receives them, first the warm-up (state only, no output, like the engine's start-up
//! replay) and then the period itself, and collects the signals it would have produced. Nothing else happens: no
//! Redis, no delivery queues, no rules, no change to `alerts.json`; the core has no rules loaded and its hits are
//! discarded. Several parameter sets run side by side over the same events, so thresholds can be compared.
//!
//! Events are read in chunks of stream-ID time from ClickHouse, ordered by stream ID (the order the live engine
//! saw them in), so memory stays bounded for a long period.

use std::sync::Arc;

use arc_swap::ArcSwap;
use futures_util::future::BoxFuture;
use vulture_proto::v1::{Event, Signal, event::Payload};

use crate::alerts::core::{Core, Output};
use crate::alerts::rules::RuleSet;
use crate::alerts::stats::EngineStats;
use crate::config::AlertsConfig;
use crate::history::{ChClient, ReadSettings};
use crate::query::cold::{Kind, Scope};
use crate::query::event::event_key;
use crate::query::sql;

/// Where the replay reads events from.
pub trait ReplaySource: Send + Sync {
    /// Liquidations, OI samples and gaps whose stream ID lies in `[from_ms, to_ms)`, in stream order.
    fn events(
        &self,
        from_ms: i64,
        to_ms: i64,
        scope: &Scope,
    ) -> BoxFuture<'_, Result<Vec<Event>, String>>;
}

/// ClickHouse as the replay source.
pub struct ChReplay {
    client: Arc<ChClient>,
    settings: ReadSettings,
}

impl ChReplay {
    /// Wraps a client.
    pub fn new(client: Arc<ChClient>) -> Self {
        Self {
            client,
            settings: ReadSettings {
                max_execution_time_secs: 300,
                max_memory_usage: 4 << 30,
                timeout: std::time::Duration::from_secs(310),
            },
        }
    }
}

impl ReplaySource for ChReplay {
    fn events(
        &self,
        from_ms: i64,
        to_ms: i64,
        scope: &Scope,
    ) -> BoxFuture<'_, Result<Vec<Event>, String>> {
        let scope = scope.clone();
        Box::pin(async move {
            let mut out: Vec<(crate::query::cold::EventKey, Event)> = Vec::new();
            for kind in [Kind::Liquidation, Kind::OpenInterest, Kind::Gap] {
                let q = sql::replay_events(kind, from_ms, to_ms, 600_000, &scope);
                let (rows, _) = self
                    .client
                    .query_rows_with(&q, &self.settings)
                    .await
                    .map_err(|e| e.to_string())?;
                for r in rows {
                    let e = crate::query::ch::decode_event(kind, &r);
                    if let Some(k) = event_key(&e) {
                        // Stream order is the order of the IDs, not of the event times.
                        out.push((k, e));
                    }
                }
            }
            out.sort_by_key(|(k, _)| (k.id_ms, k.id_seq));
            Ok(out.into_iter().map(|(_, e)| e).collect())
        })
    }
}

/// One parameter set to replay.
#[derive(Debug, Clone)]
pub struct ReplayCase {
    /// Label in the output.
    pub label: String,
    /// The configuration in force.
    pub config: AlertsConfig,
}

/// The signals one case produced.
#[derive(Debug, Clone)]
pub struct ReplayRun {
    /// Label.
    pub label: String,
    /// The signals, in the order they fired.
    pub signals: Vec<Signal>,
}

/// Replay settings.
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Start of the period (signals before it are not reported), ms.
    pub from: i64,
    /// End of the period, ms.
    pub to: i64,
    /// State-only run before `from`, ms.
    pub warmup_ms: i64,
    /// Length of one read, ms.
    pub chunk_ms: i64,
    /// Filters.
    pub scope: Scope,
}

/// Runs every case over the same events.
pub async fn replay(
    src: &dyn ReplaySource,
    cases: &[ReplayCase],
    cfg: &ReplayConfig,
) -> Result<(Vec<ReplayRun>, u64), String> {
    struct Slot {
        core: Core,
        signals: Vec<Signal>,
    }
    let mut slots: Vec<Slot> = cases
        .iter()
        .map(|c| Slot {
            core: Core::new(
                &c.config,
                Arc::new(ArcSwap::from_pointee(RuleSet::default())),
                Arc::new(EngineStats::default()),
            ),
            signals: Vec::new(),
        })
        .collect();
    let mut fed = 0u64;
    let mut at = cfg.from - cfg.warmup_ms;
    while at < cfg.to {
        let next = (at + cfg.chunk_ms).min(cfg.to);
        let events = src.events(at, next, &cfg.scope).await?;
        fed += events.len() as u64;
        for slot in &mut slots {
            let mut out = Output::default();
            for e in &events {
                let id_ms = event_key(e).map_or(0, |k| k.id_ms as i64);
                let live = id_ms >= cfg.from;
                // The live engine ticks every second; one tick per chunk boundary is enough to end quiet cascades.
                slot.core.process(e, live, &mut out);
            }
            if next > cfg.from {
                slot.core.tick(next, &mut out);
            }
            slot.signals.append(&mut out.signals);
        }
        at = next;
    }
    // Close what is still open at the end of the period.
    for slot in &mut slots {
        let mut out = Output::default();
        slot.core.tick(cfg.to + 3_600_000, &mut out);
        slot.signals.append(&mut out.signals);
        slot.signals.retain(|s| s.ts >= cfg.from);
    }
    Ok((
        slots
            .into_iter()
            .zip(cases)
            .map(|(s, c)| ReplayRun {
                label: c.label.clone(),
                signals: s.signals,
            })
            .collect(),
        fed,
    ))
}

/// Whether an event is something the engine consumes.
pub fn is_engine_input(e: &Event) -> bool {
    matches!(
        e.payload,
        Some(Payload::Liquidation(_) | Payload::OpenInterest(_) | Payload::Gap(_))
    )
}

/// In-memory source for tests: events with IDs.
pub struct MemoryReplay(pub Vec<Event>);

impl ReplaySource for MemoryReplay {
    fn events(
        &self,
        from_ms: i64,
        to_ms: i64,
        scope: &Scope,
    ) -> BoxFuture<'_, Result<Vec<Event>, String>> {
        let scope = scope.clone();
        Box::pin(async move {
            let mut v: Vec<(crate::query::cold::EventKey, Event)> = self
                .0
                .iter()
                .filter(|e| is_engine_input(e))
                .filter_map(|e| event_key(e).map(|k| (k, e.clone())))
                .filter(|(k, _)| (k.id_ms as i64) >= from_ms && (k.id_ms as i64) < to_ms)
                .filter(|(_, e)| match e.payload.as_ref() {
                    Some(Payload::Liquidation(l)) => scope.matches(&l.exchange, &l.symbol),
                    Some(Payload::OpenInterest(o)) => scope.matches(&o.exchange, &o.symbol),
                    _ => true,
                })
                .collect();
            v.sort_by_key(|(k, _)| (k.id_ms, k.id_seq));
            Ok(v.into_iter().map(|(_, e)| e).collect())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vulture_proto::v1::{Liquidation, OpenInterest, Side, SignalType};

    const MIN: i64 = 60_000;
    const T0: i64 = 1_800_000_000_000;

    fn oi(ts: i64, base: f64) -> Event {
        Event {
            id: format!("{}-0", ts + 40),
            payload: Some(Payload::OpenInterest(OpenInterest {
                exchange: "binance".into(),
                symbol_raw: "BTCUSDT".into(),
                symbol: "BTC-USDT-PERP".into(),
                exchange_ts: ts,
                recv_ts: ts + 40,
                oi_base: base,
                oi_usd: Some(base * 1000.0),
                mark_price: Some(1000.0),
                ..OpenInterest::default()
            })),
        }
    }

    /// Four hours of flat OI at 10 000 BTC (30 s samples) and a +4% jump at hour 3.
    fn history() -> Vec<Event> {
        let mut v = Vec::new();
        let mut t = T0;
        while t < T0 + 4 * 60 * MIN {
            let base = if t >= T0 + 180 * MIN {
                10_400.0
            } else {
                10_000.0 + (t / 30_000 % 3) as f64
            };
            v.push(oi(t, base));
            t += 30_000;
        }
        v
    }

    fn case(label: &str, params: &str) -> ReplayCase {
        ReplayCase {
            label: label.into(),
            config: crate::analyze::params::apply(
                crate::analyze::params::Detector::OiSpike,
                &AlertsConfig::default(),
                params,
            )
            .unwrap(),
        }
    }

    fn cfg(from: i64, warmup: i64) -> ReplayConfig {
        ReplayConfig {
            from,
            to: T0 + 4 * 60 * MIN,
            warmup_ms: warmup,
            chunk_ms: 20 * MIN,
            scope: Scope::default(),
        }
    }

    #[tokio::test]
    async fn thresholds_change_what_would_have_fired() {
        let src = MemoryReplay(history());
        let cases = [
            case("default", ""),
            case(
                "sensitive",
                "threshold_pct.300=2,min_change_usd.300=1000,windows=300,z_min=2,min_baseline_points=10",
            ),
            case(
                "strict",
                "threshold_pct.300=9,threshold_pct.60=9,threshold_pct.900=9,threshold_pct.3600=20,threshold_pct.14400=20",
            ),
        ];
        let (runs, fed) = replay(&src, &cases, &cfg(T0 + 2 * 60 * MIN, 2 * 60 * MIN))
            .await
            .unwrap();
        assert_eq!(fed as usize, history().len());
        let by = |l: &str| runs.iter().find(|r| r.label == l).unwrap();
        let up = |r: &ReplayRun| {
            r.signals
                .iter()
                .filter(|s| s.r#type == SignalType::OiSpikeUp as i32)
                .count()
        };
        assert!(
            up(by("sensitive")) >= 1,
            "the 4% jump passes a 2% threshold"
        );
        assert_eq!(up(by("strict")), 0, "and fails a 9% one");
        assert!(
            by("sensitive")
                .signals
                .iter()
                .all(|s| s.ts >= T0 + 2 * 60 * MIN),
            "nothing before the period"
        );
        let s = &by("sensitive").signals[0];
        assert_eq!(
            (s.exchange.as_str(), s.symbol.as_str()),
            ("binance", "BTC-USDT-PERP")
        );
        assert!(
            s.ts >= T0 + 180 * MIN && s.ts < T0 + 185 * MIN,
            "{}",
            s.ts - T0
        );
    }

    #[tokio::test]
    async fn the_replay_equals_feeding_the_core_directly() {
        let events = history();
        let c = case(
            "same",
            "threshold_pct.300=2,min_change_usd.300=1000,windows=300,z_min=2,min_baseline_points=10",
        );
        let (runs, _) = replay(
            &MemoryReplay(events.clone()),
            std::slice::from_ref(&c),
            &cfg(T0 + 2 * 60 * MIN, 2 * 60 * MIN),
        )
        .await
        .unwrap();
        let mut core = Core::new(
            &c.config,
            Arc::new(ArcSwap::from_pointee(RuleSet::default())),
            Arc::new(EngineStats::default()),
        );
        let mut out = Output::default();
        for e in &events {
            let ts = match e.payload.as_ref() {
                Some(Payload::OpenInterest(o)) => o.recv_ts,
                _ => 0,
            };
            core.process(e, ts >= T0 + 2 * 60 * MIN, &mut out);
        }
        let direct: Vec<(i64, i32, u32)> = out
            .signals
            .iter()
            .map(|s| (s.ts, s.r#type, s.window_secs))
            .collect();
        let replayed: Vec<(i64, i32, u32)> = runs[0]
            .signals
            .iter()
            .map(|s| (s.ts, s.r#type, s.window_secs))
            .collect();
        assert!(!direct.is_empty());
        assert_eq!(replayed, direct);
    }

    #[tokio::test]
    async fn without_warmup_the_detector_is_blind_at_the_start() {
        let src = MemoryReplay(history());
        let c = case(
            "s",
            "threshold_pct.300=2,min_change_usd.300=1000,windows=300,z_min=2,min_baseline_points=10",
        );
        // Starting right at the jump with no warm-up: no history to measure the change against.
        let (blind, _) = replay(&src, std::slice::from_ref(&c), &cfg(T0 + 180 * MIN, 0))
            .await
            .unwrap();
        let (sighted, _) = replay(
            &src,
            std::slice::from_ref(&c),
            &cfg(T0 + 180 * MIN, 60 * MIN),
        )
        .await
        .unwrap();
        assert!(blind[0].signals.is_empty());
        assert!(!sighted[0].signals.is_empty());
    }

    #[tokio::test]
    async fn cascades_replay_and_close_at_the_end() {
        let mut events = history();
        // A burst of large liquidations in the last 10 minutes on top of a quiet baseline.
        let t = T0 + 3 * 60 * MIN + 30 * MIN;
        for i in 0..40i64 {
            events.push(Event {
                id: format!("{}-{}", t + i * 1000 + 30, i),
                payload: Some(Payload::Liquidation(Liquidation {
                    exchange: "binance".into(),
                    symbol_raw: "BTCUSDT".into(),
                    symbol: "BTC-USDT-PERP".into(),
                    exchange_ts: t + i * 1000,
                    recv_ts: t + i * 1000 + 30,
                    side: Side::Long as i32,
                    price: 1000.0,
                    qty: 500.0,
                    notional_usd: 500_000.0,
                })),
            });
        }
        // A few small ones earlier give the detector a baseline.
        for i in 0..200i64 {
            events.push(Event {
                id: format!("{}-{}", T0 + i * 40_000 + 30, 900 + i),
                payload: Some(Payload::Liquidation(Liquidation {
                    exchange: "binance".into(),
                    symbol_raw: "BTCUSDT".into(),
                    symbol: "BTC-USDT-PERP".into(),
                    exchange_ts: T0 + i * 40_000,
                    recv_ts: T0 + i * 40_000 + 30,
                    side: Side::Short as i32,
                    price: 1000.0,
                    qty: 1.0,
                    notional_usd: 1_000.0,
                })),
            });
        }
        let c = ReplayCase {
            label: "cascade".into(),
            config: crate::analyze::params::apply(
                crate::analyze::params::Detector::LiqCascade,
                &AlertsConfig::default(),
                "min_baseline_minutes=30,floor_usd=1000000,floor_usd_exchange=500000,min_count=5",
            )
            .unwrap(),
        };
        let (runs, _) = replay(&MemoryReplay(events), &[c], &cfg(T0 + 60 * MIN, 60 * MIN))
            .await
            .unwrap();
        let phases: Vec<&str> = runs[0]
            .signals
            .iter()
            .filter(|s| s.r#type == SignalType::LiqCascade as i32)
            .filter_map(|s| s.details.get("phase").map(String::as_str))
            .collect();
        assert!(phases.contains(&"start"), "{phases:?}");
        assert!(
            phases.contains(&"end"),
            "the open cascade is closed at the end of the period: {phases:?}"
        );
    }
}
