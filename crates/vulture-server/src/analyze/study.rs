//! Event study of signals (B3): what the market did after each signal.
//!
//! For every signal the forward return of the mark price at +5, +15 and +60 minutes is measured (mark prices are
//! those carried by the OI rows, nearest sample within a tolerance), and the liquidated USD of the following hour.
//! Statistics are kept per signal type, window and exchange (and optionally per strength bucket).
//!
//! **Expected direction.** The study does not decide whether a signal is bullish or bearish; it measures both
//! continuation and reversal of the move that came with the signal:
//!
//! * OI spikes: the direction of the mark price over the signal's own window (price at the signal against price one
//!   window earlier). A rise of OI with the price rising is read as "continuation = up".
//! * Liquidation cascades (no symbol): the direction the liquidations push the market: more liquidated longs than
//!   shorts means down, otherwise up. The mark price of a market proxy symbol (default Binance BTC-USDT-PERP) stands
//!   for the market; liquidations of the following hour are summed over the signal's exchange scope.
//!
//! A *continuation hit* at threshold `t` is a forward return of at least `t` percent in the expected direction; a
//! *reversal hit* one of at least `t` percent against it. The shares are over the signals that have a direction and
//! a return at that horizon. Signals without a usable price, direction or horizon are counted as excluded, never
//! dropped silently.

use std::collections::BTreeMap;

use futures_util::future::BoxFuture;
use futures_util::stream::StreamExt;
use serde::Serialize;

use super::stats::Dist;

/// Kind of a stored signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
    /// `oi_spike_up`.
    OiSpikeUp,
    /// `oi_spike_down`.
    OiSpikeDown,
    /// `liq_cascade`.
    LiqCascade,
}

impl SignalKind {
    /// Name as stored in ClickHouse.
    pub fn name(self) -> &'static str {
        match self {
            Self::OiSpikeUp => "oi_spike_up",
            Self::OiSpikeDown => "oi_spike_down",
            Self::LiqCascade => "liq_cascade",
        }
    }

    /// Parses the stored name.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "oi_spike_up" => Self::OiSpikeUp,
            "oi_spike_down" => Self::OiSpikeDown,
            "liq_cascade" => Self::LiqCascade,
            _ => return None,
        })
    }
}

/// A signal as the study sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct SignalRecord {
    /// Time the signal fired, ms.
    pub ts: i64,
    /// Kind.
    pub kind: SignalKind,
    /// Exchange (`*` for market-wide).
    pub exchange: String,
    /// Symbol (empty for cascades).
    pub symbol: String,
    /// Window, seconds.
    pub window_secs: u32,
    /// Change in percent (intensity for cascades).
    pub change_pct: f64,
    /// Threshold crossed.
    pub threshold: f64,
    /// Free-form details.
    pub details: BTreeMap<String, String>,
}

impl SignalRecord {
    /// Builds the record from a protobuf signal.
    pub fn from_proto(s: &vulture_proto::v1::Signal) -> Option<Self> {
        use vulture_proto::v1::SignalType;
        let kind = match SignalType::try_from(s.r#type).ok()? {
            SignalType::OiSpikeUp => SignalKind::OiSpikeUp,
            SignalType::OiSpikeDown => SignalKind::OiSpikeDown,
            SignalType::LiqCascade => SignalKind::LiqCascade,
            SignalType::Unspecified => return None,
        };
        Some(Self {
            ts: s.ts,
            kind,
            exchange: s.exchange.clone(),
            symbol: s.symbol.clone(),
            window_secs: s.window_secs,
            change_pct: s.change_pct,
            threshold: s.threshold,
            details: s
                .details
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }

    /// Strength: how many times the threshold the signal exceeded (1 when unknown).
    pub fn strength(&self) -> f64 {
        if self.threshold.abs() > 0.0 && self.threshold.is_finite() {
            self.change_pct.abs() / self.threshold.abs()
        } else {
            1.0
        }
    }

    fn detail_f64(&self, key: &str) -> Option<f64> {
        self.details.get(key)?.parse().ok()
    }
}

/// Where the study reads prices and liquidations from.
pub trait StudySource: Send + Sync {
    /// Signals with `from <= ts < to`, oldest first.
    fn signals(&self, from: i64, to: i64) -> BoxFuture<'_, Result<Vec<SignalRecord>, String>>;

    /// Mark prices of a series with `from <= ts <= to`, oldest first.
    fn marks(
        &self,
        exchange: &str,
        symbol: &str,
        from: i64,
        to: i64,
    ) -> BoxFuture<'_, Result<Vec<(i64, f64)>, String>>;

    /// Liquidated USD with `from < ts <= to`; `exchange` `*` and an empty `symbol` mean all.
    fn liquidated_usd(
        &self,
        exchange: &str,
        symbol: &str,
        from: i64,
        to: i64,
    ) -> BoxFuture<'_, Result<f64, String>>;
}

/// Settings of the study.
#[derive(Debug, Clone)]
pub struct StudyConfig {
    /// Forward horizons, ms.
    pub horizons_ms: Vec<i64>,
    /// Return thresholds of the hit rates, percent.
    pub thresholds_pct: Vec<f64>,
    /// A price counts when a sample lies this near the wanted time, ms.
    pub tolerance_ms: i64,
    /// Length of the liquidation look-ahead, ms.
    pub liq_horizon_ms: i64,
    /// Split the groups by strength bucket.
    pub by_strength: bool,
    /// Market proxy for cascades: exchange and symbol.
    pub proxy: (String, String),
    /// Signals measured at once.
    pub concurrency: usize,
}

impl Default for StudyConfig {
    fn default() -> Self {
        Self {
            horizons_ms: vec![5 * 60_000, 15 * 60_000, 60 * 60_000],
            thresholds_pct: vec![0.1, 0.25, 0.5, 1.0],
            tolerance_ms: 60_000,
            liq_horizon_ms: 3_600_000,
            by_strength: false,
            proxy: ("binance".into(), "BTC-USDT-PERP".into()),
            concurrency: 8,
        }
    }
}

/// Why a signal has no measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Exclusion {
    /// No mark price near the time of the signal.
    NoPriceAtSignal,
}

/// What was measured for one signal.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    /// The signal.
    pub signal: SignalRecord,
    /// Mark price at the signal.
    pub price0: Option<f64>,
    /// Expected direction: +1 up, -1 down, `None` unknown.
    pub dir: Option<i8>,
    /// Forward return in percent per horizon.
    pub returns: Vec<Option<f64>>,
    /// Liquidated USD in the following hour.
    pub liq_next: Option<f64>,
}

/// The nearest sample to `target` within `tol`.
pub fn nearest(marks: &[(i64, f64)], target: i64, tol: i64) -> Option<f64> {
    let p = marks.partition_point(|m| m.0 < target);
    [p.checked_sub(1), Some(p)]
        .into_iter()
        .flatten()
        .filter_map(|i| marks.get(i))
        .filter(|m| (m.0 - target).abs() <= tol)
        .min_by_key(|m| (m.0 - target).abs())
        .map(|m| m.1)
}

async fn measure_one(
    src: &dyn StudySource,
    cfg: &StudyConfig,
    s: SignalRecord,
) -> Result<Outcome, String> {
    let (ex, sym) = if s.kind == SignalKind::LiqCascade || s.symbol.is_empty() {
        (cfg.proxy.0.as_str(), cfg.proxy.1.as_str())
    } else {
        (s.exchange.as_str(), s.symbol.as_str())
    };
    let max_h = cfg.horizons_ms.iter().copied().max().unwrap_or(0);
    let back = i64::from(s.window_secs) * 1000;
    let marks = src
        .marks(
            ex,
            sym,
            s.ts - back - cfg.tolerance_ms,
            s.ts + max_h + cfg.tolerance_ms,
        )
        .await?;
    let price0 = nearest(&marks, s.ts, cfg.tolerance_ms);
    let dir = match s.kind {
        SignalKind::OiSpikeUp | SignalKind::OiSpikeDown => {
            let before = nearest(&marks, s.ts - back, cfg.tolerance_ms);
            match (price0, before) {
                (Some(p), Some(b)) if p > b => Some(1),
                (Some(p), Some(b)) if p < b => Some(-1),
                _ => None,
            }
        }
        SignalKind::LiqCascade => match (s.detail_f64("long_usd"), s.detail_f64("short_usd")) {
            (Some(l), Some(sh)) if l > sh => Some(-1),
            (Some(l), Some(sh)) if l < sh => Some(1),
            _ => None,
        },
    };
    let returns = cfg
        .horizons_ms
        .iter()
        .map(|h| {
            let p0 = price0?;
            let p = nearest(&marks, s.ts + h, cfg.tolerance_ms)?;
            (p0 > 0.0).then(|| 100.0 * (p - p0) / p0)
        })
        .collect();
    let (lex, lsym) = if s.kind == SignalKind::LiqCascade {
        (s.exchange.as_str(), "")
    } else {
        (s.exchange.as_str(), s.symbol.as_str())
    };
    let liq_next = src
        .liquidated_usd(lex, lsym, s.ts, s.ts + cfg.liq_horizon_ms)
        .await
        .ok();
    Ok(Outcome {
        signal: s,
        price0,
        dir,
        returns,
        liq_next,
    })
}

/// Measures every signal; at most `cfg.concurrency` at a time. Order of the result follows the input.
pub async fn measure(
    src: &dyn StudySource,
    cfg: &StudyConfig,
    signals: Vec<SignalRecord>,
) -> Result<Vec<Outcome>, String> {
    let mut results: Vec<(usize, Outcome)> = Vec::with_capacity(signals.len());
    let mut stream = futures_util::stream::iter(signals.into_iter().enumerate())
        .map(|(i, s)| async move { measure_one(src, cfg, s).await.map(|o| (i, o)) })
        .buffer_unordered(cfg.concurrency.max(1));
    while let Some(r) = stream.next().await {
        results.push(r?);
    }
    results.sort_by_key(|(i, _)| *i);
    Ok(results.into_iter().map(|(_, o)| o).collect())
}

/// Share of the directed signals with a hit at one threshold.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HitRate {
    /// Return threshold, percent.
    pub threshold_pct: f64,
    /// Signals with a direction and a return at the horizon.
    pub directed: usize,
    /// Share followed by a move of at least the threshold in the expected direction.
    pub continuation: Option<f64>,
    /// Share followed by a move of at least the threshold against it.
    pub reversal: Option<f64>,
}

/// Statistics of one horizon.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HorizonStats {
    /// Horizon in minutes.
    pub minutes: i64,
    /// Signals with a return at this horizon.
    pub measured: usize,
    /// Signals of the group without one (price missing at the horizon).
    pub missing: usize,
    /// The returns, percent.
    pub raw_return: Dist,
    /// The returns signed by the expected direction (positive: continuation), percent.
    pub directional_return: Dist,
    /// Hit rates per threshold.
    pub hits: Vec<HitRate>,
}

/// Statistics of one group of signals.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GroupStats {
    /// Signal type.
    pub kind: SignalKind,
    /// Window, seconds.
    pub window_secs: u32,
    /// Exchange.
    pub exchange: String,
    /// Strength bucket when split by strength.
    pub strength: Option<String>,
    /// Signals in the group.
    pub signals: usize,
    /// Excluded: no price at the time of the signal.
    pub excluded_no_price: usize,
    /// Measured but without an expected direction (only raw returns are used).
    pub undirected: usize,
    /// Liquidated USD of the following hour.
    pub liq_next_hour_usd: Dist,
    /// One entry per horizon.
    pub horizons: Vec<HorizonStats>,
}

/// The whole report.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StudyReport {
    /// Period of the signals, ms.
    pub from_ts: i64,
    /// See `from_ts`.
    pub to_ts: i64,
    /// Signals found.
    pub signals: usize,
    /// Signals excluded for lack of a price at their time.
    pub excluded_no_price: usize,
    /// Groups, ordered by kind, window, exchange, strength.
    pub groups: Vec<GroupStats>,
}

/// Strength bucket label of a signal (matches the severity thresholds: 2x and 4x).
pub fn strength_bucket(strength: f64) -> &'static str {
    if strength >= 4.0 {
        "critical (>=4x threshold)"
    } else if strength >= 2.0 {
        "warning (2-4x)"
    } else {
        "info (<2x)"
    }
}

/// Turns outcomes into statistics.
pub fn summarize(outcomes: &[Outcome], cfg: &StudyConfig, from_ts: i64, to_ts: i64) -> StudyReport {
    type Key = (SignalKind, u32, String, Option<String>);
    let mut groups: BTreeMap<Key, Vec<&Outcome>> = BTreeMap::new();
    for o in outcomes {
        let s = &o.signal;
        let strength = cfg
            .by_strength
            .then(|| strength_bucket(s.strength()).to_string());
        groups
            .entry((s.kind, s.window_secs, s.exchange.clone(), strength))
            .or_default()
            .push(o);
    }
    let mut excluded_total = 0;
    let mut out = Vec::new();
    for ((kind, window_secs, exchange, strength), members) in groups {
        let excluded = members.iter().filter(|o| o.price0.is_none()).count();
        excluded_total += excluded;
        let measured: Vec<&&Outcome> = members.iter().filter(|o| o.price0.is_some()).collect();
        let undirected = measured.iter().filter(|o| o.dir.is_none()).count();
        let horizons = cfg
            .horizons_ms
            .iter()
            .enumerate()
            .map(|(hi, h)| {
                let with: Vec<(f64, Option<i8>)> = measured
                    .iter()
                    .filter_map(|o| o.returns.get(hi).copied().flatten().map(|r| (r, o.dir)))
                    .collect();
                let directed: Vec<f64> = with
                    .iter()
                    .filter_map(|(r, d)| d.map(|d| f64::from(d) * r))
                    .collect();
                let hits = cfg
                    .thresholds_pct
                    .iter()
                    .map(|t| {
                        let n = directed.len();
                        let share = |f: &dyn Fn(f64) -> bool| {
                            (n > 0).then(|| {
                                directed.iter().filter(|r| f(**r)).count() as f64 / n as f64
                            })
                        };
                        HitRate {
                            threshold_pct: *t,
                            directed: n,
                            continuation: share(&|r| r >= *t),
                            reversal: share(&|r| r <= -*t),
                        }
                    })
                    .collect();
                HorizonStats {
                    minutes: h / 60_000,
                    measured: with.len(),
                    missing: members.len() - excluded - with.len(),
                    raw_return: Dist::of(with.iter().map(|(r, _)| *r)),
                    directional_return: Dist::of(directed),
                    hits,
                }
            })
            .collect();
        out.push(GroupStats {
            kind,
            window_secs,
            exchange,
            strength,
            signals: members.len(),
            excluded_no_price: excluded,
            undirected,
            liq_next_hour_usd: Dist::of(members.iter().filter_map(|o| o.liq_next)),
            horizons,
        });
    }
    StudyReport {
        from_ts,
        to_ts,
        signals: outcomes.len(),
        excluded_no_price: excluded_total,
        groups: out,
    }
}

/// Reads the signals of a period and produces the report.
pub async fn run(
    src: &dyn StudySource,
    cfg: &StudyConfig,
    from: i64,
    to: i64,
) -> Result<StudyReport, String> {
    let signals = src.signals(from, to).await?;
    let outcomes = measure(src, cfg, signals).await?;
    Ok(summarize(&outcomes, cfg, from, to))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Data held in memory.
    #[derive(Default)]
    pub struct MemSource {
        pub signals: Vec<SignalRecord>,
        pub marks: HashMap<(String, String), Vec<(i64, f64)>>,
        pub liqs: Vec<(i64, String, String, f64)>,
    }

    impl StudySource for MemSource {
        fn signals(&self, from: i64, to: i64) -> BoxFuture<'_, Result<Vec<SignalRecord>, String>> {
            Box::pin(async move {
                let mut v: Vec<_> = self
                    .signals
                    .iter()
                    .filter(|s| s.ts >= from && s.ts < to)
                    .cloned()
                    .collect();
                v.sort_by_key(|s| s.ts);
                Ok(v)
            })
        }
        fn marks(
            &self,
            exchange: &str,
            symbol: &str,
            from: i64,
            to: i64,
        ) -> BoxFuture<'_, Result<Vec<(i64, f64)>, String>> {
            let key = (exchange.to_string(), symbol.to_string());
            Box::pin(async move {
                Ok(self
                    .marks
                    .get(&key)
                    .map(|m| {
                        m.iter()
                            .filter(|x| x.0 >= from && x.0 <= to)
                            .copied()
                            .collect()
                    })
                    .unwrap_or_default())
            })
        }
        fn liquidated_usd(
            &self,
            exchange: &str,
            symbol: &str,
            from: i64,
            to: i64,
        ) -> BoxFuture<'_, Result<f64, String>> {
            let (exchange, symbol) = (exchange.to_string(), symbol.to_string());
            Box::pin(async move {
                Ok(self
                    .liqs
                    .iter()
                    .filter(|l| {
                        l.0 > from
                            && l.0 <= to
                            && (exchange == "*" || l.1 == exchange)
                            && (symbol.is_empty() || l.2 == symbol)
                    })
                    .map(|l| l.3)
                    .sum())
            })
        }
    }

    pub fn oi_signal(ts: i64, exchange: &str, symbol: &str) -> SignalRecord {
        SignalRecord {
            ts,
            kind: SignalKind::OiSpikeUp,
            exchange: exchange.into(),
            symbol: symbol.into(),
            window_secs: 300,
            change_pct: 6.0,
            threshold: 3.0,
            details: BTreeMap::new(),
        }
    }

    const MIN: i64 = 60_000;

    /// A price path: flat 100 until the signal minute, a rise in the 5 minutes before it (so the direction is up),
    /// then `after(minutes_after)` as a multiplier.
    fn path(signal_ts: i64, after: impl Fn(i64) -> f64) -> Vec<(i64, f64)> {
        (-20..=70)
            .map(|m| {
                let ts = signal_ts + m * MIN;
                let p = if m < -5 {
                    100.0
                } else if m <= 0 {
                    100.0 + (m + 5) as f64 * 0.2
                } else {
                    101.0 * after(m)
                };
                (ts, p)
            })
            .collect()
    }

    #[test]
    fn nearest_respects_the_tolerance() {
        let m = [(0, 1.0), (60_000, 2.0), (120_000, 3.0)];
        assert_eq!(nearest(&m, 50_000, 60_000), Some(2.0));
        assert_eq!(nearest(&m, 10_000, 60_000), Some(1.0));
        assert_eq!(nearest(&m, 300_000, 60_000), None);
        assert_eq!(nearest(&[], 0, 1), None);
    }

    #[tokio::test]
    async fn outcomes_with_known_results() {
        let mut src = MemSource::default();
        let key = ("binance".to_string(), "BTC-USDT-PERP".to_string());
        let mut marks: Vec<(i64, f64)> = Vec::new();
        // Ten signals an hour and a half apart: 6 continue up by 1% after 15 minutes, 4 fall back by 1%.
        for i in 0..10i64 {
            let ts = 10_000_000 + i * 90 * MIN;
            src.signals.push(oi_signal(ts, "binance", "BTC-USDT-PERP"));
            let up = i < 6;
            marks.extend(path(ts, |m| {
                if m < 15 {
                    1.0
                } else if up {
                    1.01
                } else {
                    0.99
                }
            }));
            src.liqs.push((
                ts + 10 * MIN,
                "binance".into(),
                "BTC-USDT-PERP".into(),
                1_000.0 * (i + 1) as f64,
            ));
        }
        marks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        marks.dedup_by_key(|m| m.0);
        src.marks.insert(key, marks);
        // An eleventh signal on a symbol without prices, and a twelfth far from any sample.
        src.signals.push(oi_signal(
            10_000_000 + 20 * 90 * MIN,
            "bybit",
            "ETH-USDT-PERP",
        ));
        let cfg = StudyConfig::default();
        let report = run(&src, &cfg, 0, i64::MAX).await.unwrap();
        assert_eq!(report.signals, 11);
        assert_eq!(report.excluded_no_price, 1);
        let g = report
            .groups
            .iter()
            .find(|g| g.exchange == "binance")
            .unwrap();
        assert_eq!((g.signals, g.excluded_no_price, g.undirected), (10, 0, 0));
        let h15 = g.horizons.iter().find(|h| h.minutes == 15).unwrap();
        assert_eq!((h15.measured, h15.missing), (10, 0));
        // The return at +15 min is +-1% from 101 against the signal price of 101 (0 after +15 only at exactly 15).
        let cont = h15.hits.iter().find(|h| h.threshold_pct == 0.5).unwrap();
        assert_eq!(cont.directed, 10);
        assert_eq!(cont.continuation, Some(0.6));
        assert_eq!(cont.reversal, Some(0.4));
        let quarter = h15.hits.iter().find(|h| h.threshold_pct == 0.25).unwrap();
        assert_eq!(
            (quarter.continuation, quarter.reversal),
            (Some(0.6), Some(0.4))
        );
        let big = h15.hits.iter().find(|h| h.threshold_pct == 2.0);
        assert!(big.is_none(), "only the configured thresholds are reported");
        // Quartiles of the directional return: six values near +1, four near -1.
        let d = &h15.directional_return;
        assert_eq!(d.n, 10);
        assert!(d.median.unwrap() > 0.9 && d.q1.unwrap() < -0.9, "{d:?}");
        // Liquidations of the following hour per signal: 1000 * (i + 1); the median of 1..10 thousand is 5500.
        assert_eq!(g.liq_next_hour_usd.median, Some(5_500.0));
        // +5 minutes: nothing moved yet.
        let h5 = g.horizons.iter().find(|h| h.minutes == 5).unwrap();
        assert!(h5.raw_return.median.unwrap().abs() < 1e-9);
    }

    #[tokio::test]
    async fn missing_horizons_and_directions_are_counted() {
        let mut src = MemSource::default();
        let key = ("binance".to_string(), "BTC-USDT-PERP".to_string());
        let ts = 10_000_000;
        // Prices only until +10 minutes: the +15 and +60 horizons are missing; flat before: no direction.
        src.marks
            .insert(key, (-20..=10).map(|m| (ts + m * MIN, 100.0)).collect());
        src.signals.push(oi_signal(ts, "binance", "BTC-USDT-PERP"));
        let report = run(&src, &StudyConfig::default(), 0, i64::MAX)
            .await
            .unwrap();
        let g = &report.groups[0];
        assert_eq!((g.signals, g.excluded_no_price, g.undirected), (1, 0, 1));
        let by = |m: i64| g.horizons.iter().find(|h| h.minutes == m).unwrap();
        assert_eq!((by(5).measured, by(5).missing), (1, 0));
        assert_eq!((by(15).measured, by(15).missing), (0, 1));
        assert_eq!((by(60).measured, by(60).missing), (0, 1));
        // No direction: raw returns exist, hit rates do not.
        assert_eq!(by(5).raw_return.n, 1);
        assert_eq!(by(5).hits[0].directed, 0);
        assert_eq!(by(5).hits[0].continuation, None);
    }

    #[tokio::test]
    async fn cascades_use_the_proxy_and_the_liquidated_side() {
        let mut src = MemSource::default();
        let ts = 10_000_000;
        src.marks.insert(
            ("binance".into(), "BTC-USDT-PERP".into()),
            (-20..=70)
                .map(|m| (ts + m * MIN, if m <= 0 { 100.0 } else { 98.0 }))
                .collect(),
        );
        let mut c = oi_signal(ts, "*", "");
        c.kind = SignalKind::LiqCascade;
        c.window_secs = 60;
        c.details.insert("long_usd".into(), "900000".into());
        c.details.insert("short_usd".into(), "100000".into());
        src.signals.push(c);
        src.liqs
            .push((ts + MIN, "bybit".into(), "ETH-USDT-PERP".into(), 5_000.0));
        src.liqs.push((
            ts + 2 * MIN,
            "binance".into(),
            "BTC-USDT-PERP".into(),
            7_000.0,
        ));
        let report = run(&src, &StudyConfig::default(), 0, i64::MAX)
            .await
            .unwrap();
        let g = &report.groups[0];
        assert_eq!(g.kind, SignalKind::LiqCascade);
        // Longs were liquidated: expected direction down. The proxy then fell 2%: a continuation of 2%.
        let h = g.horizons.iter().find(|h| h.minutes == 15).unwrap();
        assert!((h.directional_return.median.unwrap() - 2.0).abs() < 1e-9);
        assert_eq!(
            h.hits
                .iter()
                .find(|x| x.threshold_pct == 1.0)
                .unwrap()
                .continuation,
            Some(1.0)
        );
        assert_eq!(
            g.liq_next_hour_usd.median,
            Some(12_000.0),
            "all symbols, all exchanges"
        );
    }

    #[tokio::test]
    async fn strength_buckets_split_the_groups() {
        let mut src = MemSource::default();
        let key = ("binance".to_string(), "BTC-USDT-PERP".to_string());
        src.marks.insert(
            key,
            (0..=400)
                .map(|m| (m * MIN, 100.0 + m as f64 * 0.01))
                .collect(),
        );
        for (i, change) in [3.5, 7.0, 13.0].into_iter().enumerate() {
            let mut s = oi_signal(100 * MIN + i as i64 * 100 * MIN, "binance", "BTC-USDT-PERP");
            s.change_pct = change;
            src.signals.push(s);
        }
        let mut cfg = StudyConfig::default();
        let one = run(&src, &cfg, 0, i64::MAX).await.unwrap();
        assert_eq!(one.groups.len(), 1);
        cfg.by_strength = true;
        let split = run(&src, &cfg, 0, i64::MAX).await.unwrap();
        let labels: Vec<_> = split
            .groups
            .iter()
            .map(|g| g.strength.clone().unwrap())
            .collect();
        assert_eq!(labels.len(), 3, "{labels:?}");
        assert_eq!(strength_bucket(1.0), "info (<2x)");
        assert_eq!(strength_bucket(2.0), "warning (2-4x)");
        assert_eq!(strength_bucket(4.0), "critical (>=4x threshold)");
    }
}
