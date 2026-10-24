//! The OI spike detector (V4-1a).
//!
//! For every new OI sample of a symbol and every enabled window `w` (1m, 5m, 15m, 1h, 4h by default):
//!
//! 1. `change_pct = 100 x (oi_base_now - oi_base_ref) / oi_base_ref`, where the reference is the sample nearest
//!    to `now - w` (tolerance `w/8` between 15 s and 300 s). `oi_base` is used, not `oi_usd`, so a price move
//!    alone never looks like a spike. A window that spans a `Gap`, a hole or lacks a reference is skipped.
//! 2. The move must pass: `|change_pct| >= threshold_pct(w)`, `|change_base x mark| >= min_change_usd(w)`
//!    and `oi_usd_now >= min_oi_usd` (dust is ignored).
//! 3. It must be unusual for the symbol: `|change_pct - median| >= z_min x sigma`, where median and
//!    `sigma = 1.4826 x MAD` come from the symbol's own earlier changes over the same window (the retained
//!    history before the current window); for the longest window, which has no earlier history, from the
//!    symbol's step-to-step volatility scaled by `sqrt(steps in window)`. `sigma` has a floor of
//!    `threshold_pct / 8`. A symbol that always moves 3% in 5 minutes therefore does not alarm at 3%.
//! 4. Cooldown per (exchange, symbol, window, direction): after a signal the same key stays quiet for
//!    `cooldown_secs`, unless the move extends materially (`|change_pct| >= extension_factor x` the last
//!    signalled one).
//! 5. Overlapping windows of one direction become one signal: the strongest (largest `|change_pct| /
//!    threshold`) is reported, the others are listed in `details["windows"]`.
//!
//! Severity: `|change_pct| / threshold` below 2 is `info`, below 4 `warning`, otherwise `critical`.

use std::collections::HashMap;
use std::sync::Arc;

use vulture_proto::v1::{Signal, SignalType};

use super::model::Severity;
use super::series::{Baseline, Change, Series};
use super::stats::EngineStats;
use super::text::{fmt_pct, fmt_window};
use crate::config::OiSpikeConfig;

/// A threshold override from a `detector_override` rule.
#[derive(Debug, Clone)]
pub struct OiOverride {
    /// Window, seconds.
    pub window_secs: u32,
    /// Threshold, percent.
    pub threshold_pct: f64,
    /// Floor, USD.
    pub min_change_usd: f64,
    /// Exchanges (empty = all).
    pub exchanges: Vec<String>,
    /// Symbols (empty = all).
    pub symbols: Vec<String>,
}

impl OiOverride {
    fn specificity(&self, exchange: &str, symbol: &str) -> Option<u8> {
        let ex = self.exchanges.iter().any(|e| e == exchange);
        let sy = self.symbols.iter().any(|s| s == symbol);
        if (!self.exchanges.is_empty() && !ex) || (!self.symbols.is_empty() && !sy) {
            return None;
        }
        Some(u8::from(!self.symbols.is_empty()) * 2 + u8::from(!self.exchanges.is_empty()))
    }
}

#[derive(Debug, Clone, Copy)]
struct Cooldown {
    ts: i64,
    abs_pct: f64,
}

const SLOTS: usize = 16;

/// One window that passed every test.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Index of the window in the configuration.
    pub widx: usize,
    /// Window, seconds.
    pub window_secs: u32,
    /// The change.
    pub change: Change,
    /// Threshold in force, percent.
    pub threshold_pct: f64,
    /// Robust z-score, `NaN` when no baseline was required.
    pub z: f64,
    /// The baseline used.
    pub baseline: Option<Baseline>,
}

impl Candidate {
    fn strength(&self) -> f64 {
        self.change.pct.abs() / self.threshold_pct
    }
}

/// The detector. Create with [`OiDetector::new`], feed it with [`OiDetector::evaluate`].
pub struct OiDetector {
    cfg: OiSpikeConfig,
    stats: Arc<EngineStats>,
    cooldowns: HashMap<String, HashMap<String, [Option<Cooldown>; SLOTS]>>,
}

fn slot(widx: usize, up: bool) -> usize {
    widx * 2 + usize::from(!up)
}

/// Deterministic identity of a signal: the same input yields the same key (also after a restart).
pub fn signal_key(kind: &str, exchange: &str, symbol: &str, window_secs: u32, ts: i64) -> String {
    format!("{kind}:{exchange}:{symbol}:{window_secs}:{ts}")
}

impl OiDetector {
    /// Detector with `cfg`.
    pub fn new(cfg: OiSpikeConfig, stats: Arc<EngineStats>) -> Self {
        Self {
            cfg,
            stats,
            cooldowns: HashMap::new(),
        }
    }

    /// The configuration.
    pub fn config(&self) -> &OiSpikeConfig {
        &self.cfg
    }

    /// Longest window, ms (what the ring buffers must cover).
    pub fn max_window_ms(&self) -> i64 {
        self.cfg
            .windows
            .iter()
            .map(|w| i64::from(w.secs) * 1000)
            .max()
            .unwrap_or(0)
    }

    /// Restores a cooldown from a stored signal (warm-up), so a restart does not repeat a signal.
    pub fn restore(&mut self, signal: &Signal) {
        let up = match SignalType::try_from(signal.r#type) {
            Ok(SignalType::OiSpikeUp) => true,
            Ok(SignalType::OiSpikeDown) => false,
            _ => return,
        };
        // Every window that triggered together has its cooldown restored, not only the reported one.
        let mut secs: Vec<u32> = signal
            .details
            .get("window_secs_all")
            .map(|v| v.split(',').filter_map(|s| s.parse().ok()).collect())
            .unwrap_or_default();
        if !secs.contains(&signal.window_secs) {
            secs.push(signal.window_secs);
        }
        for window_secs in secs {
            let Some(widx) = self.cfg.windows.iter().position(|w| w.secs == window_secs) else {
                continue;
            };
            let cd = self
                .cooldowns
                .entry(signal.exchange.clone())
                .or_default()
                .entry(signal.symbol.clone())
                .or_insert([None; SLOTS]);
            let s = slot(widx, up);
            if cd[s].is_none_or(|c| c.ts < signal.ts) {
                cd[s] = Some(Cooldown {
                    ts: signal.ts,
                    abs_pct: signal.change_pct.abs(),
                });
            }
        }
    }

    fn thresholds(
        exchange: &str,
        symbol: &str,
        window_secs: u32,
        default_pct: f64,
        default_usd: f64,
        overrides: &[OiOverride],
    ) -> (f64, f64) {
        let best = overrides
            .iter()
            .filter(|o| o.window_secs == window_secs)
            .filter_map(|o| o.specificity(exchange, symbol).map(|s| (s, o)))
            .max_by_key(|(s, _)| *s);
        match best {
            Some((_, o)) => (o.threshold_pct, o.min_change_usd),
            None => (default_pct, default_usd),
        }
    }

    /// Evaluates the symbol after a new sample was pushed to `series`. `extra_break` is the exchange-level
    /// break (a `Gap` ending at that time). Returns zero, one or two signals (one per direction).
    pub fn evaluate(
        &mut self,
        exchange: &str,
        symbol: &str,
        series: &Series,
        extra_break: i64,
        overrides: &[OiOverride],
    ) -> Vec<Signal> {
        if !self.cfg.enabled {
            return Vec::new();
        }
        let Some(now) = series.last().copied() else {
            return Vec::new();
        };
        let oi_usd = if now.oi_usd.is_finite() {
            now.oi_usd
        } else if now.mark.is_finite() {
            now.oi_base * now.mark
        } else {
            f64::NAN
        };
        if !super::series::ge(oi_usd, self.cfg.min_oi_usd) {
            EngineStats::bump(&self.stats.oi_dust);
            return Vec::new();
        }
        let mut triggered: Vec<(bool, Candidate)> = Vec::new();
        for widx in 0..self.cfg.windows.len() {
            let w = &self.cfg.windows[widx];
            if !w.enabled {
                continue;
            }
            let (thr, floor) = Self::thresholds(
                exchange,
                symbol,
                w.secs,
                w.threshold_pct,
                w.min_change_usd,
                overrides,
            );
            let window_secs = w.secs;
            let window_ms = i64::from(window_secs) * 1000;
            let Some(change) = series.change(window_ms, extra_break) else {
                continue;
            };
            if change.pct.abs() < thr || !super::series::ge(change.usd.abs(), floor) {
                continue;
            }
            let up = change.pct > 0.0;
            let abs_pct = change.pct.abs();
            if let Some(cd) = self
                .cooldowns
                .get(exchange)
                .and_then(|m| m.get(symbol))
                .and_then(|c| c[slot(widx, up)])
                && now.ts - cd.ts < (self.cfg.cooldown_secs as i64) * 1000
                && abs_pct < cd.abs_pct * self.cfg.extension_factor
            {
                EngineStats::bump(&self.stats.oi_suppressed_cooldown);
                continue;
            }
            // The cheap tests passed: judge whether the move is unusual for this symbol.
            let mut baseline = series
                .baseline(window_ms, extra_break, self.cfg.min_baseline_points, 200)
                .or_else(|| {
                    series.scaled_baseline(window_ms, extra_break, self.cfg.min_baseline_points)
                });
            let z = match &mut baseline {
                Some(b) => {
                    b.sigma = b.sigma.max(thr / 8.0);
                    (change.pct - b.median) / b.sigma
                }
                None => {
                    if self.cfg.require_baseline {
                        EngineStats::bump(&self.stats.oi_suppressed_no_baseline);
                        continue;
                    }
                    f64::NAN
                }
            };
            if baseline.is_some() && z.abs() < self.cfg.z_min {
                EngineStats::bump(&self.stats.oi_suppressed_usual);
                continue;
            }
            triggered.push((
                up,
                Candidate {
                    widx,
                    window_secs,
                    change,
                    threshold_pct: thr,
                    z,
                    baseline,
                },
            ));
        }
        let mut out = Vec::new();
        for up in [true, false] {
            let group: Vec<&Candidate> = triggered
                .iter()
                .filter(|(u, _)| *u == up)
                .map(|(_, c)| c)
                .collect();
            let Some(best) = group
                .iter()
                .copied()
                .max_by(|a, b| a.strength().total_cmp(&b.strength()))
            else {
                continue;
            };
            let per_symbol = self
                .cooldowns
                .entry(exchange.to_string())
                .or_default()
                .entry(symbol.to_string())
                .or_insert([None; SLOTS]);
            for c in &group {
                per_symbol[slot(c.widx, up)] = Some(Cooldown {
                    ts: now.ts,
                    abs_pct: c.change.pct.abs(),
                });
            }
            out.push(build_signal(exchange, symbol, up, best, &group, oi_usd));
        }
        for s in &out {
            let i = if s.r#type == SignalType::OiSpikeUp as i32 {
                0
            } else {
                1
            };
            EngineStats::bump(&self.stats.signals[i]);
        }
        out
    }
}

/// Severity from the ratio of the change to the threshold.
pub fn severity_of_ratio(ratio: f64) -> Severity {
    if ratio >= 4.0 {
        Severity::Critical
    } else if ratio >= 2.0 {
        Severity::Warning
    } else {
        Severity::Info
    }
}

fn build_signal(
    exchange: &str,
    symbol: &str,
    up: bool,
    best: &Candidate,
    group: &[&Candidate],
    oi_usd: f64,
) -> Signal {
    let kind = if up { "oi_spike_up" } else { "oi_spike_down" };
    let c = &best.change;
    let mut details = std::collections::BTreeMap::new();
    details.insert(
        "key".to_string(),
        signal_key(kind, exchange, symbol, best.window_secs, c.now.ts),
    );
    details.insert(
        "severity".into(),
        severity_of_ratio(best.strength()).as_str().into(),
    );
    let mut windows = group.to_vec();
    windows.sort_by_key(|c| c.window_secs);
    details.insert(
        "windows".into(),
        windows
            .iter()
            .map(|c| format!("{} {}", fmt_window(c.window_secs), fmt_pct(c.change.pct)))
            .collect::<Vec<_>>()
            .join(", "),
    );
    details.insert("oi_base".into(), format!("{}", c.now.oi_base));
    details.insert("oi_base_ref".into(), format!("{}", c.reference.oi_base));
    details.insert("oi_usd".into(), format!("{oi_usd:.0}"));
    details.insert("change_usd".into(), format!("{:.0}", c.usd));
    if c.now.mark.is_finite() {
        details.insert("mark_price".into(), format!("{}", c.now.mark));
    }
    details.insert("ref_ts".into(), c.reference.ts.to_string());
    // Machine-readable list of every window that triggered, so a restart can restore all their cooldowns.
    details.insert(
        "window_secs_all".into(),
        windows
            .iter()
            .map(|c| c.window_secs.to_string())
            .collect::<Vec<_>>()
            .join(","),
    );
    if best.z.is_finite() {
        details.insert("z".into(), format!("{:.1}", best.z));
    }
    if let Some(b) = &best.baseline {
        details.insert(
            "baseline".into(),
            format!(
                "{} n={} median={:.3}% sigma={:.3}%",
                if b.scaled { "scaled" } else { "empirical" },
                b.points,
                b.median,
                b.sigma
            ),
        );
    }
    Signal {
        r#type: if up {
            SignalType::OiSpikeUp
        } else {
            SignalType::OiSpikeDown
        } as i32,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        window_secs: best.window_secs,
        change_pct: c.pct,
        change_abs: c.abs_base,
        threshold: best.threshold_pct,
        ts: c.now.ts,
        details: details.into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::alerts::series::{Sample, SeriesLimits};

    fn limits() -> SeriesLimits {
        SeriesLimits {
            retention_ms: 4 * 3600 * 1000 + 600_000,
            max_samples: 1024,
            spacing_ms: 15_000,
            hole_ms: 300_000,
        }
    }

    struct Harness {
        det: OiDetector,
        series: Series,
        stats: Arc<EngineStats>,
        next: i64,
    }

    impl Harness {
        fn new() -> Self {
            Self::with(OiSpikeConfig::default())
        }
        fn with(cfg: OiSpikeConfig) -> Self {
            let stats = Arc::new(EngineStats::default());
            Self {
                det: OiDetector::new(cfg, stats.clone()),
                series: Series::new(),
                stats,
                next: 0,
            }
        }
        /// Pushes one sample 30 s after the previous one and evaluates.
        fn step(&mut self, oi: f64, mark: f64) -> Vec<Signal> {
            let ts = self.next;
            self.next += 30_000;
            self.series.push(
                Sample {
                    ts,
                    oi_base: oi,
                    oi_usd: oi * mark,
                    mark,
                },
                &limits(),
            );
            self.det
                .evaluate("binance", "BTC-USDT-PERP", &self.series, 0, &[])
        }
        fn run(&mut self, n: usize, f: impl Fn(usize) -> (f64, f64)) -> Vec<Signal> {
            let mut all = Vec::new();
            for i in 0..n {
                let (oi, mark) = f(i);
                all.extend(self.step(oi, mark));
            }
            all
        }
    }

    const BASE: f64 = 10_000.0;

    fn flat(_: usize) -> (f64, f64) {
        (BASE, 1000.0)
    }

    #[test]
    fn growth_fires_up_with_the_strongest_window() {
        let mut h = Harness::new();
        assert!(h.run(300, flat).is_empty());
        let sigs = h.run(2, |_| (BASE * 1.08, 1000.0));
        assert_eq!(sigs.len(), 1, "{sigs:?}");
        let s = &sigs[0];
        assert_eq!(s.r#type, SignalType::OiSpikeUp as i32);
        assert!(s.change_pct > 0.0 && s.change_abs > 0.0);
        assert!(s.details.contains_key("key") && s.details.contains_key("windows"));
        assert_eq!(s.exchange, "binance");
        assert_eq!(s.symbol, "BTC-USDT-PERP");
    }

    #[test]
    fn drop_fires_down() {
        let mut h = Harness::new();
        h.run(300, flat);
        let sigs = h.run(2, |_| (BASE * 0.9, 1000.0));
        assert_eq!(sigs.len(), 1, "{sigs:?}");
        assert_eq!(sigs[0].r#type, SignalType::OiSpikeDown as i32);
        assert!(sigs[0].change_pct < 0.0 && sigs[0].change_abs < 0.0);
    }

    #[test]
    fn price_move_without_oi_change_is_silent() {
        let mut h = Harness::new();
        h.run(300, flat);
        // The mark price rises 5x: oi_usd rises 5x, oi_base does not move.
        let sigs = h.run(40, |i| (BASE, 100.0 + (i as f64) * 10.0));
        assert!(sigs.is_empty(), "{sigs:?}");
    }

    #[test]
    fn dust_symbol_is_ignored() {
        let mut h = Harness::new();
        // 100 base x $1 = $100 of OI.
        h.run(300, |_| (100.0, 1.0));
        let sigs = h.run(3, |_| (150.0, 1.0));
        assert!(sigs.is_empty());
        assert!(h.stats.oi_dust.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn small_absolute_change_is_below_the_floor() {
        // A 6% move of a $1.2M position is $72k: under every floor of the defaults.
        let mut h = Harness::new();
        let (oi, mark) = (1_200.0, 1000.0);
        h.run(300, |_| (oi, mark));
        let sigs = h.run(3, |_| (oi * 1.06, mark));
        assert!(sigs.is_empty(), "{sigs:?}");
    }

    #[test]
    fn noisy_symbol_does_not_alarm_but_quiet_one_does() {
        // A sawtooth with a 6% swing every 5 samples: its own history makes such moves ordinary.
        let saw = |i: usize| {
            (
                BASE * if (i / 5).is_multiple_of(2) { 1.0 } else { 1.06 },
                1000.0,
            )
        };
        let mut noisy = Harness::new();
        // The first samples are a cold start: without history there is nothing to call usual.
        noisy.run(100, saw);
        let sigs = noisy.run(600, |i| saw(i + 100));
        assert!(sigs.is_empty(), "noisy symbol alarmed: {sigs:?}");
        let mut quiet = Harness::new();
        quiet.run(700, |i| (BASE * (1.0 + 0.0002 * (i % 3) as f64), 1000.0));
        let sigs = quiet.run(3, |_| (BASE * 1.06, 1000.0));
        assert!(!sigs.is_empty());
    }

    #[test]
    fn cooldown_suppresses_repeats_until_the_move_extends() {
        let mut h = Harness::new();
        h.run(300, flat);
        let first = h.run(2, |_| (BASE * 1.08, 1000.0));
        assert_eq!(first.len(), 1);
        // The level holds: the same windows must not fire again inside the cooldown.
        let again = h.run(10, |_| (BASE * 1.08, 1000.0));
        assert!(again.is_empty(), "{again:?}");
        assert!(h.stats.oi_suppressed_cooldown.load(Ordering::Relaxed) > 0);
        // A move that extends materially (8% -> 20%) passes.
        let ext = h.run(2, |_| (BASE * 1.20, 1000.0));
        assert!(!ext.is_empty());
    }

    #[test]
    fn cooldown_expires() {
        let mut h = Harness::with(OiSpikeConfig {
            cooldown_secs: 60,
            ..OiSpikeConfig::default()
        });
        h.run(300, flat);
        assert_eq!(h.run(2, |_| (BASE * 1.08, 1000.0)).len(), 1);
        // Back to flat for a while, then the same kind of move again after the cooldown.
        h.run(40, flat);
        let back = h.run(2, |_| (BASE * 1.08, 1000.0));
        assert!(!back.is_empty());
    }

    #[test]
    fn overlapping_windows_become_one_signal_listing_the_others() {
        let mut h = Harness::new();
        h.run(300, flat);
        let sigs = h.run(2, |_| (BASE * 1.12, 1000.0));
        assert_eq!(sigs.len(), 1);
        let windows = sigs[0].details.get("windows").unwrap();
        assert!(windows.contains(','), "{windows}");
    }

    #[test]
    fn gap_inside_the_window_skips_it() {
        let mut h = Harness::new();
        h.run(300, flat);
        let ts = h.next;
        h.series.push(
            Sample {
                ts,
                oi_base: BASE * 1.2,
                oi_usd: BASE * 1.2 * 1000.0,
                mark: 1000.0,
            },
            &limits(),
        );
        // A gap that ends now: every window reaches back across it.
        let sigs = h
            .det
            .evaluate("binance", "BTC-USDT-PERP", &h.series, ts, &[]);
        assert!(sigs.is_empty(), "{sigs:?}");
    }

    #[test]
    fn small_teeth_on_a_quiet_symbol_stay_silent() {
        let mut h = Harness::new();
        let sigs = h.run(600, |i| (BASE * (1.0 + 0.004 * ((i % 4) as f64)), 1000.0));
        assert!(sigs.is_empty(), "{sigs:?}");
    }

    #[test]
    fn overrides_replace_thresholds() {
        let quiet = |i: usize| (BASE * (1.0 + 0.0002 * (i % 3) as f64), 1000.0);
        let mut h = Harness::new();
        h.run(300, quiet);
        // +2%: under the default 5m threshold of 3%.
        for _ in 0..2 {
            h.step(BASE * 1.02, 1000.0);
        }
        let none = h
            .det
            .evaluate("binance", "BTC-USDT-PERP", &h.series, 0, &[]);
        assert!(none.is_empty());
        let ov = [OiOverride {
            window_secs: 300,
            threshold_pct: 1.0,
            min_change_usd: 0.0,
            exchanges: vec![],
            symbols: vec!["BTC-USDT-PERP".into()],
        }];
        let some = h
            .det
            .evaluate("binance", "BTC-USDT-PERP", &h.series, 0, &ov);
        assert_eq!(some.len(), 1);
        assert_eq!(some[0].window_secs, 300);
        assert!((some[0].threshold - 1.0).abs() < 1e-9);
        // An override for another symbol does not apply.
        let other = [OiOverride {
            symbols: vec!["ETH-USDT-PERP".into()],
            ..ov[0].clone()
        }];
        let mut h2 = Harness::new();
        h2.run(300, quiet);
        for _ in 0..2 {
            h2.step(BASE * 1.02, 1000.0);
        }
        assert!(
            h2.det
                .evaluate("binance", "BTC-USDT-PERP", &h2.series, 0, &other)
                .is_empty()
        );
    }

    #[test]
    fn restore_prevents_a_repeat_after_restart() {
        let mut a = Harness::new();
        a.run(300, flat);
        let first = a.run(2, |_| (BASE * 1.08, 1000.0));
        assert_eq!(first.len(), 1);
        // A fresh detector over the same history (as after a warm-up) knows the stored signal.
        let mut b = Harness::new();
        b.run(300, flat);
        for s in &first {
            b.det.restore(s);
        }
        let again = b.run(2, |_| (BASE * 1.08, 1000.0));
        assert!(again.is_empty(), "{again:?}");
    }

    #[test]
    fn one_minute_window_fires_on_a_fast_move() {
        let cfg = OiSpikeConfig {
            windows: vec![crate::config::OiWindow {
                secs: 60,
                threshold_pct: 1.5,
                min_change_usd: 100_000.0,
                enabled: true,
            }],
            ..OiSpikeConfig::default()
        };
        let mut h = Harness::with(cfg);
        h.run(300, flat);
        let sigs = h.run(2, |_| (BASE * 1.05, 1000.0));
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].window_secs, 60);
    }
}
