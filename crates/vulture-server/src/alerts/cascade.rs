//! The liquidation cascade detector (V4-1).
//!
//! Scopes: market-wide (`*`) and, optionally, each exchange. Per scope:
//!
//! - **Baseline.** Liquidated notional per clock-aligned minute over the previous `baseline_secs` (24 h):
//!   completed minutes only, minutes in which the process saw any event but no liquidation count as zero.
//!   `median` and `MAD` of those values; `sigma = max(1.4826 x MAD, 0.1 x median)`. The baseline is cached and
//!   recomputed when the minute changes. It needs `min_baseline_minutes` minutes; before that the trigger is the
//!   conservative `3 x floor`.
//! - **Value.** The notional of the last 60 s (a sliding window, evaluated at every liquidation).
//! - **Start.** `value >= max(floor, median + k_mad x sigma)` and at least `min_count` liquidations in the
//!   window. A `LIQ_CASCADE` signal with `phase=start` is emitted with the running totals.
//! - **End.** The cascade is *hot* while the notional of the last `min(quiet_secs, 60)` seconds is at least
//!   `end_ratio x trigger x min(quiet_secs, 60) / 60`; it ends after `quiet_secs` without being hot. A closing `LIQ_CASCADE` signal with
//!   `phase=end` carries duration, total USD, long/short split, top symbols and the price range of the top
//!   symbol.
//!
//! Signal fields: `change_pct` is the intensity relative to the baseline (`100 x value / max(median, 1000)`),
//! `change_abs` the notional in USD (the window at the start, the total at the end), `threshold` the trigger in
//! the same percent unit. Severity is `critical` at 3x the trigger or more, otherwise `warning`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use vulture_proto::v1::{Liquidation, Side, Signal, SignalType};

use super::model::Severity;
use super::oi::signal_key;
use super::series::median_mad;
use super::stats::EngineStats;
use super::text::fmt_usd;
use crate::config::CascadeConfig;

const WINDOW_MS: i64 = 60_000;
const MAX_WINDOW_RECORDS: usize = 500_000;

#[derive(Debug, Clone, Copy)]
struct Bucket {
    minute: i64,
    usd: f64,
}

#[derive(Debug)]
struct Rec {
    ts: i64,
    usd: f64,
    long: bool,
    symbol: Arc<str>,
    price: f64,
}

#[derive(Debug, Clone, Copy)]
struct BaselineCache {
    for_minute: i64,
    ready: bool,
    median: f64,
    sigma: f64,
}

#[derive(Debug, Default)]
struct SymbolAgg {
    usd: f64,
    low: f64,
    high: f64,
}

#[derive(Debug)]
struct Active {
    start_ts: i64,
    last_hot_ts: i64,
    trigger_usd: f64,
    median: f64,
    total_usd: f64,
    count: u64,
    long_usd: f64,
    short_usd: f64,
    peak_usd: f64,
    symbols: HashMap<Arc<str>, SymbolAgg>,
}

impl Scope {
    /// Notional of the last `horizon_ms` (the whole window when it is the full minute).
    fn recent_sum(&self, now: i64, horizon_ms: i64) -> f64 {
        if horizon_ms >= WINDOW_MS {
            return self.window_sum;
        }
        self.window
            .iter()
            .rev()
            .take_while(|r| r.ts > now - horizon_ms)
            .map(|r| r.usd)
            .sum()
    }
}

#[derive(Debug)]
struct Scope {
    exchange: String,
    floor_usd: f64,
    buckets: VecDeque<Bucket>,
    window: VecDeque<Rec>,
    window_sum: f64,
    baseline: Option<BaselineCache>,
    active: Option<Active>,
}

/// The detector. Feed liquidations with [`CascadeDetector::on_liquidation`], advance time with
/// [`CascadeDetector::advance`].
pub struct CascadeDetector {
    cfg: CascadeConfig,
    stats: Arc<EngineStats>,
    scopes: HashMap<String, Scope>,
    /// Last minute marked as observed per exchange (the market scope has its own).
    observed: Vec<(String, i64)>,
    market_observed: i64,
}

/// Scope name of the market-wide detector.
pub const MARKET: &str = "*";

impl CascadeDetector {
    /// Detector with `cfg`.
    pub fn new(cfg: CascadeConfig, stats: Arc<EngineStats>) -> Self {
        let mut d = Self {
            cfg,
            stats,
            scopes: HashMap::new(),
            observed: Vec::new(),
            market_observed: i64::MIN,
        };
        d.ensure_scope(MARKET);
        d
    }

    fn ensure_scope(&mut self, exchange: &str) -> &mut Scope {
        let floor = if exchange == MARKET {
            self.cfg.floor_usd
        } else {
            self.cfg.floor_usd_exchange
        };
        self.scopes
            .entry(exchange.to_string())
            .or_insert_with(|| Scope {
                exchange: exchange.to_string(),
                floor_usd: floor,
                buckets: VecDeque::new(),
                window: VecDeque::new(),
                window_sum: 0.0,
                baseline: None,
                active: None,
            })
    }

    fn keep_minutes(&self) -> i64 {
        (self.cfg.baseline_secs / 60) as i64 + 2
    }

    fn bucket(&mut self, scope: &str, minute: i64) -> Option<&mut Bucket> {
        let keep = self.keep_minutes();
        let s = self.scopes.get_mut(scope)?;
        match s.buckets.back() {
            Some(b) if b.minute == minute => {}
            Some(b) if b.minute > minute => {
                // Late event: find its bucket among the newest few, otherwise ignore it.
                let idx = s.buckets.iter().rposition(|b| b.minute == minute)?;
                return s.buckets.get_mut(idx);
            }
            _ => {
                s.buckets.push_back(Bucket { minute, usd: 0.0 });
                while s.buckets.front().is_some_and(|b| b.minute < minute - keep) {
                    s.buckets.pop_front();
                }
            }
        }
        s.buckets.back_mut()
    }

    /// Marks the minute of `ts` as observed for the exchange and the market, so that a quiet minute counts as
    /// zero in the baseline (and a minute the process did not see does not).
    pub fn observe(&mut self, exchange: &str, ts: i64) {
        let minute = ts.div_euclid(60_000);
        if self.market_observed < minute {
            self.market_observed = minute;
            self.bucket(MARKET, minute);
        }
        if !self.cfg.per_exchange {
            return;
        }
        match self.observed.iter_mut().find(|(e, _)| e == exchange) {
            Some((_, m)) if *m >= minute => {}
            Some((_, m)) => {
                *m = minute;
                self.ensure_scope(exchange);
                self.bucket(exchange, minute);
            }
            None => {
                self.observed.push((exchange.to_string(), minute));
                self.ensure_scope(exchange);
                self.bucket(exchange, minute);
            }
        }
    }

    /// Adds a liquidation of the replayed history to the baseline only (warm-up).
    pub fn warm(&mut self, liq: &Liquidation) {
        let minute = liq.recv_ts.div_euclid(60_000);
        self.observe(&liq.exchange, liq.recv_ts);
        if let Some(b) = self.bucket(MARKET, minute) {
            b.usd += liq.notional_usd;
        }
        if self.cfg.per_exchange
            && let Some(b) = self.bucket(&liq.exchange, minute)
        {
            b.usd += liq.notional_usd;
        }
        if let Some(s) = self.scopes.get_mut(MARKET) {
            s.baseline = None;
        }
    }

    /// Processes a live liquidation. Returns the signals it causes.
    pub fn on_liquidation(&mut self, liq: &Liquidation) -> Vec<Signal> {
        if !self.cfg.enabled || !super::series::gt(liq.notional_usd, 0.0) {
            return Vec::new();
        }
        let ts = liq.recv_ts;
        self.observe(&liq.exchange, ts);
        let minute = ts.div_euclid(60_000);
        let symbol: Arc<str> = Arc::from(liq.symbol.as_str());
        let mut names = vec![MARKET.to_string()];
        if self.cfg.per_exchange {
            names.push(liq.exchange.clone());
        }
        let mut out = Vec::new();
        for name in names {
            if let Some(b) = self.bucket(&name, minute) {
                b.usd += liq.notional_usd;
            }
            let rec = Rec {
                ts,
                usd: liq.notional_usd,
                long: liq.side == Side::Long as i32,
                symbol: symbol.clone(),
                price: liq.price,
            };
            if let Some(sig) = self.scope_event(&name, rec) {
                out.push(sig);
            }
            out.extend(self.scope_tick(&name, ts));
        }
        out
    }

    /// Moves time forward: prunes windows and ends cascades that have been quiet long enough.
    pub fn advance(&mut self, now_ms: i64) -> Vec<Signal> {
        let names: Vec<String> = self.scopes.keys().cloned().collect();
        let mut out = Vec::new();
        for name in names {
            out.extend(self.scope_tick(&name, now_ms));
        }
        out
    }

    fn baseline(&mut self, name: &str, minute: i64) -> BaselineCache {
        let min_minutes = self.cfg.min_baseline_minutes;
        let Some(s) = self.scopes.get_mut(name) else {
            return BaselineCache {
                for_minute: minute,
                ready: false,
                median: 0.0,
                sigma: 0.0,
            };
        };
        if let Some(c) = s.baseline
            && c.for_minute == minute
        {
            return c;
        }
        let mut values: Vec<f64> = s
            .buckets
            .iter()
            .filter(|b| b.minute < minute)
            .map(|b| b.usd)
            .collect();
        let cache = if values.len() >= min_minutes {
            let (median, mad) = median_mad(&mut values);
            BaselineCache {
                for_minute: minute,
                ready: true,
                median,
                sigma: (1.4826 * mad).max(0.1 * median),
            }
        } else {
            BaselineCache {
                for_minute: minute,
                ready: false,
                median: 0.0,
                sigma: 0.0,
            }
        };
        s.baseline = Some(cache);
        cache
    }

    fn trigger_usd(&self, floor: f64, b: &BaselineCache) -> f64 {
        if b.ready {
            floor.max(b.median + self.cfg.k_mad * b.sigma)
        } else {
            3.0 * floor
        }
    }

    fn scope_event(&mut self, name: &str, rec: Rec) -> Option<Signal> {
        let ts = rec.ts;
        let minute = ts.div_euclid(60_000);
        let b = self.baseline(name, minute);
        let cfg_min_count = self.cfg.min_count as usize;
        let top = self.cfg.top_symbols;
        let s = self.scopes.get_mut(name)?;
        s.window_sum += rec.usd;
        if let Some(a) = &mut s.active {
            a.total_usd += rec.usd;
            a.count += 1;
            if rec.long {
                a.long_usd += rec.usd;
            } else {
                a.short_usd += rec.usd;
            }
            let agg = a.symbols.entry(rec.symbol.clone()).or_default();
            agg.usd += rec.usd;
            if rec.price > 0.0 {
                agg.low = if agg.low == 0.0 {
                    rec.price
                } else {
                    agg.low.min(rec.price)
                };
                agg.high = agg.high.max(rec.price);
            }
        }
        s.window.push_back(rec);
        if s.window.len() > MAX_WINDOW_RECORDS
            && let Some(old) = s.window.pop_front()
        {
            s.window_sum -= old.usd;
        }
        // Prune the window to (ts - 60 s, ts].
        while s.window.front().is_some_and(|r| r.ts <= ts - WINDOW_MS) {
            if let Some(old) = s.window.pop_front() {
                s.window_sum -= old.usd;
            }
        }
        if s.window_sum < 0.0 || s.window.is_empty() {
            s.window_sum = s.window.iter().map(|r| r.usd).sum();
        }
        let floor = s.floor_usd;
        let trigger = if b.ready {
            floor.max(b.median + self.cfg.k_mad * b.sigma)
        } else {
            3.0 * floor
        };
        let horizon = ((self.cfg.quiet_secs as i64) * 1000).min(WINDOW_MS);
        let s = self.scopes.get_mut(name)?;
        if s.active.is_some() {
            let recent = s.recent_sum(ts, horizon);
            if let Some(a) = &mut s.active {
                a.peak_usd = a.peak_usd.max(s.window_sum);
                if recent >= self.cfg.end_ratio * a.trigger_usd * horizon as f64 / WINDOW_MS as f64
                {
                    a.last_hot_ts = ts;
                }
            }
            return None;
        }
        if s.window_sum < trigger || s.window.len() < cfg_min_count {
            return None;
        }
        // A cascade starts: seed the totals from the window that triggered it.
        let mut active = Active {
            start_ts: s.window.front().map_or(ts, |r| r.ts),
            last_hot_ts: ts,
            trigger_usd: trigger,
            median: b.median,
            total_usd: 0.0,
            count: 0,
            long_usd: 0.0,
            short_usd: 0.0,
            peak_usd: s.window_sum,
            symbols: HashMap::new(),
        };
        for r in &s.window {
            active.total_usd += r.usd;
            active.count += 1;
            if r.long {
                active.long_usd += r.usd;
            } else {
                active.short_usd += r.usd;
            }
            let agg = active.symbols.entry(r.symbol.clone()).or_default();
            agg.usd += r.usd;
            if r.price > 0.0 {
                agg.low = if agg.low == 0.0 {
                    r.price
                } else {
                    agg.low.min(r.price)
                };
                agg.high = agg.high.max(r.price);
            }
        }
        let sig = start_signal(&s.exchange, &active, s.window_sum, ts, top, b.ready);
        s.active = Some(active);
        EngineStats::bump(&self.stats.cascade_started);
        EngineStats::bump(&self.stats.signals[2]);
        Some(sig)
    }

    fn scope_tick(&mut self, name: &str, now: i64) -> Vec<Signal> {
        let end_ratio = self.cfg.end_ratio;
        let quiet = (self.cfg.quiet_secs as i64) * 1000;
        let top = self.cfg.top_symbols;
        let Some(s) = self.scopes.get_mut(name) else {
            return Vec::new();
        };
        while s.window.front().is_some_and(|r| r.ts <= now - WINDOW_MS) {
            if let Some(old) = s.window.pop_front() {
                s.window_sum -= old.usd;
            }
        }
        if s.window.is_empty() {
            s.window_sum = 0.0;
        }
        let horizon = quiet.min(WINDOW_MS);
        let recent = s.recent_sum(now, horizon);
        let Some(a) = &mut s.active else {
            return Vec::new();
        };
        if recent >= end_ratio * a.trigger_usd * horizon as f64 / WINDOW_MS as f64 {
            a.last_hot_ts = a.last_hot_ts.max(now);
            return Vec::new();
        }
        if now - a.last_hot_ts < quiet {
            return Vec::new();
        }
        let Some(a) = s.active.take() else {
            return Vec::new();
        };
        EngineStats::bump(&self.stats.cascade_ended);
        EngineStats::bump(&self.stats.signals[2]);
        vec![end_signal(&s.exchange, &a, now, top)]
    }

    /// Number of buckets held in all scopes (for tests and metrics).
    pub fn bucket_count(&self) -> usize {
        self.scopes.values().map(|s| s.buckets.len()).sum()
    }

    /// True while a cascade is active in the scope.
    pub fn is_active(&self, scope: &str) -> bool {
        self.scopes.get(scope).is_some_and(|s| s.active.is_some())
    }

    /// The trigger (USD) currently in force for a scope, for tests and diagnostics.
    pub fn current_trigger(&mut self, scope: &str, ts: i64) -> Option<f64> {
        let floor = self.scopes.get(scope)?.floor_usd;
        let b = self.baseline(scope, ts.div_euclid(60_000));
        Some(self.trigger_usd(floor, &b))
    }
}

type TopSymbols = (Vec<(Arc<str>, f64)>, Option<(Arc<str>, f64, f64)>);

fn top_symbols(a: &Active, n: usize) -> TopSymbols {
    let mut v: Vec<(&Arc<str>, &SymbolAgg)> = a.symbols.iter().collect();
    v.sort_by(|x, y| y.1.usd.total_cmp(&x.1.usd).then_with(|| x.0.cmp(y.0)));
    let range = v
        .first()
        .filter(|(_, g)| g.low > 0.0)
        .map(|(s, g)| ((*s).clone(), g.low, g.high));
    (
        v.into_iter()
            .take(n)
            .map(|(s, g)| (s.clone(), g.usd))
            .collect(),
        range,
    )
}

fn intensity(value: f64, median: f64) -> f64 {
    100.0 * value / median.max(1000.0)
}

fn severity(value: f64, trigger: f64) -> Severity {
    if value >= 3.0 * trigger {
        Severity::Critical
    } else {
        Severity::Warning
    }
}

fn fmt_top(top: &[(Arc<str>, f64)]) -> String {
    top.iter()
        .map(|(s, usd)| format!("{s} {}", fmt_usd(*usd)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn start_signal(
    exchange: &str,
    a: &Active,
    window_usd: f64,
    ts: i64,
    top: usize,
    baseline_ready: bool,
) -> Signal {
    let (tops, _) = top_symbols(a, top);
    let mut d = std::collections::BTreeMap::new();
    d.insert(
        "key".to_string(),
        signal_key("liq_cascade", exchange, "", 60, a.start_ts) + ":start",
    );
    d.insert("phase".into(), "start".into());
    d.insert("scope".into(), exchange.to_string());
    d.insert(
        "severity".into(),
        severity(window_usd, a.trigger_usd).as_str().into(),
    );
    d.insert("trigger_usd".into(), format!("{:.0}", a.trigger_usd));
    d.insert(
        "baseline_median_usd_per_min".into(),
        format!("{:.0}", a.median),
    );
    d.insert(
        "baseline".into(),
        if baseline_ready {
            "24h"
        } else {
            "warming up (3x floor)"
        }
        .into(),
    );
    d.insert("window_usd".into(), format!("{window_usd:.0}"));
    d.insert("count".into(), a.count.to_string());
    d.insert("long_usd".into(), format!("{:.0}", a.long_usd));
    d.insert("short_usd".into(), format!("{:.0}", a.short_usd));
    d.insert("symbols".into(), a.symbols.len().to_string());
    d.insert("top_symbols".into(), fmt_top(&tops));
    d.insert("start_ts".into(), a.start_ts.to_string());
    Signal {
        r#type: SignalType::LiqCascade as i32,
        exchange: exchange.to_string(),
        symbol: String::new(),
        window_secs: 60,
        change_pct: intensity(window_usd, a.median),
        change_abs: window_usd,
        threshold: intensity(a.trigger_usd, a.median),
        ts,
        details: d.into_iter().collect(),
    }
}

fn end_signal(exchange: &str, a: &Active, now: i64, top: usize) -> Signal {
    let (tops, range) = top_symbols(a, top);
    let end_ts = a.last_hot_ts;
    let mut d = std::collections::BTreeMap::new();
    d.insert(
        "key".to_string(),
        signal_key("liq_cascade", exchange, "", 60, a.start_ts) + ":end",
    );
    d.insert("phase".into(), "end".into());
    d.insert("scope".into(), exchange.to_string());
    d.insert(
        "severity".into(),
        severity(a.peak_usd, a.trigger_usd).as_str().into(),
    );
    d.insert("start_ts".into(), a.start_ts.to_string());
    d.insert("end_ts".into(), end_ts.to_string());
    d.insert(
        "duration_secs".into(),
        ((end_ts - a.start_ts).max(0) / 1000).to_string(),
    );
    d.insert("total_usd".into(), format!("{:.0}", a.total_usd));
    d.insert("count".into(), a.count.to_string());
    d.insert("long_usd".into(), format!("{:.0}", a.long_usd));
    d.insert("short_usd".into(), format!("{:.0}", a.short_usd));
    let share = if a.total_usd > 0.0 {
        a.long_usd / a.total_usd
    } else {
        0.0
    };
    d.insert("long_share".into(), format!("{share:.3}"));
    d.insert("peak_usd_per_min".into(), format!("{:.0}", a.peak_usd));
    d.insert("symbols".into(), a.symbols.len().to_string());
    d.insert("top_symbols".into(), fmt_top(&tops));
    if let Some((symbol, low, high)) = range {
        d.insert("price_symbol".into(), symbol.to_string());
        d.insert("price_low".into(), format!("{low}"));
        d.insert("price_high".into(), format!("{high}"));
    }
    Signal {
        r#type: SignalType::LiqCascade as i32,
        exchange: exchange.to_string(),
        symbol: String::new(),
        window_secs: 60,
        change_pct: intensity(a.peak_usd, a.median),
        change_abs: a.total_usd,
        threshold: intensity(a.trigger_usd, a.median),
        ts: now,
        details: d.into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    const T0: i64 = 1_760_000_000_000 - 1_760_000_000_000 % 60_000;

    fn liq(exchange: &str, symbol: &str, ts: i64, usd: f64, long: bool, price: f64) -> Liquidation {
        Liquidation {
            exchange: exchange.into(),
            symbol_raw: symbol.replace('-', ""),
            symbol: symbol.into(),
            exchange_ts: ts,
            recv_ts: ts,
            side: if long { Side::Long } else { Side::Short } as i32,
            price,
            qty: usd / price,
            notional_usd: usd,
        }
    }

    fn cfg() -> CascadeConfig {
        CascadeConfig {
            min_baseline_minutes: 120,
            floor_usd: 1_000_000.0,
            floor_usd_exchange: 500_000.0,
            ..CascadeConfig::default()
        }
    }

    /// Fills `minutes` of history before `T0` with a steady ~50k USD per minute (varying a little) and
    /// returns the detector.
    fn with_baseline(minutes: i64, cfg: CascadeConfig) -> (CascadeDetector, Arc<EngineStats>) {
        let stats = Arc::new(EngineStats::default());
        let mut d = CascadeDetector::new(cfg, stats.clone());
        for m in (1..=minutes).rev() {
            let ts = T0 - m * 60_000 + 10_000;
            let usd = 40_000.0 + ((m * 7919) % 20_000) as f64;
            d.warm(&liq(
                "binance",
                "BTC-USDT-PERP",
                ts,
                usd,
                m % 2 == 0,
                60_000.0,
            ));
        }
        (d, stats)
    }

    #[test]
    fn burst_of_forty_times_the_baseline_starts_and_ends_a_cascade() {
        let (mut d, stats) = with_baseline(24 * 60, cfg());
        let mut signals = Vec::new();
        // Normal minute: no signal.
        signals.extend(d.on_liquidation(&liq(
            "binance",
            "BTC-USDT-PERP",
            T0 - 30_000,
            50_000.0,
            true,
            60_000.0,
        )));
        assert!(signals.is_empty());
        // 40x burst (about 2M USD in a minute) across symbols, longs mostly.
        for i in 0..40 {
            let sym = if i % 4 == 0 {
                "ETH-USDT-PERP"
            } else {
                "BTC-USDT-PERP"
            };
            let price = if sym == "BTC-USDT-PERP" {
                60_000.0 - i as f64 * 100.0
            } else {
                3_000.0
            };
            signals.extend(d.on_liquidation(&liq(
                "binance",
                sym,
                T0 + 60_000 + i * 1000,
                50_000.0,
                i % 5 != 0,
                price,
            )));
        }
        let starts: Vec<_> = signals
            .iter()
            .filter(|s| s.details.get("phase").map(String::as_str) == Some("start"))
            .collect();
        assert_eq!(starts.len(), 2, "market-wide and binance: {signals:?}");
        assert!(starts.iter().any(|s| s.exchange == "*"));
        assert!(starts.iter().any(|s| s.exchange == "binance"));
        let s = starts[0];
        assert_eq!(s.r#type, SignalType::LiqCascade as i32);
        assert!(s.symbol.is_empty());
        assert!(s.change_pct >= s.threshold);
        assert!(d.is_active("*"));
        // Quiet: nothing for a while, then the closing summary arrives.
        let t_end = T0 + 60_000 + 40_000 + 60_000 + 130_000;
        let ended = d.advance(t_end);
        let ends: Vec<_> = ended
            .iter()
            .filter(|s| s.details.get("phase").map(String::as_str) == Some("end"))
            .collect();
        assert_eq!(ends.len(), 2, "{ended:?}");
        let e = ends.iter().find(|s| s.exchange == "*").unwrap();
        assert_eq!(e.details["count"], "40");
        assert_eq!(e.details["symbols"], "2");
        assert!(e.details["top_symbols"].starts_with("BTC-USDT-PERP"));
        assert!(
            e.details["price_low"].parse::<f64>().unwrap()
                < e.details["price_high"].parse::<f64>().unwrap()
        );
        assert!((e.change_abs - 2_000_000.0).abs() < 1.0);
        let long: f64 = e.details["long_usd"].parse().unwrap();
        let short: f64 = e.details["short_usd"].parse().unwrap();
        assert!((long + short - 2_000_000.0).abs() < 1.0 && long > short);
        assert!(!d.is_active("*"));
        assert_eq!(stats.cascade_started.load(Ordering::Relaxed), 2);
        assert_eq!(stats.cascade_ended.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn steady_flow_and_small_spikes_do_not_start_a_cascade() {
        let (mut d, _) = with_baseline(24 * 60, cfg());
        let mut n = 0;
        for i in 0..300 {
            n += d
                .on_liquidation(&liq(
                    "binance",
                    "BTC-USDT-PERP",
                    T0 + i * 2_000,
                    1_000.0 + (i % 7) as f64 * 2_000.0,
                    true,
                    60_000.0,
                ))
                .len();
        }
        assert_eq!(n, 0);
        // A single large liquidation (5 s, 900k) stays under the 1M market floor and has no count.
        assert!(
            d.on_liquidation(&liq(
                "binance",
                "BTC-USDT-PERP",
                T0 + 700_000,
                900_000.0,
                true,
                60_000.0
            ))
            .is_empty()
        );
    }

    #[test]
    fn floor_alone_is_not_enough_when_the_baseline_is_high() {
        // A baseline of about 2M per minute: a 1.5M minute is above the floor but not several MADs above.
        let mut d = CascadeDetector::new(cfg(), Arc::new(EngineStats::default()));
        for m in (1..=300).rev() {
            let ts = T0 - m * 60_000 + 10_000;
            d.warm(&liq(
                "binance",
                "BTC-USDT-PERP",
                ts,
                1_900_000.0 + ((m * 31) % 5) as f64 * 100_000.0,
                true,
                60_000.0,
            ));
        }
        let mut n = 0;
        for i in 0..30 {
            n += d
                .on_liquidation(&liq(
                    "binance",
                    "BTC-USDT-PERP",
                    T0 + i * 2_000,
                    50_000.0,
                    true,
                    60_000.0,
                ))
                .len();
        }
        assert_eq!(n, 0);
        let trigger = d.current_trigger("*", T0).unwrap();
        assert!(trigger > 2_000_000.0, "{trigger}");
    }

    #[test]
    fn without_a_baseline_the_trigger_is_three_times_the_floor() {
        let mut d = CascadeDetector::new(cfg(), Arc::new(EngineStats::default()));
        assert_eq!(d.current_trigger("*", T0), Some(3_000_000.0));
        let mut out = Vec::new();
        for i in 0..20 {
            out.extend(d.on_liquidation(&liq(
                "binance",
                "BTC-USDT-PERP",
                T0 + i * 1000,
                100_000.0,
                true,
                60_000.0,
            )));
        }
        assert!(
            out.iter().all(|s| s.exchange != "*"),
            "2M is under 3x floor market-wide"
        );
        for i in 20..40 {
            out.extend(d.on_liquidation(&liq(
                "binance",
                "BTC-USDT-PERP",
                T0 + i * 1000,
                100_000.0,
                true,
                60_000.0,
            )));
        }
        assert!(out.iter().any(|s| s.exchange == "*"), "4M is over it");
    }

    #[test]
    fn a_continuing_burst_keeps_one_cascade_open() {
        let (mut d, _) = with_baseline(24 * 60, cfg());
        let mut starts = 0;
        for i in 0..400 {
            for s in d.on_liquidation(&liq(
                "binance",
                "BTC-USDT-PERP",
                T0 + i * 1000,
                60_000.0,
                true,
                60_000.0,
            )) {
                if s.details["phase"] == "start" && s.exchange == "*" {
                    starts += 1;
                }
            }
        }
        assert_eq!(starts, 1);
        assert!(d.is_active("*"));
        // Not yet quiet long enough.
        assert!(d.advance(T0 + 400_000 + 60_000).is_empty());
    }

    #[test]
    fn baseline_ignores_minutes_that_were_not_observed() {
        let stats = Arc::new(EngineStats::default());
        let mut d = CascadeDetector::new(cfg(), stats);
        // 130 observed minutes, then a 10 hour hole with nothing observed.
        for m in 0..130 {
            d.observe("binance", T0 - 40 * 3_600_000 + m * 60_000);
        }
        assert_eq!(d.bucket_count(), 260, "market + binance");
    }

    #[test]
    fn disabled_detector_is_silent() {
        let mut d = CascadeDetector::new(
            CascadeConfig {
                enabled: false,
                ..cfg()
            },
            Arc::new(EngineStats::default()),
        );
        assert!(
            d.on_liquidation(&liq("binance", "BTC-USDT-PERP", T0, 9e9, true, 1.0))
                .is_empty()
        );
    }
}
