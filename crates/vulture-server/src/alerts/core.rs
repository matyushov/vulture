//! The synchronous heart of the engine: state per symbol, the detectors and the rules. It owns no task and no
//! clock, so it is tested with plain function calls; [`super::engine`] wraps it with channels and time.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use arc_swap::ArcSwap;
use vulture_proto::v1::{Event, Gap, Liquidation, OpenInterest, Signal, event::Payload};

use super::cascade::CascadeDetector;
use super::model::{CmpOp, CondMetric, Condition, RuleKind};
use super::oi::OiDetector;
use super::rules::{
    CompiledRule, Hit, RuleCooldowns, RuleSet, composite_alert, liquidation_alert,
    liquidation_matches, oi_change_alert, signal_alert, signal_cooldown_key, signal_matches,
};
use super::series::{Push, SAMPLE_BYTES, Sample, Series, SeriesLimits};
use super::stats::EngineStats;
use crate::config::AlertsConfig;

/// What processing produced.
#[derive(Debug, Default)]
pub struct Output {
    /// Signals to publish into the event stream.
    pub signals: Vec<Signal>,
    /// Rule matches to hand to the delivery queues.
    pub hits: Vec<Hit>,
}

/// Gap stream names that do not interrupt the OI series.
const LIQUIDATION_STREAMS: [&str; 1] = ["liquidations"];

/// Per-symbol state that is not an OI ring buffer: the latest funding rate and index price, and the recent
/// liquidations (for composite rules).
#[derive(Debug, Default)]
struct SymbolExtra {
    funding: Option<f64>,
    index: Option<f64>,
    /// (receive time, USD), oldest first.
    liq: std::collections::VecDeque<(i64, f64)>,
}

/// Most liquidations kept per symbol.
const MAX_LIQ_PER_SYMBOL: usize = 5000;

/// The engine state.
pub struct Core {
    stats: Arc<EngineStats>,
    rules: Arc<ArcSwap<RuleSet>>,
    series: HashMap<String, HashMap<String, Series>>,
    exchange_break: HashMap<String, i64>,
    global_break: i64,
    limits: SeriesLimits,
    max_symbols: usize,
    symbols: usize,
    oi: OiDetector,
    cascade: CascadeDetector,
    cooldowns: RuleCooldowns,
    /// Live liquidations at or before this time were already counted by the warm-up replay.
    replay_liq_watermark: i64,
    last_gauge_ms: i64,
    extras: HashMap<String, HashMap<String, SymbolExtra>>,
    extras_count: usize,
    composite_interval_ms: i64,
    last_composite_ms: i64,
    extras_retention_ms: i64,
}

impl Core {
    /// New engine state from the configuration and the live rule set.
    pub fn new(cfg: &AlertsConfig, rules: Arc<ArcSwap<RuleSet>>, stats: Arc<EngineStats>) -> Self {
        let oi = OiDetector::new(cfg.oi_spike.clone(), stats.clone());
        // Keep the longest window plus its tolerance (300 s) and some slack.
        let retention_ms = oi.max_window_ms() + 600_000;
        Self {
            limits: SeriesLimits {
                retention_ms,
                max_samples: cfg.series_max_samples,
                spacing_ms: (cfg.sample_spacing_secs as i64) * 1000,
                hole_ms: (cfg.hole_secs as i64) * 1000,
            },
            max_symbols: cfg.max_symbols,
            symbols: 0,
            cascade: CascadeDetector::new(cfg.liq_cascade.clone(), stats.clone()),
            oi,
            stats,
            rules,
            series: HashMap::new(),
            exchange_break: HashMap::new(),
            global_break: 0,
            cooldowns: RuleCooldowns::default(),
            replay_liq_watermark: i64::MIN,
            last_gauge_ms: 0,
            extras: HashMap::new(),
            extras_count: 0,
            composite_interval_ms: (cfg.composite_eval_secs as i64) * 1000,
            last_composite_ms: 0,
            extras_retention_ms: retention_ms,
        }
    }

    /// The statistics.
    pub fn stats(&self) -> &Arc<EngineStats> {
        &self.stats
    }

    /// Marks the end of the warm-up replay: live liquidations not newer than `ts` were already counted.
    pub fn set_replay_watermark(&mut self, ts: i64) {
        self.replay_liq_watermark = ts;
    }

    /// Restores detector cooldowns from a signal found in the replayed stream.
    pub fn restore_signal(&mut self, s: &Signal) {
        self.oi.restore(s);
    }

    /// Number of symbols with a buffer.
    pub fn symbol_count(&self) -> usize {
        self.symbols
    }

    /// Series of a symbol (tests).
    pub fn series(&self, exchange: &str, symbol: &str) -> Option<&Series> {
        self.series.get(exchange)?.get(symbol)
    }

    /// The cascade detector (tests).
    pub fn cascade(&self) -> &CascadeDetector {
        &self.cascade
    }

    /// Processes one event. With `live == false` (warm-up) only the state is updated: no signal, no message.
    pub fn process(&mut self, event: &Event, live: bool, out: &mut Output) {
        match &event.payload {
            Some(Payload::OpenInterest(oi)) => {
                self.stats.events_oi.fetch_add(1, Ordering::Relaxed);
                self.on_oi(oi, live, out);
            }
            Some(Payload::Liquidation(liq)) => {
                self.stats.events_liq.fetch_add(1, Ordering::Relaxed);
                self.on_liquidation(liq, live, out);
            }
            Some(Payload::Gap(gap)) => {
                self.stats.events_gap.fetch_add(1, Ordering::Relaxed);
                self.on_gap(gap);
            }
            // Signals are our own output; they are handled by the warm-up for cooldowns only.
            Some(Payload::Signal(_)) | None => {}
        }
    }

    fn on_gap(&mut self, gap: &Gap) {
        if LIQUIDATION_STREAMS.contains(&gap.stream.as_str()) {
            return;
        }
        if gap.exchange == "*" {
            self.global_break = self.global_break.max(gap.to_ts);
        } else {
            let b = self.exchange_break.entry(gap.exchange.clone()).or_insert(0);
            *b = (*b).max(gap.to_ts);
        }
    }

    fn on_oi(&mut self, oi: &OpenInterest, live: bool, out: &mut Output) {
        let ts = oi.recv_ts;
        self.cascade.observe(&oi.exchange, ts);
        let sample = Sample {
            ts,
            oi_base: oi.oi_base,
            oi_usd: oi.oi_usd.unwrap_or(f64::NAN),
            mark: oi.mark_price.unwrap_or(f64::NAN),
        };
        if !sample.oi_base.is_finite() || sample.oi_base < 0.0 {
            return;
        }
        self.note_extras(&oi.exchange, &oi.symbol, |e| {
            e.funding = oi.funding_rate;
            e.index = oi.index_price;
        });
        let by_exchange = match self.series.get_mut(&oi.exchange) {
            Some(m) => m,
            None => self.series.entry(oi.exchange.clone()).or_default(),
        };
        let series = match by_exchange.get_mut(&oi.symbol) {
            Some(s) => s,
            None => {
                if self.symbols >= self.max_symbols {
                    self.stats.symbols_rejected.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                self.symbols += 1;
                by_exchange.entry(oi.symbol.clone()).or_default()
            }
        };
        if series.push(sample, &self.limits) == Push::Ignored || !live {
            return;
        }
        let series = &*series;
        let extra = self
            .global_break
            .max(self.exchange_break.get(&oi.exchange).copied().unwrap_or(0));
        let rules = self.rules.load();
        let signals = self
            .oi
            .evaluate(&oi.exchange, &oi.symbol, series, extra, &rules.overrides);
        // Direct OI rules.
        for rule in &rules.rules {
            let RuleKind::OiChange {
                window_secs,
                min_change_pct,
                min_change_usd,
                ..
            } = &rule.kind
            else {
                continue;
            };
            if !rule.matches_scope(&oi.exchange, &oi.symbol) {
                continue;
            }
            let Some(c) = series.change(i64::from(*window_secs) * 1000, extra) else {
                continue;
            };
            if c.pct.abs() < *min_change_pct {
                continue;
            }
            if *min_change_usd > 0.0 && !super::series::ge(c.usd.abs(), *min_change_usd) {
                continue;
            }
            if rule.is_quiet(ts) {
                self.stats
                    .quiet_suppressed_rule
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let key = format!("{}|{}|{}", oi.exchange, oi.symbol, c.pct > 0.0);
            if !self.cooldowns.allow(&rule.id, &key, ts, rule.cooldown_ms) {
                self.stats
                    .rule_suppressed_cooldown
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            out.hits.push(Hit {
                rule_id: rule.id.clone(),
                channels: rule.channels.clone(),
                alert: oi_change_alert(rule, &oi.exchange, &oi.symbol, *window_secs, &c),
            });
        }
        drop(rules);
        for s in signals {
            self.emit(s, out);
        }
    }

    fn on_liquidation(&mut self, liq: &Liquidation, live: bool, out: &mut Output) {
        if !live {
            self.cascade.warm(liq);
            return;
        }
        if liq.recv_ts <= self.replay_liq_watermark {
            return;
        }
        self.note_extras(&liq.exchange, &liq.symbol, |e| {
            e.liq.push_back((liq.recv_ts, liq.notional_usd));
            while e.liq.len() > MAX_LIQ_PER_SYMBOL {
                e.liq.pop_front();
            }
        });
        let signals = self.cascade.on_liquidation(liq);
        {
            let rules = self.rules.load();
            for rule in &rules.rules {
                if !liquidation_matches(rule, liq) {
                    continue;
                }
                if rule.is_quiet(liq.recv_ts) {
                    self.stats
                        .quiet_suppressed_rule
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let key = format!("{}|{}", liq.exchange, liq.symbol);
                if !self
                    .cooldowns
                    .allow(&rule.id, &key, liq.recv_ts, rule.cooldown_ms)
                {
                    self.stats
                        .rule_suppressed_cooldown
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                out.hits.push(Hit {
                    rule_id: rule.id.clone(),
                    channels: rule.channels.clone(),
                    alert: liquidation_alert(rule, liq),
                });
            }
        }
        for s in signals {
            self.emit(s, out);
        }
    }

    /// Records a signal as output and evaluates the pass-through rules on it.
    fn emit(&mut self, s: Signal, out: &mut Output) {
        let rules = self.rules.load();
        for rule in &rules.rules {
            if !signal_matches(rule, &s) {
                continue;
            }
            if rule.is_quiet(s.ts) {
                self.stats
                    .quiet_suppressed_rule
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if !self
                .cooldowns
                .allow(&rule.id, &signal_cooldown_key(&s), s.ts, rule.cooldown_ms)
            {
                self.stats
                    .rule_suppressed_cooldown
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            out.hits.push(Hit {
                rule_id: rule.id.clone(),
                channels: rule.channels.clone(),
                alert: signal_alert(&s),
            });
        }
        drop(rules);
        out.signals.push(s);
    }

    /// Periodic work at wall-clock `now_ms`: ends quiet cascades, drops stale buffers, refreshes the gauges.
    pub fn tick(&mut self, now_ms: i64, out: &mut Output) {
        for s in self.cascade.advance(now_ms) {
            self.emit(s, out);
        }
        if self.composite_interval_ms > 0
            && now_ms - self.last_composite_ms >= self.composite_interval_ms
        {
            self.last_composite_ms = now_ms;
            self.eval_composites(now_ms, out);
        }
        if now_ms - self.last_gauge_ms >= 10_000 {
            self.last_gauge_ms = now_ms;
            self.refresh_gauges(now_ms);
        }
    }

    /// Updates the extra state of a symbol (created on first use, within the symbol limit).
    fn note_extras(&mut self, exchange: &str, symbol: &str, f: impl FnOnce(&mut SymbolExtra)) {
        if let Some(e) = self
            .extras
            .get_mut(exchange)
            .and_then(|m| m.get_mut(symbol))
        {
            f(e);
            return;
        }
        if self.extras_count >= self.max_symbols {
            return;
        }
        self.extras_count += 1;
        let mut e = SymbolExtra::default();
        f(&mut e);
        self.extras
            .entry(exchange.to_string())
            .or_default()
            .insert(symbol.to_string(), e);
    }

    /// The value of one condition for a symbol, `None` when it cannot be computed (no reference, a hole or gap
    /// in the window, no funding data).
    fn condition_value(
        c: &Condition,
        series: &Series,
        extra: Option<&SymbolExtra>,
        extra_break: i64,
        now_ms: i64,
    ) -> Option<f64> {
        let window_ms = i64::from(c.window_secs) * 1000;
        match c.metric {
            CondMetric::OiChangePct => series.change(window_ms, extra_break).map(|c| c.pct),
            CondMetric::PriceChangePct => series.mark_change(window_ms, extra_break),
            CondMetric::FundingRate => extra?.funding,
            CondMetric::BasisPct => {
                crate::query::agg::basis_pct(series.last().map(|s| s.mark), extra?.index)
            }
            CondMetric::LiqUsd => Some(
                extra
                    .map(|e| {
                        e.liq
                            .iter()
                            .filter(|(t, _)| *t > now_ms - window_ms)
                            .map(|(_, usd)| usd)
                            .sum()
                    })
                    .unwrap_or(0.0),
            ),
            CondMetric::Regime => {
                let price = series.mark_change(window_ms, extra_break)?;
                let oi = series.change(window_ms, extra_break)?.pct;
                crate::query::agg::regime_of(price, oi).map(|(r, _)| f64::from(r as i32))
            }
        }
    }

    /// Evaluates every composite rule against every active symbol. A symbol is active when its newest OI sample
    /// is not older than the hole limit. The conditions are ANDed; the cooldown is per rule, exchange and symbol.
    fn eval_composites(&mut self, now_ms: i64, out: &mut Output) {
        let rules = self.rules.load_full();
        let composites: Vec<&CompiledRule> = rules
            .rules
            .iter()
            .filter(|r| matches!(r.kind, RuleKind::Composite { .. }))
            .collect();
        if composites.is_empty() {
            return;
        }
        let started = std::time::Instant::now();
        let stale_ms = self.limits.hole_ms;
        for (exchange, by_symbol) in &self.series {
            let extra_break = self
                .global_break
                .max(self.exchange_break.get(exchange).copied().unwrap_or(0));
            for (symbol, series) in by_symbol {
                if series.last().is_none_or(|l| now_ms - l.ts > stale_ms) {
                    continue;
                }
                let extra = self.extras.get(exchange).and_then(|m| m.get(symbol));
                for rule in &composites {
                    if !rule.matches_scope(exchange, symbol) {
                        continue;
                    }
                    let RuleKind::Composite {
                        conditions,
                        severity,
                    } = &rule.kind
                    else {
                        continue;
                    };
                    self.stats
                        .composite_evaluations
                        .fetch_add(1, Ordering::Relaxed);
                    let mut values = Vec::with_capacity(conditions.len());
                    let mut all = true;
                    for c in conditions {
                        match Self::condition_value(c, series, extra, extra_break, now_ms) {
                            Some(v) if c.op != CmpOp::Eq || c.metric == CondMetric::Regime => {
                                if !c.op.holds(v, c.threshold) {
                                    all = false;
                                    break;
                                }
                                values.push(v);
                            }
                            _ => {
                                all = false;
                                break;
                            }
                        }
                    }
                    if !all {
                        continue;
                    }
                    self.stats.composite_matched.fetch_add(1, Ordering::Relaxed);
                    if rule.is_quiet(now_ms) {
                        self.stats
                            .quiet_suppressed_rule
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let key = format!("{exchange}|{symbol}");
                    if !self
                        .cooldowns
                        .allow(&rule.id, &key, now_ms, rule.cooldown_ms)
                    {
                        self.stats
                            .rule_suppressed_cooldown
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    out.hits.push(Hit {
                        rule_id: rule.id.clone(),
                        channels: rule.channels.clone(),
                        alert: composite_alert(
                            rule, exchange, symbol, conditions, &values, *severity, now_ms,
                        ),
                    });
                }
            }
        }
        self.stats.composite_pass_ns_max.fetch_max(
            started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn refresh_gauges(&mut self, now_ms: i64) {
        let stale_before = now_ms - self.limits.retention_ms;
        let mut series = 0u64;
        let mut samples = 0u64;
        let mut bytes = 0u64;
        for by_exchange in self.series.values_mut() {
            by_exchange.retain(|_, s| s.last().is_some_and(|l| l.ts >= stale_before));
            for s in by_exchange.values() {
                series += 1;
                samples += s.len() as u64;
                bytes += (s.capacity_bytes() + std::mem::size_of::<Series>()) as u64;
            }
        }
        // Funding and liquidation state of symbols that went quiet is dropped with their buffers.
        let keep_after = now_ms - self.extras_retention_ms;
        let mut count = 0;
        for by_exchange in self.extras.values_mut() {
            by_exchange.retain(|_, e| {
                while e.liq.front().is_some_and(|(t, _)| *t < keep_after) {
                    e.liq.pop_front();
                }
                let live = !e.liq.is_empty() || e.funding.is_some() || e.index.is_some();
                count += usize::from(live);
                live
            });
        }
        self.extras_count = count;
        self.symbols = series as usize;
        self.stats.series.store(series, Ordering::Relaxed);
        self.stats.samples.store(samples, Ordering::Relaxed);
        self.stats.memory_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Forces a gauge refresh (tests, end of warm-up). Stale buffers are judged against `now_ms`.
    pub fn refresh_now(&mut self, now_ms: i64) {
        self.last_gauge_ms = now_ms;
        self.refresh_gauges(now_ms);
    }

    /// Bytes per sample (documentation and tests).
    pub const fn sample_bytes() -> usize {
        SAMPLE_BYTES
    }
}

#[cfg(test)]
mod tests {
    use vulture_proto::v1::{Side, SignalType};

    use super::*;
    use crate::alerts::model::{RuleDef, RuleKind, Severity, SignalKind};

    fn oi(exchange: &str, symbol: &str, ts: i64, base: f64, mark: f64) -> Event {
        Event {
            id: String::new(),
            payload: Some(Payload::OpenInterest(OpenInterest {
                exchange: exchange.into(),
                symbol_raw: symbol.replace('-', ""),
                symbol: symbol.into(),
                exchange_ts: ts,
                recv_ts: ts,
                oi_base: base,
                oi_usd: Some(base * mark),
                mark_price: Some(mark),
                ..OpenInterest::default()
            })),
        }
    }

    fn liq_event(exchange: &str, symbol: &str, ts: i64, usd: f64) -> Event {
        Event {
            id: String::new(),
            payload: Some(Payload::Liquidation(Liquidation {
                exchange: exchange.into(),
                symbol_raw: symbol.replace('-', ""),
                symbol: symbol.into(),
                exchange_ts: ts,
                recv_ts: ts,
                side: Side::Long as i32,
                price: 100.0,
                qty: usd / 100.0,
                notional_usd: usd,
            })),
        }
    }

    fn gap(exchange: &str, stream: &str, to_ts: i64) -> Event {
        Event {
            id: String::new(),
            payload: Some(Payload::Gap(Gap {
                exchange: exchange.into(),
                stream: stream.into(),
                from_ts: to_ts - 10_000,
                to_ts,
            })),
        }
    }

    fn core_with(rules: Vec<RuleDef>) -> (Core, Arc<ArcSwap<RuleSet>>) {
        let set = Arc::new(ArcSwap::from_pointee(RuleSet::compile(&rules)));
        let cfg = AlertsConfig::default();
        (
            Core::new(&cfg, set.clone(), Arc::new(EngineStats::default())),
            set,
        )
    }

    fn feed(core: &mut Core, events: &[Event], live: bool) -> Output {
        let mut out = Output::default();
        for e in events {
            core.process(e, live, &mut out);
        }
        out
    }

    fn flat_history(n: i64, base: f64) -> Vec<Event> {
        (0..n)
            .map(|i| oi("binance", "BTC-USDT-PERP", i * 30_000, base, 1000.0))
            .collect()
    }

    fn pass_through() -> RuleDef {
        RuleDef {
            id: "sig".into(),
            name: "signals".into(),
            enabled: true,
            kind: RuleKind::Signal {
                types: vec![],
                min_severity: Severity::Info,
            },
            exchanges: vec![],
            symbols: vec![],
            channels: vec!["tg".into()],
            cooldown_secs: 0,
            quiet_hours: Vec::new(),
        }
    }

    #[test]
    fn oi_spike_becomes_a_signal_and_a_hit() {
        let (mut core, _) = core_with(vec![pass_through()]);
        let mut hist = flat_history(300, 10_000.0);
        hist.push(oi(
            "binance",
            "BTC-USDT-PERP",
            300 * 30_000,
            10_800.0,
            1000.0,
        ));
        hist.push(oi(
            "binance",
            "BTC-USDT-PERP",
            301 * 30_000,
            10_800.0,
            1000.0,
        ));
        let out = feed(&mut core, &hist, true);
        assert_eq!(out.signals.len(), 1, "{:?}", out.signals);
        assert_eq!(out.signals[0].r#type, SignalType::OiSpikeUp as i32);
        assert_eq!(out.hits.len(), 1);
        assert_eq!(out.hits[0].channels, ["tg"]);
        assert_eq!(out.hits[0].alert.title, "OI spike UP");
    }

    #[test]
    fn warm_up_builds_state_without_output_and_keeps_the_detector_sighted() {
        let (mut core, _) = core_with(vec![pass_through()]);
        let mut hist = flat_history(300, 10_000.0);
        // Even a spike inside the replayed history produces nothing while warming up.
        hist.push(oi(
            "binance",
            "BTC-USDT-PERP",
            300 * 30_000,
            11_500.0,
            1000.0,
        ));
        let out = feed(&mut core, &hist, false);
        assert!(out.signals.is_empty() && out.hits.is_empty());
        assert_eq!(core.series("binance", "BTC-USDT-PERP").unwrap().len(), 301);
        // After a restart the buffers hold 4 hours: the first live sample is judged against them.
        let (mut core2, _) = core_with(vec![]);
        feed(&mut core2, &flat_history(300, 10_000.0), false);
        let out = feed(
            &mut core2,
            &[
                oi("binance", "BTC-USDT-PERP", 300 * 30_000, 10_900.0, 1000.0),
                oi("binance", "BTC-USDT-PERP", 301 * 30_000, 10_900.0, 1000.0),
            ],
            true,
        );
        assert_eq!(
            out.signals.len(),
            1,
            "restart warm-up keeps the detector sighted"
        );
    }

    #[test]
    fn gap_event_blocks_windows_across_it() {
        let (mut core, _) = core_with(vec![]);
        feed(&mut core, &flat_history(300, 10_000.0), true);
        let t = 300 * 30_000;
        let mut evs = vec![gap("binance", "open_interest", t)];
        evs.push(oi("binance", "BTC-USDT-PERP", t, 12_000.0, 1000.0));
        evs.push(oi("binance", "BTC-USDT-PERP", t + 30_000, 12_000.0, 1000.0));
        let out = feed(&mut core, &evs, true);
        assert!(out.signals.is_empty(), "{:?}", out.signals);
        // A liquidation-stream gap does not blind the OI windows.
        let (mut core, _) = core_with(vec![]);
        feed(&mut core, &flat_history(300, 10_000.0), true);
        let evs = vec![
            gap("binance", "liquidations", t),
            oi("binance", "BTC-USDT-PERP", t, 11_000.0, 1000.0),
            oi("binance", "BTC-USDT-PERP", t + 30_000, 11_000.0, 1000.0),
        ];
        assert_eq!(feed(&mut core, &evs, true).signals.len(), 1);
        // The writer's own gap (exchange "*") blinds every exchange.
        let (mut core, _) = core_with(vec![]);
        feed(&mut core, &flat_history(300, 10_000.0), true);
        let evs = vec![
            gap("*", "redis-writer", t),
            oi("binance", "BTC-USDT-PERP", t, 11_000.0, 1000.0),
            oi("binance", "BTC-USDT-PERP", t + 30_000, 11_000.0, 1000.0),
        ];
        assert!(feed(&mut core, &evs, true).signals.is_empty());
    }

    #[test]
    fn liquidation_rule_hits_and_cools_down() {
        let rule = RuleDef {
            id: "big-liq".into(),
            name: "Big liquidations".into(),
            enabled: true,
            kind: RuleKind::Liquidation {
                min_notional_usd: 100_000.0,
                sides: vec![],
            },
            exchanges: vec![],
            symbols: vec![],
            channels: vec!["tg".into()],
            cooldown_secs: 30,
            quiet_hours: Vec::new(),
        };
        let (mut core, _) = core_with(vec![rule]);
        let evs = [
            liq_event("binance", "BTC-USDT-PERP", 1_000_000, 150_000.0),
            liq_event("binance", "BTC-USDT-PERP", 1_010_000, 150_000.0),
            liq_event("binance", "ETH-USDT-PERP", 1_011_000, 150_000.0),
            liq_event("binance", "BTC-USDT-PERP", 1_031_000, 150_000.0),
            liq_event("binance", "BTC-USDT-PERP", 1_032_000, 50_000.0),
        ];
        let out = feed(&mut core, &evs, true);
        assert_eq!(
            out.hits.len(),
            3,
            "BTC at 0s, ETH, BTC after 31s: {:?}",
            out.hits.len()
        );
        assert_eq!(
            core.stats()
                .rule_suppressed_cooldown
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn hot_swap_of_rules_applies_to_the_next_event() {
        let (mut core, set) = core_with(vec![]);
        let e = liq_event("binance", "BTC-USDT-PERP", 1_000_000, 150_000.0);
        assert!(
            feed(&mut core, std::slice::from_ref(&e), true)
                .hits
                .is_empty()
        );
        let rule = RuleDef {
            id: "r".into(),
            name: String::new(),
            enabled: true,
            kind: RuleKind::Liquidation {
                min_notional_usd: 1.0,
                sides: vec![],
            },
            exchanges: vec![],
            symbols: vec![],
            channels: vec!["tg".into()],
            cooldown_secs: 0,
            quiet_hours: Vec::new(),
        };
        set.store(Arc::new(RuleSet::compile(&[rule])));
        assert_eq!(feed(&mut core, &[e], true).hits.len(), 1);
    }

    #[test]
    fn direct_oi_rule_and_signal_filter() {
        let rule = RuleDef {
            id: "oi".into(),
            name: "OI 5m".into(),
            enabled: true,
            kind: RuleKind::OiChange {
                window_secs: 300,
                min_change_pct: 2.0,
                min_change_usd: 0.0,
                detector_override: false,
            },
            exchanges: vec!["binance".into()],
            symbols: vec![],
            channels: vec!["tg".into()],
            cooldown_secs: 600,
            quiet_hours: Vec::new(),
        };
        let sig_only_down = RuleDef {
            id: "down".into(),
            kind: RuleKind::Signal {
                types: vec![SignalKind::OiSpikeDown],
                min_severity: Severity::Info,
            },
            ..pass_through()
        };
        let (mut core, _) = core_with(vec![rule, sig_only_down]);
        let mut hist = flat_history(300, 10_000.0);
        hist.push(oi(
            "binance",
            "BTC-USDT-PERP",
            300 * 30_000,
            10_300.0,
            1000.0,
        ));
        hist.push(oi(
            "binance",
            "BTC-USDT-PERP",
            301 * 30_000,
            10_300.0,
            1000.0,
        ));
        let out = feed(&mut core, &hist, true);
        let titles: Vec<_> = out.hits.iter().map(|h| h.alert.title.as_str()).collect();
        assert_eq!(
            titles,
            ["OI up"],
            "direct rule fires once (cooldown), the spike is up so the down-only pass-through stays quiet: {titles:?}"
        );
    }

    #[test]
    fn symbol_limit_is_enforced() {
        let cfg = AlertsConfig {
            max_symbols: 2,
            ..AlertsConfig::default()
        };
        let set = Arc::new(ArcSwap::from_pointee(RuleSet::default()));
        let mut core = Core::new(&cfg, set, Arc::new(EngineStats::default()));
        for (i, s) in ["A-USDT-PERP", "B-USDT-PERP", "C-USDT-PERP"]
            .iter()
            .enumerate()
        {
            feed(
                &mut core,
                &[oi("binance", s, 1000 + i as i64, 1.0, 1.0)],
                true,
            );
        }
        assert_eq!(core.symbol_count(), 2);
        assert_eq!(core.stats().symbols_rejected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn replayed_liquidations_are_not_counted_twice() {
        let (mut core, _) = core_with(vec![]);
        core.set_replay_watermark(5_000);
        let out = feed(
            &mut core,
            &[liq_event("binance", "BTC-USDT-PERP", 4_000, 9e9)],
            true,
        );
        assert!(out.signals.is_empty());
        assert_eq!(core.cascade().bucket_count(), 0);
    }
}
