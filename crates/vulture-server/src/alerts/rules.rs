//! Compiled alert rules, their matching, cooldowns and the messages they produce.

use std::collections::{HashMap, HashSet};

use vulture_proto::v1::{Liquidation, Side, Signal, SignalType};

use super::model::{
    CmpOp, CondMetric, Condition, LiqSide, QuietRange, RuleDef, RuleKind, Severity, SignalKind,
};
use super::oi::OiOverride;
use super::series::Change;
use super::text::{Alert, fmt_num, fmt_pct, fmt_usd, fmt_window};

/// An enabled rule, ready to match.
#[derive(Debug, Clone)]
pub struct CompiledRule {
    /// Rule id.
    pub id: String,
    /// Rule name (falls back to the id).
    pub name: String,
    /// What it looks for.
    pub kind: RuleKind,
    exchanges: HashSet<String>,
    symbols: HashSet<String>,
    /// Channels that receive the alerts.
    pub channels: Vec<String>,
    /// Cooldown, ms.
    pub cooldown_ms: i64,
    /// UTC ranges of the day in which the rule is silent.
    pub quiet: Vec<QuietRange>,
}

impl CompiledRule {
    /// Whether the rule is silent at `ts_ms`.
    pub fn is_quiet(&self, ts_ms: i64) -> bool {
        super::model::in_quiet(&self.quiet, ts_ms)
    }

    /// True when the exchange and the symbol pass the rule's filters.
    pub fn matches_scope(&self, exchange: &str, symbol: &str) -> bool {
        (self.exchanges.is_empty() || self.exchanges.contains(exchange))
            && (self.symbols.is_empty() || self.symbols.contains(symbol))
    }
}

/// The rules in force, swapped atomically when the owner changes them.
#[derive(Debug, Default, Clone)]
pub struct RuleSet {
    /// Rules that send messages.
    pub rules: Vec<CompiledRule>,
    /// Threshold overrides for the OI spike detector.
    pub overrides: Vec<OiOverride>,
}

impl RuleSet {
    /// Compiles the enabled rules of `defs`.
    pub fn compile(defs: &[RuleDef]) -> Self {
        let mut set = RuleSet::default();
        for d in defs.iter().filter(|d| d.enabled) {
            if let RuleKind::OiChange {
                window_secs,
                min_change_pct,
                min_change_usd,
                detector_override: true,
            } = &d.kind
            {
                set.overrides.push(OiOverride {
                    window_secs: *window_secs,
                    threshold_pct: *min_change_pct,
                    min_change_usd: *min_change_usd,
                    exchanges: d.exchanges.clone(),
                    symbols: d.symbols.clone(),
                });
                continue;
            }
            set.rules.push(CompiledRule {
                id: d.id.clone(),
                name: if d.name.is_empty() {
                    d.id.clone()
                } else {
                    d.name.clone()
                },
                kind: d.kind.clone(),
                exchanges: d.exchanges.iter().cloned().collect(),
                symbols: d.symbols.iter().cloned().collect(),
                channels: d.channels.clone(),
                cooldown_ms: i64::from(d.cooldown_secs) * 1000,
                quiet: d.quiet_hours.clone(),
            });
        }
        set
    }
}

/// A rule matched: the alert goes to these channels.
#[derive(Debug, Clone)]
pub struct Hit {
    /// Rule id.
    pub rule_id: String,
    /// Channels.
    pub channels: Vec<String>,
    /// The message.
    pub alert: Alert,
}

/// Last time each (rule, key) alerted.
#[derive(Debug, Default)]
pub struct RuleCooldowns {
    last: HashMap<String, i64>,
}

const MAX_COOLDOWN_ENTRIES: usize = 200_000;

impl RuleCooldowns {
    /// Returns true (and records `now`) when the key is out of its cooldown; false when it is suppressed.
    pub fn allow(&mut self, rule: &str, key: &str, now: i64, cooldown_ms: i64) -> bool {
        if cooldown_ms <= 0 {
            return true;
        }
        let k = format!("{rule}|{key}");
        if let Some(&t) = self.last.get(&k)
            && now - t < cooldown_ms
        {
            return false;
        }
        if self.last.len() >= MAX_COOLDOWN_ENTRIES {
            self.last.retain(|_, t| now - *t < 86_400_000);
        }
        self.last.insert(k, now);
        true
    }

    /// Entries held.
    pub fn len(&self) -> usize {
        self.last.len()
    }

    /// True when empty.
    pub fn is_empty(&self) -> bool {
        self.last.is_empty()
    }
}

/// Does the liquidation match the rule? Returns the notional threshold crossed.
pub fn liquidation_matches(rule: &CompiledRule, liq: &Liquidation) -> bool {
    let RuleKind::Liquidation {
        min_notional_usd,
        sides,
    } = &rule.kind
    else {
        return false;
    };
    if liq.notional_usd < *min_notional_usd {
        return false;
    }
    if !sides.is_empty() {
        let side = match Side::try_from(liq.side) {
            Ok(Side::Long) => LiqSide::Long,
            Ok(Side::Short) => LiqSide::Short,
            _ => return false,
        };
        if !sides.contains(&side) {
            return false;
        }
    }
    rule.matches_scope(&liq.exchange, &liq.symbol)
}

/// The message of a composite rule: the symbol, and each condition with the value that satisfied it.
pub fn composite_alert(
    rule: &CompiledRule,
    exchange: &str,
    symbol: &str,
    conditions: &[Condition],
    values: &[f64],
    severity: Severity,
    ts_ms: i64,
) -> Alert {
    let label = |c: &Condition| -> String {
        match c.metric {
            CondMetric::OiChangePct => format!("OI change {}", fmt_window(c.window_secs)),
            CondMetric::PriceChangePct => format!("Price change {}", fmt_window(c.window_secs)),
            CondMetric::FundingRate => "Funding rate".into(),
            CondMetric::BasisPct => "Basis".into(),
            CondMetric::LiqUsd => format!("Liquidated {}", fmt_window(c.window_secs)),
            CondMetric::Regime => format!("Regime {}", fmt_window(c.window_secs)),
        }
    };
    let show = |c: &Condition, v: f64| -> String {
        match c.metric {
            CondMetric::OiChangePct | CondMetric::PriceChangePct | CondMetric::BasisPct => {
                fmt_pct(v)
            }
            CondMetric::FundingRate => format!("{:+.4}%", v * 100.0),
            CondMetric::LiqUsd => fmt_usd(v),
            CondMetric::Regime => match v as i32 {
                1 => "new longs",
                2 => "new shorts",
                3 => "short covering",
                4 => "long unwinding",
                _ => "?",
            }
            .to_string(),
        }
    };
    let op = |o: CmpOp| match o {
        CmpOp::Gt => ">",
        CmpOp::Ge => ">=",
        CmpOp::Lt => "<",
        CmpOp::Le => "<=",
        CmpOp::Eq => "=",
    };
    let summary: Vec<String> = conditions
        .iter()
        .zip(values)
        .map(|(c, v)| format!("{} {}", label(c).to_lowercase(), show(c, *v)))
        .collect();
    let mut fields: Vec<(String, String)> = conditions
        .iter()
        .zip(values)
        .map(|(c, v)| {
            let threshold = if c.metric == CondMetric::Regime {
                String::new()
            } else {
                format!(" ({} {})", op(c.op), fmt_num(c.threshold))
            };
            (label(c), format!("{}{threshold}", show(c, *v)))
        })
        .collect();
    fields.push(("Rule".into(), rule.name.clone()));
    Alert {
        ts_ms,
        severity,
        title: "Composite rule".into(),
        headline: format!("{symbol} on {exchange}: {}", summary.join(", ")),
        fields,
    }
}

/// The message of a liquidation rule.
pub fn liquidation_alert(rule: &CompiledRule, liq: &Liquidation) -> Alert {
    let (side, verb) = match Side::try_from(liq.side) {
        Ok(Side::Long) => ("LONG", "long liquidated"),
        Ok(Side::Short) => ("SHORT", "short liquidated"),
        _ => ("?", "liquidated"),
    };
    let min = match &rule.kind {
        RuleKind::Liquidation {
            min_notional_usd, ..
        } => *min_notional_usd,
        _ => 0.0,
    };
    let ratio = if min > 0.0 {
        liq.notional_usd / min
    } else {
        1.0
    };
    Alert {
        ts_ms: liq.recv_ts,
        severity: if ratio >= 10.0 {
            Severity::Critical
        } else if ratio >= 3.0 {
            Severity::Warning
        } else {
            Severity::Info
        },
        title: "Liquidation".into(),
        headline: format!(
            "{} on {}: {side} {} at {}",
            liq.symbol,
            liq.exchange,
            fmt_usd(liq.notional_usd),
            fmt_num(liq.price)
        ),
        fields: vec![
            ("Event".into(), verb.into()),
            ("Quantity".into(), fmt_num(liq.qty)),
            ("Rule".into(), rule.name.clone()),
        ],
    }
}

/// The message of a direct OI change rule.
pub fn oi_change_alert(
    rule: &CompiledRule,
    exchange: &str,
    symbol: &str,
    window_secs: u32,
    c: &Change,
) -> Alert {
    let up = c.pct > 0.0;
    let mut fields = vec![
        ("OI change".into(), format!("{} base", fmt_num(c.abs_base))),
        ("OI now".into(), fmt_num(c.now.oi_base)),
    ];
    if c.usd.is_finite() {
        fields.push(("Change USD".into(), fmt_usd(c.usd)));
    }
    if c.now.oi_usd.is_finite() {
        fields.push(("OI USD".into(), fmt_usd(c.now.oi_usd)));
    }
    fields.push(("Rule".into(), rule.name.clone()));
    Alert {
        ts_ms: c.now.ts,
        severity: Severity::Info,
        title: if up { "OI up" } else { "OI down" }.into(),
        headline: format!(
            "{symbol} on {exchange}: {} in {}",
            fmt_pct(c.pct),
            fmt_window(window_secs)
        ),
        fields,
    }
}

/// Severity recorded by the detectors in `details["severity"]`.
pub fn signal_severity(s: &Signal) -> Severity {
    s.details
        .get("severity")
        .and_then(|v| Severity::parse(v))
        .unwrap_or_default()
}

/// Does the signal match a pass-through rule?
pub fn signal_matches(rule: &CompiledRule, s: &Signal) -> bool {
    let RuleKind::Signal {
        types,
        min_severity,
    } = &rule.kind
    else {
        return false;
    };
    if !types.is_empty() {
        let kind = match SignalType::try_from(s.r#type) {
            Ok(SignalType::OiSpikeUp) => SignalKind::OiSpikeUp,
            Ok(SignalType::OiSpikeDown) => SignalKind::OiSpikeDown,
            Ok(SignalType::LiqCascade) => SignalKind::LiqCascade,
            _ => return false,
        };
        if !types.contains(&kind) {
            return false;
        }
    }
    if signal_severity(s) < *min_severity {
        return false;
    }
    // Cascades are not tied to a symbol: only the exchange filter applies (a rule with a symbol filter does
    // not match them).
    if s.symbol.is_empty() {
        rule.symbols.is_empty()
            && (rule.exchanges.is_empty() || rule.exchanges.contains(&s.exchange))
    } else {
        rule.matches_scope(&s.exchange, &s.symbol)
    }
}

/// Cooldown key of a signal: includes the cascade phase so that a closing summary is never swallowed by the
/// cooldown of the start.
pub fn signal_cooldown_key(s: &Signal) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        s.r#type,
        s.exchange,
        s.symbol,
        s.window_secs,
        s.details.get("phase").map_or("", String::as_str)
    )
}

fn detail<'a>(s: &'a Signal, key: &str) -> &'a str {
    s.details.get(key).map_or("", String::as_str)
}

fn num_detail(s: &Signal, key: &str) -> Option<f64> {
    s.details.get(key).and_then(|v| v.parse().ok())
}

/// The message of a signal.
pub fn signal_alert(s: &Signal) -> Alert {
    let severity = signal_severity(s);
    match SignalType::try_from(s.r#type) {
        Ok(t @ (SignalType::OiSpikeUp | SignalType::OiSpikeDown)) => {
            let up = t == SignalType::OiSpikeUp;
            let mut fields = vec![
                (
                    "OI change".into(),
                    format!("{} base", fmt_num(s.change_abs)),
                ),
                ("Windows".into(), detail(s, "windows").to_string()),
            ];
            if let Some(v) = num_detail(s, "change_usd") {
                fields.push(("Change USD".into(), fmt_usd(v)));
            }
            if let Some(v) = num_detail(s, "oi_usd") {
                fields.push(("OI USD".into(), fmt_usd(v)));
            }
            if let Some(v) = num_detail(s, "mark_price") {
                fields.push(("Mark".into(), fmt_num(v)));
            }
            if !detail(s, "z").is_empty() {
                fields.push(("z-score".into(), detail(s, "z").to_string()));
            }
            Alert {
                ts_ms: s.ts,
                severity,
                title: if up { "OI spike UP" } else { "OI spike DOWN" }.into(),
                headline: format!(
                    "{} on {}: {} in {}",
                    s.symbol,
                    s.exchange,
                    fmt_pct(s.change_pct),
                    fmt_window(s.window_secs)
                ),
                fields,
            }
        }
        _ => {
            let scope = if s.exchange == "*" {
                "market-wide".to_string()
            } else {
                s.exchange.clone()
            };
            let top = detail(s, "top_symbols").to_string();
            if detail(s, "phase") == "end" {
                let secs = num_detail(s, "duration_secs").unwrap_or(0.0) as u64;
                let mut fields = vec![
                    (
                        "Peak".into(),
                        format!(
                            "{}/min",
                            fmt_usd(num_detail(s, "peak_usd_per_min").unwrap_or(0.0))
                        ),
                    ),
                    (
                        "Long / short".into(),
                        format!(
                            "{} / {}",
                            fmt_usd(num_detail(s, "long_usd").unwrap_or(0.0)),
                            fmt_usd(num_detail(s, "short_usd").unwrap_or(0.0))
                        ),
                    ),
                    ("Top".into(), top),
                ];
                if !detail(s, "price_symbol").is_empty() {
                    fields.push((
                        format!("{} price", detail(s, "price_symbol")),
                        format!(
                            "{} \u{2013} {}",
                            fmt_num(num_detail(s, "price_low").unwrap_or(0.0)),
                            fmt_num(num_detail(s, "price_high").unwrap_or(0.0))
                        ),
                    ));
                }
                Alert {
                    ts_ms: s.ts,
                    severity,
                    title: "Liquidation cascade ended".into(),
                    headline: format!(
                        "{scope}: {} in {}m{:02}s, {} liquidations",
                        fmt_usd(s.change_abs),
                        secs / 60,
                        secs % 60,
                        detail(s, "count")
                    ),
                    fields,
                }
            } else {
                Alert {
                    ts_ms: s.ts,
                    severity,
                    title: "Liquidation cascade".into(),
                    headline: format!(
                        "{scope}: {} in the last minute, {} liquidations",
                        fmt_usd(s.change_abs),
                        detail(s, "count")
                    ),
                    fields: vec![
                        (
                            "Long / short".into(),
                            format!(
                                "{} / {}",
                                fmt_usd(num_detail(s, "long_usd").unwrap_or(0.0)),
                                fmt_usd(num_detail(s, "short_usd").unwrap_or(0.0))
                            ),
                        ),
                        ("Top".into(), top),
                        (
                            "Trigger".into(),
                            fmt_usd(num_detail(s, "trigger_usd").unwrap_or(0.0)),
                        ),
                    ],
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(kind: RuleKind) -> RuleDef {
        RuleDef {
            id: "r1".into(),
            name: "Rule one".into(),
            enabled: true,
            kind,
            exchanges: vec![],
            symbols: vec![],
            channels: vec!["tg".into()],
            cooldown_secs: 60,
            quiet_hours: Vec::new(),
        }
    }

    fn liq(exchange: &str, symbol: &str, usd: f64, side: Side) -> Liquidation {
        Liquidation {
            exchange: exchange.into(),
            symbol_raw: String::new(),
            symbol: symbol.into(),
            exchange_ts: 1,
            recv_ts: 2,
            side: side as i32,
            price: 100.0,
            qty: usd / 100.0,
            notional_usd: usd,
        }
    }

    #[test]
    fn disabled_rules_are_not_compiled_and_overrides_are_split_out() {
        let mut off = def(RuleKind::Liquidation {
            min_notional_usd: 1.0,
            sides: vec![],
        });
        off.enabled = false;
        let ov = RuleDef {
            channels: vec![],
            ..def(RuleKind::OiChange {
                window_secs: 300,
                min_change_pct: 2.0,
                min_change_usd: 0.0,
                detector_override: true,
            })
        };
        let set = RuleSet::compile(&[off, ov]);
        assert!(set.rules.is_empty());
        assert_eq!(set.overrides.len(), 1);
    }

    #[test]
    fn liquidation_filters() {
        let mut d = def(RuleKind::Liquidation {
            min_notional_usd: 100_000.0,
            sides: vec![LiqSide::Long],
        });
        d.exchanges = vec!["binance".into()];
        d.symbols = vec!["BTC-USDT-PERP".into()];
        let rule = RuleSet::compile(&[d]).rules.remove(0);
        assert!(liquidation_matches(
            &rule,
            &liq("binance", "BTC-USDT-PERP", 150_000.0, Side::Long)
        ));
        assert!(!liquidation_matches(
            &rule,
            &liq("binance", "BTC-USDT-PERP", 99_999.0, Side::Long)
        ));
        assert!(!liquidation_matches(
            &rule,
            &liq("binance", "BTC-USDT-PERP", 150_000.0, Side::Short)
        ));
        assert!(!liquidation_matches(
            &rule,
            &liq("okx", "BTC-USDT-PERP", 150_000.0, Side::Long)
        ));
        assert!(!liquidation_matches(
            &rule,
            &liq("binance", "ETH-USDT-PERP", 150_000.0, Side::Long)
        ));
        let a = liquidation_alert(
            &rule,
            &liq("binance", "BTC-USDT-PERP", 150_000.0, Side::Long),
        );
        assert!(a.headline.contains("LONG $150K"));
    }

    #[test]
    fn cooldown_allows_after_expiry() {
        let mut c = RuleCooldowns::default();
        assert!(c.allow("r", "k", 0, 60_000));
        assert!(!c.allow("r", "k", 59_999, 60_000));
        assert!(c.allow("r", "other", 1, 60_000));
        assert!(c.allow("r", "k", 60_000, 60_000));
        assert!(c.allow("r", "k", 60_001, 0));
        assert_eq!(c.len(), 2);
    }

    fn sig(kind: SignalType, severity: &str, symbol: &str, phase: &str) -> Signal {
        Signal {
            r#type: kind as i32,
            exchange: "binance".into(),
            symbol: symbol.into(),
            window_secs: 300,
            change_pct: 4.2,
            change_abs: 100.0,
            threshold: 3.0,
            ts: 1_761_400_992_000,
            details: [
                ("severity".to_string(), severity.to_string()),
                ("phase".to_string(), phase.to_string()),
            ]
            .into_iter()
            .collect(),
        }
    }

    #[test]
    fn signal_rule_filters_by_type_and_severity() {
        let d = def(RuleKind::Signal {
            types: vec![SignalKind::OiSpikeUp],
            min_severity: Severity::Warning,
        });
        let rule = RuleSet::compile(&[d]).rules.remove(0);
        assert!(signal_matches(
            &rule,
            &sig(SignalType::OiSpikeUp, "warning", "BTC-USDT-PERP", "")
        ));
        assert!(signal_matches(
            &rule,
            &sig(SignalType::OiSpikeUp, "critical", "BTC-USDT-PERP", "")
        ));
        assert!(!signal_matches(
            &rule,
            &sig(SignalType::OiSpikeUp, "info", "BTC-USDT-PERP", "")
        ));
        assert!(!signal_matches(
            &rule,
            &sig(SignalType::OiSpikeDown, "critical", "BTC-USDT-PERP", "")
        ));
    }

    #[test]
    fn cascade_phases_have_different_cooldown_keys() {
        let a = sig(SignalType::LiqCascade, "warning", "", "start");
        let b = sig(SignalType::LiqCascade, "warning", "", "end");
        assert_ne!(signal_cooldown_key(&a), signal_cooldown_key(&b));
    }

    #[test]
    fn alerts_read_well() {
        let a = signal_alert(&sig(SignalType::OiSpikeUp, "warning", "BTC-USDT-PERP", ""));
        assert_eq!(a.title, "OI spike UP");
        assert_eq!(a.headline, "BTC-USDT-PERP on binance: +4.20% in 5m");
        assert_eq!(a.severity, Severity::Warning);
    }
}
