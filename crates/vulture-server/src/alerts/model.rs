//! Data model of the owner's alert rules and delivery channels, and its validation.
//!
//! The same types are used in three places: `[[alerts.rules]]` / `[[alerts.channels]]` of the configuration
//! (defaults that seed `alerts.json` while that file does not exist), the file itself, and the `Control` RPCs.

use std::collections::BTreeSet;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::config::Secret;

/// Largest number of rules and channels a server accepts.
pub const MAX_RULES: usize = 1000;
/// Largest number of channels.
pub const MAX_CHANNELS: usize = 50;
/// Largest list of symbols in a rule filter.
pub const MAX_FILTER_SYMBOLS: usize = 2000;
/// Longest accepted cooldown, seconds.
pub const MAX_COOLDOWN_SECS: u32 = 86_400;
/// Shortest and longest window of a direct OI rule, seconds.
pub const MIN_WINDOW_SECS: u32 = 30;
/// Longest window of a direct OI rule (the engine keeps four hours of samples).
pub const MAX_WINDOW_SECS: u32 = 14_400;

/// Severity of a signal or an alert. Ordered: `Info < Warning < Critical`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Worth knowing.
    #[default]
    Info,
    /// Far above the threshold.
    Warning,
    /// Extreme.
    Critical,
}

impl Severity {
    /// Lowercase name, as written in `Signal.details["severity"]`.
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Critical => "critical",
        }
    }

    /// Parses [`Severity::as_str`].
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "info" => Some(Severity::Info),
            "warning" => Some(Severity::Warning),
            "critical" => Some(Severity::Critical),
            _ => None,
        }
    }
}

/// Signal types a pass-through rule can select.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
    /// `OI_SPIKE_UP`.
    OiSpikeUp,
    /// `OI_SPIKE_DOWN`.
    OiSpikeDown,
    /// `LIQ_CASCADE`.
    LiqCascade,
}

/// Liquidated side filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiqSide {
    /// Long positions.
    Long,
    /// Short positions.
    Short,
}

/// A range of the day in UTC during which a rule or a channel stays silent. Minutes since 00:00 UTC; `end` is
/// exclusive; a range with `start > end` wraps midnight (`start 1320, end 360` is 22:00 to 06:00).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuietRange {
    /// First quiet minute of the day, 0 to 1439.
    pub start_minute: u16,
    /// First minute after the range, 0 to 1440.
    pub end_minute: u16,
}

impl QuietRange {
    /// Whether minute-of-day `m` is inside.
    pub fn contains(&self, m: u16) -> bool {
        if self.start_minute < self.end_minute {
            m >= self.start_minute && m < self.end_minute
        } else {
            m >= self.start_minute || m < self.end_minute
        }
    }
}

/// Whether the time (ms since the epoch, UTC) falls in any of the ranges.
pub fn in_quiet(ranges: &[QuietRange], ts_ms: i64) -> bool {
    if ranges.is_empty() {
        return false;
    }
    let minute = (ts_ms.rem_euclid(86_400_000) / 60_000) as u16;
    ranges.iter().any(|r| r.contains(minute))
}

/// Most quiet ranges per rule or channel.
pub const MAX_QUIET_RANGES: usize = 8;

/// Checks and normalizes quiet ranges (sorted, no duplicates).
pub fn validate_quiet(ranges: &mut Vec<QuietRange>) -> Result<(), ModelError> {
    if ranges.len() > MAX_QUIET_RANGES {
        return bad(format!("more than {MAX_QUIET_RANGES} quiet ranges"));
    }
    for r in ranges.iter() {
        if r.start_minute >= 1440 || r.end_minute > 1440 {
            return bad(
                "quiet range minutes must be 0 to 1439 for the start and 0 to 1440 for the end",
            );
        }
        if r.start_minute == r.end_minute {
            return bad("a quiet range must not be empty (start equals end)");
        }
    }
    ranges.sort();
    ranges.dedup();
    Ok(())
}

/// What a condition of a composite rule measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CondMetric {
    /// Change of `oi_base` over `window_secs`, percent (the detector's function).
    OiChangePct,
    /// Change of the mark price over `window_secs`, percent.
    PriceChangePct,
    /// The latest funding rate (a fraction).
    FundingRate,
    /// The latest basis, `100 * (mark - index) / index`, percent.
    BasisPct,
    /// Liquidated USD of the symbol over `window_secs`.
    LiqUsd,
    /// The regime over `window_secs` (compared with `eq` to a number 1 to 4: new longs, new shorts, short
    /// covering, long unwinding).
    Regime,
}

impl CondMetric {
    /// Whether the metric needs a window.
    pub fn windowed(self) -> bool {
        !matches!(self, Self::FundingRate | Self::BasisPct)
    }
}

/// Comparison of a condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CmpOp {
    /// `>`.
    Gt,
    /// `>=`.
    Ge,
    /// `<`.
    Lt,
    /// `<=`.
    Le,
    /// `==` (for the regime).
    Eq,
}

impl CmpOp {
    /// Applies the comparison.
    pub fn holds(self, value: f64, threshold: f64) -> bool {
        match self {
            Self::Gt => value > threshold,
            Self::Ge => value >= threshold,
            Self::Lt => value < threshold,
            Self::Le => value <= threshold,
            Self::Eq => (value - threshold).abs() < 1e-9,
        }
    }
}

/// One condition of a composite rule.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Condition {
    /// What is measured.
    pub metric: CondMetric,
    /// How it is compared.
    pub op: CmpOp,
    /// The threshold, in the unit of the metric.
    pub threshold: f64,
    /// Window, seconds (0 for funding and basis).
    #[serde(default)]
    pub window_secs: u32,
}

/// Most conditions in a composite rule.
pub const MAX_CONDITIONS: usize = 8;

/// What a rule looks for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuleKind {
    /// All conditions hold at once for a symbol (AND). Evaluated on a fixed cadence from the engine state, not
    /// per event.
    Composite {
        /// The conditions.
        conditions: Vec<Condition>,
        /// Severity of the alert.
        #[serde(default)]
        severity: Severity,
    },
    /// A single liquidation of at least `min_notional_usd`.
    Liquidation {
        /// Threshold, USD.
        min_notional_usd: f64,
        /// Sides to match; empty = both.
        #[serde(default)]
        sides: Vec<LiqSide>,
    },
    /// Change of `oi_base` over a window, both directions. With `detector_override` the rule sends nothing and
    /// replaces the thresholds of the OI spike detector for its window and filters.
    OiChange {
        /// Window, seconds.
        window_secs: u32,
        /// Threshold on |change|, percent.
        min_change_pct: f64,
        /// Floor on |change| in USD (change of `oi_base` times the mark price); 0 = none.
        #[serde(default)]
        min_change_usd: f64,
        /// Override the detector's thresholds instead of alerting.
        #[serde(default)]
        detector_override: bool,
    },
    /// Forwards the server's own signals.
    Signal {
        /// Types to forward; empty = all.
        #[serde(default)]
        types: Vec<SignalKind>,
        /// Minimum severity.
        #[serde(default)]
        min_severity: Severity,
    },
}

fn default_true() -> bool {
    true
}

/// One alert rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleDef {
    /// Identifier: `[a-z0-9_-]{1,64}`. Empty in a request to create a rule: the server assigns one.
    #[serde(default)]
    pub id: String,
    /// Human-readable name.
    #[serde(default)]
    pub name: String,
    /// Disabled rules are kept but not evaluated.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// What the rule looks for.
    pub kind: RuleKind,
    /// Exchange filter; empty = all.
    #[serde(default)]
    pub exchanges: Vec<String>,
    /// Normalized symbol filter; empty = all.
    #[serde(default)]
    pub symbols: Vec<String>,
    /// Channels that receive the alerts.
    #[serde(default)]
    pub channels: Vec<String>,
    /// Minimum seconds between two alerts of the rule for the same exchange and symbol.
    #[serde(default)]
    pub cooldown_secs: u32,
    /// UTC ranges of the day in which the rule stays silent (no alert, no cooldown consumed).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub quiet_hours: Vec<QuietRange>,
}

/// Kind of a delivery channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelKind {
    /// Telegram Bot API.
    Telegram,
    /// Discord webhook.
    Discord,
}

impl ChannelKind {
    /// Lowercase name for logs and metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            ChannelKind::Telegram => "telegram",
            ChannelKind::Discord => "discord",
        }
    }
}

fn ser_secret<S: Serializer>(v: &Option<Secret>, s: S) -> Result<S::Ok, S::Error> {
    match v {
        Some(secret) => s.serialize_some(secret.expose()),
        None => s.serialize_none(),
    }
}

fn de_secret<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Secret>, D::Error> {
    Ok(Option::<String>::deserialize(d)?.map(Secret::new))
}

/// One delivery channel. The secret (bot token or webhook URL) comes from exactly one of `secret` (stored in
/// `alerts.json`), `secret_file` or `secret_env`.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelDef {
    /// Identifier: `[a-z0-9_-]{1,64}`. Empty in a request to create a channel: the server assigns one.
    #[serde(default)]
    pub id: String,
    /// Human-readable name.
    #[serde(default)]
    pub name: String,
    /// Telegram or Discord.
    pub kind: ChannelKind,
    /// Disabled channels drop what is sent to them.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Reserved for system alerts (V4-5); stored and returned, not used yet.
    #[serde(default)]
    pub system: bool,
    /// Log instead of sending.
    #[serde(default)]
    pub dry_run: bool,
    /// Telegram chat id or `@channelname`.
    #[serde(default)]
    pub chat_id: String,
    /// Inline secret (bot token or webhook URL).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "ser_secret",
        deserialize_with = "de_secret"
    )]
    pub secret: Option<Secret>,
    /// File holding the secret.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret_file: String,
    /// Environment variable holding the secret.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret_env: String,
    /// Minimum milliseconds between two messages; 0 = the default of the kind.
    #[serde(default)]
    pub min_interval_ms: u32,
    /// Messages produced within this window are merged into a digest; 0 = the default.
    #[serde(default)]
    pub batch_window_ms: u32,
    /// Queue length; 0 = the default.
    #[serde(default)]
    pub queue_limit: u32,
    /// UTC ranges of the day in which the channel receives no alerts (they are dropped, not delayed). The daily
    /// digest is not affected.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub quiet_hours: Vec<QuietRange>,
}

impl std::fmt::Debug for ChannelDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelDef")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("enabled", &self.enabled)
            .field("dry_run", &self.dry_run)
            .field("has_secret", &self.has_secret())
            .finish_non_exhaustive()
    }
}

impl ChannelDef {
    /// True when some source of the secret is configured.
    pub fn has_secret(&self) -> bool {
        self.secret.is_some() || !self.secret_file.is_empty() || !self.secret_env.is_empty()
    }

    /// Resolves the secret value. The error never contains the value.
    pub fn resolve_secret(&self) -> Result<Secret, String> {
        if let Some(s) = &self.secret {
            return Ok(s.clone());
        }
        if !self.secret_file.is_empty() {
            return Secret::from_file(std::path::Path::new(&self.secret_file))
                .map_err(|_| "the secret file cannot be read".to_string())
                .and_then(|s| {
                    if s.expose().is_empty() {
                        Err("the secret file is empty".into())
                    } else {
                        Ok(s)
                    }
                });
        }
        if !self.secret_env.is_empty() {
            return match std::env::var(&self.secret_env) {
                Ok(v) if !v.trim().is_empty() => Ok(Secret::new(v.trim())),
                _ => Err("the secret environment variable is not set".into()),
            };
        }
        Err("the channel has no secret".into())
    }

    /// A mask such as `****abcd`, or `""` when there is no secret and `(unavailable)` when it cannot be read.
    pub fn secret_mask(&self) -> String {
        if !self.has_secret() {
            return String::new();
        }
        match self.resolve_secret() {
            Ok(s) => mask(s.expose()),
            Err(_) => "(unavailable)".into(),
        }
    }
}

/// `****` plus the last four characters; a short secret is fully hidden.
pub fn mask(secret: &str) -> String {
    let chars: Vec<char> = secret.chars().collect();
    if chars.len() < 12 {
        "****".into()
    } else {
        format!(
            "****{}",
            chars[chars.len() - 4..].iter().collect::<String>()
        )
    }
}

/// Why a rule or a channel is not acceptable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ModelError(pub String);

fn bad<T>(msg: impl Into<String>) -> Result<T, ModelError> {
    Err(ModelError(msg.into()))
}

/// `[a-z0-9_-]{1,64}`.
pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

fn valid_symbol(s: &str) -> bool {
    s.ends_with("-PERP")
        && s.matches('-').count() == 2
        && s.len() <= 40
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

impl RuleDef {
    /// Checks the rule and normalizes it (symbols upper-cased, duplicates removed, lists sorted).
    pub fn validate(&mut self) -> Result<(), ModelError> {
        if !self.id.is_empty() && !valid_id(&self.id) {
            return bad("rule id must be 1-64 characters of a-z, 0-9, '_' or '-'");
        }
        if self.name.chars().count() > 100 {
            return bad("rule name is longer than 100 characters");
        }
        self.name = self.name.trim().to_string();
        self.exchanges = self
            .exchanges
            .iter()
            .map(|e| e.trim().to_string())
            .collect();
        for e in &self.exchanges {
            let ok = !e.is_empty()
                && e.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
            if !ok {
                return bad(format!(
                    "exchange {e:?} must be lowercase letters, digits or '_'"
                ));
            }
        }
        if self.symbols.len() > MAX_FILTER_SYMBOLS {
            return bad(format!(
                "more than {MAX_FILTER_SYMBOLS} symbols in a filter"
            ));
        }
        self.symbols = self
            .symbols
            .iter()
            .map(|s| s.trim().to_uppercase())
            .collect();
        for s in &self.symbols {
            if !valid_symbol(s) {
                return bad(format!(
                    "symbol {s:?} is not a normalized symbol (BASE-QUOTE-PERP)"
                ));
            }
        }
        sort_dedup(&mut self.exchanges);
        sort_dedup(&mut self.symbols);
        sort_dedup(&mut self.channels);
        for c in &self.channels {
            if !valid_id(c) {
                return bad(format!("channel id {c:?} is malformed"));
            }
        }
        if self.cooldown_secs > MAX_COOLDOWN_SECS {
            return bad(format!("cooldown_secs is above {MAX_COOLDOWN_SECS}"));
        }
        validate_quiet(&mut self.quiet_hours)?;
        let positive = |v: f64| v.is_finite() && v > 0.0;
        match &mut self.kind {
            RuleKind::Composite { conditions, .. } => {
                if conditions.is_empty() || conditions.len() > MAX_CONDITIONS {
                    return bad(format!(
                        "a composite rule needs 1 to {MAX_CONDITIONS} conditions"
                    ));
                }
                for (i, c) in conditions.iter().enumerate() {
                    let n = i + 1;
                    if !c.threshold.is_finite() {
                        return bad(format!("condition {n}: threshold must be a finite number"));
                    }
                    if c.metric.windowed() {
                        if !(MIN_WINDOW_SECS..=MAX_WINDOW_SECS).contains(&c.window_secs) {
                            return bad(format!(
                                "condition {n}: window_secs must be between {MIN_WINDOW_SECS} and {MAX_WINDOW_SECS}"
                            ));
                        }
                    } else if c.window_secs != 0 {
                        return bad(format!(
                            "condition {n}: funding_rate and basis_pct take no window"
                        ));
                    }
                    if c.metric == CondMetric::Regime {
                        if c.op != CmpOp::Eq || ![1.0, 2.0, 3.0, 4.0].contains(&c.threshold) {
                            return bad(format!(
                                "condition {n}: a regime condition is `eq` to 1 (new longs), 2 (new shorts), 3 (short covering) or 4 (long unwinding)"
                            ));
                        }
                    } else if c.op == CmpOp::Eq {
                        return bad(format!("condition {n}: `eq` is only for the regime"));
                    }
                    if c.metric == CondMetric::LiqUsd && c.threshold < 0.0 {
                        return bad(format!(
                            "condition {n}: liq_usd threshold must not be negative"
                        ));
                    }
                }
            }
            RuleKind::Liquidation {
                min_notional_usd,
                sides,
            } => {
                if !positive(*min_notional_usd) {
                    return bad("min_notional_usd must be a positive number");
                }
                sides.sort();
                sides.dedup();
            }
            RuleKind::OiChange {
                window_secs,
                min_change_pct,
                min_change_usd,
                detector_override,
            } => {
                if !(MIN_WINDOW_SECS..=MAX_WINDOW_SECS).contains(window_secs) {
                    return bad(format!(
                        "window_secs must be between {MIN_WINDOW_SECS} and {MAX_WINDOW_SECS}"
                    ));
                }
                if !positive(*min_change_pct) || *min_change_pct > 1000.0 {
                    return bad("min_change_pct must be a positive number up to 1000");
                }
                if !min_change_usd.is_finite() || *min_change_usd < 0.0 {
                    return bad("min_change_usd must not be negative");
                }
                if *detector_override && !self.channels.is_empty() {
                    return bad("a detector override sends no messages: leave channels empty");
                }
            }
            RuleKind::Signal { types, .. } => {
                types.sort();
                types.dedup();
            }
        }
        if !matches!(
            self.kind,
            RuleKind::OiChange {
                detector_override: true,
                ..
            }
        ) && self.channels.is_empty()
        {
            return bad("a rule needs at least one channel");
        }
        Ok(())
    }
}

fn sort_dedup(v: &mut Vec<String>) {
    let set: BTreeSet<String> = std::mem::take(v).into_iter().collect();
    *v = set.into_iter().collect();
}

impl ChannelDef {
    /// Checks the channel; `allow_custom_endpoints` accepts a Discord webhook outside discord.com (tests).
    pub fn validate(&mut self, allow_custom_endpoints: bool) -> Result<(), ModelError> {
        if !self.id.is_empty() && !valid_id(&self.id) {
            return bad("channel id must be 1-64 characters of a-z, 0-9, '_' or '-'");
        }
        if self.name.chars().count() > 100 {
            return bad("channel name is longer than 100 characters");
        }
        self.name = self.name.trim().to_string();
        validate_quiet(&mut self.quiet_hours)?;
        self.chat_id = self.chat_id.trim().to_string();
        let sources = usize::from(self.secret.is_some())
            + usize::from(!self.secret_file.is_empty())
            + usize::from(!self.secret_env.is_empty());
        if sources > 1 {
            return bad("set only one of secret, secret_file and secret_env");
        }
        if self
            .secret_env
            .bytes()
            .any(|b| !(b.is_ascii_alphanumeric() || b == b'_'))
        {
            return bad("secret_env must be an environment variable name");
        }
        if self.queue_limit > 100_000 {
            return bad("queue_limit is above 100000");
        }
        if self.min_interval_ms > 3_600_000 || self.batch_window_ms > 600_000 {
            return bad("min_interval_ms or batch_window_ms is out of range");
        }
        match self.kind {
            ChannelKind::Telegram => {
                if self.chat_id.is_empty() {
                    return bad("a Telegram channel needs chat_id");
                }
                if self.chat_id.len() > 64
                    || !self
                        .chat_id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'@' || b == b'_')
                {
                    return bad("chat_id must be a number or @channelname");
                }
            }
            ChannelKind::Discord => {
                if !self.chat_id.is_empty() {
                    return bad("chat_id is for Telegram channels only");
                }
            }
        }
        if let Some(secret) = &self.secret {
            check_secret_shape(self.kind, secret.expose(), allow_custom_endpoints)?;
        }
        Ok(())
    }
}

/// Shape checks of an inline secret; the message never contains the value.
pub fn check_secret_shape(
    kind: ChannelKind,
    secret: &str,
    allow_custom_endpoints: bool,
) -> Result<(), ModelError> {
    match kind {
        ChannelKind::Telegram => {
            let ok = !secret.is_empty()
                && secret.len() <= 128
                && secret
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b':' || b == b'_' || b == b'-');
            if !ok {
                return bad(
                    "the Telegram bot token has an unexpected shape (expected 123456:ABC...)",
                );
            }
        }
        ChannelKind::Discord => {
            let Ok(url) = reqwest::Url::parse(secret) else {
                return bad("the Discord webhook must be a URL");
            };
            let host_ok = matches!(
                url.host_str(),
                Some("discord.com" | "discordapp.com" | "ptb.discord.com" | "canary.discord.com")
            );
            let official =
                url.scheme() == "https" && host_ok && url.path().starts_with("/api/webhooks/");
            let custom = allow_custom_endpoints && matches!(url.scheme(), "http" | "https");
            if !(official || custom) {
                return bad(
                    "the Discord webhook must be https://discord.com/api/webhooks/<id>/<token>",
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn liq_rule() -> RuleDef {
        RuleDef {
            id: "big".into(),
            name: " Big ".into(),
            enabled: true,
            kind: RuleKind::Liquidation {
                min_notional_usd: 1e6,
                sides: vec![LiqSide::Short, LiqSide::Long, LiqSide::Short],
            },
            exchanges: vec!["okx".into(), "binance".into(), "okx".into()],
            symbols: vec!["btc-usdt-perp".into()],
            channels: vec!["tg".into()],
            cooldown_secs: 30,
            quiet_hours: Vec::new(),
        }
    }

    #[test]
    fn rule_is_normalized() {
        let mut r = liq_rule();
        r.validate().unwrap();
        assert_eq!(r.name, "Big");
        assert_eq!(r.exchanges, ["binance", "okx"]);
        assert_eq!(r.symbols, ["BTC-USDT-PERP"]);
        assert!(matches!(&r.kind, RuleKind::Liquidation { sides, .. } if sides.len() == 2));
    }

    #[test]
    fn bad_rules_are_refused() {
        for mutate in [
            (|r: &mut RuleDef| r.id = "Bad Id".into()) as fn(&mut RuleDef),
            |r| r.channels.clear(),
            |r| r.symbols = vec!["BTCUSDT".into()],
            |r| r.exchanges = vec!["Binance".into()],
            |r| r.cooldown_secs = 10_000_000,
            |r| {
                r.kind = RuleKind::Liquidation {
                    min_notional_usd: f64::NAN,
                    sides: vec![],
                }
            },
            |r| {
                r.kind = RuleKind::OiChange {
                    window_secs: 5,
                    min_change_pct: 3.0,
                    min_change_usd: 0.0,
                    detector_override: false,
                }
            },
        ] {
            let mut r = liq_rule();
            mutate(&mut r);
            assert!(r.validate().is_err(), "{r:?}");
        }
    }

    #[test]
    fn override_rule_has_no_channels() {
        let mut r = liq_rule();
        r.channels.clear();
        r.kind = RuleKind::OiChange {
            window_secs: 300,
            min_change_pct: 2.0,
            min_change_usd: 0.0,
            detector_override: true,
        };
        r.validate().unwrap();
        r.channels = vec!["tg".into()];
        assert!(r.validate().is_err());
    }

    fn tg() -> ChannelDef {
        ChannelDef {
            id: "tg".into(),
            name: "t".into(),
            kind: ChannelKind::Telegram,
            enabled: true,
            system: false,
            dry_run: false,
            chat_id: "-1001234".into(),
            secret: Some(Secret::new("123456:ABCdef_ghi-JKL")),
            secret_file: String::new(),
            secret_env: String::new(),
            min_interval_ms: 0,
            batch_window_ms: 0,
            queue_limit: 0,
            quiet_hours: Vec::new(),
        }
    }

    #[test]
    fn channel_validation_and_mask() {
        let mut c = tg();
        c.validate(false).unwrap();
        assert_eq!(c.secret_mask(), "****-JKL");
        assert!(!format!("{c:?}").contains("ABCdef"));
        c.chat_id.clear();
        assert!(c.validate(false).is_err());
        let mut c = tg();
        c.secret = Some(Secret::new("a/b"));
        let err = c.validate(false).unwrap_err().to_string();
        assert!(!err.contains("a/b"));
        let mut c = tg();
        c.secret_env = "X_Y".into();
        assert!(c.validate(false).is_err(), "two secret sources");
    }

    #[test]
    fn discord_webhook_must_be_official_unless_allowed() {
        let mut c = tg();
        c.kind = ChannelKind::Discord;
        c.chat_id.clear();
        c.secret = Some(Secret::new("http://127.0.0.1:9/hook"));
        assert!(c.validate(false).is_err());
        assert!(c.validate(true).is_ok());
        c.secret = Some(Secret::new("https://discord.com/api/webhooks/1/abcdefgh"));
        assert!(c.validate(false).is_ok());
        c.secret = Some(Secret::new("https://evil.example/api/webhooks/1/abc"));
        assert!(c.validate(false).is_err());
    }

    #[test]
    fn mask_hides_short_secrets() {
        assert_eq!(mask("short"), "****");
        assert_eq!(mask("0123456789abcdef"), "****cdef");
    }
}
