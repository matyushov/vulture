//! `[alerts]`: the analytics engine, its detectors and the delivery of messages.

use serde::Deserialize;

use crate::alerts::model::{ChannelDef, RuleDef};

/// `[alerts]` section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AlertsConfig {
    /// Runs the analytics engine (signals in the stream, rules, delivery). The engine is in-process and never
    /// blocks ingestion.
    pub enabled: bool,
    /// How far back the Redis stream is replayed at start to rebuild the detectors' state, seconds. At least the
    /// longest OI window plus its tolerance; the liquidation baseline grows with the data replayed (see
    /// `liq_cascade.baseline_secs`).
    pub warmup_secs: u64,
    /// Events the engine may hold in its input queue. A full queue drops the engine's own input, never the
    /// writer's.
    pub input_queue: usize,
    /// Most symbols tracked by the engine (all exchanges together); further ones are ignored and counted.
    pub max_symbols: usize,
    /// Hard cap of OI samples kept per symbol.
    pub series_max_samples: usize,
    /// Samples closer than this to the previous one are ignored, seconds.
    pub sample_spacing_secs: u64,
    /// How often composite rules are evaluated, seconds. Each pass looks at every symbol with a recent sample
    /// against every composite rule in scope (a few microseconds per pair); 10 s keeps that negligible.
    pub composite_eval_secs: u64,
    /// A pause longer than this between two samples of a symbol is a hole: windows spanning it are skipped,
    /// seconds. Without it a symbol that was untracked for a while would show a false jump when it returns.
    pub hole_secs: u64,
    /// OI spike detector.
    pub oi_spike: OiSpikeConfig,
    /// Liquidation cascade detector.
    pub liq_cascade: CascadeConfig,
    /// Delivery to Telegram and Discord.
    pub delivery: DeliveryConfig,
    /// The daily digest.
    pub digest: DigestConfig,
    /// Default channels: they seed `<data_dir>/alerts.json` while that file does not exist. Once it exists it is
    /// authoritative and these entries are ignored.
    pub channels: Vec<ChannelDef>,
    /// Default rules, seeded like `channels`.
    pub rules: Vec<RuleDef>,
}

impl Default for AlertsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            warmup_secs: 4 * 3600,
            input_queue: 100_000,
            max_symbols: 5000,
            series_max_samples: 1024,
            sample_spacing_secs: 15,
            hole_secs: 300,
            composite_eval_secs: 10,
            oi_spike: OiSpikeConfig::default(),
            liq_cascade: CascadeConfig::default(),
            delivery: DeliveryConfig::default(),
            digest: DigestConfig::default(),
            channels: Vec::new(),
            rules: Vec::new(),
        }
    }
}

/// One window of the OI spike detector.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OiWindow {
    /// Window length, seconds.
    pub secs: u32,
    /// |change of `oi_base`| that triggers, percent.
    pub threshold_pct: f64,
    /// Floor on the absolute change in USD (change of `oi_base` times the mark price).
    pub min_change_usd: f64,
    /// Switch for this window.
    #[serde(default = "yes")]
    pub enabled: bool,
}

fn yes() -> bool {
    true
}

/// `[alerts.oi_spike]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct OiSpikeConfig {
    /// Switch for the detector.
    pub enabled: bool,
    /// Windows with their thresholds and floors; `[[alerts.oi_spike.windows]]` replaces the whole list.
    pub windows: Vec<OiWindow>,
    /// Symbols whose current `oi_usd` is below this are ignored (dust), USD.
    pub min_oi_usd: f64,
    /// The change must also be at least this many robust standard deviations (1.4826 x MAD) away from the median
    /// of the symbol's own recent window changes.
    pub z_min: f64,
    /// Fewest historical window changes needed to judge a move unusual.
    pub min_baseline_points: usize,
    /// Without a usable baseline: do not signal (true) or signal on the threshold alone (false).
    pub require_baseline: bool,
    /// Seconds before the same (exchange, symbol, window, direction) can signal again.
    pub cooldown_secs: u64,
    /// A signal inside the cooldown is still emitted when |change| reaches this multiple of the last signalled
    /// one (the move extends materially).
    pub extension_factor: f64,
}

impl Default for OiSpikeConfig {
    fn default() -> Self {
        let w = |secs, threshold_pct, min_change_usd| OiWindow {
            secs,
            threshold_pct,
            min_change_usd,
            enabled: true,
        };
        Self {
            enabled: true,
            windows: vec![
                w(60, 1.5, 250_000.0),
                w(300, 3.0, 500_000.0),
                w(900, 5.0, 1_000_000.0),
                w(3600, 8.0, 2_000_000.0),
                w(14_400, 12.0, 3_000_000.0),
            ],
            min_oi_usd: 1_000_000.0,
            z_min: 4.0,
            min_baseline_points: 60,
            require_baseline: true,
            cooldown_secs: 900,
            extension_factor: 1.5,
        }
    }
}

/// `[alerts.liq_cascade]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CascadeConfig {
    /// Switch for the detector.
    pub enabled: bool,
    /// Also detect per exchange (besides market-wide).
    pub per_exchange: bool,
    /// Length of the baseline, seconds (the previous day by default).
    pub baseline_secs: u64,
    /// Fewest completed minutes of baseline needed before cascades are detected.
    pub min_baseline_minutes: usize,
    /// A cascade starts when the notional of the last 60 s is at least `median + k_mad x 1.4826 x MAD` of the
    /// baseline minutes.
    pub k_mad: f64,
    /// ... and at least this many USD market-wide.
    pub floor_usd: f64,
    /// ... and at least this many USD on one exchange (per exchange scope).
    pub floor_usd_exchange: f64,
    /// ... and at least this many liquidations in the last 60 s.
    pub min_count: u32,
    /// A cascade ends after this many seconds below `end_ratio` of the trigger level.
    pub quiet_secs: u64,
    /// Fraction of the trigger level under which the market counts as quiet.
    pub end_ratio: f64,
    /// Symbols listed in the closing summary.
    pub top_symbols: usize,
}

impl Default for CascadeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            per_exchange: true,
            baseline_secs: 86_400,
            min_baseline_minutes: 120,
            k_mad: 8.0,
            floor_usd: 2_000_000.0,
            floor_usd_exchange: 1_000_000.0,
            min_count: 5,
            quiet_secs: 120,
            end_ratio: 0.5,
            top_symbols: 5,
        }
    }
}

/// `[alerts.delivery]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DeliveryConfig {
    /// Base URL of the Telegram Bot API (tests point it at a mock server).
    pub telegram_base_url: String,
    /// Accept Discord webhook URLs outside discord.com and plain http (tests only).
    pub allow_custom_endpoints: bool,
    /// Telegram: minimum milliseconds between two messages to a chat.
    pub telegram_min_interval_ms: u32,
    /// Telegram: most messages per minute to a chat (the group limit is 20).
    pub telegram_max_per_minute: u32,
    /// Discord: minimum milliseconds between two messages to a webhook.
    pub discord_min_interval_ms: u32,
    /// Discord: most messages per minute to a webhook.
    pub discord_max_per_minute: u32,
    /// Messages produced within this window are merged into one digest, milliseconds.
    pub batch_window_ms: u32,
    /// Alerts listed in a digest; the rest are counted.
    pub digest_max_items: usize,
    /// Alerts queued per channel; above it the oldest are dropped and summarized.
    pub queue_limit: usize,
    /// First retry delay after a failure, milliseconds.
    pub retry_min_ms: u64,
    /// Longest retry delay, milliseconds.
    pub retry_max_ms: u64,
    /// Attempts per message before it is given up.
    pub max_attempts: u32,
    /// Timeout of one HTTP request, seconds.
    pub request_timeout_secs: u64,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            telegram_base_url: "https://api.telegram.org".into(),
            allow_custom_endpoints: false,
            telegram_min_interval_ms: 1100,
            telegram_max_per_minute: 20,
            discord_min_interval_ms: 2000,
            discord_max_per_minute: 20,
            batch_window_ms: 3000,
            digest_max_items: 12,
            queue_limit: 500,
            retry_min_ms: 1000,
            retry_max_ms: 60_000,
            max_attempts: 8,
            request_timeout_secs: 10,
        }
    }
}

/// The sections the daily digest can contain, in the order they appear.
pub const DIGEST_SECTIONS: [&str; 6] = [
    "oi_movers",
    "liquidations",
    "skew",
    "cascades",
    "funding",
    "gaps",
];

/// `[alerts.digest]`: one summary message a day, built from the History aggregate code.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DigestConfig {
    /// Sends the digest every day at `time_utc`. `Control.SendDigestNow` works whatever this says.
    pub enabled: bool,
    /// Time of day, UTC, `HH:MM`.
    pub time_utc: String,
    /// Channel ids that receive it; empty means every enabled channel that is not a system channel.
    pub channels: Vec<String>,
    /// Sections, in the order of [`DIGEST_SECTIONS`] whatever order is given here: `oi_movers` (top OI growth and
    /// decline over 24 h), `liquidations` (the largest), `skew` (long versus short liquidations per exchange),
    /// `cascades` (number and size), `funding` (extremes, when funding data exists), `gaps` (data gaps of the day).
    pub sections: Vec<String>,
    /// Entries per list.
    pub top_n: u32,
}

impl Default for DigestConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            time_utc: "07:00".into(),
            channels: Vec::new(),
            sections: DIGEST_SECTIONS.iter().map(|s| (*s).to_string()).collect(),
            top_n: 5,
        }
    }
}

impl DigestConfig {
    /// Seconds after midnight UTC of `time_utc`, if it is a valid `HH:MM`.
    pub fn time_of_day_secs(&self) -> Option<u32> {
        let (h, m) = self.time_utc.split_once(':')?;
        let (h, m): (u32, u32) = (h.parse().ok()?, m.parse().ok()?);
        (h < 24 && m < 60 && self.time_utc.len() == 5).then_some(h * 3600 + m * 60)
    }

    /// Adds every problem to `problems`.
    pub fn validate(&self, problems: &mut Vec<String>) {
        if self.time_of_day_secs().is_none() {
            problems.push(format!(
                "alerts.digest.time_utc: {:?} is not HH:MM (24 hour, UTC)",
                self.time_utc
            ));
        }
        for s in &self.sections {
            if !DIGEST_SECTIONS.contains(&s.as_str()) {
                problems.push(format!(
                    "alerts.digest.sections: {s:?} is not one of {}",
                    DIGEST_SECTIONS.join(", ")
                ));
            }
        }
        if !(1..=20).contains(&self.top_n) {
            problems.push("alerts.digest.top_n must be between 1 and 20".into());
        }
        for c in &self.channels {
            if !crate::alerts::model::valid_id(c) {
                problems.push(format!(
                    "alerts.digest.channels: {c:?} is not a valid channel id"
                ));
            }
        }
    }
}

impl AlertsConfig {
    /// Adds every problem to `problems`.
    pub fn validate(&self, problems: &mut Vec<String>) {
        let mut check = |name: &str, ok: bool| {
            if !ok {
                problems.push(format!("alerts.{name}"));
            }
        };
        check(
            "warmup_secs must be 0 or at least 60",
            self.warmup_secs == 0 || self.warmup_secs >= 60,
        );
        check("input_queue must be greater than 0", self.input_queue > 0);
        check("max_symbols must be greater than 0", self.max_symbols > 0);
        check(
            "series_max_samples must be between 16 and 100000",
            (16..=100_000).contains(&self.series_max_samples),
        );
        check(
            "sample_spacing_secs must be greater than 0",
            self.sample_spacing_secs > 0,
        );
        check(
            "composite_eval_secs must be between 1 and 300",
            (1..=300).contains(&self.composite_eval_secs),
        );
        check(
            "hole_secs must be greater than sample_spacing_secs",
            self.hole_secs > self.sample_spacing_secs,
        );
        let o = &self.oi_spike;
        check(
            "oi_spike.windows must not be empty",
            !o.windows.is_empty() && o.windows.len() <= 8,
        );
        let mut seen = std::collections::HashSet::new();
        let mut bad_windows = Vec::new();
        for w in &o.windows {
            if !(30..=14_400).contains(&w.secs)
                || !w.threshold_pct.is_finite()
                || w.threshold_pct <= 0.0
                || !w.min_change_usd.is_finite()
                || w.min_change_usd < 0.0
                || !seen.insert(w.secs)
            {
                bad_windows.push(w.secs);
            }
        }
        if !bad_windows.is_empty() {
            problems.push(format!(
                "alerts.oi_spike.windows: invalid or duplicate window(s) {bad_windows:?} (30..=14400 s, positive threshold_pct, non-negative min_change_usd)"
            ));
        }
        let mut check = |name: &str, ok: bool| {
            if !ok {
                problems.push(format!("alerts.{name}"));
            }
        };
        check(
            "oi_spike.min_oi_usd must not be negative",
            o.min_oi_usd >= 0.0,
        );
        check(
            "oi_spike.z_min must not be negative",
            o.z_min >= 0.0 && o.z_min.is_finite(),
        );
        check(
            "oi_spike.extension_factor must be at least 1",
            o.extension_factor >= 1.0,
        );
        let c = &self.liq_cascade;
        check(
            "liq_cascade.baseline_secs must be at least 3600",
            c.baseline_secs >= 3600,
        );
        check(
            "liq_cascade.k_mad must be positive",
            c.k_mad > 0.0 && c.k_mad.is_finite(),
        );
        check("liq_cascade.floor_usd must be positive", c.floor_usd > 0.0);
        check(
            "liq_cascade.floor_usd_exchange must be positive",
            c.floor_usd_exchange > 0.0,
        );
        check(
            "liq_cascade.quiet_secs must be greater than 0",
            c.quiet_secs > 0,
        );
        check(
            "liq_cascade.end_ratio must be in (0, 1]",
            c.end_ratio > 0.0 && c.end_ratio <= 1.0,
        );
        let d = &self.delivery;
        check(
            "delivery.telegram_base_url must start with http:// or https://",
            d.telegram_base_url.starts_with("http://")
                || d.telegram_base_url.starts_with("https://"),
        );
        check(
            "delivery.queue_limit must be greater than 0",
            d.queue_limit > 0,
        );
        check(
            "delivery.digest_max_items must be greater than 0",
            d.digest_max_items > 0,
        );
        check(
            "delivery.max_attempts must be greater than 0",
            d.max_attempts > 0,
        );
        check(
            "delivery.retry_min_ms must be greater than 0",
            d.retry_min_ms > 0,
        );
        check(
            "delivery.retry_max_ms must be at least retry_min_ms",
            d.retry_max_ms >= d.retry_min_ms,
        );
        check(
            "delivery.request_timeout_secs must be greater than 0",
            d.request_timeout_secs > 0,
        );
        check(
            "delivery.*_max_per_minute must be greater than 0",
            d.telegram_max_per_minute > 0 && d.discord_max_per_minute > 0,
        );
        self.digest.validate(problems);
    }
}
