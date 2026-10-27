//! Detector parameters of `analyze replay`: `key=value` pairs applied over the `[alerts]` configuration.
//!
//! Pairs are separated by commas or spaces. `oi_spike` keys: `z_min`, `min_oi_usd`, `cooldown_secs`,
//! `extension_factor`, `min_baseline_points`, `require_baseline` (true/false), `windows` (the windows to keep, in
//! seconds, joined by `+`, for example `windows=300+900`), `threshold_pct.<secs>` and `min_change_usd.<secs>` (the
//! threshold and the USD floor of one window). `liq_cascade` keys: `k_mad`, `floor_usd`, `floor_usd_exchange`,
//! `min_count`, `quiet_secs`, `end_ratio`, `min_baseline_minutes`, `baseline_secs`, `top_symbols`, `per_exchange`.
//! An unknown key is an error. Only the chosen detector stays enabled.

use crate::config::AlertsConfig;

/// A detector that can be replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detector {
    /// The OI spike detector.
    OiSpike,
    /// The liquidation cascade detector.
    LiqCascade,
}

impl Detector {
    /// Parses `oi_spike` or `liq_cascade`.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "oi_spike" => Ok(Self::OiSpike),
            "liq_cascade" => Ok(Self::LiqCascade),
            other => Err(format!(
                "unknown detector {other:?}: use oi_spike or liq_cascade"
            )),
        }
    }

    /// The name.
    pub fn name(self) -> &'static str {
        match self {
            Self::OiSpike => "oi_spike",
            Self::LiqCascade => "liq_cascade",
        }
    }
}

fn num<T: std::str::FromStr>(key: &str, v: &str) -> Result<T, String> {
    v.parse()
        .map_err(|_| format!("{key}: {v:?} is not a valid number"))
}

fn boolean(key: &str, v: &str) -> Result<bool, String> {
    match v {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(format!("{key}: {v:?} is not true or false")),
    }
}

/// Applies `params` to a copy of `base` for `detector` and validates the result.
pub fn apply(
    detector: Detector,
    base: &AlertsConfig,
    params: &str,
) -> Result<AlertsConfig, String> {
    let mut cfg = base.clone();
    cfg.enabled = true;
    cfg.oi_spike.enabled = detector == Detector::OiSpike;
    cfg.liq_cascade.enabled = detector == Detector::LiqCascade;
    for pair in params.split([',', ' ']).filter(|p| !p.is_empty()) {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| format!("{pair:?} is not key=value"))?;
        let o = &mut cfg.oi_spike;
        let c = &mut cfg.liq_cascade;
        match (detector, key) {
            (Detector::OiSpike, "z_min") => o.z_min = num(key, value)?,
            (Detector::OiSpike, "min_oi_usd") => o.min_oi_usd = num(key, value)?,
            (Detector::OiSpike, "cooldown_secs") => o.cooldown_secs = num(key, value)?,
            (Detector::OiSpike, "extension_factor") => o.extension_factor = num(key, value)?,
            (Detector::OiSpike, "min_baseline_points") => o.min_baseline_points = num(key, value)?,
            (Detector::OiSpike, "require_baseline") => o.require_baseline = boolean(key, value)?,
            (Detector::OiSpike, "windows") => {
                let keep: Vec<u32> = value
                    .split('+')
                    .map(|s| num::<u32>(key, s))
                    .collect::<Result<_, _>>()?;
                for k in &keep {
                    if !o.windows.iter().any(|w| w.secs == *k) {
                        return Err(format!(
                            "windows: {k} s is not one of the configured windows"
                        ));
                    }
                }
                o.windows.retain(|w| keep.contains(&w.secs));
            }
            (Detector::OiSpike, k)
                if k.starts_with("threshold_pct.") || k.starts_with("min_change_usd.") =>
            {
                let (field, secs) = k.split_once('.').unwrap_or((k, ""));
                let secs: u32 = num(k, secs)?;
                let w = o
                    .windows
                    .iter_mut()
                    .find(|w| w.secs == secs)
                    .ok_or_else(|| format!("{k}: no window of {secs} s"))?;
                if field == "threshold_pct" {
                    w.threshold_pct = num(k, value)?;
                } else {
                    w.min_change_usd = num(k, value)?;
                }
            }
            (Detector::LiqCascade, "k_mad") => c.k_mad = num(key, value)?,
            (Detector::LiqCascade, "floor_usd") => c.floor_usd = num(key, value)?,
            (Detector::LiqCascade, "floor_usd_exchange") => c.floor_usd_exchange = num(key, value)?,
            (Detector::LiqCascade, "min_count") => c.min_count = num(key, value)?,
            (Detector::LiqCascade, "quiet_secs") => c.quiet_secs = num(key, value)?,
            (Detector::LiqCascade, "end_ratio") => c.end_ratio = num(key, value)?,
            (Detector::LiqCascade, "min_baseline_minutes") => {
                c.min_baseline_minutes = num(key, value)?
            }
            (Detector::LiqCascade, "baseline_secs") => c.baseline_secs = num(key, value)?,
            (Detector::LiqCascade, "top_symbols") => c.top_symbols = num(key, value)?,
            (Detector::LiqCascade, "per_exchange") => c.per_exchange = boolean(key, value)?,
            _ => {
                return Err(format!(
                    "unknown parameter {key:?} for the {} detector (see `analyze replay --help`)",
                    detector.name()
                ));
            }
        }
    }
    let mut problems = Vec::new();
    cfg.validate(&mut problems);
    if problems.is_empty() {
        Ok(cfg)
    } else {
        Err(problems.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oi_spike_parameters() {
        let base = AlertsConfig::default();
        let c = apply(
            Detector::OiSpike,
            &base,
            "z_min=2.5, threshold_pct.300=2 windows=300+900 require_baseline=false",
        )
        .unwrap();
        assert!(c.oi_spike.enabled && !c.liq_cascade.enabled);
        assert_eq!(c.oi_spike.z_min, 2.5);
        assert!(!c.oi_spike.require_baseline);
        let secs: Vec<u32> = c.oi_spike.windows.iter().map(|w| w.secs).collect();
        assert_eq!(secs, [300, 900]);
        assert_eq!(c.oi_spike.windows[0].threshold_pct, 2.0);
        assert_eq!(c.oi_spike.windows[1].threshold_pct, 5.0, "untouched");
        // The base is not modified.
        assert_eq!(base.oi_spike.windows.len(), 5);
    }

    #[test]
    fn cascade_parameters_and_errors() {
        let base = AlertsConfig::default();
        let c = apply(
            Detector::LiqCascade,
            &base,
            "k_mad=5,floor_usd=1000000,per_exchange=false",
        )
        .unwrap();
        assert!(c.liq_cascade.enabled && !c.oi_spike.enabled);
        assert_eq!(
            (
                c.liq_cascade.k_mad,
                c.liq_cascade.floor_usd,
                c.liq_cascade.per_exchange
            ),
            (5.0, 1e6, false)
        );
        for (d, bad) in [
            (Detector::OiSpike, "k_mad=5"),
            (Detector::LiqCascade, "z_min=1"),
            (Detector::OiSpike, "z_min"),
            (Detector::OiSpike, "z_min=abc"),
            (Detector::OiSpike, "windows=7"),
            (Detector::OiSpike, "threshold_pct.7=1"),
            (Detector::OiSpike, "extension_factor=0.5"),
            (Detector::LiqCascade, "end_ratio=0"),
        ] {
            assert!(apply(d, &base, bad).is_err(), "{bad}");
        }
        assert!(Detector::parse("nope").is_err());
        assert_eq!(Detector::parse("oi_spike").unwrap().name(), "oi_spike");
    }
}
