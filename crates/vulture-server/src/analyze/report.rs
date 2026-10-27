//! Markdown and JSON output of the study.

use std::fmt::Write;

use super::study::{GroupStats, StudyReport};
use crate::query::bins::format_utc;

fn opt(v: Option<f64>, digits: usize) -> String {
    v.map_or("-".to_string(), |x| format!("{x:.digits$}"))
}

fn pct(v: Option<f64>) -> String {
    v.map_or("-".to_string(), |x| format!("{:.0}%", 100.0 * x))
}

fn usd(v: Option<f64>) -> String {
    v.map_or("-".to_string(), |x| {
        if x.abs() >= 1e6 {
            format!("{:.2}M", x / 1e6)
        } else if x.abs() >= 1e3 {
            format!("{:.1}k", x / 1e3)
        } else {
            format!("{x:.0}")
        }
    })
}

fn group_title(g: &GroupStats) -> String {
    let mut t = format!("{} / {} s / {}", g.kind.name(), g.window_secs, g.exchange);
    if let Some(s) = &g.strength {
        let _ = write!(t, " / {s}");
    }
    t
}

/// Renders the report as Markdown.
pub fn markdown(r: &StudyReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Signal event study\n");
    let _ = writeln!(
        out,
        "Period {} to {} UTC. {} signal(s); {} excluded for lack of a mark price at their time.\n",
        format_utc(r.from_ts),
        format_utc(r.to_ts),
        r.signals,
        r.excluded_no_price
    );
    let _ = writeln!(
        out,
        "Returns are forward mark-price returns in percent. *Directional* returns are signed by the expected \
         direction (momentum of the price move that came with the signal; for cascades the side that was \
         liquidated), so positive means continuation. A *continuation hit* is a move of at least the threshold \
         in the expected direction, a *reversal hit* one against it; shares are over signals that have a \
         direction and a return at the horizon.\n"
    );
    if r.groups.is_empty() {
        let _ = writeln!(out, "No signals in the period.");
        return out;
    }
    for g in &r.groups {
        let _ = writeln!(out, "## {}\n", group_title(g));
        let _ = writeln!(
            out,
            "Signals {}; excluded (no price) {}; without a direction {}; liquidated in the next hour: median {} \
             USD (quartiles {} to {}, n {}).\n",
            g.signals,
            g.excluded_no_price,
            g.undirected,
            usd(g.liq_next_hour_usd.median),
            usd(g.liq_next_hour_usd.q1),
            usd(g.liq_next_hour_usd.q3),
            g.liq_next_hour_usd.n
        );
        let _ = writeln!(
            out,
            "| horizon | measured | missing | raw median | raw q1..q3 | directional median | directional q1..q3 |"
        );
        let _ = writeln!(out, "|---|---|---|---|---|---|---|");
        for h in &g.horizons {
            let _ = writeln!(
                out,
                "| +{} min | {} | {} | {} | {}..{} | {} | {}..{} |",
                h.minutes,
                h.measured,
                h.missing,
                opt(h.raw_return.median, 3),
                opt(h.raw_return.q1, 3),
                opt(h.raw_return.q3, 3),
                opt(h.directional_return.median, 3),
                opt(h.directional_return.q1, 3),
                opt(h.directional_return.q3, 3),
            );
        }
        let _ = writeln!(out);
        let thresholds: Vec<f64> = g
            .horizons
            .first()
            .map(|h| h.hits.iter().map(|x| x.threshold_pct).collect())
            .unwrap_or_default();
        let _ = write!(out, "| hits (continuation / reversal) |");
        for t in &thresholds {
            let _ = write!(out, " >= {t}% |");
        }
        let _ = writeln!(out);
        let _ = write!(out, "|---|");
        for _ in &thresholds {
            let _ = write!(out, "---|");
        }
        let _ = writeln!(out);
        for h in &g.horizons {
            let _ = write!(
                out,
                "| +{} min (n {}) |",
                h.minutes,
                h.hits.first().map_or(0, |x| x.directed)
            );
            for x in &h.hits {
                let _ = write!(out, " {} / {} |", pct(x.continuation), pct(x.reversal));
            }
            let _ = writeln!(out);
        }
        let _ = writeln!(out);
    }
    out
}

/// Renders the report as JSON.
pub fn json(r: &StudyReport) -> String {
    serde_json::to_string_pretty(r).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyze::study::{StudyConfig, summarize};

    #[test]
    fn an_empty_report_says_so() {
        let r = summarize(&[], &StudyConfig::default(), 0, 1000);
        let md = markdown(&r);
        assert!(md.contains("No signals in the period."), "{md}");
        let v: serde_json::Value = serde_json::from_str(&json(&r)).unwrap();
        assert_eq!(v["signals"], 0);
        assert!(v["groups"].as_array().unwrap().is_empty());
    }

    #[test]
    fn number_formats() {
        assert_eq!(usd(Some(1_250_000.0)), "1.25M");
        assert_eq!(usd(Some(12_500.0)), "12.5k");
        assert_eq!(usd(Some(12.0)), "12");
        assert_eq!(usd(None), "-");
        assert_eq!(pct(Some(0.456)), "46%");
        assert_eq!(opt(Some(1.23456), 2), "1.23");
    }
}
