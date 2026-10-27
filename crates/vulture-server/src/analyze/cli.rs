//! The two `analyze` subcommands as library functions: the study of stored signals and the detector replay.

use std::fmt::Write;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde::Serialize;

use super::params::{Detector, apply};
use super::replay::{ChReplay, ReplayCase, ReplayConfig, ReplayRun, ReplaySource, replay};
use super::report;
use super::source::ChSource;
use super::study::{self, SignalRecord, StudyConfig, StudyReport, StudySource};
use crate::config::AlertsConfig;
use crate::history::ChClient;
use crate::query::bins::format_utc;
use crate::query::cold::Scope;

/// Options of `analyze signals`.
#[derive(Debug, Clone)]
pub struct SignalsOpts {
    /// Start of the period, ms.
    pub from: i64,
    /// End of the period, ms.
    pub to: i64,
    /// Only these exchanges (empty = all).
    pub exchanges: Vec<String>,
    /// The study settings.
    pub study: StudyConfig,
}

/// The output of a command.
#[derive(Debug, Clone)]
pub struct Rendered {
    /// Markdown report.
    pub markdown: String,
    /// JSON report.
    pub json: String,
}

/// Wraps a source and replaces its signals with a fixed list.
struct Fixed<'a> {
    inner: &'a dyn StudySource,
    signals: Vec<SignalRecord>,
}

impl StudySource for Fixed<'_> {
    fn signals(&self, from: i64, to: i64) -> BoxFuture<'_, Result<Vec<SignalRecord>, String>> {
        Box::pin(async move {
            Ok(self
                .signals
                .iter()
                .filter(|s| s.ts >= from && s.ts < to)
                .cloned()
                .collect())
        })
    }

    fn marks(
        &self,
        exchange: &str,
        symbol: &str,
        from: i64,
        to: i64,
    ) -> BoxFuture<'_, Result<Vec<(i64, f64)>, String>> {
        self.inner.marks(exchange, symbol, from, to)
    }

    fn liquidated_usd(
        &self,
        exchange: &str,
        symbol: &str,
        from: i64,
        to: i64,
    ) -> BoxFuture<'_, Result<f64, String>> {
        self.inner.liquidated_usd(exchange, symbol, from, to)
    }
}

/// `analyze signals`: the study of the stored signals of a period.
pub async fn signals(
    src: &dyn StudySource,
    opts: &SignalsOpts,
) -> Result<(StudyReport, Rendered), String> {
    let mut signals = src.signals(opts.from, opts.to).await?;
    if !opts.exchanges.is_empty() {
        signals.retain(|s| s.exchange == "*" || opts.exchanges.contains(&s.exchange));
    }
    let outcomes = study::measure(src, &opts.study, signals).await?;
    let report = study::summarize(&outcomes, &opts.study, opts.from, opts.to);
    let rendered = Rendered {
        markdown: report::markdown(&report),
        json: report::json(&report),
    };
    Ok((report, rendered))
}

/// `analyze signals` on ClickHouse.
pub async fn signals_on_clickhouse(
    client: Arc<ChClient>,
    opts: &SignalsOpts,
) -> Result<Rendered, String> {
    signals(&ChSource::new(client), opts).await.map(|r| r.1)
}

/// Options of `analyze replay`.
#[derive(Debug, Clone)]
pub struct ReplayOpts {
    /// The detector.
    pub detector: Detector,
    /// One parameter string per case (an empty string is the configured defaults); none means one default case.
    pub params: Vec<String>,
    /// Start of the period, ms.
    pub from: i64,
    /// End of the period, ms.
    pub to: i64,
    /// State-only run before the period, seconds.
    pub warmup_secs: u64,
    /// Length of one read, seconds.
    pub chunk_secs: u64,
    /// Filters.
    pub scope: Scope,
    /// Signals listed per case.
    pub list_max: usize,
    /// The study settings.
    pub study: StudyConfig,
}

#[derive(Serialize)]
struct SignalJson<'a> {
    ts: i64,
    #[serde(rename = "type")]
    kind: &'a str,
    exchange: &'a str,
    symbol: &'a str,
    window_secs: u32,
    change_pct: f64,
    threshold: f64,
    phase: Option<&'a str>,
}

#[derive(Serialize)]
struct CaseJson<'a> {
    label: &'a str,
    params: &'a str,
    signal_count: usize,
    signals: Vec<SignalJson<'a>>,
    study: &'a StudyReport,
}

#[derive(Serialize)]
struct ReplayJson<'a> {
    detector: &'a str,
    from_ts: i64,
    to_ts: i64,
    warmup_secs: u64,
    events_fed: u64,
    cases: Vec<CaseJson<'a>>,
}

fn records(run: &ReplayRun) -> Vec<SignalRecord> {
    run.signals
        .iter()
        .filter_map(SignalRecord::from_proto)
        .collect()
}

/// The result of a replay: structured, and rendered.
pub struct ReplayResult {
    /// The runs.
    pub runs: Vec<ReplayRun>,
    /// Statistics per run.
    pub studies: Vec<StudyReport>,
    /// Events fed to the engine core.
    pub events_fed: u64,
    /// The output.
    pub rendered: Rendered,
}

/// `analyze replay` over any sources.
pub async fn replay_with(
    events: &dyn ReplaySource,
    study_src: &dyn StudySource,
    base: &AlertsConfig,
    opts: &ReplayOpts,
) -> Result<ReplayResult, String> {
    let param_sets: Vec<String> = if opts.params.is_empty() {
        vec![String::new()]
    } else {
        opts.params.clone()
    };
    let cases = param_sets
        .iter()
        .enumerate()
        .map(|(i, p)| {
            Ok(ReplayCase {
                label: format!("case {}", i + 1),
                config: apply(opts.detector, base, p)
                    .map_err(|e| format!("--params {p:?}: {e}"))?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let cfg = ReplayConfig {
        from: opts.from,
        to: opts.to,
        warmup_ms: (opts.warmup_secs as i64) * 1000,
        chunk_ms: (opts.chunk_secs.max(60) as i64) * 1000,
        scope: opts.scope.clone(),
    };
    let (runs, fed) = replay(events, &cases, &cfg).await?;
    let mut studies = Vec::new();
    for run in &runs {
        let fixed = Fixed {
            inner: study_src,
            signals: records(run),
        };
        let out = study::measure(&fixed, &opts.study, records(run)).await?;
        studies.push(study::summarize(&out, &opts.study, opts.from, opts.to));
    }
    let md = render_markdown(opts, &param_sets, &runs, &studies, fed);
    let json = serde_json::to_string_pretty(&ReplayJson {
        detector: opts.detector.name(),
        from_ts: opts.from,
        to_ts: opts.to,
        warmup_secs: opts.warmup_secs,
        events_fed: fed,
        cases: runs
            .iter()
            .zip(&param_sets)
            .zip(&studies)
            .map(|((r, p), s)| CaseJson {
                label: &r.label,
                params: p,
                signal_count: r.signals.len(),
                signals: r
                    .signals
                    .iter()
                    .filter_map(|x| {
                        let rec = SignalRecord::from_proto(x)?;
                        Some(SignalJson {
                            ts: rec.ts,
                            kind: rec.kind.name(),
                            exchange: &x.exchange,
                            symbol: &x.symbol,
                            window_secs: x.window_secs,
                            change_pct: x.change_pct,
                            threshold: x.threshold,
                            phase: x.details.get("phase").map(String::as_str),
                        })
                    })
                    .collect(),
                study: s,
            })
            .collect(),
    })
    .unwrap_or_default();
    Ok(ReplayResult {
        runs,
        studies,
        events_fed: fed,
        rendered: Rendered { markdown: md, json },
    })
}

/// `analyze replay` on ClickHouse.
pub async fn replay_on_clickhouse(
    client: Arc<ChClient>,
    base: &AlertsConfig,
    opts: &ReplayOpts,
) -> Result<ReplayResult, String> {
    replay_with(
        &ChReplay::new(client.clone()),
        &ChSource::new(client),
        base,
        opts,
    )
    .await
}

fn hit(report: &StudyReport, minutes: i64, threshold: f64) -> (Option<f64>, Option<f64>) {
    let mut cont = (0.0, 0usize);
    let mut rev = 0.0;
    for g in &report.groups {
        if let Some(h) = g.horizons.iter().find(|h| h.minutes == minutes)
            && let Some(x) = h
                .hits
                .iter()
                .find(|x| (x.threshold_pct - threshold).abs() < 1e-9)
            && let (Some(c), Some(r)) = (x.continuation, x.reversal)
        {
            cont.0 += c * x.directed as f64;
            rev += r * x.directed as f64;
            cont.1 += x.directed;
        }
    }
    if cont.1 == 0 {
        (None, None)
    } else {
        (Some(cont.0 / cont.1 as f64), Some(rev / cont.1 as f64))
    }
}

fn render_markdown(
    opts: &ReplayOpts,
    params: &[String],
    runs: &[ReplayRun],
    studies: &[StudyReport],
    fed: u64,
) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Detector replay: {}\n", opts.detector.name());
    let _ = writeln!(
        out,
        "Period {} to {} UTC after a warm-up of {} s; {} events fed to the engine core (no Redis, no delivery, no rules).\n",
        format_utc(opts.from),
        format_utc(opts.to),
        opts.warmup_secs,
        fed
    );
    let h = opts.study.horizons_ms.get(1).map_or(15, |h| h / 60_000);
    let t = opts.study.thresholds_pct.get(1).copied().unwrap_or(0.25);
    let _ = writeln!(out, "## Comparison\n");
    let _ = writeln!(
        out,
        "| case | parameters | signals | excluded | continuation >= {t}% at +{h} min | reversal >= {t}% at +{h} min |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|");
    for ((run, p), s) in runs.iter().zip(params).zip(studies) {
        let (c, r) = hit(s, h, t);
        let f = |v: Option<f64>| v.map_or("-".to_string(), |x| format!("{:.0}%", 100.0 * x));
        let _ = writeln!(
            out,
            "| {} | `{}` | {} | {} | {} | {} |",
            run.label,
            if p.is_empty() { "(defaults)" } else { p },
            run.signals.len(),
            s.excluded_no_price,
            f(c),
            f(r)
        );
    }
    let _ = writeln!(out);
    for ((run, p), s) in runs.iter().zip(params).zip(studies) {
        let _ = writeln!(
            out,
            "## {}: `{}`\n",
            run.label,
            if p.is_empty() { "(defaults)" } else { p }
        );
        if run.signals.is_empty() {
            let _ = writeln!(out, "No signals.\n");
        } else {
            let _ = writeln!(
                out,
                "| time (UTC) | type | exchange | symbol | window | change | threshold |"
            );
            let _ = writeln!(out, "|---|---|---|---|---|---|---|");
            for x in run.signals.iter().take(opts.list_max) {
                let kind = SignalRecord::from_proto(x).map_or("?", |r| r.kind.name());
                let phase = x
                    .details
                    .get("phase")
                    .map(|p| format!(" ({p})"))
                    .unwrap_or_default();
                let _ = writeln!(
                    out,
                    "| {} | {}{} | {} | {} | {} s | {:.2} | {:.2} |",
                    format_utc(x.ts),
                    kind,
                    phase,
                    x.exchange,
                    if x.symbol.is_empty() { "-" } else { &x.symbol },
                    x.window_secs,
                    x.change_pct,
                    x.threshold
                );
            }
            if run.signals.len() > opts.list_max {
                let _ = writeln!(
                    out,
                    "\n... and {} more (see the JSON output).",
                    run.signals.len() - opts.list_max
                );
            }
            let _ = writeln!(out);
        }
        let body = report::markdown(s);
        // The study's own title would repeat; demote its headings.
        for line in body.lines().skip(2) {
            let _ = writeln!(
                out,
                "{}",
                if line.starts_with("## ") {
                    format!("#{line}")
                } else {
                    line.to_string()
                }
            );
        }
    }
    out
}
