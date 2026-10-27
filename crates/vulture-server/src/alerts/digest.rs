//! The daily digest (B5): one summary message a day to the chosen channels.
//!
//! The numbers come from the History aggregate code ([`crate::query::digest_data`]): top OI growth and decline over
//! 24 hours (the movers function the detector uses), the largest liquidations, long versus short skew per exchange,
//! the number and size of cascades, funding extremes when funding data exists and the data gaps of the day. The text
//! goes through the ordinary channel queues, so rate limits, retries, `dry_run` and the Telegram and Discord
//! message limits apply; the digest is a message of its own and never merged into an alert batch. A scheduler task
//! sends it at `[alerts.digest].time_utc` every day; `Control.SendDigestNow` sends it on request. Quiet hours do not
//! apply: the owner chose the time.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use vulture_proto::v1::Liquidation;

use super::delivery::{Delivery, PushResult};
use super::manager::AlertsManager;
use super::text::{Report, ReportSection, fmt_num, fmt_pct, fmt_time, fmt_usd, report_plain};
use crate::config::{DIGEST_SECTIONS, DigestConfig};
use crate::metrics::{Families, MetricSource};
use crate::query::HistoryService;
use crate::query::digest_data::{DigestData, DigestRequest};
use crate::ws::Clock;

const DAY_MS: i64 = 86_400_000;

/// Why a digest was not sent.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DigestError {
    /// A channel id that does not exist, an unknown section, a bad `top_n`.
    #[error("{0}")]
    Invalid(String),
    /// No channel can receive it.
    #[error("{0}")]
    NoChannel(String),
}

/// What `send_now` did.
#[derive(Debug, Clone)]
pub struct SendOutcome {
    /// Channels the report was queued for.
    pub queued: Vec<String>,
    /// Plain-text rendering of the report.
    pub preview: String,
    /// Time spent building it, ms.
    pub build_ms: u64,
    /// Sections that could not be built.
    pub unavailable: Vec<String>,
}

/// Counters of the digest.
#[derive(Debug, Default)]
pub struct DigestStats {
    ok: AtomicU64,
    failed: AtomicU64,
    last_success_ms: AtomicI64,
    last_build_ms: AtomicU64,
    next_run_ms: AtomicI64,
}

/// The digest builder and sender.
pub struct DigestService {
    history: Arc<HistoryService>,
    delivery: Arc<Delivery>,
    manager: Arc<AlertsManager>,
    cfg: DigestConfig,
    clock: Arc<dyn Clock>,
    stats: DigestStats,
}

impl std::fmt::Debug for DigestService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DigestService")
            .field("enabled", &self.cfg.enabled)
            .finish_non_exhaustive()
    }
}

/// The next time of day `tod_secs` (seconds after midnight UTC) strictly after `now_ms`.
pub fn next_fire(now_ms: i64, tod_secs: u32) -> i64 {
    let day = now_ms.div_euclid(DAY_MS) * DAY_MS;
    let today = day + i64::from(tod_secs) * 1000;
    if today > now_ms {
        today
    } else {
        today + DAY_MS
    }
}

fn pct_rate(rate: f64) -> String {
    format!("{:+.4}%", rate * 100.0)
}

fn minutes(ms: i64) -> String {
    let m = (ms as f64 / 60_000.0).round() as i64;
    if m >= 120 {
        format!("{}h{:02}m", m / 60, m % 60)
    } else {
        format!("{m}m")
    }
}

fn liq_line(l: &Liquidation) -> String {
    let side = match vulture_proto::v1::Side::try_from(l.side) {
        Ok(vulture_proto::v1::Side::Long) => "long",
        Ok(vulture_proto::v1::Side::Short) => "short",
        _ => "?",
    };
    format!(
        "{} {} {} {} at {}",
        fmt_usd(l.notional_usd),
        side,
        l.symbol,
        l.exchange,
        &fmt_time(l.exchange_ts)[11..16]
    )
}

/// Turns the numbers into the report (sections in the fixed order of [`DIGEST_SECTIONS`]).
pub fn build_report(data: &DigestData, sections: &[String], top_n: usize, now_ms: i64) -> Report {
    let want = |s: &str| sections.iter().any(|x| x == s);
    let mut out = Vec::new();
    for name in DIGEST_SECTIONS {
        if !want(name) {
            continue;
        }
        let failed = data.errors.iter().find(|(s, _)| s == name);
        if failed.is_some() {
            continue;
        }
        let section = match name {
            "oi_movers" => data.movers.as_ref().map(|m| {
                let mut lines = Vec::new();
                for (label, list) in [("up", &m.top_growth), ("down", &m.top_decline)] {
                    for x in list.iter().take(top_n) {
                        lines.push(format!(
                            "{label} {} {} {}{}{}",
                            x.symbol,
                            x.exchange,
                            fmt_pct(x.change_pct),
                            x.oi_usd_end
                                .map(|u| format!(", OI {}", fmt_usd(u)))
                                .unwrap_or_default(),
                            if x.partial { " (partial)" } else { "" }
                        ));
                    }
                }
                if lines.is_empty() {
                    lines.push("no symbol with enough history and open interest".into());
                }
                ReportSection {
                    heading: "Open interest movers, 24 h".into(),
                    lines,
                }
            }),
            "liquidations" => data.biggest.as_ref().map(|v| ReportSection {
                heading: "Largest liquidations, 24 h".into(),
                lines: if v.is_empty() {
                    vec!["none".into()]
                } else {
                    v.iter().take(top_n).map(liq_line).collect()
                },
            }),
            "skew" => data.skew.as_ref().map(|v| ReportSection {
                heading: format!(
                    "Liquidations by exchange, 24 h to {}",
                    &fmt_time(data.bins_to_ts)[11..16]
                ),
                lines: if v.is_empty() {
                    vec!["none".into()]
                } else {
                    v.iter()
                        .map(|e| {
                            let total = e.long_usd + e.short_usd;
                            let long_share = if total > 0.0 {
                                100.0 * e.long_usd / total
                            } else {
                                0.0
                            };
                            format!(
                                "{}: long {}, short {} ({long_share:.0}% long), {} events",
                                e.exchange,
                                fmt_usd(e.long_usd),
                                fmt_usd(e.short_usd),
                                fmt_num(e.count as f64)
                            )
                        })
                        .collect()
                },
            }),
            "cascades" => data.cascades.as_ref().map(|c| ReportSection {
                heading: "Liquidation cascades, 24 h".into(),
                lines: if c.count == 0 {
                    vec!["none".into()]
                } else {
                    vec![format!(
                        "{} cascade(s), {} liquidated, largest {}, longest {}",
                        c.count,
                        fmt_usd(c.total_usd),
                        fmt_usd(c.largest_usd),
                        minutes(c.longest_secs as i64 * 1000)
                    )]
                },
            }),
            "funding" => {
                if data.funding_unavailable {
                    Some(ReportSection {
                        heading: "Funding extremes".into(),
                        lines: vec!["no funding data in the last hours".into()],
                    })
                } else {
                    data.funding.as_ref().map(|f| {
                        let mut lines = Vec::new();
                        for (label, list) in [("highest", &f.highest), ("lowest", &f.lowest)] {
                            for e in list.iter().take(top_n) {
                                lines.push(format!(
                                    "{label} {} {}{}",
                                    e.symbol,
                                    pct_rate(e.rate),
                                    e.oi_usd
                                        .map(|u| format!(", OI {}", fmt_usd(u)))
                                        .unwrap_or_default()
                                ));
                            }
                        }
                        ReportSection {
                            heading: "Funding extremes (symbols with OI over $1M)".into(),
                            lines,
                        }
                    })
                }
            }
            "gaps" => data.gaps.as_ref().map(|v| ReportSection {
                heading: "Data gaps, 24 h".into(),
                lines: if v.is_empty() {
                    vec!["none".into()]
                } else {
                    v.iter()
                        .take(top_n.max(5))
                        .map(|g| {
                            format!(
                                "{} {}: {} gap(s), {} in total, longest {}",
                                g.exchange,
                                g.stream,
                                g.count,
                                minutes(g.total_ms),
                                minutes(g.longest_ms)
                            )
                        })
                        .collect()
                },
            }),
            _ => None,
        };
        out.extend(section);
    }
    if !data.errors.is_empty() {
        out.push(ReportSection {
            heading: "Not available".into(),
            lines: data
                .errors
                .iter()
                .map(|(s, e)| format!("{s}: {}", crate::alerts::text::truncate_chars(e, 120)))
                .collect(),
        });
    }
    Report {
        ts_ms: now_ms,
        title: format!("vulture daily digest, {}", &fmt_time(now_ms)[..16]),
        sections: out,
        footer: format!(
            "Window {} to {} UTC; data: {}",
            &fmt_time(data.from_ts)[..16],
            &fmt_time(data.to_ts)[..16],
            if data.source.is_empty() {
                "n/a"
            } else {
                &data.source
            }
        ),
    }
}

impl DigestService {
    /// New service.
    pub fn new(
        history: Arc<HistoryService>,
        delivery: Arc<Delivery>,
        manager: Arc<AlertsManager>,
        cfg: DigestConfig,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            history,
            delivery,
            manager,
            cfg,
            clock,
            stats: DigestStats::default(),
        })
    }

    /// Builds the digest and queues it for the channels. Empty `channels` and `sections` mean the configured ones;
    /// with no configured channels, every enabled channel that is not a system channel.
    pub async fn send_now(
        &self,
        channels: &[String],
        sections: &[String],
        top_n: Option<u32>,
    ) -> Result<SendOutcome, DigestError> {
        let result = self.send_inner(channels, sections, top_n).await;
        match &result {
            Ok(_) => {
                self.stats.ok.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .last_success_ms
                    .store(self.clock.now_ms(), Ordering::Relaxed);
            }
            Err(_) => {
                self.stats.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    async fn send_inner(
        &self,
        channels: &[String],
        sections: &[String],
        top_n: Option<u32>,
    ) -> Result<SendOutcome, DigestError> {
        let sections: Vec<String> = if sections.is_empty() {
            self.cfg.sections.clone()
        } else {
            sections.to_vec()
        };
        for s in &sections {
            if !DIGEST_SECTIONS.contains(&s.as_str()) {
                return Err(DigestError::Invalid(format!(
                    "unknown section {s:?}; use {}",
                    DIGEST_SECTIONS.join(", ")
                )));
            }
        }
        let top_n = top_n.filter(|n| *n > 0).unwrap_or(self.cfg.top_n);
        if top_n > 20 {
            return Err(DigestError::Invalid("top_n must be at most 20".into()));
        }
        let known = self.manager.list_channels().await;
        let wanted: Vec<String> = if !channels.is_empty() {
            channels.to_vec()
        } else if !self.cfg.channels.is_empty() {
            self.cfg.channels.clone()
        } else {
            known
                .iter()
                .filter(|c| c.def.enabled && !c.def.system)
                .map(|c| c.def.id.clone())
                .collect()
        };
        for id in &wanted {
            if !known.iter().any(|c| &c.def.id == id) {
                return Err(DigestError::Invalid(format!("unknown channel {id:?}")));
            }
        }
        let usable: Vec<&String> = wanted
            .iter()
            .filter(|id| known.iter().any(|c| &c.def.id == *id && c.def.enabled))
            .collect();
        if usable.is_empty() {
            return Err(DigestError::NoChannel(
                "no enabled channel to send the digest to".into(),
            ));
        }
        let started = Instant::now();
        let now = self.clock.now_ms();
        let data = self
            .history
            .digest_data(&DigestRequest {
                end: now,
                sections: sections.clone(),
                top_n: top_n as usize,
            })
            .await;
        let report = build_report(&data, &sections, top_n as usize, now);
        let build_ms = started.elapsed().as_millis() as u64;
        self.stats.last_build_ms.store(build_ms, Ordering::Relaxed);
        let mut queued = Vec::new();
        for id in usable {
            if matches!(
                self.delivery.push_report(id, report.clone()),
                PushResult::Queued | PushResult::QueuedDroppedOldest
            ) {
                queued.push(id.clone());
            }
        }
        tracing::info!(
            channels = queued.len(),
            build_ms,
            errors = data.errors.len(),
            "daily digest queued"
        );
        Ok(SendOutcome {
            queued,
            preview: report_plain(&report),
            build_ms,
            unavailable: data.errors.iter().map(|(s, _)| s.clone()).collect(),
        })
    }

    /// Runs the daily schedule until `shutdown`. Does nothing when the digest is disabled.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        if !self.cfg.enabled {
            return;
        }
        let Some(tod) = self.cfg.time_of_day_secs() else {
            return;
        };
        loop {
            let next = next_fire(self.clock.now_ms(), tod);
            self.stats.next_run_ms.store(next, Ordering::Relaxed);
            // Sleep in short steps: a wall clock that jumps (NTP, suspend) must not make the digest late by a day.
            loop {
                let left = next - self.clock.now_ms();
                if left <= 0 {
                    break;
                }
                let step = Duration::from_millis(left.min(30_000) as u64);
                tokio::select! {
                    _ = tokio::time::sleep(step) => {}
                    _ = shutdown.cancelled() => return,
                }
            }
            if let Err(e) = self.send_now(&[], &[], None).await {
                tracing::warn!(error = %e, "the scheduled daily digest was not sent");
            }
            // Never fire twice for the same minute.
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                _ = shutdown.cancelled() => return,
            }
        }
    }
}

impl MetricSource for DigestService {
    fn collect(&self, out: &mut Families) {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        out.counter(
            "vulture_alerts_digest_runs_total",
            "Daily digests built, by result.",
            &[("result", "ok")],
            g(&self.stats.ok),
        );
        out.counter(
            "vulture_alerts_digest_runs_total",
            "Daily digests built, by result.",
            &[("result", "failed")],
            g(&self.stats.failed),
        );
        out.gauge(
            "vulture_alerts_digest_build_seconds",
            "Time the last digest took to build.",
            &[],
            g(&self.stats.last_build_ms) as f64 / 1000.0,
        );
        let last = self.stats.last_success_ms.load(Ordering::Relaxed);
        if last > 0 {
            out.gauge(
                "vulture_alerts_digest_last_success_age_seconds",
                "Seconds since the last digest was queued.",
                &[],
                (self.clock.now_ms() - last).max(0) as f64 / 1000.0,
            );
        }
        let next = self.stats.next_run_ms.load(Ordering::Relaxed);
        if next > 0 {
            out.gauge(
                "vulture_alerts_digest_next_run_timestamp_seconds",
                "Unix time of the next scheduled digest (absent when the schedule is off).",
                &[],
                next as f64 / 1000.0,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::digest_data::{
        CascadeSummary, ExchangeSkew, FundingEntry, FundingExtremes, GapSummary,
    };
    use vulture_proto::v1::{OiMover, OiMovers, Side};

    const T: i64 = 1_792_972_800_000 + 7 * 3_600_000;

    fn full() -> DigestData {
        DigestData {
            from_ts: T - DAY_MS,
            to_ts: T,
            bins_to_ts: T,
            movers: Some(OiMovers {
                top_growth: vec![OiMover {
                    exchange: "binance".into(),
                    symbol: "SOL-USDT-PERP".into(),
                    change_pct: 12.5,
                    change_abs: 1000.0,
                    oi_usd_end: Some(250_000_000.0),
                    ..OiMover::default()
                }],
                top_decline: vec![OiMover {
                    exchange: "bybit".into(),
                    symbol: "DOGE-USDT-PERP".into(),
                    change_pct: -8.0,
                    change_abs: -5.0,
                    partial: true,
                    ..OiMover::default()
                }],
                ..OiMovers::default()
            }),
            biggest: Some(vec![Liquidation {
                exchange: "binance".into(),
                symbol: "BTC-USDT-PERP".into(),
                side: Side::Long as i32,
                notional_usd: 4_200_000.0,
                exchange_ts: T - 3_600_000,
                ..Liquidation::default()
            }]),
            skew: Some(vec![ExchangeSkew {
                exchange: "binance".into(),
                long_usd: 6e6,
                short_usd: 2e6,
                count: 1234,
            }]),
            cascades: Some(CascadeSummary {
                count: 2,
                total_usd: 30e6,
                largest_usd: 20e6,
                longest_secs: 900,
            }),
            funding: Some(FundingExtremes {
                highest: vec![FundingEntry {
                    symbol: "X-USDT-PERP".into(),
                    rate: 0.0005,
                    oi_usd: Some(2e6),
                }],
                lowest: vec![FundingEntry {
                    symbol: "Y-USDT-PERP".into(),
                    rate: -0.0003,
                    oi_usd: None,
                }],
            }),
            funding_unavailable: false,
            gaps: Some(vec![GapSummary {
                exchange: "okx".into(),
                stream: "open_interest".into(),
                count: 2,
                total_ms: 600_000,
                longest_ms: 420_000,
            }]),
            source: "clickhouse+redis".into(),
            errors: vec![],
        }
    }

    fn all() -> Vec<String> {
        DIGEST_SECTIONS.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn the_report_has_every_section_in_order() {
        let r = build_report(&full(), &all(), 5, T);
        let headings: Vec<_> = r.sections.iter().map(|s| s.heading.as_str()).collect();
        assert_eq!(headings.len(), 6);
        assert!(headings[0].starts_with("Open interest movers"));
        assert!(headings[1].starts_with("Largest liquidations"));
        assert!(headings[2].starts_with("Liquidations by exchange"));
        assert!(headings[3].starts_with("Liquidation cascades"));
        assert!(headings[4].starts_with("Funding extremes"));
        assert!(headings[5].starts_with("Data gaps"));
        let text = report_plain(&r);
        assert!(
            text.contains("up SOL-USDT-PERP binance +12.50%, OI $250M"),
            "{text}"
        );
        assert!(
            text.contains("down DOGE-USDT-PERP bybit -8.00% (partial)"),
            "{text}"
        );
        assert!(
            text.contains("$4.20M long BTC-USDT-PERP binance at 06:00"),
            "{text}"
        );
        assert!(
            text.contains("binance: long $6.00M, short $2.00M (75% long), 1,234 events"),
            "{text}"
        );
        assert!(
            text.contains("2 cascade(s), $30.0M liquidated, largest $20.0M, longest 15m"),
            "{text}"
        );
        assert!(
            text.contains("highest X-USDT-PERP +0.0500%, OI $2.00M"),
            "{text}"
        );
        assert!(text.contains("lowest Y-USDT-PERP -0.0300%"), "{text}");
        assert!(
            text.contains("okx open_interest: 2 gap(s), 10m in total, longest 7m"),
            "{text}"
        );
        assert!(r.footer.contains("clickhouse+redis"));
        assert!(
            r.title
                .starts_with("vulture daily digest, 2026-10-26 07:00"),
            "{}",
            r.title
        );
    }

    #[test]
    fn sections_can_be_chosen_and_failures_are_listed() {
        let mut d = full();
        d.errors.push(("skew".into(), "ClickHouse is down".into()));
        d.skew = None;
        let r = build_report(&d, &["skew".into(), "gaps".into()], 3, T);
        let headings: Vec<_> = r.sections.iter().map(|s| s.heading.as_str()).collect();
        assert_eq!(headings, ["Data gaps, 24 h", "Not available"]);
        assert!(r.sections[1].lines[0].contains("skew: ClickHouse is down"));
        // No funding data: said in one line.
        let mut d = full();
        d.funding = None;
        d.funding_unavailable = true;
        let r = build_report(&d, &["funding".into()], 5, T);
        assert_eq!(r.sections[0].lines, ["no funding data in the last hours"]);
        // Empty everything reads "none".
        let mut d = full();
        d.cascades = Some(CascadeSummary::default());
        d.gaps = Some(vec![]);
        let r = build_report(&d, &["cascades".into(), "gaps".into()], 5, T);
        assert!(r.sections.iter().all(|s| s.lines == ["none"]));
    }

    #[test]
    fn long_reports_fit_the_platform_limits() {
        let mut d = full();
        d.movers = Some(OiMovers {
            top_growth: (0..20)
                .map(|i| OiMover {
                    exchange: "binance".into(),
                    symbol: format!("LONGNAME{i}-USDT-PERP"),
                    change_pct: 5.0,
                    oi_usd_end: Some(1e8),
                    ..OiMover::default()
                })
                .collect(),
            top_decline: (0..20)
                .map(|i| OiMover {
                    exchange: "bybit".into(),
                    symbol: format!("OTHERNAME{i}-USDT-PERP"),
                    change_pct: -5.0,
                    ..OiMover::default()
                })
                .collect(),
            ..OiMovers::default()
        });
        d.biggest = Some(
            (0..20)
                .map(|_| d.biggest.as_ref().unwrap()[0].clone())
                .collect(),
        );
        let r = build_report(&d, &all(), 20, T);
        let digest = crate::alerts::text::Digest {
            report: Some(r),
            ..Default::default()
        };
        let tg = crate::alerts::text::telegram_html(&digest);
        assert!(
            tg.chars().count() <= crate::alerts::text::TELEGRAM_LIMIT,
            "{}",
            tg.chars().count()
        );
        assert!(
            !tg.contains("<b>Open interest movers, 24 h</b>\n<b>"),
            "headings are followed by lines"
        );
        let dc = crate::alerts::text::discord_body(&digest);
        let desc = dc["embeds"][0]["description"].as_str().unwrap();
        assert!(desc.chars().count() <= crate::alerts::text::DISCORD_DESC_LIMIT);
        assert_eq!(dc["allowed_mentions"]["parse"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn the_schedule_finds_the_next_time_of_day() {
        let midnight = 1_792_972_800_000;
        let tod = 7 * 3600;
        assert_eq!(next_fire(midnight, tod), midnight + 7 * 3_600_000);
        assert_eq!(
            next_fire(midnight + 7 * 3_600_000 - 1, tod),
            midnight + 7 * 3_600_000
        );
        assert_eq!(
            next_fire(midnight + 7 * 3_600_000, tod),
            midnight + DAY_MS + 7 * 3_600_000
        );
        assert_eq!(
            next_fire(midnight + 23 * 3_600_000, tod),
            midnight + DAY_MS + 7 * 3_600_000
        );
        assert_eq!(next_fire(midnight, 0), midnight + DAY_MS);
    }

    #[test]
    fn durations_read_naturally() {
        assert_eq!(minutes(0), "0m");
        assert_eq!(minutes(600_000), "10m");
        assert_eq!(minutes(7_500_000), "2h05m");
    }
}
