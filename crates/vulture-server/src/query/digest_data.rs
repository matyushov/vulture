//! The numbers of the daily digest, computed with the History aggregate code (the same cells, merge and roll-up as
//! `Aggregate`, the same movers function, the same hot/cold boundary), so the digest and the API cannot disagree.
//!
//! The window is the 24 hours before `end`. Sections that rest on hourly bins (skew, funding) use whole hours: the
//! hour in progress is left out. A section that fails (ClickHouse down and the Redis window too short) is reported
//! in `errors` and the rest of the digest is still produced.

use std::collections::BTreeMap;
use std::sync::Arc;

use vulture_proto::v1::{
    AggregateRequest, EventType, GroupBy, HistoryRequest, Liquidation, Metric, OiMovers,
    OiMoversQuery, event::Payload,
};

use super::agg::{GapRec, Needs, RollupParams, rollup};
use super::bins::BinStep;
use super::cold::{Scope, TopLiqQuery};
use super::service::HistoryService;
use super::tail::{TailPlan, after_watermark, id_for_time, max_id, scan};
use crate::store::TYPE_LIQUIDATION;

const DAY: i64 = 86_400_000;
const HOUR: i64 = 3_600_000;

/// What to compute.
#[derive(Debug, Clone)]
pub struct DigestRequest {
    /// End of the window, ms.
    pub end: i64,
    /// Sections wanted (names of [`crate::config::DIGEST_SECTIONS`]).
    pub sections: Vec<String>,
    /// Entries per list.
    pub top_n: usize,
}

/// Long and short liquidations of one exchange.
#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeSkew {
    /// Exchange.
    pub exchange: String,
    /// Liquidated longs, USD.
    pub long_usd: f64,
    /// Liquidated shorts, USD.
    pub short_usd: f64,
    /// Number of liquidations.
    pub count: u64,
}

/// Cascades of the window.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CascadeSummary {
    /// Market-wide cascades that ended in the window.
    pub count: usize,
    /// Their liquidated USD together.
    pub total_usd: f64,
    /// The largest one, USD.
    pub largest_usd: f64,
    /// Longest duration, seconds.
    pub longest_secs: u64,
}

/// A symbol with a funding rate.
#[derive(Debug, Clone, PartialEq)]
pub struct FundingEntry {
    /// Symbol.
    pub symbol: String,
    /// Funding rate as a fraction.
    pub rate: f64,
    /// OI in USD at the same time.
    pub oi_usd: Option<f64>,
}

/// Funding extremes.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FundingExtremes {
    /// Highest rates first.
    pub highest: Vec<FundingEntry>,
    /// Lowest rates first.
    pub lowest: Vec<FundingEntry>,
}

/// Interruptions of one stream.
#[derive(Debug, Clone, PartialEq)]
pub struct GapSummary {
    /// Exchange (`*` for the writer's own).
    pub exchange: String,
    /// Stream.
    pub stream: String,
    /// Gaps overlapping the window.
    pub count: usize,
    /// Their duration inside the window, ms.
    pub total_ms: i64,
    /// The longest, ms.
    pub longest_ms: i64,
}

/// The computed digest.
#[derive(Debug, Clone, Default)]
pub struct DigestData {
    /// Window start, ms.
    pub from_ts: i64,
    /// Window end, ms.
    pub to_ts: i64,
    /// End of the hourly bins used by skew and funding, ms.
    pub bins_to_ts: i64,
    /// Top OI growth and decline over 24 h.
    pub movers: Option<OiMovers>,
    /// The largest liquidations.
    pub biggest: Option<Vec<Liquidation>>,
    /// Skew per exchange, largest total first.
    pub skew: Option<Vec<ExchangeSkew>>,
    /// Cascades.
    pub cascades: Option<CascadeSummary>,
    /// Funding extremes; `Some(None-like empty)` is represented by `None` when there is no funding data at all.
    pub funding: Option<FundingExtremes>,
    /// Whether the funding section was asked for but no sample had funding data.
    pub funding_unavailable: bool,
    /// Gap summary, longest first.
    pub gaps: Option<Vec<GapSummary>>,
    /// Where the aggregate data came from.
    pub source: String,
    /// Sections that failed: (section, reason).
    pub errors: Vec<(String, String)>,
}

fn summarize_gaps(gaps: &[GapRec], from: i64, to: i64) -> Vec<GapSummary> {
    let mut m: BTreeMap<(String, String), GapSummary> = BTreeMap::new();
    for g in gaps {
        if g.from_ts >= to || g.to_ts < from {
            continue;
        }
        let inside = g.to_ts.min(to) - g.from_ts.max(from);
        let e = m
            .entry((g.exchange.clone(), g.stream.clone()))
            .or_insert_with(|| GapSummary {
                exchange: g.exchange.clone(),
                stream: g.stream.clone(),
                count: 0,
                total_ms: 0,
                longest_ms: 0,
            });
        e.count += 1;
        e.total_ms += inside.max(0);
        e.longest_ms = e.longest_ms.max(inside.max(0));
    }
    let mut v: Vec<GapSummary> = m.into_values().collect();
    v.sort_by(|a, b| {
        b.total_ms
            .cmp(&a.total_ms)
            .then(a.exchange.cmp(&b.exchange))
    });
    v
}

impl HistoryService {
    /// Computes the digest data.
    pub async fn digest_data(self: &Arc<Self>, req: &DigestRequest) -> DigestData {
        let want = |name: &str| req.sections.iter().any(|s| s == name);
        let to = req.end;
        let from = to - DAY;
        let step = BinStep::Hour1;
        let bins_to = step.start(to);
        let mut data = DigestData {
            from_ts: from,
            to_ts: to,
            bins_to_ts: bins_to,
            ..DigestData::default()
        };
        let fail = |data: &mut DigestData, section: &str, e: String| {
            data.errors.push((section.to_string(), e));
        };

        if want("oi_movers") {
            let r = self
                .movers_inner(
                    &AggregateRequest {
                        to_ts: to,
                        ..AggregateRequest::default()
                    },
                    OiMoversQuery {
                        window_secs: 86_400,
                        limit: req.top_n as u32,
                        min_oi_usd: 1_000_000.0,
                        ..OiMoversQuery::default()
                    },
                )
                .await;
            match r {
                Ok(resp) => {
                    data.source = resp.source;
                    data.movers = resp.oi_movers;
                }
                Err(e) => fail(&mut data, "oi_movers", e.message().to_string()),
            }
        }
        if want("liquidations") {
            match self.top_liquidations(from, to, req.top_n).await {
                Ok(v) => data.biggest = Some(v),
                Err(e) => fail(&mut data, "liquidations", e),
            }
        }
        if want("skew") {
            let metrics = vec![Metric::LiqLongUsd, Metric::LiqShortUsd, Metric::LiqCount];
            let lo = bins_to - DAY;
            match self
                .collect_or_fallback(&Scope::default(), step, lo, bins_to, Needs::of(&metrics))
                .await
            {
                Ok(c) => {
                    if data.source.is_empty() {
                        data.source = c.source.to_string();
                    }
                    match rollup(
                        &c.fine,
                        &c.gaps,
                        &RollupParams {
                            group_by: GroupBy::Exchange,
                            step,
                            first_bin: lo,
                            end: bins_to,
                            metrics: metrics.clone(),
                            max_rows: self.cfg.max_aggregate_rows as usize,
                        },
                    ) {
                        Ok(r) => {
                            let mut by: BTreeMap<String, ExchangeSkew> = BTreeMap::new();
                            for row in &r.rows {
                                let v = |m: Metric| {
                                    row.values
                                        .iter()
                                        .find(|x| x.metric == m as i32)
                                        .and_then(|x| x.value)
                                        .unwrap_or(0.0)
                                };
                                let e =
                                    by.entry(row.group.clone()).or_insert_with(|| ExchangeSkew {
                                        exchange: row.group.clone(),
                                        long_usd: 0.0,
                                        short_usd: 0.0,
                                        count: 0,
                                    });
                                e.long_usd += v(Metric::LiqLongUsd);
                                e.short_usd += v(Metric::LiqShortUsd);
                                e.count += v(Metric::LiqCount) as u64;
                            }
                            let mut v: Vec<ExchangeSkew> = by.into_values().collect();
                            v.sort_by(|a, b| {
                                (b.long_usd + b.short_usd).total_cmp(&(a.long_usd + a.short_usd))
                            });
                            data.skew = Some(v);
                        }
                        Err(e) => fail(&mut data, "skew", e.to_string()),
                    }
                }
                Err(e) => fail(&mut data, "skew", e.message().to_string()),
            }
        }
        if want("cascades") {
            match self.cascades_of(from, to).await {
                Ok(c) => data.cascades = Some(c),
                Err(e) => fail(&mut data, "cascades", e),
            }
        }
        if want("funding") {
            let metrics = vec![Metric::FundingLast, Metric::OiEndUsd];
            let lo = bins_to - 3 * HOUR;
            match self
                .collect_or_fallback(&Scope::default(), step, lo, bins_to, Needs::of(&metrics))
                .await
            {
                Ok(c) => match rollup(
                    &c.fine,
                    &c.gaps,
                    &RollupParams {
                        group_by: GroupBy::Symbol,
                        step,
                        first_bin: lo,
                        end: bins_to,
                        metrics: metrics.clone(),
                        max_rows: self.cfg.max_aggregate_rows as usize,
                    },
                ) {
                    Ok(r) => {
                        // The newest bin with a funding rate of each symbol.
                        let mut last: BTreeMap<String, (i64, FundingEntry)> = BTreeMap::new();
                        for row in &r.rows {
                            let val = |m: Metric| {
                                row.values
                                    .iter()
                                    .find(|x| x.metric == m as i32)
                                    .and_then(|x| x.value)
                            };
                            if let Some(rate) = val(Metric::FundingLast) {
                                let entry = FundingEntry {
                                    symbol: row.group.clone(),
                                    rate,
                                    oi_usd: val(Metric::OiEndUsd),
                                };
                                if last
                                    .get(&row.group)
                                    .is_none_or(|(t, _)| row.bucket_start >= *t)
                                {
                                    last.insert(row.group.clone(), (row.bucket_start, entry));
                                }
                            }
                        }
                        // Dust symbols have wild funding rates; skip those under 1 million USD of OI.
                        let mut all: Vec<FundingEntry> = last
                            .into_values()
                            .map(|(_, e)| e)
                            .filter(|e| e.oi_usd.is_some_and(|u| u >= 1_000_000.0))
                            .collect();
                        if all.is_empty() {
                            data.funding_unavailable = true;
                        } else {
                            all.sort_by(|a, b| {
                                b.rate.total_cmp(&a.rate).then(a.symbol.cmp(&b.symbol))
                            });
                            let n = req.top_n.min(all.len());
                            data.funding = Some(FundingExtremes {
                                highest: all[..n].to_vec(),
                                lowest: all.iter().rev().take(n).cloned().collect(),
                            });
                        }
                    }
                    Err(e) => fail(&mut data, "funding", e.to_string()),
                },
                Err(e) => fail(&mut data, "funding", e.message().to_string()),
            }
        }
        if want("gaps") {
            match self
                .collect_or_fallback(&Scope::default(), step, from, to, Needs::default())
                .await
            {
                Ok(c) => data.gaps = Some(summarize_gaps(&c.gaps, from, to)),
                Err(e) => fail(&mut data, "gaps", e.message().to_string()),
            }
        }
        data
    }

    /// The `n` largest liquidations of `[from, to)` from ClickHouse and the unflushed tail.
    pub async fn top_liquidations(
        &self,
        from: i64,
        to: i64,
        n: usize,
    ) -> Result<Vec<Liquidation>, String> {
        let mut found: BTreeMap<String, Liquidation> = BTreeMap::new();
        let mut cold_ok = false;
        let wm = match &self.cold {
            Some(_) => Some(self.current_watermark().await.map_err(|e| e.to_string())?),
            None => None,
        };
        if let Some(cold) = &self.cold {
            match cold
                .top_liquidations(TopLiqQuery {
                    from,
                    to,
                    n,
                    max_memory: self.cfg.clickhouse_max_memory_bytes,
                    timeout: self.timeout(),
                })
                .await
            {
                Ok(events) => {
                    cold_ok = true;
                    for e in events {
                        if let Some(Payload::Liquidation(l)) = e.payload {
                            found.insert(e.id, l);
                        }
                    }
                }
                Err(e) => {
                    self.stats.clickhouse_error();
                    if !self.redis_covers(from).await {
                        return Err(e.to_string());
                    }
                    self.stats.redis_fallback();
                }
            }
        }
        let slack = self.slack();
        let by_time = id_for_time(from, slack);
        let start = match wm {
            Some(w) if cold_ok => max_id(&after_watermark(w), &by_time),
            _ => by_time,
        };
        let plan = TailPlan {
            start,
            stop_after_ms: self
                .cold
                .is_none()
                .then(|| (to + slack.as_millis() as i64).max(0) as u64),
            page: self.cfg.tail_page as usize,
            max_entries: self.cfg.tail_max_entries as usize,
        };
        scan(self.store.as_ref(), &plan, |entry| {
            if entry.kind != TYPE_LIQUIDATION {
                return;
            }
            let id = entry.id.clone();
            if let Ok(ev) = entry.into_event()
                && let Some(Payload::Liquidation(l)) = ev.payload
                && l.exchange_ts >= from
                && l.exchange_ts < to
            {
                found.insert(id, l);
            }
        })
        .await
        .map_err(|e| e.to_string())?;
        let mut v: Vec<Liquidation> = found.into_values().collect();
        v.sort_by(|a, b| b.notional_usd.total_cmp(&a.notional_usd));
        v.truncate(n);
        Ok(v)
    }

    /// Market-wide liquidation cascades that ended in `[from, to)`.
    async fn cascades_of(self: &Arc<Self>, from: i64, to: i64) -> Result<CascadeSummary, String> {
        let mut rx = self
            .clone()
            .start_query(HistoryRequest {
                types: vec![EventType::Signal as i32],
                from_ts: from,
                to_ts: to,
                limit: self.cfg.max_rows.min(100_000),
                ..HistoryRequest::default()
            })
            .map_err(|s| s.message().to_string())?;
        let mut out = CascadeSummary::default();
        while let Some(item) = rx.recv().await {
            let e = item.map_err(|s| s.message().to_string())?;
            let Some(Payload::Signal(s)) = e.payload else {
                continue;
            };
            if s.r#type != vulture_proto::v1::SignalType::LiqCascade as i32
                || s.exchange != "*"
                || s.details.get("phase").map(String::as_str) != Some("end")
            {
                continue;
            }
            let total: f64 = s
                .details
                .get("total_usd")
                .and_then(|v| v.parse().ok())
                .unwrap_or(s.change_abs);
            out.count += 1;
            out.total_usd += total;
            out.largest_usd = out.largest_usd.max(total);
            out.longest_secs = out.longest_secs.max(
                s.details
                    .get("duration_secs")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0),
            );
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gaps_are_clipped_to_the_window_and_summed() {
        let g = |ex: &str, st: &str, a, b| GapRec {
            exchange: ex.into(),
            stream: st.into(),
            from_ts: a,
            to_ts: b,
        };
        let gaps = [
            g("binance", "open_interest", 100, 200),
            g("binance", "open_interest", 900, 1_300),
            g("bybit", "liquidations", 0, 50),
            g("*", "redis-writer", 400, 410),
            g("bybit", "liquidations", 2_000, 3_000),
        ];
        let s = summarize_gaps(&gaps, 150, 1_000);
        assert_eq!(
            s.len(),
            2,
            "the gaps outside the window are left out: {s:?}"
        );
        assert_eq!(
            (
                s[0].exchange.as_str(),
                s[0].count,
                s[0].total_ms,
                s[0].longest_ms
            ),
            ("binance", 2, 50 + 100, 100)
        );
        assert_eq!((s[1].exchange.as_str(), s[1].total_ms), ("*", 10));
    }
}
