//! `History.Seasonality`: profile of liquidations and OI change by hour of day and day of week.
//!
//! The profile is built from the hourly bins of `Aggregate` over the lookback (the same cells, merge and roll-up,
//! so ClickHouse and the Redis tail are combined the same way): for every group, every hourly bin goes into the
//! bucket of its UTC hour of day and of its day of week (Monday = 0), and each bucket reports the mean liquidated
//! USD per hour, the mean OI change per hour in percent and the mean absolute OI change. A bucket needs at least one
//! observation; the answer says how many distinct days it covers and is `partial` when that is fewer than asked,
//! when some weekday has no observation (less than a week of data), or when a gap touched an hour.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tonic::Status;
use vulture_proto::v1::{
    GroupBy, Metric, SeasonalityBucket, SeasonalityProfile, SeasonalityRequest, SeasonalityResponse,
};

use super::agg::{Needs, RollupParams, rollup};
use super::aggregate::Collected;
use super::bins::BinStep;
use super::service::{HistoryService, scope_of};
use super::stats::Reject;

const DAY: i64 = 86_400_000;
/// Most days a profile may look back.
pub const MAX_LOOKBACK_DAYS: u32 = 180;
/// Lookback when the request says 0.
pub const DEFAULT_LOOKBACK_DAYS: u32 = 30;

#[derive(Default, Clone)]
struct Sums {
    n: u32,
    liq: f64,
    liq_n: u32,
    oi: f64,
    abs_oi: f64,
    oi_n: u32,
}

impl Sums {
    fn add(&mut self, liq: Option<f64>, oi_pct: Option<f64>) {
        self.n += 1;
        if let Some(l) = liq {
            self.liq += l;
            self.liq_n += 1;
        }
        if let Some(o) = oi_pct {
            self.oi += o;
            self.abs_oi += o.abs();
            self.oi_n += 1;
        }
    }

    fn bucket(&self, index: u32) -> SeasonalityBucket {
        SeasonalityBucket {
            index,
            samples: self.n,
            mean_liq_usd: (self.liq_n > 0).then(|| self.liq / f64::from(self.liq_n)),
            mean_oi_change_pct: (self.oi_n > 0).then(|| self.oi / f64::from(self.oi_n)),
            mean_abs_oi_change_pct: (self.oi_n > 0).then(|| self.abs_oi / f64::from(self.oi_n)),
        }
    }
}

/// UTC hour of day (0 to 23) and day of week (Monday = 0) of a bin start.
pub fn hour_and_weekday(bin_start: i64) -> (usize, usize) {
    let day = bin_start.div_euclid(DAY);
    let hour = (bin_start.rem_euclid(DAY) / 3_600_000) as usize;
    // 1970-01-01 was a Thursday.
    (hour, (day + 3).rem_euclid(7) as usize)
}

impl HistoryService {
    pub(super) async fn seasonality_inner(
        self: Arc<Self>,
        req: SeasonalityRequest,
    ) -> Result<SeasonalityResponse, Status> {
        let group_by = match GroupBy::try_from(req.group_by) {
            Ok(g @ (GroupBy::Exchange | GroupBy::Symbol | GroupBy::BaseAsset)) => g,
            _ => {
                return Err(Status::invalid_argument(
                    "group_by must be EXCHANGE, SYMBOL or BASE_ASSET",
                ));
            }
        };
        let days = if req.lookback_days == 0 {
            DEFAULT_LOOKBACK_DAYS
        } else {
            req.lookback_days
        };
        if days > MAX_LOOKBACK_DAYS {
            return Err(self.reject(
                Reject::Range,
                Status::out_of_range(format!("lookback_days {days} is above {MAX_LOOKBACK_DAYS}")),
            ));
        }
        if u64::from(days) * 24 > u64::from(self.cfg.max_bins) {
            return Err(self.reject(
                Reject::Bins,
                Status::out_of_range(format!(
                    "{days} days are {} hourly bins, the limit is {} (history.max_bins); use a shorter lookback",
                    days * 24,
                    self.cfg.max_bins
                )),
            ));
        }
        if req.to_ts < 0 {
            return Err(Status::invalid_argument("to_ts must not be negative"));
        }
        let scope = scope_of(&req.exchanges, &req.symbols, &req.base_assets)?;
        let step = BinStep::Hour1;
        let end_ts = if req.to_ts == 0 {
            self.clock.now_ms()
        } else {
            req.to_ts
        };
        // Whole hours: the hour in progress is left out, it would drag every mean down.
        let end = step.start(end_ts);
        let first_bin = end - i64::from(days) * DAY;
        let metrics = vec![Metric::LiqSumUsd, Metric::OiChangePct];
        let needs = Needs::of(&metrics);
        let lo = step.prev(first_bin);
        let Collected { fine, gaps, source } = self
            .collect_or_fallback(&scope, step, lo, end, needs)
            .await?;
        let rolled = rollup(
            &fine,
            &gaps,
            &RollupParams {
                group_by,
                step,
                first_bin,
                end,
                metrics,
                max_rows: self.cfg.max_aggregate_rows as usize,
            },
        )
        .map_err(|e| {
            self.reject(
                Reject::Rows,
                Status::resource_exhausted(format!(
                    "{e} (history.max_aggregate_rows); filter by exchange, symbol or base asset"
                )),
            )
        })?;
        type Pair = (Vec<Sums>, Vec<Sums>);
        let mut groups: BTreeMap<String, Pair> = BTreeMap::new();
        let mut covered_days: BTreeSet<i64> = BTreeSet::new();
        let mut any_gap = false;
        for row in &rolled.rows {
            let value = |m: Metric| {
                row.values
                    .iter()
                    .find(|v| v.metric == m as i32)
                    .and_then(|v| v.value)
            };
            let (liq, oi) = (value(Metric::LiqSumUsd), value(Metric::OiChangePct));
            if liq.is_none() && oi.is_none() {
                continue;
            }
            covered_days.insert(row.bucket_start.div_euclid(DAY));
            any_gap |= row.partial;
            let (h, w) = hour_and_weekday(row.bucket_start);
            let entry = groups
                .entry(row.group.clone())
                .or_insert_with(|| (vec![Sums::default(); 24], vec![Sums::default(); 7]));
            entry.0[h].add(liq, oi);
            entry.1[w].add(liq, oi);
        }
        let days_covered = covered_days.len() as u32;
        let mut weekday_missing = false;
        let profiles = groups
            .into_iter()
            .map(|(group, (by_hour, by_weekday))| {
                weekday_missing |= by_weekday.iter().any(|s| s.n == 0);
                SeasonalityProfile {
                    group,
                    by_hour: by_hour
                        .iter()
                        .enumerate()
                        .map(|(i, s)| s.bucket(i as u32))
                        .collect(),
                    by_weekday: by_weekday
                        .iter()
                        .enumerate()
                        .map(|(i, s)| s.bucket(i as u32))
                        .collect(),
                }
            })
            .collect::<Vec<_>>();
        self.stats.bins(rolled.rows.len() as u64);
        Ok(SeasonalityResponse {
            profiles,
            days_covered,
            lookback_days: days,
            partial: days_covered < days || weekday_missing || any_gap || rolled.rows.is_empty(),
            from_ts: first_bin,
            to_ts: end,
            source: source.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hour_and_weekday_follow_utc_and_start_on_monday() {
        // 2026-10-26 (Monday) 00:00 UTC.
        let monday = 1_792_972_800_000;
        assert_eq!(hour_and_weekday(monday), (0, 0));
        assert_eq!(hour_and_weekday(monday + 5 * 3_600_000), (5, 0));
        assert_eq!(hour_and_weekday(monday + DAY + 23 * 3_600_000), (23, 1));
        assert_eq!(hour_and_weekday(monday + 6 * DAY), (0, 6));
        assert_eq!(hour_and_weekday(monday - 3_600_000), (23, 6));
        assert_eq!(hour_and_weekday(0), (0, 3), "the epoch was a Thursday");
    }

    #[test]
    fn buckets_report_means_and_nulls() {
        let mut s = Sums::default();
        s.add(Some(100.0), Some(1.0));
        s.add(Some(300.0), Some(-3.0));
        s.add(None, None);
        let b = s.bucket(4);
        assert_eq!((b.index, b.samples), (4, 3));
        assert_eq!(b.mean_liq_usd, Some(200.0));
        assert_eq!(b.mean_oi_change_pct, Some(-1.0));
        assert_eq!(b.mean_abs_oi_change_pct, Some(2.0));
        let empty = Sums::default().bucket(0);
        assert_eq!((empty.samples, empty.mean_liq_usd), (0, None));
    }
}
