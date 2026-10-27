//! `History.Aggregate`: binned liquidation and open interest metrics from ClickHouse and the unflushed Redis tail.
//!
//! # Bins
//!
//! Steps are 1m, 5m, 15m, 1h, 4h, 1d, 1w and 1M, aligned to UTC (a week starts on Monday). The period is widened to
//! whole bins: the answer covers `[bins_from_ts, bins_to_ts)`. A call may ask for at most `history.max_bins` bins
//! per group and `history.max_aggregate_rows` rows in total.
//!
//! # Hot and cold
//!
//! The cold side (ClickHouse `GROUP BY` on the interval, cells per exchange, symbol and bin) is answered for every
//! bin before the first bin the Redis tail touches, and for the bins from there on only for rows with a stream ID up
//! to the flusher's watermark; the tail (entries after the watermark) is aggregated here into cells of the same shape
//! and merged into them (sums add, last values take the later sample, first values the earlier, high and low merge).
//! The roll-up to groups happens afterwards on the merged cells, so the boundary bin is exact whatever the metric:
//! see [`super::agg`].
//!
//! # Definitions
//!
//! * Base asset: the part of the normalized symbol before the first `-` (`BTC` for `BTC-USDT-PERP`).
//! * OI end level: the last OI sample of the bin. OI change of a bin: its end level minus the end level of the bin
//!   right before it, per series, summed over the series of the group; in USD it is priced at the bin's last mark
//!   price. A series without data in the bin before contributes nothing and makes the row partial when the series
//!   has older data (a hole).
//! * Mark price open/high/low/close come from the mark prices of the 30-second OI samples: an approximation of a
//!   candle, not exchange candles. Undefined (null) when grouped by exchange.
//! * Total OI of a base asset: the sum of the end levels in USD of every series of that base asset that passes the
//!   filters (all exchanges when none is given).
//! * Liquidation share of OI: liquidated USD of the bin divided by the end OI in USD of the group.
//! * `partial`: the bin overlaps a `Gap` of a source the requested metrics depend on, or is empty inside the span
//!   of data of its group. Values of an empty bin are null; a bin without liquidations but with OI data counts 0.

use tonic::Status;
use vulture_proto::v1::{AggregateRequest, AggregateResponse, GroupBy, Metric};

use super::agg::{DEFAULT_METRICS, FineData, GapRec, Needs, RollupError, RollupParams, rollup};
use super::bins::BinStep;
use super::cold::{FineQuery, GapQuery, IdBound, Scope};
use super::event::in_period;
use super::service::{HistoryService, ch_status, scope_of, store_status};
use super::stats::Reject;
use super::tail::{TailPlan, after_watermark, id_for_time, max_id, scan};
use crate::history::ChError;
use crate::store::{TYPE_GAP, TYPE_LIQUIDATION, TYPE_OI};
use vulture_proto::v1::event::Payload;

/// A validated `Aggregate`.
struct Plan {
    group_by: GroupBy,
    step: BinStep,
    first_bin: i64,
    end: i64,
    metrics: Vec<Metric>,
    scope: Scope,
    needs: Needs,
}

/// The cells and gaps of a period, and where they came from.
pub(super) struct Collected {
    pub fine: FineData,
    pub gaps: Vec<GapRec>,
    pub source: &'static str,
}

/// A fetch failure.
pub(super) enum Fetch {
    Cold(ChError),
    Status(Status),
}

impl From<Status> for Fetch {
    fn from(s: Status) -> Self {
        Fetch::Status(s)
    }
}

impl HistoryService {
    pub(super) async fn aggregate_inner(
        self: std::sync::Arc<Self>,
        req: AggregateRequest,
    ) -> Result<AggregateResponse, Status> {
        if let Some(q) = req.oi_movers {
            return self.movers_inner(&req, q).await;
        }
        let plan = self.plan_aggregate(&req)?;
        let lo = if plan.needs.oi {
            plan.step.prev(plan.first_bin)
        } else {
            plan.first_bin
        };
        let collected = self
            .collect_or_fallback(&plan.scope, plan.step, lo, plan.end, plan.needs)
            .await?;
        let params = RollupParams {
            group_by: plan.group_by,
            step: plan.step,
            first_bin: plan.first_bin,
            end: plan.end,
            metrics: plan.metrics.clone(),
            max_rows: self.cfg.max_aggregate_rows as usize,
        };
        let rolled = rollup(&collected.fine, &collected.gaps, &params).map_err(|e| match e {
            RollupError::TooManyRows { rows, max } => self.reject(
                Reject::Rows,
                Status::resource_exhausted(format!(
                    "the answer would have {rows} rows, the limit is {max} (history.max_aggregate_rows); use a larger step, a shorter period or filter by exchange, symbol or base asset"
                )),
            ),
        })?;
        self.stats.bins(rolled.rows.len() as u64);
        Ok(AggregateResponse {
            rows: rolled.rows,
            partial: rolled.partial,
            oi_movers: None,
            bins_from_ts: plan.first_bin,
            bins_to_ts: plan.end,
            source: collected.source.to_string(),
        })
    }

    /// [`collect`](Self::collect) from ClickHouse and the tail; if ClickHouse fails and the Redis window covers the
    /// period, from Redis alone.
    pub(super) async fn collect_or_fallback(
        &self,
        scope: &Scope,
        step: BinStep,
        lo: i64,
        end: i64,
        needs: Needs,
    ) -> Result<Collected, Status> {
        match self.collect(scope, step, lo, end, needs, true).await {
            Ok(c) => Ok(c),
            Err(Fetch::Status(s)) => Err(s),
            Err(Fetch::Cold(e)) => {
                self.stats.clickhouse_error();
                if self.redis_covers(lo).await {
                    tracing::warn!(error = %e, "ClickHouse failed; answering from the Redis hot window");
                    self.stats.redis_fallback();
                    match self.collect(scope, step, lo, end, needs, false).await {
                        Ok(c) => Ok(c),
                        Err(Fetch::Status(s)) => Err(s),
                        Err(Fetch::Cold(e)) => Err(ch_status(&e)),
                    }
                } else {
                    Err(ch_status(&e))
                }
            }
        }
    }

    fn plan_aggregate(&self, req: &AggregateRequest) -> Result<Plan, Status> {
        let group_by = match GroupBy::try_from(req.group_by) {
            Ok(g @ (GroupBy::Exchange | GroupBy::Symbol | GroupBy::BaseAsset)) => g,
            _ => {
                return Err(Status::invalid_argument(
                    "group_by must be EXCHANGE, SYMBOL or BASE_ASSET",
                ));
            }
        };
        let step = BinStep::from_proto(req.step).ok_or_else(|| {
            Status::invalid_argument("step must be one of 1m, 5m, 15m, 1h, 4h, 1d, 1w, 1M")
        })?;
        if req.from_ts < 0 || req.to_ts < 0 {
            return Err(Status::invalid_argument(
                "from_ts and to_ts must not be negative",
            ));
        }
        let to_ts = if req.to_ts == 0 {
            self.clock.now_ms() + 1
        } else {
            req.to_ts
        };
        if req.from_ts >= to_ts {
            return Err(Status::invalid_argument(
                "from_ts must be before to_ts (milliseconds since the Unix epoch; to_ts = 0 means now)",
            ));
        }
        let bins = step.count(req.from_ts, to_ts);
        if bins > u64::from(self.cfg.max_bins) {
            return Err(self.reject(
                Reject::Bins,
                Status::out_of_range(format!(
                    "the period has {bins} bins of this step, the limit is {} (history.max_bins); use a larger step or a shorter period",
                    self.cfg.max_bins
                )),
            ));
        }
        let mut metrics = Vec::new();
        for raw in &req.metrics {
            let m = Metric::try_from(*raw)
                .ok()
                .filter(|m| *m != Metric::Unspecified)
                .ok_or_else(|| {
                    Status::invalid_argument(format!("metrics: {raw} is not a known metric"))
                })?;
            if !metrics.contains(&m) {
                metrics.push(m);
            }
        }
        if metrics.is_empty() {
            metrics = DEFAULT_METRICS.to_vec();
        }
        let needs = Needs::of(&metrics);
        let scope = scope_of(&req.exchanges, &req.symbols, &req.base_assets)?;
        Ok(Plan {
            group_by,
            step,
            first_bin: step.start(req.from_ts),
            end: step.next(step.start(to_ts - 1)),
            metrics,
            scope,
            needs,
        })
    }

    /// Cells and gaps of `[lo, end)` from ClickHouse (when `use_cold` and configured) and the Redis tail.
    pub(super) async fn collect(
        &self,
        scope: &Scope,
        step: BinStep,
        lo: i64,
        end: i64,
        needs: Needs,
        use_cold: bool,
    ) -> Result<Collected, Fetch> {
        let cold = if use_cold { self.cold.clone() } else { None };
        let slack = self.slack();
        let wm = match &cold {
            Some(_) => Some(
                self.current_watermark()
                    .await
                    .map_err(|e| store_status(&e))?,
            ),
            None => None,
        };

        // The Redis side.
        let mut tail = FineData::default();
        let mut tail_gaps: Vec<GapRec> = Vec::new();
        // A tail entry is written at about the time of its event; if the watermark is already past the period (by
        // more than the slack) the tail cannot hold anything of it. Gaps are recorded when they end, so they can.
        let tail_needed = match wm {
            Some((wm_ms, _)) => (wm_ms as i64) < end + slack.as_millis() as i64,
            None => true,
        };
        if tail_needed {
            let by_time = id_for_time(lo, slack);
            let start = match wm {
                Some(w) => max_id(&after_watermark(w), &by_time),
                None => by_time,
            };
            let plan = TailPlan {
                start,
                stop_after_ms: cold
                    .is_none()
                    .then(|| (end + slack.as_millis() as i64).max(0) as u64),
                page: self.cfg.tail_page as usize,
                max_entries: self.cfg.tail_max_entries as usize,
            };
            let stats = scan(self.store.as_ref(), &plan, |entry| {
                let wanted = match entry.kind.as_str() {
                    TYPE_LIQUIDATION => needs.liq,
                    TYPE_OI => needs.oi,
                    TYPE_GAP => true,
                    _ => false,
                };
                if !wanted
                    || (entry.kind != TYPE_GAP && !scope.matches(&entry.exchange, &entry.symbol))
                {
                    return;
                }
                let Ok(event) = entry.into_event() else {
                    return;
                };
                match &event.payload {
                    Some(Payload::Liquidation(l)) if l.exchange_ts >= lo && l.exchange_ts < end => {
                        tail.add_liquidation(step, l);
                    }
                    Some(Payload::OpenInterest(o))
                        if o.exchange_ts >= lo && o.exchange_ts < end =>
                    {
                        tail.add_oi(step, o);
                    }
                    Some(Payload::Gap(g)) if in_period(&event, lo, end) => {
                        tail_gaps.push(GapRec {
                            exchange: g.exchange.clone(),
                            stream: g.stream.clone(),
                            from_ts: g.from_ts,
                            to_ts: g.to_ts,
                        });
                    }
                    _ => {}
                }
            })
            .await
            .map_err(|e| store_status(&e))?;
            self.stats.scanned_redis(stats.scanned);
            if stats.truncated {
                return Err(self
                    .reject(
                        Reject::Rows,
                        Status::resource_exhausted(format!(
                            "more than {} Redis entries would have to be read (history.tail_max_entries); {}",
                            self.cfg.tail_max_entries,
                            if cold.is_some() {
                                "the ClickHouse flusher is far behind"
                            } else {
                                "enable ClickHouse history or use a shorter period and filters"
                            }
                        )),
                    )
                    .into());
            }
        }

        // The ClickHouse side.
        let mut fine = FineData::default();
        let mut gaps: Vec<GapRec> = Vec::new();
        if let Some(cold) = &cold {
            let base = FineQuery {
                step,
                from: lo,
                to: end,
                scope: scope.clone(),
                needs,
                id_bound: None,
                max_cells: self.cfg.max_fine_rows as usize,
                max_memory: self.cfg.clickhouse_max_memory_bytes,
                timeout: self.timeout(),
            };
            // Bins the tail does not touch are taken whole; from the first bin it touches on, only the rows the
            // flusher has acknowledged, so the tail's rows are not counted twice.
            let boundary = tail.first_bin().map(|b| b.max(lo));
            let mut queries = Vec::new();
            match (boundary, wm) {
                (Some(b), Some((ms, seq))) => {
                    if b > lo {
                        queries.push(FineQuery {
                            to: b,
                            ..base.clone()
                        });
                    }
                    queries.push(FineQuery {
                        from: b,
                        id_bound: Some(IdBound { ms, seq }),
                        ..base
                    });
                }
                _ => queries.push(base),
            }
            for q in queries {
                let answer = cold.fine(q).await.map_err(Fetch::Cold)?;
                self.stats.scanned_clickhouse(answer.scanned);
                if answer.truncated {
                    return Err(self
                        .reject(
                            Reject::Rows,
                            Status::resource_exhausted(format!(
                                "more than {} cells (series times bins) would be read (history.max_fine_rows); use a larger step, a shorter period or filter by exchange, symbol or base asset",
                                self.cfg.max_fine_rows
                            )),
                        )
                        .into());
                }
                fine.merge(answer.data);
            }
            gaps = cold
                .gaps(GapQuery {
                    from: lo,
                    to: end,
                    exchanges: scope.exchanges.clone(),
                    timeout: self.timeout(),
                })
                .await
                .map_err(Fetch::Cold)?;
        }
        let merged_tail = !tail.is_empty();
        fine.merge(tail);
        gaps.extend(tail_gaps.into_iter().filter(|g| {
            scope.exchanges.is_empty() || g.exchange == "*" || scope.exchanges.contains(&g.exchange)
        }));
        let source = match (&cold, merged_tail) {
            (Some(_), true) => {
                self.stats.tail_merge();
                "clickhouse+redis"
            }
            (Some(_), false) => "clickhouse",
            (None, _) => "redis",
        };
        Ok(Collected { fine, gaps, source })
    }
}
