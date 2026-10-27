//! The OI movers ranking (`AggregateRequest.oi_movers`).
//!
//! The change of a symbol over the window is **computed by the signal detector's own function**,
//! [`Series::change`](crate::alerts::series::Series::change): the sample nearest to `now - window` within the
//! tolerance `window/8` (15 s to 300 s), no reference across a hole or a `Gap`, `now` being the symbol's newest
//! sample. The ranking therefore never contradicts a signal. The samples come from ClickHouse (only those near the
//! reference point, the newest one, and the time of the latest pause, all computed by one `GROUP BY` so that the
//! whole history of 1700 series is not transferred) and from the Redis tail; they are put into a `Series` with the
//! same limits the live engine uses (`alerts.sample_spacing_secs`, `alerts.hole_secs`), `Gap` records become the
//! exchange-level break as in the engine.
//!
//! A symbol for which the detector would refuse to evaluate the window (a hole or a gap after the reference
//! point) but for which a change can still be computed ignoring them is listed with `partial = true`; request
//! `exclude_partial` to leave those out.

use std::collections::{BTreeMap, HashMap};

use tonic::Status;
use vulture_proto::v1::{
    AggregateRequest, AggregateResponse, OiMover, OiMoverSort, OiMovers, OiMoversQuery,
    event::Payload,
};

use super::agg::{GapRec, SeriesKey};
use super::cold::{GapQuery, MoverAnswer, MoverInput, MoverQuery, Scope};
use super::service::{HistoryService, SeriesParams, ch_status, scope_of, store_status};
use super::stats::Reject;
use super::tail::{TailPlan, after_watermark, id_for_time, max_id, scan};
use crate::alerts::series::{Sample, Series, SeriesLimits, tolerance_ms};
use crate::history::ChError;
use crate::store::{TYPE_GAP, TYPE_OI};

/// Shortest and longest window of the ranking, seconds.
pub const MIN_WINDOW_SECS: u32 = 60;
/// See [`MIN_WINDOW_SECS`].
pub const MAX_WINDOW_SECS: u32 = 86_400;

/// Everything known about the samples of one series.
#[derive(Debug, Clone, Default)]
pub struct SeriesInput {
    /// Samples near the reference point, from the cold store.
    pub candidates: Vec<Sample>,
    /// The newest cold sample.
    pub cold_last: Option<Sample>,
    /// Latest time that followed a pause in the cold samples (0 = none).
    pub cold_hole_ts: i64,
    /// Samples of the Redis tail.
    pub tail: Vec<Sample>,
}

/// The exchange-level breaks the live engine would have: the latest `to_ts` of a non-liquidation gap per exchange,
/// and of the writer's own gaps for all.
#[derive(Debug, Clone, Default)]
pub struct Breaks {
    per_exchange: HashMap<String, i64>,
    global: i64,
}

impl Breaks {
    /// Collects the breaks from gap records that ended at or before `end`.
    pub fn from_gaps(gaps: &[GapRec], end: i64) -> Self {
        let mut b = Breaks::default();
        for g in gaps.iter().filter(|g| g.to_ts <= end) {
            if g.stream == "liquidations" {
                continue;
            }
            if g.exchange == "*" {
                b.global = b.global.max(g.to_ts);
            } else {
                let e = b.per_exchange.entry(g.exchange.clone()).or_insert(0);
                *e = (*e).max(g.to_ts);
            }
        }
        b
    }

    /// The break in force for an exchange.
    pub fn for_exchange(&self, exchange: &str) -> i64 {
        self.global
            .max(self.per_exchange.get(exchange).copied().unwrap_or(0))
    }
}

/// What the detector's function says about one series.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Evaluated {
    /// The newest sample.
    pub now: Sample,
    /// The reference sample.
    pub reference: Sample,
    /// `now.oi_base - reference.oi_base`.
    pub abs_base: f64,
    /// Percent of the reference.
    pub pct: f64,
    /// The detector would refuse the window (a hole or a gap): the change ignores them.
    pub partial: bool,
}

fn wide_limits(spacing_ms: i64, hole_ms: i64) -> SeriesLimits {
    SeriesLimits {
        retention_ms: i64::MAX / 4,
        max_samples: usize::MAX / 4,
        spacing_ms,
        hole_ms,
    }
}

/// Builds the series the live engine would hold for these samples and asks it for the change over `window_ms`.
/// With `strict` the holes and gaps count; without, they are ignored (the estimate used for `partial` entries).
pub fn build_series(input: &SeriesInput, params: SeriesParams, strict: bool) -> Series {
    let cold = wide_limits(params.spacing_ms, i64::MAX);
    let hot = wide_limits(
        params.spacing_ms,
        if strict { params.hole_ms } else { i64::MAX },
    );
    let mut series = Series::new();
    for s in &input.candidates {
        series.push(*s, &cold);
    }
    if strict && input.cold_hole_ts > 0 {
        series.mark_break(input.cold_hole_ts);
    }
    if let Some(last) = input.cold_last {
        series.push(last, &cold);
    }
    for s in &input.tail {
        series.push(*s, &hot);
    }
    series
}

/// Evaluates one series with the detector's function.
pub fn evaluate(
    input: &SeriesInput,
    params: SeriesParams,
    window_ms: i64,
    extra_break: i64,
) -> Option<Evaluated> {
    let strict = build_series(input, params, true);
    if let Some(c) = strict.change(window_ms, extra_break) {
        return Some(Evaluated {
            now: c.now,
            reference: c.reference,
            abs_base: c.abs_base,
            pct: c.pct,
            partial: false,
        });
    }
    let relaxed = build_series(input, params, false);
    relaxed.change(window_ms, 0).map(|c| Evaluated {
        now: c.now,
        reference: c.reference,
        abs_base: c.abs_base,
        pct: c.pct,
        partial: true,
    })
}

/// Current OI in USD of a sample, as the detector computes it (`oi_usd`, else `oi_base * mark`).
pub fn oi_usd_of(s: &Sample) -> Option<f64> {
    if s.oi_usd.is_finite() {
        Some(s.oi_usd)
    } else if s.mark.is_finite() {
        Some(s.oi_base * s.mark)
    } else {
        None
    }
}

/// Ranking options.
#[derive(Debug, Clone, Copy)]
pub struct RankOptions {
    /// Entries per list.
    pub limit: usize,
    /// Sort key.
    pub sort_by: OiMoverSort,
    /// Ignore symbols whose current OI is below this many USD.
    pub min_oi_usd: f64,
    /// Leave out entries computed ignoring a hole or a gap.
    pub exclude_partial: bool,
}

/// Evaluates every series and builds the two lists.
pub fn rank(
    series: &BTreeMap<SeriesKey, SeriesInput>,
    params: SeriesParams,
    window_ms: i64,
    breaks: &Breaks,
    min_last: i64,
    opts: RankOptions,
) -> (Vec<OiMover>, Vec<OiMover>) {
    let mut all: Vec<OiMover> = Vec::new();
    for (key, input) in series {
        let Some(ev) = evaluate(input, params, window_ms, breaks.for_exchange(&key.0)) else {
            continue;
        };
        if ev.now.ts < min_last || (opts.exclude_partial && ev.partial) {
            continue;
        }
        let usd = oi_usd_of(&ev.now);
        if opts.min_oi_usd > 0.0 && !usd.is_some_and(|u| u >= opts.min_oi_usd) {
            continue;
        }
        all.push(OiMover {
            exchange: key.0.clone(),
            symbol: key.1.clone(),
            oi_base_start: ev.reference.oi_base,
            oi_base_end: ev.now.oi_base,
            change_abs: ev.abs_base,
            change_pct: ev.pct,
            oi_usd_end: usd,
            partial: ev.partial,
        });
    }
    let key = |m: &OiMover| match opts.sort_by {
        OiMoverSort::AbsBase => m.change_abs,
        _ => m.change_pct,
    };
    let mut growth: Vec<OiMover> = all.iter().filter(|m| m.change_abs > 0.0).cloned().collect();
    let mut decline: Vec<OiMover> = all.into_iter().filter(|m| m.change_abs < 0.0).collect();
    growth.sort_by(|a, b| {
        key(b)
            .total_cmp(&key(a))
            .then_with(|| a.symbol.cmp(&b.symbol))
    });
    decline.sort_by(|a, b| {
        key(a)
            .total_cmp(&key(b))
            .then_with(|| a.symbol.cmp(&b.symbol))
    });
    growth.truncate(opts.limit);
    decline.truncate(opts.limit);
    (growth, decline)
}

impl HistoryService {
    pub(super) async fn movers_inner(
        &self,
        req: &AggregateRequest,
        q: OiMoversQuery,
    ) -> Result<AggregateResponse, Status> {
        if q.window_secs < MIN_WINDOW_SECS || q.window_secs > MAX_WINDOW_SECS {
            return Err(Status::invalid_argument(format!(
                "oi_movers.window_secs must be between {MIN_WINDOW_SECS} (1 minute) and {MAX_WINDOW_SECS} (24 hours)"
            )));
        }
        if !(q.min_oi_usd.is_finite() && q.min_oi_usd >= 0.0) {
            return Err(Status::invalid_argument(
                "oi_movers.min_oi_usd must be zero or positive",
            ));
        }
        let sort_by = OiMoverSort::try_from(q.sort_by).map_err(|_| {
            Status::invalid_argument(format!(
                "oi_movers.sort_by: {} is not a known sort",
                q.sort_by
            ))
        })?;
        let limit = if q.limit == 0 {
            self.cfg.movers_default_limit
        } else {
            q.limit
        };
        if limit > self.cfg.movers_max_limit {
            return Err(self.reject(
                Reject::Limit,
                Status::resource_exhausted(format!(
                    "oi_movers.limit {limit} is above {} (history.movers_max_limit)",
                    self.cfg.movers_max_limit
                )),
            ));
        }
        if req.to_ts < 0 {
            return Err(Status::invalid_argument("to_ts must not be negative"));
        }
        let scope = scope_of(&req.exchanges, &req.symbols, &req.base_assets)?;
        let end = if req.to_ts == 0 {
            self.clock.now_ms()
        } else {
            req.to_ts
        };
        let window_ms = i64::from(q.window_secs) * 1000;
        let opts = RankOptions {
            limit: limit as usize,
            sort_by,
            min_oi_usd: q.min_oi_usd,
            exclude_partial: q.exclude_partial,
        };
        let (series, gaps, source) = match self.movers_data(&scope, end, window_ms, true).await {
            Ok(d) => d,
            Err(MoversFetch::Status(s)) => return Err(s),
            Err(MoversFetch::Cold(e)) => {
                self.stats.clickhouse_error();
                let from = end - window_ms - self.stale_ms() - tolerance_ms(window_ms);
                if self.redis_covers(from).await {
                    tracing::warn!(error = %e, "ClickHouse failed; answering the OI movers from the Redis hot window");
                    self.stats.redis_fallback();
                    match self.movers_data(&scope, end, window_ms, false).await {
                        Ok(d) => d,
                        Err(MoversFetch::Status(s)) => return Err(s),
                        Err(MoversFetch::Cold(e)) => return Err(ch_status(&e)),
                    }
                } else {
                    return Err(ch_status(&e));
                }
            }
        };
        let breaks = Breaks::from_gaps(&gaps, end);
        let (top_growth, top_decline) = rank(
            &series,
            self.series,
            window_ms,
            &breaks,
            end - self.stale_ms(),
            opts,
        );
        self.stats
            .movers((top_growth.len() + top_decline.len()) as u64);
        let partial = top_growth.iter().chain(&top_decline).any(|m| m.partial);
        Ok(AggregateResponse {
            rows: Vec::new(),
            partial,
            oi_movers: Some(OiMovers {
                top_growth,
                top_decline,
                window_start_ts: end - window_ms,
                window_end_ts: end,
            }),
            bins_from_ts: 0,
            bins_to_ts: 0,
            source: source.to_string(),
        })
    }

    fn stale_ms(&self) -> i64 {
        (self.cfg.movers_max_stale_secs as i64) * 1000
    }

    async fn movers_data(
        &self,
        scope: &Scope,
        end: i64,
        window_ms: i64,
        use_cold: bool,
    ) -> Result<(BTreeMap<SeriesKey, SeriesInput>, Vec<GapRec>, &'static str), MoversFetch> {
        let cold = if use_cold { self.cold.clone() } else { None };
        let slack = self.slack();
        let tol = tolerance_ms(window_ms);
        let stale = self.stale_ms();
        let from = end - window_ms - stale - tol;
        let mut series: BTreeMap<SeriesKey, SeriesInput> = BTreeMap::new();
        let mut gaps: Vec<GapRec> = Vec::new();

        let wm = match &cold {
            Some(_) => Some(
                self.current_watermark()
                    .await
                    .map_err(|e| store_status(&e))?,
            ),
            None => None,
        };
        // The tail: Redis entries after the watermark (or the whole window without ClickHouse).
        let by_time = id_for_time(from, slack);
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
        let mut tail_samples = 0u64;
        let stats = scan(self.store.as_ref(), &plan, |entry| {
            if entry.kind != TYPE_OI && entry.kind != TYPE_GAP {
                return;
            }
            if entry.kind == TYPE_OI && !scope.matches(&entry.exchange, &entry.symbol) {
                return;
            }
            let Ok(event) = entry.into_event() else {
                return;
            };
            match event.payload {
                Some(Payload::OpenInterest(o)) if o.recv_ts >= from && o.recv_ts <= end => {
                    tail_samples += 1;
                    series
                        .entry((o.exchange, o.symbol))
                        .or_default()
                        .tail
                        .push(Sample {
                            ts: o.recv_ts,
                            oi_base: o.oi_base,
                            oi_usd: o.oi_usd.unwrap_or(f64::NAN),
                            mark: o.mark_price.unwrap_or(f64::NAN),
                        });
                }
                Some(Payload::Gap(g)) if g.to_ts >= from && g.from_ts <= end => gaps.push(GapRec {
                    exchange: g.exchange,
                    stream: g.stream,
                    from_ts: g.from_ts,
                    to_ts: g.to_ts,
                }),
                _ => {}
            }
        })
        .await
        .map_err(|e| store_status(&e))?;
        self.stats.scanned_redis(stats.scanned);
        if stats.truncated {
            return Err(MoversFetch::Status(self.reject(
                Reject::Rows,
                Status::resource_exhausted(format!(
                    "more than {} Redis entries would have to be read (history.tail_max_entries); {}",
                    self.cfg.tail_max_entries,
                    if cold.is_some() {
                        "the ClickHouse flusher is far behind"
                    } else {
                        "enable ClickHouse history or use a shorter window"
                    }
                )),
            )));
        }
        for input in series.values_mut() {
            input.tail.sort_by_key(|s| s.ts);
        }

        if let Some(cold) = &cold {
            // A series is active when its newest sample, cold or tail, is recent. The tail can revive a series
            // whose cold part ends early only if the cold part ends at the watermark, so look that far back.
            let mut min_last = end - stale;
            if let Some((wm_ms, _)) = wm {
                min_last = min_last.min(wm_ms as i64 - stale);
            }
            let MoverAnswer {
                series: cold_series,
                scanned,
            } = cold
                .movers(MoverQuery {
                    scope: scope.clone(),
                    end,
                    from,
                    cand_from: from,
                    cand_to: end - window_ms + tol,
                    min_last,
                    hole_ms: self.series.hole_ms,
                    max_memory: self.cfg.clickhouse_max_memory_bytes,
                    timeout: self.timeout(),
                })
                .await
                .map_err(MoversFetch::Cold)?;
            self.stats.scanned_clickhouse(scanned);
            for MoverInput {
                key,
                last,
                candidates,
                hole_ts,
            } in cold_series
            {
                let entry = series.entry(key).or_default();
                entry.candidates = candidates;
                entry.cold_last = Some(last);
                entry.cold_hole_ts = hole_ts;
            }
            gaps.extend(
                cold.gaps(GapQuery {
                    from,
                    to: end + 1,
                    exchanges: scope.exchanges.clone(),
                    timeout: self.timeout(),
                })
                .await
                .map_err(MoversFetch::Cold)?,
            );
        }
        let source = match (&cold, tail_samples > 0) {
            (Some(_), true) => {
                self.stats.tail_merge();
                "clickhouse+redis"
            }
            (Some(_), false) => "clickhouse",
            (None, _) => "redis",
        };
        Ok((series, gaps, source))
    }
}

enum MoversFetch {
    Cold(ChError),
    Status(Status),
}

impl From<Status> for MoversFetch {
    fn from(s: Status) -> Self {
        MoversFetch::Status(s)
    }
}
