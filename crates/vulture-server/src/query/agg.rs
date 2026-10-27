//! Accumulators of `History.Aggregate` and the roll-up of fine cells into answer rows.
//!
//! A *cell* is one (exchange, symbol, bin). The cold store (ClickHouse `GROUP BY`) and the Redis tail (aggregated
//! here in Rust with [`FineData::add_oi`] and [`FineData::add_liquidation`]) both produce cells; cells of one
//! bin from both sides are combined with [`OiBin::merge`] / [`LiqBin::merge`]. The roll-up then groups the cells by
//! exchange, symbol or base asset and computes the metrics. Keeping the merge at the cell level is what makes the
//! boundary bin exact: sums add, "last" values take the later sample, "first" values the earlier one, high/low
//! merge, and the OI change of a bin is computed afterwards from the merged end levels.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use vulture_proto::v1::{
    AggregateRow, Divergence, GroupBy, Liquidation, Metric, MetricValue, OpenInterest, Regime, Side,
};

use super::bins::BinStep;

/// (exchange, normalized symbol).
pub type SeriesKey = (String, String);

/// A value with the time it was observed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Stamped {
    /// Observation time, ms.
    pub ts: i64,
    /// The value.
    pub v: f64,
}

/// The later of two optional observations; on a tie `new` wins (it was merged later).
fn later(old: Option<Stamped>, new: Option<Stamped>) -> Option<Stamped> {
    match (old, new) {
        (Some(a), Some(b)) => Some(if b.ts >= a.ts { b } else { a }),
        (a, b) => b.or(a),
    }
}

/// The earlier of two optional observations; on a tie the old one stays.
fn earlier(old: Option<Stamped>, new: Option<Stamped>) -> Option<Stamped> {
    match (old, new) {
        (Some(a), Some(b)) => Some(if b.ts < a.ts { b } else { a }),
        (a, b) => a.or(b),
    }
}

fn max_opt(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (x, y) => x.or(y),
    }
}

fn min_opt(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, y) => x.or(y),
    }
}

/// Liquidations of one cell.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LiqBin {
    /// Number of liquidations.
    pub count: u64,
    /// Sum of notionals, USD.
    pub sum: f64,
    /// Sum for liquidated longs, USD.
    pub long: f64,
    /// Sum for liquidated shorts, USD.
    pub short: f64,
    /// Largest single notional, USD.
    pub max: f64,
}

impl LiqBin {
    /// Adds one liquidation.
    pub fn add(&mut self, side: i32, notional_usd: f64) {
        self.count += 1;
        self.sum += notional_usd;
        match Side::try_from(side) {
            Ok(Side::Long) => self.long += notional_usd,
            Ok(Side::Short) => self.short += notional_usd,
            _ => {}
        }
        self.max = self.max.max(notional_usd);
    }

    /// Combines two parts of the same cell (sums add, the maximum is the larger one).
    pub fn merge(&mut self, o: &LiqBin) {
        self.count += o.count;
        self.sum += o.sum;
        self.long += o.long;
        self.short += o.short;
        self.max = self.max.max(o.max);
    }
}

/// Open interest samples of one cell: first and last observations, mark price candle.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct OiBin {
    /// Samples in the cell.
    pub n: u64,
    /// Time of the first sample, ms.
    pub first_ts: i64,
    /// Time of the last sample, ms.
    pub last_ts: i64,
    /// `oi_base` of the last sample.
    pub last_base: f64,
    /// Last known `oi_usd` and when.
    pub last_usd: Option<Stamped>,
    /// Earliest known mark price.
    pub mark_open: Option<Stamped>,
    /// Latest known mark price.
    pub mark_close: Option<Stamped>,
    /// Highest mark price.
    pub mark_high: Option<f64>,
    /// Lowest mark price.
    pub mark_low: Option<f64>,
    /// Last known funding rate and when.
    pub funding_last: Option<Stamped>,
    /// Sum of the funding rates of the samples that had one.
    pub funding_sum: f64,
    /// Number of those samples.
    pub funding_n: u64,
    /// Last known basis in percent (`100 * (mark - index) / index`) and when.
    pub basis_last: Option<Stamped>,
    /// Sum of the basis of the samples that had mark and index prices.
    pub basis_sum: f64,
    /// Number of those samples.
    pub basis_n: u64,
}

/// Basis in percent from a mark and an index price; `None` unless both are usable.
pub fn basis_pct(mark: Option<f64>, index: Option<f64>) -> Option<f64> {
    let (m, i) = (usable_price(mark)?, usable_price(index)?);
    Some(100.0 * (m - i) / i)
}

fn usable_price(p: Option<f64>) -> Option<f64> {
    p.filter(|v| v.is_finite() && *v > 0.0)
}

impl OiBin {
    /// Adds one sample without funding data.
    pub fn add_sample(&mut self, ts: i64, base: f64, usd: Option<f64>, mark: Option<f64>) {
        self.add_full_sample(ts, base, usd, mark, None, None);
    }

    /// Adds one sample.
    pub fn add_full_sample(
        &mut self,
        ts: i64,
        base: f64,
        usd: Option<f64>,
        mark: Option<f64>,
        funding: Option<f64>,
        index: Option<f64>,
    ) {
        if let Some(f) = funding.filter(|f| f.is_finite()) {
            self.funding_last = later(self.funding_last, Some(Stamped { ts, v: f }));
            self.funding_sum += f;
            self.funding_n += 1;
        }
        if let Some(b) = basis_pct(mark, index) {
            self.basis_last = later(self.basis_last, Some(Stamped { ts, v: b }));
            self.basis_sum += b;
            self.basis_n += 1;
        }
        if self.n == 0 {
            self.first_ts = ts;
            self.last_ts = ts;
            self.last_base = base;
        } else {
            self.first_ts = self.first_ts.min(ts);
            if ts >= self.last_ts {
                self.last_ts = ts;
                self.last_base = base;
            }
        }
        self.n += 1;
        if let Some(u) = usd.filter(|v| v.is_finite()) {
            self.last_usd = later(self.last_usd, Some(Stamped { ts, v: u }));
        }
        if let Some(m) = usable_price(mark) {
            let s = Some(Stamped { ts, v: m });
            self.mark_open = earlier(self.mark_open, s);
            self.mark_close = later(self.mark_close, s);
            self.mark_high = max_opt(self.mark_high, Some(m));
            self.mark_low = min_opt(self.mark_low, Some(m));
        }
    }

    /// Combines two parts of the same cell; `o` is the later part in stream order (it wins ties).
    pub fn merge(&mut self, o: &OiBin) {
        if o.n == 0 {
            return;
        }
        if self.n == 0 {
            *self = o.clone();
            return;
        }
        self.n += o.n;
        self.first_ts = self.first_ts.min(o.first_ts);
        if o.last_ts >= self.last_ts {
            self.last_ts = o.last_ts;
            self.last_base = o.last_base;
        }
        self.last_usd = later(self.last_usd, o.last_usd);
        self.mark_open = earlier(self.mark_open, o.mark_open);
        self.mark_close = later(self.mark_close, o.mark_close);
        self.mark_high = max_opt(self.mark_high, o.mark_high);
        self.mark_low = min_opt(self.mark_low, o.mark_low);
        self.funding_last = later(self.funding_last, o.funding_last);
        self.funding_sum += o.funding_sum;
        self.funding_n += o.funding_n;
        self.basis_last = later(self.basis_last, o.basis_last);
        self.basis_sum += o.basis_sum;
        self.basis_n += o.basis_n;
    }
}

/// Cells by series and bin start.
#[derive(Debug, Clone, Default)]
pub struct FineData {
    /// Liquidation cells.
    pub liq: HashMap<SeriesKey, BTreeMap<i64, LiqBin>>,
    /// Open interest cells.
    pub oi: HashMap<SeriesKey, BTreeMap<i64, OiBin>>,
}

impl FineData {
    /// Number of cells.
    pub fn cells(&self) -> usize {
        self.liq.values().map(BTreeMap::len).sum::<usize>()
            + self.oi.values().map(BTreeMap::len).sum::<usize>()
    }

    /// True without any cell.
    pub fn is_empty(&self) -> bool {
        self.cells() == 0
    }

    /// Adds a liquidation to its cell (binned by `exchange_ts`).
    pub fn add_liquidation(&mut self, step: BinStep, l: &Liquidation) {
        let bin = step.start(l.exchange_ts);
        self.liq
            .entry((l.exchange.clone(), l.symbol.clone()))
            .or_default()
            .entry(bin)
            .or_default()
            .add(l.side, l.notional_usd);
    }

    /// Adds an open interest sample to its cell (binned by `exchange_ts`).
    pub fn add_oi(&mut self, step: BinStep, o: &OpenInterest) {
        if !o.oi_base.is_finite() {
            return;
        }
        let bin = step.start(o.exchange_ts);
        self.oi
            .entry((o.exchange.clone(), o.symbol.clone()))
            .or_default()
            .entry(bin)
            .or_default()
            .add_full_sample(
                o.exchange_ts,
                o.oi_base,
                o.oi_usd,
                o.mark_price,
                o.funding_rate,
                o.index_price,
            );
    }

    /// Merges `later` (the newer side, for example the Redis tail) into `self`.
    pub fn merge(&mut self, later: FineData) {
        for (key, bins) in later.liq {
            let mine = self.liq.entry(key).or_default();
            for (bin, cell) in bins {
                mine.entry(bin).or_default().merge(&cell);
            }
        }
        for (key, bins) in later.oi {
            let mine = self.oi.entry(key).or_default();
            for (bin, cell) in bins {
                mine.entry(bin).or_default().merge(&cell);
            }
        }
    }

    /// Smallest event time among the cells of the given bins: used to find where the tail starts.
    pub fn first_bin(&self) -> Option<i64> {
        self.liq
            .values()
            .filter_map(|b| b.keys().next().copied())
            .chain(self.oi.values().filter_map(|b| b.keys().next().copied()))
            .min()
    }
}

/// A source interruption, as stored in `gaps`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapRec {
    /// Exchange (`*` for gaps of the writer itself).
    pub exchange: String,
    /// Stream name (`liquidations`, `open_interest`, ...).
    pub stream: String,
    /// Last event before the interruption, ms.
    pub from_ts: i64,
    /// Recovery time, ms.
    pub to_ts: i64,
}

/// The base asset of a normalized symbol: the part before the first `-` (`BTC` for `BTC-USDT-PERP`). A symbol
/// without a dash is its own base asset.
pub fn base_asset(symbol: &str) -> &str {
    symbol.split_once('-').map_or(symbol, |(base, _)| base)
}

/// The metrics every `Aggregate` call computes when the request names none.
pub const DEFAULT_METRICS: [Metric; 15] = [
    Metric::LiqSumUsd,
    Metric::LiqCount,
    Metric::LiqLongUsd,
    Metric::LiqShortUsd,
    Metric::LiqMaxUsd,
    Metric::OiChangeBase,
    Metric::OiChangeUsd,
    Metric::OiTotalUsd,
    Metric::LiqToOiRatio,
    Metric::OiEndBase,
    Metric::OiEndUsd,
    Metric::MarkOpen,
    Metric::MarkHigh,
    Metric::MarkLow,
    Metric::MarkClose,
];

/// Which families of cells a set of metrics needs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Needs {
    /// Liquidation cells.
    pub liq: bool,
    /// Open interest cells.
    pub oi: bool,
    /// The cells carry mark prices.
    pub mark: bool,
    /// The cells carry funding rates and basis.
    pub funding: bool,
}

impl Needs {
    /// What `metrics` need.
    pub fn of(metrics: &[Metric]) -> Self {
        let mut n = Needs::default();
        for m in metrics {
            match m {
                Metric::LiqSumUsd
                | Metric::LiqCount
                | Metric::LiqLongUsd
                | Metric::LiqShortUsd
                | Metric::LiqMaxUsd => n.liq = true,
                Metric::LiqToOiRatio => {
                    n.liq = true;
                    n.oi = true;
                }
                Metric::OiChangeBase
                | Metric::OiTotalUsd
                | Metric::OiEndBase
                | Metric::OiEndUsd => n.oi = true,
                // The change in USD is priced at the mark price of the bin.
                Metric::OiChangeUsd
                | Metric::MarkOpen
                | Metric::MarkHigh
                | Metric::MarkLow
                | Metric::MarkClose => {
                    n.oi = true;
                    n.mark = true;
                }
                Metric::FundingLast
                | Metric::FundingAvg
                | Metric::BasisPct
                | Metric::BasisPctAvg => {
                    n.oi = true;
                    n.funding = true;
                }
                Metric::PriceChangePct
                | Metric::Regime
                | Metric::RegimeStrength
                | Metric::OiPriceFlatness => {
                    n.oi = true;
                    n.mark = true;
                }
                Metric::OiChangePct | Metric::OiDivergencePct => n.oi = true,
                Metric::Unspecified => {}
            }
        }
        n
    }
}

/// What the roll-up needs to know about the request.
#[derive(Debug, Clone)]
pub struct RollupParams {
    /// Grouping.
    pub group_by: GroupBy,
    /// Step.
    pub step: BinStep,
    /// Start of the first requested bin (cells of earlier bins only supply the "previous end level").
    pub first_bin: i64,
    /// End of the last requested bin, exclusive.
    pub end: i64,
    /// Metrics, in answer order, without duplicates.
    pub metrics: Vec<Metric>,
    /// Row cap.
    pub max_rows: usize,
}

/// The roll-up could not be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RollupError {
    /// More rows than the cap.
    #[error("the answer would have {rows} rows, the limit is {max}")]
    TooManyRows {
        /// Rows the answer would have.
        rows: usize,
        /// The cap.
        max: usize,
    },
}

/// The rows of an answer.
#[derive(Debug, Clone, Default)]
pub struct Rollup {
    /// Rows, ordered by group and bin start.
    pub rows: Vec<AggregateRow>,
    /// Any row is partial.
    pub partial: bool,
}

#[derive(Default)]
struct OiAcc {
    series: u32,
    end_base: f64,
    usd_sum: f64,
    usd_n: u32,
    chg_base: f64,
    chg_n: u32,
    chg_usd: f64,
    chg_usd_n: u32,
    /// Sum of the previous end levels (base) of the series that have a change: the base of the percentage.
    chg_prev_base: f64,
    /// Per exchange (change, previous level) in base units, collected only for the divergence metric.
    by_exchange: BTreeMap<String, (f64, f64)>,
    chg_excluded: bool,
    mark_open: Option<Stamped>,
    mark_close: Option<Stamped>,
    mark_high: Option<f64>,
    mark_low: Option<f64>,
    funding_last_sum: f64,
    funding_last_n: u32,
    funding_avg_sum: f64,
    funding_avg_n: u32,
    basis_last_sum: f64,
    basis_last_n: u32,
    basis_avg_sum: f64,
    basis_avg_n: u32,
}

#[derive(Default)]
struct BinAcc {
    liq: Option<LiqBin>,
    oi: Option<OiAcc>,
}

#[derive(Default)]
struct GroupAcc {
    bins: BTreeMap<i64, BinAcc>,
    exchanges: BTreeSet<String>,
}

fn group_key(group_by: GroupBy, key: &SeriesKey) -> String {
    match group_by {
        GroupBy::Exchange => key.0.clone(),
        GroupBy::BaseAsset => base_asset(&key.1).to_string(),
        GroupBy::Symbol | GroupBy::Unspecified => key.1.clone(),
    }
}

/// Price used to convert an OI change in base units to USD: the last mark price of the bin, else the implied
/// price of the last `oi_usd`.
fn conversion_price(c: &OiBin) -> Option<f64> {
    c.mark_close
        .map(|m| m.v)
        .or_else(|| match c.last_usd {
            Some(u) if c.last_base > 0.0 => usable_price(Some(u.v / c.last_base)),
            _ => None,
        })
        .filter(|p| p.is_finite() && *p > 0.0)
}

/// Rolls the cells up into answer rows.
pub fn rollup(fine: &FineData, gaps: &[GapRec], p: &RollupParams) -> Result<Rollup, RollupError> {
    let needs = Needs::of(&p.metrics);
    let want_divergence =
        p.group_by == GroupBy::BaseAsset && p.metrics.contains(&Metric::OiDivergencePct);
    let mut groups: BTreeMap<String, GroupAcc> = BTreeMap::new();
    // Total OI in USD per (base asset, bin), over every series that passed the filters.
    let mut base_totals: HashMap<(String, i64), f64> = HashMap::new();

    if needs.liq {
        for (key, bins) in &fine.liq {
            let g = groups.entry(group_key(p.group_by, key)).or_default();
            g.exchanges.insert(key.0.clone());
            for (bin, cell) in bins {
                if *bin < p.first_bin || *bin >= p.end {
                    continue;
                }
                g.bins
                    .entry(*bin)
                    .or_default()
                    .liq
                    .get_or_insert_with(LiqBin::default)
                    .merge(cell);
            }
        }
    }
    if needs.oi {
        for (key, bins) in &fine.oi {
            let gk = group_key(p.group_by, key);
            let mut first_seen: Option<i64> = None;
            for (bin, cell) in bins {
                let first = *first_seen.get_or_insert(*bin);
                // Bins before the period are kept: the previous bin of the first one supplies its changes (price
                // change, regime); they never become rows.
                if *bin >= p.end {
                    continue;
                }
                let g = groups.entry(gk.clone()).or_default();
                g.exchanges.insert(key.0.clone());
                let acc = g.bins.entry(*bin).or_default();
                let oi = acc.oi.get_or_insert_with(OiAcc::default);
                oi.series += 1;
                oi.end_base += cell.last_base;
                if let Some(u) = cell.last_usd {
                    oi.usd_sum += u.v;
                    oi.usd_n += 1;
                    *base_totals
                        .entry((base_asset(&key.1).to_string(), *bin))
                        .or_default() += u.v;
                }
                match bins.get(&p.step.prev(*bin)) {
                    Some(prev) => {
                        let change = cell.last_base - prev.last_base;
                        oi.chg_base += change;
                        oi.chg_n += 1;
                        oi.chg_prev_base += prev.last_base;
                        if want_divergence {
                            let e = oi.by_exchange.entry(key.0.clone()).or_default();
                            e.0 += change;
                            e.1 += prev.last_base;
                        }
                        if let Some(price) = conversion_price(cell) {
                            oi.chg_usd += change * price;
                            oi.chg_usd_n += 1;
                        }
                    }
                    // Data before this bin exists but not in the bin right before it: a hole, the change
                    // across it is not a one-step change.
                    None if first < *bin => oi.chg_excluded = true,
                    None => {}
                }
                oi.mark_open = earlier(oi.mark_open, cell.mark_open);
                oi.mark_close = later(oi.mark_close, cell.mark_close);
                oi.mark_high = max_opt(oi.mark_high, cell.mark_high);
                oi.mark_low = min_opt(oi.mark_low, cell.mark_low);
                if let Some(f) = cell.funding_last {
                    oi.funding_last_sum += f.v;
                    oi.funding_last_n += 1;
                }
                if cell.funding_n > 0 {
                    oi.funding_avg_sum += cell.funding_sum / cell.funding_n as f64;
                    oi.funding_avg_n += 1;
                }
                if let Some(b) = cell.basis_last {
                    oi.basis_last_sum += b.v;
                    oi.basis_last_n += 1;
                }
                if cell.basis_n > 0 {
                    oi.basis_avg_sum += cell.basis_sum / cell.basis_n as f64;
                    oi.basis_avg_n += 1;
                }
            }
        }
    }

    // Row count first, so a huge answer is refused before it is built.
    let mut total_rows = 0usize;
    for g in groups.values() {
        if let (Some(lo), Some(hi)) = (
            g.bins.range(p.first_bin..).next().map(|(k, _)| k),
            g.bins.keys().next_back(),
        ) {
            total_rows += p.step.count(*lo, *hi + 1) as usize;
            if total_rows > p.max_rows {
                return Err(RollupError::TooManyRows {
                    rows: total_rows,
                    max: p.max_rows,
                });
            }
        }
    }

    let mut out = Rollup::default();
    for (name, g) in &groups {
        let (Some(&lo), Some(&hi)) = (
            g.bins.range(p.first_bin..).next().map(|(k, _)| k),
            g.bins.keys().next_back(),
        ) else {
            continue;
        };
        let relevant: Vec<(&GapRec, bool, bool)> = gaps
            .iter()
            .filter(|gap| gap.exchange == "*" || g.exchanges.contains(&gap.exchange))
            .map(|gap| {
                let writer = gap.exchange == "*";
                let liq_stream = gap.stream == "liquidations";
                (
                    gap,
                    needs.liq && (liq_stream || writer),
                    needs.oi && (!liq_stream || writer),
                )
            })
            .filter(|(_, liq, oi)| *liq || *oi)
            .collect();
        let mut bin = lo;
        while bin <= hi {
            let next = p.step.next(bin);
            let acc = g.bins.get(&bin);
            let has_liq = acc.is_some_and(|a| a.liq.is_some());
            let has_oi = acc.is_some_and(|a| a.oi.is_some());
            let covered = has_liq || has_oi || !needs.oi;
            let mut partial = !covered
                || acc
                    .and_then(|a| a.oi.as_ref())
                    .is_some_and(|o| o.chg_excluded);
            if !partial {
                partial = relevant
                    .iter()
                    .any(|(gap, _, _)| gap.from_ts < next && gap.to_ts >= bin);
            }
            let values = p
                .metrics
                .iter()
                .map(|m| MetricValue {
                    metric: *m as i32,
                    value: metric_value(
                        *m,
                        p.group_by,
                        name,
                        bin,
                        acc,
                        g.bins.get(&p.step.prev(bin)),
                        covered,
                        &base_totals,
                    ),
                })
                .collect();
            out.partial |= partial;
            out.rows.push(AggregateRow {
                bucket_start: bin,
                group: name.clone(),
                values,
                partial,
                bucket_end: next,
                divergence: if want_divergence {
                    acc.and_then(|a| a.oi.as_ref()).and_then(divergence_of)
                } else {
                    None
                },
            });
            bin = next;
        }
    }
    Ok(out)
}

/// Largest and smallest OI change (percent of the previous level) among the exchanges of a base asset.
fn divergence_of(o: &OiAcc) -> Option<Divergence> {
    let pcts: Vec<(&String, f64)> = o
        .by_exchange
        .iter()
        .filter(|(_, (_, prev))| *prev > 0.0)
        .map(|(ex, (chg, prev))| (ex, 100.0 * chg / prev))
        .collect();
    if pcts.len() < 2 {
        return None;
    }
    let rising = pcts.iter().max_by(|a, b| a.1.total_cmp(&b.1)).copied()?;
    let falling = pcts.iter().min_by(|a, b| a.1.total_cmp(&b.1)).copied()?;
    Some(Divergence {
        rising_exchange: rising.0.clone(),
        rising_pct: rising.1,
        falling_exchange: falling.0.clone(),
        falling_pct: falling.1,
        spread_pct: rising.1 - falling.1,
        exchanges: pcts.len() as u32,
    })
}

/// Price change over the bin in percent: the last mark price against the one of the bin before.
fn price_change_pct(oi: Option<&OiAcc>, prev: Option<&BinAcc>) -> Option<f64> {
    let now = oi?.mark_close?.v;
    let before = prev.and_then(|p| p.oi.as_ref())?.mark_close?.v;
    (before > 0.0).then(|| 100.0 * (now - before) / before)
}

/// OI change over the bin in percent of the level before it.
fn oi_change_pct(oi: Option<&OiAcc>) -> Option<f64> {
    let o = oi?;
    (o.chg_n > 0 && o.chg_prev_base > 0.0).then(|| 100.0 * o.chg_base / o.chg_prev_base)
}

pub fn regime_of(price_pct: f64, oi_pct: f64) -> Option<(Regime, f64)> {
    if price_pct == 0.0 || oi_pct == 0.0 {
        return None;
    }
    let regime = match (price_pct > 0.0, oi_pct > 0.0) {
        (true, true) => Regime::NewLongs,
        (false, true) => Regime::NewShorts,
        (true, false) => Regime::ShortCovering,
        (false, false) => Regime::LongUnwinding,
    };
    Some((regime, (price_pct.abs() * oi_pct.abs()).sqrt()))
}

#[allow(clippy::too_many_arguments)]
fn metric_value(
    m: Metric,
    group_by: GroupBy,
    group: &str,
    bin: i64,
    acc: Option<&BinAcc>,
    prev: Option<&BinAcc>,
    covered: bool,
    base_totals: &HashMap<(String, i64), f64>,
) -> Option<f64> {
    let liq = acc.and_then(|a| a.liq.as_ref());
    let oi = acc.and_then(|a| a.oi.as_ref());
    let by_exchange = group_by == GroupBy::Exchange;
    // A sum over zero events is a real zero as long as the bin is covered by some data.
    let liq_or_zero = |f: &dyn Fn(&LiqBin) -> f64| -> Option<f64> {
        if !covered {
            return None;
        }
        Some(liq.map_or(0.0, f))
    };
    let end_usd = oi.filter(|o| o.usd_n > 0).map(|o| o.usd_sum);
    match m {
        Metric::LiqSumUsd => liq_or_zero(&|l| l.sum),
        Metric::LiqCount => liq_or_zero(&|l| l.count as f64),
        Metric::LiqLongUsd => liq_or_zero(&|l| l.long),
        Metric::LiqShortUsd => liq_or_zero(&|l| l.short),
        Metric::LiqMaxUsd => liq.map(|l| l.max),
        Metric::OiEndBase => oi.filter(|_| !by_exchange).map(|o| o.end_base),
        Metric::OiEndUsd => end_usd,
        Metric::OiChangeBase => oi
            .filter(|o| o.chg_n > 0 && !by_exchange)
            .map(|o| o.chg_base),
        Metric::OiChangeUsd => oi.filter(|o| o.chg_usd_n > 0).map(|o| o.chg_usd),
        Metric::OiTotalUsd => match group_by {
            GroupBy::BaseAsset => end_usd,
            GroupBy::Symbol => oi.and_then(|_| {
                base_totals
                    .get(&(base_asset(group).to_string(), bin))
                    .copied()
            }),
            _ => None,
        },
        Metric::LiqToOiRatio => match (liq.map(|l| l.sum).or(covered.then_some(0.0)), end_usd) {
            (Some(l), Some(o)) if o > 0.0 => Some(l / o),
            _ => None,
        },
        Metric::MarkOpen => oi
            .filter(|_| !by_exchange)
            .and_then(|o| o.mark_open)
            .map(|s| s.v),
        Metric::MarkClose => oi
            .filter(|_| !by_exchange)
            .and_then(|o| o.mark_close)
            .map(|s| s.v),
        Metric::MarkHigh => oi.filter(|_| !by_exchange).and_then(|o| o.mark_high),
        Metric::MarkLow => oi.filter(|_| !by_exchange).and_then(|o| o.mark_low),
        Metric::PriceChangePct => price_change_pct(oi.filter(|_| !by_exchange), prev),
        Metric::OiChangePct => oi_change_pct(oi.filter(|_| !by_exchange)),
        Metric::Regime | Metric::RegimeStrength => {
            let o = oi.filter(|_| !by_exchange);
            let (pp, op) = (price_change_pct(o, prev)?, oi_change_pct(o)?);
            let (regime, strength) = regime_of(pp, op)?;
            Some(if m == Metric::Regime {
                f64::from(regime as i32)
            } else {
                strength
            })
        }
        Metric::OiPriceFlatness => {
            let o = oi.filter(|_| !by_exchange);
            Some(oi_change_pct(o)? - price_change_pct(o, prev)?.abs())
        }
        Metric::OiDivergencePct => {
            if group_by != GroupBy::BaseAsset {
                return None;
            }
            oi.and_then(divergence_of).map(|d| d.spread_pct)
        }
        Metric::FundingLast => mean(oi.map(|o| (o.funding_last_sum, o.funding_last_n))),
        Metric::FundingAvg => mean(oi.map(|o| (o.funding_avg_sum, o.funding_avg_n))),
        Metric::BasisPct => mean(oi.map(|o| (o.basis_last_sum, o.basis_last_n))),
        Metric::BasisPctAvg => mean(oi.map(|o| (o.basis_avg_sum, o.basis_avg_n))),
        Metric::Unspecified => None,
    }
}

/// Mean of `n` values with the sum given, `None` for none.
fn mean(v: Option<(f64, u32)>) -> Option<f64> {
    v.filter(|(_, n)| *n > 0).map(|(sum, n)| sum / f64::from(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: i64 = 60_000;
    const T0: i64 = 1_792_972_800_000; // a Monday, 00:00 UTC

    fn liq(exchange: &str, symbol: &str, ts: i64, side: Side, usd: f64) -> Liquidation {
        Liquidation {
            exchange: exchange.into(),
            symbol: symbol.into(),
            symbol_raw: symbol.replace('-', ""),
            exchange_ts: ts,
            recv_ts: ts,
            side: side as i32,
            price: 1.0,
            qty: usd,
            notional_usd: usd,
        }
    }

    fn oi(exchange: &str, symbol: &str, ts: i64, base: f64, mark: f64) -> OpenInterest {
        OpenInterest {
            exchange: exchange.into(),
            symbol: symbol.into(),
            symbol_raw: symbol.replace('-', ""),
            exchange_ts: ts,
            recv_ts: ts,
            oi_base: base,
            oi_usd: Some(base * mark),
            mark_price: Some(mark),
            ..OpenInterest::default()
        }
    }

    fn params(group_by: GroupBy, metrics: &[Metric]) -> RollupParams {
        RollupParams {
            group_by,
            step: BinStep::Minute1,
            first_bin: T0,
            end: T0 + 10 * MIN,
            metrics: metrics.to_vec(),
            max_rows: 10_000,
        }
    }

    fn value(row: &AggregateRow, m: Metric) -> Option<f64> {
        row.values
            .iter()
            .find(|v| v.metric == m as i32)
            .unwrap()
            .value
    }

    #[test]
    fn oi_cell_merge_is_the_same_as_one_pass() {
        let samples = [
            (T0 + 1_000, 100.0, Some(1_000.0), Some(10.0)),
            (T0 + 31_000, 101.0, Some(1_030.0), Some(10.2)),
            (T0 + 41_000, 102.0, None, Some(9.9)),
            (T0 + 51_000, 103.0, Some(1_100.0), None),
        ];
        let mut whole = OiBin::default();
        for s in samples {
            whole.add_sample(s.0, s.1, s.2, s.3);
        }
        for split in 0..=samples.len() {
            let (mut a, mut b) = (OiBin::default(), OiBin::default());
            for s in &samples[..split] {
                a.add_sample(s.0, s.1, s.2, s.3);
            }
            for s in &samples[split..] {
                b.add_sample(s.0, s.1, s.2, s.3);
            }
            a.merge(&b);
            assert_eq!(a, whole, "split at {split}");
        }
        assert_eq!(whole.last_base, 103.0);
        assert_eq!(
            whole.last_usd.unwrap().v,
            1_100.0,
            "last known USD, not the last sample's"
        );
        assert_eq!(whole.mark_open.unwrap().v, 10.0);
        assert_eq!(whole.mark_close.unwrap().v, 9.9);
        assert_eq!(whole.mark_high, Some(10.2));
        assert_eq!(whole.mark_low, Some(9.9));
    }

    #[test]
    fn funding_and_basis_merge_like_the_other_cells() {
        let samples = [
            (T0 + 1_000, Some(0.0001), Some(100.0), Some(101.0)),
            (T0 + 31_000, Some(0.0003), Some(100.0), Some(99.0)),
            (T0 + 41_000, None, Some(100.0), None),
            (T0 + 51_000, Some(-0.0002), None, Some(100.5)),
        ];
        let mut whole = OiBin::default();
        for (ts, f, mark, index) in samples {
            whole.add_full_sample(ts, 1.0, None, mark, f, index);
        }
        assert_eq!(whole.funding_n, 3);
        assert!((whole.funding_sum - 0.0002).abs() < 1e-12);
        assert_eq!(whole.funding_last.unwrap().v, -0.0002);
        // Basis only where both prices are known: 100 * (100 - 101) / 101 and 100 * (100 - 99) / 99.
        assert_eq!(whole.basis_n, 2);
        let (b1, b2) = (
            100.0 * (100.0 - 101.0) / 101.0,
            100.0 * (100.0 - 99.0) / 99.0,
        );
        assert!((whole.basis_sum - (b1 + b2)).abs() < 1e-9);
        assert!((whole.basis_last.unwrap().v - b2).abs() < 1e-9);
        for split in 0..=samples.len() {
            let (mut a, mut b) = (OiBin::default(), OiBin::default());
            for (ts, f, mark, index) in &samples[..split] {
                a.add_full_sample(*ts, 1.0, None, *mark, *f, *index);
            }
            for (ts, f, mark, index) in &samples[split..] {
                b.add_full_sample(*ts, 1.0, None, *mark, *f, *index);
            }
            a.merge(&b);
            // Sums are floating point: the order of additions may differ in the last bit.
            assert!((a.funding_sum - whole.funding_sum).abs() < 1e-15);
            assert!((a.basis_sum - whole.basis_sum).abs() < 1e-12);
            a.funding_sum = whole.funding_sum;
            a.basis_sum = whole.basis_sum;
            assert_eq!(a, whole, "split at {split}");
        }
        assert_eq!(basis_pct(Some(101.0), Some(100.0)), Some(1.0));
        assert_eq!(basis_pct(Some(101.0), None), None);
        assert_eq!(basis_pct(Some(101.0), Some(0.0)), None);
    }

    #[test]
    fn funding_metrics_are_means_over_the_series() {
        let mut fine = FineData::default();
        let s = BinStep::Minute1;
        for (ex, funding, index) in [
            ("binance", 0.0002, 100.0),
            ("bybit", 0.0004, 98.0),
            ("okx", f64::NAN, 100.0),
        ] {
            let mut o = oi(ex, "AAA-USDT-PERP", T0 + 1_000, 10.0, 100.0);
            o.funding_rate = (!funding.is_nan()).then_some(funding);
            o.index_price = Some(index);
            fine.add_oi(s, &o);
        }
        let metrics = [
            Metric::FundingLast,
            Metric::FundingAvg,
            Metric::BasisPct,
            Metric::BasisPctAvg,
        ];
        let r = rollup(&fine, &[], &params(GroupBy::Symbol, &metrics)).unwrap();
        let row = &r.rows[0];
        // Funding: mean of the two exchanges that have it. Basis: (0 + 100*(100-98)/98 + 0) / 3.
        assert!((value(row, Metric::FundingLast).unwrap() - 0.0003).abs() < 1e-12);
        assert!((value(row, Metric::FundingAvg).unwrap() - 0.0003).abs() < 1e-12);
        let expect = (0.0 + 100.0 * 2.0 / 98.0 + 0.0) / 3.0;
        assert!((value(row, Metric::BasisPct).unwrap() - expect).abs() < 1e-9);
        assert!((value(row, Metric::BasisPctAvg).unwrap() - expect).abs() < 1e-9);
        // No funding anywhere: null, not zero.
        let mut none = FineData::default();
        none.add_oi(s, &oi("okx", "AAA-USDT-PERP", T0 + 1_000, 10.0, 100.0));
        let r = rollup(&none, &[], &params(GroupBy::Symbol, &metrics)).unwrap();
        for m in metrics {
            assert_eq!(value(&r.rows[0], m), None, "{m:?}");
        }
    }

    /// One series with a level and a mark price per minute.
    fn walk(fine: &mut FineData, ex: &str, sym: &str, levels: &[(f64, f64)]) {
        for (i, (base, mark)) in levels.iter().enumerate() {
            let ts = T0 - MIN + i as i64 * MIN + 1_000;
            fine.add_oi(BinStep::Minute1, &oi(ex, sym, ts, *base, *mark));
        }
    }

    #[test]
    fn regimes_follow_the_signs_of_price_and_oi() {
        let mut fine = FineData::default();
        // Bin 0 is the lookback bin (T0 - 1 min); the answer rows are bins 1.. of the walk.
        walk(
            &mut fine,
            "binance",
            "UP-USDT-PERP",
            &[(100.0, 10.0), (110.0, 11.0), (121.0, 12.1)],
        );
        walk(
            &mut fine,
            "binance",
            "DN-USDT-PERP",
            &[(100.0, 10.0), (110.0, 9.0), (99.0, 8.1)],
        );
        walk(
            &mut fine,
            "binance",
            "SC-USDT-PERP",
            &[(100.0, 10.0), (90.0, 11.0), (81.0, 12.1)],
        );
        walk(
            &mut fine,
            "binance",
            "LU-USDT-PERP",
            &[(100.0, 10.0), (90.0, 9.0), (81.0, 8.1)],
        );
        walk(
            &mut fine,
            "binance",
            "FL-USDT-PERP",
            &[(100.0, 10.0), (110.0, 10.0), (121.0, 10.0)],
        );
        let metrics = [
            Metric::PriceChangePct,
            Metric::OiChangePct,
            Metric::Regime,
            Metric::RegimeStrength,
            Metric::OiPriceFlatness,
        ];
        let r = rollup(&fine, &[], &params(GroupBy::Symbol, &metrics)).unwrap();
        let row = |g: &str, i: usize| {
            r.rows
                .iter()
                .filter(|r| r.group == g)
                .nth(i)
                .unwrap_or_else(|| panic!("{g} {i}"))
        };
        assert_eq!(r.rows.len(), 10, "two answer bins for each of five symbols");
        for (g, regime) in [
            ("UP-USDT-PERP", Regime::NewLongs),
            ("DN-USDT-PERP", Regime::NewShorts),
            ("SC-USDT-PERP", Regime::ShortCovering),
            ("LU-USDT-PERP", Regime::LongUnwinding),
        ] {
            let first = row(g, 0);
            assert_eq!(
                value(first, Metric::Regime),
                Some(f64::from(regime as i32)),
                "{g}"
            );
            let (p, o) = (
                value(first, Metric::PriceChangePct).unwrap(),
                value(first, Metric::OiChangePct).unwrap(),
            );
            assert!(
                (p.abs() - 10.0).abs() < 0.5 && (o.abs() - 10.0).abs() < 1e-9,
                "{g}: {p} {o}"
            );
            let strength = value(first, Metric::RegimeStrength).unwrap();
            assert!((strength - (p.abs() * o.abs()).sqrt()).abs() < 1e-9);
            assert!((value(first, Metric::OiPriceFlatness).unwrap() - (o - p.abs())).abs() < 1e-9);
        }
        // Flat price: OI grows 10%, the price does not move: no regime, a large flatness.
        let flat = row("FL-USDT-PERP", 0);
        assert_eq!(value(flat, Metric::PriceChangePct), Some(0.0));
        assert_eq!(value(flat, Metric::Regime), None);
        assert_eq!(value(flat, Metric::RegimeStrength), None);
        assert!((value(flat, Metric::OiPriceFlatness).unwrap() - 10.0).abs() < 1e-9);
        // Exchange groups mix assets: none of this is defined for them.
        let by_ex = rollup(&fine, &[], &params(GroupBy::Exchange, &metrics)).unwrap();
        for m in metrics {
            assert_eq!(value(&by_ex.rows[0], m), None, "{m:?}");
        }
    }

    #[test]
    fn the_first_bin_has_no_regime_without_a_previous_bin() {
        let mut fine = FineData::default();
        fine.add_oi(
            BinStep::Minute1,
            &oi("binance", "AAA-USDT-PERP", T0 + 1_000, 100.0, 10.0),
        );
        let r = rollup(
            &fine,
            &[],
            &params(
                GroupBy::Symbol,
                &[Metric::PriceChangePct, Metric::OiChangePct, Metric::Regime],
            ),
        )
        .unwrap();
        for v in &r.rows[0].values {
            assert_eq!(v.value, None);
        }
    }

    #[test]
    fn divergence_names_the_exchanges_that_part_ways() {
        let mut fine = FineData::default();
        walk(
            &mut fine,
            "binance",
            "BTC-USDT-PERP",
            &[(100.0, 1.0), (110.0, 1.0)],
        );
        walk(
            &mut fine,
            "bybit",
            "BTC-USDT-PERP",
            &[(200.0, 1.0), (190.0, 1.0)],
        );
        walk(
            &mut fine,
            "okx",
            "BTC-USDT-PERP",
            &[(50.0, 1.0), (51.0, 1.0)],
        );
        // A second symbol of the same asset on binance adds to binance's change.
        walk(
            &mut fine,
            "binance",
            "BTC-USDC-PERP",
            &[(100.0, 1.0), (110.0, 1.0)],
        );
        // An asset on one exchange only has no divergence.
        walk(
            &mut fine,
            "binance",
            "SOLO-USDT-PERP",
            &[(10.0, 1.0), (20.0, 1.0)],
        );
        let r = rollup(
            &fine,
            &[],
            &params(
                GroupBy::BaseAsset,
                &[Metric::OiDivergencePct, Metric::OiChangePct],
            ),
        )
        .unwrap();
        let btc = r
            .rows
            .iter()
            .find(|r| r.group == "BTC" && r.bucket_start == T0)
            .unwrap();
        let d = btc.divergence.as_ref().expect("divergence");
        assert_eq!(
            (d.rising_exchange.as_str(), d.falling_exchange.as_str()),
            ("binance", "bybit")
        );
        assert!((d.rising_pct - 10.0).abs() < 1e-9, "{d:?}");
        assert!((d.falling_pct + 5.0).abs() < 1e-9, "{d:?}");
        assert!((d.spread_pct - 15.0).abs() < 1e-9);
        assert_eq!(d.exchanges, 3);
        assert!((value(btc, Metric::OiDivergencePct).unwrap() - 15.0).abs() < 1e-9);
        let solo = r
            .rows
            .iter()
            .find(|r| r.group == "SOLO" && r.bucket_start == T0)
            .unwrap();
        assert!(solo.divergence.is_none());
        assert_eq!(value(solo, Metric::OiDivergencePct), None);
        assert!((value(solo, Metric::OiChangePct).unwrap() - 100.0).abs() < 1e-9);
        // Not asked for, not computed; other groupings have none.
        let plain = rollup(
            &fine,
            &[],
            &params(GroupBy::BaseAsset, &[Metric::OiChangePct]),
        )
        .unwrap();
        assert!(plain.rows.iter().all(|r| r.divergence.is_none()));
        let by_symbol = rollup(
            &fine,
            &[],
            &params(GroupBy::Symbol, &[Metric::OiDivergencePct]),
        )
        .unwrap();
        assert!(
            by_symbol
                .rows
                .iter()
                .all(|r| r.divergence.is_none() && r.values[0].value.is_none())
        );
    }

    #[test]
    fn liquidation_cells_add() {
        let mut a = LiqBin::default();
        a.add(Side::Long as i32, 100.0);
        a.add(Side::Short as i32, 50.0);
        let mut b = LiqBin::default();
        b.add(Side::Long as i32, 300.0);
        a.merge(&b);
        assert_eq!(
            (a.count, a.sum, a.long, a.short, a.max),
            (3, 450.0, 400.0, 50.0, 300.0)
        );
    }

    #[test]
    fn symbol_rows_have_levels_changes_and_a_candle() {
        let mut fine = FineData::default();
        let s = BinStep::Minute1;
        for (i, (base, mark)) in [(100.0, 10.0), (110.0, 11.0), (110.0, 9.0)]
            .into_iter()
            .enumerate()
        {
            let ts = T0 + i as i64 * MIN;
            fine.add_oi(s, &oi("binance", "AAA-USDT-PERP", ts + 5_000, base, mark));
            fine.add_oi(
                s,
                &oi("binance", "AAA-USDT-PERP", ts + 35_000, base, mark + 0.5),
            );
        }
        fine.add_liquidation(
            s,
            &liq(
                "binance",
                "AAA-USDT-PERP",
                T0 + MIN + 10_000,
                Side::Long,
                400.0,
            ),
        );
        fine.add_liquidation(
            s,
            &liq(
                "binance",
                "AAA-USDT-PERP",
                T0 + MIN + 20_000,
                Side::Short,
                100.0,
            ),
        );
        let metrics = DEFAULT_METRICS.to_vec();
        let r = rollup(&fine, &[], &params(GroupBy::Symbol, &metrics)).unwrap();
        assert_eq!(r.rows.len(), 3);
        assert!(!r.partial);
        let b1 = &r.rows[1];
        assert_eq!(b1.bucket_start, T0 + MIN);
        assert_eq!(b1.bucket_end, T0 + 2 * MIN);
        assert_eq!(b1.group, "AAA-USDT-PERP");
        assert_eq!(value(b1, Metric::LiqSumUsd), Some(500.0));
        assert_eq!(value(b1, Metric::LiqCount), Some(2.0));
        assert_eq!(value(b1, Metric::LiqLongUsd), Some(400.0));
        assert_eq!(value(b1, Metric::LiqShortUsd), Some(100.0));
        assert_eq!(value(b1, Metric::LiqMaxUsd), Some(400.0));
        assert_eq!(value(b1, Metric::OiEndBase), Some(110.0));
        // 110 * (11 + 0.5) at the last sample.
        assert_eq!(value(b1, Metric::OiEndUsd), Some(110.0 * 11.5));
        assert_eq!(value(b1, Metric::OiChangeBase), Some(10.0));
        // The change is priced at the bin's last mark price.
        assert_eq!(value(b1, Metric::OiChangeUsd), Some(10.0 * 11.5));
        assert_eq!(value(b1, Metric::MarkOpen), Some(11.0));
        assert_eq!(value(b1, Metric::MarkClose), Some(11.5));
        assert_eq!(value(b1, Metric::MarkHigh), Some(11.5));
        assert_eq!(value(b1, Metric::MarkLow), Some(11.0));
        assert_eq!(value(b1, Metric::OiTotalUsd), Some(110.0 * 11.5));
        assert_eq!(
            value(b1, Metric::LiqToOiRatio),
            Some(500.0 / (110.0 * 11.5))
        );
        // The first bin has no previous bin inside the fetched cells: no change, but a level.
        let b0 = &r.rows[0];
        assert_eq!(value(b0, Metric::OiChangeBase), None);
        assert_eq!(value(b0, Metric::OiEndBase), Some(100.0));
        // A bin without liquidations is a real zero while OI covers it; the maximum is absent.
        assert_eq!(value(b0, Metric::LiqSumUsd), Some(0.0));
        assert_eq!(value(b0, Metric::LiqCount), Some(0.0));
        assert_eq!(value(b0, Metric::LiqMaxUsd), None);
    }

    #[test]
    fn the_previous_bin_outside_the_period_supplies_the_change() {
        let mut fine = FineData::default();
        let s = BinStep::Minute1;
        fine.add_oi(
            s,
            &oi("binance", "AAA-USDT-PERP", T0 - MIN + 1_000, 90.0, 10.0),
        );
        fine.add_oi(s, &oi("binance", "AAA-USDT-PERP", T0 + 1_000, 100.0, 10.0));
        let r = rollup(
            &fine,
            &[],
            &params(GroupBy::Symbol, &[Metric::OiChangeBase]),
        )
        .unwrap();
        assert_eq!(r.rows.len(), 1, "the lookback bin is not an answer row");
        assert_eq!(value(&r.rows[0], Metric::OiChangeBase), Some(10.0));
    }

    #[test]
    fn groups_sum_series_and_exchange_groups_skip_base_units() {
        let mut fine = FineData::default();
        let s = BinStep::Minute1;
        for (ex, sym, base) in [
            ("binance", "AAA-USDT-PERP", 100.0),
            ("bybit", "AAA-USDT-PERP", 50.0),
            ("bybit", "AAA-USDC-PERP", 10.0),
            ("bybit", "BBB-USDT-PERP", 7.0),
        ] {
            fine.add_oi(s, &oi(ex, sym, T0 - MIN + 1_000, base, 2.0));
            fine.add_oi(s, &oi(ex, sym, T0 + 1_000, base + 1.0, 2.0));
        }
        let metrics = [
            Metric::OiEndBase,
            Metric::OiEndUsd,
            Metric::OiChangeBase,
            Metric::OiTotalUsd,
        ];
        let by_base = rollup(&fine, &[], &params(GroupBy::BaseAsset, &metrics)).unwrap();
        let rows: HashMap<_, _> = by_base.rows.iter().map(|r| (r.group.as_str(), r)).collect();
        assert_eq!(
            value(rows["AAA"], Metric::OiEndBase),
            Some(101.0 + 51.0 + 11.0)
        );
        assert_eq!(value(rows["AAA"], Metric::OiChangeBase), Some(3.0));
        assert_eq!(
            value(rows["AAA"], Metric::OiTotalUsd),
            Some((101.0 + 51.0 + 11.0) * 2.0)
        );
        assert_eq!(value(rows["BBB"], Metric::OiEndUsd), Some(16.0));
        // Symbol groups: the total is still per base asset (both quote currencies of AAA).
        let by_symbol = rollup(&fine, &[], &params(GroupBy::Symbol, &metrics)).unwrap();
        let rows: HashMap<_, _> = by_symbol
            .rows
            .iter()
            .map(|r| (r.group.as_str(), r))
            .collect();
        assert_eq!(value(rows["AAA-USDT-PERP"], Metric::OiEndBase), Some(152.0));
        assert_eq!(value(rows["AAA-USDT-PERP"], Metric::OiEndUsd), Some(304.0));
        assert_eq!(
            value(rows["AAA-USDT-PERP"], Metric::OiTotalUsd),
            Some(326.0)
        );
        // Exchange groups mix assets: only USD values are meaningful.
        let by_ex = rollup(&fine, &[], &params(GroupBy::Exchange, &metrics)).unwrap();
        let rows: HashMap<_, _> = by_ex.rows.iter().map(|r| (r.group.as_str(), r)).collect();
        assert_eq!(value(rows["binance"], Metric::OiEndBase), None);
        assert_eq!(value(rows["binance"], Metric::OiChangeBase), None);
        assert_eq!(value(rows["binance"], Metric::OiEndUsd), Some(202.0));
        assert_eq!(value(rows["binance"], Metric::OiTotalUsd), None);
        let candle = rollup(&fine, &[], &params(GroupBy::Exchange, &[Metric::MarkClose])).unwrap();
        assert_eq!(value(&candle.rows[0], Metric::MarkClose), None);
    }

    #[test]
    fn gaps_and_holes_make_rows_partial_and_empty_bins_null() {
        let mut fine = FineData::default();
        let s = BinStep::Minute1;
        fine.add_oi(s, &oi("binance", "AAA-USDT-PERP", T0 + 1_000, 100.0, 1.0));
        // Nothing in the 2nd and 3rd minute, data again in the 4th.
        fine.add_oi(
            s,
            &oi("binance", "AAA-USDT-PERP", T0 + 3 * MIN + 1_000, 120.0, 1.0),
        );
        let gaps = [GapRec {
            exchange: "binance".into(),
            stream: "open_interest".into(),
            from_ts: T0 + MIN + 30_000,
            to_ts: T0 + 3 * MIN + 500,
        }];
        let r = rollup(
            &fine,
            &gaps,
            &params(
                GroupBy::Symbol,
                &[Metric::OiEndBase, Metric::OiChangeBase, Metric::LiqSumUsd],
            ),
        )
        .unwrap();
        assert_eq!(r.rows.len(), 4);
        assert!(r.partial);
        let partial: Vec<bool> = r.rows.iter().map(|r| r.partial).collect();
        assert_eq!(partial, [false, true, true, true]);
        // The hole rows carry no values at all: null, not zero.
        for row in &r.rows[1..3] {
            for v in &row.values {
                assert_eq!(v.value, None, "{row:?}");
            }
        }
        // The change across the hole is not a one-step change.
        assert_eq!(value(&r.rows[3], Metric::OiChangeBase), None);
        assert_eq!(value(&r.rows[3], Metric::OiEndBase), Some(120.0));
        // A liquidation-stream gap does not mark an OI-only answer partial.
        let liq_gap = [GapRec {
            exchange: "binance".into(),
            stream: "liquidations".into(),
            from_ts: T0,
            to_ts: T0 + 10 * MIN,
        }];
        let r = rollup(
            &fine,
            &liq_gap,
            &params(GroupBy::Symbol, &[Metric::OiEndBase]),
        )
        .unwrap();
        assert!(
            !r.rows[0].partial,
            "a liquidation gap does not touch an OI-only answer"
        );
        // The writer's own gap (exchange "*") affects everything.
        let writer = [GapRec {
            exchange: "*".into(),
            stream: "redis-writer".into(),
            from_ts: T0 - 1,
            to_ts: T0 + 1,
        }];
        let r = rollup(
            &fine,
            &writer,
            &params(GroupBy::Symbol, &[Metric::OiEndBase]),
        )
        .unwrap();
        assert!(r.rows[0].partial);
    }

    #[test]
    fn liquidation_only_answers_zero_fill_between_events() {
        let mut fine = FineData::default();
        let s = BinStep::Minute1;
        fine.add_liquidation(
            s,
            &liq("binance", "AAA-USDT-PERP", T0 + 1_000, Side::Long, 10.0),
        );
        fine.add_liquidation(
            s,
            &liq("binance", "AAA-USDT-PERP", T0 + 3 * MIN, Side::Short, 20.0),
        );
        let r = rollup(
            &fine,
            &[],
            &params(GroupBy::Symbol, &[Metric::LiqSumUsd, Metric::LiqCount]),
        )
        .unwrap();
        let sums: Vec<_> = r.rows.iter().map(|r| value(r, Metric::LiqSumUsd)).collect();
        assert_eq!(sums, [Some(10.0), Some(0.0), Some(0.0), Some(20.0)]);
        assert!(
            !r.partial,
            "event silence is not a hole when only liquidations are asked"
        );
    }

    #[test]
    fn rows_are_capped() {
        let mut fine = FineData::default();
        for i in 0..100 {
            fine.add_liquidation(
                BinStep::Minute1,
                &liq("binance", "AAA-USDT-PERP", T0 + i * MIN, Side::Long, 1.0),
            );
        }
        let mut p = params(GroupBy::Symbol, &[Metric::LiqCount]);
        p.end = T0 + 100 * MIN;
        p.max_rows = 50;
        assert_eq!(
            rollup(&fine, &[], &p).unwrap_err(),
            RollupError::TooManyRows { rows: 100, max: 50 }
        );
    }

    #[test]
    fn base_asset_rule() {
        assert_eq!(base_asset("BTC-USDT-PERP"), "BTC");
        assert_eq!(base_asset("1000PEPE-USDT-PERP"), "1000PEPE");
        assert_eq!(base_asset("ODD"), "ODD");
        assert_eq!(base_asset(""), "");
    }

    #[test]
    fn needs_follow_the_metrics() {
        assert_eq!(
            Needs::of(&[Metric::LiqSumUsd]),
            Needs {
                liq: true,
                oi: false,
                mark: false,
                funding: false
            }
        );
        assert_eq!(
            Needs::of(&[Metric::OiEndBase]),
            Needs {
                liq: false,
                oi: true,
                mark: false,
                funding: false
            }
        );
        let n = Needs::of(&[Metric::OiChangeUsd]);
        assert!(n.oi && n.mark);
        assert!(Needs::of(&[Metric::LiqToOiRatio]).liq);
        assert!(Needs::of(&[Metric::MarkHigh]).mark);
        let f = Needs::of(&[Metric::BasisPct]);
        assert!(f.oi && f.funding && !f.mark && !f.liq);
    }
}
