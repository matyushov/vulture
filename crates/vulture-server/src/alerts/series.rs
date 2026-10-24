//! Per-symbol ring buffers of open interest samples and the window arithmetic on them.
//!
//! The buffer keeps `retention` of history (the longest window plus its tolerance). A *break* marks the
//! latest moment at which the data is not continuous (a hole between two samples, or a `Gap` event): a window
//! whose reference point lies before the break is not evaluated, because the change across a hole is not a
//! market move.

use std::collections::VecDeque;

/// One open interest snapshot. `NaN` marks a missing value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    /// Receive time, ms.
    pub ts: i64,
    /// OI in the base coin.
    pub oi_base: f64,
    /// OI in USD (`NaN` when unknown).
    pub oi_usd: f64,
    /// Mark price (`NaN` when unknown).
    pub mark: f64,
}

/// `a >= b`, false when either is `NaN` (written positively so a missing value never passes a threshold).
pub fn ge(a: f64, b: f64) -> bool {
    a >= b
}

/// `a > b`, false when either is `NaN`.
pub fn gt(a: f64, b: f64) -> bool {
    a > b
}

/// Bytes one sample occupies.
pub const SAMPLE_BYTES: usize = std::mem::size_of::<Sample>();

/// What happened to a pushed sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Push {
    /// Stored.
    Added,
    /// Not newer than the last sample, or closer than the spacing: ignored.
    Ignored,
}

/// Limits of a series.
#[derive(Debug, Clone, Copy)]
pub struct SeriesLimits {
    /// History kept, ms.
    pub retention_ms: i64,
    /// Hard cap on the samples.
    pub max_samples: usize,
    /// Minimum distance between two stored samples, ms.
    pub spacing_ms: i64,
    /// A pause longer than this is a hole, ms.
    pub hole_ms: i64,
}

/// The samples of one symbol on one exchange.
#[derive(Debug, Default)]
pub struct Series {
    samples: VecDeque<Sample>,
    /// Windows with a reference point before this are invalid.
    break_ts: i64,
}

/// The change over a window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Change {
    /// The reference (older) sample.
    pub reference: Sample,
    /// The newest sample.
    pub now: Sample,
    /// `now.oi_base - reference.oi_base`.
    pub abs_base: f64,
    /// Percent of the reference.
    pub pct: f64,
    /// `abs_base` in USD at the current mark price, `NaN` when no price is known.
    pub usd: f64,
}

/// Tolerance for matching a reference sample to `now - window`, ms: `window/8` between 15 s and 300 s.
pub fn tolerance_ms(window_ms: i64) -> i64 {
    (window_ms / 8).clamp(15_000, 300_000)
}

impl Series {
    /// Empty series.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a sample.
    pub fn push(&mut self, s: Sample, limits: &SeriesLimits) -> Push {
        if let Some(last) = self.samples.back() {
            if s.ts <= last.ts || s.ts - last.ts < limits.spacing_ms {
                return Push::Ignored;
            }
            if s.ts - last.ts > limits.hole_ms {
                self.break_ts = self.break_ts.max(s.ts);
            }
        }
        if self.samples.capacity() == 0 {
            // Four hours at 30 s is 480 samples: allocate once instead of growing by doubling, which leaves
            // freed fragments behind.
            self.samples.reserve_exact(512.min(limits.max_samples));
        }
        self.samples.push_back(s);
        while self
            .samples
            .front()
            .is_some_and(|f| f.ts < s.ts - limits.retention_ms)
            || self.samples.len() > limits.max_samples
        {
            self.samples.pop_front();
        }
        Push::Added
    }

    /// Marks the data as discontinuous until `ts` (a `Gap` ending at `ts`).
    pub fn mark_break(&mut self, ts: i64) {
        self.break_ts = self.break_ts.max(ts);
    }

    /// Latest break.
    pub fn break_ts(&self) -> i64 {
        self.break_ts
    }

    /// Number of samples.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// True without samples.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Allocated bytes of the buffer.
    pub fn capacity_bytes(&self) -> usize {
        self.samples.capacity() * SAMPLE_BYTES
    }

    /// The newest sample.
    pub fn last(&self) -> Option<&Sample> {
        self.samples.back()
    }

    /// The oldest sample.
    pub fn first(&self) -> Option<&Sample> {
        self.samples.front()
    }

    /// Sample at `index`.
    pub fn get(&self, index: usize) -> Option<&Sample> {
        self.samples.get(index)
    }

    /// The sample closest to `target` within `tol`, not before `valid_from` and not at or after `before_idx`.
    fn nearest(&self, target: i64, tol: i64, valid_from: i64, before_idx: usize) -> Option<usize> {
        let p = self.samples.partition_point(|s| s.ts < target);
        let mut best: Option<(i64, usize)> = None;
        for idx in [p.checked_sub(1), Some(p)].into_iter().flatten() {
            if idx >= before_idx {
                continue;
            }
            let Some(s) = self.samples.get(idx) else {
                continue;
            };
            let d = (s.ts - target).abs();
            if d <= tol && s.ts >= valid_from && best.is_none_or(|(bd, _)| d < bd) {
                best = Some((d, idx));
            }
        }
        best.map(|(_, i)| i)
    }

    /// Change of `oi_base` from the sample nearest to `now - window` to the sample at `at` (default: the newest).
    /// `None` when there is no such reference, it lies before a break (`extra_break` is the exchange or global
    /// one), or `oi_base` of the reference is not positive.
    pub fn change(&self, window_ms: i64, extra_break: i64) -> Option<Change> {
        let idx = self.samples.len().checked_sub(1)?;
        self.change_at(idx, window_ms, extra_break)
    }

    fn change_at(&self, idx: usize, window_ms: i64, extra_break: i64) -> Option<Change> {
        let now = *self.samples.get(idx)?;
        let valid_from = self.break_ts.max(extra_break);
        let target = now.ts - window_ms;
        let r = self.nearest(target, tolerance_ms(window_ms), valid_from, idx)?;
        let reference = *self.samples.get(r)?;
        // The reference must be clearly older than the newest sample.
        if now.ts - reference.ts < window_ms / 2
            || !gt(reference.oi_base, 0.0)
            || !now.oi_base.is_finite()
        {
            return None;
        }
        let abs_base = now.oi_base - reference.oi_base;
        let price = if now.mark.is_finite() && now.mark > 0.0 {
            now.mark
        } else if now.oi_usd.is_finite() && now.oi_base > 0.0 {
            now.oi_usd / now.oi_base
        } else {
            f64::NAN
        };
        Some(Change {
            reference,
            now,
            abs_base,
            pct: 100.0 * abs_base / reference.oi_base,
            usd: abs_base * price,
        })
    }

    /// Distribution of the symbol's own window changes before the current window: median and robust sigma
    /// (1.4826 x MAD) of the percent changes of `window` ending at samples older than `newest - window`.
    /// At most about `max_points` points (evenly strided). `None` when fewer than `min_points` are valid.
    pub fn baseline(
        &self,
        window_ms: i64,
        extra_break: i64,
        min_points: usize,
        max_points: usize,
    ) -> Option<Baseline> {
        let newest = self.samples.back()?.ts;
        let end = self.samples.partition_point(|s| s.ts <= newest - window_ms);
        if end == 0 {
            return None;
        }
        let stride = (end / max_points.max(1)).max(1);
        let mut values = Vec::with_capacity(end / stride + 1);
        let mut i = end - 1;
        loop {
            if let Some(c) = self.change_at(i, window_ms, extra_break) {
                values.push(c.pct);
            }
            match i.checked_sub(stride) {
                Some(n) => i = n,
                None => break,
            }
        }
        if values.len() < min_points {
            return None;
        }
        let (median, mad) = median_mad(&mut values);
        Some(Baseline {
            median,
            sigma: robust_sigma(&values, mad),
            points: values.len(),
            scaled: false,
        })
    }

    /// Fallback when the empirical distribution of `window` is unavailable (the 4 h window has no earlier
    /// history): the typical step-to-step change of the symbol scaled to the window by the square root of the
    /// number of steps.
    pub fn scaled_baseline(
        &self,
        window_ms: i64,
        extra_break: i64,
        min_points: usize,
    ) -> Option<Baseline> {
        let valid_from = self.break_ts.max(extra_break);
        let mut steps = Vec::with_capacity(self.samples.len());
        let mut dts = Vec::with_capacity(self.samples.len());
        let mut prev: Option<&Sample> = None;
        for s in &self.samples {
            if let Some(p) = prev
                && p.ts >= valid_from
                && p.oi_base > 0.0
            {
                steps.push(100.0 * (s.oi_base - p.oi_base) / p.oi_base);
                dts.push((s.ts - p.ts) as f64);
            }
            prev = Some(s);
        }
        if steps.len() < min_points {
            return None;
        }
        let (_, mad) = median_mad(&mut steps);
        let step_sigma = robust_sigma(&steps, mad);
        let (dt, _) = median_mad(&mut dts);
        if dt <= 0.0 {
            return None;
        }
        let n = (window_ms as f64 / dt).max(1.0);
        Some(Baseline {
            median: 0.0,
            sigma: step_sigma * n.sqrt(),
            points: steps.len(),
            scaled: true,
        })
    }
}

/// Robust spread of sorted `values` with the given MAD: the larger of `1.4826 x MAD` and the 10th-to-90th
/// percentile spread divided by 2.563 (the same quantity for a normal distribution). The second term matters for
/// a symbol that jumps often: more than half of its changes can be near zero, which makes the MAD collapse
/// although the symbol is plainly restless.
fn robust_sigma(sorted: &[f64], mad: f64) -> f64 {
    let n = sorted.len();
    if n < 2 {
        return 1.4826 * mad;
    }
    let at = |q: f64| sorted[(((n - 1) as f64) * q).round() as usize];
    (1.4826 * mad).max((at(0.9) - at(0.1)) / 2.563)
}

/// Median and robust spread of a symbol's window changes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Baseline {
    /// Median percent change.
    pub median: f64,
    /// Robust standard deviation: `max(1.4826 x MAD, (p90 - p10) / 2.563)`.
    pub sigma: f64,
    /// Points used.
    pub points: usize,
    /// True for the step-volatility fallback.
    pub scaled: bool,
}

/// Median and median absolute deviation. Sorts `values` in place. Empty input gives zeros.
pub fn median_mad(values: &mut [f64]) -> (f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0);
    }
    let median = median_of(values);
    let mut dev: Vec<f64> = values.iter().map(|v| (v - median).abs()).collect();
    (median, median_of(&mut dev))
}

fn median_of(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn limits() -> SeriesLimits {
        SeriesLimits {
            retention_ms: 4 * 3600 * 1000 + 600_000,
            max_samples: 1024,
            spacing_ms: 15_000,
            hole_ms: 300_000,
        }
    }

    fn sample(ts: i64, oi: f64) -> Sample {
        Sample {
            ts,
            oi_base: oi,
            oi_usd: oi * 100.0,
            mark: 100.0,
        }
    }

    fn fill(series: &mut Series, n: i64, f: impl Fn(i64) -> f64) {
        for i in 0..n {
            series.push(sample(i * 30_000, f(i)), &limits());
        }
    }

    #[test]
    fn change_over_five_minutes() {
        let mut s = Series::new();
        fill(&mut s, 21, |i| if i < 20 { 1000.0 } else { 1100.0 });
        let c = s.change(300_000, 0).unwrap();
        assert_eq!(c.reference.ts, 300_000);
        assert!((c.pct - 10.0).abs() < 1e-9);
        assert!((c.abs_base - 100.0).abs() < 1e-9);
        assert!((c.usd - 10_000.0).abs() < 1e-6);
    }

    #[test]
    fn one_minute_window_works_with_thirty_second_samples() {
        let mut s = Series::new();
        fill(&mut s, 10, |i| 1000.0 + i as f64);
        let c = s.change(60_000, 0).unwrap();
        assert_eq!(c.now.ts - c.reference.ts, 60_000);
        // Jittery sampling: still within the tolerance.
        let mut j = Series::new();
        for (i, ts) in [0, 31_000, 59_000, 91_000, 118_000, 151_000]
            .into_iter()
            .enumerate()
        {
            j.push(sample(ts, 1000.0 + i as f64), &limits());
        }
        assert!(j.change(60_000, 0).is_some());
    }

    #[test]
    fn missing_reference_gives_none() {
        let mut s = Series::new();
        s.push(sample(0, 1000.0), &limits());
        s.push(sample(30_000, 1001.0), &limits());
        assert!(s.change(300_000, 0).is_none(), "not enough history");
        // Samples 10 minutes apart but a 5 minute window: no reference near now - 5m.
        let mut h = Series::new();
        h.push(
            sample(0, 1000.0),
            &SeriesLimits {
                hole_ms: 10_000_000,
                ..limits()
            },
        );
        h.push(
            sample(600_000, 1200.0),
            &SeriesLimits {
                hole_ms: 10_000_000,
                ..limits()
            },
        );
        assert!(h.change(300_000, 0).is_none());
    }

    #[test]
    fn hole_and_gap_invalidate_windows() {
        let mut s = Series::new();
        fill(&mut s, 20, |_| 1000.0);
        // A 20 minute pause (symbol untracked), then a very different value.
        s.push(sample(19 * 30_000 + 1_200_000, 1500.0), &limits());
        assert!(s.change(300_000, 0).is_none());
        assert!(s.break_ts() > 0);
        let mut g = Series::new();
        fill(&mut g, 20, |_| 1000.0);
        assert!(g.change(300_000, 0).is_some());
        assert!(
            g.change(300_000, 400_000).is_none(),
            "an exchange level break after the reference"
        );
        let mut g = Series::new();
        fill(&mut g, 20, |_| 1000.0);
        g.mark_break(450_000);
        assert!(
            g.change(60_000, 0).is_some(),
            "a window entirely after the break is fine"
        );
        assert!(
            g.change(300_000, 0).is_none(),
            "a window reaching back across it is not"
        );
    }

    #[test]
    fn duplicate_and_old_samples_are_ignored() {
        let mut s = Series::new();
        assert_eq!(s.push(sample(1000, 1.0), &limits()), Push::Added);
        assert_eq!(s.push(sample(1000, 2.0), &limits()), Push::Ignored);
        assert_eq!(s.push(sample(500, 2.0), &limits()), Push::Ignored);
        assert_eq!(
            s.push(sample(5000, 2.0), &limits()),
            Push::Ignored,
            "inside the spacing"
        );
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn retention_and_cap_bound_the_buffer() {
        let mut s = Series::new();
        for i in 0..3000 {
            s.push(sample(i * 30_000, 1000.0), &limits());
        }
        assert!(s.len() <= 1024);
        let span = s.last().unwrap().ts - s.first().unwrap().ts;
        assert!(span <= limits().retention_ms);
    }

    #[test]
    fn baseline_of_noisy_and_quiet_series() {
        let mut quiet = Series::new();
        fill(&mut quiet, 400, |i| 1000.0 + (i % 3) as f64 * 0.1);
        let b = quiet.baseline(300_000, 0, 30, 200).unwrap();
        assert!(b.sigma < 0.1, "{b:?}");
        let mut noisy = Series::new();
        // A sawtooth with a ~6% swing every few samples.
        fill(&mut noisy, 400, |i| {
            if (i / 5) % 2 == 0 { 1000.0 } else { 1060.0 }
        });
        let b = noisy.baseline(300_000, 0, 30, 200).unwrap();
        assert!(b.sigma > 1.0 || b.median.abs() < 1.0, "{b:?}");
        let mut short = Series::new();
        fill(&mut short, 30, |_| 1000.0);
        assert!(short.baseline(300_000, 0, 30, 200).is_none());
        assert!(short.scaled_baseline(14_400_000, 0, 60).is_none());
    }

    #[test]
    fn scaled_baseline_covers_the_longest_window() {
        let mut s = Series::new();
        fill(&mut s, 480, |i| 1000.0 + ((i * 7) % 5) as f64);
        assert!(s.baseline(14_400_000, 0, 30, 200).is_none());
        let b = s.scaled_baseline(14_400_000, 0, 30).unwrap();
        assert!(b.scaled && b.sigma > 0.0);
    }

    #[test]
    fn median_mad_basics() {
        let mut v = [1.0, 2.0, 3.0, 4.0, 100.0];
        let (m, mad) = median_mad(&mut v);
        assert_eq!(m, 3.0);
        assert_eq!(mad, 1.0);
        assert_eq!(median_mad(&mut []), (0.0, 0.0));
    }
}
