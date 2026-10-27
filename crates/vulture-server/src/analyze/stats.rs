//! Small statistics helpers of the analysis tools.

use serde::Serialize;

/// Quantile of a sorted slice by linear interpolation (`q` in 0..=1). `None` when empty.
pub fn quantile(sorted: &[f64], q: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let pos = q.clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let (lo, hi) = (pos.floor() as usize, pos.ceil() as usize);
    Some(sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo as f64))
}

/// Count, quartiles and mean of a sample.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Dist {
    /// Values.
    pub n: usize,
    /// First quartile.
    pub q1: Option<f64>,
    /// Median.
    pub median: Option<f64>,
    /// Third quartile.
    pub q3: Option<f64>,
    /// Mean.
    pub mean: Option<f64>,
}

impl Dist {
    /// Describes `values` (NaN and infinities are dropped).
    pub fn of(values: impl IntoIterator<Item = f64>) -> Self {
        let mut v: Vec<f64> = values.into_iter().filter(|x| x.is_finite()).collect();
        v.sort_by(f64::total_cmp);
        Self {
            n: v.len(),
            q1: quantile(&v, 0.25),
            median: quantile(&v, 0.5),
            q3: quantile(&v, 0.75),
            mean: (!v.is_empty()).then(|| v.iter().sum::<f64>() / v.len() as f64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_interpolate() {
        let v = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(quantile(&v, 0.0), Some(1.0));
        assert_eq!(quantile(&v, 0.5), Some(2.5));
        assert_eq!(quantile(&v, 1.0), Some(4.0));
        assert_eq!(quantile(&v, 0.25), Some(1.75));
        assert_eq!(quantile(&[], 0.5), None);
        assert_eq!(quantile(&[7.0], 0.9), Some(7.0));
    }

    #[test]
    fn distributions_ignore_non_numbers() {
        let d = Dist::of([3.0, f64::NAN, 1.0, 2.0, f64::INFINITY]);
        assert_eq!(d.n, 3);
        assert_eq!((d.q1, d.median, d.q3), (Some(1.5), Some(2.0), Some(2.5)));
        assert_eq!(d.mean, Some(2.0));
        assert_eq!(Dist::of([]), Dist::default());
    }
}
