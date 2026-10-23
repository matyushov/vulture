//! Exponential backoff with equal jitter.

use std::time::Duration;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Doubling backoff: `d = min(max, min x 2^(n-1))` for the n-th consecutive failure, and the actual delay
/// is `d/2 + uniform(0, d/2)` ("equal jitter"), so a herd of connections drifts apart while every delay stays
/// at least half of the nominal one.
#[derive(Debug)]
pub struct Backoff {
    min: Duration,
    max: Duration,
    failures: u32,
    rng: StdRng,
}

impl Backoff {
    /// New backoff; a seed makes the jitter reproducible (tests).
    pub fn new(min: Duration, max: Duration, seed: Option<u64>) -> Self {
        let rng = match seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => rand::make_rng(),
        };
        Self {
            min,
            max,
            failures: 0,
            rng,
        }
    }

    /// Nominal (jitter-free) delay after `failures` consecutive failures.
    pub fn nominal(&self, failures: u32) -> Duration {
        let shift = failures.saturating_sub(1).min(32);
        self.min
            .checked_mul(1u32.checked_shl(shift).unwrap_or(u32::MAX))
            .unwrap_or(self.max)
            .min(self.max)
    }

    /// Registers a failure and returns the jittered delay before the next attempt.
    pub fn record_failure(&mut self) -> Duration {
        self.failures = self.failures.saturating_add(1);
        let nominal = self.nominal(self.failures);
        let half = nominal / 2;
        let jitter = self.rng.random_range(0.0..=1.0);
        half + half.mul_f64(jitter)
    }

    /// Consecutive failures since the last reset.
    pub fn failures(&self) -> u32 {
        self.failures
    }

    /// Forgets all failures (after a healthy period).
    pub fn reset(&mut self) {
        self.failures = 0;
    }

    /// Uniform random duration in `[-spread, +spread]` as a signed offset in ms (for scheduling jitter).
    pub fn symmetric_jitter_ms(&mut self, spread: Duration) -> i64 {
        let s = spread.as_millis() as i64;
        if s == 0 {
            0
        } else {
            self.rng.random_range(-s..=s)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: Duration = Duration::from_secs(1);

    #[test]
    fn nominal_doubles_and_caps() {
        let b = Backoff::new(S, 60 * S, Some(1));
        let got: Vec<u64> = (1..=9).map(|n| b.nominal(n).as_secs()).collect();
        assert_eq!(got, [1, 2, 4, 8, 16, 32, 60, 60, 60]);
        assert_eq!(b.nominal(500), 60 * S);
        assert_eq!(b.nominal(0), S);
    }

    #[test]
    fn delays_stay_in_equal_jitter_bounds() {
        let mut b = Backoff::new(S, 60 * S, Some(7));
        for n in 1..=40u32 {
            let nominal = b.nominal(n);
            let d = b.record_failure();
            assert!(
                d >= nominal / 2 && d <= nominal,
                "{n}: {d:?} vs {nominal:?}"
            );
        }
        assert_eq!(b.failures(), 40);
        b.reset();
        assert_eq!(b.failures(), 0);
        let d = b.record_failure();
        assert!(d <= S);
    }

    #[test]
    fn jitter_spreads_a_herd() {
        // 20 connections that failed together must not retry at the same instant.
        let delays: Vec<Duration> = (0..20)
            .map(|seed| Backoff::new(S, 60 * S, Some(seed)).record_failure())
            .collect();
        let min = delays.iter().min().unwrap();
        let max = delays.iter().max().unwrap();
        assert!(*max - *min > Duration::from_millis(200), "{delays:?}");
        let distinct: std::collections::HashSet<_> = delays.iter().collect();
        assert!(distinct.len() >= 15);
    }

    #[test]
    fn same_seed_same_sequence() {
        let run = |seed| {
            let mut b = Backoff::new(S, 60 * S, Some(seed));
            (0..10).map(|_| b.record_failure()).collect::<Vec<_>>()
        };
        assert_eq!(run(3), run(3));
        assert_ne!(run(3), run(4));
    }

    #[test]
    fn symmetric_jitter_is_bounded() {
        let mut b = Backoff::new(S, 60 * S, Some(5));
        for _ in 0..200 {
            let j = b.symmetric_jitter_ms(Duration::from_secs(600));
            assert!((-600_000..=600_000).contains(&j));
        }
        assert_eq!(b.symmetric_jitter_ms(Duration::ZERO), 0);
    }
}
