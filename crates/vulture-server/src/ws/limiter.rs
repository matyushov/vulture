//! Connection budget of an exchange: new connections per time window and concurrent connections.
//!
//! One [`ConnectionLimiter`] is shared by every managed connection of an exchange. The manager takes a permit
//! before each connection attempt (failed attempts count as well, because the exchange counts them) and holds
//! it while the socket is open. Waiting is cancellable and uses the tokio clock, so tests run on paused time.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, sleep_until};

/// Share of the documented limits that is used (research/ws-reconnect.md: never exceed 50%).
pub const DEFAULT_BUDGET_FRACTION: f64 = 0.5;

/// Limits of one exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimiterConfig {
    /// New connections allowed per `window`.
    pub max_new_per_window: usize,
    /// Length of the window.
    pub window: Duration,
    /// Connections open at once.
    pub max_concurrent: usize,
}

impl LimiterConfig {
    /// `fraction` of the documented limits (at least 1 each).
    pub fn from_documented(
        new_per_window: usize,
        window: Duration,
        concurrent: usize,
        fraction: f64,
    ) -> Self {
        let scale = |n: usize| ((n as f64 * fraction).floor() as usize).max(1);
        Self {
            max_new_per_window: scale(new_per_window),
            window,
            max_concurrent: scale(concurrent),
        }
    }
}

/// Counters of the limiter, for metrics.
#[derive(Debug, Default)]
pub struct LimiterStats {
    /// Permits handed out.
    pub granted: AtomicU64,
    /// Requests that had to wait.
    pub delayed: AtomicU64,
}

/// The shared limiter.
#[derive(Debug)]
pub struct ConnectionLimiter {
    cfg: LimiterConfig,
    concurrent: Arc<Semaphore>,
    starts: Mutex<VecDeque<Instant>>,
    stats: LimiterStats,
}

/// Held while a connection (or an attempt) is in progress; dropping it frees the concurrency slot. The slot
/// in the time window stays used until the window passes.
#[derive(Debug)]
pub struct ConnectionPermit {
    _slot: OwnedSemaphorePermit,
}

impl ConnectionLimiter {
    /// A limiter with explicit limits.
    pub fn new(cfg: LimiterConfig) -> Arc<Self> {
        Arc::new(Self {
            concurrent: Arc::new(Semaphore::new(cfg.max_concurrent)),
            starts: Mutex::new(VecDeque::new()),
            stats: LimiterStats::default(),
            cfg,
        })
    }

    /// The configured limits.
    pub fn config(&self) -> LimiterConfig {
        self.cfg
    }

    /// Counters.
    pub fn stats(&self) -> &LimiterStats {
        &self.stats
    }

    /// Connections currently holding a permit.
    pub fn open(&self) -> usize {
        self.cfg.max_concurrent - self.concurrent.available_permits()
    }

    /// Waits until both limits allow a new connection and takes a permit. Cancel-safe: dropping the future
    /// before it resolves takes nothing.
    pub async fn acquire(&self) -> ConnectionPermit {
        let mut waited = false;
        let slot = match self.concurrent.clone().try_acquire_owned() {
            Ok(s) => s,
            Err(_) => {
                waited = true;
                self.concurrent
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("the limiter semaphore is never closed")
            }
        };
        loop {
            let wait_until = {
                let mut starts = self.starts.lock().unwrap_or_else(|e| e.into_inner());
                let now = Instant::now();
                while starts
                    .front()
                    .is_some_and(|t| now.duration_since(*t) >= self.cfg.window)
                {
                    starts.pop_front();
                }
                if starts.len() < self.cfg.max_new_per_window {
                    starts.push_back(now);
                    None
                } else {
                    Some(starts[0] + self.cfg.window)
                }
            };
            match wait_until {
                None => break,
                Some(t) => {
                    waited = true;
                    sleep_until(t).await;
                }
            }
        }
        self.stats.granted.fetch_add(1, Ordering::Relaxed);
        if waited {
            self.stats.delayed.fetch_add(1, Ordering::Relaxed);
        }
        ConnectionPermit { _slot: slot }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(new: usize, secs: u64, conc: usize) -> LimiterConfig {
        LimiterConfig {
            max_new_per_window: new,
            window: Duration::from_secs(secs),
            max_concurrent: conc,
        }
    }

    #[test]
    fn documented_limits_are_halved() {
        let c = LimiterConfig::from_documented(500, Duration::from_secs(300), 1000, 0.5);
        assert_eq!(c.max_new_per_window, 250);
        assert_eq!(c.max_concurrent, 500);
        assert_eq!(
            LimiterConfig::from_documented(1, Duration::from_secs(1), 1, 0.5).max_new_per_window,
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn window_blocks_the_next_attempt_until_it_slides() {
        let l = ConnectionLimiter::new(cfg(2, 10, 10));
        let t0 = Instant::now();
        drop(l.acquire().await);
        drop(l.acquire().await);
        assert_eq!(t0.elapsed(), Duration::ZERO);
        drop(l.acquire().await);
        assert_eq!(t0.elapsed(), Duration::from_secs(10));
        assert_eq!(l.stats().granted.load(Ordering::Relaxed), 3);
        assert_eq!(l.stats().delayed.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrency_cap_waits_for_a_release() {
        let l = ConnectionLimiter::new(cfg(100, 10, 1));
        let first = l.acquire().await;
        assert_eq!(l.open(), 1);
        let l2 = l.clone();
        let waiter = tokio::spawn(async move { l2.acquire().await });
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!waiter.is_finished());
        drop(first);
        let p = waiter.await.unwrap();
        assert_eq!(l.open(), 1);
        drop(p);
        assert_eq!(l.open(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_waiter_takes_nothing() {
        let l = ConnectionLimiter::new(cfg(1, 60, 5));
        drop(l.acquire().await);
        let l2 = l.clone();
        let w = tokio::spawn(async move { l2.acquire().await });
        tokio::time::sleep(Duration::from_secs(1)).await;
        w.abort();
        let _ = w.await;
        assert_eq!(l.open(), 0);
        let t0 = Instant::now();
        drop(l.acquire().await);
        assert_eq!(t0.elapsed(), Duration::from_secs(59));
    }

    #[tokio::test(start_paused = true)]
    async fn flapping_stays_within_the_window_budget() {
        let l = ConnectionLimiter::new(cfg(5, 60, 10));
        let t0 = Instant::now();
        let mut starts = Vec::new();
        for _ in 0..20 {
            drop(l.acquire().await);
            starts.push(t0.elapsed());
        }
        // No 60 s window holds more than 5 starts.
        for (i, s) in starts.iter().enumerate() {
            let in_window = starts
                .iter()
                .skip(i)
                .filter(|t| **t - *s < Duration::from_secs(60))
                .count();
            assert!(in_window <= 5, "{in_window} starts within a window");
        }
    }
}
