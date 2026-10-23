//! REST rate control: shared pause (429/418), used-weight tracking, slowdown and a token bucket.
//!
//! Binance limits request weight to 2400 per minute per IP; every call used here weighs 1. The connector
//! plans for 60% of that, leaving room for retries, `exchangeInfo` and other software on the same address.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time::{Instant, sleep, sleep_until};
use tokio_util::sync::CancellationToken;

use super::status::BinanceStatus;
use crate::ws::Clock;

/// Weight limit per minute and IP (`rateLimits` of `exchangeInfo`, S-1).
pub const WEIGHT_LIMIT_PER_MIN: u32 = 2400;
/// Share of the limit the poller plans for: 1440 weight per minute.
pub const BUDGET_FRACTION: f64 = 0.6;
/// Slowdown stops growing at this factor (the poller never stretches its spacing more than 4x).
pub const MAX_SLOWDOWN: f64 = 4.0;

/// Planned requests per minute (`2400 * 0.6`).
pub fn budget_per_min() -> f64 {
    f64::from(WEIGHT_LIMIT_PER_MIN) * BUDGET_FRACTION
}

/// Spacing multiplier for the observed `X-MBX-USED-WEIGHT-1M`: 1 up to 60% of the limit, then growing
/// linearly to [`MAX_SLOWDOWN`] at 100%.
pub fn slowdown_factor(used_weight: u32) -> f64 {
    let limit = f64::from(WEIGHT_LIMIT_PER_MIN);
    let soft = limit * BUDGET_FRACTION;
    let used = f64::from(used_weight);
    if used <= soft {
        1.0
    } else {
        (1.0 + (used - soft) / (limit - soft) * (MAX_SLOWDOWN - 1.0)).min(MAX_SLOWDOWN)
    }
}

/// Effective polling interval: `max(configured, N / 1440 * 60)` seconds, so N symbols never need more than
/// 60% of the weight limit. Recomputed whenever N changes.
pub fn effective_interval(configured_secs: u64, symbols: usize) -> Duration {
    let minimum = symbols as f64 / budget_per_min() * 60.0;
    Duration::from_secs_f64((configured_secs as f64).max(minimum))
}

/// A finished REST pause, in wall-clock milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PauseSpan {
    /// When the pause began.
    pub from_ms: i64,
    /// When it was planned to end (the longest one, if it was extended).
    pub to_ms: i64,
    /// Whether any part of it was a 418 ban.
    pub banned: bool,
}

/// Pause and weight state shared by every REST caller of the connector.
pub struct RateState {
    status: Arc<BinanceStatus>,
    clock: Arc<dyn Clock>,
    pause_until: Mutex<Option<Instant>>,
    /// The pause in progress.
    current: Mutex<Option<PauseSpan>>,
    /// Pauses that ended and were not yet taken by [`RateState::take_finished`].
    finished: Mutex<Vec<PauseSpan>>,
}

impl RateState {
    /// New state without a pause.
    pub fn new(status: Arc<BinanceStatus>, clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self {
            status,
            clock,
            pause_until: Mutex::new(None),
            current: Mutex::new(None),
            finished: Mutex::new(Vec::new()),
        })
    }

    /// Records the used weight from a response header.
    pub fn record_weight(&self, used: u32) {
        self.status.used_weight_1m.store(used, Ordering::Relaxed);
    }

    /// Current spacing multiplier from the last observed weight.
    pub fn slowdown(&self) -> f64 {
        slowdown_factor(self.status.used_weight_1m.load(Ordering::Relaxed))
    }

    /// Stops all REST traffic for `d`. A longer pause is never shortened by a later, shorter one.
    pub fn pause(&self, d: Duration, banned: bool) {
        let until = Instant::now() + d;
        let now_ms = self.clock.now_ms();
        let until_ms = now_ms + d.as_millis() as i64;
        let mut guard = self.pause_until.lock().unwrap_or_else(|e| e.into_inner());
        {
            let mut cur = self.current.lock().unwrap_or_else(|e| e.into_inner());
            let span = cur.get_or_insert(PauseSpan {
                from_ms: now_ms,
                to_ms: until_ms,
                banned: false,
            });
            span.to_ms = span.to_ms.max(until_ms);
            span.banned |= banned;
        }
        if guard.is_none_or(|cur| until > cur) {
            *guard = Some(until);
            self.status
                .paused_until_ms
                .store(until_ms, Ordering::Relaxed);
        }
        if banned {
            self.status.banned.store(true, Ordering::Relaxed);
        }
    }

    /// Pauses that have ended since the last call, oldest first.
    pub fn take_finished(&self) -> Vec<PauseSpan> {
        std::mem::take(&mut *self.finished.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Waits while a pause is active. Returns `None` on shutdown and `Some(waited)` otherwise.
    pub async fn wait_unpaused(&self, shutdown: &CancellationToken) -> Option<bool> {
        let mut waited = false;
        loop {
            let until = *self.pause_until.lock().unwrap_or_else(|e| e.into_inner());
            match until {
                Some(t) if t > Instant::now() => {
                    waited = true;
                    tokio::select! {
                        _ = sleep_until(t) => {}
                        _ = shutdown.cancelled() => return None,
                    }
                }
                Some(_) => {
                    let mut guard = self.pause_until.lock().unwrap_or_else(|e| e.into_inner());
                    if guard.is_some_and(|t| t <= Instant::now()) {
                        *guard = None;
                        if let Some(span) = self
                            .current
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .take()
                        {
                            self.finished
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .push(span);
                        }
                        self.status.banned.store(false, Ordering::Relaxed);
                        self.status.paused_until_ms.store(0, Ordering::Relaxed);
                    }
                }
                None => return Some(waited),
            }
        }
    }
}

/// Token bucket with reservation: concurrent callers queue up behind each other instead of bursting.
pub struct TokenBucket {
    capacity: f64,
    rate_per_sec: f64,
    state: Mutex<(f64, Instant)>,
}

impl TokenBucket {
    /// Bucket that starts full.
    pub fn new(capacity: u32, rate_per_sec: f64) -> Self {
        Self {
            capacity: f64::from(capacity.max(1)),
            rate_per_sec,
            state: Mutex::new((f64::from(capacity.max(1)), Instant::now())),
        }
    }

    /// Takes one token, waiting if none is available.
    pub async fn acquire(&self) {
        let wait = {
            let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            let elapsed = now.saturating_duration_since(s.1).as_secs_f64();
            s.0 = (s.0 + elapsed * self.rate_per_sec).min(self.capacity);
            s.1 = now;
            s.0 -= 1.0;
            if s.0 >= 0.0 {
                return;
            }
            Duration::from_secs_f64(-s.0 / self.rate_per_sec)
        };
        sleep(wait).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_scales_with_symbol_count() {
        // 560 symbols need 23.33 s at 60% of the limit; the configured 30 s wins.
        assert_eq!(effective_interval(30, 560), Duration::from_secs(30));
        // 700 symbols: 29.17 s, still below 30 s.
        assert_eq!(effective_interval(30, 700), Duration::from_secs(30));
        // 1440 symbols need exactly 60 s.
        assert_eq!(effective_interval(30, 1440), Duration::from_secs(60));
        let s = effective_interval(10, 560).as_secs_f64();
        assert!((s - 23.333).abs() < 0.01, "{s}");
        assert_eq!(effective_interval(30, 0), Duration::from_secs(30));
    }

    #[test]
    fn slowdown_starts_at_sixty_percent() {
        assert_eq!(slowdown_factor(0), 1.0);
        assert_eq!(slowdown_factor(1440), 1.0);
        assert!((slowdown_factor(1920) - 2.5).abs() < 1e-9);
        assert_eq!(slowdown_factor(2400), MAX_SLOWDOWN);
        assert_eq!(slowdown_factor(9999), MAX_SLOWDOWN);
    }

    #[tokio::test(start_paused = true)]
    async fn bucket_limits_the_rate() {
        let b = TokenBucket::new(2, 10.0);
        let start = Instant::now();
        for _ in 0..12 {
            b.acquire().await;
        }
        // 2 from the burst, then 10 more at 10/s.
        let took = start.elapsed().as_secs_f64();
        assert!((0.9..=1.1).contains(&took), "{took}");
    }
}
