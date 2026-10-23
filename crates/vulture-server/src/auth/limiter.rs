//! Failed-attempt limiter: a small defence against guessing the token.

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Limiter settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimiterConfig {
    /// Failures per peer address within `window` after which the address is blocked.
    pub max_failures: usize,
    /// Length of the sliding window.
    pub window: Duration,
}

impl Default for LimiterConfig {
    fn default() -> Self {
        Self {
            max_failures: 10,
            window: Duration::from_secs(60),
        }
    }
}

/// Counts failed authentications per peer IP address (ports differ between connections). Once an address has
/// `max_failures` failures inside the window, further *failed* attempts from it are refused with
/// `RESOURCE_EXHAUSTED` until old failures leave the window. The limiter only answers "is this address blocked";
/// the interceptor consults it after the token check failed, so a correct token is never refused.
/// Time is passed in, so tests need no sleeping.
pub struct FailureLimiter {
    cfg: LimiterConfig,
    failures: Mutex<HashMap<Option<IpAddr>, VecDeque<Instant>>>,
}

/// Upper bound for tracked addresses; beyond it expired entries are dropped first.
const MAX_TRACKED: usize = 10_000;

impl FailureLimiter {
    /// New limiter.
    pub fn new(cfg: LimiterConfig) -> Self {
        Self {
            cfg,
            failures: Mutex::new(HashMap::new()),
        }
    }

    /// Whether `peer` is blocked at `now`. `None` (unknown peer) shares one bucket.
    pub fn is_blocked(&self, peer: Option<IpAddr>, now: Instant) -> bool {
        let mut map = self.lock();
        let Some(queue) = map.get_mut(&peer) else {
            return false;
        };
        self.expire(queue, now);
        let blocked = queue.len() >= self.cfg.max_failures;
        if queue.is_empty() {
            map.remove(&peer);
        }
        blocked
    }

    /// Records a failed attempt. Returns `true` if this failure made the address blocked.
    pub fn record_failure(&self, peer: Option<IpAddr>, now: Instant) -> bool {
        let mut map = self.lock();
        if map.len() >= MAX_TRACKED && !map.contains_key(&peer) {
            let window = self.cfg.window;
            map.retain(|_, q| q.back().is_some_and(|t| now.duration_since(*t) < window));
        }
        let queue = map.entry(peer).or_default();
        self.expire(queue, now);
        // Bounded memory per address even under a flood.
        if queue.len() < self.cfg.max_failures {
            queue.push_back(now);
        }
        queue.len() == self.cfg.max_failures
    }

    fn expire(&self, queue: &mut VecDeque<Instant>, now: Instant) {
        while queue
            .front()
            .is_some_and(|t| now.duration_since(*t) >= self.cfg.window)
        {
            queue.pop_front();
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Option<IpAddr>, VecDeque<Instant>>> {
        self.failures.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(last: u8) -> Option<IpAddr> {
        Some(IpAddr::from([10, 0, 0, last]))
    }

    #[test]
    fn blocks_after_the_limit_and_recovers_when_the_window_passes() {
        let limiter = FailureLimiter::new(LimiterConfig {
            max_failures: 3,
            window: Duration::from_secs(60),
        });
        let t0 = Instant::now();
        for i in 0..2 {
            assert!(!limiter.record_failure(ip(1), t0 + Duration::from_secs(i)));
            assert!(!limiter.is_blocked(ip(1), t0 + Duration::from_secs(i)));
        }
        assert!(
            limiter.record_failure(ip(1), t0 + Duration::from_secs(2)),
            "third failure blocks"
        );
        assert!(limiter.is_blocked(ip(1), t0 + Duration::from_secs(3)));
        // Another address is unaffected.
        assert!(!limiter.is_blocked(ip(2), t0 + Duration::from_secs(3)));
        // Failures leave the window one by one.
        assert!(limiter.is_blocked(ip(1), t0 + Duration::from_secs(59)));
        assert!(!limiter.is_blocked(ip(1), t0 + Duration::from_secs(60)));
        assert!(!limiter.is_blocked(ip(1), t0 + Duration::from_secs(200)));
    }

    #[test]
    fn a_flood_does_not_grow_memory_or_extend_the_block() {
        let limiter = FailureLimiter::new(LimiterConfig {
            max_failures: 2,
            window: Duration::from_secs(10),
        });
        let t0 = Instant::now();
        for i in 0..1000 {
            limiter.record_failure(ip(1), t0 + Duration::from_millis(i));
        }
        assert_eq!(limiter.lock().get(&ip(1)).unwrap().len(), 2);
        // The block ends about one window after the first failures, not after the last attempt.
        assert!(!limiter.is_blocked(ip(1), t0 + Duration::from_secs(11)));
    }

    #[test]
    fn unknown_peers_share_a_bucket() {
        let limiter = FailureLimiter::new(LimiterConfig {
            max_failures: 1,
            window: Duration::from_secs(10),
        });
        let t0 = Instant::now();
        limiter.record_failure(None, t0);
        assert!(limiter.is_blocked(None, t0));
        assert!(!limiter.is_blocked(ip(1), t0));
    }
}
