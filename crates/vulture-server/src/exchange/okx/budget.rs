//! Request budget of the subscription changes.
//!
//! OKX allows 480 subscribe/unsubscribe/login requests per hour and connection and disables a connection that
//! exceeds it. The connector keeps one sliding-window budget for all its connections (stricter than the per
//! connection limit) at a fraction of the documented number, and applies tracked-set changes only when the
//! budget covers the requests they need. Rapid changes are first collapsed by a settle delay (see
//! `OkxTuning::settle`), so a burst of owner edits costs one diff, not one per edit.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::time::Instant;

/// Sliding-window budget of requests.
#[derive(Debug)]
pub struct RequestBudget {
    limit: usize,
    window: Duration,
    used: VecDeque<(Instant, usize)>,
}

impl RequestBudget {
    /// A budget of `limit` requests per `window`.
    pub fn new(limit: usize, window: Duration) -> Self {
        Self {
            limit: limit.max(1),
            window,
            used: VecDeque::new(),
        }
    }

    fn expire(&mut self, now: Instant) {
        while let Some((t, _)) = self.used.front() {
            if now.duration_since(*t) >= self.window {
                self.used.pop_front();
            } else {
                break;
            }
        }
    }

    /// Requests spent in the current window.
    pub fn spent(&mut self, now: Instant) -> usize {
        self.expire(now);
        self.used.iter().map(|(_, n)| n).sum()
    }

    /// Takes `n` requests now (`None`), or says how long to wait until they fit (`Some`). A single batch larger
    /// than the whole budget is admitted when nothing else was spent, so it can never wait forever.
    pub fn take(&mut self, n: usize, now: Instant) -> Option<Duration> {
        self.expire(now);
        let spent: usize = self.used.iter().map(|(_, c)| c).sum();
        let n = n.min(self.limit);
        if spent + n <= self.limit {
            if n > 0 {
                self.used.push_back((now, n));
            }
            return None;
        }
        // Wait until enough of the oldest spending has left the window.
        let mut free = self.limit - spent;
        for (t, c) in &self.used {
            free += c;
            if free >= n {
                return Some((*t + self.window).saturating_duration_since(now));
            }
        }
        Some(self.window)
    }
}

/// Requests needed to subscribe/unsubscribe `added`/`removed` symbols (two topics each) in batches.
pub fn requests_for(added: usize, removed: usize, args_per_request: usize) -> usize {
    let batches = |symbols: usize| (symbols * 2).div_ceil(args_per_request.max(1));
    batches(added) + batches(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn spending_waits_for_the_window() {
        let mut b = RequestBudget::new(10, Duration::from_secs(100));
        let t0 = Instant::now();
        assert_eq!(b.take(6, t0), None);
        assert_eq!(b.take(4, t0), None);
        assert_eq!(b.take(1, t0), Some(Duration::from_secs(100)));
        let later = t0 + Duration::from_secs(40);
        assert_eq!(b.take(1, later), Some(Duration::from_secs(60)));
        assert_eq!(b.take(1, t0 + Duration::from_secs(100)), None);
        assert_eq!(b.spent(t0 + Duration::from_secs(100)), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_oversized_batch_is_clamped_not_starved() {
        let mut b = RequestBudget::new(5, Duration::from_secs(60));
        let t0 = Instant::now();
        assert_eq!(b.take(50, t0), None);
        assert_eq!(b.take(1, t0), Some(Duration::from_secs(60)));
    }

    #[test]
    fn request_counts() {
        assert_eq!(requests_for(0, 0, 100), 0);
        assert_eq!(requests_for(1, 0, 100), 1);
        assert_eq!(requests_for(50, 0, 100), 1);
        assert_eq!(requests_for(51, 0, 100), 2);
        assert_eq!(requests_for(300, 5, 100), 6 + 1);
    }
}
