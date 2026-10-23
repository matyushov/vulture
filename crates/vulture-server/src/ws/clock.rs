//! Wall clock abstraction for event timestamps. Timers use the monotonic tokio clock instead.

/// Source of Unix epoch milliseconds, injectable so tests control `Gap` timestamps.
pub trait Clock: Send + Sync {
    /// Current wall-clock time, ms since the Unix epoch.
    fn now_ms(&self) -> i64;
}

/// The system wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}
