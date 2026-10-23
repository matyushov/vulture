//! Observable state of the Redis writer, for the future metrics and `Control` (V1-9).

use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering};

/// Connection state as seen by the writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreState {
    /// No write has been attempted yet.
    Unknown,
    /// The last write succeeded.
    Connected,
    /// The last write failed; events are buffered and retried with backoff.
    Disconnected,
}

/// Counters and gauges. Relaxed ordering: they are statistics.
#[derive(Debug, Default)]
pub struct WriterStatus {
    /// Entries written to Redis (gap events written by the writer included).
    pub written: AtomicU64,
    /// Successful pipelines.
    pub batches: AtomicU64,
    /// Failed write attempts (each one is retried after a backoff).
    pub retries: AtomicU64,
    /// Events currently waiting in memory (gauge).
    pub buffered: AtomicU64,
    /// Events dropped because the buffer was full.
    pub dropped: AtomicU64,
    /// `Gap` events written to describe dropped events.
    pub gaps_written: AtomicU64,
    /// Wall-clock ms of the last successful write; 0 if none yet (gauge).
    pub last_write_ms: AtomicI64,
    state: AtomicU8,
    last_error: Mutex<Option<String>>,
}

/// Plain copy of [`WriterStatus`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterSnapshot {
    /// See [`WriterStatus::written`].
    pub written: u64,
    /// See [`WriterStatus::batches`].
    pub batches: u64,
    /// See [`WriterStatus::retries`].
    pub retries: u64,
    /// See [`WriterStatus::buffered`].
    pub buffered: u64,
    /// See [`WriterStatus::dropped`].
    pub dropped: u64,
    /// See [`WriterStatus::gaps_written`].
    pub gaps_written: u64,
    /// See [`WriterStatus::last_write_ms`].
    pub last_write_ms: i64,
    /// Connection state.
    pub state: StoreState,
    /// Message of the last failure (no URL, no secrets), if any.
    pub last_error: Option<String>,
}

impl WriterStatus {
    pub(crate) fn set_state(&self, state: StoreState) {
        let v = match state {
            StoreState::Unknown => 0,
            StoreState::Connected => 1,
            StoreState::Disconnected => 2,
        };
        self.state.store(v, Ordering::Relaxed);
    }

    /// Current connection state.
    pub fn state(&self) -> StoreState {
        match self.state.load(Ordering::Relaxed) {
            1 => StoreState::Connected,
            2 => StoreState::Disconnected,
            _ => StoreState::Unknown,
        }
    }

    pub(crate) fn set_error(&self, message: String) {
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(message);
    }

    /// Message of the last failure.
    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// A consistent-enough copy for reporting.
    pub fn snapshot(&self) -> WriterSnapshot {
        WriterSnapshot {
            written: self.written.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            buffered: self.buffered.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            gaps_written: self.gaps_written.load(Ordering::Relaxed),
            last_write_ms: self.last_write_ms.load(Ordering::Relaxed),
            state: self.state(),
            last_error: self.last_error(),
        }
    }
}
