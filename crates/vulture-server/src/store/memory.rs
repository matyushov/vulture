//! In-memory [`RedisStore`] for unit tests: same entry semantics, exact trimming, failure injection.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures_util::future::BoxFuture;

use super::{EntryFields, RedisStore, StoreError, StoredEntry, Trim};
use crate::ws::{Clock, SystemClock};

#[derive(Default)]
struct Inner {
    entries: VecDeque<StoredEntry>,
    last_ms: i64,
    last_seq: u64,
    /// Sizes of the successful `append` calls, in order.
    batches: Vec<usize>,
}

/// A stream kept in memory. IDs are `<ms>-<seq>` from the injected clock, strictly increasing.
pub struct MemoryStore {
    inner: Mutex<Inner>,
    clock: Box<dyn Clock>,
    failing: AtomicBool,
    attempts: AtomicUsize,
    settings: Mutex<Option<Vec<(String, String)>>>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    /// Empty store on the system clock.
    pub fn new() -> Self {
        Self::with_clock(Box::new(SystemClock))
    }

    /// Empty store with an injected clock.
    pub fn with_clock(clock: Box<dyn Clock>) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            clock,
            failing: AtomicBool::new(false),
            attempts: AtomicUsize::new(0),
            settings: Mutex::new(None),
        }
    }

    /// While `true`, every operation fails with a connection error.
    pub fn set_failing(&self, failing: bool) {
        self.failing.store(failing, Ordering::SeqCst);
    }

    /// Number of `append` calls so far, failed ones included.
    pub fn append_attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    /// Sizes of the successful batches, in order.
    pub fn batch_sizes(&self) -> Vec<usize> {
        self.lock().batches.clone()
    }

    /// Everything stored, in order.
    pub fn entries(&self) -> Vec<StoredEntry> {
        self.lock().entries.iter().cloned().collect()
    }

    /// What `CONFIG GET` returns; `None` (default) makes it fail like a server that forbids `CONFIG`.
    pub fn set_settings(&self, settings: Option<Vec<(String, String)>>) {
        *self.settings.lock().unwrap_or_else(|e| e.into_inner()) = settings;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn check(&self) -> Result<(), StoreError> {
        if self.failing.load(Ordering::SeqCst) {
            Err(StoreError::Connection("injected failure".into()))
        } else {
            Ok(())
        }
    }
}

fn id_ms(id: &str) -> i64 {
    id.split('-')
        .next()
        .and_then(|p| p.parse().ok())
        .unwrap_or(0)
}

fn id_key(id: &str) -> (i64, u64) {
    let mut it = id.splitn(2, '-');
    let ms = it.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    let seq = it.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    (ms, seq)
}

impl RedisStore for MemoryStore {
    fn append<'a>(
        &'a self,
        entries: &'a [EntryFields],
        trim: Trim,
    ) -> BoxFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            self.check()?;
            let mut inner = self.lock();
            let mut ids = Vec::with_capacity(entries.len());
            for e in entries {
                let now = self.clock.now_ms();
                if now > inner.last_ms {
                    inner.last_ms = now;
                    inner.last_seq = 0;
                } else {
                    inner.last_seq += 1;
                }
                let id = format!("{}-{}", inner.last_ms, inner.last_seq);
                ids.push(id.clone());
                inner.entries.push_back(StoredEntry {
                    id,
                    kind: e.kind.to_string(),
                    exchange: e.exchange.clone(),
                    symbol: e.symbol.clone(),
                    data: e.data.clone(),
                });
            }
            inner.batches.push(entries.len());
            while inner
                .entries
                .front()
                .is_some_and(|e| id_ms(&e.id) < trim.min_id_ms)
            {
                inner.entries.pop_front();
            }
            while inner.entries.len() as u64 > trim.maxlen {
                inner.entries.pop_front();
            }
            Ok(ids)
        })
    }

    fn read_range<'a>(
        &'a self,
        start: &'a str,
        end: &'a str,
        count: Option<usize>,
    ) -> BoxFuture<'a, Result<Vec<StoredEntry>, StoreError>> {
        Box::pin(async move {
            self.check()?;
            let lo = if start == "-" {
                (i64::MIN, 0)
            } else {
                id_key(start)
            };
            let hi = if end == "+" {
                (i64::MAX, u64::MAX)
            } else {
                id_key(end)
            };
            let inner = self.lock();
            Ok(inner
                .entries
                .iter()
                .filter(|e| {
                    let k = id_key(&e.id);
                    k >= lo && k <= hi
                })
                .take(count.unwrap_or(usize::MAX))
                .cloned()
                .collect())
        })
    }

    fn stream_len(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            self.check()?;
            Ok(self.lock().entries.len() as u64)
        })
    }

    fn health(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move { self.check() })
    }

    fn settings<'a>(
        &'a self,
        names: &'a [&'a str],
    ) -> BoxFuture<'a, Result<Vec<(String, String)>, StoreError>> {
        Box::pin(async move {
            self.check()?;
            match self
                .settings
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
            {
                Some(all) => Ok(all
                    .into_iter()
                    .filter(|(k, _)| names.contains(&k.as_str()))
                    .collect()),
                None => Err(StoreError::Command("CONFIG is not allowed".into())),
            }
        })
    }
}
