//! In-memory [`RedisStore`] for unit tests: same entry semantics, exact trimming, failure injection.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::Notify;

use super::{EntryFields, RedisStore, StoreError, StoredEntry, StreamBounds, StreamReader, Trim};
use crate::ws::{Clock, SystemClock};

#[derive(Default)]
struct Inner {
    entries: VecDeque<StoredEntry>,
    last_ms: i64,
    last_seq: u64,
    /// Sizes of the successful `append` calls, in order.
    batches: Vec<usize>,
    /// Service state values (`state_get` / `state_apply`).
    state: std::collections::HashMap<String, String>,
}

/// A stream kept in memory. IDs are `<ms>-<seq>` from the injected clock, strictly increasing.
pub struct MemoryStore {
    core: Arc<Core>,
}

struct Core {
    inner: Mutex<Inner>,
    clock: Box<dyn Clock>,
    failing: AtomicBool,
    /// While set, `state_apply` fails (simulates a crash before the watermark update).
    failing_state_apply: AtomicBool,
    attempts: AtomicUsize,
    settings: Mutex<Option<Vec<(String, String)>>>,
    /// Wakes blocked readers when entries are appended or connections are killed.
    notify: Notify,
    /// Bumped by [`MemoryStore::kill_readers`]; readers opened earlier fail afterwards.
    reader_epoch: AtomicU64,
    open_readers: AtomicUsize,
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
            core: Arc::new(Core {
                inner: Mutex::new(Inner::default()),
                clock,
                failing: AtomicBool::new(false),
                failing_state_apply: AtomicBool::new(false),
                attempts: AtomicUsize::new(0),
                settings: Mutex::new(None),
                notify: Notify::new(),
                reader_epoch: AtomicU64::new(0),
                open_readers: AtomicUsize::new(0),
            }),
        }
    }

    /// Number of [`StreamReader`]s that are open (not yet dropped).
    pub fn open_readers(&self) -> usize {
        self.core.open_readers.load(Ordering::SeqCst)
    }

    /// Simulates `CLIENT KILL` of every reader connection: readers opened so far fail from now on (also while
    /// blocked); new ones can be opened.
    pub fn kill_readers(&self) {
        self.core.reader_epoch.fetch_add(1, Ordering::SeqCst);
        self.core.notify.notify_waiters();
    }

    /// While `true`, every operation fails with a connection error.
    pub fn set_failing(&self, failing: bool) {
        self.core.failing.store(failing, Ordering::SeqCst);
        self.core.notify.notify_waiters();
    }

    /// While `true`, `state_apply` fails with a connection error (other operations are unaffected).
    pub fn set_failing_state_apply(&self, failing: bool) {
        self.core
            .failing_state_apply
            .store(failing, Ordering::SeqCst);
    }

    /// A state value, for assertions.
    pub fn state_value(&self, name: &str) -> Option<String> {
        self.core.lock().state.get(name).cloned()
    }

    /// Number of `append` calls so far, failed ones included.
    pub fn append_attempts(&self) -> usize {
        self.core.attempts.load(Ordering::SeqCst)
    }

    /// Sizes of the successful batches, in order.
    pub fn batch_sizes(&self) -> Vec<usize> {
        self.core.lock().batches.clone()
    }

    /// Removes every entry with an ID smaller than `first_kept` (exact trimming, like `XTRIM MINID` without `~`).
    pub fn trim_to(&self, first_kept: &str) {
        let Some(limit) = super::parse_id(first_kept) else {
            return;
        };
        self.core
            .lock()
            .entries
            .retain(|e| super::parse_id(&e.id).is_none_or(|k| k >= limit));
    }

    /// Everything stored, in order.
    pub fn entries(&self) -> Vec<StoredEntry> {
        self.core.lock().entries.iter().cloned().collect()
    }

    /// What `CONFIG GET` returns; `None` (default) makes it fail like a server that forbids `CONFIG`.
    pub fn set_settings(&self, settings: Option<Vec<(String, String)>>) {
        *self.core.settings.lock().unwrap_or_else(|e| e.into_inner()) = settings;
    }
}

impl Core {
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
            self.core.attempts.fetch_add(1, Ordering::SeqCst);
            self.core.check()?;
            let mut inner = self.core.lock();
            let mut ids = Vec::with_capacity(entries.len());
            for e in entries {
                let now = self.core.clock.now_ms();
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
            drop(inner);
            self.core.notify.notify_waiters();
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
            self.core.check()?;
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
            let inner = self.core.lock();
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

    fn bounds(&self) -> BoxFuture<'_, Result<StreamBounds, StoreError>> {
        Box::pin(async move {
            self.core.check()?;
            let inner = self.core.lock();
            Ok(StreamBounds {
                first: inner.entries.front().map(|e| e.id.clone()),
                last: inner.entries.back().map(|e| e.id.clone()),
            })
        })
    }

    fn open_reader(&self) -> BoxFuture<'_, Result<Box<dyn StreamReader>, StoreError>> {
        Box::pin(async move {
            self.core.check()?;
            self.core.open_readers.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(MemoryReader {
                core: self.core.clone(),
                epoch: self.core.reader_epoch.load(Ordering::SeqCst),
            }) as Box<dyn StreamReader>)
        })
    }

    fn stream_len(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            self.core.check()?;
            Ok(self.core.lock().entries.len() as u64)
        })
    }

    fn health(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move { self.core.check() })
    }

    fn state_get<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Option<String>, StoreError>> {
        Box::pin(async move {
            self.core.check()?;
            Ok(self.core.lock().state.get(name).cloned())
        })
    }

    fn state_apply<'a>(
        &'a self,
        ops: &'a [(&'a str, Option<&'a str>)],
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.core.check()?;
            if self.core.failing_state_apply.load(Ordering::SeqCst) {
                return Err(StoreError::Connection("injected failure".into()));
            }
            let mut inner = self.core.lock();
            for (name, value) in ops {
                match value {
                    Some(v) => inner.state.insert((*name).to_string(), (*v).to_string()),
                    None => inner.state.remove(*name),
                };
            }
            Ok(())
        })
    }

    fn settings<'a>(
        &'a self,
        names: &'a [&'a str],
    ) -> BoxFuture<'a, Result<Vec<(String, String)>, StoreError>> {
        Box::pin(async move {
            self.core.check()?;
            match self
                .core
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

/// Reader on a [`MemoryStore`]; mimics a dedicated Redis connection.
struct MemoryReader {
    core: Arc<Core>,
    epoch: u64,
}

impl Drop for MemoryReader {
    fn drop(&mut self) {
        self.core.open_readers.fetch_sub(1, Ordering::SeqCst);
    }
}

impl MemoryReader {
    fn collect(&self, after: (u64, u64), count: usize) -> Result<Vec<StoredEntry>, StoreError> {
        self.core.check()?;
        if self.core.reader_epoch.load(Ordering::SeqCst) != self.epoch {
            return Err(StoreError::Connection("connection killed".into()));
        }
        let inner = self.core.lock();
        Ok(inner
            .entries
            .iter()
            .filter(|e| super::parse_id(&e.id).is_some_and(|k| k > after))
            .take(count)
            .cloned()
            .collect())
    }
}

impl StreamReader for MemoryReader {
    fn read_after<'a>(
        &'a mut self,
        after: &'a str,
        count: usize,
        block: Option<Duration>,
    ) -> BoxFuture<'a, Result<Vec<StoredEntry>, StoreError>> {
        Box::pin(async move {
            let after = super::parse_id(after)
                .ok_or_else(|| StoreError::Command("invalid stream ID".into()))?;
            let deadline = block.map(|d| tokio::time::Instant::now() + d);
            loop {
                // Register for wake-ups before looking, so an append in between is not missed.
                let notified = self.core.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let found = self.collect(after, count)?;
                if !found.is_empty() {
                    return Ok(found);
                }
                let Some(deadline) = deadline else {
                    return Ok(found);
                };
                if tokio::time::timeout_at(deadline, notified).await.is_err() {
                    return Ok(Vec::new());
                }
            }
        })
    }
}
