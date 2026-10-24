//! The WebSocket connections of the Bybit connector.
//!
//! Bybit documents no limit on the number of topics of a linear connection (only the size of one request),
//! so one connection normally carries everything. When the topic count would exceed a configured cap, symbols
//! are spread over several connections ("shards"). A symbol keeps its shard as long as it is tracked; new
//! symbols go to the first shard with room, and a new shard is opened only when all are full. Shards are never
//! closed: an empty one costs one idle socket.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use super::stream::topics_of;
use crate::ws::{ConnectionState, TopicControl, WsCounters, WsHandle, WsStatus};

/// What the spawner returns for a new shard.
pub struct ShardParts {
    /// The manager handle.
    pub handle: WsHandle,
}

struct Shard {
    control: Arc<TopicControl>,
    status: watch::Receiver<WsStatus>,
    counters: Arc<WsCounters>,
    symbols: BTreeSet<String>,
    handle: Option<WsHandle>,
}

/// Read-only view of one shard, for status and metrics.
#[derive(Debug, Clone)]
pub struct ShardView {
    /// Connection state.
    pub status: WsStatus,
    /// Symbols assigned to the shard.
    pub symbols: usize,
    /// Counters of the manager (shared, live).
    pub counters: Arc<WsCounters>,
}

struct Inner {
    shards: Vec<Shard>,
    assigned: HashMap<String, usize>,
}

/// All shards and the symbol assignment.
pub struct Shards {
    inner: Mutex<Inner>,
    max_symbols: usize,
}

impl std::fmt::Debug for Shards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shards")
            .field("shards", &self.lock().shards.len())
            .finish_non_exhaustive()
    }
}

impl Shards {
    /// `max_topics` is the cap per connection (two topics per symbol).
    pub fn new(max_topics: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                shards: Vec::new(),
                assigned: HashMap::new(),
            }),
            max_symbols: (max_topics / 2).max(1),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Makes the shards track exactly `wanted` (raw symbols). `spawn(index, control)` opens a connection for a
    /// new shard. Returns `(index, symbols added, symbols removed)` for every shard that changed.
    pub fn reconcile(
        &self,
        wanted: &BTreeSet<String>,
        spawn: &mut dyn FnMut(usize, Arc<TopicControl>) -> Result<ShardParts, String>,
    ) -> Result<Vec<(usize, usize, usize)>, String> {
        let mut inner = self.lock();
        if inner.shards.is_empty() {
            // The first connection is opened right away, even with nothing to track.
            Self::open_shard(&mut inner, spawn)?;
        }
        let mut added = vec![0usize; inner.shards.len()];
        let mut removed = vec![0usize; inner.shards.len()];
        let gone: Vec<String> = inner
            .assigned
            .keys()
            .filter(|s| !wanted.contains(*s))
            .cloned()
            .collect();
        for s in gone {
            if let Some(i) = inner.assigned.remove(&s) {
                inner.shards[i].symbols.remove(&s);
                removed[i] += 1;
            }
        }
        for s in wanted {
            if inner.assigned.contains_key(s) {
                continue;
            }
            let idx = match inner
                .shards
                .iter()
                .position(|sh| sh.symbols.len() < self.max_symbols)
            {
                Some(i) => i,
                None => {
                    let i = Self::open_shard(&mut inner, spawn)?;
                    added.push(0);
                    removed.push(0);
                    i
                }
            };
            inner.shards[idx].symbols.insert(s.clone());
            inner.assigned.insert(s.clone(), idx);
            added[idx] += 1;
        }
        let mut changed = Vec::new();
        for (i, sh) in inner.shards.iter().enumerate() {
            let topics: BTreeSet<String> = sh.symbols.iter().flat_map(|s| topics_of(s)).collect();
            sh.control.set(topics);
            if added[i] + removed[i] > 0 {
                changed.push((i, added[i], removed[i]));
            }
        }
        Ok(changed)
    }

    fn open_shard(
        inner: &mut Inner,
        spawn: &mut dyn FnMut(usize, Arc<TopicControl>) -> Result<ShardParts, String>,
    ) -> Result<usize, String> {
        let index = inner.shards.len();
        let control = TopicControl::new([]);
        let parts = spawn(index, control.clone())?;
        inner.shards.push(Shard {
            control,
            status: parts.handle.status.clone(),
            counters: parts.handle.counters.clone(),
            symbols: BTreeSet::new(),
            handle: Some(parts.handle),
        });
        Ok(index)
    }

    /// Whether the connection carrying `raw` has a live session.
    pub fn is_healthy(&self, raw: &str) -> bool {
        let inner = self.lock();
        inner
            .assigned
            .get(raw)
            .is_some_and(|&i| inner.shards[i].status.borrow().state == ConnectionState::Healthy)
    }

    /// Symbols of a shard.
    pub fn symbols_of(&self, shard: usize) -> Vec<String> {
        self.lock()
            .shards
            .get(shard)
            .map(|s| s.symbols.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// The shard of a symbol.
    pub fn shard_of(&self, raw: &str) -> Option<usize> {
        self.lock().assigned.get(raw).copied()
    }

    /// Views for metrics and status.
    pub fn views(&self) -> Vec<ShardView> {
        self.lock()
            .shards
            .iter()
            .map(|s| ShardView {
                status: *s.status.borrow(),
                symbols: s.symbols.len(),
                counters: s.counters.clone(),
            })
            .collect()
    }

    /// Topics refused by the exchange, over all shards.
    pub fn rejected(&self) -> Vec<(String, String)> {
        self.lock()
            .shards
            .iter()
            .flat_map(|s| s.control.rejected())
            .collect()
    }

    /// Desired topics of all shards (tests).
    pub fn desired_topics(&self) -> BTreeSet<String> {
        self.lock()
            .shards
            .iter()
            .flat_map(|s| s.control.desired().iter().cloned().collect::<Vec<_>>())
            .collect()
    }

    /// Takes the manager handles for joining at shutdown.
    pub fn take_handles(&self) -> Vec<WsHandle> {
        self.lock()
            .shards
            .iter_mut()
            .filter_map(|s| s.handle.take())
            .collect()
    }
}
