//! Hot storage: Redis Streams (V1-5).
//!
//! # Layout
//!
//! Every event of every exchange goes into a **single stream** (`redis.stream_key`, default
//! `vulture:events`). One stream gives the gRPC reader (V1-6) a single resume cursor and the ClickHouse
//! flusher (V3) a single source. The volume is small (about 20 open interest events/s and about 1
//! liquidation/s), so filtering on read is cheap.
//!
//! Each entry has four fields (see [`entry`]):
//!
//! | field | value |
//! |---|---|
//! | `t` | event type: `liq`, `oi` or `gap` |
//! | `x` | exchange name (`binance`; `*` for gaps produced by the writer itself) |
//! | `s` | normalized symbol (`BTC-USDT-PERP`), empty for gaps |
//! | `d` | the protobuf-encoded `vulture.v1.Event` **without** `id` |
//!
//! The Redis entry ID (`<ms>-<seq>`) is the public `Event.id`; readers set it from the entry ID when they
//! decode `d`. The cheap fields `t`, `x`, `s` let a reader filter without decoding.
//!
//! # Trimming
//!
//! Every batch is followed by `XTRIM ... MINID ~ <now - window>` (time window, default 24 h) and
//! `XTRIM ... MAXLEN ~ <safeguard>` (so that a broken time trimming can never fill Redis). Approximate
//! trimming (`~`) removes whole internal nodes only, so a stream may hold slightly more than the window.
//!
//! # Components
//!
//! - [`RedisStore`]: the storage trait (append a batch, read a range, health, server settings), implemented
//!   by [`RedisClientStore`] on the `redis` crate and by [`MemoryStore`] for tests.
//! - [`RedisWriter`]: consumes the connector channel, batches, survives Redis outages with a bounded
//!   in-memory buffer and writes a `Gap` for events it had to drop. See [`writer`].

mod entry;
mod memory;
mod redis_store;
mod status;
mod writer;

use futures_util::future::BoxFuture;

pub use entry::{DecodeError, EntryFields, StoredEntry, TYPE_GAP, TYPE_LIQUIDATION, TYPE_OI};
pub use memory::MemoryStore;
pub use redis_store::{RedisClientStore, validate_url};
pub use status::{StoreState, WriterSnapshot, WriterStatus};
pub use writer::{
    GAP_EXCHANGE, GAP_STREAM, RECOMMENDED_SETTINGS, RedisWriter, SettingsReport, WriterConfig,
    check_settings,
};

/// Failure of a storage operation. Messages never contain the Redis URL.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// The server is unreachable, the connection dropped or the command timed out.
    #[error("connection: {0}")]
    Connection(String),
    /// The server answered with an error (out of memory, command not allowed, wrong type, ...).
    #[error("command: {0}")]
    Command(String),
}

/// Trimming applied after every appended batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trim {
    /// Entries with an ID older than `<min_id_ms>-0` are trimmed (approximately).
    pub min_id_ms: i64,
    /// Upper bound for the stream length (approximately).
    pub maxlen: u64,
}

/// The storage the writer and the future gRPC reader talk to.
pub trait RedisStore: Send + Sync {
    /// Appends `entries` in order as one pipeline (a single `MULTI`/`EXEC`) and applies `trim`. Returns the
    /// IDs assigned to the entries.
    fn append<'a>(
        &'a self,
        entries: &'a [EntryFields],
        trim: Trim,
    ) -> BoxFuture<'a, Result<Vec<String>, StoreError>>;

    /// `XRANGE`: entries with IDs in `[start, end]` (`-` and `+` are accepted), at most `count`.
    fn read_range<'a>(
        &'a self,
        start: &'a str,
        end: &'a str,
        count: Option<usize>,
    ) -> BoxFuture<'a, Result<Vec<StoredEntry>, StoreError>>;

    /// Number of entries in the stream (`XLEN`).
    fn stream_len(&self) -> BoxFuture<'_, Result<u64, StoreError>>;

    /// Cheap liveness check (`PING`).
    fn health(&self) -> BoxFuture<'_, Result<(), StoreError>>;

    /// `CONFIG GET` of the named settings, lowercase names and values. Fails with [`StoreError::Command`]
    /// when the server does not allow `CONFIG`.
    fn settings<'a>(
        &'a self,
        names: &'a [&'a str],
    ) -> BoxFuture<'a, Result<Vec<(String, String)>, StoreError>>;
}
