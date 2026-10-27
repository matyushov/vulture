//! The cold side of the `History` service: what it needs from the long-term store, as a trait.
//!
//! [`super::ch::ChCold`] answers from ClickHouse; [`super::memory::MemoryCold`] answers from a vector of events
//! with the same semantics (the reference for the tests and the fake for the service tests). The service combines a
//! cold answer with the unflushed tail it reads from Redis itself.

use std::time::Duration;

use futures_util::future::BoxFuture;
use vulture_proto::v1::Event;

use super::agg::{FineData, GapRec, Needs, SeriesKey, base_asset};
use super::bins::BinStep;
use crate::alerts::series::Sample;
use crate::history::ChError;
use crate::store::{TYPE_GAP, TYPE_LIQUIDATION, TYPE_OI, TYPE_SIGNAL};

/// A kind of stored event (one table each).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    /// `liquidations`.
    Liquidation,
    /// `open_interest`.
    OpenInterest,
    /// `gaps`.
    Gap,
    /// `signals`.
    Signal,
}

impl Kind {
    /// Every kind.
    pub const ALL: [Kind; 4] = [
        Kind::Liquidation,
        Kind::OpenInterest,
        Kind::Gap,
        Kind::Signal,
    ];

    /// The ClickHouse table.
    pub fn table(self) -> &'static str {
        match self {
            Kind::Liquidation => "liquidations",
            Kind::OpenInterest => "open_interest",
            Kind::Gap => "gaps",
            Kind::Signal => "signals",
        }
    }

    /// The `t` field of a Redis stream entry.
    pub fn entry_type(self) -> &'static str {
        match self {
            Kind::Liquidation => TYPE_LIQUIDATION,
            Kind::OpenInterest => TYPE_OI,
            Kind::Gap => TYPE_GAP,
            Kind::Signal => TYPE_SIGNAL,
        }
    }
}

/// Filters by exchange, symbol and base asset; an empty list means no filter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scope {
    /// Exchanges.
    pub exchanges: Vec<String>,
    /// Normalized symbols.
    pub symbols: Vec<String>,
    /// Base assets (the part of the symbol before the first `-`).
    pub base_assets: Vec<String>,
}

impl Scope {
    /// Whether a series passes the filters.
    pub fn matches(&self, exchange: &str, symbol: &str) -> bool {
        (self.exchanges.is_empty() || self.exchanges.iter().any(|e| e == exchange))
            && (self.symbols.is_empty() || self.symbols.iter().any(|s| s == symbol))
            && (self.base_assets.is_empty()
                || self.base_assets.iter().any(|b| b == base_asset(symbol)))
    }

    /// True without any filter.
    pub fn is_open(&self) -> bool {
        self.exchanges.is_empty() && self.symbols.is_empty() && self.base_assets.is_empty()
    }
}

/// Position of an event in the answer order of `Query`: event time, then the stream ID as numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventKey {
    /// Event time, ms (see [`super::event::event_ts`]).
    pub ts: i64,
    /// Millisecond part of the stream ID.
    pub id_ms: u64,
    /// Sequence part of the stream ID.
    pub id_seq: u64,
}

/// A `Query` the cold store should answer.
#[derive(Debug, Clone)]
pub struct EventQuery {
    /// Kinds to read.
    pub kinds: Vec<Kind>,
    /// Exchange and symbol filters (base assets are not used by `Query`).
    pub scope: Scope,
    /// Period start, ms, inclusive.
    pub from_ts: i64,
    /// Period end, ms, exclusive.
    pub to_ts: i64,
    /// Only events strictly after this position.
    pub after: Option<EventKey>,
    /// Most events to return.
    pub limit: usize,
    /// ClickHouse limits of the query.
    pub max_memory: u64,
    /// Deadline of the whole stream.
    pub timeout: Duration,
}

/// Events in answer order, pulled one at a time.
pub trait EventCursor: Send {
    /// The next event, `None` at the end.
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Event>, ChError>>;

    /// Rows read from the store so far.
    fn scanned(&self) -> u64;
}

/// Rows with an ID after this are ignored (they belong to the unflushed tail).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdBound {
    /// Millisecond part of the last flushed ID.
    pub ms: u64,
    /// Sequence part.
    pub seq: u64,
}

/// A request for cells.
#[derive(Debug, Clone)]
pub struct FineQuery {
    /// Step.
    pub step: BinStep,
    /// Start of the first bin, ms.
    pub from: i64,
    /// End of the last bin, ms, exclusive.
    pub to: i64,
    /// Filters.
    pub scope: Scope,
    /// Which cells to read.
    pub needs: Needs,
    /// Count only rows with an ID not after this.
    pub id_bound: Option<IdBound>,
    /// Most cells; more is [`ChError`]-free truncation reported by the caller as `RESOURCE_EXHAUSTED`.
    pub max_cells: usize,
    /// Query limits.
    pub max_memory: u64,
    /// Deadline.
    pub timeout: Duration,
}

/// Cells and the rows ClickHouse read to produce them.
#[derive(Debug, Default)]
pub struct FineAnswer {
    /// The cells.
    pub data: FineData,
    /// Rows read by the store.
    pub scanned: u64,
    /// The cell limit was hit: `data` is incomplete.
    pub truncated: bool,
}

/// A request for gap records.
#[derive(Debug, Clone)]
pub struct GapQuery {
    /// Period start, ms.
    pub from: i64,
    /// Period end, ms.
    pub to: i64,
    /// Exchanges (empty = all); the writer's `*` is always included.
    pub exchanges: Vec<String>,
    /// Deadline.
    pub timeout: Duration,
}

/// A request for the inputs of the OI movers ranking.
#[derive(Debug, Clone)]
pub struct MoverQuery {
    /// Filters.
    pub scope: Scope,
    /// End of the window, ms.
    pub end: i64,
    /// Start of the range to read (before the oldest reference candidate), ms.
    pub from: i64,
    /// Candidates for the reference sample lie in `[cand_from, cand_to]`.
    pub cand_from: i64,
    /// See `cand_from`.
    pub cand_to: i64,
    /// A series whose newest sample is older than this is not returned, ms.
    pub min_last: i64,
    /// A pause longer than this between samples is a hole, ms.
    pub hole_ms: i64,
    /// Query limits.
    pub max_memory: u64,
    /// Deadline.
    pub timeout: Duration,
}

/// What the cold store knows about one series for the movers ranking.
#[derive(Debug, Clone)]
pub struct MoverInput {
    /// The series.
    pub key: SeriesKey,
    /// The newest sample not after the end of the window.
    pub last: Sample,
    /// Samples near the reference point, oldest first.
    pub candidates: Vec<Sample>,
    /// The latest sample time that followed a pause longer than `hole_ms` (0 = none).
    pub hole_ts: i64,
}

/// Cold answer of the movers query.
#[derive(Debug, Default)]
pub struct MoverAnswer {
    /// One entry per active series.
    pub series: Vec<MoverInput>,
    /// Rows read by the store.
    pub scanned: u64,
}

/// A request for the largest liquidations of a period.
#[derive(Debug, Clone)]
pub struct TopLiqQuery {
    /// Period start, ms, inclusive.
    pub from: i64,
    /// Period end, ms, exclusive.
    pub to: i64,
    /// How many.
    pub n: usize,
    /// Query limits.
    pub max_memory: u64,
    /// Deadline.
    pub timeout: Duration,
}

/// The long-term store.
pub trait ColdStore: Send + Sync {
    /// The `n` liquidations of a period with the largest notional, largest first.
    fn top_liquidations(&self, q: TopLiqQuery) -> BoxFuture<'_, Result<Vec<Event>, ChError>>;

    /// Events of a period in answer order.
    fn events(&self, q: EventQuery) -> BoxFuture<'_, Result<Box<dyn EventCursor>, ChError>>;

    /// The position of the event with this ID inside the query's period and filters (the `after` and `limit` of the
    /// query are ignored), `None` when there is no such event.
    fn event_key(
        &self,
        id: String,
        q: EventQuery,
    ) -> BoxFuture<'_, Result<Option<EventKey>, ChError>>;

    /// Cells of a period.
    fn fine(&self, q: FineQuery) -> BoxFuture<'_, Result<FineAnswer, ChError>>;

    /// Gap records overlapping a period.
    fn gaps(&self, q: GapQuery) -> BoxFuture<'_, Result<Vec<GapRec>, ChError>>;

    /// Inputs of the OI movers ranking.
    fn movers(&self, q: MoverQuery) -> BoxFuture<'_, Result<MoverAnswer, ChError>>;
}
