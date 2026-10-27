//! The `History` service (V3b): `Query` and `Aggregate` over ClickHouse and the Redis hot window.
//!
//! - [`bins`]: UTC-aligned steps (1m to 1M) and the bin arithmetic, in Rust and as SQL.
//! - [`agg`]: cells, their merge and the roll-up into answer rows (the metric definitions live there).
//! - [`cold`]: the trait of the long-term store; [`ch`] implements it on ClickHouse with the SQL of [`sql`],
//!   [`memory`] on a vector of events (reference semantics and test double).
//! - [`event`], [`tail`]: event time, order and filters; reading the Redis stream.
//! - `events`, `aggregate`, [`movers`], `seasonality`: the operations; `service`: limits, the gRPC adapter; [`stats`]:
//!   `vulture_history_*` metrics.

pub mod agg;
mod aggregate;
pub mod bins;
pub mod ch;
pub mod cold;
pub mod digest_data;
pub mod event;
mod events;
pub mod memory;
pub mod movers;
mod seasonality;
mod service;
pub mod sql;
pub mod stats;
pub mod tail;

pub use ch::ChCold;
pub use memory::MemoryCold;
pub use service::{HistoryHandle, HistoryService, SeriesParams};
pub use stats::HistoryStats;
