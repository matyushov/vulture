//! vulture server library: configuration, exchange abstraction, normalization and the WebSocket manager.
//!
//! The binary (`vulture-server`) is a thin command-line wrapper; everything testable lives here.
//!
//! - [`config`]: TOML file plus environment overrides, secrets kept out of logs.
//! - [`app`]: shared application context, signal handling, the run loop.
//! - [`exchange`]: the [`exchange::Exchange`] trait implemented by every connector.
//! - [`normalize`]: symbols, base asset aliases, open interest units, market filter.
//! - [`store`]: the Redis Streams layout, the writer and its in-memory test double.
//! - [`ws`]: the reconnecting WebSocket session manager shared by all connectors.
//! - [`logging`]: `tracing` setup.

pub mod app;
pub mod config;
pub mod exchange;
pub mod logging;
pub mod normalize;
pub mod store;
pub mod ws;
