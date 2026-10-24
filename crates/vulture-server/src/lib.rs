//! vulture server library: configuration, exchange abstraction, normalization and the WebSocket manager.
//!
//! The binary (`vulture-server`) is a thin command-line wrapper; everything testable lives here.
//!
//! - [`config`]: TOML file plus environment overrides, secrets kept out of logs.
//! - [`alerts`]: the analytics engine, OI spike and liquidation cascade signals, rules, Telegram and Discord delivery.
//! - [`app`]: shared application context, signal handling, the run loop.
//! - [`auth`]: the owner token (file, rotation), the interceptor and the failure limiter.
//! - [`grpc`]: the tonic server: `Stream.Subscribe` with resume, stubs for `History` and `Control`.
//! - [`exchange`]: the [`exchange::Exchange`] trait implemented by every connector.
//! - [`normalize`]: symbols, base asset aliases, open interest units, market filter.
//! - [`store`]: the Redis Streams layout, the writer and its in-memory test double.
//! - [`history`]: ClickHouse schema, migrations, the Redis-to-ClickHouse flusher and storage monitoring.
//! - [`symbols`]: tracked symbols per exchange, changed at runtime through `Control` (policy, state file, audit).
//! - [`ws`]: the reconnecting WebSocket session manager shared by all connectors.
//! - [`tls`]: rustls for the gRPC listener with certificate hot reload.
//! - [`metrics`]: Prometheus exposition, `/healthz` and `/readyz`.
//! - [`logging`]: `tracing` setup.

pub mod alerts;
pub mod app;
pub mod auth;
pub mod config;
pub mod exchange;
pub mod grpc;
pub mod history;
pub mod logging;
pub mod metrics;
pub mod normalize;
pub mod store;
pub mod symbols;
pub mod tls;
pub mod ws;
