//! Reconnecting WebSocket manager shared by all connectors.
//!
//! A connector describes its exchange with a [`WsProfile`] (timeouts, keepalive, silence limit, planned
//! reconnect) and a [`WsHandler`] (subscription frames, frame classification, dedup key), then calls
//! [`spawn_ws`]. The manager keeps one logical connection alive forever:
//!
//! - connect with a 10 s timeout, subscribe, read in a separate task into a bounded channel (10 000) that
//!   never blocks the socket (overflow is counted and logged);
//! - client ping/pong per profile, plus detection of "connected but mute" through a liveness stream;
//! - exponential backoff x2 from 1 s to 60 s with equal jitter, reset after 60 s of health; handshake
//!   403/429/418 wait at least 60 s and honor `Retry-After`;
//! - [`WsOutput::Gap`] after a loss, published before the first frame of the recovered connection;
//! - make-before-break planned reconnect with deduplication, which creates no `Gap`;
//! - [`ConnectionState`] (`Connecting`, `Healthy`, `Degraded`, `Stopped`) and [`WsCounters`] for metrics.

mod backoff;
mod clock;
mod connector;
mod dedup;
mod handler;
mod manager;
pub mod metrics;
mod profile;
mod session;
mod status;

pub use backoff::Backoff;
pub use clock::{Clock, SystemClock};
pub use connector::{
    BoxedConnection, ConnectError, TungsteniteConnector, WsConnection, WsConnector,
};
pub use dedup::Dedup;
pub use handler::{Frame, FrameClass, WsHandler};
pub use manager::{WsHandle, spawn_ws};
pub use profile::{MaxAge, PingConfig, PingMessage, WsProfile};
pub use session::{ConnectFailure, WsMessage, WsOutput};
pub use status::{ConnectionState, WsCounters, WsStatus};
