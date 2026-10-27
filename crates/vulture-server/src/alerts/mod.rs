//! The analytics engine, signals, rules and delivery (V4a).
//!
//! ```text
//! connectors ──► tap ──► Redis writer ──► Redis Stream ──► gRPC Stream, flusher
//!                 │                          ▲
//!                 └──► engine (bounded queue; drops its own input when full)
//!                        │  series (4 h of OI per symbol), OI spike detector, cascade detector, rules
//!                        ├──► Signal events ──► writer (the same pipeline as every event)
//!                        └──► Alert messages ──► per-channel queue ──► Telegram / Discord
//! ```
//!
//! - [`series`]: ring buffers and window arithmetic. [`oi`]: the spike detector. [`cascade`]: the liquidation
//!   cascade detector. Their formulas are documented in those modules and in the README.
//! - [`rules`]: compiled rules, matching, cooldowns and message text. [`core`]: the synchronous engine state.
//! - [`engine`]: the task, the tap and the warm-up from the Redis stream.
//! - [`model`], [`file`], [`manager`]: rules and channels, `alerts.json`, and the `Control` backend.
//! - [`delivery`], [`text`]: queues, batching, rate limits, retries and message rendering.

pub mod cascade;
pub mod core;
pub mod delivery;
pub mod digest;
pub mod engine;
pub mod file;
pub mod manager;
pub mod model;
pub mod oi;
pub mod rules;
pub mod series;
pub mod stats;
pub mod text;

use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::Event;

pub use delivery::{Delivery, PushResult, TestOutcome};
pub use digest::{DigestError, DigestService};
pub use engine::{EngineInput, EngineTask};
pub use file::{ALERTS_FILE, AlertsFile, AlertsFileError};
pub use manager::{AlertsError, AlertsManager, ChannelView};
pub use stats::EngineStats;

use crate::config::{AlertsConfig, Config};
use crate::metrics::MetricsHub;
use crate::store::RedisStore;
use crate::ws::Clock;

/// Why the alerts subsystem could not be set up.
#[derive(Debug, thiserror::Error)]
pub enum SetupError {
    /// `alerts.json` is unusable.
    #[error(transparent)]
    File(#[from] AlertsFileError),
    /// The HTTP client could not be built.
    #[error("alerts delivery: {0}")]
    Http(String),
}

/// The alerts subsystem: manager, delivery and the engine's shared state.
pub struct Alerts {
    /// Configuration.
    pub cfg: Arc<AlertsConfig>,
    /// Engine counters.
    pub stats: Arc<EngineStats>,
    /// Rules in force (swapped by the manager).
    pub rules: Arc<ArcSwap<rules::RuleSet>>,
    /// Channels and their queues.
    pub delivery: Arc<Delivery>,
    /// Rules and channels at runtime (`Control`).
    pub manager: Arc<AlertsManager>,
}

impl Alerts {
    /// Loads rules and channels (a broken `alerts.json` is an error) and starts the channel tasks. Must run in a
    /// Tokio runtime.
    pub fn open(config: &Config, shutdown: CancellationToken) -> Result<Arc<Self>, SetupError> {
        let delivery =
            Delivery::new(config.alerts.delivery.clone(), shutdown).map_err(SetupError::Http)?;
        let rules = Arc::new(ArcSwap::from_pointee(rules::RuleSet::default()));
        let manager = AlertsManager::open(config, rules.clone(), delivery.clone())?;
        Ok(Arc::new(Self {
            cfg: Arc::new(config.alerts.clone()),
            stats: Arc::new(EngineStats::default()),
            rules,
            delivery,
            manager,
        }))
    }

    /// Registers the metrics of the whole subsystem.
    pub fn register_metrics(&self, hub: &MetricsHub) {
        hub.register(self.stats.clone());
        hub.register(self.delivery.clone());
        hub.register(self.manager.clone());
    }

    /// Starts the engine. Returns the tap for the pipeline and the task handle. `writer` is the channel into
    /// which signals are injected.
    pub fn spawn_engine(
        &self,
        store: Arc<dyn RedisStore>,
        writer: mpsc::Sender<Event>,
        clock: Arc<dyn Clock>,
        shutdown: CancellationToken,
    ) -> (EngineInput, JoinHandle<()>) {
        let (input, rx) =
            EngineInput::channel(self.cfg.input_queue, self.stats.clone(), clock.clone());
        let core = core::Core::new(&self.cfg, self.rules.clone(), self.stats.clone());
        let task = EngineTask {
            core,
            rx,
            writer,
            delivery: self.delivery.clone(),
            store,
            clock,
            cfg: self.cfg.clone(),
            shutdown,
        };
        (input, tokio::spawn(task.run()))
    }
}
