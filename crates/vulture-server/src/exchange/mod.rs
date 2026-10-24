//! The connector abstraction.
//!
//! Every exchange is one implementation of [`Exchange`]. A connector owns its loop (REST pollers, WebSocket
//! sessions through [`crate::ws`]) and pushes normalized [`Event`]s into a bounded channel until the
//! shutdown token of the [`AppContext`] is cancelled. Event types come straight from `vulture-proto`.

pub mod binance;
pub mod bybit;

use std::sync::Arc;

use futures_util::future::BoxFuture;
use tokio::sync::mpsc;
use vulture_proto::v1::Event;

use crate::app::AppContext;
use crate::symbols::SymbolChannel;

/// Capacity of the channel from connectors to the writer.
pub const EVENT_CHANNEL_CAPACITY: usize = 10_000;

/// Sender side given to connectors.
pub type EventSink = mpsc::Sender<Event>;

/// Errors a connector may return from [`Exchange::run`]. Transient network problems are handled inside the
/// connector (reconnect, backoff) and never surface here.
#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    /// The connector cannot run with this configuration.
    #[error("{exchange}: configuration problem: {message}")]
    Config {
        /// Connector name.
        exchange: String,
        /// What is wrong.
        message: String,
    },
    /// The consumer of the sink is gone.
    #[error("{0}: event sink closed")]
    SinkClosed(String),
    /// Unrecoverable failure.
    #[error("{exchange}: {message}")]
    Fatal {
        /// Connector name.
        exchange: String,
        /// What happened.
        message: String,
    },
}

/// A source of liquidation, open interest and gap events.
pub trait Exchange: Send + Sync {
    /// Exchange name as in the configuration and in the `exchange` field of events.
    fn name(&self) -> &str;

    /// Runs the connector until `ctx.shutdown` is cancelled (then returns `Ok`) or an unrecoverable error
    /// occurs. Events are sent with backpressure; a closed sink ends the run with
    /// [`ExchangeError::SinkClosed`].
    fn run<'a>(
        &'a self,
        ctx: AppContext,
        sink: EventSink,
    ) -> BoxFuture<'a, Result<(), ExchangeError>>;

    /// Adds the connector's counters and state to a metrics scrape. Labels must stay low-cardinality
    /// (`exchange`, `type`, `state`; never symbols). The default reports nothing.
    fn collect_metrics(&self, _out: &mut crate::metrics::Families) {}

    /// The link through which the owner's symbol policy reaches the connector and the connector publishes its
    /// universe (see [`crate::symbols`]). A connector that returns it must apply policy changes while running:
    /// stop producing events of untracked symbols, start for added ones, never publish a `Gap` for the
    /// difference. The default (`None`) means the connector tracks whatever its configuration says and cannot
    /// be controlled at runtime; such a connector keeps compiling and running unchanged.
    fn symbols(&self) -> Option<Arc<SymbolChannel>> {
        None
    }
}

/// Reports a running connector to the metrics hub.
pub struct ExchangeMetrics(pub Arc<dyn Exchange>);

impl crate::metrics::MetricSource for ExchangeMetrics {
    fn collect(&self, out: &mut crate::metrics::Families) {
        self.0.collect_metrics(out);
    }
}

/// Builds the connectors for the enabled exchanges of the configuration.
///
/// Binance (V1-4) and Bybit (V2a) exist so far; other configured names are reported and skipped so the server
/// still starts.
pub fn build_connectors(ctx: &AppContext) -> Vec<Box<dyn Exchange>> {
    let mut connectors: Vec<Box<dyn Exchange>> = Vec::new();
    for ex in ctx.config.enabled_exchanges() {
        match ex.name.as_str() {
            binance::NAME => {
                match binance::BinanceConnector::new(binance::BinanceConfig::from_exchange_config(
                    ex,
                )) {
                    Ok(c) => connectors.push(Box::new(c)),
                    Err(e) => {
                        tracing::error!(exchange = %ex.name, error = %e, "cannot create the connector; skipped");
                    }
                }
            }
            bybit::NAME => {
                match bybit::BybitConnector::new(bybit::BybitConfig::from_exchange_config(ex)) {
                    Ok(c) => connectors.push(Box::new(c)),
                    Err(e) => {
                        tracing::error!(exchange = %ex.name, error = %e, "cannot create the connector; skipped");
                    }
                }
            }
            _ => {
                tracing::warn!(exchange = %ex.name, "no connector implementation for this exchange yet; skipped");
            }
        }
    }
    connectors
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tokio_util::sync::CancellationToken;
    use vulture_proto::v1::{Gap, event::Payload};

    /// Minimal connector: emits one gap and waits for shutdown.
    struct Dummy;

    impl Exchange for Dummy {
        fn name(&self) -> &str {
            "dummy"
        }

        fn run<'a>(
            &'a self,
            ctx: AppContext,
            sink: EventSink,
        ) -> BoxFuture<'a, Result<(), ExchangeError>> {
            Box::pin(async move {
                let gap = Gap {
                    exchange: "dummy".into(),
                    stream: "liquidations".into(),
                    from_ts: 1,
                    to_ts: 2,
                };
                sink.send(Event {
                    id: String::new(),
                    payload: Some(Payload::Gap(gap)),
                })
                .await
                .map_err(|_| ExchangeError::SinkClosed("dummy".into()))?;
                ctx.shutdown.cancelled().await;
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn connector_runs_until_cancelled() {
        let ctx = AppContext::new(Config::default(), CancellationToken::new());
        let (tx, mut rx) = mpsc::channel(4);
        let connector: Box<dyn Exchange> = Box::new(Dummy);
        let shutdown = ctx.shutdown.clone();
        let handle = tokio::spawn(async move { connector.run(ctx, tx).await });
        let ev = rx.recv().await.unwrap();
        assert!(matches!(ev.payload, Some(Payload::Gap(_))));
        shutdown.cancel();
        handle.await.unwrap().unwrap();
    }

    fn entry(name: &str, enabled: bool) -> crate::config::ExchangeConfig {
        crate::config::ExchangeConfig {
            name: name.into(),
            enabled,
            symbols: vec![],
            oi_poll_interval_secs: 30,
            ws_url: None,
            rest_url: None,
        }
    }

    #[tokio::test]
    async fn only_enabled_known_exchanges_are_built() {
        let mut cfg = Config::default();
        cfg.exchanges.push(entry("binance", true));
        cfg.exchanges.push(entry("okx", true));
        cfg.exchanges.push(entry("bybit", false));
        let ctx = AppContext::new(cfg, CancellationToken::new());
        let connectors = build_connectors(&ctx);
        assert_eq!(connectors.len(), 1);
        assert_eq!(connectors[0].name(), "binance");
    }

    #[tokio::test]
    async fn enabled_bybit_is_built() {
        let mut cfg = Config::default();
        cfg.exchanges.push(entry("bybit", true));
        let ctx = AppContext::new(cfg, CancellationToken::new());
        let connectors = build_connectors(&ctx);
        assert_eq!(connectors.len(), 1);
        assert_eq!(connectors[0].name(), "bybit");
        assert!(connectors[0].symbols().is_some());
    }

    #[test]
    fn disabled_binance_is_not_built() {
        let mut cfg = Config::default();
        cfg.exchanges.push(entry("binance", false));
        let ctx = AppContext::new(cfg, CancellationToken::new());
        assert!(build_connectors(&ctx).is_empty());
    }
}
