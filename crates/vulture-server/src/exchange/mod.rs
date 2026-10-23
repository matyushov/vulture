//! The connector abstraction.
//!
//! Every exchange is one implementation of [`Exchange`]. A connector owns its loop (REST pollers, WebSocket
//! sessions through [`crate::ws`]) and pushes normalized [`Event`]s into a bounded channel until the
//! shutdown token of the [`AppContext`] is cancelled. Event types come straight from `vulture-proto`.

use futures_util::future::BoxFuture;
use tokio::sync::mpsc;
use vulture_proto::v1::Event;

use crate::app::AppContext;

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
}

/// Builds the connectors for the enabled exchanges of the configuration.
///
/// No exchange modules exist yet (Binance arrives in V1-4); configured names without an implementation are
/// reported and skipped so the server still starts.
pub fn build_connectors(ctx: &AppContext) -> Vec<Box<dyn Exchange>> {
    for ex in ctx.config.enabled_exchanges() {
        tracing::warn!(exchange = %ex.name, "no connector implementation for this exchange yet; skipped");
    }
    Vec::new()
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

    #[test]
    fn unknown_exchanges_are_skipped() {
        let mut cfg = Config::default();
        cfg.exchanges.push(crate::config::ExchangeConfig {
            name: "binance".into(),
            enabled: true,
            symbols: vec![],
            oi_poll_interval_secs: 30,
        });
        let ctx = AppContext::new(cfg, CancellationToken::new());
        assert!(build_connectors(&ctx).is_empty());
    }
}
