//! Application context, signal handling and the top-level run loop.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::exchange::{EVENT_CHANNEL_CAPACITY, Exchange, build_connectors};
use crate::normalize::{AliasError, BaseAliases};

/// Context shared by every component. Cheap to clone.
#[derive(Clone)]
pub struct AppContext {
    /// Validated configuration.
    pub config: Arc<Config>,
    /// Cancelled on SIGINT/SIGTERM; components must stop promptly when it fires.
    pub shutdown: CancellationToken,
    /// Base asset alias table.
    pub aliases: Arc<BaseAliases>,
}

impl AppContext {
    /// Context with the built-in alias table.
    pub fn new(config: Config, shutdown: CancellationToken) -> Self {
        Self {
            config: Arc::new(config),
            shutdown,
            aliases: Arc::new(BaseAliases::builtin()),
        }
    }

    /// Context with the alias table named in the configuration (or the built-in one).
    pub fn from_config(config: Config, shutdown: CancellationToken) -> Result<Self, AliasError> {
        let aliases = match &config.normalize.base_aliases_file {
            Some(path) => BaseAliases::load(path)?,
            None => BaseAliases::builtin(),
        };
        Ok(Self {
            config: Arc::new(config),
            shutdown,
            aliases: Arc::new(aliases),
        })
    }
}

/// Cancels `token` on SIGINT or SIGTERM (Ctrl-C elsewhere).
pub fn cancel_on_signals(token: CancellationToken) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate())?;
        let mut int = signal(SignalKind::interrupt())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = term.recv() => tracing::info!("SIGTERM received, shutting down"),
                _ = int.recv() => tracing::info!("SIGINT received, shutting down"),
            }
            token.cancel();
        });
    }
    #[cfg(not(unix))]
    {
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::info!("Ctrl-C received, shutting down");
            }
            token.cancel();
        });
    }
    Ok(())
}

/// Runs the server until `ctx.shutdown` is cancelled, then stops the connectors within
/// `server.shutdown_timeout_secs`.
pub async fn run(ctx: AppContext) {
    let connectors: Vec<Arc<dyn Exchange>> =
        build_connectors(&ctx).into_iter().map(Arc::from).collect();
    let (sink, mut events) = mpsc::channel::<vulture_proto::v1::Event>(EVENT_CHANNEL_CAPACITY);

    // Placeholder consumer; the Redis writer (V1-5) replaces it. Ends when every sender is dropped.
    let consumer = tokio::spawn(async move {
        let mut count: u64 = 0;
        while let Some(event) = events.recv().await {
            count += 1;
            tracing::trace!(id = %event.id, "event");
        }
        tracing::debug!(count, "event consumer finished");
    });

    let mut tasks = Vec::new();
    for connector in connectors {
        let ctx = ctx.clone();
        let sink = sink.clone();
        tasks.push(tokio::spawn(async move {
            let name = connector.name().to_string();
            match connector.run(ctx, sink).await {
                Ok(()) => tracing::info!(exchange = %name, "connector stopped"),
                Err(e) => tracing::error!(exchange = %name, error = %e, "connector failed"),
            }
        }));
    }
    drop(sink);

    tracing::info!(connectors = tasks.len(), "vulture-server running");
    ctx.shutdown.cancelled().await;

    let deadline = Duration::from_secs(ctx.config.server.shutdown_timeout_secs);
    let stop = async {
        for task in tasks {
            let _ = task.await;
        }
        let _ = consumer.await;
    };
    if tokio::time::timeout(deadline, stop).await.is_err() {
        tracing::warn!(?deadline, "graceful shutdown timed out; exiting anyway");
    }
    tracing::info!("vulture-server stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_without_exchanges_stops_on_cancel() {
        let token = CancellationToken::new();
        let ctx = AppContext::new(Config::default(), token.clone());
        let handle = tokio::spawn(run(ctx));
        tokio::task::yield_now().await;
        token.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("shutdown must be prompt")
            .unwrap();
    }
}
