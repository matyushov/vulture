//! Application context, signal handling and the top-level run loop.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::exchange::{EVENT_CHANNEL_CAPACITY, Exchange, build_connectors};
use crate::normalize::{AliasError, BaseAliases};
use crate::store::{RedisClientStore, RedisStore, RedisWriter, WriterConfig};
use crate::ws::SystemClock;

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

/// Why the server could not start.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// Neither `redis.url` nor `redis.url_file` is set.
    #[error("redis.url or redis.url_file is required")]
    RedisUrlMissing,
    /// The Redis URL could not be read or is malformed. The message never contains the URL.
    #[error("redis: {0}")]
    Redis(String),
}

/// Runs the server with the Redis store described by the configuration until `ctx.shutdown` is cancelled.
pub async fn run(ctx: AppContext) -> Result<(), AppError> {
    let url = ctx
        .config
        .redis
        .resolve_url()
        .map_err(|e| AppError::Redis(e.to_string()))?
        .ok_or(AppError::RedisUrlMissing)?;
    let store = RedisClientStore::new(&url, &ctx.config.redis.stream_key)
        .map_err(|e| AppError::Redis(e.to_string()))?;
    run_with_store(ctx, Arc::new(store)).await;
    Ok(())
}

/// Runs the server on the given store until `ctx.shutdown` is cancelled, then stops the connectors and flushes
/// the writer within `server.shutdown_timeout_secs`.
pub async fn run_with_store(ctx: AppContext, store: Arc<dyn RedisStore>) {
    let connectors = build_connectors(&ctx);
    run_with_connectors(ctx, store, connectors).await;
}

/// Like [`run_with_store`] with an explicit list of connectors (tests inject fakes).
pub async fn run_with_connectors(
    ctx: AppContext,
    store: Arc<dyn RedisStore>,
    connectors: Vec<Box<dyn Exchange>>,
) {
    let connectors: Vec<Arc<dyn Exchange>> = connectors.into_iter().map(Arc::from).collect();
    let (sink, events) = mpsc::channel::<vulture_proto::v1::Event>(EVENT_CHANNEL_CAPACITY);

    let deadline = Duration::from_secs(ctx.config.server.shutdown_timeout_secs);
    let writer = RedisWriter::new(
        store,
        WriterConfig::from_redis(&ctx.config.redis, deadline),
        Arc::new(SystemClock),
    );
    // Ends when every sender is dropped (connectors stopped) and the buffer is flushed, or at the deadline.
    let writer_task = tokio::spawn(writer.run(events, ctx.shutdown.clone()));

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

    let stop = async {
        for task in tasks {
            let _ = task.await;
        }
        let _ = writer_task.await;
    };
    if tokio::time::timeout(deadline * 2, stop).await.is_err() {
        tracing::warn!(?deadline, "graceful shutdown timed out; exiting anyway");
    }
    tracing::info!("vulture-server stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    #[tokio::test]
    async fn run_without_exchanges_stops_on_cancel() {
        let token = CancellationToken::new();
        let ctx = AppContext::new(Config::default(), token.clone());
        let handle = tokio::spawn(run_with_store(ctx, Arc::new(MemoryStore::new())));
        tokio::task::yield_now().await;
        token.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("shutdown must be prompt")
            .unwrap();
    }
}
