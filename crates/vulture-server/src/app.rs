//! Application context, signal handling and the top-level run loop.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::auth::TokenStore;
use crate::config::Config;
use crate::exchange::{EVENT_CHANNEL_CAPACITY, Exchange, ExchangeMetrics, build_connectors};
use crate::history::{
    ClickHouseMonitor, FlusherMetrics, FlusherStatus, HistorySetup, Watermark, build_client,
};
use crate::metrics::{
    AuthMetrics, GrpcMetrics, MetricsHub, Readiness, RedisSampler, TokenFileMetrics, WriterMetrics,
};
use crate::normalize::{AliasError, BaseAliases};
use crate::store::{RedisClientStore, RedisStore, RedisWriter, WriterConfig};
use crate::symbols::SymbolManager;
use crate::tls::TlsReloader;
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
    /// The token file could not be created or read.
    #[error(transparent)]
    Token(#[from] crate::auth::TokenError),
    /// The gRPC server could not start.
    #[error(transparent)]
    Grpc(#[from] crate::grpc::GrpcError),
    /// The TLS files are unusable.
    #[error(transparent)]
    Tls(#[from] crate::tls::TlsError),
    /// The metrics endpoint could not start.
    #[error(transparent)]
    Metrics(#[from] crate::metrics::MetricsError),
    /// The state file of the symbol policies is unusable; the server refuses to start rather than guess.
    #[error(transparent)]
    State(#[from] crate::symbols::StateError),
    /// The ClickHouse client could not be built from `[clickhouse]`.
    #[error("clickhouse: {0}")]
    ClickHouse(String),
    /// Signal handlers could not be installed.
    #[error("cannot install signal handlers: {0}")]
    Signals(std::io::Error),
}

/// How often the Redis stream length and age are sampled for the metrics.
const REDIS_SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

/// Without TLS there is nothing to reload, but the default action of SIGHUP is to kill the process, and the
/// deploy hooks send it unconditionally: swallow it and say so.
fn ignore_sighup(shutdown: CancellationToken) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut hup = signal(SignalKind::hangup())?;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = hup.recv() => tracing::info!("SIGHUP received; TLS is not configured, nothing to reload"),
                    _ = shutdown.cancelled() => return,
                }
            }
        });
    }
    #[cfg(not(unix))]
    let _ = shutdown;
    Ok(())
}

/// Runs the server with the Redis store described by the configuration until `ctx.shutdown` is cancelled.
pub async fn run(ctx: AppContext) -> Result<(), AppError> {
    let url = ctx
        .config
        .redis
        .resolve_url()
        .map_err(|e| AppError::Redis(e.to_string()))?
        .ok_or(AppError::RedisUrlMissing)?;
    let store: Arc<dyn RedisStore> = Arc::new(
        RedisClientStore::new(&url, &ctx.config.redis.stream_key)
            .map_err(|e| AppError::Redis(e.to_string()))?,
    );

    // Token first (a broken data_dir should stop the start), then the listener (a refused bind as well).
    let (authority, created) = crate::grpc::open_token(&ctx.config.server)?;
    if let Some(token) = created {
        let path = TokenStore::new(&ctx.config.server.data_dir);
        crate::auth::announce_created(
            &token,
            path.path(),
            std::io::IsTerminal::is_terminal(&std::io::stderr()),
            &mut std::io::stderr(),
        );
    }
    let tls = match &ctx.config.tls {
        Some(cfg) => Some(TlsReloader::load(cfg)?),
        None => None,
    };
    // The symbol policies: configuration defaults overridden by <data_dir>/state.json. A broken state file stops
    // the start here, before anything listens.
    let connectors = build_connectors(&ctx);
    let symbols = SymbolManager::new(
        &ctx.config,
        connectors
            .iter()
            .filter_map(|c| c.symbols().map(|ch| (c.name().to_string(), ch)))
            .collect(),
    )?;
    let hub = Arc::new(MetricsHub::new());
    hub.register(symbols.clone());
    let metrics_listener = crate::metrics::bind(&ctx.config.metrics.listen_addr).await?;
    let grpc = crate::grpc::start(
        &ctx.config.server,
        tls.clone(),
        store.clone(),
        authority,
        Some(symbols),
        ctx.shutdown.clone(),
    )
    .await?;
    hub.register(Arc::new(GrpcMetrics(grpc.stats.clone())));
    hub.register(Arc::new(AuthMetrics(grpc.auth_stats.clone())));
    hub.register(Arc::new(TokenFileMetrics(grpc.authority.clone())));
    match &tls {
        Some(tls) => {
            hub.register(tls.clone());
            let interval = Duration::from_secs(
                ctx.config
                    .tls
                    .as_ref()
                    .map_or(0, |t| t.reload_interval_secs),
            );
            crate::tls::spawn_watchers(tls.clone(), interval, ctx.shutdown.clone())
                .map_err(AppError::Signals)?;
        }
        None => ignore_sighup(ctx.shutdown.clone()).map_err(AppError::Signals)?,
    }
    let sampler = RedisSampler::new();
    hub.register(sampler.clone());
    tokio::spawn(sampler.run(store.clone(), REDIS_SAMPLE_INTERVAL, ctx.shutdown.clone()));
    tracing::info!(addr = %metrics_listener.local_addr().map(|a| a.to_string()).unwrap_or_default(), "metrics endpoint listening");
    let metrics_task = tokio::spawn(crate::metrics::serve(
        metrics_listener,
        hub.clone(),
        Arc::new(Readiness::new(store.clone(), grpc.running.clone())),
        ctx.shutdown.clone(),
    ));

    let deadline = Duration::from_secs(ctx.config.server.shutdown_timeout_secs);
    let history = match ctx.config.clickhouse_enabled() {
        Some(ch) => {
            let client =
                Arc::new(build_client(ch).map_err(|e| AppError::ClickHouse(e.to_string()))?);
            let watermark = Arc::new(Watermark::new());
            let status = Arc::new(FlusherStatus::new(
                watermark.clone(),
                ctx.config.redis.maxlen_safeguard,
                crate::metrics::unix_now_ms(),
            ));
            Some(HistorySetup {
                sink: client.clone(),
                flusher: crate::history::FlusherConfig::from_config(ch, deadline),
                watermark,
                status,
                monitor: Some((client, Duration::from_secs(ch.monitor_interval_secs))),
            })
        }
        None => {
            tracing::info!(
                "[clickhouse] is not configured: no history is kept, the stream is trimmed by time only"
            );
            None
        }
    };
    run_with_history(ctx.clone(), store, connectors, Some(hub), history).await;
    grpc.wait(deadline * 2).await;
    let _ = metrics_task.await;
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
    run_with_hub(ctx, store, connectors, None).await;
}

/// Like [`run_with_connectors`]; the writer and the connectors register their metrics with `hub`.
pub async fn run_with_hub(
    ctx: AppContext,
    store: Arc<dyn RedisStore>,
    connectors: Vec<Box<dyn Exchange>>,
    hub: Option<Arc<MetricsHub>>,
) {
    run_with_history(ctx, store, connectors, hub, None).await;
}

/// Like [`run_with_hub`] with the ClickHouse flusher: it shares its watermark with the writer (so nothing
/// unflushed is trimmed by time) and gets a final flush after the writer has written its last events.
pub async fn run_with_history(
    ctx: AppContext,
    store: Arc<dyn RedisStore>,
    connectors: Vec<Box<dyn Exchange>>,
    hub: Option<Arc<MetricsHub>>,
    history: Option<HistorySetup>,
) {
    let connectors: Vec<Arc<dyn Exchange>> = connectors.into_iter().map(Arc::from).collect();
    let (sink, events) = mpsc::channel::<vulture_proto::v1::Event>(EVENT_CHANNEL_CAPACITY);

    let deadline = Duration::from_secs(ctx.config.server.shutdown_timeout_secs);
    let mut writer_cfg = WriterConfig::from_redis(&ctx.config.redis, deadline);
    writer_cfg.watermark = history.as_ref().map(|h| h.watermark.clone());
    let writer = RedisWriter::new(store.clone(), writer_cfg, Arc::new(SystemClock));
    if let Some(hub) = &hub {
        hub.register(Arc::new(WriterMetrics(writer.status())));
        for connector in &connectors {
            hub.register(Arc::new(ExchangeMetrics(connector.clone())));
        }
    }
    // The flusher outlives the writer: it is stopped only after the writer wrote its last events.
    let flusher_stop = CancellationToken::new();
    let mut flusher_task = None;
    if let Some(h) = history {
        if let Some(hub) = &hub {
            hub.register(Arc::new(FlusherMetrics(h.status.clone())));
        }
        if let Some((client, interval)) = h.monitor {
            let monitor = ClickHouseMonitor::new();
            if let Some(hub) = &hub {
                hub.register(monitor.clone());
            }
            tokio::spawn(monitor.run(client, interval, ctx.shutdown.clone()));
        }
        let flusher = crate::history::Flusher::new(
            store.clone(),
            h.sink,
            h.flusher,
            h.watermark,
            h.status,
            Arc::new(SystemClock),
        );
        flusher_task = Some(tokio::spawn(flusher.run(flusher_stop.clone())));
    }
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
    if let Some(task) = flusher_task {
        // The writer is done: let the flusher move the last events to ClickHouse (it bounds itself by `deadline`).
        flusher_stop.cancel();
        if tokio::time::timeout(deadline * 2, task).await.is_err() {
            tracing::warn!(
                "the final ClickHouse flush did not finish in time; Redis keeps the rest"
            );
        }
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
