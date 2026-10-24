//! The gRPC server (V1-6, V1-7, V1-11): `Stream.Subscribe` with resume, the symbol RPCs of `Control`, a stub for
//! `History` and `UNIMPLEMENTED` for the rest of `Control`, all behind the bearer token interceptor.
//!
//! With `[tls]` the listener serves TLS ([`crate::tls`], certificate hot reload) on any address. Without it the
//! server accepts only a loopback bind unless `server.allow_plaintext` is set, because the token would travel
//! unencrypted. HTTP/2 keepalive drops clients that vanished without closing the connection.

mod control;
mod stats;
mod stream;
mod stub;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::service::InterceptorLayer;
use vulture_proto::v1::control_server::ControlServer;
use vulture_proto::v1::history_server::HistoryServer;
use vulture_proto::v1::stream_server::StreamServer;

pub use control::ControlService;
pub use stats::{GrpcSnapshot, GrpcStats};
pub use stream::{
    ClientStream, Filter, Start, StreamConfig, StreamService, ZERO_ID, resolve_start,
};
pub use stub::HistoryStub;

use crate::alerts::AlertsManager;
use crate::auth::{
    AuthInterceptor, AuthStats, FailureLimiter, LimiterConfig, TokenAuthority, TokenError,
    TokenStore,
};
use crate::config::ServerConfig;
use crate::store::RedisStore;
use crate::symbols::SymbolManager;
use crate::tls::TlsReloader;

/// Why the gRPC server could not start or stopped abnormally.
#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    /// `server.listen_addr` is not a socket address.
    #[error("server.listen_addr: {0:?} is not a socket address (host:port)")]
    BadAddress(String),
    /// A non-loopback bind without `allow_plaintext`.
    #[error("server.listen_addr: {0}")]
    Policy(String),
    /// The listener could not be created.
    #[error("cannot listen on {addr}: {source}")]
    Bind {
        /// Address.
        addr: SocketAddr,
        /// Underlying error.
        source: std::io::Error,
    },
    /// Token handling failed.
    #[error(transparent)]
    Token(#[from] TokenError),
    /// The transport failed.
    #[error("gRPC transport: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Refuses a non-loopback bind unless the connection is encrypted (`tls`) or plaintext was allowed explicitly:
/// otherwise the bearer token would cross the network in clear text.
pub fn check_bind_policy(addr: SocketAddr, allow_plaintext: bool, tls: bool) -> Result<(), String> {
    if addr.ip().is_loopback() || allow_plaintext || tls {
        Ok(())
    } else {
        Err(format!(
            "{addr} is not a loopback address and [tls] is not configured; bind to 127.0.0.1, \
             configure [tls], or set server.allow_plaintext = true to accept an unencrypted connection"
        ))
    }
}

/// Binds the plaintext gRPC listener after applying the bind policy; logs a warning for an allowed non-loopback
/// bind.
pub async fn bind(listen_addr: &str, allow_plaintext: bool) -> Result<TcpListener, GrpcError> {
    bind_checked(listen_addr, allow_plaintext, false).await
}

/// Binds the listener of a TLS server: any address is acceptable.
pub async fn bind_tls(listen_addr: &str) -> Result<TcpListener, GrpcError> {
    bind_checked(listen_addr, false, true).await
}

async fn bind_checked(
    listen_addr: &str,
    allow_plaintext: bool,
    tls: bool,
) -> Result<TcpListener, GrpcError> {
    let addr: SocketAddr = listen_addr
        .parse()
        .map_err(|_| GrpcError::BadAddress(listen_addr.to_string()))?;
    check_bind_policy(addr, allow_plaintext, tls).map_err(GrpcError::Policy)?;
    if !addr.ip().is_loopback() && !tls {
        tracing::warn!(
            %addr,
            "gRPC listens on a non-loopback address without TLS (server.allow_plaintext = true): \
             the token and the data are not encrypted"
        );
    }
    TcpListener::bind(addr)
        .await
        .map_err(|source| GrpcError::Bind { addr, source })
}

/// Everything `serve` needs besides the store, the authority and the shutdown token.
#[derive(Debug, Clone)]
pub struct ServeOptions {
    /// HTTP/2 keepalive ping interval.
    pub keepalive_interval: Duration,
    /// HTTP/2 keepalive ping timeout.
    pub keepalive_timeout: Duration,
    /// Failure limiter of the interceptor.
    pub limiter: LimiterConfig,
    /// Stream service settings.
    pub stream: StreamConfig,
    /// Backend of the symbol RPCs of `Control`; without it they answer `UNIMPLEMENTED` like the rest.
    pub symbols: Option<Arc<SymbolManager>>,
    /// Backend of the rule and channel RPCs of `Control`; without it they answer `UNIMPLEMENTED`.
    pub alerts: Option<Arc<AlertsManager>>,
}

impl ServeOptions {
    /// Options from `[server]`.
    pub fn from_server(server: &ServerConfig) -> Self {
        Self {
            keepalive_interval: Duration::from_secs(server.keepalive_interval_secs),
            keepalive_timeout: Duration::from_secs(server.keepalive_timeout_secs),
            limiter: LimiterConfig::default(),
            stream: StreamConfig::from_server(server),
            symbols: None,
            alerts: None,
        }
    }
}

/// Serves `Stream`, `History` and `Control` in plaintext on `listener` until `shutdown` fires; open streams end
/// with `UNAVAILABLE` so the graceful shutdown is quick.
pub async fn serve(
    listener: TcpListener,
    store: Arc<dyn RedisStore>,
    authority: Arc<TokenAuthority>,
    options: ServeOptions,
    stats: Arc<GrpcStats>,
    auth_stats: Arc<AuthStats>,
    shutdown: CancellationToken,
) -> Result<(), GrpcError> {
    serve_on(
        TcpListenerStream::new(listener),
        store,
        authority,
        options,
        stats,
        auth_stats,
        shutdown,
    )
    .await
}

/// Like [`serve`] over TLS: the handshakes use the certificate currently held by `tls`.
#[allow(clippy::too_many_arguments)]
pub async fn serve_tls(
    listener: TcpListener,
    tls: Arc<TlsReloader>,
    store: Arc<dyn RedisStore>,
    authority: Arc<TokenAuthority>,
    options: ServeOptions,
    stats: Arc<GrpcStats>,
    auth_stats: Arc<AuthStats>,
    shutdown: CancellationToken,
) -> Result<(), GrpcError> {
    serve_on(
        crate::tls::incoming(listener, tls, shutdown.clone()),
        store,
        authority,
        options,
        stats,
        auth_stats,
        shutdown,
    )
    .await
}

async fn serve_on<I, IO, IE>(
    incoming: I,
    store: Arc<dyn RedisStore>,
    authority: Arc<TokenAuthority>,
    options: ServeOptions,
    stats: Arc<GrpcStats>,
    auth_stats: Arc<AuthStats>,
    shutdown: CancellationToken,
) -> Result<(), GrpcError>
where
    I: futures_util::Stream<Item = Result<IO, IE>>,
    IO: tokio::io::AsyncRead
        + tokio::io::AsyncWrite
        + tonic::transport::server::Connected
        + Unpin
        + Send
        + 'static,
    IE: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let interceptor = AuthInterceptor::new(
        authority.clone(),
        Arc::new(FailureLimiter::new(options.limiter)),
        auth_stats,
    );
    let stream = StreamService::new(store, authority, options.stream, stats, shutdown.clone());
    tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(options.keepalive_interval))
        .http2_keepalive_timeout(Some(options.keepalive_timeout))
        // One layer in front of the router: every path is authenticated, including services that do not exist.
        .layer(InterceptorLayer::new(interceptor))
        .add_service(StreamServer::new(stream))
        .add_service(HistoryServer::new(HistoryStub))
        .add_service(ControlServer::new(
            ControlService::new(options.symbols.clone()).with_alerts(options.alerts.clone()),
        ))
        .serve_with_incoming_shutdown(incoming, shutdown.cancelled_owned())
        .await?;
    Ok(())
}

/// A running gRPC server.
pub struct GrpcHandle {
    /// The address actually bound.
    pub local_addr: SocketAddr,
    /// Counters of the stream service.
    pub stats: Arc<GrpcStats>,
    /// Counters of the interceptor.
    pub auth_stats: Arc<AuthStats>,
    /// The token authority (rotation).
    pub authority: Arc<TokenAuthority>,
    /// True while the server task is serving (for `/readyz`).
    pub running: Arc<AtomicBool>,
    server: JoinHandle<()>,
    watcher: JoinHandle<()>,
}

impl GrpcHandle {
    /// Waits for the server to finish after `shutdown` was cancelled, at most `timeout`.
    pub async fn wait(self, timeout: Duration) {
        if tokio::time::timeout(timeout, self.server).await.is_err() {
            tracing::warn!("gRPC server did not stop in time");
        }
        let _ = self.watcher.await;
    }
}

/// Binds the listener and starts serving in the background: TLS when `tls` is given, plaintext otherwise.
pub async fn start(
    server: &ServerConfig,
    tls: Option<Arc<TlsReloader>>,
    store: Arc<dyn RedisStore>,
    authority: Arc<TokenAuthority>,
    symbols: Option<Arc<SymbolManager>>,
    alerts: Option<Arc<AlertsManager>>,
    shutdown: CancellationToken,
) -> Result<GrpcHandle, GrpcError> {
    let tls_enabled = tls.is_some();
    let listener = match &tls {
        Some(_) => bind_tls(&server.listen_addr).await?,
        None => bind(&server.listen_addr, server.allow_plaintext).await?,
    };
    let local_addr = listener.local_addr().map_err(|source| GrpcError::Bind {
        addr: SocketAddr::from(([0, 0, 0, 0], 0)),
        source,
    })?;
    let stats = Arc::new(GrpcStats::default());
    let auth_stats = Arc::new(AuthStats::default());
    let watcher = tokio::spawn(authority.clone().watch_file(
        Duration::from_millis(server.token_check_interval_ms),
        shutdown.clone(),
    ));
    let running = Arc::new(AtomicBool::new(true));
    let task = tokio::spawn({
        let (stats, auth_stats, authority, shutdown, running) = (
            stats.clone(),
            auth_stats.clone(),
            authority.clone(),
            shutdown.clone(),
            running.clone(),
        );
        let mut options = ServeOptions::from_server(server);
        options.symbols = symbols;
        options.alerts = alerts;
        async move {
            let result = match tls {
                Some(tls) => {
                    serve_tls(
                        listener, tls, store, authority, options, stats, auth_stats, shutdown,
                    )
                    .await
                }
                None => {
                    serve(
                        listener, store, authority, options, stats, auth_stats, shutdown,
                    )
                    .await
                }
            };
            running.store(false, Ordering::Relaxed);
            if let Err(e) = result {
                tracing::error!(error = %e, "gRPC server failed");
            }
        }
    });
    tracing::info!(addr = %local_addr, tls = tls_enabled, "gRPC server listening");
    Ok(GrpcHandle {
        local_addr,
        stats,
        auth_stats,
        authority,
        running,
        server: task,
        watcher,
    })
}

/// Opens (creating on first start) the token store of `server.data_dir` and returns the authority. The
/// first-start banner is printed by the caller from the returned flag.
pub fn open_token(
    server: &ServerConfig,
) -> Result<(Arc<TokenAuthority>, Option<crate::config::Secret>), TokenError> {
    let store = TokenStore::new(&server.data_dir);
    let (token, created) = match store.create_if_missing()? {
        crate::auth::Created::New(t) => (t, true),
        crate::auth::Created::Existing(t) => (t, false),
    };
    if let Some(problem) = store.permission_problem() {
        tracing::warn!("{problem}");
    }
    let authority = TokenAuthority::open(store)?;
    Ok((authority, created.then_some(token)))
}
