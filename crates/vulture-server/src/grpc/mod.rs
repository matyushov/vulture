//! The gRPC server (V1-6, V1-7): `Stream.Subscribe` with resume, stubs for `History` and `Control`, all behind
//! the bearer token interceptor.
//!
//! No TLS yet (V1-8): the server accepts only a loopback bind unless `server.allow_plaintext` is set, because the
//! token would travel unencrypted. HTTP/2 keepalive drops clients that vanished without closing the connection.

mod stats;
mod stream;
mod stub;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::service::InterceptorLayer;
use vulture_proto::v1::control_server::ControlServer;
use vulture_proto::v1::history_server::HistoryServer;
use vulture_proto::v1::stream_server::StreamServer;

pub use stats::{GrpcSnapshot, GrpcStats};
pub use stream::{
    ClientStream, Filter, Start, StreamConfig, StreamService, ZERO_ID, resolve_start,
};
pub use stub::{ControlStub, HistoryStub};

use crate::auth::{
    AuthInterceptor, AuthStats, FailureLimiter, LimiterConfig, TokenAuthority, TokenError,
    TokenStore,
};
use crate::config::ServerConfig;
use crate::store::RedisStore;

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

/// Refuses a non-loopback bind unless plaintext was allowed explicitly. Until TLS exists (V1-8) the bearer token
/// would cross the network in clear text.
pub fn check_bind_policy(addr: SocketAddr, allow_plaintext: bool) -> Result<(), String> {
    if addr.ip().is_loopback() || allow_plaintext {
        Ok(())
    } else {
        Err(format!(
            "{addr} is not a loopback address and TLS is not available yet; bind to 127.0.0.1 \
             or set server.allow_plaintext = true to accept an unencrypted connection"
        ))
    }
}

/// Binds the gRPC listener after applying the bind policy; logs a warning for an allowed plaintext bind.
pub async fn bind(listen_addr: &str, allow_plaintext: bool) -> Result<TcpListener, GrpcError> {
    let addr: SocketAddr = listen_addr
        .parse()
        .map_err(|_| GrpcError::BadAddress(listen_addr.to_string()))?;
    check_bind_policy(addr, allow_plaintext).map_err(GrpcError::Policy)?;
    if !addr.ip().is_loopback() {
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
}

impl ServeOptions {
    /// Options from `[server]`.
    pub fn from_server(server: &ServerConfig) -> Self {
        Self {
            keepalive_interval: Duration::from_secs(server.keepalive_interval_secs),
            keepalive_timeout: Duration::from_secs(server.keepalive_timeout_secs),
            limiter: LimiterConfig::default(),
            stream: StreamConfig::from_server(server),
        }
    }
}

/// Serves `Stream`, `History` and `Control` on `listener` until `shutdown` fires; open streams end with
/// `UNAVAILABLE` so the graceful shutdown is quick.
pub async fn serve(
    listener: TcpListener,
    store: Arc<dyn RedisStore>,
    authority: Arc<TokenAuthority>,
    options: ServeOptions,
    stats: Arc<GrpcStats>,
    auth_stats: Arc<AuthStats>,
    shutdown: CancellationToken,
) -> Result<(), GrpcError> {
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
        .add_service(ControlServer::new(ControlStub))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown.cancelled_owned())
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

/// Binds the listener, opens the token and starts serving in the background.
pub async fn start(
    server: &ServerConfig,
    store: Arc<dyn RedisStore>,
    authority: Arc<TokenAuthority>,
    shutdown: CancellationToken,
) -> Result<GrpcHandle, GrpcError> {
    let listener = bind(&server.listen_addr, server.allow_plaintext).await?;
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
    let task = tokio::spawn({
        let (stats, auth_stats, authority, shutdown) = (
            stats.clone(),
            auth_stats.clone(),
            authority.clone(),
            shutdown.clone(),
        );
        let options = ServeOptions::from_server(server);
        async move {
            if let Err(e) = serve(
                listener, store, authority, options, stats, auth_stats, shutdown,
            )
            .await
            {
                tracing::error!(error = %e, "gRPC server failed");
            }
        }
    });
    tracing::info!(addr = %local_addr, "gRPC server listening");
    Ok(GrpcHandle {
        local_addr,
        stats,
        auth_stats,
        authority,
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
