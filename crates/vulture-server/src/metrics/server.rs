//! The metrics HTTP endpoint: `/metrics`, `/healthz`, `/readyz`. A minimal HTTP/1.1 server (GET and HEAD,
//! one request per connection), enough for Prometheus, load balancers and the container health check.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::MetricsHub;
use crate::store::RedisStore;

const MAX_REQUEST_BYTES: usize = 8 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONNECTIONS: usize = 64;
const READY_TIMEOUT: Duration = Duration::from_secs(2);

/// Why the metrics endpoint could not start.
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    /// `metrics.listen_addr` is not a socket address.
    #[error("metrics.listen_addr: {0:?} is not a socket address (host:port)")]
    BadAddress(String),
    /// The listener could not be created.
    #[error("cannot listen on {addr} for metrics: {source}")]
    Bind {
        /// Address.
        addr: SocketAddr,
        /// Underlying error.
        source: std::io::Error,
    },
}

/// Binds the metrics listener. A non-loopback address is allowed (a container needs it) but logged: the
/// endpoint is not authenticated.
pub async fn bind(listen_addr: &str) -> Result<TcpListener, MetricsError> {
    let addr: SocketAddr = listen_addr
        .parse()
        .map_err(|_| MetricsError::BadAddress(listen_addr.to_string()))?;
    if !addr.ip().is_loopback() {
        tracing::warn!(
            %addr,
            "the metrics endpoint listens on a non-loopback address and is not authenticated: \
             publish it only to a trusted network or behind a reverse proxy"
        );
    }
    TcpListener::bind(addr)
        .await
        .map_err(|source| MetricsError::Bind { addr, source })
}

/// What `/readyz` checks: Redis answers and the gRPC listener is running.
pub struct Readiness {
    store: Arc<dyn RedisStore>,
    grpc_running: Arc<AtomicBool>,
}

impl Readiness {
    /// New check. `grpc_running` is set by the gRPC server while it serves.
    pub fn new(store: Arc<dyn RedisStore>, grpc_running: Arc<AtomicBool>) -> Self {
        Self {
            store,
            grpc_running,
        }
    }

    /// `Ok` when ready, otherwise the reason.
    pub async fn check(&self) -> Result<(), String> {
        if !self.grpc_running.load(Ordering::Relaxed) {
            return Err("the gRPC listener is not running".into());
        }
        match tokio::time::timeout(READY_TIMEOUT, self.store.health()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(format!("redis: {e}")),
            Err(_) => Err("redis: no answer in time".into()),
        }
    }
}

/// Serves the endpoint on `listener` until `shutdown`.
pub async fn serve(
    listener: TcpListener,
    hub: Arc<MetricsHub>,
    readiness: Arc<Readiness>,
    shutdown: CancellationToken,
) {
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let (socket, _) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!(error = %e, "metrics accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = shutdown.cancelled() => return,
        };
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            continue; // Too many scrapers: drop the connection.
        };
        let (hub, readiness) = (hub.clone(), readiness.clone());
        tokio::spawn(async move {
            let _ = tokio::time::timeout(REQUEST_TIMEOUT, handle(socket, &hub, &readiness)).await;
            drop(permit);
        });
    }
}

async fn handle(mut socket: TcpStream, hub: &MetricsHub, readiness: &Readiness) {
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        if buf.len() >= MAX_REQUEST_BYTES {
            return respond(&mut socket, 431, "text/plain", "request too large\n", true).await;
        }
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let mut parts = head.lines().next().unwrap_or("").split(' ');
    let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let is_head = method == "HEAD";
    if method != "GET" && !is_head {
        return respond(&mut socket, 405, "text/plain", "method not allowed\n", true).await;
    }
    let path = target.split('?').next().unwrap_or("");
    let (status, ctype, body) = match path {
        "/metrics" => (
            200,
            "text/plain; version=0.0.4; charset=utf-8",
            hub.render(),
        ),
        "/healthz" => (200, "text/plain", "ok\n".to_string()),
        "/readyz" => match readiness.check().await {
            Ok(()) => (200, "text/plain", "ready\n".to_string()),
            Err(reason) => (503, "text/plain", format!("not ready: {reason}\n")),
        },
        _ => (404, "text/plain", "not found\n".to_string()),
    };
    respond(&mut socket, status, ctype, &body, !is_head).await;
}

async fn respond(socket: &mut TcpStream, status: u16, ctype: &str, body: &str, with_body: bool) {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        431 => "Request Header Fields Too Large",
        _ => "Service Unavailable",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = socket.write_all(head.as_bytes()).await;
    if with_body {
        let _ = socket.write_all(body.as_bytes()).await;
    }
    let _ = socket.shutdown().await;
}

/// Calls `GET <path>` on `addr` and returns the status code and the body. Used by `vulture-server healthcheck`.
pub async fn healthcheck(addr: SocketAddr, path: &str) -> Result<(u16, String), String> {
    let work = async {
        let mut socket = TcpStream::connect(addr)
            .await
            .map_err(|e| format!("cannot connect to {addr}: {e}"))?;
        let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        socket
            .write_all(request.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let mut response = Vec::new();
        socket
            .read_to_end(&mut response)
            .await
            .map_err(|e| e.to_string())?;
        let text = String::from_utf8_lossy(&response);
        let status = text
            .lines()
            .next()
            .and_then(|l| l.split(' ').nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .ok_or_else(|| "malformed HTTP response".to_string())?;
        let body = text.split_once("\r\n\r\n").map_or("", |(_, b)| b);
        Ok((status, body.to_string()))
    };
    tokio::time::timeout(Duration::from_secs(3), work)
        .await
        .map_err(|_| format!("no answer from {addr} in time"))?
}
