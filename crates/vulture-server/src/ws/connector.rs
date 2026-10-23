//! Establishing the WebSocket connection, behind a trait so tests can run on in-memory pipes.

use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::{Sink, Stream};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

/// A connected WebSocket, type-erased.
pub trait WsConnection:
    Stream<Item = Result<Message, WsError>> + Sink<Message, Error = WsError> + Unpin + Send
{
}

impl<T> WsConnection for T where
    T: Stream<Item = Result<Message, WsError>> + Sink<Message, Error = WsError> + Unpin + Send
{
}

/// Boxed connection returned by connectors.
pub type BoxedConnection = Box<dyn WsConnection>;

/// Why a connection attempt failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectError {
    /// The server answered the upgrade request with an HTTP error (403, 429, 418, ...).
    Http {
        /// Status code.
        status: u16,
        /// `Retry-After` in seconds, if present.
        retry_after: Option<Duration>,
    },
    /// Anything else (DNS, TCP, TLS, protocol).
    Other(String),
}

/// Opens connections. The 10 s connect timeout is applied by the caller around [`WsConnector::connect`].
pub trait WsConnector: Send + Sync + 'static {
    /// Connects to `url` and completes the WebSocket upgrade.
    fn connect<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<BoxedConnection, ConnectError>>;
}

/// The production connector: `tokio-tungstenite` with rustls and the bundled web PKI roots.
#[derive(Debug, Clone, Copy, Default)]
pub struct TungsteniteConnector;

impl WsConnector for TungsteniteConnector {
    fn connect<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<BoxedConnection, ConnectError>> {
        Box::pin(async move {
            match tokio_tungstenite::connect_async(url).await {
                Ok((ws, _response)) => Ok(Box::new(ws) as BoxedConnection),
                Err(WsError::Http(response)) => Err(ConnectError::Http {
                    status: response.status().as_u16(),
                    retry_after: response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.trim().parse::<u64>().ok())
                        .map(Duration::from_secs),
                }),
                Err(e) => Err(ConnectError::Other(e.to_string())),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    /// `wss://` connections build a rustls client config; without a compiled-in crypto provider that panics
    /// at the first TLS handshake, so guard it here (no network involved).
    #[test]
    fn tls_client_config_can_be_built() {
        let roots = rustls::RootCertStore::empty();
        let _config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
    }
}
