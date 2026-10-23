//! The accept loop: TCP connections become TLS streams for tonic.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::{Connected, TcpConnectInfo};

use super::TlsReloader;

/// Handshakes in flight at once; further connections wait in the kernel backlog.
const MAX_HANDSHAKES: usize = 256;
/// A client that does not finish the handshake in this time is dropped.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A TLS stream that tonic can serve; `remote_addr` of requests keeps working through [`Connected`].
pub struct TlsConn {
    inner: TlsStream<TcpStream>,
    info: TcpConnectInfo,
}

impl Connected for TlsConn {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for TlsConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// Accepts connections on `listener`, performs the TLS handshake with the configuration of `reloader` (so a
/// swapped certificate applies to the next handshake) and yields the established streams. The stream ends when
/// `shutdown` fires.
pub fn incoming(
    listener: TcpListener,
    reloader: Arc<TlsReloader>,
    shutdown: CancellationToken,
) -> ReceiverStream<io::Result<TlsConn>> {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let acceptor = TlsAcceptor::from(reloader.server_config());
    let slots = Arc::new(Semaphore::new(MAX_HANDSHAKES));
    tokio::spawn(async move {
        loop {
            let (socket, peer) = tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::warn!(error = %e, "gRPC accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                },
                _ = shutdown.cancelled() => return,
            };
            let Ok(permit) = slots.clone().try_acquire_owned() else {
                continue; // Overloaded: drop the connection.
            };
            let (acceptor, tx, reloader) = (acceptor.clone(), tx.clone(), reloader.clone());
            tokio::spawn(async move {
                let _ = socket.set_nodelay(true);
                match handshake(&acceptor, socket, peer).await {
                    Ok(conn) => {
                        drop(permit);
                        let _ = tx.send(Ok(conn)).await;
                    }
                    Err(e) => {
                        reloader
                            .stats
                            .handshake_failures
                            .fetch_add(1, Ordering::Relaxed);
                        // Port scanners and plaintext clients are routine: not worth a warning.
                        tracing::debug!(peer = %peer.ip(), error = %e, "TLS handshake failed");
                    }
                }
            });
        }
    });
    ReceiverStream::new(rx)
}

async fn handshake(
    acceptor: &TlsAcceptor,
    socket: TcpStream,
    peer: SocketAddr,
) -> io::Result<TlsConn> {
    let local = socket.local_addr().ok();
    let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(socket))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "handshake timed out"))??;
    Ok(TlsConn {
        inner: stream,
        info: TcpConnectInfo {
            local_addr: local,
            remote_addr: Some(peer),
        },
    })
}
