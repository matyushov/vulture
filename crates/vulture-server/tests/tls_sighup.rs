//! SIGHUP reloads the certificate without dropping open streams. A real signal is sent to this test process, so
//! this file holds a single test.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::certs::{Ca, Files};
use common::*;
use rustls::pki_types::{CertificateDer, ServerName};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use vulture_proto::v1::stream_client::StreamClient;
use vulture_server::auth::{AuthStats, LimiterConfig, TokenAuthority, TokenStore};
use vulture_server::grpc::{self, GrpcStats, ServeOptions};
use vulture_server::tls::{self, TlsReloader};

async fn presented(addr: std::net::SocketAddr, ca: &Ca) -> Vec<u8> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from(ca.der())).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let conn = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap();
    conn.get_ref().1.peer_certificates().unwrap()[0]
        .as_ref()
        .to_vec()
}

#[tokio::test]
async fn sighup_swaps_the_certificate_and_open_streams_survive() {
    let (ca, backend) = (Ca::new("test ca"), Backend::memory());
    let old = ca.server("old");
    let files = Files::new(&ca, &old);

    let dir = tempfile::tempdir().unwrap();
    let token_store = TokenStore::new(dir.path());
    let token = token_store.create_if_missing().unwrap().into_secret();
    let authority = TokenAuthority::open(token_store).unwrap();
    let shutdown = CancellationToken::new();
    let reloader = TlsReloader::load(&files.config()).unwrap();
    // No polling: only the signal can trigger the reload.
    tls::spawn_watchers(reloader.clone(), Duration::ZERO, shutdown.clone()).unwrap();
    let listener = grpc::bind_tls("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let options = ServeOptions {
        keepalive_interval: Duration::from_secs(30),
        keepalive_timeout: Duration::from_secs(10),
        limiter: LimiterConfig::default(),
        stream: test_stream_config(),
        symbols: None,
        alerts: None,
        history: None,
        digest: None,
    };
    let server = tokio::spawn(grpc::serve_tls(
        listener,
        reloader.clone(),
        backend.store(),
        authority,
        options,
        Arc::new(GrpcStats::default()),
        Arc::new(AuthStats::default()),
        shutdown.clone(),
    ));

    let channel: Channel = Endpoint::from_shared(format!("https://{addr}"))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(ca.pem()))
                .domain_name("localhost"),
        )
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = StreamClient::with_interceptor(channel, Auth::bearer(token.expose()));
    let mut stream = client.subscribe(request("")).await.unwrap().into_inner();
    backend.append(&numbered(1)).await;
    recv(&mut stream, 1).await;
    assert_eq!(presented(addr, &ca).await, old.der);

    let new = ca.server("new");
    files.replace(&new);
    // Nothing changes without the signal (polling is off).
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(reloader.stats.reload_ok.load(Ordering::Relaxed), 0);

    let status = std::process::Command::new("kill")
        .args(["-HUP", &std::process::id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    wait_until("SIGHUP reload", || {
        reloader.stats.reload_ok.load(Ordering::Relaxed) == 1
    })
    .await;

    assert_eq!(presented(addr, &ca).await, new.der);
    backend.append(&numbered(2)).await;
    assert_eq!(
        recv(&mut stream, 2).await.len(),
        2,
        "the open stream survived the reload"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("stops")
        .unwrap()
        .unwrap();
}
