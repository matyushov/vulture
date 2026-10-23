//! TLS on the gRPC listener: handshake, hot reload (poll), rejection of bad replacements, mTLS and the
//! validation of the files. SIGHUP has its own file (`tls_sighup.rs`) because a signal is process-wide.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::certs::{Ca, Files, Leaf};
use common::*;
use rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::TlsConnector;
use tokio_util::sync::CancellationToken;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use vulture_proto::v1::stream_client::StreamClient;
use vulture_server::auth::{AuthStats, LimiterConfig, TokenAuthority, TokenStore};
use vulture_server::config::TlsConfig;
use vulture_server::grpc::{self, GrpcStats, ServeOptions};
use vulture_server::metrics::{MetricSource, MetricsHub};
use vulture_server::tls::{self, TlsReloader};

struct TlsServer {
    addr: std::net::SocketAddr,
    token: String,
    reloader: Arc<TlsReloader>,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl TlsServer {
    async fn start(backend: &Backend, cfg: &TlsConfig, interval: Duration) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let token_store = TokenStore::new(dir.path());
        let token = token_store.create_if_missing().unwrap().into_secret();
        let authority = TokenAuthority::open(token_store).unwrap();
        let shutdown = CancellationToken::new();
        let reloader = TlsReloader::load(cfg).expect("TLS files");
        if !interval.is_zero() {
            tls::spawn_watchers(reloader.clone(), interval, shutdown.clone()).unwrap();
        }
        let listener = grpc::bind_tls("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let options = ServeOptions {
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            limiter: LimiterConfig::default(),
            stream: test_stream_config(),
        };
        let task = tokio::spawn({
            let (store, reloader, shutdown) = (backend.store(), reloader.clone(), shutdown.clone());
            async move {
                grpc::serve_tls(
                    listener,
                    reloader,
                    store,
                    authority,
                    options,
                    Arc::new(GrpcStats::default()),
                    Arc::new(AuthStats::default()),
                    shutdown,
                )
                .await
                .expect("serve_tls");
            }
        });
        Self {
            addr,
            token: token.expose().to_string(),
            reloader,
            shutdown,
            task,
            _dir: dir,
        }
    }

    async fn channel(
        &self,
        ca: &Ca,
        identity: Option<&Leaf>,
    ) -> Result<Channel, tonic::transport::Error> {
        let mut tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca.pem()))
            .domain_name("localhost");
        if let Some(id) = identity {
            tls = tls.identity(Identity::from_pem(&id.cert_pem, &id.key_pem));
        }
        Endpoint::from_shared(format!("https://{}", self.addr))
            .unwrap()
            .tls_config(tls)
            .unwrap()
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
    }

    async fn client(&self, ca: &Ca) -> StreamClient<InterceptedService<Channel, Auth>> {
        StreamClient::with_interceptor(
            self.channel(ca, None).await.expect("TLS connect"),
            Auth::bearer(&self.token),
        )
    }

    /// The DER of the certificate the server presents to a new connection.
    async fn presented(&self, ca: &Ca) -> Vec<u8> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from(ca.der())).unwrap();
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let tcp = tokio::net::TcpStream::connect(self.addr).await.unwrap();
        let conn = TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .expect("handshake");
        conn.get_ref().1.peer_certificates().unwrap()[0]
            .as_ref()
            .to_vec()
    }

    async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .expect("the server must stop")
            .unwrap();
    }
}

fn metric(reloader: &TlsReloader, line_start: &str) -> Option<f64> {
    let mut f = vulture_server::metrics::Families::new();
    reloader.collect(&mut f);
    f.render()
        .lines()
        .find(|l| l.starts_with(line_start))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

#[tokio::test]
async fn handshake_succeeds_and_events_flow_over_tls() {
    let (ca, backend) = (Ca::new("test ca"), Backend::memory());
    let files = Files::new(&ca, &ca.server("one"));
    let server = TlsServer::start(&backend, &files.config(), Duration::ZERO).await;

    let mut client = server.client(&ca).await;
    let mut stream = client.subscribe(request("")).await.unwrap().into_inner();
    backend.append(&numbered(3)).await;
    assert_eq!(recv(&mut stream, 3).await.len(), 3);
    // ALPN picked HTTP/2 and the certificate is the one on disk.
    assert_eq!(server.presented(&ca).await, ca_leaf_der(&files));
    server.stop().await;
}

fn ca_leaf_der(files: &Files) -> Vec<u8> {
    use rustls::pki_types::pem::PemObject;
    CertificateDer::pem_file_iter(&files.cert)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .as_ref()
        .to_vec()
}

#[tokio::test]
async fn a_wrong_token_over_tls_is_still_refused_and_plaintext_clients_fail() {
    let (ca, backend) = (Ca::new("test ca"), Backend::memory());
    let files = Files::new(&ca, &ca.server("one"));
    let server = TlsServer::start(&backend, &files.config(), Duration::ZERO).await;
    let mut bad = StreamClient::with_interceptor(
        server.channel(&ca, None).await.unwrap(),
        Auth::bearer("not-the-token-0123456789abcdef"),
    );
    assert_eq!(
        bad.subscribe(request("")).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    // A plaintext client cannot talk to the TLS port and the failure is counted.
    let plain = Channel::from_shared(format!("http://{}", server.addr))
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await;
    if let Ok(channel) = plain {
        let mut c = StreamClient::with_interceptor(channel, Auth::bearer(&server.token));
        assert!(c.subscribe(request("")).await.is_err());
    }
    let reloader = server.reloader.clone();
    wait_until("handshake failure counted", || {
        reloader.stats.handshake_failures.load(Ordering::Relaxed) >= 1
    })
    .await;
    server.stop().await;
}

#[tokio::test]
async fn mtime_poll_reloads_the_certificate_without_dropping_open_streams() {
    let (ca, backend) = (Ca::new("test ca"), Backend::memory());
    let files = Files::new(&ca, &ca.server("old"));
    let server = TlsServer::start(&backend, &files.config(), Duration::from_millis(50)).await;
    let old_der = server.presented(&ca).await;

    let mut client = server.client(&ca).await;
    let mut stream = client.subscribe(request("")).await.unwrap().into_inner();
    backend.append(&numbered(2)).await;
    recv(&mut stream, 2).await;

    let new = ca.server("new");
    files.replace(&new);
    let reloader = server.reloader.clone();
    wait_until("reload by poll", || {
        reloader.stats.reload_ok.load(Ordering::Relaxed) >= 1
    })
    .await;

    // New connections get the new certificate ...
    let presented = server.presented(&ca).await;
    assert_eq!(presented, new.der);
    assert_ne!(presented, old_der);
    // ... and the stream that was open before keeps receiving.
    backend.append(&numbered(1)).await;
    assert_eq!(recv(&mut stream, 1).await.len(), 1);
    assert_eq!(
        metric(&server.reloader, "vulture_tls_reloads_total{result=\"ok\"}"),
        Some(1.0)
    );
    server.stop().await;
}

#[tokio::test]
async fn an_invalid_replacement_keeps_the_old_certificate_and_is_counted() {
    let (ca, backend) = (Ca::new("test ca"), Backend::memory());
    let good = ca.server("good");
    let files = Files::new(&ca, &good);
    let server = TlsServer::start(&backend, &files.config(), Duration::from_millis(50)).await;
    let reloader = server.reloader.clone();
    let failures = || reloader.stats.reload_failed.load(Ordering::Relaxed);

    // 1. Garbage instead of a certificate.
    common::certs::write_atomic(
        &files.cert,
        b"-----BEGIN CERTIFICATE-----\nnot base64!\n-----END CERTIFICATE-----\n",
    );
    wait_until("failure 1", || failures() >= 1).await;
    assert_eq!(server.presented(&ca).await, good.der);

    // 2. A certificate whose key does not match.
    let other = ca.server("other");
    files.replace(&good);
    wait_until("recovery", || {
        reloader.stats.reload_ok.load(Ordering::Relaxed) >= 1
    })
    .await;
    common::certs::write_atomic(&files.cert, other.cert_pem.as_bytes());
    wait_until("failure 2", || failures() >= 2).await;
    assert_eq!(server.presented(&ca).await, good.der);

    // 3. An expired certificate with its own matching key.
    files.replace(&ca.expired_server("expired"));
    wait_until("failure 3", || failures() >= 3).await;
    assert_eq!(server.presented(&ca).await, good.der);
    assert_eq!(metric(&reloader, "vulture_tls_last_reload_ok"), Some(0.0));
    assert!(metric(&reloader, "vulture_tls_reloads_total{result=\"error\"}").unwrap() >= 3.0);

    // The server keeps serving and a valid file brings it back.
    let mut client = server.client(&ca).await;
    client.subscribe(request("")).await.unwrap();
    let fixed = ca.server("fixed");
    files.replace(&fixed);
    wait_until("recovery 2", || {
        metric(&reloader, "vulture_tls_last_reload_ok") == Some(1.0)
    })
    .await;
    assert_eq!(server.presented(&ca).await, fixed.der);
    server.stop().await;
}

#[tokio::test]
async fn expiry_gauge_and_metrics_are_exported() {
    let (ca, backend) = (Ca::new("test ca"), Backend::memory());
    let files = Files::new(&ca, &ca.server("one"));
    let server = TlsServer::start(&backend, &files.config(), Duration::ZERO).await;
    let hub = MetricsHub::new();
    hub.register(server.reloader.clone());
    let text = hub.render();
    for needle in [
        "vulture_tls_enabled 1",
        "vulture_tls_certificate_expiry_seconds ",
        "vulture_tls_reloads_total{result=\"ok\"} 0",
        "vulture_tls_reloads_total{result=\"error\"} 0",
        "vulture_tls_last_reload_ok 1",
    ] {
        assert!(text.contains(needle), "missing {needle:?} in\n{text}");
    }
    // rcgen certificates are valid for a long time by default.
    assert!(
        metric(&server.reloader, "vulture_tls_certificate_expiry_seconds").unwrap()
            > 86_400.0 * 30.0
    );
    server.stop().await;
}

#[tokio::test]
async fn mtls_accepts_a_valid_client_certificate_and_rejects_a_missing_one() {
    let (ca, backend) = (Ca::new("server ca"), Backend::memory());
    let client_ca = Ca::new("client ca");
    let files = Files::new(&ca, &ca.server("one"));
    std::fs::write(files.path("client-ca.pem"), client_ca.pem()).unwrap();
    let mut cfg = files.config();
    cfg.client_ca = Some(files.path("client-ca.pem"));
    let server = TlsServer::start(&backend, &cfg, Duration::ZERO).await;
    assert!(server.reloader.mtls());

    // With a certificate from the client CA: works.
    let ok = client_ca.client("alice");
    let channel = server.channel(&ca, Some(&ok)).await.expect("mTLS connect");
    let mut client = StreamClient::with_interceptor(channel, Auth::bearer(&server.token));
    let mut stream = client.subscribe(request("")).await.unwrap().into_inner();
    backend.append(&numbered(1)).await;
    recv(&mut stream, 1).await;

    // Without one: the connection or the first call fails.
    match server.channel(&ca, None).await {
        Err(_) => {}
        Ok(channel) => {
            let mut c = StreamClient::with_interceptor(channel, Auth::bearer(&server.token));
            assert!(
                c.subscribe(request("")).await.is_err(),
                "no client certificate must be refused"
            );
        }
    }
    // With a certificate from another CA: refused as well.
    let stranger = Ca::new("stranger").client("mallory");
    match server.channel(&ca, Some(&stranger)).await {
        Err(_) => {}
        Ok(channel) => {
            let mut c = StreamClient::with_interceptor(channel, Auth::bearer(&server.token));
            assert!(c.subscribe(request("")).await.is_err());
        }
    }
    server.stop().await;
}

#[tokio::test]
async fn non_loopback_bind_with_tls_is_accepted() {
    // Without TLS the same address is refused (see grpc_auth.rs); with TLS the policy lets it through.
    let listener = grpc::bind_tls("0.0.0.0:0")
        .await
        .expect("a TLS bind may be non-loopback");
    drop(listener);
    assert!(grpc::check_bind_policy("192.0.2.1:50051".parse().unwrap(), false, true).is_ok());
    assert!(grpc::check_bind_policy("192.0.2.1:50051".parse().unwrap(), false, false).is_err());
}

#[test]
fn file_validation_errors_are_specific() {
    let ca = Ca::new("test ca");
    let good = ca.server("good");
    let files = Files::new(&ca, &good);
    let check = |f: &Files| tls::check_files(&f.config());

    let info = check(&files).expect("valid pair");
    assert!(info.expires_in_secs() > 86_400 * 30);
    assert!(info.subject.contains("good"));

    // Missing files.
    let mut cfg = files.config();
    cfg.cert = files.path("absent.pem");
    assert!(
        tls::check_files(&cfg)
            .unwrap_err()
            .to_string()
            .contains("cannot read certificate")
    );
    let mut cfg = files.config();
    cfg.key = files.path("absent.key");
    assert!(
        tls::check_files(&cfg)
            .unwrap_err()
            .to_string()
            .contains("cannot read private key")
    );

    // Not PEM / empty / wrong item.
    common::certs::write_atomic(&files.cert, b"hello");
    assert!(
        check(&files)
            .unwrap_err()
            .to_string()
            .contains("contains no certificate")
    );
    files.replace(&good);
    common::certs::write_atomic(&files.key, b"hello");
    assert!(
        check(&files)
            .unwrap_err()
            .to_string()
            .contains("private key")
    );

    // Key of another certificate.
    files.replace(&good);
    let other = ca.server("other");
    common::certs::write_atomic(&files.key, other.key_pem.as_bytes());
    let err = check(&files).unwrap_err().to_string();
    assert!(err.contains("does not match"), "{err}");
    assert!(
        !err.contains("PRIVATE KEY"),
        "key material must not appear in errors"
    );

    // Expired.
    files.replace(&ca.expired_server("old"));
    assert!(check(&files).unwrap_err().to_string().contains("expired"));

    // Client CA must hold a certificate.
    files.replace(&good);
    let mut cfg = files.config();
    std::fs::write(files.path("empty-ca.pem"), "").unwrap();
    cfg.client_ca = Some(files.path("empty-ca.pem"));
    assert!(
        tls::check_files(&cfg)
            .unwrap_err()
            .to_string()
            .contains("client CA")
    );
    cfg.client_ca = Some(files.ca.clone());
    assert!(tls::check_files(&cfg).is_ok());
}
