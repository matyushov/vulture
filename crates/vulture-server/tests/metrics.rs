//! The metrics endpoint: exposition format, key series, cardinality, readiness and the loopback default.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use futures_util::future::BoxFuture;
use prometheus_parse::{Scrape, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use vulture_server::app::{self, AppContext};
use vulture_server::config::Config;
use vulture_server::exchange::binance::BinanceStatus;
use vulture_server::exchange::{EventSink, Exchange, ExchangeError};
use vulture_server::metrics::{
    self, AuthMetrics, GrpcMetrics, MetricsHub, Readiness, RedisSampler,
};

/// A connector that publishes `count` liquidations and keeps a Binance status the test can move.
struct Synthetic {
    status: Arc<BinanceStatus>,
    count: usize,
}

impl Exchange for Synthetic {
    fn name(&self) -> &str {
        "binance"
    }

    fn run<'a>(
        &'a self,
        ctx: AppContext,
        sink: EventSink,
    ) -> BoxFuture<'a, Result<(), ExchangeError>> {
        Box::pin(async move {
            for event in numbered(self.count) {
                sink.send(event).await.unwrap();
                self.status.liquidations.fetch_add(1, Ordering::Relaxed);
            }
            ctx.shutdown.cancelled().await;
            Ok(())
        })
    }

    fn collect_metrics(&self, out: &mut metrics::Families) {
        self.status.collect_metrics(out);
    }
}

async fn http(addr: std::net::SocketAddr, request: &str) -> (u16, String) {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    let status = response.split(' ').nth(1).unwrap().parse().unwrap();
    let body = response.split_once("\r\n\r\n").unwrap().1.to_string();
    (status, body)
}

async fn get(addr: std::net::SocketAddr, path: &str) -> (u16, String) {
    http(addr, &format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n")).await
}

fn parse(text: &str) -> Scrape {
    Scrape::parse(text.lines().map(|l| Ok(l.to_string()))).expect("the exposition must parse")
}

fn value(scrape: &Scrape, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    scrape
        .samples
        .iter()
        .find(|s| {
            s.metric == name
                && labels
                    .iter()
                    .all(|(k, v)| s.labels.get(k).is_some_and(|x| x == *v))
        })
        .map(|s| match s.value {
            Value::Counter(v) | Value::Gauge(v) | Value::Untyped(v) => v,
            _ => panic!("unexpected sample type for {name}"),
        })
}

#[tokio::test]
async fn the_endpoint_reports_the_whole_server_and_readiness_follows_redis() {
    let backend = Backend::memory();
    let Backend::Memory(memory) = &backend else {
        unreachable!()
    };
    let server = Server::start(&backend, test_stream_config()).await;

    let shutdown = CancellationToken::new();
    let ctx = AppContext::new(Config::default(), shutdown.clone());
    let hub = Arc::new(MetricsHub::new());
    let status = Arc::new(BinanceStatus::default());
    status.symbols.store(2, Ordering::Relaxed);
    status.used_weight_1m.store(120, Ordering::Relaxed);
    let synthetic: Box<dyn Exchange> = Box::new(Synthetic {
        status: status.clone(),
        count: 5,
    });
    hub.register(Arc::new(GrpcMetrics(server.stats.clone())));
    hub.register(Arc::new(AuthMetrics(server.auth_stats.clone())));
    let sampler = RedisSampler::new();
    hub.register(sampler.clone());
    // The app registers the writer and the connectors with the hub itself.
    let app_task = tokio::spawn(app::run_with_hub(
        ctx,
        backend.store(),
        vec![synthetic],
        Some(hub.clone()),
    ));

    let grpc_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let listener = metrics::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    assert!(addr.ip().is_loopback());
    tokio::spawn(metrics::serve(
        listener,
        hub.clone(),
        Arc::new(Readiness::new(backend.store(), grpc_running.clone())),
        shutdown.clone(),
    ));

    // Synthetic activity: events written, a client subscribed, a bad token.
    wait_until("events written", || memory.entries().len() >= 5).await;
    sampler.sample(backend.store().as_ref()).await;
    let before = parse(&get(addr, "/metrics").await.1);
    let mut client = server.client().await;
    let mut stream = client.subscribe(request("0")).await.unwrap().into_inner();
    recv(&mut stream, 5).await;
    let _ = server
        .client_with(Auth::bearer("wrong-wrong-wrong-wrong"))
        .await
        .subscribe(request(""))
        .await;
    status.oi_events.fetch_add(3, Ordering::Relaxed);

    let (code, body) = get(addr, "/metrics").await;
    assert_eq!(code, 200);
    let after = parse(&body);

    // Key series are present ...
    for name in [
        "vulture_build_info",
        "vulture_process_uptime_seconds",
        "vulture_redis_written_total",
        "vulture_redis_batches_total",
        "vulture_redis_buffered_events",
        "vulture_redis_stream_length",
        "vulture_grpc_clients",
        "vulture_grpc_entries_sent_total",
        "vulture_auth_failures_total",
        "vulture_symbols",
        "vulture_rest_used_weight_1m",
    ] {
        assert!(
            after.samples.iter().any(|s| s.metric == name),
            "missing {name} in\n{body}"
        );
    }
    // ... and they move.
    let liq = [("exchange", "binance"), ("type", "liquidation")];
    assert_eq!(value(&after, "vulture_events_total", &liq), Some(5.0));
    assert_eq!(
        value(&after, "vulture_events_total", &[("type", "open_interest")]),
        Some(3.0)
    );
    assert!(value(&after, "vulture_redis_written_total", &[]) >= Some(5.0));
    assert_eq!(value(&before, "vulture_grpc_clients", &[]), Some(0.0));
    assert_eq!(value(&after, "vulture_grpc_clients", &[]), Some(1.0));
    assert!(value(&after, "vulture_grpc_entries_sent_total", &[]) >= Some(5.0));
    assert_eq!(value(&after, "vulture_auth_failures_total", &[]), Some(1.0));
    assert_eq!(value(&after, "vulture_symbols", &[]), Some(2.0));
    assert_eq!(
        value(&after, "vulture_rest_used_weight_1m", &[]),
        Some(120.0)
    );
    assert_eq!(
        value(
            &after,
            "vulture_redis_connection_state",
            &[("state", "connected")]
        ),
        Some(1.0)
    );
    assert_eq!(value(&after, "vulture_redis_stream_length", &[]), Some(5.0));

    // Label cardinality is bounded: a fixed set of label names, no symbols, no peers, few series.
    let allowed: BTreeSet<&str> = ["exchange", "type", "state", "result", "code", "version"].into();
    for s in &after.samples {
        for (k, v) in s.labels.iter() {
            assert!(allowed.contains(k.as_str()), "{}: label {k}", s.metric);
            assert!(
                k == "version" || (!v.contains("PERP") && !v.contains(':') && !v.contains('.')),
                "{}: label value {v:?} looks like a symbol or address",
                s.metric
            );
        }
    }
    assert!(after.samples.len() < 250, "{} series", after.samples.len());

    // Health and readiness.
    assert_eq!(get(addr, "/healthz").await.0, 200);
    assert_eq!(get(addr, "/readyz").await.0, 200);
    backend.set_failing(true);
    let (code, body) = get(addr, "/readyz").await;
    assert_eq!(code, 503, "{body}");
    assert!(body.contains("redis"), "{body}");
    assert_eq!(
        get(addr, "/healthz").await.0,
        200,
        "the process is still up"
    );
    backend.set_failing(false);
    assert_eq!(get(addr, "/readyz").await.0, 200);
    grpc_running.store(false, Ordering::Relaxed);
    assert_eq!(get(addr, "/readyz").await.0, 503);

    // Other paths and methods.
    assert_eq!(get(addr, "/nope").await.0, 404);
    assert_eq!(get(addr, "/metrics?x=1").await.0, 200);
    assert_eq!(
        http(addr, "POST /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .0,
        405
    );

    shutdown.cancel();
    server.stop().await;
    tokio::time::timeout(Duration::from_secs(10), app_task)
        .await
        .expect("the app stops")
        .unwrap();
}

#[tokio::test]
async fn healthcheck_client_reads_status_codes() {
    let backend = Backend::memory();
    let hub = Arc::new(MetricsHub::new());
    let listener = metrics::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let running = Arc::new(std::sync::atomic::AtomicBool::new(false));
    tokio::spawn(metrics::serve(
        listener,
        hub,
        Arc::new(Readiness::new(backend.store(), running.clone())),
        shutdown.clone(),
    ));
    assert_eq!(metrics::healthcheck(addr, "/healthz").await.unwrap().0, 200);
    assert_eq!(metrics::healthcheck(addr, "/readyz").await.unwrap().0, 503);
    running.store(true, Ordering::Relaxed);
    assert_eq!(metrics::healthcheck(addr, "/readyz").await.unwrap().0, 200);
    shutdown.cancel();
}

#[test]
fn the_default_listener_is_loopback() {
    let addr: std::net::SocketAddr = Config::default().metrics.listen_addr.parse().unwrap();
    assert!(addr.ip().is_loopback());
    assert_eq!(addr.port(), 9100);
}
