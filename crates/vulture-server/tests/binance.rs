//! Binance connector tests on mock transports: an in-memory WebSocket server (tokio-tungstenite over
//! `tokio::io::duplex`) and a scripted REST client. Time is paused, so schedules of tens of seconds run
//! instantly and deterministically. The `binance_live` test (ignored) talks to the real public endpoints.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async, client_async};
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::{Event, Gap, Liquidation, OpenInterest, Side, event::Payload};
use vulture_server::app::AppContext;
use vulture_server::config::Config;
use vulture_server::exchange::binance::{
    BinanceConfig, BinanceConnector, BinanceStatus, BinanceTuning, HttpClient, HttpError,
    HttpResponse, encode_query_value,
};
use vulture_server::exchange::{Exchange, ExchangeError};
use vulture_server::normalize::BaseAliases;
use vulture_server::ws::{BoxedConnection, Clock, ConnectError, MaxAge, WsConnector, WsProfile};

// ---------------------------------------------------------------------------------------------
// Harness: WebSocket
// ---------------------------------------------------------------------------------------------

type ServerWs = WebSocketStream<DuplexStream>;
type Script = Arc<dyn Fn(ServerWs, usize) -> BoxFuture<'static, ()> + Send + Sync>;

fn script<F, Fut>(f: F) -> Script
where
    F: Fn(ServerWs, usize) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    Arc::new(move |ws, n| Box::pin(f(ws, n)))
}

struct MockWs {
    scripts: Mutex<VecDeque<Script>>,
    fallback: Script,
    attempts: AtomicUsize,
    subscriptions: Mutex<Vec<String>>,
}

impl MockWs {
    fn new(scripts: Vec<Script>, fallback: Script) -> Arc<Self> {
        Arc::new(Self {
            scripts: Mutex::new(scripts.into()),
            fallback,
            attempts: AtomicUsize::new(0),
            subscriptions: Mutex::new(Vec::new()),
        })
    }
}

impl WsConnector for MockWs {
    fn connect<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<BoxedConnection, ConnectError>> {
        Box::pin(async move {
            assert!(
                url.ends_with("/market/ws") || url.contains("mock"),
                "unexpected url {url}"
            );
            let n = self.attempts.fetch_add(1, Ordering::SeqCst);
            let script = self
                .scripts
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| self.fallback.clone());
            let (client, server) = tokio::io::duplex(1 << 20);
            tokio::spawn(async move {
                if let Ok(ws) = accept_async(server).await {
                    script(ws, n).await;
                }
            });
            let (ws, _) = client_async("ws://mock.invalid/", client)
                .await
                .map_err(|e| ConnectError::Other(e.to_string()))?;
            Ok(Box::new(ws) as BoxedConnection)
        })
    }
}

/// Waits for the SUBSCRIBE request, records it and acknowledges like Binance does.
async fn handshake(ws: &mut ServerWs, record: Option<&MockWs>) -> bool {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(s))) if s.contains("SUBSCRIBE") => {
                if let Some(r) = record {
                    r.subscriptions.lock().unwrap().push(s.to_string());
                }
                return ws
                    .send(Message::text(r#"{"result":null,"id":1}"#))
                    .await
                    .is_ok();
            }
            Some(Ok(_)) => {}
            _ => return false,
        }
    }
}

fn mark_frame(prices: &[(&str, &str)]) -> String {
    let items: Vec<String> = prices
        .iter()
        .map(|(s, p)| {
            format!(
                r#"{{"e":"markPriceUpdate","E":1792775340000,"s":"{s}","p":"{p}","P":"{p}","i":"{p}","r":"0.0001","T":1792780800000}}"#
            )
        })
        .collect();
    format!("[{}]", items.join(","))
}

fn force_order(symbol: &str, side: &str, ap: &str, z: &str, t: i64) -> String {
    format!(
        r#"{{"e":"forceOrder","E":{e},"o":{{"s":"{symbol}","S":"{side}","o":"LIMIT","f":"IOC","q":"{z}","p":"{ap}","ap":"{ap}","X":"FILLED","l":"{z}","z":"{z}","T":{t},"ps":"{symbol}","st":1}}}}"#,
        e = t + 100
    )
}

/// What the mock WebSocket sends after the acknowledgement.
#[derive(Clone, Default)]
struct Plan {
    /// Mark price frame sent right after the ack and then once per second (the liveness stream).
    marks: Option<String>,
    /// Frames sent after the first mark frame and a 1 s pause, in order, without delay between them.
    frames: Vec<String>,
    /// Close the connection after the frames instead of continuing the mark ticks.
    close_after: bool,
}

fn plan_script(plan: Plan, net: Option<Arc<MockWs>>) -> Script {
    script(move |mut ws, _| {
        let plan = plan.clone();
        let net = net.clone();
        async move {
            if !handshake(&mut ws, net.as_deref()).await {
                return;
            }
            if let Some(m) = &plan.marks
                && ws.send(Message::text(m.clone())).await.is_err()
            {
                return;
            }
            sleep(Duration::from_secs(1)).await;
            for f in &plan.frames {
                if ws.send(Message::text(f.clone())).await.is_err() {
                    return;
                }
            }
            if plan.close_after {
                let _ = ws.close(None).await;
                return;
            }
            let mut timer = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    incoming = ws.next() => match incoming {
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                        Some(Ok(_)) => {}
                    },
                    _ = timer.tick() => {
                        if let Some(m) = &plan.marks
                            && ws.send(Message::text(m.clone())).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------------------------
// Harness: REST
// ---------------------------------------------------------------------------------------------

#[derive(Clone)]
struct Reply {
    delay: Duration,
    /// `None` never answers.
    result: Option<Result<HttpResponse, HttpError>>,
}

impl Reply {
    fn status(status: u16, body: &str) -> Self {
        Self {
            delay: Duration::ZERO,
            result: Some(Ok(HttpResponse {
                status,
                retry_after: None,
                used_weight_1m: None,
                body: body.to_string(),
            })),
        }
    }

    fn ok(body: &str) -> Self {
        Self::status(200, body)
    }

    fn hang() -> Self {
        Self {
            delay: Duration::ZERO,
            result: None,
        }
    }

    fn error(e: HttpError) -> Self {
        Self {
            delay: Duration::ZERO,
            result: Some(Err(e)),
        }
    }

    fn after(mut self, d: Duration) -> Self {
        self.delay = d;
        self
    }

    fn retry_after(mut self, secs: u64) -> Self {
        if let Some(Ok(r)) = &mut self.result {
            r.retry_after = Some(Duration::from_secs(secs));
        }
        self
    }

    fn weight(mut self, w: u32) -> Self {
        if let Some(Ok(r)) = &mut self.result {
            r.used_weight_1m = Some(w);
        }
        self
    }
}

type Handler = dyn Fn(&str, usize) -> Reply + Send + Sync;

struct MockHttp {
    handler: Box<Handler>,
    log: Mutex<Vec<(Instant, String)>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    start: Instant,
}

struct InFlight<'a>(&'a AtomicUsize);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl MockHttp {
    fn new(handler: impl Fn(&str, usize) -> Reply + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            handler: Box::new(handler),
            log: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            start: Instant::now(),
        })
    }

    fn oi_requests(&self) -> Vec<(Duration, String)> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, p)| p.starts_with("/fapi/v1/openInterest"))
            .map(|(t, p)| (*t - self.start, p.clone()))
            .collect()
    }

    fn info_requests(&self) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, p)| p.starts_with("/fapi/v1/exchangeInfo"))
            .count()
    }
}

impl HttpClient for MockHttp {
    fn get<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<HttpResponse, HttpError>> {
        Box::pin(async move {
            let idx = {
                let mut log = self.log.lock().unwrap();
                log.push((Instant::now(), path.to_string()));
                log.len() - 1
            };
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            let _guard = InFlight(&self.in_flight);
            let reply = (self.handler)(path, idx);
            if !reply.delay.is_zero() {
                sleep(reply.delay).await;
            }
            match reply.result {
                Some(r) => r,
                None => std::future::pending().await,
            }
        })
    }
}

/// `(raw, base, quote)` of trading perpetuals.
type Sym = (&'static str, &'static str, &'static str);

/// A USDT symbol whose base is the name without the quote (distinct bases, so nothing collides).
fn usdt(raw: &'static str) -> Sym {
    (raw, raw.strip_suffix("USDT").unwrap(), "USDT")
}

fn info_body(syms: &[Sym]) -> String {
    let items: Vec<String> = syms
        .iter()
        .map(|(raw, base, quote)| {
            format!(
                r#"{{"symbol":"{raw}","pair":"{raw}","contractType":"PERPETUAL","status":"TRADING","baseAsset":"{base}","quoteAsset":"{quote}"}}"#
            )
        })
        .collect();
    format!(r#"{{"timezone":"UTC","symbols":[{}]}}"#, items.join(","))
}

fn oi_body(symbol: &str, oi: &str, time: i64) -> String {
    format!(r#"{{"symbol":"{symbol}","openInterest":"{oi}","time":{time}}}"#)
}

/// Finds the raw symbol of an open interest request among `known`.
fn symbol_of<'a>(path: &str, known: &[&'a str]) -> Option<&'a str> {
    let q = path.strip_prefix("/fapi/v1/openInterest?symbol=")?;
    known.iter().copied().find(|s| encode_query_value(s) == q)
}

/// Serves `exchangeInfo` from `infos[min(call, last)]` and open interest answers of `oi` for everything else.
fn standard_http(
    infos: Vec<String>,
    known: Vec<&'static str>,
    oi: &'static str,
    oi_reply: impl Fn(Reply) -> Reply + Send + Sync + 'static,
) -> Arc<MockHttp> {
    let info_calls = AtomicUsize::new(0);
    MockHttp::new(move |path, idx| {
        if path.starts_with("/fapi/v1/exchangeInfo") {
            let n = info_calls.fetch_add(1, Ordering::SeqCst);
            return Reply::ok(&infos[n.min(infos.len() - 1)]);
        }
        let symbol = symbol_of(path, &known).unwrap_or("UNKNOWN");
        oi_reply(Reply::ok(&oi_body(
            symbol,
            oi,
            1_792_775_000_000 + idx as i64,
        )))
    })
}

// ---------------------------------------------------------------------------------------------
// Harness: rig
// ---------------------------------------------------------------------------------------------

const EPOCH_MS: i64 = 1_800_000_000_000;

struct TestClock {
    start: Instant,
}

impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        EPOCH_MS + self.start.elapsed().as_millis() as i64
    }
}

struct Rig {
    rx: mpsc::Receiver<Event>,
    status: Arc<BinanceStatus>,
    shutdown: CancellationToken,
    handle: JoinHandle<Result<(), ExchangeError>>,
    ws: Arc<MockWs>,
}

fn config(whitelist: &[&str], interval: u64) -> BinanceConfig {
    BinanceConfig {
        symbols: whitelist.iter().map(|s| s.to_string()).collect(),
        oi_poll_interval_secs: interval,
        ws_url: "wss://mock.invalid/market/ws".into(),
        rest_url: "https://mock.invalid".into(),
    }
}

fn start_with(
    cfg: BinanceConfig,
    tuning: BinanceTuning,
    ws: Arc<MockWs>,
    http: Arc<MockHttp>,
    hook: Option<fn(&mut WsProfile)>,
) -> Rig {
    let shutdown = CancellationToken::new();
    let mut connector = BinanceConnector::with_transport(
        cfg,
        tuning,
        ws.clone(),
        http.clone(),
        Arc::new(TestClock {
            start: Instant::now(),
        }),
    );
    if let Some(h) = hook {
        connector = connector.with_profile_hook(h);
    }
    let status = connector.status();
    let ctx = AppContext::new(Config::default(), shutdown.clone());
    let (tx, rx) = mpsc::channel(100_000);
    let handle = tokio::spawn(async move { connector.run(ctx, tx).await });
    Rig {
        rx,
        status,
        shutdown,
        handle,
        ws,
    }
}

fn fast_ws_profile(p: &mut WsProfile) {
    p.jitter_seed = Some(7);
}

fn start(cfg: BinanceConfig, ws: Arc<MockWs>, http: Arc<MockHttp>) -> Rig {
    start_with(
        cfg,
        BinanceTuning::default(),
        ws,
        http,
        Some(fast_ws_profile),
    )
}

const WAIT: Duration = Duration::from_secs(3600);

impl Rig {
    async fn next(&mut self) -> Event {
        timeout(WAIT, self.rx.recv())
            .await
            .expect("no event in time")
            .expect("channel closed")
    }

    async fn next_oi(&mut self) -> OpenInterest {
        loop {
            if let Some(Payload::OpenInterest(o)) = self.next().await.payload {
                return o;
            }
        }
    }

    async fn next_liq(&mut self) -> Liquidation {
        loop {
            if let Some(Payload::Liquidation(l)) = self.next().await.payload {
                return l;
            }
        }
    }

    async fn next_gap(&mut self) -> Gap {
        loop {
            if let Some(Payload::Gap(g)) = self.next().await.payload {
                return g;
            }
        }
    }

    /// Open interest events until `n` have arrived.
    async fn oi_events(&mut self, n: usize) -> Vec<OpenInterest> {
        let mut v = Vec::new();
        while v.len() < n {
            v.push(self.next_oi().await);
        }
        v
    }

    fn count(&self, f: impl Fn(&BinanceStatus) -> &std::sync::atomic::AtomicU64) -> u64 {
        f(&self.status).load(Ordering::Relaxed)
    }

    async fn stop(self) {
        self.shutdown.cancel();
        timeout(Duration::from_secs(30), self.handle)
            .await
            .expect("connector must stop promptly")
            .unwrap()
            .unwrap();
    }
}

fn btc_eth() -> Vec<Sym> {
    vec![("BTCUSDT", "BTC", "USDT"), ("ETHUSDT", "ETH", "USDT")]
}

const KNOWN: [&str; 6] = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "PEPEUSDT",
    "1000PEPEUSDT",
    "币安人生USDT",
];

fn quiet_ws(marks: &[(&str, &str)]) -> Arc<MockWs> {
    let plan = Plan {
        marks: Some(mark_frame(marks)),
        ..Plan::default()
    };
    MockWs::new(vec![], plan_script(plan, None))
}

// ---------------------------------------------------------------------------------------------
// Liquidations
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn fixture_replay_yields_738_correct_liquidations() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/binance/force_order_sample.jsonl"
    );
    let mut frames = Vec::new();
    let mut symbols = std::collections::BTreeSet::new();
    let mut expected = Vec::new();
    let aliases = BaseAliases::builtin();
    for line in std::fs::read_to_string(path).unwrap().lines() {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        let msg = &v["msg"];
        let o = &msg["o"];
        let raw = o["s"].as_str().unwrap().to_string();
        let quote = if raw.ends_with("USDC") {
            "USDC"
        } else {
            "USDT"
        };
        let base = raw.strip_suffix(quote).unwrap().to_string();
        let r = aliases.resolve("binance", &base);
        let ap: f64 = o["ap"].as_str().unwrap().parse().unwrap();
        let z: f64 = o["z"].as_str().unwrap().parse().unwrap();
        // Order side SELL = a long was liquidated (common.proto Side comments).
        let side = if o["S"] == "SELL" {
            Side::Long
        } else {
            Side::Short
        };
        expected.push((
            raw.clone(),
            format!("{}-{quote}-PERP", r.base),
            o["T"].as_i64().unwrap(),
            side,
            ap / r.multiplier,
            z * r.multiplier,
            ap * z,
        ));
        symbols.insert((raw, base, quote.to_string()));
        frames.push(msg.to_string());
    }
    assert_eq!(frames.len(), 738);

    // exchangeInfo built from the fixture symbols (leaked: the helper wants 'static strs).
    let syms: Vec<Sym> = symbols
        .into_iter()
        .map(|(a, b, c)| {
            (
                &*Box::leak(a.into_boxed_str()),
                &*Box::leak(b.into_boxed_str()),
                &*Box::leak(c.into_boxed_str()),
            )
        })
        .collect();
    let info = info_body(&syms);
    let known: Vec<&'static str> = syms.iter().map(|s| s.0).collect();
    let http = standard_http(vec![info], known, "1.0", |r| r);

    let plan = Plan {
        marks: Some(mark_frame(&[("BTCUSDT", "100000")])),
        frames,
        close_after: false,
    };
    let net = MockWs::new(vec![], plan_script(plan, None));
    let mut rig = start(config(&[], 30), net.clone(), http);

    let mut got = Vec::new();
    while got.len() < 738 {
        got.push(rig.next_liq().await);
    }
    for (l, e) in got.iter().zip(&expected) {
        assert_eq!(l.exchange, "binance");
        assert_eq!(l.symbol_raw, e.0);
        assert_eq!(l.symbol, e.1);
        assert_eq!(l.exchange_ts, e.2);
        assert_eq!(l.side, e.3 as i32, "{}", l.symbol_raw);
        let close = |a: f64, b: f64| (a - b).abs() <= 1e-12 * b.abs().max(1.0);
        assert!(close(l.price, e.4), "price {} vs {}", l.price, e.4);
        assert!(close(l.qty, e.5), "qty {} vs {}", l.qty, e.5);
        assert!(close(l.notional_usd, e.6), "notional");
        assert!(l.recv_ts >= EPOCH_MS);
    }
    // The subscription asked for both streams on the one socket.
    assert_eq!(rig.ws.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(rig.count(|s| &s.liquidations), 738);
    assert_eq!(rig.count(|s| &s.liquidation_parse_errors), 0);
    assert_eq!(rig.count(|s| &s.gaps), 0);
    assert!(rig.count(|s| &s.mark_frames) >= 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn subscription_requests_both_streams_on_one_socket() {
    let recorded = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = recorded.clone();
    let server = script(move |mut ws, _| {
        let sink = sink.clone();
        async move {
            while let Some(Ok(m)) = ws.next().await {
                if let Message::Text(s) = m {
                    sink.lock().unwrap().push(s.to_string());
                    let _ = ws.send(Message::text(r#"{"result":null,"id":1}"#)).await;
                    loop {
                        let _ = ws
                            .send(Message::text(mark_frame(&[("BTCUSDT", "1")])))
                            .await;
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    });
    let net = MockWs::new(vec![], server);
    let http = standard_http(vec![info_body(&btc_eth())], KNOWN.to_vec(), "1", |r| r);
    let rig = start(config(&[], 30), net.clone(), http);
    sleep(Duration::from_secs(5)).await;
    let subs = recorded.lock().unwrap().clone();
    assert_eq!(subs.len(), 1, "{subs:?}");
    assert!(subs[0].contains(r#""method":"SUBSCRIBE""#));
    assert!(subs[0].contains("!forceOrder@arr"));
    assert!(subs[0].contains("!markPrice@arr@1s"));
    assert_eq!(net.attempts.load(Ordering::SeqCst), 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn side_and_filtering_rules() {
    // BTC is collected; TRADIFI-like or unknown symbols are counted and dropped; garbage is counted.
    let frames = vec![
        force_order("BTCUSDT", "SELL", "100000", "0.5", 1_000),
        force_order("BTCUSDT", "BUY", "100000", "0.5", 2_000),
        force_order("XAUUSDT", "SELL", "2000", "1", 3_000),
        r#"{"e":"forceOrder","o":{}}"#.to_string(),
        "garbage".to_string(),
        force_order("BTCUSDT", "SELL", "100000", "0", 4_000),
        force_order("BTCUSDT", "BUY", "100001", "0.5", 5_000),
    ];
    let plan = Plan {
        marks: Some(mark_frame(&[("BTCUSDT", "100000")])),
        frames,
        close_after: false,
    };
    let net = MockWs::new(vec![], plan_script(plan, None));
    let http = standard_http(
        vec![info_body(&[("BTCUSDT", "BTC", "USDT")])],
        KNOWN.to_vec(),
        "1",
        |r| r,
    );
    let mut rig = start(config(&[], 30), net, http);
    let a = rig.next_liq().await;
    let b = rig.next_liq().await;
    let c = rig.next_liq().await;
    assert_eq!((a.side, a.exchange_ts), (Side::Long as i32, 1_000));
    assert_eq!((b.side, b.exchange_ts), (Side::Short as i32, 2_000));
    assert_eq!(
        c.exchange_ts, 5_000,
        "bad frames are skipped, the connector goes on"
    );
    assert_eq!(c.price, 100_001.0);
    assert_eq!(a.notional_usd, 50_000.0);
    assert_eq!(rig.count(|s| &s.liquidations_filtered), 1);
    assert_eq!(rig.count(|s| &s.liquidation_parse_errors), 2);
    assert_eq!(rig.count(|s| &s.unknown_frames), 1);
    rig.stop().await;
}

/// Frame ticking once per second of absolute time, the same on every session (see ws_manager tests).
fn global_ticks(t0: Instant) -> Script {
    script(move |mut ws, _| async move {
        if !handshake(&mut ws, None).await {
            return;
        }
        let mut k = (Instant::now() - t0).as_secs() + 1;
        loop {
            tokio::select! {
                incoming = ws.next() => match incoming {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                    Some(Ok(_)) => {}
                },
                _ = sleep_until(t0 + Duration::from_secs(k)) => {
                    let frames = [
                        mark_frame(&[("BTCUSDT", "100000")]),
                        force_order("BTCUSDT", "SELL", "100000", "1", 1_000 + k as i64),
                    ];
                    for f in frames {
                        if ws.send(Message::text(f)).await.is_err() {
                            return;
                        }
                    }
                    k += 1;
                }
            }
        }
    })
}

fn age_100s(p: &mut WsProfile) {
    p.jitter_seed = Some(7);
    p.max_connection_age = Some(MaxAge {
        age: Duration::from_secs(100),
        jitter: Duration::ZERO,
    });
}

#[tokio::test(start_paused = true)]
async fn planned_reconnect_delivers_every_liquidation_once() {
    let net = MockWs::new(vec![], global_ticks(Instant::now()));
    let http = standard_http(vec![info_body(&btc_eth())], KNOWN.to_vec(), "1", |r| r);
    let mut rig = start_with(
        config(&[], 30),
        BinanceTuning::default(),
        net.clone(),
        http,
        Some(age_100s),
    );
    let mut times = Vec::new();
    loop {
        let l = rig.next_liq().await;
        times.push(l.exchange_ts);
        if l.exchange_ts >= 1_000 + 150 {
            break;
        }
    }
    let expected: Vec<i64> = (times[0]..=1_150).collect();
    assert_eq!(
        times, expected,
        "no loss and no duplicate across the switch"
    );
    assert_eq!(net.attempts.load(Ordering::SeqCst), 2, "one planned switch");
    let ws = rig.status.ws.get().expect("ws link");
    assert_eq!(ws.counters.planned_reconnects.load(Ordering::Relaxed), 1);
    assert!(ws.counters.duplicates.load(Ordering::Relaxed) >= 1);
    assert_eq!(ws.counters.gaps.load(Ordering::Relaxed), 0);
    assert_eq!(rig.count(|s| &s.gaps), 0);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn dropped_connection_produces_a_gap_event() {
    let first = plan_script(
        Plan {
            marks: Some(mark_frame(&[("BTCUSDT", "100000")])),
            frames: vec![force_order("BTCUSDT", "SELL", "100000", "1", 1_000)],
            close_after: true,
        },
        None,
    );
    let second = plan_script(
        Plan {
            marks: Some(mark_frame(&[("BTCUSDT", "100000")])),
            frames: vec![force_order("BTCUSDT", "BUY", "100000", "2", 2_000)],
            close_after: false,
        },
        None,
    );
    let net = MockWs::new(vec![first], second);
    let http = standard_http(vec![info_body(&btc_eth())], KNOWN.to_vec(), "1", |r| r);
    let mut rig = start(config(&[], 30), net, http);
    let before = rig.next_liq().await;
    assert_eq!(before.exchange_ts, 1_000);
    let gap = rig.next_gap().await;
    assert_eq!(gap.exchange, "binance");
    assert_eq!(gap.stream, "liquidations");
    assert!(gap.from_ts >= before.recv_ts, "{gap:?} vs {before:?}");
    assert!(gap.to_ts > gap.from_ts);
    let after = rig.next_liq().await;
    assert_eq!(after.exchange_ts, 2_000);
    assert!(gap.to_ts <= after.recv_ts);
    assert_eq!(rig.count(|s| &s.gaps), 1);
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Open interest
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn oi_uses_mark_price_when_known_and_stays_empty_otherwise() {
    // The mark price of BTC is known, ETH has none. exchangeInfo answers late so marks arrive first.
    let infos = [info_body(&btc_eth())];
    let info_calls = AtomicUsize::new(0);
    let http = MockHttp::new(move |path, idx| {
        if path.starts_with("/fapi/v1/exchangeInfo") {
            info_calls.fetch_add(1, Ordering::SeqCst);
            return Reply::ok(&infos[0]).after(Duration::from_secs(2));
        }
        let s = symbol_of(path, &KNOWN).unwrap();
        Reply::ok(&oi_body(s, "2.5", 1_792_775_000_000 + idx as i64))
    });
    let mut rig = start(config(&[], 30), quiet_ws(&[("BTCUSDT", "100000.5")]), http);
    let mut seen = std::collections::HashMap::new();
    while seen.len() < 2 {
        let o = rig.next_oi().await;
        seen.insert(o.symbol_raw.clone(), o);
    }
    let btc = &seen["BTCUSDT"];
    assert_eq!(btc.exchange, "binance");
    assert_eq!(btc.symbol, "BTC-USDT-PERP");
    assert_eq!(btc.oi_base, 2.5);
    assert_eq!(btc.mark_price, Some(100000.5));
    assert_eq!(btc.oi_usd, Some(2.5 * 100000.5));
    assert!(
        btc.exchange_ts >= 1_792_775_000_000,
        "exchange time from the response"
    );
    assert!(btc.recv_ts >= EPOCH_MS, "local receive time");
    let eth = &seen["ETHUSDT"];
    assert_eq!(eth.oi_base, 2.5);
    assert_eq!(eth.oi_usd, None);
    assert_eq!(eth.mark_price, None);
    assert!(rig.count(|s| &s.oi_without_mark) >= 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn mark_prices_are_not_published() {
    let http = standard_http(vec![info_body(&btc_eth())], KNOWN.to_vec(), "1", |r| r);
    let mut rig = start(config(&[], 30), quiet_ws(&[("BTCUSDT", "1")]), http);
    sleep(Duration::from_secs(90)).await;
    rig.rx.close();
    let mut n = 0;
    while let Some(ev) = rig.rx.recv().await {
        n += 1;
        assert!(
            matches!(ev.payload, Some(Payload::OpenInterest(_))),
            "only open interest is expected, got {ev:?}"
        );
    }
    assert!(n >= 4);
    assert!(rig.count(|s| &s.mark_frames) >= 60);
    rig.shutdown.cancel();
    let _ = rig.handle.await;
}

#[tokio::test(start_paused = true)]
async fn requests_are_spread_evenly_over_the_interval() {
    // 60 symbols, 30 s: one request every 0.5 s.
    let names: Vec<&'static str> = (0..60)
        .map(|i| &*Box::leak(format!("S{i:02}USDT").into_boxed_str()))
        .collect();
    let syms: Vec<Sym> = names
        .iter()
        .map(|n| (*n, &*Box::leak(n[..3].to_string().into_boxed_str()), "USDT"))
        .collect();
    let http = standard_http(vec![info_body(&syms)], names.clone(), "1", |r| {
        r.after(Duration::from_secs(1))
    });
    let mut rig = start(config(&[], 30), quiet_ws(&[("S00USDT", "1")]), http.clone());
    let events = rig.oi_events(120).await;
    assert_eq!(events.len(), 120);
    let reqs = http.oi_requests();
    for w in reqs.windows(2).take(110) {
        let step = (w[1].0 - w[0].0).as_secs_f64();
        assert!((step - 0.5).abs() < 0.01, "spacing {step}");
    }
    // Every symbol exactly once per cycle.
    let first_cycle: std::collections::HashSet<_> = reqs[..60].iter().map(|r| &r.1).collect();
    assert_eq!(first_cycle.len(), 60);
    assert_eq!(rig.count(|s| &s.oi_interval_ms), 30_000);
    // About 1 s latency at 0.5 s spacing: two in flight, well inside the limit of 12.
    let max = http.max_in_flight.load(Ordering::SeqCst);
    assert!((2..=12).contains(&max), "in flight {max}");
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn concurrency_is_bounded() {
    let names: Vec<&'static str> = (0..40)
        .map(|i| &*Box::leak(format!("C{i:02}USDT").into_boxed_str()))
        .collect();
    let syms: Vec<Sym> = names.iter().map(|n| usdt(n)).collect();
    // Requests take 4 s at 0.75 s spacing: nine would be in flight without the bound.
    let http = standard_http(vec![info_body(&syms)], names, "1", |r| {
        r.after(Duration::from_secs(4))
    });
    let tuning = BinanceTuning {
        oi_concurrency: 3,
        ..BinanceTuning::default()
    };
    let mut rig = start_with(
        config(&[], 30),
        tuning,
        quiet_ws(&[("C00USDT", "1")]),
        http.clone(),
        Some(fast_ws_profile),
    );
    rig.oi_events(60).await;
    assert_eq!(http.max_in_flight.load(Ordering::SeqCst), 3);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn interval_is_recomputed_when_the_symbol_count_changes() {
    // First 2 symbols (30 s as configured), after the reload 2400 symbols (2400 / 1440 min = 100 s).
    let big: Vec<Sym> = (0..2400)
        .map(|i| usdt(Box::leak(format!("B{i:04}USDT").into_boxed_str())))
        .collect();
    let infos = vec![info_body(&btc_eth()), info_body(&big)];
    let mut known: Vec<&'static str> = big.iter().map(|s| s.0).collect();
    known.extend(["BTCUSDT", "ETHUSDT"]);
    let http = standard_http(infos, known, "1", |r| r);
    let tuning = BinanceTuning {
        info_reload_interval: Duration::from_secs(120),
        ..BinanceTuning::default()
    };
    let mut rig = start_with(
        config(&[], 30),
        tuning,
        quiet_ws(&[("BTCUSDT", "1")]),
        http,
        Some(fast_ws_profile),
    );
    rig.next_oi().await;
    assert_eq!(rig.count(|s| &s.oi_interval_ms), 30_000);
    assert_eq!(rig.count(|s| &s.symbols), 2);
    sleep(Duration::from_secs(400)).await;
    assert_eq!(rig.count(|s| &s.symbols), 2400);
    assert_eq!(rig.count(|s| &s.oi_interval_ms), 100_000);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn used_weight_header_slows_the_poller_down() {
    let names = ["A1USDT", "A2USDT", "A3USDT", "A4USDT", "A5USDT", "A6USDT"];
    let syms: Vec<Sym> = names.iter().map(|n| usdt(n)).collect();
    // Normal spacing is 30 / 6 = 5 s. At weight 1920 (80% of 2400) the factor is 2.5 -> 12.5 s.
    let http = standard_http(vec![info_body(&syms)], names.to_vec(), "1", |r| {
        r.weight(1920)
    });
    let mut rig = start(config(&[], 30), quiet_ws(&[("A1USDT", "1")]), http.clone());
    rig.oi_events(5).await;
    let reqs = http.oi_requests();
    let gap = (reqs[3].0 - reqs[2].0).as_secs_f64();
    assert!((gap - 12.5).abs() < 0.05, "slowed spacing {gap}");
    assert_eq!(rig.status.used_weight_1m.load(Ordering::Relaxed), 1920);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn http_429_pauses_for_retry_after_then_retries() {
    let calls = AtomicUsize::new(0);
    let http = MockHttp::new(move |path, _| {
        if path.starts_with("/fapi/v1/exchangeInfo") {
            return Reply::ok(&info_body(&[("BTCUSDT", "BTC", "USDT")]));
        }
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::status(429, "{}").retry_after(20);
        }
        Reply::ok(&oi_body("BTCUSDT", "1", 5))
    });
    let mut rig = start(config(&[], 30), quiet_ws(&[("BTCUSDT", "1")]), http.clone());
    let o = rig.next_oi().await;
    assert_eq!(o.symbol_raw, "BTCUSDT");
    let reqs = http.oi_requests();
    let wait = (reqs[1].0 - reqs[0].0).as_secs_f64();
    assert!(
        (20.0..21.0).contains(&wait),
        "pause {wait} must honor Retry-After: 20"
    );
    assert_eq!(rig.count(|s| &s.http_429), 1);
    assert!(rig.count(|s| &s.oi_retries) >= 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn http_418_stops_all_requests_until_the_ban_ends() {
    let names = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
    let syms: Vec<Sym> = vec![
        ("BTCUSDT", "BTC", "USDT"),
        ("ETHUSDT", "ETH", "USDT"),
        ("SOLUSDT", "SOL", "USDT"),
    ];
    let calls = AtomicUsize::new(0);
    let infos = info_body(&syms);
    let http = MockHttp::new(move |path, _| {
        if path.starts_with("/fapi/v1/exchangeInfo") {
            return Reply::ok(&infos);
        }
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::status(418, "{}").retry_after(300);
        }
        let s = symbol_of(path, &names).unwrap();
        Reply::ok(&oi_body(s, "1", 5))
    });
    let mut rig = start(config(&[], 30), quiet_ws(&[("BTCUSDT", "1")]), http.clone());
    sleep(Duration::from_secs(150)).await;
    assert_eq!(http.oi_requests().len(), 1, "no request during the ban");
    assert!(rig.status.banned.load(Ordering::Relaxed));
    assert!(rig.status.paused_until_ms.load(Ordering::Relaxed) > EPOCH_MS);
    assert_eq!(rig.count(|s| &s.http_418), 1);
    // The ban ends 300 s after the first request; the poller resumes.
    let o = rig.next_oi().await;
    assert!(!o.symbol_raw.is_empty());
    let reqs = http.oi_requests();
    assert!(
        reqs[1].0 - reqs[0].0 >= Duration::from_secs(300),
        "{:?}",
        reqs[1].0 - reqs[0].0
    );
    assert!(!rig.status.banned.load(Ordering::Relaxed));
    rig.stop().await;
}

/// A 429 with `Retry-After: secs` on the first open interest request, normal answers afterwards.
fn http_with_first_429(secs: u64) -> Arc<MockHttp> {
    let calls = AtomicUsize::new(0);
    MockHttp::new(move |path, _| {
        if path.starts_with("/fapi/v1/exchangeInfo") {
            return Reply::ok(&info_body(&[("BTCUSDT", "BTC", "USDT")]));
        }
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::status(429, "{}").retry_after(secs);
        }
        Reply::ok(&oi_body("BTCUSDT", "1", 5))
    })
}

#[tokio::test(start_paused = true)]
async fn a_ban_produces_an_open_interest_gap_covering_the_pause() {
    let calls = AtomicUsize::new(0);
    let http = MockHttp::new(move |path, _| {
        if path.starts_with("/fapi/v1/exchangeInfo") {
            return Reply::ok(&info_body(&[("BTCUSDT", "BTC", "USDT")]));
        }
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::status(418, "{}").retry_after(300);
        }
        Reply::ok(&oi_body("BTCUSDT", "1", 5))
    });
    let mut rig = start(config(&[], 30), quiet_ws(&[("BTCUSDT", "1")]), http);
    // No snapshot exists before the ban, so the gap is the first event, then data resumes.
    let first = rig.next().await;
    let Some(Payload::Gap(gap)) = first.payload else {
        panic!("expected a gap first, got {first:?}");
    };
    assert_eq!(gap.exchange, "binance");
    assert_eq!(gap.stream, "open_interest");
    assert_eq!(
        gap.to_ts - gap.from_ts,
        300_000,
        "the gap covers the whole ban"
    );
    assert!(gap.from_ts >= EPOCH_MS);
    assert_eq!(rig.next_oi().await.symbol_raw, "BTCUSDT");
    assert_eq!(rig.count(|s| &s.oi_gaps), 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_429_pause_longer_than_the_interval_produces_a_gap() {
    let mut rig = start(
        config(&[], 30),
        quiet_ws(&[("BTCUSDT", "1")]),
        http_with_first_429(100),
    );
    let gap = rig.next_gap().await;
    assert_eq!(
        (gap.exchange.as_str(), gap.stream.as_str()),
        ("binance", "open_interest")
    );
    assert_eq!(gap.to_ts - gap.from_ts, 100_000);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_short_429_pause_produces_no_gap() {
    let mut rig = start(
        config(&[], 30),
        quiet_ws(&[("BTCUSDT", "1")]),
        http_with_first_429(20),
    );
    // Several cycles after the pause: only open interest events, no gap.
    for _ in 0..4 {
        let ev = rig.next().await;
        assert!(
            matches!(ev.payload, Some(Payload::OpenInterest(_))),
            "unexpected {ev:?}"
        );
    }
    assert_eq!(rig.count(|s| &s.http_429), 1);
    assert_eq!(rig.count(|s| &s.oi_gaps), 0);
    rig.stop().await;
}

/// Mark prices start only after `delay` (the WebSocket handshake itself is immediate).
fn late_marks_ws(delay: Duration, marks: &[(&str, &str)]) -> Arc<MockWs> {
    let frame = mark_frame(marks);
    MockWs::new(
        vec![],
        script(move |mut ws, _| {
            let frame = frame.clone();
            async move {
                if !handshake(&mut ws, None).await {
                    return;
                }
                sleep(delay).await;
                let mut timer = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tokio::select! {
                        incoming = ws.next() => match incoming {
                            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                            Some(Ok(_)) => {}
                        },
                        _ = timer.tick() => {
                            if ws.send(Message::text(frame.clone())).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        }),
    )
}

#[tokio::test(start_paused = true)]
async fn the_first_poll_waits_for_the_first_mark_price() {
    let http = standard_http(vec![info_body(&btc_eth())], KNOWN.to_vec(), "2", |r| r);
    let ws = late_marks_ws(
        Duration::from_secs(4),
        &[("BTCUSDT", "100"), ("ETHUSDT", "10")],
    );
    let mut rig = start(config(&[], 30), ws, http.clone());
    let first = rig.next_oi().await;
    let second = rig.next_oi().await;
    assert!(
        first.mark_price.is_some() && first.oi_usd.is_some(),
        "{first:?}"
    );
    assert!(
        second.mark_price.is_some() && second.oi_usd.is_some(),
        "{second:?}"
    );
    let t0 = http.oi_requests()[0].0;
    assert!(
        t0 >= Duration::from_secs(4) && t0 < Duration::from_secs(6),
        "first request at {t0:?}"
    );
    assert_eq!(rig.count(|s| &s.oi_without_mark), 0);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn without_any_mark_price_the_first_poll_starts_after_ten_seconds() {
    let http = standard_http(vec![info_body(&btc_eth())], KNOWN.to_vec(), "2", |r| r);
    let ws = late_marks_ws(Duration::from_secs(3600), &[("BTCUSDT", "100")]);
    let mut rig = start_with(
        config(&[], 30),
        BinanceTuning::default(),
        ws,
        http.clone(),
        None,
    );
    let first = rig.next_oi().await;
    assert_eq!((first.mark_price, first.oi_usd), (None, None));
    let t0 = http.oi_requests()[0].0;
    assert!(
        t0 >= Duration::from_secs(10) && t0 < Duration::from_secs(11),
        "fallback after 10 s, first request at {t0:?}"
    );
    assert!(rig.count(|s| &s.oi_without_mark) >= 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn one_hanging_symbol_does_not_block_the_others() {
    let syms: Vec<Sym> = vec![
        ("BTCUSDT", "BTC", "USDT"),
        ("ETHUSDT", "ETH", "USDT"),
        ("SOLUSDT", "SOL", "USDT"),
        ("PEPEUSDT", "PEPE", "USDT"),
    ];
    let infos = info_body(&syms);
    let http = MockHttp::new(move |path, _| {
        if path.starts_with("/fapi/v1/exchangeInfo") {
            return Reply::ok(&infos);
        }
        let s = symbol_of(path, &KNOWN).unwrap();
        if s == "ETHUSDT" {
            return Reply::hang();
        }
        Reply::ok(&oi_body(s, "1", 5))
    });
    let mut rig = start(config(&[], 20), quiet_ws(&[("BTCUSDT", "1")]), http.clone());
    // Two full cycles of the three healthy symbols arrive although ETH never answers.
    let events = rig.oi_events(6).await;
    assert!(events.iter().all(|e| e.symbol_raw != "ETHUSDT"));
    sleep(Duration::from_secs(60)).await;
    assert!(rig.count(|s| &s.oi_failures) >= 1);
    assert!(
        rig.count(|s| &s.oi_network_errors) >= 3,
        "timeouts are counted"
    );
    assert!(rig.count(|s| &s.oi_retries) >= 2);
    // Three attempts per cycle for the hanging symbol.
    let eth = http
        .oi_requests()
        .iter()
        .filter(|r| r.1.contains("ETH"))
        .count();
    assert!(eth >= 3);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn network_errors_and_5xx_are_retried() {
    let calls = AtomicUsize::new(0);
    let http = MockHttp::new(move |path, _| {
        if path.starts_with("/fapi/v1/exchangeInfo") {
            return Reply::ok(&info_body(&[("BTCUSDT", "BTC", "USDT")]));
        }
        match calls.fetch_add(1, Ordering::SeqCst) {
            0 => Reply::error(HttpError::Network("reset".into())),
            1 => Reply::status(503, "unavailable"),
            _ => Reply::ok(&oi_body("BTCUSDT", "1", 5)),
        }
    });
    let mut rig = start(config(&[], 30), quiet_ws(&[("BTCUSDT", "1")]), http);
    assert_eq!(rig.next_oi().await.symbol_raw, "BTCUSDT");
    assert_eq!(rig.count(|s| &s.oi_retries), 2);
    assert_eq!(rig.count(|s| &s.oi_failures), 0);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn unusable_open_interest_responses_are_counted() {
    let calls = AtomicUsize::new(0);
    let http = MockHttp::new(move |path, _| {
        if path.starts_with("/fapi/v1/exchangeInfo") {
            return Reply::ok(&info_body(&[("BTCUSDT", "BTC", "USDT")]));
        }
        match calls.fetch_add(1, Ordering::SeqCst) {
            0 => Reply::ok("not json"),
            1 => Reply::ok(&oi_body("BTCUSDT", "-5", 5)),
            2 => Reply::ok(&oi_body("OTHERUSDT", "5", 5)),
            3 => Reply::status(400, r#"{"code":-1121,"msg":"Invalid symbol."}"#),
            _ => Reply::ok(&oi_body("BTCUSDT", "5", 5)),
        }
    });
    let mut rig = start(config(&[], 4), quiet_ws(&[("BTCUSDT", "1")]), http);
    let o = rig.next_oi().await;
    assert_eq!(o.oi_base, 5.0);
    assert_eq!(rig.count(|s| &s.oi_parse_errors), 3);
    assert_eq!(rig.count(|s| &s.oi_failures), 4);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn non_ascii_symbols_are_percent_encoded() {
    let syms: Vec<Sym> = vec![("币安人生USDT", "币安人生", "USDT")];
    let http = standard_http(vec![info_body(&syms)], KNOWN.to_vec(), "72183771", |r| r);
    let mut rig = start(
        config(&[], 30),
        quiet_ws(&[("币安人生USDT", "0.5")]),
        http.clone(),
    );
    let o = rig.next_oi().await;
    assert_eq!(o.symbol_raw, "币安人生USDT");
    assert_eq!(o.symbol, "币安人生-USDT-PERP");
    assert_eq!(
        http.oi_requests()[0].1,
        "/fapi/v1/openInterest?symbol=%E5%B8%81%E5%AE%89%E4%BA%BA%E7%94%9FUSDT"
    );
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn exchange_info_reload_adds_and_removes_symbols() {
    let first: Vec<Sym> = vec![("BTCUSDT", "BTC", "USDT"), ("ETHUSDT", "ETH", "USDT")];
    let second: Vec<Sym> = vec![("BTCUSDT", "BTC", "USDT"), ("SOLUSDT", "SOL", "USDT")];
    let http = standard_http(
        vec![info_body(&first), info_body(&second)],
        KNOWN.to_vec(),
        "1",
        |r| r,
    );
    let tuning = BinanceTuning {
        info_reload_interval: Duration::from_secs(100),
        ..BinanceTuning::default()
    };
    let mut rig = start_with(
        config(&[], 10),
        tuning,
        quiet_ws(&[("BTCUSDT", "1")]),
        http.clone(),
        Some(fast_ws_profile),
    );
    let mut before = std::collections::HashSet::new();
    while before.len() < 2 {
        before.insert(rig.next_oi().await.symbol_raw);
    }
    assert!(before.contains("ETHUSDT") && !before.contains("SOLUSDT"));
    sleep(Duration::from_secs(150)).await;
    assert!(http.info_requests() >= 2);
    while rig.rx.try_recv().is_ok() {}
    let mut after = std::collections::HashSet::new();
    while after.len() < 2 {
        after.insert(rig.next_oi().await.symbol_raw);
    }
    assert!(after.contains("SOLUSDT") && after.contains("BTCUSDT"));
    // Nothing is requested for the removed symbol any more.
    sleep(Duration::from_secs(60)).await;
    let late: Vec<_> = http
        .oi_requests()
        .into_iter()
        .filter(|(t, _)| *t > Duration::from_secs(120))
        .collect();
    assert!(late.iter().all(|(_, p)| !p.contains("ETH")), "{late:?}");
    assert!(rig.count(|s| &s.info_loads) >= 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn failed_exchange_info_is_retried_and_does_not_stop_the_connector() {
    let calls = AtomicUsize::new(0);
    let http = MockHttp::new(move |path, _| {
        if path.starts_with("/fapi/v1/exchangeInfo") {
            return match calls.fetch_add(1, Ordering::SeqCst) {
                0 => Reply::status(500, "boom"),
                1 => Reply::ok("{not json"),
                _ => Reply::ok(&info_body(&[("BTCUSDT", "BTC", "USDT")])),
            };
        }
        Reply::ok(&oi_body("BTCUSDT", "1", 5))
    });
    let mut rig = start(config(&[], 30), quiet_ws(&[("BTCUSDT", "1")]), http);
    assert_eq!(rig.next_oi().await.symbol_raw, "BTCUSDT");
    assert_eq!(rig.count(|s| &s.info_errors), 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn alias_collision_is_not_merged_silently() {
    let syms: Vec<Sym> = vec![
        ("PEPEUSDT", "PEPE", "USDT"),
        ("1000PEPEUSDT", "1000PEPE", "USDT"),
    ];
    let frames = vec![
        force_order("PEPEUSDT", "SELL", "0.00001", "1000000", 1_000),
        force_order("1000PEPEUSDT", "SELL", "0.01", "1000", 2_000),
    ];
    let plan = Plan {
        marks: Some(mark_frame(&[
            ("PEPEUSDT", "0.00001"),
            ("1000PEPEUSDT", "0.01"),
        ])),
        frames,
        close_after: false,
    };
    let http = standard_http(vec![info_body(&syms)], KNOWN.to_vec(), "100", |r| r);
    let net = MockWs::new(vec![], plan_script(plan, None));
    let mut rig = start(config(&[], 30), net, http);

    let a = rig.next_liq().await;
    let b = rig.next_liq().await;
    assert_eq!(
        (a.symbol_raw.as_str(), a.symbol.as_str()),
        ("PEPEUSDT", "PEPE-USDT-PERP")
    );
    assert_eq!(
        (b.symbol_raw.as_str(), b.symbol.as_str()),
        ("1000PEPEUSDT", "1000PEPE-USDT-PERP")
    );
    assert_eq!(b.price, 0.01);
    assert_eq!(b.qty, 1000.0);
    let mut seen = std::collections::HashMap::new();
    while seen.len() < 2 {
        let o = rig.next_oi().await;
        seen.insert(o.symbol_raw.clone(), o);
    }
    assert_eq!(seen["PEPEUSDT"].symbol, "PEPE-USDT-PERP");
    assert_eq!(seen["1000PEPEUSDT"].symbol, "1000PEPE-USDT-PERP");
    assert_eq!(rig.count(|s| &s.alias_collisions), 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn aliased_symbols_without_collision_are_scaled_to_real_coins() {
    let syms: Vec<Sym> = vec![("1000PEPEUSDT", "1000PEPE", "USDT")];
    let plan = Plan {
        marks: Some(mark_frame(&[("1000PEPEUSDT", "0.004")])),
        frames: vec![force_order("1000PEPEUSDT", "BUY", "0.004", "2000", 1_000)],
        close_after: false,
    };
    let http = standard_http(vec![info_body(&syms)], KNOWN.to_vec(), "1000", |r| r);
    let mut rig = start(
        config(&[], 30),
        MockWs::new(vec![], plan_script(plan, None)),
        http,
    );
    let l = rig.next_liq().await;
    assert_eq!(l.symbol, "PEPE-USDT-PERP");
    assert!((l.price - 0.000004).abs() < 1e-15);
    assert_eq!(l.qty, 2_000_000.0);
    assert!((l.notional_usd - 8.0).abs() < 1e-9);
    let o = rig.next_oi().await;
    assert_eq!(o.symbol, "PEPE-USDT-PERP");
    assert_eq!(o.oi_base, 1_000_000.0);
    assert!((o.oi_usd.unwrap() - 4.0).abs() < 1e-9);
    assert!((o.mark_price.unwrap() - 0.000004).abs() < 1e-15);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn whitelist_limits_polling_and_liquidations() {
    let syms: Vec<Sym> = vec![
        ("BTCUSDT", "BTC", "USDT"),
        ("ETHUSDT", "ETH", "USDT"),
        ("SOLUSDT", "SOL", "USDT"),
    ];
    let plan = Plan {
        marks: Some(mark_frame(&[("BTCUSDT", "1")])),
        frames: vec![
            force_order("ETHUSDT", "SELL", "3000", "1", 1_000),
            force_order("SOLUSDT", "SELL", "100", "1", 2_000),
        ],
        close_after: false,
    };
    let http = standard_http(vec![info_body(&syms)], KNOWN.to_vec(), "1", |r| r);
    let mut rig = start(
        config(&["ETH-USDT-PERP"], 30),
        MockWs::new(vec![], plan_script(plan, None)),
        http.clone(),
    );
    assert_eq!(rig.next_liq().await.symbol_raw, "ETHUSDT");
    rig.oi_events(3).await;
    assert!(http.oi_requests().iter().all(|r| r.1.contains("ETHUSDT")));
    assert_eq!(rig.count(|s| &s.liquidations_filtered), 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_is_prompt_and_clean() {
    let http = standard_http(vec![info_body(&btc_eth())], KNOWN.to_vec(), "1", |r| {
        r.after(Duration::from_secs(3))
    });
    let rig = start(config(&[], 30), quiet_ws(&[("BTCUSDT", "1")]), http);
    sleep(Duration::from_secs(10)).await;
    // Requests are in flight; stop() asserts the connector returns Ok within 30 s.
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn closed_sink_ends_the_run_with_an_error() {
    let http = standard_http(vec![info_body(&btc_eth())], KNOWN.to_vec(), "1", |r| r);
    let rig = start(config(&[], 30), quiet_ws(&[("BTCUSDT", "1")]), http);
    let Rig {
        rx,
        handle,
        shutdown,
        ..
    } = rig;
    drop(rx);
    let result = timeout(Duration::from_secs(120), handle)
        .await
        .expect("must end")
        .unwrap();
    assert!(
        matches!(result, Err(ExchangeError::SinkClosed(_))),
        "{result:?}"
    );
    shutdown.cancel();
}

// ---------------------------------------------------------------------------------------------
// Production HTTP client against a local socket (header parsing, encoding on the wire)
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn reqwest_client_parses_headers_and_encodes_the_path() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use vulture_server::exchange::binance::ReqwestClient;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let mut seen = Vec::new();
        for i in 0..2 {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = sock.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).to_string();
            seen.push(request.lines().next().unwrap().to_string());
            let reply = if i == 0 {
                "HTTP/1.1 200 OK\r\nX-MBX-USED-WEIGHT-1M: 66\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
            } else {
                "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 17\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            };
            sock.write_all(reply.as_bytes()).await.unwrap();
        }
        seen
    });
    let client = ReqwestClient::new(&format!("http://127.0.0.1:{port}/")).unwrap();
    let path = format!(
        "/fapi/v1/openInterest?symbol={}",
        encode_query_value("币安人生USDT")
    );
    let ok = client.get(&path).await.unwrap();
    assert_eq!(
        (ok.status, ok.used_weight_1m, ok.retry_after),
        (200, Some(66), None)
    );
    assert_eq!(ok.body, "{}");
    let limited = client.get("/fapi/v1/exchangeInfo").await.unwrap();
    assert_eq!(limited.status, 429);
    assert_eq!(limited.retry_after, Some(Duration::from_secs(17)));
    let seen = server.await.unwrap();
    assert_eq!(
        seen[0],
        "GET /fapi/v1/openInterest?symbol=%E5%B8%81%E5%AE%89%E4%BA%BA%E7%94%9FUSDT HTTP/1.1"
    );
    // A closed port is a network error, not a panic.
    let dead = ReqwestClient::new("http://127.0.0.1:1").unwrap();
    assert!(matches!(dead.get("/x").await, Err(HttpError::Network(_))));
}

// ---------------------------------------------------------------------------------------------
// Opt-in live check: public, read-only, no keys. Run manually:
//   cargo test -p vulture-server --test binance -- --ignored binance_live --nocapture
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "talks to the real Binance public endpoints; run manually"]
async fn binance_live() {
    use vulture_server::exchange::binance::{DEFAULT_REST_URL, DEFAULT_WS_URL};
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cfg = BinanceConfig {
        // Two symbols keep the REST weight at a few requests per minute.
        symbols: vec!["BTC-USDT-PERP".into(), "ETH-USDT-PERP".into()],
        oi_poll_interval_secs: 30,
        ws_url: DEFAULT_WS_URL.into(),
        rest_url: DEFAULT_REST_URL.into(),
    };
    let connector = BinanceConnector::new(cfg).expect("client");
    let status = connector.status();
    let shutdown = CancellationToken::new();
    let ctx = AppContext::new(Config::default(), shutdown.clone());
    let (tx, mut rx) = mpsc::channel(10_000);
    let run = tokio::spawn(async move { connector.run(ctx, tx).await });

    let started = std::time::Instant::now();
    let mut oi = Vec::new();
    let mut liquidations = 0u64;
    let mut gaps = 0u64;
    while started.elapsed() < Duration::from_secs(75) {
        if let Ok(Some(ev)) = timeout(Duration::from_secs(1), rx.recv()).await {
            match ev.payload {
                Some(Payload::OpenInterest(o)) => oi.push(o),
                Some(Payload::Liquidation(_)) => liquidations += 1,
                Some(Payload::Gap(_)) => gaps += 1,
                None => {}
            }
        }
    }
    shutdown.cancel();
    let result = timeout(Duration::from_secs(15), run).await;
    let get = |c: &std::sync::atomic::AtomicU64| c.load(Ordering::Relaxed);
    println!("--- binance_live result (75 s) ---");
    println!(
        "exchangeInfo loads: {}, errors: {}",
        get(&status.info_loads),
        get(&status.info_errors)
    );
    println!("symbols in set: {}", get(&status.symbols));
    println!(
        "ws frames: {}, mark frames: {}, mark parse errors: {}",
        get(&status.ws_frames),
        get(&status.mark_frames),
        get(&status.mark_parse_errors)
    );
    println!(
        "liquidations: {liquidations} (parse errors {}, filtered {})",
        get(&status.liquidation_parse_errors),
        get(&status.liquidations_filtered)
    );
    println!("gaps: {gaps}");
    println!(
        "oi events: {} (requests {}, failures {}, retries {}, 429 {}, 418 {})",
        oi.len(),
        get(&status.oi_requests),
        get(&status.oi_failures),
        get(&status.oi_retries),
        get(&status.http_429),
        get(&status.http_418)
    );
    println!(
        "used weight 1m: {}",
        status.used_weight_1m.load(Ordering::Relaxed)
    );
    for o in oi.iter().take(4) {
        println!(
            "  {} oi_base={} oi_usd={:?} mark={:?} ex_ts={} recv_ts={}",
            o.symbol, o.oi_base, o.oi_usd, o.mark_price, o.exchange_ts, o.recv_ts
        );
    }
    if let Some(ws) = status.ws.get() {
        println!("ws state: {:?}", *ws.status.borrow());
    }
    println!(
        "connector stopped: {:?}",
        result.as_ref().map(|r| r.is_ok())
    );
    assert!(
        get(&status.info_loads) >= 1,
        "exchangeInfo did not load (HTTP 451 means a blocked region)"
    );
    assert!(
        get(&status.mark_frames) >= 20,
        "mark price frames did not arrive"
    );
    assert!(oi.len() >= 2, "open interest snapshots did not arrive");
    assert!(oi.iter().all(|o| o.oi_base > 0.0));
    assert_eq!(get(&status.liquidation_parse_errors), 0);
}
