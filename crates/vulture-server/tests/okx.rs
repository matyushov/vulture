//! OKX connector tests on mock transports: an in-memory WebSocket server (tokio-tungstenite over
//! `tokio::io::duplex`) and a scripted REST client. Time is paused, so schedules of minutes run instantly.
//! Two tests use real time: the pipeline test through the store and the ignored `okx_live` test, which talks
//! to the real public endpoints once, by hand.

mod common;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async, client_async};
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::{Event, Gap, Liquidation, OpenInterest, Side, event::Payload};
use vulture_server::app::AppContext;
use vulture_server::config::Config;
use vulture_server::exchange::binance::{HttpClient, HttpError, HttpResponse};
use vulture_server::exchange::okx::{OkxConfig, OkxConnector, OkxStatus, OkxTuning};
use vulture_server::exchange::{Exchange, ExchangeError};
use vulture_server::symbols::{Mode, SymbolChannel, SymbolPolicy};
use vulture_server::ws::{BoxedConnection, Clock, ConnectError, LimiterConfig, WsConnector};

// ---------------------------------------------------------------------------------------------
// Harness: WebSocket
// ---------------------------------------------------------------------------------------------

type ServerWs = WebSocketStream<DuplexStream>;

enum Connect {
    Http(u16, Option<u64>),
}

/// Knobs the test can turn while the server runs.
#[derive(Default)]
struct Switches {
    /// No automatic mark price and open interest pushes (silence on the liveness stream).
    quiet: AtomicBool,
    /// Do not answer the text `ping`.
    ignore_ping: AtomicBool,
    /// Answer a request with one bad argument by failing it as a whole (nothing subscribed). By default the
    /// exchange answers per argument: the good ones are subscribed and acknowledged.
    atomic_errors: AtomicBool,
}

#[derive(Default)]
struct ServerState {
    /// `channel:instId` subscribed in the newest session of each connection index; the liquidation channel is
    /// listed as `liquidation-orders:SWAP`.
    live: BTreeMap<usize, BTreeSet<String>>,
    /// `(connection, op, topics, id)` in arrival order.
    requests: Vec<(usize, String, Vec<String>, String)>,
    pings: usize,
    senders: BTreeMap<usize, mpsc::UnboundedSender<String>>,
}

/// `(mark price, open interest in contracts, base coins per contract)`.
type Values = (f64, f64, f64);

struct MockWs {
    queue: Mutex<VecDeque<Connect>>,
    attempts: Mutex<Vec<Instant>>,
    state: Arc<Mutex<ServerState>>,
    switches: Arc<Switches>,
    /// Topics the server refuses.
    bad: Mutex<Vec<String>>,
    values: Mutex<BTreeMap<String, Values>>,
    start: Instant,
}

const EPOCH_MS: i64 = 1_800_000_000_000;
const LIQ_TOPIC: &str = "liquidation-orders:SWAP";

impl MockWs {
    fn new(queue: Vec<Connect>) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(queue.into()),
            attempts: Mutex::new(Vec::new()),
            state: Arc::default(),
            switches: Arc::default(),
            bad: Mutex::new(Vec::new()),
            values: Mutex::new(BTreeMap::new()),
            start: Instant::now(),
        })
    }

    fn attempts(&self) -> usize {
        self.attempts.lock().unwrap().len()
    }

    fn attempt_offsets(&self) -> Vec<Duration> {
        self.attempts
            .lock()
            .unwrap()
            .iter()
            .map(|t| *t - self.start)
            .collect()
    }

    /// Topics live over all connections.
    fn live(&self) -> BTreeSet<String> {
        self.state
            .lock()
            .unwrap()
            .live
            .values()
            .flatten()
            .cloned()
            .collect()
    }

    fn live_of(&self, conn: usize) -> BTreeSet<String> {
        self.state
            .lock()
            .unwrap()
            .live
            .get(&conn)
            .cloned()
            .unwrap_or_default()
    }

    fn requests(&self) -> Vec<(usize, String, Vec<String>, String)> {
        self.state.lock().unwrap().requests.clone()
    }

    fn requests_of(&self, op: &str) -> Vec<Vec<String>> {
        self.requests()
            .into_iter()
            .filter(|r| r.1 == op && r.2 != [LIQ_TOPIC])
            .map(|r| r.2)
            .collect()
    }

    fn pings(&self) -> usize {
        self.state.lock().unwrap().pings
    }

    /// Sends a text frame on connection `conn`.
    fn push(&self, conn: usize, text: &str) {
        let tx = self.state.lock().unwrap().senders.get(&conn).cloned();
        tx.expect("no such connection")
            .send(text.to_string())
            .expect("connection gone");
    }

    fn set_values(&self, inst: &str, mark: f64, oi_contracts: f64, ct_val: f64) {
        self.values
            .lock()
            .unwrap()
            .insert(inst.to_string(), (mark, oi_contracts, ct_val));
    }

    fn refuse(&self, topic: &str) {
        self.bad.lock().unwrap().push(topic.to_string());
    }
}

impl WsConnector for MockWs {
    fn connect<'a>(
        &'a self,
        _url: &'a str,
    ) -> BoxFuture<'a, Result<BoxedConnection, ConnectError>> {
        Box::pin(async move {
            let n = {
                let mut a = self.attempts.lock().unwrap();
                a.push(Instant::now());
                a.len() - 1
            };
            let next = self.queue.lock().unwrap().pop_front();
            if let Some(Connect::Http(status, retry_after)) = next {
                return Err(ConnectError::Http {
                    status,
                    retry_after: retry_after.map(Duration::from_secs),
                });
            }
            let (client, server) = tokio::io::duplex(1 << 20);
            let ctx = ServeCtx {
                conn: n,
                state: self.state.clone(),
                switches: self.switches.clone(),
                bad: self.bad.lock().unwrap().clone(),
                values: self.values.lock().unwrap().clone(),
                start: self.start,
            };
            let (tx, rx) = mpsc::unbounded_channel();
            self.state.lock().unwrap().senders.insert(n, tx);
            tokio::spawn(async move {
                if let Ok(ws) = accept_async(server).await {
                    serve(ws, ctx, rx).await;
                }
            });
            let (ws, _) = client_async("ws://mock.invalid/", client)
                .await
                .map_err(|e| ConnectError::Other(e.to_string()))?;
            Ok(Box::new(ws) as BoxedConnection)
        })
    }
}

struct ServeCtx {
    conn: usize,
    state: Arc<Mutex<ServerState>>,
    switches: Arc<Switches>,
    bad: Vec<String>,
    values: BTreeMap<String, Values>,
    start: Instant,
}

const DEFAULT_VALUES: Values = (100.0, 1000.0, 0.01);

fn mark_frame(inst: &str, mark: f64, ts: i64) -> String {
    json!({"arg": {"channel": "mark-price", "instId": inst},
           "data": [{"instType": "SWAP", "instId": inst, "markPx": format!("{mark}"), "ts": ts.to_string()}]})
    .to_string()
}

fn oi_frame(inst: &str, oi: f64, oi_ccy: f64, oi_usd: f64, ts: i64) -> String {
    json!({"arg": {"channel": "open-interest", "instId": inst},
           "data": [{"instId": inst, "instType": "SWAP", "oi": format!("{oi}"),
                     "oiCcy": format!("{oi_ccy}"), "oiUsd": format!("{oi_usd}"), "ts": ts.to_string()}]})
    .to_string()
}

fn ack(id: &str, op: &str, arg: &Value) -> String {
    json!({"id": id, "event": op, "arg": arg, "connId": "mock"}).to_string()
}

fn error(id: &str, channel: &str, inst: &str) -> String {
    json!({"id": id, "event": "error", "code": "60018",
           "msg": format!("Wrong URL or channel:{channel},instId:{inst} doesn't exist. Please use the correct URL, channel and parameters referring to API document."),
           "connId": "mock"})
    .to_string()
}

async fn serve(mut ws: ServerWs, c: ServeCtx, mut inject: mpsc::UnboundedReceiver<String>) {
    c.state.lock().unwrap().live.insert(c.conn, BTreeSet::new());
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    let mut tick = 0u64;
    let now_ms = |start: Instant| EPOCH_MS + start.elapsed().as_millis() as i64;
    let values = |inst: &str| c.values.get(inst).copied().unwrap_or(DEFAULT_VALUES);
    loop {
        tokio::select! {
            incoming = ws.next() => {
                let text = match incoming {
                    Some(Ok(Message::Text(s))) => s.to_string(),
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                    Some(Ok(_)) => continue,
                };
                if text == "ping" {
                    c.state.lock().unwrap().pings += 1;
                    if !c.switches.ignore_ping.load(Ordering::Relaxed)
                        && ws.send(Message::text("pong")).await.is_err()
                    {
                        return;
                    }
                    continue;
                }
                let v: Value = serde_json::from_str(&text).expect("client sends JSON");
                let op = v["op"].as_str().expect("op").to_string();
                let id = v["id"].as_str().unwrap_or("").to_string();
                let args = v["args"].as_array().expect("args").clone();
                assert!(text.len() < 64 * 1024, "request of {} bytes", text.len());
                let topic_of = |a: &Value| match a["channel"].as_str().unwrap() {
                    "liquidation-orders" => {
                        assert_eq!(a["instType"], "SWAP");
                        LIQ_TOPIC.to_string()
                    }
                    ch => format!("{ch}:{}", a["instId"].as_str().expect("instId")),
                };
                let topics: Vec<String> = args.iter().map(topic_of).collect();
                let atomic = c.switches.atomic_errors.load(Ordering::Relaxed);
                let any_bad = topics.iter().any(|t| c.bad.contains(t));
                c.state.lock().unwrap().requests.push((c.conn, op.clone(), topics.clone(), id.clone()));
                let mut pushes = Vec::new();
                for (a, t) in args.iter().zip(&topics) {
                    let bad = c.bad.contains(t);
                    if op == "subscribe" && bad {
                        let (ch, inst) = t.split_once(':').unwrap();
                        if ws.send(Message::text(error(&id, ch, inst))).await.is_err() { return; }
                        continue;
                    }
                    if op == "subscribe" && atomic && any_bad {
                        continue;
                    }
                    {
                        let mut st = c.state.lock().unwrap();
                        let live = st.live.entry(c.conn).or_default();
                        if op == "subscribe" { live.insert(t.clone()); } else { live.remove(t); }
                    }
                    if ws.send(Message::text(ack(&id, &op, a))).await.is_err() { return; }
                    if op == "subscribe" && let Some((ch, inst)) = t.split_once(':') {
                        let (mark, oi, ct_val) = values(inst);
                        match ch {
                            "mark-price" => pushes.push(mark_frame(inst, mark, now_ms(c.start))),
                            "open-interest" => pushes.push(oi_frame(inst, oi, oi * ct_val, oi * ct_val * mark, now_ms(c.start))),
                            _ => {}
                        }
                    }
                }
                for p in pushes {
                    if ws.send(Message::text(p)).await.is_err() { return; }
                }
            }
            Some(text) = inject.recv() => {
                if text == "__close__" {
                    // Abrupt end: the socket is dropped without a close handshake.
                    return;
                }
                if ws.send(Message::text(text)).await.is_err() { return; }
            }
            _ = timer.tick() => {
                if c.switches.quiet.load(Ordering::Relaxed) { continue; }
                let topics: Vec<String> = c.state.lock().unwrap().live.get(&c.conn).cloned().unwrap_or_default().into_iter().collect();
                tick += 1;
                for t in topics {
                    let Some((ch, inst)) = t.split_once(':') else { continue };
                    let (mark, oi, ct_val) = values(inst);
                    let frame = match ch {
                        "mark-price" => mark_frame(inst, mark * (1.0 + tick as f64 * 1e-5), now_ms(c.start)),
                        // Open interest every third second, as OKX does when it changed.
                        "open-interest" if tick.is_multiple_of(3) => oi_frame(inst, oi, oi * ct_val, oi * ct_val * mark, now_ms(c.start)),
                        _ => continue,
                    };
                    if ws.send(Message::text(frame)).await.is_err() { return; }
                }
            }
        }
    }
}

/// `details` entries are `(side, posSide, sz, bkPx, ts)`.
fn liq_frame(inst: &str, details: &[(&str, &str, &str, &str, i64)]) -> String {
    let details: Vec<Value> = details
        .iter()
        .map(|(side, pos, sz, px, ts)| {
            json!({"bkLoss": "0", "bkPx": px, "ccy": "", "posSide": pos, "side": side,
                   "sz": sz, "ts": ts.to_string()})
        })
        .collect();
    let family = inst.trim_end_matches("-SWAP");
    json!({"arg": {"channel": "liquidation-orders", "instType": "SWAP"},
           "data": [{"details": details, "instFamily": family, "instId": inst,
                     "instType": "SWAP", "uly": family}]})
    .to_string()
}

// ---------------------------------------------------------------------------------------------
// Harness: REST
// ---------------------------------------------------------------------------------------------

#[derive(Clone)]
struct Reply(Result<HttpResponse, HttpError>);

impl Reply {
    fn status(status: u16, body: &str) -> Self {
        Reply(Ok(HttpResponse {
            status,
            retry_after: None,
            used_weight_1m: None,
            body: body.to_string(),
        }))
    }

    fn ok(body: &str) -> Self {
        Self::status(200, body)
    }
}

type Handler = dyn Fn(&str, usize) -> Reply + Send + Sync;

struct MockHttp {
    handler: Box<Handler>,
    log: Mutex<Vec<(Duration, String)>>,
    start: Instant,
}

impl MockHttp {
    fn new(handler: impl Fn(&str, usize) -> Reply + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            handler: Box::new(handler),
            log: Mutex::new(Vec::new()),
            start: Instant::now(),
        })
    }

    fn paths(&self) -> Vec<String> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .map(|e| e.1.clone())
            .collect()
    }

    fn offsets(&self) -> Vec<Duration> {
        self.log.lock().unwrap().iter().map(|e| e.0).collect()
    }
}

impl HttpClient for MockHttp {
    fn get<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<HttpResponse, HttpError>> {
        Box::pin(async move {
            let idx = {
                let mut log = self.log.lock().unwrap();
                log.push((self.start.elapsed(), path.to_string()));
                log.len() - 1
            };
            (self.handler)(path, idx).0
        })
    }
}

/// `(instId, ctType, state, settleCcy, ctVal, ctValCcy)`.
type Inst = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
);

const BTC: Inst = ("BTC-USDT-SWAP", "linear", "live", "USDT", "0.01", "BTC");
const ETH: Inst = ("ETH-USDT-SWAP", "linear", "live", "USDT", "0.1", "ETH");
const SOL: Inst = ("SOL-USDT-SWAP", "linear", "live", "USDT", "1", "SOL");
const BTC_USDC: Inst = ("BTC-USDC-SWAP", "linear", "live", "USDC", "0.0001", "BTC");
const PEPE: Inst = (
    "PEPE-USDT-SWAP",
    "linear",
    "live",
    "USDT",
    "10000000",
    "PEPE",
);

fn list(items: &[Inst]) -> String {
    let data: Vec<Value> = items
        .iter()
        .map(|(id, ct_type, state, settle, ct_val, ccy)| {
            let family = id.trim_end_matches("-SWAP");
            json!({"instType": "SWAP", "instId": id, "instFamily": family, "uly": family,
                   "ctType": ct_type, "state": state, "settleCcy": settle,
                   "ctVal": ct_val, "ctMult": "1", "ctValCcy": ccy})
        })
        .collect();
    json!({"code": "0", "msg": "", "data": data}).to_string()
}

fn one_list(items: Vec<Inst>) -> Arc<MockHttp> {
    let body = list(&items);
    MockHttp::new(move |_, _| Reply::ok(&body))
}

// ---------------------------------------------------------------------------------------------
// Harness: rig
// ---------------------------------------------------------------------------------------------

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
    status: Arc<OkxStatus>,
    shutdown: CancellationToken,
    handle: JoinHandle<Result<(), ExchangeError>>,
    symbols: Arc<SymbolChannel>,
}

fn config(whitelist: &[&str], interval: u64) -> OkxConfig {
    OkxConfig {
        symbols: whitelist.iter().map(|s| s.to_string()).collect(),
        oi_poll_interval_secs: interval,
        ws_url: "wss://mock.invalid/ws/v5/public".into(),
        rest_url: "https://mock.invalid".into(),
    }
}

fn start_with(cfg: OkxConfig, tuning: OkxTuning, ws: Arc<MockWs>, http: Arc<MockHttp>) -> Rig {
    let shutdown = CancellationToken::new();
    let connector = OkxConnector::with_transport(
        cfg,
        tuning,
        ws.clone(),
        http.clone(),
        Arc::new(TestClock {
            start: Instant::now(),
        }),
    )
    .with_profile_hook(|p| p.jitter_seed = Some(7));
    let status = connector.status();
    let symbols = connector.symbols().expect("OKX supports symbol policies");
    let ctx = AppContext::new(Config::default(), shutdown.clone());
    let (tx, rx) = mpsc::channel(100_000);
    let handle = tokio::spawn(async move { connector.run(ctx, tx).await });
    Rig {
        rx,
        status,
        shutdown,
        handle,
        symbols,
    }
}

fn start(cfg: OkxConfig, ws: Arc<MockWs>, http: Arc<MockHttp>) -> Rig {
    start_with(cfg, OkxTuning::default(), ws, http)
}

impl Rig {
    /// Everything the connector has published so far.
    fn drain(&mut self) -> Vec<Payload> {
        let mut out = Vec::new();
        while let Ok(e) = self.rx.try_recv() {
            out.push(e.payload.expect("payload"));
        }
        out
    }

    fn count(&self, f: impl Fn(&OkxStatus) -> &std::sync::atomic::AtomicU64) -> u64 {
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

fn liquidations(events: &[Payload]) -> Vec<Liquidation> {
    events
        .iter()
        .filter_map(|p| match p {
            Payload::Liquidation(l) => Some(l.clone()),
            _ => None,
        })
        .collect()
}

fn open_interests(events: &[Payload]) -> Vec<OpenInterest> {
    events
        .iter()
        .filter_map(|p| match p {
            Payload::OpenInterest(o) => Some(o.clone()),
            _ => None,
        })
        .collect()
}

fn gaps(events: &[Payload]) -> Vec<Gap> {
    events
        .iter()
        .filter_map(|p| match p {
            Payload::Gap(g) => Some(g.clone()),
            _ => None,
        })
        .collect()
}

/// The topics of the given instruments on the first connection (with the liquidation subscription).
fn topics_of(insts: &[&str]) -> BTreeSet<String> {
    let mut set = per_instrument(insts);
    set.insert(LIQ_TOPIC.to_string());
    set
}

/// The per-instrument topics only (connections after the first carry no liquidation subscription).
fn per_instrument(insts: &[&str]) -> BTreeSet<String> {
    insts
        .iter()
        .flat_map(|s| [format!("mark-price:{s}"), format!("open-interest:{s}")])
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Liquidations
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn liquidations_are_mapped_for_usdt_usdc_and_large_contracts() {
    let ws = MockWs::new(vec![]);
    let http = one_list(vec![BTC, BTC_USDC, PEPE]);
    let mut rig = start(config(&[], 30), ws.clone(), http);
    sleep(Duration::from_secs(5)).await;
    assert_eq!(
        ws.live(),
        topics_of(&["BTC-USDT-SWAP", "BTC-USDC-SWAP", "PEPE-USDT-SWAP"])
    );
    // Hedge mode long (the exchange closes it with a sell), 5 contracts of 0.01 BTC.
    ws.push(
        0,
        &liq_frame(
            "BTC-USDT-SWAP",
            &[("sell", "long", "5", "60000", EPOCH_MS + 4_000)],
        ),
    );
    // Net mode: `sell` closes a long; USDC contract of 0.0001 BTC.
    ws.push(
        0,
        &liq_frame(
            "BTC-USDC-SWAP",
            &[("buy", "net", "20000", "59000.5", EPOCH_MS + 4_100)],
        ),
    );
    // 3 contracts of 10 000 000 PEPE.
    ws.push(
        0,
        &liq_frame(
            "PEPE-USDT-SWAP",
            &[("buy", "short", "3", "0.0000091", EPOCH_MS + 4_200)],
        ),
    );
    sleep(Duration::from_secs(1)).await;
    let liqs = liquidations(&rig.drain());
    assert_eq!(liqs.len(), 3, "{liqs:?}");

    let btc = &liqs[0];
    assert_eq!(btc.exchange, "okx");
    assert_eq!(btc.symbol_raw, "BTC-USDT-SWAP");
    assert_eq!(btc.symbol, "BTC-USDT-PERP");
    assert_eq!(btc.side, Side::Long as i32);
    assert_eq!(btc.price, 60000.0, "price is the bankruptcy price bkPx");
    assert!(
        (btc.qty - 0.05).abs() < 1e-12,
        "5 contracts x 0.01 BTC: {}",
        btc.qty
    );
    assert!((btc.notional_usd - 3000.0).abs() < 1e-9);
    assert_eq!(btc.exchange_ts, EPOCH_MS + 4_000);
    assert!(btc.recv_ts >= EPOCH_MS + 5_000);

    let usdc = &liqs[1];
    assert_eq!(usdc.symbol_raw, "BTC-USDC-SWAP");
    assert_eq!(usdc.symbol, "BTC-USDC-PERP");
    assert_eq!(usdc.side, Side::Short as i32);
    assert!((usdc.qty - 2.0).abs() < 1e-9);
    assert!((usdc.notional_usd - 2.0 * 59000.5).abs() < 1e-6);

    let pepe = &liqs[2];
    assert_eq!(pepe.symbol, "PEPE-USDT-PERP");
    assert_eq!(pepe.side, Side::Short as i32);
    assert_eq!(pepe.price, 0.0000091, "no price multiplier for OKX PEPE");
    assert_eq!(pepe.qty, 30_000_000.0);
    assert!((pepe.notional_usd - 273.0).abs() < 1e-6);
    assert!((pepe.price * pepe.qty - pepe.notional_usd).abs() < 1e-9);
    assert_eq!(rig.count(|s| &s.liquidations), 3);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_side_follows_both_position_modes() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), one_list(vec![BTC]));
    sleep(Duration::from_secs(3)).await;
    let t = EPOCH_MS + 2_000;
    for (i, (side, pos)) in [
        ("sell", "long"),
        ("buy", "short"),
        ("sell", "net"),
        ("buy", "net"),
    ]
    .iter()
    .enumerate()
    {
        ws.push(
            0,
            &liq_frame("BTC-USDT-SWAP", &[(side, pos, "1", "60000", t + i as i64)]),
        );
    }
    sleep(Duration::from_secs(1)).await;
    let sides: Vec<i32> = liquidations(&rig.drain()).iter().map(|l| l.side).collect();
    let (long, short) = (Side::Long as i32, Side::Short as i32);
    assert_eq!(sides, [long, short, long, short]);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn repeated_bad_and_foreign_liquidation_records_are_not_published() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), one_list(vec![BTC, ETH]));
    sleep(Duration::from_secs(3)).await;
    let t = EPOCH_MS + 2_000;
    let f = liq_frame("BTC-USDT-SWAP", &[("sell", "net", "1", "60000", t)]);
    ws.push(0, &f);
    ws.push(0, &f);
    // One frame with a good and three bad details: only the good one counts.
    ws.push(
        0,
        &liq_frame(
            "BTC-USDT-SWAP",
            &[
                ("sell", "net", "1", "61000", t + 1),
                ("hold", "net", "1", "60000", t + 2),
                ("sell", "net", "0", "60000", t + 3),
                ("sell", "net", "1", "0", t + 4),
            ],
        ),
    );
    ws.push(
        0,
        "{\"arg\":{\"channel\":\"liquidation-orders\",\"instType\":\"SWAP\"},\"data\":\"oops\"}",
    );
    // Another swap that is not in the universe (the feed carries all of them), and a USD_UM one.
    ws.push(
        0,
        &liq_frame("DOGE-USDT-SWAP", &[("sell", "net", "1", "0.1", t + 5)]),
    );
    ws.push(
        0,
        &liq_frame("BTC-USD_UM-SWAP", &[("sell", "net", "1", "60000", t + 6)]),
    );
    sleep(Duration::from_secs(1)).await;
    let liqs = liquidations(&rig.drain());
    assert_eq!(liqs.len(), 2);
    assert_eq!(rig.count(|s| &s.liquidations_duplicate), 1);
    // Three bad details. The frame with a non-array `data` is not a data frame at all and is ignored.
    assert_eq!(rig.count(|s| &s.liquidation_parse_errors), 3);
    assert_eq!(rig.count(|s| &s.liquidations_filtered), 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn liquidations_are_filtered_by_the_tracked_set() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(
        config(&["ETH-USDT-PERP"], 30),
        ws.clone(),
        one_list(vec![BTC, ETH]),
    );
    sleep(Duration::from_secs(3)).await;
    let t = EPOCH_MS + 2_000;
    ws.push(
        0,
        &liq_frame("BTC-USDT-SWAP", &[("sell", "net", "1", "60000", t)]),
    );
    ws.push(
        0,
        &liq_frame("ETH-USDT-SWAP", &[("sell", "net", "1", "3000", t + 1)]),
    );
    sleep(Duration::from_secs(1)).await;
    let liqs = liquidations(&rig.drain());
    assert_eq!(liqs.len(), 1);
    assert_eq!(liqs[0].symbol_raw, "ETH-USDT-SWAP");
    assert_eq!(rig.count(|s| &s.liquidations_filtered), 1);
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Open interest, mark price and the sampler
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_sampler_emits_one_event_per_symbol_per_interval_spread_over_it() {
    let ws = MockWs::new(vec![]);
    // 500 BTC in 50 000 contracts of 0.01.
    ws.set_values("BTC-USDT-SWAP", 60000.0, 50_000.0, 0.01);
    ws.set_values("ETH-USDT-SWAP", 3000.0, 100_000.0, 0.1);
    ws.set_values("SOL-USDT-SWAP", 150.0, 200_000.0, 1.0);
    let mut rig = start(config(&[], 30), ws.clone(), one_list(vec![BTC, ETH, SOL]));
    let t0 = Instant::now();
    let mut seen: Vec<(Duration, OpenInterest)> = Vec::new();
    while t0.elapsed() < Duration::from_secs(100) {
        sleep(Duration::from_millis(500)).await;
        for p in rig.drain() {
            if let Payload::OpenInterest(o) = p {
                seen.push((t0.elapsed(), o));
            }
        }
    }
    // Pushes arrive every second (mark price) and every 3 s (open interest) for every instrument; none of them
    // is an event on its own.
    assert!(rig.count(|s| &s.mark_frames) > 250);
    assert!(rig.count(|s| &s.oi_frames) > 90);
    assert!(
        (9..=10).contains(&seen.len()),
        "{} events in 100 s: {:?}",
        seen.len(),
        seen.iter().map(|e| e.0).collect::<Vec<_>>()
    );
    // Spread: within one cycle the three symbols are about a third of the interval apart.
    let first_cycle: Vec<_> = seen.iter().take(3).collect();
    for w in first_cycle.windows(2) {
        let g = w[1].0 - w[0].0;
        assert!(
            g >= Duration::from_secs(9) && g <= Duration::from_secs(11),
            "slots are 10 s apart, got {g:?}"
        );
    }
    // The cycle repeats every 30 s per symbol.
    let btc: Vec<_> = seen
        .iter()
        .filter(|e| e.1.symbol_raw == "BTC-USDT-SWAP")
        .collect();
    for w in btc.windows(2) {
        let d = w[1].0 - w[0].0;
        assert!(
            d >= Duration::from_secs(29) && d <= Duration::from_secs(31),
            "{d:?}"
        );
    }
    let o = &btc[0].1;
    assert_eq!(o.exchange, "okx");
    assert_eq!(o.symbol_raw, "BTC-USDT-SWAP");
    assert_eq!(o.symbol, "BTC-USDT-PERP");
    assert!(
        (o.oi_base - 500.0).abs() < 1e-6,
        "oiCcy in coins: {}",
        o.oi_base
    );
    let mark = o
        .mark_price
        .expect("mark price from the mark-price channel");
    assert!((60000.0..60100.0).contains(&mark), "{mark}");
    let usd = o.oi_usd.expect("oi_usd");
    assert!((usd - 500.0 * 60000.0).abs() < 1.0, "exchange oiUsd: {usd}");
    assert!(o.exchange_ts >= EPOCH_MS);
    assert!(o.recv_ts >= o.exchange_ts);
    assert_eq!(rig.count(|s| &s.oi_events), seen.len() as u64);
    assert_eq!(rig.count(|s| &s.oi_unit_mismatch), 0);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn open_interest_units_agree_with_contracts_times_contract_size() {
    let ws = MockWs::new(vec![]);
    ws.set_values("PEPE-USDT-SWAP", 0.0000091, 100.0, 10_000_000.0);
    ws.set_values("BTC-USDC-SWAP", 60000.0, 1_000_000.0, 0.0001);
    let mut rig = start(config(&[], 5), ws.clone(), one_list(vec![PEPE, BTC_USDC]));
    sleep(Duration::from_secs(12)).await;
    let oi = open_interests(&rig.drain());
    let pepe = oi.iter().find(|o| o.symbol == "PEPE-USDT-PERP").unwrap();
    // 100 contracts x 10 000 000 PEPE: the coin amount is oiCcy, equal to oi x ctVal x ctMult.
    assert_eq!(pepe.oi_base, 100.0 * 10_000_000.0);
    let mark = pepe.mark_price.unwrap();
    assert!((mark - 0.0000091).abs() < 1e-8, "{mark}");
    assert!((pepe.oi_base * mark - pepe.oi_usd.unwrap()).abs() / pepe.oi_usd.unwrap() < 0.01);
    let usdc = oi.iter().find(|o| o.symbol == "BTC-USDC-PERP").unwrap();
    assert!((usdc.oi_base - 100.0).abs() < 1e-6);
    assert_eq!(rig.count(|s| &s.oi_unit_mismatch), 0);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn an_inconsistent_coin_amount_is_counted() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 5), ws.clone(), one_list(vec![BTC]));
    sleep(Duration::from_secs(3)).await;
    // oi = 1000 contracts of 0.01 BTC = 10 BTC, but oiCcy says 1000.
    ws.push(
        0,
        &oi_frame(
            "BTC-USDT-SWAP",
            1000.0,
            1000.0,
            6_000_000.0,
            EPOCH_MS + 3_000,
        ),
    );
    sleep(Duration::from_secs(8)).await;
    let oi = open_interests(&rig.drain());
    assert!(!oi.is_empty());
    assert!(rig.count(|s| &s.oi_unit_mismatch) >= 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_latest_push_of_each_channel_wins_and_a_missing_part_is_never_zero() {
    let ws = MockWs::new(vec![]);
    ws.set_values("BTC-USDT-SWAP", 60000.0, 50_000.0, 0.01);
    let mut rig = start(config(&[], 10), ws.clone(), one_list(vec![BTC]));
    sleep(Duration::from_secs(3)).await;
    // Mark price changes; open interest stays from the last push.
    ws.switches.quiet.store(true, Ordering::Relaxed);
    ws.push(0, &mark_frame("BTC-USDT-SWAP", 61000.0, EPOCH_MS + 3_000));
    sleep(Duration::from_secs(12)).await;
    // Silence is 12 s here, below the 30 s limit.
    let last = open_interests(&rig.drain()).pop().expect("a sample");
    assert_eq!(last.mark_price, Some(61000.0));
    assert!((last.oi_base - 500.0).abs() < 1e-6);
    assert!((last.oi_usd.unwrap() - 500.0 * 60000.0).abs() < 1.0);
    // New open interest: size and value together; the mark price stays.
    ws.push(
        0,
        &oi_frame(
            "BTC-USDT-SWAP",
            70_000.0,
            700.0,
            42_700_000.0,
            EPOCH_MS + 20_000,
        ),
    );
    sleep(Duration::from_secs(10)).await;
    let last = open_interests(&rig.drain()).pop().expect("a sample");
    assert!((last.oi_base - 700.0).abs() < 1e-9);
    assert_eq!(last.oi_usd, Some(42_700_000.0));
    assert_eq!(last.mark_price, Some(61000.0));
    assert_eq!(last.exchange_ts, EPOCH_MS + 20_000);
    ws.switches.quiet.store(false, Ordering::Relaxed);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_sample_without_a_mark_price_keeps_the_exchange_value() {
    let ws = MockWs::new(vec![]);
    // The mark price of BTC is refused (ETH still provides the liveness stream).
    ws.refuse("mark-price:BTC-USDT-SWAP");
    ws.set_values("BTC-USDT-SWAP", 60000.0, 50_000.0, 0.01);
    let mut rig = start(config(&[], 10), ws.clone(), one_list(vec![BTC, ETH]));
    sleep(Duration::from_secs(25)).await;
    let oi = open_interests(&rig.drain());
    let btc = oi
        .iter()
        .rfind(|o| o.symbol_raw == "BTC-USDT-SWAP")
        .expect("a BTC sample");
    assert_eq!(btc.mark_price, None);
    assert!((btc.oi_usd.unwrap() - 500.0 * 60000.0).abs() < 1.0);
    let eth = oi
        .iter()
        .rfind(|o| o.symbol_raw == "ETH-USDT-SWAP")
        .expect("an ETH sample");
    assert!(eth.mark_price.is_some());
    assert!(rig.count(|s| &s.oi_without_mark) >= 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn symbols_without_data_are_skipped() {
    let ws = MockWs::new(vec![]);
    // ETH never gets an open interest push: its open-interest topic is refused.
    ws.refuse("open-interest:ETH-USDT-SWAP");
    let mut rig = start(config(&[], 10), ws.clone(), one_list(vec![BTC, ETH]));
    sleep(Duration::from_secs(25)).await;
    let oi = open_interests(&rig.drain());
    assert!(!oi.is_empty());
    assert!(oi.iter().all(|o| o.symbol_raw == "BTC-USDT-SWAP"), "{oi:?}");
    assert!(rig.count(|s| &s.oi_skipped) >= 2);
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Keepalive, silence, reconnect
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_text_ping_every_20_seconds_and_pong_keep_the_connection() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), one_list(vec![BTC]));
    sleep(Duration::from_secs(70)).await;
    assert_eq!(ws.pings(), 3, "pings at 20, 40 and 60 s");
    assert_eq!(ws.attempts(), 1);
    assert!(gaps(&rig.drain()).is_empty());
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_missing_pong_reconnects_and_the_set_is_resubscribed() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), one_list(vec![BTC, ETH]));
    sleep(Duration::from_secs(5)).await;
    ws.switches.ignore_ping.store(true, Ordering::Relaxed);
    // First unanswered ping at 20 s, pong timeout 5 s: the session is dropped at 25 s.
    sleep(Duration::from_secs(40)).await;
    ws.switches.ignore_ping.store(false, Ordering::Relaxed);
    sleep(Duration::from_secs(10)).await;
    assert!(ws.attempts() >= 2);
    assert_eq!(
        ws.live_of(ws.attempts() - 1),
        topics_of(&["BTC-USDT-SWAP", "ETH-USDT-SWAP"]),
        "the liquidation subscription and every topic are sent again"
    );
    let g = gaps(&rig.drain());
    assert!(g.len() >= 2 && g.len().is_multiple_of(2), "{g:?}");
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn silence_on_the_mark_price_stream_reconnects_with_gaps_for_both_streams() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), one_list(vec![BTC, ETH]));
    sleep(Duration::from_secs(6)).await;
    rig.drain();
    ws.switches.quiet.store(true, Ordering::Relaxed);
    // Pings are answered, so only the liveness watchdog can notice: 30 s without a mark price.
    sleep(Duration::from_secs(25)).await;
    assert_eq!(ws.attempts(), 1, "25 s of silence is within the limit");
    sleep(Duration::from_secs(12)).await;
    ws.switches.quiet.store(false, Ordering::Relaxed);
    sleep(Duration::from_secs(10)).await;
    assert_eq!(ws.attempts(), 2, "one reconnect after 30 s of silence");
    assert_eq!(
        ws.live_of(1),
        topics_of(&["BTC-USDT-SWAP", "ETH-USDT-SWAP"])
    );
    let events = rig.drain();
    let g = gaps(&events);
    assert_eq!(g.len(), 2, "{g:?}");
    let streams: BTreeSet<_> = g.iter().map(|g| g.stream.as_str()).collect();
    assert_eq!(streams, BTreeSet::from(["liquidations", "open_interest"]));
    assert!(g.iter().all(|g| g.exchange == "okx" && g.from_ts < g.to_ts));
    assert_eq!((g[0].from_ts, g[0].to_ts), (g[1].from_ts, g[1].to_ts));
    assert_eq!(rig.count(|s| &s.gaps), 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn an_abrupt_close_reconnects_resubscribes_and_samples_resume() {
    let ws = MockWs::new(vec![]);
    ws.set_values("BTC-USDT-SWAP", 60000.0, 50_000.0, 0.01);
    let mut rig = start(config(&[], 10), ws.clone(), one_list(vec![BTC]));
    sleep(Duration::from_secs(15)).await;
    assert!(!open_interests(&rig.drain()).is_empty());
    // Drop the server side of connection 0 without a close handshake.
    ws.push(0, "__close__");
    sleep(Duration::from_secs(30)).await;
    assert!(ws.attempts() >= 2);
    let events = rig.drain();
    assert!(gaps(&events).len().is_multiple_of(2));
    assert!(!gaps(&events).is_empty());
    assert!(!open_interests(&events).is_empty(), "samples resume");
    assert_eq!(ws.live_of(ws.attempts() - 1), topics_of(&["BTC-USDT-SWAP"]));
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_handshake_403_pauses_reconnects_for_ten_minutes() {
    let ws = MockWs::new(vec![Connect::Http(403, None)]);
    let rig = start(config(&[], 30), ws.clone(), one_list(vec![BTC]));
    sleep(Duration::from_secs(700)).await;
    let t = ws.attempt_offsets();
    assert_eq!(t.len(), 2, "{t:?}");
    assert!(t[1] - t[0] >= Duration::from_secs(600), "{:?}", t[1] - t[0]);
    assert_eq!(ws.live(), topics_of(&["BTC-USDT-SWAP"]));
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Runtime tracked symbols
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn policy_changes_subscribe_and_unsubscribe_without_gaps_or_duplicates() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), one_list(vec![BTC, ETH, SOL]));
    sleep(Duration::from_secs(5)).await;
    let all = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"];
    assert_eq!(ws.live(), topics_of(&all));
    assert_eq!(
        ws.requests_of("subscribe").len(),
        1,
        "all topics in one request"
    );
    rig.drain();

    // Track only BTC.
    rig.symbols
        .set_policy(SymbolPolicy::new(Mode::Whitelist, ["BTC-USDT-PERP"]).unwrap());
    sleep(Duration::from_secs(5)).await;
    assert_eq!(ws.live(), topics_of(&["BTC-USDT-SWAP"]));
    let un = ws.requests_of("unsubscribe");
    assert_eq!(un.len(), 1);
    assert_eq!(
        un[0].iter().cloned().collect::<BTreeSet<_>>(),
        per_instrument(&["ETH-USDT-SWAP", "SOL-USDT-SWAP"])
    );
    // A late frame of an untracked symbol (unsubscribe in flight) is dropped, not published.
    let t = EPOCH_MS + 7_000;
    ws.push(
        0,
        &liq_frame("ETH-USDT-SWAP", &[("sell", "net", "1", "3000", t)]),
    );
    ws.push(
        0,
        &liq_frame("BTC-USDT-SWAP", &[("sell", "net", "1", "60000", t + 1)]),
    );
    sleep(Duration::from_secs(40)).await;
    let events = rig.drain();
    let liqs = liquidations(&events);
    assert_eq!(liqs.len(), 1);
    assert_eq!(liqs[0].symbol_raw, "BTC-USDT-SWAP");
    assert!(
        open_interests(&events)
            .iter()
            .all(|o| o.symbol_raw == "BTC-USDT-SWAP"),
        "no samples of untracked symbols"
    );
    assert!(gaps(&events).is_empty(), "untracking is not an outage");
    assert_eq!(ws.attempts(), 1, "no reconnect");

    // Track ETH and SOL again.
    rig.symbols.set_policy(SymbolPolicy::all());
    sleep(Duration::from_secs(45)).await;
    assert_eq!(ws.live(), topics_of(&all));
    let events = rig.drain();
    assert!(gaps(&events).is_empty());
    let oi = open_interests(&events);
    let symbols: BTreeSet<_> = oi.iter().map(|o| o.symbol_raw.as_str()).collect();
    assert_eq!(symbols, BTreeSet::from(all));
    assert_eq!(ws.attempts(), 1);
    assert_eq!(rig.count(|s| &s.gaps), 0);
    // Exactly one liquidation subscription, ever.
    let liq_requests = ws
        .requests()
        .into_iter()
        .filter(|r| r.2 == [LIQ_TOPIC])
        .count();
    assert_eq!(liq_requests, 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn rapid_policy_changes_collapse_into_one_application() {
    let ws = MockWs::new(vec![]);
    let rig = start(config(&[], 30), ws.clone(), one_list(vec![BTC, ETH, SOL]));
    sleep(Duration::from_secs(5)).await;
    let before = ws.requests().len();
    // Three owner edits within the settle delay of 2 s; the last one wins.
    rig.symbols
        .set_policy(SymbolPolicy::new(Mode::Whitelist, ["BTC-USDT-PERP"]).unwrap());
    sleep(Duration::from_millis(500)).await;
    rig.symbols.set_policy(
        SymbolPolicy::new(Mode::Whitelist, ["BTC-USDT-PERP", "ETH-USDT-PERP"]).unwrap(),
    );
    sleep(Duration::from_millis(500)).await;
    rig.symbols
        .set_policy(SymbolPolicy::new(Mode::Whitelist, ["ETH-USDT-PERP"]).unwrap());
    sleep(Duration::from_secs(5)).await;
    let new: Vec<_> = ws.requests().split_off(before);
    assert_eq!(new.len(), 1, "{new:?}");
    assert_eq!(new[0].1, "unsubscribe");
    assert_eq!(
        new[0].2.iter().cloned().collect::<BTreeSet<_>>(),
        per_instrument(&["BTC-USDT-SWAP", "SOL-USDT-SWAP"])
    );
    assert_eq!(ws.live(), topics_of(&["ETH-USDT-SWAP"]));
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_hourly_request_budget_postpones_changes() {
    let ws = MockWs::new(vec![]);
    let tuning = OkxTuning {
        request_budget: 4,
        request_window: Duration::from_secs(600),
        ..OkxTuning::default()
    };
    let rig = start_with(
        config(&[], 30),
        tuning,
        ws.clone(),
        one_list(vec![BTC, ETH, SOL]),
    );
    sleep(Duration::from_secs(5)).await;
    // The initial subscription took one request of the budget of four; the first change needs one, the next
    // two (an unsubscribe and a subscribe).
    let one = |s: &str| SymbolPolicy::new(Mode::Whitelist, [s]).unwrap();
    rig.symbols.set_policy(one("BTC-USDT-PERP"));
    sleep(Duration::from_secs(5)).await;
    assert_eq!(ws.live(), topics_of(&["BTC-USDT-SWAP"]));
    rig.symbols.set_policy(one("ETH-USDT-PERP"));
    sleep(Duration::from_secs(5)).await;
    assert_eq!(ws.live(), topics_of(&["ETH-USDT-SWAP"]));
    // Budget spent: the third change waits for the window (600 s after the first spending).
    let asked = Instant::now();
    rig.symbols.set_policy(one("SOL-USDT-PERP"));
    sleep(Duration::from_secs(60)).await;
    assert_eq!(ws.live(), topics_of(&["ETH-USDT-SWAP"]), "still postponed");
    assert!(rig.count(|s| &s.changes_deferred) >= 1);
    while ws.live() != topics_of(&["SOL-USDT-SWAP"]) {
        sleep(Duration::from_secs(5)).await;
        assert!(asked.elapsed() < Duration::from_secs(700), "never applied");
    }
    assert!(asked.elapsed() >= Duration::from_secs(480));
    assert_eq!(ws.attempts(), 1, "postponing never reconnects");
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_initial_subscription_is_batched_by_arguments() {
    let ws = MockWs::new(vec![]);
    let names: Vec<String> = (0..120).map(|i| format!("C{i:03}-USDT-SWAP")).collect();
    let leaked: Vec<&'static str> = names
        .iter()
        .map(|n| &*Box::leak(n.clone().into_boxed_str()))
        .collect();
    let insts: Vec<Inst> = leaked
        .iter()
        .map(|id| (*id, "linear", "live", "USDT", "1", &id[..4]))
        .collect();
    let rig = start(config(&[], 30), ws.clone(), one_list(insts));
    sleep(Duration::from_secs(10)).await;
    // 240 topics in batches of 100 arguments: 3 requests, plus the one liquidation subscription.
    let subs = ws.requests_of("subscribe");
    assert_eq!(
        subs.iter().map(Vec::len).collect::<Vec<_>>(),
        [100, 100, 40]
    );
    assert_eq!(ws.live().len(), 241);
    assert_eq!(rig.status.connections().len(), 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_configured_whitelist_is_the_initial_tracked_set() {
    let ws = MockWs::new(vec![]);
    let rig = start(
        config(&["ETH-USDT-PERP"], 30),
        ws.clone(),
        one_list(vec![BTC, ETH]),
    );
    sleep(Duration::from_secs(5)).await;
    assert_eq!(ws.live(), topics_of(&["ETH-USDT-SWAP"]));
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_set_is_resent_in_full_after_a_reconnect_following_a_runtime_change() {
    let ws = MockWs::new(vec![]);
    let rig = start(config(&[], 30), ws.clone(), one_list(vec![BTC, ETH, SOL]));
    sleep(Duration::from_secs(5)).await;
    rig.symbols.set_policy(
        SymbolPolicy::new(Mode::Whitelist, ["SOL-USDT-PERP", "ETH-USDT-PERP"]).unwrap(),
    );
    sleep(Duration::from_secs(5)).await;
    ws.switches.quiet.store(true, Ordering::Relaxed);
    sleep(Duration::from_secs(40)).await;
    ws.switches.quiet.store(false, Ordering::Relaxed);
    sleep(Duration::from_secs(5)).await;
    assert_eq!(ws.attempts(), 2);
    assert_eq!(
        ws.live_of(1),
        topics_of(&["ETH-USDT-SWAP", "SOL-USDT-SWAP"])
    );
    let first_of_second = ws
        .requests()
        .into_iter()
        .find(|r| r.0 == 1 && r.1 == "subscribe" && r.2 != [LIQ_TOPIC])
        .unwrap();
    assert_eq!(first_of_second.2.len(), 4);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_refused_topic_is_reported_and_the_others_keep_flowing() {
    for atomic in [false, true] {
        let ws = MockWs::new(vec![]);
        ws.switches.atomic_errors.store(atomic, Ordering::Relaxed);
        ws.refuse("open-interest:SOL-USDT-SWAP");
        let mut rig = start(config(&[], 10), ws.clone(), one_list(vec![BTC, ETH, SOL]));
        sleep(Duration::from_secs(25)).await;
        let mut expected = topics_of(&["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]);
        expected.remove("open-interest:SOL-USDT-SWAP");
        assert_eq!(ws.live(), expected, "atomic={atomic}");
        let rejected = rig.status.rejected_topics();
        assert_eq!(rejected.len(), 1, "atomic={atomic}: {rejected:?}");
        assert_eq!(rejected[0].0, "open-interest:SOL-USDT-SWAP");
        assert!(rejected[0].1.contains("doesn't exist"), "{rejected:?}");
        // No reconnect, no Gap; the healthy symbols still produce samples.
        assert_eq!(ws.attempts(), 1, "atomic={atomic}");
        let events = rig.drain();
        assert!(gaps(&events).is_empty());
        let symbols: BTreeSet<_> = open_interests(&events)
            .iter()
            .map(|o| o.symbol_raw.clone())
            .collect();
        assert_eq!(
            symbols,
            BTreeSet::from(["BTC-USDT-SWAP".to_string(), "ETH-USDT-SWAP".to_string()]),
            "atomic={atomic}"
        );
        let mut families = vulture_server::metrics::Families::new();
        rig.status.collect_metrics(&mut families);
        assert!(
            families
                .render()
                .contains("vulture_ws_topics_rejected_total{exchange=\"okx\"} 1"),
            "atomic={atomic}"
        );
        rig.stop().await;
    }
}

// ---------------------------------------------------------------------------------------------
// Universe: filtering, reload, throttling
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_instrument_list_is_filtered_and_normalized() {
    let items: Vec<Inst> = vec![
        BTC,
        BTC_USDC,
        PEPE,
        // Inverse, USD-margined family, not live, odd contract size: all filtered out.
        ("BTC-USD-SWAP", "inverse", "live", "BTC", "100", "USD"),
        ("ETH-USD_UM-SWAP", "linear", "live", "USD", "0.1", "ETH"),
        ("NEW-USDT-SWAP", "linear", "preopen", "USDT", "1", "NEW"),
        ("ODD-USDT-SWAP", "linear", "live", "USDT", "", "ODD"),
        ("XRP-USD1-SWAP", "linear", "live", "USD1", "10", "XRP"),
    ];
    let http = one_list(items);
    let ws = MockWs::new(vec![]);
    let rig = start(config(&[], 30), ws.clone(), http.clone());
    sleep(Duration::from_secs(5)).await;
    assert_eq!(http.paths(), ["/api/v5/public/instruments?instType=SWAP"]);
    assert_eq!(
        ws.live(),
        topics_of(&["BTC-USDT-SWAP", "BTC-USDC-SWAP", "PEPE-USDT-SWAP"])
    );
    assert_eq!(rig.count(|s| &s.symbols), 3);
    assert_eq!(rig.count(|s| &s.unusable_instruments), 1);
    let universe = rig.symbols.universe().unwrap();
    let names: BTreeSet<_> = universe.iter().cloned().collect();
    assert_eq!(
        names,
        BTreeSet::from([
            "BTC-USDT-PERP".to_string(),
            "BTC-USDC-PERP".to_string(),
            "PEPE-USDT-PERP".to_string()
        ])
    );
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_hourly_reload_adds_and_removes_topics_without_gaps() {
    let before = list(&[BTC, ETH]);
    let after = list(&[BTC, SOL]);
    let http = MockHttp::new(move |_, idx| Reply::ok(if idx == 0 { &before } else { &after }));
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), http.clone());
    sleep(Duration::from_secs(5)).await;
    assert_eq!(ws.live(), topics_of(&["BTC-USDT-SWAP", "ETH-USDT-SWAP"]));
    rig.drain();
    sleep(Duration::from_secs(3600)).await;
    assert_eq!(http.paths().len(), 2, "one reload per hour");
    let offsets = http.offsets();
    assert!(offsets[1] >= Duration::from_secs(3600) && offsets[1] < Duration::from_secs(3610));
    assert_eq!(ws.live(), topics_of(&["BTC-USDT-SWAP", "SOL-USDT-SWAP"]));
    assert_eq!(ws.attempts(), 1);
    assert!(gaps(&rig.drain()).is_empty());
    assert_eq!(rig.count(|s| &s.info_loads), 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_failed_load_is_retried_and_a_403_waits_ten_minutes() {
    let body = list(&[BTC]);
    let http = MockHttp::new(move |_, idx| match idx {
        0 => Reply::status(500, "boom"),
        1 => Reply::status(403, "forbidden"),
        2 => Reply::ok(r#"{"code":"50011","msg":"Too many requests","data":[]}"#),
        _ => Reply::ok(&body),
    });
    let ws = MockWs::new(vec![]);
    let rig = start(config(&[], 30), ws.clone(), http.clone());
    sleep(Duration::from_secs(2000)).await;
    let t = http.offsets();
    assert_eq!(t.len(), 4);
    assert!(t[1] - t[0] >= Duration::from_secs(5) && t[1] - t[0] < Duration::from_secs(10));
    assert!(
        t[2] - t[1] >= Duration::from_secs(600),
        "a 403 waits at least 10 minutes, waited {:?}",
        t[2] - t[1]
    );
    assert_eq!(rig.count(|s| &s.http_403), 1);
    assert_eq!(rig.count(|s| &s.info_errors), 3);
    assert_eq!(ws.live(), topics_of(&["BTC-USDT-SWAP"]));
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Connections: sharding and the budget
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn symbols_spill_over_to_more_connections_only_when_the_cap_requires_it() {
    let ws = MockWs::new(vec![]);
    let tuning = OkxTuning {
        max_topics_per_connection: 4,
        ..OkxTuning::default()
    };
    let rig = start_with(
        config(&[], 30),
        tuning,
        ws.clone(),
        one_list(vec![BTC, ETH, SOL]),
    );
    sleep(Duration::from_secs(5)).await;
    // Two symbols (four topics) fit one connection; the third needs a second one.
    assert_eq!(ws.attempts(), 2);
    assert_eq!(rig.status.connections().len(), 2);
    assert_eq!(
        ws.live_of(0),
        topics_of(&["BTC-USDT-SWAP", "ETH-USDT-SWAP"])
    );
    // Only the first connection listens to liquidations.
    assert_eq!(ws.live_of(1), per_instrument(&["SOL-USDT-SWAP"]));
    let liq_requests = ws
        .requests()
        .into_iter()
        .filter(|r| r.2 == [LIQ_TOPIC])
        .count();
    assert_eq!(liq_requests, 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn all_connections_obey_the_connection_budget() {
    let ws = MockWs::new(vec![]);
    let tuning = OkxTuning {
        max_topics_per_connection: 2,
        limiter: LimiterConfig {
            max_new_per_window: 2,
            window: Duration::from_secs(60),
            max_concurrent: 10,
        },
        ..OkxTuning::default()
    };
    let rig = start_with(
        config(&[], 30),
        tuning,
        ws.clone(),
        one_list(vec![BTC, ETH, SOL, BTC_USDC]),
    );
    sleep(Duration::from_secs(200)).await;
    let t = ws.attempt_offsets();
    assert_eq!(t.len(), 4, "{t:?}");
    for (i, start) in t.iter().enumerate() {
        let in_window = t
            .iter()
            .skip(i)
            .filter(|x| **x - *start < Duration::from_secs(60))
            .count();
        assert!(
            in_window <= 2,
            "{in_window} connections within a minute: {t:?}"
        );
    }
    let (granted, delayed, open) = rig.status.limiter_stats().unwrap();
    assert_eq!((granted, open), (4, 4));
    assert!(delayed >= 2);
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------------------------

fn metric(text: &str, series: &str) -> Option<f64> {
    text.lines()
        .find(|l| l.starts_with(series) && l[series.len()..].starts_with(' '))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

#[tokio::test(start_paused = true)]
async fn connector_series_are_present_and_move() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 10), ws.clone(), one_list(vec![BTC, ETH]));
    let render = |rig: &Rig| {
        let mut f = vulture_server::metrics::Families::new();
        rig.status.collect_metrics(&mut f);
        f.render()
    };
    sleep(Duration::from_secs(25)).await;
    let t = EPOCH_MS + 20_000;
    ws.push(
        0,
        &liq_frame("BTC-USDT-SWAP", &[("sell", "net", "1", "60000", t)]),
    );
    sleep(Duration::from_secs(5)).await;
    let first = render(&rig);
    for series in [
        "vulture_events_total{exchange=\"okx\",type=\"liquidation\"}",
        "vulture_events_total{exchange=\"okx\",type=\"open_interest\"}",
        "vulture_events_total{exchange=\"okx\",type=\"gap\"}",
        "vulture_parse_errors_total{exchange=\"okx\",type=\"liquidation\"}",
        "vulture_parse_errors_total{exchange=\"okx\",type=\"mark_price\"}",
        "vulture_connector_ws_frames_total{exchange=\"okx\"}",
        "vulture_connector_mark_frames_total{exchange=\"okx\"}",
        "vulture_connector_filtered_liquidations_total{exchange=\"okx\"}",
        "vulture_oi_cycles_total{exchange=\"okx\"}",
        "vulture_oi_unit_mismatch_total{exchange=\"okx\"}",
        "vulture_symbols{exchange=\"okx\"}",
        "vulture_ws_state{exchange=\"okx\",state=\"healthy\"}",
        "vulture_ws_connections{exchange=\"okx\"}",
        "vulture_ws_topics_subscribed{exchange=\"okx\"}",
        "vulture_ws_topic_requests_total{exchange=\"okx\"}",
        "vulture_ws_changes_deferred_total{exchange=\"okx\"}",
        "vulture_ws_request_budget_spent{exchange=\"okx\"}",
        "vulture_ws_connection_permits_total{exchange=\"okx\"}",
        "vulture_ws_connections_open{exchange=\"okx\"}",
        "vulture_mark_price_age_seconds{exchange=\"okx\"}",
        "vulture_exchange_info_loads_total{exchange=\"okx\",result=\"ok\"}",
    ] {
        assert!(
            metric(&first, series).is_some(),
            "missing {series}\n{first}"
        );
    }
    assert_eq!(
        metric(&first, "vulture_symbols{exchange=\"okx\"}"),
        Some(2.0)
    );
    assert_eq!(
        metric(&first, "vulture_ws_topics_subscribed{exchange=\"okx\"}"),
        Some(4.0),
        "the four instrument topics (the fixed liquidation subscription is not a topic)"
    );
    assert_eq!(
        metric(
            &first,
            "vulture_ws_state{exchange=\"okx\",state=\"healthy\"}"
        ),
        Some(1.0)
    );
    let liq1 = metric(
        &first,
        "vulture_events_total{exchange=\"okx\",type=\"liquidation\"}",
    )
    .unwrap();
    let oi1 = metric(
        &first,
        "vulture_events_total{exchange=\"okx\",type=\"open_interest\"}",
    )
    .unwrap();
    let frames1 = metric(
        &first,
        "vulture_connector_ws_frames_total{exchange=\"okx\"}",
    )
    .unwrap();
    assert_eq!(liq1, 1.0);
    assert!(oi1 >= 2.0);

    ws.push(
        0,
        &liq_frame("ETH-USDT-SWAP", &[("buy", "net", "2", "3000", t + 11_000)]),
    );
    sleep(Duration::from_secs(25)).await;
    let second = render(&rig);
    assert_eq!(
        metric(
            &second,
            "vulture_events_total{exchange=\"okx\",type=\"liquidation\"}"
        ),
        Some(2.0)
    );
    assert!(
        metric(
            &second,
            "vulture_events_total{exchange=\"okx\",type=\"open_interest\"}"
        )
        .unwrap()
            > oi1
    );
    assert!(
        metric(
            &second,
            "vulture_connector_ws_frames_total{exchange=\"okx\"}"
        )
        .unwrap()
            > frames1
    );
    assert!(metric(&second, "vulture_oi_cycles_total{exchange=\"okx\"}").unwrap() >= 2.0);
    // Low cardinality: no symbol labels anywhere.
    assert!(!second.contains("BTC-USDT"));
    rig.drain();
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Pipeline: connector -> writer -> store (memory, and Redis when VULTURE_TEST_REDIS_URL is set)
// ---------------------------------------------------------------------------------------------

async fn pipeline(backend: common::Backend) {
    use vulture_server::store::{RedisWriter, WriterConfig};
    use vulture_server::ws::SystemClock;

    let ws = MockWs::new(vec![]);
    ws.set_values("BTC-USDT-SWAP", 60000.0, 50_000.0, 0.01);
    let http = one_list(vec![BTC, PEPE]);
    let connector = OkxConnector::with_transport(
        config(&[], 1),
        OkxTuning::default(),
        ws.clone(),
        http,
        Arc::new(SystemClock),
    );
    let shutdown = CancellationToken::new();
    let ctx = AppContext::new(Config::default(), shutdown.clone());
    let (tx, rx) = mpsc::channel(1000);
    let writer = RedisWriter::new(
        backend.store(),
        WriterConfig {
            flush_interval: Duration::from_millis(50),
            ..WriterConfig::default()
        },
        Arc::new(SystemClock),
    );
    let writer_task = tokio::spawn(writer.run(rx, shutdown.clone()));
    let connector_task = tokio::spawn(async move { connector.run(ctx, tx).await });

    common::wait_until("subscriptions", || ws.live().len() == 5).await;
    ws.push(
        0,
        &liq_frame(
            "BTC-USDT-SWAP",
            &[("sell", "long", "25", "60000", 1_800_000_000_123)],
        ),
    );
    let store = backend.store();
    let mut tries = 0;
    let entries = loop {
        let e = store.read_range("-", "+", None).await.unwrap();
        let kinds = |k: &str| e.iter().filter(|x| x.kind == k).count();
        if kinds("liq") >= 1 && kinds("oi") >= 2 {
            break e;
        }
        tries += 1;
        assert!(tries < 100, "events did not reach the store");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    shutdown.cancel();
    let _ = timeout(Duration::from_secs(10), connector_task).await;
    let _ = timeout(Duration::from_secs(10), writer_task).await;
    let events: Vec<Event> = entries
        .into_iter()
        .map(|e| e.into_event().unwrap())
        .collect();
    let liqs: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.payload {
            Some(Payload::Liquidation(l)) => Some(l.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(liqs.len(), 1);
    assert_eq!(liqs[0].exchange, "okx");
    assert_eq!(liqs[0].symbol, "BTC-USDT-PERP");
    assert_eq!(liqs[0].side, Side::Long as i32);
    assert!((liqs[0].qty - 0.25).abs() < 1e-12);
    assert!((liqs[0].notional_usd - 15000.0).abs() < 1e-6);
    let ois: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.payload {
            Some(Payload::OpenInterest(o)) => Some(o.clone()),
            _ => None,
        })
        .collect();
    assert!(
        ois.iter()
            .any(|o| o.symbol == "BTC-USDT-PERP" && (o.oi_base - 500.0).abs() < 1e-6)
    );
    assert!(
        events.iter().all(|e| !e.id.is_empty()),
        "the store assigns ids"
    );
    backend.cleanup().await;
}

#[tokio::test]
async fn the_pipeline_reaches_the_in_memory_store() {
    pipeline(common::Backend::memory()).await;
}

#[tokio::test]
async fn the_pipeline_reaches_redis() {
    let Some(backend) = common::Backend::redis("okx-pipeline").await else {
        return;
    };
    let _serial = common::SERIAL.lock().await;
    pipeline(backend).await;
}

// ---------------------------------------------------------------------------------------------
// Opt-in live run (public read-only data, no keys). Run once by hand:
//   cargo test -p vulture-server --test okx okx_live -- --ignored --nocapture
// ---------------------------------------------------------------------------------------------

#[tokio::test]
#[ignore = "talks to the real OKX public endpoints; run once by hand"]
async fn okx_live() {
    live::run().await;
}

mod live {
    use super::*;
    use vulture_server::exchange::binance::ReqwestClient;
    use vulture_server::ws::{SystemClock, TungsteniteConnector};

    /// Counts REST calls: the connector's instrument list plus one raw check make two, never more.
    struct LimitedHttp {
        inner: ReqwestClient,
        calls: AtomicUsize,
        bodies: Mutex<Vec<String>>,
    }

    impl HttpClient for LimitedHttp {
        fn get<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<HttpResponse, HttpError>> {
            Box::pin(async move {
                let n = self.calls.fetch_add(1, Ordering::SeqCst);
                assert!(
                    n < 1,
                    "the connector may make one REST call in the live run"
                );
                eprintln!("LIVE REST GET {path}");
                let r = self.inner.get(path).await;
                if let Ok(resp) = &r {
                    self.bodies
                        .lock()
                        .unwrap()
                        .push(format!("HTTP {} {}", resp.status, resp.body));
                }
                r
            })
        }
    }

    /// Raw probe on a second socket: acknowledgement and error formats, partial batches, a repeated
    /// subscribe, pong, the first pushes of `open-interest` and `mark-price`, and raw liquidation frames.
    async fn probe() {
        let (mut ws, resp) = tokio_tungstenite::connect_async("wss://ws.okx.com/ws/v5/public")
            .await
            .expect("probe connect");
        eprintln!("PROBE handshake status: {}", resp.status());
        let send = |v: Value| Message::text(v.to_string());
        let t0 = std::time::Instant::now();
        ws.send(Message::text("ping")).await.unwrap();
        // One batch: two valid arguments and an invalid instrument.
        ws.send(send(json!({"id": "p1", "op": "subscribe", "args": [
            {"channel": "mark-price", "instId": "BTC-USDT-SWAP"},
            {"channel": "open-interest", "instId": "NOSUCHCOIN-USDT-SWAP"},
            {"channel": "open-interest", "instId": "BTC-USDT-SWAP"}]})))
            .await
            .unwrap();
        ws.send(send(json!({"id": "p2", "op": "subscribe", "args": [
            {"channel": "liquidation-orders", "instType": "SWAP"}]})))
            .await
            .unwrap();
        let (mut marks, mut ois, mut liqs, mut control) = (0, 0, 0, 0);
        let mut resubscribed = false;
        let mut unsubscribed = false;
        let mut first_oi_at = None;
        let mut oi_times: Vec<f64> = Vec::new();
        let mut mark_times: Vec<f64> = Vec::new();
        while t0.elapsed() < Duration::from_secs(70) {
            if !resubscribed && t0.elapsed() > Duration::from_secs(10) {
                resubscribed = true;
                eprintln!("PROBE sending a repeated subscribe for BTC-USDT-SWAP mark-price");
                ws.send(send(json!({"id": "p3", "op": "subscribe", "args": [
                    {"channel": "mark-price", "instId": "BTC-USDT-SWAP"}]})))
                    .await
                    .unwrap();
            }
            if !unsubscribed && t0.elapsed() > Duration::from_secs(20) {
                unsubscribed = true;
                eprintln!("PROBE sending unsubscribe of a channel never subscribed");
                ws.send(send(json!({"id": "p4", "op": "unsubscribe", "args": [
                    {"channel": "mark-price", "instId": "ETH-USDT-SWAP"}]})))
                    .await
                    .unwrap();
                ws.send(send(json!({"id": "p5", "op": "unsubscribe", "args": [
                    {"channel": "mark-price", "instId": "BTC-USDT-SWAP"}]})))
                    .await
                    .unwrap();
            }
            let Ok(Some(Ok(Message::Text(t)))) = timeout(Duration::from_secs(2), ws.next()).await
            else {
                continue;
            };
            let t = t.to_string();
            let at = t0.elapsed().as_secs_f64();
            if t.contains("\"data\":[") && t.contains("\"channel\":\"open-interest\"") {
                ois += 1;
                oi_times.push(at);
                if first_oi_at.is_none() {
                    first_oi_at = Some(at);
                }
                if ois <= 4 {
                    eprintln!("PROBE open-interest push at {at:.1}s: {t}");
                }
            } else if t.contains("\"data\":[") && t.contains("\"channel\":\"mark-price\"") {
                marks += 1;
                mark_times.push(at);
                if marks <= 3 {
                    eprintln!("PROBE mark-price push at {at:.1}s: {t}");
                }
            } else if t.contains("\"data\":[") && t.contains("liquidation-orders") {
                liqs += 1;
                if liqs <= 8 {
                    eprintln!("PROBE liquidation frame at {at:.1}s: {t}");
                }
            } else {
                control += 1;
                eprintln!("PROBE control frame at {at:.1}s: {t}");
            }
        }
        let gaps: Vec<f64> = oi_times.windows(2).map(|w| w[1] - w[0]).collect();
        let mgaps: Vec<f64> = mark_times.windows(2).map(|w| w[1] - w[0]).collect();
        let stats = |v: &[f64]| {
            if v.is_empty() {
                "n/a".to_string()
            } else {
                format!(
                    "min {:.2} max {:.2} mean {:.2}",
                    v.iter().cloned().fold(f64::MAX, f64::min),
                    v.iter().cloned().fold(0.0, f64::max),
                    v.iter().sum::<f64>() / v.len() as f64
                )
            }
        };
        eprintln!(
            "PROBE summary: mark pushes {marks} (gaps {}), open-interest pushes {ois} (first at {:?}, gaps {}), liquidation frames {liqs}, control frames {control}",
            stats(&mgaps),
            first_oi_at,
            stats(&gaps)
        );
        let _ = ws.close(None).await;
    }

    pub async fn run() {
        // The raw open interest check comes first so that the numbers are close in time to the pushes.
        let rest = ReqwestClient::new("https://www.okx.com").unwrap();
        let oi_path = "/api/v5/public/open-interest?instType=SWAP&instId=BTC-USDT-SWAP";
        eprintln!("LIVE REST GET {oi_path}");
        match rest.get(oi_path).await {
            Ok(r) => eprintln!("LIVE REST open-interest: HTTP {} {}", r.status, r.body),
            Err(e) => eprintln!("LIVE REST open-interest failed: {e}"),
        }
        let http = Arc::new(LimitedHttp {
            inner: ReqwestClient::new("https://www.okx.com").unwrap(),
            calls: AtomicUsize::new(0),
            bodies: Mutex::new(Vec::new()),
        });
        let shutdown = CancellationToken::new();
        let mut cfg = config(&[], 10);
        cfg.ws_url = vulture_server::exchange::okx::DEFAULT_WS_URL.into();
        cfg.rest_url = vulture_server::exchange::okx::DEFAULT_REST_URL.into();
        let connector = OkxConnector::with_transport(
            cfg,
            OkxTuning::default(),
            Arc::new(TungsteniteConnector),
            http.clone(),
            Arc::new(SystemClock),
        );
        let status = connector.status();
        let symbols = connector.symbols().unwrap();
        let ctx = AppContext::new(Config::default(), shutdown.clone());
        let (tx, mut rx) = mpsc::channel(100_000);
        let task = tokio::spawn(async move { connector.run(ctx, tx).await });

        let t0 = std::time::Instant::now();
        while symbols.universe().is_none() {
            assert!(
                t0.elapsed() < Duration::from_secs(30),
                "the instrument list did not load (403 or region block?)"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let universe: Vec<String> = symbols.universe().unwrap().iter().cloned().collect();
        eprintln!("LIVE universe: {} symbols", universe.len());
        for b in http.bodies.lock().unwrap().iter() {
            let head: String = b.chars().take(1500).collect();
            eprintln!("LIVE REST response head: {head}");
        }
        let usdc: Vec<_> = universe
            .iter()
            .filter(|s| s.ends_with("-USDC-PERP"))
            .collect();
        eprintln!(
            "LIVE USDC perpetuals: {} e.g. {:?}",
            usdc.len(),
            usdc.iter().take(8).collect::<Vec<_>>()
        );
        let probe_task = tokio::spawn(probe());

        let mut liq_events = Vec::new();
        let mut oi_events = Vec::new();
        let mut gap_events = 0;
        let t1 = std::time::Instant::now();
        while t1.elapsed() < Duration::from_secs(75) {
            if let Ok(Some(e)) = timeout(Duration::from_secs(1), rx.recv()).await {
                match e.payload.unwrap() {
                    Payload::Liquidation(l) => liq_events.push(l),
                    Payload::OpenInterest(o) => oi_events.push(o),
                    Payload::Gap(_) => gap_events += 1,
                    _ => {}
                }
            }
        }
        let mut families = vulture_server::metrics::Families::new();
        status.collect_metrics(&mut families);
        shutdown.cancel();
        let stopped = timeout(Duration::from_secs(15), task).await;
        let _ = probe_task.await;
        eprintln!(
            "LIVE connector stopped cleanly: {}",
            matches!(stopped, Ok(Ok(Ok(()))))
        );
        eprintln!(
            "LIVE events in 75 s: liquidations {}, open interest {}, gaps {gap_events}",
            liq_events.len(),
            oi_events.len()
        );
        for l in liq_events.iter().take(10) {
            eprintln!("LIVE liquidation: {l:?}");
        }
        let mut seen = BTreeSet::new();
        for o in oi_events.iter() {
            if seen.insert(o.symbol.clone()) && (seen.len() <= 5 || o.symbol == "BTC-USDT-PERP") {
                eprintln!("LIVE open interest: {o:?}");
            }
        }
        eprintln!("LIVE symbols with open interest samples: {}", seen.len());
        eprintln!("LIVE connections: {}", status.connections().len());
        eprintln!("LIVE rejected topics: {:?}", status.rejected_topics());
        eprintln!("LIVE metrics:\n{}", families.render());
        assert!(!oi_events.is_empty(), "no open interest samples in 75 s");
    }
}
