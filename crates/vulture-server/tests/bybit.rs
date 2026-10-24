//! Bybit connector tests on mock transports: an in-memory WebSocket server (tokio-tungstenite over
//! `tokio::io::duplex`) and a scripted REST client. Time is paused, so schedules of minutes run instantly.
//! Two tests use real time: the pipeline test through the store and the ignored `bybit_live` test, which talks
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
use vulture_server::exchange::bybit::{BybitConfig, BybitConnector, BybitStatus, BybitTuning};
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
    /// No automatic ticker deltas (silence on the liveness stream).
    quiet: AtomicBool,
    /// Automatic ticker deltas that carry no fields (liveness without changing the state).
    heartbeat: AtomicBool,
    /// Do not answer `{"op":"ping"}`.
    ignore_ping: AtomicBool,
}

#[derive(Default)]
struct ServerState {
    /// Topics subscribed in the newest session of each connection index.
    live: BTreeMap<usize, BTreeSet<String>>,
    /// `(connection, op, topics)` in arrival order.
    requests: Vec<(usize, String, Vec<String>)>,
    pings: usize,
    senders: BTreeMap<usize, mpsc::UnboundedSender<String>>,
}

struct MockWs {
    queue: Mutex<VecDeque<Connect>>,
    attempts: Mutex<Vec<Instant>>,
    state: Arc<Mutex<ServerState>>,
    switches: Arc<Switches>,
    /// Topics the server refuses (the request fails as a whole, naming the topic).
    bad: Mutex<Vec<String>>,
    /// `(mark, open interest)` per raw symbol for the snapshots; others get defaults.
    values: Mutex<BTreeMap<String, (f64, f64)>>,
    start: Instant,
}

const EPOCH_MS: i64 = 1_800_000_000_000;

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

    fn requests(&self) -> Vec<(usize, String, Vec<String>)> {
        self.state.lock().unwrap().requests.clone()
    }

    fn requests_of(&self, op: &str) -> Vec<Vec<String>> {
        self.requests()
            .into_iter()
            .filter(|r| r.1 == op)
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

    fn set_values(&self, symbol: &str, mark: f64, oi: f64) {
        self.values
            .lock()
            .unwrap()
            .insert(symbol.to_string(), (mark, oi));
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
    values: BTreeMap<String, (f64, f64)>,
    start: Instant,
}

fn snapshot_frame(symbol: &str, mark: f64, oi: f64, ts: i64) -> String {
    json!({
        "topic": format!("tickers.{symbol}"), "type": "snapshot", "cs": 1, "ts": ts,
        "data": {
            "symbol": symbol, "lastPrice": format!("{mark}"), "markPrice": format!("{mark}"),
            "indexPrice": format!("{mark}"), "openInterest": format!("{oi}"),
            "openInterestValue": format!("{:.2}", oi * mark), "fundingRate": "0.0001",
            "preOpenPrice": "", "preQty": "", "curPreListingPhase": ""
        }
    })
    .to_string()
}

fn delta_frame(symbol: &str, fields: Value, ts: i64) -> String {
    let mut data = json!({"symbol": symbol});
    for (k, v) in fields.as_object().unwrap() {
        data[k] = v.clone();
    }
    json!({"topic": format!("tickers.{symbol}"), "type": "delta", "cs": 2, "ts": ts, "data": data})
        .to_string()
}

async fn serve(mut ws: ServerWs, c: ServeCtx, mut inject: mpsc::UnboundedReceiver<String>) {
    c.state.lock().unwrap().live.insert(c.conn, BTreeSet::new());
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    let mut tick = 0u64;
    let now_ms = |start: Instant| EPOCH_MS + start.elapsed().as_millis() as i64;
    loop {
        tokio::select! {
            incoming = ws.next() => {
                let text = match incoming {
                    Some(Ok(Message::Text(s))) => s.to_string(),
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                    Some(Ok(_)) => continue,
                };
                let v: Value = serde_json::from_str(&text).expect("client sends JSON");
                let op = v["op"].as_str().unwrap_or("").to_string();
                if op == "ping" {
                    c.state.lock().unwrap().pings += 1;
                    if !c.switches.ignore_ping.load(Ordering::Relaxed) {
                        let pong = json!({"success": true, "ret_msg": "pong", "conn_id": "mock", "op": "ping"});
                        if ws.send(Message::text(pong.to_string())).await.is_err() { return; }
                    }
                    continue;
                }
                let id = v["req_id"].as_str().unwrap_or("").to_string();
                let topics: Vec<String> = v["args"].as_array().expect("args").iter()
                    .map(|t| t.as_str().unwrap().to_string()).collect();
                let args_chars: usize = topics.iter().map(|t| t.len() + 3).sum();
                assert!(args_chars <= 21_000, "args of {args_chars} characters");
                let offender = topics.iter().find(|t| c.bad.contains(t)).cloned();
                {
                    let mut st = c.state.lock().unwrap();
                    st.requests.push((c.conn, op.clone(), topics.clone()));
                    let live = st.live.entry(c.conn).or_default();
                    match (op.as_str(), &offender) {
                        ("subscribe", None) => live.extend(topics.iter().cloned()),
                        ("unsubscribe", _) => for t in &topics { live.remove(t); },
                        _ => {}
                    }
                }
                let reply = match (&offender, op.as_str()) {
                    (Some(t), "subscribe") => json!({"success": false, "ret_msg": format!("error:handler not found,topic:{t}"), "conn_id": "mock", "req_id": id, "op": op}),
                    _ => json!({"success": true, "ret_msg": "", "conn_id": "mock", "req_id": id, "op": op}),
                };
                if ws.send(Message::text(reply.to_string())).await.is_err() { return; }
                if op == "subscribe" && offender.is_none() {
                    for t in &topics {
                        if let Some(sym) = t.strip_prefix("tickers.") {
                            let (mark, oi) = c.values.get(sym).copied().unwrap_or((100.0, 1000.0));
                            if ws.send(Message::text(snapshot_frame(sym, mark, oi, now_ms(c.start)))).await.is_err() { return; }
                        }
                    }
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
                let symbols: Vec<String> = c.state.lock().unwrap().live.get(&c.conn).cloned().unwrap_or_default()
                    .iter().filter_map(|t| t.strip_prefix("tickers.").map(str::to_owned)).collect();
                tick += 1;
                for sym in symbols {
                    let (mark, _) = c.values.get(&sym).copied().unwrap_or((100.0, 1000.0));
                    let f = if c.switches.heartbeat.load(Ordering::Relaxed) {
                        json!({})
                    } else {
                        json!({"markPrice": format!("{}", mark * (1.0 + tick as f64 * 1e-5))})
                    };
                    if ws.send(Message::text(delta_frame(&sym, f, now_ms(c.start)))).await.is_err() { return; }
                }
            }
        }
    }
}

fn liq_frame(symbol: &str, side: &str, v: &str, p: &str, t: i64) -> String {
    json!({
        "topic": format!("allLiquidation.{symbol}"), "type": "snapshot", "ts": t + 5,
        "data": [{"T": t, "s": symbol, "S": side, "v": v, "p": p}]
    })
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

/// `(raw, contractType, status, base, quote)`.
type Inst = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
);

const BTC: Inst = ("BTCUSDT", "LinearPerpetual", "Trading", "BTC", "USDT");
const ETH: Inst = ("ETHUSDT", "LinearPerpetual", "Trading", "ETH", "USDT");
const SOL: Inst = ("SOLUSDT", "LinearPerpetual", "Trading", "SOL", "USDT");
const BTC_USDC: Inst = ("BTCPERP", "LinearPerpetual", "Trading", "BTC", "USDC");
const PEPE: Inst = (
    "1000PEPEUSDT",
    "LinearPerpetual",
    "Trading",
    "1000PEPE",
    "USDT",
);

fn page(items: &[Inst], cursor: &str) -> String {
    let list: Vec<Value> = items
        .iter()
        .map(|(symbol, contract, status, base, quote)| {
            json!({"symbol": symbol, "contractType": contract, "status": status,
                   "baseCoin": base, "quoteCoin": quote, "settleCoin": quote})
        })
        .collect();
    json!({"retCode": 0, "retMsg": "OK",
           "result": {"category": "linear", "list": list, "nextPageCursor": cursor}})
    .to_string()
}

/// One page, always.
fn one_page(items: Vec<Inst>) -> Arc<MockHttp> {
    let body = page(&items, "");
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
    status: Arc<BybitStatus>,
    shutdown: CancellationToken,
    handle: JoinHandle<Result<(), ExchangeError>>,
    symbols: Arc<SymbolChannel>,
}

fn config(whitelist: &[&str], interval: u64) -> BybitConfig {
    BybitConfig {
        symbols: whitelist.iter().map(|s| s.to_string()).collect(),
        oi_poll_interval_secs: interval,
        ws_url: "wss://mock.invalid/v5/public/linear".into(),
        rest_url: "https://mock.invalid".into(),
    }
}

fn start_with(cfg: BybitConfig, tuning: BybitTuning, ws: Arc<MockWs>, http: Arc<MockHttp>) -> Rig {
    let shutdown = CancellationToken::new();
    let connector = BybitConnector::with_transport(
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
    let symbols = connector.symbols().expect("Bybit supports symbol policies");
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

fn start(cfg: BybitConfig, ws: Arc<MockWs>, http: Arc<MockHttp>) -> Rig {
    start_with(cfg, BybitTuning::default(), ws, http)
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

    fn count(&self, f: impl Fn(&BybitStatus) -> &std::sync::atomic::AtomicU64) -> u64 {
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

fn topics_of(symbols: &[&str]) -> BTreeSet<String> {
    symbols
        .iter()
        .flat_map(|s| [format!("allLiquidation.{s}"), format!("tickers.{s}")])
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Liquidations
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn liquidations_are_mapped_for_usdt_usdc_and_prefixed_symbols() {
    let ws = MockWs::new(vec![]);
    let http = one_page(vec![BTC, BTC_USDC, PEPE]);
    let mut rig = start(config(&[], 30), ws.clone(), http);
    sleep(Duration::from_secs(5)).await;
    assert_eq!(
        ws.live(),
        topics_of(&["BTCUSDT", "BTCPERP", "1000PEPEUSDT"])
    );
    ws.push(
        0,
        &liq_frame("BTCUSDT", "Buy", "0.5", "60000", EPOCH_MS + 4_000),
    );
    ws.push(
        0,
        &liq_frame("BTCPERP", "Sell", "2", "59000.5", EPOCH_MS + 4_100),
    );
    ws.push(
        0,
        &liq_frame("1000PEPEUSDT", "Buy", "16009", "0.0091", EPOCH_MS + 4_200),
    );
    sleep(Duration::from_secs(1)).await;
    let liqs = liquidations(&rig.drain());
    assert_eq!(liqs.len(), 3, "{liqs:?}");

    let btc = &liqs[0];
    assert_eq!(btc.exchange, "bybit");
    assert_eq!(btc.symbol_raw, "BTCUSDT");
    assert_eq!(btc.symbol, "BTC-USDT-PERP");
    // `Buy` means a long position was liquidated.
    assert_eq!(btc.side, Side::Long as i32);
    assert_eq!(btc.price, 60000.0, "price is the bankruptcy price p");
    assert_eq!(btc.qty, 0.5);
    assert_eq!(btc.notional_usd, 30000.0);
    assert_eq!(btc.exchange_ts, EPOCH_MS + 4_000);
    assert!(btc.recv_ts >= EPOCH_MS + 5_000);

    let usdc = &liqs[1];
    assert_eq!(usdc.symbol_raw, "BTCPERP");
    assert_eq!(usdc.symbol, "BTC-USDC-PERP");
    assert_eq!(usdc.side, Side::Short as i32);
    assert_eq!(usdc.notional_usd, 2.0 * 59000.5);

    let pepe = &liqs[2];
    assert_eq!(pepe.symbol, "PEPE-USDT-PERP");
    assert!((pepe.price - 0.0091 / 1000.0).abs() < 1e-15);
    assert_eq!(pepe.qty, 16_009_000.0);
    assert!((pepe.notional_usd - 0.0091 * 16009.0).abs() < 1e-9);
    assert!((pepe.price * pepe.qty - pepe.notional_usd).abs() < 1e-6);
    assert_eq!(rig.count(|s| &s.liquidations), 3);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn repeated_and_bad_liquidation_records_are_not_published() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), one_page(vec![BTC, ETH]));
    sleep(Duration::from_secs(3)).await;
    let f = liq_frame("BTCUSDT", "Sell", "1", "60000", EPOCH_MS + 2_000);
    ws.push(0, &f);
    ws.push(0, &f);
    ws.push(
        0,
        &liq_frame("BTCUSDT", "Hold", "1", "60000", EPOCH_MS + 2_001),
    );
    ws.push(
        0,
        &liq_frame("BTCUSDT", "Sell", "0", "60000", EPOCH_MS + 2_002),
    );
    ws.push(
        0,
        "{\"topic\":\"allLiquidation.BTCUSDT\",\"data\":\"oops\"}",
    );
    // Not in the universe at all.
    ws.push(
        0,
        &liq_frame("DOGEUSDT", "Sell", "1", "0.1", EPOCH_MS + 2_003),
    );
    sleep(Duration::from_secs(1)).await;
    let liqs = liquidations(&rig.drain());
    assert_eq!(liqs.len(), 1);
    assert_eq!(rig.count(|s| &s.liquidations_duplicate), 1);
    assert_eq!(rig.count(|s| &s.liquidation_parse_errors), 3);
    assert_eq!(rig.count(|s| &s.liquidations_filtered), 1);
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Tickers and the sampler
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_sampler_emits_one_event_per_symbol_per_interval_spread_over_it() {
    let ws = MockWs::new(vec![]);
    ws.set_values("BTCUSDT", 60000.0, 500.0);
    ws.set_values("ETHUSDT", 3000.0, 10_000.0);
    ws.set_values("SOLUSDT", 150.0, 200_000.0);
    let mut rig = start(config(&[], 30), ws.clone(), one_page(vec![BTC, ETH, SOL]));
    // Collect OI events with their time of arrival for three cycles.
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
    // Ticker deltas arrive every second for every symbol; none of them is an event on its own.
    assert!(rig.count(|s| &s.ticker_frames) > 250);
    assert!(
        (9..=10).contains(&seen.len()),
        "{} events in 100 s: {:?}",
        seen.len(),
        seen.iter().map(|e| e.0).collect::<Vec<_>>()
    );
    // Spread: within one cycle the three symbols are about a third of the interval apart.
    let first_cycle: Vec<_> = seen.iter().take(3).collect();
    let gap1 = first_cycle[1].0 - first_cycle[0].0;
    let gap2 = first_cycle[2].0 - first_cycle[1].0;
    for g in [gap1, gap2] {
        assert!(
            g >= Duration::from_secs(9) && g <= Duration::from_secs(11),
            "slots are 10 s apart, got {g:?}"
        );
    }
    // The cycle repeats every 30 s per symbol.
    let btc: Vec<_> = seen
        .iter()
        .filter(|e| e.1.symbol_raw == "BTCUSDT")
        .collect();
    for w in btc.windows(2) {
        let d = w[1].0 - w[0].0;
        assert!(
            d >= Duration::from_secs(29) && d <= Duration::from_secs(31),
            "{d:?}"
        );
    }
    // Values: exchange size and value, last mark seen (snapshot + 1 s deltas), per real coin.
    let o = &btc[0].1;
    assert_eq!(o.exchange, "bybit");
    assert_eq!(o.symbol, "BTC-USDT-PERP");
    assert_eq!(o.oi_base, 500.0);
    let mark = o.mark_price.expect("mark price from the ticker state");
    assert!((60000.0..60100.0).contains(&mark), "{mark}");
    let usd = o.oi_usd.expect("oi_usd");
    assert!(
        (usd - 500.0 * 60000.0).abs() < 1.0,
        "exchange value from the snapshot: {usd}"
    );
    assert!(o.exchange_ts >= EPOCH_MS);
    assert!(o.recv_ts >= o.exchange_ts);
    assert_eq!(rig.count(|s| &s.oi_events), seen.len() as u64);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn deltas_update_the_state_and_missing_fields_keep_their_values() {
    let ws = MockWs::new(vec![]);
    ws.set_values("BTCUSDT", 60000.0, 500.0);
    ws.switches.heartbeat.store(true, Ordering::Relaxed);
    let mut rig = start(config(&[], 10), ws.clone(), one_page(vec![BTC]));
    sleep(Duration::from_secs(3)).await;
    // Mark price only: OI stays from the snapshot.
    ws.push(
        0,
        &delta_frame("BTCUSDT", json!({"markPrice": "61000"}), EPOCH_MS + 3_000),
    );
    // Empty delta: nothing changes, never a zero.
    ws.push(0, &delta_frame("BTCUSDT", json!({}), EPOCH_MS + 3_100));
    // The next sample comes at the next cycle start.
    sleep(Duration::from_secs(12)).await;
    let oi = open_interests(&rig.drain());
    let last = oi.last().expect("a sample");
    assert_eq!(last.oi_base, 500.0);
    assert_eq!(last.mark_price, Some(61000.0));
    // Value is still the snapshot's (the delta carried no new size).
    assert!((last.oi_usd.unwrap() - 500.0 * 60000.0).abs() < 1.0);
    // New size and value together.
    ws.push(
        0,
        &delta_frame(
            "BTCUSDT",
            json!({"openInterest": "700", "openInterestValue": "42700000"}),
            EPOCH_MS + 20_000,
        ),
    );
    sleep(Duration::from_secs(10)).await;
    let oi = open_interests(&rig.drain());
    let last = oi.last().expect("a sample");
    assert_eq!(last.oi_base, 700.0);
    assert_eq!(last.oi_usd, Some(42_700_000.0));
    assert_eq!(last.mark_price, Some(61000.0));
    // A new size without a value: the old value is not mixed in, the sampler falls back to size x mark.
    ws.push(
        0,
        &delta_frame("BTCUSDT", json!({"openInterest": "800"}), EPOCH_MS + 31_000),
    );
    sleep(Duration::from_secs(10)).await;
    let oi = open_interests(&rig.drain());
    let last = oi.last().expect("a sample");
    assert_eq!(last.oi_base, 800.0);
    assert_eq!(last.oi_usd, Some(800.0 * 61000.0));
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn prefixed_symbols_report_open_interest_per_real_coin() {
    let ws = MockWs::new(vec![]);
    ws.set_values("1000PEPEUSDT", 0.0091, 1_000_000.0);
    let mut rig = start(config(&[], 5), ws.clone(), one_page(vec![PEPE]));
    sleep(Duration::from_secs(12)).await;
    let oi = open_interests(&rig.drain());
    let o = oi.last().expect("a sample");
    assert_eq!(o.symbol, "PEPE-USDT-PERP");
    assert_eq!(o.oi_base, 1_000_000_000.0);
    let mark = o.mark_price.unwrap();
    assert!(mark > 0.0091 / 1000.0 && mark < 0.0092 / 1000.0, "{mark}");
    assert!((o.oi_base * mark - o.oi_usd.unwrap()).abs() / o.oi_usd.unwrap() < 0.01);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn symbols_without_data_are_skipped() {
    let ws = MockWs::new(vec![]);
    ws.switches.heartbeat.store(true, Ordering::Relaxed);
    let mut rig = start(config(&[], 10), ws.clone(), one_page(vec![BTC, ETH]));
    sleep(Duration::from_secs(3)).await;
    // Bare snapshot for ETH: mark only, no open interest.
    ws.push(
        0,
        &json!({"topic": "tickers.ETHUSDT", "type": "snapshot", "ts": EPOCH_MS,
                "data": {"symbol": "ETHUSDT", "markPrice": "3000"}})
        .to_string(),
    );
    sleep(Duration::from_secs(25)).await;
    let oi = open_interests(&rig.drain());
    assert!(oi.iter().all(|o| o.symbol_raw == "BTCUSDT"), "{oi:?}");
    assert!(rig.count(|s| &s.oi_skipped) >= 2);
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Keepalive, silence, reconnect
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn ping_every_20_seconds_and_pong_keep_the_connection() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), one_page(vec![BTC]));
    sleep(Duration::from_secs(70)).await;
    assert_eq!(ws.pings(), 3, "pings at 20, 40 and 60 s");
    assert_eq!(ws.attempts(), 1);
    assert!(gaps(&rig.drain()).is_empty());
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_missing_pong_reconnects_and_the_set_is_resubscribed() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), one_page(vec![BTC, ETH]));
    sleep(Duration::from_secs(5)).await;
    ws.switches.ignore_ping.store(true, Ordering::Relaxed);
    // First unanswered ping at 20 s, pong timeout 10 s: the session is dropped at 30 s.
    sleep(Duration::from_secs(40)).await;
    ws.switches.ignore_ping.store(false, Ordering::Relaxed);
    sleep(Duration::from_secs(10)).await;
    assert!(ws.attempts() >= 2);
    assert_eq!(
        ws.live_of(ws.attempts() - 1),
        topics_of(&["BTCUSDT", "ETHUSDT"])
    );
    let g = gaps(&rig.drain());
    assert!(g.len() >= 2 && g.len().is_multiple_of(2), "{g:?}");
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn silence_on_the_ticker_stream_reconnects_with_gaps_for_both_streams() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), one_page(vec![BTC, ETH]));
    sleep(Duration::from_secs(6)).await;
    rig.drain();
    ws.switches.quiet.store(true, Ordering::Relaxed);
    sleep(Duration::from_secs(12)).await;
    ws.switches.quiet.store(false, Ordering::Relaxed);
    sleep(Duration::from_secs(10)).await;
    assert_eq!(ws.attempts(), 2, "one reconnect after 10 s of silence");
    assert_eq!(ws.live_of(1), topics_of(&["BTCUSDT", "ETHUSDT"]));
    let events = rig.drain();
    let g = gaps(&events);
    assert_eq!(g.len(), 2, "{g:?}");
    let streams: BTreeSet<_> = g.iter().map(|g| g.stream.as_str()).collect();
    assert_eq!(streams, BTreeSet::from(["liquidations", "open_interest"]));
    assert!(
        g.iter()
            .all(|g| g.exchange == "bybit" && g.from_ts < g.to_ts)
    );
    assert_eq!((g[0].from_ts, g[0].to_ts), (g[1].from_ts, g[1].to_ts));
    assert_eq!(rig.count(|s| &s.gaps), 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn an_abrupt_close_reconnects_resubscribes_and_samples_resume() {
    let ws = MockWs::new(vec![]);
    ws.set_values("BTCUSDT", 60000.0, 500.0);
    let mut rig = start(config(&[], 10), ws.clone(), one_page(vec![BTC]));
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
    assert_eq!(ws.live_of(ws.attempts() - 1), topics_of(&["BTCUSDT"]));
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Runtime tracked symbols
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn policy_changes_subscribe_and_unsubscribe_without_gaps_or_duplicates() {
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), one_page(vec![BTC, ETH, SOL]));
    sleep(Duration::from_secs(5)).await;
    assert_eq!(ws.live(), topics_of(&["BTCUSDT", "ETHUSDT", "SOLUSDT"]));
    assert_eq!(
        ws.requests_of("subscribe").len(),
        1,
        "all topics in one request"
    );
    rig.drain();

    // Track only BTC.
    rig.symbols
        .set_policy(SymbolPolicy::new(Mode::Whitelist, ["BTC-USDT-PERP"]).unwrap());
    sleep(Duration::from_secs(2)).await;
    assert_eq!(ws.live(), topics_of(&["BTCUSDT"]));
    let un = ws.requests_of("unsubscribe");
    assert_eq!(un.len(), 1);
    assert_eq!(
        un[0].iter().cloned().collect::<BTreeSet<_>>(),
        topics_of(&["ETHUSDT", "SOLUSDT"])
    );
    // A late frame of an untracked symbol (unsubscribe in flight) is dropped, not published.
    ws.push(
        0,
        &liq_frame("ETHUSDT", "Buy", "1", "3000", EPOCH_MS + 7_000),
    );
    ws.push(
        0,
        &liq_frame("BTCUSDT", "Buy", "1", "60000", EPOCH_MS + 7_001),
    );
    sleep(Duration::from_secs(40)).await;
    let events = rig.drain();
    let liqs = liquidations(&events);
    assert_eq!(liqs.len(), 1);
    assert_eq!(liqs[0].symbol_raw, "BTCUSDT");
    assert!(
        open_interests(&events)
            .iter()
            .all(|o| o.symbol_raw == "BTCUSDT"),
        "no samples of untracked symbols"
    );
    assert!(gaps(&events).is_empty(), "untracking is not an outage");
    assert_eq!(ws.attempts(), 1, "no reconnect");

    // Track ETH and SOL again.
    rig.symbols.set_policy(SymbolPolicy::all());
    sleep(Duration::from_secs(40)).await;
    assert_eq!(ws.live(), topics_of(&["BTCUSDT", "ETHUSDT", "SOLUSDT"]));
    let events = rig.drain();
    assert!(gaps(&events).is_empty());
    let oi = open_interests(&events);
    let symbols: BTreeSet<_> = oi.iter().map(|o| o.symbol_raw.as_str()).collect();
    assert_eq!(symbols, BTreeSet::from(["BTCUSDT", "ETHUSDT", "SOLUSDT"]));
    assert_eq!(ws.attempts(), 1);
    assert_eq!(rig.count(|s| &s.gaps), 0);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_configured_whitelist_is_the_initial_tracked_set() {
    let ws = MockWs::new(vec![]);
    let rig = start(
        config(&["ETH-USDT-PERP"], 30),
        ws.clone(),
        one_page(vec![BTC, ETH]),
    );
    sleep(Duration::from_secs(5)).await;
    assert_eq!(ws.live(), topics_of(&["ETHUSDT"]));
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_set_is_resent_in_full_after_a_reconnect_following_a_runtime_change() {
    let ws = MockWs::new(vec![]);
    let rig = start(config(&[], 30), ws.clone(), one_page(vec![BTC, ETH, SOL]));
    sleep(Duration::from_secs(5)).await;
    rig.symbols.set_policy(
        SymbolPolicy::new(Mode::Whitelist, ["SOL-USDT-PERP", "ETH-USDT-PERP"]).unwrap(),
    );
    sleep(Duration::from_secs(3)).await;
    ws.switches.quiet.store(true, Ordering::Relaxed);
    sleep(Duration::from_secs(12)).await;
    ws.switches.quiet.store(false, Ordering::Relaxed);
    sleep(Duration::from_secs(5)).await;
    assert_eq!(ws.attempts(), 2);
    assert_eq!(ws.live_of(1), topics_of(&["ETHUSDT", "SOLUSDT"]));
    let first_of_second = ws
        .requests()
        .into_iter()
        .find(|r| r.0 == 1 && r.1 == "subscribe")
        .unwrap();
    assert_eq!(first_of_second.2.len(), 4);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_refused_topic_is_reported_and_the_others_keep_flowing() {
    let ws = MockWs::new(vec![]);
    ws.refuse("tickers.SOLUSDT");
    let mut rig = start(config(&[], 10), ws.clone(), one_page(vec![BTC, ETH, SOL]));
    sleep(Duration::from_secs(15)).await;
    let mut expected = topics_of(&["BTCUSDT", "ETHUSDT", "SOLUSDT"]);
    expected.remove("tickers.SOLUSDT");
    assert_eq!(ws.live(), expected);
    let rejected = rig.status.rejected_topics();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].0, "tickers.SOLUSDT");
    assert!(rejected[0].1.contains("handler not found"));
    // No reconnect, no Gap; the healthy symbols still produce samples.
    assert_eq!(ws.attempts(), 1);
    let events = rig.drain();
    assert!(gaps(&events).is_empty());
    let symbols: BTreeSet<_> = open_interests(&events)
        .iter()
        .map(|o| o.symbol_raw.clone())
        .collect();
    assert_eq!(
        symbols,
        BTreeSet::from(["BTCUSDT".to_string(), "ETHUSDT".to_string()])
    );
    let mut families = vulture_server::metrics::Families::new();
    rig.status.collect_metrics(&mut families);
    assert!(
        families
            .render()
            .contains("vulture_ws_topics_rejected_total{exchange=\"bybit\"} 1")
    );
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Universe: pagination, reload, throttling
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_instrument_list_is_paged_filtered_and_normalized() {
    let first = page(&[BTC, BTC_USDC], "first%3DBTCUSDT%26last%3DBTCPERP");
    let other: Vec<Inst> = vec![
        ETH,
        // Dated future, pre-launch contract, inverse-style quote: all filtered out.
        ("BTC-27MAR26", "LinearFutures", "Trading", "BTC", "USDT"),
        ("NEWUSDT", "LinearPerpetual", "PreLaunch", "NEW", "USDT"),
        ("XRPUSD1", "LinearPerpetual", "Trading", "XRP", "USD1"),
        PEPE,
    ];
    let second = page(&other, "");
    let http = MockHttp::new(move |path, _| {
        if path.contains("cursor=first%3DBTCUSDT%26last%3DBTCPERP") {
            Reply::ok(&second)
        } else {
            Reply::ok(&first)
        }
    });
    let ws = MockWs::new(vec![]);
    let rig = start(config(&[], 30), ws.clone(), http.clone());
    sleep(Duration::from_secs(5)).await;
    let paths = http.paths();
    assert_eq!(paths.len(), 2, "{paths:?}");
    assert_eq!(
        paths[0],
        "/v5/market/instruments-info?category=linear&limit=1000"
    );
    assert_eq!(
        paths[1],
        "/v5/market/instruments-info?category=linear&limit=1000&cursor=first%3DBTCUSDT%26last%3DBTCPERP",
        "the cursor goes back exactly as Bybit sent it"
    );
    assert_eq!(
        ws.live(),
        topics_of(&["BTCUSDT", "BTCPERP", "ETHUSDT", "1000PEPEUSDT"])
    );
    assert_eq!(rig.count(|s| &s.symbols), 4);
    let universe = rig.symbols.universe().unwrap();
    let names: BTreeSet<_> = universe.iter().cloned().collect();
    assert_eq!(
        names,
        BTreeSet::from([
            "BTC-USDT-PERP".to_string(),
            "BTC-USDC-PERP".to_string(),
            "ETH-USDT-PERP".to_string(),
            "PEPE-USDT-PERP".to_string()
        ])
    );
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_hourly_reload_adds_and_removes_topics_without_gaps() {
    let before = page(&[BTC, ETH], "");
    let after = page(&[BTC, SOL], "");
    let http = MockHttp::new(move |_, idx| Reply::ok(if idx == 0 { &before } else { &after }));
    let ws = MockWs::new(vec![]);
    let mut rig = start(config(&[], 30), ws.clone(), http.clone());
    sleep(Duration::from_secs(5)).await;
    assert_eq!(ws.live(), topics_of(&["BTCUSDT", "ETHUSDT"]));
    rig.drain();
    sleep(Duration::from_secs(3600)).await;
    assert_eq!(http.paths().len(), 2, "one reload per hour");
    let offsets = http.offsets();
    assert!(offsets[1] >= Duration::from_secs(3600) && offsets[1] < Duration::from_secs(3610));
    assert_eq!(ws.live(), topics_of(&["BTCUSDT", "SOLUSDT"]));
    assert_eq!(ws.attempts(), 1);
    assert!(gaps(&rig.drain()).is_empty());
    assert_eq!(rig.count(|s| &s.info_loads), 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_failed_load_is_retried_and_a_403_waits_ten_minutes() {
    let body = page(&[BTC], "");
    let http = MockHttp::new(move |_, idx| match idx {
        0 => Reply::status(500, "boom"),
        1 => Reply::status(403, "forbidden"),
        _ => Reply::ok(&body),
    });
    let ws = MockWs::new(vec![]);
    let rig = start(config(&[], 30), ws.clone(), http.clone());
    sleep(Duration::from_secs(2000)).await;
    let t = http.offsets();
    assert_eq!(t.len(), 3);
    assert!(t[1] - t[0] >= Duration::from_secs(5) && t[1] - t[0] < Duration::from_secs(10));
    assert!(
        t[2] - t[1] >= Duration::from_secs(600),
        "a 403 waits at least 10 minutes, waited {:?}",
        t[2] - t[1]
    );
    assert_eq!(rig.count(|s| &s.http_403), 1);
    assert_eq!(rig.count(|s| &s.info_errors), 2);
    assert_eq!(ws.live(), topics_of(&["BTCUSDT"]));
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_handshake_403_pauses_reconnects_for_ten_minutes() {
    let ws = MockWs::new(vec![Connect::Http(403, None)]);
    let rig = start(config(&[], 30), ws.clone(), one_page(vec![BTC]));
    sleep(Duration::from_secs(700)).await;
    let t = ws.attempt_offsets();
    assert_eq!(t.len(), 2, "{t:?}");
    assert!(
        t[1] - t[0] >= Duration::from_secs(600),
        "waited {:?}",
        t[1] - t[0]
    );
    assert_eq!(ws.live(), topics_of(&["BTCUSDT"]));
    rig.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Connections: sharding and the budget
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn symbols_spill_over_to_more_connections_only_when_the_cap_requires_it() {
    let ws = MockWs::new(vec![]);
    let tuning = BybitTuning {
        max_topics_per_connection: 4,
        ..BybitTuning::default()
    };
    let rig = start_with(
        config(&[], 30),
        tuning.clone(),
        ws.clone(),
        one_page(vec![BTC, ETH, SOL]),
    );
    sleep(Duration::from_secs(5)).await;
    // Two symbols (four topics) fit one connection; the third needs a second one.
    assert_eq!(ws.attempts(), 2);
    assert_eq!(rig.status.connections().len(), 2);
    assert_eq!(ws.live_of(0).len(), 4);
    assert_eq!(ws.live_of(1).len(), 2);
    assert_eq!(ws.live(), topics_of(&["BTCUSDT", "ETHUSDT", "SOLUSDT"]));
    for r in ws.requests() {
        assert!(r.2.len() <= 4);
    }
    // Removing a symbol frees room; it is not reshuffled, and nothing reconnects.
    rig.symbols
        .set_policy(SymbolPolicy::new(Mode::Blacklist, ["BTC-USDT-PERP"]).unwrap());
    sleep(Duration::from_secs(2)).await;
    assert_eq!(ws.live_of(0), topics_of(&["ETHUSDT"]));
    assert_eq!(ws.live_of(1), topics_of(&["SOLUSDT"]));
    assert_eq!(ws.attempts(), 2);
    rig.symbols.set_policy(SymbolPolicy::all());
    sleep(Duration::from_secs(2)).await;
    // BTC returns to the first connection with room (index 0).
    assert_eq!(ws.live_of(0), topics_of(&["BTCUSDT", "ETHUSDT"]));
    assert_eq!(ws.attempts(), 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn all_connections_obey_the_connection_budget() {
    let ws = MockWs::new(vec![]);
    let tuning = BybitTuning {
        max_topics_per_connection: 2,
        limiter: LimiterConfig {
            max_new_per_window: 2,
            window: Duration::from_secs(60),
            max_concurrent: 10,
        },
        ..BybitTuning::default()
    };
    let rig = start_with(
        config(&[], 30),
        tuning,
        ws.clone(),
        one_page(vec![BTC, ETH, SOL, BTC_USDC]),
    );
    sleep(Duration::from_secs(200)).await;
    // The first connection is opened at once for the empty set, the others as symbols arrive; the window
    // admits two per minute.
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
    let mut rig = start(config(&[], 10), ws.clone(), one_page(vec![BTC, ETH]));
    let render = |rig: &Rig| {
        let mut f = vulture_server::metrics::Families::new();
        rig.status.collect_metrics(&mut f);
        f.render()
    };
    sleep(Duration::from_secs(25)).await;
    ws.push(
        0,
        &liq_frame("BTCUSDT", "Buy", "1", "60000", EPOCH_MS + 20_000),
    );
    sleep(Duration::from_secs(5)).await;
    let first = render(&rig);
    for series in [
        "vulture_events_total{exchange=\"bybit\",type=\"liquidation\"}",
        "vulture_events_total{exchange=\"bybit\",type=\"open_interest\"}",
        "vulture_events_total{exchange=\"bybit\",type=\"gap\"}",
        "vulture_parse_errors_total{exchange=\"bybit\",type=\"liquidation\"}",
        "vulture_connector_ws_frames_total{exchange=\"bybit\"}",
        "vulture_connector_mark_frames_total{exchange=\"bybit\"}",
        "vulture_oi_cycles_total{exchange=\"bybit\"}",
        "vulture_symbols{exchange=\"bybit\"}",
        "vulture_ws_state{exchange=\"bybit\",state=\"healthy\"}",
        "vulture_ws_connections{exchange=\"bybit\"}",
        "vulture_ws_topics_subscribed{exchange=\"bybit\"}",
        "vulture_ws_topic_requests_total{exchange=\"bybit\"}",
        "vulture_ws_connection_permits_total{exchange=\"bybit\"}",
        "vulture_ws_connections_open{exchange=\"bybit\"}",
        "vulture_mark_price_age_seconds{exchange=\"bybit\"}",
        "vulture_exchange_info_loads_total{exchange=\"bybit\",result=\"ok\"}",
    ] {
        assert!(
            metric(&first, series).is_some(),
            "missing {series}\n{first}"
        );
    }
    assert_eq!(
        metric(&first, "vulture_symbols{exchange=\"bybit\"}"),
        Some(2.0)
    );
    assert_eq!(
        metric(&first, "vulture_ws_topics_subscribed{exchange=\"bybit\"}"),
        Some(4.0)
    );
    assert_eq!(
        metric(
            &first,
            "vulture_ws_state{exchange=\"bybit\",state=\"healthy\"}"
        ),
        Some(1.0)
    );
    let liq1 = metric(
        &first,
        "vulture_events_total{exchange=\"bybit\",type=\"liquidation\"}",
    )
    .unwrap();
    let oi1 = metric(
        &first,
        "vulture_events_total{exchange=\"bybit\",type=\"open_interest\"}",
    )
    .unwrap();
    let frames1 = metric(
        &first,
        "vulture_connector_ws_frames_total{exchange=\"bybit\"}",
    )
    .unwrap();
    assert_eq!(liq1, 1.0);
    assert!(oi1 >= 2.0);

    ws.push(
        0,
        &liq_frame("ETHUSDT", "Sell", "2", "3000", EPOCH_MS + 31_000),
    );
    sleep(Duration::from_secs(25)).await;
    let second = render(&rig);
    assert_eq!(
        metric(
            &second,
            "vulture_events_total{exchange=\"bybit\",type=\"liquidation\"}"
        ),
        Some(2.0)
    );
    assert!(
        metric(
            &second,
            "vulture_events_total{exchange=\"bybit\",type=\"open_interest\"}"
        )
        .unwrap()
            > oi1
    );
    assert!(
        metric(
            &second,
            "vulture_connector_ws_frames_total{exchange=\"bybit\"}"
        )
        .unwrap()
            > frames1
    );
    assert!(metric(&second, "vulture_oi_cycles_total{exchange=\"bybit\"}").unwrap() >= 2.0);
    // Low cardinality: no symbol labels anywhere.
    assert!(!second.contains("BTCUSDT"));
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
    ws.set_values("BTCUSDT", 60000.0, 500.0);
    let http = one_page(vec![BTC, PEPE]);
    let connector = BybitConnector::with_transport(
        config(&[], 1),
        BybitTuning::default(),
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

    common::wait_until("subscriptions", || ws.live().len() == 4).await;
    ws.push(
        0,
        &liq_frame("BTCUSDT", "Buy", "0.25", "60000", 1_800_000_000_123),
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
    assert_eq!(liqs[0].exchange, "bybit");
    assert_eq!(liqs[0].symbol, "BTC-USDT-PERP");
    assert_eq!(liqs[0].notional_usd, 15000.0);
    let ois: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.payload {
            Some(Payload::OpenInterest(o)) => Some(o.clone()),
            _ => None,
        })
        .collect();
    assert!(
        ois.iter()
            .any(|o| o.symbol == "BTC-USDT-PERP" && o.oi_base == 500.0)
    );
    assert!(ois.iter().any(|o| o.symbol == "PEPE-USDT-PERP"));
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
    let Some(backend) = common::Backend::redis("bybit-pipeline").await else {
        return;
    };
    let _serial = common::SERIAL.lock().await;
    pipeline(backend).await;
}

// ---------------------------------------------------------------------------------------------
// Opt-in live run (public read-only data, no keys). Run once by hand:
//   cargo test -p vulture-server --test bybit bybit_live -- --ignored --nocapture
// ---------------------------------------------------------------------------------------------

#[tokio::test]
#[ignore = "talks to the real Bybit public endpoints; run once by hand"]
async fn bybit_live() {
    live::run().await;
}

mod live {
    use super::*;
    use vulture_server::exchange::binance::ReqwestClient;
    use vulture_server::ws::{SystemClock, TungsteniteConnector};

    /// Counts and limits REST calls: at most two, and the page size is cut to 500 so that the cursor path is
    /// exercised for real.
    struct LimitedHttp {
        inner: ReqwestClient,
        calls: AtomicUsize,
        log: Mutex<Vec<String>>,
        bodies: Mutex<Vec<String>>,
    }

    impl HttpClient for LimitedHttp {
        fn get<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<HttpResponse, HttpError>> {
            Box::pin(async move {
                let n = self.calls.fetch_add(1, Ordering::SeqCst);
                assert!(n < 2, "the live run is limited to two REST calls");
                let path = path.replace("limit=1000", "limit=500");
                self.log.lock().unwrap().push(path.clone());
                let r = self.inner.get(&path).await;
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

    /// Raw probe on a second socket: format of acknowledgements, pong, partially invalid batches, the
    /// first snapshot and delta of a ticker, and (if any) the first liquidation frames.
    async fn probe(symbols: Vec<String>) {
        let (mut ws, resp) =
            tokio_tungstenite::connect_async("wss://stream.bybit.com/v5/public/linear")
                .await
                .expect("probe connect");
        eprintln!("PROBE handshake status: {}", resp.status());
        let send = |v: Value| Message::text(v.to_string());
        ws.send(send(json!({"op": "ping"}))).await.unwrap();
        // One batch with a valid and an invalid topic.
        ws.send(send(json!({"op": "subscribe", "req_id": "p1",
            "args": ["tickers.BTCUSDT", "tickers.NOSUCHCOINXYZUSDT"]})))
            .await
            .unwrap();
        // Liquidations of many symbols, in one request.
        let args: Vec<String> = symbols
            .iter()
            .map(|s| format!("allLiquidation.{s}"))
            .collect();
        ws.send(send(
            json!({"op": "subscribe", "req_id": "p2", "args": args}),
        ))
        .await
        .unwrap();
        let t0 = std::time::Instant::now();
        let (mut snap, mut delta, mut liqs, mut other, mut btc_frames) = (0, 0, 0, 0, 0);
        while t0.elapsed() < Duration::from_secs(60) {
            let Ok(Some(Ok(Message::Text(t)))) = timeout(Duration::from_secs(5), ws.next()).await
            else {
                continue;
            };
            let t = t.to_string();
            if t.contains("\"topic\":\"tickers.") {
                if t.contains("\"type\":\"snapshot\"") && snap < 2 {
                    snap += 1;
                    eprintln!("PROBE ticker snapshot: {t}");
                } else if t.contains("\"type\":\"delta\"") {
                    delta += 1;
                    if delta <= 6 {
                        eprintln!("PROBE ticker delta: {t}");
                    }
                }
                btc_frames += 1;
            } else if t.contains("allLiquidation") {
                liqs += 1;
                if liqs <= 6 {
                    eprintln!("PROBE liquidation frame: {t}");
                }
            } else {
                other += 1;
                eprintln!("PROBE control frame: {t}");
            }
        }
        eprintln!(
            "PROBE summary: ticker frames {btc_frames}, snapshots<=2 shown, deltas {delta}, liquidation frames {liqs}, control frames {other}"
        );
        let _ = ws.close(None).await;
    }

    pub async fn run() {
        let http = Arc::new(LimitedHttp {
            inner: ReqwestClient::new("https://api.bybit.com").unwrap(),
            calls: AtomicUsize::new(0),
            log: Mutex::new(Vec::new()),
            bodies: Mutex::new(Vec::new()),
        });
        let shutdown = CancellationToken::new();
        let connector = BybitConnector::with_transport(
            config(&[], 10),
            BybitTuning::default(),
            Arc::new(TungsteniteConnector),
            http.clone(),
            Arc::new(SystemClock),
        );
        // `config` points at the mock URLs; use the real ones.
        let connector = {
            let mut cfg = config(&[], 10);
            cfg.ws_url = vulture_server::exchange::bybit::DEFAULT_WS_URL.into();
            cfg.rest_url = vulture_server::exchange::bybit::DEFAULT_REST_URL.into();
            drop(connector);
            BybitConnector::with_transport(
                cfg,
                BybitTuning::default(),
                Arc::new(TungsteniteConnector),
                http.clone(),
                Arc::new(SystemClock),
            )
        };
        let status = connector.status();
        let symbols = connector.symbols().unwrap();
        let ctx = AppContext::new(Config::default(), shutdown.clone());
        let (tx, mut rx) = mpsc::channel(100_000);
        let task = tokio::spawn(async move { connector.run(ctx, tx).await });

        // Wait for the universe, then probe on a second socket with the first 150 symbols.
        let t0 = std::time::Instant::now();
        while symbols.universe().is_none() {
            assert!(
                t0.elapsed() < Duration::from_secs(30),
                "the instrument list did not load (403 or region block?)"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let universe: Vec<String> = symbols.universe().unwrap().iter().cloned().collect();
        eprintln!(
            "LIVE universe: {} symbols; REST calls: {:?}",
            universe.len(),
            http.log.lock().unwrap()
        );
        for b in http.bodies.lock().unwrap().iter() {
            let head: String = b.chars().take(1200).collect();
            eprintln!("LIVE REST response head: {head}");
            if let Some(i) = b.find("nextPageCursor") {
                eprintln!("LIVE cursor field: {}", &b[i..(i + 140).min(b.len())]);
            }
        }
        let usdc: Vec<_> = universe
            .iter()
            .filter(|s| s.ends_with("-USDC-PERP"))
            .take(8)
            .collect();
        eprintln!("LIVE USDC perpetuals (normalized): {usdc:?}");
        let raw_probe: Vec<String> = [
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
            "DOGEUSDT",
            "XRPUSDT",
            "1000PEPEUSDT",
            "BTCPERP",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let probe_task = tokio::spawn(probe(raw_probe));

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
            if seen.insert(o.symbol.clone()) && seen.len() <= 6 {
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
