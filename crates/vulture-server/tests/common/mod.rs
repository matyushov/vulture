//! Shared harness of the gRPC tests: a store backend (memory or a real Redis), a running server on an
//! ephemeral loopback port and tonic clients that talk to it over the wire.
#![allow(dead_code)]

pub mod certs;
pub mod ch_env;
pub mod history_data;
pub mod history_rig;
pub mod log_capture;
pub mod mock_http;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Status, Streaming};
use vulture_proto::v1::stream_client::StreamClient;
use vulture_proto::v1::{
    Event, EventType, Gap, Liquidation, OpenInterest, Side, Signal, SubscribeRequest,
    event::Payload,
};
use vulture_server::auth::{AuthStats, LimiterConfig, TokenAuthority, TokenStore};
use vulture_server::config::Secret;
use vulture_server::grpc::{self, GrpcStats, ServeOptions, StreamConfig};
use vulture_server::store::{
    EntryFields, MemoryStore, READER_CLIENT_NAME, RedisClientStore, RedisStore, Trim,
};

pub static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---------------------------------------------------------------- store backends

pub enum Backend {
    Memory(Arc<MemoryStore>),
    Redis(RedisEnv),
}

pub struct RedisEnv {
    key: String,
    store: Arc<RedisClientStore>,
    admin: redis::aio::MultiplexedConnection,
}

fn unique_key(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("vulture:test:{name}:{}:{nanos}", std::process::id())
}

impl Backend {
    pub fn memory() -> Self {
        Backend::Memory(Arc::new(MemoryStore::new()))
    }

    /// `None` (with a message) when `VULTURE_TEST_REDIS_URL` is not set.
    pub async fn redis(name: &str) -> Option<Self> {
        let url = match std::env::var("VULTURE_TEST_REDIS_URL") {
            Ok(url) if !url.trim().is_empty() => url,
            _ => {
                eprintln!(
                    "SKIPPED: VULTURE_TEST_REDIS_URL is not set (no Redis server for this test)"
                );
                return None;
            }
        };
        let key = unique_key(name);
        let store = Arc::new(RedisClientStore::new(&Secret::new(url.clone()), &key).unwrap());
        let admin = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .expect("cannot connect to VULTURE_TEST_REDIS_URL");
        Some(Backend::Redis(RedisEnv { key, store, admin }))
    }

    pub fn store(&self) -> Arc<dyn RedisStore> {
        match self {
            Backend::Memory(s) => s.clone(),
            Backend::Redis(r) => r.store.clone(),
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Backend::Memory(_) => "memory",
            Backend::Redis(_) => "redis",
        }
    }

    /// Appends events in one batch and returns their entry IDs.
    pub async fn append(&self, events: &[Event]) -> Vec<String> {
        let fields: Vec<EntryFields> = events
            .iter()
            .map(|e| EntryFields::from_event(e).expect("event with payload"))
            .collect();
        self.store()
            .append(
                &fields,
                Trim {
                    min_id_ms: 0,
                    maxlen: 10_000_000,
                },
            )
            .await
            .expect("append")
    }

    /// Removes every entry older than `first_kept` (exactly).
    pub async fn trim_to(&self, first_kept: &str) {
        match self {
            Backend::Memory(s) => s.trim_to(first_kept),
            Backend::Redis(r) => {
                let mut conn = r.admin.clone();
                let _: i64 = redis::cmd("XTRIM")
                    .arg(&r.key)
                    .arg("MINID")
                    .arg(first_kept)
                    .query_async(&mut conn)
                    .await
                    .unwrap();
            }
        }
    }

    /// Ids of the reader connections (Redis) or a count (memory).
    pub async fn readers(&self) -> usize {
        match self {
            Backend::Memory(s) => s.open_readers(),
            Backend::Redis(r) => reader_ids(r).await.len(),
        }
    }

    /// Kills every dedicated reader connection (`CLIENT KILL`), like a network failure or a Redis restart.
    pub async fn kill_readers(&self) -> usize {
        match self {
            Backend::Memory(s) => {
                let n = s.open_readers();
                s.kill_readers();
                n
            }
            Backend::Redis(r) => {
                let ids = reader_ids(r).await;
                let mut conn = r.admin.clone();
                for id in &ids {
                    let _: i64 = redis::cmd("CLIENT")
                        .arg("KILL")
                        .arg("ID")
                        .arg(id)
                        .query_async(&mut conn)
                        .await
                        .unwrap();
                }
                ids.len()
            }
        }
    }

    /// Memory only: makes every store operation fail for a while.
    pub fn set_failing(&self, failing: bool) {
        match self {
            Backend::Memory(s) => s.set_failing(failing),
            Backend::Redis(_) => panic!("set_failing is for the memory backend"),
        }
    }

    pub async fn cleanup(&self) {
        if let Backend::Redis(r) = self {
            let mut conn = r.admin.clone();
            let _: i64 = redis::cmd("DEL")
                .arg(&r.key)
                .query_async(&mut conn)
                .await
                .unwrap();
        }
    }
}

async fn reader_ids(r: &RedisEnv) -> Vec<String> {
    let mut conn = r.admin.clone();
    let list: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(&mut conn)
        .await
        .unwrap();
    list.lines()
        .filter_map(|line| {
            let field = |name: &str| {
                line.split(' ')
                    .find_map(|kv| kv.strip_prefix(&format!("{name}=")))
            };
            (field("name") == Some(READER_CLIENT_NAME)).then(|| field("id").unwrap().to_string())
        })
        .collect()
}

// ---------------------------------------------------------------- events

pub fn liq(exchange: &str, symbol: &str, ts: i64) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::Liquidation(Liquidation {
            exchange: exchange.into(),
            symbol_raw: symbol.replace('-', "").replace("PERP", ""),
            symbol: symbol.into(),
            exchange_ts: ts - 5,
            recv_ts: ts,
            side: Side::Long as i32,
            price: 100.0 + ts as f64,
            qty: 1.5,
            notional_usd: 150.0 + ts as f64,
        })),
    }
}

pub fn oi(exchange: &str, symbol: &str, ts: i64) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::OpenInterest(OpenInterest {
            exchange: exchange.into(),
            symbol_raw: symbol.replace('-', "").replace("PERP", ""),
            symbol: symbol.into(),
            exchange_ts: ts - 5,
            recv_ts: ts,
            oi_base: 1000.0 + ts as f64,
            oi_usd: Some(2.0e6),
            mark_price: Some(2000.0),
        })),
    }
}

pub fn gap(exchange: &str) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::Gap(Gap {
            exchange: exchange.into(),
            stream: "liquidations".into(),
            from_ts: 1,
            to_ts: 2,
        })),
    }
}

/// A signal with a `details` value of `filler` bytes, to make events big enough to fill HTTP/2 windows.
pub fn signal(exchange: &str, symbol: &str, filler: usize) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::Signal(Signal {
            r#type: 1,
            exchange: exchange.into(),
            symbol: symbol.into(),
            window_secs: 300,
            change_pct: 12.5,
            change_abs: 100.0,
            threshold: 10.0,
            ts: 5,
            details: [("pad".to_string(), "x".repeat(filler))]
                .into_iter()
                .collect(),
        })),
    }
}

/// `n` liquidations of `binance` / `BTC-USDT-PERP` with increasing timestamps.
pub fn numbered(n: usize) -> Vec<Event> {
    (0..n)
        .map(|i| liq("binance", "BTC-USDT-PERP", 1_000 + i as i64))
        .collect()
}

pub fn with_ids(events: &[Event], ids: &[String]) -> Vec<Event> {
    events
        .iter()
        .zip(ids)
        .map(|(e, id)| Event {
            id: id.clone(),
            payload: e.payload.clone(),
        })
        .collect()
}

// ---------------------------------------------------------------- server

pub fn test_stream_config() -> StreamConfig {
    StreamConfig {
        max_clients: 16,
        client_buffer: 256,
        slow_client_timeout: Duration::from_secs(2),
        page_size: 1000,
        block: Duration::from_millis(200),
        retry_min: Duration::from_millis(20),
        retry_max: Duration::from_millis(200),
    }
}

pub struct Server {
    pub addr: SocketAddr,
    pub token: String,
    pub authority: Arc<TokenAuthority>,
    pub token_store: TokenStore,
    pub stats: Arc<GrpcStats>,
    pub auth_stats: Arc<AuthStats>,
    pub shutdown: CancellationToken,
    pub task: JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Server {
    pub async fn start(backend: &Backend, stream: StreamConfig) -> Self {
        Self::start_with(backend, stream, LimiterConfig::default()).await
    }

    pub async fn start_with(
        backend: &Backend,
        stream: StreamConfig,
        limiter: LimiterConfig,
    ) -> Self {
        Self::start_full(backend, stream, limiter, None).await
    }

    /// Like `start_with`, with the symbol control behind the `Control` service.
    pub async fn start_full(
        backend: &Backend,
        stream: StreamConfig,
        limiter: LimiterConfig,
        symbols: Option<Arc<vulture_server::symbols::SymbolManager>>,
    ) -> Self {
        Self::start_all(backend, stream, limiter, symbols, None).await
    }

    /// Like `start_full`, with the rules and channels backend as well.
    pub async fn start_all(
        backend: &Backend,
        stream: StreamConfig,
        limiter: LimiterConfig,
        symbols: Option<Arc<vulture_server::symbols::SymbolManager>>,
        alerts: Option<Arc<vulture_server::alerts::AlertsManager>>,
    ) -> Self {
        Self::start_inner(backend, stream, limiter, symbols, alerts, None).await
    }

    /// Like `start_with`, with a `History` backend.
    pub async fn start_history(
        backend: &Backend,
        history: Arc<vulture_server::query::HistoryService>,
    ) -> Self {
        Self::start_inner(
            backend,
            test_stream_config(),
            LimiterConfig::default(),
            None,
            None,
            Some(history),
        )
        .await
    }

    async fn start_inner(
        backend: &Backend,
        stream: StreamConfig,
        limiter: LimiterConfig,
        symbols: Option<Arc<vulture_server::symbols::SymbolManager>>,
        alerts: Option<Arc<vulture_server::alerts::AlertsManager>>,
        history: Option<Arc<vulture_server::query::HistoryService>>,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let token_store = TokenStore::new(dir.path());
        let token = token_store.create_if_missing().unwrap().into_secret();
        let authority = TokenAuthority::open(token_store.clone()).unwrap();
        let shutdown = CancellationToken::new();
        tokio::spawn(
            authority
                .clone()
                .watch_file(Duration::from_millis(20), shutdown.clone()),
        );
        let listener = grpc::bind("127.0.0.1:0", false).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stats = Arc::new(GrpcStats::default());
        let auth_stats = Arc::new(AuthStats::default());
        let options = ServeOptions {
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            limiter,
            stream,
            symbols,
            alerts,
            history,
        };
        let task = tokio::spawn({
            let (store, authority, stats, auth_stats, shutdown) = (
                backend.store(),
                authority.clone(),
                stats.clone(),
                auth_stats.clone(),
                shutdown.clone(),
            );
            async move {
                grpc::serve(
                    listener, store, authority, options, stats, auth_stats, shutdown,
                )
                .await
                .expect("serve");
            }
        });
        Self {
            addr,
            token: token.expose().to_string(),
            authority,
            token_store,
            stats,
            auth_stats,
            shutdown,
            task,
            _dir: dir,
        }
    }

    pub async fn channel(&self) -> Channel {
        Channel::from_shared(format!("http://{}", self.addr))
            .unwrap()
            .connect()
            .await
            .expect("connect")
    }

    /// A `Stream` client that sends the valid token.
    pub async fn client(&self) -> StreamClient<InterceptedService<Channel, Auth>> {
        self.client_with(Auth::bearer(&self.token)).await
    }

    pub async fn client_with(&self, auth: Auth) -> StreamClient<InterceptedService<Channel, Auth>> {
        StreamClient::with_interceptor(self.channel().await, auth)
    }

    pub async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .expect("the server must stop")
            .unwrap();
    }
}

/// Adds an `authorization` header with the given raw value, or none.
#[derive(Clone)]
pub struct Auth(pub Option<String>);

impl Auth {
    pub fn bearer(token: &str) -> Self {
        Auth(Some(format!("Bearer {token}")))
    }
    pub fn none() -> Self {
        Auth(None)
    }
    pub fn raw(value: &str) -> Self {
        Auth(Some(value.to_string()))
    }
}

impl Interceptor for Auth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = &self.0 {
            request
                .metadata_mut()
                .insert("authorization", value.parse().expect("ascii header"));
        }
        Ok(request)
    }
}

pub type Client = StreamClient<InterceptedService<Channel, Auth>>;

pub fn request(resume_from: &str) -> SubscribeRequest {
    SubscribeRequest {
        resume_from: resume_from.into(),
        ..Default::default()
    }
}

pub fn filtered(
    exchanges: &[&str],
    symbols: &[&str],
    types: &[EventType],
    resume_from: &str,
) -> SubscribeRequest {
    SubscribeRequest {
        exchanges: exchanges.iter().map(|s| s.to_string()).collect(),
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
        types: types.iter().map(|t| *t as i32).collect(),
        resume_from: resume_from.into(),
    }
}

/// Receives `n` events, failing the test after 10 s of silence.
pub async fn recv(stream: &mut Streaming<Event>, n: usize) -> Vec<Event> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let next = tokio::time::timeout(Duration::from_secs(10), stream.message())
            .await
            .unwrap_or_else(|_| panic!("timed out after {} of {n} events", out.len()));
        match next {
            Ok(Some(event)) => out.push(event),
            Ok(None) => panic!("stream ended after {} of {n} events", out.len()),
            Err(status) => panic!("stream failed after {} of {n} events: {status}", out.len()),
        }
    }
    out
}

/// Asserts that nothing arrives within `quiet`.
pub async fn expect_silence(stream: &mut Streaming<Event>, quiet: Duration) {
    if let Ok(msg) = tokio::time::timeout(quiet, stream.message()).await {
        panic!("expected silence, got {msg:?}");
    }
}

/// Reads until the stream ends and returns the final status code (an `Err` is expected).
pub async fn final_status(stream: &mut Streaming<Event>) -> (usize, Status) {
    let mut events = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(20), stream.message())
            .await
            .expect("the stream must end")
        {
            Ok(Some(_)) => events += 1,
            Ok(None) => panic!("the stream ended without an error status"),
            Err(status) => return (events, status),
        }
    }
}

pub async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
