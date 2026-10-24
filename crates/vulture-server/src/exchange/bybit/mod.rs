//! Bybit linear perpetual connector (V2a).
//!
//! - Universe: REST `/v5/market/instruments-info?category=linear` with cursor pagination, reloaded hourly
//!   ([`info`]); `LinearPerpetual` in `Trading` state with USDT or USDC quote. Base and quote come from
//!   `baseCoin` / `quoteCoin`, never from splitting the symbol (USDC perpetuals are not named `BASEUSDC`).
//! - WebSocket `wss://stream.bybit.com/v5/public/linear`: per tracked symbol the topics
//!   `allLiquidation.{symbol}` and `tickers.{symbol}`. They are subscribed and unsubscribed at runtime as the
//!   owner's policy changes, and re-sent after every reconnect ([`crate::ws::TopicControl`]). Application
//!   ping `{"op":"ping"}` every 20 s. The ticker stream is the liveness stream. Connections are opened within
//!   a budget of half of Bybit's documented limits ([`crate::ws::ConnectionLimiter`]).
//! - Liquidations: `S = Buy` means a **long** position was liquidated. `p` is the *bankruptcy price*, not the
//!   fill price; it is published as `price` (see README).
//! - Tickers: snapshot, then deltas that carry only changed fields ([`tickers`]). The state keeps the last
//!   `markPrice`, `openInterest` and `openInterestValue`; a missing field is never zero.
//! - Open interest: an interval sampler emits one event per tracked symbol per `oi_poll_interval_secs`, spread
//!   over the interval, from that state. Never one event per ticker delta.
//! - `Gap` events (streams `liquidations` and `open_interest`) when a connection recovers after a loss.
//!   Changing the tracked set never produces a `Gap`.

mod info;
mod metrics;
mod shards;
mod status;
mod stream;
mod tickers;

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until, timeout};
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::{Event, Gap, OpenInterest, event::Payload};

pub use info::INSTRUMENTS_PATH;
pub use shards::{ShardView, Shards};
pub use status::BybitStatus;
pub use stream::{
    BybitHandler, DEFAULT_ARGS_LIMIT, LIQUIDATION_PREFIX, Liq, PING, TICKER_PREFIX,
    liquidation_event, parse_liquidations, side_of, topic_of, topics_of,
};
pub use tickers::{TickerEntry, TickerUpdate, Tickers, parse_ticker};

use self::info::InfoLoader;
use super::binance::{HttpClient, ReqwestClient, SymbolEntry, SymbolTable};
use super::{EventSink, Exchange, ExchangeError};
use crate::app::AppContext;
use crate::config::ExchangeConfig;
use crate::symbols::{SymbolChannel, SymbolPolicy};
use crate::ws::{
    Clock, ConnectionLimiter, DEFAULT_BUDGET_FRACTION, LimiterConfig, PingConfig, PingMessage,
    SystemClock, TopicControl, TungsteniteConnector, WsConnector, WsOptions, WsOutput, WsProfile,
    spawn_ws_with,
};

/// Exchange name in the configuration and in events.
pub const NAME: &str = "bybit";
/// Default WebSocket endpoint (USDT and USDC linear perpetuals).
pub const DEFAULT_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
/// Default REST endpoint.
pub const DEFAULT_REST_URL: &str = "https://api.bybit.com";
/// `Gap.stream` for the liquidation feed (and the profile stream name of the WebSocket).
pub const LIQUIDATION_STREAM: &str = "liquidations";
/// `Gap.stream` for open interest.
pub const OPEN_INTEREST_STREAM: &str = "open_interest";

/// What the `[[exchanges]]` entry says, with defaults applied.
#[derive(Debug, Clone, PartialEq)]
pub struct BybitConfig {
    /// Default whitelist of normalized symbols; empty means all. The owner's runtime policy overrides it.
    pub symbols: Vec<String>,
    /// Interval of the open interest samples, seconds.
    pub oi_poll_interval_secs: u64,
    /// WebSocket URL.
    pub ws_url: String,
    /// REST base URL.
    pub rest_url: String,
}

impl BybitConfig {
    /// Takes the Bybit-relevant fields of a configuration entry.
    pub fn from_exchange_config(c: &ExchangeConfig) -> Self {
        Self {
            symbols: c.symbols.clone(),
            oi_poll_interval_secs: c.oi_poll_interval_secs,
            ws_url: c.ws_url.clone().unwrap_or_else(|| DEFAULT_WS_URL.into()),
            rest_url: c
                .rest_url
                .clone()
                .unwrap_or_else(|| DEFAULT_REST_URL.into()),
        }
    }
}

/// Timing and threshold knobs. The defaults are the production values; tests shorten them.
#[derive(Debug, Clone)]
pub struct BybitTuning {
    /// Regular instrument list reload.
    pub info_reload_interval: Duration,
    /// First retry delay after a failed load; doubles up to `info_retry`.
    pub info_initial_retry: Duration,
    /// Longest wait before retrying a failed load.
    pub info_retry: Duration,
    /// Deadline of one page request.
    pub info_timeout: Duration,
    /// Pause after a REST 403 (Bybit: wait at least 10 minutes).
    pub ban_pause: Duration,
    /// How long the first sampler cycle waits for the first ticker frame.
    pub first_data_wait: Duration,
    /// Topics per connection before a second connection is opened. Bybit documents no cap for linear.
    pub max_topics_per_connection: usize,
    /// Size limit of the `args` array of one request, characters.
    pub args_limit: usize,
    /// Client ping interval.
    pub ping_interval: Duration,
    /// Time to wait for the pong.
    pub pong_timeout: Duration,
    /// Connection budget: 50% of the documented 500 new connections per 5 minutes and 1000 per IP.
    pub limiter: LimiterConfig,
    /// Liquidation records remembered for deduplication.
    pub dedup_capacity: usize,
}

impl Default for BybitTuning {
    fn default() -> Self {
        Self {
            info_reload_interval: Duration::from_secs(3600),
            info_initial_retry: Duration::from_secs(5),
            info_retry: Duration::from_secs(60),
            info_timeout: Duration::from_secs(30),
            ban_pause: Duration::from_secs(600),
            first_data_wait: Duration::from_secs(10),
            max_topics_per_connection: 4000,
            args_limit: DEFAULT_ARGS_LIMIT,
            ping_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(10),
            limiter: LimiterConfig::from_documented(
                500,
                Duration::from_secs(300),
                1000,
                DEFAULT_BUDGET_FRACTION,
            ),
            dedup_capacity: 10_000,
        }
    }
}

/// The connector.
pub struct BybitConnector {
    cfg: BybitConfig,
    tuning: BybitTuning,
    ws: Arc<dyn WsConnector>,
    http: Arc<dyn HttpClient>,
    clock: Arc<dyn Clock>,
    status: Arc<BybitStatus>,
    profile_hook: Option<fn(&mut WsProfile)>,
    symbols: Arc<SymbolChannel>,
}

impl BybitConnector {
    /// Production connector: real sockets, system clock, default tuning.
    pub fn new(cfg: BybitConfig) -> Result<Self, String> {
        let http = ReqwestClient::new(&cfg.rest_url)?;
        Ok(Self::with_transport(
            cfg,
            BybitTuning::default(),
            Arc::new(TungsteniteConnector),
            Arc::new(http),
            Arc::new(SystemClock),
        ))
    }

    /// Connector with injected transports (tests).
    pub fn with_transport(
        cfg: BybitConfig,
        tuning: BybitTuning,
        ws: Arc<dyn WsConnector>,
        http: Arc<dyn HttpClient>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let symbols = SymbolChannel::new(SymbolPolicy::from_config(&cfg.symbols));
        Self {
            cfg,
            tuning,
            ws,
            http,
            clock,
            status: Arc::new(BybitStatus::default()),
            profile_hook: None,
            symbols,
        }
    }

    /// Lets tests adjust the WebSocket profile (shorter timeouts).
    pub fn with_profile_hook(mut self, hook: fn(&mut WsProfile)) -> Self {
        self.profile_hook = Some(hook);
        self
    }

    /// Counters and state, for metrics and tests.
    pub fn status(&self) -> Arc<BybitStatus> {
        self.status.clone()
    }

    fn profile(&self) -> WsProfile {
        let mut p = WsProfile::new(NAME, LIQUIDATION_STREAM, self.cfg.ws_url.clone());
        p.ping = Some(PingConfig {
            interval: self.tuning.ping_interval,
            message: PingMessage::Text(PING.into()),
            pong_timeout: self.tuning.pong_timeout,
        });
        // 403 on the handshake: terminate everything and wait at least 10 minutes (Bybit rate limit page).
        p.ban_pause_min = self.tuning.ban_pause;
        if let Some(hook) = self.profile_hook {
            hook(&mut p);
        }
        p
    }

    async fn run_inner(&self, ctx: AppContext, sink: EventSink) -> Result<(), ExchangeError> {
        let status = self.status.clone();
        let stop = ctx.shutdown.child_token();
        let sink_closed = Arc::new(AtomicBool::new(false));
        let tickers = Arc::new(Tickers::default());
        let limiter = ConnectionLimiter::new(self.tuning.limiter);
        let _ = status.limiter.set(limiter.clone());
        let shards = Shards::new(self.tuning.max_topics_per_connection);
        *status.shards.lock().unwrap_or_else(|e| e.into_inner()) = Some(shards.clone());
        let (table_tx, mut table_rx) = watch::channel::<Option<Arc<SymbolTable>>>(None);
        let mut policy_rx = self.symbols.subscribe_policy();
        let (merged_tx, mut merged_rx) = mpsc::channel::<(usize, WsOutput)>(10_000);
        let handler = Arc::new(BybitHandler::with_args_limit(self.tuning.args_limit));

        let mut tasks = JoinSet::new();
        tasks.spawn(
            InfoLoader {
                http: self.http.clone(),
                status: status.clone(),
                aliases: ctx.aliases.clone(),
                symbols: self.symbols.clone(),
                tuning: self.tuning.clone(),
                clock: self.clock.clone(),
                tx: table_tx,
                shutdown: stop.clone(),
            }
            .run(),
        );
        tasks.spawn(
            Sampler {
                status: status.clone(),
                tickers: tickers.clone(),
                shards: shards.clone(),
                clock: self.clock.clone(),
                interval: Duration::from_secs(self.cfg.oi_poll_interval_secs.max(1)),
                first_data_wait: self.tuning.first_data_wait,
                table: table_rx.clone(),
                policy: self.symbols.subscribe_policy(),
                sink: sink.clone(),
                sink_closed: sink_closed.clone(),
                shutdown: stop.clone(),
            }
            .run(),
        );

        let mut spawn = |index: usize, control: Arc<TopicControl>| {
            let (handle, mut rx) = spawn_ws_with(
                self.profile(),
                handler.clone(),
                self.ws.clone(),
                self.clock.clone(),
                stop.clone(),
                WsOptions {
                    topics: Some(control),
                    limiter: Some(limiter.clone()),
                },
            )?;
            let tx = merged_tx.clone();
            tokio::spawn(async move {
                while let Some(out) = rx.recv().await {
                    if tx.send((index, out)).await.is_err() {
                        break;
                    }
                }
            });
            Ok(shards::ShardParts { handle })
        };

        let mut dedup = RecentKeys::new(self.tuning.dedup_capacity);
        let mut result = Ok(());
        // First pass: opens the first connection even before the instrument list is known.
        if let Err(message) = self.reconcile(&shards, &table_rx, &policy_rx, &mut spawn) {
            stop.cancel();
            return Err(ExchangeError::Config {
                exchange: NAME.into(),
                message,
            });
        }
        loop {
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                changed = table_rx.changed() => {
                    if changed.is_err() { break; }
                    self.reconcile_or_stop(&shards, &table_rx, &policy_rx, &mut spawn, &tickers, &mut result, &stop);
                }
                changed = policy_rx.changed() => {
                    if changed.is_err() { break; }
                    self.reconcile_or_stop(&shards, &table_rx, &policy_rx, &mut spawn, &tickers, &mut result, &stop);
                }
                out = merged_rx.recv() => {
                    let Some((index, output)) = out else { break };
                    if sink_closed.load(Ordering::Relaxed) {
                        result = Err(ExchangeError::SinkClosed(NAME.into()));
                        break;
                    }
                    let events = self.handle_output(index, output, &tickers, &shards, &table_rx, &policy_rx, &mut dedup, &ctx);
                    for payload in events {
                        let is_liq = matches!(payload, Payload::Liquidation(_));
                        if sink.send(Event { id: String::new(), payload: Some(payload) }).await.is_err() {
                            result = Err(ExchangeError::SinkClosed(NAME.into()));
                            break;
                        }
                        if is_liq {
                            BybitStatus::inc(&status.liquidations);
                        }
                    }
                    if result.is_err() { break; }
                }
            }
        }
        if result.is_ok() && sink_closed.load(Ordering::Relaxed) {
            result = Err(ExchangeError::SinkClosed(NAME.into()));
        }
        stop.cancel();
        drop(merged_rx);
        for handle in shards.take_handles() {
            handle.join().await;
        }
        while tasks.join_next().await.is_some() {}
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn reconcile_or_stop(
        &self,
        shards: &Shards,
        table: &watch::Receiver<Option<Arc<SymbolTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
        spawn: &mut dyn FnMut(usize, Arc<TopicControl>) -> Result<shards::ShardParts, String>,
        tickers: &Tickers,
        result: &mut Result<(), ExchangeError>,
        stop: &CancellationToken,
    ) {
        match self.reconcile(shards, table, policy, spawn) {
            Ok(()) => {
                // State of symbols that left the tracked set is dropped.
                tickers.retain(|raw| shards.shard_of(raw).is_some());
            }
            Err(message) => {
                *result = Err(ExchangeError::Config {
                    exchange: NAME.into(),
                    message,
                });
                stop.cancel();
            }
        }
    }

    /// Makes the connections follow the tracked set (universe filtered by the policy).
    fn reconcile(
        &self,
        shards: &Shards,
        table: &watch::Receiver<Option<Arc<SymbolTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
        spawn: &mut dyn FnMut(usize, Arc<TopicControl>) -> Result<shards::ShardParts, String>,
    ) -> Result<(), String> {
        let wanted: BTreeSet<String> = match table.borrow().as_ref() {
            Some(t) => {
                let policy = policy.borrow().clone();
                t.tracked(&policy).map(|e| e.raw.clone()).collect()
            }
            None => BTreeSet::new(),
        };
        let changed = shards.reconcile(&wanted, spawn)?;
        for (shard, added, removed) in changed {
            tracing::info!(
                exchange = NAME,
                shard,
                added,
                removed,
                tracked = wanted.len(),
                "tracked symbols changed"
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_output(
        &self,
        shard: usize,
        output: WsOutput,
        tickers: &Tickers,
        shards: &Shards,
        table: &watch::Receiver<Option<Arc<SymbolTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
        dedup: &mut RecentKeys,
        ctx: &AppContext,
    ) -> Vec<Payload> {
        let status = &self.status;
        let _ = ctx;
        match output {
            WsOutput::Gap(gap) => {
                // The shard's symbols will be re-snapshotted; whatever is stored is from before the loss.
                tickers.forget(shards.symbols_of(shard).iter());
                status.gaps.fetch_add(2, Ordering::Relaxed);
                tracing::warn!(
                    shard,
                    from_ts = gap.from_ts,
                    to_ts = gap.to_ts,
                    "stream gap after a connection loss"
                );
                let oi = Gap {
                    stream: OPEN_INTEREST_STREAM.into(),
                    ..gap.clone()
                };
                vec![Payload::Gap(gap), Payload::Gap(oi)]
            }
            WsOutput::Data(m) => {
                BybitStatus::inc(&status.ws_frames);
                let Some(text) = m.frame.as_text() else {
                    BybitStatus::inc(&status.unknown_frames);
                    return Vec::new();
                };
                match topic_of(text) {
                    Some(t) if t.starts_with(TICKER_PREFIX) => {
                        self.handle_ticker(text, m.recv_ts_ms, tickers, table, policy);
                        Vec::new()
                    }
                    Some(t) if t.starts_with(LIQUIDATION_PREFIX) => {
                        self.handle_liquidations(text, m.recv_ts_ms, table, policy, dedup)
                    }
                    _ => {
                        BybitStatus::inc(&status.unknown_frames);
                        Vec::new()
                    }
                }
            }
        }
    }

    fn handle_ticker(
        &self,
        text: &str,
        recv_ts: i64,
        tickers: &Tickers,
        table: &watch::Receiver<Option<Arc<SymbolTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
    ) {
        let status = &self.status;
        let update = match parse_ticker(text) {
            Ok(u) => u,
            Err(e) => {
                BybitStatus::inc(&status.ticker_parse_errors);
                tracing::debug!(error = %e, "unparsable ticker frame");
                return;
            }
        };
        if update.bad_fields > 0 {
            status
                .ticker_parse_errors
                .fetch_add(update.bad_fields as u64, Ordering::Relaxed);
        }
        if !is_tracked(table, policy, &update.symbol) {
            BybitStatus::inc(&status.tickers_filtered);
            return;
        }
        tickers.apply(&update, recv_ts);
        BybitStatus::inc(&status.ticker_frames);
        status.last_ticker_ms.store(recv_ts, Ordering::Relaxed);
    }

    fn handle_liquidations(
        &self,
        text: &str,
        recv_ts: i64,
        table: &watch::Receiver<Option<Arc<SymbolTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
        dedup: &mut RecentKeys,
    ) -> Vec<Payload> {
        let status = &self.status;
        let records = match parse_liquidations(text) {
            Ok(r) => r,
            Err(e) => {
                BybitStatus::inc(&status.liquidation_parse_errors);
                tracing::warn!(error = %e, "unparsable allLiquidation frame");
                return Vec::new();
            }
        };
        let current = table.borrow().clone();
        let policy = policy.borrow().clone();
        let mut out = Vec::new();
        for rec in records {
            let liq = match rec {
                Ok(l) => l,
                Err(e) => {
                    BybitStatus::inc(&status.liquidation_parse_errors);
                    tracing::warn!(error = %e, "unusable liquidation record");
                    continue;
                }
            };
            let entry = current.as_ref().and_then(|t| t.get(&liq.symbol));
            let Some(entry) = entry.filter(|e| policy.allows(&e.symbol)) else {
                BybitStatus::inc(&status.liquidations_filtered);
                continue;
            };
            if !dedup.insert(liq.dedup_key()) {
                BybitStatus::inc(&status.liquidations_duplicate);
                continue;
            }
            out.push(Payload::Liquidation(liquidation_event(
                &liq, entry, recv_ts,
            )));
        }
        out
    }
}

fn is_tracked(
    table: &watch::Receiver<Option<Arc<SymbolTable>>>,
    policy: &watch::Receiver<Arc<SymbolPolicy>>,
    raw: &str,
) -> bool {
    let t = table.borrow();
    match t.as_ref().and_then(|t| t.get(raw)) {
        Some(e) => policy.borrow().allows(&e.symbol),
        None => false,
    }
}

/// Bounded memory of recent keys, oldest forgotten first.
struct RecentKeys {
    seen: HashSet<String>,
    order: VecDeque<String>,
    capacity: usize,
}

impl RecentKeys {
    fn new(capacity: usize) -> Self {
        Self {
            seen: HashSet::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// `true` if the key is new.
    fn insert(&mut self, key: String) -> bool {
        if !self.seen.insert(key.clone()) {
            return false;
        }
        self.order.push_back(key);
        if self.order.len() > self.capacity
            && let Some(old) = self.order.pop_front()
        {
            self.seen.remove(&old);
        }
        true
    }
}

/// Emits one `OpenInterest` per tracked symbol per interval from the ticker state, spread over the interval.
struct Sampler {
    status: Arc<BybitStatus>,
    tickers: Arc<Tickers>,
    shards: Arc<Shards>,
    clock: Arc<dyn Clock>,
    interval: Duration,
    first_data_wait: Duration,
    table: watch::Receiver<Option<Arc<SymbolTable>>>,
    policy: watch::Receiver<Arc<SymbolPolicy>>,
    sink: mpsc::Sender<Event>,
    sink_closed: Arc<AtomicBool>,
    shutdown: CancellationToken,
}

impl Sampler {
    async fn run(self) {
        // So that the first samples carry data, wait for the first ticker frame (bounded).
        tokio::select! {
            _ = self.shutdown.cancelled() => return,
            _ = timeout(self.first_data_wait, self.tickers.wait_first()) => {}
        }
        loop {
            let cycle_start = Instant::now();
            let entries: Vec<SymbolEntry> = {
                let table = self.table.borrow().clone();
                let policy = self.policy.borrow().clone();
                table
                    .map(|t| t.tracked(&policy).cloned().collect())
                    .unwrap_or_default()
            };
            let n = entries.len() as u32;
            for (i, entry) in entries.iter().enumerate() {
                let at = cycle_start + self.interval * (i as u32) / n.max(1);
                tokio::select! {
                    _ = self.shutdown.cancelled() => return,
                    _ = sleep_until(at) => {}
                }
                if self.emit(entry).await.is_err() {
                    return;
                }
            }
            BybitStatus::inc(&self.status.oi_cycles);
            // An empty cycle must not spin.
            let next = cycle_start + self.interval.max(Duration::from_millis(100));
            tokio::select! {
                _ = self.shutdown.cancelled() => return,
                _ = sleep_until(next) => {}
            }
        }
    }

    async fn emit(&self, entry: &SymbolEntry) -> Result<(), ()> {
        let st = &self.status;
        // Data of a connection that is down is stale even if the state still holds it.
        let ticker = self
            .tickers
            .get(&entry.raw)
            .filter(|_| self.shards.is_healthy(&entry.raw));
        let Some(t) = ticker.filter(|t| t.open_interest.is_some()) else {
            BybitStatus::inc(&st.oi_skipped);
            return Ok(());
        };
        if t.mark_price.is_none() {
            BybitStatus::inc(&st.oi_without_mark);
        }
        let values = match crate::normalize::oi::bybit(
            t.open_interest.unwrap_or_default(),
            t.open_interest_value,
            t.mark_price,
            entry.multiplier,
        ) {
            Ok(v) => v,
            Err(e) => {
                BybitStatus::inc(&st.oi_parse_errors);
                tracing::warn!(symbol = %entry.raw, error = %e, "unusable open interest state");
                return Ok(());
            }
        };
        let event = OpenInterest {
            exchange: NAME.into(),
            symbol_raw: entry.raw.clone(),
            symbol: entry.symbol.clone(),
            exchange_ts: t.exchange_ts,
            recv_ts: self.clock.now_ms(),
            oi_base: values.oi_base,
            oi_usd: values.oi_usd,
            // Per real coin, so that oi_base * mark_price == oi_usd.
            mark_price: t.mark_price.map(|m| m / entry.multiplier),
        };
        let sent = self
            .sink
            .send(Event {
                id: String::new(),
                payload: Some(Payload::OpenInterest(event)),
            })
            .await;
        if sent.is_err() {
            self.sink_closed.store(true, Ordering::Relaxed);
            return Err(());
        }
        BybitStatus::inc(&st.oi_events);
        Ok(())
    }
}

impl Exchange for BybitConnector {
    fn name(&self) -> &str {
        NAME
    }

    fn run<'a>(
        &'a self,
        ctx: AppContext,
        sink: EventSink,
    ) -> BoxFuture<'a, Result<(), ExchangeError>> {
        Box::pin(self.run_inner(ctx, sink))
    }

    fn collect_metrics(&self, out: &mut crate::metrics::Families) {
        self.status.collect_metrics(out);
    }

    fn symbols(&self) -> Option<Arc<SymbolChannel>> {
        Some(self.symbols.clone())
    }
}
