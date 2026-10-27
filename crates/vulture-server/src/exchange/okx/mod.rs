//! OKX linear swap connector (V2b): USDT and USDC margined perpetuals.
//!
//! - Universe: REST `GET /api/v5/public/instruments?instType=SWAP`, reloaded hourly ([`info`]); `ctType =
//!   linear`, `state = live`, settled in USDT or USDC. The USD-margined `*-USD_UM-SWAP` family and the inverse
//!   `*-USD-SWAP` contracts are excluded. Base and quote come from the `instId` (`BTC-USDT-SWAP` becomes
//!   `BTC-USDT-PERP`); `ctVal`, `ctMult` and `ctValCcy` are kept for the unit conversion ([`table`]).
//! - WebSocket `wss://ws.okx.com/ws/v5/public` (port 443; port 8443 is being retired): the channel
//!   `liquidation-orders` is subscribed once for `instType = SWAP` (all swaps) and filtered here; per tracked
//!   instrument the channels `mark-price` (also the liveness stream) and `open-interest` are subscribed through
//!   the runtime topic machinery ([`crate::ws::TopicControl`]) in batches of 100 arguments. Keepalive is the
//!   text `ping` every 20 s, answered by `pong`.
//! - Subscription requests are scarce (480 per hour and connection): tracked-set changes are collapsed by a
//!   settle delay and applied only when a sliding-window budget covers their requests ([`budget`]).
//! - Liquidations: `sz` is in **contracts**; base quantity is `sz * ctVal * ctMult`. `bkPx` is the bankruptcy
//!   price and is published as `price`. The side comes from `posSide` (hedge mode) or, in net mode, from the
//!   closing order (`sell` closes a long) - see [`stream::side_of`]. OKX documents that its liquidation feed is
//!   not complete ("doesn't represent the total number of liquidations").
//! - Open interest: `oi` (contracts), `oiCcy` (base coin) and `oiUsd` are kept per instrument next to the last
//!   `mark-price`; an interval sampler emits one `OpenInterest` per tracked instrument per
//!   `oi_poll_interval_secs`, spread over the interval. Never one event per push.
//! - Funding rate and index price: slow REST polls ([`funding`]), not WebSocket channels: the two extra channels per
//!   instrument would push the topics per connection past 2000 and spend the hourly subscription budget.
//! - `Gap` events (streams `liquidations` and `open_interest`) when a connection recovers after a loss.
//!   Changing the tracked set never produces a `Gap`.

mod budget;
mod funding;
mod info;
mod metrics;
mod state;
mod status;
mod stream;
mod table;

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

pub use budget::{RequestBudget, requests_for};
pub use funding::{
    FUNDING_RATE_ALL_PATH, FundingRecord, INDEX_TICKERS_USDC_PATH, INDEX_TICKERS_USDT_PATH,
    funding_rate_path, index_id_of, parse_funding_rates, parse_index_tickers,
};
pub use info::INSTRUMENTS_PATH;
pub use state::{FundingState, IndexState, OiState, Quote, Quotes};
pub use status::OkxStatus;
pub use stream::{
    DEFAULT_ARGS_PER_REQUEST, LIQUIDATION_CHANNEL, Liq, MARK_PRICE_CHANNEL, MarkUpdate,
    OPEN_INTEREST_CHANNEL, OiUpdate, OkxHandler, PING, PONG, channel_of, liquidation_event,
    parse_liquidations, parse_mark_price, parse_open_interest, side_of, topic, topic_in_message,
    topics_of,
};
pub use table::{OkxEntry, OkxTable};

use self::info::InfoLoader;
use super::binance::{HttpClient, ReqwestClient};
use super::{EventSink, Exchange, ExchangeError};
use crate::app::AppContext;
use crate::config::ExchangeConfig;
use crate::symbols::{SymbolChannel, SymbolPolicy};
use crate::ws::{
    Clock, ConnectionLimiter, DEFAULT_BUDGET_FRACTION, LimiterConfig, PingConfig, PingMessage,
    ShardParts, SystemClock, TopicControl, TungsteniteConnector, WsConnector, WsOptions, WsOutput,
    WsProfile, spawn_ws_with,
};
pub use crate::ws::{ShardView, Shards};

/// Exchange name in the configuration and in events.
pub const NAME: &str = "okx";
/// Default WebSocket endpoint (public channels). Port 443 is the default of `wss://`.
pub const DEFAULT_WS_URL: &str = "wss://ws.okx.com/ws/v5/public";
/// Default REST endpoint.
pub const DEFAULT_REST_URL: &str = "https://www.okx.com";
/// `Gap.stream` for the liquidation feed (and the profile stream name of the WebSocket).
pub const LIQUIDATION_STREAM: &str = "liquidations";
/// `Gap.stream` for open interest.
pub const OPEN_INTEREST_STREAM: &str = "open_interest";

/// What the `[[exchanges]]` entry says, with defaults applied.
#[derive(Debug, Clone, PartialEq)]
pub struct OkxConfig {
    /// Default whitelist of normalized symbols; empty means all. The owner's runtime policy overrides it.
    pub symbols: Vec<String>,
    /// Interval of the open interest samples, seconds.
    pub oi_poll_interval_secs: u64,
    /// WebSocket URL.
    pub ws_url: String,
    /// REST base URL.
    pub rest_url: String,
}

impl OkxConfig {
    /// Takes the OKX-relevant fields of a configuration entry.
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
pub struct OkxTuning {
    /// Regular instrument list reload.
    pub info_reload_interval: Duration,
    /// First retry delay after a failed load; doubles up to `info_retry`.
    pub info_initial_retry: Duration,
    /// Longest wait before retrying a failed load.
    pub info_retry: Duration,
    /// Deadline of the list request.
    pub info_timeout: Duration,
    /// Pause after a REST 403.
    pub ban_pause: Duration,
    /// How long the first sampler cycle waits for the first open interest push.
    pub first_data_wait: Duration,
    /// Topics per connection before a second connection is opened. OKX documents no cap.
    pub max_topics_per_connection: usize,
    /// Arguments per subscribe or unsubscribe request.
    pub args_per_request: usize,
    /// Client ping interval (OKX closes after 30 s without data).
    pub ping_interval: Duration,
    /// Time to wait for the pong.
    pub pong_timeout: Duration,
    /// Silence of the `mark-price` stream (it pushes at least every 10 s) that ends a session.
    pub data_silence: Duration,
    /// Connection budget: half of OKX's 3 new connections per second per IP and 30 per channel.
    pub limiter: LimiterConfig,
    /// Liquidation records remembered for deduplication.
    pub dedup_capacity: usize,
    /// A tracked-set change waits this long for further changes before it is applied.
    pub settle: Duration,
    /// Subscribe/unsubscribe requests allowed per `request_window` (half of the documented 480 per hour).
    pub request_budget: usize,
    /// Length of the request budget window.
    pub request_window: Duration,
    /// How often funding rates and index prices are fetched over REST. A value older than three intervals is not
    /// attached to open interest samples.
    pub funding_poll_interval: Duration,
    /// Least spacing between the per-instrument funding requests of the fallback.
    pub funding_min_spacing: Duration,
}

impl Default for OkxTuning {
    fn default() -> Self {
        Self {
            info_reload_interval: Duration::from_secs(3600),
            info_initial_retry: Duration::from_secs(5),
            info_retry: Duration::from_secs(60),
            info_timeout: Duration::from_secs(30),
            ban_pause: Duration::from_secs(600),
            first_data_wait: Duration::from_secs(10),
            max_topics_per_connection: 2000,
            args_per_request: DEFAULT_ARGS_PER_REQUEST,
            ping_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(5),
            data_silence: Duration::from_secs(30),
            limiter: LimiterConfig::from_documented(
                3,
                Duration::from_secs(1),
                30,
                DEFAULT_BUDGET_FRACTION,
            ),
            dedup_capacity: 10_000,
            settle: Duration::from_secs(2),
            request_budget: 240,
            request_window: Duration::from_secs(3600),
            funding_poll_interval: Duration::from_secs(300),
            funding_min_spacing: Duration::from_millis(500),
        }
    }
}

/// The connector.
pub struct OkxConnector {
    cfg: OkxConfig,
    tuning: OkxTuning,
    ws: Arc<dyn WsConnector>,
    http: Arc<dyn HttpClient>,
    clock: Arc<dyn Clock>,
    status: Arc<OkxStatus>,
    profile_hook: Option<fn(&mut WsProfile)>,
    symbols: Arc<SymbolChannel>,
}

async fn at(deadline: Option<Instant>) {
    match deadline {
        Some(d) => sleep_until(d).await,
        None => std::future::pending().await,
    }
}

impl OkxConnector {
    /// Production connector: real sockets, system clock, default tuning.
    pub fn new(cfg: OkxConfig) -> Result<Self, String> {
        let http = ReqwestClient::new(&cfg.rest_url)?;
        Ok(Self::with_transport(
            cfg,
            OkxTuning::default(),
            Arc::new(TungsteniteConnector),
            Arc::new(http),
            Arc::new(SystemClock),
        ))
    }

    /// Connector with injected transports (tests).
    pub fn with_transport(
        cfg: OkxConfig,
        tuning: OkxTuning,
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
            status: Arc::new(OkxStatus::default()),
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
    pub fn status(&self) -> Arc<OkxStatus> {
        self.status.clone()
    }

    fn profile(&self) -> WsProfile {
        let mut p = WsProfile::new(NAME, LIQUIDATION_STREAM, self.cfg.ws_url.clone());
        p.ping = Some(PingConfig {
            interval: self.tuning.ping_interval,
            message: PingMessage::Text(PING.into()),
            pong_timeout: self.tuning.pong_timeout,
        });
        // `mark-price` pushes at least every 10 s; OKX itself closes a connection after 30 s without data.
        p.data_silence = self.tuning.data_silence;
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
        let quotes = Arc::new(Quotes::default());
        let limiter = ConnectionLimiter::new(self.tuning.limiter);
        let _ = status.limiter.set(limiter.clone());
        let shards = Shards::new(self.tuning.max_topics_per_connection, topics_of);
        *status.shards.lock().unwrap_or_else(|e| e.into_inner()) = Some(shards.clone());
        let (table_tx, mut table_rx) = watch::channel::<Option<Arc<OkxTable>>>(None);
        let mut policy_rx = self.symbols.subscribe_policy();
        let (merged_tx, mut merged_rx) = mpsc::channel::<(usize, WsOutput)>(10_000);

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
                quotes: quotes.clone(),
                shards: shards.clone(),
                clock: self.clock.clone(),
                interval: Duration::from_secs(self.cfg.oi_poll_interval_secs.max(1)),
                first_data_wait: self.tuning.first_data_wait,
                funding_max_age: self.tuning.funding_poll_interval.saturating_mul(3),
                table: table_rx.clone(),
                policy: self.symbols.subscribe_policy(),
                sink: sink.clone(),
                sink_closed: sink_closed.clone(),
                shutdown: stop.clone(),
            }
            .run(),
        );

        tasks.spawn(
            funding::FundingPoller {
                http: self.http.clone(),
                status: status.clone(),
                quotes: quotes.clone(),
                clock: self.clock.clone(),
                tuning: self.tuning.clone(),
                table: table_rx.clone(),
                policy: self.symbols.subscribe_policy(),
                shutdown: stop.clone(),
            }
            .run(),
        );

        let args_per_request = self.tuning.args_per_request;
        let mut spawn = |index: usize, control: Arc<TopicControl>| {
            // Only the first connection listens to liquidations: a second subscription would double them.
            let handler = Arc::new(OkxHandler::new(index == 0, args_per_request));
            let (handle, mut rx) = spawn_ws_with(
                self.profile(),
                handler,
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
            Ok(ShardParts { handle })
        };

        let mut dedup = RecentKeys::new(self.tuning.dedup_capacity);
        let mut budget = RequestBudget::new(self.tuning.request_budget, self.tuning.request_window);
        let mut reconcile_at: Option<Instant> = None;
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
                    self.schedule(&mut reconcile_at, &shards);
                }
                changed = policy_rx.changed() => {
                    if changed.is_err() { break; }
                    self.schedule(&mut reconcile_at, &shards);
                }
                _ = at(reconcile_at) => {
                    let wanted = self.wanted(&table_rx, &policy_rx);
                    let (added, removed) = shards.diff_counts(&wanted);
                    let cost = requests_for(added, removed, args_per_request);
                    let now = Instant::now();
                    match budget.take(cost, now) {
                        Some(wait) => {
                            OkxStatus::inc(&status.changes_deferred);
                            tracing::warn!(
                                exchange = NAME,
                                requests = cost,
                                retry_in = ?wait,
                                "subscription request budget spent; tracked-set change postponed"
                            );
                            reconcile_at = Some(now + wait);
                        }
                        None => {
                            reconcile_at = None;
                            self.reconcile_or_stop(&shards, &table_rx, &policy_rx, &mut spawn, &quotes, &mut result, &stop);
                            status
                                .request_budget_spent
                                .store(budget.spent(now) as u64, Ordering::Relaxed);
                        }
                    }
                }
                out = merged_rx.recv() => {
                    let Some((index, output)) = out else { break };
                    if sink_closed.load(Ordering::Relaxed) {
                        result = Err(ExchangeError::SinkClosed(NAME.into()));
                        break;
                    }
                    let events = self.handle_output(index, output, &quotes, &shards, &table_rx, &policy_rx, &mut dedup);
                    for payload in events {
                        let is_liq = matches!(payload, Payload::Liquidation(_));
                        if sink.send(Event { id: String::new(), payload: Some(payload) }).await.is_err() {
                            result = Err(ExchangeError::SinkClosed(NAME.into()));
                            break;
                        }
                        if is_liq {
                            OkxStatus::inc(&status.liquidations);
                        }
                    }
                    if result.is_err() { break; }
                }
            }
            if result.is_err() {
                break;
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

    /// Plans a tracked-set change: at once while nothing is subscribed yet, otherwise after the settle delay
    /// (further changes inside the delay join the same application).
    fn schedule(&self, reconcile_at: &mut Option<Instant>, shards: &Shards) {
        if shards.assigned_count() == 0 {
            *reconcile_at = Some(Instant::now());
        } else if reconcile_at.is_none() {
            *reconcile_at = Some(Instant::now() + self.tuning.settle);
        }
    }

    fn wanted(
        &self,
        table: &watch::Receiver<Option<Arc<OkxTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
    ) -> BTreeSet<String> {
        match table.borrow().as_ref() {
            Some(t) => {
                let policy = policy.borrow().clone();
                t.tracked(&policy).map(|e| e.symbol.raw.clone()).collect()
            }
            None => BTreeSet::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn reconcile_or_stop(
        &self,
        shards: &Shards,
        table: &watch::Receiver<Option<Arc<OkxTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
        spawn: &mut dyn FnMut(usize, Arc<TopicControl>) -> Result<ShardParts, String>,
        quotes: &Quotes,
        result: &mut Result<(), ExchangeError>,
        stop: &CancellationToken,
    ) {
        match self.reconcile(shards, table, policy, spawn) {
            Ok(()) => {
                // State of instruments that left the tracked set is dropped.
                quotes.retain(|raw| shards.shard_of(raw).is_some());
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
        table: &watch::Receiver<Option<Arc<OkxTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
        spawn: &mut dyn FnMut(usize, Arc<TopicControl>) -> Result<ShardParts, String>,
    ) -> Result<(), String> {
        let wanted = self.wanted(table, policy);
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
        quotes: &Quotes,
        shards: &Shards,
        table: &watch::Receiver<Option<Arc<OkxTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
        dedup: &mut RecentKeys,
    ) -> Vec<Payload> {
        let status = &self.status;
        match output {
            WsOutput::Gap(gap) => {
                // The shard's instruments will push fresh values; whatever is stored is from before the loss.
                quotes.forget(shards.symbols_of(shard).iter());
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
                OkxStatus::inc(&status.ws_frames);
                let Some(text) = m.frame.as_text() else {
                    OkxStatus::inc(&status.unknown_frames);
                    return Vec::new();
                };
                match channel_of(text) {
                    Some(MARK_PRICE_CHANNEL) => {
                        self.handle_mark(text, m.recv_ts_ms, quotes, table, policy);
                        Vec::new()
                    }
                    Some(OPEN_INTEREST_CHANNEL) => {
                        self.handle_oi(text, quotes, table, policy);
                        Vec::new()
                    }
                    Some(LIQUIDATION_CHANNEL) => {
                        self.handle_liquidations(text, m.recv_ts_ms, table, policy, dedup)
                    }
                    _ => {
                        OkxStatus::inc(&status.unknown_frames);
                        Vec::new()
                    }
                }
            }
        }
    }

    fn handle_mark(
        &self,
        text: &str,
        recv_ts: i64,
        quotes: &Quotes,
        table: &watch::Receiver<Option<Arc<OkxTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
    ) {
        let status = &self.status;
        let updates = match parse_mark_price(text) {
            Ok(u) => u,
            Err(e) => {
                OkxStatus::inc(&status.mark_parse_errors);
                tracing::debug!(error = %e, "unparsable mark-price frame");
                return;
            }
        };
        for u in updates {
            if !is_tracked(table, policy, &u.inst_id) {
                OkxStatus::inc(&status.quotes_filtered);
                continue;
            }
            quotes.apply_mark(&u);
            OkxStatus::inc(&status.mark_frames);
            status.last_mark_ms.store(recv_ts, Ordering::Relaxed);
        }
    }

    fn handle_oi(
        &self,
        text: &str,
        quotes: &Quotes,
        table: &watch::Receiver<Option<Arc<OkxTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
    ) {
        let status = &self.status;
        let updates = match parse_open_interest(text) {
            Ok(u) => u,
            Err(e) => {
                OkxStatus::inc(&status.oi_parse_errors);
                tracing::debug!(error = %e, "unparsable open-interest frame");
                return;
            }
        };
        for u in updates {
            if u.bad_fields > 0 {
                status
                    .oi_parse_errors
                    .fetch_add(u.bad_fields as u64, Ordering::Relaxed);
            }
            if !is_tracked(table, policy, &u.inst_id) {
                OkxStatus::inc(&status.quotes_filtered);
                continue;
            }
            quotes.apply_oi(&u);
            OkxStatus::inc(&status.oi_frames);
        }
    }

    fn handle_liquidations(
        &self,
        text: &str,
        recv_ts: i64,
        table: &watch::Receiver<Option<Arc<OkxTable>>>,
        policy: &watch::Receiver<Arc<SymbolPolicy>>,
        dedup: &mut RecentKeys,
    ) -> Vec<Payload> {
        let status = &self.status;
        let records = match parse_liquidations(text) {
            Ok(r) => r,
            Err(e) => {
                OkxStatus::inc(&status.liquidation_parse_errors);
                tracing::warn!(error = %e, "unparsable liquidation-orders frame");
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
                    OkxStatus::inc(&status.liquidation_parse_errors);
                    tracing::warn!(error = %e, "unusable liquidation record");
                    continue;
                }
            };
            // The feed carries every swap of the exchange: most records are filtered here.
            let entry = current.as_ref().and_then(|t| t.get(&liq.inst_id));
            let Some(entry) = entry.filter(|e| policy.allows(&e.symbol.symbol)) else {
                OkxStatus::inc(&status.liquidations_filtered);
                continue;
            };
            if !dedup.insert(liq.dedup_key()) {
                OkxStatus::inc(&status.liquidations_duplicate);
                continue;
            }
            out.push(Payload::Liquidation(liquidation_event(
                &liq, &entry, recv_ts,
            )));
        }
        out
    }
}

fn is_tracked(
    table: &watch::Receiver<Option<Arc<OkxTable>>>,
    policy: &watch::Receiver<Arc<SymbolPolicy>>,
    raw: &str,
) -> bool {
    let t = table.borrow();
    match t.as_ref().and_then(|t| t.get(raw)) {
        Some(e) => policy.borrow().allows(&e.symbol.symbol),
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

/// Emits one `OpenInterest` per tracked instrument per interval from the quote state, spread over the interval.
struct Sampler {
    status: Arc<OkxStatus>,
    quotes: Arc<Quotes>,
    shards: Arc<Shards>,
    clock: Arc<dyn Clock>,
    interval: Duration,
    first_data_wait: Duration,
    funding_max_age: Duration,
    table: watch::Receiver<Option<Arc<OkxTable>>>,
    policy: watch::Receiver<Arc<SymbolPolicy>>,
    sink: mpsc::Sender<Event>,
    sink_closed: Arc<AtomicBool>,
    shutdown: CancellationToken,
}

impl Sampler {
    async fn run(self) {
        // So that the first samples carry data, wait for the first open interest push (bounded).
        tokio::select! {
            _ = self.shutdown.cancelled() => return,
            _ = timeout(self.first_data_wait, self.quotes.wait_first_oi()) => {}
        }
        loop {
            let cycle_start = Instant::now();
            let raws: Vec<String> = {
                let table = self.table.borrow().clone();
                let policy = self.policy.borrow().clone();
                table
                    .map(|t| t.tracked(&policy).map(|e| e.symbol.raw.clone()).collect())
                    .unwrap_or_default()
            };
            let n = raws.len() as u32;
            for (i, raw) in raws.iter().enumerate() {
                let at = cycle_start + self.interval * (i as u32) / n.max(1);
                tokio::select! {
                    _ = self.shutdown.cancelled() => return,
                    _ = sleep_until(at) => {}
                }
                if self.emit(raw).await.is_err() {
                    return;
                }
            }
            OkxStatus::inc(&self.status.oi_cycles);
            // An empty cycle must not spin.
            let next = cycle_start + self.interval.max(Duration::from_millis(100));
            tokio::select! {
                _ = self.shutdown.cancelled() => return,
                _ = sleep_until(next) => {}
            }
        }
    }

    async fn emit(&self, raw: &str) -> Result<(), ()> {
        let st = &self.status;
        // The table may have changed while the slot waited.
        let table = self.table.borrow().clone();
        let Some(table) = table else { return Ok(()) };
        let Some(entry) = table.get(raw) else {
            return Ok(());
        };
        if !self.policy.borrow().allows(&entry.symbol.symbol) {
            return Ok(());
        }
        // Data of a connection that is down is stale even if the state still holds it.
        let quote = self.quotes.get(raw).filter(|_| self.shards.is_healthy(raw));
        let Some((quote, oi)) = quote.and_then(|q| q.oi.map(|oi| (q, oi))) else {
            OkxStatus::inc(&st.oi_skipped);
            return Ok(());
        };
        if quote.mark.is_none() {
            OkxStatus::inc(&st.oi_without_mark);
        }
        // Cross-check of the two units OKX reports: coins against contracts times contract size.
        let expected = oi.oi * entry.size.ct_val * entry.size.ct_mult;
        if let Some(ccy) = oi.oi_ccy
            && expected > 0.0
            && (ccy - expected).abs() / expected > 0.01
        {
            OkxStatus::inc(&st.oi_unit_mismatch);
            tracing::warn!(
                symbol = raw,
                oi_ccy = ccy,
                expected,
                "oiCcy disagrees with oi x ctVal x ctMult"
            );
        }
        let values = match crate::normalize::oi::okx(
            oi.oi,
            oi.oi_ccy,
            oi.oi_usd,
            Some(entry.size),
            &entry.size.ct_val_ccy,
            quote.mark,
        ) {
            Ok(v) => v,
            Err(e) => {
                OkxStatus::inc(&st.oi_parse_errors);
                tracing::warn!(symbol = raw, error = %e, "unusable open interest state");
                return Ok(());
            }
        };
        let m = entry.symbol.multiplier;
        let now = self.clock.now_ms();
        let max_age = self.funding_max_age.as_millis() as i64;
        let funding = self.quotes.funding(raw, now, max_age);
        let index = funding::index_id_of(raw).and_then(|id| self.quotes.index(id, now, max_age));
        let event = OpenInterest {
            exchange: NAME.into(),
            symbol_raw: raw.to_string(),
            symbol: entry.symbol.symbol.clone(),
            exchange_ts: oi.ts,
            recv_ts: self.clock.now_ms(),
            oi_base: values.oi_base * m,
            oi_usd: values.oi_usd,
            // Per real coin, so that oi_base * mark_price == oi_usd.
            mark_price: quote.mark.map(|p| p / m),
            funding_rate: funding.map(|f| f.rate),
            // Per real coin like the mark price; the funding rate is a fraction.
            index_price: index.map(|i| i.price / m),
            next_funding_ts: funding.and_then(|f| f.next_ts),
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
        OkxStatus::inc(&st.oi_events);
        Ok(())
    }
}

impl Exchange for OkxConnector {
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
