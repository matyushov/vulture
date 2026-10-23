//! Binance USDT-M futures connector (V1-4).
//!
//! - Liquidations: one WebSocket to `wss://fstream.binance.com/market/ws` (the legacy `/ws` route accepts the
//!   subscription and then stays silent, S-1) with `!forceOrder@arr` and `!markPrice@arr@1s`. The mark price
//!   stream is also the liveness stream for the watchdog of [`crate::ws`].
//! - Mark price: kept in memory only, never published; used for `oi_usd` of the REST snapshots.
//! - Open interest: REST `/fapi/v1/openInterest` for every symbol, spread over the polling interval
//!   ([`oi`]), with rate control ([`rate`]).
//! - Symbols: `exchangeInfo`, reloaded hourly ([`info`], [`symbols`]).

mod http;
mod info;
mod oi;
mod rate;
mod status;
mod stream;
mod symbols;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::watch;
use tokio::task::JoinSet;
use vulture_proto::v1::{Event, event::Payload};

pub use http::{HttpClient, HttpError, HttpResponse, ReqwestClient, encode_query_value};
pub use info::EXCHANGE_INFO_PATH;
pub use oi::OPEN_INTEREST_PATH;
pub use rate::{
    BUDGET_FRACTION, MAX_SLOWDOWN, WEIGHT_LIMIT_PER_MIN, effective_interval, slowdown_factor,
};
pub use status::{BinanceStatus, WsLink};
pub use stream::{
    BinanceHandler, ForceOrder, MarkEntry, MarkPrices, ParseError, STREAMS, liquidation_event,
    parse_force_order,
};
pub use symbols::{BuildReport, SymbolDiff, SymbolEntry, SymbolTable};

use self::info::InfoLoader;
use self::oi::OiPoller;
use self::rate::RateState;
use super::{EventSink, Exchange, ExchangeError};
use crate::app::AppContext;
use crate::config::ExchangeConfig;
use crate::ws::{
    Clock, SystemClock, TungsteniteConnector, WsConnector, WsOutput, WsProfile, spawn_ws,
};

/// Exchange name in the configuration and in events.
pub const NAME: &str = "binance";
/// Default WebSocket endpoint (the `/market` route carries liquidations and mark price).
pub const DEFAULT_WS_URL: &str = "wss://fstream.binance.com/market/ws";
/// Default REST endpoint.
pub const DEFAULT_REST_URL: &str = "https://fapi.binance.com";
/// `Gap.stream` / profile stream name for the WebSocket data.
pub const LIQUIDATION_STREAM: &str = "liquidations";

/// What the `[[exchanges]]` entry says, with defaults applied.
#[derive(Debug, Clone, PartialEq)]
pub struct BinanceConfig {
    /// Whitelist of normalized symbols; empty means all.
    pub symbols: Vec<String>,
    /// Configured open interest interval, seconds (a floor; see [`effective_interval`]).
    pub oi_poll_interval_secs: u64,
    /// WebSocket URL.
    pub ws_url: String,
    /// REST base URL.
    pub rest_url: String,
}

impl BinanceConfig {
    /// Takes the Binance-relevant fields of a configuration entry.
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
pub struct BinanceTuning {
    /// Requests in flight at once. S-1: 30 s interval at ~560 symbols needs about 7 in flight at p50 latency
    /// and 26 at p99; 12 covers p50 with slack, and the budget stays bounded by the token bucket.
    pub oi_concurrency: usize,
    /// Token bucket burst (the bucket refills at the 60% budget rate, 24 per second).
    pub oi_burst: u32,
    /// Deadline of one open interest request (S-1 p99 was 1.4 s from a distant machine).
    pub oi_request_timeout: Duration,
    /// Attempts per symbol per cycle (network error, timeout, 5xx, 429 are retried).
    pub oi_max_attempts: u32,
    /// First retry delay; doubles, +/- 20% jitter.
    pub oi_retry_backoff: Duration,
    /// A mark price older than this is not used for `oi_usd`.
    pub mark_max_age: Duration,
    /// Pause after 429 without `Retry-After`.
    pub default_429_pause: Duration,
    /// Cap of a 429 pause.
    pub max_429_pause: Duration,
    /// Pause after 418 without `Retry-After` (bans last from 2 minutes to 3 days).
    pub default_418_pause: Duration,
    /// Cap of a 418 pause.
    pub max_418_pause: Duration,
    /// Regular `exchangeInfo` reload.
    pub info_reload_interval: Duration,
    /// First retry delay after a failed `exchangeInfo` load; doubles up to `info_retry`.
    pub info_initial_retry: Duration,
    /// Longest wait before retrying a failed `exchangeInfo` load.
    pub info_retry: Duration,
    /// Deadline of `exchangeInfo` (1.2 MB; S-1 saw 0.55 s and 7.3 s).
    pub info_timeout: Duration,
}

impl Default for BinanceTuning {
    fn default() -> Self {
        Self {
            oi_concurrency: 12,
            oi_burst: 8,
            oi_request_timeout: Duration::from_secs(5),
            oi_max_attempts: 3,
            oi_retry_backoff: Duration::from_millis(250),
            mark_max_age: Duration::from_secs(30),
            default_429_pause: Duration::from_secs(60),
            max_429_pause: Duration::from_secs(5 * 60),
            default_418_pause: Duration::from_secs(5 * 60),
            max_418_pause: Duration::from_secs(3 * 24 * 3600),
            info_reload_interval: Duration::from_secs(3600),
            info_initial_retry: Duration::from_secs(5),
            info_retry: Duration::from_secs(60),
            info_timeout: Duration::from_secs(30),
        }
    }
}

impl BinanceTuning {
    pub(crate) fn pause_429(&self, retry_after: Option<Duration>) -> Duration {
        retry_after
            .unwrap_or(self.default_429_pause)
            .min(self.max_429_pause)
    }

    pub(crate) fn pause_418(&self, retry_after: Option<Duration>) -> Duration {
        retry_after
            .unwrap_or(self.default_418_pause)
            .min(self.max_418_pause)
    }
}

/// The connector.
pub struct BinanceConnector {
    cfg: BinanceConfig,
    tuning: BinanceTuning,
    ws: Arc<dyn WsConnector>,
    http: Arc<dyn HttpClient>,
    clock: Arc<dyn Clock>,
    status: Arc<BinanceStatus>,
    profile_hook: Option<fn(&mut WsProfile)>,
}

impl BinanceConnector {
    /// Production connector: real sockets, system clock, default tuning.
    pub fn new(cfg: BinanceConfig) -> Result<Self, String> {
        let http = ReqwestClient::new(&cfg.rest_url)?;
        Ok(Self::with_transport(
            cfg,
            BinanceTuning::default(),
            Arc::new(TungsteniteConnector),
            Arc::new(http),
            Arc::new(SystemClock),
        ))
    }

    /// Connector with injected transports (tests).
    pub fn with_transport(
        cfg: BinanceConfig,
        tuning: BinanceTuning,
        ws: Arc<dyn WsConnector>,
        http: Arc<dyn HttpClient>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            cfg,
            tuning,
            ws,
            http,
            clock,
            status: Arc::new(BinanceStatus::default()),
            profile_hook: None,
        }
    }

    /// Lets tests adjust the WebSocket profile (shorter ages, fixed jitter).
    pub fn with_profile_hook(mut self, hook: fn(&mut WsProfile)) -> Self {
        self.profile_hook = Some(hook);
        self
    }

    /// Counters and state, for metrics (V1-9) and tests.
    pub fn status(&self) -> Arc<BinanceStatus> {
        self.status.clone()
    }

    fn profile(&self) -> WsProfile {
        // Binance sends a ping every 3 minutes and closes the socket after 24 hours: no client ping, planned
        // make-before-break switch at 23 h 30 min +/- 10 min. Silence is judged on the mark price stream.
        let mut p =
            WsProfile::new(NAME, LIQUIDATION_STREAM, self.cfg.ws_url.clone()).with_binance_age();
        if let Some(hook) = self.profile_hook {
            hook(&mut p);
        }
        p
    }

    async fn run_inner(&self, ctx: AppContext, sink: EventSink) -> Result<(), ExchangeError> {
        let status = self.status.clone();
        let stop = ctx.shutdown.child_token();
        let sink_closed = Arc::new(AtomicBool::new(false));
        let marks = Arc::new(MarkPrices::default());
        let rate = RateState::new(status.clone(), self.clock.clone());
        let (table_tx, table_rx) = watch::channel::<Option<Arc<SymbolTable>>>(None);

        let (ws_handle, mut ws_rx) = spawn_ws(
            self.profile(),
            Arc::new(BinanceHandler),
            self.ws.clone(),
            self.clock.clone(),
            stop.clone(),
        )
        .map_err(|message| ExchangeError::Config {
            exchange: NAME.into(),
            message,
        })?;
        let _ = status.ws.set(WsLink {
            counters: ws_handle.counters.clone(),
            status: ws_handle.status.clone(),
        });

        let mut tasks = JoinSet::new();
        tasks.spawn(
            InfoLoader {
                http: self.http.clone(),
                rate: rate.clone(),
                status: status.clone(),
                aliases: ctx.aliases.clone(),
                whitelist: self.cfg.symbols.clone(),
                tuning: self.tuning.clone(),
                tx: table_tx,
                shutdown: stop.clone(),
            }
            .run(),
        );
        tasks.spawn(
            OiPoller {
                http: self.http.clone(),
                rate: rate.clone(),
                status: status.clone(),
                marks: marks.clone(),
                clock: self.clock.clone(),
                tuning: self.tuning.clone(),
                configured_interval_secs: self.cfg.oi_poll_interval_secs,
                symbols: table_rx.clone(),
                sink: sink.clone(),
                sink_closed: sink_closed.clone(),
                shutdown: stop.clone(),
            }
            .run(),
        );

        let mut result = Ok(());
        loop {
            let output = tokio::select! {
                o = ws_rx.recv() => match o {
                    Some(o) => o,
                    None => break,
                },
                _ = stop.cancelled() => break,
            };
            if sink_closed.load(Ordering::Relaxed) {
                result = Err(ExchangeError::SinkClosed(NAME.into()));
                break;
            }
            let event = match output {
                WsOutput::Gap(gap) => {
                    BinanceStatus::inc(&status.gaps);
                    tracing::warn!(
                        from_ts = gap.from_ts,
                        to_ts = gap.to_ts,
                        "liquidation stream gap"
                    );
                    Some(Payload::Gap(gap))
                }
                WsOutput::Data(m) => {
                    BinanceStatus::inc(&status.ws_frames);
                    let Some(text) = m.frame.as_text() else {
                        BinanceStatus::inc(&status.unknown_frames);
                        continue;
                    };
                    self.handle_text(text, m.recv_ts_ms, &marks, &table_rx, &ctx)
                        .map(Payload::Liquidation)
                }
            };
            if let Some(payload) = event {
                let is_liq = matches!(payload, Payload::Liquidation(_));
                if sink
                    .send(Event {
                        id: String::new(),
                        payload: Some(payload),
                    })
                    .await
                    .is_err()
                {
                    result = Err(ExchangeError::SinkClosed(NAME.into()));
                    break;
                }
                if is_liq {
                    BinanceStatus::inc(&status.liquidations);
                }
            }
        }
        if result.is_ok() && sink_closed.load(Ordering::Relaxed) {
            result = Err(ExchangeError::SinkClosed(NAME.into()));
        }
        stop.cancel();
        ws_handle.join().await;
        while tasks.join_next().await.is_some() {}
        result
    }

    /// Mark price frames update the map; `forceOrder` frames become liquidations.
    fn handle_text(
        &self,
        text: &str,
        recv_ts: i64,
        marks: &MarkPrices,
        table: &watch::Receiver<Option<Arc<SymbolTable>>>,
        ctx: &AppContext,
    ) -> Option<vulture_proto::v1::Liquidation> {
        let status = &self.status;
        if text.trim_start().starts_with('[') {
            match marks.apply_frame(text, recv_ts) {
                Ok((_, rejected)) => {
                    BinanceStatus::inc(&status.mark_frames);
                    if rejected > 0 {
                        status
                            .mark_parse_errors
                            .fetch_add(rejected as u64, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    BinanceStatus::inc(&status.mark_parse_errors);
                    tracing::debug!(error = %e, "unparsable mark price frame");
                }
            }
            return None;
        }
        if !text.contains("\"forceOrder\"") {
            BinanceStatus::inc(&status.unknown_frames);
            return None;
        }
        let order = match parse_force_order(text) {
            Ok(o) => o,
            Err(e) => {
                BinanceStatus::inc(&status.liquidation_parse_errors);
                tracing::warn!(error = %e, "unparsable forceOrder frame");
                return None;
            }
        };
        let current = table.borrow().clone();
        let entry = match current {
            Some(t) => t.get(&order.symbol).cloned(),
            // The first seconds after start, before `exchangeInfo` has arrived: strict name parsing only.
            None => symbols::fallback_entry(&order.symbol, &ctx.aliases),
        };
        match entry {
            Some(entry) => Some(liquidation_event(&order, &entry, recv_ts)),
            None => {
                BinanceStatus::inc(&status.liquidations_filtered);
                None
            }
        }
    }
}

impl Exchange for BinanceConnector {
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
}
