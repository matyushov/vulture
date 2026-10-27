//! Open interest poller: one REST snapshot per symbol per interval, spread evenly, bounded concurrency.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_util::sync::CancellationToken;
use vulture_proto::v1::{Event, Gap, OpenInterest, event::Payload};

use super::BinanceTuning;
use super::http::{HttpClient, HttpError, encode_query_value};
use super::rate::{RateState, TokenBucket, budget_per_min, effective_interval};
use super::status::BinanceStatus;
use super::stream::MarkPrices;
use super::symbols::{SymbolEntry, SymbolTable};
use crate::symbols::SymbolPolicy;
use crate::ws::Clock;

/// REST path of one snapshot (weight 1, S-1).
pub const OPEN_INTEREST_PATH: &str = "/fapi/v1/openInterest";

/// `{"symbol":"BTCUSDT","openInterest":"91959.654","time":1792775123713}`
#[derive(Debug, Deserialize)]
struct OiResponse {
    symbol: String,
    #[serde(rename = "openInterest")]
    open_interest: String,
    time: i64,
}

/// Everything the poller needs.
pub struct OiPoller {
    pub http: Arc<dyn HttpClient>,
    pub rate: Arc<RateState>,
    pub status: Arc<BinanceStatus>,
    pub marks: Arc<MarkPrices>,
    pub clock: Arc<dyn Clock>,
    pub tuning: BinanceTuning,
    pub configured_interval_secs: u64,
    /// The universe.
    pub symbols: watch::Receiver<Option<Arc<SymbolTable>>>,
    /// The owner's policy; the tracked set is the universe filtered by it.
    pub policy: watch::Receiver<Arc<SymbolPolicy>>,
    pub sink: mpsc::Sender<Event>,
    /// Set when the sink is gone; the connector turns it into `ExchangeError::SinkClosed`.
    pub sink_closed: Arc<AtomicBool>,
    pub shutdown: CancellationToken,
}

enum Outcome {
    Done,
    /// Gave up on this symbol for the current cycle.
    Failed,
    Shutdown,
}

impl OiPoller {
    /// Runs until shutdown.
    ///
    /// Schedule: a cycle visits every symbol once. Slot `i` of a cycle is `i * interval / N` after the
    /// previous slot's time plus the slowdown stretch, so requests are evenly spread and never bursty. A
    /// request that is late (all permits busy) delays the slots after it rather than being followed by a
    /// catch-up burst. The tracked set (universe filtered by the policy), N and the interval are re-read at the
    /// start of every cycle; a symbol removed mid-cycle is skipped at once, an added one starts next cycle.
    pub async fn run(self) {
        let this = Arc::new(self);
        let semaphore = Arc::new(Semaphore::new(this.tuning.oi_concurrency.max(1)));
        // Hard cap at the budget rate; the even spacing normally stays below it.
        let bucket = Arc::new(TokenBucket::new(
            this.tuning.oi_burst,
            budget_per_min() / 60.0,
        ));
        let mut tasks: JoinSet<()> = JoinSet::new();
        let mut symbols = this.symbols.clone();
        let mut policy = this.policy.clone();
        // The first snapshots should carry `oi_usd` and `mark_price`: wait for the first mark price frame, but
        // never longer than `first_mark_wait`.
        tokio::select! {
            _ = this.marks.wait_first() => {}
            _ = sleep(this.tuning.first_mark_wait) => {
                tracing::warn!(wait = ?this.tuning.first_mark_wait, "no mark price yet; starting open interest polling without it");
            }
            _ = this.shutdown.cancelled() => return,
        }
        let mut next_at = Instant::now();

        'cycles: loop {
            let table = symbols.borrow_and_update().clone();
            let current = policy.borrow_and_update().clone();
            let tracked: Vec<SymbolEntry> = table
                .as_ref()
                .map(|t| t.tracked(&current).cloned().collect())
                .unwrap_or_default();
            if tracked.is_empty() {
                tokio::select! {
                    r = symbols.changed() => if r.is_err() { break 'cycles },
                    r = policy.changed() => if r.is_err() { break 'cycles },
                    _ = this.shutdown.cancelled() => break 'cycles,
                }
                continue;
            }
            let n = tracked.len();
            let interval = effective_interval(this.configured_interval_secs, n);
            let step = interval.div_f64(n as f64);
            this.status
                .oi_interval_ms
                .store(interval.as_millis() as u64, Ordering::Relaxed);
            for entry in &tracked {
                // Removed from the policy (or delisted) since the cycle began: do not poll it.
                if !this.still_tracked(entry) {
                    continue;
                }
                match this.rate.wait_unpaused(&this.shutdown).await {
                    None => break 'cycles,
                    Some(true) => next_at = Instant::now(),
                    Some(false) => {}
                }
                if !this.publish_pause_gaps(interval).await {
                    break 'cycles;
                }
                tokio::select! {
                    _ = sleep_until(next_at) => {}
                    _ = this.shutdown.cancelled() => break 'cycles,
                }
                // The wait for the slot may have been long: check again right before the request.
                if !this.still_tracked(entry) {
                    continue;
                }
                let permit = tokio::select! {
                    p = semaphore.clone().acquire_owned() => match p {
                        Ok(p) => p,
                        Err(_) => break 'cycles,
                    },
                    _ = this.shutdown.cancelled() => break 'cycles,
                };
                next_at = next_at.max(Instant::now()) + step.mul_f64(this.rate.slowdown());
                let task_this = this.clone();
                let bucket = bucket.clone();
                let entry = entry.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    match task_this.fetch_symbol(&entry, &bucket).await {
                        Outcome::Done | Outcome::Shutdown => {}
                        Outcome::Failed => BinanceStatus::inc(&task_this.status.oi_failures),
                    }
                });
                while tasks.try_join_next().is_some() {}
                if this.sink_closed.load(Ordering::Relaxed) {
                    break 'cycles;
                }
            }
            BinanceStatus::inc(&this.status.oi_cycles);
        }
        // Tasks end on their own: they watch the same shutdown token.
        while tasks.join_next().await.is_some() {}
    }

    /// Whether `entry` is still in the universe and allowed by the policy as of now.
    fn still_tracked(&self, entry: &SymbolEntry) -> bool {
        let listed = self
            .symbols
            .borrow()
            .as_ref()
            .is_some_and(|t| t.get(&entry.raw).is_some());
        listed && self.policy.borrow().allows(&entry.symbol)
    }

    /// Turns finished REST pauses into `Gap` events (`open_interest` stream): every 418 ban, and a 429 pause
    /// longer than one polling interval (shorter ones only delay a cycle). Returns `false` if the sink is gone.
    async fn publish_pause_gaps(&self, interval: Duration) -> bool {
        for span in self.rate.take_finished() {
            let long = span.to_ms - span.from_ms > interval.as_millis() as i64;
            if !(span.banned || long) {
                continue;
            }
            let gap = Gap {
                exchange: super::NAME.into(),
                stream: super::OPEN_INTEREST_STREAM.into(),
                from_ts: span.from_ms,
                to_ts: span.to_ms,
            };
            tracing::warn!(
                from_ts = gap.from_ts,
                to_ts = gap.to_ts,
                banned = span.banned,
                "open interest gap after a REST pause"
            );
            let event = Event {
                id: String::new(),
                payload: Some(Payload::Gap(gap)),
            };
            if self.sink.send(event).await.is_err() {
                self.sink_closed.store(true, Ordering::Relaxed);
                return false;
            }
            BinanceStatus::inc(&self.status.oi_gaps);
        }
        true
    }

    /// One symbol: up to `oi_max_attempts` tries with backoff; the pause rules of 429/418 apply to everyone.
    async fn fetch_symbol(&self, entry: &SymbolEntry, bucket: &TokenBucket) -> Outcome {
        let path = format!(
            "{OPEN_INTEREST_PATH}?symbol={}",
            encode_query_value(&entry.raw)
        );
        let mut backoff = self.tuning.oi_retry_backoff;
        for attempt in 1..=self.tuning.oi_max_attempts.max(1) {
            if attempt > 1 {
                BinanceStatus::inc(&self.status.oi_retries);
            }
            if self.rate.wait_unpaused(&self.shutdown).await.is_none() {
                return Outcome::Shutdown;
            }
            tokio::select! {
                _ = bucket.acquire() => {}
                _ = self.shutdown.cancelled() => return Outcome::Shutdown,
            }
            BinanceStatus::inc(&self.status.oi_requests);
            let result = tokio::select! {
                r = timeout(self.tuning.oi_request_timeout, self.http.get(&path)) => match r {
                    Ok(r) => r,
                    Err(_) => Err(HttpError::Timeout),
                },
                _ = self.shutdown.cancelled() => return Outcome::Shutdown,
            };
            let retry = match result {
                Err(e) => {
                    BinanceStatus::inc(&self.status.oi_network_errors);
                    tracing::debug!(symbol = %entry.raw, attempt, error = %e, "open interest request failed");
                    true
                }
                Ok(resp) => {
                    if let Some(w) = resp.used_weight_1m {
                        self.rate.record_weight(w);
                    }
                    match resp.status {
                        200 => {
                            return match self.publish(entry, &resp.body).await {
                                Ok(()) => Outcome::Done,
                                Err(()) => Outcome::Failed,
                            };
                        }
                        429 => {
                            BinanceStatus::inc(&self.status.http_429);
                            let pause = self.tuning.pause_429(resp.retry_after);
                            tracing::warn!(
                                ?pause,
                                "HTTP 429 from Binance REST; pausing all requests"
                            );
                            self.rate.pause(pause, false);
                            true
                        }
                        418 => {
                            BinanceStatus::inc(&self.status.http_418);
                            let pause = self.tuning.pause_418(resp.retry_after);
                            tracing::error!(
                                ?pause,
                                "HTTP 418 from Binance REST: IP banned; stopping requests"
                            );
                            self.rate.pause(pause, true);
                            // The ban outlasts any retry; give up on this symbol for the cycle.
                            return Outcome::Failed;
                        }
                        s if s >= 500 => true,
                        s => {
                            // 400 and friends (for example a symbol that just stopped trading): no point retrying.
                            tracing::debug!(symbol = %entry.raw, status = s, "open interest rejected");
                            return Outcome::Failed;
                        }
                    }
                }
            };
            if retry && attempt < self.tuning.oi_max_attempts {
                tokio::select! {
                    _ = sleep(jittered(backoff)) => {}
                    _ = self.shutdown.cancelled() => return Outcome::Shutdown,
                }
                backoff = backoff.saturating_mul(2);
            }
        }
        Outcome::Failed
    }

    async fn publish(&self, entry: &SymbolEntry, body: &str) -> Result<(), ()> {
        let recv_ts = self.clock.now_ms();
        let parsed: Result<OiResponse, _> = serde_json::from_str(body);
        let event = parsed
            .map_err(|e| e.to_string())
            .and_then(|r| self.build_event(entry, &r, recv_ts));
        match event {
            Ok(oi) => {
                let event = Event {
                    id: String::new(),
                    payload: Some(Payload::OpenInterest(oi)),
                };
                if self.sink.send(event).await.is_err() {
                    self.sink_closed.store(true, Ordering::Relaxed);
                    return Err(());
                }
                BinanceStatus::inc(&self.status.oi_events);
                Ok(())
            }
            Err(e) => {
                BinanceStatus::inc(&self.status.oi_parse_errors);
                tracing::warn!(symbol = %entry.raw, error = %e, "unusable open interest response");
                Err(())
            }
        }
    }

    fn build_event(
        &self,
        entry: &SymbolEntry,
        r: &OiResponse,
        recv_ts: i64,
    ) -> Result<OpenInterest, String> {
        if r.symbol != entry.raw {
            return Err(format!(
                "response for {:?}, asked for {:?}",
                r.symbol, entry.raw
            ));
        }
        let oi: f64 = r
            .open_interest
            .parse()
            .map_err(|_| format!("openInterest {:?} is not a number", r.open_interest))?;
        let max_age = self.tuning.mark_max_age.as_millis() as i64;
        let mark = self.marks.get_fresh(&entry.raw, recv_ts, max_age);
        if mark.is_none() {
            BinanceStatus::inc(&self.status.oi_without_mark);
        }
        let values = crate::normalize::oi::binance(oi, mark.map(|m| m.price), entry.multiplier)
            .map_err(|e| e.to_string())?;
        Ok(OpenInterest {
            exchange: "binance".into(),
            symbol_raw: entry.raw.clone(),
            symbol: entry.symbol.clone(),
            exchange_ts: r.time,
            recv_ts,
            oi_base: values.oi_base,
            oi_usd: values.oi_usd,
            // Per real coin, so that oi_base * mark_price == oi_usd; equals the exchange quote for multiplier 1.
            mark_price: mark.map(|m| m.price / entry.multiplier),
            // The index and the funding rate come with the same mark price frame; the index is per real coin
            // like the mark price, the rate is a fraction and needs no conversion.
            funding_rate: mark.and_then(|m| m.funding),
            index_price: mark.and_then(|m| m.index).map(|i| i / entry.multiplier),
            next_funding_ts: mark.and_then(|m| m.next_funding_ts),
        })
    }
}

/// +/- 20% so that retries of many symbols do not line up.
fn jittered(d: Duration) -> Duration {
    d.mul_f64(0.8 + rand::random::<f64>() * 0.4)
}
