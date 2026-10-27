//! Funding rate and index price of OKX swaps, by slow REST polling (task V3b, B1).
//!
//! The data exists on public WebSocket channels (`funding-rate` per instrument, `index-tickers` per index), but
//! subscribing to them would add one or two topics per instrument to the 2 now used (`mark-price`,
//! `open-interest`): with about 700 swaps that is 1400 topics on one connection today and 2100 to 2800 after, past
//! the 2000 per connection the connector allows, so it would cost a second connection and several times the
//! subscription requests of the hourly budget at every start. The REST calls cost nothing of either budget:
//!
//! - `GET /api/v5/market/index-tickers?quoteCcy=USDT` (and `USDC`): every index in one response (`instId` such as
//!   `BTC-USDT`, `idxPx`).
//! - `GET /api/v5/public/funding-rate?instId=ANY`: the funding rate of all swaps in one response, when the exchange
//!   supports `ANY`. If it answers with an error or nothing, the poll falls back to one request per tracked
//!   instrument, spread over the interval (at most 2 requests per second).
//!
//! Every `OkxTuning::funding_poll_interval` (default 5 minutes) that is at most 3 requests, or 1 + N/interval
//! per second in the fallback. A value older than three intervals is not attached to a sample. The funding rate
//! changes slowly (a settlement every 8 hours at most 1 hour apart) so the cadence loses nothing.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::watch;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_util::sync::CancellationToken;

use super::OkxTuning;
use super::state::{FundingState, IndexState, Quotes};
use super::status::OkxStatus;
use super::table::OkxTable;
use crate::exchange::binance::HttpClient;
use crate::symbols::SymbolPolicy;
use crate::ws::Clock;

/// Funding rates of every swap in one response.
pub const FUNDING_RATE_ALL_PATH: &str = "/api/v5/public/funding-rate?instId=ANY";
/// Index prices of the USDT quoted indexes.
pub const INDEX_TICKERS_USDT_PATH: &str = "/api/v5/market/index-tickers?quoteCcy=USDT";
/// Index prices of the USDC quoted indexes.
pub const INDEX_TICKERS_USDC_PATH: &str = "/api/v5/market/index-tickers?quoteCcy=USDC";

/// REST path of one instrument's funding rate (the fallback).
pub fn funding_rate_path(inst_id: &str) -> String {
    format!(
        "/api/v5/public/funding-rate?instId={}",
        crate::exchange::binance::encode_query_value(inst_id)
    )
}

/// The index an instrument is priced from: `BTC-USDT-SWAP` is `BTC-USDT`.
pub fn index_id_of(inst_id: &str) -> Option<&str> {
    inst_id.strip_suffix("-SWAP")
}

fn envelope(body: &str) -> Result<Vec<Value>, String> {
    let v: Value = serde_json::from_str(body).map_err(|e| format!("not JSON: {e}"))?;
    let code = v["code"].as_str().unwrap_or("");
    if code != "0" {
        return Err(format!(
            "code {code}: {}",
            v["msg"].as_str().unwrap_or_default()
        ));
    }
    Ok(v["data"].as_array().cloned().unwrap_or_default())
}

fn num(v: &Value, key: &str) -> Option<f64> {
    v[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|x| x.is_finite())
}

/// A parsed funding record.
#[derive(Debug, Clone, PartialEq)]
pub struct FundingRecord {
    /// `instId`.
    pub inst_id: String,
    /// `fundingRate`, a fraction.
    pub rate: f64,
    /// `nextFundingTime`, ms, or `fundingTime` when only that is given.
    pub next_ts: Option<i64>,
}

/// Parses a `funding-rate` response. Records without a rate (contracts without funding) are left out.
pub fn parse_funding_rates(body: &str) -> Result<Vec<FundingRecord>, String> {
    Ok(envelope(body)?
        .iter()
        .filter_map(|r| {
            let inst_id = r["instId"].as_str()?.to_string();
            let rate = num(r, "fundingRate")?;
            let next_ts = ["nextFundingTime", "fundingTime"]
                .iter()
                .find_map(|k| r[*k].as_str().and_then(|s| s.parse::<i64>().ok()))
                .filter(|t| *t > 0);
            Some(FundingRecord {
                inst_id,
                rate,
                next_ts,
            })
        })
        .collect())
}

/// Parses an `index-tickers` response into `(index id, price)`.
pub fn parse_index_tickers(body: &str) -> Result<Vec<(String, f64)>, String> {
    Ok(envelope(body)?
        .iter()
        .filter_map(|r| {
            let id = r["instId"].as_str()?.to_string();
            let px = num(r, "idxPx").filter(|p| *p > 0.0)?;
            Some((id, px))
        })
        .collect())
}

/// The poller task.
pub struct FundingPoller {
    pub http: Arc<dyn HttpClient>,
    pub status: Arc<OkxStatus>,
    pub quotes: Arc<Quotes>,
    pub clock: Arc<dyn Clock>,
    pub tuning: OkxTuning,
    pub table: watch::Receiver<Option<Arc<OkxTable>>>,
    pub policy: watch::Receiver<Arc<SymbolPolicy>>,
    pub shutdown: CancellationToken,
}

enum Fetched {
    Body(String),
    Failed,
}

impl FundingPoller {
    /// Runs until shutdown.
    pub async fn run(mut self) {
        // Nothing to attach the values to before the instrument list is known.
        loop {
            if self.table.borrow().is_some() {
                break;
            }
            tokio::select! {
                r = self.table.changed() => if r.is_err() { return },
                _ = self.shutdown.cancelled() => return,
            }
        }
        let interval = self.tuning.funding_poll_interval;
        loop {
            let started = Instant::now();
            let throttled = self.cycle().await;
            let wait = if throttled {
                self.tuning.ban_pause.max(interval)
            } else {
                interval
            };
            tokio::select! {
                _ = sleep_until(started + wait) => {}
                _ = self.shutdown.cancelled() => return,
            }
        }
    }

    async fn get(&self, path: &str) -> Fetched {
        OkxStatus::inc(&self.status.funding_requests);
        let result = timeout(self.tuning.info_timeout, self.http.get(path)).await;
        let response = match result {
            Ok(Ok(r)) => r,
            _ => {
                OkxStatus::inc(&self.status.funding_errors);
                return Fetched::Failed;
            }
        };
        match response.status {
            200 => Fetched::Body(response.body),
            403 => {
                OkxStatus::inc(&self.status.http_403);
                OkxStatus::inc(&self.status.funding_errors);
                self.status.paused_until_ms.store(
                    self.clock.now_ms() + self.tuning.ban_pause.as_millis() as i64,
                    Ordering::Relaxed,
                );
                Fetched::Failed
            }
            429 => {
                OkxStatus::inc(&self.status.http_429);
                OkxStatus::inc(&self.status.funding_errors);
                Fetched::Failed
            }
            _ => {
                OkxStatus::inc(&self.status.funding_errors);
                Fetched::Failed
            }
        }
    }

    /// One poll cycle. Returns `true` when the exchange throttled us (the caller waits longer).
    async fn cycle(&self) -> bool {
        let before_throttle = self.status.http_403.load(Ordering::Relaxed)
            + self.status.http_429.load(Ordering::Relaxed);
        // Index prices.
        for path in [INDEX_TICKERS_USDT_PATH, INDEX_TICKERS_USDC_PATH] {
            if let Fetched::Body(body) = self.get(path).await {
                match parse_index_tickers(&body) {
                    Ok(rows) => {
                        let now = self.clock.now_ms();
                        for (id, price) in rows {
                            self.quotes.apply_index(
                                &id,
                                IndexState {
                                    price,
                                    fetched_ms: now,
                                },
                            );
                        }
                    }
                    Err(e) => {
                        OkxStatus::inc(&self.status.funding_errors);
                        tracing::debug!(error = %e, "index tickers unusable");
                    }
                }
            }
            if self.shutdown.is_cancelled() {
                return false;
            }
        }
        // Funding rates: all at once, else one by one.
        let mut stored = 0u64;
        if let Fetched::Body(body) = self.get(FUNDING_RATE_ALL_PATH).await {
            match parse_funding_rates(&body) {
                Ok(rows) => {
                    let now = self.clock.now_ms();
                    stored = rows.len() as u64;
                    for r in rows {
                        self.quotes.apply_funding(
                            &r.inst_id,
                            FundingState {
                                rate: r.rate,
                                next_ts: r.next_ts,
                                fetched_ms: now,
                            },
                        );
                    }
                }
                Err(e) => {
                    OkxStatus::inc(&self.status.funding_errors);
                    tracing::debug!(error = %e, "funding rates for all instruments unusable");
                }
            }
        }
        if stored == 0 && !self.shutdown.is_cancelled() {
            stored = self.one_by_one().await;
        }
        self.status
            .funding_instruments
            .store(stored, Ordering::Relaxed);
        self.status.http_403.load(Ordering::Relaxed) + self.status.http_429.load(Ordering::Relaxed)
            > before_throttle
    }

    async fn one_by_one(&self) -> u64 {
        OkxStatus::inc(&self.status.funding_fallbacks);
        let ids: Vec<String> = {
            let table = self.table.borrow().clone();
            let policy = self.policy.borrow().clone();
            table
                .map(|t| t.tracked(&policy).map(|e| e.symbol.raw.clone()).collect())
                .unwrap_or_default()
        };
        if ids.is_empty() {
            return 0;
        }
        // At most 2 requests a second (the documented limit is 10 per 2 s), within the poll interval.
        let spacing = (self.tuning.funding_poll_interval / ids.len() as u32)
            .max(self.tuning.funding_min_spacing);
        let mut stored = 0;
        for id in ids {
            if let Fetched::Body(body) = self.get(&funding_rate_path(&id)).await {
                if let Ok(rows) = parse_funding_rates(&body) {
                    let now = self.clock.now_ms();
                    for r in rows.into_iter().filter(|r| r.inst_id == id) {
                        self.quotes.apply_funding(
                            &id,
                            FundingState {
                                rate: r.rate,
                                next_ts: r.next_ts,
                                fetched_ms: now,
                            },
                        );
                        stored += 1;
                    }
                } else {
                    OkxStatus::inc(&self.status.funding_errors);
                }
            }
            tokio::select! {
                _ = sleep(spacing.min(Duration::from_secs(60))) => {}
                _ = self.shutdown.cancelled() => return stored,
            }
        }
        stored
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Shapes from the OKX documentation (Get funding rate; Get index tickers).
    const FUNDING: &str = r#"{"code":"0","data":[{"formulaType":"noRate","fundingRate":"0.0000515300000000","fundingTime":"1700236800000","impactValue":"","instId":"BTC-USDT-SWAP","instType":"SWAP","interestRate":"","maxFundingRate":"0.00375","method":"current_period","minFundingRate":"-0.00375","nextFundingRate":"","nextFundingTime":"1700265600000","premium":"0.00005","settFundingRate":"","settState":"","ts":"1700236000000"},{"fundingRate":"","instId":"NOFUND-USDT-SWAP","nextFundingTime":""}],"msg":""}"#;
    const INDEX: &str = r#"{"code":"0","msg":"","data":[{"instId":"BTC-USDT","idxPx":"43350.1","high24h":"43999","sodUtc0":"43000","open24h":"43100","low24h":"42800","lastUtc8":"43350","sodUtc8":"43100","ts":"1700236000000"},{"instId":"BAD-USDT","idxPx":"0","ts":"1"}]}"#;

    #[test]
    fn funding_records_parse() {
        let rows = parse_funding_rates(FUNDING).unwrap();
        assert_eq!(rows.len(), 1, "an instrument without a rate is left out");
        assert_eq!(rows[0].inst_id, "BTC-USDT-SWAP");
        assert!((rows[0].rate - 0.00005153).abs() < 1e-12);
        assert_eq!(rows[0].next_ts, Some(1_700_265_600_000));
        let e = parse_funding_rates(
            r#"{"code":"51001","msg":"Instrument ID does not exist","data":[]}"#,
        )
        .unwrap_err();
        assert!(e.contains("51001"), "{e}");
        assert!(parse_funding_rates("<html>").is_err());
        assert_eq!(
            parse_funding_rates(r#"{"code":"0","data":[],"msg":""}"#)
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn index_tickers_parse() {
        assert_eq!(
            parse_index_tickers(INDEX).unwrap(),
            vec![("BTC-USDT".to_string(), 43350.1)]
        );
        assert!(parse_index_tickers(r#"{"code":"50011","msg":"Too Many Requests"}"#).is_err());
    }

    #[test]
    fn instruments_map_to_their_index() {
        assert_eq!(index_id_of("BTC-USDT-SWAP"), Some("BTC-USDT"));
        assert_eq!(index_id_of("ETH-USDC-SWAP"), Some("ETH-USDC"));
        assert_eq!(index_id_of("BTC-USD-250328"), None);
        assert_eq!(
            funding_rate_path("BTC-USDT-SWAP"),
            "/api/v5/public/funding-rate?instId=BTC-USDT-SWAP"
        );
    }
}
