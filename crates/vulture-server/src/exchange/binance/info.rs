//! `exchangeInfo` loader: initial load, hourly reload, diff logging, publication of the symbol table.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::watch;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::BinanceTuning;
use super::http::{HttpClient, HttpError};
use super::rate::RateState;
use super::status::BinanceStatus;
use super::symbols::SymbolTable;
use crate::normalize::{BaseAliases, parse_binance_exchange_info};

/// REST path of the symbol list (weight 1, S-1).
pub const EXCHANGE_INFO_PATH: &str = "/fapi/v1/exchangeInfo";

/// Inputs of the loader task.
pub struct InfoLoader {
    pub http: Arc<dyn HttpClient>,
    pub rate: Arc<RateState>,
    pub status: Arc<BinanceStatus>,
    pub aliases: Arc<BaseAliases>,
    pub whitelist: Vec<String>,
    pub tuning: BinanceTuning,
    pub tx: watch::Sender<Option<Arc<SymbolTable>>>,
    pub shutdown: CancellationToken,
}

impl InfoLoader {
    /// Runs until shutdown: load, publish, sleep an hour (or retry sooner after a failure).
    pub async fn run(self) {
        let mut failures: u32 = 0;
        loop {
            let delay = match self.load_once().await {
                Ok(()) => {
                    failures = 0;
                    self.tuning.info_reload_interval
                }
                Err(e) => {
                    failures += 1;
                    BinanceStatus::inc(&self.status.info_errors);
                    // 5 s, 10 s, 20 s ... up to the regular retry delay.
                    let delay = self
                        .tuning
                        .info_initial_retry
                        .saturating_mul(1 << failures.saturating_sub(1).min(8))
                        .min(self.tuning.info_retry);
                    tracing::warn!(error = %e, retry_in = ?delay, "exchangeInfo load failed");
                    delay
                }
            };
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = self.shutdown.cancelled() => return,
            }
        }
    }

    async fn load_once(&self) -> Result<(), String> {
        if self.rate.wait_unpaused(&self.shutdown).await.is_none() {
            return Ok(());
        }
        let response =
            match timeout(self.tuning.info_timeout, self.http.get(EXCHANGE_INFO_PATH)).await {
                Ok(r) => r,
                Err(_) => Err(HttpError::Timeout),
            }
            .map_err(|e| e.to_string())?;
        if let Some(w) = response.used_weight_1m {
            self.rate.record_weight(w);
        }
        match response.status {
            200 => {}
            429 => {
                BinanceStatus::inc(&self.status.http_429);
                self.rate
                    .pause(self.tuning.pause_429(response.retry_after), false);
                return Err("HTTP 429".into());
            }
            418 => {
                BinanceStatus::inc(&self.status.http_418);
                self.rate
                    .pause(self.tuning.pause_418(response.retry_after), true);
                return Err("HTTP 418 (IP banned)".into());
            }
            other => return Err(format!("HTTP {other}")),
        }
        let metas = parse_binance_exchange_info(&response.body).map_err(|e| e.to_string())?;
        let (table, report) = SymbolTable::build(&metas, &self.aliases, &self.whitelist);
        if table.is_empty() && metas.is_empty() {
            return Err("exchangeInfo contains no symbols".into());
        }
        for raw in &report.renamed_on_collision {
            tracing::warn!(
                symbol = %raw,
                "normalized name collides with another symbol; kept under its exchange-side name"
            );
        }
        for raw in &report.skipped {
            tracing::warn!(symbol = %raw, "symbol skipped: cannot be normalized unambiguously");
        }
        self.status
            .alias_collisions
            .store(report.renamed_on_collision.len() as u64, Ordering::Relaxed);
        self.status
            .symbols
            .store(table.len() as u64, Ordering::Relaxed);
        BinanceStatus::inc(&self.status.info_loads);

        let previous = self.tx.borrow().clone();
        match previous {
            None => tracing::info!(symbols = table.len(), "exchangeInfo loaded"),
            Some(old) => {
                let diff = old.diff(&table);
                if diff.is_empty() {
                    tracing::debug!(symbols = table.len(), "exchangeInfo reloaded, no changes");
                } else {
                    tracing::info!(
                        symbols = table.len(),
                        added = diff.added.len(),
                        removed = diff.removed.len(),
                        added_symbols = %preview(&diff.added),
                        removed_symbols = %preview(&diff.removed),
                        "symbol set changed (listings, delistings, settling)"
                    );
                }
            }
        }
        self.tx.send_replace(Some(Arc::new(table)));
        Ok(())
    }
}

fn preview(list: &[String]) -> String {
    const MAX: usize = 20;
    let mut s = list.iter().take(MAX).cloned().collect::<Vec<_>>().join(",");
    if list.len() > MAX {
        s.push_str(&format!(",...(+{})", list.len() - MAX));
    }
    s
}
