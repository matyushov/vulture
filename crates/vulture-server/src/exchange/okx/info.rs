//! Instrument list loader: REST load, hourly reload, diff logging, publication of the table.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::status::OkxStatus;
use super::table::OkxTable;
use super::{NAME, OkxTuning};
use crate::exchange::binance::{HttpClient, HttpError};
use crate::normalize::{BaseAliases, parse_okx_instruments};
use crate::symbols::SymbolChannel;
use crate::ws::Clock;

/// REST path of the swap instrument list. One response holds them all (no pagination).
pub const INSTRUMENTS_PATH: &str = "/api/v5/public/instruments?instType=SWAP";

/// Failure of one load.
#[derive(Debug)]
pub(super) enum LoadError {
    /// A 403 or 429.
    Throttled(u16, Option<Duration>),
    /// Anything else.
    Failed(String),
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Throttled(code, _) => write!(f, "HTTP {code}"),
            LoadError::Failed(m) => f.write_str(m),
        }
    }
}

/// Inputs of the loader task.
pub struct InfoLoader {
    pub http: Arc<dyn HttpClient>,
    pub status: Arc<OkxStatus>,
    pub aliases: Arc<BaseAliases>,
    pub symbols: Arc<SymbolChannel>,
    pub tuning: OkxTuning,
    pub clock: Arc<dyn Clock>,
    pub tx: watch::Sender<Option<Arc<OkxTable>>>,
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
                    OkxStatus::inc(&self.status.info_errors);
                    let mut delay = self
                        .tuning
                        .info_initial_retry
                        .saturating_mul(1 << failures.saturating_sub(1).min(8))
                        .min(self.tuning.info_retry);
                    if let LoadError::Throttled(code, retry_after) = &e {
                        let floor = if *code == 403 {
                            self.tuning.ban_pause
                        } else {
                            self.tuning.info_retry
                        };
                        delay = delay.max(floor).max(retry_after.unwrap_or_default());
                        self.status.paused_until_ms.store(
                            self.clock.now_ms() + delay.as_millis() as i64,
                            Ordering::Relaxed,
                        );
                    }
                    tracing::warn!(error = %e, retry_in = ?delay, "instrument list load failed");
                    delay
                }
            };
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = self.shutdown.cancelled() => return,
            }
            self.status.paused_until_ms.store(0, Ordering::Relaxed);
        }
    }

    async fn fetch(&self) -> Result<crate::normalize::OkxInstruments, LoadError> {
        let response =
            match timeout(self.tuning.info_timeout, self.http.get(INSTRUMENTS_PATH)).await {
                Ok(r) => r,
                Err(_) => Err(HttpError::Timeout),
            }
            .map_err(|e| LoadError::Failed(e.to_string()))?;
        match response.status {
            200 => {}
            403 => {
                OkxStatus::inc(&self.status.http_403);
                return Err(LoadError::Throttled(403, response.retry_after));
            }
            429 => {
                OkxStatus::inc(&self.status.http_429);
                return Err(LoadError::Throttled(429, response.retry_after));
            }
            other => return Err(LoadError::Failed(format!("HTTP {other}"))),
        }
        parse_okx_instruments(&response.body).map_err(LoadError::Failed)
    }

    async fn load_once(&self) -> Result<(), LoadError> {
        let list = self.fetch().await?;
        let (table, report) = OkxTable::build(&list, &self.aliases);
        if table.is_empty() {
            return Err(LoadError::Failed(
                "the instrument list holds no tradable linear swaps".into(),
            ));
        }
        for raw in &list.unusable {
            tracing::warn!(symbol = %raw, "instrument skipped: unusable contract size");
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
            .unusable_instruments
            .store(list.unusable.len() as u64, Ordering::Relaxed);
        self.status
            .symbols
            .store(table.len() as u64, Ordering::Relaxed);
        OkxStatus::inc(&self.status.info_loads);

        let previous = self.tx.borrow().clone();
        match previous {
            None => tracing::info!(
                exchange = NAME,
                symbols = table.len(),
                "instrument list loaded"
            ),
            Some(old) => {
                let diff = old.diff(&table);
                if diff.is_empty() {
                    tracing::debug!(
                        symbols = table.len(),
                        "instrument list reloaded, no changes"
                    );
                } else {
                    tracing::info!(
                        symbols = table.len(),
                        added = diff.added.len(),
                        removed = diff.removed.len(),
                        added_symbols = %preview(&diff.added),
                        removed_symbols = %preview(&diff.removed),
                        "symbol set changed (listings, delistings)"
                    );
                }
            }
        }
        self.symbols
            .publish_universe(table.entries().iter().map(|e| e.symbol.clone()).collect());
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
