//! Instrument list loader: paged REST load, hourly reload, diff logging, publication of the symbol table.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::status::BybitStatus;
use super::{BybitTuning, NAME};
use crate::exchange::binance::{HttpClient, HttpError, SymbolTable, encode_query_value};
use crate::normalize::{BaseAliases, InstrumentMeta, Venue, parse_bybit_instruments};
use crate::symbols::SymbolChannel;
use crate::ws::Clock;

/// REST path of the instrument list (first page).
pub const INSTRUMENTS_PATH: &str = "/v5/market/instruments-info?category=linear&limit=1000";

/// Safety stop for runaway pagination.
const MAX_PAGES: usize = 50;

/// Failure of one load.
#[derive(Debug)]
pub(super) enum LoadError {
    /// A 403 (the exchange asks for at least 10 minutes) or 429.
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
    pub status: Arc<BybitStatus>,
    pub aliases: Arc<BaseAliases>,
    pub symbols: Arc<SymbolChannel>,
    pub tuning: BybitTuning,
    pub clock: Arc<dyn Clock>,
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
                    BybitStatus::inc(&self.status.info_errors);
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

    async fn fetch_all(&self) -> Result<Vec<InstrumentMeta>, LoadError> {
        let mut metas = Vec::new();
        let mut cursor: Option<String> = None;
        let mut seen: HashSet<String> = HashSet::new();
        for _ in 0..MAX_PAGES {
            let path = match &cursor {
                None => INSTRUMENTS_PATH.to_string(),
                Some(c) => format!("{INSTRUMENTS_PATH}&cursor={}", cursor_param(c)),
            };
            let response = match timeout(self.tuning.info_timeout, self.http.get(&path)).await {
                Ok(r) => r,
                Err(_) => Err(HttpError::Timeout),
            }
            .map_err(|e| LoadError::Failed(e.to_string()))?;
            match response.status {
                200 => {}
                403 => {
                    BybitStatus::inc(&self.status.http_403);
                    return Err(LoadError::Throttled(403, response.retry_after));
                }
                429 => {
                    BybitStatus::inc(&self.status.http_429);
                    return Err(LoadError::Throttled(429, response.retry_after));
                }
                other => return Err(LoadError::Failed(format!("HTTP {other}"))),
            }
            let page = parse_bybit_instruments(&response.body).map_err(LoadError::Failed)?;
            metas.extend(page.metas);
            match page.next_cursor {
                None => return Ok(metas),
                Some(next) => {
                    if !seen.insert(next.clone()) {
                        return Err(LoadError::Failed("pagination cursor repeats".into()));
                    }
                    cursor = Some(next);
                }
            }
        }
        Err(LoadError::Failed(format!("more than {MAX_PAGES} pages")))
    }

    async fn load_once(&self) -> Result<(), LoadError> {
        let metas = self.fetch_all().await?;
        let (table, report) = SymbolTable::build_for(Venue::Bybit, &metas, &self.aliases);
        if table.is_empty() {
            return Err(LoadError::Failed(
                "the instrument list holds no tradable linear perpetuals".into(),
            ));
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
        BybitStatus::inc(&self.status.info_loads);

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

/// Bybit hands the cursor out percent-encoded (`%2C`, `%3D`...) and expects it back unchanged; a cursor
/// without any `%` is encoded here.
pub(super) fn cursor_param(cursor: &str) -> String {
    if cursor.contains('%') {
        cursor.to_string()
    } else {
        encode_query_value(cursor)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_go_back_as_sent_or_get_encoded() {
        // As Bybit sends them (already percent-encoded; live run 2026-10-24).
        assert_eq!(
            cursor_param("first%3D0GUSDT%26last%3DMETISUSDT"),
            "first%3D0GUSDT%26last%3DMETISUSDT"
        );
        assert_eq!(cursor_param("a=b&c"), "a%3Db%26c");
    }
}
