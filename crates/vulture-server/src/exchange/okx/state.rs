//! Per-instrument state of the `open-interest` and `mark-price` channels.
//!
//! Both channels push whole records, but at different cadences (mark price every 200 ms, or every 10 s when
//! unchanged; open interest every 3 s when it changed), so the latest value of each is kept separately. The
//! state is the source of the open interest samples; a missing part is never replaced by zero.

use std::collections::HashMap;
use std::sync::RwLock;

use super::stream::{MarkUpdate, OiUpdate};

/// Latest open interest push of an instrument.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OiState {
    /// `oi`, contracts.
    pub oi: f64,
    /// `oiCcy`, base coin.
    pub oi_ccy: Option<f64>,
    /// `oiUsd`, USD.
    pub oi_usd: Option<f64>,
    /// `ts` of the push, ms.
    pub ts: i64,
}

/// Latest known values of one instrument.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Quote {
    /// `markPx`.
    pub mark: Option<f64>,
    /// Open interest.
    pub oi: Option<OiState>,
}

/// Funding rate of an instrument, from the REST poll.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FundingState {
    /// `fundingRate`, a fraction.
    pub rate: f64,
    /// `nextFundingTime`, ms, when given.
    pub next_ts: Option<i64>,
    /// Local time the value was fetched, ms.
    pub fetched_ms: i64,
}

/// Index price (`idxPx`) of an index such as `BTC-USDT`, from the REST poll.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IndexState {
    /// The price.
    pub price: f64,
    /// Local time the value was fetched, ms.
    pub fetched_ms: i64,
}

/// The state of all instruments.
#[derive(Debug, Default)]
pub struct Quotes {
    map: RwLock<HashMap<String, Quote>>,
    funding: RwLock<HashMap<String, FundingState>>,
    index: RwLock<HashMap<String, IndexState>>,
    first_oi: tokio::sync::Notify,
}

impl Quotes {
    /// Applies a `mark-price` push.
    pub fn apply_mark(&self, u: &MarkUpdate) {
        self.map
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .entry(u.inst_id.clone())
            .or_default()
            .mark = Some(u.mark);
    }

    /// Applies an `open-interest` push.
    pub fn apply_oi(&self, u: &OiUpdate) {
        {
            self.map
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .entry(u.inst_id.clone())
                .or_default()
                .oi = Some(OiState {
                oi: u.oi,
                oi_ccy: u.oi_ccy,
                oi_usd: u.oi_usd,
                ts: u.ts,
            });
        }
        self.first_oi.notify_waiters();
    }

    /// Stores the funding rate of `inst_id`.
    pub fn apply_funding(&self, inst_id: &str, s: FundingState) {
        self.funding
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(inst_id.to_string(), s);
    }

    /// Stores the index price of `index_id`.
    pub fn apply_index(&self, index_id: &str, s: IndexState) {
        self.index
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(index_id.to_string(), s);
    }

    /// The funding rate of `inst_id` if it was fetched no more than `max_age_ms` before `now_ms`.
    pub fn funding(&self, inst_id: &str, now_ms: i64, max_age_ms: i64) -> Option<FundingState> {
        self.funding
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(inst_id)
            .filter(|f| now_ms - f.fetched_ms <= max_age_ms)
            .copied()
    }

    /// The index price of `index_id` if it was fetched no more than `max_age_ms` before `now_ms`.
    pub fn index(&self, index_id: &str, now_ms: i64, max_age_ms: i64) -> Option<IndexState> {
        self.index
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(index_id)
            .filter(|i| now_ms - i.fetched_ms <= max_age_ms)
            .copied()
    }

    /// The state of `inst_id`.
    pub fn get(&self, inst_id: &str) -> Option<Quote> {
        self.map
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(inst_id)
            .copied()
    }

    /// Forgets the given instruments (their connection was lost; the next pushes rebuild them).
    pub fn forget<'a>(&self, ids: impl IntoIterator<Item = &'a String>) {
        let mut map = self.map.write().unwrap_or_else(|e| e.into_inner());
        for id in ids {
            map.remove(id);
        }
    }

    /// Keeps only the instruments `keep` accepts.
    pub fn retain(&self, keep: impl Fn(&str) -> bool) {
        self.map
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|k, _| keep(k));
    }

    fn has_oi(&self) -> bool {
        self.map
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .any(|q| q.oi.is_some())
    }

    /// Resolves once some instrument has open interest (immediately if one has).
    pub async fn wait_first_oi(&self) {
        loop {
            let notified = self.first_oi.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.has_oi() {
                return;
            }
            notified.await;
        }
    }
}
