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

/// The state of all instruments.
#[derive(Debug, Default)]
pub struct Quotes {
    map: RwLock<HashMap<String, Quote>>,
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
