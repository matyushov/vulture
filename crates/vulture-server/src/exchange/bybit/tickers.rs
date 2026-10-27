//! Per-symbol state of the `tickers` topic.
//!
//! Bybit sends a `snapshot` first and `delta` messages afterwards that carry only the fields that changed
//! ("if a response param is not found in the message, its value has not changed"). The state keeps the last
//! value of every field; a missing field never becomes zero. The state is the source of the mark price and of
//! the open interest samples.

use std::collections::HashMap;
use std::sync::RwLock;

use serde::Deserialize;

/// Latest known values of one symbol (exchange units, as Bybit quotes them).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TickerEntry {
    /// `markPrice`.
    pub mark_price: Option<f64>,
    /// `openInterest`, base coin in exchange units.
    pub open_interest: Option<f64>,
    /// `openInterestValue`, quote notional.
    pub open_interest_value: Option<f64>,
    /// `indexPrice`, exchange units.
    pub index_price: Option<f64>,
    /// `fundingRate`, a fraction.
    pub funding_rate: Option<f64>,
    /// `nextFundingTime`, ms.
    pub next_funding_ts: Option<i64>,
    /// `ts` of the last message that touched the entry, ms.
    pub exchange_ts: i64,
    /// Local receive time of that message, ms.
    pub recv_ts: i64,
}

/// What one `tickers` message says.
#[derive(Debug, Clone, PartialEq)]
pub struct TickerUpdate {
    /// Raw symbol.
    pub symbol: String,
    /// `true` for `snapshot`, `false` for `delta`.
    pub snapshot: bool,
    /// Fields present in the message and usable.
    pub mark_price: Option<f64>,
    /// See [`TickerEntry::open_interest`].
    pub open_interest: Option<f64>,
    /// See [`TickerEntry::open_interest_value`].
    pub open_interest_value: Option<f64>,
    /// `indexPrice`.
    pub index_price: Option<f64>,
    /// `fundingRate` (may be negative).
    pub funding_rate: Option<f64>,
    /// `nextFundingTime`, ms.
    pub next_funding_ts: Option<i64>,
    /// Message time, ms.
    pub exchange_ts: i64,
    /// Fields that were present but unusable (not a number, negative, zero price).
    pub bad_fields: usize,
}

#[derive(Deserialize)]
struct TickerFrame {
    topic: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    ts: i64,
    data: TickerData,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickerData {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    mark_price: Option<String>,
    #[serde(default)]
    open_interest: Option<String>,
    #[serde(default)]
    open_interest_value: Option<String>,
    #[serde(default)]
    index_price: Option<String>,
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    next_funding_time: Option<String>,
}

/// Parses a `tickers.{symbol}` message.
pub fn parse_ticker(text: &str) -> Result<TickerUpdate, String> {
    let f: TickerFrame = serde_json::from_str(text).map_err(|e| e.to_string())?;
    let snapshot = match f.kind.as_str() {
        "snapshot" => true,
        "delta" => false,
        other => return Err(format!("unknown ticker message type {other:?}")),
    };
    let symbol = f
        .data
        .symbol
        .filter(|s| !s.is_empty())
        .or_else(|| f.topic.strip_prefix("tickers.").map(str::to_owned))
        .ok_or("ticker message without a symbol")?;
    let mut bad = 0;
    let mut number = |v: Option<String>, positive: bool| -> Option<f64> {
        let v = v.filter(|s| !s.is_empty())?;
        match v.parse::<f64>() {
            Ok(x) if x.is_finite() && (if positive { x > 0.0 } else { x >= 0.0 }) => Some(x),
            _ => {
                bad += 1;
                None
            }
        }
    };
    let mark_price = number(f.data.mark_price, true);
    let open_interest = number(f.data.open_interest, false);
    let open_interest_value = number(f.data.open_interest_value, false);
    let index_price = number(f.data.index_price, true);
    // A funding rate is legitimately negative or zero; only a non-number is bad.
    let funding_rate =
        f.data
            .funding_rate
            .filter(|s| !s.is_empty())
            .and_then(|v| match v.parse::<f64>() {
                Ok(x) if x.is_finite() => Some(x),
                _ => {
                    bad += 1;
                    None
                }
            });
    let next_funding_ts = f
        .data
        .next_funding_time
        .filter(|s| !s.is_empty())
        .and_then(|v| match v.parse::<i64>() {
            Ok(x) if x > 0 => Some(x),
            _ => {
                bad += 1;
                None
            }
        });
    Ok(TickerUpdate {
        symbol,
        snapshot,
        mark_price,
        open_interest,
        open_interest_value,
        index_price,
        funding_rate,
        next_funding_ts,
        exchange_ts: f.ts,
        bad_fields: bad,
    })
}

/// The state of all symbols.
#[derive(Debug, Default)]
pub struct Tickers {
    map: RwLock<HashMap<String, TickerEntry>>,
    first: tokio::sync::Notify,
}

impl Tickers {
    /// Applies a message: a snapshot replaces the entry, a delta changes only the fields it carries.
    pub fn apply(&self, u: &TickerUpdate, recv_ts: i64) {
        {
            let mut map = self.map.write().unwrap_or_else(|e| e.into_inner());
            let e = map.entry(u.symbol.clone()).or_default();
            if u.snapshot {
                *e = TickerEntry::default();
            }
            if u.mark_price.is_some() {
                e.mark_price = u.mark_price;
            }
            if u.open_interest.is_some() {
                e.open_interest = u.open_interest;
                if u.open_interest_value.is_none() {
                    // The stored value belonged to the old size; the sampler falls back to size x mark.
                    e.open_interest_value = None;
                }
            }
            if u.open_interest_value.is_some() {
                e.open_interest_value = u.open_interest_value;
            }
            if u.index_price.is_some() {
                e.index_price = u.index_price;
            }
            if u.funding_rate.is_some() {
                e.funding_rate = u.funding_rate;
            }
            if u.next_funding_ts.is_some() {
                e.next_funding_ts = u.next_funding_ts;
            }
            e.exchange_ts = u.exchange_ts;
            e.recv_ts = recv_ts;
        }
        self.first.notify_waiters();
    }

    /// The entry of `raw`.
    pub fn get(&self, raw: &str) -> Option<TickerEntry> {
        self.map
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(raw)
            .copied()
    }

    /// Forgets the given symbols (their connection was lost: the next snapshot rebuilds them).
    pub fn forget<'a>(&self, symbols: impl IntoIterator<Item = &'a String>) {
        let mut map = self.map.write().unwrap_or_else(|e| e.into_inner());
        for s in symbols {
            map.remove(s);
        }
    }

    /// Keeps only the symbols `keep` accepts.
    pub fn retain(&self, keep: impl Fn(&str) -> bool) {
        self.map
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|k, _| keep(k));
    }

    /// Number of symbols with state.
    pub fn len(&self) -> usize {
        self.map.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether there is no state.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Resolves once at least one symbol has state (immediately if it already has).
    pub async fn wait_first(&self) {
        loop {
            let notified = self.first.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.is_empty() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SNAP: &str =
        include_str!("../../../../../fixtures/bybit/tickers_linear_snapshot_doc.json");

    #[test]
    fn snapshot_fixture_parses() {
        let line = SNAP.lines().next().unwrap();
        let u = parse_ticker(line).unwrap();
        assert!(u.snapshot);
        assert_eq!(u.symbol, "BTCUSDT");
        assert_eq!(u.mark_price, Some(66666.60));
        assert_eq!(u.open_interest, Some(492373.72));
        assert_eq!(u.open_interest_value, Some(32824881841.75));
        assert_eq!(u.index_price, Some(115418.19));
        assert_eq!(u.funding_rate, Some(-0.005));
        assert_eq!(u.next_funding_ts, Some(1760342400000));
        assert_eq!(u.exchange_ts, 1760325052630);
        assert_eq!(u.bad_fields, 0);
    }

    fn delta(body: &str, ts: i64) -> TickerUpdate {
        parse_ticker(&format!(
            r#"{{"topic":"tickers.BTCUSDT","type":"delta","data":{{"symbol":"BTCUSDT"{body}}},"ts":{ts}}}"#
        ))
        .unwrap()
    }

    #[test]
    fn deltas_merge_and_missing_fields_stay() {
        let t = Tickers::default();
        t.apply(&parse_ticker(SNAP.lines().next().unwrap()).unwrap(), 10);
        t.apply(&delta(r#","markPrice":"66670.5""#, 2000), 20);
        let e = t.get("BTCUSDT").unwrap();
        assert_eq!(e.mark_price, Some(66670.5));
        assert_eq!(e.open_interest, Some(492373.72));
        assert_eq!(e.open_interest_value, Some(32824881841.75));
        assert_eq!((e.exchange_ts, e.recv_ts), (2000, 20));
        t.apply(
            &delta(
                r#","openInterest":"500000","openInterestValue":"3.3e10""#,
                3000,
            ),
            30,
        );
        // A new size without a new value invalidates the old value instead of mixing the two.
        t.apply(&delta(r#","openInterest":"510000""#, 3500), 35);
        let e = t.get("BTCUSDT").unwrap();
        assert_eq!(
            (e.open_interest, e.open_interest_value),
            (Some(510000.0), None)
        );
        t.apply(
            &delta(
                r#","openInterest":"500000","openInterestValue":"3.3e10""#,
                3600,
            ),
            36,
        );
        let e = t.get("BTCUSDT").unwrap();
        assert_eq!(e.open_interest, Some(500000.0));
        assert_eq!(e.mark_price, Some(66670.5));
        // An empty string and a delta without fields change nothing.
        t.apply(&delta(r#","markPrice":"""#, 4000), 40);
        assert_eq!(t.get("BTCUSDT").unwrap().mark_price, Some(66670.5));
    }

    #[test]
    fn funding_and_index_follow_deltas_and_snapshots() {
        let t = Tickers::default();
        t.apply(&parse_ticker(SNAP.lines().next().unwrap()).unwrap(), 10);
        let e = t.get("BTCUSDT").unwrap();
        assert_eq!(
            (e.index_price, e.funding_rate),
            (Some(115418.19), Some(-0.005))
        );
        // A delta that only carries the funding rate keeps the index; a zero rate is a value, not "missing".
        t.apply(&delta(r#","fundingRate":"0""#, 2000), 20);
        let e = t.get("BTCUSDT").unwrap();
        assert_eq!(
            (e.index_price, e.funding_rate),
            (Some(115418.19), Some(0.0))
        );
        assert_eq!(e.next_funding_ts, Some(1760342400000));
        t.apply(
            &delta(
                r#","indexPrice":"115500.5","nextFundingTime":"1760371200000""#,
                3000,
            ),
            30,
        );
        let e = t.get("BTCUSDT").unwrap();
        assert_eq!(
            (e.index_price, e.next_funding_ts),
            (Some(115500.5), Some(1760371200000))
        );
        // Empty strings (a contract without funding) change nothing.
        t.apply(
            &delta(r#","fundingRate":"","nextFundingTime":"""#, 4000),
            40,
        );
        let e = t.get("BTCUSDT").unwrap();
        assert_eq!(e.funding_rate, Some(0.0));
        // Garbage is counted, not applied.
        let u = parse_ticker(r#"{"topic":"tickers.X","type":"delta","data":{"symbol":"X","indexPrice":"-5","fundingRate":"nan?","nextFundingTime":"soon"},"ts":1}"#).unwrap();
        assert_eq!(u.bad_fields, 3);
        assert!(u.index_price.is_none() && u.funding_rate.is_none() && u.next_funding_ts.is_none());
    }

    #[test]
    fn unusable_values_are_counted_not_applied() {
        let u = parse_ticker(
            r#"{"topic":"tickers.X","type":"delta","data":{"symbol":"X","markPrice":"0","openInterest":"-1","openInterestValue":"abc"},"ts":1}"#,
        )
        .unwrap();
        assert_eq!(u.bad_fields, 3);
        assert!(
            u.mark_price.is_none() && u.open_interest.is_none() && u.open_interest_value.is_none()
        );
        assert!(parse_ticker(r#"{"topic":"tickers.X","type":"weird","data":{},"ts":1}"#).is_err());
        assert!(parse_ticker("nope").is_err());
    }

    #[test]
    fn a_new_snapshot_replaces_old_fields() {
        let t = Tickers::default();
        t.apply(&parse_ticker(SNAP.lines().next().unwrap()).unwrap(), 1);
        let bare = parse_ticker(
            r#"{"topic":"tickers.BTCUSDT","type":"snapshot","data":{"symbol":"BTCUSDT","markPrice":"1.5"},"ts":9}"#,
        )
        .unwrap();
        t.apply(&bare, 2);
        let e = t.get("BTCUSDT").unwrap();
        assert_eq!(e.mark_price, Some(1.5));
        assert!(e.open_interest.is_none());
        t.forget([&"BTCUSDT".to_string()]);
        assert!(t.get("BTCUSDT").is_none());
    }
}
