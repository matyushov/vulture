//! The WebSocket side: handler for the manager and parsing of the three channels.
//!
//! Wire facts (official documentation read 2026-10-24, `research/okx-live.md`):
//! - Requests are `{"id":"<n>","op":"subscribe","args":[{"channel":"open-interest","instId":"BTC-USDT-SWAP"},..]}`.
//! - The exchange answers with one `{"event":"subscribe","arg":{..},"connId":".."}` per argument, echoing `id`;
//!   a failure is `{"event":"error","code":"..","msg":"..","id":"..","connId":".."}`.
//! - Keepalive is the plain text `ping`, answered with `pong`.
//! - `liquidation-orders` is subscribed per instrument type (`SWAP`), not per instrument.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::Deserialize;
use vulture_proto::v1::{Liquidation, Side};

use super::table::OkxEntry;
use crate::exchange::binance::ParseError;
use crate::ws::{Frame, FrameClass, TopicOp, TopicRequest, WsHandler};

/// Channel of liquidation orders.
pub const LIQUIDATION_CHANNEL: &str = "liquidation-orders";
/// Channel of open interest (per instrument).
pub const OPEN_INTEREST_CHANNEL: &str = "open-interest";
/// Channel of the mark price (per instrument); also the liveness stream.
pub const MARK_PRICE_CHANNEL: &str = "mark-price";
/// Application ping; the answer is [`PONG`].
pub const PING: &str = "ping";
/// Answer to [`PING`].
pub const PONG: &str = "pong";
/// Arguments per subscribe or unsubscribe request. OKX allows 480 such requests per hour and connection and
/// 64 KB per request; 100 arguments are about 6 KB.
pub const DEFAULT_ARGS_PER_REQUEST: usize = 100;
/// Correlation id of the fixed liquidation subscription.
const LIQUIDATION_REQUEST_ID: &str = "liq";

/// The topic of one instrument and channel, as the tracked set holds it (`mark-price:BTC-USDT-SWAP`).
pub fn topic(channel: &str, inst_id: &str) -> String {
    format!("{channel}:{inst_id}")
}

/// The topics of one raw symbol.
pub fn topics_of(raw: &str) -> Vec<String> {
    vec![
        topic(MARK_PRICE_CHANNEL, raw),
        topic(OPEN_INTEREST_CHANNEL, raw),
    ]
}

/// The value of the first `"channel":"..."` of a message, found without parsing the JSON (data frames and
/// acknowledgements both carry it in `arg`).
pub fn channel_of(text: &str) -> Option<&str> {
    const KEY: &str = r#""channel":""#;
    let start = text.find(KEY)? + KEY.len();
    let rest = &text[start..];
    Some(&rest[..rest.find('"')?])
}

/// OKX-specific frame handling for [`crate::ws::spawn_ws_with`]. One handler per connection: only the first
/// connection subscribes to liquidations (a second subscription would duplicate every record).
#[derive(Debug)]
pub struct OkxHandler {
    liquidations: bool,
    args_per_request: usize,
    /// Acknowledgements seen per request id in the current session. A failure after an acknowledgement of the
    /// same request means the exchange answers per argument (the others are subscribed).
    acked: Mutex<HashMap<String, u32>>,
}

impl OkxHandler {
    /// A handler; `liquidations` adds the fixed `liquidation-orders` subscription to every (re)connect.
    pub fn new(liquidations: bool, args_per_request: usize) -> Self {
        Self {
            liquidations,
            args_per_request: args_per_request.max(1),
            acked: Mutex::new(HashMap::new()),
        }
    }

    fn acked(&self) -> std::sync::MutexGuard<'_, HashMap<String, u32>> {
        self.acked.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[derive(Deserialize)]
struct Control {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    arg: Option<ControlArg>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlArg {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    inst_id: Option<String>,
}

/// The topic an error message names: OKX writes `...channel:mark-price,instId:FOO-USDT-SWAP doesn't exist...`.
/// Not confirmed by the documentation; see `research/okx-live.md` for what the live run showed.
pub fn topic_in_message(msg: &str) -> Option<String> {
    let channel = value_after(msg, "channel:", |c| c == ',' || c.is_whitespace())?;
    let inst = value_after(msg, "instId:", |c| {
        c.is_whitespace() || c == '"' || c == ',' || c == '}'
    })?;
    Some(topic(channel, inst))
}

fn value_after<'a>(msg: &'a str, key: &str, end: impl Fn(char) -> bool) -> Option<&'a str> {
    let start = msg.find(key)? + key.len();
    let rest = &msg[start..];
    let len = rest.find(end).unwrap_or(rest.len());
    Some(&rest[..len]).filter(|v| !v.is_empty())
}

impl WsHandler for OkxHandler {
    fn subscribe_messages(&self) -> Vec<String> {
        // Called at the start of every session: request ids restart, so the acknowledgement memory does too.
        self.acked().clear();
        if !self.liquidations {
            return Vec::new();
        }
        vec![format!(
            r#"{{"id":"{LIQUIDATION_REQUEST_ID}","op":"subscribe","args":[{{"channel":"{LIQUIDATION_CHANNEL}","instType":"SWAP"}}]}}"#
        )]
    }

    fn classify(&self, frame: &Frame) -> FrameClass {
        let Some(text) = frame.as_text() else {
            return FrameClass::Ignore;
        };
        if text == PONG {
            return FrameClass::Pong;
        }
        // The hot path: data frames carry `"data":[`; control messages carry `"event"` and no data.
        if text.contains(r#""data":["#) && !text.contains(r#""event":"#) {
            return FrameClass::Data {
                liveness: channel_of(text) == Some(MARK_PRICE_CHANNEL),
            };
        }
        let Ok(c) = serde_json::from_str::<Control>(text) else {
            return FrameClass::Data { liveness: false };
        };
        let id = c.id.filter(|s| !s.is_empty());
        match c.event.as_deref() {
            Some("subscribe") | Some("unsubscribe") => {
                if id.as_deref() == Some(LIQUIDATION_REQUEST_ID) {
                    return FrameClass::Ack { ok: true };
                }
                if let Some(id) = &id {
                    *self.acked().entry(id.clone()).or_insert(0) += 1;
                }
                FrameClass::TopicResponse {
                    id,
                    ok: true,
                    detail: String::new(),
                }
            }
            Some("error") => {
                let msg = c.msg.unwrap_or_default();
                let code = c.code.unwrap_or_default();
                let detail = format!("{code}: {msg}");
                if id.as_deref() == Some(LIQUIDATION_REQUEST_ID) {
                    tracing::warn!(detail, "the liquidation subscription was refused");
                    return FrameClass::Ack { ok: false };
                }
                let named = topic_in_message(&msg).or_else(|| {
                    let arg = c.arg?;
                    Some(topic(&arg.channel?, &arg.inst_id?))
                });
                let after_ack = id
                    .as_ref()
                    .is_some_and(|i| self.acked().get(i).copied().unwrap_or(0) > 0);
                match (named, after_ack) {
                    // The request was partly processed: reject the named topic only.
                    (Some(t), true) => FrameClass::TopicRefused {
                        topics: vec![t],
                        detail,
                    },
                    // Whole-request failure (or the culprit comes first): let the session name it or split.
                    (Some(t), false) => FrameClass::TopicResponse {
                        id,
                        ok: false,
                        detail: format!("{detail} [topic:{t}]"),
                    },
                    (None, _) => FrameClass::TopicResponse {
                        id,
                        ok: false,
                        detail,
                    },
                }
            }
            // `notice` (upgrade ahead, code 64008) and the connection-count events carry no data.
            _ => FrameClass::Ignore,
        }
    }

    fn topic_requests(
        &self,
        op: TopicOp,
        topics: &[String],
        next_id: &mut u64,
    ) -> Vec<TopicRequest> {
        let op_name = match op {
            TopicOp::Subscribe => "subscribe",
            TopicOp::Unsubscribe => "unsubscribe",
        };
        topics
            .chunks(self.args_per_request)
            .map(|chunk| {
                let id = next_id.to_string();
                *next_id += 1;
                let args: Vec<serde_json::Value> = chunk
                    .iter()
                    .map(|t| {
                        let (channel, inst) = t.split_once(':').unwrap_or((t.as_str(), ""));
                        serde_json::json!({"channel": channel, "instId": inst})
                    })
                    .collect();
                let args = serde_json::to_string(&args).expect("json serializes");
                TopicRequest {
                    text: format!(r#"{{"id":"{id}","op":"{op_name}","args":{args}}}"#),
                    id,
                    topics: chunk.to_vec(),
                }
            })
            .collect()
    }

    /// Key for the overlap of two sessions: the keys of all records of a liquidation frame. Other channels have
    /// none (a repeated quote is harmless).
    fn dedup_key(&self, frame: &Frame) -> Option<String> {
        let text = frame.as_text()?;
        if channel_of(text)? != LIQUIDATION_CHANNEL {
            return None;
        }
        let records = parse_liquidations(text).ok()?;
        Some(
            records
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .map(|r| r.dedup_key())
                .collect::<Vec<_>>()
                .join(";"),
        )
    }
}

// ---------------------------------------------------------------------------------------------
// liquidation-orders
// ---------------------------------------------------------------------------------------------

#[derive(Deserialize)]
struct LiquidationFrame {
    data: Vec<LiquidationItem>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiquidationItem {
    inst_id: String,
    #[serde(default)]
    details: Vec<LiquidationDetail>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiquidationDetail {
    #[serde(default)]
    side: String,
    #[serde(default)]
    pos_side: String,
    sz: String,
    bk_px: String,
    ts: String,
}

/// One liquidation record of a `liquidation-orders` message.
#[derive(Debug, Clone, PartialEq)]
pub struct Liq {
    /// `instId`.
    pub inst_id: String,
    /// Position side per `common.proto`: the side of the position that was liquidated.
    pub side: Side,
    /// Size in contracts (`sz`); multiply by `ctVal * ctMult` for base coins.
    pub contracts: f64,
    /// Bankruptcy price (`bkPx`), quote currency per base coin.
    pub price: f64,
    /// Event time (`ts`), ms.
    pub time: i64,
    key: String,
}

impl Liq {
    /// Key of the record: instrument, time, both side fields, size and price.
    pub fn dedup_key(&self) -> String {
        self.key.clone()
    }
}

fn positive(name: &'static str, text: &str) -> Result<f64, ParseError> {
    match text.parse::<f64>() {
        Ok(v) if v.is_finite() && v > 0.0 => Ok(v),
        _ => Err(ParseError::InvalidNumber(name)),
    }
}

/// The side of the liquidated position from OKX's `posSide` and order `side`.
///
/// - Hedge (long/short) mode: `posSide` names the position (`long` -> [`Side::Long`], `short` ->
///   [`Side::Short`]). The documentation example (`posSide=short`, `side=buy`) fits: a short is closed by a buy.
///   A disagreement between the two fields is not treated as an error; `posSide` wins.
/// - Net mode (`posSide=net`, or empty): the position side is implied by the closing order: `sell` closes a
///   long, `buy` closes a short.
pub fn side_of(side: &str, pos_side: &str) -> Result<Side, ParseError> {
    match pos_side {
        "long" => Ok(Side::Long),
        "short" => Ok(Side::Short),
        "net" | "" => match side {
            "sell" => Ok(Side::Long),
            "buy" => Ok(Side::Short),
            other => Err(ParseError::UnknownSide(other.to_string())),
        },
        other => Err(ParseError::UnknownSide(other.to_string())),
    }
}

/// Parses a `liquidation-orders` message into its records (every entry of every `details` array); a bad record
/// is an `Err` item, the others stay usable. A malformed message as a whole is an error.
pub fn parse_liquidations(text: &str) -> Result<Vec<Result<Liq, ParseError>>, ParseError> {
    let frame: LiquidationFrame =
        serde_json::from_str(text).map_err(|e| ParseError::Malformed(e.to_string()))?;
    let mut out = Vec::new();
    for item in frame.data {
        for d in item.details {
            out.push((|| {
                let side = side_of(&d.side, &d.pos_side)?;
                let contracts = positive("sz", &d.sz)?;
                let price = positive("bkPx", &d.bk_px)?;
                let time: i64 = d.ts.parse().map_err(|_| ParseError::InvalidNumber("ts"))?;
                let key = format!(
                    "{}|{}|{}|{}|{}|{}",
                    item.inst_id, time, d.side, d.pos_side, d.sz, d.bk_px
                );
                Ok(Liq {
                    inst_id: item.inst_id.clone(),
                    side,
                    contracts,
                    price,
                    time,
                    key,
                })
            })());
        }
    }
    Ok(out)
}

/// Builds the normalized event. Quantity is in real coins: `sz * ctVal * ctMult`, then `* multiplier` for
/// pseudo-coins (alias table; 1 for OKX unless the owner adds an entry). `price` is OKX's bankruptcy price
/// (`bkPx`, divided by the multiplier), the notional is `bkPx * base quantity`.
pub fn liquidation_event(liq: &Liq, entry: &OkxEntry<'_>, recv_ts_ms: i64) -> Liquidation {
    let base_qty = liq.contracts * entry.size.ct_val * entry.size.ct_mult;
    let m = entry.symbol.multiplier;
    Liquidation {
        exchange: super::NAME.into(),
        symbol_raw: liq.inst_id.clone(),
        symbol: entry.symbol.symbol.clone(),
        exchange_ts: liq.time,
        recv_ts: recv_ts_ms,
        side: liq.side as i32,
        price: liq.price / m,
        qty: base_qty * m,
        notional_usd: liq.price * base_qty,
    }
}

// ---------------------------------------------------------------------------------------------
// open-interest and mark-price
// ---------------------------------------------------------------------------------------------

/// One `open-interest` push.
#[derive(Debug, Clone, PartialEq)]
pub struct OiUpdate {
    /// `instId`.
    pub inst_id: String,
    /// `oi`, contracts.
    pub oi: f64,
    /// `oiCcy`, base coin.
    pub oi_ccy: Option<f64>,
    /// `oiUsd`, USD.
    pub oi_usd: Option<f64>,
    /// `ts`, ms.
    pub ts: i64,
    /// Optional fields that were present but unusable.
    pub bad_fields: usize,
}

#[derive(Deserialize)]
struct QuoteFrame<T> {
    data: Vec<T>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OiRecord {
    inst_id: String,
    oi: String,
    #[serde(default)]
    oi_ccy: Option<String>,
    #[serde(default)]
    oi_usd: Option<String>,
    #[serde(default)]
    ts: Option<String>,
}

fn nonneg(text: &str) -> Option<f64> {
    text.parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && *v >= 0.0)
}

/// Parses an `open-interest` message.
pub fn parse_open_interest(text: &str) -> Result<Vec<OiUpdate>, String> {
    let f: QuoteFrame<OiRecord> = serde_json::from_str(text).map_err(|e| e.to_string())?;
    f.data
        .into_iter()
        .map(|r| {
            let oi = nonneg(&r.oi).ok_or_else(|| format!("unusable oi {:?}", r.oi))?;
            let mut bad = 0;
            let mut opt = |v: Option<String>| -> Option<f64> {
                let v = v.filter(|s| !s.is_empty())?;
                let n = nonneg(&v);
                if n.is_none() {
                    bad += 1;
                }
                n
            };
            let oi_ccy = opt(r.oi_ccy);
            let oi_usd = opt(r.oi_usd);
            Ok(OiUpdate {
                inst_id: r.inst_id,
                oi,
                oi_ccy,
                oi_usd,
                ts: r.ts.and_then(|t| t.parse().ok()).unwrap_or(0),
                bad_fields: bad,
            })
        })
        .collect()
}

/// One `mark-price` push.
#[derive(Debug, Clone, PartialEq)]
pub struct MarkUpdate {
    /// `instId`.
    pub inst_id: String,
    /// `markPx`, quote per base coin as OKX quotes it.
    pub mark: f64,
    /// `ts`, ms.
    pub ts: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarkRecord {
    inst_id: String,
    mark_px: String,
    #[serde(default)]
    ts: Option<String>,
}

/// Parses a `mark-price` message.
pub fn parse_mark_price(text: &str) -> Result<Vec<MarkUpdate>, String> {
    let f: QuoteFrame<MarkRecord> = serde_json::from_str(text).map_err(|e| e.to_string())?;
    f.data
        .into_iter()
        .map(|r| {
            let mark = r
                .mark_px
                .parse::<f64>()
                .ok()
                .filter(|v| v.is_finite() && *v > 0.0)
                .ok_or_else(|| format!("unusable markPx {:?}", r.mark_px))?;
            Ok(MarkUpdate {
                inst_id: r.inst_id,
                mark,
                ts: r.ts.and_then(|t| t.parse().ok()).unwrap_or(0),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::binance::SymbolEntry;
    use crate::normalize::ContractSize;

    const LIQ: &str = include_str!("../../../../../fixtures/okx/liquidation_orders_doc.json");
    const OI: &str = include_str!("../../../../../fixtures/okx/open_interest_doc.json");
    const MARK: &str = include_str!("../../../../../fixtures/okx/mark_price_doc.json");

    fn parts(raw: &str, symbol: &str, multiplier: f64, ct_val: f64) -> (SymbolEntry, ContractSize) {
        (
            SymbolEntry {
                raw: raw.into(),
                symbol: symbol.into(),
                multiplier,
            },
            ContractSize {
                ct_val,
                ct_mult: 1.0,
                ct_val_ccy: raw.split('-').next().unwrap().into(),
            },
        )
    }

    #[test]
    fn documentation_example_maps_to_an_event() {
        let recs = parse_liquidations(LIQ).unwrap();
        assert_eq!(recs.len(), 1);
        let liq = recs[0].as_ref().unwrap();
        // posSide short, side buy: a short position was liquidated.
        assert_eq!(liq.side, Side::Short);
        assert_eq!(
            (liq.contracts, liq.price, liq.time),
            (13.0, 0.007831, 1_692_266_434_010)
        );
        let (symbol, size) = parts("IOST-USDT-SWAP", "IOST-USDT-PERP", 1.0, 1000.0);
        let ev = liquidation_event(
            liq,
            &OkxEntry {
                symbol: &symbol,
                size: &size,
            },
            5,
        );
        assert_eq!(ev.exchange, "okx");
        assert_eq!(ev.symbol_raw, "IOST-USDT-SWAP");
        assert_eq!(ev.symbol, "IOST-USDT-PERP");
        assert_eq!(ev.exchange_ts, 1_692_266_434_010);
        assert_eq!(ev.recv_ts, 5);
        assert_eq!(ev.qty, 13_000.0, "13 contracts of 1000 IOST");
        assert_eq!(ev.price, 0.007831);
        assert!((ev.notional_usd - 13_000.0 * 0.007831).abs() < 1e-9);
    }

    #[test]
    fn side_in_both_position_modes() {
        // Hedge mode: posSide names the position.
        assert_eq!(side_of("sell", "long").unwrap(), Side::Long);
        assert_eq!(side_of("buy", "short").unwrap(), Side::Short);
        // Net mode: the closing order tells.
        assert_eq!(side_of("sell", "net").unwrap(), Side::Long);
        assert_eq!(side_of("buy", "net").unwrap(), Side::Short);
        assert_eq!(side_of("sell", "").unwrap(), Side::Long);
        // A disagreement in hedge mode: the position side wins.
        assert_eq!(side_of("buy", "long").unwrap(), Side::Long);
        assert!(side_of("hold", "net").is_err());
        assert!(side_of("sell", "both").is_err());
        let net = LIQ.replace(r#""posSide":"short""#, r#""posSide":"net""#);
        assert_eq!(
            parse_liquidations(&net).unwrap()[0].as_ref().unwrap().side,
            Side::Short
        );
        let long = LIQ
            .replace(r#""posSide":"short""#, r#""posSide":"long""#)
            .replace(r#""side":"buy""#, r#""side":"sell""#);
        assert_eq!(
            parse_liquidations(&long).unwrap()[0].as_ref().unwrap().side,
            Side::Long
        );
    }

    #[test]
    fn contract_units_and_pseudo_coin_multipliers() {
        let liq = parse_liquidations(LIQ).unwrap().remove(0).unwrap();
        // ctVal x ctMult = 10 000 000 x 2 coins per contract.
        let (symbol, mut size) = parts("IOST-USDT-SWAP", "IOST-USDT-PERP", 1.0, 10_000_000.0);
        size.ct_mult = 2.0;
        let ev = liquidation_event(
            &liq,
            &OkxEntry {
                symbol: &symbol,
                size: &size,
            },
            0,
        );
        assert_eq!(ev.qty, 13.0 * 20_000_000.0);
        // An owner-defined alias multiplier rescales price and quantity, not the notional.
        let (symbol, size) = parts("IOST-USDT-SWAP", "IOST-USDT-PERP", 1000.0, 1.0);
        let ev = liquidation_event(
            &liq,
            &OkxEntry {
                symbol: &symbol,
                size: &size,
            },
            0,
        );
        assert!((ev.price - 0.007831 / 1000.0).abs() < 1e-15);
        assert_eq!(ev.qty, 13_000.0);
        assert!((ev.price * ev.qty - ev.notional_usd).abs() < 1e-9);
    }

    #[test]
    fn several_details_and_bad_records_are_isolated() {
        let text = r#"{"arg":{"channel":"liquidation-orders","instType":"SWAP"},"data":[
          {"instId":"BTC-USDT-SWAP","details":[
            {"side":"sell","posSide":"net","sz":"2","bkPx":"60000","ts":"1"},
            {"side":"sell","posSide":"net","sz":"0","bkPx":"60000","ts":"2"},
            {"side":"x","posSide":"net","sz":"1","bkPx":"60000","ts":"3"},
            {"side":"sell","posSide":"net","sz":"1","bkPx":"nan","ts":"4"},
            {"side":"sell","posSide":"net","sz":"1","bkPx":"1","ts":"later"}]},
          {"instId":"ETH-USDT-SWAP","details":[{"side":"buy","posSide":"net","sz":"1","bkPx":"3000","ts":"5"}]}]}"#;
        let recs = parse_liquidations(text).unwrap();
        assert_eq!(recs.len(), 6);
        assert!(recs[0].is_ok() && recs[5].is_ok());
        assert!(matches!(recs[1], Err(ParseError::InvalidNumber("sz"))));
        assert!(matches!(recs[2], Err(ParseError::UnknownSide(_))));
        assert!(matches!(recs[3], Err(ParseError::InvalidNumber("bkPx"))));
        assert!(matches!(recs[4], Err(ParseError::InvalidNumber("ts"))));
        assert!(parse_liquidations("nope").is_err());
        assert!(parse_liquidations(r#"{"data":{}}"#).is_err());
    }

    #[test]
    fn dedup_key_covers_every_field() {
        let h = OkxHandler::new(true, 100);
        let key = h.dedup_key(&Frame::Text(LIQ.into())).unwrap();
        assert_eq!(key, "IOST-USDT-SWAP|1692266434010|buy|short|13|0.007831");
        assert!(h.dedup_key(&Frame::Text(OI.into())).is_none());
    }

    #[test]
    fn open_interest_and_mark_price_documentation_examples() {
        let oi = parse_open_interest(OI).unwrap();
        assert_eq!(oi.len(), 1);
        assert_eq!(oi[0].inst_id, "BTC-USDT-SWAP");
        assert_eq!(oi[0].oi, "2216113.01000000309".parse::<f64>().unwrap());
        assert_eq!(oi[0].oi_ccy, "22161.1301000000309".parse::<f64>().ok());
        assert_eq!(
            oi[0].oi_usd,
            "1939251795.54769270396321".parse::<f64>().ok()
        );
        assert_eq!(oi[0].ts, 1_743_041_250_440);
        // The docs example: 0.01 BTC per contract.
        assert!((oi[0].oi * 0.01 - oi[0].oi_ccy.unwrap()).abs() < 1e-6);
        let mark = parse_mark_price(MARK).unwrap();
        assert_eq!(
            (mark[0].inst_id.as_str(), mark[0].mark),
            ("LTC-USD-190628", 0.1)
        );
        assert!(parse_open_interest(r#"{"data":[{"instId":"X","oi":"-1"}]}"#).is_err());
        assert!(parse_mark_price(r#"{"data":[{"instId":"X","markPx":"0"}]}"#).is_err());
        let partial = parse_open_interest(
            r#"{"data":[{"instId":"X","oi":"5","oiCcy":"","oiUsd":"junk","ts":"7"}]}"#,
        )
        .unwrap();
        assert_eq!(
            (partial[0].oi_ccy, partial[0].oi_usd, partial[0].bad_fields),
            (None, None, 1)
        );
    }

    #[test]
    fn classification() {
        let h = OkxHandler::new(true, 100);
        let c = |t: &str| h.classify(&Frame::Text(t.into()));
        assert_eq!(c("pong"), FrameClass::Pong);
        assert_eq!(c(MARK), FrameClass::Data { liveness: true });
        assert_eq!(c(OI), FrameClass::Data { liveness: false });
        assert_eq!(c(LIQ), FrameClass::Data { liveness: false });
        assert_eq!(
            c(
                r#"{"id":"3","event":"subscribe","arg":{"channel":"mark-price","instId":"BTC-USDT-SWAP"},"connId":"a"}"#
            ),
            FrameClass::TopicResponse {
                id: Some("3".into()),
                ok: true,
                detail: String::new()
            }
        );
        // The fixed liquidation subscription is an Ack.
        assert_eq!(
            c(
                r#"{"id":"liq","event":"subscribe","arg":{"channel":"liquidation-orders","instType":"SWAP"},"connId":"a"}"#
            ),
            FrameClass::Ack { ok: true }
        );
        assert_eq!(
            c(r#"{"id":"liq","event":"error","code":"60012","msg":"bad","connId":"a"}"#),
            FrameClass::Ack { ok: false }
        );
        // An error before any acknowledgement of its request: the whole request failed; the culprit is named
        // in the detail so that the session finds it.
        let wrong = r#"{"id":"4","event":"error","code":"60018","msg":"Wrong URL or channel:mark-price,instId:FOO-USDT-SWAP doesn't exist. Please use the correct URL, channel and parameters referring to API document.","connId":"a"}"#;
        match c(wrong) {
            FrameClass::TopicResponse {
                id,
                ok: false,
                detail,
            } => {
                assert_eq!(id.as_deref(), Some("4"));
                assert!(
                    detail.contains("[topic:mark-price:FOO-USDT-SWAP]"),
                    "{detail}"
                );
            }
            other => panic!("{other:?}"),
        }
        // After an acknowledgement of the same request the exchange answers per argument: reject that topic only.
        let _ = c(
            r#"{"id":"5","event":"subscribe","arg":{"channel":"mark-price","instId":"BTC-USDT-SWAP"},"connId":"a"}"#,
        );
        match c(&wrong.replace("\"4\"", "\"5\"")) {
            FrameClass::TopicRefused { topics, .. } => {
                assert_eq!(topics, ["mark-price:FOO-USDT-SWAP"]);
            }
            other => panic!("{other:?}"),
        }
        // Without a recognizable topic the session bisects.
        assert!(matches!(
            c(r#"{"id":"6","event":"error","code":"60012","msg":"Invalid request","connId":"a"}"#),
            FrameClass::TopicResponse { ok: false, .. }
        ));
        assert_eq!(
            c(
                r#"{"event":"notice","code":"64008","msg":"The connection will soon be closed for a service upgrade.","connId":"a"}"#
            ),
            FrameClass::Ignore
        );
        assert_eq!(h.classify(&Frame::Binary(vec![1])), FrameClass::Ignore);
        assert_eq!(c("garbage"), FrameClass::Data { liveness: false });
        // A session start forgets the acknowledgements of the previous one.
        h.subscribe_messages();
        assert!(matches!(
            c(&wrong.replace("\"4\"", "\"5\"")),
            FrameClass::TopicResponse { ok: false, .. }
        ));
    }

    #[test]
    fn only_the_first_connection_subscribes_to_liquidations() {
        let first = OkxHandler::new(true, 100).subscribe_messages();
        assert_eq!(first.len(), 1);
        let v: serde_json::Value = serde_json::from_str(&first[0]).unwrap();
        assert_eq!(v["op"], "subscribe");
        assert_eq!(v["args"][0]["channel"], "liquidation-orders");
        assert_eq!(v["args"][0]["instType"], "SWAP");
        assert!(OkxHandler::new(false, 100).subscribe_messages().is_empty());
    }

    #[test]
    fn requests_are_batched_and_numbered() {
        let h = OkxHandler::new(false, 100);
        let topics: Vec<String> = (0..250)
            .map(|i| topic(MARK_PRICE_CHANNEL, &format!("S{i}-USDT-SWAP")))
            .collect();
        let mut id = 5;
        let reqs = h.topic_requests(TopicOp::Subscribe, &topics, &mut id);
        assert_eq!(reqs.len(), 3);
        assert_eq!(id, 8);
        let flat: Vec<String> = reqs.iter().flat_map(|r| r.topics.clone()).collect();
        assert_eq!(flat, topics);
        for (i, r) in reqs.iter().enumerate() {
            assert_eq!(r.id, (5 + i).to_string());
            assert!(r.text.len() < 64 * 1024);
            let v: serde_json::Value = serde_json::from_str(&r.text).unwrap();
            assert_eq!(v["op"], "subscribe");
            assert_eq!(v["id"], r.id.as_str());
            assert_eq!(v["args"].as_array().unwrap().len(), r.topics.len());
            assert!(r.topics.len() <= 100);
        }
        let first: serde_json::Value = serde_json::from_str(&reqs[0].text).unwrap();
        assert_eq!(first["args"][0]["channel"], "mark-price");
        assert_eq!(first["args"][0]["instId"], "S0-USDT-SWAP");
        let un = h.topic_requests(TopicOp::Unsubscribe, &topics[..1], &mut id);
        assert!(un[0].text.contains(r#""op":"unsubscribe""#));
        assert_eq!(
            topics_of("BTC-USDT-SWAP"),
            ["mark-price:BTC-USDT-SWAP", "open-interest:BTC-USDT-SWAP"]
        );
    }

    #[test]
    fn topics_named_in_messages() {
        assert_eq!(
            topic_in_message(
                "Wrong URL or channel:open-interest,instId:FOO-USDT-SWAP doesn't exist."
            )
            .as_deref(),
            Some("open-interest:FOO-USDT-SWAP")
        );
        assert_eq!(topic_in_message("Invalid request"), None);
        assert_eq!(channel_of(MARK), Some("mark-price"));
        assert_eq!(channel_of("{}"), None);
    }

    const LIVE: &str = include_str!("../../../../../fixtures/okx/live_2026-10-24.jsonl");

    /// Frames captured from the public endpoint on 2026-10-24 (`research/okx-live.md`).
    #[test]
    fn live_frames_parse_and_classify() {
        let lines: Vec<&str> = LIVE.lines().collect();
        assert_eq!(lines.len(), 6);
        let h = OkxHandler::new(true, 100);
        let c = |t: &str| h.classify(&Frame::Text(t.into()));
        // REST open interest (same fields as the channel, used for the unit check).
        let rest: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let r = &rest["data"][0];
        let (oi, ccy): (f64, f64) = (
            r["oi"].as_str().unwrap().parse().unwrap(),
            r["oiCcy"].as_str().unwrap().parse().unwrap(),
        );
        assert!(
            (oi * 0.01 - ccy).abs() / ccy < 1e-9,
            "BTC-USDT-SWAP: 0.01 BTC per contract"
        );

        assert_eq!(c(lines[1]), FrameClass::Data { liveness: true });
        let mark = parse_mark_price(lines[1]).unwrap();
        assert_eq!(mark[0].inst_id, "BTC-USDT-SWAP");
        assert!(mark[0].mark > 0.0 && mark[0].ts > 0);

        assert_eq!(c(lines[2]), FrameClass::Data { liveness: false });
        let oi = parse_open_interest(lines[2]).unwrap();
        assert!((oi[0].oi * 0.01 - oi[0].oi_ccy.unwrap()).abs() / oi[0].oi_ccy.unwrap() < 1e-9);
        assert!(oi[0].oi_usd.unwrap() > 0.0);

        assert_eq!(c(lines[3]), FrameClass::Data { liveness: false });
        let liq = parse_liquidations(lines[3]).unwrap().remove(0).unwrap();
        // posSide short with side buy: a short position was liquidated, as in the documentation example.
        assert_eq!(liq.side, Side::Short);
        assert_eq!(
            (liq.inst_id.as_str(), liq.contracts, liq.price),
            ("LIT-USDT-SWAP", 158.0, 3.1994)
        );

        // One acknowledgement per argument, echoing the request id.
        assert_eq!(
            c(lines[4]),
            FrameClass::TopicResponse {
                id: Some("p1".into()),
                ok: true,
                detail: String::new()
            }
        );
        // The error of a bad argument names the topic in its text; the exchange sent it before the
        // acknowledgements of the good arguments of the same request.
        let fresh = OkxHandler::new(true, 100);
        match fresh.classify(&Frame::Text(lines[5].into())) {
            FrameClass::TopicResponse {
                id,
                ok: false,
                detail,
            } => {
                assert_eq!(id.as_deref(), Some("p1"));
                assert!(
                    detail.contains("[topic:open-interest:NOSUCHCOIN-USDT-SWAP]"),
                    "{detail}"
                );
            }
            other => panic!("{other:?}"),
        }
        // Had an acknowledgement of the request come first, only the named topic is refused.
        assert!(matches!(c(lines[5]), FrameClass::TopicRefused { .. }));
        assert_eq!(c("pong"), FrameClass::Pong);
    }
}
