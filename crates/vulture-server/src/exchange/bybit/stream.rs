//! The WebSocket side: handler for the manager and `allLiquidation` parsing.

use serde::Deserialize;
use vulture_proto::v1::{Liquidation, Side};

use crate::exchange::binance::{ParseError, SymbolEntry};
use crate::ws::{Frame, FrameClass, TopicOp, TopicRequest, WsHandler, chunk_topics};

/// Topic prefix of the liquidation feed.
pub const LIQUIDATION_PREFIX: &str = "allLiquidation.";
/// Topic prefix of the ticker feed (mark price, open interest; also the liveness stream).
pub const TICKER_PREFIX: &str = "tickers.";
/// Application-level ping.
pub const PING: &str = r#"{"op":"ping"}"#;
/// Characters of the `args` array per request. Bybit allows 21 000; this leaves a margin for escaping.
pub const DEFAULT_ARGS_LIMIT: usize = 20_000;

/// The value of the `topic` field of a data message, found without parsing the JSON. Bybit puts it first, so
/// the search ends at once; the code does not rely on the order.
pub fn topic_of(text: &str) -> Option<&str> {
    const KEY: &str = r#""topic":""#;
    let start = text.find(KEY)? + KEY.len();
    let rest = &text[start..];
    Some(&rest[..rest.find('"')?])
}

/// The topics of one symbol.
pub fn topics_of(raw: &str) -> [String; 2] {
    [
        format!("{LIQUIDATION_PREFIX}{raw}"),
        format!("{TICKER_PREFIX}{raw}"),
    ]
}

/// Bybit-specific frame handling for [`crate::ws::spawn_ws_with`].
#[derive(Debug, Clone)]
pub struct BybitHandler {
    args_limit: usize,
}

impl Default for BybitHandler {
    fn default() -> Self {
        Self {
            args_limit: DEFAULT_ARGS_LIMIT,
        }
    }
}

impl BybitHandler {
    /// A handler whose requests keep `args` within `args_limit` characters.
    pub fn with_args_limit(args_limit: usize) -> Self {
        Self { args_limit }
    }
}

#[derive(Deserialize)]
struct Control {
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    ret_msg: Option<String>,
    #[serde(default)]
    req_id: Option<String>,
}

impl WsHandler for BybitHandler {
    fn subscribe_messages(&self) -> Vec<String> {
        // Topics come from the runtime control.
        Vec::new()
    }

    fn classify(&self, frame: &Frame) -> FrameClass {
        let Some(text) = frame.as_text() else {
            return FrameClass::Ignore;
        };
        // The hot path: data frames carry a topic. Everything else is a (small) control message.
        if let Some(topic) = topic_of(text) {
            return topic_class(topic);
        }
        match serde_json::from_str::<Control>(text) {
            Ok(c) => {
                let op = c.op.as_deref().unwrap_or("");
                if op == "ping" || op == "pong" || c.ret_msg.as_deref() == Some("pong") {
                    return FrameClass::Pong;
                }
                if op == "subscribe" || op == "unsubscribe" {
                    return FrameClass::TopicResponse {
                        id: c.req_id.filter(|s| !s.is_empty()),
                        ok: c.success.unwrap_or(false),
                        detail: c.ret_msg.unwrap_or_default(),
                    };
                }
                // Other acknowledgements (auth is not used) carry no data.
                if c.success.is_some() {
                    FrameClass::Ignore
                } else {
                    FrameClass::Data { liveness: false }
                }
            }
            Err(_) => FrameClass::Data { liveness: false },
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
        chunk_topics(topics, self.args_limit)
            .into_iter()
            .map(|chunk| {
                let id = next_id.to_string();
                *next_id += 1;
                let args = serde_json::to_string(&chunk).expect("strings serialize");
                TopicRequest {
                    text: format!(r#"{{"op":"{op_name}","req_id":"{id}","args":{args}}}"#),
                    id,
                    topics: chunk,
                }
            })
            .collect()
    }

    /// Key for the overlap of two sessions: the keys of all records of a liquidation frame. Ticker frames
    /// have none (a repeated delta is harmless).
    fn dedup_key(&self, frame: &Frame) -> Option<String> {
        let text = frame.as_text()?;
        if !topic_of(text)?.starts_with(LIQUIDATION_PREFIX) {
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

fn topic_class(topic: &str) -> FrameClass {
    if topic.starts_with(TICKER_PREFIX) {
        FrameClass::Data { liveness: true }
    } else {
        FrameClass::Data { liveness: false }
    }
}

#[derive(Deserialize)]
struct LiquidationFrame {
    #[allow(dead_code)]
    topic: String,
    data: Vec<LiquidationRecord>,
}

#[derive(Deserialize)]
struct LiquidationRecord {
    #[serde(rename = "T")]
    time: i64,
    s: String,
    #[serde(rename = "S")]
    side: String,
    v: String,
    p: String,
}

/// One liquidation record of an `allLiquidation` message.
#[derive(Debug, Clone, PartialEq)]
pub struct Liq {
    /// Raw symbol.
    pub symbol: String,
    /// Position side per `common.proto`: `Buy` means a long position was liquidated.
    pub side: Side,
    /// Executed size (`v`), in exchange units (base coin for linear contracts, confirmed in
    /// `research/bybit-live.md`).
    pub qty: f64,
    /// Bankruptcy price (`p`), per exchange unit. It is not the fill price.
    pub price: f64,
    /// Event time (`T`), ms.
    pub time: i64,
    key: String,
}

impl Liq {
    /// Key of the record: symbol, time, side, size and price.
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

/// Position side from Bybit's `S`. The documentation defines `Buy` as "a long position was liquidated"; `Sell`
/// is the opposite, a liquidated short (the docs show only `Sell` in their example, for a short).
pub fn side_of(s: &str) -> Result<Side, ParseError> {
    match s {
        "Buy" => Ok(Side::Long),
        "Sell" => Ok(Side::Short),
        other => Err(ParseError::UnknownSide(other.to_string())),
    }
}

/// Parses an `allLiquidation.{symbol}` message into its records; a bad record is an `Err` item, the others
/// stay usable. A malformed message as a whole is an error.
pub fn parse_liquidations(text: &str) -> Result<Vec<Result<Liq, ParseError>>, ParseError> {
    let frame: LiquidationFrame =
        serde_json::from_str(text).map_err(|e| ParseError::Malformed(e.to_string()))?;
    Ok(frame
        .data
        .into_iter()
        .map(|r| {
            let side = side_of(&r.side)?;
            let qty = positive("v", &r.v)?;
            let price = positive("p", &r.p)?;
            let key = format!("{}|{}|{}|{}|{}", r.s, r.time, r.side, r.v, r.p);
            Ok(Liq {
                symbol: r.s,
                side,
                qty,
                price,
                time: r.time,
                key,
            })
        })
        .collect())
}

/// Builds the normalized event. Price and quantity are per real coin: for `1000PEPE` (multiplier 1000) the
/// price is divided and the quantity multiplied, so the notional `p * v` is unchanged. `price` is Bybit's
/// bankruptcy price.
pub fn liquidation_event(liq: &Liq, entry: &SymbolEntry, recv_ts_ms: i64) -> Liquidation {
    Liquidation {
        exchange: super::NAME.into(),
        symbol_raw: liq.symbol.clone(),
        symbol: entry.symbol.clone(),
        exchange_ts: liq.time,
        recv_ts: recv_ts_ms,
        side: liq.side as i32,
        price: liq.price / entry.multiplier,
        qty: liq.qty * entry.multiplier,
        notional_usd: liq.price * liq.qty,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIQ: &str = include_str!("../../../../../fixtures/bybit/all_liquidation_doc.json");

    fn entry(raw: &str, symbol: &str, multiplier: f64) -> SymbolEntry {
        SymbolEntry {
            raw: raw.into(),
            symbol: symbol.into(),
            multiplier,
        }
    }

    #[test]
    fn fixture_record_maps_to_an_event() {
        let recs = parse_liquidations(LIQ.lines().next().unwrap()).unwrap();
        assert_eq!(recs.len(), 1);
        let liq = recs[0].as_ref().unwrap();
        // The fixture says "Sell": a short position was liquidated.
        assert_eq!(liq.side, Side::Short);
        let ev = liquidation_event(
            liq,
            &entry("ROSEUSDT", "ROSE-USDT-PERP", 1.0),
            1_739_502_303_500,
        );
        assert_eq!(ev.exchange, "bybit");
        assert_eq!(ev.symbol_raw, "ROSEUSDT");
        assert_eq!(ev.symbol, "ROSE-USDT-PERP");
        assert_eq!(ev.exchange_ts, 1_739_502_302_929);
        assert_eq!(ev.recv_ts, 1_739_502_303_500);
        assert_eq!(ev.price, 0.04499);
        assert_eq!(ev.qty, 20000.0);
        assert!((ev.notional_usd - 899.8).abs() < 1e-9);
    }

    #[test]
    fn buy_means_a_long_was_liquidated() {
        assert_eq!(side_of("Buy").unwrap(), Side::Long);
        assert_eq!(side_of("Sell").unwrap(), Side::Short);
        assert!(side_of("buy").is_err());
        let text = LIQ.lines().next().unwrap().replace("Sell", "Buy");
        let recs = parse_liquidations(&text).unwrap();
        assert_eq!(recs[0].as_ref().unwrap().side, Side::Long);
    }

    #[test]
    fn prefixed_symbols_are_expressed_per_real_coin() {
        let text = r#"{"topic":"allLiquidation.1000PEPEUSDT","type":"snapshot","ts":1,"data":[{"T":5,"s":"1000PEPEUSDT","S":"Buy","v":"16009","p":"0.0091"}]}"#;
        let liq = parse_liquidations(text).unwrap().remove(0).unwrap();
        let ev = liquidation_event(&liq, &entry("1000PEPEUSDT", "PEPE-USDT-PERP", 1000.0), 0);
        assert_eq!(ev.symbol, "PEPE-USDT-PERP");
        assert!((ev.price - 0.0091 / 1000.0).abs() < 1e-15);
        assert_eq!(ev.qty, 16_009_000.0);
        assert!((ev.price * ev.qty - ev.notional_usd).abs() < 1e-6);
    }

    #[test]
    fn a_frame_may_hold_several_records_and_bad_ones_are_isolated() {
        let text = r#"{"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1,"data":[
            {"T":1,"s":"BTCUSDT","S":"Buy","v":"0.5","p":"60000"},
            {"T":2,"s":"BTCUSDT","S":"Hold","v":"0.5","p":"60000"},
            {"T":3,"s":"BTCUSDT","S":"Sell","v":"0","p":"60000"},
            {"T":4,"s":"BTCUSDT","S":"Sell","v":"1","p":"59000"}]}"#;
        let recs = parse_liquidations(text).unwrap();
        assert_eq!(recs.len(), 4);
        assert!(recs[0].is_ok() && recs[3].is_ok());
        assert!(matches!(recs[1], Err(ParseError::UnknownSide(_))));
        assert!(matches!(recs[2], Err(ParseError::InvalidNumber("v"))));
        assert!(parse_liquidations("nope").is_err());
        assert!(parse_liquidations(r#"{"topic":"x","data":{}}"#).is_err());
    }

    #[test]
    fn dedup_key_covers_every_field() {
        let h = BybitHandler::default();
        let key = h
            .dedup_key(&Frame::Text(LIQ.lines().next().unwrap().into()))
            .unwrap();
        assert_eq!(key, "ROSEUSDT|1739502302929|Sell|20000|0.04499");
        assert!(
            h.dedup_key(&Frame::Text(r#"{"topic":"tickers.X"}"#.into()))
                .is_none()
        );
    }

    #[test]
    fn classification() {
        let h = BybitHandler::default();
        let c = |t: &str| h.classify(&Frame::Text(t.into()));
        assert_eq!(
            c(r#"{"topic":"tickers.BTCUSDT","type":"delta","data":{},"ts":1}"#),
            FrameClass::Data { liveness: true }
        );
        assert_eq!(
            c(LIQ.lines().next().unwrap()),
            FrameClass::Data { liveness: false }
        );
        assert_eq!(
            c(r#"{"success":true,"ret_msg":"pong","conn_id":"x","op":"ping"}"#),
            FrameClass::Pong
        );
        assert_eq!(c(r#"{"op":"pong","args":["1"]}"#), FrameClass::Pong);
        assert_eq!(
            c(r#"{"success":true,"ret_msg":"","conn_id":"x","req_id":"7","op":"subscribe"}"#),
            FrameClass::TopicResponse {
                id: Some("7".into()),
                ok: true,
                detail: String::new()
            }
        );
        assert_eq!(
            c(
                r#"{"success":false,"ret_msg":"error:handler not found,topic:tickers.X","conn_id":"x","req_id":"8","op":"subscribe"}"#
            ),
            FrameClass::TopicResponse {
                id: Some("8".into()),
                ok: false,
                detail: "error:handler not found,topic:tickers.X".into()
            }
        );
        assert_eq!(
            c(r#"{"success":true,"op":"unsubscribe","req_id":"9","ret_msg":""}"#),
            FrameClass::TopicResponse {
                id: Some("9".into()),
                ok: true,
                detail: String::new()
            }
        );
        // The order of the keys does not matter.
        assert_eq!(
            c(r#"{"data":{},"topic":"tickers.BTCUSDT","type":"delta"}"#),
            FrameClass::Data { liveness: true }
        );
        assert_eq!(topic_of(r#"{"op":"ping"}"#), None);
        assert_eq!(c("garbage"), FrameClass::Data { liveness: false });
        assert_eq!(h.classify(&Frame::Binary(vec![1])), FrameClass::Ignore);
    }

    #[test]
    fn requests_are_chunked_and_numbered() {
        let h = BybitHandler::with_args_limit(120);
        let topics: Vec<String> = (0..10).map(|i| format!("tickers.SYM{i}USDT")).collect();
        let mut id = 5;
        let reqs = h.topic_requests(TopicOp::Subscribe, &topics, &mut id);
        assert!(reqs.len() > 1);
        assert_eq!(id, 5 + reqs.len() as u64);
        let flat: Vec<String> = reqs.iter().flat_map(|r| r.topics.clone()).collect();
        assert_eq!(flat, topics);
        for (i, r) in reqs.iter().enumerate() {
            assert_eq!(r.id, (5 + i).to_string());
            let v: serde_json::Value = serde_json::from_str(&r.text).unwrap();
            assert_eq!(v["op"], "subscribe");
            assert_eq!(v["req_id"], r.id.as_str());
            assert_eq!(v["args"].as_array().unwrap().len(), r.topics.len());
            let args_len = serde_json::to_string(&v["args"]).unwrap().len();
            assert!(args_len <= 120 + 2, "{args_len}");
        }
        let un = h.topic_requests(TopicOp::Unsubscribe, &topics[..1], &mut id);
        assert!(un[0].text.contains(r#""op":"unsubscribe""#));
        assert_eq!(
            topics_of("BTCUSDT"),
            ["allLiquidation.BTCUSDT", "tickers.BTCUSDT"]
        );
    }
}
