//! The WebSocket side: handler for the manager, `forceOrder` parsing and the mark price store.

use std::collections::HashMap;
use std::sync::RwLock;

use serde::Deserialize;
use vulture_proto::v1::{Liquidation, Side};

use super::symbols::SymbolEntry;
use crate::ws::{Frame, FrameClass, WsHandler};

/// Streams of the single socket: liquidations and the always-ticking mark price (liveness).
pub const STREAMS: [&str; 2] = ["!forceOrder@arr", "!markPrice@arr@1s"];

/// Binance-specific frame handling for [`crate::ws::spawn_ws`].
#[derive(Debug, Default)]
pub struct BinanceHandler;

#[derive(Deserialize)]
struct Control {
    #[serde(default)]
    e: Option<String>,
    #[serde(default)]
    id: Option<serde_json::Value>,
    #[serde(default)]
    code: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<serde_json::Value>,
    #[serde(default)]
    result: Option<serde_json::Value>,
}

impl WsHandler for BinanceHandler {
    fn subscribe_messages(&self) -> Vec<String> {
        vec![format!(
            r#"{{"method":"SUBSCRIBE","params":["{}","{}"],"id":1}}"#,
            STREAMS[0], STREAMS[1]
        )]
    }

    fn classify(&self, frame: &Frame) -> FrameClass {
        let Some(text) = frame.as_text() else {
            return FrameClass::Ignore;
        };
        let text = text.trim_start();
        // `!markPrice@arr@1s` arrives as a JSON array of 500+ objects every second; do not parse it here.
        if text.starts_with('[') {
            return FrameClass::Data { liveness: true };
        }
        match serde_json::from_str::<Control>(text) {
            Ok(c) if c.e.as_deref() == Some("forceOrder") => FrameClass::Data { liveness: false },
            Ok(c) if c.code.is_some() || c.error.is_some() => FrameClass::Ack { ok: false },
            // `{"result":null,"id":1}`: the subscription was accepted.
            Ok(c) if c.id.is_some() && c.result.is_none() => FrameClass::Ack { ok: true },
            Ok(c) if c.id.is_some() => FrameClass::Ack { ok: false },
            // Unknown objects are passed on and counted by the connector as unknown frames.
            _ => FrameClass::Data { liveness: false },
        }
    }

    /// Duplicate key for the overlap of a planned reconnect.
    ///
    /// A `forceOrder` frame has no id. The key is `(s, T, S, z, ap)`: symbol, trade time, side, accumulated
    /// filled quantity and average price. S-1 saw 39 same-symbol pairs less than 1000 ms apart in `T`; they
    /// differ in at least one of these fields, so a weaker key (for example `(s, T)`) would lose real
    /// liquidations, and a key with `E` (event time) would not match across two sockets.
    fn dedup_key(&self, frame: &Frame) -> Option<String> {
        let order = parse_force_order(frame.as_text()?).ok()?;
        Some(order.dedup_key())
    }
}

/// Why a frame was not turned into an event.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// Not JSON or fields missing.
    #[error("malformed frame: {0}")]
    Malformed(String),
    /// A number is not finite or not positive.
    #[error("invalid number in field {0}")]
    InvalidNumber(&'static str),
    /// Side other than BUY/SELL.
    #[error("unknown side {0:?}")]
    UnknownSide(String),
}

#[derive(Deserialize)]
struct ForceOrderFrame {
    e: String,
    o: OrderFields,
}

#[derive(Deserialize)]
struct OrderFields {
    s: String,
    #[serde(rename = "S")]
    side: String,
    ap: String,
    z: String,
    #[serde(rename = "T")]
    trade_time: i64,
}

/// The parts of a `forceOrder` frame the connector uses. `st` and the other fields are ignored.
#[derive(Debug, Clone, PartialEq)]
pub struct ForceOrder {
    /// Symbol, raw.
    pub symbol: String,
    /// Side of the liquidation order as sent: `BUY` or `SELL`.
    pub order_side: String,
    /// Average fill price (`ap`), per exchange unit.
    pub avg_price: f64,
    /// Accumulated filled quantity (`z`), in exchange units.
    pub filled_qty: f64,
    /// Trade time (`T`), ms.
    pub trade_time: i64,
    key: String,
}

impl ForceOrder {
    /// See [`BinanceHandler::dedup_key`].
    pub fn dedup_key(&self) -> String {
        self.key.clone()
    }

    /// Position side per `common.proto`: an order to SELL closes a long, an order to BUY closes a short.
    pub fn side(&self) -> Side {
        match self.order_side.as_str() {
            "SELL" => Side::Long,
            _ => Side::Short,
        }
    }
}

fn positive(name: &'static str, text: &str) -> Result<f64, ParseError> {
    match text.parse::<f64>() {
        Ok(v) if v.is_finite() && v > 0.0 => Ok(v),
        _ => Err(ParseError::InvalidNumber(name)),
    }
}

/// Parses one `forceOrder` frame (a single JSON object, S-1).
pub fn parse_force_order(text: &str) -> Result<ForceOrder, ParseError> {
    let frame: ForceOrderFrame =
        serde_json::from_str(text).map_err(|e| ParseError::Malformed(e.to_string()))?;
    if frame.e != "forceOrder" {
        return Err(ParseError::Malformed(format!("event type {:?}", frame.e)));
    }
    let o = frame.o;
    if o.side != "BUY" && o.side != "SELL" {
        return Err(ParseError::UnknownSide(o.side));
    }
    let avg_price = positive("ap", &o.ap)?;
    let filled_qty = positive("z", &o.z)?;
    let key = format!("{}|{}|{}|{}|{}", o.s, o.trade_time, o.side, o.z, o.ap);
    Ok(ForceOrder {
        symbol: o.s,
        order_side: o.side,
        avg_price,
        filled_qty,
        trade_time: o.trade_time,
        key,
    })
}

/// Builds the normalized event. Price and quantity are expressed per real coin: for an aliased symbol
/// (`1000PEPE`, multiplier 1000) the price is divided and the quantity multiplied, so that the notional
/// `ap * z` is unchanged. For multiplier 1 this is exactly `price = ap`, `qty = z`.
pub fn liquidation_event(order: &ForceOrder, entry: &SymbolEntry, recv_ts_ms: i64) -> Liquidation {
    Liquidation {
        exchange: "binance".into(),
        symbol_raw: order.symbol.clone(),
        symbol: entry.symbol.clone(),
        exchange_ts: order.trade_time,
        recv_ts: recv_ts_ms,
        side: order.side() as i32,
        price: order.avg_price / entry.multiplier,
        qty: order.filled_qty * entry.multiplier,
        notional_usd: order.avg_price * order.filled_qty,
    }
}

/// Latest mark price of a symbol.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarkEntry {
    /// Mark price per exchange unit.
    pub price: f64,
    /// Exchange event time (`E`), ms.
    pub exchange_ts: i64,
    /// Local receive time, ms.
    pub recv_ts: i64,
}

/// In-memory map symbol -> latest mark price, fed by `!markPrice@arr@1s`.
#[derive(Debug, Default)]
pub struct MarkPrices {
    map: RwLock<HashMap<String, MarkEntry>>,
    first: tokio::sync::Notify,
}

#[derive(Deserialize)]
struct MarkItem {
    s: String,
    p: String,
    #[serde(rename = "E", default)]
    event_time: i64,
}

impl MarkPrices {
    /// Applies a frame (JSON array). Returns `(applied, rejected_items)`; a frame that is not an array of
    /// objects is an error.
    pub fn apply_frame(&self, text: &str, recv_ts: i64) -> Result<(usize, usize), ParseError> {
        let items: Vec<serde_json::Value> =
            serde_json::from_str(text).map_err(|e| ParseError::Malformed(e.to_string()))?;
        let mut parsed = Vec::with_capacity(items.len());
        let mut rejected = 0;
        for item in items {
            match serde_json::from_value::<MarkItem>(item) {
                Ok(m) => match m.p.parse::<f64>() {
                    Ok(p) if p.is_finite() && p > 0.0 => parsed.push((m.s, p, m.event_time)),
                    _ => rejected += 1,
                },
                Err(_) => rejected += 1,
            }
        }
        let applied = parsed.len();
        {
            let mut map = self.map.write().unwrap_or_else(|e| e.into_inner());
            for (s, price, exchange_ts) in parsed {
                map.insert(
                    s,
                    MarkEntry {
                        price,
                        exchange_ts,
                        recv_ts,
                    },
                );
            }
        }
        if applied > 0 {
            self.first.notify_waiters();
        }
        Ok((applied, rejected))
    }

    /// Latest mark price of `raw` if it was received no more than `max_age_ms` before `now_ms`.
    pub fn get_fresh(&self, raw: &str, now_ms: i64, max_age_ms: i64) -> Option<MarkEntry> {
        let map = self.map.read().unwrap_or_else(|e| e.into_inner());
        map.get(raw)
            .filter(|m| now_ms - m.recv_ts <= max_age_ms)
            .copied()
    }

    /// Resolves as soon as at least one price is known (immediately if one already is).
    pub async fn wait_first(&self) {
        loop {
            let notified = self.first.notified();
            tokio::pin!(notified);
            // Register before checking so that a frame applied in between cannot be missed.
            notified.as_mut().enable();
            if !self.is_empty() {
                return;
            }
            notified.await;
        }
    }

    /// Number of symbols with a price.
    pub fn len(&self) -> usize {
        self.map.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether no price is known.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SELL: &str = r#"{"e":"forceOrder","E":1792775340577,"o":{"s":"BIRBUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"16009","p":"0.0898000","ap":"0.0913215","X":"FILLED","l":"7042","z":"16009","T":1792775339573,"ps":"BIRBUSDT","st":1}}"#;

    fn entry(multiplier: f64) -> SymbolEntry {
        SymbolEntry {
            raw: "BIRBUSDT".into(),
            symbol: "BIRB-USDT-PERP".into(),
            multiplier,
        }
    }

    #[test]
    fn side_follows_the_proto_comments() {
        // common.proto: SIDE_LONG = a long position was liquidated (forced sale) -> order side SELL.
        let sell = parse_force_order(SELL).unwrap();
        assert_eq!(sell.side(), Side::Long);
        let buy = parse_force_order(&SELL.replace("\"S\":\"SELL\"", "\"S\":\"BUY\"")).unwrap();
        assert_eq!(buy.side(), Side::Short);
    }

    #[test]
    fn event_fields() {
        let o = parse_force_order(SELL).unwrap();
        let ev = liquidation_event(&o, &entry(1.0), 1_792_775_340_700);
        assert_eq!(ev.exchange, "binance");
        assert_eq!(ev.symbol_raw, "BIRBUSDT");
        assert_eq!(ev.symbol, "BIRB-USDT-PERP");
        assert_eq!(ev.exchange_ts, 1_792_775_339_573);
        assert_eq!(ev.recv_ts, 1_792_775_340_700);
        assert_eq!(ev.price, 0.0913215);
        assert_eq!(ev.qty, 16009.0);
        assert!((ev.notional_usd - 0.0913215 * 16009.0).abs() < 1e-9);
    }

    #[test]
    fn aliased_symbols_are_expressed_per_real_coin() {
        let o = parse_force_order(SELL).unwrap();
        let ev = liquidation_event(&o, &entry(1000.0), 0);
        assert!((ev.price - 0.0913215 / 1000.0).abs() < 1e-15);
        assert_eq!(ev.qty, 16_009_000.0);
        assert!((ev.notional_usd - 0.0913215 * 16009.0).abs() < 1e-9);
        assert!((ev.price * ev.qty - ev.notional_usd).abs() < 1e-6);
    }

    #[test]
    fn bad_frames_are_errors() {
        assert!(parse_force_order("nope").is_err());
        assert!(parse_force_order(r#"{"e":"other","o":{}}"#).is_err());
        assert!(parse_force_order(&SELL.replace("0.0913215", "0")).is_err());
        assert!(matches!(
            parse_force_order(&SELL.replace("\"SELL\"", "\"HOLD\"")),
            Err(ParseError::UnknownSide(_))
        ));
    }

    #[test]
    fn dedup_key_has_all_five_parts() {
        let h = BinanceHandler;
        let key = h.dedup_key(&Frame::Text(SELL.into())).unwrap();
        assert_eq!(key, "BIRBUSDT|1792775339573|SELL|16009|0.0913215");
        assert!(h.dedup_key(&Frame::Text("[]".into())).is_none());
    }

    #[test]
    fn classification() {
        let h = BinanceHandler;
        let c = |t: &str| h.classify(&Frame::Text(t.into()));
        assert_eq!(c(SELL), FrameClass::Data { liveness: false });
        assert_eq!(
            c(r#"[{"e":"markPriceUpdate"}]"#),
            FrameClass::Data { liveness: true }
        );
        assert_eq!(c(r#"{"result":null,"id":1}"#), FrameClass::Ack { ok: true });
        assert_eq!(
            c(r#"{"code":2,"msg":"Invalid request"}"#),
            FrameClass::Ack { ok: false }
        );
        assert_eq!(
            c(r#"{"error":{"code":1},"id":1}"#),
            FrameClass::Ack { ok: false }
        );
        assert_eq!(h.classify(&Frame::Binary(vec![1])), FrameClass::Ignore);
        assert_eq!(h.subscribe_messages().len(), 1);
        assert!(h.subscribe_messages()[0].contains("!forceOrder@arr"));
        assert!(h.subscribe_messages()[0].contains("!markPrice@arr@1s"));
    }

    #[test]
    fn mark_prices_apply_and_expire() {
        let m = MarkPrices::default();
        let frame = r#"[{"e":"markPriceUpdate","E":1000,"s":"BTCUSDT","p":"100000.5","i":"1","P":"1","r":"0","T":2},
                        {"e":"markPriceUpdate","E":1000,"s":"BAD","p":"x"},
                        {"e":"markPriceUpdate","E":1000,"s":"ETHUSDT","p":"3000"}]"#;
        assert_eq!(m.apply_frame(frame, 5_000).unwrap(), (2, 1));
        assert_eq!(m.len(), 2);
        let btc = m.get_fresh("BTCUSDT", 5_500, 1_000).unwrap();
        assert_eq!(btc.price, 100000.5);
        assert_eq!(btc.exchange_ts, 1000);
        assert!(m.get_fresh("BTCUSDT", 7_000, 1_000).is_none());
        assert!(m.get_fresh("SOLUSDT", 5_000, 1_000).is_none());
        assert!(m.apply_frame("{}", 0).is_err());
    }
}
