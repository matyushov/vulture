//! Encoding of events into stream entries and back.

use prost::Message;
use vulture_proto::v1::{Event, event::Payload};

/// Value of field `t` for liquidations.
pub const TYPE_LIQUIDATION: &str = "liq";
/// Value of field `t` for open interest snapshots.
pub const TYPE_OI: &str = "oi";
/// Value of field `t` for gaps.
pub const TYPE_GAP: &str = "gap";

/// Fields of one stream entry (without the ID, which Redis assigns).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryFields {
    /// Field `t`: [`TYPE_LIQUIDATION`], [`TYPE_OI`] or [`TYPE_GAP`].
    pub kind: &'static str,
    /// Field `x`: exchange.
    pub exchange: String,
    /// Field `s`: normalized symbol; empty for gaps.
    pub symbol: String,
    /// Field `d`: the protobuf `Event` with `id` cleared.
    pub data: Vec<u8>,
}

/// An entry as read back from the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEntry {
    /// The Redis entry ID, which is the public `Event.id`.
    pub id: String,
    /// Field `t`.
    pub kind: String,
    /// Field `x`.
    pub exchange: String,
    /// Field `s`.
    pub symbol: String,
    /// Field `d`.
    pub data: Vec<u8>,
}

/// An entry that cannot be turned into an event.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// `d` is not a valid `Event`.
    #[error("entry {id}: undecodable event: {message}")]
    Protobuf {
        /// Entry ID.
        id: String,
        /// Decoder message.
        message: String,
    },
}

impl EntryFields {
    /// Builds the entry for `event`. Returns `None` for an event without payload (nothing to store).
    pub fn from_event(event: &Event) -> Option<Self> {
        let (kind, exchange, symbol) = match event.payload.as_ref()? {
            Payload::Liquidation(l) => (TYPE_LIQUIDATION, &l.exchange, l.symbol.as_str()),
            Payload::OpenInterest(o) => (TYPE_OI, &o.exchange, o.symbol.as_str()),
            Payload::Gap(g) => (TYPE_GAP, &g.exchange, ""),
        };
        // The ID belongs to the stream, not to the stored bytes.
        let stripped = Event {
            id: String::new(),
            payload: event.payload.clone(),
        };
        Some(Self {
            kind,
            exchange: exchange.clone(),
            symbol: symbol.to_string(),
            data: stripped.encode_to_vec(),
        })
    }
}

impl StoredEntry {
    /// Decodes `d` and sets `Event.id` from the entry ID.
    pub fn into_event(self) -> Result<Event, DecodeError> {
        let mut event = Event::decode(self.data.as_slice()).map_err(|e| DecodeError::Protobuf {
            id: self.id.clone(),
            message: e.to_string(),
        })?;
        event.id = self.id;
        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vulture_proto::v1::{Gap, Liquidation, OpenInterest, Side};

    fn liq() -> Event {
        Event {
            id: "should-not-be-stored".into(),
            payload: Some(Payload::Liquidation(Liquidation {
                exchange: "binance".into(),
                symbol_raw: "BTCUSDT".into(),
                symbol: "BTC-USDT-PERP".into(),
                exchange_ts: 1,
                recv_ts: 2,
                side: Side::Long as i32,
                price: 1.5,
                qty: 2.0,
                notional_usd: 3.0,
            })),
        }
    }

    #[test]
    fn fields_for_each_type() {
        let l = EntryFields::from_event(&liq()).unwrap();
        assert_eq!(
            (l.kind, l.exchange.as_str(), l.symbol.as_str()),
            ("liq", "binance", "BTC-USDT-PERP")
        );
        let oi = Event {
            id: String::new(),
            payload: Some(Payload::OpenInterest(OpenInterest {
                exchange: "bybit".into(),
                symbol: "ETH-USDT-PERP".into(),
                ..OpenInterest::default()
            })),
        };
        let o = EntryFields::from_event(&oi).unwrap();
        assert_eq!(
            (o.kind, o.exchange.as_str(), o.symbol.as_str()),
            ("oi", "bybit", "ETH-USDT-PERP")
        );
        let gap = Event {
            id: String::new(),
            payload: Some(Payload::Gap(Gap {
                exchange: "*".into(),
                stream: "redis-writer".into(),
                from_ts: 1,
                to_ts: 2,
            })),
        };
        let g = EntryFields::from_event(&gap).unwrap();
        assert_eq!(
            (g.kind, g.exchange.as_str(), g.symbol.as_str()),
            ("gap", "*", "")
        );
        assert!(EntryFields::from_event(&Event::default()).is_none());
    }

    #[test]
    fn round_trip_sets_the_id_from_the_entry() {
        let event = liq();
        let f = EntryFields::from_event(&event).unwrap();
        let stored = StoredEntry {
            id: "1700000000000-3".into(),
            kind: f.kind.into(),
            exchange: f.exchange,
            symbol: f.symbol,
            data: f.data,
        };
        let decoded = stored.clone().into_event().unwrap();
        assert_eq!(decoded.id, "1700000000000-3");
        assert_eq!(decoded.payload, event.payload);
        // The stored bytes carry no id.
        let raw = Event::decode(stored.data.as_slice()).unwrap();
        assert!(raw.id.is_empty());
    }

    #[test]
    fn garbage_is_reported_with_the_entry_id() {
        let stored = StoredEntry {
            id: "5-1".into(),
            kind: "liq".into(),
            exchange: "x".into(),
            symbol: "y".into(),
            data: vec![0xff, 0xff, 0xff],
        };
        let err = stored.into_event().unwrap_err();
        assert!(err.to_string().contains("5-1"), "{err}");
    }
}
