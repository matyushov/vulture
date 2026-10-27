//! Time, order and filters of stored events, as `History.Query` defines them.
//!
//! The time of an event is `exchange_ts` for liquidations and OI samples, `from_ts` for gaps and `ts` for signals
//! (the column each table is sorted and partitioned by). A gap belongs to a period when it *overlaps* it
//! (`from_ts < to` and `to_ts >= from`); every other event when its time lies in `[from, to)`. Answers are ordered
//! by (time, stream ID as numbers).

use std::collections::HashSet;

use vulture_proto::v1::{Event, EventType, event::Payload};

use super::cold::{EventKey, Kind, Scope};
use crate::store::{StoredEntry, parse_id};

/// The kind of an event.
pub fn kind_of(e: &Event) -> Option<Kind> {
    Some(match e.payload.as_ref()? {
        Payload::Liquidation(_) => Kind::Liquidation,
        Payload::OpenInterest(_) => Kind::OpenInterest,
        Payload::Gap(_) => Kind::Gap,
        Payload::Signal(_) => Kind::Signal,
    })
}

/// The time an event is ordered by.
pub fn event_ts(e: &Event) -> Option<i64> {
    Some(match e.payload.as_ref()? {
        Payload::Liquidation(l) => l.exchange_ts,
        Payload::OpenInterest(o) => o.exchange_ts,
        Payload::Gap(g) => g.from_ts,
        Payload::Signal(s) => s.ts,
    })
}

/// Whether the event belongs to the period `[from, to)`.
pub fn in_period(e: &Event, from: i64, to: i64) -> bool {
    match e.payload.as_ref() {
        Some(Payload::Gap(g)) => g.from_ts < to && g.to_ts >= from,
        Some(_) => event_ts(e).is_some_and(|t| t >= from && t < to),
        None => false,
    }
}

/// The answer-order position of an event whose `id` is set.
pub fn event_key(e: &Event) -> Option<EventKey> {
    let (id_ms, id_seq) = parse_id(&e.id)?;
    Some(EventKey {
        ts: event_ts(e)?,
        id_ms,
        id_seq,
    })
}

/// Maps `EventType` numbers to kinds; an empty list means every kind. `Err` carries the offending number.
pub fn kinds_from_types(types: &[i32]) -> Result<Vec<Kind>, i32> {
    let mut out = Vec::new();
    for raw in types {
        let kind = match EventType::try_from(*raw) {
            Ok(EventType::Liquidation) => Kind::Liquidation,
            Ok(EventType::OpenInterest) => Kind::OpenInterest,
            Ok(EventType::Gap) => Kind::Gap,
            Ok(EventType::Signal) => Kind::Signal,
            Ok(EventType::Unspecified) | Err(_) => return Err(*raw),
        };
        if !out.contains(&kind) {
            out.push(kind);
        }
    }
    if out.is_empty() {
        out.extend(Kind::ALL);
    }
    Ok(out)
}

/// Kind, exchange and symbol filter of `Query`, the same rule as the live stream: the writer's gaps and the
/// market-wide signals (exchange `*`) pass the exchange filter, entries without a symbol pass the symbol filter.
#[derive(Debug, Clone)]
pub struct EventFilter {
    kinds: HashSet<Kind>,
    exchanges: HashSet<String>,
    symbols: HashSet<String>,
}

impl EventFilter {
    /// Builds the filter.
    pub fn new(kinds: &[Kind], scope: &Scope) -> Self {
        Self {
            kinds: kinds.iter().copied().collect(),
            exchanges: scope.exchanges.iter().cloned().collect(),
            symbols: scope.symbols.iter().cloned().collect(),
        }
    }

    fn pass(&self, exchange: &str, symbol: &str) -> bool {
        (self.exchanges.is_empty() || exchange == "*" || self.exchanges.contains(exchange))
            && (self.symbols.is_empty() || symbol.is_empty() || self.symbols.contains(symbol))
    }

    /// Decides from the cheap fields of a stream entry, without decoding it.
    pub fn matches_entry(&self, e: &StoredEntry) -> bool {
        let kind = match e.kind.as_str() {
            crate::store::TYPE_LIQUIDATION => Kind::Liquidation,
            crate::store::TYPE_OI => Kind::OpenInterest,
            crate::store::TYPE_GAP => Kind::Gap,
            crate::store::TYPE_SIGNAL => Kind::Signal,
            _ => return false,
        };
        self.kinds.contains(&kind) && self.pass(&e.exchange, &e.symbol)
    }

    /// Decides for a decoded event.
    pub fn matches_event(&self, e: &Event) -> bool {
        let Some(kind) = kind_of(e) else { return false };
        if !self.kinds.contains(&kind) {
            return false;
        }
        let (exchange, symbol) = match e.payload.as_ref() {
            Some(Payload::Liquidation(l)) => (l.exchange.as_str(), l.symbol.as_str()),
            Some(Payload::OpenInterest(o)) => (o.exchange.as_str(), o.symbol.as_str()),
            Some(Payload::Gap(g)) => (g.exchange.as_str(), ""),
            Some(Payload::Signal(s)) => (s.exchange.as_str(), s.symbol.as_str()),
            None => return false,
        };
        self.pass(exchange, symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vulture_proto::v1::{Gap, Liquidation, OpenInterest, Signal};

    fn liq(ts: i64, id: &str) -> Event {
        Event {
            id: id.into(),
            payload: Some(Payload::Liquidation(Liquidation {
                exchange: "binance".into(),
                symbol: "BTC-USDT-PERP".into(),
                exchange_ts: ts,
                recv_ts: ts + 5,
                ..Liquidation::default()
            })),
        }
    }

    #[test]
    fn times_per_kind() {
        let gap = Event {
            id: "1-0".into(),
            payload: Some(Payload::Gap(Gap {
                exchange: "binance".into(),
                stream: "open_interest".into(),
                from_ts: 100,
                to_ts: 300,
            })),
        };
        let sig = Event {
            id: "1-1".into(),
            payload: Some(Payload::Signal(Signal {
                ts: 77,
                ..Signal::default()
            })),
        };
        let oi = Event {
            id: "1-2".into(),
            payload: Some(Payload::OpenInterest(OpenInterest {
                exchange_ts: 55,
                recv_ts: 60,
                ..OpenInterest::default()
            })),
        };
        assert_eq!(event_ts(&liq(42, "1-0")), Some(42));
        assert_eq!(event_ts(&gap), Some(100));
        assert_eq!(event_ts(&sig), Some(77));
        assert_eq!(event_ts(&oi), Some(55));
        assert_eq!(event_ts(&Event::default()), None);
    }

    #[test]
    fn periods_are_half_open_and_gaps_overlap() {
        assert!(in_period(&liq(100, "1-0"), 100, 200));
        assert!(in_period(&liq(199, "1-0"), 100, 200));
        assert!(!in_period(&liq(200, "1-0"), 100, 200));
        assert!(!in_period(&liq(99, "1-0"), 100, 200));
        let gap = |from, to| Event {
            id: "1-0".into(),
            payload: Some(Payload::Gap(Gap {
                exchange: "x".into(),
                stream: "s".into(),
                from_ts: from,
                to_ts: to,
            })),
        };
        assert!(
            in_period(&gap(50, 100), 100, 200),
            "ends exactly at the start"
        );
        assert!(!in_period(&gap(50, 99), 100, 200));
        assert!(in_period(&gap(150, 400), 100, 200));
        assert!(
            !in_period(&gap(200, 400), 100, 200),
            "starts at the exclusive end"
        );
        assert!(
            in_period(&gap(0, 1000), 100, 200),
            "covers the whole period"
        );
    }

    #[test]
    fn keys_compare_ids_as_numbers() {
        let a = event_key(&liq(10, "9-3")).unwrap();
        let b = event_key(&liq(10, "10-0")).unwrap();
        assert!(
            a < b,
            "9-3 is before 10-0 although \"9-3\" > \"10-0\" as text"
        );
        let c = event_key(&liq(9, "99-0")).unwrap();
        assert!(c < a, "time comes first");
        assert_eq!(event_key(&liq(10, "garbage")), None);
    }

    #[test]
    fn type_lists() {
        assert_eq!(kinds_from_types(&[]).unwrap(), Kind::ALL.to_vec());
        assert_eq!(
            kinds_from_types(&[EventType::Gap as i32, EventType::Gap as i32]).unwrap(),
            vec![Kind::Gap]
        );
        assert_eq!(kinds_from_types(&[0]), Err(0));
        assert_eq!(kinds_from_types(&[99]), Err(99));
    }

    #[test]
    fn filter_follows_the_stream_rule() {
        let scope = Scope {
            exchanges: vec!["bybit".into()],
            symbols: vec!["ETH-USDT-PERP".into()],
            base_assets: vec![],
        };
        let f = EventFilter::new(&Kind::ALL, &scope);
        assert!(!f.matches_event(&liq(1, "1-0")));
        let writer_gap = Event {
            id: "1-0".into(),
            payload: Some(Payload::Gap(Gap {
                exchange: "*".into(),
                ..Gap::default()
            })),
        };
        assert!(f.matches_event(&writer_gap));
        let cascade = Event {
            id: "1-0".into(),
            payload: Some(Payload::Signal(Signal {
                exchange: "*".into(),
                ..Signal::default()
            })),
        };
        assert!(f.matches_event(&cascade));
        let only_liq = EventFilter::new(&[Kind::Liquidation], &Scope::default());
        assert!(only_liq.matches_event(&liq(1, "1-0")));
        assert!(!only_liq.matches_event(&writer_gap));
        let entry = StoredEntry {
            id: "1-0".into(),
            kind: "liq".into(),
            exchange: "binance".into(),
            symbol: "BTC-USDT-PERP".into(),
            data: vec![],
        };
        assert!(only_liq.matches_entry(&entry));
        assert!(!f.matches_entry(&entry));
    }
}
