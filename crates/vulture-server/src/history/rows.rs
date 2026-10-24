//! Stream entries to ClickHouse rows and their `RowBinary` encoding.
//!
//! `RowBinary` is the plain binary row format of ClickHouse: fixed-width little-endian numbers, strings as a
//! LEB128 length plus bytes, `Nullable(T)` as a flag byte (1 = NULL) followed by the value unless NULL,
//! `Map(K, V)` as an element count followed by the pairs, `Enum8` as one signed byte and `DateTime64(3)` as an
//! `Int64` of milliseconds. `LowCardinality(String)` travels as a plain string.

use std::collections::BTreeMap;

use vulture_proto::v1::{
    Event, Gap, Liquidation, OpenInterest, Side, Signal, SignalType, event::Payload,
};

use crate::store::StoredEntry;

/// The history tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Table {
    /// `liquidations`.
    Liquidations,
    /// `open_interest`.
    OpenInterest,
    /// `gaps`.
    Gaps,
    /// `signals`.
    Signals,
}

impl Table {
    /// Every table, in the order the flusher inserts them.
    pub const ALL: [Table; 4] = [
        Table::Liquidations,
        Table::OpenInterest,
        Table::Gaps,
        Table::Signals,
    ];

    /// Table name in ClickHouse and the `table` metric label.
    pub fn name(self) -> &'static str {
        match self {
            Table::Liquidations => "liquidations",
            Table::OpenInterest => "open_interest",
            Table::Gaps => "gaps",
            Table::Signals => "signals",
        }
    }

    /// Position in [`Table::ALL`].
    pub fn index(self) -> usize {
        match self {
            Table::Liquidations => 0,
            Table::OpenInterest => 1,
            Table::Gaps => 2,
            Table::Signals => 3,
        }
    }

    /// Column list of the `INSERT`, in the order [`Row::encode`] writes them.
    pub fn columns(self) -> &'static str {
        match self {
            Table::Liquidations => {
                "exchange, symbol, symbol_raw, exchange_ts, recv_ts, side, price, qty, notional_usd, event_id"
            }
            Table::OpenInterest => {
                "exchange, symbol, symbol_raw, exchange_ts, recv_ts, oi_base, oi_usd, mark_price, event_id"
            }
            Table::Gaps => "exchange, stream, from_ts, to_ts, event_id",
            Table::Signals => {
                "type, exchange, symbol, window_secs, change_pct, change_abs, threshold, ts, details, event_id"
            }
        }
    }
}

/// Latest representable `DateTime64` is 2299-12-31; timestamps are clamped into `[0, MAX_TS_MS]` so that one
/// absurd value from an exchange can neither fail an insert forever nor create thousands of partitions.
pub const MAX_TS_MS: i64 = 10_413_791_999_999;

fn clamp_ts(ms: i64) -> i64 {
    ms.clamp(0, MAX_TS_MS)
}

/// `Enum8` value of `side` in the `liquidations` table; unknown numbers become `unspecified` (0).
fn side_code(side: i32) -> i8 {
    match Side::try_from(side) {
        Ok(Side::Long) => 1,
        Ok(Side::Short) => 2,
        _ => 0,
    }
}

/// `Enum8` value of a signal type; unknown numbers become `unspecified` (0).
fn signal_type_code(t: i32) -> i8 {
    match SignalType::try_from(t) {
        Ok(SignalType::OiSpikeUp) => 1,
        Ok(SignalType::OiSpikeDown) => 2,
        Ok(SignalType::LiqCascade) => 3,
        _ => 0,
    }
}

/// A row of `liquidations`.
#[derive(Debug, Clone, PartialEq)]
pub struct LiquidationRow {
    /// Exchange.
    pub exchange: String,
    /// Normalized symbol.
    pub symbol: String,
    /// Symbol as the exchange spells it.
    pub symbol_raw: String,
    /// Exchange event time, ms.
    pub exchange_ts: i64,
    /// Receive time, ms.
    pub recv_ts: i64,
    /// `Enum8`: 0 unspecified, 1 long, 2 short.
    pub side: i8,
    /// Price.
    pub price: f64,
    /// Quantity in the base coin.
    pub qty: f64,
    /// Notional, USD.
    pub notional_usd: f64,
    /// Redis stream entry ID.
    pub event_id: String,
}

/// A row of `open_interest`.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenInterestRow {
    /// Exchange.
    pub exchange: String,
    /// Normalized symbol.
    pub symbol: String,
    /// Symbol as the exchange spells it.
    pub symbol_raw: String,
    /// Exchange time, ms.
    pub exchange_ts: i64,
    /// Receive time, ms.
    pub recv_ts: i64,
    /// OI in the base coin.
    pub oi_base: f64,
    /// OI in USD, when known.
    pub oi_usd: Option<f64>,
    /// Mark price, when known.
    pub mark_price: Option<f64>,
    /// Redis stream entry ID.
    pub event_id: String,
}

/// A row of `gaps`.
#[derive(Debug, Clone, PartialEq)]
pub struct GapRow {
    /// Exchange (`*` for gaps of the writer itself).
    pub exchange: String,
    /// Stream name.
    pub stream: String,
    /// Start of the interruption, ms.
    pub from_ts: i64,
    /// End of the interruption, ms.
    pub to_ts: i64,
    /// Redis stream entry ID.
    pub event_id: String,
}

/// A row of `signals`.
#[derive(Debug, Clone, PartialEq)]
pub struct SignalRow {
    /// `Enum8` signal type.
    pub kind: i8,
    /// Exchange (`*` for several).
    pub exchange: String,
    /// Normalized symbol, possibly empty.
    pub symbol: String,
    /// Observation window, seconds.
    pub window_secs: u32,
    /// Change in percent.
    pub change_pct: f64,
    /// Absolute change.
    pub change_abs: f64,
    /// Threshold that was crossed.
    pub threshold: f64,
    /// Time the signal fired, ms.
    pub ts: i64,
    /// Free-form details, sorted by key.
    pub details: BTreeMap<String, String>,
    /// Redis stream entry ID.
    pub event_id: String,
}

/// One row of any table.
#[derive(Debug, Clone, PartialEq)]
pub enum Row {
    /// `liquidations`.
    Liquidation(LiquidationRow),
    /// `open_interest`.
    OpenInterest(OpenInterestRow),
    /// `gaps`.
    Gap(GapRow),
    /// `signals`.
    Signal(SignalRow),
}

impl Row {
    /// The table the row belongs to.
    pub fn table(&self) -> Table {
        match self {
            Row::Liquidation(_) => Table::Liquidations,
            Row::OpenInterest(_) => Table::OpenInterest,
            Row::Gap(_) => Table::Gaps,
            Row::Signal(_) => Table::Signals,
        }
    }

    /// Maps a decoded event; `id` is the Redis entry ID. `None` for an event without payload.
    pub fn from_event(event: &Event, id: &str) -> Option<Row> {
        Some(match event.payload.as_ref()? {
            Payload::Liquidation(l) => Row::Liquidation(liquidation(l, id)),
            Payload::OpenInterest(o) => Row::OpenInterest(open_interest(o, id)),
            Payload::Gap(g) => Row::Gap(gap(g, id)),
            Payload::Signal(s) => Row::Signal(signal(s, id)),
        })
    }

    /// Appends the `RowBinary` encoding of the row, columns as in [`Table::columns`].
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Row::Liquidation(r) => {
                put_str(out, &r.exchange);
                put_str(out, &r.symbol);
                put_str(out, &r.symbol_raw);
                put_i64(out, r.exchange_ts);
                put_i64(out, r.recv_ts);
                out.push(r.side as u8);
                put_f64(out, r.price);
                put_f64(out, r.qty);
                put_f64(out, r.notional_usd);
                put_str(out, &r.event_id);
            }
            Row::OpenInterest(r) => {
                put_str(out, &r.exchange);
                put_str(out, &r.symbol);
                put_str(out, &r.symbol_raw);
                put_i64(out, r.exchange_ts);
                put_i64(out, r.recv_ts);
                put_f64(out, r.oi_base);
                put_nullable_f64(out, r.oi_usd);
                put_nullable_f64(out, r.mark_price);
                put_str(out, &r.event_id);
            }
            Row::Gap(r) => {
                put_str(out, &r.exchange);
                put_str(out, &r.stream);
                put_i64(out, r.from_ts);
                put_i64(out, r.to_ts);
                put_str(out, &r.event_id);
            }
            Row::Signal(r) => {
                out.push(r.kind as u8);
                put_str(out, &r.exchange);
                put_str(out, &r.symbol);
                out.extend_from_slice(&r.window_secs.to_le_bytes());
                put_f64(out, r.change_pct);
                put_f64(out, r.change_abs);
                put_f64(out, r.threshold);
                put_i64(out, r.ts);
                put_varint(out, r.details.len() as u64);
                for (k, v) in &r.details {
                    put_str(out, k);
                    put_str(out, v);
                }
                put_str(out, &r.event_id);
            }
        }
    }
}

fn liquidation(l: &Liquidation, id: &str) -> LiquidationRow {
    LiquidationRow {
        exchange: l.exchange.clone(),
        symbol: l.symbol.clone(),
        symbol_raw: l.symbol_raw.clone(),
        exchange_ts: clamp_ts(l.exchange_ts),
        recv_ts: clamp_ts(l.recv_ts),
        side: side_code(l.side),
        price: l.price,
        qty: l.qty,
        notional_usd: l.notional_usd,
        event_id: id.to_string(),
    }
}

fn open_interest(o: &OpenInterest, id: &str) -> OpenInterestRow {
    OpenInterestRow {
        exchange: o.exchange.clone(),
        symbol: o.symbol.clone(),
        symbol_raw: o.symbol_raw.clone(),
        exchange_ts: clamp_ts(o.exchange_ts),
        recv_ts: clamp_ts(o.recv_ts),
        oi_base: o.oi_base,
        oi_usd: o.oi_usd,
        mark_price: o.mark_price,
        event_id: id.to_string(),
    }
}

fn gap(g: &Gap, id: &str) -> GapRow {
    GapRow {
        exchange: g.exchange.clone(),
        stream: g.stream.clone(),
        from_ts: clamp_ts(g.from_ts),
        to_ts: clamp_ts(g.to_ts),
        event_id: id.to_string(),
    }
}

fn signal(s: &Signal, id: &str) -> SignalRow {
    SignalRow {
        kind: signal_type_code(s.r#type),
        exchange: s.exchange.clone(),
        symbol: s.symbol.clone(),
        window_secs: s.window_secs,
        change_pct: s.change_pct,
        change_abs: s.change_abs,
        threshold: s.threshold,
        ts: clamp_ts(s.ts),
        details: s
            .details
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        event_id: id.to_string(),
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_varint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

fn put_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_f64(out: &mut Vec<u8>, v: f64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_nullable_f64(out: &mut Vec<u8>, v: Option<f64>) {
    match v {
        Some(x) => {
            out.push(0);
            put_f64(out, x);
        }
        None => out.push(1),
    }
}

/// Why an entry produced no row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// `d` is not a valid `Event`.
    Undecodable,
    /// The event has no payload.
    Empty,
}

impl SkipReason {
    /// Metric label.
    pub fn label(self) -> &'static str {
        match self {
            SkipReason::Undecodable => "undecodable",
            SkipReason::Empty => "empty",
        }
    }
}

/// Rows of one table, already encoded.
#[derive(Debug, Default, Clone)]
pub struct TableBatch {
    /// Number of rows.
    pub rows: u64,
    /// `RowBinary` body.
    pub body: Vec<u8>,
}

/// The rows of a page of stream entries, grouped by table.
#[derive(Debug, Default, Clone)]
pub struct PageRows {
    /// Per table, indexed by [`Table::index`].
    pub tables: [TableBatch; 4],
    /// Entries that gave no row, with the reason.
    pub skipped: Vec<(String, SkipReason)>,
}

impl PageRows {
    /// Maps and encodes `entries`. An entry that cannot be decoded is skipped (and reported), never fatal: one
    /// corrupt entry must not stop the flusher for good.
    pub fn build(entries: Vec<StoredEntry>) -> Self {
        let mut page = PageRows::default();
        for entry in entries {
            let id = entry.id.clone();
            match entry.into_event() {
                Err(_) => page.skipped.push((id, SkipReason::Undecodable)),
                Ok(event) => match Row::from_event(&event, &id) {
                    None => page.skipped.push((id, SkipReason::Empty)),
                    Some(row) => {
                        let batch = &mut page.tables[row.table().index()];
                        row.encode(&mut batch.body);
                        batch.rows += 1;
                    }
                },
            }
        }
        page
    }

    /// Total rows over all tables.
    pub fn total_rows(&self) -> u64 {
        self.tables.iter().map(|t| t.rows).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::EntryFields;

    fn stored(event: &Event, id: &str) -> StoredEntry {
        let f = EntryFields::from_event(event).unwrap();
        StoredEntry {
            id: id.into(),
            kind: f.kind.into(),
            exchange: f.exchange,
            symbol: f.symbol,
            data: f.data,
        }
    }

    fn ev(payload: Payload) -> Event {
        Event {
            id: String::new(),
            payload: Some(payload),
        }
    }

    #[test]
    fn liquidation_maps_every_field() {
        let e = ev(Payload::Liquidation(Liquidation {
            exchange: "binance".into(),
            symbol_raw: "BTCUSDT".into(),
            symbol: "BTC-USDT-PERP".into(),
            exchange_ts: 1_700_000_000_123,
            recv_ts: 1_700_000_000_150,
            side: Side::Short as i32,
            price: 42_000.5,
            qty: 0.25,
            notional_usd: 10_500.125,
        }));
        let row = Row::from_event(&e, "1700000000151-3").unwrap();
        assert_eq!(
            row,
            Row::Liquidation(LiquidationRow {
                exchange: "binance".into(),
                symbol: "BTC-USDT-PERP".into(),
                symbol_raw: "BTCUSDT".into(),
                exchange_ts: 1_700_000_000_123,
                recv_ts: 1_700_000_000_150,
                side: 2,
                price: 42_000.5,
                qty: 0.25,
                notional_usd: 10_500.125,
                event_id: "1700000000151-3".into(),
            })
        );
        assert_eq!(row.table(), Table::Liquidations);
    }

    #[test]
    fn unknown_side_and_signal_type_become_unspecified() {
        assert_eq!(side_code(Side::Long as i32), 1);
        assert_eq!(side_code(0), 0);
        assert_eq!(side_code(99), 0);
        assert_eq!(signal_type_code(SignalType::LiqCascade as i32), 3);
        assert_eq!(signal_type_code(42), 0);
    }

    #[test]
    fn open_interest_keeps_nullable_values() {
        let with = ev(Payload::OpenInterest(OpenInterest {
            exchange: "bybit".into(),
            oi_base: 10.0,
            oi_usd: Some(5.0),
            mark_price: Some(0.5),
            ..OpenInterest::default()
        }));
        let without = ev(Payload::OpenInterest(OpenInterest {
            exchange: "bybit".into(),
            oi_base: 10.0,
            ..OpenInterest::default()
        }));
        let Some(Row::OpenInterest(a)) = Row::from_event(&with, "1-0") else {
            panic!()
        };
        let Some(Row::OpenInterest(b)) = Row::from_event(&without, "2-0") else {
            panic!()
        };
        assert_eq!((a.oi_usd, a.mark_price), (Some(5.0), Some(0.5)));
        assert_eq!((b.oi_usd, b.mark_price), (None, None));
        // 3 strings (1 byte each) + 2 i64 + f64 + (1 + 8) + (1 + 8) + event id (1 + 3).
        let (mut ba, mut bb) = (Vec::new(), Vec::new());
        Row::OpenInterest(a).encode(&mut ba);
        Row::OpenInterest(b).encode(&mut bb);
        assert_eq!(ba.len() - bb.len(), 16);
        // The NULL flags of the second row: both 1, no value bytes after them.
        let tail = &bb[bb.len() - 4 - 2..bb.len() - 4];
        assert_eq!(tail, &[1, 1]);
    }

    #[test]
    fn gap_and_signal_rows() {
        let g = ev(Payload::Gap(Gap {
            exchange: "*".into(),
            stream: "redis-writer".into(),
            from_ts: 10,
            to_ts: 20,
        }));
        assert_eq!(
            Row::from_event(&g, "5-1"),
            Some(Row::Gap(GapRow {
                exchange: "*".into(),
                stream: "redis-writer".into(),
                from_ts: 10,
                to_ts: 20,
                event_id: "5-1".into(),
            }))
        );
        let mut details = std::collections::HashMap::new();
        details.insert("b".to_string(), "2".to_string());
        details.insert("a".to_string(), "1".to_string());
        let s = ev(Payload::Signal(Signal {
            r#type: SignalType::OiSpikeDown as i32,
            exchange: "binance".into(),
            symbol: "ETH-USDT-PERP".into(),
            window_secs: 300,
            change_pct: -7.5,
            change_abs: -1000.0,
            threshold: 5.0,
            ts: 99,
            details,
        }));
        let Some(Row::Signal(row)) = Row::from_event(&s, "9-0") else {
            panic!()
        };
        assert_eq!(row.kind, 2);
        assert_eq!(row.window_secs, 300);
        assert_eq!(
            row.details.iter().collect::<Vec<_>>(),
            vec![
                (&"a".to_string(), &"1".to_string()),
                (&"b".to_string(), &"2".to_string())
            ]
        );
        assert!(Row::from_event(&Event::default(), "1-0").is_none());
    }

    #[test]
    fn timestamps_are_clamped() {
        assert_eq!(clamp_ts(-5), 0);
        assert_eq!(clamp_ts(i64::MAX), MAX_TS_MS);
        assert_eq!(clamp_ts(1_700_000_000_000), 1_700_000_000_000);
    }

    #[test]
    fn row_binary_layout_of_a_gap() {
        let row = Row::Gap(GapRow {
            exchange: "x".into(),
            stream: "ab".into(),
            from_ts: 1,
            to_ts: 258,
            event_id: "7-0".into(),
        });
        let mut out = Vec::new();
        row.encode(&mut out);
        let mut want = vec![1, b'x', 2, b'a', b'b'];
        want.extend_from_slice(&1i64.to_le_bytes());
        want.extend_from_slice(&258i64.to_le_bytes());
        want.extend_from_slice(&[3, b'7', b'-', b'0']);
        assert_eq!(out, want);
    }

    #[test]
    fn varint_handles_long_strings() {
        let mut out = Vec::new();
        put_varint(&mut out, 300);
        assert_eq!(out, vec![0xac, 0x02]);
    }

    #[test]
    fn page_groups_rows_and_reports_skips() {
        let liq = ev(Payload::Liquidation(Liquidation::default()));
        let oi = ev(Payload::OpenInterest(OpenInterest::default()));
        let entries = vec![
            stored(&liq, "1-0"),
            stored(&oi, "2-0"),
            StoredEntry {
                id: "3-0".into(),
                kind: "oi".into(),
                exchange: "x".into(),
                symbol: "y".into(),
                data: vec![0xff, 0xff],
            },
            stored(&oi, "4-0"),
            StoredEntry {
                id: "5-0".into(),
                kind: "oi".into(),
                exchange: "x".into(),
                symbol: "y".into(),
                data: Vec::new(),
            },
        ];
        let page = PageRows::build(entries);
        assert_eq!(page.tables[Table::Liquidations.index()].rows, 1);
        assert_eq!(page.tables[Table::OpenInterest.index()].rows, 2);
        assert_eq!(page.tables[Table::Gaps.index()].rows, 0);
        assert_eq!(page.total_rows(), 3);
        assert_eq!(
            page.skipped,
            vec![
                ("3-0".to_string(), SkipReason::Undecodable),
                ("5-0".to_string(), SkipReason::Empty)
            ]
        );
    }
}
