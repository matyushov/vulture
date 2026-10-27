//! [`ColdStore`] on ClickHouse: SQL from [`super::sql`], answers decoded from `JSONEachRow`.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use serde_json::Value;
use vulture_proto::v1::{
    Event, Gap, Liquidation, OpenInterest, Side, Signal, SignalType, event::Payload,
};

use super::agg::{FineData, GapRec, LiqBin, OiBin, Stamped};
use super::cold::{
    ColdStore, EventCursor, EventKey, EventQuery, FineAnswer, FineQuery, GapQuery, Kind,
    MoverAnswer, MoverInput, MoverQuery, TopLiqQuery,
};
use super::event::event_key;
use super::sql;
use crate::alerts::series::Sample;
use crate::history::{ChClient, ChError, ReadSettings, RowStream};
use crate::store::parse_id;

/// ClickHouse as the cold store.
pub struct ChCold {
    client: Arc<ChClient>,
}

impl ChCold {
    /// Wraps a client.
    pub fn new(client: Arc<ChClient>) -> Self {
        Self { client }
    }
}

fn settings(max_memory: u64, timeout: Duration) -> ReadSettings {
    ReadSettings {
        max_execution_time_secs: timeout.as_secs().max(1),
        max_memory_usage: max_memory,
        timeout,
    }
}

// ---------------------------------------------------------------- JSON helpers

fn text(v: &Value, key: &str) -> String {
    v[key].as_str().unwrap_or_default().to_string()
}

fn num(v: &Value, key: &str) -> f64 {
    as_f64(&v[key]).unwrap_or(0.0)
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn opt_num(v: &Value, key: &str) -> Option<f64> {
    as_f64(&v[key])
}

fn int(v: &Value, key: &str) -> i64 {
    as_i64(&v[key])
}

fn as_i64(v: &Value) -> i64 {
    match v {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .unwrap_or(0),
        Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn uint(v: &Value, key: &str) -> u64 {
    crate::history::client::json_u64(&v[key])
}

// ---------------------------------------------------------------- events

pub(crate) fn decode_event(kind: Kind, v: &Value) -> Event {
    let id = text(v, "event_id");
    let payload = match kind {
        Kind::Liquidation => Payload::Liquidation(Liquidation {
            exchange: text(v, "exchange"),
            symbol_raw: text(v, "symbol_raw"),
            symbol: text(v, "symbol"),
            exchange_ts: int(v, "t_ms"),
            recv_ts: int(v, "rts"),
            side: match v["side"].as_str() {
                Some("long") => Side::Long as i32,
                Some("short") => Side::Short as i32,
                _ => Side::Unspecified as i32,
            },
            price: num(v, "price"),
            qty: num(v, "qty"),
            notional_usd: num(v, "notional_usd"),
        }),
        Kind::OpenInterest => Payload::OpenInterest(OpenInterest {
            exchange: text(v, "exchange"),
            symbol_raw: text(v, "symbol_raw"),
            symbol: text(v, "symbol"),
            exchange_ts: int(v, "t_ms"),
            recv_ts: int(v, "rts"),
            oi_base: num(v, "oi_base"),
            oi_usd: opt_num(v, "oi_usd"),
            mark_price: opt_num(v, "mark_price"),
            funding_rate: opt_num(v, "funding_rate"),
            index_price: opt_num(v, "index_price"),
            next_funding_ts: v["nf_ms"].as_i64(),
        }),
        Kind::Gap => Payload::Gap(Gap {
            exchange: text(v, "exchange"),
            stream: text(v, "stream"),
            from_ts: int(v, "t_ms"),
            to_ts: int(v, "to_ms"),
        }),
        Kind::Signal => Payload::Signal(Signal {
            r#type: match v["type"].as_str() {
                Some("oi_spike_up") => SignalType::OiSpikeUp as i32,
                Some("oi_spike_down") => SignalType::OiSpikeDown as i32,
                Some("liq_cascade") => SignalType::LiqCascade as i32,
                _ => SignalType::Unspecified as i32,
            },
            exchange: text(v, "exchange"),
            symbol: text(v, "symbol"),
            window_secs: uint(v, "window_secs") as u32,
            change_pct: num(v, "change_pct"),
            change_abs: num(v, "change_abs"),
            threshold: num(v, "threshold"),
            ts: int(v, "t_ms"),
            details: v["details"]
                .as_object()
                .map(|m| {
                    m.iter()
                        .map(|(k, val)| (k.clone(), val.as_str().unwrap_or_default().to_string()))
                        .collect()
                })
                .unwrap_or_default(),
        }),
    };
    Event {
        id,
        payload: Some(payload),
    }
}

struct KindCursor {
    kind: Kind,
    rows: RowStream,
    head: Option<Event>,
    done: bool,
}

/// Merges the per-kind answers (each sorted by ClickHouse) into one stream in answer order.
struct MergedCursor {
    parts: Vec<KindCursor>,
    scanned: u64,
}

impl MergedCursor {
    async fn pull(&mut self) -> Result<Option<Event>, ChError> {
        for p in &mut self.parts {
            if p.head.is_none() && !p.done {
                match p.rows.next_row().await? {
                    Some(row) => {
                        self.scanned += 1;
                        p.head = Some(decode_event(p.kind, &row));
                    }
                    None => p.done = true,
                }
            }
        }
        let mut best: Option<(usize, EventKey)> = None;
        for (i, p) in self.parts.iter().enumerate() {
            let Some(e) = &p.head else { continue };
            let key = event_key(e).unwrap_or(EventKey {
                ts: 0,
                id_ms: 0,
                id_seq: 0,
            });
            if best.is_none_or(|(_, bk)| key < bk) {
                best = Some((i, key));
            }
        }
        Ok(best.and_then(|(i, _)| self.parts[i].head.take()))
    }
}

impl EventCursor for MergedCursor {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Event>, ChError>> {
        Box::pin(self.pull())
    }

    fn scanned(&self) -> u64 {
        self.scanned
    }
}

// ---------------------------------------------------------------- cells

fn decode_liq_cells(rows: &[Value], data: &mut FineData) {
    for r in rows {
        let cell = LiqBin {
            count: uint(r, "n"),
            sum: num(r, "s"),
            long: num(r, "l"),
            short: num(r, "sh"),
            max: num(r, "m"),
        };
        data.liq
            .entry((text(r, "exchange"), text(r, "symbol")))
            .or_default()
            .entry(int(r, "b"))
            .or_default()
            .merge(&cell);
    }
}

fn decode_oi_cells(rows: &[Value], data: &mut FineData) {
    for r in rows {
        let usd = (uint(r, "usd_n") > 0).then(|| Stamped {
            ts: int(r, "usd_ts"),
            v: num(r, "usd"),
        });
        let mark = |key: &str, ts: &str| {
            (uint(r, "mark_n") > 0).then(|| Stamped {
                ts: int(r, ts),
                v: num(r, key),
            })
        };
        let has_marks = r.get("mark_n").is_some();
        let cell = OiBin {
            n: uint(r, "n"),
            first_ts: int(r, "first_ts"),
            last_ts: int(r, "last_ts"),
            last_base: num(r, "last_base"),
            last_usd: usd,
            mark_open: if has_marks {
                mark("mark_open", "mark_open_ts")
            } else {
                None
            },
            mark_close: if has_marks {
                mark("mark_close", "mark_close_ts")
            } else {
                None
            },
            mark_high: if has_marks && uint(r, "mark_n") > 0 {
                opt_num(r, "mark_high")
            } else {
                None
            },
            mark_low: if has_marks && uint(r, "mark_n") > 0 {
                opt_num(r, "mark_low")
            } else {
                None
            },
            funding_last: (uint(r, "f_n") > 0).then(|| Stamped {
                ts: int(r, "f_ts"),
                v: num(r, "f_last"),
            }),
            funding_sum: if r.get("f_n").is_some() {
                num(r, "f_sum")
            } else {
                0.0
            },
            funding_n: uint(r, "f_n"),
            basis_last: (uint(r, "b_n") > 0).then(|| Stamped {
                ts: int(r, "b_ts"),
                v: num(r, "b_last"),
            }),
            basis_sum: if r.get("b_n").is_some() {
                num(r, "b_sum")
            } else {
                0.0
            },
            basis_n: uint(r, "b_n"),
        };
        data.oi
            .entry((text(r, "exchange"), text(r, "symbol")))
            .or_default()
            .entry(int(r, "b"))
            .or_default()
            .merge(&cell);
    }
}

// ---------------------------------------------------------------- movers

fn sample(ts: i64, base: &Value, usd: &Value, mark: &Value) -> Sample {
    Sample {
        ts,
        oi_base: as_f64(base).unwrap_or(f64::NAN),
        oi_usd: as_f64(usd).unwrap_or(f64::NAN),
        mark: as_f64(mark).unwrap_or(f64::NAN),
    }
}

fn decode_mover(r: &Value) -> Option<MoverInput> {
    let last = r["last_t"].as_array()?;
    let last = sample(
        int(r, "last_ts"),
        last.first()?,
        last.get(1).unwrap_or(&Value::Null),
        last.get(2).unwrap_or(&Value::Null),
    );
    let candidates = r["cands"]
        .as_array()?
        .iter()
        .filter_map(|c| {
            let c = c.as_array()?;
            Some(sample(
                as_i64(c.first()?),
                c.get(1)?,
                c.get(2).unwrap_or(&Value::Null),
                c.get(3).unwrap_or(&Value::Null),
            ))
        })
        .collect();
    Some(MoverInput {
        key: (text(r, "exchange"), text(r, "symbol")),
        last,
        candidates,
        hole_ts: int(r, "hole_ts"),
    })
}

// ---------------------------------------------------------------- the trait

impl ColdStore for ChCold {
    fn top_liquidations(&self, q: TopLiqQuery) -> BoxFuture<'_, Result<Vec<Event>, ChError>> {
        Box::pin(async move {
            let st = settings(q.max_memory, q.timeout);
            let (rows, _) = self
                .client
                .query_rows_with(&sql::top_liquidations(&q), &st)
                .await?;
            Ok(rows
                .iter()
                .map(|r| decode_event(Kind::Liquidation, r))
                .collect())
        })
    }

    fn events(&self, q: EventQuery) -> BoxFuture<'_, Result<Box<dyn EventCursor>, ChError>> {
        Box::pin(async move {
            let st = settings(q.max_memory, q.timeout);
            let mut parts = Vec::new();
            for kind in &q.kinds {
                let rows = self
                    .client
                    .query_stream(&sql::events(*kind, &q), &st)
                    .await?;
                parts.push(KindCursor {
                    kind: *kind,
                    rows,
                    head: None,
                    done: false,
                });
            }
            Ok(Box::new(MergedCursor { parts, scanned: 0 }) as Box<dyn EventCursor>)
        })
    }

    fn event_key(
        &self,
        id: String,
        q: EventQuery,
    ) -> BoxFuture<'_, Result<Option<EventKey>, ChError>> {
        Box::pin(async move {
            let Some((id_ms, id_seq)) = parse_id(&id) else {
                return Ok(None);
            };
            let st = settings(q.max_memory, q.timeout);
            for kind in &q.kinds {
                let (rows, _) = self
                    .client
                    .query_rows_with(&sql::event_time(*kind, &id, &q), &st)
                    .await?;
                if let Some(row) = rows.first() {
                    return Ok(Some(EventKey {
                        ts: int(row, "t_ms"),
                        id_ms,
                        id_seq,
                    }));
                }
            }
            Ok(None)
        })
    }

    fn fine(&self, q: FineQuery) -> BoxFuture<'_, Result<FineAnswer, ChError>> {
        Box::pin(async move {
            let st = settings(q.max_memory, q.timeout);
            let mut answer = FineAnswer::default();
            if q.needs.liq {
                let (rows, summary) = self
                    .client
                    .query_rows_with(&sql::fine_liquidations(&q), &st)
                    .await?;
                answer.scanned += summary.read_rows;
                if rows.len() > q.max_cells {
                    answer.truncated = true;
                    return Ok(answer);
                }
                decode_liq_cells(&rows, &mut answer.data);
            }
            if q.needs.oi {
                let (rows, summary) = self
                    .client
                    .query_rows_with(&sql::fine_open_interest(&q), &st)
                    .await?;
                answer.scanned += summary.read_rows;
                if rows.len() + answer.data.cells() > q.max_cells {
                    answer.truncated = true;
                    return Ok(answer);
                }
                decode_oi_cells(&rows, &mut answer.data);
            }
            Ok(answer)
        })
    }

    fn gaps(&self, q: GapQuery) -> BoxFuture<'_, Result<Vec<GapRec>, ChError>> {
        Box::pin(async move {
            let st = settings(1 << 30, q.timeout);
            let (rows, _) = self
                .client
                .query_rows_with(&sql::gaps(q.from, q.to, &q.exchanges), &st)
                .await?;
            Ok(rows
                .iter()
                .map(|r| GapRec {
                    exchange: text(r, "exchange"),
                    stream: text(r, "stream"),
                    from_ts: int(r, "f"),
                    to_ts: int(r, "t"),
                })
                .collect())
        })
    }

    fn movers(&self, q: MoverQuery) -> BoxFuture<'_, Result<MoverAnswer, ChError>> {
        Box::pin(async move {
            let st = settings(q.max_memory, q.timeout);
            let (rows, summary) = self.client.query_rows_with(&sql::movers(&q), &st).await?;
            Ok(MoverAnswer {
                series: rows.iter().filter_map(decode_mover).collect(),
                scanned: summary.read_rows,
            })
        })
    }
}
