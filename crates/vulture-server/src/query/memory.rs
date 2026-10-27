//! [`ColdStore`] over a vector of events: the reference semantics of the cold side (what the SQL must produce),
//! the fake for the service tests and the stand-in when a test needs no ClickHouse.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::future::BoxFuture;
use vulture_proto::v1::{Event, event::Payload};

use super::agg::{FineData, GapRec, SeriesKey};
use super::cold::{
    ColdStore, EventCursor, EventKey, EventQuery, FineAnswer, FineQuery, GapQuery, MoverAnswer,
    MoverInput, MoverQuery, TopLiqQuery,
};
use super::event::{EventFilter, event_key, in_period};
use crate::alerts::series::Sample;
use crate::history::ChError;

/// Events held in memory. Every event must carry its stream ID (`Event.id`).
#[derive(Default)]
pub struct MemoryCold {
    events: Mutex<Vec<Event>>,
    failing: AtomicBool,
    queries: std::sync::atomic::AtomicU64,
}

impl MemoryCold {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds events (with IDs).
    pub fn insert(&self, events: impl IntoIterator<Item = Event>) {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(events);
    }

    /// Number of events held.
    pub fn len(&self) -> usize {
        self.events.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// True when empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// While `true`, every call fails with a connection error (an unreachable ClickHouse).
    pub fn set_failing(&self, failing: bool) {
        self.failing.store(failing, Ordering::SeqCst);
    }

    /// Calls answered so far.
    pub fn queries(&self) -> u64 {
        self.queries.load(Ordering::SeqCst)
    }

    fn check(&self) -> Result<(), ChError> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        if self.failing.load(Ordering::SeqCst) {
            return Err(ChError::Connection("memory cold store switched off".into()));
        }
        Ok(())
    }

    fn snapshot(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn matching(&self, q: &EventQuery) -> Vec<(EventKey, Event)> {
        let filter = EventFilter::new(&q.kinds, &q.scope);
        let mut out: Vec<(EventKey, Event)> = self
            .snapshot()
            .into_iter()
            .filter(|e| filter.matches_event(e) && in_period(e, q.from_ts, q.to_ts))
            .filter_map(|e| event_key(&e).map(|k| (k, e)))
            .collect();
        out.sort_by_key(|(k, _)| *k);
        out
    }
}

struct VecCursor {
    events: std::vec::IntoIter<Event>,
    scanned: u64,
}

impl EventCursor for VecCursor {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Event>, ChError>> {
        Box::pin(async move { Ok(self.events.next()) })
    }

    fn scanned(&self) -> u64 {
        self.scanned
    }
}

impl ColdStore for MemoryCold {
    fn top_liquidations(&self, q: TopLiqQuery) -> BoxFuture<'_, Result<Vec<Event>, ChError>> {
        Box::pin(async move {
            self.check()?;
            let mut v: Vec<(f64, EventKey, Event)> = self
                .snapshot()
                .into_iter()
                .filter_map(|e| match &e.payload {
                    Some(Payload::Liquidation(l))
                        if l.exchange_ts >= q.from && l.exchange_ts < q.to =>
                    {
                        event_key(&e).map(|k| (l.notional_usd, k, e.clone()))
                    }
                    _ => None,
                })
                .collect();
            v.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            v.truncate(q.n);
            Ok(v.into_iter().map(|x| x.2).collect())
        })
    }

    fn events(&self, q: EventQuery) -> BoxFuture<'_, Result<Box<dyn EventCursor>, ChError>> {
        Box::pin(async move {
            self.check()?;
            let mut rows = self.matching(&q);
            if let Some(after) = q.after {
                rows.retain(|(k, _)| *k > after);
            }
            rows.truncate(q.limit);
            let scanned = rows.len() as u64;
            Ok(Box::new(VecCursor {
                events: rows
                    .into_iter()
                    .map(|(_, e)| e)
                    .collect::<Vec<_>>()
                    .into_iter(),
                scanned,
            }) as Box<dyn EventCursor>)
        })
    }

    fn event_key(
        &self,
        id: String,
        mut q: EventQuery,
    ) -> BoxFuture<'_, Result<Option<EventKey>, ChError>> {
        Box::pin(async move {
            self.check()?;
            q.after = None;
            Ok(self
                .matching(&q)
                .into_iter()
                .find(|(_, e)| e.id == id)
                .map(|(k, _)| k))
        })
    }

    fn fine(&self, q: FineQuery) -> BoxFuture<'_, Result<FineAnswer, ChError>> {
        Box::pin(async move {
            self.check()?;
            let mut data = FineData::default();
            let mut scanned = 0u64;
            for e in self.snapshot() {
                if let Some(b) = q.id_bound
                    && event_key(&e).is_some_and(|k| (k.id_ms, k.id_seq) > (b.ms, b.seq))
                {
                    continue;
                }
                match e.payload.as_ref() {
                    Some(Payload::Liquidation(l))
                        if q.needs.liq
                            && l.exchange_ts >= q.from
                            && l.exchange_ts < q.to
                            && q.scope.matches(&l.exchange, &l.symbol) =>
                    {
                        scanned += 1;
                        data.add_liquidation(q.step, l);
                    }
                    Some(Payload::OpenInterest(o))
                        if q.needs.oi
                            && o.exchange_ts >= q.from
                            && o.exchange_ts < q.to
                            && q.scope.matches(&o.exchange, &o.symbol) =>
                    {
                        scanned += 1;
                        data.add_oi(q.step, o);
                    }
                    _ => {}
                }
            }
            let truncated = data.cells() > q.max_cells;
            if truncated {
                data = FineData::default();
            }
            Ok(FineAnswer {
                data,
                scanned,
                truncated,
            })
        })
    }

    fn gaps(&self, q: GapQuery) -> BoxFuture<'_, Result<Vec<GapRec>, ChError>> {
        Box::pin(async move {
            self.check()?;
            let mut out: Vec<GapRec> = self
                .snapshot()
                .into_iter()
                .filter_map(|e| match e.payload {
                    Some(Payload::Gap(g))
                        if g.from_ts < q.to
                            && g.to_ts >= q.from
                            && (q.exchanges.is_empty()
                                || g.exchange == "*"
                                || q.exchanges.contains(&g.exchange)) =>
                    {
                        Some(GapRec {
                            exchange: g.exchange,
                            stream: g.stream,
                            from_ts: g.from_ts,
                            to_ts: g.to_ts,
                        })
                    }
                    _ => None,
                })
                .collect();
            out.sort_by_key(|g| g.from_ts);
            Ok(out)
        })
    }

    fn movers(&self, q: MoverQuery) -> BoxFuture<'_, Result<MoverAnswer, ChError>> {
        Box::pin(async move {
            self.check()?;
            let mut by_series: BTreeMap<SeriesKey, Vec<Sample>> = BTreeMap::new();
            for e in self.snapshot() {
                let Some(Payload::OpenInterest(o)) = e.payload else {
                    continue;
                };
                if o.recv_ts < q.from
                    || o.recv_ts > q.end
                    || o.exchange_ts < q.from - 120_000
                    || o.exchange_ts > q.end + 120_000
                    || !q.scope.matches(&o.exchange, &o.symbol)
                {
                    continue;
                }
                by_series
                    .entry((o.exchange.clone(), o.symbol.clone()))
                    .or_default()
                    .push(Sample {
                        ts: o.recv_ts,
                        oi_base: o.oi_base,
                        oi_usd: o.oi_usd.unwrap_or(f64::NAN),
                        mark: o.mark_price.unwrap_or(f64::NAN),
                    });
            }
            let mut series = Vec::new();
            let mut scanned = 0u64;
            for (key, mut samples) in by_series {
                scanned += samples.len() as u64;
                samples.sort_by_key(|s| s.ts);
                let Some(last) = samples.last().copied() else {
                    continue;
                };
                if last.ts < q.min_last {
                    continue;
                }
                let mut hole_ts = 0;
                for w in samples.windows(2) {
                    if w[1].ts - w[0].ts > q.hole_ms {
                        hole_ts = hole_ts.max(w[1].ts);
                    }
                }
                let candidates = samples
                    .iter()
                    .filter(|s| s.ts >= q.cand_from && s.ts <= q.cand_to)
                    .copied()
                    .collect();
                series.push(MoverInput {
                    key,
                    last,
                    candidates,
                    hole_ts,
                });
            }
            Ok(MoverAnswer { series, scanned })
        })
    }
}
