//! `History.Query`: events of a period from ClickHouse and the unflushed Redis tail, merged without loss or
//! duplicates.
//!
//! # Sources and the boundary
//!
//! With ClickHouse configured, everything the flusher has acknowledged (stream IDs up to its watermark) is read
//! from ClickHouse and everything after the watermark from Redis. The two overlap only in the moment between an
//! acknowledged insert and the watermark update, and after a crash that interrupted exactly that; an event of the
//! tail that ClickHouse also returns is dropped from the ClickHouse side by its `event_id`. Without ClickHouse the
//! Redis hot window is the only source. If ClickHouse is unreachable and the Redis window still covers the start of
//! the period, the answer comes from Redis alone (counted in `vulture_history_redis_fallbacks_total`).
//!
//! # Order and resume
//!
//! Events are ordered by (time, stream ID as numbers); the time is defined in [`super::event`]. `cursor` is the
//! `Event.id` of the last event received: the answer continues strictly after it. The cursor's own time is looked up
//! in the tail and then in ClickHouse, restricted to the period and filters of the request, so a resumed call must
//! repeat the same period and filters.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::{Code, Status};
use vulture_proto::v1::{Event, HistoryRequest};

use super::cold::{EventCursor, EventKey, EventQuery, Kind, Scope};
use super::event::{EventFilter, event_key, in_period, kinds_from_types};
use super::service::{HistoryService, ch_status, scope_of, store_status};
use super::stats::{Method, Reject};
use super::tail::{TailPlan, after_watermark, id_for_time, max_id, scan};
use crate::history::ChError;
use crate::store::parse_id;

/// A validated `Query`.
#[derive(Debug, Clone)]
pub(super) struct QueryPlan {
    kinds: Vec<Kind>,
    scope: Scope,
    from_ts: i64,
    to_ts: i64,
    limit: usize,
    cursor: Option<String>,
}

enum Mode {
    /// ClickHouse for what is flushed, Redis for the rest (or Redis alone when ClickHouse is not configured).
    Normal,
    /// ClickHouse failed: Redis alone.
    RedisOnly,
}

/// How a production attempt failed.
enum Failure {
    /// ClickHouse failed; `retryable` when nothing has been sent yet.
    Cold(ChError),
    Status(Status),
}

impl From<Status> for Failure {
    fn from(s: Status) -> Self {
        Failure::Status(s)
    }
}

impl HistoryService {
    pub(super) fn plan_query(&self, req: &HistoryRequest) -> Result<QueryPlan, Status> {
        let now = self.clock.now_ms();
        if req.from_ts < 0 || req.to_ts < 0 {
            return Err(Status::invalid_argument(
                "from_ts and to_ts must not be negative",
            ));
        }
        let to_ts = if req.to_ts == 0 { now + 1 } else { req.to_ts };
        if req.from_ts >= to_ts {
            return Err(Status::invalid_argument(
                "from_ts must be before to_ts (milliseconds since the Unix epoch; to_ts = 0 means now)",
            ));
        }
        let max_range = (self.cfg.max_query_range_secs as i64).saturating_mul(1000);
        if to_ts - req.from_ts > max_range {
            return Err(self.reject(
                Reject::Range,
                Status::out_of_range(format!(
                    "the period is {} s long, the limit is {} s (history.max_query_range_secs); split it into shorter periods, or use History.Aggregate for a long overview",
                    (to_ts - req.from_ts) / 1000,
                    self.cfg.max_query_range_secs
                )),
            ));
        }
        let limit = if req.limit == 0 {
            self.cfg.default_limit
        } else {
            req.limit
        };
        if limit > self.cfg.max_rows {
            return Err(self.reject(
                Reject::Limit,
                Status::resource_exhausted(format!(
                    "limit {limit} is above the row cap of {} (history.max_rows); ask for fewer events and continue with the cursor",
                    self.cfg.max_rows
                )),
            ));
        }
        let kinds = kinds_from_types(&req.types).map_err(|raw| {
            Status::invalid_argument(format!("types: {raw} is not a known event type"))
        })?;
        let scope = scope_of(&req.exchanges, &req.symbols, &[])?;
        let cursor = if req.cursor.is_empty() {
            None
        } else if parse_id(&req.cursor).is_some() {
            Some(req.cursor.clone())
        } else {
            return Err(Status::invalid_argument(
                "cursor must be the Event.id of the last event received (<ms>-<seq>)",
            ));
        };
        Ok(QueryPlan {
            kinds,
            scope,
            from_ts: req.from_ts,
            to_ts,
            limit: limit as usize,
            cursor,
        })
    }

    /// Validates the request, starts the producer task and returns the receiving end of the stream.
    pub(super) fn start_query(
        self: Arc<Self>,
        req: HistoryRequest,
    ) -> Result<mpsc::Receiver<Result<Event, Status>>, Status> {
        let mut call = self.begin(Method::Query)?;
        let plan = match self.plan_query(&req) {
            Ok(p) => p,
            Err(s) => {
                call.finish(s.code());
                return Err(s);
            }
        };
        let (tx, rx) = mpsc::channel(256);
        let svc = self.clone();
        tokio::spawn(async move {
            let limit = svc.timeout();
            let result = match tokio::time::timeout(limit, svc.produce(&plan, &tx)).await {
                Ok(r) => r,
                Err(_) => Err(Status::deadline_exceeded(format!(
                    "the query did not finish within {} s (history.request_timeout_secs); ask for fewer events and continue with the cursor",
                    limit.as_secs()
                ))),
            };
            match result {
                Ok(()) => call.finish(Code::Ok),
                Err(status) => {
                    call.finish(status.code());
                    // The client may be gone already; then there is nobody to tell.
                    let _ = tx.send(Err(status)).await;
                }
            }
        });
        Ok(rx)
    }

    async fn produce(
        &self,
        plan: &QueryPlan,
        tx: &mpsc::Sender<Result<Event, Status>>,
    ) -> Result<(), Status> {
        let mut sent = 0u64;
        let first = self.produce_with(plan, tx, Mode::Normal, &mut sent).await;
        match first {
            Ok(()) => Ok(()),
            Err(Failure::Status(s)) => Err(s),
            Err(Failure::Cold(e)) => {
                self.stats.clickhouse_error();
                if sent == 0 && self.redis_covers(plan.from_ts).await {
                    tracing::warn!(error = %e, "ClickHouse failed; answering History.Query from the Redis hot window");
                    self.stats.redis_fallback();
                    match self
                        .produce_with(plan, tx, Mode::RedisOnly, &mut sent)
                        .await
                    {
                        Ok(()) => Ok(()),
                        Err(Failure::Status(s)) => Err(s),
                        Err(Failure::Cold(e)) => Err(ch_status(&e)),
                    }
                } else {
                    Err(ch_status(&e))
                }
            }
        }
    }

    async fn produce_with(
        &self,
        plan: &QueryPlan,
        tx: &mpsc::Sender<Result<Event, Status>>,
        mode: Mode,
        sent: &mut u64,
    ) -> Result<(), Failure> {
        let cold = match mode {
            Mode::Normal => self.cold.clone(),
            Mode::RedisOnly => None,
        };
        let slack = self.slack();
        let filter = EventFilter::new(&plan.kinds, &plan.scope);
        let wants_gaps = plan.kinds.contains(&Kind::Gap);

        // 1. The unflushed tail (or the whole answer's Redis part without ClickHouse).
        let wm = match &cold {
            Some(_) => Some(
                self.current_watermark()
                    .await
                    .map_err(|e| store_status(&e))?,
            ),
            None => None,
        };
        let mut tail: Vec<Event> = Vec::new();
        let tail_needed = match wm {
            Some((wm_ms, _)) => {
                wants_gaps || (wm_ms as i64) < plan.to_ts + slack.as_millis() as i64
            }
            None => true,
        };
        if tail_needed {
            let by_time = id_for_time(plan.from_ts, slack);
            let start = match wm {
                Some(w) => max_id(&after_watermark(w), &by_time),
                None => by_time,
            };
            let tail_plan = TailPlan {
                start,
                // With ClickHouse the tail is short, read it to the end. Without it, stop where no entry can
                // still belong to the period.
                stop_after_ms: cold
                    .is_none()
                    .then(|| (plan.to_ts + slack.as_millis() as i64).max(0) as u64),
                page: self.cfg.tail_page as usize,
                max_entries: self.cfg.tail_max_entries as usize,
            };
            let mut too_many = false;
            let redis = scan(self.store.as_ref(), &tail_plan, |entry| {
                if tail.len() >= self.cfg.tail_max_entries as usize {
                    too_many = true;
                    return;
                }
                if !filter.matches_entry(&entry) {
                    return;
                }
                if let Ok(event) = entry.into_event()
                    && in_period(&event, plan.from_ts, plan.to_ts)
                {
                    tail.push(event);
                }
            })
            .await
            .map_err(|e| store_status(&e))?;
            self.stats.scanned_redis(redis.scanned);
            if redis.truncated || too_many {
                return Err(self
                    .reject(
                        Reject::Rows,
                        Status::resource_exhausted(format!(
                            "more than {} Redis entries would have to be read for this period (history.tail_max_entries); {}",
                            self.cfg.tail_max_entries,
                            if cold.is_some() {
                                "the ClickHouse flusher is far behind"
                            } else {
                                "enable ClickHouse history or narrow the period and the filters"
                            }
                        )),
                    )
                    .into());
            }
            if cold.is_some() && !tail.is_empty() {
                self.stats.tail_merge();
            }
        }

        // 2. Where to resume.
        let after: Option<EventKey> = match &plan.cursor {
            None => None,
            Some(id) => {
                let mut key = tail.iter().find(|e| &e.id == id).and_then(event_key);
                if key.is_none()
                    && let Some(cold) = &cold
                {
                    key = cold
                        .event_key(id.clone(), self.event_query(plan, None, 1))
                        .await
                        .map_err(Failure::Cold)?;
                }
                Some(key.ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "cursor {id} is not an event of the requested period and filters; repeat the original period, exchanges, symbols and types to resume"
                    ))
                })?)
            }
        };
        tail.retain(|e| match (after, event_key(e)) {
            (Some(a), Some(k)) => k > a,
            (_, None) => false,
            (None, Some(_)) => true,
        });
        tail.sort_by_key(|e| {
            event_key(e).unwrap_or(EventKey {
                ts: 0,
                id_ms: 0,
                id_seq: 0,
            })
        });
        let tail_ids: HashSet<String> = tail.iter().map(|e| e.id.clone()).collect();

        // 3. ClickHouse, then the merge.
        let mut cursor: Option<Box<dyn EventCursor>> = match &cold {
            Some(cold) => Some(
                cold.events(self.event_query(plan, after, plan.limit + tail.len()))
                    .await
                    .map_err(Failure::Cold)?,
            ),
            None => None,
        };
        let mut cold_head = next_cold(&mut cursor, &tail_ids).await?;
        let mut tail_iter = tail.into_iter().peekable();
        let mut emitted = 0usize;
        while emitted < plan.limit {
            let take_tail = match (tail_iter.peek(), &cold_head) {
                (Some(t), Some(c)) => key_of(t) <= key_of(c),
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let event = if take_tail {
                tail_iter.next().unwrap_or_default()
            } else {
                let e = cold_head.take().unwrap_or_default();
                cold_head = next_cold(&mut cursor, &tail_ids).await?;
                e
            };
            if tx.send(Ok(event)).await.is_err() {
                // The client went away.
                if let Some(c) = &cursor {
                    self.stats.scanned_clickhouse(c.scanned());
                }
                return Err(Status::cancelled("the client closed the stream").into());
            }
            emitted += 1;
            *sent += 1;
            self.stats.events(1);
        }
        if let Some(c) = &cursor {
            self.stats.scanned_clickhouse(c.scanned());
        }
        Ok(())
    }

    fn event_query(&self, plan: &QueryPlan, after: Option<EventKey>, limit: usize) -> EventQuery {
        EventQuery {
            kinds: plan.kinds.clone(),
            scope: plan.scope.clone(),
            from_ts: plan.from_ts,
            to_ts: plan.to_ts,
            after,
            limit,
            max_memory: self.cfg.clickhouse_max_memory_bytes,
            timeout: self.timeout(),
        }
    }
}

fn key_of(e: &Event) -> EventKey {
    event_key(e).unwrap_or(EventKey {
        ts: i64::MAX,
        id_ms: u64::MAX,
        id_seq: u64::MAX,
    })
}

/// The next ClickHouse event that is not also in the tail.
async fn next_cold(
    cursor: &mut Option<Box<dyn EventCursor>>,
    tail_ids: &HashSet<String>,
) -> Result<Option<Event>, Failure> {
    let Some(c) = cursor else { return Ok(None) };
    loop {
        match c.next().await.map_err(Failure::Cold)? {
            Some(e) if tail_ids.contains(&e.id) => continue,
            other => return Ok(other),
        }
    }
}
