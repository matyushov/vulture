//! The `History` gRPC service: validation, limits, the shared plumbing of its three operations.

use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status};
use vulture_proto::v1::{
    AggregateRequest, AggregateResponse, Event, HistoryRequest, history_server::History,
};

use super::cold::{ColdStore, Scope};
use super::stats::{HistoryStats, Method, Reject};
use crate::config::{AlertsConfig, HistoryApiConfig};
use crate::history::{ChError, Watermark};
use crate::store::{RedisStore, StoreError, parse_id};
use crate::ws::Clock;

/// The parameters of an OI series that the movers ranking must share with the live detector.
#[derive(Debug, Clone, Copy)]
pub struct SeriesParams {
    /// Samples closer than this to the previous one are ignored, ms (`alerts.sample_spacing_secs`).
    pub spacing_ms: i64,
    /// A pause longer than this between two samples is a hole, ms (`alerts.hole_secs`).
    pub hole_ms: i64,
}

impl SeriesParams {
    /// The detector's parameters.
    pub fn from_alerts(cfg: &AlertsConfig) -> Self {
        Self {
            spacing_ms: (cfg.sample_spacing_secs as i64) * 1000,
            hole_ms: (cfg.hole_secs as i64) * 1000,
        }
    }
}

impl Default for SeriesParams {
    fn default() -> Self {
        Self::from_alerts(&AlertsConfig::default())
    }
}

/// The service state.
pub struct HistoryService {
    pub(super) store: Arc<dyn RedisStore>,
    pub(super) cold: Option<Arc<dyn ColdStore>>,
    pub(super) watermark: Option<Arc<Watermark>>,
    pub(super) cfg: HistoryApiConfig,
    pub(super) series: SeriesParams,
    pub(super) clock: Arc<dyn Clock>,
    pub(super) stats: Arc<HistoryStats>,
    permits: Arc<Semaphore>,
}

impl std::fmt::Debug for HistoryService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryService")
            .field("clickhouse", &self.cold.is_some())
            .finish_non_exhaustive()
    }
}

/// One running call: holds the concurrency permit and records the outcome.
pub(super) struct Call {
    svc: Arc<HistoryService>,
    method: Method,
    started: Instant,
    code: Option<Code>,
    _permit: OwnedSemaphorePermit,
}

impl Call {
    /// Records the outcome (the first one wins).
    pub(super) fn finish(&mut self, code: Code) {
        if self.code.is_none() {
            self.code = Some(code);
            self.svc.stats.finished(
                self.method,
                code,
                self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            );
        }
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        // A call that never reported (the client went away, the task was aborted) counts as cancelled.
        self.finish(Code::Cancelled);
        self.svc.stats.leave();
    }
}

/// A `Status` for a failed ClickHouse call.
pub(super) fn ch_status(e: &ChError) -> Status {
    Status::unavailable(format!(
        "the history store (ClickHouse) failed: {e}; try again, or ask for a period that is still in the Redis hot window"
    ))
}

/// A `Status` for a failed Redis call.
pub(super) fn store_status(e: &StoreError) -> Status {
    Status::unavailable(format!("Redis failed: {e}"))
}

const MAX_NAMES: usize = 1000;
const MAX_NAME_LEN: usize = 64;

pub(super) fn check_names(what: &str, names: &[String]) -> Result<(), Status> {
    if names.len() > MAX_NAMES {
        return Err(Status::invalid_argument(format!(
            "{what}: at most {MAX_NAMES} entries"
        )));
    }
    for n in names {
        if n.is_empty() || n.len() > MAX_NAME_LEN || n.chars().any(char::is_control) {
            return Err(Status::invalid_argument(format!(
                "{what}: {n:?} is not a valid name (1 to {MAX_NAME_LEN} characters, no control characters)"
            )));
        }
    }
    Ok(())
}

pub(super) fn scope_of(
    exchanges: &[String],
    symbols: &[String],
    base_assets: &[String],
) -> Result<Scope, Status> {
    check_names("exchanges", exchanges)?;
    check_names("symbols", symbols)?;
    check_names("base_assets", base_assets)?;
    Ok(Scope {
        exchanges: exchanges.to_vec(),
        symbols: symbols.to_vec(),
        base_assets: base_assets.to_vec(),
    })
}

impl HistoryService {
    /// Builds the service. `cold` is `None` without ClickHouse: the service then answers from the Redis hot
    /// window only. `watermark` is the flusher's shared watermark when the flusher runs in this process.
    pub fn new(
        store: Arc<dyn RedisStore>,
        cold: Option<Arc<dyn ColdStore>>,
        watermark: Option<Arc<Watermark>>,
        cfg: HistoryApiConfig,
        series: SeriesParams,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        let permits = Arc::new(Semaphore::new(cfg.max_concurrent as usize));
        Arc::new(Self {
            store,
            cold,
            watermark,
            cfg,
            series,
            clock,
            stats: Arc::new(HistoryStats::default()),
            permits,
        })
    }

    /// The counters.
    pub fn stats(&self) -> &Arc<HistoryStats> {
        &self.stats
    }

    /// Starts a call or refuses it when `max_concurrent` calls are already running.
    pub(super) fn begin(self: &Arc<Self>, method: Method) -> Result<Call, Status> {
        match self.permits.clone().try_acquire_owned() {
            Ok(permit) => {
                self.stats.enter();
                Ok(Call {
                    svc: self.clone(),
                    method,
                    started: Instant::now(),
                    code: None,
                    _permit: permit,
                })
            }
            Err(_) => {
                self.stats.rejected(Reject::Concurrency);
                self.stats.finished(method, Code::ResourceExhausted, 0);
                Err(Status::resource_exhausted(format!(
                    "{} History calls are already running (history.max_concurrent); retry shortly",
                    self.cfg.max_concurrent
                )))
            }
        }
    }

    /// Counts a refusal and passes the `Status` on.
    pub(super) fn reject(&self, reason: Reject, status: Status) -> Status {
        self.stats.rejected(reason);
        status
    }

    pub(super) fn timeout(&self) -> Duration {
        Duration::from_secs(self.cfg.request_timeout_secs)
    }

    pub(super) fn slack(&self) -> Duration {
        Duration::from_secs(self.cfg.redis_slack_secs)
    }

    /// The last flushed stream ID as (ms, seq); `(0, 0)` when nothing was flushed. Only meaningful with ClickHouse.
    pub(super) async fn current_watermark(&self) -> Result<(u64, u64), StoreError> {
        if let Some(w) = &self.watermark
            && let Some(id) = parse_id(&w.id())
            && id != (0, 0)
        {
            return Ok(id);
        }
        // Not loaded yet by the flusher of this process, or the flusher runs elsewhere: ask Redis.
        let stored = self.store.state_get(crate::history::WATERMARK_KEY).await?;
        Ok(stored.as_deref().and_then(parse_id).unwrap_or((0, 0)))
    }

    /// Whether the Redis hot window reaches back to `from_ms` (within the slack: the window is trimmed
    /// approximately and entry IDs are write times), so that a period starting there can be answered from Redis
    /// alone.
    pub(super) async fn redis_covers(&self, from_ms: i64) -> bool {
        let slack = self.slack().as_millis() as i64;
        match self.store.bounds().await {
            Ok(b) => b
                .first
                .as_deref()
                .and_then(parse_id)
                .is_some_and(|(ms, _)| (ms as i64) <= from_ms.saturating_add(slack)),
            Err(_) => false,
        }
    }
}

/// The tonic adapter.
#[derive(Clone)]
pub struct HistoryHandle(pub Arc<HistoryService>);

type EventStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<Event, Status>> + Send>>;

#[tonic::async_trait]
impl History for HistoryHandle {
    type QueryStream = EventStream;

    async fn query(
        &self,
        request: Request<HistoryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let rx: mpsc::Receiver<Result<Event, Status>> =
            self.0.clone().start_query(request.into_inner())?;
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn aggregate(
        &self,
        request: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        let req = request.into_inner();
        let svc = self.0.clone();
        let method = if req.oi_movers.is_some() {
            Method::Movers
        } else {
            Method::Aggregate
        };
        let mut call = svc.begin(method)?;
        let limit = svc.timeout();
        let result = match tokio::time::timeout(limit, svc.aggregate_inner(req)).await {
            Ok(r) => r,
            Err(_) => Err(Status::deadline_exceeded(format!(
                "the aggregate did not finish within {} s (history.request_timeout_secs); narrow the period or the filters",
                limit.as_secs()
            ))),
        };
        call.finish(result.as_ref().map_or_else(Status::code, |_| Code::Ok));
        result.map(Response::new)
    }
}
