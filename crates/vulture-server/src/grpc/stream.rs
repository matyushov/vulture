//! `Stream.Subscribe`: replay from Redis with a cursor, then live tailing (V1-6).
//!
//! # Cursor rules
//!
//! The cursor is a Redis entry ID, exclusive: only entries strictly after it are delivered, so a client that
//! reconnects with the `Event.id` of the last event it received gets neither duplicates nor gaps.
//!
//! | `resume_from` | meaning |
//! |---|---|
//! | empty | live only: start at the current tail |
//! | `0` (or `0-0`) | replay everything still in the hot window, then live |
//! | `<ms>-<seq>` | exclusive cursor, replay what follows, then live |
//!
//! With `F` the oldest retained entry: a cursor `C` with `C < F` is answered `OUT_OF_RANGE` (entries between `C`
//! and `F` may have been trimmed; use `History`). `C == F`, `F < C <= tail` and `C > tail` (also an empty
//! stream) are accepted. The check is repeated between replay pages: if trimming overtakes a slow replay the
//! stream ends with `OUT_OF_RANGE`. The rule is conservative: a cursor whose successors were never trimmed can
//! still be refused when nothing newer than 24 hours had been written; the client then asks `History`.
//!
//! # Data path
//!
//! One task per client reads pages of `page_size` entries through its own Redis connection
//! ([`StreamReader`]), filters on the cheap entry fields `t`, `x`, `s` without decoding, decodes `d` only for
//! matches, sets `Event.id` from the entry ID and pushes into a bounded queue. When the queue stays full for
//! `slow_client_timeout` the client is disconnected with `RESOURCE_EXHAUSTED`; it resumes from its last id.
//! Redis errors are retried with backoff from the last scanned cursor, so nothing is repeated or skipped.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use vulture_proto::v1::stream_server::Stream;
use vulture_proto::v1::{Event, EventType, SubscribeRequest};

use super::GrpcStats;
use crate::auth::{AuthContext, TokenAuthority};
use crate::config::ServerConfig;
use crate::store::{
    RedisStore, StoreError, StoredEntry, StreamBounds, StreamReader, TYPE_GAP, TYPE_LIQUIDATION,
    TYPE_OI, TYPE_SIGNAL, parse_id,
};
use crate::ws::Backoff;

/// Cursor that precedes every entry.
pub const ZERO_ID: &str = "0-0";

/// Tunables of the stream service.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Simultaneous clients.
    pub max_clients: usize,
    /// Events queued per client.
    pub client_buffer: usize,
    /// How long a full queue is tolerated.
    pub slow_client_timeout: Duration,
    /// Entries read from Redis per call.
    pub page_size: usize,
    /// `XREAD BLOCK` duration of one live read.
    pub block: Duration,
    /// First delay after a Redis error.
    pub retry_min: Duration,
    /// Longest delay between Redis retries.
    pub retry_max: Duration,
}

impl StreamConfig {
    /// Settings from `[server]`; the rest has fixed defaults.
    pub fn from_server(server: &ServerConfig) -> Self {
        Self {
            max_clients: server.max_clients,
            client_buffer: server.client_buffer,
            slow_client_timeout: Duration::from_millis(server.slow_client_timeout_ms),
            page_size: 1000,
            block: Duration::from_secs(2),
            retry_min: Duration::from_millis(200),
            retry_max: Duration::from_secs(5),
        }
    }
}

/// The `Stream` service.
pub struct StreamService {
    store: Arc<dyn RedisStore>,
    authority: Arc<TokenAuthority>,
    cfg: StreamConfig,
    stats: Arc<GrpcStats>,
    shutdown: CancellationToken,
}

impl StreamService {
    /// New service. `shutdown` ends every open stream with `UNAVAILABLE`.
    pub fn new(
        store: Arc<dyn RedisStore>,
        authority: Arc<TokenAuthority>,
        cfg: StreamConfig,
        stats: Arc<GrpcStats>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            store,
            authority,
            cfg,
            stats,
            shutdown,
        }
    }
}

/// Subscription filter. An empty set means "no filter on this field".
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Filter {
    exchanges: HashSet<String>,
    symbols: HashSet<String>,
    kinds: HashSet<&'static str>,
}

impl Filter {
    /// Builds the filter; unknown or unspecified event types are `INVALID_ARGUMENT`.
    pub fn from_request(req: &SubscribeRequest) -> Result<Self, Status> {
        let mut kinds = HashSet::new();
        for raw in &req.types {
            let kind = match EventType::try_from(*raw) {
                Ok(EventType::Liquidation) => TYPE_LIQUIDATION,
                Ok(EventType::OpenInterest) => TYPE_OI,
                Ok(EventType::Gap) => TYPE_GAP,
                Ok(EventType::Signal) => TYPE_SIGNAL,
                Ok(EventType::Unspecified) | Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "types: {raw} is not a known event type"
                    )));
                }
            };
            kinds.insert(kind);
        }
        Ok(Self {
            exchanges: req.exchanges.iter().cloned().collect(),
            symbols: req.symbols.iter().cloned().collect(),
            kinds,
        })
    }

    /// Whether the entry passes, decided from the cheap fields only. A Gap written by the server itself
    /// (exchange `*`) passes the exchange filter, and entries without a symbol (gaps) pass the symbol filter.
    pub fn matches(&self, entry: &StoredEntry) -> bool {
        (self.kinds.is_empty() || self.kinds.contains(entry.kind.as_str()))
            && (self.exchanges.is_empty()
                || entry.exchange == "*"
                || self.exchanges.contains(&entry.exchange))
            && (self.symbols.is_empty()
                || entry.symbol.is_empty()
                || self.symbols.contains(&entry.symbol))
    }
}

/// Where a subscription starts: the exclusive cursor and whether to replay before going live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Start {
    /// Entries strictly after this ID are delivered.
    pub cursor: String,
    /// `true` when entries after the cursor may already exist and must be read in pages first.
    pub replay: bool,
}

/// Applies the cursor rules (see the module documentation) to the current stream bounds.
pub fn resolve_start(resume_from: &str, bounds: &StreamBounds) -> Result<Start, Status> {
    if resume_from.is_empty() {
        return Ok(Start {
            cursor: bounds.last.clone().unwrap_or_else(|| ZERO_ID.to_string()),
            replay: false,
        });
    }
    let Some(cursor) = parse_id(resume_from) else {
        return Err(Status::invalid_argument(
            "resume_from must be empty, \"0\" or an Event.id of the form <ms>-<seq>",
        ));
    };
    if cursor != (0, 0)
        && let Some(first) = bounds.first.as_deref().and_then(parse_id)
        && cursor < first
    {
        return Err(out_of_range());
    }
    Ok(Start {
        cursor: format!("{}-{}", cursor.0, cursor.1),
        replay: true,
    })
}

fn out_of_range() -> Status {
    Status::out_of_range(
        "resume_from is older than the oldest event kept in the hot window; \
         read the missed period with History and subscribe again",
    )
}

/// Takes one client slot or fails with `RESOURCE_EXHAUSTED`.
struct SlotGuard {
    stats: Arc<GrpcStats>,
}

impl SlotGuard {
    fn acquire(stats: &Arc<GrpcStats>, max: usize) -> Option<Self> {
        stats
            .connected_clients
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < max).then_some(n + 1)
            })
            .ok()?;
        Some(Self {
            stats: stats.clone(),
        })
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.stats.connected_clients.fetch_sub(1, Ordering::SeqCst);
    }
}

/// State shared between the producer task and the response stream.
#[derive(Default)]
struct Shared {
    terminal: Mutex<Option<Status>>,
    urgent: AtomicBool,
}

impl Shared {
    /// Ends the stream with `status` now, dropping anything still queued.
    fn finish(&self, status: Status) {
        *self.terminal.lock().unwrap_or_else(|e| e.into_inner()) = Some(status);
        self.urgent.store(true, Ordering::Release);
    }

    fn take(&self) -> Option<Status> {
        self.terminal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
}

/// The response stream of one client: the bounded queue plus a final status. Dropping it (the client went
/// away) closes the queue, which stops the producer and frees its Redis connection.
pub struct ClientStream {
    rx: mpsc::Receiver<Event>,
    shared: Arc<Shared>,
    ended: bool,
}

impl tokio_stream::Stream for ClientStream {
    type Item = Result<Event, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        if self.shared.urgent.load(Ordering::Acquire) {
            self.ended = true;
            return Poll::Ready(self.shared.take().map(Err));
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(event)) => Poll::Ready(Some(Ok(event))),
            Poll::Ready(None) => {
                self.ended = true;
                Poll::Ready(self.shared.take().map(Err))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Why a producer stops.
enum Stop {
    /// The client dropped the stream.
    Gone,
    /// End the stream with this status.
    Terminal(Status),
}

struct Producer {
    store: Arc<dyn RedisStore>,
    cfg: StreamConfig,
    stats: Arc<GrpcStats>,
    shutdown: CancellationToken,
    filter: Filter,
    tx: mpsc::Sender<Event>,
    generation: u64,
    generations: watch::Receiver<u64>,
    cursor: String,
    replay: bool,
}

impl Producer {
    /// Runs until the client leaves, the server shuts down, the token changes or the client is too slow.
    async fn run(mut self, shared: Arc<Shared>, slot: SlotGuard) {
        let stop = self.stream().await;
        if let Stop::Terminal(status) = stop {
            tracing::debug!(code = ?status.code(), "stream ended");
            shared.finish(status);
        }
        // Free the slot before the queue closes, so a client that reconnects at once is not refused.
        drop(slot);
        // `self` (the Redis reader and the sender) is dropped here; dropping the sender wakes the response stream.
    }

    async fn stream(&mut self) -> Stop {
        let mut reader: Option<Box<dyn StreamReader>> = None;
        let mut backoff = Backoff::new(self.cfg.retry_min, self.cfg.retry_max, None);
        let mut recheck_trim = false;
        loop {
            if reader.is_none() {
                match self.guard(self.store.open_reader()).await {
                    Err(stop) => return stop,
                    Ok(Ok(r)) => reader = Some(r),
                    Ok(Err(e)) => {
                        if let Err(stop) = self.retry_wait(&mut backoff, &e).await {
                            return stop;
                        }
                        continue;
                    }
                }
            }
            if self.replay && recheck_trim && self.cursor != ZERO_ID {
                match self.guard(self.store.bounds()).await {
                    Err(stop) => return stop,
                    Ok(Err(e)) => {
                        if let Err(stop) = self.retry_wait(&mut backoff, &e).await {
                            return stop;
                        }
                        continue;
                    }
                    Ok(Ok(bounds)) => {
                        let first = bounds.first.as_deref().and_then(parse_id);
                        if let (Some(first), Some(cursor)) = (first, parse_id(&self.cursor))
                            && cursor < first
                        {
                            self.stats.out_of_range.fetch_add(1, Ordering::Relaxed);
                            return Stop::Terminal(out_of_range());
                        }
                    }
                }
            }
            let block = (!self.replay).then_some(self.cfg.block);
            let Some(active) = reader.as_mut() else {
                continue;
            };
            let read = self
                .guard(active.read_after(&self.cursor, self.cfg.page_size, block))
                .await;
            let entries = match read {
                Err(stop) => return stop,
                Ok(Ok(entries)) => {
                    backoff.reset();
                    entries
                }
                Ok(Err(e)) => {
                    // A failed connection is not reused.
                    reader = None;
                    if let Err(stop) = self.retry_wait(&mut backoff, &e).await {
                        return stop;
                    }
                    continue;
                }
            };
            let full_page = entries.len() >= self.cfg.page_size;
            for entry in entries {
                let id = entry.id.clone();
                if self.filter.matches(&entry) {
                    match entry.into_event() {
                        Ok(event) => {
                            if let Err(stop) = self.send(event).await {
                                return stop;
                            }
                        }
                        Err(e) => tracing::warn!(error = %e, "skipping an undecodable entry"),
                    }
                }
                // The cursor moves over filtered-out entries too, so they are not read again.
                self.cursor = id;
            }
            if self.replay {
                recheck_trim = true;
                if !full_page {
                    self.replay = false;
                }
            }
        }
    }

    /// Queues one event; a queue that stays full for `slow_client_timeout` ends the stream.
    async fn send(&self, event: Event) -> Result<(), Stop> {
        let event = match self.tx.try_send(event) {
            Ok(()) => {
                self.stats.entries_sent.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(Stop::Gone),
            Err(mpsc::error::TrySendError::Full(event)) => event,
        };
        match self
            .guard(tokio::time::timeout(
                self.cfg.slow_client_timeout,
                self.tx.send(event),
            ))
            .await?
        {
            Ok(Ok(())) => {
                self.stats.entries_sent.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Ok(Err(_)) => Err(Stop::Gone),
            Err(_elapsed) => {
                self.stats
                    .slow_client_disconnects
                    .fetch_add(1, Ordering::Relaxed);
                Err(Stop::Terminal(Status::resource_exhausted(
                    "the client does not keep up; reconnect with resume_from set to the last received id",
                )))
            }
        }
    }

    async fn retry_wait(&self, backoff: &mut Backoff, error: &StoreError) -> Result<(), Stop> {
        self.stats.redis_read_errors.fetch_add(1, Ordering::Relaxed);
        let delay = backoff.record_failure();
        if backoff.failures() == 1 {
            tracing::warn!(%error, ?delay, "Redis read failed; the stream stays open and retries");
        } else {
            tracing::debug!(%error, ?delay, failures = backoff.failures(), "Redis read still failing");
        }
        self.guard(tokio::time::sleep(delay)).await
    }

    /// Runs `fut`, abandoning it when the client leaves, the server shuts down or the token is rotated.
    async fn guard<T>(&self, fut: impl Future<Output = T>) -> Result<T, Stop> {
        tokio::select! {
            biased;
            _ = self.tx.closed() => Err(Stop::Gone),
            _ = self.shutdown.cancelled() => {
                Err(Stop::Terminal(Status::unavailable("the server is shutting down")))
            }
            _ = self.token_changed() => {
                Err(Stop::Terminal(Status::unauthenticated("the token was rotated; authenticate again")))
            }
            value = fut => Ok(value),
        }
    }

    async fn token_changed(&self) {
        let mut rx = self.generations.clone();
        let changed = rx.wait_for(|g| *g != self.generation).await.is_ok();
        if !changed {
            std::future::pending::<()>().await;
        }
    }
}

#[tonic::async_trait]
impl Stream for StreamService {
    type SubscribeStream = ClientStream;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // The interceptor sets this; a service wired without it refuses everything.
        let auth = request
            .extensions()
            .get::<AuthContext>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("missing or invalid token"))?;
        let peer = request.remote_addr();
        let req = request.into_inner();
        let filter = Filter::from_request(&req)?;

        let slot = SlotGuard::acquire(&self.stats, self.cfg.max_clients).ok_or_else(|| {
            self.stats
                .rejected_max_clients
                .fetch_add(1, Ordering::Relaxed);
            Status::resource_exhausted("too many connected clients")
        })?;

        if !req.resume_from.is_empty() {
            self.stats.resume_requests.fetch_add(1, Ordering::Relaxed);
        }
        let bounds = self.store.bounds().await.map_err(|e| {
            tracing::warn!(error = %e, "cannot read the stream bounds for a new subscription");
            Status::unavailable("storage is unavailable; retry shortly")
        })?;
        let start = resolve_start(&req.resume_from, &bounds).inspect_err(|s| {
            if s.code() == tonic::Code::OutOfRange {
                self.stats.out_of_range.fetch_add(1, Ordering::Relaxed);
            }
        })?;
        // The token may have been rotated between the interceptor and now.
        if self.authority.generation() != auth.generation {
            return Err(Status::unauthenticated(
                "the token was rotated; authenticate again",
            ));
        }
        self.stats
            .subscriptions_total
            .fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            peer = ?peer,
            resume = !req.resume_from.is_empty(),
            replay = start.replay,
            exchanges = req.exchanges.len(),
            symbols = req.symbols.len(),
            types = req.types.len(),
            "client subscribed"
        );

        let (tx, rx) = mpsc::channel(self.cfg.client_buffer);
        let shared = Arc::new(Shared::default());
        let producer = Producer {
            store: self.store.clone(),
            cfg: self.cfg.clone(),
            stats: self.stats.clone(),
            shutdown: self.shutdown.clone(),
            filter,
            tx,
            generation: auth.generation,
            generations: self.authority.subscribe(),
            cursor: start.cursor,
            replay: start.replay,
        };
        tokio::spawn(producer.run(shared.clone(), slot));
        Ok(Response::new(ClientStream {
            rx,
            shared,
            ended: false,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounds(first: &str, last: &str) -> StreamBounds {
        StreamBounds {
            first: Some(first.into()),
            last: Some(last.into()),
        }
    }

    #[test]
    fn empty_resume_starts_at_the_tail() {
        let s = resolve_start("", &bounds("10-0", "50-3")).unwrap();
        assert_eq!(
            s,
            Start {
                cursor: "50-3".into(),
                replay: false
            }
        );
        let s = resolve_start("", &StreamBounds::default()).unwrap();
        assert_eq!(
            s,
            Start {
                cursor: "0-0".into(),
                replay: false
            }
        );
    }

    #[test]
    fn zero_means_the_start_of_the_window() {
        for zero in ["0", "0-0"] {
            let s = resolve_start(zero, &bounds("10-0", "50-3")).unwrap();
            assert_eq!(
                s,
                Start {
                    cursor: "0-0".into(),
                    replay: true
                }
            );
        }
        assert!(resolve_start("0", &StreamBounds::default()).is_ok());
    }

    #[test]
    fn cursor_rules_around_the_first_entry_and_the_tail() {
        let b = bounds("100-5", "200-0");
        // Exactly at the first entry: accepted.
        assert!(resolve_start("100-5", &b).unwrap().replay);
        // Before it (also by sequence number): out of range.
        for old in ["100-4", "99-9", "1-0"] {
            let err = resolve_start(old, &b).unwrap_err();
            assert_eq!(err.code(), tonic::Code::OutOfRange, "{old}");
            assert!(err.message().contains("History"));
        }
        // Inside, at the last entry, and after the last entry: accepted.
        for ok in ["100-6", "150-0", "200-0", "200-1", "999999-0"] {
            assert_eq!(resolve_start(ok, &b).unwrap().cursor, ok, "{ok}");
        }
        // Bare milliseconds are normalized.
        assert_eq!(resolve_start("150", &b).unwrap().cursor, "150-0");
    }

    #[test]
    fn empty_stream_accepts_any_cursor() {
        assert!(resolve_start("123-4", &StreamBounds::default()).is_ok());
    }

    #[test]
    fn malformed_cursors_are_invalid_arguments() {
        let b = bounds("1-0", "2-0");
        for bad in ["abc", "1-", "-1", "1-2-3", "1.5", " 1-0", "1-0 "] {
            let err = resolve_start(bad, &b).unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{bad:?}");
        }
    }

    fn entry(kind: &str, exchange: &str, symbol: &str) -> StoredEntry {
        StoredEntry {
            id: "1-0".into(),
            kind: kind.into(),
            exchange: exchange.into(),
            symbol: symbol.into(),
            data: vec![],
        }
    }

    fn filter(exchanges: &[&str], symbols: &[&str], types: &[EventType]) -> Filter {
        Filter::from_request(&SubscribeRequest {
            exchanges: exchanges.iter().map(|s| s.to_string()).collect(),
            symbols: symbols.iter().map(|s| s.to_string()).collect(),
            types: types.iter().map(|t| *t as i32).collect(),
            resume_from: String::new(),
        })
        .unwrap()
    }

    #[test]
    fn filters_combine_with_and_and_empty_means_all() {
        let all = filter(&[], &[], &[]);
        assert!(all.matches(&entry("liq", "binance", "BTC-USDT-PERP")));
        let f = filter(&["binance"], &["BTC-USDT-PERP"], &[EventType::Liquidation]);
        assert!(f.matches(&entry("liq", "binance", "BTC-USDT-PERP")));
        assert!(!f.matches(&entry("oi", "binance", "BTC-USDT-PERP")));
        assert!(!f.matches(&entry("liq", "okx", "BTC-USDT-PERP")));
        assert!(!f.matches(&entry("liq", "binance", "ETH-USDT-PERP")));
    }

    #[test]
    fn gaps_pass_symbol_filters_and_server_gaps_pass_exchange_filters() {
        let f = filter(&["binance"], &["BTC-USDT-PERP"], &[]);
        assert!(
            f.matches(&entry("gap", "binance", "")),
            "no symbol: not excluded"
        );
        assert!(f.matches(&entry("gap", "*", "")), "server-wide gap");
        assert!(!f.matches(&entry("gap", "okx", "")));
        // An explicit types filter still decides.
        let only_oi = filter(&[], &[], &[EventType::OpenInterest]);
        assert!(!only_oi.matches(&entry("gap", "*", "")));
        let sig = filter(&[], &[], &[EventType::Signal]);
        assert!(sig.matches(&entry("sig", "binance", "BTC-USDT-PERP")));
    }

    #[test]
    fn unknown_event_types_are_rejected() {
        for bad in [0, 99, -1] {
            let err = Filter::from_request(&SubscribeRequest {
                types: vec![bad],
                ..Default::default()
            })
            .unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
        }
    }
}
