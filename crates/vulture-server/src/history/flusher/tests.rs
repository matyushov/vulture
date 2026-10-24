use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize};

use futures_util::future::BoxFuture;
use vulture_proto::v1::{Event, Gap, event::Payload};

use super::*;
use crate::store::{EntryFields, MemoryStore, Trim};

/// A clock the test advances by hand; every `now_ms` call moves it by 1 ms so that entry IDs differ.
struct TickClock(AtomicI64);

impl Clock for TickClock {
    fn now_ms(&self) -> i64 {
        self.0.fetch_add(1, Ordering::SeqCst)
    }
}

fn gap_entry(n: i64) -> EntryFields {
    EntryFields::from_event(&Event {
        id: String::new(),
        payload: Some(Payload::Gap(Gap {
            exchange: "binance".into(),
            stream: "s".into(),
            from_ts: n,
            to_ts: n + 1,
        })),
    })
    .unwrap()
}

fn garbage_entry() -> EntryFields {
    EntryFields {
        kind: "oi",
        exchange: "x".into(),
        symbol: "y".into(),
        data: vec![0xff, 0xff, 0xff],
    }
}

/// Reads the `event_id` out of gap rows: exchange, stream, from_ts, to_ts, event_id.
fn gap_event_ids(mut body: &[u8]) -> Vec<String> {
    fn take_str<'a>(b: &mut &'a [u8]) -> &'a str {
        let len = b[0] as usize;
        let s = std::str::from_utf8(&b[1..1 + len]).unwrap();
        *b = &b[1 + len..];
        s
    }
    let mut ids = Vec::new();
    while !body.is_empty() {
        take_str(&mut body);
        take_str(&mut body);
        body = &body[16..];
        ids.push(take_str(&mut body).to_string());
    }
    ids
}

/// A ClickHouse stand-in that deduplicates by token like the real server.
#[derive(Default)]
struct FakeSink {
    blocks: Mutex<Vec<(Table, String, Vec<String>)>>,
    tokens: Mutex<HashSet<(Table, String)>>,
    attempts: AtomicUsize,
    dropped_duplicates: AtomicUsize,
    migrations: AtomicUsize,
    failing: AtomicBool,
    /// Called after a block is stored (before the caller learns about it).
    after_insert: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl FakeSink {
    fn event_ids(&self) -> Vec<String> {
        self.blocks
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(_, _, ids)| ids.clone())
            .collect()
    }
    fn block_tokens(&self) -> Vec<String> {
        self.blocks
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.1.clone())
            .collect()
    }
}

impl HistorySink for FakeSink {
    fn migrate(&self) -> BoxFuture<'_, Result<Vec<u32>, ChError>> {
        Box::pin(async move {
            if self.failing.load(Ordering::SeqCst) {
                return Err(ChError::Connection("injected".into()));
            }
            self.migrations.fetch_add(1, Ordering::SeqCst);
            Ok(vec![])
        })
    }

    fn insert<'a>(
        &'a self,
        table: Table,
        body: Vec<u8>,
        token: &'a str,
    ) -> BoxFuture<'a, Result<(), ChError>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.failing.load(Ordering::SeqCst) {
                return Err(ChError::Connection("injected".into()));
            }
            let fresh = self
                .tokens
                .lock()
                .unwrap()
                .insert((table, token.to_string()));
            if fresh {
                self.blocks
                    .lock()
                    .unwrap()
                    .push((table, token.to_string(), gap_event_ids(&body)));
            } else {
                self.dropped_duplicates.fetch_add(1, Ordering::SeqCst);
            }
            if let Some(hook) = &*self.after_insert.lock().unwrap() {
                hook();
            }
            Ok(())
        })
    }

    fn ping(&self) -> BoxFuture<'_, Result<(), ChError>> {
        Box::pin(async move {
            if self.failing.load(Ordering::SeqCst) {
                Err(ChError::Connection("injected".into()))
            } else {
                Ok(())
            }
        })
    }
}

fn cfg(rows: usize) -> FlusherConfig {
    FlusherConfig {
        interval: Duration::from_secs(600),
        batch_max_rows: rows,
        retry_min: Duration::from_secs(1),
        retry_max: Duration::from_secs(60),
        page_pause: Duration::from_millis(1),
        stats_interval: Duration::from_secs(15),
        shutdown_timeout: Duration::from_secs(2),
    }
}

struct Rig {
    store: Arc<MemoryStore>,
    sink: Arc<FakeSink>,
    watermark: Arc<Watermark>,
}

impl Rig {
    fn new() -> Self {
        Self {
            store: Arc::new(MemoryStore::with_clock(Box::new(TickClock(
                AtomicI64::new(1_000),
            )))),
            sink: Arc::new(FakeSink::default()),
            watermark: Arc::new(Watermark::new()),
        }
    }

    /// A flusher as a fresh process would build it: shared Redis and ClickHouse, new in-memory state.
    fn flusher(&self, rows: usize) -> Flusher {
        Flusher::new(
            self.store.clone(),
            self.sink.clone(),
            cfg(rows),
            self.watermark.clone(),
            Arc::new(FlusherStatus::new(self.watermark.clone(), 5_000_000, 0)),
            Arc::new(TickClock(AtomicI64::new(50_000))),
        )
    }

    async fn append(&self, n: i64, count: i64) -> Vec<String> {
        let entries: Vec<EntryFields> = (n..n + count).map(gap_entry).collect();
        self.store
            .append(
                &entries,
                Trim {
                    min_id_ms: 0,
                    maxlen: u64::MAX,
                },
            )
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn flushes_everything_and_persists_the_watermark() {
    let r = Rig::new();
    let ids = r.append(0, 7).await;
    let f = r.flusher(100);
    assert_eq!(f.cycle(None, None).await.unwrap(), 7);
    assert_eq!(r.sink.event_ids(), ids);
    assert_eq!(r.watermark.id(), *ids.last().unwrap());
    assert_eq!(r.store.state_value(WATERMARK_KEY), ids.last().cloned());
    assert_eq!(r.store.state_value(INFLIGHT_KEY), None);
    assert_eq!(r.sink.migrations.load(Ordering::SeqCst), 1);
    // Nothing new: nothing inserted.
    assert_eq!(f.cycle(None, None).await.unwrap(), 0);
    assert_eq!(r.sink.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(f.status().rows_of(Table::Gaps), 7);
}

#[tokio::test]
async fn a_backlog_larger_than_one_page_is_flushed_in_pages_without_gaps_or_duplicates() {
    let r = Rig::new();
    let ids = r.append(0, 10).await;
    let f = r.flusher(3);
    assert_eq!(f.cycle(None, None).await.unwrap(), 10);
    // Pages of 3, 3, 3 and 1, each with its own token.
    assert_eq!(r.sink.blocks.lock().unwrap().len(), 4);
    let tokens = r.sink.block_tokens();
    assert_eq!(tokens.iter().collect::<HashSet<_>>().len(), 4);
    assert_eq!(r.sink.event_ids(), ids);
    assert_eq!(r.watermark.id(), *ids.last().unwrap());
}

#[tokio::test]
async fn a_crash_between_the_insert_and_the_watermark_creates_no_duplicates() {
    let r = Rig::new();
    let first_batch = r.append(0, 5).await;
    // The "process" dies right after the insert succeeded: the watermark update fails.
    let store = r.store.clone();
    *r.sink.after_insert.lock().unwrap() = Some(Box::new(move || {
        store.set_failing_state_apply(true);
    }));
    let f = r.flusher(100);
    let err = f.cycle(None, None).await.unwrap_err();
    assert!(matches!(err, FlushError::Redis(_)), "{err:?}");
    assert_eq!(r.sink.event_ids(), first_batch);
    // The watermark did not move, but the range is recorded.
    assert_eq!(r.store.state_value(WATERMARK_KEY), None);
    assert!(r.store.state_value(INFLIGHT_KEY).is_some());
    drop(f);

    // Meanwhile more entries arrive: a naive retry would read a longer page with a different token.
    let more = r.append(100, 4).await;
    *r.sink.after_insert.lock().unwrap() = None;
    r.store.set_failing_state_apply(false);
    // Restart: new in-memory state (a fresh watermark object too), same Redis and ClickHouse.
    let restarted = Rig {
        store: r.store.clone(),
        sink: r.sink.clone(),
        watermark: Arc::new(Watermark::new()),
    };
    let f2 = restarted.flusher(100);
    f2.cycle(None, None).await.unwrap();

    let mut want = first_batch.clone();
    want.extend(more);
    assert_eq!(
        r.sink.event_ids(),
        want,
        "every entry exactly once, in order"
    );
    assert_eq!(
        r.sink.dropped_duplicates.load(Ordering::SeqCst),
        1,
        "the re-sent block was dropped by its token"
    );
    assert_eq!(r.store.state_value(INFLIGHT_KEY), None);
    assert_eq!(restarted.watermark.id(), *want.last().unwrap());
}

#[tokio::test]
async fn the_watermark_survives_a_restart() {
    let r = Rig::new();
    let ids = r.append(0, 6).await;
    r.flusher(100).cycle(None, None).await.unwrap();
    let attempts = r.sink.attempts.load(Ordering::SeqCst);
    let more = r.append(50, 2).await;

    let restarted = Rig {
        store: r.store.clone(),
        sink: r.sink.clone(),
        watermark: Arc::new(Watermark::new()),
    };
    assert_eq!(restarted.watermark.id(), "0-0");
    restarted.flusher(100).cycle(None, None).await.unwrap();
    assert_eq!(restarted.watermark.id(), *more.last().unwrap());
    // Only the two new entries went out.
    assert_eq!(r.sink.attempts.load(Ordering::SeqCst), attempts + 1);
    let mut want = ids;
    want.extend(more);
    assert_eq!(r.sink.event_ids(), want);
}

#[tokio::test]
async fn an_undecodable_entry_is_skipped_and_counted_not_fatal() {
    let r = Rig::new();
    let mut entries = vec![gap_entry(1), garbage_entry(), gap_entry(2)];
    entries.push(garbage_entry());
    r.store
        .append(
            &entries,
            Trim {
                min_id_ms: 0,
                maxlen: u64::MAX,
            },
        )
        .await
        .unwrap();
    let f = r.flusher(100);
    assert_eq!(f.cycle(None, None).await.unwrap(), 2);
    assert_eq!(f.status().skipped_undecodable.load(Ordering::Relaxed), 2);
    assert_eq!(r.sink.event_ids().len(), 2);
    assert_eq!(f.status().cycles.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn a_failing_clickhouse_keeps_the_watermark_and_counts_failures() {
    let r = Rig::new();
    r.append(0, 4).await;
    r.sink.failing.store(true, Ordering::SeqCst);
    let f = r.flusher(100);
    let err = f.cycle(None, None).await.unwrap_err();
    assert!(matches!(err, FlushError::ClickHouse(_)), "{err:?}");
    assert_eq!(r.watermark.id(), "0-0");
    assert_eq!(f.status().failures_migrate.load(Ordering::Relaxed), 1);
    assert_eq!(f.status().ch_state(), ChState::Down);
    // Migration passes, insert fails.
    r.sink.failing.store(false, Ordering::SeqCst);
    f.cycle(None, None).await.unwrap();
    assert_eq!(f.status().ch_state(), ChState::Up);
}

#[tokio::test(start_paused = true)]
async fn the_run_loop_retries_with_backoff_and_catches_up_after_recovery() {
    let r = Rig::new();
    let ids = r.append(0, 25).await;
    r.sink.failing.store(true, Ordering::SeqCst);
    let f = r.flusher(10);
    let status = f.status();
    let stop = CancellationToken::new();
    let handle = tokio::spawn(f.run(stop.clone()));

    // Two minutes of outage: several attempts with growing delays, nothing flushed, watermark unchanged.
    tokio::time::sleep(Duration::from_secs(120)).await;
    let failures = status.failures_migrate.load(Ordering::Relaxed);
    assert!(
        (4..=9).contains(&failures),
        "backoff 1,2,4,...,60 s: {failures}"
    );
    assert_eq!(r.watermark.id(), "0-0");
    assert!(r.sink.event_ids().is_empty());
    assert_eq!(status.ch_state(), ChState::Down);

    // ClickHouse comes back; new events keep arriving during the outage and after.
    let more = r.append(100, 5).await;
    r.sink.failing.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(70)).await;
    let mut want = ids;
    want.extend(more);
    assert_eq!(r.sink.event_ids(), want);
    assert_eq!(r.watermark.id(), *want.last().unwrap());
    assert_eq!(status.ch_state(), ChState::Up);
    // Pages of 10 (30 entries): 3 inserts, none repeated.
    assert_eq!(r.sink.blocks.lock().unwrap().len(), 3);

    stop.cancel();
    handle.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_runs_a_final_flush_of_what_arrived_since_the_last_cycle() {
    let r = Rig::new();
    let f = r.flusher(100);
    let stop = CancellationToken::new();
    let handle = tokio::spawn(f.run(stop.clone()));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let ids = r.append(0, 3).await;
    // The next regular cycle is 10 minutes away.
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert!(r.sink.event_ids().is_empty());
    stop.cancel();
    handle.await.unwrap();
    assert_eq!(r.sink.event_ids(), ids);
}

#[tokio::test]
async fn a_final_flush_against_a_dead_clickhouse_gives_up_quickly() {
    let r = Rig::new();
    r.append(0, 3).await;
    r.sink.failing.store(true, Ordering::SeqCst);
    let f = r.flusher(100);
    let started = std::time::Instant::now();
    f.final_flush().await;
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(r.watermark.id(), "0-0");
}

#[tokio::test]
async fn trimming_that_removed_unflushed_entries_is_reported() {
    let r = Rig::new();
    r.append(0, 6).await;
    let f = r.flusher(100);
    f.cycle(None, None).await.unwrap();
    // New entries arrive; then MAXLEN-style trimming removes everything up to the newest ones.
    let more = r.append(10, 6).await;
    r.store.trim_to(&more[4]);
    f.cycle(None, None).await.unwrap();
    assert_eq!(f.status().possible_loss.load(Ordering::Relaxed), 1);
    // Reported once, not at every cycle.
    r.append(20, 1).await;
    f.cycle(None, None).await.unwrap();
    assert_eq!(f.status().possible_loss.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn a_corrupt_stored_watermark_stops_the_flusher_instead_of_guessing() {
    let r = Rig::new();
    r.append(0, 2).await;
    r.store
        .state_apply(&[(WATERMARK_KEY, Some("nonsense"))])
        .await
        .unwrap();
    let err = r.flusher(100).cycle(None, None).await.unwrap_err();
    assert!(matches!(err, FlushError::State(_)), "{err:?}");
    assert!(r.sink.event_ids().is_empty());
}

#[test]
fn backlog_estimate_follows_the_id_span() {
    assert_eq!(estimate_backlog(0, 0, 0, 0), 0);
    // Watermark older than everything retained: all of it is backlog.
    assert_eq!(estimate_backlog(1000, 100, 200, 50), 1000);
    // Halfway through the span.
    assert_eq!(estimate_backlog(1000, 100, 200, 150), 500);
    // Caught up.
    assert_eq!(estimate_backlog(1000, 100, 200, 200), 0);
    assert_eq!(estimate_backlog(1000, 100, 200, 300), 0);
    // One-millisecond stream.
    assert_eq!(estimate_backlog(10, 100, 100, 50), 10);
}

#[test]
fn previous_id() {
    assert_eq!(prev_id("5-3").as_deref(), Some("5-2"));
    assert_eq!(prev_id("5-0"), Some(format!("4-{}", u64::MAX)));
    assert_eq!(prev_id("0-0"), None);
}

#[test]
fn flusher_config_follows_the_section() {
    let ch = ClickHouseConfig {
        flush_interval_secs: 42,
        batch_max_rows: 7,
        ..ClickHouseConfig::default()
    };
    let c = FlusherConfig::from_config(&ch, Duration::from_secs(4));
    assert_eq!(c.interval, Duration::from_secs(42));
    assert_eq!(c.batch_max_rows, 7);
    assert_eq!(c.shutdown_timeout, Duration::from_secs(4));
}
