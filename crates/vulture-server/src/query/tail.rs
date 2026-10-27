//! Reading the Redis stream for `History`: the unflushed tail (entries after the flusher's watermark) or, without
//! ClickHouse, the part of the hot window that covers a period.
//!
//! Redis entry IDs are write times, event timestamps are exchange times a little earlier, so a period is mapped
//! to an ID range with a slack (`history.redis_slack_secs`); every entry read is then checked against the period
//! by its own timestamp.

use std::time::Duration;

use crate::history::next_id;
use crate::store::{RedisStore, StoreError, StoredEntry, parse_id};

/// Where and how far to read.
#[derive(Debug, Clone)]
pub struct TailPlan {
    /// First entry ID to read (inclusive).
    pub start: String,
    /// Stop once an entry ID is newer than this many ms (the stream is ordered by ID).
    pub stop_after_ms: Option<u64>,
    /// Entries per Redis read.
    pub page: usize,
    /// Most entries to scan.
    pub max_entries: usize,
}

/// What a scan did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TailStats {
    /// Entries read from Redis.
    pub scanned: u64,
    /// The scan stopped at `max_entries` before reaching the end.
    pub truncated: bool,
}

/// The larger of two stream IDs (`ms-seq`); an unparsable one loses.
pub fn max_id(a: &str, b: &str) -> String {
    match (parse_id(a), parse_id(b)) {
        (Some(x), Some(y)) => {
            if y > x {
                b.to_string()
            } else {
                a.to_string()
            }
        }
        (Some(_), None) => a.to_string(),
        _ => b.to_string(),
    }
}

/// Start ID for a read of everything after the watermark `wm` (`0-0` reads from the very beginning).
pub fn after_watermark(wm: (u64, u64)) -> String {
    next_id(&format!("{}-{}", wm.0, wm.1)).unwrap_or_else(|| "0-0".to_string())
}

/// Start ID for events of time `ms` or later (minus slack).
pub fn id_for_time(ms: i64, slack: Duration) -> String {
    let t = ms.saturating_sub(slack.as_millis() as i64).max(0);
    format!("{t}-0")
}

/// Reads entries from `plan.start` to the end of the stream in pages and hands each one to `each`.
pub async fn scan(
    store: &dyn RedisStore,
    plan: &TailPlan,
    mut each: impl FnMut(StoredEntry),
) -> Result<TailStats, StoreError> {
    let mut stats = TailStats::default();
    let mut next = plan.start.clone();
    loop {
        let want = plan
            .page
            .min(plan.max_entries.saturating_sub(stats.scanned as usize))
            .max(1);
        let page = store.read_range(&next, "+", Some(want)).await?;
        let full = page.len() >= want;
        let Some(last_id) = page.last().map(|e| e.id.clone()) else {
            return Ok(stats);
        };
        for entry in page {
            if let (Some(stop), Some((ms, _))) = (plan.stop_after_ms, parse_id(&entry.id))
                && ms > stop
            {
                return Ok(stats);
            }
            stats.scanned += 1;
            each(entry);
        }
        if !full {
            return Ok(stats);
        }
        if stats.scanned as usize >= plan.max_entries {
            stats.truncated = true;
            return Ok(stats);
        }
        match next_id(&last_id) {
            Some(n) => next = n,
            None => return Ok(stats),
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{EntryFields, MemoryStore, Trim};
    use crate::ws::Clock;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    struct Tick(Arc<AtomicI64>);
    impl Clock for Tick {
        fn now_ms(&self) -> i64 {
            self.0.fetch_add(1, Ordering::SeqCst)
        }
    }

    async fn store_with(n: usize) -> MemoryStore {
        let store = MemoryStore::with_clock(Box::new(Tick(Arc::new(AtomicI64::new(1_000)))));
        let entries: Vec<EntryFields> = (0..n)
            .map(|i| EntryFields {
                kind: "liq",
                exchange: "x".into(),
                symbol: format!("S{i}"),
                data: vec![],
            })
            .collect();
        store
            .append(
                &entries,
                Trim {
                    min_id_ms: 0,
                    maxlen: 1_000_000,
                },
            )
            .await
            .unwrap();
        store
    }

    fn plan(start: &str) -> TailPlan {
        TailPlan {
            start: start.into(),
            stop_after_ms: None,
            page: 7,
            max_entries: 1_000,
        }
    }

    #[tokio::test]
    async fn reads_in_pages_to_the_end() {
        let store = store_with(30).await;
        let mut seen = Vec::new();
        let stats = scan(&store, &plan("0-0"), |e| seen.push(e.id))
            .await
            .unwrap();
        assert_eq!(
            stats,
            TailStats {
                scanned: 30,
                truncated: false
            }
        );
        assert_eq!(seen.len(), 30);
        let mut sorted = seen.clone();
        sorted.sort_by_key(|id| parse_id(id));
        assert_eq!(seen, sorted);
        // From the middle: the start is inclusive.
        let mut tail = Vec::new();
        scan(&store, &plan(&seen[20]), |e| tail.push(e.id))
            .await
            .unwrap();
        assert_eq!(tail, seen[20..].to_vec());
    }

    #[tokio::test]
    async fn stops_at_the_cap_and_at_the_time_limit() {
        let store = store_with(30).await;
        let mut p = plan("0-0");
        p.max_entries = 10;
        let stats = scan(&store, &p, |_| {}).await.unwrap();
        assert_eq!((stats.scanned, stats.truncated), (10, true));
        let mut p = plan("0-0");
        p.stop_after_ms = Some(1_009);
        let mut n = 0;
        let stats = scan(&store, &p, |_| n += 1).await.unwrap();
        assert_eq!(n, 10, "ids 1000..=1009");
        assert!(!stats.truncated);
        let empty = MemoryStore::new();
        assert_eq!(scan(&empty, &plan("0-0"), |_| {}).await.unwrap().scanned, 0);
    }

    #[test]
    fn id_helpers() {
        assert_eq!(max_id("5-1", "5-10"), "5-10");
        assert_eq!(max_id("6-0", "5-10"), "6-0");
        assert_eq!(max_id("x", "5-10"), "5-10");
        assert_eq!(after_watermark((5, 1)), "5-2");
        assert_eq!(after_watermark((0, 0)), "0-1");
        assert_eq!(id_for_time(10_000, Duration::from_secs(5)), "5000-0");
        assert_eq!(id_for_time(1_000, Duration::from_secs(5)), "0-0");
    }
}
