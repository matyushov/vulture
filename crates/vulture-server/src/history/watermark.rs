//! The flush watermark shared by the flusher and the Redis writer, and the deduplication token.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use super::rows::Table;
use crate::store::parse_id;

/// The ID of the last stream entry that is safely in ClickHouse (exclusive start of the next page). `0-0` means
/// nothing has been flushed yet. The flusher owns it and persists it in Redis; the writer only reads the
/// millisecond part to bound its time trimming.
#[derive(Debug)]
pub struct Watermark {
    ms: AtomicU64,
    id: Mutex<String>,
}

impl Default for Watermark {
    fn default() -> Self {
        Self {
            ms: AtomicU64::new(0),
            id: Mutex::new("0-0".to_string()),
        }
    }
}

impl Watermark {
    /// New watermark at `0-0`: until the flusher has loaded the persisted value, nothing is trimmed by time.
    pub fn new() -> Self {
        Self::default()
    }

    /// Moves the watermark. An ID that does not parse is ignored; so is one that would move it backwards.
    pub fn set(&self, id: &str) {
        let Some((ms, seq)) = parse_id(id) else {
            return;
        };
        let mut cur = self.id.lock().unwrap_or_else(|e| e.into_inner());
        if parse_id(&cur).is_some_and(|c| c > (ms, seq)) {
            return;
        }
        *cur = id.to_string();
        self.ms.store(ms, Ordering::SeqCst);
    }

    /// Millisecond part of the ID.
    pub fn ms(&self) -> u64 {
        self.ms.load(Ordering::SeqCst)
    }

    /// The full ID.
    pub fn id(&self) -> String {
        self.id.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// The `XTRIM MINID` cutoff (ms): `min(now - window, watermark)`, never negative. Without a watermark (the
/// flusher is off) it is the plain time window. An entry newer than the watermark is never trimmed by time,
/// so nothing unflushed disappears while ClickHouse is slow or down.
pub fn trim_cutoff_ms(now_ms: i64, window_ms: i64, watermark: Option<&Watermark>) -> i64 {
    let by_time = now_ms - window_ms;
    let cutoff = match watermark {
        Some(w) => by_time.min(i64::try_from(w.ms()).unwrap_or(i64::MAX)),
        None => by_time,
    };
    cutoff.max(0)
}

/// Deduplication token of one insert: a pure function of the table and the entry ID range of the page, so a
/// retry of the same range (after a timeout, a crash or a restart) carries the same token and ClickHouse drops
/// the block it already stored.
pub fn dedup_token(table: Table, first_id: &str, last_id: &str) -> String {
    format!("vulture-v1:{}:{first_id}:{last_id}", table.name())
}

/// The smallest ID after `id` (`<ms>-<seq+1>`), used as the inclusive start of the next `XRANGE`.
pub fn next_id(id: &str) -> Option<String> {
    let (ms, seq) = parse_id(id)?;
    Some(match seq.checked_add(1) {
        Some(s) => format!("{ms}-{s}"),
        None => format!("{}-0", ms.checked_add(1)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_moves_forward_only() {
        let w = Watermark::new();
        assert_eq!((w.ms(), w.id().as_str()), (0, "0-0"));
        w.set("1000-5");
        assert_eq!((w.ms(), w.id().as_str()), (1000, "1000-5"));
        w.set("999-9");
        assert_eq!(w.id(), "1000-5");
        w.set("garbage");
        assert_eq!(w.id(), "1000-5");
        w.set("1000-6");
        assert_eq!(w.id(), "1000-6");
        w.set("2000-0");
        assert_eq!(w.ms(), 2000);
    }

    #[test]
    fn cutoff_is_the_smaller_of_window_and_watermark() {
        let w = Watermark::new();
        let (now, window) = (1_000_000, 86_400);
        // No watermark: the plain time window.
        assert_eq!(trim_cutoff_ms(now, window, None), now - window);
        // Watermark at 0-0 (nothing flushed or not loaded yet): nothing is trimmed by time.
        assert_eq!(trim_cutoff_ms(now, window, Some(&w)), 0);
        // Flusher behind: the cutoff stays at the watermark although the window has passed it.
        w.set("500000-0");
        assert_eq!(trim_cutoff_ms(now, window, Some(&w)), 500_000);
        // Flusher caught up: the window decides.
        w.set("999999-0");
        assert_eq!(trim_cutoff_ms(now, window, Some(&w)), now - window);
        // A young process clock never goes negative.
        assert_eq!(trim_cutoff_ms(10, 86_400, None), 0);
    }

    #[test]
    fn token_is_deterministic_and_distinguishes_table_and_range() {
        let a = dedup_token(Table::OpenInterest, "1-0", "9-3");
        assert_eq!(a, dedup_token(Table::OpenInterest, "1-0", "9-3"));
        assert_ne!(a, dedup_token(Table::Liquidations, "1-0", "9-3"));
        assert_ne!(a, dedup_token(Table::OpenInterest, "1-0", "9-4"));
        assert_ne!(a, dedup_token(Table::OpenInterest, "1-1", "9-3"));
    }

    #[test]
    fn next_id_increments_the_sequence() {
        assert_eq!(next_id("5-7").as_deref(), Some("5-8"));
        assert_eq!(next_id("0-0").as_deref(), Some("0-1"));
        assert_eq!(next_id(&format!("5-{}", u64::MAX)).as_deref(), Some("6-0"));
        assert_eq!(next_id("x"), None);
    }
}
