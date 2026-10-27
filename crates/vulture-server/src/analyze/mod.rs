//! Offline analysis of the stored history (B3, B4): the event study of signals and the replay of the detectors.
//!
//! Everything here only reads: [`source::ChSource`] issues `SELECT`s (best used with the read-only ClickHouse user
//! of `deploy/clickhouse`), and the replay runs the engine's own core over events read from ClickHouse with no
//! Redis, no delivery and no change to `alerts.json`.

pub mod cli;
pub mod params;
pub mod replay;
pub mod report;
pub mod source;
pub mod stats;
pub mod study;

/// Parses a time given as epoch milliseconds, `YYYY-MM-DD`, or `YYYY-MM-DDTHH:MM[:SS]` with an optional `Z` (UTC).
pub fn parse_time(text: &str) -> Result<i64, String> {
    let t = text.trim();
    if let Ok(ms) = t.parse::<i64>() {
        return Ok(ms);
    }
    let t = t.trim_end_matches('Z');
    let (date, time) = match t.split_once(['T', ' ']) {
        Some((d, tm)) => (d, Some(tm)),
        None => (t, None),
    };
    let mut parts = date.split('-');
    let bad = || {
        format!(
            "{text:?} is not a time: use epoch milliseconds, YYYY-MM-DD or YYYY-MM-DDTHH:MM[:SS]Z"
        )
    };
    let y: i32 = parts.next().and_then(|p| p.parse().ok()).ok_or_else(bad)?;
    let m: u32 = parts.next().and_then(|p| p.parse().ok()).ok_or_else(bad)?;
    let d: u32 = parts.next().and_then(|p| p.parse().ok()).ok_or_else(bad)?;
    if parts.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(bad());
    }
    let mut ms = crate::query::bins::days_from_civil(y, m, d) * 86_400_000;
    if let Some(tm) = time {
        let mut f = tm.split(':');
        let hh: i64 = f.next().and_then(|p| p.parse().ok()).ok_or_else(bad)?;
        let mm: i64 = f.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        let ss: i64 = f.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        if hh > 23 || mm > 59 || ss > 59 {
            return Err(bad());
        }
        ms += (hh * 3600 + mm * 60 + ss) * 1000;
    }
    Ok(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_parse() {
        assert_eq!(parse_time("1792972800000").unwrap(), 1_792_972_800_000);
        assert_eq!(parse_time("2026-10-26").unwrap(), 1_792_972_800_000);
        assert_eq!(
            parse_time("2026-10-26T01:30Z").unwrap(),
            1_792_972_800_000 + 5_400_000
        );
        assert_eq!(
            parse_time("2026-10-26T01:30:15Z").unwrap(),
            1_792_972_800_000 + 5_415_000
        );
        assert_eq!(
            parse_time("2026-10-26 00:00:01").unwrap(),
            1_792_972_801_000
        );
        for bad in [
            "",
            "yesterday",
            "2026-13-01",
            "2026-10",
            "2026-10-26T25:00Z",
        ] {
            assert!(parse_time(bad).is_err(), "{bad}");
        }
    }
}
