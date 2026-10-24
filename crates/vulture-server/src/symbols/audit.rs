//! Append-only audit log of symbol policy changes: `<data_dir>/audit.log`, one JSON object per line.
//!
//! Size-bounded: when the next line would push the file over the limit, `audit.log` is renamed to `audit.log.1`
//! (replacing an older one) and a new file starts, so at most about twice the limit stays on disk. The log holds
//! no secrets: times, exchange, mode, the changed symbols, counts and the peer address.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::Serialize;

use super::state::open_private;

/// File name inside `server.data_dir`.
pub const AUDIT_FILE: &str = "audit.log";
/// Default size limit of one file, bytes.
pub const DEFAULT_MAX_BYTES: u64 = 4 * 1024 * 1024;

/// One line of the log.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AuditRecord {
    /// UTC time, RFC 3339 with seconds.
    pub time: String,
    /// Unix time, milliseconds.
    pub ts_ms: i64,
    /// What happened (`symbol_policy`).
    pub event: &'static str,
    /// Exchange name.
    pub exchange: String,
    /// Policy mode after the change.
    pub mode: String,
    /// Number of symbols listed in the policy after the change.
    pub policy_symbols: usize,
    /// Symbols that started to be tracked.
    pub added: Vec<String>,
    /// Symbols that stopped being tracked.
    pub removed: Vec<String>,
    /// Tracked symbols before the change.
    pub tracked_before: usize,
    /// Tracked symbols after the change.
    pub tracked_after: usize,
    /// Symbols the exchange offers.
    pub universe: usize,
    /// Address of the client (`ip:port`), empty when unknown.
    pub peer: String,
}

/// The log file.
#[derive(Debug)]
pub struct AuditLog {
    path: PathBuf,
    max_bytes: u64,
    lock: Mutex<()>,
}

impl AuditLog {
    /// Log in `data_dir` with the default size limit.
    pub fn new(data_dir: &Path) -> Self {
        Self::with_limit(data_dir, DEFAULT_MAX_BYTES)
    }

    /// Log with an explicit limit (tests).
    pub fn with_limit(data_dir: &Path, max_bytes: u64) -> Self {
        Self {
            path: data_dir.join(AUDIT_FILE),
            max_bytes,
            lock: Mutex::new(()),
        }
    }

    /// Path of the current file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn rotated(&self) -> PathBuf {
        let mut name = self.path.file_name().unwrap_or_default().to_os_string();
        name.push(".1");
        self.path.with_file_name(name)
    }

    /// Appends one line, rotating first when needed.
    pub fn append(&self, record: &AuditRecord) -> std::io::Result<()> {
        let mut line = serde_json::to_string(record).map_err(std::io::Error::other)?;
        line.push('\n');
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let size = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        if size > 0 && size + line.len() as u64 > self.max_bytes {
            std::fs::rename(&self.path, self.rotated())?;
        }
        let mut file = open_private(&self.path, false)?;
        file.write_all(line.as_bytes())?;
        file.sync_data()
    }
}

/// UTC time as `YYYY-MM-DDTHH:MM:SSZ` for a Unix time in milliseconds.
pub fn rfc3339(ts_ms: i64) -> String {
    let secs = ts_ms.div_euclid(1000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(n: usize) -> AuditRecord {
        AuditRecord {
            time: rfc3339(1_792_800_000_000),
            ts_ms: 1_792_800_000_000,
            event: "symbol_policy",
            exchange: "binance".into(),
            mode: "blacklist".into(),
            policy_symbols: n,
            added: vec![],
            removed: vec!["ETH-USDT-PERP".into()],
            tracked_before: 10,
            tracked_after: 9,
            universe: 10,
            peer: "127.0.0.1:5555".into(),
        }
    }

    #[test]
    fn time_formatting() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(951_782_400_000), "2000-02-29T00:00:00Z");
        assert_eq!(rfc3339(1_792_800_000_000 + 1999), "2026-10-24T00:00:01Z");
        assert_eq!(rfc3339(-1000), "1969-12-31T23:59:59Z");
    }

    #[test]
    fn lines_are_json_and_private() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::new(dir.path());
        log.append(&record(1)).unwrap();
        log.append(&record(2)).unwrap();
        let text = std::fs::read_to_string(log.path()).unwrap();
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let v: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v["exchange"], "binance");
        assert_eq!(v["policy_symbols"], 2);
        assert_eq!(v["removed"][0], "ETH-USDT-PERP");
        assert_eq!(v["peer"], "127.0.0.1:5555");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(log.path()).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn size_is_bounded_by_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let one = serde_json::to_string(&record(1)).unwrap().len() as u64 + 1;
        let log = AuditLog::with_limit(dir.path(), one * 3);
        for i in 0..10 {
            log.append(&record(i)).unwrap();
        }
        let current = std::fs::metadata(log.path()).unwrap().len();
        let old = std::fs::metadata(log.rotated()).unwrap().len();
        assert!(current <= one * 3 && old <= one * 3, "{current} {old}");
        // The newest line is in the current file; nothing but audit.log and audit.log.1 exists.
        let text = std::fs::read_to_string(log.path()).unwrap();
        assert!(
            text.lines()
                .last()
                .unwrap()
                .contains("\"policy_symbols\":9")
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
    }
}
