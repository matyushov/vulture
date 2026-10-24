//! Disk and table statistics of ClickHouse (V3-6), sampled periodically from `system.disks`, `system.parts` and
//! `system.tables` and exported as gauges. Sampling is never done at scrape time.
//!
//! Merges temporarily need extra free space (a merge writes the new part before the old ones are removed; plan
//! for up to the size of the largest partitions being merged), so alert on free space well before it is gone.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::client::{ChClient, ChError, json_u64};
use crate::metrics::{Families, MetricSource, unix_now_ms};
use std::sync::atomic::{AtomicU64, Ordering};

/// Free and total bytes of one disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskSample {
    /// Disk name in the ClickHouse configuration (`default`).
    pub name: String,
    /// Free bytes.
    pub free: u64,
    /// Total bytes.
    pub total: u64,
}

/// Statistics of one table.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableSample {
    /// Table name.
    pub name: String,
    /// Active parts.
    pub parts: u64,
    /// The most parts in a single partition (what `parts_to_throw_insert` limits).
    pub max_parts_per_partition: u64,
    /// Compressed bytes on disk (active parts).
    pub bytes: u64,
    /// Rows.
    pub rows: u64,
}

/// One sample of the server.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Sample {
    /// Disks.
    pub disks: Vec<DiskSample>,
    /// Tables of the configured database.
    pub tables: Vec<TableSample>,
    /// Wall-clock ms the sample was taken.
    pub taken_ms: i64,
}

/// Builds a sample from the JSON rows of the three queries.
pub fn parse_sample(
    disks: &[serde_json::Value],
    parts: &[serde_json::Value],
    tables: &[serde_json::Value],
    taken_ms: i64,
) -> Sample {
    let disks = disks
        .iter()
        .map(|r| DiskSample {
            name: r["name"].as_str().unwrap_or_default().to_string(),
            free: json_u64(&r["free_space"]),
            total: json_u64(&r["total_space"]),
        })
        .collect();
    let mut out: Vec<TableSample> = tables
        .iter()
        .map(|r| TableSample {
            name: r["name"].as_str().unwrap_or_default().to_string(),
            rows: json_u64(&r["total_rows"]),
            bytes: json_u64(&r["total_bytes"]),
            ..TableSample::default()
        })
        .collect();
    for r in parts {
        let name = r["table"].as_str().unwrap_or_default();
        if let Some(t) = out.iter_mut().find(|t| t.name == name) {
            t.parts = json_u64(&r["parts"]);
            t.max_parts_per_partition = json_u64(&r["max_parts_per_partition"]);
            // The sum over active parts is the authoritative on-disk size.
            t.bytes = json_u64(&r["bytes"]);
        }
    }
    Sample {
        disks,
        tables: out,
        taken_ms,
    }
}

/// Samples ClickHouse and reports the last sample as metrics.
#[derive(Default)]
pub struct ClickHouseMonitor {
    last: Mutex<Option<Sample>>,
    errors: AtomicU64,
}

impl ClickHouseMonitor {
    /// New monitor without a sample.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Takes one sample.
    pub async fn sample(&self, client: &ChClient) -> Result<(), ChError> {
        let db = client.database();
        let disks = client
            .query_rows("SELECT name, free_space, total_space FROM system.disks")
            .await?;
        let parts = client
            .query_rows(&format!(
                "SELECT table, sum(c) AS parts, max(c) AS max_parts_per_partition, sum(b) AS bytes \
                 FROM (SELECT table, partition, count() AS c, sum(bytes_on_disk) AS b FROM system.parts \
                       WHERE database = '{db}' AND active AND table != 'schema_migrations' GROUP BY table, partition) GROUP BY table"
            ))
            .await?;
        let tables = client
            .query_rows(&format!(
                "SELECT name, total_rows, total_bytes FROM system.tables \
                 WHERE database = '{db}' AND engine LIKE '%MergeTree' AND name != 'schema_migrations' ORDER BY name"
            ))
            .await?;
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(parse_sample(&disks, &parts, &tables, unix_now_ms()));
        Ok(())
    }

    /// The last sample, if any.
    pub fn last(&self) -> Option<Sample> {
        self.last.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Samples every `interval` until `shutdown`. A failure is counted and retried at the next tick.
    pub async fn run(
        self: Arc<Self>,
        client: Arc<ChClient>,
        interval: Duration,
        shutdown: CancellationToken,
    ) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.sample(&client).await {
                        self.errors.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(error = %e, "cannot sample ClickHouse statistics");
                    }
                }
                _ = shutdown.cancelled() => return,
            }
        }
    }
}

impl MetricSource for ClickHouseMonitor {
    fn collect(&self, out: &mut Families) {
        out.counter(
            "vulture_clickhouse_sample_errors_total",
            "Failed samplings of the ClickHouse disk and table statistics.",
            &[],
            self.errors.load(Ordering::Relaxed),
        );
        let Some(s) = self.last() else {
            return;
        };
        out.gauge(
            "vulture_clickhouse_sample_age_seconds",
            "Age of the ClickHouse statistics below; a growing value means sampling fails.",
            &[],
            (unix_now_ms() - s.taken_ms).max(0) as f64 / 1000.0,
        );
        for d in &s.disks {
            let l = [("disk", d.name.as_str())];
            out.gauge(
                "vulture_clickhouse_disk_free_bytes",
                "Free bytes on a ClickHouse disk.",
                &l,
                d.free as f64,
            );
            out.gauge(
                "vulture_clickhouse_disk_total_bytes",
                "Total bytes of a ClickHouse disk.",
                &l,
                d.total as f64,
            );
        }
        for t in &s.tables {
            let l = [("table", t.name.as_str())];
            out.gauge(
                "vulture_clickhouse_table_parts",
                "Active parts of a history table.",
                &l,
                t.parts as f64,
            );
            out.gauge(
                "vulture_clickhouse_table_max_parts_per_partition",
                "Most active parts in one partition of a history table.",
                &l,
                t.max_parts_per_partition as f64,
            );
            out.gauge(
                "vulture_clickhouse_table_bytes",
                "Compressed bytes on disk of a history table.",
                &l,
                t.bytes as f64,
            );
            out.gauge(
                "vulture_clickhouse_table_rows",
                "Rows of a history table.",
                &l,
                t.rows as f64,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_the_three_answers() {
        let disks = [json!({"name": "default", "free_space": "250", "total_space": "1000"})];
        let parts = [
            json!({"table": "open_interest", "parts": "12", "max_parts_per_partition": "7", "bytes": "4096"}),
            json!({"table": "unknown", "parts": "1", "max_parts_per_partition": "1", "bytes": "1"}),
        ];
        let tables = [
            json!({"name": "liquidations", "total_rows": "5", "total_bytes": "99"}),
            json!({"name": "open_interest", "total_rows": "1000", "total_bytes": "4000"}),
        ];
        let s = parse_sample(&disks, &parts, &tables, 7);
        assert_eq!(
            s.disks,
            vec![DiskSample {
                name: "default".into(),
                free: 250,
                total: 1000
            }]
        );
        assert_eq!(s.tables.len(), 2);
        assert_eq!(s.tables[0].parts, 0);
        assert_eq!(s.tables[0].bytes, 99);
        assert_eq!(
            s.tables[1],
            TableSample {
                name: "open_interest".into(),
                parts: 12,
                max_parts_per_partition: 7,
                bytes: 4096,
                rows: 1000
            }
        );
    }

    #[test]
    fn metrics_report_the_last_sample() {
        let m = ClickHouseMonitor::new();
        let mut f = Families::new();
        m.collect(&mut f);
        assert!(!f.render().contains("disk_free"));
        *m.last.lock().unwrap() = Some(parse_sample(
            &[json!({"name": "default", "free_space": 5, "total_space": 10})],
            &[],
            &[json!({"name": "gaps", "total_rows": 1, "total_bytes": 2})],
            unix_now_ms(),
        ));
        let mut f = Families::new();
        m.collect(&mut f);
        let text = f.render();
        assert!(
            text.contains("vulture_clickhouse_disk_free_bytes{disk=\"default\"} 5"),
            "{text}"
        );
        assert!(
            text.contains("vulture_clickhouse_table_rows{table=\"gaps\"} 1"),
            "{text}"
        );
    }
}
