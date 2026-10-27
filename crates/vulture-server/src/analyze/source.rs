//! ClickHouse as the source of the analysis tools: read-only `SELECT`s through [`ChClient`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;

use super::study::{SignalKind, SignalRecord, StudySource};
use crate::history::{ChClient, ReadSettings};
use crate::query::sql::{lit, ts_lit};

/// The study on ClickHouse.
pub struct ChSource {
    client: Arc<ChClient>,
    settings: ReadSettings,
}

impl ChSource {
    /// Wraps a client; queries get `max_execution_time` and `max_memory_usage` limits.
    pub fn new(client: Arc<ChClient>) -> Self {
        Self {
            client,
            settings: ReadSettings {
                max_execution_time_secs: 120,
                max_memory_usage: 4 << 30,
                timeout: Duration::from_secs(130),
            },
        }
    }

    async fn rows(&self, sql: String) -> Result<Vec<serde_json::Value>, String> {
        self.client
            .query_rows_with(&sql, &self.settings)
            .await
            .map(|(r, _)| r)
            .map_err(|e| e.to_string())
    }
}

fn int(v: &serde_json::Value) -> i64 {
    match v {
        serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn float(v: &serde_json::Value) -> f64 {
    match v {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

impl StudySource for ChSource {
    fn signals(&self, from: i64, to: i64) -> BoxFuture<'_, Result<Vec<SignalRecord>, String>> {
        Box::pin(async move {
            let rows = self
                .rows(format!(
                    "SELECT type, exchange, symbol, window_secs, change_pct, threshold, \
                     toUnixTimestamp64Milli(ts) AS t_ms, details FROM signals \
                     WHERE ts >= {} AND ts < {} ORDER BY ts LIMIT 2000000",
                    ts_lit(from),
                    ts_lit(to)
                ))
                .await?;
            Ok(rows
                .iter()
                .filter_map(|r| {
                    let kind = SignalKind::parse(r["type"].as_str()?)?;
                    Some(SignalRecord {
                        ts: int(&r["t_ms"]),
                        kind,
                        exchange: r["exchange"].as_str()?.to_string(),
                        symbol: r["symbol"].as_str().unwrap_or_default().to_string(),
                        window_secs: int(&r["window_secs"]) as u32,
                        change_pct: float(&r["change_pct"]),
                        threshold: float(&r["threshold"]),
                        details: r["details"]
                            .as_object()
                            .map(|m| {
                                m.iter()
                                    .map(|(k, v)| {
                                        (k.clone(), v.as_str().unwrap_or_default().to_string())
                                    })
                                    .collect::<BTreeMap<_, _>>()
                            })
                            .unwrap_or_default(),
                    })
                })
                .collect())
        })
    }

    fn marks(
        &self,
        exchange: &str,
        symbol: &str,
        from: i64,
        to: i64,
    ) -> BoxFuture<'_, Result<Vec<(i64, f64)>, String>> {
        let sql = format!(
            "SELECT toUnixTimestamp64Milli(recv_ts) AS t, mark_price AS p FROM open_interest \
             WHERE exchange = {} AND symbol = {} AND exchange_ts >= {} AND exchange_ts <= {} \
             AND mark_price IS NOT NULL ORDER BY exchange_ts",
            lit(exchange),
            lit(symbol),
            ts_lit(from),
            ts_lit(to)
        );
        Box::pin(async move {
            let rows = self.rows(sql).await?;
            let mut v: Vec<(i64, f64)> = rows
                .iter()
                .map(|r| (int(&r["t"]), float(&r["p"])))
                .collect();
            v.sort_by_key(|x| x.0);
            Ok(v)
        })
    }

    fn liquidated_usd(
        &self,
        exchange: &str,
        symbol: &str,
        from: i64,
        to: i64,
    ) -> BoxFuture<'_, Result<f64, String>> {
        let mut sql = format!(
            "SELECT sum(notional_usd) AS s FROM liquidations WHERE exchange_ts > {} AND exchange_ts <= {}",
            ts_lit(from),
            ts_lit(to)
        );
        if exchange != "*" {
            sql.push_str(&format!(" AND exchange = {}", lit(exchange)));
        }
        if !symbol.is_empty() {
            sql.push_str(&format!(" AND symbol = {}", lit(symbol)));
        }
        Box::pin(async move {
            let rows = self.rows(sql).await?;
            Ok(rows.first().map_or(0.0, |r| float(&r["s"])))
        })
    }
}
