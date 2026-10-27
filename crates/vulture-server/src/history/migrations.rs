//! Numbered SQL migrations, embedded in the binary.
//!
//! Rules: forward-only and additive. A migration may create tables or add columns; it never drops, truncates or
//! rewrites data (a unit test rejects destructive statements). Applied versions are recorded in
//! `schema_migrations` together with a checksum, so editing a migration that has already run is detected.
//! Applying is idempotent and safe to run from several processes: every statement is `IF NOT EXISTS`.

use super::client::{ChClient, ChError, json_u64};

/// One migration.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    /// Strictly increasing version.
    pub version: u32,
    /// Short name.
    pub name: &'static str,
    /// SQL text: statements separated by `;`.
    pub sql: &'static str,
}

/// Every migration, in order. Append new ones at the end; never edit or remove an existing entry.
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "create_history_tables",
        sql: include_str!("../../migrations/0001_create_history_tables.sql"),
    },
    Migration {
        version: 2,
        name: "open_interest_funding",
        sql: include_str!("../../migrations/0002_open_interest_funding.sql"),
    },
];

const SCHEMA_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS schema_migrations \
    (version UInt32, name String, checksum String, applied_at DateTime64(3, 'UTC') DEFAULT now64(3)) \
    ENGINE = MergeTree ORDER BY version";

/// FNV-1a 64-bit hash of the SQL text, as 16 hex digits (change detection, not security).
pub fn checksum(sql: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in sql.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{h:016x}")
}

/// Splits SQL text into statements at `;`, ignoring `--` comments and semicolons inside `'...'` strings.
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_string = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if in_string {
            cur.push(c);
            if c == '\\' {
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
            } else if c == '\'' {
                in_string = false;
            }
            continue;
        }
        match c {
            '\'' => {
                in_string = true;
                cur.push(c);
            }
            '-' if chars.peek() == Some(&'-') => {
                for n in chars.by_ref() {
                    if n == '\n' {
                        cur.push('\n');
                        break;
                    }
                }
            }
            ';' => {
                let stmt = cur.trim().to_string();
                if !stmt.is_empty() {
                    out.push(stmt);
                }
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    let rest = cur.trim();
    if !rest.is_empty() {
        out.push(rest.to_string());
    }
    out
}

/// Creates the database if needed, then applies every migration that has not run yet. Returns the versions
/// applied by this call (empty when the schema is current).
pub async fn apply(client: &ChClient) -> Result<Vec<u32>, ChError> {
    client
        .execute_server(&format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            client.database()
        ))
        .await?;
    client.execute(SCHEMA_TABLE_DDL).await?;
    let rows = client
        .query_rows("SELECT version, checksum FROM schema_migrations ORDER BY version")
        .await?;
    let applied: Vec<(u32, String)> = rows
        .iter()
        .map(|r| {
            (
                json_u64(&r["version"]) as u32,
                r["checksum"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    if let Some(newest) = applied.iter().map(|(v, _)| *v).max()
        && newest > MIGRATIONS.last().map_or(0, |m| m.version)
    {
        tracing::warn!(
            newest,
            "the ClickHouse schema is newer than this binary knows; continuing with the tables as they are"
        );
    }
    let mut done = Vec::new();
    for m in MIGRATIONS {
        let sum = checksum(m.sql);
        if let Some((_, recorded)) = applied.iter().find(|(v, _)| *v == m.version) {
            if *recorded != sum {
                return Err(ChError::Migration(format!(
                    "migration {} ({}) was modified after it was applied (checksum {recorded} recorded, {sum} now)",
                    m.version, m.name
                )));
            }
            continue;
        }
        for stmt in split_statements(m.sql) {
            client.execute(&stmt).await?;
        }
        client
            .execute(&format!(
                "INSERT INTO schema_migrations (version, name, checksum) VALUES ({}, '{}', '{}')",
                m.version, m.name, sum
            ))
            .await?;
        tracing::info!(
            version = m.version,
            name = m.name,
            "ClickHouse migration applied"
        );
        done.push(m.version);
    }
    Ok(done)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_strictly_increasing_from_one() {
        assert!(!MIGRATIONS.is_empty());
        for (i, m) in MIGRATIONS.iter().enumerate() {
            assert_eq!(m.version as usize, i + 1, "{}", m.name);
            assert!(
                !m.name.is_empty() && m.name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
            );
            assert!(
                !split_statements(m.sql).is_empty(),
                "{} has no statements",
                m.name
            );
        }
    }

    #[test]
    fn migrations_are_additive_and_idempotent() {
        for m in MIGRATIONS {
            for stmt in split_statements(m.sql) {
                let upper = stmt.to_ascii_uppercase();
                for forbidden in [
                    "DROP ",
                    "TRUNCATE",
                    "DELETE ",
                    "RENAME ",
                    "DETACH ",
                    "OPTIMIZE ",
                    " MODIFY ",
                    " TTL ",
                ] {
                    assert!(
                        !upper.contains(forbidden),
                        "migration {} contains {forbidden:?}: {stmt}",
                        m.name
                    );
                }
                assert!(
                    upper.contains("IF NOT EXISTS") || upper.contains("ADD COLUMN IF NOT EXISTS"),
                    "migration {} has a statement that is not idempotent: {stmt}",
                    m.name
                );
            }
        }
    }

    #[test]
    fn first_migration_creates_the_four_tables_without_ttl() {
        let sql = MIGRATIONS[0].sql;
        for t in ["liquidations", "open_interest", "gaps", "signals"] {
            assert!(
                sql.contains(&format!("CREATE TABLE IF NOT EXISTS {t}")),
                "{t}"
            );
        }
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 4);
        for s in &stmts {
            assert!(s.contains("non_replicated_deduplication_window"), "{s}");
            assert!(s.contains("PARTITION BY toYYYYMM("), "{s}");
        }
    }

    #[test]
    fn second_migration_adds_three_nullable_columns() {
        let stmts = split_statements(MIGRATIONS[1].sql);
        assert_eq!(stmts.len(), 3);
        for (s, col) in stmts
            .iter()
            .zip(["funding_rate", "index_price", "next_funding_ts"])
        {
            assert!(
                s.contains(&format!("ADD COLUMN IF NOT EXISTS {col} Nullable(")),
                "{s}"
            );
        }
    }

    #[test]
    fn the_splitter_handles_comments_and_strings() {
        let sql = "-- c; one\nSELECT 'a;b' ; -- tail\nSELECT 'it\\'s;';\n\n;SELECT 3";
        assert_eq!(
            split_statements(sql),
            vec!["SELECT 'a;b'", "SELECT 'it\\'s;'", "SELECT 3"]
        );
    }

    #[test]
    fn checksum_is_stable_and_sensitive() {
        assert_eq!(checksum(""), "cbf29ce484222325");
        assert_ne!(checksum("a"), checksum("b"));
        assert_eq!(checksum("abc"), checksum("abc"));
    }
}
