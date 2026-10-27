//! The read-only ClickHouse user of `deploy/clickhouse/users.d/reader.xml`, against a real server started with that
//! file (gated: it needs its own container, see below).
//!
//! ```sh
//! docker run -d --rm --name vulture-test-reader -p 127.0.0.1:18124:8123 \
//!   -e CLICKHOUSE_USER=vulture -e CLICKHOUSE_PASSWORD=apppw -e CLICKHOUSE_DB=vulture \
//!   -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=0 -e CLICKHOUSE_READER_PASSWORD=readerpw \
//!   -v "$PWD/deploy/clickhouse/users.d/reader.xml:/etc/clickhouse-server/users.d/reader.xml:ro" \
//!   clickhouse/clickhouse-server:25.8
//! VULTURE_TEST_READER_URL=http://127.0.0.1:18124 VULTURE_TEST_READER_PASSWORD=readerpw \
//!   VULTURE_TEST_READER_ADMIN_PASSWORD=apppw cargo test -p vulture-server --test clickhouse_reader
//! ```
//!
//! The application user is `vulture` and the database `vulture`, as in the compose file.

mod common;

use std::sync::Arc;

use common::history_data::*;
use vulture_server::config::{ClickHouseConfig, Secret};
use vulture_server::history::ReadSettings;
use vulture_server::history::{ChClient, ChError, HistorySink, PageRows, Table, dedup_token};

fn clients() -> Option<(Arc<ChClient>, Arc<ChClient>)> {
    let url = match std::env::var("VULTURE_TEST_READER_URL") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!(
                "SKIPPED: VULTURE_TEST_READER_URL is not set (no ClickHouse with the reader user)"
            );
            return None;
        }
    };
    let make = |user: &str, pw: &str| {
        let cfg = ClickHouseConfig {
            url: url.clone(),
            user: user.into(),
            database: "vulture".into(),
            insert_timeout_secs: 60,
            ..ClickHouseConfig::default()
        };
        Arc::new(ChClient::new(&cfg, Secret::new(pw)).unwrap())
    };
    Some((
        make(
            "vulture",
            &std::env::var("VULTURE_TEST_READER_ADMIN_PASSWORD").unwrap_or_default(),
        ),
        make(
            "reader",
            &std::env::var("VULTURE_TEST_READER_PASSWORD").unwrap_or_default(),
        ),
    ))
}

fn settings() -> ReadSettings {
    // Deliberately different from the reader's profile: a read-only user must still work.
    ReadSettings {
        max_execution_time_secs: 120,
        max_memory_usage: 4 << 30,
        timeout: std::time::Duration::from_secs(60),
    }
}

fn is_denied(e: &ChError) -> bool {
    let m = e.to_string();
    m.contains("ACCESS_DENIED")
        || m.contains("READONLY")
        || m.contains("Not enough privileges")
        || m.contains("readonly")
}

#[tokio::test]
async fn the_reader_reads_the_history_and_nothing_else() {
    let Some((admin, reader)) = clients() else {
        return;
    };
    admin.migrate().await.unwrap();
    // Some history to read.
    let ds = dataset().await;
    let entries: Vec<_> = ds
        .events
        .iter()
        .take(2000)
        .map(|e| {
            let f = vulture_server::store::EntryFields::from_event(e).unwrap();
            vulture_server::store::StoredEntry {
                id: e.id.clone(),
                kind: f.kind.to_string(),
                exchange: f.exchange,
                symbol: f.symbol,
                data: f.data,
            }
        })
        .collect();
    let (first, last) = (entries[0].id.clone(), entries.last().unwrap().id.clone());
    let page = PageRows::build(entries);
    for t in Table::ALL {
        let b = &page.tables[t.index()];
        if b.rows > 0 {
            admin
                .insert(
                    t,
                    b.body.clone(),
                    &format!("{}:reader-test", dedup_token(t, &first, &last)),
                )
                .await
                .unwrap();
        }
    }

    // It reads, with the History client path too (settings it may not change are left to its profile).
    let (rows, _) = reader
        .query_rows_with("SELECT count() AS n FROM open_interest", &settings())
        .await
        .unwrap();
    assert!(vulture_server::history::client::json_u64(&rows[0]["n"]) > 0);
    let mut stream = reader
        .query_stream("SELECT exchange FROM liquidations LIMIT 5", &settings())
        .await
        .unwrap();
    let mut n = 0;
    while stream.next_row().await.unwrap().is_some() {
        n += 1;
    }
    assert!(n > 0);
    // Its profile is what the file says.
    let (rows, _) = reader
        .query_rows_with("SELECT getSetting('readonly') AS ro, getSetting('max_execution_time') AS t, getSetting('max_memory_usage') AS m, getSetting('max_rows_to_read') AS r, getSetting('max_result_rows') AS o, currentUser() AS u", &settings())
        .await
        .unwrap();
    let r = &rows[0];
    assert_eq!(r["u"], "reader");
    for (k, v) in [
        ("ro", 1u64),
        ("t", 30),
        ("m", 2_000_000_000),
        ("r", 500_000_000),
        ("o", 1_000_000),
    ] {
        assert_eq!(vulture_server::history::client::json_u64(&r[k]), v, "{k}");
    }

    // It writes nothing and changes nothing.
    for stmt in [
        "INSERT INTO liquidations (exchange, symbol, symbol_raw, exchange_ts, recv_ts, side, price, qty, notional_usd, event_id) VALUES ('x', 'x', 'x', now(), now(), 'long', 1, 1, 1, 'x')",
        "CREATE TABLE vulture.sneaky (a UInt8) ENGINE = Memory",
        "DROP TABLE liquidations",
        "TRUNCATE TABLE open_interest",
        "ALTER TABLE open_interest DELETE WHERE 1",
        "ALTER TABLE open_interest ADD COLUMN extra UInt8",
        "OPTIMIZE TABLE open_interest FINAL",
        "CREATE USER sneaky",
        "GRANT ALL ON *.* TO reader",
        "SET max_execution_time = 1000",
        "SET readonly = 0",
        "SELECT 1 SETTINGS readonly = 0",
        "SELECT 1 SETTINGS max_execution_time = 1000",
        "SELECT * FROM system.parts",
        "SELECT * FROM system.query_log",
    ] {
        let err = reader.execute(stmt).await.expect_err(stmt);
        assert!(is_denied(&err), "{stmt}: {err}");
    }
    assert!(rows_in(&admin, "SELECT count() AS n FROM liquidations").await > 0);

    // The limits hold.
    let err = reader
        .query_rows_with("SELECT count() FROM numbers(1000000000000)", &settings())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("max_rows_to_read"), "{err}");
    let err = reader
        .query_rows_with("SELECT number FROM numbers(2000000)", &settings())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("Limit for result exceeded"),
        "{err}"
    );
    // Wrong password, and no password, are refused.
    let cfg = ClickHouseConfig {
        url: std::env::var("VULTURE_TEST_READER_URL").unwrap(),
        user: "reader".into(),
        database: "vulture".into(),
        ..ClickHouseConfig::default()
    };
    let bad = ChClient::new(&cfg, Secret::new("wrong")).unwrap();
    assert!(bad.query_rows_with("SELECT 1", &settings()).await.is_err());
    let none = ChClient::new(&cfg, Secret::new("")).unwrap();
    assert!(none.query_rows_with("SELECT 1", &settings()).await.is_err());

    // The example queries all run as the reader.
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/clickhouse/queries");
    let mut ran = 0;
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if !name.ends_with(".sql") {
            continue;
        }
        let sql = std::fs::read_to_string(e.path()).unwrap().replace(
            "now()",
            &format!(
                "fromUnixTimestamp64Milli({}, 'UTC')",
                START + 3 * HOUR + 10 * MIN
            ),
        );
        reader
            .query_rows_with(&sql, &settings())
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        ran += 1;
    }
    assert_eq!(ran, 7);

    // Clean up what this test created.
    for t in [
        "liquidations",
        "open_interest",
        "gaps",
        "signals",
        "schema_migrations",
    ] {
        admin
            .execute(&format!("DROP TABLE IF EXISTS {t}"))
            .await
            .unwrap();
    }
}

async fn rows_in(c: &ChClient, sql: &str) -> u64 {
    let (rows, _) = c.query_rows_with(sql, &settings()).await.unwrap();
    vulture_server::history::client::json_u64(&rows[0]["n"])
}
