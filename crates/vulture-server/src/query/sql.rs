//! The SQL the `History` service sends to ClickHouse. Pure string builders: values are quoted with [`lit`] or are
//! numbers, identifiers come from fixed tables, so nothing a client sends reaches the text unescaped.
//!
//! Every query constrains the sort key `(exchange, symbol, <time>)` first (equality or `IN` on the exchange and the
//! symbol when given, a range on the time column) and the partition key through the time range, so the primary
//! index prunes whatever it can.

use super::bins::BinStep;
use super::cold::{EventQuery, FineQuery, Kind, MoverQuery, Scope};

/// A ClickHouse string literal.
pub fn lit(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02X}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// `DateTime64(3, 'UTC')` constant for a millisecond timestamp.
pub fn ts_lit(ms: i64) -> String {
    format!("fromUnixTimestamp64Milli({ms}, 'UTC')")
}

fn in_list(col: &str, values: &[String], extra: &[&str]) -> Option<String> {
    if values.is_empty() {
        return None;
    }
    let mut items: Vec<String> = values.iter().map(|v| lit(v)).collect();
    items.extend(extra.iter().map(|v| lit(v)));
    Some(format!("{col} IN ({})", items.join(", ")))
}

/// The time column of a table.
pub fn time_col(kind: Kind) -> &'static str {
    match kind {
        Kind::Liquidation | Kind::OpenInterest => "exchange_ts",
        Kind::Gap => "from_ts",
        Kind::Signal => "ts",
    }
}

/// `WHERE` terms for the period of a kind: a range on the time column, or an overlap for gaps.
pub fn period(kind: Kind, from: i64, to: i64) -> String {
    match kind {
        Kind::Gap => format!("from_ts < {} AND to_ts >= {}", ts_lit(to), ts_lit(from)),
        k => {
            let c = time_col(k);
            format!("{c} >= {} AND {c} < {}", ts_lit(from), ts_lit(to))
        }
    }
}

/// `WHERE` terms for the filters, each starting with ` AND `. Gaps and signals of the writer or of several
/// exchanges (exchange `*`) and entries without a symbol pass, like in the live stream.
pub fn scope_terms(kind: Kind, scope: &Scope) -> String {
    let mut terms = Vec::new();
    match kind {
        Kind::Liquidation | Kind::OpenInterest => {
            terms.extend(in_list("exchange", &scope.exchanges, &[]));
            terms.extend(in_list("symbol", &scope.symbols, &[]));
            terms.extend(in_list(
                "splitByChar('-', symbol)[1]",
                &scope.base_assets,
                &[],
            ));
        }
        Kind::Gap => {
            terms.extend(in_list("exchange", &scope.exchanges, &["*"]));
        }
        Kind::Signal => {
            terms.extend(in_list("exchange", &scope.exchanges, &["*"]));
            if let Some(list) = in_list("symbol", &scope.symbols, &[""]) {
                terms.push(list);
            }
        }
    }
    terms.iter().map(|t| format!(" AND {t}")).collect()
}

const ID_COLS: &str = "toUInt64OrZero(splitByChar('-', event_id)[1]) AS idm, \
     toUInt64OrZero(splitByChar('-', event_id)[2]) AS ids";

/// Columns of a kind in the order [`super::ch`] decodes them.
fn event_columns(kind: Kind) -> &'static str {
    match kind {
        Kind::Liquidation => {
            "exchange, symbol, symbol_raw, toUnixTimestamp64Milli(exchange_ts) AS t_ms, \
             toUnixTimestamp64Milli(recv_ts) AS rts, side, price, qty, notional_usd, event_id"
        }
        Kind::OpenInterest => {
            "exchange, symbol, symbol_raw, toUnixTimestamp64Milli(exchange_ts) AS t_ms, \
             toUnixTimestamp64Milli(recv_ts) AS rts, oi_base, oi_usd, mark_price, event_id"
        }
        Kind::Gap => {
            "exchange, stream, toUnixTimestamp64Milli(from_ts) AS t_ms, \
             toUnixTimestamp64Milli(to_ts) AS to_ms, event_id"
        }
        Kind::Signal => {
            "type, exchange, symbol, window_secs, change_pct, change_abs, threshold, \
             toUnixTimestamp64Milli(ts) AS t_ms, details, event_id"
        }
    }
}

/// `SELECT` of the events of one kind in answer order.
pub fn events(kind: Kind, q: &EventQuery) -> String {
    let c = time_col(kind);
    let mut sql = format!(
        "SELECT {}, {ID_COLS} FROM {} WHERE {}{}",
        event_columns(kind),
        kind.table(),
        period(kind, q.from_ts, q.to_ts),
        scope_terms(kind, &q.scope),
    );
    if let Some(k) = q.after {
        sql.push_str(&format!(
            " AND ({c}, idm, ids) > ({}, {}, {})",
            ts_lit(k.ts),
            k.id_ms,
            k.id_seq
        ));
    }
    sql.push_str(&format!(" ORDER BY {c}, idm, ids LIMIT {}", q.limit));
    sql
}

/// Looks up the time of one event by its ID within the period and filters of `q`.
pub fn event_time(kind: Kind, id: &str, q: &EventQuery) -> String {
    format!(
        "SELECT toUnixTimestamp64Milli({}) AS t_ms FROM {} WHERE {}{} AND event_id = {} LIMIT 1",
        time_col(kind),
        kind.table(),
        period(kind, q.from_ts, q.to_ts),
        scope_terms(kind, &q.scope),
        lit(id)
    )
}

/// Rows with a stream ID not after the bound, as a condition on `event_id`.
pub fn id_bound_term(b: &super::cold::IdBound) -> String {
    format!(
        " AND (toUInt64OrZero(splitByChar('-', event_id)[1]), toUInt64OrZero(splitByChar('-', event_id)[2])) <= ({}, {})",
        b.ms, b.seq
    )
}

/// Liquidation cells.
pub fn fine_liquidations(q: &FineQuery) -> String {
    format!(
        "SELECT exchange, symbol, {bin} AS b, count() AS n, sum(notional_usd) AS s, \
         sumIf(notional_usd, side = 'long') AS l, sumIf(notional_usd, side = 'short') AS sh, \
         max(notional_usd) AS m FROM liquidations WHERE {period}{scope}{bound} \
         GROUP BY exchange, symbol, b LIMIT {limit}",
        bin = q.step.sql_bin("exchange_ts"),
        period = period(Kind::Liquidation, q.from, q.to),
        scope = scope_terms(Kind::Liquidation, &q.scope),
        bound = q.id_bound.as_ref().map(id_bound_term).unwrap_or_default(),
        limit = q.max_cells + 1,
    )
}

/// Open interest cells.
pub fn fine_open_interest(q: &FineQuery) -> String {
    let mut cols = String::from(
        "count() AS n, \
         toUnixTimestamp64Milli(min(exchange_ts)) AS first_ts, \
         toUnixTimestamp64Milli(max(exchange_ts)) AS last_ts, \
         argMax(oi_base, exchange_ts) AS last_base, \
         countIf(oi_usd IS NOT NULL) AS usd_n, \
         argMaxIf(oi_usd, exchange_ts, oi_usd IS NOT NULL) AS usd, \
         toUnixTimestamp64Milli(maxIf(exchange_ts, oi_usd IS NOT NULL)) AS usd_ts",
    );
    if q.needs.mark {
        cols.push_str(
            ", countIf(mark_price IS NOT NULL) AS mark_n, \
             argMinIf(mark_price, exchange_ts, mark_price IS NOT NULL) AS mark_open, \
             toUnixTimestamp64Milli(minIf(exchange_ts, mark_price IS NOT NULL)) AS mark_open_ts, \
             argMaxIf(mark_price, exchange_ts, mark_price IS NOT NULL) AS mark_close, \
             toUnixTimestamp64Milli(maxIf(exchange_ts, mark_price IS NOT NULL)) AS mark_close_ts, \
             maxIf(mark_price, mark_price IS NOT NULL) AS mark_high, \
             minIf(mark_price, mark_price IS NOT NULL) AS mark_low",
        );
    }
    format!(
        "SELECT exchange, symbol, {bin} AS b, {cols} FROM open_interest WHERE {period}{scope}{bound} \
         GROUP BY exchange, symbol, b LIMIT {limit}",
        bin = q.step.sql_bin("exchange_ts"),
        period = period(Kind::OpenInterest, q.from, q.to),
        scope = scope_terms(Kind::OpenInterest, &q.scope),
        bound = q.id_bound.as_ref().map(id_bound_term).unwrap_or_default(),
        limit = q.max_cells + 1,
    )
}

/// Gap records overlapping `[from, to)`.
pub fn gaps(from: i64, to: i64, exchanges: &[String]) -> String {
    let scope = Scope {
        exchanges: exchanges.to_vec(),
        ..Scope::default()
    };
    format!(
        "SELECT exchange, stream, toUnixTimestamp64Milli(from_ts) AS f, toUnixTimestamp64Milli(to_ts) AS t \
         FROM gaps WHERE {}{} ORDER BY from_ts LIMIT 100000",
        period(Kind::Gap, from, to),
        scope_terms(Kind::Gap, &scope),
    )
}

/// One row per active series with its newest sample, the samples near the reference point and the latest time
/// after a pause.
pub fn movers(q: &MoverQuery) -> String {
    format!(
        "SELECT exchange, symbol, max(rts) AS last_ts, \
         argMax((oi_base, oi_usd, mark_price), rts) AS last_t, \
         arraySort(c -> c.1, groupArrayIf((rts, oi_base, oi_usd, mark_price), rts >= {cf} AND rts <= {ct})) AS cands, \
         arraySort(groupArray(rts)) AS tss, \
         arrayMax(arrayMap((t, d) -> if(d > {hole}, t, 0), tss, arrayDifference(tss))) AS hole_ts \
         FROM (SELECT exchange, symbol, toUnixTimestamp64Milli(recv_ts) AS rts, oi_base, oi_usd, mark_price \
         FROM open_interest WHERE exchange_ts >= {lo} AND exchange_ts <= {hi}{scope} \
         AND recv_ts >= {from} AND recv_ts <= {end}) \
         GROUP BY exchange, symbol HAVING last_ts >= {min_last}",
        cf = q.cand_from,
        ct = q.cand_to,
        hole = q.hole_ms,
        lo = ts_lit(q.from - 120_000),
        hi = ts_lit(q.end + 120_000),
        from = ts_lit(q.from),
        end = ts_lit(q.end),
        scope = scope_terms(Kind::OpenInterest, &q.scope),
        min_last = q.min_last,
    )
}

/// Bin expression check used by tests of the gated suite: the start of the bin of a constant timestamp.
pub fn bin_of_constant(step: BinStep, ms: i64) -> String {
    format!(
        "SELECT {} AS b FROM (SELECT {} AS exchange_ts)",
        step.sql_bin("exchange_ts"),
        ts_lit(ms)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::agg::Needs;
    use crate::query::cold::{EventKey, IdBound};
    use std::time::Duration;

    fn q() -> EventQuery {
        EventQuery {
            kinds: vec![Kind::Liquidation],
            scope: Scope {
                exchanges: vec!["binance".into()],
                symbols: vec!["BTC-USDT-PERP".into()],
                base_assets: vec![],
            },
            from_ts: 1_000,
            to_ts: 2_000,
            after: None,
            limit: 50,
            max_memory: 1 << 30,
            timeout: Duration::from_secs(5),
        }
    }

    #[test]
    fn literals_are_escaped() {
        assert_eq!(lit("abc"), "'abc'");
        assert_eq!(lit("a'b"), "'a\\'b'");
        assert_eq!(lit("a\\b"), "'a\\\\b'");
        assert_eq!(lit("x' OR 1=1 --"), "'x\\' OR 1=1 --'");
        assert_eq!(lit("a\nb"), "'a\\x0Ab'");
        assert_eq!(lit("é"), "'é'");
    }

    #[test]
    fn event_queries_order_and_resume() {
        let sql = events(Kind::Liquidation, &q());
        assert!(
            sql.contains(
                "FROM liquidations WHERE exchange_ts >= fromUnixTimestamp64Milli(1000, 'UTC')"
            ),
            "{sql}"
        );
        assert!(
            sql.contains("AND exchange_ts < fromUnixTimestamp64Milli(2000, 'UTC')"),
            "{sql}"
        );
        assert!(
            sql.contains("AND exchange IN ('binance') AND symbol IN ('BTC-USDT-PERP')"),
            "{sql}"
        );
        assert!(
            sql.ends_with("ORDER BY exchange_ts, idm, ids LIMIT 50"),
            "{sql}"
        );
        assert!(!sql.contains("> ("), "no cursor, no resume term");
        let mut resumed = q();
        resumed.after = Some(EventKey {
            ts: 1500,
            id_ms: 7,
            id_seq: 3,
        });
        let sql = events(Kind::Liquidation, &resumed);
        assert!(
            sql.contains(
                "AND (exchange_ts, idm, ids) > (fromUnixTimestamp64Milli(1500, 'UTC'), 7, 3)"
            ),
            "{sql}"
        );
    }

    #[test]
    fn gaps_overlap_and_signals_keep_market_wide_rows() {
        let sql = events(Kind::Gap, &q());
        assert!(sql.contains("from_ts < fromUnixTimestamp64Milli(2000, 'UTC') AND to_ts >= fromUnixTimestamp64Milli(1000, 'UTC')"), "{sql}");
        assert!(sql.contains("exchange IN ('binance', '*')"), "{sql}");
        assert!(!sql.contains("symbol"), "gaps have no symbol: {sql}");
        let sql = events(Kind::Signal, &q());
        assert!(sql.contains("exchange IN ('binance', '*')"), "{sql}");
        assert!(sql.contains("symbol IN ('BTC-USDT-PERP', '')"), "{sql}");
        assert!(sql.contains("ORDER BY ts, idm, ids"), "{sql}");
    }

    #[test]
    fn hostile_values_stay_inside_literals() {
        let mut hostile = q();
        hostile.scope.symbols = vec!["x') OR 1=1; DROP TABLE liquidations; --".into()];
        let sql = events(Kind::Liquidation, &hostile);
        assert!(
            sql.contains("symbol IN ('x\\') OR 1=1; DROP TABLE liquidations; --')"),
            "{sql}"
        );
    }

    #[test]
    fn cells_use_the_bin_expression_and_the_id_bound() {
        let fq = FineQuery {
            step: BinStep::Minute5,
            from: 0,
            to: 600_000,
            scope: Scope::default(),
            needs: Needs {
                liq: true,
                oi: true,
                mark: true,
            },
            id_bound: Some(IdBound { ms: 99, seq: 4 }),
            max_cells: 10,
            max_memory: 1,
            timeout: Duration::from_secs(1),
        };
        let sql = fine_open_interest(&fq);
        assert!(
            sql.contains("intDiv(toUnixTimestamp64Milli(exchange_ts), 300000) * 300000 AS b"),
            "{sql}"
        );
        assert!(sql.contains("mark_high"), "{sql}");
        assert!(sql.contains("<= (99, 4)"), "{sql}");
        assert!(
            sql.ends_with("GROUP BY exchange, symbol, b LIMIT 11"),
            "{sql}"
        );
        let no_mark = FineQuery {
            needs: Needs {
                liq: false,
                oi: true,
                mark: false,
            },
            id_bound: None,
            ..fq.clone()
        };
        let sql = fine_open_interest(&no_mark);
        assert!(!sql.contains("mark_high") && !sql.contains("<= ("), "{sql}");
        let liq = fine_liquidations(&fq);
        assert!(liq.contains("sumIf(notional_usd, side = 'long')"), "{liq}");
    }

    #[test]
    fn base_assets_filter_on_the_first_symbol_segment() {
        let scope = Scope {
            base_assets: vec!["BTC".into(), "ETH".into()],
            ..Scope::default()
        };
        assert_eq!(
            scope_terms(Kind::OpenInterest, &scope),
            " AND splitByChar('-', symbol)[1] IN ('BTC', 'ETH')"
        );
        assert_eq!(scope_terms(Kind::Gap, &scope), "");
    }

    #[test]
    fn movers_query_names_the_ranges() {
        let sql = movers(&MoverQuery {
            scope: Scope::default(),
            end: 10_000_000,
            from: 1_000_000,
            cand_from: 2_000_000,
            cand_to: 3_000_000,
            min_last: 9_000_000,
            hole_ms: 300_000,
            max_memory: 1,
            timeout: Duration::from_secs(1),
        });
        assert!(sql.contains("rts >= 2000000 AND rts <= 3000000"), "{sql}");
        assert!(sql.contains("d > 300000"), "{sql}");
        assert!(sql.ends_with("HAVING last_ts >= 9000000"), "{sql}");
    }
}
