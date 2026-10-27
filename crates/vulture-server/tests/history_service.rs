//! `History.Query` and `History.Aggregate` on in-memory stores: ordering, pagination, the hot/cold boundary,
//! bin arithmetic against a brute-force reference, the OI movers against the detector's own series arithmetic,
//! limits and failures. The same scenarios run against real ClickHouse and Redis in `history_clickhouse.rs`.

mod common;

use std::collections::{BTreeMap, HashSet};

use common::history_data::*;
use common::history_rig::*;
use tonic::{Code, Request, Status};
use vulture_proto::v1::history_server::History;
use vulture_proto::v1::{
    AggregateRequest, Event, EventType, GroupBy, HistoryRequest, Metric, OiMoverSort, Step,
    event::Payload,
};
use vulture_server::alerts::series::{Sample, Series, SeriesLimits};
use vulture_server::query::agg::base_asset;
use vulture_server::query::event::event_key;

// ---------------------------------------------------------------- Query

#[tokio::test]
async fn query_returns_the_period_in_time_then_id_order() {
    let ds = dataset().await;
    let r = rig(&ds, None, true, cfg()).await;
    let got = query(&r.handle, period()).await.unwrap();
    let want = expected(&ds, &period());
    assert!(got.len() > 2000, "{}", got.len());
    assert_eq!(got, want);
    // Strictly ordered by (time, id as numbers).
    let keys: Vec<_> = got.iter().map(|e| event_key(e).unwrap()).collect();
    assert!(keys.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(r.service.stats().events_total(), got.len() as u64);
}

#[tokio::test]
async fn pagination_by_cursor_reproduces_the_whole_answer_across_the_boundary() {
    let ds = dataset().await;
    // The watermark in the middle; 25 events are in both stores (flushed, watermark not yet moved).
    let r = rig(&ds, Some((0.5, 25)), true, cfg()).await;
    let want = expected(&ds, &period());
    let mut got: Vec<Event> = Vec::new();
    let mut cursor = String::new();
    let mut pages = 0;
    loop {
        let page = query(
            &r.handle,
            HistoryRequest {
                limit: 333,
                cursor: cursor.clone(),
                ..period()
            },
        )
        .await
        .unwrap();
        pages += 1;
        if page.is_empty() {
            break;
        }
        cursor = page.last().unwrap().id.clone();
        let full = page.len() == 333;
        got.extend(page);
        if !full {
            break;
        }
        assert!(pages < 50);
    }
    assert!(pages > 5);
    assert_eq!(
        ids(&got),
        ids(&want),
        "no loss, no duplicate across the watermark"
    );
    assert_eq!(got, want);
    let unique: HashSet<_> = ids(&got).into_iter().collect();
    assert_eq!(unique.len(), got.len());
    assert!(r.service.stats().fallbacks() == 0);
}

#[tokio::test]
async fn the_boundary_is_exact_wherever_the_watermark_stands() {
    let ds = dataset().await;
    let want = expected(&ds, &period());
    for (fraction, overlap) in [(0.001, 0), (0.3, 0), (0.3, 7), (0.9, 100), (0.999, 0)] {
        let r = rig(&ds, Some((fraction, overlap)), true, cfg()).await;
        let got = query(&r.handle, period()).await.unwrap();
        assert_eq!(
            ids(&got),
            ids(&want),
            "fraction {fraction} overlap {overlap}"
        );
    }
}

#[tokio::test]
async fn a_cursor_inside_the_tail_resumes_from_redis() {
    let ds = dataset().await;
    let r = rig(&ds, Some((0.5, 0)), true, cfg()).await;
    let want = expected(&ds, &period());
    let pick = want.len() * 3 / 4;
    let got = query(
        &r.handle,
        HistoryRequest {
            cursor: want[pick].id.clone(),
            ..period()
        },
    )
    .await
    .unwrap();
    assert_eq!(ids(&got), ids(&want[pick + 1..]));
}

#[tokio::test]
async fn filters_by_exchange_symbol_and_type() {
    let ds = dataset().await;
    let r = rig(&ds, Some((0.5, 3)), true, cfg()).await;
    for req in [
        HistoryRequest {
            exchanges: vec!["bybit".into()],
            ..period()
        },
        HistoryRequest {
            symbols: vec!["ETH-USDT-PERP".into()],
            ..period()
        },
        HistoryRequest {
            types: vec![EventType::Liquidation as i32],
            exchanges: vec!["binance".into(), "okx".into()],
            ..period()
        },
        HistoryRequest {
            types: vec![EventType::Gap as i32, EventType::Signal as i32],
            ..period()
        },
        HistoryRequest {
            from_ts: START + 61 * MIN,
            to_ts: START + 62 * MIN,
            ..period()
        },
    ] {
        let got = query(&r.handle, req.clone()).await.unwrap();
        let want = expected(&ds, &req);
        assert_eq!(ids(&got), ids(&want), "{req:?}");
    }
    // The two gap records both belong to a period that overlaps them; a period inside the bybit hole gets that gap.
    let hole = HistoryRequest {
        from_ts: START + 80 * MIN,
        to_ts: START + 81 * MIN,
        types: vec![EventType::Gap as i32],
        ..period()
    };
    let got = query(&r.handle, hole).await.unwrap();
    assert_eq!(got.len(), 1);
    let Some(Payload::Gap(g)) = &got[0].payload else {
        panic!()
    };
    assert_eq!(
        (g.exchange.as_str(), g.stream.as_str()),
        ("bybit", "open_interest")
    );
}

#[tokio::test]
async fn without_clickhouse_redis_answers_alone() {
    let ds = dataset().await;
    let r = rig(&ds, None, false, cfg()).await;
    let req = HistoryRequest {
        from_ts: START + 30 * MIN,
        to_ts: START + 105 * MIN,
        ..HistoryRequest::default()
    };
    let got = query(&r.handle, req.clone()).await.unwrap();
    let want = expected(&ds, &req);
    assert_eq!(ids(&got), ids(&want));
    assert!(!got.is_empty());
    // Resume works there too.
    let tail = query(
        &r.handle,
        HistoryRequest {
            cursor: want[10].id.clone(),
            ..req
        },
    )
    .await
    .unwrap();
    assert_eq!(ids(&tail), ids(&want[11..]));
}

#[tokio::test]
async fn a_failing_clickhouse_falls_back_to_redis_when_it_covers_the_period() {
    let ds = dataset().await;
    let r = rig(&ds, None, true, cfg()).await;
    r.cold.as_ref().unwrap().set_failing(true);
    let got = query(&r.handle, period()).await.unwrap();
    assert_eq!(ids(&got), ids(&expected(&ds, &period())));
    assert_eq!(r.service.stats().fallbacks(), 1);
    // A period older than anything Redis holds cannot be answered: UNAVAILABLE with a hint.
    let old = HistoryRequest {
        from_ts: 1_000_000,
        to_ts: 2_000_000,
        ..HistoryRequest::default()
    };
    let err = query(&r.handle, old).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");
    assert!(err.message().contains("ClickHouse"), "{err}");
}

#[tokio::test]
async fn invalid_requests_and_limits_have_clear_statuses() {
    let ds = dataset().await;
    let mut c = cfg();
    c.max_query_range_secs = 4 * 3600;
    c.max_rows = 500;
    c.default_limit = 100;
    let r = rig(&ds, None, true, c).await;
    let code = |e: Result<Vec<Event>, Status>| e.unwrap_err().code();
    assert_eq!(
        code(
            query(
                &r.handle,
                HistoryRequest {
                    from_ts: END,
                    to_ts: START,
                    ..Default::default()
                }
            )
            .await
        ),
        Code::InvalidArgument
    );
    assert_eq!(
        code(
            query(
                &r.handle,
                HistoryRequest {
                    from_ts: -1,
                    to_ts: START,
                    ..Default::default()
                }
            )
            .await
        ),
        Code::InvalidArgument
    );
    assert_eq!(
        code(
            query(
                &r.handle,
                HistoryRequest {
                    types: vec![0],
                    ..period()
                }
            )
            .await
        ),
        Code::InvalidArgument
    );
    assert_eq!(
        code(
            query(
                &r.handle,
                HistoryRequest {
                    cursor: "nonsense".into(),
                    ..period()
                }
            )
            .await
        ),
        Code::InvalidArgument
    );
    assert_eq!(
        code(
            query(
                &r.handle,
                HistoryRequest {
                    symbols: vec!["a\nb".into()],
                    ..period()
                }
            )
            .await
        ),
        Code::InvalidArgument
    );
    // A cursor that is not an event of the period.
    let err = query(
        &r.handle,
        HistoryRequest {
            cursor: "1-1".into(),
            from_ts: START,
            to_ts: START + HOUR,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("cursor"), "{err}");
    // Too long a period, too large a limit.
    let err = query(
        &r.handle,
        HistoryRequest {
            from_ts: START - 2 * HOUR,
            ..period()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::OutOfRange);
    assert!(err.message().contains("Aggregate"), "hint: {err}");
    let err = query(
        &r.handle,
        HistoryRequest {
            to_ts: START + HOUR,
            limit: 501,
            ..period()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
    assert!(err.message().contains("cursor"), "hint: {err}");
    // limit = 0 is the default limit.
    let got = query(
        &r.handle,
        HistoryRequest {
            to_ts: START + HOUR,
            ..period()
        },
    )
    .await
    .unwrap();
    assert_eq!(got.len(), 100);
    let stats = r.service.stats();
    assert_eq!(
        stats.rejections(vulture_server::query::stats::Reject::Range),
        1
    );
    assert_eq!(
        stats.rejections(vulture_server::query::stats::Reject::Limit),
        1
    );
}

#[tokio::test]
async fn too_many_concurrent_calls_are_refused() {
    let ds = dataset().await;
    let mut c = cfg();
    c.max_concurrent = 1;
    let r = rig(&ds, None, true, c).await;
    // Hold the only permit with a stream nobody reads: the producer blocks on the full channel.
    let held = r.handle.query(Request::new(period())).await.unwrap();
    let err = query(&r.handle, period()).await.unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
    assert!(err.message().contains("max_concurrent"), "{err}");
    let err = aggregate(
        &r.handle,
        AggregateRequest {
            group_by: GroupBy::Symbol as i32,
            step: Step::Hour as i32,
            from_ts: START,
            to_ts: END,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
    drop(held);
    // The permit is released when the stream is dropped.
    for _ in 0..100 {
        if query(
            &r.handle,
            HistoryRequest {
                limit: 1,
                ..period()
            },
        )
        .await
        .is_ok()
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("the permit was not released");
}

// ---------------------------------------------------------------- Aggregate

type LastCell = (i64, f64, Option<(i64, f64)>);

#[tokio::test]
async fn aggregate_matches_a_brute_force_reference() {
    let ds = dataset().await;
    let r = rig(&ds, None, true, cfg()).await;
    for (step, step_ms) in [
        (Step::Minute5, 5 * MIN),
        (Step::Minute15, 15 * MIN),
        (Step::Hour, HOUR),
        (Step::Hour4, 4 * HOUR),
        (Step::Minute1, MIN),
    ] {
        for group_by in [GroupBy::Symbol, GroupBy::BaseAsset, GroupBy::Exchange] {
            let resp = aggregate(
                &r.handle,
                req(
                    group_by,
                    step,
                    &[
                        Metric::LiqSumUsd,
                        Metric::LiqCount,
                        Metric::LiqLongUsd,
                        Metric::LiqShortUsd,
                        Metric::LiqMaxUsd,
                        Metric::OiEndBase,
                        Metric::OiEndUsd,
                    ],
                ),
            )
            .await
            .unwrap();
            let key = |ex: &str, sym: &str| match group_by {
                GroupBy::Exchange => ex.to_string(),
                GroupBy::Symbol => sym.to_string(),
                _ => base_asset(sym).to_string(),
            };
            #[derive(Default)]
            struct Ref {
                sum: f64,
                count: f64,
                long: f64,
                short: f64,
                max: f64,
                has_liq: bool,
            }
            let mut liq: BTreeMap<(String, i64), Ref> = BTreeMap::new();
            let mut last: BTreeMap<(String, String, i64), LastCell> = BTreeMap::new();
            for e in &ds.events {
                match e.payload.as_ref().unwrap() {
                    Payload::Liquidation(l) => {
                        let bin = l.exchange_ts.div_euclid(step_ms) * step_ms;
                        let x = liq.entry((key(&l.exchange, &l.symbol), bin)).or_default();
                        x.sum += l.notional_usd;
                        x.count += 1.0;
                        if l.side == 1 {
                            x.long += l.notional_usd
                        } else {
                            x.short += l.notional_usd
                        }
                        x.max = x.max.max(l.notional_usd);
                        x.has_liq = true;
                    }
                    Payload::OpenInterest(o) => {
                        let bin = o.exchange_ts.div_euclid(step_ms) * step_ms;
                        let slot = last
                            .entry((o.exchange.clone(), o.symbol.clone(), bin))
                            .or_insert((i64::MIN, 0.0, None));
                        if o.exchange_ts >= slot.0 {
                            slot.0 = o.exchange_ts;
                            slot.1 = o.oi_base;
                        }
                        if let Some(u) = o.oi_usd
                            && slot.2.is_none_or(|(t, _)| o.exchange_ts >= t)
                        {
                            slot.2 = Some((o.exchange_ts, u));
                        }
                    }
                    _ => {}
                }
            }
            // Sum of the end levels of the series of each group.
            let mut end: BTreeMap<(String, i64), (f64, f64, bool)> = BTreeMap::new();
            for ((ex, sym, bin), (_, base, usd)) in &last {
                let e = end.entry((key(ex, sym), *bin)).or_default();
                e.0 += base;
                if let Some((_, u)) = usd {
                    e.1 += u;
                    e.2 = true;
                }
            }
            assert!(!resp.rows.is_empty());
            for row in &resp.rows {
                let k = (row.group.clone(), row.bucket_start);
                assert_eq!(row.bucket_end - row.bucket_start, step_ms);
                let l = liq.get(&k);
                assert!(
                    close(
                        val(&row.values, Metric::LiqSumUsd),
                        (end.contains_key(&k) || l.is_some()).then(|| l.map_or(0.0, |x| x.sum))
                    ),
                    "{step:?} {group_by:?} {k:?}"
                );
                if let Some(l) = l {
                    assert_eq!(val(&row.values, Metric::LiqCount), Some(l.count));
                    assert!(close(val(&row.values, Metric::LiqLongUsd), Some(l.long)));
                    assert!(close(val(&row.values, Metric::LiqShortUsd), Some(l.short)));
                    assert!(close(val(&row.values, Metric::LiqMaxUsd), Some(l.max)));
                } else {
                    assert_eq!(val(&row.values, Metric::LiqMaxUsd), None);
                }
                let e = end.get(&k);
                if group_by == GroupBy::Exchange {
                    assert_eq!(
                        val(&row.values, Metric::OiEndBase),
                        None,
                        "base units mix assets"
                    );
                } else {
                    assert!(
                        close(val(&row.values, Metric::OiEndBase), e.map(|x| x.0)),
                        "{step:?} {group_by:?} {k:?}"
                    );
                }
                assert!(
                    close(
                        val(&row.values, Metric::OiEndUsd),
                        e.filter(|x| x.2).map(|x| x.1)
                    ),
                    "{step:?} {group_by:?} {k:?}"
                );
            }
            // Every reference cell with liquidations is in the answer.
            let rows: HashSet<_> = resp
                .rows
                .iter()
                .map(|r| (r.group.clone(), r.bucket_start))
                .collect();
            for k in liq.keys() {
                assert!(rows.contains(k), "{k:?}");
            }
        }
    }
}

#[tokio::test]
async fn aggregate_over_the_boundary_equals_aggregate_over_one_store() {
    let ds = dataset().await;
    let whole = rig(&ds, None, true, cfg()).await;
    let all_metrics: Vec<Metric> = vulture_server::query::agg::DEFAULT_METRICS.to_vec();
    for (fraction, overlap) in [(0.5, 0), (0.5, 40), (0.97, 5), (0.2, 0)] {
        let split = rig(&ds, Some((fraction, overlap)), true, cfg()).await;
        for step in [Step::Minute1, Step::Minute5, Step::Hour] {
            for group_by in [GroupBy::Symbol, GroupBy::BaseAsset, GroupBy::Exchange] {
                let r = req(group_by, step, &all_metrics);
                let a = aggregate(&whole.handle, r.clone()).await.unwrap();
                let b = aggregate(&split.handle, r).await.unwrap();
                assert_eq!(a.source, "clickhouse");
                assert_eq!(b.source, "clickhouse+redis");
                assert_same_rows(
                    &a,
                    &b,
                    &format!("{fraction}/{overlap} {step:?} {group_by:?}"),
                );
            }
        }
    }
}

#[tokio::test]
async fn redis_alone_gives_the_same_bins() {
    let ds = dataset().await;
    let whole = rig(&ds, None, true, cfg()).await;
    let redis = rig(&ds, None, false, cfg()).await;
    let r = req(
        GroupBy::Symbol,
        Step::Minute15,
        &vulture_server::query::agg::DEFAULT_METRICS,
    );
    let a = aggregate(&whole.handle, r.clone()).await.unwrap();
    let b = aggregate(&redis.handle, r).await.unwrap();
    assert_eq!(b.source, "redis");
    assert_same_rows(&a, &b, "redis only");
}

#[tokio::test]
async fn oi_change_levels_candles_and_totals() {
    let ds = dataset().await;
    let r = rig(&ds, Some((0.6, 10)), true, cfg()).await;
    let mut q = req(
        GroupBy::Symbol,
        Step::Minute5,
        &[
            Metric::OiEndBase,
            Metric::OiChangeBase,
            Metric::MarkOpen,
            Metric::MarkHigh,
            Metric::MarkLow,
            Metric::MarkClose,
            Metric::OiTotalUsd,
            Metric::OiEndUsd,
        ],
    );
    q.symbols = vec!["BTC-USDT-PERP".into()];
    let resp = aggregate(&r.handle, q).await.unwrap();
    // Two exchanges have BTC-USDT-PERP: the group sums them.
    let rows: Vec<_> = resp
        .rows
        .iter()
        .filter(|r| r.group == "BTC-USDT-PERP")
        .collect();
    assert_eq!(rows.len(), 36, "3 h of 5 minute bins");
    for w in rows.windows(2) {
        let (a, b) = (&w[0].values, &w[1].values);
        // change(b) = end(b) - end(a) for consecutive bins of the group.
        let d = val(b, Metric::OiEndBase).unwrap() - val(a, Metric::OiEndBase).unwrap();
        assert!(close(val(b, Metric::OiChangeBase), Some(d)), "{d}");
    }
    for row in &rows {
        let (o, h, l, c) = (
            val(&row.values, Metric::MarkOpen),
            val(&row.values, Metric::MarkHigh),
            val(&row.values, Metric::MarkLow),
            val(&row.values, Metric::MarkClose),
        );
        let (o, h, l, c) = (o.unwrap(), h.unwrap(), l.unwrap(), c.unwrap());
        assert!(l <= o && o <= h && l <= c && c <= h, "{row:?}");
        // The symbol group is the base asset restricted by the filter: the total equals the end level in USD.
        assert!(close(
            val(&row.values, Metric::OiTotalUsd),
            val(&row.values, Metric::OiEndUsd)
        ));
    }
    // The spike of Binance BTC (+6% in four minutes at hour 2) shows as a large change in the 5 minute bins there.
    let spike_bin = START + 2 * HOUR;
    let row = rows.iter().find(|r| r.bucket_start == spike_bin).unwrap();
    let change = val(&row.values, Metric::OiChangeBase).unwrap();
    assert!(change > 40.0, "{change}");
}

#[tokio::test]
async fn bins_holes_and_gaps_are_partial_and_null() {
    let ds = dataset().await;
    let r = rig(&ds, Some((0.5, 0)), true, cfg()).await;
    let mut q = req(
        GroupBy::Symbol,
        Step::Minute5,
        &[Metric::OiEndBase, Metric::OiChangeBase, Metric::LiqSumUsd],
    );
    q.symbols = vec!["SOL-USDT-PERP".into()];
    let resp = aggregate(&r.handle, q).await.unwrap();
    assert!(resp.partial);
    let in_hole = |t: i64| (START + 70 * MIN..START + 100 * MIN).contains(&t);
    for row in &resp.rows {
        let inside = in_hole(row.bucket_start) && in_hole(row.bucket_end - 1);
        if inside {
            assert!(row.partial, "{row:?}");
            assert_eq!(val(&row.values, Metric::OiEndBase), None, "null, not zero");
            // A liquidation of the symbol in the hole is still data; without one the bin has no data at all.
            let liq = val(&row.values, Metric::LiqSumUsd);
            assert!(liq.is_none_or(|v| v > 0.0), "{row:?}");
        }
    }
    // The first bin after the hole has a level but no one-step change.
    let after = resp
        .rows
        .iter()
        .find(|r| r.bucket_start == START + 100 * MIN)
        .unwrap();
    assert!(after.partial);
    assert!(val(&after.values, Metric::OiEndBase).is_some());
    assert_eq!(val(&after.values, Metric::OiChangeBase), None);
    // Series far from the hole are untouched.
    let mut q = req(
        GroupBy::Symbol,
        Step::Minute5,
        &[Metric::OiEndBase, Metric::OiChangeBase],
    );
    q.symbols = vec!["ETH-USDT-PERP".into()];
    q.exchanges = vec!["binance".into()];
    let resp = aggregate(&r.handle, q).await.unwrap();
    assert!(!resp.partial);
    assert!(resp.rows.iter().all(|r| !r.partial));
}

#[tokio::test]
async fn bins_are_aligned_and_widened_to_utc_edges() {
    let ds = dataset().await;
    let r = rig(&ds, None, true, cfg()).await;
    let mut q = req(GroupBy::Exchange, Step::Day, &[Metric::LiqCount]);
    q.from_ts = START + 17 * MIN;
    q.to_ts = START + 2 * HOUR;
    let resp = aggregate(&r.handle, q).await.unwrap();
    assert_eq!(resp.bins_from_ts, T0);
    assert_eq!(resp.bins_to_ts, T0 + 24 * HOUR);
    assert!(
        resp.rows
            .iter()
            .all(|r| r.bucket_start == T0 && r.bucket_end == T0 + 24 * HOUR)
    );
    // The weekly bin starts on the Monday the data lies in.
    let mut q = req(GroupBy::Exchange, Step::Week, &[Metric::LiqCount]);
    q.from_ts = START;
    let resp = aggregate(&r.handle, q).await.unwrap();
    assert_eq!(resp.bins_from_ts, T0);
    assert_eq!(resp.bins_to_ts - resp.bins_from_ts, 7 * 24 * HOUR);
    // The 4 hour bins are aligned to 00:00 UTC.
    let resp = aggregate(
        &r.handle,
        req(GroupBy::Exchange, Step::Hour4, &[Metric::LiqCount]),
    )
    .await
    .unwrap();
    assert!(resp.rows.iter().all(|r| r.bucket_start % (4 * HOUR) == 0));
    // The month.
    let mut q = req(GroupBy::Exchange, Step::Month, &[Metric::LiqCount]);
    q.to_ts = END;
    let resp = aggregate(&r.handle, q).await.unwrap();
    assert_eq!(resp.bins_from_ts, 1_790_812_800_000, "2026-10-01");
    assert_eq!(resp.bins_to_ts, 1_793_491_200_000, "2026-11-01");
}

#[tokio::test]
async fn aggregate_limits_and_validation() {
    let ds = dataset().await;
    let mut c = cfg();
    c.max_bins = 100;
    c.max_aggregate_rows = 100;
    let r = rig(&ds, None, true, c).await;
    let ok = |q: AggregateRequest| q;
    // 3 hours of minutes is 180 bins > 100.
    let err = aggregate(
        &r.handle,
        ok(req(GroupBy::Symbol, Step::Minute1, &[Metric::LiqCount])),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::OutOfRange);
    assert!(err.message().contains("larger step"), "{err}");
    // Few bins, many groups: 36 bins x 4 symbols > 100 rows.
    let err = aggregate(
        &r.handle,
        ok(req(GroupBy::Symbol, Step::Minute5, &[Metric::OiEndUsd])),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
    assert!(err.message().contains("filter"), "{err}");
    for bad in [
        AggregateRequest {
            group_by: 0,
            ..req(GroupBy::Symbol, Step::Hour, &[])
        },
        AggregateRequest {
            step: 0,
            ..req(GroupBy::Symbol, Step::Hour, &[])
        },
        AggregateRequest {
            step: 99,
            ..req(GroupBy::Symbol, Step::Hour, &[])
        },
        AggregateRequest {
            metrics: vec![0],
            ..req(GroupBy::Symbol, Step::Hour, &[])
        },
        AggregateRequest {
            metrics: vec![999],
            ..req(GroupBy::Symbol, Step::Hour, &[])
        },
        AggregateRequest {
            from_ts: END,
            to_ts: START,
            ..req(GroupBy::Symbol, Step::Hour, &[])
        },
        AggregateRequest {
            symbols: vec![String::new()],
            ..req(GroupBy::Symbol, Step::Hour, &[])
        },
    ] {
        let err = aggregate(&r.handle, bad.clone()).await.unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{bad:?}");
    }
    // An empty metric list means the default set.
    let resp = aggregate(&r.handle, req(GroupBy::Exchange, Step::Hour, &[]))
        .await
        .unwrap();
    assert_eq!(
        resp.rows[0].values.len(),
        vulture_server::query::agg::DEFAULT_METRICS.len()
    );
}

// ---------------------------------------------------------------- movers

/// What the live engine holds for each series after the stream up to `end` was fed to it, and its change over the
/// window: `Series` with the engine's limits, `Gap` records as exchange-level breaks.
fn engine_reference(
    ds: &Dataset,
    end: i64,
    window_ms: i64,
) -> BTreeMap<(String, String), (f64, f64, f64, f64)> {
    let limits = SeriesLimits {
        retention_ms: 4 * HOUR + 10 * MIN,
        max_samples: 1024,
        spacing_ms: 15_000,
        hole_ms: 300_000,
    };
    let mut series: BTreeMap<(String, String), Series> = BTreeMap::new();
    let mut exchange_break: BTreeMap<String, i64> = BTreeMap::new();
    let mut global = 0;
    let mut by_recv: Vec<&Event> = ds.events.iter().collect();
    by_recv.sort_by_key(|e| event_key(e).map(|k| (k.id_ms, k.id_seq)));
    for e in by_recv {
        match e.payload.as_ref().unwrap() {
            Payload::OpenInterest(o) if o.recv_ts <= end => {
                series
                    .entry((o.exchange.clone(), o.symbol.clone()))
                    .or_default()
                    .push(
                        Sample {
                            ts: o.recv_ts,
                            oi_base: o.oi_base,
                            oi_usd: o.oi_usd.unwrap_or(f64::NAN),
                            mark: o.mark_price.unwrap_or(f64::NAN),
                        },
                        &limits,
                    );
            }
            Payload::Gap(g) if g.stream != "liquidations" && g.to_ts <= end => {
                if g.exchange == "*" {
                    global = global.max(g.to_ts);
                } else {
                    let b = exchange_break.entry(g.exchange.clone()).or_insert(0);
                    *b = (*b).max(g.to_ts);
                }
            }
            _ => {}
        }
    }
    series
        .into_iter()
        .filter_map(|((ex, sym), s)| {
            let extra = global.max(exchange_break.get(&ex).copied().unwrap_or(0));
            let c = s.change(window_ms, extra)?;
            Some((
                (ex, sym),
                (c.reference.oi_base, c.now.oi_base, c.abs_base, c.pct),
            ))
        })
        .collect()
}

#[tokio::test]
async fn movers_equal_the_detectors_series_change() {
    let ds = dataset().await;
    for split in [None, Some((0.8, 0)), Some((0.8, 30)), Some((0.55, 0))] {
        let r = rig(&ds, split, true, cfg()).await;
        for end in [
            START + 2 * HOUR + 6 * MIN,
            START + 2 * HOUR + 30 * MIN,
            END + 60_000,
            START + 45 * MIN,
        ] {
            for window_secs in [60u32, 300, 900, 3600] {
                let resp = aggregate(&r.handle, movers_request(end, window_secs))
                    .await
                    .unwrap();
                let m = resp.oi_movers.unwrap();
                assert_eq!(m.window_end_ts, end);
                assert_eq!(m.window_start_ts, end - i64::from(window_secs) * 1000);
                let reference = engine_reference(&ds, end, i64::from(window_secs) * 1000);
                let listed: BTreeMap<_, _> = m
                    .top_growth
                    .iter()
                    .chain(&m.top_decline)
                    .map(|x| ((x.exchange.clone(), x.symbol.clone()), x))
                    .collect();
                for (key, (start, now, abs, pct)) in &reference {
                    if abs.abs() == 0.0 {
                        continue;
                    }
                    let got = listed.get(key).unwrap_or_else(|| {
                        panic!("{key:?} missing at {end} w{window_secs} split {split:?}")
                    });
                    assert!(!got.partial, "{key:?}");
                    assert!((got.oi_base_start - start).abs() < 1e-9, "{key:?}");
                    assert!((got.oi_base_end - now).abs() < 1e-9, "{key:?}");
                    assert!((got.change_abs - abs).abs() < 1e-9, "{key:?}");
                    assert!((got.change_pct - pct).abs() < 1e-9, "{key:?}");
                }
                // Nothing is listed that the detector's function would refuse, except as partial.
                for (key, got) in &listed {
                    assert!(
                        reference.contains_key(key) || got.partial,
                        "{key:?} end {end} w{window_secs}: listed but the detector has no change"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn the_ranking_orders_filters_and_flags_partial_series() {
    let ds = dataset().await;
    let r = rig(&ds, Some((0.9, 0)), true, cfg()).await;
    // Just after the Binance BTC spike: it tops the growth list by percent, with the absolute change.
    let end = START + 2 * HOUR + 6 * MIN;
    let resp = aggregate(&r.handle, movers_request(end, 300))
        .await
        .unwrap();
    let m = resp.oi_movers.unwrap();
    let top = &m.top_growth[0];
    assert_eq!(
        (top.exchange.as_str(), top.symbol.as_str()),
        ("binance", "BTC-USDT-PERP")
    );
    assert!(top.change_pct > 4.0 && top.change_abs > 0.0, "{top:?}");
    assert!(top.oi_usd_end.is_some());
    assert!(
        m.top_growth
            .windows(2)
            .all(|w| w[0].change_pct >= w[1].change_pct)
    );
    assert!(
        m.top_decline
            .windows(2)
            .all(|w| w[0].change_pct <= w[1].change_pct)
    );
    assert!(
        m.top_growth.iter().all(|x| x.change_abs > 0.0)
            && m.top_decline.iter().all(|x| x.change_abs < 0.0)
    );
    // Sorted by absolute change, filtered by exchange, limited.
    let mut q = movers_request(end, 300);
    q.exchanges = vec!["bybit".into()];
    let o = q.oi_movers.as_mut().unwrap();
    o.limit = 1;
    o.sort_by = OiMoverSort::AbsBase as i32;
    let resp = aggregate(&r.handle, q).await.unwrap();
    let m = resp.oi_movers.unwrap();
    assert!(m.top_growth.len() <= 1 && m.top_decline.len() <= 1);
    assert!(
        m.top_growth
            .iter()
            .chain(&m.top_decline)
            .all(|x| x.exchange == "bybit")
    );
    // A dust filter removes everything below the limit.
    let mut q = movers_request(end, 300);
    q.oi_movers.as_mut().unwrap().min_oi_usd = 1e18;
    let m = aggregate(&r.handle, q).await.unwrap().oi_movers.unwrap();
    assert!(m.top_growth.is_empty() && m.top_decline.is_empty());
    // The SOL series had a hole in its hour: the change is listed as partial, or left out on request.
    let end = START + 105 * MIN;
    let all = aggregate(&r.handle, movers_request(end, 3600))
        .await
        .unwrap();
    let sol = |m: &vulture_proto::v1::OiMovers| {
        m.top_growth
            .iter()
            .chain(&m.top_decline)
            .find(|x| x.symbol == "SOL-USDT-PERP")
            .cloned()
    };
    let m = all.oi_movers.unwrap();
    let sol_entry = sol(&m).expect("listed as partial");
    assert!(sol_entry.partial);
    assert!(all.partial);
    let mut q = movers_request(end, 3600);
    q.oi_movers.as_mut().unwrap().exclude_partial = true;
    let m = aggregate(&r.handle, q).await.unwrap().oi_movers.unwrap();
    assert!(sol(&m).is_none());
    // Window and limit validation.
    for window in [0u32, 59, 86_401] {
        let err = aggregate(&r.handle, movers_request(end, window))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{window}");
    }
    let mut q = movers_request(end, 300);
    q.oi_movers.as_mut().unwrap().limit = 10_000;
    assert_eq!(
        aggregate(&r.handle, q).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
}

#[tokio::test]
async fn a_symbol_that_stopped_reporting_is_not_a_mover() {
    let ds = dataset().await;
    let r = rig(&ds, None, true, cfg()).await;
    // 30 minutes after the end of the data every series is older than movers_max_stale_secs (15 min).
    let m = aggregate(&r.handle, movers_request(END + 30 * MIN, 300))
        .await
        .unwrap()
        .oi_movers
        .unwrap();
    assert!(m.top_growth.is_empty() && m.top_decline.is_empty());
}

#[tokio::test]
async fn stats_are_counted_per_method_and_code() {
    use vulture_server::query::stats::Method;
    let ds = dataset().await;
    let r = rig(&ds, None, true, cfg()).await;
    query(
        &r.handle,
        HistoryRequest {
            limit: 5,
            ..period()
        },
    )
    .await
    .unwrap();
    aggregate(
        &r.handle,
        req(GroupBy::Exchange, Step::Hour, &[Metric::LiqCount]),
    )
    .await
    .unwrap();
    aggregate(&r.handle, movers_request(START + HOUR, 300))
        .await
        .unwrap();
    let _ = aggregate(&r.handle, AggregateRequest::default()).await;
    let s = r.service.stats();
    assert_eq!(s.count(Method::Query, Code::Ok), 1);
    assert_eq!(s.count(Method::Aggregate, Code::Ok), 1);
    assert_eq!(s.count(Method::Movers, Code::Ok), 1);
    assert_eq!(s.count(Method::Aggregate, Code::InvalidArgument), 1);
    assert!(s.bins_total() > 0);
    let mut families = vulture_server::metrics::Families::new();
    vulture_server::metrics::MetricSource::collect(s.as_ref(), &mut families);
    let text = families.render();
    for name in [
        "vulture_history_requests_total{method=\"query\",code=\"ok\"} 1",
        "vulture_history_request_seconds_total",
        "vulture_history_rows_scanned_total{source=\"clickhouse\"}",
        "vulture_history_events_returned_total 5",
        "vulture_history_rejections_total{reason=\"range\"} 0",
        "vulture_history_in_flight 0",
    ] {
        assert!(text.contains(name), "{name}\n{text}");
    }
}
