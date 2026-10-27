//! `History` against real ClickHouse and Redis: the SQL must give what the in-memory reference gives, across the
//! hot/cold boundary, for every step, grouping and filter. Gated by `VULTURE_TEST_CLICKHOUSE_URL` (and
//! `VULTURE_TEST_REDIS_URL` for the Redis tail); see `common/ch_env.rs`. Throwaway containers on loopback:
//!
//! ```sh
//! docker run -d --rm --name vulture-test-ch -p 127.0.0.1:8123:8123 -e CLICKHOUSE_PASSWORD=test clickhouse/clickhouse-server:25.8
//! docker run -d --rm --name vulture-test-redis -p 127.0.0.1:6379:6379 redis:7
//! ```

mod common;

use std::sync::Arc;

use common::ch_env::*;
use common::history_data::*;
use common::history_rig::*;
use tonic::Code;
use vulture_proto::v1::{
    AggregateRequest, EventType, GroupBy, HistoryRequest, Metric, OiMoverSort, Step,
};
use vulture_server::history::Watermark;
use vulture_server::query::bins::BinStep;
use vulture_server::query::cold::ColdStore;
use vulture_server::query::{ChCold, MemoryCold};
use vulture_server::store::RedisStore;

struct Pair {
    env: ChEnv,
    ch: HistoryHandle,
    mem: HistoryHandle,
    ds: Dataset,
}

use vulture_server::query::HistoryHandle;

/// ClickHouse holds the first `fraction` of the events plus `overlap`; the stream (Redis or in memory) holds all;
/// the watermark is after the first `fraction`.
async fn pair(name: &str, fraction: f64, overlap: usize, real_redis: bool) -> Option<Pair> {
    let env = ChEnv::new(name, real_redis).await?;
    let ds = dataset().await;
    let (i, wm_id) = ds.watermark_at(fraction);
    let cold_events = &ds.events[..(i + overlap).min(ds.events.len())];
    env.insert(cold_events).await;
    let mem_cold = Arc::new(MemoryCold::new());
    mem_cold.insert(cold_events.iter().cloned());
    let (store, watermark): (Arc<dyn RedisStore>, Option<Arc<Watermark>>) = if real_redis {
        env.redis_fill(&ds.events).await;
        env.redis_set_watermark(&wm_id).await;
        // The in-process watermark is left at 0-0 on purpose: the service must read the persisted one.
        (env.redis_store(), Some(Arc::new(Watermark::new())))
    } else {
        let wm = Arc::new(Watermark::new());
        wm.set(&wm_id);
        (ds.stream.store.clone(), Some(wm))
    };
    let (ch, _) = service_with(
        store.clone(),
        Some(Arc::new(ChCold::new(env.client.clone())) as Arc<dyn ColdStore>),
        watermark.clone(),
        cfg(),
    );
    let (mem, _) = service_with(
        store,
        Some(mem_cold as Arc<dyn ColdStore>),
        watermark,
        cfg(),
    );
    Some(Pair { env, ch, mem, ds })
}

#[tokio::test]
async fn sql_bins_agree_with_the_rust_bins() {
    let Some(env) = ChEnv::new("bins", false).await else {
        return;
    };
    let stamps = [
        0i64,
        1,
        T0,
        T0 + 3 * HOUR + 17 * MIN + 41_123,
        T0 + 7 * 24 * HOUR - 1,
        T0 + 7 * 24 * HOUR,
        1_790_812_800_000 - 1,
        1_790_812_800_000,
        1_793_491_200_000 - 1,
        1_793_491_200_000,
        1_835_000_000_123,
        951_782_400_000,
        4_102_444_799_999,
    ];
    for step in [
        BinStep::Minute1,
        BinStep::Minute5,
        BinStep::Minute15,
        BinStep::Hour1,
        BinStep::Hour4,
        BinStep::Day,
        BinStep::Week,
        BinStep::Month,
    ] {
        for ts in stamps {
            let rows = env
                .client
                .query_rows(&vulture_server::query::sql::bin_of_constant(step, ts))
                .await
                .unwrap();
            let got = rows[0]["b"].as_i64().expect("an integer");
            assert_eq!(got, step.start(ts), "{step:?} at {ts}");
        }
    }
    env.cleanup().await;
}

#[tokio::test]
async fn events_round_trip_through_clickhouse() {
    let Some(p) = pair("roundtrip", 0.5, 0, false).await else {
        return;
    };
    // Everything is in ClickHouse: read it through a service with no tail (watermark at the end).
    let (ch, _) = service_with(
        p.ds.stream.store.clone(),
        Some(Arc::new(ChCold::new(p.env.client.clone())) as Arc<dyn ColdStore>),
        Some({
            let w = Arc::new(Watermark::new());
            w.set(&p.ds.events.last().unwrap().id);
            w
        }),
        cfg(),
    );
    p.env.insert(&p.ds.events[p.ds.watermark_at(0.5).0..]).await;
    let got = query(&ch, period()).await.unwrap();
    assert_eq!(got, expected(&p.ds, &period()));
    // Signals keep their enum, details map and market-wide marker; nullable columns stay null.
    assert!(got.iter().any(|e| matches!(&e.payload, Some(vulture_proto::v1::event::Payload::Signal(s)) if s.details.contains_key("windows"))));
    p.env.cleanup().await;
}

#[tokio::test]
async fn query_matches_the_reference_across_the_boundary() {
    for (fraction, overlap, real_redis) in [(0.5, 0, false), (0.5, 30, false), (0.85, 12, true)] {
        let Some(p) = pair("query", fraction, overlap, real_redis).await else {
            return;
        };
        let reqs = [
            period(),
            HistoryRequest {
                exchanges: vec!["bybit".into()],
                ..period()
            },
            HistoryRequest {
                symbols: vec!["ETH-USDT-PERP".into(), "SOL-USDT-PERP".into()],
                ..period()
            },
            HistoryRequest {
                types: vec![EventType::Liquidation as i32, EventType::Signal as i32],
                ..period()
            },
            HistoryRequest {
                types: vec![EventType::Gap as i32],
                from_ts: START + 80 * MIN,
                to_ts: START + 81 * MIN,
                ..period()
            },
            HistoryRequest {
                from_ts: START + 90 * MIN,
                to_ts: START + 91 * MIN,
                ..period()
            },
        ];
        for req in reqs {
            let a = query(&p.ch, req.clone()).await.unwrap();
            let b = query(&p.mem, req.clone()).await.unwrap();
            assert_eq!(a, b, "{req:?}");
            assert_eq!(ids(&a), ids(&expected(&p.ds, &req)), "{req:?}");
        }
        // Pagination by cursor over the boundary reproduces the answer.
        let want = expected(&p.ds, &period());
        let mut got = Vec::new();
        let mut cursor = String::new();
        loop {
            let page = query(
                &p.ch,
                HistoryRequest {
                    limit: 411,
                    cursor: cursor.clone(),
                    ..period()
                },
            )
            .await
            .unwrap();
            let n = page.len();
            if let Some(last) = page.last() {
                cursor = last.id.clone();
            }
            got.extend(page);
            if n < 411 {
                break;
            }
        }
        assert_eq!(ids(&got), ids(&want), "paged, fraction {fraction}");
        p.env.cleanup().await;
    }
}

#[tokio::test]
async fn aggregate_matches_the_reference_across_the_boundary() {
    for (fraction, overlap, real_redis) in [(0.5, 0, false), (0.5, 40, false), (0.9, 7, true)] {
        let Some(p) = pair("aggregate", fraction, overlap, real_redis).await else {
            return;
        };
        let metrics = vulture_server::query::agg::DEFAULT_METRICS.to_vec();
        for step in [
            Step::Minute1,
            Step::Minute5,
            Step::Minute15,
            Step::Hour,
            Step::Hour4,
            Step::Day,
            Step::Week,
            Step::Month,
        ] {
            for group_by in [GroupBy::Symbol, GroupBy::BaseAsset, GroupBy::Exchange] {
                let r = req(group_by, step, &metrics);
                let a = aggregate(&p.ch, r.clone()).await.unwrap();
                let b = aggregate(&p.mem, r).await.unwrap();
                assert_same_rows(
                    &a,
                    &b,
                    &format!("{fraction}/{overlap} {step:?} {group_by:?}"),
                );
                assert!(!a.rows.is_empty());
            }
        }
        // Filters.
        let mut r = req(GroupBy::BaseAsset, Step::Minute5, &metrics);
        r.base_assets = vec!["BTC".into()];
        r.exchanges = vec!["bybit".into(), "binance".into()];
        let a = aggregate(&p.ch, r.clone()).await.unwrap();
        let b = aggregate(&p.mem, r).await.unwrap();
        assert_same_rows(&a, &b, "filters");
        assert!(a.rows.iter().all(|r| r.group == "BTC"));
        p.env.cleanup().await;
    }
}

#[tokio::test]
async fn movers_match_the_reference_across_the_boundary() {
    for (fraction, overlap, real_redis) in [(0.55, 0, false), (0.8, 25, false), (0.9, 0, true)] {
        let Some(p) = pair("movers", fraction, overlap, real_redis).await else {
            return;
        };
        for end in [START + 2 * HOUR + 6 * MIN, END + 60_000, START + 105 * MIN] {
            for window in [60u32, 300, 3600] {
                let a = aggregate(&p.ch, movers_request(end, window))
                    .await
                    .unwrap()
                    .oi_movers
                    .unwrap();
                let b = aggregate(&p.mem, movers_request(end, window))
                    .await
                    .unwrap()
                    .oi_movers
                    .unwrap();
                assert_eq!(a, b, "end {end} window {window} fraction {fraction}");
            }
        }
        let mut q = movers_request(START + 2 * HOUR + 6 * MIN, 300);
        q.oi_movers.as_mut().unwrap().sort_by = OiMoverSort::AbsBase as i32;
        let a = aggregate(&p.ch, q.clone()).await.unwrap();
        let b = aggregate(&p.mem, q).await.unwrap();
        assert_eq!(a.oi_movers, b.oi_movers);
        p.env.cleanup().await;
    }
}

#[tokio::test]
async fn a_dead_clickhouse_falls_back_or_says_unavailable() {
    let Some(env) = ChEnv::new("dead", false).await else {
        return;
    };
    let ds = dataset().await;
    let (h, svc) = service_with(
        ds.stream.store.clone(),
        Some(Arc::new(ChCold::new(env.dead_client())) as Arc<dyn ColdStore>),
        None,
        cfg(),
    );
    // No persisted watermark and no in-process one: the whole stream is the tail, ClickHouse is not needed for
    // the stream part, but the cold query fails, so the Redis fallback answers.
    let got = query(&h, period()).await.unwrap();
    assert_eq!(ids(&got), ids(&expected(&ds, &period())));
    assert!(svc.stats().fallbacks() >= 1);
    let old = HistoryRequest {
        from_ts: 1_000,
        to_ts: 2_000,
        ..Default::default()
    };
    assert_eq!(query(&h, old).await.unwrap_err().code(), Code::Unavailable);
    env.cleanup().await;
}

#[tokio::test]
async fn hostile_filter_values_are_data_not_sql() {
    let Some(p) = pair("inject", 0.5, 0, false).await else {
        return;
    };
    let hostile = "x') OR 1=1; DROP TABLE liquidations; --";
    let got = query(
        &p.ch,
        HistoryRequest {
            symbols: vec![hostile.into()],
            ..period()
        },
    )
    .await
    .unwrap();
    // Only symbol-less events (gaps, market-wide signals) pass a symbol filter, as in the live stream.
    assert!(got.iter().all(|e| !matches!(
        &e.payload,
        Some(
            vulture_proto::v1::event::Payload::Liquidation(_)
                | vulture_proto::v1::event::Payload::OpenInterest(_)
        )
    )));
    assert!(
        p.env.scalar("SELECT count() FROM liquidations").await > 0,
        "the table is intact"
    );
    let r = aggregate(
        &p.ch,
        AggregateRequest {
            symbols: vec!["a\\'b".into()],
            ..req(GroupBy::Symbol, Step::Hour, &[Metric::LiqCount])
        },
    )
    .await
    .unwrap();
    assert!(r.rows.is_empty());
    p.env.cleanup().await;
}

#[tokio::test]
async fn too_many_cells_are_refused_not_truncated() {
    let Some(p) = pair("cells", 0.99, 0, false).await else {
        return;
    };
    let mut c = cfg();
    c.max_fine_rows = 10;
    let wm = Arc::new(Watermark::new());
    wm.set(&p.ds.events.last().unwrap().id);
    let (h, _) = service_with(
        p.ds.stream.store.clone(),
        Some(Arc::new(ChCold::new(p.env.client.clone())) as Arc<dyn ColdStore>),
        Some(wm),
        c,
    );
    let err = aggregate(
        &h,
        req(GroupBy::Symbol, Step::Minute5, &[Metric::OiEndBase]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted, "{err}");
    p.env.cleanup().await;
}
