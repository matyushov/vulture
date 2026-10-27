//! The analytics metrics of `History.Aggregate` (regime, fragility, divergence) and `History.Seasonality`.

mod common;

use std::sync::Arc;

use common::history_data::*;
use common::history_rig::*;
use tonic::{Code, Request};
use vulture_proto::v1::history_server::History;
use vulture_proto::v1::{
    Event, GroupBy, Liquidation, Metric, OpenInterest, Regime, SeasonalityRequest, Side, Step,
    event::Payload,
};
use vulture_server::history::Watermark;
use vulture_server::query::MemoryCold;
use vulture_server::query::cold::ColdStore;

fn analytics_metrics() -> Vec<Metric> {
    vec![
        Metric::PriceChangePct,
        Metric::OiChangePct,
        Metric::Regime,
        Metric::RegimeStrength,
        Metric::OiPriceFlatness,
        Metric::OiDivergencePct,
        Metric::LiqToOiRatio,
    ]
}

#[tokio::test]
async fn analytics_metrics_agree_across_the_boundary_and_redis_alone() {
    let ds = dataset().await;
    let whole = rig(&ds, None, true, cfg()).await;
    let redis = rig(&ds, None, false, cfg()).await;
    for (fraction, overlap) in [(0.5, 0), (0.45, 30), (0.95, 3)] {
        let split = rig(&ds, Some((fraction, overlap)), true, cfg()).await;
        for group_by in [GroupBy::Symbol, GroupBy::BaseAsset, GroupBy::Exchange] {
            for step in [Step::Minute5, Step::Hour] {
                let r = req(group_by, step, &analytics_metrics());
                let a = aggregate(&whole.handle, r.clone()).await.unwrap();
                let b = aggregate(&split.handle, r.clone()).await.unwrap();
                let c = aggregate(&redis.handle, r).await.unwrap();
                assert_same_rows(
                    &a,
                    &b,
                    &format!("split {fraction}/{overlap} {group_by:?} {step:?}"),
                );
                assert_same_rows(&a, &c, &format!("redis {group_by:?} {step:?}"));
            }
        }
    }
}

#[tokio::test]
async fn the_spike_of_binance_btc_reads_as_new_longs_and_a_divergence() {
    let ds = dataset().await;
    let r = rig(&ds, None, true, cfg()).await;
    // BTC on binance and bybit; only binance spikes (+6% of OI) in the hour starting at START + 2 h.
    let mut q = req(
        GroupBy::BaseAsset,
        Step::Hour,
        &[Metric::OiDivergencePct, Metric::OiChangePct],
    );
    q.base_assets = vec!["BTC".into()];
    q.exchanges = vec!["binance".into(), "bybit".into()];
    let resp = aggregate(&r.handle, q).await.unwrap();
    let spike = resp
        .rows
        .iter()
        .find(|row| row.bucket_start == START + 2 * HOUR)
        .unwrap();
    let d = spike.divergence.as_ref().expect("two exchanges list BTC");
    assert_eq!(d.rising_exchange, "binance");
    assert!(d.rising_pct > 5.0, "{d:?}");
    assert!(d.spread_pct > d.rising_pct - d.falling_pct - 1e-9);
    assert!(d.spread_pct > 4.0);
    // The same hour for the binance symbol alone: OI grew by about 6%, the regime is "new longs" or "new shorts"
    // depending on the price, never "unwinding".
    let mut q = req(
        GroupBy::Symbol,
        Step::Hour,
        &[Metric::OiChangePct, Metric::Regime],
    );
    q.symbols = vec!["BTC-USDT-PERP".into()];
    q.exchanges = vec!["binance".into()];
    let resp = aggregate(&r.handle, q).await.unwrap();
    let row = resp
        .rows
        .iter()
        .find(|row| row.bucket_start == START + 2 * HOUR)
        .unwrap();
    assert!(val(&row.values, Metric::OiChangePct).unwrap() > 5.0);
    let regime = val(&row.values, Metric::Regime).unwrap() as i32;
    assert!(
        regime == Regime::NewLongs as i32 || regime == Regime::NewShorts as i32,
        "{regime}"
    );
}

/// Nine days of hourly data for BTC on two exchanges: liquidations are large at 14:00 UTC and OI jumps then.
async fn nine_days() -> (Dataset, i64) {
    let monday = T0 - 7 * 24 * HOUR;
    let stream = Stream::new();
    let mut events: Vec<Event> = Vec::new();
    let mut level = [1000.0_f64, 2000.0];
    for day in 0..9 {
        for hour in 0..24i64 {
            let t = monday + (day * 24 + hour) * HOUR;
            for (i, ex) in ["binance", "bybit"].iter().enumerate() {
                if hour == 14 {
                    level[i] *= 1.02;
                }
                let ts = t + 30 * MIN;
                let e = Event {
                    id: String::new(),
                    payload: Some(Payload::OpenInterest(OpenInterest {
                        exchange: (*ex).into(),
                        symbol_raw: "BTCUSDT".into(),
                        symbol: "BTC-USDT-PERP".into(),
                        exchange_ts: ts,
                        recv_ts: ts + 40,
                        oi_base: level[i],
                        oi_usd: Some(level[i] * 100.0),
                        mark_price: Some(100.0),
                        ..OpenInterest::default()
                    })),
                };
                events.push(stream.append(&e, ts + 40).await);
            }
            let ts = t + 10 * MIN;
            let usd = if hour == 14 { 1_000_000.0 } else { 10_000.0 };
            let e = Event {
                id: String::new(),
                payload: Some(Payload::Liquidation(Liquidation {
                    exchange: "binance".into(),
                    symbol_raw: "BTCUSDT".into(),
                    symbol: "BTC-USDT-PERP".into(),
                    exchange_ts: ts,
                    recv_ts: ts + 30,
                    side: Side::Long as i32,
                    price: 100.0,
                    qty: usd / 100.0,
                    notional_usd: usd,
                })),
            };
            // Streams must be appended in time order; the OI of this hour is later than its liquidation, so
            // append it after: re-sort below.
            events.push(stream.append(&e, ts + 30).await);
        }
    }
    (Dataset { stream, events }, monday + 9 * 24 * HOUR)
}

#[tokio::test]
async fn seasonality_profiles_hour_of_day_and_day_of_week() {
    // The hand-built stream is appended liquidation after OI within an hour; ids follow append order, which is all
    // the memory stores need here.
    let (ds, end) = nine_days().await;
    let cold = Arc::new(MemoryCold::new());
    cold.insert(ds.events.iter().cloned());
    let wm = Arc::new(Watermark::new());
    wm.set(&ds.events.last().unwrap().id);
    let (h, svc) = service_with(
        ds.stream.store.clone(),
        Some(cold as Arc<dyn ColdStore>),
        Some(wm),
        cfg(),
    );
    let call = |days: u32| {
        let h = h.clone();
        async move {
            h.seasonality(Request::new(SeasonalityRequest {
                group_by: GroupBy::BaseAsset as i32,
                base_assets: vec!["BTC".into()],
                lookback_days: days,
                to_ts: end,
                ..Default::default()
            }))
            .await
            .map(|r| r.into_inner())
        }
    };
    let resp = call(7).await.unwrap();
    assert_eq!((resp.lookback_days, resp.days_covered), (7, 7));
    assert!(!resp.partial, "seven full days cover every weekday");
    assert_eq!(resp.profiles.len(), 1);
    let p = &resp.profiles[0];
    assert_eq!(p.group, "BTC");
    assert_eq!((p.by_hour.len(), p.by_weekday.len()), (24, 7));
    // 7 days: every hour has 7 observations; 14:00 stands out.
    assert!(p.by_hour.iter().all(|b| b.samples == 7));
    assert_eq!(p.by_hour[14].mean_liq_usd, Some(1_000_000.0));
    assert_eq!(p.by_hour[3].mean_liq_usd, Some(10_000.0));
    // OI jumps 2% at 14:00 on both exchanges, nowhere else (the first hour of the lookback has no previous bin).
    let oi14 = p.by_hour[14].mean_oi_change_pct.unwrap();
    assert!((oi14 - 2.0).abs() < 1e-9, "{oi14}");
    assert!(p.by_hour[15].mean_oi_change_pct.unwrap().abs() < 1e-9);
    assert!((p.by_hour[14].mean_abs_oi_change_pct.unwrap() - 2.0).abs() < 1e-9);
    // Weekday buckets: 24 hourly bins each (the lookback is exactly 7 days).
    assert!(p.by_weekday.iter().all(|b| b.samples == 24));
    let all_liq: f64 = p
        .by_weekday
        .iter()
        .map(|b| b.mean_liq_usd.unwrap())
        .sum::<f64>()
        / 7.0;
    assert!((all_liq - (23.0 * 10_000.0 + 1_000_000.0) / 24.0).abs() < 1e-6);
    // Asking for more days than the data has: partial, with the days covered.
    let resp = call(14).await.unwrap();
    assert_eq!((resp.lookback_days, resp.days_covered), (14, 9));
    assert!(resp.partial);
    // Less than a week of data: some weekdays are empty.
    let few = h
        .seasonality(Request::new(SeasonalityRequest {
            group_by: GroupBy::BaseAsset as i32,
            lookback_days: 3,
            to_ts: end,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(few.days_covered, 3);
    assert!(few.partial, "three days cannot cover seven weekdays");
    assert!(
        few.profiles[0]
            .by_weekday
            .iter()
            .any(|b| b.samples == 0 && b.mean_liq_usd.is_none())
    );
    // Validation.
    for bad in [
        SeasonalityRequest {
            group_by: 0,
            ..Default::default()
        },
        SeasonalityRequest {
            group_by: GroupBy::Symbol as i32,
            to_ts: -1,
            ..Default::default()
        },
    ] {
        assert_eq!(
            h.seasonality(Request::new(bad)).await.unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    let too_long = SeasonalityRequest {
        group_by: GroupBy::Symbol as i32,
        lookback_days: 181,
        ..Default::default()
    };
    assert_eq!(
        h.seasonality(Request::new(too_long))
            .await
            .unwrap_err()
            .code(),
        Code::OutOfRange
    );
    use vulture_server::query::stats::Method;
    assert_eq!(svc.stats().count(Method::Seasonality, Code::Ok), 3);
}

#[tokio::test]
async fn seasonality_over_the_wire_needs_the_token() {
    use common::{Auth, Backend, Server};
    use vulture_proto::v1::history_client::HistoryClient;
    let ds = dataset().await;
    let r = rig(&ds, None, true, cfg()).await;
    let backend = Backend::memory();
    let server = Server::start_history(&backend, r.service.clone()).await;
    let req = SeasonalityRequest {
        group_by: GroupBy::Exchange as i32,
        lookback_days: 1,
        to_ts: END + HOUR,
        ..Default::default()
    };
    let mut anon = HistoryClient::with_interceptor(server.channel().await, Auth::none());
    assert_eq!(
        anon.seasonality(req.clone()).await.unwrap_err().code(),
        Code::Unauthenticated
    );
    let mut c =
        HistoryClient::with_interceptor(server.channel().await, Auth::bearer(&server.token));
    let resp = c.seasonality(req).await.unwrap().into_inner();
    assert_eq!(resp.profiles.len(), 3, "three exchanges");
    server.stop().await;
}
