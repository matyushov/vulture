//! Shared helpers of the `History` tests: building a service over a data set, calling it, comparing answers.
#![allow(dead_code)]

use std::sync::Arc;

use futures_util::StreamExt;
use tonic::{Request, Status};
use vulture_proto::v1::history_server::History;
use vulture_proto::v1::{
    AggregateRequest, AggregateResponse, Event, GroupBy, HistoryRequest, Metric, MetricValue,
    OiMoverSort, OiMoversQuery, Step,
};
use vulture_server::config::HistoryApiConfig;
use vulture_server::history::Watermark;
use vulture_server::query::cold::ColdStore;
use vulture_server::query::event::{event_key, in_period};
use vulture_server::query::{HistoryHandle, HistoryService, MemoryCold, SeriesParams};
use vulture_server::store::RedisStore;

use super::history_data::*;

pub struct Rig {
    pub handle: HistoryHandle,
    pub service: Arc<HistoryService>,
    pub cold: Option<Arc<MemoryCold>>,
}

/// A service over `ds`. `split = Some((fraction, overlap))`: ClickHouse holds the first `fraction` of the events plus
/// `overlap` more (flushed but not yet acknowledged by the watermark), Redis holds everything. `None`: ClickHouse
/// holds everything and the watermark is at the end. `cold = false`: no ClickHouse at all.
pub async fn rig(
    ds: &Dataset,
    split: Option<(f64, usize)>,
    cold: bool,
    cfg: HistoryApiConfig,
) -> Rig {
    let clock = SetClock::new(NOW);
    let (cold_store, watermark) = if cold {
        let store = Arc::new(MemoryCold::new());
        let wm = Arc::new(Watermark::new());
        match split {
            Some((fraction, overlap)) => {
                let (i, id) = ds.watermark_at(fraction);
                store.insert(
                    ds.events[..(i + overlap).min(ds.events.len())]
                        .iter()
                        .cloned(),
                );
                wm.set(&id);
            }
            None => {
                store.insert(ds.events.iter().cloned());
                wm.set(&ds.events.last().unwrap().id);
            }
        }
        (Some(store), Some(wm))
    } else {
        (None, None)
    };
    let service = HistoryService::new(
        ds.stream.store.clone(),
        cold_store.clone().map(|c| c as Arc<dyn ColdStore>),
        watermark,
        cfg,
        SeriesParams::default(),
        Arc::new(clock),
    );
    Rig {
        handle: HistoryHandle(service.clone()),
        service,
        cold: cold_store,
    }
}

pub async fn query(h: &HistoryHandle, req: HistoryRequest) -> Result<Vec<Event>, Status> {
    let mut stream = h.query(Request::new(req)).await?.into_inner();
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

pub async fn aggregate(
    h: &HistoryHandle,
    req: AggregateRequest,
) -> Result<AggregateResponse, Status> {
    h.aggregate(Request::new(req)).await.map(|r| r.into_inner())
}

pub fn period() -> HistoryRequest {
    HistoryRequest {
        from_ts: START,
        to_ts: END,
        ..HistoryRequest::default()
    }
}

/// The events of a request, brute force: filter, order by (time, id).
pub fn expected(ds: &Dataset, req: &HistoryRequest) -> Vec<Event> {
    use vulture_server::query::cold::Scope;
    use vulture_server::query::event::{EventFilter, kinds_from_types};
    let kinds = kinds_from_types(&req.types).unwrap();
    let scope = Scope {
        exchanges: req.exchanges.clone(),
        symbols: req.symbols.clone(),
        base_assets: vec![],
    };
    let filter = EventFilter::new(&kinds, &scope);
    let mut v: Vec<_> = ds
        .events
        .iter()
        .filter(|e| filter.matches_event(e) && in_period(e, req.from_ts, req.to_ts))
        .cloned()
        .collect();
    v.sort_by_key(|e| event_key(e).unwrap());
    v
}

pub fn ids(v: &[Event]) -> Vec<String> {
    v.iter().map(|e| e.id.clone()).collect()
}

pub fn req(group_by: GroupBy, step: Step, metrics: &[Metric]) -> AggregateRequest {
    AggregateRequest {
        group_by: group_by as i32,
        step: step as i32,
        from_ts: START,
        to_ts: END,
        metrics: metrics.iter().map(|m| *m as i32).collect(),
        ..AggregateRequest::default()
    }
}

pub fn val(values: &[MetricValue], m: Metric) -> Option<f64> {
    values.iter().find(|v| v.metric == m as i32).unwrap().value
}

pub fn close(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0),
        _ => false,
    }
}

pub fn assert_same_rows(a: &AggregateResponse, b: &AggregateResponse, what: &str) {
    assert_eq!(a.rows.len(), b.rows.len(), "{what}: row count");
    assert_eq!(a.partial, b.partial, "{what}");
    for (x, y) in a.rows.iter().zip(&b.rows) {
        assert_eq!(
            (x.bucket_start, x.bucket_end, &x.group, x.partial),
            (y.bucket_start, y.bucket_end, &y.group, y.partial),
            "{what}"
        );
        for (vx, vy) in x.values.iter().zip(&y.values) {
            assert_eq!(vx.metric, vy.metric);
            assert!(
                close(vx.value, vy.value),
                "{what}: {} {} metric {}: {:?} vs {:?}",
                x.group,
                x.bucket_start,
                vx.metric,
                vx.value,
                vy.value
            );
        }
    }
}

pub fn movers_request(end: i64, window_secs: u32) -> AggregateRequest {
    AggregateRequest {
        to_ts: end,
        oi_movers: Some(OiMoversQuery {
            window_secs,
            limit: 100,
            sort_by: OiMoverSort::Pct as i32,
            ..OiMoversQuery::default()
        }),
        ..AggregateRequest::default()
    }
}

/// A service over any store/cold/watermark combination.
pub fn service_with(
    store: Arc<dyn RedisStore>,
    cold: Option<Arc<dyn ColdStore>>,
    watermark: Option<Arc<Watermark>>,
    cfg: HistoryApiConfig,
) -> (HistoryHandle, Arc<HistoryService>) {
    let service = HistoryService::new(
        store,
        cold,
        watermark,
        cfg,
        SeriesParams::default(),
        Arc::new(SetClock::new(NOW)),
    );
    (HistoryHandle(service.clone()), service)
}

pub fn cfg() -> HistoryApiConfig {
    HistoryApiConfig::default()
}
