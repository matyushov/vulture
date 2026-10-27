//! `History` over the wire: authentication, status codes, streaming of a large answer with a slow reader,
//! client deadlines and the metrics of the service.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::history_data::*;
use common::history_rig::*;
use common::*;
use tonic::Code;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use vulture_proto::v1::history_client::HistoryClient;
use vulture_proto::v1::{AggregateRequest, GroupBy, HistoryRequest, Metric, Step};
use vulture_server::history::Watermark;
use vulture_server::query::MemoryCold;
use vulture_server::query::cold::ColdStore;

type Client = HistoryClient<InterceptedService<Channel, Auth>>;

async fn client(server: &Server, auth: Auth) -> Client {
    HistoryClient::with_interceptor(server.channel().await, auth)
}

async fn start(
    cfg: vulture_server::config::HistoryApiConfig,
) -> (Server, Dataset, Arc<vulture_server::query::HistoryService>) {
    let ds = dataset().await;
    let cold = Arc::new(MemoryCold::new());
    cold.insert(ds.events.iter().cloned());
    let wm = Arc::new(Watermark::new());
    wm.set(&ds.events.last().unwrap().id);
    let (_, svc) = service_with(
        ds.stream.store.clone(),
        Some(cold as Arc<dyn ColdStore>),
        Some(wm),
        cfg,
    );
    let backend = Backend::memory();
    let server = Server::start_history(&backend, svc.clone()).await;
    (server, ds, svc)
}

fn period() -> HistoryRequest {
    HistoryRequest {
        from_ts: START,
        to_ts: END,
        ..Default::default()
    }
}

#[tokio::test]
async fn history_needs_the_token_and_serves_both_methods() {
    let (server, ds, _) = start(cfg()).await;
    let mut anon = client(&server, Auth::none()).await;
    assert_eq!(
        anon.query(period()).await.unwrap_err().code(),
        Code::Unauthenticated
    );
    assert_eq!(
        anon.aggregate(AggregateRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    let mut c = client(&server, Auth::bearer(&server.token)).await;
    let mut stream = c
        .query(HistoryRequest {
            limit: 100,
            ..period()
        })
        .await
        .unwrap()
        .into_inner();
    let mut n = 0;
    let mut last = String::new();
    while let Some(e) = stream.message().await.unwrap() {
        n += 1;
        last = e.id;
    }
    assert_eq!(n, 100);
    assert!(ds.events.iter().any(|e| e.id == last));
    let resp = c
        .aggregate(AggregateRequest {
            group_by: GroupBy::Exchange as i32,
            step: Step::Hour as i32,
            from_ts: START,
            to_ts: END,
            metrics: vec![Metric::LiqSumUsd as i32, Metric::OiEndUsd as i32],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.rows.len(), 9, "3 exchanges x 3 hours");
    assert_eq!(resp.source, "clickhouse");
    server.stop().await;
}

#[tokio::test]
async fn limits_come_back_as_status_codes_with_hints() {
    let mut c = cfg();
    c.max_query_range_secs = 3600;
    c.max_rows = 50;
    c.max_bins = 10;
    let (server, _, _) = start(c).await;
    let mut cl = client(&server, Auth::bearer(&server.token)).await;
    // The status of a refused request arrives with the call itself or as the first item of the stream.
    let err = match cl.query(period()).await {
        Ok(s) => s.into_inner().message().await.unwrap_err(),
        Err(e) => e,
    };
    assert_eq!(err.code(), Code::OutOfRange);
    let e = match cl
        .query(HistoryRequest {
            limit: 51,
            to_ts: START + HOUR,
            ..period()
        })
        .await
    {
        Ok(s) => s.into_inner().message().await.unwrap_err(),
        Err(e) => e,
    };
    assert_eq!(e.code(), Code::ResourceExhausted);
    let e = cl
        .aggregate(AggregateRequest {
            group_by: GroupBy::Symbol as i32,
            step: Step::Minute1 as i32,
            from_ts: START,
            to_ts: END,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::OutOfRange);
    assert!(e.message().contains("step"), "{e}");
    server.stop().await;
}

#[tokio::test]
async fn a_slow_reader_gets_everything_without_the_server_buffering_it_all() {
    let (server, ds, svc) = start(cfg()).await;
    let mut cl = client(&server, Auth::bearer(&server.token)).await;
    let mut stream = cl.query(period()).await.unwrap().into_inner();
    let mut n = 0usize;
    while let Some(e) = stream.message().await.unwrap() {
        n += 1;
        if n.is_multiple_of(500) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!e.id.is_empty());
    }
    assert_eq!(n, ds.events.len());
    assert_eq!(svc.stats().events_total(), n as u64);
    server.stop().await;
}

#[tokio::test]
async fn a_client_deadline_stops_the_work() {
    let (server, _, svc) = start(cfg()).await;
    let mut cl = client(&server, Auth::bearer(&server.token)).await;
    // Read one event and walk away: the producer notices and the call is released.
    let mut stream = cl.query(period()).await.unwrap().into_inner();
    let _ = stream.message().await.unwrap();
    drop(stream);
    for _ in 0..100 {
        if svc
            .stats()
            .count(vulture_server::query::stats::Method::Query, Code::Cancelled)
            == 1
        {
            server.stop().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the abandoned query was not recorded as cancelled");
}

/// A cold store that answers after a pause.
struct SlowCold(MemoryCold, Duration);

impl ColdStore for SlowCold {
    fn top_liquidations(
        &self,
        q: vulture_server::query::cold::TopLiqQuery,
    ) -> futures_util::future::BoxFuture<
        '_,
        Result<Vec<vulture_proto::v1::Event>, vulture_server::history::ChError>,
    > {
        self.0.top_liquidations(q)
    }

    fn events(
        &self,
        q: vulture_server::query::cold::EventQuery,
    ) -> futures_util::future::BoxFuture<
        '_,
        Result<Box<dyn vulture_server::query::cold::EventCursor>, vulture_server::history::ChError>,
    > {
        Box::pin(async move {
            tokio::time::sleep(self.1).await;
            self.0.events(q).await
        })
    }
    fn event_key(
        &self,
        id: String,
        q: vulture_server::query::cold::EventQuery,
    ) -> futures_util::future::BoxFuture<
        '_,
        Result<Option<vulture_server::query::cold::EventKey>, vulture_server::history::ChError>,
    > {
        self.0.event_key(id, q)
    }
    fn fine(
        &self,
        q: vulture_server::query::cold::FineQuery,
    ) -> futures_util::future::BoxFuture<
        '_,
        Result<vulture_server::query::cold::FineAnswer, vulture_server::history::ChError>,
    > {
        Box::pin(async move {
            tokio::time::sleep(self.1).await;
            self.0.fine(q).await
        })
    }
    fn gaps(
        &self,
        q: vulture_server::query::cold::GapQuery,
    ) -> futures_util::future::BoxFuture<
        '_,
        Result<Vec<vulture_server::query::agg::GapRec>, vulture_server::history::ChError>,
    > {
        self.0.gaps(q)
    }
    fn movers(
        &self,
        q: vulture_server::query::cold::MoverQuery,
    ) -> futures_util::future::BoxFuture<
        '_,
        Result<vulture_server::query::cold::MoverAnswer, vulture_server::history::ChError>,
    > {
        self.0.movers(q)
    }
}

#[tokio::test]
async fn the_server_side_timeout_ends_a_slow_query_and_a_slow_aggregate() {
    let ds = dataset().await;
    let slow = Arc::new(SlowCold(MemoryCold::new(), Duration::from_secs(5)));
    slow.0.insert(ds.events.iter().cloned());
    let wm = Arc::new(Watermark::new());
    wm.set(&ds.events.last().unwrap().id);
    let mut c = cfg();
    c.request_timeout_secs = 1;
    let (_, svc) = service_with(
        ds.stream.store.clone(),
        Some(slow as Arc<dyn ColdStore>),
        Some(wm),
        c,
    );
    let backend = Backend::memory();
    let server = Server::start_history(&backend, svc.clone()).await;
    let mut cl = client(&server, Auth::bearer(&server.token)).await;
    let mut stream = cl.query(period()).await.unwrap().into_inner();
    let e = stream.message().await.unwrap_err();
    assert_eq!(e.code(), Code::DeadlineExceeded, "{e}");
    assert!(e.message().contains("request_timeout_secs"), "{e}");
    let e = cl
        .aggregate(AggregateRequest {
            group_by: GroupBy::Exchange as i32,
            step: Step::Hour as i32,
            from_ts: START,
            to_ts: END,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::DeadlineExceeded, "{e}");
    use vulture_server::query::stats::Method;
    assert_eq!(svc.stats().count(Method::Query, Code::DeadlineExceeded), 1);
    assert_eq!(
        svc.stats().count(Method::Aggregate, Code::DeadlineExceeded),
        1
    );
    server.stop().await;
}
