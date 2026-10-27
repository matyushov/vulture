//! `History` without a backend. The service is always registered so that the contract is visible to `grpcurl`
//! and it sits behind the same authentication as `Stream`: a caller without a valid token gets `UNAUTHENTICATED`.
//! In `vulture-server run` the backend is [`crate::query::HistoryService`]; where none is given (some tests) a
//! caller with a valid token gets `UNIMPLEMENTED`.

use std::pin::Pin;

use tonic::{Request, Response, Status};
use vulture_proto::v1::*;

const HISTORY: &str = "History has no backend in this server";

/// `History` service that answers `UNIMPLEMENTED`.
#[derive(Debug, Default, Clone, Copy)]
pub struct HistoryStub;

#[tonic::async_trait]
impl history_server::History for HistoryStub {
    type QueryStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<Event, Status>> + Send>>;

    async fn query(
        &self,
        _request: Request<HistoryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        Err(Status::unimplemented(HISTORY))
    }

    async fn aggregate(
        &self,
        _request: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        Err(Status::unimplemented(HISTORY))
    }
}

/// The `History` service as `serve` registers it: the live backend, or the stub without one.
#[derive(Clone)]
pub enum HistoryBackend {
    /// No backend: every call is `UNIMPLEMENTED`.
    Stub(HistoryStub),
    /// The real service.
    Live(crate::query::HistoryHandle),
}

#[tonic::async_trait]
impl history_server::History for HistoryBackend {
    type QueryStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<Event, Status>> + Send>>;

    async fn query(
        &self,
        request: Request<HistoryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        match self {
            Self::Stub(s) => s.query(request).await,
            Self::Live(l) => l.query(request).await,
        }
    }

    async fn aggregate(
        &self,
        request: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        match self {
            Self::Stub(s) => s.aggregate(request).await,
            Self::Live(l) => l.aggregate(request).await,
        }
    }
}
