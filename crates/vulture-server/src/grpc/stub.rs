//! `History` is part of the contract but not implemented yet. It is registered so that the contract is visible to
//! `grpcurl` and reflection, and it sits behind the same authentication as `Stream`: a caller without a valid
//! token gets `UNAUTHENTICATED`, one with a valid token gets `UNIMPLEMENTED`.

use std::pin::Pin;

use tonic::{Request, Response, Status};
use vulture_proto::v1::*;

const HISTORY: &str = "History is not implemented yet (epic V3)";

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
