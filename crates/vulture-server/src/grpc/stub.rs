//! `History` and `Control` are part of the contract but not implemented yet. They are registered so that the
//! contract is visible to `grpcurl` and reflection, and they sit behind the same authentication as `Stream`:
//! a caller without a valid token gets `UNAUTHENTICATED`, one with a valid token gets `UNIMPLEMENTED`.

use std::pin::Pin;

use tonic::{Request, Response, Status};
use vulture_proto::v1::*;

const HISTORY: &str = "History is not implemented yet (epic V3)";
const CONTROL: &str = "Control is not implemented yet (epic V4)";

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

macro_rules! control_stub {
    ($($method:ident: $req:ty => $resp:ty),* $(,)?) => {
        /// `Control` service that answers `UNIMPLEMENTED`.
        #[derive(Debug, Default, Clone, Copy)]
        pub struct ControlStub;

        #[tonic::async_trait]
        impl control_server::Control for ControlStub {
            $(
                async fn $method(&self, _request: Request<$req>) -> Result<Response<$resp>, Status> {
                    Err(Status::unimplemented(CONTROL))
                }
            )*
        }
    };
}

control_stub! {
    list_subscriptions: ListSubscriptionsRequest => ListSubscriptionsResponse,
    set_subscription: SetSubscriptionRequest => SetSubscriptionResponse,
    list_symbol_policies: ListSymbolPoliciesRequest => ListSymbolPoliciesResponse,
    set_symbol_policy: SetSymbolPolicyRequest => SetSymbolPolicyResponse,
    get_connector_status: GetConnectorStatusRequest => GetConnectorStatusResponse,
    set_oi_interval: SetOiIntervalRequest => SetOiIntervalResponse,
    list_rules: ListRulesRequest => ListRulesResponse,
    upsert_rule: UpsertRuleRequest => UpsertRuleResponse,
    delete_rule: DeleteRuleRequest => DeleteRuleResponse,
    list_channels: ListChannelsRequest => ListChannelsResponse,
    upsert_channel: UpsertChannelRequest => UpsertChannelResponse,
    delete_channel: DeleteChannelRequest => DeleteChannelResponse,
    get_storage_status: GetStorageStatusRequest => GetStorageStatusResponse,
    rotate_token: RotateTokenRequest => RotateTokenResponse,
}
