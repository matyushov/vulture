//! The `Control` service. Only the symbol RPCs exist so far (V1-11); every other RPC answers `UNIMPLEMENTED`
//! until its epic (V4) lands. Everything sits behind the bearer token interceptor.
//!
//! Errors of `SetSymbolPolicy`: `INVALID_ARGUMENT` for an unknown or disabled exchange, an unspecified mode,
//! malformed names, an empty whitelist and names the exchange does not list (a typo must not silently track
//! nothing); `UNIMPLEMENTED` for per-symbol OI interval overrides; `FAILED_PRECONDITION` while the exchange's
//! symbol list is not loaded yet; `INTERNAL` when the state file cannot be written (nothing changed).

use std::sync::Arc;

use tonic::{Request, Response, Status};
use vulture_proto::v1::*;

use crate::symbols::{Mode, PolicyView, SetError, SymbolManager, SymbolPolicy};

const NOT_YET: &str = "this Control RPC is not implemented yet (epic V4)";
const NO_OVERRIDES: &str = "per-symbol OI interval overrides (oi_intervals) are not supported yet; send an empty list. Use the exchange-wide oi_poll_interval_secs of the configuration";

/// `Control` backed by the symbol manager. Without a manager every RPC is `UNIMPLEMENTED`.
#[derive(Clone)]
pub struct ControlService {
    symbols: Option<Arc<SymbolManager>>,
}

impl ControlService {
    /// Service with the given symbol control (or none).
    pub fn new(symbols: Option<Arc<SymbolManager>>) -> Self {
        Self { symbols }
    }

    fn manager(&self) -> Result<&Arc<SymbolManager>, Status> {
        self.symbols
            .as_ref()
            .ok_or_else(|| Status::unimplemented(NOT_YET))
    }
}

fn to_proto_mode(mode: Mode) -> SymbolMode {
    match mode {
        Mode::All => SymbolMode::All,
        Mode::Whitelist => SymbolMode::Whitelist,
        Mode::Blacklist => SymbolMode::Blacklist,
    }
}

fn proto_policy(exchange: &str, policy: &SymbolPolicy) -> vulture_proto::v1::SymbolPolicy {
    vulture_proto::v1::SymbolPolicy {
        exchange: exchange.to_string(),
        mode: to_proto_mode(policy.mode()) as i32,
        symbols: policy.symbols().iter().cloned().collect(),
        oi_intervals: Vec::new(),
    }
}

fn status_of(view: &PolicyView) -> SymbolPolicyStatus {
    SymbolPolicyStatus {
        exchange: view.exchange.clone(),
        tracked_count: view.tracked as u32,
        universe_size: view.universe as u32,
        universe_loaded: view.universe_loaded,
        overridden: view.from_state,
    }
}

fn status_of_error(e: SetError) -> Status {
    match e {
        SetError::UnknownExchange(_)
        | SetError::NotRunning(_)
        | SetError::Policy(_)
        | SetError::UnknownSymbols { .. } => Status::invalid_argument(e.to_string()),
        SetError::UniverseNotLoaded(_) => Status::failed_precondition(e.to_string()),
        // The detail (a path) is in the server log, not in the answer.
        SetError::Persist(_) => Status::internal(
            "cannot store the symbol policy; nothing was changed (see the server log)",
        ),
    }
}

impl ControlService {
    async fn list(
        &self,
        request: Request<ListSymbolPoliciesRequest>,
    ) -> Result<Response<ListSymbolPoliciesResponse>, Status> {
        let manager = self.manager()?;
        let views = manager
            .list(&request.get_ref().exchanges)
            .await
            .map_err(status_of_error)?;
        Ok(Response::new(ListSymbolPoliciesResponse {
            policies: views
                .iter()
                .map(|v| proto_policy(&v.exchange, &v.policy))
                .collect(),
            statuses: views.iter().map(status_of).collect(),
        }))
    }

    async fn set(
        &self,
        request: Request<SetSymbolPolicyRequest>,
    ) -> Result<Response<SetSymbolPolicyResponse>, Status> {
        let manager = self.manager()?;
        let peer = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_default();
        let Some(policy) = request.into_inner().policy else {
            return Err(Status::invalid_argument("policy is required"));
        };
        let mode = match SymbolMode::try_from(policy.mode) {
            Ok(SymbolMode::All) => Mode::All,
            Ok(SymbolMode::Whitelist) => Mode::Whitelist,
            Ok(SymbolMode::Blacklist) => Mode::Blacklist,
            Ok(SymbolMode::Unspecified) | Err(_) => {
                return Err(Status::invalid_argument(
                    "policy.mode is required (ALL, WHITELIST or BLACKLIST)",
                ));
            }
        };
        if !policy.oi_intervals.is_empty() {
            return Err(Status::unimplemented(NO_OVERRIDES));
        }
        let outcome = manager
            .set_policy(&policy.exchange, mode, &policy.symbols, &peer)
            .await
            .map_err(status_of_error)?;
        Ok(Response::new(SetSymbolPolicyResponse {
            policy: Some(proto_policy(&policy.exchange, &outcome.policy)),
            tracked_count: outcome.tracked as u32,
            universe_size: outcome.universe as u32,
            added: outcome.added,
            removed: outcome.removed,
            changed: outcome.changed,
        }))
    }
}

macro_rules! control_impl {
    ($($method:ident: $req:ty => $resp:ty),* $(,)?) => {
        #[tonic::async_trait]
        impl control_server::Control for ControlService {
            async fn list_symbol_policies(
                &self,
                request: Request<ListSymbolPoliciesRequest>,
            ) -> Result<Response<ListSymbolPoliciesResponse>, Status> {
                self.list(request).await
            }

            async fn set_symbol_policy(
                &self,
                request: Request<SetSymbolPolicyRequest>,
            ) -> Result<Response<SetSymbolPolicyResponse>, Status> {
                self.set(request).await
            }

            $(
                async fn $method(&self, _request: Request<$req>) -> Result<Response<$resp>, Status> {
                    Err(Status::unimplemented(NOT_YET))
                }
            )*
        }
    };
}

control_impl! {
    list_subscriptions: ListSubscriptionsRequest => ListSubscriptionsResponse,
    set_subscription: SetSubscriptionRequest => SetSubscriptionResponse,
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
