//! The `Control` service. The symbol RPCs (V1-11) and the rule and channel RPCs (V4a: `ListRules`, `UpsertRule`,
//! `DeleteRule`, `ListChannels`, `UpsertChannel`, `DeleteChannel`, `SendTestMessage`) exist; every other RPC
//! answers `UNIMPLEMENTED` until its epic (V4) lands. Everything sits behind the bearer token interceptor.
//!
//! Errors of the rule and channel RPCs: `INVALID_ARGUMENT` for a malformed or unacceptable rule or channel
//! (unknown channel in a rule, bad secret shape, missing kind), `FAILED_PRECONDITION` for deleting a channel
//! that rules still use, `NOT_FOUND` for `SendTestMessage` on an unknown channel, `UNIMPLEMENTED` for gap rules
//! (system alerts, V4-5), `INTERNAL` when `alerts.json` cannot be written (nothing changed). Deleting something
//! that does not exist succeeds. Responses never contain a secret, only a mask.
//!
//! Errors of `SetSymbolPolicy`: `INVALID_ARGUMENT` for an unknown or disabled exchange, an unspecified mode,
//! malformed names, an empty whitelist and names the exchange does not list (a typo must not silently track
//! nothing); `UNIMPLEMENTED` for per-symbol OI interval overrides; `FAILED_PRECONDITION` while the exchange's
//! symbol list is not loaded yet; `INTERNAL` when the state file cannot be written (nothing changed).

use std::sync::Arc;

use tonic::{Request, Response, Status};
use vulture_proto::v1::*;

use crate::alerts::model::{
    ChannelDef, ChannelKind, LiqSide, RuleDef, RuleKind, Severity, SignalKind,
};
use crate::alerts::{AlertsError, AlertsManager};
use crate::symbols::{Mode, PolicyView, SetError, SymbolManager, SymbolPolicy};

const NOT_YET: &str = "this Control RPC is not implemented yet (epic V4)";
const NO_OVERRIDES: &str = "per-symbol OI interval overrides (oi_intervals) are not supported yet; send an empty list. Use the exchange-wide oi_poll_interval_secs of the configuration";

/// `Control` backed by the symbol manager. Without a manager every RPC is `UNIMPLEMENTED`.
#[derive(Clone)]
pub struct ControlService {
    symbols: Option<Arc<SymbolManager>>,
    alerts: Option<Arc<AlertsManager>>,
}

impl ControlService {
    /// Service with the given symbol control (or none).
    pub fn new(symbols: Option<Arc<SymbolManager>>) -> Self {
        Self {
            symbols,
            alerts: None,
        }
    }

    /// Adds the rules and channels backend.
    pub fn with_alerts(mut self, alerts: Option<Arc<AlertsManager>>) -> Self {
        self.alerts = alerts;
        self
    }

    fn alerts(&self) -> Result<&Arc<AlertsManager>, Status> {
        self.alerts
            .as_ref()
            .ok_or_else(|| Status::unimplemented(NOT_YET))
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

fn status_of_alerts(e: AlertsError) -> Status {
    match e {
        AlertsError::Invalid(m) => Status::invalid_argument(m),
        AlertsError::Conflict(m) => Status::failed_precondition(m),
        AlertsError::NotFound(m) => Status::not_found(m),
        // The detail (a path) is in the server log, not in the answer.
        AlertsError::Persist(ref inner) => {
            tracing::error!(error = %inner, "cannot store the alerts file");
            Status::internal("cannot store the change; nothing was changed (see the server log)")
        }
    }
}

fn peer_of<T>(request: &Request<T>) -> String {
    request
        .remote_addr()
        .map(|a| a.to_string())
        .unwrap_or_default()
}

fn rule_from_proto(rule: Rule) -> Result<RuleDef, Status> {
    let kind = match rule.kind {
        Some(rule::Kind::Liquidation(l)) => {
            let mut sides = Vec::new();
            for s in l.sides {
                sides.push(match Side::try_from(s) {
                    Ok(Side::Long) => LiqSide::Long,
                    Ok(Side::Short) => LiqSide::Short,
                    _ => {
                        return Err(Status::invalid_argument(
                            "liquidation.sides may contain LONG and SHORT only",
                        ));
                    }
                });
            }
            RuleKind::Liquidation {
                min_notional_usd: l.min_notional_usd,
                sides,
            }
        }
        Some(rule::Kind::OiChange(o)) => RuleKind::OiChange {
            window_secs: o.window_seconds,
            min_change_pct: o.min_change_pct,
            min_change_usd: o.min_change_usd,
            detector_override: o.detector_override,
        },
        Some(rule::Kind::Signal(s)) => {
            let mut types = Vec::new();
            for t in s.types {
                types.push(match SignalType::try_from(t) {
                    Ok(SignalType::OiSpikeUp) => SignalKind::OiSpikeUp,
                    Ok(SignalType::OiSpikeDown) => SignalKind::OiSpikeDown,
                    Ok(SignalType::LiqCascade) => SignalKind::LiqCascade,
                    _ => {
                        return Err(Status::invalid_argument(
                            "signal.types: unknown signal type",
                        ));
                    }
                });
            }
            let min_severity = match vulture_proto::v1::Severity::try_from(s.min_severity) {
                Ok(
                    vulture_proto::v1::Severity::Unspecified | vulture_proto::v1::Severity::Info,
                ) => Severity::Info,
                Ok(vulture_proto::v1::Severity::Warning) => Severity::Warning,
                Ok(vulture_proto::v1::Severity::Critical) => Severity::Critical,
                Err(_) => return Err(Status::invalid_argument("signal.min_severity is unknown")),
            };
            RuleKind::Signal {
                types,
                min_severity,
            }
        }
        Some(rule::Kind::Gap(_)) => {
            return Err(Status::unimplemented(
                "gap rules belong to the system alerts (epic V4-5) and are not implemented yet",
            ));
        }
        None => return Err(Status::invalid_argument("rule.kind is required")),
    };
    Ok(RuleDef {
        id: rule.id,
        name: rule.name,
        enabled: rule.enabled,
        kind,
        exchanges: rule.exchanges,
        symbols: rule.symbols,
        channels: rule.channel_ids,
        cooldown_secs: rule.cooldown_seconds,
    })
}

fn rule_to_proto(rule: &RuleDef) -> Rule {
    let kind = match &rule.kind {
        RuleKind::Liquidation {
            min_notional_usd,
            sides,
        } => rule::Kind::Liquidation(LiquidationRule {
            min_notional_usd: *min_notional_usd,
            sides: sides
                .iter()
                .map(|s| match s {
                    LiqSide::Long => Side::Long as i32,
                    LiqSide::Short => Side::Short as i32,
                })
                .collect(),
        }),
        RuleKind::OiChange {
            window_secs,
            min_change_pct,
            min_change_usd,
            detector_override,
        } => rule::Kind::OiChange(OiChangeRule {
            min_change_pct: *min_change_pct,
            window_seconds: *window_secs,
            min_change_usd: *min_change_usd,
            detector_override: *detector_override,
        }),
        RuleKind::Signal {
            types,
            min_severity,
        } => rule::Kind::Signal(SignalRule {
            types: types
                .iter()
                .map(|t| match t {
                    SignalKind::OiSpikeUp => SignalType::OiSpikeUp as i32,
                    SignalKind::OiSpikeDown => SignalType::OiSpikeDown as i32,
                    SignalKind::LiqCascade => SignalType::LiqCascade as i32,
                })
                .collect(),
            min_severity: match min_severity {
                Severity::Info => vulture_proto::v1::Severity::Info,
                Severity::Warning => vulture_proto::v1::Severity::Warning,
                Severity::Critical => vulture_proto::v1::Severity::Critical,
            } as i32,
        }),
    };
    Rule {
        id: rule.id.clone(),
        name: rule.name.clone(),
        enabled: rule.enabled,
        kind: Some(kind),
        exchanges: rule.exchanges.clone(),
        symbols: rule.symbols.clone(),
        channel_ids: rule.channels.clone(),
        cooldown_seconds: rule.cooldown_secs,
    }
}

fn channel_from_proto(c: vulture_proto::v1::Channel) -> Result<ChannelDef, Status> {
    let kind = match vulture_proto::v1::ChannelKind::try_from(c.kind) {
        Ok(vulture_proto::v1::ChannelKind::Telegram) => ChannelKind::Telegram,
        Ok(vulture_proto::v1::ChannelKind::Discord) => ChannelKind::Discord,
        _ => {
            return Err(Status::invalid_argument(
                "channel.kind is required (TELEGRAM or DISCORD)",
            ));
        }
    };
    Ok(ChannelDef {
        id: c.id,
        name: c.name,
        kind,
        enabled: c.enabled,
        system: c.system,
        dry_run: c.dry_run,
        chat_id: c.chat_id,
        secret: None,
        secret_file: c.secret_file,
        secret_env: c.secret_env,
        min_interval_ms: c.min_interval_ms,
        batch_window_ms: c.batch_window_ms,
        queue_limit: c.queue_limit,
    })
}

fn channel_to_proto(c: &ChannelDef, mask: String) -> vulture_proto::v1::Channel {
    vulture_proto::v1::Channel {
        id: c.id.clone(),
        name: c.name.clone(),
        kind: match c.kind {
            ChannelKind::Telegram => vulture_proto::v1::ChannelKind::Telegram,
            ChannelKind::Discord => vulture_proto::v1::ChannelKind::Discord,
        } as i32,
        enabled: c.enabled,
        system: c.system,
        secret_mask: mask,
        chat_id: c.chat_id.clone(),
        dry_run: c.dry_run,
        secret_file: c.secret_file.clone(),
        secret_env: c.secret_env.clone(),
        min_interval_ms: c.min_interval_ms,
        batch_window_ms: c.batch_window_ms,
        queue_limit: c.queue_limit,
    }
}

impl ControlService {
    async fn rules_list(&self) -> Result<Response<ListRulesResponse>, Status> {
        let rules = self.alerts()?.list_rules().await;
        Ok(Response::new(ListRulesResponse {
            rules: rules.iter().map(rule_to_proto).collect(),
        }))
    }

    async fn rule_upsert(
        &self,
        request: Request<UpsertRuleRequest>,
    ) -> Result<Response<UpsertRuleResponse>, Status> {
        let alerts = self.alerts()?;
        let peer = peer_of(&request);
        let Some(rule) = request.into_inner().rule else {
            return Err(Status::invalid_argument("rule is required"));
        };
        let stored = alerts
            .upsert_rule(rule_from_proto(rule)?, &peer)
            .await
            .map_err(status_of_alerts)?;
        Ok(Response::new(UpsertRuleResponse {
            rule: Some(rule_to_proto(&stored)),
        }))
    }

    async fn rule_delete(
        &self,
        request: Request<DeleteRuleRequest>,
    ) -> Result<Response<DeleteRuleResponse>, Status> {
        let alerts = self.alerts()?;
        let peer = peer_of(&request);
        alerts
            .delete_rule(&request.into_inner().id, &peer)
            .await
            .map_err(status_of_alerts)?;
        Ok(Response::new(DeleteRuleResponse {}))
    }

    async fn channels_list(&self) -> Result<Response<ListChannelsResponse>, Status> {
        let channels = self.alerts()?.list_channels().await;
        Ok(Response::new(ListChannelsResponse {
            channels: channels
                .into_iter()
                .map(|v| channel_to_proto(&v.def, v.mask))
                .collect(),
        }))
    }

    async fn channel_upsert(
        &self,
        request: Request<UpsertChannelRequest>,
    ) -> Result<Response<UpsertChannelResponse>, Status> {
        let alerts = self.alerts()?;
        let peer = peer_of(&request);
        let req = request.into_inner();
        let Some(channel) = req.channel else {
            return Err(Status::invalid_argument("channel is required"));
        };
        let stored = alerts
            .upsert_channel(channel_from_proto(channel)?, &req.secret, &peer)
            .await
            .map_err(status_of_alerts)?;
        let mask = stored.secret_mask();
        Ok(Response::new(UpsertChannelResponse {
            channel: Some(channel_to_proto(&stored, mask)),
        }))
    }

    async fn channel_delete(
        &self,
        request: Request<DeleteChannelRequest>,
    ) -> Result<Response<DeleteChannelResponse>, Status> {
        let alerts = self.alerts()?;
        let peer = peer_of(&request);
        alerts
            .delete_channel(&request.into_inner().id, &peer)
            .await
            .map_err(status_of_alerts)?;
        Ok(Response::new(DeleteChannelResponse {}))
    }

    async fn test_message(
        &self,
        request: Request<SendTestMessageRequest>,
    ) -> Result<Response<SendTestMessageResponse>, Status> {
        let alerts = self.alerts()?;
        let peer = peer_of(&request);
        let outcome = alerts
            .send_test(&request.into_inner().channel_id, &peer)
            .await
            .map_err(status_of_alerts)?;
        Ok(Response::new(SendTestMessageResponse {
            delivered: outcome.delivered,
            dry_run: outcome.dry_run,
            detail: outcome.detail,
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

            async fn list_rules(
                &self,
                _request: Request<ListRulesRequest>,
            ) -> Result<Response<ListRulesResponse>, Status> {
                self.rules_list().await
            }

            async fn upsert_rule(
                &self,
                request: Request<UpsertRuleRequest>,
            ) -> Result<Response<UpsertRuleResponse>, Status> {
                self.rule_upsert(request).await
            }

            async fn delete_rule(
                &self,
                request: Request<DeleteRuleRequest>,
            ) -> Result<Response<DeleteRuleResponse>, Status> {
                self.rule_delete(request).await
            }

            async fn list_channels(
                &self,
                _request: Request<ListChannelsRequest>,
            ) -> Result<Response<ListChannelsResponse>, Status> {
                self.channels_list().await
            }

            async fn upsert_channel(
                &self,
                request: Request<UpsertChannelRequest>,
            ) -> Result<Response<UpsertChannelResponse>, Status> {
                self.channel_upsert(request).await
            }

            async fn delete_channel(
                &self,
                request: Request<DeleteChannelRequest>,
            ) -> Result<Response<DeleteChannelResponse>, Status> {
                self.channel_delete(request).await
            }

            async fn send_test_message(
                &self,
                request: Request<SendTestMessageRequest>,
            ) -> Result<Response<SendTestMessageResponse>, Status> {
                self.test_message(request).await
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
    get_storage_status: GetStorageStatusRequest => GetStorageStatusResponse,
    rotate_token: RotateTokenRequest => RotateTokenResponse,
}
