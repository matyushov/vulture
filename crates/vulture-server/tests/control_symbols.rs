//! `Control.ListSymbolPolicies` and `SetSymbolPolicy` over the wire: authentication, validation, idempotence,
//! persistence across a restart, the audit log, metrics and the RPCs that stay unimplemented.

mod common;

use std::sync::Arc;

use common::*;
use tonic::Code;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use vulture_proto::v1::control_client::ControlClient;
use vulture_proto::v1::*;
use vulture_server::config::{Config, ExchangeConfig};
use vulture_server::metrics::{Families, MetricSource};
use vulture_server::symbols::{SymbolChannel, SymbolManager, SymbolPolicy};

type Control = ControlClient<InterceptedService<Channel, Auth>>;

const UNIVERSE: [&str; 4] = [
    "BTC-USDT-PERP",
    "ETH-USDT-PERP",
    "SOL-USDT-PERP",
    "币安人生-USDT-PERP",
];

fn config(dir: &std::path::Path) -> Config {
    let mut cfg = Config::default();
    cfg.server.data_dir = dir.to_path_buf();
    for (name, enabled) in [("binance", true), ("okx", false)] {
        cfg.exchanges.push(ExchangeConfig {
            name: name.into(),
            enabled,
            symbols: vec![],
            oi_poll_interval_secs: 30,
            ws_url: None,
            rest_url: None,
        });
    }
    cfg
}

struct Rig {
    server: Server,
    channel: Arc<SymbolChannel>,
    manager: Arc<SymbolManager>,
    data: tempfile::TempDir,
}

async fn rig(load_universe: bool) -> Rig {
    let data = tempfile::tempdir().unwrap();
    let channel = SymbolChannel::new(SymbolPolicy::all());
    if load_universe {
        channel.publish_universe(UNIVERSE.iter().map(|s| s.to_string()).collect());
    }
    let manager = SymbolManager::new(
        &config(data.path()),
        vec![("binance".into(), channel.clone())],
    )
    .unwrap();
    let backend = Backend::memory();
    let server = Server::start_full(
        &backend,
        test_stream_config(),
        Default::default(),
        Some(manager.clone()),
    )
    .await;
    Rig {
        server,
        channel,
        manager,
        data,
    }
}

async fn control(server: &Server, auth: Auth) -> Control {
    ControlClient::with_interceptor(server.channel().await, auth)
}

fn set_request(exchange: &str, mode: SymbolMode, symbols: &[&str]) -> SetSymbolPolicyRequest {
    SetSymbolPolicyRequest {
        policy: Some(SymbolPolicy_ {
            exchange: exchange.into(),
            mode: mode as i32,
            symbols: symbols.iter().map(|s| s.to_string()).collect(),
            oi_intervals: vec![],
        }),
    }
}

// The generated message shares its name with the server type imported above.
use vulture_proto::v1::SymbolPolicy as SymbolPolicy_;

#[tokio::test]
async fn authentication_is_required() {
    let r = rig(true).await;
    for auth in [Auth::none(), Auth::bearer("wrong-wrong-wrong-wrong")] {
        let mut c = control(&r.server, auth).await;
        let e = c
            .list_symbol_policies(ListSymbolPoliciesRequest::default())
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = c
            .set_symbol_policy(set_request("binance", SymbolMode::All, &[]))
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
    }
    assert_eq!(*r.channel.policy(), SymbolPolicy::all());
    r.server.stop().await;
}

#[tokio::test]
async fn list_shows_policy_and_counts() {
    let r = rig(true).await;
    let mut c = control(&r.server, Auth::bearer(&r.server.token)).await;
    let list = c
        .list_symbol_policies(ListSymbolPoliciesRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.policies.len(), 1);
    assert_eq!(list.policies[0].exchange, "binance");
    assert_eq!(list.policies[0].mode, SymbolMode::All as i32);
    assert!(list.policies[0].symbols.is_empty());
    let st = &list.statuses[0];
    assert_eq!(
        (
            st.tracked_count,
            st.universe_size,
            st.universe_loaded,
            st.overridden
        ),
        (4, 4, true, false)
    );
    // A named exchange; an unknown or disabled one is an argument error.
    let one = c
        .list_symbol_policies(ListSymbolPoliciesRequest {
            exchanges: vec!["binance".into()],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(one.policies.len(), 1);
    for name in ["nope", "okx"] {
        let e = c
            .list_symbol_policies(ListSymbolPoliciesRequest {
                exchanges: vec![name.into()],
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::InvalidArgument, "{name}");
    }
    r.server.stop().await;
}

#[tokio::test]
async fn set_applies_reports_the_diff_and_survives_a_restart() {
    let r = rig(true).await;
    let mut c = control(&r.server, Auth::bearer(&r.server.token)).await;
    let resp = c
        .set_symbol_policy(set_request(
            "binance",
            SymbolMode::Whitelist,
            &["btc-usdt-perp", "BTC-USDT-PERP", "币安人生-USDT-PERP"],
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.changed);
    assert_eq!((resp.tracked_count, resp.universe_size), (2, 4));
    assert_eq!(resp.removed, ["ETH-USDT-PERP", "SOL-USDT-PERP"]);
    assert!(resp.added.is_empty());
    let stored = resp.policy.unwrap();
    assert_eq!(stored.mode, SymbolMode::Whitelist as i32);
    assert_eq!(stored.symbols, ["BTC-USDT-PERP", "币安人生-USDT-PERP"]);
    // The connector side sees it at once.
    assert_eq!(r.channel.tracked_count(), 2);

    // Listing shows an override now.
    let list = c
        .list_symbol_policies(ListSymbolPoliciesRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert!(list.statuses[0].overridden);
    assert_eq!(list.statuses[0].tracked_count, 2);

    // A restart: a fresh channel (config default ALL) and a fresh manager over the same data directory.
    let channel2 = SymbolChannel::new(SymbolPolicy::all());
    channel2.publish_universe(UNIVERSE.iter().map(|s| s.to_string()).collect());
    let manager2 = SymbolManager::new(
        &config(r.data.path()),
        vec![("binance".into(), channel2.clone())],
    )
    .unwrap();
    assert_eq!(channel2.tracked_count(), 2);
    assert_eq!(*channel2.policy(), *r.channel.policy());
    let server2 = Server::start_full(
        &Backend::memory(),
        test_stream_config(),
        Default::default(),
        Some(manager2),
    )
    .await;
    let mut c2 = control(&server2, Auth::bearer(&server2.token)).await;
    let list = c2
        .list_symbol_policies(ListSymbolPoliciesRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.policies[0].mode, SymbolMode::Whitelist as i32);
    assert!(list.statuses[0].overridden);
    server2.stop().await;
    r.server.stop().await;
}

#[tokio::test]
async fn blacklist_and_all_round_trip() {
    let r = rig(true).await;
    let mut c = control(&r.server, Auth::bearer(&r.server.token)).await;
    let b = c
        .set_symbol_policy(set_request(
            "binance",
            SymbolMode::Blacklist,
            &["ETH-USDT-PERP"],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (b.tracked_count, b.removed.clone()),
        (3, vec!["ETH-USDT-PERP".to_string()])
    );
    let a = c
        .set_symbol_policy(set_request(
            "binance",
            SymbolMode::All,
            &["ignored-is-fine"],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(a.tracked_count, 4);
    assert_eq!(a.added, ["ETH-USDT-PERP"]);
    assert!(a.policy.unwrap().symbols.is_empty());
    r.server.stop().await;
}

#[tokio::test]
async fn repeating_a_request_is_idempotent() {
    let r = rig(true).await;
    let mut c = control(&r.server, Auth::bearer(&r.server.token)).await;
    let req = set_request("binance", SymbolMode::Blacklist, &["SOL-USDT-PERP"]);
    let first = c.set_symbol_policy(req.clone()).await.unwrap().into_inner();
    let second = c.set_symbol_policy(req).await.unwrap().into_inner();
    assert!(first.changed && !second.changed);
    assert_eq!(first.policy, second.policy);
    assert_eq!(first.tracked_count, second.tracked_count);
    assert!(second.added.is_empty() && second.removed.is_empty());
    // One change, one audit line.
    let audit = std::fs::read_to_string(r.data.path().join("audit.log")).unwrap();
    assert_eq!(audit.lines().count(), 1);
    r.server.stop().await;
}

#[tokio::test]
async fn invalid_requests_are_refused_and_change_nothing() {
    let r = rig(true).await;
    let mut c = control(&r.server, Auth::bearer(&r.server.token)).await;
    let cases: Vec<(&str, SetSymbolPolicyRequest, Code)> = vec![
        (
            "no policy",
            SetSymbolPolicyRequest { policy: None },
            Code::InvalidArgument,
        ),
        (
            "unspecified mode",
            set_request("binance", SymbolMode::Unspecified, &[]),
            Code::InvalidArgument,
        ),
        (
            "unknown exchange",
            set_request("nope", SymbolMode::All, &[]),
            Code::InvalidArgument,
        ),
        (
            "disabled exchange",
            set_request("okx", SymbolMode::All, &[]),
            Code::InvalidArgument,
        ),
        (
            "empty whitelist",
            set_request("binance", SymbolMode::Whitelist, &[]),
            Code::InvalidArgument,
        ),
        (
            "malformed name",
            set_request("binance", SymbolMode::Blacklist, &["BTCUSDT"]),
            Code::InvalidArgument,
        ),
        (
            "unknown symbol",
            set_request(
                "binance",
                SymbolMode::Whitelist,
                &["BTC-USDT-PERP", "TYPO-USDT-PERP"],
            ),
            Code::InvalidArgument,
        ),
    ];
    for (name, req, code) in cases {
        let e = c.set_symbol_policy(req).await.unwrap_err();
        assert_eq!(e.code(), code, "{name}: {e}");
    }
    // The unknown names are listed in the message.
    let e = c
        .set_symbol_policy(set_request(
            "binance",
            SymbolMode::Whitelist,
            &["BTC-USDT-PERP", "TYPO-USDT-PERP", "OOPS-USDC-PERP"],
        ))
        .await
        .unwrap_err();
    assert!(e.message().contains("TYPO-USDT-PERP") && e.message().contains("OOPS-USDC-PERP"));
    assert!(!e.message().contains("BTC-USDT-PERP"));
    // An unspecified mode value from a newer client is an argument error as well.
    let mut req = set_request("binance", SymbolMode::All, &[]);
    req.policy.as_mut().unwrap().mode = 99;
    assert_eq!(
        c.set_symbol_policy(req).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    assert_eq!(*r.channel.policy(), SymbolPolicy::all());
    assert!(!r.data.path().join("state.json").exists());
    assert!(!r.data.path().join("audit.log").exists());
    r.server.stop().await;
}

#[tokio::test]
async fn overrides_are_unimplemented_not_ignored() {
    let r = rig(true).await;
    let mut c = control(&r.server, Auth::bearer(&r.server.token)).await;
    let mut req = set_request("binance", SymbolMode::Blacklist, &["ETH-USDT-PERP"]);
    req.policy.as_mut().unwrap().oi_intervals = vec![SymbolOiInterval {
        symbol: "BTC-USDT-PERP".into(),
        interval_seconds: 5,
    }];
    let e = c.set_symbol_policy(req).await.unwrap_err();
    assert_eq!(e.code(), Code::Unimplemented);
    assert!(e.message().contains("oi_intervals"));
    assert_eq!(*r.channel.policy(), SymbolPolicy::all());
    assert!(!r.data.path().join("state.json").exists());
    r.server.stop().await;
}

#[tokio::test]
async fn names_need_a_loaded_symbol_list() {
    let r = rig(false).await;
    let mut c = control(&r.server, Auth::bearer(&r.server.token)).await;
    let e = c
        .set_symbol_policy(set_request(
            "binance",
            SymbolMode::Whitelist,
            &["BTC-USDT-PERP"],
        ))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition);
    let list = c
        .list_symbol_policies(ListSymbolPoliciesRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert!(!list.statuses[0].universe_loaded);
    // ALL needs no names.
    c.set_symbol_policy(set_request("binance", SymbolMode::All, &[]))
        .await
        .unwrap();
    r.server.stop().await;
}

#[tokio::test]
async fn the_audit_log_has_the_change_and_no_secrets() {
    let r = rig(true).await;
    let mut c = control(&r.server, Auth::bearer(&r.server.token)).await;
    c.set_symbol_policy(set_request(
        "binance",
        SymbolMode::Blacklist,
        &["ETH-USDT-PERP"],
    ))
    .await
    .unwrap();
    c.set_symbol_policy(set_request(
        "binance",
        SymbolMode::Whitelist,
        &["BTC-USDT-PERP"],
    ))
    .await
    .unwrap();
    let text = std::fs::read_to_string(r.data.path().join("audit.log")).unwrap();
    let lines: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    let first = &lines[0];
    assert_eq!(first["event"], "symbol_policy");
    assert_eq!(first["exchange"], "binance");
    assert_eq!(first["mode"], "blacklist");
    assert_eq!(first["removed"][0], "ETH-USDT-PERP");
    assert_eq!(
        (
            first["tracked_before"].as_u64(),
            first["tracked_after"].as_u64()
        ),
        (Some(4), Some(3))
    );
    assert!(first["peer"].as_str().unwrap().starts_with("127.0.0.1:"));
    assert!(first["time"].as_str().unwrap().ends_with('Z'));
    assert_eq!(lines[1]["mode"], "whitelist");
    assert_eq!(lines[1]["removed"].as_array().unwrap().len(), 2);
    // Neither the audit log nor the state file contains the token.
    let state = std::fs::read_to_string(r.data.path().join("state.json")).unwrap();
    for file in [&text, &state] {
        assert!(!file.contains(&r.server.token));
        assert!(!file.to_lowercase().contains("bearer"));
    }
    r.server.stop().await;
}

#[tokio::test]
async fn metrics_series_move() {
    let r = rig(true).await;
    let mut c = control(&r.server, Auth::bearer(&r.server.token)).await;
    let scrape = |m: &SymbolManager| {
        let mut f = Families::new();
        m.collect(&mut f);
        f.render()
    };
    let before = scrape(&r.manager);
    assert!(before.contains("vulture_symbols_tracked{exchange=\"binance\"} 4"));
    assert!(before.contains("vulture_symbols_universe{exchange=\"binance\"} 4"));
    c.set_symbol_policy(set_request(
        "binance",
        SymbolMode::Blacklist,
        &["ETH-USDT-PERP"],
    ))
    .await
    .unwrap();
    let _ = c
        .set_symbol_policy(set_request("binance", SymbolMode::Whitelist, &[]))
        .await;
    let after = scrape(&r.manager);
    assert!(
        after.contains("vulture_symbols_tracked{exchange=\"binance\"} 3"),
        "{after}"
    );
    assert!(after.contains(
        "vulture_symbol_policy_changes_total{exchange=\"binance\",result=\"applied\"} 1"
    ));
    assert!(after.contains(
        "vulture_symbol_policy_changes_total{exchange=\"binance\",result=\"rejected\"} 1"
    ));
    r.server.stop().await;
}

#[tokio::test]
async fn the_other_control_rpcs_stay_unimplemented() {
    let r = rig(true).await;
    let mut c = control(&r.server, Auth::bearer(&r.server.token)).await;
    macro_rules! unimplemented {
        ($($method:ident($req:ty)),* $(,)?) => {$(
            let e = c.$method(<$req>::default()).await.unwrap_err();
            assert_eq!(e.code(), Code::Unimplemented, stringify!($method));
        )*};
    }
    unimplemented!(
        list_subscriptions(ListSubscriptionsRequest),
        set_subscription(SetSubscriptionRequest),
        get_connector_status(GetConnectorStatusRequest),
        set_oi_interval(SetOiIntervalRequest),
        list_rules(ListRulesRequest),
        upsert_rule(UpsertRuleRequest),
        delete_rule(DeleteRuleRequest),
        list_channels(ListChannelsRequest),
        upsert_channel(UpsertChannelRequest),
        delete_channel(DeleteChannelRequest),
        send_test_message(SendTestMessageRequest),
        get_storage_status(GetStorageStatusRequest),
        rotate_token(RotateTokenRequest),
    );
    r.server.stop().await;
}

#[tokio::test]
async fn without_a_symbol_control_the_symbol_rpcs_are_unimplemented_too() {
    let backend = Backend::memory();
    let server = Server::start(&backend, test_stream_config()).await;
    let mut c = control(&server, Auth::bearer(&server.token)).await;
    let e = c
        .list_symbol_policies(ListSymbolPoliciesRequest::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Unimplemented);
    server.stop().await;
}
