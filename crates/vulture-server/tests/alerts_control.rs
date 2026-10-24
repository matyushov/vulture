//! `Control` rules and channels over the wire: authentication, validation, idempotence, hot apply, persistence
//! across a restart, a corrupt `alerts.json`, the test-message RPC, the audit log, and the absence of secrets in
//! replies, logs and audit lines.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::log_capture::capture;
use common::mock_http::{MockHttp, Reply};
use common::*;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use vulture_proto::v1::control_client::ControlClient;
use vulture_proto::v1::*;
use vulture_server::alerts::Alerts;
use vulture_server::config::Config;

type Control = ControlClient<InterceptedService<Channel, Auth>>;

const TOKEN: &str = "123456:SECRETtokenABCdef_ghi-JKL";
const WEBHOOK_TOKEN: &str = "WEBHOOKtokenXYZ123";

struct Rig {
    server: Server,
    alerts: Arc<Alerts>,
    mock: MockHttp,
    data: tempfile::TempDir,
    stop: CancellationToken,
}

fn config(dir: &std::path::Path, mock: &MockHttp) -> Config {
    let mut cfg = Config::default();
    cfg.server.data_dir = dir.to_path_buf();
    cfg.alerts.delivery.telegram_base_url = mock.url();
    cfg.alerts.delivery.allow_custom_endpoints = true;
    cfg.alerts.delivery.batch_window_ms = 50;
    cfg.alerts.delivery.telegram_min_interval_ms = 100;
    cfg
}

async fn rig() -> Rig {
    let mock = MockHttp::start(Reply::ok()).await;
    let data = tempfile::tempdir().unwrap();
    let stop = CancellationToken::new();
    let alerts = Alerts::open(&config(data.path(), &mock), stop.clone()).unwrap();
    let server = Server::start_all(
        &Backend::memory(),
        test_stream_config(),
        Default::default(),
        None,
        Some(alerts.manager.clone()),
    )
    .await;
    Rig {
        server,
        alerts,
        mock,
        data,
        stop,
    }
}

impl Rig {
    async fn control(&self) -> Control {
        ControlClient::with_interceptor(
            self.server.channel().await,
            Auth::bearer(&self.server.token),
        )
    }
    async fn finish(self) {
        self.stop.cancel();
        self.server.stop().await;
    }
}

fn telegram_channel(id: &str) -> UpsertChannelRequest {
    UpsertChannelRequest {
        channel: Some(vulture_proto::v1::Channel {
            id: id.into(),
            name: "Owner Telegram".into(),
            kind: ChannelKind::Telegram as i32,
            enabled: true,
            chat_id: "-1001234".into(),
            ..Default::default()
        }),
        secret: TOKEN.into(),
    }
}

fn discord_channel(id: &str, mock: &MockHttp) -> UpsertChannelRequest {
    UpsertChannelRequest {
        channel: Some(vulture_proto::v1::Channel {
            id: id.into(),
            name: "Owner Discord".into(),
            kind: ChannelKind::Discord as i32,
            enabled: true,
            ..Default::default()
        }),
        secret: format!("{}/api/webhooks/42/{WEBHOOK_TOKEN}", mock.url()),
    }
}

fn liquidation_rule(id: &str, channels: &[&str]) -> UpsertRuleRequest {
    UpsertRuleRequest {
        rule: Some(Rule {
            id: id.into(),
            name: "Big liquidations".into(),
            enabled: true,
            kind: Some(rule::Kind::Liquidation(LiquidationRule {
                min_notional_usd: 250_000.0,
                sides: vec![Side::Long as i32],
            })),
            exchanges: vec!["binance".into()],
            symbols: vec!["btc-usdt-perp".into()],
            channel_ids: channels.iter().map(|s| s.to_string()).collect(),
            cooldown_seconds: 60,
        }),
    }
}

fn audit_text(dir: &std::path::Path) -> String {
    std::fs::read_to_string(dir.join("audit.log")).unwrap_or_default()
}

#[tokio::test]
async fn every_rule_and_channel_rpc_needs_the_token() {
    let r = rig().await;
    let mut anon = ControlClient::with_interceptor(r.server.channel().await, Auth::none());
    macro_rules! denied {
        ($($method:ident($req:ty)),* $(,)?) => {$(
            let e = anon.$method(<$req>::default()).await.unwrap_err();
            assert_eq!(e.code(), Code::Unauthenticated, stringify!($method));
        )*};
    }
    denied!(
        list_rules(ListRulesRequest),
        upsert_rule(UpsertRuleRequest),
        delete_rule(DeleteRuleRequest),
        list_channels(ListChannelsRequest),
        upsert_channel(UpsertChannelRequest),
        delete_channel(DeleteChannelRequest),
        send_test_message(SendTestMessageRequest),
    );
    r.finish().await;
}

#[tokio::test]
async fn channels_round_trip_with_masked_secrets_and_are_idempotent() {
    let (logs, _guard) = capture();
    let r = rig().await;
    let mut c = r.control().await;

    let created = c
        .upsert_channel(telegram_channel("tg"))
        .await
        .unwrap()
        .into_inner();
    let ch = created.channel.clone().unwrap();
    assert_eq!(ch.id, "tg");
    assert_eq!(ch.secret_mask, "****-JKL");
    let created2 = c
        .upsert_channel(discord_channel("dc", &r.mock))
        .await
        .unwrap()
        .into_inner();
    assert!(
        created2
            .channel
            .clone()
            .unwrap()
            .secret_mask
            .starts_with("****")
    );

    let listed = c
        .list_channels(ListChannelsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.channels.len(), 2);
    let dump = format!("{listed:?} {created:?} {created2:?}");
    for secret in [TOKEN, "SECRETtoken", WEBHOOK_TOKEN] {
        assert!(
            !dump.contains(secret),
            "API replies must not carry the secret: {dump}"
        );
    }
    let tg = listed.channels.iter().find(|c| c.id == "tg").unwrap();
    assert_eq!(tg.chat_id, "-1001234");
    assert!(tg.enabled && tg.kind == ChannelKind::Telegram as i32);

    // Repeating the same request changes nothing and leaves no second audit line.
    let before = audit_text(r.data.path()).lines().count();
    c.upsert_channel(telegram_channel("tg")).await.unwrap();
    assert_eq!(audit_text(r.data.path()).lines().count(), before);

    // An empty secret keeps the current one: rename only.
    let mut rename = telegram_channel("tg");
    rename.secret.clear();
    rename.channel.as_mut().unwrap().name = "Renamed".into();
    let renamed = c
        .upsert_channel(rename)
        .await
        .unwrap()
        .into_inner()
        .channel
        .unwrap();
    assert_eq!(renamed.name, "Renamed");
    assert_eq!(renamed.secret_mask, "****-JKL", "the secret was kept");

    // The server really holds the secret: a test message reaches the mock with it.
    let t = c
        .send_test_message(SendTestMessageRequest {
            channel_id: "tg".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(t.delivered && !t.dry_run && t.detail == "ok", "{t:?}");
    assert_eq!(
        r.mock.requests()[0].path,
        format!("/bot{TOKEN}/sendMessage")
    );

    // Deleting is idempotent.
    c.delete_channel(DeleteChannelRequest { id: "dc".into() })
        .await
        .unwrap();
    c.delete_channel(DeleteChannelRequest { id: "dc".into() })
        .await
        .unwrap();
    assert_eq!(
        c.list_channels(ListChannelsRequest {})
            .await
            .unwrap()
            .into_inner()
            .channels
            .len(),
        1
    );

    // Neither the audit log nor the log lines hold a secret.
    let audit = audit_text(r.data.path());
    assert!(
        audit.contains("channel_upsert")
            && audit.contains("channel_delete")
            && audit.contains("channel_test")
    );
    let all = format!("{audit}\n{}", logs.text());
    for secret in [TOKEN, "SECRETtoken", WEBHOOK_TOKEN] {
        assert!(!all.contains(secret), "secret in logs or audit: {all}");
    }
    r.finish().await;
}

#[tokio::test]
async fn invalid_channels_are_refused_and_change_nothing() {
    let r = rig().await;
    let mut c = r.control().await;
    let bad: Vec<(&str, UpsertChannelRequest)> = vec![
        ("no kind", {
            let mut q = telegram_channel("x");
            q.channel.as_mut().unwrap().kind = 0;
            q
        }),
        ("no secret", {
            let mut q = telegram_channel("x");
            q.secret.clear();
            q
        }),
        ("no chat id", {
            let mut q = telegram_channel("x");
            q.channel.as_mut().unwrap().chat_id.clear();
            q
        }),
        ("bad token shape", {
            let mut q = telegram_channel("x");
            q.secret = "has/slash".into();
            q
        }),
        ("bad id", telegram_channel("Bad Id")),
        ("discord webhook is not a URL", {
            let mut q = discord_channel("x", &r.mock);
            q.secret = "not a url".into();
            q
        }),
        ("two secret sources", {
            let mut q = telegram_channel("x");
            q.channel.as_mut().unwrap().secret_env = "SOME_VAR".into();
            q
        }),
    ];
    for (what, req) in bad {
        let e = c.upsert_channel(req).await.unwrap_err();
        assert_eq!(e.code(), Code::InvalidArgument, "{what}: {e}");
        assert!(
            !e.message().contains(TOKEN) && !e.message().contains("has/slash"),
            "{what}: {e}"
        );
    }
    let missing = c
        .upsert_channel(UpsertChannelRequest::default())
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Code::InvalidArgument);
    assert!(
        c.list_channels(ListChannelsRequest {})
            .await
            .unwrap()
            .into_inner()
            .channels
            .is_empty()
    );
    assert!(
        !r.data.path().join("alerts.json").exists(),
        "a refused change writes nothing"
    );
    r.finish().await;
}

#[tokio::test]
async fn rules_round_trip_validate_and_protect_channels() {
    let r = rig().await;
    let mut c = r.control().await;
    c.upsert_channel(telegram_channel("tg")).await.unwrap();

    // A rule must name existing channels.
    let e = c
        .upsert_rule(liquidation_rule("big", &["nope"]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);
    let e = c
        .upsert_rule(liquidation_rule("big", &[]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "a rule needs a channel");

    let stored = c
        .upsert_rule(liquidation_rule("big", &["tg"]))
        .await
        .unwrap()
        .into_inner()
        .rule
        .unwrap();
    assert_eq!(stored.symbols, ["BTC-USDT-PERP"], "names are normalized");
    // Hot apply: the engine's rule set changed without a restart.
    assert_eq!(r.alerts.rules.load().rules.len(), 1);

    // An empty id creates a rule with a server-assigned id.
    let mut created = liquidation_rule("", &["tg"]);
    created.rule.as_mut().unwrap().name = "Second".into();
    let created = c
        .upsert_rule(created)
        .await
        .unwrap()
        .into_inner()
        .rule
        .unwrap();
    assert!(created.id.starts_with("r-") && created.id.len() > 3);

    // Other kinds.
    let oi = UpsertRuleRequest {
        rule: Some(Rule {
            id: "oi5".into(),
            enabled: true,
            kind: Some(rule::Kind::OiChange(OiChangeRule {
                min_change_pct: 4.0,
                window_seconds: 300,
                min_change_usd: 500_000.0,
                detector_override: false,
            })),
            channel_ids: vec!["tg".into()],
            ..Default::default()
        }),
    };
    c.upsert_rule(oi).await.unwrap();
    let signal = UpsertRuleRequest {
        rule: Some(Rule {
            id: "sig".into(),
            enabled: true,
            kind: Some(rule::Kind::Signal(SignalRule {
                types: vec![SignalType::LiqCascade as i32],
                min_severity: Severity::Warning as i32,
            })),
            channel_ids: vec!["tg".into()],
            ..Default::default()
        }),
    };
    c.upsert_rule(signal).await.unwrap();
    let over = UpsertRuleRequest {
        rule: Some(Rule {
            id: "ov".into(),
            enabled: true,
            kind: Some(rule::Kind::OiChange(OiChangeRule {
                min_change_pct: 1.0,
                window_seconds: 300,
                min_change_usd: 0.0,
                detector_override: true,
            })),
            symbols: vec!["BTC-USDT-PERP".into()],
            ..Default::default()
        }),
    };
    c.upsert_rule(over).await.unwrap();
    assert_eq!(r.alerts.rules.load().overrides.len(), 1);

    // Refusals.
    let gap = UpsertRuleRequest {
        rule: Some(Rule {
            id: "g".into(),
            kind: Some(rule::Kind::Gap(GapRule { min_duration_ms: 1 })),
            channel_ids: vec!["tg".into()],
            ..Default::default()
        }),
    };
    assert_eq!(
        c.upsert_rule(gap).await.unwrap_err().code(),
        Code::Unimplemented
    );
    let no_kind = UpsertRuleRequest {
        rule: Some(Rule {
            id: "k".into(),
            channel_ids: vec!["tg".into()],
            ..Default::default()
        }),
    };
    assert_eq!(
        c.upsert_rule(no_kind).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    let mut zero = liquidation_rule("z", &["tg"]);
    if let Some(rule::Kind::Liquidation(l)) = &mut zero.rule.as_mut().unwrap().kind {
        l.min_notional_usd = 0.0;
    }
    assert_eq!(
        c.upsert_rule(zero).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    let mut oi_bad = liquidation_rule("w", &["tg"]);
    oi_bad.rule.as_mut().unwrap().kind = Some(rule::Kind::OiChange(OiChangeRule {
        min_change_pct: 3.0,
        window_seconds: 5,
        ..Default::default()
    }));
    assert_eq!(
        c.upsert_rule(oi_bad).await.unwrap_err().code(),
        Code::InvalidArgument
    );

    // A channel that rules use cannot be deleted.
    let e = c
        .delete_channel(DeleteChannelRequest { id: "tg".into() })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition);
    assert!(e.message().contains("big"));

    // Idempotent repeat: no audit line.
    let lines = audit_text(r.data.path()).lines().count();
    c.upsert_rule(liquidation_rule("big", &["tg"]))
        .await
        .unwrap();
    assert_eq!(audit_text(r.data.path()).lines().count(), lines);

    // Delete (twice: idempotent), then the list shrinks and the engine follows.
    c.delete_rule(DeleteRuleRequest { id: "big".into() })
        .await
        .unwrap();
    c.delete_rule(DeleteRuleRequest { id: "big".into() })
        .await
        .unwrap();
    let ids: Vec<String> = c
        .list_rules(ListRulesRequest {})
        .await
        .unwrap()
        .into_inner()
        .rules
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert!(
        !ids.contains(&"big".to_string()) && ids.contains(&"oi5".to_string()),
        "{ids:?}"
    );
    assert_eq!(
        r.alerts.rules.load().rules.len(),
        ids.len() - 1,
        "all but the override"
    );
    assert!(audit_text(r.data.path()).contains("rule_delete"));
    r.finish().await;
}

#[tokio::test]
async fn rules_and_channels_survive_a_restart_secret_included() {
    let r = rig().await;
    let mut c = r.control().await;
    c.upsert_channel(telegram_channel("tg")).await.unwrap();
    c.upsert_rule(liquidation_rule("big", &["tg"]))
        .await
        .unwrap();
    let path = r.data.path().join("alerts.json");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "alerts.json holds secrets");
    }
    let dir = r.data.path().to_path_buf();
    let mock_url = r.mock.url();
    let stop2 = CancellationToken::new();
    // A second instance over the same directory, as after a restart.
    let mut cfg = Config::default();
    cfg.server.data_dir = dir.clone();
    cfg.alerts.delivery.telegram_base_url = mock_url;
    let again = Alerts::open(&cfg, stop2.clone()).unwrap();
    let rules = again.manager.list_rules().await;
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].id, "big");
    let channels = again.manager.list_channels().await;
    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].mask, "****-JKL");
    let sent = again.manager.send_test("tg", "restart-test").await.unwrap();
    assert!(sent.delivered, "{sent:?}");
    assert_eq!(
        again.rules.load().rules.len(),
        1,
        "the rules are live right after the start"
    );
    stop2.cancel();
    r.finish().await;
}

#[tokio::test]
async fn a_corrupt_alerts_file_refuses_to_start_and_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alerts.json");
    let mut cfg = Config::default();
    cfg.server.data_dir = dir.path().to_path_buf();
    for content in [
        "{ not json".to_string(),
        r#"{"version": 9}"#.to_string(),
        r#"{"version": 1, "rules": [{"id": "x", "kind": {"type": "liquidation", "min_notional_usd": -5}, "channels": ["c"]}]}"#.to_string(),
        format!(r#"{{"version": 1, "channels": [{{"id": "c", "kind": "telegram", "chat_id": "1", "secret": "{TOKEN}/oops"}}]}}"#),
    ] {
        std::fs::write(&path, &content).unwrap();
        let err = match Alerts::open(&cfg, CancellationToken::new()) {
            Ok(_) => panic!("must refuse: {content}"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("alerts file"), "{err}");
        assert!(!err.contains(TOKEN) && !err.contains("SECRETtoken"), "{err}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), content, "the file is not touched");
    }
}

#[tokio::test]
async fn configuration_defaults_seed_the_rules_until_the_file_exists() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = Config::default();
    cfg.server.data_dir = dir.path().to_path_buf();
    cfg.alerts = toml_alerts(
        r#"
        [[channels]]
        id = "tg"
        kind = "telegram"
        chat_id = "-100"
        dry_run = true
        secret_env = "VULTURE_TEST_SEED_TOKEN"

        [[rules]]
        id = "seed"
        name = "seeded"
        channels = ["tg"]
        cooldown_secs = 30
        [rules.kind]
        type = "liquidation"
        min_notional_usd = 500000.0
        sides = ["long"]
        "#,
    );
    let a = Alerts::open(&cfg, CancellationToken::new()).unwrap();
    assert_eq!(a.manager.list_rules().await.len(), 1);
    assert_eq!(a.rules.load().rules.len(), 1);
    assert!(
        !dir.path().join("alerts.json").exists(),
        "nothing is written until the owner changes something"
    );
    // The first change writes the file with the defaults and the change; later config edits are ignored.
    a.manager
        .upsert_rule(
            serde_json::from_str(r#"{"id":"mine","kind":{"type":"signal"},"channels":["tg"]}"#)
                .unwrap(),
            "test",
        )
        .await
        .unwrap();
    let doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("alerts.json")).unwrap())
            .unwrap();
    assert_eq!(doc["rules"].as_array().unwrap().len(), 2);
    assert_eq!(doc["channels"].as_array().unwrap().len(), 1);
}

fn toml_alerts(text: &str) -> vulture_server::config::AlertsConfig {
    let wrapped = text
        .replace("[[channels]]", "[[alerts.channels]]")
        .replace("[[rules]]", "[[alerts.rules]]")
        .replace("[rules.kind]", "[alerts.rules.kind]");
    Config::from_toml_with_env(&wrapped, std::iter::empty())
        .unwrap()
        .alerts
}

#[tokio::test]
async fn the_test_message_rpc_covers_dry_run_failures_and_unknown_channels() {
    let r = rig().await;
    let mut c = r.control().await;
    let e = c
        .send_test_message(SendTestMessageRequest {
            channel_id: "nope".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::NotFound);

    let mut dry = telegram_channel("dry");
    dry.channel.as_mut().unwrap().dry_run = true;
    c.upsert_channel(dry).await.unwrap();
    let t = c
        .send_test_message(SendTestMessageRequest {
            channel_id: "dry".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(t.dry_run && !t.delivered && t.detail == "dry run");
    assert_eq!(r.mock.count(), 0);

    c.upsert_channel(telegram_channel("tg")).await.unwrap();
    r.mock.set_default(Reply::status(
        403,
        r#"{"ok":false,"description":"Forbidden: bot was blocked"}"#,
    ));
    let t = c
        .send_test_message(SendTestMessageRequest {
            channel_id: "tg".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!t.delivered && t.detail.contains("HTTP 403"), "{t:?}");
    assert!(!t.detail.contains(TOKEN));
    r.finish().await;
}

#[tokio::test]
async fn config_changes_show_up_in_the_metrics() {
    use vulture_server::metrics::{Families, MetricSource};
    let r = rig().await;
    let mut c = r.control().await;
    c.upsert_channel(telegram_channel("tg")).await.unwrap();
    c.upsert_rule(liquidation_rule("big", &["tg"]))
        .await
        .unwrap();
    let _ = c.upsert_rule(liquidation_rule("bad", &["missing"])).await;
    let mut f = Families::new();
    r.alerts.manager.collect(&mut f);
    let text = f.render();
    assert!(
        text.contains("vulture_alerts_config_changes_total 2"),
        "{text}"
    );
    assert!(
        text.contains("vulture_alerts_config_rejected_total 1"),
        "{text}"
    );
    assert!(text.contains("vulture_alerts_rules_active 1"), "{text}");
    tokio::time::sleep(Duration::from_millis(10)).await;
    r.finish().await;
}
