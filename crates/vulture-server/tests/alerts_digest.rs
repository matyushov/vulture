//! The daily digest against mock Telegram and Discord servers: content, limits, separate message, dry run, the
//! `SendDigestNow` RPC and the schedule. Nothing here talks to a real service.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::history_data::*;
use common::history_rig::*;
use common::mock_http::{MockHttp, Reply};
use common::*;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use vulture_proto::v1::control_client::ControlClient;
use vulture_proto::v1::{SendDigestNowRequest, UpsertChannelRequest};
use vulture_server::alerts::model::Severity;
use vulture_server::alerts::text::Alert;
use vulture_server::alerts::{Alerts, DigestService};
use vulture_server::config::{Config, DigestConfig};
use vulture_server::query::MemoryCold;
use vulture_server::query::cold::ColdStore;

const TOKEN: &str = "123456:SECRETtokenABCdef_ghi-JKL";

struct Rig {
    mock: MockHttp,
    alerts: Arc<Alerts>,
    digest: Arc<DigestService>,
    history: Arc<vulture_server::query::HistoryService>,
    clock: SetClock,
    stop: CancellationToken,
    _data: tempfile::TempDir,
}

async fn rig(digest_cfg: DigestConfig) -> Rig {
    let mock = MockHttp::start(Reply::ok()).await;
    let data = tempfile::tempdir().unwrap();
    let mut cfg = Config::default();
    cfg.server.data_dir = data.path().to_path_buf();
    cfg.alerts.delivery.telegram_base_url = mock.url();
    cfg.alerts.delivery.allow_custom_endpoints = true;
    cfg.alerts.delivery.batch_window_ms = 50;
    cfg.alerts.delivery.telegram_min_interval_ms = 100;
    cfg.alerts.delivery.discord_min_interval_ms = 100;
    let stop = CancellationToken::new();
    let alerts = Alerts::open(&cfg, stop.clone()).unwrap();
    let ds = dataset().await;
    let cold = Arc::new(MemoryCold::new());
    cold.insert(ds.events.iter().cloned());
    let wm = Arc::new(vulture_server::history::Watermark::new());
    wm.set(&ds.events.last().unwrap().id);
    let clock = SetClock::new(NOW);
    let history = vulture_server::query::HistoryService::new(
        ds.stream.store.clone(),
        Some(cold as Arc<dyn ColdStore>),
        Some(wm),
        cfg_history(),
        Default::default(),
        Arc::new(clock.clone()),
    );
    let digest = DigestService::new(
        history.clone(),
        alerts.delivery.clone(),
        alerts.manager.clone(),
        digest_cfg,
        Arc::new(clock.clone()),
    );
    Rig {
        mock,
        alerts,
        digest,
        history,
        clock,
        stop,
        _data: data,
    }
}

fn cfg_history() -> vulture_server::config::HistoryApiConfig {
    cfg()
}

impl Rig {
    async fn add_telegram(&self, id: &str, dry_run: bool) {
        self.alerts
            .manager
            .upsert_channel(
                vulture_server::alerts::model::ChannelDef {
                    id: id.into(),
                    name: format!("Telegram {id}"),
                    kind: vulture_server::alerts::model::ChannelKind::Telegram,
                    enabled: true,
                    system: false,
                    dry_run,
                    chat_id: "-1001234".into(),
                    secret: None,
                    secret_file: String::new(),
                    secret_env: String::new(),
                    min_interval_ms: 0,
                    batch_window_ms: 0,
                    queue_limit: 0,
                    quiet_hours: vec![],
                },
                TOKEN,
                "test",
            )
            .await
            .unwrap();
    }

    async fn add_discord(&self, id: &str) {
        self.alerts
            .manager
            .upsert_channel(
                vulture_server::alerts::model::ChannelDef {
                    id: id.into(),
                    name: format!("Discord {id}"),
                    kind: vulture_server::alerts::model::ChannelKind::Discord,
                    enabled: true,
                    system: false,
                    dry_run: false,
                    chat_id: String::new(),
                    secret: None,
                    secret_file: String::new(),
                    secret_env: String::new(),
                    min_interval_ms: 0,
                    batch_window_ms: 0,
                    queue_limit: 0,
                    quiet_hours: vec![],
                },
                &format!("{}/api/webhooks/42/WEBHOOKtokenXYZ123", self.mock.url()),
                "test",
            )
            .await
            .unwrap();
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

#[tokio::test]
async fn the_digest_reaches_telegram_and_discord_within_their_limits() {
    let r = rig(DigestConfig::default()).await;
    r.add_telegram("tg", false).await;
    r.add_discord("dc").await;
    let out = r.digest.send_now(&[], &[], None).await.unwrap();
    assert_eq!(out.queued, ["dc", "tg"]);
    assert!(out.unavailable.is_empty(), "{:?}", out.unavailable);
    wait_until("both messages", || r.mock.count() == 2).await;
    let reqs = r.mock.requests();
    let tg = reqs
        .iter()
        .find(|q| q.path.contains("/sendMessage"))
        .unwrap()
        .json();
    let text = tg["text"].as_str().unwrap();
    assert_eq!(tg["parse_mode"], "HTML");
    assert!(text.chars().count() <= 4096, "{}", text.chars().count());
    for heading in [
        "vulture daily digest, 2026-10-26 04:30",
        "Open interest movers, 24 h",
        "Largest liquidations, 24 h",
        "Liquidations by exchange, 24 h to 04:00",
        "Liquidation cascades, 24 h",
        "Funding extremes",
        "Data gaps, 24 h",
    ] {
        assert!(text.contains(heading), "{heading}\n{text}");
    }
    // Content from the data set: the movers need 24 hours of history and there are three, so that list is empty
    // (the layout of full lists is covered by the unit tests); the bybit hole shows up as a gap, funding is there
    // for binance and bybit, and the largest liquidations come from the three hours.
    assert!(text.contains("no symbol with enough history"), "{text}");
    assert!(text.contains("long ") && text.contains("at 0"), "{text}");
    assert!(
        text.contains("bybit open_interest: 1 gap(s), 30m in total, longest 30m"),
        "{text}"
    );
    assert!(text.contains("highest "), "{text}");
    let dc = reqs
        .iter()
        .find(|q| q.path.contains("/api/webhooks/"))
        .unwrap()
        .json();
    let desc = dc["embeds"][0]["description"].as_str().unwrap();
    assert!(desc.chars().count() <= 4000);
    assert!(desc.contains("**Data gaps, 24 h**"), "{desc}");
    assert_eq!(dc["allowed_mentions"]["parse"].as_array().unwrap().len(), 0);
    // No secret anywhere in what was sent.
    for q in &reqs {
        assert!(!q.body.contains("SECRET"));
    }
    wait_until("report counters", || {
        r.alerts.delivery.counters("tg").is_some_and(|c| c[0] == 1)
    })
    .await;
    let mut fam = vulture_server::metrics::Families::new();
    vulture_server::metrics::MetricSource::collect(r.alerts.delivery.as_ref(), &mut fam);
    assert!(
        fam.render().contains(
            "vulture_alerts_channel_reports_sent_total{channel=\"tg\",kind=\"telegram\"} 1"
        )
    );
}

#[tokio::test]
async fn sections_and_channels_can_be_chosen_and_bad_input_is_refused() {
    let r = rig(DigestConfig::default()).await;
    r.add_telegram("tg", false).await;
    r.add_telegram("tg2", false).await;
    let out = r
        .digest
        .send_now(
            &["tg2".into()],
            &["gaps".into(), "cascades".into()],
            Some(3),
        )
        .await
        .unwrap();
    assert_eq!(out.queued, ["tg2"]);
    assert!(out.preview.contains("Data gaps") && out.preview.contains("Liquidation cascades"));
    assert!(!out.preview.contains("Open interest movers"));
    wait_until("message", || r.mock.count() == 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(r.mock.count(), 1, "only the chosen channel");
    use vulture_server::alerts::DigestError;
    assert!(matches!(
        r.digest.send_now(&["nope".into()], &[], None).await,
        Err(DigestError::Invalid(_))
    ));
    assert!(matches!(
        r.digest.send_now(&[], &["weather".into()], None).await,
        Err(DigestError::Invalid(_))
    ));
    assert!(matches!(
        r.digest.send_now(&[], &[], Some(21)).await,
        Err(DigestError::Invalid(_))
    ));
}

#[tokio::test]
async fn without_channels_there_is_nothing_to_send_to() {
    let r = rig(DigestConfig::default()).await;
    use vulture_server::alerts::DigestError;
    assert!(matches!(
        r.digest.send_now(&[], &[], None).await,
        Err(DigestError::NoChannel(_))
    ));
}

#[tokio::test]
async fn a_dry_run_channel_logs_instead_of_sending() {
    let r = rig(DigestConfig::default()).await;
    r.add_telegram("tg", true).await;
    r.digest.send_now(&[], &[], None).await.unwrap();
    wait_until("dry run", || {
        r.alerts.delivery.counters("tg").is_some_and(|c| c[6] == 1)
    })
    .await;
    assert_eq!(r.mock.count(), 0);
}

#[tokio::test]
async fn the_digest_is_a_message_of_its_own_and_alerts_keep_batching() {
    let r = rig(DigestConfig::default()).await;
    r.add_telegram("tg", false).await;
    for i in 0..3 {
        r.alerts.delivery.push(
            "tg",
            Alert {
                ts_ms: 1_761_400_992_000 + i,
                severity: Severity::Warning,
                title: "OI spike UP".into(),
                headline: format!("alert {i}"),
                fields: vec![],
            },
        );
    }
    r.digest
        .send_now(&[], &["gaps".into()], None)
        .await
        .unwrap();
    r.alerts.delivery.push(
        "tg",
        Alert {
            ts_ms: 1_761_400_999_000,
            severity: Severity::Info,
            title: "late".into(),
            headline: "after the digest".into(),
            fields: vec![],
        },
    );
    wait_until("messages", || r.mock.count() >= 2).await;
    wait_until("all", || {
        r.alerts.delivery.queue_depth("tg") == 0 && r.mock.count() >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let texts: Vec<String> = r
        .mock
        .requests()
        .iter()
        .map(|q| q.json()["text"].as_str().unwrap_or_default().to_string())
        .collect();
    let digest_msgs: Vec<_> = texts
        .iter()
        .filter(|t| t.contains("daily digest"))
        .collect();
    assert_eq!(digest_msgs.len(), 1, "{texts:#?}");
    assert!(
        !digest_msgs[0].contains("alert 0"),
        "the digest is not merged with alerts"
    );
    assert!(
        texts
            .iter()
            .any(|t| t.contains("alert 0") && t.contains("alert 2")),
        "{texts:#?}"
    );
}

#[tokio::test]
async fn send_digest_now_over_the_wire() {
    let r = rig(DigestConfig::default()).await;
    let server = Server::start_digest(
        &Backend::memory(),
        r.alerts.manager.clone(),
        r.history.clone(),
        r.digest.clone(),
    )
    .await;
    let mut anon = ControlClient::with_interceptor(server.channel().await, Auth::none());
    assert_eq!(
        anon.send_digest_now(SendDigestNowRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    let mut c =
        ControlClient::with_interceptor(server.channel().await, Auth::bearer(&server.token));
    // No channel yet.
    assert_eq!(
        c.send_digest_now(SendDigestNowRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    c.upsert_channel(UpsertChannelRequest {
        channel: Some(vulture_proto::v1::Channel {
            id: "tg".into(),
            name: "Owner Telegram".into(),
            kind: vulture_proto::v1::ChannelKind::Telegram as i32,
            enabled: true,
            chat_id: "-1001234".into(),
            ..Default::default()
        }),
        secret: TOKEN.into(),
    })
    .await
    .unwrap();
    let resp = c
        .send_digest_now(SendDigestNowRequest {
            sections: vec!["gaps".into(), "skew".into()],
            top_n: 3,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.queued_channel_ids, ["tg"]);
    assert!(
        resp.preview.contains("Data gaps, 24 h")
            && resp.preview.contains("Liquidations by exchange")
    );
    assert!(!resp.preview.contains(TOKEN));
    wait_until("the message", || r.mock.count() == 1).await;
    for (req, code) in [
        (
            SendDigestNowRequest {
                channel_ids: vec!["nope".into()],
                ..Default::default()
            },
            Code::InvalidArgument,
        ),
        (
            SendDigestNowRequest {
                sections: vec!["weather".into()],
                ..Default::default()
            },
            Code::InvalidArgument,
        ),
        (
            SendDigestNowRequest {
                top_n: 50,
                ..Default::default()
            },
            Code::InvalidArgument,
        ),
    ] {
        assert_eq!(c.send_digest_now(req).await.unwrap_err().code(), code);
    }
    server.stop().await;
}

#[tokio::test]
async fn the_schedule_fires_at_the_configured_time_once_a_day() {
    // The clock stands just before 06:00 and then jumps past it.
    let cfg = DigestConfig {
        enabled: true,
        time_utc: "06:00".into(),
        sections: vec!["gaps".into()],
        ..DigestConfig::default()
    };
    let r = rig(cfg).await;
    r.add_telegram("tg", false).await;
    let fire = (NOW / 86_400_000) * 86_400_000 + 6 * 3_600_000;
    r.clock.set(fire - 2_000);
    let task = tokio::spawn(r.digest.clone().run(r.stop.clone()));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(r.mock.count(), 0, "not before the time");
    r.clock.set(fire + 1_000);
    wait_until("the scheduled digest", || r.mock.count() == 1).await;
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    assert_eq!(r.mock.count(), 1, "once");
    let mut fam = vulture_server::metrics::Families::new();
    vulture_server::metrics::MetricSource::collect(r.digest.as_ref(), &mut fam);
    let text = fam.render();
    assert!(
        text.contains("vulture_alerts_digest_runs_total{result=\"ok\"} 1"),
        "{text}"
    );
    assert!(
        text.contains("vulture_alerts_digest_next_run_timestamp_seconds"),
        "{text}"
    );
    r.stop.cancel();
    let _ = task.await;
}

#[tokio::test]
async fn a_disabled_schedule_does_nothing_and_bad_settings_are_refused() {
    let r = rig(DigestConfig::default()).await;
    r.add_telegram("tg", false).await;
    let task = tokio::spawn(r.digest.clone().run(r.stop.clone()));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(task.is_finished(), "disabled: the task ends at once");
    assert_eq!(r.mock.count(), 0);
    for bad in [
        "[alerts.digest]\ntime_utc = \"7am\"\n",
        "[alerts.digest]\ntime_utc = \"24:00\"\n",
        "[alerts.digest]\nsections = [\"weather\"]\n",
        "[alerts.digest]\ntop_n = 0\n",
        "[alerts.digest]\nchannels = [\"Bad Id\"]\n",
    ] {
        assert!(
            Config::from_toml_with_env(bad, std::iter::empty()).is_err(),
            "{bad}"
        );
    }
    let ok = Config::from_toml_with_env("[alerts.digest]\nenabled = true\ntime_utc = \"23:59\"\nsections = [\"gaps\", \"skew\"]\ntop_n = 10\n", std::iter::empty()).unwrap();
    assert!(ok.alerts.digest.enabled);
    assert_eq!(
        ok.alerts.digest.time_of_day_secs(),
        Some(23 * 3600 + 59 * 60)
    );
}
