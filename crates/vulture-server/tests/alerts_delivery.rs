//! Delivery against local mock Telegram and Discord servers: formatting, escaping, batching, rate-limit
//! spacing, `429` with `retry_after`, failures with backoff, queue overflow, dry run, the test message and the
//! absence of secrets in logs and errors. Nothing here talks to a real service.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::log_capture::capture;
use common::mock_http::{MockHttp, Reply};
use common::wait_until;
use tokio_util::sync::CancellationToken;
use vulture_server::alerts::model::{ChannelDef, ChannelKind, Severity};
use vulture_server::alerts::text::Alert;
use vulture_server::alerts::{Delivery, PushResult};
use vulture_server::config::{DeliveryConfig, Secret};

const TOKEN: &str = "123456:SECRETtokenABCdef_ghi-JKL";

fn cfg(mock: &MockHttp) -> DeliveryConfig {
    DeliveryConfig {
        telegram_base_url: mock.url(),
        allow_custom_endpoints: true,
        telegram_min_interval_ms: 150,
        telegram_max_per_minute: 600,
        discord_min_interval_ms: 150,
        discord_max_per_minute: 600,
        batch_window_ms: 80,
        digest_max_items: 12,
        queue_limit: 500,
        retry_min_ms: 100,
        retry_max_ms: 400,
        max_attempts: 4,
        request_timeout_secs: 3,
    }
}

fn telegram(id: &str) -> ChannelDef {
    ChannelDef {
        id: id.into(),
        name: "Telegram test".into(),
        kind: ChannelKind::Telegram,
        enabled: true,
        system: false,
        dry_run: false,
        chat_id: "-1001234".into(),
        secret: Some(Secret::new(TOKEN)),
        secret_file: String::new(),
        secret_env: String::new(),
        min_interval_ms: 0,
        batch_window_ms: 0,
        queue_limit: 0,
        quiet_hours: Vec::new(),
    }
}

fn discord(id: &str, mock: &MockHttp) -> ChannelDef {
    ChannelDef {
        kind: ChannelKind::Discord,
        chat_id: String::new(),
        secret: Some(Secret::new(format!(
            "{}/api/webhooks/42/WEBHOOKtokenXYZ123",
            mock.url()
        ))),
        name: "Discord test".into(),
        ..telegram(id)
    }
}

fn alert(n: usize, headline: &str) -> Alert {
    Alert {
        ts_ms: 1_761_400_992_000 + n as i64,
        severity: Severity::Warning,
        title: "OI spike UP".into(),
        headline: headline.into(),
        fields: vec![("window".into(), "5m".into())],
    }
}

struct Rig {
    mock: MockHttp,
    delivery: Arc<Delivery>,
    stop: CancellationToken,
}

async fn rig(default: Reply, tweak: impl FnOnce(&mut DeliveryConfig)) -> Rig {
    let mock = MockHttp::start(default).await;
    let mut c = cfg(&mock);
    tweak(&mut c);
    let stop = CancellationToken::new();
    let delivery = Delivery::new(c, stop.clone()).unwrap();
    Rig {
        mock,
        delivery,
        stop,
    }
}

impl Rig {
    fn counters(&self, id: &str) -> [u64; 7] {
        self.delivery.counters(id).unwrap()
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

#[tokio::test]
async fn telegram_message_is_formatted_and_escaped() {
    let r = rig(Reply::ok(), |_| {}).await;
    r.delivery.apply(&[telegram("tg")]);
    assert_eq!(
        r.delivery
            .push("tg", alert(0, "BTC <b>&</b> on binance: +4.20% in 5m")),
        PushResult::Queued
    );
    wait_until("the message", || r.mock.count() == 1).await;
    let req = &r.mock.requests()[0];
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, format!("/bot{TOKEN}/sendMessage"));
    assert!(
        req.headers
            .iter()
            .any(|(k, v)| k == "content-type" && v.starts_with("application/json"))
    );
    let j = req.json();
    assert_eq!(j["chat_id"], "-1001234");
    assert_eq!(j["parse_mode"], "HTML");
    let text = j["text"].as_str().unwrap();
    assert!(text.contains("<b>OI spike UP</b>"), "{text}");
    assert!(
        text.contains("BTC &lt;b&gt;&amp;&lt;/b&gt; on binance"),
        "{text}"
    );
    assert!(text.contains("<code>5m</code>"));
    assert!(text.contains("2025-10-25 14:03:12 UTC"), "{text}");
    wait_until("the counter", || r.counters("tg")[0] == 1).await;
    assert_eq!(r.counters("tg")[1], 1);
}

#[tokio::test]
async fn discord_message_is_an_embed_without_mentions() {
    let r = rig(Reply::no_content(), |_| {}).await;
    r.delivery.apply(&[discord("dc", &r.mock)]);
    r.delivery.push("dc", alert(0, "@everyone *loud* _x_"));
    wait_until("the message", || r.mock.count() == 1).await;
    let req = &r.mock.requests()[0];
    assert_eq!(req.path, "/api/webhooks/42/WEBHOOKtokenXYZ123");
    let j = req.json();
    assert_eq!(j["allowed_mentions"]["parse"], serde_json::json!([]));
    let desc = j["embeds"][0]["description"].as_str().unwrap();
    assert!(
        !desc.contains("@everyone") && desc.contains("\\*loud\\*"),
        "{desc}"
    );
    assert_eq!(j["embeds"][0]["title"], "OI spike UP");
    assert_eq!(j["embeds"][0]["timestamp"], "2025-10-25T14:03:12Z");
}

#[tokio::test]
async fn a_burst_inside_the_batch_window_is_one_digest() {
    let r = rig(Reply::ok(), |c| c.batch_window_ms = 300).await;
    r.delivery.apply(&[telegram("tg")]);
    for i in 0..7 {
        r.delivery.push("tg", alert(i, &format!("symbol {i}")));
    }
    wait_until("the digest", || r.mock.count() >= 1).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(r.mock.count(), 1, "one message for the whole burst");
    let text = r.mock.requests()[0].json()["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(text.contains("7 alerts"), "{text}");
    assert!(text.contains("symbol 0") && text.contains("symbol 6"));
    assert_eq!(r.counters("tg")[1], 7);
}

#[tokio::test]
async fn digest_lists_a_few_and_counts_the_rest() {
    let r = rig(Reply::ok(), |c| {
        c.batch_window_ms = 300;
        c.digest_max_items = 3;
    })
    .await;
    r.delivery.apply(&[telegram("tg")]);
    for i in 0..10 {
        r.delivery.push("tg", alert(i, &format!("symbol {i}")));
    }
    wait_until("the digest", || r.mock.count() == 1).await;
    let text = r.mock.requests()[0].json()["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.contains("10 alerts") && text.contains("and 7 more"),
        "{text}"
    );
}

#[tokio::test]
async fn messages_keep_the_rate_limit_spacing_and_nothing_is_lost() {
    let r = rig(Reply::ok(), |c| {
        c.batch_window_ms = 20;
        c.telegram_min_interval_ms = 250;
    })
    .await;
    r.delivery.apply(&[telegram("tg")]);
    // A cascade: 120 alerts within about 600 ms.
    for i in 0..120 {
        r.delivery.push("tg", alert(i, &format!("liquidation {i}")));
        if i % 20 == 19 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    wait_until("everything delivered", || r.counters("tg")[1] == 120).await;
    let reqs = r.mock.requests();
    assert!(
        reqs.len() >= 2 && reqs.len() <= 8,
        "{} messages",
        reqs.len()
    );
    for w in reqs.windows(2) {
        let gap = w[1].at.duration_since(w[0].at);
        assert!(
            gap >= Duration::from_millis(230),
            "{gap:?} between two messages"
        );
    }
    // The totals add up: nothing lost to the digest.
    let total: usize = reqs
        .iter()
        .map(|q| {
            let text = q.json()["text"].as_str().unwrap().to_string();
            if let Some(i) = text.find(" alerts</b>") {
                let head = &text[..i];
                head.rsplit("<b>").next().unwrap().parse::<usize>().unwrap()
            } else {
                1
            }
        })
        .sum();
    assert_eq!(total, 120);
    assert_eq!(r.counters("tg")[5], 0, "no 429");
}

#[tokio::test]
async fn telegram_429_waits_retry_after_and_resends() {
    let r = rig(Reply::ok(), |_| {}).await;
    r.mock.script([Reply::status(
        429,
        r#"{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 1","parameters":{"retry_after":1}}"#,
    )]);
    r.delivery.apply(&[telegram("tg")]);
    r.delivery.push("tg", alert(0, "once"));
    wait_until("the retry", || r.mock.count() == 2).await;
    let reqs = r.mock.requests();
    let gap = reqs[1].at.duration_since(reqs[0].at);
    assert!(gap >= Duration::from_millis(950), "waited only {gap:?}");
    assert_eq!(reqs[0].body, reqs[1].body, "the same message is sent again");
    wait_until("accepted", || r.counters("tg")[0] == 1).await;
    let c = r.counters("tg");
    assert_eq!(c[5], 1, "one 429 counted");
    assert_eq!(c[3], 0, "nothing dropped");
}

#[tokio::test]
async fn discord_429_uses_the_json_retry_after() {
    let r = rig(Reply::no_content(), |_| {}).await;
    r.mock.script([Reply::status(
        429,
        r#"{"message":"You are being rate limited.","retry_after":0.4,"global":false}"#,
    )
    .header("retry-after", "1")]);
    r.delivery.apply(&[discord("dc", &r.mock)]);
    r.delivery.push("dc", alert(0, "once"));
    wait_until("the retry", || r.mock.count() == 2).await;
    let reqs = r.mock.requests();
    let gap = reqs[1].at.duration_since(reqs[0].at);
    assert!(
        gap >= Duration::from_millis(380) && gap < Duration::from_millis(900),
        "{gap:?}"
    );
}

#[tokio::test]
async fn failures_back_off_exponentially_then_succeed() {
    let r = rig(Reply::ok(), |c| {
        c.retry_min_ms = 200;
        c.retry_max_ms = 2000;
    })
    .await;
    r.mock
        .script([Reply::status(500, "{}"), Reply::status(502, "{}")]);
    r.delivery.apply(&[telegram("tg")]);
    r.delivery.push("tg", alert(0, "flaky"));
    wait_until("three attempts", || r.mock.count() == 3).await;
    let reqs = r.mock.requests();
    let g1 = reqs[1].at.duration_since(reqs[0].at);
    let g2 = reqs[2].at.duration_since(reqs[1].at);
    // Equal jitter: at least half of the nominal 200 ms and 400 ms.
    assert!(g1 >= Duration::from_millis(95), "{g1:?}");
    assert!(g2 >= Duration::from_millis(190), "{g2:?}");
    wait_until("accepted", || r.counters("tg")[0] == 1).await;
    let c = r.counters("tg");
    assert_eq!((c[2], c[4]), (2, 2), "two failures, two retries");
}

#[tokio::test]
async fn a_message_is_given_up_after_the_attempts_and_the_loss_is_reported() {
    let r = rig(Reply::status(500, "{}"), |c| {
        c.max_attempts = 3;
        c.retry_min_ms = 20;
        c.retry_max_ms = 50;
    })
    .await;
    r.delivery.apply(&[telegram("tg")]);
    r.delivery.push("tg", alert(0, "lost one"));
    wait_until("three attempts", || r.mock.count() == 3).await;
    wait_until("given up", || r.counters("tg")[3] == 1).await;
    // The endpoint recovers: the next message says one was lost.
    r.mock.set_default(Reply::ok());
    r.delivery.push("tg", alert(1, "after recovery"));
    wait_until("the next message", || r.mock.count() == 4).await;
    let text = r.mock.requests()[3].json()["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(text.contains("1 messages dropped"), "{text}");
}

#[tokio::test]
async fn a_permanent_error_is_not_retried() {
    let r = rig(
        Reply::status(
            401,
            r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#,
        ),
        |_| {},
    )
    .await;
    r.delivery.apply(&[telegram("tg")]);
    r.delivery.push("tg", alert(0, "refused"));
    wait_until("one attempt", || r.mock.count() == 1).await;
    wait_until("dropped", || r.counters("tg")[3] == 1).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(r.mock.count(), 1);
    assert_eq!(r.counters("tg")[4], 0, "no retry");
}

#[tokio::test]
async fn queue_overflow_drops_the_oldest_and_says_so() {
    let r = rig(Reply::ok(), |c| {
        c.batch_window_ms = 400;
        c.queue_limit = 5;
    })
    .await;
    r.delivery.apply(&[telegram("tg")]);
    let mut results = Vec::new();
    for i in 0..20 {
        results.push(r.delivery.push("tg", alert(i, &format!("item {i}"))));
    }
    assert!(results.contains(&PushResult::QueuedDroppedOldest));
    assert!(r.delivery.queue_depth("tg") <= 5);
    wait_until("the digest", || r.mock.count() == 1).await;
    let text = r.mock.requests()[0].json()["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(text.contains("5 alerts"), "{text}");
    assert!(text.contains("15 messages dropped"), "{text}");
    assert!(
        text.contains("item 19") && !text.contains("item 0 "),
        "the newest are kept: {text}"
    );
    assert_eq!(r.counters("tg")[3], 15);
}

#[tokio::test]
async fn dry_run_logs_and_sends_nothing() {
    let (logs, _guard) = capture();
    let r = rig(Reply::ok(), |_| {}).await;
    let mut def = telegram("tg");
    def.dry_run = true;
    r.delivery.apply(&[def]);
    r.delivery.push("tg", alert(0, "would be sent"));
    wait_until("dry run counted", || r.counters("tg")[6] == 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(r.mock.count(), 0);
    let text = logs.text();
    assert!(
        text.contains("dry run") && text.contains("would be sent"),
        "{text}"
    );
    assert!(!text.contains(TOKEN), "the token must not be logged");
}

#[tokio::test]
async fn disabled_and_unknown_channels_take_nothing() {
    let r = rig(Reply::ok(), |_| {}).await;
    let mut off = telegram("off");
    off.enabled = false;
    r.delivery.apply(&[off]);
    assert_eq!(r.delivery.push("off", alert(0, "x")), PushResult::Disabled);
    assert_eq!(r.delivery.push("nope", alert(0, "x")), PushResult::Unknown);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(r.mock.count(), 0);
}

#[tokio::test]
async fn test_message_reports_the_outcome_without_secrets() {
    let (logs, _guard) = capture();
    let r = rig(Reply::ok(), |_| {}).await;
    let ok = r.delivery.send_test(&telegram("tg")).await;
    assert!(ok.delivered && !ok.dry_run && ok.detail == "ok", "{ok:?}");
    assert_eq!(r.mock.count(), 1);
    assert!(
        r.mock.requests()[0].json()["text"]
            .as_str()
            .unwrap()
            .contains("test message")
    );

    r.mock.set_default(Reply::status(
        401,
        r#"{"ok":false,"description":"Unauthorized"}"#,
    ));
    let bad = r.delivery.send_test(&telegram("tg")).await;
    assert!(!bad.delivered && bad.detail.contains("HTTP 401"), "{bad:?}");
    assert!(!bad.detail.contains(TOKEN));

    // Nothing listens: the transport error must not echo the URL that holds the token.
    let mut c = cfg(&r.mock);
    c.telegram_base_url = "http://127.0.0.1:1".into();
    let d = Delivery::new(c, CancellationToken::new()).unwrap();
    let down = d.send_test(&telegram("tg")).await;
    assert!(!down.delivered);
    assert!(
        !down.detail.contains(TOKEN) && !down.detail.contains("123456"),
        "{down:?}"
    );

    let mut dry = telegram("tg");
    dry.dry_run = true;
    let before = r.mock.count();
    let dr = r.delivery.send_test(&dry).await;
    assert!(dr.dry_run && !dr.delivered);
    assert_eq!(r.mock.count(), before);
    assert!(!logs.text().contains(TOKEN));
}

#[tokio::test]
async fn failures_never_put_the_secret_into_the_log() {
    let (logs, _guard) = capture();
    let r = rig(Reply::status(500, "{}"), |c| {
        c.max_attempts = 2;
        c.retry_min_ms = 20;
        c.retry_max_ms = 40;
    })
    .await;
    // Point Discord at a closed port as well: connection errors carry the URL in some stacks.
    r.delivery.apply(&[telegram("tg")]);
    r.delivery.push("tg", alert(0, "fails"));
    wait_until("given up", || r.counters("tg")[3] == 1).await;
    let text = logs.text();
    assert!(text.contains("giving up"), "{text}");
    assert!(
        !text.contains(TOKEN) && !text.contains("SECRETtoken"),
        "{text}"
    );
}

#[tokio::test]
async fn secret_from_a_file_is_read_when_sending() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    std::fs::write(&path, format!("{TOKEN}\n")).unwrap();
    let r = rig(Reply::ok(), |_| {}).await;
    let mut def = telegram("tg");
    def.secret = None;
    def.secret_file = path.to_string_lossy().into();
    r.delivery.apply(&[def]);
    r.delivery.push("tg", alert(0, "from a file"));
    wait_until("the message", || r.mock.count() == 1).await;
    assert_eq!(
        r.mock.requests()[0].path,
        format!("/bot{TOKEN}/sendMessage")
    );
}

#[tokio::test]
async fn metrics_report_per_channel_series() {
    use vulture_server::metrics::{Families, MetricSource};
    let r = rig(Reply::ok(), |_| {}).await;
    r.delivery.apply(&[telegram("tg")]);
    r.delivery.push("tg", alert(0, "m"));
    wait_until("sent", || r.counters("tg")[0] == 1).await;
    let mut f = Families::new();
    r.delivery.collect(&mut f);
    let text = f.render();
    for needle in [
        "vulture_alerts_channel_sent_total{channel=\"tg\",kind=\"telegram\"} 1",
        "vulture_alerts_channel_failed_total{channel=\"tg\",kind=\"telegram\"} 0",
        "vulture_alerts_channel_dropped_total{channel=\"tg\",kind=\"telegram\"} 0",
        "vulture_alerts_channel_queue_depth{channel=\"tg\",kind=\"telegram\"} 0",
        "vulture_alerts_channel_last_success_age_seconds{channel=\"tg\",kind=\"telegram\"}",
    ] {
        assert!(text.contains(needle), "missing {needle} in\n{text}");
    }
    assert!(!text.contains(TOKEN));
}

#[tokio::test]
async fn a_secret_file_with_the_wrong_shape_is_never_put_into_a_url() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("not-a-token");
    std::fs::write(&path, "root:x:0:0:/root:/bin/sh\nsecond line\n").unwrap();
    let r = rig(Reply::ok(), |_| {}).await;
    let mut def = telegram("tg");
    def.secret = None;
    def.secret_file = path.to_string_lossy().into();
    let out = r.delivery.send_test(&def).await;
    assert!(
        !out.delivered && out.detail.contains("unexpected shape"),
        "{out:?}"
    );
    assert!(!out.detail.contains("root"));
    assert_eq!(r.mock.count(), 0, "nothing was sent");
}
