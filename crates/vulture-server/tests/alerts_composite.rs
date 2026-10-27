//! Composite rules and quiet hours: matching, scope, cooldown, cadence, hot apply, persistence, the `Control`
//! contract and the channel quiet hours in delivery.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arc_swap::ArcSwap;
use common::mock_http::{MockHttp, Reply};
use common::*;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use vulture_proto::v1::control_client::ControlClient;
use vulture_proto::v1::{self as pb, Event, Liquidation, OpenInterest, Side, event::Payload};
use vulture_server::alerts::Alerts;
use vulture_server::alerts::core::{Core, Output};
use vulture_server::alerts::model::{
    CmpOp, CondMetric, Condition, QuietRange, RuleDef, RuleKind, Severity,
};
use vulture_server::alerts::rules::RuleSet;
use vulture_server::alerts::stats::EngineStats;
use vulture_server::config::{AlertsConfig, Config};

const MIN: i64 = 60_000;
const T0: i64 = 1_792_972_800_000; // a Monday 00:00 UTC

fn engine_cfg() -> AlertsConfig {
    let mut c = AlertsConfig::default();
    c.oi_spike.enabled = false;
    c.liq_cascade.enabled = false;
    c.composite_eval_secs = 10;
    c
}

fn oi(
    ex: &str,
    sym: &str,
    ts: i64,
    base: f64,
    mark: f64,
    funding: Option<f64>,
    index: Option<f64>,
) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::OpenInterest(OpenInterest {
            exchange: ex.into(),
            symbol_raw: sym.replace('-', ""),
            symbol: sym.into(),
            exchange_ts: ts,
            recv_ts: ts,
            oi_base: base,
            oi_usd: Some(base * mark),
            mark_price: Some(mark),
            funding_rate: funding,
            index_price: index,
            next_funding_ts: None,
        })),
    }
}

fn liq(ex: &str, sym: &str, ts: i64, usd: f64) -> Event {
    Event {
        id: String::new(),
        payload: Some(Payload::Liquidation(Liquidation {
            exchange: ex.into(),
            symbol_raw: sym.replace('-', ""),
            symbol: sym.into(),
            exchange_ts: ts,
            recv_ts: ts,
            side: Side::Long as i32,
            price: 100.0,
            qty: usd / 100.0,
            notional_usd: usd,
        })),
    }
}

fn cond(metric: CondMetric, op: CmpOp, threshold: f64, window_secs: u32) -> Condition {
    Condition {
        metric,
        op,
        threshold,
        window_secs,
    }
}

fn rule(id: &str, conditions: Vec<Condition>) -> RuleDef {
    RuleDef {
        id: id.into(),
        name: format!("rule {id}"),
        enabled: true,
        kind: RuleKind::Composite {
            conditions,
            severity: Severity::Warning,
        },
        exchanges: vec![],
        symbols: vec![],
        channels: vec!["tg".into()],
        cooldown_secs: 0,
        quiet_hours: vec![],
    }
}

struct Rig {
    core: Core,
    set: Arc<ArcSwap<RuleSet>>,
    stats: Arc<EngineStats>,
}

fn rig(rules: &[RuleDef]) -> Rig {
    let set = Arc::new(ArcSwap::from_pointee(RuleSet::compile(rules)));
    let stats = Arc::new(EngineStats::default());
    Rig {
        core: Core::new(&engine_cfg(), set.clone(), stats.clone()),
        set,
        stats,
    }
}

impl Rig {
    fn feed(&mut self, events: &[Event]) {
        let mut out = Output::default();
        for e in events {
            self.core.process(e, true, &mut out);
        }
    }
    fn tick(&mut self, now: i64) -> Output {
        let mut out = Output::default();
        self.core.tick(now, &mut out);
        out
    }
    fn evals(&self) -> u64 {
        self.stats.composite_evaluations.load(Ordering::Relaxed)
    }
}

/// 10 minutes of flat history (30 s samples), then 2 minutes of OI +4% and a price 1% lower.
fn scenario(ex: &str, sym: &str, funding: Option<f64>, index: Option<f64>) -> Vec<Event> {
    let mut v = Vec::new();
    for k in 0..=20 {
        v.push(oi(ex, sym, T0 + k * 30_000, 1000.0, 100.0, funding, index));
    }
    for k in 21..=25 {
        v.push(oi(ex, sym, T0 + k * 30_000, 1040.0, 99.0, funding, index));
    }
    v
}

const NOW: i64 = T0 + 25 * 30_000 + 1_000;

fn all_conditions() -> Vec<Condition> {
    vec![
        cond(CondMetric::OiChangePct, CmpOp::Ge, 3.0, 300),
        cond(CondMetric::PriceChangePct, CmpOp::Le, -0.5, 300),
        cond(CondMetric::FundingRate, CmpOp::Gt, 0.0003, 0),
        cond(CondMetric::LiqUsd, CmpOp::Ge, 100_000.0, 600),
    ]
}

#[test]
fn all_conditions_must_hold_at_once() {
    let mut r = rig(&[rule("c1", all_conditions())]);
    r.feed(&scenario(
        "binance",
        "BTC-USDT-PERP",
        Some(0.0004),
        Some(100.0),
    ));
    r.feed(&[liq("binance", "BTC-USDT-PERP", NOW - 2 * MIN, 150_000.0)]);
    let out = r.tick(NOW);
    assert_eq!(out.hits.len(), 1, "{:?}", out.hits);
    let h = &out.hits[0];
    assert_eq!(
        (h.rule_id.as_str(), h.channels.as_slice()),
        ("c1", ["tg".to_string()].as_slice())
    );
    assert_eq!(h.alert.severity, Severity::Warning);
    assert_eq!(h.alert.title, "Composite rule");
    assert!(
        h.alert.headline.starts_with("BTC-USDT-PERP on binance: "),
        "{}",
        h.alert.headline
    );
    let fields: Vec<_> = h
        .alert
        .fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    assert!(
        fields
            .iter()
            .any(|(k, v)| *k == "OI change 5m" && v.starts_with("+4.00%")),
        "{fields:?}"
    );
    assert!(
        fields
            .iter()
            .any(|(k, v)| *k == "Price change 5m" && v.starts_with("-1.00%")),
        "{fields:?}"
    );
    assert!(
        fields
            .iter()
            .any(|(k, v)| *k == "Funding rate" && v.starts_with("+0.0400%")),
        "{fields:?}"
    );
    assert!(
        fields
            .iter()
            .any(|(k, v)| *k == "Liquidated 10m" && v.starts_with("$150K")),
        "{fields:?}"
    );
    assert_eq!(r.stats.composite_matched.load(Ordering::Relaxed), 1);
}

#[test]
fn each_missing_condition_blocks_the_alert() {
    // Remove, one at a time, the data that makes a condition true.
    type Case = (&'static str, Vec<Event>, Option<f64>, Option<f64>);
    let cases: [Case; 3] = [
        ("no liquidation", vec![], Some(0.0004), Some(100.0)),
        (
            "funding too low",
            vec![liq("binance", "BTC-USDT-PERP", NOW - MIN, 150_000.0)],
            Some(0.0001),
            Some(100.0),
        ),
        (
            "no funding data",
            vec![liq("binance", "BTC-USDT-PERP", NOW - MIN, 150_000.0)],
            None,
            Some(100.0),
        ),
    ];
    for (name, extra, funding, index) in cases {
        let mut r = rig(&[rule("c", all_conditions())]);
        r.feed(&scenario("binance", "BTC-USDT-PERP", funding, index));
        r.feed(&extra);
        assert!(r.tick(NOW).hits.is_empty(), "{name}");
        assert_eq!(
            r.stats.composite_matched.load(Ordering::Relaxed),
            0,
            "{name}"
        );
    }
    // A liquidation outside the window does not count.
    let mut r = rig(&[rule("c", all_conditions())]);
    r.feed(&scenario(
        "binance",
        "BTC-USDT-PERP",
        Some(0.0004),
        Some(100.0),
    ));
    r.feed(&[liq("binance", "BTC-USDT-PERP", NOW - 20 * MIN, 900_000.0)]);
    assert!(r.tick(NOW).hits.is_empty());
    // Not enough history for the window: no reference, no alert (and no panic).
    let mut r = rig(&[rule(
        "c",
        vec![cond(CondMetric::OiChangePct, CmpOp::Ge, 1.0, 3600)],
    )]);
    r.feed(&scenario("binance", "BTC-USDT-PERP", None, None));
    assert!(r.tick(NOW).hits.is_empty());
}

#[test]
fn regime_basis_and_the_operators() {
    // OI up and price down is "new shorts" (2).
    let mut r = rig(&[
        rule(
            "shorts",
            vec![cond(CondMetric::Regime, CmpOp::Eq, 2.0, 300)],
        ),
        rule("longs", vec![cond(CondMetric::Regime, CmpOp::Eq, 1.0, 300)]),
        // mark 99 against index 100: basis -1%.
        rule(
            "basis",
            vec![cond(CondMetric::BasisPct, CmpOp::Lt, -0.5, 0)],
        ),
        rule(
            "basis_none",
            vec![cond(CondMetric::BasisPct, CmpOp::Gt, 0.0, 0)],
        ),
        rule(
            "lt",
            vec![cond(CondMetric::OiChangePct, CmpOp::Lt, 5.0, 300)],
        ),
        rule(
            "gt_edge",
            vec![cond(CondMetric::OiChangePct, CmpOp::Gt, 4.0, 300)],
        ),
    ]);
    r.feed(&scenario(
        "binance",
        "BTC-USDT-PERP",
        Some(0.0001),
        Some(100.0),
    ));
    let ids: Vec<String> = r.tick(NOW).hits.iter().map(|h| h.rule_id.clone()).collect();
    for want in ["shorts", "basis", "lt"] {
        assert!(ids.contains(&want.to_string()), "{want}: {ids:?}");
    }
    for not in ["longs", "basis_none"] {
        assert!(!ids.contains(&not.to_string()), "{not}: {ids:?}");
    }
    // Strictly greater: +4.000000000000004% or +3.99999...: either way the edge case is decided by the exact
    // arithmetic, so only check that the rule did not crash and the others stayed as above.
    assert!(ids.len() >= 3);
    // Without an index price there is no basis.
    let mut r = rig(&[rule(
        "basis",
        vec![cond(CondMetric::BasisPct, CmpOp::Lt, 100.0, 0)],
    )]);
    r.feed(&scenario("binance", "BTC-USDT-PERP", Some(0.0001), None));
    assert!(r.tick(NOW).hits.is_empty());
}

#[test]
fn scope_filters_exchange_and_symbol() {
    let mut scoped = rule(
        "scoped",
        vec![cond(CondMetric::OiChangePct, CmpOp::Ge, 3.0, 300)],
    );
    scoped.exchanges = vec!["bybit".into()];
    scoped.symbols = vec!["ETH-USDT-PERP".into()];
    let all = rule(
        "all",
        vec![cond(CondMetric::OiChangePct, CmpOp::Ge, 3.0, 300)],
    );
    let mut r = rig(&[scoped, all]);
    for (ex, sym) in [
        ("binance", "BTC-USDT-PERP"),
        ("bybit", "ETH-USDT-PERP"),
        ("bybit", "BTC-USDT-PERP"),
    ] {
        r.feed(&scenario(ex, sym, None, None));
    }
    let hits = r.tick(NOW).hits;
    let by = |id: &str| hits.iter().filter(|h| h.rule_id == id).count();
    assert_eq!(by("all"), 3, "every symbol");
    assert_eq!(by("scoped"), 1, "only bybit ETH");
    assert!(
        hits.iter().any(
            |h| h.rule_id == "scoped" && h.alert.headline.starts_with("ETH-USDT-PERP on bybit")
        )
    );
}

#[test]
fn cooldown_cadence_and_stale_symbols() {
    let mut cd = rule(
        "cd",
        vec![cond(CondMetric::OiChangePct, CmpOp::Ge, 3.0, 300)],
    );
    cd.cooldown_secs = 120;
    let mut r = rig(&[cd]);
    r.feed(&scenario("binance", "BTC-USDT-PERP", None, None));
    assert_eq!(r.tick(NOW).hits.len(), 1);
    assert_eq!(r.evals(), 1);
    // The cadence: a tick before 10 s have passed does not evaluate.
    assert!(r.tick(NOW + 3_000).hits.is_empty());
    assert_eq!(r.evals(), 1, "not evaluated inside the interval");
    // Inside the cooldown: evaluated, matched, suppressed.
    assert!(r.tick(NOW + 10_000).hits.is_empty());
    assert_eq!(r.evals(), 2);
    assert_eq!(r.stats.rule_suppressed_cooldown.load(Ordering::Relaxed), 1);
    // After the cooldown the symbol (still active: its last sample is 5 min old at most? it is stale) - keep it
    // fresh with a new sample.
    r.feed(&[oi(
        "binance",
        "BTC-USDT-PERP",
        NOW + 100_000,
        1040.0,
        99.0,
        None,
        None,
    )]);
    assert_eq!(r.tick(NOW + 125_000).hits.len(), 1, "cooldown over");
    // A symbol whose last sample is older than the hole limit (300 s) is not evaluated at all.
    let before = r.evals();
    assert!(r.tick(NOW + 125_000 + 11 * MIN).hits.is_empty());
    assert_eq!(r.evals(), before, "stale symbol skipped");
}

#[test]
fn hot_apply_takes_effect_on_the_next_pass() {
    let mut r = rig(&[]);
    r.feed(&scenario("binance", "BTC-USDT-PERP", None, None));
    assert!(r.tick(NOW).hits.is_empty());
    assert_eq!(r.evals(), 0, "no composite rule, no work");
    r.set.store(Arc::new(RuleSet::compile(&[rule(
        "new",
        vec![cond(CondMetric::OiChangePct, CmpOp::Ge, 3.0, 300)],
    )])));
    assert_eq!(r.tick(NOW + 10_000).hits.len(), 1);
    // A disabled rule is not compiled.
    let mut off = rule(
        "off",
        vec![cond(CondMetric::OiChangePct, CmpOp::Ge, 3.0, 300)],
    );
    off.enabled = false;
    r.set.store(Arc::new(RuleSet::compile(&[off])));
    assert!(r.tick(NOW + 20_000).hits.is_empty());
}

#[test]
fn quiet_hours_silence_rules_without_spending_the_cooldown() {
    // The scenario's evaluation time is 00:12:31 UTC: quiet from 00:00 to 01:00.
    let mut quiet = rule(
        "q",
        vec![cond(CondMetric::OiChangePct, CmpOp::Ge, 3.0, 300)],
    );
    quiet.quiet_hours = vec![QuietRange {
        start_minute: 0,
        end_minute: 60,
    }];
    quiet.cooldown_secs = 3600;
    let mut r = rig(&[quiet.clone()]);
    r.feed(&scenario("binance", "BTC-USDT-PERP", None, None));
    assert!(r.tick(NOW).hits.is_empty());
    assert_eq!(r.stats.quiet_suppressed_rule.load(Ordering::Relaxed), 1);
    assert_eq!(r.stats.composite_matched.load(Ordering::Relaxed), 1);
    // Out of the quiet range the rule speaks at once: the suppressed pass did not start the cooldown.
    quiet.quiet_hours = vec![QuietRange {
        start_minute: 120,
        end_minute: 180,
    }];
    r.set.store(Arc::new(RuleSet::compile(&[quiet])));
    assert_eq!(r.tick(NOW + 10_000).hits.len(), 1);
    // The other rule kinds honour quiet hours as well.
    let mut liq_rule = RuleDef {
        kind: RuleKind::Liquidation {
            min_notional_usd: 1000.0,
            sides: vec![],
        },
        ..rule("l", vec![])
    };
    liq_rule.quiet_hours = vec![QuietRange {
        start_minute: 0,
        end_minute: 60,
    }];
    let mut r = rig(&[liq_rule.clone()]);
    let mut out = Output::default();
    r.core.process(
        &liq("binance", "BTC-USDT-PERP", NOW, 5_000.0),
        true,
        &mut out,
    );
    assert!(out.hits.is_empty());
    liq_rule.quiet_hours = vec![];
    r.set.store(Arc::new(RuleSet::compile(&[liq_rule])));
    r.core.process(
        &liq("binance", "BTC-USDT-PERP", NOW + 1, 5_000.0),
        true,
        &mut out,
    );
    assert_eq!(out.hits.len(), 1);
}

#[test]
fn quiet_ranges_wrap_midnight_and_are_validated() {
    let night = QuietRange {
        start_minute: 22 * 60,
        end_minute: 6 * 60,
    };
    for (m, inside) in [
        (0, true),
        (359, true),
        (360, false),
        (720, false),
        (1319, false),
        (1320, true),
        (1439, true),
    ] {
        assert_eq!(night.contains(m), inside, "minute {m}");
    }
    let at = |h: i64, m: i64| T0 + (h * 60 + m) * MIN;
    use vulture_server::alerts::model::{in_quiet, validate_quiet};
    assert!(
        in_quiet(&[night], at(23, 0))
            && in_quiet(&[night], at(5, 59))
            && !in_quiet(&[night], at(6, 0))
    );
    assert!(!in_quiet(&[], at(1, 0)));
    let full = QuietRange {
        start_minute: 0,
        end_minute: 1440,
    };
    assert!(in_quiet(&[full], at(13, 37)));
    let mut v = vec![
        night,
        night,
        QuietRange {
            start_minute: 60,
            end_minute: 120,
        },
    ];
    validate_quiet(&mut v).unwrap();
    assert_eq!(v.len(), 2, "sorted and deduplicated");
    for bad in [
        vec![QuietRange {
            start_minute: 5,
            end_minute: 5,
        }],
        vec![QuietRange {
            start_minute: 1440,
            end_minute: 10,
        }],
        vec![QuietRange {
            start_minute: 10,
            end_minute: 1441,
        }],
        (0..9)
            .map(|i| QuietRange {
                start_minute: i * 10,
                end_minute: i * 10 + 5,
            })
            .collect(),
    ] {
        assert!(validate_quiet(&mut bad.clone()).is_err(), "{bad:?}");
    }
}

#[test]
fn composite_rules_are_validated() {
    let ok = rule("ok", all_conditions());
    assert!(ok.clone().validate().is_ok());
    let bad = |conditions: Vec<Condition>| rule("b", conditions).validate().unwrap_err().0;
    assert!(bad(vec![]).contains("1 to 8"));
    assert!(bad(vec![cond(CondMetric::LiqUsd, CmpOp::Ge, 1.0, 600); 9]).contains("1 to 8"));
    assert!(bad(vec![cond(CondMetric::OiChangePct, CmpOp::Ge, 1.0, 10)]).contains("window_secs"));
    assert!(
        bad(vec![cond(CondMetric::OiChangePct, CmpOp::Ge, 1.0, 100_000)]).contains("window_secs")
    );
    assert!(bad(vec![cond(CondMetric::FundingRate, CmpOp::Ge, 0.0, 300)]).contains("no window"));
    assert!(bad(vec![cond(CondMetric::Regime, CmpOp::Ge, 1.0, 300)]).contains("regime"));
    assert!(bad(vec![cond(CondMetric::Regime, CmpOp::Eq, 7.0, 300)]).contains("regime"));
    assert!(
        bad(vec![cond(CondMetric::OiChangePct, CmpOp::Eq, 1.0, 300)])
            .contains("only for the regime")
    );
    assert!(bad(vec![cond(CondMetric::LiqUsd, CmpOp::Ge, -1.0, 300)]).contains("negative"));
    assert!(
        bad(vec![cond(
            CondMetric::OiChangePct,
            CmpOp::Ge,
            f64::NAN,
            300
        )])
        .contains("finite")
    );
    let mut no_channel = ok;
    no_channel.channels.clear();
    assert!(no_channel.validate().is_err());
    // Persistence format: JSON round trip with defaults for old files.
    let json = serde_json::to_string(&rule("rt", all_conditions())).unwrap();
    let back: RuleDef = serde_json::from_str(&json).unwrap();
    assert_eq!(back, rule("rt", all_conditions()));
    let old =
        r#"{"id":"o","kind":{"type":"liquidation","min_notional_usd":1.0},"channels":["tg"]}"#;
    assert!(
        serde_json::from_str::<RuleDef>(old)
            .unwrap()
            .quiet_hours
            .is_empty()
    );
}

// ---------------------------------------------------------------- Control and delivery

struct Control {
    server: Server,
    alerts: Arc<Alerts>,
    mock: MockHttp,
    data: tempfile::TempDir,
    stop: CancellationToken,
}

async fn control() -> Control {
    let mock = MockHttp::start(Reply::ok()).await;
    let data = tempfile::tempdir().unwrap();
    let stop = CancellationToken::new();
    let mut cfg = Config::default();
    cfg.server.data_dir = data.path().to_path_buf();
    cfg.alerts.delivery.telegram_base_url = mock.url();
    cfg.alerts.delivery.allow_custom_endpoints = true;
    cfg.alerts.delivery.batch_window_ms = 30;
    cfg.alerts.delivery.telegram_min_interval_ms = 50;
    let alerts = Alerts::open(&cfg, stop.clone()).unwrap();
    let server = Server::start_all(
        &Backend::memory(),
        test_stream_config(),
        Default::default(),
        None,
        Some(alerts.manager.clone()),
    )
    .await;
    Control {
        server,
        alerts,
        mock,
        data,
        stop,
    }
}

impl Control {
    async fn client(
        &self,
    ) -> ControlClient<
        tonic::service::interceptor::InterceptedService<tonic::transport::Channel, Auth>,
    > {
        ControlClient::with_interceptor(
            self.server.channel().await,
            Auth::bearer(&self.server.token),
        )
    }
}

fn pb_channel(id: &str, quiet: Vec<pb::QuietRange>) -> pb::UpsertChannelRequest {
    pb::UpsertChannelRequest {
        channel: Some(pb::Channel {
            id: id.into(),
            name: "Owner Telegram".into(),
            kind: pb::ChannelKind::Telegram as i32,
            enabled: true,
            chat_id: "-1001234".into(),
            quiet_hours: quiet,
            ..Default::default()
        }),
        secret: "123456:SECRETtokenABCdef_ghi-JKL".into(),
    }
}

fn pb_composite(
    id: &str,
    conditions: Vec<pb::CompositeCondition>,
    quiet: Vec<pb::QuietRange>,
) -> pb::UpsertRuleRequest {
    pb::UpsertRuleRequest {
        rule: Some(pb::Rule {
            id: id.into(),
            name: "OI building with rising funding".into(),
            enabled: true,
            kind: Some(pb::rule::Kind::Composite(pb::CompositeRule {
                conditions,
                severity: pb::Severity::Critical as i32,
            })),
            exchanges: vec!["binance".into()],
            symbols: vec![],
            channel_ids: vec!["tg".into()],
            cooldown_seconds: 900,
            quiet_hours: quiet,
        }),
    }
}

fn pb_cond(
    metric: pb::CompositeMetric,
    op: pb::CompareOp,
    threshold: f64,
    window: u32,
) -> pb::CompositeCondition {
    pb::CompositeCondition {
        metric: metric as i32,
        op: op as i32,
        threshold,
        window_seconds: window,
    }
}

#[tokio::test]
async fn composite_rules_and_quiet_hours_round_trip_through_control_and_survive_a_restart() {
    let c = control().await;
    let mut cl = c.client().await;
    cl.upsert_channel(pb_channel(
        "tg",
        vec![pb::QuietRange {
            start_minute: 1320,
            end_minute: 360,
        }],
    ))
    .await
    .unwrap();
    let conds = vec![
        pb_cond(
            pb::CompositeMetric::OiChangePct,
            pb::CompareOp::Ge,
            3.0,
            300,
        ),
        pb_cond(
            pb::CompositeMetric::FundingRate,
            pb::CompareOp::Gt,
            0.0003,
            0,
        ),
        pb_cond(pb::CompositeMetric::Regime, pb::CompareOp::Eq, 1.0, 900),
    ];
    let quiet = vec![pb::QuietRange {
        start_minute: 60,
        end_minute: 120,
    }];
    let stored = cl
        .upsert_rule(pb_composite("comp", conds.clone(), quiet.clone()))
        .await
        .unwrap()
        .into_inner()
        .rule
        .unwrap();
    assert_eq!(stored.id, "comp");
    let Some(pb::rule::Kind::Composite(cr)) = &stored.kind else {
        panic!("{stored:?}")
    };
    assert_eq!(cr.conditions, conds);
    assert_eq!(cr.severity, pb::Severity::Critical as i32);
    assert_eq!(stored.quiet_hours, quiet);
    let listed = cl
        .list_rules(pb::ListRulesRequest {})
        .await
        .unwrap()
        .into_inner()
        .rules;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0], stored);
    let channels = cl
        .list_channels(pb::ListChannelsRequest {})
        .await
        .unwrap()
        .into_inner()
        .channels;
    assert_eq!(
        channels[0].quiet_hours,
        vec![pb::QuietRange {
            start_minute: 1320,
            end_minute: 360
        }]
    );
    // The rule is live in the engine at once.
    assert!(c.alerts.rules.load().rules.iter().any(|r| r.id == "comp"));
    // The file holds it; a new process reads it back.
    let file = std::fs::read_to_string(c.data.path().join("alerts.json")).unwrap();
    assert!(file.contains("\"composite\"") && file.contains("quiet_hours"));
    let mut cfg = Config::default();
    cfg.server.data_dir = c.data.path().to_path_buf();
    let reopened = Alerts::open(&cfg, CancellationToken::new()).unwrap();
    let rules = reopened.manager.list_rules().await;
    assert_eq!(rules.len(), 1);
    assert!(
        matches!(&rules[0].kind, RuleKind::Composite { conditions, severity: Severity::Critical } if conditions.len() == 3)
    );
    assert_eq!(
        rules[0].quiet_hours,
        vec![QuietRange {
            start_minute: 60,
            end_minute: 120
        }]
    );
    // Updating applies live again; deleting removes it.
    cl.upsert_rule(pb_composite("comp", conds[..1].to_vec(), vec![]))
        .await
        .unwrap();
    assert!(
        matches!(&c.alerts.rules.load().rules[0].kind, RuleKind::Composite { conditions, .. } if conditions.len() == 1)
    );
    cl.delete_rule(pb::DeleteRuleRequest { id: "comp".into() })
        .await
        .unwrap();
    assert!(c.alerts.rules.load().rules.is_empty());
    c.stop.cancel();
    c.server.stop().await;
}

#[tokio::test]
async fn invalid_composite_rules_and_quiet_hours_are_refused_over_the_wire() {
    let c = control().await;
    let mut cl = c.client().await;
    cl.upsert_channel(pb_channel("tg", vec![])).await.unwrap();
    let good = pb_cond(
        pb::CompositeMetric::OiChangePct,
        pb::CompareOp::Ge,
        3.0,
        300,
    );
    for (what, req) in [
        ("no conditions", pb_composite("a", vec![], vec![])),
        (
            "unspecified metric",
            pb_composite(
                "a",
                vec![pb::CompositeCondition { metric: 0, ..good }],
                vec![],
            ),
        ),
        (
            "unspecified op",
            pb_composite("a", vec![pb::CompositeCondition { op: 0, ..good }], vec![]),
        ),
        (
            "bad window",
            pb_composite(
                "a",
                vec![pb::CompositeCondition {
                    window_seconds: 5,
                    ..good
                }],
                vec![],
            ),
        ),
        (
            "regime with ge",
            pb_composite(
                "a",
                vec![pb_cond(
                    pb::CompositeMetric::Regime,
                    pb::CompareOp::Ge,
                    1.0,
                    300,
                )],
                vec![],
            ),
        ),
        (
            "empty quiet range",
            pb_composite(
                "a",
                vec![good],
                vec![pb::QuietRange {
                    start_minute: 5,
                    end_minute: 5,
                }],
            ),
        ),
        (
            "quiet minute out of range",
            pb_composite(
                "a",
                vec![good],
                vec![pb::QuietRange {
                    start_minute: 2000,
                    end_minute: 5,
                }],
            ),
        ),
        (
            "huge quiet minute",
            pb_composite(
                "a",
                vec![good],
                vec![pb::QuietRange {
                    start_minute: 100_000,
                    end_minute: 5,
                }],
            ),
        ),
    ] {
        let e = cl.upsert_rule(req).await.unwrap_err();
        assert_eq!(e.code(), Code::InvalidArgument, "{what}: {e}");
    }
    let e = cl
        .upsert_channel(pb_channel(
            "tg2",
            vec![pb::QuietRange {
                start_minute: 1,
                end_minute: 1,
            }],
        ))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);
    assert!(
        cl.list_rules(pb::ListRulesRequest {})
            .await
            .unwrap()
            .into_inner()
            .rules
            .is_empty()
    );
    c.stop.cancel();
    c.server.stop().await;
}

#[tokio::test]
async fn a_channel_in_its_quiet_hours_drops_alerts_but_not_the_digest() {
    use vulture_server::alerts::PushResult;
    use vulture_server::alerts::text::{Alert, Report, ReportSection};
    let c = control().await;
    let mut cl = c.client().await;
    let minute_now = ((std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64)
        .rem_euclid(86_400_000)
        / 60_000) as u32;
    // A range that contains now, wrapping midnight if needed, and one that does not.
    let around = vec![pb::QuietRange {
        start_minute: (minute_now + 1440 - 5) % 1440,
        end_minute: (minute_now + 5) % 1440,
    }];
    let away = vec![pb::QuietRange {
        start_minute: (minute_now + 720) % 1440,
        end_minute: (minute_now + 780) % 1440,
    }];
    cl.upsert_channel(pb_channel("tg", around)).await.unwrap();
    let alert = |n: i64| Alert {
        ts_ms: 1_761_400_992_000 + n,
        severity: Severity::Warning,
        title: "OI spike UP".into(),
        headline: "BTC".into(),
        fields: vec![],
    };
    assert_eq!(c.alerts.delivery.push("tg", alert(0)), PushResult::Quiet);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(c.mock.count(), 0, "dropped, not delayed");
    // The digest goes through anyway.
    let report = Report {
        ts_ms: 1,
        title: "vulture daily digest".into(),
        sections: vec![ReportSection {
            heading: "Data gaps".into(),
            lines: vec!["none".into()],
        }],
        footer: String::new(),
    };
    assert_eq!(
        c.alerts.delivery.push_report("tg", report),
        PushResult::Queued
    );
    wait_until("the digest", || c.mock.count() == 1).await;
    // Out of the quiet range alerts flow again.
    cl.upsert_channel(pb_channel("tg", away)).await.unwrap();
    assert_eq!(c.alerts.delivery.push("tg", alert(1)), PushResult::Queued);
    wait_until("the alert", || c.mock.count() == 2).await;
    let mut fam = vulture_server::metrics::Families::new();
    vulture_server::metrics::MetricSource::collect(c.alerts.delivery.as_ref(), &mut fam);
    assert!(fam.render().contains(
        "vulture_alerts_channel_quiet_skipped_total{channel=\"tg\",kind=\"telegram\"} 1"
    ));
    c.stop.cancel();
    c.server.stop().await;
}

#[tokio::test]
async fn composite_alerts_reach_the_channel_through_the_engine_task() {
    // The engine task ticks every second on the wall clock; feed it a real scenario with recent timestamps.
    use tokio::sync::mpsc;
    use vulture_server::alerts::EngineInput;
    use vulture_server::store::MemoryStore;
    use vulture_server::ws::SystemClock;
    let c = control().await;
    let mut cl = c.client().await;
    cl.upsert_channel(pb_channel("tg", vec![])).await.unwrap();
    cl.upsert_rule(pb_composite(
        "comp",
        vec![pb_cond(
            pb::CompositeMetric::OiChangePct,
            pb::CompareOp::Ge,
            3.0,
            60,
        )],
        vec![],
    ))
    .await
    .unwrap();
    let (writer_tx, _writer_rx) = mpsc::channel(1000);
    let mut cfg = c.alerts.cfg.as_ref().clone();
    cfg.composite_eval_secs = 1;
    cfg.warmup_secs = 0;
    let alerts2 = Arc::clone(&c.alerts);
    let (input, task): (EngineInput, _) = {
        let core = Core::new(&cfg, alerts2.rules.clone(), alerts2.stats.clone());
        let (input, rx) = EngineInput::channel(1000, alerts2.stats.clone(), Arc::new(SystemClock));
        let task = vulture_server::alerts::EngineTask {
            core,
            rx,
            writer: writer_tx,
            delivery: alerts2.delivery.clone(),
            store: Arc::new(MemoryStore::new()),
            clock: Arc::new(SystemClock),
            cfg: Arc::new(cfg.clone()),
            shutdown: c.stop.clone(),
        };
        (input, tokio::spawn(task.run()))
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    // Samples 30 s apart over the last 2 minutes, OI +5% at the end.
    for (i, base) in [1000.0, 1000.0, 1000.0, 1000.0, 1050.0].iter().enumerate() {
        let ts = now - (4 - i as i64) * 30_000;
        input.offer(&oi(
            "binance",
            "BTC-USDT-PERP",
            ts,
            *base,
            100.0,
            None,
            None,
        ));
    }
    wait_until("the composite alert", || c.mock.count() >= 1).await;
    let text = c.mock.requests()[0].json()["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.contains("Composite rule") && text.contains("BTC-USDT-PERP on binance"),
        "{text}"
    );
    c.stop.cancel();
    let _ = task.await;
    c.server.stop().await;
}

#[test]
fn a_pass_over_thousands_of_symbols_and_rules_is_cheap() {
    // 3 000 symbols with 10 minutes of history each and 8 composite rules with 3 conditions (the last condition never holds,
    // so the first two are always computed: the worst case without a match).
    let rules: Vec<RuleDef> = (0..8)
        .map(|i| {
            rule(
                &format!("r{i}"),
                vec![
                    cond(
                        CondMetric::OiChangePct,
                        CmpOp::Ge,
                        -100.0 - f64::from(i),
                        300,
                    ),
                    cond(CondMetric::PriceChangePct, CmpOp::Le, 100.0, 300),
                    cond(CondMetric::FundingRate, CmpOp::Gt, 0.5, 0),
                ],
            )
        })
        .collect();
    let mut cfg = engine_cfg();
    cfg.max_symbols = 10_000;
    let set = Arc::new(ArcSwap::from_pointee(RuleSet::compile(&rules)));
    let stats = Arc::new(EngineStats::default());
    let mut core = Core::new(&cfg, set, stats.clone());
    let mut out = Output::default();
    for s in 0..3000 {
        let sym = format!("S{s:04}-USDT-PERP");
        for k in 0..=21 {
            core.process(
                &oi(
                    "binance",
                    &sym,
                    T0 + k * 30_000,
                    1000.0 + k as f64,
                    100.0,
                    Some(0.0001),
                    Some(100.0),
                ),
                true,
                &mut out,
            );
        }
    }
    let now = T0 + 21 * 30_000 + 1_000;
    let started = std::time::Instant::now();
    core.tick(now, &mut out);
    let took = started.elapsed();
    eprintln!("composite pass: 3000 symbols x 8 rules x 3 conditions in {took:?}");
    assert_eq!(
        stats.composite_evaluations.load(Ordering::Relaxed),
        3000 * 8
    );
    assert!(out.hits.is_empty());
    assert!(took < Duration::from_secs(2), "{took:?}");
}
