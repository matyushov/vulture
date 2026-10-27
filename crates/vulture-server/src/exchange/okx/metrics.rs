//! Prometheus samples of the OKX connector.

use std::sync::atomic::Ordering;

use super::{NAME, OkxStatus};
use crate::metrics::{Families, unix_now_ms};
use crate::ws::{ConnectionState, WsCounters, WsStatus};

pub(super) fn collect(s: &OkxStatus, out: &mut Families) {
    let x = [("exchange", NAME)];
    let c = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);

    for (ty, v) in [
        ("liquidation", c(&s.liquidations)),
        ("open_interest", c(&s.oi_events)),
        ("gap", c(&s.gaps)),
    ] {
        out.counter(
            "vulture_events_total",
            "Events published by the connector, by type.",
            &[("exchange", NAME), ("type", ty)],
            v,
        );
    }
    for (ty, v) in [
        ("liquidation", c(&s.liquidation_parse_errors)),
        ("mark_price", c(&s.mark_parse_errors)),
        ("open_interest", c(&s.oi_parse_errors)),
    ] {
        out.counter(
            "vulture_parse_errors_total",
            "Frames or responses that could not be parsed, by type.",
            &[("exchange", NAME), ("type", ty)],
            v,
        );
    }
    for (name, help, v) in [
        (
            "vulture_connector_ws_frames_total",
            "Data frames taken from the WebSocket manager.",
            c(&s.ws_frames),
        ),
        (
            "vulture_connector_mark_frames_total",
            "Mark price and open interest pushes applied to the state.",
            c(&s.mark_frames) + c(&s.oi_frames),
        ),
        (
            "vulture_connector_unknown_frames_total",
            "Frames of an unknown kind.",
            c(&s.unknown_frames),
        ),
        (
            "vulture_connector_filtered_liquidations_total",
            "Liquidations of symbols outside the tracked set.",
            c(&s.liquidations_filtered),
        ),
        (
            "vulture_connector_duplicate_liquidations_total",
            "Liquidation records dropped as duplicates.",
            c(&s.liquidations_duplicate),
        ),
        (
            "vulture_connector_filtered_tickers_total",
            "Quote pushes of symbols outside the tracked set.",
            c(&s.quotes_filtered),
        ),
        (
            "vulture_oi_samples_skipped_total",
            "Sampler slots skipped for lack of data or a live connection.",
            c(&s.oi_skipped),
        ),
        (
            "vulture_oi_without_mark_total",
            "Open interest samples without a usable mark price.",
            c(&s.oi_without_mark),
        ),
        (
            "vulture_oi_unit_mismatch_total",
            "Open interest samples whose coin amount disagrees with contracts times contract size.",
            c(&s.oi_unit_mismatch),
        ),
        (
            "vulture_ws_changes_deferred_total",
            "Tracked-set changes postponed by the hourly subscription request budget.",
            c(&s.changes_deferred),
        ),
        (
            "vulture_oi_cycles_total",
            "Completed open interest sampler cycles.",
            c(&s.oi_cycles),
        ),
        (
            "vulture_funding_requests_total",
            "REST requests for funding rates and index prices.",
            c(&s.funding_requests),
        ),
        (
            "vulture_funding_errors_total",
            "Funding and index REST requests that failed.",
            c(&s.funding_errors),
        ),
        (
            "vulture_funding_fallbacks_total",
            "Funding poll cycles that asked instrument by instrument.",
            c(&s.funding_fallbacks),
        ),
    ] {
        out.counter(name, help, &x, v);
    }
    for (result, v) in [("ok", c(&s.info_loads)), ("error", c(&s.info_errors))] {
        out.counter(
            "vulture_exchange_info_loads_total",
            "Symbol list (exchangeInfo) loads by result.",
            &[("exchange", NAME), ("result", result)],
            v,
        );
    }
    for (code, v) in [("403", c(&s.http_403)), ("429", c(&s.http_429))] {
        out.counter(
            "vulture_rest_throttled_total",
            "REST responses that throttle or ban, by HTTP status.",
            &[("exchange", NAME), ("code", code)],
            v,
        );
    }

    let now = unix_now_ms();
    let paused_until = s.paused_until_ms.load(Ordering::Relaxed);
    out.gauge(
        "vulture_symbols",
        "Symbols in the collected set.",
        &x,
        c(&s.symbols) as f64,
    );
    out.gauge(
        "vulture_unusable_instruments",
        "Linear swaps dropped for an unusable contract size (last load).",
        &x,
        c(&s.unusable_instruments) as f64,
    );
    out.gauge(
        "vulture_rest_paused",
        "1 while REST requests are paused (403 or 429).",
        &x,
        f64::from(u8::from(paused_until > now)),
    );
    out.gauge(
        "vulture_ws_request_budget_spent",
        "Subscribe and unsubscribe requests spent in the current hour.",
        &x,
        c(&s.request_budget_spent) as f64,
    );
    let tick = s.last_mark_ms.load(Ordering::Relaxed);
    if tick > 0 {
        out.gauge(
            "vulture_mark_price_age_seconds",
            "Seconds since the last mark price frame (the liveness stream).",
            &x,
            (now - tick).max(0) as f64 / 1000.0,
        );
    }

    let views = s.connections();
    if views.is_empty() {
        return;
    }
    // One logical series per exchange: counters are summed, the state is the worst one.
    let total = WsCounters::default();
    let mut reconnects = 0;
    let mut failures = 0;
    let mut state = ConnectionState::Stopped;
    let rank = |s: ConnectionState| match s {
        ConnectionState::Degraded => 3,
        ConnectionState::Connecting => 2,
        ConnectionState::Healthy => 1,
        ConnectionState::Stopped => 0,
    };
    let mut active = 0;
    let mut requests = 0;
    let mut rejected = 0;
    let mut ack_timeouts = 0;
    for v in &views {
        add(&total, &v.counters);
        reconnects += v.status.reconnects;
        failures = failures.max(v.status.consecutive_failures);
        if rank(v.status.state) > rank(state) {
            state = v.status.state;
        }
        active += c(&v.counters.topics_active);
        requests += c(&v.counters.topic_requests);
        rejected += c(&v.counters.topics_rejected);
        ack_timeouts += c(&v.counters.topic_ack_timeouts);
    }
    crate::ws::metrics::collect(
        NAME,
        &total,
        &WsStatus {
            state,
            reconnects,
            consecutive_failures: failures,
        },
        out,
    );
    out.gauge(
        "vulture_ws_connections",
        "WebSocket connections the connector keeps (shards).",
        &x,
        views.len() as f64,
    );
    out.gauge(
        "vulture_ws_topics_subscribed",
        "Topics subscribed in the current sessions.",
        &x,
        active as f64,
    );
    out.counter(
        "vulture_ws_topic_requests_total",
        "Subscribe and unsubscribe requests sent.",
        &x,
        requests,
    );
    out.counter(
        "vulture_ws_topics_rejected_total",
        "Topics the exchange refused.",
        &x,
        rejected,
    );
    out.counter(
        "vulture_ws_topic_ack_timeouts_total",
        "Sessions ended because a topic request was not answered.",
        &x,
        ack_timeouts,
    );
    if let Some((granted, delayed, open)) = s.limiter_stats() {
        out.counter(
            "vulture_ws_connection_permits_total",
            "Connection attempts admitted by the connection budget.",
            &x,
            granted,
        );
        out.counter(
            "vulture_ws_connection_budget_waits_total",
            "Connection attempts delayed by the connection budget.",
            &x,
            delayed,
        );
        out.gauge(
            "vulture_ws_connections_open",
            "Connections holding a budget permit.",
            &x,
            open as f64,
        );
    }
}

fn add(total: &WsCounters, v: &WsCounters) {
    let pairs = [
        (&total.failed_attempts, &v.failed_attempts),
        (&total.reconnects, &v.reconnects),
        (&total.planned_reconnects, &v.planned_reconnects),
        (&total.planned_failures, &v.planned_failures),
        (&total.gaps, &v.gaps),
        (&total.dropped_overflow, &v.dropped_overflow),
        (&total.duplicates, &v.duplicates),
        (&total.frames_forwarded, &v.frames_forwarded),
        (&total.degraded_entered, &v.degraded_entered),
        (&total.handshake_bans, &v.handshake_bans),
    ];
    for (t, s) in pairs {
        t.fetch_add(s.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}
