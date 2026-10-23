//! Prometheus samples of the Binance connector.

use std::sync::atomic::Ordering;

use super::{BinanceStatus, NAME};
use crate::metrics::{Families, unix_now_ms};

pub(super) fn collect(s: &BinanceStatus, out: &mut Families) {
    let x = [("exchange", NAME)];
    let c = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);

    for (ty, v) in [
        ("liquidation", c(&s.liquidations)),
        ("open_interest", c(&s.oi_events)),
        ("gap", c(&s.gaps) + c(&s.oi_gaps)),
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
            "Mark price frames applied.",
            c(&s.mark_frames),
        ),
        (
            "vulture_connector_unknown_frames_total",
            "Frames of an unknown kind.",
            c(&s.unknown_frames),
        ),
        (
            "vulture_connector_filtered_liquidations_total",
            "Liquidations of symbols outside the collected set.",
            c(&s.liquidations_filtered),
        ),
        (
            "vulture_oi_requests_total",
            "Open interest REST requests sent (retries included).",
            c(&s.oi_requests),
        ),
        (
            "vulture_oi_retries_total",
            "Open interest request retries.",
            c(&s.oi_retries),
        ),
        (
            "vulture_oi_failures_total",
            "Symbols given up on in a cycle after all attempts failed.",
            c(&s.oi_failures),
        ),
        (
            "vulture_oi_network_errors_total",
            "Open interest timeouts and network errors.",
            c(&s.oi_network_errors),
        ),
        (
            "vulture_oi_without_mark_total",
            "Open interest snapshots without a usable mark price.",
            c(&s.oi_without_mark),
        ),
        (
            "vulture_oi_cycles_total",
            "Completed open interest polling cycles.",
            c(&s.oi_cycles),
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
    for (code, v) in [("429", c(&s.http_429)), ("418", c(&s.http_418))] {
        out.counter(
            "vulture_rest_throttled_total",
            "REST responses that throttle or ban, by HTTP status.",
            &[("exchange", NAME), ("code", code)],
            v,
        );
    }

    let now = unix_now_ms();
    let paused_until = s.paused_until_ms.load(Ordering::Relaxed);
    for (name, help, v) in [
        (
            "vulture_symbols",
            "Symbols in the collected set.",
            c(&s.symbols) as f64,
        ),
        (
            "vulture_rest_used_weight_1m",
            "Last X-MBX-USED-WEIGHT-1M seen.",
            f64::from(s.used_weight_1m.load(Ordering::Relaxed)),
        ),
        (
            "vulture_rest_banned",
            "1 while REST is paused after an IP ban (HTTP 418).",
            f64::from(u8::from(s.banned.load(Ordering::Relaxed))),
        ),
        (
            "vulture_rest_paused",
            "1 while REST requests are paused (429 or 418).",
            f64::from(u8::from(paused_until > now)),
        ),
        (
            "vulture_oi_poll_interval_seconds",
            "Current effective open interest polling interval.",
            c(&s.oi_interval_ms) as f64 / 1000.0,
        ),
    ] {
        out.gauge(name, help, &x, v);
    }
    let mark = s.last_mark_ms.load(Ordering::Relaxed);
    if mark > 0 {
        out.gauge(
            "vulture_mark_price_age_seconds",
            "Seconds since the last mark price frame (the liveness stream).",
            &x,
            (now - mark).max(0) as f64 / 1000.0,
        );
    }
    if let Some(link) = s.ws.get() {
        let status = *link.status.borrow();
        crate::ws::metrics::collect(NAME, &link.counters, &status, out);
    }
}
