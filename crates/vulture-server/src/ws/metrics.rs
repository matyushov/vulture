//! Metrics of the WebSocket manager (one logical connection per exchange).

use std::sync::atomic::Ordering;

use super::{ConnectionState, WsCounters, WsStatus};
use crate::metrics::Families;

/// Adds the manager's state and counters, labelled with the exchange name.
pub fn collect(exchange: &str, counters: &WsCounters, status: &WsStatus, out: &mut Families) {
    let l = [("exchange", exchange)];
    for (name, state) in [
        ("connecting", ConnectionState::Connecting),
        ("healthy", ConnectionState::Healthy),
        ("degraded", ConnectionState::Degraded),
        ("stopped", ConnectionState::Stopped),
    ] {
        out.gauge(
            "vulture_ws_state",
            "WebSocket connection state; 1 for the current state.",
            &[("exchange", exchange), ("state", name)],
            f64::from(u8::from(status.state == state)),
        );
    }
    out.gauge(
        "vulture_ws_consecutive_failures",
        "Failed connection attempts since the last healthy period.",
        &l,
        f64::from(status.consecutive_failures),
    );
    let c = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
    for (name, help, v) in [
        (
            "vulture_ws_reconnects_total",
            "Recoveries after a lost connection.",
            status.reconnects,
        ),
        (
            "vulture_ws_failed_attempts_total",
            "Failed or lost WebSocket sessions.",
            c(&counters.failed_attempts),
        ),
        (
            "vulture_ws_planned_reconnects_total",
            "Completed planned make-before-break switches.",
            c(&counters.planned_reconnects),
        ),
        (
            "vulture_ws_planned_failures_total",
            "Planned switches whose new session failed to come up.",
            c(&counters.planned_failures),
        ),
        (
            "vulture_ws_gaps_total",
            "Gap events published after a connection loss.",
            c(&counters.gaps),
        ),
        (
            "vulture_ws_dropped_overflow_total",
            "Frames dropped because the consumer channel was full.",
            c(&counters.dropped_overflow),
        ),
        (
            "vulture_ws_duplicates_total",
            "Frames dropped as duplicates during a switch.",
            c(&counters.duplicates),
        ),
        (
            "vulture_ws_frames_forwarded_total",
            "Frames delivered to the connector.",
            c(&counters.frames_forwarded),
        ),
        (
            "vulture_ws_degraded_total",
            "Entries into the degraded state.",
            c(&counters.degraded_entered),
        ),
        (
            "vulture_ws_handshake_bans_total",
            "Handshake rejections with 403, 429 or 418.",
            c(&counters.handshake_bans),
        ),
    ] {
        out.counter(name, help, &l, v);
    }
}
