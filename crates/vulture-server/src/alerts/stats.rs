//! Counters of the analytics engine, read by the metrics endpoint.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::{Families, MetricSource};

/// Atomic counters shared by the engine, the detectors and the rule evaluation.
#[derive(Debug, Default)]
pub struct EngineStats {
    /// OI events seen.
    pub events_oi: AtomicU64,
    /// Liquidation events seen.
    pub events_liq: AtomicU64,
    /// Gap events seen.
    pub events_gap: AtomicU64,
    /// Events dropped because the engine's input queue was full.
    pub input_dropped: AtomicU64,
    /// Symbols ignored because `max_symbols` was reached.
    pub symbols_rejected: AtomicU64,
    /// Signals emitted: OI up, OI down, cascade.
    pub signals: [AtomicU64; 3],
    /// OI candidates suppressed by the cooldown.
    pub oi_suppressed_cooldown: AtomicU64,
    /// OI candidates that passed the thresholds but were not unusual for the symbol.
    pub oi_suppressed_usual: AtomicU64,
    /// OI candidates dropped for lack of a baseline.
    pub oi_suppressed_no_baseline: AtomicU64,
    /// Evaluations skipped because the symbol is dust.
    pub oi_dust: AtomicU64,
    /// Cascades started.
    pub cascade_started: AtomicU64,
    /// Cascades ended.
    pub cascade_ended: AtomicU64,
    /// Rule matches suppressed by the rule's cooldown.
    pub rule_suppressed_cooldown: AtomicU64,
    /// Alerts handed to the delivery queues.
    pub alerts_queued: AtomicU64,
    /// Evaluation time, total nanoseconds.
    pub eval_ns_total: AtomicU64,
    /// Evaluations.
    pub eval_count: AtomicU64,
    /// Slowest evaluation, nanoseconds.
    pub eval_ns_max: AtomicU64,
    /// Series held (gauge).
    pub series: AtomicU64,
    /// Samples held (gauge).
    pub samples: AtomicU64,
    /// Allocated memory of the series (gauge), bytes.
    pub memory_bytes: AtomicU64,
    /// Duration of the last warm-up, milliseconds.
    pub warmup_ms: AtomicU64,
    /// Entries replayed by the last warm-up.
    pub warmup_events: AtomicU64,
    /// 1 once the warm-up has finished.
    pub warmed_up: AtomicU64,
}

impl EngineStats {
    /// Records one evaluation of `ns` nanoseconds.
    pub fn record_eval(&self, ns: u64) {
        self.eval_ns_total.fetch_add(ns, Ordering::Relaxed);
        self.eval_count.fetch_add(1, Ordering::Relaxed);
        self.eval_ns_max.fetch_max(ns, Ordering::Relaxed);
    }

    /// Adds one to a counter.
    pub fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// Signal type labels in the order of [`EngineStats::signals`].
pub const SIGNAL_LABELS: [&str; 3] = ["oi_spike_up", "oi_spike_down", "liq_cascade"];

impl MetricSource for EngineStats {
    fn collect(&self, out: &mut Families) {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        for (kind, a) in [
            ("open_interest", &self.events_oi),
            ("liquidation", &self.events_liq),
            ("gap", &self.events_gap),
        ] {
            out.counter(
                "vulture_alerts_engine_events_total",
                "Events processed by the analytics engine.",
                &[("type", kind)],
                g(a),
            );
        }
        out.counter(
            "vulture_alerts_engine_input_dropped_total",
            "Events the engine dropped because its own input queue was full (the writer is unaffected).",
            &[],
            g(&self.input_dropped),
        );
        out.counter(
            "vulture_alerts_engine_symbols_rejected_total",
            "Symbols ignored because the engine's symbol limit was reached.",
            &[],
            g(&self.symbols_rejected),
        );
        for (label, a) in SIGNAL_LABELS.iter().zip(&self.signals) {
            out.counter(
                "vulture_alerts_signals_total",
                "Signals emitted by type.",
                &[("type", label)],
                g(a),
            );
        }
        for (detector, reason, a) in [
            ("oi_spike", "cooldown", &self.oi_suppressed_cooldown),
            ("oi_spike", "usual_for_symbol", &self.oi_suppressed_usual),
            ("oi_spike", "no_baseline", &self.oi_suppressed_no_baseline),
            ("oi_spike", "dust", &self.oi_dust),
            ("rules", "cooldown", &self.rule_suppressed_cooldown),
        ] {
            out.counter(
                "vulture_alerts_suppressed_total",
                "Candidates suppressed before they became signals or messages.",
                &[("detector", detector), ("reason", reason)],
                g(a),
            );
        }
        out.counter(
            "vulture_alerts_cascades_total",
            "Liquidation cascades by phase.",
            &[("phase", "start")],
            g(&self.cascade_started),
        );
        out.counter(
            "vulture_alerts_cascades_total",
            "Liquidation cascades by phase.",
            &[("phase", "end")],
            g(&self.cascade_ended),
        );
        out.counter(
            "vulture_alerts_queued_total",
            "Alerts handed to the delivery queues.",
            &[],
            g(&self.alerts_queued),
        );
        out.counter(
            "vulture_alerts_evaluations_total",
            "Evaluations of one event by the engine.",
            &[],
            g(&self.eval_count),
        );
        out.gauge(
            "vulture_alerts_evaluation_seconds_sum",
            "Total time spent evaluating events, seconds.",
            &[],
            g(&self.eval_ns_total) as f64 / 1e9,
        );
        out.gauge(
            "vulture_alerts_evaluation_seconds_max",
            "Slowest evaluation of one event, seconds.",
            &[],
            g(&self.eval_ns_max) as f64 / 1e9,
        );
        out.gauge(
            "vulture_alerts_series",
            "Symbols with an OI ring buffer.",
            &[],
            g(&self.series) as f64,
        );
        out.gauge(
            "vulture_alerts_samples",
            "OI samples held in the ring buffers.",
            &[],
            g(&self.samples) as f64,
        );
        out.gauge(
            "vulture_alerts_engine_memory_bytes",
            "Allocated memory of the OI ring buffers.",
            &[],
            g(&self.memory_bytes) as f64,
        );
        out.gauge(
            "vulture_alerts_warmup_seconds",
            "Duration of the last warm-up from the Redis stream.",
            &[],
            g(&self.warmup_ms) as f64 / 1000.0,
        );
        out.gauge(
            "vulture_alerts_warmup_entries",
            "Stream entries replayed by the last warm-up.",
            &[],
            g(&self.warmup_events) as f64,
        );
        out.gauge(
            "vulture_alerts_warmed_up",
            "1 once the warm-up has finished and the detectors are live.",
            &[],
            g(&self.warmed_up) as f64,
        );
    }
}
