//! A tiny Prometheus text exposition writer.
//!
//! Samples are collected into families first and rendered afterwards, so every family is printed once with its
//! `# HELP` and `# TYPE` lines and its samples side by side, whatever order the sources report them in.

use std::collections::BTreeMap;
use std::fmt::Write;

/// Metric type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Counter,
    Gauge,
}

#[derive(Debug)]
struct Family {
    help: String,
    kind: Kind,
    samples: Vec<(Vec<(String, String)>, f64)>,
}

/// Metric families collected from the sources during one scrape.
#[derive(Debug, Default)]
pub struct Families {
    map: BTreeMap<String, Family>,
}

impl Families {
    /// Empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a counter sample. By convention the name ends with `_total`.
    pub fn counter(&mut self, name: &str, help: &str, labels: &[(&str, &str)], value: u64) {
        self.add(name, help, Kind::Counter, labels, value as f64);
    }

    /// Adds a gauge sample.
    pub fn gauge(&mut self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        self.add(name, help, Kind::Gauge, labels, value);
    }

    fn add(&mut self, name: &str, help: &str, kind: Kind, labels: &[(&str, &str)], value: f64) {
        debug_assert!(valid_name(name), "bad metric name {name}");
        let family = self.map.entry(name.to_string()).or_insert_with(|| Family {
            help: help.to_string(),
            kind,
            samples: Vec::new(),
        });
        debug_assert_eq!(family.kind, kind, "{name} reported with two types");
        family.samples.push((
            labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            value,
        ));
    }

    /// Number of samples (series) in all families.
    pub fn series(&self) -> usize {
        self.map.values().map(|f| f.samples.len()).sum()
    }

    /// Text exposition format 0.0.4.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for (name, family) in &self.map {
            let kind = match family.kind {
                Kind::Counter => "counter",
                Kind::Gauge => "gauge",
            };
            let _ = writeln!(out, "# HELP {name} {}", escape_help(&family.help));
            let _ = writeln!(out, "# TYPE {name} {kind}");
            for (labels, value) in &family.samples {
                out.push_str(name);
                if !labels.is_empty() {
                    out.push('{');
                    for (i, (k, v)) in labels.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        let _ = write!(out, "{k}=\"{}\"", escape_label(v));
                    }
                    out.push('}');
                }
                let _ = writeln!(out, " {}", format_value(*value));
            }
        }
        out
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b':')
        && !name.as_bytes()[0].is_ascii_digit()
}

fn escape_help(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\n', "\\n")
}

fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf" } else { "-Inf" }.into()
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn families_are_grouped_and_escaped() {
        let mut f = Families::new();
        f.counter("a_total", "A.", &[("x", "1")], 3);
        f.gauge("b", "B\nline", &[], 1.5);
        f.counter("a_total", "A.", &[("x", "q\"\\\n")], 4);
        let text = f.render();
        assert_eq!(
            text,
            "# HELP a_total A.\n# TYPE a_total counter\na_total{x=\"1\"} 3\na_total{x=\"q\\\"\\\\\\n\"} 4\n\
             # HELP b B\\nline\n# TYPE b gauge\nb 1.5\n"
        );
        assert_eq!(f.series(), 3);
    }
}
