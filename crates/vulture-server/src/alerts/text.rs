//! The alert message model and its rendering for Telegram (HTML) and Discord (embeds): compact numbers, UTC
//! timestamps, escaping.

use serde_json::{Value, json};

use super::model::Severity;
use crate::symbols::audit::rfc3339;

/// One message produced by a rule, before it is rendered for a platform.
#[derive(Debug, Clone, PartialEq)]
pub struct Alert {
    /// Event time, ms since the Unix epoch.
    pub ts_ms: i64,
    /// Severity.
    pub severity: Severity,
    /// Short title, for example `OI spike UP`.
    pub title: String,
    /// One line with the essentials.
    pub headline: String,
    /// Further `name: value` lines.
    pub fields: Vec<(String, String)>,
}

/// `$1.23M`, `$456.7K`, `$12`.
pub fn fmt_usd(v: f64) -> String {
    let a = v.abs();
    let sign = if v < 0.0 { "-" } else { "" };
    let (n, unit) = if a >= 1e9 {
        (a / 1e9, "B")
    } else if a >= 1e6 {
        (a / 1e6, "M")
    } else if a >= 1e3 {
        (a / 1e3, "K")
    } else {
        return format!("{sign}${a:.0}");
    };
    let s = if n >= 100.0 {
        format!("{n:.0}")
    } else if n >= 10.0 {
        format!("{n:.1}")
    } else {
        format!("{n:.2}")
    };
    format!("{sign}${s}{unit}")
}

/// A quantity or price with sensible precision and thousands separators.
pub fn fmt_num(v: f64) -> String {
    let a = v.abs();
    let digits = if a >= 1000.0 {
        0
    } else if a >= 10.0 {
        2
    } else if a >= 1.0 {
        3
    } else if a >= 0.01 {
        4
    } else {
        6
    };
    let s = format!("{a:.digits$}");
    let (int, frac) = match s.split_once('.') {
        Some((i, f)) => (i.to_string(), Some(f.to_string())),
        None => (s, None),
    };
    let mut out = String::new();
    for (i, ch) in int.chars().enumerate() {
        if i > 0 && (int.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    if let Some(f) = frac {
        out.push('.');
        out.push_str(&f);
    }
    if v < 0.0 { format!("-{out}") } else { out }
}

/// `+4.21%` / `-3.50%`.
pub fn fmt_pct(v: f64) -> String {
    format!("{v:+.2}%")
}

/// `1m`, `5m`, `1h`, `4h`, `90s`.
pub fn fmt_window(secs: u32) -> String {
    if secs >= 3600 && secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs >= 60 && secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

/// `2026-10-25 14:03:12 UTC`.
pub fn fmt_time(ts_ms: i64) -> String {
    let s = rfc3339(ts_ms);
    format!("{} {} UTC", &s[..10], &s[11..19])
}

/// Escapes text for Telegram's HTML parse mode.
pub fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            c => out.push(c),
        }
    }
    out
}

/// Escapes Discord markdown and neutralizes mentions in plain text.
pub fn discord_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' | '*' | '_' | '`' | '~' | '|' | '>' | '[' | ']' | '(' | ')' | '#' => {
                out.push('\\');
                out.push(ch);
            }
            // A zero-width space after '@' stops @everyone, @here and user mentions from resolving.
            '@' => out.push_str("@\u{200b}"),
            c => out.push(c),
        }
    }
    out
}

/// Truncates to at most `max` characters, ending with an ellipsis when cut.
pub fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('\u{2026}');
    out
}

fn marker(sev: Severity) -> &'static str {
    match sev {
        Severity::Info => "\u{2139}\u{fe0f}",
        Severity::Warning => "\u{26a0}\u{fe0f}",
        Severity::Critical => "\u{1f6a8}",
    }
}

/// Telegram's message limit.
pub const TELEGRAM_LIMIT: usize = 4096;
/// Discord's embed description limit.
pub const DISCORD_DESC_LIMIT: usize = 4000;

/// What a batch of alerts turns into: the alerts, how many more were left out of the list, and how many were
/// dropped from the queue before.
#[derive(Debug, Clone, Default)]
pub struct Digest {
    /// Alerts to list, oldest first.
    pub alerts: Vec<Alert>,
    /// Alerts of the batch that did not fit the list.
    pub more: usize,
    /// Alerts dropped by the queue since the last message.
    pub dropped: u64,
    /// A report (the daily digest) sent as a message of its own; when set the fields above are ignored.
    pub report: Option<Report>,
}

/// A titled report in sections of plain lines: the daily digest. It is rendered as one message and never merged with
/// alerts.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Report {
    /// Time the report was built, ms.
    pub ts_ms: i64,
    /// Title line.
    pub title: String,
    /// Sections in order of importance (a message that is too long loses the last ones first).
    pub sections: Vec<ReportSection>,
    /// Last line (period, data source).
    pub footer: String,
}

/// One section of a [`Report`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReportSection {
    /// Heading.
    pub heading: String,
    /// Plain-text lines (no markup).
    pub lines: Vec<String>,
}

/// Plain-text form of a report: the dry-run log line, the Control preview and the base of both renderings.
pub fn report_plain(r: &Report) -> String {
    let mut out = format!("{}\n", r.title);
    for s in &r.sections {
        out.push_str(&format!("\n{}\n", s.heading));
        for l in &s.lines {
            out.push_str(&format!("- {l}\n"));
        }
    }
    if !r.footer.is_empty() {
        out.push_str(&format!("\n{}\n", r.footer));
    }
    out
}

fn telegram_report(r: &Report) -> String {
    let mut lines: Vec<String> = vec![format!("\u{1f4ca} <b>{}</b>", html_escape(&r.title))];
    for s in &r.sections {
        lines.push(String::new());
        lines.push(format!("<b>{}</b>", html_escape(&s.heading)));
        for l in &s.lines {
            lines.push(format!("\u{2022} {}", html_escape(l)));
        }
    }
    if !r.footer.is_empty() {
        lines.push(String::new());
        lines.push(format!("<i>{}</i>", html_escape(&r.footer)));
    }
    truncate_html(&lines.join("\n"), TELEGRAM_LIMIT)
}

fn discord_report(r: &Report) -> Value {
    let mut lines: Vec<String> = Vec::new();
    for s in &r.sections {
        lines.push(format!("**{}**", discord_escape(&s.heading)));
        for l in &s.lines {
            lines.push(format!("\u{2022} {}", discord_escape(l)));
        }
        lines.push(String::new());
    }
    if !r.footer.is_empty() {
        lines.push(format!("_{}_", discord_escape(&r.footer)));
    }
    // Cut at a line boundary: a half-cut bold marker would swallow the rest of the message.
    let mut desc = String::new();
    for (i, line) in lines.iter().enumerate() {
        if desc.chars().count() + line.chars().count() + 24 > DISCORD_DESC_LIMIT {
            desc.push_str(&format!("\n\u{2026} ({} more lines cut)", lines.len() - i));
            break;
        }
        if !desc.is_empty() {
            desc.push('\n');
        }
        desc.push_str(line);
    }
    json!({
        "allowed_mentions": {"parse": []},
        "embeds": [{
            "title": truncate_chars(&r.title, 250),
            "description": desc,
            "color": color(Severity::Info),
            "timestamp": rfc3339(r.ts_ms),
        }],
    })
}

/// Renders for Telegram (HTML). One alert gets the full form, several a compact digest.
pub fn telegram_html(d: &Digest) -> String {
    if let Some(r) = &d.report {
        return telegram_report(r);
    }
    let mut out = String::new();
    if d.alerts.len() == 1 && d.more == 0 {
        let a = &d.alerts[0];
        out.push_str(&format!(
            "{} <b>{}</b>\n{}\n",
            marker(a.severity),
            html_escape(&a.title),
            html_escape(&a.headline)
        ));
        for (k, v) in &a.fields {
            out.push_str(&format!(
                "{}: <code>{}</code>\n",
                html_escape(k),
                html_escape(v)
            ));
        }
        out.push_str(&format!("<i>{}</i>", fmt_time(a.ts_ms)));
    } else {
        let total = d.alerts.len() + d.more;
        let worst = d
            .alerts
            .iter()
            .map(|a| a.severity)
            .max()
            .unwrap_or_default();
        out.push_str(&format!("{} <b>{total} alerts</b>\n", marker(worst)));
        for a in &d.alerts {
            out.push_str(&format!(
                "\u{2022} <b>{}</b> {} <i>{}</i>\n",
                html_escape(&a.title),
                html_escape(&a.headline),
                &fmt_time(a.ts_ms)[11..19]
            ));
        }
        if d.more > 0 {
            out.push_str(&format!("\u{2026} and {} more\n", d.more));
        }
        if let (Some(first), Some(last)) = (d.alerts.first(), d.alerts.last()) {
            out.push_str(&format!(
                "<i>{} \u{2013} {}</i>",
                fmt_time(first.ts_ms),
                &fmt_time(last.ts_ms)[11..]
            ));
        }
    }
    if d.dropped > 0 {
        out.push_str(&format!(
            "\n\u{26a0}\u{fe0f} {} messages dropped",
            d.dropped
        ));
    }
    truncate_html(&out, TELEGRAM_LIMIT)
}

/// Cuts at a line boundary so that no tag is left open (every tag of the renderer closes within its line).
fn truncate_html(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out = String::new();
    for line in s.lines() {
        if out.chars().count() + line.chars().count() + 12 > max {
            out.push_str("\n\u{2026} (cut)");
            break;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(line);
    }
    out
}

fn color(sev: Severity) -> u32 {
    match sev {
        Severity::Info => 0x3498db,
        Severity::Warning => 0xf1c40f,
        Severity::Critical => 0xe74c3c,
    }
}

/// Renders the JSON body of a Discord webhook call (one embed, mentions disabled).
pub fn discord_body(d: &Digest) -> Value {
    if let Some(r) = &d.report {
        return discord_report(r);
    }
    let (title, description, sev, ts) = if d.alerts.len() == 1 && d.more == 0 {
        let a = &d.alerts[0];
        let mut desc = discord_escape(&a.headline);
        for (k, v) in &a.fields {
            desc.push_str(&format!(
                "\n{}: `{}`",
                discord_escape(k),
                v.replace('`', "'")
            ));
        }
        (discord_escape(&a.title), desc, a.severity, a.ts_ms)
    } else {
        let total = d.alerts.len() + d.more;
        let mut desc = String::new();
        for a in &d.alerts {
            desc.push_str(&format!(
                "\u{2022} **{}** {} _{}_\n",
                discord_escape(&a.title),
                discord_escape(&a.headline),
                &fmt_time(a.ts_ms)[11..19]
            ));
        }
        if d.more > 0 {
            desc.push_str(&format!("\u{2026} and {} more\n", d.more));
        }
        let worst = d
            .alerts
            .iter()
            .map(|a| a.severity)
            .max()
            .unwrap_or_default();
        let ts = d.alerts.last().map_or(0, |a| a.ts_ms);
        (format!("{total} alerts"), desc, worst, ts)
    };
    let mut description = description;
    if d.dropped > 0 {
        description.push_str(&format!(
            "\n\u{26a0}\u{fe0f} {} messages dropped",
            d.dropped
        ));
    }
    json!({
        "allowed_mentions": {"parse": []},
        "embeds": [{
            "title": truncate_chars(&title, 250),
            "description": truncate_chars(&description, DISCORD_DESC_LIMIT),
            "color": color(sev),
            "timestamp": rfc3339(ts),
        }],
    })
}

/// The text of a test message (no data of the deployment).
pub fn test_alert(channel_name: &str, ts_ms: i64) -> Alert {
    Alert {
        ts_ms,
        severity: Severity::Info,
        title: "vulture test message".into(),
        headline: format!("Channel \"{channel_name}\" is wired up correctly."),
        fields: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alert(title: &str, headline: &str) -> Alert {
        Alert {
            ts_ms: 1_761_400_992_000,
            severity: Severity::Warning,
            title: title.into(),
            headline: headline.into(),
            fields: vec![("window".into(), "5m".into())],
        }
    }

    #[test]
    fn numbers_are_compact() {
        assert_eq!(fmt_usd(1_234_567.0), "$1.23M");
        assert_eq!(fmt_usd(456_700.0), "$457K");
        assert_eq!(fmt_usd(45_600.0), "$45.6K");
        assert_eq!(fmt_usd(12.4), "$12");
        assert_eq!(fmt_usd(-2.5e9), "-$2.50B");
        assert_eq!(fmt_num(67_432.6), "67,433");
        assert_eq!(fmt_num(1_234_567.0), "1,234,567");
        assert_eq!(fmt_num(0.123456), "0.1235");
        assert_eq!(fmt_pct(4.213), "+4.21%");
        assert_eq!(fmt_pct(-3.5), "-3.50%");
        assert_eq!(fmt_window(300), "5m");
        assert_eq!(fmt_window(14_400), "4h");
        assert_eq!(fmt_window(90), "90s");
    }

    #[test]
    fn time_is_utc() {
        assert_eq!(fmt_time(1_761_400_992_000), "2025-10-25 14:03:12 UTC");
    }

    #[test]
    fn telegram_escapes_html() {
        let d = Digest {
            alerts: vec![alert("A <b>", "x & y <script>")],
            ..Default::default()
        };
        let t = telegram_html(&d);
        assert!(t.contains("A &lt;b&gt;") && t.contains("x &amp; y &lt;script&gt;"));
        assert!(!t.contains("<script>"));
        assert!(t.contains("<b>") && t.contains("<code>5m</code>"));
    }

    #[test]
    fn digest_counts_and_dropped() {
        let d = Digest {
            alerts: vec![alert("A", "one"), alert("B", "two")],
            more: 3,
            dropped: 7,
            ..Default::default()
        };
        let t = telegram_html(&d);
        assert!(t.contains("5 alerts"));
        assert!(t.contains("and 3 more"));
        assert!(t.contains("7 messages dropped"));
        let j = discord_body(&d);
        assert_eq!(j["embeds"][0]["title"], "5 alerts");
        assert!(
            j["embeds"][0]["description"]
                .as_str()
                .unwrap()
                .contains("7 messages dropped")
        );
    }

    #[test]
    fn discord_neutralizes_mentions_and_markdown() {
        let d = Digest {
            alerts: vec![alert("T", "@everyone *bold* _x_")],
            ..Default::default()
        };
        let j = discord_body(&d);
        let desc = j["embeds"][0]["description"].as_str().unwrap();
        assert!(!desc.contains("@everyone"));
        assert!(desc.contains("\\*bold\\*"));
        assert_eq!(j["allowed_mentions"]["parse"], json!([]));
    }

    #[test]
    fn long_messages_are_cut() {
        let alerts: Vec<Alert> = (0..400)
            .map(|i| alert("T", &format!("line {i} {}", "x".repeat(40))))
            .collect();
        let d = Digest {
            alerts,
            ..Default::default()
        };
        assert!(telegram_html(&d).chars().count() <= TELEGRAM_LIMIT);
        let j = discord_body(&d);
        assert!(
            j["embeds"][0]["description"]
                .as_str()
                .unwrap()
                .chars()
                .count()
                <= DISCORD_DESC_LIMIT
        );
    }
}
