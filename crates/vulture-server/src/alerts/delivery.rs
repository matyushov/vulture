//! Delivery of alerts to Telegram and Discord.
//!
//! Every channel has one task and a bounded queue. The task waits for the first alert, collects what else
//! arrives within the batch window and sends it as one message (a digest with a count when there are several),
//! never faster than the channel's rate limit. Failures are retried with exponential backoff; `429` waits the
//! `retry_after` the platform names; a queue that overflows drops its oldest alerts and the next message says
//! how many. Secrets are read when a message is sent and never appear in logs or error text.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::model::{ChannelDef, ChannelKind, check_secret_shape};
use super::text::{Alert, Digest, discord_body, telegram_html, test_alert};
use crate::config::{DeliveryConfig, Secret};
use crate::metrics::{Families, MetricSource, unix_now_ms};
use crate::ws::Backoff;

/// Counters of one channel.
#[derive(Debug, Default)]
pub struct ChannelStats {
    /// Messages the platform accepted.
    pub sent: AtomicU64,
    /// Alerts contained in those messages.
    pub alerts_sent: AtomicU64,
    /// Failed attempts (HTTP errors, timeouts, refusals).
    pub failed: AtomicU64,
    /// Alerts dropped (queue overflow, or given up after the attempts ran out).
    pub dropped: AtomicU64,
    /// Retries after a failure or a rate-limit answer.
    pub retries: AtomicU64,
    /// `429` answers.
    pub rate_limited: AtomicU64,
    /// Messages logged instead of sent (dry run).
    pub dry_run: AtomicU64,
    /// Alerts refused because the channel is disabled.
    pub disabled_skipped: AtomicU64,
    /// Time of the last accepted message, ms (0 = never).
    pub last_success_ms: AtomicI64,
}

#[derive(Default)]
struct QueueState {
    items: VecDeque<Alert>,
    /// Alerts dropped and not yet mentioned in a message.
    dropped_unreported: u64,
}

struct ChannelRt {
    id: String,
    def: Mutex<ChannelDef>,
    queue: Mutex<QueueState>,
    notify: Notify,
    stats: ChannelStats,
    cancel: CancellationToken,
}

impl ChannelRt {
    fn def(&self) -> ChannelDef {
        self.def.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Why `push` did not queue an alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushResult {
    /// Queued.
    Queued,
    /// Queued, and the oldest alert was dropped to make room.
    QueuedDroppedOldest,
    /// The channel is disabled.
    Disabled,
    /// No such channel.
    Unknown,
}

/// Result of a test message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestOutcome {
    /// The platform accepted the message.
    pub delivered: bool,
    /// The channel is in dry-run mode.
    pub dry_run: bool,
    /// Short outcome without secrets.
    pub detail: String,
}

#[derive(Debug)]
enum SendError {
    /// Try again after this delay (the platform asked for it).
    RateLimited(Duration),
    /// Try again with backoff.
    Transient(String),
    /// Will not work by retrying.
    Permanent(String),
}

struct SendOk {
    /// The platform asks for this pause before the next call.
    pause: Option<Duration>,
}

/// The delivery service: all channels.
pub struct Delivery {
    cfg: DeliveryConfig,
    http: reqwest::Client,
    channels: RwLock<HashMap<String, Arc<ChannelRt>>>,
    shutdown: CancellationToken,
}

#[derive(Clone, Copy)]
struct Params {
    min_interval: Duration,
    max_per_minute: u32,
    batch_window: Duration,
    queue_limit: usize,
}

fn redact(text: &str, secrets: &[&str]) -> String {
    let mut out = text.to_string();
    for s in secrets {
        if s.len() >= 4 {
            out = out.replace(s, "<redacted>");
        }
    }
    out
}

impl Delivery {
    /// Builds the HTTP client. Channels are added with [`Delivery::apply`].
    pub fn new(cfg: DeliveryConfig, shutdown: CancellationToken) -> Result<Arc<Self>, String> {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let http = reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .connect_timeout(Duration::from_secs(cfg.request_timeout_secs.min(10)))
            .timeout(Duration::from_secs(cfg.request_timeout_secs))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("vulture-server/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| e.without_url().to_string())?;
        Ok(Arc::new(Self {
            cfg,
            http,
            channels: RwLock::new(HashMap::new()),
            shutdown,
        }))
    }

    /// The delivery settings.
    pub fn config(&self) -> &DeliveryConfig {
        &self.cfg
    }

    fn params(&self, def: &ChannelDef) -> Params {
        let (min_i, per_min) = match def.kind {
            ChannelKind::Telegram => (
                self.cfg.telegram_min_interval_ms,
                self.cfg.telegram_max_per_minute,
            ),
            ChannelKind::Discord => (
                self.cfg.discord_min_interval_ms,
                self.cfg.discord_max_per_minute,
            ),
        };
        let pick = |own: u32, default: u32| if own > 0 { own } else { default };
        Params {
            min_interval: Duration::from_millis(u64::from(pick(def.min_interval_ms, min_i))),
            max_per_minute: per_min,
            batch_window: Duration::from_millis(u64::from(if def.batch_window_ms > 0 {
                def.batch_window_ms
            } else {
                self.cfg.batch_window_ms
            })),
            queue_limit: if def.queue_limit > 0 {
                def.queue_limit as usize
            } else {
                self.cfg.queue_limit
            },
        }
    }

    /// Makes the running channels match `defs`: new ones start, changed ones are updated in place (their queue
    /// is kept), missing ones stop.
    pub fn apply(self: &Arc<Self>, defs: &[ChannelDef]) {
        let mut map = self.channels.write().unwrap_or_else(|e| e.into_inner());
        map.retain(|id, rt| {
            let keep = defs.iter().any(|d| &d.id == id);
            if !keep {
                rt.cancel.cancel();
            }
            keep
        });
        for def in defs {
            match map.get(&def.id) {
                Some(rt) => {
                    *rt.def.lock().unwrap_or_else(|e| e.into_inner()) = def.clone();
                    rt.notify.notify_one();
                }
                None => {
                    let rt = Arc::new(ChannelRt {
                        id: def.id.clone(),
                        def: Mutex::new(def.clone()),
                        queue: Mutex::new(QueueState::default()),
                        notify: Notify::new(),
                        stats: ChannelStats::default(),
                        cancel: self.shutdown.child_token(),
                    });
                    map.insert(def.id.clone(), rt.clone());
                    tokio::spawn(self.clone().run_channel(rt));
                }
            }
        }
    }

    /// Queues an alert for a channel.
    pub fn push(&self, channel_id: &str, alert: Alert) -> PushResult {
        let rt = {
            let map = self.channels.read().unwrap_or_else(|e| e.into_inner());
            match map.get(channel_id) {
                Some(rt) => rt.clone(),
                None => return PushResult::Unknown,
            }
        };
        let def = rt.def();
        if !def.enabled {
            rt.stats.disabled_skipped.fetch_add(1, Ordering::Relaxed);
            return PushResult::Disabled;
        }
        let limit = self.params(&def).queue_limit;
        let mut result = PushResult::Queued;
        {
            let mut q = rt.queue.lock().unwrap_or_else(|e| e.into_inner());
            while q.items.len() >= limit {
                q.items.pop_front();
                q.dropped_unreported += 1;
                rt.stats.dropped.fetch_add(1, Ordering::Relaxed);
                result = PushResult::QueuedDroppedOldest;
            }
            q.items.push_back(alert);
        }
        rt.notify.notify_one();
        result
    }

    /// Alerts waiting in a channel's queue.
    pub fn queue_depth(&self, channel_id: &str) -> usize {
        let map = self.channels.read().unwrap_or_else(|e| e.into_inner());
        map.get(channel_id).map_or(0, |rt| {
            rt.queue
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .items
                .len()
        })
    }

    /// Counters of a channel (tests, diagnostics).
    pub fn counters(&self, channel_id: &str) -> Option<[u64; 7]> {
        let map = self.channels.read().unwrap_or_else(|e| e.into_inner());
        map.get(channel_id).map(|rt| {
            let s = &rt.stats;
            let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
            [
                g(&s.sent),
                g(&s.alerts_sent),
                g(&s.failed),
                g(&s.dropped),
                g(&s.retries),
                g(&s.rate_limited),
                g(&s.dry_run),
            ]
        })
    }

    /// Sends a test message right away: no queue, no batching, no retry.
    pub async fn send_test(&self, def: &ChannelDef) -> TestOutcome {
        let alert = test_alert(
            if def.name.is_empty() {
                &def.id
            } else {
                &def.name
            },
            unix_now_ms(),
        );
        let digest = Digest {
            alerts: vec![alert],
            ..Default::default()
        };
        if def.dry_run {
            tracing::info!(channel = %def.id, kind = def.kind.as_str(), "dry run: test message not sent");
            return TestOutcome {
                delivered: false,
                dry_run: true,
                detail: "dry run".into(),
            };
        }
        match self.send_once(def, &digest).await {
            Ok(_) => TestOutcome {
                delivered: true,
                dry_run: false,
                detail: "ok".into(),
            },
            Err(SendError::RateLimited(d)) => TestOutcome {
                delivered: false,
                dry_run: false,
                detail: format!("rate limited, retry after {}s", d.as_secs().max(1)),
            },
            Err(SendError::Transient(m) | SendError::Permanent(m)) => TestOutcome {
                delivered: false,
                dry_run: false,
                detail: m,
            },
        }
    }

    async fn run_channel(self: Arc<Self>, rt: Arc<ChannelRt>) {
        let mut sent_at: VecDeque<Instant> = VecDeque::new();
        let mut not_before: Option<Instant> = None;
        let mut backoff = Backoff::new(
            Duration::from_millis(self.cfg.retry_min_ms),
            Duration::from_millis(self.cfg.retry_max_ms),
            None,
        );
        loop {
            // Wait for the first alert.
            loop {
                let empty = rt
                    .queue
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .items
                    .is_empty();
                if !empty {
                    break;
                }
                tokio::select! {
                    _ = rt.notify.notified() => {}
                    _ = rt.cancel.cancelled() => return,
                }
            }
            let def = rt.def();
            let params = self.params(&def);
            // Let more alerts of the same burst join.
            if !params.batch_window.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(params.batch_window) => {}
                    _ = rt.cancel.cancelled() => return,
                }
            }
            // Respect the rate limit.
            let now = Instant::now();
            let mut ready = not_before.unwrap_or(now);
            if let Some(last) = sent_at.back() {
                ready = ready.max(*last + params.min_interval);
            }
            while sent_at
                .front()
                .is_some_and(|t| now.duration_since(*t) >= Duration::from_secs(60))
            {
                sent_at.pop_front();
            }
            if sent_at.len() >= params.max_per_minute as usize
                && let Some(oldest) = sent_at.front()
            {
                ready = ready.max(*oldest + Duration::from_secs(60));
            }
            if ready > now {
                tokio::select! {
                    _ = tokio::time::sleep_until(ready) => {}
                    _ = rt.cancel.cancelled() => return,
                }
            }
            // Take everything queued.
            let (alerts, dropped) = {
                let mut q = rt.queue.lock().unwrap_or_else(|e| e.into_inner());
                (
                    std::mem::take(&mut q.items).into_iter().collect::<Vec<_>>(),
                    std::mem::take(&mut q.dropped_unreported),
                )
            };
            if alerts.is_empty() && dropped == 0 {
                continue;
            }
            let total = alerts.len();
            let listed = total.min(self.cfg.digest_max_items);
            let digest = Digest {
                alerts: alerts[..listed].to_vec(),
                more: total - listed,
                dropped,
            };
            let def = rt.def();
            if def.dry_run {
                let text = match def.kind {
                    ChannelKind::Telegram => telegram_html(&digest),
                    ChannelKind::Discord => discord_body(&digest).to_string(),
                };
                tracing::info!(channel = %def.id, kind = def.kind.as_str(), alerts = total, message = %text, "dry run: message not sent");
                rt.stats.dry_run.fetch_add(1, Ordering::Relaxed);
                sent_at.push_back(Instant::now());
                continue;
            }
            let mut attempts = 0u32;
            loop {
                let def = rt.def();
                match self.send_once(&def, &digest).await {
                    Ok(ok) => {
                        rt.stats.sent.fetch_add(1, Ordering::Relaxed);
                        rt.stats
                            .alerts_sent
                            .fetch_add(total as u64, Ordering::Relaxed);
                        rt.stats
                            .last_success_ms
                            .store(unix_now_ms(), Ordering::Relaxed);
                        backoff.reset();
                        sent_at.push_back(Instant::now());
                        not_before = ok.pause.map(|p| Instant::now() + p);
                        break;
                    }
                    Err(e) => {
                        attempts += 1;
                        rt.stats.failed.fetch_add(1, Ordering::Relaxed);
                        let (delay, give_up, why) = match e {
                            SendError::RateLimited(d) => {
                                rt.stats.rate_limited.fetch_add(1, Ordering::Relaxed);
                                (
                                    d,
                                    attempts >= self.cfg.max_attempts,
                                    "rate limited".to_string(),
                                )
                            }
                            SendError::Transient(m) => (
                                backoff.record_failure(),
                                attempts >= self.cfg.max_attempts,
                                m,
                            ),
                            SendError::Permanent(m) => (Duration::ZERO, true, m),
                        };
                        if give_up {
                            tracing::error!(channel = %rt.id, kind = def.kind.as_str(), alerts = total, attempts, reason = %why, "giving up on a message");
                            rt.stats.dropped.fetch_add(total as u64, Ordering::Relaxed);
                            // Tell the owner in the next message that these were lost.
                            rt.queue
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .dropped_unreported += total as u64 + digest.dropped;
                            break;
                        }
                        rt.stats.retries.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(channel = %rt.id, kind = def.kind.as_str(), attempts, retry_in_ms = delay.as_millis() as u64, reason = %why, "delivery failed; will retry");
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = rt.cancel.cancelled() => return,
                        }
                    }
                }
            }
        }
    }

    async fn send_once(&self, def: &ChannelDef, digest: &Digest) -> Result<SendOk, SendError> {
        let secret = def
            .resolve_secret()
            .map_err(|m| SendError::Transient(format!("secret unavailable: {m}")))?;
        // A secret read from a file or the environment gets the same shape check as an inline one, so that
        // an arbitrary file can never end up in a request URL.
        check_secret_shape(def.kind, secret.expose(), self.cfg.allow_custom_endpoints)
            .map_err(|_| SendError::Permanent("the secret has an unexpected shape".into()))?;
        match def.kind {
            ChannelKind::Telegram => self.send_telegram(def, &secret, digest).await,
            ChannelKind::Discord => self.send_discord(&secret, digest).await,
        }
    }

    async fn post(
        &self,
        url: String,
        body: Vec<u8>,
        secret: &Secret,
    ) -> Result<reqwest::Response, SendError> {
        self.http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| {
                let msg = e.without_url().to_string();
                SendError::Transient(redact(
                    &format!("request failed: {msg}"),
                    &[secret.expose()],
                ))
            })
    }

    async fn send_telegram(
        &self,
        def: &ChannelDef,
        secret: &Secret,
        digest: &Digest,
    ) -> Result<SendOk, SendError> {
        let url = format!(
            "{}/bot{}/sendMessage",
            self.cfg.telegram_base_url.trim_end_matches('/'),
            secret.expose()
        );
        let body = serde_json::json!({
            "chat_id": def.chat_id,
            "text": telegram_html(digest),
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });
        let resp = self
            .post(url, serde_json::to_vec(&body).unwrap_or_default(), secret)
            .await?;
        let status = resp.status();
        let header_retry = retry_after_header(&resp);
        let text = resp.text().await.unwrap_or_default();
        if status.is_success() {
            return Ok(SendOk { pause: None });
        }
        let json: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        let description = json
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("")
            .chars()
            .take(120)
            .collect::<String>();
        let description = redact(&description, &[secret.expose()]);
        if status.as_u16() == 429 {
            let secs = json
                .pointer("/parameters/retry_after")
                .and_then(Value::as_f64)
                .or(header_retry)
                .unwrap_or(1.0);
            return Err(SendError::RateLimited(clamp_wait(secs)));
        }
        let msg = format!("HTTP {} {description}", status.as_u16());
        if status.is_server_error() || status.as_u16() == 408 {
            Err(SendError::Transient(msg))
        } else {
            Err(SendError::Permanent(msg))
        }
    }

    async fn send_discord(&self, secret: &Secret, digest: &Digest) -> Result<SendOk, SendError> {
        let body = discord_body(digest);
        let resp = self
            .post(
                secret.expose().to_string(),
                serde_json::to_vec(&body).unwrap_or_default(),
                secret,
            )
            .await?;
        let status = resp.status();
        let header_retry = retry_after_header(&resp);
        let remaining = header_f64(&resp, "x-ratelimit-remaining");
        let reset_after = header_f64(&resp, "x-ratelimit-reset-after");
        let text = resp.text().await.unwrap_or_default();
        if status.is_success() {
            let pause = match (remaining, reset_after) {
                (Some(r), Some(after)) if r < 1.0 => Some(clamp_wait(after)),
                _ => None,
            };
            return Ok(SendOk { pause });
        }
        let json: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        if status.as_u16() == 429 {
            let secs = json
                .get("retry_after")
                .and_then(Value::as_f64)
                .or(header_retry)
                .unwrap_or(1.0);
            return Err(SendError::RateLimited(clamp_wait(secs)));
        }
        let message = redact(
            &json
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .chars()
                .take(120)
                .collect::<String>(),
            &[secret.expose()],
        );
        let msg = format!("HTTP {} {message}", status.as_u16());
        if status.is_server_error() || status.as_u16() == 408 {
            Err(SendError::Transient(msg))
        } else {
            Err(SendError::Permanent(msg))
        }
    }
}

fn header_f64(resp: &reqwest::Response, name: &str) -> Option<f64> {
    resp.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse().ok())
}

fn retry_after_header(resp: &reqwest::Response) -> Option<f64> {
    header_f64(resp, "retry-after")
}

fn clamp_wait(secs: f64) -> Duration {
    Duration::from_secs_f64(secs.clamp(0.0, 3600.0))
}

impl MetricSource for Delivery {
    fn collect(&self, out: &mut Families) {
        let map = self.channels.read().unwrap_or_else(|e| e.into_inner());
        let mut ids: Vec<&Arc<ChannelRt>> = map.values().collect();
        ids.sort_by(|a, b| a.id.cmp(&b.id));
        let now = unix_now_ms();
        for rt in ids {
            let kind = rt.def().kind.as_str();
            let l = [("channel", rt.id.as_str()), ("kind", kind)];
            let s = &rt.stats;
            let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
            out.counter(
                "vulture_alerts_channel_sent_total",
                "Messages the platform accepted.",
                &l,
                g(&s.sent),
            );
            out.counter(
                "vulture_alerts_channel_alerts_sent_total",
                "Alerts contained in accepted messages.",
                &l,
                g(&s.alerts_sent),
            );
            out.counter(
                "vulture_alerts_channel_failed_total",
                "Failed delivery attempts.",
                &l,
                g(&s.failed),
            );
            out.counter(
                "vulture_alerts_channel_dropped_total",
                "Alerts dropped by queue overflow or after the attempts ran out.",
                &l,
                g(&s.dropped),
            );
            out.counter(
                "vulture_alerts_channel_retries_total",
                "Retries after a failure.",
                &l,
                g(&s.retries),
            );
            out.counter(
                "vulture_alerts_channel_rate_limited_total",
                "429 answers from the platform.",
                &l,
                g(&s.rate_limited),
            );
            out.counter(
                "vulture_alerts_channel_dry_run_total",
                "Messages logged instead of sent.",
                &l,
                g(&s.dry_run),
            );
            out.counter(
                "vulture_alerts_channel_disabled_skipped_total",
                "Alerts refused because the channel is disabled.",
                &l,
                g(&s.disabled_skipped),
            );
            out.gauge(
                "vulture_alerts_channel_queue_depth",
                "Alerts waiting in the channel's queue.",
                &l,
                rt.queue
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .items
                    .len() as f64,
            );
            let last = s.last_success_ms.load(Ordering::Relaxed);
            if last > 0 {
                out.gauge(
                    "vulture_alerts_channel_last_success_age_seconds",
                    "Seconds since the last accepted message (absent before the first one).",
                    &l,
                    (now - last).max(0) as f64 / 1000.0,
                );
            }
        }
    }
}
