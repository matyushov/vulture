//! Rules and channels at runtime: validation, persistence, audit, and the hot hand-over to the engine and the
//! delivery. This is the backend of the rule and channel RPCs of `Control`.
//!
//! Every change is validated, then written to `alerts.json` (atomically), and only then applied; if the write
//! fails nothing changes. Repeating a request that changes nothing writes nothing and leaves no audit line. The
//! audit line (`<data_dir>/audit.log`) names the change, never a secret.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use serde::Serialize;

use super::delivery::{Delivery, TestOutcome};
use super::file::{ALERTS_VERSION, AlertsDoc, AlertsFile, AlertsFileError, validate_doc};
use super::model::{ChannelDef, MAX_CHANNELS, MAX_RULES, RuleDef};
use super::rules::RuleSet;
use crate::config::{Config, Secret};
use crate::metrics::{Families, MetricSource, unix_now_ms};
use crate::symbols::audit::{AuditLog, rfc3339};

/// Why a request was refused or failed.
#[derive(Debug, thiserror::Error)]
pub enum AlertsError {
    /// The request is malformed or not acceptable.
    #[error("{0}")]
    Invalid(String),
    /// The state does not allow it (for example a channel still used by rules).
    #[error("{0}")]
    Conflict(String),
    /// No such rule or channel.
    #[error("{0}")]
    NotFound(String),
    /// The file could not be written; nothing changed.
    #[error(transparent)]
    Persist(#[from] AlertsFileError),
}

#[derive(Debug, Serialize)]
struct AuditEntry<'a> {
    time: String,
    ts_ms: i64,
    event: &'a str,
    id: &'a str,
    name: &'a str,
    kind: &'a str,
    enabled: Option<bool>,
    secret_changed: bool,
    peer: &'a str,
}

impl<'a> AuditEntry<'a> {
    fn new(event: &'a str, id: &'a str, name: &'a str, kind: &'a str, peer: &'a str) -> Self {
        let now = unix_now_ms();
        Self {
            time: rfc3339(now),
            ts_ms: now,
            event,
            id,
            name,
            kind,
            enabled: None,
            secret_changed: false,
            peer,
        }
    }

    fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    fn secret_changed(mut self, changed: bool) -> Self {
        self.secret_changed = changed;
        self
    }
}

/// A channel as the API shows it: no secret, a mask.
#[derive(Debug, Clone)]
pub struct ChannelView {
    /// The definition (its inline secret is never copied into API replies).
    pub def: ChannelDef,
    /// `****abcd` or empty.
    pub mask: String,
}

/// The runtime owner of rules and channels.
pub struct AlertsManager {
    file: AlertsFile,
    state: tokio::sync::Mutex<AlertsDoc>,
    rules: Arc<ArcSwap<RuleSet>>,
    delivery: Arc<Delivery>,
    audit: Arc<AuditLog>,
    allow_custom: bool,
    changes: AtomicU64,
    rejected: AtomicU64,
}

impl std::fmt::Debug for AlertsManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlertsManager").finish_non_exhaustive()
    }
}

fn new_id(prefix: &str) -> String {
    let mut b = [0u8; 4];
    let _ = getrandom::fill(&mut b);
    format!("{prefix}-{:02x}{:02x}{:02x}{:02x}", b[0], b[1], b[2], b[3])
}

impl AlertsManager {
    /// Loads `<data_dir>/alerts.json` (a broken file is an error: the server must not start) or, without the
    /// file, the defaults of the configuration, and applies them.
    pub fn open(
        config: &Config,
        rules: Arc<ArcSwap<RuleSet>>,
        delivery: Arc<Delivery>,
    ) -> Result<Arc<Self>, AlertsFileError> {
        let allow_custom = config.alerts.delivery.allow_custom_endpoints;
        let file = AlertsFile::new(&config.server.data_dir);
        let doc = match file.load(allow_custom)? {
            Some(doc) => {
                tracing::info!(
                    rules = doc.rules.len(),
                    channels = doc.channels.len(),
                    "alert rules and channels loaded from the alerts file"
                );
                doc
            }
            None => {
                let mut doc = AlertsDoc {
                    version: ALERTS_VERSION,
                    rules: config.alerts.rules.clone(),
                    channels: config.alerts.channels.clone(),
                };
                validate_doc(&mut doc, allow_custom).map_err(|message| {
                    AlertsFileError::Corrupt {
                        path: file.path().to_path_buf(),
                        message: format!(
                            "the defaults in the configuration are invalid: {message}"
                        ),
                    }
                })?;
                tracing::info!(
                    rules = doc.rules.len(),
                    channels = doc.channels.len(),
                    "no alerts file yet: using the defaults of the configuration"
                );
                doc
            }
        };
        rules.store(Arc::new(RuleSet::compile(&doc.rules)));
        delivery.apply(&doc.channels);
        Ok(Arc::new(Self {
            file,
            state: tokio::sync::Mutex::new(doc),
            rules,
            delivery,
            audit: AuditLog::shared(&config.server.data_dir),
            allow_custom,
            changes: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }))
    }

    fn audit_line(&self, entry: AuditEntry<'_>) {
        if let Err(e) = self.audit.append_any(&entry) {
            tracing::error!(error = %e, "cannot write the audit log");
        }
        tracing::info!(
            event = entry.event,
            id = entry.id,
            kind = entry.kind,
            peer = entry.peer,
            "alerts configuration changed"
        );
    }

    /// Writes `doc`, then applies it to the engine and the delivery.
    fn commit(&self, state: &mut AlertsDoc, doc: AlertsDoc) -> Result<(), AlertsError> {
        self.file.save(&doc)?;
        self.rules.store(Arc::new(RuleSet::compile(&doc.rules)));
        self.delivery.apply(&doc.channels);
        *state = doc;
        self.changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// All rules, sorted by id.
    pub async fn list_rules(&self) -> Vec<RuleDef> {
        let mut rules = self.state.lock().await.rules.clone();
        rules.sort_by(|a, b| a.id.cmp(&b.id));
        rules
    }

    /// All channels with masked secrets, sorted by id.
    pub async fn list_channels(&self) -> Vec<ChannelView> {
        let mut channels = self.state.lock().await.channels.clone();
        channels.sort_by(|a, b| a.id.cmp(&b.id));
        channels
            .into_iter()
            .map(|def| ChannelView {
                mask: def.secret_mask(),
                def,
            })
            .collect()
    }

    /// Creates or replaces a rule (an empty id creates one). Idempotent.
    pub async fn upsert_rule(&self, mut rule: RuleDef, peer: &str) -> Result<RuleDef, AlertsError> {
        rule.validate().map_err(|e| self.refuse(e.0))?;
        let mut state = self.state.lock().await;
        for c in &rule.channels {
            if !state.channels.iter().any(|ch| &ch.id == c) {
                return Err(self.refuse(format!("channel {c:?} does not exist")));
            }
        }
        let mut doc = state.clone();
        let created = rule.id.is_empty();
        if created {
            rule.id = new_id("r");
        }
        match doc.rules.iter_mut().find(|r| r.id == rule.id) {
            Some(existing) if *existing == rule => return Ok(rule),
            Some(existing) => *existing = rule.clone(),
            None => {
                if doc.rules.len() >= MAX_RULES {
                    return Err(self.refuse(format!("at most {MAX_RULES} rules")));
                }
                doc.rules.push(rule.clone());
            }
        }
        self.commit(&mut state, doc)?;
        self.audit_line(
            AuditEntry::new("rule_upsert", &rule.id, &rule.name, "rule", peer)
                .enabled(rule.enabled),
        );
        Ok(rule)
    }

    /// Deletes a rule. Deleting a rule that does not exist succeeds (returns false).
    pub async fn delete_rule(&self, id: &str, peer: &str) -> Result<bool, AlertsError> {
        let mut state = self.state.lock().await;
        let Some(pos) = state.rules.iter().position(|r| r.id == id) else {
            return Ok(false);
        };
        let mut doc = state.clone();
        let removed = doc.rules.remove(pos);
        self.commit(&mut state, doc)?;
        self.audit_line(AuditEntry::new(
            "rule_delete",
            &removed.id,
            &removed.name,
            "rule",
            peer,
        ));
        Ok(true)
    }

    /// Creates or replaces a channel. `secret` is the bot token or webhook URL of the request; empty keeps the
    /// current secret (unless the channel names a `secret_file` or `secret_env`). Idempotent.
    pub async fn upsert_channel(
        &self,
        mut channel: ChannelDef,
        secret: &str,
        peer: &str,
    ) -> Result<ChannelDef, AlertsError> {
        // The request never carries an inline secret in the channel itself.
        channel.secret = None;
        let mut state = self.state.lock().await;
        let created = channel.id.is_empty();
        if created {
            channel.id = new_id("c");
        }
        let existing = state.channels.iter().find(|c| c.id == channel.id).cloned();
        let secret = secret.trim();
        if !secret.is_empty() {
            channel.secret = Some(Secret::new(secret));
        } else if channel.secret_file.is_empty()
            && channel.secret_env.is_empty()
            && let Some(old) = &existing
        {
            channel.secret = old.secret.clone();
            channel.secret_file = old.secret_file.clone();
            channel.secret_env = old.secret_env.clone();
        }
        channel
            .validate(self.allow_custom)
            .map_err(|e| self.refuse(e.0))?;
        if !channel.has_secret() {
            return Err(self.refuse("a channel needs a secret (bot token or webhook URL)".into()));
        }
        let secret_changed = existing.as_ref().is_none_or(|o| {
            o.secret != channel.secret
                || o.secret_file != channel.secret_file
                || o.secret_env != channel.secret_env
        });
        let mut doc = state.clone();
        match doc.channels.iter_mut().find(|c| c.id == channel.id) {
            Some(old) if *old == channel => return Ok(channel),
            Some(old) => *old = channel.clone(),
            None => {
                if doc.channels.len() >= MAX_CHANNELS {
                    return Err(self.refuse(format!("at most {MAX_CHANNELS} channels")));
                }
                doc.channels.push(channel.clone());
            }
        }
        self.commit(&mut state, doc)?;
        self.audit_line(
            AuditEntry::new(
                "channel_upsert",
                &channel.id,
                &channel.name,
                channel.kind.as_str(),
                peer,
            )
            .enabled(channel.enabled)
            .secret_changed(secret_changed),
        );
        Ok(channel)
    }

    /// Deletes a channel that no rule uses. Deleting one that does not exist succeeds (returns false).
    pub async fn delete_channel(&self, id: &str, peer: &str) -> Result<bool, AlertsError> {
        let mut state = self.state.lock().await;
        let Some(pos) = state.channels.iter().position(|c| c.id == id) else {
            return Ok(false);
        };
        let users: Vec<&str> = state
            .rules
            .iter()
            .filter(|r| r.channels.iter().any(|c| c == id))
            .map(|r| r.id.as_str())
            .collect();
        if !users.is_empty() {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(AlertsError::Conflict(format!(
                "channel {id:?} is used by rules: {}",
                users.join(", ")
            )));
        }
        let mut doc = state.clone();
        let removed = doc.channels.remove(pos);
        self.commit(&mut state, doc)?;
        self.audit_line(AuditEntry::new(
            "channel_delete",
            &removed.id,
            &removed.name,
            removed.kind.as_str(),
            peer,
        ));
        Ok(true)
    }

    /// Sends a test message to a channel.
    pub async fn send_test(&self, id: &str, peer: &str) -> Result<TestOutcome, AlertsError> {
        let def = {
            let state = self.state.lock().await;
            state.channels.iter().find(|c| c.id == id).cloned()
        }
        .ok_or_else(|| AlertsError::NotFound(format!("channel {id:?} does not exist")))?;
        let outcome = self.delivery.send_test(&def).await;
        self.audit_line(
            AuditEntry::new("channel_test", &def.id, &def.name, def.kind.as_str(), peer)
                .enabled(def.enabled),
        );
        Ok(outcome)
    }

    fn refuse(&self, message: String) -> AlertsError {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        AlertsError::Invalid(message)
    }

    /// Ids of the channels currently defined.
    pub async fn channel_ids(&self) -> HashSet<String> {
        self.state
            .lock()
            .await
            .channels
            .iter()
            .map(|c| c.id.clone())
            .collect()
    }
}

impl MetricSource for AlertsManager {
    fn collect(&self, out: &mut Families) {
        out.counter(
            "vulture_alerts_config_changes_total",
            "Changes to rules and channels applied through Control.",
            &[],
            self.changes.load(Ordering::Relaxed),
        );
        out.counter(
            "vulture_alerts_config_rejected_total",
            "Rule and channel changes refused.",
            &[],
            self.rejected.load(Ordering::Relaxed),
        );
        let rules = self.rules.load();
        out.gauge(
            "vulture_alerts_rules_active",
            "Enabled rules in force (detector overrides included).",
            &[],
            (rules.rules.len() + rules.overrides.len()) as f64,
        );
    }
}
