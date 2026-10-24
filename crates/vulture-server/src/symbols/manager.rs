//! The symbol control: validation, persistence, audit and the hand-over to the connectors.
//!
//! One [`SymbolManager`] serves all exchanges. [`SymbolManager::set_policy`] is the only writer; it runs under a
//! lock so concurrent changes are applied one after the other, and it persists the state file *before* the new
//! policy reaches the connector and before the caller is answered.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::audit::{AuditLog, AuditRecord, rfc3339};
use super::channel::SymbolChannel;
use super::policy::{Mode, PolicyError, SetDiff, SymbolPolicy};
use super::state::{State, StateError, StateStore};
use crate::config::Config;
use crate::metrics::{Families, MetricSource};

/// Why a change was refused or failed.
#[derive(Debug, thiserror::Error)]
pub enum SetError {
    /// The exchange is not in the configuration.
    #[error("exchange {0:?} is not configured")]
    UnknownExchange(String),
    /// Configured but switched off, or without symbol support.
    #[error("exchange {0:?} is not enabled or does not support symbol policies yet")]
    NotRunning(String),
    /// The policy itself is invalid.
    #[error(transparent)]
    Policy(#[from] PolicyError),
    /// Names that the exchange does not list. A typo must not silently track nothing.
    #[error("symbols not offered by {exchange}: {}", .symbols.join(", "))]
    UnknownSymbols {
        /// Exchange.
        exchange: String,
        /// The unknown names.
        symbols: Vec<String>,
    },
    /// The universe has not been loaded yet, so names cannot be checked.
    #[error("the symbol list of {0:?} has not been loaded yet; retry in a few seconds")]
    UniverseNotLoaded(String),
    /// The state file could not be written; nothing changed.
    #[error(transparent)]
    Persist(#[from] StateError),
}

/// Result of a successful [`SymbolManager::set_policy`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetOutcome {
    /// The stored policy.
    pub policy: SymbolPolicy,
    /// Tracked symbols after the change.
    pub tracked: usize,
    /// Symbols the exchange offers.
    pub universe: usize,
    /// Symbols that started to be tracked.
    pub added: Vec<String>,
    /// Symbols that stopped being tracked.
    pub removed: Vec<String>,
    /// False when the request repeated the policy already in force.
    pub changed: bool,
}

/// Current policy of one exchange with the counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyView {
    /// Exchange.
    pub exchange: String,
    /// Policy in force.
    pub policy: SymbolPolicy,
    /// Tracked symbols.
    pub tracked: usize,
    /// Symbols the exchange offers (0 until loaded).
    pub universe: usize,
    /// Whether the universe has been loaded.
    pub universe_loaded: bool,
    /// Whether the policy comes from the state file (true) or from the configuration default (false).
    pub from_state: bool,
}

#[derive(Default)]
struct Counters {
    applied: AtomicU64,
    unchanged: AtomicU64,
    rejected: AtomicU64,
    error: AtomicU64,
}

struct Entry {
    channel: Arc<SymbolChannel>,
    counters: Counters,
}

/// Owner-managed symbol policies of all running exchanges.
pub struct SymbolManager {
    entries: BTreeMap<String, Entry>,
    /// Exchanges named in the configuration (enabled or not), to tell "unknown" from "not running".
    configured: BTreeSet<String>,
    store: StateStore,
    audit: Arc<AuditLog>,
    /// The persisted state; held while a change is validated, stored and applied.
    state: tokio::sync::Mutex<State>,
    unknown_exchange_rejected: AtomicU64,
}

impl std::fmt::Debug for SymbolManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SymbolManager")
            .field("exchanges", &self.entries.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl SymbolManager {
    /// Loads `<data_dir>/state.json` (a broken file is an error: the server must not start), applies the
    /// stored policies over the configuration defaults and returns the manager.
    ///
    /// `channels` are the symbol channels of the running connectors by exchange name.
    pub fn new(
        config: &Config,
        channels: Vec<(String, Arc<SymbolChannel>)>,
    ) -> Result<Arc<Self>, StateError> {
        let store = StateStore::new(&config.server.data_dir);
        let state = store.load()?;
        let audit = Arc::new(AuditLog::new(&config.server.data_dir));
        let mut entries = BTreeMap::new();
        for (name, channel) in channels {
            if let Some(policy) = state.policies.get(&name) {
                channel.set_policy(policy.clone());
                tracing::info!(exchange = %name, mode = policy.mode().as_str(), symbols = policy.symbols().len(), "symbol policy restored from the state file");
            } else {
                let policy = channel.policy();
                tracing::info!(exchange = %name, mode = policy.mode().as_str(), symbols = policy.symbols().len(), "symbol policy from the configuration");
            }
            entries.insert(
                name,
                Entry {
                    channel,
                    counters: Counters::default(),
                },
            );
        }
        for name in state.policies.keys() {
            if !entries.contains_key(name) {
                tracing::warn!(exchange = %name, "the state file holds a symbol policy for an exchange that is not running; kept, not applied");
            }
        }
        Ok(Arc::new(Self {
            entries,
            configured: config.exchanges.iter().map(|e| e.name.clone()).collect(),
            store,
            audit,
            state: tokio::sync::Mutex::new(state),
            unknown_exchange_rejected: AtomicU64::new(0),
        }))
    }

    /// The audit log (tests read its path).
    pub fn audit(&self) -> &AuditLog {
        &self.audit
    }

    /// Exchanges that have a symbol policy, sorted by name.
    pub fn exchanges(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    /// Policies of `exchanges` (all when empty).
    pub async fn list(&self, exchanges: &[String]) -> Result<Vec<PolicyView>, SetError> {
        let names: Vec<String> = if exchanges.is_empty() {
            self.exchanges()
        } else {
            let mut seen = BTreeSet::new();
            let mut out = Vec::new();
            for n in exchanges {
                self.entry(n)?;
                if seen.insert(n.clone()) {
                    out.push(n.clone());
                }
            }
            out
        };
        let state = self.state.lock().await;
        Ok(names
            .into_iter()
            .filter_map(|name| {
                let entry = self.entries.get(&name)?;
                let universe = entry.channel.universe();
                Some(PolicyView {
                    from_state: state.policies.contains_key(&name),
                    policy: (*entry.channel.policy()).clone(),
                    tracked: entry.channel.tracked_count(),
                    universe: universe.as_ref().map_or(0, |u| u.len()),
                    universe_loaded: universe.is_some(),
                    exchange: name,
                })
            })
            .collect())
    }

    fn entry(&self, exchange: &str) -> Result<&Entry, SetError> {
        match self.entries.get(exchange) {
            Some(e) => Ok(e),
            None if self.configured.contains(exchange) => {
                Err(SetError::NotRunning(exchange.to_string()))
            }
            None => Err(SetError::UnknownExchange(exchange.to_string())),
        }
    }

    /// Replaces the policy of `exchange`. Idempotent. `peer` goes to the audit log.
    pub async fn set_policy(
        &self,
        exchange: &str,
        mode: Mode,
        symbols: &[String],
        peer: &str,
    ) -> Result<SetOutcome, SetError> {
        let entry = match self.entry(exchange) {
            Ok(e) => e,
            Err(e) => {
                if matches!(e, SetError::UnknownExchange(_)) {
                    self.unknown_exchange_rejected
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Err(e);
            }
        };
        match self.set_inner(exchange, entry, mode, symbols, peer).await {
            Ok(outcome) => {
                let counter = if outcome.changed {
                    &entry.counters.applied
                } else {
                    &entry.counters.unchanged
                };
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(outcome)
            }
            Err(e) => {
                let counter = match e {
                    SetError::Persist(_) => &entry.counters.error,
                    _ => &entry.counters.rejected,
                };
                counter.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(exchange, error = %e, peer, "symbol policy change refused");
                Err(e)
            }
        }
    }

    async fn set_inner(
        &self,
        exchange: &str,
        entry: &Entry,
        mode: Mode,
        symbols: &[String],
        peer: &str,
    ) -> Result<SetOutcome, SetError> {
        let new = SymbolPolicy::new(mode, symbols)?;
        let mut state = self.state.lock().await;
        let universe = entry.channel.universe();
        if new.mode() != Mode::All || !new.symbols().is_empty() {
            let Some(universe) = &universe else {
                if !new.symbols().is_empty() {
                    return Err(SetError::UniverseNotLoaded(exchange.to_string()));
                }
                return self.finish(exchange, entry, new, &mut state, peer).await;
            };
            let unknown = new.unknown(universe);
            if !unknown.is_empty() {
                return Err(SetError::UnknownSymbols {
                    exchange: exchange.to_string(),
                    symbols: unknown,
                });
            }
        }
        self.finish(exchange, entry, new, &mut state, peer).await
    }

    async fn finish(
        &self,
        exchange: &str,
        entry: &Entry,
        new: SymbolPolicy,
        state: &mut State,
        peer: &str,
    ) -> Result<SetOutcome, SetError> {
        let universe = entry.channel.universe();
        let empty = BTreeSet::new();
        let uni_ref = universe.as_deref().unwrap_or(&empty);
        let old = entry.channel.policy();
        let old_set = old.effective(uni_ref);
        let new_set = new.effective(uni_ref);
        let diff = SetDiff::between(&old_set, &new_set);
        let policy_changed = *old != new;
        let stored = state.policies.get(exchange) == Some(&new);
        let outcome = SetOutcome {
            policy: new.clone(),
            tracked: new_set.len(),
            universe: uni_ref.len(),
            added: diff.added,
            removed: diff.removed,
            changed: policy_changed,
        };
        if !policy_changed && stored {
            return Ok(outcome);
        }
        // Persist first: a policy that was acknowledged must survive a restart.
        let mut next = state.clone();
        next.policies.insert(exchange.to_string(), new.clone());
        let store = self.store.clone();
        let to_write = next.clone();
        tokio::task::spawn_blocking(move || store.save(&to_write))
            .await
            .map_err(|e| StateError::Write {
                path: self.store.path().to_path_buf(),
                source: std::io::Error::other(e),
            })??;
        *state = next;
        entry.channel.set_policy(new.clone());
        if policy_changed {
            self.record(exchange, &new, &old_set, &outcome, peer).await;
        }
        Ok(outcome)
    }

    async fn record(
        &self,
        exchange: &str,
        policy: &SymbolPolicy,
        old_set: &BTreeSet<String>,
        outcome: &SetOutcome,
        peer: &str,
    ) {
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        let record = AuditRecord {
            time: rfc3339(ts_ms),
            ts_ms,
            event: "symbol_policy",
            exchange: exchange.to_string(),
            mode: policy.mode().as_str().to_string(),
            policy_symbols: policy.symbols().len(),
            added: outcome.added.clone(),
            removed: outcome.removed.clone(),
            tracked_before: old_set.len(),
            tracked_after: outcome.tracked,
            universe: outcome.universe,
            peer: peer.to_string(),
        };
        tracing::info!(
            exchange,
            mode = record.mode,
            policy_symbols = record.policy_symbols,
            added = record.added.len(),
            removed = record.removed.len(),
            tracked_before = record.tracked_before,
            tracked_after = record.tracked_after,
            peer,
            "symbol policy changed"
        );
        let audit = self.audit.clone();
        match tokio::task::spawn_blocking(move || audit.append(&record)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "cannot append to the audit log"),
            Err(e) => tracing::error!(error = %e, "audit log task failed"),
        }
    }
}

impl MetricSource for SymbolManager {
    fn collect(&self, out: &mut Families) {
        for (name, entry) in &self.entries {
            let x = [("exchange", name.as_str())];
            out.gauge(
                "vulture_symbols_universe",
                "Symbols the exchange offers (linear USDT/USDC perpetuals).",
                &x,
                entry.channel.universe().map_or(0, |u| u.len()) as f64,
            );
            out.gauge(
                "vulture_symbols_tracked",
                "Symbols tracked under the current policy.",
                &x,
                entry.channel.tracked_count() as f64,
            );
            let c = &entry.counters;
            for (result, v) in [
                ("applied", &c.applied),
                ("unchanged", &c.unchanged),
                ("rejected", &c.rejected),
                ("error", &c.error),
            ] {
                out.counter(
                    "vulture_symbol_policy_changes_total",
                    "SetSymbolPolicy requests by result (applied, unchanged, rejected, error).",
                    &[("exchange", name.as_str()), ("result", result)],
                    v.load(Ordering::Relaxed),
                );
            }
        }
        out.counter(
            "vulture_symbol_policy_changes_total",
            "SetSymbolPolicy requests by result (applied, unchanged, rejected, error).",
            &[("exchange", "unknown"), ("result", "rejected")],
            self.unknown_exchange_rejected.load(Ordering::Relaxed),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExchangeConfig;

    fn config(dir: &std::path::Path) -> Config {
        let mut cfg = Config::default();
        cfg.server.data_dir = dir.to_path_buf();
        for (name, enabled) in [("binance", true), ("okx", false)] {
            cfg.exchanges.push(ExchangeConfig {
                name: name.into(),
                enabled,
                symbols: vec![],
                oi_poll_interval_secs: 30,
                ws_url: None,
                rest_url: None,
            });
        }
        cfg
    }

    fn universe(ch: &SymbolChannel) {
        ch.publish_universe(
            ["BTC-USDT-PERP", "ETH-USDT-PERP", "SOL-USDT-PERP"]
                .into_iter()
                .map(String::from)
                .collect(),
        );
    }

    fn setup(dir: &std::path::Path) -> (Arc<SymbolManager>, Arc<SymbolChannel>) {
        let ch = SymbolChannel::new(SymbolPolicy::all());
        universe(&ch);
        let m = SymbolManager::new(&config(dir), vec![("binance".into(), ch.clone())]).unwrap();
        (m, ch)
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn set_applies_persists_and_reports_the_diff() {
        let dir = tempfile::tempdir().unwrap();
        let (m, ch) = setup(dir.path());
        let out = m
            .set_policy(
                "binance",
                Mode::Blacklist,
                &names(&["eth-usdt-perp"]),
                "1.2.3.4:5",
            )
            .await
            .unwrap();
        assert!(out.changed);
        assert_eq!(out.tracked, 2);
        assert_eq!(out.universe, 3);
        assert_eq!(out.removed, ["ETH-USDT-PERP"]);
        assert!(out.added.is_empty());
        assert_eq!(ch.policy().mode(), Mode::Blacklist);
        // Persisted: a new manager over the same directory restores it.
        let ch2 = SymbolChannel::new(SymbolPolicy::all());
        let _m2 =
            SymbolManager::new(&config(dir.path()), vec![("binance".into(), ch2.clone())]).unwrap();
        assert_eq!(ch2.policy().symbols().len(), 1);
        assert_eq!(ch2.policy().mode(), Mode::Blacklist);
        // Audited.
        let log = std::fs::read_to_string(m.audit().path()).unwrap();
        assert_eq!(log.lines().count(), 1);
        assert!(log.contains("\"peer\":\"1.2.3.4:5\"") && log.contains("ETH-USDT-PERP"));
    }

    #[tokio::test]
    async fn repeat_is_idempotent_and_not_audited_twice() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _ch) = setup(dir.path());
        let first = m
            .set_policy("binance", Mode::Whitelist, &names(&["BTC-USDT-PERP"]), "p")
            .await
            .unwrap();
        let second = m
            .set_policy("binance", Mode::Whitelist, &names(&["BTC-USDT-PERP"]), "p")
            .await
            .unwrap();
        assert!(first.changed && !second.changed);
        assert!(second.added.is_empty() && second.removed.is_empty());
        assert_eq!(first.policy, second.policy);
        assert_eq!(second.tracked, 1);
        let log = std::fs::read_to_string(m.audit().path()).unwrap();
        assert_eq!(log.lines().count(), 1);
    }

    #[tokio::test]
    async fn setting_the_default_pins_it_in_the_state_file() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _ch) = setup(dir.path());
        let out = m.set_policy("binance", Mode::All, &[], "p").await.unwrap();
        assert!(!out.changed);
        assert!(
            StateStore::new(dir.path())
                .load()
                .unwrap()
                .policies
                .contains_key("binance")
        );
        let v = m.list(&[]).await.unwrap();
        assert!(v[0].from_state);
    }

    #[tokio::test]
    async fn validation() {
        let dir = tempfile::tempdir().unwrap();
        let (m, ch) = setup(dir.path());
        let e = m.set_policy("nope", Mode::All, &[], "p").await.unwrap_err();
        assert!(matches!(e, SetError::UnknownExchange(_)));
        let e = m.set_policy("okx", Mode::All, &[], "p").await.unwrap_err();
        assert!(matches!(e, SetError::NotRunning(_)));
        let e = m
            .set_policy("binance", Mode::Whitelist, &[], "p")
            .await
            .unwrap_err();
        assert!(matches!(e, SetError::Policy(PolicyError::EmptyWhitelist)));
        let e = m
            .set_policy("binance", Mode::Blacklist, &names(&["junk"]), "p")
            .await
            .unwrap_err();
        assert!(matches!(e, SetError::Policy(PolicyError::Malformed(_))));
        let e = m
            .set_policy(
                "binance",
                Mode::Whitelist,
                &names(&["BTC-USDT-PERP", "TYPO-USDT-PERP"]),
                "p",
            )
            .await
            .unwrap_err();
        match e {
            SetError::UnknownSymbols { symbols, .. } => assert_eq!(symbols, ["TYPO-USDT-PERP"]),
            other => panic!("{other}"),
        }
        // Nothing changed, nothing written.
        assert_eq!(*ch.policy(), SymbolPolicy::all());
        assert!(!StateStore::new(dir.path()).path().exists());
        assert!(!m.audit().path().exists());
    }

    #[tokio::test]
    async fn names_cannot_be_checked_before_the_universe_is_loaded() {
        let dir = tempfile::tempdir().unwrap();
        let ch = SymbolChannel::new(SymbolPolicy::all());
        let m = SymbolManager::new(&config(dir.path()), vec![("binance".into(), ch)]).unwrap();
        let e = m
            .set_policy("binance", Mode::Whitelist, &names(&["BTC-USDT-PERP"]), "p")
            .await
            .unwrap_err();
        assert!(matches!(e, SetError::UniverseNotLoaded(_)));
        // ALL needs no names.
        assert!(m.set_policy("binance", Mode::All, &[], "p").await.is_ok());
    }

    #[tokio::test]
    async fn failed_persist_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let ch = SymbolChannel::new(SymbolPolicy::all());
        universe(&ch);
        let mut cfg = config(dir.path());
        let m = SymbolManager::new(&cfg, vec![("binance".into(), ch.clone())]).unwrap();
        // Remove the data directory under the running manager.
        drop(dir);
        cfg.server.data_dir = std::path::PathBuf::new();
        let e = m
            .set_policy("binance", Mode::Blacklist, &names(&["ETH-USDT-PERP"]), "p")
            .await
            .unwrap_err();
        assert!(matches!(e, SetError::Persist(_)), "{e}");
        assert_eq!(*ch.policy(), SymbolPolicy::all());
    }

    #[tokio::test]
    async fn config_default_and_state_override() {
        let dir = tempfile::tempdir().unwrap();
        let ch = SymbolChannel::new(SymbolPolicy::new(Mode::Whitelist, ["BTC-USDT-PERP"]).unwrap());
        universe(&ch);
        let m =
            SymbolManager::new(&config(dir.path()), vec![("binance".into(), ch.clone())]).unwrap();
        assert!(!m.list(&[]).await.unwrap()[0].from_state);
        assert_eq!(m.list(&[]).await.unwrap()[0].tracked, 1);
        m.set_policy("binance", Mode::All, &[], "p").await.unwrap();
        // After a restart the state wins over the (unchanged) configuration default.
        let ch2 =
            SymbolChannel::new(SymbolPolicy::new(Mode::Whitelist, ["BTC-USDT-PERP"]).unwrap());
        universe(&ch2);
        let m2 =
            SymbolManager::new(&config(dir.path()), vec![("binance".into(), ch2.clone())]).unwrap();
        assert_eq!(*ch2.policy(), SymbolPolicy::all());
        assert_eq!(m2.list(&[]).await.unwrap()[0].tracked, 3);
    }

    #[test]
    fn a_broken_state_file_stops_the_start() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("state.json"), "{ not json").unwrap();
        let ch = SymbolChannel::new(SymbolPolicy::all());
        assert!(matches!(
            SymbolManager::new(&config(dir.path()), vec![("binance".into(), ch)]),
            Err(StateError::Corrupt { .. })
        ));
    }

    #[tokio::test]
    async fn metrics_series() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _ch) = setup(dir.path());
        let _ = m.set_policy("binance", Mode::Whitelist, &[], "p").await;
        m.set_policy("binance", Mode::Blacklist, &names(&["ETH-USDT-PERP"]), "p")
            .await
            .unwrap();
        let _ = m.set_policy("zzz", Mode::All, &[], "p").await;
        let mut f = Families::new();
        m.collect(&mut f);
        let text = f.render();
        for needle in [
            "vulture_symbols_universe{exchange=\"binance\"} 3",
            "vulture_symbols_tracked{exchange=\"binance\"} 2",
            "vulture_symbol_policy_changes_total{exchange=\"binance\",result=\"applied\"} 1",
            "vulture_symbol_policy_changes_total{exchange=\"binance\",result=\"rejected\"} 1",
            "vulture_symbol_policy_changes_total{exchange=\"unknown\",result=\"rejected\"} 1",
        ] {
            assert!(text.contains(needle), "{needle}\n{text}");
        }
    }
}
