//! The live link between the symbol control and one connector.
//!
//! The connector owns a [`SymbolChannel`] and exposes it through [`crate::exchange::Exchange::symbols`]. The
//! control side writes the policy; the connector reads the policy and publishes its universe (every symbol the
//! exchange offers, normalized). Both directions are `tokio::sync::watch` values, so a receiver always sees the
//! latest state and a change wakes a task that waits on it.

use std::sync::Arc;

use tokio::sync::watch;

use super::policy::{SymbolPolicy, Universe};

/// Policy and universe of one exchange.
#[derive(Debug)]
pub struct SymbolChannel {
    policy: watch::Sender<Arc<SymbolPolicy>>,
    universe: watch::Sender<Option<Arc<Universe>>>,
    default_policy: Arc<SymbolPolicy>,
}

impl SymbolChannel {
    /// A channel whose initial policy is `default_policy` (from the configuration). The universe is unknown
    /// until the connector publishes it.
    pub fn new(default_policy: SymbolPolicy) -> Arc<Self> {
        let default_policy = Arc::new(default_policy);
        let (policy, _) = watch::channel(default_policy.clone());
        let (universe, _) = watch::channel(None);
        Arc::new(Self {
            policy,
            universe,
            default_policy,
        })
    }

    /// The policy that came from the configuration.
    pub fn default_policy(&self) -> Arc<SymbolPolicy> {
        self.default_policy.clone()
    }

    /// The policy in force.
    pub fn policy(&self) -> Arc<SymbolPolicy> {
        self.policy.borrow().clone()
    }

    /// Receiver for connector tasks.
    pub fn subscribe_policy(&self) -> watch::Receiver<Arc<SymbolPolicy>> {
        self.policy.subscribe()
    }

    /// Replaces the policy; waiting connector tasks wake up.
    pub fn set_policy(&self, policy: SymbolPolicy) {
        self.policy.send_replace(Arc::new(policy));
    }

    /// The universe, once the connector has published it.
    pub fn universe(&self) -> Option<Arc<Universe>> {
        self.universe.borrow().clone()
    }

    /// Receiver of the universe.
    pub fn subscribe_universe(&self) -> watch::Receiver<Option<Arc<Universe>>> {
        self.universe.subscribe()
    }

    /// Called by the connector after every (re)load of the symbol list.
    pub fn publish_universe(&self, universe: Universe) {
        self.universe.send_replace(Some(Arc::new(universe)));
    }

    /// Number of tracked symbols for the current policy and universe (0 while the universe is unknown).
    pub fn tracked_count(&self) -> usize {
        match self.universe() {
            Some(u) => {
                let p = self.policy();
                u.iter().filter(|s| p.allows(s)).count()
            }
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::symbols::Mode;

    #[tokio::test]
    async fn receivers_see_changes() {
        let ch = SymbolChannel::new(SymbolPolicy::all());
        let mut p = ch.subscribe_policy();
        let mut u = ch.subscribe_universe();
        assert_eq!(ch.tracked_count(), 0);
        ch.publish_universe(
            ["A-USDT-PERP", "B-USDT-PERP"]
                .into_iter()
                .map(String::from)
                .collect(),
        );
        u.changed().await.unwrap();
        assert_eq!(ch.tracked_count(), 2);
        ch.set_policy(SymbolPolicy::new(Mode::Whitelist, ["A-USDT-PERP"]).unwrap());
        p.changed().await.unwrap();
        assert_eq!(p.borrow().mode(), Mode::Whitelist);
        assert_eq!(ch.tracked_count(), 1);
        // The configured default is kept for reference.
        assert_eq!(*ch.default_policy(), SymbolPolicy::all());
    }
}
