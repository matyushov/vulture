//! Runtime topic subscriptions of a managed connection.
//!
//! A connector that wants to change what a live connection listens to holds a [`TopicControl`], which carries
//! the *desired* set of topics. Every session of the connection follows it: right after connecting it
//! subscribes to the whole set, and from then on it sends `subscribe` / `unsubscribe` requests for the
//! difference whenever the set changes, without reconnecting. Because the state is a set and not a queue of
//! commands, a reconnect needs no special handling (the new session starts with nothing subscribed and sends
//! the whole set), and a burst of changes collapses into one diff.
//!
//! The wire format belongs to the [`crate::ws::WsHandler`]; this module owns the bookkeeping and the helper
//! that splits a request to respect per-message size limits.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

/// Direction of a topic request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicOp {
    /// Start receiving the topics.
    Subscribe,
    /// Stop receiving the topics.
    Unsubscribe,
}

/// One request frame built by the handler, with the topics it covers (for acknowledgement tracking).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRequest {
    /// Correlation id the exchange echoes in its response.
    pub id: String,
    /// Text to send.
    pub text: String,
    /// Topics in this request.
    pub topics: Vec<String>,
}

/// The desired topics of one connection and the topics the exchange refused.
#[derive(Debug)]
pub struct TopicControl {
    desired: watch::Sender<Arc<BTreeSet<String>>>,
    rejected: Mutex<BTreeMap<String, String>>,
}

impl TopicControl {
    /// A control with an initial desired set (may be empty).
    pub fn new(initial: impl IntoIterator<Item = String>) -> Arc<Self> {
        let (desired, _) = watch::channel(Arc::new(initial.into_iter().collect()));
        Arc::new(Self {
            desired,
            rejected: Mutex::new(BTreeMap::new()),
        })
    }

    /// Replaces the desired set. Returns whether it changed. Rejected topics that are no longer desired are
    /// forgotten.
    pub fn set(&self, topics: BTreeSet<String>) -> bool {
        self.rejected
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|t, _| topics.contains(t));
        self.desired.send_if_modified(|current| {
            if **current == topics {
                false
            } else {
                *current = Arc::new(topics);
                true
            }
        })
    }

    /// The desired set.
    pub fn desired(&self) -> Arc<BTreeSet<String>> {
        self.desired.borrow().clone()
    }

    /// Receiver the sessions follow.
    pub(crate) fn watch(&self) -> watch::Receiver<Arc<BTreeSet<String>>> {
        self.desired.subscribe()
    }

    /// Topics the exchange refused in the current session, with the reason it gave. They stay in the desired
    /// set (the owner's policy is not overridden) and are tried again on the next connection.
    pub fn rejected(&self) -> Vec<(String, String)> {
        self.rejected
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(t, r)| (t.clone(), r.clone()))
            .collect()
    }

    pub(crate) fn mark_rejected(&self, topic: &str, reason: &str) {
        self.rejected
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(topic.to_string(), reason.to_string());
    }

    pub(crate) fn clear_rejected(&self) {
        self.rejected
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

/// Splits `topics` into groups whose JSON `args` array stays within `max_chars` characters (each topic costs
/// its length plus two quotes and a comma). Order is kept. A topic that alone exceeds the limit gets its own
/// group (the exchange will refuse it; that is reported as a rejection).
pub fn chunk_topics(topics: &[String], max_chars: usize) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = Vec::new();
    let mut size = 0usize;
    for t in topics {
        let cost = t.chars().count() + 3;
        match out.last_mut() {
            Some(group) if size + cost <= max_chars => {
                group.push(t.clone());
                size += cost;
            }
            _ => {
                out.push(vec![t.clone()]);
                size = cost;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("tickers.SYM{i:04}USDT")).collect()
    }

    #[test]
    fn chunks_respect_the_limit_and_keep_order() {
        let t = names(1000);
        let chunks = chunk_topics(&t, 2000);
        assert!(chunks.len() > 1);
        for c in &chunks {
            let size: usize = c.iter().map(|t| t.len() + 3).sum();
            assert!(size <= 2000, "{size}");
        }
        let flat: Vec<String> = chunks.concat();
        assert_eq!(flat, t);
        assert!(chunk_topics(&[], 100).is_empty());
        // An oversized topic is alone in its group.
        let big = vec!["a".into(), "x".repeat(50), "b".into()];
        assert_eq!(chunk_topics(&big, 20).len(), 3);
    }

    #[test]
    fn set_reports_changes_and_forgets_rejections() {
        let c = TopicControl::new(["a".to_string()]);
        let mut rx = c.watch();
        assert!(!c.set(["a".to_string()].into()));
        c.mark_rejected("a", "nope");
        assert_eq!(c.rejected().len(), 1);
        assert!(c.set(["b".to_string()].into()));
        assert!(rx.has_changed().unwrap());
        assert!(c.rejected().is_empty());
        assert_eq!(c.desired().len(), 1);
        rx.borrow_and_update();
    }
}
