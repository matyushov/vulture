//! Duplicate suppression for the overlap of a make-before-break switch.

use std::collections::{HashSet, VecDeque};

/// Bounded set of recently seen keys. It records and filters only while active, so outside a switch
/// genuinely repeated frames are never dropped.
#[derive(Debug)]
pub struct Dedup {
    active: bool,
    capacity: usize,
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl Dedup {
    /// Inactive deduplicator remembering at most `capacity` keys.
    pub fn new(capacity: usize) -> Self {
        Self {
            active: false,
            capacity: capacity.max(1),
            seen: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    /// Starts recording and filtering.
    pub fn activate(&mut self) {
        self.active = true;
    }

    /// Stops filtering and forgets all keys.
    pub fn deactivate(&mut self) {
        self.active = false;
        self.seen.clear();
        self.order.clear();
    }

    /// Whether filtering is on.
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Returns `true` if the key was seen before (the frame is a duplicate). Records new keys.
    pub fn is_duplicate(&mut self, key: String) -> bool {
        if !self.active {
            return false;
        }
        if self.seen.contains(&key) {
            return true;
        }
        if self.order.len() >= self.capacity
            && let Some(old) = self.order.pop_front()
        {
            self.seen.remove(&old);
        }
        self.seen.insert(key.clone());
        self.order.push_back(key);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inactive_never_filters() {
        let mut d = Dedup::new(10);
        assert!(!d.is_duplicate("a".into()));
        assert!(!d.is_duplicate("a".into()));
    }

    #[test]
    fn active_filters_repeats_and_is_bounded() {
        let mut d = Dedup::new(2);
        d.activate();
        assert!(!d.is_duplicate("a".into()));
        assert!(d.is_duplicate("a".into()));
        assert!(!d.is_duplicate("b".into()));
        assert!(!d.is_duplicate("c".into())); // evicts "a"
        assert!(!d.is_duplicate("a".into()));
        d.deactivate();
        assert!(!d.is_active());
        assert!(!d.is_duplicate("a".into()));
    }
}
