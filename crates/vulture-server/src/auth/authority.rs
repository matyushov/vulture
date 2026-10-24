//! The token a running server checks requests against.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::store::{TokenError, TokenStore, constant_time_eq};
use crate::config::Secret;

/// What [`TokenAuthority::refresh`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// The file holds the token already in use.
    Unchanged,
    /// The file holds a different token: it is in use now and the generation changed.
    Rotated,
    /// The file is missing or unreadable; the token in use is kept. It is never regenerated here.
    Kept,
}

struct Current {
    token: Secret,
    generation: u64,
}

/// The current token and its generation. The generation starts at 1 and grows with every rotation; streams
/// remember the generation they authenticated with and end when it changes.
pub struct TokenAuthority {
    store: TokenStore,
    current: RwLock<Current>,
    generation_tx: watch::Sender<u64>,
    /// Whether the last refresh failed, to log a persistent problem once instead of every poll.
    degraded: AtomicBool,
    /// Fingerprint of the last rejected file content, so one bad edit is logged and counted once, not at every
    /// poll.
    rejected_fingerprint: AtomicU64,
    /// Tokens taken over from the file after startup.
    file_changes: AtomicU64,
    /// Edits of the file that were rejected because the content is not a valid token.
    invalid_edits: AtomicU64,
}

impl TokenAuthority {
    /// Loads the token from `store`; it must exist already (see [`TokenStore::create_if_missing`]).
    pub fn open(store: TokenStore) -> Result<Arc<Self>, TokenError> {
        let token = store.read_required()?;
        let (generation_tx, _) = watch::channel(1);
        Ok(Arc::new(Self {
            store,
            current: RwLock::new(Current {
                token,
                generation: 1,
            }),
            generation_tx,
            degraded: AtomicBool::new(false),
            rejected_fingerprint: AtomicU64::new(0),
            file_changes: AtomicU64::new(0),
            invalid_edits: AtomicU64::new(0),
        }))
    }

    /// How many times a changed token was taken over from the file since start.
    pub fn file_changes(&self) -> u64 {
        self.file_changes.load(Ordering::Relaxed)
    }

    /// How many edits of the file were rejected as invalid since start.
    pub fn invalid_edits(&self) -> u64 {
        self.invalid_edits.load(Ordering::Relaxed)
    }

    /// Current generation.
    pub fn generation(&self) -> u64 {
        self.read().generation
    }

    /// A receiver that changes whenever the token is rotated; its value is the new generation.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.generation_tx.subscribe()
    }

    /// Checks `presented` in constant time. Returns the generation it is valid for, or `None`.
    pub fn verify(&self, presented: &str) -> Option<u64> {
        let current = self.read();
        constant_time_eq(presented.as_bytes(), current.token.expose().as_bytes())
            .then_some(current.generation)
    }

    /// Writes a new token to the file, switches to it at once and returns it. The old token is rejected from
    /// this moment and streams that used it end.
    pub fn rotate(&self) -> Result<Secret, TokenError> {
        let new = self.store.rotate()?;
        self.install(new.clone());
        Ok(new)
    }

    /// Re-reads the file and switches to the token found there if it differs from the one in use. A missing or
    /// broken file keeps the current token (the server never invents a token at run time).
    pub fn refresh(&self) -> RefreshOutcome {
        let (result, fingerprint) = self.store.read_fingerprinted();
        match result {
            Ok(Some(token)) => {
                self.rejected_fingerprint.store(0, Ordering::Relaxed);
                if self.degraded.swap(false, Ordering::Relaxed) {
                    tracing::info!("token file readable again");
                }
                let same = constant_time_eq(
                    token.expose().as_bytes(),
                    self.read().token.expose().as_bytes(),
                );
                if same {
                    RefreshOutcome::Unchanged
                } else {
                    // Count first: installing the token ends the open streams, and an observer that sees
                    // a stream end must already see the change counted.
                    self.file_changes.fetch_add(1, Ordering::Relaxed);
                    self.install(token);
                    tracing::info!(
                        generation = self.generation(),
                        "token changed from file; streams opened with the old token are ending"
                    );
                    RefreshOutcome::Rotated
                }
            }
            Ok(None) => {
                if !self.degraded.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        path = %self.store.path().display(),
                        "token file is missing; keeping the token in use (it is not recreated while running)"
                    );
                }
                RefreshOutcome::Kept
            }
            Err(TokenError::Malformed(path)) => {
                self.degraded.store(true, Ordering::Relaxed);
                // Count and log a given bad content once, however many polls see it.
                if self
                    .rejected_fingerprint
                    .swap(fingerprint, Ordering::Relaxed)
                    != fingerprint
                {
                    self.invalid_edits.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        path = %path.display(),
                        "token file edit rejected: not a valid token (at least {} characters from A-Z a-z 0-9 - _); keeping the previous token",
                        super::store::MIN_TOKEN_LEN
                    );
                }
                RefreshOutcome::Kept
            }
            Err(e) => {
                if !self.degraded.swap(true, Ordering::Relaxed) {
                    // The error text names the file, never the token.
                    tracing::warn!(error = %e, "token file unavailable; keeping the token in use");
                }
                RefreshOutcome::Kept
            }
        }
    }

    /// Polls the token file every `interval` until `shutdown` fires, so that `token rotate` run by another
    /// process takes effect within one interval.
    pub async fn watch_file(self: Arc<Self>, interval: Duration, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    let this = self.clone();
                    // File I/O off the async worker threads.
                    match tokio::task::spawn_blocking(move || this.refresh()).await {
                        Ok(_) => {}
                        Err(e) => tracing::error!(error = %e, "token refresh task failed"),
                    }
                }
            }
        }
    }

    fn install(&self, token: Secret) {
        let generation = {
            let mut current = self.current.write().unwrap_or_else(|e| e.into_inner());
            current.generation += 1;
            current.token = token;
            current.generation
        };
        self.generation_tx.send_replace(generation);
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Current> {
        self.current.read().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority() -> (tempfile::TempDir, TokenStore, Arc<TokenAuthority>, Secret) {
        let dir = tempfile::tempdir().unwrap();
        let store = TokenStore::new(dir.path());
        let token = store.create_if_missing().unwrap().into_secret();
        let authority = TokenAuthority::open(store.clone()).unwrap();
        (dir, store, authority, token)
    }

    #[test]
    fn verify_accepts_only_the_current_token() {
        let (_dir, _store, authority, token) = authority();
        assert_eq!(authority.verify(token.expose()), Some(1));
        assert_eq!(authority.verify("nope"), None);
        assert_eq!(authority.verify(""), None);
        assert_eq!(authority.verify(&format!("{} ", token.expose())), None);
    }

    #[test]
    fn rotate_rejects_the_old_token_immediately_and_bumps_the_generation() {
        let (_dir, store, authority, old) = authority();
        let mut rx = authority.subscribe();
        let new = authority.rotate().unwrap();
        assert_eq!(authority.verify(old.expose()), None);
        assert_eq!(authority.verify(new.expose()), Some(2));
        assert_eq!(store.read_required().unwrap(), new);
        assert!(rx.has_changed().unwrap());
        assert_eq!(*rx.borrow_and_update(), 2);
    }

    #[test]
    fn refresh_picks_up_a_rotation_done_by_another_process() {
        let (_dir, store, authority, old) = authority();
        assert_eq!(authority.refresh(), RefreshOutcome::Unchanged);
        let new = store.rotate().unwrap(); // the CLI would do this
        assert_eq!(authority.verify(old.expose()), Some(1), "not noticed yet");
        assert_eq!(authority.refresh(), RefreshOutcome::Rotated);
        assert_eq!(authority.verify(old.expose()), None);
        assert_eq!(authority.verify(new.expose()), Some(2));
        assert_eq!(authority.refresh(), RefreshOutcome::Unchanged);
    }

    #[test]
    fn a_vanished_file_keeps_the_token_and_is_not_regenerated() {
        let (_dir, store, authority, token) = authority();
        std::fs::remove_file(store.path()).unwrap();
        assert_eq!(authority.refresh(), RefreshOutcome::Kept);
        assert_eq!(authority.verify(token.expose()), Some(1));
        assert!(!store.path().exists());
        std::fs::write(store.path(), "bad").unwrap();
        assert_eq!(authority.refresh(), RefreshOutcome::Kept);
        assert_eq!(authority.generation(), 1);
    }

    #[tokio::test]
    async fn the_watcher_notices_rotation_and_stops_on_shutdown() {
        let (_dir, store, authority, _old) = authority();
        let mut rx = authority.subscribe();
        let stop = CancellationToken::new();
        let task = tokio::spawn(
            authority
                .clone()
                .watch_file(Duration::from_millis(10), stop.clone()),
        );
        store.rotate().unwrap();
        tokio::time::timeout(Duration::from_secs(5), rx.changed())
            .await
            .expect("rotation must be noticed")
            .unwrap();
        assert_eq!(*rx.borrow(), 2);
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }
}
