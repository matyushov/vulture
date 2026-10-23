//! TLS for the gRPC listener with certificate hot reload (V1-8).
//!
//! tonic's built-in `ServerTlsConfig` cannot change the certificate of a running server, so TLS is done here
//! with rustls directly:
//!
//! - the rustls `ServerConfig` has a [`ResolvesServerCert`] whose `CertifiedKey` sits in an [`ArcSwap`];
//!   [`TlsReloader::reload`] builds a new key from the files and swaps it in. New handshakes get the new
//!   certificate, connections that are already open keep running on the old one;
//! - [`incoming`] accepts TCP connections, performs the handshakes in separate tasks (a slow client cannot block
//!   the accept loop) and hands the TLS streams to tonic's `serve_with_incoming_shutdown`;
//! - reloads are triggered by `SIGHUP`, by a poll of the files' mtime and size ([`spawn_watchers`]) and checked
//!   without a server by `vulture-server tls check`.
//!
//! An invalid replacement (parse error, key mismatch, expired certificate) never replaces the working one: the
//! error is logged (without key material) and counted in `vulture_tls_reloads_total{result="error"}`.

mod incoming;
mod load;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use rustls::ServerConfig;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tokio_util::sync::CancellationToken;

pub use incoming::{TlsConn, incoming};
pub use load::{CertInfo, TlsError, check_files};

use crate::config::TlsConfig;
use crate::metrics::{Families, MetricSource, unix_now_secs};

/// A certificate with less than this validity left is reported with a warning.
pub const EXPIRY_WARNING: Duration = Duration::from_secs(14 * 24 * 3600);

/// Counters of the TLS layer.
#[derive(Debug, Default)]
pub struct TlsStats {
    /// Successful reloads (the initial load is not counted).
    pub reload_ok: AtomicU64,
    /// Failed reloads; the previous certificate stays in use.
    pub reload_failed: AtomicU64,
    /// TLS handshakes that failed (bad client, missing client certificate in mTLS, timeouts).
    pub handshake_failures: AtomicU64,
    /// `not_after` of the certificate in use, seconds since the Unix epoch.
    pub not_after_unix: AtomicI64,
    /// Whether the last load or reload attempt succeeded.
    pub last_reload_ok: AtomicBool,
}

#[derive(Debug)]
struct SwappableCert(Arc<ArcSwap<CertifiedKey>>);

impl ResolvesServerCert for SwappableCert {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.load_full())
    }
}

/// Modification time and size of a file; a change of either means "reload".
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct FileSig(Option<(SystemTime, u64)>);

impl FileSig {
    fn of(path: &std::path::Path) -> Self {
        Self(
            std::fs::metadata(path)
                .and_then(|m| Ok((m.modified()?, m.len())))
                .ok(),
        )
    }
}

/// The certificate in use, its reload logic and the rustls configuration built on it.
pub struct TlsReloader {
    cert: PathBuf,
    key: PathBuf,
    current: Arc<ArcSwap<CertifiedKey>>,
    info: ArcSwap<CertInfo>,
    config: Arc<ServerConfig>,
    mtls: bool,
    seen: Mutex<(FileSig, FileSig)>,
    /// Counters.
    pub stats: Arc<TlsStats>,
}

impl TlsReloader {
    /// Loads the files and builds the server configuration. Fails (and the server must not start) when the files
    /// are unusable.
    pub fn load(cfg: &TlsConfig) -> Result<Arc<Self>, TlsError> {
        let (certified, info) = load::load_certified_key(&cfg.cert, &cfg.key)?;
        let verifier = match &cfg.client_ca {
            Some(path) => Some(load::client_verifier(path)?),
            None => None,
        };
        let current = Arc::new(ArcSwap::from_pointee(certified));
        let config =
            load::server_config(verifier.clone(), Arc::new(SwappableCert(current.clone())))?;
        let stats = Arc::new(TlsStats::default());
        stats
            .not_after_unix
            .store(info.not_after_unix, Ordering::Relaxed);
        stats.last_reload_ok.store(true, Ordering::Relaxed);
        info.log("TLS certificate loaded");
        Ok(Arc::new(Self {
            cert: cfg.cert.clone(),
            key: cfg.key.clone(),
            current,
            info: ArcSwap::from_pointee(info),
            config: Arc::new(config),
            mtls: verifier.is_some(),
            seen: Mutex::new((FileSig::of(&cfg.cert), FileSig::of(&cfg.key))),
            stats,
        }))
    }

    /// The rustls configuration (shared by all handshakes).
    pub fn server_config(&self) -> Arc<ServerConfig> {
        self.config.clone()
    }

    /// Whether client certificates are required.
    pub fn mtls(&self) -> bool {
        self.mtls
    }

    /// Details of the certificate in use.
    pub fn info(&self) -> Arc<CertInfo> {
        self.info.load_full()
    }

    /// Re-reads the files. On success the new certificate is used for new connections; on failure the old one
    /// stays and the error is logged and counted. `reason` is only for the log (`sighup`, `file change`).
    pub fn reload(&self, reason: &str) -> Result<Arc<CertInfo>, TlsError> {
        match load::load_certified_key(&self.cert, &self.key) {
            Ok((certified, info)) => {
                self.current.store(Arc::new(certified));
                self.stats
                    .not_after_unix
                    .store(info.not_after_unix, Ordering::Relaxed);
                self.stats.reload_ok.fetch_add(1, Ordering::Relaxed);
                self.stats.last_reload_ok.store(true, Ordering::Relaxed);
                info.log(&format!("TLS certificate reloaded ({reason})"));
                let info = Arc::new(info);
                self.info.store(info.clone());
                Ok(info)
            }
            Err(e) => {
                self.stats.reload_failed.fetch_add(1, Ordering::Relaxed);
                self.stats.last_reload_ok.store(false, Ordering::Relaxed);
                tracing::error!(
                    reason,
                    error = %e,
                    "TLS certificate reload failed; keeping the previous certificate"
                );
                Err(e)
            }
        }
    }

    /// Reloads when the certificate or key file changed since the last look. Returns whether a reload was tried.
    /// A change is acted on once: a broken pair is not retried (and not logged again) until a file changes again
    /// or SIGHUP arrives.
    pub fn reload_if_changed(&self) -> bool {
        let now = (FileSig::of(&self.cert), FileSig::of(&self.key));
        {
            let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
            if *seen == now {
                return false;
            }
            *seen = now;
        }
        let _ = self.reload("file change");
        true
    }

    /// SIGHUP and the poll both go through here so that `seen` stays in step.
    fn reload_on_signal(&self) {
        {
            let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
            *seen = (FileSig::of(&self.cert), FileSig::of(&self.key));
        }
        let _ = self.reload("SIGHUP");
    }
}

impl MetricSource for TlsReloader {
    fn collect(&self, out: &mut Families) {
        let s = &self.stats;
        out.gauge(
            "vulture_tls_enabled",
            "1 when the gRPC listener serves TLS.",
            &[],
            1.0,
        );
        out.gauge(
            "vulture_tls_certificate_expiry_seconds",
            "Seconds until the certificate in use expires (negative once expired).",
            &[],
            s.not_after_unix.load(Ordering::Relaxed) as f64 - unix_now_secs(),
        );
        out.counter(
            "vulture_tls_reloads_total",
            "Certificate reloads by result.",
            &[("result", "ok")],
            s.reload_ok.load(Ordering::Relaxed),
        );
        out.counter(
            "vulture_tls_reloads_total",
            "Certificate reloads by result.",
            &[("result", "error")],
            s.reload_failed.load(Ordering::Relaxed),
        );
        out.gauge(
            "vulture_tls_last_reload_ok",
            "1 when the last load or reload succeeded, 0 when the latest files were rejected.",
            &[],
            f64::from(u8::from(s.last_reload_ok.load(Ordering::Relaxed))),
        );
        out.counter(
            "vulture_tls_handshake_failures_total",
            "Failed TLS handshakes.",
            &[],
            s.handshake_failures.load(Ordering::Relaxed),
        );
    }
}

/// Starts the reload triggers: SIGHUP (always) and, when `interval` is not zero, the file poll. Both stop at
/// `shutdown`.
pub fn spawn_watchers(
    reloader: Arc<TlsReloader>,
    interval: Duration,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut hup = signal(SignalKind::hangup())?;
        let (r, stop) = (reloader.clone(), shutdown.clone());
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = hup.recv() => {
                        tracing::info!("SIGHUP received, reloading the TLS certificate");
                        r.reload_on_signal();
                    }
                    _ = stop.cancelled() => return,
                }
            }
        });
    }
    if !interval.is_zero() {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let r = reloader.clone();
                        // File metadata and parsing: keep it off the async threads.
                        let _ = tokio::task::spawn_blocking(move || r.reload_if_changed()).await;
                    }
                    _ = shutdown.cancelled() => return,
                }
            }
        });
    }
    Ok(())
}
