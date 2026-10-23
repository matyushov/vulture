//! Reading and validating the certificate, the key and the client CA bundle. Error messages name files and
//! reasons, never key material.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{RootCertStore, ServerConfig};
use x509_parser::prelude::{FromDer, X509Certificate};

use super::EXPIRY_WARNING;
use crate::config::TlsConfig;
use crate::metrics::unix_now_secs;

/// Why TLS material was rejected.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A file could not be read.
    #[error("cannot read {what} {path}: {source}")]
    Read {
        /// "certificate", "private key" or "client CA".
        what: &'static str,
        /// The file.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// A file is not valid PEM or holds the wrong kind of item.
    #[error("{what} {path}: {message}")]
    Invalid {
        /// "certificate", "private key" or "client CA".
        what: &'static str,
        /// The file.
        path: PathBuf,
        /// What is wrong.
        message: String,
    },
    /// The certificate does not match the key.
    #[error("the private key {key} does not match the certificate {cert}")]
    KeyMismatch {
        /// Certificate file.
        cert: PathBuf,
        /// Key file.
        key: PathBuf,
    },
    /// The certificate is not valid now.
    #[error("certificate {path} {message}")]
    NotValid {
        /// Certificate file.
        path: PathBuf,
        /// "expired on ..." or "is not valid before ...".
        message: String,
    },
    /// The rustls configuration could not be built.
    #[error("TLS configuration: {0}")]
    Config(String),
}

/// What is known about the certificate in use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertInfo {
    /// Subject of the leaf certificate (distinguished name, for the log and `tls check`).
    pub subject: String,
    /// Start of validity, seconds since the Unix epoch.
    pub not_before_unix: i64,
    /// End of validity, seconds since the Unix epoch.
    pub not_after_unix: i64,
    /// End of validity as text (RFC 2822).
    pub not_after: String,
    /// Certificates in the chain file.
    pub chain_len: usize,
}

impl CertInfo {
    /// Seconds until expiry (negative after).
    pub fn expires_in_secs(&self) -> i64 {
        self.not_after_unix - unix_now_secs() as i64
    }

    pub(super) fn log(&self, message: &str) {
        let days = self.expires_in_secs() / 86_400;
        tracing::info!(
            subject = %self.subject,
            not_after = %self.not_after,
            expires_in_days = days,
            chain = self.chain_len,
            "{message}"
        );
        if self.expires_in_secs() < EXPIRY_WARNING.as_secs() as i64 {
            tracing::warn!(
                not_after = %self.not_after,
                expires_in_days = days,
                "the TLS certificate expires in less than 14 days: check the renewal"
            );
        }
    }
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn read_certs(path: &Path, what: &'static str) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let pem = std::fs::read(path).map_err(|source| TlsError::Read {
        what,
        path: path.to_path_buf(),
        source,
    })?;
    let certs = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::Invalid {
            what,
            path: path.to_path_buf(),
            message: format!("not valid PEM ({e})"),
        })?;
    if certs.is_empty() {
        return Err(TlsError::Invalid {
            what,
            path: path.to_path_buf(),
            message: "contains no certificate".into(),
        });
    }
    Ok(certs)
}

fn read_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let pem = std::fs::read(path).map_err(|source| TlsError::Read {
        what: "private key",
        path: path.to_path_buf(),
        source,
    })?;
    PrivateKeyDer::from_pem_slice(&pem).map_err(|e| TlsError::Invalid {
        what: "private key",
        path: path.to_path_buf(),
        message: format!("no usable private key in the PEM file ({e})"),
    })
}

/// Loads and validates a certificate chain and its key: PEM parses, the key type is supported, the key matches
/// the leaf certificate, the leaf is valid now.
pub(super) fn load_certified_key(
    cert: &Path,
    key: &Path,
) -> Result<(CertifiedKey, CertInfo), TlsError> {
    let chain = read_certs(cert, "certificate")?;
    let key_der = read_key(key)?;
    let info = describe(&chain[0], cert, chain.len())?;
    let signing = rustls::crypto::ring::sign::any_supported_type(&key_der).map_err(|e| {
        TlsError::Invalid {
            what: "private key",
            path: key.to_path_buf(),
            message: format!("unsupported or malformed key ({e})"),
        }
    })?;
    let certified = CertifiedKey::new(chain, signing);
    certified.keys_match().map_err(|_| TlsError::KeyMismatch {
        cert: cert.to_path_buf(),
        key: key.to_path_buf(),
    })?;
    let now = unix_now_secs() as i64;
    if info.not_after_unix <= now {
        return Err(TlsError::NotValid {
            path: cert.to_path_buf(),
            message: format!("expired on {}", info.not_after),
        });
    }
    // A few minutes of clock skew are tolerated for a certificate that was just issued.
    if info.not_before_unix > now + 300 {
        return Err(TlsError::NotValid {
            path: cert.to_path_buf(),
            message: "is not valid yet (check the system clock)".into(),
        });
    }
    Ok((certified, info))
}

fn describe(
    leaf: &CertificateDer<'_>,
    path: &Path,
    chain_len: usize,
) -> Result<CertInfo, TlsError> {
    let (_, parsed) = X509Certificate::from_der(leaf.as_ref()).map_err(|e| TlsError::Invalid {
        what: "certificate",
        path: path.to_path_buf(),
        message: format!("cannot parse the certificate ({e})"),
    })?;
    let validity = parsed.validity();
    Ok(CertInfo {
        subject: parsed.subject().to_string(),
        not_before_unix: validity.not_before.timestamp(),
        not_after_unix: validity.not_after.timestamp(),
        not_after: validity
            .not_after
            .to_rfc2822()
            .unwrap_or_else(|_| validity.not_after.to_string()),
        chain_len,
    })
}

pub(super) fn client_verifier(
    path: &Path,
) -> Result<Arc<dyn rustls::server::danger::ClientCertVerifier>, TlsError> {
    let certs = read_certs(path, "client CA")?;
    let mut roots = RootCertStore::empty();
    let (added, _ignored) = roots.add_parsable_certificates(certs);
    if added == 0 {
        return Err(TlsError::Invalid {
            what: "client CA",
            path: path.to_path_buf(),
            message: "no usable CA certificate".into(),
        });
    }
    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
        .build()
        .map_err(|e| TlsError::Config(format!("client certificate verifier: {e}")))
}

pub(super) fn server_config(
    verifier: Option<Arc<dyn rustls::server::danger::ClientCertVerifier>>,
    resolver: Arc<dyn ResolvesServerCert>,
) -> Result<ServerConfig, TlsError> {
    let builder = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| TlsError::Config(e.to_string()))?;
    let builder = match verifier {
        Some(v) => builder.with_client_cert_verifier(v),
        None => builder.with_no_client_auth(),
    };
    let mut config = builder.with_cert_resolver(resolver);
    // gRPC is HTTP/2 only.
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(config)
}

/// Validates the files named in `[tls]` without starting a server (`config check`, `tls check`). Returns the
/// details of the certificate.
pub fn check_files(cfg: &TlsConfig) -> Result<CertInfo, TlsError> {
    let (_, info) = load_certified_key(&cfg.cert, &cfg.key)?;
    if let Some(ca) = &cfg.client_ca {
        client_verifier(ca)?;
    }
    Ok(info)
}
