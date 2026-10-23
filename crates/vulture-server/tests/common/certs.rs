//! Test certificates generated on the fly (rcgen, ring). Nothing here is a real credential.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, date_time_ymd,
};

/// A throwaway certificate authority.
pub struct Ca {
    issuer: CertifiedIssuer<'static, KeyPair>,
}

/// A certificate with its key, as PEM and DER.
#[derive(Clone)]
pub struct Leaf {
    pub cert_pem: String,
    pub key_pem: String,
    pub der: Vec<u8>,
}

impl Ca {
    pub fn new(name: &str) -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name.push(DnType::CommonName, name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let issuer = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
        Self { issuer }
    }

    pub fn pem(&self) -> String {
        self.issuer.pem()
    }

    pub fn der(&self) -> Vec<u8> {
        self.issuer.der().as_ref().to_vec()
    }

    /// A server certificate for `localhost` / `127.0.0.1`; `cn` tells certificates apart in tests.
    pub fn server(&self, cn: &str) -> Leaf {
        self.leaf(cn, false, false)
    }

    /// A client certificate.
    pub fn client(&self, cn: &str) -> Leaf {
        self.leaf(cn, true, false)
    }

    /// A server certificate that expired in the past.
    pub fn expired_server(&self, cn: &str) -> Leaf {
        self.leaf(cn, false, true)
    }

    fn leaf(&self, cn: &str, client: bool, expired: bool) -> Leaf {
        let mut params =
            CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
        params.distinguished_name.push(DnType::CommonName, cn);
        params.extended_key_usages = vec![if client {
            ExtendedKeyUsagePurpose::ClientAuth
        } else {
            ExtendedKeyUsagePurpose::ServerAuth
        }];
        if expired {
            params.not_before = date_time_ymd(2020, 1, 1);
            params.not_after = date_time_ymd(2021, 1, 1);
        }
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &*self.issuer).unwrap();
        Leaf {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            der: cert.der().as_ref().to_vec(),
        }
    }
}

/// Where a test keeps its TLS files.
pub struct Files {
    pub dir: tempfile::TempDir,
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

impl Files {
    pub fn new(ca: &Ca, leaf: &Leaf) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let files = Self {
            cert: dir.path().join("fullchain.pem"),
            key: dir.path().join("privkey.pem"),
            ca: dir.path().join("ca.pem"),
            dir,
        };
        std::fs::write(&files.ca, ca.pem()).unwrap();
        files.replace(leaf);
        files
    }

    /// Replaces certificate and key atomically per file (write + rename), like a renewal does.
    pub fn replace(&self, leaf: &Leaf) {
        write_atomic(&self.cert, leaf.cert_pem.as_bytes());
        write_atomic(&self.key, leaf.key_pem.as_bytes());
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    pub fn config(&self) -> vulture_server::config::TlsConfig {
        vulture_server::config::TlsConfig {
            cert: self.cert.clone(),
            key: self.key.clone(),
            client_ca: None,
            reload_interval_secs: 0,
        }
    }
}

pub fn write_atomic(path: &Path, content: &[u8]) {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, content).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}
