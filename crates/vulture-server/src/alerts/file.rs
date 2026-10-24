//! The state file `<data_dir>/alerts.json`: the owner's rules and channels.
//!
//! Versioned, written atomically with mode `0600` (it may hold bot tokens and webhook URLs). A file that cannot
//! be read, parsed or validated stops the server from starting: silently falling back to the configuration
//! could switch off rules the owner relies on or re-enable ones they removed. While the file does not exist the
//! defaults of `[[alerts.rules]]` and `[[alerts.channels]]` of the configuration are in force; the first change
//! through `Control` writes the file with those defaults and the change.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::model::{ChannelDef, RuleDef};
use crate::symbols::state::write_atomic;

/// File name inside `server.data_dir`.
pub const ALERTS_FILE: &str = "alerts.json";
/// Schema version written and accepted.
pub const ALERTS_VERSION: u32 = 1;

/// Why the file could not be used.
#[derive(Debug, thiserror::Error)]
pub enum AlertsFileError {
    /// The file exists but cannot be read.
    #[error("cannot read the alerts file {path}: {source}")]
    Read {
        /// File.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// Not valid JSON, not the schema, or semantically invalid. The message never contains secrets.
    #[error(
        "the alerts file {path} is corrupt: {message}. Fix or remove it deliberately: the server does not fall back to the configuration, because that could switch off rules you rely on"
    )]
    Corrupt {
        /// File.
        path: PathBuf,
        /// What is wrong.
        message: String,
    },
    /// A version this build does not know.
    #[error(
        "the alerts file {path} has version {found}, this build understands version {ALERTS_VERSION}. Upgrade the server or restore a compatible file"
    )]
    UnknownVersion {
        /// File.
        path: PathBuf,
        /// Version found.
        found: String,
    },
    /// Writing failed.
    #[error("cannot write the alerts file {path}: {source}")]
    Write {
        /// File.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
}

/// Content of the file.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertsDoc {
    /// Schema version.
    pub version: u32,
    /// Rules.
    #[serde(default)]
    pub rules: Vec<RuleDef>,
    /// Channels.
    #[serde(default)]
    pub channels: Vec<ChannelDef>,
}

/// The file of one data directory.
#[derive(Debug, Clone)]
pub struct AlertsFile {
    path: PathBuf,
}

impl AlertsFile {
    /// `<data_dir>/alerts.json`.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            path: data_dir.join(ALERTS_FILE),
        }
    }

    /// Path of the file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Reads and validates the file; `None` when it does not exist.
    pub fn load(&self, allow_custom_endpoints: bool) -> Result<Option<AlertsDoc>, AlertsFileError> {
        let text = match std::fs::read_to_string(&self.path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(AlertsFileError::Read {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = std::fs::metadata(&self.path)
                && meta.permissions().mode() & 0o077 != 0
            {
                tracing::warn!(
                    path = %self.path.display(),
                    mode = format!("{:o}", meta.permissions().mode() & 0o777),
                    "the alerts file holds secrets and should be readable by the owner only (chmod 600)"
                );
            }
        }
        let corrupt = |message: String| AlertsFileError::Corrupt {
            path: self.path.clone(),
            message,
        };
        let value: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| corrupt(clean(&e.to_string())))?;
        match value.get("version") {
            Some(v) if v.as_u64() == Some(u64::from(ALERTS_VERSION)) => {}
            Some(v) => {
                return Err(AlertsFileError::UnknownVersion {
                    path: self.path.clone(),
                    found: v.to_string(),
                });
            }
            None => return Err(corrupt("no \"version\" field".into())),
        }
        let mut doc: AlertsDoc =
            serde_json::from_value(value).map_err(|e| corrupt(clean(&e.to_string())))?;
        validate_doc(&mut doc, allow_custom_endpoints).map_err(corrupt)?;
        Ok(Some(doc))
    }

    /// Writes the file atomically with mode 0600.
    pub fn save(&self, doc: &AlertsDoc) -> Result<(), AlertsFileError> {
        let write_err = |source| AlertsFileError::Write {
            path: self.path.clone(),
            source,
        };
        let mut json = serde_json::to_string_pretty(doc)
            .map_err(|e| write_err(std::io::Error::other(e.to_string())))?;
        json.push('\n');
        write_atomic(&self.path, json.as_bytes()).map_err(write_err)
    }
}

/// serde errors can quote input text; keep only the first line and cut it.
fn clean(msg: &str) -> String {
    msg.lines().next().unwrap_or("").chars().take(200).collect()
}

/// Validates every entry, assigns nothing, and checks identifiers are unique. Messages carry ids, not secrets.
pub fn validate_doc(doc: &mut AlertsDoc, allow_custom_endpoints: bool) -> Result<(), String> {
    if doc.rules.len() > super::model::MAX_RULES {
        return Err(format!("more than {} rules", super::model::MAX_RULES));
    }
    if doc.channels.len() > super::model::MAX_CHANNELS {
        return Err(format!("more than {} channels", super::model::MAX_CHANNELS));
    }
    let mut ids = std::collections::HashSet::new();
    for c in &mut doc.channels {
        if c.id.is_empty() {
            return Err("a channel has no id".into());
        }
        c.validate(allow_custom_endpoints)
            .map_err(|e| format!("channel {:?}: {e}", c.id))?;
        if !ids.insert(c.id.clone()) {
            return Err(format!("duplicate channel id {:?}", c.id));
        }
    }
    let mut rule_ids = std::collections::HashSet::new();
    for r in &mut doc.rules {
        if r.id.is_empty() {
            return Err("a rule has no id".into());
        }
        r.validate().map_err(|e| format!("rule {:?}: {e}", r.id))?;
        if !rule_ids.insert(r.id.clone()) {
            return Err(format!("duplicate rule id {:?}", r.id));
        }
        for c in &r.channels {
            if !ids.contains(c) {
                tracing::warn!(rule = %r.id, channel = %c, "a rule refers to a channel that does not exist");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerts::model::{ChannelKind, RuleKind};
    use crate::config::Secret;

    fn doc() -> AlertsDoc {
        AlertsDoc {
            version: ALERTS_VERSION,
            rules: vec![RuleDef {
                id: "r1".into(),
                name: "n".into(),
                enabled: true,
                kind: RuleKind::Liquidation {
                    min_notional_usd: 1e6,
                    sides: vec![],
                },
                exchanges: vec![],
                symbols: vec![],
                channels: vec!["tg".into()],
                cooldown_secs: 10,
            }],
            channels: vec![ChannelDef {
                id: "tg".into(),
                name: "t".into(),
                kind: ChannelKind::Telegram,
                enabled: true,
                system: false,
                dry_run: false,
                chat_id: "-100123".into(),
                secret: Some(Secret::new("123456:ABC-def_ghijkl")),
                secret_file: String::new(),
                secret_env: String::new(),
                min_interval_ms: 0,
                batch_window_ms: 0,
                queue_limit: 0,
            }],
        }
    }

    #[test]
    fn missing_file_is_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(AlertsFile::new(dir.path()).load(false).unwrap().is_none());
    }

    #[test]
    fn round_trip_with_private_mode() {
        let dir = tempfile::tempdir().unwrap();
        let f = AlertsFile::new(dir.path());
        f.save(&doc()).unwrap();
        assert_eq!(f.load(false).unwrap().unwrap(), doc());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(f.path()).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    fn corrupt_files_are_refused_without_leaking_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let f = AlertsFile::new(dir.path());
        for text in [
            "not json",
            "{}",
            r#"{"version": 1, "rules": [{"id": "x"}]}"#,
            r#"{"version": 1, "bogus": 1}"#,
            r#"{"version": 1, "channels": [{"id": "tg", "kind": "telegram", "chat_id": "1", "secret": "bad token/with slash"}]}"#,
        ] {
            std::fs::write(f.path(), text).unwrap();
            let err = f.load(false).unwrap_err();
            assert!(
                matches!(err, AlertsFileError::Corrupt { .. }),
                "{text}: {err}"
            );
            assert!(!err.to_string().contains("bad token"), "{err}");
        }
        std::fs::write(f.path(), r#"{"version": 2}"#).unwrap();
        assert!(matches!(
            f.load(false).unwrap_err(),
            AlertsFileError::UnknownVersion { .. }
        ));
        let mut d = doc();
        d.rules.push(d.rules[0].clone());
        f.save(&d).unwrap();
        assert!(f.load(false).is_err(), "duplicate rule id");
    }
}
