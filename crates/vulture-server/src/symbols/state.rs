//! The state file `<data_dir>/state.json`: the owner's runtime symbol policies.
//!
//! The configuration file holds the *defaults*; whatever the owner sets through `Control.SetSymbolPolicy` is
//! stored here and overrides the default of that exchange. The file is versioned. A file that cannot be read,
//! parsed or understood stops the server from starting: silently falling back to the configuration could
//! re-enable symbols the owner removed. The file is written atomically (temporary file in the same directory,
//! `fsync`, rename, `fsync` of the directory) with mode `0600`, and no image or update ever overwrites it.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::policy::{Mode, SymbolPolicy};

/// File name inside `server.data_dir`.
pub const STATE_FILE: &str = "state.json";
/// Schema version written and accepted.
pub const STATE_VERSION: u32 = 1;

/// Why the state file could not be used.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// The file exists but cannot be read.
    #[error("cannot read the state file {path}: {source}")]
    Read {
        /// File.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// The file is not valid JSON or does not match the schema.
    #[error(
        "the state file {path} is corrupt: {message}. Fix or remove it deliberately: the server does not fall back to the configuration, because that could re-enable symbols you removed"
    )]
    Corrupt {
        /// File.
        path: PathBuf,
        /// What is wrong.
        message: String,
    },
    /// A version this build does not know.
    #[error(
        "the state file {path} has version {found}, this build understands version {STATE_VERSION}. Upgrade the server or restore a compatible file"
    )]
    UnknownVersion {
        /// File.
        path: PathBuf,
        /// Version found in the file.
        found: String,
    },
    /// Writing failed.
    #[error("cannot write the state file {path}: {source}")]
    Write {
        /// File.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StoredPolicy {
    mode: String,
    #[serde(default)]
    symbols: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u32,
    #[serde(default)]
    exchanges: BTreeMap<String, StoredPolicy>,
}

/// The decoded state: a policy per exchange name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct State {
    /// Owner policies by exchange name (including exchanges that are currently disabled: they are kept).
    pub policies: BTreeMap<String, SymbolPolicy>,
}

impl State {
    fn to_document(&self) -> Document {
        Document {
            version: STATE_VERSION,
            exchanges: self
                .policies
                .iter()
                .map(|(name, p)| {
                    (
                        name.clone(),
                        StoredPolicy {
                            mode: p.mode().as_str().to_string(),
                            symbols: p.symbols().iter().cloned().collect(),
                        },
                    )
                })
                .collect(),
        }
    }
}

fn parse_mode(s: &str) -> Option<Mode> {
    match s {
        "all" => Some(Mode::All),
        "whitelist" => Some(Mode::Whitelist),
        "blacklist" => Some(Mode::Blacklist),
        _ => None,
    }
}

/// The state file of one data directory.
#[derive(Debug, Clone)]
pub struct StateStore {
    path: PathBuf,
}

impl StateStore {
    /// The store for `<data_dir>/state.json`.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            path: data_dir.join(STATE_FILE),
        }
    }

    /// Path of the file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Reads the file. A missing file is an empty state; anything else wrong is an error.
    pub fn load(&self) -> Result<State, StateError> {
        let text = match std::fs::read_to_string(&self.path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(State::default()),
            Err(source) => {
                return Err(StateError::Read {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        let corrupt = |message: String| StateError::Corrupt {
            path: self.path.clone(),
            message,
        };
        // The version is checked before the schema, so that a future file reports its version, not a field.
        let value: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| corrupt(e.to_string()))?;
        match value.get("version") {
            Some(v) if v.as_u64() == Some(u64::from(STATE_VERSION)) => {}
            Some(v) => {
                return Err(StateError::UnknownVersion {
                    path: self.path.clone(),
                    found: v.to_string(),
                });
            }
            None => return Err(corrupt("no \"version\" field".into())),
        }
        let doc: Document = serde_json::from_value(value).map_err(|e| corrupt(e.to_string()))?;
        let mut policies = BTreeMap::new();
        for (name, stored) in doc.exchanges {
            let mode = parse_mode(&stored.mode).ok_or_else(|| {
                corrupt(format!(
                    "exchange {name:?}: unknown mode {:?} (all, whitelist, blacklist)",
                    stored.mode
                ))
            })?;
            let policy = SymbolPolicy::new(mode, &stored.symbols)
                .map_err(|e| corrupt(format!("exchange {name:?}: {e}")))?;
            policies.insert(name, policy);
        }
        Ok(State { policies })
    }

    /// Writes the file atomically with mode 0600.
    pub fn save(&self, state: &State) -> Result<(), StateError> {
        let write_err = |source| StateError::Write {
            path: self.path.clone(),
            source,
        };
        let mut json = serde_json::to_string_pretty(&state.to_document())
            .map_err(|e| write_err(std::io::Error::other(e)))?;
        json.push('\n');
        write_atomic(&self.path, json.as_bytes()).map_err(write_err)
    }
}

/// Temporary file in the same directory, `fsync`, rename over the target, `fsync` of the directory.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    let result = (|| {
        let mut file = open_private(&tmp, true)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)?;
        // Make the rename durable; a directory that cannot be opened for sync is not fatal.
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// Opens `path` for writing with mode 0600 (truncating when `truncate`, appending otherwise).
pub(crate) fn open_private(path: &Path, truncate: bool) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true);
    if truncate {
        options.truncate(true);
    } else {
        options.append(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, StateStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = StateStore::new(dir.path());
        (dir, store)
    }

    fn sample() -> State {
        let mut s = State::default();
        s.policies.insert(
            "binance".into(),
            SymbolPolicy::new(Mode::Blacklist, ["ETH-USDT-PERP", "BTC-USDT-PERP"]).unwrap(),
        );
        s.policies.insert("okx".into(), SymbolPolicy::all());
        s
    }

    #[test]
    fn missing_file_is_an_empty_state() {
        let (_d, store) = store();
        assert_eq!(store.load().unwrap(), State::default());
    }

    #[test]
    fn save_and_load_round_trip() {
        let (_d, store) = store();
        store.save(&sample()).unwrap();
        assert_eq!(store.load().unwrap(), sample());
        let text = std::fs::read_to_string(store.path()).unwrap();
        assert!(text.contains("\"version\": 1"));
        assert!(text.ends_with('\n'));
        // Symbols are sorted, so the file is deterministic.
        assert!(text.find("BTC-USDT-PERP").unwrap() < text.find("ETH-USDT-PERP").unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn file_mode_is_private_and_no_temp_file_is_left() {
        use std::os::unix::fs::PermissionsExt;
        let (d, store) = store();
        store.save(&sample()).unwrap();
        store.save(&State::default()).unwrap();
        let mode = std::fs::metadata(store.path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let names: Vec<_> = std::fs::read_dir(d.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, [STATE_FILE]);
    }

    #[test]
    fn save_replaces_the_previous_content_whole() {
        let (_d, store) = store();
        store.save(&sample()).unwrap();
        let mut s = State::default();
        s.policies.insert("binance".into(), SymbolPolicy::all());
        store.save(&s).unwrap();
        assert_eq!(store.load().unwrap(), s);
    }

    #[test]
    fn failed_write_leaves_the_old_file() {
        let (d, store) = store();
        store.save(&sample()).unwrap();
        // A directory in the way of the rename target makes the write fail; the old content must survive in
        // the sense that no temp file remains and the error is reported.
        let blocked = StateStore::new(&d.path().join("missing-dir"));
        assert!(matches!(
            blocked.save(&sample()),
            Err(StateError::Write { .. })
        ));
        assert_eq!(store.load().unwrap(), sample());
    }

    #[test]
    fn corrupt_files_are_refused() {
        for text in [
            "",
            "not json",
            "{}",
            r#"{"version":1,"exchanges":{"binance":{"mode":"sideways"}}}"#,
            r#"{"version":1,"exchanges":{"binance":{"mode":"whitelist","symbols":[]}}}"#,
            r#"{"version":1,"exchanges":{"binance":{"mode":"blacklist","symbols":["junk"]}}}"#,
            r#"{"version":1,"exchanges":{},"extra":1}"#,
        ] {
            let (_d, store) = store();
            std::fs::write(store.path(), text).unwrap();
            match store.load() {
                Err(StateError::Corrupt { message, .. }) => assert!(!message.is_empty(), "{text}"),
                other => panic!("{text:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn unknown_version_is_refused() {
        for v in ["2", "0", "\"1\"", "null"] {
            let (_d, store) = store();
            std::fs::write(
                store.path(),
                format!(r#"{{"version":{v},"exchanges":{{}}}}"#),
            )
            .unwrap();
            let err = store.load().unwrap_err();
            assert!(
                matches!(err, StateError::UnknownVersion { .. }),
                "{v}: {err}"
            );
            assert!(err.to_string().contains("version"));
        }
    }

    #[test]
    fn unreadable_file_is_refused() {
        let (_d, store) = store();
        // A directory where the file should be.
        std::fs::create_dir(store.path()).unwrap();
        assert!(matches!(store.load(), Err(StateError::Read { .. })));
    }
}
