//! Base asset alias table loaded at startup.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

const BUILTIN: &str = include_str!("../../data/base_aliases.toml");

/// Errors of loading the alias table.
#[derive(Debug, thiserror::Error)]
pub enum AliasError {
    /// The file could not be read.
    #[error("cannot read alias file {path}: {source}")]
    Read {
        /// Path of the file.
        path: std::path::PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// The text is not valid TOML or has the wrong structure.
    #[error("invalid alias table: {0}")]
    Parse(String),
    /// An entry has an unusable value.
    #[error("invalid alias entry [{exchange}.{name}]: {message}")]
    Entry {
        /// Exchange section.
        exchange: String,
        /// Exchange-side name.
        name: String,
        /// What is wrong.
        message: &'static str,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    base: String,
    multiplier: f64,
}

/// Result of resolving an exchange-side base name.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedBase {
    /// Real base asset.
    pub base: String,
    /// Real coins per one unit of the exchange-side name (1.0 when there is no alias).
    pub multiplier: f64,
}

/// `exchange -> exchange-side name -> (base, multiplier)`.
#[derive(Debug, Clone, Default)]
pub struct BaseAliases {
    table: HashMap<String, HashMap<String, Entry>>,
}

impl BaseAliases {
    /// The table shipped with the binary (`data/base_aliases.toml`).
    pub fn builtin() -> Self {
        Self::from_toml_str(BUILTIN).expect("built-in alias table is valid")
    }

    /// Parses a table from TOML text.
    pub fn from_toml_str(text: &str) -> Result<Self, AliasError> {
        let table: HashMap<String, HashMap<String, Entry>> =
            toml::from_str(text).map_err(|e| AliasError::Parse(e.to_string()))?;
        for (exchange, names) in &table {
            for (name, entry) in names {
                let bad = |message| AliasError::Entry {
                    exchange: exchange.clone(),
                    name: name.clone(),
                    message,
                };
                if entry.base.is_empty() || entry.base.contains(['-', ' ']) {
                    return Err(bad("base must be non-empty without '-' or spaces"));
                }
                if !(entry.multiplier.is_finite() && entry.multiplier > 0.0) {
                    return Err(bad("multiplier must be a positive finite number"));
                }
            }
        }
        Ok(Self { table })
    }

    /// Loads a table from a file (replaces the built-in one).
    pub fn load(path: &Path) -> Result<Self, AliasError> {
        let text = std::fs::read_to_string(path).map_err(|source| AliasError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_toml_str(&text)
    }

    /// Resolves a name; names without an entry are returned unchanged with multiplier 1.
    pub fn resolve(&self, exchange: &str, name: &str) -> ResolvedBase {
        match self.table.get(exchange).and_then(|m| m.get(name)) {
            Some(e) => ResolvedBase {
                base: e.base.clone(),
                multiplier: e.multiplier,
            },
            None => ResolvedBase {
                base: name.to_string(),
                multiplier: 1.0,
            },
        }
    }

    /// Whether an explicit entry exists.
    pub fn contains(&self, exchange: &str, name: &str) -> bool {
        self.table
            .get(exchange)
            .is_some_and(|m| m.contains_key(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_resolves_known_pseudo_coins() {
        let a = BaseAliases::builtin();
        let r = a.resolve("binance", "1000PEPE");
        assert_eq!(r.base, "PEPE");
        assert_eq!(r.multiplier, 1000.0);
        assert_eq!(
            a.resolve("bybit", "1000000BABYDOGE").multiplier,
            1_000_000.0
        );
        assert_eq!(a.resolve("bitmex", "XBT").base, "BTC");
        let mog = a.resolve("bybit", "1000000MOG");
        assert_eq!((mog.base.as_str(), mog.multiplier), ("MOG", 1_000_000.0));
        assert_eq!(a.resolve("bybit", "10000SATS").multiplier, 10_000.0);
        assert_eq!(a.resolve("bybit", "10000NEX").base, "NEX");
    }

    #[test]
    fn unknown_names_pass_through() {
        let a = BaseAliases::builtin();
        let r = a.resolve("okx", "PEPE");
        assert_eq!(r.base, "PEPE");
        assert_eq!(r.multiplier, 1.0);
        // The same name on an exchange without an entry is not rewritten.
        assert_eq!(a.resolve("okx", "1000PEPE").base, "1000PEPE");
        assert_eq!(a.resolve("nowhere", "XBT").base, "XBT");
        assert!(!a.contains("okx", "PEPE"));
    }

    #[test]
    fn rejects_bad_entries() {
        let zero = "[binance.X]\nbase = \"Y\"\nmultiplier = 0\n";
        assert!(matches!(
            BaseAliases::from_toml_str(zero),
            Err(AliasError::Entry { .. })
        ));
        let empty = "[binance.X]\nbase = \"\"\nmultiplier = 1\n";
        assert!(BaseAliases::from_toml_str(empty).is_err());
        let extra = "[binance.X]\nbase = \"Y\"\nmultiplier = 1\nfoo = 2\n";
        assert!(matches!(
            BaseAliases::from_toml_str(extra),
            Err(AliasError::Parse(_))
        ));
    }
}
