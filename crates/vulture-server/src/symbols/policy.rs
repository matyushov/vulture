//! The symbol policy: which symbols of an exchange are tracked.
//!
//! Pure data and pure functions, no I/O. A policy is a mode plus a set of normalized symbol names
//! (`BTC-USDT-PERP`); the *effective* tracked set is computed from a policy and the exchange's universe (every
//! symbol the exchange offers, already filtered to linear USDT/USDC perpetuals):
//!
//! - [`Mode::All`]: the whole universe; the symbol list is empty.
//! - [`Mode::Whitelist`]: only the listed symbols that exist in the universe.
//! - [`Mode::Blacklist`]: the universe minus the listed symbols.
//!
//! The effective set is recomputed whenever the policy or the universe changes, so a whitelisted symbol that is
//! delisted and listed again comes back without any action.

use std::collections::BTreeSet;

use crate::normalize::symbol::QUOTES;

/// Longest accepted symbol name, bytes.
pub const MAX_SYMBOL_LEN: usize = 64;
/// Most symbols in one policy (guards the state file and the audit log against absurd requests).
pub const MAX_POLICY_SYMBOLS: usize = 5_000;

/// How the symbol list of a policy is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Mode {
    /// Every symbol of the universe.
    #[default]
    All,
    /// Only the listed symbols.
    Whitelist,
    /// Everything except the listed symbols.
    Blacklist,
}

impl Mode {
    /// Lowercase name used in the state file, the audit log and messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::All => "all",
            Mode::Whitelist => "whitelist",
            Mode::Blacklist => "blacklist",
        }
    }
}

/// The universe of an exchange: normalized names, sorted.
pub type Universe = BTreeSet<String>;

/// Why a policy was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    /// A whitelist without symbols would track nothing.
    #[error("a WHITELIST policy needs at least one symbol (use BLACKLIST or ALL to track more)")]
    EmptyWhitelist,
    /// Too many symbols.
    #[error("a policy may list at most {MAX_POLICY_SYMBOLS} symbols, got {0}")]
    TooMany(usize),
    /// Names that are not `BASE-QUOTE-PERP`.
    #[error("malformed symbol names (expected BASE-USDT-PERP or BASE-USDC-PERP): {}", .0.join(", "))]
    Malformed(Vec<String>),
}

/// Trims, upper-cases ASCII letters and checks `BASE-QUOTE-PERP` with a USDT/USDC quote. Returns the canonical
/// form or `None`. Non-ASCII bases (for example Chinese tickers) are kept as they are.
pub fn normalize_symbol_name(raw: &str) -> Option<String> {
    let name = raw.trim().to_ascii_uppercase();
    if name.is_empty() || name.len() > MAX_SYMBOL_LEN {
        return None;
    }
    let mut parts = name.split('-');
    let (base, quote, kind, rest) = (parts.next()?, parts.next()?, parts.next()?, parts.next());
    if rest.is_some() || kind != "PERP" || !QUOTES.contains(&quote) {
        return None;
    }
    if base.is_empty() || base.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return None;
    }
    Some(name)
}

/// A symbol policy. Symbols are canonical and unique (a set).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SymbolPolicy {
    mode: Mode,
    symbols: BTreeSet<String>,
}

impl SymbolPolicy {
    /// Track the whole universe.
    pub fn all() -> Self {
        Self::default()
    }

    /// Validates and normalizes a request. Duplicates collapse, names are canonicalized, `All` drops the list.
    /// Every malformed name is reported at once.
    pub fn new<I, S>(mode: Mode, symbols: I) -> Result<Self, PolicyError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut set = BTreeSet::new();
        let mut bad = Vec::new();
        let mut count = 0usize;
        for raw in symbols {
            count += 1;
            match normalize_symbol_name(raw.as_ref()) {
                Some(n) => {
                    set.insert(n);
                }
                None => bad.push(raw.as_ref().to_string()),
            }
        }
        if mode == Mode::All {
            // The list is documented as ignored for ALL; the stored policy keeps it empty.
            return Ok(Self::all());
        }
        if count > MAX_POLICY_SYMBOLS {
            return Err(PolicyError::TooMany(count));
        }
        if !bad.is_empty() {
            bad.sort();
            bad.dedup();
            return Err(PolicyError::Malformed(bad));
        }
        if mode == Mode::Whitelist && set.is_empty() {
            return Err(PolicyError::EmptyWhitelist);
        }
        Ok(Self { mode, symbols: set })
    }

    /// The default policy from the configuration: an empty `symbols` list means every symbol, a non-empty one
    /// is a whitelist. The configuration validator has already checked the names; unusable ones are dropped.
    pub fn from_config(symbols: &[String]) -> Self {
        let set: BTreeSet<String> = symbols
            .iter()
            .filter_map(|s| normalize_symbol_name(s))
            .collect();
        if set.is_empty() {
            Self::all()
        } else {
            Self {
                mode: Mode::Whitelist,
                symbols: set,
            }
        }
    }

    /// The mode.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// The listed symbols (empty for `All`).
    pub fn symbols(&self) -> &BTreeSet<String> {
        &self.symbols
    }

    /// Whether `symbol` (canonical form) is tracked, assuming it is in the universe.
    pub fn allows(&self, symbol: &str) -> bool {
        match self.mode {
            Mode::All => true,
            Mode::Whitelist => self.symbols.contains(symbol),
            Mode::Blacklist => !self.symbols.contains(symbol),
        }
    }

    /// The tracked set for `universe`.
    pub fn effective(&self, universe: &Universe) -> BTreeSet<String> {
        universe
            .iter()
            .filter(|s| self.allows(s))
            .cloned()
            .collect()
    }

    /// Listed symbols that the universe does not contain (typos, delisted, other quote).
    pub fn unknown(&self, universe: &Universe) -> Vec<String> {
        self.symbols
            .iter()
            .filter(|s| !universe.contains(*s))
            .cloned()
            .collect()
    }
}

/// What changed between two tracked sets.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SetDiff {
    /// In the new set only, sorted.
    pub added: Vec<String>,
    /// In the old set only, sorted.
    pub removed: Vec<String>,
}

impl SetDiff {
    /// Computes the difference `old` -> `new`.
    pub fn between(old: &BTreeSet<String>, new: &BTreeSet<String>) -> Self {
        Self {
            added: new.difference(old).cloned().collect(),
            removed: old.difference(new).cloned().collect(),
        }
    }

    /// Whether nothing changed.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uni(names: &[&str]) -> Universe {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn set(names: &[&str]) -> BTreeSet<String> {
        uni(names)
    }

    #[test]
    fn name_normalization() {
        assert_eq!(
            normalize_symbol_name(" btc-usdt-perp ").as_deref(),
            Some("BTC-USDT-PERP")
        );
        assert_eq!(
            normalize_symbol_name("币安人生-USDT-PERP").as_deref(),
            Some("币安人生-USDT-PERP")
        );
        assert_eq!(
            normalize_symbol_name("1000PEPE-USDC-PERP").as_deref(),
            Some("1000PEPE-USDC-PERP")
        );
        for bad in [
            "",
            "   ",
            "BTCUSDT",
            "BTC-USDT",
            "BTC-USDT-SWAP",
            "BTC-EUR-PERP",
            "-USDT-PERP",
            "BTC-USDT-PERP-X",
            "B C-USDT-PERP",
            "BTC--PERP",
        ] {
            assert_eq!(normalize_symbol_name(bad), None, "{bad:?}");
        }
        assert_eq!(
            normalize_symbol_name(&format!("{}-USDT-PERP", "A".repeat(MAX_SYMBOL_LEN))),
            None
        );
    }

    #[test]
    fn all_ignores_the_list() {
        let p = SymbolPolicy::new(Mode::All, ["garbage", "BTC-USDT-PERP"]).unwrap();
        assert_eq!(p, SymbolPolicy::all());
        assert!(p.symbols().is_empty());
    }

    #[test]
    fn whitelist_rules() {
        assert_eq!(
            SymbolPolicy::new(Mode::Whitelist, Vec::<String>::new()),
            Err(PolicyError::EmptyWhitelist)
        );
        // Duplicates and case collapse into one entry.
        let p = SymbolPolicy::new(
            Mode::Whitelist,
            ["eth-usdt-perp", "BTC-USDT-PERP", "ETH-USDT-PERP"],
        )
        .unwrap();
        assert_eq!(p.symbols().len(), 2);
        let u = uni(&["BTC-USDT-PERP", "ETH-USDT-PERP", "SOL-USDT-PERP"]);
        assert_eq!(p.effective(&u), set(&["BTC-USDT-PERP", "ETH-USDT-PERP"]));
    }

    #[test]
    fn blacklist_rules() {
        let u = uni(&["BTC-USDT-PERP", "ETH-USDT-PERP", "SOL-USDT-PERP"]);
        let p = SymbolPolicy::new(Mode::Blacklist, ["ETH-USDT-PERP"]).unwrap();
        assert_eq!(p.effective(&u), set(&["BTC-USDT-PERP", "SOL-USDT-PERP"]));
        // An empty blacklist is the whole universe, and stays a blacklist.
        let empty = SymbolPolicy::new(Mode::Blacklist, Vec::<String>::new()).unwrap();
        assert_eq!(empty.mode(), Mode::Blacklist);
        assert_eq!(empty.effective(&u), u);
    }

    #[test]
    fn malformed_names_are_all_reported() {
        let err = SymbolPolicy::new(
            Mode::Whitelist,
            ["BTC-USDT-PERP", "oops", "bad-name", "oops"],
        )
        .unwrap_err();
        assert_eq!(
            err,
            PolicyError::Malformed(vec!["bad-name".into(), "oops".into()])
        );
        assert!(err.to_string().contains("oops"));
    }

    #[test]
    fn too_many_symbols() {
        let names: Vec<String> = (0..=MAX_POLICY_SYMBOLS)
            .map(|i| format!("C{i}-USDT-PERP"))
            .collect();
        assert!(matches!(
            SymbolPolicy::new(Mode::Blacklist, &names),
            Err(PolicyError::TooMany(_))
        ));
    }

    #[test]
    fn universe_changes_recompute_the_effective_set() {
        let wl = SymbolPolicy::new(Mode::Whitelist, ["BTC-USDT-PERP", "NEW-USDT-PERP"]).unwrap();
        let bl = SymbolPolicy::new(Mode::Blacklist, ["BTC-USDT-PERP"]).unwrap();
        let before = uni(&["BTC-USDT-PERP", "ETH-USDT-PERP"]);
        let after = uni(&["BTC-USDT-PERP", "ETH-USDT-PERP", "NEW-USDT-PERP"]);
        // A whitelisted symbol that gets listed later starts being tracked.
        assert_eq!(wl.effective(&before), set(&["BTC-USDT-PERP"]));
        assert_eq!(
            wl.effective(&after),
            set(&["BTC-USDT-PERP", "NEW-USDT-PERP"])
        );
        // A new listing is tracked under a blacklist and ALL.
        assert_eq!(
            bl.effective(&after),
            set(&["ETH-USDT-PERP", "NEW-USDT-PERP"])
        );
        assert_eq!(SymbolPolicy::all().effective(&after), after);
        // A delisting leaves the set but stays in the policy.
        let delisted = uni(&["ETH-USDT-PERP"]);
        assert!(wl.effective(&delisted).is_empty());
        assert_eq!(wl.symbols().len(), 2);
    }

    #[test]
    fn unknown_symbols_are_found() {
        let u = uni(&["BTC-USDT-PERP"]);
        let p = SymbolPolicy::new(
            Mode::Whitelist,
            ["ZZZ-USDT-PERP", "BTC-USDT-PERP", "AAA-USDC-PERP"],
        )
        .unwrap();
        assert_eq!(p.unknown(&u), ["AAA-USDC-PERP", "ZZZ-USDT-PERP"]);
        assert!(SymbolPolicy::all().unknown(&u).is_empty());
    }

    #[test]
    fn config_default() {
        assert_eq!(SymbolPolicy::from_config(&[]), SymbolPolicy::all());
        let p = SymbolPolicy::from_config(&["eth-usdt-perp".into(), "junk".into()]);
        assert_eq!(p.mode(), Mode::Whitelist);
        assert_eq!(p.symbols().len(), 1);
        // Only junk: nothing usable, so no accidental empty whitelist.
        assert_eq!(
            SymbolPolicy::from_config(&["junk".into()]),
            SymbolPolicy::all()
        );
    }

    #[test]
    fn deterministic_and_order_independent() {
        let a = SymbolPolicy::new(Mode::Whitelist, ["B-USDT-PERP", "A-USDT-PERP"]).unwrap();
        let b = SymbolPolicy::new(Mode::Whitelist, ["A-USDT-PERP", "B-USDT-PERP"]).unwrap();
        assert_eq!(a, b);
        let u = uni(&["A-USDT-PERP", "B-USDT-PERP", "C-USDT-PERP"]);
        assert_eq!(a.effective(&u), b.effective(&u));
        assert_eq!(
            a.symbols().iter().collect::<Vec<_>>(),
            ["A-USDT-PERP", "B-USDT-PERP"]
        );
    }

    #[test]
    fn diff() {
        let d = SetDiff::between(
            &set(&["A-USDT-PERP", "B-USDT-PERP"]),
            &set(&["B-USDT-PERP", "C-USDT-PERP"]),
        );
        assert_eq!(d.added, ["C-USDT-PERP"]);
        assert_eq!(d.removed, ["A-USDT-PERP"]);
        assert!(SetDiff::between(&set(&["A-USDT-PERP"]), &set(&["A-USDT-PERP"])).is_empty());
    }
}
