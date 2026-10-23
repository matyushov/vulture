//! The set of symbols the connector works with, built from `exchangeInfo`.

use std::collections::{HashMap, HashSet};

use crate::normalize::{
    BaseAliases, InstrumentMeta, Venue, is_wanted_market, normalize_instrument, normalize_symbol,
    symbol::QUOTES,
};

/// One collected symbol.
#[derive(Debug, Clone, PartialEq)]
pub struct SymbolEntry {
    /// Symbol as Binance names it (`1000PEPEUSDT`), UTF-8.
    pub raw: String,
    /// Normalized name (`PEPE-USDT-PERP`).
    pub symbol: String,
    /// Real coins per exchange unit (1000 for `1000PEPE`, 1 when the name had to stay as is).
    pub multiplier: f64,
}

/// Result of [`SymbolTable::build`] besides the table itself.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BuildReport {
    /// Symbols whose normalized name collided with another symbol and that were renamed to their
    /// exchange-side name (`1000PEPEUSDT` -> `1000PEPE-USDT-PERP`).
    pub renamed_on_collision: Vec<String>,
    /// Symbols dropped because normalization failed or the name stayed ambiguous.
    pub skipped: Vec<String>,
}

/// Immutable snapshot of the collected symbols, ordered by raw name.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SymbolTable {
    entries: Vec<SymbolEntry>,
    index: HashMap<String, usize>,
}

/// Difference between two tables, by raw symbol.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SymbolDiff {
    /// In the new table only (new listings).
    pub added: Vec<String>,
    /// In the old table only (delisted, settling, no longer trading).
    pub removed: Vec<String>,
}

impl SymbolDiff {
    /// Whether nothing changed.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }
}

impl SymbolTable {
    /// Filters `metas` (perpetual, trading, USDT/USDC; `TRADIFI_PERPETUAL` stays out), normalizes the names
    /// and applies the optional whitelist of normalized symbols.
    ///
    /// **Alias collisions.** `PEPEUSDT` and `1000PEPEUSDT` both normalize to `PEPE-USDT-PERP`. They are
    /// different markets (different price scale and different open interest units), so merging them into one
    /// series would corrupt both. Rule: a symbol whose base is not an alias keeps the normalized name; the
    /// aliased one is renamed to its exchange-side name with multiplier 1 (`1000PEPE-USDT-PERP`), reported in
    /// [`BuildReport::renamed_on_collision`] and logged by the caller. Nothing is dropped and nothing is
    /// merged silently. If the fallback name is taken as well, the symbol is skipped and reported.
    pub fn build(
        metas: &[InstrumentMeta],
        aliases: &BaseAliases,
        whitelist: &[String],
    ) -> (Self, BuildReport) {
        let mut report = BuildReport::default();
        struct Candidate<'a> {
            meta: &'a InstrumentMeta,
            symbol: String,
            multiplier: f64,
            aliased: bool,
        }
        let mut candidates = Vec::new();
        for meta in metas.iter().filter(|m| is_wanted_market(m)) {
            match normalize_instrument(Venue::Binance, meta, aliases) {
                Ok(n) => candidates.push(Candidate {
                    aliased: n.base != meta.base,
                    meta,
                    symbol: n.symbol,
                    multiplier: n.multiplier,
                }),
                Err(_) => report.skipped.push(meta.raw.clone()),
            }
        }
        // Plain symbols first so they win a collision; raw name as a deterministic tie-breaker.
        candidates.sort_by(|a, b| (a.aliased, &a.meta.raw).cmp(&(b.aliased, &b.meta.raw)));

        let mut taken: HashSet<String> = HashSet::new();
        let mut entries = Vec::new();
        for c in candidates {
            let (symbol, multiplier) = if taken.insert(c.symbol.clone()) {
                (c.symbol, c.multiplier)
            } else {
                let fallback =
                    format!("{}-{}-PERP", c.meta.base, c.meta.quote.to_ascii_uppercase());
                if taken.insert(fallback.clone()) {
                    report.renamed_on_collision.push(c.meta.raw.clone());
                    (fallback, 1.0)
                } else {
                    report.skipped.push(c.meta.raw.clone());
                    continue;
                }
            };
            if !whitelist.is_empty() && !whitelist.contains(&symbol) {
                continue;
            }
            entries.push(SymbolEntry {
                raw: c.meta.raw.clone(),
                symbol,
                multiplier,
            });
        }
        entries.sort_by(|a, b| a.raw.cmp(&b.raw));
        report.renamed_on_collision.sort();
        report.skipped.sort();
        let index = entries
            .iter()
            .enumerate()
            .map(|(i, e)| (e.raw.clone(), i))
            .collect();
        (Self { entries, index }, report)
    }

    /// Symbols in raw-name order.
    pub fn entries(&self) -> &[SymbolEntry] {
        &self.entries
    }

    /// Number of symbols.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Looks a symbol up by its raw name.
    pub fn get(&self, raw: &str) -> Option<&SymbolEntry> {
        self.index.get(raw).map(|&i| &self.entries[i])
    }

    /// What changed from `self` (old) to `new`.
    pub fn diff(&self, new: &SymbolTable) -> SymbolDiff {
        SymbolDiff {
            added: new
                .entries
                .iter()
                .filter(|e| !self.index.contains_key(&e.raw))
                .map(|e| e.raw.clone())
                .collect(),
            removed: self
                .entries
                .iter()
                .filter(|e| !new.index.contains_key(&e.raw))
                .map(|e| e.raw.clone())
                .collect(),
        }
    }
}

/// Resolves a raw symbol of a liquidation when no table has been loaded yet (first seconds after start):
/// parse the name with the strict rules of the normalizer. Anything unrecognized is `None`.
pub fn fallback_entry(raw: &str, aliases: &BaseAliases) -> Option<SymbolEntry> {
    if !QUOTES.iter().any(|q| raw.ends_with(q)) {
        return None;
    }
    let n = normalize_symbol(Venue::Binance, raw, None, aliases).ok()?;
    Some(SymbolEntry {
        raw: raw.to_string(),
        symbol: n.symbol,
        multiplier: n.multiplier,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::{ContractKind, parse_binance_exchange_info};

    fn meta(raw: &str, base: &str, quote: &str) -> InstrumentMeta {
        InstrumentMeta {
            raw: raw.into(),
            base: base.into(),
            quote: quote.into(),
            kind: ContractKind::Perpetual,
            trading: true,
        }
    }

    #[test]
    fn fixture_selection_and_normalization() {
        let json = include_str!("../../../../../fixtures/binance/exchange_info_sample.json");
        let metas = parse_binance_exchange_info(json).unwrap();
        let (t, report) = SymbolTable::build(&metas, &BaseAliases::builtin(), &[]);
        // Ordered by raw name (byte order).
        let names: Vec<_> = t.entries().iter().map(|e| e.symbol.as_str()).collect();
        assert_eq!(
            names,
            [
                "PEPE-USDT-PERP",
                "BTC-USDC-PERP",
                "BTC-USDT-PERP",
                "ETH-USDT-PERP",
                "币安人生-USDT-PERP"
            ]
        );
        assert_eq!(t.get("1000PEPEUSDT").unwrap().multiplier, 1000.0);
        assert!(t.get("币安人生USDT").is_some());
        assert!(report.renamed_on_collision.is_empty() && report.skipped.is_empty());
    }

    #[test]
    fn whitelist_selects_by_normalized_name() {
        let metas = vec![
            meta("BTCUSDT", "BTC", "USDT"),
            meta("ETHUSDT", "ETH", "USDT"),
        ];
        let (t, _) = SymbolTable::build(
            &metas,
            &BaseAliases::builtin(),
            &["ETH-USDT-PERP".to_string()],
        );
        assert_eq!(t.len(), 1);
        assert_eq!(t.entries()[0].raw, "ETHUSDT");
    }

    #[test]
    fn alias_collision_keeps_both_markets_apart() {
        let metas = vec![
            meta("1000PEPEUSDT", "1000PEPE", "USDT"),
            meta("PEPEUSDT", "PEPE", "USDT"),
        ];
        let (t, report) = SymbolTable::build(&metas, &BaseAliases::builtin(), &[]);
        assert_eq!(t.len(), 2);
        let plain = t.get("PEPEUSDT").unwrap();
        assert_eq!(plain.symbol, "PEPE-USDT-PERP");
        assert_eq!(plain.multiplier, 1.0);
        let scaled = t.get("1000PEPEUSDT").unwrap();
        assert_eq!(scaled.symbol, "1000PEPE-USDT-PERP");
        assert_eq!(scaled.multiplier, 1.0);
        assert_ne!(plain.symbol, scaled.symbol);
        assert_eq!(report.renamed_on_collision, ["1000PEPEUSDT"]);
        // The result does not depend on the input order.
        let mut reversed = metas.clone();
        reversed.reverse();
        let (t2, _) = SymbolTable::build(&reversed, &BaseAliases::builtin(), &[]);
        assert_eq!(t, t2);
    }

    #[test]
    fn diff_reports_listings_and_removals() {
        let a = vec![
            meta("BTCUSDT", "BTC", "USDT"),
            meta("ETHUSDT", "ETH", "USDT"),
        ];
        let mut b = vec![
            meta("BTCUSDT", "BTC", "USDT"),
            meta("SOLUSDT", "SOL", "USDT"),
        ];
        let al = BaseAliases::builtin();
        let (ta, _) = SymbolTable::build(&a, &al, &[]);
        let (tb, _) = SymbolTable::build(&b, &al, &[]);
        let d = ta.diff(&tb);
        assert_eq!(d.added, ["SOLUSDT"]);
        assert_eq!(d.removed, ["ETHUSDT"]);
        // A symbol that stops trading leaves the set.
        b[1].trading = false;
        let (tc, _) = SymbolTable::build(&b, &al, &[]);
        assert_eq!(tb.diff(&tc).removed, ["SOLUSDT"]);
        assert!(ta.diff(&ta).is_empty());
    }

    #[test]
    fn fallback_parses_only_unambiguous_names() {
        let al = BaseAliases::builtin();
        assert_eq!(
            fallback_entry("BTCUSDT", &al).unwrap().symbol,
            "BTC-USDT-PERP"
        );
        assert!(fallback_entry("BTCUSD1", &al).is_none());
    }
}
