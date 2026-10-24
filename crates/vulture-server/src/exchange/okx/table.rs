//! The OKX universe: the generic symbol table plus the contract size of every instrument.

use std::collections::HashMap;

use crate::exchange::binance::{BuildReport, SymbolDiff, SymbolEntry, SymbolTable};
use crate::normalize::{BaseAliases, ContractSize, OkxInstruments, Venue};
use crate::symbols::SymbolPolicy;

/// A collected instrument with its contract size.
#[derive(Debug, Clone, Copy)]
pub struct OkxEntry<'a> {
    /// Names and the alias multiplier.
    pub symbol: &'a SymbolEntry,
    /// `ctVal x ctMult` in `ctValCcy` (the base coin) per contract.
    pub size: &'a ContractSize,
}

/// Immutable snapshot of the collected instruments.
#[derive(Debug, Clone, Default)]
pub struct OkxTable {
    table: SymbolTable,
    sizes: HashMap<String, ContractSize>,
}

impl OkxTable {
    /// Filters (linear, live, USDT/USDC, no `USD_UM`), normalizes the names and keeps the contract sizes.
    pub fn build(list: &OkxInstruments, aliases: &BaseAliases) -> (Self, BuildReport) {
        let (table, report) = SymbolTable::build_for(Venue::Okx, &list.metas, aliases);
        let sizes = table
            .entries()
            .iter()
            .filter_map(|e| list.sizes.get(&e.raw).map(|s| (e.raw.clone(), s.clone())))
            .collect();
        (Self { table, sizes }, report)
    }

    /// Number of instruments.
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Entries in raw-name order.
    pub fn entries(&self) -> &[SymbolEntry] {
        self.table.entries()
    }

    /// Looks an instrument up by `instId`.
    pub fn get(&self, raw: &str) -> Option<OkxEntry<'_>> {
        Some(OkxEntry {
            symbol: self.table.get(raw)?,
            size: self.sizes.get(raw)?,
        })
    }

    /// The entries the policy tracks.
    pub fn tracked<'a>(&'a self, policy: &'a SymbolPolicy) -> impl Iterator<Item = OkxEntry<'a>> {
        self.table.tracked(policy).filter_map(|e| {
            Some(OkxEntry {
                symbol: e,
                size: self.sizes.get(&e.raw)?,
            })
        })
    }

    /// What changed from `self` (old) to `new`.
    pub fn diff(&self, new: &OkxTable) -> SymbolDiff {
        self.table.diff(&new.table)
    }
}
