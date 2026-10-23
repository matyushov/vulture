//! Normalization of exchange data into the common schema.
//!
//! - [`symbol`]: raw instrument names to `BASE-QUOTE-PERP`.
//! - [`aliases`]: pseudo-coin names (`1000PEPE`, `XBT`) to real base assets with a multiplier.
//! - [`oi`]: open interest to `oi_base` and `oi_usd`; pure functions, explicit errors for unknown cases.
//! - [`market`]: which instruments are collected (linear USDT/USDC perpetuals only).

pub mod aliases;
pub mod market;
pub mod oi;
pub mod symbol;

pub use aliases::{AliasError, BaseAliases, ResolvedBase};
pub use market::{ContractKind, InstrumentMeta, is_wanted_market, parse_binance_exchange_info};
pub use oi::{ContractSize, DeribitContract, OiError, OiValues};
pub use symbol::{NormalizeError, Normalized, Venue, normalize_instrument, normalize_symbol};
