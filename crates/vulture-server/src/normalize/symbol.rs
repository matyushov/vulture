//! Raw instrument names to `BASE-QUOTE-PERP`.
//!
//! Base and quote come from exchange metadata when the caller has it; parsing the string is only the
//! fallback and only for formats that are unambiguous. Everything else is an error, never a guess.

use super::aliases::BaseAliases;
use super::market::InstrumentMeta;

/// Quote assets of the supported linear perpetuals.
pub const QUOTES: [&str; 2] = ["USDT", "USDC"];

/// Exchanges with a known naming scheme. This only selects the parsing rule; which exchanges run is
/// decided by the configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    /// `BASEQUOTE`, for example `BTCUSDT`.
    Binance,
    /// `BASEQUOTE`, for example `BTCUSDT`.
    Bybit,
    /// `BASE-QUOTE-SWAP`, for example `BTC-USDT-SWAP`.
    Okx,
    /// `BASEQUOTE` with `XBT` for bitcoin, for example `XBTUSDT`.
    Bitmex,
    /// `BASE_QUOTE-PERPETUAL`, for example `BTC_USDC-PERPETUAL`.
    Deribit,
}

impl Venue {
    /// Venue for a configured exchange name.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "binance" => Self::Binance,
            "bybit" => Self::Bybit,
            "okx" => Self::Okx,
            "bitmex" => Self::Bitmex,
            "deribit" => Self::Deribit,
            _ => return None,
        })
    }

    /// Lowercase exchange name; also the section name in the alias table.
    pub fn name(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Okx => "okx",
            Self::Bitmex => "bitmex",
            Self::Deribit => "deribit",
        }
    }
}

/// Errors of symbol normalization.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NormalizeError {
    /// The raw symbol is empty.
    #[error("empty symbol")]
    Empty,
    /// The string does not match the venue's perpetual format.
    #[error("{venue} symbol {raw:?} does not match the expected format")]
    UnrecognizedFormat {
        /// Exchange name.
        venue: &'static str,
        /// Raw symbol.
        raw: String,
    },
    /// The quote asset is not USDT or USDC.
    #[error("unsupported quote asset {0:?} (only USDT and USDC linear perpetuals are collected)")]
    UnsupportedQuote(String),
    /// Inverse contract (settled in the base coin); out of scope.
    #[error("inverse contract {0:?} is not supported")]
    Inverse(String),
    /// The base asset cannot be used in a normalized symbol.
    #[error("invalid base asset {0:?}")]
    InvalidBase(String),
}

/// Result of normalization.
#[derive(Debug, Clone, PartialEq)]
pub struct Normalized {
    /// `BASE-QUOTE-PERP`.
    pub symbol: String,
    /// Real base asset (after the alias table).
    pub base: String,
    /// `USDT` or `USDC`.
    pub quote: String,
    /// Real coins per one exchange-side base unit (1000 for `1000PEPE`).
    pub multiplier: f64,
}

/// Normalizes an instrument described by metadata.
pub fn normalize_instrument(
    venue: Venue,
    meta: &InstrumentMeta,
    aliases: &BaseAliases,
) -> Result<Normalized, NormalizeError> {
    normalize_symbol(venue, &meta.raw, Some((&meta.base, &meta.quote)), aliases)
}

/// Normalizes a raw symbol. `meta` is `(base, quote)` as the exchange reports them; when present it is
/// used instead of parsing `raw`.
pub fn normalize_symbol(
    venue: Venue,
    raw: &str,
    meta: Option<(&str, &str)>,
    aliases: &BaseAliases,
) -> Result<Normalized, NormalizeError> {
    if raw.is_empty() {
        return Err(NormalizeError::Empty);
    }
    let (base_raw, quote_raw) = match meta {
        Some((b, q)) => (b.to_string(), q.to_string()),
        None => parse_raw(venue, raw)?,
    };
    let quote = quote_raw.to_ascii_uppercase();
    if !QUOTES.contains(&quote.as_str()) {
        return Err(NormalizeError::UnsupportedQuote(quote_raw));
    }
    if base_raw.is_empty() || base_raw.contains(['-', '_', ' ', '/']) {
        return Err(NormalizeError::InvalidBase(base_raw));
    }
    // Exchanges report bases in canonical case; the name is used byte for byte (non-ASCII tickers too).
    let resolved = aliases.resolve(venue.name(), &base_raw);
    Ok(Normalized {
        symbol: format!("{}-{}-PERP", resolved.base, quote),
        base: resolved.base,
        quote,
        multiplier: resolved.multiplier,
    })
}

fn parse_raw(venue: Venue, raw: &str) -> Result<(String, String), NormalizeError> {
    let unrecognized = || NormalizeError::UnrecognizedFormat {
        venue: venue.name(),
        raw: raw.to_string(),
    };
    match venue {
        Venue::Binance | Venue::Bybit | Venue::Bitmex => {
            for quote in QUOTES {
                if let Some(base) = raw.strip_suffix(quote) {
                    if base.is_empty() {
                        return Err(unrecognized());
                    }
                    return Ok((base.to_string(), quote.to_string()));
                }
            }
            Err(unrecognized())
        }
        Venue::Okx => {
            let parts: Vec<&str> = raw.split('-').collect();
            match parts.as_slice() {
                [base, quote, "SWAP"] if !base.is_empty() => {
                    Ok((base.to_string(), quote.to_string()))
                }
                _ => Err(unrecognized()),
            }
        }
        Venue::Deribit => {
            let Some(head) = raw.strip_suffix("-PERPETUAL") else {
                return Err(unrecognized());
            };
            match head.split_once('_') {
                Some((base, quote)) if !base.is_empty() => {
                    Ok((base.to_string(), quote.to_string()))
                }
                // `BTC-PERPETUAL`: no quote part, an inverse contract.
                None if !head.contains('-') && !head.is_empty() => {
                    Err(NormalizeError::Inverse(raw.to_string()))
                }
                _ => Err(unrecognized()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(venue: Venue, raw: &str) -> Result<String, NormalizeError> {
        normalize_symbol(venue, raw, None, &BaseAliases::builtin()).map(|n| n.symbol)
    }

    #[test]
    fn binance_and_bybit_concatenated() {
        assert_eq!(norm(Venue::Binance, "BTCUSDT").unwrap(), "BTC-USDT-PERP");
        assert_eq!(norm(Venue::Binance, "BTCUSDC").unwrap(), "BTC-USDC-PERP");
        assert_eq!(norm(Venue::Bybit, "ETHUSDT").unwrap(), "ETH-USDT-PERP");
        assert_eq!(
            norm(Venue::Binance, "1000PEPEUSDT").unwrap(),
            "PEPE-USDT-PERP"
        );
        assert_eq!(
            norm(Venue::Bybit, "1000PEPEUSDT").unwrap(),
            "PEPE-USDT-PERP"
        );
    }

    #[test]
    fn dated_and_unknown_quotes_are_rejected() {
        assert!(matches!(
            norm(Venue::Binance, "BTCUSDT_261225"),
            Err(NormalizeError::UnrecognizedFormat { .. })
        ));
        assert!(matches!(
            norm(Venue::Binance, "BTCBUSD"),
            Err(NormalizeError::UnrecognizedFormat { .. })
        ));
        assert!(matches!(
            norm(Venue::Binance, "USDT"),
            Err(NormalizeError::UnrecognizedFormat { .. })
        ));
        assert_eq!(norm(Venue::Binance, ""), Err(NormalizeError::Empty));
    }

    #[test]
    fn non_ascii_symbols_do_not_panic() {
        assert_eq!(
            norm(Venue::Binance, "币安人生USDT").unwrap(),
            "币安人生-USDT-PERP"
        );
        // Truncation in the middle of a code point would panic with byte slicing.
        assert!(norm(Venue::Binance, "币安").is_err());
        assert!(norm(Venue::Okx, "币-USDT-SWAP").is_ok());
        assert!(norm(Venue::Deribit, "币_USDC-PERPETUAL").is_ok());
        assert!(norm(Venue::Bitmex, "\u{1F600}").is_err());
    }

    #[test]
    fn okx_swap() {
        assert_eq!(norm(Venue::Okx, "BTC-USDT-SWAP").unwrap(), "BTC-USDT-PERP");
        assert_eq!(
            norm(Venue::Okx, "PEPE-USDC-SWAP").unwrap(),
            "PEPE-USDC-PERP"
        );
        assert!(matches!(
            norm(Venue::Okx, "BTC-USD-SWAP"),
            Err(NormalizeError::UnsupportedQuote(_))
        ));
        assert!(matches!(
            norm(Venue::Okx, "BTC-USD_UM-SWAP"),
            Err(NormalizeError::UnsupportedQuote(_))
        ));
        assert!(norm(Venue::Okx, "BTC-USDT-250328").is_err());
        assert!(norm(Venue::Okx, "BTC-USDT").is_err());
    }

    #[test]
    fn deribit_perpetual() {
        assert_eq!(
            norm(Venue::Deribit, "BTC_USDC-PERPETUAL").unwrap(),
            "BTC-USDC-PERP"
        );
        assert_eq!(
            norm(Venue::Deribit, "BTC-PERPETUAL"),
            Err(NormalizeError::Inverse("BTC-PERPETUAL".into()))
        );
        assert!(norm(Venue::Deribit, "BTC-27MAR26").is_err());
        assert!(norm(Venue::Deribit, "_USDC-PERPETUAL").is_err());
    }

    #[test]
    fn bitmex_xbt_alias() {
        assert_eq!(norm(Venue::Bitmex, "XBTUSDT").unwrap(), "BTC-USDT-PERP");
        assert_eq!(norm(Venue::Bitmex, "ETHUSDT").unwrap(), "ETH-USDT-PERP");
        // Not rewritten on other exchanges.
        assert_eq!(norm(Venue::Binance, "XBTUSDT").unwrap(), "XBT-USDT-PERP");
    }

    #[test]
    fn metadata_wins_over_parsing() {
        let aliases = BaseAliases::builtin();
        // Bybit USDC perpetuals are not named BASEQUOTE; metadata gives the answer.
        let n = normalize_symbol(Venue::Bybit, "BTCPERP", Some(("BTC", "USDC")), &aliases).unwrap();
        assert_eq!(n.symbol, "BTC-USDC-PERP");
        assert!(norm(Venue::Bybit, "BTCPERP").is_err());
        let n = normalize_symbol(
            Venue::Binance,
            "1000PEPEUSDT",
            Some(("1000PEPE", "USDT")),
            &aliases,
        )
        .unwrap();
        assert_eq!((n.base.as_str(), n.multiplier), ("PEPE", 1000.0));
        let n =
            normalize_symbol(Venue::Bitmex, "XBTUSDT", Some(("XBT", "USDT")), &aliases).unwrap();
        assert_eq!(n.symbol, "BTC-USDT-PERP");
        assert!(matches!(
            normalize_symbol(Venue::Bybit, "BTCUSD", Some(("BTC", "USD")), &aliases),
            Err(NormalizeError::UnsupportedQuote(_))
        ));
    }

    #[test]
    fn invalid_base_is_rejected() {
        let a = BaseAliases::builtin();
        for base in ["", "A-B", "A_B", "A B"] {
            assert!(matches!(
                normalize_symbol(Venue::Binance, "X", Some((base, "USDT")), &a),
                Err(NormalizeError::InvalidBase(_))
            ));
        }
    }

    #[test]
    fn venue_lookup() {
        assert_eq!(Venue::from_name("okx"), Some(Venue::Okx));
        assert_eq!(Venue::from_name("kraken"), None);
        assert_eq!(Venue::Deribit.name(), "deribit");
    }
}
