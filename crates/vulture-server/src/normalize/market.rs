//! Which instruments are collected: linear USDT/USDC perpetuals in trading state.
//!
//! Binance `TRADIFI_PERPETUAL` (tokenized stocks, metals) is deliberately excluded (owner decision
//! 2026-10-23); dated futures, settling and not yet trading contracts are excluded as well.

use serde::Deserialize;

use super::symbol::QUOTES;

/// Contract kind as far as the filter cares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractKind {
    /// Regular perpetual.
    Perpetual,
    /// Binance `TRADIFI_PERPETUAL`; not collected.
    TradFiPerpetual,
    /// Quarterly and other dated futures.
    Dated,
    /// Anything else, with the exchange's own label.
    Other(String),
}

impl ContractKind {
    /// Maps a Binance `contractType`.
    pub fn from_binance(contract_type: &str) -> Self {
        match contract_type {
            "PERPETUAL" => Self::Perpetual,
            "TRADIFI_PERPETUAL" => Self::TradFiPerpetual,
            "CURRENT_QUARTER"
            | "NEXT_QUARTER"
            | "CURRENT_MONTH"
            | "NEXT_MONTH"
            | "CURRENT_QUARTER DELIVERING"
            | "NEXT_QUARTER DELIVERING" => Self::Dated,
            other => Self::Other(other.to_string()),
        }
    }
}

/// Instrument metadata as reported by an exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentMeta {
    /// Raw symbol.
    pub raw: String,
    /// Base asset as the exchange names it.
    pub base: String,
    /// Quote asset.
    pub quote: String,
    /// Contract kind.
    pub kind: ContractKind,
    /// Whether the contract is currently trading (not settling or pending).
    pub trading: bool,
}

/// The collection filter.
pub fn is_wanted_market(meta: &InstrumentMeta) -> bool {
    meta.kind == ContractKind::Perpetual && meta.trading && QUOTES.contains(&meta.quote.as_str())
}

#[derive(Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceSymbol>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceSymbol {
    symbol: String,
    contract_type: String,
    status: String,
    base_asset: String,
    quote_asset: String,
}

/// Parses a Binance USDT-M `exchangeInfo` response into metadata (unfiltered).
pub fn parse_binance_exchange_info(json: &str) -> Result<Vec<InstrumentMeta>, serde_json::Error> {
    let info: BinanceExchangeInfo = serde_json::from_str(json)?;
    Ok(info
        .symbols
        .into_iter()
        .map(|s| InstrumentMeta {
            raw: s.symbol,
            base: s.base_asset,
            quote: s.quote_asset,
            kind: ContractKind::from_binance(&s.contract_type),
            trading: s.status == "TRADING",
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(kind: ContractKind, quote: &str, trading: bool) -> InstrumentMeta {
        InstrumentMeta {
            raw: "X".into(),
            base: "X".into(),
            quote: quote.into(),
            kind,
            trading,
        }
    }

    #[test]
    fn filter_rules() {
        assert!(is_wanted_market(&meta(
            ContractKind::Perpetual,
            "USDT",
            true
        )));
        assert!(is_wanted_market(&meta(
            ContractKind::Perpetual,
            "USDC",
            true
        )));
        assert!(!is_wanted_market(&meta(
            ContractKind::Perpetual,
            "USDT",
            false
        )));
        assert!(!is_wanted_market(&meta(
            ContractKind::Perpetual,
            "USD1",
            true
        )));
        assert!(!is_wanted_market(&meta(
            ContractKind::TradFiPerpetual,
            "USDT",
            true
        )));
        assert!(!is_wanted_market(&meta(ContractKind::Dated, "USDT", true)));
        assert!(!is_wanted_market(&meta(
            ContractKind::Other("DELIVERING".into()),
            "USDT",
            true
        )));
    }

    #[test]
    fn binance_fixture_selection() {
        let json = include_str!("../../../../fixtures/binance/exchange_info_sample.json");
        let metas = parse_binance_exchange_info(json).unwrap();
        assert_eq!(metas.len(), 9);
        let wanted: Vec<&str> = metas
            .iter()
            .filter(|m| is_wanted_market(m))
            .map(|m| m.raw.as_str())
            .collect();
        // Excluded: quarterly, TradFi, SETTLING and PENDING_TRADING.
        assert_eq!(
            wanted,
            [
                "BTCUSDT",
                "ETHUSDT",
                "BTCUSDC",
                "1000PEPEUSDT",
                "币安人生USDT"
            ]
        );
    }

    #[test]
    fn malformed_exchange_info_is_an_error() {
        assert!(parse_binance_exchange_info("{\"symbols\":[{\"symbol\":1}]}").is_err());
        assert!(parse_binance_exchange_info("not json").is_err());
    }
}
