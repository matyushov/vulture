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

impl ContractKind {
    /// Maps a Bybit `contractType` (`LinearPerpetual`, `LinearFutures`, `InversePerpetual`, ...).
    pub fn from_bybit(contract_type: &str) -> Self {
        match contract_type {
            "LinearPerpetual" => Self::Perpetual,
            "LinearFutures" | "InverseFutures" => Self::Dated,
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

/// One page of Bybit `GET /v5/market/instruments-info`.
#[derive(Debug, Clone, PartialEq)]
pub struct BybitInstrumentsPage {
    /// Instruments of the page (unfiltered).
    pub metas: Vec<InstrumentMeta>,
    /// Cursor of the next page; `None` on the last one.
    pub next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct BybitEnvelope {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg", default)]
    ret_msg: String,
    #[serde(default)]
    result: Option<BybitResult>,
}

#[derive(Deserialize)]
struct BybitResult {
    #[serde(default)]
    list: Vec<BybitInstrument>,
    #[serde(rename = "nextPageCursor", default)]
    next_page_cursor: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrument {
    symbol: String,
    #[serde(default)]
    contract_type: String,
    #[serde(default)]
    status: String,
    base_coin: String,
    quote_coin: String,
}

/// Parses a page of Bybit `instruments-info`. A non-zero `retCode` is an error.
pub fn parse_bybit_instruments(json: &str) -> Result<BybitInstrumentsPage, String> {
    let env: BybitEnvelope = serde_json::from_str(json).map_err(|e| e.to_string())?;
    if env.ret_code != 0 {
        return Err(format!("retCode {}: {}", env.ret_code, env.ret_msg));
    }
    let result = env.result.ok_or("response has no result")?;
    Ok(BybitInstrumentsPage {
        metas: result
            .list
            .into_iter()
            .map(|i| InstrumentMeta {
                raw: i.symbol,
                base: i.base_coin,
                quote: i.quote_coin,
                kind: ContractKind::from_bybit(&i.contract_type),
                trading: i.status == "Trading",
            })
            .collect(),
        next_cursor: Some(result.next_page_cursor).filter(|c| !c.is_empty()),
    })
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

    /// Shape of a live `instruments-info` page (first item as received on 2026-10-24, trimmed).
    const BYBIT_PAGE: &str = r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[
        {"symbol":"0GUSDT","contractType":"LinearPerpetual","status":"Trading","baseCoin":"0G","quoteCoin":"USDT","launchTime":"1758175736000","settleCoin":"USDT","lotSizeFilter":{"qtyStep":"0.1"}},
        {"symbol":"1000BONKPERP","contractType":"LinearPerpetual","status":"Trading","baseCoin":"1000BONK","quoteCoin":"USDC","settleCoin":"USDC"},
        {"symbol":"BTC-27MAR26","contractType":"LinearFutures","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT"},
        {"symbol":"NEWUSDT","contractType":"LinearPerpetual","status":"PreLaunch","baseCoin":"NEW","quoteCoin":"USDT"}
        ],"nextPageCursor":"first%3D0GUSDT%26last%3DMETISUSDT"},"retExtInfo":{},"time":1792851150134}"#;

    #[test]
    fn bybit_page_selection_and_cursor() {
        let page = parse_bybit_instruments(BYBIT_PAGE).unwrap();
        assert_eq!(
            page.next_cursor.as_deref(),
            Some("first%3D0GUSDT%26last%3DMETISUSDT")
        );
        let wanted: Vec<&str> = page
            .metas
            .iter()
            .filter(|m| is_wanted_market(m))
            .map(|m| m.raw.as_str())
            .collect();
        assert_eq!(wanted, ["0GUSDT", "1000BONKPERP"]);
        let usdc = &page.metas[1];
        assert_eq!(
            (usdc.base.as_str(), usdc.quote.as_str()),
            ("1000BONK", "USDC")
        );
        let last = BYBIT_PAGE.replace("first%3D0GUSDT%26last%3DMETISUSDT", "");
        assert!(
            parse_bybit_instruments(&last)
                .unwrap()
                .next_cursor
                .is_none()
        );
        assert!(parse_bybit_instruments(r#"{"retCode":10001,"retMsg":"bad"}"#).is_err());
        assert!(parse_bybit_instruments("nope").is_err());
    }

    #[test]
    fn malformed_exchange_info_is_an_error() {
        assert!(parse_binance_exchange_info("{\"symbols\":[{\"symbol\":1}]}").is_err());
        assert!(parse_binance_exchange_info("not json").is_err());
    }
}
