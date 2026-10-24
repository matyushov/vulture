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

/// Result of parsing OKX `GET /api/v5/public/instruments?instType=SWAP`.
#[derive(Debug, Clone, PartialEq)]
pub struct OkxInstruments {
    /// Swap instruments (unfiltered; unusable ones carry [`ContractKind::Other`]).
    pub metas: Vec<InstrumentMeta>,
    /// Contract size per `instId`, for the instruments that are usable.
    pub sizes: std::collections::HashMap<String, super::oi::ContractSize>,
    /// Instruments that look collectable but have an unusable contract size or names; they are dropped.
    pub unusable: Vec<String>,
}

#[derive(Deserialize)]
struct OkxEnvelope {
    code: String,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: Vec<OkxInstrument>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OkxInstrument {
    inst_id: String,
    #[serde(default)]
    inst_type: String,
    #[serde(default)]
    inst_family: String,
    #[serde(default)]
    ct_type: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    settle_ccy: String,
    #[serde(default)]
    ct_val: String,
    #[serde(default)]
    ct_mult: String,
    #[serde(default)]
    ct_val_ccy: String,
}

fn positive_number(text: &str) -> Option<f64> {
    text.parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && *v > 0.0)
}

/// Parses an OKX swap instrument list. A response code other than `"0"` is an error.
///
/// Only `ctType = linear` is a perpetual we collect; the USD-margined `*-USD_UM-SWAP` family (settled in USD
/// stablecoins) is excluded by decision, as are inverse contracts. Base and quote come from the `instId`
/// (`BASE-QUOTE-SWAP`) with `settleCcy` as the quote; the contract size (`ctVal x ctMult` in `ctValCcy`) must be
/// positive and expressed in the base coin, otherwise the instrument is reported as unusable.
pub fn parse_okx_instruments(json: &str) -> Result<OkxInstruments, String> {
    let env: OkxEnvelope = serde_json::from_str(json).map_err(|e| e.to_string())?;
    if env.code != "0" {
        return Err(format!("code {}: {}", env.code, env.msg));
    }
    let mut out = OkxInstruments {
        metas: Vec::new(),
        sizes: std::collections::HashMap::new(),
        unusable: Vec::new(),
    };
    for i in env.data {
        if !i.inst_type.is_empty() && i.inst_type != "SWAP" {
            continue;
        }
        let trading = i.state == "live";
        let parts: Vec<&str> = i.inst_id.split('-').collect();
        let base = match parts.as_slice() {
            [base, _, "SWAP"] if !base.is_empty() => (*base).to_string(),
            _ => {
                out.metas.push(InstrumentMeta {
                    raw: i.inst_id,
                    base: String::new(),
                    quote: i.settle_ccy,
                    kind: ContractKind::Other("unrecognized instId".into()),
                    trading,
                });
                continue;
            }
        };
        let mut kind = match i.ct_type.as_str() {
            "linear" => ContractKind::Perpetual,
            "inverse" => ContractKind::Other("inverse".into()),
            other => ContractKind::Other(other.to_string()),
        };
        if i.inst_family.ends_with("_UM") || i.inst_id.contains("_UM-") {
            kind = ContractKind::Other("USD_UM".into());
        }
        let wanted_shape = kind == ContractKind::Perpetual
            && trading
            && QUOTES.contains(&i.settle_ccy.as_str())
            && parts[1] == i.settle_ccy;
        if wanted_shape {
            let val = positive_number(&i.ct_val);
            let mult = positive_number(&i.ct_mult);
            match (val, mult) {
                (Some(ct_val), Some(ct_mult)) if i.ct_val_ccy.eq_ignore_ascii_case(&base) => {
                    out.sizes.insert(
                        i.inst_id.clone(),
                        super::oi::ContractSize {
                            ct_val,
                            ct_mult,
                            ct_val_ccy: i.ct_val_ccy.clone(),
                        },
                    );
                }
                _ => {
                    out.unusable.push(i.inst_id.clone());
                    kind = ContractKind::Other("unusable contract size".into());
                }
            }
        }
        out.metas.push(InstrumentMeta {
            raw: i.inst_id,
            base,
            quote: i.settle_ccy,
            kind,
            trading,
        });
    }
    out.unusable.sort();
    Ok(out)
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

    /// Shape of `GET /api/v5/public/instruments?instType=SWAP` (fields as documented 2026-10-24; values
    /// constructed, `ctVal` of PEPE per the research notes).
    const OKX_LIST: &str = r#"{"code":"0","msg":"","data":[
        {"instType":"SWAP","instId":"BTC-USDT-SWAP","instFamily":"BTC-USDT","ctType":"linear","state":"live","settleCcy":"USDT","ctVal":"0.01","ctMult":"1","ctValCcy":"BTC"},
        {"instType":"SWAP","instId":"BTC-USDC-SWAP","instFamily":"BTC-USDC","ctType":"linear","state":"live","settleCcy":"USDC","ctVal":"0.0001","ctMult":"1","ctValCcy":"BTC"},
        {"instType":"SWAP","instId":"PEPE-USDT-SWAP","instFamily":"PEPE-USDT","ctType":"linear","state":"live","settleCcy":"USDT","ctVal":"10000000","ctMult":"1","ctValCcy":"PEPE"},
        {"instType":"SWAP","instId":"BTC-USD-SWAP","instFamily":"BTC-USD","ctType":"inverse","state":"live","settleCcy":"BTC","ctVal":"100","ctMult":"1","ctValCcy":"USD"},
        {"instType":"SWAP","instId":"BTC-USD_UM-SWAP","instFamily":"BTC-USD_UM","ctType":"linear","state":"live","settleCcy":"USD","ctVal":"0.01","ctMult":"1","ctValCcy":"BTC"},
        {"instType":"SWAP","instId":"NEW-USDT-SWAP","instFamily":"NEW-USDT","ctType":"linear","state":"preopen","settleCcy":"USDT","ctVal":"1","ctMult":"1","ctValCcy":"NEW"},
        {"instType":"SWAP","instId":"ODD-USDT-SWAP","instFamily":"ODD-USDT","ctType":"linear","state":"live","settleCcy":"USDT","ctVal":"","ctMult":"1","ctValCcy":"ODD"},
        {"instType":"SWAP","instId":"WRONG-USDT-SWAP","instFamily":"WRONG-USDT","ctType":"linear","state":"live","settleCcy":"USDT","ctVal":"1","ctMult":"1","ctValCcy":"USD"}
        ]}"#;

    #[test]
    fn okx_selection_and_contract_sizes() {
        let list = parse_okx_instruments(OKX_LIST).unwrap();
        let wanted: Vec<&str> = list
            .metas
            .iter()
            .filter(|m| is_wanted_market(m))
            .map(|m| m.raw.as_str())
            .collect();
        assert_eq!(wanted, ["BTC-USDT-SWAP", "BTC-USDC-SWAP", "PEPE-USDT-SWAP"]);
        assert_eq!(list.unusable, ["ODD-USDT-SWAP", "WRONG-USDT-SWAP"]);
        let pepe = &list.sizes["PEPE-USDT-SWAP"];
        assert_eq!((pepe.ct_val, pepe.ct_mult), (10_000_000.0, 1.0));
        assert_eq!(list.sizes["BTC-USDC-SWAP"].ct_val_ccy, "BTC");
        assert!(!list.sizes.contains_key("BTC-USD_UM-SWAP"));
        assert!(!list.sizes.contains_key("BTC-USD-SWAP"));
        assert!(
            parse_okx_instruments(r#"{"code":"50011","msg":"Too many requests","data":[]}"#)
                .is_err()
        );
        assert!(parse_okx_instruments("nope").is_err());
    }

    #[test]
    fn okx_fixture_selection() {
        let json = include_str!("../../../../fixtures/okx/instruments_swap_doc.json");
        let list = parse_okx_instruments(json).unwrap();
        let wanted: Vec<&str> = list
            .metas
            .iter()
            .filter(|m| is_wanted_market(m))
            .map(|m| m.raw.as_str())
            .collect();
        assert_eq!(wanted, ["BTC-USDT-SWAP", "BTC-USDC-SWAP", "PEPE-USDT-SWAP"]);
        assert!(list.unusable.is_empty());
    }

    #[test]
    fn malformed_exchange_info_is_an_error() {
        assert!(parse_binance_exchange_info("{\"symbols\":[{\"symbol\":1}]}").is_err());
        assert!(parse_binance_exchange_info("not json").is_err());
    }
}
