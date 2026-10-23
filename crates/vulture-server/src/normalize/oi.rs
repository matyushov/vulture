//! Open interest normalization: `oi_base` (real coins) and `oi_usd` (notional).
//!
//! The rules follow `research/exchanges.md`. All functions are pure. Inputs that are not finite or
//! negative are errors. A mark price that is not finite and positive counts as "not known": `oi_usd` stays
//! empty unless the exchange reports it itself. Cases where the unit is not established are errors, not
//! guesses.

/// Normalized open interest.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OiValues {
    /// Open interest in real base coins.
    pub oi_base: f64,
    /// Open interest in USD (quote notional), if known.
    pub oi_usd: Option<f64>,
}

/// Errors of OI normalization.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum OiError {
    /// A number is NaN, infinite or negative.
    #[error("{0} is not a finite non-negative number")]
    InvalidNumber(&'static str),
    /// A mark price is needed to convert and was not available.
    #[error("mark price is required for this conversion")]
    MissingMarkPrice,
    /// No way to obtain `oi_base` from the given fields without guessing.
    #[error("cannot determine the base-coin open interest: {0}")]
    UnknownUnit(&'static str),
}

fn check(name: &'static str, v: f64) -> Result<f64, OiError> {
    if v.is_finite() && v >= 0.0 {
        Ok(v)
    } else {
        Err(OiError::InvalidNumber(name))
    }
}

fn check_opt(name: &'static str, v: Option<f64>) -> Result<Option<f64>, OiError> {
    v.map(|x| check(name, x)).transpose()
}

fn mark(v: Option<f64>) -> Option<f64> {
    v.filter(|m| m.is_finite() && *m > 0.0)
}

/// Binance `openInterest`: base coin in exchange units (`1000PEPE` units for `1000PEPEUSDT`).
///
/// `multiplier` is real coins per exchange unit; `mark_price` is per exchange unit, as the exchange quotes it.
pub fn binance(
    open_interest: f64,
    mark_price: Option<f64>,
    multiplier: f64,
) -> Result<OiValues, OiError> {
    let oi = check("openInterest", open_interest)?;
    Ok(OiValues {
        oi_base: oi * check("multiplier", multiplier)?,
        oi_usd: mark(mark_price).map(|m| oi * m),
    })
}

/// Bybit linear ticker: `openInterest` is in exchange base units, `openInterestValue` in quote.
/// The exchange's own value wins; otherwise `openInterest x markPrice`.
pub fn bybit(
    open_interest: f64,
    open_interest_value: Option<f64>,
    mark_price: Option<f64>,
    multiplier: f64,
) -> Result<OiValues, OiError> {
    let oi = check("openInterest", open_interest)?;
    let value = check_opt("openInterestValue", open_interest_value)?;
    Ok(OiValues {
        oi_base: oi * check("multiplier", multiplier)?,
        oi_usd: value.or_else(|| mark(mark_price).map(|m| oi * m)),
    })
}

/// OKX contract size: `ctVal x ctMult` units of `ctValCcy` per contract.
#[derive(Debug, Clone, PartialEq)]
pub struct ContractSize {
    /// `ctVal`.
    pub ct_val: f64,
    /// `ctMult`.
    pub ct_mult: f64,
    /// `ctValCcy`.
    pub ct_val_ccy: String,
}

/// OKX `open-interest`: `oi` in contracts, `oiCcy` in real coins, `oiUsd` in USD.
///
/// `oiCcy` is used directly. Without it the contract size is required and its currency must equal `base`
/// (the real base asset); a size quoted in anything else is an error.
pub fn okx(
    oi_contracts: f64,
    oi_ccy: Option<f64>,
    oi_usd: Option<f64>,
    contract_size: Option<&ContractSize>,
    base: &str,
    mark_price: Option<f64>,
) -> Result<OiValues, OiError> {
    let contracts = check("oi", oi_contracts)?;
    let oi_base = match (check_opt("oiCcy", oi_ccy)?, contract_size) {
        (Some(ccy), _) => ccy,
        (None, Some(cs)) => {
            if !cs.ct_val_ccy.eq_ignore_ascii_case(base) {
                return Err(OiError::UnknownUnit("ctValCcy differs from the base asset"));
            }
            contracts * check("ctVal", cs.ct_val)? * check("ctMult", cs.ct_mult)?
        }
        (None, None) => return Err(OiError::UnknownUnit("neither oiCcy nor contract size")),
    };
    Ok(OiValues {
        oi_base,
        oi_usd: check_opt("oiUsd", oi_usd)?.or_else(|| mark(mark_price).map(|m| oi_base * m)),
    })
}

/// Deribit perpetual kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeribitContract {
    /// `BTC-PERPETUAL`: open interest in USD.
    Inverse,
    /// `BTC_USDC-PERPETUAL`: open interest in base coin.
    Linear,
}

/// Deribit ticker `open_interest`.
///
/// Inverse: USD, so `oi_base = oi / mark` (mark price required). Linear: base coin, `oi_usd = oi x mark`
/// when the mark price is known.
pub fn deribit(
    open_interest: f64,
    mark_price: Option<f64>,
    contract: DeribitContract,
) -> Result<OiValues, OiError> {
    let oi = check("open_interest", open_interest)?;
    let mark = mark(mark_price);
    match contract {
        DeribitContract::Inverse => {
            let m = mark.ok_or(OiError::MissingMarkPrice)?;
            Ok(OiValues {
                oi_base: oi / m,
                oi_usd: Some(oi),
            })
        }
        DeribitContract::Linear => Ok(OiValues {
            oi_base: oi,
            oi_usd: mark.map(|m| oi * m),
        }),
    }
}

// BitMEX is intentionally absent: the unit of `openInterest`/`openValue` is NOT CONFIRMED in
// `research/exchanges.md` (section 3.3), and the rule for unknown cases is an error, not a guess.

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn num(v: &Value) -> f64 {
        match v {
            Value::String(s) => s.parse().unwrap(),
            other => other.as_f64().unwrap(),
        }
    }

    fn approx(a: f64, b: f64) {
        assert!((a - b).abs() <= 1e-9 * b.abs().max(1.0), "{a} != {b}");
    }

    #[test]
    fn binance_fixture_values() {
        let json: Value = serde_json::from_str(include_str!(
            "../../../../fixtures/binance/open_interest_sample.json"
        ))
        .unwrap();
        let btc = &json[0];
        assert_eq!(btc["symbol"], "BTCUSDT");
        let oi = num(&btc["openInterest"]);
        approx(oi, 91955.828);
        let v = binance(oi, Some(100_000.0), 1.0).unwrap();
        approx(v.oi_base, 91955.828);
        approx(v.oi_usd.unwrap(), 9_195_582_800.0);
        // No mark price: no USD value.
        assert_eq!(binance(oi, None, 1.0).unwrap().oi_usd, None);
        assert_eq!(binance(oi, Some(0.0), 1.0).unwrap().oi_usd, None);
        assert_eq!(binance(oi, Some(f64::NAN), 1.0).unwrap().oi_usd, None);
    }

    #[test]
    fn binance_pseudo_coin_multiplier() {
        // 1000PEPE: 2_000_000 units are 2e9 PEPE; the mark price is per unit.
        let v = binance(2_000_000.0, Some(0.0123), 1000.0).unwrap();
        approx(v.oi_base, 2.0e9);
        approx(v.oi_usd.unwrap(), 24_600.0);
    }

    #[test]
    fn bybit_documentation_example() {
        let doc: Value = serde_json::from_str(include_str!(
            "../../../../fixtures/bybit/tickers_linear_snapshot_doc.json"
        ))
        .unwrap();
        let d = &doc["data"];
        let oi = num(&d["openInterest"]);
        let value = num(&d["openInterestValue"]);
        let mark_price = num(&d["markPrice"]);
        // The documentation's own numbers are consistent: openInterest x markPrice = openInterestValue.
        assert!((oi * mark_price - value).abs() < 1.0);
        let v = bybit(oi, Some(value), Some(mark_price), 1.0).unwrap();
        approx(v.oi_base, 492_373.72);
        approx(v.oi_usd.unwrap(), 32_824_881_841.75);
        // Without the exchange value it is derived from the mark price.
        let derived = bybit(oi, None, Some(mark_price), 1.0).unwrap();
        assert!((derived.oi_usd.unwrap() - value).abs() < 1.0);
        assert_eq!(bybit(oi, None, None, 1.0).unwrap().oi_usd, None);
    }

    #[test]
    fn bybit_pseudo_coin() {
        let v = bybit(500.0, Some(60.0), None, 1000.0).unwrap();
        approx(v.oi_base, 500_000.0);
        approx(v.oi_usd.unwrap(), 60.0);
    }

    #[test]
    fn okx_ccy_is_used_directly() {
        // Sample from the ccxt issue quoted in research/exchanges.md: 1000 NEIRO per contract.
        let v = okx(
            14_546_007.0,
            Some(14_546_007_000.0),
            Some(30_921_319.84),
            None,
            "NEIRO",
            None,
        )
        .unwrap();
        approx(v.oi_base, 14_546_007_000.0);
        approx(v.oi_usd.unwrap(), 30_921_319.84);
    }

    #[test]
    fn okx_falls_back_to_contract_size() {
        let cs = ContractSize {
            ct_val: 1000.0,
            ct_mult: 1.0,
            ct_val_ccy: "NEIRO".into(),
        };
        let v = okx(14_546_007.0, None, None, Some(&cs), "NEIRO", Some(0.002)).unwrap();
        approx(v.oi_base, 14_546_007_000.0);
        approx(v.oi_usd.unwrap(), 29_092_014.0);
        let wrong = ContractSize {
            ct_val_ccy: "USD".into(),
            ..cs.clone()
        };
        assert!(matches!(
            okx(1.0, None, None, Some(&wrong), "NEIRO", None),
            Err(OiError::UnknownUnit(_))
        ));
        assert!(matches!(
            okx(1.0, None, None, None, "NEIRO", None),
            Err(OiError::UnknownUnit(_))
        ));
    }

    #[test]
    fn deribit_doc_inverse_and_linear() {
        let doc: Value = serde_json::from_str(include_str!(
            "../../../../fixtures/deribit/ticker_btc_perpetual_doc.json"
        ))
        .unwrap();
        let d = &doc["data"];
        let oi = num(&d["open_interest"]);
        let mark_price = num(&d["mark_price"]);
        assert_eq!(d["instrument_name"], "BTC-PERPETUAL");
        let v = deribit(oi, Some(mark_price), DeribitContract::Inverse).unwrap();
        approx(v.oi_usd.unwrap(), 502_097_590.0);
        approx(v.oi_base, 502_097_590.0 / 36_446.51);
        assert_eq!(
            deribit(oi, None, DeribitContract::Inverse),
            Err(OiError::MissingMarkPrice)
        );
        let lin = deribit(1234.5, Some(50_000.0), DeribitContract::Linear).unwrap();
        approx(lin.oi_base, 1234.5);
        approx(lin.oi_usd.unwrap(), 61_725_000.0);
        assert_eq!(
            deribit(1.0, None, DeribitContract::Linear).unwrap().oi_usd,
            None
        );
    }

    #[test]
    fn invalid_numbers_are_errors() {
        assert!(matches!(
            binance(f64::NAN, None, 1.0),
            Err(OiError::InvalidNumber(_))
        ));
        assert!(matches!(
            binance(-1.0, None, 1.0),
            Err(OiError::InvalidNumber(_))
        ));
        assert!(matches!(
            bybit(1.0, Some(f64::INFINITY), None, 1.0),
            Err(OiError::InvalidNumber(_))
        ));
        assert!(matches!(
            deribit(f64::NEG_INFINITY, Some(1.0), DeribitContract::Linear),
            Err(OiError::InvalidNumber(_))
        ));
    }
}
