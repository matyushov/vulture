//! Normalization against the recorded and documentation fixtures in `fixtures/`.

use serde_json::Value;
use vulture_server::normalize::{
    BaseAliases, NormalizeError, Venue, is_wanted_market, normalize_instrument, normalize_symbol,
    parse_binance_exchange_info,
};

fn fixture(path: &str) -> String {
    let full = format!("{}/../../fixtures/{path}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("{full}: {e}"))
}

#[test]
fn binance_exchange_info_selection_and_normalization() {
    let aliases = BaseAliases::builtin();
    let metas = parse_binance_exchange_info(&fixture("binance/exchange_info_sample.json")).unwrap();
    let normalized: Vec<(String, String, f64)> = metas
        .iter()
        .filter(|m| is_wanted_market(m))
        .map(|m| {
            let n = normalize_instrument(Venue::Binance, m, &aliases).unwrap();
            (m.raw.clone(), n.symbol, n.multiplier)
        })
        .collect();
    assert_eq!(
        normalized,
        [
            ("BTCUSDT".to_string(), "BTC-USDT-PERP".to_string(), 1.0),
            ("ETHUSDT".to_string(), "ETH-USDT-PERP".to_string(), 1.0),
            ("BTCUSDC".to_string(), "BTC-USDC-PERP".to_string(), 1.0),
            (
                "1000PEPEUSDT".to_string(),
                "PEPE-USDT-PERP".to_string(),
                1000.0
            ),
            (
                "币安人生USDT".to_string(),
                "币安人生-USDT-PERP".to_string(),
                1.0
            ),
        ]
    );
}

#[test]
fn binance_tradifi_and_dated_contracts_are_not_collected() {
    let metas = parse_binance_exchange_info(&fixture("binance/exchange_info_sample.json")).unwrap();
    for raw in ["XAUUSDT", "BTCUSDT_261225", "OMGUSDT", "GAIBUSDT"] {
        let m = metas.iter().find(|m| m.raw == raw).unwrap();
        assert!(!is_wanted_market(m), "{raw} must be filtered out");
    }
}

#[test]
fn binance_oi_and_liquidation_symbols_normalize_without_metadata() {
    let aliases = BaseAliases::builtin();
    let oi: Value = serde_json::from_str(&fixture("binance/open_interest_sample.json")).unwrap();
    let mut seen = 0;
    for row in oi.as_array().unwrap() {
        let raw = row["symbol"].as_str().unwrap();
        let n = normalize_symbol(Venue::Binance, raw, None, &aliases)
            .unwrap_or_else(|e| panic!("{raw}: {e}"));
        assert!(n.symbol.ends_with("-PERP"));
        seen += 1;
    }
    assert!(seen >= 4);

    let mut events = 0;
    for line in fixture("binance/force_order_sample.jsonl").lines() {
        let v: Value = serde_json::from_str(line).unwrap();
        let raw = v["msg"]["o"]["s"].as_str().unwrap();
        let n = normalize_symbol(Venue::Binance, raw, None, &aliases)
            .unwrap_or_else(|e| panic!("{raw}: {e}"));
        assert!(["USDT", "USDC"].contains(&n.quote.as_str()));
        events += 1;
    }
    assert!(events > 0);
}

#[test]
fn bybit_documentation_symbols() {
    let aliases = BaseAliases::builtin();
    let liq: Value = serde_json::from_str(&fixture("bybit/all_liquidation_doc.json")).unwrap();
    let raw = liq["data"][0]["s"].as_str().unwrap();
    assert_eq!(
        normalize_symbol(Venue::Bybit, raw, None, &aliases)
            .unwrap()
            .symbol,
        "ROSE-USDT-PERP"
    );
    let ticker: Value =
        serde_json::from_str(&fixture("bybit/tickers_linear_snapshot_doc.json")).unwrap();
    let raw = ticker["data"]["symbol"].as_str().unwrap();
    assert_eq!(
        normalize_symbol(Venue::Bybit, raw, None, &aliases)
            .unwrap()
            .symbol,
        "BTC-USDT-PERP"
    );
}

#[test]
fn deribit_documentation_instruments() {
    let aliases = BaseAliases::builtin();
    let doc: Value = serde_json::from_str(&fixture("deribit/get_instruments_doc.json")).unwrap();
    let name = doc["result"][0]["instrument_name"].as_str().unwrap();
    // The documented BTC-PERPETUAL is inverse: explicit error, not a guess.
    assert_eq!(
        normalize_symbol(Venue::Deribit, name, None, &aliases),
        Err(NormalizeError::Inverse("BTC-PERPETUAL".into()))
    );
    assert_eq!(
        normalize_symbol(Venue::Deribit, "BTC_USDC-PERPETUAL", None, &aliases)
            .unwrap()
            .symbol,
        "BTC-USDC-PERP"
    );
}
