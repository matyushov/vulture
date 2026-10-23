//! Generated code of the `vulture.v1` contract.

#[allow(clippy::all)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/vulture.v1.rs"));
}

#[cfg(test)]
mod tests {
    use super::v1::event::Payload;
    use super::v1::*;
    use prost::Message;

    fn round_trip<M: Message + Default + PartialEq + std::fmt::Debug>(msg: &M) {
        let bytes = msg.encode_to_vec();
        assert_eq!(&M::decode(bytes.as_slice()).expect("decode"), msg);
    }

    fn liquidation() -> Liquidation {
        Liquidation {
            exchange: "binance".into(),
            symbol_raw: "BTCUSDT".into(),
            symbol: "BTC-USDT-PERP".into(),
            exchange_ts: 1_700_000_000_123,
            recv_ts: 1_700_000_000_150,
            side: Side::Long as i32,
            price: 35_000.5,
            qty: 0.25,
            notional_usd: 8_750.125,
        }
    }

    fn open_interest() -> OpenInterest {
        OpenInterest {
            exchange: "bybit".into(),
            symbol_raw: "ETHUSDT".into(),
            symbol: "ETH-USDT-PERP".into(),
            exchange_ts: 1_700_000_001_000,
            recv_ts: 1_700_000_001_010,
            oi_base: 1_000.0,
            oi_usd: Some(2_000_000.0),
            mark_price: Some(2_000.0),
        }
    }

    fn gap() -> Gap {
        Gap {
            exchange: "okx".into(),
            stream: "liquidations".into(),
            from_ts: 1_700_000_002_000,
            to_ts: 1_700_000_005_000,
        }
    }

    fn event(payload: Payload) -> Event {
        Event {
            id: "1700000000123-0".into(),
            payload: Some(payload),
        }
    }

    #[test]
    fn messages_round_trip() {
        round_trip(&liquidation());
        round_trip(&open_interest());
        round_trip(&gap());
        round_trip(&event(Payload::Liquidation(liquidation())));
        round_trip(&event(Payload::OpenInterest(open_interest())));
        round_trip(&event(Payload::Gap(gap())));
        round_trip(&SubscribeRequest {
            exchanges: vec!["binance".into()],
            symbols: vec!["BTC-USDT-PERP".into()],
            types: vec![EventType::Liquidation as i32, EventType::Gap as i32],
            resume_from: "1700000000123-0".into(),
        });
        round_trip(&HistoryRequest {
            exchanges: vec!["okx".into()],
            symbols: vec![],
            types: vec![EventType::OpenInterest as i32],
            from_ts: 1,
            to_ts: 2,
            limit: 100,
            cursor: "1700000000123-0".into(),
        });
        round_trip(&AggregateRequest {
            group_by: GroupBy::BaseAsset as i32,
            step: Step::Day as i32,
            from_ts: 1,
            to_ts: 2,
            exchanges: vec![],
            symbols: vec![],
            base_assets: vec!["BTC".into()],
            metrics: vec![Metric::LiqSumUsd as i32, Metric::OiTotalUsd as i32],
        });
        round_trip(&AggregateResponse {
            rows: vec![AggregateRow {
                bucket_start: 1_700_000_000_000,
                group: "BTC".into(),
                values: vec![
                    MetricValue {
                        metric: Metric::LiqCount as i32,
                        value: Some(42.0),
                    },
                    MetricValue {
                        metric: Metric::LiqToOiRatio as i32,
                        value: None,
                    },
                ],
                partial: true,
            }],
            partial: true,
        });
        round_trip(&ListSubscriptionsResponse {
            subscriptions: vec![Subscription {
                exchange: "binance".into(),
                symbols: vec![],
                enabled: true,
            }],
        });
        round_trip(&GetConnectorStatusResponse {
            connectors: vec![ConnectorStatus {
                exchange: "binance".into(),
                state: ConnectorState::Running as i32,
                last_event_ts: 5,
                lag_ms: 7,
            }],
        });
        round_trip(&UpsertRuleRequest {
            rule: Some(Rule {
                id: String::new(),
                name: "big liq".into(),
                enabled: true,
                kind: Some(rule::Kind::Liquidation(LiquidationRule {
                    min_notional_usd: 1e6,
                })),
                exchanges: vec![],
                symbols: vec![],
                channel_ids: vec!["tg".into()],
                cooldown_seconds: 30,
            }),
        });
        round_trip(&UpsertChannelRequest {
            channel: Some(Channel {
                id: "tg".into(),
                name: "telegram".into(),
                kind: ChannelKind::Telegram as i32,
                enabled: true,
                system: false,
                secret_mask: String::new(),
            }),
            secret: "x".into(),
        });
        round_trip(&GetStorageStatusResponse {
            redis_used_bytes: 1,
            redis_max_bytes: 2,
            stream_lengths: [("liq".to_string(), 3u64)].into_iter().collect(),
            flusher_watermark: "1-0".into(),
            flusher_lag_ms: 4,
        });
        round_trip(&RotateTokenResponse {
            new_token: "t".into(),
        });
    }

    #[test]
    fn optional_oi_fields_keep_presence() {
        let mut oi = open_interest();
        oi.oi_usd = None;
        oi.mark_price = Some(0.0);
        let decoded = OpenInterest::decode(oi.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded.oi_usd, None);
        assert_eq!(decoded.mark_price, Some(0.0));
    }

    // Golden bytes: they pin the wire format. They must not be changed; a change means
    // a contract incompatibility.
    const GOLDEN_LIQUIDATION: &str = "0a0f313730303030303030303132332d30524c0a0762696e616e63651207425443555344541a0d4254432d555344542d5045525020fbd095ffbc312896d195ffbc31300139000000001017e14041000000000000d03f49000000001017c140";
    const GOLDEN_OI: &str = "0a0f313730303030303030303132332d305a480a0562796269741207455448555344541a0d4554482d555344542d5045525020e8d795ffbc3128f2d795ffbc31310000000000408f40390000000080843e41410000000000409f40";
    const GOLDEN_GAP: &str = "0a0f313730303030303030303132332d3062210a036f6b78120c6c69717569646174696f6e7318d0df95ffbc312088f795ffbc31";

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn check_golden(ev: Event, golden: &str) {
        assert_eq!(hex(&ev.encode_to_vec()), golden, "encoding changed");
        assert_eq!(Event::decode(unhex(golden).as_slice()).unwrap(), ev);
    }

    #[test]
    fn event_golden_liquidation() {
        check_golden(
            event(Payload::Liquidation(liquidation())),
            GOLDEN_LIQUIDATION,
        );
    }

    #[test]
    fn event_golden_open_interest() {
        check_golden(event(Payload::OpenInterest(open_interest())), GOLDEN_OI);
    }

    #[test]
    fn event_golden_gap() {
        check_golden(event(Payload::Gap(gap())), GOLDEN_GAP);
    }
}
