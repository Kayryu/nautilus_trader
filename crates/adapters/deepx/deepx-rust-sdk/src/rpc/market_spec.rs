// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Bounded decoding of DeepX market order constraints.

use subxt_core::{
    Metadata,
    ext::scale_value::{Composite, ValueDef, scale::decode_as_type},
};

use super::{precompile::decode_perp_market_return, spot_market_spec::DeepXSpotMarketSpec};

const MAX_MARKET_STORAGE_BYTES: usize = 16_384;

/// Raw perpetual order constraints shared by the precompile and runtime storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpOrderSpec {
    pub min_order_size: u128,
    pub tick_size: u128,
    pub step_size: u128,
}

/// Raw perpetual order constraints read from runtime storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpMarketSpec {
    pub order_spec: DeepXPerpOrderSpec,
    pub min_notional: u128,
}

/// Decodes a matching, positive Spot order specification from SCALE market storage.
#[must_use]
pub fn decode_spot_market_storage(
    encoded: &str,
    metadata: &Metadata,
    pair: [u8; 32],
) -> Option<DeepXSpotMarketSpec> {
    let value_type = metadata
        .pallet_by_name("SpotMarket")?
        .storage()?
        .entry_by_name("SpotMarkets")?
        .entry_type()
        .value_ty();
    let bytes = decode_market_storage_hex(encoded)?;
    let mut input = bytes.as_slice();
    let market = decode_as_type(&mut input, value_type, metadata.types()).ok()?;
    if !input.is_empty() {
        return None;
    }
    let ValueDef::Composite(Composite::Named(fields)) = &market.value else {
        return None;
    };
    let ValueDef::Composite(Composite::Unnamed(pair_wrapper)) =
        &fields.iter().find(|(name, _)| name == "pair")?.1.value
    else {
        return None;
    };
    let [wrapped_pair] = pair_wrapper.as_slice() else {
        return None;
    };
    let ValueDef::Composite(Composite::Unnamed(stored_pair)) = &wrapped_pair.value else {
        return None;
    };
    if stored_pair.len() != pair.len()
        || !stored_pair
            .iter()
            .zip(pair)
            .all(|(value, expected)| value.as_u128() == Some(u128::from(expected)))
    {
        return None;
    }
    let spec = fields.iter().find(|(name, _)| name == "order_spec")?;
    let min_order_size = named_scale_u128(&spec.1.value, "min_qty")?;
    let tick_size = named_scale_u128(&spec.1.value, "tick_size")?;
    let step_size = named_scale_u128(&spec.1.value, "step_size")?;
    (min_order_size > 0 && tick_size > 0 && step_size > 0).then_some(DeepXSpotMarketSpec {
        min_order_size,
        tick_size,
        step_size,
    })
}

/// Decodes a matching, positive perpetual order specification from an ABI hex response.
#[must_use]
pub fn decode_perp_order_spec(encoded: &str, market_id: u16) -> Option<DeepXPerpOrderSpec> {
    let bytes = encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())?;
    let market = decode_perp_market_return(&bytes)?;
    let spec = market.order_spec;
    (market.id == market_id && spec.min_order_size > 0 && spec.tick_size > 0 && spec.step_size > 0)
        .then_some(DeepXPerpOrderSpec {
            min_order_size: spec.min_order_size,
            tick_size: spec.tick_size,
            step_size: spec.step_size,
        })
}

/// Decodes a matching, positive perpetual specification from SCALE market storage.
#[must_use]
pub fn decode_perp_market_storage(
    encoded: &str,
    metadata: &Metadata,
    market_id: u16,
) -> Option<DeepXPerpMarketSpec> {
    let value_type = metadata
        .pallet_by_name("PerpMarket")?
        .storage()?
        .entry_by_name("PerpMarkets")?
        .entry_type()
        .value_ty();
    let bytes = decode_market_storage_hex(encoded)?;
    let mut input = bytes.as_slice();
    let market = decode_as_type(&mut input, value_type, metadata.types()).ok()?;
    if !input.is_empty() || named_scale_u128(&market.value, "id")? != u128::from(market_id) {
        return None;
    }
    let ValueDef::Composite(Composite::Named(fields)) = &market.value else {
        return None;
    };
    let spec = fields.iter().find(|(name, _)| name == "order_spec")?;
    let min_order_size = named_scale_u128(&spec.1.value, "min_qty")?;
    let tick_size = named_scale_u128(&spec.1.value, "tick_size")?;
    let step_size = named_scale_u128(&spec.1.value, "step_size")?;
    let min_notional = named_scale_u128(&spec.1.value, "min_notional")?;
    (min_order_size > 0 && tick_size > 0 && step_size > 0).then_some(DeepXPerpMarketSpec {
        order_spec: DeepXPerpOrderSpec {
            min_order_size,
            tick_size,
            step_size,
        },
        min_notional,
    })
}

fn decode_market_storage_hex(encoded: &str) -> Option<Vec<u8>> {
    if encoded.len() > 2 + 2 * MAX_MARKET_STORAGE_BYTES {
        return None;
    }
    encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())
}

fn named_scale_u128(value: &ValueDef<u32>, name: &str) -> Option<u128> {
    let ValueDef::Composite(Composite::Named(fields)) = value else {
        return None;
    };
    fields.iter().find(|(field, _)| field == name)?.1.as_u128()
}

#[cfg(test)]
mod tests {
    use alloy::{primitives::Bytes, sol_types::SolValue};
    use parity_scale_codec::DecodeAll;
    use rstest::rstest;
    use serde_json::Value;

    use super::*;
    use crate::rpc::precompile::{DeepXPerpMarketAbi, DeepXPerpOrderSpecAbi};

    const METADATA: &str = include_str!(
        "../../test_data/runtime/testnet/\
        genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json"
    );
    const SPOT_MARKET: &str = include_str!(
        "../../test_data/runtime/testnet/\
        genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/spot_market_eth_usdc.json"
    );

    fn metadata() -> Metadata {
        let response: Value = serde_json::from_str(METADATA).unwrap();
        let bytes = hex::decode(
            response["result"]
                .as_str()
                .unwrap()
                .strip_prefix("0x")
                .unwrap(),
        )
        .unwrap();
        Metadata::decode_all(&mut &bytes[..]).unwrap()
    }

    fn encoded_perp_market(spec: (u128, u128, u128)) -> String {
        let market = DeepXPerpMarketAbi {
            id: 3,
            name: Bytes::from_static(b"BTC"),
            base_symbol: Bytes::from_static(b"BTC"),
            base_decimal: 8,
            quote_market_id: 0,
            network: Bytes::from_static(b"bitcoin"),
            height: 1,
            funding_rate: 0,
            last_cacl_funding_rate_time: 0,
            oracle_price: 1,
            mark_price: 1,
            max_deviation_bps: 0,
            maintenance_margin_ratio: 1,
            taker_fee_rate: 0,
            maker_fee_rate: 0,
            order_spec: DeepXPerpOrderSpecAbi {
                min_order_size: spec.0,
                tick_size: spec.1,
                step_size: spec.2,
            },
            open_interest: 0,
            long_open_pos_num: 0,
            short_open_pos_num: 0,
            base_interest_rate: 0,
            impact_margin_value: 0,
            funding_rate_change_cap: 0,
            funding_rate_change_floor: 0,
            cumulative_funding_index: 0,
        };
        format!("0x{}", hex::encode(market.abi_encode()))
    }

    fn encoded_perp_market_storage() -> String {
        use subxt_core::ext::scale_value::Value as ScaleValue;

        let fixed_i128 = || ScaleValue::unnamed_composite([ScaleValue::i128(0)]);
        let market = ScaleValue::named_composite([
            ("id", ScaleValue::u128(3)),
            ("name", ScaleValue::from_bytes([])),
            ("base_symbol", ScaleValue::from_bytes([])),
            ("base_address", ScaleValue::from_bytes([0; 20])),
            ("base_decimal", ScaleValue::i128(8)),
            ("quote_market_id", ScaleValue::u128(0)),
            ("quote_symbol", ScaleValue::from_bytes([])),
            ("quote_address", ScaleValue::from_bytes([0; 20])),
            ("quote_decimal", ScaleValue::i128(6)),
            ("network", ScaleValue::from_bytes([])),
            ("height", ScaleValue::u128(1)),
            ("funding_rate", ScaleValue::i128(0)),
            ("cumulative_funding_index", ScaleValue::i128(0)),
            ("last_funding_rate_time", ScaleValue::u128(0)),
            ("last_cacl_funding_rate_time", ScaleValue::u128(0)),
            ("oracle_price", ScaleValue::u128(1)),
            ("mark_price", ScaleValue::u128(1)),
            ("max_deviation_bps", ScaleValue::u128(0)),
            ("initial_margin_ratio", ScaleValue::u128(1)),
            ("maintenance_margin_ratio", ScaleValue::u128(1)),
            ("max_active_orders", ScaleValue::u128(1)),
            ("is_quote_market", ScaleValue::bool(false)),
            ("taker_fee_rate", ScaleValue::u128(0)),
            ("maker_fee_rate", ScaleValue::i128(0)),
            (
                "order_spec",
                ScaleValue::named_composite([
                    ("min_qty", ScaleValue::u128(11)),
                    ("tick_size", ScaleValue::u128(2)),
                    ("step_size", ScaleValue::u128(3)),
                    ("min_notional", ScaleValue::u128(50)),
                ]),
            ),
            (
                "limit_order_guard",
                ScaleValue::named_composite([
                    ("limit_long", ScaleValue::u128(1)),
                    ("limit_short", ScaleValue::u128(1)),
                ]),
            ),
            ("open_interest", ScaleValue::u128(0)),
            ("long_open_pos_num", ScaleValue::u128(0)),
            ("short_open_pos_num", ScaleValue::u128(0)),
            ("base_interest_rate", fixed_i128()),
            ("impact_margin_value", ScaleValue::u128(0)),
            ("funding_rate_change_cap", fixed_i128()),
            ("funding_rate_change_floor", fixed_i128()),
            ("funding_rate_clamp_upper_bound", fixed_i128()),
            ("funding_rate_clamp_lower_bound", fixed_i128()),
            (
                "liquidation_spec",
                ScaleValue::named_composite([
                    ("liquidation_duration", ScaleValue::u128(0)),
                    ("liquidity_bucket_slippage_step", ScaleValue::u128(0)),
                    ("liquidity_bucket_slippage_limit", ScaleValue::u128(0)),
                    ("liquidity_dust_value", ScaleValue::u128(0)),
                    (
                        "liquidation_fee_rate",
                        ScaleValue::unnamed_composite([
                            ScaleValue::u128(0),
                            ScaleValue::u128(0),
                            ScaleValue::u128(0),
                            ScaleValue::u128(0),
                        ]),
                    ),
                ]),
            ),
            ("deployer_spec", ScaleValue::unnamed_variant("None", [])),
            ("is_paused", ScaleValue::bool(false)),
        ]);
        let metadata = metadata();
        let value_type = metadata
            .pallet_by_name("PerpMarket")
            .unwrap()
            .storage()
            .unwrap()
            .entry_by_name("PerpMarkets")
            .unwrap()
            .entry_type()
            .value_ty();
        let mut bytes = Vec::new();
        subxt_core::ext::scale_value::scale::encode_as_type(
            &market,
            value_type,
            metadata.types(),
            &mut bytes,
        )
        .unwrap();
        format!("0x{}", hex::encode(bytes))
    }

    #[rstest]
    fn decodes_only_matching_positive_perp_order_specs() {
        let encoded = encoded_perp_market((11, 2, 3));
        assert_eq!(
            decode_perp_order_spec(&encoded, 3),
            Some(DeepXPerpOrderSpec {
                min_order_size: 11,
                tick_size: 2,
                step_size: 3,
            })
        );
        assert_eq!(decode_perp_order_spec(&encoded, 4), None);
        assert_eq!(decode_perp_order_spec("0x", 3), None);
        assert_eq!(decode_perp_order_spec("0xzz", 3), None);
        for spec in [(0, 2, 3), (11, 0, 3), (11, 2, 0)] {
            assert_eq!(decode_perp_order_spec(&encoded_perp_market(spec), 3), None);
        }
        assert_eq!(decode_perp_order_spec(&format!("{encoded}00"), 3), None);
    }

    #[rstest]
    fn decodes_only_matching_complete_perp_market_storage() {
        let metadata = metadata();
        let encoded = encoded_perp_market_storage();
        assert_eq!(
            decode_perp_market_storage(&encoded, &metadata, 3),
            Some(DeepXPerpMarketSpec {
                order_spec: DeepXPerpOrderSpec {
                    min_order_size: 11,
                    tick_size: 2,
                    step_size: 3,
                },
                min_notional: 50,
            }),
        );
        assert_eq!(decode_perp_market_storage(&encoded, &metadata, 4), None);
        assert_eq!(
            decode_perp_market_storage(&format!("{encoded}00"), &metadata, 3),
            None,
        );
        assert_eq!(decode_perp_market_storage("0x", &metadata, 3), None);
    }

    #[rstest]
    fn decodes_only_matching_complete_spot_market_storage() {
        let metadata = metadata();
        let storage: Value = serde_json::from_str(SPOT_MARKET).unwrap();
        let encoded = storage["result"].as_str().unwrap();
        let pair = hex::decode("9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37")
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(
            decode_spot_market_storage(encoded, &metadata, pair),
            Some(DeepXSpotMarketSpec {
                min_order_size: 100_000_000_000_000,
                tick_size: 10_000,
                step_size: 100_000_000_000_000,
            })
        );
        for encoded in [
            "0x".to_string(),
            format!("{encoded}00"),
            encoded.replace("00407a10f35a00000000000000000000", &"00".repeat(16)),
        ] {
            assert_eq!(decode_spot_market_storage(&encoded, &metadata, pair), None);
        }
        assert_eq!(
            decode_spot_market_storage(encoded, &metadata, [0x33; 32]),
            None
        );
    }
}
