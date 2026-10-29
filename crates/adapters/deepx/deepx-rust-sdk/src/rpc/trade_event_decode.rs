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

//! Decoding complete finalized perpetual and Spot trade event storage.

use parity_scale_codec::{Compact, Decode};
use subxt_core::{
    events::Events,
    ext::scale_value::{Composite, Value as ScaleValue, ValueDef},
    metadata::Metadata,
};
use thiserror::Error;

use super::{
    event_decode::{decode_event_bytes, decode_unit_variant},
    events::{DeepXFinalizedPerpTrade, DeepXFinalizedSpotTrade},
};
use crate::signing::{
    DeepXRuntimeConfig,
    runtime_interface::{DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError},
};

/// Failures decoding complete finalized perpetual trade events.
#[derive(Debug, Error)]
pub enum DeepXPerpTradeEventError {
    /// The approved runtime does not expose the trade event.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE event decoding failed against approved metadata.
    #[error("unable to decode DeepX perpetual trade events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The event sequence was truncated or contained trailing bytes.
    #[error("DeepX perpetual trade evidence is not complete System.Events")]
    MalformedEventBytes,
    /// A trade event violates the runtime field or block identity contract.
    #[error("invalid DeepX finalized perpetual trade event at index {0}")]
    InvalidTrade(u32),
}

/// Failures decoding complete Spot trade event storage.
#[derive(Debug, Error)]
pub enum DeepXSpotTradeEventError {
    /// The approved runtime does not expose the Spot match event.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE decoding failed against approved metadata.
    #[error("unable to decode DeepX Spot trade events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The event sequence was truncated or contained trailing bytes.
    #[error("DeepX Spot trade evidence is not complete System.Events")]
    MalformedEventBytes,
    /// An event violates the approved runtime field contract.
    #[error("invalid DeepX finalized Spot trade event at index {0}")]
    InvalidTrade(u32),
}

/// Decodes perpetual trades from one complete finalized `System.Events` value.
///
/// The caller establishes canonical finality for the supplied block hash separately.
///
/// # Errors
///
/// Returns an error for unsupported metadata, incomplete SCALE bytes, or invalid trade fields.
pub fn decode_finalized_perp_trade_events(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedPerpTrade>, DeepXPerpTradeEventError> {
    interfaces.event("PerpMarket", "NewTrade")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXPerpTradeEventError::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut trades = Vec::new();
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    for event in events.iter() {
        let event = event.map_err(|e| DeepXPerpTradeEventError::Decode(Box::new(e)))?;
        let event_index = count;
        count = count
            .checked_add(1)
            .ok_or(DeepXPerpTradeEventError::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(DeepXPerpTradeEventError::MalformedEventBytes)?;
        if event.pallet_name() != "PerpMarket" || event.variant_name() != "NewTrade" {
            continue;
        }
        let fields = event
            .field_values()
            .map_err(|e| DeepXPerpTradeEventError::Decode(Box::new(e)))?;
        let trade =
            parse_finalized_perp_trade_fields(&fields, block_hash, block_number, event_index)
                .ok_or(DeepXPerpTradeEventError::InvalidTrade(event_index))?;
        trades.push(trade);
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(DeepXPerpTradeEventError::MalformedEventBytes);
    }
    Ok(trades)
}

/// Decodes Spot matches from one complete finalized `System.Events` value.
///
/// The caller establishes canonical finality for the supplied block hash separately.
///
/// # Errors
///
/// Returns an error for unsupported metadata, incomplete SCALE bytes, or invalid trade fields.
pub fn decode_finalized_spot_trade_events(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedSpotTrade>, DeepXSpotTradeEventError> {
    interfaces.event("SpotMarket", "OrderMatched")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXSpotTradeEventError::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut trades = Vec::new();
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    for event in events.iter() {
        let event = event.map_err(|e| DeepXSpotTradeEventError::Decode(Box::new(e)))?;
        let event_index = count;
        count = count
            .checked_add(1)
            .ok_or(DeepXSpotTradeEventError::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(DeepXSpotTradeEventError::MalformedEventBytes)?;
        if event.pallet_name() != "SpotMarket" || event.variant_name() != "OrderMatched" {
            continue;
        }
        let fields = event
            .field_values()
            .map_err(|e| DeepXSpotTradeEventError::Decode(Box::new(e)))?;
        let trade =
            parse_finalized_spot_trade_fields(&fields, block_hash, block_number, event_index)
                .ok_or(DeepXSpotTradeEventError::InvalidTrade(event_index))?;
        trades.push(trade);
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(DeepXSpotTradeEventError::MalformedEventBytes);
    }
    Ok(trades)
}

fn parse_finalized_perp_trade_fields(
    fields: &Composite<u32>,
    block_hash: [u8; 32],
    block_number: u64,
    event_index: u32,
) -> Option<DeepXFinalizedPerpTrade> {
    let Composite::Named(fields) = fields else {
        return None;
    };
    if fields.len() != 6 {
        return None;
    }
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    let ScaleValue {
        value: ValueDef::Composite(Composite::Named(trade)),
        ..
    } = field("trade")?
    else {
        return None;
    };
    if trade.len() != 11 {
        return None;
    }
    let trade_field = |name| {
        trade
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    let market_id = u16::try_from(trade_field("market_id")?.as_u128()?).ok()?;
    let buyer_order_id = u64::try_from(trade_field("buyer_order_id")?.as_u128()?).ok()?;
    let seller_order_id = u64::try_from(trade_field("seller_order_id")?.as_u128()?).ok()?;
    let buyer = decode_event_bytes(trade_field("buyer")?)?.try_into().ok()?;
    let seller = decode_event_bytes(trade_field("seller")?)?
        .try_into()
        .ok()?;
    let price = trade_field("price")?.as_u128()?;
    let size = trade_field("size")?.as_u128()?;
    let buyer_leverage = u64::try_from(trade_field("buyer_leverage")?.as_u128()?).ok()?;
    let seller_leverage = u64::try_from(trade_field("seller_leverage")?.as_u128()?).ok()?;
    let created_at = u64::try_from(trade_field("created_at")?.as_u128()?).ok()?;
    let filled_direction =
        decode_unit_variant(trade_field("filled_direction")?, &["Long", "Short", "Both"])?;
    let taker = decode_unit_variant(field("taker")?, &["Buyer", "Seller"])?;
    let taker_fee = field("taker_fee")?.as_i128()?;
    let maker_fee = field("maker_fee")?.as_i128()?;
    let fee_asset = decode_event_bytes(field("taker_fee_asset")?)?;
    if market_id == 0
        || price == 0
        || size == 0
        || buyer_leverage == 0
        || seller_leverage == 0
        || created_at != block_number
        || fee_asset.is_empty()
        || decode_event_bytes(field("maker_fee_asset")?)? != fee_asset
    {
        return None;
    }
    Some(DeepXFinalizedPerpTrade {
        block_hash,
        block_number,
        event_index,
        market_id,
        buyer_order_id,
        buyer,
        seller_order_id,
        seller,
        price,
        size,
        buyer_leverage,
        seller_leverage,
        filled_direction: filled_direction.to_string(),
        taker: taker.to_string(),
        taker_fee,
        maker_fee,
        fee_asset,
    })
}

fn parse_finalized_spot_trade_fields(
    fields: &Composite<u32>,
    block_hash: [u8; 32],
    block_number: u64,
    event_index: u32,
) -> Option<DeepXFinalizedSpotTrade> {
    let Composite::Named(fields) = fields else {
        return None;
    };
    if fields.len() != 13 {
        return None;
    }
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    let pair = decode_event_bytes(field("pair")?)?.try_into().ok()?;
    let buyer = decode_event_bytes(field("buyer")?)?.try_into().ok()?;
    let seller = decode_event_bytes(field("seller")?)?.try_into().ok()?;
    let buy_id = u64::try_from(field("buy_id")?.as_u128()?).ok()?;
    let sell_id = u64::try_from(field("sell_id")?.as_u128()?).ok()?;
    let trade_amount_u = decode_event_u256_le(field("trade_amount_u")?)?;
    let trade_amount_b = decode_event_u256_le(field("trade_amount_b")?)?;
    let price = decode_event_u256_le(field("price")?)?;
    let taker = decode_unit_variant(field("taker")?, &["Buyer", "Seller"])?;
    let taker_fee = field("taker_fee")?.as_i128()?;
    let maker_fee = field("maker_fee")?.as_i128()?;
    let taker_fee_asset = decode_event_bytes(field("taker_fee_asset")?)?;
    let maker_fee_asset = decode_event_bytes(field("maker_fee_asset")?)?;
    if pair == [0; 32]
        || buyer == [0; 20]
        || seller == [0; 20]
        || trade_amount_u == [0; 32]
        || trade_amount_b == [0; 32]
        || price == [0; 32]
        || taker_fee_asset.is_empty()
        || maker_fee_asset.is_empty()
    {
        return None;
    }
    Some(DeepXFinalizedSpotTrade {
        block_hash,
        block_number,
        event_index,
        pair,
        buyer,
        buy_id,
        seller,
        sell_id,
        trade_amount_u,
        trade_amount_b,
        taker: taker.to_string(),
        taker_fee,
        taker_fee_asset,
        maker_fee,
        maker_fee_asset,
        price,
    })
}

fn decode_event_u256_le(value: &ScaleValue<u32>) -> Option<[u8; 32]> {
    let ValueDef::Composite(composite) = &value.value else {
        return None;
    };
    let values: Vec<_> = composite.values().collect();
    if values.len() == 1 {
        return decode_event_u256_le(values[0]);
    }
    if values.len() != 4 {
        return None;
    }
    let mut bytes = [0; 32];
    for (index, value) in values.into_iter().enumerate() {
        let limb = u64::try_from(value.as_u128()?).ok()?;
        bytes[index * 8..(index + 1) * 8].copy_from_slice(&limb.to_le_bytes());
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use parity_scale_codec::Encode;
    use serde_json::Value;
    use subxt_core::{events::Phase, ext::scale_value::Value as ScaleValue};

    use super::*;
    use crate::{
        enums::DeepXEnvironment,
        signing::{DeepXValidatedRuntime, validate_testnet_runtime},
    };

    fn runtime() -> DeepXValidatedRuntime {
        let response: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        let metadata = hex::decode(
            response["result"]
                .as_str()
                .unwrap()
                .trim_start_matches("0x"),
        )
        .unwrap();
        validate_testnet_runtime(
            &DeepXEnvironment::Testnet,
            hex::decode("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap()
                .try_into()
                .unwrap(),
            376,
            1,
            &metadata,
        )
        .unwrap()
    }

    fn system_events(record: Vec<u8>) -> Vec<u8> {
        let mut bytes = Compact(1_u32).encode();
        bytes.extend(record);
        bytes
    }

    fn u256(value: u128) -> ScaleValue<()> {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(&value.to_le_bytes());
        ScaleValue::unnamed_composite([ScaleValue::unnamed_composite(
            bytes
                .as_chunks::<8>()
                .0
                .iter()
                .map(|chunk| ScaleValue::u128(u128::from(u64::from_le_bytes(*chunk)))),
        )])
    }

    #[test]
    fn decodes_first_perpetual_order_id_and_exact_trade_values() {
        let runtime = runtime();
        let pallet = runtime.metadata().pallet_by_name("PerpMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|variant| variant.name == "NewTrade")
            .unwrap();
        let trade = ScaleValue::named_composite([
            ("market_id", ScaleValue::u128(3)),
            ("buyer_order_id", ScaleValue::u128(0)),
            ("buyer", ScaleValue::from_bytes([0x11; 20])),
            ("seller_order_id", ScaleValue::u128(7)),
            ("seller", ScaleValue::from_bytes([0x22; 20])),
            ("price", ScaleValue::u128(2_000_000_000)),
            ("size", ScaleValue::u128(1_000_000_000_000_000_000)),
            ("buyer_leverage", ScaleValue::u128(1_000)),
            ("seller_leverage", ScaleValue::u128(1_000)),
            ("created_at", ScaleValue::u128(42)),
            ("filled_direction", ScaleValue::unnamed_variant("Long", [])),
        ]);
        let fields = ScaleValue::named_composite([
            ("trade", trade),
            ("taker", ScaleValue::unnamed_variant("Buyer", [])),
            ("taker_fee", ScaleValue::i128(1_000)),
            ("taker_fee_asset", ScaleValue::from_bytes(b"USDC")),
            ("maker_fee", ScaleValue::i128(0)),
            ("maker_fee_asset", ScaleValue::from_bytes(b"USDC")),
        ]);
        let ValueDef::Composite(Composite::Named(values)) = &fields.value else {
            panic!("expected named NewTrade fields");
        };
        let mut record = Phase::ApplyExtrinsic(3).encode();
        record.extend([pallet.index(), event.index]);
        for field in &event.fields {
            let value = &values
                .iter()
                .find(|(name, _)| Some(name) == field.name.as_ref())
                .unwrap()
                .1;
            subxt_core::ext::scale_value::scale::encode_as_type(
                value,
                field.ty.id,
                runtime.metadata().types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        let events = system_events(record);
        let trades = decode_finalized_perp_trade_events(
            runtime.interfaces(),
            runtime.metadata(),
            [3; 32],
            42,
            &events,
        )
        .unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].buyer_order_id, 0);
        assert_eq!(trades[0].seller_order_id, 7);
        assert_eq!(trades[0].price, 2_000_000_000);
        assert_eq!(trades[0].size, 1_000_000_000_000_000_000);
        assert_eq!(trades[0].event_index, 0);
        let mut trailing = events;
        trailing.push(0);
        assert!(
            decode_finalized_perp_trade_events(
                runtime.interfaces(),
                runtime.metadata(),
                [3; 32],
                42,
                &trailing,
            )
            .is_err()
        );
    }

    #[test]
    fn decodes_spot_u256_amounts_and_zero_order_id() {
        let runtime = runtime();
        let pallet = runtime.metadata().pallet_by_name("SpotMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|variant| variant.name == "OrderMatched")
            .unwrap();
        let mut record = Phase::ApplyExtrinsic(3).encode();
        record.extend([pallet.index(), event.index]);
        for field in &event.fields {
            let value = match field.name.as_deref().unwrap() {
                "pair" => ScaleValue::from_bytes([0x5a; 32]),
                "buyer" => ScaleValue::from_bytes([0x11; 20]),
                "buy_id" => ScaleValue::u128(0),
                "seller" => ScaleValue::from_bytes([0x22; 20]),
                "sell_id" => ScaleValue::u128(8),
                "trade_amount_u" => u256(50_000_000),
                "trade_amount_b" => u256(1_000_000_000_000_000_000),
                "taker" => ScaleValue::unnamed_variant("Buyer", []),
                "taker_fee" => ScaleValue::i128(10_000_000_000_000_000),
                "taker_fee_asset" => ScaleValue::from_bytes(b"ETH"),
                "maker_fee" => ScaleValue::i128(100_000),
                "maker_fee_asset" => ScaleValue::from_bytes(b"USDC"),
                "price" => u256(50_000_000),
                unknown => panic!("unexpected Spot match field {unknown}"),
            };
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                runtime.metadata().types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        let events = system_events(record);
        let trades = decode_finalized_spot_trade_events(
            runtime.interfaces(),
            runtime.metadata(),
            [3; 32],
            42,
            &events,
        )
        .unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].buy_id, 0);
        assert_eq!(trades[0].sell_id, 8);
        assert_eq!(
            trades[0].trade_amount_b[..16],
            1_000_000_000_000_000_000_u128.to_le_bytes()
        );
        assert_eq!(
            trades[0].trade_amount_u[..16],
            50_000_000_u128.to_le_bytes()
        );
        let mut trailing = events;
        trailing.push(0);
        assert!(
            decode_finalized_spot_trade_events(
                runtime.interfaces(),
                runtime.metadata(),
                [3; 32],
                42,
                &trailing,
            )
            .is_err()
        );
    }
}
