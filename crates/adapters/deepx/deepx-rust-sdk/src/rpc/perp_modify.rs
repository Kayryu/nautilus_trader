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

//! Perpetual modify sizing and event order from DeepX runtime evidence.

use scale_info::{TypeDef, TypeDefPrimitive};
use subxt_core::{
    events::{Events, Phase},
    ext::scale_value::{Composite, Value as ScaleValue},
    metadata::Metadata,
};
use thiserror::Error;

use super::event_decode::decode_event_bytes;
use crate::signing::DeepXRuntimeConfig;

/// Durable terms required to verify a `Subaccount.PerpOrderModified` event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpModifySizingExpected {
    /// Account that owns both order legs.
    pub subaccount: [u8; 20],
    /// Order canceled by the modification.
    pub old_order_id: u64,
    /// Requested replacement target size.
    pub target_size: u128,
    /// New order ID reserved for the replacement leg.
    pub new_order_id: u64,
    /// Extrinsic index whose events may prove the modification.
    pub extrinsic_index: u32,
}

/// Runtime sizing evidence for the replacement order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpModifySizingEvidence {
    /// Size already filled from the old order.
    pub filled_size: u128,
    /// Original requested target size.
    pub target_size: u128,
    /// Exact size to use for the replacement order.
    pub replacement_size: u128,
}

/// Relative order of cancellation, placement, and modify events at one index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXPerpModifyEventOrder {
    /// Cancellation precedes placement, which precedes the modify event.
    Ordered,
    /// One or more events are absent or appear in a different order.
    NotOrdered,
}

/// Errors while decoding perpetual modify event evidence.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeepXPerpModifyEventError {
    /// Metadata lacks the supported event layout.
    #[error("unsupported DeepX perpetual modify event layout")]
    UnsupportedRuntime,
    /// Runtime event bytes or fields cannot be decoded.
    #[error("malformed DeepX perpetual modify event bytes")]
    MalformedEventBytes,
    /// Event terms conflict with the expected modification or each other.
    #[error("conflicting DeepX perpetual modify event evidence")]
    ConflictingEvidence,
}

/// Returns whether metadata exposes the exact supported modify sizing event layout.
#[must_use]
pub fn perp_modify_sizing_event_supported(metadata: &Metadata) -> bool {
    let Some(event) = metadata
        .pallet_by_name("Subaccount")
        .and_then(|pallet| pallet.event_variants())
        .and_then(|variants| {
            variants
                .iter()
                .find(|variant| variant.name == "PerpOrderModified")
        })
    else {
        return false;
    };
    let expected = [
        "subaccount",
        "old_order_id",
        "filled_size",
        "target_size",
        "replacement_size",
        "new_order_id",
    ];
    event.fields.len() == expected.len()
        && event
            .fields
            .iter()
            .zip(expected)
            .all(|(field, name)| field.name.as_deref() == Some(name))
        && event
            .fields
            .iter()
            .enumerate()
            .skip(1)
            .all(|(index, field)| {
                let expected_type = if index == 1 || index == 5 {
                    TypeDefPrimitive::U64
                } else {
                    TypeDefPrimitive::U128
                };
                matches!(
                    metadata.types().resolve(field.ty.id).map(|ty| &ty.type_def),
                    Some(TypeDef::Primitive(primitive)) if *primitive == expected_type
                )
            })
}

/// Decodes exact nonzero replacement size from a unique modify event.
///
/// # Errors
///
/// Returns an error for an unsupported event layout, malformed bytes, or conflicting evidence.
pub fn decode_perp_modify_sizing_event(
    metadata: &Metadata,
    expected: DeepXPerpModifySizingExpected,
    event_bytes: &[u8],
) -> Result<DeepXPerpModifySizingEvidence, DeepXPerpModifyEventError> {
    use DeepXPerpModifyEventError as Error;

    if !perp_modify_sizing_event_supported(metadata) {
        return Err(Error::UnsupportedRuntime);
    }
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    let mut evidence = None;
    for event in events.iter() {
        let event = event.map_err(|_| Error::MalformedEventBytes)?;
        if event.phase() != Phase::ApplyExtrinsic(expected.extrinsic_index)
            || event.pallet_name() != "Subaccount"
            || event.variant_name() != "PerpOrderModified"
        {
            continue;
        }
        if evidence.is_some() {
            return Err(Error::ConflictingEvidence);
        }
        let fields = event
            .field_values()
            .map_err(|_| Error::MalformedEventBytes)?;
        let Composite::Named(fields) = fields else {
            return Err(Error::MalformedEventBytes);
        };
        let field = |name| {
            fields
                .iter()
                .find_map(|(candidate, value)| (candidate == name).then_some(value))
        };
        let filled_size = field("filled_size")
            .and_then(ScaleValue::as_u128)
            .ok_or(Error::MalformedEventBytes)?;
        let target_size = field("target_size")
            .and_then(ScaleValue::as_u128)
            .ok_or(Error::MalformedEventBytes)?;
        let replacement_size = field("replacement_size")
            .and_then(ScaleValue::as_u128)
            .ok_or(Error::MalformedEventBytes)?;
        let old_order_id = field("old_order_id")
            .and_then(ScaleValue::as_u128)
            .ok_or(Error::MalformedEventBytes)?;
        let new_order_id = field("new_order_id")
            .and_then(ScaleValue::as_u128)
            .ok_or(Error::MalformedEventBytes)?;
        if field("subaccount").and_then(decode_event_bytes).as_deref()
            != Some(expected.subaccount.as_slice())
            || old_order_id != u128::from(expected.old_order_id)
            || target_size != expected.target_size
            || new_order_id != u128::from(expected.new_order_id)
            || expected.target_size.checked_sub(filled_size) != Some(replacement_size)
            || replacement_size == 0
        {
            return Err(Error::ConflictingEvidence);
        }
        evidence = Some(DeepXPerpModifySizingEvidence {
            filled_size,
            target_size,
            replacement_size,
        });
    }
    evidence.ok_or(Error::ConflictingEvidence)
}

/// Reads event order at one extrinsic index.
///
/// # Errors
///
/// Returns an error when an event cannot be decoded using the supplied metadata.
pub fn decode_perp_modify_event_order(
    metadata: &Metadata,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXPerpModifyEventOrder, DeepXPerpModifyEventError> {
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    let mut cancel_position = None;
    let mut place_position = None;
    let mut modify_position = None;
    for (position, event) in events.iter().enumerate() {
        let event = event.map_err(|_| DeepXPerpModifyEventError::MalformedEventBytes)?;
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index) {
            continue;
        }
        if event.pallet_name() == "Subaccount" && event.variant_name() == "PerpOrderModified" {
            modify_position = Some(position);
            continue;
        }
        if event.pallet_name() != "PerpMarket" {
            continue;
        }
        match event.variant_name() {
            "OrderCancelled" => cancel_position = Some(position),
            "OrderPlaced" => place_position = Some(position),
            _ => {}
        }
    }
    Ok(
        if cancel_position
            .zip(place_position)
            .is_some_and(|(cancel, place)| cancel < place)
            && place_position
                .zip(modify_position)
                .is_some_and(|(place, modify)| place < modify)
        {
            DeepXPerpModifyEventOrder::Ordered
        } else {
            DeepXPerpModifyEventOrder::NotOrdered
        },
    )
}

#[cfg(test)]
mod tests {
    use frame_metadata::{META_RESERVED, RuntimeMetadata, RuntimeMetadataPrefixed};
    use parity_scale_codec::{Compact, Decode, Encode};
    use rstest::rstest;
    use scale_info::{Field, Variant};
    use serde_json::Value;
    use subxt_core::ext::scale_value::{Value as ScaleValue, ValueDef};

    use super::*;

    fn synthetic_metadata() -> Metadata {
        let response: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        let bytes = hex::decode(
            response["result"]
                .as_str()
                .unwrap()
                .trim_start_matches("0x"),
        )
        .unwrap();
        let RuntimeMetadataPrefixed(_, RuntimeMetadata::V14(mut metadata)) =
            RuntimeMetadataPrefixed::decode(&mut bytes.as_slice()).unwrap()
        else {
            panic!("Expected V14 fixture metadata");
        };
        let event_type_id = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.name == "Subaccount")
            .and_then(|pallet| pallet.event.as_ref())
            .unwrap()
            .ty
            .id;
        let account_type_id = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.name == "PerpMarket")
            .and_then(|pallet| pallet.event.as_ref())
            .and_then(|event| metadata.types.resolve(event.ty.id))
            .and_then(|ty| match &ty.type_def {
                TypeDef::Variant(variants) => variants
                    .variants
                    .iter()
                    .find(|variant| variant.name == "OrderCancelled"),
                _ => None,
            })
            .and_then(|variant| {
                variant
                    .fields
                    .iter()
                    .find(|field| field.name.as_deref() == Some("user"))
            })
            .unwrap()
            .ty
            .id;
        let primitive_id = |primitive| {
            metadata
                .types
                .types
                .iter()
                .find_map(|ty| {
                    matches!(
                        &ty.ty.type_def,
                        TypeDef::Primitive(candidate) if *candidate == primitive
                    )
                    .then_some(ty.id)
                })
                .unwrap()
        };
        let u64_id = primitive_id(TypeDefPrimitive::U64);
        let u128_id = primitive_id(TypeDefPrimitive::U128);
        let fields = vec![
            Field::new(
                Some("subaccount".into()),
                account_type_id.into(),
                None,
                Vec::new(),
            ),
            Field::new(Some("old_order_id".into()), u64_id.into(), None, Vec::new()),
            Field::new(Some("filled_size".into()), u128_id.into(), None, Vec::new()),
            Field::new(Some("target_size".into()), u128_id.into(), None, Vec::new()),
            Field::new(
                Some("replacement_size".into()),
                u128_id.into(),
                None,
                Vec::new(),
            ),
            Field::new(Some("new_order_id".into()), u64_id.into(), None, Vec::new()),
        ];
        let event_type = metadata
            .types
            .types
            .iter_mut()
            .find(|ty| ty.id == event_type_id)
            .unwrap();
        let TypeDef::Variant(variants) = &mut event_type.ty.type_def else {
            panic!("Expected Subaccount event variants");
        };
        let event_index = (0..=u8::MAX)
            .find(|index| {
                !variants
                    .variants
                    .iter()
                    .any(|variant| variant.index == *index)
            })
            .unwrap();
        variants.variants.push(Variant::new(
            "PerpOrderModified".into(),
            fields,
            event_index,
            Vec::new(),
        ));
        let bytes = RuntimeMetadataPrefixed(META_RESERVED, RuntimeMetadata::V14(metadata)).encode();
        subxt_core::metadata::decode_from(&bytes).unwrap()
    }

    fn modify_events(
        metadata: &Metadata,
        expected: DeepXPerpModifySizingExpected,
        filled_size: u128,
        replacement_size: u128,
    ) -> Vec<u8> {
        let pallet = metadata.pallet_by_name("Subaccount").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == "PerpOrderModified")
            .unwrap();
        let values = ScaleValue::named_composite([
            ("subaccount", ScaleValue::from_bytes(expected.subaccount)),
            (
                "old_order_id",
                ScaleValue::u128(u128::from(expected.old_order_id)),
            ),
            ("filled_size", ScaleValue::u128(filled_size)),
            ("target_size", ScaleValue::u128(expected.target_size)),
            ("replacement_size", ScaleValue::u128(replacement_size)),
            (
                "new_order_id",
                ScaleValue::u128(u128::from(expected.new_order_id)),
            ),
        ]);
        let ValueDef::Composite(Composite::Named(fields)) = &values.value else {
            unreachable!();
        };
        let mut record = Phase::ApplyExtrinsic(expected.extrinsic_index).encode();
        record.extend([pallet.index(), event.index]);
        for field in &event.fields {
            let value = fields
                .iter()
                .find_map(|(name, value)| (Some(name) == field.name.as_ref()).then_some(value))
                .unwrap();
            subxt_core::ext::scale_value::scale::encode_as_type(
                value,
                field.ty.id,
                metadata.types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        let mut bytes = Compact(1_u32).encode();
        bytes.extend(record);
        bytes
    }

    #[rstest]
    fn modify_sizing_binds_exact_runtime_fields() {
        let metadata = synthetic_metadata();
        assert!(perp_modify_sizing_event_supported(&metadata));
        let expected = DeepXPerpModifySizingExpected {
            subaccount: [0x11; 20],
            old_order_id: 7,
            target_size: 13,
            new_order_id: 42,
            extrinsic_index: 4,
        };
        let valid = modify_events(&metadata, expected, 5, 8);
        assert_eq!(
            decode_perp_modify_sizing_event(&metadata, expected, &valid),
            Ok(DeepXPerpModifySizingEvidence {
                filled_size: 5,
                target_size: 13,
                replacement_size: 8,
            }),
        );
        assert_eq!(
            decode_perp_modify_sizing_event(
                &metadata,
                expected,
                &modify_events(&metadata, expected, 5, 9)
            ),
            Err(DeepXPerpModifyEventError::ConflictingEvidence),
        );
        let wrong_account = DeepXPerpModifySizingExpected {
            subaccount: [0x22; 20],
            ..expected
        };
        assert_eq!(
            decode_perp_modify_sizing_event(&metadata, wrong_account, &valid),
            Err(DeepXPerpModifyEventError::ConflictingEvidence),
        );
        assert_eq!(
            decode_perp_modify_event_order(&metadata, expected.extrinsic_index, &valid),
            Ok(DeepXPerpModifyEventOrder::NotOrdered),
        );
    }
}
