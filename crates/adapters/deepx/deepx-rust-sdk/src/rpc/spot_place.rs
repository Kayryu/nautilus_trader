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

//! Exact Spot placement inclusion from complete DeepX runtime events.

use parity_scale_codec::{Compact, Decode};
use subxt_core::{
    events::{Events, Phase},
    ext::scale_value::{Composite, Value as ScaleValue, ValueDef},
    metadata::Metadata,
};
use thiserror::Error;

use super::event_decode::decode_event_bytes;
use crate::signing::{
    DeepXPostOnlyParam, DeepXRuntimeConfig, DeepXSpotOrderType, DeepXSpotPlaceParams,
    DeepXTimeInForce,
    runtime_interface::{DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError},
};

/// Durable Spot order terms required to bind a placement event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSpotPlaceExpected {
    /// Exact runtime call arguments.
    pub params: DeepXSpotPlaceParams,
    /// Order identifier reserved as the timestamp nonce.
    pub order_id: u64,
    /// Index of the signed extrinsic in the block.
    pub extrinsic_index: u32,
}

/// Outcome established from indexed dispatch and Spot placement events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXSpotPlaceInclusionOutcome {
    /// Dispatch succeeded and the matching order state event was emitted.
    Success,
    /// Dispatch failed without applying the placement.
    Failed,
}

/// Indexed inclusion evidence for a Spot placement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSpotPlaceInclusionEvidence {
    /// Index of the signed extrinsic in the block.
    pub extrinsic_index: u32,
    /// Authoritative inclusion outcome at that index.
    pub outcome: DeepXSpotPlaceInclusionOutcome,
}

/// Errors while binding complete runtime events to a Spot placement.
#[derive(Debug, Error)]
pub enum DeepXSpotPlaceEventError {
    /// Required runtime events are absent from the approved interface.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE decoding failed against the supplied metadata.
    #[error("unable to decode DeepX Spot placement events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// Event storage is incomplete, or no dispatch event exists at the target index.
    #[error("incomplete DeepX Spot placement event storage")]
    MalformedEventBytes,
    /// Runtime order state does not match the reserved order terms.
    #[error("DeepX Spot placement event conflicts with expected order terms")]
    ConflictingEvent,
    /// Dispatch or placement was emitted more than once at the target index.
    #[error("duplicate DeepX Spot placement event")]
    DuplicateEvent,
    /// Successful dispatch did not emit a matching order state event.
    #[error("successful DeepX Spot placement has no matching business event")]
    MissingBusinessEvent,
    /// A failed dispatch also emitted a matching order state event.
    #[error("failed DeepX Spot placement conflicts with a business event")]
    ConflictingBusinessEvent,
}

/// Verifies one Spot placement against complete `System.Events` storage.
///
/// The caller verifies the signed operation and block canonicality.
///
/// # Errors
///
/// Returns an error for unsupported runtime events, incomplete or conflicting storage, or an
/// impossible dispatch and order-state combination.
pub fn decode_spot_place_inclusion(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    expected: DeepXSpotPlaceExpected,
    event_bytes: &[u8],
) -> Result<DeepXSpotPlaceInclusionEvidence, DeepXSpotPlaceEventError> {
    use DeepXSpotPlaceEventError as Error;

    let event_name = if expected.params.is_buy {
        "StateOrderBuy"
    } else {
        "StateOrderSell"
    };
    interfaces.event("SpotMarket", event_name)?;
    interfaces.event("System", "ExtrinsicSuccess")?;
    interfaces.event("System", "ExtrinsicFailed")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| Error::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut dispatch = None;
    let mut matched = false;
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    for event in events.iter() {
        let event = event.map_err(|e| Error::Decode(Box::new(e)))?;
        count = count.checked_add(1).ok_or(Error::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(Error::MalformedEventBytes)?;
        if event.phase() != Phase::ApplyExtrinsic(expected.extrinsic_index) {
            continue;
        }
        if event.pallet_name() == "System" {
            let outcome = match event.variant_name() {
                "ExtrinsicSuccess" => DeepXSpotPlaceInclusionOutcome::Success,
                "ExtrinsicFailed" => DeepXSpotPlaceInclusionOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "SpotMarket" && event.variant_name() == event_name {
            let fields = event
                .field_values()
                .map_err(|e| Error::Decode(Box::new(e)))?;
            let Composite::Named(fields) = fields else {
                return Err(Error::ConflictingEvent);
            };
            if fields.len() != 1
                || !fields.iter().any(|(name, order)| {
                    name == "order" && spot_place_order_matches(order, expected)
                })
            {
                return Err(Error::ConflictingEvent);
            }
            if matched {
                return Err(Error::DuplicateEvent);
            }
            matched = true;
        }
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(Error::MalformedEventBytes);
    }
    let outcome = match (dispatch.ok_or(Error::MalformedEventBytes)?, matched) {
        (DeepXSpotPlaceInclusionOutcome::Success, true) => DeepXSpotPlaceInclusionOutcome::Success,
        (DeepXSpotPlaceInclusionOutcome::Failed, false) => DeepXSpotPlaceInclusionOutcome::Failed,
        (DeepXSpotPlaceInclusionOutcome::Success, false) => {
            return Err(Error::MissingBusinessEvent);
        }
        (DeepXSpotPlaceInclusionOutcome::Failed, true) => {
            return Err(Error::ConflictingBusinessEvent);
        }
    };
    Ok(DeepXSpotPlaceInclusionEvidence {
        extrinsic_index: expected.extrinsic_index,
        outcome,
    })
}

fn spot_place_order_matches(value: &ScaleValue<u32>, expected: DeepXSpotPlaceExpected) -> bool {
    let ValueDef::Composite(Composite::Named(fields)) = &value.value else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    fields.len() == 12
        && field("id").and_then(ScaleValue::as_u128) == Some(u128::from(expected.order_id))
        && field("maker").and_then(decode_event_bytes).as_deref()
            == Some(expected.params.subaccount.as_slice())
        && field("pair").and_then(decode_event_bytes).as_deref()
            == Some(expected.params.pair.as_slice())
        && field("price").is_some()
        && field("quote_amount")
            .is_some_and(|value| value_matches_u256_le(value, &expected.params.quote_amount))
        && field("base_amount")
            .is_some_and(|value| value_matches_u256_le(value, &expected.params.base_amount))
        && field("create_time").is_some()
        && field("status").is_some()
        && field("order_type")
            .is_some_and(|value| value_matches_order_type(value, expected.params.order_type))
        && field("post_only")
            .is_some_and(|value| value_matches_post_only(value, expected.params.post_only))
        && field("reduce_only").and_then(ScaleValue::as_bool) == Some(expected.params.reduce_only)
        && field("is_buy").and_then(ScaleValue::as_bool) == Some(expected.params.is_buy)
}

fn value_matches_u256_le(value: &ScaleValue<u32>, expected: &[u8; 32]) -> bool {
    let ValueDef::Composite(composite) = &value.value else {
        return false;
    };
    let values: Vec<_> = composite.values().collect();
    if values.len() == 1 {
        return value_matches_u256_le(values[0], expected);
    }
    values.len() == 4
        && values.iter().enumerate().all(|(index, value)| {
            let offset = index * 8;
            let expected_limb = u64::from_le_bytes(
                expected[offset..offset + 8]
                    .try_into()
                    .expect("fixed U256 limb"),
            );
            value.as_u128() == Some(u128::from(expected_limb))
        })
}

pub(super) fn value_matches_order_type(
    value: &ScaleValue<u32>,
    expected: DeepXSpotOrderType,
) -> bool {
    let ValueDef::Variant(variant) = &value.value else {
        return false;
    };
    match expected {
        DeepXSpotOrderType::Limit(time_in_force) => {
            let values: Vec<_> = variant.values.values().collect();
            variant.name == "Limit"
                && values.len() == 1
                && matches!(&values[0].value, ValueDef::Variant(value)
                    if value.name == match time_in_force {
                        DeepXTimeInForce::Gtc => "GTC",
                        DeepXTimeInForce::Ioc => "IOC",
                        DeepXTimeInForce::Fok => "FOK",
                    } && value.values.values().next().is_none())
        }
        DeepXSpotOrderType::Market(slippage) => {
            let values: Vec<_> = variant.values.values().collect();
            if variant.name != "Market" || values.len() != 1 {
                return false;
            }
            let ValueDef::Variant(option) = &values[0].value else {
                return false;
            };
            let option_values: Vec<_> = option.values.values().collect();
            match slippage {
                Some(expected) => {
                    option.name == "Some"
                        && option_values.len() == 1
                        && option_values[0].as_u128() == Some(u128::from(expected))
                }
                None => option.name == "None" && option_values.is_empty(),
            }
        }
        DeepXSpotOrderType::Stop => {
            variant.name == "Stop" && variant.values.values().next().is_none()
        }
    }
}

pub(super) fn value_matches_post_only(
    value: &ScaleValue<u32>,
    expected: DeepXPostOnlyParam,
) -> bool {
    let ValueDef::Variant(variant) = &value.value else {
        return false;
    };
    let expected = match expected {
        DeepXPostOnlyParam::None => "None",
        DeepXPostOnlyParam::MustPostOnly => "MustPostOnly",
        DeepXPostOnlyParam::Adaptive => "Adaptive",
    };
    variant.name == expected && variant.values.values().next().is_none()
}

#[cfg(test)]
mod tests {
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde_json::Value;
    use subxt_core::ext::scale_value::Value as ScaleValue;

    use super::*;
    use crate::{enums::DeepXEnvironment, signing::validate_testnet_runtime};

    fn runtime() -> crate::signing::DeepXValidatedRuntime {
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

    fn u256_value(bytes: [u8; 32]) -> ScaleValue<()> {
        ScaleValue::unnamed_composite([ScaleValue::unnamed_composite(
            bytes
                .as_chunks::<8>()
                .0
                .iter()
                .map(|chunk| ScaleValue::u128(u128::from(u64::from_le_bytes(*chunk)))),
        )])
    }

    fn spot_place_record(
        runtime: &crate::signing::DeepXValidatedRuntime,
        expected: DeepXSpotPlaceExpected,
    ) -> Vec<u8> {
        let pallet = runtime.metadata().pallet_by_name("SpotMarket").unwrap();
        let name = if expected.params.is_buy {
            "StateOrderBuy"
        } else {
            "StateOrderSell"
        };
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|v| v.name == name)
            .unwrap();
        assert_eq!(event.fields.len(), 1);
        assert_eq!(event.fields[0].name.as_deref(), Some("order"));
        let order_type = match expected.params.order_type {
            DeepXSpotOrderType::Limit(time_in_force) => {
                let name = match time_in_force {
                    DeepXTimeInForce::Gtc => "GTC",
                    DeepXTimeInForce::Ioc => "IOC",
                    DeepXTimeInForce::Fok => "FOK",
                };
                ScaleValue::unnamed_variant("Limit", [ScaleValue::unnamed_variant(name, [])])
            }
            DeepXSpotOrderType::Market(slippage) => {
                let value = match slippage {
                    Some(value) => {
                        ScaleValue::unnamed_variant("Some", [ScaleValue::u128(u128::from(value))])
                    }
                    None => ScaleValue::unnamed_variant("None", []),
                };
                ScaleValue::unnamed_variant("Market", [value])
            }
            DeepXSpotOrderType::Stop => ScaleValue::unnamed_variant("Stop", []),
        };
        let post_only = match expected.params.post_only {
            DeepXPostOnlyParam::None => "None",
            DeepXPostOnlyParam::MustPostOnly => "MustPostOnly",
            DeepXPostOnlyParam::Adaptive => "Adaptive",
        };
        let order = ScaleValue::named_composite([
            ("id", ScaleValue::u128(u128::from(expected.order_id))),
            ("maker", ScaleValue::from_bytes(expected.params.subaccount)),
            ("pair", ScaleValue::from_bytes(expected.params.pair)),
            (
                "price",
                ScaleValue::unnamed_variant("Some", [u256_value([1; 32])]),
            ),
            ("quote_amount", u256_value(expected.params.quote_amount)),
            ("base_amount", u256_value(expected.params.base_amount)),
            ("create_time", ScaleValue::u128(42)),
            ("status", ScaleValue::unnamed_variant("Open", [])),
            ("order_type", order_type),
            ("post_only", ScaleValue::unnamed_variant(post_only, [])),
            ("reduce_only", ScaleValue::bool(expected.params.reduce_only)),
            ("is_buy", ScaleValue::bool(expected.params.is_buy)),
        ]);
        let mut record = Phase::ApplyExtrinsic(expected.extrinsic_index).encode();
        record.extend([pallet.index(), event.index]);
        subxt_core::ext::scale_value::scale::encode_as_type(
            &order,
            event.fields[0].ty.id,
            runtime.metadata().types(),
            &mut record,
        )
        .unwrap();
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        record
    }

    fn dispatch_record(runtime: &crate::signing::DeepXValidatedRuntime, success: bool) -> Vec<u8> {
        let pallet = runtime.metadata().pallet_by_name("System").unwrap();
        let name = if success {
            "ExtrinsicSuccess"
        } else {
            "ExtrinsicFailed"
        };
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|v| v.name == name)
            .unwrap();
        let mut bytes = Phase::ApplyExtrinsic(3).encode();
        bytes.extend([pallet.index(), event.index]);
        let dispatch_info = ScaleValue::named_composite([
            (
                "weight",
                ScaleValue::named_composite([
                    ("ref_time", ScaleValue::u128(0)),
                    ("proof_size", ScaleValue::u128(0)),
                ]),
            ),
            (
                "call_type",
                ScaleValue::unnamed_variant("Timestamp", [ScaleValue::u128(0)]),
            ),
            (
                "priority",
                ScaleValue::unnamed_composite([ScaleValue::u128(0), ScaleValue::u128(0)]),
            ),
            ("class", ScaleValue::unnamed_variant("Normal", [])),
            ("pays_fee", ScaleValue::unnamed_variant("Yes", [])),
        ]);
        let values = if success {
            vec![dispatch_info]
        } else {
            vec![ScaleValue::unnamed_variant("Other", []), dispatch_info]
        };
        for (field, value) in event.fields.iter().zip(values) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                runtime.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    fn events(records: &[Vec<u8>]) -> Vec<u8> {
        let mut bytes = Compact(u32::try_from(records.len()).unwrap()).encode();
        for record in records {
            bytes.extend(record);
        }
        bytes
    }

    #[rstest]
    fn spot_place_binds_exact_order_policy_matrix() {
        let runtime = runtime();
        let decode = |expected, bytes: &[u8]| {
            decode_spot_place_inclusion(runtime.interfaces(), runtime.metadata(), expected, bytes)
        };
        let success = dispatch_record(&runtime, true);
        let failed = dispatch_record(&runtime, false);
        let mut expected = DeepXSpotPlaceExpected {
            params: DeepXSpotPlaceParams {
                subaccount: [0x11; 20],
                pair: [0xa5; 32],
                is_buy: true,
                quote_amount: std::array::from_fn(|index| index as u8),
                base_amount: std::array::from_fn(|index| (31 - index) as u8),
                order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc),
                post_only: DeepXPostOnlyParam::MustPostOnly,
                reduce_only: false,
            },
            order_id: 1_725_000_000_125,
            extrinsic_index: 3,
        };
        for is_buy in [true, false] {
            for order_type in [
                DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc),
                DeepXSpotOrderType::Limit(DeepXTimeInForce::Ioc),
                DeepXSpotOrderType::Limit(DeepXTimeInForce::Fok),
                DeepXSpotOrderType::Market(None),
                DeepXSpotOrderType::Market(Some(u64::MAX)),
                DeepXSpotOrderType::Stop,
            ] {
                for post_only in [
                    DeepXPostOnlyParam::None,
                    DeepXPostOnlyParam::MustPostOnly,
                    DeepXPostOnlyParam::Adaptive,
                ] {
                    expected.params.is_buy = is_buy;
                    expected.params.order_type = order_type;
                    expected.params.post_only = post_only;
                    let business = spot_place_record(&runtime, expected);
                    let complete = events(&[success.clone(), business.clone()]);
                    assert!(matches!(
                        decode(expected, &complete),
                        Ok(DeepXSpotPlaceInclusionEvidence {
                            extrinsic_index: 3,
                            outcome: DeepXSpotPlaceInclusionOutcome::Success,
                        }),
                    ));
                    for changed in [
                        DeepXSpotPlaceExpected {
                            params: DeepXSpotPlaceParams {
                                quote_amount: [0xff; 32],
                                ..expected.params
                            },
                            ..expected
                        },
                        DeepXSpotPlaceExpected {
                            params: DeepXSpotPlaceParams {
                                order_type: DeepXSpotOrderType::Stop,
                                ..expected.params
                            },
                            ..expected
                        },
                    ] {
                        if changed != expected {
                            assert!(matches!(
                                decode(changed, &complete),
                                Err(DeepXSpotPlaceEventError::ConflictingEvent),
                            ));
                        }
                    }
                }
            }
        }
        assert!(matches!(
            decode(expected, &events(std::slice::from_ref(&failed))),
            Ok(DeepXSpotPlaceInclusionEvidence {
                outcome: DeepXSpotPlaceInclusionOutcome::Failed,
                ..
            }),
        ));
        assert!(matches!(
            decode(expected, &events(std::slice::from_ref(&success))),
            Err(DeepXSpotPlaceEventError::MissingBusinessEvent),
        ));
        assert!(matches!(
            decode(expected, &[0, 0]),
            Err(DeepXSpotPlaceEventError::MalformedEventBytes),
        ));
    }
}
