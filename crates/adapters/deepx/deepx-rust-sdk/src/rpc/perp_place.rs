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

//! Exact perpetual placement inclusion from complete DeepX runtime events.

use parity_scale_codec::{Compact, Decode};
use subxt_core::{
    events::{Events, Phase},
    ext::scale_value::{Composite, Value as ScaleValue, ValueDef},
    metadata::Metadata,
};
use thiserror::Error;

use super::{
    event_decode::decode_event_bytes,
    spot_place::{value_matches_order_type, value_matches_post_only},
};
use crate::signing::{
    DeepXPerpOrderType, DeepXPerpPlaceParams, DeepXRuntimeConfig, DeepXSpotOrderType,
    runtime_interface::{DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError},
};

/// Exact order terms required to bind a perpetual placement event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpPlaceExpected {
    /// Exact runtime call arguments.
    pub params: DeepXPerpPlaceParams,
    /// Order identifier reserved as the timestamp nonce.
    pub order_id: u64,
    /// Block number used by the runtime as order creation time.
    pub block_number: u64,
    /// Index of the signed extrinsic in the block.
    pub extrinsic_index: u32,
}

/// Outcome established from indexed dispatch and perpetual placement events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXPerpPlaceInclusionOutcome {
    /// Dispatch succeeded and the matching order event was emitted.
    Success,
    /// Dispatch failed without applying the placement.
    Failed,
}

/// Indexed inclusion evidence for a perpetual placement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpPlaceInclusionEvidence {
    /// Index of the signed extrinsic in the block.
    pub extrinsic_index: u32,
    /// Authoritative inclusion outcome at that index.
    pub outcome: DeepXPerpPlaceInclusionOutcome,
}

/// Errors while binding complete runtime events to a perpetual placement.
#[derive(Debug, Error)]
pub enum DeepXPerpPlaceEventError {
    /// Required runtime events are absent from the approved interface.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE decoding failed against the supplied metadata.
    #[error("unable to decode DeepX perpetual placement events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// Event storage is incomplete, or no dispatch event exists at the target index.
    #[error("incomplete DeepX perpetual placement event storage")]
    MalformedEventBytes,
    /// Runtime order state does not match the reserved order terms.
    #[error("DeepX perpetual placement event conflicts with expected order terms")]
    ConflictingEvent,
    /// Dispatch or placement was emitted more than once at the target index.
    #[error("duplicate DeepX perpetual placement event")]
    DuplicateEvent,
    /// Successful dispatch did not emit a matching placement event.
    #[error("successful DeepX perpetual placement has no matching business event")]
    MissingBusinessEvent,
    /// A failed dispatch also emitted a matching placement event.
    #[error("failed DeepX perpetual placement conflicts with a business event")]
    ConflictingBusinessEvent,
}

/// Verifies one perpetual placement against complete `System.Events` storage.
///
/// The caller verifies the signed operation and block canonicality.
///
/// # Errors
///
/// Returns an error for unsupported runtime events, incomplete or conflicting storage, or an
/// impossible dispatch and placement combination.
pub fn decode_perp_place_inclusion(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    expected: DeepXPerpPlaceExpected,
    event_bytes: &[u8],
) -> Result<DeepXPerpPlaceInclusionEvidence, DeepXPerpPlaceEventError> {
    use DeepXPerpPlaceEventError as Error;

    interfaces.event("PerpMarket", "OrderPlaced")?;
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
                "ExtrinsicSuccess" => DeepXPerpPlaceInclusionOutcome::Success,
                "ExtrinsicFailed" => DeepXPerpPlaceInclusionOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "PerpMarket" && event.variant_name() == "OrderPlaced" {
            let fields = event
                .field_values()
                .map_err(|e| Error::Decode(Box::new(e)))?;
            if !perp_place_fields_match(&fields, expected) {
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
        (DeepXPerpPlaceInclusionOutcome::Success, true) => DeepXPerpPlaceInclusionOutcome::Success,
        (DeepXPerpPlaceInclusionOutcome::Failed, false) => DeepXPerpPlaceInclusionOutcome::Failed,
        (DeepXPerpPlaceInclusionOutcome::Success, false) => {
            return Err(Error::MissingBusinessEvent);
        }
        (DeepXPerpPlaceInclusionOutcome::Failed, true) => {
            return Err(Error::ConflictingBusinessEvent);
        }
    };
    Ok(DeepXPerpPlaceInclusionEvidence {
        extrinsic_index: expected.extrinsic_index,
        outcome,
    })
}

fn perp_place_fields_match(fields: &Composite<u32>, expected: DeepXPerpPlaceExpected) -> bool {
    let Composite::Named(fields) = fields else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    if fields.len() != 2
        || field("order_id").and_then(ScaleValue::as_u128) != Some(u128::from(expected.order_id))
    {
        return false;
    }
    let Some(ScaleValue {
        value: ValueDef::Composite(Composite::Named(order)),
        ..
    }) = field("order")
    else {
        return false;
    };
    let field = |name| {
        order
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    let params = expected.params;
    let spot_order_type = match params.order_type {
        DeepXPerpOrderType::Limit(tif) => DeepXSpotOrderType::Limit(tif),
        DeepXPerpOrderType::Market(slippage) => DeepXSpotOrderType::Market(slippage),
        DeepXPerpOrderType::Stop => return false,
    };
    order.len() == 16
        && field("order_id").and_then(ScaleValue::as_u128) == Some(u128::from(expected.order_id))
        && field("owner").and_then(decode_event_bytes).as_deref() == Some(params.subaccount.as_slice())
        && field("market_id").and_then(ScaleValue::as_u128) == Some(u128::from(params.market_id))
        && field("is_long").and_then(ScaleValue::as_bool) == Some(params.is_long)
        && field("size").and_then(ScaleValue::as_u128) == Some(params.size)
        && field("price").and_then(ScaleValue::as_u128).is_some_and(|actual| {
            matches!(params.order_type, DeepXPerpOrderType::Market(_)) || actual == params.price
        })
        && field("order_type").is_some_and(|value| value_matches_order_type(value, spot_order_type))
        && field("take_profit").is_some_and(|value| value_matches_optional_u128(value, params.take_profit))
        && field("stop_loss").is_some_and(|value| value_matches_optional_u128(value, params.stop_loss))
        && field("reduce_only").and_then(ScaleValue::as_bool) == Some(params.reduce_only)
        && field("post_only").is_some_and(|value| value_matches_post_only(value, params.post_only))
        && field("create_time").and_then(ScaleValue::as_u128) == Some(u128::from(expected.block_number))
        && field("leverage").and_then(ScaleValue::as_u128).is_some()
        && field("status").is_some_and(|value| matches!(&value.value, ValueDef::Variant(variant) if variant.name == "Open" && variant.values.values().next().is_none()))
        && field("size_filled").and_then(ScaleValue::as_u128) == Some(0)
        && field("size_remain").and_then(ScaleValue::as_u128) == Some(params.size)
}

fn value_matches_optional_u128(value: &ScaleValue<u32>, expected: Option<u128>) -> bool {
    let ValueDef::Variant(variant) = &value.value else {
        return false;
    };
    let values: Vec<_> = variant.values.values().collect();
    match expected {
        None => variant.name == "None" && values.is_empty(),
        Some(expected) => {
            variant.name == "Some" && values.len() == 1 && values[0].as_u128() == Some(expected)
        }
    }
}

#[cfg(test)]
mod tests {
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde_json::Value;
    use subxt_core::ext::scale_value::Value as ScaleValue;

    use super::*;
    use crate::{
        enums::DeepXEnvironment,
        signing::{DeepXPostOnlyParam, DeepXTimeInForce, validate_testnet_runtime},
    };

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

    fn expected() -> DeepXPerpPlaceExpected {
        DeepXPerpPlaceExpected {
            params: DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 100,
                is_long: true,
                size: u128::MAX,
                price: u128::MAX,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                take_profit: Some(u128::MAX),
                stop_loss: Some(0),
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
            order_id: 1_725_000_000_125,
            block_number: 42,
            extrinsic_index: 3,
        }
    }

    fn order_value(expected: DeepXPerpPlaceExpected) -> ScaleValue<()> {
        let params = expected.params;
        let option = |value: Option<u128>| match value {
            Some(value) => ScaleValue::unnamed_variant("Some", [ScaleValue::u128(value)]),
            None => ScaleValue::unnamed_variant("None", []),
        };
        let order_type = match params.order_type {
            DeepXPerpOrderType::Limit(tif) => ScaleValue::unnamed_variant(
                "Limit",
                [ScaleValue::unnamed_variant(
                    match tif {
                        DeepXTimeInForce::Gtc => "GTC",
                        DeepXTimeInForce::Ioc => "IOC",
                        DeepXTimeInForce::Fok => "FOK",
                    },
                    [],
                )],
            ),
            DeepXPerpOrderType::Market(slippage) => {
                ScaleValue::unnamed_variant("Market", [option(slippage.map(u128::from))])
            }
            DeepXPerpOrderType::Stop => ScaleValue::unnamed_variant("Stop", []),
        };
        let post_only = match params.post_only {
            DeepXPostOnlyParam::None => "None",
            DeepXPostOnlyParam::MustPostOnly => "MustPostOnly",
            DeepXPostOnlyParam::Adaptive => "Adaptive",
        };
        ScaleValue::named_composite([
            ("order_id", ScaleValue::u128(u128::from(expected.order_id))),
            ("owner", ScaleValue::from_bytes(params.subaccount)),
            ("market_id", ScaleValue::u128(u128::from(params.market_id))),
            ("is_long", ScaleValue::bool(params.is_long)),
            ("size", ScaleValue::u128(params.size)),
            (
                "price",
                ScaleValue::u128(
                    if matches!(params.order_type, DeepXPerpOrderType::Market(_)) {
                        123
                    } else {
                        params.price
                    },
                ),
            ),
            ("order_type", order_type),
            (
                "create_time",
                ScaleValue::u128(u128::from(expected.block_number)),
            ),
            ("leverage", ScaleValue::u128(25000)),
            ("status", ScaleValue::unnamed_variant("Open", [])),
            ("size_filled", ScaleValue::u128(0)),
            ("size_remain", ScaleValue::u128(params.size)),
            ("take_profit", option(params.take_profit)),
            ("stop_loss", option(params.stop_loss)),
            ("reduce_only", ScaleValue::bool(params.reduce_only)),
            ("post_only", ScaleValue::unnamed_variant(post_only, [])),
        ])
    }

    fn place_record(
        runtime: &crate::signing::DeepXValidatedRuntime,
        expected: DeepXPerpPlaceExpected,
        order: &ScaleValue<()>,
    ) -> Vec<u8> {
        let pallet = runtime.metadata().pallet_by_name("PerpMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == "OrderPlaced")
            .unwrap();
        let mut record = Phase::ApplyExtrinsic(expected.extrinsic_index).encode();
        record.extend([pallet.index(), event.index]);
        for field in &event.fields {
            let value = match field.name.as_deref() {
                Some("order_id") => ScaleValue::u128(u128::from(expected.order_id)),
                Some("order") => order.clone(),
                _ => panic!("unexpected placement field"),
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
    fn perp_place_binds_exact_order_terms_and_dispatch() {
        let runtime = runtime();
        let expected = expected();
        let place = place_record(&runtime, expected, &order_value(expected));
        let success = dispatch_record(&runtime, true);
        let failed = dispatch_record(&runtime, false);
        let decode = |bytes: &[u8]| {
            decode_perp_place_inclusion(runtime.interfaces(), runtime.metadata(), expected, bytes)
        };
        assert_eq!(
            decode(&events(&[place.clone(), success.clone()]))
                .unwrap()
                .outcome,
            DeepXPerpPlaceInclusionOutcome::Success,
        );
        assert_eq!(
            decode(&events(std::slice::from_ref(&failed)))
                .unwrap()
                .outcome,
            DeepXPerpPlaceInclusionOutcome::Failed,
        );
        assert!(matches!(
            decode(&events(std::slice::from_ref(&success))),
            Err(DeepXPerpPlaceEventError::MissingBusinessEvent),
        ));
        assert!(matches!(
            decode(&events(&[place.clone(), failed])),
            Err(DeepXPerpPlaceEventError::ConflictingBusinessEvent),
        ));
        assert!(matches!(
            decode(&events(&[place.clone(), place, success])),
            Err(DeepXPerpPlaceEventError::DuplicateEvent),
        ));

        let mut order = order_value(expected);
        let ValueDef::Composite(Composite::Named(fields)) = &mut order.value else {
            unreachable!();
        };
        fields
            .iter_mut()
            .find(|(name, _)| name == "size")
            .unwrap()
            .1 = ScaleValue::u128(1);
        let bad = place_record(&runtime, expected, &order);
        assert!(matches!(
            decode(&events(&[bad, dispatch_record(&runtime, true)])),
            Err(DeepXPerpPlaceEventError::ConflictingEvent),
        ));
    }

    #[rstest]
    fn perp_place_order_policy_matrix() {
        let runtime = runtime();
        let mut expected = expected();
        for is_long in [true, false] {
            for order_type in [
                DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                DeepXPerpOrderType::Limit(DeepXTimeInForce::Ioc),
                DeepXPerpOrderType::Limit(DeepXTimeInForce::Fok),
                DeepXPerpOrderType::Market(None),
                DeepXPerpOrderType::Market(Some(u64::MAX)),
            ] {
                for post_only in [
                    DeepXPostOnlyParam::None,
                    DeepXPostOnlyParam::MustPostOnly,
                    DeepXPostOnlyParam::Adaptive,
                ] {
                    expected.params.is_long = is_long;
                    expected.params.order_type = order_type;
                    expected.params.post_only = post_only;
                    expected.params.price = if matches!(order_type, DeepXPerpOrderType::Market(_)) {
                        0
                    } else {
                        u128::MAX
                    };
                    let place = place_record(&runtime, expected, &order_value(expected));
                    let records = events(&[place, dispatch_record(&runtime, true)]);
                    assert_eq!(
                        decode_perp_place_inclusion(
                            runtime.interfaces(),
                            runtime.metadata(),
                            expected,
                            &records,
                        )
                        .unwrap()
                        .outcome,
                        DeepXPerpPlaceInclusionOutcome::Success,
                    );
                }
            }
        }
    }

    #[rstest]
    #[case("order_id", ScaleValue::u128(1))]
    #[case("owner", ScaleValue::from_bytes([0x22; 20]))]
    #[case("market_id", ScaleValue::u128(1))]
    #[case("is_long", ScaleValue::bool(false))]
    #[case("size", ScaleValue::u128(1))]
    #[case("price", ScaleValue::u128(1))]
    #[case("take_profit", ScaleValue::unnamed_variant("None", []))]
    #[case("stop_loss", ScaleValue::unnamed_variant("None", []))]
    #[case("reduce_only", ScaleValue::bool(true))]
    #[case("post_only", ScaleValue::unnamed_variant("MustPostOnly", []))]
    #[case("create_time", ScaleValue::u128(43))]
    #[case("size_filled", ScaleValue::u128(1))]
    #[case("size_remain", ScaleValue::u128(1))]
    #[case("status", ScaleValue::unnamed_variant("Filled", []))]
    #[case("order_type", ScaleValue::unnamed_variant("Limit", [ScaleValue::unnamed_variant("IOC", [])]))]
    fn perp_place_rejects_conflicting_fields(#[case] name: &str, #[case] value: ScaleValue<()>) {
        let runtime = runtime();
        let expected = expected();
        let mut order = order_value(expected);
        let ValueDef::Composite(Composite::Named(fields)) = &mut order.value else {
            unreachable!();
        };
        fields
            .iter_mut()
            .find(|(field, _)| field == name)
            .unwrap()
            .1 = value;
        let records = events(&[
            place_record(&runtime, expected, &order),
            dispatch_record(&runtime, true),
        ]);
        assert!(matches!(
            decode_perp_place_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                expected,
                &records,
            ),
            Err(DeepXPerpPlaceEventError::ConflictingEvent),
        ));
    }
}
