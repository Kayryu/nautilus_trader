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

//! Finalized Spot modify evidence from its cancellation and replacement events.

use subxt_core::{
    events::{Events, Phase},
    metadata::Metadata,
};
use thiserror::Error;

use super::{
    spot_cancel::{
        DeepXSpotCancelEventError, DeepXSpotCancelExpected, DeepXSpotCancelInclusionOutcome,
        decode_spot_cancel_inclusion,
    },
    spot_place::{
        DeepXSpotPlaceEventError, DeepXSpotPlaceExpected, DeepXSpotPlaceInclusionOutcome,
        decode_spot_place_inclusion,
    },
};
use crate::signing::{
    DeepXRuntimeConfig, DeepXSpotModifyParams, runtime_interface::DeepXRuntimeInterfaceCatalog,
};

/// Exact terms of one transactional Spot modification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSpotModifyExpected {
    /// The signed cancel and replacement arguments.
    pub params: DeepXSpotModifyParams,
    /// Replacement order ID reserved by the signed nonce.
    pub new_order_id: u64,
    /// Signed extrinsic index in the finalized block.
    pub extrinsic_index: u32,
}

/// Outcome established by dispatch and both Spot business events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXSpotModifyInclusionOutcome {
    /// Both legs succeeded in the intended order.
    Success,
    /// Dispatch failed without applying either leg.
    Failed,
}

/// Errors while verifying one Spot modify transaction.
#[derive(Debug, Error)]
pub enum DeepXSpotModifyEventError {
    /// The cancel leg could not be verified.
    #[error(transparent)]
    Cancel(#[from] DeepXSpotCancelEventError),
    /// The replacement leg could not be verified.
    #[error(transparent)]
    Place(#[from] DeepXSpotPlaceEventError),
    /// Event storage could not be decoded for ordering.
    #[error("unable to decode DeepX Spot modify event order: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The two verified business events appeared in the wrong order.
    #[error("DeepX Spot modify cancellation must precede replacement")]
    EventOrder,
    /// Signed modify legs disagree on their runtime scope.
    #[error("DeepX Spot modify legs must target the same subaccount, pair, and side")]
    ScopeMismatch,
    /// Fast cancellation has no approved business event contract.
    #[error("DeepX Spot fast cancellation cannot be verified by this event decoder")]
    FastCancelUnsupported,
}

/// Verifies the finalized dispatch and both ordered Spot modify legs.
///
/// The caller verifies the signed operation and block canonicality. Fast cancellation is excluded
/// because its business event contract is not approved.
///
/// # Errors
///
/// Returns an error for conflicting terms, malformed or missing events, or reversed event order.
pub fn decode_spot_modify_inclusion(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    expected: DeepXSpotModifyExpected,
    event_bytes: &[u8],
) -> Result<DeepXSpotModifyInclusionOutcome, DeepXSpotModifyEventError> {
    let cancel = expected.params.cancel;
    let place = expected.params.place;
    if cancel.subaccount != place.subaccount
        || cancel.pair != place.pair
        || cancel.is_buy != place.is_buy
    {
        return Err(DeepXSpotModifyEventError::ScopeMismatch);
    }
    if cancel.fast_cancel {
        return Err(DeepXSpotModifyEventError::FastCancelUnsupported);
    }
    let cancel_evidence = decode_spot_cancel_inclusion(
        interfaces,
        metadata,
        DeepXSpotCancelExpected {
            subaccount: cancel.subaccount,
            pair: cancel.pair,
            order_id: cancel.order_id,
            is_buy: cancel.is_buy,
            extrinsic_index: expected.extrinsic_index,
        },
        event_bytes,
    )?;
    let place_evidence = decode_spot_place_inclusion(
        interfaces,
        metadata,
        DeepXSpotPlaceExpected {
            params: place,
            order_id: expected.new_order_id,
            extrinsic_index: expected.extrinsic_index,
        },
        event_bytes,
    )?;
    match (cancel_evidence.outcome, place_evidence.outcome) {
        (DeepXSpotCancelInclusionOutcome::Failed, DeepXSpotPlaceInclusionOutcome::Failed) => {
            return Ok(DeepXSpotModifyInclusionOutcome::Failed);
        }
        (DeepXSpotCancelInclusionOutcome::Success, DeepXSpotPlaceInclusionOutcome::Success) => {}
        _ => return Err(DeepXSpotModifyEventError::EventOrder),
    }
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    let mut cancel_index = None;
    let mut place_index = None;
    let place_event = if place.is_buy {
        "StateOrderBuy"
    } else {
        "StateOrderSell"
    };
    for (index, event) in events.iter().enumerate() {
        let event = event.map_err(|e| DeepXSpotModifyEventError::Decode(Box::new(e)))?;
        if event.phase() != Phase::ApplyExtrinsic(expected.extrinsic_index)
            || event.pallet_name() != "SpotMarket"
        {
            continue;
        }
        match event.variant_name() {
            "OrderCancelled" => cancel_index = Some(index),
            name if name == place_event => place_index = Some(index),
            _ => {}
        }
    }
    if !cancel_index
        .zip(place_index)
        .is_some_and(|(cancel, place)| cancel < place)
    {
        return Err(DeepXSpotModifyEventError::EventOrder);
    }
    Ok(DeepXSpotModifyInclusionOutcome::Success)
}

#[cfg(test)]
mod tests {
    use parity_scale_codec::{Compact, Encode};
    use rstest::rstest;
    use serde_json::Value;
    use subxt_core::{events::Phase, ext::scale_value::Value as ScaleValue};

    use super::*;
    use crate::{
        enums::DeepXEnvironment,
        signing::{
            DeepXPostOnlyParam, DeepXSpotCancelParams, DeepXSpotOrderType, DeepXSpotPlaceParams,
            DeepXTimeInForce, DeepXValidatedRuntime, validate_testnet_runtime,
        },
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

    fn record(
        runtime: &DeepXValidatedRuntime,
        pallet_name: &str,
        event_name: &str,
        values: &[ScaleValue<()>],
    ) -> Vec<u8> {
        let pallet = runtime.metadata().pallet_by_name(pallet_name).unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == event_name)
            .unwrap();
        assert_eq!(event.fields.len(), values.len());
        let mut bytes = Phase::ApplyExtrinsic(3).encode();
        bytes.extend([pallet.index(), event.index]);
        for (field, value) in event.fields.iter().zip(values) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                value,
                field.ty.id,
                runtime.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    fn u256(bytes: [u8; 32]) -> ScaleValue<()> {
        ScaleValue::unnamed_composite([ScaleValue::unnamed_composite(
            bytes
                .as_chunks::<8>()
                .0
                .iter()
                .map(|chunk| ScaleValue::u128(u128::from(u64::from_le_bytes(*chunk)))),
        )])
    }

    fn event_bytes(records: &[Vec<u8>]) -> Vec<u8> {
        let mut bytes = Compact(u32::try_from(records.len()).unwrap()).encode();
        for record in records {
            bytes.extend(record);
        }
        bytes
    }

    #[rstest]
    fn spot_modify_requires_ordered_complete_evidence() {
        let runtime = runtime();
        let params = DeepXSpotModifyParams {
            cancel: DeepXSpotCancelParams {
                subaccount: [0x11; 20],
                pair: [0x22; 32],
                order_id: 42,
                is_buy: true,
                fast_cancel: false,
            },
            place: DeepXSpotPlaceParams {
                subaccount: [0x11; 20],
                pair: [0x22; 32],
                is_buy: true,
                quote_amount: [0x33; 32],
                base_amount: [0x44; 32],
                order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc),
                post_only: DeepXPostOnlyParam::None,
                reduce_only: false,
            },
        };
        let expected = DeepXSpotModifyExpected {
            params,
            new_order_id: 43,
            extrinsic_index: 3,
        };
        let cancel = record(
            &runtime,
            "SpotMarket",
            "OrderCancelled",
            &[
                ScaleValue::from_bytes(params.cancel.pair),
                ScaleValue::u128(42),
                ScaleValue::from_bytes(params.cancel.subaccount),
                ScaleValue::bool(true),
                ScaleValue::unnamed_variant("UserCanceled", []),
            ],
        );
        let place = record(
            &runtime,
            "SpotMarket",
            "StateOrderBuy",
            &[ScaleValue::named_composite([
                ("id", ScaleValue::u128(43)),
                ("maker", ScaleValue::from_bytes(params.place.subaccount)),
                ("pair", ScaleValue::from_bytes(params.place.pair)),
                (
                    "price",
                    ScaleValue::unnamed_variant("Some", [u256([1; 32])]),
                ),
                ("quote_amount", u256(params.place.quote_amount)),
                ("base_amount", u256(params.place.base_amount)),
                ("create_time", ScaleValue::u128(43)),
                ("status", ScaleValue::unnamed_variant("Open", [])),
                (
                    "order_type",
                    ScaleValue::unnamed_variant("Limit", [ScaleValue::unnamed_variant("GTC", [])]),
                ),
                ("post_only", ScaleValue::unnamed_variant("None", [])),
                ("reduce_only", ScaleValue::bool(false)),
                ("is_buy", ScaleValue::bool(true)),
            ])],
        );
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
        let success = record(
            &runtime,
            "System",
            "ExtrinsicSuccess",
            std::slice::from_ref(&dispatch_info),
        );
        let failed = record(
            &runtime,
            "System",
            "ExtrinsicFailed",
            &[ScaleValue::unnamed_variant("Other", []), dispatch_info],
        );
        let decode = |bytes: &[u8]| {
            decode_spot_modify_inclusion(runtime.interfaces(), runtime.metadata(), expected, bytes)
        };

        assert!(matches!(
            decode(&event_bytes(&[
                cancel.clone(),
                place.clone(),
                success.clone()
            ])),
            Ok(DeepXSpotModifyInclusionOutcome::Success)
        ));
        assert!(matches!(
            decode(&event_bytes(&[place.clone(), cancel.clone(), success])),
            Err(DeepXSpotModifyEventError::EventOrder)
        ));
        assert!(matches!(
            decode(&event_bytes(&[failed])),
            Ok(DeepXSpotModifyInclusionOutcome::Failed)
        ));
        assert!(matches!(
            decode(&event_bytes(&[cancel, place])),
            Err(DeepXSpotModifyEventError::Cancel(
                DeepXSpotCancelEventError::MalformedEventBytes
            ))
        ));
        let mut fast = expected;
        fast.params.cancel.fast_cancel = true;
        assert!(matches!(
            decode_spot_modify_inclusion(runtime.interfaces(), runtime.metadata(), fast, &[]),
            Err(DeepXSpotModifyEventError::FastCancelUnsupported)
        ));
    }
}
