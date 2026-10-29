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

//! Exact Spot cancellation inclusion from complete DeepX runtime events.

use parity_scale_codec::{Compact, Decode};
use subxt_core::{
    events::{Events, Phase},
    ext::scale_value::{Composite, Value as ScaleValue, ValueDef},
    metadata::Metadata,
};
use thiserror::Error;

use super::event_decode::decode_event_bytes;
use crate::signing::{
    DeepXRuntimeConfig,
    runtime_interface::{DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError},
};

/// Durable terms expected from one ordinary Spot cancellation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSpotCancelExpected {
    /// Account which owns the order.
    pub subaccount: [u8; 20],
    /// Spot market pair identifier.
    pub pair: [u8; 32],
    /// Canceled order identifier.
    pub order_id: u64,
    /// Side of the canceled order.
    pub is_buy: bool,
    /// Index of the signed extrinsic in the block.
    pub extrinsic_index: u32,
}

/// Outcome established from indexed dispatch and Spot business events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXSpotCancelInclusionOutcome {
    /// Dispatch succeeded and the matching cancellation event was emitted.
    Success,
    /// Dispatch failed without applying the cancellation.
    Failed,
}

/// Indexed inclusion evidence for an ordinary Spot cancellation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSpotCancelInclusionEvidence {
    /// Index of the signed extrinsic in the block.
    pub extrinsic_index: u32,
    /// Authoritative inclusion outcome at that index.
    pub outcome: DeepXSpotCancelInclusionOutcome,
}

/// Errors while binding complete runtime events to a Spot cancellation.
#[derive(Debug, Error)]
pub enum DeepXSpotCancelEventError {
    /// Required runtime events are absent from the approved interface.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE decoding failed against the supplied metadata.
    #[error("unable to decode DeepX Spot cancellation events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// Event storage is incomplete, or no dispatch event exists at the target index.
    #[error("incomplete DeepX Spot cancellation event storage")]
    MalformedEventBytes,
    /// Cancellation fields do not match the expected order.
    #[error("DeepX Spot cancellation event conflicts with expected terms")]
    ConflictingEvent,
    /// Dispatch or cancellation was emitted more than once at the target index.
    #[error("duplicate DeepX Spot cancellation event")]
    DuplicateEvent,
    /// Successful dispatch did not emit a matching cancellation event.
    #[error("successful DeepX Spot cancellation has no matching business event")]
    MissingBusinessEvent,
    /// A failed dispatch also emitted a matching cancellation event.
    #[error("failed DeepX Spot cancellation conflicts with a business event")]
    ConflictingBusinessEvent,
}

/// Verifies one ordinary Spot cancellation against complete `System.Events` storage.
///
/// The caller verifies the signed operation and block canonicality. Fast cancellation is not
/// supported because its event contract is not approved.
///
/// # Errors
///
/// Returns an error for unsupported runtime events, incomplete or conflicting storage, or an
/// impossible dispatch and business-event combination.
pub fn decode_spot_cancel_inclusion(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    expected: DeepXSpotCancelExpected,
    event_bytes: &[u8],
) -> Result<DeepXSpotCancelInclusionEvidence, DeepXSpotCancelEventError> {
    use DeepXSpotCancelEventError as Error;

    interfaces.event("SpotMarket", "OrderCancelled")?;
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
                "ExtrinsicSuccess" => DeepXSpotCancelInclusionOutcome::Success,
                "ExtrinsicFailed" => DeepXSpotCancelInclusionOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "SpotMarket" && event.variant_name() == "OrderCancelled" {
            let fields = event
                .field_values()
                .map_err(|e| Error::Decode(Box::new(e)))?;
            if !spot_cancel_fields_match(&fields, expected) {
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
        (DeepXSpotCancelInclusionOutcome::Success, true) => {
            DeepXSpotCancelInclusionOutcome::Success
        }
        (DeepXSpotCancelInclusionOutcome::Failed, false) => DeepXSpotCancelInclusionOutcome::Failed,
        (DeepXSpotCancelInclusionOutcome::Success, false) => {
            return Err(Error::MissingBusinessEvent);
        }
        (DeepXSpotCancelInclusionOutcome::Failed, true) => {
            return Err(Error::ConflictingBusinessEvent);
        }
    };
    Ok(DeepXSpotCancelInclusionEvidence {
        extrinsic_index: expected.extrinsic_index,
        outcome,
    })
}

fn spot_cancel_fields_match(fields: &Composite<u32>, expected: DeepXSpotCancelExpected) -> bool {
    let Composite::Named(fields) = fields else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    fields.len() == 5
        && field("pair").and_then(decode_event_bytes).as_deref() == Some(expected.pair.as_slice())
        && field("maker").and_then(decode_event_bytes).as_deref()
            == Some(expected.subaccount.as_slice())
        && field("order_id").and_then(ScaleValue::as_u128) == Some(u128::from(expected.order_id))
        && field("is_buy").and_then(ScaleValue::as_bool) == Some(expected.is_buy)
        && field("reason").is_some_and(|value| {
            matches!(&value.value, ValueDef::Variant(reason)
                if reason.name == "UserCanceled" && reason.values.values().next().is_none())
        })
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

    fn spot_cancel_record(
        runtime: &crate::signing::DeepXValidatedRuntime,
        expected: DeepXSpotCancelExpected,
    ) -> Vec<u8> {
        let pallet = runtime.metadata().pallet_by_name("SpotMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == "OrderCancelled")
            .unwrap();
        assert_eq!(
            event
                .fields
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["pair", "order_id", "maker", "is_buy", "reason"]
        );
        let values = [
            ScaleValue::from_bytes(expected.pair),
            ScaleValue::u128(u128::from(expected.order_id)),
            ScaleValue::from_bytes(expected.subaccount),
            ScaleValue::bool(expected.is_buy),
            ScaleValue::unnamed_variant("UserCanceled", []),
        ];
        let mut bytes = Phase::ApplyExtrinsic(expected.extrinsic_index).encode();
        bytes.extend([pallet.index(), event.index]);
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
    fn spot_cancel_binds_exact_order_and_dispatch() {
        let runtime = runtime();
        let expected = DeepXSpotCancelExpected {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            order_id: 7,
            is_buy: true,
            extrinsic_index: 3,
        };
        let business = spot_cancel_record(&runtime, expected);
        let success = dispatch_record(&runtime, true);
        let failed = dispatch_record(&runtime, false);
        let decode = |terms, bytes: &[u8]| {
            decode_spot_cancel_inclusion(runtime.interfaces(), runtime.metadata(), terms, bytes)
        };

        assert!(matches!(
            decode(expected, &events(&[success.clone(), business.clone()])),
            Ok(DeepXSpotCancelInclusionEvidence {
                extrinsic_index: 3,
                outcome: DeepXSpotCancelInclusionOutcome::Success,
            }),
        ));
        assert!(matches!(
            decode(expected, &events(std::slice::from_ref(&failed))),
            Ok(DeepXSpotCancelInclusionEvidence {
                extrinsic_index: 3,
                outcome: DeepXSpotCancelInclusionOutcome::Failed,
            }),
        ));
        for terms in [
            DeepXSpotCancelExpected {
                subaccount: [0x33; 20],
                ..expected
            },
            DeepXSpotCancelExpected {
                pair: [0x33; 32],
                ..expected
            },
            DeepXSpotCancelExpected {
                order_id: 8,
                ..expected
            },
            DeepXSpotCancelExpected {
                is_buy: false,
                ..expected
            },
        ] {
            assert!(matches!(
                decode(terms, &events(&[success.clone(), business.clone()])),
                Err(DeepXSpotCancelEventError::ConflictingEvent),
            ));
        }
        assert!(matches!(
            decode(expected, &events(std::slice::from_ref(&success))),
            Err(DeepXSpotCancelEventError::MissingBusinessEvent),
        ));
        assert!(matches!(
            decode(expected, &events(&[failed, business.clone()])),
            Err(DeepXSpotCancelEventError::ConflictingBusinessEvent),
        ));
        assert!(matches!(
            decode(expected, &events(&[success, business.clone(), business])),
            Err(DeepXSpotCancelEventError::DuplicateEvent),
        ));
        assert!(matches!(
            decode(expected, &[0, 0]),
            Err(DeepXSpotCancelEventError::MalformedEventBytes),
        ));
    }
}
