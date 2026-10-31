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

//! Indexed subaccount creation and deletion events from complete finalized storage.

use parity_scale_codec::{Compact, Decode};
use subxt_core::{
    events::{Events, Phase},
    ext::scale_value::Composite,
    metadata::Metadata,
};
use thiserror::Error;

use super::event_decode::{DeepXSystemDispatchOutcome, decode_event_bytes};
use crate::signing::{
    DeepXRuntimeConfig,
    runtime_interface::{DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError},
};

/// Exact terms of one submitted subaccount operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXOnboardingExpected<'a> {
    /// Creation by this owner with this name; the event supplies the new address.
    Create { owner: [u8; 20], name: &'a [u8] },
    /// Deletion of this subaccount.
    Delete { subaccount: [u8; 20] },
}

/// Indexed result of one finalized subaccount operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXOnboardingInclusionEvidence {
    /// Index of the signed extrinsic.
    pub extrinsic_index: u32,
    /// Runtime dispatch outcome.
    pub outcome: DeepXSystemDispatchOutcome,
    /// Address emitted by successful creation or deletion.
    pub subaccount: Option<[u8; 20]>,
}

/// Errors while verifying complete subaccount event storage.
#[derive(Debug, Error)]
pub enum DeepXOnboardingEventError {
    /// Required runtime events are absent.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The event layout differs from the supported runtime.
    #[error("unsupported DeepX subaccount event layout")]
    UnsupportedRuntime,
    /// SCALE decoding failed against the supplied metadata.
    #[error("unable to decode DeepX subaccount events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// Complete event storage or its dispatch event is absent or malformed.
    #[error("incomplete DeepX subaccount event storage")]
    MalformedEventBytes,
    /// Business event fields disagree with the submitted operation.
    #[error("DeepX subaccount event conflicts with expected terms")]
    ConflictingEvent,
    /// More than one business or dispatch event exists at the target index.
    #[error("duplicate DeepX subaccount event")]
    DuplicateEvent,
    /// Successful dispatch lacks its business event.
    #[error("successful DeepX subaccount operation has no business event")]
    MissingBusinessEvent,
    /// Failed dispatch emitted its business event.
    #[error("failed DeepX subaccount operation emitted a business event")]
    ConflictingBusinessEvent,
}

/// Verifies one operation against complete finalized `System.Events` storage.
///
/// The caller separately binds the signed bytes, block, runtime, and event storage.
///
/// # Errors
///
/// Returns an error for unsupported metadata, malformed storage, or inconsistent events.
pub fn decode_onboarding_inclusion(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    extrinsic_index: u32,
    expected: DeepXOnboardingExpected<'_>,
    event_bytes: &[u8],
) -> Result<DeepXOnboardingInclusionEvidence, DeepXOnboardingEventError> {
    use DeepXOnboardingEventError as Error;
    let (name, field_names): (&str, &[&str]) = match expected {
        DeepXOnboardingExpected::Create { .. } => {
            ("NewUserRecord", &["owner", "subaccount", "name"])
        }
        DeepXOnboardingExpected::Delete { .. } => ("SubaccountDeleted", &["subaccount"]),
    };
    interfaces.event("Subaccount", name)?;
    interfaces.event("System", "ExtrinsicSuccess")?;
    interfaces.event("System", "ExtrinsicFailed")?;
    let variant = metadata
        .pallet_by_name("Subaccount")
        .and_then(|pallet| pallet.event_variants())
        .and_then(|variants| variants.iter().find(|variant| variant.name == name))
        .ok_or(Error::UnsupportedRuntime)?;
    if variant.fields.len() != field_names.len()
        || !variant
            .fields
            .iter()
            .zip(field_names)
            .all(|(field, name)| field.name.as_deref() == Some(*name))
    {
        return Err(Error::UnsupportedRuntime);
    }
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| Error::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut dispatch = None;
    let mut subaccount = None;
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    for event in events.iter() {
        let event = event.map_err(|error| Error::Decode(Box::new(error)))?;
        count = count.checked_add(1).ok_or(Error::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(Error::MalformedEventBytes)?;
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index) {
            continue;
        }
        if event.pallet_name() == "System" {
            let outcome = match event.variant_name() {
                "ExtrinsicSuccess" => DeepXSystemDispatchOutcome::Success,
                "ExtrinsicFailed" => DeepXSystemDispatchOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "Subaccount" && event.variant_name() == name {
            let fields = event
                .field_values()
                .map_err(|error| Error::Decode(Box::new(error)))?;
            let address = onboarding_fields(&fields, expected).ok_or(Error::ConflictingEvent)?;
            if subaccount.replace(address).is_some() {
                return Err(Error::DuplicateEvent);
            }
        }
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(Error::MalformedEventBytes);
    }
    let outcome = dispatch.ok_or(Error::MalformedEventBytes)?;
    match (outcome, subaccount) {
        (DeepXSystemDispatchOutcome::Success, None) => return Err(Error::MissingBusinessEvent),
        (DeepXSystemDispatchOutcome::Failed, Some(_)) => {
            return Err(Error::ConflictingBusinessEvent);
        }
        _ => {}
    }
    Ok(DeepXOnboardingInclusionEvidence {
        extrinsic_index,
        outcome,
        subaccount,
    })
}

fn onboarding_fields(
    fields: &Composite<u32>,
    expected: DeepXOnboardingExpected<'_>,
) -> Option<[u8; 20]> {
    let Composite::Named(fields) = fields else {
        return None;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
            .and_then(decode_event_bytes)
    };
    let address: [u8; 20] = field("subaccount")?.try_into().ok()?;
    match expected {
        DeepXOnboardingExpected::Create { owner, name }
            if fields.len() == 3
                && field("owner").as_deref() == Some(owner.as_slice())
                && field("name").as_deref() == Some(name) =>
        {
            Some(address)
        }
        DeepXOnboardingExpected::Delete { subaccount }
            if fields.len() == 1 && address == subaccount =>
        {
            Some(address)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enums::DeepXEnvironment, signing::validate_testnet_runtime};
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde_json::Value;
    use subxt_core::ext::scale_value::Value as ScaleValue;

    #[rstest]
    fn captured_onboarding_event_field_contract() {
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
        let runtime = validate_testnet_runtime(
            &DeepXEnvironment::Testnet,
            hex::decode("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap()
                .try_into()
                .unwrap(),
            376,
            1,
            &metadata,
        )
        .unwrap();
        let events = runtime
            .metadata()
            .pallet_by_name("Subaccount")
            .unwrap()
            .event_variants()
            .unwrap();
        for (name, expected) in [
            ("NewUserRecord", &["owner", "subaccount", "name"][..]),
            ("SubaccountDeleted", &["subaccount"][..]),
        ] {
            let event = events.iter().find(|event| event.name == name).unwrap();
            assert_eq!(
                event
                    .fields
                    .iter()
                    .map(|field| field.name.as_deref().unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
        }

        let record = |pallet_name: &str, event_name: &str, values: Vec<ScaleValue<()>>| {
            let pallet = runtime.metadata().pallet_by_name(pallet_name).unwrap();
            let event = pallet
                .event_variants()
                .unwrap()
                .iter()
                .find(|event| event.name == event_name)
                .unwrap();
            let mut record = Phase::ApplyExtrinsic(4).encode();
            record.extend([pallet.index(), event.index]);
            for (field, value) in event.fields.iter().zip(values) {
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
        };
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
        let dispatch = record("System", "ExtrinsicSuccess", vec![dispatch_info.clone()]);
        let failed_dispatch = record(
            "System",
            "ExtrinsicFailed",
            vec![ScaleValue::unnamed_variant("Other", []), dispatch_info],
        );
        for (name, values, expected) in [
            (
                "NewUserRecord",
                vec![
                    ScaleValue::from_bytes([1; 20]),
                    ScaleValue::from_bytes([2; 20]),
                    ScaleValue::from_bytes(b"first"),
                ],
                DeepXOnboardingExpected::Create {
                    owner: [1; 20],
                    name: b"first",
                },
            ),
            (
                "SubaccountDeleted",
                vec![ScaleValue::from_bytes([2; 20])],
                DeepXOnboardingExpected::Delete {
                    subaccount: [2; 20],
                },
            ),
        ] {
            let business = record("Subaccount", name, values);
            let storage = |records: &[&Vec<u8>]| {
                let mut bytes = Compact(u32::try_from(records.len()).unwrap()).encode();
                for record in records {
                    bytes.extend(*record);
                }
                bytes
            };
            let valid = storage(&[&business, &dispatch]);
            let evidence = decode_onboarding_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                4,
                expected,
                &valid,
            )
            .unwrap();
            assert_eq!(evidence.outcome, DeepXSystemDispatchOutcome::Success);
            assert_eq!(evidence.subaccount, Some([2; 20]));
            let duplicate = storage(&[&business, &business, &dispatch]);
            assert!(matches!(
                decode_onboarding_inclusion(
                    runtime.interfaces(),
                    runtime.metadata(),
                    4,
                    expected,
                    &duplicate
                ),
                Err(DeepXOnboardingEventError::DuplicateEvent)
            ));
            let missing = storage(&[&dispatch]);
            assert!(matches!(
                decode_onboarding_inclusion(
                    runtime.interfaces(),
                    runtime.metadata(),
                    4,
                    expected,
                    &missing
                ),
                Err(DeepXOnboardingEventError::MissingBusinessEvent)
            ));
            let failed = storage(&[&failed_dispatch]);
            let evidence = decode_onboarding_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                4,
                expected,
                &failed,
            )
            .unwrap();
            assert_eq!(evidence.outcome, DeepXSystemDispatchOutcome::Failed);
            assert_eq!(evidence.subaccount, None);
            let contradictory = storage(&[&business, &failed_dispatch]);
            assert!(matches!(
                decode_onboarding_inclusion(
                    runtime.interfaces(),
                    runtime.metadata(),
                    4,
                    expected,
                    &contradictory
                ),
                Err(DeepXOnboardingEventError::ConflictingBusinessEvent)
            ));
            let conflicting = match expected {
                DeepXOnboardingExpected::Create { name, .. } => DeepXOnboardingExpected::Create {
                    owner: [3; 20],
                    name,
                },
                DeepXOnboardingExpected::Delete { .. } => DeepXOnboardingExpected::Delete {
                    subaccount: [3; 20],
                },
            };
            assert!(matches!(
                decode_onboarding_inclusion(
                    runtime.interfaces(),
                    runtime.metadata(),
                    4,
                    conflicting,
                    &valid
                ),
                Err(DeepXOnboardingEventError::ConflictingEvent)
            ));
            let mut truncated = valid.clone();
            truncated.pop();
            assert!(
                decode_onboarding_inclusion(
                    runtime.interfaces(),
                    runtime.metadata(),
                    4,
                    expected,
                    &truncated
                )
                .is_err()
            );
        }
    }
}
