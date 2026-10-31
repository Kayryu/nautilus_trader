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

//! Block-selected perpetual leverage views from the DeepX precompile.

use alloy::{primitives::Address, sol, sol_types::SolCall};
use parity_scale_codec::{Compact, Decode};
use subxt_core::{
    events::{Events, Phase},
    ext::scale_value::{Composite, Value as ScaleValue, ValueDef},
    metadata::Metadata,
};
use thiserror::Error;

use super::{
    DeepXRpcClient,
    event_decode::{DeepXSystemDispatchOutcome, decode_event_bytes},
    precompile::DEEPX_PERP_MARKET_PRECOMPILE_ADDRESS,
};
use crate::signing::{
    DeepXRuntimeConfig,
    runtime_interface::{DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError},
};

sol! {
    function globalMaxLeverage(address subaccount) external view returns (uint64);
    function perMarketMaxLeverage(address subaccount, uint16 marketId) external view returns (uint64);
    function effectiveLeverage(address subaccount, uint16 marketId) external view returns (uint64);
}

/// Raw leverage caps in units of 1/1000 leverage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpLeverageView {
    /// Global user cap, or zero when the protocol default applies.
    pub global_max_leverage: u64,
    /// Per-market user cap, or zero when no override exists.
    pub per_market_max_leverage: u64,
    /// Runtime-computed minimum of the user caps and protocol market maximum.
    pub effective_leverage: u64,
}

/// Exact terms for one global or per-market leverage-setting event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpLeverageExpected {
    /// Subaccount whose cap was changed.
    pub subaccount: [u8; 20],
    /// Market for an override, or `None` for the global cap.
    pub market_id: Option<u16>,
    /// Requested cap, or `None` when clearing the setting.
    pub max_leverage: Option<u64>,
    /// Index of the signed extrinsic in its finalized block.
    pub extrinsic_index: u32,
}

/// Indexed dispatch outcome proven against an exact leverage event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpLeverageInclusionEvidence {
    /// Index of the signed extrinsic in the block.
    pub extrinsic_index: u32,
    /// Dispatch result consistent with the presence or absence of the business event.
    pub outcome: DeepXSystemDispatchOutcome,
}

/// Errors while verifying a leverage-setting extrinsic from complete event storage.
#[derive(Debug, Error)]
pub enum DeepXPerpLeverageEventError {
    /// Required runtime events are absent.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The runtime event does not have the supported field layout.
    #[error("unsupported DeepX leverage event layout")]
    UnsupportedRuntime,
    /// SCALE decoding failed against the supplied metadata.
    #[error("unable to decode DeepX leverage events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// Complete event storage is malformed or lacks a dispatch event.
    #[error("incomplete DeepX leverage event storage")]
    MalformedEventBytes,
    /// A leverage event has terms different from the submitted call.
    #[error("DeepX leverage event conflicts with expected terms")]
    ConflictingEvent,
    /// More than one dispatch or leverage event exists at the target index.
    #[error("duplicate DeepX leverage event")]
    DuplicateEvent,
    /// Successful dispatch did not emit the expected leverage event.
    #[error("successful DeepX leverage setting has no matching business event")]
    MissingBusinessEvent,
    /// Failed dispatch also emitted a matching leverage event.
    #[error("failed DeepX leverage setting conflicts with a business event")]
    ConflictingBusinessEvent,
}

/// Errors while reading block-selected perpetual leverage views.
#[derive(Debug, Error)]
pub enum DeepXPerpLeverageReadError {
    /// A precompile request failed.
    #[error("DeepX leverage precompile RPC failed: {0}")]
    Rpc(#[source] anyhow::Error),
    /// The precompile did not return one canonical ABI `uint64`.
    #[error("DeepX leverage precompile returned invalid uint64 data")]
    InvalidResponse,
}

/// Reads three leverage views at the same EVM block number.
///
/// Callers must select a canonical block and check its hash again after the reads. Zero in either
/// user cap means the setting is absent; zero effective leverage is returned by the runtime when
/// no valid market leverage can be computed.
///
/// # Errors
///
/// Returns an error for RPC failure or a malformed ABI response.
pub async fn get_perp_leverage_at_block(
    client: &DeepXRpcClient,
    subaccount: [u8; 20],
    market_id: u16,
    block_number: u64,
) -> Result<DeepXPerpLeverageView, DeepXPerpLeverageReadError> {
    let account = Address::from(subaccount);
    let block = format!("0x{block_number:x}");
    let address = DEEPX_PERP_MARKET_PRECOMPILE_ADDRESS.to_string();
    let global = client
        .eth_call(
            &address,
            &globalMaxLeverageCall {
                subaccount: account,
            }
            .abi_encode(),
            &block,
        )
        .await
        .map_err(DeepXPerpLeverageReadError::Rpc)?;
    let per_market = client
        .eth_call(
            &address,
            &perMarketMaxLeverageCall {
                subaccount: account,
                marketId: market_id,
            }
            .abi_encode(),
            &block,
        )
        .await
        .map_err(DeepXPerpLeverageReadError::Rpc)?;
    let effective = client
        .eth_call(
            &address,
            &effectiveLeverageCall {
                subaccount: account,
                marketId: market_id,
            }
            .abi_encode(),
            &block,
        )
        .await
        .map_err(DeepXPerpLeverageReadError::Rpc)?;
    Ok(DeepXPerpLeverageView {
        global_max_leverage: decode_u64(&global)
            .ok_or(DeepXPerpLeverageReadError::InvalidResponse)?,
        per_market_max_leverage: decode_u64(&per_market)
            .ok_or(DeepXPerpLeverageReadError::InvalidResponse)?,
        effective_leverage: decode_u64(&effective)
            .ok_or(DeepXPerpLeverageReadError::InvalidResponse)?,
    })
}

/// Verifies one leverage setting against complete finalized `System.Events` storage.
///
/// The caller separately binds the signed extrinsic, block, runtime, and post-state reads.
///
/// # Errors
///
/// Returns an error for an unsupported event layout, malformed storage, or inconsistent events.
pub fn decode_perp_leverage_inclusion(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    expected: DeepXPerpLeverageExpected,
    event_bytes: &[u8],
) -> Result<DeepXPerpLeverageInclusionEvidence, DeepXPerpLeverageEventError> {
    use DeepXPerpLeverageEventError as Error;

    let event_name = if expected.market_id.is_some() {
        "PerMarketLeverageSet"
    } else {
        "GlobalLeverageSet"
    };
    interfaces.event("PerpMarket", event_name)?;
    interfaces.event("System", "ExtrinsicSuccess")?;
    interfaces.event("System", "ExtrinsicFailed")?;
    let event = metadata
        .pallet_by_name("PerpMarket")
        .and_then(|pallet| pallet.event_variants())
        .and_then(|variants| variants.iter().find(|variant| variant.name == event_name))
        .ok_or(Error::UnsupportedRuntime)?;
    let expected_fields: &[&str] = if expected.market_id.is_some() {
        &["subaccount", "market_id", "max_leverage"]
    } else {
        &["subaccount", "max_leverage"]
    };
    if event.fields.len() != expected_fields.len()
        || !event
            .fields
            .iter()
            .zip(expected_fields)
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
                "ExtrinsicSuccess" => DeepXSystemDispatchOutcome::Success,
                "ExtrinsicFailed" => DeepXSystemDispatchOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "PerpMarket" && event.variant_name() == event_name {
            let fields = event
                .field_values()
                .map_err(|e| Error::Decode(Box::new(e)))?;
            if !leverage_fields_match(&fields, expected) {
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
    let outcome = dispatch.ok_or(Error::MalformedEventBytes)?;
    match (outcome, matched) {
        (DeepXSystemDispatchOutcome::Success, false) => return Err(Error::MissingBusinessEvent),
        (DeepXSystemDispatchOutcome::Failed, true) => return Err(Error::ConflictingBusinessEvent),
        _ => {}
    }
    Ok(DeepXPerpLeverageInclusionEvidence {
        extrinsic_index: expected.extrinsic_index,
        outcome,
    })
}

fn leverage_fields_match(fields: &Composite<u32>, expected: DeepXPerpLeverageExpected) -> bool {
    let Composite::Named(fields) = fields else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    if fields.len() != if expected.market_id.is_some() { 3 } else { 2 }
        || field("subaccount").and_then(decode_event_bytes).as_deref()
            != Some(expected.subaccount.as_slice())
        || field("market_id").and_then(ScaleValue::as_u128) != expected.market_id.map(u128::from)
    {
        return false;
    }
    let Some(value) = field("max_leverage") else {
        return false;
    };
    let ValueDef::Variant(variant) = &value.value else {
        return false;
    };
    let values: Vec<_> = variant.values.values().collect();
    match expected.max_leverage {
        None => variant.name == "None" && values.is_empty(),
        Some(expected) => {
            variant.name == "Some"
                && values.len() == 1
                && values[0].as_u128() == Some(u128::from(expected))
        }
    }
}

fn decode_u64(encoded: &str) -> Option<u64> {
    let bytes = hex::decode(encoded.strip_prefix("0x")?).ok()?;
    let value = bytes.get(24..32)?;
    (bytes.len() == 32 && bytes[..24].iter().all(|byte| *byte == 0))
        .then(|| u64::from_be_bytes(value.try_into().expect("fixed ABI uint64")))
}

#[cfg(test)]
mod tests {
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde_json::Value;

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

    #[rstest]
    fn captured_leverage_event_field_contract() {
        let runtime = runtime();
        let events = runtime
            .metadata()
            .pallet_by_name("PerpMarket")
            .unwrap()
            .event_variants()
            .unwrap();
        for (name, expected) in [
            ("GlobalLeverageSet", &["subaccount", "max_leverage"][..]),
            (
                "PerMarketLeverageSet",
                &["subaccount", "market_id", "max_leverage"][..],
            ),
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
    }

    fn business_record(
        runtime: &DeepXValidatedRuntime,
        expected: DeepXPerpLeverageExpected,
    ) -> Vec<u8> {
        let pallet = runtime.metadata().pallet_by_name("PerpMarket").unwrap();
        let name = if expected.market_id.is_some() {
            "PerMarketLeverageSet"
        } else {
            "GlobalLeverageSet"
        };
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == name)
            .unwrap();
        let mut record = Phase::ApplyExtrinsic(expected.extrinsic_index).encode();
        record.extend([pallet.index(), event.index]);
        let mut values = vec![ScaleValue::from_bytes(expected.subaccount)];
        if let Some(market_id) = expected.market_id {
            values.push(ScaleValue::u128(u128::from(market_id)));
        }
        values.push(match expected.max_leverage {
            Some(leverage) => {
                ScaleValue::unnamed_variant("Some", [ScaleValue::u128(u128::from(leverage))])
            }
            None => ScaleValue::unnamed_variant("None", []),
        });
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
    }

    fn dispatch_record(runtime: &DeepXValidatedRuntime, success: bool, index: u32) -> Vec<u8> {
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
            .find(|event| event.name == name)
            .unwrap();
        let mut record = Phase::ApplyExtrinsic(index).encode();
        record.extend([pallet.index(), event.index]);
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
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        record
    }

    fn event_storage(records: &[Vec<u8>]) -> Vec<u8> {
        let mut bytes = Compact(u32::try_from(records.len()).unwrap()).encode();
        for record in records {
            bytes.extend(record);
        }
        bytes
    }

    #[rstest]
    #[case(None, Some(10_000))]
    #[case(Some(3), Some(3_000))]
    #[case(Some(3), None)]
    fn leverage_inclusion_binds_set_and_clear_events(
        #[case] market_id: Option<u16>,
        #[case] max_leverage: Option<u64>,
    ) {
        let runtime = runtime();
        let expected = DeepXPerpLeverageExpected {
            subaccount: [0x11; 20],
            market_id,
            max_leverage,
            extrinsic_index: 4,
        };
        let bytes = event_storage(&[
            business_record(&runtime, expected),
            dispatch_record(&runtime, true, 4),
        ]);
        let evidence = decode_perp_leverage_inclusion(
            runtime.interfaces(),
            runtime.metadata(),
            expected,
            &bytes,
        )
        .unwrap();
        assert_eq!(evidence.extrinsic_index, 4);
        assert_eq!(evidence.outcome, DeepXSystemDispatchOutcome::Success);
    }

    #[rstest]
    fn leverage_inclusion_rejects_conflicting_terms() {
        let runtime = runtime();
        let expected = DeepXPerpLeverageExpected {
            subaccount: [0x11; 20],
            market_id: Some(3),
            max_leverage: Some(3_000),
            extrinsic_index: 4,
        };
        let conflicting = DeepXPerpLeverageExpected {
            max_leverage: Some(4_000),
            ..expected
        };
        let bytes = event_storage(&[
            business_record(&runtime, conflicting),
            dispatch_record(&runtime, true, 4),
        ]);
        assert!(matches!(
            decode_perp_leverage_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                expected,
                &bytes
            ),
            Err(DeepXPerpLeverageEventError::ConflictingEvent)
        ));
    }

    #[rstest]
    fn leverage_inclusion_rejects_missing_and_duplicate_business_events() {
        let runtime = runtime();
        let expected = DeepXPerpLeverageExpected {
            subaccount: [0x11; 20],
            market_id: Some(3),
            max_leverage: None,
            extrinsic_index: 4,
        };
        let dispatch = dispatch_record(&runtime, true, 4);
        let business = business_record(&runtime, expected);
        assert!(matches!(
            decode_perp_leverage_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                expected,
                &event_storage(std::slice::from_ref(&dispatch)),
            ),
            Err(DeepXPerpLeverageEventError::MissingBusinessEvent)
        ));
        assert!(matches!(
            decode_perp_leverage_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                expected,
                &event_storage(&[business.clone(), business, dispatch]),
            ),
            Err(DeepXPerpLeverageEventError::DuplicateEvent)
        ));
    }

    #[rstest]
    fn leverage_inclusion_distinguishes_failed_dispatch_from_conflicting_event() {
        let runtime = runtime();
        let expected = DeepXPerpLeverageExpected {
            subaccount: [0x11; 20],
            market_id: Some(3),
            max_leverage: Some(3_000),
            extrinsic_index: 4,
        };
        let failed = dispatch_record(&runtime, false, 4);
        let evidence = decode_perp_leverage_inclusion(
            runtime.interfaces(),
            runtime.metadata(),
            expected,
            &event_storage(std::slice::from_ref(&failed)),
        )
        .unwrap();
        assert_eq!(evidence.outcome, DeepXSystemDispatchOutcome::Failed);
        assert!(matches!(
            decode_perp_leverage_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                expected,
                &event_storage(&[business_record(&runtime, expected), failed]),
            ),
            Err(DeepXPerpLeverageEventError::ConflictingBusinessEvent)
        ));
    }

    #[rstest]
    fn decodes_only_canonical_uint64() {
        assert_eq!(
            decode_u64(&format!("0x{}0000000000002710", "00".repeat(24))),
            Some(10_000)
        );
        assert_eq!(decode_u64(&format!("0x01{}", "00".repeat(31))), None);
        assert_eq!(decode_u64("0x0000"), None);
        assert_eq!(decode_u64("0x"), None);
    }
}
