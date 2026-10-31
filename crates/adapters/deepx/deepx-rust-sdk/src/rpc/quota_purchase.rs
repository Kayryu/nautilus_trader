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

//! Exact quota purchase price and finalized event evidence.

use parity_scale_codec::{Compact, Decode, DecodeAll};
use scale_info::{TypeDef, TypeDefPrimitive};
use subxt_core::{
    events::{Events, Phase},
    ext::scale_value::{Composite, Value as ScaleValue, ValueDef},
    metadata::Metadata,
};
use thiserror::Error;

use super::{DeepXRpcClient, event_decode::decode_event_bytes};
use crate::signing::{
    DeepXBuyQuotaParams, DeepXRuntimeConfig,
    runtime_interface::{DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError},
};

/// Exact terms of a signed `Lending.buy_quota` call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXBuyQuotaExpected {
    /// Complete call arguments.
    pub params: DeepXBuyQuotaParams,
    /// AccountId20 signer of the extrinsic.
    pub signer: [u8; 20],
    /// Index of the signed extrinsic in its finalized block.
    pub extrinsic_index: u32,
}

/// Dispatch outcome bound to an exact `Lending.BuyQuota` event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXBuyQuotaInclusionOutcome {
    /// Purchase succeeded and emitted the matching quota event.
    Success,
    /// Dispatch failed without emitting a purchase event.
    Failed,
}

/// Errors while binding quota purchase events to one extrinsic.
#[derive(Debug, Error)]
pub enum DeepXBuyQuotaEventError {
    /// The approved runtime interface lacks a required event.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE event decoding failed.
    #[error("unable to decode DeepX quota purchase events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// Complete event storage or the dispatch event is missing.
    #[error("incomplete DeepX quota purchase event storage")]
    MalformedEventBytes,
    /// The purchase event disagrees with signed call terms.
    #[error("DeepX quota purchase event conflicts with signed terms")]
    ConflictingEvent,
    /// More than one dispatch or purchase event was emitted at the target index.
    #[error("duplicate DeepX quota purchase event")]
    DuplicateEvent,
    /// A successful dispatch omitted the purchase event.
    #[error("successful DeepX quota purchase has no matching business event")]
    MissingBusinessEvent,
    /// A failed dispatch emitted a purchase event.
    #[error("failed DeepX quota purchase conflicts with a business event")]
    ConflictingBusinessEvent,
}

/// Verifies one quota purchase against complete finalized `System.Events` storage.
///
/// The caller verifies the exact signed extrinsic, runtime metadata, and block canonicality.
///
/// # Errors
///
/// Returns an error for unsupported, incomplete, duplicate, or conflicting events.
pub fn decode_buy_quota_inclusion(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    expected: DeepXBuyQuotaExpected,
    event_bytes: &[u8],
) -> Result<DeepXBuyQuotaInclusionOutcome, DeepXBuyQuotaEventError> {
    use DeepXBuyQuotaEventError as Error;

    interfaces.event("Lending", "BuyQuota")?;
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
                "ExtrinsicSuccess" => DeepXBuyQuotaInclusionOutcome::Success,
                "ExtrinsicFailed" => DeepXBuyQuotaInclusionOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "Lending" && event.variant_name() == "BuyQuota" {
            let fields = event
                .field_values()
                .map_err(|e| Error::Decode(Box::new(e)))?;
            if !buy_quota_fields_match(&fields, expected) {
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
    match (dispatch.ok_or(Error::MalformedEventBytes)?, matched) {
        (DeepXBuyQuotaInclusionOutcome::Success, true) => {
            Ok(DeepXBuyQuotaInclusionOutcome::Success)
        }
        (DeepXBuyQuotaInclusionOutcome::Failed, false) => Ok(DeepXBuyQuotaInclusionOutcome::Failed),
        (DeepXBuyQuotaInclusionOutcome::Success, false) => Err(Error::MissingBusinessEvent),
        (DeepXBuyQuotaInclusionOutcome::Failed, true) => Err(Error::ConflictingBusinessEvent),
    }
}

fn buy_quota_fields_match(fields: &Composite<u32>, expected: DeepXBuyQuotaExpected) -> bool {
    let Composite::Named(fields) = fields else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    let buyer = match expected.params.from_subaccount {
        Some(account) => ("Subaccount", account),
        None => ("Wallet", expected.signer),
    };
    fields.len() == 3
        && field("buyer").is_some_and(|value| {
            matches!(&value.value, ValueDef::Variant(variant)
                if variant.name == buyer.0
                    && variant.values.values().next().and_then(decode_event_bytes).as_deref()
                        == Some(buyer.1.as_slice())
                    && variant.values.values().count() == 1)
        })
        && field("to").and_then(decode_event_bytes).as_deref()
            == Some(expected.params.address.as_slice())
        && field("quota").and_then(ScaleValue::as_u128) == Some(u128::from(expected.params.quota))
}

/// Returns the storage key for the plain `Lending.QuoteAmountPerQuota` value.
#[must_use]
pub fn quote_amount_per_quota_storage_key() -> String {
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(&sp_crypto_hashing::twox_128(b"Lending"));
    key.extend_from_slice(&sp_crypto_hashing::twox_128(b"QuoteAmountPerQuota"));
    format!("0x{}", hex::encode(key))
}

/// Errors while reading the runtime's raw per-quota purchase price.
#[derive(Debug, Error)]
pub enum DeepXQuotaPriceReadError {
    /// Block-selected storage RPC failed.
    #[error("DeepX quota price storage RPC failed: {0}")]
    Rpc(#[source] anyhow::Error),
    /// Metadata does not expose the expected `u128` plain storage value.
    #[error("unsupported DeepX quota price storage layout")]
    UnsupportedRuntime,
    /// The storage value was missing or malformed.
    #[error("DeepX quota price storage is invalid")]
    InvalidStorage,
}

/// Reads the exact quote-asset base units charged for one quota at a block hash.
///
/// An absent storage value uses the runtime metadata's SCALE default.
///
/// # Errors
///
/// Returns an error for RPC failure, unsupported metadata, or malformed or zero runtime storage.
pub async fn get_quote_amount_per_quota_at_block(
    client: &DeepXRpcClient,
    metadata: &Metadata,
    block_hash: &str,
) -> Result<u128, DeepXQuotaPriceReadError> {
    let storage = metadata
        .pallet_by_name("Lending")
        .and_then(|pallet| pallet.storage())
        .and_then(|storage| storage.entry_by_name("QuoteAmountPerQuota"))
        .ok_or(DeepXQuotaPriceReadError::UnsupportedRuntime)?;
    if !matches!(
        metadata
            .types()
            .resolve(storage.entry_type().value_ty())
            .map(|ty| &ty.type_def),
        Some(TypeDef::Primitive(TypeDefPrimitive::U128))
    ) {
        return Err(DeepXQuotaPriceReadError::UnsupportedRuntime);
    }
    let encoded = client
        .get_storage(&quote_amount_per_quota_storage_key(), block_hash)
        .await
        .map_err(DeepXQuotaPriceReadError::Rpc)?;
    let bytes = match encoded {
        Some(encoded) => encoded
            .strip_prefix("0x")
            .and_then(|value| hex::decode(value).ok())
            .ok_or(DeepXQuotaPriceReadError::InvalidStorage)?,
        None => storage.default_bytes().to_vec(),
    };
    decode_quote_amount_per_quota(&bytes).ok_or(DeepXQuotaPriceReadError::InvalidStorage)
}

fn decode_quote_amount_per_quota(bytes: &[u8]) -> Option<u128> {
    let amount = u128::decode_all(&mut &bytes[..]).ok()?;
    (amount > 0).then_some(amount)
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

    fn events(records: &[Vec<u8>]) -> Vec<u8> {
        let mut bytes = Compact(u32::try_from(records.len()).unwrap()).encode();
        for record in records {
            bytes.extend(record);
        }
        bytes
    }

    fn dispatch(runtime: &DeepXValidatedRuntime, success: bool) -> Vec<u8> {
        let info = ScaleValue::named_composite([
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
        if success {
            record(runtime, "System", "ExtrinsicSuccess", &[info])
        } else {
            record(
                runtime,
                "System",
                "ExtrinsicFailed",
                &[ScaleValue::unnamed_variant("Other", []), info],
            )
        }
    }

    fn purchase(runtime: &DeepXValidatedRuntime, expected: DeepXBuyQuotaExpected) -> Vec<u8> {
        let (buyer_name, payer) = match expected.params.from_subaccount {
            Some(payer) => ("Subaccount", payer),
            None => ("Wallet", expected.signer),
        };
        record(
            runtime,
            "Lending",
            "BuyQuota",
            &[
                ScaleValue::unnamed_variant(buyer_name, [ScaleValue::from_bytes(payer)]),
                ScaleValue::from_bytes(expected.params.address),
                ScaleValue::u128(u128::from(expected.params.quota)),
            ],
        )
    }

    #[rstest]
    fn buy_quota_binds_payer_beneficiary_and_dispatch() {
        let runtime = runtime();
        let mut expected = DeepXBuyQuotaExpected {
            params: DeepXBuyQuotaParams {
                address: [0x11; 20],
                quota: 10,
                from_subaccount: None,
            },
            signer: [0x22; 20],
            extrinsic_index: 3,
        };
        let decode = |expected, bytes: &[u8]| {
            decode_buy_quota_inclusion(runtime.interfaces(), runtime.metadata(), expected, bytes)
        };
        let success = dispatch(&runtime, true);
        let failed = dispatch(&runtime, false);
        for payer in [None, Some([0x33; 20])] {
            expected.params.from_subaccount = payer;
            let business = purchase(&runtime, expected);
            assert!(matches!(
                decode(expected, &events(&[business.clone(), success.clone()])),
                Ok(DeepXBuyQuotaInclusionOutcome::Success)
            ));
            assert!(matches!(
                decode(expected, &events(std::slice::from_ref(&failed))),
                Ok(DeepXBuyQuotaInclusionOutcome::Failed)
            ));
            assert!(matches!(
                decode(expected, &events(std::slice::from_ref(&success))),
                Err(DeepXBuyQuotaEventError::MissingBusinessEvent)
            ));
            assert!(matches!(
                decode(expected, &events(&[business.clone(), failed.clone()])),
                Err(DeepXBuyQuotaEventError::ConflictingBusinessEvent)
            ));
            assert!(matches!(
                decode(
                    expected,
                    &events(&[business.clone(), business.clone(), success.clone()])
                ),
                Err(DeepXBuyQuotaEventError::DuplicateEvent)
            ));
            let mut wrong = expected;
            wrong.params.quota += 1;
            assert!(matches!(
                decode(wrong, &events(&[business.clone(), success.clone()])),
                Err(DeepXBuyQuotaEventError::ConflictingEvent)
            ));
            wrong = expected;
            wrong.params.address = [0x44; 20];
            assert!(matches!(
                decode(wrong, &events(&[business.clone(), success.clone()])),
                Err(DeepXBuyQuotaEventError::ConflictingEvent)
            ));
            wrong = expected;
            wrong.params.from_subaccount = if payer.is_some() {
                None
            } else {
                Some([0x33; 20])
            };
            assert!(matches!(
                decode(wrong, &events(&[business, success.clone()])),
                Err(DeepXBuyQuotaEventError::ConflictingEvent)
            ));
        }
        assert!(matches!(
            decode(expected, &[0, 0]),
            Err(DeepXBuyQuotaEventError::MalformedEventBytes)
        ));
    }

    #[rstest]
    fn quota_price_key_matches_captured_storage_type() {
        let runtime = runtime();
        let storage = runtime
            .metadata()
            .pallet_by_name("Lending")
            .unwrap()
            .storage()
            .unwrap()
            .entry_by_name("QuoteAmountPerQuota")
            .unwrap();
        assert_eq!(storage.name(), "QuoteAmountPerQuota");
        assert!(matches!(
            runtime
                .metadata()
                .types()
                .resolve(storage.entry_type().value_ty())
                .map(|ty| &ty.type_def),
            Some(scale_info::TypeDef::Primitive(
                scale_info::TypeDefPrimitive::U128
            ))
        ));
        assert_eq!(quote_amount_per_quota_storage_key().len(), 66);
        assert_eq!(
            decode_quote_amount_per_quota(storage.default_bytes()),
            Some(500)
        );
        assert_eq!(
            decode_quote_amount_per_quota(&500_u128.to_le_bytes()),
            Some(500)
        );
        assert_eq!(decode_quote_amount_per_quota(&0_u128.to_le_bytes()), None);
        assert_eq!(decode_quote_amount_per_quota(&[1]), None);
        assert_eq!(
            decode_quote_amount_per_quota(&[500_u128.to_le_bytes().as_slice(), &[0]].concat()),
            None
        );
    }
}
