// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Fail-closed DeepX canonical block and submission-pool observations.

use nautilus_blockchain::rpc::http::BlockchainHttpRpcClient;
use nautilus_core::hex;
#[cfg(test)]
use parity_scale_codec::Encode;
use parity_scale_codec::{Compact, Decode};
use serde::Deserialize;
use serde_json::json;
use subxt_core::config::{Hasher, substrate::BlakeTwo256};
use subxt_core::events::{Events, Phase};
use subxt_core::ext::scale_value::{Composite, Value as ScaleValue, ValueDef};
use thiserror::Error;

use super::{
    DeepXBusinessEventOutcome, DeepXCanonicalBlockEvidence, DeepXDispatchOutcome,
    DeepXInclusionEvidence, DeepXInclusionEvidenceError, DeepXIndexedOutcome,
    DeepXMissedBlockScanPlan, DeepXRecoveryScan, DeepXRecoveryScanCollectionError,
    DeepXRecoveryScanCollector, DeepXRecoveryScanPlanError, DeepXReorganizationDecision,
    DeepXSubmissionPoolEvidence, DeepXTransactionIdentity, DeepXTransactionOperation,
    classify_reorganization, plan_missed_block_scan,
};
use crate::{
    config::{DeepXRpcRole, DeepXValidatedRpcEndpoints},
    rpc::{
        DEEPX_RECOVERY_RPC_METHODS, DEEPX_SUBMISSION_RPC_METHODS, DEEPX_WATCH_RPC_METHODS,
        DeepXValidatedRpcMethodCapabilities,
    },
    signing::{DeepXRuntimeConfig, DeepXRuntimeInterfaceError, RuntimeSnapshot},
};

const SYSTEM_EVENTS_STORAGE_KEY: &str = concat!(
    "0x26aa394eea5630e07c48ae0c9558cef7",
    "80d41e5e16056765bc8461851072c9d7",
);

/// Failures while binding perpetual placement events to an approved durable operation.
#[derive(Debug, Error)]
pub enum DeepXPerpPlaceEventVerificationError {
    /// The approved runtime does not expose the required event interface.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The operation, nonce domain, or exact runtime scope cannot be verified.
    #[error("unsupported DeepX perpetual placement event identity")]
    UnsupportedOperation,
    /// SCALE event decoding failed against the approved metadata.
    #[error("unable to decode DeepX perpetual placement events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// System.Events was truncated, count-mismatched, or contained trailing bytes.
    #[error("DeepX perpetual placement evidence is not complete System.Events")]
    MalformedEventBytes,
    /// The placement event disagrees with durable order terms.
    #[error("DeepX perpetual placement event conflicts with durable order terms")]
    ConflictingEvent,
    /// The target extrinsic emitted duplicate placement or dispatch events.
    #[error("duplicate DeepX perpetual placement evidence")]
    DuplicateEvent,
    /// Indexed dispatch and business evidence could not establish inclusion.
    #[error(transparent)]
    InclusionEvidence(#[from] DeepXInclusionEvidenceError),
}

/// Verifies complete perpetual placement dispatch and business events at one extrinsic index.
///
/// A successful dispatch requires exactly one matching `PerpMarket.OrderPlaced`. Both order IDs,
/// owner, market, side, size, order type, optional points and flags must match durable inputs.
/// Limit price must match; market price is runtime-computed and is not compared to the input.
/// This proves indexed event binding, not block canonicality, signer authorization, or fill success.
///
/// # Errors
///
/// Returns an error for unsupported scope, malformed SCALE, conflicting/duplicate events, or
/// successful dispatch without the expected placement event.
pub fn verify_perp_place_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpPlaceEventVerificationError> {
    use DeepXPerpPlaceEventVerificationError as Error;
    let Some(DeepXTransactionOperation::PerpPlace { is_long, .. }) = identity.operation() else {
        return Err(Error::UnsupportedOperation);
    };
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(Error::UnsupportedOperation);
    }
    let expected_side = if *is_long {
        nautilus_model::enums::OrderSide::Buy
    } else {
        nautilus_model::enums::OrderSide::Sell
    };
    if identity.order_side() != expected_side {
        return Err(Error::ConflictingEvent);
    }
    verify_indexed_perp_events(
        snapshot,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
        "OrderPlaced",
        |fields| perp_place_fields_match(fields, identity, block_number),
    )
}

/// Verifies TP/SL updates against exact durable points, including zero-to-`None` conversion.
///
/// Only operation-proven position fields are compared. This does not establish canonicality
/// or authorize submission. A successful dispatch requires one matching position update.
///
/// # Errors
///
/// Returns an error for foreign scope, incomplete events, conflicting fields or missing evidence.
pub fn verify_perp_profit_and_loss_point_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpProfitAndLossPointEventVerificationError> {
    let Some(DeepXTransactionOperation::PerpProfitAndLossPoint {
        subaccount,
        market_id,
        take_profit_point,
        stop_loss_point,
    }) = identity.operation()
    else {
        return Err(DeepXPerpProfitAndLossPointEventVerificationError::UnsupportedOperation);
    };
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(DeepXPerpProfitAndLossPointEventVerificationError::UnsupportedOperation);
    }
    verify_indexed_perp_events(
        snapshot,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
        "PositionUpdated",
        |fields| {
            let Composite::Named(fields) = fields else {
                return false;
            };
            let field = |name| {
                fields
                    .iter()
                    .find_map(|(key, value)| (key == name).then_some(value))
            };
            if fields.len() != 4
                || !field("owner").is_some_and(|v| value_matches_bytes(v, subaccount))
                || field("market_id").and_then(ScaleValue::as_u128) != Some(u128::from(*market_id))
                || field("pnl").and_then(ScaleValue::as_i128) != Some(0)
            {
                return false;
            }
            let Some(ScaleValue {
                value: ValueDef::Composite(Composite::Named(pos)),
                ..
            }) = field("pos")
            else {
                return false;
            };
            let field = |name| {
                pos.iter()
                    .find_map(|(key, value)| (key == name).then_some(value))
            };
            field("owner").is_some_and(|v| value_matches_bytes(v, subaccount))
                && field("market_id").and_then(ScaleValue::as_u128) == Some(u128::from(*market_id))
                && field("take_profit").is_some_and(|v| {
                    value_matches_optional_u128(
                        v,
                        (*take_profit_point != 0).then_some(*take_profit_point),
                    )
                })
                && field("stop_loss").is_some_and(|v| {
                    value_matches_optional_u128(
                        v,
                        (*stop_loss_point != 0).then_some(*stop_loss_point),
                    )
                })
        },
    )
    .map_err(DeepXPerpProfitAndLossPointEventVerificationError::IndexedEvents)
}

/// Failures binding TP/SL position updates to a durable operation.
#[derive(Debug, Error)]
pub enum DeepXPerpProfitAndLossPointEventVerificationError {
    /// The exact runtime, operation or nonce domain is unsupported.
    #[error("unsupported DeepX perpetual TP/SL event identity")]
    UnsupportedOperation,
    /// Shared indexed perpetual dispatch/business event validation failed.
    #[error("DeepX perpetual TP/SL indexed event verification failed: {0}")]
    IndexedEvents(#[source] DeepXPerpPlaceEventVerificationError),
}

fn verify_indexed_perp_events(
    snapshot: &RuntimeSnapshot,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
    event_name: &str,
    fields_match: impl Fn(&Composite<u32>) -> bool,
) -> Result<DeepXInclusionEvidence, DeepXPerpPlaceEventVerificationError> {
    use DeepXPerpPlaceEventVerificationError as Error;
    snapshot.interfaces().event("PerpMarket", event_name)?;
    snapshot.interfaces().event("System", "ExtrinsicSuccess")?;
    snapshot.interfaces().event("System", "ExtrinsicFailed")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| Error::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut dispatch = None;
    let mut matched = false;
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    for event in events.iter() {
        let event = event.map_err(|e| Error::Decode(Box::new(e)))?;
        count += 1;
        consumed += event.bytes().len();
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index) {
            continue;
        }
        if event.pallet_name() == "System" {
            let outcome = match event.variant_name() {
                "ExtrinsicSuccess" => DeepXDispatchOutcome::Success,
                "ExtrinsicFailed" => DeepXDispatchOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "PerpMarket" && event.variant_name() == event_name {
            let fields = event
                .field_values()
                .map_err(|e| Error::Decode(Box::new(e)))?;
            if !fields_match(&fields) {
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
    Ok(DeepXInclusionEvidence::from_indexed_observations(
        block_hash,
        block_number,
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: dispatch.ok_or(Error::MalformedEventBytes)?,
        },
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: if matched {
                DeepXBusinessEventOutcome::Success
            } else {
                DeepXBusinessEventOutcome::NotObserved
            },
        },
    )?)
}

fn perp_place_fields_match(
    fields: &Composite<u32>,
    identity: &DeepXTransactionIdentity,
    block_number: u64,
) -> bool {
    let Some(DeepXTransactionOperation::PerpPlace {
        subaccount,
        market_id,
        is_long,
        size,
        price,
        order_type,
        take_profit,
        stop_loss,
        reduce_only,
        post_only,
    }) = identity.operation()
    else {
        return false;
    };
    let super::DeepXNonceReservation::TimestampOrderId { value: order_id } = identity.nonce()
    else {
        return false;
    };
    let Composite::Named(fields) = fields else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    if fields.len() != 2
        || field("order_id").and_then(ScaleValue::as_u128) != Some(u128::from(order_id))
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
    let spot_order_type = match order_type {
        crate::signing::DeepXPerpOrderType::Limit(tif) => {
            crate::signing::DeepXSpotOrderType::Limit(*tif)
        }
        crate::signing::DeepXPerpOrderType::Market(slippage) => {
            crate::signing::DeepXSpotOrderType::Market(*slippage)
        }
        crate::signing::DeepXPerpOrderType::Stop => return false,
    };
    order.len() == 16
        && field("order_id").and_then(ScaleValue::as_u128) == Some(u128::from(order_id))
        && field("owner").is_some_and(|value| value_matches_bytes(value, subaccount))
        && field("market_id").and_then(ScaleValue::as_u128) == Some(u128::from(*market_id))
        && field("is_long").and_then(ScaleValue::as_bool) == Some(*is_long)
        && field("size").and_then(ScaleValue::as_u128) == Some(*size)
        && field("price").and_then(ScaleValue::as_u128).is_some_and(|actual| matches!(order_type, crate::signing::DeepXPerpOrderType::Market(_)) || actual == *price)
        && field("order_type").is_some_and(|value| value_matches_order_type(value, spot_order_type))
        && field("take_profit").is_some_and(|value| value_matches_optional_u128(value, *take_profit))
        && field("stop_loss").is_some_and(|value| value_matches_optional_u128(value, *stop_loss))
        && field("reduce_only").and_then(ScaleValue::as_bool) == Some(*reduce_only)
        && field("post_only").is_some_and(|value| value_matches_post_only(value, *post_only))
        && field("create_time").and_then(ScaleValue::as_u128) == Some(u128::from(block_number))
        && field("leverage").and_then(ScaleValue::as_u128).is_some()
        && field("status").is_some_and(|value| matches!(&value.value, ValueDef::Variant(variant) if variant.name == "Open" && variant.values.values().next().is_none()))
        && field("size_filled").and_then(ScaleValue::as_u128) == Some(0)
        && field("size_remain").and_then(ScaleValue::as_u128) == Some(*size)
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

/// Verifies the close-generated order without equating its account-sequence ID to the nonce.
///
/// This binds creation of a reduce-only order, not execution or complete position closure.
/// Position-derived direction and size are not inferred from the durable close call.
///
/// # Errors
///
/// Returns an error for unsupported scope, malformed events or conflicting close-order terms.
pub fn verify_perp_close_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpCloseEventVerificationError> {
    let Some(DeepXTransactionOperation::PerpClose {
        subaccount,
        market_id,
        price,
        slippage,
    }) = identity.operation()
    else {
        return Err(DeepXPerpCloseEventVerificationError::UnsupportedOperation);
    };
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(DeepXPerpCloseEventVerificationError::UnsupportedOperation);
    }
    verify_indexed_perp_events(snapshot, block_hash, block_number, extrinsic_index, event_bytes,
        "OrderPlaced", |fields| {
            let Composite::Named(outer) = fields else { return false; };
            let field = |name| outer.iter().find_map(|(key, value)| (key == name).then_some(value));
            let Some(order_id) = field("order_id").and_then(ScaleValue::as_u128).filter(|id| *id <= u128::from(u64::MAX)) else { return false; };
            let Some(ScaleValue { value: ValueDef::Composite(Composite::Named(order)), .. }) = field("order") else { return false; };
            let field = |name| order.iter().find_map(|(key, value)| (key == name).then_some(value));
            outer.len() == 2 && order.len() == 16
                && field("order_id").and_then(ScaleValue::as_u128) == Some(order_id)
                && field("owner").is_some_and(|v| value_matches_bytes(v, subaccount))
                && field("market_id").and_then(ScaleValue::as_u128) == Some(u128::from(*market_id))
                && field("is_long").and_then(ScaleValue::as_bool).is_some()
                && field("size").and_then(ScaleValue::as_u128).is_some()
                && field("size_remain").and_then(ScaleValue::as_u128) == field("size").and_then(ScaleValue::as_u128)
                && field("size_filled").and_then(ScaleValue::as_u128) == Some(0)
                && field("create_time").and_then(ScaleValue::as_u128) == Some(u128::from(block_number))
                && field("leverage").and_then(ScaleValue::as_u128).is_some()
                && field("price").and_then(ScaleValue::as_u128).is_some_and(|actual| *price == 0 || actual == *price)
                && field("order_type").is_some_and(|v| if *price == 0 {
                    value_matches_order_type(v, crate::signing::DeepXSpotOrderType::Market(*slippage))
                } else { matches!(&v.value, ValueDef::Variant(variant) if variant.name == "Stop" && variant.values.values().next().is_none()) })
                && field("status").is_some_and(|v| matches!(&v.value, ValueDef::Variant(variant) if variant.name == "Open" && variant.values.values().next().is_none()))
                && field("reduce_only").and_then(ScaleValue::as_bool) == Some(true)
                && field("post_only").is_some_and(|v| value_matches_post_only(v, crate::signing::DeepXPostOnlyParam::None))
                && field("take_profit").is_some_and(|v| value_matches_optional_u128(v, None))
                && field("stop_loss").is_some_and(|v| value_matches_optional_u128(v, None))
        }).map_err(DeepXPerpCloseEventVerificationError::IndexedEvents)
}

/// Failures binding a generated close order to durable call terms.
#[derive(Debug, Error)]
pub enum DeepXPerpCloseEventVerificationError {
    /// The exact operation, runtime or nonce domain is unsupported.
    #[error("unsupported DeepX perpetual close event identity")]
    UnsupportedOperation,
    /// Shared indexed perpetual event validation failed.
    #[error("DeepX perpetual close indexed event verification failed: {0}")]
    IndexedEvents(#[source] DeepXPerpPlaceEventVerificationError),
}

/// Errors raised while verifying offline Spot placement event evidence.
#[derive(Debug, Error)]
pub enum DeepXSpotPlaceEventVerificationError {
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    #[error("DeepX identity or runtime does not describe a supported Spot place")]
    UnsupportedOperation,
    #[error("unable to decode DeepX Spot place event evidence: {0}")]
    Decode(#[source] subxt_core::Error),
    #[error("DeepX Spot place event evidence is not a complete System.Events value")]
    MalformedEventBytes,
    #[error("DeepX Spot place event conflicts with the durable identity")]
    ConflictingEvent,
    #[error("DeepX Spot place evidence contains duplicate order state events")]
    DuplicateEvent,
    #[error(transparent)]
    InclusionEvidence(#[from] DeepXInclusionEvidenceError),
}

/// Verifies Spot placement inclusion against an approved offline snapshot.
///
/// Successful dispatch requires a unique, same-index `StateOrderBuy` or `StateOrderSell` event
/// whose durable input fields match the reservation. Failed dispatch is authoritative without a
/// business event.
///
/// # Errors
///
/// Returns an error for unsupported operations, conflicting identities, incomplete SCALE,
/// duplicate evidence, or successful dispatch without the expected business event.
pub fn verify_spot_place_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXSpotPlaceEventVerificationError> {
    use DeepXSpotPlaceEventVerificationError as Error;
    let Some(DeepXTransactionOperation::SpotPlace {
        subaccount,
        pair,
        is_buy,
        quote_amount,
        base_amount,
        order_type,
        post_only,
        reduce_only,
    }) = identity.operation()
    else {
        return Err(Error::UnsupportedOperation);
    };
    let super::DeepXNonceReservation::TimestampOrderId { value: order_id } = identity.nonce()
    else {
        return Err(Error::UnsupportedOperation);
    };
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity()) {
        return Err(Error::UnsupportedOperation);
    }
    let expected_side = if *is_buy {
        nautilus_model::enums::OrderSide::Buy
    } else {
        nautilus_model::enums::OrderSide::Sell
    };
    if identity.order_side() != expected_side {
        return Err(Error::ConflictingEvent);
    }
    let event_name = if *is_buy {
        "StateOrderBuy"
    } else {
        "StateOrderSell"
    };
    snapshot.interfaces().event("SpotMarket", event_name)?;
    snapshot.interfaces().event("System", "ExtrinsicSuccess")?;
    snapshot.interfaces().event("System", "ExtrinsicFailed")?;

    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| Error::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut dispatch = None;
    let mut matched = false;
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    for event in events.iter() {
        let event = event.map_err(Error::Decode)?;
        count += 1;
        consumed += event.bytes().len();
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index) {
            continue;
        }
        if event.pallet_name() == "System" {
            let outcome = match event.variant_name() {
                "ExtrinsicSuccess" => DeepXDispatchOutcome::Success,
                "ExtrinsicFailed" => DeepXDispatchOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "SpotMarket" && event.variant_name() == event_name {
            let fields = event.field_values().map_err(Error::Decode)?;
            let Composite::Named(fields) = fields else {
                return Err(Error::ConflictingEvent);
            };
            if fields.len() != 1
                || !fields.iter().any(|(name, order)| {
                    name == "order"
                        && spot_place_order_matches(
                            order,
                            *subaccount,
                            *pair,
                            order_id,
                            *quote_amount,
                            *base_amount,
                            *order_type,
                            *post_only,
                            *reduce_only,
                            *is_buy,
                        )
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
    Ok(DeepXInclusionEvidence::from_indexed_observations(
        block_hash,
        block_number,
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: dispatch.ok_or(Error::MalformedEventBytes)?,
        },
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: if matched {
                DeepXBusinessEventOutcome::Success
            } else {
                DeepXBusinessEventOutcome::NotObserved
            },
        },
    )?)
}

/// Errors raised while verifying offline Spot cancellation event evidence.
#[derive(Debug, Error)]
pub enum DeepXSpotCancelEventVerificationError {
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    #[error("DeepX identity or runtime does not describe a supported Spot cancel")]
    UnsupportedOperation,
    #[error("DeepX fast Spot cancel event suppression is not approved")]
    FastCancelUnsupported,
    #[error("unable to decode DeepX Spot event evidence: {0}")]
    Decode(#[source] subxt_core::Error),
    #[error("DeepX Spot event evidence is not a complete System.Events value")]
    MalformedEventBytes,
    #[error("DeepX Spot cancel event conflicts with the durable identity")]
    ConflictingEvent,
    #[error("DeepX Spot cancel evidence contains duplicate events")]
    DuplicateEvent,
    #[error(transparent)]
    InclusionEvidence(#[from] DeepXInclusionEvidenceError),
}

/// Verifies ordinary Spot cancellation inclusion against an approved offline snapshot.
///
/// Successful dispatch requires a same-index, identity-matching `OrderCancelled` event.
/// Failed dispatch is authoritative without a successful business event. Fast cancellation
/// remains gated pending runtime-tagged evidence of event suppression.
///
/// # Errors
///
/// Returns an error for unsupported operations, conflicting identities, incomplete SCALE,
/// duplicate evidence, or successful dispatch without the expected business event.
pub fn verify_spot_cancel_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXSpotCancelEventVerificationError> {
    use DeepXSpotCancelEventVerificationError as Error;
    let Some(DeepXTransactionOperation::SpotCancel {
        subaccount,
        pair,
        order_id,
        is_buy,
        fast_cancel,
    }) = identity.operation()
    else {
        return Err(Error::UnsupportedOperation);
    };
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity()) {
        return Err(Error::UnsupportedOperation);
    }
    if *fast_cancel {
        return Err(Error::FastCancelUnsupported);
    }
    snapshot
        .interfaces()
        .event("SpotMarket", "OrderCancelled")?;
    snapshot.interfaces().event("System", "ExtrinsicSuccess")?;
    snapshot.interfaces().event("System", "ExtrinsicFailed")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| Error::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut dispatch = None;
    let mut matched = false;
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    for event in events.iter() {
        let event = event.map_err(Error::Decode)?;
        count += 1;
        consumed += event.bytes().len();
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index) {
            continue;
        }
        if event.pallet_name() == "System" {
            let outcome = match event.variant_name() {
                "ExtrinsicSuccess" => DeepXDispatchOutcome::Success,
                "ExtrinsicFailed" => DeepXDispatchOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "SpotMarket" && event.variant_name() == "OrderCancelled" {
            let fields = event.field_values().map_err(Error::Decode)?;
            let Composite::Named(fields) = fields else {
                return Err(Error::ConflictingEvent);
            };
            let field = |name| {
                fields
                    .iter()
                    .find_map(|(candidate, value)| (candidate == name).then_some(value))
            };
            if fields.len() != 5
                || !field("pair").is_some_and(|value| value_matches_bytes(value, pair))
                || !field("maker").is_some_and(|value| value_matches_bytes(value, subaccount))
                || field("order_id").and_then(ScaleValue::as_u128) != Some(u128::from(*order_id))
                || field("is_buy").and_then(ScaleValue::as_bool) != Some(*is_buy)
                || !field("reason").is_some_and(|value| matches!(&value.value,
                    ValueDef::Variant(reason) if reason.name == "UserCanceled" && reason.values.values().next().is_none()))
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
    Ok(DeepXInclusionEvidence::from_indexed_observations(
        block_hash,
        block_number,
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: dispatch.ok_or(Error::MalformedEventBytes)?,
        },
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: if matched {
                DeepXBusinessEventOutcome::Success
            } else {
                DeepXBusinessEventOutcome::NotObserved
            },
        },
    )?)
}

/// Errors raised while verifying a perpetual cancellation from runtime event evidence.
#[derive(Debug, Error)]
pub enum DeepXPerpCancelEventVerificationError {
    /// The approved runtime snapshot does not expose the required event.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The durable identity does not describe an event-verifiable perpetual cancellation.
    #[error("DeepX transaction identity has no event-verifiable perpetual cancel operation")]
    UnsupportedOperation,
    /// Fast cancel deliberately omits the authoritative pallet cancellation event.
    #[error("DeepX fast perpetual cancel has no authoritative pallet event")]
    FastCancelUnsupported,
    /// Runtime event bytes could not be decoded against the approved snapshot.
    #[error("unable to decode DeepX runtime event evidence: {0}")]
    Decode(#[source] subxt_core::Error),
    /// Runtime event bytes were incomplete, count-mismatched, or contained trailing data.
    #[error("DeepX runtime event evidence is not a complete System.Events value")]
    MalformedEventBytes,
    /// A cancellation event at the target index did not match the durable operation.
    #[error("DeepX perpetual cancel event conflicts with the durable transaction identity")]
    ConflictingEvent,
    /// More than one matching cancellation event was observed at the target index.
    #[error("DeepX perpetual cancel emitted duplicate matching events")]
    DuplicateEvent,
    /// Indexed dispatch and business-event observations could not prove inclusion.
    #[error(transparent)]
    InclusionEvidence(#[from] DeepXInclusionEvidenceError),
}

/// Verifies the expected business event for one durable perpetual cancel operation.
///
/// The event bytes must be the complete SCALE value returned from `System.Events`, including its
/// compact event-count prefix. Only `ApplyExtrinsic` events at `extrinsic_index` are considered.
/// Fast cancel remains unsupported because the runtime deliberately suppresses this pallet event.
///
/// # Errors
///
/// Returns an error when the runtime interface is unavailable, the operation cannot emit
/// authoritative evidence, event decoding fails, or cancellation evidence conflicts or repeats.
pub fn verify_perp_cancel_business_event(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXIndexedOutcome<DeepXBusinessEventOutcome>, DeepXPerpCancelEventVerificationError> {
    snapshot
        .interfaces()
        .event("PerpMarket", "OrderCancelled")?;
    let Some(DeepXTransactionOperation::PerpCancel {
        subaccount,
        order_id,
        fast_cancel,
        ..
    }) = identity.operation()
    else {
        return Err(DeepXPerpCancelEventVerificationError::UnsupportedOperation);
    };
    if *fast_cancel {
        return Err(DeepXPerpCancelEventVerificationError::FastCancelUnsupported);
    }

    let mut remaining = event_bytes;
    let declared_count = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXPerpCancelEventVerificationError::MalformedEventBytes)?
        .0;
    let prefix_len = event_bytes.len() - remaining.len();
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    let mut matched = false;
    let mut decoded_count = 0_u32;
    let mut consumed_len = prefix_len;
    for event in events.iter() {
        let event = event.map_err(DeepXPerpCancelEventVerificationError::Decode)?;
        decoded_count += 1;
        consumed_len += event.bytes().len();
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index)
            || event.pallet_name() != "PerpMarket"
            || event.variant_name() != "OrderCancelled"
        {
            continue;
        }
        let fields = event
            .field_values()
            .map_err(DeepXPerpCancelEventVerificationError::Decode)?;
        if !perp_cancel_fields_match(&fields, *subaccount, *order_id) {
            return Err(DeepXPerpCancelEventVerificationError::ConflictingEvent);
        }
        if matched {
            return Err(DeepXPerpCancelEventVerificationError::DuplicateEvent);
        }
        matched = true;
    }
    if decoded_count != declared_count || consumed_len != event_bytes.len() {
        return Err(DeepXPerpCancelEventVerificationError::MalformedEventBytes);
    }

    Ok(DeepXIndexedOutcome {
        extrinsic_index,
        outcome: if matched {
            DeepXBusinessEventOutcome::Success
        } else {
            DeepXBusinessEventOutcome::NotObserved
        },
    })
}

fn verify_perp_cancel_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpCancelEventVerificationError> {
    let Some(DeepXTransactionOperation::PerpCancel { fast_cancel, .. }) = identity.operation()
    else {
        return Err(DeepXPerpCancelEventVerificationError::UnsupportedOperation);
    };
    snapshot.interfaces().event("System", "ExtrinsicSuccess")?;
    snapshot.interfaces().event("System", "ExtrinsicFailed")?;

    let mut remaining = event_bytes;
    let declared_count = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXPerpCancelEventVerificationError::MalformedEventBytes)?
        .0;
    let prefix_len = event_bytes.len() - remaining.len();
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    let mut dispatch = None;
    let mut decoded_count = 0_u32;
    let mut consumed_len = prefix_len;
    for event in events.iter() {
        let event = event.map_err(DeepXPerpCancelEventVerificationError::Decode)?;
        decoded_count += 1;
        consumed_len += event.bytes().len();
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index)
            || event.pallet_name() != "System"
        {
            continue;
        }
        let outcome = match event.variant_name() {
            "ExtrinsicSuccess" => DeepXDispatchOutcome::Success,
            "ExtrinsicFailed" => DeepXDispatchOutcome::Failed,
            _ => continue,
        };
        if dispatch.replace(outcome).is_some() {
            return Err(DeepXPerpCancelEventVerificationError::DuplicateEvent);
        }
    }
    if decoded_count != declared_count || consumed_len != event_bytes.len() {
        return Err(DeepXPerpCancelEventVerificationError::MalformedEventBytes);
    }
    let dispatch = DeepXIndexedOutcome {
        extrinsic_index,
        outcome: dispatch.ok_or(DeepXPerpCancelEventVerificationError::MalformedEventBytes)?,
    };
    if *fast_cancel {
        return Ok(DeepXInclusionEvidence {
            block_hash,
            block_number,
            extrinsic_index,
            outcome: match dispatch.outcome {
                DeepXDispatchOutcome::Success => super::DeepXInclusionOutcome::Success,
                DeepXDispatchOutcome::Failed => super::DeepXInclusionOutcome::Failed,
            },
        });
    }
    let business_event =
        verify_perp_cancel_business_event(snapshot, identity, extrinsic_index, event_bytes)?;
    Ok(DeepXInclusionEvidence::from_indexed_observations(
        block_hash,
        block_number,
        dispatch,
        business_event,
    )?)
}

fn perp_cancel_fields_match(
    fields: &Composite<u32>,
    expected_subaccount: [u8; 20],
    expected_order_id: u64,
) -> bool {
    let Composite::Named(fields) = fields else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    field("user").is_some_and(|value| value_matches_bytes(value, &expected_subaccount))
        && field("order_id").and_then(ScaleValue::as_u128) == Some(u128::from(expected_order_id))
        && field("reason").is_some_and(|value| {
            matches!(&value.value, ValueDef::Variant(reason) if reason.name == "UserCanceled")
        })
}

fn value_matches_bytes(value: &ScaleValue<u32>, expected: &[u8]) -> bool {
    let ValueDef::Composite(bytes) = &value.value else {
        return false;
    };
    let values: Vec<_> = bytes.values().collect();
    if values.len() == 1 {
        return value_matches_bytes(values[0], expected);
    }
    values.into_iter().map(ScaleValue::as_u128).eq(expected
        .iter()
        .copied()
        .map(u128::from)
        .map(Some))
}

#[allow(clippy::too_many_arguments)]
fn spot_place_order_matches(
    value: &ScaleValue<u32>,
    subaccount: [u8; 20],
    pair: [u8; 32],
    order_id: u64,
    quote_amount: [u8; 32],
    base_amount: [u8; 32],
    order_type: crate::signing::DeepXSpotOrderType,
    post_only: crate::signing::DeepXPostOnlyParam,
    reduce_only: bool,
    is_buy: bool,
) -> bool {
    let ValueDef::Composite(Composite::Named(fields)) = &value.value else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    fields.len() == 12
        && field("id").and_then(ScaleValue::as_u128) == Some(u128::from(order_id))
        && field("maker").is_some_and(|value| value_matches_bytes(value, &subaccount))
        && field("pair").is_some_and(|value| value_matches_bytes(value, &pair))
        && field("price").is_some()
        && field("quote_amount").is_some_and(|value| value_matches_u256_le(value, &quote_amount))
        && field("base_amount").is_some_and(|value| value_matches_u256_le(value, &base_amount))
        && field("create_time").is_some()
        && field("status").is_some()
        && field("order_type").is_some_and(|value| value_matches_order_type(value, order_type))
        && field("post_only").is_some_and(|value| value_matches_post_only(value, post_only))
        && field("reduce_only").and_then(ScaleValue::as_bool) == Some(reduce_only)
        && field("is_buy").and_then(ScaleValue::as_bool) == Some(is_buy)
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

fn value_matches_order_type(
    value: &ScaleValue<u32>,
    expected: crate::signing::DeepXSpotOrderType,
) -> bool {
    let ValueDef::Variant(variant) = &value.value else {
        return false;
    };
    match expected {
        crate::signing::DeepXSpotOrderType::Limit(time_in_force) => {
            let values: Vec<_> = variant.values.values().collect();
            variant.name == "Limit"
                && values.len() == 1
                && matches!(&values[0].value, ValueDef::Variant(value)
                    if value.name == match time_in_force {
                        crate::signing::DeepXTimeInForce::Gtc => "GTC",
                        crate::signing::DeepXTimeInForce::Ioc => "IOC",
                        crate::signing::DeepXTimeInForce::Fok => "FOK",
                    } && value.values.values().next().is_none())
        }
        crate::signing::DeepXSpotOrderType::Market(slippage) => {
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
        crate::signing::DeepXSpotOrderType::Stop => {
            variant.name == "Stop" && variant.values.values().next().is_none()
        }
    }
}

fn value_matches_post_only(
    value: &ScaleValue<u32>,
    expected: crate::signing::DeepXPostOnlyParam,
) -> bool {
    let ValueDef::Variant(variant) = &value.value else {
        return false;
    };
    let expected = match expected {
        crate::signing::DeepXPostOnlyParam::None => "None",
        crate::signing::DeepXPostOnlyParam::MustPostOnly => "MustPostOnly",
        crate::signing::DeepXPostOnlyParam::Adaptive => "Adaptive",
    };
    variant.name == expected && variant.values.values().next().is_none()
}

/// Errors raised while observing transaction presence through DeepX RPC endpoints.
#[derive(Debug, Error)]
pub enum DeepXTransactionWatchError {
    /// A request or JSON-RPC response failed.
    #[error("failed to observe DeepX transaction using `{method}`: {source}")]
    Rpc {
        /// Method which failed.
        method: &'static str,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The node returned a malformed block hash.
    #[error("DeepX canonical block returned an invalid block hash: {0}")]
    InvalidBlockHash(String),
    /// The requested canonical block was unavailable.
    #[error("DeepX canonical block {0} was unavailable")]
    BlockUnavailable(u64),
    /// The finalized head did not have an available header.
    #[error("DeepX finalized head header was unavailable")]
    FinalizedHeaderUnavailable,
    /// Finality reached the recorded height without preserving the exact recorded inclusion.
    #[error(
        "DeepX finalized chain does not preserve the recorded transaction inclusion at block {0}"
    )]
    FinalityEvidenceConflict(u64),
    /// The durable recovery checkpoint is no longer canonical at its recorded height.
    #[error(
        "DeepX recovery checkpoint at block {block_number} no longer matches the canonical hash"
    )]
    RecoveryCheckpointMismatch {
        /// Durable checkpoint block number.
        block_number: u64,
        /// Hash retained with the durable checkpoint.
        expected: [u8; 32],
        /// Current canonical hash at the checkpoint height.
        received: [u8; 32],
    },
    /// The returned block header did not identify the requested height.
    #[error("DeepX canonical block number mismatch: requested {requested}, received {received}")]
    BlockNumberMismatch {
        /// Requested canonical block number.
        requested: u64,
        /// Block number decoded from the returned header.
        received: u64,
    },
    /// A block or pool entry was not a prefixed SCALE extrinsic.
    #[error("DeepX {evidence_source} extrinsic at index {index} was invalid")]
    InvalidExtrinsic {
        /// Evidence source containing the malformed entry.
        evidence_source: &'static str,
        /// Zero-based entry index.
        index: usize,
    },
    /// A node returned the target extrinsic more than once.
    #[error("DeepX {evidence_source} contained duplicate target extrinsics")]
    DuplicateExtrinsic {
        /// Evidence source containing duplicate entries.
        evidence_source: &'static str,
    },
    /// The block header number was malformed or outside `u64`.
    #[error("DeepX canonical block returned an invalid block number: {0}")]
    InvalidBlockNumber(String),
    /// RPC capability evidence does not belong to the selected endpoint or is incomplete.
    #[error(
        "DeepX RPC capability evidence for role {0:?} does not match the validated endpoint set"
    )]
    CapabilitiesMismatch(DeepXRpcRole),
    /// Exact inclusion cannot be classified without authoritative event evidence.
    #[error(
        "DeepX transaction was found in canonical block {block_number} at extrinsic index {extrinsic_index}, but authoritative event evidence is unavailable"
    )]
    EventEvidenceUnavailable {
        /// Canonical block containing the exact target extrinsic.
        block_number: u64,
        /// Index of the exact target extrinsic in the block.
        extrinsic_index: u32,
    },
    /// The exact finalized block did not expose `System.Events` storage.
    #[error("DeepX System.Events storage was unavailable at finalized block {0}")]
    EventStorageUnavailable(u64),
    /// The exact finalized block returned malformed `System.Events` storage.
    #[error("DeepX System.Events storage at finalized block {0} was not prefixed hexadecimal")]
    InvalidEventStorage(u64),
    /// Runtime event evidence did not prove the durable perpetual cancellation.
    #[error(transparent)]
    EventVerification(#[from] DeepXPerpCancelEventVerificationError),
    /// Runtime event evidence did not prove the explicitly selected Spot cancellation.
    #[error(transparent)]
    SpotEventVerification(#[from] DeepXSpotCancelEventVerificationError),
    /// Runtime event evidence did not prove the explicitly selected Spot placement.
    #[error(transparent)]
    SpotPlaceEventVerification(#[from] DeepXSpotPlaceEventVerificationError),
    /// Runtime events could not establish the durable perpetual placement.
    #[error(transparent)]
    PerpPlaceEventVerification(#[from] DeepXPerpPlaceEventVerificationError),
    /// Close-generated order evidence conflicts with durable call terms.
    #[error(transparent)]
    PerpCloseEventVerification(#[from] DeepXPerpCloseEventVerificationError),
    /// Perpetual TP/SL update evidence did not match the durable operation.
    #[error(transparent)]
    PerpProfitAndLossPointEventVerification(
        #[from] DeepXPerpProfitAndLossPointEventVerificationError,
    ),
    /// A bounded recovery scan could not be planned safely.
    #[error(transparent)]
    ScanPlan(#[from] DeepXRecoveryScanPlanError),
    /// Collected canonical evidence did not match the bounded scan plan.
    #[error(transparent)]
    ScanCollection(#[from] DeepXRecoveryScanCollectionError),
}

#[derive(Debug, Deserialize)]
struct RpcBlockResponse {
    block: RpcBlock,
}

#[derive(Debug, Deserialize)]
struct RpcBlock {
    header: RpcBlockHeader,
    extrinsics: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RpcBlockHeader {
    number: String,
}

/// Exact transaction-location observation from one canonical block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXCanonicalBlockObservation {
    block_number: u64,
    block_hash: [u8; 32],
    extrinsic_index: Option<u32>,
}

/// Finality observation for one exact recorded inclusion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXFinalityObservation {
    /// The finalized head has not reached the recorded inclusion height.
    Pending(DeepXFinalizedRecoveryCheckpoint),
    /// The exact recorded inclusion remains canonical at a finalized height.
    Finalized(DeepXInclusionEvidence),
}

impl DeepXCanonicalBlockObservation {
    /// Returns the canonical block number inspected by the watch endpoint.
    #[must_use]
    pub const fn block_number(&self) -> u64 {
        self.block_number
    }

    /// Returns the canonical block hash supplied by the watch endpoint.
    #[must_use]
    pub const fn block_hash(&self) -> [u8; 32] {
        self.block_hash
    }

    /// Returns the unique extrinsic index whose recomputed hash matched the target.
    #[must_use]
    pub const fn extrinsic_index(&self) -> Option<u32> {
        self.extrinsic_index
    }
}

/// Classifies a recorded non-finalized inclusion against an exact canonical block observation.
///
/// A different block hash at the recorded height proves a reorganization. An unchanged block is
/// canonical only when the target extrinsic remains at the recorded index. Missing or displaced
/// transaction evidence requires operator action rather than an inferred absence.
#[must_use]
fn classify_canonical_block_observation(
    recorded_inclusion: DeepXInclusionEvidence,
    observation: DeepXCanonicalBlockObservation,
) -> DeepXReorganizationDecision {
    let inclusion = (observation.block_hash() == recorded_inclusion.block_hash()
        && observation.extrinsic_index() == Some(recorded_inclusion.extrinsic_index()))
    .then_some(recorded_inclusion);
    classify_reorganization(
        recorded_inclusion,
        Some(DeepXCanonicalBlockEvidence::new(
            observation.block_number(),
            observation.block_hash(),
            inclusion,
        )),
    )
}

/// Observes whether a recorded non-finalized inclusion remains canonical at its exact height.
///
/// The durable extrinsic hash binds the canonical block lookup to the recorded transaction. This
/// boundary performs no lifecycle mutation and grants no replay or replacement authority.
///
/// # Errors
///
/// Returns an error for mismatched Watch capabilities, RPC failure, malformed or inconsistent
/// block identity, malformed extrinsics, or duplicate target entries.
pub async fn observe_reorganization(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    target_extrinsic_hash: [u8; 32],
    recorded_inclusion: DeepXInclusionEvidence,
) -> Result<DeepXReorganizationDecision, DeepXTransactionWatchError> {
    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != watch_url
        || DEEPX_WATCH_RPC_METHODS
            .iter()
            .any(|method| !watch_capabilities.methods().contains(*method))
    {
        return Err(DeepXTransactionWatchError::CapabilitiesMismatch(
            DeepXRpcRole::Watch,
        ));
    }

    let observation = observe_canonical_block_at(
        watch_url,
        recorded_inclusion.block_number(),
        target_extrinsic_hash,
    )
    .await?;
    Ok(classify_canonical_block_observation(
        recorded_inclusion,
        observation,
    ))
}

/// Observes whether an exact recorded inclusion has become final on the Watch endpoint.
///
/// Finality is released only when the finalized head reaches the recorded height and the same
/// block hash still contains the target extrinsic at its recorded index. Conflicting canonical
/// evidence is rejected for separate reorganization handling.
///
/// # Errors
///
/// Returns an error for mismatched Watch capabilities, RPC failure, malformed chain data, or
/// canonical evidence which no longer preserves the exact recorded inclusion.
pub async fn observe_finality(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    target_extrinsic_hash: [u8; 32],
    recorded_inclusion: DeepXInclusionEvidence,
) -> Result<DeepXFinalityObservation, DeepXTransactionWatchError> {
    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != watch_url
        || DEEPX_WATCH_RPC_METHODS
            .iter()
            .any(|method| !watch_capabilities.methods().contains(*method))
    {
        return Err(DeepXTransactionWatchError::CapabilitiesMismatch(
            DeepXRpcRole::Watch,
        ));
    }

    let checkpoint = observe_finalized_checkpoint_at(watch_url).await?;
    if checkpoint.block_number() < recorded_inclusion.block_number() {
        return Ok(DeepXFinalityObservation::Pending(checkpoint));
    }

    let observation = observe_canonical_block_at(
        watch_url,
        recorded_inclusion.block_number(),
        target_extrinsic_hash,
    )
    .await?;
    match classify_canonical_block_observation(recorded_inclusion, observation) {
        DeepXReorganizationDecision::Canonical => {
            Ok(DeepXFinalityObservation::Finalized(recorded_inclusion))
        }
        DeepXReorganizationDecision::Reorganized(_)
        | DeepXReorganizationDecision::ActionRequired => Err(
            DeepXTransactionWatchError::FinalityEvidenceConflict(recorded_inclusion.block_number()),
        ),
    }
}

/// Exact transaction-membership observation from the submission node pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXPoolObservation {
    /// The exact target extrinsic is present in the pending pool.
    Present,
    /// Every returned pending extrinsic was valid and none matched the target.
    Absent,
}

/// Finalized chain checkpoint observed from the recovery endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedRecoveryCheckpoint {
    block_number: u64,
    block_hash: [u8; 32],
}

impl DeepXFinalizedRecoveryCheckpoint {
    /// Returns the finalized block number.
    #[must_use]
    pub const fn block_number(&self) -> u64 {
        self.block_number
    }

    /// Returns the finalized block hash.
    #[must_use]
    pub const fn block_hash(&self) -> [u8; 32] {
        self.block_hash
    }
}

async fn observe_finalized_checkpoint_at(
    endpoint_url: &str,
) -> Result<DeepXFinalizedRecoveryCheckpoint, DeepXTransactionWatchError> {
    let client = BlockchainHttpRpcClient::new(endpoint_url.to_string(), None, None);
    let encoded_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getFinalizedHead",
            "params": [],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getFinalizedHead",
            source,
        })?;
    let block_hash = decode_hash(&encoded_hash)?;
    let header: Option<RpcBlockHeader> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getHeader",
            "params": [encoded_hash],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getHeader",
            source,
        })?;
    let header = header.ok_or(DeepXTransactionWatchError::FinalizedHeaderUnavailable)?;
    Ok(DeepXFinalizedRecoveryCheckpoint {
        block_number: decode_block_number(&header.number)?,
        block_hash,
    })
}

/// Complete result of observing a bounded finalized recovery interval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeepXFinalizedRecoveryCollection {
    /// The durable scan checkpoint already equals the current finalized head.
    UpToDate(DeepXFinalizedRecoveryCheckpoint),
    /// Every planned canonical block and the pending pool were inspected successfully.
    Scan(DeepXRecoveryScan),
}

/// Observes one canonical block and locates a transaction by its recomputed extrinsic hash.
///
/// This read-only boundary proves only exact transaction presence at an extrinsic index. It does
/// not decode dispatch or business events and therefore cannot produce inclusion success, failure,
/// or finality evidence.
///
/// # Errors
///
/// Returns an error for RPC failure, malformed or inconsistent block identity, malformed
/// extrinsics, duplicate target entries, or an index outside `u32`.
pub async fn observe_canonical_block(
    endpoints: &DeepXValidatedRpcEndpoints,
    block_number: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXCanonicalBlockObservation, DeepXTransactionWatchError> {
    observe_canonical_block_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        block_number,
        target_extrinsic_hash,
    )
    .await
}

async fn observe_canonical_block_at(
    endpoint_url: &str,
    block_number: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXCanonicalBlockObservation, DeepXTransactionWatchError> {
    let client = BlockchainHttpRpcClient::new(endpoint_url.to_string(), None, None);
    let encoded_hash: Option<String> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [block_number],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getBlockHash",
            source,
        })?;
    let encoded_hash =
        encoded_hash.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?;
    let block_hash = decode_hash(&encoded_hash)?;
    let response: Option<RpcBlockResponse> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlock",
            "params": [encoded_hash],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getBlock",
            source,
        })?;
    let response = response.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?;
    let observed_number = decode_block_number(&response.block.header.number)?;
    if observed_number != block_number {
        return Err(DeepXTransactionWatchError::BlockNumberMismatch {
            requested: block_number,
            received: observed_number,
        });
    }
    let extrinsic_index = find_extrinsic(
        &response.block.extrinsics,
        target_extrinsic_hash,
        "canonical block",
    )?;

    Ok(DeepXCanonicalBlockObservation {
        block_number,
        block_hash,
        extrinsic_index,
    })
}

/// Collects a complete bounded canonical scan through the recovery endpoint's finalized head.
///
/// The supplied capability evidence must belong to the validated Recovery endpoint and include
/// every method required by the recovery flow. Exact transaction absence can then be classified by
/// [`DeepXRecoveryScan::classify`]. Finding the target extrinsic stops collection because block
/// location without authoritative dispatch and business-event evidence cannot construct a trusted
/// inclusion outcome.
///
/// # Errors
///
/// Returns an error for mismatched capabilities, RPC or decoding failure, an invalid scan plan,
/// inconsistent range collection, or target inclusion without authoritative event evidence.
pub async fn collect_finalized_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, DeepXTransactionWatchError> {
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        None,
    )
    .await
}

/// Collects canonical finalized evidence for an explicitly selected ordinary Spot cancel.
///
/// This read-only boundary uses the approved snapshot and durable operation to verify dispatch
/// and business events at the exact canonical block and extrinsic index. It does not commit,
/// submit, replay, or enable execution recovery. Non-atomic pool absence remains unknown.
///
/// # Errors
///
/// Returns an error for a foreign runtime, unsupported or fast operation, invalid canonical
/// checkpoint, RPC failure, or incomplete or conflicting inclusion event evidence.
pub async fn collect_finalized_spot_cancel_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, DeepXTransactionWatchError> {
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.operation(),
            Some(DeepXTransactionOperation::SpotCancel { .. })
        )
    {
        return Err(DeepXSpotCancelEventVerificationError::UnsupportedOperation.into());
    }
    if matches!(
        identity.operation(),
        Some(DeepXTransactionOperation::SpotCancel {
            fast_cancel: true,
            ..
        })
    ) {
        return Err(DeepXSpotCancelEventVerificationError::FastCancelUnsupported.into());
    }
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::Spot(snapshot, identity)),
    )
    .await
}

/// Collects canonical finalized evidence for an explicitly selected Spot placement.
///
/// This read-only boundary verifies dispatch and the exact side-specific order state event. It
/// does not commit, submit, replay, or enable Spot execution.
///
/// # Errors
///
/// Returns an error for a foreign runtime, unsupported operation, invalid canonical checkpoint,
/// RPC failure, or incomplete or conflicting inclusion event evidence.
pub async fn collect_finalized_spot_place_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, DeepXTransactionWatchError> {
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.operation(),
            Some(DeepXTransactionOperation::SpotPlace { .. })
        )
    {
        return Err(DeepXSpotPlaceEventVerificationError::UnsupportedOperation.into());
    }
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::SpotPlace(snapshot, identity)),
    )
    .await
}

enum CancelEventEvidence<'a> {
    Perp(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
    Spot(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
    SpotPlace(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
    PerpPlace(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
    PerpClose(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
    PerpProfitAndLossPoint(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
}

/// Collects bounded canonical finalized evidence for a close-generated perpetual order.
///
/// # Errors
///
/// Returns an error for unsupported scope, RPC failure or conflicting order evidence.
#[expect(
    clippy::too_many_arguments,
    reason = "matches bounded recovery scan inputs"
)]
pub async fn collect_finalized_perp_close_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, Box<DeepXTransactionWatchError>> {
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.operation(),
            Some(DeepXTransactionOperation::PerpClose { .. })
        )
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(Box::new(
            DeepXPerpCloseEventVerificationError::UnsupportedOperation.into(),
        ));
    }
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::PerpClose(snapshot, identity)),
    )
    .await
    .map_err(Box::new)
}

/// Collects bounded finalized recovery evidence for an explicit TP/SL update.
///
/// # Errors
///
/// Returns an error for unsupported scope, RPC failure or conflicting indexed events.
#[expect(
    clippy::too_many_arguments,
    reason = "matches bounded recovery scan inputs"
)]
pub async fn collect_finalized_perp_profit_and_loss_point_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, Box<DeepXTransactionWatchError>> {
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.operation(),
            Some(DeepXTransactionOperation::PerpProfitAndLossPoint { .. })
        )
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(Box::new(
            DeepXPerpProfitAndLossPointEventVerificationError::UnsupportedOperation.into(),
        ));
    }
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::PerpProfitAndLossPoint(
            snapshot, identity,
        )),
    )
    .await
    .map_err(Box::new)
}

/// Collects bounded finalized recovery evidence for an explicit perpetual placement.
///
/// # Errors
///
/// Returns an error for unsupported operation/runtime scope, RPC failure, or conflicting events.
#[expect(
    clippy::too_many_arguments,
    reason = "matches existing bounded recovery scan inputs"
)]
pub async fn collect_finalized_perp_place_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, Box<DeepXTransactionWatchError>> {
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.operation(),
            Some(DeepXTransactionOperation::PerpPlace { .. })
        )
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(Box::new(
            DeepXPerpPlaceEventVerificationError::UnsupportedOperation.into(),
        ));
    }
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::PerpPlace(snapshot, identity)),
    )
    .await
    .map_err(Box::new)
}

pub(crate) async fn collect_finalized_recovery_scan_with_event_evidence(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, DeepXTransactionWatchError> {
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::Perp(snapshot, identity)),
    )
    .await
}

async fn collect_finalized_recovery_scan_inner(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
    event_evidence: Option<CancelEventEvidence<'_>>,
) -> Result<DeepXFinalizedRecoveryCollection, DeepXTransactionWatchError> {
    let recovery_url = endpoints.url_for(DeepXRpcRole::Recovery);
    let recovery_capabilities = capabilities.for_role(DeepXRpcRole::Recovery);
    if recovery_capabilities.role() != DeepXRpcRole::Recovery
        || recovery_capabilities.endpoint_url() != recovery_url
        || DEEPX_RECOVERY_RPC_METHODS
            .iter()
            .any(|method| !recovery_capabilities.methods().contains(*method))
    {
        return Err(DeepXTransactionWatchError::CapabilitiesMismatch(
            DeepXRpcRole::Recovery,
        ));
    }
    let submission_url = endpoints.url_for(DeepXRpcRole::Submission);
    let submission_capabilities = capabilities.for_role(DeepXRpcRole::Submission);
    if submission_capabilities.role() != DeepXRpcRole::Submission
        || submission_capabilities.endpoint_url() != submission_url
        || DEEPX_SUBMISSION_RPC_METHODS
            .iter()
            .any(|method| !submission_capabilities.methods().contains(*method))
    {
        return Err(DeepXTransactionWatchError::CapabilitiesMismatch(
            DeepXRpcRole::Submission,
        ));
    }

    let client = BlockchainHttpRpcClient::new(recovery_url.to_string(), None, None);
    let encoded_checkpoint_hash: Option<String> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [last_scanned_block],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getBlockHash",
            source,
        })?;
    let encoded_checkpoint_hash = encoded_checkpoint_hash.ok_or(
        DeepXTransactionWatchError::BlockUnavailable(last_scanned_block),
    )?;
    let observed_checkpoint_hash = decode_hash(&encoded_checkpoint_hash)?;
    if observed_checkpoint_hash != last_scanned_block_hash {
        return Err(DeepXTransactionWatchError::RecoveryCheckpointMismatch {
            block_number: last_scanned_block,
            expected: last_scanned_block_hash,
            received: observed_checkpoint_hash,
        });
    }
    let encoded_finalized_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getFinalizedHead",
            "params": [],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getFinalizedHead",
            source,
        })?;
    let finalized_hash = decode_hash(&encoded_finalized_hash)?;
    let finalized_header: Option<RpcBlockHeader> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getHeader",
            "params": [encoded_finalized_hash],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getHeader",
            source,
        })?;
    let finalized_header =
        finalized_header.ok_or(DeepXTransactionWatchError::FinalizedHeaderUnavailable)?;
    let finalized_block_number = decode_block_number(&finalized_header.number)?;
    let checkpoint = DeepXFinalizedRecoveryCheckpoint {
        block_number: finalized_block_number,
        block_hash: finalized_hash,
    };

    let plan = plan_missed_block_scan(
        last_scanned_block,
        finalized_block_number,
        max_blocks_per_range,
    )?;
    let DeepXMissedBlockScanPlan::Scan(ranges) = plan else {
        if finalized_hash != last_scanned_block_hash {
            return Err(DeepXTransactionWatchError::RecoveryCheckpointMismatch {
                block_number: last_scanned_block,
                expected: last_scanned_block_hash,
                received: finalized_hash,
            });
        }
        return Ok(DeepXFinalizedRecoveryCollection::UpToDate(checkpoint));
    };
    let mut collector = DeepXRecoveryScanCollector::new(ranges)
        .ok_or(DeepXRecoveryScanCollectionError::CollectionComplete)?;
    while let Some(range) = collector.next_range() {
        let mut blocks = Vec::new();
        for block_number in range.first_block()..=range.last_block() {
            let observation =
                observe_canonical_block_at(recovery_url, block_number, target_extrinsic_hash)
                    .await?;
            if let Some(extrinsic_index) = observation.extrinsic_index() {
                let Some(ref evidence) = event_evidence else {
                    return Err(DeepXTransactionWatchError::EventEvidenceUnavailable {
                        block_number,
                        extrinsic_index,
                    });
                };
                let event_bytes = fetch_system_events(
                    recovery_url,
                    observation.block_hash(),
                    observation.block_number(),
                )
                .await?;
                let inclusion = match evidence {
                    CancelEventEvidence::PerpClose(snapshot, identity) => {
                        verify_perp_close_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                    CancelEventEvidence::PerpProfitAndLossPoint(snapshot, identity) => {
                        verify_perp_profit_and_loss_point_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                    CancelEventEvidence::PerpPlace(snapshot, identity) => {
                        verify_perp_place_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                    CancelEventEvidence::Perp(snapshot, identity) => {
                        verify_perp_cancel_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                    CancelEventEvidence::Spot(snapshot, identity) => {
                        verify_spot_cancel_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                    CancelEventEvidence::SpotPlace(snapshot, identity) => {
                        verify_spot_place_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                };
                blocks.push(DeepXCanonicalBlockEvidence::new(
                    observation.block_number(),
                    observation.block_hash(),
                    Some(inclusion),
                ));
                continue;
            }
            blocks.push(DeepXCanonicalBlockEvidence::new(
                observation.block_number(),
                observation.block_hash(),
                None,
            ));
        }
        collector.push_range(range, blocks)?;
    }
    let pool = observe_submission_pool(endpoints, target_extrinsic_hash).await?;
    let pool_evidence = match pool {
        DeepXPoolObservation::Present => DeepXSubmissionPoolEvidence::Present,
        DeepXPoolObservation::Absent => DeepXSubmissionPoolEvidence::Unknown,
    };
    let scan = collector.finish(finalized_hash, pool_evidence)?;
    Ok(DeepXFinalizedRecoveryCollection::Scan(scan))
}

async fn fetch_system_events(
    recovery_url: &str,
    block_hash: [u8; 32],
    block_number: u64,
) -> Result<Vec<u8>, DeepXTransactionWatchError> {
    let client = BlockchainHttpRpcClient::new(recovery_url.to_string(), None, None);
    let encoded: Option<String> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [SYSTEM_EVENTS_STORAGE_KEY, format!("0x{}", hex::encode(block_hash))],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "state_getStorage",
            source,
        })?;
    let encoded = encoded.ok_or(DeepXTransactionWatchError::EventStorageUnavailable(
        block_number,
    ))?;
    let value =
        encoded
            .strip_prefix("0x")
            .ok_or(DeepXTransactionWatchError::InvalidEventStorage(
                block_number,
            ))?;
    if value.is_empty() {
        return Err(DeepXTransactionWatchError::InvalidEventStorage(
            block_number,
        ));
    }
    hex::decode(value).map_err(|_| DeepXTransactionWatchError::InvalidEventStorage(block_number))
}

#[cfg(test)]
const SYSTEM_THREADS_STORAGE_KEY_PREFIX: &str = "System";
#[cfg(test)]
const SYSTEM_THREADS_STORAGE_ITEM: &str = "Threads";
#[cfg(test)]
const SYSTEM_EVENTS_MAP_STORAGE_ITEM: &str = "EventsMap";

#[cfg(test)]
fn system_map_storage_key(prefix: &str, item: &str, key: &[u8]) -> String {
    let mut encoded = Vec::with_capacity(32 + 16);
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(prefix.as_bytes()));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(item.as_bytes()));
    encoded.extend_from_slice(key);
    format!("0x{}", hex::encode(encoded))
}

#[cfg(test)]
fn system_threads_storage_key(block_number: u64) -> String {
    system_map_storage_key(
        SYSTEM_THREADS_STORAGE_KEY_PREFIX,
        SYSTEM_THREADS_STORAGE_ITEM,
        &block_number.to_le_bytes(),
    )
}

#[cfg(test)]
fn system_events_map_storage_key(block_number: u64, thread: u8) -> String {
    let block_key = sp_crypto_hashing::blake2_128(&block_number.to_le_bytes());
    let thread_key = sp_crypto_hashing::blake2_128(&[thread]);
    let mut encoded = Vec::with_capacity(64);
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(
        SYSTEM_THREADS_STORAGE_KEY_PREFIX.as_bytes(),
    ));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(
        SYSTEM_EVENTS_MAP_STORAGE_ITEM.as_bytes(),
    ));
    encoded.extend_from_slice(&block_key);
    encoded.extend_from_slice(&thread_key);
    format!("0x{}", hex::encode(encoded))
}

#[cfg(test)]
fn combine_events_map_batches(
    batches: &[Vec<u8>],
    block_number: u64,
) -> Result<Vec<u8>, DeepXTransactionWatchError> {
    let mut records = Vec::new();
    for batch in batches {
        let mut input = batch.as_slice();
        let count = Compact::<u32>::decode(&mut input)
            .map_err(|_| DeepXTransactionWatchError::InvalidEventStorage(block_number))?;
        let count = count.0 as usize;
        if input.is_empty() && count != 0 {
            return Err(DeepXTransactionWatchError::InvalidEventStorage(
                block_number,
            ));
        }
        records.extend_from_slice(input);
        if !input.is_empty() && count == 0 {
            return Err(DeepXTransactionWatchError::InvalidEventStorage(
                block_number,
            ));
        }
    }
    let count = batches
        .iter()
        .try_fold(0usize, |total, batch| {
            let mut input = batch.as_slice();
            let count = Compact::<u32>::decode(&mut input).ok()?.0 as usize;
            Some(total.checked_add(count)?)
        })
        .ok_or(DeepXTransactionWatchError::InvalidEventStorage(
            block_number,
        ))?;
    let mut combined = Compact(count as u32).encode();
    combined.extend_from_slice(&records);
    Ok(combined)
}

/// Observes exact transaction membership in the submission endpoint's pending pool.
///
/// `Absent` is returned only after every entry was decoded and hashed successfully. Because the
/// pool response is not atomic with any chain checkpoint, this observation must not be converted
/// directly into authoritative absence or `not-included` evidence.
///
/// # Errors
///
/// Returns an error for RPC failure, malformed pending extrinsics, or duplicate target entries.
pub async fn observe_submission_pool(
    endpoints: &DeepXValidatedRpcEndpoints,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXPoolObservation, DeepXTransactionWatchError> {
    let client = BlockchainHttpRpcClient::new(
        endpoints.url_for(DeepXRpcRole::Submission).to_string(),
        None,
        None,
    );
    let extrinsics: Vec<String> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "author_pendingExtrinsics",
            "params": [],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "author_pendingExtrinsics",
            source,
        })?;

    Ok(
        if find_extrinsic(&extrinsics, target_extrinsic_hash, "submission pool")?.is_some() {
            DeepXPoolObservation::Present
        } else {
            DeepXPoolObservation::Absent
        },
    )
}

fn find_extrinsic(
    encoded_extrinsics: &[String],
    target_hash: [u8; 32],
    source: &'static str,
) -> Result<Option<u32>, DeepXTransactionWatchError> {
    let mut matched = None;
    for (index, encoded) in encoded_extrinsics.iter().enumerate() {
        let bytes = encoded
            .strip_prefix("0x")
            .ok_or(DeepXTransactionWatchError::InvalidExtrinsic {
                evidence_source: source,
                index,
            })
            .and_then(|value| {
                hex::decode(value).map_err(|_| DeepXTransactionWatchError::InvalidExtrinsic {
                    evidence_source: source,
                    index,
                })
            })?;
        validate_extrinsic(&bytes).map_err(|()| DeepXTransactionWatchError::InvalidExtrinsic {
            evidence_source: source,
            index,
        })?;
        if BlakeTwo256.hash(&bytes).0 == target_hash {
            let index =
                u32::try_from(index).map_err(|_| DeepXTransactionWatchError::InvalidExtrinsic {
                    evidence_source: source,
                    index,
                })?;
            if matched.replace(index).is_some() {
                return Err(DeepXTransactionWatchError::DuplicateExtrinsic {
                    evidence_source: source,
                });
            }
        }
    }
    Ok(matched)
}

fn validate_extrinsic(bytes: &[u8]) -> Result<(), ()> {
    let mut remaining = bytes;
    let payload_length = Compact::<u32>::decode(&mut remaining).map_err(|_| ())?.0;
    usize::try_from(payload_length)
        .ok()
        .filter(|length| *length == remaining.len())
        .map(|_| ())
        .ok_or(())
}

fn decode_hash(encoded: &str) -> Result<[u8; 32], DeepXTransactionWatchError> {
    encoded
        .strip_prefix("0x")
        .ok_or_else(|| DeepXTransactionWatchError::InvalidBlockHash(encoded.to_string()))
        .and_then(|value| {
            hex::decode_array::<32>(value)
                .map_err(|_| DeepXTransactionWatchError::InvalidBlockHash(encoded.to_string()))
        })
}

fn decode_block_number(encoded: &str) -> Result<u64, DeepXTransactionWatchError> {
    let value = encoded
        .strip_prefix("0x")
        .ok_or_else(|| DeepXTransactionWatchError::InvalidBlockNumber(encoded.to_string()))?;
    u64::from_str_radix(value, 16)
        .map_err(|_| DeepXTransactionWatchError::InvalidBlockNumber(encoded.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{Json, Router, extract::State, routing::post};
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde_json::Value;
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn events_map_storage_keys_use_substrate_hashers() {
        let threads = system_threads_storage_key(42);
        let events = system_events_map_storage_key(42, 3);
        assert_eq!(threads.len(), 82);
        assert_eq!(events.len(), 130);
        assert_ne!(events, system_events_map_storage_key(42, 4));
        assert_ne!(events, system_events_map_storage_key(43, 3));
    }

    #[test]
    fn events_map_batches_combine_compact_counts() {
        let mut first = Compact(1_u32).encode();
        first.push(10);
        let mut second = Compact(2_u32).encode();
        second.extend_from_slice(&[20, 30]);
        let combined = combine_events_map_batches(&[first, second], 42).unwrap();
        assert_eq!(
            combined,
            [Compact(3_u32).encode(), vec![10, 20, 30]].concat()
        );
    }

    #[test]
    fn events_map_batches_reject_nonempty_zero_count() {
        let mut batch = Compact(0_u32).encode();
        batch.push(1);
        assert!(combine_events_map_batches(&[batch], 42).is_err());
    }
    use crate::{
        common::{
            DeepXEnvironment, DeepXKeyScheme, DeepXPrivateKey, consts::DEEPX_TESTNET_GENESIS_HASH,
        },
        config::{DeepXNetworkConfig, DeepXObservedRpcEndpoint, validate_rpc_endpoint_identities},
        rpc::observe_and_validate_rpc_method_capabilities,
        signing::derive_signer_account_id,
        transaction::{
            DeepXDirectRuntimeIdentity, DeepXInclusionOutcome, DeepXNonceReservation,
            DeepXRecoveryDecision,
        },
    };
    use nautilus_model::{
        enums::OrderSide,
        identifiers::{ClientOrderId, InstrumentId},
    };

    #[derive(Clone)]
    struct RpcState {
        extrinsics: Arc<[String]>,
        block_number: String,
    }

    async fn rpc(State(state): State<RpcState>, Json(request): Json<Value>) -> Json<Value> {
        let result = match request["method"].as_str().unwrap() {
            "chain_getBlockHash" => json!(format!("0x{}", "2a".repeat(32))),
            "chain_getBlock" => json!({
                "block": {
                    "header": { "number": state.block_number },
                    "extrinsics": state.extrinsics.as_ref(),
                },
            }),
            "author_pendingExtrinsics" => json!(state.extrinsics.as_ref()),
            method => panic!("unexpected method {method}"),
        };
        Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
    }

    async fn endpoints(extrinsics: Vec<String>, block_number: &str) -> DeepXValidatedRpcEndpoints {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = RpcState {
            extrinsics: Arc::from(extrinsics),
            block_number: block_number.to_string(),
        };
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/", post(rpc)).with_state(state),
            )
            .await
            .unwrap();
        });
        let url = format!("http://{address}");
        let config = DeepXNetworkConfig {
            base_url_rpc: Some(url.clone()),
            ..Default::default()
        };
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        validate_rpc_endpoint_identities(
            &config,
            [
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Submission, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Recovery, url, genesis_hash),
            ],
        )
        .unwrap()
    }

    fn extrinsic(payload: &[u8]) -> Vec<u8> {
        let mut encoded = Compact(u32::try_from(payload.len()).unwrap()).encode();
        encoded.extend_from_slice(payload);
        encoded
    }

    fn inclusion(
        block_hash: [u8; 32],
        block_number: u64,
        extrinsic_index: u32,
    ) -> DeepXInclusionEvidence {
        DeepXInclusionEvidence::from_indexed_observations(
            block_hash,
            block_number,
            super::super::DeepXIndexedOutcome {
                extrinsic_index,
                outcome: super::super::DeepXDispatchOutcome::Success,
            },
            super::super::DeepXIndexedOutcome {
                extrinsic_index,
                outcome: super::super::DeepXBusinessEventOutcome::Success,
            },
        )
        .unwrap()
    }

    fn runtime_snapshot() -> RuntimeSnapshot {
        #[derive(Deserialize)]
        struct RpcResponse {
            result: String,
        }
        let metadata: RpcResponse = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json",
        )))
        .unwrap();
        RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap(),
            366,
            1,
            &hex::decode(metadata.result.trim_start_matches("0x")).unwrap(),
        )
        .unwrap()
    }

    fn runtime_snapshot_for_spec(spec: u32) -> RuntimeSnapshot {
        if spec == 366 {
            runtime_snapshot()
        } else {
            let metadata: Value = serde_json::from_str(include_str!(concat!(env!("CARGO_MANIFEST_DIR"),
                "/test_data/runtime/testnet/genesis-86604388_metadata-98136fdb_spec-369_tx-1_finalized-95febbff/metadata.json"))).unwrap();
            RuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
                hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap(),
                369,
                1,
                &hex::decode(
                    metadata["result"]
                        .as_str()
                        .unwrap()
                        .trim_start_matches("0x"),
                )
                .unwrap(),
            )
            .unwrap()
        }
    }

    #[rstest]
    #[case(0, 0)]
    #[case(u128::MAX, 0)]
    #[case(0, u128::MAX)]
    #[case(u128::MAX, 17)]
    fn perp_profit_and_loss_events_bind_points(
        #[case] take: u128,
        #[case] stop: u128,
        #[values(366, 369)] spec: u32,
    ) {
        let snapshot = runtime_snapshot_for_spec(spec);
        let baseline = cancel_identity([0x11; 20], 7, false);
        let identity = DeepXTransactionIdentity::new_perp_profit_and_loss_point(
            ClientOrderId::new(baseline.client_order_id()),
            baseline.signer(),
            baseline.instrument_id(),
            baseline.order_side(),
            baseline.nonce(),
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
            crate::signing::DeepXPerpProfitAndLossPointParams {
                subaccount: [0x11; 20],
                market_id: 100,
                take_profit_point: take,
                stop_loss_point: stop,
            },
        );
        let optional = |point| {
            if point == 0 {
                ScaleValue::unnamed_variant("None", [])
            } else {
                ScaleValue::unnamed_variant("Some", [ScaleValue::u128(point)])
            }
        };
        let position = ScaleValue::named_composite([
            ("market_id", ScaleValue::u128(100)),
            ("is_long", ScaleValue::bool(false)),
            ("base_asset_amount", ScaleValue::u128(1)),
            ("entry_price", ScaleValue::u128(2)),
            ("leverage", ScaleValue::u128(3)),
            ("last_funding_rate", ScaleValue::i128(-4)),
            ("version", ScaleValue::u128(5)),
            ("realized_pnl", ScaleValue::i128(-6)),
            ("funding_payment", ScaleValue::i128(7)),
            ("owner", ScaleValue::from_bytes([0x11; 20])),
            ("take_profit", optional(take)),
            ("stop_loss", optional(stop)),
            ("last_settle_price", ScaleValue::u128(8)),
        ]);
        let fields = ScaleValue::named_composite([
            ("owner", ScaleValue::from_bytes([0x11; 20])),
            ("market_id", ScaleValue::u128(100)),
            ("pos", position),
            ("pnl", ScaleValue::i128(0)),
        ]);
        let pallet = snapshot.metadata().pallet_by_name("PerpMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|e| e.name == "PositionUpdated")
            .unwrap();
        let encode = |fields: &ScaleValue<()>, index| {
            let ValueDef::Composite(Composite::Named(fields)) = &fields.value else {
                panic!();
            };
            let mut record = Phase::ApplyExtrinsic(index).encode();
            record.extend([pallet.index(), event.index]);
            for field in &event.fields {
                let value = &fields
                    .iter()
                    .find(|(name, _)| Some(name) == field.name.as_ref())
                    .unwrap()
                    .1;
                subxt_core::ext::scale_value::scale::encode_as_type(
                    value,
                    field.ty.id,
                    snapshot.metadata().types(),
                    &mut record,
                )
                .unwrap();
            }
            Vec::<[u8; 32]>::new().encode_to(&mut record);
            record
        };
        let verify = |bytes: &[u8]| {
            verify_perp_profit_and_loss_point_inclusion_events(
                &snapshot, &identity, [8; 32], 42, 4, bytes,
            )
        };
        let business = encode(&fields, 4);
        let dispatch = dispatch_event_record(&snapshot, 4, true);
        let valid = system_events(&[business.clone(), dispatch.clone()]);
        assert!(verify(&valid).is_ok());
        assert!(
            verify_perp_profit_and_loss_point_inclusion_events(
                &snapshot, &baseline, [8; 32], 42, 4, &valid
            )
            .is_err()
        );
        if spec == 369 {
            assert!(
                verify_perp_profit_and_loss_point_inclusion_events(
                    &runtime_snapshot(),
                    &identity,
                    [8; 32],
                    42,
                    4,
                    &valid
                )
                .is_err()
            );
        }
        assert!(verify(&system_events(std::slice::from_ref(&business))).is_err());
        assert!(
            verify(&system_events(&[
                business.clone(),
                dispatch.clone(),
                dispatch.clone()
            ]))
            .is_err()
        );
        let mut wrong_count = valid.clone();
        wrong_count[0] = Compact(3_u32).encode()[0];
        assert!(verify(&wrong_count).is_err());
        assert!(
            verify(&system_events(&[
                business.clone(),
                business,
                dispatch.clone()
            ]))
            .is_err()
        );
        assert!(verify(&system_events(&[encode(&fields, 5), dispatch.clone()])).is_err());
        assert!(verify(&system_events(std::slice::from_ref(&dispatch))).is_err());
        assert!(
            verify(&system_events(&[dispatch_event_record(
                &snapshot, 4, false
            )]))
            .is_ok()
        );
        assert!(verify(&valid[..valid.len() - 1]).is_err());
        let mut trailing = valid;
        trailing.push(0);
        assert!(verify(&trailing).is_err());
        for path in [
            "owner",
            "market_id",
            "pnl",
            "pos.owner",
            "pos.market_id",
            "pos.take_profit",
            "pos.stop_loss",
        ] {
            let mut changed = fields.clone();
            let replacement = match path {
                "owner" | "pos.owner" => ScaleValue::from_bytes([0x22; 20]),
                "market_id" | "pos.market_id" => ScaleValue::u128(101),
                "pnl" => ScaleValue::i128(1),
                _ => ScaleValue::unnamed_variant("Some", [ScaleValue::u128(1)]),
            };
            let ValueDef::Composite(Composite::Named(outer)) = &mut changed.value else {
                panic!();
            };
            if let Some(name) = path.strip_prefix("pos.") {
                let pos = &mut outer.iter_mut().find(|(name, _)| name == "pos").unwrap().1;
                let ValueDef::Composite(Composite::Named(inner)) = &mut pos.value else {
                    panic!();
                };
                inner.iter_mut().find(|(key, _)| key == name).unwrap().1 = replacement;
            } else {
                outer.iter_mut().find(|(key, _)| key == path).unwrap().1 = replacement;
            }
            assert!(
                verify(&system_events(&[encode(&changed, 4), dispatch.clone()])).is_err(),
                "{path}"
            );
        }
    }

    #[rstest]
    #[case(0, None)]
    #[case(0, Some(u64::MAX))]
    #[case(u128::MAX, Some(19))]
    fn perp_close_events_bind_generated_order(
        #[case] price: u128,
        #[case] slippage: Option<u64>,
        #[values(366, 369)] spec: u32,
    ) {
        let snapshot = runtime_snapshot_for_spec(spec);
        let baseline = perp_place_identity(true, price == 0);
        let close = DeepXTransactionIdentity::new_perp_close(
            ClientOrderId::new(baseline.client_order_id()),
            baseline.signer(),
            baseline.instrument_id(),
            OrderSide::Sell,
            baseline.nonce(),
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
            crate::signing::DeepXPerpCloseParams {
                subaccount: [0x11; 20],
                market_id: 100,
                price,
                slippage,
            },
        );
        let generated = DeepXTransactionIdentity::new_perp_close(
            ClientOrderId::new(baseline.client_order_id()),
            baseline.signer(),
            baseline.instrument_id(),
            OrderSide::Sell,
            DeepXNonceReservation::TimestampOrderId { value: 19 },
            baseline.runtime().clone(),
            crate::signing::DeepXPerpCloseParams {
                subaccount: [0x11; 20],
                market_id: 100,
                price,
                slippage,
            },
        );
        assert_ne!(generated.nonce(), close.nonce());
        let mut order = perp_place_order_value(&baseline, 42);
        let ValueDef::Composite(Composite::Named(fields)) = &mut order.value else {
            panic!();
        };
        let mut set = |name, replacement| {
            fields.iter_mut().find(|(key, _)| key == name).unwrap().1 = replacement;
        };
        set("order_id", ScaleValue::u128(19));
        set("reduce_only", ScaleValue::bool(true));
        set("take_profit", ScaleValue::unnamed_variant("None", []));
        set("stop_loss", ScaleValue::unnamed_variant("None", []));
        set(
            "order_type",
            if price == 0 {
                ScaleValue::unnamed_variant(
                    "Market",
                    [match slippage {
                        None => ScaleValue::unnamed_variant("None", []),
                        Some(value) => ScaleValue::unnamed_variant(
                            "Some",
                            [ScaleValue::u128(u128::from(value))],
                        ),
                    }],
                )
            } else {
                ScaleValue::unnamed_variant("Stop", [])
            },
        );
        let business =
            |order: &ScaleValue<()>| perp_place_event_record(&snapshot, 4, &generated, order);
        let dispatch = dispatch_event_record(&snapshot, 4, true);
        let verify = |events: &[u8]| {
            verify_perp_close_inclusion_events(&snapshot, &close, [8; 32], 42, 4, events)
        };
        let valid = system_events(&[business(&order), dispatch.clone()]);
        assert!(verify(&valid).is_ok());
        assert!(verify(&system_events(&[business(&order)])).is_err());
        assert!(
            verify(&system_events(&[
                perp_place_event_record(&snapshot, 5, &generated, &order),
                dispatch.clone()
            ]))
            .is_err()
        );
        if spec == 369 {
            assert!(
                verify_perp_close_inclusion_events(
                    &runtime_snapshot(),
                    &close,
                    [8; 32],
                    42,
                    4,
                    &valid
                )
                .is_err()
            );
        }
        if price != 0 {
            let mut changed = order.clone();
            let ValueDef::Composite(Composite::Named(fields)) = &mut changed.value else {
                panic!();
            };
            fields
                .iter_mut()
                .find(|(name, _)| name == "price")
                .unwrap()
                .1 = ScaleValue::u128(1);
            assert!(verify(&system_events(&[business(&changed), dispatch.clone()])).is_err());
        }
        assert!(
            verify(&system_events(&[
                business(&order),
                business(&order),
                dispatch.clone()
            ]))
            .is_err()
        );
        assert!(verify(&system_events(std::slice::from_ref(&dispatch))).is_err());
        assert!(
            verify(&system_events(&[dispatch_event_record(
                &snapshot, 4, false
            )]))
            .is_ok()
        );
        assert!(verify(&valid[..valid.len() - 1]).is_err());
        let mut trailing = valid;
        trailing.push(0);
        assert!(verify(&trailing).is_err());
        for (name, replacement) in [
            ("order_id", ScaleValue::u128(20)),
            ("owner", ScaleValue::from_bytes([0x22; 20])),
            ("market_id", ScaleValue::u128(101)),
            ("reduce_only", ScaleValue::bool(false)),
            (
                "take_profit",
                ScaleValue::unnamed_variant("Some", [ScaleValue::u128(1)]),
            ),
            (
                "stop_loss",
                ScaleValue::unnamed_variant("Some", [ScaleValue::u128(1)]),
            ),
            ("post_only", ScaleValue::unnamed_variant("MustPostOnly", [])),
            (
                "order_type",
                ScaleValue::unnamed_variant(
                    "Market",
                    [ScaleValue::unnamed_variant("Some", [ScaleValue::u128(1)])],
                ),
            ),
            ("size_filled", ScaleValue::u128(1)),
            ("size_remain", ScaleValue::u128(1)),
            ("create_time", ScaleValue::u128(41)),
            ("status", ScaleValue::unnamed_variant("Filled", [])),
        ] {
            let mut changed = order.clone();
            let ValueDef::Composite(Composite::Named(fields)) = &mut changed.value else {
                panic!();
            };
            fields.iter_mut().find(|(key, _)| key == name).unwrap().1 = replacement;
            assert!(
                verify(&system_events(&[business(&changed), dispatch.clone()])).is_err(),
                "{name}"
            );
        }
    }

    fn perp_place_identity(is_long: bool, market: bool) -> DeepXTransactionIdentity {
        let baseline = cancel_identity([0x11; 20], 7, false);
        DeepXTransactionIdentity::new_perp_place(
            ClientOrderId::new(baseline.client_order_id()),
            baseline.signer(),
            baseline.instrument_id(),
            if is_long {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            baseline.nonce(),
            baseline.runtime().clone(),
            crate::signing::DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 100,
                is_long,
                size: u128::MAX,
                price: if market { 0 } else { u128::MAX },
                order_type: if market {
                    crate::signing::DeepXPerpOrderType::Market(Some(0))
                } else {
                    crate::signing::DeepXPerpOrderType::Limit(crate::signing::DeepXTimeInForce::Gtc)
                },
                take_profit: Some(u128::MAX),
                stop_loss: Some(0),
                reduce_only: false,
                post_only: crate::signing::DeepXPostOnlyParam::None,
            },
        )
    }

    fn perp_place_order_value(
        identity: &DeepXTransactionIdentity,
        block_number: u64,
    ) -> ScaleValue<()> {
        let Some(DeepXTransactionOperation::PerpPlace {
            subaccount,
            market_id,
            is_long,
            size,
            price,
            order_type,
            take_profit,
            stop_loss,
            reduce_only,
            post_only,
        }) = identity.operation()
        else {
            unreachable!();
        };
        let DeepXNonceReservation::TimestampOrderId { value: order_id } = identity.nonce() else {
            unreachable!();
        };
        let optional = |point: Option<u128>| match point {
            Some(value) => ScaleValue::unnamed_variant("Some", [ScaleValue::u128(value)]),
            None => ScaleValue::unnamed_variant("None", []),
        };
        let order_type_value = match order_type {
            crate::signing::DeepXPerpOrderType::Limit(tif) => ScaleValue::unnamed_variant(
                "Limit",
                [ScaleValue::unnamed_variant(
                    match tif {
                        crate::signing::DeepXTimeInForce::Gtc => "GTC",
                        crate::signing::DeepXTimeInForce::Ioc => "IOC",
                        crate::signing::DeepXTimeInForce::Fok => "FOK",
                    },
                    [],
                )],
            ),
            crate::signing::DeepXPerpOrderType::Market(slippage) => {
                ScaleValue::unnamed_variant("Market", [optional(slippage.map(u128::from))])
            }
            crate::signing::DeepXPerpOrderType::Stop => ScaleValue::unnamed_variant("Stop", []),
        };
        let post_only = match post_only {
            crate::signing::DeepXPostOnlyParam::None => "None",
            crate::signing::DeepXPostOnlyParam::MustPostOnly => "MustPostOnly",
            crate::signing::DeepXPostOnlyParam::Adaptive => "Adaptive",
        };
        ScaleValue::named_composite([
            ("order_id", ScaleValue::u128(u128::from(order_id))),
            ("owner", ScaleValue::from_bytes(subaccount)),
            ("market_id", ScaleValue::u128(u128::from(*market_id))),
            ("is_long", ScaleValue::bool(*is_long)),
            ("size", ScaleValue::u128(*size)),
            (
                "price",
                ScaleValue::u128(
                    if matches!(order_type, crate::signing::DeepXPerpOrderType::Market(_)) {
                        123
                    } else {
                        *price
                    },
                ),
            ),
            ("order_type", order_type_value),
            ("create_time", ScaleValue::u128(u128::from(block_number))),
            ("leverage", ScaleValue::u128(25000)),
            ("status", ScaleValue::unnamed_variant("Open", [])),
            ("size_filled", ScaleValue::u128(0)),
            ("size_remain", ScaleValue::u128(*size)),
            ("take_profit", optional(*take_profit)),
            ("stop_loss", optional(*stop_loss)),
            ("reduce_only", ScaleValue::bool(*reduce_only)),
            ("post_only", ScaleValue::unnamed_variant(post_only, [])),
        ])
    }

    fn perp_place_event_record(
        snapshot: &RuntimeSnapshot,
        index: u32,
        identity: &DeepXTransactionIdentity,
        order: &ScaleValue<()>,
    ) -> Vec<u8> {
        let pallet = snapshot.metadata().pallet_by_name("PerpMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == "OrderPlaced")
            .unwrap();
        let DeepXNonceReservation::TimestampOrderId { value: order_id } = identity.nonce() else {
            unreachable!();
        };
        let mut record = Phase::ApplyExtrinsic(index).encode();
        record.extend([pallet.index(), event.index]);
        for field in &event.fields {
            let value = match field.name.as_deref() {
                Some("order_id") => ScaleValue::u128(u128::from(order_id)),
                Some("order") => order.clone(),
                _ => panic!("unexpected placement field"),
            };
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        record
    }

    #[rstest]
    #[case(true, false)]
    #[case(false, false)]
    #[case(true, true)]
    #[case(false, true)]
    fn perp_place_events_bind_exact_inputs(#[case] is_long: bool, #[case] market: bool) {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(is_long, market);
        let events = system_events(&[
            perp_place_event_record(
                &snapshot,
                4,
                &identity,
                &perp_place_order_value(&identity, 42),
            ),
            dispatch_event_record(&snapshot, 4, true),
        ]);
        let evidence =
            verify_perp_place_inclusion_events(&snapshot, &identity, [8; 32], 42, 4, &events)
                .unwrap();
        assert_eq!(
            evidence.outcome,
            super::super::DeepXInclusionOutcome::Success
        );
        assert_eq!(evidence.extrinsic_index, 4);
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
    fn perp_place_conflicting_order_fields_fail(#[case] name: &str, #[case] value: ScaleValue<()>) {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(true, false);
        let mut order = perp_place_order_value(&identity, 42);
        let ValueDef::Composite(Composite::Named(fields)) = &mut order.value else {
            unreachable!();
        };
        *fields
            .iter_mut()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value)
            .unwrap() = value;
        let events = system_events(&[
            perp_place_event_record(&snapshot, 4, &identity, &order),
            dispatch_event_record(&snapshot, 4, true),
        ]);
        assert!(matches!(
            verify_perp_place_inclusion_events(&snapshot, &identity, [8; 32], 42, 4, &events),
            Err(DeepXPerpPlaceEventVerificationError::ConflictingEvent)
        ));
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    fn perp_place_missing_duplicate_or_malformed_events_fail(#[case] mutation: u8) {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(true, false);
        let order = perp_place_event_record(
            &snapshot,
            4,
            &identity,
            &perp_place_order_value(&identity, 42),
        );
        let dispatch = dispatch_event_record(&snapshot, 4, true);
        let mut events = match mutation {
            0 => system_events(&[dispatch]),
            1 => system_events(&[order.clone(), order, dispatch]),
            2 => system_events(&[order, dispatch.clone(), dispatch]),
            _ => system_events(&[order, dispatch]),
        };
        if mutation == 3 {
            events.pop();
        }
        if mutation == 4 {
            events.push(0);
        }
        assert!(
            verify_perp_place_inclusion_events(&snapshot, &identity, [8; 32], 42, 4, &events)
                .is_err()
        );
    }

    #[rstest]
    fn perp_place_failed_dispatch_needs_no_placement_event() {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(true, false);
        let events = system_events(&[dispatch_event_record(&snapshot, 4, false)]);
        let evidence =
            verify_perp_place_inclusion_events(&snapshot, &identity, [8; 32], 42, 4, &events)
                .unwrap();
        assert_eq!(
            evidence.outcome,
            super::super::DeepXInclusionOutcome::Failed
        );
    }

    #[rstest]
    #[case(5, 4)]
    #[case(4, 5)]
    fn perp_place_events_at_other_indices_cannot_prove_inclusion(
        #[case] order_index: u32,
        #[case] dispatch_index: u32,
    ) {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(true, false);
        let events = system_events(&[
            perp_place_event_record(
                &snapshot,
                order_index,
                &identity,
                &perp_place_order_value(&identity, 42),
            ),
            dispatch_event_record(&snapshot, dispatch_index, true),
        ]);
        assert!(
            verify_perp_place_inclusion_events(&snapshot, &identity, [8; 32], 42, 4, &events)
                .is_err()
        );
    }

    #[rstest]
    fn perp_place_spec369_event_binding_requires_exact_runtime_scope() {
        let metadata: Value = serde_json::from_str(include_str!(concat!(env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/genesis-86604388_metadata-98136fdb_spec-369_tx-1_finalized-95febbff/metadata.json"))).unwrap();
        let snapshot = RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap(),
            369,
            1,
            &hex::decode(
                metadata["result"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("0x"),
            )
            .unwrap(),
        )
        .unwrap();
        let old_identity = perp_place_identity(true, false);
        let mut encoded = serde_json::to_value(&old_identity).unwrap();
        encoded["runtime"] =
            serde_json::to_value(DeepXDirectRuntimeIdentity::from(snapshot.identity())).unwrap();
        let identity: DeepXTransactionIdentity = serde_json::from_value(encoded).unwrap();
        let events = system_events(&[
            perp_place_event_record(
                &snapshot,
                4,
                &identity,
                &perp_place_order_value(&identity, 42),
            ),
            dispatch_event_record(&snapshot, 4, true),
        ]);
        assert!(
            verify_perp_place_inclusion_events(&snapshot, &identity, [8; 32], 42, 4, &events)
                .is_ok()
        );
        assert!(matches!(
            verify_perp_place_inclusion_events(&snapshot, &old_identity, [8; 32], 42, 4, &events),
            Err(DeepXPerpPlaceEventVerificationError::UnsupportedOperation)
        ));
    }

    #[rstest]
    #[tokio::test]
    async fn finalized_perp_order_scan_verifies_same_index_business_events(
        #[values(false, true)] close: bool,
    ) {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(true, false);
        let close_identity = DeepXTransactionIdentity::new_perp_close(
            ClientOrderId::new(identity.client_order_id()),
            identity.signer(),
            identity.instrument_id(),
            OrderSide::Sell,
            identity.nonce(),
            identity.runtime().clone(),
            crate::signing::DeepXPerpCloseParams {
                subaccount: [0x11; 20],
                market_id: 100,
                price: u128::MAX,
                slippage: None,
            },
        );
        let mut order = perp_place_order_value(&identity, 41);
        if close {
            let ValueDef::Composite(Composite::Named(fields)) = &mut order.value else {
                panic!();
            };
            for (name, value) in [
                ("order_type", ScaleValue::unnamed_variant("Stop", [])),
                ("take_profit", ScaleValue::unnamed_variant("None", [])),
                ("stop_loss", ScaleValue::unnamed_variant("None", [])),
                ("reduce_only", ScaleValue::bool(true)),
            ] {
                fields.iter_mut().find(|(key, _)| key == name).unwrap().1 = value;
            }
        }
        let target = extrinsic(&[1, 2, 3, 4]);
        let hash = BlakeTwo256.hash(&target).0;
        let blocks = BTreeMap::from([(41, vec![format!("0x{}", hex::encode(target))])]);
        let events = system_events(&[
            perp_place_event_record(&snapshot, 0, &identity, &order),
            dispatch_event_record(&snapshot, 0, true),
        ]);
        let storage = BTreeMap::from([(41, format!("0x{}", hex::encode(events)))]);
        let (endpoints, capabilities, _) =
            recovery_endpoints_with_events(42, None, blocks, vec![], storage).await;
        let collection = if close {
            collect_finalized_perp_close_recovery_scan(
                &endpoints,
                &capabilities,
                &snapshot,
                &close_identity,
                39,
                decode_hash(&block_hash(39)).unwrap(),
                2,
                hash,
            )
            .await
            .unwrap()
        } else {
            collect_finalized_perp_place_recovery_scan(
                &endpoints,
                &capabilities,
                &snapshot,
                &identity,
                39,
                decode_hash(&block_hash(39)).unwrap(),
                2,
                hash,
            )
            .await
            .unwrap()
        };
        let DeepXFinalizedRecoveryCollection::Scan(scan) = collection else {
            panic!("expected scan");
        };
        assert_eq!(
            scan.classify(),
            DeepXRecoveryDecision::FinalizedInclusion(inclusion(
                decode_hash(&block_hash(41)).unwrap(),
                41,
                0
            ))
        );
    }

    fn cancel_identity(
        subaccount: [u8; 20],
        order_id: u64,
        fast_cancel: bool,
    ) -> DeepXTransactionIdentity {
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let snapshot = runtime_snapshot();
        DeepXTransactionIdentity::new_perp_cancel(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            derive_signer_account_id(&key).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId {
                value: 1_725_000_000_125,
            },
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
            subaccount,
            order_id,
            100,
            fast_cancel,
        )
    }

    fn spot_place_identity(is_buy: bool) -> DeepXTransactionIdentity {
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let snapshot = runtime_snapshot();
        DeepXTransactionIdentity::new_spot_place(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            derive_signer_account_id(&key).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC.DEEPX").unwrap(),
            if is_buy {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            DeepXNonceReservation::TimestampOrderId {
                value: 1_725_000_000_125,
            },
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
            crate::signing::DeepXSpotPlaceParams {
                subaccount: [0x11; 20],
                pair: [0xa5; 32],
                is_buy,
                quote_amount: std::array::from_fn(|index| index as u8),
                base_amount: std::array::from_fn(|index| (31 - index) as u8),
                order_type: crate::signing::DeepXSpotOrderType::Limit(
                    crate::signing::DeepXTimeInForce::Gtc,
                ),
                post_only: crate::signing::DeepXPostOnlyParam::MustPostOnly,
                reduce_only: false,
            },
        )
    }

    fn u256_value(bytes: [u8; 32]) -> ScaleValue<()> {
        ScaleValue::unnamed_composite([ScaleValue::unnamed_composite(bytes.chunks_exact(8).map(
            |chunk| ScaleValue::u128(u128::from(u64::from_le_bytes(chunk.try_into().unwrap()))),
        ))])
    }

    fn spot_place_event_record(
        snapshot: &RuntimeSnapshot,
        extrinsic_index: u32,
        identity: &DeepXTransactionIdentity,
    ) -> Vec<u8> {
        let Some(DeepXTransactionOperation::SpotPlace {
            subaccount,
            pair,
            is_buy,
            quote_amount,
            base_amount,
            order_type,
            post_only,
            reduce_only,
        }) = identity.operation()
        else {
            unreachable!()
        };
        let DeepXNonceReservation::TimestampOrderId { value: order_id } = identity.nonce() else {
            unreachable!()
        };
        let pallet = snapshot.metadata().pallet_by_name("SpotMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| {
                event.name
                    == if *is_buy {
                        "StateOrderBuy"
                    } else {
                        "StateOrderSell"
                    }
            })
            .unwrap();
        assert_eq!(event.fields.len(), 1);
        assert_eq!(event.fields[0].name.as_deref(), Some("order"));
        let order_type = match order_type {
            crate::signing::DeepXSpotOrderType::Limit(time_in_force) => {
                let name = match time_in_force {
                    crate::signing::DeepXTimeInForce::Gtc => "GTC",
                    crate::signing::DeepXTimeInForce::Ioc => "IOC",
                    crate::signing::DeepXTimeInForce::Fok => "FOK",
                };
                ScaleValue::unnamed_variant("Limit", [ScaleValue::unnamed_variant(name, [])])
            }
            crate::signing::DeepXSpotOrderType::Market(slippage) => {
                let value = match slippage {
                    Some(value) => {
                        ScaleValue::unnamed_variant("Some", [ScaleValue::u128(u128::from(*value))])
                    }
                    None => ScaleValue::unnamed_variant("None", []),
                };
                ScaleValue::unnamed_variant("Market", [value])
            }
            crate::signing::DeepXSpotOrderType::Stop => ScaleValue::unnamed_variant("Stop", []),
        };
        let post_only = match post_only {
            crate::signing::DeepXPostOnlyParam::None => "None",
            crate::signing::DeepXPostOnlyParam::MustPostOnly => "MustPostOnly",
            crate::signing::DeepXPostOnlyParam::Adaptive => "Adaptive",
        };
        let order = ScaleValue::named_composite([
            ("id", ScaleValue::u128(u128::from(order_id))),
            ("maker", ScaleValue::from_bytes(subaccount)),
            ("pair", ScaleValue::from_bytes(pair)),
            (
                "price",
                ScaleValue::unnamed_variant("Some", [u256_value([1; 32])]),
            ),
            ("quote_amount", u256_value(*quote_amount)),
            ("base_amount", u256_value(*base_amount)),
            ("create_time", ScaleValue::u128(42)),
            ("status", ScaleValue::unnamed_variant("Open", [])),
            ("order_type", order_type),
            ("post_only", ScaleValue::unnamed_variant(post_only, [])),
            ("reduce_only", ScaleValue::bool(*reduce_only)),
            ("is_buy", ScaleValue::bool(*is_buy)),
        ]);
        let mut record = Phase::ApplyExtrinsic(extrinsic_index).encode();
        record.extend([pallet.index(), event.index]);
        subxt_core::ext::scale_value::scale::encode_as_type(
            &order,
            event.fields[0].ty.id,
            snapshot.metadata().types(),
            &mut record,
        )
        .unwrap();
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        record
    }

    fn cancel_event_record(
        snapshot: &RuntimeSnapshot,
        extrinsic_index: u32,
        subaccount: [u8; 20],
        order_id: u64,
        reason_index: u8,
    ) -> Vec<u8> {
        cancel_event_record_with_phase(
            snapshot,
            Phase::ApplyExtrinsic(extrinsic_index),
            subaccount,
            order_id,
            reason_index,
        )
    }

    fn cancel_event_record_with_phase(
        snapshot: &RuntimeSnapshot,
        phase: Phase,
        subaccount: [u8; 20],
        order_id: u64,
        reason_index: u8,
    ) -> Vec<u8> {
        let pallet = snapshot.interfaces().pallet("PerpMarket").unwrap();
        let event = snapshot
            .interfaces()
            .event("PerpMarket", "OrderCancelled")
            .unwrap();
        let mut bytes = phase.encode();
        bytes.push(pallet.index());
        bytes.push(event.index());
        bytes.extend_from_slice(&subaccount);
        order_id.encode_to(&mut bytes);
        bytes.push(reason_index);
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    fn dispatch_event_record(
        snapshot: &RuntimeSnapshot,
        extrinsic_index: u32,
        success: bool,
    ) -> Vec<u8> {
        let pallet = snapshot.interfaces().pallet("System").unwrap();
        let event = snapshot
            .interfaces()
            .event(
                "System",
                if success {
                    "ExtrinsicSuccess"
                } else {
                    "ExtrinsicFailed"
                },
            )
            .unwrap();
        let mut bytes = Phase::ApplyExtrinsic(extrinsic_index).encode();
        bytes.push(pallet.index());
        bytes.push(event.index());
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
        let metadata_event = snapshot
            .metadata()
            .pallet_by_name("System")
            .unwrap()
            .event_variant_by_index(event.index())
            .unwrap();
        if success {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &dispatch_info,
                metadata_event.fields[0].ty.id,
                snapshot.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        } else {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &ScaleValue::unnamed_variant("Other", []),
                metadata_event.fields[0].ty.id,
                snapshot.metadata().types(),
                &mut bytes,
            )
            .unwrap();
            subxt_core::ext::scale_value::scale::encode_as_type(
                &dispatch_info,
                metadata_event.fields[1].ty.id,
                snapshot.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    fn system_events(records: &[Vec<u8>]) -> Vec<u8> {
        let mut bytes = Compact(u32::try_from(records.len()).unwrap()).encode();
        for record in records {
            bytes.extend_from_slice(record);
        }
        bytes
    }

    #[rstest::rstest]
    fn spot_cancel_approved_metadata_regression() {
        let snapshot = runtime_snapshot();
        let mut wire = serde_json::to_value(cancel_identity([0x11; 20], 7, false)).unwrap();
        wire["operation"] = json!({"type": "spot-cancel", "subaccount": vec![0x11; 20],
            "pair": vec![0x22; 32], "order_id": 7, "is_buy": true, "fast_cancel": false});
        let identity: DeepXTransactionIdentity = serde_json::from_value(wire.clone()).unwrap();
        let pallet = snapshot.metadata().pallet_by_name("SpotMarket").unwrap();
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
            ScaleValue::from_bytes([0x22; 32]),
            ScaleValue::u128(7),
            ScaleValue::from_bytes([0x11; 20]),
            ScaleValue::bool(true),
            ScaleValue::unnamed_variant("UserCanceled", []),
        ];
        let mut record = Phase::ApplyExtrinsic(3).encode();
        record.extend([pallet.index(), event.index]);
        for (field, value) in event.fields.iter().zip(values) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        let events = system_events(&[dispatch_event_record(&snapshot, 3, true), record.clone()]);
        assert_eq!(
            verify_spot_cancel_inclusion_events(&snapshot, &identity, [1; 32], 42, 3, &events)
                .unwrap()
                .outcome(),
            DeepXInclusionOutcome::Success
        );
        for name in ["subaccount", "pair", "order_id", "is_buy", "fast_cancel"] {
            let mut changed = wire.clone();
            match name {
                "subaccount" | "pair" => changed["operation"][name][0] = json!(0),
                "order_id" => changed["operation"][name] = json!(8),
                "is_buy" => changed["operation"][name] = json!(false),
                "fast_cancel" => changed["operation"][name] = json!(true),
                _ => unreachable!(),
            }
            let changed = serde_json::from_value(changed).unwrap();
            assert!(
                verify_spot_cancel_inclusion_events(&snapshot, &changed, [1; 32], 42, 3, &events)
                    .is_err(),
                "{name}"
            );
        }
        let failed = system_events(&[dispatch_event_record(&snapshot, 3, false)]);
        assert_eq!(
            verify_spot_cancel_inclusion_events(&snapshot, &identity, [1; 32], 42, 3, &failed)
                .unwrap()
                .outcome(),
            DeepXInclusionOutcome::Failed
        );
        for invalid in [
            system_events(&[dispatch_event_record(&snapshot, 3, true)]),
            system_events(&[dispatch_event_record(&snapshot, 4, true), record.clone()]),
            system_events(&[
                dispatch_event_record(&snapshot, 3, true),
                dispatch_event_record(&snapshot, 3, false),
                record.clone(),
            ]),
            system_events(&[
                dispatch_event_record(&snapshot, 3, true),
                record.clone(),
                record.clone(),
            ]),
            system_events(&[dispatch_event_record(&snapshot, 4, false)]),
            Vec::new(),
            events[..events.len() - 1].to_vec(),
            [events.as_slice(), &[0]].concat(),
        ] {
            assert!(
                verify_spot_cancel_inclusion_events(&snapshot, &identity, [1; 32], 42, 3, &invalid)
                    .is_err()
            );
        }
        let mut wrong_index = record.clone();
        wrong_index[1..5].copy_from_slice(&4_u32.to_le_bytes());
        assert!(
            verify_spot_cancel_inclusion_events(
                &snapshot,
                &identity,
                [1; 32],
                42,
                3,
                &system_events(&[dispatch_event_record(&snapshot, 3, true), wrong_index])
            )
            .is_err()
        );
        let mut wrong_reason = record;
        let reason_offset = wrong_reason.len() - 2;
        wrong_reason[reason_offset] = 255;
        assert!(
            verify_spot_cancel_inclusion_events(
                &snapshot,
                &identity,
                [1; 32],
                42,
                3,
                &system_events(&[dispatch_event_record(&snapshot, 3, true), wrong_reason])
            )
            .is_err()
        );
    }

    #[rstest::rstest]
    #[case(true)]
    #[case(false)]
    fn spot_place_approved_metadata_regression(#[case] is_buy: bool) {
        let snapshot = runtime_snapshot();
        let identity = spot_place_identity(is_buy);
        let record = spot_place_event_record(&snapshot, 3, &identity);
        let events = system_events(&[dispatch_event_record(&snapshot, 3, true), record.clone()]);
        assert_eq!(
            verify_spot_place_inclusion_events(&snapshot, &identity, [1; 32], 42, 3, &events)
                .unwrap()
                .outcome(),
            DeepXInclusionOutcome::Success
        );

        let mut changed = serde_json::to_value(&identity).unwrap();
        changed["operation"]["quote_amount"][0] = json!(255);
        let changed = serde_json::from_value(changed).unwrap();
        assert!(
            verify_spot_place_inclusion_events(&snapshot, &changed, [1; 32], 42, 3, &events)
                .is_err()
        );

        for name in ["runtime", "nonce", "side"] {
            let mut changed = serde_json::to_value(&identity).unwrap();
            match name {
                "runtime" => changed["runtime"]["spec_version"] = json!(367),
                "nonce" => changed["nonce"]["value"] = json!(1_725_000_000_126_u64),
                "side" => {
                    changed["order_side"] = serde_json::to_value(if is_buy {
                        OrderSide::Sell
                    } else {
                        OrderSide::Buy
                    })
                    .unwrap();
                }
                _ => unreachable!(),
            }
            let changed = serde_json::from_value(changed).unwrap();
            assert!(
                verify_spot_place_inclusion_events(&snapshot, &changed, [1; 32], 42, 3, &events)
                    .is_err(),
                "{name}"
            );
        }

        let failed = system_events(&[dispatch_event_record(&snapshot, 3, false)]);
        assert_eq!(
            verify_spot_place_inclusion_events(&snapshot, &identity, [1; 32], 42, 3, &failed)
                .unwrap()
                .outcome(),
            DeepXInclusionOutcome::Failed
        );
        for invalid in [
            system_events(&[dispatch_event_record(&snapshot, 3, true)]),
            system_events(&[
                dispatch_event_record(&snapshot, 3, true),
                dispatch_event_record(&snapshot, 3, false),
                record.clone(),
            ]),
            system_events(&[
                dispatch_event_record(&snapshot, 3, true),
                spot_place_event_record(&snapshot, 4, &identity),
            ]),
            system_events(&[dispatch_event_record(&snapshot, 4, false)]),
            Vec::new(),
            system_events(&[
                dispatch_event_record(&snapshot, 3, true),
                record.clone(),
                record.clone(),
            ]),
            system_events(&[dispatch_event_record(&snapshot, 4, true), record.clone()]),
            events[..events.len() - 1].to_vec(),
            [events.as_slice(), &[0]].concat(),
        ] {
            assert!(
                verify_spot_place_inclusion_events(&snapshot, &identity, [1; 32], 42, 3, &invalid)
                    .is_err()
            );
        }
    }

    #[test]
    fn spot_place_policy_metadata_matrix() {
        use crate::signing::{DeepXPostOnlyParam, DeepXSpotOrderType, DeepXTimeInForce};

        let snapshot = runtime_snapshot();
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
                    let mut wire = serde_json::to_value(spot_place_identity(is_buy)).unwrap();
                    wire["operation"]["order_type"] = serde_json::to_value(order_type).unwrap();
                    wire["operation"]["post_only"] = serde_json::to_value(post_only).unwrap();
                    wire["operation"]["reduce_only"] = json!(true);
                    let identity = serde_json::from_value(wire.clone()).unwrap();
                    let events = system_events(&[
                        dispatch_event_record(&snapshot, 3, true),
                        spot_place_event_record(&snapshot, 3, &identity),
                    ]);
                    assert_eq!(
                        verify_spot_place_inclusion_events(
                            &snapshot, &identity, [1; 32], 42, 3, &events
                        )
                        .unwrap()
                        .outcome(),
                        DeepXInclusionOutcome::Success
                    );
                    for name in [
                        "subaccount",
                        "pair",
                        "quote_amount",
                        "base_amount",
                        "order_type",
                        "post_only",
                        "reduce_only",
                        "is_buy",
                    ] {
                        let mut changed = wire.clone();
                        match name {
                            "subaccount" | "pair" | "quote_amount" | "base_amount" => {
                                changed["operation"][name][0] = json!(255);
                            }
                            "order_type" => {
                                let other = if order_type == DeepXSpotOrderType::Stop {
                                    DeepXSpotOrderType::Market(None)
                                } else {
                                    DeepXSpotOrderType::Stop
                                };
                                changed["operation"][name] = serde_json::to_value(other).unwrap();
                            }
                            "post_only" => {
                                let other = if post_only == DeepXPostOnlyParam::None {
                                    DeepXPostOnlyParam::Adaptive
                                } else {
                                    DeepXPostOnlyParam::None
                                };
                                changed["operation"][name] = serde_json::to_value(other).unwrap();
                            }
                            "reduce_only" => changed["operation"][name] = json!(false),
                            "is_buy" => changed["operation"][name] = json!(!is_buy),
                            _ => unreachable!(),
                        }
                        let changed = serde_json::from_value(changed).unwrap();
                        assert!(
                            verify_spot_place_inclusion_events(
                                &snapshot, &changed, [1; 32], 42, 3, &events
                            )
                            .is_err(),
                            "{order_type:?} {post_only:?} {name}"
                        );
                    }
                }
            }
        }
    }

    #[rstest::rstest]
    fn perpetual_cancel_event_requires_exact_index_and_fields() {
        let snapshot = runtime_snapshot();
        let subaccount = [42; 20];
        let identity = cancel_identity(subaccount, 9001, false);
        let events = system_events(&[cancel_event_record(&snapshot, 3, subaccount, 9001, 0)]);

        assert_eq!(
            verify_perp_cancel_business_event(&snapshot, &identity, 3, &events).unwrap(),
            DeepXIndexedOutcome {
                extrinsic_index: 3,
                outcome: DeepXBusinessEventOutcome::Success,
            },
        );
    }

    #[rstest::rstest]
    fn perpetual_cancel_inclusion_requires_same_index_dispatch_and_business_event() {
        let snapshot = runtime_snapshot();
        let subaccount = [42; 20];
        let identity = cancel_identity(subaccount, 9001, false);
        let events = system_events(&[
            cancel_event_record(&snapshot, 3, subaccount, 9001, 0),
            dispatch_event_record(&snapshot, 3, true),
        ]);

        let inclusion =
            verify_perp_cancel_inclusion_events(&snapshot, &identity, [41; 32], 41, 3, &events)
                .unwrap();

        assert_eq!(inclusion.block_hash(), [41; 32]);
        assert_eq!(inclusion.block_number(), 41);
        assert_eq!(inclusion.extrinsic_index(), 3);
        assert_eq!(
            inclusion.outcome(),
            super::super::DeepXInclusionOutcome::Success
        );
    }

    #[rstest::rstest]
    fn perpetual_cancel_inclusion_rejects_missing_business_event() {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, false);
        let events = system_events(&[dispatch_event_record(&snapshot, 3, true)]);

        assert!(matches!(
            verify_perp_cancel_inclusion_events(&snapshot, &identity, [41; 32], 41, 3, &events,),
            Err(DeepXPerpCancelEventVerificationError::InclusionEvidence(_)),
        ));
    }

    #[rstest::rstest]
    #[case::success(true, DeepXInclusionOutcome::Success)]
    #[case::failed(false, DeepXInclusionOutcome::Failed)]
    fn fast_perpetual_cancel_inclusion_uses_same_index_dispatch(
        #[case] success: bool,
        #[case] expected: DeepXInclusionOutcome,
    ) {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, true);
        let events = system_events(&[
            cancel_event_record(&snapshot, 4, [42; 20], 9001, 0),
            dispatch_event_record(&snapshot, 3, success),
        ]);

        let inclusion =
            verify_perp_cancel_inclusion_events(&snapshot, &identity, [41; 32], 41, 3, &events)
                .unwrap();

        assert_eq!(inclusion.extrinsic_index(), 3);
        assert_eq!(inclusion.outcome(), expected);
    }

    #[rstest::rstest]
    fn fast_perpetual_cancel_inclusion_rejects_duplicate_dispatch() {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, true);
        let events = system_events(&[
            dispatch_event_record(&snapshot, 3, true),
            dispatch_event_record(&snapshot, 3, true),
        ]);

        assert!(matches!(
            verify_perp_cancel_inclusion_events(&snapshot, &identity, [41; 32], 41, 3, &events),
            Err(DeepXPerpCancelEventVerificationError::DuplicateEvent),
        ));
    }

    #[rstest::rstest]
    fn fast_perpetual_cancel_inclusion_rejects_wrong_dispatch_index() {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, true);
        let events = system_events(&[dispatch_event_record(&snapshot, 4, true)]);

        assert!(matches!(
            verify_perp_cancel_inclusion_events(&snapshot, &identity, [41; 32], 41, 3, &events),
            Err(DeepXPerpCancelEventVerificationError::MalformedEventBytes),
        ));
    }

    #[rstest::rstest]
    fn fast_perpetual_cancel_inclusion_rejects_trailing_event_bytes() {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, true);
        let mut events = system_events(&[dispatch_event_record(&snapshot, 3, true)]);
        events.push(0);

        assert!(matches!(
            verify_perp_cancel_inclusion_events(&snapshot, &identity, [41; 32], 41, 3, &events),
            Err(DeepXPerpCancelEventVerificationError::MalformedEventBytes),
        ));
    }

    #[rstest::rstest]
    fn absent_or_other_extrinsic_cancel_event_is_not_observed() {
        let snapshot = runtime_snapshot();
        let subaccount = [42; 20];
        let identity = cancel_identity(subaccount, 9001, false);

        for events in [
            system_events(&[]),
            system_events(&[cancel_event_record(&snapshot, 4, subaccount, 9001, 0)]),
            system_events(&[cancel_event_record_with_phase(
                &snapshot,
                Phase::Finalization,
                subaccount,
                9001,
                0,
            )]),
        ] {
            assert_eq!(
                verify_perp_cancel_business_event(&snapshot, &identity, 3, &events).unwrap(),
                DeepXIndexedOutcome {
                    extrinsic_index: 3,
                    outcome: DeepXBusinessEventOutcome::NotObserved,
                },
            );
        }
    }

    #[rstest::rstest]
    #[case::wrong_user([41; 20], 9001, 0)]
    #[case::wrong_order([42; 20], 9002, 0)]
    #[case::wrong_reason([42; 20], 9001, 1)]
    fn conflicting_perpetual_cancel_event_is_rejected(
        #[case] subaccount: [u8; 20],
        #[case] order_id: u64,
        #[case] reason_index: u8,
    ) {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, false);
        let events = system_events(&[cancel_event_record(
            &snapshot,
            3,
            subaccount,
            order_id,
            reason_index,
        )]);

        assert!(matches!(
            verify_perp_cancel_business_event(&snapshot, &identity, 3, &events),
            Err(DeepXPerpCancelEventVerificationError::ConflictingEvent),
        ));
    }

    #[rstest::rstest]
    fn duplicate_perpetual_cancel_event_is_rejected() {
        let snapshot = runtime_snapshot();
        let subaccount = [42; 20];
        let identity = cancel_identity(subaccount, 9001, false);
        let record = cancel_event_record(&snapshot, 3, subaccount, 9001, 0);
        let events = system_events(&[record.clone(), record]);

        assert!(matches!(
            verify_perp_cancel_business_event(&snapshot, &identity, 3, &events),
            Err(DeepXPerpCancelEventVerificationError::DuplicateEvent),
        ));
    }

    #[rstest::rstest]
    fn malformed_perpetual_cancel_event_bytes_are_rejected() {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, false);
        let mut trailing = system_events(&[]);
        trailing.push(0);

        for events in [vec![4], trailing] {
            assert!(matches!(
                verify_perp_cancel_business_event(&snapshot, &identity, 3, &events),
                Err(DeepXPerpCancelEventVerificationError::MalformedEventBytes),
            ));
        }
    }

    #[rstest::rstest]
    fn unsupported_and_fast_cancel_operations_are_rejected() {
        let snapshot = runtime_snapshot();
        let fast_cancel = cancel_identity([42; 20], 9001, true);
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let legacy = DeepXTransactionIdentity::new(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            derive_signer_account_id(&key).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId {
                value: 1_725_000_000_125,
            },
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
        );

        assert!(matches!(
            verify_perp_cancel_business_event(&snapshot, &fast_cancel, 3, &[0]),
            Err(DeepXPerpCancelEventVerificationError::FastCancelUnsupported),
        ));
        assert!(matches!(
            verify_perp_cancel_business_event(&snapshot, &legacy, 3, &[0]),
            Err(DeepXPerpCancelEventVerificationError::UnsupportedOperation),
        ));
    }

    #[rstest::rstest]
    #[case::canonical([42; 32], Some(3), DeepXReorganizationDecision::Canonical)]
    #[case::reorganized(
        [43; 32],
        None,
        DeepXReorganizationDecision::Reorganized(inclusion([42; 32], 42, 3)),
    )]
    #[case::missing([42; 32], None, DeepXReorganizationDecision::ActionRequired)]
    #[case::displaced([42; 32], Some(4), DeepXReorganizationDecision::ActionRequired)]
    fn canonical_observation_classifies_reorganization_fail_closed(
        #[case] block_hash: [u8; 32],
        #[case] extrinsic_index: Option<u32>,
        #[case] expected: DeepXReorganizationDecision,
    ) {
        let recorded = inclusion([42; 32], 42, 3);
        let observation = DeepXCanonicalBlockObservation {
            block_number: 42,
            block_hash,
            extrinsic_index,
        };

        assert_eq!(
            classify_canonical_block_observation(recorded, observation),
            expected,
        );
    }

    #[derive(Clone)]
    struct RecoveryRpcState {
        finalized_block: u64,
        finalized_hash: Option<String>,
        block_extrinsics: Arc<BTreeMap<u64, Vec<String>>>,
        pool_extrinsics: Arc<[String]>,
        event_storage: Arc<BTreeMap<u64, String>>,
        post_checkpoint_requests: Arc<AtomicUsize>,
    }

    async fn recovery_rpc(
        State(state): State<RecoveryRpcState>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
        let method = request["method"].as_str().unwrap();
        if matches!(
            method,
            "chain_getFinalizedHead"
                | "chain_getHeader"
                | "chain_getBlock"
                | "state_getStorage"
                | "author_pendingExtrinsics"
        ) {
            state
                .post_checkpoint_requests
                .fetch_add(1, Ordering::Relaxed);
        }
        let result = match method {
            "rpc_methods" => json!({
                "methods": [
                    "author_pendingExtrinsics",
                    "author_submitExtrinsic",
                    "chain_getBlock",
                    "chain_getBlockHash",
                    "chain_getFinalizedHead",
                    "chain_getHeader",
                    "state_getMetadata",
                    "state_getRuntimeVersion",
                    "state_getStorage",
                ],
            }),
            "chain_getFinalizedHead" => json!(
                state
                    .finalized_hash
                    .clone()
                    .unwrap_or_else(|| block_hash(state.finalized_block))
            ),
            "chain_getHeader" => json!({ "number": format!("0x{:x}", state.finalized_block) }),
            "chain_getBlockHash" => {
                let number = request["params"][0].as_u64().unwrap();
                json!(block_hash(number))
            }
            "chain_getBlock" => {
                let number = decode_mock_block_hash(request["params"][0].as_str().unwrap());
                json!({
                    "block": {
                        "header": { "number": format!("0x{number:x}") },
                        "extrinsics": state
                            .block_extrinsics
                            .get(&number)
                            .map(Vec::as_slice)
                            .unwrap_or_default(),
                    },
                })
            }
            "state_getStorage" => {
                assert_eq!(request["params"][0], SYSTEM_EVENTS_STORAGE_KEY);
                let number = decode_mock_block_hash(request["params"][1].as_str().unwrap());
                json!(state.event_storage.get(&number))
            }
            "author_pendingExtrinsics" => json!(state.pool_extrinsics.as_ref()),
            method => panic!("unexpected method {method}"),
        };
        Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
    }

    fn block_hash(number: u64) -> String {
        format!("0x{number:064x}")
    }

    fn decode_mock_block_hash(encoded: &str) -> u64 {
        u64::from_str_radix(encoded.trim_start_matches("0x"), 16).unwrap()
    }

    async fn recovery_endpoints(
        finalized_block: u64,
        finalized_hash: Option<String>,
        block_extrinsics: BTreeMap<u64, Vec<String>>,
        pool_extrinsics: Vec<String>,
    ) -> (
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
        Arc<AtomicUsize>,
    ) {
        recovery_endpoints_with_events(
            finalized_block,
            finalized_hash,
            block_extrinsics,
            pool_extrinsics,
            BTreeMap::new(),
        )
        .await
    }

    async fn recovery_endpoints_with_events(
        finalized_block: u64,
        finalized_hash: Option<String>,
        block_extrinsics: BTreeMap<u64, Vec<String>>,
        pool_extrinsics: Vec<String>,
        event_storage: BTreeMap<u64, String>,
    ) -> (
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let post_checkpoint_requests = Arc::new(AtomicUsize::new(0));
        let state = RecoveryRpcState {
            finalized_block,
            finalized_hash,
            block_extrinsics: Arc::new(block_extrinsics),
            pool_extrinsics: Arc::from(pool_extrinsics),
            event_storage: Arc::new(event_storage),
            post_checkpoint_requests: Arc::clone(&post_checkpoint_requests),
        };
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(recovery_rpc))
                    .with_state(state),
            )
            .await
            .unwrap();
        });
        let url = format!("http://{address}");
        let config = DeepXNetworkConfig {
            base_url_rpc: Some(url.clone()),
            ..Default::default()
        };
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let endpoints = validate_rpc_endpoint_identities(
            &config,
            [
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Submission, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Recovery, url, genesis_hash),
            ],
        )
        .unwrap();
        let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap();
        (endpoints, capabilities, post_checkpoint_requests)
    }

    #[test]
    fn finalized_event_storage_metadata_layout() {
        let snapshot = runtime_snapshot();
        let system = snapshot.metadata().pallet_by_name("System").unwrap();
        let storage = system.storage().unwrap();
        assert_eq!(storage.prefix(), "System");
        let events = storage.entry_by_name("Events").unwrap();
        let threads = storage.entry_by_name("Threads").unwrap();
        let batches = storage.entry_by_name("EventsMap").unwrap();
        assert_eq!(events.entry_type().key_ty(), None);
        assert_eq!(threads.entry_type().key_ty(), Some(4));
        assert_eq!(threads.entry_type().value_ty(), 2);
        assert_eq!(batches.entry_type().key_ty(), Some(135));
        assert_eq!(
            batches.entry_type().value_ty(),
            events.entry_type().value_ty()
        );
        assert_eq!(threads.default_bytes(), &[0]);
        assert_eq!(batches.default_bytes(), &[0]);
    }

    #[tokio::test]
    async fn finalized_event_storage_rejects_missing_and_malformed_values() {
        for encoded in [None, Some("00"), Some("0x"), Some("0xgg"), Some("0x0")] {
            let storage = encoded
                .map(|value| BTreeMap::from([(41, value.to_string())]))
                .unwrap_or_default();
            let (endpoints, _, _) =
                recovery_endpoints_with_events(42, None, BTreeMap::new(), vec![], storage).await;
            let hash = hex::decode_array(block_hash(41).trim_start_matches("0x")).unwrap();
            let result =
                fetch_system_events(endpoints.url_for(DeepXRpcRole::Recovery), hash, 41).await;
            match encoded {
                None => assert!(matches!(
                    result,
                    Err(DeepXTransactionWatchError::Rpc {
                        method: "state_getStorage",
                        ..
                    })
                )),
                Some(_) => assert!(matches!(
                    result,
                    Err(DeepXTransactionWatchError::InvalidEventStorage(41))
                )),
            }
        }
    }

    #[tokio::test]
    async fn finalized_event_storage_preserves_exact_bytes_at_requested_block() {
        let storage = BTreeMap::from([(41, "0x00aBff".to_string()), (42, "0x04".to_string())]);
        let (endpoints, _, _) =
            recovery_endpoints_with_events(42, None, BTreeMap::new(), vec![], storage).await;
        let hash = hex::decode_array(block_hash(41).trim_start_matches("0x")).unwrap();
        let bytes = fetch_system_events(endpoints.url_for(DeepXRpcRole::Recovery), hash, 41)
            .await
            .unwrap();
        assert_eq!(bytes, vec![0, 171, 255]);
    }

    async fn spawn_recovery_rpc(
        finalized_block: u64,
        block_extrinsics: BTreeMap<u64, Vec<String>>,
        pool_extrinsics: Vec<String>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = RecoveryRpcState {
            finalized_block,
            finalized_hash: None,
            block_extrinsics: Arc::new(block_extrinsics),
            pool_extrinsics: Arc::from(pool_extrinsics),
            event_storage: Arc::new(BTreeMap::new()),
            post_checkpoint_requests: Arc::new(AtomicUsize::new(0)),
        };
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(recovery_rpc))
                    .with_state(state),
            )
            .await
            .unwrap();
        });
        format!("http://{address}")
    }

    async fn split_recovery_endpoints(
        submission_pool: Vec<String>,
        recovery_pool: Vec<String>,
    ) -> (
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
    ) {
        let submission_url = spawn_recovery_rpc(42, BTreeMap::new(), submission_pool).await;
        let recovery_url = spawn_recovery_rpc(42, BTreeMap::new(), recovery_pool).await;
        let config = DeepXNetworkConfig {
            base_url_rpc_submission: Some(submission_url.clone()),
            base_url_rpc_watch: Some(recovery_url.clone()),
            base_url_rpc_recovery: Some(recovery_url.clone()),
            ..Default::default()
        };
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let endpoints = validate_rpc_endpoint_identities(
            &config,
            [
                DeepXObservedRpcEndpoint::new(
                    DeepXRpcRole::Submission,
                    submission_url,
                    genesis_hash,
                ),
                DeepXObservedRpcEndpoint::new(
                    DeepXRpcRole::Watch,
                    recovery_url.clone(),
                    genesis_hash,
                ),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Recovery, recovery_url, genesis_hash),
            ],
        )
        .unwrap();
        let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap();
        (endpoints, capabilities)
    }

    #[tokio::test]
    async fn canonical_block_locates_exact_extrinsic_hash() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let endpoints = endpoints(
            vec![
                format!("0x{}", hex::encode(extrinsic(&[5, 6]))),
                format!("0x{}", hex::encode(target)),
            ],
            "0x2a",
        )
        .await;

        let observation = observe_canonical_block(&endpoints, 42, target_hash)
            .await
            .unwrap();

        assert_eq!(observation.block_number(), 42);
        assert_eq!(observation.block_hash(), [42; 32]);
        assert_eq!(observation.extrinsic_index(), Some(1));
    }

    #[tokio::test]
    async fn canonical_block_does_not_infer_inclusion_for_absent_hash() {
        let endpoints = endpoints(
            vec![format!("0x{}", hex::encode(extrinsic(&[1, 2, 3])))],
            "0x2a",
        )
        .await;

        let observation = observe_canonical_block(&endpoints, 42, [9; 32])
            .await
            .unwrap();

        assert_eq!(observation.extrinsic_index(), None);
    }

    #[tokio::test]
    async fn malformed_pool_entry_prevents_absence_evidence() {
        let endpoints = endpoints(vec!["not-hex".to_string()], "0x2a").await;

        let error = observe_submission_pool(&endpoints, [9; 32])
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::InvalidExtrinsic {
                evidence_source: "submission pool",
                index: 0,
            },
        ));
    }

    #[tokio::test]
    async fn malformed_scale_length_prevents_absence_evidence() {
        let endpoints = endpoints(vec!["0x100102".to_string()], "0x2a").await;

        let error = observe_submission_pool(&endpoints, [9; 32])
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::InvalidExtrinsic {
                evidence_source: "submission pool",
                index: 0,
            },
        ));
    }

    #[tokio::test]
    async fn block_number_mismatch_is_rejected() {
        let endpoints = endpoints(vec![], "0x2b").await;

        let error = observe_canonical_block(&endpoints, 42, [9; 32])
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::BlockNumberMismatch {
                requested: 42,
                received: 43,
            },
        ));
    }

    #[tokio::test]
    async fn duplicate_target_extrinsic_is_rejected() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let encoded = format!("0x{}", hex::encode(target));
        let endpoints = endpoints(vec![encoded.clone(), encoded], "0x2a").await;

        let error = observe_canonical_block(&endpoints, 42, target_hash)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::DuplicateExtrinsic {
                evidence_source: "canonical block",
            },
        ));
    }

    #[tokio::test]
    async fn pool_membership_requires_exact_extrinsic_hash() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let endpoints = endpoints(
            vec![
                format!("0x{}", hex::encode(extrinsic(&[5, 6]))),
                format!("0x{}", hex::encode(target)),
            ],
            "0x2a",
        )
        .await;

        assert_eq!(
            observe_submission_pool(&endpoints, target_hash)
                .await
                .unwrap(),
            DeepXPoolObservation::Present,
        );
    }

    #[tokio::test]
    async fn finalized_recovery_scan_requires_action_after_non_atomic_pool_absence() {
        let (endpoints, capabilities, _) =
            recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;

        let collection = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            [9; 32],
        )
        .await
        .unwrap();

        let DeepXFinalizedRecoveryCollection::Scan(scan) = collection else {
            panic!("expected a completed recovery scan");
        };
        assert_eq!(scan.classify(), DeepXRecoveryDecision::ActionRequired);
    }

    #[tokio::test]
    async fn finalized_recovery_scan_stops_when_event_evidence_is_required() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = BTreeMap::from([(41, vec![format!("0x{}", hex::encode(target))])]);
        let (endpoints, capabilities, _) = recovery_endpoints(42, None, blocks, vec![]).await;

        let error = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            target_hash,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::EventEvidenceUnavailable {
                block_number: 41,
                extrinsic_index: 0,
            },
        ));
    }

    #[tokio::test]
    async fn finalized_recovery_scan_verifies_events_at_exact_canonical_block() {
        let snapshot = runtime_snapshot();
        let subaccount = [42; 20];
        let identity = cancel_identity(subaccount, 9001, false);
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = BTreeMap::from([(41, vec![format!("0x{}", hex::encode(target))])]);
        let events = system_events(&[
            cancel_event_record(&snapshot, 0, subaccount, 9001, 0),
            dispatch_event_record(&snapshot, 0, true),
        ]);
        let event_storage = BTreeMap::from([(41, format!("0x{}", hex::encode(events)))]);
        let (endpoints, capabilities, _) =
            recovery_endpoints_with_events(42, None, blocks, vec![], event_storage).await;

        let collection = collect_finalized_recovery_scan_with_event_evidence(
            &endpoints,
            &capabilities,
            &snapshot,
            &identity,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            target_hash,
        )
        .await
        .unwrap();

        let DeepXFinalizedRecoveryCollection::Scan(scan) = collection else {
            panic!("expected a completed recovery scan");
        };
        assert_eq!(
            scan.classify(),
            DeepXRecoveryDecision::FinalizedInclusion(inclusion(
                decode_hash(&block_hash(41)).unwrap(),
                41,
                0,
            )),
        );
    }

    #[tokio::test]
    async fn finalized_recovery_scan_reports_up_to_date_checkpoint() {
        let (endpoints, capabilities, _) =
            recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;

        let collection = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            42,
            decode_hash(&block_hash(42)).unwrap(),
            2,
            [9; 32],
        )
        .await
        .unwrap();

        assert_eq!(
            collection,
            DeepXFinalizedRecoveryCollection::UpToDate(DeepXFinalizedRecoveryCheckpoint {
                block_number: 42,
                block_hash: decode_hash(&block_hash(42)).unwrap(),
            }),
        );
    }

    #[rstest::rstest]
    #[case("success")]
    #[case("failed")]
    #[case("identity")]
    #[case("runtime")]
    #[case("fast")]
    #[case("checkpoint")]
    #[case("missing-business")]
    #[case("wrong-index")]
    #[case("absent")]
    #[case("unsupported")]
    #[tokio::test]
    async fn explicit_spot_cancel_recovery_scan(#[case] scenario: &str) {
        let snapshot = runtime_snapshot();
        let mut wire = serde_json::to_value(cancel_identity([0x11; 20], 7, false)).unwrap();
        wire["operation"] = json!({"type": "spot-cancel", "subaccount": vec![0x11; 20],
            "pair": vec![0x22; 32], "order_id": 7, "is_buy": true, "fast_cancel": false});
        if scenario == "identity" {
            wire["operation"]["order_id"] = json!(8);
        }
        if scenario == "fast" {
            wire["operation"]["fast_cancel"] = json!(true);
        }
        if scenario == "runtime" {
            wire["runtime"]["spec_version"] = json!(369);
        }
        let mut identity: DeepXTransactionIdentity = serde_json::from_value(wire).unwrap();
        if scenario == "unsupported" {
            identity = cancel_identity([0x11; 20], 7, false);
        }
        let pallet = snapshot.metadata().pallet_by_name("SpotMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == "OrderCancelled")
            .unwrap();
        let mut record =
            Phase::ApplyExtrinsic(if scenario == "wrong-index" { 0 } else { 1 }).encode();
        record.extend([pallet.index(), event.index]);
        let values = [
            ScaleValue::from_bytes([0x22; 32]),
            ScaleValue::u128(7),
            ScaleValue::from_bytes([0x11; 20]),
            ScaleValue::bool(true),
            ScaleValue::unnamed_variant("UserCanceled", []),
        ];
        for (field, value) in event.fields.iter().zip(values) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        let mut records = vec![dispatch_event_record(&snapshot, 1, scenario != "failed")];
        if scenario != "failed" && scenario != "missing-business" {
            records.push(record);
        }
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = if scenario == "absent" {
            BTreeMap::new()
        } else {
            BTreeMap::from([(
                41,
                vec!["0x0400".to_string(), format!("0x{}", hex::encode(target))],
            )])
        };
        let storage = BTreeMap::from([(41, format!("0x{}", hex::encode(system_events(&records))))]);
        let (endpoints, capabilities, _) =
            recovery_endpoints_with_events(42, None, blocks, vec![], storage).await;
        let result = collect_finalized_spot_cancel_recovery_scan(
            &endpoints,
            &capabilities,
            &snapshot,
            &identity,
            39,
            if scenario == "checkpoint" {
                [0; 32]
            } else {
                decode_hash(&block_hash(39)).unwrap()
            },
            2,
            target_hash,
        )
        .await;
        match scenario {
            "success" | "failed" => {
                let DeepXFinalizedRecoveryCollection::Scan(scan) = result.unwrap() else {
                    panic!("expected scan")
                };
                let DeepXRecoveryDecision::FinalizedInclusion(inclusion) = scan.classify() else {
                    panic!("expected inclusion")
                };
                assert_eq!(
                    inclusion.block_hash(),
                    decode_hash(&block_hash(41)).unwrap()
                );
                assert_eq!(inclusion.block_number(), 41);
                assert_eq!(inclusion.extrinsic_index(), 1);
                assert_eq!(
                    inclusion.outcome(),
                    if scenario == "success" {
                        DeepXInclusionOutcome::Success
                    } else {
                        DeepXInclusionOutcome::Failed
                    }
                );
            }
            "absent" => {
                let DeepXFinalizedRecoveryCollection::Scan(scan) = result.unwrap() else {
                    panic!("expected scan")
                };
                assert_eq!(scan.classify(), DeepXRecoveryDecision::ActionRequired);
            }
            "checkpoint" => assert!(matches!(
                result,
                Err(DeepXTransactionWatchError::RecoveryCheckpointMismatch { .. })
            )),
            _ => assert!(matches!(
                result,
                Err(DeepXTransactionWatchError::SpotEventVerification(_))
            )),
        }
    }

    #[tokio::test]
    async fn explicit_spot_place_recovery_scan() {
        let snapshot = runtime_snapshot();
        let identity = spot_place_identity(true);
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = BTreeMap::from([(
            41,
            vec!["0x0400".to_string(), format!("0x{}", hex::encode(&target))],
        )]);
        let events = system_events(&[
            dispatch_event_record(&snapshot, 1, true),
            spot_place_event_record(&snapshot, 1, &identity),
        ]);
        let storage = BTreeMap::from([(41, format!("0x{}", hex::encode(events)))]);
        let (endpoints, capabilities, _) =
            recovery_endpoints_with_events(42, None, blocks, vec![], storage).await;

        let collection = collect_finalized_spot_place_recovery_scan(
            &endpoints,
            &capabilities,
            &snapshot,
            &identity,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            target_hash,
        )
        .await
        .unwrap();
        let DeepXFinalizedRecoveryCollection::Scan(scan) = collection else {
            panic!("expected scan")
        };
        assert_eq!(
            scan.classify(),
            DeepXRecoveryDecision::FinalizedInclusion(inclusion(
                decode_hash(&block_hash(41)).unwrap(),
                41,
                1,
            ))
        );

        assert!(matches!(
            collect_finalized_spot_place_recovery_scan(
                &endpoints,
                &capabilities,
                &snapshot,
                &cancel_identity([0x11; 20], 7, false),
                39,
                decode_hash(&block_hash(39)).unwrap(),
                2,
                target_hash,
            )
            .await,
            Err(DeepXTransactionWatchError::SpotPlaceEventVerification(
                DeepXSpotPlaceEventVerificationError::UnsupportedOperation
            ))
        ));
    }

    #[tokio::test]
    async fn finalized_recovery_scan_rejects_conflicting_up_to_date_hash() {
        let conflicting_hash = block_hash(43);
        let (endpoints, capabilities, post_checkpoint_requests) =
            recovery_endpoints(42, Some(conflicting_hash), BTreeMap::new(), vec![]).await;

        let error = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            42,
            decode_hash(&block_hash(42)).unwrap(),
            2,
            [9; 32],
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::RecoveryCheckpointMismatch {
                block_number: 42,
                expected,
                received,
            } if expected == decode_hash(&block_hash(42)).unwrap()
                && received == decode_hash(&block_hash(43)).unwrap()
        ));
        assert_eq!(post_checkpoint_requests.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn finalized_recovery_scan_preserves_pool_acceptance() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let pool = vec![format!("0x{}", hex::encode(target))];
        let (endpoints, capabilities, _) =
            recovery_endpoints(42, None, BTreeMap::new(), pool).await;

        let collection = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            target_hash,
        )
        .await
        .unwrap();

        let DeepXFinalizedRecoveryCollection::Scan(scan) = collection else {
            panic!("expected a completed recovery scan");
        };
        assert_eq!(scan.classify(), DeepXRecoveryDecision::PoolAccepted);
    }

    #[tokio::test]
    async fn finalized_recovery_scan_queries_node_that_accepted_submission() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let submission_pool = vec![format!("0x{}", hex::encode(target))];
        let (endpoints, capabilities) = split_recovery_endpoints(submission_pool, vec![]).await;

        let collection = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            target_hash,
        )
        .await
        .unwrap();

        let DeepXFinalizedRecoveryCollection::Scan(scan) = collection else {
            panic!("expected a completed recovery scan");
        };
        assert_eq!(scan.classify(), DeepXRecoveryDecision::PoolAccepted);
    }

    #[tokio::test]
    async fn reorganization_observation_binds_recorded_height_and_extrinsic_hash() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = BTreeMap::from([(42, vec![format!("0x{}", hex::encode(target))])]);
        let (endpoints, capabilities, _) = recovery_endpoints(42, None, blocks, vec![]).await;
        let recorded = inclusion(decode_hash(&block_hash(42)).unwrap(), 42, 0);

        assert_eq!(
            observe_reorganization(&endpoints, &capabilities, target_hash, recorded)
                .await
                .unwrap(),
            DeepXReorganizationDecision::Canonical,
        );
        assert_eq!(
            observe_reorganization(&endpoints, &capabilities, [9; 32], recorded)
                .await
                .unwrap(),
            DeepXReorganizationDecision::ActionRequired,
        );
    }

    #[tokio::test]
    async fn reorganization_observation_rejects_capabilities_from_other_endpoints() {
        let (endpoints, _, _) = recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;
        let (_, other_capabilities, _) =
            recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;
        let recorded = inclusion(decode_hash(&block_hash(42)).unwrap(), 42, 0);

        let error = observe_reorganization(&endpoints, &other_capabilities, [9; 32], recorded)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::CapabilitiesMismatch(DeepXRpcRole::Watch),
        ));
    }

    #[tokio::test]
    async fn finalized_recovery_scan_rejects_capabilities_from_other_endpoints() {
        let (endpoints, _, _) = recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;
        let (_, other_capabilities, _) =
            recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;

        let error = collect_finalized_recovery_scan(
            &endpoints,
            &other_capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            [9; 32],
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::CapabilitiesMismatch(DeepXRpcRole::Recovery),
        ));
    }

    #[tokio::test]
    async fn finalized_recovery_scan_rejects_changed_checkpoint_before_scanning() {
        let (endpoints, capabilities, post_checkpoint_requests) =
            recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;

        let error =
            collect_finalized_recovery_scan(&endpoints, &capabilities, 39, [7; 32], 2, [9; 32])
                .await
                .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::RecoveryCheckpointMismatch {
                block_number: 39,
                expected,
                received,
            } if expected == [7; 32] && received == decode_hash(&block_hash(39)).unwrap(),
        ));
        assert_eq!(post_checkpoint_requests.load(Ordering::Relaxed), 0);
    }
}
