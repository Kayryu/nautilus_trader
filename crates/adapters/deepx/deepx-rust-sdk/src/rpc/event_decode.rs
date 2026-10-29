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

//! Decoding complete finalized DeepX runtime event storage.

use parity_scale_codec::{Compact, Decode};
use subxt_core::{
    events::{Events, Phase},
    ext::scale_value::{Composite, Value as ScaleValue, ValueDef},
    metadata::Metadata,
};
use thiserror::Error;

use super::events::{
    DeepXDelegateAccountChangeKind, DeepXFinalizedDelegateAccountChange,
    DeepXFinalizedDelegateModeChange,
};
use crate::{
    rest::models::DeepXDelegateMode,
    signing::{
        DeepXRuntimeConfig,
        runtime_interface::{DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError},
    },
};

/// The runtime dispatch outcome for one extrinsic in complete `System.Events` storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXSystemDispatchOutcome {
    /// The runtime reported successful dispatch.
    Success,
    /// The runtime reported failed dispatch.
    Failed,
}

/// Failure to establish a unique dispatch outcome from complete event storage.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeepXSystemDispatchError {
    /// The event count, SCALE data, or encoded length is invalid.
    #[error("invalid DeepX System.Events storage")]
    InvalidEventStorage,
    /// No unique System dispatch outcome exists at the requested extrinsic index.
    #[error("missing or duplicate DeepX System dispatch outcome")]
    MissingSystemDispatch,
}

/// Indexed System dispatch evidence from complete runtime event storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXIndexedSystemDispatch {
    /// Index of the extrinsic which emitted the dispatch event.
    pub extrinsic_index: u32,
    /// Runtime dispatch outcome at that index.
    pub outcome: DeepXSystemDispatchOutcome,
}

/// Detailed errors while reading an indexed System dispatch event.
#[derive(Debug, Error)]
pub enum DeepXIndexedSystemDispatchError {
    /// SCALE event decoding failed against the supplied metadata.
    #[error("unable to decode DeepX System dispatch events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The event count or encoded length is invalid.
    #[error("incomplete DeepX System dispatch event storage")]
    MalformedEventBytes,
    /// No dispatch event was emitted at the target index.
    #[error("missing DeepX System dispatch event")]
    MissingEvent,
    /// More than one dispatch event was emitted at the target index.
    #[error("duplicate DeepX System dispatch event")]
    DuplicateEvent,
}

/// Decodes the unique System dispatch outcome for an extrinsic in complete event storage.
///
/// # Errors
///
/// Returns an error for invalid storage or a missing or duplicate dispatch outcome.
pub fn decode_system_dispatch(
    metadata: &Metadata,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXSystemDispatchOutcome, DeepXSystemDispatchError> {
    decode_indexed_system_dispatch(metadata, extrinsic_index, event_bytes)
        .map(|evidence| evidence.outcome)
        .map_err(|e| match e {
            DeepXIndexedSystemDispatchError::Decode(_)
            | DeepXIndexedSystemDispatchError::MalformedEventBytes => {
                DeepXSystemDispatchError::InvalidEventStorage
            }
            DeepXIndexedSystemDispatchError::MissingEvent
            | DeepXIndexedSystemDispatchError::DuplicateEvent => {
                DeepXSystemDispatchError::MissingSystemDispatch
            }
        })
}

/// Decodes one unique indexed System dispatch from complete event storage.
///
/// # Errors
///
/// Returns a decoding, completeness, or duplicate-event error.
pub fn decode_indexed_system_dispatch(
    metadata: &Metadata,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXIndexedSystemDispatch, DeepXIndexedSystemDispatchError> {
    use DeepXIndexedSystemDispatchError as Error;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| Error::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut dispatch = None;
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    for event in events.iter() {
        let event = event.map_err(|e| Error::Decode(Box::new(e)))?;
        count = count.checked_add(1).ok_or(Error::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(Error::MalformedEventBytes)?;
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index)
            || event.pallet_name() != "System"
        {
            continue;
        }
        let outcome = match event.variant_name() {
            "ExtrinsicSuccess" => DeepXSystemDispatchOutcome::Success,
            "ExtrinsicFailed" => DeepXSystemDispatchOutcome::Failed,
            _ => continue,
        };
        if dispatch.replace(outcome).is_some() {
            return Err(Error::DuplicateEvent);
        }
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(Error::MalformedEventBytes);
    }
    Ok(DeepXIndexedSystemDispatch {
        extrinsic_index,
        outcome: dispatch.ok_or(Error::MissingEvent)?,
    })
}

/// Durable terms expected from a perpetual cancellation event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpCancelExpected {
    /// Account which owns the order.
    pub subaccount: [u8; 20],
    /// Order identifier in the cancellation request.
    pub order_id: u64,
    /// Index of the signed extrinsic in the block.
    pub extrinsic_index: u32,
}

/// Business result observed at the expected extrinsic index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXPerpCancelBusinessOutcome {
    /// One matching cancellation event was observed.
    Success,
    /// No cancellation event was observed at this index.
    NotObserved,
}

/// Indexed business evidence from complete runtime event storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpCancelBusinessEvent {
    /// Index of the signed extrinsic in the block.
    pub extrinsic_index: u32,
    /// Whether the expected cancellation event was observed.
    pub outcome: DeepXPerpCancelBusinessOutcome,
}

/// Errors while binding a perpetual cancellation event to durable terms.
#[derive(Debug, Error)]
pub enum DeepXPerpCancelEventError {
    /// The approved runtime does not expose the cancellation event.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE event decoding failed against the supplied metadata.
    #[error("unable to decode DeepX perpetual cancellation events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The count or encoded length does not describe complete `System.Events` storage.
    #[error("DeepX perpetual cancellation evidence is not complete System.Events")]
    MalformedEventBytes,
    /// The cancellation disagrees with the expected account, order, or reason.
    #[error("DeepX perpetual cancellation event conflicts with expected terms")]
    ConflictingEvent,
    /// The target extrinsic emitted more than one matching cancellation event.
    #[error("duplicate DeepX perpetual cancellation event")]
    DuplicateEvent,
}

/// Binds one perpetual cancellation to complete `System.Events` storage.
///
/// The caller establishes block canonicality and verifies the signed operation. Other extrinsic
/// phases and indices do not provide evidence for this cancellation.
///
/// # Errors
///
/// Returns an error for unsupported metadata, incomplete storage, or conflicting or duplicate
/// cancellation events.
pub fn decode_perp_cancel_business_event(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    expected: DeepXPerpCancelExpected,
    event_bytes: &[u8],
) -> Result<DeepXPerpCancelBusinessEvent, DeepXPerpCancelEventError> {
    use DeepXPerpCancelEventError as Error;

    interfaces.event("PerpMarket", "OrderCancelled")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| Error::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut matched = false;
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    for event in events.iter() {
        let event = event.map_err(|e| Error::Decode(Box::new(e)))?;
        count = count.checked_add(1).ok_or(Error::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(Error::MalformedEventBytes)?;
        if event.phase() != Phase::ApplyExtrinsic(expected.extrinsic_index)
            || event.pallet_name() != "PerpMarket"
            || event.variant_name() != "OrderCancelled"
        {
            continue;
        }
        let fields = event
            .field_values()
            .map_err(|e| Error::Decode(Box::new(e)))?;
        if !perp_cancel_fields_match(&fields, expected) {
            return Err(Error::ConflictingEvent);
        }
        if matched {
            return Err(Error::DuplicateEvent);
        }
        matched = true;
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(Error::MalformedEventBytes);
    }
    Ok(DeepXPerpCancelBusinessEvent {
        extrinsic_index: expected.extrinsic_index,
        outcome: if matched {
            DeepXPerpCancelBusinessOutcome::Success
        } else {
            DeepXPerpCancelBusinessOutcome::NotObserved
        },
    })
}

/// Inclusion outcome established by DeepX dispatch and cancellation events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXPerpCancelInclusionOutcome {
    /// The runtime dispatched and applied the cancellation.
    Success,
    /// The runtime rejected the extrinsic without applying the cancellation.
    Failed,
}

/// Indexed inclusion evidence for a perpetual cancellation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpCancelInclusionEvidence {
    /// Index of the signed extrinsic in the block.
    pub extrinsic_index: u32,
    /// Authoritative outcome at that index.
    pub outcome: DeepXPerpCancelInclusionOutcome,
}

/// Errors while deriving cancellation inclusion from complete runtime events.
#[derive(Debug, Error)]
pub enum DeepXPerpCancelInclusionError {
    /// The approved runtime lacks a required System dispatch event.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// System dispatch evidence could not be established.
    #[error(transparent)]
    Dispatch(#[from] DeepXIndexedSystemDispatchError),
    /// The expected cancellation event could not be established.
    #[error(transparent)]
    BusinessEvent(#[from] DeepXPerpCancelEventError),
    /// Successful dispatch did not emit the expected cancellation event.
    #[error("successful DeepX perpetual cancellation has no matching business event")]
    MissingBusinessEvent,
    /// A failed dispatch also emitted the expected cancellation event.
    #[error("failed DeepX perpetual cancellation conflicts with a business event")]
    ConflictingBusinessEvent,
}

/// Verifies one perpetual cancellation against complete indexed runtime events.
///
/// The caller verifies the signed operation and canonical block. Fast cancel has no authoritative
/// pallet event, so its outcome is determined by System dispatch alone.
///
/// # Errors
///
/// Returns an error for unsupported runtime events, incomplete or conflicting event storage, or
/// an impossible dispatch and business-event combination.
pub fn decode_perp_cancel_inclusion(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    expected: DeepXPerpCancelExpected,
    fast_cancel: bool,
    event_bytes: &[u8],
) -> Result<DeepXPerpCancelInclusionEvidence, DeepXPerpCancelInclusionError> {
    use DeepXPerpCancelInclusionError as Error;

    interfaces.event("System", "ExtrinsicSuccess")?;
    interfaces.event("System", "ExtrinsicFailed")?;
    let dispatch = decode_indexed_system_dispatch(metadata, expected.extrinsic_index, event_bytes)?;
    let outcome = if fast_cancel {
        match dispatch.outcome {
            DeepXSystemDispatchOutcome::Success => DeepXPerpCancelInclusionOutcome::Success,
            DeepXSystemDispatchOutcome::Failed => DeepXPerpCancelInclusionOutcome::Failed,
        }
    } else {
        let business =
            decode_perp_cancel_business_event(interfaces, metadata, expected, event_bytes)?;
        match (dispatch.outcome, business.outcome) {
            (DeepXSystemDispatchOutcome::Success, DeepXPerpCancelBusinessOutcome::Success) => {
                DeepXPerpCancelInclusionOutcome::Success
            }
            (DeepXSystemDispatchOutcome::Failed, DeepXPerpCancelBusinessOutcome::NotObserved) => {
                DeepXPerpCancelInclusionOutcome::Failed
            }
            (DeepXSystemDispatchOutcome::Success, DeepXPerpCancelBusinessOutcome::NotObserved) => {
                return Err(Error::MissingBusinessEvent);
            }
            (DeepXSystemDispatchOutcome::Failed, DeepXPerpCancelBusinessOutcome::Success) => {
                return Err(Error::ConflictingBusinessEvent);
            }
        }
    };
    Ok(DeepXPerpCancelInclusionEvidence {
        extrinsic_index: dispatch.extrinsic_index,
        outcome,
    })
}

fn perp_cancel_fields_match(fields: &Composite<u32>, expected: DeepXPerpCancelExpected) -> bool {
    let Composite::Named(fields) = fields else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    field("user").and_then(decode_event_bytes).as_deref() == Some(expected.subaccount.as_slice())
        && field("order_id").and_then(ScaleValue::as_u128) == Some(u128::from(expected.order_id))
        && field("reason").is_some_and(|value| {
            matches!(&value.value, ValueDef::Variant(reason) if reason.name == "UserCanceled")
        })
}

/// Failures decoding complete delegate account event storage.
#[derive(Debug, Error)]
pub enum DeepXDelegateAccountEventError {
    /// The approved runtime does not expose the required account events.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE decoding failed against approved metadata.
    #[error("unable to decode DeepX delegate account events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The event count or encoded length is inconsistent.
    #[error("DeepX delegate account evidence is not complete System.Events")]
    MalformedEventBytes,
    /// A delegate account event violates the approved field or phase contract.
    #[error("invalid DeepX delegate account event at index {0}")]
    InvalidChange(u32),
}

/// Failures decoding complete delegate-mode event storage.
#[derive(Debug, Error)]
pub enum DeepXDelegateModeEventError {
    /// The approved runtime does not expose the mode-change event.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE decoding failed against approved metadata.
    #[error("unable to decode DeepX delegate mode events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The event count or encoded length is inconsistent.
    #[error("DeepX delegate mode evidence is not complete System.Events")]
    MalformedEventBytes,
    /// A mode-change event violates the approved field or phase contract.
    #[error("invalid DeepX delegate mode event at index {0}")]
    InvalidChange(u32),
}

/// Failures binding a delegate account event to one finalized extrinsic.
#[derive(Debug, Error)]
pub enum DeepXDelegateAccountVerificationError {
    /// Complete event storage could not be decoded.
    #[error(transparent)]
    Events(#[from] DeepXDelegateAccountEventError),
    /// The runtime does not expose the required dispatch events.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// Dispatch event decoding failed against approved metadata.
    #[error("unable to decode DeepX delegate account dispatch events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The target extrinsic has no unique successful dispatch event.
    #[error("DeepX delegate account extrinsic has no unique successful dispatch")]
    Dispatch,
    /// The target extrinsic has no unique expected account event.
    #[error("DeepX delegate account extrinsic has no unique expected event")]
    Change,
    /// The event differs from the expected owner, delegate, or operation.
    #[error("DeepX delegate account event conflicts with expected terms")]
    ConflictingChange,
}

/// Failures binding one delegate mode change to an exact finalized extrinsic location.
#[derive(Debug, Error)]
pub enum DeepXDelegateModeVerificationError {
    /// The complete event storage could not be decoded.
    #[error(transparent)]
    Events(#[from] DeepXDelegateModeEventError),
    /// Dispatch event decoding failed against approved metadata.
    #[error("unable to decode DeepX delegate mode dispatch events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The runtime does not expose the required dispatch events.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The target extrinsic has no unique successful dispatch event.
    #[error("DeepX delegate mode extrinsic has no unique successful dispatch")]
    Dispatch,
    /// The target extrinsic has no unique mode-change event.
    #[error("DeepX delegate mode extrinsic has no unique mode change")]
    Change,
    /// The mode-change event differs from the expected wallet, delegate, or mode.
    #[error("DeepX delegate mode change conflicts with expected terms")]
    ConflictingChange,
}

/// Block and extrinsic identity supplied after the caller establishes canonical finality.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedEventLocation {
    /// Canonical block hash containing the event storage.
    pub block_hash: [u8; 32],
    /// Canonical block height containing the event storage.
    pub block_number: u64,
    /// Index of the signed extrinsic in that block.
    pub extrinsic_index: u32,
}

/// Delegate account terms expected from a finalized signed call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXDelegateAccountExpected {
    /// Account that owns the delegate relationship.
    pub owner: [u8; 20],
    /// Delegate account being set or removed.
    pub delegate: [u8; 20],
    /// Operation encoded by the signed call.
    pub kind: DeepXDelegateAccountChangeKind,
}

/// Delegate mode terms expected from a finalized signed call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXDelegateModeExpected {
    /// Account that owns the delegate relationship.
    pub owner: [u8; 20],
    /// Delegate account whose mode changes.
    pub delegate: [u8; 20],
    /// Mode encoded by the signed call.
    pub mode: DeepXDelegateMode,
}

/// Decodes delegate setup and removal from one complete finalized `System.Events` value.
///
/// The caller establishes canonical finality for the supplied block identity. The events identify
/// owner and delegate only; expiry and creation versus renewal require other evidence.
///
/// # Errors
///
/// Returns an error for unsupported metadata, incomplete SCALE bytes, or invalid event fields.
pub fn decode_finalized_delegate_account_changes(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedDelegateAccountChange>, DeepXDelegateAccountEventError> {
    interfaces.event("Subaccount", "DelegateAccountSet")?;
    interfaces.event("Subaccount", "DelegateAccountRemoved")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXDelegateAccountEventError::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut changes = Vec::new();
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    for event in events.iter() {
        let event = event.map_err(|e| DeepXDelegateAccountEventError::Decode(Box::new(e)))?;
        let event_index = count;
        count = count
            .checked_add(1)
            .ok_or(DeepXDelegateAccountEventError::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(DeepXDelegateAccountEventError::MalformedEventBytes)?;
        if event.pallet_name() != "Subaccount" {
            continue;
        }
        let kind = match event.variant_name() {
            "DelegateAccountSet" => DeepXDelegateAccountChangeKind::Set,
            "DelegateAccountRemoved" => DeepXDelegateAccountChangeKind::Removed,
            _ => continue,
        };
        let Phase::ApplyExtrinsic(extrinsic_index) = event.phase() else {
            return Err(DeepXDelegateAccountEventError::InvalidChange(event_index));
        };
        let fields = event
            .field_values()
            .map_err(|e| DeepXDelegateAccountEventError::Decode(Box::new(e)))?;
        let Composite::Named(fields) = fields else {
            return Err(DeepXDelegateAccountEventError::InvalidChange(event_index));
        };
        if fields.len() != 2 {
            return Err(DeepXDelegateAccountEventError::InvalidChange(event_index));
        }
        let field = |name| {
            fields
                .iter()
                .find_map(|(candidate, value)| (candidate == name).then_some(value))
        };
        let owner = field("owner")
            .and_then(decode_event_bytes)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(DeepXDelegateAccountEventError::InvalidChange(event_index))?;
        let delegate = field("delegate")
            .and_then(decode_event_bytes)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(DeepXDelegateAccountEventError::InvalidChange(event_index))?;
        changes.push(DeepXFinalizedDelegateAccountChange {
            block_hash,
            block_number,
            event_index,
            extrinsic_index,
            owner,
            delegate,
            kind,
        });
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(DeepXDelegateAccountEventError::MalformedEventBytes);
    }
    Ok(changes)
}

/// Decodes delegate mode changes from one complete finalized `System.Events` value.
///
/// The caller establishes canonical finality and binds an observed extrinsic to its signed call.
/// This decoder alone grants no management result.
///
/// # Errors
///
/// Returns an error for unsupported metadata, incomplete SCALE bytes, or invalid event fields.
pub fn decode_finalized_delegate_mode_changes(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedDelegateModeChange>, DeepXDelegateModeEventError> {
    interfaces.event("Subaccount", "DelegateAccountModeUpdated")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXDelegateModeEventError::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut changes = Vec::new();
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    for event in events.iter() {
        let event = event.map_err(|e| DeepXDelegateModeEventError::Decode(Box::new(e)))?;
        let event_index = count;
        count = count
            .checked_add(1)
            .ok_or(DeepXDelegateModeEventError::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(DeepXDelegateModeEventError::MalformedEventBytes)?;
        if event.pallet_name() != "Subaccount"
            || event.variant_name() != "DelegateAccountModeUpdated"
        {
            continue;
        }
        let Phase::ApplyExtrinsic(extrinsic_index) = event.phase() else {
            return Err(DeepXDelegateModeEventError::InvalidChange(event_index));
        };
        let fields = event
            .field_values()
            .map_err(|e| DeepXDelegateModeEventError::Decode(Box::new(e)))?;
        let Composite::Named(fields) = fields else {
            return Err(DeepXDelegateModeEventError::InvalidChange(event_index));
        };
        let field = |name| {
            fields
                .iter()
                .find_map(|(candidate, value)| (candidate == name).then_some(value))
        };
        let parsed = (|| {
            if fields.len() != 3 {
                return None;
            }
            let owner = decode_event_bytes(field("owner")?)?.try_into().ok()?;
            let delegate = decode_event_bytes(field("delegate")?)?.try_into().ok()?;
            let mode = match decode_unit_variant(
                field("new_mode")?,
                &[
                    "PlaceOrCancelOrder",
                    "DepositOrWithdraw",
                    "UpdateSubaccount",
                    "Disable",
                ],
            )? {
                "PlaceOrCancelOrder" => DeepXDelegateMode::PlaceOrCancelOrder,
                "DepositOrWithdraw" => DeepXDelegateMode::DepositOrWithdraw,
                "UpdateSubaccount" => DeepXDelegateMode::UpdateSubaccount,
                "Disable" => DeepXDelegateMode::Disable,
                _ => return None,
            };
            Some((owner, delegate, mode))
        })()
        .ok_or(DeepXDelegateModeEventError::InvalidChange(event_index))?;
        changes.push(DeepXFinalizedDelegateModeChange {
            block_hash,
            block_number,
            event_index,
            extrinsic_index,
            owner: parsed.0,
            delegate: parsed.1,
            mode: parsed.2,
        });
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(DeepXDelegateModeEventError::MalformedEventBytes);
    }
    Ok(changes)
}

/// Binds a delegate account change and successful dispatch to one finalized extrinsic index.
///
/// The caller establishes canonical finality and separately verifies the signed call.
///
/// # Errors
///
/// Returns an error for incomplete events, missing or duplicate evidence, failed dispatch, or
/// an event that differs from the expected operation and accounts.
pub fn verify_finalized_delegate_account_change_events(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    location: DeepXFinalizedEventLocation,
    event_bytes: &[u8],
    expected: DeepXDelegateAccountExpected,
) -> Result<DeepXFinalizedDelegateAccountChange, DeepXDelegateAccountVerificationError> {
    use DeepXDelegateAccountVerificationError as Error;

    interfaces.event("System", "ExtrinsicSuccess")?;
    interfaces.event("System", "ExtrinsicFailed")?;
    let changes = decode_finalized_delegate_account_changes(
        interfaces,
        metadata,
        location.block_hash,
        location.block_number,
        event_bytes,
    )?;
    let mut change = None;
    for candidate in changes {
        if candidate.extrinsic_index != location.extrinsic_index {
            continue;
        }
        if candidate.owner != expected.owner
            || candidate.delegate != expected.delegate
            || candidate.kind != expected.kind
        {
            return Err(Error::ConflictingChange);
        }
        if change.replace(candidate).is_some() {
            return Err(Error::Change);
        }
    }
    let dispatch = decode_indexed_system_dispatch(metadata, location.extrinsic_index, event_bytes)
        .map_err(|e| match e {
            DeepXIndexedSystemDispatchError::Decode(e) => Error::Decode(e),
            _ => Error::Dispatch,
        })?;
    if dispatch.outcome != DeepXSystemDispatchOutcome::Success {
        return Err(Error::Dispatch);
    }
    change.ok_or(Error::Change)
}

/// Binds a delegate mode change and successful dispatch to one finalized extrinsic index.
///
/// The caller establishes canonical finality and separately verifies the signed call.
///
/// # Errors
///
/// Returns an error for incomplete events, missing or duplicate evidence, failed dispatch, or
/// a mode change that differs from the expected terms.
pub fn verify_finalized_delegate_mode_change_events(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &Metadata,
    location: DeepXFinalizedEventLocation,
    event_bytes: &[u8],
    expected: DeepXDelegateModeExpected,
) -> Result<DeepXFinalizedDelegateModeChange, DeepXDelegateModeVerificationError> {
    use DeepXDelegateModeVerificationError as Error;

    interfaces.event("System", "ExtrinsicSuccess")?;
    interfaces.event("System", "ExtrinsicFailed")?;
    let changes = decode_finalized_delegate_mode_changes(
        interfaces,
        metadata,
        location.block_hash,
        location.block_number,
        event_bytes,
    )?;
    let mut change = None;
    for candidate in changes {
        if candidate.extrinsic_index != location.extrinsic_index {
            continue;
        }
        if candidate.owner != expected.owner
            || candidate.delegate != expected.delegate
            || candidate.mode != expected.mode
        {
            return Err(Error::ConflictingChange);
        }
        if change.replace(candidate).is_some() {
            return Err(Error::Change);
        }
    }
    let dispatch = decode_indexed_system_dispatch(metadata, location.extrinsic_index, event_bytes)
        .map_err(|e| match e {
            DeepXIndexedSystemDispatchError::Decode(e) => Error::Decode(e),
            _ => Error::Dispatch,
        })?;
    if dispatch.outcome != DeepXSystemDispatchOutcome::Success {
        return Err(Error::Dispatch);
    }
    change.ok_or(Error::Change)
}

pub(crate) fn decode_event_bytes(value: &ScaleValue<u32>) -> Option<Vec<u8>> {
    let ValueDef::Composite(bytes) = &value.value else {
        return None;
    };
    let values: Vec<_> = bytes.values().collect();
    if values.len() == 1 && values[0].as_u128().is_none() {
        return decode_event_bytes(values[0]);
    }
    values
        .into_iter()
        .map(|value| u8::try_from(value.as_u128()?).ok())
        .collect()
}

pub(crate) fn decode_unit_variant<'a>(
    value: &ScaleValue<u32>,
    allowed: &'a [&str],
) -> Option<&'a str> {
    let ValueDef::Variant(variant) = &value.value else {
        return None;
    };
    allowed
        .iter()
        .copied()
        .find(|name| variant.name == *name && variant.values.values().next().is_none())
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

    fn event_record(
        runtime: &crate::signing::DeepXValidatedRuntime,
        name: &str,
        phase: Phase,
        mode: Option<&str>,
    ) -> Vec<u8> {
        let pallet = runtime.metadata().pallet_by_name("Subaccount").unwrap();
        let event_index = runtime
            .interfaces()
            .event("Subaccount", name)
            .unwrap()
            .index();
        let event = pallet.event_variant_by_index(event_index).unwrap();
        let mut bytes = phase.encode();
        bytes.extend([pallet.index(), event.index]);
        let values = [
            ScaleValue::from_bytes([0x11; 20]),
            ScaleValue::from_bytes([0x22; 20]),
        ];
        for (field, value) in event.fields.iter().take(2).zip(values) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                runtime.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        }
        if let Some(mode) = mode {
            let field = &event.fields[2];
            subxt_core::ext::scale_value::scale::encode_as_type(
                &ScaleValue::unnamed_variant(mode, []),
                field.ty.id,
                runtime.metadata().types(),
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

    fn dispatch_record(
        runtime: &crate::signing::DeepXValidatedRuntime,
        index: u32,
        success: bool,
    ) -> Vec<u8> {
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
        let mut bytes = Phase::ApplyExtrinsic(index).encode();
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

    fn perp_cancel_record(
        runtime: &crate::signing::DeepXValidatedRuntime,
        index: u32,
        subaccount: [u8; 20],
        order_id: u64,
        reason_index: u8,
    ) -> Vec<u8> {
        let pallet = runtime.interfaces().pallet("PerpMarket").unwrap();
        let event = runtime
            .interfaces()
            .event("PerpMarket", "OrderCancelled")
            .unwrap();
        let mut bytes = Phase::ApplyExtrinsic(index).encode();
        bytes.extend([pallet.index(), event.index()]);
        bytes.extend(subaccount);
        order_id.encode_to(&mut bytes);
        bytes.push(reason_index);
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    #[rstest]
    fn perp_cancel_business_event_binds_exact_terms_and_complete_storage() {
        let runtime = runtime();
        let expected = DeepXPerpCancelExpected {
            subaccount: [42; 20],
            order_id: 9001,
            extrinsic_index: 3,
        };
        let record = perp_cancel_record(&runtime, 3, [42; 20], 9001, 0);
        let decode = |bytes: &[u8]| {
            decode_perp_cancel_business_event(
                runtime.interfaces(),
                runtime.metadata(),
                expected,
                bytes,
            )
        };
        let observed = decode(&system_events(std::slice::from_ref(&record))).unwrap();
        assert_eq!(
            observed,
            DeepXPerpCancelBusinessEvent {
                extrinsic_index: 3,
                outcome: DeepXPerpCancelBusinessOutcome::Success,
            }
        );
        assert_eq!(
            decode(&system_events(&[perp_cancel_record(
                &runtime, 4, [42; 20], 9001, 0
            )]))
            .unwrap()
            .outcome,
            DeepXPerpCancelBusinessOutcome::NotObserved,
        );
        for (account, order_id, reason) in [
            ([41; 20], 9001, 0),
            ([42; 20], 9002, 0),
            ([42; 20], 9001, 1),
        ] {
            assert!(matches!(
                decode(&system_events(&[perp_cancel_record(
                    &runtime, 3, account, order_id, reason
                )])),
                Err(DeepXPerpCancelEventError::ConflictingEvent),
            ));
        }
        assert!(matches!(
            decode(&system_events(&[record.clone(), record])),
            Err(DeepXPerpCancelEventError::DuplicateEvent),
        ));
        for bytes in [vec![4], vec![0, 0]] {
            assert!(matches!(
                decode(&bytes),
                Err(DeepXPerpCancelEventError::MalformedEventBytes),
            ));
        }
    }

    #[rstest]
    fn perp_cancel_inclusion_requires_consistent_dispatch_and_business_evidence() {
        let runtime = runtime();
        let expected = DeepXPerpCancelExpected {
            subaccount: [42; 20],
            order_id: 9001,
            extrinsic_index: 3,
        };
        let business = perp_cancel_record(&runtime, 3, expected.subaccount, expected.order_id, 0);
        let success = dispatch_record(&runtime, 3, true);
        let failed = dispatch_record(&runtime, 3, false);
        let decode = |fast_cancel, bytes: &[u8]| {
            decode_perp_cancel_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                expected,
                fast_cancel,
                bytes,
            )
        };

        assert!(matches!(
            decode(false, &system_events(&[business.clone(), success.clone()])),
            Ok(DeepXPerpCancelInclusionEvidence {
                extrinsic_index: 3,
                outcome: DeepXPerpCancelInclusionOutcome::Success,
            }),
        ));
        assert!(matches!(
            decode(false, &system_events(std::slice::from_ref(&failed))),
            Ok(DeepXPerpCancelInclusionEvidence {
                extrinsic_index: 3,
                outcome: DeepXPerpCancelInclusionOutcome::Failed,
            }),
        ));
        assert!(matches!(
            decode(false, &system_events(std::slice::from_ref(&success))),
            Err(DeepXPerpCancelInclusionError::MissingBusinessEvent),
        ));
        assert!(matches!(
            decode(false, &system_events(&[business.clone(), failed.clone()])),
            Err(DeepXPerpCancelInclusionError::ConflictingBusinessEvent),
        ));
        assert!(matches!(
            decode(true, &system_events(std::slice::from_ref(&success))),
            Ok(DeepXPerpCancelInclusionEvidence {
                extrinsic_index: 3,
                outcome: DeepXPerpCancelInclusionOutcome::Success,
            }),
        ));
        assert!(matches!(
            decode(true, &system_events(&[success.clone(), success])),
            Err(DeepXPerpCancelInclusionError::Dispatch(
                DeepXIndexedSystemDispatchError::DuplicateEvent,
            )),
        ));
        assert!(matches!(
            decode(
                false,
                &system_events(&[business, failed, dispatch_record(&runtime, 4, true)])
            ),
            Err(DeepXPerpCancelInclusionError::ConflictingBusinessEvent),
        ));
    }

    #[rstest]
    fn decodes_unique_system_dispatch_from_complete_storage() {
        let runtime = runtime();
        let success = dispatch_record(&runtime, 2, true);
        let failed = dispatch_record(&runtime, 2, false);
        assert_eq!(
            decode_system_dispatch(
                runtime.metadata(),
                2,
                &system_events(std::slice::from_ref(&success)),
            ),
            Ok(DeepXSystemDispatchOutcome::Success),
        );
        assert!(matches!(
            decode_indexed_system_dispatch(
                runtime.metadata(),
                2,
                &system_events(std::slice::from_ref(&success)),
            ),
            Ok(DeepXIndexedSystemDispatch {
                extrinsic_index: 2,
                outcome: DeepXSystemDispatchOutcome::Success,
            }),
        ));
        assert_eq!(
            decode_system_dispatch(runtime.metadata(), 2, &system_events(&[failed])),
            Ok(DeepXSystemDispatchOutcome::Failed),
        );
        for records in [
            Vec::new(),
            vec![dispatch_record(&runtime, 1, true)],
            vec![success.clone(), success.clone()],
        ] {
            assert_eq!(
                decode_system_dispatch(runtime.metadata(), 2, &system_events(&records)),
                Err(DeepXSystemDispatchError::MissingSystemDispatch),
            );
        }
        assert!(matches!(
            decode_indexed_system_dispatch(runtime.metadata(), 2, &system_events(&[])),
            Err(DeepXIndexedSystemDispatchError::MissingEvent),
        ));
        assert!(matches!(
            decode_indexed_system_dispatch(
                runtime.metadata(),
                2,
                &system_events(&[success.clone(), success.clone()]),
            ),
            Err(DeepXIndexedSystemDispatchError::DuplicateEvent),
        ));
        let mut malformed = system_events(&[success]);
        malformed.push(0);
        assert_eq!(
            decode_system_dispatch(runtime.metadata(), 2, &malformed),
            Err(DeepXSystemDispatchError::InvalidEventStorage),
        );
        assert!(matches!(
            decode_indexed_system_dispatch(runtime.metadata(), 2, &malformed),
            Err(DeepXIndexedSystemDispatchError::MalformedEventBytes),
        ));
    }

    #[rstest]
    #[case("DelegateAccountSet", DeepXDelegateAccountChangeKind::Set)]
    #[case("DelegateAccountRemoved", DeepXDelegateAccountChangeKind::Removed)]
    fn decodes_delegate_account_event(
        #[case] name: &str,
        #[case] kind: DeepXDelegateAccountChangeKind,
    ) {
        let runtime = runtime();
        let bytes = system_events(&[event_record(&runtime, name, Phase::ApplyExtrinsic(7), None)]);
        let changes = decode_finalized_delegate_account_changes(
            runtime.interfaces(),
            runtime.metadata(),
            [3; 32],
            42,
            &bytes,
        )
        .unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].block_hash, [3; 32]);
        assert_eq!(changes[0].block_number, 42);
        assert_eq!(changes[0].event_index, 0);
        assert_eq!(changes[0].extrinsic_index, 7);
        assert_eq!(changes[0].owner, [0x11; 20]);
        assert_eq!(changes[0].delegate, [0x22; 20]);
        assert_eq!(changes[0].kind, kind);
    }

    #[rstest]
    fn verifies_delegate_account_change_and_successful_dispatch() {
        let runtime = runtime();
        let change = event_record(
            &runtime,
            "DelegateAccountSet",
            Phase::ApplyExtrinsic(7),
            None,
        );
        let success = dispatch_record(&runtime, 7, true);
        let verify = |bytes: &[u8], owner| {
            verify_finalized_delegate_account_change_events(
                runtime.interfaces(),
                runtime.metadata(),
                DeepXFinalizedEventLocation {
                    block_hash: [3; 32],
                    block_number: 42,
                    extrinsic_index: 7,
                },
                bytes,
                DeepXDelegateAccountExpected {
                    owner,
                    delegate: [0x22; 20],
                    kind: DeepXDelegateAccountChangeKind::Set,
                },
            )
        };

        let complete = system_events(&[change.clone(), success.clone()]);
        let observed = verify(&complete, [0x11; 20]).unwrap();
        assert_eq!(observed.extrinsic_index, 7);
        assert_eq!(observed.event_index, 0);
        assert!(matches!(
            verify(&complete, [0x33; 20]),
            Err(DeepXDelegateAccountVerificationError::ConflictingChange),
        ));
        assert!(matches!(
            verify(
                &system_events(&[change.clone(), dispatch_record(&runtime, 7, false)]),
                [0x11; 20]
            ),
            Err(DeepXDelegateAccountVerificationError::Dispatch),
        ));
        assert!(matches!(
            verify(
                &system_events(&[change.clone(), change, success]),
                [0x11; 20]
            ),
            Err(DeepXDelegateAccountVerificationError::Change),
        ));
        let mut trailing = complete;
        trailing.push(0);
        assert!(verify(&trailing, [0x11; 20]).is_err());
    }

    #[rstest]
    #[case("PlaceOrCancelOrder", DeepXDelegateMode::PlaceOrCancelOrder)]
    #[case("DepositOrWithdraw", DeepXDelegateMode::DepositOrWithdraw)]
    #[case("UpdateSubaccount", DeepXDelegateMode::UpdateSubaccount)]
    #[case("Disable", DeepXDelegateMode::Disable)]
    fn decodes_delegate_mode_event(#[case] name: &str, #[case] mode: DeepXDelegateMode) {
        let runtime = runtime();
        let bytes = system_events(&[event_record(
            &runtime,
            "DelegateAccountModeUpdated",
            Phase::ApplyExtrinsic(7),
            Some(name),
        )]);
        let changes = decode_finalized_delegate_mode_changes(
            runtime.interfaces(),
            runtime.metadata(),
            [3; 32],
            42,
            &bytes,
        )
        .unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].block_hash, [3; 32]);
        assert_eq!(changes[0].block_number, 42);
        assert_eq!(changes[0].event_index, 0);
        assert_eq!(changes[0].extrinsic_index, 7);
        assert_eq!(changes[0].owner, [0x11; 20]);
        assert_eq!(changes[0].delegate, [0x22; 20]);
        assert_eq!(changes[0].mode, mode);
        let unrelated = system_events(&[event_record(
            &runtime,
            "DelegateAccountSet",
            Phase::ApplyExtrinsic(7),
            None,
        )]);
        assert!(
            decode_finalized_delegate_mode_changes(
                runtime.interfaces(),
                runtime.metadata(),
                [3; 32],
                42,
                &unrelated,
            )
            .unwrap()
            .is_empty()
        );
    }

    #[rstest]
    fn verifies_delegate_mode_change_and_successful_dispatch() {
        let runtime = runtime();
        let change = event_record(
            &runtime,
            "DelegateAccountModeUpdated",
            Phase::ApplyExtrinsic(7),
            Some("Disable"),
        );
        let success = dispatch_record(&runtime, 7, true);
        let verify = |bytes: &[u8], mode| {
            verify_finalized_delegate_mode_change_events(
                runtime.interfaces(),
                runtime.metadata(),
                DeepXFinalizedEventLocation {
                    block_hash: [3; 32],
                    block_number: 42,
                    extrinsic_index: 7,
                },
                bytes,
                DeepXDelegateModeExpected {
                    owner: [0x11; 20],
                    delegate: [0x22; 20],
                    mode,
                },
            )
        };

        let complete = system_events(&[change.clone(), success.clone()]);
        let observed = verify(&complete, DeepXDelegateMode::Disable).unwrap();
        assert_eq!(observed.extrinsic_index, 7);
        assert_eq!(observed.event_index, 0);
        assert!(matches!(
            verify(&complete, DeepXDelegateMode::PlaceOrCancelOrder),
            Err(DeepXDelegateModeVerificationError::ConflictingChange),
        ));
        assert!(matches!(
            verify(
                &system_events(&[change.clone(), dispatch_record(&runtime, 7, false)]),
                DeepXDelegateMode::Disable
            ),
            Err(DeepXDelegateModeVerificationError::Dispatch),
        ));
        assert!(matches!(
            verify(
                &system_events(&[change.clone(), change, success]),
                DeepXDelegateMode::Disable
            ),
            Err(DeepXDelegateModeVerificationError::Change),
        ));
        let mut trailing = complete;
        trailing.push(0);
        assert!(verify(&trailing, DeepXDelegateMode::Disable).is_err());
    }

    #[test]
    fn rejects_delegate_events_without_extrinsic_phase() {
        let runtime = runtime();
        let account = system_events(&[event_record(
            &runtime,
            "DelegateAccountSet",
            Phase::Initialization,
            None,
        )]);
        assert!(matches!(
            decode_finalized_delegate_account_changes(
                runtime.interfaces(),
                runtime.metadata(),
                [3; 32],
                42,
                &account,
            ),
            Err(DeepXDelegateAccountEventError::InvalidChange(0)),
        ));
        let mode = system_events(&[event_record(
            &runtime,
            "DelegateAccountModeUpdated",
            Phase::Initialization,
            Some("Disable"),
        )]);
        assert!(matches!(
            decode_finalized_delegate_mode_changes(
                runtime.interfaces(),
                runtime.metadata(),
                [3; 32],
                42,
                &mode,
            ),
            Err(DeepXDelegateModeEventError::InvalidChange(0)),
        ));
    }

    #[test]
    fn rejects_incomplete_delegate_mode_event_storage() {
        let runtime = runtime();
        let change = event_record(
            &runtime,
            "DelegateAccountModeUpdated",
            Phase::ApplyExtrinsic(7),
            Some("PlaceOrCancelOrder"),
        );
        let complete = system_events(std::slice::from_ref(&change));
        let mut trailing = complete.clone();
        trailing.push(0);
        let mut wrong_count = complete.clone();
        wrong_count[0] = Compact(2_u32).encode()[0];
        let truncated = &complete[..complete.len() - 1];
        for bytes in [&trailing[..], &wrong_count, truncated] {
            assert!(
                decode_finalized_delegate_mode_changes(
                    runtime.interfaces(),
                    runtime.metadata(),
                    [3; 32],
                    42,
                    bytes,
                )
                .is_err()
            );
        }
        let mut invalid_mode = change;
        let phase_length = Phase::ApplyExtrinsic(7).encode().len();
        invalid_mode[phase_length + 2 + 40] = u8::MAX;
        assert!(
            decode_finalized_delegate_mode_changes(
                runtime.interfaces(),
                runtime.metadata(),
                [3; 32],
                42,
                &system_events(&[invalid_mode]),
            )
            .is_err()
        );
    }

    #[test]
    fn accepts_complete_empty_event_storage() {
        let runtime = runtime();
        assert!(
            decode_finalized_delegate_account_changes(
                runtime.interfaces(),
                runtime.metadata(),
                [3; 32],
                42,
                &[0]
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            decode_finalized_delegate_mode_changes(
                runtime.interfaces(),
                runtime.metadata(),
                [3; 32],
                42,
                &[0]
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn rejects_truncated_or_trailing_event_storage() {
        let runtime = runtime();
        for bytes in [&[][..], &[4][..], &[0, 0][..]] {
            assert!(
                decode_finalized_delegate_account_changes(
                    runtime.interfaces(),
                    runtime.metadata(),
                    [3; 32],
                    42,
                    bytes
                )
                .is_err()
            );
            assert!(
                decode_finalized_delegate_mode_changes(
                    runtime.interfaces(),
                    runtime.metadata(),
                    [3; 32],
                    42,
                    bytes
                )
                .is_err()
            );
        }
    }
}
