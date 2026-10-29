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

#[cfg(test)]
use std::collections::HashSet;

#[cfg(test)]
use deepx_rust_sdk::rpc::execute_rpc_read_with_retry;
use deepx_rust_sdk::rpc::{
    DeepXRpcBlockHeader as RpcBlockHeader, DeepXRpcBlockResponse as RpcBlockResponse,
    DeepXRpcClient, DeepXRpcRuntimeVersion as HistoricalRuntimeVersion,
    chain_decode::{decode_block_number_u64, decode_prefixed_hash},
    event_decode::{
        DeepXDelegateAccountExpected, DeepXDelegateModeExpected, DeepXFinalizedEventLocation,
        DeepXIndexedSystemDispatchError, DeepXPerpCancelBusinessOutcome, DeepXPerpCancelEventError,
        DeepXPerpCancelExpected, DeepXPerpCancelInclusionError, DeepXPerpCancelInclusionOutcome,
        DeepXSystemDispatchError, DeepXSystemDispatchOutcome,
        decode_finalized_delegate_account_changes as sdk_decode_finalized_delegate_account_changes,
        decode_finalized_delegate_mode_changes as sdk_decode_finalized_delegate_mode_changes,
        decode_perp_cancel_business_event, decode_perp_cancel_inclusion, decode_system_dispatch,
        verify_finalized_delegate_account_change_events as sdk_verify_delegate_account_change,
        verify_finalized_delegate_mode_change_events as sdk_verify_delegate_mode_change,
    },
    event_storage::{
        SYSTEM_EVENTS_STORAGE_KEY, combine_events_map_batches, decode_event_storage_value,
        decode_event_threads, system_events_map_storage_key, system_threads_storage_key,
    },
    get_metadata, get_runtime_version,
    perp_modify::{
        DeepXPerpModifyEventError, DeepXPerpModifyEventOrder, DeepXPerpModifySizingExpected,
        decode_perp_modify_event_order, decode_perp_modify_sizing_event,
    },
    perp_place::{
        DeepXPerpPlaceEventError, DeepXPerpPlaceExpected, DeepXPerpPlaceInclusionOutcome,
        decode_perp_place_inclusion,
    },
    perp_position::{
        DeepXPerpCloseExpected, DeepXPerpPointUpdateExpected, DeepXPerpPositionEventError,
        DeepXPerpPositionInclusionOutcome, decode_perp_close_inclusion,
        decode_perp_point_update_inclusion,
    },
    spot_cancel::{
        DeepXSpotCancelEventError, DeepXSpotCancelExpected, DeepXSpotCancelInclusionOutcome,
        decode_spot_cancel_inclusion,
    },
    spot_place::{
        DeepXSpotPlaceEventError, DeepXSpotPlaceExpected, DeepXSpotPlaceInclusionOutcome,
        decode_spot_place_inclusion,
    },
    submission::{DeepXExtrinsicLookupError, find_extrinsic_by_hash},
    trade_match::{
        match_finalized_perp_account_trade as sdk_match_finalized_perp_account_trade,
        match_finalized_spot_account_trade as sdk_match_finalized_spot_account_trade,
        match_finalized_wallet_perp_trades as sdk_match_finalized_wallet_perp_trades,
    },
};
pub use deepx_rust_sdk::rpc::{
    event_decode::{
        DeepXDelegateAccountEventError, DeepXDelegateAccountVerificationError,
        DeepXDelegateModeEventError, DeepXDelegateModeVerificationError,
    },
    events::{
        DeepXDelegateAccountChangeKind, DeepXFinalizedDelegateAccountChange,
        DeepXFinalizedDelegateModeChange, DeepXFinalizedPerpTrade, DeepXFinalizedSpotTrade,
    },
    trade_event_decode::{DeepXPerpTradeEventError, DeepXSpotTradeEventError},
    trade_match::{
        DeepXFinalizedWalletPerpTradeMatchError, DeepXPerpTradeMatchError, DeepXSpotTradeMatchError,
    },
};
use nautilus_core::hex;
#[cfg(test)]
use parity_scale_codec::{Compact, Decode};
#[cfg(test)]
use serde_json::json;
#[cfg(test)]
use subxt_core::config::{Hasher, substrate::BlakeTwo256};
#[cfg(test)]
use subxt_core::events::{Events, Phase};
#[cfg(test)]
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
    common::{DeepXEnvironment, DeepXPrivateKey},
    config::{DeepXRpcRole, DeepXValidatedRpcEndpoints},
    http::{
        DeepXDelegateMode, DeepXPerpAccountTradeRecord, DeepXPerpMarketLookup,
        DeepXPerpWalletTradeMarket, DeepXSpotAccountTradeRecord,
    },
    rpc::{
        DEEPX_RECOVERY_RPC_METHODS, DEEPX_SUBMISSION_RPC_METHODS, DEEPX_WATCH_RPC_METHODS,
        DeepXHistoricalRuntimeError, DeepXValidatedRpcMethodCapabilities,
    },
    signing::{
        ApprovedRuntimeIdentity, DeepXPerpModifyParams, DeepXReadOnlyRuntimeSnapshot,
        DeepXRemoveDelegateAccountParams, DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError,
        DeepXRuntimeSnapshotPermit, DeepXSetDelegateAccountParams, DeepXSpotPlaceParams,
        DeepXUpdateDelegateModeParams, DeepXWritableDelegateMode, RuntimeSnapshot, SigningError,
        SnapshotError, sign_remove_delegate_account, sign_set_delegate_account,
        sign_update_delegate_mode,
    },
};

#[cfg(test)]
use crate::signing::DeepXRuntimeConfig;

async fn observe_historical_read_only_runtime(
    environment: &DeepXEnvironment,
    genesis_hash: [u8; 32],
    watch_url: &str,
    block_hash: [u8; 32],
) -> Result<DeepXReadOnlyRuntimeSnapshot, DeepXHistoricalRuntimeError> {
    let client = DeepXRpcClient::new(watch_url.to_string());
    let encoded_hash = format!("0x{}", hex::encode(block_hash));
    let version: HistoricalRuntimeVersion = get_runtime_version(&client, &encoded_hash)
        .await
        .map_err(|source| DeepXHistoricalRuntimeError::Rpc {
            method: "state_getRuntimeVersion",
            source,
        })?;
    let encoded_metadata = get_metadata(&client, &encoded_hash)
        .await
        .map_err(|source| DeepXHistoricalRuntimeError::Rpc {
            method: "state_getMetadata",
            source,
        })?;
    let metadata = encoded_metadata
        .strip_prefix("0x")
        .filter(|value| !value.is_empty())
        .and_then(|value| hex::decode(value).ok())
        .ok_or(DeepXHistoricalRuntimeError::InvalidMetadata)?;
    DeepXReadOnlyRuntimeSnapshot::approved_testnet(
        environment,
        genesis_hash,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )
    .map_err(DeepXHistoricalRuntimeError::from)
}

trait RuntimeMetadataView {
    fn identity(&self) -> &ApprovedRuntimeIdentity;
    fn interfaces(&self) -> &DeepXRuntimeInterfaceCatalog;
    fn metadata(&self) -> &subxt_core::metadata::Metadata;
}

impl RuntimeMetadataView for RuntimeSnapshot {
    fn identity(&self) -> &ApprovedRuntimeIdentity {
        self.identity()
    }

    fn interfaces(&self) -> &DeepXRuntimeInterfaceCatalog {
        self.interfaces()
    }

    fn metadata(&self) -> &subxt_core::metadata::Metadata {
        self.metadata()
    }
}

impl RuntimeMetadataView for DeepXReadOnlyRuntimeSnapshot {
    fn identity(&self) -> &ApprovedRuntimeIdentity {
        self.identity()
    }

    fn interfaces(&self) -> &DeepXRuntimeInterfaceCatalog {
        self.interfaces()
    }

    fn metadata(&self) -> &subxt_core::metadata::Metadata {
        self.metadata()
    }
}

impl<T: RuntimeMetadataView + ?Sized> RuntimeMetadataView for &T {
    fn identity(&self) -> &ApprovedRuntimeIdentity {
        (*self).identity()
    }

    fn interfaces(&self) -> &DeepXRuntimeInterfaceCatalog {
        (*self).interfaces()
    }

    fn metadata(&self) -> &subxt_core::metadata::Metadata {
        (*self).metadata()
    }
}

/// Matches one REST account trade to one decoded event, including its venue trade ID.
///
/// The caller supplies the validated account and market's base/quote decimals and quote symbol.
/// Runtime prices use the chain's fixed six-decimal quote precision and leverage uses three
/// decimals. The REST ID encodes the block number times one million plus the global event index.
/// The caller must independently establish canonical finality
/// for the event's block; a match does not prove the REST timestamp.
///
/// # Errors
///
/// Returns an error for malformed inputs, inexact values, no match, or multiple exact matches.
pub fn match_finalized_perp_account_trade<'a>(
    trades: &'a [DeepXFinalizedPerpTrade],
    record: &DeepXPerpAccountTradeRecord,
    subaccount: [u8; 20],
    base_decimal: u8,
    quote_decimal: u8,
    quote_symbol: &str,
) -> Result<&'a DeepXFinalizedPerpTrade, DeepXPerpTradeMatchError> {
    sdk_match_finalized_perp_account_trade(
        trades,
        &record.0,
        subaccount,
        base_decimal,
        quote_decimal,
        quote_symbol,
    )
}

/// Matches one Spot REST trade to a supplied finalized event and its global event index.
///
/// The caller validates the REST response's account request and independently establishes
/// canonical finality of each event block. The six-decimal runtime price and event-ID formula
/// follow the approved node source and the REST trade-ID example; this match does not prove
/// the REST timestamp or historical completeness.
///
/// # Errors
///
/// Returns an error for invalid market scales, inexact values, or missing or ambiguous evidence.
pub fn match_finalized_spot_account_trade<'a>(
    trades: &'a [DeepXFinalizedSpotTrade],
    record: &DeepXSpotAccountTradeRecord,
    subaccount: [u8; 20],
    base_decimal: u8,
    quote_decimal: u8,
    base_symbol: &str,
    quote_symbol: &str,
) -> Result<&'a DeepXFinalizedSpotTrade, DeepXSpotTradeMatchError> {
    sdk_match_finalized_spot_account_trade(
        trades,
        &record.0,
        subaccount,
        base_decimal,
        quote_decimal,
        base_symbol,
        quote_symbol,
    )
}

/// One wallet account trade matched to a finalized perpetual event.
pub type DeepXFinalizedWalletPerpTradeMatch<'a> =
    deepx_rust_sdk::rpc::trade_match::DeepXFinalizedWalletPerpTradeMatch<
        'a,
        DeepXPerpAccountTradeRecord,
    >;

/// Matches every event involving a supplied wallet subaccount against complete REST groups.
///
/// The caller must independently validate wallet ownership and obtain finalized events. Market
/// metadata and wallet pages are current, mutable REST observations; this match does not prove a
/// historical wallet directory, REST timestamp, or cryptographic storage inclusion.
///
/// # Errors
///
/// Returns an error without partial matches for conflicting identity, incomplete pages, missing
/// records, duplicate records, or exact financial mismatches.
pub fn match_finalized_wallet_perp_trades<'a>(
    trades: &'a [DeepXFinalizedPerpTrade],
    groups: &'a [DeepXPerpWalletTradeMarket],
    subaccounts: &[[u8; 20]],
    markets: &[DeepXPerpMarketLookup],
) -> Result<Vec<DeepXFinalizedWalletPerpTradeMatch<'a>>, DeepXFinalizedWalletPerpTradeMatchError> {
    sdk_match_finalized_wallet_perp_trades(trades, groups, subaccounts, markets)
}

/// Decodes delegate setup and removal from one complete finalized `System.Events` value.
///
/// The events identify owner and delegate only. Setup labels, expiry, and creation versus renewal
/// must be established through the exact signed call and any subsequent state observation.
/// The caller independently establishes canonical finality for the supplied block identity.
///
/// # Errors
///
/// Returns an error for unsupported metadata, incomplete SCALE bytes, or invalid event fields.
pub fn decode_finalized_delegate_account_changes(
    snapshot: &RuntimeSnapshot,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedDelegateAccountChange>, DeepXDelegateAccountEventError> {
    decode_finalized_delegate_account_changes_with_runtime(
        snapshot,
        block_hash,
        block_number,
        event_bytes,
    )
}

fn decode_finalized_delegate_account_changes_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedDelegateAccountChange>, DeepXDelegateAccountEventError> {
    sdk_decode_finalized_delegate_account_changes(
        snapshot.interfaces(),
        snapshot.metadata(),
        block_hash,
        block_number,
        event_bytes,
    )
}

/// Binds a delegate setup or removal and successful dispatch to one finalized location.
///
/// The caller supplies complete `System.Events` read at `location.block_hash()` and separately
/// proves that the located signed extrinsic encodes the expected account call.
///
/// # Errors
///
/// Returns an error for incomplete events, missing or duplicate same-index evidence, failed
/// dispatch, or an event that differs from the expected identities or operation.
pub fn verify_finalized_delegate_account_change_events(
    snapshot: &RuntimeSnapshot,
    location: DeepXFinalizedTransactionLocation,
    event_bytes: &[u8],
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_kind: DeepXDelegateAccountChangeKind,
) -> Result<DeepXFinalizedDelegateAccountChange, DeepXDelegateAccountVerificationError> {
    verify_finalized_delegate_account_change_events_with_runtime(
        snapshot,
        location,
        event_bytes,
        expected_owner,
        expected_delegate,
        expected_kind,
    )
}

fn verify_finalized_delegate_account_change_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    location: DeepXFinalizedTransactionLocation,
    event_bytes: &[u8],
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_kind: DeepXDelegateAccountChangeKind,
) -> Result<DeepXFinalizedDelegateAccountChange, DeepXDelegateAccountVerificationError> {
    sdk_verify_delegate_account_change(
        snapshot.interfaces(),
        snapshot.metadata(),
        DeepXFinalizedEventLocation {
            block_hash: location.block_hash(),
            block_number: location.block_number(),
            extrinsic_index: location.extrinsic_index(),
        },
        event_bytes,
        DeepXDelegateAccountExpected {
            owner: expected_owner,
            delegate: expected_delegate,
            kind: expected_kind,
        },
    )
}

/// Decodes delegate mode changes from one complete finalized `System.Events` value.
///
/// The caller independently establishes canonical finality for the supplied block identity and
/// binds an observed extrinsic to its signed call. This decoder alone grants no management result.
///
/// # Errors
///
/// Returns an error for unsupported metadata, incomplete SCALE bytes, or invalid event fields.
pub fn decode_finalized_delegate_mode_changes(
    snapshot: &RuntimeSnapshot,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedDelegateModeChange>, DeepXDelegateModeEventError> {
    decode_finalized_delegate_mode_changes_with_runtime(
        snapshot,
        block_hash,
        block_number,
        event_bytes,
    )
}

fn decode_finalized_delegate_mode_changes_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedDelegateModeChange>, DeepXDelegateModeEventError> {
    sdk_decode_finalized_delegate_mode_changes(
        snapshot.interfaces(),
        snapshot.metadata(),
        block_hash,
        block_number,
        event_bytes,
    )
}

/// Binds a mode change and successful dispatch to one finalized extrinsic location.
///
/// The caller supplies complete `System.Events` read at `location.block_hash()` and independently
/// verifies that the located signed extrinsic encodes the expected mode-change call. This function
/// does not authorize submission or treat a later REST delegate state as finalized.
///
/// # Errors
///
/// Returns an error for incomplete events, missing or duplicate same-index evidence, failed
/// dispatch, or a mode change that differs from the expected terms.
pub fn verify_finalized_delegate_mode_change_events(
    snapshot: &RuntimeSnapshot,
    location: DeepXFinalizedTransactionLocation,
    event_bytes: &[u8],
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_mode: DeepXDelegateMode,
) -> Result<DeepXFinalizedDelegateModeChange, DeepXDelegateModeVerificationError> {
    verify_finalized_delegate_mode_change_events_with_runtime(
        snapshot,
        location,
        event_bytes,
        expected_owner,
        expected_delegate,
        expected_mode,
    )
}

fn verify_finalized_delegate_mode_change_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    location: DeepXFinalizedTransactionLocation,
    event_bytes: &[u8],
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_mode: DeepXDelegateMode,
) -> Result<DeepXFinalizedDelegateModeChange, DeepXDelegateModeVerificationError> {
    sdk_verify_delegate_mode_change(
        snapshot.interfaces(),
        snapshot.metadata(),
        DeepXFinalizedEventLocation {
            block_hash: location.block_hash(),
            block_number: location.block_number(),
            extrinsic_index: location.extrinsic_index(),
        },
        event_bytes,
        DeepXDelegateModeExpected {
            owner: expected_owner,
            delegate: expected_delegate,
            mode: expected_mode,
        },
    )
}

/// Failures observing a delegate change at an exact finalized location.
#[derive(Debug, Error)]
pub enum DeepXFinalizedDelegateObservationError {
    /// Watch endpoint capabilities are absent or belong to a different endpoint.
    #[error("DeepX Watch RPC capabilities do not authorize delegate reads")]
    CapabilitiesMismatch,
    /// The supplied location is above the observed finalized head.
    #[error("DeepX delegate block {requested} is above finalized block {finalized}")]
    NotFinalized { requested: u64, finalized: u64 },
    /// The historical runtime request or validation failed.
    #[error("failed to observe DeepX historical delegate runtime: {0}")]
    HistoricalRuntime(#[from] DeepXHistoricalRuntimeError),
    /// Canonical block or event-storage observation failed.
    #[error("failed to observe DeepX delegate block: {0}")]
    Watch(#[from] DeepXTransactionWatchError),
    /// Historical metadata or runtime version is not fixture-approved.
    #[error("DeepX historical delegate runtime is not approved: {0}")]
    Runtime(#[from] SnapshotError),
    /// Historical metadata is not a complete prefixed SCALE value.
    #[error("invalid DeepX historical delegate metadata")]
    InvalidMetadata,
    /// The signed call used a different approved runtime than the event block.
    #[error("DeepX delegate signing runtime differs from the historical block runtime")]
    RuntimeMismatch,
    /// Event storage changed between reads at the same block hash.
    #[error("DeepX delegate event storage changed at a fixed block hash")]
    EventStorageChanged,
    /// Canonical block identity or target membership changed during observation.
    #[error("DeepX finalized delegate location changed during observation")]
    LocationChanged,
    /// Complete events do not prove the expected successful change.
    #[error(transparent)]
    Verification(#[from] DeepXDelegateModeVerificationError),
    /// Complete events do not prove the expected account setup or removal.
    #[error(transparent)]
    AccountVerification(#[from] DeepXDelegateAccountVerificationError),
}

/// Observes one delegate mode change from the exact finalized Watch block.
///
/// Historical metadata must match the configured testnet genesis. The target extrinsic membership
/// and complete event storage are each checked twice. The caller must independently prove that the
/// target signed extrinsic encodes the expected wallet call; this does not submit or activate it.
///
/// # Errors
///
/// Returns an error for non-finality, invalid runtime, changed block or event evidence, or
/// failed, missing, duplicate, or conflicting mode-change evidence.
pub async fn observe_finalized_delegate_mode_change_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    location: DeepXFinalizedTransactionLocation,
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_mode: DeepXDelegateMode,
) -> Result<DeepXFinalizedDelegateModeChange, DeepXFinalizedDelegateObservationError> {
    observe_finalized_delegate_mode_change_at_inner(
        environment,
        endpoints,
        capabilities,
        location,
        ExpectedDelegateModeChange {
            owner: expected_owner,
            delegate: expected_delegate,
            mode: expected_mode,
        },
        None,
    )
    .await
}

#[derive(Clone, Copy)]
struct ExpectedDelegateModeChange {
    owner: [u8; 20],
    delegate: [u8; 20],
    mode: DeepXDelegateMode,
}

async fn observe_finalized_delegate_mode_change_at_inner(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    location: DeepXFinalizedTransactionLocation,
    expected: ExpectedDelegateModeChange,
    expected_runtime: Option<&ApprovedRuntimeIdentity>,
) -> Result<DeepXFinalizedDelegateModeChange, DeepXFinalizedDelegateObservationError> {
    observe_finalized_delegate_change_at_inner(
        environment,
        endpoints,
        capabilities,
        location,
        expected_runtime,
        |snapshot, events| {
            verify_finalized_delegate_mode_change_events_with_runtime(
                snapshot,
                location,
                events,
                expected.owner,
                expected.delegate,
                expected.mode,
            )
            .map_err(DeepXFinalizedDelegateObservationError::Verification)
        },
    )
    .await
}

/// Observes delegate setup or removal from the exact finalized Watch block.
///
/// The runtime must be fixture-approved, and target membership and complete event storage are
/// checked twice. The caller separately binds the located signed call, including setup label and
/// expiry, which are absent from the runtime event. This does not enable management submission.
///
/// # Errors
///
/// Returns an error for non-finality, invalid runtime, changed block or event evidence, or
/// missing, duplicate, failed, or conflicting account-change evidence.
pub async fn observe_finalized_delegate_account_change_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    location: DeepXFinalizedTransactionLocation,
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_kind: DeepXDelegateAccountChangeKind,
) -> Result<DeepXFinalizedDelegateAccountChange, DeepXFinalizedDelegateObservationError> {
    observe_finalized_delegate_change_at_inner(
        environment,
        endpoints,
        capabilities,
        location,
        None,
        |snapshot, events| {
            verify_finalized_delegate_account_change_events_with_runtime(
                snapshot,
                location,
                events,
                expected_owner,
                expected_delegate,
                expected_kind,
            )
            .map_err(DeepXFinalizedDelegateObservationError::AccountVerification)
        },
    )
    .await
}

/// Exact offline wallet delegate call to locate and verify without submitting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeepXSignedDelegateAccountOperation {
    /// Delegate creation or renewal, including the signed label and expiry.
    Set(DeepXSetDelegateAccountParams),
    /// Delegate removal.
    Remove(DeepXRemoveDelegateAccountParams),
}

/// Exact signing terms and requested height for one delegate account observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXSignedDelegateAccountQuery {
    /// Typed runtime call to regenerate exactly.
    pub operation: DeepXSignedDelegateAccountOperation,
    /// Explicit account nonce encoded in the signed extrinsic.
    pub nonce: u64,
    /// Block height at which the exact signed extrinsic is expected.
    pub block_number: u64,
}

/// Result of observing one exact offline-signed delegate account call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeepXSignedDelegateAccountObservation {
    /// The Watch finalized head has not reached the requested block.
    Pending(DeepXFinalizedRecoveryCheckpoint),
    /// The exact signed call and matching event succeeded in the finalized block.
    Finalized(DeepXFinalizedDelegateAccountChange),
}

/// Failures binding an offline-signed delegate account call to finalized chain evidence.
#[derive(Debug, Error)]
pub enum DeepXSignedDelegateAccountObservationError {
    /// The permitted snapshot or signing key cannot produce the expected call.
    #[error(transparent)]
    Signing(#[from] SigningError),
    /// The exact signed extrinsic could not be located at the requested height.
    #[error(transparent)]
    Watch(#[from] DeepXTransactionWatchError),
    /// The located call lacks approved, matching runtime event evidence.
    #[error(transparent)]
    Events(#[from] DeepXFinalizedDelegateObservationError),
}

/// Observes one caller-owned, offline-signed delegate setup or removal without submitting it.
///
/// The exact signed bytes are regenerated from the retained permit, key, operation, and nonce.
/// Their hash must be present at the requested finalized height, and an approved historical
/// runtime must emit the matching account event and successful dispatch. The setup event does not
/// echo the signed label or expiry, and historical success does not prove current permissions.
///
/// # Errors
///
/// Returns an error if signing, finalized membership, historical runtime, or event evidence fails.
pub async fn observe_signed_delegate_account_change_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    query: &DeepXSignedDelegateAccountQuery,
) -> Result<DeepXSignedDelegateAccountObservation, DeepXSignedDelegateAccountObservationError> {
    let (signed, delegate, kind) = match &query.operation {
        DeepXSignedDelegateAccountOperation::Set(params) => (
            sign_set_delegate_account(permit, key, params.clone(), query.nonce)?,
            params.delegate,
            DeepXDelegateAccountChangeKind::Set,
        ),
        DeepXSignedDelegateAccountOperation::Remove(params) => (
            sign_remove_delegate_account(permit, key, *params, query.nonce)?,
            params.delegate,
            DeepXDelegateAccountChangeKind::Removed,
        ),
    };
    let location = observe_finalized_transaction_location(
        endpoints,
        capabilities,
        signed.extrinsic_hash(),
        query.block_number,
    )
    .await?;
    let location = match location {
        DeepXFinalizedTransactionLocationObservation::Pending(checkpoint) => {
            return Ok(DeepXSignedDelegateAccountObservation::Pending(checkpoint));
        }
        DeepXFinalizedTransactionLocationObservation::Finalized(location) => location,
    };
    let change = observe_finalized_delegate_change_at_inner(
        environment,
        endpoints,
        capabilities,
        location,
        Some(signed.runtime()),
        |snapshot, events| {
            verify_finalized_delegate_account_change_events_with_runtime(
                snapshot,
                location,
                events,
                signed.signer(),
                delegate,
                kind,
            )
            .map_err(DeepXFinalizedDelegateObservationError::AccountVerification)
        },
    )
    .await?;
    Ok(DeepXSignedDelegateAccountObservation::Finalized(change))
}

async fn observe_finalized_delegate_change_at_inner<T>(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    location: DeepXFinalizedTransactionLocation,
    expected_runtime: Option<&ApprovedRuntimeIdentity>,
    verify: impl FnOnce(
        &DeepXReadOnlyRuntimeSnapshot,
        &[u8],
    ) -> Result<T, DeepXFinalizedDelegateObservationError>,
) -> Result<T, DeepXFinalizedDelegateObservationError> {
    use DeepXFinalizedDelegateObservationError as Error;

    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != watch_url
        || DEEPX_WATCH_RPC_METHODS
            .iter()
            .any(|method| !watch_capabilities.methods().contains(*method))
    {
        return Err(Error::CapabilitiesMismatch);
    }
    let finalized = observe_finalized_checkpoint_at(watch_url).await?;
    if location.block_number() > finalized.block_number() {
        return Err(Error::NotFinalized {
            requested: location.block_number(),
            finalized: finalized.block_number(),
        });
    }
    let observe_location = || async {
        observe_canonical_block_at(
            watch_url,
            location.block_number(),
            location.extrinsic_hash(),
        )
        .await
    };
    let matches_location = |observation: DeepXCanonicalBlockObservation| {
        observation.block_hash() == location.block_hash()
            && observation.extrinsic_index() == Some(location.extrinsic_index())
    };
    if !matches_location(observe_location().await?) {
        return Err(Error::LocationChanged);
    }

    let snapshot = observe_historical_read_only_runtime(
        environment,
        endpoints.genesis_hash(),
        watch_url,
        location.block_hash(),
    )
    .await?;
    if expected_runtime.is_some_and(|runtime| runtime != snapshot.identity()) {
        return Err(Error::RuntimeMismatch);
    }
    let first =
        fetch_system_events(watch_url, location.block_hash(), location.block_number()).await?;
    let second =
        fetch_system_events(watch_url, location.block_hash(), location.block_number()).await?;
    if first != second {
        return Err(Error::EventStorageChanged);
    }
    let change = verify(&snapshot, &first)?;
    if !matches_location(observe_location().await?) {
        return Err(Error::LocationChanged);
    }
    Ok(change)
}

/// Result of observing one exact offline-signed delegate mode call at a requested height.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeepXSignedDelegateModeObservation {
    /// The Watch finalized head has not reached the requested block.
    Pending(DeepXFinalizedRecoveryCheckpoint),
    /// The exact signed call and matching mode change succeeded in the finalized block.
    Finalized(DeepXFinalizedDelegateModeChange),
}

/// Failures binding an offline-signed delegate mode call to finalized chain evidence.
#[derive(Debug, Error)]
pub enum DeepXSignedDelegateModeObservationError {
    /// The caller's snapshot permit or signing key cannot produce the exact expected call.
    #[error(transparent)]
    Signing(#[from] SigningError),
    /// The exact signed extrinsic could not be located at the requested height.
    #[error(transparent)]
    Watch(#[from] DeepXTransactionWatchError),
    /// The located call lacks approved, matching runtime event evidence.
    #[error(transparent)]
    Events(#[from] DeepXFinalizedDelegateObservationError),
}

/// Exact offline signing terms and requested chain height for one read-only observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeepXSignedDelegateModeQuery {
    /// Delegate address and writable runtime mode.
    pub params: DeepXUpdateDelegateModeParams,
    /// Explicit account nonce encoded in the signed extrinsic.
    pub nonce: u64,
    /// Block height at which the exact signed extrinsic is expected.
    pub block_number: u64,
}

/// Observes one caller-owned, offline-signed delegate mode update without submitting it.
///
/// The exact signed bytes are regenerated with the supplied permit, key, arguments, and nonce.
/// Their hash must be present in the finalized canonical block at `block_number`; the event and
/// successful dispatch must then match the signer and requested terms. This does not authorize a
/// management transaction or infer current delegate permissions from historical success.
///
/// # Errors
///
/// Returns an error if signing, finalized membership, approved finalized runtime selection, or
/// same-index event evidence fails.
pub async fn observe_signed_delegate_mode_change_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    query: DeepXSignedDelegateModeQuery,
) -> Result<DeepXSignedDelegateModeObservation, DeepXSignedDelegateModeObservationError> {
    let signed = sign_update_delegate_mode(permit, key, query.params, query.nonce)?;
    let location = observe_finalized_transaction_location(
        endpoints,
        capabilities,
        signed.extrinsic_hash(),
        query.block_number,
    )
    .await?;
    let location = match location {
        DeepXFinalizedTransactionLocationObservation::Pending(checkpoint) => {
            return Ok(DeepXSignedDelegateModeObservation::Pending(checkpoint));
        }
        DeepXFinalizedTransactionLocationObservation::Finalized(location) => location,
    };
    let mode = match query.params.new_mode {
        DeepXWritableDelegateMode::PlaceOrCancelOrder => DeepXDelegateMode::PlaceOrCancelOrder,
        DeepXWritableDelegateMode::Disable => DeepXDelegateMode::Disable,
    };
    let change = observe_finalized_delegate_mode_change_at_inner(
        environment,
        endpoints,
        capabilities,
        location,
        ExpectedDelegateModeChange {
            owner: signed.signer(),
            delegate: query.params.address,
            mode,
        },
        Some(signed.runtime()),
    )
    .await?;
    Ok(DeepXSignedDelegateModeObservation::Finalized(change))
}

/// Decodes perpetual trades from one complete finalized `System.Events` value.
///
/// Event index and block identity are retained without assuming how REST trade IDs are formed.
/// The caller must establish canonical finality for the supplied block hash separately.
///
/// # Errors
///
/// Returns an error for an unsupported runtime event, incomplete SCALE value, or invalid trade.
pub fn decode_finalized_perp_trade_events(
    snapshot: &RuntimeSnapshot,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedPerpTrade>, DeepXPerpTradeEventError> {
    decode_finalized_perp_trade_events_with_metadata(
        snapshot.interfaces(),
        snapshot.metadata(),
        block_hash,
        block_number,
        event_bytes,
    )
}

fn decode_finalized_perp_trade_events_with_metadata(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &subxt_core::metadata::Metadata,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedPerpTrade>, DeepXPerpTradeEventError> {
    deepx_rust_sdk::rpc::trade_event_decode::decode_finalized_perp_trade_events(
        interfaces,
        metadata,
        block_hash,
        block_number,
        event_bytes,
    )
}

/// Decodes Spot matches from one complete finalized `System.Events` value.
///
/// The caller independently establishes canonical finality for the supplied block identity.
/// Event indices are global positions within the complete event sequence.
///
/// # Errors
///
/// Returns an error for unsupported metadata, incomplete SCALE bytes, or invalid trade fields.
pub fn decode_finalized_spot_trade_events(
    snapshot: &RuntimeSnapshot,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedSpotTrade>, DeepXSpotTradeEventError> {
    decode_finalized_spot_trade_events_with_metadata(
        snapshot.interfaces(),
        snapshot.metadata(),
        block_hash,
        block_number,
        event_bytes,
    )
}

fn decode_finalized_spot_trade_events_with_metadata(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &subxt_core::metadata::Metadata,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedSpotTrade>, DeepXSpotTradeEventError> {
    deepx_rust_sdk::rpc::trade_event_decode::decode_finalized_spot_trade_events(
        interfaces,
        metadata,
        block_hash,
        block_number,
        event_bytes,
    )
}

/// Failures observing historical perpetual executions at a finalized block.
#[derive(Debug, Error)]
pub enum DeepXFinalizedPerpTradeObservationError {
    /// Watch endpoint capabilities are absent or belong to a different endpoint.
    #[error("DeepX Watch RPC capabilities do not authorize finalized trade reads")]
    CapabilitiesMismatch,
    /// The requested height is above the observed finalized head.
    #[error("DeepX trade block {requested} is above finalized block {finalized}")]
    NotFinalized { requested: u64, finalized: u64 },
    /// The historical runtime request or validation failed.
    #[error("failed to observe DeepX historical trade runtime: {0}")]
    HistoricalRuntime(#[from] DeepXHistoricalRuntimeError),
    /// Canonical block or event-storage observation failed.
    #[error("failed to observe DeepX finalized trade block: {0}")]
    Watch(#[from] DeepXTransactionWatchError),
    /// Historical metadata or runtime version could not be trusted.
    #[error("DeepX historical trade runtime is not approved: {0}")]
    Runtime(#[from] SnapshotError),
    /// Historical metadata is not a complete prefixed SCALE value.
    #[error("invalid DeepX historical trade metadata")]
    InvalidMetadata,
    /// One historical event read disagreed with another at the same block hash.
    #[error("DeepX historical trade event storage changed at a fixed block hash")]
    EventStorageChanged,
    /// The target block hash changed while reading its events.
    #[error("DeepX historical trade block hash changed during observation")]
    CanonicalHashChanged,
    /// Complete events could not be decoded against the approved runtime.
    #[error("DeepX finalized trade event decoding failed: {0}")]
    Events(#[from] DeepXPerpTradeEventError),
}

/// Observes all perpetual trades in one finalized historical block through the Watch endpoint.
///
/// The runtime metadata must match an approved fixture for the selected hash. Complete event
/// storage is read twice, and canonical block identity is rechecked before returning any trade.
/// These checks cannot replace a storage proof from the endpoint.
///
/// # Errors
///
/// Returns an error for unsupported capabilities or runtime, non-finality, inconsistent block or
/// event bytes, malformed storage, or invalid trade events.
pub async fn observe_finalized_perp_trades_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    block_number: u64,
) -> Result<Vec<DeepXFinalizedPerpTrade>, DeepXFinalizedPerpTradeObservationError> {
    use DeepXFinalizedPerpTradeObservationError as Error;

    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != watch_url
        || DEEPX_WATCH_RPC_METHODS
            .iter()
            .any(|method| !watch_capabilities.methods().contains(*method))
    {
        return Err(Error::CapabilitiesMismatch);
    }
    let finalized = observe_finalized_checkpoint_at(watch_url).await?;
    if block_number > finalized.block_number() {
        return Err(Error::NotFinalized {
            requested: block_number,
            finalized: finalized.block_number(),
        });
    }
    let client = DeepXRpcClient::new(watch_url.to_string());
    let read_hash = || async {
        let encoded: Option<String> = client
            .get_block_hash_at_height(block_number)
            .await
            .map_err(|source| DeepXTransactionWatchError::Rpc {
                method: "chain_getBlockHash",
                source,
            })?;
        decode_hash(&encoded.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?)
    };
    let block_hash = read_hash().await?;
    let encoded_hash = format!("0x{}", hex::encode(block_hash));
    let header: Option<RpcBlockHeader> =
        client
            .get_header_optional(&encoded_hash)
            .await
            .map_err(|source| DeepXTransactionWatchError::Rpc {
                method: "chain_getHeader",
                source,
            })?;
    let header = header.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?;
    let received = decode_block_number(&header.number)?;
    if received != block_number {
        return Err(DeepXTransactionWatchError::BlockNumberMismatch {
            requested: block_number,
            received,
        }
        .into());
    }
    let snapshot = observe_historical_read_only_runtime(
        environment,
        endpoints.genesis_hash(),
        watch_url,
        block_hash,
    )
    .await?;
    let first = fetch_system_events(watch_url, block_hash, block_number).await?;
    let second = fetch_system_events(watch_url, block_hash, block_number).await?;
    if first != second {
        return Err(Error::EventStorageChanged);
    }
    let trades = decode_finalized_perp_trade_events_with_metadata(
        snapshot.interfaces(),
        snapshot.metadata(),
        block_hash,
        block_number,
        &first,
    )?;
    if read_hash().await? != block_hash {
        return Err(Error::CanonicalHashChanged);
    }
    Ok(trades)
}

/// Failures observing Spot matches at one finalized historical block.
#[derive(Debug, Error)]
pub enum DeepXFinalizedSpotTradeObservationError {
    /// Watch endpoint capabilities are absent or belong to a different endpoint.
    #[error("DeepX Watch RPC capabilities do not authorize finalized Spot trade reads")]
    CapabilitiesMismatch,
    /// The requested height is above the observed finalized head.
    #[error("DeepX Spot trade block {requested} is above finalized block {finalized}")]
    NotFinalized { requested: u64, finalized: u64 },
    /// The historical runtime request or validation failed.
    #[error("failed to observe DeepX historical Spot trade runtime: {0}")]
    HistoricalRuntime(#[from] DeepXHistoricalRuntimeError),
    /// Canonical block or event storage observation failed.
    #[error("failed to observe DeepX finalized Spot trade block: {0}")]
    Watch(#[from] DeepXTransactionWatchError),
    /// Historical metadata or runtime version is not fixture-approved.
    #[error("DeepX historical Spot trade runtime is not approved: {0}")]
    Runtime(#[from] SnapshotError),
    /// Historical metadata was not a complete prefixed SCALE value.
    #[error("invalid DeepX historical Spot trade metadata")]
    InvalidMetadata,
    /// Repeated event storage reads at one hash disagreed.
    #[error("DeepX historical Spot trade event storage changed at a fixed block hash")]
    EventStorageChanged,
    /// The canonical block hash changed during the read.
    #[error("DeepX historical Spot trade block hash changed during observation")]
    CanonicalHashChanged,
    /// Complete events could not be decoded under approved metadata.
    #[error("DeepX finalized Spot trade event decoding failed: {0}")]
    Events(#[from] DeepXSpotTradeEventError),
}

/// Observes all Spot matches in one finalized historical block through the Watch endpoint.
///
/// Runtime identity must match an approved fixture at the block hash. Complete event storage is
/// read twice and canonical identity is rechecked before any trade is returned. These observations
/// do not replace a cryptographic storage proof.
///
/// # Errors
///
/// Returns an error for unsupported capabilities or runtime, non-finality, inconsistent block or
/// event storage, malformed bytes, or invalid Spot trade fields.
pub async fn observe_finalized_spot_trades_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    block_number: u64,
) -> Result<Vec<DeepXFinalizedSpotTrade>, DeepXFinalizedSpotTradeObservationError> {
    use DeepXFinalizedSpotTradeObservationError as Error;

    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != watch_url
        || DEEPX_WATCH_RPC_METHODS
            .iter()
            .any(|method| !watch_capabilities.methods().contains(*method))
    {
        return Err(Error::CapabilitiesMismatch);
    }
    let finalized = observe_finalized_checkpoint_at(watch_url).await?;
    if block_number > finalized.block_number() {
        return Err(Error::NotFinalized {
            requested: block_number,
            finalized: finalized.block_number(),
        });
    }
    let client = DeepXRpcClient::new(watch_url.to_string());
    let read_hash = || async {
        let encoded: Option<String> = client
            .get_block_hash_at_height(block_number)
            .await
            .map_err(|source| DeepXTransactionWatchError::Rpc {
                method: "chain_getBlockHash",
                source,
            })?;
        decode_hash(&encoded.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?)
    };
    let block_hash = read_hash().await?;
    let encoded_hash = format!("0x{}", hex::encode(block_hash));
    let header: Option<RpcBlockHeader> =
        client
            .get_header_optional(&encoded_hash)
            .await
            .map_err(|source| DeepXTransactionWatchError::Rpc {
                method: "chain_getHeader",
                source,
            })?;
    let header = header.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?;
    let received = decode_block_number(&header.number)?;
    if received != block_number {
        return Err(DeepXTransactionWatchError::BlockNumberMismatch {
            requested: block_number,
            received,
        }
        .into());
    }
    let snapshot = observe_historical_read_only_runtime(
        environment,
        endpoints.genesis_hash(),
        watch_url,
        block_hash,
    )
    .await?;
    let first = fetch_system_events(watch_url, block_hash, block_number).await?;
    let second = fetch_system_events(watch_url, block_hash, block_number).await?;
    if first != second {
        return Err(Error::EventStorageChanged);
    }
    let trades = decode_finalized_spot_trade_events_with_metadata(
        snapshot.interfaces(),
        snapshot.metadata(),
        block_hash,
        block_number,
        &first,
    )?;
    if read_hash().await? != block_hash {
        return Err(Error::CanonicalHashChanged);
    }
    Ok(trades)
}

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
    verify_perp_place_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_perp_place_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpPlaceEventVerificationError> {
    use DeepXPerpPlaceEventVerificationError as Error;
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
    let super::DeepXNonceReservation::TimestampOrderId { value: order_id } = identity.nonce()
    else {
        return Err(Error::UnsupportedOperation);
    };
    let observed = decode_perp_place_inclusion(
        snapshot.interfaces(),
        snapshot.metadata(),
        DeepXPerpPlaceExpected {
            params: crate::signing::DeepXPerpPlaceParams {
                subaccount: *subaccount,
                market_id: *market_id,
                is_long: *is_long,
                size: *size,
                price: *price,
                order_type: *order_type,
                take_profit: *take_profit,
                stop_loss: *stop_loss,
                reduce_only: *reduce_only,
                post_only: *post_only,
            },
            order_id,
            block_number,
            extrinsic_index,
        },
        event_bytes,
    )
    .map_err(|e| match e {
        DeepXPerpPlaceEventError::RuntimeInterface(e) => Error::RuntimeInterface(e),
        DeepXPerpPlaceEventError::Decode(e) => Error::Decode(e),
        DeepXPerpPlaceEventError::MalformedEventBytes => Error::MalformedEventBytes,
        DeepXPerpPlaceEventError::ConflictingEvent => Error::ConflictingEvent,
        DeepXPerpPlaceEventError::DuplicateEvent => Error::DuplicateEvent,
        DeepXPerpPlaceEventError::MissingBusinessEvent => {
            Error::InclusionEvidence(DeepXInclusionEvidenceError::MissingBusinessEvent)
        }
        DeepXPerpPlaceEventError::ConflictingBusinessEvent => {
            Error::InclusionEvidence(DeepXInclusionEvidenceError::ConflictingBusinessEvent)
        }
    })?;
    Ok(DeepXInclusionEvidence {
        block_hash,
        block_number,
        extrinsic_index: observed.extrinsic_index,
        outcome: match observed.outcome {
            DeepXPerpPlaceInclusionOutcome::Success => super::DeepXInclusionOutcome::Success,
            DeepXPerpPlaceInclusionOutcome::Failed => super::DeepXInclusionOutcome::Failed,
        },
    })
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
    verify_perp_profit_and_loss_point_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_perp_profit_and_loss_point_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
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
    let observed = decode_perp_point_update_inclusion(
        snapshot.interfaces(),
        snapshot.metadata(),
        DeepXPerpPointUpdateExpected {
            params: crate::signing::DeepXPerpProfitAndLossPointParams {
                subaccount: *subaccount,
                market_id: *market_id,
                take_profit_point: *take_profit_point,
                stop_loss_point: *stop_loss_point,
            },
            extrinsic_index,
        },
        event_bytes,
    )
    .map_err(map_perp_position_event_error)
    .map_err(DeepXPerpProfitAndLossPointEventVerificationError::IndexedEvents)?;
    Ok(perp_position_inclusion_evidence(
        block_hash,
        block_number,
        observed,
    ))
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

fn perp_position_inclusion_evidence(
    block_hash: [u8; 32],
    block_number: u64,
    observed: deepx_rust_sdk::rpc::perp_position::DeepXPerpPositionInclusionEvidence,
) -> DeepXInclusionEvidence {
    DeepXInclusionEvidence {
        block_hash,
        block_number,
        extrinsic_index: observed.extrinsic_index,
        outcome: match observed.outcome {
            DeepXPerpPositionInclusionOutcome::Success => super::DeepXInclusionOutcome::Success,
            DeepXPerpPositionInclusionOutcome::Failed => super::DeepXInclusionOutcome::Failed,
        },
    }
}

fn map_perp_position_event_error(
    error: DeepXPerpPositionEventError,
) -> DeepXPerpPlaceEventVerificationError {
    use DeepXPerpPlaceEventVerificationError as Error;
    match error {
        DeepXPerpPositionEventError::RuntimeInterface(e) => Error::RuntimeInterface(e),
        DeepXPerpPositionEventError::Decode(e) => Error::Decode(e),
        DeepXPerpPositionEventError::MalformedEventBytes => Error::MalformedEventBytes,
        DeepXPerpPositionEventError::ConflictingEvent => Error::ConflictingEvent,
        DeepXPerpPositionEventError::DuplicateEvent => Error::DuplicateEvent,
        DeepXPerpPositionEventError::MissingBusinessEvent => {
            Error::InclusionEvidence(DeepXInclusionEvidenceError::MissingBusinessEvent)
        }
        DeepXPerpPositionEventError::ConflictingBusinessEvent => {
            Error::InclusionEvidence(DeepXInclusionEvidenceError::ConflictingBusinessEvent)
        }
    }
}

/// Verifies the close-generated order without equating its account-sequence ID to the nonce.
///
/// This binds creation of a reduce-only order, not execution or complete position closure.
/// Position-derived direction and size are not inferred from the durable close call. The generated
/// order must still have positive size and leverage, with no initial fill.
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
    verify_perp_close_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_perp_close_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
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
    let observed = decode_perp_close_inclusion(
        snapshot.interfaces(),
        snapshot.metadata(),
        DeepXPerpCloseExpected {
            params: crate::signing::DeepXPerpCloseParams {
                subaccount: *subaccount,
                market_id: *market_id,
                price: *price,
                slippage: *slippage,
            },
            block_number,
            extrinsic_index,
        },
        event_bytes,
    )
    .map_err(map_perp_position_event_error)
    .map_err(DeepXPerpCloseEventVerificationError::IndexedEvents)?;
    Ok(perp_position_inclusion_evidence(
        block_hash,
        block_number,
        observed,
    ))
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
    Decode(#[source] Box<subxt_core::Error>),
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
    verify_spot_place_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_spot_place_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
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
    let observed = decode_spot_place_inclusion(
        snapshot.interfaces(),
        snapshot.metadata(),
        DeepXSpotPlaceExpected {
            params: DeepXSpotPlaceParams {
                subaccount: *subaccount,
                pair: *pair,
                is_buy: *is_buy,
                quote_amount: *quote_amount,
                base_amount: *base_amount,
                order_type: *order_type,
                post_only: *post_only,
                reduce_only: *reduce_only,
            },
            order_id,
            extrinsic_index,
        },
        event_bytes,
    )
    .map_err(|e| match e {
        DeepXSpotPlaceEventError::RuntimeInterface(e) => Error::RuntimeInterface(e),
        DeepXSpotPlaceEventError::Decode(e) => Error::Decode(e),
        DeepXSpotPlaceEventError::MalformedEventBytes => Error::MalformedEventBytes,
        DeepXSpotPlaceEventError::ConflictingEvent => Error::ConflictingEvent,
        DeepXSpotPlaceEventError::DuplicateEvent => Error::DuplicateEvent,
        DeepXSpotPlaceEventError::MissingBusinessEvent => {
            Error::InclusionEvidence(DeepXInclusionEvidenceError::MissingBusinessEvent)
        }
        DeepXSpotPlaceEventError::ConflictingBusinessEvent => {
            Error::InclusionEvidence(DeepXInclusionEvidenceError::ConflictingBusinessEvent)
        }
    })?;
    Ok(DeepXInclusionEvidence {
        block_hash,
        block_number,
        extrinsic_index: observed.extrinsic_index,
        outcome: match observed.outcome {
            DeepXSpotPlaceInclusionOutcome::Success => super::DeepXInclusionOutcome::Success,
            DeepXSpotPlaceInclusionOutcome::Failed => super::DeepXInclusionOutcome::Failed,
        },
    })
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
    Decode(#[source] Box<subxt_core::Error>),
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
    verify_spot_cancel_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_spot_cancel_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
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
    let observed = decode_spot_cancel_inclusion(
        snapshot.interfaces(),
        snapshot.metadata(),
        DeepXSpotCancelExpected {
            subaccount: *subaccount,
            pair: *pair,
            order_id: *order_id,
            is_buy: *is_buy,
            extrinsic_index,
        },
        event_bytes,
    )
    .map_err(|e| match e {
        DeepXSpotCancelEventError::RuntimeInterface(e) => Error::RuntimeInterface(e),
        DeepXSpotCancelEventError::Decode(e) => Error::Decode(e),
        DeepXSpotCancelEventError::MalformedEventBytes => Error::MalformedEventBytes,
        DeepXSpotCancelEventError::ConflictingEvent => Error::ConflictingEvent,
        DeepXSpotCancelEventError::DuplicateEvent => Error::DuplicateEvent,
        DeepXSpotCancelEventError::MissingBusinessEvent => {
            Error::InclusionEvidence(DeepXInclusionEvidenceError::MissingBusinessEvent)
        }
        DeepXSpotCancelEventError::ConflictingBusinessEvent => {
            Error::InclusionEvidence(DeepXInclusionEvidenceError::ConflictingBusinessEvent)
        }
    })?;
    Ok(DeepXInclusionEvidence {
        block_hash,
        block_number,
        extrinsic_index: observed.extrinsic_index,
        outcome: match observed.outcome {
            DeepXSpotCancelInclusionOutcome::Success => super::DeepXInclusionOutcome::Success,
            DeepXSpotCancelInclusionOutcome::Failed => super::DeepXInclusionOutcome::Failed,
        },
    })
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
    Decode(#[source] Box<subxt_core::Error>),
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

/// Failures while verifying both legs of a perpetual modify at one extrinsic index.
#[derive(Debug, Error)]
pub enum DeepXPerpModifyEventVerificationError {
    /// The base identity, modify scope, or cancel mode cannot be verified.
    #[error("unsupported DeepX perpetual modify event identity")]
    UnsupportedOperation,
    /// The existing order cancellation event does not match the modify request.
    #[error(transparent)]
    Cancel(#[from] DeepXPerpCancelEventVerificationError),
    /// The replacement order placement event does not match the modify request.
    #[error(transparent)]
    Place(#[from] DeepXPerpPlaceEventVerificationError),
    /// Complete runtime events could not be decoded to verify leg ordering.
    #[error("DeepX perpetual modify event ordering could not be verified")]
    MalformedEventBytes,
    /// The cancel and replacement legs produced conflicting inclusion evidence.
    #[error("DeepX perpetual modify legs produced conflicting inclusion evidence")]
    ConflictingEvidence,
}

/// Verifies exact perpetual cancellation and replacement placement at the same extrinsic index.
///
/// The replacement order ID is the timestamp nonce. The identity must contain the exact modify
/// operation, reserved signer, instrument, side, nonce and runtime scope.
/// Both complete `System.Events` decodes must independently prove dispatch and the expected
/// business event. This verifies event evidence only; the signed call still requires canonical
/// `Subaccount.modify_orders` binding before it can be used for durable recovery.
///
/// # Errors
///
/// Returns an error for unsupported identity/scope, malformed event bytes, conflicting leg
/// evidence, or a successful dispatch missing either expected business event.
pub fn verify_perp_modify_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    params: DeepXPerpModifyParams,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpModifyEventVerificationError> {
    verify_perp_modify_inclusion_events_with_runtime(
        snapshot,
        identity,
        params,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_perp_modify_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    identity: &DeepXTransactionIdentity,
    params: DeepXPerpModifyParams,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpModifyEventVerificationError> {
    use DeepXPerpModifyEventVerificationError as Error;

    if !matches!(
        identity.nonce(),
        super::DeepXNonceReservation::TimestampOrderId { .. }
    ) {
        return Err(Error::UnsupportedOperation);
    }
    let expected_side = if params.place.is_long {
        nautilus_model::enums::OrderSide::Buy
    } else {
        nautilus_model::enums::OrderSide::Sell
    };
    let Some(DeepXTransactionOperation::PerpModify {
        subaccount,
        place_subaccount,
        cancel_order_id,
        market_id,
        place_market_id,
        fast_cancel,
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
        return Err(Error::UnsupportedOperation);
    };
    if *subaccount != params.cancel.subaccount
        || *place_subaccount != params.place.subaccount
        || *cancel_order_id != params.cancel.order_id
        || *market_id != params.cancel.market_id
        || *place_market_id != params.place.market_id
        || *fast_cancel != params.cancel.fast_cancel
        || *is_long != params.place.is_long
        || *size != params.place.size
        || *price != params.place.price
        || *order_type != params.place.order_type
        || *take_profit != params.place.take_profit
        || *stop_loss != params.place.stop_loss
        || *reduce_only != params.place.reduce_only
        || *post_only != params.place.post_only
        || identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || identity.order_side() != expected_side
        || params.cancel.fast_cancel
        || params.cancel.subaccount != params.place.subaccount
        || params.cancel.market_id != params.place.market_id
    {
        return Err(Error::UnsupportedOperation);
    }

    let client_order_id =
        nautilus_model::identifiers::ClientOrderId::new(identity.client_order_id());
    let cancel_identity = DeepXTransactionIdentity::new_perp_cancel(
        client_order_id,
        identity.signer(),
        identity.instrument_id(),
        expected_side,
        identity.nonce(),
        identity.runtime().clone(),
        params.cancel,
    );
    let cancel_evidence = verify_perp_cancel_inclusion_events_with_runtime(
        snapshot,
        &cancel_identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )?;
    let mut place_params = params.place;
    if cancel_evidence.outcome() == super::DeepXInclusionOutcome::Success {
        if !perp_modify_sizing_event_supported(snapshot.metadata()) {
            return Err(Error::UnsupportedOperation);
        }
        place_params.size = verify_perp_modify_sizing_event(
            snapshot.metadata(),
            identity,
            extrinsic_index,
            event_bytes,
        )?;
    }
    let place_identity = DeepXTransactionIdentity::new_perp_place(
        client_order_id,
        identity.signer(),
        identity.instrument_id(),
        expected_side,
        identity.nonce(),
        identity.runtime().clone(),
        place_params,
    );
    let place_evidence = verify_perp_place_inclusion_events_with_runtime(
        snapshot,
        &place_identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )?;
    if cancel_evidence != place_evidence {
        return Err(Error::ConflictingEvidence);
    }
    if cancel_evidence.outcome() == super::DeepXInclusionOutcome::Success
        && !perp_modify_events_are_ordered(snapshot, extrinsic_index, event_bytes)?
    {
        return Err(Error::ConflictingEvidence);
    }
    Ok(cancel_evidence)
}

pub(crate) use deepx_rust_sdk::rpc::perp_modify::perp_modify_sizing_event_supported;

fn verify_perp_modify_sizing_event(
    metadata: &subxt_core::metadata::Metadata,
    identity: &DeepXTransactionIdentity,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<u128, DeepXPerpModifyEventVerificationError> {
    use DeepXPerpModifyEventVerificationError as Error;

    let Some(DeepXTransactionOperation::PerpModify {
        subaccount,
        cancel_order_id,
        size: target_size,
        ..
    }) = identity.operation()
    else {
        return Err(Error::UnsupportedOperation);
    };
    let super::DeepXNonceReservation::TimestampOrderId {
        value: expected_new_order_id,
    } = identity.nonce()
    else {
        return Err(Error::UnsupportedOperation);
    };
    decode_perp_modify_sizing_event(
        metadata,
        DeepXPerpModifySizingExpected {
            subaccount: *subaccount,
            old_order_id: *cancel_order_id,
            target_size: *target_size,
            new_order_id: expected_new_order_id,
            extrinsic_index,
        },
        event_bytes,
    )
    .map(|evidence| evidence.replacement_size)
    .map_err(|e| match e {
        DeepXPerpModifyEventError::UnsupportedRuntime => Error::UnsupportedOperation,
        DeepXPerpModifyEventError::MalformedEventBytes => Error::MalformedEventBytes,
        DeepXPerpModifyEventError::ConflictingEvidence => Error::ConflictingEvidence,
    })
}

fn perp_modify_events_are_ordered(
    snapshot: &impl RuntimeMetadataView,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<bool, DeepXPerpModifyEventVerificationError> {
    decode_perp_modify_event_order(snapshot.metadata(), extrinsic_index, event_bytes)
        .map(|order| order == DeepXPerpModifyEventOrder::Ordered)
        .map_err(|_| DeepXPerpModifyEventVerificationError::MalformedEventBytes)
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
    verify_perp_cancel_business_event_with_runtime(snapshot, identity, extrinsic_index, event_bytes)
}

fn verify_perp_cancel_business_event_with_runtime(
    snapshot: &impl RuntimeMetadataView,
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
    let observed = decode_perp_cancel_business_event(
        snapshot.interfaces(),
        snapshot.metadata(),
        DeepXPerpCancelExpected {
            subaccount: *subaccount,
            order_id: *order_id,
            extrinsic_index,
        },
        event_bytes,
    )
    .map_err(map_perp_cancel_business_error)?;
    Ok(DeepXIndexedOutcome {
        extrinsic_index: observed.extrinsic_index,
        outcome: match observed.outcome {
            DeepXPerpCancelBusinessOutcome::Success => DeepXBusinessEventOutcome::Success,
            DeepXPerpCancelBusinessOutcome::NotObserved => DeepXBusinessEventOutcome::NotObserved,
        },
    })
}

fn map_perp_cancel_business_error(
    e: DeepXPerpCancelEventError,
) -> DeepXPerpCancelEventVerificationError {
    match e {
        DeepXPerpCancelEventError::RuntimeInterface(e) => e.into(),
        DeepXPerpCancelEventError::Decode(e) => DeepXPerpCancelEventVerificationError::Decode(e),
        DeepXPerpCancelEventError::MalformedEventBytes => {
            DeepXPerpCancelEventVerificationError::MalformedEventBytes
        }
        DeepXPerpCancelEventError::ConflictingEvent => {
            DeepXPerpCancelEventVerificationError::ConflictingEvent
        }
        DeepXPerpCancelEventError::DuplicateEvent => {
            DeepXPerpCancelEventVerificationError::DuplicateEvent
        }
    }
}

fn verify_perp_cancel_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpCancelEventVerificationError> {
    verify_perp_cancel_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_perp_cancel_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpCancelEventVerificationError> {
    let Some(DeepXTransactionOperation::PerpCancel {
        subaccount,
        order_id,
        fast_cancel,
        ..
    }) = identity.operation()
    else {
        return Err(DeepXPerpCancelEventVerificationError::UnsupportedOperation);
    };
    let observed = decode_perp_cancel_inclusion(
        snapshot.interfaces(),
        snapshot.metadata(),
        DeepXPerpCancelExpected {
            subaccount: *subaccount,
            order_id: *order_id,
            extrinsic_index,
        },
        *fast_cancel,
        event_bytes,
    )
    .map_err(|e| match e {
        DeepXPerpCancelInclusionError::RuntimeInterface(e) => e.into(),
        DeepXPerpCancelInclusionError::Dispatch(e) => match e {
            DeepXIndexedSystemDispatchError::Decode(e) => {
                DeepXPerpCancelEventVerificationError::Decode(e)
            }
            DeepXIndexedSystemDispatchError::MalformedEventBytes
            | DeepXIndexedSystemDispatchError::MissingEvent => {
                DeepXPerpCancelEventVerificationError::MalformedEventBytes
            }
            DeepXIndexedSystemDispatchError::DuplicateEvent => {
                DeepXPerpCancelEventVerificationError::DuplicateEvent
            }
        },
        DeepXPerpCancelInclusionError::BusinessEvent(e) => map_perp_cancel_business_error(e),
        DeepXPerpCancelInclusionError::MissingBusinessEvent => {
            DeepXInclusionEvidenceError::MissingBusinessEvent.into()
        }
        DeepXPerpCancelInclusionError::ConflictingBusinessEvent => {
            DeepXInclusionEvidenceError::ConflictingBusinessEvent.into()
        }
    })?;
    Ok(DeepXInclusionEvidence {
        block_hash,
        block_number,
        extrinsic_index: observed.extrinsic_index,
        outcome: match observed.outcome {
            DeepXPerpCancelInclusionOutcome::Success => super::DeepXInclusionOutcome::Success,
            DeepXPerpCancelInclusionOutcome::Failed => super::DeepXInclusionOutcome::Failed,
        },
    })
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
    /// A finalized canonical block did not contain the exact target extrinsic.
    #[error("DeepX finalized block {0} does not contain the target extrinsic")]
    FinalizedExtrinsicUnavailable(u64),
    /// The exact finalized block did not expose complete system event storage.
    #[error("DeepX system event storage was unavailable at finalized block {0}")]
    EventStorageUnavailable(u64),
    /// The exact finalized block returned malformed system event storage.
    #[error("DeepX system event storage at finalized block {0} was malformed")]
    InvalidEventStorage(u64),
    /// The exact finalized extrinsic had no unique System dispatch outcome.
    #[error(
        "DeepX finalized extrinsic at block {block_number} index {extrinsic_index} has no unique System dispatch outcome"
    )]
    MissingSystemDispatch {
        /// Finalized block containing the exact extrinsic.
        block_number: u64,
        /// Index of the exact extrinsic in that block.
        extrinsic_index: u32,
    },
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
    /// Both perpetual modify legs could not be verified at the exact index.
    #[error(transparent)]
    PerpModifyEventVerification(#[from] DeepXPerpModifyEventVerificationError),
    /// A bounded recovery scan could not be planned safely.
    #[error(transparent)]
    ScanPlan(#[from] DeepXRecoveryScanPlanError),
    /// Collected canonical evidence did not match the bounded scan plan.
    #[error(transparent)]
    ScanCollection(#[from] DeepXRecoveryScanCollectionError),
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

/// Exact location of one extrinsic in a finalized canonical block.
///
/// This proves transaction membership and finality, but not dispatch or business outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedTransactionLocation {
    extrinsic_hash: [u8; 32],
    block_number: u64,
    block_hash: [u8; 32],
    extrinsic_index: u32,
}

impl DeepXFinalizedTransactionLocation {
    /// Returns the exact extrinsic hash located in the finalized block.
    #[must_use]
    pub const fn extrinsic_hash(&self) -> [u8; 32] {
        self.extrinsic_hash
    }

    /// Returns the finalized block number containing the extrinsic.
    #[must_use]
    pub const fn block_number(&self) -> u64 {
        self.block_number
    }

    /// Returns the finalized canonical block hash.
    #[must_use]
    pub const fn block_hash(&self) -> [u8; 32] {
        self.block_hash
    }

    /// Returns the unique extrinsic index in the finalized block.
    #[must_use]
    pub const fn extrinsic_index(&self) -> u32 {
        self.extrinsic_index
    }
}

/// Finalized-location observation for an exact transaction and reported block height.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXFinalizedTransactionLocationObservation {
    /// The finalized head has not reached the requested block height.
    Pending(DeepXFinalizedRecoveryCheckpoint),
    /// The exact target extrinsic was found uniquely in the finalized canonical block.
    Finalized(DeepXFinalizedTransactionLocation),
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

/// Locates an exact extrinsic at a requested height after that height becomes finalized.
///
/// This read-only boundary verifies the Watch endpoint capabilities, observes its finalized head,
/// and recomputes every extrinsic hash in the canonical block at the requested height. It does not
/// inspect events or infer dispatch or business success.
///
/// # Errors
///
/// Returns an error for mismatched capabilities, malformed chain data, duplicate target
/// extrinsics, or a finalized canonical block which does not contain the exact target.
pub async fn observe_finalized_transaction_location(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    target_extrinsic_hash: [u8; 32],
    block_number: u64,
) -> Result<DeepXFinalizedTransactionLocationObservation, DeepXTransactionWatchError> {
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
    if checkpoint.block_number() < block_number {
        return Ok(DeepXFinalizedTransactionLocationObservation::Pending(
            checkpoint,
        ));
    }

    let observation =
        observe_canonical_block_at(watch_url, block_number, target_extrinsic_hash).await?;
    let extrinsic_index = observation.extrinsic_index().ok_or(
        DeepXTransactionWatchError::FinalizedExtrinsicUnavailable(block_number),
    )?;
    Ok(DeepXFinalizedTransactionLocationObservation::Finalized(
        DeepXFinalizedTransactionLocation {
            extrinsic_hash: target_extrinsic_hash,
            block_number,
            block_hash: observation.block_hash(),
            extrinsic_index,
        },
    ))
}

/// Reads the unique System dispatch outcome of an exact finalized extrinsic.
///
/// The caller supplies a location already verified against a finalized canonical block. Complete
/// event storage is required, including every thread when the runtime shards `System.Events`.
///
/// # Errors
///
/// Returns an error for missing or malformed event storage, malformed event decoding, or a missing
/// or duplicate dispatch outcome at the exact extrinsic index.
pub async fn observe_finalized_system_dispatch(
    endpoints: &DeepXValidatedRpcEndpoints,
    snapshot: &RuntimeSnapshot,
    location: DeepXFinalizedTransactionLocation,
) -> Result<DeepXDispatchOutcome, DeepXTransactionWatchError> {
    let block_number = location.block_number();
    let event_bytes = fetch_system_events(
        endpoints.url_for(DeepXRpcRole::Watch),
        location.block_hash(),
        block_number,
    )
    .await?;
    decode_finalized_system_dispatch(snapshot, location, &event_bytes)
}

fn decode_finalized_system_dispatch(
    snapshot: &RuntimeSnapshot,
    location: DeepXFinalizedTransactionLocation,
    event_bytes: &[u8],
) -> Result<DeepXDispatchOutcome, DeepXTransactionWatchError> {
    let block_number = location.block_number();
    decode_system_dispatch(snapshot.metadata(), location.extrinsic_index(), event_bytes)
        .map(|outcome| match outcome {
            DeepXSystemDispatchOutcome::Success => DeepXDispatchOutcome::Success,
            DeepXSystemDispatchOutcome::Failed => DeepXDispatchOutcome::Failed,
        })
        .map_err(|e| match e {
            DeepXSystemDispatchError::InvalidEventStorage => {
                DeepXTransactionWatchError::InvalidEventStorage(block_number)
            }
            DeepXSystemDispatchError::MissingSystemDispatch => {
                DeepXTransactionWatchError::MissingSystemDispatch {
                    block_number,
                    extrinsic_index: location.extrinsic_index(),
                }
            }
        })
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
    /// Creates a finalized checkpoint to verify against canonical chain evidence.
    #[must_use]
    pub const fn new(block_number: u64, block_hash: [u8; 32]) -> Self {
        Self {
            block_number,
            block_hash,
        }
    }

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
    let client = DeepXRpcClient::new(endpoint_url.to_string());
    let encoded_hash = client.get_finalized_head_once().await.map_err(|source| {
        DeepXTransactionWatchError::Rpc {
            method: "chain_getFinalizedHead",
            source,
        }
    })?;
    let block_hash = decode_hash(&encoded_hash)?;
    let header: Option<RpcBlockHeader> =
        client
            .get_header_optional(&encoded_hash)
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
    let client = DeepXRpcClient::new(endpoint_url.to_string());
    let encoded_hash = client
        .get_block_hash_at_height_with_retry(block_number)
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getBlockHash",
            source,
        })?;
    let encoded_hash =
        encoded_hash.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?;
    let block_hash = decode_hash(&encoded_hash)?;
    let response: Option<RpcBlockResponse> = client
        .get_block_optional_with_retry(&encoded_hash)
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
    last_scanned: DeepXFinalizedRecoveryCheckpoint,
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
        last_scanned.block_number(),
        last_scanned.block_hash(),
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
    last_scanned: DeepXFinalizedRecoveryCheckpoint,
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
        last_scanned.block_number(),
        last_scanned.block_hash(),
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
    PerpModify(
        &'a RuntimeSnapshot,
        &'a DeepXTransactionIdentity,
        DeepXPerpModifyParams,
    ),
}

/// Collects bounded canonical finalized evidence for a perpetual modify operation.
#[expect(
    clippy::too_many_arguments,
    reason = "matches bounded recovery scan inputs"
)]
pub async fn collect_finalized_perp_modify_recovery_scan(
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
            Some(DeepXTransactionOperation::PerpModify { .. })
        )
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(Box::new(
            DeepXPerpModifyEventVerificationError::UnsupportedOperation.into(),
        ));
    }
    let Some(DeepXTransactionOperation::PerpModify {
        subaccount,
        place_subaccount,
        cancel_order_id,
        market_id,
        place_market_id,
        fast_cancel,
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
        return Err(Box::new(
            DeepXPerpModifyEventVerificationError::UnsupportedOperation.into(),
        ));
    };
    let params = DeepXPerpModifyParams {
        cancel: crate::signing::DeepXPerpCancelParams {
            subaccount: *subaccount,
            order_id: *cancel_order_id,
            market_id: *market_id,
            fast_cancel: *fast_cancel,
        },
        place: crate::signing::DeepXPerpPlaceParams {
            subaccount: *place_subaccount,
            market_id: *place_market_id,
            is_long: *is_long,
            size: *size,
            price: *price,
            order_type: *order_type,
            take_profit: *take_profit,
            stop_loss: *stop_loss,
            reduce_only: *reduce_only,
            post_only: *post_only,
        },
    };
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::PerpModify(snapshot, identity, params)),
    )
    .await
    .map_err(Box::new)
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
    last_scanned: DeepXFinalizedRecoveryCheckpoint,
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, DeepXTransactionWatchError> {
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned.block_number(),
        last_scanned.block_hash(),
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

    let client = DeepXRpcClient::new(recovery_url.to_string());
    let encoded_checkpoint_hash = client
        .get_block_hash_at_height_with_retry(last_scanned_block)
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
    let encoded_finalized_hash = deepx_rust_sdk::rpc::get_finalized_head(&client)
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getFinalizedHead",
            source,
        })?;
    let finalized_hash = decode_hash(&encoded_finalized_hash)?;
    let finalized_header: Option<RpcBlockHeader> = client
        .get_header_optional_with_retry(&encoded_finalized_hash)
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
                    CancelEventEvidence::PerpModify(snapshot, identity, params) => {
                        verify_perp_modify_inclusion_events(
                            snapshot,
                            identity,
                            *params,
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
    let client = DeepXRpcClient::new(recovery_url.to_string());
    let block_hash = format!("0x{}", hex::encode(block_hash));
    let encoded = fetch_storage_value(&client, SYSTEM_EVENTS_STORAGE_KEY, &block_hash).await?;
    if let Some(encoded) = encoded {
        return decode_event_storage_value(&encoded).ok_or(
            DeepXTransactionWatchError::InvalidEventStorage(block_number),
        );
    }

    let threads_key = system_threads_storage_key(block_number);
    let encoded_threads = fetch_storage_value(&client, &threads_key, &block_hash)
        .await?
        .ok_or(DeepXTransactionWatchError::EventStorageUnavailable(
            block_number,
        ))?;
    let last_thread = decode_event_threads(&encoded_threads).ok_or(
        DeepXTransactionWatchError::InvalidEventStorage(block_number),
    )?;
    let mut batches = Vec::with_capacity(usize::from(last_thread) + 1);
    for thread in 0..=last_thread {
        let key = system_events_map_storage_key(block_number, thread);
        let encoded = fetch_storage_value(&client, &key, &block_hash)
            .await?
            .ok_or(DeepXTransactionWatchError::EventStorageUnavailable(
                block_number,
            ))?;
        batches.push(decode_event_storage_value(&encoded).ok_or(
            DeepXTransactionWatchError::InvalidEventStorage(block_number),
        )?);
    }
    combine_events_map_batches(&batches).ok_or(DeepXTransactionWatchError::InvalidEventStorage(
        block_number,
    ))
}

async fn fetch_storage_value(
    client: &DeepXRpcClient,
    storage_key: &str,
    block_hash: &str,
) -> Result<Option<String>, DeepXTransactionWatchError> {
    client
        .get_storage(storage_key, block_hash)
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "state_getStorage",
            source,
        })
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
    let client = DeepXRpcClient::new(endpoints.url_for(DeepXRpcRole::Submission).to_string());
    let extrinsics = client
        .get_pending_extrinsics_with_retry()
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
    find_extrinsic_by_hash(encoded_extrinsics, target_hash).map_err(|e| match e {
        DeepXExtrinsicLookupError::InvalidEntry { index } => {
            DeepXTransactionWatchError::InvalidExtrinsic {
                evidence_source: source,
                index,
            }
        }
        DeepXExtrinsicLookupError::DuplicateTarget => {
            DeepXTransactionWatchError::DuplicateExtrinsic {
                evidence_source: source,
            }
        }
    })
}

fn decode_hash(encoded: &str) -> Result<[u8; 32], DeepXTransactionWatchError> {
    decode_prefixed_hash(encoded)
        .ok_or_else(|| DeepXTransactionWatchError::InvalidBlockHash(encoded.to_string()))
}

fn decode_block_number(encoded: &str) -> Result<u64, DeepXTransactionWatchError> {
    decode_block_number_u64(encoded)
        .ok_or_else(|| DeepXTransactionWatchError::InvalidBlockNumber(encoded.to_string()))
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

    use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde_json::Value;
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn historical_runtime_uses_metadata_at_the_requested_block() {
        let block_hash = [0x42; 32];
        let encoded_hash = format!("0x{}", hex::encode(block_hash));
        let metadata: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/deepx-rust-sdk/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let router = Router::new().route(
            "/",
            post({
                let calls = Arc::clone(&calls);
                let encoded_hash = encoded_hash.clone();
                move |Json(request): Json<Value>| {
                    let calls = Arc::clone(&calls);
                    let encoded_hash = encoded_hash.clone();
                    let metadata = metadata.clone();
                    async move {
                        assert_eq!(request["params"], json!([encoded_hash]));
                        calls.fetch_add(1, Ordering::Relaxed);
                        let result = match request["method"].as_str().unwrap() {
                            "state_getRuntimeVersion" => json!({
                                "specVersion": 377,
                                "transactionVersion": 2,
                            }),
                            "state_getMetadata" => metadata["result"].clone(),
                            method => panic!("unexpected historical runtime RPC: {method}"),
                        };
                        Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let genesis_hash =
            hex::decode_array("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap();

        let snapshot = observe_historical_read_only_runtime(
            &DeepXEnvironment::Testnet,
            genesis_hash,
            &format!("http://{address}"),
            block_hash,
        )
        .await
        .unwrap();

        assert_eq!(snapshot.identity().spec_version, 377);
        assert_eq!(snapshot.identity().transaction_version, 2);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn rpc_read_retries_transient_transport_failures() {
        async fn rpc(State(attempts): State<Arc<AtomicUsize>>) -> Result<Json<Value>, StatusCode> {
            if attempts.fetch_add(1, Ordering::Relaxed) < 2 {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            Ok(Json(json!({ "jsonrpc": "2.0", "id": 1, "result": 42 })))
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        tokio::spawn({
            let attempts = Arc::clone(&attempts);
            async move {
                axum::serve(
                    listener,
                    Router::new().route("/", post(rpc)).with_state(attempts),
                )
                .await
                .unwrap();
            }
        });
        let client = DeepXRpcClient::new(format!("http://{address}"));

        let value: u64 = execute_rpc_read_with_retry(
            &client,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "test", "params": [] }),
        )
        .await
        .unwrap();

        assert_eq!(value, 42);
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[rstest]
    fn spec_376_finalized_event_fixture_is_complete_and_read_only_approved() {
        const FIXTURE_ROOT: &str = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/deepx-rust-sdk/test_data/runtime/testnet/finalized-events-19592700-spec376/",
        );
        let metadata_response: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/deepx-rust-sdk/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        let metadata_bytes = hex::decode(
            metadata_response["result"]
                .as_str()
                .unwrap()
                .trim_start_matches("0x"),
        )
        .unwrap();
        let read_only = crate::signing::DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap(),
            376,
            1,
            &metadata_bytes,
        )
        .unwrap();
        let manifest: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/deepx-rust-sdk/test_data/runtime/testnet/finalized-events-19592700-spec376/manifest.json",
        )))
        .unwrap();
        let initial: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/deepx-rust-sdk/test_data/runtime/testnet/finalized-events-19592700-spec376/system_events.json",
        )))
        .unwrap();
        let repeated: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/deepx-rust-sdk/test_data/runtime/testnet/finalized-events-19592700-spec376/system_events_repeated.json",
        )))
        .unwrap();
        let queried: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/deepx-rust-sdk/test_data/runtime/testnet/finalized-events-19592700-spec376/system_events_query_storage_at.json",
        )))
        .unwrap();
        assert_eq!(
            manifest["rpc_payloads"],
            json!([
                "system_events.json",
                "system_events_repeated.json",
                "system_events_query_storage_at.json",
            ]),
        );
        assert_eq!(initial["result"], repeated["result"]);
        assert_eq!(queried["result"][0]["block"], manifest["block_hash"]);
        assert_eq!(
            queried["result"][0]["changes"][0][0],
            manifest["storage"]["standard_events_key"],
        );
        assert_eq!(queried["result"][0]["changes"][0][1], initial["result"]);
        let event_bytes = hex::decode(
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/deepx-rust-sdk/test_data/runtime/testnet/finalized-events-19592700-spec376/",
                "reassembled_events.hex",
            ))
            .trim()
            .trim_start_matches("0x"),
        )
        .unwrap();
        assert!(std::path::Path::new(FIXTURE_ROOT).is_dir());
        let mut remaining = event_bytes.as_slice();
        let declared = Compact::<u32>::decode(&mut remaining).unwrap().0;
        let mut consumed = event_bytes.len() - remaining.len();
        let mut decoded = 0_u32;
        let mut inventory = Vec::new();

        for event in Events::<DeepXRuntimeConfig>::decode_from(
            event_bytes.clone(),
            read_only.metadata().clone(),
        )
        .iter()
        {
            let event = event.unwrap();
            decoded += 1;
            consumed += event.bytes().len();
            inventory.push(format!("{}.{}", event.pallet_name(), event.variant_name()));
        }

        assert_eq!(declared, 15);
        assert_eq!(decoded, declared);
        assert_eq!(consumed, event_bytes.len());
        assert_eq!(
            inventory,
            [
                "System.ExtrinsicSuccess",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "PerpMarket.OrderCancelled",
                "System.ExtrinsicSuccess",
                "PerpMarket.OrderCancelled",
                "System.ExtrinsicSuccess",
                "System.ExtrinsicFailed",
                "PerpMarket.OrderCancelled",
                "System.ExtrinsicSuccess",
            ]
        );
    }
    use nautilus_model::{
        enums::OrderSide,
        identifiers::{ClientOrderId, InstrumentId},
    };

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

    fn runtime_metadata_bytes() -> Vec<u8> {
        let metadata: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/deepx-rust-sdk/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        hex::decode(
            metadata["result"]
                .as_str()
                .unwrap()
                .trim_start_matches("0x"),
        )
        .unwrap()
    }

    fn read_only_runtime_snapshot() -> DeepXReadOnlyRuntimeSnapshot {
        DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap(),
            376,
            1,
            &runtime_metadata_bytes(),
        )
        .unwrap()
    }

    fn runtime_snapshot() -> RuntimeSnapshot {
        RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap(),
            376,
            1,
            &runtime_metadata_bytes(),
        )
        .unwrap()
    }

    #[rstest]
    #[case(0, 0)]
    #[case(u128::MAX, 0)]
    #[case(0, u128::MAX)]
    #[case(u128::MAX, 17)]
    fn perp_profit_and_loss_events_bind_points(#[case] take: u128, #[case] stop: u128) {
        let snapshot = runtime_snapshot();
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
            verify_perp_profit_and_loss_point_inclusion_events_with_runtime(
                &snapshot, &identity, [8; 32], 42, 4, bytes,
            )
        };
        let business = encode(&fields, 4);
        let dispatch = dispatch_event_record(&snapshot, 4, true);
        let valid = system_events(&[business.clone(), dispatch.clone()]);
        assert!(verify(&valid).is_ok());
        assert!(
            verify_perp_profit_and_loss_point_inclusion_events_with_runtime(
                &snapshot, &baseline, [8; 32], 42, 4, &valid
            )
            .is_err()
        );
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
    fn perp_close_events_bind_generated_order(#[case] price: u128, #[case] slippage: Option<u64>) {
        let snapshot = runtime_snapshot();
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
            verify_perp_close_inclusion_events_with_runtime(
                &snapshot, &close, [8; 32], 42, 4, events,
            )
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
        snapshot: &impl RuntimeMetadataView,
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

    fn perp_modify_params() -> DeepXPerpModifyParams {
        DeepXPerpModifyParams {
            cancel: crate::signing::DeepXPerpCancelParams {
                subaccount: [0x11; 20],
                order_id: 7,
                market_id: 100,
                fast_cancel: false,
            },
            place: crate::signing::DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 100,
                is_long: true,
                size: u128::MAX,
                price: u128::MAX,
                order_type: crate::signing::DeepXPerpOrderType::Limit(
                    crate::signing::DeepXTimeInForce::Gtc,
                ),
                take_profit: Some(u128::MAX),
                stop_loss: Some(0),
                reduce_only: false,
                post_only: crate::signing::DeepXPostOnlyParam::None,
            },
        }
    }

    fn perp_modify_base_identity() -> DeepXTransactionIdentity {
        let existing = cancel_identity([0x11; 20], 7, false);
        DeepXTransactionIdentity::new_perp_modify(
            ClientOrderId::new(existing.client_order_id()),
            existing.signer(),
            existing.instrument_id(),
            OrderSide::Buy,
            existing.nonce(),
            existing.runtime().clone(),
            perp_modify_params(),
        )
    }

    fn perp_modify_event_records(
        snapshot: &impl RuntimeMetadataView,
        index: u32,
        block_number: u64,
        params: DeepXPerpModifyParams,
    ) -> Vec<Vec<u8>> {
        let base = perp_modify_base_identity();
        let place_identity = DeepXTransactionIdentity::new_perp_place(
            ClientOrderId::new(base.client_order_id()),
            base.signer(),
            base.instrument_id(),
            OrderSide::Buy,
            base.nonce(),
            base.runtime().clone(),
            params.place,
        );
        vec![
            cancel_event_record(
                snapshot,
                index,
                params.cancel.subaccount,
                params.cancel.order_id,
                0,
            ),
            perp_place_event_record(
                snapshot,
                index,
                &place_identity,
                &perp_place_order_value(&place_identity, block_number),
            ),
            dispatch_event_record(snapshot, index, true),
        ]
    }

    #[rstest]
    fn perp_modify_inclusion_requires_runtime_sizing_event() {
        let snapshot = runtime_snapshot();
        let identity = perp_modify_base_identity();
        let params = perp_modify_params();
        let events = system_events(&perp_modify_event_records(&snapshot, 4, 42, params));

        assert!(matches!(
            verify_perp_modify_inclusion_events_with_runtime(
                &snapshot, &identity, params, [8; 32], 42, 4, &events,
            ),
            Err(DeepXPerpModifyEventVerificationError::UnsupportedOperation),
        ));
    }

    #[rstest]
    fn perp_modify_successful_dispatch_without_cancel_event_is_rejected() {
        let snapshot = runtime_snapshot();
        let identity = perp_modify_base_identity();
        let params = perp_modify_params();
        let records = perp_modify_event_records(&snapshot, 4, 42, params);
        let events = system_events(&records[1..]);

        assert!(matches!(
            verify_perp_modify_inclusion_events_with_runtime(
                &snapshot, &identity, params, [8; 32], 42, 4, &events,
            ),
            Err(DeepXPerpModifyEventVerificationError::Cancel(_)),
        ));
    }

    #[rstest]
    fn perp_modify_without_runtime_sizing_event_cannot_verify_leg_order() {
        let snapshot = runtime_snapshot();
        let identity = perp_modify_base_identity();
        let params = perp_modify_params();
        let mut records = perp_modify_event_records(&snapshot, 4, 42, params);
        records.swap(0, 1);
        let events = system_events(&records);

        assert!(matches!(
            verify_perp_modify_inclusion_events_with_runtime(
                &snapshot, &identity, params, [8; 32], 42, 4, &events,
            ),
            Err(DeepXPerpModifyEventVerificationError::UnsupportedOperation),
        ));
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
        let evidence = verify_perp_place_inclusion_events_with_runtime(
            &snapshot, &identity, [8; 32], 42, 4, &events,
        )
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
            verify_perp_place_inclusion_events_with_runtime(
                &snapshot, &identity, [8; 32], 42, 4, &events,
            ),
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
            verify_perp_place_inclusion_events_with_runtime(
                &snapshot, &identity, [8; 32], 42, 4, &events,
            )
            .is_err()
        );
    }

    #[rstest]
    fn perp_place_failed_dispatch_needs_no_placement_event() {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(true, false);
        let events = system_events(&[dispatch_event_record(&snapshot, 4, false)]);
        let evidence = verify_perp_place_inclusion_events_with_runtime(
            &snapshot, &identity, [8; 32], 42, 4, &events,
        )
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
            verify_perp_place_inclusion_events_with_runtime(
                &snapshot, &identity, [8; 32], 42, 4, &events,
            )
            .is_err()
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
            crate::signing::DeepXPerpCancelParams {
                subaccount,
                order_id,
                market_id: 100,
                fast_cancel,
            },
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
        ScaleValue::unnamed_composite([ScaleValue::unnamed_composite(
            bytes
                .as_chunks::<8>()
                .0
                .iter()
                .map(|chunk| ScaleValue::u128(u128::from(u64::from_le_bytes(*chunk)))),
        )])
    }

    fn spot_place_event_record(
        snapshot: &impl RuntimeMetadataView,
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
        snapshot: &impl RuntimeMetadataView,
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
        snapshot: &impl RuntimeMetadataView,
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
        snapshot: &impl RuntimeMetadataView,
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

    #[rstest]
    fn finalized_system_dispatch_requires_one_exact_indexed_event() {
        let snapshot = runtime_snapshot();
        let location = DeepXFinalizedTransactionLocation {
            extrinsic_hash: [0x11; 32],
            block_number: 42,
            block_hash: [0x22; 32],
            extrinsic_index: 2,
        };
        let success = dispatch_event_record(&snapshot, 2, true);
        let failed = dispatch_event_record(&snapshot, 2, false);
        assert_eq!(
            decode_finalized_system_dispatch(
                &snapshot,
                location,
                &system_events(std::slice::from_ref(&success))
            )
            .unwrap(),
            DeepXDispatchOutcome::Success,
        );
        assert_eq!(
            decode_finalized_system_dispatch(&snapshot, location, &system_events(&[failed]))
                .unwrap(),
            DeepXDispatchOutcome::Failed,
        );
        for records in [
            Vec::new(),
            vec![dispatch_event_record(&snapshot, 1, true)],
            vec![success.clone(), success],
        ] {
            assert!(matches!(
                decode_finalized_system_dispatch(&snapshot, location, &system_events(&records)),
                Err(DeepXTransactionWatchError::MissingSystemDispatch { .. }),
            ));
        }
        let mut malformed = system_events(&[dispatch_event_record(&snapshot, 2, true)]);
        malformed.push(0);
        assert!(matches!(
            decode_finalized_system_dispatch(&snapshot, location, &malformed),
            Err(DeepXTransactionWatchError::InvalidEventStorage(42)),
        ));
    }

    fn delegate_account_event_record(
        snapshot: &impl RuntimeMetadataView,
        phase: Phase,
        kind: DeepXDelegateAccountChangeKind,
    ) -> Vec<u8> {
        delegate_account_event_record_for_owner(snapshot, phase, kind, [0x11; 20])
    }

    fn delegate_account_event_record_for_owner(
        snapshot: &impl RuntimeMetadataView,
        phase: Phase,
        kind: DeepXDelegateAccountChangeKind,
        owner: [u8; 20],
    ) -> Vec<u8> {
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let event_name = match kind {
            DeepXDelegateAccountChangeKind::Set => "DelegateAccountSet",
            DeepXDelegateAccountChangeKind::Removed => "DelegateAccountRemoved",
        };
        let event_index = snapshot
            .interfaces()
            .event("Subaccount", event_name)
            .unwrap()
            .index();
        let event = pallet.event_variant_by_index(event_index).unwrap();
        assert_eq!(
            event
                .fields
                .iter()
                .map(|field| field.name.as_deref())
                .collect::<Vec<_>>(),
            [Some("owner"), Some("delegate")],
        );
        let mut bytes = phase.encode();
        bytes.extend([pallet.index(), event.index]);
        for (field, value) in event.fields.iter().zip([
            ScaleValue::from_bytes(owner),
            ScaleValue::from_bytes([0x22; 20]),
        ]) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    #[rstest::rstest]
    #[case::set(DeepXDelegateAccountChangeKind::Set)]
    #[case::removed(DeepXDelegateAccountChangeKind::Removed)]
    fn finalized_delegate_account_change_binds_complete_event_and_dispatch(
        #[case] kind: DeepXDelegateAccountChangeKind,
    ) {
        let snapshot = read_only_runtime_snapshot();
        let location = DeepXFinalizedTransactionLocation {
            extrinsic_hash: [9; 32],
            block_number: 42,
            block_hash: [3; 32],
            extrinsic_index: 7,
        };
        let events = system_events(&[
            dispatch_event_record(&snapshot, 6, true),
            delegate_account_event_record(&snapshot, Phase::ApplyExtrinsic(7), kind),
            dispatch_event_record(&snapshot, 7, true),
        ]);
        let decoded =
            decode_finalized_delegate_account_changes_with_runtime(&snapshot, [3; 32], 42, &events)
                .unwrap();
        let change = verify_finalized_delegate_account_change_events_with_runtime(
            &snapshot, location, &events, [0x11; 20], [0x22; 20], kind,
        )
        .unwrap();
        assert_eq!(decoded, vec![change.clone()]);
        assert_eq!(change.block_hash, [3; 32]);
        assert_eq!(change.block_number, 42);
        assert_eq!(change.event_index, 1);
        assert_eq!(change.extrinsic_index, 7);
        assert_eq!(change.owner, [0x11; 20]);
        assert_eq!(change.delegate, [0x22; 20]);
        assert_eq!(change.kind, kind);
    }

    #[rstest::rstest]
    fn finalized_delegate_account_change_rejects_incomplete_or_conflicting_evidence() {
        let snapshot = read_only_runtime_snapshot();
        let location = DeepXFinalizedTransactionLocation {
            extrinsic_hash: [9; 32],
            block_number: 42,
            block_hash: [3; 32],
            extrinsic_index: 7,
        };
        let set = delegate_account_event_record(
            &snapshot,
            Phase::ApplyExtrinsic(7),
            DeepXDelegateAccountChangeKind::Set,
        );
        let success = dispatch_event_record(&snapshot, 7, true);
        let valid = system_events(&[set.clone(), success.clone()]);
        let mut trailing = valid.clone();
        trailing.push(0);
        let mut wrong_count = valid.clone();
        wrong_count[0] = Compact(3_u32).encode()[0];
        for bytes in [&trailing, &wrong_count, &valid[..valid.len() - 1]] {
            assert!(
                decode_finalized_delegate_account_changes_with_runtime(
                    &snapshot, [3; 32], 42, bytes,
                )
                .is_err()
            );
        }
        let wrong_phase = system_events(&[delegate_account_event_record(
            &snapshot,
            Phase::Initialization,
            DeepXDelegateAccountChangeKind::Set,
        )]);
        assert!(matches!(
            decode_finalized_delegate_account_changes_with_runtime(
                &snapshot,
                [3; 32],
                42,
                &wrong_phase,
            ),
            Err(DeepXDelegateAccountEventError::InvalidChange(0))
        ));
        let verify = |records: &[Vec<u8>], owner, kind| {
            verify_finalized_delegate_account_change_events_with_runtime(
                &snapshot,
                location,
                &system_events(records),
                owner,
                [0x22; 20],
                kind,
            )
        };
        assert!(matches!(
            verify(
                &[set.clone(), success.clone()],
                [0x33; 20],
                DeepXDelegateAccountChangeKind::Set
            ),
            Err(DeepXDelegateAccountVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify_finalized_delegate_account_change_events_with_runtime(
                &snapshot,
                location,
                &valid,
                [0x11; 20],
                [0x33; 20],
                DeepXDelegateAccountChangeKind::Set,
            ),
            Err(DeepXDelegateAccountVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify(
                &[set.clone(), success.clone()],
                [0x11; 20],
                DeepXDelegateAccountChangeKind::Removed
            ),
            Err(DeepXDelegateAccountVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify(
                &[set.clone(), set.clone(), success.clone()],
                [0x11; 20],
                DeepXDelegateAccountChangeKind::Set
            ),
            Err(DeepXDelegateAccountVerificationError::Change)
        ));
        assert!(matches!(
            verify(
                std::slice::from_ref(&set),
                [0x11; 20],
                DeepXDelegateAccountChangeKind::Set
            ),
            Err(DeepXDelegateAccountVerificationError::Dispatch)
        ));
        assert!(matches!(
            verify(
                &[set.clone(), dispatch_event_record(&snapshot, 7, false)],
                [0x11; 20],
                DeepXDelegateAccountChangeKind::Set
            ),
            Err(DeepXDelegateAccountVerificationError::Dispatch)
        ));
        assert!(matches!(
            verify(
                &[set, success.clone(), success],
                [0x11; 20],
                DeepXDelegateAccountChangeKind::Set
            ),
            Err(DeepXDelegateAccountVerificationError::Dispatch)
        ));
    }

    fn delegate_mode_event_record(
        snapshot: &impl RuntimeMetadataView,
        phase: Phase,
        mode: &str,
    ) -> Vec<u8> {
        delegate_mode_event_record_for_owner(snapshot, phase, mode, [0x11; 20])
    }

    fn delegate_mode_event_record_for_owner(
        snapshot: &impl RuntimeMetadataView,
        phase: Phase,
        mode: &str,
        owner: [u8; 20],
    ) -> Vec<u8> {
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let event_index = snapshot
            .interfaces()
            .event("Subaccount", "DelegateAccountModeUpdated")
            .unwrap()
            .index();
        let event = pallet.event_variant_by_index(event_index).unwrap();
        assert_eq!(
            event
                .fields
                .iter()
                .map(|field| field.name.as_deref())
                .collect::<Vec<_>>(),
            [Some("owner"), Some("delegate"), Some("new_mode")],
        );
        let mut bytes = phase.encode();
        bytes.extend([pallet.index(), event.index]);
        for (field, value) in event.fields.iter().zip([
            ScaleValue::from_bytes(owner),
            ScaleValue::from_bytes([0x22; 20]),
            ScaleValue::unnamed_variant(mode, []),
        ]) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    #[rstest::rstest]
    fn finalized_delegate_mode_change_binds_successful_dispatch_and_terms() {
        let snapshot = runtime_snapshot();
        let location = DeepXFinalizedTransactionLocation {
            extrinsic_hash: [9; 32],
            block_number: 42,
            block_hash: [3; 32],
            extrinsic_index: 7,
        };
        let events = system_events(&[
            dispatch_event_record(&snapshot, 6, true),
            delegate_mode_event_record(&snapshot, Phase::ApplyExtrinsic(6), "Disable"),
            delegate_mode_event_record(&snapshot, Phase::ApplyExtrinsic(7), "PlaceOrCancelOrder"),
            dispatch_event_record(&snapshot, 7, true),
        ]);
        let change = verify_finalized_delegate_mode_change_events_with_runtime(
            &snapshot,
            location,
            &events,
            [0x11; 20],
            [0x22; 20],
            DeepXDelegateMode::PlaceOrCancelOrder,
        )
        .unwrap();
        assert_eq!(change.block_hash, [3; 32]);
        assert_eq!(change.block_number, 42);
        assert_eq!(change.event_index, 2);
        assert_eq!(change.extrinsic_index, 7);
        assert_eq!(change.owner, [0x11; 20]);
        assert_eq!(change.delegate, [0x22; 20]);
        assert_eq!(change.mode, DeepXDelegateMode::PlaceOrCancelOrder);
    }

    #[rstest::rstest]
    fn finalized_delegate_mode_change_rejects_conflicting_or_incomplete_evidence() {
        let snapshot = runtime_snapshot();
        let location = DeepXFinalizedTransactionLocation {
            extrinsic_hash: [9; 32],
            block_number: 42,
            block_hash: [3; 32],
            extrinsic_index: 7,
        };
        let change =
            delegate_mode_event_record(&snapshot, Phase::ApplyExtrinsic(7), "PlaceOrCancelOrder");
        let success = dispatch_event_record(&snapshot, 7, true);
        let verify = |records: &[Vec<u8>], owner, mode| {
            verify_finalized_delegate_mode_change_events_with_runtime(
                &snapshot,
                location,
                &system_events(records),
                owner,
                [0x22; 20],
                mode,
            )
        };
        assert!(matches!(
            verify(
                &[change.clone(), success.clone()],
                [0x33; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify_finalized_delegate_mode_change_events_with_runtime(
                &snapshot,
                location,
                &system_events(&[change.clone(), success.clone()]),
                [0x11; 20],
                [0x33; 20],
                DeepXDelegateMode::PlaceOrCancelOrder,
            ),
            Err(DeepXDelegateModeVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify(
                &[change.clone(), success.clone()],
                [0x11; 20],
                DeepXDelegateMode::Disable
            ),
            Err(DeepXDelegateModeVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify(
                &[change.clone(), change.clone(), success.clone()],
                [0x11; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::Change)
        ));
        assert!(matches!(
            verify(
                std::slice::from_ref(&change),
                [0x11; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::Dispatch)
        ));
        assert!(matches!(
            verify(
                &[change.clone(), dispatch_event_record(&snapshot, 7, false)],
                [0x11; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::Dispatch)
        ));
        assert!(matches!(
            verify(
                &[change, success.clone(), success.clone()],
                [0x11; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::Dispatch)
        ));
        assert!(matches!(
            verify(
                &[success],
                [0x11; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::Change)
        ));
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
            verify_spot_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [1; 32], 42, 3, &events,
            )
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
                verify_spot_cancel_inclusion_events_with_runtime(
                    &snapshot, &changed, [1; 32], 42, 3, &events,
                )
                .is_err(),
                "{name}"
            );
        }
        let failed = system_events(&[dispatch_event_record(&snapshot, 3, false)]);
        assert_eq!(
            verify_spot_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [1; 32], 42, 3, &failed,
            )
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
                verify_spot_cancel_inclusion_events_with_runtime(
                    &snapshot, &identity, [1; 32], 42, 3, &invalid,
                )
                .is_err()
            );
        }
        let mut wrong_index = record.clone();
        wrong_index[1..5].copy_from_slice(&4_u32.to_le_bytes());
        assert!(
            verify_spot_cancel_inclusion_events_with_runtime(
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
            verify_spot_cancel_inclusion_events_with_runtime(
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
            verify_spot_place_inclusion_events_with_runtime(
                &snapshot, &identity, [1; 32], 42, 3, &events,
            )
            .unwrap()
            .outcome(),
            DeepXInclusionOutcome::Success
        );

        let mut changed = serde_json::to_value(&identity).unwrap();
        changed["operation"]["quote_amount"][0] = json!(255);
        let changed = serde_json::from_value(changed).unwrap();
        assert!(
            verify_spot_place_inclusion_events_with_runtime(
                &snapshot, &changed, [1; 32], 42, 3, &events,
            )
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
                verify_spot_place_inclusion_events_with_runtime(
                    &snapshot, &changed, [1; 32], 42, 3, &events,
                )
                .is_err(),
                "{name}"
            );
        }

        let failed = system_events(&[dispatch_event_record(&snapshot, 3, false)]);
        assert_eq!(
            verify_spot_place_inclusion_events_with_runtime(
                &snapshot, &identity, [1; 32], 42, 3, &failed,
            )
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
            system_events(&[dispatch_event_record(&snapshot, 4, true), record]),
            events[..events.len() - 1].to_vec(),
            [events.as_slice(), &[0]].concat(),
        ] {
            assert!(
                verify_spot_place_inclusion_events_with_runtime(
                    &snapshot, &identity, [1; 32], 42, 3, &invalid,
                )
                .is_err()
            );
        }
    }

    #[rstest::rstest]
    fn perpetual_cancel_event_requires_exact_index_and_fields() {
        let snapshot = runtime_snapshot();
        let subaccount = [42; 20];
        let identity = cancel_identity(subaccount, 9001, false);
        let events = system_events(&[cancel_event_record(&snapshot, 3, subaccount, 9001, 0)]);

        assert_eq!(
            verify_perp_cancel_business_event_with_runtime(&snapshot, &identity, 3, &events)
                .unwrap(),
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

        let inclusion = verify_perp_cancel_inclusion_events_with_runtime(
            &snapshot, &identity, [41; 32], 41, 3, &events,
        )
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
            verify_perp_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [41; 32], 41, 3, &events,
            ),
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

        let inclusion = verify_perp_cancel_inclusion_events_with_runtime(
            &snapshot, &identity, [41; 32], 41, 3, &events,
        )
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
            verify_perp_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [41; 32], 41, 3, &events,
            ),
            Err(DeepXPerpCancelEventVerificationError::DuplicateEvent),
        ));
    }

    #[rstest::rstest]
    fn fast_perpetual_cancel_inclusion_rejects_wrong_dispatch_index() {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, true);
        let events = system_events(&[dispatch_event_record(&snapshot, 4, true)]);

        assert!(matches!(
            verify_perp_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [41; 32], 41, 3, &events,
            ),
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
            verify_perp_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [41; 32], 41, 3, &events,
            ),
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
                verify_perp_cancel_business_event_with_runtime(&snapshot, &identity, 3, &events)
                    .unwrap(),
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
            verify_perp_cancel_business_event_with_runtime(&snapshot, &identity, 3, &events),
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
            verify_perp_cancel_business_event_with_runtime(&snapshot, &identity, 3, &events),
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
                verify_perp_cancel_business_event_with_runtime(&snapshot, &identity, 3, &events),
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
            verify_perp_cancel_business_event_with_runtime(&snapshot, &fast_cancel, 3, &[0]),
            Err(DeepXPerpCancelEventVerificationError::FastCancelUnsupported),
        ));
        assert!(matches!(
            verify_perp_cancel_business_event_with_runtime(&snapshot, &legacy, 3, &[0]),
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
        unavailable_blocks: Arc<HashSet<u64>>,
        pool_extrinsics: Arc<[String]>,
        event_storage: Arc<BTreeMap<(u64, String), String>>,
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
                    "eth_call",
                    "state_getStorage",
                    "state_queryStorageAt",
                    "state_call",
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
                if state.unavailable_blocks.contains(&number) {
                    Value::Null
                } else {
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
            }
            "state_getStorage" => {
                let key = request["params"][0].as_str().unwrap();
                let number = decode_mock_block_hash(request["params"][1].as_str().unwrap());
                json!(state.event_storage.get(&(number, key.to_string())))
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
        let event_storage = event_storage
            .into_iter()
            .map(|(block_number, value)| {
                ((block_number, SYSTEM_EVENTS_STORAGE_KEY.to_string()), value)
            })
            .collect();
        recovery_endpoints_with_storage(
            finalized_block,
            finalized_hash,
            block_extrinsics,
            pool_extrinsics,
            event_storage,
        )
        .await
    }

    async fn recovery_endpoints_with_storage(
        finalized_block: u64,
        finalized_hash: Option<String>,
        block_extrinsics: BTreeMap<u64, Vec<String>>,
        pool_extrinsics: Vec<String>,
        event_storage: BTreeMap<(u64, String), String>,
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
            unavailable_blocks: Arc::new(HashSet::new()),
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

    async fn unavailable_block_endpoints(block_number: u64) -> DeepXValidatedRpcEndpoints {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = RecoveryRpcState {
            finalized_block: block_number,
            finalized_hash: None,
            block_extrinsics: Arc::new(BTreeMap::new()),
            unavailable_blocks: Arc::new(HashSet::from([block_number])),
            pool_extrinsics: Arc::from([]),
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

    #[rstest]
    fn finalized_event_storage_uses_current_metadata_layout() {
        let snapshot = runtime_snapshot();
        let system = snapshot.metadata().pallet_by_name("System").unwrap();
        let storage = system.storage().unwrap();
        assert_eq!(storage.prefix(), "System");
        let events = storage.entry_by_name("Events").unwrap();
        assert_eq!(events.entry_type().key_ty(), None);
        assert!(storage.entry_by_name("Threads").is_none());
        assert!(storage.entry_by_name("EventsMap").is_none());
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
                    Err(DeepXTransactionWatchError::EventStorageUnavailable(41))
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

    #[tokio::test]
    async fn finalized_event_storage_combines_every_thread_through_inclusive_last_index() {
        let block_number = 41;
        let mut first = Compact(1u32).encode();
        first.push(0xaa);
        let mut second = Compact(2u32).encode();
        second.extend_from_slice(&[0xbb, 0xcc]);
        let third = Compact(0u32).encode();
        let storage = BTreeMap::from([
            (
                (block_number, system_threads_storage_key(block_number)),
                "0x02".to_string(),
            ),
            (
                (block_number, system_events_map_storage_key(block_number, 0)),
                format!("0x{}", hex::encode(&first)),
            ),
            (
                (block_number, system_events_map_storage_key(block_number, 1)),
                format!("0x{}", hex::encode(&second)),
            ),
            (
                (block_number, system_events_map_storage_key(block_number, 2)),
                format!("0x{}", hex::encode(&third)),
            ),
        ]);
        let (endpoints, _, _) =
            recovery_endpoints_with_storage(42, None, BTreeMap::new(), vec![], storage).await;
        let hash = hex::decode_array(block_hash(block_number).trim_start_matches("0x")).unwrap();

        let bytes = fetch_system_events(
            endpoints.url_for(DeepXRpcRole::Recovery),
            hash,
            block_number,
        )
        .await
        .unwrap();

        let mut expected = Compact(3u32).encode();
        expected.extend_from_slice(&[0xaa, 0xbb, 0xcc]);
        assert_eq!(bytes, expected);
    }

    #[tokio::test]
    async fn finalized_event_storage_rejects_missing_thread_batch() {
        let block_number = 41;
        let storage = BTreeMap::from([
            (
                (block_number, system_threads_storage_key(block_number)),
                "0x01".to_string(),
            ),
            (
                (block_number, system_events_map_storage_key(block_number, 0)),
                "0x00".to_string(),
            ),
        ]);
        let (endpoints, _, _) =
            recovery_endpoints_with_storage(42, None, BTreeMap::new(), vec![], storage).await;
        let hash = hex::decode_array(block_hash(block_number).trim_start_matches("0x")).unwrap();

        let result = fetch_system_events(
            endpoints.url_for(DeepXRpcRole::Recovery),
            hash,
            block_number,
        )
        .await;

        assert!(matches!(
            result,
            Err(DeepXTransactionWatchError::EventStorageUnavailable(41)),
        ));
    }

    #[tokio::test]
    async fn finalized_event_storage_rejects_malformed_last_thread() {
        let block_number = 41;
        for encoded in ["0x", "0x0001", "0xgg"] {
            let storage = BTreeMap::from([(
                (block_number, system_threads_storage_key(block_number)),
                encoded.to_string(),
            )]);
            let (endpoints, _, _) =
                recovery_endpoints_with_storage(42, None, BTreeMap::new(), vec![], storage).await;
            let hash =
                hex::decode_array(block_hash(block_number).trim_start_matches("0x")).unwrap();

            let result = fetch_system_events(
                endpoints.url_for(DeepXRpcRole::Recovery),
                hash,
                block_number,
            )
            .await;

            assert!(matches!(
                result,
                Err(DeepXTransactionWatchError::InvalidEventStorage(41)),
            ));
        }
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
            unavailable_blocks: Arc::new(HashSet::new()),
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
    async fn canonical_block_reports_unavailable_null_result() {
        let endpoints = unavailable_block_endpoints(42).await;

        let error = observe_canonical_block(&endpoints, 42, [9; 32])
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::BlockUnavailable(42),
        ));
    }

    #[tokio::test]
    async fn finalized_transaction_location_waits_then_binds_exact_membership() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = BTreeMap::from([(
            41,
            vec![
                format!("0x{}", hex::encode(extrinsic(&[5, 6]))),
                format!("0x{}", hex::encode(&target)),
            ],
        )]);
        let (pending_endpoints, pending_capabilities, _) =
            recovery_endpoints(40, None, blocks.clone(), vec![]).await;

        let pending = observe_finalized_transaction_location(
            &pending_endpoints,
            &pending_capabilities,
            target_hash,
            41,
        )
        .await
        .unwrap();

        assert!(matches!(
            pending,
            DeepXFinalizedTransactionLocationObservation::Pending(checkpoint)
                if checkpoint.block_number() == 40
        ));

        let (endpoints, capabilities, _) = recovery_endpoints(42, None, blocks, vec![]).await;
        let finalized =
            observe_finalized_transaction_location(&endpoints, &capabilities, target_hash, 41)
                .await
                .unwrap();
        let DeepXFinalizedTransactionLocationObservation::Finalized(location) = finalized else {
            panic!("expected finalized transaction location");
        };

        assert_eq!(location.extrinsic_hash(), target_hash);
        assert_eq!(location.block_number(), 41);
        assert_eq!(
            location.block_hash(),
            hex::decode_array(block_hash(41).trim_start_matches("0x")).unwrap(),
        );
        assert_eq!(location.extrinsic_index(), 1);
    }

    #[tokio::test]
    async fn finalized_transaction_location_rejects_absent_target() {
        let blocks = BTreeMap::from([(
            41,
            vec![format!("0x{}", hex::encode(extrinsic(&[1, 2, 3])))],
        )]);
        let (endpoints, capabilities, _) = recovery_endpoints(42, None, blocks, vec![]).await;

        let result =
            observe_finalized_transaction_location(&endpoints, &capabilities, [9; 32], 41).await;

        assert!(matches!(
            result,
            Err(DeepXTransactionWatchError::FinalizedExtrinsicUnavailable(
                41
            )),
        ));
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
    async fn finalized_recovery_scan_rejects_excessive_history_before_block_reads() {
        let (endpoints, capabilities, post_checkpoint_requests) =
            recovery_endpoints(1_040, None, BTreeMap::new(), vec![]).await;

        let error = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            100,
            [9; 32],
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::ScanPlan(DeepXRecoveryScanPlanError::ScanLimitExceeded {
                missing_blocks: 1_001,
                max_blocks: 1_000,
            }),
        ));
        assert_eq!(post_checkpoint_requests.load(Ordering::Relaxed), 2);
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
            DeepXFinalizedRecoveryCheckpoint::new(39, decode_hash(&block_hash(39)).unwrap()),
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
        let mut record = Phase::ApplyExtrinsic(u32::from(scenario != "wrong-index")).encode();
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
            DeepXFinalizedRecoveryCheckpoint::new(
                39,
                if scenario == "checkpoint" {
                    [0; 32]
                } else {
                    decode_hash(&block_hash(39)).unwrap()
                },
            ),
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
            DeepXFinalizedRecoveryCheckpoint::new(39, decode_hash(&block_hash(39)).unwrap()),
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
                DeepXFinalizedRecoveryCheckpoint::new(39, decode_hash(&block_hash(39)).unwrap()),
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

    #[rstest]
    #[case(0)]
    #[case(7)]
    fn finalized_spot_match_decodes_approved_event_and_exact_account_trade(#[case] buy_id: u64) {
        let snapshot = read_only_runtime_snapshot();
        let pallet = snapshot.metadata().pallet_by_name("SpotMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|variant| variant.name == "OrderMatched")
            .unwrap();
        assert_eq!(event.fields.len(), 13);
        let u256 = |value: u128| {
            let mut bytes = [0; 32];
            bytes[..16].copy_from_slice(&value.to_le_bytes());
            u256_value(bytes)
        };
        let mut record = Phase::ApplyExtrinsic(3).encode();
        record.extend([pallet.index(), event.index]);
        for field in &event.fields {
            let value = match field.name.as_deref().unwrap() {
                "pair" => ScaleValue::from_bytes([0x5a; 32]),
                "buyer" => ScaleValue::from_bytes([0x11; 20]),
                "buy_id" => ScaleValue::u128(u128::from(buy_id)),
                "seller" => ScaleValue::from_bytes([0x22; 20]),
                "sell_id" => ScaleValue::u128(8),
                "trade_amount_u" => u256(50_000_000),
                "trade_amount_b" => u256(1_000_000_000_000_000_000),
                "taker" => ScaleValue::unnamed_variant("Buyer", []),
                "taker_fee" => ScaleValue::i128(10_000_000_000_000_000),
                "taker_fee_asset" => ScaleValue::from_bytes(b"ETH"),
                "maker_fee" => ScaleValue::i128(100_000),
                "maker_fee_asset" => ScaleValue::from_bytes(b"USDC"),
                "price" => u256(50_000_000),
                unknown => panic!("unexpected Spot match field {unknown}"),
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
        let events = system_events(&[record]);
        let trades = decode_finalized_spot_trade_events_with_metadata(
            snapshot.interfaces(),
            snapshot.metadata(),
            [3; 32],
            42,
            &events,
        )
        .unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].event_index, 0);
        assert_eq!(
            trades[0].trade_amount_b[..16],
            1_000_000_000_000_000_000_u128.to_le_bytes()
        );
        let account_trade = DeepXSpotAccountTradeRecord(
            deepx_rust_sdk::rest::models::DeepXSpotAccountTradeRecord {
                id: 42_000_000,
                order_id: buy_id.to_string(),
                order_side: "Buy".to_string(),
                pair: format!("0x{}", hex::encode([0x5a; 32])),
                pair_name: "ETH/USDC".to_string(),
                base_amount: "1".parse().unwrap(),
                quote_amount: "50".parse().unwrap(),
                price: "50".parse().unwrap(),
                fee: "-0.01".parse().unwrap(),
                fee_asset: "ETH".to_string(),
                taker: "Buyer".to_string(),
                created_at: "2026-06-18T15:13:14.739Z".to_string(),
            },
        );
        assert_eq!(
            match_finalized_spot_account_trade(
                &trades,
                &account_trade,
                [0x11; 20],
                18,
                6,
                "ETH",
                "USDC",
            )
            .unwrap(),
            &trades[0]
        );
        let mut seller_trade = account_trade.clone();
        seller_trade.order_id = "8".to_string();
        seller_trade.order_side = "Sell".to_string();
        seller_trade.fee = "-0.1".parse().unwrap();
        seller_trade.fee_asset = "USDC".to_string();
        assert_eq!(
            match_finalized_spot_account_trade(
                &trades,
                &seller_trade,
                [0x22; 20],
                18,
                6,
                "ETH",
                "USDC",
            )
            .unwrap(),
            &trades[0]
        );
        seller_trade.fee_asset = "ETH".to_string();
        assert!(matches!(
            match_finalized_spot_account_trade(
                &trades,
                &seller_trade,
                [0x22; 20],
                18,
                6,
                "ETH",
                "USDC"
            ),
            Err(DeepXSpotTradeMatchError::InvalidInput)
        ));
        let mut conflicting = account_trade;
        conflicting.quote_amount = "49.999999".parse().unwrap();
        assert!(matches!(
            match_finalized_spot_account_trade(
                &trades,
                &conflicting,
                [0x11; 20],
                18,
                6,
                "ETH",
                "USDC"
            ),
            Err(DeepXSpotTradeMatchError::NotFound)
        ));
        conflicting.quote_amount = "50".parse().unwrap();
        conflicting.id += 1;
        assert!(matches!(
            match_finalized_spot_account_trade(
                &trades,
                &conflicting,
                [0x11; 20],
                18,
                6,
                "ETH",
                "USDC"
            ),
            Err(DeepXSpotTradeMatchError::NotFound)
        ));
        let mut truncated = events;
        truncated.pop();
        assert!(
            decode_finalized_spot_trade_events_with_metadata(
                snapshot.interfaces(),
                snapshot.metadata(),
                [3; 32],
                42,
                &truncated,
            )
            .is_err()
        );
    }
}
