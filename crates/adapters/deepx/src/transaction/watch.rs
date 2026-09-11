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
    let business_event =
        verify_perp_cancel_business_event(snapshot, identity, extrinsic_index, event_bytes)?;
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    let mut dispatch = None;
    for event in events.iter() {
        let event = event.map_err(DeepXPerpCancelEventVerificationError::Decode)?;
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
    let dispatch = DeepXIndexedOutcome {
        extrinsic_index,
        outcome: dispatch.ok_or(DeepXPerpCancelEventVerificationError::MalformedEventBytes)?,
    };
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
        Some((snapshot, identity)),
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
    event_evidence: Option<(&RuntimeSnapshot, &DeepXTransactionIdentity)>,
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
                let Some((snapshot, identity)) = event_evidence else {
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
                let inclusion = verify_perp_cancel_inclusion_events(
                    snapshot,
                    identity,
                    observation.block_hash(),
                    observation.block_number(),
                    extrinsic_index,
                    &event_bytes,
                )?;
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
    hex::decode(value).map_err(|_| DeepXTransactionWatchError::InvalidEventStorage(block_number))
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
    use serde_json::Value;
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        common::{
            DeepXEnvironment, DeepXKeyScheme, DeepXPrivateKey, consts::DEEPX_TESTNET_GENESIS_HASH,
        },
        config::{DeepXNetworkConfig, DeepXObservedRpcEndpoint, validate_rpc_endpoint_identities},
        rpc::observe_and_validate_rpc_method_capabilities,
        signing::derive_signer_account_id,
        transaction::{DeepXDirectRuntimeIdentity, DeepXNonceReservation, DeepXRecoveryDecision},
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
        if success {
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
            subxt_core::ext::scale_value::scale::encode_as_type(
                &dispatch_info,
                metadata_event.fields[0].ty.id,
                snapshot.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        } else {
            bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
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
