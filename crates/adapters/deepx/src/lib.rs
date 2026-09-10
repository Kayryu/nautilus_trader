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

//! [NautilusTrader](https://nautilustrader.io) adapter for the DeepX testnet.

#![warn(rustc::all)]
#![deny(unsafe_code)]
#![deny(nonstandard_style)]
#![deny(missing_debug_implementations)]
#![deny(clippy::missing_panics_doc)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod common;
pub mod config;
pub mod data;
pub mod execution;
pub mod factories;
pub mod http;
pub mod instruments;
pub mod providers;
#[cfg(feature = "python")]
pub mod python;
pub mod rpc;
pub mod signing;
pub mod spot;
pub mod transaction;
pub mod websocket;

pub use common::{DeepXEnvironment, DeepXError, DeepXKeyScheme, DeepXPrivateKey, DeepXProductType};
pub use config::{
    DeepXDataClientConfig, DeepXExecutionBackend, DeepXExecutionClientConfig,
    DeepXHttpReadRetryConfig, DeepXNetworkConfig, DeepXObservedRpcEndpoint,
    DeepXRpcEndpointValidationError, DeepXRpcRole, DeepXValidatedRpcEndpoints,
    validate_rpc_endpoint_identities,
};
pub use data::DeepXDataClient;
pub use execution::{
    DeepXExecutionClient, DeepXExecutionStartupError, DeepXExecutionStartupEvidence,
    DeepXExecutionUpdateRoute, DeepXExternalOrderContext, DeepXMassReconciliationError,
    DeepXNonceRestorationError, DeepXOrderContextError, DeepXOrderContextRestorationError,
    DeepXRestoredOrderContext, DeepXTradeDedupError,
};
pub use factories::{DeepXDataClientFactory, DeepXExecutionClientFactory};
pub use instruments::parse_perpetual_instrument;
pub use providers::{
    DeepXInstrumentProvider, DeepXMarketMetadata, DeepXMarketProvider,
    DeepXSpotInstrumentUnsupported,
};
pub use rpc::{
    DeepXAppliedRuntimeSnapshot, DeepXFinalizedCheckpoint, DeepXObservedRuntimeSnapshot,
    DeepXRpcEndpointIdentityError, DeepXRpcIdentityError, DeepXRpcMethodCapabilities,
    DeepXRpcMethodCapabilitiesError, DeepXRpcMethodCapabilityError,
    DeepXRuntimeSnapshotObservationError, DeepXRuntimeSnapshotRefreshError,
    DeepXValidatedRpcMethodCapabilities, observe_and_apply_approved_finalized_runtime_snapshot,
    observe_and_validate_rpc_endpoint_identities, observe_and_validate_rpc_method_capabilities,
    observe_approved_finalized_runtime_snapshot, observe_rpc_endpoint_identity,
    observe_rpc_method_capabilities,
};
pub use signing::{
    ApprovedRuntimeIdentity, DeepXRuntimeChangeDecision, DeepXRuntimeConfig,
    DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError, DeepXRuntimePalletInterface,
    DeepXRuntimeSnapshotPermit, DeepXRuntimeSnapshotService, DeepXRuntimeSnapshotServiceError,
    DeepXRuntimeSnapshotUpdate, DeepXRuntimeVariantIdentity, RuntimeSnapshot,
    SignedPalletExtrinsic, SigningError, SnapshotError, sign_dynamic_pallet_call,
};
pub use spot::{DeepXSpotMarketSpec, DeepXSpotMarketSpecClient, DeepXSpotMarketSpecError};
pub use transaction::{
    DEEPX_TRANSACTION_CACHE_KEY_PREFIX, DEEPX_TRANSACTION_RECORD_VERSION, DeepXAbsenceEvidence,
    DeepXAutomaticReplayDecision, DeepXBusinessCallBindingError, DeepXBusinessCallVerifier,
    DeepXBusinessEventOutcome, DeepXCanonicalBlockEvidence, DeepXCanonicalBlockObservation,
    DeepXCommittedObservation, DeepXCommittedTransactionRecord, DeepXDirectRuntimeIdentity,
    DeepXDispatchOutcome, DeepXDurableSignedExtrinsic, DeepXFinalityCommitError,
    DeepXFinalityObservation, DeepXFinalizedRecoveryCheckpoint, DeepXFinalizedRecoveryCollection,
    DeepXFinalizedRecoveryCommitError, DeepXInclusionEvidence, DeepXInclusionEvidenceError,
    DeepXInclusionOutcome, DeepXIndexedOutcome, DeepXMissedBlockScanPlan, DeepXNonceReservation,
    DeepXObservationCommitError, DeepXPoolObservation, DeepXPoolReconciliationCommitError,
    DeepXPostgresSignerLease, DeepXPostgresTransactionStore, DeepXPreparedReservation,
    DeepXPreparedSignedTransaction, DeepXPreparedSubmission, DeepXRecoveryDecision,
    DeepXRecoveryScan, DeepXRecoveryScanCollectionError, DeepXRecoveryScanCollector,
    DeepXRecoveryScanPlanError, DeepXRecoveryScanRange, DeepXRecoveryScanRanges,
    DeepXRemarkCallVerifier, DeepXReorganizationCommitError, DeepXReorganizationDecision,
    DeepXReservationPreparationError, DeepXRestoredTransactionRecord,
    DeepXSignedTransactionPreparationError, DeepXSignerLease, DeepXSubmissionAcceptanceCommitError,
    DeepXSubmissionFailure, DeepXSubmissionPermit, DeepXSubmissionPoolEvidence,
    DeepXSubmissionPreparationError, DeepXSubmissionRetryError, DeepXTimestampNonceAllocator,
    DeepXTimestampNonceError, DeepXTransactionError, DeepXTransactionIdentity,
    DeepXTransactionLifecycle, DeepXTransactionObservation, DeepXTransactionPersistenceError,
    DeepXTransactionRecord, DeepXTransactionRecordError, DeepXTransactionRecoveryAction,
    DeepXTransactionRevision, DeepXTransactionState, DeepXTransactionStore,
    DeepXTransactionWatchError, DeepXUnsupportedBusinessCallVerifier, classify_reorganization,
    collect_finalized_recovery_scan, commit_initial_submission_acceptance,
    commit_reconciliation_observation, commit_recovery_decision, commit_reorganization_decision,
    observe_and_commit_finality, observe_and_commit_reorganization, observe_canonical_block,
    observe_finality, observe_reorganization, observe_submission_pool, plan_missed_block_scan,
    prepare_initial_submission, prepare_signed_transaction, prepare_timestamp_reservation,
    reconcile_not_included_checkpoint, reconcile_submission_pool,
    restore_timestamp_nonce_allocator, submit_with_bounded_ambiguity_retry, verify_signer_lease,
};
