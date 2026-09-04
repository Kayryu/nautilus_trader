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
pub mod execution;
pub mod http;
pub mod instruments;
pub mod providers;
pub mod rpc;
pub mod signing;
pub mod spot;
pub mod transaction;
pub mod websocket;

pub use common::{DeepXEnvironment, DeepXError, DeepXKeyScheme, DeepXPrivateKey, DeepXProductType};
pub use config::{
    DeepXExecutionBackend, DeepXExecutionClientConfig, DeepXNetworkConfig,
    DeepXObservedRpcEndpoint, DeepXRpcEndpointValidationError, DeepXRpcRole,
    DeepXValidatedRpcEndpoints, validate_rpc_endpoint_identities,
};
pub use execution::{
    DeepXExecutionClient, DeepXExecutionStartupError, DeepXExecutionStartupEvidence,
    DeepXExecutionUpdateRoute, DeepXExternalOrderContext, DeepXMassReconciliationError,
    DeepXOrderContextError, DeepXOrderContextRestorationError, DeepXTradeDedupError,
};
pub use instruments::parse_perpetual_instrument;
pub use providers::{DeepXMarketMetadata, DeepXMarketProvider};
pub use rpc::{
    DeepXAppliedRuntimeSnapshot, DeepXFinalizedCheckpoint, DeepXObservedRuntimeSnapshot,
    DeepXRpcEndpointIdentityError, DeepXRpcIdentityError, DeepXRpcMethodCapabilities,
    DeepXRpcMethodCapabilityError, DeepXRuntimeSnapshotObservationError,
    DeepXRuntimeSnapshotRefreshError, observe_and_apply_approved_finalized_runtime_snapshot,
    observe_and_validate_rpc_endpoint_identities, observe_approved_finalized_runtime_snapshot,
    observe_rpc_endpoint_identity, observe_rpc_method_capabilities,
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
    DeepXBusinessEventOutcome, DeepXCanonicalBlockEvidence, DeepXCommittedObservation,
    DeepXCommittedTransactionRecord, DeepXDirectRuntimeIdentity, DeepXDispatchOutcome,
    DeepXDurableSignedExtrinsic, DeepXInclusionEvidence, DeepXInclusionEvidenceError,
    DeepXInclusionOutcome, DeepXIndexedOutcome, DeepXMissedBlockScanPlan, DeepXNonceReservation,
    DeepXObservationCommitError, DeepXPostgresSignerLease, DeepXPostgresTransactionStore,
    DeepXPreparedReservation, DeepXPreparedSignedTransaction, DeepXPreparedSubmission,
    DeepXRecoveryDecision, DeepXRecoveryScan, DeepXRecoveryScanCollectionError,
    DeepXRecoveryScanCollector, DeepXRecoveryScanPlanError, DeepXRecoveryScanRange,
    DeepXRecoveryScanRanges, DeepXReorganizationDecision, DeepXReservationPreparationError,
    DeepXRestoredTransactionRecord, DeepXSignedTransactionPreparationError, DeepXSignerLease,
    DeepXSubmissionFailure, DeepXSubmissionPermit, DeepXSubmissionPoolEvidence,
    DeepXSubmissionPreparationError, DeepXTimestampNonceAllocator, DeepXTimestampNonceError,
    DeepXTransactionError, DeepXTransactionIdentity, DeepXTransactionLifecycle,
    DeepXTransactionObservation, DeepXTransactionPersistenceError, DeepXTransactionRecord,
    DeepXTransactionRecordError, DeepXTransactionRecoveryAction, DeepXTransactionRevision,
    DeepXTransactionState, DeepXTransactionStore, DeepXUnsupportedBusinessCallVerifier,
    classify_reorganization, commit_reconciliation_observation, commit_recovery_decision,
    commit_reorganization_decision, plan_missed_block_scan, prepare_initial_submission,
    prepare_signed_transaction, prepare_timestamp_reservation, restore_timestamp_nonce_allocator,
    verify_signer_lease,
};
