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

//! Immutable runtime identity and metadata used by offline signing.

use std::sync::{Arc, Mutex};

use deepx_rust_sdk::signing::validate_testnet_runtime;
pub use deepx_rust_sdk::signing::{
    ApprovedRuntimeIdentity, SnapshotError,
    runtime_interface::{
        DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError, DeepXRuntimePalletInterface,
        DeepXRuntimeVariantIdentity,
    },
};
#[cfg(test)]
use nautilus_core::hex;
use subxt_core::{
    client::{ClientState, RuntimeVersion},
    metadata::Metadata,
    utils::H256,
};
use thiserror::Error;

use super::DeepXRuntimeConfig;
use crate::common::DeepXEnvironment;

#[cfg(test)]
const TESTNET_SPEC_376_GENESIS_HASH: &str =
    "87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7";
#[cfg(test)]
const TESTNET_MIN_SPEC_VERSION: u32 = 376;
#[cfg(test)]
const TESTNET_SPEC_376_METADATA_SHA256: &str =
    "57f1bedf1e04522176a2b422af9249d5c990693fb92d11327870d8ef751a8447";
#[cfg(test)]
const TESTNET_TRANSACTION_VERSION: u32 = 1;
#[cfg(test)]
const TESTNET_SPEC_376_METADATA_RESPONSE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/deepx-rust-sdk/test_data/runtime/testnet/",
    "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
));
#[cfg(test)]
const TESTNET_SPEC_376_SIGNED_EXTENSIONS: &[&str] = &[
    "CheckNonZeroSender",
    "CheckSpecVersion",
    "CheckTxVersion",
    "CheckGenesis",
    "CheckNonceEra",
    "CheckNonce",
    "CheckWeight",
    "ChargeTransactionPayment",
    "CheckPriority",
];

/// Decision produced after comparing an observed runtime identity with the active snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXRuntimeChangeDecision {
    /// The observed identity matches the active immutable snapshot.
    Unchanged,
    /// New signing is blocked until the observed identity is validated and installed.
    RefreshRequired,
}

/// Result of atomically applying a fixture-validated runtime snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXRuntimeSnapshotUpdate {
    /// The candidate identity matches the active immutable snapshot.
    Unchanged,
    /// The candidate replaced the active immutable snapshot.
    Installed,
}

/// Errors produced by the runtime snapshot quiescence boundary.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXRuntimeSnapshotServiceError {
    /// New signing is blocked while the execution account connection is unavailable.
    #[error("DeepX execution account connection is unavailable")]
    AccountConnectionUnavailable,
    /// New signing is blocked while a changed runtime is being validated.
    #[error("DeepX runtime snapshot refresh is in progress")]
    RefreshInProgress,
    /// Contradictory finalized RPC evidence invalidated this service for new signing.
    #[error("DeepX finalized RPC evidence is inconsistent; restart the runtime snapshot service")]
    EvidenceInconsistent,
    /// The execution client retired this runtime before a pending operation could transmit.
    #[error("DeepX execution runtime has been retired")]
    ClientRetired,
    /// A different runtime change was observed while refresh was already in progress.
    #[error("DeepX runtime snapshot refresh already targets a different identity")]
    ConflictingRuntimeChange,
    /// Installation was attempted before a changed runtime was observed.
    #[error("DeepX runtime snapshot refresh has not started")]
    RefreshNotStarted,
    /// The validated snapshot does not match the identity that triggered refresh.
    #[error("validated DeepX runtime snapshot does not match the observed identity")]
    SnapshotIdentityMismatch,
    /// Existing signing permits still hold the previous immutable snapshot.
    #[error("DeepX runtime snapshot has {0} in-flight signing permits")]
    InFlightSigningPermits(usize),
    /// Shared runtime snapshot state cannot be trusted after synchronization failure.
    #[error("DeepX runtime snapshot service state is unavailable")]
    StateUnavailable,
}

/// An immutable metadata and runtime-version snapshot for deterministic signing.
#[derive(Clone, Debug)]
pub struct RuntimeSnapshot {
    identity: ApprovedRuntimeIdentity,
    interfaces: DeepXRuntimeInterfaceCatalog,
    client_state: ClientState<DeepXRuntimeConfig>,
}

/// Immutable runtime identity approved only for finalized read operations.
///
/// Unlike [`RuntimeSnapshot`], this type cannot initialize a signing service or produce a signing
/// permit. It exists so a captured runtime can be approved for read-only evidence without also
/// authorizing transaction encoding or signing.
#[derive(Clone, Debug)]
pub struct DeepXReadOnlyRuntimeSnapshot {
    identity: ApprovedRuntimeIdentity,
    interfaces: DeepXRuntimeInterfaceCatalog,
    metadata: Metadata,
}

#[derive(Debug)]
struct RuntimeSnapshotServiceState {
    active: Arc<RuntimeSnapshot>,
    pending_identity: Option<PendingRuntimeIdentity>,
    in_flight: usize,
    connection_generation: u64,
    account_connection_available: bool,
    evidence_inconsistent: bool,
    client_retired: bool,
}

#[derive(Debug)]
enum PendingRuntimeIdentity {
    Approved(ApprovedRuntimeIdentity),
}

impl PendingRuntimeIdentity {
    fn matches(&self, identity: &ApprovedRuntimeIdentity) -> bool {
        match self {
            Self::Approved(approved) => approved == identity,
        }
    }
}

/// Coordinates immutable runtime snapshots across signing and runtime upgrades.
#[derive(Clone, Debug)]
pub struct DeepXRuntimeSnapshotService {
    state: Arc<Mutex<RuntimeSnapshotServiceState>>,
}

impl DeepXRuntimeSnapshotService {
    /// Creates an active service from a fixture-validated runtime snapshot.
    #[must_use]
    pub fn new(snapshot: RuntimeSnapshot) -> Self {
        Self {
            state: Arc::new(Mutex::new(RuntimeSnapshotServiceState {
                active: Arc::new(snapshot),
                pending_identity: None,
                in_flight: 0,
                connection_generation: 0,
                account_connection_available: true,
                evidence_inconsistent: false,
                client_retired: false,
            })),
        }
    }

    /// Acquires the immutable snapshot used for one in-flight signing operation.
    ///
    /// # Errors
    ///
    /// Returns an error when the client retired this service, runtime refresh is in progress, or
    /// shared state is unavailable.
    pub fn acquire(&self) -> Result<DeepXRuntimeSnapshotPermit, DeepXRuntimeSnapshotServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        if state.client_retired {
            return Err(DeepXRuntimeSnapshotServiceError::ClientRetired);
        }
        if !state.account_connection_available {
            return Err(DeepXRuntimeSnapshotServiceError::AccountConnectionUnavailable);
        }
        if state.evidence_inconsistent {
            return Err(DeepXRuntimeSnapshotServiceError::EvidenceInconsistent);
        }
        if state.pending_identity.is_some() {
            return Err(DeepXRuntimeSnapshotServiceError::RefreshInProgress);
        }
        state.in_flight = state
            .in_flight
            .checked_add(1)
            .ok_or(DeepXRuntimeSnapshotServiceError::StateUnavailable)?;

        Ok(DeepXRuntimeSnapshotPermit {
            service_state: Arc::clone(&self.state),
            snapshot: Arc::clone(&state.active),
            connection_generation: state.connection_generation,
        })
    }

    /// Invalidates current permits and blocks new signing until the account stream is restored.
    pub(crate) fn suspend_for_account_disconnect(
        &self,
    ) -> Result<(), DeepXRuntimeSnapshotServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        if state.client_retired {
            return Err(DeepXRuntimeSnapshotServiceError::ClientRetired);
        }
        state.connection_generation = state
            .connection_generation
            .checked_add(1)
            .ok_or(DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        state.account_connection_available = false;
        Ok(())
    }

    /// Reopens signing after a replacement account stream is fully validated.
    pub(crate) fn resume_after_account_reconnect(
        &self,
    ) -> Result<(), DeepXRuntimeSnapshotServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        if state.client_retired {
            return Err(DeepXRuntimeSnapshotServiceError::ClientRetired);
        }
        if state.evidence_inconsistent {
            return Err(DeepXRuntimeSnapshotServiceError::EvidenceInconsistent);
        }
        state.account_connection_available = true;
        Ok(())
    }

    /// Permanently closes new permits after contradictory finalized RPC evidence.
    pub(crate) fn halt_for_inconsistent_evidence(
        &self,
    ) -> Result<(), DeepXRuntimeSnapshotServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        state.evidence_inconsistent = true;
        Ok(())
    }

    /// Revokes existing and future permits when their execution client retires.
    pub(crate) fn retire_client(&self) -> Result<(), DeepXRuntimeSnapshotServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        state.client_retired = true;
        Ok(())
    }

    /// Compares an observed runtime identity and blocks new signing when it changes.
    ///
    /// # Errors
    ///
    /// Returns an error when refresh already targets another identity or shared state is
    /// unavailable.
    pub fn observe_runtime_identity(
        &self,
        observed: ApprovedRuntimeIdentity,
    ) -> Result<DeepXRuntimeChangeDecision, DeepXRuntimeSnapshotServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        if let Some(pending) = &state.pending_identity {
            return if pending.matches(&observed) {
                state.pending_identity = Some(PendingRuntimeIdentity::Approved(observed));
                Ok(DeepXRuntimeChangeDecision::RefreshRequired)
            } else {
                Err(DeepXRuntimeSnapshotServiceError::ConflictingRuntimeChange)
            };
        }
        if state.active.identity() == &observed {
            return Ok(DeepXRuntimeChangeDecision::Unchanged);
        }

        state.pending_identity = Some(PendingRuntimeIdentity::Approved(observed));
        Ok(DeepXRuntimeChangeDecision::RefreshRequired)
    }

    /// Installs a fixture-validated replacement after all old signing permits are released.
    ///
    /// # Errors
    ///
    /// Returns an error unless the service is active, refresh is in progress, the snapshot matches
    /// the observed identity, no old signing permits remain, and shared state is available.
    pub fn install(
        &self,
        snapshot: RuntimeSnapshot,
    ) -> Result<(), DeepXRuntimeSnapshotServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        if state.client_retired {
            return Err(DeepXRuntimeSnapshotServiceError::ClientRetired);
        }
        if state.evidence_inconsistent {
            return Err(DeepXRuntimeSnapshotServiceError::EvidenceInconsistent);
        }
        let pending = state
            .pending_identity
            .as_ref()
            .ok_or(DeepXRuntimeSnapshotServiceError::RefreshNotStarted)?;
        if !pending.matches(snapshot.identity()) {
            return Err(DeepXRuntimeSnapshotServiceError::SnapshotIdentityMismatch);
        }
        if state.in_flight != 0 {
            return Err(DeepXRuntimeSnapshotServiceError::InFlightSigningPermits(
                state.in_flight,
            ));
        }

        state.active = Arc::new(snapshot);
        state.pending_identity = None;
        Ok(())
    }

    /// Atomically compares and applies a fixture-validated runtime snapshot.
    ///
    /// A changed candidate immediately blocks new signing permits. When old permits remain, the
    /// candidate identity stays pending so the same snapshot can be retried after they are
    /// released.
    ///
    /// # Errors
    ///
    /// Returns an error when the client retired this service, another runtime identity is pending,
    /// old signing permits remain, or shared state is unavailable.
    pub fn apply_validated(
        &self,
        snapshot: &RuntimeSnapshot,
    ) -> Result<DeepXRuntimeSnapshotUpdate, DeepXRuntimeSnapshotServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        if state.client_retired {
            return Err(DeepXRuntimeSnapshotServiceError::ClientRetired);
        }
        if state.evidence_inconsistent {
            return Err(DeepXRuntimeSnapshotServiceError::EvidenceInconsistent);
        }

        if let Some(pending) = &state.pending_identity {
            if !pending.matches(snapshot.identity()) {
                return Err(DeepXRuntimeSnapshotServiceError::ConflictingRuntimeChange);
            }
        } else if state.active.identity() == snapshot.identity() {
            return Ok(DeepXRuntimeSnapshotUpdate::Unchanged);
        }
        state.pending_identity = Some(PendingRuntimeIdentity::Approved(
            snapshot.identity().clone(),
        ));

        if state.in_flight != 0 {
            return Err(DeepXRuntimeSnapshotServiceError::InFlightSigningPermits(
                state.in_flight,
            ));
        }

        state.active = Arc::new(snapshot.clone());
        state.pending_identity = None;
        Ok(DeepXRuntimeSnapshotUpdate::Installed)
    }
}

/// In-flight ownership of one immutable runtime snapshot.
#[derive(Debug)]
pub struct DeepXRuntimeSnapshotPermit {
    service_state: Arc<Mutex<RuntimeSnapshotServiceState>>,
    snapshot: Arc<RuntimeSnapshot>,
    connection_generation: u64,
}

impl DeepXRuntimeSnapshotPermit {
    /// Returns the immutable snapshot retained for this signing operation.
    #[must_use]
    pub fn snapshot(&self) -> &RuntimeSnapshot {
        &self.snapshot
    }

    /// Verifies that this permit still belongs to an active, consistent runtime service.
    ///
    /// # Errors
    ///
    /// Returns an error after contradictory RPC evidence or if shared state is unavailable.
    pub(crate) fn ensure_active(&self) -> Result<(), DeepXRuntimeSnapshotServiceError> {
        let state = self
            .service_state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        if state.client_retired {
            return Err(DeepXRuntimeSnapshotServiceError::ClientRetired);
        }
        if !state.account_connection_available
            || state.connection_generation != self.connection_generation
        {
            return Err(DeepXRuntimeSnapshotServiceError::AccountConnectionUnavailable);
        }
        if state.evidence_inconsistent {
            return Err(DeepXRuntimeSnapshotServiceError::EvidenceInconsistent);
        }
        if state.active.identity() != self.snapshot.identity() {
            return Err(DeepXRuntimeSnapshotServiceError::SnapshotIdentityMismatch);
        }
        Ok(())
    }
}

impl Drop for DeepXRuntimeSnapshotPermit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.service_state.lock() {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }
}

impl RuntimeSnapshot {
    #[cfg(test)]
    pub(crate) fn from_read_only_for_test(snapshot: DeepXReadOnlyRuntimeSnapshot) -> Self {
        let client_state = ClientState {
            metadata: snapshot.metadata,
            genesis_hash: H256::from(snapshot.identity.genesis_hash),
            runtime_version: RuntimeVersion {
                spec_version: snapshot.identity.spec_version,
                transaction_version: snapshot.identity.transaction_version,
            },
        };

        Self {
            identity: snapshot.identity,
            interfaces: snapshot.interfaces,
            client_state,
        }
    }

    /// Builds a signing runtime from metadata observed on the DeepX testnet chain.
    ///
    /// # Errors
    ///
    /// Returns an error for another genesis, invalid metadata, zero runtime versions, or
    /// unsupported signing extensions.
    pub fn approved_testnet(
        environment: &DeepXEnvironment,
        observed_genesis_hash: [u8; 32],
        observed_spec_version: u32,
        observed_transaction_version: u32,
        metadata_bytes: &[u8],
    ) -> Result<Self, SnapshotError> {
        Self::observed_testnet(
            environment,
            observed_genesis_hash,
            observed_spec_version,
            observed_transaction_version,
            metadata_bytes,
        )
    }

    /// Builds a signing runtime from metadata observed on the configured testnet chain.
    ///
    /// # Errors
    ///
    /// Returns an error for another genesis, invalid metadata or unsupported signing extensions.
    pub(crate) fn observed_testnet(
        environment: &DeepXEnvironment,
        observed_genesis_hash: [u8; 32],
        observed_spec_version: u32,
        observed_transaction_version: u32,
        metadata_bytes: &[u8],
    ) -> Result<Self, SnapshotError> {
        let (identity, interfaces, metadata) = validate_testnet_runtime(
            environment,
            observed_genesis_hash,
            observed_spec_version,
            observed_transaction_version,
            metadata_bytes,
        )?
        .into_parts();
        let client_state = ClientState {
            metadata,
            genesis_hash: H256::from(identity.genesis_hash),
            runtime_version: RuntimeVersion {
                spec_version: identity.spec_version,
                transaction_version: identity.transaction_version,
            },
        };

        Ok(Self {
            identity,
            interfaces,
            client_state,
        })
    }

    /// Builds the approved DeepX testnet signing runtime from built-in metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the built-in fixture fails identity, integrity, or interface
    /// validation.
    #[cfg(test)]
    pub(crate) fn built_in_testnet() -> Result<Self, SnapshotError> {
        Self::approved_testnet(
            &DeepXEnvironment::Testnet,
            decode_32(TESTNET_SPEC_376_GENESIS_HASH)?,
            TESTNET_MIN_SPEC_VERSION,
            TESTNET_TRANSACTION_VERSION,
            &built_in_testnet_metadata()?,
        )
    }

    /// Returns the validated runtime identity associated with this snapshot.
    #[must_use]
    pub const fn identity(&self) -> &ApprovedRuntimeIdentity {
        &self.identity
    }

    /// Returns the immutable pallet, call, and event identities for this snapshot.
    #[must_use]
    pub const fn interfaces(&self) -> &DeepXRuntimeInterfaceCatalog {
        &self.interfaces
    }

    pub(super) const fn client_state(&self) -> &ClientState<DeepXRuntimeConfig> {
        &self.client_state
    }

    pub(crate) const fn metadata(&self) -> &Metadata {
        &self.client_state.metadata
    }
}

impl DeepXReadOnlyRuntimeSnapshot {
    /// Builds a read-only runtime from metadata observed on the configured testnet chain.
    ///
    /// # Errors
    ///
    /// Returns an error for another genesis, invalid metadata or unsupported extensions.
    pub(crate) fn observed_testnet(
        environment: &DeepXEnvironment,
        observed_genesis_hash: [u8; 32],
        observed_spec_version: u32,
        observed_transaction_version: u32,
        metadata_bytes: &[u8],
    ) -> Result<Self, SnapshotError> {
        let (identity, interfaces, metadata) = validate_testnet_runtime(
            environment,
            observed_genesis_hash,
            observed_spec_version,
            observed_transaction_version,
            metadata_bytes,
        )?
        .into_parts();
        Ok(Self {
            identity,
            interfaces,
            metadata,
        })
    }

    /// Builds a read-only runtime from observed DeepX testnet metadata.
    ///
    /// # Errors
    ///
    /// Returns an error for another genesis, invalid metadata, zero runtime versions, or
    /// unsupported extensions.
    pub fn approved_testnet(
        environment: &DeepXEnvironment,
        observed_genesis_hash: [u8; 32],
        observed_spec_version: u32,
        observed_transaction_version: u32,
        metadata_bytes: &[u8],
    ) -> Result<Self, SnapshotError> {
        Self::observed_testnet(
            environment,
            observed_genesis_hash,
            observed_spec_version,
            observed_transaction_version,
            metadata_bytes,
        )
    }

    /// Builds the approved DeepX testnet read-only runtime from built-in metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the built-in fixture fails identity, integrity, or interface
    /// validation.
    #[cfg(test)]
    pub(crate) fn built_in_testnet() -> Result<Self, SnapshotError> {
        Self::approved_testnet(
            &DeepXEnvironment::Testnet,
            decode_32(TESTNET_SPEC_376_GENESIS_HASH)?,
            TESTNET_MIN_SPEC_VERSION,
            TESTNET_TRANSACTION_VERSION,
            &built_in_testnet_metadata()?,
        )
    }

    /// Returns the validated runtime identity associated with this read-only snapshot.
    #[must_use]
    pub const fn identity(&self) -> &ApprovedRuntimeIdentity {
        &self.identity
    }

    /// Returns the immutable pallet, call, and event identities for this snapshot.
    #[must_use]
    pub const fn interfaces(&self) -> &DeepXRuntimeInterfaceCatalog {
        &self.interfaces
    }

    pub(crate) const fn metadata(&self) -> &Metadata {
        &self.metadata
    }
}

#[cfg(test)]
fn decode_32(value: &str) -> Result<[u8; 32], SnapshotError> {
    hex::decode_array(value).map_err(|_| SnapshotError::InvalidApprovedIdentity)
}

#[cfg(test)]
fn built_in_testnet_metadata() -> Result<Vec<u8>, SnapshotError> {
    let response: serde_json::Value = serde_json::from_str(TESTNET_SPEC_376_METADATA_RESPONSE)
        .map_err(|_| SnapshotError::InvalidBuiltInMetadata)?;
    let encoded = response["result"]
        .as_str()
        .ok_or(SnapshotError::InvalidBuiltInMetadata)?;
    hex::decode(encoded.trim_start_matches("0x")).map_err(|_| SnapshotError::InvalidBuiltInMetadata)
}

#[cfg(test)]
mod tests {
    use aws_lc_rs::digest::{SHA256, digest};
    use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
    use parity_scale_codec::{Decode, Encode};
    use rstest::rstest;
    use serde::Deserialize;
    use subxt_core::metadata;

    use super::*;

    #[derive(Deserialize)]
    struct RpcResponse {
        result: String,
    }

    fn spec_376_metadata_bytes() -> Vec<u8> {
        let response: RpcResponse =
            serde_json::from_str(TESTNET_SPEC_376_METADATA_RESPONSE).unwrap();
        hex::decode(response.result.trim_start_matches("0x")).unwrap()
    }

    #[rstest]
    fn built_in_runtime_matches_captured_spec_376_fixture() {
        let signing = RuntimeSnapshot::built_in_testnet().unwrap();
        let read_only = DeepXReadOnlyRuntimeSnapshot::built_in_testnet().unwrap();

        assert_eq!(signing.identity(), read_only.identity());
        assert_eq!(signing.identity().spec_version, TESTNET_MIN_SPEC_VERSION);
        assert_eq!(
            hex::encode(signing.identity().metadata_sha256),
            TESTNET_SPEC_376_METADATA_SHA256,
        );
        assert_eq!(signing.interfaces(), read_only.interfaces());
    }

    fn test_snapshot() -> RuntimeSnapshot {
        let metadata = metadata::decode_from(&spec_376_metadata_bytes()).unwrap();
        let identity = ApprovedRuntimeIdentity {
            environment: DeepXEnvironment::Testnet,
            genesis_hash: decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap(),
            metadata_sha256: decode_32(TESTNET_SPEC_376_METADATA_SHA256).unwrap(),
            spec_version: 376,
            transaction_version: TESTNET_TRANSACTION_VERSION,
            signed_extensions: TESTNET_SPEC_376_SIGNED_EXTENSIONS
                .iter()
                .map(|extension| (*extension).to_string())
                .collect(),
        };
        let interfaces = DeepXRuntimeInterfaceCatalog::from_metadata(&metadata).unwrap();
        let client_state = ClientState {
            metadata,
            genesis_hash: H256::from(identity.genesis_hash),
            runtime_version: RuntimeVersion {
                spec_version: identity.spec_version,
                transaction_version: identity.transaction_version,
            },
        };

        RuntimeSnapshot {
            identity,
            interfaces,
            client_state,
        }
    }

    #[rstest]
    fn public_testnet_constructors_accept_valid_metadata_without_a_fixture_hash() {
        let fixture = spec_376_metadata_bytes();
        let mut input = fixture.as_slice();
        let mut decoded = RuntimeMetadataPrefixed::decode(&mut input).unwrap();
        let RuntimeMetadata::V14(metadata) = &mut decoded.1 else {
            panic!("Expected V14 fixture metadata");
        };
        metadata
            .pallets
            .iter_mut()
            .find_map(|pallet| pallet.constants.first_mut())
            .unwrap()
            .docs
            .push("Updated runtime documentation".to_string());
        let changed = decoded.encode();
        let genesis = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();

        let signing = RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            genesis,
            377,
            2,
            &changed,
        )
        .unwrap();
        let read_only = DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            genesis,
            377,
            2,
            &changed,
        )
        .unwrap();

        assert_ne!(
            signing.identity().metadata_sha256,
            decode_32(TESTNET_SPEC_376_METADATA_SHA256).unwrap()
        );
        assert_eq!(signing.identity(), read_only.identity());
        assert_eq!(signing.identity().spec_version, 377);
        assert_eq!(signing.identity().transaction_version, 2);
    }

    #[rstest]
    fn read_only_runtime_rejects_invalid_identity_and_metadata() {
        let genesis_hash = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();
        let metadata = spec_376_metadata_bytes();

        for result in [
            DeepXReadOnlyRuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
                [0; 32],
                376,
                TESTNET_TRANSACTION_VERSION,
                &metadata,
            ),
            DeepXReadOnlyRuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
                genesis_hash,
                0,
                TESTNET_TRANSACTION_VERSION,
                &metadata,
            ),
            DeepXReadOnlyRuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
                genesis_hash,
                376,
                0,
                &metadata,
            ),
        ] {
            assert!(matches!(
                result,
                Err(SnapshotError::RuntimeIdentityMismatch)
            ));
        }
        assert!(matches!(
            DeepXReadOnlyRuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
                genesis_hash,
                376,
                TESTNET_TRANSACTION_VERSION,
                &metadata[..metadata.len() / 2],
            ),
            Err(SnapshotError::InvalidMetadata(_)),
        ));
    }

    #[rstest]
    fn rejects_malformed_observed_metadata() {
        let bytes = spec_376_metadata_bytes();

        assert!(matches!(
            DeepXReadOnlyRuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
                decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap(),
                376,
                TESTNET_TRANSACTION_VERSION,
                &bytes[..bytes.len() / 2],
            ),
            Err(SnapshotError::InvalidMetadata(_)),
        ));
    }

    #[rstest]
    fn observed_runtime_accepts_new_spec_without_a_metadata_allowlist() {
        let genesis = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();
        let metadata = spec_376_metadata_bytes();
        let snapshot = RuntimeSnapshot::observed_testnet(
            &DeepXEnvironment::Testnet,
            genesis,
            377,
            2,
            &metadata,
        )
        .unwrap();

        assert_eq!(snapshot.identity().genesis_hash, genesis);
        assert_eq!(snapshot.identity().spec_version, 377);
        assert_eq!(snapshot.identity().transaction_version, 2);
        assert_eq!(snapshot.client_state().runtime_version.spec_version, 377);
        assert_eq!(
            snapshot.client_state().runtime_version.transaction_version,
            2
        );
        assert_eq!(snapshot.identity().metadata_sha256, {
            let mut hash = [0; 32];
            hash.copy_from_slice(digest(&SHA256, &metadata).as_ref());
            hash
        });
        let read_only = DeepXReadOnlyRuntimeSnapshot::observed_testnet(
            &DeepXEnvironment::Testnet,
            genesis,
            377,
            2,
            &metadata,
        )
        .unwrap();
        assert_eq!(read_only.identity(), snapshot.identity());
        assert_eq!(read_only.interfaces(), snapshot.interfaces());
    }

    #[rstest]
    fn observed_runtime_rejects_another_genesis() {
        assert!(matches!(
            RuntimeSnapshot::observed_testnet(
                &DeepXEnvironment::Testnet,
                [0; 32],
                377,
                1,
                &spec_376_metadata_bytes(),
            ),
            Err(SnapshotError::RuntimeIdentityMismatch),
        ));
    }

    #[rstest]
    fn rejects_zero_runtime_version() {
        assert!(matches!(
            RuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
                decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap(),
                0,
                TESTNET_TRANSACTION_VERSION,
                &spec_376_metadata_bytes(),
            ),
            Err(SnapshotError::RuntimeIdentityMismatch),
        ));
    }

    #[rstest]
    #[case(DeepXEnvironment::Mainnet)]
    #[case(DeepXEnvironment::Unknown("staging".to_string()))]
    fn rejects_unapproved_deployments(#[case] environment: DeepXEnvironment) {
        assert!(matches!(
            RuntimeSnapshot::approved_testnet(
                &environment,
                decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap(),
                TESTNET_MIN_SPEC_VERSION,
                TESTNET_TRANSACTION_VERSION,
                &spec_376_metadata_bytes(),
            ),
            Err(SnapshotError::UnsupportedDeployment(_)),
        ));
    }

    #[rstest]
    fn runtime_change_blocks_new_signing_until_old_permits_finish() {
        let snapshot = test_snapshot();
        let service = DeepXRuntimeSnapshotService::new(snapshot.clone());
        let permit = service.acquire().unwrap();
        let mut changed_identity = snapshot.identity().clone();
        changed_identity.spec_version += 1;

        assert_eq!(
            service
                .observe_runtime_identity(changed_identity.clone())
                .unwrap(),
            DeepXRuntimeChangeDecision::RefreshRequired,
        );
        assert!(matches!(
            service.acquire(),
            Err(DeepXRuntimeSnapshotServiceError::RefreshInProgress),
        ));

        let mut replacement = snapshot;
        replacement.identity = changed_identity;
        assert_eq!(
            service.install(replacement.clone()),
            Err(DeepXRuntimeSnapshotServiceError::InFlightSigningPermits(1)),
        );

        drop(permit);
        service.install(replacement).unwrap();
        assert_eq!(
            service
                .acquire()
                .unwrap()
                .snapshot()
                .identity()
                .spec_version,
            377
        );
    }

    #[rstest]
    fn runtime_refresh_rejects_unobserved_and_mismatched_snapshots() {
        let snapshot = test_snapshot();
        let service = DeepXRuntimeSnapshotService::new(snapshot.clone());

        assert_eq!(
            service.install(snapshot.clone()),
            Err(DeepXRuntimeSnapshotServiceError::RefreshNotStarted),
        );

        let mut changed_identity = snapshot.identity().clone();
        changed_identity.transaction_version += 1;
        service.observe_runtime_identity(changed_identity).unwrap();
        assert_eq!(
            service.install(snapshot),
            Err(DeepXRuntimeSnapshotServiceError::SnapshotIdentityMismatch),
        );
        assert!(matches!(
            service.acquire(),
            Err(DeepXRuntimeSnapshotServiceError::RefreshInProgress),
        ));
    }

    #[rstest]
    fn validated_snapshot_application_is_idempotent() {
        let snapshot = test_snapshot();
        let service = DeepXRuntimeSnapshotService::new(snapshot.clone());

        assert_eq!(
            service.apply_validated(&snapshot).unwrap(),
            DeepXRuntimeSnapshotUpdate::Unchanged,
        );
    }

    #[rstest]
    fn inconsistent_evidence_cannot_be_cleared_by_snapshot_refresh() {
        let snapshot = test_snapshot();
        let service = DeepXRuntimeSnapshotService::new(snapshot.clone());
        let old_permit = service.acquire().unwrap();
        assert!(old_permit.ensure_active().is_ok());
        service.halt_for_inconsistent_evidence().unwrap();

        assert_eq!(
            old_permit.ensure_active(),
            Err(DeepXRuntimeSnapshotServiceError::EvidenceInconsistent),
        );
        assert_eq!(
            service.acquire().unwrap_err(),
            DeepXRuntimeSnapshotServiceError::EvidenceInconsistent,
        );
        assert_eq!(
            service.apply_validated(&snapshot),
            Err(DeepXRuntimeSnapshotServiceError::EvidenceInconsistent),
        );
        drop(old_permit);
        assert_eq!(
            service.acquire().unwrap_err(),
            DeepXRuntimeSnapshotServiceError::EvidenceInconsistent,
        );
    }

    #[rstest]
    fn retired_client_revokes_existing_and_future_runtime_permits() {
        let snapshot = test_snapshot();
        let service = DeepXRuntimeSnapshotService::new(snapshot.clone());
        let permit = service.acquire().unwrap();

        service.retire_client().unwrap();

        assert_eq!(
            permit.ensure_active(),
            Err(DeepXRuntimeSnapshotServiceError::ClientRetired),
        );
        assert_eq!(
            service.acquire().unwrap_err(),
            DeepXRuntimeSnapshotServiceError::ClientRetired,
        );
        assert_eq!(
            service.apply_validated(&snapshot),
            Err(DeepXRuntimeSnapshotServiceError::ClientRetired),
        );
        assert_eq!(
            service.install(snapshot),
            Err(DeepXRuntimeSnapshotServiceError::ClientRetired),
        );
    }

    #[rstest]
    fn account_reconnect_invalidates_old_permits_and_reopens_new_signing() {
        let service = DeepXRuntimeSnapshotService::new(test_snapshot());
        let old_permit = service.acquire().unwrap();

        service.suspend_for_account_disconnect().unwrap();

        assert_eq!(
            old_permit.ensure_active(),
            Err(DeepXRuntimeSnapshotServiceError::AccountConnectionUnavailable),
        );
        assert_eq!(
            service.acquire().unwrap_err(),
            DeepXRuntimeSnapshotServiceError::AccountConnectionUnavailable,
        );

        service.resume_after_account_reconnect().unwrap();

        assert_eq!(
            old_permit.ensure_active(),
            Err(DeepXRuntimeSnapshotServiceError::AccountConnectionUnavailable),
        );
        assert!(service.acquire().unwrap().ensure_active().is_ok());
    }

    #[rstest]
    fn validated_snapshot_application_waits_for_old_permits() {
        let snapshot = test_snapshot();
        let service = DeepXRuntimeSnapshotService::new(snapshot.clone());
        let permit = service.acquire().unwrap();
        let mut replacement = snapshot;
        replacement.identity.spec_version += 1;

        assert_eq!(
            service.apply_validated(&replacement),
            Err(DeepXRuntimeSnapshotServiceError::InFlightSigningPermits(1)),
        );
        assert!(matches!(
            service.acquire(),
            Err(DeepXRuntimeSnapshotServiceError::RefreshInProgress),
        ));

        drop(permit);
        assert_eq!(
            service.apply_validated(&replacement).unwrap(),
            DeepXRuntimeSnapshotUpdate::Installed,
        );
        assert_eq!(
            service.acquire().unwrap().snapshot().identity(),
            replacement.identity(),
        );
    }

    #[rstest]
    fn validated_snapshot_application_rejects_conflicting_pending_identity() {
        let snapshot = test_snapshot();
        let service = DeepXRuntimeSnapshotService::new(snapshot.clone());
        let permit = service.acquire().unwrap();
        let mut first_replacement = snapshot.clone();
        first_replacement.identity.spec_version += 1;
        let mut conflicting_replacement = snapshot;
        conflicting_replacement.identity.transaction_version += 1;

        assert_eq!(
            service.apply_validated(&first_replacement),
            Err(DeepXRuntimeSnapshotServiceError::InFlightSigningPermits(1)),
        );
        assert_eq!(
            service.apply_validated(&conflicting_replacement),
            Err(DeepXRuntimeSnapshotServiceError::ConflictingRuntimeChange),
        );

        drop(permit);
        assert_eq!(
            service.apply_validated(&first_replacement).unwrap(),
            DeepXRuntimeSnapshotUpdate::Installed,
        );
    }
}
