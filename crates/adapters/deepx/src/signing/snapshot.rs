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

use aws_lc_rs::digest::{SHA256, digest};
use nautilus_core::hex;
use scale_info::TypeDef;
use subxt_core::{
    client::{ClientState, RuntimeVersion},
    metadata,
    metadata::Metadata,
    utils::H256,
};
use thiserror::Error;

use super::DeepXRuntimeConfig;

const TESTNET_GENESIS_HASH: &str =
    "86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b";
const TESTNET_METADATA_SHA256: &str =
    "e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37";
const TESTNET_SPEC_VERSION: u32 = 366;
const TESTNET_TRANSACTION_VERSION: u32 = 1;
const TESTNET_SIGNED_EXTENSIONS: &[&str] = &[
    "CheckNonZeroSender",
    "CheckSpecVersion",
    "CheckTxVersion",
    "CheckGenesis",
    "CheckMortality",
    "CheckNonce",
    "CheckWeight",
    "ChargeTransactionPayment",
    "CheckPriority",
];

/// Runtime identity approved by the captured DeepX testnet fixture set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovedRuntimeIdentity {
    /// Testnet genesis hash.
    pub genesis_hash: [u8; 32],
    /// SHA-256 of the complete SCALE metadata bytes.
    pub metadata_sha256: [u8; 32],
    /// Runtime specification version.
    pub spec_version: u32,
    /// Runtime transaction version.
    pub transaction_version: u32,
    /// Signed extensions in metadata order.
    pub signed_extensions: Vec<String>,
}

/// Errors produced while validating an immutable runtime snapshot.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// The pinned identity constant could not be decoded.
    #[error("invalid built-in DeepX runtime identity")]
    InvalidApprovedIdentity,
    /// The SCALE metadata cannot be decoded by the pinned DeepX Subxt fork.
    #[error("invalid DeepX runtime metadata: {0}")]
    InvalidMetadata(#[from] parity_scale_codec::Error),
    /// The metadata does not match the approved testnet SHA-256.
    #[error("DeepX runtime metadata hash is not approved")]
    MetadataHashMismatch,
    /// The observed deployment or runtime version differs from the approved fixture.
    #[error("DeepX genesis or runtime version is not approved")]
    RuntimeIdentityMismatch,
    /// The signed-extension sequence differs from the approved fixture.
    #[error("DeepX runtime signed-extension sequence is not approved")]
    SignedExtensionsMismatch,
    /// An unknown non-empty transaction extension would make signing ambiguous.
    #[error("unsupported non-empty DeepX transaction extension: {0}")]
    UnsupportedTransactionExtension(String),
}

/// Decision produced after comparing an observed runtime identity with the active snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXRuntimeChangeDecision {
    /// The observed identity matches the active immutable snapshot.
    Unchanged,
    /// New signing is blocked until the observed identity is validated and installed.
    RefreshRequired,
}

/// Errors produced by the runtime snapshot quiescence boundary.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXRuntimeSnapshotServiceError {
    /// New signing is blocked while a changed runtime is being validated.
    #[error("DeepX runtime snapshot refresh is in progress")]
    RefreshInProgress,
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
    client_state: ClientState<DeepXRuntimeConfig>,
}

#[derive(Debug)]
struct RuntimeSnapshotServiceState {
    active: Arc<RuntimeSnapshot>,
    pending_identity: Option<ApprovedRuntimeIdentity>,
    in_flight: usize,
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
            })),
        }
    }

    /// Acquires the immutable snapshot used for one in-flight signing operation.
    ///
    /// # Errors
    ///
    /// Returns an error while runtime refresh is in progress or shared state is unavailable.
    pub fn acquire(&self) -> Result<DeepXRuntimeSnapshotPermit, DeepXRuntimeSnapshotServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
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
        })
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
            return if pending == &observed {
                Ok(DeepXRuntimeChangeDecision::RefreshRequired)
            } else {
                Err(DeepXRuntimeSnapshotServiceError::ConflictingRuntimeChange)
            };
        }
        if state.active.identity() == &observed {
            return Ok(DeepXRuntimeChangeDecision::Unchanged);
        }

        state.pending_identity = Some(observed);
        Ok(DeepXRuntimeChangeDecision::RefreshRequired)
    }

    /// Installs a fixture-validated replacement after all old signing permits are released.
    ///
    /// # Errors
    ///
    /// Returns an error unless refresh is active, the snapshot matches the observed identity, no
    /// old signing permits remain, and shared state is available.
    pub fn install(
        &self,
        snapshot: RuntimeSnapshot,
    ) -> Result<(), DeepXRuntimeSnapshotServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        let pending = state
            .pending_identity
            .as_ref()
            .ok_or(DeepXRuntimeSnapshotServiceError::RefreshNotStarted)?;
        if pending != snapshot.identity() {
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
}

/// In-flight ownership of one immutable runtime snapshot.
#[derive(Debug)]
pub struct DeepXRuntimeSnapshotPermit {
    service_state: Arc<Mutex<RuntimeSnapshotServiceState>>,
    snapshot: Arc<RuntimeSnapshot>,
}

impl DeepXRuntimeSnapshotPermit {
    /// Returns the immutable snapshot retained for this signing operation.
    #[must_use]
    pub fn snapshot(&self) -> &RuntimeSnapshot {
        &self.snapshot
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
    /// Builds the only runtime identity currently approved for DeepX testnet signing.
    ///
    /// # Errors
    ///
    /// Returns an error unless the observed genesis, runtime versions, metadata hash, extension
    /// order, and extension encodings match the captured finalized fixture.
    pub fn approved_testnet(
        observed_genesis_hash: [u8; 32],
        observed_spec_version: u32,
        observed_transaction_version: u32,
        metadata_bytes: &[u8],
    ) -> Result<Self, SnapshotError> {
        let genesis_hash = decode_32(TESTNET_GENESIS_HASH)?;
        if observed_genesis_hash != genesis_hash
            || observed_spec_version != TESTNET_SPEC_VERSION
            || observed_transaction_version != TESTNET_TRANSACTION_VERSION
        {
            return Err(SnapshotError::RuntimeIdentityMismatch);
        }
        let approved_metadata_hash = decode_32(TESTNET_METADATA_SHA256)?;
        let actual_metadata_hash: [u8; 32] = digest(&SHA256, metadata_bytes)
            .as_ref()
            .try_into()
            .map_err(|_| SnapshotError::InvalidApprovedIdentity)?;
        if actual_metadata_hash != approved_metadata_hash {
            return Err(SnapshotError::MetadataHashMismatch);
        }

        let metadata = metadata::decode_from(metadata_bytes)?;
        let extension_metadata = metadata
            .extrinsic()
            .transaction_extensions_to_use_for_encoding()
            .collect::<Vec<_>>();
        let signed_extensions = extension_metadata
            .iter()
            .map(|extension| extension.identifier().to_string())
            .collect::<Vec<_>>();
        if signed_extensions != TESTNET_SIGNED_EXTENSIONS {
            return Err(SnapshotError::SignedExtensionsMismatch);
        }
        validate_unknown_extensions(&metadata)?;

        let identity = ApprovedRuntimeIdentity {
            genesis_hash,
            metadata_sha256: actual_metadata_hash,
            spec_version: TESTNET_SPEC_VERSION,
            transaction_version: TESTNET_TRANSACTION_VERSION,
            signed_extensions,
        };
        let client_state = ClientState {
            metadata,
            genesis_hash: H256::from(genesis_hash),
            runtime_version: RuntimeVersion {
                spec_version: TESTNET_SPEC_VERSION,
                transaction_version: TESTNET_TRANSACTION_VERSION,
            },
        };

        Ok(Self {
            identity,
            client_state,
        })
    }

    /// Returns the approved identity associated with this snapshot.
    #[must_use]
    pub const fn identity(&self) -> &ApprovedRuntimeIdentity {
        &self.identity
    }

    pub(super) const fn client_state(&self) -> &ClientState<DeepXRuntimeConfig> {
        &self.client_state
    }
}

fn validate_unknown_extensions(metadata: &Metadata) -> Result<(), SnapshotError> {
    const SUPPORTED: &[&str] = &[
        "CheckSpecVersion",
        "CheckTxVersion",
        "CheckNonce",
        "CheckGenesis",
        "CheckMortality",
        "ChargeAssetTxPayment",
        "ChargeTransactionPayment",
        "CheckMetadataHash",
    ];

    for extension in metadata
        .extrinsic()
        .transaction_extensions_to_use_for_encoding()
    {
        if !SUPPORTED.contains(&extension.identifier())
            && (!is_empty_type(extension.extra_ty(), metadata)
                || !is_empty_type(extension.additional_ty(), metadata))
        {
            return Err(SnapshotError::UnsupportedTransactionExtension(
                extension.identifier().to_string(),
            ));
        }
    }
    Ok(())
}

fn is_empty_type(type_id: u32, metadata: &Metadata) -> bool {
    let Some(ty) = metadata.types().resolve(type_id) else {
        return false;
    };
    match &ty.type_def {
        TypeDef::Composite(value) => value
            .fields
            .iter()
            .all(|field| is_empty_type(field.ty.id, metadata)),
        TypeDef::Array(value) => value.len == 0 || is_empty_type(value.type_param.id, metadata),
        TypeDef::Tuple(value) => value
            .fields
            .iter()
            .all(|field| is_empty_type(field.id, metadata)),
        TypeDef::BitSequence(_)
        | TypeDef::Variant(_)
        | TypeDef::Sequence(_)
        | TypeDef::Compact(_)
        | TypeDef::Primitive(_) => false,
    }
}

fn decode_32(value: &str) -> Result<[u8; 32], SnapshotError> {
    hex::decode_array(value).map_err(|_| SnapshotError::InvalidApprovedIdentity)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde::Deserialize;

    use super::*;

    #[derive(Deserialize)]
    struct RpcResponse {
        result: String,
    }

    fn metadata_bytes() -> Vec<u8> {
        let response: RpcResponse = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json",
        )))
        .unwrap();
        hex::decode(response.result.trim_start_matches("0x")).unwrap()
    }

    #[rstest]
    fn accepts_the_approved_finalized_testnet_metadata() {
        let snapshot = RuntimeSnapshot::approved_testnet(
            decode_32(TESTNET_GENESIS_HASH).unwrap(),
            TESTNET_SPEC_VERSION,
            TESTNET_TRANSACTION_VERSION,
            &metadata_bytes(),
        )
        .unwrap();

        assert_eq!(snapshot.identity().spec_version, 366);
        assert_eq!(snapshot.identity().transaction_version, 1);
        assert_eq!(
            snapshot.identity().signed_extensions,
            TESTNET_SIGNED_EXTENSIONS
        );
    }

    #[rstest]
    fn rejects_metadata_outside_the_approved_fixture_identity() {
        let mut bytes = metadata_bytes();
        bytes[100] ^= 1;

        assert!(matches!(
            RuntimeSnapshot::approved_testnet(
                decode_32(TESTNET_GENESIS_HASH).unwrap(),
                TESTNET_SPEC_VERSION,
                TESTNET_TRANSACTION_VERSION,
                &bytes,
            ),
            Err(SnapshotError::MetadataHashMismatch),
        ));
    }

    #[rstest]
    fn rejects_runtime_versions_outside_the_approved_fixture_identity() {
        assert!(matches!(
            RuntimeSnapshot::approved_testnet(
                decode_32(TESTNET_GENESIS_HASH).unwrap(),
                TESTNET_SPEC_VERSION + 1,
                TESTNET_TRANSACTION_VERSION,
                &metadata_bytes(),
            ),
            Err(SnapshotError::RuntimeIdentityMismatch),
        ));
    }

    #[rstest]
    fn runtime_change_blocks_new_signing_until_old_permits_finish() {
        let snapshot = RuntimeSnapshot::approved_testnet(
            decode_32(TESTNET_GENESIS_HASH).unwrap(),
            TESTNET_SPEC_VERSION,
            TESTNET_TRANSACTION_VERSION,
            &metadata_bytes(),
        )
        .unwrap();
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
            367
        );
    }

    #[rstest]
    fn runtime_refresh_rejects_unobserved_and_mismatched_snapshots() {
        let snapshot = RuntimeSnapshot::approved_testnet(
            decode_32(TESTNET_GENESIS_HASH).unwrap(),
            TESTNET_SPEC_VERSION,
            TESTNET_TRANSACTION_VERSION,
            &metadata_bytes(),
        )
        .unwrap();
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
}
