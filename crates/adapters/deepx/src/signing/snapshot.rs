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

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use aws_lc_rs::digest::{SHA256, digest};
use nautilus_core::hex;
use scale_info::{TypeDef, Variant, form::PortableForm};
use subxt_core::{
    client::{ClientState, RuntimeVersion},
    metadata,
    metadata::Metadata,
    utils::H256,
};
use thiserror::Error;

use super::DeepXRuntimeConfig;
use crate::common::DeepXEnvironment;

const TESTNET_MIN_SPEC_VERSION: u32 = 376;
const TESTNET_SPEC_376_GENESIS_HASH: &str =
    "87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7";
const TESTNET_SPEC_376_METADATA_SHA256: &str =
    "57f1bedf1e04522176a2b422af9249d5c990693fb92d11327870d8ef751a8447";
const TESTNET_TRANSACTION_VERSION: u32 = 1;
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

#[derive(Clone, Copy)]
struct ApprovedTestnetRuntime {
    genesis_hash: &'static str,
    metadata_sha256: &'static str,
    signed_extensions: &'static [&'static str],
}

/// Runtime identity approved by the captured DeepX testnet fixture set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovedRuntimeIdentity {
    /// DeepX deployment associated with the fixture set.
    pub environment: DeepXEnvironment,
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
    /// Runtime snapshots are currently approved only for DeepX testnet.
    #[error("unsupported DeepX runtime deployment: {0}")]
    UnsupportedDeployment(String),
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
    /// The approved metadata contains an inconsistent pallet, call, event, or error identity.
    #[error(transparent)]
    InvalidRuntimeInterface(#[from] DeepXRuntimeInterfaceError),
}

/// Errors produced while constructing or querying a runtime interface catalog.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXRuntimeInterfaceError {
    /// A pallet name or index is duplicated in the runtime metadata.
    #[error("duplicate DeepX runtime pallet identity: {0}")]
    DuplicatePallet(String),
    /// A call name or index is duplicated within a pallet.
    #[error("duplicate DeepX runtime call identity: {0}.{1}")]
    DuplicateCall(String, String),
    /// An event name or index is duplicated within a pallet.
    #[error("duplicate DeepX runtime event identity: {0}.{1}")]
    DuplicateEvent(String, String),
    /// An error name or index is duplicated within a pallet.
    #[error("duplicate DeepX runtime error identity: {0}.{1}")]
    DuplicateError(String, String),
    /// The requested SCALE pallet index is absent from the approved runtime metadata.
    #[error("DeepX runtime pallet index is unavailable: {0}")]
    PalletIndexUnavailable(u8),
    /// The requested SCALE error index is absent from the specified pallet.
    #[error("DeepX runtime error index is unavailable: {0}.{1}")]
    ErrorIndexUnavailable(u8, u8),
    /// The requested pallet is absent from the approved runtime metadata.
    #[error("DeepX runtime pallet is unavailable: {0}")]
    PalletUnavailable(String),
    /// The requested call is absent from the approved runtime metadata.
    #[error("DeepX runtime call is unavailable: {0}.{1}")]
    CallUnavailable(String, String),
    /// The requested event is absent from the approved runtime metadata.
    #[error("DeepX runtime event is unavailable: {0}.{1}")]
    EventUnavailable(String, String),
}

/// Immutable metadata identity for one runtime call, event, or error variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXRuntimeVariantIdentity {
    /// Variant name declared by the runtime metadata.
    name: String,
    /// SCALE variant index declared by the runtime metadata.
    index: u8,
}

impl DeepXRuntimeVariantIdentity {
    /// Returns the variant name declared by the runtime metadata.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the SCALE variant index declared by the runtime metadata.
    #[must_use]
    pub const fn index(&self) -> u8 {
        self.index
    }
}

/// Immutable metadata identity and variants for one runtime pallet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXRuntimePalletInterface {
    /// Pallet name declared by the runtime metadata.
    name: String,
    /// SCALE pallet index declared by the runtime metadata.
    index: u8,
    /// Call variants declared by the runtime metadata.
    calls: Vec<DeepXRuntimeVariantIdentity>,
    /// Event variants declared by the runtime metadata.
    events: Vec<DeepXRuntimeVariantIdentity>,
    /// Error variants declared by the runtime metadata.
    errors: Vec<DeepXRuntimeVariantIdentity>,
}

impl DeepXRuntimePalletInterface {
    /// Returns the pallet name declared by the runtime metadata.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the SCALE pallet index declared by the runtime metadata.
    #[must_use]
    pub const fn index(&self) -> u8 {
        self.index
    }

    /// Returns the call variants declared by the runtime metadata.
    #[must_use]
    pub fn calls(&self) -> &[DeepXRuntimeVariantIdentity] {
        &self.calls
    }

    /// Returns the event variants declared by the runtime metadata.
    #[must_use]
    pub fn events(&self) -> &[DeepXRuntimeVariantIdentity] {
        &self.events
    }

    /// Returns the error variants declared by the runtime metadata.
    #[must_use]
    pub fn errors(&self) -> &[DeepXRuntimeVariantIdentity] {
        &self.errors
    }
}

/// Immutable pallet, call, event, and error identities from approved runtime metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXRuntimeInterfaceCatalog {
    pallets: Vec<DeepXRuntimePalletInterface>,
}

impl DeepXRuntimeInterfaceCatalog {
    fn from_metadata(metadata: &Metadata) -> Result<Self, DeepXRuntimeInterfaceError> {
        let mut pallet_names = BTreeSet::new();
        let mut pallet_indices = BTreeSet::new();
        let mut pallets = Vec::with_capacity(metadata.pallets().len());

        for pallet in metadata.pallets() {
            if !pallet_names.insert(pallet.name()) || !pallet_indices.insert(pallet.index()) {
                return Err(DeepXRuntimeInterfaceError::DuplicatePallet(
                    pallet.name().to_string(),
                ));
            }
            let calls = collect_variants(
                pallet.name(),
                pallet.call_variants().unwrap_or_default(),
                DeepXRuntimeInterfaceError::DuplicateCall,
            )?;
            let events = collect_variants(
                pallet.name(),
                pallet.event_variants().unwrap_or_default(),
                DeepXRuntimeInterfaceError::DuplicateEvent,
            )?;
            let errors = collect_variants(
                pallet.name(),
                pallet.error_variants().unwrap_or_default(),
                DeepXRuntimeInterfaceError::DuplicateError,
            )?;
            pallets.push(DeepXRuntimePalletInterface {
                name: pallet.name().to_string(),
                index: pallet.index(),
                calls,
                events,
                errors,
            });
        }

        Ok(Self { pallets })
    }

    /// Returns all pallet interfaces in metadata order.
    #[must_use]
    pub fn pallets(&self) -> &[DeepXRuntimePalletInterface] {
        &self.pallets
    }

    /// Returns a pallet identity from the approved runtime metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the pallet is absent.
    pub fn pallet(
        &self,
        pallet: &str,
    ) -> Result<&DeepXRuntimePalletInterface, DeepXRuntimeInterfaceError> {
        self.pallets
            .iter()
            .find(|candidate| candidate.name == pallet)
            .ok_or_else(|| DeepXRuntimeInterfaceError::PalletUnavailable(pallet.to_string()))
    }

    /// Returns a call identity from the approved runtime metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the pallet or call is absent.
    pub fn call(
        &self,
        pallet: &str,
        call: &str,
    ) -> Result<&DeepXRuntimeVariantIdentity, DeepXRuntimeInterfaceError> {
        self.pallet(pallet)?
            .calls
            .iter()
            .find(|candidate| candidate.name == call)
            .ok_or_else(|| {
                DeepXRuntimeInterfaceError::CallUnavailable(pallet.to_string(), call.to_string())
            })
    }

    /// Returns pallet and error identities by their declared SCALE indices.
    ///
    /// This lookup does not decode a `DispatchError` or classify a transaction outcome.
    ///
    /// # Errors
    ///
    /// Returns distinct errors when the pallet index or its error index is absent.
    pub fn error_by_index(
        &self,
        pallet_index: u8,
        error_index: u8,
    ) -> Result<
        (&DeepXRuntimePalletInterface, &DeepXRuntimeVariantIdentity),
        DeepXRuntimeInterfaceError,
    > {
        let pallet = self
            .pallets
            .iter()
            .find(|candidate| candidate.index == pallet_index)
            .ok_or(DeepXRuntimeInterfaceError::PalletIndexUnavailable(
                pallet_index,
            ))?;
        let error = pallet
            .errors
            .iter()
            .find(|candidate| candidate.index == error_index)
            .ok_or(DeepXRuntimeInterfaceError::ErrorIndexUnavailable(
                pallet_index,
                error_index,
            ))?;
        Ok((pallet, error))
    }

    /// Returns an event identity from the approved runtime metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the pallet or event is absent.
    pub fn event(
        &self,
        pallet: &str,
        event: &str,
    ) -> Result<&DeepXRuntimeVariantIdentity, DeepXRuntimeInterfaceError> {
        self.pallet(pallet)?
            .events
            .iter()
            .find(|candidate| candidate.name == event)
            .ok_or_else(|| {
                DeepXRuntimeInterfaceError::EventUnavailable(pallet.to_string(), event.to_string())
            })
    }
}

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
    evidence_inconsistent: bool,
    client_retired: bool,
}

#[derive(Debug)]
enum PendingRuntimeIdentity {
    Approved(ApprovedRuntimeIdentity),
    Observed {
        environment: DeepXEnvironment,
        genesis_hash: [u8; 32],
        spec_version: u32,
        transaction_version: u32,
        metadata_sha256: Option<[u8; 32]>,
    },
}

impl PendingRuntimeIdentity {
    fn matches(&self, identity: &ApprovedRuntimeIdentity) -> bool {
        match self {
            Self::Approved(approved) => approved == identity,
            Self::Observed {
                environment,
                genesis_hash,
                spec_version,
                transaction_version,
                metadata_sha256,
            } => {
                environment == &identity.environment
                    && genesis_hash == &identity.genesis_hash
                    && spec_version == &identity.spec_version
                    && transaction_version == &identity.transaction_version
                    && metadata_sha256.is_none_or(|hash| hash == identity.metadata_sha256)
            }
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
        })
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

    /// Latches runtime change evidence before metadata decoding or fixture approval.
    pub(crate) fn observe_runtime_fingerprint(
        &self,
        environment: &DeepXEnvironment,
        genesis_hash: [u8; 32],
        spec_version: u32,
        transaction_version: u32,
        metadata_bytes: Option<&[u8]>,
    ) -> Result<(), DeepXRuntimeSnapshotServiceError> {
        let metadata_sha256 = metadata_bytes.map(|bytes| {
            let mut hash = [0; 32];
            hash.copy_from_slice(digest(&SHA256, bytes).as_ref());
            hash
        });
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;
        let matches = |identity: &ApprovedRuntimeIdentity| {
            environment == &identity.environment
                && genesis_hash == identity.genesis_hash
                && spec_version == identity.spec_version
                && transaction_version == identity.transaction_version
                && metadata_sha256.is_none_or(|hash| hash == identity.metadata_sha256)
        };

        if let Some(pending) = &mut state.pending_identity {
            match pending {
                PendingRuntimeIdentity::Approved(identity) if matches(identity) => return Ok(()),
                PendingRuntimeIdentity::Observed {
                    environment: pending_environment,
                    genesis_hash: pending_genesis,
                    spec_version: pending_spec,
                    transaction_version: pending_transaction,
                    metadata_sha256: pending_hash,
                } if environment == pending_environment
                    && genesis_hash == *pending_genesis
                    && spec_version == *pending_spec
                    && transaction_version == *pending_transaction
                    && (metadata_sha256.is_none()
                        || pending_hash.is_none()
                        || metadata_sha256 == *pending_hash) =>
                {
                    if metadata_sha256.is_some() {
                        *pending_hash = metadata_sha256;
                    }
                    return Ok(());
                }
                _ => return Err(DeepXRuntimeSnapshotServiceError::ConflictingRuntimeChange),
            }
        }

        if !matches(state.active.identity()) {
            state.pending_identity = Some(PendingRuntimeIdentity::Observed {
                environment: environment.clone(),
                genesis_hash,
                spec_version,
                transaction_version,
                metadata_sha256,
            });
        }
        Ok(())
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

    /// Builds a runtime identity currently approved for DeepX testnet signing.
    ///
    /// # Errors
    ///
    /// Returns an error unless the observed genesis, runtime versions, metadata hash, extension
    /// order, and extension encodings match the captured finalized fixture.
    pub fn approved_testnet(
        environment: &DeepXEnvironment,
        observed_genesis_hash: [u8; 32],
        observed_spec_version: u32,
        observed_transaction_version: u32,
        metadata_bytes: &[u8],
    ) -> Result<Self, SnapshotError> {
        let approved_runtime = approved_testnet_runtime(observed_spec_version)?;
        let (identity, interfaces, metadata) = validate_testnet_runtime_snapshot(
            environment,
            observed_genesis_hash,
            observed_spec_version,
            observed_transaction_version,
            metadata_bytes,
            approved_runtime,
        )?;
        let client_state = ClientState {
            metadata,
            genesis_hash: H256::from(identity.genesis_hash),
            runtime_version: RuntimeVersion {
                spec_version: observed_spec_version,
                transaction_version: TESTNET_TRANSACTION_VERSION,
            },
        };

        Ok(Self {
            identity,
            interfaces,
            client_state,
        })
    }

    /// Returns the approved identity associated with this snapshot.
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
    /// Builds a fixture-approved DeepX testnet snapshot which cannot authorize signing.
    ///
    /// # Errors
    ///
    /// Returns an error unless the observed genesis, runtime versions, metadata hash, extension
    /// order, and extension encodings match a captured finalized read-only fixture.
    pub fn approved_testnet(
        environment: &DeepXEnvironment,
        observed_genesis_hash: [u8; 32],
        observed_spec_version: u32,
        observed_transaction_version: u32,
        metadata_bytes: &[u8],
    ) -> Result<Self, SnapshotError> {
        let approved_runtime = approved_testnet_runtime(observed_spec_version)?;
        let (identity, interfaces, metadata) = validate_testnet_runtime_snapshot(
            environment,
            observed_genesis_hash,
            observed_spec_version,
            observed_transaction_version,
            metadata_bytes,
            approved_runtime,
        )?;
        Ok(Self {
            identity,
            interfaces,
            metadata,
        })
    }

    /// Returns the fixture-approved identity associated with this read-only snapshot.
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

fn approved_testnet_runtime(
    spec_version: u32,
) -> Result<ApprovedTestnetRuntime, SnapshotError> {
    if spec_version < TESTNET_MIN_SPEC_VERSION {
        return Err(SnapshotError::RuntimeIdentityMismatch);
    }

    Ok(ApprovedTestnetRuntime {
        genesis_hash: TESTNET_SPEC_376_GENESIS_HASH,
        metadata_sha256: TESTNET_SPEC_376_METADATA_SHA256,
        signed_extensions: TESTNET_SPEC_376_SIGNED_EXTENSIONS,
    })
}

fn validate_testnet_runtime_snapshot(
    environment: &DeepXEnvironment,
    observed_genesis_hash: [u8; 32],
    observed_spec_version: u32,
    observed_transaction_version: u32,
    metadata_bytes: &[u8],
    approved_runtime: ApprovedTestnetRuntime,
) -> Result<
    (
        ApprovedRuntimeIdentity,
        DeepXRuntimeInterfaceCatalog,
        Metadata,
    ),
    SnapshotError,
> {
    if !environment.is_testnet() {
        return Err(SnapshotError::UnsupportedDeployment(
            environment.to_string(),
        ));
    }
    let genesis_hash = decode_32(approved_runtime.genesis_hash)?;
    if observed_genesis_hash != genesis_hash
        || observed_transaction_version != TESTNET_TRANSACTION_VERSION
    {
        return Err(SnapshotError::RuntimeIdentityMismatch);
    }
    let approved_metadata_hash = decode_32(approved_runtime.metadata_sha256)?;
    let actual_metadata_hash: [u8; 32] = digest(&SHA256, metadata_bytes)
        .as_ref()
        .try_into()
        .map_err(|_| SnapshotError::InvalidApprovedIdentity)?;
    if actual_metadata_hash != approved_metadata_hash {
        return Err(SnapshotError::MetadataHashMismatch);
    }

    let metadata = metadata::decode_from(metadata_bytes)?;
    let signed_extensions = metadata
        .extrinsic()
        .transaction_extensions_to_use_for_encoding()
        .map(|extension| extension.identifier().to_string())
        .collect::<Vec<_>>();
    if signed_extensions != approved_runtime.signed_extensions {
        return Err(SnapshotError::SignedExtensionsMismatch);
    }
    validate_unknown_extensions(&metadata)?;
    let interfaces = DeepXRuntimeInterfaceCatalog::from_metadata(&metadata)?;
    let identity = ApprovedRuntimeIdentity {
        environment: environment.clone(),
        genesis_hash,
        metadata_sha256: actual_metadata_hash,
        spec_version: observed_spec_version,
        transaction_version: TESTNET_TRANSACTION_VERSION,
        signed_extensions,
    };

    Ok((identity, interfaces, metadata))
}

fn collect_variants(
    pallet: &str,
    variants: &[Variant<PortableForm>],
    duplicate_error: fn(String, String) -> DeepXRuntimeInterfaceError,
) -> Result<Vec<DeepXRuntimeVariantIdentity>, DeepXRuntimeInterfaceError> {
    let mut names = BTreeSet::new();
    let mut indices = BTreeSet::new();
    let mut identities = Vec::with_capacity(variants.len());
    for variant in variants {
        if !names.insert(variant.name.clone()) || !indices.insert(variant.index) {
            return Err(duplicate_error(pallet.to_string(), variant.name.clone()));
        }
        identities.push(DeepXRuntimeVariantIdentity {
            name: variant.name.clone(),
            index: variant.index,
        });
    }
    Ok(identities)
}

fn validate_unknown_extensions(metadata: &Metadata) -> Result<(), SnapshotError> {
    const SUPPORTED: &[&str] = &[
        "CheckSpecVersion",
        "CheckTxVersion",
        "CheckNonce",
        "CheckGenesis",
        "CheckMortality",
        "CheckNonceEra",
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
    use frame_metadata::{
        META_RESERVED, RuntimeMetadata, RuntimeMetadataPrefixed,
        v14::{StorageEntryModifier, StorageEntryType, StorageHasher},
    };
    use parity_scale_codec::{Decode, Encode};
    use rstest::rstest;
    use scale_info::{TypeDefPrimitive, form::PortableForm};
    use serde::Deserialize;

    use super::*;

    const SPEC_376_METADATA_RESPONSE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test_data/runtime/testnet/",
        "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
    ));

    #[derive(Deserialize)]
    struct RpcResponse {
        result: String,
    }

    fn spec_376_metadata_bytes() -> Vec<u8> {
        let response: RpcResponse = serde_json::from_str(SPEC_376_METADATA_RESPONSE).unwrap();
        hex::decode(response.result.trim_start_matches("0x")).unwrap()
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
    #[case(376)]
    #[case(377)]
    #[case(u32::MAX)]
    fn signing_policy_approves_spec_376_and_later(#[case] spec_version: u32) {
        let metadata = spec_376_metadata_bytes();
        let genesis = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();

        let signing = RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            genesis,
            spec_version,
            TESTNET_TRANSACTION_VERSION,
            &metadata,
        )
        .unwrap();
        assert_eq!(signing.identity().spec_version, spec_version);
        assert_eq!(signing.identity().signed_extensions[4], "CheckNonceEra");
        assert_eq!(
            hex::encode(signing.identity().metadata_sha256),
            TESTNET_SPEC_376_METADATA_SHA256,
        );
    }

    #[rstest]
    #[case(365)]
    #[case(366)]
    #[case(367)]
    #[case(368)]
    #[case(369)]
    #[case(370)]
    #[case(371)]
    #[case(372)]
    #[case(375)]
    fn signing_policy_rejects_versions_before_376(#[case] spec_version: u32) {
        assert!(matches!(
            approved_testnet_runtime(spec_version),
            Err(SnapshotError::RuntimeIdentityMismatch),
        ));
    }

    #[rstest]
    fn spec_376_read_only_approval_rejects_identity_drift() {
        let genesis_hash = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();
        let metadata = spec_376_metadata_bytes();
        let mut changed_metadata = metadata.clone();
        changed_metadata[100] ^= 1;

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
                375,
                TESTNET_TRANSACTION_VERSION,
                &metadata,
            ),
            DeepXReadOnlyRuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
                genesis_hash,
                376,
                2,
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
                &changed_metadata,
            ),
            Err(SnapshotError::MetadataHashMismatch),
        ));
    }

    #[rstest]
    fn spec_376_perp_position_storage_metadata_layout_is_exact() {
        let bytes = spec_376_metadata_bytes();
        let mut input = bytes.as_slice();
        let RuntimeMetadataPrefixed(prefix, RuntimeMetadata::V14(metadata)) =
            RuntimeMetadataPrefixed::decode(&mut input).unwrap()
        else {
            panic!("Expected V14 fixture metadata");
        };
        assert_eq!(prefix, META_RESERVED);
        assert!(input.is_empty());
        let perp_market = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.name == "PerpMarket")
            .unwrap();
        let perp_storage = perp_market.storage.as_ref().unwrap();
        assert_eq!(perp_storage.prefix, "PerpMarket");
        let positions = perp_storage
            .entries
            .iter()
            .find(|entry| entry.name == "UserPerpPositions")
            .unwrap();
        assert_eq!(positions.modifier, StorageEntryModifier::Default);
        let StorageEntryType::Map {
            hashers,
            key,
            value,
        } = &positions.ty
        else {
            panic!("UserPerpPositions must be a storage map");
        };
        assert!(matches!(hashers.as_slice(), [StorageHasher::Identity]));
        assert_fixed_u8_array(&metadata.types, key.id, 20, Some("H160"));
        assert_eq!(positions.default, [0]);

        let position_sequence = metadata.types.resolve(value.id).unwrap();
        let TypeDef::Sequence(position_sequence) = &position_sequence.type_def else {
            panic!("UserPerpPositions value must be Vec<PerpPosition>");
        };
        let position = metadata
            .types
            .resolve(position_sequence.type_param.id)
            .unwrap();
        assert_eq!(
            position.path.segments.last().map(String::as_str),
            Some("PerpPosition")
        );
        let TypeDef::Composite(position) = &position.type_def else {
            panic!("PerpPosition must be a composite type");
        };
        let expected_fields = [
            ("market_id", TypeDefPrimitive::U16),
            ("is_long", TypeDefPrimitive::Bool),
            ("base_asset_amount", TypeDefPrimitive::U128),
            ("entry_price", TypeDefPrimitive::U128),
            ("leverage", TypeDefPrimitive::U64),
            ("last_funding_rate", TypeDefPrimitive::I128),
            ("version", TypeDefPrimitive::U64),
            ("realized_pnl", TypeDefPrimitive::I128),
            ("funding_payment", TypeDefPrimitive::I128),
        ];
        assert_eq!(position.fields.len(), 13);
        for (field, (expected_name, expected_type)) in position.fields.iter().zip(expected_fields) {
            assert_eq!(field.name.as_deref(), Some(expected_name));
            assert_primitive(&metadata.types, field.ty.id, expected_type);
        }
        assert_eq!(position.fields[9].name.as_deref(), Some("owner"));
        assert_fixed_u8_array(&metadata.types, position.fields[9].ty.id, 20, Some("H160"));
        assert_eq!(position.fields[10].name.as_deref(), Some("take_profit"));
        assert_option_primitive(
            &metadata.types,
            position.fields[10].ty.id,
            TypeDefPrimitive::U128,
        );
        assert_eq!(position.fields[11].name.as_deref(), Some("stop_loss"));
        assert_option_primitive(
            &metadata.types,
            position.fields[11].ty.id,
            TypeDefPrimitive::U128,
        );
        assert_eq!(
            position.fields[12].name.as_deref(),
            Some("last_settle_price")
        );
        assert_primitive(
            &metadata.types,
            position.fields[12].ty.id,
            TypeDefPrimitive::U128,
        );

        let subaccount = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.name == "Subaccount")
            .unwrap();
        let subaccount_storage = subaccount.storage.as_ref().unwrap();
        assert_eq!(subaccount_storage.prefix, "Subaccount");
        let maximum = subaccount_storage
            .entries
            .iter()
            .find(|entry| entry.name == "MaxPerpPositions")
            .unwrap();
        assert_eq!(maximum.modifier, StorageEntryModifier::Default);
        let StorageEntryType::Plain(maximum_ty) = &maximum.ty else {
            panic!("MaxPerpPositions must be plain storage");
        };
        assert_primitive(&metadata.types, maximum_ty.id, TypeDefPrimitive::U32);
        assert_eq!(maximum.default, 8_u32.encode());
    }

    #[rstest]
    fn spec_376_perp_order_storage_metadata_layout_is_exact() {
        let RuntimeMetadataPrefixed(_, RuntimeMetadata::V14(metadata)) =
            RuntimeMetadataPrefixed::decode(&mut spec_376_metadata_bytes().as_slice()).unwrap()
        else {
            panic!("Expected V14 fixture metadata");
        };
        let storage = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.name == "PerpMarket")
            .unwrap()
            .storage
            .as_ref()
            .unwrap();

        let order_info = storage
            .entries
            .iter()
            .find(|entry| entry.name == "OrderInfoForMarket")
            .unwrap();
        assert_eq!(order_info.modifier, StorageEntryModifier::Optional);
        assert_eq!(order_info.default, [0]);
        let StorageEntryType::Map {
            hashers,
            key,
            value,
        } = &order_info.ty
        else {
            panic!("OrderInfoForMarket must be a storage map");
        };
        assert!(matches!(
            hashers.as_slice(),
            [StorageHasher::Identity, StorageHasher::Identity]
        ));
        let order_key = metadata.types.resolve(key.id).unwrap();
        let TypeDef::Tuple(order_key) = &order_key.type_def else {
            panic!("OrderInfoForMarket key must be a tuple");
        };
        assert_eq!(order_key.fields.len(), 2);
        assert_fixed_u8_array(&metadata.types, order_key.fields[0].id, 20, Some("H160"));
        assert_primitive(
            &metadata.types,
            order_key.fields[1].id,
            TypeDefPrimitive::U16,
        );
        let orders = metadata.types.resolve(value.id).unwrap();
        assert_eq!(
            orders.path.segments.last().map(String::as_str),
            Some("BTreeMap")
        );
        assert_eq!(orders.type_params.len(), 2);
        assert_primitive(
            &metadata.types,
            orders.type_params[0].ty.as_ref().unwrap().id,
            TypeDefPrimitive::U64,
        );
        let order = metadata
            .types
            .resolve(orders.type_params[1].ty.as_ref().unwrap().id)
            .unwrap();
        assert_eq!(
            order.path.segments.last().map(String::as_str),
            Some("PerpOrder")
        );
        let TypeDef::Composite(order) = &order.type_def else {
            panic!("PerpOrder must be a composite type");
        };
        assert_eq!(order.fields.len(), 16);
        let primitive_fields = [
            (0, "order_id", TypeDefPrimitive::U64),
            (2, "market_id", TypeDefPrimitive::U16),
            (3, "is_long", TypeDefPrimitive::Bool),
            (4, "size", TypeDefPrimitive::U128),
            (5, "price", TypeDefPrimitive::U128),
            (7, "create_time", TypeDefPrimitive::U64),
            (8, "leverage", TypeDefPrimitive::U64),
            (10, "size_filled", TypeDefPrimitive::U128),
            (11, "size_remain", TypeDefPrimitive::U128),
            (14, "reduce_only", TypeDefPrimitive::Bool),
        ];
        for (index, name, primitive) in primitive_fields {
            assert_eq!(order.fields[index].name.as_deref(), Some(name));
            assert_primitive(&metadata.types, order.fields[index].ty.id, primitive);
        }
        assert_eq!(order.fields[1].name.as_deref(), Some("owner"));
        assert_fixed_u8_array(&metadata.types, order.fields[1].ty.id, 20, Some("H160"));
        assert_eq!(order.fields[6].name.as_deref(), Some("order_type"));
        assert_order_type(&metadata.types, order.fields[6].ty.id);
        assert_eq!(order.fields[9].name.as_deref(), Some("status"));
        assert_unit_variants(
            &metadata.types,
            order.fields[9].ty.id,
            &[
                "Open",
                "PartiallyFilled",
                "Filled",
                "Canceled",
                "Rejected",
                "Expired",
            ],
        );
        for (index, name) in [(12, "take_profit"), (13, "stop_loss")] {
            assert_eq!(order.fields[index].name.as_deref(), Some(name));
            assert_option_primitive(
                &metadata.types,
                order.fields[index].ty.id,
                TypeDefPrimitive::U128,
            );
        }
        assert_eq!(order.fields[15].name.as_deref(), Some("post_only"));
        assert_unit_variants(
            &metadata.types,
            order.fields[15].ty.id,
            &["None", "MustPostOnly", "Adaptive"],
        );
    }

    fn assert_order_type(types: &scale_info::PortableRegistry, type_id: u32) {
        let ty = types.resolve(type_id).unwrap();
        assert_eq!(
            ty.path.segments.last().map(String::as_str),
            Some("OrderType")
        );
        let TypeDef::Variant(order_type) = &ty.type_def else {
            panic!("OrderType must be a variant type");
        };
        assert_eq!(
            order_type
                .variants
                .iter()
                .map(|variant| variant.name.as_str())
                .collect::<Vec<_>>(),
            ["Limit", "Market", "Stop"],
        );
        assert_eq!(order_type.variants[0].fields.len(), 1);
        assert_unit_variants(
            types,
            order_type.variants[0].fields[0].ty.id,
            &["GTC", "IOC", "FOK"],
        );
        assert_eq!(order_type.variants[1].fields.len(), 1);
        assert_option_primitive(
            types,
            order_type.variants[1].fields[0].ty.id,
            TypeDefPrimitive::U64,
        );
        assert!(order_type.variants[2].fields.is_empty());
    }

    fn assert_unit_variants(
        types: &scale_info::PortableRegistry,
        type_id: u32,
        expected_names: &[&str],
    ) {
        let ty = types.resolve(type_id).unwrap();
        let TypeDef::Variant(variants) = &ty.type_def else {
            panic!("expected a variant type");
        };
        assert_eq!(
            variants
                .variants
                .iter()
                .map(|variant| variant.name.as_str())
                .collect::<Vec<_>>(),
            expected_names,
        );
        assert!(
            variants
                .variants
                .iter()
                .all(|variant| variant.fields.is_empty())
        );
    }

    fn assert_primitive(
        types: &scale_info::PortableRegistry,
        type_id: u32,
        expected: TypeDefPrimitive,
    ) {
        let ty = types.resolve(type_id).unwrap();
        assert_eq!(ty.type_def, TypeDef::Primitive(expected));
    }

    fn assert_fixed_u8_array(
        types: &scale_info::PortableRegistry,
        type_id: u32,
        expected_len: u32,
        expected_name: Option<&str>,
    ) {
        let ty = types.resolve(type_id).unwrap();
        if let Some(expected_name) = expected_name {
            assert_eq!(
                ty.path.segments.last().map(String::as_str),
                Some(expected_name)
            );
        }
        let TypeDef::Composite(composite) = &ty.type_def else {
            panic!("fixed byte wrapper must be a composite type");
        };
        assert_eq!(composite.fields.len(), 1);
        let bytes = types.resolve(composite.fields[0].ty.id).unwrap();
        let TypeDef::Array(bytes) = &bytes.type_def else {
            panic!("fixed byte wrapper must contain an array");
        };
        assert_eq!(bytes.len, expected_len);
        assert_primitive(types, bytes.type_param.id, TypeDefPrimitive::U8);
    }

    fn assert_option_primitive(
        types: &scale_info::PortableRegistry,
        type_id: u32,
        expected: TypeDefPrimitive,
    ) {
        let ty = types.resolve(type_id).unwrap();
        assert_eq!(ty.path.segments.last().map(String::as_str), Some("Option"));
        let TypeDef::Variant(option) = &ty.type_def else {
            panic!("optional field must use Option");
        };
        assert_eq!(option.variants.len(), 2);
        assert_eq!(option.variants[0].name, "None");
        assert!(option.variants[0].fields.is_empty());
        assert_eq!(option.variants[1].name, "Some");
        assert_eq!(option.variants[1].fields.len(), 1);
        assert_primitive(types, option.variants[1].fields[0].ty.id, expected);
    }

    #[rstest]
    fn runtime_error_identities_match_independently_decoded_fixture() {
        let bytes = spec_376_metadata_bytes();
        let snapshot = DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap(),
            376,
            TESTNET_TRANSACTION_VERSION,
            &bytes,
        )
        .unwrap();
        let mut input = bytes.as_slice();
        let RuntimeMetadataPrefixed(prefix, RuntimeMetadata::V14(metadata)) =
            RuntimeMetadataPrefixed::decode(&mut input).unwrap()
        else {
            panic!("Expected V14 fixture metadata");
        };
        assert_eq!(prefix, META_RESERVED);
        assert!(input.is_empty());
        let catalog = snapshot.interfaces();
        assert_eq!(catalog.pallets().len(), metadata.pallets.len());
        let mut error_count = 0;

        for pallet in &metadata.pallets {
            let actual_pallet = catalog.pallet(&pallet.name).unwrap();
            assert_eq!(actual_pallet.index(), pallet.index);
            let Some(errors) = &pallet.error else {
                assert!(actual_pallet.errors().is_empty());
                assert_eq!(
                    catalog.error_by_index(pallet.index, 0),
                    Err(DeepXRuntimeInterfaceError::ErrorIndexUnavailable(
                        pallet.index,
                        0,
                    )),
                );
                continue;
            };
            let TypeDef::Variant(variants) =
                &metadata.types.resolve(errors.ty.id).unwrap().type_def
            else {
                panic!("Expected pallet error variants");
            };
            assert_eq!(actual_pallet.errors().len(), variants.variants.len());

            for (actual, expected) in actual_pallet.errors().iter().zip(&variants.variants) {
                assert_eq!(actual.name(), expected.name);
                assert_eq!(actual.index(), expected.index);
                let (found_pallet, found_error) = catalog
                    .error_by_index(pallet.index, expected.index)
                    .unwrap();
                assert!(std::ptr::eq(found_pallet, actual_pallet));
                assert!(std::ptr::eq(found_error, actual));
                error_count += 1;
            }

            let unknown_error = (0..=u8::MAX)
                .find(|index| {
                    !variants
                        .variants
                        .iter()
                        .any(|variant| variant.index == *index)
                })
                .unwrap();
            assert_eq!(
                catalog.error_by_index(pallet.index, unknown_error),
                Err(DeepXRuntimeInterfaceError::ErrorIndexUnavailable(
                    pallet.index,
                    unknown_error,
                )),
            );
        }

        assert!(error_count > 0, "Fixture must contain pallet errors");
        let unknown_pallet = (0..=u8::MAX)
            .find(|index| !metadata.pallets.iter().any(|pallet| pallet.index == *index))
            .unwrap();
        assert_eq!(
            catalog.error_by_index(unknown_pallet, 0),
            Err(DeepXRuntimeInterfaceError::PalletIndexUnavailable(
                unknown_pallet,
            )),
        );
    }

    fn synthetic_error_metadata(second_name: &str, second_index: u8) -> Metadata {
        let RuntimeMetadataPrefixed(_, RuntimeMetadata::V14(mut metadata)) =
            RuntimeMetadataPrefixed::decode(&mut spec_376_metadata_bytes().as_slice()).unwrap()
        else {
            panic!("Expected V14 fixture metadata");
        };
        let mut pallet = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.error.is_some())
            .unwrap()
            .clone();
        let error_type = pallet.error.as_ref().unwrap().ty.id;
        pallet.name = "SparsePallet".to_string();
        pallet.index = 71;
        metadata.pallets = vec![pallet];
        let ty = metadata
            .types
            .types
            .iter_mut()
            .find(|ty| ty.id == error_type)
            .unwrap();
        let TypeDef::Variant(variants) = &mut ty.ty.type_def else {
            panic!("Expected pallet error variants");
        };
        variants.variants = vec![
            Variant {
                name: "FirstError".to_string(),
                fields: Vec::new(),
                index: 203,
                docs: Vec::new(),
            },
            Variant {
                name: second_name.to_string(),
                fields: Vec::new(),
                index: second_index,
                docs: Vec::new(),
            },
        ];
        let bytes = RuntimeMetadataPrefixed(META_RESERVED, RuntimeMetadata::V14(metadata)).encode();
        metadata::decode_from(&bytes).unwrap()
    }

    #[rstest]
    fn runtime_error_lookup_uses_sparse_declared_indices() {
        let metadata = synthetic_error_metadata("SecondError", 9);
        let catalog = DeepXRuntimeInterfaceCatalog::from_metadata(&metadata).unwrap();

        for (index, name) in [(203, "FirstError"), (9, "SecondError")] {
            let (pallet, error) = catalog.error_by_index(71, index).unwrap();
            assert_eq!(pallet.name(), "SparsePallet");
            assert_eq!(pallet.index(), 71);
            assert_eq!(error.name(), name);
            assert_eq!(error.index(), index);
        }

        assert_eq!(
            catalog.error_by_index(0, 203),
            Err(DeepXRuntimeInterfaceError::PalletIndexUnavailable(0)),
        );

        for index in [0, 1, 255] {
            assert_eq!(
                catalog.error_by_index(71, index),
                Err(DeepXRuntimeInterfaceError::ErrorIndexUnavailable(71, index)),
            );
        }
    }

    #[rstest]
    #[case("FirstError", 9)]
    #[case("SecondError", 203)]
    fn runtime_interface_rejects_duplicate_error_identity(
        #[case] second_name: &str,
        #[case] second_index: u8,
    ) {
        let metadata = synthetic_error_metadata(second_name, second_index);
        assert_eq!(
            DeepXRuntimeInterfaceCatalog::from_metadata(&metadata),
            Err(DeepXRuntimeInterfaceError::DuplicateError(
                "SparsePallet".to_string(),
                second_name.to_string(),
            )),
        );
    }

    #[rstest]
    fn runtime_interface_lookups_fail_closed() {
        let snapshot = DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap(),
            376,
            TESTNET_TRANSACTION_VERSION,
            &spec_376_metadata_bytes(),
        )
        .unwrap();

        assert_eq!(
            snapshot.interfaces().pallet("UnknownPallet"),
            Err(DeepXRuntimeInterfaceError::PalletUnavailable(
                "UnknownPallet".to_string(),
            )),
        );
        assert_eq!(
            snapshot.interfaces().call("System", "unknown_call"),
            Err(DeepXRuntimeInterfaceError::CallUnavailable(
                "System".to_string(),
                "unknown_call".to_string(),
            )),
        );
        assert_eq!(
            snapshot.interfaces().event("System", "UnknownEvent"),
            Err(DeepXRuntimeInterfaceError::EventUnavailable(
                "System".to_string(),
                "UnknownEvent".to_string(),
            )),
        );
    }

    #[rstest]
    #[case("duplicate name", "first", 1, "first", 2)]
    #[case("duplicate index", "first", 1, "second", 1)]
    fn runtime_interface_rejects_duplicate_variant_identity(
        #[case] _description: &str,
        #[case] first_name: &str,
        #[case] first_index: u8,
        #[case] second_name: &str,
        #[case] second_index: u8,
    ) {
        let variants = [
            Variant::<PortableForm> {
                name: first_name.to_string(),
                fields: Vec::new(),
                index: first_index,
                docs: Vec::new(),
            },
            Variant::<PortableForm> {
                name: second_name.to_string(),
                fields: Vec::new(),
                index: second_index,
                docs: Vec::new(),
            },
        ];

        assert_eq!(
            collect_variants(
                "System",
                &variants,
                DeepXRuntimeInterfaceError::DuplicateCall,
            ),
            Err(DeepXRuntimeInterfaceError::DuplicateCall(
                "System".to_string(),
                second_name.to_string(),
            )),
        );
    }

    #[rstest]
    fn rejects_metadata_outside_the_approved_fixture_identity() {
        let mut bytes = spec_376_metadata_bytes();
        bytes[100] ^= 1;

        assert!(matches!(
            DeepXReadOnlyRuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
                decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap(),
                376,
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
                &DeepXEnvironment::Testnet,
                decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap(),
                372,
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
    #[case(false, false)]
    #[case(false, true)]
    #[case(true, false)]
    #[case(true, true)]
    fn observed_fingerprint_requires_matching_quiescent_install(
        #[case] include_metadata: bool,
        #[case] apply: bool,
    ) {
        let bytes = spec_376_metadata_bytes();
        let replacement = test_snapshot();
        let mut old = replacement.clone();
        old.identity.spec_version -= 1;
        let service = DeepXRuntimeSnapshotService::new(old.clone());
        let permit = service.acquire().unwrap();
        let identity = replacement.identity();
        service
            .observe_runtime_fingerprint(
                &identity.environment,
                identity.genesis_hash,
                identity.spec_version,
                identity.transaction_version,
                None,
            )
            .unwrap();
        if include_metadata {
            service
                .observe_runtime_fingerprint(
                    &identity.environment,
                    identity.genesis_hash,
                    identity.spec_version,
                    identity.transaction_version,
                    Some(&bytes),
                )
                .unwrap();
            assert_eq!(
                service.observe_runtime_fingerprint(
                    &identity.environment,
                    identity.genesis_hash,
                    identity.spec_version,
                    identity.transaction_version,
                    Some(&[0]),
                ),
                Err(DeepXRuntimeSnapshotServiceError::ConflictingRuntimeChange)
            );
        }
        assert_eq!(
            service.install(old.clone()),
            Err(DeepXRuntimeSnapshotServiceError::SnapshotIdentityMismatch)
        );
        assert_eq!(
            service.observe_runtime_identity(old.identity().clone()),
            Err(DeepXRuntimeSnapshotServiceError::ConflictingRuntimeChange)
        );
        let install = || {
            if apply {
                service.apply_validated(&replacement).map(|_| ())
            } else {
                service.install(replacement.clone())
            }
        };
        assert_eq!(
            install(),
            Err(DeepXRuntimeSnapshotServiceError::InFlightSigningPermits(1))
        );
        assert_eq!(permit.snapshot().identity(), old.identity());
        assert!(matches!(
            service.acquire(),
            Err(DeepXRuntimeSnapshotServiceError::RefreshInProgress)
        ));
        drop(permit);
        install().unwrap();
        assert_eq!(
            service.acquire().unwrap().snapshot().identity(),
            replacement.identity()
        );
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
