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
    /// The approved metadata contains an inconsistent pallet, call, or event identity.
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

/// Immutable metadata identity for one runtime call or event variant.
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
}

/// Immutable pallet, call, and event identities from approved runtime metadata.
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
            pallets.push(DeepXRuntimePalletInterface {
                name: pallet.name().to_string(),
                index: pallet.index(),
                calls,
                events,
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

    /// Atomically compares and applies a fixture-validated runtime snapshot.
    ///
    /// A changed candidate immediately blocks new signing permits. When old permits remain, the
    /// candidate identity stays pending so the same snapshot can be retried after they are
    /// released.
    ///
    /// # Errors
    ///
    /// Returns an error when another runtime identity is pending, old signing permits remain, or
    /// shared state is unavailable.
    pub fn apply_validated(
        &self,
        snapshot: &RuntimeSnapshot,
    ) -> Result<DeepXRuntimeSnapshotUpdate, DeepXRuntimeSnapshotServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXRuntimeSnapshotServiceError::StateUnavailable)?;

        if let Some(pending) = &state.pending_identity {
            if pending != snapshot.identity() {
                return Err(DeepXRuntimeSnapshotServiceError::ConflictingRuntimeChange);
            }
        } else if state.active.identity() == snapshot.identity() {
            return Ok(DeepXRuntimeSnapshotUpdate::Unchanged);
        } else {
            state.pending_identity = Some(snapshot.identity().clone());
        }

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
        environment: &DeepXEnvironment,
        observed_genesis_hash: [u8; 32],
        observed_spec_version: u32,
        observed_transaction_version: u32,
        metadata_bytes: &[u8],
    ) -> Result<Self, SnapshotError> {
        if !environment.is_testnet() {
            return Err(SnapshotError::UnsupportedDeployment(
                environment.to_string(),
            ));
        }
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
        let interfaces = DeepXRuntimeInterfaceCatalog::from_metadata(&metadata)?;

        let identity = ApprovedRuntimeIdentity {
            environment: environment.clone(),
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
    use scale_info::form::PortableForm;
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
            &DeepXEnvironment::Testnet,
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
        assert_eq!(
            snapshot.interfaces().call("System", "remark").unwrap(),
            &DeepXRuntimeVariantIdentity {
                name: "remark".to_string(),
                index: 0,
            },
        );
        assert_eq!(
            snapshot
                .interfaces()
                .event("System", "ExtrinsicSuccess")
                .unwrap(),
            &DeepXRuntimeVariantIdentity {
                name: "ExtrinsicSuccess".to_string(),
                index: 0,
            },
        );
    }

    #[rstest]
    fn runtime_interface_lookups_fail_closed() {
        let snapshot = RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            decode_32(TESTNET_GENESIS_HASH).unwrap(),
            TESTNET_SPEC_VERSION,
            TESTNET_TRANSACTION_VERSION,
            &metadata_bytes(),
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
        let mut bytes = metadata_bytes();
        bytes[100] ^= 1;

        assert!(matches!(
            RuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
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
                &DeepXEnvironment::Testnet,
                decode_32(TESTNET_GENESIS_HASH).unwrap(),
                TESTNET_SPEC_VERSION + 1,
                TESTNET_TRANSACTION_VERSION,
                &metadata_bytes(),
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
                decode_32(TESTNET_GENESIS_HASH).unwrap(),
                TESTNET_SPEC_VERSION,
                TESTNET_TRANSACTION_VERSION,
                &metadata_bytes(),
            ),
            Err(SnapshotError::UnsupportedDeployment(_)),
        ));
    }

    #[rstest]
    fn runtime_change_blocks_new_signing_until_old_permits_finish() {
        let snapshot = RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
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
            &DeepXEnvironment::Testnet,
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

    #[rstest]
    fn validated_snapshot_application_is_idempotent() {
        let snapshot = RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            decode_32(TESTNET_GENESIS_HASH).unwrap(),
            TESTNET_SPEC_VERSION,
            TESTNET_TRANSACTION_VERSION,
            &metadata_bytes(),
        )
        .unwrap();
        let service = DeepXRuntimeSnapshotService::new(snapshot.clone());

        assert_eq!(
            service.apply_validated(&snapshot).unwrap(),
            DeepXRuntimeSnapshotUpdate::Unchanged,
        );
    }

    #[rstest]
    fn validated_snapshot_application_waits_for_old_permits() {
        let snapshot = RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            decode_32(TESTNET_GENESIS_HASH).unwrap(),
            TESTNET_SPEC_VERSION,
            TESTNET_TRANSACTION_VERSION,
            &metadata_bytes(),
        )
        .unwrap();
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
        let snapshot = RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            decode_32(TESTNET_GENESIS_HASH).unwrap(),
            TESTNET_SPEC_VERSION,
            TESTNET_TRANSACTION_VERSION,
            &metadata_bytes(),
        )
        .unwrap();
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
