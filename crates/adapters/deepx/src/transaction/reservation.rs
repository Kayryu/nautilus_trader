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

//! Durable identity and reservation records for DeepX direct-pallet transactions.

use nautilus_core::hex;
use nautilus_model::{
    enums::OrderSide,
    identifiers::{ClientOrderId, InstrumentId},
};
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use subxt_core::config::{Hasher, substrate::BlakeTwo256};
use thiserror::Error;

use super::{
    DeepXAutomaticReplayDecision, DeepXTransactionLifecycle, DeepXTransactionObservation,
    DeepXTransactionRecoveryAction, DeepXTransactionState,
};
use crate::signing::{ApprovedRuntimeIdentity, SignedPalletExtrinsic};

/// Version of the durable DeepX transaction record schema.
pub const DEEPX_TRANSACTION_RECORD_VERSION: u16 = 2;

/// Namespace used for DeepX transaction records in the generic cache.
pub const DEEPX_TRANSACTION_CACHE_KEY_PREFIX: &str = "deepx:transaction:v2:";

/// A reserved nonce in one of DeepX's distinct nonce domains.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "domain", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DeepXNonceReservation {
    /// Timestamp-derived order identity used by the signed direct-pallet extrinsic.
    TimestampOrderId {
        /// Reserved timestamp nonce.
        value: u64,
    },
    /// Sequential account nonce reserved for protocols which prove that domain.
    SequentialAccount {
        /// Venue account index owning the sequence.
        account_index: u64,
        /// Reserved sequential nonce.
        nonce: u64,
    },
}

/// Immutable runtime identity attached to a direct-pallet reservation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeepXDirectRuntimeIdentity {
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

impl From<&ApprovedRuntimeIdentity> for DeepXDirectRuntimeIdentity {
    fn from(value: &ApprovedRuntimeIdentity) -> Self {
        Self {
            genesis_hash: value.genesis_hash,
            metadata_sha256: value.metadata_sha256,
            spec_version: value.spec_version,
            transaction_version: value.transaction_version,
            signed_extensions: value.signed_extensions.clone(),
        }
    }
}

/// Immutable local identity persisted before a direct-pallet transaction is signed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeepXTransactionIdentity {
    client_order_id: String,
    /// Ethereum AccountId20 expected to sign the extrinsic.
    signer: [u8; 20],
    /// Nautilus instrument associated with the mutation.
    instrument_id: InstrumentId,
    /// Side associated with the mutation.
    order_side: OrderSide,
    /// Reserved nonce and its protocol domain.
    nonce: DeepXNonceReservation,
    /// Runtime against which signing must occur.
    runtime: DeepXDirectRuntimeIdentity,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeepXTransactionIdentityWire {
    client_order_id: String,
    signer: [u8; 20],
    instrument_id: InstrumentId,
    order_side: OrderSide,
    nonce: DeepXNonceReservation,
    runtime: DeepXDirectRuntimeIdentity,
}

impl DeepXTransactionIdentity {
    /// Creates immutable transaction identity from validated Nautilus identifiers.
    #[must_use]
    pub fn new(
        client_order_id: ClientOrderId,
        signer: [u8; 20],
        instrument_id: InstrumentId,
        order_side: OrderSide,
        nonce: DeepXNonceReservation,
        runtime: DeepXDirectRuntimeIdentity,
    ) -> Self {
        Self {
            client_order_id: client_order_id.to_string(),
            signer,
            instrument_id,
            order_side,
            nonce,
            runtime,
        }
    }

    /// Returns the client order ID text.
    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    /// Returns the reserved signing AccountId20.
    #[must_use]
    pub const fn signer(&self) -> [u8; 20] {
        self.signer
    }

    /// Returns the reserved Nautilus instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the reserved order side.
    #[must_use]
    pub const fn order_side(&self) -> OrderSide {
        self.order_side
    }

    /// Returns the reserved nonce domain and value.
    #[must_use]
    pub const fn nonce(&self) -> DeepXNonceReservation {
        self.nonce
    }

    /// Returns the reserved direct-pallet runtime identity.
    #[must_use]
    pub const fn runtime(&self) -> &DeepXDirectRuntimeIdentity {
        &self.runtime
    }
}

impl<'de> Deserialize<'de> for DeepXTransactionIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = DeepXTransactionIdentityWire::deserialize(deserializer)?;
        let client_order_id =
            ClientOrderId::new_checked(&wire.client_order_id).map_err(D::Error::custom)?;
        Ok(Self::new(
            client_order_id,
            wire.signer,
            wire.instrument_id,
            wire.order_side,
            wire.nonce,
            wire.runtime,
        ))
    }
}

/// Complete signed extrinsic bytes retained by a durable transaction record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeepXDurableSignedExtrinsic {
    bytes: Vec<u8>,
    extrinsic_hash: [u8; 32],
}

impl DeepXDurableSignedExtrinsic {
    /// Returns the complete compact-length-prefixed SCALE extrinsic bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the Blake2-256 hash of the complete extrinsic bytes.
    #[must_use]
    pub const fn extrinsic_hash(&self) -> [u8; 32] {
        self.extrinsic_hash
    }

    fn from_signed(signed: &SignedPalletExtrinsic) -> Self {
        Self {
            bytes: signed.bytes().to_vec(),
            extrinsic_hash: signed.extrinsic_hash(),
        }
    }

    fn has_valid_hash(&self) -> bool {
        BlakeTwo256.hash(&self.bytes).0 == self.extrinsic_hash
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeepXDurableSignedExtrinsicWire {
    bytes: Vec<u8>,
    extrinsic_hash: [u8; 32],
}

/// Versioned transaction identity and evidence suitable for generic cache persistence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeepXTransactionRecord {
    version: u16,
    /// Immutable transaction identity.
    identity: DeepXTransactionIdentity,
    /// Complete signed bytes and deterministic hash, once signing succeeds.
    signed_extrinsic: Option<DeepXDurableSignedExtrinsic>,
    /// Evidence-driven transaction lifecycle.
    lifecycle: DeepXTransactionLifecycle,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeepXTransactionRecordWire {
    version: u16,
    identity: DeepXTransactionIdentity,
    signed_extrinsic: Option<DeepXDurableSignedExtrinsicWire>,
    lifecycle: DeepXTransactionLifecycleWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeepXTransactionLifecycleWire {
    state: DeepXTransactionState,
    extrinsic_hash: Option<[u8; 32]>,
    inclusion: Option<super::DeepXInclusionEvidence>,
    absence: Option<DeepXAbsenceEvidenceWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeepXAbsenceEvidenceWire {
    first_scanned_block: u64,
    finalized_block_number: u64,
    finalized_block_hash: [u8; 32],
    canonical_scan_complete: bool,
    submission_pool_absence: bool,
}

/// Errors raised while binding or restoring durable DeepX transaction records.
#[derive(Debug, Error)]
pub enum DeepXTransactionRecordError {
    /// JSON encoding or decoding failed.
    #[error("invalid DeepX transaction record encoding: {0}")]
    Encoding(#[from] serde_json::Error),
    /// The record uses an unsupported durable schema version.
    #[error("unsupported DeepX transaction record version: {0}")]
    UnsupportedVersion(u16),
    /// The restored client order ID is invalid.
    #[error("invalid DeepX transaction record client order ID")]
    InvalidClientOrderId,
    /// The restored lifecycle fields are inconsistent with its state.
    #[error("inconsistent DeepX transaction lifecycle record")]
    InconsistentLifecycle,
    /// The signed extrinsic belongs to a different signer.
    #[error("DeepX signed extrinsic signer does not match its reservation")]
    SignerMismatch,
    /// The signed extrinsic uses a different reserved nonce.
    #[error("DeepX signed extrinsic nonce does not match its reservation")]
    NonceMismatch,
    /// The signed extrinsic was encoded against a different runtime snapshot.
    #[error("DeepX signed extrinsic runtime does not match its reservation")]
    RuntimeMismatch,
    /// Sequential account nonce signing remains disabled pending protocol evidence.
    #[error("DeepX sequential account nonce signing is unsupported")]
    UnsupportedNonceDomain,
    /// The signed extrinsic bytes do not match their deterministic hash.
    #[error("DeepX signed extrinsic bytes do not match their hash")]
    ExtrinsicHashMismatch,
    /// Durable signed bytes conflict with previously recorded evidence.
    #[error("DeepX durable signed extrinsic conflicts with its lifecycle evidence")]
    SignedExtrinsicMismatch,
    /// The lifecycle rejected the signed extrinsic evidence.
    #[error(transparent)]
    Lifecycle(#[from] super::DeepXTransactionError),
}

impl DeepXTransactionRecord {
    /// Creates a record after durable identity and nonce reservation.
    #[must_use]
    pub const fn created(identity: DeepXTransactionIdentity) -> Self {
        Self {
            version: DEEPX_TRANSACTION_RECORD_VERSION,
            identity,
            signed_extrinsic: None,
            lifecycle: DeepXTransactionLifecycle::created(),
        }
    }

    /// Returns the immutable transaction identity.
    #[must_use]
    pub const fn identity(&self) -> &DeepXTransactionIdentity {
        &self.identity
    }

    /// Returns the complete durable signed extrinsic when available.
    #[must_use]
    pub const fn signed_extrinsic(&self) -> Option<&DeepXDurableSignedExtrinsic> {
        self.signed_extrinsic.as_ref()
    }

    /// Returns the evidence-driven transaction lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> &DeepXTransactionLifecycle {
        &self.lifecycle
    }

    /// Classifies the fail-closed action required after restoring this record.
    ///
    /// This classification does not authorize submission or replay of retained signed bytes.
    #[must_use]
    pub const fn recovery_action(&self) -> DeepXTransactionRecoveryAction {
        match self.lifecycle.state() {
            DeepXTransactionState::Created => DeepXTransactionRecoveryAction::RecreateSigningInputs,
            DeepXTransactionState::Signed => {
                DeepXTransactionRecoveryAction::SubmissionDecisionRequired
            }
            DeepXTransactionState::Submitting
            | DeepXTransactionState::Accepted
            | DeepXTransactionState::InBlockSuccess
            | DeepXTransactionState::InBlockFailed
            | DeepXTransactionState::NotIncluded => {
                DeepXTransactionRecoveryAction::ReconciliationRequired
            }
            DeepXTransactionState::Finalized => DeepXTransactionRecoveryAction::Complete,
            DeepXTransactionState::ActionRequired => {
                DeepXTransactionRecoveryAction::OperatorActionRequired
            }
        }
    }

    /// Classifies the fail-closed automatic replay decision after restoring this record.
    ///
    /// This method never returns signed bytes or grants transmission authority. Even complete
    /// absence evidence requires a newly built and independently validated replacement.
    #[must_use]
    pub const fn automatic_replay_decision(&self) -> DeepXAutomaticReplayDecision {
        match self.lifecycle.state() {
            DeepXTransactionState::Created => DeepXAutomaticReplayDecision::RecreateSigningInputs,
            DeepXTransactionState::Signed => {
                DeepXAutomaticReplayDecision::InitialSubmissionPolicyRequired
            }
            DeepXTransactionState::Submitting
            | DeepXTransactionState::Accepted
            | DeepXTransactionState::InBlockSuccess
            | DeepXTransactionState::InBlockFailed => {
                DeepXAutomaticReplayDecision::ReconciliationRequired
            }
            DeepXTransactionState::NotIncluded => DeepXAutomaticReplayDecision::ReplacementRequired,
            DeepXTransactionState::Finalized => DeepXAutomaticReplayDecision::Complete,
            DeepXTransactionState::ActionRequired => {
                DeepXAutomaticReplayDecision::OperatorActionRequired
            }
        }
    }

    /// Returns a stable, versioned cache key scoped by client order ID.
    #[must_use]
    pub fn cache_key(client_order_id: ClientOrderId) -> String {
        format!(
            "{DEEPX_TRANSACTION_CACHE_KEY_PREFIX}{}",
            hex::encode(client_order_id.as_str().as_bytes()),
        )
    }

    pub(crate) fn cache_key_for_record(&self) -> String {
        format!(
            "{DEEPX_TRANSACTION_CACHE_KEY_PREFIX}{}",
            hex::encode(self.identity.client_order_id.as_bytes()),
        )
    }

    /// Encodes the record after checking durable invariants.
    ///
    /// # Errors
    ///
    /// Returns an error when the record is inconsistent or JSON encoding fails.
    pub fn encode(&self) -> Result<Vec<u8>, DeepXTransactionRecordError> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    /// Decodes a record and fails closed on schema drift or inconsistent evidence.
    ///
    /// # Errors
    ///
    /// Returns an error when JSON decoding or durable invariant validation fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, DeepXTransactionRecordError> {
        let wire: DeepXTransactionRecordWire = serde_json::from_slice(bytes)?;
        Self::try_from_wire(wire)
    }

    /// Binds an offline signed extrinsic to its reservation.
    ///
    /// Returns `false` when the same signed evidence was already recorded.
    ///
    /// # Errors
    ///
    /// Returns an error without mutation when signer, nonce, or hash evidence conflicts.
    pub fn record_signed(
        &mut self,
        signed: &SignedPalletExtrinsic,
    ) -> Result<bool, DeepXTransactionRecordError> {
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = self.identity.nonce else {
            return Err(DeepXTransactionRecordError::UnsupportedNonceDomain);
        };
        if signed.signer() != self.identity.signer {
            return Err(DeepXTransactionRecordError::SignerMismatch);
        }
        if signed.nonce() != nonce {
            return Err(DeepXTransactionRecordError::NonceMismatch);
        }
        if DeepXDirectRuntimeIdentity::from(signed.runtime()) != self.identity.runtime {
            return Err(DeepXTransactionRecordError::RuntimeMismatch);
        }
        if !signed.has_valid_hash() {
            return Err(DeepXTransactionRecordError::ExtrinsicHashMismatch);
        }
        let durable = DeepXDurableSignedExtrinsic::from_signed(signed);
        if let Some(existing) = &self.signed_extrinsic {
            if existing != &durable
                || self.lifecycle.extrinsic_hash() != Some(durable.extrinsic_hash())
            {
                return Err(DeepXTransactionRecordError::SignedExtrinsicMismatch);
            }
            return Ok(false);
        }
        let mut lifecycle = self.lifecycle.clone();
        let changed = lifecycle.apply(DeepXTransactionObservation::Signed {
            extrinsic_hash: signed.extrinsic_hash(),
        })?;
        self.lifecycle = lifecycle;
        self.signed_extrinsic = Some(durable);
        Ok(changed)
    }

    /// Applies lifecycle evidence while preserving complete record invariants.
    ///
    /// Returns `false` when the same evidence was already recorded.
    ///
    /// # Errors
    ///
    /// Returns an error without mutation when the lifecycle rejects the observation or the
    /// resulting record would conflict with its durable signed payload.
    pub fn apply_observation(
        &mut self,
        observation: DeepXTransactionObservation,
    ) -> Result<bool, DeepXTransactionRecordError> {
        let mut candidate = self.clone();
        let changed = candidate.lifecycle.apply(observation)?;
        candidate.validate()?;
        self.lifecycle = candidate.lifecycle;
        Ok(changed)
    }

    fn validate(&self) -> Result<(), DeepXTransactionRecordError> {
        if self.version != DEEPX_TRANSACTION_RECORD_VERSION {
            return Err(DeepXTransactionRecordError::UnsupportedVersion(
                self.version,
            ));
        }
        ClientOrderId::new_checked(&self.identity.client_order_id)
            .map_err(|_| DeepXTransactionRecordError::InvalidClientOrderId)?;

        let consistent = match self.lifecycle.state() {
            DeepXTransactionState::Created => {
                self.lifecycle.extrinsic_hash().is_none()
                    && self.lifecycle.inclusion().is_none()
                    && self.lifecycle.absence().is_none()
            }
            DeepXTransactionState::Signed
            | DeepXTransactionState::Submitting
            | DeepXTransactionState::Accepted => {
                self.lifecycle.extrinsic_hash().is_some()
                    && self.lifecycle.inclusion().is_none()
                    && self.lifecycle.absence().is_none()
            }
            DeepXTransactionState::InBlockSuccess => {
                self.lifecycle.extrinsic_hash().is_some()
                    && self
                        .lifecycle
                        .inclusion()
                        .is_some_and(|e| e.outcome == super::DeepXInclusionOutcome::Success)
                    && self.lifecycle.absence().is_none()
            }
            DeepXTransactionState::InBlockFailed => {
                self.lifecycle.extrinsic_hash().is_some()
                    && self
                        .lifecycle
                        .inclusion()
                        .is_some_and(|e| e.outcome == super::DeepXInclusionOutcome::Failed)
                    && self.lifecycle.absence().is_none()
            }
            DeepXTransactionState::Finalized => {
                self.lifecycle.extrinsic_hash().is_some()
                    && self.lifecycle.inclusion().is_some()
                    && self.lifecycle.absence().is_none()
            }
            DeepXTransactionState::NotIncluded => {
                self.lifecycle.extrinsic_hash().is_some()
                    && self.lifecycle.inclusion().is_none()
                    && self.lifecycle.absence().is_some_and(|e| e.is_complete())
            }
            DeepXTransactionState::ActionRequired => {
                let has_inclusion = self.lifecycle.inclusion().is_some();
                let has_absence = self.lifecycle.absence().is_some();
                let evidence_has_hash =
                    (!has_inclusion && !has_absence) || self.lifecycle.extrinsic_hash().is_some();
                let absence_is_complete = self.lifecycle.absence().is_none_or(|e| e.is_complete());
                !(has_inclusion && has_absence) && evidence_has_hash && absence_is_complete
            }
        };
        if !consistent {
            return Err(DeepXTransactionRecordError::InconsistentLifecycle);
        }

        let signed_extrinsic_is_consistent = match &self.signed_extrinsic {
            Some(signed) => {
                signed.has_valid_hash()
                    && self.lifecycle.extrinsic_hash() == Some(signed.extrinsic_hash())
            }
            None => self.lifecycle.extrinsic_hash().is_none(),
        };
        if !signed_extrinsic_is_consistent {
            return Err(DeepXTransactionRecordError::SignedExtrinsicMismatch);
        }
        Ok(())
    }

    fn try_from_wire(
        wire: DeepXTransactionRecordWire,
    ) -> Result<Self, DeepXTransactionRecordError> {
        let absence = wire
            .lifecycle
            .absence
            .map(|e| {
                super::DeepXAbsenceEvidence::new(
                    e.first_scanned_block,
                    e.finalized_block_number,
                    e.finalized_block_hash,
                    e.canonical_scan_complete,
                    e.submission_pool_absence,
                )
                .map_err(|_| DeepXTransactionRecordError::InconsistentLifecycle)
            })
            .transpose()?;
        let record = Self {
            version: wire.version,
            identity: wire.identity,
            signed_extrinsic: wire
                .signed_extrinsic
                .map(|signed| DeepXDurableSignedExtrinsic {
                    bytes: signed.bytes,
                    extrinsic_hash: signed.extrinsic_hash,
                }),
            lifecycle: DeepXTransactionLifecycle {
                state: wire.lifecycle.state,
                extrinsic_hash: wire.lifecycle.extrinsic_hash,
                inclusion: wire.lifecycle.inclusion,
                absence,
            },
        };
        record.validate()?;
        Ok(record)
    }
}

impl<'de> Deserialize<'de> for DeepXTransactionRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = DeepXTransactionRecordWire::deserialize(deserializer)?;
        Self::try_from_wire(wire).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::Value;
    use subxt_core::config::{Hasher, substrate::BlakeTwo256};

    use super::*;

    fn identity(nonce: DeepXNonceReservation) -> DeepXTransactionIdentity {
        DeepXTransactionIdentity::new(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            [7; 20],
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            nonce,
            DeepXDirectRuntimeIdentity {
                genesis_hash: [1; 32],
                metadata_sha256: [2; 32],
                spec_version: 366,
                transaction_version: 1,
                signed_extensions: vec!["CheckNonce".to_string()],
            },
        )
    }

    fn signed(nonce: u64) -> SignedPalletExtrinsic {
        let runtime = identity(DeepXNonceReservation::TimestampOrderId { value: nonce }).runtime;
        let bytes = vec![1, 2, 3];
        SignedPalletExtrinsic {
            extrinsic_hash: BlakeTwo256.hash(&bytes).0,
            bytes,
            signer: [7; 20],
            nonce,
            runtime: ApprovedRuntimeIdentity {
                genesis_hash: runtime.genesis_hash,
                metadata_sha256: runtime.metadata_sha256,
                spec_version: runtime.spec_version,
                transaction_version: runtime.transaction_version,
                signed_extensions: runtime.signed_extensions,
            },
        }
    }

    fn record_in_state(state: DeepXTransactionState) -> DeepXTransactionRecord {
        let mut record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        if state == DeepXTransactionState::Created {
            return record;
        }
        if state == DeepXTransactionState::ActionRequired {
            record
                .apply_observation(DeepXTransactionObservation::ActionRequired)
                .unwrap();
            return record;
        }

        record.record_signed(&signed(42)).unwrap();
        if state == DeepXTransactionState::Signed {
            return record;
        }
        record
            .apply_observation(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        if state == DeepXTransactionState::Submitting {
            return record;
        }
        if state == DeepXTransactionState::NotIncluded {
            record
                .apply_observation(DeepXTransactionObservation::NotIncluded(
                    super::super::DeepXAbsenceEvidence::new(40, 72, [3; 32], true, true).unwrap(),
                ))
                .unwrap();
            return record;
        }

        record
            .apply_observation(DeepXTransactionObservation::PoolAccepted)
            .unwrap();
        if state == DeepXTransactionState::Accepted {
            return record;
        }
        let outcome = if state == DeepXTransactionState::InBlockFailed {
            super::super::DeepXInclusionOutcome::Failed
        } else {
            super::super::DeepXInclusionOutcome::Success
        };
        let inclusion = super::super::DeepXInclusionEvidence {
            block_hash: [3; 32],
            block_number: 72,
            extrinsic_index: 4,
            outcome,
        };
        record
            .apply_observation(DeepXTransactionObservation::Included(inclusion))
            .unwrap();
        if state == DeepXTransactionState::Finalized {
            record
                .apply_observation(DeepXTransactionObservation::Finalized(inclusion))
                .unwrap();
        }
        record
    }

    #[rstest]
    fn created_record_round_trips_without_losing_identity() {
        let record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));

        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();

        assert_eq!(restored, record);
        assert_eq!(
            restored.identity().client_order_id(),
            record.identity().client_order_id()
        );
    }

    #[rstest]
    #[case(
        DeepXTransactionState::Created,
        DeepXTransactionRecoveryAction::RecreateSigningInputs
    )]
    #[case(
        DeepXTransactionState::Signed,
        DeepXTransactionRecoveryAction::SubmissionDecisionRequired
    )]
    #[case(
        DeepXTransactionState::Submitting,
        DeepXTransactionRecoveryAction::ReconciliationRequired
    )]
    #[case(
        DeepXTransactionState::Accepted,
        DeepXTransactionRecoveryAction::ReconciliationRequired
    )]
    #[case(
        DeepXTransactionState::InBlockSuccess,
        DeepXTransactionRecoveryAction::ReconciliationRequired
    )]
    #[case(
        DeepXTransactionState::InBlockFailed,
        DeepXTransactionRecoveryAction::ReconciliationRequired
    )]
    #[case(
        DeepXTransactionState::Finalized,
        DeepXTransactionRecoveryAction::Complete
    )]
    #[case(
        DeepXTransactionState::NotIncluded,
        DeepXTransactionRecoveryAction::ReconciliationRequired
    )]
    #[case(
        DeepXTransactionState::ActionRequired,
        DeepXTransactionRecoveryAction::OperatorActionRequired
    )]
    fn restored_record_classifies_recovery_without_mutation(
        #[case] state: DeepXTransactionState,
        #[case] expected: DeepXTransactionRecoveryAction,
    ) {
        let record = record_in_state(state);
        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
        let before = restored.encode().unwrap();

        assert_eq!(restored.recovery_action(), expected);
        assert_eq!(restored.encode().unwrap(), before);
        assert_eq!(restored.lifecycle().state(), state);
    }

    #[rstest]
    #[case(
        DeepXTransactionState::Created,
        DeepXAutomaticReplayDecision::RecreateSigningInputs
    )]
    #[case(
        DeepXTransactionState::Signed,
        DeepXAutomaticReplayDecision::InitialSubmissionPolicyRequired
    )]
    #[case(
        DeepXTransactionState::Submitting,
        DeepXAutomaticReplayDecision::ReconciliationRequired
    )]
    #[case(
        DeepXTransactionState::Accepted,
        DeepXAutomaticReplayDecision::ReconciliationRequired
    )]
    #[case(
        DeepXTransactionState::InBlockSuccess,
        DeepXAutomaticReplayDecision::ReconciliationRequired
    )]
    #[case(
        DeepXTransactionState::InBlockFailed,
        DeepXAutomaticReplayDecision::ReconciliationRequired
    )]
    #[case(
        DeepXTransactionState::NotIncluded,
        DeepXAutomaticReplayDecision::ReplacementRequired
    )]
    #[case(
        DeepXTransactionState::Finalized,
        DeepXAutomaticReplayDecision::Complete
    )]
    #[case(
        DeepXTransactionState::ActionRequired,
        DeepXAutomaticReplayDecision::OperatorActionRequired
    )]
    fn automatic_replay_gate_never_mutates_or_releases_retained_bytes(
        #[case] state: DeepXTransactionState,
        #[case] expected: DeepXAutomaticReplayDecision,
    ) {
        let record = record_in_state(state);
        let before = record.encode().unwrap();
        let retained_bytes = record
            .signed_extrinsic()
            .map(|signed| signed.bytes().to_vec());

        assert_eq!(record.automatic_replay_decision(), expected);
        assert_eq!(record.encode().unwrap(), before);
        assert_eq!(
            record
                .signed_extrinsic()
                .map(|signed| signed.bytes().to_vec()),
            retained_bytes,
        );
    }

    #[rstest]
    fn nonce_domains_serialize_distinctly() {
        let timestamp =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        let sequential =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::SequentialAccount {
                account_index: 5,
                nonce: 42,
            }));

        assert_ne!(timestamp.encode().unwrap(), sequential.encode().unwrap());
    }

    #[rstest]
    fn sequential_nonce_domain_cannot_bind_current_signed_output() {
        let mut record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::SequentialAccount {
                account_index: 5,
                nonce: 42,
            }));
        let before = record.clone();

        assert!(matches!(
            record.record_signed(&signed(42)),
            Err(DeepXTransactionRecordError::UnsupportedNonceDomain),
        ));
        assert_eq!(record, before);
    }

    #[rstest]
    fn unknown_version_and_fields_are_rejected() {
        let record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        let mut value = serde_json::to_value(&record).unwrap();
        value["version"] = Value::from(1);
        assert!(matches!(
            DeepXTransactionRecord::decode(&serde_json::to_vec(&value).unwrap()),
            Err(DeepXTransactionRecordError::UnsupportedVersion(1)),
        ));

        value["version"] = Value::from(DEEPX_TRANSACTION_RECORD_VERSION);
        value["unexpected"] = Value::Bool(true);
        assert!(matches!(
            DeepXTransactionRecord::decode(&serde_json::to_vec(&value).unwrap()),
            Err(DeepXTransactionRecordError::Encoding(_)),
        ));
    }

    #[rstest]
    fn standalone_identity_rejects_invalid_client_order_id() {
        let identity = identity(DeepXNonceReservation::TimestampOrderId { value: 42 });
        let mut value = serde_json::to_value(identity).unwrap();
        value["client_order_id"] = Value::from("");

        assert!(serde_json::from_value::<DeepXTransactionIdentity>(value).is_err());
    }

    #[rstest]
    fn matching_signed_extrinsic_advances_idempotently() {
        let mut record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        let signed = signed(42);

        assert!(record.record_signed(&signed).unwrap());
        assert!(!record.record_signed(&signed).unwrap());
        assert_eq!(record.lifecycle().state(), DeepXTransactionState::Signed);
        assert_eq!(
            record.lifecycle().extrinsic_hash(),
            Some(signed.extrinsic_hash()),
        );
        assert_eq!(record.signed_extrinsic().unwrap().bytes(), signed.bytes());

        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
        assert_eq!(restored, record);
        assert_eq!(restored.signed_extrinsic().unwrap().bytes(), signed.bytes());
    }

    #[rstest]
    fn orphaned_signed_observation_is_rejected_without_mutation() {
        let mut record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        let before = record.clone();

        assert!(matches!(
            record.apply_observation(DeepXTransactionObservation::Signed {
                extrinsic_hash: [9; 32],
            }),
            Err(DeepXTransactionRecordError::SignedExtrinsicMismatch),
        ));
        assert_eq!(record, before);
    }

    #[rstest]
    fn post_sign_observation_preserves_durable_payload() {
        let mut record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        record.record_signed(&signed(42)).unwrap();
        let durable = record.signed_extrinsic().unwrap().clone();

        assert!(
            record
                .apply_observation(DeepXTransactionObservation::SubmissionStarted)
                .unwrap()
        );
        assert_eq!(
            record.lifecycle().state(),
            DeepXTransactionState::Submitting
        );
        assert_eq!(record.signed_extrinsic(), Some(&durable));
    }

    #[rstest]
    fn matching_signed_extrinsic_remains_idempotent_after_submission_starts() {
        let mut record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        let signed = signed(42);
        record.record_signed(&signed).unwrap();
        record
            .lifecycle
            .apply(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        let before = record.clone();

        assert!(!record.record_signed(&signed).unwrap());
        assert_eq!(record, before);
    }

    #[rstest]
    #[case("bytes")]
    #[case("extrinsic_hash")]
    fn tampered_durable_signed_extrinsic_is_rejected(#[case] field: &str) {
        let mut record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        record.record_signed(&signed(42)).unwrap();
        let mut value = serde_json::to_value(record).unwrap();
        value["signed_extrinsic"][field][0] = Value::from(9);

        assert!(matches!(
            DeepXTransactionRecord::decode(&serde_json::to_vec(&value).unwrap()),
            Err(DeepXTransactionRecordError::SignedExtrinsicMismatch),
        ));
        assert!(serde_json::from_value::<DeepXTransactionRecord>(value).is_err());
    }

    #[rstest]
    fn mismatched_signed_bytes_and_hash_are_rejected_without_mutation() {
        let mut record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        let before = record.clone();
        let mut signed = signed(42);
        signed.bytes.push(4);

        assert!(matches!(
            record.record_signed(&signed),
            Err(DeepXTransactionRecordError::ExtrinsicHashMismatch),
        ));
        assert_eq!(record, before);
    }

    #[rstest]
    #[case(true, false, false, DeepXTransactionRecordError::SignerMismatch)]
    #[case(false, true, false, DeepXTransactionRecordError::NonceMismatch)]
    #[case(false, false, true, DeepXTransactionRecordError::RuntimeMismatch)]
    fn signed_identity_mismatch_is_rejected_without_mutation(
        #[case] signer_mismatch: bool,
        #[case] nonce_mismatch: bool,
        #[case] runtime_mismatch: bool,
        #[case] expected: DeepXTransactionRecordError,
    ) {
        let mut record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        let before = record.clone();
        let mut signed = signed(42);
        if signer_mismatch {
            signed.signer = [8; 20];
        }
        if nonce_mismatch {
            signed.nonce = 43;
        }
        if runtime_mismatch {
            signed.runtime.spec_version += 1;
        }

        let error = record.record_signed(&signed).unwrap_err();

        assert_eq!(error.to_string(), expected.to_string());
        assert_eq!(record, before);
    }

    #[rstest]
    fn conflicting_signed_hash_is_rejected_without_mutation() {
        let mut record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        record.record_signed(&signed(42)).unwrap();
        let before = record.clone();
        let mut conflicting = signed(42);
        conflicting.bytes = vec![4, 5, 6];
        conflicting.extrinsic_hash = BlakeTwo256.hash(&conflicting.bytes).0;

        assert!(record.record_signed(&conflicting).is_err());
        assert_eq!(record, before);
    }

    #[rstest]
    fn cache_key_is_stable_ascii_and_client_order_scoped() {
        let key = DeepXTransactionRecord::cache_key(ClientOrderId::new("ORDER:1"));

        assert_eq!(key, "deepx:transaction:v2:4f524445523a31");
        assert!(key.is_ascii());
    }

    #[rstest]
    fn malformed_records_never_decode() {
        for bytes in [b"".as_slice(), b"{".as_slice(), b"null".as_slice()] {
            assert!(DeepXTransactionRecord::decode(bytes).is_err());
        }
    }

    #[rstest]
    fn inconsistent_restored_lifecycle_is_rejected() {
        let record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        let mut value = serde_json::to_value(record).unwrap();
        value["lifecycle"]["extrinsic_hash"] = serde_json::to_value([9_u8; 32]).unwrap();

        assert!(matches!(
            DeepXTransactionRecord::decode(&serde_json::to_vec(&value).unwrap()),
            Err(DeepXTransactionRecordError::InconsistentLifecycle),
        ));
        assert!(serde_json::from_value::<DeepXTransactionRecord>(value).is_err());
    }

    #[rstest]
    fn incomplete_restored_absence_evidence_is_rejected() {
        let mut record =
            DeepXTransactionRecord::created(identity(DeepXNonceReservation::TimestampOrderId {
                value: 42,
            }));
        record.record_signed(&signed(42)).unwrap();
        record
            .lifecycle
            .apply(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        record
            .lifecycle
            .apply(DeepXTransactionObservation::NotIncluded(
                super::super::DeepXAbsenceEvidence::new(40, 72, [3; 32], true, true).unwrap(),
            ))
            .unwrap();
        let mut value = serde_json::to_value(record).unwrap();
        value["lifecycle"]["absence"]["canonical_scan_complete"] = Value::Bool(false);

        assert!(matches!(
            DeepXTransactionRecord::decode(&serde_json::to_vec(&value).unwrap()),
            Err(DeepXTransactionRecordError::InconsistentLifecycle),
        ));
        assert!(serde_json::from_value::<DeepXTransactionRecord>(value).is_err());
    }
}
