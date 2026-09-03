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

//! Persistence capabilities required before DeepX transaction signing or submission.

mod postgres;

use std::fmt::Debug;

use nautilus_model::{
    enums::OrderSide,
    identifiers::{ClientOrderId, InstrumentId},
};
pub use postgres::{DeepXPostgresSignerLease, DeepXPostgresTransactionStore};
use thiserror::Error;

use super::{
    DeepXDirectRuntimeIdentity, DeepXDurableSignedExtrinsic, DeepXRecoveryDecision,
    DeepXReorganizationDecision, DeepXTimestampNonceAllocator, DeepXTimestampNonceError,
    DeepXTransactionIdentity, DeepXTransactionObservation, DeepXTransactionRecord,
    DeepXTransactionRecordError, DeepXTransactionState,
};
use crate::signing::{SignedPalletExtrinsic, SigningError};

/// Durable revision assigned to an acknowledged transaction record write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeepXTransactionRevision(u64);

impl DeepXTransactionRevision {
    /// Creates a durable record revision returned by a transaction store.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the durable revision value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Evidence that a store committed the exact encoded transaction record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXCommittedTransactionRecord {
    cache_key: String,
    revision: DeepXTransactionRevision,
    encoded_record: Vec<u8>,
}

/// One decoded transaction record paired with its exact durable acknowledgement.
#[derive(Clone, Debug)]
pub struct DeepXRestoredTransactionRecord {
    record: DeepXTransactionRecord,
    committed: DeepXCommittedTransactionRecord,
}

impl DeepXRestoredTransactionRecord {
    /// Creates a restored record after verifying its durable acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error if the acknowledgement does not cover the exact record encoding.
    pub fn new(
        record: DeepXTransactionRecord,
        committed: DeepXCommittedTransactionRecord,
    ) -> Result<Self, DeepXTransactionPersistenceError> {
        committed.verify(&record)?;
        Ok(Self { record, committed })
    }

    /// Returns the decoded durable transaction record.
    #[must_use]
    pub const fn record(&self) -> &DeepXTransactionRecord {
        &self.record
    }

    /// Returns acknowledgement of the exact durable transaction record.
    #[must_use]
    pub const fn committed(&self) -> &DeepXCommittedTransactionRecord {
        &self.committed
    }
}

impl DeepXCommittedTransactionRecord {
    /// Returns the committed record cache key.
    #[must_use]
    pub fn cache_key(&self) -> &str {
        &self.cache_key
    }

    /// Returns the committed record revision.
    #[must_use]
    pub const fn revision(&self) -> DeepXTransactionRevision {
        self.revision
    }

    /// Returns whether this acknowledgement covers the record's exact current encoding.
    #[must_use]
    pub fn matches(&self, record: &DeepXTransactionRecord) -> bool {
        record.encode().is_ok_and(|encoded| {
            self.cache_key == record.cache_key_for_record() && self.encoded_record == encoded
        })
    }

    /// Verifies that this acknowledgement covers the exact current record.
    ///
    /// # Errors
    ///
    /// Returns an error if the acknowledgement belongs to another record or an older encoding.
    pub fn verify(
        &self,
        record: &DeepXTransactionRecord,
    ) -> Result<(), DeepXTransactionPersistenceError> {
        if self.matches(record) {
            Ok(())
        } else {
            Err(DeepXTransactionPersistenceError::AcknowledgementMismatch)
        }
    }

    /// Creates acknowledgement evidence after `record` has crossed the backend's durability
    /// boundary at `revision`.
    ///
    /// This constructor is for trusted [`DeepXTransactionStore`] implementations. Calling it
    /// before the backend commit is a contract violation.
    ///
    /// # Errors
    ///
    /// Returns an error if the record cannot be encoded for exact-match verification.
    pub fn acknowledge_committed(
        record: &DeepXTransactionRecord,
        revision: DeepXTransactionRevision,
    ) -> Result<Self, DeepXTransactionPersistenceError> {
        let encoded_record = record
            .encode()
            .map_err(|error| DeepXTransactionPersistenceError::BeforeCommit(error.to_string()))?;
        Ok(Self {
            cache_key: record.cache_key_for_record(),
            revision,
            encoded_record,
        })
    }
}

/// Store-owned proof that this process exclusively owns a signer nonce domain.
pub trait DeepXSignerLease: Debug + Send + Sync {
    /// Returns the AccountId20 covered by this lease.
    fn signer(&self) -> [u8; 20];

    /// Returns the store-assigned lease generation.
    fn generation(&self) -> u64;
}

/// Verifies that a signer lease covers the transaction record's signer.
///
/// The store remains responsible for proving that the lease generation is current.
///
/// # Errors
///
/// Returns an error if the lease belongs to another signer.
pub fn verify_signer_lease(
    lease: &impl DeepXSignerLease,
    record: &DeepXTransactionRecord,
) -> Result<(), DeepXTransactionPersistenceError> {
    if lease.signer() == record.identity().signer() {
        Ok(())
    } else {
        Err(DeepXTransactionPersistenceError::LeaseMismatch)
    }
}

/// Persistence failures classified by whether a write may have committed.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXTransactionPersistenceError {
    /// The backend cannot provide the required persistence semantics.
    #[error("DeepX transaction persistence capability is unsupported: {0}")]
    Unsupported(String),
    /// Another owner holds the signer nonce domain.
    #[error("DeepX signer nonce domain is already leased: {0}")]
    LeaseUnavailable(String),
    /// Evidence proves the write did not commit.
    #[error("DeepX transaction write failed before commit: {0}")]
    BeforeCommit(String),
    /// The expected durable revision was no longer current.
    #[error("DeepX transaction record revision conflict")]
    RevisionConflict,
    /// A committed-write acknowledgement did not cover the exact current record.
    #[error("DeepX transaction committed-write acknowledgement does not match the record")]
    AcknowledgementMismatch,
    /// The signer lease belongs to another account.
    #[error("DeepX signer lease does not match the transaction signer")]
    LeaseMismatch,
    /// The commit acknowledgement was lost, so the durable outcome is unknown.
    #[error("DeepX transaction commit outcome is unknown; reconciliation is required: {0}")]
    CommitOutcomeUnknown(String),
}

impl DeepXTransactionPersistenceError {
    /// Returns whether the failure proves that no write committed.
    #[must_use]
    pub const fn is_proven_not_committed(&self) -> bool {
        matches!(
            self,
            Self::Unsupported(_)
                | Self::LeaseUnavailable(_)
                | Self::BeforeCommit(_)
                | Self::RevisionConflict
                | Self::AcknowledgementMismatch
                | Self::LeaseMismatch
        )
    }
}

/// Store contract required for cross-process signer ownership and committed record writes.
///
/// Implementations must hold a lease across processes, must not acknowledge a write before its
/// durability boundary commits, and must implement replacement as an atomic compare-and-set.
/// A transport failure during commit must return
/// [`DeepXTransactionPersistenceError::CommitOutcomeUnknown`].
#[async_trait::async_trait]
pub trait DeepXTransactionStore: Debug + Send + Sync {
    /// Backend-specific signer lease held for the complete transaction decision window.
    type Lease: DeepXSignerLease;

    /// Acquires exclusive ownership of all nonce domains for `signer`.
    async fn acquire_signer_lease(
        &self,
        signer: [u8; 20],
    ) -> Result<Self::Lease, DeepXTransactionPersistenceError>;

    /// Verifies that `lease` is the current store-owned generation for its signer.
    async fn verify_signer_lease(
        &self,
        lease: &Self::Lease,
    ) -> Result<(), DeepXTransactionPersistenceError>;

    /// Loads the complete committed transaction record set for the leased signer.
    ///
    /// Implementations must return a point-in-time complete set while the lease remains current.
    async fn load_committed_for_signer(
        &self,
        lease: &Self::Lease,
    ) -> Result<Vec<DeepXRestoredTransactionRecord>, DeepXTransactionPersistenceError>;

    /// Atomically creates and durably commits a record while `lease` remains valid.
    async fn create_committed(
        &self,
        lease: &Self::Lease,
        record: &DeepXTransactionRecord,
    ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError>;

    /// Atomically replaces a record only if the exact acknowledged prior record is still current.
    async fn compare_and_set_committed(
        &self,
        lease: &Self::Lease,
        expected: &DeepXCommittedTransactionRecord,
        record: &DeepXTransactionRecord,
    ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError>;
}

/// Restores a timestamp nonce allocator from the complete durable signer record set.
///
/// # Errors
///
/// Returns an error if signer ownership, record completeness, acknowledgement integrity, or
/// record signer identity cannot be proven.
pub async fn restore_timestamp_nonce_allocator<S>(
    store: &S,
    lease: &S::Lease,
    max_clock_drift_ms: u64,
) -> Result<
    (
        DeepXTimestampNonceAllocator,
        Vec<DeepXRestoredTransactionRecord>,
    ),
    DeepXTransactionPersistenceError,
>
where
    S: DeepXTransactionStore,
{
    store.verify_signer_lease(lease).await?;
    let restored = store.load_committed_for_signer(lease).await?;
    for item in &restored {
        item.committed().verify(item.record())?;
        verify_signer_lease(lease, item.record())?;
    }
    let allocator = DeepXTimestampNonceAllocator::from_records(
        lease.signer(),
        restored.iter().map(DeepXRestoredTransactionRecord::record),
        max_clock_drift_ms,
    );
    Ok((allocator, restored))
}

/// Failure while reserving and durably committing a timestamp transaction identity.
#[derive(Debug, Error)]
pub enum DeepXReservationPreparationError {
    /// Timestamp nonce allocation could not be proven safe.
    #[error(transparent)]
    TimestampNonce(#[from] DeepXTimestampNonceError),
    /// Persistence or signer ownership could not be proven.
    #[error(transparent)]
    Persistence(#[from] DeepXTransactionPersistenceError),
}

/// A created transaction record released only after its reservation commits durably.
#[derive(Debug)]
pub struct DeepXPreparedReservation {
    record: DeepXTransactionRecord,
    committed: DeepXCommittedTransactionRecord,
}

impl DeepXPreparedReservation {
    /// Returns the durably committed created record.
    #[must_use]
    pub const fn record(&self) -> &DeepXTransactionRecord {
        &self.record
    }

    /// Returns acknowledgement of the committed created record.
    #[must_use]
    pub const fn committed(&self) -> &DeepXCommittedTransactionRecord {
        &self.committed
    }
}

/// Allocates and durably commits one timestamp transaction identity before signing.
///
/// This function performs no signing or network submission. The allocated nonce is never rolled
/// back, including when the durable commit fails or its outcome is unknown.
///
/// # Errors
///
/// Returns an error without a prepared reservation if signer ownership, timestamp allocation, the
/// durable create, or its exact acknowledgement cannot be proven.
#[allow(clippy::too_many_arguments)]
pub async fn prepare_timestamp_reservation<S>(
    store: &S,
    lease: &S::Lease,
    allocator: &DeepXTimestampNonceAllocator,
    local_time_ms: u64,
    chain_time_ms: u64,
    client_order_id: ClientOrderId,
    instrument_id: InstrumentId,
    order_side: OrderSide,
    runtime: DeepXDirectRuntimeIdentity,
) -> Result<DeepXPreparedReservation, DeepXReservationPreparationError>
where
    S: DeepXTransactionStore,
{
    if lease.signer() != allocator.signer() {
        return Err(DeepXTransactionPersistenceError::LeaseMismatch.into());
    }
    store.verify_signer_lease(lease).await?;

    let nonce = allocator.reserve(local_time_ms, chain_time_ms)?;
    let record = DeepXTransactionRecord::created(DeepXTransactionIdentity::new(
        client_order_id,
        allocator.signer(),
        instrument_id,
        order_side,
        nonce,
        runtime,
    ));
    let committed = store.create_committed(lease, &record).await?;
    committed.verify(&record)?;

    Ok(DeepXPreparedReservation { record, committed })
}

/// Failure while signing and durably committing a reserved transaction.
#[derive(Debug, Error)]
pub enum DeepXSignedTransactionPreparationError {
    /// The durable record is not in the only state eligible for signing.
    #[error("DeepX transaction must be durably created before signing")]
    InvalidState,
    /// Persistence or signer ownership could not be proven.
    #[error(transparent)]
    Persistence(#[from] DeepXTransactionPersistenceError),
    /// Offline signing failed.
    #[error(transparent)]
    Signing(#[from] SigningError),
    /// Signed evidence conflicted with the durable reservation.
    #[error(transparent)]
    Record(#[from] DeepXTransactionRecordError),
}

/// A signed transaction record released only after its signed bytes commit durably.
#[derive(Debug)]
pub struct DeepXPreparedSignedTransaction {
    record: DeepXTransactionRecord,
    committed: DeepXCommittedTransactionRecord,
}

impl DeepXPreparedSignedTransaction {
    /// Returns the durably committed signed record.
    #[must_use]
    pub const fn record(&self) -> &DeepXTransactionRecord {
        &self.record
    }

    /// Returns acknowledgement of the committed signed record.
    #[must_use]
    pub const fn committed(&self) -> &DeepXCommittedTransactionRecord {
        &self.committed
    }
}

/// Signs a durably created transaction and commits its signed bytes before releasing the record.
///
/// The signer is invoked only after the signer lease and exact committed `created` record are
/// verified. The store's compare-and-set authoritatively rejects a stale revision after offline
/// signing. This function performs no network submission and releases no transmission permit.
///
/// # Errors
///
/// Returns an error without a prepared signed transaction if any prerequisite, signing invariant,
/// or compare-and-set commit cannot be proven.
pub async fn prepare_signed_transaction<S, F>(
    store: &S,
    lease: &S::Lease,
    committed_created: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    signer: F,
) -> Result<DeepXPreparedSignedTransaction, DeepXSignedTransactionPreparationError>
where
    S: DeepXTransactionStore,
    F: FnOnce(&DeepXTransactionIdentity) -> Result<SignedPalletExtrinsic, SigningError>,
{
    verify_signer_lease(lease, record)?;
    store.verify_signer_lease(lease).await?;
    committed_created.verify(record)?;
    if record.lifecycle().state() != DeepXTransactionState::Created {
        return Err(DeepXSignedTransactionPreparationError::InvalidState);
    }

    let signed = signer(record.identity())?;
    let mut signed_record = record.clone();
    signed_record.record_signed(&signed)?;
    let committed = store
        .compare_and_set_committed(lease, committed_created, &signed_record)
        .await?;
    committed.verify(&signed_record)?;

    Ok(DeepXPreparedSignedTransaction {
        record: signed_record,
        committed,
    })
}

/// Verifies that signed bytes encode the exact business identity reserved by the record.
pub trait DeepXBusinessCallVerifier: Debug + Send + Sync {
    /// Verifies the call binding without mutating state or performing network I/O.
    ///
    /// # Errors
    ///
    /// Returns an error when the call cannot be authoritatively decoded or does not bind every
    /// required identity field.
    fn verify(
        &self,
        identity: &DeepXTransactionIdentity,
        signed_extrinsic: &DeepXDurableSignedExtrinsic,
    ) -> Result<(), DeepXBusinessCallBindingError>;
}

/// Verifier used while no golden-vector-backed business-call schema is available.
#[derive(Clone, Copy, Debug, Default)]
pub struct DeepXUnsupportedBusinessCallVerifier;

impl DeepXBusinessCallVerifier for DeepXUnsupportedBusinessCallVerifier {
    fn verify(
        &self,
        _identity: &DeepXTransactionIdentity,
        _signed_extrinsic: &DeepXDurableSignedExtrinsic,
    ) -> Result<(), DeepXBusinessCallBindingError> {
        Err(DeepXBusinessCallBindingError::Unsupported(
            "authoritative DeepX business-call vectors are unavailable".to_string(),
        ))
    }
}

/// Failure to prove that signed bytes encode the reserved DeepX business operation.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXBusinessCallBindingError {
    /// No authoritative decoder or golden-vector-backed schema supports this call.
    #[error("DeepX business call binding is unsupported: {0}")]
    Unsupported(String),
    /// Decoded call fields do not match the durable transaction identity.
    #[error("DeepX signed business call does not match its transaction identity: {0}")]
    Mismatch(String),
}

/// Failure while atomically preparing one signed extrinsic for initial submission.
#[derive(Debug, Error)]
pub enum DeepXSubmissionPreparationError {
    /// The durable record is not in the only state eligible for initial submission.
    #[error("DeepX transaction must be durably signed before initial submission")]
    InvalidState,
    /// The record has no complete signed payload.
    #[error("DeepX signed transaction record has no durable payload")]
    MissingSignedPayload,
    /// Persistence or signer ownership could not be proven.
    #[error(transparent)]
    Persistence(#[from] DeepXTransactionPersistenceError),
    /// Business-call identity binding could not be proven.
    #[error(transparent)]
    CallBinding(#[from] DeepXBusinessCallBindingError),
    /// The lifecycle transition was inconsistent with the durable record.
    #[error(transparent)]
    Record(#[from] DeepXTransactionRecordError),
}

/// Single-use signed bytes released only after the submitting state commits durably.
#[derive(Debug)]
pub struct DeepXSubmissionPermit {
    bytes: Vec<u8>,
    extrinsic_hash: [u8; 32],
}

impl DeepXSubmissionPermit {
    /// Consumes the permit and returns the complete SCALE extrinsic and expected hash.
    #[must_use]
    pub fn into_payload(self) -> (Vec<u8>, [u8; 32]) {
        (self.bytes, self.extrinsic_hash)
    }
}

/// Result of durably advancing one signed record to `submitting`.
#[derive(Debug)]
pub struct DeepXPreparedSubmission {
    record: DeepXTransactionRecord,
    committed: DeepXCommittedTransactionRecord,
    permit: DeepXSubmissionPermit,
}

impl DeepXPreparedSubmission {
    /// Returns the durably committed `submitting` record.
    #[must_use]
    pub const fn record(&self) -> &DeepXTransactionRecord {
        &self.record
    }

    /// Returns acknowledgement of the committed `submitting` record.
    #[must_use]
    pub const fn committed(&self) -> &DeepXCommittedTransactionRecord {
        &self.committed
    }

    /// Consumes the preparation and releases its single-use transmission permit.
    #[must_use]
    pub fn into_permit(self) -> DeepXSubmissionPermit {
        self.permit
    }
}

/// Atomically prepares a durably signed record for its first transmission.
///
/// This function does not perform network I/O and does not authorize replay. It releases signed
/// bytes only after the store confirms the exact `Signed` record, validates current signer lease
/// ownership, verifies business-call binding, and commits the `Submitting` transition with CAS.
///
/// # Errors
///
/// Returns an error without a transmission permit if any prerequisite or commit is unproven.
pub async fn prepare_initial_submission<S, V>(
    store: &S,
    lease: &S::Lease,
    committed_signed: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    verifier: &V,
) -> Result<DeepXPreparedSubmission, DeepXSubmissionPreparationError>
where
    S: DeepXTransactionStore,
    V: DeepXBusinessCallVerifier,
{
    verify_signer_lease(lease, record)?;
    store.verify_signer_lease(lease).await?;
    committed_signed.verify(record)?;
    if record.lifecycle().state() != DeepXTransactionState::Signed {
        return Err(DeepXSubmissionPreparationError::InvalidState);
    }
    let signed = record
        .signed_extrinsic()
        .ok_or(DeepXSubmissionPreparationError::MissingSignedPayload)?;
    verifier.verify(record.identity(), signed)?;

    let permit = DeepXSubmissionPermit {
        bytes: signed.bytes().to_vec(),
        extrinsic_hash: signed.extrinsic_hash(),
    };
    let mut submitting = record.clone();
    submitting.apply_observation(DeepXTransactionObservation::SubmissionStarted)?;
    let committed = store
        .compare_and_set_committed(lease, committed_signed, &submitting)
        .await?;
    committed.verify(&submitting)?;

    Ok(DeepXPreparedSubmission {
        record: submitting,
        committed,
        permit,
    })
}

/// Failure while durably committing authoritative transaction reconciliation evidence.
#[derive(Debug, Error)]
pub enum DeepXObservationCommitError {
    /// Signing and submission-start observations require their dedicated preparation boundaries.
    #[error("DeepX transaction observation cannot be committed through reconciliation")]
    UnsupportedObservation,
    /// Persistence or signer ownership could not be proven.
    #[error(transparent)]
    Persistence(#[from] DeepXTransactionPersistenceError),
    /// The observation conflicted with the durable transaction record.
    #[error(transparent)]
    Record(#[from] DeepXTransactionRecordError),
}

/// Result of durably applying one authoritative reconciliation observation.
#[derive(Debug)]
pub struct DeepXCommittedObservation {
    record: DeepXTransactionRecord,
    committed: DeepXCommittedTransactionRecord,
}

impl DeepXCommittedObservation {
    /// Returns the transaction record containing the committed observation.
    #[must_use]
    pub const fn record(&self) -> &DeepXTransactionRecord {
        &self.record
    }

    /// Returns acknowledgement of the committed transaction record.
    #[must_use]
    pub const fn committed(&self) -> &DeepXCommittedTransactionRecord {
        &self.committed
    }
}

/// Applies authoritative reconciliation evidence through a revision-checked durable commit.
///
/// This function performs no RPC, submission, replay, or order-event emission. Signing and
/// submission-start observations are rejected because their dedicated preparation boundaries
/// enforce additional protocol and payload invariants. An identical repeated observation returns
/// the existing exact acknowledgement without advancing the durable revision.
///
/// # Errors
///
/// Returns an error without a committed observation if the lease, prior acknowledgement,
/// lifecycle transition, record invariants, or compare-and-set outcome cannot be proven.
pub async fn commit_reconciliation_observation<S>(
    store: &S,
    lease: &S::Lease,
    committed_record: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    observation: DeepXTransactionObservation,
) -> Result<DeepXCommittedObservation, DeepXObservationCommitError>
where
    S: DeepXTransactionStore,
{
    if matches!(
        observation,
        DeepXTransactionObservation::Signed { .. } | DeepXTransactionObservation::SubmissionStarted
    ) {
        return Err(DeepXObservationCommitError::UnsupportedObservation);
    }

    verify_signer_lease(lease, record)?;
    store.verify_signer_lease(lease).await?;
    committed_record.verify(record)?;

    let mut candidate = record.clone();
    if !candidate.apply_observation(observation)? {
        return Ok(DeepXCommittedObservation {
            record: candidate,
            committed: committed_record.clone(),
        });
    }

    let committed = store
        .compare_and_set_committed(lease, committed_record, &candidate)
        .await?;
    committed.verify(&candidate)?;

    Ok(DeepXCommittedObservation {
        record: candidate,
        committed,
    })
}

/// Commits one crash-recoverable lifecycle step from classified recovery evidence.
///
/// A finalized inclusion requires two calls: the first durably records inclusion and the second,
/// using the returned acknowledgement, records finality. This preserves an exact durable boundary
/// if the process stops between those transitions.
///
/// # Errors
///
/// Returns an error when the decision is invalid for the durable record state or its single
/// compare-and-set cannot be proven committed.
pub async fn commit_recovery_decision<S>(
    store: &S,
    lease: &S::Lease,
    committed_record: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    decision: DeepXRecoveryDecision,
) -> Result<DeepXCommittedObservation, DeepXObservationCommitError>
where
    S: DeepXTransactionStore,
{
    let observation = match decision {
        DeepXRecoveryDecision::PoolAccepted => DeepXTransactionObservation::PoolAccepted,
        DeepXRecoveryDecision::FinalizedInclusion(inclusion)
            if matches!(
                record.lifecycle().state(),
                DeepXTransactionState::InBlockSuccess
                    | DeepXTransactionState::InBlockFailed
                    | DeepXTransactionState::Finalized
            ) =>
        {
            DeepXTransactionObservation::Finalized(inclusion)
        }
        DeepXRecoveryDecision::FinalizedInclusion(inclusion) => {
            DeepXTransactionObservation::Included(inclusion)
        }
        DeepXRecoveryDecision::NotIncluded(absence) => {
            DeepXTransactionObservation::NotIncluded(absence)
        }
        DeepXRecoveryDecision::ActionRequired => DeepXTransactionObservation::ActionRequired,
    };
    commit_reconciliation_observation(store, lease, committed_record, record, observation).await
}

/// Commits one lifecycle step from classified canonical reorganization evidence.
///
/// A canonical decision reapplies the exact inclusion idempotently and does not advance the
/// durable revision. A replacement decision removes only the exact recorded inclusion.
///
/// # Errors
///
/// Returns an error when the decision conflicts with the durable record or its compare-and-set
/// cannot be proven committed.
pub async fn commit_reorganization_decision<S>(
    store: &S,
    lease: &S::Lease,
    committed_record: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    decision: DeepXReorganizationDecision,
) -> Result<DeepXCommittedObservation, DeepXObservationCommitError>
where
    S: DeepXTransactionStore,
{
    let observation = match decision {
        DeepXReorganizationDecision::Canonical => record
            .lifecycle()
            .inclusion()
            .map(DeepXTransactionObservation::Included)
            .ok_or(DeepXObservationCommitError::UnsupportedObservation)?,
        DeepXReorganizationDecision::Reorganized(inclusion) => {
            DeepXTransactionObservation::Reorged(inclusion)
        }
        DeepXReorganizationDecision::ActionRequired => DeepXTransactionObservation::ActionRequired,
    };
    commit_reconciliation_observation(store, lease, committed_record, record, observation).await
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, sync::Mutex};

    use nautilus_model::{
        enums::OrderSide,
        identifiers::{ClientOrderId, InstrumentId},
    };
    use rstest::rstest;
    use subxt_core::config::{Hasher, substrate::BlakeTwo256};

    use super::*;
    use crate::{
        signing::{ApprovedRuntimeIdentity, SignedPalletExtrinsic},
        transaction::{
            DeepXDirectRuntimeIdentity, DeepXInclusionEvidence, DeepXInclusionOutcome,
            DeepXNonceReservation, DeepXTransactionIdentity, DeepXTransactionObservation,
        },
    };

    #[derive(Debug)]
    struct TestLease {
        signer: [u8; 20],
        generation: u64,
    }

    impl DeepXSignerLease for TestLease {
        fn signer(&self) -> [u8; 20] {
            self.signer
        }

        fn generation(&self) -> u64 {
            self.generation
        }
    }

    #[derive(Debug)]
    struct TestStore {
        revision: Mutex<u64>,
        encoded_record: Mutex<Vec<u8>>,
        active_generation: u64,
        create_outcome_unknown: bool,
        commit_outcome_unknown: bool,
    }

    impl TestStore {
        fn new(revision: u64, record: &DeepXTransactionRecord) -> Self {
            Self {
                revision: Mutex::new(revision),
                encoded_record: Mutex::new(record.encode().unwrap()),
                active_generation: 4,
                create_outcome_unknown: false,
                commit_outcome_unknown: false,
            }
        }

        fn empty() -> Self {
            Self {
                revision: Mutex::new(0),
                encoded_record: Mutex::new(Vec::new()),
                active_generation: 4,
                create_outcome_unknown: false,
                commit_outcome_unknown: false,
            }
        }

        fn current_revision(&self) -> u64 {
            *self.revision.lock().unwrap()
        }
    }

    #[async_trait::async_trait]
    impl DeepXTransactionStore for TestStore {
        type Lease = TestLease;

        async fn acquire_signer_lease(
            &self,
            signer: [u8; 20],
        ) -> Result<Self::Lease, DeepXTransactionPersistenceError> {
            Ok(TestLease {
                signer,
                generation: self.active_generation,
            })
        }

        async fn verify_signer_lease(
            &self,
            lease: &Self::Lease,
        ) -> Result<(), DeepXTransactionPersistenceError> {
            if lease.generation == self.active_generation {
                Ok(())
            } else {
                Err(DeepXTransactionPersistenceError::LeaseUnavailable(
                    "stale generation".to_string(),
                ))
            }
        }

        async fn load_committed_for_signer(
            &self,
            lease: &Self::Lease,
        ) -> Result<Vec<DeepXRestoredTransactionRecord>, DeepXTransactionPersistenceError> {
            self.verify_signer_lease(lease).await?;
            let encoded_record = self.encoded_record.lock().unwrap().clone();
            if encoded_record.is_empty() {
                return Ok(Vec::new());
            }
            let record = DeepXTransactionRecord::decode(&encoded_record).map_err(|error| {
                DeepXTransactionPersistenceError::BeforeCommit(error.to_string())
            })?;
            if record.identity().signer() != lease.signer() {
                return Ok(Vec::new());
            }
            let revision = DeepXTransactionRevision::new(*self.revision.lock().unwrap());
            let committed =
                DeepXCommittedTransactionRecord::acknowledge_committed(&record, revision)?;
            Ok(vec![DeepXRestoredTransactionRecord::new(
                record, committed,
            )?])
        }

        async fn create_committed(
            &self,
            _lease: &Self::Lease,
            record: &DeepXTransactionRecord,
        ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError> {
            let mut revision = self.revision.lock().unwrap();
            let mut encoded_record = self.encoded_record.lock().unwrap();
            if !encoded_record.is_empty() {
                return Err(DeepXTransactionPersistenceError::RevisionConflict);
            }
            *revision += 1;
            *encoded_record = record.encode().map_err(|error| {
                DeepXTransactionPersistenceError::BeforeCommit(error.to_string())
            })?;
            if self.create_outcome_unknown {
                return Err(DeepXTransactionPersistenceError::CommitOutcomeUnknown(
                    "acknowledgement lost".to_string(),
                ));
            }
            DeepXCommittedTransactionRecord::acknowledge_committed(
                record,
                DeepXTransactionRevision::new(*revision),
            )
        }

        async fn compare_and_set_committed(
            &self,
            _lease: &Self::Lease,
            expected: &DeepXCommittedTransactionRecord,
            record: &DeepXTransactionRecord,
        ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError> {
            let mut revision = self.revision.lock().unwrap();
            let mut encoded_record = self.encoded_record.lock().unwrap();
            if *revision != expected.revision().value()
                || expected.encoded_record != *encoded_record
                || expected.cache_key != record.cache_key_for_record()
            {
                return Err(DeepXTransactionPersistenceError::RevisionConflict);
            }
            *revision += 1;
            *encoded_record = record.encode().map_err(|error| {
                DeepXTransactionPersistenceError::BeforeCommit(error.to_string())
            })?;
            if self.commit_outcome_unknown {
                return Err(DeepXTransactionPersistenceError::CommitOutcomeUnknown(
                    "acknowledgement lost".to_string(),
                ));
            }
            DeepXCommittedTransactionRecord::acknowledge_committed(
                record,
                DeepXTransactionRevision::new(*revision),
            )
        }
    }

    #[derive(Debug)]
    struct TestVerifier {
        result: Result<(), DeepXBusinessCallBindingError>,
    }

    #[derive(Debug)]
    struct CrossSignerRestoreStore {
        restored: DeepXRestoredTransactionRecord,
    }

    #[async_trait::async_trait]
    impl DeepXTransactionStore for CrossSignerRestoreStore {
        type Lease = TestLease;

        async fn acquire_signer_lease(
            &self,
            signer: [u8; 20],
        ) -> Result<Self::Lease, DeepXTransactionPersistenceError> {
            Ok(TestLease {
                signer,
                generation: 1,
            })
        }

        async fn verify_signer_lease(
            &self,
            _lease: &Self::Lease,
        ) -> Result<(), DeepXTransactionPersistenceError> {
            Ok(())
        }

        async fn load_committed_for_signer(
            &self,
            _lease: &Self::Lease,
        ) -> Result<Vec<DeepXRestoredTransactionRecord>, DeepXTransactionPersistenceError> {
            Ok(vec![self.restored.clone()])
        }

        async fn create_committed(
            &self,
            _lease: &Self::Lease,
            _record: &DeepXTransactionRecord,
        ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError> {
            unreachable!()
        }

        async fn compare_and_set_committed(
            &self,
            _lease: &Self::Lease,
            _expected: &DeepXCommittedTransactionRecord,
            _record: &DeepXTransactionRecord,
        ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError> {
            unreachable!()
        }
    }

    impl DeepXBusinessCallVerifier for TestVerifier {
        fn verify(
            &self,
            _identity: &DeepXTransactionIdentity,
            _signed_extrinsic: &DeepXDurableSignedExtrinsic,
        ) -> Result<(), DeepXBusinessCallBindingError> {
            self.result.clone()
        }
    }

    fn record() -> DeepXTransactionRecord {
        DeepXTransactionRecord::created(DeepXTransactionIdentity::new(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            [7; 20],
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId { value: 42 },
            DeepXDirectRuntimeIdentity {
                genesis_hash: [1; 32],
                metadata_sha256: [2; 32],
                spec_version: 366,
                transaction_version: 1,
                signed_extensions: vec!["CheckNonce".to_string()],
            },
        ))
    }

    fn signed_record() -> DeepXTransactionRecord {
        let mut record = record();
        let bytes = vec![1, 2, 3];
        let identity = record.identity();
        let runtime = identity.runtime();
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
            unreachable!();
        };
        let signed = SignedPalletExtrinsic {
            extrinsic_hash: BlakeTwo256.hash(&bytes).0,
            bytes,
            signer: identity.signer(),
            nonce,
            runtime: ApprovedRuntimeIdentity {
                genesis_hash: runtime.genesis_hash,
                metadata_sha256: runtime.metadata_sha256,
                spec_version: runtime.spec_version,
                transaction_version: runtime.transaction_version,
                signed_extensions: runtime.signed_extensions.clone(),
            },
        };
        record.record_signed(&signed).unwrap();
        record
    }

    fn submitting_record() -> DeepXTransactionRecord {
        let mut record = signed_record();
        record
            .apply_observation(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        record
    }

    fn signed_for(record: &DeepXTransactionRecord) -> SignedPalletExtrinsic {
        let bytes = vec![1, 2, 3];
        let identity = record.identity();
        let runtime = identity.runtime();
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
            unreachable!();
        };
        SignedPalletExtrinsic {
            extrinsic_hash: BlakeTwo256.hash(&bytes).0,
            bytes,
            signer: identity.signer(),
            nonce,
            runtime: ApprovedRuntimeIdentity {
                genesis_hash: runtime.genesis_hash,
                metadata_sha256: runtime.metadata_sha256,
                spec_version: runtime.spec_version,
                transaction_version: runtime.transaction_version,
                signed_extensions: runtime.signed_extensions.clone(),
            },
        }
    }

    fn runtime() -> DeepXDirectRuntimeIdentity {
        DeepXDirectRuntimeIdentity {
            genesis_hash: [1; 32],
            metadata_sha256: [2; 32],
            spec_version: 366,
            transaction_version: 1,
            signed_extensions: vec!["CheckNonce".to_string()],
        }
    }

    async fn prepare_reservation(
        store: &TestStore,
        lease: &TestLease,
        allocator: &DeepXTimestampNonceAllocator,
    ) -> Result<DeepXPreparedReservation, DeepXReservationPreparationError> {
        prepare_timestamp_reservation(
            store,
            lease,
            allocator,
            1_000,
            1_001,
            ClientOrderId::new("O-19700101-000000-001-001-2"),
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            runtime(),
        )
        .await
    }

    #[tokio::test]
    async fn timestamp_reservation_is_released_only_after_durable_create() {
        let store = TestStore::empty();
        let allocator = DeepXTimestampNonceAllocator::from_records([7; 20], [], 10);
        let lease = store.acquire_signer_lease([7; 20]).await.unwrap();

        let prepared = prepare_reservation(&store, &lease, &allocator)
            .await
            .unwrap();

        assert_eq!(
            prepared.record().lifecycle().state(),
            DeepXTransactionState::Created
        );
        assert_eq!(
            prepared.record().identity().nonce(),
            DeepXNonceReservation::TimestampOrderId { value: 1_001 },
        );
        assert_eq!(prepared.committed().revision().value(), 1);
        assert!(prepared.committed().verify(prepared.record()).is_ok());
    }

    #[tokio::test]
    async fn timestamp_allocator_restores_from_complete_committed_signer_records() {
        let record = record();
        let store = TestStore::new(3, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        let (allocator, restored) = restore_timestamp_nonce_allocator(&store, &lease, 10)
            .await
            .unwrap();

        assert_eq!(allocator.signer(), record.identity().signer());
        assert_eq!(allocator.last_reserved(), Some(42));
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].record(), &record);
        assert_eq!(restored[0].committed().revision().value(), 3);
    }

    #[tokio::test]
    async fn timestamp_allocator_restore_rejects_cross_signer_records() {
        let record = record();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();
        let store = CrossSignerRestoreStore {
            restored: DeepXRestoredTransactionRecord::new(record, committed).unwrap(),
        };
        let lease = store.acquire_signer_lease([8; 20]).await.unwrap();

        assert!(matches!(
            restore_timestamp_nonce_allocator(&store, &lease, 10).await,
            Err(DeepXTransactionPersistenceError::LeaseMismatch),
        ));
    }

    #[tokio::test]
    async fn invalid_lease_does_not_allocate_timestamp_nonce() {
        let store = TestStore::empty();
        let allocator = DeepXTimestampNonceAllocator::from_records([7; 20], [], 10);
        let lease = TestLease {
            signer: [8; 20],
            generation: 4,
        };

        assert!(matches!(
            prepare_reservation(&store, &lease, &allocator).await,
            Err(DeepXReservationPreparationError::Persistence(
                DeepXTransactionPersistenceError::LeaseMismatch
            )),
        ));
        assert_eq!(allocator.last_reserved(), None);
        assert_eq!(store.current_revision(), 0);
    }

    #[tokio::test]
    async fn unknown_create_outcome_burns_timestamp_nonce_without_releasing_reservation() {
        let store = TestStore {
            create_outcome_unknown: true,
            ..TestStore::empty()
        };
        let allocator = DeepXTimestampNonceAllocator::from_records([7; 20], [], 10);
        let lease = store.acquire_signer_lease([7; 20]).await.unwrap();

        assert!(matches!(
            prepare_reservation(&store, &lease, &allocator).await,
            Err(DeepXReservationPreparationError::Persistence(
                DeepXTransactionPersistenceError::CommitOutcomeUnknown(_)
            )),
        ));
        assert_eq!(allocator.last_reserved(), Some(1_001));
        assert_eq!(store.current_revision(), 1);
    }

    #[tokio::test]
    async fn signed_transaction_is_released_only_after_durable_cas() {
        let record = record();
        let store = TestStore::new(3, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();

        let prepared =
            prepare_signed_transaction(&store, &lease, &committed, &record, |identity| {
                assert_eq!(
                    identity.client_order_id(),
                    record.identity().client_order_id()
                );
                Ok(signed_for(&record))
            })
            .await
            .unwrap();

        assert_eq!(
            prepared.record().lifecycle().state(),
            DeepXTransactionState::Signed
        );
        assert_eq!(prepared.committed().revision().value(), 4);
        assert!(prepared.committed().verify(prepared.record()).is_ok());
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn stale_created_acknowledgement_never_releases_signed_record() {
        let record = record();
        let store = TestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let stale = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();
        let signer_invoked = Cell::new(false);

        assert!(matches!(
            prepare_signed_transaction(&store, &lease, &stale, &record, |_| {
                signer_invoked.set(true);
                Ok(signed_for(&record))
            })
            .await,
            Err(DeepXSignedTransactionPreparationError::Persistence(
                DeepXTransactionPersistenceError::RevisionConflict
            )),
        ));
        assert!(signer_invoked.get());
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn unknown_signed_commit_never_releases_signed_record() {
        let record = record();
        let store = TestStore {
            revision: Mutex::new(3),
            encoded_record: Mutex::new(record.encode().unwrap()),
            active_generation: 4,
            create_outcome_unknown: false,
            commit_outcome_unknown: true,
        };
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();

        assert!(matches!(
            prepare_signed_transaction(&store, &lease, &committed, &record, |_| {
                Ok(signed_for(&record))
            })
            .await,
            Err(DeepXSignedTransactionPreparationError::Persistence(
                DeepXTransactionPersistenceError::CommitOutcomeUnknown(_)
            )),
        ));
        assert_eq!(store.current_revision(), 4);
    }

    #[rstest]
    #[case::unsupported(DeepXTransactionPersistenceError::Unsupported("cache add has no commit contract".to_string()), true)]
    #[case::lease_unavailable(DeepXTransactionPersistenceError::LeaseUnavailable("owned by another process".to_string()), true)]
    #[case::before_commit(DeepXTransactionPersistenceError::BeforeCommit("connection unavailable".to_string()), true)]
    #[case::revision_conflict(DeepXTransactionPersistenceError::RevisionConflict, true)]
    #[case::acknowledgement_mismatch(
        DeepXTransactionPersistenceError::AcknowledgementMismatch,
        true
    )]
    #[case::lease_mismatch(DeepXTransactionPersistenceError::LeaseMismatch, true)]
    #[case::commit_unknown(DeepXTransactionPersistenceError::CommitOutcomeUnknown("acknowledgement lost".to_string()), false)]
    fn persistence_failure_classifies_commit_certainty(
        #[case] error: DeepXTransactionPersistenceError,
        #[case] expected: bool,
    ) {
        assert_eq!(error.is_proven_not_committed(), expected);
    }

    #[rstest]
    fn committed_acknowledgement_is_bound_to_exact_record_encoding() {
        let mut record = record();
        let acknowledgement = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();

        assert_eq!(acknowledgement.revision().value(), 3);
        assert!(acknowledgement.verify(&record).is_ok());

        record
            .apply_observation(DeepXTransactionObservation::ActionRequired)
            .unwrap();
        assert_eq!(
            acknowledgement.verify(&record),
            Err(DeepXTransactionPersistenceError::AcknowledgementMismatch),
        );
    }

    #[rstest]
    fn signer_lease_cannot_authorize_another_account() {
        let record = record();
        let lease = TestLease {
            signer: [8; 20],
            generation: 4,
        };

        assert_eq!(lease.generation(), 4);
        assert_eq!(
            verify_signer_lease(&lease, &record),
            Err(DeepXTransactionPersistenceError::LeaseMismatch),
        );
    }

    #[rstest]
    fn default_business_call_verifier_fails_closed() {
        let record = signed_record();
        let verifier = DeepXUnsupportedBusinessCallVerifier;

        assert!(matches!(
            verifier.verify(record.identity(), record.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Unsupported(_)),
        ));
    }

    #[tokio::test]
    async fn initial_submission_releases_payload_only_after_committed_transition() {
        let record = signed_record();
        let store = TestStore::new(3, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();
        let verifier = TestVerifier { result: Ok(()) };

        let prepared = prepare_initial_submission(&store, &lease, &committed, &record, &verifier)
            .await
            .unwrap();

        assert_eq!(
            prepared.record().lifecycle().state(),
            DeepXTransactionState::Submitting,
        );
        assert_eq!(prepared.committed().revision().value(), 4);
        assert_eq!(prepared.into_permit().into_payload().0, vec![1, 2, 3]);
        assert_eq!(store.current_revision(), 4);
        assert!(matches!(
            prepare_initial_submission(&store, &lease, &committed, &record, &verifier).await,
            Err(DeepXSubmissionPreparationError::Persistence(
                DeepXTransactionPersistenceError::RevisionConflict
            )),
        ));
    }

    #[tokio::test]
    async fn failed_call_binding_does_not_advance_durable_revision() {
        let record = signed_record();
        let store = TestStore::new(3, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();
        let verifier = TestVerifier {
            result: Err(DeepXBusinessCallBindingError::Unsupported(
                "no authoritative vector".to_string(),
            )),
        };

        assert!(matches!(
            prepare_initial_submission(&store, &lease, &committed, &record, &verifier).await,
            Err(DeepXSubmissionPreparationError::CallBinding(
                DeepXBusinessCallBindingError::Unsupported(_)
            )),
        ));
        assert_eq!(store.current_revision(), 3);
    }

    #[tokio::test]
    async fn stale_lease_does_not_advance_durable_revision() {
        let record = signed_record();
        let store = TestStore::new(3, &record);
        let lease = TestLease {
            signer: record.identity().signer(),
            generation: 2,
        };
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();
        let verifier = TestVerifier { result: Ok(()) };

        assert!(matches!(
            prepare_initial_submission(&store, &lease, &committed, &record, &verifier).await,
            Err(DeepXSubmissionPreparationError::Persistence(
                DeepXTransactionPersistenceError::LeaseUnavailable(_)
            )),
        ));
        assert_eq!(store.current_revision(), 3);
    }

    #[tokio::test]
    async fn unknown_commit_outcome_never_releases_submission_payload() {
        let record = signed_record();
        let store = TestStore {
            revision: Mutex::new(3),
            encoded_record: Mutex::new(record.encode().unwrap()),
            active_generation: 4,
            create_outcome_unknown: false,
            commit_outcome_unknown: true,
        };
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();
        let verifier = TestVerifier { result: Ok(()) };

        assert!(matches!(
            prepare_initial_submission(&store, &lease, &committed, &record, &verifier).await,
            Err(DeepXSubmissionPreparationError::Persistence(
                DeepXTransactionPersistenceError::CommitOutcomeUnknown(_)
            )),
        ));
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn forged_prior_record_cannot_authorize_submission() {
        let persisted_record = signed_record();
        let mut forged_record = persisted_record.clone();
        forged_record
            .apply_observation(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        let store = TestStore::new(3, &persisted_record);
        let lease = store
            .acquire_signer_lease(forged_record.identity().signer())
            .await
            .unwrap();
        let forged_acknowledgement = DeepXCommittedTransactionRecord::acknowledge_committed(
            &forged_record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();

        assert_eq!(
            store
                .compare_and_set_committed(&lease, &forged_acknowledgement, &persisted_record)
                .await,
            Err(DeepXTransactionPersistenceError::RevisionConflict),
        );
        assert_eq!(store.current_revision(), 3);
    }

    #[tokio::test]
    async fn reconciliation_observation_is_released_only_after_durable_cas() {
        let record = submitting_record();
        let store = TestStore::new(3, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();

        let result = commit_reconciliation_observation(
            &store,
            &lease,
            &committed,
            &record,
            DeepXTransactionObservation::PoolAccepted,
        )
        .await
        .unwrap();

        assert_eq!(
            result.record().lifecycle().state(),
            DeepXTransactionState::Accepted
        );
        assert_eq!(result.committed().revision().value(), 4);
        assert!(result.committed().verify(result.record()).is_ok());
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn recovery_finalized_inclusion_commits_in_two_durable_steps() {
        let record = submitting_record();
        let store = TestStore::new(3, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();
        let inclusion = DeepXInclusionEvidence {
            block_hash: [8; 32],
            block_number: 72,
            extrinsic_index: 4,
            outcome: DeepXInclusionOutcome::Success,
        };
        let decision = DeepXRecoveryDecision::FinalizedInclusion(inclusion);

        let included = commit_recovery_decision(&store, &lease, &committed, &record, decision)
            .await
            .unwrap();
        assert_eq!(
            included.record().lifecycle().state(),
            DeepXTransactionState::InBlockSuccess,
        );
        assert_eq!(included.committed().revision().value(), 4);

        let finalized = commit_recovery_decision(
            &store,
            &lease,
            included.committed(),
            included.record(),
            decision,
        )
        .await
        .unwrap();
        assert_eq!(
            finalized.record().lifecycle().state(),
            DeepXTransactionState::Finalized,
        );
        assert_eq!(finalized.committed().revision().value(), 5);
        assert_eq!(store.current_revision(), 5);
    }

    #[tokio::test]
    async fn recovery_action_required_is_durably_committed() {
        let record = submitting_record();
        let store = TestStore::new(3, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();

        let result = commit_recovery_decision(
            &store,
            &lease,
            &committed,
            &record,
            DeepXRecoveryDecision::ActionRequired,
        )
        .await
        .unwrap();

        assert_eq!(
            result.record().lifecycle().state(),
            DeepXTransactionState::ActionRequired,
        );
        assert_eq!(result.committed().revision().value(), 4);
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn repeated_reconciliation_observation_preserves_durable_revision() {
        let mut record = submitting_record();
        record
            .apply_observation(DeepXTransactionObservation::PoolAccepted)
            .unwrap();
        let store = TestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(4),
        )
        .unwrap();

        let result = commit_reconciliation_observation(
            &store,
            &lease,
            &committed,
            &record,
            DeepXTransactionObservation::PoolAccepted,
        )
        .await
        .unwrap();

        assert_eq!(result.committed().revision().value(), 4);
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn reorganization_is_committed_once_and_requires_fresh_reconciliation() {
        let mut record = submitting_record();
        let inclusion = DeepXInclusionEvidence {
            block_hash: [8; 32],
            block_number: 72,
            extrinsic_index: 4,
            outcome: DeepXInclusionOutcome::Success,
        };
        record
            .apply_observation(DeepXTransactionObservation::Included(inclusion))
            .unwrap();
        let store = TestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(4),
        )
        .unwrap();

        let reverted = commit_reconciliation_observation(
            &store,
            &lease,
            &committed,
            &record,
            DeepXTransactionObservation::Reorged(inclusion),
        )
        .await
        .unwrap();
        assert_eq!(
            reverted.record().lifecycle().state(),
            DeepXTransactionState::Submitting,
        );
        assert_eq!(
            reverted.record().lifecycle().reverted_inclusion(),
            Some(inclusion),
        );
        assert_eq!(reverted.committed().revision().value(), 5);

        let repeated = commit_reconciliation_observation(
            &store,
            &lease,
            reverted.committed(),
            reverted.record(),
            DeepXTransactionObservation::Reorged(inclusion),
        )
        .await
        .unwrap();
        assert_eq!(repeated.committed().revision().value(), 5);
        assert_eq!(store.current_revision(), 5);
    }

    #[tokio::test]
    async fn canonical_reorganization_decision_preserves_durable_revision() {
        let mut record = submitting_record();
        let inclusion = DeepXInclusionEvidence {
            block_hash: [8; 32],
            block_number: 72,
            extrinsic_index: 4,
            outcome: DeepXInclusionOutcome::Success,
        };
        record
            .apply_observation(DeepXTransactionObservation::Included(inclusion))
            .unwrap();
        let store = TestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(4),
        )
        .unwrap();

        let result = commit_reorganization_decision(
            &store,
            &lease,
            &committed,
            &record,
            DeepXReorganizationDecision::Canonical,
        )
        .await
        .unwrap();

        assert_eq!(result.record().lifecycle().inclusion(), Some(inclusion));
        assert_eq!(result.committed().revision().value(), 4);
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn reorganized_decision_removes_exact_inclusion_durably() {
        let mut record = submitting_record();
        let inclusion = DeepXInclusionEvidence {
            block_hash: [8; 32],
            block_number: 72,
            extrinsic_index: 4,
            outcome: DeepXInclusionOutcome::Success,
        };
        record
            .apply_observation(DeepXTransactionObservation::Included(inclusion))
            .unwrap();
        let store = TestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(4),
        )
        .unwrap();

        let result = commit_reorganization_decision(
            &store,
            &lease,
            &committed,
            &record,
            DeepXReorganizationDecision::Reorganized(inclusion),
        )
        .await
        .unwrap();

        assert_eq!(
            result.record().lifecycle().state(),
            DeepXTransactionState::Submitting,
        );
        assert_eq!(
            result.record().lifecycle().reverted_inclusion(),
            Some(inclusion),
        );
        assert_eq!(result.committed().revision().value(), 5);
        assert_eq!(store.current_revision(), 5);
    }

    #[tokio::test]
    async fn uncertain_reorganization_decision_is_durably_fail_closed() {
        let mut record = submitting_record();
        let inclusion = DeepXInclusionEvidence {
            block_hash: [8; 32],
            block_number: 72,
            extrinsic_index: 4,
            outcome: DeepXInclusionOutcome::Success,
        };
        record
            .apply_observation(DeepXTransactionObservation::Included(inclusion))
            .unwrap();
        let store = TestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(4),
        )
        .unwrap();

        let result = commit_reorganization_decision(
            &store,
            &lease,
            &committed,
            &record,
            DeepXReorganizationDecision::ActionRequired,
        )
        .await
        .unwrap();

        assert_eq!(
            result.record().lifecycle().state(),
            DeepXTransactionState::ActionRequired,
        );
        assert_eq!(result.committed().revision().value(), 5);
        assert_eq!(store.current_revision(), 5);
    }

    #[tokio::test]
    async fn stale_revision_never_releases_reconciliation_observation() {
        let record = submitting_record();
        let store = TestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let stale = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();

        assert!(matches!(
            commit_reconciliation_observation(
                &store,
                &lease,
                &stale,
                &record,
                DeepXTransactionObservation::PoolAccepted,
            )
            .await,
            Err(DeepXObservationCommitError::Persistence(
                DeepXTransactionPersistenceError::RevisionConflict
            )),
        ));
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn unknown_commit_never_releases_reconciliation_observation() {
        let record = submitting_record();
        let store = TestStore {
            revision: Mutex::new(3),
            encoded_record: Mutex::new(record.encode().unwrap()),
            active_generation: 4,
            create_outcome_unknown: false,
            commit_outcome_unknown: true,
        };
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();

        assert!(matches!(
            commit_reconciliation_observation(
                &store,
                &lease,
                &committed,
                &record,
                DeepXTransactionObservation::PoolAccepted,
            )
            .await,
            Err(DeepXObservationCommitError::Persistence(
                DeepXTransactionPersistenceError::CommitOutcomeUnknown(_)
            )),
        ));
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn reconciliation_cannot_bypass_signing_or_submission_preparation() {
        let record = signed_record();
        let store = TestStore::new(3, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(3),
        )
        .unwrap();

        for observation in [
            DeepXTransactionObservation::Signed {
                extrinsic_hash: record.signed_extrinsic().unwrap().extrinsic_hash(),
            },
            DeepXTransactionObservation::SubmissionStarted,
        ] {
            assert!(matches!(
                commit_reconciliation_observation(
                    &store,
                    &lease,
                    &committed,
                    &record,
                    observation,
                )
                .await,
                Err(DeepXObservationCommitError::UnsupportedObservation),
            ));
        }
        assert_eq!(store.current_revision(), 3);
    }
}
