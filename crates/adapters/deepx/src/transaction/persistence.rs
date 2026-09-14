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

use std::fmt::{self, Debug};

use nautilus_model::{
    enums::OrderSide,
    identifiers::{ClientOrderId, InstrumentId},
};
pub use postgres::{DeepXPostgresSignerLease, DeepXPostgresTransactionStore};
use subxt_core::dynamic::Value;
use thiserror::Error;

use super::{
    DeepXDirectRuntimeIdentity, DeepXDurableSignedExtrinsic, DeepXFinalityObservation,
    DeepXFinalizedRecoveryCollection, DeepXNonceReservation, DeepXRecoveryDecision,
    DeepXReorganizationDecision, DeepXSubmittedExtrinsic, DeepXTimestampNonceAllocator,
    DeepXTimestampNonceError, DeepXTransactionIdentity, DeepXTransactionObservation,
    DeepXTransactionRecord, DeepXTransactionRecordError, DeepXTransactionState,
    DeepXTransactionWatchError, collect_finalized_recovery_scan_with_event_evidence,
    observe_finality, observe_reorganization, observe_submission_pool,
};
use crate::{
    common::DeepXPrivateKey,
    config::DeepXValidatedRpcEndpoints,
    rpc::DeepXValidatedRpcMethodCapabilities,
    signing::{
        DeepXPerpCancelParams, DeepXSpotPlaceParams, RuntimeSnapshot, SignedPalletExtrinsic,
        SigningError, derive_signer_account_id, sign_dynamic_pallet_call_with_snapshot,
        sign_perp_cancel, sign_spot_place_order,
    },
};

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

/// Loads and verifies the complete durable transaction set for a signer lease.
///
/// # Errors
///
/// Returns an error if the lease is not current, an acknowledgement does not cover the exact
/// record, or any record belongs to another signer.
pub async fn load_verified_committed_for_signer<S>(
    store: &S,
    lease: &S::Lease,
) -> Result<Vec<DeepXRestoredTransactionRecord>, DeepXTransactionPersistenceError>
where
    S: DeepXTransactionStore,
{
    store.verify_signer_lease(lease).await?;
    let restored = store.load_committed_for_signer(lease).await?;
    for item in &restored {
        item.committed().verify(item.record())?;
        verify_signer_lease(lease, item.record())?;
    }
    Ok(restored)
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
    let restored = load_verified_committed_for_signer(store, lease).await?;
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
    /// The operation or signed bytes do not match the reserved close identity.
    #[error(transparent)]
    Binding(#[from] DeepXBusinessCallBindingError),
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

/// Signs and commits a reservation only after explicit business-call verification.
///
/// All raw call arguments come from the durable identity. Exact call verification precedes
/// the signed-record compare-and-set. This proves checkpoint integrity, not business success,
/// financial units, subaccount authorization, or independent SDK parity.
///
/// # Errors
///
/// Returns an error if ownership, acknowledgement, state, operation, runtime, signing, exact
/// call binding, or durable compare-and-set cannot be proven.
pub async fn prepare_signed_transaction_with_verifier<S, F, V>(
    store: &S,
    lease: &S::Lease,
    committed_created: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    signer: F,
    verifier: &V,
) -> Result<DeepXPreparedSignedTransaction, DeepXSignedTransactionPreparationError>
where
    S: DeepXTransactionStore,
    F: FnOnce(&DeepXTransactionIdentity) -> Result<SignedPalletExtrinsic, SigningError>,
    V: DeepXBusinessCallVerifier + ?Sized,
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
    let durable = signed_record.signed_extrinsic().ok_or_else(|| {
        DeepXBusinessCallBindingError::Mismatch("signed evidence is missing".to_string())
    })?;
    verifier.verify(record.identity(), durable)?;
    let committed = store
        .compare_and_set_committed(lease, committed_created, &signed_record)
        .await?;
    committed.verify(&signed_record)?;
    Ok(DeepXPreparedSignedTransaction {
        record: signed_record,
        committed,
    })
}

/// Signs and commits an acknowledged perpetual close reservation without submission.
///
/// All raw call arguments come from the durable identity. Exact call verification precedes
/// the signed-record compare-and-set. This proves checkpoint integrity, not business success,
/// financial units, subaccount authorization, or independent SDK parity.
///
/// # Errors
///
/// Returns an error if ownership, acknowledgement, state, operation, runtime, signing, exact
/// call binding, or durable compare-and-set cannot be proven.
pub async fn prepare_signed_perp_close_transaction<S>(
    store: &S,
    lease: &S::Lease,
    committed_created: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    permit: &crate::signing::DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
) -> Result<DeepXPreparedSignedTransaction, DeepXSignedTransactionPreparationError>
where
    S: DeepXTransactionStore,
{
    verify_signer_lease(lease, record)?;
    store.verify_signer_lease(lease).await?;
    committed_created.verify(record)?;
    if record.lifecycle().state() != DeepXTransactionState::Created {
        return Err(DeepXSignedTransactionPreparationError::InvalidState);
    }
    let identity = record.identity();
    let Some(super::DeepXTransactionOperation::PerpClose {
        subaccount,
        market_id,
        price,
        slippage,
    }) = identity.operation()
    else {
        return Err(DeepXBusinessCallBindingError::Unsupported(
            "durable identity is not a perpetual close operation".to_string(),
        )
        .into());
    };
    let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
        return Err(DeepXBusinessCallBindingError::Unsupported(
            "sequential account nonce domain remains unproven".to_string(),
        )
        .into());
    };
    let verifier = DeepXPerpCloseCallVerifier::new(permit.snapshot().clone(), key.clone())?;
    let signed = crate::signing::sign_perp_close(
        permit,
        key,
        crate::signing::DeepXPerpCloseParams {
            subaccount: *subaccount,
            market_id: *market_id,
            price: *price,
            slippage: *slippage,
        },
        nonce,
    )?;
    let mut signed_record = record.clone();
    signed_record.record_signed(&signed)?;
    let durable = signed_record.signed_extrinsic().ok_or_else(|| {
        DeepXBusinessCallBindingError::Mismatch("signed close evidence is missing".to_string())
    })?;
    verifier.verify(identity, durable)?;
    let committed = store
        .compare_and_set_committed(lease, committed_created, &signed_record)
        .await?;
    committed.verify(&signed_record)?;
    Ok(DeepXPreparedSignedTransaction {
        record: signed_record,
        committed,
    })
}

/// Signs and commits an acknowledged perpetual cancel reservation without submission.
///
/// All call arguments and the timestamp nonce come from the durable identity. Canonical
/// reconstruction proves exact binding, not independent SDK parity or operational permission.
/// No signed record is released until its exact compare-and-set acknowledgement is verified.
///
/// # Errors
///
/// Returns an error if ownership, acknowledgement, state, operation, runtime, signing,
/// exact binding, or the durable compare-and-set cannot be proven.
pub async fn prepare_signed_perp_cancel_transaction<S>(
    store: &S,
    lease: &S::Lease,
    committed_created: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    permit: &crate::signing::DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
) -> Result<DeepXPreparedSignedTransaction, DeepXSignedTransactionPreparationError>
where
    S: DeepXTransactionStore,
{
    verify_signer_lease(lease, record)?;
    store.verify_signer_lease(lease).await?;
    committed_created.verify(record)?;
    if record.lifecycle().state() != DeepXTransactionState::Created {
        return Err(DeepXSignedTransactionPreparationError::InvalidState);
    }
    let identity = record.identity();
    let Some(super::DeepXTransactionOperation::PerpCancel {
        subaccount,
        order_id,
        market_id,
        fast_cancel,
    }) = identity.operation()
    else {
        return Err(DeepXBusinessCallBindingError::Unsupported(
            "durable identity is not a perpetual cancel operation".to_string(),
        )
        .into());
    };
    let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
        return Err(DeepXBusinessCallBindingError::Unsupported(
            "sequential account nonce domain remains unproven".to_string(),
        )
        .into());
    };
    let verifier = DeepXPerpCancelCallVerifier::new(permit.snapshot().clone(), key.clone())?;
    if verifier.signer != identity.signer()
        || DeepXDirectRuntimeIdentity::from(permit.snapshot().identity()) != *identity.runtime()
    {
        return Err(DeepXBusinessCallBindingError::Mismatch(
            "perpetual cancel signing key or runtime differs from reserved identity".to_string(),
        )
        .into());
    }
    let signed = sign_perp_cancel(
        permit,
        key,
        DeepXPerpCancelParams {
            subaccount: *subaccount,
            order_id: *order_id,
            market_id: *market_id,
            fast_cancel: *fast_cancel,
        },
        nonce,
    )?;
    let mut signed_record = record.clone();
    signed_record.record_signed(&signed)?;
    let durable = signed_record.signed_extrinsic().ok_or_else(|| {
        DeepXBusinessCallBindingError::Mismatch("signed cancel evidence is missing".to_string())
    })?;
    verifier.verify(identity, durable)?;
    let committed = store
        .compare_and_set_committed(lease, committed_created, &signed_record)
        .await?;
    committed.verify(&signed_record)?;
    Ok(DeepXPreparedSignedTransaction {
        record: signed_record,
        committed,
    })
}

/// Signs and commits an acknowledged Spot cancel reservation without submission.
///
/// All call arguments and the timestamp nonce come from the durable identity. Canonical
/// reconstruction proves exact binding, not SDK parity or live authority. Fast cancel signing
/// does not enable inclusion or recovery without approved spec366 event evidence.
///
/// # Errors
///
/// Returns an error if ownership, acknowledgement, state, operation, runtime, signing,
/// exact binding, or the durable compare-and-set cannot be proven.
pub async fn prepare_signed_spot_cancel_transaction<S>(
    store: &S,
    lease: &S::Lease,
    committed_created: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    permit: &crate::signing::DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
) -> Result<DeepXPreparedSignedTransaction, DeepXSignedTransactionPreparationError>
where
    S: DeepXTransactionStore,
{
    verify_signer_lease(lease, record)?;
    store.verify_signer_lease(lease).await?;
    committed_created.verify(record)?;
    if record.lifecycle().state() != DeepXTransactionState::Created {
        return Err(DeepXSignedTransactionPreparationError::InvalidState);
    }
    let identity = record.identity();
    let Some(super::DeepXTransactionOperation::SpotCancel {
        subaccount,
        pair,
        order_id,
        is_buy,
        fast_cancel,
    }) = identity.operation()
    else {
        return Err(DeepXBusinessCallBindingError::Unsupported(
            "durable identity is not a Spot cancel operation".to_string(),
        )
        .into());
    };
    let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
        return Err(DeepXBusinessCallBindingError::Unsupported(
            "sequential account nonce domain remains unproven".to_string(),
        )
        .into());
    };
    let verifier = DeepXSpotCancelCallVerifier::new(permit.snapshot().clone(), key.clone())?;
    if verifier.signer != identity.signer()
        || DeepXDirectRuntimeIdentity::from(permit.snapshot().identity()) != *identity.runtime()
    {
        return Err(DeepXBusinessCallBindingError::Mismatch(
            "Spot cancel signing key or runtime differs from reserved identity".to_string(),
        )
        .into());
    }
    let signed = crate::signing::sign_spot_cancel(
        permit,
        key,
        crate::signing::DeepXSpotCancelParams {
            subaccount: *subaccount,
            pair: *pair,
            order_id: *order_id,
            is_buy: *is_buy,
            fast_cancel: *fast_cancel,
        },
        nonce,
    )?;
    let mut signed_record = record.clone();
    signed_record.record_signed(&signed)?;
    let durable = signed_record.signed_extrinsic().ok_or_else(|| {
        DeepXBusinessCallBindingError::Mismatch(
            "signed Spot cancel evidence is missing".to_string(),
        )
    })?;
    verifier.verify(identity, durable)?;
    let committed = store
        .compare_and_set_committed(lease, committed_created, &signed_record)
        .await?;
    committed.verify(&signed_record)?;
    Ok(DeepXPreparedSignedTransaction {
        record: signed_record,
        committed,
    })
}

/// Signs and commits an acknowledged Spot place reservation without submission.
///
/// All call arguments and the timestamp order ID come from the durable identity. Canonical
/// reconstruction proves exact binding but does not enable submission or trading capability.
///
/// # Errors
///
/// Returns an error if ownership, acknowledgement, state, operation, side, runtime, signing,
/// exact binding, or the durable compare-and-set cannot be proven.
pub async fn prepare_signed_spot_place_transaction<S>(
    store: &S,
    lease: &S::Lease,
    committed_created: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    permit: &crate::signing::DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
) -> Result<DeepXPreparedSignedTransaction, DeepXSignedTransactionPreparationError>
where
    S: DeepXTransactionStore,
{
    verify_signer_lease(lease, record)?;
    store.verify_signer_lease(lease).await?;
    committed_created.verify(record)?;
    if record.lifecycle().state() != DeepXTransactionState::Created {
        return Err(DeepXSignedTransactionPreparationError::InvalidState);
    }
    let identity = record.identity();
    let Some(super::DeepXTransactionOperation::SpotPlace {
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
        return Err(DeepXBusinessCallBindingError::Unsupported(
            "durable identity is not a Spot place operation".to_string(),
        )
        .into());
    };
    let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
        return Err(DeepXBusinessCallBindingError::Unsupported(
            "sequential account nonce domain remains unproven".to_string(),
        )
        .into());
    };
    let verifier = DeepXSpotPlaceCallVerifier::new(permit.snapshot().clone(), key.clone())?;
    if verifier.signer != identity.signer()
        || DeepXDirectRuntimeIdentity::from(permit.snapshot().identity()) != *identity.runtime()
    {
        return Err(DeepXBusinessCallBindingError::Mismatch(
            "Spot place signing key or runtime differs from reserved identity".to_string(),
        )
        .into());
    }
    let signed = sign_spot_place_order(
        permit,
        key,
        DeepXSpotPlaceParams {
            subaccount: *subaccount,
            pair: *pair,
            is_buy: *is_buy,
            quote_amount: *quote_amount,
            base_amount: *base_amount,
            order_type: *order_type,
            post_only: *post_only,
            reduce_only: *reduce_only,
        },
        nonce,
    )?;
    let mut signed_record = record.clone();
    signed_record.record_signed(&signed)?;
    let durable = signed_record.signed_extrinsic().ok_or_else(|| {
        DeepXBusinessCallBindingError::Mismatch("signed Spot place evidence is missing".to_string())
    })?;
    verifier.verify(identity, durable)?;
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

/// Fixture-gated verifier for the one business call whose encoding is proven today.
///
/// This verifier binds a `System.remark` signed payload to its reserved transaction identity by
/// deterministically re-signing the canonical remark payload derived from that identity against
/// the verifier's approved runtime snapshot, then requiring byte-for-byte equality with the
/// durable signed extrinsic. Because the pinned signing path is deterministic for a fixed
/// snapshot, key, nonce, and call, equality proves that the durable bytes encode exactly the
/// reserved signer, nonce, runtime, and canonical business payload.
///
/// The canonical payload embeds the client order ID, instrument, side, nonce, and runtime spec
/// version, so a durable extrinsic signed for any other identity cannot compare equal. The
/// remark call mutates no chain state; every DeepX order call remains unsupported here pending
/// authoritative golden vectors.
///
/// The key is retained only to re-derive the deterministic comparison bytes. The verifier
/// performs no network I/O and never releases the retained bytes.
#[derive(Clone)]
pub struct DeepXRemarkCallVerifier {
    snapshot: RuntimeSnapshot,
    key: DeepXPrivateKey,
}

impl DeepXRemarkCallVerifier {
    /// Binds remark verification to an approved snapshot and the reserved signing key.
    ///
    /// # Errors
    ///
    /// Returns an error if the key is rejected by the pinned DeepX signer implementation.
    pub fn new(snapshot: RuntimeSnapshot, key: DeepXPrivateKey) -> Result<Self, SigningError> {
        derive_signer_account_id(&key)?;
        Ok(Self { snapshot, key })
    }

    /// Derives the canonical remark payload binding one reserved transaction identity.
    ///
    /// The payload embeds the client order ID, instrument, side, timestamp nonce, and runtime
    /// spec version, so a remark signed for any other identity compares unequal.
    #[must_use]
    pub fn canonical_remark_payload(identity: &DeepXTransactionIdentity, nonce: u64) -> Vec<u8> {
        format!(
            "nautilus-deepx:remark:v1:{}:{}:{}:{}:{}",
            identity.client_order_id(),
            identity.instrument_id(),
            identity.order_side(),
            nonce,
            identity.runtime().spec_version,
        )
        .into_bytes()
    }

    fn canonical_signed_remark(
        &self,
        identity: &DeepXTransactionIdentity,
    ) -> Result<SignedPalletExtrinsic, DeepXBusinessCallBindingError> {
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
            return Err(DeepXBusinessCallBindingError::Unsupported(
                "sequential account nonce domain remains unproven".to_string(),
            ));
        };
        sign_dynamic_pallet_call_with_snapshot(
            &self.snapshot,
            &self.key,
            "System",
            "remark",
            vec![Value::from_bytes(Self::canonical_remark_payload(
                identity, nonce,
            ))],
            nonce,
        )
        .map_err(|error| {
            DeepXBusinessCallBindingError::Unsupported(format!(
                "canonical DeepX remark call could not be encoded: {error}"
            ))
        })
    }
}

impl DeepXBusinessCallVerifier for DeepXRemarkCallVerifier {
    fn verify(
        &self,
        identity: &DeepXTransactionIdentity,
        signed_extrinsic: &DeepXDurableSignedExtrinsic,
    ) -> Result<(), DeepXBusinessCallBindingError> {
        if DeepXDirectRuntimeIdentity::from(self.snapshot.identity()) != *identity.runtime() {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "verifier runtime snapshot does not match the reserved runtime".to_string(),
            ));
        }
        let canonical = self.canonical_signed_remark(identity)?;
        if canonical.bytes() != signed_extrinsic.bytes() {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "durable bytes are not the canonical remark for this identity".to_string(),
            ));
        }
        if canonical.extrinsic_hash() != signed_extrinsic.extrinsic_hash() {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "durable hash does not match the canonical remark hash".to_string(),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for DeepXRemarkCallVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeepXRemarkCallVerifier")
            .field("snapshot", &self.snapshot.identity())
            .field("key", &"<redacted>")
            .finish()
    }
}

/// Opt-in exact-call verifier for an offline perpetual close reservation.
///
/// Deterministic reconstruction proves byte binding, not independent SDK parity, financial
/// units, subaccount authorization, or operational close capability.
#[derive(Clone)]
pub struct DeepXPerpCloseCallVerifier {
    snapshot: RuntimeSnapshot,
    key: DeepXPrivateKey,
    signer: [u8; 20],
}

impl DeepXPerpCloseCallVerifier {
    /// Binds close verification to an approved snapshot and signing key.
    ///
    /// # Errors
    ///
    /// Returns an error if the pinned signer rejects the key.
    pub fn new(snapshot: RuntimeSnapshot, key: DeepXPrivateKey) -> Result<Self, SigningError> {
        let signer = derive_signer_account_id(&key)?;
        Ok(Self {
            snapshot,
            key,
            signer,
        })
    }
}

impl DeepXBusinessCallVerifier for DeepXPerpCloseCallVerifier {
    fn verify(
        &self,
        identity: &DeepXTransactionIdentity,
        signed_extrinsic: &DeepXDurableSignedExtrinsic,
    ) -> Result<(), DeepXBusinessCallBindingError> {
        let actual_hash: [u8; 32] = subxt_core::config::Hasher::hash(
            &subxt_core::config::substrate::BlakeTwo256,
            signed_extrinsic.bytes(),
        )
        .into();
        if signed_extrinsic.bytes().is_empty() || actual_hash != signed_extrinsic.extrinsic_hash() {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "durable perpetual close payload has invalid byte or hash integrity".to_string(),
            ));
        }
        if self.signer != identity.signer()
            || DeepXDirectRuntimeIdentity::from(self.snapshot.identity()) != *identity.runtime()
        {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "perpetual close verifier signer or runtime differs from reserved identity"
                    .to_string(),
            ));
        }
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
            return Err(DeepXBusinessCallBindingError::Unsupported(
                "sequential account nonce domain remains unproven".to_string(),
            ));
        };
        let Some(super::DeepXTransactionOperation::PerpClose {
            subaccount,
            market_id,
            price,
            slippage,
        }) = identity.operation()
        else {
            return Err(DeepXBusinessCallBindingError::Unsupported(
                "durable identity is not a perpetual close operation".to_string(),
            ));
        };
        let service = crate::signing::DeepXRuntimeSnapshotService::new(self.snapshot.clone());
        let permit = service.acquire().map_err(|e| {
            DeepXBusinessCallBindingError::Unsupported(format!("runtime permit unavailable: {e}"))
        })?;
        let canonical = crate::signing::sign_perp_close(
            &permit,
            &self.key,
            crate::signing::DeepXPerpCloseParams {
                subaccount: *subaccount,
                market_id: *market_id,
                price: *price,
                slippage: *slippage,
            },
            nonce,
        )
        .map_err(|e| {
            DeepXBusinessCallBindingError::Unsupported(format!(
                "canonical perpetual close could not be encoded: {e}"
            ))
        })?;
        if canonical.bytes() != signed_extrinsic.bytes()
            || canonical.extrinsic_hash() != signed_extrinsic.extrinsic_hash()
        {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "durable bytes are not the canonical perpetual close for this identity".to_string(),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for DeepXPerpCloseCallVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeepXPerpCloseCallVerifier")
            .field("snapshot", &self.snapshot.identity())
            .field("key", &"<redacted>")
            .finish()
    }
}

/// Fixture-gated verifier for a durable direct perpetual cancel operation.
#[derive(Clone)]
pub struct DeepXPerpCancelCallVerifier {
    snapshot: RuntimeSnapshot,
    key: DeepXPrivateKey,
    signer: [u8; 20],
}

impl DeepXPerpCancelCallVerifier {
    /// Binds perpetual cancel verification to an approved snapshot and signing key.
    ///
    /// # Errors
    ///
    /// Returns an error if the key is rejected by the pinned DeepX signer implementation.
    pub fn new(snapshot: RuntimeSnapshot, key: DeepXPrivateKey) -> Result<Self, SigningError> {
        let signer = derive_signer_account_id(&key)?;
        Ok(Self {
            snapshot,
            key,
            signer,
        })
    }

    fn canonical_signed_cancel(
        &self,
        identity: &DeepXTransactionIdentity,
    ) -> Result<SignedPalletExtrinsic, DeepXBusinessCallBindingError> {
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
            return Err(DeepXBusinessCallBindingError::Unsupported(
                "sequential account nonce domain remains unproven".to_string(),
            ));
        };
        let Some(super::DeepXTransactionOperation::PerpCancel {
            subaccount,
            order_id,
            market_id,
            fast_cancel,
        }) = identity.operation()
        else {
            return Err(DeepXBusinessCallBindingError::Unsupported(
                "durable identity is not a proven perpetual cancel operation".to_string(),
            ));
        };
        let service = crate::signing::DeepXRuntimeSnapshotService::new(self.snapshot.clone());
        let permit = service.acquire().map_err(|e| {
            DeepXBusinessCallBindingError::Unsupported(format!(
                "approved DeepX runtime snapshot is unavailable: {e}"
            ))
        })?;
        sign_perp_cancel(
            &permit,
            &self.key,
            DeepXPerpCancelParams {
                subaccount: *subaccount,
                order_id: *order_id,
                market_id: *market_id,
                fast_cancel: *fast_cancel,
            },
            nonce,
        )
        .map_err(|e| {
            DeepXBusinessCallBindingError::Unsupported(format!(
                "canonical DeepX perpetual cancel could not be encoded: {e}"
            ))
        })
    }
}

impl DeepXBusinessCallVerifier for DeepXPerpCancelCallVerifier {
    fn verify(
        &self,
        identity: &DeepXTransactionIdentity,
        signed_extrinsic: &DeepXDurableSignedExtrinsic,
    ) -> Result<(), DeepXBusinessCallBindingError> {
        let actual_hash: [u8; 32] = subxt_core::config::Hasher::hash(
            &subxt_core::config::substrate::BlakeTwo256,
            signed_extrinsic.bytes(),
        )
        .into();
        if signed_extrinsic.bytes().is_empty() || actual_hash != signed_extrinsic.extrinsic_hash() {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "durable perpetual cancel payload has invalid byte or hash integrity".to_string(),
            ));
        }
        if self.signer != identity.signer() {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "verifier signing key does not match the reserved signer".to_string(),
            ));
        }
        if DeepXDirectRuntimeIdentity::from(self.snapshot.identity()) != *identity.runtime() {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "verifier runtime snapshot does not match the reserved runtime".to_string(),
            ));
        }
        let canonical = self.canonical_signed_cancel(identity)?;
        if canonical.bytes() != signed_extrinsic.bytes() {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "durable bytes are not the canonical perpetual cancel for this identity"
                    .to_string(),
            ));
        }
        if canonical.extrinsic_hash() != signed_extrinsic.extrinsic_hash() {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "durable hash does not match the canonical perpetual cancel hash".to_string(),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for DeepXPerpCancelCallVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeepXPerpCancelCallVerifier")
            .field("snapshot", &self.snapshot.identity())
            .field("key", &"<redacted>")
            .finish()
    }
}

/// Fixture-gated, opt-in verifier for an offline direct Spot place operation.
#[derive(Clone)]
pub struct DeepXSpotPlaceCallVerifier {
    snapshot: RuntimeSnapshot,
    key: DeepXPrivateKey,
    signer: [u8; 20],
}

impl DeepXSpotPlaceCallVerifier {
    /// Binds Spot place verification to an approved snapshot and signing key.
    ///
    /// # Errors
    ///
    /// Returns an error if the pinned signer rejects the key.
    pub fn new(snapshot: RuntimeSnapshot, key: DeepXPrivateKey) -> Result<Self, SigningError> {
        let signer = derive_signer_account_id(&key)?;
        Ok(Self {
            snapshot,
            key,
            signer,
        })
    }
}

impl DeepXBusinessCallVerifier for DeepXSpotPlaceCallVerifier {
    fn verify(
        &self,
        identity: &DeepXTransactionIdentity,
        signed_extrinsic: &DeepXDurableSignedExtrinsic,
    ) -> Result<(), DeepXBusinessCallBindingError> {
        let actual_hash: [u8; 32] = subxt_core::config::Hasher::hash(
            &subxt_core::config::substrate::BlakeTwo256,
            signed_extrinsic.bytes(),
        )
        .into();
        if signed_extrinsic.bytes().is_empty() || actual_hash != signed_extrinsic.extrinsic_hash() {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "durable Spot place payload has invalid byte or hash integrity".to_string(),
            ));
        }
        if self.signer != identity.signer()
            || DeepXDirectRuntimeIdentity::from(self.snapshot.identity()) != *identity.runtime()
        {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "Spot place verifier signer or runtime differs from reserved identity".to_string(),
            ));
        }
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
            return Err(DeepXBusinessCallBindingError::Unsupported(
                "sequential account nonce domain remains unproven".to_string(),
            ));
        };
        let Some(super::DeepXTransactionOperation::SpotPlace {
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
            return Err(DeepXBusinessCallBindingError::Unsupported(
                "durable identity is not a Spot place operation".to_string(),
            ));
        };
        let expected_side = if *is_buy {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        if identity.order_side() != expected_side {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "Spot place side differs from reserved order side".to_string(),
            ));
        }
        let service = crate::signing::DeepXRuntimeSnapshotService::new(self.snapshot.clone());
        let permit = service.acquire().map_err(|e| {
            DeepXBusinessCallBindingError::Unsupported(format!("runtime permit unavailable: {e}"))
        })?;
        let canonical = sign_spot_place_order(
            &permit,
            &self.key,
            DeepXSpotPlaceParams {
                subaccount: *subaccount,
                pair: *pair,
                is_buy: *is_buy,
                quote_amount: *quote_amount,
                base_amount: *base_amount,
                order_type: *order_type,
                post_only: *post_only,
                reduce_only: *reduce_only,
            },
            nonce,
        )
        .map_err(|e| {
            DeepXBusinessCallBindingError::Unsupported(format!(
                "canonical Spot place could not be encoded: {e}"
            ))
        })?;
        if canonical.bytes() != signed_extrinsic.bytes()
            || canonical.extrinsic_hash() != signed_extrinsic.extrinsic_hash()
        {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "durable bytes are not the canonical Spot place for this identity".to_string(),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for DeepXSpotPlaceCallVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeepXSpotPlaceCallVerifier")
            .field("snapshot", &self.snapshot.identity())
            .field("key", &"<redacted>")
            .finish()
    }
}

/// Fixture-gated, opt-in verifier for an offline direct Spot cancel operation.
#[derive(Clone)]
pub struct DeepXSpotCancelCallVerifier {
    snapshot: RuntimeSnapshot,
    key: DeepXPrivateKey,
    signer: [u8; 20],
}

impl DeepXSpotCancelCallVerifier {
    /// Binds Spot cancel verification to an approved snapshot and signing key.
    ///
    /// # Errors
    ///
    /// Returns an error if the pinned signer rejects the key.
    pub fn new(snapshot: RuntimeSnapshot, key: DeepXPrivateKey) -> Result<Self, SigningError> {
        let signer = derive_signer_account_id(&key)?;
        Ok(Self {
            snapshot,
            key,
            signer,
        })
    }
}

impl DeepXBusinessCallVerifier for DeepXSpotCancelCallVerifier {
    fn verify(
        &self,
        identity: &DeepXTransactionIdentity,
        signed_extrinsic: &DeepXDurableSignedExtrinsic,
    ) -> Result<(), DeepXBusinessCallBindingError> {
        let actual_hash: [u8; 32] = subxt_core::config::Hasher::hash(
            &subxt_core::config::substrate::BlakeTwo256,
            signed_extrinsic.bytes(),
        )
        .into();
        if signed_extrinsic.bytes().is_empty() || actual_hash != signed_extrinsic.extrinsic_hash() {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "durable Spot cancel payload has invalid byte or hash integrity".to_string(),
            ));
        }
        if self.signer != identity.signer()
            || DeepXDirectRuntimeIdentity::from(self.snapshot.identity()) != *identity.runtime()
        {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "Spot cancel verifier signer or runtime differs from reserved identity".to_string(),
            ));
        }
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
            return Err(DeepXBusinessCallBindingError::Unsupported(
                "sequential account nonce domain remains unproven".to_string(),
            ));
        };
        let Some(super::DeepXTransactionOperation::SpotCancel {
            subaccount,
            pair,
            order_id,
            is_buy,
            fast_cancel,
        }) = identity.operation()
        else {
            return Err(DeepXBusinessCallBindingError::Unsupported(
                "durable identity is not a Spot cancel operation".to_string(),
            ));
        };
        let expected_side = if *is_buy {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        if identity.order_side() != expected_side {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "Spot cancel side differs from reserved order side".to_string(),
            ));
        }
        let service = crate::signing::DeepXRuntimeSnapshotService::new(self.snapshot.clone());
        let permit = service.acquire().map_err(|e| {
            DeepXBusinessCallBindingError::Unsupported(format!("runtime permit unavailable: {e}"))
        })?;
        let canonical = crate::signing::sign_spot_cancel(
            &permit,
            &self.key,
            crate::signing::DeepXSpotCancelParams {
                subaccount: *subaccount,
                pair: *pair,
                order_id: *order_id,
                is_buy: *is_buy,
                fast_cancel: *fast_cancel,
            },
            nonce,
        )
        .map_err(|e| {
            DeepXBusinessCallBindingError::Unsupported(format!(
                "canonical Spot cancel could not be encoded: {e}"
            ))
        })?;
        if canonical.bytes() != signed_extrinsic.bytes()
            || canonical.extrinsic_hash() != signed_extrinsic.extrinsic_hash()
        {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "durable bytes are not the canonical Spot cancel for this identity".to_string(),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for DeepXSpotCancelCallVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeepXSpotCancelCallVerifier")
            .field("snapshot", &self.snapshot.identity())
            .field("key", &"<redacted>")
            .finish()
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
    pub(super) bytes: Vec<u8>,
    pub(super) extrinsic_hash: [u8; 32],
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

/// Failure while durably committing verified initial-submission acceptance.
#[derive(Debug, Error)]
pub enum DeepXSubmissionAcceptanceCommitError {
    /// Only a submitting transaction can consume initial-submission acceptance evidence.
    #[error("DeepX transaction state {0:?} cannot commit initial-submission acceptance")]
    InvalidState(DeepXTransactionState),
    /// The durable record has no signed extrinsic hash to bind to the submission evidence.
    #[error("DeepX initial-submission acceptance requires a durable signed extrinsic")]
    MissingSignedExtrinsic,
    /// Verified submission evidence identifies a different signed extrinsic.
    #[error("DeepX initial-submission acceptance does not match the durable extrinsic hash")]
    ExtrinsicHashMismatch,
    /// Persistence or signer ownership could not be proven.
    #[error(transparent)]
    Persistence(#[from] DeepXTransactionPersistenceError),
    /// The acceptance transition conflicted with the durable transaction record.
    #[error(transparent)]
    Record(#[from] DeepXTransactionRecordError),
}

/// Commits hash-verified initial-submission acceptance to the exact submitting record.
///
/// This function performs no submission, retry, classification, replay, or order-event emission.
/// The verified node response and durable record must identify the same signed extrinsic before a
/// revision-checked compare-and-set advances the lifecycle to `accepted`.
///
/// # Errors
///
/// Returns an error without an accepted record if the lease, prior acknowledgement, submitting
/// state, signed hash binding, lifecycle transition, or compare-and-set outcome cannot be proven.
pub async fn commit_initial_submission_acceptance<S>(
    store: &S,
    lease: &S::Lease,
    committed_submitting: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    submitted: DeepXSubmittedExtrinsic,
) -> Result<DeepXCommittedObservation, DeepXSubmissionAcceptanceCommitError>
where
    S: DeepXTransactionStore,
{
    verify_signer_lease(lease, record)?;
    store.verify_signer_lease(lease).await?;
    committed_submitting.verify(record)?;
    if record.lifecycle().state() != DeepXTransactionState::Submitting {
        return Err(DeepXSubmissionAcceptanceCommitError::InvalidState(
            record.lifecycle().state(),
        ));
    }
    let durable_hash = record
        .signed_extrinsic()
        .map(DeepXDurableSignedExtrinsic::extrinsic_hash)
        .ok_or(DeepXSubmissionAcceptanceCommitError::MissingSignedExtrinsic)?;
    if submitted.extrinsic_hash() != durable_hash || submitted.node_hash() != durable_hash {
        return Err(DeepXSubmissionAcceptanceCommitError::ExtrinsicHashMismatch);
    }

    let mut accepted = record.clone();
    accepted.apply_observation(DeepXTransactionObservation::PoolAccepted)?;
    let committed = store
        .compare_and_set_committed(lease, committed_submitting, &accepted)
        .await?;
    committed.verify(&accepted)?;

    Ok(DeepXCommittedObservation {
        record: accepted,
        committed,
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

/// Failure while observing and durably committing canonical reorganization evidence.
#[derive(Debug, Error)]
pub enum DeepXReorganizationCommitError {
    /// The durable record has no signed extrinsic hash to bind to the canonical lookup.
    #[error("DeepX transaction reorganization observation requires durable signed bytes")]
    MissingSignedExtrinsic,
    /// The durable lifecycle has no current non-finalized inclusion eligible for reorganization.
    #[error("DeepX transaction state {0:?} is not eligible for reorganization observation")]
    IneligibleState(DeepXTransactionState),
    /// Canonical chain observation failed before any lifecycle mutation was attempted.
    #[error(transparent)]
    Watch(#[from] DeepXTransactionWatchError),
    /// The classified observation could not be committed durably.
    #[error(transparent)]
    Commit(#[from] DeepXObservationCommitError),
}

/// Failure while observing and durably committing finality for an in-block record.
#[derive(Debug, Error)]
pub enum DeepXFinalityCommitError {
    /// The durable record has no signed extrinsic hash to bind to the canonical lookup.
    #[error("DeepX transaction finality observation requires durable signed bytes")]
    MissingSignedExtrinsic,
    /// The durable lifecycle has no current in-block inclusion eligible for finality observation.
    #[error("DeepX transaction state {0:?} is not eligible for finality observation")]
    IneligibleState(DeepXTransactionState),
    /// Finalized-chain observation failed before any lifecycle mutation was attempted.
    #[error(transparent)]
    Watch(#[from] DeepXTransactionWatchError),
    /// The finalized observation could not be committed durably.
    #[error(transparent)]
    Commit(#[from] DeepXObservationCommitError),
}

/// Failure while reconciling a submitting transaction against the pending pool.
#[derive(Debug, Error)]
pub enum DeepXPoolReconciliationCommitError {
    /// The durable record has no signed extrinsic hash to bind to the pool lookup.
    #[error("DeepX pending-pool reconciliation requires durable signed bytes")]
    MissingSignedExtrinsic,
    /// Only submitting or accepted transactions are eligible for pending-pool reconciliation.
    #[error("DeepX transaction state {0:?} is not eligible for pending-pool reconciliation")]
    IneligibleState(DeepXTransactionState),
    /// Pending-pool observation failed before any lifecycle mutation was attempted.
    #[error(transparent)]
    Watch(#[from] DeepXTransactionWatchError),
    /// The pool observation could not be committed durably.
    #[error(transparent)]
    Commit(#[from] DeepXObservationCommitError),
}

/// Failure while reconciling a durable not-included checkpoint against finalized evidence.
#[derive(Debug, Error)]
pub enum DeepXFinalizedRecoveryCommitError {
    /// The durable record has no signed extrinsic hash to bind to the finalized scan.
    #[error("DeepX finalized recovery requires durable signed bytes")]
    MissingSignedExtrinsic,
    /// Only a prior complete not-included checkpoint can authorize this recovery scan.
    #[error("DeepX transaction state {0:?} is not eligible for finalized checkpoint recovery")]
    IneligibleState(DeepXTransactionState),
    /// The not-included lifecycle state did not retain its required checkpoint evidence.
    #[error("DeepX not-included transaction is missing durable absence evidence")]
    MissingAbsenceEvidence,
    /// The approved snapshot does not match the durable runtime identity being recovered.
    #[error("DeepX finalized recovery runtime snapshot does not match durable identity")]
    RuntimeSnapshotMismatch,
    /// The selected observer cannot verify the exact durable operation and bytes.
    #[error(transparent)]
    Binding(#[from] DeepXBusinessCallBindingError),
    /// Finalized-chain observation failed before any lifecycle mutation was attempted.
    #[error(transparent)]
    Watch(#[from] DeepXTransactionWatchError),
    /// The classified observation could not be committed durably.
    #[error(transparent)]
    Commit(#[from] DeepXObservationCommitError),
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

/// Observes and durably commits reorganization evidence for one acknowledged transaction record.
///
/// The target extrinsic hash, recorded inclusion, record, and acknowledgement are derived from the
/// same restored value. This function does not submit, replay, replace, or emit order events.
///
/// # Errors
///
/// Returns an error before network access when the durable record lacks signed bytes or a current
/// inclusion. RPC and commit failures remain distinct and do not grant replay authority.
pub async fn observe_and_commit_reorganization<S>(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    store: &S,
    lease: &S::Lease,
    restored: &DeepXRestoredTransactionRecord,
) -> Result<DeepXCommittedObservation, DeepXReorganizationCommitError>
where
    S: DeepXTransactionStore,
{
    let record = restored.record();
    if !matches!(
        record.lifecycle().state(),
        DeepXTransactionState::InBlockSuccess | DeepXTransactionState::InBlockFailed
    ) {
        return Err(DeepXReorganizationCommitError::IneligibleState(
            record.lifecycle().state(),
        ));
    }
    let target_extrinsic_hash = record
        .signed_extrinsic()
        .ok_or(DeepXReorganizationCommitError::MissingSignedExtrinsic)?
        .extrinsic_hash();
    let recorded_inclusion =
        record
            .lifecycle()
            .inclusion()
            .ok_or(DeepXReorganizationCommitError::IneligibleState(
                record.lifecycle().state(),
            ))?;
    let decision = observe_reorganization(
        endpoints,
        capabilities,
        target_extrinsic_hash,
        recorded_inclusion,
    )
    .await?;
    Ok(
        commit_reorganization_decision(store, lease, restored.committed(), record, decision)
            .await?,
    )
}

/// Observes and durably commits finality for one acknowledged in-block transaction record.
///
/// A finalized head behind the recorded inclusion preserves the exact record and acknowledgement.
/// This function does not submit, replay, replace, or emit order events.
///
/// # Errors
///
/// Returns an error before network access when the durable record lacks signed bytes or a current
/// in-block inclusion. RPC conflicts and commit failures do not mutate the lifecycle.
pub async fn observe_and_commit_finality<S>(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    store: &S,
    lease: &S::Lease,
    restored: &DeepXRestoredTransactionRecord,
) -> Result<DeepXCommittedObservation, DeepXFinalityCommitError>
where
    S: DeepXTransactionStore,
{
    let record = restored.record();
    if !matches!(
        record.lifecycle().state(),
        DeepXTransactionState::InBlockSuccess | DeepXTransactionState::InBlockFailed
    ) {
        return Err(DeepXFinalityCommitError::IneligibleState(
            record.lifecycle().state(),
        ));
    }
    let target_extrinsic_hash = record
        .signed_extrinsic()
        .ok_or(DeepXFinalityCommitError::MissingSignedExtrinsic)?
        .extrinsic_hash();
    let recorded_inclusion =
        record
            .lifecycle()
            .inclusion()
            .ok_or(DeepXFinalityCommitError::IneligibleState(
                record.lifecycle().state(),
            ))?;
    let observation = observe_finality(
        endpoints,
        capabilities,
        target_extrinsic_hash,
        recorded_inclusion,
    )
    .await?;
    let DeepXFinalityObservation::Finalized(inclusion) = observation else {
        return Ok(DeepXCommittedObservation {
            record: record.clone(),
            committed: restored.committed().clone(),
        });
    };
    Ok(commit_recovery_decision(
        store,
        lease,
        restored.committed(),
        record,
        DeepXRecoveryDecision::FinalizedInclusion(inclusion),
    )
    .await?)
}

/// Reconciles one acknowledged submitting transaction against the submission-node pool.
///
/// Exact presence durably records pool acceptance. Absence preserves the existing record because
/// a non-atomic pool snapshot cannot prove that the transaction was never included.
///
/// # Errors
///
/// Returns an error before network access unless the record is submitting or accepted and retains
/// signed bytes. RPC and commit failures do not mutate the lifecycle.
pub async fn reconcile_submission_pool<S>(
    endpoints: &DeepXValidatedRpcEndpoints,
    store: &S,
    lease: &S::Lease,
    restored: &DeepXRestoredTransactionRecord,
) -> Result<DeepXCommittedObservation, DeepXPoolReconciliationCommitError>
where
    S: DeepXTransactionStore,
{
    let record = restored.record();
    if !matches!(
        record.lifecycle().state(),
        DeepXTransactionState::Submitting | DeepXTransactionState::Accepted
    ) {
        return Err(DeepXPoolReconciliationCommitError::IneligibleState(
            record.lifecycle().state(),
        ));
    }
    let target_extrinsic_hash = record
        .signed_extrinsic()
        .ok_or(DeepXPoolReconciliationCommitError::MissingSignedExtrinsic)?
        .extrinsic_hash();
    match observe_submission_pool(endpoints, target_extrinsic_hash).await? {
        super::DeepXPoolObservation::Present => Ok(commit_recovery_decision(
            store,
            lease,
            restored.committed(),
            record,
            DeepXRecoveryDecision::PoolAccepted,
        )
        .await?),
        super::DeepXPoolObservation::Absent => Ok(DeepXCommittedObservation {
            record: record.clone(),
            committed: restored.committed().clone(),
        }),
    }
}

/// Reconciles one acknowledged not-included record from its exact finalized checkpoint.
///
/// All authority-bearing inputs are derived from `restored`. An up-to-date checkpoint preserves
/// the existing acknowledgement. A transaction which later appears in the non-atomic submission
/// pool is treated as conflicting evidence and durably requires operator action. This function
/// does not submit, replay, replace, or emit order events.
///
/// # Errors
///
/// Returns an error before network access unless the durable record is not-included and retains
/// signed bytes plus complete absence evidence. RPC and commit failures do not mutate lifecycle.
pub async fn reconcile_not_included_checkpoint<S>(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    store: &S,
    lease: &S::Lease,
    restored: &DeepXRestoredTransactionRecord,
    max_blocks_per_range: u64,
) -> Result<DeepXCommittedObservation, DeepXFinalizedRecoveryCommitError>
where
    S: DeepXTransactionStore,
{
    reconcile_not_included_checkpoint_with_observer(
        endpoints,
        capabilities,
        snapshot,
        store,
        lease,
        restored,
        max_blocks_per_range,
        DeepXDurableRecoveryObserver::Default,
    )
    .await
}

/// Explicit offline observer selection; the default does not support Spot inclusion.
#[derive(Debug, Default)]
pub enum DeepXDurableRecoveryObserver<'a> {
    /// Preserves the existing recovery boundary and unsupported Spot inclusion behavior.
    #[default]
    Default,
    /// Verifies an ordinary Spot cancel using exact durable bytes and an approved snapshot.
    OrdinarySpotCancel(&'a DeepXSpotCancelCallVerifier),
    /// Verifies an ordinary or fast Perp cancel using exact durable bytes.
    PerpCancel(&'a DeepXPerpCancelCallVerifier),
}

/// Reconciles a durable checkpoint with an explicitly selected offline observer.
///
/// This boundary does not submit, replay, emit order events, or enable live execution.
///
/// # Errors
///
/// Returns an error before RPC for an ineligible record, foreign snapshot, fast Spot cancel,
/// or an operation whose exact durable bytes do not match the selected verifier.
pub async fn reconcile_not_included_checkpoint_with_observer<S>(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    store: &S,
    lease: &S::Lease,
    restored: &DeepXRestoredTransactionRecord,
    max_blocks_per_range: u64,
    observer: DeepXDurableRecoveryObserver<'_>,
) -> Result<DeepXCommittedObservation, DeepXFinalizedRecoveryCommitError>
where
    S: DeepXTransactionStore,
{
    let record = restored.record();
    if record.lifecycle().state() != DeepXTransactionState::NotIncluded {
        return Err(DeepXFinalizedRecoveryCommitError::IneligibleState(
            record.lifecycle().state(),
        ));
    }
    if record.identity().runtime() != &DeepXDirectRuntimeIdentity::from(snapshot.identity()) {
        return Err(DeepXFinalizedRecoveryCommitError::RuntimeSnapshotMismatch);
    }
    let signed_extrinsic = record
        .signed_extrinsic()
        .ok_or(DeepXFinalizedRecoveryCommitError::MissingSignedExtrinsic)?;
    let target_extrinsic_hash = signed_extrinsic.extrinsic_hash();
    let absence = record
        .lifecycle()
        .absence()
        .ok_or(DeepXFinalizedRecoveryCommitError::MissingAbsenceEvidence)?;
    if let DeepXDurableRecoveryObserver::PerpCancel(verifier) = &observer {
        verifier.verify(record.identity(), signed_extrinsic)?;
    }
    if let DeepXDurableRecoveryObserver::OrdinarySpotCancel(verifier) = &observer {
        if matches!(
            record.identity().operation(),
            Some(super::DeepXTransactionOperation::SpotCancel {
                fast_cancel: true,
                ..
            })
        ) {
            return Err(DeepXTransactionWatchError::from(
                super::DeepXSpotCancelEventVerificationError::FastCancelUnsupported,
            )
            .into());
        }
        verifier.verify(record.identity(), signed_extrinsic)?;
    }
    let collection = match observer {
        DeepXDurableRecoveryObserver::Default | DeepXDurableRecoveryObserver::PerpCancel(_) => {
            collect_finalized_recovery_scan_with_event_evidence(
                endpoints,
                capabilities,
                snapshot,
                record.identity(),
                absence.finalized_block_number(),
                absence.finalized_block_hash(),
                max_blocks_per_range,
                target_extrinsic_hash,
            )
            .await?
        }
        DeepXDurableRecoveryObserver::OrdinarySpotCancel(_) => {
            super::collect_finalized_spot_cancel_recovery_scan(
                endpoints,
                capabilities,
                snapshot,
                record.identity(),
                absence.finalized_block_number(),
                absence.finalized_block_hash(),
                max_blocks_per_range,
                target_extrinsic_hash,
            )
            .await?
        }
    };
    let decision = match collection {
        DeepXFinalizedRecoveryCollection::UpToDate(_) => {
            DeepXRecoveryDecision::NotIncluded(absence)
        }
        DeepXFinalizedRecoveryCollection::Scan(scan) => match scan.classify() {
            DeepXRecoveryDecision::PoolAccepted => DeepXRecoveryDecision::ActionRequired,
            decision => decision,
        },
    };
    Ok(commit_recovery_decision(store, lease, restored.committed(), record, decision).await?)
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        future::ready,
        num::NonZeroU32,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{Json, Router, extract::State, routing::post};
    use nautilus_model::{
        enums::OrderSide,
        identifiers::{ClientOrderId, InstrumentId},
    };
    use rstest::rstest;
    use serde_json::{Value as JsonValue, json};
    use subxt_core::config::{Hasher, substrate::BlakeTwo256};
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        common::{DeepXEnvironment, consts::DEEPX_TESTNET_GENESIS_HASH},
        config::{
            DeepXNetworkConfig, DeepXObservedRpcEndpoint, DeepXRpcRole,
            validate_rpc_endpoint_identities,
        },
        rpc::observe_and_validate_rpc_method_capabilities,
        signing::{ApprovedRuntimeIdentity, SignedPalletExtrinsic},
        transaction::{
            DeepXAbsenceEvidence, DeepXAutomaticReplayDecision, DeepXDirectRuntimeIdentity,
            DeepXInclusionEvidence, DeepXInclusionOutcome, DeepXNonceReservation,
            DeepXSubmissionPermit, DeepXTransactionIdentity, DeepXTransactionObservation,
            submit_with_bounded_ambiguity_retry,
        },
    };

    async fn reorganization_rpc(
        State(request_count): State<Arc<AtomicUsize>>,
        Json(request): Json<JsonValue>,
    ) -> Json<JsonValue> {
        request_count.fetch_add(1, Ordering::Relaxed);
        let result = match request["method"].as_str().unwrap() {
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
            "chain_getBlockHash" => json!(format!("0x{}", "09".repeat(32))),
            "chain_getFinalizedHead" => json!(format!("0x{}", "09".repeat(32))),
            "chain_getHeader" => json!({ "number": "0x48" }),
            "author_pendingExtrinsics" => json!([]),
            "chain_getBlock" => json!({
                "block": {
                    "header": { "number": "0x48" },
                    "extrinsics": [],
                },
            }),
            method => panic!("unexpected method {method}"),
        };
        Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
    }

    async fn reorganization_endpoints() -> (
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let request_count = Arc::new(AtomicUsize::new(0));
        let server_request_count = Arc::clone(&request_count);
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(reorganization_rpc))
                    .with_state(server_request_count),
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
            nautilus_core::hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x"))
                .unwrap();
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
        (endpoints, capabilities, request_count)
    }

    #[derive(Clone, Debug)]
    struct FinalizedRecoveryRpcState {
        finalized_block: u64,
    }

    async fn finalized_recovery_rpc(
        State(state): State<FinalizedRecoveryRpcState>,
        Json(request): Json<JsonValue>,
    ) -> Json<JsonValue> {
        let result = match request["method"].as_str().unwrap() {
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
            "chain_getFinalizedHead" => {
                json!(format!("0x{:064x}", state.finalized_block))
            }
            "chain_getHeader" => json!({
                "number": format!("0x{:x}", state.finalized_block),
            }),
            "chain_getBlockHash" => {
                let block_number = request["params"][0].as_u64().unwrap();
                if block_number == 72 {
                    json!(format!("0x{}", "09".repeat(32)))
                } else {
                    json!(format!("0x{block_number:064x}"))
                }
            }
            "chain_getBlock" => {
                let encoded_hash = request["params"][0].as_str().unwrap();
                let block_number =
                    u64::from_str_radix(encoded_hash.trim_start_matches("0x"), 16).unwrap();
                json!({
                    "block": {
                        "header": { "number": format!("0x{block_number:x}") },
                        "extrinsics": [],
                    },
                })
            }
            "author_pendingExtrinsics" => json!([]),
            method => panic!("unexpected method {method}"),
        };
        Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
    }

    async fn finalized_recovery_endpoints(
        finalized_block: u64,
    ) -> (
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(finalized_recovery_rpc))
                    .with_state(FinalizedRecoveryRpcState { finalized_block }),
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
            nautilus_core::hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x"))
                .unwrap();
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
        (endpoints, capabilities)
    }

    #[derive(Clone, Debug)]
    struct FinalityRpcState {
        finalized_block: u64,
        target_extrinsic: String,
        canonical_requests: Arc<AtomicUsize>,
    }

    async fn finality_rpc(
        State(state): State<FinalityRpcState>,
        Json(request): Json<JsonValue>,
    ) -> Json<JsonValue> {
        let result = match request["method"].as_str().unwrap() {
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
            "chain_getFinalizedHead" => json!(format!("0x{}", "09".repeat(32))),
            "chain_getHeader" => json!({ "number": format!("0x{:x}", state.finalized_block) }),
            "chain_getBlockHash" => {
                state.canonical_requests.fetch_add(1, Ordering::Relaxed);
                json!(format!("0x{}", "08".repeat(32)))
            }
            "chain_getBlock" => {
                state.canonical_requests.fetch_add(1, Ordering::Relaxed);
                json!({
                    "block": {
                        "header": { "number": "0x48" },
                        "extrinsics": ["0x0400", state.target_extrinsic],
                    },
                })
            }
            method => panic!("unexpected method {method}"),
        };
        Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
    }

    async fn finality_endpoints(
        finalized_block: u64,
        target_extrinsic: &[u8],
    ) -> (
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let canonical_requests = Arc::new(AtomicUsize::new(0));
        let state = FinalityRpcState {
            finalized_block,
            target_extrinsic: format!("0x{}", nautilus_core::hex::encode(target_extrinsic)),
            canonical_requests: Arc::clone(&canonical_requests),
        };
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(finality_rpc))
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
            nautilus_core::hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x"))
                .unwrap();
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
        (endpoints, capabilities, canonical_requests)
    }

    #[derive(Clone, Debug)]
    struct PendingPoolRpcState {
        target_extrinsic: String,
        requests: Arc<AtomicUsize>,
    }

    async fn pending_pool_rpc(
        State(state): State<PendingPoolRpcState>,
        Json(request): Json<JsonValue>,
    ) -> Json<JsonValue> {
        state.requests.fetch_add(1, Ordering::Relaxed);
        let result = match request["method"].as_str().unwrap() {
            "author_pendingExtrinsics" => json!([state.target_extrinsic]),
            method => panic!("unexpected method {method}"),
        };
        Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
    }

    async fn pending_pool_endpoints(
        target_extrinsic: &[u8],
    ) -> (DeepXValidatedRpcEndpoints, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let state = PendingPoolRpcState {
            target_extrinsic: format!("0x{}", nautilus_core::hex::encode(target_extrinsic),),
            requests: Arc::clone(&requests),
        };
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(pending_pool_rpc))
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
            nautilus_core::hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x"))
                .unwrap();
        let endpoints = validate_rpc_endpoint_identities(
            &config,
            [
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Submission, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Recovery, url, genesis_hash),
            ],
        )
        .unwrap();
        (endpoints, requests)
    }

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
        signed_commit_fault: u8,
    }

    impl TestStore {
        fn new(revision: u64, record: &DeepXTransactionRecord) -> Self {
            Self {
                revision: Mutex::new(revision),
                encoded_record: Mutex::new(record.encode().unwrap()),
                active_generation: 4,
                create_outcome_unknown: false,
                commit_outcome_unknown: false,
                signed_commit_fault: 0,
            }
        }

        fn empty() -> Self {
            Self {
                revision: Mutex::new(0),
                encoded_record: Mutex::new(Vec::new()),
                active_generation: 4,
                create_outcome_unknown: false,
                commit_outcome_unknown: false,
                signed_commit_fault: 0,
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
            if self.signed_commit_fault == 1 {
                return Err(DeepXTransactionPersistenceError::BeforeCommit(
                    "write rejected".to_string(),
                ));
            }
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
            if self.signed_commit_fault == 2 {
                let (identity, signed) = spot_cancel_fixture(false, true);
                let mut conflicting = DeepXTransactionRecord::created(identity);
                conflicting.record_signed(&signed).unwrap();
                return DeepXCommittedTransactionRecord::acknowledge_committed(
                    &conflicting,
                    DeepXTransactionRevision::new(*revision),
                );
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
                environment: DeepXEnvironment::Testnet,
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

    fn valid_extrinsic_submitting_record() -> DeepXTransactionRecord {
        let mut record = record();
        let bytes = vec![8, 1, 2];
        let identity = record.identity();
        let runtime = identity.runtime();
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
            unreachable!();
        };
        record
            .record_signed(&SignedPalletExtrinsic {
                extrinsic_hash: BlakeTwo256.hash(&bytes).0,
                bytes,
                signer: identity.signer(),
                nonce,
                runtime: ApprovedRuntimeIdentity {
                    environment: DeepXEnvironment::Testnet,
                    genesis_hash: runtime.genesis_hash,
                    metadata_sha256: runtime.metadata_sha256,
                    spec_version: runtime.spec_version,
                    transaction_version: runtime.transaction_version,
                    signed_extensions: runtime.signed_extensions.clone(),
                },
            })
            .unwrap();
        record
            .apply_observation(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        record
    }

    fn not_included_record_for(snapshot: &RuntimeSnapshot) -> DeepXTransactionRecord {
        let identity = DeepXTransactionIdentity::new(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            [7; 20],
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId { value: 42 },
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
        );
        let mut record = DeepXTransactionRecord::created(identity);
        let bytes = vec![1, 2, 3];
        record
            .record_signed(&SignedPalletExtrinsic {
                extrinsic_hash: BlakeTwo256.hash(&bytes).0,
                bytes,
                signer: record.identity().signer(),
                nonce: 42,
                runtime: snapshot.identity().clone(),
            })
            .unwrap();
        record
            .apply_observation(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        record
            .apply_observation(DeepXTransactionObservation::NotIncluded(
                DeepXAbsenceEvidence::new(70, 72, [9; 32], true, true).unwrap(),
            ))
            .unwrap();
        record
    }

    fn in_block_record() -> DeepXTransactionRecord {
        let mut record = record();
        let bytes = vec![12, 1, 2, 3];
        let identity = record.identity();
        let runtime = identity.runtime();
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
            unreachable!();
        };
        record
            .record_signed(&SignedPalletExtrinsic {
                extrinsic_hash: BlakeTwo256.hash(&bytes).0,
                bytes,
                signer: identity.signer(),
                nonce,
                runtime: ApprovedRuntimeIdentity {
                    environment: DeepXEnvironment::Testnet,
                    genesis_hash: runtime.genesis_hash,
                    metadata_sha256: runtime.metadata_sha256,
                    spec_version: runtime.spec_version,
                    transaction_version: runtime.transaction_version,
                    signed_extensions: runtime.signed_extensions.clone(),
                },
            })
            .unwrap();
        record
            .apply_observation(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        record
            .apply_observation(DeepXTransactionObservation::Included(
                DeepXInclusionEvidence {
                    block_hash: [8; 32],
                    block_number: 72,
                    extrinsic_index: 1,
                    outcome: DeepXInclusionOutcome::Success,
                },
            ))
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
                environment: DeepXEnvironment::Testnet,
                genesis_hash: runtime.genesis_hash,
                metadata_sha256: runtime.metadata_sha256,
                spec_version: runtime.spec_version,
                transaction_version: runtime.transaction_version,
                signed_extensions: runtime.signed_extensions.clone(),
            },
        }
    }

    async fn submitted_for_bytes(bytes: Vec<u8>) -> DeepXSubmittedExtrinsic {
        let extrinsic_hash = BlakeTwo256.hash(&bytes).0;
        submit_with_bounded_ambiguity_retry(
            DeepXSubmissionPermit {
                bytes,
                extrinsic_hash,
            },
            NonZeroU32::new(1).unwrap(),
            move |_, _| ready(Ok(extrinsic_hash)),
        )
        .await
        .unwrap()
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
    async fn verified_preparation_rejection_never_commits_signed_record() {
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
        let result = prepare_signed_transaction_with_verifier(
            &store,
            &lease,
            &committed,
            &record,
            |_| Ok(signed_for(&record)),
            &DeepXUnsupportedBusinessCallVerifier,
        )
        .await;
        assert!(matches!(
            result,
            Err(DeepXSignedTransactionPreparationError::Binding(
                DeepXBusinessCallBindingError::Unsupported(_)
            ))
        ));
        assert_eq!(store.current_revision(), 3);
    }

    #[tokio::test]
    async fn verified_preparation_commits_exact_perp_close() {
        let expected = perp_close_fixture(u128::MAX, None);
        let record = DeepXTransactionRecord::created(expected.identity().clone());
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
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let permit = service.acquire().unwrap();
        let key = remark_key();
        let verifier = DeepXPerpCloseCallVerifier::new(remark_snapshot(), key.clone()).unwrap();
        let prepared = prepare_signed_transaction_with_verifier(
            &store,
            &lease,
            &committed,
            &record,
            |_| {
                crate::signing::sign_perp_close(
                    &permit,
                    &key,
                    crate::signing::DeepXPerpCloseParams {
                        subaccount: [0x11; 20],
                        market_id: u16::MAX,
                        price: u128::MAX,
                        slippage: None,
                    },
                    u64::MAX,
                )
            },
            &verifier,
        )
        .await
        .unwrap();
        assert_eq!(prepared.record(), &expected);
        assert!(prepared.committed().matches(&expected));
        assert_eq!(store.current_revision(), 4);
    }

    #[rstest]
    #[case::subaccount(0)]
    #[case::market(1)]
    #[case::price(2)]
    #[case::slippage(3)]
    #[tokio::test]
    async fn verified_preparation_rejects_wrong_close_call(#[case] mutation: u8) {
        let expected = perp_close_fixture(u128::MAX, None);
        let record = DeepXTransactionRecord::created(expected.identity().clone());
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
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let permit = service.acquire().unwrap();
        let key = remark_key();
        let verifier = DeepXPerpCloseCallVerifier::new(remark_snapshot(), key.clone()).unwrap();
        let mut params = crate::signing::DeepXPerpCloseParams {
            subaccount: [0x11; 20],
            market_id: u16::MAX,
            price: u128::MAX,
            slippage: None,
        };
        match mutation {
            0 => params.subaccount = [0x12; 20],
            1 => params.market_id -= 1,
            2 => params.price -= 1,
            3 => params.slippage = Some(0),
            _ => unreachable!(),
        }
        let result = prepare_signed_transaction_with_verifier(
            &store,
            &lease,
            &committed,
            &record,
            |_| crate::signing::sign_perp_close(&permit, &key, params, u64::MAX),
            &verifier,
        )
        .await;
        assert!(matches!(
            result,
            Err(DeepXSignedTransactionPreparationError::Binding(
                DeepXBusinessCallBindingError::Mismatch(_)
            ))
        ));
        assert_eq!(store.current_revision(), 3);
        assert!(committed.matches(&record));
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
            signed_commit_fault: 0,
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

    fn remark_snapshot() -> crate::signing::RuntimeSnapshot {
        #[derive(serde::Deserialize)]
        struct RpcResponse {
            result: String,
        }
        let metadata: RpcResponse = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json",
        )))
        .unwrap();
        let bytes = nautilus_core::hex::decode(metadata.result.trim_start_matches("0x")).unwrap();
        crate::signing::RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            nautilus_core::hex::decode_array(
                "86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b",
            )
            .unwrap(),
            366,
            1,
            &bytes,
        )
        .unwrap()
    }

    fn remark_key() -> crate::common::DeepXPrivateKey {
        crate::common::DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &crate::common::DeepXKeyScheme::Secp256k1,
        )
        .unwrap()
    }

    fn remark_runtime() -> DeepXDirectRuntimeIdentity {
        DeepXDirectRuntimeIdentity::from(remark_snapshot().identity())
    }

    fn remark_identity() -> DeepXTransactionIdentity {
        DeepXTransactionIdentity::new(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            crate::signing::derive_signer_account_id(&remark_key()).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId {
                value: 1_725_000_000_123,
            },
            remark_runtime(),
        )
    }

    fn remark_record(identity: &DeepXTransactionIdentity) -> DeepXTransactionRecord {
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
            unreachable!("remark records reserve a timestamp nonce");
        };
        let mut record = DeepXTransactionRecord::created(identity.clone());
        let signed = sign_dynamic_pallet_call_with_snapshot(
            &remark_snapshot(),
            &remark_key(),
            "System",
            "remark",
            vec![Value::from_bytes(
                DeepXRemarkCallVerifier::canonical_remark_payload(identity, nonce),
            )],
            nonce,
        )
        .unwrap();
        record.record_signed(&signed).unwrap();
        record
    }

    #[rstest]
    fn remark_verifier_accepts_exactly_the_canonical_identity_binding() {
        let identity = remark_identity();
        let record = remark_record(&identity);
        let verifier = DeepXRemarkCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        assert_eq!(
            verifier.verify(&identity, record.signed_extrinsic().unwrap()),
            Ok(()),
        );
    }

    #[rstest]
    fn remark_verifier_rejects_bytes_signed_for_another_payload() {
        let identity = remark_identity();
        let mut record = DeepXTransactionRecord::created(identity.clone());
        let signed = sign_dynamic_pallet_call_with_snapshot(
            &remark_snapshot(),
            &remark_key(),
            "System",
            "remark",
            vec![Value::from_bytes(b"deepx-unrelated-payload")],
            1_725_000_000_123,
        )
        .unwrap();
        record.record_signed(&signed).unwrap();
        let verifier = DeepXRemarkCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        assert!(matches!(
            verifier.verify(&identity, record.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Mismatch(_)),
        ));
    }

    #[rstest]
    fn remark_verifier_rejects_a_runtime_the_snapshot_does_not_cover() {
        let record = remark_record(&remark_identity());
        let unproven_runtime_identity = DeepXTransactionIdentity::new(
            ClientOrderId::new(remark_identity().client_order_id()),
            remark_identity().signer(),
            remark_identity().instrument_id(),
            remark_identity().order_side(),
            remark_identity().nonce(),
            DeepXDirectRuntimeIdentity {
                spec_version: 999,
                ..remark_runtime()
            },
        );
        let verifier = DeepXRemarkCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        assert!(matches!(
            verifier.verify(
                &unproven_runtime_identity,
                record.signed_extrinsic().unwrap(),
            ),
            Err(DeepXBusinessCallBindingError::Mismatch(_)),
        ));
    }

    #[rstest]
    fn remark_verifier_rejects_bytes_signed_with_a_different_nonce() {
        let identity = remark_identity();
        let other_nonce_identity = DeepXTransactionIdentity::new(
            ClientOrderId::new(identity.client_order_id()),
            identity.signer(),
            identity.instrument_id(),
            identity.order_side(),
            DeepXNonceReservation::TimestampOrderId {
                value: 1_725_000_000_124,
            },
            remark_runtime(),
        );
        let record = remark_record(&other_nonce_identity);
        let verifier = DeepXRemarkCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        assert!(matches!(
            verifier.verify(&identity, record.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Mismatch(_)),
        ));
    }

    #[rstest]
    fn remark_verifier_rejects_the_unproven_sequential_nonce_domain() {
        let identity = remark_identity();
        let sequential_identity = DeepXTransactionIdentity::new(
            ClientOrderId::new(identity.client_order_id()),
            identity.signer(),
            identity.instrument_id(),
            identity.order_side(),
            DeepXNonceReservation::SequentialAccount {
                account_index: 0,
                nonce: 7,
            },
            remark_runtime(),
        );
        let record = remark_record(&identity);
        let verifier = DeepXRemarkCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        assert!(matches!(
            verifier.verify(&sequential_identity, record.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Unsupported(_)),
        ));
    }

    #[rstest]
    fn remark_verifier_debug_output_redacts_the_private_key() {
        let verifier = DeepXRemarkCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        let debug = format!("{verifier:?}");
        assert!(debug.contains("DeepXRemarkCallVerifier"));
        assert!(!debug.contains("0123456789abcdef"));
    }

    fn perp_close_fixture(price: u128, slippage: Option<u64>) -> DeepXTransactionRecord {
        let params = crate::signing::DeepXPerpCloseParams {
            subaccount: [0x11; 20],
            market_id: u16::MAX,
            price,
            slippage,
        };
        let identity = DeepXTransactionIdentity::new_perp_close(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            derive_signer_account_id(&remark_key()).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId { value: u64::MAX },
            remark_runtime(),
            params,
        );
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let signed = crate::signing::sign_perp_close(
            &service.acquire().unwrap(),
            &remark_key(),
            params,
            u64::MAX,
        )
        .unwrap();
        let mut record = DeepXTransactionRecord::created(identity);
        record.record_signed(&signed).unwrap();
        record
    }

    #[tokio::test]
    async fn perp_close_preparation_commits_exact_checkpoint() {
        let expected = perp_close_fixture(u128::MAX, Some(u64::MAX));
        let record = DeepXTransactionRecord::created(expected.identity().clone());
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
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let prepared = prepare_signed_perp_close_transaction(
            &store,
            &lease,
            &committed,
            &record,
            &service.acquire().unwrap(),
            &remark_key(),
        )
        .await
        .unwrap();
        assert_eq!(prepared.record(), &expected);
        assert!(prepared.committed().matches(&expected));
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn perp_close_preparation_rejects_non_close_before_commit() {
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
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        assert!(matches!(
            prepare_signed_perp_close_transaction(
                &store,
                &lease,
                &committed,
                &record,
                &service.acquire().unwrap(),
                &remark_key(),
            )
            .await,
            Err(DeepXSignedTransactionPreparationError::Binding(
                DeepXBusinessCallBindingError::Unsupported(_)
            ))
        ));
        assert_eq!(store.current_revision(), 3);
    }

    #[rstest]
    #[case::none(0, None)]
    #[case::zero(u128::MAX, Some(0))]
    #[case::maximum(u128::MAX, Some(u64::MAX))]
    #[case::above_u64(u128::from(u64::MAX) + 1, Some(1))]
    #[tokio::test]
    async fn perp_close_preparation_boundaries(#[case] price: u128, #[case] slippage: Option<u64>) {
        let expected = perp_close_fixture(price, slippage);
        let record = DeepXTransactionRecord::created(expected.identity().clone());
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
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let prepared = prepare_signed_perp_close_transaction(
            &store,
            &lease,
            &committed,
            &record,
            &service.acquire().unwrap(),
            &remark_key(),
        )
        .await
        .unwrap();
        let restored =
            DeepXTransactionRecord::decode(&prepared.record().encode().unwrap()).unwrap();
        assert_eq!(restored, expected);
        assert!(prepared.committed().matches(&restored));
    }

    #[rstest]
    #[case::stale_revision(0)]
    #[case::wrong_key(1)]
    #[case::runtime(2)]
    #[case::lease(3)]
    #[case::already_signed(4)]
    #[case::unknown_commit(5)]
    #[tokio::test]
    async fn perp_close_preparation_failures(#[case] mutation: u8) {
        let expected = perp_close_fixture(u128::MAX, None);
        let mut record = DeepXTransactionRecord::created(expected.identity().clone());
        if mutation == 2 {
            let mut runtime = remark_runtime();
            runtime.spec_version += 1;
            record = DeepXTransactionRecord::created(DeepXTransactionIdentity::new_perp_close(
                ClientOrderId::new(record.identity().client_order_id()),
                record.identity().signer(),
                record.identity().instrument_id(),
                record.identity().order_side(),
                record.identity().nonce(),
                runtime,
                crate::signing::DeepXPerpCloseParams {
                    subaccount: [0x11; 20],
                    market_id: u16::MAX,
                    price: u128::MAX,
                    slippage: None,
                },
            ));
        }
        if mutation == 4 {
            record = expected;
        }
        let store = TestStore {
            commit_outcome_unknown: mutation == 5,
            ..TestStore::new(3, &record)
        };
        let mut lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        if mutation == 3 {
            lease.signer = [0xff; 20];
        }
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(if mutation == 0 { 2 } else { 3 }),
        )
        .unwrap();
        let key = if mutation == 1 {
            DeepXPrivateKey::new(&"22".repeat(32), &crate::common::DeepXKeyScheme::Secp256k1)
                .unwrap()
        } else {
            remark_key()
        };
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let result = prepare_signed_perp_close_transaction(
            &store,
            &lease,
            &committed,
            &record,
            &service.acquire().unwrap(),
            &key,
        )
        .await;
        match mutation {
            0 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::RevisionConflict
                ))
            )),
            1 | 2 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Record(_))
                    | Err(DeepXSignedTransactionPreparationError::Binding(_))
            )),
            3 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::LeaseMismatch
                ))
            )),
            4 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::InvalidState)
            )),
            5 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::CommitOutcomeUnknown(_)
                ))
            )),
            _ => unreachable!(),
        }
        assert_eq!(store.current_revision(), if mutation == 5 { 4 } else { 3 });
    }

    #[rstest]
    #[case::none(0, None)]
    #[case::zero(u128::MAX, Some(0))]
    #[case::maximum(u128::MAX, Some(u64::MAX))]
    #[case::above_u64(u128::from(u64::MAX) + 1, Some(1))]
    fn perp_close_verifier_round_trip(#[case] price: u128, #[case] slippage: Option<u64>) {
        let record = perp_close_fixture(price, slippage);
        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
        assert_eq!(restored, record);
        let verifier = DeepXPerpCloseCallVerifier::new(remark_snapshot(), remark_key()).unwrap();
        assert_eq!(
            verifier.verify(restored.identity(), restored.signed_extrinsic().unwrap()),
            Ok(())
        );
        assert!(
            DeepXUnsupportedBusinessCallVerifier
                .verify(restored.identity(), restored.signed_extrinsic().unwrap(),)
                .is_err()
        );
        let debug = format!("{verifier:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("0123456789abcdef"));
    }

    #[rstest]
    #[case::subaccount(0)]
    #[case::market(1)]
    #[case::price(2)]
    #[case::slippage_value(3)]
    #[case::slippage_none(4)]
    #[case::signer(5)]
    #[case::genesis(6)]
    #[case::metadata(7)]
    #[case::spec(8)]
    #[case::transaction_version(9)]
    #[case::extensions(10)]
    #[case::timestamp(11)]
    #[case::sequential(12)]
    #[case::missing_operation(13)]
    #[case::other_operation(14)]
    fn perp_close_verifier_rejects_identity_mismatch(#[case] mutation: u8) {
        let record = perp_close_fixture(u128::MAX, Some(u64::MAX));
        let mut wire = serde_json::to_value(record.identity()).unwrap();
        match mutation {
            0 => wire["operation"]["subaccount"][19] = 34.into(),
            1 => wire["operation"]["market_id"] = 0.into(),
            2 => wire["operation"]["price"] = "1".into(),
            3 => wire["operation"]["slippage"] = 0.into(),
            4 => wire["operation"]["slippage"] = serde_json::Value::Null,
            5 => wire["signer"][0] = 34.into(),
            6 => wire["runtime"]["genesis_hash"][0] = 34.into(),
            7 => wire["runtime"]["metadata_sha256"][0] = 34.into(),
            8 => wire["runtime"]["spec_version"] = 369.into(),
            9 => wire["runtime"]["transaction_version"] = 2.into(),
            10 => wire["runtime"]["signed_extensions"] = serde_json::json!([]),
            11 => {
                wire["nonce"] =
                    serde_json::to_value(DeepXNonceReservation::TimestampOrderId { value: 0 })
                        .unwrap()
            }
            12 => {
                wire["nonce"] = serde_json::to_value(DeepXNonceReservation::SequentialAccount {
                    account_index: 0,
                    nonce: u64::MAX,
                })
                .unwrap()
            }
            13 => {
                wire.as_object_mut().unwrap().remove("operation");
            }
            14 => {
                wire["operation"] =
                    serde_json::to_value(perp_cancel_identity([0x11; 20], 1, 7, false).operation())
                        .unwrap()
            }
            _ => unreachable!(),
        }
        let identity: DeepXTransactionIdentity = serde_json::from_value(wire).unwrap();
        let verifier = DeepXPerpCloseCallVerifier::new(remark_snapshot(), remark_key()).unwrap();
        let result = verifier.verify(&identity, record.signed_extrinsic().unwrap());
        if mutation >= 12 {
            assert!(matches!(
                result,
                Err(DeepXBusinessCallBindingError::Unsupported(_))
            ));
        } else {
            assert!(matches!(
                result,
                Err(DeepXBusinessCallBindingError::Mismatch(_))
            ));
        }
    }

    #[rstest]
    #[case::empty(0)]
    #[case::truncated(1)]
    #[case::trailing(2)]
    #[case::signature(3)]
    #[case::hash(4)]
    fn perp_close_verifier_rejects_corrupt_evidence(#[case] mutation: u8) {
        let record = perp_close_fixture(u128::MAX, None);
        let mut bytes = record.signed_extrinsic().unwrap().bytes().to_vec();
        match mutation {
            0 => bytes.clear(),
            1 => {
                bytes.pop();
            }
            2 => bytes.push(0),
            3 => bytes[30] ^= 1,
            4 => (),
            _ => unreachable!(),
        }
        let mut hash: [u8; 32] =
            subxt_core::config::Hasher::hash(&subxt_core::config::substrate::BlakeTwo256, &bytes)
                .into();
        if mutation == 4 {
            hash[0] ^= 1;
        }
        let signed = SignedPalletExtrinsic {
            bytes,
            extrinsic_hash: hash,
            signer: record.identity().signer(),
            nonce: u64::MAX,
            runtime: remark_snapshot().identity().clone(),
        };
        let mut corrupted = DeepXTransactionRecord::created(record.identity().clone());
        if mutation == 4 {
            assert!(corrupted.record_signed(&signed).is_err());
        } else {
            corrupted.record_signed(&signed).unwrap();
            let verifier =
                DeepXPerpCloseCallVerifier::new(remark_snapshot(), remark_key()).unwrap();
            assert!(matches!(
                verifier.verify(record.identity(), corrupted.signed_extrinsic().unwrap()),
                Err(DeepXBusinessCallBindingError::Mismatch(_)),
            ));
        }
    }

    #[rstest]
    fn perp_close_verifier_rejects_wrong_key() {
        let record = perp_close_fixture(1, Some(0));
        let key = DeepXPrivateKey::new(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            &crate::common::DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let verifier = DeepXPerpCloseCallVerifier::new(remark_snapshot(), key).unwrap();
        assert!(matches!(
            verifier.verify(record.identity(), record.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Mismatch(_)),
        ));
    }

    fn perp_cancel_identity(
        subaccount: [u8; 20],
        order_id: u64,
        market_id: u16,
        fast_cancel: bool,
    ) -> DeepXTransactionIdentity {
        DeepXTransactionIdentity::new_perp_cancel(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            crate::signing::derive_signer_account_id(&remark_key()).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId {
                value: 1_725_000_000_125,
            },
            remark_runtime(),
            subaccount,
            order_id,
            market_id,
            fast_cancel,
        )
    }

    #[rstest]
    #[case::ordinary_buy(false, OrderSide::Buy, 0, 0)]
    #[case::ordinary_sell(false, OrderSide::Sell, u64::MAX, u16::MAX)]
    #[case::fast_buy(true, OrderSide::Buy, u64::MAX, u16::MAX)]
    #[case::fast_sell(true, OrderSide::Sell, 1, 1)]
    #[tokio::test]
    async fn perp_cancel_preparation_exact_checkpoint(
        #[case] fast_cancel: bool,
        #[case] side: OrderSide,
        #[case] order_id: u64,
        #[case] market_id: u16,
    ) {
        let identity = DeepXTransactionIdentity::new_perp_cancel(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            derive_signer_account_id(&remark_key()).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            side,
            DeepXNonceReservation::TimestampOrderId { value: u64::MAX },
            remark_runtime(),
            [0xa5; 20],
            order_id,
            market_id,
            fast_cancel,
        );
        let record = DeepXTransactionRecord::created(identity);
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
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let permit = service.acquire().unwrap();
        let signed = sign_perp_cancel(
            &permit,
            &remark_key(),
            DeepXPerpCancelParams {
                subaccount: [0xa5; 20],
                order_id,
                market_id,
                fast_cancel,
            },
            u64::MAX,
        )
        .unwrap();
        let mut expected = record.clone();
        expected.record_signed(&signed).unwrap();
        let prepared = prepare_signed_perp_cancel_transaction(
            &store,
            &lease,
            &committed,
            &record,
            &permit,
            &remark_key(),
        )
        .await
        .unwrap();
        assert_eq!(prepared.record(), &expected);
        assert!(prepared.committed().matches(&expected));
        assert_eq!(
            DeepXTransactionRecord::decode(&prepared.record().encode().unwrap()).unwrap(),
            expected
        );
        assert_eq!(store.current_revision(), 4);
    }

    #[rstest]
    #[case::stale_revision(0)]
    #[case::wrong_key(1)]
    #[case::runtime(2)]
    #[case::lease(3)]
    #[case::already_signed(4)]
    #[case::unknown_commit(5)]
    #[case::wrong_operation(6)]
    #[case::wrong_created_ack(7)]
    #[tokio::test]
    async fn perp_cancel_preparation_failures(#[case] mutation: u8) {
        let identity = perp_cancel_identity([0x11; 20], u64::MAX, u16::MAX, true);
        let mut record = DeepXTransactionRecord::created(identity.clone());
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let permit = service.acquire().unwrap();
        if mutation == 2 {
            let mut runtime = remark_runtime();
            runtime.spec_version += 1;
            record = DeepXTransactionRecord::created(DeepXTransactionIdentity::new_perp_cancel(
                ClientOrderId::new(identity.client_order_id()),
                identity.signer(),
                identity.instrument_id(),
                identity.order_side(),
                identity.nonce(),
                runtime,
                [0x11; 20],
                u64::MAX,
                u16::MAX,
                true,
            ));
        }
        if mutation == 4 {
            let signed = sign_perp_cancel(
                &permit,
                &remark_key(),
                DeepXPerpCancelParams {
                    subaccount: [0x11; 20],
                    order_id: u64::MAX,
                    market_id: u16::MAX,
                    fast_cancel: true,
                },
                1_725_000_000_125,
            )
            .unwrap();
            record.record_signed(&signed).unwrap();
        }
        if mutation == 6 {
            record =
                DeepXTransactionRecord::created(perp_close_fixture(0, None).identity().clone());
        }
        let store = TestStore {
            commit_outcome_unknown: mutation == 5,
            ..TestStore::new(3, &record)
        };
        let mut lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        if mutation == 3 {
            lease.signer = [0xff; 20];
        }
        let ack_record = if mutation == 7 {
            DeepXTransactionRecord::created(perp_cancel_identity(
                [0x22; 20],
                u64::MAX,
                u16::MAX,
                true,
            ))
        } else {
            record.clone()
        };
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &ack_record,
            DeepXTransactionRevision::new(if mutation == 0 { 2 } else { 3 }),
        )
        .unwrap();
        let key = if mutation == 1 {
            DeepXPrivateKey::new(&"22".repeat(32), &crate::common::DeepXKeyScheme::Secp256k1)
                .unwrap()
        } else {
            remark_key()
        };
        let result = prepare_signed_perp_cancel_transaction(
            &store, &lease, &committed, &record, &permit, &key,
        )
        .await;
        match mutation {
            0 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::RevisionConflict
                ))
            )),
            1 | 2 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Binding(
                    DeepXBusinessCallBindingError::Mismatch(_)
                ))
            )),
            3 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::LeaseMismatch
                ))
            )),
            4 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::InvalidState)
            )),
            5 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::CommitOutcomeUnknown(_)
                ))
            )),
            6 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Binding(
                    DeepXBusinessCallBindingError::Unsupported(_)
                ))
            )),
            7 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::AcknowledgementMismatch
                ))
            )),
            _ => unreachable!(),
        }
        assert_eq!(store.current_revision(), if mutation == 5 { 4 } else { 3 });
    }

    #[rstest]
    #[case::ordinary_buy(true, false, 0)]
    #[case::ordinary_sell(false, false, u64::MAX)]
    #[case::fast_buy(true, true, u64::MAX)]
    #[case::fast_sell(false, true, 1)]
    #[tokio::test]
    async fn spot_cancel_preparation_exact_checkpoint(
        #[case] is_buy: bool,
        #[case] fast_cancel: bool,
        #[case] order_id: u64,
    ) {
        let mut pair = [0xa5; 32];
        pair[0] = 0;
        pair[31] = 0xff;
        let params = crate::signing::DeepXSpotCancelParams {
            subaccount: [0xff; 20],
            pair,
            order_id,
            is_buy,
            fast_cancel,
        };
        let identity = DeepXTransactionIdentity::new_spot_cancel(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            derive_signer_account_id(&remark_key()).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC.DEEPX").unwrap(),
            if is_buy {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            DeepXNonceReservation::TimestampOrderId { value: u64::MAX },
            remark_runtime(),
            params,
        );
        let record = DeepXTransactionRecord::created(identity);
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
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let permit = service.acquire().unwrap();
        let signed =
            crate::signing::sign_spot_cancel(&permit, &remark_key(), params, u64::MAX).unwrap();
        let mut expected = record.clone();
        expected.record_signed(&signed).unwrap();
        let prepared = prepare_signed_spot_cancel_transaction(
            &store,
            &lease,
            &committed,
            &record,
            &permit,
            &remark_key(),
        )
        .await
        .unwrap();
        assert_eq!(prepared.record(), &expected);
        assert!(prepared.committed().matches(&expected));
        assert_eq!(
            DeepXTransactionRecord::decode(&prepared.record().encode().unwrap()).unwrap(),
            expected
        );
        assert_eq!(store.current_revision(), 4);
    }

    #[rstest]
    #[case::stale_revision(0)]
    #[case::wrong_key(1)]
    #[case::runtime(2)]
    #[case::lease_signer(3)]
    #[case::already_signed(4)]
    #[case::unknown_commit(5)]
    #[case::wrong_operation(6)]
    #[case::wrong_created_ack(7)]
    #[case::lease_generation(8)]
    #[case::rejected_write(9)]
    #[case::conflicting_signed_ack(10)]
    #[tokio::test]
    async fn spot_cancel_preparation_failures(#[case] mutation: u8) {
        let (identity, signed) = spot_cancel_fixture(true, true);
        let mut record = DeepXTransactionRecord::created(identity.clone());
        if mutation == 2 {
            let mut runtime = remark_runtime();
            runtime.spec_version += 1;
            let Some(super::super::DeepXTransactionOperation::SpotCancel {
                subaccount,
                pair,
                order_id,
                is_buy,
                fast_cancel,
            }) = identity.operation()
            else {
                unreachable!()
            };
            record = DeepXTransactionRecord::created(DeepXTransactionIdentity::new_spot_cancel(
                ClientOrderId::new(identity.client_order_id()),
                identity.signer(),
                identity.instrument_id(),
                identity.order_side(),
                identity.nonce(),
                runtime,
                crate::signing::DeepXSpotCancelParams {
                    subaccount: *subaccount,
                    pair: *pair,
                    order_id: *order_id,
                    is_buy: *is_buy,
                    fast_cancel: *fast_cancel,
                },
            ));
        }
        if mutation == 4 {
            record.record_signed(&signed).unwrap();
        }
        if mutation == 6 {
            record =
                DeepXTransactionRecord::created(perp_close_fixture(0, None).identity().clone());
        }
        let store = TestStore {
            commit_outcome_unknown: mutation == 5,
            signed_commit_fault: match mutation {
                9 => 1,
                10 => 2,
                _ => 0,
            },
            ..TestStore::new(3, &record)
        };
        let mut lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        if mutation == 3 {
            lease.signer = [0xff; 20];
        }
        if mutation == 8 {
            lease.generation += 1;
        }
        let ack_record = if mutation == 7 {
            DeepXTransactionRecord::created(spot_cancel_fixture(false, true).0)
        } else {
            record.clone()
        };
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &ack_record,
            DeepXTransactionRevision::new(if mutation == 0 { 2 } else { 3 }),
        )
        .unwrap();
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let permit = service.acquire().unwrap();
        let key = if mutation == 1 {
            DeepXPrivateKey::new(&"22".repeat(32), &crate::common::DeepXKeyScheme::Secp256k1)
                .unwrap()
        } else {
            remark_key()
        };
        let result = prepare_signed_spot_cancel_transaction(
            &store, &lease, &committed, &record, &permit, &key,
        )
        .await;
        match mutation {
            0 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::RevisionConflict
                ))
            )),
            1 | 2 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Binding(
                    DeepXBusinessCallBindingError::Mismatch(_)
                ))
            )),
            3 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::LeaseMismatch
                ))
            )),
            4 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::InvalidState)
            )),
            5 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::CommitOutcomeUnknown(_)
                ))
            )),
            6 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Binding(
                    DeepXBusinessCallBindingError::Unsupported(_)
                ))
            )),
            7 | 10 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::AcknowledgementMismatch
                ))
            )),
            8 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::LeaseUnavailable(_)
                ))
            )),
            9 => assert!(matches!(
                result,
                Err(DeepXSignedTransactionPreparationError::Persistence(
                    DeepXTransactionPersistenceError::BeforeCommit(_)
                ))
            )),
            _ => unreachable!(),
        }
        assert_eq!(
            store.current_revision(),
            if matches!(mutation, 5 | 10) { 4 } else { 3 }
        );
        let durable =
            DeepXTransactionRecord::decode(&store.encoded_record.lock().unwrap()).unwrap();
        if matches!(mutation, 5 | 10) {
            let mut expected = record.clone();
            expected.record_signed(&signed).unwrap();
            assert_eq!(durable, expected);
        } else {
            assert_eq!(durable, record);
        }
    }

    fn spot_cancel_fixture(
        is_buy: bool,
        fast_cancel: bool,
    ) -> (DeepXTransactionIdentity, SignedPalletExtrinsic) {
        let params = crate::signing::DeepXSpotCancelParams {
            subaccount: [0x11; 20],
            pair: [0xa5; 32],
            order_id: 1_725_000_000_001,
            is_buy,
            fast_cancel,
        };
        let identity = DeepXTransactionIdentity::new_spot_cancel(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            derive_signer_account_id(&remark_key()).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC.DEEPX").unwrap(),
            if is_buy {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            DeepXNonceReservation::TimestampOrderId {
                value: 1_725_000_000_125,
            },
            remark_runtime(),
            params,
        );
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let signed = crate::signing::sign_spot_cancel(
            &service.acquire().unwrap(),
            &remark_key(),
            params,
            1_725_000_000_125,
        )
        .unwrap();
        (identity, signed)
    }

    fn spot_place_fixture(is_buy: bool) -> (DeepXTransactionIdentity, SignedPalletExtrinsic) {
        let params = crate::signing::DeepXSpotPlaceParams {
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
        };
        let nonce = 1_725_000_000_125;
        let identity = DeepXTransactionIdentity::new_spot_place(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            derive_signer_account_id(&remark_key()).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC.DEEPX").unwrap(),
            if is_buy {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            DeepXNonceReservation::TimestampOrderId { value: nonce },
            remark_runtime(),
            params,
        );
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let signed = crate::signing::sign_spot_place_order(
            &service.acquire().unwrap(),
            &remark_key(),
            params,
            nonce,
        )
        .unwrap();
        (identity, signed)
    }

    #[tokio::test]
    async fn spot_place_preparation_exact_checkpoint() {
        let (identity, signed) = spot_place_fixture(true);
        let record = DeepXTransactionRecord::created(identity);
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
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let permit = service.acquire().unwrap();
        let mut expected = record.clone();
        expected.record_signed(&signed).unwrap();
        let prepared = prepare_signed_spot_place_transaction(
            &store,
            &lease,
            &committed,
            &record,
            &permit,
            &remark_key(),
        )
        .await
        .unwrap();
        assert_eq!(prepared.record(), &expected);
        assert!(prepared.committed().matches(&expected));
        assert_eq!(store.current_revision(), 4);
    }

    #[rstest]
    #[case::buy(true)]
    #[case::sell(false)]
    fn spot_place_verifier_round_trip(#[case] is_buy: bool) {
        let (identity, signed) = spot_place_fixture(is_buy);
        let mut record = DeepXTransactionRecord::created(identity);
        record.record_signed(&signed).unwrap();
        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
        assert_eq!(restored, record);
        let verifier = DeepXSpotPlaceCallVerifier::new(remark_snapshot(), remark_key()).unwrap();
        assert_eq!(
            verifier.verify(restored.identity(), restored.signed_extrinsic().unwrap()),
            Ok(())
        );
        assert!(!format!("{verifier:?}").contains("0123456789abcdef"));
    }

    #[rstest]
    #[case::quote_amount(0)]
    #[case::base_amount(1)]
    #[case::order_side(2)]
    #[case::nonce(3)]
    fn spot_place_verifier_rejects_identity_mismatch(#[case] mutation: u8) {
        let (identity, signed) = spot_place_fixture(true);
        let mut wire = serde_json::to_value(&identity).unwrap();
        match mutation {
            0 => wire["operation"]["quote_amount"][0] = 0xff.into(),
            1 => wire["operation"]["base_amount"][31] = 0xff.into(),
            2 => wire["order_side"] = serde_json::to_value(OrderSide::Sell).unwrap(),
            3 => {
                wire["nonce"] =
                    serde_json::to_value(DeepXNonceReservation::TimestampOrderId { value: 1 })
                        .unwrap()
            }
            _ => unreachable!(),
        }
        let mismatched: DeepXTransactionIdentity = serde_json::from_value(wire).unwrap();
        let mut record = DeepXTransactionRecord::created(identity);
        record.record_signed(&signed).unwrap();
        let verifier = DeepXSpotPlaceCallVerifier::new(remark_snapshot(), remark_key()).unwrap();
        assert!(matches!(
            verifier.verify(&mismatched, record.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Mismatch(_))
        ));
    }

    #[rstest]
    #[case::ordinary_buy(true, false)]
    #[case::ordinary_sell(false, false)]
    #[case::fast_buy(true, true)]
    #[case::fast_sell(false, true)]
    fn spot_cancel_verifier_round_trip(#[case] is_buy: bool, #[case] fast_cancel: bool) {
        let (identity, signed) = spot_cancel_fixture(is_buy, fast_cancel);
        let mut record = DeepXTransactionRecord::created(identity.clone());
        record.record_signed(&signed).unwrap();
        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
        assert_eq!(restored, record);
        let verifier = DeepXSpotCancelCallVerifier::new(remark_snapshot(), remark_key()).unwrap();
        assert_eq!(
            verifier.verify(restored.identity(), restored.signed_extrinsic().unwrap()),
            Ok(())
        );
        assert!(
            DeepXUnsupportedBusinessCallVerifier
                .verify(&identity, restored.signed_extrinsic().unwrap())
                .is_err()
        );
        assert!(!format!("{verifier:?}").contains("0123456789abcdef"));
    }

    #[rstest]
    #[case::subaccount(0)]
    #[case::pair_first(1)]
    #[case::pair_last(2)]
    #[case::order_id(3)]
    #[case::is_buy(4)]
    #[case::fast_cancel(5)]
    #[case::signer(6)]
    #[case::genesis(7)]
    #[case::metadata(8)]
    #[case::spec(9)]
    #[case::transaction_version(10)]
    #[case::extensions(11)]
    #[case::nonce(12)]
    #[case::sequential(13)]
    #[case::order_side(14)]
    #[case::missing_operation(15)]
    fn spot_cancel_verifier_rejects_identity_mismatch(#[case] mutation: u8) {
        let (identity, signed) = spot_cancel_fixture(true, false);
        let mut wire = serde_json::to_value(&identity).unwrap();
        match mutation {
            0 => wire["operation"]["subaccount"][0] = 34.into(),
            1 => wire["operation"]["pair"][0] = 34.into(),
            2 => wire["operation"]["pair"][31] = 34.into(),
            3 => wire["operation"]["order_id"] = 1u64.into(),
            4 => wire["operation"]["is_buy"] = false.into(),
            5 => wire["operation"]["fast_cancel"] = true.into(),
            6 => wire["signer"][0] = 34.into(),
            7 => wire["runtime"]["genesis_hash"][0] = 34.into(),
            8 => wire["runtime"]["metadata_sha256"][0] = 34.into(),
            9 => wire["runtime"]["spec_version"] = 369.into(),
            10 => wire["runtime"]["transaction_version"] = 2.into(),
            11 => wire["runtime"]["signed_extensions"] = serde_json::json!([]),
            12 => {
                wire["nonce"] =
                    serde_json::to_value(DeepXNonceReservation::TimestampOrderId { value: 1 })
                        .unwrap()
            }
            13 => {
                wire["nonce"] = serde_json::to_value(DeepXNonceReservation::SequentialAccount {
                    account_index: 0,
                    nonce: 7,
                })
                .unwrap()
            }
            14 => wire["order_side"] = serde_json::to_value(OrderSide::Sell).unwrap(),
            15 => wire
                .as_object_mut()
                .unwrap()
                .remove("operation")
                .map(|_| ())
                .unwrap(),
            _ => unreachable!(),
        }
        let mismatched: DeepXTransactionIdentity = serde_json::from_value(wire).unwrap();
        let mut record = DeepXTransactionRecord::created(identity);
        record.record_signed(&signed).unwrap();
        let verifier = DeepXSpotCancelCallVerifier::new(remark_snapshot(), remark_key()).unwrap();
        assert!(
            verifier
                .verify(&mismatched, record.signed_extrinsic().unwrap())
                .is_err()
        );
    }

    #[rstest]
    #[case::empty(0)]
    #[case::truncated(1)]
    #[case::trailing(2)]
    #[case::signature(3)]
    #[case::hash(4)]
    fn spot_cancel_verifier_rejects_corrupt_evidence(#[case] mutation: u8) {
        let (identity, mut signed) = spot_cancel_fixture(true, false);
        match mutation {
            0 => signed.bytes.clear(),
            1 => {
                signed.bytes.pop();
            }
            2 => signed.bytes.push(0),
            3 => signed.bytes[30] ^= 1,
            4 => signed.extrinsic_hash[0] ^= 1,
            _ => unreachable!(),
        }
        let mut record = DeepXTransactionRecord::created(identity.clone());
        if mutation == 4 {
            assert!(record.record_signed(&signed).is_err());
            return;
        }
        signed.extrinsic_hash = subxt_core::config::Hasher::hash(
            &subxt_core::config::substrate::BlakeTwo256,
            &signed.bytes,
        )
        .into();
        record.record_signed(&signed).unwrap();
        let verifier = DeepXSpotCancelCallVerifier::new(remark_snapshot(), remark_key()).unwrap();
        assert!(matches!(
            verifier.verify(&identity, record.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Mismatch(_))
        ));
    }

    #[rstest]
    fn spot_cancel_verifier_rejects_another_key() {
        let (identity, signed) = spot_cancel_fixture(true, false);
        let mut record = DeepXTransactionRecord::created(identity.clone());
        record.record_signed(&signed).unwrap();
        let key = DeepXPrivateKey::new(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            &crate::common::DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let verifier = DeepXSpotCancelCallVerifier::new(remark_snapshot(), key).unwrap();
        assert!(matches!(
            verifier.verify(&identity, record.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Mismatch(_)),
        ));
    }

    fn perp_cancel_record(identity: &DeepXTransactionIdentity) -> DeepXTransactionRecord {
        let DeepXNonceReservation::TimestampOrderId { value: nonce } = identity.nonce() else {
            unreachable!("perpetual cancel records reserve a timestamp nonce");
        };
        let Some(crate::transaction::DeepXTransactionOperation::PerpCancel {
            subaccount,
            order_id,
            market_id,
            fast_cancel,
        }) = identity.operation()
        else {
            unreachable!("perpetual cancel records retain their operation");
        };
        let service = crate::signing::DeepXRuntimeSnapshotService::new(remark_snapshot());
        let permit = service.acquire().unwrap();
        let signed = sign_perp_cancel(
            &permit,
            &remark_key(),
            DeepXPerpCancelParams {
                subaccount: *subaccount,
                order_id: *order_id,
                market_id: *market_id,
                fast_cancel: *fast_cancel,
            },
            nonce,
        )
        .unwrap();
        let mut record = DeepXTransactionRecord::created(identity.clone());
        record.record_signed(&signed).unwrap();
        record
    }

    #[rstest]
    #[case::ordinary(false)]
    #[case::fast(true)]
    fn perp_cancel_verifier_accepts_exact_durable_operation(#[case] fast_cancel: bool) {
        let identity = perp_cancel_identity([0x11; 20], 1_725_000_000_001, 7, fast_cancel);
        let record = perp_cancel_record(&identity);
        let verifier = DeepXPerpCancelCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        assert_eq!(
            verifier.verify(&identity, record.signed_extrinsic().unwrap()),
            Ok(()),
        );
    }

    #[rstest]
    fn perp_cancel_operation_survives_durable_record_round_trip() {
        let identity = perp_cancel_identity([0x11; 20], 1_725_000_000_001, 7, false);
        let record = perp_cancel_record(&identity);

        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();

        assert_eq!(restored, record);
        assert_eq!(restored.identity().operation(), identity.operation());
    }

    #[rstest]
    #[case::subaccount([0x22; 20], 1_725_000_000_001, 7, false)]
    #[case::order_id([0x11; 20], 1_725_000_000_002, 7, false)]
    #[case::market_id([0x11; 20], 1_725_000_000_001, 8, false)]
    #[case::fast_cancel([0x11; 20], 1_725_000_000_001, 7, true)]
    fn perp_cancel_verifier_rejects_any_operation_field_mismatch(
        #[case] subaccount: [u8; 20],
        #[case] order_id: u64,
        #[case] market_id: u16,
        #[case] fast_cancel: bool,
    ) {
        let signed_identity = perp_cancel_identity([0x11; 20], 1_725_000_000_001, 7, false);
        let record = perp_cancel_record(&signed_identity);
        let mismatched_identity =
            perp_cancel_identity(subaccount, order_id, market_id, fast_cancel);
        let verifier = DeepXPerpCancelCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        assert!(matches!(
            verifier.verify(&mismatched_identity, record.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Mismatch(_)),
        ));
    }

    #[rstest]
    fn perp_cancel_verifier_rejects_identity_without_proven_operation() {
        let signed_identity = perp_cancel_identity([0x11; 20], 1_725_000_000_001, 7, false);
        let record = perp_cancel_record(&signed_identity);
        let verifier = DeepXPerpCancelCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        assert!(matches!(
            verifier.verify(&remark_identity(), record.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Unsupported(_)),
        ));
    }

    #[rstest]
    fn perp_cancel_verifier_rejects_another_reserved_signer() {
        let signed_identity = perp_cancel_identity([0x11; 20], 1_725_000_000_001, 7, false);
        let record = perp_cancel_record(&signed_identity);
        let mismatched_identity = DeepXTransactionIdentity::new_perp_cancel(
            ClientOrderId::new(signed_identity.client_order_id()),
            [0x22; 20],
            signed_identity.instrument_id(),
            signed_identity.order_side(),
            signed_identity.nonce(),
            signed_identity.runtime().clone(),
            [0x11; 20],
            1_725_000_000_001,
            7,
            false,
        );
        let verifier = DeepXPerpCancelCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        assert!(matches!(
            verifier.verify(&mismatched_identity, record.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Mismatch(_)),
        ));
    }

    #[rstest]
    fn perp_cancel_verifier_debug_output_redacts_the_private_key() {
        let verifier = DeepXPerpCancelCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        let debug = format!("{verifier:?}");
        assert!(debug.contains("DeepXPerpCancelCallVerifier"));
        assert!(!debug.contains("0123456789abcdef"));
    }

    #[rstest]
    #[case::empty(0)]
    #[case::truncated(1)]
    #[case::trailing(2)]
    #[case::corrupt_signature(3)]
    #[case::wrong_hash(4)]
    fn perp_cancel_verifier_rejects_malformed_durable_payload(#[case] mutation: u8) {
        let identity = perp_cancel_identity([0x11; 20], 1_725_000_000_001, 7, false);
        let record = perp_cancel_record(&identity);
        let mut signed = SignedPalletExtrinsic {
            bytes: record.signed_extrinsic().unwrap().bytes().to_vec(),
            extrinsic_hash: record.signed_extrinsic().unwrap().extrinsic_hash(),
            signer: identity.signer(),
            nonce: 1_725_000_000_125,
            runtime: remark_snapshot().identity().clone(),
        };
        match mutation {
            0 => signed.bytes.clear(),
            1 => {
                signed.bytes.pop();
            }
            2 => signed.bytes.push(0),
            3 => signed.bytes[30] ^= 1,
            4 => signed.extrinsic_hash[0] ^= 1,
            _ => unreachable!(),
        }
        let mut corrupted = DeepXTransactionRecord::created(identity.clone());
        if mutation == 4 {
            assert!(corrupted.record_signed(&signed).is_err());
            return;
        }
        signed.extrinsic_hash = subxt_core::config::Hasher::hash(
            &subxt_core::config::substrate::BlakeTwo256,
            &signed.bytes,
        )
        .into();
        corrupted.record_signed(&signed).unwrap();
        let verifier = DeepXPerpCancelCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        assert!(matches!(
            verifier.verify(&identity, corrupted.signed_extrinsic().unwrap()),
            Err(DeepXBusinessCallBindingError::Mismatch(_)),
        ));
    }

    #[rstest]
    #[case::genesis(0)]
    #[case::metadata(1)]
    #[case::spec_version(2)]
    #[case::transaction_version(3)]
    #[case::nonce(4)]
    #[case::sequential_nonce(5)]
    fn perp_cancel_verifier_rejects_runtime_or_nonce_mismatch(#[case] mutation: u8) {
        let identity = perp_cancel_identity([0x11; 20], 1_725_000_000_001, 7, false);
        let record = perp_cancel_record(&identity);
        let mut runtime = identity.runtime().clone();
        let mut nonce = identity.nonce();
        match mutation {
            0 => runtime.genesis_hash[0] ^= 1,
            1 => runtime.metadata_sha256[0] ^= 1,
            2 => runtime.spec_version = 369,
            3 => runtime.transaction_version += 1,
            4 => {
                nonce = DeepXNonceReservation::TimestampOrderId {
                    value: 1_725_000_000_126,
                }
            }
            5 => {
                nonce = DeepXNonceReservation::SequentialAccount {
                    account_index: 0,
                    nonce: 7,
                }
            }
            _ => unreachable!(),
        }
        let mismatched = DeepXTransactionIdentity::new_perp_cancel(
            ClientOrderId::new(identity.client_order_id()),
            identity.signer(),
            identity.instrument_id(),
            identity.order_side(),
            nonce,
            runtime,
            [0x11; 20],
            1_725_000_000_001,
            7,
            false,
        );
        let verifier = DeepXPerpCancelCallVerifier::new(remark_snapshot(), remark_key()).unwrap();

        assert!(
            verifier
                .verify(&mismatched, record.signed_extrinsic().unwrap())
                .is_err()
        );
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
            signed_commit_fault: 0,
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
    async fn verified_initial_submission_acceptance_commits_exact_record() {
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
        let submitted =
            submitted_for_bytes(record.signed_extrinsic().unwrap().bytes().to_vec()).await;

        let accepted =
            commit_initial_submission_acceptance(&store, &lease, &committed, &record, submitted)
                .await
                .unwrap();

        assert_eq!(
            accepted.record().lifecycle().state(),
            DeepXTransactionState::Accepted,
        );
        assert_eq!(accepted.committed().revision().value(), 4);
        assert!(accepted.committed().verify(accepted.record()).is_ok());
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn mismatched_submission_acceptance_never_advances_record() {
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
        let submitted = submitted_for_bytes(vec![9, 8, 7]).await;

        assert!(matches!(
            commit_initial_submission_acceptance(&store, &lease, &committed, &record, submitted,)
                .await,
            Err(DeepXSubmissionAcceptanceCommitError::ExtrinsicHashMismatch),
        ));
        assert_eq!(store.current_revision(), 3);
    }

    #[tokio::test]
    async fn stale_acknowledgement_cannot_commit_submission_acceptance() {
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
        let submitted =
            submitted_for_bytes(record.signed_extrinsic().unwrap().bytes().to_vec()).await;

        assert!(matches!(
            commit_initial_submission_acceptance(&store, &lease, &stale, &record, submitted).await,
            Err(DeepXSubmissionAcceptanceCommitError::Persistence(
                DeepXTransactionPersistenceError::RevisionConflict
            )),
        ));
        assert_eq!(store.current_revision(), 4);
    }

    #[tokio::test]
    async fn unknown_submission_acceptance_commit_requires_reconciliation() {
        let record = submitting_record();
        let store = TestStore {
            revision: Mutex::new(3),
            encoded_record: Mutex::new(record.encode().unwrap()),
            active_generation: 4,
            create_outcome_unknown: false,
            commit_outcome_unknown: true,
            signed_commit_fault: 0,
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
        let submitted =
            submitted_for_bytes(record.signed_extrinsic().unwrap().bytes().to_vec()).await;

        assert!(matches!(
            commit_initial_submission_acceptance(&store, &lease, &committed, &record, submitted,)
                .await,
            Err(DeepXSubmissionAcceptanceCommitError::Persistence(
                DeepXTransactionPersistenceError::CommitOutcomeUnknown(_)
            )),
        ));
        assert_eq!(store.current_revision(), 4);
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
    async fn recovery_not_included_is_durably_committed_once() {
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
        let absence = DeepXAbsenceEvidence::new(70, 72, [9; 32], true, true).unwrap();
        let decision = DeepXRecoveryDecision::NotIncluded(absence);

        let not_included = commit_recovery_decision(&store, &lease, &committed, &record, decision)
            .await
            .unwrap();

        assert_eq!(
            not_included.record().lifecycle().state(),
            DeepXTransactionState::NotIncluded,
        );
        assert_eq!(not_included.record().lifecycle().absence(), Some(absence));
        assert_eq!(not_included.committed().revision().value(), 4);
        assert_eq!(store.current_revision(), 4);

        let repeated = commit_recovery_decision(
            &store,
            &lease,
            not_included.committed(),
            not_included.record(),
            decision,
        )
        .await
        .unwrap();

        assert_eq!(repeated.record().lifecycle().absence(), Some(absence));
        assert_eq!(repeated.committed().revision().value(), 4);
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
    async fn pending_pool_reconciliation_commits_acceptance_once() {
        let record = valid_extrinsic_submitting_record();
        let signed_bytes = record.signed_extrinsic().unwrap().bytes();
        let (endpoints, requests) = pending_pool_endpoints(signed_bytes).await;
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
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();

        let accepted = reconcile_submission_pool(&endpoints, &store, &lease, &restored)
            .await
            .unwrap();

        assert_eq!(
            accepted.record().lifecycle().state(),
            DeepXTransactionState::Accepted,
        );
        assert_eq!(accepted.committed().revision().value(), 4);
        assert_eq!(store.current_revision(), 4);

        let restored = DeepXRestoredTransactionRecord::new(
            accepted.record().clone(),
            accepted.committed().clone(),
        )
        .unwrap();
        let repeated = reconcile_submission_pool(&endpoints, &store, &lease, &restored)
            .await
            .unwrap();

        assert_eq!(repeated.committed().revision().value(), 4);
        assert_eq!(store.current_revision(), 4);
        assert_eq!(requests.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn pending_pool_reconciliation_rejects_ineligible_state_before_rpc() {
        let record = signed_record();
        let signed_bytes = record.signed_extrinsic().unwrap().bytes();
        let (endpoints, requests) = pending_pool_endpoints(signed_bytes).await;
        let store = TestStore::new(2, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(2),
        )
        .unwrap();
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();

        assert!(matches!(
            reconcile_submission_pool(&endpoints, &store, &lease, &restored).await,
            Err(DeepXPoolReconciliationCommitError::IneligibleState(
                DeepXTransactionState::Signed,
            )),
        ));
        assert_eq!(requests.load(Ordering::Relaxed), 0);
        assert_eq!(store.current_revision(), 2);
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
    async fn record_bound_reorganization_is_observed_and_committed() {
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
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let (endpoints, capabilities, _) = reorganization_endpoints().await;

        let result =
            observe_and_commit_reorganization(&endpoints, &capabilities, &store, &lease, &restored)
                .await
                .unwrap();

        assert_eq!(
            result.record().lifecycle().state(),
            DeepXTransactionState::Submitting,
        );
        assert_eq!(result.record().lifecycle().inclusion(), None);
        assert_eq!(
            result.record().lifecycle().reverted_inclusion(),
            Some(inclusion),
        );
        assert_eq!(result.committed().revision().value(), 5);
        assert_eq!(store.current_revision(), 5);
        assert_eq!(
            result.record().automatic_replay_decision(),
            DeepXAutomaticReplayDecision::ReconciliationRequired,
        );
    }

    #[tokio::test]
    async fn record_bound_finality_is_observed_and_committed() {
        let record = in_block_record();
        let signed_bytes = record.signed_extrinsic().unwrap().bytes().to_vec();
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
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let (endpoints, capabilities, canonical_requests) =
            finality_endpoints(72, &signed_bytes).await;

        let result =
            observe_and_commit_finality(&endpoints, &capabilities, &store, &lease, &restored)
                .await
                .unwrap();

        assert_eq!(
            result.record().lifecycle().state(),
            DeepXTransactionState::Finalized,
        );
        assert_eq!(result.committed().revision().value(), 5);
        assert!(result.committed().verify(result.record()).is_ok());
        assert_eq!(store.current_revision(), 5);
        assert_eq!(canonical_requests.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn record_bound_finality_pending_preserves_durable_revision() {
        let record = in_block_record();
        let signed_bytes = record.signed_extrinsic().unwrap().bytes().to_vec();
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
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let (endpoints, capabilities, canonical_requests) =
            finality_endpoints(71, &signed_bytes).await;

        let result =
            observe_and_commit_finality(&endpoints, &capabilities, &store, &lease, &restored)
                .await
                .unwrap();

        assert_eq!(
            result.record().lifecycle().state(),
            DeepXTransactionState::InBlockSuccess,
        );
        assert_eq!(result.committed().revision().value(), 4);
        assert_eq!(store.current_revision(), 4);
        assert_eq!(canonical_requests.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn up_to_date_not_included_checkpoint_preserves_durable_revision() {
        let snapshot = remark_snapshot();
        let record = not_included_record_for(&snapshot);
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
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let (endpoints, capabilities, _) = reorganization_endpoints().await;
        let result = reconcile_not_included_checkpoint(
            &endpoints,
            &capabilities,
            &snapshot,
            &store,
            &lease,
            &restored,
            10,
        )
        .await
        .unwrap();

        assert_eq!(
            result.record().lifecycle().state(),
            DeepXTransactionState::NotIncluded,
        );
        assert_eq!(result.committed().revision().value(), 4);
        assert_eq!(store.current_revision(), 4);
    }

    #[rstest]
    #[case::ordinary(0)]
    #[case::fast(1)]
    #[case::wrong_operation(2)]
    #[case::wrong_runtime(3)]
    #[case::wrong_signer(4)]
    #[case::advancing_scan(5)]
    #[tokio::test]
    async fn durable_spot_observer_selection(#[case] scenario: u8) {
        let snapshot = remark_snapshot();
        let (identity, signed) = spot_cancel_fixture(true, scenario == 1);
        let mut record = if scenario == 2 {
            not_included_record_for(&snapshot)
        } else {
            let mut record = DeepXTransactionRecord::created(identity);
            record.record_signed(&signed).unwrap();
            record
                .apply_observation(DeepXTransactionObservation::SubmissionStarted)
                .unwrap();
            record
                .apply_observation(DeepXTransactionObservation::NotIncluded(
                    DeepXAbsenceEvidence::new(70, 72, [9; 32], true, true).unwrap(),
                ))
                .unwrap();
            record
        };
        if scenario == 3 {
            record = submitting_record();
            record
                .apply_observation(DeepXTransactionObservation::NotIncluded(
                    DeepXAbsenceEvidence::new(70, 72, [9; 32], true, true).unwrap(),
                ))
                .unwrap();
        }
        record = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
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
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let (endpoints, capabilities, requests) = reorganization_endpoints().await;
        let before = requests.load(Ordering::Relaxed);
        let (endpoints, capabilities) = if scenario == 5 {
            finalized_recovery_endpoints(74).await
        } else {
            (endpoints, capabilities)
        };
        let selected_snapshot = snapshot.clone();
        let key = if scenario == 4 {
            DeepXPrivateKey::new(
                "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                &crate::common::DeepXKeyScheme::Secp256k1,
            )
            .unwrap()
        } else {
            remark_key()
        };
        let verifier = DeepXSpotCancelCallVerifier::new(snapshot, key).unwrap();
        let result = reconcile_not_included_checkpoint_with_observer(
            &endpoints,
            &capabilities,
            &selected_snapshot,
            &store,
            &lease,
            &restored,
            10,
            DeepXDurableRecoveryObserver::OrdinarySpotCancel(&verifier),
        )
        .await;
        if scenario == 5 {
            let result = result.unwrap();
            assert_eq!(
                result.record().lifecycle().state(),
                DeepXTransactionState::ActionRequired
            );
            assert_eq!(result.committed().revision().value(), 5);
            assert_eq!(
                result.record().lifecycle().absence(),
                restored.record().lifecycle().absence()
            );
        } else if scenario == 0 {
            let result = result.unwrap();
            assert_eq!(
                result.record().lifecycle().state(),
                DeepXTransactionState::NotIncluded
            );
            assert_eq!(result.committed().revision().value(), 4);
            assert!(requests.load(Ordering::Relaxed) > before);
        } else {
            assert!(result.is_err());
            assert_eq!(requests.load(Ordering::Relaxed), before);
        }
        assert_eq!(store.current_revision(), if scenario == 5 { 5 } else { 4 });
    }

    #[rstest]
    #[case::ordinary_checkpoint(false, 0)]
    #[case::fast_checkpoint(true, 0)]
    #[case::ordinary_scan(false, 1)]
    #[case::fast_scan(true, 1)]
    #[case::wrong_operation(false, 2)]
    #[case::wrong_runtime(false, 3)]
    #[case::wrong_signer(false, 4)]
    #[case::corrupted_bytes(false, 5)]
    #[tokio::test]
    async fn durable_perp_observer_selection(#[case] fast_cancel: bool, #[case] scenario: u8) {
        let snapshot = remark_snapshot();
        let identity = perp_cancel_identity([0x11; 20], 1_725_000_000_001, 7, fast_cancel);
        let mut record = match scenario {
            2 => {
                let (identity, signed) = spot_cancel_fixture(true, false);
                let mut record = DeepXTransactionRecord::created(identity);
                record.record_signed(&signed).unwrap();
                record
            }
            3 => submitting_record(),
            _ => perp_cancel_record(&identity),
        };
        if scenario == 5 {
            let mut signed = SignedPalletExtrinsic {
                bytes: record.signed_extrinsic().unwrap().bytes().to_vec(),
                extrinsic_hash: record.signed_extrinsic().unwrap().extrinsic_hash(),
                signer: identity.signer(),
                nonce: 1_725_000_000_125,
                runtime: snapshot.identity().clone(),
            };
            signed.bytes[30] ^= 1;
            signed.extrinsic_hash = subxt_core::config::Hasher::hash(
                &subxt_core::config::substrate::BlakeTwo256,
                &signed.bytes,
            )
            .into();
            record = DeepXTransactionRecord::created(identity);
            record.record_signed(&signed).unwrap();
        }
        if scenario != 3 {
            record
                .apply_observation(DeepXTransactionObservation::SubmissionStarted)
                .unwrap();
        }
        record
            .apply_observation(DeepXTransactionObservation::NotIncluded(
                DeepXAbsenceEvidence::new(70, 72, [9; 32], true, true).unwrap(),
            ))
            .unwrap();
        let record = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
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
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let (endpoints, capabilities, requests) = reorganization_endpoints().await;
        let before = requests.load(Ordering::Relaxed);
        let (endpoints, capabilities) = if scenario == 1 {
            finalized_recovery_endpoints(74).await
        } else {
            (endpoints, capabilities)
        };
        let key = if scenario == 4 {
            DeepXPrivateKey::new(
                "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                &crate::common::DeepXKeyScheme::Secp256k1,
            )
            .unwrap()
        } else {
            remark_key()
        };
        let verifier = DeepXPerpCancelCallVerifier::new(snapshot.clone(), key).unwrap();
        let result = reconcile_not_included_checkpoint_with_observer(
            &endpoints,
            &capabilities,
            &snapshot,
            &store,
            &lease,
            &restored,
            10,
            DeepXDurableRecoveryObserver::PerpCancel(&verifier),
        )
        .await;
        if scenario <= 1 {
            let result = result.unwrap();
            assert_eq!(
                result.record().lifecycle().state(),
                if scenario == 1 {
                    DeepXTransactionState::ActionRequired
                } else {
                    DeepXTransactionState::NotIncluded
                },
            );
            assert_eq!(
                result.committed().revision().value(),
                if scenario == 1 { 5 } else { 4 }
            );
            assert_eq!(
                result.record().lifecycle().absence(),
                restored.record().lifecycle().absence()
            );
        } else {
            assert!(result.is_err());
            assert_eq!(requests.load(Ordering::Relaxed), before);
        }
        assert_eq!(store.current_revision(), if scenario == 1 { 5 } else { 4 });
    }

    #[tokio::test]
    async fn non_atomic_pool_absence_durably_requires_operator_action() {
        let snapshot = remark_snapshot();
        let record = not_included_record_for(&snapshot);
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
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let (endpoints, capabilities) = finalized_recovery_endpoints(74).await;
        let result = reconcile_not_included_checkpoint(
            &endpoints,
            &capabilities,
            &snapshot,
            &store,
            &lease,
            &restored,
            2,
        )
        .await
        .unwrap();

        assert_eq!(
            result.record().lifecycle().state(),
            DeepXTransactionState::ActionRequired,
        );
        assert_eq!(
            result.record().lifecycle().absence(),
            restored.record().lifecycle().absence(),
        );
        assert_eq!(result.committed().revision().value(), 5);
        assert_eq!(store.current_revision(), 5);
    }

    #[tokio::test]
    async fn submitting_record_is_rejected_before_finalized_recovery_rpc() {
        let record = submitting_record();
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
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let (endpoints, capabilities, request_count) = reorganization_endpoints().await;
        let requests_before_recovery = request_count.load(Ordering::Relaxed);
        let snapshot = remark_snapshot();

        let error = reconcile_not_included_checkpoint(
            &endpoints,
            &capabilities,
            &snapshot,
            &store,
            &lease,
            &restored,
            10,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXFinalizedRecoveryCommitError::IneligibleState(DeepXTransactionState::Submitting),
        ));
        assert_eq!(store.current_revision(), 4);
        assert_eq!(
            request_count.load(Ordering::Relaxed),
            requests_before_recovery,
        );
    }

    #[tokio::test]
    async fn finalized_record_is_not_observed_or_committed_as_reorganized() {
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
        record
            .apply_observation(DeepXTransactionObservation::Finalized(inclusion))
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
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let (endpoints, capabilities, request_count) = reorganization_endpoints().await;
        let requests_before_observation = request_count.load(Ordering::Relaxed);

        let error =
            observe_and_commit_reorganization(&endpoints, &capabilities, &store, &lease, &restored)
                .await
                .unwrap_err();

        assert!(matches!(
            error,
            DeepXReorganizationCommitError::IneligibleState(DeepXTransactionState::Finalized),
        ));
        assert_eq!(store.current_revision(), 4);
        assert_eq!(
            request_count.load(Ordering::Relaxed),
            requests_before_observation,
        );
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
    async fn runtime_snapshot_mismatch_is_rejected_before_finalized_recovery_rpc() {
        let mut record = submitting_record();
        record
            .apply_observation(DeepXTransactionObservation::NotIncluded(
                DeepXAbsenceEvidence::new(70, 72, [9; 32], true, true).unwrap(),
            ))
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
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let (endpoints, capabilities, request_count) = reorganization_endpoints().await;
        let requests_before_recovery = request_count.load(Ordering::Relaxed);

        let error = reconcile_not_included_checkpoint(
            &endpoints,
            &capabilities,
            &remark_snapshot(),
            &store,
            &lease,
            &restored,
            10,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXFinalizedRecoveryCommitError::RuntimeSnapshotMismatch,
        ));
        assert_eq!(store.current_revision(), 4);
        assert_eq!(
            request_count.load(Ordering::Relaxed),
            requests_before_recovery
        );
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
            signed_commit_fault: 0,
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
