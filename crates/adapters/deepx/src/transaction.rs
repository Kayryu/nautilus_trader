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

//! Evidence-driven lifecycle primitives for DeepX transactions.

mod persistence;
mod recovery;
mod reservation;

pub use persistence::{
    DeepXBusinessCallBindingError, DeepXBusinessCallVerifier, DeepXCommittedObservation,
    DeepXCommittedTransactionRecord, DeepXObservationCommitError, DeepXPostgresSignerLease,
    DeepXPostgresTransactionStore, DeepXPreparedReservation, DeepXPreparedSignedTransaction,
    DeepXPreparedSubmission, DeepXReservationPreparationError, DeepXRestoredTransactionRecord,
    DeepXSignedTransactionPreparationError, DeepXSignerLease, DeepXSubmissionPermit,
    DeepXSubmissionPreparationError, DeepXTransactionPersistenceError, DeepXTransactionRevision,
    DeepXTransactionStore, DeepXUnsupportedBusinessCallVerifier, commit_reconciliation_observation,
    commit_recovery_decision, commit_reorganization_decision, prepare_initial_submission,
    prepare_signed_transaction, prepare_timestamp_reservation, restore_timestamp_nonce_allocator,
    verify_signer_lease,
};
pub use recovery::{
    DeepXCanonicalBlockEvidence, DeepXMissedBlockScanPlan, DeepXRecoveryDecision,
    DeepXRecoveryScan, DeepXRecoveryScanCollectionError, DeepXRecoveryScanCollector,
    DeepXRecoveryScanPlanError, DeepXRecoveryScanRange, DeepXRecoveryScanRanges,
    DeepXReorganizationDecision, DeepXSubmissionPoolEvidence, classify_reorganization,
    plan_missed_block_scan,
};
pub use reservation::{
    DEEPX_TRANSACTION_CACHE_KEY_PREFIX, DEEPX_TRANSACTION_RECORD_VERSION,
    DeepXDirectRuntimeIdentity, DeepXDurableSignedExtrinsic, DeepXNonceReservation,
    DeepXTimestampNonceAllocator, DeepXTimestampNonceError, DeepXTransactionIdentity,
    DeepXTransactionRecord, DeepXTransactionRecordError,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The fail-closed action required after restoring a durable transaction record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXTransactionRecoveryAction {
    /// Recreate and verify the protocol call inputs before signing can resume.
    RecreateSigningInputs,
    /// Apply an external persistence and submission policy before transmission starts.
    SubmissionDecisionRequired,
    /// Reconcile authoritative pool, canonical-chain, and finality evidence.
    ReconciliationRequired,
    /// No further transaction recovery is required.
    Complete,
    /// Automatic recovery must stop pending operator review.
    OperatorActionRequired,
}

/// The fail-closed automatic replay decision for a restored transaction record.
///
/// No variant authorizes transmission of retained signed bytes. A transaction proved absent must
/// be rebuilt and revalidated as a new replacement because its mortality and business semantics
/// may no longer be valid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXAutomaticReplayDecision {
    /// Recreate and verify protocol inputs before producing any signed bytes.
    RecreateSigningInputs,
    /// Apply the explicit initial-submission policy; restoration alone grants no authority.
    InitialSubmissionPolicyRequired,
    /// Obtain fresh authoritative reconciliation evidence before making another decision.
    ReconciliationRequired,
    /// Build and validate a new replacement instead of replaying retained signed bytes.
    ReplacementRequired,
    /// No transmission is required for the finalized transaction.
    Complete,
    /// Stop automatic processing pending operator review.
    OperatorActionRequired,
}

/// Classifies a failed DeepX transaction submission by the available delivery evidence.
///
/// This axis is independent of retryability. An authoritative acceptance is represented by
/// [`DeepXTransactionObservation::PoolAccepted`] rather than a failure variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeepXSubmissionFailure {
    /// Local evidence proves transmission never started.
    NotSent(String),
    /// The submission node explicitly rejected the signed extrinsic.
    VenueRejected(String),
    /// Transmission may have started and authoritative resolution is unavailable.
    Ambiguous(String),
}

impl DeepXSubmissionFailure {
    /// Creates a failure for an operation proved not to have started transmission.
    #[must_use]
    pub fn not_sent(reason: impl Into<String>) -> Self {
        Self::NotSent(reason.into())
    }

    /// Creates a failure for an authoritative submission-node rejection.
    #[must_use]
    pub fn venue_rejected(reason: impl Into<String>) -> Self {
        Self::VenueRejected(reason.into())
    }

    /// Creates a failure whose delivery outcome requires reconciliation.
    #[must_use]
    pub fn ambiguous(reason: impl Into<String>) -> Self {
        Self::Ambiguous(reason.into())
    }
}

/// The observed lifecycle state of a DeepX transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeepXTransactionState {
    /// Identity and nonce ownership were persisted before signing.
    Created,
    /// Complete signed bytes and their deterministic hash were persisted.
    Signed,
    /// Transmission started and its outcome may be ambiguous.
    Submitting,
    /// A submission node authoritatively accepted the transaction.
    Accepted,
    /// The extrinsic and expected business event succeeded in a best block.
    InBlockSuccess,
    /// The matching extrinsic failed authoritatively in a best block.
    InBlockFailed,
    /// The recorded inclusion is canonical at the finalized boundary.
    Finalized,
    /// Complete canonical scanning and a pool check proved absence.
    NotIncluded,
    /// Evidence is incomplete or conflicting and requires operator action.
    ActionRequired,
}

impl DeepXTransactionState {
    /// Returns the stable string representation used by durable records.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Signed => "signed",
            Self::Submitting => "submitting",
            Self::Accepted => "accepted",
            Self::InBlockSuccess => "in-block-success",
            Self::InBlockFailed => "in-block-failed",
            Self::Finalized => "finalized",
            Self::NotIncluded => "not-included",
            Self::ActionRequired => "action-required",
        }
    }

    /// Returns whether automatic lifecycle mutation must stop.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Finalized | Self::ActionRequired)
    }

    /// Returns whether authoritative reconciliation evidence is still required.
    #[must_use]
    pub const fn requires_reconciliation(self) -> bool {
        matches!(
            self,
            Self::Submitting
                | Self::Accepted
                | Self::InBlockSuccess
                | Self::InBlockFailed
                | Self::NotIncluded
                | Self::ActionRequired
        )
    }
}

/// The authoritative result associated with one included extrinsic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeepXInclusionOutcome {
    /// Both the extrinsic and expected business event succeeded.
    Success,
    /// An authoritative dispatch or expected business event failure was observed.
    Failed,
}

/// Canonical identity and outcome for one included extrinsic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeepXInclusionEvidence {
    /// Hash of the block containing the extrinsic.
    pub block_hash: [u8; 32],
    /// Number of the block containing the extrinsic.
    pub block_number: u64,
    /// Extrinsic index used to match pallet events.
    pub extrinsic_index: u32,
    /// Authoritative extrinsic and business-event outcome.
    pub outcome: DeepXInclusionOutcome,
}

/// Proof boundary required before a transaction can be classified as not included.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct DeepXAbsenceEvidence {
    /// First block covered by the complete canonical scan.
    first_scanned_block: u64,
    /// Finalized block through which the complete canonical scan was performed.
    finalized_block_number: u64,
    /// Hash of the finalized scan boundary.
    finalized_block_hash: [u8; 32],
    /// Whether every canonical block in the inclusive range was scanned.
    canonical_scan_complete: bool,
    /// Whether the submission node authoritatively reported pool absence.
    submission_pool_absence: bool,
}

impl DeepXAbsenceEvidence {
    /// Creates evidence after a complete canonical scan and authoritative submission-pool check.
    ///
    /// Constructing this value asserts that the transaction was absent from every canonical block
    /// in the inclusive range and from the submission node pool at the finalized boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when the scan range ends before it starts.
    pub fn new(
        first_scanned_block: u64,
        finalized_block_number: u64,
        finalized_block_hash: [u8; 32],
        canonical_scan_complete: bool,
        submission_pool_absence: bool,
    ) -> Result<Self, DeepXTransactionError> {
        if finalized_block_number < first_scanned_block {
            return Err(DeepXTransactionError::InvalidAbsenceRange {
                first_scanned_block,
                finalized_block_number,
            });
        }
        if !canonical_scan_complete || !submission_pool_absence {
            return Err(DeepXTransactionError::IncompleteAbsenceEvidence);
        }
        Ok(Self {
            first_scanned_block,
            finalized_block_number,
            finalized_block_hash,
            canonical_scan_complete,
            submission_pool_absence,
        })
    }

    /// Returns the first block covered by the complete canonical scan.
    #[must_use]
    pub const fn first_scanned_block(&self) -> u64 {
        self.first_scanned_block
    }

    /// Returns the finalized block number through which scanning was complete.
    #[must_use]
    pub const fn finalized_block_number(&self) -> u64 {
        self.finalized_block_number
    }

    /// Returns the hash of the finalized scan boundary.
    #[must_use]
    pub const fn finalized_block_hash(&self) -> [u8; 32] {
        self.finalized_block_hash
    }

    pub(super) const fn is_complete(&self) -> bool {
        self.finalized_block_number >= self.first_scanned_block
            && self.canonical_scan_complete
            && self.submission_pool_absence
    }
}

/// An authoritative observation applied to a DeepX transaction lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXTransactionObservation {
    /// Complete signed bytes were persisted under the supplied deterministic hash.
    Signed { extrinsic_hash: [u8; 32] },
    /// Transmission is about to start.
    SubmissionStarted,
    /// A submission node authoritatively accepted the transaction.
    PoolAccepted,
    /// The transaction was observed at a block extrinsic index.
    Included(DeepXInclusionEvidence),
    /// A previously recorded best-block inclusion was removed by a reorganization.
    Reorged(DeepXInclusionEvidence),
    /// The recorded inclusion was proved canonical at the finalized boundary.
    Finalized(DeepXInclusionEvidence),
    /// Complete canonical scanning and a submission-pool check proved absence.
    NotIncluded(DeepXAbsenceEvidence),
    /// Available evidence is incomplete or conflicting.
    ActionRequired,
}

/// Errors raised when transaction evidence violates lifecycle invariants.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXTransactionError {
    /// The observation is not valid from the current lifecycle state.
    #[error("invalid DeepX transaction transition from {state:?} using {observation:?}")]
    InvalidTransition {
        /// Current lifecycle state.
        state: DeepXTransactionState,
        /// Rejected observation.
        observation: DeepXTransactionObservation,
    },
    /// A signed observation attempted to replace the immutable extrinsic hash.
    #[error("conflicting DeepX signed transaction hash")]
    ConflictingExtrinsicHash,
    /// An included observation conflicts with previously recorded block evidence.
    #[error("conflicting DeepX transaction inclusion evidence")]
    ConflictingInclusionEvidence,
    /// Reorganization evidence does not identify the previously recorded inclusion.
    #[error("DeepX reorganization evidence does not match the recorded inclusion")]
    ReorganizationMismatch,
    /// Reorganization evidence conflicts with a previously recorded reorganization.
    #[error("conflicting DeepX transaction reorganization evidence")]
    ConflictingReorganizationEvidence,
    /// A not-included observation conflicts with previously recorded absence evidence.
    #[error("conflicting DeepX transaction absence evidence")]
    ConflictingAbsenceEvidence,
    /// Finality evidence does not identify the previously recorded inclusion.
    #[error("DeepX finality evidence does not match the recorded inclusion")]
    FinalizationMismatch,
    /// A complete scan cannot end before its first block.
    #[error("invalid DeepX absence scan range {first_scanned_block}..={finalized_block_number}")]
    InvalidAbsenceRange {
        /// First block requested for scanning.
        first_scanned_block: u64,
        /// Finalized scan boundary.
        finalized_block_number: u64,
    },
    /// Not-included classification requires both complete scanning and authoritative pool absence.
    #[error("incomplete DeepX transaction absence evidence")]
    IncompleteAbsenceEvidence,
}

/// Pure transaction lifecycle state retained independently from order state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeepXTransactionLifecycle {
    state: DeepXTransactionState,
    extrinsic_hash: Option<[u8; 32]>,
    inclusion: Option<DeepXInclusionEvidence>,
    reverted_inclusion: Option<DeepXInclusionEvidence>,
    absence: Option<DeepXAbsenceEvidence>,
}

impl Default for DeepXTransactionLifecycle {
    fn default() -> Self {
        Self::created()
    }
}

impl DeepXTransactionLifecycle {
    /// Creates a lifecycle after durable identity and nonce reservation.
    #[must_use]
    pub const fn created() -> Self {
        Self {
            state: DeepXTransactionState::Created,
            extrinsic_hash: None,
            inclusion: None,
            reverted_inclusion: None,
            absence: None,
        }
    }

    /// Returns the current observed state.
    #[must_use]
    pub const fn state(&self) -> DeepXTransactionState {
        self.state
    }

    /// Returns the immutable signed extrinsic hash when available.
    #[must_use]
    pub const fn extrinsic_hash(&self) -> Option<[u8; 32]> {
        self.extrinsic_hash
    }

    /// Returns the latest authoritative inclusion evidence when available.
    #[must_use]
    pub const fn inclusion(&self) -> Option<DeepXInclusionEvidence> {
        self.inclusion
    }

    /// Returns the latest best-block inclusion removed by a reorganization.
    #[must_use]
    pub const fn reverted_inclusion(&self) -> Option<DeepXInclusionEvidence> {
        self.reverted_inclusion
    }

    /// Returns complete absence evidence when a scan classified the transaction as not included.
    #[must_use]
    pub const fn absence(&self) -> Option<DeepXAbsenceEvidence> {
        self.absence
    }

    /// Applies one authoritative observation without performing I/O or persistence.
    ///
    /// Returns `true` when the lifecycle changed and `false` for an identical repeated
    /// observation.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid transitions or evidence conflicting with immutable identity.
    pub fn apply(
        &mut self,
        observation: DeepXTransactionObservation,
    ) -> Result<bool, DeepXTransactionError> {
        use DeepXTransactionObservation as Observation;
        use DeepXTransactionState as State;

        match (self.state, observation) {
            (State::Created, Observation::Signed { extrinsic_hash }) => {
                self.extrinsic_hash = Some(extrinsic_hash);
                self.state = State::Signed;
            }
            (State::Signed, Observation::Signed { extrinsic_hash }) => {
                return if self.extrinsic_hash == Some(extrinsic_hash) {
                    Ok(false)
                } else {
                    Err(DeepXTransactionError::ConflictingExtrinsicHash)
                };
            }
            (State::Signed, Observation::SubmissionStarted) => self.state = State::Submitting,
            (State::Submitting, Observation::SubmissionStarted) => return Ok(false),
            (State::Submitting, Observation::PoolAccepted) => self.state = State::Accepted,
            (State::Accepted, Observation::PoolAccepted) => return Ok(false),
            (
                State::Submitting | State::Accepted | State::NotIncluded,
                Observation::Included(e),
            ) => {
                self.inclusion = Some(e);
                self.reverted_inclusion = None;
                self.absence = None;
                self.state = match e.outcome {
                    DeepXInclusionOutcome::Success => State::InBlockSuccess,
                    DeepXInclusionOutcome::Failed => State::InBlockFailed,
                };
            }
            (State::InBlockSuccess | State::InBlockFailed, Observation::Included(e)) => {
                return if self.inclusion == Some(e) {
                    Ok(false)
                } else {
                    Err(DeepXTransactionError::ConflictingInclusionEvidence)
                };
            }
            (State::InBlockSuccess | State::InBlockFailed, Observation::Reorged(e)) => {
                if self.inclusion != Some(e) {
                    return Err(DeepXTransactionError::ReorganizationMismatch);
                }
                self.inclusion = None;
                self.reverted_inclusion = Some(e);
                self.state = State::Submitting;
            }
            (State::Submitting, Observation::Reorged(e)) => {
                return if self.reverted_inclusion == Some(e) {
                    Ok(false)
                } else {
                    Err(DeepXTransactionError::ConflictingReorganizationEvidence)
                };
            }
            (State::InBlockSuccess | State::InBlockFailed, Observation::Finalized(e)) => {
                if self.inclusion != Some(e) {
                    return Err(DeepXTransactionError::FinalizationMismatch);
                }
                self.state = State::Finalized;
            }
            (State::Finalized, Observation::Finalized(e)) => {
                return if self.inclusion == Some(e) {
                    Ok(false)
                } else {
                    Err(DeepXTransactionError::FinalizationMismatch)
                };
            }
            (State::Submitting | State::Accepted, Observation::NotIncluded(e)) => {
                self.absence = Some(e);
                self.state = State::NotIncluded;
            }
            (State::NotIncluded, Observation::NotIncluded(e)) => {
                return if self.absence == Some(e) {
                    Ok(false)
                } else {
                    Err(DeepXTransactionError::ConflictingAbsenceEvidence)
                };
            }
            (state, Observation::ActionRequired) if !state.is_terminal() => {
                self.state = State::ActionRequired;
            }
            (State::ActionRequired, Observation::ActionRequired) => return Ok(false),
            (state, observation) => {
                return Err(DeepXTransactionError::InvalidTransition { state, observation });
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const EXTRINSIC_HASH: [u8; 32] = [1; 32];

    fn inclusion(outcome: DeepXInclusionOutcome) -> DeepXInclusionEvidence {
        DeepXInclusionEvidence {
            block_hash: [2; 32],
            block_number: 42,
            extrinsic_index: 3,
            outcome,
        }
    }

    fn absence() -> DeepXAbsenceEvidence {
        DeepXAbsenceEvidence::new(40, 72, [3; 32], true, true).unwrap()
    }

    fn submitting() -> DeepXTransactionLifecycle {
        let mut lifecycle = DeepXTransactionLifecycle::created();
        lifecycle
            .apply(DeepXTransactionObservation::Signed {
                extrinsic_hash: EXTRINSIC_HASH,
            })
            .unwrap();
        lifecycle
            .apply(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        lifecycle
    }

    #[rstest]
    #[case(DeepXInclusionOutcome::Success, DeepXTransactionState::InBlockSuccess)]
    #[case(DeepXInclusionOutcome::Failed, DeepXTransactionState::InBlockFailed)]
    fn inclusion_can_race_a_pool_acknowledgement(
        #[case] outcome: DeepXInclusionOutcome,
        #[case] expected: DeepXTransactionState,
    ) {
        let evidence = inclusion(outcome);
        let mut lifecycle = submitting();

        assert!(
            lifecycle
                .apply(DeepXTransactionObservation::Included(evidence))
                .unwrap()
        );
        assert_eq!(lifecycle.state(), expected);
        assert_eq!(lifecycle.inclusion(), Some(evidence));
        assert!(lifecycle.state().requires_reconciliation());
    }

    #[rstest]
    #[case(DeepXInclusionOutcome::Success)]
    #[case(DeepXInclusionOutcome::Failed)]
    fn finalization_retains_the_authoritative_business_outcome(
        #[case] outcome: DeepXInclusionOutcome,
    ) {
        let evidence = inclusion(outcome);
        let mut lifecycle = submitting();
        lifecycle
            .apply(DeepXTransactionObservation::Included(evidence))
            .unwrap();

        assert!(
            lifecycle
                .apply(DeepXTransactionObservation::Finalized(evidence))
                .unwrap()
        );
        assert_eq!(lifecycle.state(), DeepXTransactionState::Finalized);
        assert_eq!(lifecycle.inclusion(), Some(evidence));
        assert!(lifecycle.state().is_terminal());
        assert!(!lifecycle.state().requires_reconciliation());
    }

    #[rstest]
    fn repeated_observations_are_idempotent_but_conflicting_identity_is_rejected() {
        let mut lifecycle = DeepXTransactionLifecycle::created();
        let signed = DeepXTransactionObservation::Signed {
            extrinsic_hash: EXTRINSIC_HASH,
        };

        assert!(lifecycle.apply(signed).unwrap());
        assert!(!lifecycle.apply(signed).unwrap());
        assert_eq!(lifecycle.extrinsic_hash(), Some(EXTRINSIC_HASH));
        assert_eq!(
            lifecycle.apply(DeepXTransactionObservation::Signed {
                extrinsic_hash: [9; 32],
            }),
            Err(DeepXTransactionError::ConflictingExtrinsicHash),
        );
        assert_eq!(lifecycle.extrinsic_hash(), Some(EXTRINSIC_HASH));
    }

    #[rstest]
    fn not_included_requires_complete_range_evidence_and_can_be_corrected() {
        assert!(matches!(
            DeepXAbsenceEvidence::new(73, 72, [3; 32], true, true),
            Err(DeepXTransactionError::InvalidAbsenceRange { .. }),
        ));
        assert_eq!(
            DeepXAbsenceEvidence::new(40, 72, [3; 32], false, true),
            Err(DeepXTransactionError::IncompleteAbsenceEvidence),
        );
        assert_eq!(
            DeepXAbsenceEvidence::new(40, 72, [3; 32], true, false),
            Err(DeepXTransactionError::IncompleteAbsenceEvidence),
        );

        let mut lifecycle = submitting();
        let absence = absence();
        lifecycle
            .apply(DeepXTransactionObservation::NotIncluded(absence))
            .unwrap();
        assert_eq!(lifecycle.state(), DeepXTransactionState::NotIncluded);
        assert_eq!(lifecycle.absence(), Some(absence));

        let evidence = inclusion(DeepXInclusionOutcome::Success);
        lifecycle
            .apply(DeepXTransactionObservation::Included(evidence))
            .unwrap();
        assert_eq!(lifecycle.state(), DeepXTransactionState::InBlockSuccess);
        assert_eq!(lifecycle.inclusion(), Some(evidence));
    }

    #[rstest]
    fn finality_must_match_the_recorded_block_and_extrinsic_index() {
        let evidence = inclusion(DeepXInclusionOutcome::Success);
        let mut lifecycle = submitting();
        lifecycle
            .apply(DeepXTransactionObservation::Included(evidence))
            .unwrap();
        let conflicting = DeepXInclusionEvidence {
            extrinsic_index: evidence.extrinsic_index + 1,
            ..evidence
        };

        assert_eq!(
            lifecycle.apply(DeepXTransactionObservation::Finalized(conflicting)),
            Err(DeepXTransactionError::FinalizationMismatch),
        );
        assert_eq!(lifecycle.state(), DeepXTransactionState::InBlockSuccess);
    }

    #[rstest]
    #[case(DeepXInclusionOutcome::Success)]
    #[case(DeepXInclusionOutcome::Failed)]
    fn exact_reorganization_returns_to_reconciliation_and_allows_canonical_reinclusion(
        #[case] outcome: DeepXInclusionOutcome,
    ) {
        let reverted = inclusion(outcome);
        let mut lifecycle = submitting();
        lifecycle
            .apply(DeepXTransactionObservation::Included(reverted))
            .unwrap();

        assert!(
            lifecycle
                .apply(DeepXTransactionObservation::Reorged(reverted))
                .unwrap()
        );
        assert_eq!(lifecycle.state(), DeepXTransactionState::Submitting);
        assert_eq!(lifecycle.inclusion(), None);
        assert_eq!(lifecycle.reverted_inclusion(), Some(reverted));
        assert!(lifecycle.state().requires_reconciliation());
        assert!(
            !lifecycle
                .apply(DeepXTransactionObservation::Reorged(reverted))
                .unwrap()
        );

        let canonical = DeepXInclusionEvidence {
            block_hash: [8; 32],
            block_number: reverted.block_number + 1,
            ..reverted
        };
        lifecycle
            .apply(DeepXTransactionObservation::Included(canonical))
            .unwrap();
        assert_eq!(lifecycle.inclusion(), Some(canonical));
        assert_eq!(lifecycle.reverted_inclusion(), None);
    }

    #[rstest]
    fn reorganization_requires_exact_non_finalized_inclusion_identity() {
        let evidence = inclusion(DeepXInclusionOutcome::Success);
        let conflicting = DeepXInclusionEvidence {
            extrinsic_index: evidence.extrinsic_index + 1,
            ..evidence
        };
        let mut lifecycle = submitting();
        lifecycle
            .apply(DeepXTransactionObservation::Included(evidence))
            .unwrap();

        assert_eq!(
            lifecycle.apply(DeepXTransactionObservation::Reorged(conflicting)),
            Err(DeepXTransactionError::ReorganizationMismatch),
        );
        assert_eq!(lifecycle.inclusion(), Some(evidence));

        lifecycle
            .apply(DeepXTransactionObservation::Finalized(evidence))
            .unwrap();
        assert!(matches!(
            lifecycle.apply(DeepXTransactionObservation::Reorged(evidence)),
            Err(DeepXTransactionError::InvalidTransition { .. }),
        ));
        assert_eq!(lifecycle.state(), DeepXTransactionState::Finalized);
    }

    #[rstest]
    fn terminal_states_reject_automatic_transitions() {
        let mut lifecycle = submitting();
        lifecycle
            .apply(DeepXTransactionObservation::ActionRequired)
            .unwrap();

        assert!(lifecycle.state().is_terminal());
        assert!(matches!(
            lifecycle.apply(DeepXTransactionObservation::PoolAccepted),
            Err(DeepXTransactionError::InvalidTransition { .. }),
        ));
        assert_eq!(lifecycle.state(), DeepXTransactionState::ActionRequired);
    }

    #[rstest]
    #[case(DeepXTransactionObservation::SubmissionStarted)]
    #[case(DeepXTransactionObservation::PoolAccepted)]
    #[case(DeepXTransactionObservation::Included(inclusion(DeepXInclusionOutcome::Success)))]
    #[case(DeepXTransactionObservation::Reorged(inclusion(DeepXInclusionOutcome::Success)))]
    #[case(DeepXTransactionObservation::Finalized(inclusion(DeepXInclusionOutcome::Success)))]
    #[case(DeepXTransactionObservation::NotIncluded(absence()))]
    fn created_state_rejects_observations_that_skip_durable_signing(
        #[case] observation: DeepXTransactionObservation,
    ) {
        let mut lifecycle = DeepXTransactionLifecycle::created();

        assert!(matches!(
            lifecycle.apply(observation),
            Err(DeepXTransactionError::InvalidTransition { .. }),
        ));
        assert_eq!(lifecycle.state(), DeepXTransactionState::Created);
        assert!(lifecycle.extrinsic_hash().is_none());
    }

    #[rstest]
    fn acceptance_does_not_imply_business_success() {
        let mut lifecycle = submitting();

        lifecycle
            .apply(DeepXTransactionObservation::PoolAccepted)
            .unwrap();

        assert_eq!(lifecycle.state(), DeepXTransactionState::Accepted);
        assert!(lifecycle.inclusion().is_none());
        assert!(lifecycle.state().requires_reconciliation());
    }

    #[rstest]
    #[case(DeepXTransactionState::Created, "created")]
    #[case(DeepXTransactionState::Signed, "signed")]
    #[case(DeepXTransactionState::Submitting, "submitting")]
    #[case(DeepXTransactionState::Accepted, "accepted")]
    #[case(DeepXTransactionState::InBlockSuccess, "in-block-success")]
    #[case(DeepXTransactionState::InBlockFailed, "in-block-failed")]
    #[case(DeepXTransactionState::Finalized, "finalized")]
    #[case(DeepXTransactionState::NotIncluded, "not-included")]
    #[case(DeepXTransactionState::ActionRequired, "action-required")]
    fn state_has_stable_durable_representation(
        #[case] state: DeepXTransactionState,
        #[case] expected: &str,
    ) {
        assert_eq!(state.as_str(), expected);
    }

    #[rstest]
    #[case(
        DeepXSubmissionFailure::not_sent("failed before transmission"),
        DeepXSubmissionFailure::NotSent("failed before transmission".to_string()),
    )]
    #[case(
        DeepXSubmissionFailure::venue_rejected("submission node rejected extrinsic"),
        DeepXSubmissionFailure::VenueRejected(
            "submission node rejected extrinsic".to_string(),
        ),
    )]
    #[case(
        DeepXSubmissionFailure::ambiguous("connection closed after send"),
        DeepXSubmissionFailure::Ambiguous("connection closed after send".to_string()),
    )]
    fn submission_failure_constructor_preserves_classification_and_reason(
        #[case] failure: DeepXSubmissionFailure,
        #[case] expected: DeepXSubmissionFailure,
    ) {
        assert_eq!(failure, expected);
    }

    #[rstest]
    fn ambiguous_submission_failure_does_not_manufacture_lifecycle_evidence() {
        let lifecycle = submitting();
        let failure = DeepXSubmissionFailure::ambiguous("connection closed after send");

        assert!(matches!(failure, DeepXSubmissionFailure::Ambiguous(_)));
        assert_eq!(lifecycle.state(), DeepXTransactionState::Submitting);
        assert!(lifecycle.inclusion().is_none());
        assert!(lifecycle.absence().is_none());
    }
}
