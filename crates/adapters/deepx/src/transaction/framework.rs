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

//! Framework order-event eligibility derived from durable transaction evidence.

use nautilus_common::messages::{
    OrderEventApplicationStatus, OrderEventConsumerReceipt, OrderEventPersistenceStatus,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    events::{OrderAccepted, OrderEventAny, OrderRejected, OrderSubmitted},
    identifiers::VenueOrderId,
};
use thiserror::Error;

use super::{
    DeepXFrameworkOrderContext, DeepXFrameworkOrderEventPayload, DeepXFrameworkOrderEventStage,
    DeepXInclusionEvidence, DeepXInclusionOutcome, DeepXNonceReservation,
    DeepXTransactionOperation, DeepXTransactionRecord, DeepXTransactionRecordError,
    DeepXTransactionState,
};

const FINALIZED_REJECTION_REASON: &str = "DeepX finalized transaction failed";

/// Authoritative finalized evidence eligible for one terminal framework order event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXFrameworkOrderTerminalEvidence {
    /// A matching finalized `PerpMarket.OrderPlaced` proves venue acceptance.
    Accepted {
        /// Venue order ID proved by the matching placement event.
        venue_order_id: u64,
        /// Canonical finalized inclusion evidence for the placement.
        inclusion: DeepXInclusionEvidence,
    },
    /// Finalized dispatch or business-event failure proves venue rejection.
    Rejected {
        /// Canonical finalized inclusion evidence for the failure.
        inclusion: DeepXInclusionEvidence,
    },
}

/// Framework order-event milestones supported by current durable evidence.
///
/// This value is an eligibility decision, not an emission receipt. A durable outbox must record
/// framework event identity and delivery progress before a caller may emit any eligible event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXFrameworkOrderEventEligibility {
    context: DeepXFrameworkOrderContext,
    submitted: bool,
    terminal: Option<DeepXFrameworkOrderTerminalEvidence>,
}

impl DeepXFrameworkOrderEventEligibility {
    /// Returns immutable framework identity required to materialize eligible events.
    #[must_use]
    pub const fn context(&self) -> DeepXFrameworkOrderContext {
        self.context
    }

    /// Returns whether durable evidence proves that transmission started.
    #[must_use]
    pub const fn submitted(&self) -> bool {
        self.submitted
    }

    /// Returns authoritative finalized evidence for a terminal order event, when available.
    #[must_use]
    pub const fn terminal(&self) -> Option<DeepXFrameworkOrderTerminalEvidence> {
        self.terminal
    }
}

/// Failures while classifying durable placement evidence for framework events.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum DeepXFrameworkOrderEventEligibilityError {
    /// The durable transaction is not a perpetual placement.
    #[error("DeepX framework order-event classification requires a perpetual placement")]
    UnsupportedOperation,
    /// The placement does not use the timestamp-derived venue order ID domain.
    #[error("DeepX framework order-event classification requires a timestamp order ID")]
    UnsupportedNonceDomain,
    /// The durable placement was not created from a framework command.
    #[error("DeepX framework order-event classification requires durable framework context")]
    MissingFrameworkContext,
    /// A finalized lifecycle has no retained inclusion evidence.
    #[error("DeepX finalized framework order-event classification has no inclusion evidence")]
    MissingFinalizedInclusion,
    /// Operator-action state no longer proves whether submission previously started.
    #[error("DeepX action-required lifecycle has ambiguous framework order-event history")]
    AmbiguousLifecycle,
}

/// Failures while staging eligible framework order events in a durable record.
#[derive(Debug, Error)]
pub enum DeepXFrameworkOrderEventStagingError {
    /// Framework event eligibility could not be established.
    #[error(transparent)]
    Eligibility(#[from] DeepXFrameworkOrderEventEligibilityError),
    /// The durable record or staged outbox violates its schema invariants.
    #[error(transparent)]
    Record(#[from] DeepXTransactionRecordError),
}

/// Failures while materializing pending durable entries as Nautilus order events.
#[derive(Debug, Error)]
pub enum DeepXFrameworkOrderEventMaterializationError {
    /// The durable record or staged outbox violates its schema invariants.
    #[error(transparent)]
    Record(#[from] DeepXTransactionRecordError),
    /// The durable record was not created from a framework command.
    #[error("DeepX framework order-event materialization requires durable framework context")]
    MissingFrameworkContext,
}

/// Failures while applying a consumer receipt to a durable framework outbox.
#[derive(Debug, Error)]
pub enum DeepXFrameworkOrderEventAcknowledgementError {
    /// The durable record or outbox violates its schema invariants.
    #[error(transparent)]
    Record(#[from] DeepXTransactionRecordError),
    /// The consumer did not prove canonical application.
    #[error("DeepX framework order-event receipt did not confirm canonical application")]
    ApplicationUnconfirmed,
    /// The consumer did not prove durable cache persistence.
    #[error("DeepX framework order-event receipt did not confirm durable persistence")]
    PersistenceUnconfirmed,
    /// The receipt event ID is not staged in this outbox.
    #[error("DeepX framework order-event receipt ID is not staged in this outbox")]
    UnknownEvent,
    /// A terminal event cannot be acknowledged before its submitted predecessor.
    #[error("DeepX terminal framework order event cannot be acknowledged before submission")]
    OutOfOrder,
}

/// Classifies framework order-event eligibility from one durable perpetual placement.
///
/// Submission-pool acceptance proves only that transmission was acknowledged. Best-block
/// inclusion remains reorganization-sensitive. Only a finalized successful placement is eligible
/// for `OrderAccepted`, and only a finalized authoritative failure is eligible for
/// `OrderRejected`. Ambiguous and not-included outcomes never become rejection evidence.
///
/// This pure boundary does not construct, persist, deduplicate, or emit framework events.
///
/// # Errors
///
/// Returns an error unless the record describes a framework-owned, timestamp-identified perpetual
/// placement with an unambiguous lifecycle and required finalized inclusion evidence.
pub fn classify_perp_place_framework_order_events(
    record: &DeepXTransactionRecord,
) -> Result<DeepXFrameworkOrderEventEligibility, DeepXFrameworkOrderEventEligibilityError> {
    if !matches!(
        record.identity().operation(),
        Some(DeepXTransactionOperation::PerpPlace { .. })
    ) {
        return Err(DeepXFrameworkOrderEventEligibilityError::UnsupportedOperation);
    }
    let DeepXNonceReservation::TimestampOrderId {
        value: venue_order_id,
    } = record.identity().nonce()
    else {
        return Err(DeepXFrameworkOrderEventEligibilityError::UnsupportedNonceDomain);
    };
    let context = record
        .framework_order_context()
        .ok_or(DeepXFrameworkOrderEventEligibilityError::MissingFrameworkContext)?;

    let state = record.lifecycle().state();
    if state == DeepXTransactionState::ActionRequired {
        return Err(DeepXFrameworkOrderEventEligibilityError::AmbiguousLifecycle);
    }
    let submitted = matches!(
        state,
        DeepXTransactionState::Submitting
            | DeepXTransactionState::Accepted
            | DeepXTransactionState::InBlockSuccess
            | DeepXTransactionState::InBlockFailed
            | DeepXTransactionState::Finalized
            | DeepXTransactionState::NotIncluded
    );
    let terminal = if state == DeepXTransactionState::Finalized {
        let inclusion = record
            .lifecycle()
            .inclusion()
            .ok_or(DeepXFrameworkOrderEventEligibilityError::MissingFinalizedInclusion)?;
        Some(match inclusion.outcome() {
            DeepXInclusionOutcome::Success => DeepXFrameworkOrderTerminalEvidence::Accepted {
                venue_order_id,
                inclusion,
            },
            DeepXInclusionOutcome::Failed => {
                DeepXFrameworkOrderTerminalEvidence::Rejected { inclusion }
            }
        })
    } else {
        None
    };

    Ok(DeepXFrameworkOrderEventEligibility {
        context,
        submitted,
        terminal,
    })
}

/// Stages every currently eligible framework order event in one in-memory record mutation.
///
/// Existing entries retain their original identity and timestamps. Newly eligible entries use the
/// supplied timestamps and remain pending; this function performs no persistence or delivery and
/// cannot mark an event delivered.
///
/// # Errors
///
/// Returns an error when eligibility cannot be classified or the resulting durable record would
/// violate its framework context and lifecycle invariants.
pub fn stage_perp_place_framework_order_events(
    record: &mut DeepXTransactionRecord,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
    reconciliation: bool,
) -> Result<bool, DeepXFrameworkOrderEventStagingError> {
    record.encode()?;
    let eligibility = classify_perp_place_framework_order_events(record)?;
    let context = eligibility.context();
    let mut outbox = record
        .framework_order_outbox()
        .ok_or(DeepXFrameworkOrderEventEligibilityError::MissingFrameworkContext)?;
    let mut changed = false;
    if eligibility.submitted() {
        changed |= outbox.stage_submitted(DeepXFrameworkOrderEventStage::new(
            context.submitted_event_id(),
            ts_event,
            ts_init,
            reconciliation,
            DeepXFrameworkOrderEventPayload::Submitted,
        ));
    }
    if let Some(terminal) = eligibility.terminal() {
        let payload = match terminal {
            DeepXFrameworkOrderTerminalEvidence::Accepted { venue_order_id, .. } => {
                DeepXFrameworkOrderEventPayload::Accepted { venue_order_id }
            }
            DeepXFrameworkOrderTerminalEvidence::Rejected { .. } => {
                DeepXFrameworkOrderEventPayload::Rejected
            }
        };
        changed |= outbox.stage_terminal(DeepXFrameworkOrderEventStage::new(
            context.terminal_event_id(),
            ts_event,
            ts_init,
            reconciliation,
            payload,
        ));
    }
    if changed {
        let mut candidate = record.clone();
        candidate.set_framework_order_outbox(outbox);
        candidate.encode()?;
        *record = candidate;
    }
    Ok(changed)
}

/// Applies an exact durable consumer receipt to one staged framework order event.
///
/// Returns `false` when the same event was already acknowledged. The mutation is in-memory only;
/// callers must commit the resulting record through revision-checked durable storage.
///
/// # Errors
///
/// Returns an error unless the receipt proves canonical application and durable cache persistence
/// for a staged event ID in Submitted-before-terminal order.
pub fn acknowledge_perp_place_framework_order_event(
    record: &mut DeepXTransactionRecord,
    receipt: OrderEventConsumerReceipt,
) -> Result<bool, DeepXFrameworkOrderEventAcknowledgementError> {
    record.encode()?;
    if !matches!(
        receipt.application,
        OrderEventApplicationStatus::Applied | OrderEventApplicationStatus::AlreadyApplied
    ) {
        return Err(DeepXFrameworkOrderEventAcknowledgementError::ApplicationUnconfirmed);
    }
    if receipt.persistence != OrderEventPersistenceStatus::Persisted {
        return Err(DeepXFrameworkOrderEventAcknowledgementError::PersistenceUnconfirmed);
    }

    let mut outbox = record
        .framework_order_outbox()
        .ok_or(DeepXFrameworkOrderEventAcknowledgementError::UnknownEvent)?;
    let changed = if outbox
        .submitted()
        .is_some_and(|stage| stage.event_id() == receipt.event_id)
    {
        outbox
            .acknowledge_submitted(receipt.event_id)
            .ok_or(DeepXFrameworkOrderEventAcknowledgementError::UnknownEvent)?
    } else if outbox
        .terminal()
        .is_some_and(|stage| stage.event_id() == receipt.event_id)
    {
        if !outbox
            .submitted()
            .is_some_and(|stage| stage.is_acknowledged())
        {
            return Err(DeepXFrameworkOrderEventAcknowledgementError::OutOfOrder);
        }
        outbox
            .acknowledge_terminal(receipt.event_id)
            .ok_or(DeepXFrameworkOrderEventAcknowledgementError::UnknownEvent)?
    } else {
        return Err(DeepXFrameworkOrderEventAcknowledgementError::UnknownEvent);
    };

    if changed {
        let mut candidate = record.clone();
        candidate.set_framework_order_outbox(outbox);
        candidate.encode()?;
        *record = candidate;
    }
    Ok(changed)
}

/// Materializes pending durable outbox entries in framework transition order.
///
/// The returned values retain the exact staged IDs and timestamps. This pure function performs no
/// channel send, cache mutation, delivery acknowledgement, or outbox mutation.
///
/// # Errors
///
/// Returns an error when the durable record is invalid or has no framework context and outbox.
pub fn materialize_perp_place_framework_order_events(
    record: &DeepXTransactionRecord,
) -> Result<Vec<OrderEventAny>, DeepXFrameworkOrderEventMaterializationError> {
    record.encode()?;
    let context = record
        .framework_order_context()
        .ok_or(DeepXFrameworkOrderEventMaterializationError::MissingFrameworkContext)?;
    let outbox = record
        .framework_order_outbox()
        .ok_or(DeepXFrameworkOrderEventMaterializationError::MissingFrameworkContext)?;
    let mut events = Vec::with_capacity(2);
    if let Some(stage) = outbox.submitted()
        && !stage.is_acknowledged()
    {
        let mut event = OrderSubmitted::new(
            context.trader_id(),
            context.strategy_id(),
            context.instrument_id(),
            context.client_order_id(),
            context.account_id(),
            stage.event_id(),
            stage.ts_event(),
            stage.ts_init(),
        );
        event.causation_id = Some(context.command_id());
        events.push(OrderEventAny::Submitted(event));
    }
    if let Some(stage) = outbox.terminal()
        && !stage.is_acknowledged()
    {
        let event = match stage.payload() {
            DeepXFrameworkOrderEventPayload::Accepted { venue_order_id } => {
                let mut event = OrderAccepted::new(
                    context.trader_id(),
                    context.strategy_id(),
                    context.instrument_id(),
                    context.client_order_id(),
                    VenueOrderId::new(venue_order_id.to_string()),
                    context.account_id(),
                    stage.event_id(),
                    stage.ts_event(),
                    stage.ts_init(),
                    stage.reconciliation(),
                );
                event.causation_id = Some(context.command_id());
                OrderEventAny::Accepted(event)
            }
            DeepXFrameworkOrderEventPayload::Rejected => {
                let mut event = OrderRejected::new(
                    context.trader_id(),
                    context.strategy_id(),
                    context.instrument_id(),
                    context.client_order_id(),
                    context.account_id(),
                    FINALIZED_REJECTION_REASON.into(),
                    stage.event_id(),
                    stage.ts_event(),
                    stage.ts_init(),
                    stage.reconciliation(),
                    false,
                );
                event.causation_id = Some(context.command_id());
                OrderEventAny::Rejected(event)
            }
            DeepXFrameworkOrderEventPayload::Submitted => {
                unreachable!("durable outbox validation rejects submitted terminal payload")
            }
        };
        events.push(event);
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::OrderSide,
        identifiers::{ClientOrderId, InstrumentId},
    };
    use rstest::rstest;
    use subxt_core::config::{Hasher, substrate::BlakeTwo256};

    use super::*;
    use crate::{
        common::DeepXEnvironment,
        signing::{
            ApprovedRuntimeIdentity, DeepXPerpOrderType, DeepXPerpPlaceParams, DeepXPostOnlyParam,
            DeepXTimeInForce, SignedPalletExtrinsic,
        },
        transaction::{
            DeepXAbsenceEvidence, DeepXDirectRuntimeIdentity, DeepXSubmissionScanCheckpoint,
            DeepXTransactionIdentity, DeepXTransactionObservation,
        },
    };

    const ORDER_ID: u64 = 1_789_445_053_841;

    fn event_id(value: u8) -> nautilus_core::UUID4 {
        nautilus_core::UUID4::from_bytes([value; 16])
    }

    fn framework_context() -> DeepXFrameworkOrderContext {
        DeepXFrameworkOrderContext::new(
            nautilus_model::identifiers::TraderId::from("TRADER-001"),
            nautilus_model::identifiers::StrategyId::from("S-DEEPX-001"),
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            ClientOrderId::from("O-19700101-000000-001-001-1"),
            nautilus_model::identifiers::AccountId::from("DEEPX-001"),
            event_id(1),
            nautilus_core::UnixNanos::from(10),
            event_id(2),
            nautilus_core::UnixNanos::from(9),
            Some(event_id(3)),
            Some(event_id(4)),
            event_id(5),
            event_id(6),
        )
    }

    fn record() -> DeepXTransactionRecord {
        let runtime = ApprovedRuntimeIdentity {
            environment: DeepXEnvironment::Testnet,
            genesis_hash: [1; 32],
            metadata_sha256: [2; 32],
            spec_version: 369,
            transaction_version: 1,
            signed_extensions: vec!["CheckNonce".to_string()],
        };
        let identity = DeepXTransactionIdentity::new_perp_place(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            [7; 20],
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId { value: ORDER_ID },
            DeepXDirectRuntimeIdentity::from(&runtime),
            DeepXPerpPlaceParams {
                subaccount: [7; 20],
                market_id: 3,
                is_long: true,
                size: 1_250_000_000,
                price: 2_500_000_000,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                take_profit: None,
                stop_loss: None,
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
        );
        DeepXTransactionRecord::created_with_framework_order_context(
            identity,
            DeepXSubmissionScanCheckpoint::new(100, [3; 32]),
            framework_context(),
        )
    }

    fn signed_record() -> DeepXTransactionRecord {
        let mut record = record();
        let bytes = vec![1, 2, 3];
        let identity = record.identity();
        let runtime = identity.runtime();
        record
            .record_signed(&SignedPalletExtrinsic {
                extrinsic_hash: BlakeTwo256.hash(&bytes).0,
                bytes,
                signer: identity.signer(),
                nonce: ORDER_ID,
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
    }

    fn submitted_record() -> DeepXTransactionRecord {
        let mut record = signed_record();
        record
            .apply_observation(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        record
    }

    fn inclusion(outcome: DeepXInclusionOutcome) -> DeepXInclusionEvidence {
        DeepXInclusionEvidence::from_durable_parts([4; 32], 101, 5, outcome)
    }

    fn finalized_staged_record() -> DeepXTransactionRecord {
        let mut record = submitted_record();
        let inclusion = inclusion(DeepXInclusionOutcome::Success);
        record
            .apply_observation(DeepXTransactionObservation::Included(inclusion))
            .unwrap();
        record
            .apply_observation(DeepXTransactionObservation::Finalized(inclusion))
            .unwrap();
        stage_perp_place_framework_order_events(
            &mut record,
            UnixNanos::from(100),
            UnixNanos::from(101),
            true,
        )
        .unwrap();
        record
    }

    fn receipt(
        event_id: nautilus_core::UUID4,
        application: OrderEventApplicationStatus,
        persistence: OrderEventPersistenceStatus,
    ) -> OrderEventConsumerReceipt {
        OrderEventConsumerReceipt {
            event_id,
            application,
            persistence,
        }
    }

    #[rstest]
    fn created_and_signed_records_are_not_submitted() {
        for record in [record(), signed_record()] {
            let eligibility = classify_perp_place_framework_order_events(&record).unwrap();

            assert_eq!(eligibility.context(), framework_context());
            assert!(!eligibility.submitted());
            assert_eq!(eligibility.terminal(), None);
        }
    }

    #[rstest]
    fn generic_durable_placement_is_not_framework_event_authority() {
        let record = record();
        let generic = DeepXTransactionRecord::created_with_submission_scan_checkpoint(
            record.identity().clone(),
            record.submission_scan_checkpoint().unwrap(),
        );

        assert_eq!(
            classify_perp_place_framework_order_events(&generic),
            Err(DeepXFrameworkOrderEventEligibilityError::MissingFrameworkContext),
        );
        assert!(matches!(
            materialize_perp_place_framework_order_events(&generic),
            Err(DeepXFrameworkOrderEventMaterializationError::MissingFrameworkContext),
        ));
    }

    #[rstest]
    fn pool_acceptance_is_not_framework_order_acceptance() {
        let mut record = submitted_record();
        record
            .apply_observation(DeepXTransactionObservation::PoolAccepted)
            .unwrap();

        let eligibility = classify_perp_place_framework_order_events(&record).unwrap();

        assert!(eligibility.submitted());
        assert_eq!(eligibility.terminal(), None);
    }

    #[rstest]
    #[case(DeepXInclusionOutcome::Success)]
    #[case(DeepXInclusionOutcome::Failed)]
    fn best_block_inclusion_is_not_a_terminal_framework_event(
        #[case] outcome: DeepXInclusionOutcome,
    ) {
        let mut record = submitted_record();
        record
            .apply_observation(DeepXTransactionObservation::Included(inclusion(outcome)))
            .unwrap();

        let eligibility = classify_perp_place_framework_order_events(&record).unwrap();

        assert!(eligibility.submitted());
        assert_eq!(eligibility.terminal(), None);
    }

    #[rstest]
    fn not_included_is_not_rejection_evidence() {
        let mut record = submitted_record();
        record
            .apply_observation(DeepXTransactionObservation::NotIncluded(
                DeepXAbsenceEvidence::new(101, 120, [5; 32], true, true).unwrap(),
            ))
            .unwrap();

        let eligibility = classify_perp_place_framework_order_events(&record).unwrap();

        assert!(eligibility.submitted());
        assert_eq!(eligibility.terminal(), None);
    }

    #[rstest]
    fn action_required_fails_closed_on_lost_submission_history() {
        let mut record = submitted_record();
        record
            .apply_observation(DeepXTransactionObservation::ActionRequired)
            .unwrap();

        assert_eq!(
            classify_perp_place_framework_order_events(&record),
            Err(DeepXFrameworkOrderEventEligibilityError::AmbiguousLifecycle),
        );
    }

    #[rstest]
    #[case(DeepXInclusionOutcome::Success)]
    #[case(DeepXInclusionOutcome::Failed)]
    fn only_finalized_outcome_is_terminal(#[case] outcome: DeepXInclusionOutcome) {
        let mut record = submitted_record();
        let inclusion = inclusion(outcome);
        record
            .apply_observation(DeepXTransactionObservation::Included(inclusion))
            .unwrap();
        record
            .apply_observation(DeepXTransactionObservation::Finalized(inclusion))
            .unwrap();

        let eligibility = classify_perp_place_framework_order_events(&record).unwrap();

        assert!(eligibility.submitted());
        let expected = match outcome {
            DeepXInclusionOutcome::Success => DeepXFrameworkOrderTerminalEvidence::Accepted {
                venue_order_id: ORDER_ID,
                inclusion,
            },
            DeepXInclusionOutcome::Failed => {
                DeepXFrameworkOrderTerminalEvidence::Rejected { inclusion }
            }
        };
        assert_eq!(eligibility.terminal(), Some(expected));
    }

    #[rstest]
    fn staging_before_submission_is_a_noop() {
        let mut record = signed_record();

        assert!(
            !stage_perp_place_framework_order_events(
                &mut record,
                UnixNanos::from(100),
                UnixNanos::from(101),
                false,
            )
            .unwrap()
        );
        assert_eq!(
            record.framework_order_outbox(),
            Some(crate::transaction::DeepXFrameworkOrderOutbox::empty()),
        );
    }

    #[rstest]
    fn submitted_staging_is_idempotent_and_retains_first_timestamps() {
        let mut record = submitted_record();

        assert!(
            stage_perp_place_framework_order_events(
                &mut record,
                UnixNanos::from(100),
                UnixNanos::from(101),
                false,
            )
            .unwrap()
        );
        assert!(
            !stage_perp_place_framework_order_events(
                &mut record,
                UnixNanos::from(200),
                UnixNanos::from(201),
                true,
            )
            .unwrap()
        );

        let outbox = record.framework_order_outbox().unwrap();
        let submitted = outbox.submitted().unwrap();
        assert_eq!(
            submitted.event_id(),
            framework_context().submitted_event_id()
        );
        assert_eq!(submitted.ts_event(), UnixNanos::from(100));
        assert_eq!(submitted.ts_init(), UnixNanos::from(101));
        assert_eq!(
            submitted.payload(),
            DeepXFrameworkOrderEventPayload::Submitted,
        );
        assert_eq!(outbox.terminal(), None);

        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
        assert_eq!(restored.framework_order_outbox(), Some(outbox));
        assert!(
            !restored
                .framework_order_outbox()
                .unwrap()
                .submitted()
                .unwrap()
                .reconciliation()
        );

        let events = materialize_perp_place_framework_order_events(&restored).unwrap();
        let [OrderEventAny::Submitted(event)] = events.as_slice() else {
            panic!("expected exactly one submitted event")
        };
        assert_eq!(event.trader_id, framework_context().trader_id());
        assert_eq!(event.strategy_id, framework_context().strategy_id());
        assert_eq!(event.instrument_id, framework_context().instrument_id());
        assert_eq!(event.client_order_id, framework_context().client_order_id());
        assert_eq!(event.account_id, framework_context().account_id());
        assert_eq!(event.event_id, framework_context().submitted_event_id());
        assert_eq!(event.ts_event, UnixNanos::from(100));
        assert_eq!(event.ts_init, UnixNanos::from(101));
        assert_eq!(event.causation_id, Some(framework_context().command_id()));
    }

    #[rstest]
    fn malformed_staged_reconciliation_is_rejected() {
        let mut record = submitted_record();
        stage_perp_place_framework_order_events(
            &mut record,
            UnixNanos::from(100),
            UnixNanos::from(101),
            true,
        )
        .unwrap();
        let mut value = serde_json::to_value(record).unwrap();
        value["framework_order_outbox"]["submitted"]["reconciliation"] =
            serde_json::Value::String("true".to_string());

        assert!(matches!(
            DeepXTransactionRecord::decode(&serde_json::to_vec(&value).unwrap()),
            Err(DeepXTransactionRecordError::Encoding(_)),
        ));
    }

    #[rstest]
    #[case(
        DeepXInclusionOutcome::Success,
        DeepXFrameworkOrderEventPayload::Accepted { venue_order_id: ORDER_ID }
    )]
    #[case(
        DeepXInclusionOutcome::Failed,
        DeepXFrameworkOrderEventPayload::Rejected
    )]
    fn finalized_staging_adds_submitted_and_matching_terminal(
        #[case] outcome: DeepXInclusionOutcome,
        #[case] expected_payload: DeepXFrameworkOrderEventPayload,
    ) {
        let mut record = submitted_record();
        let inclusion = inclusion(outcome);
        record
            .apply_observation(DeepXTransactionObservation::Included(inclusion))
            .unwrap();
        record
            .apply_observation(DeepXTransactionObservation::Finalized(inclusion))
            .unwrap();

        assert!(
            stage_perp_place_framework_order_events(
                &mut record,
                UnixNanos::from(300),
                UnixNanos::from(301),
                true,
            )
            .unwrap()
        );

        let outbox = record.framework_order_outbox().unwrap();
        assert_eq!(
            outbox.submitted().unwrap().payload(),
            DeepXFrameworkOrderEventPayload::Submitted,
        );
        let terminal = outbox.terminal().unwrap();
        assert_eq!(terminal.event_id(), framework_context().terminal_event_id());
        assert_eq!(terminal.payload(), expected_payload);
        assert_eq!(terminal.ts_event(), UnixNanos::from(300));
        assert_eq!(terminal.ts_init(), UnixNanos::from(301));
        assert!(terminal.reconciliation());

        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
        assert_eq!(restored.framework_order_outbox(), Some(outbox));

        let events = materialize_perp_place_framework_order_events(&restored).unwrap();
        assert!(matches!(events.first(), Some(OrderEventAny::Submitted(_))));
        match (outcome, &events[1]) {
            (DeepXInclusionOutcome::Success, OrderEventAny::Accepted(event)) => {
                assert_eq!(event.trader_id, framework_context().trader_id());
                assert_eq!(event.strategy_id, framework_context().strategy_id());
                assert_eq!(event.instrument_id, framework_context().instrument_id());
                assert_eq!(event.client_order_id, framework_context().client_order_id());
                assert_eq!(
                    event.venue_order_id,
                    VenueOrderId::new(ORDER_ID.to_string())
                );
                assert_eq!(event.account_id, framework_context().account_id());
                assert_eq!(event.event_id, framework_context().terminal_event_id());
                assert_eq!(event.ts_event, UnixNanos::from(300));
                assert_eq!(event.ts_init, UnixNanos::from(301));
                assert!(event.reconciliation);
                assert_eq!(event.causation_id, Some(framework_context().command_id()));
            }
            (DeepXInclusionOutcome::Failed, OrderEventAny::Rejected(event)) => {
                assert_eq!(event.trader_id, framework_context().trader_id());
                assert_eq!(event.strategy_id, framework_context().strategy_id());
                assert_eq!(event.instrument_id, framework_context().instrument_id());
                assert_eq!(event.client_order_id, framework_context().client_order_id());
                assert_eq!(event.account_id, framework_context().account_id());
                assert_eq!(event.reason.as_str(), FINALIZED_REJECTION_REASON);
                assert_eq!(event.event_id, framework_context().terminal_event_id());
                assert_eq!(event.ts_event, UnixNanos::from(300));
                assert_eq!(event.ts_init, UnixNanos::from(301));
                assert!(event.reconciliation);
                assert!(!event.due_post_only);
                assert_eq!(event.causation_id, Some(framework_context().command_id()));
            }
            _ => panic!("unexpected terminal materialization"),
        }
    }

    #[rstest]
    #[case(OrderEventApplicationStatus::Applied)]
    #[case(OrderEventApplicationStatus::AlreadyApplied)]
    fn confirmed_submitted_receipt_is_acknowledged(
        #[case] application: OrderEventApplicationStatus,
    ) {
        let mut record = finalized_staged_record();
        let submitted_event_id = framework_context().submitted_event_id();

        assert!(
            acknowledge_perp_place_framework_order_event(
                &mut record,
                receipt(
                    submitted_event_id,
                    application,
                    OrderEventPersistenceStatus::Persisted,
                ),
            )
            .unwrap()
        );
        assert!(
            record
                .framework_order_outbox()
                .unwrap()
                .submitted()
                .unwrap()
                .is_acknowledged()
        );
        let pending = materialize_perp_place_framework_order_events(&record).unwrap();
        let [OrderEventAny::Accepted(event)] = pending.as_slice() else {
            panic!("expected only the pending terminal event")
        };
        assert_eq!(event.event_id, framework_context().terminal_event_id());

        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
        assert_eq!(restored, record);
        assert!(
            restored
                .framework_order_outbox()
                .unwrap()
                .submitted()
                .unwrap()
                .is_acknowledged()
        );
    }

    #[rstest]
    #[case(OrderEventApplicationStatus::Rejected)]
    #[case(OrderEventApplicationStatus::Unconfirmed)]
    fn unconfirmed_application_does_not_acknowledge(
        #[case] application: OrderEventApplicationStatus,
    ) {
        let mut record = finalized_staged_record();
        let before = record.clone();

        assert!(matches!(
            acknowledge_perp_place_framework_order_event(
                &mut record,
                receipt(
                    framework_context().submitted_event_id(),
                    application,
                    OrderEventPersistenceStatus::Persisted,
                ),
            ),
            Err(DeepXFrameworkOrderEventAcknowledgementError::ApplicationUnconfirmed),
        ));
        assert_eq!(record, before);
    }

    #[rstest]
    #[case(OrderEventPersistenceStatus::NotConfigured)]
    #[case(OrderEventPersistenceStatus::Unconfirmed)]
    #[case(OrderEventPersistenceStatus::Failed)]
    #[case(OrderEventPersistenceStatus::NotApplicable)]
    fn unconfirmed_persistence_does_not_acknowledge(
        #[case] persistence: OrderEventPersistenceStatus,
    ) {
        let mut record = finalized_staged_record();
        let before = record.clone();

        assert!(matches!(
            acknowledge_perp_place_framework_order_event(
                &mut record,
                receipt(
                    framework_context().submitted_event_id(),
                    OrderEventApplicationStatus::Applied,
                    persistence,
                ),
            ),
            Err(DeepXFrameworkOrderEventAcknowledgementError::PersistenceUnconfirmed),
        ));
        assert_eq!(record, before);
    }

    #[rstest]
    fn unknown_and_out_of_order_receipts_do_not_acknowledge() {
        let mut record = finalized_staged_record();
        let before = record.clone();

        assert!(matches!(
            acknowledge_perp_place_framework_order_event(
                &mut record,
                receipt(
                    event_id(99),
                    OrderEventApplicationStatus::Applied,
                    OrderEventPersistenceStatus::Persisted,
                ),
            ),
            Err(DeepXFrameworkOrderEventAcknowledgementError::UnknownEvent),
        ));
        assert!(matches!(
            acknowledge_perp_place_framework_order_event(
                &mut record,
                receipt(
                    framework_context().terminal_event_id(),
                    OrderEventApplicationStatus::Applied,
                    OrderEventPersistenceStatus::Persisted,
                ),
            ),
            Err(DeepXFrameworkOrderEventAcknowledgementError::OutOfOrder),
        ));
        assert_eq!(record, before);
    }

    #[rstest]
    fn exact_acknowledgements_are_idempotent_and_drain_the_outbox_in_order() {
        let mut record = finalized_staged_record();
        let submitted_receipt = receipt(
            framework_context().submitted_event_id(),
            OrderEventApplicationStatus::Applied,
            OrderEventPersistenceStatus::Persisted,
        );
        let terminal_receipt = receipt(
            framework_context().terminal_event_id(),
            OrderEventApplicationStatus::AlreadyApplied,
            OrderEventPersistenceStatus::Persisted,
        );

        assert!(
            acknowledge_perp_place_framework_order_event(&mut record, submitted_receipt).unwrap()
        );
        assert!(
            !acknowledge_perp_place_framework_order_event(&mut record, submitted_receipt).unwrap()
        );
        let pending = materialize_perp_place_framework_order_events(&record).unwrap();
        let [OrderEventAny::Accepted(event)] = pending.as_slice() else {
            panic!("expected only the pending terminal event")
        };
        assert_eq!(event.event_id, framework_context().terminal_event_id());

        assert!(
            acknowledge_perp_place_framework_order_event(&mut record, terminal_receipt).unwrap()
        );
        assert!(
            !acknowledge_perp_place_framework_order_event(&mut record, terminal_receipt).unwrap()
        );
        assert!(
            materialize_perp_place_framework_order_events(&record)
                .unwrap()
                .is_empty()
        );
    }
}
