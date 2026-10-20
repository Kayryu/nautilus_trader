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
    events::{
        OrderAccepted, OrderCancelRejected, OrderCanceled, OrderEventAny, OrderModifyRejected,
        OrderPendingCancel, OrderPendingUpdate, OrderRejected, OrderSubmitted, OrderUpdated,
    },
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
const FINALIZED_CANCEL_REJECTION_REASON: &str = "DeepX finalized cancellation failed";
const FINALIZED_MODIFY_REJECTION_REASON: &str = "DeepX finalized modification failed";

/// Verified backend or finalized evidence eligible for one terminal framework order event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXFrameworkOrderTerminalEvidence {
    /// A hash-bound backend order record proves the order was placed.
    BackendAccepted {
        /// Venue order ID verified against the durable nonce.
        venue_order_id: u64,
    },
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
    /// A matching finalized `PerpMarket.OrderCancelled` proves order cancellation.
    Canceled {
        /// Venue order ID proved by the matching cancellation event.
        venue_order_id: u64,
        /// Canonical finalized inclusion evidence for the cancellation.
        inclusion: DeepXInclusionEvidence,
    },
    /// Finalized dispatch or cancellation-event failure proves the cancel command failed.
    CancelRejected {
        /// Canonical finalized inclusion evidence for the failed cancellation.
        inclusion: DeepXInclusionEvidence,
    },
    /// Finalized transactional replacement proves the order update.
    Updated {
        /// Replacement venue order ID derived from the modify nonce.
        venue_order_id: u64,
        /// Canonical finalized inclusion evidence for the modify.
        inclusion: DeepXInclusionEvidence,
    },
    /// Finalized dispatch failure proves the modification failed.
    ModifyRejected {
        /// Canonical finalized inclusion evidence for the failed modify.
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
    submitted_payload: DeepXFrameworkOrderEventPayload,
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

    /// Returns the durable event type corresponding to the submission boundary.
    #[must_use]
    pub const fn submitted_payload(&self) -> DeepXFrameworkOrderEventPayload {
        self.submitted_payload
    }

    /// Returns verified evidence for a terminal order event, when available.
    #[must_use]
    pub const fn terminal(&self) -> Option<DeepXFrameworkOrderTerminalEvidence> {
        self.terminal
    }
}

/// Failures while classifying durable placement or cancellation evidence for framework events.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum DeepXFrameworkOrderEventEligibilityError {
    /// The durable transaction is not a supported placement or cancellation.
    #[error("DeepX framework order-event classification requires a supported order operation")]
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

/// Classifies framework order-event eligibility from a Perp or Spot placement, or cancellation.
///
/// Submission-pool acceptance proves only that transmission was acknowledged. Best-block
/// inclusion remains reorganization-sensitive. Placement eligibility can also follow a
/// hash-bound backend order record; other terminal outcomes require finalized business evidence.
/// Ambiguous and not-included outcomes never become terminal events.
///
/// This pure boundary does not construct, persist, deduplicate, or emit framework events.
///
/// # Errors
///
/// Returns an error unless the record describes a framework-owned, timestamp-identified placement
/// or Perp/Spot cancellation with an unambiguous lifecycle.
pub fn classify_perp_place_framework_order_events(
    record: &DeepXTransactionRecord,
) -> Result<DeepXFrameworkOrderEventEligibility, DeepXFrameworkOrderEventEligibilityError> {
    let is_place = matches!(
        record.identity().operation(),
        Some(
            DeepXTransactionOperation::PerpPlace { .. }
                | DeepXTransactionOperation::SpotPlace { .. }
        )
    );
    let is_modify = matches!(
        record.identity().operation(),
        Some(DeepXTransactionOperation::PerpModify { .. })
    );
    let cancellation_order_id = match record.identity().operation() {
        Some(DeepXTransactionOperation::PerpCancel { order_id, .. }) => Some(*order_id),
        Some(DeepXTransactionOperation::SpotCancel {
            order_id,
            fast_cancel: false,
            ..
        }) => Some(*order_id),
        Some(DeepXTransactionOperation::SpotCancel {
            fast_cancel: true, ..
        }) => return Err(DeepXFrameworkOrderEventEligibilityError::UnsupportedOperation),
        _ => None,
    };
    if !is_place && !is_modify && cancellation_order_id.is_none() {
        return Err(DeepXFrameworkOrderEventEligibilityError::UnsupportedOperation);
    }
    let DeepXNonceReservation::TimestampOrderId {
        value: place_order_id,
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
            | DeepXTransactionState::BackendConfirmed
            | DeepXTransactionState::NotIncluded
    );
    let terminal = if state == DeepXTransactionState::BackendConfirmed {
        record.lifecycle().backend_order().map(|e| {
            DeepXFrameworkOrderTerminalEvidence::BackendAccepted {
                venue_order_id: e.order_id,
            }
        })
    } else if state == DeepXTransactionState::Finalized {
        let inclusion = record
            .lifecycle()
            .inclusion()
            .ok_or(DeepXFrameworkOrderEventEligibilityError::MissingFinalizedInclusion)?;
        Some(
            match (is_place, cancellation_order_id, inclusion.outcome()) {
                (true, _, DeepXInclusionOutcome::Success) => {
                    DeepXFrameworkOrderTerminalEvidence::Accepted {
                        venue_order_id: place_order_id,
                        inclusion,
                    }
                }
                (true, _, DeepXInclusionOutcome::Failed) => {
                    DeepXFrameworkOrderTerminalEvidence::Rejected { inclusion }
                }
                (false, Some(order_id), DeepXInclusionOutcome::Success) => {
                    DeepXFrameworkOrderTerminalEvidence::Canceled {
                        venue_order_id: order_id,
                        inclusion,
                    }
                }
                (false, Some(_), DeepXInclusionOutcome::Failed) => {
                    DeepXFrameworkOrderTerminalEvidence::CancelRejected { inclusion }
                }
                (false, None, DeepXInclusionOutcome::Success) if is_modify => {
                    DeepXFrameworkOrderTerminalEvidence::Updated {
                        venue_order_id: place_order_id,
                        inclusion,
                    }
                }
                (false, None, DeepXInclusionOutcome::Failed) if is_modify => {
                    DeepXFrameworkOrderTerminalEvidence::ModifyRejected { inclusion }
                }
                _ => unreachable!("validated operation has a supported terminal result"),
            },
        )
    } else {
        None
    };

    Ok(DeepXFrameworkOrderEventEligibility {
        context,
        submitted,
        submitted_payload: if is_place {
            DeepXFrameworkOrderEventPayload::Submitted
        } else if is_modify {
            DeepXFrameworkOrderEventPayload::PendingUpdate
        } else {
            DeepXFrameworkOrderEventPayload::PendingCancel
        },
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
/// violate its framework context and lifecycle invariants. Supported records are Perp and Spot
/// placements and Perp cancellations.
pub fn stage_perp_framework_order_events(
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
            eligibility.submitted_payload(),
        ));
    }
    if let Some(terminal) = eligibility.terminal() {
        let payload = match terminal {
            DeepXFrameworkOrderTerminalEvidence::BackendAccepted { venue_order_id } => {
                DeepXFrameworkOrderEventPayload::Accepted { venue_order_id }
            }
            DeepXFrameworkOrderTerminalEvidence::Accepted { venue_order_id, .. } => {
                DeepXFrameworkOrderEventPayload::Accepted { venue_order_id }
            }
            DeepXFrameworkOrderTerminalEvidence::Rejected { .. } => {
                DeepXFrameworkOrderEventPayload::Rejected
            }
            DeepXFrameworkOrderTerminalEvidence::Canceled { venue_order_id, .. } => {
                DeepXFrameworkOrderEventPayload::Canceled { venue_order_id }
            }
            DeepXFrameworkOrderTerminalEvidence::CancelRejected { .. } => {
                DeepXFrameworkOrderEventPayload::CancelRejected
            }
            DeepXFrameworkOrderTerminalEvidence::Updated { venue_order_id, .. } => {
                DeepXFrameworkOrderEventPayload::Updated { venue_order_id }
            }
            DeepXFrameworkOrderTerminalEvidence::ModifyRejected { .. } => {
                DeepXFrameworkOrderEventPayload::ModifyRejected
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
pub fn materialize_perp_framework_order_events(
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
        let event = match stage.payload() {
            DeepXFrameworkOrderEventPayload::Submitted => {
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
                OrderEventAny::Submitted(event)
            }
            DeepXFrameworkOrderEventPayload::PendingCancel => {
                let order_id = match record.identity().operation() {
                    Some(
                        DeepXTransactionOperation::PerpCancel { order_id, .. }
                        | DeepXTransactionOperation::SpotCancel { order_id, .. },
                    ) => *order_id,
                    _ => {
                        unreachable!("durable validation rejects cancel event on another operation")
                    }
                };
                let mut event = OrderPendingCancel::new(
                    context.trader_id(),
                    context.strategy_id(),
                    context.instrument_id(),
                    context.client_order_id(),
                    Some(context.account_id()),
                    stage.event_id(),
                    stage.ts_event(),
                    stage.ts_init(),
                    stage.reconciliation(),
                    Some(VenueOrderId::new(order_id.to_string())),
                );
                event.causation_id = Some(context.command_id());
                OrderEventAny::PendingCancel(event)
            }
            DeepXFrameworkOrderEventPayload::PendingUpdate => {
                let order_id = context
                    .modify_venue_order_id()
                    .ok_or(DeepXFrameworkOrderEventMaterializationError::MissingFrameworkContext)?;
                let mut event = OrderPendingUpdate::new(
                    context.trader_id(),
                    context.strategy_id(),
                    context.instrument_id(),
                    context.client_order_id(),
                    Some(context.account_id()),
                    stage.event_id(),
                    stage.ts_event(),
                    stage.ts_init(),
                    stage.reconciliation(),
                    Some(VenueOrderId::new(order_id.to_string())),
                );
                event.causation_id = Some(context.command_id());
                OrderEventAny::PendingUpdate(event)
            }
            _ => unreachable!("durable validation rejects terminal payload in submitted slot"),
        };
        events.push(event);
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
            DeepXFrameworkOrderEventPayload::Canceled { venue_order_id } => {
                let mut event = OrderCanceled::new(
                    context.trader_id(),
                    context.strategy_id(),
                    context.instrument_id(),
                    context.client_order_id(),
                    stage.event_id(),
                    stage.ts_event(),
                    stage.ts_init(),
                    stage.reconciliation(),
                    Some(VenueOrderId::new(venue_order_id.to_string())),
                    Some(context.account_id()),
                );
                event.causation_id = Some(context.command_id());
                OrderEventAny::Canceled(event)
            }
            DeepXFrameworkOrderEventPayload::CancelRejected => {
                let order_id = match record.identity().operation() {
                    Some(
                        DeepXTransactionOperation::PerpCancel { order_id, .. }
                        | DeepXTransactionOperation::SpotCancel { order_id, .. },
                    ) => *order_id,
                    _ => {
                        unreachable!("durable validation rejects cancel event on another operation")
                    }
                };
                let mut event = OrderCancelRejected::new(
                    context.trader_id(),
                    context.strategy_id(),
                    context.instrument_id(),
                    context.client_order_id(),
                    FINALIZED_CANCEL_REJECTION_REASON.into(),
                    stage.event_id(),
                    stage.ts_event(),
                    stage.ts_init(),
                    stage.reconciliation(),
                    Some(VenueOrderId::new(order_id.to_string())),
                    Some(context.account_id()),
                );
                event.causation_id = Some(context.command_id());
                OrderEventAny::CancelRejected(event)
            }
            DeepXFrameworkOrderEventPayload::Updated { venue_order_id } => {
                let quantity = context
                    .modify_quantity()
                    .ok_or(DeepXFrameworkOrderEventMaterializationError::MissingFrameworkContext)?;
                let price = context
                    .modify_price()
                    .ok_or(DeepXFrameworkOrderEventMaterializationError::MissingFrameworkContext)?;
                let mut event = OrderUpdated::new(
                    context.trader_id(),
                    context.strategy_id(),
                    context.instrument_id(),
                    context.client_order_id(),
                    quantity,
                    stage.event_id(),
                    stage.ts_event(),
                    stage.ts_init(),
                    stage.reconciliation(),
                    Some(VenueOrderId::new(venue_order_id.to_string())),
                    Some(context.account_id()),
                    Some(price),
                    None,
                    None,
                    false,
                );
                event.causation_id = Some(context.command_id());
                OrderEventAny::Updated(event)
            }
            DeepXFrameworkOrderEventPayload::ModifyRejected => {
                let order_id = context
                    .modify_venue_order_id()
                    .ok_or(DeepXFrameworkOrderEventMaterializationError::MissingFrameworkContext)?;
                let mut event = OrderModifyRejected::new(
                    context.trader_id(),
                    context.strategy_id(),
                    context.instrument_id(),
                    context.client_order_id(),
                    FINALIZED_MODIFY_REJECTION_REASON.into(),
                    stage.event_id(),
                    stage.ts_event(),
                    stage.ts_init(),
                    stage.reconciliation(),
                    Some(VenueOrderId::new(order_id.to_string())),
                    Some(context.account_id()),
                );
                event.causation_id = Some(context.command_id());
                OrderEventAny::ModifyRejected(event)
            }
            DeepXFrameworkOrderEventPayload::Submitted => {
                unreachable!("durable outbox validation rejects submitted terminal payload")
            }
            DeepXFrameworkOrderEventPayload::PendingCancel => {
                unreachable!("durable outbox validation rejects pending cancel terminal payload")
            }
            DeepXFrameworkOrderEventPayload::PendingUpdate => {
                unreachable!("durable outbox validation rejects pending update terminal payload")
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

    fn cancel_record() -> DeepXTransactionRecord {
        let runtime = ApprovedRuntimeIdentity {
            environment: DeepXEnvironment::Testnet,
            genesis_hash: [1; 32],
            metadata_sha256: [2; 32],
            spec_version: 369,
            transaction_version: 1,
            signed_extensions: vec!["CheckNonce".to_string()],
        };
        let identity = DeepXTransactionIdentity::new_perp_cancel(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            [7; 20],
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId {
                value: ORDER_ID + 1,
            },
            DeepXDirectRuntimeIdentity::from(&runtime),
            crate::signing::DeepXPerpCancelParams {
                subaccount: [7; 20],
                order_id: 42,
                market_id: 3,
                fast_cancel: false,
            },
        );
        DeepXTransactionRecord::created_with_framework_order_context(
            identity,
            DeepXSubmissionScanCheckpoint::new(100, [3; 32]),
            framework_context(),
        )
    }

    fn spot_cancel_record() -> DeepXTransactionRecord {
        let runtime = ApprovedRuntimeIdentity {
            environment: DeepXEnvironment::Testnet,
            genesis_hash: [1; 32],
            metadata_sha256: [2; 32],
            spec_version: 369,
            transaction_version: 1,
            signed_extensions: vec!["CheckNonce".to_string()],
        };
        let instrument_id = InstrumentId::from_as_ref("ETH-USDC.DEEPX").unwrap();
        let context = DeepXFrameworkOrderContext::new(
            nautilus_model::identifiers::TraderId::from("TRADER-001"),
            nautilus_model::identifiers::StrategyId::from("S-DEEPX-001"),
            instrument_id,
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
        );
        let identity = DeepXTransactionIdentity::new_spot_cancel(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            [7; 20],
            instrument_id,
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId {
                value: ORDER_ID + 1,
            },
            DeepXDirectRuntimeIdentity::from(&runtime),
            crate::signing::DeepXSpotCancelParams {
                subaccount: [7; 20],
                pair: [8; 32],
                order_id: 42,
                is_buy: true,
                fast_cancel: false,
            },
        );
        DeepXTransactionRecord::created_with_framework_order_context(
            identity,
            DeepXSubmissionScanCheckpoint::new(100, [3; 32]),
            context,
        )
    }

    fn submitted_cancel_record() -> DeepXTransactionRecord {
        let mut record = cancel_record();
        let identity = record.identity();
        let runtime = identity.runtime();
        let bytes = vec![4, 5, 6];
        record
            .record_signed(&SignedPalletExtrinsic {
                extrinsic_hash: BlakeTwo256.hash(&bytes).0,
                bytes,
                signer: identity.signer(),
                nonce: ORDER_ID + 1,
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
        stage_perp_framework_order_events(
            &mut record,
            UnixNanos::from(100),
            UnixNanos::from(101),
            true,
        )
        .unwrap();
        record
    }

    #[rstest]
    fn backend_confirmed_place_stages_durable_acceptance_without_inclusion() {
        let mut record = submitted_record();
        let evidence = super::super::DeepXBackendOrderEvidence {
            extrinsic_hash: record.lifecycle().extrinsic_hash().unwrap(),
            order_id: ORDER_ID,
            observed_block_number: 101,
        };
        record
            .apply_observation(DeepXTransactionObservation::BackendOrderConfirmed(evidence))
            .unwrap();
        assert_eq!(record.lifecycle().inclusion(), None);
        assert_eq!(
            record.recovery_action(),
            super::super::DeepXTransactionRecoveryAction::Complete
        );
        assert!(
            stage_perp_framework_order_events(
                &mut record,
                UnixNanos::from(100),
                UnixNanos::from(101),
                true,
            )
            .unwrap()
        );
        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
        assert_eq!(restored.lifecycle().backend_order(), Some(evidence));
        assert_eq!(
            restored
                .framework_order_outbox()
                .unwrap()
                .terminal()
                .unwrap()
                .payload(),
            DeepXFrameworkOrderEventPayload::Accepted {
                venue_order_id: ORDER_ID
            },
        );
    }

    #[rstest]
    fn backend_order_with_wrong_nonce_cannot_change_durable_record() {
        let mut record = submitted_record();
        let original = record.clone();
        let result = record.apply_observation(DeepXTransactionObservation::BackendOrderConfirmed(
            super::super::DeepXBackendOrderEvidence {
                extrinsic_hash: record.lifecycle().extrinsic_hash().unwrap(),
                order_id: ORDER_ID + 1,
                observed_block_number: 101,
            },
        ));
        assert!(matches!(
            result,
            Err(DeepXTransactionRecordError::InconsistentLifecycle)
        ));
        assert_eq!(record, original);
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
            materialize_perp_framework_order_events(&generic),
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
    fn finalized_perp_cancel_stages_pending_and_authoritative_terminal_event() {
        for (outcome, expected_payload) in [
            (
                DeepXInclusionOutcome::Success,
                DeepXFrameworkOrderEventPayload::Canceled { venue_order_id: 42 },
            ),
            (
                DeepXInclusionOutcome::Failed,
                DeepXFrameworkOrderEventPayload::CancelRejected,
            ),
        ] {
            let mut record = submitted_cancel_record();
            let inclusion = inclusion(outcome);
            record
                .apply_observation(DeepXTransactionObservation::Included(inclusion))
                .unwrap();
            record
                .apply_observation(DeepXTransactionObservation::Finalized(inclusion))
                .unwrap();

            assert!(
                stage_perp_framework_order_events(
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
                DeepXFrameworkOrderEventPayload::PendingCancel,
            );
            assert_eq!(outbox.terminal().unwrap().payload(), expected_payload);
            let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
            assert_eq!(restored.framework_order_outbox(), Some(outbox));
            let events = materialize_perp_framework_order_events(&restored).unwrap();
            assert!(
                matches!(events.first(), Some(OrderEventAny::PendingCancel(event))
                if event.venue_order_id == Some(VenueOrderId::new("42"))
                    && event.causation_id == Some(framework_context().command_id()))
            );
            match outcome {
                DeepXInclusionOutcome::Success => assert!(matches!(
                    events.get(1),
                    Some(OrderEventAny::Canceled(event))
                        if event.venue_order_id == Some(VenueOrderId::new("42"))
                            && event.causation_id == Some(framework_context().command_id())
                )),
                DeepXInclusionOutcome::Failed => assert!(matches!(
                    events.get(1),
                    Some(OrderEventAny::CancelRejected(event))
                        if event.venue_order_id == Some(VenueOrderId::new("42"))
                            && event.causation_id == Some(framework_context().command_id())
                )),
            }
        }
    }

    #[test]
    fn finalized_spot_cancel_stages_pending_and_authoritative_terminal_event() {
        let mut record = spot_cancel_record();
        let identity = record.identity();
        let bytes = vec![7, 8, 9];
        record
            .record_signed(&SignedPalletExtrinsic {
                extrinsic_hash: BlakeTwo256.hash(&bytes).0,
                bytes,
                signer: identity.signer(),
                nonce: ORDER_ID + 1,
                runtime: ApprovedRuntimeIdentity {
                    environment: DeepXEnvironment::Testnet,
                    genesis_hash: identity.runtime().genesis_hash,
                    metadata_sha256: identity.runtime().metadata_sha256,
                    spec_version: identity.runtime().spec_version,
                    transaction_version: identity.runtime().transaction_version,
                    signed_extensions: identity.runtime().signed_extensions.clone(),
                },
            })
            .unwrap();
        record
            .apply_observation(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        let inclusion = inclusion(DeepXInclusionOutcome::Success);
        record
            .apply_observation(DeepXTransactionObservation::Included(inclusion))
            .unwrap();
        record
            .apply_observation(DeepXTransactionObservation::Finalized(inclusion))
            .unwrap();

        stage_perp_framework_order_events(
            &mut record,
            UnixNanos::from(300),
            UnixNanos::from(301),
            false,
        )
        .unwrap();
        let restored = DeepXTransactionRecord::decode(&record.encode().unwrap()).unwrap();
        let events = materialize_perp_framework_order_events(&restored).unwrap();
        assert!(matches!(
            events.first(),
            Some(OrderEventAny::PendingCancel(event))
                if event.venue_order_id == Some(VenueOrderId::new("42"))
        ));
        assert!(matches!(
            events.get(1),
            Some(OrderEventAny::Canceled(event))
                if event.venue_order_id == Some(VenueOrderId::new("42"))
        ));
    }

    #[rstest]
    fn staging_before_submission_is_a_noop() {
        let mut record = signed_record();

        assert!(
            !stage_perp_framework_order_events(
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
            stage_perp_framework_order_events(
                &mut record,
                UnixNanos::from(100),
                UnixNanos::from(101),
                false,
            )
            .unwrap()
        );
        assert!(
            !stage_perp_framework_order_events(
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

        let events = materialize_perp_framework_order_events(&restored).unwrap();
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
        stage_perp_framework_order_events(
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
            stage_perp_framework_order_events(
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

        let events = materialize_perp_framework_order_events(&restored).unwrap();
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
        let pending = materialize_perp_framework_order_events(&record).unwrap();
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
        let pending = materialize_perp_framework_order_events(&record).unwrap();
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
            materialize_perp_framework_order_events(&record)
                .unwrap()
                .is_empty()
        );
    }
}
