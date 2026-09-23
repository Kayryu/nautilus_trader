// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Message types for system communication.
//!
//! This module provides message types used for communication between different
//! parts of the NautilusTrader system, including data requests, execution commands,
//! and system control messages.

use nautilus_core::UUID4;
use nautilus_model::{
    data::{Data, FundingRateUpdate, InstrumentStatus, option_chain::OptionGreeks},
    events::{
        AccountState, OrderAcceptedBatch, OrderCanceledBatch, OrderEventAny, OrderSubmittedBatch,
    },
    instruments::InstrumentAny,
};
use strum::Display;

pub mod data;
pub mod execution;
pub mod system;

#[cfg(feature = "defi")]
pub mod defi;

// Re-exports
pub use data::{DataResponse, SubscribeCommand, UnsubscribeCommand};
pub use execution::ExecutionReport;

// TODO: Refine this to reduce disparity between enum sizes
#[allow(
    clippy::large_enum_variant,
    reason = "event enum keeps all data variants in one routing type"
)]
#[derive(Debug, Display)]
pub enum DataEvent {
    Response(DataResponse),
    Data(Data),
    // Kept separate from `Data` pending the decision on generic dispatch versus this routing enum
    Instrument(InstrumentAny),
    FundingRate(FundingRateUpdate),
    InstrumentStatus(InstrumentStatus),
    OptionGreeks(OptionGreeks),
    // nautilus-import-ok: conditional compilation import
    #[cfg(feature = "defi")]
    DeFi(nautilus_model::defi::data::DefiData),
}

/// System command variants routed to a live node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub enum SystemCommand {
    ReconnectSocket(system::ReconnectSocket),
}

/// System event variants routed to a live node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub enum SystemEvent {
    SocketState(system::SocketStateChange),
}

/// Result of applying an acknowledged order event to the canonical execution cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderEventApplicationStatus {
    /// The event was newly applied.
    Applied,
    /// The identical event was already present in canonical order history.
    AlreadyApplied,
    /// The event was not present after processing or its ID conflicted.
    Rejected,
    /// The routing path could not observe the execution engine result.
    Unconfirmed,
}

/// Persistence knowledge attached to an order-event consumer receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderEventPersistenceStatus {
    /// The cache database durably persisted the event.
    Persisted,
    /// The cache database rejected or failed to persist the event.
    Failed,
    /// No cache database backing is configured.
    NotConfigured,
    /// A cache database backing exists, but its asynchronous write is not confirmed.
    Unconfirmed,
    /// Persistence was not attempted because application was rejected or unobservable.
    NotApplicable,
}

/// Consumer receipt for one acknowledged order event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderEventConsumerReceipt {
    /// Durable event ID being acknowledged.
    pub event_id: UUID4,
    /// Canonical execution-engine application result.
    pub application: OrderEventApplicationStatus,
    /// Cache database persistence knowledge.
    pub persistence: OrderEventPersistenceStatus,
}

/// One-shot receiver for an order-event consumer receipt.
pub type OrderEventConsumerReceiptReceiver =
    futures::channel::oneshot::Receiver<OrderEventConsumerReceipt>;

/// Order event carrying a one-shot consumer receipt channel.
pub struct AcknowledgedOrderEvent {
    event: OrderEventAny,
    receipt_tx: futures::channel::oneshot::Sender<OrderEventConsumerReceipt>,
}

impl AcknowledgedOrderEvent {
    /// Creates an acknowledged order-event envelope.
    #[must_use]
    pub const fn new(
        event: OrderEventAny,
        receipt_tx: futures::channel::oneshot::Sender<OrderEventConsumerReceipt>,
    ) -> Self {
        Self { event, receipt_tx }
    }

    /// Creates an envelope and its one-shot receipt receiver.
    pub fn with_receipt_channel(event: OrderEventAny) -> (Self, OrderEventConsumerReceiptReceiver) {
        let (receipt_tx, receipt_rx) = futures::channel::oneshot::channel();
        (Self::new(event, receipt_tx), receipt_rx)
    }

    /// Returns the order event.
    #[must_use]
    pub const fn event(&self) -> &OrderEventAny {
        &self.event
    }

    /// Consumes the envelope into its event and receipt sender.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        OrderEventAny,
        futures::channel::oneshot::Sender<OrderEventConsumerReceipt>,
    ) {
        (self.event, self.receipt_tx)
    }
}

impl std::fmt::Debug for AcknowledgedOrderEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AcknowledgedOrderEvent))
            .field("event", &self.event)
            .finish_non_exhaustive()
    }
}

/// Execution event variants for order events and reports.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ExecutionEvent {
    Order(OrderEventAny),
    AcknowledgedOrder(AcknowledgedOrderEvent),
    OrderSubmittedBatch(OrderSubmittedBatch),
    OrderAcceptedBatch(OrderAcceptedBatch),
    OrderCanceledBatch(OrderCanceledBatch),
    Report(ExecutionReport),
    Account(AccountState),
}

impl std::fmt::Display for ExecutionEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Order(_) => "Order",
            Self::AcknowledgedOrder(_) => "AcknowledgedOrder",
            Self::OrderSubmittedBatch(_) => "OrderSubmittedBatch",
            Self::OrderAcceptedBatch(_) => "OrderAcceptedBatch",
            Self::OrderCanceledBatch(_) => "OrderCanceledBatch",
            Self::Report(_) => "Report",
            Self::Account(_) => "Account",
        })
    }
}
