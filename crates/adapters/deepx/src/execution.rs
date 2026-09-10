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

//! Fail-closed DeepX execution client startup boundary.

use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use async_trait::async_trait;
use nautilus_common::{
    cache::fifo::{FifoCache, FifoCacheMap},
    clients::ExecutionClient,
    live::runner::get_exec_event_sender,
    log_error,
    messages::execution::{
        BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{Params, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter, execution::context::OrderContext};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, LiquiditySide, OmsType},
    events::AccountState,
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TradeId, Venue, VenueOrderId,
    },
    instruments::InstrumentAny,
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Money, Price, Quantity},
};
use thiserror::Error;

use crate::{
    common::{DeepXEnvironment, DeepXPrivateKey, consts::DEEPX_VENUE},
    config::{
        DeepXExecutionBackend, DeepXExecutionClientConfig, DeepXRpcRole, DeepXValidatedRpcEndpoints,
    },
    providers::DeepXMarketProvider,
    rpc::{DeepXAppliedRuntimeSnapshot, DeepXValidatedRpcMethodCapabilities},
    signing::{SigningError, derive_signer_account_id},
    transaction::{
        DeepXFinalityCommitError, DeepXFinalizedRecoveryCommitError,
        DeepXPoolReconciliationCommitError, DeepXReorganizationCommitError,
        DeepXRestoredTransactionRecord, DeepXSignerLease, DeepXTimestampNonceAllocator,
        DeepXTransactionPersistenceError, DeepXTransactionRecoveryAction, DeepXTransactionState,
        DeepXTransactionStore, DeepXTransactionWatchError, load_verified_committed_for_signer,
        observe_and_commit_finality, observe_and_commit_reorganization,
        reconcile_not_included_checkpoint, reconcile_submission_pool,
        restore_timestamp_nonce_allocator,
    },
    websocket::{DeepXWsAuthenticatedFrame, DeepXWsAuthenticatedSession, DeepXWsProtocolCore},
};

const TRADE_DEDUP_CAPACITY: usize = 10_000;
const TERMINAL_CONTEXT_CAPACITY: usize = 10_000;

/// Ordered evidence required before a DeepX execution client can become connected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXExecutionStartupEvidence {
    /// All Spot and perpetual instruments completed a failure-atomic preload.
    InstrumentsLoaded,
    /// Durable order identity and transaction context completed restoration.
    OrderContextRestored,
    /// Signer, subaccount, backend, RPC roles, and runtime snapshot were validated.
    RuntimeValidated,
    /// The private stream authenticated for the current connection epoch.
    PrivateStreamAuthenticated,
    /// The verified account-state event for this startup epoch was received.
    AccountStateInitialized,
    /// Startup mass reconciliation completed from authoritative evidence.
    MassReconciliationCompleted,
    /// The current startup account-state event is present in the matching cached account history.
    AccountRegistered,
}

/// Errors raised by the DeepX execution startup boundary.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXExecutionStartupError {
    /// Startup evidence was supplied out of order.
    #[error("expected DeepX startup evidence {expected:?}, received {received:?}")]
    OutOfOrder {
        /// Evidence required at the current startup step.
        expected: DeepXExecutionStartupEvidence,
        /// Evidence supplied by the caller.
        received: DeepXExecutionStartupEvidence,
    },
    /// Startup evidence was supplied after readiness had already been reached.
    #[error("DeepX execution startup is already complete")]
    AlreadyComplete,
    /// The public market catalog has not completed a failure-atomic load.
    #[error("DeepX market catalog is not initialized")]
    MarketCatalogNotInitialized,
    /// The initialized public market catalog contains no markets.
    #[error("DeepX market catalog is empty")]
    MarketCatalogEmpty,
    /// The public market catalog was loaded from an endpoint outside this execution configuration.
    #[error("DeepX market catalog endpoint does not match execution configuration")]
    MarketCatalogEndpointMismatch,
    /// The applied runtime snapshot belongs to another deployment environment.
    #[error("DeepX runtime deployment mismatch: expected {expected}, received {received}")]
    RuntimeEnvironmentMismatch {
        /// Configured execution deployment.
        expected: DeepXEnvironment,
        /// Deployment associated with the applied runtime fixture.
        received: DeepXEnvironment,
    },
    /// The applied runtime and validated RPC roles do not identify the same chain.
    #[error("DeepX applied runtime genesis hash does not match validated RPC endpoints")]
    RuntimeGenesisMismatch,
    /// The configured transaction backend has no fixture-approved runtime interface.
    #[error("DeepX runtime validation does not support execution backend {0:?}")]
    UnsupportedRuntimeBackend(DeepXExecutionBackend),
    /// The validated RPC role selection belongs to another network configuration.
    #[error("DeepX validated RPC endpoint does not match execution configuration for role {0:?}")]
    RuntimeRpcEndpointMismatch(DeepXRpcRole),
    /// RPC method capability evidence belongs to another validated role endpoint.
    #[error("DeepX RPC method capabilities do not match validated endpoint for role {0:?}")]
    RuntimeRpcCapabilitiesMismatch(DeepXRpcRole),
    /// The private-stream authentication receipt is not current for its protocol owner.
    #[error("DeepX private-stream authenticated session is not current")]
    PrivateStreamAuthenticationMismatch,
    /// Account-state initialization was not recorded through the event identity boundary.
    #[error("DeepX account-state initialization evidence requires event verification")]
    AccountStateVerificationRequired,
    /// The verified account-state event could not be dispatched to the execution engine.
    #[error("DeepX account-state event dispatch failed: {0}")]
    AccountStateDispatchFailed(String),
    /// The observed account state does not match the configured execution account.
    #[error(
        "DeepX account state identity mismatch: expected {expected_account_id} ({expected_account_type:?}), received {received_account_id} ({received_account_type:?})"
    )]
    AccountStateIdentityMismatch {
        /// Configured execution account ID.
        expected_account_id: AccountId,
        /// Configured execution account type.
        expected_account_type: AccountType,
        /// Observed account-state ID.
        received_account_id: AccountId,
        /// Observed account-state type.
        received_account_type: AccountType,
    },
    /// The current startup account-state event is absent from the shared execution cache.
    #[error(
        "DeepX account {account_id} state event {event_id} is not registered in the execution cache"
    )]
    AccountStateNotRegistered {
        /// Configured execution account ID.
        account_id: AccountId,
        /// Current startup account-state event ID.
        event_id: UUID4,
    },
    /// The shared execution cache is temporarily unavailable for verification.
    #[error("DeepX execution cache is already mutably borrowed")]
    CacheBorrowConflict,
}

/// Errors raised when DeepX order context cannot be registered or read safely.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXOrderContextError {
    /// The order context belongs to another venue.
    #[error("DeepX order context {client_order_id} has instrument venue {venue}")]
    InstrumentVenueMismatch {
        /// Client order ID from the rejected context.
        client_order_id: ClientOrderId,
        /// Instrument venue from the rejected context.
        venue: Venue,
    },
    /// A client order ID was already bound to different immutable order terms.
    #[error("DeepX client order ID {0} is already registered with different order context")]
    Conflict(ClientOrderId),
    /// A client order ID cannot be both locally tracked and externally owned.
    #[error("DeepX client order ID {0} has conflicting tracked and external ownership")]
    OwnershipConflict(ClientOrderId),
    /// A terminal transition was requested for an order without active or terminal ownership.
    #[error("DeepX client order ID {0} has no registered order context")]
    ContextNotFound(ClientOrderId),
    /// An external client order ID was already bound to different identity metadata.
    #[error("DeepX external client order ID {0} is already registered with different context")]
    ExternalClientConflict(ClientOrderId),
    /// An external venue order ID was already bound to a different client order ID.
    #[error("DeepX external venue order ID {0} is already registered to another client order ID")]
    VenueOrderOwnershipConflict(VenueOrderId),
    /// A tracked client order ID was already bound to a different venue order ID.
    #[error(
        "DeepX client order ID {client_order_id} is already bound to venue order ID {venue_order_id}"
    )]
    VenueOrderBindingConflict {
        /// Client order ID owning the existing binding.
        client_order_id: ClientOrderId,
        /// Existing venue order ID bound to the client order.
        venue_order_id: VenueOrderId,
    },
    /// Client and venue order IDs resolve to different registered ownership.
    #[error(
        "DeepX execution update identity conflict for client order ID {client_order_id} and venue order ID {venue_order_id}"
    )]
    UpdateIdentityConflict {
        /// Client order ID from the conflicting update.
        client_order_id: ClientOrderId,
        /// Venue order ID from the conflicting update.
        venue_order_id: VenueOrderId,
    },
    /// Another thread panicked while holding the order-context registry lock.
    #[error("DeepX order-context registry lock is poisoned")]
    LockPoisoned,
}

/// Errors raised when DeepX trade replay state cannot be accessed safely.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXTradeDedupError {
    /// Another thread panicked while holding the trade replay-state lock.
    #[error("DeepX trade deduplication lock is poisoned")]
    LockPoisoned,
}

/// Errors raised while merging already validated DeepX fill reports.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXFillReportMergeError {
    /// A fill report belongs to another execution account.
    #[error(
        "DeepX fill report account mismatch: expected {expected}, received {received} for trade ID {trade_id}"
    )]
    AccountMismatch {
        /// Configured execution account ID.
        expected: AccountId,
        /// Account ID carried by the fill report.
        received: AccountId,
        /// Venue trade ID carried by the fill report.
        trade_id: TradeId,
    },
    /// A fill report belongs to another venue.
    #[error("DeepX fill report trade ID {trade_id} has instrument venue {venue}")]
    InstrumentVenueMismatch {
        /// Venue trade ID carried by the fill report.
        trade_id: TradeId,
        /// Instrument venue carried by the fill report.
        venue: Venue,
    },
    /// One venue trade ID was associated with conflicting execution evidence.
    #[error("DeepX trade ID {0} has conflicting fill report evidence")]
    ConflictingTrade(TradeId),
}

/// Errors raised while merging already validated DeepX order status reports.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXOrderReportMergeError {
    /// An order report belongs to another execution account.
    #[error(
        "DeepX order report account mismatch: expected {expected}, received {received} for venue order ID {venue_order_id}"
    )]
    AccountMismatch {
        /// Configured execution account ID.
        expected: AccountId,
        /// Account ID carried by the order report.
        received: AccountId,
        /// Venue order ID carried by the order report.
        venue_order_id: VenueOrderId,
    },
    /// An order report belongs to another venue.
    #[error("DeepX order report venue order ID {venue_order_id} has instrument venue {venue}")]
    InstrumentVenueMismatch {
        /// Venue order ID carried by the order report.
        venue_order_id: VenueOrderId,
        /// Instrument venue carried by the order report.
        venue: Venue,
    },
    /// One client order ID was associated with multiple venue order IDs.
    #[error(
        "DeepX client order ID {client_order_id} has conflicting venue order IDs {first_venue_order_id} and {second_venue_order_id}"
    )]
    ClientOrderIdentitySplit {
        /// Client order ID carried by both reports.
        client_order_id: ClientOrderId,
        /// First venue order ID in deterministic identity order.
        first_venue_order_id: VenueOrderId,
        /// Second venue order ID in deterministic identity order.
        second_venue_order_id: VenueOrderId,
    },
    /// One venue order ID was associated with conflicting order evidence.
    #[error("DeepX venue order ID {0} has conflicting order report evidence")]
    ConflictingOrder(VenueOrderId),
}

/// Errors raised while restoring the complete startup order-context set.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXOrderContextRestorationError {
    /// Startup was not waiting for order-context restoration.
    #[error(transparent)]
    Startup(#[from] DeepXExecutionStartupError),
    /// The shared execution cache is temporarily unavailable for restoration.
    #[error("DeepX execution cache is already mutably borrowed")]
    CacheBorrowConflict,
    /// The replacement context snapshot could not be committed without conflict.
    #[error(transparent)]
    Registry(#[from] DeepXOrderContextError),
}

/// Errors raised while proving startup mass reconciliation is complete.
#[derive(Debug, Error)]
pub enum DeepXMassReconciliationError {
    /// Startup was not waiting for mass reconciliation.
    #[error(transparent)]
    Startup(#[from] DeepXExecutionStartupError),
    /// The configured signing identity could not be derived.
    #[error(transparent)]
    Signing(#[from] SigningError),
    /// The supplied signer lease belongs to another signing identity.
    #[error("DeepX transaction store lease does not match the configured signing identity")]
    SignerLeaseMismatch,
    /// A durable transaction belongs to another chain.
    #[error(
        "DeepX transaction {client_order_id} genesis does not match the validated RPC endpoints: expected {expected_genesis_hash:?}, received {received_genesis_hash:?}"
    )]
    RuntimeGenesisMismatch {
        /// Client order ID owning the mismatched durable transaction.
        client_order_id: String,
        /// Genesis hash observed from every validated RPC role.
        expected_genesis_hash: [u8; 32],
        /// Genesis hash persisted with the transaction before signing.
        received_genesis_hash: [u8; 32],
    },
    /// Complete durable transaction evidence could not be verified.
    #[error(transparent)]
    Persistence(#[from] DeepXTransactionPersistenceError),
    /// An in-block transaction could not be reconciled against finalized chain evidence.
    #[error(transparent)]
    Finality(#[from] DeepXFinalityCommitError),
    /// A submitting transaction could not be reconciled against the submission-node pool.
    #[error(transparent)]
    PoolReconciliation(#[from] DeepXPoolReconciliationCommitError),
    /// An in-block transaction could not be reconciled against canonical chain evidence.
    #[error(transparent)]
    Reorganization(#[from] DeepXReorganizationCommitError),
    /// A not-included transaction could not be reconciled from its finalized checkpoint.
    #[error(transparent)]
    FinalizedRecovery(#[from] DeepXFinalizedRecoveryCommitError),
    /// A durable transaction still requires recovery or operator action.
    #[error("DeepX transaction {client_order_id} still requires startup action {action:?}")]
    UnresolvedTransaction {
        /// Client order ID owning the unresolved transaction.
        client_order_id: String,
        /// Fail-closed action required before startup may continue.
        action: DeepXTransactionRecoveryAction,
    },
}

/// Errors raised while restoring the configured signer's timestamp nonce domain.
#[derive(Debug, Error)]
pub enum DeepXNonceRestorationError {
    /// The configured signing identity could not be derived.
    #[error(transparent)]
    Signing(#[from] SigningError),
    /// The supplied signer lease belongs to another signing identity.
    #[error("DeepX transaction store lease does not match the configured signing identity")]
    SignerLeaseMismatch,
    /// Complete durable transaction evidence could not be verified.
    #[error(transparent)]
    Persistence(#[from] DeepXTransactionPersistenceError),
}

/// Classification of an execution update against registered Nautilus order context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXExecutionUpdateRoute {
    /// The update belongs to an order tracked by this execution client.
    Tracked(OrderContext),
    /// The update belongs to an order whose tracked lifecycle is terminal.
    Terminal(OrderContext),
    /// The update belongs to an external order registered during reconciliation.
    RegisteredExternal(DeepXExternalOrderContext),
    /// The update has no registered Nautilus order context.
    External,
}

/// Framework-provided identity for a reconciled external DeepX order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXExternalOrderContext {
    /// Client order ID assigned during external order reconciliation.
    pub client_order_id: ClientOrderId,
    /// Venue order ID reported by DeepX.
    pub venue_order_id: VenueOrderId,
    /// Instrument associated with the external order.
    pub instrument_id: InstrumentId,
    /// Strategy which claimed the external order.
    pub strategy_id: StrategyId,
    /// Initialization timestamp assigned during reconciliation.
    pub ts_init: UnixNanos,
}

/// Complete tracked order identity restored before execution updates are dispatched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXRestoredOrderContext {
    /// Immutable Nautilus order context.
    pub context: OrderContext,
    /// Venue order ID already assigned to the order, when known.
    pub venue_order_id: Option<VenueOrderId>,
}

impl DeepXRestoredOrderContext {
    /// Creates a restored tracked order identity.
    #[must_use]
    pub const fn new(context: OrderContext, venue_order_id: Option<VenueOrderId>) -> Self {
        Self {
            context,
            venue_order_id,
        }
    }
}

type DeepXOrderContextRegistry = DeepXOrderContextRegistryInner<TERMINAL_CONTEXT_CAPACITY>;

#[derive(Debug, Default)]
struct DeepXOrderContextRegistryInner<const N: usize> {
    state: Mutex<DeepXOrderContextState<N>>,
}

#[derive(Debug)]
struct DeepXOrderContextState<const N: usize> {
    tracked: HashMap<ClientOrderId, OrderContext>,
    terminal: FifoCacheMap<ClientOrderId, OrderContext, N>,
    tracked_venue_by_client: HashMap<ClientOrderId, VenueOrderId>,
    tracked_client_by_venue: HashMap<VenueOrderId, ClientOrderId>,
    external_by_client: HashMap<ClientOrderId, DeepXExternalOrderContext>,
    external_client_by_venue: HashMap<VenueOrderId, ClientOrderId>,
}

impl<const N: usize> Default for DeepXOrderContextState<N> {
    fn default() -> Self {
        Self {
            tracked: HashMap::new(),
            terminal: FifoCacheMap::new(),
            tracked_venue_by_client: HashMap::new(),
            tracked_client_by_venue: HashMap::new(),
            external_by_client: HashMap::new(),
            external_client_by_venue: HashMap::new(),
        }
    }
}

impl<const N: usize> DeepXOrderContextState<N> {
    fn retain_owned_venue_bindings(&mut self) {
        let Self {
            tracked,
            terminal,
            tracked_venue_by_client,
            tracked_client_by_venue,
            ..
        } = self;
        tracked_venue_by_client.retain(|client_order_id, _| {
            tracked.contains_key(client_order_id) || terminal.contains_key(client_order_id)
        });
        tracked_client_by_venue.retain(|venue_order_id, client_order_id| {
            tracked_venue_by_client.get(client_order_id) == Some(venue_order_id)
        });
    }
}

#[derive(Debug)]
struct DeepXTradeDedupState<const N: usize> {
    committed: FifoCache<TradeId, N>,
    reserved: HashSet<TradeId>,
}

impl<const N: usize> Default for DeepXTradeDedupState<N> {
    fn default() -> Self {
        Self {
            committed: FifoCache::new(),
            reserved: HashSet::new(),
        }
    }
}

#[derive(Debug, Default)]
struct DeepXTradeDedup<const N: usize> {
    state: Mutex<DeepXTradeDedupState<N>>,
}

impl<const N: usize> DeepXTradeDedup<N> {
    fn reserve(
        &self,
        trade_id: TradeId,
    ) -> Result<Option<DeepXTradeReservation<'_, N>>, DeepXTradeDedupError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXTradeDedupError::LockPoisoned)?;
        if state.committed.contains(&trade_id) || !state.reserved.insert(trade_id) {
            return Ok(None);
        }
        Ok(Some(DeepXTradeReservation {
            dedup: self,
            trade_id,
            committed: false,
        }))
    }
}

#[derive(Debug)]
struct DeepXTradeReservation<'a, const N: usize> {
    dedup: &'a DeepXTradeDedup<N>,
    trade_id: TradeId,
    committed: bool,
}

impl<const N: usize> DeepXTradeReservation<'_, N> {
    #[allow(
        dead_code,
        reason = "reserved for the fixture-gated private fill dispatch path"
    )]
    fn commit(mut self) -> Result<(), DeepXTradeDedupError> {
        let mut state = self
            .dedup
            .state
            .lock()
            .map_err(|_| DeepXTradeDedupError::LockPoisoned)?;
        state.reserved.remove(&self.trade_id);
        state.committed.add(self.trade_id);
        self.committed = true;
        Ok(())
    }
}

impl<const N: usize> Drop for DeepXTradeReservation<'_, N> {
    fn drop(&mut self) {
        if !self.committed
            && let Ok(mut state) = self.dedup.state.lock()
        {
            state.reserved.remove(&self.trade_id);
        }
    }
}

impl<const N: usize> DeepXOrderContextRegistryInner<N> {
    fn register(&self, context: OrderContext) -> Result<(), DeepXOrderContextError> {
        let client_order_id = context.identity.client_order_id;
        if context.identity.instrument_id.venue != *DEEPX_VENUE {
            return Err(DeepXOrderContextError::InstrumentVenueMismatch {
                client_order_id,
                venue: context.identity.instrument_id.venue,
            });
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXOrderContextError::LockPoisoned)?;
        if state.external_by_client.contains_key(&client_order_id)
            || state.terminal.contains_key(&client_order_id)
        {
            return Err(DeepXOrderContextError::OwnershipConflict(client_order_id));
        }
        match state.tracked.get(&client_order_id) {
            Some(existing) if existing != &context => {
                Err(DeepXOrderContextError::Conflict(client_order_id))
            }
            Some(_) => Ok(()),
            None => {
                state.tracked.insert(client_order_id, context);
                Ok(())
            }
        }
    }

    fn restore(
        &self,
        contexts: impl IntoIterator<Item = OrderContext>,
    ) -> Result<(), DeepXOrderContextError> {
        let mut restored = HashMap::new();

        for context in contexts {
            let client_order_id = context.identity.client_order_id;
            if context.identity.instrument_id.venue != *DEEPX_VENUE {
                return Err(DeepXOrderContextError::InstrumentVenueMismatch {
                    client_order_id,
                    venue: context.identity.instrument_id.venue,
                });
            }
            if restored
                .get(&client_order_id)
                .is_some_and(|existing| existing != &context)
            {
                return Err(DeepXOrderContextError::Conflict(client_order_id));
            }
            restored.insert(client_order_id, context);
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXOrderContextError::LockPoisoned)?;
        if let Some(client_order_id) = restored.keys().find(|client_order_id| {
            state.external_by_client.contains_key(client_order_id)
                || state.terminal.contains_key(client_order_id)
        }) {
            return Err(DeepXOrderContextError::OwnershipConflict(*client_order_id));
        }
        state.tracked = restored;
        state.retain_owned_venue_bindings();
        Ok(())
    }

    fn restore_with_venue_ids(
        &self,
        contexts: impl IntoIterator<Item = DeepXRestoredOrderContext>,
    ) -> Result<(), DeepXOrderContextError> {
        let mut restored = HashMap::new();
        let mut restored_venue_by_client = HashMap::new();
        let mut restored_client_by_venue = HashMap::new();

        for restored_context in contexts {
            let context = restored_context.context;
            let client_order_id = context.identity.client_order_id;
            if context.identity.instrument_id.venue != *DEEPX_VENUE {
                return Err(DeepXOrderContextError::InstrumentVenueMismatch {
                    client_order_id,
                    venue: context.identity.instrument_id.venue,
                });
            }
            if restored
                .get(&client_order_id)
                .is_some_and(|existing| existing != &context)
            {
                return Err(DeepXOrderContextError::Conflict(client_order_id));
            }
            if let Some(venue_order_id) = restored_context.venue_order_id {
                if let Some(existing) = restored_venue_by_client.get(&client_order_id)
                    && existing != &venue_order_id
                {
                    return Err(DeepXOrderContextError::VenueOrderBindingConflict {
                        client_order_id,
                        venue_order_id: *existing,
                    });
                }
                if restored_client_by_venue
                    .get(&venue_order_id)
                    .is_some_and(|existing| existing != &client_order_id)
                {
                    return Err(DeepXOrderContextError::VenueOrderOwnershipConflict(
                        venue_order_id,
                    ));
                }
                restored_venue_by_client.insert(client_order_id, venue_order_id);
                restored_client_by_venue.insert(venue_order_id, client_order_id);
            }
            restored.insert(client_order_id, context);
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXOrderContextError::LockPoisoned)?;
        if let Some(client_order_id) = restored.keys().find(|client_order_id| {
            state.external_by_client.contains_key(client_order_id)
                || state.terminal.contains_key(client_order_id)
        }) {
            return Err(DeepXOrderContextError::OwnershipConflict(*client_order_id));
        }
        if let Some(venue_order_id) = restored_client_by_venue.keys().find(|venue_order_id| {
            state.external_client_by_venue.contains_key(venue_order_id)
                || state
                    .tracked_client_by_venue
                    .get(venue_order_id)
                    .is_some_and(|client_order_id| state.terminal.contains_key(client_order_id))
        }) {
            return Err(DeepXOrderContextError::VenueOrderOwnershipConflict(
                *venue_order_id,
            ));
        }
        state.tracked = restored;
        let DeepXOrderContextState {
            terminal,
            tracked_venue_by_client,
            tracked_client_by_venue,
            ..
        } = &mut *state;
        tracked_venue_by_client.retain(|client_order_id, _| terminal.contains_key(client_order_id));
        tracked_client_by_venue.retain(|_, client_order_id| terminal.contains_key(client_order_id));
        tracked_venue_by_client.extend(restored_venue_by_client);
        tracked_client_by_venue.extend(restored_client_by_venue);
        Ok(())
    }

    fn bind_tracked_venue_order_id(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> Result<(), DeepXOrderContextError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXOrderContextError::LockPoisoned)?;
        if !state.tracked.contains_key(&client_order_id)
            && !state.terminal.contains_key(&client_order_id)
        {
            return Err(DeepXOrderContextError::ContextNotFound(client_order_id));
        }
        if let Some(existing) = state.tracked_venue_by_client.get(&client_order_id) {
            return if existing == &venue_order_id {
                Ok(())
            } else {
                Err(DeepXOrderContextError::VenueOrderBindingConflict {
                    client_order_id,
                    venue_order_id: *existing,
                })
            };
        }
        if state
            .tracked_client_by_venue
            .get(&venue_order_id)
            .is_some_and(|existing| existing != &client_order_id)
            || state.external_client_by_venue.contains_key(&venue_order_id)
        {
            return Err(DeepXOrderContextError::VenueOrderOwnershipConflict(
                venue_order_id,
            ));
        }

        state
            .tracked_venue_by_client
            .insert(client_order_id, venue_order_id);
        state
            .tracked_client_by_venue
            .insert(venue_order_id, client_order_id);
        Ok(())
    }

    fn register_external(
        &self,
        context: DeepXExternalOrderContext,
    ) -> Result<(), DeepXOrderContextError> {
        if context.instrument_id.venue != *DEEPX_VENUE {
            return Err(DeepXOrderContextError::InstrumentVenueMismatch {
                client_order_id: context.client_order_id,
                venue: context.instrument_id.venue,
            });
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXOrderContextError::LockPoisoned)?;
        if state.tracked.contains_key(&context.client_order_id)
            || state.terminal.contains_key(&context.client_order_id)
        {
            return Err(DeepXOrderContextError::OwnershipConflict(
                context.client_order_id,
            ));
        }
        if let Some(existing) = state.external_by_client.get(&context.client_order_id) {
            return if existing == &context {
                Ok(())
            } else {
                Err(DeepXOrderContextError::ExternalClientConflict(
                    context.client_order_id,
                ))
            };
        }
        if state
            .external_client_by_venue
            .get(&context.venue_order_id)
            .is_some_and(|client_order_id| client_order_id != &context.client_order_id)
            || state
                .tracked_client_by_venue
                .contains_key(&context.venue_order_id)
        {
            return Err(DeepXOrderContextError::VenueOrderOwnershipConflict(
                context.venue_order_id,
            ));
        }

        state
            .external_client_by_venue
            .insert(context.venue_order_id, context.client_order_id);
        state
            .external_by_client
            .insert(context.client_order_id, context);
        Ok(())
    }

    fn external_by_client(
        &self,
        client_order_id: &ClientOrderId,
    ) -> Result<Option<DeepXExternalOrderContext>, DeepXOrderContextError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| DeepXOrderContextError::LockPoisoned)?
            .external_by_client
            .get(client_order_id)
            .copied())
    }

    fn external_by_venue(
        &self,
        venue_order_id: &VenueOrderId,
    ) -> Result<Option<DeepXExternalOrderContext>, DeepXOrderContextError> {
        let state = self
            .state
            .lock()
            .map_err(|_| DeepXOrderContextError::LockPoisoned)?;
        Ok(state
            .external_client_by_venue
            .get(venue_order_id)
            .and_then(|client_order_id| state.external_by_client.get(client_order_id))
            .copied())
    }

    fn route(
        &self,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
    ) -> Result<DeepXExecutionUpdateRoute, DeepXOrderContextError> {
        let state = self
            .state
            .lock()
            .map_err(|_| DeepXOrderContextError::LockPoisoned)?;
        let external_by_client = client_order_id
            .and_then(|client_order_id| state.external_by_client.get(&client_order_id));
        let external_by_venue = venue_order_id.and_then(|venue_order_id| {
            state
                .external_client_by_venue
                .get(&venue_order_id)
                .and_then(|client_order_id| state.external_by_client.get(client_order_id))
        });
        let tracked_client_by_venue = venue_order_id
            .and_then(|venue_order_id| state.tracked_client_by_venue.get(&venue_order_id));
        if let (Some(client_order_id), Some(venue_order_id)) = (client_order_id, venue_order_id)
            && (state
                .tracked_venue_by_client
                .get(&client_order_id)
                .is_some_and(|bound| bound != &venue_order_id)
                || tracked_client_by_venue.is_some_and(|bound| bound != &client_order_id)
                || external_by_client
                    .is_some_and(|context| context.venue_order_id != venue_order_id)
                || external_by_venue
                    .is_some_and(|context| context.client_order_id != client_order_id))
        {
            return Err(DeepXOrderContextError::UpdateIdentityConflict {
                client_order_id,
                venue_order_id,
            });
        }
        if let Some(client_order_id) = client_order_id {
            if let Some(context) = state.tracked.get(&client_order_id) {
                if let (Some(venue_order_id), Some(_)) = (venue_order_id, external_by_venue) {
                    return Err(DeepXOrderContextError::UpdateIdentityConflict {
                        client_order_id,
                        venue_order_id,
                    });
                }
                return Ok(DeepXExecutionUpdateRoute::Tracked(*context));
            }
            if let Some(context) = state.terminal.get(&client_order_id) {
                if let (Some(venue_order_id), Some(_)) = (venue_order_id, external_by_venue) {
                    return Err(DeepXOrderContextError::UpdateIdentityConflict {
                        client_order_id,
                        venue_order_id,
                    });
                }
                return Ok(DeepXExecutionUpdateRoute::Terminal(*context));
            }
        }
        if let Some(client_order_id) = tracked_client_by_venue {
            if let Some(context) = state.tracked.get(client_order_id) {
                return Ok(DeepXExecutionUpdateRoute::Tracked(*context));
            }
            if let Some(context) = state.terminal.get(client_order_id) {
                return Ok(DeepXExecutionUpdateRoute::Terminal(*context));
            }
        }
        Ok(external_by_client
            .or(external_by_venue)
            .copied()
            .map_or(DeepXExecutionUpdateRoute::External, |context| {
                DeepXExecutionUpdateRoute::RegisteredExternal(context)
            }))
    }

    fn finish(&self, client_order_id: &ClientOrderId) -> Result<(), DeepXOrderContextError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXOrderContextError::LockPoisoned)?;
        if state.terminal.contains_key(client_order_id) {
            return Ok(());
        }
        let context = state
            .tracked
            .remove(client_order_id)
            .ok_or(DeepXOrderContextError::ContextNotFound(*client_order_id))?;
        state.terminal.insert(*client_order_id, context);
        state.retain_owned_venue_bindings();
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct DeepXExecutionStartup {
    completed_steps: usize,
}

impl DeepXExecutionStartup {
    const REQUIRED: [DeepXExecutionStartupEvidence; 7] = [
        DeepXExecutionStartupEvidence::InstrumentsLoaded,
        DeepXExecutionStartupEvidence::OrderContextRestored,
        DeepXExecutionStartupEvidence::RuntimeValidated,
        DeepXExecutionStartupEvidence::PrivateStreamAuthenticated,
        DeepXExecutionStartupEvidence::AccountStateInitialized,
        DeepXExecutionStartupEvidence::MassReconciliationCompleted,
        DeepXExecutionStartupEvidence::AccountRegistered,
    ];

    fn record(
        &mut self,
        evidence: DeepXExecutionStartupEvidence,
    ) -> Result<bool, DeepXExecutionStartupError> {
        let Some(expected) = Self::REQUIRED.get(self.completed_steps).copied() else {
            return Err(DeepXExecutionStartupError::AlreadyComplete);
        };
        if evidence != expected {
            return Err(DeepXExecutionStartupError::OutOfOrder {
                expected,
                received: evidence,
            });
        }
        self.completed_steps += 1;
        Ok(self.is_ready())
    }

    fn validate_next(
        &self,
        evidence: DeepXExecutionStartupEvidence,
    ) -> Result<(), DeepXExecutionStartupError> {
        let Some(expected) = Self::REQUIRED.get(self.completed_steps).copied() else {
            return Err(DeepXExecutionStartupError::AlreadyComplete);
        };
        if evidence != expected {
            return Err(DeepXExecutionStartupError::OutOfOrder {
                expected,
                received: evidence,
            });
        }
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.completed_steps == Self::REQUIRED.len()
    }

    fn reset(&mut self) {
        self.completed_steps = 0;
    }
}

/// Non-operational DeepX execution client foundation.
///
/// This type owns execution identity and event construction, but intentionally does not implement
/// order commands or network connection until venue fixtures prove those protocol semantics.
#[derive(Debug)]
pub struct DeepXExecutionClient {
    core: ExecutionClientCore,
    config: DeepXExecutionClientConfig,
    credential: DeepXPrivateKey,
    emitter: ExecutionEventEmitter,
    order_contexts: DeepXOrderContextRegistry,
    trade_dedup: DeepXTradeDedup<TRADE_DEDUP_CAPACITY>,
    startup: DeepXExecutionStartup,
    startup_authenticated_session: Option<DeepXWsAuthenticatedSession>,
    startup_account_event_id: Option<UUID4>,
}

impl DeepXExecutionClient {
    #[allow(
        dead_code,
        reason = "reserved for the fixture-gated order report reconciliation path"
    )]
    pub(crate) fn merge_validated_order_reports(
        &self,
        reports: impl IntoIterator<Item = OrderStatusReport>,
    ) -> Result<Vec<OrderStatusReport>, DeepXOrderReportMergeError> {
        fn has_same_evidence(first: &OrderStatusReport, second: &OrderStatusReport) -> bool {
            first.account_id == second.account_id
                && first.instrument_id == second.instrument_id
                && first.client_order_id == second.client_order_id
                && first.venue_order_id == second.venue_order_id
                && first.order_side == second.order_side
                && first.order_type == second.order_type
                && first.time_in_force == second.time_in_force
                && first.order_status == second.order_status
                && first.quantity == second.quantity
                && first.filled_qty == second.filled_qty
                && first.ts_accepted == second.ts_accepted
                && first.ts_last == second.ts_last
                && first.order_list_id == second.order_list_id
                && first.venue_position_id == second.venue_position_id
                && first.linked_order_ids == second.linked_order_ids
                && first.parent_order_id == second.parent_order_id
                && first.contingency_type == second.contingency_type
                && first.expire_time == second.expire_time
                && first.price == second.price
                && first.activation_price == second.activation_price
                && first.trigger_price == second.trigger_price
                && first.trigger_type == second.trigger_type
                && first.limit_offset == second.limit_offset
                && first.trailing_offset == second.trailing_offset
                && first.trailing_offset_type == second.trailing_offset_type
                && first.avg_px == second.avg_px
                && first.display_qty == second.display_qty
                && first.post_only == second.post_only
                && first.reduce_only == second.reduce_only
                && first.cancel_reason == second.cancel_reason
                && first.ts_triggered == second.ts_triggered
        }

        let mut reports: Vec<_> = reports.into_iter().collect();
        reports.sort_by_key(|report| {
            (
                report.venue_order_id,
                report.account_id,
                report.instrument_id.venue,
                report.client_order_id,
                report.ts_init,
                report.report_id.to_string(),
            )
        });

        for report in &reports {
            if report.account_id != self.core.account_id {
                return Err(DeepXOrderReportMergeError::AccountMismatch {
                    expected: self.core.account_id,
                    received: report.account_id,
                    venue_order_id: report.venue_order_id,
                });
            }
        }
        for report in &reports {
            if report.instrument_id.venue != *DEEPX_VENUE {
                return Err(DeepXOrderReportMergeError::InstrumentVenueMismatch {
                    venue_order_id: report.venue_order_id,
                    venue: report.instrument_id.venue,
                });
            }
        }

        let mut venue_order_ids_by_client = HashMap::new();
        for report in &reports {
            if let Some(client_order_id) = report.client_order_id
                && let Some(first_venue_order_id) =
                    venue_order_ids_by_client.get(&client_order_id).copied()
                && first_venue_order_id != report.venue_order_id
            {
                return Err(DeepXOrderReportMergeError::ClientOrderIdentitySplit {
                    client_order_id,
                    first_venue_order_id,
                    second_venue_order_id: report.venue_order_id,
                });
            } else if let Some(client_order_id) = report.client_order_id {
                venue_order_ids_by_client.insert(client_order_id, report.venue_order_id);
            }
        }

        let mut reports_by_venue_order_id = HashMap::new();
        for report in reports {
            if let Some(existing) = reports_by_venue_order_id.get(&report.venue_order_id) {
                if !has_same_evidence(existing, &report) {
                    return Err(DeepXOrderReportMergeError::ConflictingOrder(
                        report.venue_order_id,
                    ));
                }
            } else {
                reports_by_venue_order_id.insert(report.venue_order_id, report);
            }
        }

        let mut merged: Vec<_> = reports_by_venue_order_id.into_values().collect();
        merged.sort_by_key(|report| report.venue_order_id);
        Ok(merged)
    }

    #[allow(
        dead_code,
        reason = "reserved for the fixture-gated fill report reconciliation path"
    )]
    pub(crate) fn merge_validated_fill_reports(
        &self,
        reports: impl IntoIterator<Item = FillReport>,
    ) -> Result<Vec<FillReport>, DeepXFillReportMergeError> {
        fn has_same_evidence(first: &FillReport, second: &FillReport) -> bool {
            first.account_id == second.account_id
                && first.instrument_id == second.instrument_id
                && first.venue_order_id == second.venue_order_id
                && first.trade_id == second.trade_id
                && first.order_side == second.order_side
                && first.last_qty == second.last_qty
                && first.last_px == second.last_px
                && first.commission == second.commission
                && first.liquidity_side == second.liquidity_side
                && first.avg_px == second.avg_px
                && first.ts_event == second.ts_event
                && first.client_order_id == second.client_order_id
                && first.venue_position_id == second.venue_position_id
        }

        let mut reports: Vec<_> = reports.into_iter().collect();
        reports.sort_by_key(|report| {
            (
                report.trade_id,
                report.account_id,
                report.instrument_id.venue,
                report.ts_event,
                report.ts_init,
                report.report_id.to_string(),
            )
        });

        for report in &reports {
            if report.account_id != self.core.account_id {
                return Err(DeepXFillReportMergeError::AccountMismatch {
                    expected: self.core.account_id,
                    received: report.account_id,
                    trade_id: report.trade_id,
                });
            }
        }
        for report in &reports {
            if report.instrument_id.venue != *DEEPX_VENUE {
                return Err(DeepXFillReportMergeError::InstrumentVenueMismatch {
                    trade_id: report.trade_id,
                    venue: report.instrument_id.venue,
                });
            }
        }

        let mut reports_by_trade_id = HashMap::new();

        for report in reports {
            if let Some(existing) = reports_by_trade_id.get(&report.trade_id) {
                if !has_same_evidence(existing, &report) {
                    return Err(DeepXFillReportMergeError::ConflictingTrade(report.trade_id));
                }
            } else {
                reports_by_trade_id.insert(report.trade_id, report);
            }
        }

        let mut merged: Vec<_> = reports_by_trade_id.into_values().collect();
        merged.sort_by_key(|report| (report.ts_event, report.trade_id));
        Ok(merged)
    }

    fn validate_rpc_evidence(
        &self,
        endpoints: &DeepXValidatedRpcEndpoints,
        capabilities: &DeepXValidatedRpcMethodCapabilities,
    ) -> Result<(), DeepXExecutionStartupError> {
        for role in [
            DeepXRpcRole::Submission,
            DeepXRpcRole::Watch,
            DeepXRpcRole::Recovery,
        ] {
            let configured_url = self
                .config
                .network
                .rpc_url_for(role)
                .map_err(|_| DeepXExecutionStartupError::RuntimeRpcEndpointMismatch(role))?;
            if endpoints.url_for(role) != configured_url {
                return Err(DeepXExecutionStartupError::RuntimeRpcEndpointMismatch(role));
            }
            let role_capabilities = capabilities.for_role(role);
            if role_capabilities.role() != role
                || role_capabilities.endpoint_url() != endpoints.url_for(role)
            {
                return Err(DeepXExecutionStartupError::RuntimeRpcCapabilitiesMismatch(
                    role,
                ));
            }
        }
        Ok(())
    }

    /// Creates a disconnected DeepX execution client foundation.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported deployment, invalid identity, malformed credential, or
    /// a core whose venue or account differs from the execution configuration.
    pub fn new(
        core: ExecutionClientCore,
        config: DeepXExecutionClientConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        anyhow::ensure!(
            core.venue == *DEEPX_VENUE,
            "DeepX execution core venue must be {}",
            *DEEPX_VENUE,
        );
        anyhow::ensure!(
            core.account_id == config.account_id,
            "DeepX execution core account ID must match configured account ID",
        );
        let credential = config.resolve_private_key()?;
        let emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );

        Ok(Self {
            core,
            config,
            credential,
            emitter,
            order_contexts: DeepXOrderContextRegistry::default(),
            trade_dedup: DeepXTradeDedup::default(),
            startup: DeepXExecutionStartup::default(),
            startup_authenticated_session: None,
            startup_account_event_id: None,
        })
    }

    #[allow(
        dead_code,
        reason = "reserved for the fixture-gated private fill dispatch path"
    )]
    fn reserve_trade_id(
        &self,
        trade_id: TradeId,
    ) -> Result<Option<DeepXTradeReservation<'_, TRADE_DEDUP_CAPACITY>>, DeepXTradeDedupError> {
        self.trade_dedup.reserve(trade_id)
    }

    /// Registers immutable Nautilus context restored before execution updates are dispatched.
    ///
    /// Re-registering the exact context is idempotent. Reusing a client order ID for different
    /// terms fails closed and preserves the original context.
    ///
    /// # Errors
    ///
    /// Returns an error when the client order ID conflicts with an existing context or registry
    /// access fails.
    pub fn register_order_context(
        &self,
        context: OrderContext,
    ) -> Result<(), DeepXOrderContextError> {
        self.order_contexts.register(context)
    }

    /// Captures and registers immutable Nautilus context before an order can be submitted.
    ///
    /// # Errors
    ///
    /// Returns an error when the client order ID conflicts with an existing context or registry
    /// access fails.
    pub fn register_order(&self, order: &OrderAny) -> Result<(), DeepXOrderContextError> {
        self.register_order_context(OrderContext::from(order))
    }

    /// Registers framework-provided identity for a reconciled external order.
    ///
    /// Registration is idempotent for identical context and fails closed if either order ID is
    /// already bound to different ownership or identity metadata.
    ///
    /// # Errors
    ///
    /// Returns an error for conflicting ownership, identity bindings, or registry access failure.
    pub fn register_external_order(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        strategy_id: StrategyId,
        ts_init: UnixNanos,
    ) -> Result<(), DeepXOrderContextError> {
        self.order_contexts
            .register_external(DeepXExternalOrderContext {
                client_order_id,
                venue_order_id,
                instrument_id,
                strategy_id,
                ts_init,
            })
    }

    /// Returns registered external order context by client order ID.
    ///
    /// # Errors
    ///
    /// Returns an error when registry access fails.
    pub fn external_order_context_by_client(
        &self,
        client_order_id: &ClientOrderId,
    ) -> Result<Option<DeepXExternalOrderContext>, DeepXOrderContextError> {
        self.order_contexts.external_by_client(client_order_id)
    }

    /// Returns registered external order context by venue order ID.
    ///
    /// # Errors
    ///
    /// Returns an error when registry access fails.
    pub fn external_order_context_by_venue(
        &self,
        venue_order_id: &VenueOrderId,
    ) -> Result<Option<DeepXExternalOrderContext>, DeepXOrderContextError> {
        self.order_contexts.external_by_venue(venue_order_id)
    }

    /// Verifies the complete public market catalog and advances the startup gate.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for instrument loading and the provider has
    /// completed a failure-atomic Spot and perpetual market load.
    pub fn record_instruments_loaded(
        &mut self,
        provider: &DeepXMarketProvider,
    ) -> Result<(), DeepXExecutionStartupError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::InstrumentsLoaded)?;
        if !provider.initialized() {
            return Err(DeepXExecutionStartupError::MarketCatalogNotInitialized);
        }
        let configured_urls = self
            .config
            .network
            .rest_urls()
            .map_err(|_| DeepXExecutionStartupError::MarketCatalogEndpointMismatch)?;
        if provider.base_urls().len() != configured_urls.len()
            || provider
                .base_urls()
                .iter()
                .zip(configured_urls)
                .any(|(actual, configured)| actual != configured.trim_end_matches('/'))
        {
            return Err(DeepXExecutionStartupError::MarketCatalogEndpointMismatch);
        }
        if provider.is_empty() {
            return Err(DeepXExecutionStartupError::MarketCatalogEmpty);
        }
        self.startup
            .record(DeepXExecutionStartupEvidence::InstrumentsLoaded)?;
        Ok(())
    }

    /// Atomically replaces the complete order-context snapshot and advances the startup gate.
    ///
    /// An explicitly empty set is valid when no active local orders require restoration. The
    /// registry and startup gate remain unchanged if validation or registration fails.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for restoration and the supplied snapshot has no
    /// conflicting duplicate client order IDs.
    pub fn restore_order_contexts(
        &mut self,
        contexts: impl IntoIterator<Item = OrderContext>,
    ) -> Result<(), DeepXOrderContextRestorationError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::OrderContextRestored)?;
        self.order_contexts.restore(contexts)?;
        self.startup
            .record(DeepXExecutionStartupEvidence::OrderContextRestored)?;
        Ok(())
    }

    /// Atomically restores complete order context and venue identity bindings.
    ///
    /// An explicitly empty set is valid when no active local orders require restoration. The
    /// registry and startup gate remain unchanged if validation or registration fails.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for restoration and the supplied snapshot has
    /// unique, conflict-free client and venue order identities.
    pub fn restore_order_context_identities(
        &mut self,
        contexts: impl IntoIterator<Item = DeepXRestoredOrderContext>,
    ) -> Result<(), DeepXOrderContextRestorationError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::OrderContextRestored)?;
        self.order_contexts.restore_with_venue_ids(contexts)?;
        self.startup
            .record(DeepXExecutionStartupEvidence::OrderContextRestored)?;
        Ok(())
    }

    /// Restores the complete open-order context snapshot from the shared execution cache.
    ///
    /// Only DeepX orders assigned to the configured execution account are restored. Existing venue
    /// order IDs are preserved in the atomic replacement snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for restoration, the cache can be borrowed, and
    /// the derived snapshot has unique, conflict-free client and venue order identities.
    pub fn restore_order_contexts_from_cache(
        &mut self,
    ) -> Result<(), DeepXOrderContextRestorationError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::OrderContextRestored)?;
        let contexts = self
            .core
            .try_cache()
            .map_err(|_| DeepXOrderContextRestorationError::CacheBorrowConflict)?
            .orders_open(
                Some(&self.core.venue),
                None,
                None,
                Some(&self.core.account_id),
                None,
            )
            .into_iter()
            .map(|order| {
                let order = order.cloned();
                DeepXRestoredOrderContext::new(OrderContext::from(&order), order.venue_order_id())
            })
            .collect::<Vec<_>>();
        self.restore_order_context_identities(contexts)
    }

    /// Verifies applied finalized runtime and RPC-role evidence and advances the startup gate.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for runtime validation, the applied snapshot
    /// matches the configured deployment, and every validated RPC role matches this configuration
    /// and the snapshot genesis hash.
    pub fn record_runtime_validated(
        &mut self,
        applied: &DeepXAppliedRuntimeSnapshot,
        endpoints: &DeepXValidatedRpcEndpoints,
        capabilities: &DeepXValidatedRpcMethodCapabilities,
    ) -> Result<(), DeepXExecutionStartupError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::RuntimeValidated)?;
        let identity = applied.identity();
        if identity.environment != self.config.network.environment {
            return Err(DeepXExecutionStartupError::RuntimeEnvironmentMismatch {
                expected: self.config.network.environment.clone(),
                received: identity.environment.clone(),
            });
        }
        if identity.genesis_hash != endpoints.genesis_hash() {
            return Err(DeepXExecutionStartupError::RuntimeGenesisMismatch);
        }
        if self.config.execution_backend != DeepXExecutionBackend::DirectPallet {
            return Err(DeepXExecutionStartupError::UnsupportedRuntimeBackend(
                self.config.execution_backend,
            ));
        }
        self.validate_rpc_evidence(endpoints, capabilities)?;
        self.startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)?;
        Ok(())
    }

    /// Verifies current private-stream authentication and advances the startup gate.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for private-stream authentication and the
    /// supplied receipt is still current for the protocol owner and connection epoch.
    pub fn record_private_stream_authenticated(
        &mut self,
        protocol: &DeepXWsProtocolCore,
        session: DeepXWsAuthenticatedSession,
    ) -> Result<(), DeepXExecutionStartupError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::PrivateStreamAuthenticated)?;
        if !protocol.is_authenticated_session(session) {
            return Err(DeepXExecutionStartupError::PrivateStreamAuthenticationMismatch);
        }
        self.startup
            .record(DeepXExecutionStartupEvidence::PrivateStreamAuthenticated)?;
        self.startup_authenticated_session = Some(session);
        Ok(())
    }

    /// Classifies an execution update as tracked or external without accessing the engine cache.
    ///
    /// # Errors
    ///
    /// Returns an error when registry access fails.
    pub fn route_execution_update(
        &self,
        client_order_id: Option<ClientOrderId>,
    ) -> Result<DeepXExecutionUpdateRoute, DeepXOrderContextError> {
        self.route_execution_update_identity(client_order_id, None)
    }

    /// Classifies an execution update using every available venue identity.
    ///
    /// # Errors
    ///
    /// Returns an error when client and venue order IDs resolve to conflicting ownership or
    /// registry access fails.
    pub fn route_execution_update_identity(
        &self,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
    ) -> Result<DeepXExecutionUpdateRoute, DeepXOrderContextError> {
        self.order_contexts.route(client_order_id, venue_order_id)
    }

    /// Binds a venue order ID to tracked or retained terminal order ownership.
    ///
    /// # Errors
    ///
    /// Returns an error when the client order has no tracked ownership, either identity is already
    /// bound inconsistently, or registry access fails.
    pub fn bind_tracked_venue_order_id(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> Result<(), DeepXOrderContextError> {
        self.order_contexts
            .bind_tracked_venue_order_id(client_order_id, venue_order_id)
    }

    /// Moves terminal order context from active routing into bounded ownership history.
    ///
    /// # Errors
    ///
    /// Returns an error when the order has no registered context or registry access fails.
    pub fn finish_order_context(
        &self,
        client_order_id: &ClientOrderId,
    ) -> Result<(), DeepXOrderContextError> {
        self.order_contexts.finish(client_order_id)
    }

    /// Restores the configured signer's timestamp nonce allocator from durable records.
    ///
    /// # Errors
    ///
    /// Returns an error unless the lease belongs to the configured signing key and the complete
    /// durable signer record set passes acknowledgement and identity verification.
    pub async fn restore_timestamp_nonce_allocator<S>(
        &self,
        store: &S,
        lease: &S::Lease,
    ) -> Result<
        (
            DeepXTimestampNonceAllocator,
            Vec<DeepXRestoredTransactionRecord>,
        ),
        DeepXNonceRestorationError,
    >
    where
        S: DeepXTransactionStore,
    {
        if lease.signer() != derive_signer_account_id(&self.credential)? {
            return Err(DeepXNonceRestorationError::SignerLeaseMismatch);
        }
        restore_timestamp_nonce_allocator(
            store,
            lease,
            self.config.timestamp_nonce_max_clock_drift_ms,
        )
        .await
        .map_err(Into::into)
    }

    /// Verifies, emits, and records the account-state event for the current startup epoch.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for account-state initialization, the private
    /// stream authentication is still current, and the event matches the configured execution
    /// account identity and type, or event dispatch fails.
    pub fn record_account_state_initialized(
        &mut self,
        protocol: &DeepXWsProtocolCore,
        frame: &DeepXWsAuthenticatedFrame,
        state: &AccountState,
    ) -> Result<(), DeepXExecutionStartupError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::AccountStateInitialized)?;
        if self.startup_authenticated_session != Some(frame.session())
            || !protocol.is_authenticated_session(frame.session())
        {
            return Err(DeepXExecutionStartupError::PrivateStreamAuthenticationMismatch);
        }
        if state.account_id != self.core.account_id || state.account_type != self.core.account_type
        {
            return Err(DeepXExecutionStartupError::AccountStateIdentityMismatch {
                expected_account_id: self.core.account_id,
                expected_account_type: self.core.account_type,
                received_account_id: state.account_id,
                received_account_type: state.account_type,
            });
        }
        self.emitter
            .try_send_account_state(state.clone())
            .map_err(|e| DeepXExecutionStartupError::AccountStateDispatchFailed(e.to_string()))?;
        self.startup_account_event_id = Some(state.event_id);
        self.startup
            .record(DeepXExecutionStartupEvidence::AccountStateInitialized)?;
        Ok(())
    }

    /// Verifies the complete durable signer record set and advances startup reconciliation.
    ///
    /// An empty complete set is valid. Every restored transaction must have an exact durable
    /// acknowledgement and require no further recovery, submission decision, or operator action.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for mass reconciliation, the current store lease
    /// belongs to the configured signing key, the private-stream authentication is still current,
    /// and every durable transaction is complete.
    pub async fn record_mass_reconciliation_completed<S>(
        &mut self,
        protocol: &DeepXWsProtocolCore,
        session: DeepXWsAuthenticatedSession,
        endpoints: &DeepXValidatedRpcEndpoints,
        capabilities: &DeepXValidatedRpcMethodCapabilities,
        store: &S,
        lease: &S::Lease,
    ) -> Result<(), DeepXMassReconciliationError>
    where
        S: DeepXTransactionStore,
    {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::MassReconciliationCompleted)?;
        if self.startup_authenticated_session != Some(session)
            || !protocol.is_authenticated_session(session)
        {
            return Err(DeepXExecutionStartupError::PrivateStreamAuthenticationMismatch.into());
        }
        if lease.signer() != derive_signer_account_id(&self.credential)? {
            return Err(DeepXMassReconciliationError::SignerLeaseMismatch);
        }
        self.validate_rpc_evidence(endpoints, capabilities)?;
        let restored = load_verified_committed_for_signer(store, lease).await?;
        for item in restored {
            let client_order_id = item.record().identity().client_order_id().to_string();
            let received_genesis_hash = item.record().identity().runtime().genesis_hash;
            let expected_genesis_hash = endpoints.genesis_hash();
            if received_genesis_hash != expected_genesis_hash {
                return Err(DeepXMassReconciliationError::RuntimeGenesisMismatch {
                    client_order_id,
                    expected_genesis_hash,
                    received_genesis_hash,
                });
            }
            let action = match item.record().lifecycle().state() {
                DeepXTransactionState::Submitting | DeepXTransactionState::Accepted => {
                    reconcile_submission_pool(endpoints, store, lease, &item)
                        .await?
                        .record()
                        .recovery_action()
                }
                DeepXTransactionState::InBlockSuccess | DeepXTransactionState::InBlockFailed => {
                    match observe_and_commit_finality(endpoints, capabilities, store, lease, &item)
                        .await
                    {
                        Ok(committed) => committed.record().recovery_action(),
                        Err(DeepXFinalityCommitError::Watch(
                            DeepXTransactionWatchError::FinalityEvidenceConflict(_),
                        )) => observe_and_commit_reorganization(
                            endpoints,
                            capabilities,
                            store,
                            lease,
                            &item,
                        )
                        .await?
                        .record()
                        .recovery_action(),
                        Err(e) => return Err(e.into()),
                    }
                }
                DeepXTransactionState::NotIncluded => reconcile_not_included_checkpoint(
                    endpoints,
                    capabilities,
                    store,
                    lease,
                    &item,
                    self.config.recovery_blocks_per_range,
                )
                .await?
                .record()
                .recovery_action(),
                _ => item.record().recovery_action(),
            };
            if action != DeepXTransactionRecoveryAction::Complete {
                return Err(DeepXMassReconciliationError::UnresolvedTransaction {
                    client_order_id,
                    action,
                });
            }
        }
        self.startup
            .record(DeepXExecutionStartupEvidence::MassReconciliationCompleted)?;
        Ok(())
    }

    /// Verifies the configured account is registered and completes the startup gate.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for account registration, the private-stream
    /// authentication is still current, and the configured account exists in the shared execution
    /// cache.
    pub fn complete_account_registration(
        &mut self,
        protocol: &DeepXWsProtocolCore,
        session: DeepXWsAuthenticatedSession,
    ) -> Result<(), DeepXExecutionStartupError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::AccountRegistered)?;
        if self.startup_authenticated_session != Some(session)
            || !protocol.is_authenticated_session(session)
        {
            return Err(DeepXExecutionStartupError::PrivateStreamAuthenticationMismatch);
        }
        let event_id = self
            .startup_account_event_id
            .ok_or(DeepXExecutionStartupError::AccountStateVerificationRequired)?;
        let registered = self
            .core
            .try_cache()
            .map_err(|_| DeepXExecutionStartupError::CacheBorrowConflict)?
            .account(&self.core.account_id)
            .is_some_and(|account| {
                account.events().iter().any(|event| {
                    event.event_id == event_id && event.account_type == self.core.account_type
                })
            });
        if !registered {
            return Err(DeepXExecutionStartupError::AccountStateNotRegistered {
                account_id: self.core.account_id,
                event_id,
            });
        }
        if self
            .startup
            .record(DeepXExecutionStartupEvidence::AccountRegistered)?
        {
            self.core.set_connected();
        }
        Ok(())
    }

    /// Clears startup evidence and marks the execution core disconnected.
    pub fn reset_startup(&mut self) {
        self.core.set_disconnected();
        self.startup.reset();
        self.startup_authenticated_session = None;
        self.startup_account_event_id = None;
    }

    /// Returns whether the execution core passed every startup gate.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    /// Returns the validated execution configuration.
    #[must_use]
    pub const fn config(&self) -> &DeepXExecutionClientConfig {
        &self.config
    }

    /// Returns the redacted signing credential boundary.
    #[must_use]
    pub const fn credential(&self) -> &DeepXPrivateKey {
        &self.credential
    }

    /// Returns the execution event emitter owned by this client.
    #[must_use]
    pub const fn emitter(&self) -> &ExecutionEventEmitter {
        &self.emitter
    }
}

#[async_trait(?Send)]
impl ExecutionClient for DeepXExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *DEEPX_VENUE
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn provides_bulk_position_coverage(&self, _instrument_id: InstrumentId) -> bool {
        false
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event, info);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }

        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.reset_startup();
        if self.core.is_stopped() {
            return Ok(());
        }

        self.core.set_stopped();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.reset_startup();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.startup.is_ready() && self.core.is_connected(),
            "DeepX execution startup has not completed",
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.reset_startup();
        Ok(())
    }

    fn submit_order(&self, _cmd: SubmitOrder) -> anyhow::Result<()> {
        anyhow::bail!("DeepX order submission is not operational")
    }

    fn submit_order_list(&self, _cmd: SubmitOrderList) -> anyhow::Result<()> {
        anyhow::bail!("DeepX order-list submission is not operational")
    }

    fn modify_order(&self, _cmd: ModifyOrder) -> anyhow::Result<()> {
        anyhow::bail!("DeepX order modification is not operational")
    }

    fn batch_modify_orders(&self, _cmd: BatchModifyOrders) -> anyhow::Result<()> {
        anyhow::bail!("DeepX batch order modification is not operational")
    }

    fn cancel_order(&self, _cmd: CancelOrder) -> anyhow::Result<()> {
        anyhow::bail!("DeepX order cancellation is not operational")
    }

    fn cancel_all_orders(&self, _cmd: CancelAllOrders) -> anyhow::Result<()> {
        anyhow::bail!("DeepX cancel-all is not operational")
    }

    fn batch_cancel_orders(&self, _cmd: BatchCancelOrders) -> anyhow::Result<()> {
        anyhow::bail!("DeepX batch cancellation is not operational")
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        anyhow::bail!("DeepX account queries are not operational")
    }

    fn query_order(&self, _cmd: QueryOrder) -> anyhow::Result<()> {
        anyhow::bail!("DeepX order queries are not operational")
    }

    async fn generate_order_status_report(
        &self,
        _cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        anyhow::bail!("DeepX order status reports are not operational")
    }

    async fn generate_order_status_reports(
        &self,
        _cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        anyhow::bail!("DeepX order status reports are not operational")
    }

    async fn generate_fill_reports(
        &self,
        _cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        anyhow::bail!("DeepX fill reports are not operational")
    }

    async fn generate_position_status_reports(
        &self,
        _cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        anyhow::bail!("DeepX position status reports are not operational")
    }

    async fn generate_mass_status(
        &self,
        _lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        anyhow::bail!("DeepX mass status reports are not operational")
    }

    fn calculate_commission(
        &self,
        _instrument: &InstrumentAny,
        _last_qty: Quantity,
        _last_px: Price,
        _liquidity_side: LiquiditySide,
    ) -> anyhow::Result<Option<Money>> {
        anyhow::bail!("DeepX commission calculation is not operational")
    }

    fn register_external_order(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        strategy_id: StrategyId,
        ts_init: UnixNanos,
    ) {
        if let Err(e) = Self::register_external_order(
            self,
            client_order_id,
            venue_order_id,
            instrument_id,
            strategy_id,
            ts_init,
        ) {
            log_error!("Failed to register external DeepX order: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{
        Json, Router,
        routing::{get, post},
    };
    use nautilus_common::{
        cache::Cache, live::runner::replace_exec_event_sender, messages::ExecutionEvent,
    };
    use nautilus_core::{UUID4, UnixNanos, hex};
    use nautilus_model::instruments::stubs::crypto_perpetual_ethusdt;
    use nautilus_model::{
        accounts::{AccountAny, MarginAccount},
        enums::{
            AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce,
        },
        events::{AccountState, OrderAccepted, OrderEventAny, OrderSubmitted},
        identifiers::{
            AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TradeId, TraderId,
            VenueOrderId,
        },
        orders::OrderTestBuilder,
        types::{Money, Price, Quantity},
    };
    use rstest::rstest;
    use serde_json::{Value, json};
    use subxt_core::config::{Hasher, substrate::BlakeTwo256};
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        common::consts::DEEPX_TESTNET_GENESIS_HASH,
        config::{DeepXObservedRpcEndpoint, validate_rpc_endpoint_identities},
        rpc::{
            DeepXValidatedRpcMethodCapabilities,
            observe_and_apply_approved_finalized_runtime_snapshot,
            observe_and_validate_rpc_method_capabilities,
        },
        signing::{
            ApprovedRuntimeIdentity, DeepXRuntimeSnapshotService, RuntimeSnapshot,
            SignedPalletExtrinsic,
        },
        transaction::{
            DeepXBusinessEventOutcome, DeepXCommittedTransactionRecord, DeepXDirectRuntimeIdentity,
            DeepXDispatchOutcome, DeepXInclusionEvidence, DeepXIndexedOutcome,
            DeepXNonceReservation, DeepXRestoredTransactionRecord, DeepXTimestampNonceError,
            DeepXTransactionIdentity, DeepXTransactionObservation, DeepXTransactionRecord,
            DeepXTransactionRevision,
        },
        websocket::DeepXWsFrame,
    };

    const GENESIS_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
         genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/\
         genesis_hash.json"
    );
    const FINALIZED_HEAD_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
         genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/\
         finalized_head.json"
    );
    const RUNTIME_VERSION_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
         genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/\
         runtime_version.json"
    );
    const METADATA_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
            genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/\
         metadata.json"
    );

    #[derive(Debug)]
    struct TestSignerLease {
        signer: [u8; 20],
    }

    impl DeepXSignerLease for TestSignerLease {
        fn signer(&self) -> [u8; 20] {
            self.signer
        }

        fn generation(&self) -> u64 {
            1
        }
    }

    #[derive(Debug)]
    struct TestTransactionStore {
        restored: Vec<DeepXRestoredTransactionRecord>,
    }

    #[async_trait::async_trait]
    impl DeepXTransactionStore for TestTransactionStore {
        type Lease = TestSignerLease;

        async fn acquire_signer_lease(
            &self,
            signer: [u8; 20],
        ) -> Result<Self::Lease, DeepXTransactionPersistenceError> {
            Ok(TestSignerLease { signer })
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
            Ok(self.restored.clone())
        }

        async fn create_committed(
            &self,
            _lease: &Self::Lease,
            _record: &DeepXTransactionRecord,
        ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError> {
            Err(DeepXTransactionPersistenceError::Unsupported(
                "read-only test store".to_string(),
            ))
        }

        async fn compare_and_set_committed(
            &self,
            _lease: &Self::Lease,
            _expected: &DeepXCommittedTransactionRecord,
            _record: &DeepXTransactionRecord,
        ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError> {
            Err(DeepXTransactionPersistenceError::Unsupported(
                "read-only test store".to_string(),
            ))
        }
    }

    #[derive(Debug)]
    struct FinalityTestStore {
        revision: Mutex<u64>,
        encoded_record: Mutex<Vec<u8>>,
    }

    impl FinalityTestStore {
        fn new(revision: u64, record: &DeepXTransactionRecord) -> Self {
            Self {
                revision: Mutex::new(revision),
                encoded_record: Mutex::new(record.encode().unwrap()),
            }
        }

        fn current_revision(&self) -> u64 {
            *self.revision.lock().unwrap()
        }

        fn persisted_record(&self) -> DeepXTransactionRecord {
            DeepXTransactionRecord::decode(&self.encoded_record.lock().unwrap()).unwrap()
        }
    }

    #[async_trait::async_trait]
    impl DeepXTransactionStore for FinalityTestStore {
        type Lease = TestSignerLease;

        async fn acquire_signer_lease(
            &self,
            signer: [u8; 20],
        ) -> Result<Self::Lease, DeepXTransactionPersistenceError> {
            Ok(TestSignerLease { signer })
        }

        async fn verify_signer_lease(
            &self,
            _lease: &Self::Lease,
        ) -> Result<(), DeepXTransactionPersistenceError> {
            Ok(())
        }

        async fn load_committed_for_signer(
            &self,
            lease: &Self::Lease,
        ) -> Result<Vec<DeepXRestoredTransactionRecord>, DeepXTransactionPersistenceError> {
            let record = DeepXTransactionRecord::decode(&self.encoded_record.lock().unwrap())
                .map_err(|e| DeepXTransactionPersistenceError::BeforeCommit(e.to_string()))?;
            if record.identity().signer() != lease.signer() {
                return Ok(Vec::new());
            }
            let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
                &record,
                DeepXTransactionRevision::new(*self.revision.lock().unwrap()),
            )?;
            Ok(vec![DeepXRestoredTransactionRecord::new(
                record, committed,
            )?])
        }

        async fn create_committed(
            &self,
            _lease: &Self::Lease,
            _record: &DeepXTransactionRecord,
        ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError> {
            Err(DeepXTransactionPersistenceError::Unsupported(
                "finality test store does not create records".to_string(),
            ))
        }

        async fn compare_and_set_committed(
            &self,
            _lease: &Self::Lease,
            expected: &DeepXCommittedTransactionRecord,
            record: &DeepXTransactionRecord,
        ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError> {
            let mut revision = self.revision.lock().unwrap();
            let mut encoded_record = self.encoded_record.lock().unwrap();
            let current_record = DeepXTransactionRecord::decode(&encoded_record)
                .map_err(|e| DeepXTransactionPersistenceError::BeforeCommit(e.to_string()))?;
            if *revision != expected.revision().value()
                || expected.verify(&current_record).is_err()
                || current_record.identity() != record.identity()
            {
                return Err(DeepXTransactionPersistenceError::RevisionConflict);
            }
            *revision += 1;
            *encoded_record = record
                .encode()
                .map_err(|e| DeepXTransactionPersistenceError::BeforeCommit(e.to_string()))?;
            DeepXCommittedTransactionRecord::acknowledge_committed(
                record,
                DeepXTransactionRevision::new(*revision),
            )
        }
    }

    #[rstest]
    fn startup_requires_authoritative_evidence_in_order() {
        let mut startup = DeepXExecutionStartup::default();

        assert_eq!(
            startup.record(DeepXExecutionStartupEvidence::OrderContextRestored),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::InstrumentsLoaded,
                received: DeepXExecutionStartupEvidence::OrderContextRestored,
            }),
        );
        assert!(!startup.is_ready());
    }

    #[rstest]
    fn startup_becomes_ready_only_after_every_requirement() {
        let mut startup = DeepXExecutionStartup::default();

        for evidence in DeepXExecutionStartup::REQUIRED {
            let ready = startup.record(evidence).unwrap();
            assert_eq!(
                ready,
                evidence == DeepXExecutionStartupEvidence::AccountRegistered
            );
        }

        assert!(startup.is_ready());
        assert_eq!(
            startup.record(DeepXExecutionStartupEvidence::AccountRegistered),
            Err(DeepXExecutionStartupError::AlreadyComplete),
        );
    }

    #[rstest]
    fn reset_requires_startup_evidence_to_be_replayed() {
        let mut startup = DeepXExecutionStartup::default();
        startup
            .record(DeepXExecutionStartupEvidence::InstrumentsLoaded)
            .unwrap();

        startup.reset();

        assert!(!startup.is_ready());
        assert!(
            startup
                .record(DeepXExecutionStartupEvidence::InstrumentsLoaded)
                .is_ok()
        );
    }

    fn test_order(quantity: &str) -> OrderAny {
        test_order_with_id("O-DEEPX-001", quantity)
    }

    fn test_order_with_id(client_order_id: &str, quantity: &str) -> OrderAny {
        test_order_with_instrument(client_order_id, quantity, "ETH-USDC-PERP.DEEPX")
    }

    fn test_order_with_instrument(
        client_order_id: &str,
        quantity: &str,
        instrument_id: &str,
    ) -> OrderAny {
        OrderTestBuilder::new(OrderType::Limit)
            .client_order_id(ClientOrderId::from(client_order_id))
            .strategy_id(StrategyId::from("S-DEEPX-001"))
            .instrument_id(InstrumentId::from(instrument_id))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(quantity))
            .price(Price::from("2500.00"))
            .time_in_force(TimeInForce::Gtc)
            .build()
    }

    fn accept_order_in_cache(
        cache: &Rc<RefCell<Cache>>,
        order: &OrderAny,
        account_id: AccountId,
        venue_order_id: VenueOrderId,
    ) {
        cache
            .borrow_mut()
            .add_order(order.clone(), None, Some(ClientId::from("DEEPX")), false)
            .unwrap();
        let submitted = OrderSubmitted::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            account_id,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
        );
        cache
            .borrow_mut()
            .update_order(&OrderEventAny::Submitted(submitted))
            .unwrap();
        let accepted = OrderAccepted::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            venue_order_id,
            account_id,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            false,
        );
        cache
            .borrow_mut()
            .update_order(&OrderEventAny::Accepted(accepted))
            .unwrap();
    }

    fn test_external_order_context(
        client_order_id: &str,
        venue_order_id: &str,
    ) -> DeepXExternalOrderContext {
        DeepXExternalOrderContext {
            client_order_id: ClientOrderId::from(client_order_id),
            venue_order_id: VenueOrderId::from(venue_order_id),
            instrument_id: InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            strategy_id: StrategyId::from("S-DEEPX-EXTERNAL"),
            ts_init: UnixNanos::from(1_000_000),
        }
    }

    fn register_external_order(
        client: &DeepXExecutionClient,
        context: DeepXExternalOrderContext,
    ) -> Result<(), DeepXOrderContextError> {
        client.register_external_order(
            context.client_order_id,
            context.venue_order_id,
            context.instrument_id,
            context.strategy_id,
            context.ts_init,
        )
    }

    fn test_client() -> DeepXExecutionClient {
        test_client_with_cache().0
    }

    fn test_client_with_cache() -> (DeepXExecutionClient, Rc<RefCell<Cache>>) {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let core = ExecutionClientCore::new(
            TraderId::from("TRADER-001"),
            ClientId::from("DEEPX"),
            *DEEPX_VENUE,
            OmsType::Netting,
            AccountId::from("DEEPX-001"),
            AccountType::Margin,
            None,
            Rc::clone(&cache),
        );
        let config = DeepXExecutionClientConfig {
            subaccount_id: Some("subaccount-1".to_string()),
            private_key: Some(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            ),
            ..Default::default()
        };
        (DeepXExecutionClient::new(core, config).unwrap(), cache)
    }

    #[tokio::test]
    async fn nonce_restoration_uses_configured_clock_drift() {
        let mut client = test_client();
        client.config.timestamp_nonce_max_clock_drift_ms = 10;
        let store = TestTransactionStore {
            restored: Vec::new(),
        };
        let signer = derive_signer_account_id(&client.credential).unwrap();
        let lease = store.acquire_signer_lease(signer).await.unwrap();

        let (allocator, restored) = client
            .restore_timestamp_nonce_allocator(&store, &lease)
            .await
            .unwrap();

        assert!(restored.is_empty());
        assert_eq!(allocator.signer(), signer);
        assert_eq!(
            allocator.reserve(1_000, 1_011),
            Err(DeepXTimestampNonceError::ClockDrift {
                observed_drift_ms: 11,
                max_drift_ms: 10,
            }),
        );
    }

    #[tokio::test]
    async fn nonce_restoration_rejects_another_signer() {
        let client = test_client();
        let store = TestTransactionStore {
            restored: Vec::new(),
        };
        let lease = TestSignerLease { signer: [42; 20] };

        assert!(matches!(
            client
                .restore_timestamp_nonce_allocator(&store, &lease)
                .await,
            Err(DeepXNonceRestorationError::SignerLeaseMismatch),
        ));
    }

    fn record_instruments_loaded(client: &mut DeepXExecutionClient) {
        client
            .startup
            .record(DeepXExecutionStartupEvidence::InstrumentsLoaded)
            .unwrap();
    }

    async fn applied_runtime_evidence() -> (
        String,
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
        DeepXAppliedRuntimeSnapshot,
    ) {
        let genesis: Value = serde_json::from_str(GENESIS_FIXTURE).unwrap();
        let finalized_head: Value = serde_json::from_str(FINALIZED_HEAD_FIXTURE).unwrap();
        let finalized_header = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "number": "0x2a" },
        });
        let runtime_version: Value = serde_json::from_str(RUNTIME_VERSION_FIXTURE).unwrap();
        let metadata: Value = serde_json::from_str(METADATA_FIXTURE).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&calls);
                let responses = [
                    genesis.clone(),
                    finalized_head.clone(),
                    finalized_header.clone(),
                    runtime_version.clone(),
                    metadata.clone(),
                ];
                async move {
                    if request["method"] == "rpc_methods" {
                        return Json(json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "result": {
                                "methods": [
                                    "author_pendingExtrinsics",
                                    "author_submitExtrinsic",
                                    "chain_getBlock",
                                    "chain_getBlockHash",
                                    "chain_getFinalizedHead",
                                    "chain_getHeader",
                                    "state_getMetadata",
                                    "state_getRuntimeVersion",
                                ],
                            },
                        }));
                    }
                    let index = calls.fetch_add(1, Ordering::Relaxed);
                    Json(responses[index].clone())
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let rpc_url = format!("http://{address}");
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let network = crate::config::DeepXNetworkConfig {
            base_url_rpc_submission: Some(rpc_url.clone()),
            base_url_rpc_watch: Some(rpc_url.clone()),
            base_url_rpc_recovery: Some(rpc_url.clone()),
            ..Default::default()
        };
        let endpoints = validate_rpc_endpoint_identities(
            &network,
            [
                DeepXObservedRpcEndpoint::new(
                    DeepXRpcRole::Submission,
                    rpc_url.clone(),
                    genesis_hash,
                ),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, rpc_url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(
                    DeepXRpcRole::Recovery,
                    rpc_url.clone(),
                    genesis_hash,
                ),
            ],
        )
        .unwrap();
        let encoded_metadata = metadata_fixture_bytes();
        let service = DeepXRuntimeSnapshotService::new(
            RuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
                genesis_hash,
                366,
                1,
                &encoded_metadata,
            )
            .unwrap(),
        );
        let applied = observe_and_apply_approved_finalized_runtime_snapshot(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &service,
        )
        .await
        .unwrap();
        let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap();
        (rpc_url, endpoints, capabilities, applied)
    }

    fn metadata_fixture_bytes() -> Vec<u8> {
        let metadata: Value = serde_json::from_str(METADATA_FIXTURE).unwrap();
        hex::decode(
            metadata["result"]
                .as_str()
                .unwrap()
                .trim_start_matches("0x"),
        )
        .unwrap()
    }

    fn configure_rpc_url(client: &mut DeepXExecutionClient, rpc_url: String) {
        client.config.network.base_url_rpc_submission = Some(rpc_url.clone());
        client.config.network.base_url_rpc_watch = Some(rpc_url.clone());
        client.config.network.base_url_rpc_recovery = Some(rpc_url);
    }

    fn in_block_record(client: &DeepXExecutionClient) -> DeepXTransactionRecord {
        let mut record = submitting_record(client);
        record
            .apply_observation(DeepXTransactionObservation::Included(
                DeepXInclusionEvidence::from_indexed_observations(
                    [8; 32],
                    72,
                    DeepXIndexedOutcome {
                        extrinsic_index: 1,
                        outcome: DeepXDispatchOutcome::Success,
                    },
                    DeepXIndexedOutcome {
                        extrinsic_index: 1,
                        outcome: DeepXBusinessEventOutcome::Success,
                    },
                )
                .unwrap(),
            ))
            .unwrap();
        record
    }

    fn submitting_record(client: &DeepXExecutionClient) -> DeepXTransactionRecord {
        let signer = derive_signer_account_id(&client.credential).unwrap();
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let mut record = DeepXTransactionRecord::created(DeepXTransactionIdentity::new(
            ClientOrderId::from("O-DEEPX-IN-BLOCK"),
            signer,
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId { value: 42 },
            DeepXDirectRuntimeIdentity {
                genesis_hash,
                metadata_sha256: [2; 32],
                spec_version: 366,
                transaction_version: 1,
                signed_extensions: vec!["CheckNonce".to_string()],
            },
        ));
        let bytes = vec![12, 1, 2, 3];
        let identity = record.identity();
        let runtime = identity.runtime();
        record
            .record_signed(&SignedPalletExtrinsic {
                extrinsic_hash: BlakeTwo256.hash(&bytes).0,
                bytes,
                signer,
                nonce: 42,
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

    fn not_included_record(client: &DeepXExecutionClient) -> DeepXTransactionRecord {
        let signer = derive_signer_account_id(&client.credential).unwrap();
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let mut record = DeepXTransactionRecord::created(DeepXTransactionIdentity::new(
            ClientOrderId::from("O-DEEPX-NOT-INCLUDED"),
            signer,
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId { value: 42 },
            DeepXDirectRuntimeIdentity {
                genesis_hash,
                metadata_sha256: [2; 32],
                spec_version: 366,
                transaction_version: 1,
                signed_extensions: vec!["CheckNonce".to_string()],
            },
        ));
        let bytes = vec![12, 1, 2, 3];
        let identity = record.identity();
        let runtime = identity.runtime();
        record
            .record_signed(&SignedPalletExtrinsic {
                extrinsic_hash: BlakeTwo256.hash(&bytes).0,
                bytes,
                signer,
                nonce: 42,
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
        let mut encoded: Value = serde_json::from_slice(&record.encode().unwrap()).unwrap();
        encoded["lifecycle"]["state"] = json!(DeepXTransactionState::NotIncluded);
        encoded["lifecycle"]["absence"] = json!({
            "first_scanned_block": 70,
            "finalized_block_number": 72,
            "finalized_block_hash": vec![9; 32],
            "canonical_scan_complete": true,
            "submission_pool_absence": true,
        });
        DeepXTransactionRecord::decode(&serde_json::to_vec(&encoded).unwrap()).unwrap()
    }

    #[derive(Clone, Debug)]
    struct RecoveryRpcState {
        pool_extrinsics: Vec<String>,
    }

    async fn recovery_rpc(
        axum::extract::State(state): axum::extract::State<RecoveryRpcState>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
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
                ],
            }),
            "chain_getFinalizedHead" => json!(format!("0x{:064x}", 73)),
            "chain_getHeader" => json!({ "number": "0x49" }),
            "chain_getBlockHash" => {
                let block_number = request["params"][0].as_u64().unwrap();
                if block_number == 72 {
                    json!(format!("0x{}", "09".repeat(32)))
                } else {
                    json!(format!("0x{block_number:064x}"))
                }
            }
            "chain_getBlock" => json!({
                "block": {
                    "header": { "number": "0x49" },
                    "extrinsics": [],
                },
            }),
            "author_pendingExtrinsics" => json!(state.pool_extrinsics),
            method => panic!("unexpected method {method}"),
        };
        Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
    }

    async fn recovery_evidence(
        pool_extrinsics: &[&[u8]],
    ) -> (
        String,
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = RecoveryRpcState {
            pool_extrinsics: pool_extrinsics
                .iter()
                .map(|extrinsic| format!("0x{}", hex::encode(extrinsic)))
                .collect(),
        };
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(recovery_rpc))
                    .with_state(state),
            )
            .await
            .unwrap();
        });
        let rpc_url = format!("http://{address}");
        let network = crate::config::DeepXNetworkConfig {
            base_url_rpc: Some(rpc_url.clone()),
            ..Default::default()
        };
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let endpoints = validate_rpc_endpoint_identities(
            &network,
            [
                DeepXObservedRpcEndpoint::new(
                    DeepXRpcRole::Submission,
                    rpc_url.clone(),
                    genesis_hash,
                ),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, rpc_url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(
                    DeepXRpcRole::Recovery,
                    rpc_url.clone(),
                    genesis_hash,
                ),
            ],
        )
        .unwrap();
        let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap();
        (rpc_url, endpoints, capabilities)
    }

    #[derive(Clone, Debug)]
    struct FinalityRpcState {
        finalized_block: u64,
        canonical_block_hash: [u8; 32],
        target_extrinsic: String,
        canonical_requests: Arc<AtomicUsize>,
    }

    async fn finality_rpc(
        axum::extract::State(state): axum::extract::State<FinalityRpcState>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
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
                ],
            }),
            "chain_getFinalizedHead" => json!(format!("0x{}", "09".repeat(32))),
            "chain_getHeader" => json!({ "number": format!("0x{:x}", state.finalized_block) }),
            "chain_getBlockHash" => {
                state.canonical_requests.fetch_add(1, Ordering::Relaxed);
                json!(format!("0x{}", hex::encode(state.canonical_block_hash)))
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

    async fn finality_evidence(
        finalized_block: u64,
        target_extrinsic: &[u8],
    ) -> (
        String,
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
        Arc<AtomicUsize>,
    ) {
        finality_evidence_with_hash(finalized_block, [8; 32], target_extrinsic).await
    }

    async fn finality_evidence_with_hash(
        finalized_block: u64,
        canonical_block_hash: [u8; 32],
        target_extrinsic: &[u8],
    ) -> (
        String,
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let canonical_requests = Arc::new(AtomicUsize::new(0));
        let state = FinalityRpcState {
            finalized_block,
            canonical_block_hash,
            target_extrinsic: format!("0x{}", hex::encode(target_extrinsic)),
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
        let rpc_url = format!("http://{address}");
        let network = crate::config::DeepXNetworkConfig {
            base_url_rpc: Some(rpc_url.clone()),
            ..Default::default()
        };
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let endpoints = validate_rpc_endpoint_identities(
            &network,
            [
                DeepXObservedRpcEndpoint::new(
                    DeepXRpcRole::Submission,
                    rpc_url.clone(),
                    genesis_hash,
                ),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, rpc_url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(
                    DeepXRpcRole::Recovery,
                    rpc_url.clone(),
                    genesis_hash,
                ),
            ],
        )
        .unwrap();
        let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap();
        (rpc_url, endpoints, capabilities, canonical_requests)
    }

    #[tokio::test]
    async fn runtime_startup_accepts_applied_snapshot_for_configured_rpc_roles() {
        let (rpc_url, endpoints, capabilities, applied) = applied_runtime_evidence().await;
        let mut client = test_client();
        client.config.network.base_url_rpc_submission = Some(rpc_url.clone());
        client.config.network.base_url_rpc_watch = Some(rpc_url.clone());
        client.config.network.base_url_rpc_recovery = Some(rpc_url);
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();

        client
            .record_runtime_validated(&applied, &endpoints, &capabilities)
            .unwrap();

        assert!(
            client
                .startup
                .validate_next(DeepXExecutionStartupEvidence::PrivateStreamAuthenticated)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn private_stream_startup_accepts_current_authenticated_session() {
        let (rpc_url, endpoints, capabilities, applied) = applied_runtime_evidence().await;
        let mut client = test_client();
        client.config.network.base_url_rpc_submission = Some(rpc_url.clone());
        client.config.network.base_url_rpc_watch = Some(rpc_url.clone());
        client.config.network.base_url_rpc_recovery = Some(rpc_url);
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .record_runtime_validated(&applied, &endpoints, &capabilities)
            .unwrap();
        let mut protocol = DeepXWsProtocolCore::new('/');
        let (attempt, _) = protocol.begin_authentication().unwrap();
        assert!(protocol.complete_authentication(attempt));
        let session = protocol.authenticated_session().unwrap();

        client
            .record_private_stream_authenticated(&protocol, session)
            .unwrap();

        assert!(
            client
                .startup
                .validate_next(DeepXExecutionStartupEvidence::AccountStateInitialized)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn private_stream_startup_rejects_session_from_stale_connection_without_advancing() {
        let (rpc_url, endpoints, capabilities, applied) = applied_runtime_evidence().await;
        let mut client = test_client();
        client.config.network.base_url_rpc_submission = Some(rpc_url.clone());
        client.config.network.base_url_rpc_watch = Some(rpc_url.clone());
        client.config.network.base_url_rpc_recovery = Some(rpc_url);
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .record_runtime_validated(&applied, &endpoints, &capabilities)
            .unwrap();
        let mut protocol = DeepXWsProtocolCore::new('/');
        let (stale_attempt, _) = protocol.begin_authentication().unwrap();
        assert!(protocol.complete_authentication(stale_attempt));
        let stale_session = protocol.authenticated_session().unwrap();
        protocol.reset_after_reconnect(1, "test reconnect").unwrap();

        assert_eq!(
            client.record_private_stream_authenticated(&protocol, stale_session),
            Err(DeepXExecutionStartupError::PrivateStreamAuthenticationMismatch),
        );

        let (current_attempt, _) = protocol.begin_authentication().unwrap();
        assert!(protocol.complete_authentication(current_attempt));
        client
            .record_private_stream_authenticated(
                &protocol,
                protocol.authenticated_session().unwrap(),
            )
            .unwrap();
    }

    #[tokio::test]
    async fn runtime_startup_rejects_mismatched_rpc_role_without_advancing() {
        let (rpc_url, endpoints, capabilities, applied) = applied_runtime_evidence().await;
        let mut client = test_client();
        client.config.network.base_url_rpc_submission = Some(rpc_url.clone());
        client.config.network.base_url_rpc_watch = Some("http://127.0.0.1:1".to_string());
        client.config.network.base_url_rpc_recovery = Some(rpc_url.clone());
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();

        assert_eq!(
            client.record_runtime_validated(&applied, &endpoints, &capabilities),
            Err(DeepXExecutionStartupError::RuntimeRpcEndpointMismatch(
                DeepXRpcRole::Watch,
            )),
        );

        client.config.network.base_url_rpc_watch = Some(rpc_url);
        assert!(
            client
                .record_runtime_validated(&applied, &endpoints, &capabilities)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn runtime_startup_rejects_capabilities_from_another_endpoint_without_advancing() {
        let (rpc_url, endpoints, _capabilities, applied) = applied_runtime_evidence().await;
        let (_, _, other_capabilities, _) = applied_runtime_evidence().await;
        let mut client = test_client();
        client.config.network.base_url_rpc_submission = Some(rpc_url.clone());
        client.config.network.base_url_rpc_watch = Some(rpc_url.clone());
        client.config.network.base_url_rpc_recovery = Some(rpc_url);
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();

        assert_eq!(
            client.record_runtime_validated(&applied, &endpoints, &other_capabilities),
            Err(DeepXExecutionStartupError::RuntimeRpcCapabilitiesMismatch(
                DeepXRpcRole::Submission,
            )),
        );
        assert_eq!(
            client
                .startup
                .validate_next(DeepXExecutionStartupEvidence::RuntimeValidated),
            Ok(()),
        );
    }

    #[tokio::test]
    async fn runtime_startup_rejects_unsupported_backend_without_advancing() {
        let (rpc_url, endpoints, capabilities, applied) = applied_runtime_evidence().await;
        let mut client = test_client();
        client.config.execution_backend = DeepXExecutionBackend::LegacyEvm;
        client.config.network.base_url_rpc_submission = Some(rpc_url.clone());
        client.config.network.base_url_rpc_watch = Some(rpc_url.clone());
        client.config.network.base_url_rpc_recovery = Some(rpc_url);
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();

        assert_eq!(
            client.record_runtime_validated(&applied, &endpoints, &capabilities),
            Err(DeepXExecutionStartupError::UnsupportedRuntimeBackend(
                DeepXExecutionBackend::LegacyEvm,
            )),
        );

        client.config.execution_backend = DeepXExecutionBackend::DirectPallet;
        assert!(
            client
                .record_runtime_validated(&applied, &endpoints, &capabilities)
                .is_ok()
        );
    }

    #[rstest]
    fn instrument_startup_rejects_uninitialized_market_catalog_without_advancing() {
        let mut client = test_client();
        let http_client =
            crate::http::DeepXHttpClient::new("https://api.testnet.deepx.trade", Some(5), None)
                .unwrap();
        let provider = DeepXMarketProvider::new(http_client);

        assert_eq!(
            client.record_instruments_loaded(&provider),
            Err(DeepXExecutionStartupError::MarketCatalogNotInitialized),
        );
        assert_eq!(
            client.restore_order_contexts([]),
            Err(DeepXOrderContextRestorationError::Startup(
                DeepXExecutionStartupError::OutOfOrder {
                    expected: DeepXExecutionStartupEvidence::InstrumentsLoaded,
                    received: DeepXExecutionStartupEvidence::OrderContextRestored,
                },
            )),
        );
    }

    #[tokio::test]
    async fn instrument_startup_accepts_complete_market_catalog() {
        const SPOT_RESPONSE: &str = include_str!("../test_data/http/testnet/spot_markets.json");
        const PERP_RESPONSE: &str = include_str!("../test_data/http/testnet/perp_markets.json");
        let router = Router::new()
            .route(
                "/internal/v1/market/spot/markets",
                get(|| async { SPOT_RESPONSE }),
            )
            .route(
                "/internal/v1/market/perp/markets",
                get(|| async { PERP_RESPONSE }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let http_client =
            crate::http::DeepXHttpClient::new(format!("http://{address}"), Some(5), None).unwrap();
        let mut provider = DeepXMarketProvider::new(http_client);
        provider.load_all().await.unwrap();
        let mut client = test_client();
        client.config.network.base_url_rest = Some(format!("http://{address}/"));

        client.record_instruments_loaded(&provider).unwrap();

        assert!(client.restore_order_contexts([]).is_ok());
    }

    #[tokio::test]
    async fn instrument_startup_rejects_empty_market_catalog_without_advancing() {
        const EMPTY_RESPONSE: &str = r#"{"code":200,"msg":"success","data":[],"fail":false}"#;
        let router = Router::new()
            .route(
                "/internal/v1/market/spot/markets",
                get(|| async { EMPTY_RESPONSE }),
            )
            .route(
                "/internal/v1/market/perp/markets",
                get(|| async { EMPTY_RESPONSE }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let http_client =
            crate::http::DeepXHttpClient::new(format!("http://{address}"), Some(5), None).unwrap();
        let mut provider = DeepXMarketProvider::new(http_client);
        provider.load_all().await.unwrap();
        let mut client = test_client();
        client.config.network.base_url_rest = Some(format!("http://{address}"));

        assert!(provider.initialized());
        assert!(provider.instrument_ids().is_empty());
        assert_eq!(
            client.record_instruments_loaded(&provider),
            Err(DeepXExecutionStartupError::MarketCatalogEmpty),
        );
        assert_eq!(
            client.restore_order_contexts([]),
            Err(DeepXOrderContextRestorationError::Startup(
                DeepXExecutionStartupError::OutOfOrder {
                    expected: DeepXExecutionStartupEvidence::InstrumentsLoaded,
                    received: DeepXExecutionStartupEvidence::OrderContextRestored,
                },
            )),
        );
    }

    #[tokio::test]
    async fn instrument_startup_rejects_unconfigured_rest_endpoint_without_advancing() {
        const SPOT_RESPONSE: &str = include_str!("../test_data/http/testnet/spot_markets.json");
        const PERP_RESPONSE: &str = include_str!("../test_data/http/testnet/perp_markets.json");
        let router = Router::new()
            .route(
                "/internal/v1/market/spot/markets",
                get(|| async { SPOT_RESPONSE }),
            )
            .route(
                "/internal/v1/market/perp/markets",
                get(|| async { PERP_RESPONSE }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let http_client =
            crate::http::DeepXHttpClient::new(format!("http://{address}"), Some(5), None).unwrap();
        let mut provider = DeepXMarketProvider::new(http_client);
        provider.load_all().await.unwrap();
        let mut client = test_client();

        assert_eq!(
            client.record_instruments_loaded(&provider),
            Err(DeepXExecutionStartupError::MarketCatalogEndpointMismatch),
        );
        assert_eq!(
            client.restore_order_contexts([]),
            Err(DeepXOrderContextRestorationError::Startup(
                DeepXExecutionStartupError::OutOfOrder {
                    expected: DeepXExecutionStartupEvidence::InstrumentsLoaded,
                    received: DeepXExecutionStartupEvidence::OrderContextRestored,
                },
            )),
        );
    }

    #[tokio::test]
    async fn instrument_startup_rejects_unconfigured_failover_endpoint() {
        const SPOT_RESPONSE: &str = include_str!("../test_data/http/testnet/spot_markets.json");
        const PERP_RESPONSE: &str = include_str!("../test_data/http/testnet/perp_markets.json");
        let router = Router::new()
            .route(
                "/internal/v1/market/spot/markets",
                get(|| async { SPOT_RESPONSE }),
            )
            .route(
                "/internal/v1/market/perp/markets",
                get(|| async { PERP_RESPONSE }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let primary = format!("http://{address}");
        let http_client = crate::http::DeepXHttpClient::new_with_endpoints(
            [primary.clone(), "https://other.example.invalid".to_string()],
            Some(5),
            None,
            crate::http::deepx_http_retry_config(),
        )
        .unwrap();
        let mut provider = DeepXMarketProvider::new(http_client);
        provider.load_all().await.unwrap();
        let mut client = test_client();
        client.config.network.base_urls_rest = Some(vec![
            primary,
            "https://configured.example.invalid".to_string(),
        ]);

        assert_eq!(
            client.record_instruments_loaded(&provider),
            Err(DeepXExecutionStartupError::MarketCatalogEndpointMismatch),
        );
    }

    fn advance_through_mass_reconciliation(
        client: &mut DeepXExecutionClient,
    ) -> (
        AccountState,
        DeepXWsProtocolCore,
        DeepXWsAuthenticatedSession,
    ) {
        record_instruments_loaded(client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        let (protocol, session) = authenticated_protocol();
        client
            .record_private_stream_authenticated(&protocol, session)
            .unwrap();
        let frame = authenticated_account_frame(&protocol, session);
        let state = test_account_state();
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);
        client
            .record_account_state_initialized(&protocol, &frame, &state)
            .unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::MassReconciliationCompleted)
            .unwrap();
        (state, protocol, session)
    }

    fn advance_to_mass_reconciliation(
        client: &mut DeepXExecutionClient,
    ) -> (DeepXWsProtocolCore, DeepXWsAuthenticatedSession) {
        record_instruments_loaded(client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        let (protocol, session) = authenticated_protocol();
        client
            .record_private_stream_authenticated(&protocol, session)
            .unwrap();
        let frame = authenticated_account_frame(&protocol, session);
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);
        client
            .record_account_state_initialized(&protocol, &frame, &test_account_state())
            .unwrap();
        (protocol, session)
    }

    #[tokio::test]
    async fn mass_reconciliation_accepts_empty_complete_store_snapshot() {
        let mut client = test_client();
        let (rpc_url, endpoints, capabilities, _) = applied_runtime_evidence().await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client);
        let store = TestTransactionStore {
            restored: Vec::new(),
        };
        let signer = derive_signer_account_id(&client.credential).unwrap();
        let lease = store.acquire_signer_lease(signer).await.unwrap();

        client
            .record_mass_reconciliation_completed(
                &protocol,
                session,
                &endpoints,
                &capabilities,
                &store,
                &lease,
            )
            .await
            .unwrap();

        assert!(matches!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::AccountStateNotRegistered { .. }),
        ));
    }

    #[tokio::test]
    async fn mass_reconciliation_rejects_another_signer_without_advancing() {
        let mut client = test_client();
        let (rpc_url, endpoints, capabilities, _) = applied_runtime_evidence().await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client);
        let store = TestTransactionStore {
            restored: Vec::new(),
        };
        let lease = TestSignerLease { signer: [42; 20] };

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(
                    &protocol,
                    session,
                    &endpoints,
                    &capabilities,
                    &store,
                    &lease,
                )
                .await,
            Err(DeepXMassReconciliationError::SignerLeaseMismatch),
        ));
        assert!(matches!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::MassReconciliationCompleted,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        ));
    }

    #[tokio::test]
    async fn mass_reconciliation_rejects_durable_record_from_another_genesis_without_advancing() {
        let mut client = test_client();
        let (rpc_url, endpoints, capabilities, _) = applied_runtime_evidence().await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client);
        let signer = derive_signer_account_id(&client.credential).unwrap();
        let record = DeepXTransactionRecord::created(DeepXTransactionIdentity::new(
            ClientOrderId::from("O-DEEPX-FOREIGN-GENESIS"),
            signer,
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId { value: 42 },
            DeepXDirectRuntimeIdentity {
                genesis_hash: [1; 32],
                metadata_sha256: [2; 32],
                spec_version: 366,
                transaction_version: 1,
                signed_extensions: vec!["CheckNonce".to_string()],
            },
        ));
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(1),
        )
        .unwrap();
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let store = TestTransactionStore {
            restored: vec![restored],
        };
        let lease = TestSignerLease { signer };

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(
                    &protocol,
                    session,
                    &endpoints,
                    &capabilities,
                    &store,
                    &lease,
                )
                .await,
            Err(DeepXMassReconciliationError::RuntimeGenesisMismatch {
                client_order_id,
                expected_genesis_hash,
                received_genesis_hash,
            }) if client_order_id == "O-DEEPX-FOREIGN-GENESIS"
                && expected_genesis_hash == endpoints.genesis_hash()
                && received_genesis_hash == [1; 32],
        ));
        assert!(matches!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::MassReconciliationCompleted,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        ));
    }

    #[tokio::test]
    async fn mass_reconciliation_rejects_unresolved_durable_transaction_without_advancing() {
        let mut client = test_client();
        let (rpc_url, endpoints, capabilities, _) = applied_runtime_evidence().await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client);
        let signer = derive_signer_account_id(&client.credential).unwrap();
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let record = DeepXTransactionRecord::created(DeepXTransactionIdentity::new(
            ClientOrderId::from("O-DEEPX-UNRESOLVED"),
            signer,
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId { value: 42 },
            DeepXDirectRuntimeIdentity {
                genesis_hash,
                metadata_sha256: [2; 32],
                spec_version: 366,
                transaction_version: 1,
                signed_extensions: vec!["CheckNonce".to_string()],
            },
        ));
        let committed = DeepXCommittedTransactionRecord::acknowledge_committed(
            &record,
            DeepXTransactionRevision::new(1),
        )
        .unwrap();
        let restored = DeepXRestoredTransactionRecord::new(record, committed).unwrap();
        let store = TestTransactionStore {
            restored: vec![restored],
        };
        let lease = TestSignerLease { signer };

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(
                    &protocol,
                    session,
                    &endpoints,
                    &capabilities,
                    &store,
                    &lease,
                )
                .await,
            Err(DeepXMassReconciliationError::UnresolvedTransaction {
                action: DeepXTransactionRecoveryAction::RecreateSigningInputs,
                ..
            }),
        ));
        assert!(matches!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::MassReconciliationCompleted,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        ));
    }

    #[tokio::test]
    async fn mass_reconciliation_commits_pending_pool_acceptance() {
        let mut client = test_client();
        let record = submitting_record(&client);
        let signed_bytes = record.signed_extrinsic().unwrap().bytes().to_vec();
        let (rpc_url, endpoints, capabilities) = recovery_evidence(&[&signed_bytes]).await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client);
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(
                    &protocol,
                    session,
                    &endpoints,
                    &capabilities,
                    &store,
                    &lease,
                )
                .await,
            Err(DeepXMassReconciliationError::UnresolvedTransaction {
                action: DeepXTransactionRecoveryAction::ReconciliationRequired,
                ..
            }),
        ));

        assert_eq!(store.current_revision(), 5);
        assert_eq!(
            store.persisted_record().lifecycle().state(),
            DeepXTransactionState::Accepted,
        );
    }

    #[tokio::test]
    async fn mass_reconciliation_preserves_submitting_record_when_pool_is_absent() {
        let mut client = test_client();
        let record = submitting_record(&client);
        let (rpc_url, endpoints, capabilities) = recovery_evidence(&[&[8, 99, 98]]).await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client);
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(
                    &protocol,
                    session,
                    &endpoints,
                    &capabilities,
                    &store,
                    &lease,
                )
                .await,
            Err(DeepXMassReconciliationError::UnresolvedTransaction {
                action: DeepXTransactionRecoveryAction::ReconciliationRequired,
                ..
            }),
        ));

        assert_eq!(store.current_revision(), 4);
        assert_eq!(
            store.persisted_record().lifecycle().state(),
            DeepXTransactionState::Submitting,
        );
    }

    #[tokio::test]
    async fn mass_reconciliation_finalizes_restored_in_block_transaction() {
        let mut client = test_client();
        let record = in_block_record(&client);
        let signed_bytes = record.signed_extrinsic().unwrap().bytes().to_vec();
        let (rpc_url, endpoints, capabilities, canonical_requests) =
            finality_evidence(72, &signed_bytes).await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client);
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        client
            .record_mass_reconciliation_completed(
                &protocol,
                session,
                &endpoints,
                &capabilities,
                &store,
                &lease,
            )
            .await
            .unwrap();

        assert_eq!(store.current_revision(), 5);
        assert_eq!(canonical_requests.load(Ordering::Relaxed), 2);
        assert!(matches!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::AccountStateNotRegistered { .. }),
        ));
    }

    #[tokio::test]
    async fn mass_reconciliation_keeps_pending_in_block_transaction_unresolved() {
        let mut client = test_client();
        let record = in_block_record(&client);
        let signed_bytes = record.signed_extrinsic().unwrap().bytes().to_vec();
        let (rpc_url, endpoints, capabilities, canonical_requests) =
            finality_evidence(71, &signed_bytes).await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client);
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(
                    &protocol,
                    session,
                    &endpoints,
                    &capabilities,
                    &store,
                    &lease,
                )
                .await,
            Err(DeepXMassReconciliationError::UnresolvedTransaction {
                action: DeepXTransactionRecoveryAction::ReconciliationRequired,
                ..
            }),
        ));
        assert_eq!(store.current_revision(), 4);
        assert_eq!(canonical_requests.load(Ordering::Relaxed), 0);
        assert!(matches!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::MassReconciliationCompleted,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        ));
    }

    #[tokio::test]
    async fn mass_reconciliation_commits_reorganized_in_block_transaction() {
        let mut client = test_client();
        let record = in_block_record(&client);
        let inclusion = record.lifecycle().inclusion().unwrap();
        let signed_bytes = record.signed_extrinsic().unwrap().bytes().to_vec();
        let (rpc_url, endpoints, capabilities, canonical_requests) =
            finality_evidence_with_hash(72, [9; 32], &signed_bytes).await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client);
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(
                    &protocol,
                    session,
                    &endpoints,
                    &capabilities,
                    &store,
                    &lease,
                )
                .await,
            Err(DeepXMassReconciliationError::UnresolvedTransaction {
                action: DeepXTransactionRecoveryAction::ReconciliationRequired,
                ..
            }),
        ));

        let persisted = store.persisted_record();
        assert_eq!(store.current_revision(), 5);
        assert_eq!(
            persisted.lifecycle().state(),
            DeepXTransactionState::Submitting
        );
        assert_eq!(persisted.lifecycle().inclusion(), None);
        assert_eq!(persisted.lifecycle().reverted_inclusion(), Some(inclusion));
        assert_eq!(canonical_requests.load(Ordering::Relaxed), 4);
        assert!(matches!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::MassReconciliationCompleted,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        ));
    }

    #[tokio::test]
    async fn mass_reconciliation_requires_action_for_not_included_pool_conflict() {
        let mut client = test_client();
        let record = not_included_record(&client);
        let signed_bytes = record.signed_extrinsic().unwrap().bytes().to_vec();
        let (rpc_url, endpoints, capabilities) = recovery_evidence(&[&signed_bytes]).await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client);
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(
                    &protocol,
                    session,
                    &endpoints,
                    &capabilities,
                    &store,
                    &lease,
                )
                .await,
            Err(DeepXMassReconciliationError::UnresolvedTransaction {
                action: DeepXTransactionRecoveryAction::OperatorActionRequired,
                ..
            }),
        ));

        assert_eq!(store.current_revision(), 5);
        assert_eq!(
            store.persisted_record().lifecycle().state(),
            DeepXTransactionState::ActionRequired,
        );
        assert!(matches!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::MassReconciliationCompleted,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        ));
    }

    #[tokio::test]
    async fn mass_reconciliation_requires_action_after_non_atomic_pool_absence() {
        let mut client = test_client();
        let record = not_included_record(&client);
        let (rpc_url, endpoints, capabilities) = recovery_evidence(&[]).await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client);
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(
                    &protocol,
                    session,
                    &endpoints,
                    &capabilities,
                    &store,
                    &lease,
                )
                .await,
            Err(DeepXMassReconciliationError::UnresolvedTransaction {
                action: DeepXTransactionRecoveryAction::OperatorActionRequired,
                ..
            }),
        ));

        assert_eq!(store.current_revision(), 5);
        assert_eq!(
            store.persisted_record().lifecycle().state(),
            DeepXTransactionState::ActionRequired,
        );
        assert!(matches!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::MassReconciliationCompleted,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        ));
    }

    #[tokio::test]
    async fn mass_reconciliation_rejects_stale_session_without_mutation() {
        let mut client = test_client();
        let record = submitting_record(&client);
        let signed_bytes = record.signed_extrinsic().unwrap().bytes().to_vec();
        let (rpc_url, endpoints, capabilities) = recovery_evidence(&[&signed_bytes]).await;
        configure_rpc_url(&mut client, rpc_url);
        let (mut protocol, session) = advance_to_mass_reconciliation(&mut client);
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        protocol.reset_after_reconnect(1, "test reconnect").unwrap();

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(
                    &protocol,
                    session,
                    &endpoints,
                    &capabilities,
                    &store,
                    &lease,
                )
                .await,
            Err(DeepXMassReconciliationError::Startup(
                DeepXExecutionStartupError::PrivateStreamAuthenticationMismatch,
            )),
        ));
        assert_eq!(store.current_revision(), 4);
        assert_eq!(
            store.persisted_record().lifecycle().state(),
            DeepXTransactionState::Submitting,
        );
        assert_eq!(client.startup.completed_steps, 5);
        assert!(!client.is_connected());
    }

    fn test_account_state() -> AccountState {
        AccountState::new(
            AccountId::from("DEEPX-001"),
            AccountType::Margin,
            vec![],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            None,
        )
    }

    fn authenticated_protocol() -> (DeepXWsProtocolCore, DeepXWsAuthenticatedSession) {
        let mut protocol = DeepXWsProtocolCore::new('/');
        let (attempt, _) = protocol.begin_authentication().unwrap();
        assert!(protocol.complete_authentication(attempt));
        let session = protocol.authenticated_session().unwrap();
        (protocol, session)
    }

    fn authenticated_account_frame(
        protocol: &DeepXWsProtocolCore,
        session: DeepXWsAuthenticatedSession,
    ) -> DeepXWsAuthenticatedFrame {
        protocol
            .admit_authenticated_frame(
                session.connection_epoch(),
                session,
                DeepXWsFrame::parse(r#"{"channel":"account","data":{}}"#).unwrap(),
            )
            .unwrap()
    }

    fn reconciliation_fill_report(trade_id: &str, ts_event: u64) -> FillReport {
        FillReport::new(
            AccountId::from("DEEPX-001"),
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            VenueOrderId::from("venue-order-1"),
            TradeId::from(trade_id),
            OrderSide::Buy,
            Quantity::from("0.10"),
            Price::from("2500.00"),
            Money::from("0.25 USDC"),
            LiquiditySide::Taker,
            Some(ClientOrderId::from("client-order-1")),
            None,
            UnixNanos::from(ts_event),
            UnixNanos::from(ts_event + 100),
            None,
        )
    }

    fn reconciliation_order_report(
        client_order_id: &str,
        venue_order_id: &str,
        ts_last: u64,
    ) -> OrderStatusReport {
        OrderStatusReport::new(
            AccountId::from("DEEPX-001"),
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            Some(ClientOrderId::from(client_order_id)),
            VenueOrderId::from(venue_order_id),
            Some(OrderSide::Buy),
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Accepted,
            Quantity::from("0.10"),
            Quantity::zero(2),
            UnixNanos::from(10),
            UnixNanos::from(ts_last),
            UnixNanos::from(ts_last + 100),
            None,
        )
    }

    #[rstest]
    fn order_report_merge_deduplicates_overlap_ignoring_local_identity() {
        let client = test_client();
        let first = reconciliation_order_report("client-order-1", "venue-order-1", 20);
        let mut replay = first.clone();
        replay.report_id = UUID4::new();
        replay.ts_init = UnixNanos::from(999);

        let merged = client
            .merge_validated_order_reports([first.clone(), replay])
            .unwrap();

        assert_eq!(merged, vec![first]);
    }

    #[rstest]
    fn order_report_merge_orders_deterministically_by_venue_identity() {
        let client = test_client();
        let second = reconciliation_order_report("client-order-2", "venue-order-2", 10);
        let first = reconciliation_order_report("client-order-1", "venue-order-1", 20);

        let merged = client
            .merge_validated_order_reports([second, first])
            .unwrap();
        let identities: Vec<_> = merged.iter().map(|report| report.venue_order_id).collect();

        assert_eq!(
            identities,
            vec![
                VenueOrderId::from("venue-order-1"),
                VenueOrderId::from("venue-order-2"),
            ],
        );
    }

    #[rstest]
    fn order_report_merge_rejects_conflicting_venue_order_evidence() {
        let client = test_client();
        let first = reconciliation_order_report("client-order-1", "venue-order-1", 20);
        let mut conflicting = first.clone();
        conflicting.filled_qty = Quantity::from("0.01");

        assert_eq!(
            client.merge_validated_order_reports([first, conflicting]),
            Err(DeepXOrderReportMergeError::ConflictingOrder(
                VenueOrderId::from("venue-order-1"),
            )),
        );
    }

    #[rstest]
    fn order_report_merge_rejects_client_identity_split() {
        let client = test_client();
        let first = reconciliation_order_report("client-order-1", "venue-order-1", 10);
        let second = reconciliation_order_report("client-order-1", "venue-order-2", 20);

        assert_eq!(
            client.merge_validated_order_reports([second, first]),
            Err(DeepXOrderReportMergeError::ClientOrderIdentitySplit {
                client_order_id: ClientOrderId::from("client-order-1"),
                first_venue_order_id: VenueOrderId::from("venue-order-1"),
                second_venue_order_id: VenueOrderId::from("venue-order-2"),
            }),
        );
    }

    #[rstest]
    fn order_report_merge_is_permutation_invariant() {
        let client = test_client();
        let first = reconciliation_order_report("client-order-1", "venue-order-1", 20);
        let mut replay = first.clone();
        replay.report_id = UUID4::new();
        replay.ts_init = UnixNanos::from(999);

        let forward = client
            .merge_validated_order_reports([first.clone(), replay.clone()])
            .unwrap();
        let reverse = client
            .merge_validated_order_reports([replay, first])
            .unwrap();

        assert_eq!(forward, reverse);
    }

    #[rstest]
    fn order_report_merge_error_precedence_is_permutation_invariant() {
        let client = test_client();
        let mut foreign_account =
            reconciliation_order_report("client-order-1", "venue-order-1", 20);
        foreign_account.account_id = AccountId::from("DEEPX-002");
        let mut foreign_venue = reconciliation_order_report("client-order-2", "venue-order-2", 10);
        foreign_venue.instrument_id = InstrumentId::from("ETH-USDC-PERP.OTHER");
        let expected = Err(DeepXOrderReportMergeError::AccountMismatch {
            expected: AccountId::from("DEEPX-001"),
            received: AccountId::from("DEEPX-002"),
            venue_order_id: VenueOrderId::from("venue-order-1"),
        });

        assert_eq!(
            client.merge_validated_order_reports([foreign_account.clone(), foreign_venue.clone(),]),
            expected,
        );
        assert_eq!(
            client.merge_validated_order_reports([foreign_venue, foreign_account]),
            expected,
        );
    }

    #[tokio::test]
    async fn order_report_merge_keeps_generation_unsupported() {
        let client = test_client();
        let command = GenerateOrderStatusReport::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
            None,
            None,
            None,
        );

        let error = ExecutionClient::generate_order_status_report(&client, &command)
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "DeepX order status reports are not operational",
        );
    }

    #[rstest]
    fn fill_report_merge_deduplicates_page_overlap_ignoring_local_identity() {
        let client = test_client();
        let first = reconciliation_fill_report("trade-1", 20);
        let mut replay = first.clone();
        replay.report_id = UUID4::new();
        replay.ts_init = UnixNanos::from(999);

        let merged = client
            .merge_validated_fill_reports([first.clone(), replay])
            .unwrap();

        assert_eq!(merged, vec![first]);
    }

    #[rstest]
    fn fill_report_merge_orders_deterministically_by_event_and_trade_identity() {
        let client = test_client();
        let later = reconciliation_fill_report("trade-2", 20);
        let same_time_later_identity = reconciliation_fill_report("trade-3", 10);
        let earlier_identity = reconciliation_fill_report("trade-1", 10);

        let merged = client
            .merge_validated_fill_reports([later, same_time_later_identity, earlier_identity])
            .unwrap();
        let identities: Vec<_> = merged
            .iter()
            .map(|report| (report.ts_event, report.trade_id))
            .collect();

        assert_eq!(
            identities,
            vec![
                (UnixNanos::from(10), TradeId::from("trade-1")),
                (UnixNanos::from(10), TradeId::from("trade-3")),
                (UnixNanos::from(20), TradeId::from("trade-2")),
            ],
        );
    }

    #[rstest]
    fn fill_report_merge_rejects_conflicting_trade_evidence() {
        let client = test_client();
        let first = reconciliation_fill_report("trade-1", 20);
        let mut conflicting = first.clone();
        conflicting.last_qty = Quantity::from("0.20");

        assert_eq!(
            client.merge_validated_fill_reports([first, conflicting]),
            Err(DeepXFillReportMergeError::ConflictingTrade(TradeId::from(
                "trade-1"
            ))),
        );
    }

    #[rstest]
    fn fill_report_merge_rejects_foreign_account() {
        let client = test_client();
        let mut report = reconciliation_fill_report("trade-1", 20);
        report.account_id = AccountId::from("DEEPX-002");

        assert_eq!(
            client.merge_validated_fill_reports([report]),
            Err(DeepXFillReportMergeError::AccountMismatch {
                expected: AccountId::from("DEEPX-001"),
                received: AccountId::from("DEEPX-002"),
                trade_id: TradeId::from("trade-1"),
            }),
        );
    }

    #[rstest]
    fn fill_report_merge_rejects_foreign_venue() {
        let client = test_client();
        let mut report = reconciliation_fill_report("trade-1", 20);
        report.instrument_id = InstrumentId::from("ETH-USDC-PERP.OTHER");

        assert_eq!(
            client.merge_validated_fill_reports([report]),
            Err(DeepXFillReportMergeError::InstrumentVenueMismatch {
                trade_id: TradeId::from("trade-1"),
                venue: Venue::from("OTHER"),
            }),
        );
    }

    #[rstest]
    fn fill_report_merge_is_permutation_invariant() {
        let client = test_client();
        let first = reconciliation_fill_report("trade-1", 20);
        let mut replay = first.clone();
        replay.report_id = UUID4::new();
        replay.ts_init = UnixNanos::from(999);

        let forward = client
            .merge_validated_fill_reports([first.clone(), replay.clone()])
            .unwrap();
        let reverse = client
            .merge_validated_fill_reports([replay, first])
            .unwrap();

        assert_eq!(forward, reverse);
    }

    #[rstest]
    fn fill_report_merge_error_precedence_is_permutation_invariant() {
        let client = test_client();
        let mut foreign_account = reconciliation_fill_report("trade-1", 20);
        foreign_account.account_id = AccountId::from("DEEPX-002");
        let mut foreign_venue = reconciliation_fill_report("trade-2", 10);
        foreign_venue.instrument_id = InstrumentId::from("ETH-USDC-PERP.OTHER");
        let expected = Err(DeepXFillReportMergeError::AccountMismatch {
            expected: AccountId::from("DEEPX-001"),
            received: AccountId::from("DEEPX-002"),
            trade_id: TradeId::from("trade-1"),
        });

        assert_eq!(
            client.merge_validated_fill_reports([foreign_account.clone(), foreign_venue.clone()]),
            expected,
        );
        assert_eq!(
            client.merge_validated_fill_reports([foreign_venue, foreign_account]),
            expected,
        );
    }

    #[tokio::test]
    async fn fill_report_generation_remains_unsupported() {
        let client = test_client();
        let command = GenerateFillReports::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
            None,
            None,
            None,
            None,
        );

        let error = ExecutionClient::generate_fill_reports(&client, command)
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "DeepX fill reports are not operational");
    }

    #[tokio::test]
    async fn mass_status_generation_remains_unsupported() {
        let client = test_client();

        let error = ExecutionClient::generate_mass_status(&client, Some(u64::MAX))
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "DeepX mass status reports are not operational",
        );
    }

    #[rstest]
    fn commission_calculation_remains_unsupported() {
        let client = test_client();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());

        let error = ExecutionClient::calculate_commission(
            &client,
            &instrument,
            Quantity::from("1.000"),
            Price::from("2500.00"),
            LiquiditySide::Taker,
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "DeepX commission calculation is not operational",
        );
    }

    fn register_test_account(cache: &Rc<RefCell<Cache>>, state: AccountState) {
        cache
            .borrow_mut()
            .add_account(AccountAny::Margin(MarginAccount::new(state, false)))
            .unwrap();
    }

    #[rstest]
    fn trade_id_is_suppressed_only_after_commit() {
        let dedup = DeepXTradeDedup::<4>::default();
        let trade_id = TradeId::from("T-DEEPX-001");

        dedup.reserve(trade_id).unwrap().unwrap().commit().unwrap();

        assert!(dedup.reserve(trade_id).unwrap().is_none());
    }

    #[rstest]
    fn uncommitted_trade_id_reservation_is_released() {
        let dedup = DeepXTradeDedup::<4>::default();
        let trade_id = TradeId::from("T-DEEPX-001");

        drop(dedup.reserve(trade_id).unwrap().unwrap());

        assert!(dedup.reserve(trade_id).unwrap().is_some());
    }

    #[rstest]
    fn active_trade_id_reservation_suppresses_duplicate() {
        let dedup = DeepXTradeDedup::<4>::default();
        let trade_id = TradeId::from("T-DEEPX-001");
        let reservation = dedup.reserve(trade_id).unwrap().unwrap();

        assert!(dedup.reserve(trade_id).unwrap().is_none());

        drop(reservation);
        assert!(dedup.reserve(trade_id).unwrap().is_some());
    }

    #[rstest]
    fn different_trade_ids_are_independent() {
        let dedup = DeepXTradeDedup::<4>::default();
        let first = TradeId::from("T-DEEPX-001");
        let second = TradeId::from("T-DEEPX-002");

        dedup.reserve(first).unwrap().unwrap().commit().unwrap();

        assert!(dedup.reserve(second).unwrap().is_some());
    }

    #[rstest]
    fn trade_id_dedup_is_retained_across_startup_reset() {
        let mut client = test_client();
        let trade_id = TradeId::from("T-DEEPX-001");
        client
            .reserve_trade_id(trade_id)
            .unwrap()
            .unwrap()
            .commit()
            .unwrap();

        client.reset_startup();

        assert!(client.reserve_trade_id(trade_id).unwrap().is_none());
    }

    #[rstest]
    fn oldest_trade_id_becomes_eligible_after_capacity_eviction() {
        let dedup = DeepXTradeDedup::<2>::default();
        let first = TradeId::from("T-DEEPX-001");
        let second = TradeId::from("T-DEEPX-002");
        let third = TradeId::from("T-DEEPX-003");
        dedup.reserve(first).unwrap().unwrap().commit().unwrap();
        dedup.reserve(second).unwrap().unwrap().commit().unwrap();

        dedup.reserve(third).unwrap().unwrap().commit().unwrap();

        assert!(dedup.reserve(first).unwrap().is_some());
        assert!(dedup.reserve(second).unwrap().is_none());
        assert!(dedup.reserve(third).unwrap().is_none());
    }

    #[rstest]
    fn poisoned_trade_dedup_lock_returns_typed_error() {
        let dedup = DeepXTradeDedup::<4>::default();
        let _ = std::panic::catch_unwind(|| {
            let _state = dedup.state.lock().unwrap();
            panic!("poison trade dedup lock");
        });

        assert_eq!(
            dedup.reserve(TradeId::from("T-DEEPX-001")).unwrap_err(),
            DeepXTradeDedupError::LockPoisoned,
        );
    }

    #[rstest]
    fn order_context_registration_routes_exact_context() {
        let registry = DeepXOrderContextRegistry::default();
        let order = test_order("1.250");
        let expected = OrderContext::from(&order);

        registry.register(expected).unwrap();

        assert_eq!(
            registry
                .route(Some(expected.identity.client_order_id), None)
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(expected),
        );
    }

    #[rstest]
    fn identical_order_context_registration_is_idempotent() {
        let registry = DeepXOrderContextRegistry::default();
        let context = OrderContext::from(&test_order("1.250"));

        registry.register(context).unwrap();
        registry.register(context).unwrap();

        assert_eq!(
            registry
                .route(Some(context.identity.client_order_id), None)
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(context),
        );
    }

    #[rstest]
    fn conflicting_order_context_preserves_original() {
        let registry = DeepXOrderContextRegistry::default();
        let original = OrderContext::from(&test_order("1.250"));
        let conflicting = OrderContext::from(&test_order("2.500"));
        registry.register(original).unwrap();

        assert_eq!(
            registry.register(conflicting),
            Err(DeepXOrderContextError::Conflict(
                original.identity.client_order_id
            )),
        );
        assert_eq!(
            registry
                .route(Some(original.identity.client_order_id), None)
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(original),
        );
    }

    #[rstest]
    fn missing_order_context_routes_external() {
        let registry = DeepXOrderContextRegistry::default();

        assert_eq!(
            registry.route(None, None).unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
        assert_eq!(
            registry
                .route(Some(ClientOrderId::from("O-DEEPX-UNKNOWN")), None)
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
    }

    #[rstest]
    fn finished_order_context_routes_as_terminal() {
        let registry = DeepXOrderContextRegistry::default();
        let context = OrderContext::from(&test_order("1.250"));
        registry.register(context).unwrap();

        registry.finish(&context.identity.client_order_id).unwrap();

        assert_eq!(
            registry
                .route(Some(context.identity.client_order_id), None)
                .unwrap(),
            DeepXExecutionUpdateRoute::Terminal(context),
        );
    }

    #[rstest]
    fn tracked_venue_order_identity_survives_terminal_transition() {
        let client = test_client();
        let context = OrderContext::from(&test_order("1.250"));
        let venue_order_id = VenueOrderId::from("V-DEEPX-001");
        client.register_order_context(context).unwrap();
        client
            .bind_tracked_venue_order_id(context.identity.client_order_id, venue_order_id)
            .unwrap();

        assert_eq!(
            client
                .route_execution_update_identity(None, Some(venue_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(context),
        );

        client
            .finish_order_context(&context.identity.client_order_id)
            .unwrap();

        assert_eq!(
            client
                .route_execution_update_identity(None, Some(venue_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Terminal(context),
        );
    }

    #[rstest]
    fn matching_tracked_client_and_venue_identities_route_to_same_order() {
        let client = test_client();
        let context = OrderContext::from(&test_order("1.250"));
        let venue_order_id = VenueOrderId::from("V-DEEPX-001");
        client.register_order_context(context).unwrap();
        client
            .bind_tracked_venue_order_id(context.identity.client_order_id, venue_order_id)
            .unwrap();

        assert_eq!(
            client
                .route_execution_update_identity(
                    Some(context.identity.client_order_id),
                    Some(venue_order_id),
                )
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(context),
        );
    }

    #[rstest]
    fn conflicting_tracked_venue_binding_preserves_original() {
        let client = test_client();
        let context = OrderContext::from(&test_order("1.250"));
        let original = VenueOrderId::from("V-DEEPX-001");
        let conflicting = VenueOrderId::from("V-DEEPX-002");
        client.register_order_context(context).unwrap();
        client
            .bind_tracked_venue_order_id(context.identity.client_order_id, original)
            .unwrap();

        assert_eq!(
            client.bind_tracked_venue_order_id(context.identity.client_order_id, conflicting,),
            Err(DeepXOrderContextError::VenueOrderBindingConflict {
                client_order_id: context.identity.client_order_id,
                venue_order_id: original,
            }),
        );
        assert_eq!(
            client
                .route_execution_update_identity(None, Some(original))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(context),
        );
        assert_eq!(
            client
                .route_execution_update_identity(None, Some(conflicting))
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
    }

    #[rstest]
    fn repeated_terminal_transition_is_idempotent() {
        let registry = DeepXOrderContextRegistry::default();
        let context = OrderContext::from(&test_order("1.250"));
        registry.register(context).unwrap();

        registry.finish(&context.identity.client_order_id).unwrap();
        registry.finish(&context.identity.client_order_id).unwrap();

        assert_eq!(
            registry
                .route(Some(context.identity.client_order_id), None)
                .unwrap(),
            DeepXExecutionUpdateRoute::Terminal(context),
        );
    }

    #[rstest]
    fn finishing_unknown_order_context_fails_closed() {
        let registry = DeepXOrderContextRegistry::default();
        let client_order_id = ClientOrderId::from("O-DEEPX-UNKNOWN");

        assert_eq!(
            registry.finish(&client_order_id),
            Err(DeepXOrderContextError::ContextNotFound(client_order_id)),
        );
    }

    #[rstest]
    fn terminal_ownership_rejects_external_registration() {
        let registry = DeepXOrderContextRegistry::default();
        let context = OrderContext::from(&test_order("1.250"));
        registry.register(context).unwrap();
        registry.finish(&context.identity.client_order_id).unwrap();
        let external = test_external_order_context("O-DEEPX-001", "V-DEEPX-001");

        assert_eq!(
            registry.register_external(external),
            Err(DeepXOrderContextError::OwnershipConflict(
                context.identity.client_order_id
            )),
        );
    }

    #[rstest]
    fn restoration_conflict_preserves_terminal_ownership() {
        let registry = DeepXOrderContextRegistry::default();
        let context = OrderContext::from(&test_order("1.250"));
        registry.register(context).unwrap();
        registry.finish(&context.identity.client_order_id).unwrap();

        assert_eq!(
            registry.restore(vec![context]),
            Err(DeepXOrderContextError::OwnershipConflict(
                context.identity.client_order_id
            )),
        );
        assert_eq!(
            registry
                .route(Some(context.identity.client_order_id), None)
                .unwrap(),
            DeepXExecutionUpdateRoute::Terminal(context),
        );
    }

    #[rstest]
    fn terminal_order_context_is_retained_across_startup_reset() {
        let mut client = test_client();
        let context = OrderContext::from(&test_order("1.250"));
        client.register_order_context(context).unwrap();
        client
            .finish_order_context(&context.identity.client_order_id)
            .unwrap();

        client.reset_startup();

        assert_eq!(
            client
                .route_execution_update(Some(context.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Terminal(context),
        );
    }

    #[rstest]
    fn oldest_terminal_context_routes_external_after_capacity_eviction() {
        let registry = DeepXOrderContextRegistryInner::<2>::default();
        let first = OrderContext::from(&test_order_with_id("O-DEEPX-001", "1.250"));
        let second = OrderContext::from(&test_order_with_id("O-DEEPX-002", "2.500"));
        let third = OrderContext::from(&test_order_with_id("O-DEEPX-003", "3.750"));
        for context in [first, second, third] {
            registry.register(context).unwrap();
            registry.finish(&context.identity.client_order_id).unwrap();
        }

        assert_eq!(
            registry
                .route(Some(first.identity.client_order_id), None)
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
        assert_eq!(
            registry
                .route(Some(second.identity.client_order_id), None)
                .unwrap(),
            DeepXExecutionUpdateRoute::Terminal(second),
        );
        assert_eq!(
            registry
                .route(Some(third.identity.client_order_id), None)
                .unwrap(),
            DeepXExecutionUpdateRoute::Terminal(third),
        );
    }

    #[rstest]
    fn terminal_context_eviction_removes_venue_order_binding() {
        let registry = DeepXOrderContextRegistryInner::<2>::default();
        let first = OrderContext::from(&test_order_with_id("O-DEEPX-001", "1.250"));
        let second = OrderContext::from(&test_order_with_id("O-DEEPX-002", "2.500"));
        let third = OrderContext::from(&test_order_with_id("O-DEEPX-003", "3.750"));
        let first_venue_order_id = VenueOrderId::from("V-DEEPX-001");
        for (context, venue_order_id) in [
            (first, first_venue_order_id),
            (second, VenueOrderId::from("V-DEEPX-002")),
            (third, VenueOrderId::from("V-DEEPX-003")),
        ] {
            registry.register(context).unwrap();
            registry
                .bind_tracked_venue_order_id(context.identity.client_order_id, venue_order_id)
                .unwrap();
            registry.finish(&context.identity.client_order_id).unwrap();
        }

        assert_eq!(
            registry.route(None, Some(first_venue_order_id)).unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
    }

    #[rstest]
    fn external_order_registration_is_idempotent_and_preserves_external_route() {
        let client = test_client();
        let context = test_external_order_context("O-DEEPX-EXT-001", "V-DEEPX-001");

        register_external_order(&client, context).unwrap();
        register_external_order(&client, context).unwrap();

        assert_eq!(
            client
                .external_order_context_by_client(&context.client_order_id)
                .unwrap(),
            Some(context),
        );
        assert_eq!(
            client
                .external_order_context_by_venue(&context.venue_order_id)
                .unwrap(),
            Some(context),
        );
        assert_eq!(
            client
                .route_execution_update(Some(context.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::RegisteredExternal(context),
        );
        assert_eq!(
            client
                .route_execution_update_identity(None, Some(context.venue_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::RegisteredExternal(context),
        );
    }

    #[rstest]
    fn execution_update_identity_conflict_fails_closed() {
        let client = test_client();
        let first = test_external_order_context("O-DEEPX-EXT-001", "V-DEEPX-001");
        let second = test_external_order_context("O-DEEPX-EXT-002", "V-DEEPX-002");
        register_external_order(&client, first).unwrap();
        register_external_order(&client, second).unwrap();

        assert_eq!(
            client.route_execution_update_identity(
                Some(first.client_order_id),
                Some(second.venue_order_id),
            ),
            Err(DeepXOrderContextError::UpdateIdentityConflict {
                client_order_id: first.client_order_id,
                venue_order_id: second.venue_order_id,
            }),
        );
    }

    #[rstest]
    fn unknown_client_identity_cannot_claim_registered_external_venue_identity() {
        let client = test_client();
        let external = test_external_order_context("O-DEEPX-EXT-001", "V-DEEPX-001");
        register_external_order(&client, external).unwrap();
        let unknown_client_order_id = ClientOrderId::from("O-DEEPX-UNKNOWN");

        assert_eq!(
            client.route_execution_update_identity(
                Some(unknown_client_order_id),
                Some(external.venue_order_id),
            ),
            Err(DeepXOrderContextError::UpdateIdentityConflict {
                client_order_id: unknown_client_order_id,
                venue_order_id: external.venue_order_id,
            }),
        );
    }

    #[rstest]
    fn tracked_client_identity_cannot_claim_registered_external_venue_identity() {
        let client = test_client();
        let tracked = OrderContext::from(&test_order_with_id("O-DEEPX-001", "1.250"));
        let external = test_external_order_context("O-DEEPX-EXT-001", "V-DEEPX-001");
        client.register_order_context(tracked).unwrap();
        register_external_order(&client, external).unwrap();

        assert_eq!(
            client.route_execution_update_identity(
                Some(tracked.identity.client_order_id),
                Some(external.venue_order_id),
            ),
            Err(DeepXOrderContextError::UpdateIdentityConflict {
                client_order_id: tracked.identity.client_order_id,
                venue_order_id: external.venue_order_id,
            }),
        );
    }

    #[rstest]
    fn tracked_and_external_venue_ownership_cannot_overlap() {
        let client = test_client();
        let tracked = OrderContext::from(&test_order("1.250"));
        let external = test_external_order_context("O-DEEPX-EXT-001", "V-DEEPX-001");
        client.register_order_context(tracked).unwrap();
        client
            .bind_tracked_venue_order_id(tracked.identity.client_order_id, external.venue_order_id)
            .unwrap();

        assert_eq!(
            register_external_order(&client, external),
            Err(DeepXOrderContextError::VenueOrderOwnershipConflict(
                external.venue_order_id
            )),
        );
        assert_eq!(
            client
                .route_execution_update_identity(None, Some(external.venue_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(tracked),
        );
    }

    #[rstest]
    fn external_venue_ownership_rejects_tracked_binding() {
        let client = test_client();
        let tracked = OrderContext::from(&test_order("1.250"));
        let external = test_external_order_context("O-DEEPX-EXT-001", "V-DEEPX-001");
        register_external_order(&client, external).unwrap();
        client.register_order_context(tracked).unwrap();

        assert_eq!(
            client.bind_tracked_venue_order_id(
                tracked.identity.client_order_id,
                external.venue_order_id,
            ),
            Err(DeepXOrderContextError::VenueOrderOwnershipConflict(
                external.venue_order_id
            )),
        );
        assert_eq!(
            client
                .route_execution_update_identity(None, Some(external.venue_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::RegisteredExternal(external),
        );
    }

    #[rstest]
    fn external_client_conflict_preserves_original_and_reverse_mapping() {
        let client = test_client();
        let original = test_external_order_context("O-DEEPX-EXT-001", "V-DEEPX-001");
        let conflicting = test_external_order_context("O-DEEPX-EXT-001", "V-DEEPX-002");
        register_external_order(&client, original).unwrap();

        assert_eq!(
            register_external_order(&client, conflicting),
            Err(DeepXOrderContextError::ExternalClientConflict(
                original.client_order_id
            )),
        );
        assert_eq!(
            client
                .external_order_context_by_client(&original.client_order_id)
                .unwrap(),
            Some(original),
        );
        assert_eq!(
            client
                .external_order_context_by_venue(&conflicting.venue_order_id)
                .unwrap(),
            None,
        );
    }

    #[rstest]
    fn external_venue_conflict_preserves_original_and_client_mapping() {
        let client = test_client();
        let original = test_external_order_context("O-DEEPX-EXT-001", "V-DEEPX-001");
        let conflicting = test_external_order_context("O-DEEPX-EXT-002", "V-DEEPX-001");
        register_external_order(&client, original).unwrap();

        assert_eq!(
            register_external_order(&client, conflicting),
            Err(DeepXOrderContextError::VenueOrderOwnershipConflict(
                original.venue_order_id
            )),
        );
        assert_eq!(
            client
                .external_order_context_by_venue(&original.venue_order_id)
                .unwrap(),
            Some(original),
        );
        assert_eq!(
            client
                .external_order_context_by_client(&conflicting.client_order_id)
                .unwrap(),
            None,
        );
    }

    #[rstest]
    fn tracked_and_external_order_ownership_cannot_overlap() {
        let client = test_client();
        let tracked = OrderContext::from(&test_order_with_id("O-DEEPX-001", "1.250"));
        let external = test_external_order_context("O-DEEPX-001", "V-DEEPX-001");
        client.register_order_context(tracked).unwrap();

        assert_eq!(
            register_external_order(&client, external),
            Err(DeepXOrderContextError::OwnershipConflict(
                tracked.identity.client_order_id
            )),
        );

        let client = test_client();
        register_external_order(&client, external).unwrap();
        assert_eq!(
            client.register_order_context(tracked),
            Err(DeepXOrderContextError::OwnershipConflict(
                tracked.identity.client_order_id
            )),
        );
    }

    #[rstest]
    fn restoration_ownership_conflict_preserves_previous_tracked_snapshot() {
        let mut client = test_client();
        let previous = OrderContext::from(&test_order_with_id("O-DEEPX-PREVIOUS", "1.250"));
        let external = test_external_order_context("O-DEEPX-EXT-001", "V-DEEPX-001");
        client.register_order_context(previous).unwrap();
        register_external_order(&client, external).unwrap();
        record_instruments_loaded(&mut client);
        let conflicting = OrderContext::from(&test_order_with_id("O-DEEPX-EXT-001", "2.500"));

        assert_eq!(
            client.restore_order_contexts([conflicting]),
            Err(DeepXOrderContextRestorationError::Registry(
                DeepXOrderContextError::OwnershipConflict(external.client_order_id)
            )),
        );
        assert_eq!(
            client
                .route_execution_update(Some(previous.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(previous),
        );
        assert_eq!(
            client
                .external_order_context_by_client(&external.client_order_id)
                .unwrap(),
            Some(external),
        );
    }

    #[rstest]
    fn registry_population_does_not_advance_startup() {
        let registry = DeepXOrderContextRegistry::default();
        let startup = DeepXExecutionStartup::default();

        registry
            .register(OrderContext::from(&test_order("1.250")))
            .unwrap();

        assert!(!startup.is_ready());
    }

    #[rstest]
    fn restoration_registers_complete_batch_and_advances_startup() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        let first = OrderContext::from(&test_order_with_id("O-DEEPX-001", "1.250"));
        let second = OrderContext::from(&test_order_with_id("O-DEEPX-002", "2.500"));

        client.restore_order_contexts([first, second]).unwrap();

        assert_eq!(
            client
                .route_execution_update(Some(first.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(first),
        );
        assert_eq!(
            client
                .route_execution_update(Some(second.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(second),
        );
        assert!(
            client
                .startup
                .record(DeepXExecutionStartupEvidence::RuntimeValidated)
                .is_ok()
        );
    }

    #[rstest]
    fn restoration_registers_complete_venue_identity_snapshot() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        let context = OrderContext::from(&test_order("1.250"));
        let venue_order_id = VenueOrderId::from("V-DEEPX-001");

        client
            .restore_order_context_identities([DeepXRestoredOrderContext::new(
                context,
                Some(venue_order_id),
            )])
            .unwrap();

        assert_eq!(
            client
                .route_execution_update_identity(None, Some(venue_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(context),
        );
        assert!(
            client
                .startup
                .record(DeepXExecutionStartupEvidence::RuntimeValidated)
                .is_ok()
        );
    }

    #[rstest]
    fn restoration_venue_identity_conflict_preserves_previous_snapshot() {
        let mut client = test_client();
        let previous = OrderContext::from(&test_order_with_id("O-DEEPX-PREVIOUS", "3.750"));
        client.register_order_context(previous).unwrap();
        record_instruments_loaded(&mut client);
        let first = OrderContext::from(&test_order_with_id("O-DEEPX-001", "1.250"));
        let second = OrderContext::from(&test_order_with_id("O-DEEPX-002", "2.500"));
        let venue_order_id = VenueOrderId::from("V-DEEPX-DUPLICATE");

        assert_eq!(
            client.restore_order_context_identities([
                DeepXRestoredOrderContext::new(first, Some(venue_order_id)),
                DeepXRestoredOrderContext::new(second, Some(venue_order_id)),
            ]),
            Err(DeepXOrderContextRestorationError::Registry(
                DeepXOrderContextError::VenueOrderOwnershipConflict(venue_order_id),
            )),
        );
        assert_eq!(
            client
                .route_execution_update(Some(previous.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(previous),
        );
        assert_eq!(
            client
                .route_execution_update_identity(None, Some(venue_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
        assert_eq!(
            client
                .startup
                .validate_next(DeepXExecutionStartupEvidence::OrderContextRestored),
            Ok(()),
        );
    }

    #[rstest]
    fn cache_restoration_uses_configured_account_open_deepx_orders() {
        let (mut client, cache) = test_client_with_cache();
        let restored = test_order_with_id("O-DEEPX-RESTORED", "1.250");
        let restored_context = OrderContext::from(&restored);
        let restored_venue_order_id = VenueOrderId::from("V-DEEPX-RESTORED");
        accept_order_in_cache(
            &cache,
            &restored,
            AccountId::from("DEEPX-001"),
            restored_venue_order_id,
        );
        let other_account = test_order_with_id("O-DEEPX-OTHER-ACCOUNT", "2.500");
        accept_order_in_cache(
            &cache,
            &other_account,
            AccountId::from("DEEPX-002"),
            VenueOrderId::from("V-DEEPX-OTHER-ACCOUNT"),
        );
        let other_venue =
            test_order_with_instrument("O-DEEPX-OTHER-VENUE", "3.750", "ETH-USDC-PERP.OTHER");
        accept_order_in_cache(
            &cache,
            &other_venue,
            AccountId::from("DEEPX-001"),
            VenueOrderId::from("V-DEEPX-OTHER-VENUE"),
        );
        let initialized = test_order_with_id("O-DEEPX-INITIALIZED", "5.000");
        cache
            .borrow_mut()
            .add_order(
                initialized.clone(),
                None,
                Some(ClientId::from("DEEPX")),
                false,
            )
            .unwrap();
        record_instruments_loaded(&mut client);

        client.restore_order_contexts_from_cache().unwrap();

        assert_eq!(
            client
                .route_execution_update_identity(None, Some(restored_venue_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(restored_context),
        );
        for excluded in [other_account, other_venue, initialized] {
            assert_eq!(
                client
                    .route_execution_update(Some(excluded.client_order_id()))
                    .unwrap(),
                DeepXExecutionUpdateRoute::External,
            );
        }
    }

    #[rstest]
    fn cache_restoration_borrow_conflict_does_not_advance_startup() {
        let (mut client, cache) = test_client_with_cache();
        record_instruments_loaded(&mut client);
        let borrowed = cache.borrow_mut();

        assert_eq!(
            client.restore_order_contexts_from_cache(),
            Err(DeepXOrderContextRestorationError::CacheBorrowConflict),
        );
        assert_eq!(
            client
                .startup
                .validate_next(DeepXExecutionStartupEvidence::OrderContextRestored),
            Ok(()),
        );
        drop(borrowed);
        client.restore_order_contexts_from_cache().unwrap();
    }

    #[rstest]
    fn empty_restoration_advances_startup_explicitly() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);

        client.restore_order_contexts([]).unwrap();

        assert!(
            client
                .startup
                .record(DeepXExecutionStartupEvidence::RuntimeValidated)
                .is_ok()
        );
    }

    #[rstest]
    fn restoration_replaces_previous_complete_snapshot() {
        let mut client = test_client();
        let original = OrderContext::from(&test_order_with_id("O-DEEPX-002", "1.250"));
        let original_venue_order_id = VenueOrderId::from("V-DEEPX-002");
        client.register_order_context(original).unwrap();
        client
            .bind_tracked_venue_order_id(original.identity.client_order_id, original_venue_order_id)
            .unwrap();
        record_instruments_loaded(&mut client);
        let new_context = OrderContext::from(&test_order_with_id("O-DEEPX-001", "1.250"));

        client.restore_order_contexts([new_context]).unwrap();

        assert_eq!(
            client
                .route_execution_update(Some(new_context.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(new_context),
        );
        assert_eq!(
            client
                .route_execution_update(Some(original.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
        assert_eq!(
            client
                .route_execution_update_identity(None, Some(original_venue_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
    }

    #[rstest]
    fn restoration_retains_venue_binding_for_restored_context() {
        let mut client = test_client();
        let context = OrderContext::from(&test_order("1.250"));
        let venue_order_id = VenueOrderId::from("V-DEEPX-001");
        client.register_order_context(context).unwrap();
        client
            .bind_tracked_venue_order_id(context.identity.client_order_id, venue_order_id)
            .unwrap();
        record_instruments_loaded(&mut client);

        client.restore_order_contexts([context]).unwrap();

        assert_eq!(
            client
                .route_execution_update_identity(None, Some(venue_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(context),
        );
    }

    #[rstest]
    fn restoration_rejects_conflicting_duplicate_within_batch() {
        let mut client = test_client();
        let previous = OrderContext::from(&test_order_with_id("O-DEEPX-PREVIOUS", "3.750"));
        client.register_order_context(previous).unwrap();
        record_instruments_loaded(&mut client);
        let original = OrderContext::from(&test_order("1.250"));
        let conflicting = OrderContext::from(&test_order("2.500"));

        assert_eq!(
            client.restore_order_contexts([original, conflicting]),
            Err(DeepXOrderContextRestorationError::Registry(
                DeepXOrderContextError::Conflict(original.identity.client_order_id)
            )),
        );
        assert_eq!(
            client
                .route_execution_update(Some(original.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
        assert_eq!(
            client
                .route_execution_update(Some(previous.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(previous),
        );
    }

    #[rstest]
    fn registering_order_context_rejects_another_venue() {
        let client = test_client();
        let order = test_order_with_instrument("O-OTHER-001", "1.0", "ETH-USDC-PERP.OTHER");

        assert_eq!(
            client.register_order(&order),
            Err(DeepXOrderContextError::InstrumentVenueMismatch {
                client_order_id: ClientOrderId::from("O-OTHER-001"),
                venue: Venue::from("OTHER"),
            }),
        );
        assert_eq!(
            client
                .route_execution_update(Some(ClientOrderId::from("O-OTHER-001")))
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
    }

    #[rstest]
    fn registering_external_order_rejects_another_venue() {
        let client = test_client();
        let mut context = test_external_order_context("O-OTHER-001", "V-OTHER-001");
        context.instrument_id = InstrumentId::from("ETH-USDC-PERP.OTHER");

        assert_eq!(
            register_external_order(&client, context),
            Err(DeepXOrderContextError::InstrumentVenueMismatch {
                client_order_id: context.client_order_id,
                venue: Venue::from("OTHER"),
            }),
        );
        assert_eq!(
            client
                .external_order_context_by_client(&context.client_order_id)
                .unwrap(),
            None,
        );
        assert_eq!(
            client
                .external_order_context_by_venue(&context.venue_order_id)
                .unwrap(),
            None,
        );
    }

    #[rstest]
    fn restoration_rejects_another_venue_without_replacing_snapshot() {
        let mut client = test_client();
        let previous = OrderContext::from(&test_order_with_id("O-DEEPX-PREVIOUS", "1.250"));
        client.register_order_context(previous).unwrap();
        record_instruments_loaded(&mut client);
        let replacement = OrderContext::from(&test_order_with_id("O-DEEPX-NEW", "2.500"));
        let foreign = OrderContext::from(&test_order_with_instrument(
            "O-OTHER-001",
            "3.750",
            "ETH-USDC-PERP.OTHER",
        ));

        assert_eq!(
            client.restore_order_contexts([replacement, foreign]),
            Err(DeepXOrderContextRestorationError::Registry(
                DeepXOrderContextError::InstrumentVenueMismatch {
                    client_order_id: ClientOrderId::from("O-OTHER-001"),
                    venue: Venue::from("OTHER"),
                },
            )),
        );
        assert_eq!(
            client
                .route_execution_update(Some(previous.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(previous),
        );
        assert_eq!(
            client
                .route_execution_update(Some(replacement.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
    }

    #[rstest]
    fn out_of_order_restoration_does_not_mutate_registry() {
        let mut client = test_client();
        let context = OrderContext::from(&test_order("1.250"));

        assert_eq!(
            client.restore_order_contexts([context]),
            Err(DeepXOrderContextRestorationError::Startup(
                DeepXExecutionStartupError::OutOfOrder {
                    expected: DeepXExecutionStartupEvidence::InstrumentsLoaded,
                    received: DeepXExecutionStartupEvidence::OrderContextRestored,
                }
            )),
        );
        assert_eq!(
            client
                .route_execution_update(Some(context.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
    }

    #[rstest]
    fn reset_retains_context_but_requires_restoration_replay() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        let context = OrderContext::from(&test_order("1.250"));
        client.restore_order_contexts([context]).unwrap();

        client.reset_startup();
        record_instruments_loaded(&mut client);

        assert_eq!(
            client
                .route_execution_update(Some(context.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(context),
        );
        assert_eq!(
            client
                .startup
                .record(DeepXExecutionStartupEvidence::RuntimeValidated),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::OrderContextRestored,
                received: DeepXExecutionStartupEvidence::RuntimeValidated,
            }),
        );
        client.restore_order_contexts([context]).unwrap();
    }

    #[rstest]
    fn reconnect_restoration_replaces_modified_context() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        let original = OrderContext::from(&test_order("1.250"));
        client.restore_order_contexts([original]).unwrap();
        client.reset_startup();
        record_instruments_loaded(&mut client);
        let modified = OrderContext::from(&test_order("2.500"));

        client.restore_order_contexts([modified]).unwrap();

        assert_eq!(
            client
                .route_execution_update(Some(modified.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(modified),
        );
    }

    #[rstest]
    fn reconnect_empty_restoration_clears_previous_contexts() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        let context = OrderContext::from(&test_order("1.250"));
        client.restore_order_contexts([context]).unwrap();
        client.reset_startup();
        record_instruments_loaded(&mut client);

        client.restore_order_contexts([]).unwrap();

        assert_eq!(
            client
                .route_execution_update(Some(context.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
    }

    #[rstest]
    fn account_state_initialization_rejects_wrong_account_type() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        let (protocol, session) = authenticated_protocol();
        client
            .record_private_stream_authenticated(&protocol, session)
            .unwrap();
        let frame = authenticated_account_frame(&protocol, session);
        let mut state = test_account_state();
        state.account_type = AccountType::Cash;

        assert_eq!(
            client.record_account_state_initialized(&protocol, &frame, &state),
            Err(DeepXExecutionStartupError::AccountStateIdentityMismatch {
                expected_account_id: AccountId::from("DEEPX-001"),
                expected_account_type: AccountType::Margin,
                received_account_id: AccountId::from("DEEPX-001"),
                received_account_type: AccountType::Cash,
            }),
        );
        assert_eq!(
            client
                .startup
                .record(DeepXExecutionStartupEvidence::MassReconciliationCompleted),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::AccountStateInitialized,
                received: DeepXExecutionStartupEvidence::MassReconciliationCompleted,
            }),
        );
    }

    #[rstest]
    fn account_state_initialization_dispatches_exact_event() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        let (protocol, session) = authenticated_protocol();
        client
            .record_private_stream_authenticated(&protocol, session)
            .unwrap();
        let frame = authenticated_account_frame(&protocol, session);
        let state = test_account_state();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);

        client
            .record_account_state_initialized(&protocol, &frame, &state)
            .unwrap();

        let ExecutionEvent::Account(dispatched) = receiver.try_recv().unwrap() else {
            panic!("expected account state event");
        };
        assert_eq!(dispatched, state);
        assert_eq!(client.startup_account_event_id, Some(state.event_id));
        assert_eq!(client.startup.completed_steps, 5);
    }

    #[rstest]
    fn account_state_initialization_dispatch_failure_does_not_advance_startup() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        let (protocol, session) = authenticated_protocol();
        client
            .record_private_stream_authenticated(&protocol, session)
            .unwrap();
        let frame = authenticated_account_frame(&protocol, session);
        let state = test_account_state();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        drop(receiver);
        client.emitter.set_sender(sender);

        let result = client.record_account_state_initialized(&protocol, &frame, &state);

        assert!(matches!(
            result,
            Err(DeepXExecutionStartupError::AccountStateDispatchFailed(_)),
        ));
        assert_eq!(client.startup_account_event_id, None);
        assert_eq!(client.startup.completed_steps, 4);
        assert_eq!(
            client
                .startup
                .validate_next(DeepXExecutionStartupEvidence::AccountStateInitialized),
            Ok(()),
        );
    }

    #[rstest]
    fn account_state_initialization_rejects_stale_authenticated_session_without_dispatch() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        let (mut protocol, session) = authenticated_protocol();
        client
            .record_private_stream_authenticated(&protocol, session)
            .unwrap();
        let frame = authenticated_account_frame(&protocol, session);
        protocol.reset_after_reconnect(1, "test reconnect").unwrap();
        let state = test_account_state();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);

        assert_eq!(
            client.record_account_state_initialized(&protocol, &frame, &state),
            Err(DeepXExecutionStartupError::PrivateStreamAuthenticationMismatch),
        );
        assert!(receiver.try_recv().is_err());
        assert_eq!(client.startup_account_event_id, None);
        assert_eq!(client.startup.completed_steps, 4);
    }

    #[rstest]
    fn account_state_initialization_rejects_session_from_another_protocol_owner() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        let (protocol, session) = authenticated_protocol();
        client
            .record_private_stream_authenticated(&protocol, session)
            .unwrap();
        let (other_protocol, other_session) = authenticated_protocol();
        let other_frame = authenticated_account_frame(&other_protocol, other_session);
        let state = test_account_state();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);

        assert_eq!(
            client.record_account_state_initialized(&other_protocol, &other_frame, &state),
            Err(DeepXExecutionStartupError::PrivateStreamAuthenticationMismatch),
        );
        assert!(receiver.try_recv().is_err());
        assert_eq!(client.startup_account_event_id, None);
        assert_eq!(client.startup.completed_steps, 4);
    }

    #[rstest]
    fn reset_clears_authenticated_session_receipt() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        let (protocol, session) = authenticated_protocol();
        client
            .record_private_stream_authenticated(&protocol, session)
            .unwrap();
        let frame = authenticated_account_frame(&protocol, session);

        client.reset_startup();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::PrivateStreamAuthenticated)
            .unwrap();
        let state = test_account_state();

        assert_eq!(
            client.record_account_state_initialized(&protocol, &frame, &state),
            Err(DeepXExecutionStartupError::PrivateStreamAuthenticationMismatch),
        );
        assert_eq!(client.startup_account_event_id, None);
        assert_eq!(client.startup.completed_steps, 4);
    }

    #[rstest]
    fn account_registration_requires_configured_account_in_cache() {
        let mut client = test_client();
        let (state, protocol, session) = advance_through_mass_reconciliation(&mut client);

        assert_eq!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::AccountStateNotRegistered {
                account_id: AccountId::from("DEEPX-001"),
                event_id: state.event_id,
            }),
        );
        assert!(!client.is_connected());
    }

    #[rstest]
    fn account_registration_connects_after_cache_verification() {
        let (mut client, cache) = test_client_with_cache();
        let (state, protocol, session) = advance_through_mass_reconciliation(&mut client);
        register_test_account(&cache, state);

        client
            .complete_account_registration(&protocol, session)
            .unwrap();

        assert!(client.is_connected());
    }

    #[rstest]
    fn account_registration_reports_cache_borrow_conflict() {
        let (mut client, cache) = test_client_with_cache();
        let (state, protocol, session) = advance_through_mass_reconciliation(&mut client);
        register_test_account(&cache, state);
        let borrowed = cache.borrow_mut();

        assert_eq!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::CacheBorrowConflict),
        );
        assert!(!client.is_connected());
        drop(borrowed);
        client
            .complete_account_registration(&protocol, session)
            .unwrap();
        assert!(client.is_connected());
    }

    #[rstest]
    fn account_registration_checks_startup_order_before_cache() {
        let (mut client, cache) = test_client_with_cache();
        register_test_account(&cache, test_account_state());
        let (protocol, session) = authenticated_protocol();

        assert_eq!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::InstrumentsLoaded,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        );
        assert!(!client.is_connected());
    }

    #[rstest]
    fn reconnect_requires_startup_replay_before_cached_account_registration() {
        let (mut client, cache) = test_client_with_cache();
        let (state, protocol, session) = advance_through_mass_reconciliation(&mut client);
        register_test_account(&cache, state);
        client
            .complete_account_registration(&protocol, session)
            .unwrap();
        client.reset_startup();

        assert_eq!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::InstrumentsLoaded,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        );
        assert!(!client.is_connected());
    }

    #[rstest]
    fn reconnect_rejects_account_state_from_previous_startup_epoch() {
        let (mut client, cache) = test_client_with_cache();
        let (initial_state, initial_protocol, initial_session) =
            advance_through_mass_reconciliation(&mut client);
        register_test_account(&cache, initial_state);
        client
            .complete_account_registration(&initial_protocol, initial_session)
            .unwrap();
        client.reset_startup();
        let (current_state, current_protocol, current_session) =
            advance_through_mass_reconciliation(&mut client);

        assert_eq!(
            client.complete_account_registration(&current_protocol, current_session),
            Err(DeepXExecutionStartupError::AccountStateNotRegistered {
                account_id: AccountId::from("DEEPX-001"),
                event_id: current_state.event_id,
            }),
        );
        assert!(!client.is_connected());
    }

    #[rstest]
    fn account_registration_rejects_session_invalidated_after_account_state() {
        let (mut client, cache) = test_client_with_cache();
        let (state, mut protocol, session) = advance_through_mass_reconciliation(&mut client);
        register_test_account(&cache, state);
        protocol.reset_after_reconnect(1, "test reconnect").unwrap();

        assert_eq!(
            client.complete_account_registration(&protocol, session),
            Err(DeepXExecutionStartupError::PrivateStreamAuthenticationMismatch),
        );
        assert!(!client.is_connected());
        assert_eq!(client.startup.completed_steps, 6);
    }

    #[rstest]
    fn execution_client_exposes_framework_identity() {
        let client = test_client();

        assert_eq!(ExecutionClient::client_id(&client), ClientId::from("DEEPX"));
        assert_eq!(
            ExecutionClient::account_id(&client),
            AccountId::from("DEEPX-001"),
        );
        assert_eq!(ExecutionClient::venue(&client), *DEEPX_VENUE);
        assert_eq!(ExecutionClient::oms_type(&client), OmsType::Netting);
        assert!(ExecutionClient::get_account(&client).is_none());
        assert!(!ExecutionClient::provides_bulk_position_coverage(
            &client,
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
        ));
        assert!(!ExecutionClient::is_connected(&client));
    }

    #[rstest]
    fn execution_client_start_and_stop_are_idempotent() {
        let mut client = test_client();
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(sender);

        ExecutionClient::start(&mut client).unwrap();
        ExecutionClient::start(&mut client).unwrap();
        assert!(client.core.is_started());

        ExecutionClient::stop(&mut client).unwrap();
        ExecutionClient::stop(&mut client).unwrap();
        assert!(client.core.is_stopped());
        assert!(!client.is_connected());
        assert_eq!(client.startup.completed_steps, 0);
    }

    #[rstest]
    fn execution_client_dispose_clears_connected_startup_state() {
        let (mut client, cache) = test_client_with_cache();
        let (state, protocol, session) = advance_through_mass_reconciliation(&mut client);
        register_test_account(&cache, state);
        client
            .complete_account_registration(&protocol, session)
            .unwrap();
        assert!(client.is_connected());

        ExecutionClient::dispose(&mut client).unwrap();

        assert!(client.core.is_stopped());
        assert!(!client.is_connected());
        assert_eq!(client.startup.completed_steps, 0);
        assert_eq!(client.startup_authenticated_session, None);
        assert_eq!(client.startup_account_event_id, None);
    }

    #[tokio::test]
    async fn execution_client_connect_rejects_incomplete_startup() {
        let mut client = test_client();

        let error = ExecutionClient::connect(&mut client).await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "DeepX execution startup has not completed"
        );
        assert!(!client.is_connected());
        assert_eq!(client.startup.completed_steps, 0);
    }
}
