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

use nautilus_common::cache::fifo::{FifoCache, FifoCacheMap};
use nautilus_core::{UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter, execution::context::OrderContext};
use nautilus_model::{
    enums::AccountType,
    events::AccountState,
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TradeId, VenueOrderId},
    orders::OrderAny,
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
        DeepXSignerLease, DeepXTransactionPersistenceError, DeepXTransactionRecoveryAction,
        DeepXTransactionStore, load_verified_committed_for_signer,
    },
    websocket::{DeepXWsAuthenticatedSession, DeepXWsProtocolCore},
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
    ExternalVenueConflict(VenueOrderId),
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

/// Errors raised while restoring the complete startup order-context set.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXOrderContextRestorationError {
    /// Startup was not waiting for order-context restoration.
    #[error(transparent)]
    Startup(#[from] DeepXExecutionStartupError),
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
    /// Complete durable transaction evidence could not be verified.
    #[error(transparent)]
    Persistence(#[from] DeepXTransactionPersistenceError),
    /// A durable transaction still requires recovery or operator action.
    #[error("DeepX transaction {client_order_id} still requires startup action {action:?}")]
    UnresolvedTransaction {
        /// Client order ID owning the unresolved transaction.
        client_order_id: String,
        /// Fail-closed action required before startup may continue.
        action: DeepXTransactionRecoveryAction,
    },
}

/// Classification of an execution update against registered Nautilus order context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXExecutionUpdateRoute {
    /// The update belongs to an order tracked by this execution client.
    Tracked(OrderContext),
    /// The update belongs to an order whose tracked lifecycle is terminal.
    Terminal(OrderContext),
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

type DeepXOrderContextRegistry = DeepXOrderContextRegistryInner<TERMINAL_CONTEXT_CAPACITY>;

#[derive(Debug, Default)]
struct DeepXOrderContextRegistryInner<const N: usize> {
    state: Mutex<DeepXOrderContextState<N>>,
}

#[derive(Debug)]
struct DeepXOrderContextState<const N: usize> {
    tracked: HashMap<ClientOrderId, OrderContext>,
    terminal: FifoCacheMap<ClientOrderId, OrderContext, N>,
    external_by_client: HashMap<ClientOrderId, DeepXExternalOrderContext>,
    external_client_by_venue: HashMap<VenueOrderId, ClientOrderId>,
}

impl<const N: usize> Default for DeepXOrderContextState<N> {
    fn default() -> Self {
        Self {
            tracked: HashMap::new(),
            terminal: FifoCacheMap::new(),
            external_by_client: HashMap::new(),
            external_client_by_venue: HashMap::new(),
        }
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
        Ok(())
    }

    fn register_external(
        &self,
        context: DeepXExternalOrderContext,
    ) -> Result<(), DeepXOrderContextError> {
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
        {
            return Err(DeepXOrderContextError::ExternalVenueConflict(
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
    ) -> Result<DeepXExecutionUpdateRoute, DeepXOrderContextError> {
        let Some(client_order_id) = client_order_id else {
            return Ok(DeepXExecutionUpdateRoute::External);
        };
        let state = self
            .state
            .lock()
            .map_err(|_| DeepXOrderContextError::LockPoisoned)?;
        Ok(if let Some(context) = state.tracked.get(&client_order_id) {
            DeepXExecutionUpdateRoute::Tracked(*context)
        } else if let Some(context) = state.terminal.get(&client_order_id) {
            DeepXExecutionUpdateRoute::Terminal(*context)
        } else {
            DeepXExecutionUpdateRoute::External
        })
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
    startup_account_event_id: Option<UUID4>,
}

impl DeepXExecutionClient {
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
        let configured_url = self
            .config
            .network
            .rest_url()
            .map_err(|_| DeepXExecutionStartupError::MarketCatalogEndpointMismatch)?;
        if provider.base_url() != configured_url.trim_end_matches('/') {
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
        self.order_contexts.route(client_order_id)
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

    /// Verifies and records the account-state event for the current startup epoch.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for account-state initialization and the event
    /// matches the configured execution account identity and type.
    pub fn record_account_state_initialized(
        &mut self,
        state: &AccountState,
    ) -> Result<(), DeepXExecutionStartupError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::AccountStateInitialized)?;
        if state.account_id != self.core.account_id || state.account_type != self.core.account_type
        {
            return Err(DeepXExecutionStartupError::AccountStateIdentityMismatch {
                expected_account_id: self.core.account_id,
                expected_account_type: self.core.account_type,
                received_account_id: state.account_id,
                received_account_type: state.account_type,
            });
        }
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
    /// belongs to the configured signing key, and every durable transaction is complete.
    pub async fn record_mass_reconciliation_completed<S>(
        &mut self,
        store: &S,
        lease: &S::Lease,
    ) -> Result<(), DeepXMassReconciliationError>
    where
        S: DeepXTransactionStore,
    {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::MassReconciliationCompleted)?;
        if lease.signer() != derive_signer_account_id(&self.credential)? {
            return Err(DeepXMassReconciliationError::SignerLeaseMismatch);
        }
        let restored = load_verified_committed_for_signer(store, lease).await?;
        for item in restored {
            let action = item.record().recovery_action();
            if action != DeepXTransactionRecoveryAction::Complete {
                return Err(DeepXMassReconciliationError::UnresolvedTransaction {
                    client_order_id: item.record().identity().client_order_id().to_string(),
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
    /// Returns an error unless startup is waiting for account registration and the configured
    /// account exists in the shared execution cache.
    pub fn complete_account_registration(&mut self) -> Result<(), DeepXExecutionStartupError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::AccountRegistered)?;
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

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{
        Json, Router,
        routing::{get, post},
    };
    use nautilus_common::cache::Cache;
    use nautilus_core::{UUID4, UnixNanos, hex};
    use nautilus_model::{
        accounts::{AccountAny, MarginAccount},
        enums::{AccountType, OmsType, OrderSide, OrderType, TimeInForce},
        events::AccountState,
        identifiers::{
            AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TradeId, TraderId,
            VenueOrderId,
        },
        orders::OrderTestBuilder,
        types::{Price, Quantity},
    };
    use rstest::rstest;
    use serde_json::{Value, json};
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
        signing::{DeepXRuntimeSnapshotService, RuntimeSnapshot},
        transaction::{
            DeepXCommittedTransactionRecord, DeepXDirectRuntimeIdentity, DeepXNonceReservation,
            DeepXRestoredTransactionRecord, DeepXTransactionIdentity, DeepXTransactionRecord,
            DeepXTransactionRevision,
        },
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
        OrderTestBuilder::new(OrderType::Limit)
            .client_order_id(ClientOrderId::from(client_order_id))
            .strategy_id(StrategyId::from("S-DEEPX-001"))
            .instrument_id(InstrumentId::from("ETH-USDC-PERP.DEEPX"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(quantity))
            .price(Price::from("2500.00"))
            .time_in_force(TimeInForce::Gtc)
            .build()
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

    fn advance_through_mass_reconciliation(client: &mut DeepXExecutionClient) -> AccountState {
        record_instruments_loaded(client);
        client.restore_order_contexts([]).unwrap();
        for evidence in [
            DeepXExecutionStartupEvidence::RuntimeValidated,
            DeepXExecutionStartupEvidence::PrivateStreamAuthenticated,
        ] {
            client.startup.record(evidence).unwrap();
        }
        let state = test_account_state();
        client.record_account_state_initialized(&state).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::MassReconciliationCompleted)
            .unwrap();
        state
    }

    fn advance_to_mass_reconciliation(client: &mut DeepXExecutionClient) {
        record_instruments_loaded(client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::PrivateStreamAuthenticated)
            .unwrap();
        client
            .record_account_state_initialized(&test_account_state())
            .unwrap();
    }

    #[tokio::test]
    async fn mass_reconciliation_accepts_empty_complete_store_snapshot() {
        let mut client = test_client();
        advance_to_mass_reconciliation(&mut client);
        let store = TestTransactionStore {
            restored: Vec::new(),
        };
        let signer = derive_signer_account_id(&client.credential).unwrap();
        let lease = store.acquire_signer_lease(signer).await.unwrap();

        client
            .record_mass_reconciliation_completed(&store, &lease)
            .await
            .unwrap();

        assert!(matches!(
            client.complete_account_registration(),
            Err(DeepXExecutionStartupError::AccountStateNotRegistered { .. }),
        ));
    }

    #[tokio::test]
    async fn mass_reconciliation_rejects_another_signer_without_advancing() {
        let mut client = test_client();
        advance_to_mass_reconciliation(&mut client);
        let store = TestTransactionStore {
            restored: Vec::new(),
        };
        let lease = TestSignerLease { signer: [42; 20] };

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(&store, &lease)
                .await,
            Err(DeepXMassReconciliationError::SignerLeaseMismatch),
        ));
        assert!(matches!(
            client.complete_account_registration(),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::MassReconciliationCompleted,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        ));
    }

    #[tokio::test]
    async fn mass_reconciliation_rejects_unresolved_durable_transaction_without_advancing() {
        let mut client = test_client();
        advance_to_mass_reconciliation(&mut client);
        let signer = derive_signer_account_id(&client.credential).unwrap();
        let record = DeepXTransactionRecord::created(DeepXTransactionIdentity::new(
            ClientOrderId::from("O-DEEPX-UNRESOLVED"),
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
                .record_mass_reconciliation_completed(&store, &lease)
                .await,
            Err(DeepXMassReconciliationError::UnresolvedTransaction {
                action: DeepXTransactionRecoveryAction::RecreateSigningInputs,
                ..
            }),
        ));
        assert!(matches!(
            client.complete_account_registration(),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::MassReconciliationCompleted,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        ));
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
                .route(Some(expected.identity.client_order_id))
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
                .route(Some(context.identity.client_order_id))
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
                .route(Some(original.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Tracked(original),
        );
    }

    #[rstest]
    fn missing_order_context_routes_external() {
        let registry = DeepXOrderContextRegistry::default();

        assert_eq!(
            registry.route(None).unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
        assert_eq!(
            registry
                .route(Some(ClientOrderId::from("O-DEEPX-UNKNOWN")))
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
                .route(Some(context.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Terminal(context),
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
                .route(Some(context.identity.client_order_id))
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
                .route(Some(context.identity.client_order_id))
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
                .route(Some(first.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::External,
        );
        assert_eq!(
            registry
                .route(Some(second.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Terminal(second),
        );
        assert_eq!(
            registry
                .route(Some(third.identity.client_order_id))
                .unwrap(),
            DeepXExecutionUpdateRoute::Terminal(third),
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
            DeepXExecutionUpdateRoute::External,
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
            Err(DeepXOrderContextError::ExternalVenueConflict(
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
        client.register_order_context(original).unwrap();
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
        for evidence in [
            DeepXExecutionStartupEvidence::RuntimeValidated,
            DeepXExecutionStartupEvidence::PrivateStreamAuthenticated,
        ] {
            client.startup.record(evidence).unwrap();
        }
        let mut state = test_account_state();
        state.account_type = AccountType::Cash;

        assert_eq!(
            client.record_account_state_initialized(&state),
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
    fn account_registration_requires_configured_account_in_cache() {
        let mut client = test_client();
        let state = advance_through_mass_reconciliation(&mut client);

        assert_eq!(
            client.complete_account_registration(),
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
        let state = advance_through_mass_reconciliation(&mut client);
        register_test_account(&cache, state);

        client.complete_account_registration().unwrap();

        assert!(client.is_connected());
    }

    #[rstest]
    fn account_registration_reports_cache_borrow_conflict() {
        let (mut client, cache) = test_client_with_cache();
        let state = advance_through_mass_reconciliation(&mut client);
        register_test_account(&cache, state);
        let borrowed = cache.borrow_mut();

        assert_eq!(
            client.complete_account_registration(),
            Err(DeepXExecutionStartupError::CacheBorrowConflict),
        );
        assert!(!client.is_connected());
        drop(borrowed);
        client.complete_account_registration().unwrap();
        assert!(client.is_connected());
    }

    #[rstest]
    fn account_registration_checks_startup_order_before_cache() {
        let (mut client, cache) = test_client_with_cache();
        register_test_account(&cache, test_account_state());

        assert_eq!(
            client.complete_account_registration(),
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
        let state = advance_through_mass_reconciliation(&mut client);
        register_test_account(&cache, state);
        client.complete_account_registration().unwrap();
        client.reset_startup();

        assert_eq!(
            client.complete_account_registration(),
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
        let initial_state = advance_through_mass_reconciliation(&mut client);
        register_test_account(&cache, initial_state);
        client.complete_account_registration().unwrap();
        client.reset_startup();
        let current_state = advance_through_mass_reconciliation(&mut client);

        assert_eq!(
            client.complete_account_registration(),
            Err(DeepXExecutionStartupError::AccountStateNotRegistered {
                account_id: AccountId::from("DEEPX-001"),
                event_id: current_state.event_id,
            }),
        );
        assert!(!client.is_connected());
    }
}
