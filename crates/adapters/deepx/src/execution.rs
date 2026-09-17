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
    sync::{Arc, Mutex},
    time::Duration,
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
use nautilus_live::{
    ExecutionClientCore, ExecutionEventEmitter, execution::context::OrderContext, task::TaskGroup,
};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, PositionSide},
    events::AccountState,
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TradeId, Venue, VenueOrderId,
    },
    instruments::InstrumentAny,
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use thiserror::Error;

use crate::{
    account::DeepXAccountOwnershipProof,
    common::{DeepXEnvironment, DeepXPrivateKey, consts::DEEPX_VENUE},
    config::{
        DeepXExecutionBackend, DeepXExecutionClientConfig, DeepXRpcRole, DeepXValidatedRpcEndpoints,
    },
    http::{
        DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpAccountTradeRecord,
        DeepXPerpAccountTradesRequest, DeepXPerpOrderRecord, DeepXPerpPositionRecord,
        DeepXPerpPositionsRequest,
    },
    providers::{DeepXMarketMetadata, DeepXMarketProvider},
    rpc::{
        DeepXAppliedRuntimeSnapshot, DeepXFinalizedChainTimeError, DeepXFinalizedChainTimeEvidence,
        DeepXValidatedRpcMethodCapabilities, observe_and_apply_finalized_chain_time,
    },
    signing::{
        DeepXRuntimeSnapshotService, DeepXRuntimeSnapshotServiceError, RuntimeSnapshot,
        SigningError, derive_signer_account_id,
    },
    transaction::{
        DeepXDurableRecoveryObserver, DeepXFinalityCommitError, DeepXFinalizedRecoveryCommitError,
        DeepXPerpCancelCallVerifier, DeepXPerpCloseCallVerifier, DeepXPerpPlaceCallVerifier,
        DeepXPerpProfitAndLossPointCallVerifier, DeepXPoolReconciliationCommitError,
        DeepXPostgresSignerLease, DeepXPostgresTransactionStore, DeepXReorganizationCommitError,
        DeepXRestoredTransactionRecord, DeepXSignerLease, DeepXSpotCancelCallVerifier,
        DeepXSpotPlaceCallVerifier, DeepXTimestampNonceAllocator, DeepXTransactionOperation,
        DeepXTransactionPersistenceError, DeepXTransactionRecoveryAction, DeepXTransactionState,
        DeepXTransactionStore, DeepXTransactionWatchError, load_verified_committed_for_signer,
        observe_and_commit_finality, observe_and_commit_reorganization,
        reconcile_not_included_checkpoint, reconcile_not_included_checkpoint_with_observer,
        reconcile_submission_pool, restore_timestamp_nonce_allocator,
    },
    websocket::{
        DeepXWsAccountConnection, DeepXWsConfirmedAccountSubscription,
        DeepXWsConfirmedBalancesFrame,
    },
};

const TRADE_DEDUP_CAPACITY: usize = 10_000;
const TERMINAL_CONTEXT_CAPACITY: usize = 10_000;
const POSITION_REPORT_PAGE_SIZE: u32 = 100;
const POSITION_REPORT_MAX_PAGES: usize = 100;
const FILL_REPORT_PAGE_SIZE: u32 = 100;
const FILL_REPORT_MAX_PAGES: usize = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DeepXPerpetualReportMetadata {
    price_precision: u8,
    size_precision: u8,
    price_increment: rust_decimal::Decimal,
    size_increment: rust_decimal::Decimal,
    quote_currency: Currency,
    maker_fee_rate: rust_decimal::Decimal,
    taker_fee_rate: rust_decimal::Decimal,
}

#[derive(Clone, Debug)]
struct DeepXTrackedOrderQuery {
    account_id: AccountId,
    subaccount: String,
    market_id: u64,
    venue_order_id: VenueOrderId,
    context: OrderContext,
    ts_init: UnixNanos,
}

/// Ordered evidence required before a DeepX execution client can become connected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXExecutionStartupEvidence {
    /// All Spot and perpetual instruments completed a failure-atomic preload.
    InstrumentsLoaded,
    /// Durable order identity and transaction context completed restoration.
    OrderContextRestored,
    /// Backend, RPC roles, and runtime snapshot were validated.
    RuntimeValidated,
    /// REST directory and profile evidence bound the configured subaccount to the signer.
    AccountOwnershipValidated,
    /// The address-scoped account stream acknowledged the configured subaccount.
    AccountStreamConfirmed,
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
    /// The public market catalog contains inconsistent perpetual identity indexes.
    #[error("DeepX market catalog contains inconsistent perpetual market identity")]
    MarketCatalogIdentityMismatch,
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
    /// The account-stream subscription receipt is not current for its connection owner.
    #[error("DeepX account-stream subscription is not current")]
    AccountStreamSubscriptionMismatch,
    /// The ownership proof belongs to another signing identity.
    #[error("DeepX account ownership proof does not match the configured signing identity")]
    AccountOwnershipSignerMismatch,
    /// The ownership proof belongs to another configured subaccount.
    #[error("DeepX account ownership proof does not match the configured subaccount")]
    AccountOwnershipSubaccountMismatch,
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

/// Errors raised while converting validated DeepX account trades into fill reports.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum DeepXFillReportError {
    /// The execution configuration no longer contains a subaccount identity.
    #[error("DeepX fill reports require a configured subaccount")]
    MissingConfiguredSubaccount,
    /// The requested venue order identity cannot be sent or compared exactly.
    #[error("DeepX fill report venue order ID must be an exact decimal u64")]
    InvalidVenueOrderId,
    /// The requested instrument was absent from the immutable startup catalog.
    #[error("DeepX fill report has no validated perpetual market for {0}")]
    UnknownInstrument(InstrumentId),
    /// A REST trade belongs to a market absent from the immutable startup catalog.
    #[error("DeepX fill report contains unknown perpetual market ID {0}")]
    UnknownMarket(u64),
    /// The startup catalog's forward, reverse, or report metadata disagree.
    #[error("DeepX fill report market identity snapshot is inconsistent")]
    MarketIdentityMismatch,
    /// The venue taker side cannot be interpreted against the account order side.
    #[error("unsupported DeepX fill report taker: {0}")]
    UnsupportedTaker(String),
    /// The venue fill-direction value is outside the chain enum.
    #[error("unsupported DeepX fill report direction: {0}")]
    UnsupportedFilledDirection(String),
    /// The trade quantity is not an exact multiple of the startup instrument increment.
    #[error("DeepX fill report quantity does not align with instrument increment")]
    QuantityIncrementMismatch,
    /// The trade quantity could not be represented exactly by the startup instrument precision.
    #[error("invalid DeepX fill report quantity: {0}")]
    QuantityConversion(String),
    /// The trade quantity would be rounded at the startup instrument precision.
    #[error("DeepX fill report quantity loses precision")]
    QuantityPrecisionLoss,
    /// The trade price is not an exact multiple of the startup instrument increment.
    #[error("DeepX fill report price does not align with instrument increment")]
    PriceIncrementMismatch,
    /// The trade price could not be represented exactly by the startup instrument precision.
    #[error("invalid DeepX fill report price: {0}")]
    PriceConversion(String),
    /// The trade price would be rounded at the startup instrument precision.
    #[error("DeepX fill report price loses precision")]
    PricePrecisionLoss,
    /// A nonempty REST fee asset conflicts with the market quote currency.
    #[error("DeepX fill report fee asset does not match market quote currency")]
    FeeAssetMismatch,
    /// The REST account-delta fee sign conflicts with the applicable market fee rate.
    #[error("DeepX fill report fee sign conflicts with market fee rate")]
    FeeSignMismatch,
    /// The negated REST account-delta fee could not be represented as commission.
    #[error("invalid DeepX fill report commission: {0}")]
    CommissionConversion(String),
    /// Commission would be rounded at the quote currency precision.
    #[error("DeepX fill report commission loses precision")]
    CommissionPrecisionLoss,
    /// The trade timestamp is malformed or before the Unix epoch.
    #[error("invalid DeepX fill report createdAt timestamp")]
    InvalidTimestamp,
    /// Registry access or identity routing failed.
    #[error(transparent)]
    Registry(#[from] DeepXOrderContextError),
    /// Restored local context disagrees with the REST trade instrument.
    #[error("DeepX fill report instrument does not match registered order context")]
    ContextInstrumentMismatch,
    /// Restored local context disagrees with the REST trade side.
    #[error("DeepX fill report side does not match registered order context")]
    ContextSideMismatch,
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

/// Errors raised while converting a validated DeepX order into a tracked-order report.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum DeepXTrackedOrderReportError {
    /// The execution configuration no longer contains a subaccount identity.
    #[error("DeepX tracked-order report requires a configured subaccount")]
    MissingConfiguredSubaccount,
    /// The REST order belongs to another subaccount.
    #[error("DeepX tracked-order report belongs to another subaccount")]
    SubaccountMismatch,
    /// The REST order belongs to another market.
    #[error("DeepX tracked-order report market mismatch: expected {expected}, received {received}")]
    MarketMismatch { expected: u64, received: u64 },
    /// The supplied venue order ID does not match the REST order identity.
    #[error("DeepX tracked-order report venue order ID mismatch")]
    VenueOrderIdMismatch,
    /// Registry access or identity routing failed.
    #[error(transparent)]
    Registry(#[from] DeepXOrderContextError),
    /// The venue order ID is not bound to locally tracked order context.
    #[error("DeepX venue order ID {0} is not bound to tracked order context")]
    UntrackedOrder(VenueOrderId),
    /// DeepX perpetual order quantities are base-denominated.
    #[error("DeepX tracked-order report cannot use quote-denominated quantity context")]
    QuoteQuantityUnsupported,
    /// The REST side conflicts with immutable local order context.
    #[error("DeepX tracked-order report side does not match local order context")]
    SideMismatch,
    /// The REST order type cannot be represented by this tracked-order boundary.
    #[error("unsupported DeepX tracked-order report type: {0}")]
    UnsupportedOrderType(String),
    /// The REST order type conflicts with immutable local order context.
    #[error("DeepX tracked-order report type does not match local order context")]
    OrderTypeMismatch,
    /// The REST quantity conflicts with immutable local order context.
    #[error("DeepX tracked-order report quantity does not match local order context")]
    QuantityMismatch,
    /// The REST limit price conflicts with immutable local order context.
    #[error("DeepX tracked-order report limit price does not match local order context")]
    LimitPriceMismatch,
    /// The REST post-only value cannot be represented by local boolean order context.
    #[error("unsupported DeepX tracked-order report post-only value: {0}")]
    UnsupportedPostOnly(String),
    /// The REST post-only value conflicts with immutable local order context.
    #[error("DeepX tracked-order report post-only value does not match local order context")]
    PostOnlyMismatch,
    /// The REST reduce-only value conflicts with immutable local order context.
    #[error("DeepX tracked-order report reduce-only value does not match local order context")]
    ReduceOnlyMismatch,
    /// The REST order contains invalid or internally inconsistent financial values.
    #[error("DeepX tracked-order report contains invalid financial values")]
    InvalidFinancialValues,
    /// Filled and remaining quantity do not reconstruct the original order quantity.
    #[error("DeepX tracked-order report filled and remaining quantities do not equal order size")]
    SizeAccountingMismatch,
    /// A nonzero filled quantity has no average fill price.
    #[error("DeepX tracked-order report is missing average fill price for executed quantity")]
    MissingAverageFillPrice,
    /// An unfilled order unexpectedly carries an average fill price.
    #[error("DeepX tracked-order report has average fill price without executed quantity")]
    UnexpectedAverageFillPrice,
    /// The REST status cannot be represented by this tracked-order boundary.
    #[error("unsupported DeepX tracked-order report status: {0}")]
    UnsupportedStatus(String),
    /// The REST status conflicts with its filled and remaining quantities.
    #[error("DeepX tracked-order report status conflicts with executed quantities")]
    StatusQuantityMismatch,
    /// The REST order has no last-update timestamp required by the framework report.
    #[error("DeepX tracked-order report is missing updatedTime")]
    MissingUpdatedTime,
    /// A REST timestamp is invalid or outside the framework timestamp range.
    #[error("DeepX tracked-order report has invalid {0}")]
    InvalidTimestamp(&'static str),
    /// The last-update timestamp precedes order acceptance.
    #[error("DeepX tracked-order report updatedTime precedes createTime")]
    TimestampOrderMismatch,
    /// Filled quantity cannot be represented at the immutable local quantity precision.
    #[error("DeepX tracked-order report filled quantity cannot be represented: {0}")]
    FilledQuantityConversion(String),
    /// Filled quantity would lose information at the immutable local quantity precision.
    #[error("DeepX tracked-order report filled quantity exceeds local quantity precision")]
    FilledQuantityPrecisionLoss,
}

/// Errors raised while converting validated DeepX position lifecycles into current reports.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum DeepXPositionReportError {
    /// The execution configuration no longer contains a subaccount identity.
    #[error("DeepX position reports require a configured subaccount")]
    MissingConfiguredSubaccount,
    /// The requested instrument was absent from the immutable startup catalog.
    #[error("DeepX position report has no validated perpetual market for {0}")]
    UnknownInstrument(InstrumentId),
    /// The startup catalog's forward and reverse market identities disagree.
    #[error("DeepX position report market identity snapshot is inconsistent")]
    MarketIdentityMismatch,
    /// A REST position belongs to another subaccount.
    #[error("DeepX position report belongs to another subaccount")]
    SubaccountMismatch,
    /// A REST position belongs to a market absent from the immutable startup catalog.
    #[error("DeepX position report contains unknown perpetual market ID {0}")]
    UnknownMarket(u64),
    /// A REST lifecycle status cannot be classified as current or historical.
    #[error("unsupported DeepX position report status: {0}")]
    UnsupportedStatus(String),
    /// More than one current net position was returned for a market.
    #[error("DeepX position report contains multiple open positions for market ID {0}")]
    DuplicateOpenMarket(u64),
    /// The position quantity could not be represented by the startup instrument precision.
    #[error("invalid DeepX position report quantity: {0}")]
    QuantityConversion(String),
    /// The position quantity would be rounded at the startup instrument precision.
    #[error("DeepX position report quantity loses precision")]
    QuantityPrecisionLoss,
    /// The position quantity is not an exact multiple of the startup instrument increment.
    #[error("DeepX position report quantity does not align with instrument increment")]
    QuantityIncrementMismatch,
    /// A position update timestamp is malformed or before the Unix epoch.
    #[error("invalid DeepX position report updatedAt timestamp")]
    InvalidTimestamp,
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
    /// The client does not own a restored durable transaction runtime.
    #[error("DeepX transaction runtime has not been initialized")]
    TransactionRuntimeNotInitialized,
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

/// Errors raised by the client-owned durable transaction runtime.
#[derive(Debug, Error)]
pub enum DeepXTransactionRuntimeError {
    /// The client does not own a restored durable transaction runtime.
    #[error("DeepX transaction runtime has not been initialized")]
    NotInitialized,
    /// RPC endpoint or capability evidence does not match execution startup.
    #[error(transparent)]
    Startup(#[from] DeepXExecutionStartupError),
    /// Finalized runtime-bound chain time could not be observed.
    #[error(transparent)]
    ChainTime(#[from] DeepXFinalizedChainTimeError),
    /// The retained signing snapshot service is unavailable or refreshing.
    #[error(transparent)]
    SnapshotService(#[from] DeepXRuntimeSnapshotServiceError),
    /// The nonce allocator and retained signer lease identify different accounts.
    #[error("DeepX transaction runtime signer identity mismatch")]
    SignerIdentityMismatch,
    /// The retained signing snapshot differs from startup runtime evidence.
    #[error("DeepX transaction runtime signing snapshot identity mismatch")]
    SnapshotIdentityMismatch,
    /// The PostgreSQL signer lease is no longer current.
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

    fn tracked_venue_order_id(
        &self,
        client_order_id: &ClientOrderId,
    ) -> Result<Option<VenueOrderId>, DeepXOrderContextError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| DeepXOrderContextError::LockPoisoned)?
            .tracked_venue_by_client
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
    const REQUIRED: [DeepXExecutionStartupEvidence; 8] = [
        DeepXExecutionStartupEvidence::InstrumentsLoaded,
        DeepXExecutionStartupEvidence::OrderContextRestored,
        DeepXExecutionStartupEvidence::RuntimeValidated,
        DeepXExecutionStartupEvidence::AccountOwnershipValidated,
        DeepXExecutionStartupEvidence::AccountStreamConfirmed,
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

#[derive(Debug)]
struct DeepXExecutionTransactionRuntime {
    store: DeepXPostgresTransactionStore,
    lease: DeepXPostgresSignerLease,
    nonce_allocator: DeepXTimestampNonceAllocator,
    snapshot_service: DeepXRuntimeSnapshotService,
    restored: Vec<DeepXRestoredTransactionRecord>,
}

/// Fail-closed DeepX execution client foundation.
///
/// This type owns execution identity, event construction, and verified read-only report queries.
/// Order commands and autonomous network startup remain disabled until their complete protocol and
/// recovery semantics are proven.
#[derive(Debug)]
pub struct DeepXExecutionClient {
    core: ExecutionClientCore,
    config: DeepXExecutionClientConfig,
    credential: DeepXPrivateKey,
    http: DeepXHttpClient,
    emitter: ExecutionEventEmitter,
    query_tasks: TaskGroup,
    query_epoch: Arc<Mutex<bool>>,
    perpetual_market_ids: HashMap<InstrumentId, u64>,
    perpetual_instrument_ids: HashMap<u64, InstrumentId>,
    perpetual_report_metadata: HashMap<InstrumentId, DeepXPerpetualReportMetadata>,
    order_contexts: DeepXOrderContextRegistry,
    trade_dedup: DeepXTradeDedup<TRADE_DEDUP_CAPACITY>,
    startup: DeepXExecutionStartup,
    runtime_snapshot: Option<RuntimeSnapshot>,
    account_ownership: Option<DeepXAccountOwnershipProof>,
    transaction_runtime: Option<DeepXExecutionTransactionRuntime>,
    startup_account_subscription: Option<DeepXWsConfirmedAccountSubscription>,
    startup_account_event_id: Option<UUID4>,
}

impl DeepXExecutionClient {
    fn resolve_tracked_order_report_query(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<(VenueOrderId, InstrumentId, u64)> {
        let venue_order_id = match (cmd.client_order_id, cmd.venue_order_id) {
            (None, None) => {
                anyhow::bail!(
                    "DeepX order status report requires a client order ID or venue order ID"
                )
            }
            (Some(client_order_id), requested_venue_order_id) => {
                let bound_venue_order_id = self
                    .order_contexts
                    .tracked_venue_order_id(&client_order_id)?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "DeepX client order ID {client_order_id} has no tracked venue order ID"
                        )
                    })?;
                if requested_venue_order_id
                    .is_some_and(|requested| requested != bound_venue_order_id)
                {
                    anyhow::bail!("DeepX order status report client and venue order IDs conflict");
                }
                bound_venue_order_id
            }
            (None, Some(venue_order_id)) => venue_order_id,
        };
        let context = match self
            .order_contexts
            .route(cmd.client_order_id, Some(venue_order_id))?
        {
            DeepXExecutionUpdateRoute::Tracked(context)
            | DeepXExecutionUpdateRoute::Terminal(context) => context,
            DeepXExecutionUpdateRoute::RegisteredExternal(_)
            | DeepXExecutionUpdateRoute::External => {
                anyhow::bail!(
                    "DeepX venue order ID {venue_order_id} is not bound to tracked order context"
                )
            }
        };
        let instrument_id = context.identity.instrument_id;
        if cmd
            .instrument_id
            .is_some_and(|requested| requested != instrument_id)
        {
            anyhow::bail!(
                "DeepX order status report instrument conflicts with tracked order context"
            );
        }
        let market_id = self
            .perpetual_market_ids
            .get(&instrument_id)
            .copied()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "DeepX order status report has no validated perpetual market for {instrument_id}"
                )
            })?;
        anyhow::ensure!(
            self.perpetual_instrument_ids.get(&market_id) == Some(&instrument_id),
            "DeepX order status report market identity snapshot is inconsistent",
        );
        Ok((venue_order_id, instrument_id, market_id))
    }

    fn prepare_tracked_order_query(
        &self,
        cmd: &QueryOrder,
    ) -> anyhow::Result<DeepXTrackedOrderQuery> {
        anyhow::ensure!(
            self.core.is_connected(),
            "DeepX order query requires a connected execution client"
        );
        anyhow::ensure!(
            *self
                .query_epoch
                .lock()
                .map_err(|_| anyhow::anyhow!("DeepX order query epoch lock is poisoned"))?,
            "DeepX order query task generation is inactive"
        );
        anyhow::ensure!(
            cmd.trader_id == self.core.trader_id,
            "DeepX order query trader ID mismatch"
        );
        anyhow::ensure!(
            cmd.client_id
                .is_none_or(|client_id| client_id == self.core.client_id),
            "DeepX order query client ID mismatch"
        );
        anyhow::ensure!(
            cmd.params.as_ref().is_none_or(|params| params.is_empty()),
            "DeepX order query parameters are unsupported"
        );
        let report_query = GenerateOrderStatusReport::new(
            cmd.command_id,
            cmd.ts_init,
            Some(cmd.instrument_id),
            Some(cmd.client_order_id),
            cmd.venue_order_id,
            None,
            cmd.correlation_id,
        );
        let (venue_order_id, instrument_id, market_id) =
            self.resolve_tracked_order_report_query(&report_query)?;
        let context = match self
            .order_contexts
            .route(Some(cmd.client_order_id), Some(venue_order_id))?
        {
            DeepXExecutionUpdateRoute::Tracked(context)
            | DeepXExecutionUpdateRoute::Terminal(context) => context,
            DeepXExecutionUpdateRoute::RegisteredExternal(_)
            | DeepXExecutionUpdateRoute::External => {
                anyhow::bail!("DeepX order query requires tracked order context")
            }
        };
        anyhow::ensure!(
            context.identity.instrument_id == instrument_id,
            "DeepX order query context instrument mismatch"
        );
        anyhow::ensure!(
            context.identity.strategy_id == cmd.strategy_id,
            "DeepX order query strategy ID mismatch"
        );
        let subaccount =
            self.config.subaccount_id.clone().ok_or_else(|| {
                anyhow::anyhow!("DeepX order query requires a configured subaccount")
            })?;
        Ok(DeepXTrackedOrderQuery {
            account_id: self.core.account_id,
            subaccount,
            market_id,
            venue_order_id,
            context,
            ts_init: cmd.ts_init,
        })
    }

    #[allow(
        dead_code,
        reason = "reserved for the fixture-gated tracked-order reconciliation path"
    )]
    pub(crate) fn build_tracked_order_status_report(
        &self,
        record: &DeepXPerpOrderRecord,
        expected_market_id: u64,
        venue_order_id: VenueOrderId,
        ts_init: UnixNanos,
    ) -> Result<OrderStatusReport, DeepXTrackedOrderReportError> {
        let subaccount = self
            .config
            .subaccount_id
            .as_deref()
            .ok_or(DeepXTrackedOrderReportError::MissingConfiguredSubaccount)?;
        if !record.owner.eq_ignore_ascii_case(subaccount) {
            return Err(DeepXTrackedOrderReportError::SubaccountMismatch);
        }
        if record.market_id != expected_market_id {
            return Err(DeepXTrackedOrderReportError::MarketMismatch {
                expected: expected_market_id,
                received: record.market_id,
            });
        }
        if record.order_id != venue_order_id.as_str() {
            return Err(DeepXTrackedOrderReportError::VenueOrderIdMismatch);
        }
        let context = match self.order_contexts.route(None, Some(venue_order_id))? {
            DeepXExecutionUpdateRoute::Tracked(context)
            | DeepXExecutionUpdateRoute::Terminal(context) => context,
            DeepXExecutionUpdateRoute::RegisteredExternal(_)
            | DeepXExecutionUpdateRoute::External => {
                return Err(DeepXTrackedOrderReportError::UntrackedOrder(venue_order_id));
            }
        };
        Self::build_tracked_order_status_report_from_context(
            record,
            expected_market_id,
            venue_order_id,
            ts_init,
            self.core.account_id,
            subaccount,
            context,
        )
    }

    fn build_tracked_order_status_report_from_context(
        record: &DeepXPerpOrderRecord,
        expected_market_id: u64,
        venue_order_id: VenueOrderId,
        ts_init: UnixNanos,
        account_id: AccountId,
        subaccount: &str,
        context: OrderContext,
    ) -> Result<OrderStatusReport, DeepXTrackedOrderReportError> {
        if !record.owner.eq_ignore_ascii_case(subaccount) {
            return Err(DeepXTrackedOrderReportError::SubaccountMismatch);
        }
        if record.market_id != expected_market_id {
            return Err(DeepXTrackedOrderReportError::MarketMismatch {
                expected: expected_market_id,
                received: record.market_id,
            });
        }
        if record.order_id != venue_order_id.as_str() {
            return Err(DeepXTrackedOrderReportError::VenueOrderIdMismatch);
        }
        if context.is_quote_quantity {
            return Err(DeepXTrackedOrderReportError::QuoteQuantityUnsupported);
        }

        let order_side = if record.is_long {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        if order_side != context.identity.order_side {
            return Err(DeepXTrackedOrderReportError::SideMismatch);
        }
        let order_type = match record.order_type.as_str() {
            "Limit" => OrderType::Limit,
            "Market" => OrderType::Market,
            value => {
                return Err(DeepXTrackedOrderReportError::UnsupportedOrderType(
                    value.to_string(),
                ));
            }
        };
        if order_type != context.identity.order_type {
            return Err(DeepXTrackedOrderReportError::OrderTypeMismatch);
        }
        if record.size != context.quantity.as_decimal() {
            return Err(DeepXTrackedOrderReportError::QuantityMismatch);
        }
        let report_price = match order_type {
            OrderType::Limit => match context.price {
                Some(price) if price.as_decimal() == record.price => Some(price),
                _ => return Err(DeepXTrackedOrderReportError::LimitPriceMismatch),
            },
            OrderType::Market => None,
            _ => unreachable!("order type was restricted above"),
        };
        let post_only = match record.post_only.as_str() {
            "None" => false,
            "MustPostOnly" => true,
            value => {
                return Err(DeepXTrackedOrderReportError::UnsupportedPostOnly(
                    value.to_string(),
                ));
            }
        };
        if post_only != context.is_post_only {
            return Err(DeepXTrackedOrderReportError::PostOnlyMismatch);
        }
        if record.reduce_only != context.is_reduce_only {
            return Err(DeepXTrackedOrderReportError::ReduceOnlyMismatch);
        }
        if record.size <= rust_decimal::Decimal::ZERO
            || record.size_filled.is_sign_negative()
            || record.size_remain.is_sign_negative()
            || order_type == OrderType::Limit && record.price <= rust_decimal::Decimal::ZERO
            || record
                .avg_fill_price
                .is_some_and(|price| price <= rust_decimal::Decimal::ZERO)
        {
            return Err(DeepXTrackedOrderReportError::InvalidFinancialValues);
        }
        if record.size_filled + record.size_remain != record.size {
            return Err(DeepXTrackedOrderReportError::SizeAccountingMismatch);
        }
        let avg_fill_price = match (record.size_filled.is_zero(), record.avg_fill_price) {
            (false, Some(price)) => Some(price),
            (false, None) => {
                return Err(DeepXTrackedOrderReportError::MissingAverageFillPrice);
            }
            (true, Some(_)) => {
                return Err(DeepXTrackedOrderReportError::UnexpectedAverageFillPrice);
            }
            (true, None) => None,
        };
        let order_status = match record.status.as_str() {
            "Open" => OrderStatus::Accepted,
            "PartiallyFilled" => OrderStatus::PartiallyFilled,
            "Filled" => OrderStatus::Filled,
            "Canceled" => OrderStatus::Canceled,
            "Rejected" => OrderStatus::Rejected,
            "Expired" => OrderStatus::Expired,
            value => {
                return Err(DeepXTrackedOrderReportError::UnsupportedStatus(
                    value.to_string(),
                ));
            }
        };
        let status_quantities_match = match order_status {
            OrderStatus::Accepted | OrderStatus::Rejected => {
                record.size_filled.is_zero() && record.size_remain == record.size
            }
            OrderStatus::PartiallyFilled => {
                !record.size_filled.is_zero() && !record.size_remain.is_zero()
            }
            OrderStatus::Filled => {
                record.size_filled == record.size && record.size_remain.is_zero()
            }
            OrderStatus::Canceled | OrderStatus::Expired => true,
            _ => unreachable!("order status was restricted above"),
        };
        if !status_quantities_match {
            return Err(DeepXTrackedOrderReportError::StatusQuantityMismatch);
        }

        let ts_accepted = parse_tracked_order_report_timestamp("createTime", &record.create_time)?;
        let updated_time = record
            .updated_time
            .as_deref()
            .ok_or(DeepXTrackedOrderReportError::MissingUpdatedTime)?;
        let ts_last = parse_tracked_order_report_timestamp("updatedTime", updated_time)?;
        if ts_last < ts_accepted {
            return Err(DeepXTrackedOrderReportError::TimestampOrderMismatch);
        }
        let filled_qty = Quantity::from_decimal_dp(record.size_filled, context.quantity.precision)
            .map_err(|e| DeepXTrackedOrderReportError::FilledQuantityConversion(e.to_string()))?;
        if filled_qty.as_decimal() != record.size_filled {
            return Err(DeepXTrackedOrderReportError::FilledQuantityPrecisionLoss);
        }

        let mut report = OrderStatusReport::new(
            account_id,
            context.identity.instrument_id,
            Some(context.identity.client_order_id),
            venue_order_id,
            Some(order_side),
            order_type,
            context.time_in_force,
            order_status,
            context.quantity,
            filled_qty,
            ts_accepted,
            ts_last,
            ts_init,
            None,
        )
        .with_post_only(post_only)
        .with_reduce_only(record.reduce_only);
        if let Some(price) = report_price {
            report = report.with_price(price);
        }
        if let Some(avg_fill_price) = avg_fill_price {
            report = report.with_avg_px(avg_fill_price);
        }
        if order_status == OrderStatus::Canceled && !record.cancel_reason.is_empty() {
            report = report.with_cancel_reason(record.cancel_reason.clone());
        }
        Ok(report)
    }

    pub(crate) fn build_position_status_report(
        &self,
        record: &DeepXPerpPositionRecord,
        ts_init: UnixNanos,
    ) -> Result<Option<PositionStatusReport>, DeepXPositionReportError> {
        let subaccount = self
            .config
            .subaccount_id
            .as_deref()
            .ok_or(DeepXPositionReportError::MissingConfiguredSubaccount)?;
        if !record.owner.eq_ignore_ascii_case(subaccount) {
            return Err(DeepXPositionReportError::SubaccountMismatch);
        }
        let instrument_id = self
            .perpetual_instrument_ids
            .get(&record.market_id)
            .copied()
            .ok_or(DeepXPositionReportError::UnknownMarket(record.market_id))?;
        if self.perpetual_market_ids.get(&instrument_id) != Some(&record.market_id) {
            return Err(DeepXPositionReportError::MarketIdentityMismatch);
        }
        let metadata = self
            .perpetual_report_metadata
            .get(&instrument_id)
            .copied()
            .ok_or(DeepXPositionReportError::MarketIdentityMismatch)?;

        match record.status.as_str() {
            "Closed" => return Ok(None),
            "Open" => {}
            value => {
                return Err(DeepXPositionReportError::UnsupportedStatus(
                    value.to_string(),
                ));
            }
        }
        if record.base_asset_amount <= rust_decimal::Decimal::ZERO {
            return Err(DeepXPositionReportError::QuantityConversion(
                "open position quantity must be positive".to_string(),
            ));
        }
        if record.base_asset_amount % metadata.size_increment != rust_decimal::Decimal::ZERO {
            return Err(DeepXPositionReportError::QuantityIncrementMismatch);
        }
        let quantity = Quantity::from_decimal_dp(record.base_asset_amount, metadata.size_precision)
            .map_err(|e| DeepXPositionReportError::QuantityConversion(e.to_string()))?;
        if quantity.as_decimal() != record.base_asset_amount {
            return Err(DeepXPositionReportError::QuantityPrecisionLoss);
        }
        let timestamp = record
            .updated_at
            .parse::<jiff::Timestamp>()
            .map_err(|_| DeepXPositionReportError::InvalidTimestamp)?;
        let nanos = u64::try_from(timestamp.as_nanosecond())
            .map_err(|_| DeepXPositionReportError::InvalidTimestamp)?;
        let position_side = if record.is_long {
            PositionSide::Long
        } else {
            PositionSide::Short
        };

        Ok(Some(PositionStatusReport::new(
            self.core.account_id,
            instrument_id,
            position_side,
            quantity,
            UnixNanos::from(nanos),
            ts_init,
            None,
            None,
            Some(record.entry_price),
        )))
    }

    pub(crate) fn build_fill_report(
        &self,
        record: &DeepXPerpAccountTradeRecord,
        ts_init: UnixNanos,
    ) -> Result<FillReport, DeepXFillReportError> {
        let instrument_id = self
            .perpetual_instrument_ids
            .get(&record.market_id)
            .copied()
            .ok_or(DeepXFillReportError::UnknownMarket(record.market_id))?;
        if self.perpetual_market_ids.get(&instrument_id) != Some(&record.market_id) {
            return Err(DeepXFillReportError::MarketIdentityMismatch);
        }
        let metadata = self
            .perpetual_report_metadata
            .get(&instrument_id)
            .copied()
            .ok_or(DeepXFillReportError::MarketIdentityMismatch)?;
        let order_side = if record.is_long {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let taker_side = match record.taker.as_str() {
            "Buyer" => OrderSide::Buy,
            "Seller" => OrderSide::Sell,
            value => return Err(DeepXFillReportError::UnsupportedTaker(value.to_string())),
        };
        if !matches!(record.filled_direction.as_str(), "Long" | "Short" | "Both") {
            return Err(DeepXFillReportError::UnsupportedFilledDirection(
                record.filled_direction.clone(),
            ));
        }
        let liquidity_side = if taker_side == order_side {
            LiquiditySide::Taker
        } else {
            LiquiditySide::Maker
        };

        if record.size % metadata.size_increment != rust_decimal::Decimal::ZERO {
            return Err(DeepXFillReportError::QuantityIncrementMismatch);
        }
        let last_qty = Quantity::from_decimal_dp(record.size, metadata.size_precision)
            .map_err(|e| DeepXFillReportError::QuantityConversion(e.to_string()))?;
        if last_qty.as_decimal() != record.size {
            return Err(DeepXFillReportError::QuantityPrecisionLoss);
        }
        if record.price % metadata.price_increment != rust_decimal::Decimal::ZERO {
            return Err(DeepXFillReportError::PriceIncrementMismatch);
        }
        let last_px = Price::from_decimal_dp(record.price, metadata.price_precision)
            .map_err(|e| DeepXFillReportError::PriceConversion(e.to_string()))?;
        if last_px.as_decimal() != record.price {
            return Err(DeepXFillReportError::PricePrecisionLoss);
        }

        if !record.fee_asset.is_empty()
            && !record
                .fee_asset
                .eq_ignore_ascii_case(metadata.quote_currency.code.as_str())
        {
            return Err(DeepXFillReportError::FeeAssetMismatch);
        }
        let fee_rate = match liquidity_side {
            LiquiditySide::Maker => metadata.maker_fee_rate,
            LiquiditySide::Taker => metadata.taker_fee_rate,
            _ => unreachable!("liquidity side was restricted above"),
        };
        if fee_rate > rust_decimal::Decimal::ZERO && record.fee > rust_decimal::Decimal::ZERO
            || fee_rate < rust_decimal::Decimal::ZERO && record.fee < rust_decimal::Decimal::ZERO
            || fee_rate == rust_decimal::Decimal::ZERO && record.fee != rust_decimal::Decimal::ZERO
        {
            return Err(DeepXFillReportError::FeeSignMismatch);
        }
        let commission_value = -record.fee;
        let commission = Money::from_decimal(commission_value, metadata.quote_currency)
            .map_err(|e| DeepXFillReportError::CommissionConversion(e.to_string()))?;
        if commission.as_decimal() != commission_value {
            return Err(DeepXFillReportError::CommissionPrecisionLoss);
        }

        let venue_order_id = VenueOrderId::new(record.order_id.as_str());
        let client_order_id = match self.order_contexts.route(None, Some(venue_order_id))? {
            DeepXExecutionUpdateRoute::Tracked(context)
            | DeepXExecutionUpdateRoute::Terminal(context) => {
                if context.identity.instrument_id != instrument_id {
                    return Err(DeepXFillReportError::ContextInstrumentMismatch);
                }
                if context.identity.order_side != order_side {
                    return Err(DeepXFillReportError::ContextSideMismatch);
                }
                Some(context.identity.client_order_id)
            }
            DeepXExecutionUpdateRoute::RegisteredExternal(context) => {
                if context.instrument_id != instrument_id {
                    return Err(DeepXFillReportError::ContextInstrumentMismatch);
                }
                Some(context.client_order_id)
            }
            DeepXExecutionUpdateRoute::External => None,
        };
        let timestamp = record
            .created_at
            .parse::<jiff::Timestamp>()
            .map_err(|_| DeepXFillReportError::InvalidTimestamp)?;
        let nanos = u64::try_from(timestamp.as_nanosecond())
            .map_err(|_| DeepXFillReportError::InvalidTimestamp)?;

        Ok(FillReport::new(
            self.core.account_id,
            instrument_id,
            venue_order_id,
            TradeId::new(record.id.to_string()),
            order_side,
            last_qty,
            last_px,
            commission,
            liquidity_side,
            client_order_id,
            None,
            UnixNanos::from(nanos),
            ts_init,
            None,
        ))
    }

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
        let http = DeepXHttpClient::from_network_config(
            &config.network,
            Some(config.http_timeout_secs),
            config.proxy_url.clone(),
        )?;
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
            http,
            emitter,
            query_tasks: TaskGroup::default(),
            query_epoch: Arc::new(Mutex::new(false)),
            perpetual_market_ids: HashMap::new(),
            perpetual_instrument_ids: HashMap::new(),
            perpetual_report_metadata: HashMap::new(),
            order_contexts: DeepXOrderContextRegistry::default(),
            trade_dedup: DeepXTradeDedup::default(),
            startup: DeepXExecutionStartup::default(),
            runtime_snapshot: None,
            account_ownership: None,
            transaction_runtime: None,
            startup_account_subscription: None,
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
        let mut perpetual_market_ids = HashMap::new();
        let mut perpetual_instrument_ids = HashMap::new();
        let mut perpetual_report_metadata = HashMap::new();
        for instrument_id in provider.instrument_ids() {
            let Some(DeepXMarketMetadata::Perpetual(market)) = provider.market(&instrument_id)
            else {
                continue;
            };
            if market.order_spec_step_size <= rust_decimal::Decimal::ZERO {
                return Err(DeepXExecutionStartupError::MarketCatalogIdentityMismatch);
            }
            let size_increment = Quantity::from_decimal(market.order_spec_step_size)
                .map_err(|_| DeepXExecutionStartupError::MarketCatalogIdentityMismatch)?;
            let price_increment = Price::from_decimal(market.order_spec_tick_size)
                .map_err(|_| DeepXExecutionStartupError::MarketCatalogIdentityMismatch)?;
            let report_metadata = DeepXPerpetualReportMetadata {
                price_precision: price_increment.precision,
                size_precision: size_increment.precision,
                price_increment: market.order_spec_tick_size,
                size_increment: market.order_spec_step_size,
                quote_currency: Currency::get_or_create_crypto(
                    market.quote_symbol.to_ascii_uppercase(),
                ),
                maker_fee_rate: market.maker_fee_rate,
                taker_fee_rate: market.taker_fee_rate,
            };
            if provider.perpetual_instrument_id(market.id) != Some(instrument_id)
                || perpetual_market_ids
                    .insert(instrument_id, market.id)
                    .is_some()
                || perpetual_instrument_ids
                    .insert(market.id, instrument_id)
                    .is_some()
                || perpetual_report_metadata
                    .insert(instrument_id, report_metadata)
                    .is_some()
            {
                return Err(DeepXExecutionStartupError::MarketCatalogIdentityMismatch);
            }
        }
        self.startup
            .record(DeepXExecutionStartupEvidence::InstrumentsLoaded)?;
        self.perpetual_market_ids = perpetual_market_ids;
        self.perpetual_instrument_ids = perpetual_instrument_ids;
        self.perpetual_report_metadata = perpetual_report_metadata;
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
        self.runtime_snapshot = Some(applied.snapshot().clone());
        Ok(())
    }

    /// Verifies signer-bound subaccount ownership and advances the startup gate.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for ownership validation and the proof matches
    /// both the configured signing key and configured AccountId20 subaccount.
    pub fn record_account_ownership_validated(
        &mut self,
        proof: DeepXAccountOwnershipProof,
    ) -> Result<(), DeepXExecutionStartupError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::AccountOwnershipValidated)?;
        if derive_signer_account_id(&self.credential).ok() != Some(proof.signer()) {
            return Err(DeepXExecutionStartupError::AccountOwnershipSignerMismatch);
        }
        let configured = format!("0x{}", nautilus_core::hex::encode(proof.subaccount()));
        if !self
            .config
            .subaccount_id
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case(&configured))
        {
            return Err(DeepXExecutionStartupError::AccountOwnershipSubaccountMismatch);
        }
        self.startup
            .record(DeepXExecutionStartupEvidence::AccountOwnershipValidated)?;
        self.account_ownership = Some(proof);
        Ok(())
    }

    /// Verifies the current address-scoped account subscription and advances the startup gate.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for account-stream confirmation, the supplied
    /// receipt is current for its connection, and it matches the configured owned subaccount.
    pub fn record_account_stream_confirmed(
        &mut self,
        connection: &DeepXWsAccountConnection,
        subscription: DeepXWsConfirmedAccountSubscription,
    ) -> Result<(), DeepXExecutionStartupError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::AccountStreamConfirmed)?;
        if !connection.is_current_subscription(subscription)
            || self
                .account_ownership
                .as_ref()
                .map(|proof| proof.subaccount())
                != Some(subscription.subaccount())
        {
            return Err(DeepXExecutionStartupError::AccountStreamSubscriptionMismatch);
        }
        self.startup
            .record(DeepXExecutionStartupEvidence::AccountStreamConfirmed)?;
        self.startup_account_subscription = Some(subscription);
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

    /// Acquires exclusive signer ownership and restores transaction state from PostgreSQL.
    ///
    /// The runtime remains owned by this client until startup is reset, stopped, or disconnected.
    /// Initializing it does not authorize signing or submission.
    ///
    /// # Errors
    ///
    /// Returns an error unless a durable database is configured, finalized runtime and account
    /// ownership evidence are current, and the signer lease and complete durable record set can be
    /// restored exactly once for this startup epoch.
    pub async fn initialize_transaction_runtime(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.core.is_connected(),
            "DeepX transaction runtime must be initialized before connection"
        );
        anyhow::ensure!(
            self.transaction_runtime.is_none(),
            "DeepX transaction runtime is already initialized"
        );
        let options = self
            .config
            .postgres_cache_database_config
            .clone()
            .ok_or_else(|| {
                anyhow::anyhow!("DeepX transaction runtime requires a PostgreSQL cache database")
            })?;
        let snapshot = self.runtime_snapshot.clone().ok_or_else(|| {
            anyhow::anyhow!("DeepX transaction runtime requires validated runtime evidence")
        })?;
        anyhow::ensure!(
            self.account_ownership.is_some(),
            "DeepX transaction runtime requires validated account ownership"
        );

        let signer = derive_signer_account_id(&self.credential)?;
        let store = DeepXPostgresTransactionStore::connect(options).await?;
        let lease = store.acquire_signer_lease(signer).await?;
        let (nonce_allocator, restored) = self
            .restore_timestamp_nonce_allocator(&store, &lease)
            .await?;
        self.transaction_runtime = Some(DeepXExecutionTransactionRuntime {
            store,
            lease,
            nonce_allocator,
            snapshot_service: DeepXRuntimeSnapshotService::new(snapshot),
            restored,
        });
        Ok(())
    }

    /// Returns whether this startup epoch owns a restored durable transaction runtime.
    #[must_use]
    pub const fn transaction_runtime_is_initialized(&self) -> bool {
        self.transaction_runtime.is_some()
    }

    /// Returns the number of durable signer records restored into the current runtime.
    #[must_use]
    pub fn restored_transaction_count(&self) -> Option<usize> {
        self.transaction_runtime
            .as_ref()
            .map(|runtime| runtime.restored.len())
    }

    /// Verifies that the current durable runtime still owns its signer lease.
    ///
    /// # Errors
    ///
    /// Returns an error when no runtime is initialized, its signer identities differ, or the
    /// PostgreSQL lease is no longer current.
    pub async fn verify_transaction_runtime(&self) -> Result<(), DeepXTransactionRuntimeError> {
        let runtime = self
            .transaction_runtime
            .as_ref()
            .ok_or(DeepXTransactionRuntimeError::NotInitialized)?;
        if runtime.nonce_allocator.signer() != runtime.lease.signer() {
            return Err(DeepXTransactionRuntimeError::SignerIdentityMismatch);
        }
        let permit = runtime.snapshot_service.acquire()?;
        if self
            .runtime_snapshot
            .as_ref()
            .map(RuntimeSnapshot::identity)
            != Some(permit.snapshot().identity())
        {
            return Err(DeepXTransactionRuntimeError::SnapshotIdentityMismatch);
        }
        runtime.store.verify_signer_lease(&runtime.lease).await?;
        Ok(())
    }

    /// Refreshes approved runtime evidence and reads finalized chain time through the owned runtime.
    ///
    /// This operation performs no nonce allocation, signing, persistence mutation, or submission.
    ///
    /// # Errors
    ///
    /// Returns an error unless the runtime is initialized, RPC evidence still matches startup, the
    /// observed runtime remains fixture-approved, and `Timestamp.Now` is valid at the exact
    /// finalized Watch checkpoint.
    pub async fn observe_finalized_chain_time(
        &self,
        endpoints: &DeepXValidatedRpcEndpoints,
        capabilities: &DeepXValidatedRpcMethodCapabilities,
    ) -> Result<DeepXFinalizedChainTimeEvidence, DeepXTransactionRuntimeError> {
        let runtime = self
            .transaction_runtime
            .as_ref()
            .ok_or(DeepXTransactionRuntimeError::NotInitialized)?;
        self.validate_rpc_evidence(endpoints, capabilities)?;
        Ok(observe_and_apply_finalized_chain_time(
            &self.config.network.environment,
            endpoints,
            capabilities,
            &runtime.snapshot_service,
        )
        .await?)
    }

    /// Verifies, emits, and records the account-state event for the current startup epoch.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for account-state initialization, the confirmed
    /// balance frame belongs to the current account subscription, and the event matches the
    /// configured execution account identity and type, or event dispatch fails.
    pub fn record_account_state_initialized(
        &mut self,
        connection: &DeepXWsAccountConnection,
        frame: &DeepXWsConfirmedBalancesFrame,
        state: &AccountState,
    ) -> Result<(), DeepXExecutionStartupError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::AccountStateInitialized)?;
        if self.startup_account_subscription != Some(frame.subscription())
            || !connection.is_current_subscription(frame.subscription())
        {
            return Err(DeepXExecutionStartupError::AccountStreamSubscriptionMismatch);
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
    /// Returns an error unless startup is waiting for mass reconciliation, the client owns its
    /// durable transaction runtime and signer lease, the account subscription is still current,
    /// and every durable transaction is complete.
    pub async fn record_mass_reconciliation_completed(
        &mut self,
        connection: &DeepXWsAccountConnection,
        subscription: DeepXWsConfirmedAccountSubscription,
        endpoints: &DeepXValidatedRpcEndpoints,
        capabilities: &DeepXValidatedRpcMethodCapabilities,
    ) -> Result<(), DeepXMassReconciliationError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::MassReconciliationCompleted)?;
        let runtime = self
            .transaction_runtime
            .as_ref()
            .ok_or(DeepXMassReconciliationError::TransactionRuntimeNotInitialized)?;
        self.reconcile_durable_transactions(
            connection,
            subscription,
            endpoints,
            capabilities,
            &runtime.store,
            &runtime.lease,
        )
        .await?;
        self.startup
            .record(DeepXExecutionStartupEvidence::MassReconciliationCompleted)?;
        Ok(())
    }

    async fn reconcile_durable_transactions<S>(
        &self,
        connection: &DeepXWsAccountConnection,
        subscription: DeepXWsConfirmedAccountSubscription,
        endpoints: &DeepXValidatedRpcEndpoints,
        capabilities: &DeepXValidatedRpcMethodCapabilities,
        store: &S,
        lease: &S::Lease,
    ) -> Result<(), DeepXMassReconciliationError>
    where
        S: DeepXTransactionStore,
    {
        if self.startup_account_subscription != Some(subscription)
            || !connection.is_current_subscription(subscription)
        {
            return Err(DeepXExecutionStartupError::AccountStreamSubscriptionMismatch.into());
        }
        if lease.signer() != derive_signer_account_id(&self.credential)? {
            return Err(DeepXMassReconciliationError::SignerLeaseMismatch);
        }
        self.validate_rpc_evidence(endpoints, capabilities)?;
        let snapshot =
            self.runtime_snapshot
                .as_ref()
                .ok_or(DeepXExecutionStartupError::OutOfOrder {
                    expected: DeepXExecutionStartupEvidence::RuntimeValidated,
                    received: DeepXExecutionStartupEvidence::MassReconciliationCompleted,
                })?;
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
                DeepXTransactionState::NotIncluded => {
                    let committed = if matches!(
                        item.record().identity().operation(),
                        Some(DeepXTransactionOperation::PerpPlace { .. })
                    ) {
                        let verifier = DeepXPerpPlaceCallVerifier::new(
                            snapshot.clone(),
                            self.credential.clone(),
                        )?;
                        reconcile_not_included_checkpoint_with_observer(
                            endpoints,
                            capabilities,
                            snapshot,
                            store,
                            lease,
                            &item,
                            self.config.recovery_blocks_per_range,
                            DeepXDurableRecoveryObserver::PerpPlace(&verifier),
                        )
                        .await?
                    } else if matches!(
                        item.record().identity().operation(),
                        Some(DeepXTransactionOperation::PerpClose { .. })
                    ) {
                        let verifier = DeepXPerpCloseCallVerifier::new(
                            snapshot.clone(),
                            self.credential.clone(),
                        )?;
                        reconcile_not_included_checkpoint_with_observer(
                            endpoints,
                            capabilities,
                            snapshot,
                            store,
                            lease,
                            &item,
                            self.config.recovery_blocks_per_range,
                            DeepXDurableRecoveryObserver::PerpClose(&verifier),
                        )
                        .await?
                    } else if matches!(
                        item.record().identity().operation(),
                        Some(DeepXTransactionOperation::PerpProfitAndLossPoint { .. })
                    ) {
                        let verifier = DeepXPerpProfitAndLossPointCallVerifier::new(
                            snapshot.clone(),
                            self.credential.clone(),
                        )?;
                        reconcile_not_included_checkpoint_with_observer(
                            endpoints,
                            capabilities,
                            snapshot,
                            store,
                            lease,
                            &item,
                            self.config.recovery_blocks_per_range,
                            DeepXDurableRecoveryObserver::PerpProfitAndLossPoint(&verifier),
                        )
                        .await?
                    } else if matches!(
                        item.record().identity().operation(),
                        Some(DeepXTransactionOperation::SpotPlace { .. })
                    ) {
                        let verifier = DeepXSpotPlaceCallVerifier::new(
                            snapshot.clone(),
                            self.credential.clone(),
                        )?;
                        reconcile_not_included_checkpoint_with_observer(
                            endpoints,
                            capabilities,
                            snapshot,
                            store,
                            lease,
                            &item,
                            self.config.recovery_blocks_per_range,
                            DeepXDurableRecoveryObserver::SpotPlace(&verifier),
                        )
                        .await?
                    } else if matches!(
                        item.record().identity().operation(),
                        Some(DeepXTransactionOperation::SpotCancel { .. })
                    ) {
                        let verifier = DeepXSpotCancelCallVerifier::new(
                            snapshot.clone(),
                            self.credential.clone(),
                        )?;
                        reconcile_not_included_checkpoint_with_observer(
                            endpoints,
                            capabilities,
                            snapshot,
                            store,
                            lease,
                            &item,
                            self.config.recovery_blocks_per_range,
                            DeepXDurableRecoveryObserver::OrdinarySpotCancel(&verifier),
                        )
                        .await?
                    } else if matches!(
                        item.record().identity().operation(),
                        Some(DeepXTransactionOperation::PerpCancel { .. })
                    ) {
                        let verifier = DeepXPerpCancelCallVerifier::new(
                            snapshot.clone(),
                            self.credential.clone(),
                        )?;
                        reconcile_not_included_checkpoint_with_observer(
                            endpoints,
                            capabilities,
                            snapshot,
                            store,
                            lease,
                            &item,
                            self.config.recovery_blocks_per_range,
                            DeepXDurableRecoveryObserver::PerpCancel(&verifier),
                        )
                        .await?
                    } else {
                        reconcile_not_included_checkpoint(
                            endpoints,
                            capabilities,
                            snapshot,
                            store,
                            lease,
                            &item,
                            self.config.recovery_blocks_per_range,
                        )
                        .await?
                    };
                    committed.record().recovery_action()
                }
                _ => item.record().recovery_action(),
            };
            if action != DeepXTransactionRecoveryAction::Complete {
                return Err(DeepXMassReconciliationError::UnresolvedTransaction {
                    client_order_id,
                    action,
                });
            }
        }
        Ok(())
    }

    /// Verifies the configured account is registered and completes the startup gate.
    ///
    /// # Errors
    ///
    /// Returns an error unless startup is waiting for account registration, the account-stream
    /// subscription is still current, and the configured account exists in the shared execution
    /// cache.
    pub fn complete_account_registration(
        &mut self,
        connection: &DeepXWsAccountConnection,
        subscription: DeepXWsConfirmedAccountSubscription,
    ) -> Result<(), DeepXExecutionStartupError> {
        self.startup
            .validate_next(DeepXExecutionStartupEvidence::AccountRegistered)?;
        if self.startup_account_subscription != Some(subscription)
            || !connection.is_current_subscription(subscription)
        {
            return Err(DeepXExecutionStartupError::AccountStreamSubscriptionMismatch);
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
        if let Ok(mut active) = self.query_epoch.lock() {
            *active = false;
        }
        self.query_tasks.abort();
        self.core.set_disconnected();
        self.startup.reset();
        self.runtime_snapshot = None;
        self.account_ownership = None;
        self.transaction_runtime = None;
        self.startup_account_subscription = None;
        self.startup_account_event_id = None;
        self.perpetual_market_ids.clear();
        self.perpetual_instrument_ids.clear();
        self.perpetual_report_metadata.clear();
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

fn parse_tracked_order_report_timestamp(
    field: &'static str,
    value: &str,
) -> Result<UnixNanos, DeepXTrackedOrderReportError> {
    let timestamp = value
        .parse::<jiff::Timestamp>()
        .map_err(|_| DeepXTrackedOrderReportError::InvalidTimestamp(field))?;
    let nanos = u64::try_from(timestamp.as_nanosecond())
        .map_err(|_| DeepXTrackedOrderReportError::InvalidTimestamp(field))?;
    Ok(UnixNanos::from(nanos))
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
        if !self.query_tasks.is_open() {
            self.query_tasks
                .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
                .await?;
            self.query_tasks.start_generation()?;
        }
        *self
            .query_epoch
            .lock()
            .map_err(|_| anyhow::anyhow!("DeepX order query epoch lock is poisoned"))? = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.reset_startup();
        self.query_tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
            .await?;
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

    fn query_account(&self, cmd: QueryAccount) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.core.is_connected(),
            "DeepX account query requires a connected execution client"
        );
        anyhow::ensure!(
            cmd.trader_id == self.core.trader_id,
            "DeepX account query trader ID mismatch"
        );
        anyhow::ensure!(
            cmd.client_id
                .is_none_or(|client_id| client_id == self.core.client_id),
            "DeepX account query client ID mismatch"
        );
        anyhow::ensure!(
            cmd.account_id == self.core.account_id,
            "DeepX account query account ID mismatch"
        );
        anyhow::ensure!(
            cmd.params.as_ref().is_none_or(|params| params.is_empty()),
            "DeepX account query parameters are unsupported"
        );
        let startup_event_id = self
            .startup_account_event_id
            .ok_or_else(|| anyhow::anyhow!("DeepX account query has no current startup event"))?;
        let state = {
            let cache = self.core.try_cache().map_err(|_| {
                anyhow::anyhow!("DeepX account query cache is already mutably borrowed")
            })?;
            let account = cache.account(&self.core.account_id).ok_or_else(|| {
                anyhow::anyhow!("DeepX account query account is not registered in the cache")
            })?;
            let events = account.events();
            let baseline = events
                .iter()
                .position(|state| state.event_id == startup_event_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "DeepX account query current startup event is not registered in the cache"
                    )
                })?;
            let state = events[baseline..]
                .iter()
                .rev()
                .find(|state| state.is_reported)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!("DeepX account query has no current reported account state")
                })?;
            anyhow::ensure!(
                state.account_id == self.core.account_id
                    && state.account_type == self.core.account_type,
                "DeepX account query cached account identity mismatch"
            );
            state
        };
        self.emitter.try_send_account_state(state)
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let query = self.prepare_tracked_order_query(&cmd)?;
        let http = self.http.clone();
        let emitter = self.emitter.clone();
        let epoch = Arc::clone(&self.query_epoch);
        self.query_tasks.spawn(async move {
            let result = async {
                let record = http
                    .get_perp_order_by_id(
                        &query.subaccount,
                        query.market_id,
                        query.venue_order_id.as_str(),
                    )
                    .await?;
                Self::build_tracked_order_status_report_from_context(
                    &record,
                    query.market_id,
                    query.venue_order_id,
                    query.ts_init,
                    query.account_id,
                    &query.subaccount,
                    query.context,
                )
                .map_err(anyhow::Error::from)
            }
            .await;
            match result {
                Ok(report) => match epoch.lock() {
                    Ok(active) if *active => emitter.send_order_status_report(report),
                    Ok(_) => {}
                    Err(_) => log::error!("DeepX order query epoch lock is poisoned"),
                },
                Err(e) => log::error!("DeepX order query failed: {e}"),
            }
        })?;
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let (venue_order_id, _instrument_id, market_id) =
            self.resolve_tracked_order_report_query(cmd)?;
        let subaccount = self.config.subaccount_id.as_deref().ok_or_else(|| {
            anyhow::anyhow!("DeepX order status report requires a configured subaccount")
        })?;
        let record = self
            .http
            .get_perp_order_by_id(subaccount, market_id, venue_order_id.as_str())
            .await?;
        Ok(Some(self.build_tracked_order_status_report(
            &record,
            market_id,
            venue_order_id,
            cmd.ts_init,
        )?))
    }

    async fn generate_order_status_reports(
        &self,
        _cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        anyhow::bail!("DeepX order status reports are not operational")
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        anyhow::ensure!(
            !cmd.start
                .zip(cmd.end)
                .is_some_and(|(start, end)| start > end),
            "DeepX fill report start must not exceed end",
        );
        let subaccount = self
            .config
            .subaccount_id
            .as_deref()
            .ok_or(DeepXFillReportError::MissingConfiguredSubaccount)?;
        if let Some(venue_order_id) = cmd.venue_order_id
            && (venue_order_id.as_str().is_empty()
                || !venue_order_id
                    .as_str()
                    .bytes()
                    .all(|value| value.is_ascii_digit())
                || venue_order_id.as_str().parse::<u64>().is_err())
        {
            return Err(DeepXFillReportError::InvalidVenueOrderId.into());
        }
        let market_id = cmd
            .instrument_id
            .map(|instrument_id| {
                let market_id = self
                    .perpetual_market_ids
                    .get(&instrument_id)
                    .copied()
                    .ok_or(DeepXFillReportError::UnknownInstrument(instrument_id))?;
                if self.perpetual_instrument_ids.get(&market_id) != Some(&instrument_id)
                    || !self.perpetual_report_metadata.contains_key(&instrument_id)
                {
                    return Err(DeepXFillReportError::MarketIdentityMismatch);
                }
                Ok(market_id)
            })
            .transpose()?;
        if market_id.is_none()
            && (self.perpetual_market_ids.is_empty()
                || self.perpetual_market_ids.len() != self.perpetual_instrument_ids.len()
                || self.perpetual_market_ids.len() != self.perpetual_report_metadata.len())
        {
            return Err(DeepXFillReportError::MarketIdentityMismatch.into());
        }

        let pages = self
            .http
            .get_perp_account_trade_pages(
                &DeepXPerpAccountTradesRequest {
                    subaccount: subaccount.to_string(),
                    order_id: None,
                    market_id,
                    is_long: None,
                    cursor: None,
                    sort: DeepXAccountSortOrder::Ascending,
                    start_ms: cmd.start.map(|value| value.as_u64() / 1_000_000),
                    end_ms: cmd.end.map(|value| value.as_u64() / 1_000_000),
                    page_size: Some(FILL_REPORT_PAGE_SIZE),
                },
                FILL_REPORT_MAX_PAGES,
            )
            .await?;
        let mut reports = Vec::new();
        for record in pages.into_iter().flat_map(|page| page.items) {
            if cmd
                .venue_order_id
                .is_some_and(|venue_order_id| venue_order_id.as_str() != record.order_id)
            {
                continue;
            }
            let report = self.build_fill_report(&record, cmd.ts_init)?;
            if cmd.start.is_some_and(|start| report.ts_event < start)
                || cmd.end.is_some_and(|end| report.ts_event > end)
            {
                continue;
            }
            reports.push(report);
        }
        Ok(self.merge_validated_fill_reports(reports)?)
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        anyhow::ensure!(
            !cmd.start
                .zip(cmd.end)
                .is_some_and(|(start, end)| start > end),
            "DeepX position report start must not exceed end",
        );
        let subaccount = self
            .config
            .subaccount_id
            .as_deref()
            .ok_or(DeepXPositionReportError::MissingConfiguredSubaccount)?;
        let market_id = cmd
            .instrument_id
            .map(|instrument_id| {
                let market_id = self
                    .perpetual_market_ids
                    .get(&instrument_id)
                    .copied()
                    .ok_or(DeepXPositionReportError::UnknownInstrument(instrument_id))?;
                if self.perpetual_instrument_ids.get(&market_id) != Some(&instrument_id)
                    || !self.perpetual_report_metadata.contains_key(&instrument_id)
                {
                    return Err(DeepXPositionReportError::MarketIdentityMismatch);
                }
                Ok(market_id)
            })
            .transpose()?;
        if market_id.is_none()
            && (self.perpetual_market_ids.is_empty()
                || self.perpetual_market_ids.len() != self.perpetual_instrument_ids.len()
                || self.perpetual_market_ids.len() != self.perpetual_report_metadata.len())
        {
            return Err(DeepXPositionReportError::MarketIdentityMismatch.into());
        }

        let pages = self
            .http
            .get_perp_position_pages(
                &DeepXPerpPositionsRequest {
                    subaccount: subaccount.to_string(),
                    market_id,
                    only_closed: Some(false),
                    cursor: None,
                    page_size: Some(POSITION_REPORT_PAGE_SIZE),
                },
                POSITION_REPORT_MAX_PAGES,
            )
            .await?;
        let mut open_markets = HashSet::new();
        let mut reports = Vec::new();
        for record in pages.into_iter().flat_map(|page| page.items) {
            if let Some(report) = self.build_position_status_report(&record, cmd.ts_init)? {
                if !open_markets.insert(record.market_id) {
                    return Err(
                        DeepXPositionReportError::DuplicateOpenMarket(record.market_id).into(),
                    );
                }
                reports.push(report);
            }
        }
        if let Some(start) = cmd.start {
            reports.retain(|report| report.ts_last >= start);
        }
        if let Some(end) = cmd.end {
            reports.retain(|report| report.ts_last <= end);
        }
        reports.sort_by_key(|report| report.instrument_id);
        Ok(reports)
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
        num::NonZeroUsize,
        rc::Rc,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum::{
        Json, Router,
        extract::ws::{Message as WsMessage, WebSocketUpgrade},
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
            AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, PositionSide,
            TimeInForce,
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
        account::verify_account_ownership,
        common::consts::DEEPX_TESTNET_GENESIS_HASH,
        config::{DeepXObservedRpcEndpoint, validate_rpc_endpoint_identities},
        http::{
            DeepXAccountPage, DeepXApiResponse, DeepXPerpOrderRecord, DeepXSubaccountProfile,
            DeepXWalletSubaccounts,
        },
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
        websocket::{
            DeepXWsAccountConnection, DeepXWsConfirmedAccountSubscription,
            DeepXWsConfirmedBalancesFrame,
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
    const PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE: &str =
        include_str!("../test_data/http/testnet/perp_history_orders_account_market_3.json");
    const PERP_POSITIONS_ACCOUNT_RESPONSE: &str =
        include_str!("../test_data/http/testnet/perp_positions_account_market_3.json");
    const PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE: &str =
        include_str!("../test_data/http/testnet/perp_account_trades_account_market_3.json");
    const TEST_SUBACCOUNT: &str = "0x1111111111111111111111111111111111111111";

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

    fn tracked_limit_order() -> OrderAny {
        OrderTestBuilder::new(OrderType::Limit)
            .client_order_id(ClientOrderId::from("O-DEEPX-REPORT"))
            .strategy_id(StrategyId::from("S-DEEPX-001"))
            .instrument_id(InstrumentId::from("ETH-USDC-PERP.DEEPX"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.3"))
            .price(Price::from("2499.05"))
            .time_in_force(TimeInForce::Gtc)
            .build()
    }

    fn tracked_limit_order_record() -> DeepXPerpOrderRecord {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
            serde_json::from_str(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE).unwrap();
        let mut record = response.data.items[2].clone();
        record.owner = TEST_SUBACCOUNT.to_string();
        record
    }

    fn tracked_limit_report_fixture() -> (DeepXExecutionClient, DeepXPerpOrderRecord, VenueOrderId)
    {
        let client = test_client();
        let order = tracked_limit_order();
        let record = tracked_limit_order_record();
        let venue_order_id = VenueOrderId::from(record.order_id.as_str());
        client.register_order(&order).unwrap();
        client
            .bind_tracked_venue_order_id(order.client_order_id(), venue_order_id)
            .unwrap();
        (client, record, venue_order_id)
    }

    async fn report_test_client(path: &'static str, mut response: Value) -> DeepXExecutionClient {
        const SPOT_RESPONSE: &str = include_str!("../test_data/http/testnet/spot_markets.json");
        const PERP_RESPONSE: &str = include_str!("../test_data/http/testnet/perp_markets.json");
        for item in response["data"]["items"].as_array_mut().unwrap() {
            item["owner"] = TEST_SUBACCOUNT.into();
        }
        let router = Router::new()
            .route(
                "/internal/v1/market/spot/markets",
                get(|| async { SPOT_RESPONSE }),
            )
            .route(
                "/internal/v1/market/perp/markets",
                get(|| async { PERP_RESPONSE }),
            )
            .route(
                path,
                get(move || {
                    let response = response.clone();
                    async move { Json(response) }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let base_url = format!("http://{address}");
        let cache = Rc::new(RefCell::new(Cache::default()));
        let core = ExecutionClientCore::new(
            TraderId::from("TRADER-001"),
            ClientId::from("DEEPX"),
            *DEEPX_VENUE,
            OmsType::Netting,
            AccountId::from("DEEPX-001"),
            AccountType::Margin,
            None,
            cache,
        );
        let config = DeepXExecutionClientConfig {
            subaccount_id: Some(TEST_SUBACCOUNT.to_string()),
            private_key: Some(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            ),
            http_timeout_secs: 5,
            network: crate::config::DeepXNetworkConfig {
                base_url_rest: Some(base_url),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut client = DeepXExecutionClient::new(core, config).unwrap();
        let mut provider = DeepXMarketProvider::new(client.http.clone());
        provider.load_all().await.unwrap();
        client.record_instruments_loaded(&provider).unwrap();
        client
    }

    async fn position_report_test_client(response: Value) -> DeepXExecutionClient {
        report_test_client("/internal/v1/account/position", response).await
    }

    async fn fill_report_test_client(response: Value) -> DeepXExecutionClient {
        report_test_client("/internal/v1/account/perp/trades", response).await
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
            subaccount_id: Some(TEST_SUBACCOUNT.to_string()),
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

    fn account_ownership_proof(
        credential: &DeepXPrivateKey,
        subaccount: &str,
    ) -> DeepXAccountOwnershipProof {
        let signer = derive_signer_account_id(credential).unwrap();
        let wallet = format!("0x{}", hex::encode(signer));
        let directory = DeepXWalletSubaccounts::new(wallet.clone(), vec![subaccount.to_string()]);
        let profile = DeepXSubaccountProfile {
            authority: wallet,
            address: subaccount.to_string(),
            name: "test".to_string(),
            status: "Active".to_string(),
            spot_positions: Vec::new(),
            next_order_id: 1,
            spot_margin_trading_enabled: false,
            margin_strategy: "Cross".to_string(),
            height: 1,
            created_at: 1,
        };
        verify_account_ownership(credential, subaccount, &directory, &profile).unwrap()
    }

    fn record_account_ownership(client: &mut DeepXExecutionClient) {
        let subaccount = client.config.subaccount_id.clone().unwrap();
        let proof = account_ownership_proof(&client.credential, &subaccount);
        client.record_account_ownership_validated(proof).unwrap();
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
                                    "state_getStorage",
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

    fn test_runtime_snapshot() -> RuntimeSnapshot {
        RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap(),
            366,
            1,
            &metadata_fixture_bytes(),
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

    fn not_included_record(
        client: &DeepXExecutionClient,
        snapshot: &RuntimeSnapshot,
    ) -> DeepXTransactionRecord {
        let signer = derive_signer_account_id(&client.credential).unwrap();
        let runtime = snapshot.identity();
        let mut record = DeepXTransactionRecord::created(DeepXTransactionIdentity::new(
            ClientOrderId::from("O-DEEPX-NOT-INCLUDED"),
            signer,
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId { value: 42 },
            DeepXDirectRuntimeIdentity::from(runtime),
        ));
        let bytes = vec![12, 1, 2, 3];
        record
            .record_signed(&SignedPalletExtrinsic {
                extrinsic_hash: BlakeTwo256.hash(&bytes).0,
                bytes,
                signer,
                nonce: 42,
                runtime: runtime.clone(),
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
                    "state_getStorage",
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
                    "state_getStorage",
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

        assert_eq!(
            client.runtime_snapshot.as_ref().unwrap().identity(),
            applied.snapshot().identity(),
        );
        assert!(
            client
                .startup
                .validate_next(DeepXExecutionStartupEvidence::AccountOwnershipValidated)
                .is_ok()
        );
        client.reset_startup();
        assert!(client.runtime_snapshot.is_none());
        assert!(client.account_ownership.is_none());
    }

    #[rstest]
    fn account_ownership_startup_rejects_foreign_signer_and_subaccount_without_advancing() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        let subaccount = client.config.subaccount_id.clone().unwrap();
        let foreign_key = DeepXPrivateKey::new(
            "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &crate::common::DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let foreign_signer = account_ownership_proof(&foreign_key, &subaccount);

        assert_eq!(
            client.record_account_ownership_validated(foreign_signer),
            Err(DeepXExecutionStartupError::AccountOwnershipSignerMismatch),
        );

        let proof = account_ownership_proof(&client.credential, &subaccount);
        client.config.subaccount_id =
            Some("0x2222222222222222222222222222222222222222".to_string());
        assert_eq!(
            client.record_account_ownership_validated(proof),
            Err(DeepXExecutionStartupError::AccountOwnershipSubaccountMismatch),
        );
        assert!(client.account_ownership.is_none());
        assert_eq!(
            client
                .startup
                .validate_next(DeepXExecutionStartupEvidence::AccountOwnershipValidated),
            Ok(()),
        );

        client.config.subaccount_id = Some(subaccount);
        client.record_account_ownership_validated(proof).unwrap();
        assert_eq!(client.account_ownership, Some(proof));
    }

    #[tokio::test]
    async fn account_stream_startup_accepts_current_subscription() {
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
        record_account_ownership(&mut client);
        let (connection, subscription, _) = confirmed_account_stream().await;

        client
            .record_account_stream_confirmed(&connection, subscription)
            .unwrap();

        assert!(
            client
                .startup
                .validate_next(DeepXExecutionStartupEvidence::AccountStateInitialized)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn transaction_runtime_requires_configured_durable_database() {
        let mut client = test_client();

        let error = client.initialize_transaction_runtime().await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("requires a PostgreSQL cache database")
        );
        assert!(!client.transaction_runtime_is_initialized());
        assert_eq!(client.restored_transaction_count(), None);
        assert!(client.verify_transaction_runtime().await.is_err());
    }

    #[tokio::test]
    async fn account_stream_startup_rejects_stale_subscription_without_advancing() {
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
        record_account_ownership(&mut client);
        let (mut stale_connection, stale_subscription, _) = confirmed_account_stream().await;
        stale_connection.close().await.unwrap();

        assert_eq!(
            client.record_account_stream_confirmed(&stale_connection, stale_subscription),
            Err(DeepXExecutionStartupError::AccountStreamSubscriptionMismatch),
        );

        let (connection, subscription, _) = confirmed_account_stream().await;
        client
            .record_account_stream_confirmed(&connection, subscription)
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

        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");
        assert_eq!(client.perpetual_market_ids.get(&instrument_id), Some(&3));
        assert_eq!(
            client.perpetual_instrument_ids.get(&3),
            Some(&instrument_id)
        );
        let metadata = client
            .perpetual_report_metadata
            .get(&instrument_id)
            .unwrap();
        assert_eq!(metadata.size_precision, 4);
        assert_eq!(metadata.price_precision, 4);
        assert_eq!(metadata.size_increment, rust_decimal::Decimal::new(10, 4));
        assert_eq!(metadata.price_increment, rust_decimal::Decimal::new(100, 4));
        assert_eq!(metadata.quote_currency, Currency::USDC());
        assert!(client.restore_order_contexts([]).is_ok());
    }

    #[tokio::test]
    async fn instrument_catalog_snapshot_is_cleared_on_startup_reset() {
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
        let http_client = DeepXHttpClient::new(format!("http://{address}"), Some(5), None).unwrap();
        let mut provider = DeepXMarketProvider::new(http_client);
        provider.load_all().await.unwrap();
        let mut client = test_client();
        client.config.network.base_url_rest = Some(format!("http://{address}"));
        client.record_instruments_loaded(&provider).unwrap();

        client.reset_startup();

        assert!(client.perpetual_market_ids.is_empty());
        assert!(client.perpetual_instrument_ids.is_empty());
        assert!(client.perpetual_report_metadata.is_empty());
        assert_eq!(
            client
                .startup
                .validate_next(DeepXExecutionStartupEvidence::InstrumentsLoaded),
            Ok(()),
        );
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

    async fn advance_through_mass_reconciliation(
        client: &mut DeepXExecutionClient,
    ) -> (
        AccountState,
        DeepXWsAccountConnection,
        DeepXWsConfirmedAccountSubscription,
    ) {
        record_instruments_loaded(client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        client.runtime_snapshot = Some(test_runtime_snapshot());
        record_account_ownership(client);
        let (connection, subscription, frame) = confirmed_account_stream().await;
        client
            .record_account_stream_confirmed(&connection, subscription)
            .unwrap();
        let state = test_account_state();
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);
        client
            .record_account_state_initialized(&connection, &frame, &state)
            .unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::MassReconciliationCompleted)
            .unwrap();
        (state, connection, subscription)
    }

    async fn advance_to_mass_reconciliation(
        client: &mut DeepXExecutionClient,
    ) -> (
        DeepXWsAccountConnection,
        DeepXWsConfirmedAccountSubscription,
    ) {
        record_instruments_loaded(client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        client.runtime_snapshot = Some(test_runtime_snapshot());
        record_account_ownership(client);
        let (connection, subscription, frame) = confirmed_account_stream().await;
        client
            .record_account_stream_confirmed(&connection, subscription)
            .unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);
        client
            .record_account_state_initialized(&connection, &frame, &test_account_state())
            .unwrap();
        (connection, subscription)
    }

    async fn record_mass_reconciliation_completed_with_store<S>(
        client: &mut DeepXExecutionClient,
        connection: &DeepXWsAccountConnection,
        subscription: DeepXWsConfirmedAccountSubscription,
        endpoints: &DeepXValidatedRpcEndpoints,
        capabilities: &DeepXValidatedRpcMethodCapabilities,
        store: &S,
        lease: &S::Lease,
    ) -> Result<(), DeepXMassReconciliationError>
    where
        S: DeepXTransactionStore,
    {
        client
            .startup
            .validate_next(DeepXExecutionStartupEvidence::MassReconciliationCompleted)?;
        client
            .reconcile_durable_transactions(
                connection,
                subscription,
                endpoints,
                capabilities,
                store,
                lease,
            )
            .await?;
        client
            .startup
            .record(DeepXExecutionStartupEvidence::MassReconciliationCompleted)?;
        Ok(())
    }

    #[tokio::test]
    async fn mass_reconciliation_requires_owned_transaction_runtime_without_advancing() {
        let mut client = test_client();
        let (rpc_url, endpoints, capabilities, _) = applied_runtime_evidence().await;
        configure_rpc_url(&mut client, rpc_url);
        let (connection, subscription) = advance_to_mass_reconciliation(&mut client).await;

        assert!(matches!(
            client
                .record_mass_reconciliation_completed(
                    &connection,
                    subscription,
                    &endpoints,
                    &capabilities,
                )
                .await,
            Err(DeepXMassReconciliationError::TransactionRuntimeNotInitialized),
        ));
        assert_eq!(client.startup.completed_steps, 6);
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn mass_reconciliation_accepts_empty_complete_store_snapshot() {
        let mut client = test_client();
        let (rpc_url, endpoints, capabilities, _) = applied_runtime_evidence().await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
        let store = TestTransactionStore {
            restored: Vec::new(),
        };
        let signer = derive_signer_account_id(&client.credential).unwrap();
        let lease = store.acquire_signer_lease(signer).await.unwrap();

        record_mass_reconciliation_completed_with_store(
            &mut client,
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
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
        let store = TestTransactionStore {
            restored: Vec::new(),
        };
        let lease = TestSignerLease { signer: [42; 20] };

        assert!(matches!(
            record_mass_reconciliation_completed_with_store(
                &mut client,
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
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
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
            record_mass_reconciliation_completed_with_store(
                &mut client,
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
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
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
            record_mass_reconciliation_completed_with_store(
                &mut client,
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
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            record_mass_reconciliation_completed_with_store(
                &mut client,
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
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            record_mass_reconciliation_completed_with_store(
                &mut client,
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
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        record_mass_reconciliation_completed_with_store(
            &mut client,
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
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            record_mass_reconciliation_completed_with_store(
                &mut client,
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
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            record_mass_reconciliation_completed_with_store(
                &mut client,
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
        let snapshot = test_runtime_snapshot();
        let record = not_included_record(&client, &snapshot);
        let signed_bytes = record.signed_extrinsic().unwrap().bytes().to_vec();
        let (rpc_url, endpoints, capabilities) = recovery_evidence(&[&signed_bytes]).await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            record_mass_reconciliation_completed_with_store(
                &mut client,
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
        let snapshot = test_runtime_snapshot();
        let record = not_included_record(&client, &snapshot);
        let (rpc_url, endpoints, capabilities) = recovery_evidence(&[]).await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();

        assert!(matches!(
            record_mass_reconciliation_completed_with_store(
                &mut client,
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

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn mass_reconciliation_selects_verified_operation_observer(
        #[case] corrupt: bool,
        #[values(0, 1, 2, 3, 4, 5, 6)] operation: u8,
    ) {
        let mut client = test_client();
        let snapshot = test_runtime_snapshot();
        let params = crate::signing::DeepXPerpPlaceParams {
            subaccount: [0x11; 20],
            market_id: 3,
            is_long: true,
            size: 1,
            price: 100,
            order_type: crate::signing::DeepXPerpOrderType::Limit(
                crate::signing::DeepXTimeInForce::Gtc,
            ),
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: crate::signing::DeepXPostOnlyParam::None,
        };
        let service = crate::signing::DeepXRuntimeSnapshotService::new(snapshot.clone());
        let mut signed = crate::signing::sign_perp_place_order(
            &service.acquire().unwrap(),
            &client.credential,
            params,
            42,
        )
        .unwrap();
        let mut identity = DeepXTransactionIdentity::new_perp_place(
            ClientOrderId::new("O-DEEPX-PERP-RECOVERY"),
            signed.signer(),
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId { value: 42 },
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
            params,
        );
        let permit = service.acquire().unwrap();
        match operation {
            0 => {}
            1 | 2 => {
                let params = crate::signing::DeepXSpotPlaceParams {
                    subaccount: [0x11; 20],
                    pair: [0x22; 32],
                    is_buy: operation == 1,
                    quote_amount: [0x33; 32],
                    base_amount: [0x44; 32],
                    order_type: crate::signing::DeepXSpotOrderType::Limit(
                        crate::signing::DeepXTimeInForce::Gtc,
                    ),
                    post_only: crate::signing::DeepXPostOnlyParam::None,
                    reduce_only: false,
                };
                signed =
                    crate::signing::sign_spot_place_order(&permit, &client.credential, params, 42)
                        .unwrap();
                identity = DeepXTransactionIdentity::new_spot_place(
                    ClientOrderId::new("O-DEEPX-SPOT-RECOVERY"),
                    signed.signer(),
                    InstrumentId::from("ETH-USDC.DEEPX"),
                    if params.is_buy {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    },
                    DeepXNonceReservation::TimestampOrderId { value: 42 },
                    DeepXDirectRuntimeIdentity::from(snapshot.identity()),
                    params,
                );
            }
            3 | 4 => {
                let params = crate::signing::DeepXPerpCancelParams {
                    subaccount: [0x11; 20],
                    order_id: 7,
                    market_id: 3,
                    fast_cancel: operation == 4,
                };
                signed = crate::signing::sign_perp_cancel(&permit, &client.credential, params, 42)
                    .unwrap();
                identity = DeepXTransactionIdentity::new_perp_cancel(
                    ClientOrderId::new("O-DEEPX-PERP-CANCEL-RECOVERY"),
                    signed.signer(),
                    InstrumentId::from("ETH-USDC-PERP.DEEPX"),
                    OrderSide::Buy,
                    DeepXNonceReservation::TimestampOrderId { value: 42 },
                    DeepXDirectRuntimeIdentity::from(snapshot.identity()),
                    params.subaccount,
                    params.order_id,
                    params.market_id,
                    params.fast_cancel,
                );
            }
            _ => {
                let params = crate::signing::DeepXSpotCancelParams {
                    subaccount: [0x11; 20],
                    pair: [0x22; 32],
                    order_id: 7,
                    is_buy: true,
                    fast_cancel: operation == 6,
                };
                signed = crate::signing::sign_spot_cancel(&permit, &client.credential, params, 42)
                    .unwrap();
                identity = DeepXTransactionIdentity::new_spot_cancel(
                    ClientOrderId::new("O-DEEPX-SPOT-CANCEL-RECOVERY"),
                    signed.signer(),
                    InstrumentId::from("ETH-USDC.DEEPX"),
                    OrderSide::Buy,
                    DeepXNonceReservation::TimestampOrderId { value: 42 },
                    DeepXDirectRuntimeIdentity::from(snapshot.identity()),
                    params,
                );
            }
        }
        if corrupt {
            signed.bytes[30] ^= 1;
            signed.extrinsic_hash = BlakeTwo256.hash(&signed.bytes).0;
        }
        let mut record = DeepXTransactionRecord::created(identity);
        record.record_signed(&signed).unwrap();
        record
            .apply_observation(DeepXTransactionObservation::SubmissionStarted)
            .unwrap();
        record
            .apply_observation(DeepXTransactionObservation::NotIncluded(
                not_included_record(&client, &snapshot)
                    .lifecycle()
                    .absence()
                    .unwrap(),
            ))
            .unwrap();
        let (rpc_url, endpoints, capabilities) = recovery_evidence(&[]).await;
        configure_rpc_url(&mut client, rpc_url);
        let (protocol, session) = advance_to_mass_reconciliation(&mut client).await;
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        let result = record_mass_reconciliation_completed_with_store(
            &mut client,
            &protocol,
            session,
            &endpoints,
            &capabilities,
            &store,
            &lease,
        )
        .await;
        if operation == 6 {
            assert!(matches!(result, Err(DeepXMassReconciliationError::FinalizedRecovery(
                DeepXFinalizedRecoveryCommitError::Watch(DeepXTransactionWatchError::SpotEventVerification(
                    crate::transaction::DeepXSpotCancelEventVerificationError::FastCancelUnsupported
                ))
            ))));
            assert_eq!(store.current_revision(), 4);
            assert_eq!(
                store.persisted_record().lifecycle().state(),
                DeepXTransactionState::NotIncluded
            );
        } else if corrupt {
            assert!(matches!(
                result,
                Err(DeepXMassReconciliationError::FinalizedRecovery(
                    DeepXFinalizedRecoveryCommitError::Binding(_)
                ))
            ));
            assert_eq!(store.current_revision(), 4);
            assert_eq!(
                store.persisted_record().lifecycle().state(),
                DeepXTransactionState::NotIncluded
            );
        } else {
            assert!(matches!(
                result,
                Err(DeepXMassReconciliationError::UnresolvedTransaction {
                    action: DeepXTransactionRecoveryAction::OperatorActionRequired,
                    ..
                })
            ));
            assert_eq!(store.current_revision(), 5);
            assert_eq!(
                store.persisted_record().lifecycle().state(),
                DeepXTransactionState::ActionRequired
            );
        }
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn mass_reconciliation_rejects_stale_subscription_without_mutation() {
        let mut client = test_client();
        let record = submitting_record(&client);
        let signed_bytes = record.signed_extrinsic().unwrap().bytes().to_vec();
        let (rpc_url, endpoints, capabilities) = recovery_evidence(&[&signed_bytes]).await;
        configure_rpc_url(&mut client, rpc_url);
        let (mut connection, subscription) = advance_to_mass_reconciliation(&mut client).await;
        let store = FinalityTestStore::new(4, &record);
        let lease = store
            .acquire_signer_lease(record.identity().signer())
            .await
            .unwrap();
        connection.close().await.unwrap();

        assert!(matches!(
            record_mass_reconciliation_completed_with_store(
                &mut client,
                &connection,
                subscription,
                &endpoints,
                &capabilities,
                &store,
                &lease,
            )
            .await,
            Err(DeepXMassReconciliationError::Startup(
                DeepXExecutionStartupError::AccountStreamSubscriptionMismatch,
            )),
        ));
        assert_eq!(store.current_revision(), 4);
        assert_eq!(
            store.persisted_record().lifecycle().state(),
            DeepXTransactionState::Submitting,
        );
        assert_eq!(client.startup.completed_steps, 6);
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

    fn account_query() -> QueryAccount {
        QueryAccount::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DEEPX")),
            AccountId::from("DEEPX-001"),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    fn tracked_order_query(order: &OrderAny, venue_order_id: Option<VenueOrderId>) -> QueryOrder {
        QueryOrder::new(
            order.trader_id(),
            Some(ClientId::from("DEEPX")),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            venue_order_id,
            UUID4::new(),
            UnixNanos::from(123_456_789),
            None,
            None,
        )
    }

    fn account_query_client() -> (
        DeepXExecutionClient,
        Rc<RefCell<Cache>>,
        tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
        AccountState,
    ) {
        let (mut client, cache) = test_client_with_cache();
        let state = test_account_state();
        register_test_account(&cache, state.clone());
        client.startup_account_event_id = Some(state.event_id);
        client.core.set_connected();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);
        (client, cache, receiver, state)
    }

    async fn confirmed_account_stream() -> (
        DeepXWsAccountConnection,
        DeepXWsConfirmedAccountSubscription,
        DeepXWsConfirmedBalancesFrame,
    ) {
        const SUBACCOUNT: &str = "0x1111111111111111111111111111111111111111";
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(|upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(|mut socket| async move {
                    let Some(Ok(WsMessage::Text(request))) = socket.recv().await else {
                        panic!("expected account subscription request");
                    };
                    let request: Value = serde_json::from_str(&request).unwrap();
                    assert_eq!(request["market"]["type"], "all");
                    assert_eq!(request["subscriptions"][0]["address"], SUBACCOUNT);
                    socket
                        .send(WsMessage::Text(
                            json!({
                                "type": "subscribed",
                                "market": {"type": "all"},
                                "subscriptions": [{
                                    "channel": "user_balances",
                                    "address": SUBACCOUNT
                                }],
                                "message": "Successfully subscribed to 1 channels"
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .unwrap();
                    socket
                        .send(WsMessage::Text(
                            json!({
                                "type": "data",
                                "channel": "user_balances",
                                "market": {"type": "all"},
                                "data": {"address": SUBACCOUNT, "assets": []},
                                "timestamp": 1_789_451_784_924_u64
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .unwrap();
                    while socket.recv().await.is_some() {}
                })
            }),
        );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let mut connection = DeepXWsAccountConnection::connect(
            &network,
            None,
            Duration::from_secs(1),
            NonZeroUsize::new(1).unwrap(),
        )
        .await
        .unwrap();
        let subscription = connection
            .subscribe_user_balances(SUBACCOUNT)
            .await
            .unwrap();
        let frame = connection.next_balances(subscription).await.unwrap();
        (connection, subscription, frame)
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
    fn tracked_order_report_preserves_validated_local_and_rest_evidence() {
        let (client, record, venue_order_id) = tracked_limit_report_fixture();
        let ts_init = UnixNanos::from(1_000_000);

        let report = client
            .build_tracked_order_status_report(&record, 3, venue_order_id, ts_init)
            .unwrap();

        assert_eq!(report.account_id, AccountId::from("DEEPX-001"));
        assert_eq!(
            report.instrument_id,
            InstrumentId::from("ETH-USDC-PERP.DEEPX")
        );
        assert_eq!(
            report.client_order_id,
            Some(ClientOrderId::from("O-DEEPX-REPORT"))
        );
        assert_eq!(report.venue_order_id, venue_order_id);
        assert_eq!(report.order_side, Some(OrderSide::Buy));
        assert_eq!(report.order_type, OrderType::Limit);
        assert_eq!(report.time_in_force, TimeInForce::Gtc);
        assert_eq!(report.order_status, OrderStatus::Filled);
        assert_eq!(report.quantity, Quantity::from("0.3"));
        assert_eq!(report.filled_qty, Quantity::from("0.3"));
        assert_eq!(report.price, Some(Price::from("2499.05")));
        assert_eq!(report.avg_px, record.avg_fill_price);
        assert!(!report.post_only);
        assert!(!report.reduce_only);
        assert_eq!(report.ts_init, ts_init);
        assert_eq!(
            report.ts_accepted,
            parse_tracked_order_report_timestamp("createTime", &record.create_time).unwrap()
        );
        assert_eq!(report.ts_last, report.ts_accepted);
    }

    #[rstest]
    #[case("Open", OrderStatus::Accepted)]
    #[case("PartiallyFilled", OrderStatus::PartiallyFilled)]
    #[case("Filled", OrderStatus::Filled)]
    #[case("Canceled", OrderStatus::Canceled)]
    #[case("Rejected", OrderStatus::Rejected)]
    #[case("Expired", OrderStatus::Expired)]
    fn tracked_order_report_maps_documented_chain_statuses(
        #[case] venue_status: &str,
        #[case] expected: OrderStatus,
    ) {
        let (client, mut record, venue_order_id) = tracked_limit_report_fixture();
        record.status = venue_status.to_string();
        if venue_status == "PartiallyFilled" {
            record.size_filled = "0.1".parse().unwrap();
            record.size_remain = "0.2".parse().unwrap();
        } else if venue_status != "Filled" {
            record.size_filled = rust_decimal::Decimal::ZERO;
            record.size_remain = record.size;
            record.avg_fill_price = None;
        }
        if venue_status == "Canceled" {
            record.cancel_reason = "UserCanceled".to_string();
        }

        let report = client
            .build_tracked_order_status_report(&record, 3, venue_order_id, UnixNanos::default())
            .unwrap();

        assert_eq!(report.order_status, expected);
        assert_eq!(
            report.cancel_reason.as_deref(),
            (venue_status == "Canceled").then_some("UserCanceled")
        );
    }

    #[rstest]
    fn tracked_order_report_accepts_retained_terminal_context() {
        let (client, record, venue_order_id) = tracked_limit_report_fixture();
        client
            .finish_order_context(&ClientOrderId::from("O-DEEPX-REPORT"))
            .unwrap();

        let report = client
            .build_tracked_order_status_report(&record, 3, venue_order_id, UnixNanos::default())
            .unwrap();

        assert_eq!(report.order_status, OrderStatus::Filled);
    }

    #[rstest]
    fn tracked_market_order_report_does_not_publish_venue_slippage_guard_as_price() {
        let client = test_client();
        let order = OrderTestBuilder::new(OrderType::Market)
            .client_order_id(ClientOrderId::from("O-DEEPX-MARKET-REPORT"))
            .strategy_id(StrategyId::from("S-DEEPX-001"))
            .instrument_id(InstrumentId::from("ETH-USDC-PERP.DEEPX"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.3"))
            .time_in_force(TimeInForce::Ioc)
            .build();
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
            serde_json::from_str(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE).unwrap();
        let mut record = response.data.items[0].clone();
        record.owner = TEST_SUBACCOUNT.to_string();
        let venue_order_id = VenueOrderId::from(record.order_id.as_str());
        client.register_order(&order).unwrap();
        client
            .bind_tracked_venue_order_id(order.client_order_id(), venue_order_id)
            .unwrap();

        let report = client
            .build_tracked_order_status_report(&record, 3, venue_order_id, UnixNanos::default())
            .unwrap();

        assert_eq!(report.order_type, OrderType::Market);
        assert_eq!(report.time_in_force, TimeInForce::Ioc);
        assert_eq!(report.price, None);
    }

    #[rstest]
    fn tracked_limit_order_report_maps_confirmed_post_only_context() {
        let client = test_client();
        let order = OrderTestBuilder::new(OrderType::Limit)
            .client_order_id(ClientOrderId::from("O-DEEPX-POST-ONLY-REPORT"))
            .strategy_id(StrategyId::from("S-DEEPX-001"))
            .instrument_id(InstrumentId::from("ETH-USDC-PERP.DEEPX"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.3"))
            .price(Price::from("2499.05"))
            .time_in_force(TimeInForce::Gtc)
            .post_only(true)
            .build();
        let mut record = tracked_limit_order_record();
        record.post_only = "MustPostOnly".to_string();
        let venue_order_id = VenueOrderId::from(record.order_id.as_str());
        client.register_order(&order).unwrap();
        client
            .bind_tracked_venue_order_id(order.client_order_id(), venue_order_id)
            .unwrap();

        let report = client
            .build_tracked_order_status_report(&record, 3, venue_order_id, UnixNanos::default())
            .unwrap();

        assert!(report.post_only);
    }

    #[rstest]
    fn tracked_order_report_rejects_identity_and_immutable_term_mismatches() {
        let (client, record, venue_order_id) = tracked_limit_report_fixture();

        let mut mismatched = record.clone();
        mismatched.owner = "0x2222222222222222222222222222222222222222".to_string();
        assert_eq!(
            client.build_tracked_order_status_report(
                &mismatched,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::SubaccountMismatch),
        );
        assert_eq!(
            client.build_tracked_order_status_report(
                &record,
                4,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::MarketMismatch {
                expected: 4,
                received: 3,
            }),
        );
        assert_eq!(
            client.build_tracked_order_status_report(
                &record,
                3,
                VenueOrderId::from("999"),
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::VenueOrderIdMismatch),
        );

        let mut mismatched = record.clone();
        mismatched.is_long = false;
        assert!(matches!(
            client.build_tracked_order_status_report(
                &mismatched,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::SideMismatch),
        ));
        let mut mismatched = record.clone();
        mismatched.order_type = "Market".to_string();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &mismatched,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::OrderTypeMismatch),
        ));
        let mut mismatched = record.clone();
        mismatched.size = "0.4".parse().unwrap();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &mismatched,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::QuantityMismatch),
        ));
        let mut mismatched = record.clone();
        mismatched.price = "2499.06".parse().unwrap();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &mismatched,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::LimitPriceMismatch),
        ));
        let mut mismatched = record.clone();
        mismatched.post_only = "MustPostOnly".to_string();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &mismatched,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::PostOnlyMismatch),
        ));
        let mut mismatched = record.clone();
        mismatched.reduce_only = true;
        assert!(matches!(
            client.build_tracked_order_status_report(
                &mismatched,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::ReduceOnlyMismatch),
        ));
    }

    #[rstest]
    fn tracked_order_report_rejects_unproven_or_inconsistent_rest_semantics() {
        let (client, record, venue_order_id) = tracked_limit_report_fixture();

        let mut invalid = record.clone();
        invalid.order_type = "Stop".to_string();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::UnsupportedOrderType(value)) if value == "Stop",
        ));
        let mut invalid = record.clone();
        invalid.post_only = "Adaptive".to_string();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::UnsupportedPostOnly(value)) if value == "Adaptive",
        ));
        let mut invalid = record.clone();
        invalid.size_filled = "0.1".parse().unwrap();
        invalid.size_remain = "0.1".parse().unwrap();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::SizeAccountingMismatch),
        ));
        let mut invalid = record.clone();
        invalid.avg_fill_price = None;
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::MissingAverageFillPrice),
        ));
        let mut invalid = record.clone();
        invalid.size_filled = rust_decimal::Decimal::ZERO;
        invalid.size_remain = invalid.size;
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::UnexpectedAverageFillPrice),
        ));
        let mut invalid = record.clone();
        invalid.status = "Unknown".to_string();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::UnsupportedStatus(value)) if value == "Unknown",
        ));
        let mut invalid = record.clone();
        invalid.status = "Open".to_string();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::StatusQuantityMismatch),
        ));
        let mut invalid = record.clone();
        invalid.status = "Filled".to_string();
        invalid.size_filled = "0.1".parse().unwrap();
        invalid.size_remain = "0.2".parse().unwrap();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::StatusQuantityMismatch),
        ));
        let mut invalid = record.clone();
        invalid.updated_time = None;
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::MissingUpdatedTime),
        ));
        let mut invalid = record.clone();
        invalid.updated_time = Some("invalid".to_string());
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::InvalidTimestamp(
                "updatedTime"
            )),
        ));
        let mut invalid = record.clone();
        invalid.updated_time = Some("2026-09-15T04:04:14.647Z".to_string());
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::TimestampOrderMismatch),
        ));
        let mut invalid = record;
        invalid.status = "PartiallyFilled".to_string();
        invalid.size_filled = "0.05".parse().unwrap();
        invalid.size_remain = "0.25".parse().unwrap();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &invalid,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::FilledQuantityPrecisionLoss),
        ));
    }

    #[rstest]
    fn tracked_order_report_rejects_untracked_external_and_quote_quantity_context() {
        let record = tracked_limit_order_record();
        let venue_order_id = VenueOrderId::from(record.order_id.as_str());
        let client = test_client();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &record,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::UntrackedOrder(id)) if id == venue_order_id,
        ));

        let client = test_client();
        register_external_order(
            &client,
            test_external_order_context("O-DEEPX-EXTERNAL", venue_order_id.as_str()),
        )
        .unwrap();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &record,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::UntrackedOrder(id)) if id == venue_order_id,
        ));

        let client = test_client();
        let order = OrderTestBuilder::new(OrderType::Limit)
            .client_order_id(ClientOrderId::from("O-DEEPX-QUOTE-REPORT"))
            .strategy_id(StrategyId::from("S-DEEPX-001"))
            .instrument_id(InstrumentId::from("ETH-USDC-PERP.DEEPX"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.3"))
            .price(Price::from("2499.05"))
            .time_in_force(TimeInForce::Gtc)
            .quote_quantity(true)
            .build();
        client.register_order(&order).unwrap();
        client
            .bind_tracked_venue_order_id(order.client_order_id(), venue_order_id)
            .unwrap();
        assert!(matches!(
            client.build_tracked_order_status_report(
                &record,
                3,
                venue_order_id,
                UnixNanos::default(),
            ),
            Err(DeepXTrackedOrderReportError::QuoteQuantityUnsupported),
        ));
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
    async fn order_report_generation_rejects_missing_identity() {
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
            "DeepX order status report requires a client order ID or venue order ID",
        );
    }

    #[tokio::test]
    async fn tracked_order_report_generation_uses_validated_catalog_and_rest_record() {
        const SPOT_RESPONSE: &str = include_str!("../test_data/http/testnet/spot_markets.json");
        const PERP_RESPONSE: &str = include_str!("../test_data/http/testnet/perp_markets.json");
        let mut fixture: Value =
            serde_json::from_str(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE).unwrap();
        let mut order = fixture["data"]["items"][2].take();
        order["owner"] = TEST_SUBACCOUNT.into();
        let order_response = json!({
            "code": 200,
            "msg": "success",
            "data": order,
            "fail": false,
        });
        let router = Router::new()
            .route(
                "/internal/v1/market/spot/markets",
                get(|| async { SPOT_RESPONSE }),
            )
            .route(
                "/internal/v1/market/perp/markets",
                get(|| async { PERP_RESPONSE }),
            )
            .route(
                "/internal/v1/account/perp/order-by-id",
                get(move || {
                    let response = order_response.clone();
                    async move { Json(response) }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let base_url = format!("http://{address}");
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
            subaccount_id: Some(TEST_SUBACCOUNT.to_string()),
            private_key: Some(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            ),
            http_timeout_secs: 5,
            network: crate::config::DeepXNetworkConfig {
                base_url_rest: Some(base_url),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut client = DeepXExecutionClient::new(core, config).unwrap();
        let mut provider = DeepXMarketProvider::new(client.http.clone());
        provider.load_all().await.unwrap();
        client.record_instruments_loaded(&provider).unwrap();
        let tracked_order = tracked_limit_order();
        let venue_order_id = VenueOrderId::from("1789445053841");
        client.register_order(&tracked_order).unwrap();
        client
            .bind_tracked_venue_order_id(tracked_order.client_order_id(), venue_order_id)
            .unwrap();
        let ts_init = UnixNanos::from(123_456_789);

        for (client_order_id, requested_venue_order_id) in [
            (Some(tracked_order.client_order_id()), None),
            (None, Some(venue_order_id)),
            (Some(tracked_order.client_order_id()), Some(venue_order_id)),
        ] {
            let command = GenerateOrderStatusReport::new(
                UUID4::new(),
                ts_init,
                Some(tracked_order.instrument_id()),
                client_order_id,
                requested_venue_order_id,
                None,
                None,
            );
            let report = ExecutionClient::generate_order_status_report(&client, &command)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(
                report.client_order_id,
                Some(tracked_order.client_order_id())
            );
            assert_eq!(report.venue_order_id, venue_order_id);
            assert_eq!(report.instrument_id, tracked_order.instrument_id());
            assert_eq!(report.order_status, OrderStatus::Filled);
            assert_eq!(report.ts_init, ts_init);
        }

        client.core.set_connected();
        *client.query_epoch.lock().unwrap() = true;
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);
        ExecutionClient::query_order(
            &client,
            tracked_order_query(&tracked_order, Some(venue_order_id)),
        )
        .unwrap();

        let event = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let ExecutionEvent::Report(nautilus_common::messages::execution::ExecutionReport::Order(
            report,
        )) = event
        else {
            panic!("expected queried order status report")
        };
        assert_eq!(
            report.client_order_id,
            Some(tracked_order.client_order_id())
        );
        assert_eq!(report.venue_order_id, venue_order_id);
        assert_eq!(report.order_status, OrderStatus::Filled);
        assert_eq!(report.ts_init, UnixNanos::from(123_456_789));
        client.reset_startup();
        client
            .query_tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn tracked_order_report_generation_rejects_command_identity_conflicts() {
        let (mut client, _, venue_order_id) = tracked_limit_report_fixture();
        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");
        client.perpetual_market_ids.insert(instrument_id, 3);
        client.perpetual_instrument_ids.insert(3, instrument_id);
        let command = GenerateOrderStatusReport::new(
            UUID4::new(),
            UnixNanos::default(),
            Some(InstrumentId::from("BTC-USDC-PERP.DEEPX")),
            Some(ClientOrderId::from("O-DEEPX-REPORT")),
            Some(venue_order_id),
            None,
            None,
        );

        let error = ExecutionClient::generate_order_status_report(&client, &command)
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "DeepX order status report instrument conflicts with tracked order context",
        );
    }

    #[rstest]
    #[case("disconnected")]
    #[case("inactive")]
    #[case("trader")]
    #[case("client")]
    #[case("strategy")]
    #[case("instrument")]
    #[case("client-order")]
    #[case("venue-order")]
    #[case("params")]
    fn tracked_order_query_rejects_unbound_command_identity(#[case] invalid: &str) {
        let (mut client, _, venue_order_id) = tracked_limit_report_fixture();
        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");
        client.perpetual_market_ids.insert(instrument_id, 3);
        client.perpetual_instrument_ids.insert(3, instrument_id);
        client.core.set_connected();
        *client.query_epoch.lock().unwrap() = true;
        let order = tracked_limit_order();
        let mut command = tracked_order_query(&order, Some(venue_order_id));
        match invalid {
            "disconnected" => client.core.set_disconnected(),
            "inactive" => *client.query_epoch.lock().unwrap() = false,
            "trader" => command.trader_id = TraderId::from("OTHER-001"),
            "client" => command.client_id = Some(ClientId::from("OTHER")),
            "strategy" => command.strategy_id = StrategyId::from("OTHER-001"),
            "instrument" => command.instrument_id = InstrumentId::from("BTC-USDC-PERP.DEEPX"),
            "client-order" => command.client_order_id = ClientOrderId::from("O-OTHER-001"),
            "venue-order" => command.venue_order_id = Some(VenueOrderId::from("999")),
            "params" => {
                let mut params = Params::new();
                params.insert("unsupported".to_string(), serde_json::json!(true));
                command.params = Some(params);
            }
            _ => unreachable!(),
        }

        assert!(client.prepare_tracked_order_query(&command).is_err());
        assert!(client.query_tasks.is_empty());
    }

    #[tokio::test]
    async fn disconnect_retires_order_query_generation_before_reconnect() {
        let mut client = test_client();
        client.core.set_connected();
        *client.query_epoch.lock().unwrap() = true;
        client
            .query_tasks
            .spawn(std::future::pending::<()>())
            .unwrap();

        ExecutionClient::disconnect(&mut client).await.unwrap();

        assert!(!client.core.is_connected());
        assert!(!*client.query_epoch.lock().unwrap());
        assert!(!client.query_tasks.is_open());
        assert!(client.query_tasks.is_empty());

        client.startup.completed_steps = DeepXExecutionStartup::REQUIRED.len();
        client.core.set_connected();
        ExecutionClient::connect(&mut client).await.unwrap();
        assert!(client.query_tasks.is_open());
        assert!(*client.query_epoch.lock().unwrap());
    }

    #[tokio::test]
    async fn position_report_generation_converts_only_current_open_lifecycle() {
        let response = serde_json::from_str(PERP_POSITIONS_ACCOUNT_RESPONSE).unwrap();
        let client = position_report_test_client(response).await;
        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");
        let ts_init = UnixNanos::from(123_456_789);
        let command = GeneratePositionStatusReports::new(
            UUID4::new(),
            ts_init,
            Some(instrument_id),
            None,
            None,
            None,
            None,
        );

        let reports = ExecutionClient::generate_position_status_reports(&client, &command)
            .await
            .unwrap();

        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert_eq!(report.account_id, AccountId::from("DEEPX-001"));
        assert_eq!(report.instrument_id, instrument_id);
        assert_eq!(report.position_side, PositionSide::Long);
        assert_eq!(report.quantity, Quantity::from("0.300"));
        assert_eq!(report.quantity.precision, 4);
        assert_eq!(report.signed_decimal_qty, rust_decimal::Decimal::new(3, 1));
        assert_eq!(
            report.avg_px_open,
            Some(rust_decimal::Decimal::new(24_996, 1))
        );
        assert_eq!(report.ts_last, UnixNanos::from(1_789_445_193_757_000_000),);
        assert_eq!(report.ts_init, ts_init);
        assert_eq!(report.venue_position_id, None);

        let all_markets =
            GeneratePositionStatusReports::new(UUID4::new(), ts_init, None, None, None, None, None);
        let reports = ExecutionClient::generate_position_status_reports(&client, &all_markets)
            .await
            .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].instrument_id, instrument_id);

        let before_update = GeneratePositionStatusReports::new(
            UUID4::new(),
            ts_init,
            Some(instrument_id),
            None,
            Some(UnixNanos::from(1_789_445_193_756_999_999)),
            None,
            None,
        );
        let reports = ExecutionClient::generate_position_status_reports(&client, &before_update)
            .await
            .unwrap();
        assert!(reports.is_empty());
    }

    #[tokio::test]
    async fn position_report_conversion_rejects_unknown_status_and_precision_loss() {
        let response = serde_json::from_str(PERP_POSITIONS_ACCOUNT_RESPONSE).unwrap();
        let client = position_report_test_client(response).await;
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpPositionRecord>> =
            serde_json::from_str(PERP_POSITIONS_ACCOUNT_RESPONSE).unwrap();
        let mut record = response.data.items[0].clone();
        record.owner = TEST_SUBACCOUNT.to_string();
        record.status = "Liquidated".to_string();

        assert_eq!(
            client.build_position_status_report(&record, UnixNanos::default()),
            Err(DeepXPositionReportError::UnsupportedStatus(
                "Liquidated".to_string()
            )),
        );

        record.status = "Open".to_string();
        record.base_asset_amount = rust_decimal::Decimal::new(3_001, 4);
        assert_eq!(
            client.build_position_status_report(&record, UnixNanos::default()),
            Err(DeepXPositionReportError::QuantityIncrementMismatch),
        );
    }

    #[tokio::test]
    async fn position_report_generation_rejects_duplicate_open_market() {
        let mut response: Value = serde_json::from_str(PERP_POSITIONS_ACCOUNT_RESPONSE).unwrap();
        response["data"]["items"][1]["status"] = "Open".into();
        response["data"]["items"][1]["closePrice"] = Value::Null;
        response["data"]["items"][1]["closeBlockNum"] = Value::Null;
        response["data"]["items"][1]["closeTime"] = Value::Null;
        response["data"]["items"][1]["closeEventIdx"] = Value::Null;
        let client = position_report_test_client(response).await;
        let command = GeneratePositionStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            Some(InstrumentId::from("ETH-USDC-PERP.DEEPX")),
            None,
            None,
            None,
            None,
        );

        let error = ExecutionClient::generate_position_status_reports(&client, &command)
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "DeepX position report contains multiple open positions for market ID 3",
        );
    }

    #[tokio::test]
    async fn position_report_generation_rejects_missing_catalog_before_http() {
        let client = test_client();
        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");
        let command = GeneratePositionStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            Some(instrument_id),
            None,
            None,
            None,
            None,
        );

        let error = ExecutionClient::generate_position_status_reports(&client, &command)
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            format!("DeepX position report has no validated perpetual market for {instrument_id}"),
        );
    }

    #[tokio::test]
    async fn position_report_generation_rejects_reversed_time_bounds_before_http() {
        let client = test_client();
        let command = GeneratePositionStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            Some(UnixNanos::from(2)),
            Some(UnixNanos::from(1)),
            None,
            None,
        );

        let error = ExecutionClient::generate_position_status_reports(&client, &command)
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "DeepX position report start must not exceed end",
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
    async fn fill_report_generation_maps_taker_trades_exactly() {
        let response = serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        let client = fill_report_test_client(response).await;
        let ts_init = UnixNanos::from(123);
        let command =
            GenerateFillReports::new(UUID4::new(), ts_init, None, None, None, None, None, None);

        let reports = ExecutionClient::generate_fill_reports(&client, command)
            .await
            .unwrap();

        assert_eq!(reports.len(), 3);
        assert_eq!(reports[0].trade_id, TradeId::from("185969410000051"));
        assert_eq!(
            reports[0].venue_order_id,
            VenueOrderId::from("1789445053841")
        );
        assert_eq!(reports[0].order_side, OrderSide::Buy);
        assert_eq!(reports[0].last_qty, Quantity::from("0.3000"));
        assert_eq!(reports[0].last_px, Price::from("2498.7600"));
        assert_eq!(reports[0].commission, Money::from("0.149837 USDC"));
        assert_eq!(reports[0].liquidity_side, LiquiditySide::Taker);
        assert_eq!(
            reports[0].ts_event,
            UnixNanos::from(1_789_445_055_647_000_000),
        );
        assert_eq!(reports[0].ts_init, ts_init);
        assert_eq!(reports[0].client_order_id, None);
        assert_eq!(reports[1].trade_id, TradeId::from("185971191000008"));
        assert_eq!(reports[1].order_side, OrderSide::Sell);
        assert_eq!(reports[2].trade_id, TradeId::from("185971383000008"));
    }

    #[tokio::test]
    async fn fill_report_generation_maps_maker_rebate_and_tracked_identity() {
        let mut response: Value =
            serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        response["data"]["items"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        response["data"]["items"][0]["isLong"] = false.into();
        response["data"]["items"][0]["fee"] = serde_json::json!(0.074988);
        let client = fill_report_test_client(response).await;
        let order = tracked_limit_order();
        let venue_order_id = VenueOrderId::from("1789445193480");
        client.register_order(&order).unwrap();
        client
            .bind_tracked_venue_order_id(order.client_order_id(), venue_order_id)
            .unwrap();
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

        assert_eq!(
            error.to_string(),
            "DeepX fill report side does not match registered order context",
        );

        let mut response: DeepXApiResponse<DeepXAccountPage<DeepXPerpAccountTradeRecord>> =
            serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        let mut record = response.data.items.remove(0);
        record.fee = rust_decimal::Decimal::new(74_988, 6);
        record.taker = "Seller".to_string();
        let report = client
            .build_fill_report(&record, UnixNanos::default())
            .unwrap();

        assert_eq!(report.client_order_id, Some(order.client_order_id()));
        assert_eq!(report.liquidity_side, LiquiditySide::Maker);
        assert_eq!(report.commission, Money::from("-0.074988 USDC"));
    }

    #[tokio::test]
    async fn fill_report_generation_applies_order_and_exact_time_filters() {
        let response = serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        let client = fill_report_test_client(response).await;
        let command = GenerateFillReports::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            Some(VenueOrderId::from("1789445180000")),
            None,
            None,
            None,
            None,
        );
        let reports = ExecutionClient::generate_fill_reports(&client, command)
            .await
            .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].trade_id, TradeId::from("185971191000008"));

        let mut response: Value =
            serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        response["data"]["items"]
            .as_array_mut()
            .unwrap()
            .truncate(2);
        let client = fill_report_test_client(response).await;
        let command = GenerateFillReports::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
            Some(UnixNanos::from(1_789_445_180_317_000_001)),
            None,
            None,
            None,
        );
        let reports = ExecutionClient::generate_fill_reports(&client, command)
            .await
            .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].trade_id, TradeId::from("185971383000008"));
    }

    #[tokio::test]
    async fn fill_report_conversion_rejects_invalid_venue_semantics() {
        let response = serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        let client = fill_report_test_client(response).await;
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpAccountTradeRecord>> =
            serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        let record = &response.data.items[0];

        let mut invalid = record.clone();
        invalid.fee_asset = "ETH".to_string();
        assert_eq!(
            client.build_fill_report(&invalid, UnixNanos::default()),
            Err(DeepXFillReportError::FeeAssetMismatch),
        );

        invalid = record.clone();
        invalid.fee = -invalid.fee;
        assert_eq!(
            client.build_fill_report(&invalid, UnixNanos::default()),
            Err(DeepXFillReportError::FeeSignMismatch),
        );

        invalid = record.clone();
        invalid.taker = "Neither".to_string();
        assert_eq!(
            client.build_fill_report(&invalid, UnixNanos::default()),
            Err(DeepXFillReportError::UnsupportedTaker(
                "Neither".to_string()
            )),
        );

        invalid = record.clone();
        invalid.filled_direction = "Flat".to_string();
        assert_eq!(
            client.build_fill_report(&invalid, UnixNanos::default()),
            Err(DeepXFillReportError::UnsupportedFilledDirection(
                "Flat".to_string()
            )),
        );

        invalid = record.clone();
        invalid.price = rust_decimal::Decimal::new(24_996_001, 4);
        assert_eq!(
            client.build_fill_report(&invalid, UnixNanos::default()),
            Err(DeepXFillReportError::PriceIncrementMismatch),
        );

        invalid = record.clone();
        invalid.size = rust_decimal::Decimal::new(3_001, 4);
        assert_eq!(
            client.build_fill_report(&invalid, UnixNanos::default()),
            Err(DeepXFillReportError::QuantityIncrementMismatch),
        );
    }

    #[tokio::test]
    async fn fill_report_generation_rejects_invalid_request_before_http() {
        let client = test_client();
        let invalid_order = GenerateFillReports::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            Some(VenueOrderId::from("not-decimal")),
            None,
            None,
            None,
            None,
        );
        let error = ExecutionClient::generate_fill_reports(&client, invalid_order)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "DeepX fill report venue order ID must be an exact decimal u64",
        );

        let reversed_time = GenerateFillReports::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
            Some(UnixNanos::from(2)),
            Some(UnixNanos::from(1)),
            None,
            None,
        );
        let error = ExecutionClient::generate_fill_reports(&client, reversed_time)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "DeepX fill report start must not exceed end",
        );

        let missing_catalog = GenerateFillReports::new(
            UUID4::new(),
            UnixNanos::default(),
            Some(InstrumentId::from("ETH-USDC-PERP.DEEPX")),
            None,
            None,
            None,
            None,
            None,
        );
        let error = ExecutionClient::generate_fill_reports(&client, missing_catalog)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "DeepX fill report has no validated perpetual market for ETH-USDC-PERP.DEEPX",
        );
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

    #[tokio::test]
    async fn account_state_initialization_rejects_wrong_account_type() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        record_account_ownership(&mut client);
        let (connection, subscription, frame) = confirmed_account_stream().await;
        client
            .record_account_stream_confirmed(&connection, subscription)
            .unwrap();
        let mut state = test_account_state();
        state.account_type = AccountType::Cash;

        assert_eq!(
            client.record_account_state_initialized(&connection, &frame, &state),
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

    #[tokio::test]
    async fn account_state_initialization_dispatches_exact_event() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        record_account_ownership(&mut client);
        let (connection, subscription, frame) = confirmed_account_stream().await;
        client
            .record_account_stream_confirmed(&connection, subscription)
            .unwrap();
        let state = test_account_state();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);

        client
            .record_account_state_initialized(&connection, &frame, &state)
            .unwrap();

        let ExecutionEvent::Account(dispatched) = receiver.try_recv().unwrap() else {
            panic!("expected account state event");
        };
        assert_eq!(dispatched, state);
        assert_eq!(client.startup_account_event_id, Some(state.event_id));
        assert_eq!(client.startup.completed_steps, 6);
    }

    #[tokio::test]
    async fn account_state_initialization_dispatch_failure_does_not_advance_startup() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        record_account_ownership(&mut client);
        let (connection, subscription, frame) = confirmed_account_stream().await;
        client
            .record_account_stream_confirmed(&connection, subscription)
            .unwrap();
        let state = test_account_state();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        drop(receiver);
        client.emitter.set_sender(sender);

        let result = client.record_account_state_initialized(&connection, &frame, &state);

        assert!(matches!(
            result,
            Err(DeepXExecutionStartupError::AccountStateDispatchFailed(_)),
        ));
        assert_eq!(client.startup_account_event_id, None);
        assert_eq!(client.startup.completed_steps, 5);
        assert_eq!(
            client
                .startup
                .validate_next(DeepXExecutionStartupEvidence::AccountStateInitialized),
            Ok(()),
        );
    }

    #[tokio::test]
    async fn account_state_initialization_rejects_stale_subscription_without_dispatch() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        record_account_ownership(&mut client);
        let (mut connection, subscription, frame) = confirmed_account_stream().await;
        client
            .record_account_stream_confirmed(&connection, subscription)
            .unwrap();
        connection.close().await.unwrap();
        let state = test_account_state();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);

        assert_eq!(
            client.record_account_state_initialized(&connection, &frame, &state),
            Err(DeepXExecutionStartupError::AccountStreamSubscriptionMismatch),
        );
        assert!(receiver.try_recv().is_err());
        assert_eq!(client.startup_account_event_id, None);
        assert_eq!(client.startup.completed_steps, 5);
    }

    #[tokio::test]
    async fn account_state_initialization_rejects_frame_from_another_connection() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        record_account_ownership(&mut client);
        let (connection, subscription, _) = confirmed_account_stream().await;
        client
            .record_account_stream_confirmed(&connection, subscription)
            .unwrap();
        let (other_connection, _, other_frame) = confirmed_account_stream().await;
        let state = test_account_state();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);

        assert_eq!(
            client.record_account_state_initialized(&other_connection, &other_frame, &state),
            Err(DeepXExecutionStartupError::AccountStreamSubscriptionMismatch),
        );
        assert!(receiver.try_recv().is_err());
        assert_eq!(client.startup_account_event_id, None);
        assert_eq!(client.startup.completed_steps, 5);
    }

    #[tokio::test]
    async fn reset_clears_ownership_and_account_subscription_receipts() {
        let mut client = test_client();
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        record_account_ownership(&mut client);
        let (connection, subscription, frame) = confirmed_account_stream().await;
        client
            .record_account_stream_confirmed(&connection, subscription)
            .unwrap();

        client.reset_startup();
        assert!(client.account_ownership.is_none());
        record_instruments_loaded(&mut client);
        client.restore_order_contexts([]).unwrap();
        client
            .startup
            .record(DeepXExecutionStartupEvidence::RuntimeValidated)
            .unwrap();
        record_account_ownership(&mut client);
        client
            .startup
            .record(DeepXExecutionStartupEvidence::AccountStreamConfirmed)
            .unwrap();
        let state = test_account_state();

        assert_eq!(
            client.record_account_state_initialized(&connection, &frame, &state),
            Err(DeepXExecutionStartupError::AccountStreamSubscriptionMismatch),
        );
        assert_eq!(client.startup_account_event_id, None);
        assert_eq!(client.startup.completed_steps, 5);
    }

    #[tokio::test]
    async fn account_registration_requires_configured_account_in_cache() {
        let mut client = test_client();
        let (state, connection, subscription) =
            advance_through_mass_reconciliation(&mut client).await;

        assert_eq!(
            client.complete_account_registration(&connection, subscription),
            Err(DeepXExecutionStartupError::AccountStateNotRegistered {
                account_id: AccountId::from("DEEPX-001"),
                event_id: state.event_id,
            }),
        );
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn account_registration_connects_after_cache_verification() {
        let (mut client, cache) = test_client_with_cache();
        let (state, connection, subscription) =
            advance_through_mass_reconciliation(&mut client).await;
        register_test_account(&cache, state);

        client
            .complete_account_registration(&connection, subscription)
            .unwrap();

        assert!(client.is_connected());
    }

    #[rstest]
    fn account_query_replays_latest_state_after_current_startup_baseline() {
        let (client, cache, mut receiver, initial) = account_query_client();
        let latest = AccountState::new(
            initial.account_id,
            initial.account_type,
            vec![],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::from(2_000u64),
            UnixNanos::from(3_000u64),
            None,
        );
        cache.borrow_mut().update_account_state(&latest).unwrap();
        let mut command = account_query();
        command.params = Some(Params::new());

        ExecutionClient::query_account(&client, command).unwrap();

        let ExecutionEvent::Account(dispatched) = receiver.try_recv().unwrap() else {
            panic!("expected account state event")
        };
        assert_eq!(dispatched, latest);
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn account_query_does_not_replay_locally_calculated_state() {
        let (client, cache, mut receiver, baseline) = account_query_client();
        let calculated = AccountState::new(
            baseline.account_id,
            baseline.account_type,
            vec![],
            vec![],
            false,
            UUID4::new(),
            UnixNanos::from(2_000u64),
            UnixNanos::from(3_000u64),
            None,
        );
        cache
            .borrow_mut()
            .update_account_state(&calculated)
            .unwrap();

        ExecutionClient::query_account(&client, account_query()).unwrap();

        let ExecutionEvent::Account(dispatched) = receiver.try_recv().unwrap() else {
            panic!("expected account state event")
        };
        assert_eq!(dispatched, baseline);
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    #[case("disconnected")]
    #[case("trader")]
    #[case("client")]
    #[case("account")]
    #[case("params")]
    #[case("stale-startup")]
    fn invalid_account_query_never_replays_cached_state(#[case] invalid: &str) {
        let (mut client, _cache, mut receiver, _) = account_query_client();
        let mut command = account_query();
        match invalid {
            "disconnected" => client.core.set_disconnected(),
            "trader" => command.trader_id = TraderId::from("OTHER-001"),
            "client" => command.client_id = Some(ClientId::from("OTHER")),
            "account" => command.account_id = AccountId::from("DEEPX-002"),
            "params" => {
                let mut params = Params::new();
                params.insert("unsupported".to_string(), serde_json::json!(true));
                command.params = Some(params);
            }
            "stale-startup" => client.startup_account_event_id = Some(UUID4::new()),
            _ => unreachable!(),
        }

        assert!(ExecutionClient::query_account(&client, command).is_err());
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn account_query_reports_cache_borrow_conflict_without_panicking() {
        let (client, cache, mut receiver, _) = account_query_client();
        let borrowed = cache.borrow_mut();

        let error = ExecutionClient::query_account(&client, account_query()).unwrap_err();

        assert!(error.to_string().contains("mutably borrowed"));
        assert!(receiver.try_recv().is_err());
        drop(borrowed);
    }

    #[rstest]
    fn account_query_reports_dispatch_failure() {
        let (client, _cache, receiver, _) = account_query_client();
        drop(receiver);

        let error = ExecutionClient::query_account(&client, account_query()).unwrap_err();

        assert!(error.to_string().contains("Failed to send account state"));
    }

    #[tokio::test]
    async fn account_registration_reports_cache_borrow_conflict() {
        let (mut client, cache) = test_client_with_cache();
        let (state, connection, subscription) =
            advance_through_mass_reconciliation(&mut client).await;
        register_test_account(&cache, state);
        let borrowed = cache.borrow_mut();

        assert_eq!(
            client.complete_account_registration(&connection, subscription),
            Err(DeepXExecutionStartupError::CacheBorrowConflict),
        );
        assert!(!client.is_connected());
        drop(borrowed);
        client
            .complete_account_registration(&connection, subscription)
            .unwrap();
        assert!(client.is_connected());
    }

    #[tokio::test]
    async fn account_registration_checks_startup_order_before_cache() {
        let (mut client, cache) = test_client_with_cache();
        register_test_account(&cache, test_account_state());
        let (connection, subscription, _) = confirmed_account_stream().await;

        assert_eq!(
            client.complete_account_registration(&connection, subscription),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::InstrumentsLoaded,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        );
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn reconnect_requires_startup_replay_before_cached_account_registration() {
        let (mut client, cache) = test_client_with_cache();
        let (state, connection, subscription) =
            advance_through_mass_reconciliation(&mut client).await;
        register_test_account(&cache, state);
        client
            .complete_account_registration(&connection, subscription)
            .unwrap();
        client.reset_startup();

        assert_eq!(
            client.complete_account_registration(&connection, subscription),
            Err(DeepXExecutionStartupError::OutOfOrder {
                expected: DeepXExecutionStartupEvidence::InstrumentsLoaded,
                received: DeepXExecutionStartupEvidence::AccountRegistered,
            }),
        );
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn reconnect_rejects_account_state_from_previous_startup_epoch() {
        let (mut client, cache) = test_client_with_cache();
        let (initial_state, initial_connection, initial_subscription) =
            advance_through_mass_reconciliation(&mut client).await;
        register_test_account(&cache, initial_state);
        client
            .complete_account_registration(&initial_connection, initial_subscription)
            .unwrap();
        client.reset_startup();
        let (current_state, current_connection, current_subscription) =
            advance_through_mass_reconciliation(&mut client).await;

        assert_eq!(
            client.complete_account_registration(&current_connection, current_subscription),
            Err(DeepXExecutionStartupError::AccountStateNotRegistered {
                account_id: AccountId::from("DEEPX-001"),
                event_id: current_state.event_id,
            }),
        );
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn account_registration_rejects_subscription_invalidated_after_account_state() {
        let (mut client, cache) = test_client_with_cache();
        let (state, mut connection, subscription) =
            advance_through_mass_reconciliation(&mut client).await;
        register_test_account(&cache, state);
        connection.close().await.unwrap();

        assert_eq!(
            client.complete_account_registration(&connection, subscription),
            Err(DeepXExecutionStartupError::AccountStreamSubscriptionMismatch),
        );
        assert!(!client.is_connected());
        assert_eq!(client.startup.completed_steps, 7);
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

    #[tokio::test]
    async fn execution_client_dispose_clears_connected_startup_state() {
        let (mut client, cache) = test_client_with_cache();
        let (state, connection, subscription) =
            advance_through_mass_reconciliation(&mut client).await;
        register_test_account(&cache, state);
        client
            .complete_account_registration(&connection, subscription)
            .unwrap();
        assert!(client.is_connected());

        ExecutionClient::dispose(&mut client).unwrap();

        assert!(client.core.is_stopped());
        assert!(!client.is_connected());
        assert_eq!(client.startup.completed_steps, 0);
        assert_eq!(client.startup_account_subscription, None);
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
