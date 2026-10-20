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

//! Read-only DeepX Substrate RPC identity collection.

mod spot_margin;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Formatter},
    time::{Duration, Instant},
};

use alloy::{
    primitives::{Address, Bytes},
    sol,
    sol_types::{SolCall, SolValue},
};
use futures_util::{StreamExt, TryStreamExt, stream};
use nautilus_blockchain::rpc::http::BlockchainHttpRpcClient;
use nautilus_common::live::dst;
use nautilus_core::hex;
use nautilus_model::defi::rpc::RpcNodeHttpResponse;
use parity_scale_codec::{Compact, Decode, Encode};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::json;
pub use spot_margin::{
    DeepXFinalizedSpotMarginError, DeepXFinalizedSpotMarginEvidence,
    observe_finalized_spot_margin_evidence,
};
use subxt_core::ext::scale_value::{Composite, ValueDef, scale::decode_as_type};
use thiserror::Error;

use crate::{
    common::{DeepXEnvironment, DeepXError},
    config::{
        DeepXNetworkConfig, DeepXObservedRpcEndpoint, DeepXRpcEndpointValidationError,
        DeepXRpcRole, DeepXValidatedRpcEndpoints, validate_rpc_endpoint_identities,
    },
    signing::{
        ApprovedRuntimeIdentity, DeepXReadOnlyRuntimeSnapshot, DeepXRuntimeSnapshotService,
        DeepXRuntimeSnapshotServiceError, DeepXRuntimeSnapshotUpdate, RuntimeSnapshot,
        SnapshotError,
    },
};

/// RPC methods required from the transaction submission endpoint.
pub const DEEPX_SUBMISSION_RPC_METHODS: &[&str] =
    &["author_pendingExtrinsics", "author_submitExtrinsic"];

/// RPC methods required from the finalized runtime watch endpoint.
pub const DEEPX_WATCH_RPC_METHODS: &[&str] = &[
    "chain_getBlock",
    "chain_getBlockHash",
    "chain_getFinalizedHead",
    "chain_getHeader",
    "eth_call",
    "state_getStorage",
    "state_queryStorageAt",
    "state_call",
];

const DEEPX_MARGIN_RUNTIME_API_METHOD: &str =
    "PerpRuntimeRpcApi_total_collateral_and_margin_required_for";
const DEEPX_LENDING_POSITIONS_RUNTIME_API_METHOD: &str = "LendingRuntimeRpcApi_batch_position_for";
const DEEPX_LENDING_POOL_RUNTIME_API_METHOD: &str = "LendingRuntimeRpcApi_pool_accounting_v2";
const DEEPX_LENDING_DEPOSIT_RUNTIME_API_METHOD: &str = "LendingRuntimeRpcApi_effective_deposit_v2";
const DEEPX_LENDING_BORROW_RUNTIME_API_METHOD: &str = "LendingRuntimeRpcApi_effective_borrow_v2";
const DEEPX_ETH_CALL_PROBE_ADDRESS: &str = "0x0000000000000000000000000000000000000001";
const DEEPX_MAX_LENDING_ASSET_SYMBOL_BYTES: usize = 32;
const DEEPX_MAX_PERP_MARKET_STORAGE_BYTES: usize = 16_384;
const DEEPX_MAX_SPOT_MARKET_STORAGE_BYTES: usize = 16_384;
const DEEPX_MAX_LENDING_POSITION_ASSETS: usize = 64;
const DEEPX_LENDING_ASSET_QUERY_CONCURRENCY: usize = 8;
const DEEPX_DEFAULT_MAX_PERP_POSITIONS: u32 = 8;
const DEEPX_ABSOLUTE_MAX_PERP_POSITIONS: u32 = 256;
const DEEPX_ABSOLUTE_MAX_PERP_ORDER_MARKETS: u32 = 256;
const DEEPX_ABSOLUTE_MAX_PERP_ORDERS: u32 = 4_096;
const DEEPX_LENDING_PRECOMPILE_ADDRESS: Address = Address::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x50,
]);

sol! {
    function maxWithdrawAmountFor(
        address subaccount,
        uint8 lendingMarket,
        bytes asset
    ) external view returns (uint128);

    struct DeepXPerpOrderSpecAbi {
        uint128 min_order_size;
        uint128 tick_size;
        uint128 step_size;
    }

    struct DeepXPerpMarketAbi {
        uint16 id;
        bytes name;
        bytes base_symbol;
        int32 base_decimal;
        uint16 quote_market_id;
        bytes network;
        uint64 height;
        int128 funding_rate;
        uint64 last_cacl_funding_rate_time;
        uint128 oracle_price;
        uint128 mark_price;
        uint64 max_deviation_bps;
        uint128 maintenance_margin_ratio;
        uint32 taker_fee_rate;
        int32 maker_fee_rate;
        DeepXPerpOrderSpecAbi order_spec;
        uint128 open_interest;
        uint128 long_open_pos_num;
        uint128 short_open_pos_num;
        int128 base_interest_rate;
        uint128 impact_margin_value;
        int128 funding_rate_change_cap;
        int128 funding_rate_change_floor;
        int128 cumulative_funding_index;
    }

    function perpMarkets(uint16 market_id) external view returns (DeepXPerpMarketAbi);
}

const DEEPX_PERP_MARKET_PRECOMPILE_ADDRESS: Address = Address::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x4e,
]);

/// RPC methods required from the transaction recovery endpoint.
pub const DEEPX_RECOVERY_RPC_METHODS: &[&str] = &[
    "chain_getBlock",
    "chain_getBlockHash",
    "chain_getFinalizedHead",
    "chain_getHeader",
    "state_getStorage",
];

#[derive(Debug, Deserialize)]
struct ObservedBlockHeader {
    number: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ObservedRuntimeVersion {
    spec_version: u32,
    transaction_version: u32,
}

#[derive(Debug, Deserialize)]
struct ObservedTimestampBlockResponse {
    block: ObservedTimestampBlock,
}

#[derive(Debug, Deserialize)]
struct ObservedTimestampBlock {
    header: ObservedBlockHeader,
    extrinsics: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ObservedStorageChangeSet {
    block: String,
    changes: Vec<(String, Option<String>)>,
}

#[derive(Debug, Deserialize)]
struct ObservedRpcMethods {
    methods: BTreeSet<String>,
}

/// Evidence that one identity-validated endpoint advertised every requested RPC method.
#[derive(Clone, PartialEq, Eq)]
pub struct DeepXRpcMethodCapabilities {
    role: DeepXRpcRole,
    endpoint_url: String,
    methods: BTreeSet<String>,
}

impl Debug for DeepXRpcMethodCapabilities {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXRpcMethodCapabilities))
            .field("role", &self.role)
            .field("endpoint_url", &"<redacted>")
            .field("methods", &self.methods)
            .finish()
    }
}

impl DeepXRpcMethodCapabilities {
    /// Returns the role whose endpoint supplied this evidence.
    #[must_use]
    pub const fn role(&self) -> DeepXRpcRole {
        self.role
    }

    /// Returns the identity-validated endpoint which supplied this evidence.
    #[must_use]
    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    /// Returns the required methods advertised by the endpoint at observation time.
    #[must_use]
    pub const fn methods(&self) -> &BTreeSet<String> {
        &self.methods
    }
}

/// Complete role-specific RPC method evidence for one validated endpoint set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXValidatedRpcMethodCapabilities {
    submission: DeepXRpcMethodCapabilities,
    watch: DeepXRpcMethodCapabilities,
    recovery: DeepXRpcMethodCapabilities,
}

impl DeepXValidatedRpcMethodCapabilities {
    /// Returns the capabilities observed for `role`.
    #[must_use]
    pub const fn for_role(&self, role: DeepXRpcRole) -> &DeepXRpcMethodCapabilities {
        match role {
            DeepXRpcRole::Submission => &self.submission,
            DeepXRpcRole::Watch => &self.watch,
            DeepXRpcRole::Recovery => &self.recovery,
        }
    }
}

/// Finalized block identity which pins one observed DeepX runtime snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedCheckpoint {
    block_hash: [u8; 32],
    block_number: u32,
}

impl DeepXFinalizedCheckpoint {
    /// Returns the finalized block hash used for all pinned state queries.
    #[must_use]
    pub const fn block_hash(&self) -> [u8; 32] {
        self.block_hash
    }

    /// Returns the decoded finalized block number.
    #[must_use]
    pub const fn block_number(&self) -> u32 {
        self.block_number
    }
}

/// Validated runtime metadata paired with its observed finalized checkpoint.
#[derive(Clone, Debug)]
pub struct DeepXObservedRuntimeSnapshot {
    snapshot: RuntimeSnapshot,
    checkpoint: DeepXFinalizedCheckpoint,
}

/// Validated read-only runtime paired with its observed finalized checkpoint.
#[derive(Clone, Debug)]
pub struct DeepXObservedReadOnlyRuntimeSnapshot {
    snapshot: DeepXReadOnlyRuntimeSnapshot,
    checkpoint: DeepXFinalizedCheckpoint,
}

impl DeepXObservedReadOnlyRuntimeSnapshot {
    /// Returns the read-only runtime snapshot observed at the checkpoint.
    #[must_use]
    pub const fn snapshot(&self) -> &DeepXReadOnlyRuntimeSnapshot {
        &self.snapshot
    }

    /// Returns the finalized checkpoint used to collect the snapshot.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }
}

impl DeepXObservedRuntimeSnapshot {
    /// Returns the immutable runtime snapshot observed at the checkpoint.
    #[must_use]
    pub const fn snapshot(&self) -> &RuntimeSnapshot {
        &self.snapshot
    }

    /// Returns the finalized checkpoint used to collect the snapshot.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Consumes the observation and returns its runtime snapshot.
    #[must_use]
    pub fn into_snapshot(self) -> RuntimeSnapshot {
        self.snapshot
    }
}

/// Result of one approved finalized runtime observation and snapshot application.
#[derive(Clone, Debug)]
pub struct DeepXAppliedRuntimeSnapshot {
    checkpoint: DeepXFinalizedCheckpoint,
    update: DeepXRuntimeSnapshotUpdate,
    snapshot: RuntimeSnapshot,
}

impl PartialEq for DeepXAppliedRuntimeSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.checkpoint == other.checkpoint
            && self.update == other.update
            && self.identity() == other.identity()
    }
}

impl Eq for DeepXAppliedRuntimeSnapshot {}

impl DeepXAppliedRuntimeSnapshot {
    /// Returns the finalized checkpoint used for the approved observation.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns whether the validated snapshot was unchanged or installed.
    #[must_use]
    pub const fn update(&self) -> DeepXRuntimeSnapshotUpdate {
        self.update
    }

    /// Returns the approved runtime identity observed and applied at the finalized checkpoint.
    #[must_use]
    pub const fn identity(&self) -> &ApprovedRuntimeIdentity {
        self.snapshot.identity()
    }

    /// Returns the approved immutable runtime snapshot applied at the finalized checkpoint.
    #[must_use]
    pub const fn snapshot(&self) -> &RuntimeSnapshot {
        &self.snapshot
    }
}

/// Finalized runtime and on-chain timestamp observed from one exact Watch checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedChainTimeEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    timestamp_ms: u64,
}

/// Current-chain timestamp and runtime with a separate finalized recovery boundary.
#[derive(Clone, Debug)]
pub(crate) struct DeepXCurrentSubmissionClockEvidence {
    scan_checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    timestamp_ms: u64,
}

impl DeepXCurrentSubmissionClockEvidence {
    pub(crate) const fn scan_checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.scan_checkpoint
    }

    pub(crate) const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    pub(crate) const fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }
}

fn advance_submission_timestamp(timestamp_ms: u64, elapsed: Duration) -> anyhow::Result<u64> {
    let elapsed_ms = u64::try_from(elapsed.as_millis())
        .map_err(|_| anyhow::anyhow!("DeepX current submission clock observation overflowed"))?;
    timestamp_ms
        .checked_add(elapsed_ms)
        .ok_or_else(|| anyhow::anyhow!("DeepX current submission clock overflowed"))
}

impl DeepXFinalizedChainTimeEvidence {
    /// Returns the finalized checkpoint used for runtime and timestamp reads.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the fixture-approved runtime identity at the finalized checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns `Timestamp.Now` decoded as Unix milliseconds.
    #[must_use]
    pub const fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }
}

/// Raw Spot market constraints observed at one finalized runtime checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedSpotMarketSpecEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    pair: [u8; 32],
    min_order_size: u128,
    tick_size: u128,
    step_size: u128,
}

impl DeepXFinalizedSpotMarketSpecEvidence {
    /// Returns the finalized checkpoint used for the storage read.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the fixture-approved runtime identity observed at the checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the deployment-provided bytes32 pair identifier.
    #[must_use]
    pub const fn pair(&self) -> [u8; 32] {
        self.pair
    }

    /// Returns the raw runtime minimum order size.
    #[must_use]
    pub const fn min_order_size(&self) -> u128 {
        self.min_order_size
    }

    /// Returns the raw runtime price tick size.
    #[must_use]
    pub const fn tick_size(&self) -> u128 {
        self.tick_size
    }

    /// Returns the raw runtime quantity step size.
    #[must_use]
    pub const fn step_size(&self) -> u128 {
        self.step_size
    }
}

/// Spot order constraints read from one current, hash-pinned runtime state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeepXCurrentSpotMarketSpec {
    pair: [u8; 32],
    min_order_size: u128,
    tick_size: u128,
    step_size: u128,
}

impl DeepXCurrentSpotMarketSpec {
    pub(crate) const fn pair(&self) -> [u8; 32] {
        self.pair
    }

    pub(crate) const fn min_order_size(&self) -> u128 {
        self.min_order_size
    }

    pub(crate) const fn tick_size(&self) -> u128 {
        self.tick_size
    }

    pub(crate) const fn step_size(&self) -> u128 {
        self.step_size
    }
}

/// Raw Perp order constraints observed at one finalized runtime checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedPerpOrderSpecEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    market_id: u16,
    min_order_size: u128,
    tick_size: u128,
    step_size: u128,
    min_notional: u128,
}

impl DeepXFinalizedPerpOrderSpecEvidence {
    /// Returns the finalized checkpoint used for the precompile read.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the fixture-approved runtime identity at the checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the requested and returned Perp market identifier.
    #[must_use]
    pub const fn market_id(&self) -> u16 {
        self.market_id
    }

    /// Returns the raw runtime minimum order size.
    #[must_use]
    pub const fn min_order_size(&self) -> u128 {
        self.min_order_size
    }

    /// Returns the raw runtime price tick size.
    #[must_use]
    pub const fn tick_size(&self) -> u128 {
        self.tick_size
    }

    /// Returns the raw runtime quantity step size.
    #[must_use]
    pub const fn step_size(&self) -> u128 {
        self.step_size
    }

    /// Returns the raw runtime minimum notional, scaled by 1e6.
    #[must_use]
    pub const fn min_notional(&self) -> u128 {
        self.min_notional
    }
}

/// Margin weighting used by the DeepX runtime calculation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXMarginDirection {
    /// Initial collateral weights and order/position requirements.
    Initial,
    /// Maintenance collateral weights, requirements, unrealized PnL, and unsettled funding.
    Maintenance,
}

impl DeepXMarginDirection {
    const fn as_runtime_u8(self) -> u8 {
        match self {
            Self::Initial => 0,
            Self::Maintenance => 1,
        }
    }
}

/// One direction-specific aggregate returned by the DeepX perpetual runtime API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXMarginRequirement {
    total_collateral: u128,
    total_margin_required: u128,
}

/// Initial and maintenance margin observed at one current canonical Watch block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXCurrentMarginEvidence {
    block_hash: [u8; 32],
    block_number: u32,
    subaccount: [u8; 20],
    initial: DeepXMarginRequirement,
    maintenance: DeepXMarginRequirement,
}

impl DeepXCurrentMarginEvidence {
    /// Returns the canonical block hash used for the margin calls.
    #[must_use]
    pub const fn block_hash(&self) -> [u8; 32] {
        self.block_hash
    }

    /// Returns the current block number used for the margin calls.
    #[must_use]
    pub const fn block_number(&self) -> u32 {
        self.block_number
    }

    /// Returns the observed subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns initial margin and weighted collateral in runtime quote units.
    #[must_use]
    pub const fn initial(&self) -> DeepXMarginRequirement {
        self.initial
    }

    /// Returns maintenance margin and weighted collateral in runtime quote units.
    #[must_use]
    pub const fn maintenance(&self) -> DeepXMarginRequirement {
        self.maintenance
    }
}

impl DeepXMarginRequirement {
    /// Returns aggregate weighted collateral in the runtime quote precision.
    #[must_use]
    pub const fn total_collateral(&self) -> u128 {
        self.total_collateral
    }

    /// Returns aggregate required margin in the runtime quote precision.
    #[must_use]
    pub const fn total_margin_required(&self) -> u128 {
        self.total_margin_required
    }
}

/// Initial and maintenance margin evidence pinned to one finalized runtime checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedMarginEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
    initial: DeepXMarginRequirement,
    maintenance: DeepXMarginRequirement,
}

impl DeepXFinalizedMarginEvidence {
    /// Returns the finalized checkpoint used for both runtime API calls.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the fixture-approved runtime identity at the finalized checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the exact queried subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns initial-margin collateral and requirement aggregates.
    #[must_use]
    pub const fn initial(&self) -> DeepXMarginRequirement {
        self.initial
    }

    /// Returns maintenance-margin collateral and requirement aggregates.
    #[must_use]
    pub const fn maintenance(&self) -> DeepXMarginRequirement {
        self.maintenance
    }
}

/// Runtime status of one DeepX V2 lending pool.
#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXLendingPoolStatus {
    /// New lending activity is paused.
    Paused,
    /// The pool is undergoing a storage migration and is not readable as V2 state.
    Migrating,
    /// The pool is active.
    Active,
    /// The pool is frozen.
    Frozen,
}

/// Raw accounting evidence for one DeepX V2 lending pool.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub struct DeepXLendingPoolEvidence {
    market_id: u8,
    asset: Vec<u8>,
    decimal: u32,
    cash_balance: u128,
    locked_liquidity: u128,
    available_liquidity: u128,
    total_scaled_deposits: u128,
    total_scaled_borrows: u128,
    total_effective_deposits: u128,
    total_effective_borrows: u128,
    deposit_index: u128,
    borrow_index: u128,
    protocol_reserve: u128,
    bad_debt: u128,
    accounting_dust: i128,
    internal_transfer_dust: i128,
    status: DeepXLendingPoolStatus,
    apr_borrow: u128,
    apr_lend: u128,
}

impl DeepXLendingPoolEvidence {
    /// Returns the lending market ID.
    #[must_use]
    pub const fn market_id(&self) -> u8 {
        self.market_id
    }

    /// Returns the exact runtime asset symbol bytes.
    #[must_use]
    pub fn asset(&self) -> &[u8] {
        &self.asset
    }

    /// Returns the token decimal precision reported by the pool.
    #[must_use]
    pub const fn decimal(&self) -> u32 {
        self.decimal
    }

    /// Returns the raw V2 cumulative deposit index.
    #[must_use]
    pub const fn deposit_index(&self) -> u128 {
        self.deposit_index
    }

    /// Returns the raw V2 cumulative borrow index.
    #[must_use]
    pub const fn borrow_index(&self) -> u128 {
        self.borrow_index
    }

    /// Returns the pool-level locked liquidity.
    ///
    /// This value is not a user balance and must not be mapped to Nautilus locked funds.
    #[must_use]
    pub const fn locked_liquidity(&self) -> u128 {
        self.locked_liquidity
    }

    /// Returns the runtime pool status.
    #[must_use]
    pub const fn status(&self) -> DeepXLendingPoolStatus {
        self.status
    }
}

/// Finalized raw lending position and effective balances for one market asset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXLendingPositionEvidence {
    market_id: u8,
    asset: Vec<u8>,
    scaled_deposit: u128,
    scaled_borrow: u128,
    effective_deposit: Option<u128>,
    effective_borrow: Option<u128>,
    max_withdraw_amount: Option<u128>,
    pool: DeepXLendingPoolEvidence,
}

impl DeepXLendingPositionEvidence {
    /// Returns the lending market ID.
    #[must_use]
    pub const fn market_id(&self) -> u8 {
        self.market_id
    }

    /// Returns the exact runtime asset symbol bytes.
    #[must_use]
    pub fn asset(&self) -> &[u8] {
        &self.asset
    }

    /// Returns the stored V2 scaled deposit shares.
    #[must_use]
    pub const fn scaled_deposit(&self) -> u128 {
        self.scaled_deposit
    }

    /// Returns the stored V2 scaled borrow shares.
    #[must_use]
    pub const fn scaled_borrow(&self) -> u128 {
        self.scaled_borrow
    }

    /// Returns the effective deposit amount calculated by the runtime at the checkpoint.
    #[must_use]
    pub const fn effective_deposit(&self) -> Option<u128> {
        self.effective_deposit
    }

    /// Returns the effective borrow amount calculated by the runtime at the checkpoint.
    #[must_use]
    pub const fn effective_borrow(&self) -> Option<u128> {
        self.effective_borrow
    }

    /// Returns the precompile-reported maximum currently withdrawable token amount.
    ///
    /// This value is capped by account-wide free collateral, the effective deposit, and pool
    /// liquidity. It is not a Nautilus free balance or evidence of pending-order locked funds.
    #[must_use]
    pub const fn max_withdraw_amount(&self) -> Option<u128> {
        self.max_withdraw_amount
    }

    /// Returns matching pool-level accounting evidence from the same checkpoint.
    #[must_use]
    pub const fn pool(&self) -> &DeepXLendingPoolEvidence {
        &self.pool
    }
}

/// Complete raw lending evidence for one account at one finalized runtime checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedLendingEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
    positions: Vec<DeepXLendingPositionEvidence>,
}

/// One raw perpetual position decoded directly from finalized runtime storage.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub struct DeepXPerpPositionStorageEvidence {
    market_id: u16,
    is_long: bool,
    base_asset_amount: u128,
    entry_price: u128,
    leverage: u64,
    last_funding_rate: i128,
    version: u64,
    realized_pnl: i128,
    funding_payment: i128,
    owner: [u8; 20],
    take_profit: Option<u128>,
    stop_loss: Option<u128>,
    last_settle_price: u128,
}

impl DeepXPerpPositionStorageEvidence {
    /// Returns the deployment-provided perpetual market ID.
    #[must_use]
    pub const fn market_id(&self) -> u16 {
        self.market_id
    }

    /// Returns whether the position is long.
    #[must_use]
    pub const fn is_long(&self) -> bool {
        self.is_long
    }

    /// Returns the raw base-asset position amount.
    #[must_use]
    pub const fn base_asset_amount(&self) -> u128 {
        self.base_asset_amount
    }

    /// Returns the raw entry price.
    #[must_use]
    pub const fn entry_price(&self) -> u128 {
        self.entry_price
    }

    /// Returns effective leverage in runtime leverage precision.
    #[must_use]
    pub const fn leverage(&self) -> u64 {
        self.leverage
    }

    /// Returns the cumulative-funding-index snapshot at the last settlement.
    #[must_use]
    pub const fn last_funding_rate(&self) -> i128 {
        self.last_funding_rate
    }

    /// Returns the runtime position version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns raw realized PnL.
    #[must_use]
    pub const fn realized_pnl(&self) -> i128 {
        self.realized_pnl
    }

    /// Returns raw settled funding payment.
    #[must_use]
    pub const fn funding_payment(&self) -> i128 {
        self.funding_payment
    }

    /// Returns the exact owner encoded inside the position.
    #[must_use]
    pub const fn owner(&self) -> [u8; 20] {
        self.owner
    }

    /// Returns the optional raw take-profit point.
    #[must_use]
    pub const fn take_profit(&self) -> Option<u128> {
        self.take_profit
    }

    /// Returns the optional raw stop-loss point.
    #[must_use]
    pub const fn stop_loss(&self) -> Option<u128> {
        self.stop_loss
    }

    /// Returns the raw last settlement price.
    #[must_use]
    pub const fn last_settle_price(&self) -> u128 {
        self.last_settle_price
    }
}

/// Perpetual positions read from one account's finalized runtime storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedPerpPositionsEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
    runtime_max_positions: u32,
    positions: Vec<DeepXPerpPositionStorageEvidence>,
}

/// Time-in-force encoded in finalized perpetual-order storage.
#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXStoredTimeInForce {
    /// Good until canceled.
    Gtc,
    /// Immediate or cancel.
    Ioc,
    /// Fill or kill.
    Fok,
}

/// Order type encoded in finalized perpetual-order storage.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXStoredPerpOrderType {
    /// Limit order with its exact runtime time-in-force value.
    Limit(DeepXStoredTimeInForce),
    /// Market order with an optional raw slippage limit.
    Market(Option<u64>),
    /// System stop order.
    Stop,
}

/// Lifecycle state encoded in finalized perpetual-order storage.
#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXStoredOrderStatus {
    /// Active on the order book.
    Open,
    /// Partially filled with a remainder still active.
    PartiallyFilled,
    /// Fully filled.
    Filled,
    /// Canceled by a user or the protocol.
    Canceled,
    /// Reverted or failed on-chain.
    Rejected,
    /// Canceled according to order-type rules or by the exchange.
    Expired,
}

impl DeepXStoredOrderStatus {
    const fn is_active(self) -> bool {
        matches!(self, Self::Open | Self::PartiallyFilled)
    }
}

/// Post-only behavior encoded in finalized perpetual-order storage.
#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXStoredPostOnlyParam {
    /// No post-only constraint.
    None,
    /// The order must rest without immediately matching.
    MustPostOnly,
    /// The runtime may adapt the price to post the order.
    Adaptive,
}

#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
enum DeepXStoredOrderSide {
    #[codec(index = 1)]
    Buy,
    #[codec(index = 0)]
    Sell,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
struct DeepXActivePerpOrderIndexEntry {
    address: [u8; 20],
    market_id: u16,
    order_side: DeepXStoredOrderSide,
    order_type: DeepXStoredPerpOrderType,
    order_id: u64,
    price: u128,
    created_at: u64,
}

/// One complete active perpetual order decoded from finalized runtime storage.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub struct DeepXPerpOrderStorageEvidence {
    order_id: u64,
    owner: [u8; 20],
    market_id: u16,
    is_long: bool,
    size: u128,
    price: u128,
    order_type: DeepXStoredPerpOrderType,
    create_time: u64,
    leverage: u64,
    status: DeepXStoredOrderStatus,
    size_filled: u128,
    size_remain: u128,
    take_profit: Option<u128>,
    stop_loss: Option<u128>,
    reduce_only: bool,
    post_only: DeepXStoredPostOnlyParam,
}

impl DeepXPerpOrderStorageEvidence {
    /// Returns the runtime order ID.
    #[must_use]
    pub const fn order_id(&self) -> u64 {
        self.order_id
    }

    /// Returns the exact owner encoded in `OrderInfo`.
    #[must_use]
    pub const fn owner(&self) -> [u8; 20] {
        self.owner
    }

    /// Returns the runtime perpetual market ID.
    #[must_use]
    pub const fn market_id(&self) -> u16 {
        self.market_id
    }

    /// Returns whether the order increases or reduces the long side.
    #[must_use]
    pub const fn is_long(&self) -> bool {
        self.is_long
    }

    /// Returns the raw total order size.
    #[must_use]
    pub const fn size(&self) -> u128 {
        self.size
    }

    /// Returns the raw order price.
    #[must_use]
    pub const fn price(&self) -> u128 {
        self.price
    }

    /// Returns the exact stored order type.
    #[must_use]
    pub const fn order_type(&self) -> &DeepXStoredPerpOrderType {
        &self.order_type
    }

    /// Returns the raw runtime creation timestamp.
    #[must_use]
    pub const fn create_time(&self) -> u64 {
        self.create_time
    }

    /// Returns the raw effective leverage cap.
    #[must_use]
    pub const fn leverage(&self) -> u64 {
        self.leverage
    }

    /// Returns the active stored lifecycle state.
    #[must_use]
    pub const fn status(&self) -> DeepXStoredOrderStatus {
        self.status
    }

    /// Returns the raw filled size.
    #[must_use]
    pub const fn size_filled(&self) -> u128 {
        self.size_filled
    }

    /// Returns the raw remaining size.
    #[must_use]
    pub const fn size_remain(&self) -> u128 {
        self.size_remain
    }

    /// Returns the optional raw take-profit point.
    #[must_use]
    pub const fn take_profit(&self) -> Option<u128> {
        self.take_profit
    }

    /// Returns the optional raw stop-loss point.
    #[must_use]
    pub const fn stop_loss(&self) -> Option<u128> {
        self.stop_loss
    }

    /// Returns whether the order is reduce-only.
    #[must_use]
    pub const fn reduce_only(&self) -> bool {
        self.reduce_only
    }

    /// Returns the exact stored post-only behavior.
    #[must_use]
    pub const fn post_only(&self) -> DeepXStoredPostOnlyParam {
        self.post_only
    }
}

/// One perpetual order corroborated against finalized `OrderInfo` and active-index storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedPerpOrderStorageEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
    order: DeepXPerpOrderStorageEvidence,
}

impl DeepXFinalizedPerpOrderStorageEvidence {
    /// Returns the finalized checkpoint used for both order storage reads.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the fixture-approved runtime identity at the finalized checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the exact queried subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns the active or terminal order stored for this account and order ID.
    #[must_use]
    pub const fn order(&self) -> &DeepXPerpOrderStorageEvidence {
        &self.order
    }
}

/// A bounded set of active or terminal perpetual orders from one finalized checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedPerpOrderStorageBatchEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
    orders: Vec<DeepXPerpOrderStorageEvidence>,
}

/// One complete Spot order decoded from finalized runtime storage.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub struct DeepXSpotOrderStorageEvidence {
    order_id: u64,
    owner: [u8; 20],
    pair: [u8; 32],
    price: Option<[u8; 32]>,
    quote_amount: [u8; 32],
    base_amount: [u8; 32],
    create_time: u64,
    status: DeepXStoredOrderStatus,
    order_type: DeepXStoredSpotOrderType,
    post_only: DeepXStoredPostOnlyParam,
    reduce_only: bool,
    is_buy: bool,
}

impl DeepXSpotOrderStorageEvidence {
    /// Returns the runtime order ID.
    #[must_use]
    pub const fn order_id(&self) -> u64 {
        self.order_id
    }

    /// Returns the exact owner encoded in `OrderInfo`.
    #[must_use]
    pub const fn owner(&self) -> [u8; 20] {
        self.owner
    }

    /// Returns the deployment-provided Spot pair identity.
    #[must_use]
    pub const fn pair(&self) -> [u8; 32] {
        self.pair
    }

    /// Returns the order creation block number retained by the runtime.
    #[must_use]
    pub const fn create_time(&self) -> u64 {
        self.create_time
    }

    /// Returns the raw optional runtime price as little-endian U256 bytes.
    #[must_use]
    pub const fn price(&self) -> Option<[u8; 32]> {
        self.price
    }

    /// Returns the raw quote amount as little-endian U256 bytes.
    #[must_use]
    pub const fn quote_amount(&self) -> [u8; 32] {
        self.quote_amount
    }

    /// Returns the raw base amount as little-endian U256 bytes.
    #[must_use]
    pub const fn base_amount(&self) -> [u8; 32] {
        self.base_amount
    }

    /// Returns the runtime order status.
    #[must_use]
    pub const fn status(&self) -> DeepXStoredOrderStatus {
        self.status
    }

    /// Returns the runtime Spot order type.
    #[must_use]
    pub const fn order_type(&self) -> DeepXStoredSpotOrderType {
        self.order_type
    }

    /// Returns the runtime post-only parameter.
    #[must_use]
    pub const fn post_only(&self) -> DeepXStoredPostOnlyParam {
        self.post_only
    }

    /// Returns the runtime reduce-only flag.
    #[must_use]
    pub const fn reduce_only(&self) -> bool {
        self.reduce_only
    }

    /// Returns whether the order is a buy.
    #[must_use]
    pub const fn is_buy(&self) -> bool {
        self.is_buy
    }
}

/// Spot order type stored by the runtime.
#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXStoredSpotOrderType {
    Limit(DeepXStoredTimeInForce),
    Market(Option<u64>),
    Stop,
}

/// Batch Spot order evidence observed at one finalized checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedSpotOrderStorageBatchEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
    orders: Vec<DeepXSpotOrderStorageEvidence>,
    active_order_ids: Vec<u64>,
    active_order_pairs: Vec<(u64, [u8; 32])>,
}

impl DeepXFinalizedSpotOrderStorageBatchEvidence {
    /// Returns the finalized checkpoint used for storage reads.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the approved runtime identity at the checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the queried subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns the decoded order evidence.
    #[must_use]
    pub fn orders(&self) -> &[DeepXSpotOrderStorageEvidence] {
        &self.orders
    }

    /// Returns every active Spot order ID in the finalized account index.
    #[must_use]
    pub fn active_order_ids(&self) -> &[u64] {
        &self.active_order_ids
    }

    /// Returns each active Spot order ID with its runtime pair identity.
    #[must_use]
    pub fn active_order_pairs(&self) -> &[(u64, [u8; 32])] {
        &self.active_order_pairs
    }
}

impl DeepXFinalizedPerpOrderStorageBatchEvidence {
    /// Returns the finalized checkpoint used for every storage read.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the fixture-approved runtime identity at the finalized checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the exact queried subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns the requested orders in canonical order-ID order.
    #[must_use]
    pub fn orders(&self) -> &[DeepXPerpOrderStorageEvidence] {
        &self.orders
    }
}

/// Active perpetual orders corroborated across the finalized account index and `OrderInfo` map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedPerpOrdersEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
    orders: Vec<DeepXPerpOrderStorageEvidence>,
}

impl DeepXFinalizedPerpOrdersEvidence {
    /// Returns the finalized checkpoint used for every storage read.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the fixture-approved runtime identity at the finalized checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the exact queried subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns canonical market-then-order-ID ordered active orders.
    #[must_use]
    pub fn orders(&self) -> &[DeepXPerpOrderStorageEvidence] {
        &self.orders
    }
}

impl DeepXFinalizedPerpPositionsEvidence {
    /// Returns the finalized checkpoint used for both storage reads.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the fixture-approved runtime identity at the finalized checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the exact queried subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns the runtime-configured concurrent position limit at the checkpoint.
    #[must_use]
    pub const fn runtime_max_positions(&self) -> u32 {
        self.runtime_max_positions
    }

    /// Returns canonical market-ordered positions owned by the queried subaccount.
    #[must_use]
    pub fn positions(&self) -> &[DeepXPerpPositionStorageEvidence] {
        &self.positions
    }
}

impl DeepXFinalizedLendingEvidence {
    /// Returns the finalized checkpoint used for every lending runtime API call.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the fixture-approved runtime identity at the finalized checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the exact queried account.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns all non-zero lending positions across the complete `u8` market domain.
    #[must_use]
    pub fn positions(&self) -> &[DeepXLendingPositionEvidence] {
        &self.positions
    }
}

/// Complete raw account evidence pinned to one finalized runtime checkpoint.
///
/// This groups the independently validated chain time, margin, lending, perpetual-position, and
/// perpetual and Spot active-order evidence without assigning framework lifecycle semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedAccountEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
    timestamp_ms: u64,
    margin: DeepXFinalizedMarginEvidence,
    lending: DeepXFinalizedLendingEvidence,
    perp_positions: DeepXFinalizedPerpPositionsEvidence,
    perp_orders: DeepXFinalizedPerpOrdersEvidence,
    spot_orders: DeepXFinalizedSpotOrderStorageBatchEvidence,
}

impl DeepXFinalizedAccountEvidence {
    /// Returns the finalized checkpoint shared by every contained observation.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the read-only fixture-approved runtime identity at the checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the exact queried subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns `Timestamp.Now` decoded as Unix milliseconds at the shared checkpoint.
    #[must_use]
    pub const fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    /// Returns both direction-specific aggregate margin observations.
    #[must_use]
    pub const fn margin(&self) -> &DeepXFinalizedMarginEvidence {
        &self.margin
    }

    /// Returns complete raw V2 lending observations.
    #[must_use]
    pub const fn lending(&self) -> &DeepXFinalizedLendingEvidence {
        &self.lending
    }

    /// Returns finalized perpetual-position storage observations.
    #[must_use]
    pub const fn perp_positions(&self) -> &DeepXFinalizedPerpPositionsEvidence {
        &self.perp_positions
    }

    /// Returns finalized active perpetual-order storage observations.
    #[must_use]
    pub const fn perp_orders(&self) -> &DeepXFinalizedPerpOrdersEvidence {
        &self.perp_orders
    }

    /// Returns finalized Spot order-index observations.
    #[must_use]
    pub const fn spot_orders(&self) -> &DeepXFinalizedSpotOrderStorageBatchEvidence {
        &self.spot_orders
    }
}

/// Errors raised while collecting DeepX RPC endpoint identity.
#[derive(Debug, Error)]
pub enum DeepXRpcIdentityError {
    /// The network configuration is unsupported.
    #[error(transparent)]
    Configuration(#[from] DeepXError),
    /// The endpoint request or JSON-RPC response failed.
    #[error("failed to query DeepX RPC endpoint identity: {0}")]
    Rpc(#[source] anyhow::Error),
    /// The endpoint returned a genesis hash other than one prefixed 32-byte value.
    #[error("DeepX RPC endpoint returned an invalid genesis hash")]
    InvalidGenesisHash,
}

/// Errors raised while probing role-specific RPC method availability.
#[derive(Debug, Error)]
pub enum DeepXRpcMethodCapabilityError {
    /// No methods were supplied for the role probe.
    #[error("no required DeepX RPC methods supplied for role {0:?}")]
    EmptyRequirements(DeepXRpcRole),
    /// The endpoint request or JSON-RPC response failed.
    #[error("failed to query DeepX RPC methods for role {role:?}: {source}")]
    Rpc {
        /// Role whose identity-validated endpoint was queried.
        role: DeepXRpcRole,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The endpoint did not advertise one required method.
    #[error("DeepX RPC endpoint for role {role:?} does not advertise required method {method}")]
    MissingMethod {
        /// Role whose endpoint lacks the method.
        role: DeepXRpcRole,
        /// Required method absent from the advertised method set.
        method: String,
    },
}

/// Errors raised while collecting complete role-specific RPC method capabilities.
#[derive(Debug, Error)]
#[error("failed to observe DeepX RPC method capabilities for role {role:?}: {source}")]
pub struct DeepXRpcMethodCapabilitiesError {
    /// Role whose identity-validated endpoint was queried.
    pub role: DeepXRpcRole,
    /// Underlying capability observation error.
    #[source]
    pub source: DeepXRpcMethodCapabilityError,
}

/// Errors raised while collecting and validating every DeepX RPC role endpoint.
#[derive(Debug, Error)]
pub enum DeepXRpcEndpointIdentityError {
    /// Identity collection failed for one configured role endpoint.
    #[error("failed to observe DeepX RPC endpoint identity for role {role:?}: {source}")]
    Observation {
        /// Role whose configured endpoint could not be observed.
        role: DeepXRpcRole,
        /// Underlying collection error.
        #[source]
        source: DeepXRpcIdentityError,
    },
    /// Complete observations failed endpoint identity validation.
    #[error(transparent)]
    Validation(#[from] DeepXRpcEndpointValidationError),
}

/// Errors raised while collecting a DeepX runtime snapshot.
#[derive(Debug, Error)]
pub enum DeepXRuntimeSnapshotObservationError {
    /// The network configuration is unsupported.
    #[error(transparent)]
    Configuration(#[from] DeepXError),
    /// An endpoint request or JSON-RPC response failed.
    #[error("failed to query DeepX runtime snapshot method {method}: {source}")]
    Rpc {
        /// RPC method which failed.
        method: &'static str,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The endpoint returned a hash other than one prefixed 32-byte value.
    #[error("DeepX runtime snapshot returned an invalid {0}")]
    InvalidHash(&'static str),
    /// The block header returned a malformed or out-of-range block number.
    #[error("DeepX runtime snapshot returned an invalid block number: {0}")]
    InvalidBlockNumber(String),
    /// The current block ceased to be canonical during metadata observation.
    #[error("DeepX runtime snapshot block is no longer canonical")]
    InvalidCheckpoint,
    /// The endpoint did not reject a runtime query at an invalid block hash.
    #[error("DeepX Watch RPC ignored an invalid runtime block hash")]
    BlockHashIgnored,
    /// The endpoint returned metadata other than one prefixed hexadecimal value.
    #[error("DeepX runtime snapshot returned invalid metadata")]
    InvalidMetadata,
    /// The observed runtime snapshot is not approved for use.
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    /// The endpoint does not honor the captured historical runtime identity.
    #[error(transparent)]
    HistoricalRuntime(#[from] DeepXHistoricalRuntimeError),
}

/// Errors raised by one approved runtime snapshot observation and application.
#[derive(Debug, Error)]
pub enum DeepXRuntimeSnapshotRefreshError {
    /// The finalized runtime observation failed, possibly after a runtime change was latched.
    #[error(transparent)]
    Observation(#[from] DeepXRuntimeSnapshotObservationError),
    /// Runtime change evidence or a validated snapshot conflicted with the snapshot service.
    #[error(transparent)]
    Application(#[from] DeepXRuntimeSnapshotServiceError),
}

/// Errors raised while observing finalized runtime-bound chain time.
#[derive(Debug, Error)]
pub enum DeepXFinalizedChainTimeError {
    /// Watch capability evidence does not belong to the validated endpoint or lacks storage reads.
    #[error("DeepX Watch RPC capabilities do not authorize finalized chain-time storage reads")]
    RpcCapabilitiesMismatch,
    /// Finalized runtime observation or application failed.
    #[error(transparent)]
    Runtime(#[from] DeepXRuntimeSnapshotRefreshError),
    /// The Watch endpoint did not preserve the captured historical runtime version.
    #[error(transparent)]
    HistoricalRuntime(#[from] DeepXHistoricalRuntimeError),
    /// The timestamp storage request or JSON-RPC response failed.
    #[error("failed to query DeepX finalized Timestamp.Now storage: {0}")]
    Rpc(#[source] anyhow::Error),
    /// The timestamp storage value is not one exact non-zero SCALE `u64`.
    #[error("DeepX finalized Timestamp.Now storage is invalid")]
    InvalidStorage,
    /// The block inherent or independent storage query disagreed with `Timestamp.Now`.
    #[error("DeepX finalized chain-time block evidence is invalid: {0}")]
    BlockEvidence(#[source] Box<DeepXFinalizedAccountError>),
}

/// Errors raised while reading data at a captured historical runtime checkpoint.
#[derive(Debug, Error)]
pub enum DeepXHistoricalRuntimeError {
    /// A historical runtime RPC request failed.
    #[error("failed to query DeepX historical runtime method {method}: {source}")]
    Rpc {
        /// RPC method which failed.
        method: &'static str,
        /// Underlying RPC failure.
        #[source]
        source: anyhow::Error,
    },
    /// Historical metadata is missing or is not a complete hex-encoded SCALE value.
    #[error("invalid DeepX historical runtime metadata")]
    InvalidMetadata,
    /// Historical metadata or runtime identity failed validation.
    #[error("invalid DeepX historical runtime: {0}")]
    Snapshot(#[from] SnapshotError),
}

/// Errors raised while reading `Timestamp.Now` from a finalized historical block.
#[cfg(test)]
#[derive(Debug, Error)]
pub(crate) enum DeepXFinalizedBlockTimeError {
    /// Watch capability evidence does not belong to the validated endpoint or lacks required reads.
    #[error("DeepX Watch RPC capabilities do not authorize finalized block-time reads")]
    RpcCapabilitiesMismatch,
    /// The requested block height is not finalized yet.
    #[error("DeepX block {requested} is above finalized block {finalized}")]
    NotFinalized { requested: u32, finalized: u32 },
    /// One block identity or timestamp request failed.
    #[error("failed to query DeepX finalized block-time evidence: {0}")]
    Rpc(#[source] anyhow::Error),
    /// The RPC returned a malformed or inconsistent block identity.
    #[error("DeepX finalized block-time identity is invalid")]
    InvalidBlock,
    /// Timestamp storage at the requested block is malformed.
    #[error("DeepX finalized block Timestamp.Now storage is invalid")]
    InvalidStorage,
}

/// Errors raised while collecting finalized DeepX margin evidence.
#[derive(Debug, Error)]
pub enum DeepXFinalizedMarginError {
    /// Watch capability evidence does not belong to the validated endpoint or lacks runtime calls.
    #[error("DeepX Watch RPC capabilities do not authorize finalized margin runtime calls")]
    RpcCapabilitiesMismatch,
    /// Finalized runtime observation or application failed.
    #[error(transparent)]
    Runtime(#[from] DeepXRuntimeSnapshotRefreshError),
    /// Read-only finalized runtime observation failed.
    #[error(transparent)]
    RuntimeObservation(DeepXRuntimeSnapshotObservationError),
    /// One direction-specific runtime API request or JSON-RPC response failed.
    #[error("failed to query DeepX finalized {direction:?} margin evidence: {source}")]
    Rpc {
        /// Margin weighting requested from the runtime.
        direction: DeepXMarginDirection,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// One runtime response was not one exact SCALE map entry for the requested subaccount.
    #[error("DeepX finalized {0:?} margin evidence is invalid")]
    InvalidEvidence(DeepXMarginDirection),
}

/// Errors raised while collecting finalized DeepX lending evidence.
#[derive(Debug, Error)]
pub enum DeepXFinalizedLendingError {
    /// Watch capability evidence does not belong to the endpoint or lacks required read methods.
    #[error("DeepX Watch RPC capabilities do not authorize finalized lending reads")]
    RpcCapabilitiesMismatch,
    /// Finalized runtime observation or application failed.
    #[error(transparent)]
    Runtime(#[from] DeepXRuntimeSnapshotRefreshError),
    /// Read-only finalized runtime observation failed.
    #[error(transparent)]
    RuntimeObservation(DeepXRuntimeSnapshotObservationError),
    /// The complete account-position runtime request or JSON-RPC response failed.
    #[error("failed to query DeepX finalized lending positions: {0}")]
    PositionsRpc(#[source] anyhow::Error),
    /// One asset-specific runtime request or JSON-RPC response failed.
    #[error(
        "failed to query DeepX finalized lending method {method} for market {market_id}: {source}"
    )]
    AssetRpc {
        /// Runtime API method which failed.
        method: &'static str,
        /// Lending market containing the asset.
        market_id: u8,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// One finalized EVM precompile request or JSON-RPC response failed.
    #[error("failed to query DeepX finalized maximum withdrawal for market {market_id}: {source}")]
    WithdrawalRpc {
        /// Lending market containing the asset.
        market_id: u8,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The account-position response was not canonical, complete, or exactly scoped.
    #[error("DeepX finalized lending position evidence is invalid")]
    InvalidPositions,
    /// One asset response was malformed or contradicted the retained position identity.
    #[error("DeepX finalized lending asset evidence is invalid for market {0}")]
    InvalidAssetEvidence(u8),
    /// One maximum-withdrawal response was not one exact ABI-encoded `uint128`.
    #[error("DeepX finalized maximum withdrawal evidence is invalid for market {0}")]
    InvalidWithdrawalEvidence(u8),
}

/// Errors raised while collecting finalized Spot market specification evidence.
#[derive(Debug, Error)]
pub enum DeepXFinalizedSpotMarketSpecError {
    /// Watch capability evidence does not belong to the endpoint or lacks required reads.
    #[error("DeepX Watch RPC capabilities do not authorize finalized Spot market reads")]
    RpcCapabilitiesMismatch,
    /// The bounded batch size is zero or exceeds the supported maximum.
    #[error("DeepX finalized Spot spec batch size {0} is outside 1..=256")]
    InvalidBatchSize(usize),
    /// A deployment pair identifier appears more than once in a batch.
    #[error("DeepX finalized Spot spec batch contains a duplicate pair")]
    DuplicatePair,
    /// Read-only finalized runtime observation failed.
    #[error(transparent)]
    RuntimeObservation(DeepXRuntimeSnapshotObservationError),
    /// The selected block or its storage did not establish a consistent checkpoint.
    #[error("DeepX finalized Spot market checkpoint evidence is inconsistent: {0}")]
    Checkpoint(Box<DeepXFinalizedAccountError>),
    /// The finalized storage request or JSON-RPC response failed.
    #[error("failed to query DeepX finalized Spot market spec for pair 0x{pair}: {source}")]
    Rpc {
        /// Deployment-provided bytes32 Spot pair identity.
        pair: String,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The storage response is malformed or contains an invalid zero constraint.
    #[error("DeepX finalized Spot market spec is invalid for pair 0x{0}")]
    InvalidEvidence(String),
}

/// Errors raised while collecting finalized Perp precompile order constraints.
#[derive(Debug, Error)]
pub enum DeepXFinalizedPerpOrderSpecError {
    /// Watch capability evidence does not belong to the endpoint or lacks required reads.
    #[error("DeepX Watch RPC capabilities do not authorize finalized Perp market reads")]
    RpcCapabilitiesMismatch,
    /// Read-only finalized runtime observation failed.
    #[error(transparent)]
    RuntimeObservation(DeepXRuntimeSnapshotObservationError),
    /// The selected block or its storage did not establish a consistent checkpoint.
    #[error("DeepX finalized Perp market checkpoint evidence is inconsistent: {0}")]
    Checkpoint(Box<DeepXFinalizedAccountError>),
    /// The finalized EVM precompile request or JSON-RPC response failed.
    #[error("failed to query DeepX finalized Perp market {market_id}: {source}")]
    Rpc {
        /// Requested Perp market identifier.
        market_id: u16,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The finalized storage request or JSON-RPC response failed.
    #[error("failed to query DeepX finalized Perp market storage {market_id}: {source}")]
    StorageRpc {
        /// Requested Perp market identifier.
        market_id: u16,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The ABI response is malformed, mismatched, or contains a zero constraint.
    #[error("DeepX finalized Perp market evidence is invalid for market {0}")]
    InvalidEvidence(u16),
}

/// Errors raised while collecting finalized perpetual-position storage evidence.
#[derive(Debug, Error)]
pub enum DeepXFinalizedPerpPositionsError {
    /// Watch capability evidence does not belong to the endpoint or lacks storage reads.
    #[error("DeepX Watch RPC capabilities do not authorize finalized position storage reads")]
    RpcCapabilitiesMismatch,
    /// Finalized runtime observation or signing-service application failed.
    #[error(transparent)]
    Runtime(#[from] DeepXRuntimeSnapshotRefreshError),
    /// Read-only finalized runtime observation failed.
    #[error(transparent)]
    RuntimeObservation(DeepXRuntimeSnapshotObservationError),
    /// One finalized storage request or JSON-RPC response failed.
    #[error("failed to query DeepX finalized perpetual-position storage {storage}: {source}")]
    Rpc {
        /// Storage entry which failed.
        storage: &'static str,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The runtime position limit was zero, excessive, malformed, or had trailing bytes.
    #[error("DeepX finalized maximum perpetual-position count is invalid")]
    InvalidMaximum,
    /// The position vector was malformed, exceeded the runtime maximum, contained trailing bytes,
    /// or contradicted account and market invariants.
    #[error("DeepX finalized perpetual-position storage evidence is invalid")]
    InvalidEvidence,
}

/// Errors raised while collecting finalized active perpetual-order storage evidence.
#[derive(Debug, Error)]
pub enum DeepXFinalizedPerpOrdersError {
    /// Watch capability evidence does not belong to the endpoint or lacks storage reads.
    #[error("DeepX Watch RPC capabilities do not authorize finalized order storage reads")]
    RpcCapabilitiesMismatch,
    /// Finalized runtime observation or signing-service application failed.
    #[error(transparent)]
    Runtime(#[from] DeepXRuntimeSnapshotRefreshError),
    /// Read-only finalized runtime observation failed.
    #[error(transparent)]
    RuntimeObservation(DeepXRuntimeSnapshotObservationError),
    /// One finalized storage request or JSON-RPC response failed.
    #[error("failed to query DeepX finalized perpetual-order storage {storage}: {source}")]
    Rpc {
        /// Storage entry which failed.
        storage: &'static str,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The active-order index was malformed, excessive, non-canonical, or incorrectly scoped.
    #[error("DeepX finalized active perpetual-order index is invalid")]
    InvalidIndex,
    /// `OrderInfo` was missing, malformed, non-active, or contradicted its account index entry.
    #[error("DeepX finalized perpetual-order detail evidence is invalid")]
    InvalidOrderEvidence,
}

/// Errors raised while collecting finalized Spot order storage evidence.
#[derive(Debug, Error)]
pub enum DeepXFinalizedSpotOrdersError {
    /// Watch capability evidence does not belong to the endpoint or lacks storage reads.
    #[error("DeepX Watch RPC capabilities do not authorize finalized Spot order storage reads")]
    RpcCapabilitiesMismatch,
    /// Finalized runtime observation or signing-service application failed.
    #[error(transparent)]
    Runtime(#[from] DeepXRuntimeSnapshotRefreshError),
    /// Read-only finalized runtime observation failed.
    #[error(transparent)]
    RuntimeObservation(DeepXRuntimeSnapshotObservationError),
    /// A finalized storage request or JSON-RPC response failed.
    #[error("failed to query DeepX finalized Spot order storage {storage}: {source}")]
    Rpc {
        /// Storage entry which failed.
        storage: &'static str,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The active-order index was malformed or did not match the requested order IDs.
    #[error("DeepX finalized Spot active-order index is invalid")]
    InvalidIndex,
    /// `OrderInfo` was missing, malformed, or contradicted its account index entry.
    #[error("DeepX finalized Spot order detail evidence is invalid")]
    InvalidOrderEvidence,
}

/// Errors raised while collecting complete finalized raw account evidence.
#[derive(Debug, Error)]
pub enum DeepXFinalizedAccountError {
    /// Watch capability evidence does not belong to the endpoint or lacks required read methods.
    #[error("DeepX Watch RPC capabilities do not authorize complete finalized account reads")]
    RpcCapabilitiesMismatch,
    /// The endpoint did not preserve the approved historical runtime version.
    #[error(transparent)]
    HistoricalRuntime(#[from] DeepXHistoricalRuntimeError),
    /// The harmless control call failed at the requested finalized block.
    #[error("failed to verify DeepX eth_call at the finalized checkpoint: {0}")]
    EthCallProbeRpc(#[source] anyhow::Error),
    /// The harmless control call returned an unexpected value at the valid checkpoint.
    #[error("DeepX eth_call control result at the finalized checkpoint is invalid")]
    InvalidEthCallProbeResult,
    /// The invalid block-number probe did not return a JSON-RPC error.
    #[error("DeepX eth_call accepted an impossible block selector")]
    EthCallBlockHashIgnored,
    /// The invalid block-number probe failed without a valid JSON-RPC error envelope.
    #[error("DeepX eth_call block-selector rejection was not verifiable: {0}")]
    EthCallRejectionRpc(#[source] anyhow::Error),
    /// The invalid storage-block probe did not return a JSON-RPC error.
    #[error("DeepX state_getStorage accepted an impossible block hash")]
    StorageBlockHashIgnored,
    /// The invalid storage-block probe failed without a valid JSON-RPC error envelope.
    #[error("DeepX state_getStorage block rejection was not verifiable: {0}")]
    StorageRejectionRpc(#[source] anyhow::Error),
    /// No subaccounts were supplied for a batch observation.
    #[error("DeepX finalized account batch requires at least one subaccount")]
    EmptySubaccounts,
    /// One subaccount appeared more than once in a batch account observation.
    #[error("DeepX finalized account batch contains a duplicate subaccount: 0x{}", hex::encode(.0))]
    DuplicateSubaccount([u8; 20]),
    /// One account in a batch failed at the shared finalized checkpoint.
    #[error(
        "DeepX finalized account evidence failed for subaccount 0x{}: {source}",
        hex::encode(subaccount)
    )]
    SubaccountObservation {
        /// AccountId20 whose evidence could not be collected.
        subaccount: [u8; 20],
        /// Underlying account evidence failure.
        source: Box<Self>,
    },
    /// Timestamp storage changed despite every read targeting the same finalized block hash.
    #[error(
        "DeepX finalized Timestamp.Now changed at one checkpoint for subaccount 0x{}: expected {expected_ms}, received {received_ms}",
        hex::encode(subaccount)
    )]
    InconsistentTimestamp {
        /// Subaccount whose timestamp observation conflicted.
        subaccount: [u8; 20],
        /// Timestamp from the preceding read at this checkpoint.
        expected_ms: u64,
        /// Conflicting timestamp read from the same checkpoint.
        received_ms: u64,
    },
    /// The canonical block timestamp inherent disagreed with storage at that block hash.
    #[error(
        "DeepX finalized Timestamp.Now differs from the block timestamp inherent: block {block_ms}, storage {storage_ms}"
    )]
    BlockTimestampMismatch {
        /// Timestamp from the canonical block's `Timestamp.set` inherent.
        block_ms: u64,
        /// Timestamp read from storage using the same block hash.
        storage_ms: u64,
    },
    /// Timestamp returned by `state_queryStorageAt` disagreed with the block inherent.
    #[error(
        "DeepX finalized state_queryStorageAt Timestamp.Now differs from the block timestamp inherent: block {block_ms}, query {query_ms}"
    )]
    StorageQueryTimestampMismatch {
        /// Timestamp from the canonical block's `Timestamp.set` inherent.
        block_ms: u64,
        /// Timestamp returned by `state_queryStorageAt` at the same block hash.
        query_ms: u64,
    },
    /// The canonical block could not be queried at the checkpoint hash.
    #[error("failed to query DeepX finalized block timestamp inherent: {0}")]
    BlockRpc(#[source] anyhow::Error),
    /// The storage query for the timestamp at the checkpoint failed.
    #[error("failed to query DeepX finalized Timestamp.Now through state_queryStorageAt: {0}")]
    StorageQueryRpc(#[source] anyhow::Error),
    /// The canonical block or its timestamp inherent was malformed or missing.
    #[error("DeepX finalized block timestamp inherent is invalid")]
    InvalidBlockTimestamp,
    /// The timestamp storage query response was incomplete or malformed.
    #[error("DeepX finalized state_queryStorageAt Timestamp.Now evidence is invalid")]
    InvalidStorageQueryTimestamp,
    /// Read-only finalized runtime observation failed.
    #[error(transparent)]
    RuntimeObservation(#[from] DeepXRuntimeSnapshotObservationError),
    /// Finalized runtime observation or signing-service application failed.
    #[error(transparent)]
    Runtime(#[from] DeepXRuntimeSnapshotRefreshError),
    /// Finalized chain-time collection failed.
    #[error(transparent)]
    ChainTime(#[from] DeepXFinalizedChainTimeError),
    /// Finalized margin collection failed.
    #[error(transparent)]
    Margin(#[from] Box<DeepXFinalizedMarginError>),
    /// Finalized lending collection failed.
    #[error(transparent)]
    Lending(#[from] Box<DeepXFinalizedLendingError>),
    /// Finalized perpetual-position collection failed.
    #[error(transparent)]
    PerpPositions(#[from] Box<DeepXFinalizedPerpPositionsError>),
    /// Finalized active perpetual-order collection failed.
    #[error(transparent)]
    PerpOrders(#[from] Box<DeepXFinalizedPerpOrdersError>),
    /// Finalized active Spot-order collection failed.
    #[error(transparent)]
    SpotOrders(#[from] Box<DeepXFinalizedSpotOrdersError>),
}

/// Collects the genesis hash directly from the configured endpoint for `role`.
///
/// This observation proves only the endpoint URL and reported chain identity. It does not prove
/// support for submission, watch, finality, pool, or recovery methods.
///
/// # Errors
///
/// Returns an error for unsupported configuration, transport or JSON-RPC failure, or a genesis
/// hash that is not exactly one `0x`-prefixed 32-byte hexadecimal value.
pub async fn observe_rpc_endpoint_identity(
    config: &DeepXNetworkConfig,
    role: DeepXRpcRole,
) -> Result<DeepXObservedRpcEndpoint, DeepXRpcIdentityError> {
    let url = config.rpc_url_for(role)?;
    let client = BlockchainHttpRpcClient::new(url.clone(), None, None);
    let encoded_hash: String = execute_rpc_probe_with_retry(
        &client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [0],
        }),
    )
    .await
    .map_err(DeepXRpcIdentityError::Rpc)?;
    let hash = encoded_hash
        .strip_prefix("0x")
        .ok_or(DeepXRpcIdentityError::InvalidGenesisHash)
        .and_then(|value| {
            hex::decode_array::<32>(value).map_err(|_| DeepXRpcIdentityError::InvalidGenesisHash)
        })?;

    Ok(DeepXObservedRpcEndpoint::new(role, url, hash))
}

/// Collects and validates chain identity for every configured DeepX RPC role endpoint.
///
/// All three read-only observations complete before errors are evaluated in submission, watch,
/// then recovery order. Validated endpoints are returned only after every observation succeeds and
/// the complete set matches the configured URLs and approved testnet genesis hash.
///
/// This result does not prove support for role-specific RPC methods.
///
/// # Errors
///
/// Returns the role-specific observation failure or complete endpoint validation failure without
/// releasing a partial endpoint set.
pub async fn observe_and_validate_rpc_endpoint_identities(
    config: &DeepXNetworkConfig,
) -> Result<DeepXValidatedRpcEndpoints, DeepXRpcEndpointIdentityError> {
    let (submission, watch, recovery) = tokio::join!(
        observe_rpc_endpoint_identity(config, DeepXRpcRole::Submission),
        observe_rpc_endpoint_identity(config, DeepXRpcRole::Watch),
        observe_rpc_endpoint_identity(config, DeepXRpcRole::Recovery),
    );
    let submission = submission.map_err(|source| DeepXRpcEndpointIdentityError::Observation {
        role: DeepXRpcRole::Submission,
        source,
    })?;
    let watch = watch.map_err(|source| DeepXRpcEndpointIdentityError::Observation {
        role: DeepXRpcRole::Watch,
        source,
    })?;
    let recovery = recovery.map_err(|source| DeepXRpcEndpointIdentityError::Observation {
        role: DeepXRpcRole::Recovery,
        source,
    })?;

    validate_rpc_endpoint_identities(config, [submission, watch, recovery]).map_err(Into::into)
}

/// Probes required methods on one identity-validated role endpoint.
///
/// This evidence proves only that `rpc_methods` advertised each requested name at observation
/// time. It does not prove method semantics, authorize requests, or enable transaction operations.
///
/// # Errors
///
/// Returns an error when no methods are requested, the probe fails, or any required method is not
/// advertised by the endpoint.
pub async fn observe_rpc_method_capabilities(
    endpoints: &DeepXValidatedRpcEndpoints,
    role: DeepXRpcRole,
    required_methods: impl IntoIterator<Item = impl Into<String>>,
) -> Result<DeepXRpcMethodCapabilities, DeepXRpcMethodCapabilityError> {
    let required_methods: BTreeSet<String> = required_methods.into_iter().map(Into::into).collect();
    if required_methods.is_empty() {
        return Err(DeepXRpcMethodCapabilityError::EmptyRequirements(role));
    }
    let client = BlockchainHttpRpcClient::new(endpoints.url_for(role).to_string(), None, None);
    let observed: ObservedRpcMethods = execute_rpc_probe_with_retry(
        &client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "rpc_methods",
            "params": [],
        }),
    )
    .await
    .map_err(|source| DeepXRpcMethodCapabilityError::Rpc { role, source })?;
    for method in &required_methods {
        if !observed.methods.contains(method) {
            return Err(DeepXRpcMethodCapabilityError::MissingMethod {
                role,
                method: method.clone(),
            });
        }
    }

    Ok(DeepXRpcMethodCapabilities {
        role,
        endpoint_url: endpoints.url_for(role).to_string(),
        methods: required_methods,
    })
}

async fn execute_rpc_probe_with_retry<T: DeserializeOwned>(
    client: &BlockchainHttpRpcClient,
    request: serde_json::Value,
) -> anyhow::Result<T> {
    for delay_ms in [250, 500] {
        match client.execute_rpc_call(request.clone()).await {
            Ok(value) => return Ok(value),
            Err(_) => dst::time::sleep(Duration::from_millis(delay_ms)).await,
        }
    }
    client.execute_rpc_call(request).await
}

/// Collects the minimum RPC method capabilities required for all endpoint roles.
///
/// All three probes complete before errors are evaluated in submission, watch, then recovery
/// order. This proves advertised method names only; protocol semantics remain separately gated.
///
/// # Errors
///
/// Returns a role-attributed error unless every identity-validated endpoint advertises all methods
/// required by its role.
pub async fn observe_and_validate_rpc_method_capabilities(
    endpoints: &DeepXValidatedRpcEndpoints,
) -> Result<DeepXValidatedRpcMethodCapabilities, DeepXRpcMethodCapabilitiesError> {
    let (submission, watch, recovery) = tokio::join!(
        observe_rpc_method_capabilities(
            endpoints,
            DeepXRpcRole::Submission,
            DEEPX_SUBMISSION_RPC_METHODS.iter().copied(),
        ),
        observe_rpc_method_capabilities(
            endpoints,
            DeepXRpcRole::Watch,
            DEEPX_WATCH_RPC_METHODS.iter().copied(),
        ),
        observe_rpc_method_capabilities(
            endpoints,
            DeepXRpcRole::Recovery,
            DEEPX_RECOVERY_RPC_METHODS.iter().copied(),
        ),
    );
    let submission = submission.map_err(|source| DeepXRpcMethodCapabilitiesError {
        role: DeepXRpcRole::Submission,
        source,
    })?;
    let watch = watch.map_err(|source| DeepXRpcMethodCapabilitiesError {
        role: DeepXRpcRole::Watch,
        source,
    })?;
    let recovery = recovery.map_err(|source| DeepXRpcMethodCapabilitiesError {
        role: DeepXRpcRole::Recovery,
        source,
    })?;

    Ok(DeepXValidatedRpcMethodCapabilities {
        submission,
        watch,
        recovery,
    })
}

/// Collects a signing runtime from one finalized DeepX block.
///
/// The ordinary configured RPC endpoint is used without asserting role-specific method support.
/// Runtime version and metadata reads use the same block hash returned by the endpoint. This
/// function does not install the snapshot or authorize signing.
///
/// # Errors
///
/// Returns an error for unsupported configuration, request or response failure, malformed hashes
/// or metadata, another genesis, or unsupported signing extensions.
pub async fn observe_approved_finalized_runtime_snapshot(
    config: &DeepXNetworkConfig,
) -> Result<DeepXObservedRuntimeSnapshot, DeepXRuntimeSnapshotObservationError> {
    let url = config.rpc_url()?;
    let client = BlockchainHttpRpcClient::new(url.clone(), None, None);
    let encoded_genesis: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [0],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "chain_getBlockHash",
            source,
        })?;
    let genesis_hash = decode_prefixed_hash(&encoded_genesis, "genesis hash")?;
    let checkpoint = observe_finalized_checkpoint_at(url).await?;
    let (version, metadata) = observe_runtime_metadata_at(&client, checkpoint.block_hash()).await?;
    let snapshot = RuntimeSnapshot::observed_testnet(
        &config.environment,
        genesis_hash,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )?;
    Ok(DeepXObservedRuntimeSnapshot {
        snapshot,
        checkpoint,
    })
}

/// Collects a read-only runtime snapshot pinned to one finalized DeepX block.
///
/// The returned type cannot initialize a signing service or authorize transaction encoding. This
/// permits observed runtime metadata to support finalized evidence reads without authorizing
/// signing.
///
/// # Errors
///
/// Returns an error for unsupported configuration, request or response failure, malformed hashes
/// or metadata, another genesis, or unsupported runtime extensions.
pub async fn observe_approved_finalized_read_only_runtime_snapshot(
    config: &DeepXNetworkConfig,
) -> Result<DeepXObservedReadOnlyRuntimeSnapshot, DeepXRuntimeSnapshotObservationError> {
    let url = config.rpc_url()?;
    observe_approved_finalized_read_only_runtime_snapshot_at(&config.environment, url).await
}

/// Collects a signing runtime from the chain-identity-validated Watch endpoint.
///
/// This bootstrap observation initializes a snapshot service before its first atomic refresh. The
/// endpoint set must already have passed complete role identity validation; this function does not
/// probe method capabilities, install the snapshot, or authorize signing.
///
/// # Errors
///
/// Returns an error for request or response failure, malformed hashes or metadata, or a runtime
/// snapshot with another genesis or unsupported signing extensions.
pub(crate) async fn observe_approved_finalized_runtime_snapshot_for_endpoints(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
) -> Result<DeepXObservedRuntimeSnapshot, DeepXRuntimeSnapshotObservationError> {
    let url = endpoints.url_for(DeepXRpcRole::Watch).to_string();
    let checkpoint = observe_finalized_checkpoint_at(url.clone()).await?;
    let client = BlockchainHttpRpcClient::new(url, None, None);
    let (version, metadata) = observe_runtime_metadata_at(&client, checkpoint.block_hash()).await?;
    let snapshot = RuntimeSnapshot::observed_testnet(
        environment,
        endpoints.genesis_hash(),
        version.spec_version,
        version.transaction_version,
        &metadata,
    )?;
    Ok(DeepXObservedRuntimeSnapshot {
        snapshot,
        checkpoint,
    })
}

/// Observes signing metadata at the current canonical Watch block.
///
/// # Errors
///
/// Returns an error for an invalid runtime, RPC failure, or a block that changed canonical hash.
pub async fn observe_current_runtime_snapshot_for_endpoints(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
) -> Result<RuntimeSnapshot, DeepXRuntimeSnapshotObservationError> {
    Ok(
        observe_current_runtime_snapshot_with_checkpoint(environment, endpoints)
            .await?
            .0,
    )
}

async fn observe_current_runtime_snapshot_with_checkpoint(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
) -> Result<(RuntimeSnapshot, [u8; 32], u32), DeepXRuntimeSnapshotObservationError> {
    let client = BlockchainHttpRpcClient::new(
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
        None,
        None,
    );
    let encoded_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "chain_getBlockHash", "params": [],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "chain_getBlockHash",
            source,
        })?;
    let block_hash = decode_prefixed_hash(&encoded_hash, "current block hash")?;
    let header: ObservedBlockHeader = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "chain_getHeader", "params": [encoded_hash],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "chain_getHeader",
            source,
        })?;
    let block_number = decode_block_number(&header.number)?;
    let rejection: RpcNodeHttpResponse<ObservedRuntimeVersion> = client
        .execute_rpc_response(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "state_getRuntimeVersion",
            "params": [format!("0x{}", "00".repeat(32))],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "state_getRuntimeVersion",
            source,
        })?;
    if rejection.jsonrpc.as_deref() != Some("2.0")
        || rejection.id != Some(1)
        || rejection.result.is_some()
        || rejection.error.is_none()
    {
        return Err(DeepXRuntimeSnapshotObservationError::BlockHashIgnored);
    }
    let (version, metadata) = observe_runtime_metadata_at(&client, block_hash).await?;
    let snapshot = RuntimeSnapshot::observed_testnet(
        environment,
        endpoints.genesis_hash(),
        version.spec_version,
        version.transaction_version,
        &metadata,
    )?;
    let canonical_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "chain_getBlockHash", "params": [block_number],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "chain_getBlockHash",
            source,
        })?;
    if decode_prefixed_hash(&canonical_hash, "canonical runtime block hash")? != block_hash {
        return Err(DeepXRuntimeSnapshotObservationError::InvalidCheckpoint);
    }
    Ok((snapshot, block_hash, block_number))
}

/// Reads current runtime and chain time while retaining a finalized recovery scan boundary.
///
/// The timestamp storage, block inherent, and runtime metadata are read at one current canonical
/// hash. A finalized head is queried only as a durable pre-submission scan starting point.
pub(crate) async fn observe_and_apply_current_submission_clock(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    service: &DeepXRuntimeSnapshotService,
) -> anyhow::Result<DeepXCurrentSubmissionClockEvidence> {
    let observation_started = Instant::now();
    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch = capabilities.for_role(DeepXRpcRole::Watch);
    anyhow::ensure!(
        watch.role() == DeepXRpcRole::Watch
            && watch.endpoint_url() == watch_url
            && [
                "chain_getBlockHash",
                "chain_getHeader",
                "chain_getBlock",
                "chain_getFinalizedHead",
                "state_getStorage",
            ]
            .into_iter()
            .all(|method| watch.methods().contains(method)),
        "DeepX Watch RPC capabilities do not authorize current submission clock reads",
    );
    let (snapshot, block_hash, block_number) =
        observe_current_runtime_snapshot_with_checkpoint(environment, endpoints).await?;
    service.apply_validated(&snapshot)?;
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    let encoded_hash = format!("0x{}", hex::encode(block_hash));
    let rejected: RpcNodeHttpResponse<String> = client
        .execute_rpc_response(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "state_getStorage",
            "params": [timestamp_now_storage_key(), format!("0x{}", "00".repeat(32))],
        }))
        .await?;
    if rejected.jsonrpc.as_deref() != Some("2.0")
        || rejected.id != Some(1)
        || rejected.result.is_some()
        || rejected.error.is_none()
    {
        service.halt_for_inconsistent_evidence()?;
        anyhow::bail!("DeepX Watch RPC ignored an invalid timestamp block hash");
    }
    let encoded_time: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "state_getStorage",
            "params": [timestamp_now_storage_key(), encoded_hash],
        }))
        .await?;
    let observed_timestamp_ms = match decode_timestamp_now(&encoded_time) {
        Ok(timestamp_ms) => timestamp_ms,
        Err(e) => {
            service.halt_for_inconsistent_evidence()?;
            return Err(e.into());
        }
    };
    let block: ObservedTimestampBlockResponse = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "chain_getBlock",
            "params": [encoded_hash],
        }))
        .await?;
    let block_number_matches = decode_block_number(&block.block.header.number)
        .is_ok_and(|received| received == block_number);
    let inherent_time = decode_timestamp_set_inherent(&block.block.extrinsics);
    if !block_number_matches
        || !inherent_time.is_ok_and(|received| received == observed_timestamp_ms)
    {
        service.halt_for_inconsistent_evidence()?;
        anyhow::bail!("DeepX current timestamp storage and block inherent differ");
    }
    let canonical_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "chain_getBlockHash", "params": [block_number],
        }))
        .await?;
    anyhow::ensure!(
        decode_prefixed_hash(&canonical_hash, "canonical submission block hash")? == block_hash,
        "DeepX current submission clock block is no longer canonical",
    );
    let scan_checkpoint = observe_finalized_checkpoint_at(watch_url.to_string()).await?;
    let timestamp_ms =
        advance_submission_timestamp(observed_timestamp_ms, observation_started.elapsed())?;
    Ok(DeepXCurrentSubmissionClockEvidence {
        scan_checkpoint,
        runtime_identity: snapshot.identity().clone(),
        timestamp_ms,
    })
}

/// Observes and atomically applies an approved finalized runtime snapshot.
///
/// The observation uses only the chain-identity-validated Watch endpoint. Changed runtime versions
/// block new signing before the metadata request; changed raw metadata hashes block it before
/// fixture validation. Failures after this evidence leave the gate blocked and old permits intact.
/// Errors before change evidence do not block signing. This one-shot operation does not
/// poll for runtime upgrades, select mortality parameters, or authorize signing or submission.
///
/// # Errors
///
/// Returns an error when runtime observation or fixture validation fails, another runtime identity
/// is pending, old signing permits remain, or snapshot service state is unavailable.
pub async fn observe_and_apply_approved_finalized_runtime_snapshot(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    service: &DeepXRuntimeSnapshotService,
) -> Result<DeepXAppliedRuntimeSnapshot, DeepXRuntimeSnapshotRefreshError> {
    let observation =
        observe_approved_finalized_runtime_snapshot_for_endpoints(environment, endpoints).await?;
    let update = service.apply_validated(observation.snapshot())?;

    Ok(DeepXAppliedRuntimeSnapshot {
        checkpoint: observation.checkpoint(),
        update,
        snapshot: observation.into_snapshot(),
    })
}

/// Observes runtime identity and `Timestamp.Now` at one finalized Watch block.
///
/// The observed runtime snapshot is applied before timestamp storage is read from the exact same
/// checkpoint. `Timestamp.Now` must also agree with the block inherent and independent storage
/// query. Inconsistent checkpoint evidence blocks new signing permits even when a later check fails.
/// This function performs no nonce allocation, signing, or submission.
///
/// # Errors
///
/// Returns an error unless capability evidence authorizes the required Watch reads, the observed
/// runtime snapshot can be applied at a finalized checkpoint, and all timestamp observations agree.
pub async fn observe_and_apply_finalized_chain_time(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    service: &DeepXRuntimeSnapshotService,
) -> Result<DeepXFinalizedChainTimeEvidence, DeepXFinalizedChainTimeError> {
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || !["state_getStorage", "state_queryStorageAt", "chain_getBlock"]
            .into_iter()
            .all(|method| watch_capabilities.methods().contains(method))
    {
        return Err(DeepXFinalizedChainTimeError::RpcCapabilitiesMismatch);
    }
    let applied =
        observe_and_apply_approved_finalized_runtime_snapshot(environment, endpoints, service)
            .await?;
    let client = BlockchainHttpRpcClient::new(
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
        None,
        None,
    );
    let checkpoint = applied.checkpoint();
    let timestamp_ms = observe_finalized_chain_time_with_client(&client, checkpoint).await?;
    if let Err(e) = validate_block_timestamp_inherent(&client, checkpoint, timestamp_ms).await {
        if matches!(
            &e,
            DeepXFinalizedAccountError::BlockTimestampMismatch { .. }
                | DeepXFinalizedAccountError::StorageQueryTimestampMismatch { .. }
                | DeepXFinalizedAccountError::InvalidBlockTimestamp
                | DeepXFinalizedAccountError::InvalidStorageQueryTimestamp
        ) {
            service
                .halt_for_inconsistent_evidence()
                .map_err(DeepXRuntimeSnapshotRefreshError::from)?;
        }
        return Err(DeepXFinalizedChainTimeError::BlockEvidence(Box::new(e)));
    }

    Ok(DeepXFinalizedChainTimeEvidence {
        checkpoint,
        runtime_identity: applied.identity().clone(),
        timestamp_ms,
    })
}

/// Reads `Timestamp.Now` at `block_number` after proving the height is finalized.
///
/// The block hash is resolved by height from the identity-validated Watch endpoint, then its
/// header number is checked before reading storage at that exact hash. Heights above the current
/// finalized head are rejected.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes every required method and the
/// requested canonical block is finalized with one exact non-zero SCALE timestamp.
#[cfg(test)]
pub(crate) async fn observe_finalized_block_timestamp(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    block_number: u32,
) -> Result<u64, DeepXFinalizedBlockTimeError> {
    let watch = capabilities.for_role(DeepXRpcRole::Watch);
    if watch.role() != DeepXRpcRole::Watch
        || watch.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || [
            "chain_getBlockHash",
            "chain_getFinalizedHead",
            "chain_getHeader",
            "state_getStorage",
        ]
        .iter()
        .any(|method| !watch.methods().contains(*method))
    {
        return Err(DeepXFinalizedBlockTimeError::RpcCapabilitiesMismatch);
    }

    let client = BlockchainHttpRpcClient::new(
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
        None,
        None,
    );
    let finalized_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getFinalizedHead",
            "params": [],
        }))
        .await
        .map_err(DeepXFinalizedBlockTimeError::Rpc)?;
    decode_prefixed_hash(&finalized_hash, "finalized block hash")
        .map_err(|_| DeepXFinalizedBlockTimeError::InvalidBlock)?;
    let finalized_header: ObservedBlockHeader = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getHeader",
            "params": [finalized_hash],
        }))
        .await
        .map_err(DeepXFinalizedBlockTimeError::Rpc)?;
    let finalized_number = decode_block_number(&finalized_header.number)
        .map_err(|_| DeepXFinalizedBlockTimeError::InvalidBlock)?;
    if block_number > finalized_number {
        return Err(DeepXFinalizedBlockTimeError::NotFinalized {
            requested: block_number,
            finalized: finalized_number,
        });
    }

    let block_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [block_number],
        }))
        .await
        .map_err(DeepXFinalizedBlockTimeError::Rpc)?;
    decode_prefixed_hash(&block_hash, "historical block hash")
        .map_err(|_| DeepXFinalizedBlockTimeError::InvalidBlock)?;
    let header: ObservedBlockHeader = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getHeader",
            "params": [block_hash.clone()],
        }))
        .await
        .map_err(DeepXFinalizedBlockTimeError::Rpc)?;
    if decode_block_number(&header.number)
        .map_err(|_| DeepXFinalizedBlockTimeError::InvalidBlock)?
        != block_number
    {
        return Err(DeepXFinalizedBlockTimeError::InvalidBlock);
    }
    let encoded: Option<String> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [timestamp_now_storage_key(), block_hash],
        }))
        .await
        .map_err(DeepXFinalizedBlockTimeError::Rpc)?;
    let encoded = encoded.ok_or(DeepXFinalizedBlockTimeError::InvalidStorage)?;
    decode_timestamp_now(&encoded).map_err(|_| DeepXFinalizedBlockTimeError::InvalidStorage)
}

/// Observes initial and maintenance margin aggregates at one finalized Watch checkpoint.
///
/// Both `state_call` requests use the exact checkpoint whose runtime metadata was fixture-approved
/// and atomically applied to `service`. The node's custom lending RPC is deliberately not used
/// because its current implementation ignores the caller's block hash and reads best state.
/// Values remain raw runtime quote units; this boundary does not infer a currency, asset balances,
/// or Nautilus free/locked semantics.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes `state_call`, the finalized runtime
/// remains approved, and both SCALE responses contain exactly the requested subaccount and no
/// trailing data.
pub async fn observe_and_apply_finalized_margin_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    service: &DeepXRuntimeSnapshotService,
    subaccount: [u8; 20],
) -> Result<DeepXFinalizedMarginEvidence, DeepXFinalizedMarginError> {
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || !watch_capabilities.methods().contains("state_call")
    {
        return Err(DeepXFinalizedMarginError::RpcCapabilitiesMismatch);
    }
    let applied =
        observe_and_apply_approved_finalized_runtime_snapshot(environment, endpoints, service)
            .await?;
    let (initial, maintenance) = observe_finalized_margin_requirements_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        applied.checkpoint(),
        subaccount,
    )
    .await?;

    Ok(DeepXFinalizedMarginEvidence {
        checkpoint: applied.checkpoint(),
        runtime_identity: applied.identity().clone(),
        subaccount,
        initial,
        maintenance,
    })
}

/// Observes finalized margin evidence under the read-only runtime approval boundary.
///
/// This path never constructs or updates a signing service. Both direction-specific runtime calls
/// remain pinned to the exact finalized checkpoint whose runtime identity was approved.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes `state_call`, the finalized
/// runtime is approved for read-only use, and both SCALE responses are canonical and belong only
/// to `subaccount`.
pub async fn observe_finalized_margin_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    subaccount: [u8; 20],
) -> Result<DeepXFinalizedMarginEvidence, DeepXFinalizedMarginError> {
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || !watch_capabilities.methods().contains("state_call")
    {
        return Err(DeepXFinalizedMarginError::RpcCapabilitiesMismatch);
    }
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await
    .map_err(DeepXFinalizedMarginError::RuntimeObservation)?;
    let (initial, maintenance) = observe_finalized_margin_requirements_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        observation.checkpoint(),
        subaccount,
    )
    .await?;

    Ok(DeepXFinalizedMarginEvidence {
        checkpoint: observation.checkpoint(),
        runtime_identity: observation.snapshot().identity().clone(),
        subaccount,
        initial,
        maintenance,
    })
}

/// Observes both margin directions at one current canonical Watch block.
///
/// This observation has no finality guarantee and must be rechecked before account publication.
///
/// # Errors
///
/// Returns an error for unsupported RPC capabilities, malformed or inconsistent margin evidence,
/// an ignored block-hash parameter, or a block removed from the canonical chain.
pub async fn observe_current_margin_evidence(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    subaccount: [u8; 20],
) -> anyhow::Result<DeepXCurrentMarginEvidence> {
    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch = capabilities.for_role(DeepXRpcRole::Watch);
    anyhow::ensure!(
        watch.role() == DeepXRpcRole::Watch
            && watch.endpoint_url() == watch_url
            && ["chain_getBlockHash", "chain_getHeader", "state_call"]
                .into_iter()
                .all(|method| watch.methods().contains(method)),
        "DeepX Watch RPC capabilities do not authorize current margin reads",
    );
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    let encoded_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "chain_getBlockHash", "params": [],
        }))
        .await?;
    let block_hash = decode_prefixed_hash(&encoded_hash, "current margin block hash")?;
    let header: ObservedBlockHeader = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "chain_getHeader", "params": [encoded_hash],
        }))
        .await?;
    let block_number = decode_block_number(&header.number)?;
    let input = (
        vec![subaccount],
        DeepXMarginDirection::Initial.as_runtime_u8(),
    )
        .encode();
    let rejection: RpcNodeHttpResponse<String> = client
        .execute_rpc_response(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "state_call",
            "params": [
                DEEPX_MARGIN_RUNTIME_API_METHOD,
                format!("0x{}", hex::encode(input)),
                format!("0x{}", "00".repeat(32)),
            ],
        }))
        .await?;
    anyhow::ensure!(
        rejection.jsonrpc.as_deref() == Some("2.0")
            && rejection.id == Some(1)
            && rejection.result.is_none()
            && rejection.error.is_some(),
        "DeepX Watch RPC ignored an invalid margin block hash",
    );
    let first = observe_margin_requirements_at_hash(&client, block_hash, subaccount).await?;
    let second = observe_margin_requirements_at_hash(&client, block_hash, subaccount).await?;
    anyhow::ensure!(
        first == second,
        "DeepX margin changed at a fixed block hash"
    );
    let canonical_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "chain_getBlockHash", "params": [block_number],
        }))
        .await?;
    anyhow::ensure!(
        decode_prefixed_hash(&canonical_hash, "canonical margin block hash")? == block_hash,
        "DeepX current margin block is no longer canonical",
    );
    Ok(DeepXCurrentMarginEvidence {
        block_hash,
        block_number,
        subaccount,
        initial: first.0,
        maintenance: first.1,
    })
}

/// Observes raw Spot market constraints at one approved finalized Watch checkpoint.
///
/// The call reads metadata-typed `SpotMarket.SpotMarkets` at one finalized block hash. The result
/// preserves runtime units and does not infer token scaling.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes the required reads, the finalized
/// runtime and block checkpoint pass consistency checks, and storage contains three positive
/// constraints for the requested pair.
pub async fn observe_finalized_spot_market_spec_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    pair: [u8; 32],
) -> Result<DeepXFinalizedSpotMarketSpecEvidence, DeepXFinalizedSpotMarketSpecError> {
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || !watch_capabilities.methods().contains("state_getStorage")
        || !watch_capabilities
            .methods()
            .contains("state_queryStorageAt")
        || !watch_capabilities.methods().contains("chain_getBlock")
    {
        return Err(DeepXFinalizedSpotMarketSpecError::RpcCapabilitiesMismatch);
    }
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await
    .map_err(DeepXFinalizedSpotMarketSpecError::RuntimeObservation)?;
    let client = BlockchainHttpRpcClient::new(
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
        None,
        None,
    );
    let timestamp_ms =
        validate_finalized_storage_read_checkpoint(&client, observation.checkpoint())
            .await
            .map_err(|e| DeepXFinalizedSpotMarketSpecError::Checkpoint(Box::new(e)))?;
    let evidence = observe_spot_market_spec_at(
        &client,
        pair,
        &observation,
        observation.snapshot().metadata(),
    )
    .await?;
    validate_finalized_market_timestamp_unchanged(&client, observation.checkpoint(), timestamp_ms)
        .await
        .map_err(|e| DeepXFinalizedSpotMarketSpecError::Checkpoint(Box::new(e)))?;
    Ok(evidence)
}

/// Observes a bounded set of Spot market constraints at one approved finalized Watch checkpoint.
///
/// Every storage read shares one checkpoint and the returned entries retain that checkpoint.
/// The input must contain between one and 256 unique deployment pair identifiers.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes the required reads, the finalized
/// runtime and block checkpoint pass consistency checks, and every storage response is valid.
pub async fn observe_finalized_spot_market_spec_batch(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    pairs: &[[u8; 32]],
) -> Result<Vec<DeepXFinalizedSpotMarketSpecEvidence>, DeepXFinalizedSpotMarketSpecError> {
    if pairs.is_empty() || pairs.len() > 256 {
        return Err(DeepXFinalizedSpotMarketSpecError::InvalidBatchSize(
            pairs.len(),
        ));
    }
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || !watch_capabilities.methods().contains("state_getStorage")
        || !watch_capabilities
            .methods()
            .contains("state_queryStorageAt")
        || !watch_capabilities.methods().contains("chain_getBlock")
    {
        return Err(DeepXFinalizedSpotMarketSpecError::RpcCapabilitiesMismatch);
    }
    let mut unique = std::collections::BTreeSet::new();
    if pairs.iter().any(|pair| !unique.insert(*pair)) {
        return Err(DeepXFinalizedSpotMarketSpecError::DuplicatePair);
    }
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await
    .map_err(DeepXFinalizedSpotMarketSpecError::RuntimeObservation)?;
    let client = BlockchainHttpRpcClient::new(
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
        None,
        None,
    );
    let timestamp_ms =
        validate_finalized_storage_read_checkpoint(&client, observation.checkpoint())
            .await
            .map_err(|e| DeepXFinalizedSpotMarketSpecError::Checkpoint(Box::new(e)))?;
    let mut evidence = Vec::with_capacity(pairs.len());
    for pair in pairs {
        evidence.push(
            observe_spot_market_spec_at(
                &client,
                *pair,
                &observation,
                observation.snapshot().metadata(),
            )
            .await?,
        );
    }
    validate_finalized_market_timestamp_unchanged(&client, observation.checkpoint(), timestamp_ms)
        .await
        .map_err(|e| DeepXFinalizedSpotMarketSpecError::Checkpoint(Box::new(e)))?;
    Ok(evidence)
}

pub(crate) async fn observe_current_spot_market_spec_batch(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    pairs: &[[u8; 32]],
) -> anyhow::Result<Vec<DeepXCurrentSpotMarketSpec>> {
    anyhow::ensure!(
        !pairs.is_empty() && pairs.len() <= 256,
        "invalid DeepX Spot spec batch size"
    );
    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch = capabilities.for_role(DeepXRpcRole::Watch);
    anyhow::ensure!(
        watch.role() == DeepXRpcRole::Watch
            && watch.endpoint_url() == watch_url
            && watch.methods().contains("chain_getBlockHash")
            && watch.methods().contains("chain_getHeader")
            && watch.methods().contains("state_getStorage"),
        "DeepX Watch RPC capabilities do not authorize current Spot market reads",
    );
    let mut unique = BTreeSet::new();
    anyhow::ensure!(
        pairs.iter().all(|pair| unique.insert(*pair)),
        "duplicate DeepX Spot market pair",
    );
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    let encoded_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "chain_getBlockHash", "params": [],
        }))
        .await?;
    let block_hash = decode_prefixed_hash(&encoded_hash, "current block hash")?;
    let header: ObservedBlockHeader = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "chain_getHeader", "params": [encoded_hash],
        }))
        .await?;
    let block_number = decode_block_number(&header.number)?;
    let (version, metadata) = observe_runtime_metadata_at(&client, block_hash).await?;
    let snapshot = DeepXReadOnlyRuntimeSnapshot::observed_testnet(
        environment,
        endpoints.genesis_hash(),
        version.spec_version,
        version.transaction_version,
        &metadata,
    )?;
    let rejection: RpcNodeHttpResponse<String> = client
        .execute_rpc_response(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "state_getStorage",
            "params": [spot_market_storage_key(pairs[0]), format!("0x{}", "00".repeat(32))],
        }))
        .await?;
    anyhow::ensure!(
        rejection.jsonrpc.as_deref() == Some("2.0")
            && rejection.id == Some(1)
            && rejection.result.is_none()
            && rejection.error.is_some(),
        "DeepX Watch RPC ignored an invalid Spot market block hash",
    );
    let mut specs = Vec::with_capacity(pairs.len());
    for pair in pairs {
        let key = spot_market_storage_key(*pair);
        let read = || async {
            client
                .execute_rpc_call::<Option<String>>(json!({
                    "jsonrpc": "2.0", "id": 1,
                    "method": "state_getStorage", "params": [key, encoded_hash],
                }))
                .await
        };
        let first = read().await?;
        let second = read().await?;
        anyhow::ensure!(
            first == second,
            "DeepX current Spot market storage changed at a fixed hash"
        );
        let (min_order_size, tick_size, step_size) = first
            .as_deref()
            .and_then(|encoded| {
                decode_finalized_spot_market_storage(encoded, snapshot.metadata(), *pair)
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid current DeepX Spot market spec for {}",
                    hex::encode(pair)
                )
            })?;
        specs.push(DeepXCurrentSpotMarketSpec {
            pair: *pair,
            min_order_size,
            tick_size,
            step_size,
        });
    }
    let canonical_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "chain_getBlockHash", "params": [block_number],
        }))
        .await?;
    anyhow::ensure!(
        decode_prefixed_hash(&canonical_hash, "canonical Spot market block hash")? == block_hash,
        "DeepX current Spot market block is no longer canonical",
    );
    Ok(specs)
}

async fn observe_spot_market_spec_at(
    client: &BlockchainHttpRpcClient,
    pair: [u8; 32],
    observation: &DeepXObservedReadOnlyRuntimeSnapshot,
    metadata: &subxt_core::Metadata,
) -> Result<DeepXFinalizedSpotMarketSpecEvidence, DeepXFinalizedSpotMarketSpecError> {
    let pair_hex = hex::encode(pair);
    let key = spot_market_storage_key(pair);
    let checkpoint_hash = format!("0x{}", hex::encode(observation.checkpoint().block_hash()));
    let stored: Option<String> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [key, checkpoint_hash],
        }))
        .await
        .map_err(|source| DeepXFinalizedSpotMarketSpecError::Rpc {
            pair: pair_hex.clone(),
            source,
        })?;
    let (min_order_size, tick_size, step_size) = stored
        .as_deref()
        .and_then(|encoded| decode_finalized_spot_market_storage(encoded, metadata, pair))
        .ok_or_else(|| DeepXFinalizedSpotMarketSpecError::InvalidEvidence(pair_hex.clone()))?;
    let repeated: Option<String> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [key, checkpoint_hash],
        }))
        .await
        .map_err(|source| DeepXFinalizedSpotMarketSpecError::Rpc {
            pair: pair_hex.clone(),
            source,
        })?;
    if repeated != stored {
        return Err(DeepXFinalizedSpotMarketSpecError::InvalidEvidence(pair_hex));
    }

    Ok(DeepXFinalizedSpotMarketSpecEvidence {
        checkpoint: observation.checkpoint(),
        runtime_identity: observation.snapshot().identity().clone(),
        pair,
        min_order_size,
        tick_size,
        step_size,
    })
}

fn decode_finalized_spot_market_storage(
    encoded: &str,
    metadata: &subxt_core::Metadata,
    pair: [u8; 32],
) -> Option<(u128, u128, u128)> {
    let value_type = metadata
        .pallet_by_name("SpotMarket")?
        .storage()?
        .entry_by_name("SpotMarkets")?
        .entry_type()
        .value_ty();
    if encoded.len() > 2 + 2 * DEEPX_MAX_SPOT_MARKET_STORAGE_BYTES {
        return None;
    }
    let bytes = decode_storage_hex(encoded)?;
    let mut input = bytes.as_slice();
    let market = decode_as_type(&mut input, value_type, metadata.types()).ok()?;
    if !input.is_empty() {
        return None;
    }
    let ValueDef::Composite(Composite::Named(fields)) = &market.value else {
        return None;
    };
    let ValueDef::Composite(Composite::Unnamed(pair_wrapper)) =
        &fields.iter().find(|(name, _)| name == "pair")?.1.value
    else {
        return None;
    };
    let [wrapped_pair] = pair_wrapper.as_slice() else {
        return None;
    };
    let ValueDef::Composite(Composite::Unnamed(stored_pair)) = &wrapped_pair.value else {
        return None;
    };
    if stored_pair.len() != pair.len()
        || !stored_pair
            .iter()
            .zip(pair)
            .all(|(value, expected)| value.as_u128() == Some(u128::from(expected)))
    {
        return None;
    }
    let spec = fields.iter().find(|(name, _)| name == "order_spec")?;
    let min_size = named_scale_u128(&spec.1.value, "min_qty")?;
    let tick = named_scale_u128(&spec.1.value, "tick_size")?;
    let step = named_scale_u128(&spec.1.value, "step_size")?;
    (min_size > 0 && tick > 0 && step > 0).then_some((min_size, tick, step))
}

/// Observes raw Perp order constraints at an approved finalized checkpoint.
///
/// The precompile supplies minimum size, tick, and step; metadata-typed pallet storage supplies
/// the runtime's additional minimum notional. Both reads use the same finalized block hash, and
/// shared fields must agree. The market storage bytes must also remain unchanged across two reads
/// at that hash. Other market and account checks still govern trading eligibility.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes all reads, the finalized runtime
/// is approved for read-only use, the selected block passes timestamp and EVM-selection checks,
/// and both market records agree on the requested market and three positive shared constraints.
pub async fn observe_finalized_perp_order_spec_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    market_id: u16,
) -> Result<DeepXFinalizedPerpOrderSpecEvidence, DeepXFinalizedPerpOrderSpecError> {
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || !watch_capabilities.methods().contains("eth_call")
        || !watch_capabilities.methods().contains("state_getStorage")
        || !watch_capabilities
            .methods()
            .contains("state_queryStorageAt")
        || !watch_capabilities.methods().contains("chain_getBlock")
    {
        return Err(DeepXFinalizedPerpOrderSpecError::RpcCapabilitiesMismatch);
    }
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await
    .map_err(DeepXFinalizedPerpOrderSpecError::RuntimeObservation)?;
    let call_data = perpMarketsCall { market_id }.abi_encode();
    let client = BlockchainHttpRpcClient::new(
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
        None,
        None,
    );
    let timestamp_ms = validate_finalized_market_read_checkpoint(&client, observation.checkpoint())
        .await
        .map_err(|e| DeepXFinalizedPerpOrderSpecError::Checkpoint(Box::new(e)))?;
    let encoded: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": DEEPX_PERP_MARKET_PRECOMPILE_ADDRESS.to_string(),
                    "data": format!("0x{}", hex::encode(call_data)),
                },
                format!("0x{:x}", observation.checkpoint().block_number()),
            ],
        }))
        .await
        .map_err(|source| DeepXFinalizedPerpOrderSpecError::Rpc { market_id, source })?;
    let (min_order_size, tick_size, step_size) =
        decode_finalized_perp_order_spec(&encoded, market_id)
            .ok_or(DeepXFinalizedPerpOrderSpecError::InvalidEvidence(market_id))?;
    let stored: Option<String> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [
                perp_market_storage_key(market_id),
                format!("0x{}", hex::encode(observation.checkpoint().block_hash())),
            ],
        }))
        .await
        .map_err(|source| DeepXFinalizedPerpOrderSpecError::StorageRpc { market_id, source })?;
    let (stored_min_size, stored_tick, stored_step, min_notional) = stored
        .as_deref()
        .and_then(|encoded| {
            decode_finalized_perp_market_storage(
                encoded,
                observation.snapshot().metadata(),
                market_id,
            )
        })
        .ok_or(DeepXFinalizedPerpOrderSpecError::InvalidEvidence(market_id))?;
    if (min_order_size, tick_size, step_size) != (stored_min_size, stored_tick, stored_step) {
        return Err(DeepXFinalizedPerpOrderSpecError::InvalidEvidence(market_id));
    }
    let repeated: Option<String> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [
                perp_market_storage_key(market_id),
                format!("0x{}", hex::encode(observation.checkpoint().block_hash())),
            ],
        }))
        .await
        .map_err(|source| DeepXFinalizedPerpOrderSpecError::StorageRpc { market_id, source })?;
    if repeated != stored {
        return Err(DeepXFinalizedPerpOrderSpecError::InvalidEvidence(market_id));
    }
    validate_finalized_market_timestamp_unchanged(&client, observation.checkpoint(), timestamp_ms)
        .await
        .map_err(|e| DeepXFinalizedPerpOrderSpecError::Checkpoint(Box::new(e)))?;

    Ok(DeepXFinalizedPerpOrderSpecEvidence {
        checkpoint: observation.checkpoint(),
        runtime_identity: observation.snapshot().identity().clone(),
        market_id,
        min_order_size,
        tick_size,
        step_size,
        min_notional,
    })
}

fn decode_finalized_perp_order_spec(encoded: &str, market_id: u16) -> Option<(u128, u128, u128)> {
    let bytes = encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())?;
    let market = perpMarketsCall::abi_decode_returns(&bytes).ok()?;
    if market.abi_encode() != bytes {
        return None;
    }
    let spec = market.order_spec;
    (market.id == market_id && spec.min_order_size > 0 && spec.tick_size > 0 && spec.step_size > 0)
        .then_some((spec.min_order_size, spec.tick_size, spec.step_size))
}

fn decode_finalized_perp_market_storage(
    encoded: &str,
    metadata: &subxt_core::Metadata,
    market_id: u16,
) -> Option<(u128, u128, u128, u128)> {
    let value_type = metadata
        .pallet_by_name("PerpMarket")?
        .storage()?
        .entry_by_name("PerpMarkets")?
        .entry_type()
        .value_ty();
    if encoded.len() > 2 + 2 * DEEPX_MAX_PERP_MARKET_STORAGE_BYTES {
        return None;
    }
    let bytes = decode_storage_hex(encoded)?;
    let mut input = bytes.as_slice();
    let market = decode_as_type(&mut input, value_type, metadata.types()).ok()?;
    if !input.is_empty() || named_scale_u128(&market.value, "id")? != u128::from(market_id) {
        return None;
    }
    let ValueDef::Composite(Composite::Named(fields)) = &market.value else {
        return None;
    };
    let spec = fields.iter().find(|(name, _)| name == "order_spec")?;
    let min_size = named_scale_u128(&spec.1.value, "min_qty")?;
    let tick = named_scale_u128(&spec.1.value, "tick_size")?;
    let step = named_scale_u128(&spec.1.value, "step_size")?;
    let min_notional = named_scale_u128(&spec.1.value, "min_notional")?;
    (min_size > 0 && tick > 0 && step > 0).then_some((min_size, tick, step, min_notional))
}

fn named_scale_u128(value: &ValueDef<u32>, name: &str) -> Option<u128> {
    let ValueDef::Composite(Composite::Named(fields)) = value else {
        return None;
    };
    fields.iter().find(|(field, _)| field == name)?.1.as_u128()
}

/// Observes all V2 lending positions and effective balances at one finalized Watch checkpoint.
///
/// The initial batch request enumerates the complete `u8` lending market domain. Every returned
/// asset is then corroborated with its V2 pool accounting and runtime-calculated effective deposit
/// and borrow at the exact same block hash. Assets with a deposit are also queried through the
/// lending precompile for their current maximum withdrawal. Values remain raw runtime units;
/// neither maximum withdrawal nor pool-level locked liquidity is interpreted as a user free or
/// locked balance.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes `state_call` and `eth_call`, the
/// finalized runtime remains approved, and every SCALE and ABI response is canonical,
/// identity-consistent, and contains no trailing data.
pub async fn observe_and_apply_finalized_lending_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    service: &DeepXRuntimeSnapshotService,
    subaccount: [u8; 20],
) -> Result<DeepXFinalizedLendingEvidence, DeepXFinalizedLendingError> {
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || !watch_capabilities.methods().contains("state_call")
        || !watch_capabilities.methods().contains("eth_call")
    {
        return Err(DeepXFinalizedLendingError::RpcCapabilitiesMismatch);
    }
    let applied =
        observe_and_apply_approved_finalized_runtime_snapshot(environment, endpoints, service)
            .await?;
    let positions = observe_finalized_lending_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        applied.checkpoint(),
        subaccount,
    )
    .await?;

    Ok(DeepXFinalizedLendingEvidence {
        checkpoint: applied.checkpoint(),
        runtime_identity: applied.identity().clone(),
        subaccount,
        positions,
    })
}

/// Observes finalized lending evidence under the read-only runtime approval boundary.
///
/// This path never constructs or updates a signing service. All runtime and precompile reads remain
/// pinned to the exact finalized checkpoint whose runtime identity was approved.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes `state_call` and `eth_call`, the
/// finalized runtime is approved for read-only use, and every SCALE and ABI response is canonical,
/// identity-consistent, and contains no trailing data.
pub async fn observe_finalized_lending_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    subaccount: [u8; 20],
) -> Result<DeepXFinalizedLendingEvidence, DeepXFinalizedLendingError> {
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || !watch_capabilities.methods().contains("state_call")
        || !watch_capabilities.methods().contains("eth_call")
    {
        return Err(DeepXFinalizedLendingError::RpcCapabilitiesMismatch);
    }
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await
    .map_err(DeepXFinalizedLendingError::RuntimeObservation)?;
    let positions = observe_finalized_lending_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        observation.checkpoint(),
        subaccount,
    )
    .await?;

    Ok(DeepXFinalizedLendingEvidence {
        checkpoint: observation.checkpoint(),
        runtime_identity: observation.snapshot().identity().clone(),
        subaccount,
        positions,
    })
}

/// Observes finalized active perpetual orders and applies the runtime to a signing snapshot service.
///
/// The account's `ActivePerpOrdersFor` index and every referenced `OrderInfo` value are read from
/// one finalized checkpoint. This path performs no signing or mutation, but runtime changes are
/// applied to `service` so execution-owned callers retain the existing signing gate.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes `state_queryStorageAt`, the runtime
/// can be applied to `service`, and the complete account index and order details corroborate.
pub async fn observe_and_apply_finalized_perp_orders_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    service: &DeepXRuntimeSnapshotService,
    subaccount: [u8; 20],
) -> Result<DeepXFinalizedPerpOrdersEvidence, DeepXFinalizedPerpOrdersError> {
    validate_perp_order_capabilities(endpoints, capabilities)?;
    let applied =
        observe_and_apply_approved_finalized_runtime_snapshot(environment, endpoints, service)
            .await?;
    let orders = observe_finalized_perp_orders_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        applied.checkpoint(),
        subaccount,
    )
    .await?;

    Ok(DeepXFinalizedPerpOrdersEvidence {
        checkpoint: applied.checkpoint(),
        runtime_identity: applied.identity().clone(),
        subaccount,
        orders,
    })
}

/// Observes finalized active perpetual orders under the read-only runtime approval boundary.
///
/// This path cannot construct or update a signing service. The account index and all referenced
/// order details remain pinned to the exact finalized checkpoint whose runtime identity was
/// approved for read-only use.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes `state_queryStorageAt`, the runtime
/// is approved for read-only use, and the complete account index and order details corroborate.
pub async fn observe_finalized_perp_orders_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    subaccount: [u8; 20],
) -> Result<DeepXFinalizedPerpOrdersEvidence, DeepXFinalizedPerpOrdersError> {
    validate_perp_order_capabilities(endpoints, capabilities)?;
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await
    .map_err(DeepXFinalizedPerpOrdersError::RuntimeObservation)?;
    let orders = observe_finalized_perp_orders_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        observation.checkpoint(),
        subaccount,
    )
    .await?;

    Ok(DeepXFinalizedPerpOrdersEvidence {
        checkpoint: observation.checkpoint(),
        runtime_identity: observation.snapshot().identity().clone(),
        subaccount,
        orders,
    })
}

/// Observes one active or terminal perpetual order and applies the observed runtime snapshot.
pub async fn observe_and_apply_finalized_perp_order_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    service: &DeepXRuntimeSnapshotService,
    subaccount: [u8; 20],
    order_id: u64,
) -> Result<DeepXFinalizedPerpOrderStorageEvidence, DeepXFinalizedPerpOrdersError> {
    validate_perp_order_capabilities(endpoints, capabilities)?;
    let applied =
        observe_and_apply_approved_finalized_runtime_snapshot(environment, endpoints, service)
            .await?;
    let order = observe_finalized_perp_order_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        applied.checkpoint(),
        subaccount,
        order_id,
    )
    .await?;
    Ok(DeepXFinalizedPerpOrderStorageEvidence {
        checkpoint: applied.checkpoint(),
        runtime_identity: applied.identity().clone(),
        subaccount,
        order,
    })
}

/// Observes one active or terminal perpetual order under read-only runtime approval.
pub async fn observe_finalized_perp_order_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    subaccount: [u8; 20],
    order_id: u64,
) -> Result<DeepXFinalizedPerpOrderStorageEvidence, DeepXFinalizedPerpOrdersError> {
    validate_perp_order_capabilities(endpoints, capabilities)?;
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await
    .map_err(DeepXFinalizedPerpOrdersError::RuntimeObservation)?;
    let order = observe_finalized_perp_order_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        observation.checkpoint(),
        subaccount,
        order_id,
    )
    .await?;
    Ok(DeepXFinalizedPerpOrderStorageEvidence {
        checkpoint: observation.checkpoint(),
        runtime_identity: observation.snapshot().identity().clone(),
        subaccount,
        order,
    })
}

/// Observes a bounded set of active or terminal perpetual orders at one finalized checkpoint.
pub async fn observe_and_apply_finalized_perp_orders_by_id_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    service: &DeepXRuntimeSnapshotService,
    subaccount: [u8; 20],
    order_ids: &[u64],
) -> Result<DeepXFinalizedPerpOrderStorageBatchEvidence, DeepXFinalizedPerpOrdersError> {
    validate_perp_order_capabilities(endpoints, capabilities)?;
    let applied =
        observe_and_apply_approved_finalized_runtime_snapshot(environment, endpoints, service)
            .await?;
    let orders = observe_finalized_perp_orders_by_id_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        applied.checkpoint(),
        subaccount,
        order_ids,
    )
    .await?;
    Ok(DeepXFinalizedPerpOrderStorageBatchEvidence {
        checkpoint: applied.checkpoint(),
        runtime_identity: applied.identity().clone(),
        subaccount,
        orders,
    })
}

/// Observes a bounded set of active or terminal perpetual orders under read-only approval.
pub async fn observe_finalized_perp_orders_by_id_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    subaccount: [u8; 20],
    order_ids: &[u64],
) -> Result<DeepXFinalizedPerpOrderStorageBatchEvidence, DeepXFinalizedPerpOrdersError> {
    validate_perp_order_capabilities(endpoints, capabilities)?;
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await
    .map_err(DeepXFinalizedPerpOrdersError::RuntimeObservation)?;
    let orders = observe_finalized_perp_orders_by_id_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        observation.checkpoint(),
        subaccount,
        order_ids,
    )
    .await?;
    Ok(DeepXFinalizedPerpOrderStorageBatchEvidence {
        checkpoint: observation.checkpoint(),
        runtime_identity: observation.snapshot().identity().clone(),
        subaccount,
        orders,
    })
}

fn validate_perp_order_capabilities(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
) -> Result<(), DeepXFinalizedPerpOrdersError> {
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || !watch_capabilities
            .methods()
            .contains("state_queryStorageAt")
    {
        return Err(DeepXFinalizedPerpOrdersError::RpcCapabilitiesMismatch);
    }
    Ok(())
}

async fn observe_finalized_perp_orders_at(
    watch_url: &str,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
) -> Result<Vec<DeepXPerpOrderStorageEvidence>, DeepXFinalizedPerpOrdersError> {
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    observe_finalized_perp_orders_with_client(&client, checkpoint, subaccount).await
}

async fn observe_finalized_perp_orders_with_client(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
) -> Result<Vec<DeepXPerpOrderStorageEvidence>, DeepXFinalizedPerpOrdersError> {
    let block_hash = format!("0x{}", hex::encode(checkpoint.block_hash()));
    let index_key = active_perp_orders_storage_key(subaccount);
    let mut index_change_sets: Vec<ObservedStorageChangeSet> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_queryStorageAt",
            "params": [[index_key.clone()], block_hash.clone()],
        }))
        .await
        .map_err(|source| DeepXFinalizedPerpOrdersError::Rpc {
            storage: "PerpMarket.ActivePerpOrdersFor",
            source,
        })?;
    if index_change_sets.len() != 1 || index_change_sets[0].block != block_hash {
        return Err(DeepXFinalizedPerpOrdersError::InvalidIndex);
    }
    let index_changes = std::mem::take(&mut index_change_sets[0].changes);
    if index_changes.len() != 1 || index_changes[0].0 != index_key {
        return Err(DeepXFinalizedPerpOrdersError::InvalidIndex);
    }
    let encoded_index = index_changes
        .into_iter()
        .next()
        .map(|(_, value)| value)
        .ok_or(DeepXFinalizedPerpOrdersError::InvalidIndex)?;
    let index = decode_active_perp_order_index(encoded_index, subaccount)?;
    if index.is_empty() {
        return Ok(Vec::new());
    }

    let expected = index
        .into_iter()
        .map(|entry| {
            (
                perp_order_info_storage_key(subaccount, entry.order_id),
                entry,
            )
        })
        .collect::<BTreeMap<_, _>>();
    if expected.len() > DEEPX_ABSOLUTE_MAX_PERP_ORDERS as usize {
        return Err(DeepXFinalizedPerpOrdersError::InvalidIndex);
    }
    let keys = expected.keys().cloned().collect::<Vec<_>>();
    let mut order_change_sets: Vec<ObservedStorageChangeSet> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_queryStorageAt",
            "params": [keys, block_hash.clone()],
        }))
        .await
        .map_err(|source| DeepXFinalizedPerpOrdersError::Rpc {
            storage: "PerpMarket.OrderInfo",
            source,
        })?;
    if order_change_sets.len() != 1 || order_change_sets[0].block != block_hash {
        return Err(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence);
    }
    let changes = std::mem::take(&mut order_change_sets[0].changes);
    if changes.len() != expected.len() {
        return Err(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence);
    }
    let changes = changes.into_iter().collect::<BTreeMap<_, _>>();
    if changes.len() != expected.len() || changes.keys().ne(expected.keys()) {
        return Err(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence);
    }

    let mut orders = Vec::with_capacity(expected.len());
    for (key, index_entry) in expected {
        let encoded = changes
            .get(&key)
            .and_then(Option::as_deref)
            .ok_or(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence)?;
        let bytes = decode_storage_hex(encoded)
            .ok_or(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence)?;
        let mut input = bytes.as_slice();
        let order = DeepXPerpOrderStorageEvidence::decode(&mut input)
            .map_err(|_| DeepXFinalizedPerpOrdersError::InvalidOrderEvidence)?;
        if !input.is_empty() || !perp_order_matches_index(&order, &index_entry, subaccount) {
            return Err(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence);
        }
        orders.push(order);
    }
    orders.sort_unstable_by_key(|order| (order.market_id, order.order_id));
    Ok(orders)
}

async fn observe_finalized_perp_order_at(
    watch_url: &str,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
    order_id: u64,
) -> Result<DeepXPerpOrderStorageEvidence, DeepXFinalizedPerpOrdersError> {
    observe_finalized_perp_orders_by_id_at(watch_url, checkpoint, subaccount, &[order_id])
        .await?
        .into_iter()
        .next()
        .ok_or(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence)
}

async fn observe_finalized_perp_orders_by_id_at(
    watch_url: &str,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
    order_ids: &[u64],
) -> Result<Vec<DeepXPerpOrderStorageEvidence>, DeepXFinalizedPerpOrdersError> {
    if order_ids.len() > DEEPX_ABSOLUTE_MAX_PERP_ORDERS as usize || order_ids.contains(&0) {
        return Err(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence);
    }
    let expected_ids = order_ids.iter().copied().collect::<BTreeSet<_>>();
    if expected_ids.len() != order_ids.len() {
        return Err(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence);
    }
    if expected_ids.is_empty() {
        return Ok(Vec::new());
    }
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    let block_hash = format!("0x{}", hex::encode(checkpoint.block_hash()));
    let index_key = active_perp_orders_storage_key(subaccount);
    let order_keys = expected_ids
        .iter()
        .map(|order_id| {
            (
                *order_id,
                perp_order_info_storage_key(subaccount, *order_id),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut keys = vec![index_key.clone()];
    keys.extend(order_keys.values().cloned());
    let mut changesets: Vec<ObservedStorageChangeSet> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_queryStorageAt",
            "params": [keys, block_hash.clone()],
        }))
        .await
        .map_err(|source| DeepXFinalizedPerpOrdersError::Rpc {
            storage: "PerpMarket.OrderInfo",
            source,
        })?;
    if changesets.len() != 1 || changesets[0].block != block_hash {
        return Err(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence);
    }
    let raw_changes = std::mem::take(&mut changesets[0].changes);
    if raw_changes.len() != keys.len() {
        return Err(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence);
    }
    let changes = raw_changes.into_iter().collect::<BTreeMap<_, _>>();
    if changes.len() != keys.len() || keys.iter().any(|key| !changes.contains_key(key)) {
        return Err(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence);
    }
    let index = decode_active_perp_order_index(
        changes
            .get(&index_key)
            .cloned()
            .ok_or(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence)?,
        subaccount,
    )?;
    let active_by_id = index
        .into_iter()
        .map(|entry| (entry.order_id, entry))
        .collect::<BTreeMap<_, _>>();
    let mut orders = Vec::with_capacity(expected_ids.len());
    for order_id in expected_ids {
        let order_key = order_keys
            .get(&order_id)
            .ok_or(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence)?;
        let encoded_order = changes
            .get(order_key)
            .and_then(Option::as_deref)
            .ok_or(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence)?;
        let bytes = decode_storage_hex(encoded_order)
            .ok_or(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence)?;
        let mut input = bytes.as_slice();
        let order = DeepXPerpOrderStorageEvidence::decode(&mut input)
            .map_err(|_| DeepXFinalizedPerpOrdersError::InvalidOrderEvidence)?;
        let active_entry = active_by_id.get(&order_id);
        if !input.is_empty()
            || order.order_id != order_id
            || order.owner != subaccount
            || order.create_time == 0
            || order.size == 0
            || order.size_filled.checked_add(order.size_remain) != Some(order.size)
            || order.status.is_active() != active_entry.is_some()
            || active_entry
                .is_some_and(|entry| !perp_order_matches_index(&order, entry, subaccount))
        {
            return Err(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence);
        }
        let status_quantities_match = match order.status {
            DeepXStoredOrderStatus::Open => {
                order.size_filled == 0 && order.size_remain == order.size
            }
            DeepXStoredOrderStatus::PartiallyFilled => {
                order.size_filled > 0 && order.size_remain > 0
            }
            DeepXStoredOrderStatus::Filled => {
                order.size_filled == order.size && order.size_remain == 0
            }
            DeepXStoredOrderStatus::Canceled
            | DeepXStoredOrderStatus::Rejected
            | DeepXStoredOrderStatus::Expired => true,
        };
        if !status_quantities_match {
            return Err(DeepXFinalizedPerpOrdersError::InvalidOrderEvidence);
        }
        orders.push(order);
    }
    Ok(orders)
}

/// Observes Spot orders by ID from one finalized checkpoint and applies its runtime identity.
pub async fn observe_and_apply_finalized_spot_orders_by_id_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    service: &DeepXRuntimeSnapshotService,
    subaccount: [u8; 20],
    order_ids: &[u64],
) -> Result<DeepXFinalizedSpotOrderStorageBatchEvidence, DeepXFinalizedSpotOrdersError> {
    validate_spot_order_capabilities(endpoints, capabilities)?;
    let applied =
        observe_and_apply_approved_finalized_runtime_snapshot(environment, endpoints, service)
            .await?;
    let orders = observe_finalized_spot_orders_by_id_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        applied.checkpoint(),
        subaccount,
        order_ids,
    )
    .await?;
    Ok(DeepXFinalizedSpotOrderStorageBatchEvidence {
        checkpoint: applied.checkpoint(),
        runtime_identity: applied.identity().clone(),
        subaccount,
        orders: orders.0,
        active_order_ids: orders.1,
        active_order_pairs: orders.2,
    })
}

/// Observes Spot orders by ID under read-only runtime approval.
pub async fn observe_finalized_spot_orders_by_id_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    subaccount: [u8; 20],
    order_ids: &[u64],
) -> Result<DeepXFinalizedSpotOrderStorageBatchEvidence, DeepXFinalizedSpotOrdersError> {
    validate_spot_order_capabilities(endpoints, capabilities)?;
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await
    .map_err(DeepXFinalizedSpotOrdersError::RuntimeObservation)?;
    let orders = observe_finalized_spot_orders_by_id_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        observation.checkpoint(),
        subaccount,
        order_ids,
    )
    .await?;
    Ok(DeepXFinalizedSpotOrderStorageBatchEvidence {
        checkpoint: observation.checkpoint(),
        runtime_identity: observation.snapshot().identity().clone(),
        subaccount,
        orders: orders.0,
        active_order_ids: orders.1,
        active_order_pairs: orders.2,
    })
}

fn validate_spot_order_capabilities(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
) -> Result<(), DeepXFinalizedSpotOrdersError> {
    let watch = capabilities.for_role(DeepXRpcRole::Watch);
    if watch.role() != DeepXRpcRole::Watch
        || watch.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || !watch.methods().contains("state_queryStorageAt")
    {
        return Err(DeepXFinalizedSpotOrdersError::RpcCapabilitiesMismatch);
    }
    Ok(())
}

async fn observe_finalized_spot_orders_by_id_at(
    watch_url: &str,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
    order_ids: &[u64],
) -> Result<
    (
        Vec<DeepXSpotOrderStorageEvidence>,
        Vec<u64>,
        Vec<(u64, [u8; 32])>,
    ),
    DeepXFinalizedSpotOrdersError,
> {
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    observe_finalized_spot_orders_by_id_with_client(&client, checkpoint, subaccount, order_ids)
        .await
}

async fn observe_finalized_spot_orders_by_id_with_client(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
    order_ids: &[u64],
) -> Result<
    (
        Vec<DeepXSpotOrderStorageEvidence>,
        Vec<u64>,
        Vec<(u64, [u8; 32])>,
    ),
    DeepXFinalizedSpotOrdersError,
> {
    if order_ids.len() > DEEPX_ABSOLUTE_MAX_PERP_ORDERS as usize || order_ids.contains(&0) {
        return Err(DeepXFinalizedSpotOrdersError::InvalidOrderEvidence);
    }
    let expected = order_ids.iter().copied().collect::<BTreeSet<_>>();
    if expected.len() != order_ids.len() {
        return Err(DeepXFinalizedSpotOrdersError::InvalidOrderEvidence);
    }
    let index_key = active_spot_orders_storage_key(subaccount);
    let order_keys = expected
        .iter()
        .map(|id| (*id, spot_order_info_storage_key(subaccount, *id)))
        .collect::<BTreeMap<_, _>>();
    let mut keys = vec![index_key.clone()];
    keys.extend(order_keys.values().cloned());
    let block_hash = format!("0x{}", hex::encode(checkpoint.block_hash()));
    let mut sets: Vec<ObservedStorageChangeSet> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_queryStorageAt",
            "params": [keys, block_hash.clone()],
        }))
        .await
        .map_err(|source| DeepXFinalizedSpotOrdersError::Rpc {
            storage: "SpotMarket.OrderInfo",
            source,
        })?;
    if sets.len() != 1 || sets[0].block != block_hash {
        return Err(DeepXFinalizedSpotOrdersError::InvalidOrderEvidence);
    }
    let raw = std::mem::take(&mut sets[0].changes);
    if raw.len() != keys.len() {
        return Err(DeepXFinalizedSpotOrdersError::InvalidOrderEvidence);
    }
    let changes = raw.into_iter().collect::<BTreeMap<_, _>>();
    if changes.len() != keys.len() || keys.iter().any(|key| !changes.contains_key(key)) {
        return Err(DeepXFinalizedSpotOrdersError::InvalidOrderEvidence);
    }
    let mut active_by_id = BTreeMap::new();
    let encoded_index = changes
        .get(&index_key)
        .cloned()
        .ok_or(DeepXFinalizedSpotOrdersError::InvalidIndex)?;
    if let Some(encoded_index) = encoded_index {
        let index_bytes = decode_storage_hex(&encoded_index)
            .ok_or(DeepXFinalizedSpotOrdersError::InvalidIndex)?;
        let mut input = index_bytes.as_slice();
        let Compact(pair_count) = Compact::<u32>::decode(&mut input)
            .map_err(|_| DeepXFinalizedSpotOrdersError::InvalidIndex)?;
        if pair_count > DEEPX_ABSOLUTE_MAX_PERP_ORDER_MARKETS {
            return Err(DeepXFinalizedSpotOrdersError::InvalidIndex);
        }
        let mut previous_pair = None;
        let mut total_orders = 0_u32;
        for _ in 0..pair_count {
            let pair = <[u8; 32]>::decode(&mut input)
                .map_err(|_| DeepXFinalizedSpotOrdersError::InvalidIndex)?;
            if previous_pair.is_some_and(|previous| previous >= pair) {
                return Err(DeepXFinalizedSpotOrdersError::InvalidIndex);
            }
            previous_pair = Some(pair);
            let Compact(order_count) = Compact::<u32>::decode(&mut input)
                .map_err(|_| DeepXFinalizedSpotOrdersError::InvalidIndex)?;
            total_orders = total_orders
                .checked_add(order_count)
                .ok_or(DeepXFinalizedSpotOrdersError::InvalidIndex)?;
            if order_count == 0 || total_orders > DEEPX_ABSOLUTE_MAX_PERP_ORDERS {
                return Err(DeepXFinalizedSpotOrdersError::InvalidIndex);
            }
            for _ in 0..order_count {
                let id = u64::decode(&mut input)
                    .map_err(|_| DeepXFinalizedSpotOrdersError::InvalidIndex)?;
                let is_buy = bool::decode(&mut input)
                    .map_err(|_| DeepXFinalizedSpotOrdersError::InvalidIndex)?;
                if id == 0 || active_by_id.insert(id, (pair, is_buy)).is_some() {
                    return Err(DeepXFinalizedSpotOrdersError::InvalidIndex);
                }
            }
        }
        if !input.is_empty() {
            return Err(DeepXFinalizedSpotOrdersError::InvalidIndex);
        }
    }
    let active_order_ids = active_by_id.keys().copied().collect::<Vec<_>>();
    let active_order_pairs = active_by_id
        .iter()
        .map(|(id, (pair, _))| (*id, *pair))
        .collect::<Vec<_>>();
    let mut orders = Vec::with_capacity(expected.len());
    for id in expected {
        let key = order_keys
            .get(&id)
            .ok_or(DeepXFinalizedSpotOrdersError::InvalidOrderEvidence)?;
        let encoded = changes
            .get(key)
            .and_then(Option::as_deref)
            .ok_or(DeepXFinalizedSpotOrdersError::InvalidOrderEvidence)?;
        let bytes = decode_storage_hex(encoded)
            .ok_or(DeepXFinalizedSpotOrdersError::InvalidOrderEvidence)?;
        let mut input = bytes.as_slice();
        let order = DeepXSpotOrderStorageEvidence::decode(&mut input)
            .map_err(|_| DeepXFinalizedSpotOrdersError::InvalidOrderEvidence)?;
        if !input.is_empty()
            || order.order_id != id
            || order.owner != subaccount
            || order.create_time == 0
        {
            return Err(DeepXFinalizedSpotOrdersError::InvalidOrderEvidence);
        }
        let indexed = active_by_id.get(&id);
        if order.status.is_active() != indexed.is_some()
            || indexed.is_some_and(|(pair, is_buy)| *pair != order.pair || *is_buy != order.is_buy)
        {
            return Err(DeepXFinalizedSpotOrdersError::InvalidOrderEvidence);
        }
        orders.push(order);
    }
    Ok((orders, active_order_ids, active_order_pairs))
}

fn decode_active_perp_order_index(
    encoded: Option<String>,
    subaccount: [u8; 20],
) -> Result<Vec<DeepXActivePerpOrderIndexEntry>, DeepXFinalizedPerpOrdersError> {
    let Some(encoded) = encoded else {
        return Ok(Vec::new());
    };
    let bytes = decode_storage_hex(&encoded).ok_or(DeepXFinalizedPerpOrdersError::InvalidIndex)?;
    let mut input = bytes.as_slice();
    let Compact(market_count) = Compact::<u32>::decode(&mut input)
        .map_err(|_| DeepXFinalizedPerpOrdersError::InvalidIndex)?;
    if market_count > DEEPX_ABSOLUTE_MAX_PERP_ORDER_MARKETS {
        return Err(DeepXFinalizedPerpOrdersError::InvalidIndex);
    }
    let mut previous_market = None;
    let mut order_ids = BTreeSet::new();
    let mut entries = Vec::new();
    for _ in 0..market_count {
        let market_id =
            u16::decode(&mut input).map_err(|_| DeepXFinalizedPerpOrdersError::InvalidIndex)?;
        if previous_market.is_some_and(|previous| previous >= market_id) {
            return Err(DeepXFinalizedPerpOrdersError::InvalidIndex);
        }
        previous_market = Some(market_id);
        let Compact(order_count) = Compact::<u32>::decode(&mut input)
            .map_err(|_| DeepXFinalizedPerpOrdersError::InvalidIndex)?;
        let total = u32::try_from(entries.len())
            .ok()
            .and_then(|length| length.checked_add(order_count))
            .ok_or(DeepXFinalizedPerpOrdersError::InvalidIndex)?;
        if order_count == 0 || total > DEEPX_ABSOLUTE_MAX_PERP_ORDERS {
            return Err(DeepXFinalizedPerpOrdersError::InvalidIndex);
        }
        entries.reserve(order_count as usize);
        for _ in 0..order_count {
            let entry = DeepXActivePerpOrderIndexEntry::decode(&mut input)
                .map_err(|_| DeepXFinalizedPerpOrdersError::InvalidIndex)?;
            if entry.address != subaccount
                || entry.market_id != market_id
                || entry.created_at == 0
                || !order_ids.insert(entry.order_id)
            {
                return Err(DeepXFinalizedPerpOrdersError::InvalidIndex);
            }
            entries.push(entry);
        }
    }
    if !input.is_empty() {
        return Err(DeepXFinalizedPerpOrdersError::InvalidIndex);
    }
    Ok(entries)
}

fn perp_order_matches_index(
    order: &DeepXPerpOrderStorageEvidence,
    index: &DeepXActivePerpOrderIndexEntry,
    subaccount: [u8; 20],
) -> bool {
    let expected_long = matches!(index.order_side, DeepXStoredOrderSide::Buy);
    order.order_id == index.order_id
        && order.owner == subaccount
        && order.market_id == index.market_id
        && order.is_long == expected_long
        && order.order_type == index.order_type
        && order.price == index.price
        && order.create_time == index.created_at
        && order.create_time != 0
        && order.leverage != 0
        && order.status.is_active()
        && order.size != 0
        && order.size_remain != 0
        && order.size_filled.checked_add(order.size_remain) == Some(order.size)
        && (!matches!(order.order_type, DeepXStoredPerpOrderType::Limit(_)) || order.price != 0)
}

/// Observes finalized perpetual positions and applies the runtime to a signing snapshot service.
///
/// Both `Subaccount.MaxPerpPositions` and the account's `PerpMarket.UserPerpPositions` value are
/// read from the same finalized checkpoint. This path performs no signing or mutation, but runtime
/// changes are applied to `service` so execution-owned callers retain the existing signing gate.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes `state_queryStorageAt`, the runtime
/// can be applied to `service`, and both storage values decode canonically with exact account
/// scope.
pub async fn observe_and_apply_finalized_perp_positions_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    service: &DeepXRuntimeSnapshotService,
    subaccount: [u8; 20],
) -> Result<DeepXFinalizedPerpPositionsEvidence, DeepXFinalizedPerpPositionsError> {
    validate_perp_position_capabilities(endpoints, capabilities)?;
    let applied =
        observe_and_apply_approved_finalized_runtime_snapshot(environment, endpoints, service)
            .await?;
    let (runtime_max_positions, positions) = observe_finalized_perp_positions_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        applied.checkpoint(),
        subaccount,
    )
    .await?;

    Ok(DeepXFinalizedPerpPositionsEvidence {
        checkpoint: applied.checkpoint(),
        runtime_identity: applied.identity().clone(),
        subaccount,
        runtime_max_positions,
        positions,
    })
}

/// Observes finalized perpetual positions under the read-only runtime approval boundary.
///
/// This path cannot construct or update a signing service. Both runtime storage reads remain pinned
/// to the exact finalized checkpoint whose runtime identity was approved for read-only use.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes `state_queryStorageAt`, the runtime
/// is approved for read-only use, and both storage values decode canonically with exact account
/// scope.
pub async fn observe_finalized_perp_positions_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    subaccount: [u8; 20],
) -> Result<DeepXFinalizedPerpPositionsEvidence, DeepXFinalizedPerpPositionsError> {
    validate_perp_position_capabilities(endpoints, capabilities)?;
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await
    .map_err(DeepXFinalizedPerpPositionsError::RuntimeObservation)?;
    let (runtime_max_positions, positions) = observe_finalized_perp_positions_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        observation.checkpoint(),
        subaccount,
    )
    .await?;

    Ok(DeepXFinalizedPerpPositionsEvidence {
        checkpoint: observation.checkpoint(),
        runtime_identity: observation.snapshot().identity().clone(),
        subaccount,
        runtime_max_positions,
        positions,
    })
}

/// Observes complete finalized raw account evidence and applies the runtime to a signing service.
///
/// Chain time, margin, lending, perpetual-position, and active-order reads request the checkpoint
/// whose runtime was applied to `service`. Chain time is checked against that block's timestamp
/// inherent. Other component reads still depend on the RPC endpoint honoring the supplied hash.
/// This function performs no signing, persistence mutation, or transaction submission.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes every required read method, the
/// runtime can be applied to `service`, and every component is complete and internally valid.
pub async fn observe_and_apply_finalized_account_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    service: &DeepXRuntimeSnapshotService,
    subaccount: [u8; 20],
) -> Result<DeepXFinalizedAccountEvidence, DeepXFinalizedAccountError> {
    validate_account_evidence_capabilities(endpoints, capabilities)?;
    let applied =
        observe_and_apply_approved_finalized_runtime_snapshot(environment, endpoints, service)
            .await?;
    let client = BlockchainHttpRpcClient::new(
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
        None,
        None,
    );
    let evidence = collect_finalized_account_evidence(
        &client,
        applied.checkpoint(),
        applied.identity(),
        subaccount,
    )
    .await;
    if let Err(e) = &evidence
        && matches!(
            e,
            DeepXFinalizedAccountError::EthCallBlockHashIgnored
                | DeepXFinalizedAccountError::BlockTimestampMismatch { .. }
                | DeepXFinalizedAccountError::StorageQueryTimestampMismatch { .. }
                | DeepXFinalizedAccountError::InvalidBlockTimestamp
                | DeepXFinalizedAccountError::InvalidStorageQueryTimestamp
        )
    {
        service
            .halt_for_inconsistent_evidence()
            .map_err(DeepXRuntimeSnapshotRefreshError::from)?;
    }
    evidence
}

/// Observes complete finalized raw account evidence under the read-only runtime boundary.
///
/// This path cannot construct or update a signing service. Every account read requests the
/// finalized checkpoint whose runtime identity was approved for read-only use. Timestamp storage
/// is checked against the block inherent. Historical runtime and EIP-1898 block selection probes
/// reject known endpoint defects; other component reads still depend on RPC hash semantics.
///
/// # Errors
///
/// Returns an error unless Watch capability evidence authorizes every required read method, the
/// runtime is approved for read-only use, and every component is complete and internally valid.
pub async fn observe_finalized_account_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    subaccount: [u8; 20],
) -> Result<DeepXFinalizedAccountEvidence, DeepXFinalizedAccountError> {
    validate_account_evidence_capabilities(endpoints, capabilities)?;
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await?;
    let client = BlockchainHttpRpcClient::new(
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
        None,
        None,
    );
    collect_finalized_account_evidence(
        &client,
        observation.checkpoint(),
        observation.snapshot().identity(),
        subaccount,
    )
    .await
}

/// Requests complete account evidence for every subaccount at one finalized runtime checkpoint.
///
/// The returned order matches `subaccounts`. An empty list, duplicate address, or incomplete
/// account read rejects the batch without returning partial evidence. Wallet ownership is not
/// established by this chain observation.
///
/// # Errors
///
/// Returns an error if capability or runtime evidence is invalid, the list is empty, an address is
/// duplicated, or any account component fails at the shared checkpoint.
pub async fn observe_finalized_accounts_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    subaccounts: &[[u8; 20]],
) -> Result<Vec<DeepXFinalizedAccountEvidence>, DeepXFinalizedAccountError> {
    validate_account_evidence_capabilities(endpoints, capabilities)?;
    if subaccounts.is_empty() {
        return Err(DeepXFinalizedAccountError::EmptySubaccounts);
    }
    let mut unique = BTreeSet::new();
    for subaccount in subaccounts {
        if !unique.insert(*subaccount) {
            return Err(DeepXFinalizedAccountError::DuplicateSubaccount(*subaccount));
        }
    }
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await?;
    let client = BlockchainHttpRpcClient::new(
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
        None,
        None,
    );
    let mut evidence = Vec::with_capacity(subaccounts.len());
    let mut shared_timestamp_ms = None;
    for subaccount in subaccounts {
        let account = collect_finalized_account_evidence(
            &client,
            observation.checkpoint(),
            observation.snapshot().identity(),
            *subaccount,
        )
        .await
        .map_err(|e| DeepXFinalizedAccountError::SubaccountObservation {
            subaccount: *subaccount,
            source: Box::new(e),
        })?;
        if let Some(expected_ms) = shared_timestamp_ms
            && expected_ms != account.timestamp_ms()
        {
            return Err(DeepXFinalizedAccountError::InconsistentTimestamp {
                subaccount: *subaccount,
                expected_ms,
                received_ms: account.timestamp_ms(),
            });
        }
        shared_timestamp_ms = Some(account.timestamp_ms());
        evidence.push(account);
    }
    Ok(evidence)
}

fn validate_account_evidence_capabilities(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
) -> Result<(), DeepXFinalizedAccountError> {
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || ![
            "state_call",
            "eth_call",
            "state_getStorage",
            "state_queryStorageAt",
            "chain_getBlock",
        ]
        .into_iter()
        .all(|method| watch_capabilities.methods().contains(method))
    {
        return Err(DeepXFinalizedAccountError::RpcCapabilitiesMismatch);
    }
    Ok(())
}

async fn collect_finalized_account_evidence(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: &ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
) -> Result<DeepXFinalizedAccountEvidence, DeepXFinalizedAccountError> {
    validate_eth_call_block_selection(client, checkpoint).await?;
    let timestamp_ms = observe_finalized_chain_time_with_client(client, checkpoint).await?;
    validate_block_timestamp_inherent(client, checkpoint, timestamp_ms).await?;
    let (initial, maintenance) =
        observe_finalized_margin_requirements_with_client(client, checkpoint, subaccount)
            .await
            .map_err(|source| DeepXFinalizedAccountError::Margin(Box::new(source)))?;
    let lending_positions = observe_finalized_lending_with_client(client, checkpoint, subaccount)
        .await
        .map_err(|source| DeepXFinalizedAccountError::Lending(Box::new(source)))?;
    let (runtime_max_positions, perp_positions) =
        observe_finalized_perp_positions_with_client(client, checkpoint, subaccount)
            .await
            .map_err(|source| DeepXFinalizedAccountError::PerpPositions(Box::new(source)))?;
    let perp_orders = observe_finalized_perp_orders_with_client(client, checkpoint, subaccount)
        .await
        .map_err(|source| DeepXFinalizedAccountError::PerpOrders(Box::new(source)))?;
    let mut spot_orders =
        observe_finalized_spot_orders_by_id_with_client(client, checkpoint, subaccount, &[])
            .await
            .map_err(|source| DeepXFinalizedAccountError::SpotOrders(Box::new(source)))?;
    if !spot_orders.1.is_empty() {
        spot_orders = observe_finalized_spot_orders_by_id_with_client(
            client,
            checkpoint,
            subaccount,
            &spot_orders.1,
        )
        .await
        .map_err(|source| DeepXFinalizedAccountError::SpotOrders(Box::new(source)))?;
    }
    let closing_timestamp_ms = observe_finalized_chain_time_with_client(client, checkpoint).await?;
    if closing_timestamp_ms != timestamp_ms {
        return Err(DeepXFinalizedAccountError::InconsistentTimestamp {
            subaccount,
            expected_ms: timestamp_ms,
            received_ms: closing_timestamp_ms,
        });
    }

    let runtime_identity = runtime_identity.clone();
    Ok(DeepXFinalizedAccountEvidence {
        checkpoint,
        runtime_identity: runtime_identity.clone(),
        subaccount,
        timestamp_ms,
        margin: DeepXFinalizedMarginEvidence {
            checkpoint,
            runtime_identity: runtime_identity.clone(),
            subaccount,
            initial,
            maintenance,
        },
        lending: DeepXFinalizedLendingEvidence {
            checkpoint,
            runtime_identity: runtime_identity.clone(),
            subaccount,
            positions: lending_positions,
        },
        perp_positions: DeepXFinalizedPerpPositionsEvidence {
            checkpoint,
            runtime_identity: runtime_identity.clone(),
            subaccount,
            runtime_max_positions,
            positions: perp_positions,
        },
        perp_orders: DeepXFinalizedPerpOrdersEvidence {
            checkpoint,
            runtime_identity: runtime_identity.clone(),
            subaccount,
            orders: perp_orders,
        },
        spot_orders: DeepXFinalizedSpotOrderStorageBatchEvidence {
            checkpoint,
            runtime_identity,
            subaccount,
            orders: spot_orders.0,
            active_order_ids: spot_orders.1,
            active_order_pairs: spot_orders.2,
        },
    })
}

async fn validate_finalized_market_read_checkpoint(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
) -> Result<u64, DeepXFinalizedAccountError> {
    let timestamp_ms = observe_finalized_chain_time_with_client(client, checkpoint).await?;
    validate_block_timestamp_inherent(client, checkpoint, timestamp_ms).await?;
    validate_eth_call_block_selection(client, checkpoint).await?;
    Ok(timestamp_ms)
}

async fn validate_finalized_storage_read_checkpoint(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
) -> Result<u64, DeepXFinalizedAccountError> {
    let timestamp_ms = observe_finalized_chain_time_with_client(client, checkpoint).await?;
    validate_block_timestamp_inherent(client, checkpoint, timestamp_ms).await?;
    let response: RpcNodeHttpResponse<String> = client
        .execute_rpc_response(json!({
            "jsonrpc": "2.0", "id": 1, "method": "state_getStorage",
            "params": [timestamp_now_storage_key(), format!("0x{}", "00".repeat(32))],
        }))
        .await
        .map_err(DeepXFinalizedAccountError::StorageRejectionRpc)?;
    if response.jsonrpc.as_deref() != Some("2.0")
        || response.id != Some(1)
        || response.result.is_some()
        || response.error.is_none()
    {
        return Err(DeepXFinalizedAccountError::StorageBlockHashIgnored);
    }
    Ok(timestamp_ms)
}

async fn validate_finalized_market_timestamp_unchanged(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
    timestamp_ms: u64,
) -> Result<(), DeepXFinalizedAccountError> {
    let received_ms = observe_finalized_chain_time_with_client(client, checkpoint).await?;
    if received_ms != timestamp_ms {
        return Err(DeepXFinalizedAccountError::BlockTimestampMismatch {
            block_ms: timestamp_ms,
            storage_ms: received_ms,
        });
    }
    Ok(())
}

async fn validate_eth_call_block_selection(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
) -> Result<(), DeepXFinalizedAccountError> {
    let call = json!({"to": DEEPX_ETH_CALL_PROBE_ADDRESS, "data": "0x"});
    let selected = format!("0x{:x}", checkpoint.block_number());
    let valid: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [call.clone(), selected],
        }))
        .await
        .map_err(DeepXFinalizedAccountError::EthCallProbeRpc)?;
    if valid != "0x" {
        return Err(DeepXFinalizedAccountError::InvalidEthCallProbeResult);
    }
    let impossible = "0xffffffff";
    let response: RpcNodeHttpResponse<String> = client
        .execute_rpc_response(json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [call, impossible],
        }))
        .await
        .map_err(DeepXFinalizedAccountError::EthCallRejectionRpc)?;
    if response.jsonrpc.as_deref() != Some("2.0")
        || response.id != Some(1)
        || response.result.is_some()
        || response.error.is_none()
    {
        return Err(DeepXFinalizedAccountError::EthCallBlockHashIgnored);
    }
    Ok(())
}

async fn validate_block_timestamp_inherent(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
    storage_ms: u64,
) -> Result<(), DeepXFinalizedAccountError> {
    let block: ObservedTimestampBlockResponse = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlock",
            "params": [format!("0x{}", hex::encode(checkpoint.block_hash()))],
        }))
        .await
        .map_err(DeepXFinalizedAccountError::BlockRpc)?;
    if decode_block_number(&block.block.header.number)
        .map_err(|_| DeepXFinalizedAccountError::InvalidBlockTimestamp)?
        != checkpoint.block_number()
    {
        return Err(DeepXFinalizedAccountError::InvalidBlockTimestamp);
    }
    let block_ms = decode_timestamp_set_inherent(&block.block.extrinsics)?;
    if block_ms != storage_ms {
        return Err(DeepXFinalizedAccountError::BlockTimestampMismatch {
            block_ms,
            storage_ms,
        });
    }
    let block_hash = format!("0x{}", hex::encode(checkpoint.block_hash()));
    let timestamp_key = timestamp_now_storage_key();
    let mut sets: Vec<ObservedStorageChangeSet> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_queryStorageAt",
            "params": [[timestamp_key.clone()], block_hash.clone()],
        }))
        .await
        .map_err(DeepXFinalizedAccountError::StorageQueryRpc)?;
    if sets.len() != 1 || sets[0].block != block_hash {
        return Err(DeepXFinalizedAccountError::InvalidStorageQueryTimestamp);
    }
    let changes = std::mem::take(&mut sets[0].changes);
    let [(key, Some(encoded))] = <[(String, Option<String>); 1]>::try_from(changes)
        .map_err(|_| DeepXFinalizedAccountError::InvalidStorageQueryTimestamp)?
    else {
        return Err(DeepXFinalizedAccountError::InvalidStorageQueryTimestamp);
    };
    if key != timestamp_key {
        return Err(DeepXFinalizedAccountError::InvalidStorageQueryTimestamp);
    }
    let query_ms = decode_timestamp_now(&encoded)
        .map_err(|_| DeepXFinalizedAccountError::InvalidStorageQueryTimestamp)?;
    if query_ms != block_ms {
        return Err(DeepXFinalizedAccountError::StorageQueryTimestampMismatch {
            block_ms,
            query_ms,
        });
    }
    Ok(())
}

fn decode_timestamp_set_inherent(extrinsics: &[String]) -> Result<u64, DeepXFinalizedAccountError> {
    let mut block_ms = None;
    for encoded in extrinsics {
        let bytes = hex::decode(
            encoded
                .strip_prefix("0x")
                .ok_or(DeepXFinalizedAccountError::InvalidBlockTimestamp)?,
        )
        .map_err(|_| DeepXFinalizedAccountError::InvalidBlockTimestamp)?;
        let mut input = bytes.as_slice();
        let length = Compact::<u32>::decode(&mut input)
            .map_err(|_| DeepXFinalizedAccountError::InvalidBlockTimestamp)?;
        if input.len() != length.0 as usize {
            return Err(DeepXFinalizedAccountError::InvalidBlockTimestamp);
        }
        if input.starts_with(&[4, 2, 0]) {
            let mut timestamp_input = &input[3..];
            let timestamp = Compact::<u64>::decode(&mut timestamp_input)
                .map_err(|_| DeepXFinalizedAccountError::InvalidBlockTimestamp)?;
            if timestamp.0 == 0
                || !timestamp_input.is_empty()
                || block_ms.replace(timestamp.0).is_some()
            {
                return Err(DeepXFinalizedAccountError::InvalidBlockTimestamp);
            }
        }
    }
    block_ms.ok_or(DeepXFinalizedAccountError::InvalidBlockTimestamp)
}

fn validate_perp_position_capabilities(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
) -> Result<(), DeepXFinalizedPerpPositionsError> {
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != endpoints.url_for(DeepXRpcRole::Watch)
        || !watch_capabilities
            .methods()
            .contains("state_queryStorageAt")
    {
        return Err(DeepXFinalizedPerpPositionsError::RpcCapabilitiesMismatch);
    }
    Ok(())
}

async fn observe_finalized_perp_positions_at(
    watch_url: &str,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
) -> Result<(u32, Vec<DeepXPerpPositionStorageEvidence>), DeepXFinalizedPerpPositionsError> {
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    observe_finalized_perp_positions_with_client(&client, checkpoint, subaccount).await
}

async fn observe_finalized_perp_positions_with_client(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
) -> Result<(u32, Vec<DeepXPerpPositionStorageEvidence>), DeepXFinalizedPerpPositionsError> {
    let block_hash = format!("0x{}", hex::encode(checkpoint.block_hash()));
    let maximum_key = subaccount_max_perp_positions_storage_key();
    let positions_key = user_perp_positions_storage_key(subaccount);
    let mut change_sets: Vec<ObservedStorageChangeSet> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_queryStorageAt",
            "params": [[maximum_key.clone(), positions_key.clone()], block_hash.clone()],
        }))
        .await
        .map_err(|source| DeepXFinalizedPerpPositionsError::Rpc {
            storage: "Subaccount.MaxPerpPositions and PerpMarket.UserPerpPositions",
            source,
        })?;
    if change_sets.len() != 1 || change_sets[0].block != block_hash {
        return Err(DeepXFinalizedPerpPositionsError::InvalidEvidence);
    }
    let changes = std::mem::take(&mut change_sets[0].changes);
    if changes.len() != 2 {
        return Err(DeepXFinalizedPerpPositionsError::InvalidEvidence);
    }
    let mut changes = changes.into_iter().collect::<BTreeMap<_, _>>();
    if changes.len() != 2 {
        return Err(DeepXFinalizedPerpPositionsError::InvalidEvidence);
    }
    let encoded_maximum = changes
        .remove(&maximum_key)
        .ok_or(DeepXFinalizedPerpPositionsError::InvalidEvidence)?;
    let encoded_positions = changes
        .remove(&positions_key)
        .ok_or(DeepXFinalizedPerpPositionsError::InvalidEvidence)?;
    let runtime_max_positions = decode_max_perp_positions(encoded_maximum)?;
    let positions = decode_perp_positions(encoded_positions, subaccount, runtime_max_positions)?;
    Ok((runtime_max_positions, positions))
}

fn decode_max_perp_positions(
    encoded: Option<String>,
) -> Result<u32, DeepXFinalizedPerpPositionsError> {
    let maximum = match encoded {
        None => DEEPX_DEFAULT_MAX_PERP_POSITIONS,
        Some(encoded) => {
            let bytes = decode_storage_hex(&encoded)
                .ok_or(DeepXFinalizedPerpPositionsError::InvalidMaximum)?;
            let mut input = bytes.as_slice();
            let value = u32::decode(&mut input)
                .map_err(|_| DeepXFinalizedPerpPositionsError::InvalidMaximum)?;
            if !input.is_empty() {
                return Err(DeepXFinalizedPerpPositionsError::InvalidMaximum);
            }
            value
        }
    };
    if maximum == 0 || maximum > DEEPX_ABSOLUTE_MAX_PERP_POSITIONS {
        return Err(DeepXFinalizedPerpPositionsError::InvalidMaximum);
    }
    Ok(maximum)
}

fn decode_perp_positions(
    encoded: Option<String>,
    subaccount: [u8; 20],
    maximum: u32,
) -> Result<Vec<DeepXPerpPositionStorageEvidence>, DeepXFinalizedPerpPositionsError> {
    let Some(encoded) = encoded else {
        return Ok(Vec::new());
    };
    let bytes =
        decode_storage_hex(&encoded).ok_or(DeepXFinalizedPerpPositionsError::InvalidEvidence)?;
    let mut input = bytes.as_slice();
    let Compact(length) = Compact::<u32>::decode(&mut input)
        .map_err(|_| DeepXFinalizedPerpPositionsError::InvalidEvidence)?;
    if length > maximum {
        return Err(DeepXFinalizedPerpPositionsError::InvalidEvidence);
    }
    let mut market_ids = BTreeSet::new();
    let mut positions = Vec::with_capacity(length as usize);
    for _ in 0..length {
        let position = DeepXPerpPositionStorageEvidence::decode(&mut input)
            .map_err(|_| DeepXFinalizedPerpPositionsError::InvalidEvidence)?;
        if position.owner != subaccount
            || position.base_asset_amount == 0
            || position.entry_price == 0
            || position.leverage == 0
            || position.last_settle_price == 0
            || !market_ids.insert(position.market_id)
        {
            return Err(DeepXFinalizedPerpPositionsError::InvalidEvidence);
        }
        positions.push(position);
    }
    if !input.is_empty() {
        return Err(DeepXFinalizedPerpPositionsError::InvalidEvidence);
    }
    positions.sort_unstable_by_key(DeepXPerpPositionStorageEvidence::market_id);
    Ok(positions)
}

fn decode_storage_hex(encoded: &str) -> Option<Vec<u8>> {
    encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())
}

fn storage_value_key(pallet: &[u8], item: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(32);
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(pallet));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(item));
    encoded
}

fn subaccount_max_perp_positions_storage_key() -> String {
    format!(
        "0x{}",
        hex::encode(storage_value_key(b"Subaccount", b"MaxPerpPositions")),
    )
}

fn perp_market_storage_key(market_id: u16) -> String {
    let mut encoded = storage_value_key(b"PerpMarket", b"PerpMarkets");
    encoded.extend_from_slice(&market_id.encode());
    format!("0x{}", hex::encode(encoded))
}

fn spot_market_storage_key(pair: [u8; 32]) -> String {
    let mut encoded = storage_value_key(b"SpotMarket", b"SpotMarkets");
    encoded.extend_from_slice(&pair);
    format!("0x{}", hex::encode(encoded))
}

fn user_perp_positions_storage_key(subaccount: [u8; 20]) -> String {
    let mut encoded = storage_value_key(b"PerpMarket", b"UserPerpPositions");
    encoded.extend_from_slice(&subaccount);
    format!("0x{}", hex::encode(encoded))
}

fn active_perp_orders_storage_key(subaccount: [u8; 20]) -> String {
    let mut encoded = storage_value_key(b"PerpMarket", b"ActivePerpOrdersFor");
    encoded.extend_from_slice(&subaccount);
    format!("0x{}", hex::encode(encoded))
}

fn active_spot_orders_storage_key(subaccount: [u8; 20]) -> String {
    let mut encoded = storage_value_key(b"SpotMarket", b"ActiveSpotOrdersFor");
    encoded.extend_from_slice(&subaccount);
    format!("0x{}", hex::encode(encoded))
}

fn spot_order_info_storage_key(subaccount: [u8; 20], order_id: u64) -> String {
    let mut encoded = storage_value_key(b"SpotMarket", b"OrderInfo");
    encoded.extend_from_slice(&subaccount);
    encoded.extend_from_slice(&order_id.encode());
    format!("0x{}", hex::encode(encoded))
}

fn perp_order_info_storage_key(subaccount: [u8; 20], order_id: u64) -> String {
    let mut encoded = storage_value_key(b"PerpMarket", b"OrderInfo");
    encoded.extend_from_slice(&subaccount);
    encoded.extend_from_slice(&order_id.encode());
    format!("0x{}", hex::encode(encoded))
}

async fn observe_finalized_lending_at(
    watch_url: &str,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
) -> Result<Vec<DeepXLendingPositionEvidence>, DeepXFinalizedLendingError> {
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    observe_finalized_lending_with_client(&client, checkpoint, subaccount).await
}

async fn observe_finalized_lending_with_client(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
) -> Result<Vec<DeepXLendingPositionEvidence>, DeepXFinalizedLendingError> {
    let block_hash = format!("0x{}", hex::encode(checkpoint.block_hash()));
    let block_number = format!("0x{:x}", checkpoint.block_number());
    let market_ids = (u8::MIN..=u8::MAX).collect::<Vec<_>>();
    let input = (vec![subaccount], market_ids).encode();
    let encoded: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_call",
            "params": [
                DEEPX_LENDING_POSITIONS_RUNTIME_API_METHOD,
                format!("0x{}", hex::encode(input)),
                block_hash.clone(),
            ],
        }))
        .await
        .map_err(DeepXFinalizedLendingError::PositionsRpc)?;
    let raw_positions = decode_lending_positions(&encoded, subaccount)?;
    stream::iter(raw_positions.into_iter().map(|raw| {
        observe_finalized_lending_asset(client, &block_hash, &block_number, subaccount, raw)
    }))
    .buffered(DEEPX_LENDING_ASSET_QUERY_CONCURRENCY)
    .try_collect()
    .await
}

#[derive(Debug, PartialEq, Eq)]
struct DeepXRawLendingPosition {
    market_id: u8,
    asset: Vec<u8>,
    scaled_deposit: u128,
    scaled_borrow: u128,
}

async fn observe_finalized_lending_asset(
    client: &BlockchainHttpRpcClient,
    block_hash: &str,
    block_number: &str,
    subaccount: [u8; 20],
    raw: DeepXRawLendingPosition,
) -> Result<DeepXLendingPositionEvidence, DeepXFinalizedLendingError> {
    let market_id = raw.market_id;
    let pool_input = (market_id, raw.asset.clone()).encode();
    let balance_input = (subaccount, market_id, raw.asset.clone()).encode();
    let call = |method: &'static str, input: Vec<u8>| async move {
        client
            .execute_rpc_call::<String>(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "state_call",
                "params": [
                    method,
                    format!("0x{}", hex::encode(input)),
                    block_hash,
                ],
            }))
            .await
            .map_err(|source| DeepXFinalizedLendingError::AssetRpc {
                method,
                market_id,
                source,
            })
    };
    let withdrawal = async {
        if raw.scaled_deposit == 0 {
            Ok(None)
        } else {
            observe_finalized_max_withdrawal(
                client,
                block_number,
                subaccount,
                market_id,
                &raw.asset,
            )
            .await
            .map(Some)
        }
    };
    let (pool, effective_deposit, effective_borrow, max_withdraw_amount) = tokio::join!(
        call(DEEPX_LENDING_POOL_RUNTIME_API_METHOD, pool_input),
        call(
            DEEPX_LENDING_DEPOSIT_RUNTIME_API_METHOD,
            balance_input.clone()
        ),
        call(DEEPX_LENDING_BORROW_RUNTIME_API_METHOD, balance_input),
        withdrawal,
    );
    let pool =
        decode_lending_asset_response::<Option<DeepXLendingPoolEvidence>>(&pool?, market_id)?
            .ok_or(DeepXFinalizedLendingError::InvalidAssetEvidence(market_id))?;
    let effective_deposit =
        decode_lending_asset_response::<Option<u128>>(&effective_deposit?, market_id)?;
    let effective_borrow =
        decode_lending_asset_response::<Option<u128>>(&effective_borrow?, market_id)?;
    let max_withdraw_amount = max_withdraw_amount?;

    validate_lending_asset_evidence(
        raw,
        pool,
        effective_deposit,
        effective_borrow,
        max_withdraw_amount,
    )
}

async fn observe_finalized_max_withdrawal(
    client: &BlockchainHttpRpcClient,
    block_number: &str,
    subaccount: [u8; 20],
    market_id: u8,
    asset: &[u8],
) -> Result<u128, DeepXFinalizedLendingError> {
    let call_data = maxWithdrawAmountForCall {
        subaccount: Address::from(subaccount),
        lendingMarket: market_id,
        asset: Bytes::copy_from_slice(asset),
    }
    .abi_encode();
    let encoded: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": DEEPX_LENDING_PRECOMPILE_ADDRESS.to_string(),
                    "data": format!("0x{}", hex::encode(call_data)),
                },
                block_number,
            ],
        }))
        .await
        .map_err(|source| DeepXFinalizedLendingError::WithdrawalRpc { market_id, source })?;
    decode_max_withdrawal(&encoded, market_id)
}

fn decode_max_withdrawal(encoded: &str, market_id: u8) -> Result<u128, DeepXFinalizedLendingError> {
    let bytes = encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())
        .filter(|value| value.len() == 32 && value[..16].iter().all(|byte| *byte == 0))
        .ok_or(DeepXFinalizedLendingError::InvalidWithdrawalEvidence(
            market_id,
        ))?;
    let raw = <[u8; 16]>::try_from(&bytes[16..])
        .map_err(|_| DeepXFinalizedLendingError::InvalidWithdrawalEvidence(market_id))?;
    Ok(u128::from_be_bytes(raw))
}

fn validate_lending_asset_evidence(
    raw: DeepXRawLendingPosition,
    pool: DeepXLendingPoolEvidence,
    effective_deposit: Option<u128>,
    effective_borrow: Option<u128>,
    max_withdraw_amount: Option<u128>,
) -> Result<DeepXLendingPositionEvidence, DeepXFinalizedLendingError> {
    let market_id = raw.market_id;
    if pool.market_id != market_id
        || pool.asset != raw.asset
        || (raw.scaled_deposit == 0) != effective_deposit.is_none()
        || (raw.scaled_borrow == 0) != effective_borrow.is_none()
        || (raw.scaled_deposit == 0) != max_withdraw_amount.is_none()
        || max_withdraw_amount.is_some_and(|amount| {
            amount > effective_deposit.unwrap_or_default() || amount > pool.available_liquidity
        })
    {
        return Err(DeepXFinalizedLendingError::InvalidAssetEvidence(market_id));
    }

    Ok(DeepXLendingPositionEvidence {
        market_id,
        asset: raw.asset,
        scaled_deposit: raw.scaled_deposit,
        scaled_borrow: raw.scaled_borrow,
        effective_deposit,
        effective_borrow,
        max_withdraw_amount,
        pool,
    })
}

fn decode_lending_positions(
    encoded: &str,
    subaccount: [u8; 20],
) -> Result<Vec<DeepXRawLendingPosition>, DeepXFinalizedLendingError> {
    let bytes = encoded
        .strip_prefix("0x")
        .ok_or(DeepXFinalizedLendingError::InvalidPositions)
        .and_then(|value| {
            hex::decode(value).map_err(|_| DeepXFinalizedLendingError::InvalidPositions)
        })?;
    let mut input = bytes.as_slice();
    let account_count = decode_bounded_compact_len(&mut input, 1)
        .ok_or(DeepXFinalizedLendingError::InvalidPositions)?;
    if account_count == 0 {
        if input.is_empty() {
            return Ok(Vec::new());
        }
        return Err(DeepXFinalizedLendingError::InvalidPositions);
    }
    let account =
        <[u8; 20]>::decode(&mut input).map_err(|_| DeepXFinalizedLendingError::InvalidPositions)?;
    if account != subaccount {
        return Err(DeepXFinalizedLendingError::InvalidPositions);
    }
    let market_count = decode_bounded_compact_len(&mut input, usize::from(u8::MAX) + 1)
        .filter(|count| *count > 0)
        .ok_or(DeepXFinalizedLendingError::InvalidPositions)?;
    let mut positions = Vec::new();
    let mut previous_market = None;
    for _ in 0..market_count {
        let market_id =
            u8::decode(&mut input).map_err(|_| DeepXFinalizedLendingError::InvalidPositions)?;
        if previous_market.is_some_and(|previous| market_id <= previous) {
            return Err(DeepXFinalizedLendingError::InvalidPositions);
        }
        previous_market = Some(market_id);
        let asset_count = decode_bounded_compact_len(&mut input, DEEPX_MAX_LENDING_POSITION_ASSETS)
            .filter(|count| *count > 0)
            .ok_or(DeepXFinalizedLendingError::InvalidPositions)?;
        let mut previous_asset: Option<Vec<u8>> = None;
        for _ in 0..asset_count {
            let asset = decode_bounded_bytes(&mut input, DEEPX_MAX_LENDING_ASSET_SYMBOL_BYTES)
                .filter(|value| !value.is_empty())
                .ok_or(DeepXFinalizedLendingError::InvalidPositions)?;
            if previous_asset
                .as_ref()
                .is_some_and(|previous| asset <= *previous)
            {
                return Err(DeepXFinalizedLendingError::InvalidPositions);
            }
            previous_asset = Some(asset.clone());
            let scaled_deposit = u128::decode(&mut input)
                .map_err(|_| DeepXFinalizedLendingError::InvalidPositions)?;
            let scaled_borrow = u128::decode(&mut input)
                .map_err(|_| DeepXFinalizedLendingError::InvalidPositions)?;
            if scaled_deposit == 0 && scaled_borrow == 0 {
                return Err(DeepXFinalizedLendingError::InvalidPositions);
            }
            positions.push(DeepXRawLendingPosition {
                market_id,
                asset,
                scaled_deposit,
                scaled_borrow,
            });
        }
    }
    if !input.is_empty() {
        return Err(DeepXFinalizedLendingError::InvalidPositions);
    }
    Ok(positions)
}

fn decode_bounded_compact_len(input: &mut &[u8], maximum: usize) -> Option<usize> {
    let Compact(value) = Compact::<u32>::decode(input).ok()?;
    let value = usize::try_from(value).ok()?;
    (value <= maximum).then_some(value)
}

fn decode_bounded_bytes(input: &mut &[u8], maximum: usize) -> Option<Vec<u8>> {
    let len = decode_bounded_compact_len(input, maximum)?;
    if input.len() < len {
        return None;
    }
    let (value, remaining) = input.split_at(len);
    *input = remaining;
    Some(value.to_vec())
}

fn decode_lending_asset_response<T: Decode>(
    encoded: &str,
    market_id: u8,
) -> Result<T, DeepXFinalizedLendingError> {
    let bytes = encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())
        .ok_or(DeepXFinalizedLendingError::InvalidAssetEvidence(market_id))?;
    let mut input = bytes.as_slice();
    let value = T::decode(&mut input)
        .map_err(|_| DeepXFinalizedLendingError::InvalidAssetEvidence(market_id))?;
    if !input.is_empty() {
        return Err(DeepXFinalizedLendingError::InvalidAssetEvidence(market_id));
    }
    Ok(value)
}

async fn observe_finalized_margin_requirements_at(
    watch_url: &str,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
) -> Result<(DeepXMarginRequirement, DeepXMarginRequirement), DeepXFinalizedMarginError> {
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    observe_finalized_margin_requirements_with_client(&client, checkpoint, subaccount).await
}

async fn observe_finalized_margin_requirements_with_client(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
    subaccount: [u8; 20],
) -> Result<(DeepXMarginRequirement, DeepXMarginRequirement), DeepXFinalizedMarginError> {
    observe_margin_requirements_at_hash(client, checkpoint.block_hash(), subaccount).await
}

async fn observe_margin_requirements_at_hash(
    client: &BlockchainHttpRpcClient,
    block_hash: [u8; 32],
    subaccount: [u8; 20],
) -> Result<(DeepXMarginRequirement, DeepXMarginRequirement), DeepXFinalizedMarginError> {
    let block_hash = format!("0x{}", hex::encode(block_hash));
    let observe = |direction: DeepXMarginDirection| {
        let input = (vec![subaccount], direction.as_runtime_u8()).encode();
        client.execute_rpc_call::<String>(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_call",
            "params": [
                DEEPX_MARGIN_RUNTIME_API_METHOD,
                format!("0x{}", hex::encode(input)),
                block_hash,
            ],
        }))
    };
    let (initial, maintenance) = tokio::join!(
        observe(DeepXMarginDirection::Initial),
        observe(DeepXMarginDirection::Maintenance),
    );
    let initial = initial.map_err(|source| DeepXFinalizedMarginError::Rpc {
        direction: DeepXMarginDirection::Initial,
        source,
    })?;
    let maintenance = maintenance.map_err(|source| DeepXFinalizedMarginError::Rpc {
        direction: DeepXMarginDirection::Maintenance,
        source,
    })?;
    Ok((
        decode_margin_requirement(&initial, subaccount, DeepXMarginDirection::Initial)?,
        decode_margin_requirement(&maintenance, subaccount, DeepXMarginDirection::Maintenance)?,
    ))
}

fn decode_margin_requirement(
    encoded: &str,
    subaccount: [u8; 20],
    direction: DeepXMarginDirection,
) -> Result<DeepXMarginRequirement, DeepXFinalizedMarginError> {
    let bytes = encoded
        .strip_prefix("0x")
        .ok_or(DeepXFinalizedMarginError::InvalidEvidence(direction))
        .and_then(|value| {
            hex::decode(value).map_err(|_| DeepXFinalizedMarginError::InvalidEvidence(direction))
        })?;
    let mut input = bytes.as_slice();
    let values = BTreeMap::<[u8; 20], (u128, u128)>::decode(&mut input)
        .map_err(|_| DeepXFinalizedMarginError::InvalidEvidence(direction))?;
    if !input.is_empty() || values.len() != 1 {
        return Err(DeepXFinalizedMarginError::InvalidEvidence(direction));
    }
    let (total_collateral, total_margin_required) = values
        .get(&subaccount)
        .copied()
        .ok_or(DeepXFinalizedMarginError::InvalidEvidence(direction))?;
    Ok(DeepXMarginRequirement {
        total_collateral,
        total_margin_required,
    })
}

fn timestamp_now_storage_key() -> String {
    let mut encoded = Vec::with_capacity(32);
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(b"Timestamp"));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(b"Now"));
    format!("0x{}", hex::encode(encoded))
}

async fn observe_finalized_chain_time_with_client(
    client: &BlockchainHttpRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
) -> Result<u64, DeepXFinalizedChainTimeError> {
    let encoded: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [
                timestamp_now_storage_key(),
                format!("0x{}", hex::encode(checkpoint.block_hash())),
            ],
        }))
        .await
        .map_err(DeepXFinalizedChainTimeError::Rpc)?;
    decode_timestamp_now(&encoded)
}

fn decode_timestamp_now(encoded: &str) -> Result<u64, DeepXFinalizedChainTimeError> {
    let bytes = encoded
        .strip_prefix("0x")
        .ok_or(DeepXFinalizedChainTimeError::InvalidStorage)
        .and_then(|value| {
            hex::decode(value).map_err(|_| DeepXFinalizedChainTimeError::InvalidStorage)
        })?;
    let mut input = bytes.as_slice();
    let timestamp_ms =
        u64::decode(&mut input).map_err(|_| DeepXFinalizedChainTimeError::InvalidStorage)?;
    if timestamp_ms == 0 || !input.is_empty() {
        return Err(DeepXFinalizedChainTimeError::InvalidStorage);
    }
    Ok(timestamp_ms)
}

async fn observe_approved_finalized_read_only_runtime_snapshot_at(
    environment: &DeepXEnvironment,
    url: String,
) -> Result<DeepXObservedReadOnlyRuntimeSnapshot, DeepXRuntimeSnapshotObservationError> {
    let client = BlockchainHttpRpcClient::new(url.clone(), None, None);
    let encoded_genesis: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [0],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "chain_getBlockHash",
            source,
        })?;
    let genesis_hash = decode_prefixed_hash(&encoded_genesis, "genesis hash")?;
    let checkpoint = observe_finalized_checkpoint_at(url).await?;
    let (version, metadata) = observe_runtime_metadata_at(&client, checkpoint.block_hash()).await?;
    let snapshot = DeepXReadOnlyRuntimeSnapshot::observed_testnet(
        environment,
        genesis_hash,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )?;
    Ok(DeepXObservedReadOnlyRuntimeSnapshot {
        snapshot,
        checkpoint,
    })
}

async fn observe_runtime_metadata_at(
    client: &BlockchainHttpRpcClient,
    block_hash: [u8; 32],
) -> Result<(ObservedRuntimeVersion, Vec<u8>), DeepXRuntimeSnapshotObservationError> {
    let block_hash = format!("0x{}", hex::encode(block_hash));
    let version = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getRuntimeVersion",
            "params": [block_hash],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "state_getRuntimeVersion",
            source,
        })?;
    let encoded_metadata: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getMetadata",
            "params": [block_hash],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "state_getMetadata",
            source,
        })?;
    let metadata = encoded_metadata
        .strip_prefix("0x")
        .filter(|value| !value.is_empty())
        .and_then(|value| hex::decode(value).ok())
        .ok_or(DeepXRuntimeSnapshotObservationError::InvalidMetadata)?;
    Ok((version, metadata))
}

async fn observe_finalized_checkpoint_at(
    url: String,
) -> Result<DeepXFinalizedCheckpoint, DeepXRuntimeSnapshotObservationError> {
    let client = BlockchainHttpRpcClient::new(url, None, None);
    let encoded_finalized_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getFinalizedHead",
            "params": [],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "chain_getFinalizedHead",
            source,
        })?;
    let finalized_hash = decode_prefixed_hash(&encoded_finalized_hash, "finalized hash")?;
    let finalized_header: ObservedBlockHeader = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getHeader",
            "params": [encoded_finalized_hash],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "chain_getHeader",
            source,
        })?;
    Ok(DeepXFinalizedCheckpoint {
        block_hash: finalized_hash,
        block_number: decode_block_number(&finalized_header.number)?,
    })
}

fn decode_prefixed_hash(
    encoded_hash: &str,
    name: &'static str,
) -> Result<[u8; 32], DeepXRuntimeSnapshotObservationError> {
    encoded_hash
        .strip_prefix("0x")
        .ok_or(DeepXRuntimeSnapshotObservationError::InvalidHash(name))
        .and_then(|value| {
            hex::decode_array::<32>(value)
                .map_err(|_| DeepXRuntimeSnapshotObservationError::InvalidHash(name))
        })
}

fn decode_block_number(encoded_number: &str) -> Result<u32, DeepXRuntimeSnapshotObservationError> {
    let value = encoded_number
        .strip_prefix("0x")
        .filter(|value| !value.is_empty());
    value
        .and_then(|value| u32::from_str_radix(value, 16).ok())
        .ok_or_else(|| {
            DeepXRuntimeSnapshotObservationError::InvalidBlockNumber(encoded_number.to_string())
        })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{Json, Router, routing::post};
    use nautilus_core::UnixNanos;
    use nautilus_model::{enums::AccountType, identifiers::AccountId, types::Money};
    use parity_scale_codec::DecodeAll;
    use rstest::rstest;
    use serde_json::Value;
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        DeepXEnvironment, DeepXFlatAccountBalanceError, common::consts::DEEPX_TESTNET_GENESIS_HASH,
        map_finalized_flat_account_balances,
    };

    const DEEPX_SPEC_376_FINALIZED_HASH: &str =
        "0xe38016d218d6748473ffae7165aabee28aba3418306e64f6c781ea41b5d5a38c";
    const DEEPX_SPEC_376_VERSION: u32 = 376;

    const GENESIS_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
            genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/\
         genesis_hash.json"
    );
    const FINALIZED_HEAD_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
            genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/\
         finalized_head.json"
    );
    const SPEC_376_METADATA_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
            genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/\
         metadata.json"
    );
    const SPOT_MARKET_STORAGE_SPEC_376_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
            genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/\
            spot_market_eth_usdc.json"
    );
    const FINALIZED_HASH: &str = DEEPX_SPEC_376_FINALIZED_HASH;
    const FINALIZED_BLOCK_NUMBER: u32 = 19_592_700;
    const FINALIZED_BLOCK_NUMBER_HEX: &str = "0x12af5fc";

    #[rstest]
    fn timestamp_inherent_decodes_exact_milliseconds() {
        let captured = "0x280402000bf4a65c1ba101".to_string();
        assert_eq!(
            decode_timestamp_set_inherent(std::slice::from_ref(&captured)).unwrap(),
            1_791_460_419_316,
        );
        assert!(matches!(
            decode_timestamp_set_inherent(&[captured.clone(), captured]),
            Err(DeepXFinalizedAccountError::InvalidBlockTimestamp),
        ));
        assert!(matches!(
            decode_timestamp_set_inherent(&["0x280402000bf4a65c1ba1".to_string()]),
            Err(DeepXFinalizedAccountError::InvalidBlockTimestamp),
        ));
    }

    #[derive(Encode)]
    struct EncodedLendingPosition {
        positions: BTreeMap<Vec<u8>, (u128, u128)>,
    }

    fn lending_pool(market_id: u8, asset: &[u8]) -> DeepXLendingPoolEvidence {
        DeepXLendingPoolEvidence {
            market_id,
            asset: asset.to_vec(),
            decimal: 6,
            cash_balance: 2_000_000,
            locked_liquidity: 100_000,
            available_liquidity: 1_900_000,
            total_scaled_deposits: 1_500_000,
            total_scaled_borrows: 500_000,
            total_effective_deposits: 1_600_000,
            total_effective_borrows: 550_000,
            deposit_index: 1_010_000_000_000_000_000,
            borrow_index: 1_020_000_000_000_000_000,
            protocol_reserve: 10_000,
            bad_debt: 0,
            accounting_dust: 0,
            internal_transfer_dust: 0,
            status: DeepXLendingPoolStatus::Active,
            apr_borrow: 50_000_000_000_000_000,
            apr_lend: 20_000_000_000_000_000,
        }
    }

    fn flat_account_evidence() -> DeepXFinalizedAccountEvidence {
        let checkpoint = DeepXFinalizedCheckpoint {
            block_hash: [0x22; 32],
            block_number: 42,
        };
        let runtime_identity = ApprovedRuntimeIdentity {
            environment: DeepXEnvironment::Testnet,
            genesis_hash: [0x11; 32],
            metadata_sha256: [0x33; 32],
            spec_version: 371,
            transaction_version: 1,
            signed_extensions: Vec::new(),
        };
        let subaccount = [0x44; 20];
        let lending_position = DeepXLendingPositionEvidence {
            market_id: 1,
            asset: b"usdc".to_vec(),
            scaled_deposit: 1_200_000,
            scaled_borrow: 0,
            effective_deposit: Some(1_234_567),
            effective_borrow: None,
            max_withdraw_amount: Some(1_234_567),
            pool: lending_pool(1, b"usdc"),
        };
        DeepXFinalizedAccountEvidence {
            checkpoint,
            runtime_identity: runtime_identity.clone(),
            subaccount,
            timestamp_ms: 1_700_000_000_123,
            margin: DeepXFinalizedMarginEvidence {
                checkpoint,
                runtime_identity: runtime_identity.clone(),
                subaccount,
                initial: DeepXMarginRequirement {
                    total_collateral: 1_234_567,
                    total_margin_required: 0,
                },
                maintenance: DeepXMarginRequirement {
                    total_collateral: 1_234_567,
                    total_margin_required: 0,
                },
            },
            lending: DeepXFinalizedLendingEvidence {
                checkpoint,
                runtime_identity: runtime_identity.clone(),
                subaccount,
                positions: vec![lending_position],
            },
            perp_positions: DeepXFinalizedPerpPositionsEvidence {
                checkpoint,
                runtime_identity: runtime_identity.clone(),
                subaccount,
                runtime_max_positions: 8,
                positions: Vec::new(),
            },
            perp_orders: DeepXFinalizedPerpOrdersEvidence {
                checkpoint,
                runtime_identity: runtime_identity.clone(),
                subaccount,
                orders: Vec::new(),
            },
            spot_orders: DeepXFinalizedSpotOrderStorageBatchEvidence {
                checkpoint,
                runtime_identity,
                subaccount,
                orders: Vec::new(),
                active_order_ids: Vec::new(),
                active_order_pairs: Vec::new(),
            },
        }
    }

    #[test]
    fn finalized_flat_account_maps_only_fully_withdrawable_deposits() {
        let evidence = flat_account_evidence();

        let mapped = map_finalized_flat_account_balances(&evidence).unwrap();

        assert_eq!(mapped.checkpoint(), evidence.checkpoint());
        assert_eq!(mapped.runtime_identity(), evidence.runtime_identity());
        assert_eq!(mapped.subaccount(), evidence.subaccount());
        assert_eq!(mapped.timestamp_ms(), evidence.timestamp_ms());
        assert_eq!(mapped.balances().len(), 1);
        let balance = mapped.balances()[0];
        assert_eq!(balance.currency.code.as_str(), "USDC");
        assert_eq!(balance.total, balance.free);
        assert_eq!(balance.locked, Money::zero(balance.currency));
        assert_eq!(
            balance.total,
            Money::from_decimal(rust_decimal::Decimal::new(1_234_567, 6), balance.currency,)
                .unwrap(),
        );

        let state = mapped
            .into_account_state(
                AccountId::from("DEEPX-001"),
                AccountType::Margin,
                UnixNanos::from(1_700_000_000_999_000_000),
            )
            .unwrap();
        assert_eq!(state.account_id, AccountId::from("DEEPX-001"));
        assert_eq!(state.account_type, AccountType::Margin);
        assert_eq!(state.balances, vec![balance]);
        assert!(state.margins.is_empty());
        assert!(state.is_reported);
        assert_eq!(state.ts_event, UnixNanos::from(1_700_000_000_123_000_000));
        assert_eq!(state.ts_init, UnixNanos::from(1_700_000_000_999_000_000));
        assert_eq!(state.base_currency, None);
    }

    #[test]
    fn finalized_flat_account_rejects_exposure_liability_and_restrictions() {
        let mut evidence = flat_account_evidence();
        evidence.margin.initial.total_margin_required = 1;
        assert!(matches!(
            map_finalized_flat_account_balances(&evidence),
            Err(DeepXFlatAccountBalanceError::MarginRequired),
        ));

        let mut evidence = flat_account_evidence();
        evidence.perp_positions.positions = vec![perp_position(evidence.subaccount, 3)];
        assert!(matches!(
            map_finalized_flat_account_balances(&evidence),
            Err(DeepXFlatAccountBalanceError::PerpetualPositionOpen),
        ));

        let mut evidence = flat_account_evidence();
        evidence.perp_orders.orders = vec![perp_order(evidence.subaccount, 3, 42)];
        assert!(matches!(
            map_finalized_flat_account_balances(&evidence),
            Err(DeepXFlatAccountBalanceError::PerpetualOrderActive),
        ));

        let mut evidence = flat_account_evidence();
        evidence.spot_orders.active_order_ids = vec![42];
        assert!(matches!(
            map_finalized_flat_account_balances(&evidence),
            Err(DeepXFlatAccountBalanceError::SpotOrderActive),
        ));

        let mut evidence = flat_account_evidence();
        evidence.lending.positions[0].scaled_borrow = 1;
        evidence.lending.positions[0].effective_borrow = Some(1);
        assert!(matches!(
            map_finalized_flat_account_balances(&evidence),
            Err(DeepXFlatAccountBalanceError::BorrowLiability { .. }),
        ));

        let mut evidence = flat_account_evidence();
        evidence.lending.positions[0].max_withdraw_amount = Some(1_234_566);
        assert!(matches!(
            map_finalized_flat_account_balances(&evidence),
            Err(DeepXFlatAccountBalanceError::WithdrawalRestricted { .. }),
        ));
    }

    #[test]
    fn finalized_flat_account_rejects_unrepresentable_asset_evidence() {
        let mut evidence = flat_account_evidence();
        evidence.lending.positions.clear();
        assert!(matches!(
            map_finalized_flat_account_balances(&evidence),
            Err(DeepXFlatAccountBalanceError::Empty),
        ));

        let mut evidence = flat_account_evidence();
        evidence
            .lending
            .positions
            .push(evidence.lending.positions[0].clone());
        assert!(matches!(
            map_finalized_flat_account_balances(&evidence),
            Err(DeepXFlatAccountBalanceError::DuplicateAsset(asset)) if asset == "USDC",
        ));

        let mut evidence = flat_account_evidence();
        evidence.lending.positions[0].asset = vec![0xff];
        assert!(matches!(
            map_finalized_flat_account_balances(&evidence),
            Err(DeepXFlatAccountBalanceError::InvalidAsset),
        ));

        let mut evidence = flat_account_evidence();
        evidence.lending.positions[0].effective_deposit = Some(1);
        evidence.lending.positions[0].max_withdraw_amount = Some(1);
        evidence.lending.positions[0].pool.decimal = 18;
        assert!(matches!(
            map_finalized_flat_account_balances(&evidence),
            Err(DeepXFlatAccountBalanceError::InexactAmount { .. }),
        ));

        let mut evidence = flat_account_evidence();
        evidence.timestamp_ms = u64::MAX;
        let mapped = map_finalized_flat_account_balances(&evidence).unwrap();
        assert!(matches!(
            mapped.into_account_state(
                AccountId::from("DEEPX-001"),
                AccountType::Margin,
                UnixNanos::default(),
            ),
            Err(DeepXFlatAccountBalanceError::TimestampOutOfRange(u64::MAX)),
        ));
    }

    fn perp_position(subaccount: [u8; 20], market_id: u16) -> DeepXPerpPositionStorageEvidence {
        DeepXPerpPositionStorageEvidence {
            market_id,
            is_long: true,
            base_asset_amount: 250_000_000_000_000_000,
            entry_price: 2_500_000_000_000_000_000,
            leverage: 12_500,
            last_funding_rate: -1_000,
            version: 7,
            realized_pnl: 500,
            funding_payment: -25,
            owner: subaccount,
            take_profit: Some(3_000_000_000_000_000_000),
            stop_loss: None,
            last_settle_price: 2_550_000_000_000_000_000,
        }
    }

    fn perp_order(
        subaccount: [u8; 20],
        market_id: u16,
        order_id: u64,
    ) -> DeepXPerpOrderStorageEvidence {
        DeepXPerpOrderStorageEvidence {
            order_id,
            owner: subaccount,
            market_id,
            is_long: true,
            size: 250_000_000_000_000_000,
            price: 2_500_000_000,
            order_type: DeepXStoredPerpOrderType::Limit(DeepXStoredTimeInForce::Gtc),
            create_time: 1_700_000_000_000,
            leverage: 12_500,
            status: DeepXStoredOrderStatus::Open,
            size_filled: 50_000_000_000_000_000,
            size_remain: 200_000_000_000_000_000,
            take_profit: Some(3_000_000_000),
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXStoredPostOnlyParam::MustPostOnly,
        }
    }

    fn spot_order(
        subaccount: [u8; 20],
        pair: [u8; 32],
        order_id: u64,
    ) -> DeepXSpotOrderStorageEvidence {
        DeepXSpotOrderStorageEvidence {
            order_id,
            owner: subaccount,
            pair,
            price: Some([0x11; 32]),
            quote_amount: [0x33; 32],
            base_amount: [0x44; 32],
            create_time: 42,
            status: DeepXStoredOrderStatus::Open,
            order_type: DeepXStoredSpotOrderType::Limit(DeepXStoredTimeInForce::Gtc),
            post_only: DeepXStoredPostOnlyParam::None,
            reduce_only: false,
            is_buy: true,
        }
    }

    fn active_perp_order_index_entry(
        order: &DeepXPerpOrderStorageEvidence,
    ) -> DeepXActivePerpOrderIndexEntry {
        DeepXActivePerpOrderIndexEntry {
            address: order.owner,
            market_id: order.market_id,
            order_side: if order.is_long {
                DeepXStoredOrderSide::Buy
            } else {
                DeepXStoredOrderSide::Sell
            },
            order_type: order.order_type.clone(),
            order_id: order.order_id,
            price: order.price,
            created_at: order.create_time,
        }
    }

    #[test]
    fn finalized_perp_order_storage_keys_use_identity_encoding() {
        let subaccount = [0x11; 20];
        let index = active_perp_orders_storage_key(subaccount);
        let order = perp_order_info_storage_key(subaccount, 0x0102_0304_0506_0708);

        assert_eq!(index.len(), 106);
        assert_eq!(order.len(), 122);
        assert!(index.ends_with(&hex::encode(subaccount)));
        assert!(order.ends_with("0807060504030201"));
        assert_ne!(order, perp_order_info_storage_key(subaccount, 2));
        assert_eq!(DeepXStoredOrderSide::Buy.encode(), [1]);
        assert_eq!(DeepXStoredOrderSide::Sell.encode(), [0]);
    }

    #[test]
    fn finalized_perp_order_index_decoder_is_bounded_scoped_and_canonical() {
        let subaccount = [0x11; 20];
        let first = perp_order(subaccount, 2, 10);
        let second = perp_order(subaccount, 3, 11);
        let encoded = BTreeMap::from([
            (2_u16, vec![active_perp_order_index_entry(&first)]),
            (3_u16, vec![active_perp_order_index_entry(&second)]),
        ])
        .encode();

        let decoded =
            decode_active_perp_order_index(Some(format!("0x{}", hex::encode(encoded))), subaccount)
                .unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].market_id, 2);
        assert_eq!(decoded[1].order_id, 11);
        assert_eq!(
            decode_active_perp_order_index(None, subaccount).unwrap(),
            vec![]
        );
        assert!(perp_order_matches_index(
            &first,
            &active_perp_order_index_entry(&first),
            subaccount,
        ));
        let first_generated = perp_order(subaccount, 2, 0);
        let encoded =
            BTreeMap::from([(2_u16, vec![active_perp_order_index_entry(&first_generated)])])
                .encode();
        let decoded =
            decode_active_perp_order_index(Some(format!("0x{}", hex::encode(encoded))), subaccount)
                .unwrap();
        assert_eq!(decoded[0].order_id, 0);
        assert!(perp_order_matches_index(
            &first_generated,
            &decoded[0],
            subaccount,
        ));
    }

    #[test]
    fn finalized_perp_order_evidence_rejects_contradictions() {
        let subaccount = [0x11; 20];
        let order = perp_order(subaccount, 2, 10);
        let mut foreign = active_perp_order_index_entry(&order);
        foreign.address = [0x22; 20];
        let mut trailing =
            BTreeMap::from([(2_u16, vec![active_perp_order_index_entry(&order)])]).encode();
        trailing.push(0);
        for encoded in [
            BTreeMap::from([(2_u16, vec![foreign])]).encode(),
            trailing,
            Compact(DEEPX_ABSOLUTE_MAX_PERP_ORDER_MARKETS + 1).encode(),
        ] {
            assert!(matches!(
                decode_active_perp_order_index(
                    Some(format!("0x{}", hex::encode(encoded))),
                    subaccount,
                ),
                Err(DeepXFinalizedPerpOrdersError::InvalidIndex),
            ));
        }

        let index = active_perp_order_index_entry(&order);
        let mut invalid = order.clone();
        invalid.status = DeepXStoredOrderStatus::Filled;
        assert!(!perp_order_matches_index(&invalid, &index, subaccount));
        invalid = order.clone();
        invalid.size_remain -= 1;
        assert!(!perp_order_matches_index(&invalid, &index, subaccount));
        invalid = order;
        invalid.owner = [0x22; 20];
        assert!(!perp_order_matches_index(&invalid, &index, subaccount));
    }

    #[test]
    fn finalized_perp_position_storage_keys_use_identity_map_encoding() {
        let subaccount = [0x11; 20];
        let maximum = subaccount_max_perp_positions_storage_key();
        let positions = user_perp_positions_storage_key(subaccount);

        assert_eq!(maximum.len(), 66);
        assert_eq!(positions.len(), 106);
        assert!(positions.ends_with(&hex::encode(subaccount)));
        assert_ne!(positions, user_perp_positions_storage_key([0x22; 20]));
    }

    #[test]
    fn finalized_perp_position_decoder_is_bounded_scoped_and_canonical() {
        let subaccount = [0x11; 20];
        let first = perp_position(subaccount, 3);
        let second = perp_position(subaccount, 2);
        let encoded = format!("0x{}", hex::encode(vec![first, second].encode()));

        let positions = decode_perp_positions(Some(encoded), subaccount, 8).unwrap();

        assert_eq!(
            positions
                .iter()
                .map(DeepXPerpPositionStorageEvidence::market_id)
                .collect::<Vec<_>>(),
            vec![2, 3],
        );
        assert_eq!(decode_perp_positions(None, subaccount, 8).unwrap(), vec![]);
        assert_eq!(decode_max_perp_positions(None).unwrap(), 8);
        assert_eq!(
            decode_max_perp_positions(Some(format!("0x{}", hex::encode(12_u32.encode())))).unwrap(),
            12,
        );
    }

    #[test]
    fn finalized_perp_position_decoder_rejects_contradictory_evidence() {
        let subaccount = [0x11; 20];
        let valid = perp_position(subaccount, 2);
        let mut foreign = valid.clone();
        foreign.owner = [0x22; 20];
        let mut zero_amount = valid.clone();
        zero_amount.base_asset_amount = 0;

        for positions in [vec![valid.clone(), valid], vec![foreign], vec![zero_amount]] {
            assert!(matches!(
                decode_perp_positions(
                    Some(format!("0x{}", hex::encode(positions.encode()))),
                    subaccount,
                    8,
                ),
                Err(DeepXFinalizedPerpPositionsError::InvalidEvidence),
            ));
        }
        let mut trailing = vec![perp_position(subaccount, 2)].encode();
        trailing.push(0);
        assert!(matches!(
            decode_perp_positions(Some(format!("0x{}", hex::encode(trailing))), subaccount, 8,),
            Err(DeepXFinalizedPerpPositionsError::InvalidEvidence),
        ));
        assert!(matches!(
            decode_perp_positions(
                Some(format!(
                    "0x{}",
                    hex::encode(vec![perp_position(subaccount, 2)].encode()),
                )),
                subaccount,
                0,
            ),
            Err(DeepXFinalizedPerpPositionsError::InvalidEvidence),
        ));
        for invalid in [Some("0x".to_string()), Some("0x0000000000".to_string())] {
            assert!(matches!(
                decode_max_perp_positions(invalid),
                Err(DeepXFinalizedPerpPositionsError::InvalidMaximum),
            ));
        }
    }

    #[tokio::test]
    async fn finalized_margin_observation_pins_both_directions_to_one_checkpoint() {
        let subaccount = [0x11; 20];
        let checkpoint = DeepXFinalizedCheckpoint {
            block_hash: [0x22; 32],
            block_number: 42,
        };
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| async move {
                assert_eq!(request["method"], "state_call");
                assert_eq!(request["params"][0], DEEPX_MARGIN_RUNTIME_API_METHOD);
                assert_eq!(
                    request["params"][2],
                    format!("0x{}", hex::encode(checkpoint.block_hash()))
                );
                let encoded = request["params"][1].as_str().unwrap();
                let bytes = hex::decode(encoded.trim_start_matches("0x")).unwrap();
                let (accounts, direction) =
                    <(Vec<[u8; 20]>, u8)>::decode(&mut bytes.as_slice()).unwrap();
                assert_eq!(accounts, vec![subaccount]);
                let values = match direction {
                    0 => (1_000_000_u128, 250_000_u128),
                    1 => (900_000_u128, 100_000_u128),
                    _ => panic!("unexpected margin direction"),
                };
                let result = BTreeMap::from([(subaccount, values)]).encode();
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": format!("0x{}", hex::encode(result)),
                }))
            }),
        );
        let url = spawn_server(router).await;

        let (initial, maintenance) =
            observe_finalized_margin_requirements_at(&url, checkpoint, subaccount)
                .await
                .unwrap();

        assert_eq!(initial.total_collateral(), 1_000_000);
        assert_eq!(initial.total_margin_required(), 250_000);
        assert_eq!(maintenance.total_collateral(), 900_000);
        assert_eq!(maintenance.total_margin_required(), 100_000);
    }

    #[tokio::test]
    async fn current_margin_rejects_ignored_hash_and_reorganization() {
        const BLOCK_HASH: &str =
            "0x2222222222222222222222222222222222222222222222222222222222222222";
        let subaccount = [0x11; 20];
        for (ignored_hash, reorganized) in [(false, false), (true, false), (false, true)] {
            let calls = Arc::new(AtomicUsize::new(0));
            let router = Router::new().route(
                "/",
                post({
                    let calls = Arc::clone(&calls);
                    move |Json(request): Json<Value>| {
                        let calls = Arc::clone(&calls);
                        async move {
                            let response = match request["method"].as_str().unwrap() {
                                "chain_getBlockHash" => {
                                    let hash = if reorganized && request["params"] == json!([42]) {
                                        format!("0x{}", "33".repeat(32))
                                    } else {
                                        BLOCK_HASH.to_string()
                                    };
                                    json!({"jsonrpc": "2.0", "id": 1, "result": hash})
                                }
                                "chain_getHeader" => {
                                    assert_eq!(request["params"], json!([BLOCK_HASH]));
                                    json!({"jsonrpc": "2.0", "id": 1,
                                        "result": {"number": "0x2a"}})
                                }
                                "state_call" => {
                                    assert_eq!(
                                        request["params"][0],
                                        DEEPX_MARGIN_RUNTIME_API_METHOD
                                    );
                                    let hash = request["params"][2].as_str().unwrap();
                                    if hash == format!("0x{}", "00".repeat(32)) && !ignored_hash {
                                        json!({"jsonrpc": "2.0", "id": 1,
                                            "error": {"code": -32000, "message": "unknown block"}})
                                    } else {
                                        assert!(hash == BLOCK_HASH || ignored_hash);
                                        calls.fetch_add(1, Ordering::Relaxed);
                                        let data = BTreeMap::from([(
                                            subaccount,
                                            (10_000_000_u128, 0_u128),
                                        )])
                                        .encode();
                                        json!({"jsonrpc": "2.0", "id": 1,
                                            "result": format!("0x{}", hex::encode(data))})
                                    }
                                }
                                method => panic!("unexpected current margin method: {method}"),
                            };
                            Json(response)
                        }
                    }
                }),
            );
            let watch_url = spawn_server(router).await;
            let endpoints =
                validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
            let capabilities = validated_capabilities(&endpoints);
            let result =
                observe_current_margin_evidence(&endpoints, &capabilities, subaccount).await;
            if ignored_hash || reorganized {
                assert!(result.is_err());
            } else {
                let margin = result.unwrap();
                assert_eq!(margin.initial().total_collateral(), 10_000_000);
                assert_eq!(margin.maintenance().total_margin_required(), 0);
                assert_eq!(calls.load(Ordering::Relaxed), 4);
            }
        }
    }

    #[tokio::test]
    async fn finalized_lending_observation_pins_complete_raw_evidence_to_one_checkpoint() {
        let subaccount = [0x11; 20];
        let checkpoint = DeepXFinalizedCheckpoint {
            block_hash: [0x22; 32],
            block_number: 42,
        };
        let asset = b"usdc".to_vec();
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let asset = asset.clone();
                async move {
                    if request["method"] == "eth_call" {
                        assert_eq!(
                            request["params"][0]["to"],
                            DEEPX_LENDING_PRECOMPILE_ADDRESS.to_string()
                        );
                        assert_eq!(
                            request["params"][1],
                            format!("0x{:x}", checkpoint.block_number())
                        );
                        let encoded = request["params"][0]["data"].as_str().unwrap();
                        let bytes = hex::decode(encoded.trim_start_matches("0x")).unwrap();
                        let call = maxWithdrawAmountForCall::abi_decode(&bytes).unwrap();
                        assert_eq!(call.subaccount, Address::from(subaccount));
                        assert_eq!(call.lendingMarket, 1);
                        assert_eq!(call.asset.as_ref(), asset.as_slice());
                        return Json(json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "result": format!("0x{:064x}", 750_000),
                        }));
                    }
                    assert_eq!(request["method"], "state_call");
                    assert_eq!(
                        request["params"][2],
                        format!("0x{}", hex::encode(checkpoint.block_hash()))
                    );
                    let method = request["params"][0].as_str().unwrap();
                    let encoded = request["params"][1].as_str().unwrap();
                    let bytes = hex::decode(encoded.trim_start_matches("0x")).unwrap();
                    let result = match method {
                        DEEPX_LENDING_POSITIONS_RUNTIME_API_METHOD => {
                            let (accounts, market_ids) =
                                <(Vec<[u8; 20]>, Vec<u8>)>::decode(&mut bytes.as_slice()).unwrap();
                            assert_eq!(accounts, vec![subaccount]);
                            assert_eq!(market_ids, (u8::MIN..=u8::MAX).collect::<Vec<_>>());
                            BTreeMap::from([(
                                subaccount,
                                BTreeMap::from([(
                                    1_u8,
                                    EncodedLendingPosition {
                                        positions: BTreeMap::from([(
                                            asset.clone(),
                                            (970_936_185_u128, 0_u128),
                                        )]),
                                    },
                                )]),
                            )])
                            .encode()
                        }
                        DEEPX_LENDING_POOL_RUNTIME_API_METHOD => {
                            let (market_id, requested_asset) =
                                <(u8, Vec<u8>)>::decode(&mut bytes.as_slice()).unwrap();
                            assert_eq!((market_id, &requested_asset), (1, &asset));
                            Some(lending_pool(market_id, &requested_asset)).encode()
                        }
                        DEEPX_LENDING_DEPOSIT_RUNTIME_API_METHOD => {
                            let (account, market_id, requested_asset) =
                                <([u8; 20], u8, Vec<u8>)>::decode(&mut bytes.as_slice()).unwrap();
                            assert_eq!(
                                (account, market_id, &requested_asset),
                                (subaccount, 1, &asset)
                            );
                            Some(970_957_336_u128).encode()
                        }
                        DEEPX_LENDING_BORROW_RUNTIME_API_METHOD => {
                            let (account, market_id, requested_asset) =
                                <([u8; 20], u8, Vec<u8>)>::decode(&mut bytes.as_slice()).unwrap();
                            assert_eq!(
                                (account, market_id, &requested_asset),
                                (subaccount, 1, &asset)
                            );
                            Option::<u128>::None.encode()
                        }
                        _ => panic!("unexpected runtime API method"),
                    };
                    Json(json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": format!("0x{}", hex::encode(result)),
                    }))
                }
            }),
        );
        let url = spawn_server(router).await;

        let positions = observe_finalized_lending_at(&url, checkpoint, subaccount)
            .await
            .unwrap();

        assert_eq!(positions.len(), 1);
        let position = &positions[0];
        assert_eq!(position.market_id(), 1);
        assert_eq!(position.asset(), b"usdc");
        assert_eq!(position.scaled_deposit(), 970_936_185);
        assert_eq!(position.scaled_borrow(), 0);
        assert_eq!(position.effective_deposit(), Some(970_957_336));
        assert_eq!(position.effective_borrow(), None);
        assert_eq!(position.max_withdraw_amount(), Some(750_000));
        assert_eq!(position.pool().decimal(), 6);
        assert_eq!(position.pool().asset(), b"usdc");
        assert_eq!(position.pool().locked_liquidity(), 100_000);
        assert_eq!(position.pool().status(), DeepXLendingPoolStatus::Active);
    }

    #[rstest::rstest]
    fn finalized_lending_asset_validation_rejects_identity_and_presence_contradictions() {
        let raw = || DeepXRawLendingPosition {
            market_id: 1,
            asset: b"usdc".to_vec(),
            scaled_deposit: 100,
            scaled_borrow: 0,
        };

        assert!(matches!(
            validate_lending_asset_evidence(
                raw(),
                lending_pool(2, b"usdc"),
                Some(101),
                None,
                Some(50),
            ),
            Err(DeepXFinalizedLendingError::InvalidAssetEvidence(1)),
        ));
        assert!(matches!(
            validate_lending_asset_evidence(
                raw(),
                lending_pool(1, b"usdt"),
                Some(101),
                None,
                Some(50),
            ),
            Err(DeepXFinalizedLendingError::InvalidAssetEvidence(1)),
        ));
        assert!(matches!(
            validate_lending_asset_evidence(raw(), lending_pool(1, b"usdc"), None, None, Some(50)),
            Err(DeepXFinalizedLendingError::InvalidAssetEvidence(1)),
        ));
        assert!(matches!(
            validate_lending_asset_evidence(
                raw(),
                lending_pool(1, b"usdc"),
                Some(101),
                Some(1),
                Some(50),
            ),
            Err(DeepXFinalizedLendingError::InvalidAssetEvidence(1)),
        ));
        assert!(matches!(
            validate_lending_asset_evidence(
                raw(),
                lending_pool(1, b"usdc"),
                Some(101),
                None,
                Some(102),
            ),
            Err(DeepXFinalizedLendingError::InvalidAssetEvidence(1)),
        ));
        assert!(matches!(
            validate_lending_asset_evidence(
                raw(),
                lending_pool(1, b"usdc"),
                Some(2_000_000),
                None,
                Some(1_900_001),
            ),
            Err(DeepXFinalizedLendingError::InvalidAssetEvidence(1)),
        ));

        let borrow_only = DeepXRawLendingPosition {
            market_id: 1,
            asset: b"usdc".to_vec(),
            scaled_deposit: 0,
            scaled_borrow: 100,
        };
        let evidence = validate_lending_asset_evidence(
            borrow_only,
            lending_pool(1, b"usdc"),
            None,
            Some(101),
            None,
        )
        .unwrap();
        assert_eq!(evidence.max_withdraw_amount(), None);
    }

    #[rstest::rstest]
    #[case("0x")]
    #[case("0x01")]
    #[case(&format!("0x01{}", "00".repeat(31)))]
    #[case(&format!("0x{}00", "00".repeat(32)))]
    fn finalized_lending_rejects_non_canonical_withdrawal_abi(#[case] encoded: &str) {
        assert!(matches!(
            decode_max_withdrawal(encoded, 1),
            Err(DeepXFinalizedLendingError::InvalidWithdrawalEvidence(1)),
        ));
    }

    #[rstest::rstest]
    #[case(BTreeMap::from([(
        [0x22; 20],
        BTreeMap::from([(
            1_u8,
            EncodedLendingPosition {
                positions: BTreeMap::from([(b"usdc".to_vec(), (1_u128, 0_u128))]),
            },
        )]),
    )]).encode())]
    #[case(BTreeMap::from([(
        [0x11; 20],
        BTreeMap::from([(
            1_u8,
            EncodedLendingPosition {
                positions: BTreeMap::from([(b"usdc".to_vec(), (0_u128, 0_u128))]),
            },
        )]),
    )]).encode())]
    #[case(BTreeMap::from([(
        [0x11; 20],
        BTreeMap::from([(
            1_u8,
            EncodedLendingPosition {
                positions: BTreeMap::from([(vec![b'x'; 33], (1_u128, 0_u128))]),
            },
        )]),
    )]).encode())]
    #[case({
        let mut encoded = BTreeMap::from([(
            [0x11; 20],
            BTreeMap::from([(
                1_u8,
                EncodedLendingPosition {
                    positions: BTreeMap::from([(b"usdc".to_vec(), (1_u128, 0_u128))]),
                },
            )]),
        )]).encode();
        encoded.push(0);
        encoded
    })]
    fn finalized_lending_decoder_rejects_foreign_zero_overlong_and_trailing_evidence(
        #[case] encoded: Vec<u8>,
    ) {
        let result = decode_lending_positions(&format!("0x{}", hex::encode(encoded)), [0x11; 20]);

        assert!(matches!(
            result,
            Err(DeepXFinalizedLendingError::InvalidPositions)
        ));
    }

    #[rstest::rstest]
    fn finalized_lending_decoder_accepts_canonical_empty_account_evidence() {
        let encoded = BTreeMap::<[u8; 20], BTreeMap<u8, EncodedLendingPosition>>::new().encode();

        assert_eq!(
            decode_lending_positions(&format!("0x{}", hex::encode(encoded)), [0x11; 20]).unwrap(),
            Vec::new(),
        );
    }

    #[rstest::rstest]
    #[case(BTreeMap::<[u8; 20], (u128, u128)>::new().encode())]
    #[case(BTreeMap::from([([0x22; 20], (1_u128, 2_u128))]).encode())]
    #[case({
        let mut encoded = BTreeMap::from([([0x11; 20], (1_u128, 2_u128))]).encode();
        encoded.push(0);
        encoded
    })]
    fn finalized_margin_decoder_rejects_missing_foreign_and_trailing_evidence(
        #[case] encoded: Vec<u8>,
    ) {
        let result = decode_margin_requirement(
            &format!("0x{}", hex::encode(encoded)),
            [0x11; 20],
            DeepXMarginDirection::Initial,
        );

        assert!(matches!(
            result,
            Err(DeepXFinalizedMarginError::InvalidEvidence(
                DeepXMarginDirection::Initial
            ))
        ));
    }

    #[rstest::rstest]
    fn rpc_capability_debug_redacts_endpoint_url() {
        let secret_url = "https://rpc.example.test/path?token=secret";
        let endpoints = validated_endpoints(secret_url);
        let capabilities = validated_capabilities(&endpoints);
        let debug = format!("{capabilities:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains(secret_url));
        assert!(!debug.contains("secret"));
        assert!(debug.contains("author_submitExtrinsic"));
    }

    #[tokio::test]
    async fn observes_and_validates_all_rpc_role_identities() {
        let response: Value = serde_json::from_str(GENESIS_FIXTURE).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let response = response.clone();
                let calls = Arc::clone(&server_calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(request["jsonrpc"], "2.0");
                    assert_eq!(request["id"], 1);
                    assert_eq!(request["method"], "chain_getBlockHash");
                    assert_eq!(request["params"], json!([0]));
                    Json(response)
                }
            }),
        );
        let rpc_url = spawn_server(router).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc_submission: Some(rpc_url.clone()),
            base_url_rpc_watch: Some(rpc_url.clone()),
            base_url_rpc_recovery: Some(rpc_url),
            ..Default::default()
        };
        let validated = observe_and_validate_rpc_endpoint_identities(&config)
            .await
            .unwrap();

        assert_eq!(
            validated.url_for(DeepXRpcRole::Submission),
            config.rpc_url_for(DeepXRpcRole::Submission).unwrap()
        );
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn rpc_identity_probe_retries_failed_read_then_decodes_genesis() {
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&server_calls);
                async move {
                    assert_eq!(request["method"], "chain_getBlockHash");
                    if calls.fetch_add(1, Ordering::Relaxed) == 0 {
                        Json(json!({
                            "jsonrpc": "2.0", "id": 1,
                            "error": {"code": -32000, "message": "temporary failure"},
                        }))
                    } else {
                        Json(serde_json::from_str::<Value>(GENESIS_FIXTURE).unwrap())
                    }
                }
            }),
        );
        let rpc_url = spawn_server(router).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc_watch: Some(rpc_url.clone()),
            ..Default::default()
        };

        let observed = observe_rpc_endpoint_identity(&config, DeepXRpcRole::Watch)
            .await
            .unwrap();

        assert_eq!(observed.role(), DeepXRpcRole::Watch);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn rpc_method_probe_stops_after_bounded_failed_reads() {
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&server_calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(request["method"], "rpc_methods");
                    Json(json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": {"code": -32000, "message": "temporary failure"},
                    }))
                }
            }),
        );
        let rpc_url = spawn_server(router).await;
        let endpoints = validated_endpoints(&rpc_url);

        assert!(matches!(
            observe_rpc_method_capabilities(&endpoints, DeepXRpcRole::Watch, ["state_getStorage"])
                .await,
            Err(DeepXRpcMethodCapabilityError::Rpc {
                role: DeepXRpcRole::Watch,
                ..
            }),
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn observes_each_configured_rpc_role_endpoint() {
        let submission_calls = Arc::new(AtomicUsize::new(0));
        let watch_calls = Arc::new(AtomicUsize::new(0));
        let recovery_calls = Arc::new(AtomicUsize::new(0));
        let submission_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&submission_calls)).await;
        let watch_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&watch_calls)).await;
        let recovery_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&recovery_calls)).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc: Some("http://127.0.0.1:1".to_string()),
            base_url_rpc_submission: Some(submission_url.clone()),
            base_url_rpc_watch: Some(watch_url.clone()),
            base_url_rpc_recovery: Some(recovery_url.clone()),
            ..Default::default()
        };

        let validated = observe_and_validate_rpc_endpoint_identities(&config)
            .await
            .unwrap();

        assert_eq!(submission_calls.load(Ordering::Relaxed), 1);
        assert_eq!(watch_calls.load(Ordering::Relaxed), 1);
        assert_eq!(recovery_calls.load(Ordering::Relaxed), 1);
        assert_eq!(validated.url_for(DeepXRpcRole::Submission), submission_url);
        assert_eq!(validated.url_for(DeepXRpcRole::Watch), watch_url);
        assert_eq!(validated.url_for(DeepXRpcRole::Recovery), recovery_url);
    }

    #[tokio::test]
    async fn rejects_complete_observations_when_one_role_has_wrong_chain_identity() {
        const WRONG_GENESIS_HASH: &str =
            "0x9999999999999999999999999999999999999999999999999999999999999999";
        let submission_calls = Arc::new(AtomicUsize::new(0));
        let watch_calls = Arc::new(AtomicUsize::new(0));
        let recovery_calls = Arc::new(AtomicUsize::new(0));
        let submission_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&submission_calls)).await;
        let watch_url = spawn_identity_server(WRONG_GENESIS_HASH, Arc::clone(&watch_calls)).await;
        let recovery_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&recovery_calls)).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc_submission: Some(submission_url),
            base_url_rpc_watch: Some(watch_url),
            base_url_rpc_recovery: Some(recovery_url),
            ..Default::default()
        };

        let error = observe_and_validate_rpc_endpoint_identities(&config)
            .await
            .unwrap_err();

        assert_eq!(submission_calls.load(Ordering::Relaxed), 1);
        assert_eq!(watch_calls.load(Ordering::Relaxed), 1);
        assert_eq!(recovery_calls.load(Ordering::Relaxed), 1);
        assert!(matches!(
            error,
            DeepXRpcEndpointIdentityError::Validation(
                DeepXRpcEndpointValidationError::GenesisHashMismatch(DeepXRpcRole::Watch)
            )
        ));
    }

    #[tokio::test]
    async fn attributes_observation_failure_after_all_role_requests_complete() {
        let submission_calls = Arc::new(AtomicUsize::new(0));
        let watch_calls = Arc::new(AtomicUsize::new(0));
        let recovery_calls = Arc::new(AtomicUsize::new(0));
        let submission_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&submission_calls)).await;
        let watch_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&watch_calls)).await;
        let recovery_url = spawn_identity_server("malformed", Arc::clone(&recovery_calls)).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc_submission: Some(submission_url),
            base_url_rpc_watch: Some(watch_url),
            base_url_rpc_recovery: Some(recovery_url),
            ..Default::default()
        };

        let error = observe_and_validate_rpc_endpoint_identities(&config)
            .await
            .unwrap_err();

        assert_eq!(submission_calls.load(Ordering::Relaxed), 1);
        assert_eq!(watch_calls.load(Ordering::Relaxed), 1);
        assert_eq!(recovery_calls.load(Ordering::Relaxed), 1);
        assert!(matches!(
            error,
            DeepXRpcEndpointIdentityError::Observation {
                role: DeepXRpcRole::Recovery,
                source: DeepXRpcIdentityError::InvalidGenesisHash,
            }
        ));
    }

    #[tokio::test]
    async fn observes_required_rpc_methods_for_validated_role_endpoint() {
        let rpc_url = spawn_rpc_methods_server(&["author_submitExtrinsic", "system_health"]).await;
        let endpoints = validated_endpoints(&rpc_url);

        let capabilities = observe_rpc_method_capabilities(
            &endpoints,
            DeepXRpcRole::Submission,
            ["author_submitExtrinsic"],
        )
        .await
        .unwrap();

        assert_eq!(capabilities.role(), DeepXRpcRole::Submission);
        assert_eq!(
            capabilities.methods(),
            &BTreeSet::from(["author_submitExtrinsic".to_string()])
        );
    }

    #[tokio::test]
    async fn rejects_rpc_role_endpoint_missing_required_method() {
        let rpc_url = spawn_rpc_methods_server(&["chain_getFinalizedHead"]).await;
        let endpoints = validated_endpoints(&rpc_url);

        let error = observe_rpc_method_capabilities(
            &endpoints,
            DeepXRpcRole::Watch,
            ["chain_getFinalizedHead", "chain_getHeader"],
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXRpcMethodCapabilityError::MissingMethod {
                role: DeepXRpcRole::Watch,
                method,
            } if method == "chain_getHeader"
        ));
    }

    #[tokio::test]
    async fn rejects_empty_rpc_method_requirements_without_network_io() {
        let endpoints = validated_endpoints("http://127.0.0.1:1");

        let error = observe_rpc_method_capabilities(
            &endpoints,
            DeepXRpcRole::Recovery,
            std::iter::empty::<&str>(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXRpcMethodCapabilityError::EmptyRequirements(DeepXRpcRole::Recovery)
        ));
    }

    #[tokio::test]
    async fn observes_required_rpc_methods_for_all_validated_role_endpoints() {
        let submission_url = spawn_rpc_methods_server(DEEPX_SUBMISSION_RPC_METHODS).await;
        let watch_url = spawn_rpc_methods_server(DEEPX_WATCH_RPC_METHODS).await;
        let recovery_url = spawn_rpc_methods_server(DEEPX_RECOVERY_RPC_METHODS).await;
        let endpoints = validated_role_endpoints(&submission_url, &watch_url, &recovery_url);

        let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap();

        for (role, expected_url, expected_methods) in [
            (
                DeepXRpcRole::Submission,
                submission_url.as_str(),
                DEEPX_SUBMISSION_RPC_METHODS,
            ),
            (
                DeepXRpcRole::Watch,
                watch_url.as_str(),
                DEEPX_WATCH_RPC_METHODS,
            ),
            (
                DeepXRpcRole::Recovery,
                recovery_url.as_str(),
                DEEPX_RECOVERY_RPC_METHODS,
            ),
        ] {
            let observed = capabilities.for_role(role);
            assert_eq!(observed.role(), role);
            assert_eq!(observed.endpoint_url(), expected_url);
            assert_eq!(
                observed.methods(),
                &expected_methods
                    .iter()
                    .map(|method| (*method).to_string())
                    .collect(),
            );
        }
    }

    #[tokio::test]
    async fn shared_rpc_endpoint_is_probed_independently_for_each_role() {
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let methods = DEEPX_SUBMISSION_RPC_METHODS
            .iter()
            .chain(DEEPX_WATCH_RPC_METHODS)
            .chain(DEEPX_RECOVERY_RPC_METHODS)
            .copied()
            .collect::<BTreeSet<_>>();
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&server_calls);
                let methods = methods.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(request["method"], "rpc_methods");
                    Json(json!({
                        "jsonrpc": "2.0", "id": 1,
                        "result": {"methods": methods},
                    }))
                }
            }),
        );
        let rpc_url = spawn_server(router).await;
        let endpoints = validated_endpoints(&rpc_url);

        let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 3);
        for (role, required) in [
            (DeepXRpcRole::Submission, DEEPX_SUBMISSION_RPC_METHODS),
            (DeepXRpcRole::Watch, DEEPX_WATCH_RPC_METHODS),
            (DeepXRpcRole::Recovery, DEEPX_RECOVERY_RPC_METHODS),
        ] {
            assert_eq!(capabilities.for_role(role).role(), role);
            assert_eq!(capabilities.for_role(role).endpoint_url(), rpc_url);
            assert_eq!(
                capabilities.for_role(role).methods(),
                &required
                    .iter()
                    .map(|method| (*method).to_string())
                    .collect(),
            );
        }
    }

    #[tokio::test]
    async fn shared_rpc_endpoint_attributes_missing_method_to_its_role() {
        let methods = DEEPX_SUBMISSION_RPC_METHODS
            .iter()
            .chain(
                DEEPX_WATCH_RPC_METHODS
                    .iter()
                    .filter(|method| **method != "state_call"),
            )
            .chain(DEEPX_RECOVERY_RPC_METHODS)
            .copied()
            .collect::<BTreeSet<_>>();
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let methods = methods.clone();
                async move {
                    assert_eq!(request["method"], "rpc_methods");
                    Json(json!({
                        "jsonrpc": "2.0", "id": 1,
                        "result": {"methods": methods},
                    }))
                }
            }),
        );
        let rpc_url = spawn_server(router).await;
        let endpoints = validated_endpoints(&rpc_url);

        let error = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap_err();

        assert_eq!(error.role, DeepXRpcRole::Watch);
        assert!(matches!(
            error.source,
            DeepXRpcMethodCapabilityError::MissingMethod {
                role: DeepXRpcRole::Watch,
                method,
            } if method == "state_call"
        ));
    }

    #[tokio::test]
    async fn attributes_complete_rpc_method_failure_to_role() {
        let submission_url = spawn_rpc_methods_server(DEEPX_SUBMISSION_RPC_METHODS).await;
        let watch_url = spawn_rpc_methods_server(&["chain_getFinalizedHead"]).await;
        let recovery_url = spawn_rpc_methods_server(DEEPX_RECOVERY_RPC_METHODS).await;
        let endpoints = validated_role_endpoints(&submission_url, &watch_url, &recovery_url);

        let error = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap_err();

        assert_eq!(error.role, DeepXRpcRole::Watch);
        assert!(matches!(
            error.source,
            DeepXRpcMethodCapabilityError::MissingMethod {
                role: DeepXRpcRole::Watch,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn observes_spec_376_signing_runtime_snapshot() {
        let calls = Arc::new(AtomicUsize::new(0));
        let rpc_url =
            spawn_runtime_snapshot_server(DEEPX_SPEC_376_VERSION, Arc::clone(&calls)).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc: Some(rpc_url),
            ..Default::default()
        };

        let observation = observe_approved_finalized_runtime_snapshot(&config)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 5);
        assert_eq!(
            observation.snapshot().identity().spec_version,
            DEEPX_SPEC_376_VERSION,
        );
        assert_eq!(
            observation.checkpoint().block_number(),
            FINALIZED_BLOCK_NUMBER
        );
    }

    #[tokio::test]
    async fn observes_spec_376_through_read_only_runtime_boundary() {
        let read_only_calls = Arc::new(AtomicUsize::new(0));
        let read_only_url = spawn_runtime_snapshot_server_with(
            DEEPX_SPEC_376_VERSION,
            1,
            serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap(),
            None,
            None,
            |_| {},
            Arc::clone(&read_only_calls),
        )
        .await;
        let read_only_config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc: Some(read_only_url),
            ..Default::default()
        };

        let observation = observe_approved_finalized_read_only_runtime_snapshot(&read_only_config)
            .await
            .unwrap();

        assert_eq!(read_only_calls.load(Ordering::Relaxed), 5);
        assert_eq!(
            observation.snapshot().identity().spec_version,
            DEEPX_SPEC_376_VERSION
        );
        assert_eq!(
            observation.checkpoint().block_number(),
            FINALIZED_BLOCK_NUMBER
        );
    }

    #[rstest]
    #[case::consistent(false, false, false, false)]
    #[case::inconsistent_timestamp(true, false, false, false)]
    #[case::timestamp_changes_during_batch(false, true, false, false)]
    #[case::storage_changes_during_batch(false, false, true, false)]
    #[case::storage_hash_ignored(false, false, false, true)]
    #[tokio::test]
    async fn finalized_spot_market_spec_is_pinned_to_approved_checkpoint(
        #[case] timestamp_mismatch: bool,
        #[case] late_timestamp_change: bool,
        #[case] late_storage_change: bool,
        #[case] storage_hash_ignored: bool,
    ) {
        let pair = [0x33; 32];
        let second_pair = [0x44; 32];
        let genesis: Value = serde_json::from_str(GENESIS_FIXTURE).unwrap();
        let finalized_head: Value = serde_json::from_str(FINALIZED_HEAD_FIXTURE).unwrap();
        let spot_market_storage: Value =
            serde_json::from_str(SPOT_MARKET_STORAGE_SPEC_376_FIXTURE).unwrap();
        let spot_market_storage = spot_market_storage["result"].as_str().unwrap().to_string();
        let header = json!({
            "jsonrpc": "2.0", "id": 1,
            "result": { "number": FINALIZED_BLOCK_NUMBER_HEX }
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&server_calls);
                let genesis = genesis.clone();
                let finalized_head = finalized_head.clone();
                let header = header.clone();
                let spot_market_storage = spot_market_storage.clone();
                async move {
                    if let Some(response) = read_only_runtime_rpc_response(&request) {
                        return Json(response);
                    }
                    let index = calls.fetch_add(1, Ordering::Relaxed);
                    let response = match index {
                        0 => genesis,
                        1 => finalized_head,
                        2 => header,
                        3 => {
                            assert_eq!(request["method"], "state_getStorage");
                            assert_eq!(
                                request["params"],
                                json!([timestamp_now_storage_key(), FINALIZED_HASH]),
                            );
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": format!("0x{}", hex::encode(1_700_000_000_123u64.encode())),
                            })
                        }
                        4 => {
                            assert_eq!(request["method"], "chain_getBlock");
                            assert_eq!(request["params"], json!([FINALIZED_HASH]));
                            let mut inherent = vec![4, 2, 0];
                            inherent.extend(Compact(
                                1_700_000_000_123u64 + u64::from(timestamp_mismatch),
                            ).encode());
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": {
                                    "block": {
                                        "header": {"number": FINALIZED_BLOCK_NUMBER_HEX},
                                        "extrinsics": [format!("0x{}", hex::encode(inherent.encode()))],
                                    },
                                },
                            })
                        }
                        5 => {
                            assert_eq!(request["method"], "state_queryStorageAt");
                            assert_eq!(
                                request["params"],
                                json!([[timestamp_now_storage_key()], FINALIZED_HASH]),
                            );
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": [{
                                    "block": FINALIZED_HASH,
                                    "changes": [[
                                        timestamp_now_storage_key(),
                                        format!("0x{}", hex::encode(1_700_000_000_123u64.encode())),
                                    ]],
                                }],
                            })
                        }
                        6 => {
                            assert_eq!(request["method"], "state_getStorage");
                            assert_eq!(
                                request["params"],
                                json!([timestamp_now_storage_key(), format!("0x{}", "00".repeat(32))]),
                            );
                            if storage_hash_ignored {
                                json!({"jsonrpc": "2.0", "id": 1, "result": "0x"})
                            } else {
                                json!({"jsonrpc": "2.0", "id": 1,
                                    "error": {"code": -32000, "message": "unknown block"}})
                            }
                        }
                        7..=10 => {
                            assert_eq!(request["method"], "state_getStorage");
                            assert_eq!(request["params"][1], FINALIZED_HASH);
                            let observed_pair = if index < 9 { pair } else { second_pair };
                            assert_eq!(request["params"][0], spot_market_storage_key(observed_pair));
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": spot_market_storage.replace(
                                    "9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37",
                                    &hex::encode(observed_pair),
                                ) + if index == 8 && late_storage_change { "00" } else { "" },
                            })
                        }
                        11 => {
                            assert_eq!(request["method"], "state_getStorage");
                            assert_eq!(
                                request["params"],
                                json!([timestamp_now_storage_key(), FINALIZED_HASH]),
                            );
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": format!(
                                    "0x{}",
                                    hex::encode((1_700_000_000_123u64
                                        + u64::from(late_timestamp_change)).encode()),
                                ),
                            })
                        }
                        _ => panic!("unexpected finalized Spot market spec request"),
                    };
                    Json(response)
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);

        let result = observe_finalized_spot_market_spec_batch(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &[pair, second_pair],
        )
        .await;

        if timestamp_mismatch {
            assert!(matches!(
                result,
                Err(DeepXFinalizedSpotMarketSpecError::Checkpoint(source))
                    if matches!(*source, DeepXFinalizedAccountError::BlockTimestampMismatch { .. })
            ));
            assert_eq!(calls.load(Ordering::Relaxed), 5);
        } else if storage_hash_ignored {
            assert!(matches!(
                result,
                Err(DeepXFinalizedSpotMarketSpecError::Checkpoint(source))
                    if matches!(*source, DeepXFinalizedAccountError::StorageBlockHashIgnored)
            ));
            assert_eq!(calls.load(Ordering::Relaxed), 7);
        } else if late_storage_change {
            assert!(matches!(
                result,
                Err(DeepXFinalizedSpotMarketSpecError::InvalidEvidence(_))
            ));
            assert_eq!(calls.load(Ordering::Relaxed), 9);
        } else if late_timestamp_change {
            assert!(matches!(
                result,
                Err(DeepXFinalizedSpotMarketSpecError::Checkpoint(source))
                    if matches!(*source, DeepXFinalizedAccountError::BlockTimestampMismatch { .. })
            ));
            assert_eq!(calls.load(Ordering::Relaxed), 12);
        } else {
            let evidence = result.unwrap();
            assert_eq!(evidence.len(), 2);
            assert_eq!(
                evidence[0].runtime_identity().spec_version,
                DEEPX_SPEC_376_VERSION
            );
            assert_eq!(
                evidence[0].checkpoint().block_number(),
                FINALIZED_BLOCK_NUMBER
            );
            assert_eq!(evidence[0].checkpoint(), evidence[1].checkpoint());
            assert_eq!(evidence[0].pair(), pair);
            assert_eq!(evidence[0].min_order_size(), 100_000_000_000_000);
            assert_eq!(evidence[0].tick_size(), 10_000);
            assert_eq!(evidence[0].step_size(), 100_000_000_000_000);
            assert_eq!(calls.load(Ordering::Relaxed), 12);
        }
    }

    #[rstest]
    #[case::stable(false, false, false)]
    #[case::changed(true, false, false)]
    #[case::reorganized(false, true, false)]
    #[case::hash_ignored(false, false, true)]
    #[tokio::test]
    async fn current_spot_market_spec_reads_one_hash_without_finality(
        #[case] changed: bool,
        #[case] reorganized: bool,
        #[case] hash_ignored: bool,
    ) {
        const CURRENT_HASH: &str =
            "0x4242424242424242424242424242424242424242424242424242424242424242";
        let pair =
            hex::decode_array("9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37")
                .unwrap();
        let metadata: Value = serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap();
        let stored: Value = serde_json::from_str(SPOT_MARKET_STORAGE_SPEC_376_FIXTURE).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let router = Router::new().route(
            "/",
            post({
                let calls = Arc::clone(&calls);
                move |Json(request): Json<Value>| {
                    let calls = Arc::clone(&calls);
                    let metadata = metadata.clone();
                    let stored = stored.clone();
                    async move {
                        let index = calls.fetch_add(1, Ordering::Relaxed);
                        let response = match request["method"].as_str().unwrap() {
                            "chain_getBlockHash" => {
                                assert!(
                                    request["params"] == json!([])
                                        || request["params"] == json!([42])
                                );
                                let hash = if reorganized && index == 7 {
                                    format!("0x{}", "43".repeat(32))
                                } else {
                                    CURRENT_HASH.to_string()
                                };
                                json!({"jsonrpc": "2.0", "id": 1, "result": hash})
                            }
                            "chain_getHeader" => {
                                assert_eq!(request["params"], json!([CURRENT_HASH]));
                                json!({"jsonrpc": "2.0", "id": 1, "result": {
                                    "number": "0x2a",
                                }})
                            }
                            "state_getRuntimeVersion" => {
                                assert_eq!(request["params"], json!([CURRENT_HASH]));
                                json!({"jsonrpc": "2.0", "id": 1, "result": {
                                    "specVersion": 377, "transactionVersion": 2,
                                }})
                            }
                            "state_getMetadata" => {
                                assert_eq!(request["params"], json!([CURRENT_HASH]));
                                metadata
                            }
                            "state_getStorage" => {
                                assert_eq!(request["params"][0], spot_market_storage_key(pair));
                                if request["params"][1] == format!("0x{}", "00".repeat(32)) {
                                    if hash_ignored {
                                        return Json(json!({"jsonrpc": "2.0", "id": 1,
                                            "result": stored["result"]}));
                                    }
                                    return Json(json!({"jsonrpc": "2.0", "id": 1,
                                        "error": {"code": -32000, "message": "unknown block"}}));
                                }
                                assert_eq!(request["params"][1], CURRENT_HASH);
                                let mut encoded = stored["result"].as_str().unwrap().to_string();
                                if changed && index == 6 {
                                    encoded.push_str("00");
                                }
                                json!({"jsonrpc": "2.0", "id": 1, "result": encoded})
                            }
                            method => panic!("unexpected current Spot spec RPC: {method}"),
                        };
                        Json(response)
                    }
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);

        let result = observe_current_spot_market_spec_batch(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &[pair],
        )
        .await;

        if hash_ignored {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("ignored an invalid")
            );
        } else if changed {
            assert!(result.unwrap_err().to_string().contains("storage changed"));
        } else if reorganized {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("no longer canonical")
            );
        } else {
            let specs = result.unwrap();
            assert_eq!(specs.len(), 1);
            assert_eq!(specs[0].pair(), pair);
            assert_eq!(specs[0].min_order_size(), 100_000_000_000_000);
            assert_eq!(specs[0].tick_size(), 10_000);
            assert_eq!(specs[0].step_size(), 100_000_000_000_000);
        }
        assert_eq!(
            calls.load(Ordering::Relaxed),
            if hash_ignored {
                5
            } else if changed {
                7
            } else {
                8
            }
        );
    }

    #[rstest]
    #[case("state_getStorage")]
    #[case("state_queryStorageAt")]
    #[case("chain_getBlock")]
    #[tokio::test]
    async fn spot_market_spec_requires_checkpoint_capabilities(#[case] missing: &str) {
        let endpoints = validated_role_endpoints(
            "http://127.0.0.1:1",
            "http://127.0.0.1:2",
            "http://127.0.0.1:3",
        );
        let mut capabilities = validated_capabilities(&endpoints);
        capabilities.watch.methods.remove(missing);

        let error = observe_finalized_spot_market_spec_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            [0x33; 32],
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXFinalizedSpotMarketSpecError::RpcCapabilitiesMismatch,
        ));
    }

    fn encoded_perp_market(market_id: u16, spec: (u128, u128, u128)) -> String {
        let market = DeepXPerpMarketAbi {
            id: market_id,
            name: Bytes::from_static(b"BTC"),
            base_symbol: Bytes::from_static(b"BTC"),
            base_decimal: 8,
            quote_market_id: 0,
            network: Bytes::from_static(b"bitcoin"),
            height: 1,
            funding_rate: 0,
            last_cacl_funding_rate_time: 0,
            oracle_price: 1,
            mark_price: 1,
            max_deviation_bps: 0,
            maintenance_margin_ratio: 1,
            taker_fee_rate: 0,
            maker_fee_rate: 0,
            order_spec: DeepXPerpOrderSpecAbi {
                min_order_size: spec.0,
                tick_size: spec.1,
                step_size: spec.2,
            },
            open_interest: 0,
            long_open_pos_num: 0,
            short_open_pos_num: 0,
            base_interest_rate: 0,
            impact_margin_value: 0,
            funding_rate_change_cap: 0,
            funding_rate_change_floor: 0,
            cumulative_funding_index: 0,
        };
        format!("0x{}", hex::encode(market.abi_encode()))
    }

    fn encoded_perp_market_storage() -> String {
        use subxt_core::ext::scale_value::Value as ScaleValue;

        let fixed_i128 = || ScaleValue::unnamed_composite([ScaleValue::i128(0)]);
        let market = ScaleValue::named_composite([
            ("id", ScaleValue::u128(3)),
            ("name", ScaleValue::from_bytes([])),
            ("base_symbol", ScaleValue::from_bytes([])),
            ("base_address", ScaleValue::from_bytes([0; 20])),
            ("base_decimal", ScaleValue::i128(8)),
            ("quote_market_id", ScaleValue::u128(0)),
            ("quote_symbol", ScaleValue::from_bytes([])),
            ("quote_address", ScaleValue::from_bytes([0; 20])),
            ("quote_decimal", ScaleValue::i128(6)),
            ("network", ScaleValue::from_bytes([])),
            ("height", ScaleValue::u128(1)),
            ("funding_rate", ScaleValue::i128(0)),
            ("cumulative_funding_index", ScaleValue::i128(0)),
            ("last_funding_rate_time", ScaleValue::u128(0)),
            ("last_cacl_funding_rate_time", ScaleValue::u128(0)),
            ("oracle_price", ScaleValue::u128(1)),
            ("mark_price", ScaleValue::u128(1)),
            ("max_deviation_bps", ScaleValue::u128(0)),
            ("initial_margin_ratio", ScaleValue::u128(1)),
            ("maintenance_margin_ratio", ScaleValue::u128(1)),
            ("max_active_orders", ScaleValue::u128(1)),
            ("is_quote_market", ScaleValue::bool(false)),
            ("taker_fee_rate", ScaleValue::u128(0)),
            ("maker_fee_rate", ScaleValue::i128(0)),
            (
                "order_spec",
                ScaleValue::named_composite([
                    ("min_qty", ScaleValue::u128(11)),
                    ("tick_size", ScaleValue::u128(2)),
                    ("step_size", ScaleValue::u128(3)),
                    ("min_notional", ScaleValue::u128(50)),
                ]),
            ),
            (
                "limit_order_guard",
                ScaleValue::named_composite([
                    ("limit_long", ScaleValue::u128(1)),
                    ("limit_short", ScaleValue::u128(1)),
                ]),
            ),
            ("open_interest", ScaleValue::u128(0)),
            ("long_open_pos_num", ScaleValue::u128(0)),
            ("short_open_pos_num", ScaleValue::u128(0)),
            ("base_interest_rate", fixed_i128()),
            ("impact_margin_value", ScaleValue::u128(0)),
            ("funding_rate_change_cap", fixed_i128()),
            ("funding_rate_change_floor", fixed_i128()),
            ("funding_rate_clamp_upper_bound", fixed_i128()),
            ("funding_rate_clamp_lower_bound", fixed_i128()),
            (
                "liquidation_spec",
                ScaleValue::named_composite([
                    ("liquidation_duration", ScaleValue::u128(0)),
                    ("liquidity_bucket_slippage_step", ScaleValue::u128(0)),
                    ("liquidity_bucket_slippage_limit", ScaleValue::u128(0)),
                    ("liquidity_dust_value", ScaleValue::u128(0)),
                    (
                        "liquidation_fee_rate",
                        ScaleValue::unnamed_composite([
                            ScaleValue::u128(0),
                            ScaleValue::u128(0),
                            ScaleValue::u128(0),
                            ScaleValue::u128(0),
                        ]),
                    ),
                ]),
            ),
            ("deployer_spec", ScaleValue::unnamed_variant("None", [])),
            ("is_paused", ScaleValue::bool(false)),
        ]);
        let metadata: Value = serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap();
        let metadata_bytes = decode_storage_hex(metadata["result"].as_str().unwrap()).unwrap();
        let metadata = subxt_core::Metadata::decode_all(&mut &metadata_bytes[..]).unwrap();
        let value_type = metadata
            .pallet_by_name("PerpMarket")
            .unwrap()
            .storage()
            .unwrap()
            .entry_by_name("PerpMarkets")
            .unwrap()
            .entry_type()
            .value_ty();
        let mut bytes = Vec::new();
        subxt_core::ext::scale_value::scale::encode_as_type(
            &market,
            value_type,
            metadata.types(),
            &mut bytes,
        )
        .unwrap();
        format!("0x{}", hex::encode(bytes))
    }

    #[rstest]
    fn decodes_complete_finalized_perp_market_storage() {
        assert_eq!(
            perp_market_storage_key(3),
            "0xb052a07884e121f56b64aa711c766532bdc7a97672ba816c4074df5f464124630300",
        );
        let snapshot = DeepXReadOnlyRuntimeSnapshot::built_in_testnet().unwrap();
        let metadata = snapshot.metadata();
        let encoded = encoded_perp_market_storage();
        assert_eq!(
            decode_finalized_perp_market_storage(&encoded, metadata, 3),
            Some((11, 2, 3, 50)),
        );
        assert_eq!(
            decode_finalized_perp_market_storage(&encoded, metadata, 4),
            None,
        );
        assert_eq!(
            decode_finalized_perp_market_storage(&format!("{encoded}00"), metadata, 3),
            None,
        );
        assert_eq!(
            decode_finalized_perp_market_storage("0x", metadata, 3),
            None,
        );
    }

    #[rstest]
    fn decodes_only_matching_positive_perp_order_specs() {
        let encoded = encoded_perp_market(3, (11, 2, 3));
        assert_eq!(
            decode_finalized_perp_order_spec(&encoded, 3),
            Some((11, 2, 3))
        );
        assert_eq!(decode_finalized_perp_order_spec(&encoded, 4), None);
        assert_eq!(decode_finalized_perp_order_spec("0x", 3), None);
        assert_eq!(decode_finalized_perp_order_spec("0xzz", 3), None);
        for spec in [(0, 2, 3), (11, 0, 3), (11, 2, 0)] {
            assert_eq!(
                decode_finalized_perp_order_spec(&encoded_perp_market(3, spec), 3),
                None
            );
        }
        assert_eq!(
            decode_finalized_perp_order_spec(&format!("{encoded}00"), 3),
            None
        );
    }

    #[rstest]
    #[case::matching((11, 2, 3), true, false, false, false)]
    #[case::mismatched((12, 2, 3), false, false, false, false)]
    #[case::inconsistent_timestamp((11, 2, 3), false, true, false, false)]
    #[case::timestamp_changes_during_read((11, 2, 3), false, false, true, false)]
    #[case::market_changes_during_read((11, 2, 3), false, false, false, true)]
    #[tokio::test]
    async fn finalized_perp_order_spec_is_pinned_to_approved_checkpoint(
        #[case] evm_spec: (u128, u128, u128),
        #[case] matching: bool,
        #[case] timestamp_mismatch: bool,
        #[case] late_timestamp_change: bool,
        #[case] late_market_change: bool,
    ) {
        let genesis: Value = serde_json::from_str(GENESIS_FIXTURE).unwrap();
        let finalized_head: Value = serde_json::from_str(FINALIZED_HEAD_FIXTURE).unwrap();
        let header = json!({
            "jsonrpc": "2.0", "id": 1,
            "result": { "number": FINALIZED_BLOCK_NUMBER_HEX }
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&server_calls);
                let genesis = genesis.clone();
                let finalized_head = finalized_head.clone();
                let header = header.clone();
                async move {
                    if let Some(response) = read_only_runtime_rpc_response(&request) {
                        return Json(response);
                    }
                    let index = calls.fetch_add(1, Ordering::Relaxed);
                    let response = match index {
                        0 => genesis,
                        1 => finalized_head,
                        2 => header,
                        3 => {
                            assert_eq!(request["method"], "state_getStorage");
                            assert_eq!(
                                request["params"],
                                json!([timestamp_now_storage_key(), FINALIZED_HASH]),
                            );
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": format!("0x{}", hex::encode(1_700_000_000_123u64.encode())),
                            })
                        }
                        4 => {
                            assert_eq!(request["method"], "chain_getBlock");
                            assert_eq!(request["params"], json!([FINALIZED_HASH]));
                            let mut inherent = vec![4, 2, 0];
                            inherent.extend(Compact(
                                1_700_000_000_123u64 + u64::from(timestamp_mismatch),
                            ).encode());
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": {
                                    "block": {
                                        "header": {"number": FINALIZED_BLOCK_NUMBER_HEX},
                                        "extrinsics": [format!("0x{}", hex::encode(inherent.encode()))],
                                    },
                                },
                            })
                        }
                        5 => {
                            assert_eq!(request["method"], "state_queryStorageAt");
                            assert_eq!(
                                request["params"],
                                json!([[timestamp_now_storage_key()], FINALIZED_HASH]),
                            );
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": [{
                                    "block": FINALIZED_HASH,
                                    "changes": [[
                                        timestamp_now_storage_key(),
                                        format!("0x{}", hex::encode(1_700_000_000_123u64.encode())),
                                    ]],
                                }],
                            })
                        }
                        6 => {
                            assert_eq!(request["method"], "eth_call");
                            assert_eq!(request["params"][0]["to"], DEEPX_ETH_CALL_PROBE_ADDRESS);
                            assert_eq!(request["params"][1], FINALIZED_BLOCK_NUMBER_HEX);
                            json!({"jsonrpc": "2.0", "id": 1, "result": "0x"})
                        }
                        7 => {
                            assert_eq!(request["method"], "eth_call");
                            assert_eq!(request["params"][1], "0xffffffff");
                            json!({"jsonrpc": "2.0", "id": 1,
                                "error": {"code": -32000, "message": "unknown block"}})
                        }
                        8 => {
                            assert_eq!(request["method"], "eth_call");
                            assert_eq!(
                                request["params"][0]["to"],
                                DEEPX_PERP_MARKET_PRECOMPILE_ADDRESS.to_string(),
                            );
                            assert_eq!(request["params"][1], FINALIZED_BLOCK_NUMBER_HEX);
                            let calldata = hex::decode(
                                request["params"][0]["data"]
                                    .as_str()
                                    .unwrap()
                                    .trim_start_matches("0x"),
                            )
                            .unwrap();
                            assert_eq!(
                                perpMarketsCall::abi_decode(&calldata).unwrap().market_id,
                                3
                            );
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": encoded_perp_market(3, evm_spec),
                            })
                        }
                        9 => {
                            assert_eq!(request["method"], "state_getStorage");
                            assert_eq!(request["params"][0], perp_market_storage_key(3));
                            assert_eq!(request["params"][1], FINALIZED_HASH);
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": encoded_perp_market_storage(),
                            })
                        }
                        10 => {
                            assert_eq!(request["method"], "state_getStorage");
                            assert_eq!(request["params"][0], perp_market_storage_key(3));
                            assert_eq!(request["params"][1], FINALIZED_HASH);
                            let mut stored = encoded_perp_market_storage();
                            if late_market_change {
                                stored.push_str("00");
                            }
                            json!({"jsonrpc": "2.0", "id": 1, "result": stored})
                        }
                        11 => {
                            assert_eq!(request["method"], "state_getStorage");
                            assert_eq!(
                                request["params"],
                                json!([timestamp_now_storage_key(), FINALIZED_HASH]),
                            );
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": format!(
                                    "0x{}",
                                    hex::encode((1_700_000_000_123u64
                                        + u64::from(late_timestamp_change)).encode()),
                                ),
                            })
                        }
                        _ => panic!("unexpected finalized Perp market request"),
                    };
                    Json(response)
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);

        let result = observe_finalized_perp_order_spec_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            3,
        )
        .await;

        if timestamp_mismatch || late_timestamp_change {
            assert!(matches!(
                result,
                Err(DeepXFinalizedPerpOrderSpecError::Checkpoint(source))
                    if matches!(*source, DeepXFinalizedAccountError::BlockTimestampMismatch { .. })
            ));
        } else if late_market_change {
            assert!(matches!(
                result,
                Err(DeepXFinalizedPerpOrderSpecError::InvalidEvidence(3))
            ));
        } else if matching {
            let evidence = result.unwrap();
            assert_eq!(
                evidence.runtime_identity().spec_version,
                DEEPX_SPEC_376_VERSION
            );
            assert_eq!(evidence.checkpoint().block_number(), FINALIZED_BLOCK_NUMBER);
            assert_eq!(evidence.market_id(), 3);
            assert_eq!(evidence.min_order_size(), 11);
            assert_eq!(evidence.tick_size(), 2);
            assert_eq!(evidence.step_size(), 3);
            assert_eq!(evidence.min_notional(), 50);
        } else {
            assert!(matches!(
                result,
                Err(DeepXFinalizedPerpOrderSpecError::InvalidEvidence(3))
            ));
        }
        assert_eq!(
            calls.load(Ordering::Relaxed),
            if timestamp_mismatch {
                5
            } else if !matching && !late_timestamp_change && !late_market_change {
                10
            } else if late_market_change {
                11
            } else {
                12
            },
        );
    }

    #[rstest]
    fn decodes_only_matching_complete_spot_market_storage() {
        let snapshot = DeepXReadOnlyRuntimeSnapshot::built_in_testnet().unwrap();
        let metadata = snapshot.metadata();
        let storage: Value = serde_json::from_str(SPOT_MARKET_STORAGE_SPEC_376_FIXTURE).unwrap();
        let encoded = storage["result"].as_str().unwrap();
        let pair = hex::decode("9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37")
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(
            decode_finalized_spot_market_storage(encoded, metadata, pair,),
            Some((100_000_000_000_000, 10_000, 100_000_000_000_000)),
        );
        for encoded in [
            "0x".to_string(),
            format!("{encoded}00"),
            encoded.replace("00407a10f35a00000000000000000000", &"00".repeat(16)),
        ] {
            assert_eq!(
                decode_finalized_spot_market_storage(&encoded, metadata, pair),
                None
            );
        }
        assert_eq!(
            decode_finalized_spot_market_storage(encoded, metadata, [0x33; 32],),
            None,
        );
    }

    #[tokio::test]
    async fn spec_376_read_only_snapshot_authorizes_finalized_margin_evidence() {
        let subaccount = [0x11; 20];
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&server_calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    let response = match request["method"].as_str().unwrap() {
                        "chain_getBlockHash" => serde_json::from_str(GENESIS_FIXTURE).unwrap(),
                        "chain_getFinalizedHead" => {
                            serde_json::from_str(FINALIZED_HEAD_FIXTURE).unwrap()
                        }
                        "chain_getHeader" => json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "result": { "number": FINALIZED_BLOCK_NUMBER_HEX },
                        }),
                        "state_call" => {
                            assert_eq!(request["params"][0], DEEPX_MARGIN_RUNTIME_API_METHOD);
                            assert_eq!(request["params"][2], FINALIZED_HASH);
                            let encoded = request["params"][1].as_str().unwrap();
                            let bytes = hex::decode(encoded.trim_start_matches("0x")).unwrap();
                            let (accounts, direction) =
                                <(Vec<[u8; 20]>, u8)>::decode(&mut bytes.as_slice()).unwrap();
                            assert_eq!(accounts, vec![subaccount]);
                            let values = match direction {
                                0 => (1_000_000_u128, 250_000_u128),
                                1 => (900_000_u128, 100_000_u128),
                                _ => panic!("unexpected margin direction"),
                            };
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": format!(
                                    "0x{}",
                                    hex::encode(BTreeMap::from([(subaccount, values)]).encode()),
                                ),
                            })
                        }
                        "state_getRuntimeVersion" | "state_getMetadata" => {
                            read_only_runtime_rpc_response(&request).unwrap()
                        }
                        _ => panic!("unexpected read-only finalized margin request"),
                    };
                    Json(response)
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);

        let evidence = observe_finalized_margin_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            subaccount,
        )
        .await
        .unwrap();

        assert_eq!(
            evidence.runtime_identity().spec_version,
            DEEPX_SPEC_376_VERSION
        );
        assert_eq!(evidence.checkpoint().block_number(), 19_592_700);
        assert_eq!(evidence.initial().total_collateral(), 1_000_000);
        assert_eq!(evidence.maintenance().total_margin_required(), 100_000);
        assert_eq!(calls.load(Ordering::Relaxed), 7);
    }

    #[tokio::test]
    async fn spec_376_read_only_snapshot_authorizes_finalized_perp_positions() {
        let subaccount = [0x11; 20];
        let maximum_key = subaccount_max_perp_positions_storage_key();
        let positions_key = user_perp_positions_storage_key(subaccount);
        let encoded_positions = format!(
            "0x{}",
            hex::encode(vec![perp_position(subaccount, 3)].encode()),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&server_calls);
                let maximum_key = maximum_key.clone();
                let positions_key = positions_key.clone();
                let encoded_positions = encoded_positions.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    let response = match request["method"].as_str().unwrap() {
                        "chain_getBlockHash" => serde_json::from_str(GENESIS_FIXTURE).unwrap(),
                        "chain_getFinalizedHead" => {
                            serde_json::from_str(FINALIZED_HEAD_FIXTURE).unwrap()
                        }
                        "chain_getHeader" => json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "result": { "number": FINALIZED_BLOCK_NUMBER_HEX },
                        }),
                        "state_queryStorageAt" => {
                            assert_eq!(request["params"][1], FINALIZED_HASH);
                            assert_eq!(request["params"][0], json!([maximum_key, positions_key]),);
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": [{
                                    "block": FINALIZED_HASH,
                                    "changes": [
                                        [maximum_key, Value::Null],
                                        [positions_key, encoded_positions],
                                    ],
                                }],
                            })
                        }
                        "state_getRuntimeVersion" | "state_getMetadata" => {
                            read_only_runtime_rpc_response(&request).unwrap()
                        }
                        _ => panic!("unexpected read-only finalized position request"),
                    };
                    Json(response)
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);

        let evidence = observe_finalized_perp_positions_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            subaccount,
        )
        .await
        .unwrap();

        assert_eq!(
            evidence.runtime_identity().spec_version,
            DEEPX_SPEC_376_VERSION
        );
        assert_eq!(evidence.checkpoint().block_number(), 19_592_700);
        assert_eq!(evidence.runtime_max_positions(), 8);
        assert_eq!(evidence.positions().len(), 1);
        assert_eq!(evidence.positions()[0].market_id(), 3);
        assert_eq!(evidence.positions()[0].owner(), subaccount);
        assert_eq!(calls.load(Ordering::Relaxed), 6);
    }

    #[tokio::test]
    async fn spec_376_read_only_snapshot_corroborates_finalized_perp_orders() {
        let subaccount = [0x11; 20];
        let order = perp_order(subaccount, 3, 42);
        let index_key = active_perp_orders_storage_key(subaccount);
        let order_key = perp_order_info_storage_key(subaccount, order.order_id);
        let encoded_index = format!(
            "0x{}",
            hex::encode(
                BTreeMap::from([(order.market_id, vec![active_perp_order_index_entry(&order)],)])
                    .encode(),
            ),
        );
        let encoded_order = format!("0x{}", hex::encode(order.encode()));
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&server_calls);
                let index_key = index_key.clone();
                let order_key = order_key.clone();
                let encoded_index = encoded_index.clone();
                let encoded_order = encoded_order.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    let response = match request["method"].as_str().unwrap() {
                        "chain_getBlockHash" => serde_json::from_str(GENESIS_FIXTURE).unwrap(),
                        "chain_getFinalizedHead" => {
                            serde_json::from_str(FINALIZED_HEAD_FIXTURE).unwrap()
                        }
                        "chain_getHeader" => json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "result": { "number": FINALIZED_BLOCK_NUMBER_HEX },
                        }),
                        "state_queryStorageAt" => {
                            assert_eq!(request["params"][1], FINALIZED_HASH);
                            let keys = request["params"][0].as_array().unwrap();
                            let (key, value) = if keys == &[json!(index_key.clone())] {
                                (index_key, encoded_index)
                            } else {
                                assert_eq!(keys, &[json!(order_key.clone())]);
                                (order_key, encoded_order)
                            };
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": [{
                                    "block": FINALIZED_HASH,
                                    "changes": [[key, value]],
                                }],
                            })
                        }
                        "state_getRuntimeVersion" | "state_getMetadata" => {
                            read_only_runtime_rpc_response(&request).unwrap()
                        }
                        _ => panic!("unexpected read-only finalized order request"),
                    };
                    Json(response)
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);

        let evidence = observe_finalized_perp_orders_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            subaccount,
        )
        .await
        .unwrap();

        assert_eq!(
            evidence.runtime_identity().spec_version,
            DEEPX_SPEC_376_VERSION
        );
        assert_eq!(evidence.checkpoint().block_number(), 19_592_700);
        assert_eq!(evidence.orders().len(), 1);
        assert_eq!(evidence.orders()[0].order_id(), 42);
        assert_eq!(evidence.orders()[0].market_id(), 3);
        assert_eq!(evidence.orders()[0].owner(), subaccount);
        assert_eq!(calls.load(Ordering::Relaxed), 7);
    }

    #[tokio::test]
    async fn spec_376_read_only_snapshot_corroborates_finalized_spot_orders() {
        let subaccount = [0x11; 20];
        let pair = [0x22; 32];
        let order = spot_order(subaccount, pair, 42);
        let index_key = active_spot_orders_storage_key(subaccount);
        let order_key = spot_order_info_storage_key(subaccount, order.order_id);
        let encoded_index = format!(
            "0x{}",
            hex::encode(BTreeMap::from([(pair, vec![(order.order_id, order.is_buy)])]).encode()),
        );
        let encoded_order = format!("0x{}", hex::encode(order.encode()));
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&server_calls);
                let index_key = index_key.clone();
                let order_key = order_key.clone();
                let encoded_index = encoded_index.clone();
                let encoded_order = encoded_order.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    let response = match request["method"].as_str().unwrap() {
                        "chain_getBlockHash" => serde_json::from_str(GENESIS_FIXTURE).unwrap(),
                        "chain_getFinalizedHead" => {
                            serde_json::from_str(FINALIZED_HEAD_FIXTURE).unwrap()
                        }
                        "chain_getHeader" => json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "result": { "number": FINALIZED_BLOCK_NUMBER_HEX },
                        }),
                        "state_queryStorageAt" => {
                            assert_eq!(request["params"][1], FINALIZED_HASH);
                            let keys = request["params"][0].as_array().unwrap();
                            let changes = if keys == &[json!(index_key.clone())] {
                                vec![json!([index_key, encoded_index])]
                            } else {
                                assert_eq!(
                                    keys,
                                    &[json!(index_key.clone()), json!(order_key.clone())],
                                );
                                vec![
                                    json!([index_key, encoded_index]),
                                    json!([order_key, encoded_order]),
                                ]
                            };
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": [{
                                    "block": FINALIZED_HASH,
                                    "changes": changes,
                                }],
                            })
                        }
                        "state_getRuntimeVersion" | "state_getMetadata" => {
                            read_only_runtime_rpc_response(&request).unwrap()
                        }
                        _ => panic!("unexpected read-only finalized Spot order request"),
                    };
                    Json(response)
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);

        let evidence = observe_finalized_spot_orders_by_id_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            subaccount,
            &[42],
        )
        .await
        .unwrap();

        assert_eq!(
            evidence.runtime_identity().spec_version,
            DEEPX_SPEC_376_VERSION
        );
        assert_eq!(evidence.checkpoint().block_number(), 19_592_700);
        assert_eq!(evidence.orders().len(), 1);
        assert_eq!(evidence.active_order_ids(), &[42]);
        assert_eq!(evidence.orders()[0].owner(), subaccount);
        assert_eq!(evidence.orders()[0].pair(), pair);
        assert_eq!(evidence.orders()[0].is_buy(), order.is_buy());
        assert_eq!(evidence.orders()[0].quote_amount(), order.quote_amount());
        assert_eq!(evidence.orders()[0].base_amount(), order.base_amount());
        assert_eq!(calls.load(Ordering::Relaxed), 6);
    }

    #[tokio::test]
    async fn spec_376_read_only_snapshot_pins_complete_account_evidence_to_one_checkpoint() {
        let subaccount = [0x11; 20];
        let timestamp_ms = 1_700_000_000_123_u64;
        let maximum_key = subaccount_max_perp_positions_storage_key();
        let positions_key = user_perp_positions_storage_key(subaccount);
        let orders_key = active_perp_orders_storage_key(subaccount);
        let spot_orders_key = active_spot_orders_storage_key(subaccount);
        let spot_active_order = spot_order(subaccount, [0x22; 32], 77);
        let spot_order_key = spot_order_info_storage_key(subaccount, spot_active_order.order_id);
        let spot_order_index = format!(
            "0x{}",
            hex::encode(
                BTreeMap::from([(
                    spot_active_order.pair,
                    vec![(spot_active_order.order_id, spot_active_order.is_buy)],
                )])
                .encode(),
            ),
        );
        let spot_order_detail = format!("0x{}", hex::encode(spot_active_order.encode()));
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&server_calls);
                let maximum_key = maximum_key.clone();
                let positions_key = positions_key.clone();
                let orders_key = orders_key.clone();
                let spot_orders_key = spot_orders_key.clone();
                let spot_order_key = spot_order_key.clone();
                let spot_order_index = spot_order_index.clone();
                let spot_order_detail = spot_order_detail.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    let response = match request["method"].as_str().unwrap() {
                        "chain_getBlockHash" => serde_json::from_str(GENESIS_FIXTURE).unwrap(),
                        "chain_getBlock" => {
                            assert_eq!(request["params"], json!([FINALIZED_HASH]));
                            let mut call = vec![4, 2, 0];
                            call.extend(Compact(timestamp_ms).encode());
                            json!({
                                "jsonrpc": "2.0", "id": 1,
                                "result": {"block": {
                                    "header": {"number": FINALIZED_BLOCK_NUMBER_HEX},
                                    "extrinsics": [format!("0x{}", hex::encode(call.encode()))],
                                }},
                            })
                        }
                        "chain_getFinalizedHead" => {
                            serde_json::from_str(FINALIZED_HEAD_FIXTURE).unwrap()
                        }
                        "chain_getHeader" => json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "result": { "number": FINALIZED_BLOCK_NUMBER_HEX },
                        }),
                        "state_getStorage" => {
                            assert_eq!(
                                request["params"],
                                json!([timestamp_now_storage_key(), FINALIZED_HASH])
                            );
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": format!(
                                    "0x{}",
                                    hex::encode(timestamp_ms.to_le_bytes())
                                ),
                            })
                        }
                        "eth_call" => {
                            assert_eq!(request["params"][0]["to"], DEEPX_ETH_CALL_PROBE_ADDRESS);
                            assert_eq!(request["params"][0]["data"], "0x");
                            if request["params"][1] == FINALIZED_BLOCK_NUMBER_HEX {
                                json!({"jsonrpc": "2.0", "id": 1, "result": "0x"})
                            } else {
                                assert_eq!(request["params"][1], "0xffffffff");
                                json!({
                                    "jsonrpc": "2.0", "id": 1,
                                    "error": {"code": -32000, "message": "unknown block"},
                                })
                            }
                        }
                        "state_call" => {
                            let method = request["params"][0].as_str().unwrap();
                            assert_eq!(request["params"][2], FINALIZED_HASH);
                            let result = if method == DEEPX_MARGIN_RUNTIME_API_METHOD {
                                let encoded = request["params"][1].as_str().unwrap();
                                let bytes = hex::decode(encoded.trim_start_matches("0x")).unwrap();
                                let (accounts, direction) =
                                    <(Vec<[u8; 20]>, u8)>::decode(&mut bytes.as_slice()).unwrap();
                                assert_eq!(accounts, vec![subaccount]);
                                let values = match direction {
                                    0 => (1_000_000_u128, 250_000_u128),
                                    1 => (900_000_u128, 100_000_u128),
                                    _ => panic!("unexpected margin direction"),
                                };
                                BTreeMap::from([(subaccount, values)]).encode()
                            } else {
                                assert_eq!(method, DEEPX_LENDING_POSITIONS_RUNTIME_API_METHOD);
                                BTreeMap::<[u8; 20], BTreeMap<u8, EncodedLendingPosition>>::new()
                                    .encode()
                            };
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": format!("0x{}", hex::encode(result)),
                            })
                        }
                        "state_queryStorageAt" => {
                            assert_eq!(request["params"][1], FINALIZED_HASH);
                            let keys = request["params"][0].as_array().unwrap();
                            let changes = if keys == &[json!(timestamp_now_storage_key())] {
                                vec![json!([
                                    timestamp_now_storage_key(),
                                    format!("0x{}", hex::encode(timestamp_ms.to_le_bytes())),
                                ])]
                            } else if keys
                                == &[json!(maximum_key.clone()), json!(positions_key.clone())]
                            {
                                vec![
                                    json!([maximum_key, Value::Null]),
                                    json!([positions_key, Value::Null]),
                                ]
                            } else if keys == &[json!(orders_key.clone())] {
                                vec![json!([orders_key, Value::Null])]
                            } else if keys == &[json!(spot_orders_key.clone())] {
                                vec![json!([spot_orders_key, spot_order_index])]
                            } else {
                                assert_eq!(
                                    keys,
                                    &[
                                        json!(spot_orders_key.clone()),
                                        json!(spot_order_key.clone()),
                                    ],
                                );
                                vec![
                                    json!([spot_orders_key, spot_order_index]),
                                    json!([spot_order_key, spot_order_detail]),
                                ]
                            };
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": [{
                                    "block": FINALIZED_HASH,
                                    "changes": changes,
                                }],
                            })
                        }
                        "state_getRuntimeVersion" | "state_getMetadata" => {
                            read_only_runtime_rpc_response(&request).unwrap()
                        }
                        _ => panic!("unexpected complete finalized account request"),
                    };
                    Json(response)
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);

        let evidence = observe_finalized_account_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            subaccount,
        )
        .await
        .unwrap();

        assert_eq!(
            evidence.runtime_identity().spec_version,
            DEEPX_SPEC_376_VERSION
        );
        assert_eq!(evidence.checkpoint().block_number(), FINALIZED_BLOCK_NUMBER);
        assert_eq!(evidence.subaccount(), subaccount);
        assert_eq!(evidence.timestamp_ms(), timestamp_ms);
        assert_eq!(evidence.margin().checkpoint(), evidence.checkpoint());
        assert_eq!(evidence.lending().checkpoint(), evidence.checkpoint());
        assert_eq!(
            evidence.perp_positions().checkpoint(),
            evidence.checkpoint()
        );
        assert_eq!(evidence.perp_orders().checkpoint(), evidence.checkpoint());
        assert_eq!(evidence.spot_orders().checkpoint(), evidence.checkpoint());
        assert_eq!(evidence.margin().initial().total_collateral(), 1_000_000);
        assert_eq!(
            evidence.margin().maintenance().total_margin_required(),
            100_000
        );
        assert!(evidence.lending().positions().is_empty());
        assert_eq!(evidence.perp_positions().runtime_max_positions(), 8);
        assert!(evidence.perp_positions().positions().is_empty());
        assert!(evidence.perp_orders().orders().is_empty());
        assert_eq!(evidence.spot_orders().active_order_ids(), &[77]);
        assert_eq!(evidence.spot_orders().orders().len(), 1);
        assert_eq!(evidence.spot_orders().orders()[0].pair(), [0x22; 32]);
        assert_eq!(evidence.spot_orders().orders()[0].owner(), subaccount);
        assert_eq!(calls.load(Ordering::Relaxed), 18);
    }

    #[tokio::test]
    async fn account_batch_uses_one_finalized_checkpoint_for_all_subaccounts() {
        let subaccounts = [[0x11; 20], [0x22; 20]];
        let finalized_heads = Arc::new(AtomicUsize::new(0));
        let server_heads = Arc::clone(&finalized_heads);
        let timestamp_mode = Arc::new(AtomicUsize::new(0));
        let server_timestamp_mode = Arc::clone(&timestamp_mode);
        let timestamp_reads = Arc::new(AtomicUsize::new(0));
        let server_timestamp_reads = Arc::clone(&timestamp_reads);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let heads = Arc::clone(&server_heads);
                let timestamp_mode = Arc::clone(&server_timestamp_mode);
                let timestamp_reads = Arc::clone(&server_timestamp_reads);
                async move {
                    let result = match request["method"].as_str().unwrap() {
                        "chain_getBlockHash" => {
                            return Json(serde_json::from_str::<Value>(GENESIS_FIXTURE).unwrap());
                        }
                        "chain_getBlock" => {
                            assert_eq!(request["params"], json!([FINALIZED_HASH]));
                            let mode = timestamp_mode.load(Ordering::Relaxed);
                            let offset = if mode == 2 {
                                timestamp_reads.load(Ordering::Relaxed).saturating_sub(1) / 2
                            } else {
                                0
                            };
                            let mut call = vec![4, 2, 0];
                            call.extend(Compact(1_700_000_000_123_u64 + offset as u64).encode());
                            json!({"block": {
                                "header": {"number": FINALIZED_BLOCK_NUMBER_HEX},
                                "extrinsics": [format!("0x{}", hex::encode(call.encode()))],
                            }})
                        }
                        "chain_getFinalizedHead" => {
                            heads.fetch_add(1, Ordering::Relaxed);
                            return Json(
                                serde_json::from_str::<Value>(FINALIZED_HEAD_FIXTURE).unwrap(),
                            );
                        }
                        "chain_getHeader" => {
                            json!({ "number": FINALIZED_BLOCK_NUMBER_HEX })
                        }
                        "state_getStorage" => {
                            assert_eq!(
                                request["params"],
                                json!([timestamp_now_storage_key(), FINALIZED_HASH])
                            );
                            let mode = timestamp_mode.load(Ordering::Relaxed);
                            let read = timestamp_reads.fetch_add(1, Ordering::Relaxed);
                            let offset = match mode {
                                0 | 4 | 5 | 6 | 7 => 0,
                                1 => read % 2,
                                2 => read / 2,
                                3 => 1,
                                _ => unreachable!(),
                            };
                            let timestamp_ms = 1_700_000_000_123_u64 + offset as u64;
                            json!(format!("0x{}", hex::encode(timestamp_ms.to_le_bytes())))
                        }
                        "state_call" => {
                            let method = request["params"][0].as_str().unwrap();
                            assert_eq!(request["params"][2], FINALIZED_HASH);
                            let bytes = hex::decode(
                                request["params"][1]
                                    .as_str()
                                    .unwrap()
                                    .trim_start_matches("0x"),
                            )
                            .unwrap();
                            let encoded = if method == DEEPX_MARGIN_RUNTIME_API_METHOD {
                                let (accounts, _): (Vec<[u8; 20]>, u8) =
                                    Decode::decode(&mut bytes.as_slice()).unwrap();
                                assert_eq!(accounts.len(), 1);
                                if subaccounts.contains(&accounts[0]) {
                                    BTreeMap::from([(accounts[0], (0_u128, 0_u128))]).encode()
                                } else {
                                    BTreeMap::<[u8; 20], (u128, u128)>::new().encode()
                                }
                            } else {
                                assert_eq!(method, DEEPX_LENDING_POSITIONS_RUNTIME_API_METHOD);
                                BTreeMap::<[u8; 20], BTreeMap<u8, EncodedLendingPosition>>::new()
                                    .encode()
                            };
                            json!(format!("0x{}", hex::encode(encoded)))
                        }
                        "eth_call" => {
                            assert_eq!(request["params"][0]["to"], DEEPX_ETH_CALL_PROBE_ADDRESS);
                            assert_eq!(request["params"][0]["data"], "0x");
                            if request["params"][1] == FINALIZED_BLOCK_NUMBER_HEX {
                                json!("0x")
                            } else {
                                assert_eq!(request["params"][1], "0xffffffff");
                                if timestamp_mode.load(Ordering::Relaxed) == 7 {
                                    json!("0x")
                                } else {
                                    return Json(json!({
                                        "jsonrpc": "2.0", "id": 1,
                                        "error": {"code": -32000, "message": "unknown block"},
                                    }));
                                }
                            }
                        }
                        "state_queryStorageAt" => {
                            assert_eq!(request["params"][1], FINALIZED_HASH);
                            let mode = timestamp_mode.load(Ordering::Relaxed);
                            let changes = request["params"][0]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|key| {
                                    if key == &json!(timestamp_now_storage_key()) {
                                        let offset = match mode {
                                            2 => {
                                                timestamp_reads
                                                    .load(Ordering::Relaxed)
                                                    .saturating_sub(1)
                                                    / 2
                                            }
                                            4 => 1,
                                            _ => 0,
                                        };
                                        let timestamp_ms = 1_700_000_000_123_u64 + offset as u64;
                                        json!([
                                            key,
                                            format!(
                                                "0x{}",
                                                hex::encode(timestamp_ms.to_le_bytes())
                                            )
                                        ])
                                    } else {
                                        json!([key, Value::Null])
                                    }
                                })
                                .collect::<Vec<_>>();
                            json!([{ "block": FINALIZED_HASH, "changes": changes }])
                        }
                        "state_getRuntimeVersion" | "state_getMetadata" => {
                            return Json(read_only_runtime_rpc_response(&request).unwrap());
                        }
                        method => panic!("unexpected account batch RPC method: {method}"),
                    };
                    Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);

        let evidence = observe_finalized_accounts_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &subaccounts,
        )
        .await
        .unwrap();

        assert_eq!(evidence.len(), 2);
        assert_eq!(finalized_heads.load(Ordering::Relaxed), 1);
        for (account, expected) in evidence.iter().zip(subaccounts) {
            assert_eq!(account.subaccount(), expected);
            assert_eq!(account.checkpoint(), evidence[0].checkpoint());
            assert_eq!(account.runtime_identity(), evidence[0].runtime_identity());
            assert_eq!(account.margin().checkpoint(), evidence[0].checkpoint());
            assert_eq!(account.lending().checkpoint(), evidence[0].checkpoint());
            assert_eq!(
                account.perp_positions().checkpoint(),
                evidence[0].checkpoint()
            );
            assert_eq!(account.perp_orders().checkpoint(), evidence[0].checkpoint());
            assert_eq!(account.spot_orders().checkpoint(), evidence[0].checkpoint());
        }

        let error = observe_finalized_accounts_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &[subaccounts[0], [0x33; 20]],
        )
        .await
        .unwrap_err();
        assert!(matches!(
            &error,
            DeepXFinalizedAccountError::SubaccountObservation {
                subaccount,
                ..
            } if *subaccount == [0x33; 20],
        ));
        assert!(
            error
                .to_string()
                .contains(&format!("0x{}", "33".repeat(20)))
        );

        timestamp_mode.store(1, Ordering::Relaxed);
        timestamp_reads.store(0, Ordering::Relaxed);
        let error = observe_finalized_accounts_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &subaccounts,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            DeepXFinalizedAccountError::SubaccountObservation {
                subaccount,
                source,
            } if subaccount == subaccounts[0]
                && matches!(*source, DeepXFinalizedAccountError::InconsistentTimestamp { .. })
        ));

        timestamp_mode.store(2, Ordering::Relaxed);
        timestamp_reads.store(0, Ordering::Relaxed);
        let error = observe_finalized_accounts_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &subaccounts,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            DeepXFinalizedAccountError::InconsistentTimestamp {
                subaccount,
                expected_ms: 1_700_000_000_123,
                received_ms: 1_700_000_000_124,
            } if subaccount == subaccounts[1]
        ));

        timestamp_mode.store(3, Ordering::Relaxed);
        timestamp_reads.store(0, Ordering::Relaxed);
        let error = observe_finalized_accounts_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &subaccounts,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            DeepXFinalizedAccountError::SubaccountObservation {
                subaccount,
                source,
            } if subaccount == subaccounts[0]
                && matches!(*source, DeepXFinalizedAccountError::BlockTimestampMismatch { .. })
        ));

        timestamp_mode.store(4, Ordering::Relaxed);
        timestamp_reads.store(0, Ordering::Relaxed);
        let error = observe_finalized_accounts_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &subaccounts,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            DeepXFinalizedAccountError::SubaccountObservation {
                subaccount,
                source,
            } if subaccount == subaccounts[0]
                && matches!(*source, DeepXFinalizedAccountError::StorageQueryTimestampMismatch { .. })
        ));

        timestamp_mode.store(7, Ordering::Relaxed);
        timestamp_reads.store(0, Ordering::Relaxed);
        let error = observe_finalized_accounts_evidence(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &subaccounts,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            DeepXFinalizedAccountError::SubaccountObservation {
                subaccount,
                source,
            } if subaccount == subaccounts[0]
                && matches!(*source, DeepXFinalizedAccountError::EthCallBlockHashIgnored)
        ));
    }

    #[tokio::test]
    async fn account_batch_rejects_duplicate_subaccounts_before_rpc() {
        let endpoints = validated_endpoints("http://127.0.0.1:1");
        let capabilities = validated_capabilities(&endpoints);
        let subaccount = [0x11; 20];

        assert!(matches!(
            observe_finalized_accounts_evidence(
                &DeepXEnvironment::Testnet,
                &endpoints,
                &capabilities,
                &[subaccount, subaccount],
            )
            .await,
            Err(DeepXFinalizedAccountError::DuplicateSubaccount(value)) if value == subaccount,
        ));
        assert!(matches!(
            observe_finalized_accounts_evidence(
                &DeepXEnvironment::Testnet,
                &endpoints,
                &capabilities,
                &[],
            )
            .await,
            Err(DeepXFinalizedAccountError::EmptySubaccounts),
        ));
    }

    #[tokio::test]
    async fn rejects_finalized_account_evidence_without_every_capability_before_rpc() {
        let endpoints = validated_endpoints("http://127.0.0.1:1");
        for missing in [
            "state_call",
            "eth_call",
            "state_getStorage",
            "state_queryStorageAt",
            "chain_getBlock",
        ] {
            let mut capabilities = validated_capabilities(&endpoints);
            capabilities.watch.methods.remove(missing);

            assert!(matches!(
                observe_finalized_account_evidence(
                    &DeepXEnvironment::Testnet,
                    &endpoints,
                    &capabilities,
                    [0x11; 20],
                )
                .await,
                Err(DeepXFinalizedAccountError::RpcCapabilitiesMismatch),
            ));
        }
    }

    #[tokio::test]
    async fn rejects_finalized_perp_orders_without_query_storage_capability_before_rpc() {
        let endpoints = validated_endpoints("http://127.0.0.1:1");
        let mut capabilities = validated_capabilities(&endpoints);
        capabilities.watch.methods.remove("state_queryStorageAt");

        assert!(matches!(
            observe_finalized_perp_orders_evidence(
                &DeepXEnvironment::Testnet,
                &endpoints,
                &capabilities,
                [0x11; 20],
            )
            .await,
            Err(DeepXFinalizedPerpOrdersError::RpcCapabilitiesMismatch),
        ));
    }

    #[tokio::test]
    async fn rejects_finalized_perp_positions_without_query_storage_capability_before_rpc() {
        let endpoints = validated_endpoints("http://127.0.0.1:1");
        let mut capabilities = validated_capabilities(&endpoints);
        capabilities.watch.methods.remove("state_queryStorageAt");

        assert!(matches!(
            observe_finalized_perp_positions_evidence(
                &DeepXEnvironment::Testnet,
                &endpoints,
                &capabilities,
                [0x11; 20],
            )
            .await,
            Err(DeepXFinalizedPerpPositionsError::RpcCapabilitiesMismatch),
        ));
    }

    #[tokio::test]
    async fn observes_and_applies_snapshot_from_validated_watch_endpoint() {
        let calls = Arc::new(AtomicUsize::new(0));
        let watch_url =
            spawn_runtime_snapshot_server(DEEPX_SPEC_376_VERSION, Arc::clone(&calls)).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let service = DeepXRuntimeSnapshotService::new(approved_runtime_snapshot());

        let applied = observe_and_apply_approved_finalized_runtime_snapshot(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &service,
        )
        .await
        .unwrap();

        assert_eq!(applied.update(), DeepXRuntimeSnapshotUpdate::Unchanged);
        assert_eq!(applied.identity().spec_version, DEEPX_SPEC_376_VERSION);
        assert_eq!(calls.load(Ordering::Relaxed), 4);
        assert!(service.acquire().is_ok());
    }

    #[tokio::test]
    async fn observes_finalized_chain_time_with_built_in_runtime() {
        let calls = Arc::new(AtomicUsize::new(0));
        let timestamp_ms = 1_700_000_000_123u64;
        let watch_url = spawn_runtime_snapshot_server_with(
            366,
            1,
            serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap(),
            Some(json!(format!(
                "0x{}",
                hex::encode(timestamp_ms.to_le_bytes())
            ))),
            None,
            |_| {},
            Arc::clone(&calls),
        )
        .await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);
        let service = DeepXRuntimeSnapshotService::new(approved_runtime_snapshot());

        let evidence = observe_and_apply_finalized_chain_time(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &service,
        )
        .await
        .unwrap();
        assert_eq!(evidence.timestamp_ms(), timestamp_ms);
        assert_eq!(calls.load(Ordering::Relaxed), 7);
        assert!(service.acquire().is_ok());
    }

    #[tokio::test]
    async fn rejects_chain_time_that_disagrees_with_finalized_block() {
        let timestamp_ms = 1_700_000_000_123u64;
        let watch_url = spawn_runtime_snapshot_server_with(
            366,
            1,
            serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap(),
            Some(json!(format!(
                "0x{}",
                hex::encode(timestamp_ms.to_le_bytes())
            ))),
            Some("block_timestamp_mismatch"),
            |_| {},
            Arc::new(AtomicUsize::new(0)),
        )
        .await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let service = DeepXRuntimeSnapshotService::new(approved_runtime_snapshot());

        assert!(matches!(
            observe_and_apply_finalized_chain_time(
                &DeepXEnvironment::Testnet,
                &endpoints,
                &validated_capabilities(&endpoints),
                &service,
            )
            .await,
            Err(DeepXFinalizedChainTimeError::BlockEvidence(source))
                if matches!(*source, DeepXFinalizedAccountError::BlockTimestampMismatch { .. })
        ));
        assert!(matches!(
            service.acquire(),
            Err(DeepXRuntimeSnapshotServiceError::EvidenceInconsistent),
        ));
    }

    #[tokio::test]
    async fn observes_timestamp_from_exact_finalized_historical_block() {
        let calls = Arc::new(AtomicUsize::new(0));
        let timestamp_ms = 1_700_000_000_456u64;
        let block_hash = format!("0x{}", "11".repeat(32));
        let finalized_hash = format!("0x{}", "22".repeat(32));
        let router = Router::new().route(
            "/",
            post({
                let calls = Arc::clone(&calls);
                let block_hash = block_hash.clone();
                let finalized_hash = finalized_hash.clone();
                move |Json(request): Json<Value>| {
                    let calls = Arc::clone(&calls);
                    let block_hash = block_hash.clone();
                    let finalized_hash = finalized_hash.clone();
                    async move {
                        calls.fetch_add(1, Ordering::Relaxed);
                        let result = match request["method"].as_str().unwrap() {
                            "chain_getFinalizedHead" => json!(finalized_hash),
                            "chain_getHeader" => {
                                let number = if request["params"][0] == finalized_hash {
                                    "0x2a"
                                } else {
                                    "0x29"
                                };
                                json!({"number": number})
                            }
                            "chain_getBlockHash" => {
                                assert_eq!(request["params"][0], 41);
                                json!(block_hash)
                            }
                            "state_getStorage" => {
                                assert_eq!(request["params"][1], block_hash);
                                json!(format!("0x{}", hex::encode(timestamp_ms.to_le_bytes())))
                            }
                            method => panic!("unexpected RPC method {method}"),
                        };
                        Json(json!({"jsonrpc": "2.0", "id": request["id"], "result": result}))
                    }
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);

        let observed = observe_finalized_block_timestamp(&endpoints, &capabilities, 41)
            .await
            .unwrap();

        assert_eq!(observed, timestamp_ms);
        assert_eq!(calls.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn rejects_historical_timestamp_above_finalized_head_before_block_lookup() {
        let calls = Arc::new(AtomicUsize::new(0));
        let router = Router::new().route(
            "/",
            post({
                let calls = Arc::clone(&calls);
                move |Json(request): Json<Value>| {
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.fetch_add(1, Ordering::Relaxed);
                        let result = match request["method"].as_str().unwrap() {
                            "chain_getFinalizedHead" => json!(format!("0x{}", "22".repeat(32))),
                            "chain_getHeader" => json!({"number": "0x2a"}),
                            method => panic!("unexpected RPC method {method}"),
                        };
                        Json(json!({"jsonrpc": "2.0", "id": request["id"], "result": result}))
                    }
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);

        assert!(matches!(
            observe_finalized_block_timestamp(&endpoints, &capabilities, 43).await,
            Err(DeepXFinalizedBlockTimeError::NotFinalized {
                requested: 43,
                finalized: 42,
            }),
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn rejects_missing_finalized_chain_time_as_rpc_failure() {
        let calls = Arc::new(AtomicUsize::new(0));
        let watch_url = spawn_runtime_snapshot_server_with(
            366,
            1,
            serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap(),
            None,
            None,
            |_| {},
            Arc::clone(&calls),
        )
        .await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let service = DeepXRuntimeSnapshotService::new(approved_runtime_snapshot());

        assert!(matches!(
            observe_and_apply_finalized_chain_time(
                &DeepXEnvironment::Testnet,
                &endpoints,
                &validated_capabilities(&endpoints),
                &service,
            )
            .await,
            Err(DeepXFinalizedChainTimeError::Rpc(_))
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 5);
        assert!(service.acquire().is_ok());
    }

    #[tokio::test]
    async fn rejects_chain_time_without_watch_storage_capability_before_rpc() {
        let endpoints = validated_endpoints("http://127.0.0.1:1");
        let service = DeepXRuntimeSnapshotService::new(approved_runtime_snapshot());

        for missing in ["state_getStorage", "state_queryStorageAt", "chain_getBlock"] {
            let mut capabilities = validated_capabilities(&endpoints);
            capabilities.watch.methods.remove(missing);

            assert!(matches!(
                observe_and_apply_finalized_chain_time(
                    &DeepXEnvironment::Testnet,
                    &endpoints,
                    &capabilities,
                    &service,
                )
                .await,
                Err(DeepXFinalizedChainTimeError::RpcCapabilitiesMismatch),
            ));
            assert!(service.acquire().is_ok());
        }
    }

    #[rstest::rstest]
    #[case("")]
    #[case("42")]
    #[case("0x")]
    #[case("0x0000000000000000")]
    #[case("0x01000000000000")]
    #[case("0x010000000000000000")]
    #[case("0xgg00000000000000")]
    fn rejects_invalid_timestamp_now_storage(#[case] encoded: &str) {
        assert!(matches!(
            decode_timestamp_now(encoded),
            Err(DeepXFinalizedChainTimeError::InvalidStorage)
        ));
    }

    #[rstest::rstest]
    #[case("chain_getFinalizedHead")]
    #[case("chain_getHeader")]
    #[tokio::test]
    async fn pre_evidence_rpc_errors_do_not_block_signing(#[case] method: &'static str) {
        let service = DeepXRuntimeSnapshotService::new(approved_runtime_snapshot());
        let url = spawn_runtime_snapshot_server_with(
            366,
            1,
            serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap(),
            None,
            Some(method),
            |_| {},
            Arc::new(AtomicUsize::new(0)),
        )
        .await;
        assert!(matches!(
            observe_and_apply_approved_finalized_runtime_snapshot(
                &DeepXEnvironment::Testnet, &validated_endpoints(&url), &service,
            ).await,
            Err(DeepXRuntimeSnapshotRefreshError::Observation(
                DeepXRuntimeSnapshotObservationError::Rpc { method: failed, .. }
            )) if failed == method,
        ));
        assert!(service.acquire().is_ok());
    }

    #[tokio::test]
    async fn uses_observed_runtime_version() {
        let calls = Arc::new(AtomicUsize::new(0));
        let rpc_url = spawn_runtime_snapshot_server(367, Arc::clone(&calls)).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc: Some(rpc_url),
            ..Default::default()
        };

        let observation = observe_approved_finalized_runtime_snapshot(&config)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 5);
        assert_eq!(observation.snapshot().identity().spec_version, 367);
    }

    #[tokio::test]
    async fn rejects_invalid_genesis_hash() {
        let router = Router::new().route(
            "/",
            post(|| async {
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": "86604388"
                }))
            }),
        );
        let rpc_url = spawn_server(router).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc_watch: Some(rpc_url),
            ..Default::default()
        };

        let error = observe_rpc_endpoint_identity(&config, DeepXRpcRole::Watch)
            .await
            .unwrap_err();

        assert!(matches!(error, DeepXRpcIdentityError::InvalidGenesisHash));
    }

    #[rstest::rstest]
    #[case("")]
    #[case("42")]
    #[case("0x")]
    #[case("0xgg")]
    #[case("0x100000000")]
    fn rejects_invalid_finalized_block_number(#[case] value: &str) {
        assert!(matches!(
            decode_block_number(value),
            Err(DeepXRuntimeSnapshotObservationError::InvalidBlockNumber(_))
        ));
    }

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    async fn spawn_identity_server(encoded_hash: &'static str, calls: Arc<AtomicUsize>) -> String {
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(request["method"], "chain_getBlockHash");
                    assert_eq!(request["params"], json!([0]));
                    Json(json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": encoded_hash
                    }))
                }
            }),
        );
        spawn_server(router).await
    }

    async fn spawn_rpc_methods_server(methods: &'static [&'static str]) -> String {
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| async move {
                assert_eq!(request["method"], "rpc_methods");
                assert_eq!(request["params"], json!([]));
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": { "methods": methods }
                }))
            }),
        );
        spawn_server(router).await
    }

    fn validated_endpoints(rpc_url: &str) -> DeepXValidatedRpcEndpoints {
        validated_role_endpoints(rpc_url, rpc_url, rpc_url)
    }

    fn validated_role_endpoints(
        submission_url: &str,
        watch_url: &str,
        recovery_url: &str,
    ) -> DeepXValidatedRpcEndpoints {
        let config = DeepXNetworkConfig {
            base_url_rpc_submission: Some(submission_url.to_string()),
            base_url_rpc_watch: Some(watch_url.to_string()),
            base_url_rpc_recovery: Some(recovery_url.to_string()),
            ..Default::default()
        };
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let observations = [
            DeepXObservedRpcEndpoint::new(
                DeepXRpcRole::Submission,
                submission_url.to_string(),
                genesis_hash,
            ),
            DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, watch_url.to_string(), genesis_hash),
            DeepXObservedRpcEndpoint::new(
                DeepXRpcRole::Recovery,
                recovery_url.to_string(),
                genesis_hash,
            ),
        ];
        validate_rpc_endpoint_identities(&config, observations).unwrap()
    }

    fn validated_capabilities(
        endpoints: &DeepXValidatedRpcEndpoints,
    ) -> DeepXValidatedRpcMethodCapabilities {
        let capabilities = |role, methods: &[&str]| DeepXRpcMethodCapabilities {
            role,
            endpoint_url: endpoints.url_for(role).to_string(),
            methods: methods.iter().map(|method| (*method).to_string()).collect(),
        };
        DeepXValidatedRpcMethodCapabilities {
            submission: capabilities(DeepXRpcRole::Submission, DEEPX_SUBMISSION_RPC_METHODS),
            watch: capabilities(DeepXRpcRole::Watch, DEEPX_WATCH_RPC_METHODS),
            recovery: capabilities(DeepXRpcRole::Recovery, DEEPX_RECOVERY_RPC_METHODS),
        }
    }

    fn approved_runtime_snapshot() -> RuntimeSnapshot {
        let metadata: Value = serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap();
        let encoded_metadata = metadata["result"].as_str().unwrap();
        let read_only = DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap(),
            DEEPX_SPEC_376_VERSION,
            1,
            &hex::decode(encoded_metadata.trim_start_matches("0x")).unwrap(),
        )
        .unwrap();
        RuntimeSnapshot::from_read_only_for_test(read_only)
    }

    fn read_only_runtime_rpc_response(request: &Value) -> Option<Value> {
        match request["method"].as_str()? {
            "state_getRuntimeVersion" => {
                assert_eq!(request["params"], json!([FINALIZED_HASH]));
                Some(json!({
                    "jsonrpc": "2.0", "id": 1,
                    "result": {
                        "specVersion": DEEPX_SPEC_376_VERSION,
                        "transactionVersion": 1,
                    },
                }))
            }
            "state_getMetadata" => {
                assert_eq!(request["params"], json!([FINALIZED_HASH]));
                Some(serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap())
            }
            _ => None,
        }
    }

    #[rstest]
    #[case::canonical(false, false)]
    #[case::reorganized(true, false)]
    #[case::hash_ignored(false, true)]
    #[tokio::test]
    async fn current_signing_runtime_rechecks_canonical_block(
        #[case] reorganized: bool,
        #[case] hash_ignored: bool,
    ) {
        const CURRENT_HASH: &str =
            "0x4242424242424242424242424242424242424242424242424242424242424242";
        let metadata: Value = serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let router = Router::new().route(
            "/",
            post({
                let calls = Arc::clone(&calls);
                move |Json(request): Json<Value>| {
                    let calls = Arc::clone(&calls);
                    let metadata = metadata.clone();
                    async move {
                        let index = calls.fetch_add(1, Ordering::Relaxed);
                        let result = match request["method"].as_str().unwrap() {
                            "chain_getBlockHash" => {
                                assert!(
                                    request["params"] == json!([])
                                        || request["params"] == json!([42])
                                );
                                if reorganized && index == 5 {
                                    json!(format!("0x{}", "43".repeat(32)))
                                } else {
                                    json!(CURRENT_HASH)
                                }
                            }
                            "chain_getHeader" => {
                                assert_eq!(request["params"], json!([CURRENT_HASH]));
                                json!({"number": "0x2a"})
                            }
                            "state_getRuntimeVersion" => {
                                if request["params"] == json!([format!("0x{}", "00".repeat(32))]) {
                                    if hash_ignored {
                                        return Json(json!({"jsonrpc": "2.0", "id": 1,
                                            "result": {"specVersion": 377, "transactionVersion": 2}}));
                                    }
                                    return Json(json!({"jsonrpc": "2.0", "id": 1,
                                        "error": {"code": -32000, "message": "unknown block"}}));
                                }
                                assert_eq!(request["params"], json!([CURRENT_HASH]));
                                json!({"specVersion": 377, "transactionVersion": 2})
                            }
                            "state_getMetadata" => {
                                assert_eq!(request["params"], json!([CURRENT_HASH]));
                                metadata["result"].clone()
                            }
                            method => panic!("unexpected current runtime RPC: {method}"),
                        };
                        Json(json!({"jsonrpc": "2.0", "id": 1, "result": result}))
                    }
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");

        let result =
            observe_current_runtime_snapshot_for_endpoints(&DeepXEnvironment::Testnet, &endpoints)
                .await;

        if hash_ignored {
            assert!(matches!(
                result,
                Err(DeepXRuntimeSnapshotObservationError::BlockHashIgnored)
            ));
        } else if reorganized {
            assert!(matches!(
                result,
                Err(DeepXRuntimeSnapshotObservationError::InvalidCheckpoint)
            ));
        } else {
            let snapshot = result.unwrap();
            assert_eq!(snapshot.identity().spec_version, 377);
            assert_eq!(snapshot.identity().transaction_version, 2);
        }
        assert_eq!(
            calls.load(Ordering::Relaxed),
            if hash_ignored { 3 } else { 6 }
        );
    }

    #[rstest]
    #[case::valid(false, false, false, false)]
    #[case::later_recovery_head(false, false, false, true)]
    #[case::ignored_storage_hash(true, false, false, false)]
    #[case::conflicting_inherent(false, true, false, false)]
    #[case::reorganized(false, false, true, false)]
    #[tokio::test]
    async fn current_submission_clock_keeps_time_current_and_scan_boundary_finalized(
        #[case] ignored_storage_hash: bool,
        #[case] conflicting_inherent: bool,
        #[case] reorganized: bool,
        #[case] later_recovery_head: bool,
    ) {
        const CURRENT_HASH: &str =
            "0x4242424242424242424242424242424242424242424242424242424242424242";
        const RECOVERY_HASH: &str =
            "0x4141414141414141414141414141414141414141414141414141414141414141";
        let metadata: Value = serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap();
        let canonical_reads = Arc::new(AtomicUsize::new(0));
        let router = Router::new().route(
            "/",
            post({
                let canonical_reads = Arc::clone(&canonical_reads);
                move |Json(request): Json<Value>| {
                let metadata = metadata.clone();
                let canonical_reads = Arc::clone(&canonical_reads);
                async move {
                    let params = &request["params"];
                    let response = match request["method"].as_str().unwrap() {
                        "chain_getBlockHash" => {
                            assert!(params == &json!([]) || params == &json!([42]));
                            let hash = if params == &json!([42])
                                && reorganized
                                && canonical_reads.fetch_add(1, Ordering::Relaxed) == 1
                            {
                                format!("0x{}", "43".repeat(32))
                            } else {
                                CURRENT_HASH.to_string()
                            };
                            json!({"jsonrpc": "2.0", "id": 1, "result": hash})
                        }
                        "chain_getHeader" => {
                            let number = if params == &json!([CURRENT_HASH]) {
                                "0x2a"
                            } else {
                                assert_eq!(params, &json!([RECOVERY_HASH]));
                                    if later_recovery_head { "0x2b" } else { "0x29" }
                            };
                            json!({"jsonrpc": "2.0", "id": 1,
                                "result": {"number": number}})
                        }
                        "state_getRuntimeVersion" => {
                            if params == &json!([format!("0x{}", "00".repeat(32))]) {
                                json!({"jsonrpc": "2.0", "id": 1,
                                    "error": {"code": -32000, "message": "unknown block"}})
                            } else {
                                assert_eq!(params, &json!([CURRENT_HASH]));
                                json!({"jsonrpc": "2.0", "id": 1,
                                    "result": {"specVersion": 376, "transactionVersion": 1}})
                            }
                        }
                        "state_getMetadata" => {
                            assert_eq!(params, &json!([CURRENT_HASH]));
                            json!({"jsonrpc": "2.0", "id": 1,
                                "result": metadata["result"]})
                        }
                        "state_getStorage" => {
                            if params[1] == format!("0x{}", "00".repeat(32)) {
                                if ignored_storage_hash {
                                    json!({"jsonrpc": "2.0", "id": 1,
                                        "result": format!("0x{}", hex::encode(1_000_u64.to_le_bytes()))})
                                } else {
                                    json!({"jsonrpc": "2.0", "id": 1,
                                        "error": {"code": -32000, "message": "unknown block"}})
                                }
                            } else {
                                assert_eq!(params[1], CURRENT_HASH);
                                json!({"jsonrpc": "2.0", "id": 1,
                                    "result": format!("0x{}", hex::encode(1_000_u64.to_le_bytes()))})
                            }
                        }
                        "chain_getBlock" => {
                            assert_eq!(params, &json!([CURRENT_HASH]));
                            let mut call = vec![4, 2, 0];
                            call.extend(Compact(if conflicting_inherent { 1_001_u64 } else { 1_000_u64 }).encode());
                            json!({"jsonrpc": "2.0", "id": 1,
                                "result": {"block": {"header": {"number": "0x2a"},
                                    "extrinsics": [format!("0x{}", hex::encode(call.encode()))]}}})
                        }
                        "chain_getFinalizedHead" => {
                            json!({"jsonrpc": "2.0", "id": 1, "result": RECOVERY_HASH})
                        }
                        method => panic!("unexpected submission clock method: {method}"),
                    };
                    Json(response)
                }
                }
            }),
        );
        let watch_url = spawn_server(router).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let capabilities = validated_capabilities(&endpoints);
        let service = DeepXRuntimeSnapshotService::new(approved_runtime_snapshot());
        let result = observe_and_apply_current_submission_clock(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &service,
        )
        .await;
        if ignored_storage_hash || conflicting_inherent || reorganized {
            assert!(result.is_err());
            if reorganized {
                assert!(service.acquire().is_ok());
            } else {
                assert!(matches!(
                    service.acquire(),
                    Err(DeepXRuntimeSnapshotServiceError::EvidenceInconsistent)
                ));
            }
        } else {
            let clock = result.unwrap();
            assert!(clock.timestamp_ms() >= 1_000);
            assert_eq!(
                clock.scan_checkpoint().block_number(),
                if later_recovery_head { 43 } else { 41 },
            );
            assert_eq!(
                clock.runtime_identity(),
                service.acquire().unwrap().snapshot().identity()
            );
        }
    }

    #[rstest]
    fn submission_timestamp_advances_by_observation_latency() {
        assert_eq!(
            advance_submission_timestamp(1_000, Duration::from_millis(9_775)).unwrap(),
            10_775,
        );
        assert!(advance_submission_timestamp(u64::MAX, Duration::from_millis(1)).is_err());
    }

    #[tokio::test]
    #[ignore = "requires the live DeepX testnet Watch RPC"]
    async fn live_current_submission_clock_uses_current_runtime_and_time() {
        let network = DeepXNetworkConfig::default();
        let endpoints = observe_and_validate_rpc_endpoint_identities(&network)
            .await
            .unwrap();
        let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap();
        let service = DeepXRuntimeSnapshotService::new(approved_runtime_snapshot());
        let clock = observe_and_apply_current_submission_clock(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &capabilities,
            &service,
        )
        .await
        .unwrap();
        assert!(clock.timestamp_ms() > 0);
        assert!(clock.scan_checkpoint().block_number() > 0);
        assert_eq!(
            clock.runtime_identity(),
            service.acquire().unwrap().snapshot().identity()
        );
    }

    async fn spawn_runtime_snapshot_server(spec_version: u32, calls: Arc<AtomicUsize>) -> String {
        spawn_runtime_snapshot_server_with(
            spec_version,
            1,
            serde_json::from_str(SPEC_376_METADATA_FIXTURE).unwrap(),
            None,
            None,
            |_| {},
            calls,
        )
        .await
    }

    async fn spawn_runtime_snapshot_server_with(
        spec_version: u32,
        transaction_version: u32,
        metadata: Value,
        timestamp_now: Option<Value>,
        fail_method: Option<&'static str>,
        on_request: impl Fn(&str) + Clone + Send + Sync + 'static,
        calls: Arc<AtomicUsize>,
    ) -> String {
        let genesis: Value = serde_json::from_str(GENESIS_FIXTURE).unwrap();
        let finalized_head: Value = serde_json::from_str(FINALIZED_HEAD_FIXTURE).unwrap();
        let finalized_header = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "number": FINALIZED_BLOCK_NUMBER_HEX
            }
        });
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&calls);
                let genesis = genesis.clone();
                let finalized_head = finalized_head.clone();
                let finalized_header = finalized_header.clone();
                let metadata = metadata.clone();
                let on_request = on_request.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    let method = request["method"].as_str().unwrap();
                    on_request(method);
                    if fail_method == Some(method) {
                        return Json(json!({
                            "jsonrpc": "2.0", "id": 1,
                            "error": { "code": -32000, "message": "injected observation failure" }
                        }));
                    }
                    let response = match method {
                        "chain_getBlockHash" => {
                            assert_eq!(request["params"], json!([0]));
                            genesis
                        }
                        "chain_getFinalizedHead" => {
                            assert_eq!(request["params"], json!([]));
                            finalized_head
                        }
                        "chain_getHeader" => {
                            assert_eq!(request["params"], json!([FINALIZED_HASH]));
                            finalized_header
                        }
                        "state_getRuntimeVersion" => {
                            assert_eq!(request["params"], json!([FINALIZED_HASH]));
                            json!({
                                "jsonrpc": "2.0", "id": 1,
                                "result": {
                                    "specVersion": spec_version,
                                    "transactionVersion": transaction_version,
                                },
                            })
                        }
                        "state_getMetadata" => {
                            assert_eq!(request["params"], json!([FINALIZED_HASH]));
                            metadata
                        }
                        "state_getStorage" => {
                            assert_eq!(
                                request["params"],
                                json!([timestamp_now_storage_key(), FINALIZED_HASH])
                            );
                            json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": timestamp_now
                            })
                        }
                        "chain_getBlock" => {
                            assert_eq!(request["params"], json!([FINALIZED_HASH]));
                            let mut timestamp_ms = timestamp_now
                                .as_ref()
                                .and_then(Value::as_str)
                                .and_then(|value| value.strip_prefix("0x"))
                                .and_then(|value| hex::decode(value).ok())
                                .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                                .map(u64::from_le_bytes)
                                .unwrap();
                            if fail_method == Some("block_timestamp_mismatch") {
                                timestamp_ms += 1;
                            }
                            let mut call = vec![4, 2, 0];
                            call.extend(Compact(timestamp_ms).encode());
                            json!({
                                "jsonrpc": "2.0", "id": 1,
                                "result": {"block": {
                                    "header": {"number": FINALIZED_BLOCK_NUMBER_HEX},
                                    "extrinsics": [format!("0x{}", hex::encode(call.encode()))],
                                }},
                            })
                        }
                        "state_queryStorageAt" => {
                            assert_eq!(
                                request["params"],
                                json!([[timestamp_now_storage_key()], FINALIZED_HASH])
                            );
                            json!({
                                "jsonrpc": "2.0", "id": 1,
                                "result": [{
                                    "block": FINALIZED_HASH,
                                    "changes": [[timestamp_now_storage_key(), timestamp_now]],
                                }],
                            })
                        }
                        _ => panic!("unexpected DeepX runtime snapshot request: {method}"),
                    };
                    Json(response)
                }
            }),
        );
        spawn_server(router).await
    }

    #[rstest]
    fn current_flat_usdc_balance_requires_backend_exact_collateral_and_chain_coverage() {
        use crate::{
            account::map_current_flat_usdc_balance,
            http::{
                DeepXSubaccountBalanceAsset, DeepXSubaccountBalances, DeepXSubaccountMarginRatio,
                models::DeepXExactMargin,
            },
        };

        let amount = rust_decimal::Decimal::new(10_000_000, 6);
        let zero = rust_decimal::Decimal::ZERO;
        let mut balances = DeepXSubaccountBalances {
            address: format!("0x{}", hex::encode([0x11; 20])),
            assets: vec![DeepXSubaccountBalanceAsset {
                symbol: "USDC".to_string(),
                decimals: 6,
                price: rust_decimal::Decimal::ONE,
                balance: amount,
                balance_usd: amount,
                balance_borrowed: zero,
                balance_borrowed_usd: zero,
                borrow_interest: zero,
                borrow_interest_usd: zero,
            }],
        };
        let mut margin = DeepXSubaccountMarginRatio {
            collateral: amount,
            margin_required: zero,
            margin_ratio: None,
            exact_margin: Some(DeepXExactMargin {
                total_collateral: amount,
                maint_margin_req: zero,
                uni_mmr: None,
            }),
        };
        let mut current = DeepXCurrentMarginEvidence {
            block_hash: [0x22; 32],
            block_number: 42,
            subaccount: [0x11; 20],
            initial: DeepXMarginRequirement {
                total_collateral: 10_000_000,
                total_margin_required: 0,
            },
            maintenance: DeepXMarginRequirement {
                total_collateral: 10_000_000,
                total_margin_required: 0,
            },
        };
        let mapped = map_current_flat_usdc_balance(&balances, &margin, &current).unwrap();
        balances.assets[0].price = rust_decimal::Decimal::new(2, 0);
        balances.assets[0].balance_usd = rust_decimal::Decimal::new(9, 0);
        assert_eq!(
            map_current_flat_usdc_balance(&balances, &margin, &current).unwrap(),
            mapped,
        );

        balances.assets[0].balance_borrowed = rust_decimal::Decimal::new(1, 6);
        assert!(map_current_flat_usdc_balance(&balances, &margin, &current).is_err());
        balances.assets[0].balance_borrowed = zero;

        current.initial.total_margin_required = 1;
        assert!(map_current_flat_usdc_balance(&balances, &margin, &current).is_err());
        current.initial.total_margin_required = 0;

        margin.exact_margin.as_mut().unwrap().total_collateral =
            rust_decimal::Decimal::new(9_999_999, 6);
        assert!(map_current_flat_usdc_balance(&balances, &margin, &current).is_err());
        margin.exact_margin.as_mut().unwrap().total_collateral = amount;

        current.initial.total_collateral = 10_000_001;
        current.maintenance.total_collateral = 10_000_001;
        assert_eq!(
            map_current_flat_usdc_balance(&balances, &margin, &current).unwrap(),
            mapped,
        );
        current.initial.total_collateral = 9_999_999;
        assert!(map_current_flat_usdc_balance(&balances, &margin, &current).is_err());
        current.initial.total_collateral = 10_000_000;

        current.maintenance.total_collateral = 9_999_999;
        assert!(map_current_flat_usdc_balance(&balances, &margin, &current).is_err());
    }
}
