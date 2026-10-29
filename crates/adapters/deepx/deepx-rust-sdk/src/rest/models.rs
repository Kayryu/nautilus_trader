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

//! Exact response models for DeepX REST endpoints.

use std::collections::HashSet;

use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::DeepXAccountPage;
use crate::{
    decimal::{
        exact_decimal, exact_i128, exact_u128, optional_exact_decimal, optional_exact_u64,
        optional_exact_u128, optional_none_decimal,
    },
    query::{DeepXHourlyUnsettledFundingCursor, DeepXQuotaBuyerType, DeepXQuotaHistoryType},
};

/// Wallet-scoped subaccount directory in venue response order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXWalletSubaccounts {
    wallet: String,
    /// Subaccount addresses in venue response order.
    pub addresses: Vec<String>,
}

impl DeepXWalletSubaccounts {
    /// Binds a directory response to the wallet used for the query.
    #[must_use]
    pub fn new(wallet: String, addresses: Vec<String>) -> Self {
        Self { wallet, addresses }
    }

    /// Returns the wallet address used for the directory query.
    #[must_use]
    pub fn wallet(&self) -> &str {
        &self.wallet
    }

    /// Checks case-insensitive subaccount membership, independent of response order.
    #[must_use]
    pub fn has_same_membership(&self, addresses: &[String]) -> bool {
        if self.addresses.len() != addresses.len() {
            return false;
        }
        let expected: HashSet<_> = self
            .addresses
            .iter()
            .map(|address| address.to_ascii_lowercase())
            .collect();
        let observed: HashSet<_> = addresses
            .iter()
            .map(|address| address.to_ascii_lowercase())
            .collect();
        expected.len() == self.addresses.len() && expected == observed
    }
}

/// One lending pool asset identity exposed by the public market directory.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXLendingAsset {
    /// Deployment-provided lending market ID.
    pub market_id: u64,
    /// Venue asset symbol.
    pub asset: String,
    /// Venue block height associated with the directory entry.
    pub height: u64,
    /// Venue creation timestamp preserved without freshness inference.
    pub created_at: String,
}

/// Exact borrow-rate curve parameters for one lending pool asset.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXLendingInterestRateParams {
    /// Deployment-provided lending market ID.
    pub market_id: u64,
    /// Venue asset symbol.
    pub asset: String,
    /// Minimum borrow rate.
    #[serde(rename = "Rmin", deserialize_with = "exact_decimal::deserialize")]
    pub r_min: Decimal,
    /// First utilization kink.
    #[serde(rename = "U1", deserialize_with = "exact_decimal::deserialize")]
    pub u1: Decimal,
    /// Second utilization kink.
    #[serde(rename = "U2", deserialize_with = "exact_decimal::deserialize")]
    pub u2: Decimal,
    /// Third utilization kink.
    #[serde(rename = "U3", deserialize_with = "exact_decimal::deserialize")]
    pub u3: Decimal,
    /// Optional fourth utilization kink.
    #[serde(rename = "U4", deserialize_with = "optional_none_decimal::deserialize")]
    pub u4: Option<Decimal>,
    /// Optional fifth utilization kink.
    #[serde(rename = "U5", deserialize_with = "optional_none_decimal::deserialize")]
    pub u5: Option<Decimal>,
    /// Borrow rate at the first utilization kink.
    #[serde(rename = "R1", deserialize_with = "exact_decimal::deserialize")]
    pub r1: Decimal,
    /// Borrow rate at the second utilization kink.
    #[serde(rename = "R2", deserialize_with = "exact_decimal::deserialize")]
    pub r2: Decimal,
    /// Borrow rate at the third utilization kink.
    #[serde(rename = "R3", deserialize_with = "exact_decimal::deserialize")]
    pub r3: Decimal,
    /// Optional borrow rate at the fourth utilization kink.
    #[serde(rename = "R4", deserialize_with = "optional_none_decimal::deserialize")]
    pub r4: Option<Decimal>,
    /// Optional borrow rate at the fifth utilization kink.
    #[serde(rename = "R5", deserialize_with = "optional_none_decimal::deserialize")]
    pub r5: Option<Decimal>,
    /// Maximum borrow rate at full utilization.
    #[serde(rename = "Rmax", deserialize_with = "exact_decimal::deserialize")]
    pub r_max: Decimal,
    /// Venue curve parameter retained without additional interpretation.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub rho: Decimal,
}

/// One exact lending supply and borrow APR observation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXLendingInterestRateRecord {
    /// Deployment-provided lending market ID.
    pub market_id: u64,
    /// Venue asset symbol.
    pub asset: String,
    /// Venue supply APR represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub supply_apr: Decimal,
    /// Venue borrow APR represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub borrow_apr: Decimal,
    /// Aggregation bucket timestamp in Unix milliseconds.
    pub statistic_time: u64,
}

/// One exact lending pool status observation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXLendingStatusRecord {
    /// Deployment-provided lending market ID.
    pub market_id: u64,
    /// Venue asset symbol.
    pub asset: String,
    /// Venue index price represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub index_price: Decimal,
    /// Total supplied quantity with asset units left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_supplied: Decimal,
    /// Total borrowed quantity with asset units left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_borrowed: Decimal,
    /// Venue utilization ratio.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub utilization_rate: Decimal,
    /// Venue supply APR represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub supply_apr: Decimal,
    /// Venue borrow APR represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub borrow_apr: Decimal,
    /// Aggregation bucket timestamp in Unix milliseconds.
    pub statistic_time: u64,
}

/// One lending asset balance in the observed account-balance wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSubaccountBalanceAsset {
    /// Venue asset symbol.
    pub symbol: String,
    /// Venue asset decimal precision.
    pub decimals: u8,
    /// Current asset price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Deposited asset quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub balance: Decimal,
    /// Deposited value in USD.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub balance_usd: Decimal,
    /// Borrowed asset quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub balance_borrowed: Decimal,
    /// Borrowed value in USD.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub balance_borrowed_usd: Decimal,
    /// Accrued borrow-interest quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub borrow_interest: Decimal,
    /// Accrued borrow-interest value in USD.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub borrow_interest_usd: Decimal,
}

/// Exact equity summary for one subaccount.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSubaccountEquity {
    /// Subaccount address.
    pub subaccount: String,
    /// Total deposits valued in USD.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_deposits_usd: Decimal,
    /// Total borrows valued in USD.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_borrows_usd: Decimal,
    /// Signed unrealized perpetual profit and loss in USD.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub unrealized_pnl_usd: Decimal,
    /// Signed account equity in USD.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub equity_usd: Decimal,
}

/// Optional position identity attached to a balance change.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXBalanceChangePosition {
    /// Venue position lifecycle ID.
    pub id: u64,
    /// Owning subaccount address.
    pub owner: String,
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
}

/// One account liquidation record with exact raw chain integer values.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXLiquidationRecord {
    /// Stable backend record ID.
    pub id: u64,
    /// Per-account chain liquidation sequence ID, which may start at zero.
    pub liquidation_id: u64,
    /// Closed chain liquidation classification.
    pub liquidation_type: crate::query::DeepXLiquidationType,
    /// JSON-encoded chain detail retained without unit conversion.
    pub liquidation_detail: String,
    /// Optional transaction hash reported by the backend.
    pub tx_hash: Option<String>,
    /// Liquidated subaccount AccountId20.
    pub target_account: String,
    /// Liquidator AccountId20.
    pub liquidator: String,
    /// Exact raw chain margin shortage.
    #[serde(deserialize_with = "exact_u128::deserialize")]
    pub margin_shortage: u128,
    /// JSON-encoded canceled-order identities retained without interpretation.
    pub canceled_order_ids: String,
    /// Exact raw chain margin released by the operation.
    #[serde(deserialize_with = "exact_u128::deserialize")]
    pub margin_freed: u128,
    /// Optional market index associated with the liquidation type.
    pub market_index: Option<u64>,
    /// Optional exact raw liquidator fee.
    #[serde(default, deserialize_with = "optional_exact_u128::deserialize")]
    pub liquidator_fee: Option<u128>,
    /// Exact raw Insurance Fund fee or payment.
    #[serde(deserialize_with = "exact_u128::deserialize")]
    pub if_fee: u128,
    /// Optional exact raw base or asset amount liquidated.
    #[serde(default, deserialize_with = "optional_exact_u128::deserialize")]
    pub liquidate_base_amount: Option<u128>,
    /// Optional exact raw oracle price.
    #[serde(default, deserialize_with = "optional_exact_u128::deserialize")]
    pub oracle_price: Option<u128>,
    /// Optional liquidator venue order ID.
    #[serde(default, deserialize_with = "optional_exact_u64::deserialize")]
    pub liquidator_order_id: Option<u64>,
    /// Optional target-account venue order ID.
    #[serde(default, deserialize_with = "optional_exact_u64::deserialize")]
    pub target_account_order_id: Option<u64>,
    /// Optional exact raw spot borrow amount.
    #[serde(default, deserialize_with = "optional_exact_u128::deserialize")]
    pub borrow_amount: Option<u128>,
    /// Whether the chain record reports bankruptcy after the operation.
    pub bankrupt: bool,
    /// Block height containing the liquidation event.
    pub height: u64,
    /// Event index within the block.
    pub event_idx: u64,
    /// Backend observation time in RFC 3339 format.
    pub created_at: String,
}

/// One perpetual position lifecycle in the observed account-position wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpPositionRecord {
    /// Venue position lifecycle ID.
    pub id: u64,
    /// Owning subaccount address.
    pub owner: String,
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue long-side flag preserved without framework-side interpretation.
    pub is_long: bool,
    /// Position base-asset quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub base_asset_amount: Decimal,
    /// Position entry price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub entry_price: Decimal,
    /// Venue-reported leverage.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub leverage: Decimal,
    /// Last funding-rate snapshot reported for the position.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub last_funding_rate: Decimal,
    /// Venue position version.
    pub version: u64,
    /// Venue-reported realized profit and loss.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub realized_pnl: Decimal,
    /// Venue-reported funding payment.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_payment: Decimal,
    /// Venue-reported take-profit price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub take_profit: Decimal,
    /// Venue-reported stop-loss price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub stop_loss: Decimal,
    /// Last settlement price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub last_settle_price: Decimal,
    /// Venue-reported position profit and loss.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub pnl: Decimal,
    /// Position opening block number.
    pub open_block_num: u64,
    /// Position opening timestamp preserved without interpretation.
    pub open_time: String,
    /// Position opening event index.
    pub open_event_idx: u64,
    /// Optional position closing price.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub close_price: Option<Decimal>,
    /// Optional position closing block number.
    pub close_block_num: Option<u64>,
    /// Optional position closing timestamp preserved without interpretation.
    pub close_time: Option<String>,
    /// Optional position closing event index.
    pub close_event_idx: Option<u64>,
    /// Venue lifecycle-status value preserved without interpretation.
    pub status: String,
    /// Venue-reported total fee.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_fee: Decimal,
    /// Optional venue liquidation price.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub liquidate_price: Option<Decimal>,
    /// Venue creation timestamp preserved without interpretation.
    pub created_at: String,
    /// Venue update timestamp preserved without interpretation.
    pub updated_at: String,
}

/// One wallet-level delegate configuration in the observed REST wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXDelegateAccount {
    /// Delegate AccountId20.
    pub delegate_address: String,
    /// Wallet-provided delegate label.
    pub delegate_name: String,
    /// Expiry timestamp in Unix milliseconds, where zero denotes no expiry for legacy records.
    pub valid_until: u64,
    /// Chain-defined delegate permission mode.
    pub mode: DeepXDelegateMode,
    /// Initial configuration timestamp in Unix milliseconds.
    pub create_time: u64,
    /// Backend-reported activity flag, or true when omitted by current responses.
    #[serde(default = "default_delegate_active")]
    pub active: bool,
}

/// Delegate configurations returned for one queried wallet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXWalletDelegateAccounts<T = DeepXDelegateAccount> {
    wallet: String,
    /// Delegate configurations in venue response order.
    pub accounts: Vec<T>,
}

impl<T> DeepXWalletDelegateAccounts<T> {
    /// Associates delegate configurations with the queried wallet.
    #[must_use]
    pub fn new(wallet: String, accounts: Vec<T>) -> Self {
        Self { wallet, accounts }
    }

    /// Returns the wallet address used for the directory query.
    #[must_use]
    pub fn wallet(&self) -> &str {
        &self.wallet
    }
}

/// Grouped account reads for one subaccount.
#[derive(Clone, Debug)]
pub struct DeepXSubaccountSnapshot<
    P = DeepXSubaccountProfile,
    B = DeepXSubaccountBalances,
    E = DeepXSubaccountEquity,
    M = DeepXSubaccountMarginRatio,
> {
    /// Subaccount profile bound to the requested wallet authority.
    pub profile: P,
    /// Exact lending balances bound to the profile address.
    pub balances: B,
    /// Exact equity summary bound to the profile address.
    pub equity: E,
    /// Request-scoped collateral and margin requirement summary.
    pub margin_ratio: M,
}

/// Grouped account reads for every subaccount in one wallet directory.
///
/// Constituent REST reads are not block-pinned and do not form an atomic venue snapshot.
#[derive(Clone, Debug)]
pub struct DeepXWalletAccountSnapshot<
    D = DeepXWalletSubaccounts,
    S = DeepXUserStats,
    A = DeepXSubaccountSnapshot,
> {
    /// Wallet directory in venue response order.
    pub directory: D,
    /// Wallet statistics for the directory's subaccounts.
    pub stats: S,
    /// Account reads in directory order.
    pub subaccounts: Vec<A>,
}

/// Grouped position lifecycle pages for one subaccount.
#[derive(Clone, Debug)]
pub struct DeepXSubaccountPerpPositions<P = DeepXSubaccountProfile, R = DeepXPerpPositionRecord> {
    /// Profile whose authority matches the requested wallet.
    pub profile: P,
    /// Position pages in venue cursor order.
    pub pages: Vec<crate::rest::DeepXAccountPage<R>>,
}

/// Grouped position lifecycle observations for one wallet.
///
/// Constituent REST reads are mutable and do not form a block-pinned snapshot.
#[derive(Clone, Debug)]
pub struct DeepXWalletPerpPositions<
    D = DeepXWalletSubaccounts,
    S = DeepXUserStats,
    P = DeepXSubaccountPerpPositions,
> {
    /// Wallet directory in venue response order.
    pub directory: D,
    /// Wallet statistics for the directory's subaccounts.
    pub stats: S,
    /// Position pages in directory order.
    pub subaccounts: Vec<P>,
}

/// Wallet addresses returned for one queried delegate account.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXDelegateWallets {
    delegate: String,
    /// Wallet addresses in venue response order.
    pub wallets: Vec<String>,
}

impl DeepXDelegateWallets {
    /// Associates wallet addresses with the queried delegate account.
    #[must_use]
    pub fn new(delegate: String, wallets: Vec<String>) -> Self {
        Self { delegate, wallets }
    }

    /// Returns the delegate address used for the reverse-directory query.
    #[must_use]
    pub fn delegate(&self) -> &str {
        &self.delegate
    }
}

fn default_delegate_active() -> bool {
    true
}

/// One subaccount profile in the observed account-info wire form.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSubaccountProfile {
    /// Wallet authority that owns the subaccount.
    pub authority: String,
    /// Subaccount address.
    pub address: String,
    /// Venue display name.
    pub name: String,
    /// Venue account-status value.
    pub status: String,
    /// Spot-position payloads retained without semantic interpretation.
    pub spot_positions: Vec<Box<RawValue>>,
    /// Next venue order identity.
    pub next_order_id: u32,
    /// Whether spot margin trading is enabled.
    pub spot_margin_trading_enabled: bool,
    /// Venue margin-strategy value.
    pub margin_strategy: String,
    /// Block height associated with the profile observation.
    pub height: u64,
    /// Account creation timestamp in Unix milliseconds.
    pub created_at: u64,
}

/// Account-wide margin displays and the exact values supplied by current REST responses.
///
/// The legacy numeric fields are rounded displays and must not be used for financial decisions.
/// Historical responses lack `exact_margin`; the live reader rejects them.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSubaccountMarginRatio {
    /// Rounded legacy collateral display in USDC.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub collateral: Decimal,
    /// Rounded legacy margin requirement display in USDC.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub margin_required: Decimal,
    /// Rounded legacy percentage display, absent when the venue reports no applicable ratio.
    #[serde(deserialize_with = "optional_exact_decimal::deserialize")]
    pub margin_ratio: Option<Decimal>,
    /// Exact collateral, maintenance requirement, and dimensionless ratio when supplied.
    #[serde(default)]
    pub exact_margin: Option<DeepXExactMargin>,
}

/// Exact lending balances for one subaccount.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSubaccountBalances {
    /// Subaccount address.
    pub address: String,
    /// Asset balances in venue response order.
    pub assets: Vec<DeepXSubaccountBalanceAsset>,
}

/// Validates the account and exact asset amounts in one balance response.
///
/// # Errors
///
/// Returns an error for another account, duplicate assets, invalid precision, or negative values.
pub fn validate_subaccount_balances(
    balances: &DeepXSubaccountBalances,
    requested_address: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        balances.address.eq_ignore_ascii_case(requested_address),
        "balances belong to another subaccount"
    );
    let mut symbols = HashSet::new();
    for (index, asset) in balances.assets.iter().enumerate() {
        let symbol = asset.symbol.trim();
        anyhow::ensure!(
            !symbol.is_empty() && symbols.insert(symbol.to_ascii_uppercase()),
            "asset {index} has an empty or duplicate symbol"
        );
        let quantity_exceeds_precision =
            [asset.balance, asset.balance_borrowed, asset.borrow_interest]
                .into_iter()
                .any(|amount| amount.normalize().scale() > u32::from(asset.decimals));
        anyhow::ensure!(
            asset.decimals <= 28
                && !quantity_exceeds_precision
                && !asset.price.is_sign_negative()
                && !asset.balance.is_sign_negative()
                && !asset.balance_usd.is_sign_negative()
                && !asset.balance_borrowed.is_sign_negative()
                && !asset.balance_borrowed_usd.is_sign_negative()
                && !asset.borrow_interest.is_sign_negative()
                && !asset.borrow_interest_usd.is_sign_negative(),
            "asset {index} has invalid token precision or financial values"
        );
    }
    Ok(())
}

/// One exact account balance-change record.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXBalanceChangeRecord {
    /// Stable venue record ID.
    pub id: u64,
    /// Venue asset symbol.
    pub asset: String,
    /// Signed asset-quantity change.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub balance_change: Decimal,
    /// Documented change classification.
    pub change_type: crate::query::DeepXBalanceChangeType,
    /// Venue event timestamp in Unix milliseconds.
    pub time: u64,
    /// Venue block height.
    pub height: u64,
    /// Event index within the block.
    pub event_idx: u64,
    /// Optional venue transaction hash.
    pub tx_hash: Option<String>,
    /// Optional venue transaction-hash namespace.
    pub tx_hash_type: Option<String>,
    /// Optional source AccountId20.
    pub from: Option<String>,
    /// Optional destination AccountId20.
    pub to: Option<String>,
    /// Cross-chain extension retained without interpretation.
    pub cross_chain: Option<Box<RawValue>>,
    /// Optional perpetual position identity.
    pub position: Option<DeepXBalanceChangePosition>,
}

/// Lending interest-rate history in venue response order.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXLendingInterestRateHistory<T = DeepXLendingInterestRateRecord> {
    /// Exact APR observations.
    pub details: Vec<T>,
}

/// Lending pool status history in venue response order.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXLendingStatusHistory<T = DeepXLendingStatusRecord> {
    /// Exact pool observations.
    pub details: Vec<T>,
}

/// Spot orders for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotWalletSubaccountOrders<T = DeepXSpotOrderRecord> {
    /// Subaccount identity reported for this group.
    pub subaccount: String,
    /// Orders and repeated global page metadata for this subaccount group.
    pub orders: DeepXAccountPage<T>,
}

/// One market group in a wallet-scoped Spot order response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotWalletOrderMarket<T = DeepXSpotWalletSubaccountOrders> {
    /// Deployment-provided bytes32 pair identity.
    pub pair: String,
    /// Venue market name.
    pub name: String,
    /// Per-subaccount order groups returned for this market.
    pub subaccounts: Vec<T>,
}

/// Globally paginated Spot wallet orders grouped by market and subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotWalletOrdersPage<T = DeepXSpotWalletOrderMarket> {
    /// Market groups in venue response order.
    pub markets: Vec<T>,
    /// Opaque cursor repeated by every nonempty nested group.
    pub next_cursor: Option<String>,
    /// Whether every nonempty nested group reports another global page.
    pub has_next: bool,
}

/// Spot trades for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotWalletSubaccountTrades<T = DeepXSpotAccountTradeRecord> {
    /// Subaccount identity reported for this group.
    pub subaccount: String,
    /// Trades and repeated global page metadata for this subaccount group.
    pub trades: DeepXAccountPage<T>,
}

/// One market group in a wallet-scoped Spot trade response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotWalletTradeMarket<T = DeepXSpotWalletSubaccountTrades> {
    /// Deployment-provided bytes32 pair identity.
    pub pair: String,
    /// Venue market name.
    pub name: String,
    /// Per-subaccount trade groups returned for this market.
    pub subaccounts: Vec<T>,
}

/// Globally paginated Spot wallet trades grouped by market and subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotWalletTradesPage<T = DeepXSpotWalletTradeMarket> {
    /// Market groups in venue response order.
    pub markets: Vec<T>,
    /// Opaque cursor repeated by every nonempty nested group.
    pub next_cursor: Option<String>,
    /// Whether every nonempty nested group reports another global page.
    pub has_next: bool,
}

/// Perpetual trades for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpWalletSubaccountTrades<T = DeepXPerpAccountTradeRecord> {
    /// Subaccount identity reported for this group.
    pub subaccount: String,
    /// Independently paginated trades for this subaccount.
    pub trades: DeepXAccountPage<T>,
}

/// One market group in a wallet-scoped perpetual trade response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpWalletTradeMarket<T = DeepXPerpWalletSubaccountTrades> {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue market name.
    pub market_name: String,
    /// Per-subaccount trade pages returned for this market.
    pub subaccounts: Vec<T>,
}

/// Perpetual orders for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpWalletSubaccountOrders<T = DeepXPerpOrderRecord> {
    /// Subaccount identity reported for this group.
    pub subaccount: String,
    /// Orders and repeated global page metadata for this subaccount group.
    pub orders: DeepXAccountPage<T>,
}

/// One market group in a wallet-scoped perpetual order response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpWalletOrderMarket<T = DeepXPerpWalletSubaccountOrders> {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue market name.
    pub market_name: String,
    /// Per-subaccount order groups returned for this market.
    pub subaccounts: Vec<T>,
}

/// Globally paginated perpetual wallet orders grouped by market and subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpWalletOrdersPage<T = DeepXPerpWalletOrderMarket> {
    /// Market groups in venue response order.
    pub markets: Vec<T>,
    /// Opaque cursor repeated by every nested group.
    pub next_cursor: Option<String>,
    /// Whether every nested group reports another global page.
    pub has_next: bool,
}

/// Aggregated wallet directory counters returned by the user-stats endpoint.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXUserStats {
    /// Subaccounts reported for the queried wallet in venue response order.
    pub subaccounts: Vec<String>,
    /// Venue-reported Insurance Fund staked quote amount with units left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub if_staked_quote_asset_amount: Decimal,
    /// Current number of subaccounts reported by the venue.
    pub number_of_sub_accounts: u64,
    /// Historical number of subaccounts created for the wallet.
    pub number_of_sub_accounts_created: u64,
}

/// Exact wallet-level trading-volume and quota aggregate.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXQuotaSummary {
    /// Wallet that owns the aggregate.
    pub owner: String,
    /// Number of subaccounts included by the venue.
    pub subaccount_count: u64,
    /// Aggregated Spot trading volume in USD.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub spot_volume_usd: Decimal,
    /// Aggregated perpetual trading volume in USD.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub perp_volume_usd: Decimal,
    /// Aggregated total trading volume in USD.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_volume_usd: Decimal,
    /// Venue-reported quota earned from trading volume.
    pub quota_earned: u64,
    /// Venue-reported quota already granted on chain.
    pub quota_granted: u64,
    /// Venue-reported quota reserved by active claims.
    pub quota_reserved: u64,
    /// Venue-reported quota currently claimable.
    pub quota_pending: u64,
    /// First counted trade timestamp in Unix milliseconds, when present.
    pub first_trade_ts_ms: Option<u64>,
    /// Last counted trade timestamp in Unix milliseconds, when present.
    pub last_trade_ts_ms: Option<u64>,
    /// First counted trade timestamp in RFC 3339 form, when present.
    pub first_trade_at: Option<String>,
    /// Last counted trade timestamp in RFC 3339 form, when present.
    pub last_trade_at: Option<String>,
    /// Last venue aggregate update timestamp in RFC 3339 form, when present.
    pub updated_at: Option<String>,
}

/// One chain-confirmed wallet quota-history record.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXQuotaHistoryRecord {
    /// Stable venue record identity.
    pub id: String,
    /// Chain-confirmed quota operation classification.
    pub history_type: DeepXQuotaHistoryType,
    /// Wallet that owns this quota history.
    pub owner_address: String,
    /// Purchase payer account classification, absent for non-purchase records.
    pub buyer_type: Option<DeepXQuotaBuyerType>,
    /// Purchase payer address, absent for non-purchase records.
    pub buyer_address: Option<String>,
    /// Venue-reported quota amount.
    pub quota: u64,
    /// Transaction hash associated with the chain event.
    pub tx_hash: String,
    /// Venue transaction-hash classification.
    pub tx_hash_type: String,
    /// Block containing the quota event.
    pub block_number: u64,
    /// Event index within the block.
    pub event_index: u64,
    /// Venue creation timestamp in RFC 3339 form.
    pub created_at: String,
}

/// One perpetual funding-fee record in the observed account-history wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpFundingFeeRecord {
    /// Owning subaccount address.
    pub owner: String,
    /// Deployment-provided perpetual market ID.
    pub market: u64,
    /// Position quantity used by the venue fee calculation.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub position_size: Decimal,
    /// Venue long-side flag preserved without framework-side interpretation.
    pub is_long: bool,
    /// Signed funding fee in venue-reported units.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub fee: Decimal,
    /// Signed funding rate used by the venue.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub fee_rate: Decimal,
    /// Venue creation timestamp preserved without cadence inference.
    pub created_at: String,
    /// Venue block height associated with the observation.
    pub height: u64,
    /// Venue event index associated with the observation.
    pub event_idx: u64,
    /// Whether the venue identifies the record as settled.
    pub is_settled: bool,
}

/// One complete hourly unsettled-funding boundary in exact on-chain integer units.
///
/// Values remain unscaled without inferred token precision, PnL, or account-state semantics.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXHourlyUnsettledFundingRecord {
    /// Subaccount whose position was observed at the boundary.
    pub subaccount: String,
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Position version associated with this boundary.
    pub position_version: u64,
    /// Signed position quantity in raw on-chain units.
    #[serde(deserialize_with = "exact_i128::deserialize")]
    pub signed_position_size_raw: i128,
    /// Funding index at the preceding settlement boundary.
    #[serde(deserialize_with = "exact_i128::deserialize")]
    pub baseline_index_raw: i128,
    /// Cumulative funding index at this boundary.
    #[serde(deserialize_with = "exact_i128::deserialize")]
    pub cumulative_index_raw: i128,
    /// Exact difference between the cumulative and baseline funding indexes.
    #[serde(deserialize_with = "exact_i128::deserialize")]
    pub delta_index_raw: i128,
    /// Mark price in raw on-chain units.
    #[serde(deserialize_with = "exact_u128::deserialize")]
    pub mark_price_raw: u128,
    /// Signed unsettled funding payment in raw quote-token units.
    #[serde(deserialize_with = "exact_i128::deserialize")]
    pub payment_raw: i128,
    /// Finalized chain timestamp associated with the boundary, in Unix milliseconds.
    pub boundary_timestamp_ms: u64,
    /// Block height containing the boundary event.
    pub boundary_block: u64,
    /// Event index within the boundary block.
    pub boundary_event_index: u64,
    /// Stable backend boundary-event identity used by keyset pagination.
    pub boundary_event_id: String,
}

impl DeepXHourlyUnsettledFundingRecord {
    /// Returns the complete keyset cursor for this funding boundary.
    #[must_use]
    pub fn cursor(&self) -> DeepXHourlyUnsettledFundingCursor {
        DeepXHourlyUnsettledFundingCursor {
            boundary_timestamp_ms: self.boundary_timestamp_ms,
            market_id: self.market_id,
            subaccount: self.subaccount.clone(),
            event_id: self.boundary_event_id.clone(),
        }
    }
}

/// One perpetual execution in the observed account-trade wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpAccountTradeRecord {
    /// Venue trade ID.
    pub id: u64,
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Exact decimal venue order ID.
    pub order_id: String,
    /// Venue long-side flag preserved without framework-side interpretation.
    pub is_long: bool,
    /// Venue taker-role value preserved without interpretation.
    pub taker: String,
    /// Execution price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Execution quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub size: Decimal,
    /// Venue-reported leverage.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub leverage: Decimal,
    /// Venue creation timestamp preserved without interpretation.
    pub created_at: String,
    /// Venue fill-direction value preserved without interpretation.
    pub filled_direction: String,
    /// Venue-reported signed fee.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub fee: Decimal,
    /// Venue fee-asset value preserved without interpretation.
    pub fee_asset: String,
}

/// One Spot execution in the observed account-trade wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotAccountTradeRecord {
    /// Venue trade identity.
    pub id: u64,
    /// Exact decimal venue order ID.
    pub order_id: String,
    /// Venue buy- or sell-side value preserved without framework-side interpretation.
    pub order_side: String,
    /// Deployment-provided bytes32 pair identity.
    pub pair: String,
    /// Venue market name.
    pub pair_name: String,
    /// Executed base-asset quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub base_amount: Decimal,
    /// Executed quote-asset amount.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub quote_amount: Decimal,
    /// Execution price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Venue-reported signed fee with asset ownership left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub fee: Decimal,
    /// Venue-reported fee asset, which may be empty or omitted.
    #[serde(default)]
    pub fee_asset: String,
    /// Venue taker-side value preserved without framework-side interpretation.
    pub taker: String,
    /// Venue creation timestamp.
    pub created_at: String,
}

/// One perpetual order in the observed account-history wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpOrderRecord {
    /// Exact decimal venue order ID.
    pub order_id: String,
    /// Owning subaccount address.
    pub owner: String,
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue long-side flag preserved without framework-side interpretation.
    pub is_long: bool,
    /// Requested quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub size: Decimal,
    /// Submitted order price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Venue average fill price when any quantity has executed.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub avg_fill_price: Option<Decimal>,
    /// Venue order-type value preserved without interpretation.
    pub order_type: String,
    /// Venue creation timestamp preserved without interpretation.
    pub create_time: String,
    /// Venue update timestamp preserved without interpretation when supplied.
    #[serde(default)]
    pub updated_time: Option<String>,
    /// Venue-reported leverage.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub leverage: Decimal,
    /// Venue-reported slippage parameter.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub slippage: Decimal,
    /// Venue order-status value preserved without interpretation.
    pub status: String,
    /// Executed quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub size_filled: Decimal,
    /// Remaining quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub size_remain: Decimal,
    /// Optional take-profit price.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub take_profit: Option<Decimal>,
    /// Optional stop-loss price.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub stop_loss: Option<Decimal>,
    /// Venue reduce-only flag.
    pub reduce_only: bool,
    /// Venue post-only value preserved without interpretation.
    pub post_only: String,
    /// Venue block height associated with the order observation.
    pub height: u64,
    /// Venue transaction hash.
    pub tx_hash: String,
    /// Venue transaction-hash type preserved without interpretation.
    pub tx_hash_type: String,
    /// Venue cancellation reason.
    pub cancel_reason: String,
    /// Optional cancellation block height.
    pub cancel_height: Option<u64>,
    /// Venue-reported signed fee.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub fee: Decimal,
}

/// One Spot order in the observed account-history wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotOrderRecord {
    /// Deployment-provided bytes32 pair identity.
    pub pair: String,
    /// Venue market name.
    pub pair_name: String,
    /// Exact decimal venue order ID.
    pub order_id: String,
    /// Venue buy- or sell-side value preserved without framework-side interpretation.
    pub order_side: String,
    /// Submitted order price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Venue average fill price when any quantity has executed.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub avg_fill_price: Option<Decimal>,
    /// Venue-reported slippage parameter.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub slippage: Decimal,
    /// Venue creation timestamp.
    pub create_time: String,
    /// Venue block height associated with the order observation.
    pub block_number: u64,
    /// Owning subaccount address.
    pub maker: String,
    /// Venue order-status value preserved without interpretation.
    pub status: String,
    /// Venue price-type value preserved without interpretation.
    pub price_type: String,
    /// Requested base-asset quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub base_amount: Decimal,
    /// Remaining base-asset quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub base_remaining_amount: Decimal,
    /// Requested quote-asset amount reported by the venue.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub quote_amount: Decimal,
    /// Remaining quote-asset amount reported by the venue.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub quote_remaining_amount: Decimal,
    /// Venue post-only value preserved without interpretation.
    pub post_only: String,
    /// Venue reduce-only flag.
    pub reduce_only: bool,
    /// Venue transaction hash.
    pub tx_hash: String,
    /// Venue transaction-hash type preserved without interpretation.
    pub tx_hash_type: String,
    /// Venue-reported signed fee with asset ownership left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub fee: Decimal,
    /// Venue cancellation reason when supplied.
    #[serde(default)]
    pub cancel_reason: Option<String>,
    /// Venue cancellation block height when supplied.
    #[serde(default)]
    pub cancel_height: Option<u64>,
}

/// Nullable liquidation-price observation for one subaccount and perpetual market.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpLiquidationPrice {
    /// Queried subaccount address.
    pub address: String,
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue perpetual market name.
    pub market_name: String,
    /// Current venue liquidation price when applicable.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub liquidate_price: Option<Decimal>,
}

/// One exact perpetual funding-rate observation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXFundingRateRecord {
    /// Funding rate represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate: Decimal,
    /// UTC interval bucket timestamp in Unix milliseconds, not the payment time.
    pub time: u64,
}

/// One cursor-paginated perpetual funding-rate response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXFundingRatePage {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Funding-rate observations in venue response order.
    pub details: Vec<DeepXFundingRateRecord>,
    /// Opaque cursor for the next page when present.
    pub next_cursor: Option<String>,
    /// Whether the venue reports another page.
    pub has_next: bool,
}

/// One exact perpetual long-short ratio observation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXLongShortRatioRecord {
    /// Venue-reported long-to-short position ratio.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub long_short_ratio: Decimal,
    /// UTC interval bucket timestamp in Unix milliseconds.
    pub time: u64,
}

/// One cursor-paginated perpetual long-short ratio response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXLongShortRatioPage {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Ratio observations in venue response order.
    pub details: Vec<DeepXLongShortRatioRecord>,
    /// Opaque cursor for the next page when present.
    pub next_cursor: Option<String>,
    /// Whether the venue reports another page.
    pub has_next: bool,
}

/// One exact perpetual open-interest observation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXOpenInterestRecord {
    /// Venue-reported total open interest with units left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_oi: Decimal,
    /// Venue-reported long-to-short ratio.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub long_short_ratio: Decimal,
    /// Number of long positions.
    pub long_position_count: u64,
    /// Number of short positions.
    pub short_position_count: u64,
    /// Observation timestamp in Unix milliseconds.
    pub statistic_time: u64,
}

/// One perpetual open-interest response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXOpenInterestPage {
    /// Venue pair name.
    pub pair: String,
    /// Open-interest observations in venue response order.
    pub details: Vec<DeepXOpenInterestRecord>,
}
/// One raw perpetual trade in venue response form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpTrade {
    /// Venue trade identity.
    pub id: u64,
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Buyer order identity.
    pub buyer_order_id: String,
    /// Buyer account address.
    pub buyer: String,
    /// Seller order identity.
    pub seller_order_id: String,
    /// Seller account address.
    pub seller: String,
    /// Execution price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Execution quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub size: Decimal,
    /// Buyer leverage reported by the venue.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub buyer_leverage: Decimal,
    /// Seller leverage reported by the venue.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub seller_leverage: Decimal,
    /// Venue timestamp preserved without interpretation.
    pub created_at: String,
    /// Venue fill-direction value preserved without interpretation.
    pub filled_direction: String,
    /// Venue taker-role value preserved without interpretation.
    pub taker: String,
    /// Taker fee represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub taker_fee: Decimal,
    /// Maker fee represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maker_fee: Decimal,
}

/// One cursor-paginated raw perpetual trades response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpTradesPage {
    /// Trades in venue response order.
    pub items: Vec<DeepXPerpTrade>,
    /// Opaque cursor for the next page when present.
    pub next_cursor: Option<String>,
    /// Whether the venue reports another page.
    pub has_next: bool,
}

/// One raw Spot execution in the observed market-trade wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotTrade {
    /// Venue trade identity.
    pub id: u64,
    /// Exact decimal sell-order identity.
    pub sell_id: String,
    /// Seller AccountId20.
    pub seller: String,
    /// Exact decimal buy-order identity.
    pub buy_id: String,
    /// Buyer AccountId20.
    pub buyer: String,
    /// Venue market name.
    pub pair_name: String,
    /// Deployment-provided bytes32 pair identity.
    pub pair: String,
    /// Venue execution timestamp in RFC 3339 format.
    pub trade_time: String,
    /// Execution price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Executed base-asset amount.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub base_amount: Decimal,
    /// Executed quote-asset amount.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub quote_amount: Decimal,
    /// Venue-reported taker fee with asset semantics left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub taker_fee: Decimal,
    /// Venue-reported maker fee with asset semantics left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maker_fee: Decimal,
    /// Venue token-value observation with units left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub token_value: Decimal,
    /// Venue block height associated with the execution.
    pub height: u64,
    /// Venue taker-role value preserved without framework-side interpretation.
    pub taker: String,
}

/// One globally cursor-paginated raw Spot trades response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotTradesPage {
    /// Trades in venue response order.
    pub items: Vec<DeepXSpotTrade>,
    /// Opaque cursor for the next page when present.
    pub next_cursor: Option<String>,
    /// Whether the venue reports another page.
    pub has_next: bool,
    /// Venue-reported number of rows matching the request filters.
    pub total: u64,
}

/// One public subaccount-directory record in the observed REST wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSubaccountDirectoryRecord {
    /// Wallet address reported as the subaccount owner.
    pub owner: String,
    /// Subaccount address.
    pub subaccount: String,
    /// Venue subaccount name.
    pub name: String,
    /// Optional venue status value, which is null in the captured response.
    pub status: Option<String>,
    /// Block height associated with subaccount creation.
    pub height: u64,
    /// Creation time in the observed RFC 3339 wire form.
    pub created_at: String,
}
/// One raw perpetual candle in venue response form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXPerpCandle {
    /// Traded quantity reported for the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub volume: Decimal,
    /// Highest execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub high: Decimal,
    /// Lowest execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub low: Decimal,
    /// First execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub open: Decimal,
    /// Last execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub close: Decimal,
    /// Venue bucket timestamp in Unix milliseconds.
    pub time: u64,
}

/// One raw perpetual candle response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXPerpCandlesPage {
    /// Venue pair name.
    pub pair: String,
    /// Candles in venue response order.
    pub details: Vec<DeepXPerpCandle>,
}

/// One perpetual market volume-statistics window.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpVolume {
    /// Venue-reported aggregate volume with units left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_volume: Decimal,
    /// Venue-reported number of trades in the window.
    pub trade_count: u64,
    /// Inclusive or exclusive window start in Unix milliseconds, left uninterpreted.
    pub start_time: u64,
    /// Inclusive or exclusive window end in Unix milliseconds, left uninterpreted.
    pub end_time: u64,
    /// Venue statistics timestamp in Unix milliseconds.
    pub statistic_time: u64,
}

/// One raw perpetual last price without observation-time semantics.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct DeepXPerpLastPrice(
    /// Venue-reported last price represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub Decimal,
);

/// One server-provided level in a perpetual order-book snapshot.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpOrderBookLevel {
    /// Aggregated level price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Aggregated base quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub qty: Decimal,
    /// Server-provided quote notional, without recomputation or inferred rounding.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub value: Decimal,
    /// Deployment-provided market identity echoed on the level.
    pub market_id: u64,
}

/// One exact server-generated perpetual order-book snapshot.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpOrderBook {
    /// Deployment-provided market identity.
    pub market_id: u64,
    /// Opaque venue update sequence.
    pub last_update_id: u64,
    /// Server engine time in Unix milliseconds.
    pub engine_time: u64,
    /// Bid levels in server order.
    pub order_buy_list: Vec<DeepXPerpOrderBookLevel>,
    /// Ask levels in server order.
    pub order_sell_list: Vec<DeepXPerpOrderBookLevel>,
    /// Venue latest-price observation, which may be zero.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub latest_price: Decimal,
    /// Venue mid-price observation, retained without tick quantization.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub mid_price: Decimal,
}

/// One raw Spot candle in venue response form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXSpotCandle {
    /// Traded base-asset quantity reported for the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub volume: Decimal,
    /// Highest execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub high: Decimal,
    /// Lowest execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub low: Decimal,
    /// First execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub open: Decimal,
    /// Last execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub close: Decimal,
    /// Venue bucket timestamp in Unix milliseconds.
    pub time: u64,
}

/// One raw Spot candle response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXSpotCandlesPage {
    /// Venue pair name.
    pub pair: String,
    /// Candles in venue response order.
    pub details: Vec<DeepXSpotCandle>,
}

/// One Spot market volume-statistics window.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotVolume {
    /// Venue-reported aggregate base volume with units left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_volume: Decimal,
    /// Venue-reported number of trades in the window.
    pub trade_count: u64,
    /// Inclusive or exclusive window start in Unix milliseconds, left uninterpreted.
    pub start_time: u64,
    /// Inclusive or exclusive window end in Unix milliseconds, left uninterpreted.
    pub end_time: u64,
    /// Venue statistics timestamp in Unix milliseconds.
    pub statistic_time: u64,
}

/// One raw Spot last price without observation-time semantics.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct DeepXSpotLastPrice(
    /// Venue-reported last price represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub Decimal,
);

/// One server-provided level in a Spot order-book snapshot.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXSpotOrderBookLevel {
    /// Aggregated level price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Aggregated base-asset quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub qty: Decimal,
    /// Server-provided quote notional, without recomputation or inferred rounding.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub value: Decimal,
}

/// One exact server-generated Spot order-book snapshot.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotOrderBook {
    /// Deployment-provided bytes32 pair identity.
    pub pair: String,
    /// Venue market name.
    pub pair_name: String,
    /// Opaque venue update sequence.
    pub last_update_id: u64,
    /// Server engine time in Unix milliseconds.
    pub engine_time: u64,
    /// Bid levels in server order.
    pub order_buy_list: Vec<DeepXSpotOrderBookLevel>,
    /// Ask levels in server order.
    pub order_sell_list: Vec<DeepXSpotOrderBookLevel>,
    /// Venue latest-price observation, which may be zero.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub latest_price: Decimal,
    /// Venue mid-price observation.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub mid_price: Decimal,
}

/// DeepX Spot market metadata.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotMarket {
    /// Venue market name.
    pub name: String,
    /// Deployment-provided bytes32 pair identity.
    pub pair: String,
    /// Quote asset address.
    pub quote_address: String,
    /// Quote asset decimal precision.
    pub quote_decimal: u8,
    /// Quote asset symbol.
    pub quote_symbol: String,
    /// Base asset address.
    pub base_address: String,
    /// Base asset decimal precision.
    pub base_decimal: u8,
    /// Base asset symbol.
    pub base_symbol: String,
    /// Taker fee rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub taker_fee_rate: Decimal,
    /// Maker fee rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maker_fee_rate: Decimal,
    /// Current market price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Minimum price increment.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub tick_size: Decimal,
    /// Whether trading is paused.
    pub is_paused: bool,
    /// Maximum permitted price deviation.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub max_deviation_bps: Decimal,
    /// Long-side limit-order guard.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub limit_order_guard_limit_long: Decimal,
    /// Short-side limit-order guard.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub limit_order_guard_limit_short: Decimal,
    /// Latest 24-hour price change rate when available.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub last_24h_price_change_rate: Option<Decimal>,
}

/// DeepX perpetual market metadata required for instrument construction.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpMarket {
    /// Deployment-provided market identity.
    pub id: u64,
    /// Venue market name.
    pub name: String,
    /// Base asset symbol.
    pub base_symbol: String,
    /// Base asset address.
    pub base_address: String,
    /// Base asset decimal precision.
    pub base_decimal: u8,
    /// Quote market identity.
    pub quote_market_id: u64,
    /// Quote asset symbol.
    pub quote_symbol: String,
    /// Quote asset address.
    pub quote_address: String,
    /// Quote asset decimal precision.
    pub quote_decimal: u8,
    /// Venue network label.
    pub network: String,
    /// Venue-reported market height.
    pub height: u64,
    /// Current funding rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate: Decimal,
    /// Cumulative funding index.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub cumulative_funding_index: Decimal,
    /// Latest funding-rate timestamp in milliseconds.
    pub last_funding_rate_time: u64,
    /// Latest funding calculation timestamp in milliseconds.
    #[serde(rename = "lastCaclFundingRateTime")]
    pub last_calc_funding_rate_time: u64,
    /// Current oracle price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub oracle_price: Decimal,
    /// Current mark price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub mark_price: Decimal,
    /// Latest 24-hour price change rate when available.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub last_24h_price_change_rate: Option<Decimal>,
    /// Maximum permitted price deviation.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub max_deviation_bps: Decimal,
    /// Initial margin ratio.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub initial_margin_ratio: Decimal,
    /// Maintenance margin ratio.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maintenance_margin_ratio: Decimal,
    /// Maximum number of active orders.
    pub max_active_orders: u32,
    /// Taker fee rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub taker_fee_rate: Decimal,
    /// Maker fee rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maker_fee_rate: Decimal,
    /// Minimum order quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub order_spec_min_qty: Decimal,
    /// Minimum price increment.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub order_spec_tick_size: Decimal,
    /// Minimum quantity increment.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub order_spec_step_size: Decimal,
    /// Minimum order notional.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub order_spec_min_notional: Decimal,
    /// Long-side limit-order guard.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub limit_order_guard_limit_long: Decimal,
    /// Short-side limit-order guard.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub limit_order_guard_limit_short: Decimal,
    /// Current open interest.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub open_interest: Decimal,
    /// Number of open long positions.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub long_open_pos_num: Decimal,
    /// Number of open short positions.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub short_open_pos_num: Decimal,
    /// Base interest rate used for funding.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub base_interest_rate: Decimal,
    /// Impact margin value.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub impact_margin_value: Decimal,
    /// Funding-rate change cap.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate_change_cap: Decimal,
    /// Funding-rate change floor.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate_change_floor: Decimal,
    /// Funding-rate clamp upper bound.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate_clamp_upper_bound: Decimal,
    /// Funding-rate clamp lower bound.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate_clamp_lower_bound: Decimal,
    /// Liquidation duration.
    pub liquidation_duration: u64,
    /// Liquidity-bucket slippage step.
    pub liquidity_bucket_slippage_step: u64,
    /// Liquidity-bucket slippage limit.
    pub liquidity_bucket_slippage_limit: u64,
    /// Liquidation dust value in venue units.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub liquidation_dust_value: Decimal,
    /// Liquidator fee share in venue units.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub liquidator_share_fee_rate: Decimal,
    /// Insurance-fund fee share in venue units.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub insurance_fund_share_fee_rate: Decimal,
    /// Optional deployer address.
    pub deployer: Option<String>,
    /// Optional deployer delegate address.
    pub deployer_delegate: Option<String>,
    /// Optional deployer fee-recipient address.
    pub deployer_fee_recipient: Option<String>,
    /// Optional deployer builder fee in basis points.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub deployer_builder_fee_bps: Option<Decimal>,
    /// Whether the market is restricted to isolated margin by its deployer.
    pub deployer_isolated_margin_only: Option<bool>,
    /// Whether trading is paused.
    pub is_paused: bool,
    /// Whether the market is deleted.
    pub is_deleted: bool,
}

/// DeepX perpetual market details returned by the single-market lookup endpoints.
///
/// Unlike [`DeepXPerpMarket`], this response does not include the order tick/step sizes, market
/// height, network label, open interest, active-order limit, deletion state, or 24-hour change.
/// It is therefore not sufficient for instrument construction.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpMarketLookup {
    /// Deployment-provided market identity.
    pub id: u64,
    /// Venue market name.
    pub name: String,
    /// Base asset symbol.
    pub base_symbol: String,
    /// Base asset address.
    pub base_address: String,
    /// Base asset decimal precision.
    pub base_decimal: u8,
    /// Quote market identity.
    pub quote_market_id: u64,
    /// Quote asset symbol.
    pub quote_symbol: String,
    /// Quote asset address.
    pub quote_address: String,
    /// Quote asset decimal precision.
    pub quote_decimal: u8,
    /// Current funding rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate: Decimal,
    /// Cumulative funding index.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub cumulative_funding_index: Decimal,
    /// Latest funding-rate timestamp in milliseconds.
    pub last_funding_rate_time: u64,
    /// Latest funding calculation timestamp in milliseconds.
    #[serde(rename = "lastCaclFundingRateTime")]
    pub last_calc_funding_rate_time: u64,
    /// Current oracle price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub oracle_price: Decimal,
    /// Current mark price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub mark_price: Decimal,
    /// Maximum permitted price deviation.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub max_deviation_bps: Decimal,
    /// Initial margin ratio.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub initial_margin_ratio: Decimal,
    /// Maintenance margin ratio.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maintenance_margin_ratio: Decimal,
    /// Taker fee rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub taker_fee_rate: Decimal,
    /// Maker fee rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maker_fee_rate: Decimal,
    /// Minimum order quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub order_spec_min_qty: Decimal,
    /// Minimum order notional.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub order_spec_min_notional: Decimal,
    /// Long-side limit-order guard.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub limit_order_guard_limit_long: Decimal,
    /// Short-side limit-order guard.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub limit_order_guard_limit_short: Decimal,
    /// Impact margin value.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub impact_margin_value: Decimal,
    /// Funding-rate clamp upper bound.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate_clamp_upper_bound: Decimal,
    /// Funding-rate clamp lower bound.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate_clamp_lower_bound: Decimal,
    /// Liquidation duration.
    pub liquidation_duration: u64,
    /// Liquidity-bucket slippage step.
    pub liquidity_bucket_slippage_step: u64,
    /// Liquidity-bucket slippage limit.
    pub liquidity_bucket_slippage_limit: u64,
    /// Liquidation dust value in venue units.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub liquidation_dust_value: Decimal,
    /// Liquidator fee share in venue units.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub liquidator_share_fee_rate: Decimal,
    /// Insurance-fund fee share in venue units.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub insurance_fund_share_fee_rate: Decimal,
    /// Optional deployer address.
    pub deployer: Option<String>,
    /// Optional deployer delegate address.
    pub deployer_delegate: Option<String>,
    /// Optional deployer fee-recipient address.
    pub deployer_fee_recipient: Option<String>,
    /// Optional deployer builder fee in basis points.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub deployer_builder_fee_bps: Option<Decimal>,
    /// Whether the market is restricted to isolated margin by its deployer.
    pub deployer_isolated_margin_only: Option<bool>,
    /// Whether trading is paused.
    pub is_paused: bool,
}

/// Exact account-wide collateral and maintenance requirement from the current REST response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXExactMargin {
    /// Exact collateral in USDC.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_collateral: Decimal,
    /// Exact maintenance margin requirement in USDC.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maint_margin_req: Decimal,
    /// Collateral divided by maintenance requirement, truncated by the venue to eight places.
    #[serde(
        rename = "uniMMR",
        deserialize_with = "optional_exact_decimal::deserialize"
    )]
    pub uni_mmr: Option<Decimal>,
}

/// One candidate in a spot ADL discovery response, with amounts retained as raw integers.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotAdlCandidate {
    /// Candidate subaccount address.
    pub subaccount: String,
    /// Venue-reported available cover amount in its raw integer unit.
    pub available_amount: String,
    /// Venue-reported borrow amount in its raw integer unit.
    pub borrow_amount: String,
}

/// Spot ADL candidate discovery response, preserving all amounts without financial scaling.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotAdlCandidates {
    /// Target subaccount whose liability is being considered.
    pub target_subaccount: String,
    /// Asset proposed for covering the liability.
    pub asset: String,
    /// Liability asset symbol.
    pub liability: String,
    /// Requested minimum cover amount in the endpoint's raw integer unit.
    pub min_cover_amount: String,
    /// Venue-reported aggregate covered amount in its raw integer unit.
    pub covered_amount: String,
    /// Candidate subaccounts in venue response order; ranking is not inferred.
    pub candidates: Vec<DeepXSpotAdlCandidate>,
}

/// Invitation binding status for one wallet address.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXInvitationStatus {
    /// Wallet queried by the request.
    pub address: String,
    /// Whether the wallet is currently bound to an invitation code.
    pub is_bound: bool,
}

/// Response shapes returned by the invitation binding endpoint.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DeepXInvitationStatusWire {
    /// Current testnet response shape.
    Flag(bool),
    /// OpenAPI-documented response example shape.
    Detailed(DeepXInvitationStatus),
}

/// Stable claim identity within an otherwise uninterpreted quota task response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXQuotaClaimIdentity {
    /// Venue claim ID.
    pub id: u64,
}

/// Claim identity and exact backend payload for an asynchronous quota task.
#[derive(Clone, Debug)]
pub struct DeepXQuotaClaim {
    /// Venue claim ID bound to the request.
    pub id: u64,
    data: Box<RawValue>,
}

impl DeepXQuotaClaim {
    pub(crate) fn new(id: u64, data: Box<RawValue>) -> Self {
        Self { id, data }
    }

    /// Returns the original task payload, including fields without a stable schema.
    #[must_use]
    pub fn data(&self) -> &RawValue {
        &self.data
    }

    /// Returns the original task payload without changing its numeric lexemes.
    #[must_use]
    pub fn into_raw(self) -> Box<RawValue> {
        self.data
    }
}

/// Chain-defined permission mode reported for one wallet delegate.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub enum DeepXDelegateMode {
    /// Permits order placement and cancellation only.
    PlaceOrCancelOrder,
    /// Chain mode for deposit and withdrawal operations.
    DepositOrWithdraw,
    /// Chain mode for subaccount-management operations.
    UpdateSubaccount,
    /// Disables delegate permissions.
    Disable,
}

impl DeepXDelegateMode {
    /// Returns the exact chain variant name used by the REST response.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PlaceOrCancelOrder => "PlaceOrCancelOrder",
            Self::DepositOrWithdraw => "DepositOrWithdraw",
            Self::UpdateSubaccount => "UpdateSubaccount",
            Self::Disable => "Disable",
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn invitation_status_accepts_documented_and_observed_shapes() {
        assert_eq!(
            serde_json::from_str::<DeepXInvitationStatusWire>("true").unwrap(),
            DeepXInvitationStatusWire::Flag(true)
        );
        let detailed =
            r#"{"address":"0x1111111111111111111111111111111111111111","isBound":false}"#;
        assert_eq!(
            serde_json::from_str::<DeepXInvitationStatusWire>(detailed).unwrap(),
            DeepXInvitationStatusWire::Detailed(DeepXInvitationStatus {
                address: "0x1111111111111111111111111111111111111111".to_string(),
                is_bound: false,
            })
        );
    }

    #[rstest]
    fn quota_claim_identity_reads_exact_numeric_id() {
        let identity: DeepXQuotaClaimIdentity =
            serde_json::from_str(r#"{"id":9007199254740993}"#).unwrap();
        assert_eq!(identity.id, 9_007_199_254_740_993);
    }
}
