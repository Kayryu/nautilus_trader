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

//! Wire models for verified DeepX public market responses.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, de::Error as _};
use serde_json::value::RawValue;

fn parse_decimal(raw: &str) -> Result<Decimal, rust_decimal::Error> {
    Decimal::from_str(raw).or_else(|_| Decimal::from_scientific(raw))
}

pub(crate) mod exact_decimal {
    use super::*;

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };

        parse_decimal(&value).map_err(D::Error::custom)
    }
}

mod optional_exact_decimal {
    use super::*;

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        if raw.get() == "null" {
            return Ok(None);
        }
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };

        parse_decimal(&value).map(Some).map_err(D::Error::custom)
    }
}

mod optional_none_decimal {
    use super::*;

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        if raw.get() == "null" {
            return Ok(None);
        }
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };
        if value == "none" {
            return Ok(None);
        }

        parse_decimal(&value).map(Some).map_err(D::Error::custom)
    }
}

mod exact_u128 {
    use super::*;

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };
        value.parse().map_err(D::Error::custom)
    }
}

mod exact_i128 {
    use super::*;

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<i128, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };
        value.parse().map_err(D::Error::custom)
    }
}

mod optional_exact_u128 {
    use super::*;

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Option<u128>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        if raw.get() == "null" {
            return Ok(None);
        }
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };
        value.parse().map(Some).map_err(D::Error::custom)
    }
}

mod optional_exact_u64 {
    use super::*;

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        if raw.get() == "null" {
            return Ok(None);
        }
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };
        value.parse().map(Some).map_err(D::Error::custom)
    }
}

/// DeepX response code, including API-layer and on-chain pallet failures.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DeepXResponseCode {
    /// Success or API-layer error code.
    Api(u16),
    /// On-chain runtime revert code in `pallet_index_error_index` format.
    Pallet(String),
}

impl DeepXResponseCode {
    /// Returns whether this is the venue success code.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Api(200))
    }
}

impl std::fmt::Display for DeepXResponseCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api(code) => code.fmt(f),
            Self::Pallet(code) => code.fmt(f),
        }
    }
}

/// Standard DeepX API response envelope.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXApiResponse<T> {
    /// Venue response code.
    pub code: DeepXResponseCode,
    /// Human-readable venue response message.
    pub msg: String,
    /// Response payload.
    pub data: T,
    /// Venue failure indicator.
    pub fail: bool,
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

/// Lending interest-rate history in venue response order.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXLendingInterestRateHistory {
    /// Exact APR observations.
    pub details: Vec<DeepXLendingInterestRateRecord>,
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

/// Lending pool status history in venue response order.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXLendingStatusHistory {
    /// Exact pool observations.
    pub details: Vec<DeepXLendingStatusRecord>,
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

/// One cursor-paginated account response with uninterpreted record payloads.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXRawAccountPage {
    /// Record payloads in venue response order, preserving exact JSON numeric lexemes.
    pub items: Vec<Box<RawValue>>,
    /// Opaque cursor for the next page when present.
    pub next_cursor: Option<String>,
    /// Whether the venue reports another page.
    pub has_next: bool,
}

/// One typed cursor-paginated account response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXAccountPage<T> {
    /// Records in venue response order.
    pub items: Vec<T>,
    /// Opaque cursor for the next page when present.
    pub next_cursor: Option<String>,
    /// Whether the venue reports another page.
    pub has_next: bool,
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

/// Wallet-owned subaccounts returned by the account directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXWalletSubaccounts {
    /// Wallet address used for the directory query.
    wallet: String,
    /// Subaccount addresses in venue response order.
    pub addresses: Vec<String>,
}

impl DeepXWalletSubaccounts {
    pub(crate) fn new(wallet: String, addresses: Vec<String>) -> Self {
        Self { wallet, addresses }
    }

    /// Returns the wallet address used for the directory query.
    #[must_use]
    pub fn wallet(&self) -> &str {
        &self.wallet
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
    /// Backend-reported current activity flag.
    pub active: bool,
}

/// Request-scoped delegate configurations for one wallet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXWalletDelegateAccounts {
    /// Wallet address used for the delegate-directory query.
    wallet: String,
    /// Delegate configurations in venue response order.
    pub accounts: Vec<DeepXDelegateAccount>,
}

impl DeepXWalletDelegateAccounts {
    pub(crate) fn new(wallet: String, accounts: Vec<DeepXDelegateAccount>) -> Self {
        Self { wallet, accounts }
    }

    /// Returns the wallet address used for the delegate-directory query.
    #[must_use]
    pub fn wallet(&self) -> &str {
        &self.wallet
    }
}

/// Request-scoped wallets bound to one delegate account.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXDelegateWallets {
    /// Delegate address used for the reverse-directory query.
    delegate: String,
    /// Wallet addresses in venue response order.
    pub wallets: Vec<String>,
}

impl DeepXDelegateWallets {
    pub(crate) fn new(delegate: String, wallets: Vec<String>) -> Self {
        Self { delegate, wallets }
    }

    /// Returns the delegate address used for the reverse-directory query.
    #[must_use]
    pub fn delegate(&self) -> &str {
        &self.delegate
    }
}

/// One fully validated point-in-time subaccount snapshot.
#[derive(Clone, Debug)]
pub struct DeepXSubaccountSnapshot {
    /// Subaccount profile bound to the requested wallet authority.
    pub profile: DeepXSubaccountProfile,
    /// Exact lending balances bound to the profile address.
    pub balances: DeepXSubaccountBalances,
    /// Exact equity summary bound to the profile address.
    pub equity: DeepXSubaccountEquity,
    /// Request-scoped collateral and margin requirement summary.
    pub margin_ratio: DeepXSubaccountMarginRatio,
}

/// Failure-atomic point-in-time snapshots for every subaccount in one wallet directory.
///
/// The constituent REST reads are not block-pinned and therefore do not form an atomic venue
/// snapshot. Failure atomicity means callers receive either every locally validated response or
/// an error, never a partial collection.
#[derive(Clone, Debug)]
pub struct DeepXWalletAccountSnapshot {
    /// Validated wallet directory in venue response order.
    pub directory: DeepXWalletSubaccounts,
    /// Validated subaccount snapshots in the same order as `directory.addresses`.
    pub subaccounts: Vec<DeepXSubaccountSnapshot>,
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

/// Exact lending balances for one subaccount.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSubaccountBalances {
    /// Subaccount address.
    pub address: String,
    /// Asset balances in venue response order.
    pub assets: Vec<DeepXSubaccountBalanceAsset>,
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

/// Exact account-wide collateral and margin requirement observation.
///
/// The response does not identify whether `margin_required` uses initial or maintenance weights,
/// so this model is not a framework [`nautilus_model::types::MarginBalance`] conversion.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSubaccountMarginRatio {
    /// Account collateral value in the endpoint's USD unit.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub collateral: Decimal,
    /// Account-wide margin requirement in the endpoint's USD unit.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub margin_required: Decimal,
    /// Venue margin ratio, absent when the venue reports no applicable ratio.
    #[serde(deserialize_with = "optional_exact_decimal::deserialize")]
    pub margin_ratio: Option<Decimal>,
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
    pub change_type: super::query::DeepXBalanceChangeType,
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

/// One account liquidation record with exact raw chain integer values.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXLiquidationRecord {
    /// Stable backend record ID.
    pub id: u64,
    /// Per-account chain liquidation sequence ID, which may start at zero.
    pub liquidation_id: u64,
    /// Closed chain liquidation classification.
    pub liquidation_type: super::query::DeepXLiquidationType,
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
    pub history_type: super::query::DeepXQuotaHistoryType,
    /// Wallet that owns this quota history.
    pub owner_address: String,
    /// Purchase payer account classification, absent for non-purchase records.
    pub buyer_type: Option<super::query::DeepXQuotaBuyerType>,
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

/// Spot orders for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotWalletSubaccountOrders {
    /// Subaccount identity reported for this group.
    pub subaccount: String,
    /// Orders and repeated global page metadata for this subaccount group.
    pub orders: DeepXAccountPage<DeepXSpotOrderRecord>,
}

/// One market group in a wallet-scoped Spot order response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotWalletOrderMarket {
    /// Deployment-provided bytes32 pair identity.
    pub pair: String,
    /// Venue market name.
    pub name: String,
    /// Per-subaccount order groups returned for this market.
    pub subaccounts: Vec<DeepXSpotWalletSubaccountOrders>,
}

/// One normalized globally paginated Spot wallet-order response page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotWalletOrdersPage {
    /// Market and subaccount groups in venue response order.
    pub markets: Vec<DeepXSpotWalletOrderMarket>,
    /// Opaque global cursor repeated consistently by every nonempty nested group.
    pub next_cursor: Option<String>,
    /// Whether every nonempty nested group reports another global page.
    pub has_next: bool,
}

/// Spot trades for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotWalletSubaccountTrades {
    /// Subaccount identity reported for this group.
    pub subaccount: String,
    /// Trades and repeated global page metadata for this subaccount group.
    pub trades: DeepXAccountPage<DeepXSpotAccountTradeRecord>,
}

/// One market group in a wallet-scoped Spot trade response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotWalletTradeMarket {
    /// Deployment-provided bytes32 pair identity.
    pub pair: String,
    /// Venue market name.
    pub name: String,
    /// Per-subaccount trade groups returned for this market.
    pub subaccounts: Vec<DeepXSpotWalletSubaccountTrades>,
}

/// One normalized globally paginated Spot wallet-trade response page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotWalletTradesPage {
    /// Market and subaccount groups in venue response order.
    pub markets: Vec<DeepXSpotWalletTradeMarket>,
    /// Opaque global cursor repeated consistently by every nonempty nested group.
    pub next_cursor: Option<String>,
    /// Whether every nonempty nested group reports another global page.
    pub has_next: bool,
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
/// The adapter preserves these values without assigning token precision or converting them into
/// framework money, PnL, or account-state events.
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

/// Perpetual trades for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpWalletSubaccountTrades {
    /// Subaccount identity reported for this group.
    pub subaccount: String,
    /// Independently paginated trades for this subaccount.
    pub trades: DeepXAccountPage<DeepXPerpAccountTradeRecord>,
}

/// One market group in a wallet-scoped perpetual trade response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpWalletTradeMarket {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue market name.
    pub market_name: String,
    /// Per-subaccount trade pages returned for this market.
    pub subaccounts: Vec<DeepXPerpWalletSubaccountTrades>,
}

/// Perpetual orders for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpWalletSubaccountOrders {
    /// Subaccount identity reported for this group.
    pub subaccount: String,
    /// Orders and repeated global page metadata for this subaccount group.
    pub orders: DeepXAccountPage<DeepXPerpOrderRecord>,
}

/// One market group in a wallet-scoped perpetual order response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpWalletOrderMarket {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue market name.
    pub market_name: String,
    /// Per-subaccount order groups returned for this market.
    pub subaccounts: Vec<DeepXPerpWalletSubaccountOrders>,
}

/// One normalized globally paginated wallet-order response page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpWalletOrdersPage {
    /// Market and subaccount groups in venue response order.
    pub markets: Vec<DeepXPerpWalletOrderMarket>,
    /// Opaque global cursor repeated consistently by every nested group.
    pub next_cursor: Option<String>,
    /// Whether every nested group reports another global page.
    pub has_next: bool,
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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const PERP_VOLUME_1H_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_volume_1h.json");
    const PERP_VOLUME_1H_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/perp_volume_1h.manifest.json");
    const SPOT_TRADES_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_trades_eth_usdc_page_1.json");
    const SPOT_TRADES_ETH_USDC_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/spot_trades_eth_usdc_page_1.manifest.json");
    const SPOT_CANDLES_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_candles_eth_usdc_1m.json");
    const SPOT_CANDLES_ETH_USDC_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/spot_candles_eth_usdc_1m.manifest.json");
    const SPOT_LAST_PRICE_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_last_price_eth_usdc.json");
    const SPOT_LAST_PRICE_ETH_USDC_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/spot_last_price_eth_usdc.manifest.json");
    const SPOT_VOLUME_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_volume_eth_usdc_1h.json");
    const SPOT_VOLUME_ETH_USDC_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/spot_volume_eth_usdc_1h.manifest.json");
    const SPOT_ORDER_BOOK_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_order_book_eth_usdc_tick_001.json");
    const SPOT_ORDER_BOOK_ETH_USDC_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/spot_order_book_eth_usdc_tick_001.manifest.json"
    );
    const PERP_ORDER_BOOK_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_order_book_eth_usdc_tick_001.json");
    const PERP_ORDER_BOOK_ETH_USDC_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/perp_order_book_eth_usdc_tick_001.manifest.json"
    );
    const SPOT_MARKETS_SPEC369_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_markets_spec369.json");
    const SPOT_MARKETS_SPEC369_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/spot_markets_spec369.manifest.json");
    const SPOT_MARKET_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_market_eth_usdc_by_pair.json");
    const SPOT_MARKET_ETH_USDC_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/spot_market_eth_usdc_by_pair.manifest.json");
    const PERP_MARKET_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_market_eth_usdc_by_id.json");
    const PERP_MARKET_ETH_USDC_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/perp_market_eth_usdc_by_id.manifest.json");
    const RUNTIME_MANIFEST: &str = include_str!(
        "../../test_data/runtime/testnet/genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/manifest.json"
    );
    const PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_history_orders_account_market_3.json");
    const PERP_HISTORY_ORDERS_ACCOUNT_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/perp_history_orders_account_market_3.manifest.json"
    );
    const SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_history_orders_eth_usdc_page_1.json");
    const SPOT_HISTORY_ORDERS_ETH_USDC_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/spot_history_orders_eth_usdc_page_1.manifest.json"
    );
    const SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_account_trades_eth_usdc_page_1.json");
    const SPOT_ACCOUNT_TRADES_ETH_USDC_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/spot_account_trades_eth_usdc_page_1.manifest.json"
    );
    const SPOT_WALLET_ORDERS_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_wallet_orders_eth_usdc_page_1.json");
    const SPOT_WALLET_ORDERS_ETH_USDC_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/spot_wallet_orders_eth_usdc_page_1.manifest.json"
    );
    const SPOT_WALLET_TRADES_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_wallet_trades_eth_usdc_page_1.json");
    const SPOT_WALLET_TRADES_ETH_USDC_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/spot_wallet_trades_eth_usdc_page_1.manifest.json"
    );
    const SPOT_ORDER_BY_ID_CANCELED_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_order_by_id_canceled.json");
    const SPOT_ORDER_BY_ID_CANCELED_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/spot_order_by_id_canceled.manifest.json");
    const PERP_ORDER_BY_TX_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_order_by_tx.json");
    const PERP_ORDER_BY_TX_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/perp_order_by_tx.manifest.json");
    const PERP_FUNDING_FEES_ACCOUNT_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_funding_fees_account_market_3.json");
    const PERP_FUNDING_FEES_ACCOUNT_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/perp_funding_fees_account_market_3.manifest.json"
    );
    const WALLET_FUNDING_FEES_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_funding_fees_page_2.json");
    const WALLET_FUNDING_FEES_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/wallet_funding_fees_page_2.manifest.json");
    const WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE: &str = include_str!(
        "../../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_1.json"
    );
    const WALLET_HOURLY_FUNDING_PAGE_1_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_1.manifest.json"
    );
    const WALLET_HOURLY_FUNDING_PAGE_2_RESPONSE: &str = include_str!(
        "../../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_2.json"
    );
    const WALLET_HOURLY_FUNDING_PAGE_2_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_2.manifest.json"
    );
    const PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_account_trades_account_market_3.json");
    const PERP_ACCOUNT_TRADES_ACCOUNT_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/perp_account_trades_account_market_3.manifest.json"
    );
    const PERP_WALLET_TRADES_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_wallet_trades_page.json");
    const PERP_WALLET_TRADES_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/perp_wallet_trades_page.manifest.json");
    const PERP_WALLET_ORDERS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_wallet_orders_page_1.json");
    const PERP_WALLET_ORDERS_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/perp_wallet_orders_page_1.manifest.json");
    const PERP_POSITIONS_ACCOUNT_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_positions_account_market_3.json");
    const PERP_POSITIONS_ACCOUNT_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/perp_positions_account_market_3.manifest.json");
    const WALLET_SUBACCOUNTS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_subaccounts.json");
    const ALL_SUBACCOUNTS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/all_subaccounts_page_1.json");
    const ALL_SUBACCOUNTS_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/all_subaccounts_page_1.manifest.json");
    const WALLET_DELEGATE_ACCOUNTS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_delegate_accounts.json");
    const WALLET_DELEGATE_ACCOUNTS_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/wallet_delegate_accounts.manifest.json");
    const DELEGATE_DELEGATOR_ACCOUNTS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/delegate_delegator_accounts.json");
    const DELEGATE_DELEGATOR_ACCOUNTS_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/delegate_delegator_accounts.manifest.json");
    const SUBACCOUNT_INFO_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/subaccount_info.json");
    const SUBACCOUNT_BALANCES_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/subaccount_balances.json");
    const SUBACCOUNT_EQUITY_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/subaccount_equity.json");
    const SUBACCOUNT_MARGIN_RATIO_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/subaccount_margin_ratio.json");
    const WALLET_BALANCE_CHANGES_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_balance_changes_page_2.json");
    const WALLET_LIQUIDATION_RECORDS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_liquidation_records_page_1.json");
    const WALLET_LIQUIDATION_RECORDS_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/wallet_liquidation_records_page_1.manifest.json"
    );
    const WALLET_USER_STATS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_user_stats.json");
    const PERP_LIQUIDATION_PRICE_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_liquidation_price_account_market_3.json");
    const WALLET_SUBACCOUNTS_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/wallet_subaccounts.manifest.json");
    const SUBACCOUNT_INFO_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/subaccount_info.manifest.json");
    const SUBACCOUNT_BALANCES_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/subaccount_balances.manifest.json");
    const SUBACCOUNT_EQUITY_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/subaccount_equity.manifest.json");
    const SUBACCOUNT_MARGIN_RATIO_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/subaccount_margin_ratio.manifest.json");
    const WALLET_BALANCE_CHANGES_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/wallet_balance_changes_page_2.manifest.json");
    const WALLET_USER_STATS_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/wallet_user_stats.manifest.json");
    const PERP_LIQUIDATION_PRICE_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/perp_liquidation_price_account_market_3.manifest.json"
    );
    const WALLET_QUOTA_SUMMARY_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_quota_summary.json");
    const WALLET_QUOTA_SUMMARY_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/wallet_quota_summary.manifest.json");
    const QUOTA_HISTORY_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/quota_history_purchase_page.json");
    const QUOTA_HISTORY_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/quota_history_purchase_page.manifest.json");
    const LENDING_ASSETS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/lending_assets.json");
    const LENDING_ASSETS_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/lending_assets.manifest.json");
    const LENDING_INTEREST_RATE_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/lending_interest_rate_usdc.json");
    const LENDING_INTEREST_RATE_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/lending_interest_rate_usdc.manifest.json");
    const LENDING_INTEREST_RATE_HISTORY_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/lending_interest_rate_history_usdc_1h.json");
    const LENDING_INTEREST_RATE_HISTORY_MANIFEST: &str = include_str!(
        "../../test_data/http/testnet/lending_interest_rate_history_usdc_1h.manifest.json"
    );
    const LENDING_STATUS_HISTORY_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/lending_status_history_usdc_1h.json");
    const LENDING_STATUS_HISTORY_MANIFEST: &str =
        include_str!("../../test_data/http/testnet/lending_status_history_usdc_1h.manifest.json");

    #[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
    struct FixtureIdentity {
        genesis_hash: String,
        metadata_sha256: String,
        spec_version: u32,
        transaction_version: u32,
    }

    #[derive(Debug, Deserialize)]
    struct PerpVolumeFixtureManifest {
        deployment: String,
        endpoint_role: String,
        request: PerpVolumeFixtureRequest,
        response: PerpVolumeFixtureResponse,
        runtime_identity: PerpVolumeRuntimeIdentity,
    }

    #[derive(Debug, Deserialize)]
    struct PerpVolumeFixtureRequest {
        method: String,
        path: String,
        query: PerpVolumeFixtureQuery,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PerpVolumeFixtureQuery {
        market_id: u32,
        period: String,
    }

    #[derive(Debug, Deserialize)]
    struct PerpVolumeFixtureResponse {
        status: u16,
        content_type: String,
        payload_path: String,
    }

    #[derive(Debug, Deserialize)]
    struct PerpVolumeRuntimeIdentity {
        #[serde(flatten)]
        identity: FixtureIdentity,
        source_manifest: String,
    }

    #[derive(Debug, Deserialize)]
    struct RuntimeFixtureManifest {
        deployment: String,
        endpoint_role: String,
        identity: FixtureIdentity,
    }

    fn validate_perp_volume_fixture_manifest(
        manifest: &PerpVolumeFixtureManifest,
        runtime_manifest: &RuntimeFixtureManifest,
    ) -> Result<(), String> {
        if manifest.deployment != "testnet"
            || manifest.endpoint_role != "public_rest_market_data"
            || manifest.request.method != "GET"
            || manifest.request.path != "/internal/v1/market/perp/volume"
            || manifest.request.query.market_id != 3
            || manifest.request.query.period != "1h"
            || manifest.response.status != 200
            || manifest.response.content_type != "application/json"
            || manifest.response.payload_path != "perp_volume_1h.json"
            || manifest.runtime_identity.source_manifest
                != "../../runtime/testnet/genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/manifest.json"
            || runtime_manifest.deployment != "testnet"
            || runtime_manifest.endpoint_role != "runtime_identity"
        {
            return Err("unexpected DeepX perpetual volume fixture provenance".to_string());
        }
        if manifest.runtime_identity.identity != runtime_manifest.identity {
            return Err("DeepX perpetual volume fixture runtime identity mismatch".to_string());
        }

        Ok(())
    }

    #[rstest]
    fn preserves_exact_json_number_lexeme() {
        #[derive(Deserialize)]
        struct ExactPrice {
            #[serde(deserialize_with = "exact_decimal::deserialize")]
            price: Decimal,
        }

        let value: ExactPrice = serde_json::from_str(r#"{"price":2453.980000000000001}"#).unwrap();

        assert_eq!(
            value.price,
            "2453.980000000000001".parse::<Decimal>().unwrap()
        );
    }

    #[rstest]
    fn decodes_spot_trade_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(SPOT_TRADES_ETH_USDC_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_TRADES_ETH_USDC_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<DeepXSpotTradesPage> =
            serde_json::from_str(SPOT_TRADES_ETH_USDC_RESPONSE).unwrap();

        assert_eq!(manifest["captured_at"], "2026-09-17T03:27:31Z");
        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["endpoint_role"], "public_rest_market_data");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/market/spot/trades"
        );
        assert_eq!(manifest["request"]["query"]["name"], "ETH/USDC");
        assert_eq!(manifest["request"]["query"]["pageSize"], 2);
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"],
        );
        assert_eq!(response.data.items.len(), 2);
        assert!(response.data.has_next);
        assert!(response.data.next_cursor.is_some());
        assert_eq!(response.data.total, 11_437_005);
        assert_eq!(response.data.items[0].id, 188_405_950_000_036);
        assert_eq!(response.data.items[0].price, Decimal::new(243_436, 2));
        assert_eq!(response.data.items[0].base_amount, Decimal::new(7_039, 4));
        assert_eq!(
            response.data.items[0].quote_amount,
            Decimal::new(1_713_546_004, 6)
        );
        assert_eq!(response.data.items[0].maker_fee, Decimal::new(7_039, 8));
        assert_eq!(response.data.items[1].maker_fee, Decimal::new(955, 8));
    }

    #[rstest]
    fn decodes_spot_market_observation_fixtures_exactly() {
        let candles_manifest: serde_json::Value =
            serde_json::from_str(SPOT_CANDLES_ETH_USDC_MANIFEST).unwrap();
        let candles_digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_CANDLES_ETH_USDC_RESPONSE.as_bytes(),
        );
        let candles: DeepXApiResponse<DeepXSpotCandlesPage> =
            serde_json::from_str(SPOT_CANDLES_ETH_USDC_RESPONSE).unwrap();
        assert_eq!(candles_manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(candles_digest.as_ref()),
            candles_manifest["response"]["payload_sha256"],
        );
        assert_eq!(candles.data.pair, "ETH/USDC");
        assert_eq!(candles.data.details.len(), 3);
        assert_eq!(
            candles.data.details[0].volume,
            "5.6965012112303635".parse::<Decimal>().unwrap()
        );
        assert_eq!(candles.data.details[2].close, Decimal::new(243_569, 2));

        let price_manifest: serde_json::Value =
            serde_json::from_str(SPOT_LAST_PRICE_ETH_USDC_MANIFEST).unwrap();
        let price_digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_LAST_PRICE_ETH_USDC_RESPONSE.as_bytes(),
        );
        let price: DeepXApiResponse<DeepXSpotLastPrice> =
            serde_json::from_str(SPOT_LAST_PRICE_ETH_USDC_RESPONSE).unwrap();
        assert_eq!(
            nautilus_core::hex::encode(price_digest.as_ref()),
            price_manifest["response"]["payload_sha256"],
        );
        assert_eq!(price.data.0, Decimal::new(242_894, 2));

        let volume_manifest: serde_json::Value =
            serde_json::from_str(SPOT_VOLUME_ETH_USDC_MANIFEST).unwrap();
        let volume_digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_VOLUME_ETH_USDC_RESPONSE.as_bytes(),
        );
        let volume: DeepXApiResponse<DeepXSpotVolume> =
            serde_json::from_str(SPOT_VOLUME_ETH_USDC_RESPONSE).unwrap();
        assert_eq!(
            nautilus_core::hex::encode(volume_digest.as_ref()),
            volume_manifest["response"]["payload_sha256"],
        );
        assert_eq!(
            volume.data.total_volume,
            "293.50766491243".parse::<Decimal>().unwrap()
        );
        assert_eq!(volume.data.trade_count, 1_186);
        assert_eq!(volume.data.end_time - volume.data.start_time, 3_600_000);

        let book_manifest: serde_json::Value =
            serde_json::from_str(SPOT_ORDER_BOOK_ETH_USDC_MANIFEST).unwrap();
        let book_digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_ORDER_BOOK_ETH_USDC_RESPONSE.as_bytes(),
        );
        let book: DeepXApiResponse<DeepXSpotOrderBook> =
            serde_json::from_str(SPOT_ORDER_BOOK_ETH_USDC_RESPONSE).unwrap();
        assert_eq!(
            nautilus_core::hex::encode(book_digest.as_ref()),
            book_manifest["response"]["payload_sha256"],
        );
        assert_eq!(book.data.order_buy_list.len(), 20);
        assert_eq!(book.data.order_sell_list.len(), 20);
        assert_eq!(book.data.mid_price, Decimal::new(2_435_745, 3));
        assert_eq!(book.data.order_buy_list[6].qty, Decimal::new(16_613, 4));
        assert_eq!(
            book.data.order_sell_list[0].value,
            Decimal::new(391_070_901, 5)
        );
    }

    #[rstest]
    fn decodes_spot_market_directory_and_lookup_fixtures_exactly() {
        let directory_manifest: serde_json::Value =
            serde_json::from_str(SPOT_MARKETS_SPEC369_MANIFEST).unwrap();
        let directory_digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_MARKETS_SPEC369_RESPONSE.as_bytes(),
        );
        let directory: DeepXApiResponse<Vec<DeepXSpotMarket>> =
            serde_json::from_str(SPOT_MARKETS_SPEC369_RESPONSE).unwrap();
        assert_eq!(directory_manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(directory_digest.as_ref()),
            directory_manifest["response"]["payload_sha256"],
        );
        assert_eq!(directory.data.len(), 2);
        assert_eq!(directory.data[0].name, "SOL/USDC");
        assert_eq!(directory.data[0].base_decimal, 9);
        assert_eq!(
            directory.data[0].last_24h_price_change_rate,
            Some(Decimal::new(303, 2))
        );
        assert_eq!(directory.data[1].name, "ETH/USDC");
        assert_eq!(directory.data[1].tick_size, Decimal::new(1, 2));

        let lookup_manifest: serde_json::Value =
            serde_json::from_str(SPOT_MARKET_ETH_USDC_MANIFEST).unwrap();
        let lookup_digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_MARKET_ETH_USDC_RESPONSE.as_bytes(),
        );
        let lookup: DeepXApiResponse<DeepXSpotMarket> =
            serde_json::from_str(SPOT_MARKET_ETH_USDC_RESPONSE).unwrap();
        assert_eq!(
            nautilus_core::hex::encode(lookup_digest.as_ref()),
            lookup_manifest["response"]["payload_sha256"],
        );
        assert_eq!(lookup.data.price, Decimal::new(244_059, 2));
        assert_eq!(lookup.data.last_24h_price_change_rate, None);
        assert_eq!(lookup.data.base_decimal, 18);
        assert_eq!(lookup.data.quote_decimal, 6);
    }

    #[rstest]
    fn decodes_perp_order_book_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(PERP_ORDER_BOOK_ETH_USDC_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            PERP_ORDER_BOOK_ETH_USDC_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<DeepXPerpOrderBook> =
            serde_json::from_str(PERP_ORDER_BOOK_ETH_USDC_RESPONSE).unwrap();

        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["endpoint_role"], "public_rest_market_data");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/market/perp/order-books"
        );
        assert_eq!(manifest["request"]["query"]["marketId"], 3);
        assert_eq!(manifest["request"]["query"]["tickSize"], "0.01");
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"],
        );
        assert_eq!(response.data.market_id, 3);
        assert_eq!(response.data.last_update_id, 6_923_589);
        assert_eq!(response.data.engine_time, 1_789_626_369_132);
        assert_eq!(response.data.order_buy_list.len(), 20);
        assert_eq!(response.data.order_sell_list.len(), 20);
        assert_eq!(
            response.data.order_buy_list[0].price,
            Decimal::new(24_339, 1)
        );
        assert_eq!(response.data.order_buy_list[1].qty, Decimal::new(2_394, 3));
        assert_eq!(
            response.data.order_sell_list[0].value,
            Decimal::new(140_451_609, 5)
        );
        assert_eq!(response.data.order_sell_list[0].market_id, 3);
        assert_eq!(response.data.latest_price, Decimal::ZERO);
        assert_eq!(response.data.mid_price, Decimal::new(2_434_035, 3));
    }

    #[rstest]
    fn decodes_perp_market_lookup_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(PERP_MARKET_ETH_USDC_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            PERP_MARKET_ETH_USDC_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<DeepXPerpMarketLookup> =
            serde_json::from_str(PERP_MARKET_ETH_USDC_RESPONSE).unwrap();

        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"],
        );
        assert_eq!(response.data.id, 3);
        assert_eq!(response.data.name, "ETH-USDC");
        assert_eq!(response.data.funding_rate, Decimal::new(125, 7));
        assert_eq!(response.data.mark_price, Decimal::new(2_438_185_111, 6));
        assert_eq!(response.data.order_spec_min_qty, Decimal::new(1, 3));
        assert_eq!(response.data.liquidation_dust_value, Decimal::new(50, 0));
        assert_eq!(response.data.last_funding_rate_time, 1_789_623_085_669);
    }

    #[rstest]
    fn decodes_perp_wallet_trade_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(PERP_WALLET_TRADES_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            PERP_WALLET_TRADES_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<Vec<DeepXPerpWalletTradeMarket>> =
            serde_json::from_str(PERP_WALLET_TRADES_RESPONSE).unwrap();

        assert_eq!(manifest["captured_at"], "2026-09-17T03:51:08Z");
        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/perp/trades-by-wallet"
        );
        assert_eq!(
            manifest["request"]["query"]["address"],
            "0x781ed35b167068c93dfadab41dfb680edaca4e50"
        );
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"],
        );
        assert_eq!(response.data.len(), 2);
        assert_eq!(response.data[0].subaccounts.len(), 3);
        assert_eq!(response.data[1].subaccounts.len(), 2);
        let zero_size = &response.data[1].subaccounts[0].trades.items[2];
        assert_eq!(zero_size.id, 50_726_011_012_081);
        assert_eq!(zero_size.price, Decimal::new(8_436, 2));
        assert_eq!(zero_size.size, Decimal::ZERO);
        assert_ne!(
            response.data[1].subaccounts[0].trades.next_cursor,
            response.data[1].subaccounts[1].trades.next_cursor,
        );
    }

    #[rstest]
    fn decodes_perp_wallet_order_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(PERP_WALLET_ORDERS_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            PERP_WALLET_ORDERS_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<Vec<DeepXPerpWalletOrderMarket>> =
            serde_json::from_str(PERP_WALLET_ORDERS_RESPONSE).unwrap();

        assert_eq!(manifest["captured_at"], "2026-09-17T04:09:21Z");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/perp/orders-by-wallet"
        );
        assert_eq!(manifest["request"]["query"]["pageSize"], 5);
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"],
        );
        assert_eq!(response.data.len(), 1);
        assert_eq!(response.data[0].subaccounts.len(), 2);
        assert_eq!(
            response.data[0].subaccounts[0].orders.items.len()
                + response.data[0].subaccounts[1].orders.items.len(),
            5
        );
        let order = &response.data[0].subaccounts[0].orders.items[0];
        assert_eq!(order.order_id, "1789522185370");
        assert_eq!(
            order.avg_fill_price,
            Some("2403.9885999999997".parse::<Decimal>().unwrap())
        );
        assert_eq!(order.fee, Decimal::new(-144_111, 6));
        assert_eq!(
            response.data[0].subaccounts[0].orders.next_cursor,
            response.data[0].subaccounts[1].orders.next_cursor,
        );
    }

    #[rstest]
    fn decodes_sanitized_perp_volume_fixture() {
        let manifest: PerpVolumeFixtureManifest =
            serde_json::from_str(PERP_VOLUME_1H_MANIFEST).unwrap();
        let runtime_manifest: RuntimeFixtureManifest =
            serde_json::from_str(RUNTIME_MANIFEST).unwrap();
        let response: DeepXApiResponse<DeepXPerpVolume> =
            serde_json::from_str(PERP_VOLUME_1H_RESPONSE).unwrap();

        validate_perp_volume_fixture_manifest(&manifest, &runtime_manifest).unwrap();
        assert_eq!(response.code, DeepXResponseCode::Api(200));
        assert!(!response.fail);
        assert_eq!(response.data.total_volume, Decimal::new(2_117_975, 3));
        assert_eq!(response.data.trade_count, 2_492);
        assert_eq!(response.data.end_time - response.data.start_time, 3_600_000);
        assert_eq!(response.data.statistic_time, response.data.end_time);
    }

    #[rstest]
    fn decodes_lending_fixtures_exactly() {
        for (payload, manifest, path) in [
            (
                LENDING_ASSETS_RESPONSE,
                LENDING_ASSETS_MANIFEST,
                "/internal/v1/market/lending/assets",
            ),
            (
                LENDING_INTEREST_RATE_RESPONSE,
                LENDING_INTEREST_RATE_MANIFEST,
                "/internal/v1/market/lending/interest-rate",
            ),
            (
                LENDING_INTEREST_RATE_HISTORY_RESPONSE,
                LENDING_INTEREST_RATE_HISTORY_MANIFEST,
                "/internal/v1/market/lending/interest-rate-history",
            ),
            (
                LENDING_STATUS_HISTORY_RESPONSE,
                LENDING_STATUS_HISTORY_MANIFEST,
                "/internal/v1/market/lending/status-history",
            ),
        ] {
            let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
            let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, payload.as_bytes());
            assert_eq!(manifest["deployment"], "testnet");
            assert_eq!(manifest["endpoint_role"], "public_rest_market_data");
            assert_eq!(manifest["request"]["method"], "GET");
            assert_eq!(manifest["request"]["path"], path);
            assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
            assert_eq!(
                nautilus_core::hex::encode(digest.as_ref()),
                manifest["response"]["payload_sha256"],
            );
        }

        let assets: DeepXApiResponse<Vec<DeepXLendingAsset>> =
            serde_json::from_str(LENDING_ASSETS_RESPONSE).unwrap();
        assert_eq!(assets.data.len(), 3);
        assert_eq!(assets.data[2].asset, "usdc");
        assert_eq!(assets.data[2].height, 110_143);
        assert_eq!(assets.data[2].created_at, "2026-04-10T10:09:44.628Z");

        let curves: DeepXApiResponse<Vec<DeepXLendingInterestRateParams>> =
            serde_json::from_str(LENDING_INTEREST_RATE_RESPONSE).unwrap();
        let curve = &curves.data[0];
        assert_eq!(curve.u1, Decimal::new(8, 1));
        assert_eq!(curve.r3, Decimal::new(7, 1));
        assert_eq!(curve.u4, None);
        assert_eq!(curve.r5, None);
        assert_eq!(curve.r_max, Decimal::new(15, 1));
        assert_eq!(curve.rho, Decimal::new(1, 1));

        let rates: DeepXApiResponse<DeepXLendingInterestRateHistory> =
            serde_json::from_str(LENDING_INTEREST_RATE_HISTORY_RESPONSE).unwrap();
        assert_eq!(rates.data.details.len(), 3);
        assert_eq!(
            rates.data.details[0].supply_apr,
            "0.00002115040506874738".parse::<Decimal>().unwrap(),
        );
        assert_eq!(
            rates.data.details[0].borrow_apr,
            "0.001327604517817316".parse::<Decimal>().unwrap(),
        );

        let statuses: DeepXApiResponse<DeepXLendingStatusHistory> =
            serde_json::from_str(LENDING_STATUS_HISTORY_RESPONSE).unwrap();
        assert_eq!(statuses.data.details.len(), 3);
        assert_eq!(statuses.data.details[0].index_price, Decimal::ONE);
        assert_eq!(
            statuses.data.details[0].total_supplied,
            "830055808.591571".parse::<Decimal>().unwrap(),
        );
        assert_eq!(
            statuses.data.details[0].utilization_rate,
            "0.017701393570897547".parse::<Decimal>().unwrap(),
        );
    }

    #[rstest]
    fn rejects_perp_volume_fixture_runtime_identity_drift() {
        let manifest: PerpVolumeFixtureManifest =
            serde_json::from_str(PERP_VOLUME_1H_MANIFEST).unwrap();
        let mut runtime_manifest: RuntimeFixtureManifest =
            serde_json::from_str(RUNTIME_MANIFEST).unwrap();
        runtime_manifest.identity.spec_version += 1;

        assert_eq!(
            validate_perp_volume_fixture_manifest(&manifest, &runtime_manifest),
            Err("DeepX perpetual volume fixture runtime identity mismatch".to_string()),
        );
    }

    #[rstest]
    fn decodes_nonempty_account_record_fixtures_exactly() {
        for (payload, manifest) in [
            (
                PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE,
                PERP_HISTORY_ORDERS_ACCOUNT_MANIFEST,
            ),
            (
                PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE,
                PERP_ACCOUNT_TRADES_ACCOUNT_MANIFEST,
            ),
            (
                PERP_FUNDING_FEES_ACCOUNT_RESPONSE,
                PERP_FUNDING_FEES_ACCOUNT_MANIFEST,
            ),
            (
                PERP_POSITIONS_ACCOUNT_RESPONSE,
                PERP_POSITIONS_ACCOUNT_MANIFEST,
            ),
        ] {
            let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
            let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, payload.as_bytes());
            assert_eq!(manifest["deployment"], "testnet");
            assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
            assert_eq!(manifest["request"]["method"], "GET");
            assert_eq!(manifest["request"]["query"]["marketId"], 3);
            assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
            assert_eq!(
                nautilus_core::hex::encode(digest.as_ref()),
                manifest["response"]["payload_sha256"].as_str().unwrap()
            );
        }

        let orders: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
            serde_json::from_str(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE).unwrap();
        let trades: DeepXApiResponse<DeepXAccountPage<DeepXPerpAccountTradeRecord>> =
            serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        let funding_fees: DeepXApiResponse<DeepXAccountPage<DeepXPerpFundingFeeRecord>> =
            serde_json::from_str(PERP_FUNDING_FEES_ACCOUNT_RESPONSE).unwrap();
        let positions: DeepXApiResponse<DeepXAccountPage<DeepXPerpPositionRecord>> =
            serde_json::from_str(PERP_POSITIONS_ACCOUNT_RESPONSE).unwrap();

        assert!(!orders.fail);
        assert_eq!(orders.data.items.len(), 3);
        assert_eq!(orders.data.items[0].order_id, "1789445193480");
        assert_eq!(
            orders.data.items[0].avg_fill_price,
            Some(Decimal::new(24_996, 1))
        );
        assert_eq!(orders.data.items[0].fee, Decimal::new(-149_921, 6));
        assert_eq!(orders.data.items[1].status, "Filled");
        assert!(orders.data.items[1].reduce_only);

        assert!(!trades.fail);
        assert_eq!(trades.data.items.len(), 3);
        assert_eq!(trades.data.items[0].id, 185_971_383_000_008);
        assert_eq!(trades.data.items[0].order_id, orders.data.items[0].order_id);
        assert_eq!(trades.data.items[0].taker, "Buyer");
        assert_eq!(trades.data.items[0].filled_direction, "Long");
        assert_eq!(trades.data.items[0].price, Decimal::new(24_996, 1));
        assert_eq!(trades.data.items[0].fee, Decimal::new(-149_921, 6));

        assert!(!funding_fees.fail);
        assert_eq!(funding_fees.data.items.len(), 4);
        assert_eq!(funding_fees.data.items[0].market, 3);
        assert_eq!(
            funding_fees.data.items[0].position_size,
            Decimal::new(238, 3)
        );
        assert_eq!(funding_fees.data.items[0].fee, Decimal::new(-17, 5));
        assert_eq!(
            funding_fees.data.items[0].fee_rate,
            "0.000489097868208476".parse::<Decimal>().unwrap()
        );
        assert!(funding_fees.data.items[0].is_settled);

        assert!(!positions.fail);
        assert_eq!(positions.data.items.len(), 2);
        assert_eq!(positions.data.items[0].status, "Open");
        assert_eq!(
            positions.data.items[0].base_asset_amount,
            Decimal::new(3, 1)
        );
        assert_eq!(
            positions.data.items[0].pnl,
            "-0.4018472999999176".parse::<Decimal>().unwrap()
        );
        assert_eq!(positions.data.items[0].close_price, None);
        assert_eq!(positions.data.items[1].status, "Closed");
        assert_eq!(
            positions.data.items[1].close_price,
            Some(Decimal::new(249_941, 2))
        );
        assert_eq!(positions.data.items[1].version, 1);
    }

    #[rstest]
    fn decodes_spot_history_order_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(SPOT_HISTORY_ORDERS_ETH_USDC_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE.as_bytes(),
        );
        let orders: DeepXApiResponse<DeepXAccountPage<DeepXSpotOrderRecord>> =
            serde_json::from_str(SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE).unwrap();

        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/spot/history-orders"
        );
        assert_eq!(manifest["request"]["query"]["name"], "ETH/USDC");
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"]
        );
        assert_eq!(orders.data.items.len(), 3);
        assert!(orders.data.has_next);
        assert_eq!(orders.data.items[0].order_id, "1789627379350");
        assert_eq!(orders.data.items[0].price, Decimal::new(245_605, 2));
        assert_eq!(orders.data.items[0].avg_fill_price, None);
        assert_eq!(orders.data.items[0].base_amount, Decimal::new(8_849, 4));
        assert_eq!(
            orders.data.items[0].quote_amount,
            Decimal::new(2_173_358_645, 6)
        );
        assert_eq!(orders.data.items[0].fee, Decimal::ZERO);
    }

    #[rstest]
    fn decodes_spot_account_trade_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(SPOT_ACCOUNT_TRADES_ETH_USDC_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE.as_bytes(),
        );
        let trades: DeepXApiResponse<DeepXAccountPage<DeepXSpotAccountTradeRecord>> =
            serde_json::from_str(SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE).unwrap();

        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/spot/trades"
        );
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"]
        );
        assert_eq!(trades.data.items.len(), 3);
        assert!(trades.data.has_next);
        assert_eq!(trades.data.items[0].id, 188_573_374_000_099);
        assert_eq!(trades.data.items[0].order_id, "1789627349907");
        assert_eq!(
            trades.data.items[0].base_amount,
            "0.33079181307615474".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            trades.data.items[0].quote_amount,
            Decimal::new(808_041_701, 6)
        );
        assert_eq!(trades.data.items[0].fee, Decimal::new(-323_216, 6));
        assert_eq!(trades.data.items[0].fee_asset, "");

        let mut without_fee_asset: serde_json::Value =
            serde_json::from_str(SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE).unwrap();
        without_fee_asset["data"]["items"][0]
            .as_object_mut()
            .unwrap()
            .remove("feeAsset");
        let without_fee_asset: DeepXApiResponse<DeepXAccountPage<DeepXSpotAccountTradeRecord>> =
            serde_json::from_value(without_fee_asset).unwrap();
        assert_eq!(without_fee_asset.data.items[0].fee_asset, "");
    }

    #[rstest]
    fn decodes_spot_wallet_order_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(SPOT_WALLET_ORDERS_ETH_USDC_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_WALLET_ORDERS_ETH_USDC_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<Vec<DeepXSpotWalletOrderMarket>> =
            serde_json::from_str(SPOT_WALLET_ORDERS_ETH_USDC_RESPONSE).unwrap();

        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/spot/orders-by-wallet"
        );
        assert_eq!(manifest["request"]["query"]["pageSize"], 5);
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"]
        );
        assert_eq!(response.data.len(), 1);
        assert_eq!(response.data[0].name, "ETH/USDC");
        assert_eq!(response.data[0].subaccounts[0].orders.items.len(), 5);
        let order = &response.data[0].subaccounts[0].orders.items[1];
        assert_eq!(order.order_id, "1789631508484");
        assert_eq!(order.price, Decimal::new(245_139, 2));
        assert_eq!(order.avg_fill_price, None);
        assert_eq!(order.base_remaining_amount, Decimal::new(7_926, 4));
        assert_eq!(order.quote_amount, Decimal::new(1_942_971_714, 6));
        assert_eq!(order.fee, Decimal::ZERO);
        assert!(response.data[0].subaccounts[0].orders.has_next);
    }

    #[rstest]
    fn decodes_spot_wallet_trade_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(SPOT_WALLET_TRADES_ETH_USDC_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_WALLET_TRADES_ETH_USDC_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<Vec<DeepXSpotWalletTradeMarket>> =
            serde_json::from_str(SPOT_WALLET_TRADES_ETH_USDC_RESPONSE).unwrap();

        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/spot/trades-by-wallet"
        );
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"]
        );
        assert_eq!(response.data.len(), 1);
        assert_eq!(response.data[0].subaccounts[0].trades.items.len(), 5);
        let trade = &response.data[0].subaccounts[0].trades.items[0];
        assert_eq!(trade.id, 188_632_828_000_015);
        assert_eq!(
            trade.base_amount,
            "0.26400108068721884".parse::<Decimal>().unwrap()
        );
        assert_eq!(trade.quote_amount, Decimal::new(6_449_256, 4));
        assert_eq!(trade.fee, Decimal::new(-64_492, 6));
        assert_eq!(trade.fee_asset, "");
        assert!(response.data[0].subaccounts[0].trades.has_next);
    }

    #[rstest]
    fn decodes_canceled_spot_order_lookup_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(SPOT_ORDER_BY_ID_CANCELED_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            SPOT_ORDER_BY_ID_CANCELED_RESPONSE.as_bytes(),
        );
        let order: DeepXApiResponse<DeepXSpotOrderRecord> =
            serde_json::from_str(SPOT_ORDER_BY_ID_CANCELED_RESPONSE).unwrap();

        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/spot/order-by-id"
        );
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"]
        );
        assert_eq!(order.data.order_id, "1789627379350");
        assert_eq!(order.data.status, "Canceled");
        assert_eq!(order.data.cancel_reason.as_deref(), Some("UserCanceled"));
        assert_eq!(order.data.cancel_height, Some(188_574_100));
        assert_eq!(order.data.price, Decimal::new(245_605, 2));
        assert_eq!(order.data.avg_fill_price, None);
    }

    #[rstest]
    fn decodes_perp_order_by_tx_fixture_exactly() {
        let manifest: serde_json::Value = serde_json::from_str(PERP_ORDER_BY_TX_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            PERP_ORDER_BY_TX_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<DeepXPerpOrderRecord> =
            serde_json::from_str(PERP_ORDER_BY_TX_RESPONSE).unwrap();

        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["captured_at"], "2026-09-17T02:29:39Z");
        assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
        assert_eq!(manifest["request"]["method"], "GET");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/perp/order-by-tx"
        );
        assert_eq!(
            manifest["request"]["query"]["txHash"],
            "0x247ef3967338c6985714731a49752455ebe877d8678e0b261b3638892cfae554"
        );
        assert_eq!(manifest["response"]["status"], 200);
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"],
        );
        assert!(!response.fail);
        assert_eq!(response.data.order_id, "1789445193480");
        assert_eq!(response.data.market_id, 3);
        assert_eq!(response.data.size, Decimal::new(3, 1));
        assert_eq!(response.data.price, Decimal::new(2_523_683_573, 6));
        assert_eq!(response.data.avg_fill_price, Some(Decimal::new(24_996, 1)));
        assert_eq!(response.data.fee, Decimal::new(-149_921, 6));
        assert_eq!(
            response.data.tx_hash,
            "0x247ef3967338c6985714731a49752455ebe877d8678e0b261b3638892cfae554"
        );
        assert_eq!(response.data.tx_hash_type, "EXTRINSIC_HASH");
    }

    #[rstest]
    fn decodes_wallet_funding_fee_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(WALLET_FUNDING_FEES_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            WALLET_FUNDING_FEES_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpFundingFeeRecord>> =
            serde_json::from_str(WALLET_FUNDING_FEES_RESPONSE).unwrap();

        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["request"]["method"], "GET");
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"].as_str().unwrap()
        );
        assert_eq!(response.data.items.len(), 2);
        assert_eq!(response.data.items[0].position_size, Decimal::new(238, 3));
        assert_eq!(response.data.items[0].fee, Decimal::new(-17, 5));
        assert_eq!(
            response.data.items[1].fee_rate,
            "0.000469620169921581".parse::<Decimal>().unwrap()
        );
        assert!(response.data.has_next);
        assert!(response.data.next_cursor.is_some());
    }

    #[rstest]
    fn decodes_hourly_unsettled_funding_fixtures_exactly() {
        for (payload, manifest) in [
            (
                WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE,
                WALLET_HOURLY_FUNDING_PAGE_1_MANIFEST,
            ),
            (
                WALLET_HOURLY_FUNDING_PAGE_2_RESPONSE,
                WALLET_HOURLY_FUNDING_PAGE_2_MANIFEST,
            ),
        ] {
            let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
            let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, payload.as_bytes());
            assert_eq!(manifest["deployment"], "testnet");
            assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
            assert_eq!(
                manifest["request"]["path"],
                "/internal/v1/account/hourly-unsettled-funding"
            );
            assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
            assert_eq!(
                nautilus_core::hex::encode(digest.as_ref()),
                manifest["response"]["payload_sha256"],
            );
        }

        let first: DeepXApiResponse<Vec<DeepXHourlyUnsettledFundingRecord>> =
            serde_json::from_str(WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE).unwrap();
        let second: DeepXApiResponse<Vec<DeepXHourlyUnsettledFundingRecord>> =
            serde_json::from_str(WALLET_HOURLY_FUNDING_PAGE_2_RESPONSE).unwrap();
        let newest = &first.data[0];
        let first_boundary = first.data.last().unwrap();
        let second_boundary = &second.data[0];

        assert_eq!(first.data.len(), 5);
        assert_eq!(second.data.len(), 5);
        assert_eq!(newest.signed_position_size_raw, 300_000_000_000_000_000);
        assert_eq!(newest.baseline_index_raw, 77_064_622_395_991_726);
        assert_eq!(newest.cumulative_index_raw, 77_077_122_395_991_726);
        assert_eq!(newest.delta_index_raw, 12_500_000_000_000);
        assert_eq!(newest.mark_price_raw, 2_397_824_333);
        assert_eq!(newest.payment_raw, -8_991);
        assert_eq!(newest.boundary_timestamp_ms, 1_789_521_262_677);
        assert_eq!(newest.boundary_block, 187_058_004);
        assert_eq!(newest.boundary_event_index, 14);
        assert_eq!(first_boundary.baseline_index_raw, 76_843_886_126_453_109);
        assert_eq!(second_boundary.cumulative_index_raw, 76_843_886_126_453_109);
        assert!(first_boundary.boundary_timestamp_ms > second_boundary.boundary_timestamp_ms);
    }

    #[rstest]
    fn decodes_user_stats_and_liquidation_price_fixtures_exactly() {
        for (payload, manifest) in [
            (WALLET_USER_STATS_RESPONSE, WALLET_USER_STATS_MANIFEST),
            (
                PERP_LIQUIDATION_PRICE_RESPONSE,
                PERP_LIQUIDATION_PRICE_MANIFEST,
            ),
        ] {
            let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
            let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, payload.as_bytes());
            assert_eq!(manifest["deployment"], "testnet");
            assert_eq!(manifest["request"]["method"], "GET");
            assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
            assert_eq!(
                nautilus_core::hex::encode(digest.as_ref()),
                manifest["response"]["payload_sha256"].as_str().unwrap()
            );
        }

        let stats: DeepXApiResponse<DeepXUserStats> =
            serde_json::from_str(WALLET_USER_STATS_RESPONSE).unwrap();
        let price: DeepXApiResponse<DeepXPerpLiquidationPrice> =
            serde_json::from_str(PERP_LIQUIDATION_PRICE_RESPONSE).unwrap();

        assert_eq!(stats.data.subaccounts.len(), 4);
        assert_eq!(stats.data.if_staked_quote_asset_amount, Decimal::ZERO);
        assert_eq!(stats.data.number_of_sub_accounts, 4);
        assert_eq!(stats.data.number_of_sub_accounts_created, 4);
        assert_eq!(price.data.market_id, 3);
        assert_eq!(price.data.market_name, "ETH-USDC");
        assert_eq!(price.data.liquidate_price, None);
    }

    #[rstest]
    fn decodes_all_subaccounts_fixture_exactly() {
        let manifest: serde_json::Value = serde_json::from_str(ALL_SUBACCOUNTS_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            ALL_SUBACCOUNTS_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<DeepXAccountPage<DeepXSubaccountDirectoryRecord>> =
            serde_json::from_str(ALL_SUBACCOUNTS_RESPONSE).unwrap();

        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/subaccounts/all"
        );
        assert_eq!(manifest["request"]["query"]["pageSize"], 5);
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"]
        );
        assert_eq!(response.data.items.len(), 5);
        assert!(response.data.has_next);
        let record = &response.data.items[0];
        assert_eq!(record.owner, "0x0a40c3efbc3b3bebdf1fb6f0f8c612eb336b25ae");
        assert_eq!(
            record.subaccount,
            "0x260949c96c0d32a8126ab666a14131321e08f563"
        );
        assert_eq!(record.name, "Subaccount11");
        assert_eq!(record.status, None);
        assert_eq!(record.height, 181_249_057);
        assert_eq!(record.created_at, "2026-09-11T08:16:49.885Z");
    }

    #[rstest]
    fn decodes_quota_history_fixture_exactly() {
        let manifest: serde_json::Value = serde_json::from_str(QUOTA_HISTORY_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            QUOTA_HISTORY_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<DeepXAccountPage<DeepXQuotaHistoryRecord>> =
            serde_json::from_str(QUOTA_HISTORY_RESPONSE).unwrap();
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"]
        );
        let record = &response.data.items[0];
        assert_eq!(
            record.history_type,
            super::super::query::DeepXQuotaHistoryType::Purchase
        );
        assert_eq!(
            record.buyer_type,
            Some(super::super::query::DeepXQuotaBuyerType::Wallet)
        );
        assert_eq!(record.quota, 10_000);
        assert_eq!(record.block_number, 181_248_894);
        assert_eq!(record.event_index, 49);
    }

    #[rstest]
    fn decodes_wallet_quota_summary_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(WALLET_QUOTA_SUMMARY_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            WALLET_QUOTA_SUMMARY_RESPONSE.as_bytes(),
        );
        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
        assert_eq!(manifest["request"]["method"], "GET");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/quota/summary"
        );
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"],
        );

        let response: DeepXApiResponse<DeepXQuotaSummary> =
            serde_json::from_str(WALLET_QUOTA_SUMMARY_RESPONSE).unwrap();
        assert_eq!(
            response.data.owner,
            "0x781ed35b167068c93dfadab41dfb680edaca4e50"
        );
        assert_eq!(response.data.subaccount_count, 4);
        assert_eq!(response.data.spot_volume_usd, Decimal::ZERO);
        assert_eq!(
            response.data.perp_volume_usd,
            "2970.527580000000000000".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            response.data.total_volume_usd,
            response.data.perp_volume_usd
        );
        assert_eq!(response.data.quota_earned, 2_970);
        assert_eq!(response.data.quota_pending, 2_970);
        assert_eq!(response.data.first_trade_ts_ms, Some(1_789_445_055_647));
        assert_eq!(response.data.last_trade_ts_ms, Some(1_789_522_187_243));
    }

    #[rstest]
    fn decodes_account_identity_and_state_fixtures_exactly() {
        for (payload, manifest) in [
            (WALLET_SUBACCOUNTS_RESPONSE, WALLET_SUBACCOUNTS_MANIFEST),
            (SUBACCOUNT_INFO_RESPONSE, SUBACCOUNT_INFO_MANIFEST),
            (SUBACCOUNT_BALANCES_RESPONSE, SUBACCOUNT_BALANCES_MANIFEST),
            (SUBACCOUNT_EQUITY_RESPONSE, SUBACCOUNT_EQUITY_MANIFEST),
            (
                SUBACCOUNT_MARGIN_RATIO_RESPONSE,
                SUBACCOUNT_MARGIN_RATIO_MANIFEST,
            ),
            (
                WALLET_BALANCE_CHANGES_RESPONSE,
                WALLET_BALANCE_CHANGES_MANIFEST,
            ),
        ] {
            let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
            let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, payload.as_bytes());
            assert_eq!(manifest["deployment"], "testnet");
            assert_eq!(manifest["request"]["method"], "GET");
            assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
            assert_eq!(
                nautilus_core::hex::encode(digest.as_ref()),
                manifest["response"]["payload_sha256"].as_str().unwrap()
            );
        }

        let subaccounts: DeepXApiResponse<Vec<String>> =
            serde_json::from_str(WALLET_SUBACCOUNTS_RESPONSE).unwrap();
        let profile: DeepXApiResponse<DeepXSubaccountProfile> =
            serde_json::from_str(SUBACCOUNT_INFO_RESPONSE).unwrap();
        let balances: DeepXApiResponse<DeepXSubaccountBalances> =
            serde_json::from_str(SUBACCOUNT_BALANCES_RESPONSE).unwrap();
        let equity: DeepXApiResponse<DeepXSubaccountEquity> =
            serde_json::from_str(SUBACCOUNT_EQUITY_RESPONSE).unwrap();
        let margin: DeepXApiResponse<DeepXSubaccountMarginRatio> =
            serde_json::from_str(SUBACCOUNT_MARGIN_RATIO_RESPONSE).unwrap();
        let changes: DeepXApiResponse<DeepXAccountPage<DeepXBalanceChangeRecord>> =
            serde_json::from_str(WALLET_BALANCE_CHANGES_RESPONSE).unwrap();

        assert_eq!(subaccounts.data.len(), 4);
        assert_eq!(profile.data.name, "kazee");
        assert_eq!(profile.data.status, "Active");
        assert_eq!(profile.data.margin_strategy, "Cross");
        assert!(profile.data.spot_positions.is_empty());
        assert_eq!(balances.data.assets.len(), 3);
        assert_eq!(balances.data.assets[0].symbol, "USDC");
        assert_eq!(
            balances.data.assets[0].balance,
            Decimal::new(999_783_258, 6)
        );
        assert_eq!(
            balances.data.assets[1].price,
            Decimal::new(2_490_275_945, 6)
        );
        assert_eq!(equity.data.total_deposits_usd, Decimal::new(99_978, 2));
        assert_eq!(equity.data.unrealized_pnl_usd, Decimal::new(-273, 2));
        assert_eq!(equity.data.equity_usd, Decimal::new(99_705, 2));
        assert_eq!(margin.data.collateral, Decimal::new(97_096, 2));
        assert_eq!(margin.data.margin_required, Decimal::ZERO);
        assert_eq!(margin.data.margin_ratio, None);
        assert_eq!(changes.data.items.len(), 2);
        assert_eq!(changes.data.items[0].balance_change, Decimal::new(-17, 5));
        assert_eq!(
            changes.data.items[0].change_type,
            super::super::query::DeepXBalanceChangeType::FundingFee
        );
        assert_eq!(
            changes.data.items[0].position.as_ref().unwrap().market_id,
            3
        );
        assert_eq!(
            changes.data.items[1].balance_change,
            Decimal::new(-10_235_558, 6)
        );
        assert_eq!(
            changes.data.next_cursor.as_deref(),
            Some("MTc4OTQ5ODIxMTExNToxODY3Mjg3MTcwMDAyMDAw")
        );
        assert!(changes.data.has_next);
    }

    #[rstest]
    fn decodes_delegate_directory_fixtures_exactly() {
        for (payload, manifest, path) in [
            (
                WALLET_DELEGATE_ACCOUNTS_RESPONSE,
                WALLET_DELEGATE_ACCOUNTS_MANIFEST,
                "/internal/v1/account/delegate-accounts",
            ),
            (
                DELEGATE_DELEGATOR_ACCOUNTS_RESPONSE,
                DELEGATE_DELEGATOR_ACCOUNTS_MANIFEST,
                "/internal/v1/account/delegator-accounts",
            ),
        ] {
            let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
            let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, payload.as_bytes());
            assert_eq!(manifest["captured_at"], "2026-09-17T02:43:04Z");
            assert_eq!(manifest["deployment"], "testnet");
            assert_eq!(manifest["endpoint_role"], "public_rest_account_identity");
            assert_eq!(manifest["request"]["method"], "GET");
            assert_eq!(manifest["request"]["path"], path);
            assert_eq!(manifest["response"]["status"], 200);
            assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
            assert_eq!(
                nautilus_core::hex::encode(digest.as_ref()),
                manifest["response"]["payload_sha256"],
            );
        }

        let delegates: DeepXApiResponse<Vec<DeepXDelegateAccount>> =
            serde_json::from_str(WALLET_DELEGATE_ACCOUNTS_RESPONSE).unwrap();
        let delegators: DeepXApiResponse<Vec<String>> =
            serde_json::from_str(DELEGATE_DELEGATOR_ACCOUNTS_RESPONSE).unwrap();
        assert_eq!(delegates.data.len(), 1);
        assert_eq!(
            delegates.data[0].delegate_address,
            "0x1b856b9bf1d0ceeb0927a081c759813cd703a54f"
        );
        assert_eq!(delegates.data[0].delegate_name, "One-Click Trading");
        assert_eq!(delegates.data[0].valid_until, 1_804_997_046_158);
        assert_eq!(delegates.data[0].create_time, 1_789_445_050_397);
        assert_eq!(
            delegates.data[0].mode,
            DeepXDelegateMode::PlaceOrCancelOrder
        );
        assert!(delegates.data[0].active);
        assert_eq!(
            delegators.data,
            ["0x781ed35b167068c93dfadab41dfb680edaca4e50"]
        );
    }

    #[rstest]
    fn decodes_wallet_liquidation_record_fixture_exactly() {
        let manifest: serde_json::Value =
            serde_json::from_str(WALLET_LIQUIDATION_RECORDS_MANIFEST).unwrap();
        let digest = aws_lc_rs::digest::digest(
            &aws_lc_rs::digest::SHA256,
            WALLET_LIQUIDATION_RECORDS_RESPONSE.as_bytes(),
        );
        let response: DeepXApiResponse<DeepXAccountPage<DeepXLiquidationRecord>> =
            serde_json::from_str(WALLET_LIQUIDATION_RECORDS_RESPONSE).unwrap();

        assert_eq!(manifest["captured_at"], "2026-09-17T03:01:34Z");
        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/subaccounts/liquidation-records"
        );
        assert_eq!(manifest["request"]["query"]["pageSize"], 5);
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"],
        );
        assert!(response.data.has_next);
        assert!(response.data.next_cursor.is_some());
        assert_eq!(response.data.items.len(), 5);
        assert_eq!(response.data.items[0].id, 1_763);
        assert_eq!(
            response.data.items[0].liquidation_type,
            crate::http::query::DeepXLiquidationType::LiquidatePerp
        );
        assert_eq!(response.data.items[0].margin_shortage, 2_771);
        assert_eq!(response.data.items[0].margin_freed, 16_917);
        assert_eq!(response.data.items[0].liquidator_fee, Some(16_911));
        assert_eq!(
            response.data.items[0].liquidate_base_amount,
            Some(20_000_000)
        );
        assert_eq!(response.data.items[0].oracle_price, Some(84_566_331));
        assert_eq!(response.data.items[0].target_account_order_id, Some(14));
        assert_eq!(response.data.items[0].tx_hash, None);
    }

    #[rstest]
    fn liquidation_record_exact_integer_decoder_preserves_u128_boundaries() {
        let response: serde_json::Value =
            serde_json::from_str(WALLET_LIQUIDATION_RECORDS_RESPONSE).unwrap();
        let mut record = response["data"]["items"][0].clone();
        record["marginShortage"] = serde_json::json!(u128::MAX.to_string());
        let decoded: DeepXLiquidationRecord = serde_json::from_value(record.clone()).unwrap();
        assert_eq!(decoded.margin_shortage, u128::MAX);

        record["marginShortage"] = serde_json::json!("340282366920938463463374607431768211456");
        assert!(serde_json::from_value::<DeepXLiquidationRecord>(record).is_err());
    }
}
