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

//! Typed query parameters for verified DeepX public endpoints.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{DeepXHttpError, Result};

/// Sort order accepted by account history endpoints.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub enum DeepXAccountSortOrder {
    /// Oldest records first.
    #[serde(rename = "ASC")]
    Ascending,
    /// Newest records first.
    #[default]
    #[serde(rename = "DESC")]
    Descending,
}

/// Closed Spot order-side filters documented by DeepX.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum DeepXSpotOrderSide {
    /// Buy-side orders.
    Buy,
    /// Sell-side orders.
    Sell,
}

impl DeepXSpotOrderSide {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "Buy",
            Self::Sell => "Sell",
        }
    }
}

/// Closed balance-change filter values documented by DeepX.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeepXBalanceChangeType {
    Deposit,
    Withdraw,
    FundingFee,
    LiquidationFee,
    Transfer,
    Borrow,
    Repay,
    Settlement,
    Liquidation,
    Funding,
    Reconcile,
}

/// Closed liquidation-record classifications documented by DeepX and defined by the chain.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub enum DeepXLiquidationType {
    /// Perpetual position liquidation.
    LiquidatePerp,
    /// Spot lending position liquidation.
    LiquidateSpot,
    /// Perpetual bankruptcy handling.
    PerpBankruptcy,
    /// Spot bankruptcy handling.
    SpotBankruptcy,
}

/// Closed quota-history classifications documented by DeepX.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeepXQuotaHistoryType {
    /// Purchased quota.
    Purchase,
    /// Activated account quota.
    Activate,
    /// Quota granted without purchase.
    Free,
}

/// Closed quota-purchase payer classifications documented by DeepX.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeepXQuotaBuyerType {
    /// Wallet-funded purchase.
    Wallet,
    /// Subaccount-funded purchase.
    Subaccount,
}

impl DeepXLiquidationType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LiquidatePerp => "LiquidatePerp",
            Self::LiquidateSpot => "LiquidateSpot",
            Self::PerpBankruptcy => "PerpBankruptcy",
            Self::SpotBankruptcy => "SpotBankruptcy",
        }
    }

    pub(crate) const fn detail_key(self) -> &'static str {
        match self {
            Self::LiquidatePerp => "liquidatePerp",
            Self::LiquidateSpot => "liquidateSpot",
            Self::PerpBankruptcy => "perpBankruptcy",
            Self::SpotBankruptcy => "spotBankruptcy",
        }
    }

    pub(crate) const fn is_bankruptcy(self) -> bool {
        matches!(self, Self::PerpBankruptcy | Self::SpotBankruptcy)
    }
}

impl DeepXBalanceChangeType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Deposit => "DEPOSIT",
            Self::Withdraw => "WITHDRAW",
            Self::FundingFee => "FUNDING_FEE",
            Self::LiquidationFee => "LIQUIDATION_FEE",
            Self::Transfer => "TRANSFER",
            Self::Borrow => "BORROW",
            Self::Repay => "REPAY",
            Self::Settlement => "SETTLEMENT",
            Self::Liquidation => "LIQUIDATION",
            Self::Funding => "FUNDING",
            Self::Reconcile => "RECONCILE",
        }
    }
}

/// Request for one page of balance changes scoped to one subaccount or wallet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXBalanceChangesRequest {
    /// Exact subaccount address, mutually exclusive with `wallet`.
    pub subaccount: Option<String>,
    /// Exact wallet address, mutually exclusive with `subaccount`.
    pub wallet: Option<String>,
    /// Optional lower timestamp bound in Unix milliseconds.
    pub start_ms: Option<u64>,
    /// Optional upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Optional closed set of change-type filters.
    pub change_types: Vec<DeepXBalanceChangeType>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Optional number of records requested from the venue.
    pub page_size: Option<u32>,
}

/// Complete keyset cursor for hourly unsettled-funding history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXHourlyUnsettledFundingCursor {
    /// Boundary timestamp of the last returned row.
    pub boundary_timestamp_ms: u64,
    /// Market ID of the last returned row.
    pub market_id: u64,
    /// Subaccount of the last returned row.
    pub subaccount: String,
    /// Boundary event ID of the last returned row.
    pub event_id: String,
}

/// Request for complete hourly unsettled-funding boundaries under one account scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXHourlyUnsettledFundingRequest {
    /// Exact subaccount address, mutually exclusive with `wallet`.
    pub subaccount: Option<String>,
    /// Exact wallet address, mutually exclusive with `subaccount`.
    pub wallet: Option<String>,
    /// Optional deployment-provided perpetual market ID.
    pub market_id: Option<u64>,
    /// Optional inclusive lower boundary timestamp in Unix milliseconds.
    pub start_ms: Option<u64>,
    /// Optional inclusive upper boundary timestamp in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Complete keyset cursor returned from the preceding page boundary.
    pub cursor: Option<DeepXHourlyUnsettledFundingCursor>,
    /// Optional number of records requested from the venue, from 1 through 100.
    pub page_size: Option<u32>,
    /// Requested venue response order.
    pub sort: DeepXAccountSortOrder,
}

/// Request for one page of liquidation records scoped to one subaccount or wallet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXLiquidationRecordsRequest {
    /// Exact subaccount address, mutually exclusive with `wallet`.
    pub subaccount: Option<String>,
    /// Exact wallet address, mutually exclusive with `subaccount`.
    pub wallet: Option<String>,
    /// Optional closed set of liquidation-type filters.
    pub liquidation_types: Vec<DeepXLiquidationType>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Requested venue response order.
    pub sort: DeepXAccountSortOrder,
    /// Optional number of records requested from the venue.
    pub page_size: Option<u32>,
}

/// Request for the current liquidation price of one subaccount and perpetual market.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpLiquidationPriceRequest {
    /// Exact 20-byte hex subaccount address.
    pub subaccount: String,
    /// Optional perpetual market name, mutually exclusive with `market_id`.
    pub market_name: Option<String>,
    /// Optional deployment-provided market ID, mutually exclusive with `market_name`.
    pub market_id: Option<u64>,
}

/// Request for one page of open perpetual orders owned by a subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpOpenOrdersRequest {
    /// Exact 20-byte hex subaccount address.
    pub subaccount: String,
    /// Deployment-provided perpetual market ID required by the verified market-ID request path.
    pub market_id: Option<u64>,
    /// Optional long-side filter.
    pub is_long: Option<bool>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Optional number of records requested from the venue.
    pub page_size: Option<u32>,
    /// Requested venue response order.
    pub sort: DeepXAccountSortOrder,
}

/// Request for one page of historical perpetual orders owned by a subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpHistoryOrdersRequest {
    /// Exact 20-byte hex subaccount address.
    pub subaccount: String,
    /// Optional deployment-provided perpetual market ID.
    pub market_id: Option<u64>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Optional number of records requested from the venue.
    pub page_size: Option<u32>,
    /// Requested venue response order.
    pub sort: DeepXAccountSortOrder,
}

/// Request for one page of active Spot orders owned by a subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotOpenOrdersRequest {
    /// Exact 20-byte hex subaccount address.
    pub subaccount: String,
    /// Optional market name, mutually exclusive with `pair`.
    pub name: Option<String>,
    /// Optional deployment-provided bytes32 pair, mutually exclusive with `name`.
    pub pair: Option<String>,
    /// Optional buy- or sell-side filter.
    pub order_side: Option<DeepXSpotOrderSide>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Requested venue response order.
    pub sort: DeepXAccountSortOrder,
    /// Optional number of records requested from the venue.
    pub page_size: Option<u32>,
}

/// Request for one exact Spot order lookup by venue order ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotOrderByIdRequest {
    /// Exact 20-byte hex subaccount address.
    pub subaccount: String,
    /// Market name, mutually exclusive with `pair`.
    pub name: Option<String>,
    /// Deployment-provided bytes32 pair, mutually exclusive with `name`.
    pub pair: Option<String>,
    /// Exact decimal venue order ID.
    pub order_id: String,
    /// Exact buy- or sell-side identity.
    pub order_side: DeepXSpotOrderSide,
}

/// Request for one page of historical Spot orders owned by a subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotHistoryOrdersRequest {
    /// Exact 20-byte hex subaccount address.
    pub subaccount: String,
    /// Optional market name, mutually exclusive with `pair`.
    pub name: Option<String>,
    /// Optional deployment-provided bytes32 pair, mutually exclusive with `name`.
    pub pair: Option<String>,
    /// Optional buy- or sell-side filter.
    pub order_side: Option<DeepXSpotOrderSide>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Requested venue response order.
    pub sort: DeepXAccountSortOrder,
    /// Optional number of records requested from the venue.
    pub page_size: Option<u32>,
}

/// Request for one page of Spot trades returned for a subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotAccountTradesRequest {
    /// Exact 20-byte hex subaccount address.
    pub subaccount: String,
    /// Optional exact decimal order ID, requiring a market selector and `order_side`.
    pub order_id: Option<String>,
    /// Optional buy- or sell-side filter, requiring `order_id` when present.
    pub order_side: Option<DeepXSpotOrderSide>,
    /// Optional market name, mutually exclusive with `pair`.
    pub name: Option<String>,
    /// Optional deployment-provided bytes32 pair, mutually exclusive with `name`.
    pub pair: Option<String>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Requested venue response order.
    pub sort: DeepXAccountSortOrder,
    /// Optional inclusive lower creation-time bound in Unix milliseconds.
    pub start_ms: Option<u64>,
    /// Optional inclusive upper creation-time bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Optional number of records requested from the venue.
    pub page_size: Option<u32>,
}

/// Request for one globally paginated Spot order page grouped by market and subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotWalletOrdersRequest {
    /// Optional exact wallet address; omission requests the venue's all-wallet view.
    pub wallet: Option<String>,
    /// Optional market name, mutually exclusive with `pair`.
    pub name: Option<String>,
    /// Optional deployment-provided bytes32 pair, mutually exclusive with `name`.
    pub pair: Option<String>,
    /// Optional buy- or sell-side filter.
    pub order_side: Option<DeepXSpotOrderSide>,
    /// Opaque global venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Requested venue response order within each subaccount group.
    pub sort: DeepXAccountSortOrder,
    /// Optional inclusive lower creation-time bound in Unix milliseconds.
    pub start_ms: Option<u64>,
    /// Optional inclusive upper creation-time bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Optional maximum total records returned across all groups.
    pub page_size: Option<u32>,
}

/// Request for one globally paginated Spot trade page grouped by market and subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotWalletTradesRequest {
    /// Exact 20-byte hex wallet address.
    pub wallet: String,
    /// Market name required when `pair` is absent.
    pub name: Option<String>,
    /// Deployment-provided bytes32 pair required when `name` is absent.
    pub pair: Option<String>,
    /// Opaque global venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Requested venue response order within each subaccount group.
    pub sort: DeepXAccountSortOrder,
    /// Optional inclusive lower creation-time bound in Unix milliseconds.
    pub start_ms: Option<u64>,
    /// Optional inclusive upper creation-time bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Optional maximum total records returned across all groups.
    pub page_size: Option<u32>,
}

/// Request for one page of the public global subaccount directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXAllSubaccountsRequest {
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Optional number of directory records requested from the venue.
    pub page_size: Option<u32>,
}

/// Request for one page of chain-confirmed wallet quota history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXQuotaHistoryRequest {
    /// Wallet that owns the requested quota history.
    pub wallet: String,
    /// Optional purchase payer address filter.
    pub buyer_address: Option<String>,
    /// Optional closed quota operation filter.
    pub history_type: Option<DeepXQuotaHistoryType>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Optional page size from 1 through 100.
    pub limit: Option<u32>,
}

/// Request for one page of perpetual trades owned by a subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpAccountTradesRequest {
    /// Exact 20-byte hex subaccount address.
    pub subaccount: String,
    /// Optional exact decimal venue order ID.
    pub order_id: Option<String>,
    /// Optional deployment-provided perpetual market ID.
    pub market_id: Option<u64>,
    /// Order side required only when filtering by order ID.
    pub is_long: Option<bool>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Requested venue response order.
    pub sort: DeepXAccountSortOrder,
    /// Optional lower timestamp bound in Unix milliseconds.
    pub start_ms: Option<u64>,
    /// Optional upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Optional number of records requested from the venue.
    pub page_size: Option<u32>,
}

/// Request for one globally paginated perpetual order page across a wallet's subaccounts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpWalletOrdersRequest {
    /// Exact 20-byte hex wallet address.
    pub wallet: String,
    /// Optional perpetual market name, mutually exclusive with `market_id`.
    pub market_name: Option<String>,
    /// Optional deployment-provided perpetual market ID, mutually exclusive with `market_name`.
    pub market_id: Option<u64>,
    /// Optional long-side filter.
    pub is_long: Option<bool>,
    /// Opaque global venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Requested venue response order within each subaccount group.
    pub sort: DeepXAccountSortOrder,
    /// Optional inclusive lower timestamp bound in Unix milliseconds.
    pub start_ms: Option<u64>,
    /// Optional inclusive upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Optional maximum total records returned across all groups.
    pub page_size: Option<u32>,
}

/// Request for one grouped perpetual trade snapshot across a wallet's subaccounts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpWalletTradesRequest {
    /// Exact 20-byte hex wallet address.
    pub wallet: String,
    /// Optional perpetual market name, mutually exclusive with `market_id`.
    pub market_name: Option<String>,
    /// Optional deployment-provided perpetual market ID, mutually exclusive with `market_name`.
    pub market_id: Option<u64>,
    /// Opaque venue cursor applied to every returned subaccount group.
    pub cursor: Option<String>,
    /// Requested venue response order within each subaccount group.
    pub sort: DeepXAccountSortOrder,
    /// Optional inclusive lower timestamp bound in Unix milliseconds.
    pub start_ms: Option<u64>,
    /// Optional inclusive upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Optional maximum records returned in each subaccount group.
    pub page_size: Option<u32>,
}

/// Request for one page of perpetual funding fees owned by a subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpFundingFeeRequest {
    /// Exact 20-byte hex subaccount address.
    pub subaccount: String,
    /// Optional deployment-provided perpetual market ID.
    pub market_id: Option<u64>,
    /// Optional lower timestamp bound in Unix milliseconds.
    pub start_ms: Option<u64>,
    /// Optional upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Optional number of records requested from the venue.
    pub page_size: Option<u32>,
}

/// Request for one globally ordered page of perpetual funding fees owned by a wallet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXWalletFundingFeeRequest {
    /// Exact 20-byte hex wallet address.
    pub wallet: String,
    /// Optional perpetual market name, mutually exclusive with `market_id`.
    pub market_name: Option<String>,
    /// Optional deployment-provided perpetual market ID, mutually exclusive with `market_name`.
    pub market_id: Option<u64>,
    /// Optional lower timestamp bound in Unix milliseconds.
    pub start_ms: Option<u64>,
    /// Optional upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Opaque global venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Optional number of records requested from the venue.
    pub page_size: Option<u32>,
}

/// Request for one page of perpetual position lifecycles owned by a subaccount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpPositionsRequest {
    /// Exact 20-byte hex subaccount address.
    pub subaccount: String,
    /// Optional deployment-provided perpetual market ID.
    pub market_id: Option<u64>,
    /// Whether only closed position lifecycles should be returned.
    pub only_closed: Option<bool>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Optional number of records requested from the venue.
    pub page_size: Option<u32>,
}

impl DeepXPerpOpenOrdersRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_account_page_request(
            "perp-open-orders",
            &self.subaccount,
            self.market_id,
            self.cursor.as_deref(),
            self.page_size,
        )?;
        if self.market_id.is_none() {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-open-orders market_id is required".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXPerpOpenOrdersQuery<'_> {
        DeepXPerpOpenOrdersQuery {
            user: &self.subaccount,
            market_id: self.market_id,
            is_long: self.is_long,
            cursor: self.cursor.as_deref(),
            sort: self.sort,
            page_size: self.page_size,
        }
    }
}

impl DeepXBalanceChangesRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        let address = match (&self.subaccount, &self.wallet) {
            (Some(subaccount), None) => subaccount,
            (None, Some(wallet)) => wallet,
            _ => {
                return Err(DeepXHttpError::InvalidRequest(
                    "balance-changes requires exactly one subaccount or wallet".to_string(),
                ));
            }
        };
        if address.len() != 42
            || !address.starts_with("0x")
            || !address.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
        {
            return Err(DeepXHttpError::InvalidRequest(
                "balance-changes requires a 20-byte hex account address".to_string(),
            ));
        }
        if self
            .start_ms
            .zip(self.end_ms)
            .is_some_and(|(start, end)| start > end)
        {
            return Err(DeepXHttpError::InvalidRequest(
                "balance-changes start_ms must not exceed end_ms".to_string(),
            ));
        }
        for bound in [self.start_ms, self.end_ms].into_iter().flatten() {
            if i64::try_from(bound)
                .ok()
                .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
                .is_none()
            {
                return Err(DeepXHttpError::InvalidRequest(
                    "balance-changes timestamp bound is out of range".to_string(),
                ));
            }
        }
        if self.page_size == Some(0) {
            return Err(DeepXHttpError::InvalidRequest(
                "balance-changes page_size must be greater than zero".to_string(),
            ));
        }
        let mut filters = std::collections::HashSet::new();
        if self
            .change_types
            .iter()
            .any(|change_type| !filters.insert(*change_type))
        {
            return Err(DeepXHttpError::InvalidRequest(
                "balance-changes contains duplicate change types".to_string(),
            ));
        }
        validate_cursor("balance-changes", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXBalanceChangesQuery<'_> {
        DeepXBalanceChangesQuery {
            user: self.subaccount.as_deref(),
            wallet: self.wallet.as_deref(),
            start_time: self.start_ms,
            end_time: self.end_ms,
            change_type: (!self.change_types.is_empty()).then(|| {
                self.change_types
                    .iter()
                    .map(|value| value.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            }),
            cursor: self.cursor.as_deref(),
            page_size: self.page_size,
        }
    }
}

impl DeepXHourlyUnsettledFundingRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        let account = match (&self.subaccount, &self.wallet) {
            (Some(subaccount), None) => subaccount,
            (None, Some(wallet)) => wallet,
            _ => {
                return Err(DeepXHttpError::InvalidRequest(
                    "hourly-unsettled-funding requires exactly one subaccount or wallet"
                        .to_string(),
                ));
            }
        };
        validate_account_id("hourly-unsettled-funding", account)?;
        if self.market_id == Some(0) {
            return Err(DeepXHttpError::InvalidRequest(
                "hourly-unsettled-funding market_id must be greater than zero".to_string(),
            ));
        }
        if self
            .start_ms
            .zip(self.end_ms)
            .is_some_and(|(start, end)| start > end)
        {
            return Err(DeepXHttpError::InvalidRequest(
                "hourly-unsettled-funding start_ms must not exceed end_ms".to_string(),
            ));
        }
        for bound in [self.start_ms, self.end_ms].into_iter().flatten() {
            validate_millisecond("hourly-unsettled-funding", bound)?;
        }
        if self
            .page_size
            .is_some_and(|size| !(1..=100).contains(&size))
        {
            return Err(DeepXHttpError::InvalidRequest(
                "hourly-unsettled-funding page_size must be from 1 through 100".to_string(),
            ));
        }
        if let Some(cursor) = &self.cursor {
            validate_millisecond(
                "hourly-unsettled-funding cursor",
                cursor.boundary_timestamp_ms,
            )?;
            if cursor.market_id == 0
                || cursor.event_id.trim().is_empty()
                || validate_account_id("hourly-unsettled-funding cursor", &cursor.subaccount)
                    .is_err()
                || self
                    .subaccount
                    .as_deref()
                    .is_some_and(|value| !cursor.subaccount.eq_ignore_ascii_case(value))
                || self
                    .market_id
                    .is_some_and(|value| cursor.market_id != value)
                || self
                    .start_ms
                    .is_some_and(|value| cursor.boundary_timestamp_ms < value)
                || self
                    .end_ms
                    .is_some_and(|value| cursor.boundary_timestamp_ms > value)
            {
                return Err(DeepXHttpError::InvalidRequest(
                    "hourly-unsettled-funding cursor is invalid or outside the request scope"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXHourlyUnsettledFundingQuery<'_> {
        DeepXHourlyUnsettledFundingQuery {
            subaccount: self.subaccount.as_deref(),
            wallet: self.wallet.as_deref(),
            market_id: self.market_id,
            start: self.start_ms,
            end: self.end_ms,
            cursor_timestamp: self
                .cursor
                .as_ref()
                .map(|cursor| cursor.boundary_timestamp_ms),
            cursor_market_id: self.cursor.as_ref().map(|cursor| cursor.market_id),
            cursor_subaccount: self
                .cursor
                .as_ref()
                .map(|cursor| cursor.subaccount.as_str()),
            cursor_event_id: self.cursor.as_ref().map(|cursor| cursor.event_id.as_str()),
            page_size: self.page_size,
            sort: self.sort,
        }
    }
}

impl DeepXLiquidationRecordsRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        let address = match (&self.subaccount, &self.wallet) {
            (Some(subaccount), None) => subaccount,
            (None, Some(wallet)) => wallet,
            _ => {
                return Err(DeepXHttpError::InvalidRequest(
                    "liquidation-records requires exactly one subaccount or wallet".to_string(),
                ));
            }
        };
        if address.len() != 42
            || !address.starts_with("0x")
            || !address.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
        {
            return Err(DeepXHttpError::InvalidRequest(
                "liquidation-records requires a 20-byte hex account address".to_string(),
            ));
        }
        if self.page_size == Some(0) {
            return Err(DeepXHttpError::InvalidRequest(
                "liquidation-records page_size must be greater than zero".to_string(),
            ));
        }
        let mut filters = std::collections::HashSet::new();
        if self
            .liquidation_types
            .iter()
            .any(|liquidation_type| !filters.insert(*liquidation_type))
        {
            return Err(DeepXHttpError::InvalidRequest(
                "liquidation-records contains duplicate liquidation types".to_string(),
            ));
        }
        validate_cursor("liquidation-records", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXLiquidationRecordsQuery<'_> {
        DeepXLiquidationRecordsQuery {
            wallet: self.wallet.as_deref(),
            subaccount: self.subaccount.as_deref(),
            liquidation_type: (!self.liquidation_types.is_empty()).then(|| {
                self.liquidation_types
                    .iter()
                    .map(|value| value.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            }),
            cursor: self.cursor.as_deref(),
            sort: self.sort,
            page_size: self.page_size,
        }
    }
}

impl DeepXPerpLiquidationPriceRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_account_page_request(
            "perp-liquidation-price",
            &self.subaccount,
            self.market_id,
            None,
            None,
        )?;
        if self
            .market_name
            .as_deref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-liquidation-price market_name must not be empty".to_string(),
            ));
        }
        if self.market_name.is_some() == self.market_id.is_some() {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-liquidation-price requires exactly one market_name or market_id".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXPerpLiquidationPriceQuery<'_> {
        DeepXPerpLiquidationPriceQuery {
            address: &self.subaccount,
            name: self.market_name.as_deref(),
            market_id: self.market_id,
        }
    }
}

impl DeepXPerpHistoryOrdersRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_account_page_request(
            "perp-history-orders",
            &self.subaccount,
            self.market_id,
            self.cursor.as_deref(),
            self.page_size,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXPerpHistoryOrdersQuery<'_> {
        DeepXPerpHistoryOrdersQuery {
            user: &self.subaccount,
            market_id: self.market_id,
            cursor: self.cursor.as_deref(),
            sort: self.sort,
            page_size: self.page_size,
        }
    }
}

impl DeepXSpotOpenOrdersRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_spot_order_request(
            "spot-open-orders",
            &self.subaccount,
            self.name.as_deref(),
            self.pair.as_deref(),
            self.cursor.as_deref(),
            self.page_size,
            true,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXSpotOrdersQuery<'_> {
        DeepXSpotOrdersQuery {
            name: self.name.as_deref(),
            pair: self.pair.as_deref(),
            user: &self.subaccount,
            order_side: self.order_side,
            cursor: self.cursor.as_deref(),
            sort: self.sort,
            page_size: self.page_size,
        }
    }
}

impl DeepXSpotOrderByIdRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_account_id("spot-order-by-id", &self.subaccount)?;
        validate_spot_market_selector(
            "spot-order-by-id",
            self.name.as_deref(),
            self.pair.as_deref(),
        )?;
        if self.order_id.is_empty()
            || !self.order_id.bytes().all(|value| value.is_ascii_digit())
            || self.order_id.parse::<u64>().is_err()
        {
            return Err(DeepXHttpError::InvalidRequest(
                "spot-order-by-id order_id must be an exact decimal u64".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXSpotOrderByIdQuery<'_> {
        DeepXSpotOrderByIdQuery {
            name: self.name.as_deref(),
            pair: self.pair.as_deref(),
            user: &self.subaccount,
            oid: &self.order_id,
            order_side: self.order_side,
        }
    }
}

impl DeepXSpotHistoryOrdersRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_spot_order_request(
            "spot-history-orders",
            &self.subaccount,
            self.name.as_deref(),
            self.pair.as_deref(),
            self.cursor.as_deref(),
            self.page_size,
            false,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXSpotOrdersQuery<'_> {
        DeepXSpotOrdersQuery {
            name: self.name.as_deref(),
            pair: self.pair.as_deref(),
            user: &self.subaccount,
            order_side: self.order_side,
            cursor: self.cursor.as_deref(),
            sort: self.sort,
            page_size: self.page_size,
        }
    }
}

impl DeepXSpotAccountTradesRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_spot_order_request(
            "spot-account-trades",
            &self.subaccount,
            self.name.as_deref(),
            self.pair.as_deref(),
            self.cursor.as_deref(),
            self.page_size,
            false,
        )?;
        match (&self.order_id, self.order_side) {
            (None, None) => {}
            (Some(order_id), Some(_)) if self.name.is_some() || self.pair.is_some() => {
                if order_id.is_empty()
                    || !order_id.bytes().all(|value| value.is_ascii_digit())
                    || order_id.parse::<u64>().is_err()
                {
                    return Err(DeepXHttpError::InvalidRequest(
                        "spot-account-trades order_id must be an exact decimal u64".to_string(),
                    ));
                }
            }
            _ => {
                return Err(DeepXHttpError::InvalidRequest(
                    concat!(
                        "spot-account-trades order_id requires one market selector and order_side, ",
                        "which must otherwise be omitted",
                    )
                    .to_string(),
                ));
            }
        }
        if self
            .start_ms
            .zip(self.end_ms)
            .is_some_and(|(start, end)| start > end)
        {
            return Err(DeepXHttpError::InvalidRequest(
                "spot-account-trades start_ms must not exceed end_ms".to_string(),
            ));
        }
        for bound in [self.start_ms, self.end_ms].into_iter().flatten() {
            validate_millisecond("spot-account-trades", bound)?;
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXSpotAccountTradesQuery<'_> {
        DeepXSpotAccountTradesQuery {
            order_id: self.order_id.as_deref(),
            order_side: self.order_side,
            name: self.name.as_deref(),
            pair: self.pair.as_deref(),
            user: &self.subaccount,
            cursor: self.cursor.as_deref(),
            sort: self.sort,
            start: self.start_ms,
            end: self.end_ms,
            page_size: self.page_size,
        }
    }
}

impl DeepXSpotWalletOrdersRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_spot_wallet_grouped_request(
            "spot-wallet-orders",
            self.wallet.as_deref(),
            self.name.as_deref(),
            self.pair.as_deref(),
            self.cursor.as_deref(),
            self.start_ms,
            self.end_ms,
            self.page_size,
            false,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXSpotWalletOrdersQuery<'_> {
        DeepXSpotWalletOrdersQuery {
            name: self.name.as_deref(),
            pair: self.pair.as_deref(),
            address: self.wallet.as_deref(),
            order_side: self.order_side,
            cursor: self.cursor.as_deref(),
            start: self.start_ms,
            end: self.end_ms,
            sort: self.sort,
            page_size: self.page_size,
        }
    }
}

impl DeepXSpotWalletTradesRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_spot_wallet_grouped_request(
            "spot-wallet-trades",
            Some(&self.wallet),
            self.name.as_deref(),
            self.pair.as_deref(),
            self.cursor.as_deref(),
            self.start_ms,
            self.end_ms,
            self.page_size,
            true,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXSpotWalletTradesQuery<'_> {
        DeepXSpotWalletTradesQuery {
            name: self.name.as_deref(),
            pair: self.pair.as_deref(),
            address: &self.wallet,
            cursor: self.cursor.as_deref(),
            sort: self.sort,
            start: self.start_ms,
            end: self.end_ms,
            page_size: self.page_size,
        }
    }
}

impl DeepXAllSubaccountsRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.page_size == Some(0) {
            return Err(DeepXHttpError::InvalidRequest(
                "all-subaccounts page_size must be greater than zero".to_string(),
            ));
        }
        validate_cursor("all-subaccounts", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXAllSubaccountsQuery<'_> {
        DeepXAllSubaccountsQuery {
            cursor: self.cursor.as_deref(),
            page_size: self.page_size,
        }
    }
}

impl DeepXQuotaHistoryRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_account_id("quota-history", &self.wallet)?;
        if let Some(buyer) = &self.buyer_address {
            validate_account_id("quota-history", buyer)?;
        }
        if self.limit.is_some_and(|limit| !(1..=100).contains(&limit)) {
            return Err(DeepXHttpError::InvalidRequest(
                "quota-history limit must be between 1 and 100".to_string(),
            ));
        }
        validate_cursor("quota-history", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXQuotaHistoryQuery<'_> {
        DeepXQuotaHistoryQuery {
            wallet: &self.wallet,
            limit: self.limit,
            buyer_address: self.buyer_address.as_deref(),
            history_type: self.history_type,
            cursor: self.cursor.as_deref(),
        }
    }
}

impl DeepXPerpAccountTradesRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_account_page_request(
            "perp-account-trades",
            &self.subaccount,
            self.market_id,
            self.cursor.as_deref(),
            self.page_size,
        )?;
        match (&self.order_id, self.is_long) {
            (None, None) => {}
            (Some(order_id), Some(_)) if self.market_id.is_some() => {
                if order_id.is_empty()
                    || !order_id.bytes().all(|value| value.is_ascii_digit())
                    || order_id.parse::<u64>().is_err()
                {
                    return Err(DeepXHttpError::InvalidRequest(
                        "perp-account-trades order_id must be an exact decimal u64".to_string(),
                    ));
                }
            }
            _ => {
                return Err(DeepXHttpError::InvalidRequest(
                    concat!(
                        "perp-account-trades order_id requires market_id and is_long, ",
                        "which must otherwise be omitted",
                    )
                    .to_string(),
                ));
            }
        }
        if self
            .start_ms
            .zip(self.end_ms)
            .is_some_and(|(start, end)| start > end)
        {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-account-trades start_ms must not exceed end_ms".to_string(),
            ));
        }
        for bound in [self.start_ms, self.end_ms].into_iter().flatten() {
            let valid = i64::try_from(bound)
                .ok()
                .and_then(|value| jiff::Timestamp::from_millisecond(value).ok());
            if valid.is_none() {
                return Err(DeepXHttpError::InvalidRequest(
                    "perp-account-trades timestamp bound is out of range".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXPerpAccountTradesQuery<'_> {
        DeepXPerpAccountTradesQuery {
            order_id: self.order_id.as_deref(),
            is_long: self.is_long,
            user: &self.subaccount,
            market_id: self.market_id,
            cursor: self.cursor.as_deref(),
            sort: self.sort,
            start: self.start_ms,
            end: self.end_ms,
            page_size: self.page_size,
        }
    }
}

impl DeepXPerpWalletTradesRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.wallet.len() != 42
            || !self.wallet.starts_with("0x")
            || !self.wallet.as_bytes()[2..]
                .iter()
                .all(u8::is_ascii_hexdigit)
        {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-wallet-trades requires a 20-byte hex wallet".to_string(),
            ));
        }
        match (&self.market_name, self.market_id) {
            (Some(name), None) if name.trim().is_empty() => {
                return Err(DeepXHttpError::InvalidRequest(
                    "perp-wallet-trades market_name must not be empty".to_string(),
                ));
            }
            (None, Some(0)) => {
                return Err(DeepXHttpError::InvalidRequest(
                    "perp-wallet-trades market_id must be greater than zero".to_string(),
                ));
            }
            (Some(_), Some(_)) => {
                return Err(DeepXHttpError::InvalidRequest(
                    "perp-wallet-trades market_name and market_id are mutually exclusive"
                        .to_string(),
                ));
            }
            _ => {}
        }
        if self
            .start_ms
            .zip(self.end_ms)
            .is_some_and(|(start, end)| start > end)
        {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-wallet-trades start_ms must not exceed end_ms".to_string(),
            ));
        }
        for bound in [self.start_ms, self.end_ms].into_iter().flatten() {
            if i64::try_from(bound)
                .ok()
                .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
                .is_none()
            {
                return Err(DeepXHttpError::InvalidRequest(
                    "perp-wallet-trades timestamp bound is out of range".to_string(),
                ));
            }
        }
        if self.page_size == Some(0) {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-wallet-trades page_size must be greater than zero".to_string(),
            ));
        }
        validate_cursor("perp-wallet-trades", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXPerpWalletTradesQuery<'_> {
        DeepXPerpWalletTradesQuery {
            address: &self.wallet,
            name: self.market_name.as_deref(),
            market_id: self.market_id,
            cursor: self.cursor.as_deref(),
            sort: self.sort,
            start: self.start_ms,
            end: self.end_ms,
            page_size: self.page_size,
        }
    }
}

impl DeepXPerpWalletOrdersRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_wallet_grouped_request(
            "perp-wallet-orders",
            &self.wallet,
            self.market_name.as_deref(),
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.cursor.as_deref(),
            self.page_size,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXPerpWalletOrdersQuery<'_> {
        DeepXPerpWalletOrdersQuery {
            address: &self.wallet,
            name: self.market_name.as_deref(),
            market_id: self.market_id,
            is_long: self.is_long,
            cursor: self.cursor.as_deref(),
            sort: self.sort,
            start: self.start_ms,
            end: self.end_ms,
            page_size: self.page_size,
        }
    }
}

impl DeepXPerpFundingFeeRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_account_page_request(
            "perp-funding-fee",
            &self.subaccount,
            self.market_id,
            self.cursor.as_deref(),
            self.page_size,
        )?;
        if self
            .start_ms
            .zip(self.end_ms)
            .is_some_and(|(start, end)| start > end)
        {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-funding-fee start_ms must not exceed end_ms".to_string(),
            ));
        }
        for bound in [self.start_ms, self.end_ms].into_iter().flatten() {
            let valid = i64::try_from(bound)
                .ok()
                .and_then(|value| jiff::Timestamp::from_millisecond(value).ok());
            if valid.is_none() {
                return Err(DeepXHttpError::InvalidRequest(
                    "perp-funding-fee timestamp bound is out of range".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXPerpFundingFeeQuery<'_> {
        DeepXPerpFundingFeeQuery {
            user: &self.subaccount,
            market_id: self.market_id,
            start: self.start_ms,
            end: self.end_ms,
            cursor: self.cursor.as_deref(),
            page_size: self.page_size,
        }
    }
}

impl DeepXWalletFundingFeeRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_account_page_request(
            "wallet-funding-fee",
            &self.wallet,
            self.market_id,
            self.cursor.as_deref(),
            self.page_size,
        )?;
        if self
            .market_name
            .as_deref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(DeepXHttpError::InvalidRequest(
                "wallet-funding-fee market_name must not be empty".to_string(),
            ));
        }
        if self.market_name.is_some() && self.market_id.is_some() {
            return Err(DeepXHttpError::InvalidRequest(
                "wallet-funding-fee accepts either market_name or market_id, not both".to_string(),
            ));
        }
        if self
            .start_ms
            .zip(self.end_ms)
            .is_some_and(|(start, end)| start > end)
        {
            return Err(DeepXHttpError::InvalidRequest(
                "wallet-funding-fee start_ms must not exceed end_ms".to_string(),
            ));
        }
        for bound in [self.start_ms, self.end_ms].into_iter().flatten() {
            if i64::try_from(bound)
                .ok()
                .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
                .is_none()
            {
                return Err(DeepXHttpError::InvalidRequest(
                    "wallet-funding-fee timestamp bound is out of range".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXWalletFundingFeeQuery<'_> {
        DeepXWalletFundingFeeQuery {
            address: &self.wallet,
            name: self.market_name.as_deref(),
            market_id: self.market_id,
            start: self.start_ms,
            end: self.end_ms,
            cursor: self.cursor.as_deref(),
            page_size: self.page_size,
        }
    }
}

impl DeepXPerpPositionsRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_account_page_request(
            "perp-positions",
            &self.subaccount,
            self.market_id,
            self.cursor.as_deref(),
            self.page_size,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXPerpPositionsQuery<'_> {
        DeepXPerpPositionsQuery {
            user: &self.subaccount,
            market_id: self.market_id,
            only_closed: self.only_closed,
            address_type: "subaccount",
            cursor: self.cursor.as_deref(),
            page_size: self.page_size,
        }
    }
}

/// Supported aggregation periods for perpetual volume statistics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum DeepXPerpVolumePeriod {
    /// One-hour window.
    #[serde(rename = "1h")]
    OneHour,
    /// Twenty-four-hour window.
    #[serde(rename = "24h")]
    TwentyFourHours,
    /// Seven-day window.
    #[serde(rename = "7d")]
    SevenDays,
    /// Thirty-day window.
    #[serde(rename = "30d")]
    ThirtyDays,
}

/// Supported aggregation periods for Spot volume statistics.
pub type DeepXSpotVolumePeriod = DeepXPerpVolumePeriod;

/// Supported time frames for perpetual candle-shaped history.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub enum DeepXPerpCandleInterval {
    /// One-minute buckets.
    #[default]
    #[serde(rename = "1m")]
    OneMinute,
    /// Three-minute buckets.
    #[serde(rename = "3m")]
    ThreeMinutes,
    /// Five-minute buckets.
    #[serde(rename = "5m")]
    FiveMinutes,
    /// Fifteen-minute buckets.
    #[serde(rename = "15m")]
    FifteenMinutes,
    /// Thirty-minute buckets.
    #[serde(rename = "30m")]
    ThirtyMinutes,
    /// One-hour buckets.
    #[serde(rename = "1h")]
    OneHour,
    /// Two-hour buckets.
    #[serde(rename = "2h")]
    TwoHours,
    /// Four-hour buckets.
    #[serde(rename = "4h")]
    FourHours,
    /// Eight-hour buckets.
    #[serde(rename = "8h")]
    EightHours,
    /// Twelve-hour buckets.
    #[serde(rename = "12h")]
    TwelveHours,
    /// One-day buckets.
    #[serde(rename = "1d")]
    OneDay,
    /// Three-day buckets.
    #[serde(rename = "3d")]
    ThreeDays,
    /// One-week buckets.
    #[serde(rename = "1w")]
    OneWeek,
    /// One-month buckets.
    #[serde(rename = "1M")]
    OneMonth,
}

/// Supported time frames for Spot candle history.
pub type DeepXSpotCandleInterval = DeepXPerpCandleInterval;

/// Supported aggregation intervals for lending histories.
pub type DeepXLendingHistoryInterval = DeepXPerpCandleInterval;

/// Optional lending market and asset filters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeepXLendingMarketRequest {
    /// Optional deployment-provided lending market ID.
    pub market_id: Option<u64>,
    /// Optional venue asset symbol.
    pub asset: Option<String>,
}

/// Request for one bounded lending APR or pool-status history response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXLendingHistoryRequest {
    /// Optional deployment-provided lending market ID.
    pub market_id: Option<u64>,
    /// Optional venue asset symbol.
    pub asset: Option<String>,
    /// Venue aggregation interval.
    pub interval: DeepXLendingHistoryInterval,
    /// Inclusive lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Optional inclusive upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Optional maximum number of observations, from 1 through 5000.
    pub limit: Option<u32>,
    /// Requested venue response order.
    pub sort: DeepXAccountSortOrder,
}

impl DeepXLendingMarketRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.market_id == Some(0) {
            return Err(DeepXHttpError::InvalidRequest(
                "lending market_id must be greater than zero".to_string(),
            ));
        }
        if self
            .asset
            .as_deref()
            .is_some_and(|asset| asset.trim().is_empty())
        {
            return Err(DeepXHttpError::InvalidRequest(
                "lending asset must not be empty".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXLendingMarketQuery<'_> {
        DeepXLendingMarketQuery {
            market_id: self.market_id,
            asset: self.asset.as_deref(),
        }
    }
}

impl DeepXLendingHistoryRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        DeepXLendingMarketRequest {
            market_id: self.market_id,
            asset: self.asset.clone(),
        }
        .validate()?;
        if self.end_ms.is_some_and(|end| self.start_ms > end) {
            return Err(DeepXHttpError::InvalidRequest(
                "lending history start_ms must not exceed end_ms".to_string(),
            ));
        }
        for bound in std::iter::once(self.start_ms).chain(self.end_ms) {
            if i64::try_from(bound)
                .ok()
                .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
                .is_none()
            {
                return Err(DeepXHttpError::InvalidRequest(
                    "lending history timestamp bound is out of range".to_string(),
                ));
            }
        }
        if self
            .limit
            .is_some_and(|limit| !(1..=5_000).contains(&limit))
        {
            return Err(DeepXHttpError::InvalidRequest(
                "lending history limit must be from 1 through 5000".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXLendingHistoryQuery<'_> {
        DeepXLendingHistoryQuery {
            market_id: self.market_id,
            asset: self.asset.as_deref(),
            time_frame: self.interval,
            start: self.start_ms,
            end: self.end_ms,
            limit: self.limit,
            sort: self.sort,
        }
    }
}

/// Request for one perpetual volume-statistics window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpVolumeRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue-defined aggregation period.
    pub period: DeepXPerpVolumePeriod,
}

/// Request for the raw perpetual last price.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpLastPriceRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
}

/// Request for one potentially price-aggregated perpetual order book.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpOrderBookRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Optional positive server-side price aggregation tick.
    pub tick_size: Option<Decimal>,
}

impl DeepXPerpLastPriceRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.market_id == 0 {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-last-price market_id must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXPerpLastPriceQuery {
        DeepXPerpLastPriceQuery {
            market_id: self.market_id,
        }
    }
}

impl DeepXPerpOrderBookRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.market_id == 0 {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-order-book market_id must be greater than zero".to_string(),
            ));
        }
        if self.tick_size.is_some_and(|tick| tick <= Decimal::ZERO) {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-order-book tick_size must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXPerpOrderBookQuery {
        DeepXPerpOrderBookQuery {
            market_id: self.market_id,
            tick_size: self.tick_size.map(|tick| tick.to_string()),
        }
    }
}

impl DeepXPerpVolumeRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.market_id == 0 {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-volume market_id must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXPerpVolumeQuery {
        DeepXPerpVolumeQuery {
            market_id: self.market_id,
            period: self.period,
        }
    }
}

/// Request for one page of perpetual funding-rate history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFundingRateRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of rows requested from the venue.
    pub limit: Option<u32>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
}

/// Request for one page of perpetual open-interest history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXOpenInterestRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of rows requested from the venue.
    pub limit: Option<u32>,
}

/// Request for one page of perpetual long-short ratio history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXLongShortRatioRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of rows requested from the venue.
    pub limit: Option<u32>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
}

/// Request for one descending page of raw perpetual trades.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpTradesRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Number of trades requested from the venue.
    pub page_size: Option<u32>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
}

/// Request for one globally paginated raw Spot trade page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotTradesRequest {
    /// Optional venue market name, mutually exclusive with `pair`.
    pub name: Option<String>,
    /// Optional deployment-provided bytes32 pair identity, mutually exclusive with `name`.
    pub pair: Option<String>,
    /// Optional wallet whose owned subaccounts must participate in returned trades.
    pub wallet: Option<String>,
    /// Optional inclusive lower timestamp bound in Unix milliseconds.
    pub start_ms: Option<u64>,
    /// Optional inclusive upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
    /// Requested response ordering.
    pub sort: DeepXAccountSortOrder,
    /// Optional number of trades requested from the venue.
    pub page_size: Option<u32>,
}

/// Request for one ascending page of raw Spot candles.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotCandlesRequest {
    /// Optional venue market name, mutually exclusive with `pair`.
    pub name: Option<String>,
    /// Optional deployment-provided bytes32 pair identity, mutually exclusive with `name`.
    pub pair: Option<String>,
    /// Venue candle time frame.
    pub interval: DeepXSpotCandleInterval,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Optional upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Optional maximum number of candles, from 1 through 5000.
    pub limit: Option<u32>,
}

/// Request for one Spot volume-statistics window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotVolumeRequest {
    /// Optional venue market name, mutually exclusive with `pair`.
    pub name: Option<String>,
    /// Optional deployment-provided bytes32 pair identity, mutually exclusive with `name`.
    pub pair: Option<String>,
    /// Venue-defined aggregation period.
    pub period: DeepXSpotVolumePeriod,
}

/// Request for the raw Spot last price.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotLastPriceRequest {
    /// Optional venue market name, mutually exclusive with `pair`.
    pub name: Option<String>,
    /// Optional deployment-provided bytes32 pair identity, mutually exclusive with `name`.
    pub pair: Option<String>,
}

/// Request for one potentially price-aggregated Spot order book.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSpotOrderBookRequest {
    /// Optional venue market name, mutually exclusive with `pair`.
    pub name: Option<String>,
    /// Optional deployment-provided bytes32 pair identity, mutually exclusive with `name`.
    pub pair: Option<String>,
    /// Optional positive server-side price aggregation tick.
    pub tick_size: Option<Decimal>,
}

/// Bounded, descending raw perpetual trade history over an inclusive millisecond range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpTradesHistoryRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Inclusive lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Inclusive upper timestamp bound in Unix milliseconds.
    pub end_ms: u64,
    /// Maximum number of records requested on each page.
    pub page_size: u32,
    /// Strict local page budget; exhaustion fails rather than returning a partial history.
    pub max_pages: usize,
}

impl DeepXPerpTradesHistoryRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_history_request(
            "perp-trades-history",
            self.market_id,
            self.start_ms,
            Some(self.end_ms),
            Some(self.page_size),
        )?;
        if self.max_pages == 0 {
            return Err(DeepXHttpError::InvalidPaginationLimit);
        }
        if self
            .max_pages
            .checked_mul(self.page_size as usize)
            .is_none()
        {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-trades-history record budget overflows".to_string(),
            ));
        }
        for bound in [self.start_ms, self.end_ms] {
            let valid = i64::try_from(bound)
                .ok()
                .and_then(|value| jiff::Timestamp::from_millisecond(value).ok());
            if valid.is_none() {
                return Err(DeepXHttpError::InvalidRequest(
                    "perp-trades-history timestamp bound is out of range".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn as_query<'a>(&self, cursor: Option<&'a str>) -> DeepXPerpTradesQuery<'a> {
        DeepXPerpTradesQuery {
            market_id: self.market_id,
            page_size: Some(self.page_size),
            cursor,
            start: Some(self.start_ms),
            end: Some(self.end_ms),
            sort: "DESC",
        }
    }
}

/// Request for one ascending page of raw perpetual candles.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpCandlesRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue candle time frame.
    pub interval: DeepXPerpCandleInterval,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of candles requested from the venue.
    pub limit: Option<u32>,
}

/// Request for one ascending page of raw perpetual mark-price history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpMarkPriceRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue candle time frame.
    pub interval: DeepXPerpCandleInterval,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of observations requested from the venue.
    pub limit: Option<u32>,
}

/// Request for one ascending page of raw perpetual oracle-price history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpOraclePriceRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue candle time frame.
    pub interval: DeepXPerpCandleInterval,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of observations requested from the venue.
    pub limit: Option<u32>,
}

impl DeepXPerpOraclePriceRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_bounded_candle_request(
            "perp-oracle-price",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXPerpCandlesQuery {
        DeepXPerpCandlesQuery::new(
            self.market_id,
            self.interval,
            self.start_ms,
            self.end_ms,
            self.limit,
        )
    }
}

impl DeepXPerpMarkPriceRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_bounded_candle_request(
            "perp-mark-price",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXPerpCandlesQuery {
        DeepXPerpCandlesQuery::new(
            self.market_id,
            self.interval,
            self.start_ms,
            self.end_ms,
            self.limit,
        )
    }
}

impl DeepXPerpCandlesRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_bounded_candle_request(
            "perp-candles",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXPerpCandlesQuery {
        DeepXPerpCandlesQuery::new(
            self.market_id,
            self.interval,
            self.start_ms,
            self.end_ms,
            self.limit,
        )
    }
}

impl DeepXPerpTradesRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.market_id == 0 {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-trades market_id must be greater than zero".to_string(),
            ));
        }
        if self.page_size == Some(0) {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-trades page_size must be greater than zero".to_string(),
            ));
        }
        validate_cursor("perp-trades", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXPerpTradesQuery<'_> {
        DeepXPerpTradesQuery {
            market_id: self.market_id,
            page_size: self.page_size,
            cursor: self.cursor.as_deref(),
            start: None,
            end: None,
            sort: "DESC",
        }
    }
}

impl DeepXSpotTradesRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        match (&self.name, &self.pair) {
            (Some(name), None) if name.trim().is_empty() => {
                return Err(DeepXHttpError::InvalidRequest(
                    "spot-trades name must not be empty".to_string(),
                ));
            }
            (None, Some(pair))
                if pair.len() != 66
                    || !pair.starts_with("0x")
                    || !pair.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit) =>
            {
                return Err(DeepXHttpError::InvalidRequest(
                    "spot-trades pair must be a 32-byte hex identity".to_string(),
                ));
            }
            (Some(_), Some(_)) => {
                return Err(DeepXHttpError::InvalidRequest(
                    "spot-trades name and pair are mutually exclusive".to_string(),
                ));
            }
            _ => {}
        }
        if self.wallet.as_deref().is_some_and(|wallet| {
            wallet.len() != 42
                || !wallet.starts_with("0x")
                || !wallet.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
        }) {
            return Err(DeepXHttpError::InvalidRequest(
                "spot-trades wallet must be a 20-byte hex account address".to_string(),
            ));
        }
        if self
            .end_ms
            .is_some_and(|end_ms| self.start_ms.is_some_and(|start_ms| start_ms > end_ms))
        {
            return Err(DeepXHttpError::InvalidRequest(
                "spot-trades start_ms must not exceed end_ms".to_string(),
            ));
        }
        for bound in [self.start_ms, self.end_ms].into_iter().flatten() {
            let valid = i64::try_from(bound)
                .ok()
                .and_then(|value| jiff::Timestamp::from_millisecond(value).ok());
            if valid.is_none() {
                return Err(DeepXHttpError::InvalidRequest(
                    "spot-trades timestamp bound is out of range".to_string(),
                ));
            }
        }
        if self.page_size == Some(0) {
            return Err(DeepXHttpError::InvalidRequest(
                "spot-trades page_size must be greater than zero".to_string(),
            ));
        }
        validate_cursor("spot-trades", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXSpotTradesQuery<'_> {
        DeepXSpotTradesQuery {
            name: self.name.as_deref(),
            pair: self.pair.as_deref(),
            wallet: self.wallet.as_deref(),
            start: self.start_ms,
            end: self.end_ms,
            cursor: self.cursor.as_deref(),
            sort: self.sort,
            page_size: self.page_size,
        }
    }
}

impl DeepXSpotCandlesRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_spot_market_selector("spot-candles", self.name.as_deref(), self.pair.as_deref())?;
        if self.end_ms.is_some_and(|end| self.start_ms > end) {
            return Err(DeepXHttpError::InvalidRequest(
                "spot-candles start_ms must not exceed end_ms".to_string(),
            ));
        }
        for bound in std::iter::once(self.start_ms).chain(self.end_ms) {
            if i64::try_from(bound)
                .ok()
                .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
                .is_none()
            {
                return Err(DeepXHttpError::InvalidRequest(
                    "spot-candles timestamp bound is out of range".to_string(),
                ));
            }
        }
        if self
            .limit
            .is_some_and(|limit| !(1..=5_000).contains(&limit))
        {
            return Err(DeepXHttpError::InvalidRequest(
                "spot-candles limit must be from 1 through 5000".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXSpotCandlesQuery<'_> {
        DeepXSpotCandlesQuery {
            name: self.name.as_deref(),
            pair: self.pair.as_deref(),
            time_frame: self.interval,
            start: self.start_ms,
            end: self.end_ms,
            limit: self.limit,
            sort: "ASC",
            trade_view: false,
        }
    }
}

impl DeepXSpotVolumeRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_spot_market_selector("spot-volume", self.name.as_deref(), self.pair.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXSpotVolumeQuery<'_> {
        DeepXSpotVolumeQuery {
            name: self.name.as_deref(),
            pair: self.pair.as_deref(),
            period: self.period,
        }
    }
}

impl DeepXSpotLastPriceRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_spot_market_selector(
            "spot-last-price",
            self.name.as_deref(),
            self.pair.as_deref(),
        )
    }

    pub(crate) fn as_query(&self) -> DeepXSpotLastPriceQuery<'_> {
        DeepXSpotLastPriceQuery {
            name: self.name.as_deref(),
            pair: self.pair.as_deref(),
        }
    }
}

impl DeepXSpotOrderBookRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_spot_market_selector(
            "spot-order-book",
            self.name.as_deref(),
            self.pair.as_deref(),
        )?;
        if self.tick_size.is_some_and(|tick| tick <= Decimal::ZERO) {
            return Err(DeepXHttpError::InvalidRequest(
                "spot-order-book tick_size must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXSpotOrderBookQuery<'_> {
        DeepXSpotOrderBookQuery {
            name: self.name.as_deref(),
            pair: self.pair.as_deref(),
            tick_size: self.tick_size.map(|tick| tick.to_string()),
        }
    }
}

impl DeepXLongShortRatioRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_history_request(
            "long-short-ratio",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )?;
        validate_cursor("long-short-ratio", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXLongShortRatioQuery<'_> {
        DeepXLongShortRatioQuery {
            market_id: self.market_id,
            start: self.start_ms,
            end: self.end_ms,
            limit: self.limit,
            cursor: self.cursor.as_deref(),
            interval: "1m",
            sort: "ASC",
        }
    }
}

impl DeepXOpenInterestRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_history_request(
            "open-interest",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXOpenInterestQuery {
        DeepXOpenInterestQuery {
            market_id: self.market_id,
            time_frame: "1m",
            start: self.start_ms,
            end: self.end_ms,
            limit: self.limit,
            sort: "ASC",
        }
    }
}

impl DeepXFundingRateRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_history_request(
            "funding-rate",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )?;
        validate_cursor("funding-rate", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXFundingRateQuery<'_> {
        DeepXFundingRateQuery {
            market_id: self.market_id,
            start: self.start_ms,
            end: self.end_ms,
            limit: self.limit,
            cursor: self.cursor.as_deref(),
            interval: "1m",
            sort: "ASC",
        }
    }

    pub(crate) fn as_descending_query(&self) -> DeepXFundingRateQuery<'_> {
        let mut query = self.as_query();
        query.sort = "DESC";
        query
    }
}

fn validate_account_id(endpoint: &str, account: &str) -> Result<()> {
    if account.len() == 42
        && account.starts_with("0x")
        && account.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
    {
        return Ok(());
    }
    Err(DeepXHttpError::InvalidRequest(format!(
        "{endpoint} requires a 20-byte hex account address"
    )))
}

fn validate_millisecond(endpoint: &str, value: u64) -> Result<()> {
    if i64::try_from(value)
        .ok()
        .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
        .is_some()
    {
        return Ok(());
    }
    Err(DeepXHttpError::InvalidRequest(format!(
        "{endpoint} timestamp is out of range"
    )))
}

fn validate_history_request(
    endpoint: &str,
    market_id: u64,
    start_ms: u64,
    end_ms: Option<u64>,
    limit: Option<u32>,
) -> Result<()> {
    if market_id == 0 {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} market_id must be greater than zero"
        )));
    }
    if end_ms.is_some_and(|end_ms| start_ms > end_ms) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} start_ms must not exceed end_ms"
        )));
    }
    if limit == Some(0) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} limit must be greater than zero"
        )));
    }
    Ok(())
}

fn validate_bounded_candle_request(
    endpoint: &str,
    market_id: u64,
    start_ms: u64,
    end_ms: Option<u64>,
    limit: Option<u32>,
) -> Result<()> {
    validate_history_request(endpoint, market_id, start_ms, end_ms, limit)?;
    if limit.is_some_and(|limit| limit > 5_000) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} limit must not exceed 5000"
        )));
    }
    Ok(())
}

fn validate_cursor(endpoint: &str, cursor: Option<&str>) -> Result<()> {
    if cursor.is_some_and(str::is_empty) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} cursor must not be empty"
        )));
    }
    Ok(())
}

fn validate_spot_market_selector(
    endpoint: &str,
    name: Option<&str>,
    pair: Option<&str>,
) -> Result<()> {
    match (name, pair) {
        (Some(name), None) if !name.trim().is_empty() => Ok(()),
        (None, Some(pair))
            if pair.len() == 66
                && pair.starts_with("0x")
                && pair.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit) =>
        {
            Ok(())
        }
        _ => Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} requires exactly one nonempty name or bytes32 pair"
        ))),
    }
}

fn validate_spot_order_request(
    endpoint: &str,
    subaccount: &str,
    name: Option<&str>,
    pair: Option<&str>,
    cursor: Option<&str>,
    page_size: Option<u32>,
    require_market: bool,
) -> Result<()> {
    validate_account_id(endpoint, subaccount)?;
    if require_market || name.is_some() || pair.is_some() {
        validate_spot_market_selector(endpoint, name, pair)?;
    }
    if page_size == Some(0) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} page_size must be greater than zero"
        )));
    }
    validate_cursor(endpoint, cursor)
}

fn validate_spot_wallet_grouped_request(
    endpoint: &str,
    wallet: Option<&str>,
    name: Option<&str>,
    pair: Option<&str>,
    cursor: Option<&str>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    require_market: bool,
) -> Result<()> {
    if let Some(wallet) = wallet {
        validate_account_id(endpoint, wallet)?;
    }
    if require_market || name.is_some() || pair.is_some() {
        validate_spot_market_selector(endpoint, name, pair)?;
    }
    if start_ms.zip(end_ms).is_some_and(|(start, end)| start > end) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} start_ms must not exceed end_ms"
        )));
    }
    for bound in [start_ms, end_ms].into_iter().flatten() {
        validate_millisecond(endpoint, bound)?;
    }
    if page_size == Some(0) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} page_size must be greater than zero"
        )));
    }
    validate_cursor(endpoint, cursor)
}

fn validate_account_page_request(
    endpoint: &str,
    subaccount: &str,
    market_id: Option<u64>,
    cursor: Option<&str>,
    page_size: Option<u32>,
) -> Result<()> {
    if subaccount.len() != 42
        || !subaccount.starts_with("0x")
        || !subaccount.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
    {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} requires a 20-byte hex subaccount"
        )));
    }
    if market_id == Some(0) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} market_id must be greater than zero"
        )));
    }
    if page_size == Some(0) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} page_size must be greater than zero"
        )));
    }
    validate_cursor(endpoint, cursor)
}

fn validate_wallet_grouped_request(
    endpoint: &str,
    wallet: &str,
    market_name: Option<&str>,
    market_id: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    cursor: Option<&str>,
    page_size: Option<u32>,
) -> Result<()> {
    if wallet.len() != 42
        || !wallet.starts_with("0x")
        || !wallet.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
    {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} requires a 20-byte hex wallet"
        )));
    }
    match (market_name, market_id) {
        (Some(name), None) if name.trim().is_empty() => {
            return Err(DeepXHttpError::InvalidRequest(format!(
                "{endpoint} market_name must not be empty"
            )));
        }
        (None, Some(0)) => {
            return Err(DeepXHttpError::InvalidRequest(format!(
                "{endpoint} market_id must be greater than zero"
            )));
        }
        (Some(_), Some(_)) => {
            return Err(DeepXHttpError::InvalidRequest(format!(
                "{endpoint} market_name and market_id are mutually exclusive"
            )));
        }
        _ => {}
    }
    if start_ms.zip(end_ms).is_some_and(|(start, end)| start > end) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} start_ms must not exceed end_ms"
        )));
    }
    for bound in [start_ms, end_ms].into_iter().flatten() {
        if i64::try_from(bound)
            .ok()
            .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
            .is_none()
        {
            return Err(DeepXHttpError::InvalidRequest(format!(
                "{endpoint} timestamp bound is out of range"
            )));
        }
    }
    if page_size == Some(0) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} page_size must be greater than zero"
        )));
    }
    validate_cursor(endpoint, cursor)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpOpenOrdersQuery<'a> {
    user: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_long: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    sort: DeepXAccountSortOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXBalanceChangesQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wallet: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_time: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_time: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    change_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXHourlyUnsettledFundingQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    subaccount: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wallet: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor_timestamp: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor_market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor_subaccount: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor_event_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
    sort: DeepXAccountSortOrder,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXLiquidationRecordsQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    wallet: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subaccount: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    liquidation_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    sort: DeepXAccountSortOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpLiquidationPriceQuery<'a> {
    address: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpHistoryOrdersQuery<'a> {
    user: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    sort: DeepXAccountSortOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXSpotOrdersQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair: Option<&'a str>,
    user: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_side: Option<DeepXSpotOrderSide>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    sort: DeepXAccountSortOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXSpotOrderByIdQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair: Option<&'a str>,
    user: &'a str,
    oid: &'a str,
    order_side: DeepXSpotOrderSide,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXSpotAccountTradesQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    order_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_side: Option<DeepXSpotOrderSide>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair: Option<&'a str>,
    user: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    sort: DeepXAccountSortOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXSpotWalletOrdersQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_side: Option<DeepXSpotOrderSide>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    sort: DeepXAccountSortOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXSpotWalletTradesQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair: Option<&'a str>,
    address: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    sort: DeepXAccountSortOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXAllSubaccountsQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXQuotaHistoryQuery<'a> {
    wallet: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    buyer_address: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history_type: Option<DeepXQuotaHistoryType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpAccountTradesQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    order_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_long: Option<bool>,
    user: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    sort: DeepXAccountSortOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpWalletTradesQuery<'a> {
    address: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    sort: DeepXAccountSortOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpWalletOrdersQuery<'a> {
    address: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_long: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    sort: DeepXAccountSortOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpFundingFeeQuery<'a> {
    user: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXWalletFundingFeeQuery<'a> {
    address: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpPositionsQuery<'a> {
    user: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    only_closed: Option<bool>,
    address_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXLendingMarketQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    asset: Option<&'a str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXLendingHistoryQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    asset: Option<&'a str>,
    time_frame: DeepXLendingHistoryInterval,
    start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    sort: DeepXAccountSortOrder,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXFundingRateQuery<'a> {
    market_id: u64,
    start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    interval: &'static str,
    sort: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXLongShortRatioQuery<'a> {
    market_id: u64,
    start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    interval: &'static str,
    sort: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXOpenInterestQuery {
    market_id: u64,
    time_frame: &'static str,
    start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    sort: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpTradesQuery<'a> {
    market_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    sort: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXSpotTradesQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wallet: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    sort: DeepXAccountSortOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXSpotCandlesQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair: Option<&'a str>,
    time_frame: DeepXSpotCandleInterval,
    start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    sort: &'static str,
    trade_view: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXSpotVolumeQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair: Option<&'a str>,
    period: DeepXSpotVolumePeriod,
}

#[derive(Debug, Serialize)]
pub(crate) struct DeepXSpotLastPriceQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair: Option<&'a str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXSpotOrderBookQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tick_size: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpCandlesQuery {
    market_id: u64,
    time_frame: DeepXPerpCandleInterval,
    start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    sort: &'static str,
    trade_view: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpVolumeQuery {
    market_id: u64,
    period: DeepXPerpVolumePeriod,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpLastPriceQuery {
    market_id: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpOrderBookQuery {
    market_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tick_size: Option<String>,
}

impl DeepXPerpCandlesQuery {
    fn new(
        market_id: u64,
        time_frame: DeepXPerpCandleInterval,
        start: u64,
        end: Option<u64>,
        limit: Option<u32>,
    ) -> Self {
        Self {
            market_id,
            time_frame,
            start,
            end,
            limit,
            sort: "ASC",
            trade_view: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn balance_changes_request() -> DeepXBalanceChangesRequest {
        DeepXBalanceChangesRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            start_ms: Some(1_789_498_000_000),
            end_ms: Some(1_789_523_000_000),
            change_types: vec![
                DeepXBalanceChangeType::FundingFee,
                DeepXBalanceChangeType::Settlement,
            ],
            cursor: Some("next".to_string()),
            page_size: Some(2),
        }
    }

    #[rstest]
    fn balance_changes_query_encodes_closed_filters() {
        let request = balance_changes_request();
        request.validate().unwrap();

        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["wallet"], request.wallet.unwrap());
        assert_eq!(encoded["startTime"], 1_789_498_000_000_u64);
        assert_eq!(encoded["endTime"], 1_789_523_000_000_u64);
        assert_eq!(encoded["changeType"], "FUNDING_FEE,SETTLEMENT");
        assert_eq!(encoded["cursor"], "next");
        assert_eq!(encoded["pageSize"], 2);
        assert!(encoded.get("user").is_none());
    }

    #[rstest]
    #[case("scope")]
    #[case("both")]
    #[case("address")]
    #[case("bounds")]
    #[case("timestamp")]
    #[case("page-size")]
    #[case("duplicate-filter")]
    #[case("cursor")]
    fn invalid_balance_changes_request_is_rejected(#[case] mutation: &str) {
        let mut request = balance_changes_request();
        match mutation {
            "scope" => request.wallet = None,
            "both" => request.subaccount = request.wallet.clone(),
            "address" => request.wallet = Some("invalid".to_string()),
            "bounds" => request.start_ms = Some(request.end_ms.unwrap() + 1),
            "timestamp" => request.end_ms = Some(u64::MAX),
            "page-size" => request.page_size = Some(0),
            "duplicate-filter" => request
                .change_types
                .push(DeepXBalanceChangeType::FundingFee),
            "cursor" => request.cursor = Some(String::new()),
            _ => unreachable!(),
        }
        assert!(request.validate().is_err());
    }

    fn hourly_unsettled_funding_request() -> DeepXHourlyUnsettledFundingRequest {
        DeepXHourlyUnsettledFundingRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            market_id: Some(3),
            start_ms: Some(1_789_488_000_000),
            end_ms: Some(1_789_522_000_000),
            cursor: Some(DeepXHourlyUnsettledFundingCursor {
                boundary_timestamp_ms: 1_789_506_862_596,
                market_id: 3,
                subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
                event_id: "4846:186852294:event:none:26".to_string(),
            }),
            page_size: Some(5),
            sort: DeepXAccountSortOrder::Descending,
        }
    }

    #[rstest]
    fn hourly_unsettled_funding_query_encodes_complete_keyset_cursor() {
        let request = hourly_unsettled_funding_request();
        request.validate().unwrap();

        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["wallet"], request.wallet.unwrap());
        assert_eq!(encoded["marketId"], 3);
        assert_eq!(encoded["start"], 1_789_488_000_000_u64);
        assert_eq!(encoded["end"], 1_789_522_000_000_u64);
        assert_eq!(encoded["cursorTimestamp"], 1_789_506_862_596_u64);
        assert_eq!(encoded["cursorMarketId"], 3);
        assert_eq!(
            encoded["cursorSubaccount"],
            "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
        );
        assert_eq!(encoded["cursorEventId"], "4846:186852294:event:none:26");
        assert_eq!(encoded["pageSize"], 5);
        assert_eq!(encoded["sort"], "DESC");
        assert!(encoded.get("subaccount").is_none());
    }

    #[rstest]
    #[case("scope")]
    #[case("both")]
    #[case("address")]
    #[case("market-id")]
    #[case("bounds")]
    #[case("timestamp")]
    #[case("zero-page-size")]
    #[case("large-page-size")]
    #[case("cursor-market")]
    #[case("cursor-subaccount")]
    #[case("cursor-time")]
    #[case("cursor-event")]
    fn invalid_hourly_unsettled_funding_request_is_rejected(#[case] mutation: &str) {
        let mut request = hourly_unsettled_funding_request();
        match mutation {
            "scope" => request.wallet = None,
            "both" => request.subaccount = request.wallet.clone(),
            "address" => request.wallet = Some("invalid".to_string()),
            "market-id" => request.market_id = Some(0),
            "bounds" => request.start_ms = Some(request.end_ms.unwrap() + 1),
            "timestamp" => request.end_ms = Some(u64::MAX),
            "zero-page-size" => request.page_size = Some(0),
            "large-page-size" => request.page_size = Some(101),
            "cursor-market" => request.cursor.as_mut().unwrap().market_id = 4,
            "cursor-subaccount" => {
                request.subaccount = Some(request.cursor.as_ref().unwrap().subaccount.clone());
                request.wallet = None;
                request.cursor.as_mut().unwrap().subaccount =
                    "0x1111111111111111111111111111111111111111".to_string();
            }
            "cursor-time" => {
                request.cursor.as_mut().unwrap().boundary_timestamp_ms =
                    request.start_ms.unwrap() - 1;
            }
            "cursor-event" => request.cursor.as_mut().unwrap().event_id.clear(),
            _ => unreachable!(),
        }

        assert!(request.validate().is_err());
    }

    fn spot_trades_request() -> DeepXSpotTradesRequest {
        DeepXSpotTradesRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            start_ms: Some(1_789_615_000_000),
            end_ms: Some(1_789_616_000_000),
            cursor: Some("next".to_string()),
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(2),
        }
    }

    #[rstest]
    fn spot_trades_query_encodes_exact_fields() {
        let request = spot_trades_request();
        request.validate().unwrap();

        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["name"], "ETH/USDC");
        assert_eq!(
            encoded["wallet"],
            "0x781ed35b167068c93dfadab41dfb680edaca4e50"
        );
        assert_eq!(encoded["start"], 1_789_615_000_000_u64);
        assert_eq!(encoded["end"], 1_789_616_000_000_u64);
        assert_eq!(encoded["cursor"], "next");
        assert_eq!(encoded["sort"], "DESC");
        assert_eq!(encoded["pageSize"], 2);
        assert!(encoded.get("pair").is_none());
    }

    #[rstest]
    fn spot_trades_query_accepts_global_scope_and_pair_identity() {
        let mut request = spot_trades_request();
        request.name = None;
        request.pair =
            Some("0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37".to_string());
        request.wallet = None;
        request.validate().unwrap();

        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["pair"], request.pair.unwrap());
        assert!(encoded.get("name").is_none());
        assert!(encoded.get("wallet").is_none());
    }

    #[rstest]
    #[case("name")]
    #[case("pair")]
    #[case("both-markets")]
    #[case("wallet")]
    #[case("bounds")]
    #[case("timestamp")]
    #[case("page-size")]
    #[case("cursor")]
    fn invalid_spot_trades_request_is_rejected(#[case] mutation: &str) {
        let mut request = spot_trades_request();
        match mutation {
            "name" => request.name = Some(" ".to_string()),
            "pair" => {
                request.name = None;
                request.pair = Some("0x1234".to_string());
            }
            "both-markets" => {
                request.pair = Some(
                    "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"
                        .to_string(),
                )
            }
            "wallet" => request.wallet = Some("invalid".to_string()),
            "bounds" => request.start_ms = Some(request.end_ms.unwrap() + 1),
            "timestamp" => request.end_ms = Some(u64::MAX),
            "page-size" => request.page_size = Some(0),
            "cursor" => request.cursor = Some(String::new()),
            _ => unreachable!(),
        }

        assert!(request.validate().is_err());
    }

    fn spot_candles_request() -> DeepXSpotCandlesRequest {
        DeepXSpotCandlesRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
            interval: DeepXSpotCandleInterval::OneMinute,
            start_ms: 1_789_616_280_000,
            end_ms: Some(1_789_619_880_000),
            limit: Some(3),
        }
    }

    #[rstest]
    fn spot_market_queries_encode_exact_fields() {
        let candles = spot_candles_request();
        candles.validate().unwrap();
        let encoded = serde_json::to_value(candles.as_query()).unwrap();
        assert_eq!(encoded["name"], "ETH/USDC");
        assert_eq!(encoded["timeFrame"], "1m");
        assert_eq!(encoded["start"], 1_789_616_280_000_u64);
        assert_eq!(encoded["end"], 1_789_619_880_000_u64);
        assert_eq!(encoded["limit"], 3);
        assert_eq!(encoded["sort"], "ASC");
        assert_eq!(encoded["tradeView"], false);
        assert!(encoded.get("pair").is_none());

        let volume = DeepXSpotVolumeRequest {
            name: None,
            pair: Some(
                "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37".to_string(),
            ),
            period: DeepXSpotVolumePeriod::OneHour,
        };
        volume.validate().unwrap();
        let encoded = serde_json::to_value(volume.as_query()).unwrap();
        assert_eq!(encoded["pair"], volume.pair.unwrap());
        assert_eq!(encoded["period"], "1h");
        assert!(encoded.get("name").is_none());

        let last_price = DeepXSpotLastPriceRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
        };
        last_price.validate().unwrap();
        assert_eq!(
            serde_json::to_value(last_price.as_query()).unwrap(),
            serde_json::json!({"name": "ETH/USDC"})
        );

        let order_book = DeepXSpotOrderBookRequest {
            name: None,
            pair: Some(
                "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37".to_string(),
            ),
            tick_size: Some(Decimal::new(1, 2)),
        };
        order_book.validate().unwrap();
        assert_eq!(
            serde_json::to_value(order_book.as_query()).unwrap(),
            serde_json::json!({
                "pair": order_book.pair.unwrap(),
                "tickSize": "0.01"
            })
        );

        let invalid_order_book = DeepXSpotOrderBookRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
            tick_size: Some(Decimal::ZERO),
        };
        assert!(invalid_order_book.validate().is_err());
    }

    #[rstest]
    fn perp_order_book_query_encodes_exact_fields() {
        let request = DeepXPerpOrderBookRequest {
            market_id: 3,
            tick_size: Some(Decimal::new(1, 2)),
        };
        request.validate().unwrap();

        assert_eq!(
            serde_json::to_value(request.as_query()).unwrap(),
            serde_json::json!({"marketId": 3, "tickSize": "0.01"})
        );
    }

    #[rstest]
    #[case(0, Some(Decimal::ONE))]
    #[case(3, Some(Decimal::ZERO))]
    #[case(3, Some(Decimal::NEGATIVE_ONE))]
    fn invalid_perp_order_book_request_is_rejected(
        #[case] market_id: u64,
        #[case] tick_size: Option<Decimal>,
    ) {
        assert!(
            DeepXPerpOrderBookRequest {
                market_id,
                tick_size,
            }
            .validate()
            .is_err()
        );
    }

    #[rstest]
    fn spot_order_requests_encode_exact_fields() {
        let pair = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
        let open = DeepXSpotOpenOrdersRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            name: None,
            pair: Some(pair.to_string()),
            order_side: Some(DeepXSpotOrderSide::Sell),
            cursor: Some("next-page".to_string()),
            sort: DeepXAccountSortOrder::Ascending,
            page_size: Some(3),
        };
        open.validate().unwrap();
        assert_eq!(
            serde_json::to_value(open.as_query()).unwrap(),
            serde_json::json!({
                "pair": pair,
                "user": open.subaccount,
                "orderSide": "Sell",
                "cursor": "next-page",
                "sort": "ASC",
                "pageSize": 3,
            })
        );

        let history = DeepXSpotHistoryOrdersRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            name: None,
            pair: None,
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: None,
        };
        history.validate().unwrap();
        assert_eq!(
            serde_json::to_value(history.as_query()).unwrap(),
            serde_json::json!({"user": history.subaccount, "sort": "DESC"})
        );
    }

    #[rstest]
    fn spot_order_by_id_query_encodes_exact_identity() {
        let request = DeepXSpotOrderByIdRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            order_id: "1789627379350".to_string(),
            order_side: DeepXSpotOrderSide::Sell,
        };
        request.validate().unwrap();

        assert_eq!(
            serde_json::to_value(request.as_query()).unwrap(),
            serde_json::json!({
                "name": "ETH/USDC",
                "user": request.subaccount,
                "oid": "1789627379350",
                "orderSide": "Sell",
            })
        );
    }

    #[rstest]
    #[case(None, None, "1789627379350")]
    #[case(
        Some("ETH/USDC"),
        Some("0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"),
        "1789627379350"
    )]
    #[case(Some("ETH/USDC"), None, "")]
    #[case(Some("ETH/USDC"), None, "bad")]
    fn invalid_spot_order_by_id_request_is_rejected(
        #[case] name: Option<&str>,
        #[case] pair: Option<&str>,
        #[case] order_id: &str,
    ) {
        assert!(
            DeepXSpotOrderByIdRequest {
                subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
                name: name.map(str::to_string),
                pair: pair.map(str::to_string),
                order_id: order_id.to_string(),
                order_side: DeepXSpotOrderSide::Sell,
            }
            .validate()
            .is_err()
        );
    }

    #[rstest]
    fn invalid_spot_order_requests_are_rejected() {
        let subaccount = "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c";
        let pair = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
        let open = |name: Option<&str>, pair: Option<&str>| DeepXSpotOpenOrdersRequest {
            subaccount: subaccount.to_string(),
            name: name.map(str::to_string),
            pair: pair.map(str::to_string),
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(3),
        };
        assert!(open(None, None).validate().is_err());
        assert!(open(Some("ETH/USDC"), Some(pair)).validate().is_err());

        let mut history = DeepXSpotHistoryOrdersRequest {
            subaccount: subaccount.to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: Some(pair.to_string()),
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(3),
        };
        assert!(history.validate().is_err());
        history.pair = None;
        history.cursor = Some(String::new());
        assert!(history.validate().is_err());
        history.cursor = None;
        history.page_size = Some(0);
        assert!(history.validate().is_err());
        history.page_size = Some(3);
        history.subaccount = "bad".to_string();
        assert!(history.validate().is_err());
    }

    #[rstest]
    fn spot_account_trade_query_encodes_exact_fields() {
        let request = DeepXSpotAccountTradesRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            order_id: Some("1789627349907".to_string()),
            order_side: Some(DeepXSpotOrderSide::Sell),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            cursor: Some("opaque&cursor".to_string()),
            sort: DeepXAccountSortOrder::Ascending,
            start_ms: Some(1_789_627_350_000),
            end_ms: Some(1_789_627_351_000),
            page_size: Some(3),
        };
        request.validate().unwrap();

        assert_eq!(
            serde_json::to_value(request.as_query()).unwrap(),
            serde_json::json!({
                "orderId": "1789627349907",
                "orderSide": "Sell",
                "name": "ETH/USDC",
                "user": request.subaccount,
                "cursor": "opaque&cursor",
                "sort": "ASC",
                "start": 1_789_627_350_000_u64,
                "end": 1_789_627_351_000_u64,
                "pageSize": 3,
            })
        );
    }

    #[rstest]
    fn invalid_spot_account_trade_filters_are_rejected() {
        let mut request = DeepXSpotAccountTradesRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            order_id: Some("1789627349907".to_string()),
            order_side: None,
            name: Some("ETH/USDC".to_string()),
            pair: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(3),
        };
        assert!(request.validate().is_err());
        request.order_side = Some(DeepXSpotOrderSide::Sell);
        request.name = None;
        assert!(request.validate().is_err());
        request.name = Some("ETH/USDC".to_string());
        request.order_id = Some("bad".to_string());
        assert!(request.validate().is_err());
        request.order_id = None;
        assert!(request.validate().is_err());
        request.order_side = None;
        request.start_ms = Some(2);
        request.end_ms = Some(1);
        assert!(request.validate().is_err());
    }

    fn spot_wallet_orders_request() -> DeepXSpotWalletOrdersRequest {
        DeepXSpotWalletOrdersRequest {
            wallet: Some("0x89bb0946046588f4f257ffc71bd3231aba13474a".to_string()),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            order_side: Some(DeepXSpotOrderSide::Buy),
            cursor: Some("next".to_string()),
            sort: DeepXAccountSortOrder::Descending,
            start_ms: Some(1_789_631_000_000),
            end_ms: Some(1_789_632_000_000),
            page_size: Some(5),
        }
    }

    fn spot_wallet_trades_request() -> DeepXSpotWalletTradesRequest {
        DeepXSpotWalletTradesRequest {
            wallet: "0x89bb0946046588f4f257ffc71bd3231aba13474a".to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            cursor: Some("next".to_string()),
            sort: DeepXAccountSortOrder::Descending,
            start_ms: Some(1_789_631_000_000),
            end_ms: Some(1_789_632_000_000),
            page_size: Some(5),
        }
    }

    #[rstest]
    fn spot_wallet_order_query_encodes_exact_fields_and_all_wallet_scope() {
        let request = spot_wallet_orders_request();
        request.validate().unwrap();
        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["address"], request.wallet.unwrap());
        assert_eq!(encoded["name"], "ETH/USDC");
        assert_eq!(encoded["orderSide"], "Buy");
        assert_eq!(encoded["cursor"], "next");
        assert_eq!(encoded["sort"], "DESC");
        assert_eq!(encoded["start"], 1_789_631_000_000_u64);
        assert_eq!(encoded["end"], 1_789_632_000_000_u64);
        assert_eq!(encoded["pageSize"], 5);
        assert!(encoded.get("pair").is_none());

        let mut all_wallets = spot_wallet_orders_request();
        all_wallets.wallet = None;
        all_wallets.name = None;
        all_wallets.cursor = None;
        all_wallets.validate().unwrap();
        let encoded = serde_json::to_value(all_wallets.as_query()).unwrap();
        assert!(encoded.get("address").is_none());
        assert!(encoded.get("name").is_none());
    }

    #[rstest]
    fn spot_wallet_trade_query_encodes_exact_fields() {
        let request = spot_wallet_trades_request();
        request.validate().unwrap();
        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["address"], request.wallet);
        assert_eq!(encoded["name"], "ETH/USDC");
        assert_eq!(encoded["cursor"], "next");
        assert_eq!(encoded["sort"], "DESC");
        assert_eq!(encoded["start"], 1_789_631_000_000_u64);
        assert_eq!(encoded["end"], 1_789_632_000_000_u64);
        assert_eq!(encoded["pageSize"], 5);
        assert!(encoded.get("pair").is_none());
    }

    #[rstest]
    #[case("address")]
    #[case("both-markets")]
    #[case("bounds")]
    #[case("timestamp")]
    #[case("page-size")]
    #[case("cursor")]
    fn invalid_spot_wallet_order_request_is_rejected(#[case] mutation: &str) {
        let mut request = spot_wallet_orders_request();
        match mutation {
            "address" => request.wallet = Some("invalid".to_string()),
            "both-markets" => {
                request.pair = Some(
                    "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"
                        .to_string(),
                )
            }
            "bounds" => request.start_ms = Some(request.end_ms.unwrap() + 1),
            "timestamp" => request.end_ms = Some(u64::MAX),
            "page-size" => request.page_size = Some(0),
            "cursor" => request.cursor = Some(String::new()),
            _ => unreachable!(),
        }
        assert!(request.validate().is_err());
    }

    #[rstest]
    #[case("address")]
    #[case("market")]
    #[case("both-markets")]
    #[case("bounds")]
    #[case("timestamp")]
    #[case("page-size")]
    #[case("cursor")]
    fn invalid_spot_wallet_trade_request_is_rejected(#[case] mutation: &str) {
        let mut request = spot_wallet_trades_request();
        match mutation {
            "address" => request.wallet = "invalid".to_string(),
            "market" => request.name = None,
            "both-markets" => {
                request.pair = Some(
                    "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"
                        .to_string(),
                )
            }
            "bounds" => request.start_ms = Some(request.end_ms.unwrap() + 1),
            "timestamp" => request.end_ms = Some(u64::MAX),
            "page-size" => request.page_size = Some(0),
            "cursor" => request.cursor = Some(String::new()),
            _ => unreachable!(),
        }
        assert!(request.validate().is_err());
    }

    #[rstest]
    fn all_subaccounts_query_encodes_exact_fields() {
        let request = DeepXAllSubaccountsRequest {
            cursor: Some("opaque&cursor".to_string()),
            page_size: Some(5),
        };
        request.validate().unwrap();

        assert_eq!(
            serde_json::to_value(request.as_query()).unwrap(),
            serde_json::json!({
                "cursor": "opaque&cursor",
                "pageSize": 5,
            })
        );
    }

    #[rstest]
    #[case(Some(""), Some(5))]
    #[case(None, Some(0))]
    fn invalid_all_subaccounts_request_is_rejected(
        #[case] cursor: Option<&str>,
        #[case] page_size: Option<u32>,
    ) {
        assert!(
            DeepXAllSubaccountsRequest {
                cursor: cursor.map(str::to_string),
                page_size,
            }
            .validate()
            .is_err()
        );
    }

    #[rstest]
    fn quota_history_query_encodes_exact_filters() {
        let request = DeepXQuotaHistoryRequest {
            wallet: "0x0a40c3efbc3b3bebdf1fb6f0f8c612eb336b25ae".to_string(),
            buyer_address: Some("0x5f5f4a75ca927ff4ef5f2debb6214ee2b5e63eb2".to_string()),
            history_type: Some(DeepXQuotaHistoryType::Purchase),
            cursor: Some("opaque&cursor".to_string()),
            limit: Some(100),
        };
        request.validate().unwrap();
        assert_eq!(
            serde_json::to_value(request.as_query()).unwrap(),
            serde_json::json!({
                "wallet": request.wallet,
                "buyerAddress": request.buyer_address,
                "historyType": "purchase",
                "cursor": "opaque&cursor",
                "limit": 100,
            })
        );
    }

    #[rstest]
    #[case("wallet")]
    #[case("buyer")]
    #[case("zero-limit")]
    #[case("large-limit")]
    #[case("cursor")]
    fn invalid_quota_history_request_is_rejected(#[case] mutation: &str) {
        let mut request = DeepXQuotaHistoryRequest {
            wallet: "0x0a40c3efbc3b3bebdf1fb6f0f8c612eb336b25ae".to_string(),
            buyer_address: None,
            history_type: None,
            cursor: None,
            limit: Some(5),
        };
        match mutation {
            "wallet" => request.wallet = "invalid".to_string(),
            "buyer" => request.buyer_address = Some("invalid".to_string()),
            "zero-limit" => request.limit = Some(0),
            "large-limit" => request.limit = Some(101),
            "cursor" => request.cursor = Some(String::new()),
            _ => unreachable!(),
        }
        assert!(request.validate().is_err());
    }

    #[rstest]
    #[case("missing")]
    #[case("empty-name")]
    #[case("bad-pair")]
    #[case("both")]
    #[case("bounds")]
    #[case("timestamp")]
    #[case("zero-limit")]
    #[case("large-limit")]
    fn invalid_spot_candle_request_is_rejected(#[case] mutation: &str) {
        let mut request = spot_candles_request();
        match mutation {
            "missing" => request.name = None,
            "empty-name" => request.name = Some(" ".to_string()),
            "bad-pair" => {
                request.name = None;
                request.pair = Some("0x1234".to_string());
            }
            "both" => {
                request.pair = Some(
                    "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"
                        .to_string(),
                );
            }
            "bounds" => request.start_ms = request.end_ms.unwrap() + 1,
            "timestamp" => request.end_ms = Some(u64::MAX),
            "zero-limit" => request.limit = Some(0),
            "large-limit" => request.limit = Some(5_001),
            _ => unreachable!(),
        }
        assert!(request.validate().is_err());
    }

    fn liquidation_records_request() -> DeepXLiquidationRecordsRequest {
        DeepXLiquidationRecordsRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            liquidation_types: vec![
                DeepXLiquidationType::LiquidatePerp,
                DeepXLiquidationType::PerpBankruptcy,
            ],
            cursor: Some("next".to_string()),
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(5),
        }
    }

    #[rstest]
    fn liquidation_records_query_encodes_closed_filters() {
        let request = liquidation_records_request();
        request.validate().unwrap();

        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["wallet"], request.wallet.unwrap());
        assert_eq!(encoded["liquidationType"], "LiquidatePerp,PerpBankruptcy");
        assert_eq!(encoded["cursor"], "next");
        assert_eq!(encoded["sort"], "DESC");
        assert_eq!(encoded["pageSize"], 5);
        assert!(encoded.get("subaccount").is_none());
    }

    #[rstest]
    #[case("scope")]
    #[case("both")]
    #[case("address")]
    #[case("page-size")]
    #[case("duplicate-filter")]
    #[case("cursor")]
    fn invalid_liquidation_records_request_is_rejected(#[case] mutation: &str) {
        let mut request = liquidation_records_request();
        match mutation {
            "scope" => request.wallet = None,
            "both" => request.subaccount = request.wallet.clone(),
            "address" => request.wallet = Some("invalid".to_string()),
            "page-size" => request.page_size = Some(0),
            "duplicate-filter" => request
                .liquidation_types
                .push(DeepXLiquidationType::LiquidatePerp),
            "cursor" => request.cursor = Some(String::new()),
            _ => unreachable!(),
        }
        assert!(request.validate().is_err());
    }

    fn perp_wallet_trades_request() -> DeepXPerpWalletTradesRequest {
        DeepXPerpWalletTradesRequest {
            wallet: "0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string(),
            market_name: None,
            market_id: Some(3),
            cursor: Some("next".to_string()),
            sort: DeepXAccountSortOrder::Descending,
            start_ms: Some(1_789_445_000_000),
            end_ms: Some(1_789_523_000_000),
            page_size: Some(5),
        }
    }

    fn perp_wallet_orders_request() -> DeepXPerpWalletOrdersRequest {
        DeepXPerpWalletOrdersRequest {
            wallet: "0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string(),
            market_name: None,
            market_id: Some(3),
            is_long: Some(true),
            cursor: Some("next".to_string()),
            sort: DeepXAccountSortOrder::Descending,
            start_ms: Some(1_789_445_000_000),
            end_ms: Some(1_789_523_000_000),
            page_size: Some(5),
        }
    }

    #[rstest]
    fn perp_wallet_orders_query_encodes_exact_fields() {
        let request = perp_wallet_orders_request();
        request.validate().unwrap();

        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["address"], request.wallet);
        assert_eq!(encoded["marketId"], 3);
        assert_eq!(encoded["isLong"], true);
        assert_eq!(encoded["cursor"], "next");
        assert_eq!(encoded["sort"], "DESC");
        assert_eq!(encoded["start"], 1_789_445_000_000_u64);
        assert_eq!(encoded["end"], 1_789_523_000_000_u64);
        assert_eq!(encoded["pageSize"], 5);
        assert!(encoded.get("name").is_none());
    }

    #[rstest]
    #[case("address")]
    #[case("market-id")]
    #[case("market-name")]
    #[case("both-markets")]
    #[case("bounds")]
    #[case("timestamp")]
    #[case("page-size")]
    #[case("cursor")]
    fn invalid_perp_wallet_orders_request_is_rejected(#[case] mutation: &str) {
        let mut request = perp_wallet_orders_request();
        match mutation {
            "address" => request.wallet = "invalid".to_string(),
            "market-id" => request.market_id = Some(0),
            "market-name" => {
                request.market_id = None;
                request.market_name = Some(" ".to_string());
            }
            "both-markets" => request.market_name = Some("ETH-USDC".to_string()),
            "bounds" => request.start_ms = Some(request.end_ms.unwrap() + 1),
            "timestamp" => request.end_ms = Some(u64::MAX),
            "page-size" => request.page_size = Some(0),
            "cursor" => request.cursor = Some(String::new()),
            _ => unreachable!(),
        }
        assert!(request.validate().is_err());
    }

    #[rstest]
    fn perp_wallet_trades_query_encodes_exact_fields() {
        let request = perp_wallet_trades_request();
        request.validate().unwrap();

        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["address"], request.wallet);
        assert_eq!(encoded["marketId"], 3);
        assert_eq!(encoded["cursor"], "next");
        assert_eq!(encoded["sort"], "DESC");
        assert_eq!(encoded["start"], 1_789_445_000_000_u64);
        assert_eq!(encoded["end"], 1_789_523_000_000_u64);
        assert_eq!(encoded["pageSize"], 5);
        assert!(encoded.get("name").is_none());
    }

    #[rstest]
    #[case("address")]
    #[case("market-id")]
    #[case("market-name")]
    #[case("both-markets")]
    #[case("bounds")]
    #[case("timestamp")]
    #[case("page-size")]
    #[case("cursor")]
    fn invalid_perp_wallet_trades_request_is_rejected(#[case] mutation: &str) {
        let mut request = perp_wallet_trades_request();
        match mutation {
            "address" => request.wallet = "invalid".to_string(),
            "market-id" => request.market_id = Some(0),
            "market-name" => {
                request.market_id = None;
                request.market_name = Some(" ".to_string());
            }
            "both-markets" => request.market_name = Some("ETH-USDC".to_string()),
            "bounds" => request.start_ms = Some(request.end_ms.unwrap() + 1),
            "timestamp" => request.end_ms = Some(u64::MAX),
            "page-size" => request.page_size = Some(0),
            "cursor" => request.cursor = Some(String::new()),
            _ => unreachable!(),
        }
        assert!(request.validate().is_err());
    }

    fn wallet_funding_fee_request() -> DeepXWalletFundingFeeRequest {
        DeepXWalletFundingFeeRequest {
            wallet: "0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string(),
            market_name: None,
            market_id: Some(3),
            start_ms: Some(1_789_498_000_000),
            end_ms: Some(1_789_523_000_000),
            cursor: Some("next".to_string()),
            page_size: Some(2),
        }
    }

    #[rstest]
    fn wallet_funding_fee_query_encodes_exact_fields() {
        let request = wallet_funding_fee_request();
        request.validate().unwrap();

        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["address"], request.wallet);
        assert_eq!(encoded["marketId"], 3);
        assert_eq!(encoded["start"], 1_789_498_000_000_u64);
        assert_eq!(encoded["end"], 1_789_523_000_000_u64);
        assert_eq!(encoded["cursor"], "next");
        assert_eq!(encoded["pageSize"], 2);
        assert!(encoded.get("name").is_none());
    }

    #[rstest]
    #[case("address")]
    #[case("market-id")]
    #[case("market-name")]
    #[case("both-markets")]
    #[case("bounds")]
    #[case("timestamp")]
    #[case("page-size")]
    #[case("cursor")]
    fn invalid_wallet_funding_fee_request_is_rejected(#[case] mutation: &str) {
        let mut request = wallet_funding_fee_request();
        match mutation {
            "address" => request.wallet = "invalid".to_string(),
            "market-id" => request.market_id = Some(0),
            "market-name" => {
                request.market_id = None;
                request.market_name = Some(" ".to_string());
            }
            "both-markets" => request.market_name = Some("ETH-USDC".to_string()),
            "bounds" => request.start_ms = Some(request.end_ms.unwrap() + 1),
            "timestamp" => request.end_ms = Some(u64::MAX),
            "page-size" => request.page_size = Some(0),
            "cursor" => request.cursor = Some(String::new()),
            _ => unreachable!(),
        }
        assert!(request.validate().is_err());
    }

    #[rstest]
    fn perp_liquidation_price_query_encodes_market_id() {
        let request = DeepXPerpLiquidationPriceRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_name: None,
            market_id: Some(3),
        };
        request.validate().unwrap();

        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["address"], request.subaccount);
        assert_eq!(encoded["marketId"], 3);
        assert!(encoded.get("name").is_none());
    }

    #[rstest]
    #[case("address")]
    #[case("missing-market")]
    #[case("zero-market")]
    #[case("empty-name")]
    #[case("both-markets")]
    fn invalid_perp_liquidation_price_request_is_rejected(#[case] mutation: &str) {
        let mut request = DeepXPerpLiquidationPriceRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_name: None,
            market_id: Some(3),
        };
        match mutation {
            "address" => request.subaccount = "invalid".to_string(),
            "missing-market" => request.market_id = None,
            "zero-market" => request.market_id = Some(0),
            "empty-name" => {
                request.market_id = None;
                request.market_name = Some(" ".to_string());
            }
            "both-markets" => request.market_name = Some("ETH-USDC".to_string()),
            _ => unreachable!(),
        }
        assert!(request.validate().is_err());
    }

    #[rstest]
    fn lending_market_query_encodes_optional_filters() {
        let request = DeepXLendingMarketRequest {
            market_id: Some(1),
            asset: Some("USDC".to_string()),
        };
        request.validate().unwrap();

        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["marketId"], 1);
        assert_eq!(encoded["asset"], "USDC");
    }

    #[rstest]
    fn lending_history_query_encodes_exact_fields() {
        let request = DeepXLendingHistoryRequest {
            market_id: Some(1),
            asset: Some("usdc".to_string()),
            interval: DeepXLendingHistoryInterval::OneHour,
            start_ms: 1_789_430_000_000,
            end_ms: Some(1_789_552_800_000),
            limit: Some(3),
            sort: DeepXAccountSortOrder::Descending,
        };
        request.validate().unwrap();

        let encoded = serde_json::to_value(request.as_query()).unwrap();

        assert_eq!(encoded["marketId"], 1);
        assert_eq!(encoded["asset"], "usdc");
        assert_eq!(encoded["timeFrame"], "1h");
        assert_eq!(encoded["start"], 1_789_430_000_000_u64);
        assert_eq!(encoded["end"], 1_789_552_800_000_u64);
        assert_eq!(encoded["limit"], 3);
        assert_eq!(encoded["sort"], "DESC");
    }

    #[rstest]
    #[case("market-id")]
    #[case("asset")]
    #[case("bounds")]
    #[case("timestamp")]
    #[case("zero-limit")]
    #[case("excessive-limit")]
    fn invalid_lending_request_is_rejected(#[case] mutation: &str) {
        let mut request = DeepXLendingHistoryRequest {
            market_id: Some(1),
            asset: Some("usdc".to_string()),
            interval: DeepXLendingHistoryInterval::OneHour,
            start_ms: 1_789_430_000_000,
            end_ms: Some(1_789_552_800_000),
            limit: Some(3),
            sort: DeepXAccountSortOrder::Descending,
        };
        match mutation {
            "market-id" => request.market_id = Some(0),
            "asset" => request.asset = Some(" ".to_string()),
            "bounds" => request.start_ms = request.end_ms.unwrap() + 1,
            "timestamp" => request.end_ms = Some(u64::MAX),
            "zero-limit" => request.limit = Some(0),
            "excessive-limit" => request.limit = Some(5_001),
            _ => unreachable!(),
        }

        assert!(request.validate().is_err());
    }

    #[rstest]
    #[case(DeepXPerpCandleInterval::OneMinute, "1m")]
    #[case(DeepXPerpCandleInterval::ThreeMinutes, "3m")]
    #[case(DeepXPerpCandleInterval::FiveMinutes, "5m")]
    #[case(DeepXPerpCandleInterval::FifteenMinutes, "15m")]
    #[case(DeepXPerpCandleInterval::ThirtyMinutes, "30m")]
    #[case(DeepXPerpCandleInterval::OneHour, "1h")]
    #[case(DeepXPerpCandleInterval::TwoHours, "2h")]
    #[case(DeepXPerpCandleInterval::FourHours, "4h")]
    #[case(DeepXPerpCandleInterval::EightHours, "8h")]
    #[case(DeepXPerpCandleInterval::TwelveHours, "12h")]
    #[case(DeepXPerpCandleInterval::OneDay, "1d")]
    #[case(DeepXPerpCandleInterval::ThreeDays, "3d")]
    #[case(DeepXPerpCandleInterval::OneWeek, "1w")]
    #[case(DeepXPerpCandleInterval::OneMonth, "1M")]
    fn candle_intervals_encode_exact_openapi_values(
        #[case] interval: DeepXPerpCandleInterval,
        #[case] expected: &str,
    ) {
        let query = DeepXPerpCandlesQuery::new(3, interval, 1, Some(2), Some(3));

        let encoded = serde_json::to_value(query).unwrap();

        assert_eq!(encoded["timeFrame"], expected);
        assert_eq!(encoded["sort"], "ASC");
        assert_eq!(encoded["tradeView"], false);
    }
}
