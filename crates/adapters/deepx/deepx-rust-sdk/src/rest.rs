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

//! Read-only DeepX REST transport.

pub mod models;
pub mod ownership;
pub mod status;

mod account;
mod account_trades;
mod balance_change_pages;
mod candle_history;
mod directory_pages;
mod funding_fees;
mod hourly_funding;
mod liquidation;
mod liquidation_pages;
mod market;
mod market_history;
mod open_orders;
mod order_history;
mod perp_trade_history;
mod position_pages;
mod quota;
mod quota_pages;
mod spot_trade_history;
mod wallet_pages;
mod wallet_positions;
mod wallet_snapshot;

use std::{collections::HashSet, time::Duration};

pub use account::{
    DeepXPerpWalletOrderContinuity, DeepXSpotWalletOrderContinuity, DeepXSpotWalletTradeContinuity,
    DeepXSubaccountDirectoryContinuity,
};
pub use account_trades::{
    collect_perp_account_trade_pages, collect_perp_account_trade_pages_raw,
    collect_spot_account_trade_pages, collect_spot_account_trade_pages_raw,
};
pub use balance_change_pages::{collect_balance_change_pages, collect_balance_change_pages_raw};
pub use candle_history::{collect_perp_candle_history, collect_spot_candle_history};
pub use directory_pages::{collect_all_subaccount_pages, collect_all_subaccount_pages_raw};
pub use funding_fees::{
    collect_perp_funding_fee_pages, collect_perp_funding_fee_pages_raw,
    collect_wallet_funding_fee_pages, collect_wallet_funding_fee_pages_raw,
};
pub use hourly_funding::collect_hourly_unsettled_funding_pages;
pub use liquidation::{DeepXLiquidationExecutionRoute, DeepXLiquidationRecordContinuity};
pub use liquidation_pages::{
    collect_liquidation_adl_record_pages_raw, collect_liquidation_execution_record_pages_raw,
    collect_liquidation_record_pages, collect_liquidation_record_pages_raw,
};
pub use market::DeepXSpotTradeContinuity;
pub use market_history::{
    collect_perp_funding_rates_history_limited, collect_perp_long_short_ratios_history_limited,
    collect_perp_open_interest_history,
};
pub use open_orders::{
    collect_perp_open_order_pages, collect_perp_open_order_pages_raw, collect_spot_open_order_pages,
};
pub use order_history::{
    collect_perp_history_order_pages, collect_perp_history_order_pages_raw,
    collect_perp_position_order_pages, collect_perp_position_order_pages_raw,
    collect_spot_history_order_pages, collect_spot_history_order_pages_raw,
};
pub use perp_trade_history::collect_perp_trade_history;
pub use position_pages::{collect_perp_position_pages, collect_perp_position_pages_raw};
pub use quota::DeepXQuotaHistoryContinuity;
pub use quota_pages::collect_quota_history_pages;
use reqwest::{Client, Proxy, StatusCode, redirect::Policy};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json, value::RawValue};
pub use spot_trade_history::{collect_spot_trade_pages, collect_spot_trades_limited};
use thiserror::Error;
pub use wallet_pages::{
    collect_perp_wallet_order_pages, collect_perp_wallet_trade_pages_for_subaccount,
    collect_spot_wallet_order_pages, collect_spot_wallet_trade_pages,
};
pub use wallet_positions::DeepXWalletPerpPositionContinuity;
pub use wallet_snapshot::DeepXWalletCollectionValidation;

use crate::{
    identity::decode_account_id20,
    pagination::validate_cursor_page,
    parse::parse_canonical_order_id,
    query::{
        DeepXAccountSortOrder, DeepXBalanceChangesRequest, DeepXPerpAccountTradesRequest,
        DeepXPerpFundingFeeRequest, DeepXPerpHistoryOrdersRequest, DeepXPerpOpenOrdersRequest,
        DeepXPerpPositionOrdersRequest, DeepXPerpPositionsRequest, DeepXPerpWalletTradesRequest,
        DeepXSpotAccountTradesRequest, DeepXSpotHistoryOrdersRequest, DeepXSpotOpenOrdersRequest,
        DeepXSpotOrderByIdRequest, DeepXSpotOrderSide, DeepXSpotWalletTradesRequest,
        DeepXWalletFundingFeeRequest,
    },
    rest::models::{
        DeepXBalanceChangeRecord, DeepXPerpAccountTradeRecord, DeepXPerpFundingFeeRecord,
        DeepXPerpOrderRecord, DeepXPerpPositionRecord, DeepXSpotAccountTradeRecord,
        DeepXSpotOrderRecord, DeepXWalletSubaccounts,
    },
    symbol::decode_spot_pair_identity,
    transport::{install_tls_provider, read_bounded_body},
};

const MAX_ERROR_BODY_CHARS: usize = 1_024;

/// Wallet subaccount directory endpoint.
pub const WALLET_SUBACCOUNTS_PATH: &str = "/internal/v1/account/subaccounts";

const PERP_ORDER_BY_ID_PATH: &str = "/internal/v1/account/perp/order-by-id";
const PERP_ORDER_BY_TX_PATH: &str = "/internal/v1/account/perp/order-by-tx";
const PERP_HISTORY_ORDERS_PATH: &str = "/internal/v1/account/perp/history-orders";
const PERP_ACCOUNT_TRADES_PATH: &str = "/internal/v1/account/perp/trades";
const PERP_FUNDING_FEE_PATH: &str = "/internal/v1/account/funding-fee";
const WALLET_FUNDING_FEE_PATH: &str = "/internal/v1/account/funding-fee-by-wallet";
const PERP_POSITIONS_PATH: &str = "/internal/v1/account/position";
const BALANCE_CHANGES_PATH: &str = "/internal/v1/account/balance-changes";
const PERP_OPEN_ORDERS_PATH: &str = "/internal/v1/account/perp/open-orders";
const PERP_POSITION_ORDERS_PATH: &str = "/internal/v1/account/perp/position-orders";
const SPOT_HISTORY_ORDERS_PATH: &str = "/internal/v1/account/spot/history-orders";
const SPOT_ACCOUNT_TRADES_PATH: &str = "/internal/v1/account/spot/trades";
const SPOT_OPEN_ORDERS_PATH: &str = "/internal/v1/account/spot/open-orders";
const SPOT_ORDER_BY_ID_PATH: &str = "/internal/v1/account/spot/order-by-id";
const SPOT_ORDER_BY_TX_PATH: &str = "/internal/v1/account/spot/order-by-tx";

/// Errors returned by one DeepX REST request.
#[derive(Debug, Error)]
pub enum DeepXRestError {
    /// Transport construction or request failed.
    #[error("DeepX REST transport error: {0}")]
    Transport(String),
    /// The venue returned a non-success HTTP status.
    #[error("DeepX REST HTTP {status}: {message}")]
    Http { status: u16, message: String },
    /// The venue returned invalid JSON.
    #[error("failed to decode DeepX REST response: {0}")]
    Decode(#[from] serde_json::Error),
    /// A successful HTTP response contained a DeepX API failure.
    #[error("DeepX API {code}: {message}")]
    Api { code: DeepXApiCode, message: String },
    /// A transaction-status response violates its schema or requested hash.
    #[error("invalid DeepX REST transaction status: {message}")]
    InvalidTransactionStatus { message: String },
    /// Request parameters violate a DeepX endpoint requirement.
    #[error("invalid DeepX REST request: {0}")]
    InvalidRequest(String),
    /// A page budget must be positive.
    #[error("DeepX HTTP pagination max_pages must be greater than zero")]
    InvalidPaginationLimit,
    /// A cursor paginator exhausted its local page budget.
    #[error("DeepX HTTP pagination exceeded its limit of {max_pages} pages")]
    PaginationLimitExceeded { max_pages: usize },
    /// A response supplied a cursor without records and made no progress.
    #[error("DeepX HTTP pagination returned an empty page with cursor '{cursor}'")]
    PaginationNoProgress { cursor: String },
    /// A page advertised a continuation without a cursor.
    #[error("DeepX HTTP {endpoint} pagination hasNext is true without a non-empty nextCursor")]
    MissingPaginationCursor { endpoint: &'static str },
    /// A page repeated an already observed cursor.
    #[error("DeepX HTTP pagination repeated cursor '{cursor}'")]
    RepeatedPaginationCursor { cursor: String },
    /// A decoded account directory contains malformed or repeated identities.
    #[error("invalid DeepX REST {endpoint} response: {message}")]
    InvalidAccountResponse {
        endpoint: &'static str,
        message: String,
    },
    /// A history page exceeds its requested size or violates a record invariant.
    #[error("invalid DeepX REST {endpoint} response: {message}")]
    InvalidHistoryResponse {
        endpoint: &'static str,
        message: String,
    },
    /// A history response identified another deployment market.
    #[error(
        "DeepX REST {endpoint} response market ID mismatch: expected {expected}, received {received}"
    )]
    ResponseMarketMismatch {
        endpoint: &'static str,
        expected: u64,
        received: u64,
    },
    /// The request path could escape the configured base URL.
    #[error("invalid DeepX REST path '{0}'")]
    InvalidPath(String),
    /// The configured endpoint is not an HTTP base URL.
    #[error("invalid DeepX REST base URL '{0}'")]
    InvalidBaseUrl(String),
}

/// Success and failure codes in DeepX REST response envelopes.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DeepXApiCode {
    Api(u16),
    Pallet(String),
}

impl DeepXApiCode {
    /// Returns whether this is the venue success code.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Api(200))
    }
}

impl std::fmt::Display for DeepXApiCode {
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
    pub code: DeepXApiCode,
    /// Human-readable venue response message.
    pub msg: String,
    /// Response payload.
    pub data: T,
    /// Venue failure indicator.
    pub fail: bool,
}

type ApiEnvelope = DeepXApiResponse<Box<RawValue>>;

/// Performs one unauthenticated DeepX REST read at a time.
#[derive(Clone, Debug)]
pub struct DeepXRestTransport {
    client: Client,
    transaction_client: Client,
    timeout_secs: Option<u64>,
}

/// HTTP status and exact response bytes for a one-shot transaction request.
#[derive(Debug)]
pub struct DeepXRestResponse {
    pub status: StatusCode,
    pub body: Vec<u8>,
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

impl DeepXRawAccountPage {
    /// Decodes every record into a caller-selected type without changing pagination metadata.
    ///
    /// # Errors
    ///
    /// Returns a decoding error if any record does not match the selected type.
    pub fn decode<T: DeserializeOwned>(self) -> Result<DeepXAccountPage<T>, DeepXRestError> {
        let items = self
            .items
            .into_iter()
            .map(|item| serde_json::from_str(item.get()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(DeepXAccountPage {
            items,
            next_cursor: self.next_cursor,
            has_next: self.has_next,
        })
    }
}

fn decode_account_history_page<T: DeserializeOwned>(
    page: DeepXRawAccountPage,
    endpoint: &'static str,
    page_size: Option<u32>,
) -> Result<DeepXAccountPage<T>, DeepXRestError> {
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if page_size.is_some_and(|limit| page.items.len() > limit as usize) {
        return Err(invalid("page exceeds requested page size".to_string()));
    }
    let mut items = Vec::with_capacity(page.items.len());
    for (index, raw) in page.items.into_iter().enumerate() {
        let item = serde_json::from_str(raw.get())
            .map_err(|e| invalid(format!("item {index} failed decoding: {e}")))?;
        items.push(item);
    }
    Ok(DeepXAccountPage {
        items,
        next_cursor: page.next_cursor,
        has_next: page.has_next,
    })
}

fn validate_balance_changes_page(
    page: &DeepXAccountPage<DeepXBalanceChangeRecord>,
    request: &DeepXBalanceChangesRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "balance changes";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut ids = HashSet::new();
    for (index, record) in page.items.iter().enumerate() {
        validate_balance_change_record(index, record, request)?;
        if !ids.insert(record.id) {
            return Err(invalid(format!("item {index} repeats record ID")));
        }
    }
    Ok(())
}

/// Tracks balance-change record identities across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXBalanceChangeContinuity {
    ids: HashSet<u64>,
}

impl DeepXBalanceChangeContinuity {
    /// Checks one validated balance-change page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error when a record ID repeats.
    pub fn observe_page(
        &mut self,
        page: &DeepXAccountPage<DeepXBalanceChangeRecord>,
    ) -> Result<(), DeepXRestError> {
        for (index, record) in page.items.iter().enumerate() {
            if !self.ids.insert(record.id) {
                return Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint: "balance changes",
                    message: format!("item {index} repeats record ID"),
                });
            }
        }
        Ok(())
    }
}

fn validate_balance_change_record(
    index: usize,
    record: &DeepXBalanceChangeRecord,
    request: &DeepXBalanceChangesRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "balance changes";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if record.id == 0 || record.height == 0 || record.asset.trim().is_empty() {
        return Err(invalid(format!(
            "item {index} has an invalid identity, height, or asset"
        )));
    }
    let valid_time = i64::try_from(record.time)
        .ok()
        .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
        .is_some();
    if !valid_time
        || request.start_ms.is_some_and(|start| record.time < start)
        || request.end_ms.is_some_and(|end| record.time > end)
    {
        return Err(invalid(format!(
            "item {index} has an invalid or out-of-bounds timestamp"
        )));
    }
    if !request.change_types.is_empty() && !request.change_types.contains(&record.change_type) {
        return Err(invalid(format!(
            "item {index} does not match the requested change types"
        )));
    }
    if record.tx_hash.is_some() != record.tx_hash_type.is_some()
        || record
            .tx_hash
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        || record
            .tx_hash_type
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
    {
        return Err(invalid(format!(
            "item {index} has incomplete transaction identity"
        )));
    }
    for (role, address) in [("source", &record.from), ("destination", &record.to)] {
        if let Some(address) = address
            && !is_account_address(address)
        {
            return Err(invalid(format!(
                "item {index} has an invalid {role} address"
            )));
        }
    }
    if let Some(position) = &record.position
        && (position.id == 0
            || position.market_id == 0
            || !is_account_address(&position.owner)
            || request
                .subaccount
                .as_deref()
                .is_some_and(|owner| !position.owner.eq_ignore_ascii_case(owner)))
    {
        return Err(invalid(format!(
            "item {index} has an invalid or foreign position identity"
        )));
    }
    Ok(())
}

fn validate_funding_fee_page(
    page: &DeepXAccountPage<DeepXPerpFundingFeeRecord>,
    endpoint: &'static str,
    expected_owner: Option<&str>,
    expected_market_id: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
) -> Result<(), DeepXRestError> {
    let mut event_ids = HashSet::new();
    for (index, fee) in page.items.iter().enumerate() {
        validate_funding_fee_record(
            endpoint,
            index,
            fee,
            expected_owner,
            expected_market_id,
            start_ms,
            end_ms,
        )?;
        if !event_ids.insert((fee.height, fee.event_idx)) {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint,
                message: format!("item {index} repeats event identity"),
            });
        }
    }
    Ok(())
}

/// Tracks funding-fee event identities across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXFundingFeeContinuity {
    event_ids: HashSet<(u64, u64)>,
}

impl DeepXFundingFeeContinuity {
    /// Checks one validated funding-fee page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error when an event identity repeats.
    pub fn observe_page(
        &mut self,
        endpoint: &'static str,
        page: &DeepXAccountPage<DeepXPerpFundingFeeRecord>,
    ) -> Result<(), DeepXRestError> {
        for (index, fee) in page.items.iter().enumerate() {
            if !self.event_ids.insert((fee.height, fee.event_idx)) {
                return Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint,
                    message: format!("item {index} repeats event identity"),
                });
            }
        }
        Ok(())
    }
}

fn validate_funding_fee_record(
    endpoint: &'static str,
    index: usize,
    fee: &DeepXPerpFundingFeeRecord,
    expected_owner: Option<&str>,
    expected_market_id: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
) -> Result<(), DeepXRestError> {
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if !is_account_address(&fee.owner)
        || expected_owner.is_some_and(|owner| !fee.owner.eq_ignore_ascii_case(owner))
    {
        return Err(invalid(format!(
            "item {index} has an invalid or unexpected owner"
        )));
    }
    if fee.market == 0 || expected_market_id.is_some_and(|market| fee.market != market) {
        return Err(invalid(format!("item {index} has an unexpected market ID")));
    }
    if fee.position_size <= Decimal::ZERO || fee.height == 0 {
        return Err(invalid(format!(
            "item {index} has invalid financial or event values"
        )));
    }
    let created = fee
        .created_at
        .parse::<jiff::Timestamp>()
        .map_err(|e| invalid(format!("item {index} has invalid createdAt: {e}")))?;
    let created_ms = u64::try_from(created.as_millisecond())
        .map_err(|_| invalid(format!("item {index} timestamp is before the Unix epoch")))?;
    if start_ms.is_some_and(|start| created_ms < start)
        || end_ms.is_some_and(|end| created_ms > end)
    {
        return Err(invalid(format!(
            "item {index} is outside the requested time range"
        )));
    }
    Ok(())
}

fn validate_perp_positions_page(
    page: &DeepXAccountPage<DeepXPerpPositionRecord>,
    request: &DeepXPerpPositionsRequest,
) -> Result<(), DeepXRestError> {
    let mut position_ids = HashSet::new();
    for (index, position) in page.items.iter().enumerate() {
        validate_perp_position_record(index, position, request)?;
        if !position_ids.insert(position.id) {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp positions",
                message: format!("item {index} repeats position ID"),
            });
        }
    }
    Ok(())
}

/// Tracks perpetual position identities across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXPerpPositionContinuity {
    position_ids: HashSet<u64>,
}

impl DeepXPerpPositionContinuity {
    /// Checks one validated position page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error when a position ID repeats.
    pub fn observe_page(
        &mut self,
        page: &DeepXAccountPage<DeepXPerpPositionRecord>,
    ) -> Result<(), DeepXRestError> {
        for (index, position) in page.items.iter().enumerate() {
            if !self.position_ids.insert(position.id) {
                return Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint: "perp positions",
                    message: format!("item {index} repeats position ID"),
                });
            }
        }
        Ok(())
    }
}

/// Validates one perpetual position against its request scope and lifecycle invariants.
///
/// # Errors
///
/// Returns an invalid-history error for a mismatched scope, financial value, or timestamp.
pub fn validate_perp_position_record(
    index: usize,
    position: &DeepXPerpPositionRecord,
    request: &DeepXPerpPositionsRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "perp positions";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if !position.owner.eq_ignore_ascii_case(&request.subaccount) {
        return Err(invalid(format!(
            "item {index} belongs to another subaccount"
        )));
    }
    if position.market_id == 0
        || request
            .market_id
            .is_some_and(|expected| position.market_id != expected)
    {
        return Err(invalid(format!("item {index} has an unexpected market ID")));
    }
    if position.base_asset_amount <= Decimal::ZERO
        || position.entry_price <= Decimal::ZERO
        || position.leverage <= Decimal::ZERO
        || position.take_profit.is_sign_negative()
        || position.stop_loss.is_sign_negative()
        || position.last_settle_price <= Decimal::ZERO
        || position
            .close_price
            .is_some_and(|value| value <= Decimal::ZERO)
        || position
            .liquidate_price
            .is_some_and(|value| value <= Decimal::ZERO)
    {
        return Err(invalid(format!(
            "item {index} has invalid financial values"
        )));
    }
    if position.status.trim().is_empty()
        || request.only_closed == Some(true) && position.status != "Closed"
    {
        return Err(invalid(format!(
            "item {index} does not match the requested lifecycle filter"
        )));
    }
    let timestamp = |field: &str, value: &str| {
        value
            .parse::<jiff::Timestamp>()
            .map_err(|e| invalid(format!("item {index} has invalid {field}: {e}")))
    };
    let opened = timestamp("openTime", &position.open_time)?;
    let created = timestamp("createdAt", &position.created_at)?;
    let updated = timestamp("updatedAt", &position.updated_at)?;
    if updated < created {
        return Err(invalid(format!(
            "item {index} update timestamp precedes creation"
        )));
    }
    if let Some(closed_at) = &position.close_time {
        let closed = timestamp("closeTime", closed_at)?;
        if closed < opened {
            return Err(invalid(format!(
                "item {index} close timestamp precedes opening"
            )));
        }
    }
    Ok(())
}

fn validate_perp_account_trades_page(
    page: &DeepXAccountPage<DeepXPerpAccountTradeRecord>,
    request: &DeepXPerpAccountTradesRequest,
) -> Result<(), DeepXRestError> {
    let mut trade_ids = HashSet::new();
    let mut previous = None;
    for (index, trade) in page.items.iter().enumerate() {
        validate_perp_account_trade_record(index, trade, request, &mut previous)?;
        if !trade_ids.insert(trade.id) {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp account trades",
                message: format!("item {index} repeats trade ID"),
            });
        }
    }
    Ok(())
}

/// Tracks perpetual account trade IDs and time order across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXPerpAccountTradeContinuity {
    trade_ids: HashSet<u64>,
    previous: Option<jiff::Timestamp>,
}

impl DeepXPerpAccountTradeContinuity {
    /// Checks one validated perpetual account-trade page against prior pages.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid trade, repeated ID, or inconsistent time order.
    pub fn observe_page(
        &mut self,
        page: &DeepXAccountPage<DeepXPerpAccountTradeRecord>,
        request: &DeepXPerpAccountTradesRequest,
    ) -> Result<(), DeepXRestError> {
        for (index, trade) in page.items.iter().enumerate() {
            validate_perp_account_trade_record(index, trade, request, &mut self.previous)?;
            if !self.trade_ids.insert(trade.id) {
                return Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint: "perp account trades",
                    message: format!("item {index} repeats trade ID"),
                });
            }
        }
        Ok(())
    }
}

/// Validates one perpetual account trade against its request and the preceding trade time.
///
/// On success, `previous` becomes the trade creation timestamp.
///
/// # Errors
///
/// Returns an invalid-history error for a malformed record, filter mismatch, or time-order violation.
pub fn validate_perp_account_trade_record(
    index: usize,
    trade: &DeepXPerpAccountTradeRecord,
    request: &DeepXPerpAccountTradesRequest,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<(), DeepXRestError> {
    let endpoint = "perp account trades";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if trade.id == 0 {
        return Err(invalid(format!("item {index} has an invalid trade ID")));
    }
    if trade.market_id == 0
        || request
            .market_id
            .is_some_and(|market_id| trade.market_id != market_id)
    {
        return Err(invalid(format!("item {index} has an unexpected market ID")));
    }
    if parse_canonical_order_id(&trade.order_id).is_none() {
        return Err(invalid(format!(
            "item {index} has an invalid decimal order ID"
        )));
    }
    if request
        .order_id
        .as_ref()
        .is_some_and(|order_id| trade.order_id != *order_id)
        || request
            .is_long
            .is_some_and(|is_long| trade.is_long != is_long)
    {
        return Err(invalid(format!(
            "item {index} does not match the requested order filter"
        )));
    }
    if trade.price <= Decimal::ZERO
        || trade.size <= Decimal::ZERO
        || trade.leverage <= Decimal::ZERO
    {
        return Err(invalid(format!(
            "item {index} has invalid financial values"
        )));
    }
    if trade.taker.trim().is_empty() || trade.filled_direction.trim().is_empty() {
        return Err(invalid(format!(
            "item {index} has an empty venue enum value"
        )));
    }
    let created = trade
        .created_at
        .parse::<jiff::Timestamp>()
        .map_err(|e| invalid(format!("item {index} has invalid createdAt: {e}")))?;
    let created_ms = u64::try_from(created.as_millisecond())
        .map_err(|_| invalid(format!("item {index} timestamp is before the Unix epoch")))?;
    if request.start_ms.is_some_and(|start| created_ms < start)
        || request.end_ms.is_some_and(|end| created_ms > end)
    {
        return Err(invalid(format!(
            "item {index} is outside the requested time range"
        )));
    }
    let out_of_order = previous
        .as_ref()
        .is_some_and(|previous| match request.sort {
            DeepXAccountSortOrder::Ascending => created < *previous,
            DeepXAccountSortOrder::Descending => created > *previous,
        });
    if out_of_order {
        return Err(invalid(format!(
            "item {index} violates the requested time order"
        )));
    }
    *previous = Some(created);
    Ok(())
}

/// Validates a wallet-grouped perpetual trade against its group and time filters.
///
/// On success, `previous` becomes the trade creation timestamp.
///
/// # Errors
///
/// Returns an invalid-history error for a malformed trade, filter mismatch, or time-order violation.
pub fn validate_perp_wallet_trade_record(
    index: usize,
    trade: &DeepXPerpAccountTradeRecord,
    request: &DeepXPerpWalletTradesRequest,
    group_market_id: u64,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<(), DeepXRestError> {
    let endpoint = "perp wallet trades";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if trade.id == 0 || trade.market_id != group_market_id {
        return Err(invalid(format!(
            "item {index} has an invalid trade ID or mismatched market ID"
        )));
    }
    if parse_canonical_order_id(&trade.order_id).is_none() {
        return Err(invalid(format!(
            "item {index} has an invalid decimal order ID"
        )));
    }
    if trade.price <= Decimal::ZERO || trade.size < Decimal::ZERO || trade.leverage <= Decimal::ZERO
    {
        return Err(invalid(format!(
            "item {index} has invalid financial values"
        )));
    }
    if trade.taker.trim().is_empty() || trade.filled_direction.trim().is_empty() {
        return Err(invalid(format!(
            "item {index} has an empty venue enum value"
        )));
    }
    let timestamp = trade
        .created_at
        .parse::<jiff::Timestamp>()
        .map_err(|e| invalid(format!("item {index} has invalid createdAt: {e}")))?;
    let timestamp_ms = u64::try_from(timestamp.as_millisecond()).map_err(|_| {
        invalid(format!(
            "item {index} execution timestamp is before the Unix epoch"
        ))
    })?;
    if request.start_ms.is_some_and(|start| timestamp_ms < start)
        || request.end_ms.is_some_and(|end| timestamp_ms > end)
    {
        return Err(invalid(format!(
            "item {index} is outside the requested time range"
        )));
    }
    let out_of_order = previous
        .as_ref()
        .is_some_and(|previous| match request.sort {
            DeepXAccountSortOrder::Ascending => timestamp < *previous,
            DeepXAccountSortOrder::Descending => timestamp > *previous,
        });
    if out_of_order {
        return Err(invalid(format!(
            "item {index} violates the requested time order"
        )));
    }
    *previous = Some(timestamp);
    Ok(())
}

/// Tracks one perpetual wallet-trade group's IDs and time order across pages.
#[derive(Debug, Default)]
pub struct DeepXPerpWalletTradeContinuity {
    trade_ids: HashSet<u64>,
    previous: Option<jiff::Timestamp>,
}

impl DeepXPerpWalletTradeContinuity {
    /// Checks one validated group page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid trade, repeated ID, or inconsistent time order.
    pub fn observe_page(
        &mut self,
        page: &DeepXAccountPage<DeepXPerpAccountTradeRecord>,
        request: &DeepXPerpWalletTradesRequest,
        market_id: u64,
    ) -> Result<(), DeepXRestError> {
        for (index, trade) in page.items.iter().enumerate() {
            validate_perp_wallet_trade_record(
                index,
                trade,
                request,
                market_id,
                &mut self.previous,
            )?;
            if !self.trade_ids.insert(trade.id) {
                return Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint: "perp wallet trades",
                    message: format!("item {index} repeats trade ID"),
                });
            }
        }
        Ok(())
    }
}

fn validate_spot_account_trades_page(
    page: &DeepXAccountPage<DeepXSpotAccountTradeRecord>,
    request: &DeepXSpotAccountTradesRequest,
) -> Result<(), DeepXRestError> {
    let mut trade_ids = HashSet::new();
    let mut previous = None;
    for (index, trade) in page.items.iter().enumerate() {
        validate_spot_account_trade_record(index, trade, request, &mut previous)?;
        if !trade_ids.insert(trade.id) {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot account trades",
                message: format!("item {index} repeats trade ID"),
            });
        }
    }
    Ok(())
}

/// Tracks Spot account trade IDs and time order across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXSpotAccountTradeContinuity {
    trade_ids: HashSet<u64>,
    previous: Option<jiff::Timestamp>,
}

impl DeepXSpotAccountTradeContinuity {
    /// Checks one validated Spot account-trade page against prior pages.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid trade, repeated ID, or inconsistent time order.
    pub fn observe_page(
        &mut self,
        page: &DeepXAccountPage<DeepXSpotAccountTradeRecord>,
        request: &DeepXSpotAccountTradesRequest,
    ) -> Result<(), DeepXRestError> {
        for (index, trade) in page.items.iter().enumerate() {
            validate_spot_account_trade_record(index, trade, request, &mut self.previous)?;
            if !self.trade_ids.insert(trade.id) {
                return Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint: "spot account trades",
                    message: format!("item {index} repeats trade ID"),
                });
            }
        }
        Ok(())
    }
}

/// Validates one Spot account trade against its request and the preceding trade time.
///
/// On success, `previous` becomes the trade creation timestamp.
///
/// # Errors
///
/// Returns an invalid-history error for a malformed record, filter mismatch, or time-order violation.
pub fn validate_spot_account_trade_record(
    index: usize,
    trade: &DeepXSpotAccountTradeRecord,
    request: &DeepXSpotAccountTradesRequest,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<(), DeepXRestError> {
    let endpoint = "spot account trades";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if trade.id == 0 {
        return Err(invalid(format!("item {index} has an invalid trade ID")));
    }
    if parse_canonical_order_id(&trade.order_id).is_none() {
        return Err(invalid(format!(
            "item {index} has an invalid decimal order ID"
        )));
    }
    if request
        .order_id
        .as_ref()
        .is_some_and(|order_id| trade.order_id != *order_id)
        || !matches!(trade.order_side.as_str(), "Buy" | "Sell")
        || request
            .order_side
            .is_some_and(|side| trade.order_side != side.as_str())
    {
        return Err(invalid(format!(
            "item {index} does not match the requested order filter"
        )));
    }
    let pair = decode_spot_pair_identity(&trade.pair)
        .ok_or_else(|| invalid(format!("item {index} has an invalid pair identity")))?;
    if request
        .pair
        .as_deref()
        .and_then(decode_spot_pair_identity)
        .is_some_and(|expected| pair != expected)
        || trade.pair_name.trim().is_empty()
        || request
            .name
            .as_deref()
            .is_some_and(|expected| trade.pair_name != expected)
    {
        return Err(invalid(format!(
            "item {index} belongs to another Spot market"
        )));
    }
    if trade.price <= Decimal::ZERO
        || trade.base_amount <= Decimal::ZERO
        || trade.quote_amount <= Decimal::ZERO
        || trade.taker.trim().is_empty()
    {
        return Err(invalid(format!(
            "item {index} has invalid financial or taker values"
        )));
    }
    let created = trade
        .created_at
        .parse::<jiff::Timestamp>()
        .map_err(|e| invalid(format!("item {index} has invalid createdAt: {e}")))?;
    let created_ms = u64::try_from(created.as_millisecond())
        .map_err(|_| invalid(format!("item {index} timestamp is before the Unix epoch")))?;
    if request.start_ms.is_some_and(|start| created_ms < start)
        || request.end_ms.is_some_and(|end| created_ms > end)
    {
        return Err(invalid(format!(
            "item {index} is outside the requested time range"
        )));
    }
    let out_of_order = previous
        .as_ref()
        .is_some_and(|previous| match request.sort {
            DeepXAccountSortOrder::Ascending => created < *previous,
            DeepXAccountSortOrder::Descending => created > *previous,
        });
    if out_of_order {
        return Err(invalid(format!(
            "item {index} violates the requested time order"
        )));
    }
    *previous = Some(created);
    Ok(())
}

/// Validates a wallet-grouped Spot trade against its market and time filters.
///
/// Returns the trade creation timestamp for group sequence validation.
///
/// # Errors
///
/// Returns an invalid-history error for a malformed trade, filter mismatch, or time-order violation.
pub fn validate_spot_wallet_trade_record(
    index: usize,
    trade: &DeepXSpotAccountTradeRecord,
    request: &DeepXSpotWalletTradesRequest,
    group_name: &str,
    group_pair: [u8; 32],
    previous: Option<&jiff::Timestamp>,
) -> Result<jiff::Timestamp, DeepXRestError> {
    let endpoint = "spot wallet trades";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if trade.id == 0 {
        return Err(invalid(format!("item {index} has an invalid trade ID")));
    }
    if parse_canonical_order_id(&trade.order_id).is_none() {
        return Err(invalid(format!(
            "item {index} has an invalid decimal order ID"
        )));
    }
    if !matches!(trade.order_side.as_str(), "Buy" | "Sell") {
        return Err(invalid(format!("item {index} has an invalid order side")));
    }
    let pair = decode_spot_pair_identity(&trade.pair)
        .ok_or_else(|| invalid(format!("item {index} has an invalid pair identity")))?;
    if pair != group_pair || trade.pair_name != group_name {
        return Err(invalid(format!(
            "item {index} belongs to another Spot market"
        )));
    }
    if trade.price <= Decimal::ZERO
        || trade.base_amount <= Decimal::ZERO
        || trade.quote_amount <= Decimal::ZERO
        || trade.taker.trim().is_empty()
    {
        return Err(invalid(format!(
            "item {index} has invalid financial or taker values"
        )));
    }
    let created = trade
        .created_at
        .parse::<jiff::Timestamp>()
        .map_err(|e| invalid(format!("item {index} has invalid createdAt: {e}")))?;
    let created_ms = u64::try_from(created.as_millisecond())
        .map_err(|_| invalid(format!("item {index} timestamp is before the Unix epoch")))?;
    if request.start_ms.is_some_and(|start| created_ms < start)
        || request.end_ms.is_some_and(|end| created_ms > end)
    {
        return Err(invalid(format!(
            "item {index} is outside the requested time range"
        )));
    }
    let out_of_order = previous.is_some_and(|previous| match request.sort {
        DeepXAccountSortOrder::Ascending => created < *previous,
        DeepXAccountSortOrder::Descending => created > *previous,
    });
    if out_of_order {
        return Err(invalid(format!(
            "item {index} violates its group time order"
        )));
    }
    Ok(created)
}

/// Validates one Spot order against its account, market, side, and financial invariants.
///
/// Returns the venue creation timestamp for sequence validation.
///
/// # Errors
///
/// Returns an invalid-history error for a malformed or out-of-scope order.
pub fn validate_spot_order_record(
    endpoint: &'static str,
    index: usize,
    order: &DeepXSpotOrderRecord,
    requested_owner: &str,
    requested_name: Option<&str>,
    requested_pair: Option<&str>,
    requested_side: Option<DeepXSpotOrderSide>,
) -> Result<jiff::Timestamp, DeepXRestError> {
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if !is_account_address(&order.maker) || !order.maker.eq_ignore_ascii_case(requested_owner) {
        return Err(invalid(format!(
            "item {index} belongs to another subaccount"
        )));
    }
    let pair = decode_spot_pair_identity(&order.pair)
        .ok_or_else(|| invalid(format!("item {index} has an invalid pair identity")))?;
    if requested_pair
        .and_then(decode_spot_pair_identity)
        .is_some_and(|expected| pair != expected)
        || order.pair_name.trim().is_empty()
        || requested_name.is_some_and(|expected| order.pair_name != expected)
    {
        return Err(invalid(format!(
            "item {index} belongs to another Spot market"
        )));
    }
    if parse_canonical_order_id(&order.order_id).is_none() {
        return Err(invalid(format!(
            "item {index} has an invalid decimal order ID"
        )));
    }
    if !matches!(order.order_side.as_str(), "Buy" | "Sell")
        || requested_side.is_some_and(|side| order.order_side != side.as_str())
    {
        return Err(invalid(format!(
            "item {index} does not match the requested side"
        )));
    }
    if order.price.is_sign_negative()
        || order
            .avg_fill_price
            .is_some_and(|value| value.is_sign_negative())
        || order.slippage.is_sign_negative()
        || order.base_amount <= Decimal::ZERO
        || order.base_remaining_amount.is_sign_negative()
        || order.base_remaining_amount > order.base_amount
        || order.quote_amount <= Decimal::ZERO
        || order.quote_remaining_amount.is_sign_negative()
        || order.quote_remaining_amount > order.quote_amount
    {
        return Err(invalid(format!(
            "item {index} has invalid financial values"
        )));
    }
    if order.block_number == 0
        || order.status.trim().is_empty()
        || order.price_type.trim().is_empty()
        || order.post_only.trim().is_empty()
        || order.tx_hash_type.trim().is_empty()
        || order
            .cancel_reason
            .as_deref()
            .is_some_and(|reason| reason.trim().is_empty())
        || order.cancel_height == Some(0)
    {
        return Err(invalid(format!(
            "item {index} has invalid metadata or an empty venue enum value"
        )));
    }
    if order
        .tx_hash
        .strip_prefix("0x")
        .and_then(decode_spot_pair_identity)
        .is_none()
    {
        return Err(invalid(format!(
            "item {index} has an invalid transaction hash"
        )));
    }
    let created = order
        .create_time
        .parse::<jiff::Timestamp>()
        .map_err(|e| invalid(format!("item {index} has invalid createTime: {e}")))?;
    u64::try_from(created.as_millisecond())
        .map_err(|_| invalid(format!("item {index} timestamp is before the Unix epoch")))?;
    Ok(created)
}

#[derive(Clone, Copy)]
struct SpotOrderScope<'a> {
    endpoint: &'static str,
    owner: &'a str,
    name: Option<&'a str>,
    pair: Option<&'a str>,
    side: Option<DeepXSpotOrderSide>,
    sort: DeepXAccountSortOrder,
    active: bool,
}

fn validate_spot_order_page(
    page: &DeepXAccountPage<DeepXSpotOrderRecord>,
    scope: SpotOrderScope<'_>,
) -> Result<(), DeepXRestError> {
    let mut order_ids = HashSet::new();
    let mut previous = None;
    for (index, order) in page.items.iter().enumerate() {
        let created = validate_spot_order_record(
            scope.endpoint,
            index,
            order,
            scope.owner,
            scope.name,
            scope.pair,
            scope.side,
        )?;
        if scope.active {
            validate_spot_open_order_state(index, order)?;
        }
        validate_spot_order_sequence(
            scope.endpoint,
            index,
            &order.order_id,
            created,
            scope.sort,
            &mut previous,
            &mut order_ids,
        )?;
    }
    Ok(())
}

/// Tracks Spot order identities and time order across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXSpotOrderContinuity {
    order_ids: HashSet<String>,
    previous: Option<jiff::Timestamp>,
}

impl DeepXSpotOrderContinuity {
    /// Checks one validated Spot open-order page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid order, repeated order ID, or inconsistent time order.
    pub fn observe_open_page(
        &mut self,
        page: &DeepXAccountPage<DeepXSpotOrderRecord>,
        request: &DeepXSpotOpenOrdersRequest,
    ) -> Result<(), DeepXRestError> {
        self.observe_page(
            page,
            SpotOrderScope {
                endpoint: "spot open orders",
                owner: &request.subaccount,
                name: request.name.as_deref(),
                pair: request.pair.as_deref(),
                side: request.order_side,
                sort: request.sort,
                active: true,
            },
        )
    }

    /// Checks one validated Spot history-order page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid order, repeated order ID, or inconsistent time order.
    pub fn observe_history_page(
        &mut self,
        page: &DeepXAccountPage<DeepXSpotOrderRecord>,
        request: &DeepXSpotHistoryOrdersRequest,
    ) -> Result<(), DeepXRestError> {
        self.observe_page(
            page,
            SpotOrderScope {
                endpoint: "spot history orders",
                owner: &request.subaccount,
                name: request.name.as_deref(),
                pair: request.pair.as_deref(),
                side: request.order_side,
                sort: request.sort,
                active: false,
            },
        )
    }

    fn observe_page(
        &mut self,
        page: &DeepXAccountPage<DeepXSpotOrderRecord>,
        scope: SpotOrderScope<'_>,
    ) -> Result<(), DeepXRestError> {
        for (index, order) in page.items.iter().enumerate() {
            let created = validate_spot_order_record(
                scope.endpoint,
                index,
                order,
                scope.owner,
                scope.name,
                scope.pair,
                scope.side,
            )?;
            validate_spot_order_sequence(
                scope.endpoint,
                index,
                &order.order_id,
                created,
                scope.sort,
                &mut self.previous,
                &mut self.order_ids,
            )?;
        }
        Ok(())
    }
}

fn validate_spot_open_order_state(
    index: usize,
    order: &DeepXSpotOrderRecord,
) -> Result<(), DeepXRestError> {
    let active = match order.status.as_str() {
        "Open" => {
            order.base_remaining_amount == order.base_amount && order.avg_fill_price.is_none()
        }
        "PartiallyFilled" => {
            order.base_remaining_amount > Decimal::ZERO
                && order.base_remaining_amount < order.base_amount
                && order
                    .avg_fill_price
                    .is_some_and(|price| price > Decimal::ZERO)
        }
        _ => false,
    };
    if !active {
        return Err(DeepXRestError::InvalidHistoryResponse {
            endpoint: "spot open orders",
            message: format!(
                "item {index} order {} has invalid active state or quantities",
                order.order_id
            ),
        });
    }
    Ok(())
}

/// Validates Spot order time ordering and ID uniqueness across a sequence of pages.
///
/// Updates the state only after validation succeeds.
///
/// # Errors
///
/// Returns an invalid-history error for a time-order violation or repeated order ID.
pub fn validate_spot_order_sequence(
    endpoint: &'static str,
    index: usize,
    order_id: &str,
    created: jiff::Timestamp,
    sort: DeepXAccountSortOrder,
    previous: &mut Option<jiff::Timestamp>,
    order_ids: &mut HashSet<String>,
) -> Result<(), DeepXRestError> {
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let out_of_order = previous.as_ref().is_some_and(|previous| match sort {
        DeepXAccountSortOrder::Ascending => created < *previous,
        DeepXAccountSortOrder::Descending => created > *previous,
    });
    if out_of_order {
        return Err(invalid(format!(
            "item {index} violates the requested time order"
        )));
    }
    if !order_ids.insert(order_id.to_string()) {
        return Err(invalid(format!("item {index} repeats order ID")));
    }
    *previous = Some(created);
    Ok(())
}

/// Validates an ordinary perpetual order against its account and market scope.
///
/// # Errors
///
/// Returns an invalid-history error for malformed identity, financial, venue, or time fields.
pub fn validate_perp_order_record_scope(
    endpoint: &'static str,
    index: usize,
    order: &DeepXPerpOrderRecord,
    requested_owner: &str,
    requested_market_id: Option<u64>,
) -> Result<(), DeepXRestError> {
    validate_perp_order_record_scope_with_size_policy(
        endpoint,
        index,
        order,
        requested_owner,
        requested_market_id,
        false,
    )
}

/// Validates a wallet-grouped perpetual order, including venue zero-size observations.
///
/// # Errors
///
/// Returns an invalid-history error for malformed identity, financial, venue, or time fields.
pub fn validate_perp_wallet_order_record_scope(
    endpoint: &'static str,
    index: usize,
    order: &DeepXPerpOrderRecord,
    requested_owner: &str,
    requested_market_id: Option<u64>,
) -> Result<(), DeepXRestError> {
    validate_perp_order_record_scope_with_size_policy(
        endpoint,
        index,
        order,
        requested_owner,
        requested_market_id,
        true,
    )
}

/// Validates the active status and exact size accounting of an open perpetual order.
///
/// # Errors
///
/// Returns an invalid-history error for a terminal or inconsistent order state.
pub fn validate_perp_open_order_state(
    index: usize,
    order: &DeepXPerpOrderRecord,
) -> Result<(), DeepXRestError> {
    let size_accounted = order
        .size_filled
        .checked_add(order.size_remain)
        .is_some_and(|size| size == order.size);
    let active = match order.status.as_str() {
        "Open" => {
            order.size_filled.is_zero()
                && order.size_remain == order.size
                && order.avg_fill_price.is_none()
        }
        "PartiallyFilled" => {
            !order.size_filled.is_zero()
                && !order.size_remain.is_zero()
                && size_accounted
                && order
                    .avg_fill_price
                    .is_some_and(|price| price > Decimal::ZERO)
        }
        _ => false,
    };
    if !active {
        return Err(DeepXRestError::InvalidHistoryResponse {
            endpoint: "perp open orders",
            message: format!(
                "item {index} order {} has invalid active state or quantities",
                order.order_id
            ),
        });
    }
    Ok(())
}

fn validate_perp_order_record_scope_with_size_policy(
    endpoint: &'static str,
    index: usize,
    order: &DeepXPerpOrderRecord,
    requested_owner: &str,
    requested_market_id: Option<u64>,
    allow_zero_size: bool,
) -> Result<(), DeepXRestError> {
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if !order.owner.eq_ignore_ascii_case(requested_owner) {
        return Err(invalid(format!(
            "item {index} belongs to another subaccount"
        )));
    }
    if order.market_id == 0
        || requested_market_id.is_some_and(|expected| order.market_id != expected)
    {
        return Err(invalid(format!("item {index} has an unexpected market ID")));
    }
    if parse_canonical_order_id(&order.order_id).is_none() {
        return Err(invalid(format!(
            "item {index} has an invalid decimal order ID"
        )));
    }
    if order.size.is_sign_negative()
        || (!allow_zero_size && order.size.is_zero())
        || order.price.is_sign_negative()
        || order
            .avg_fill_price
            .is_some_and(|value| value.is_sign_negative())
        || order.leverage <= Decimal::ZERO
        || order.slippage.is_sign_negative()
        || order.size_filled.is_sign_negative()
        || order.size_remain.is_sign_negative()
        || order.size_filled > order.size
        || order.size_remain > order.size
        || order
            .take_profit
            .is_some_and(|value| value.is_sign_negative())
        || order
            .stop_loss
            .is_some_and(|value| value.is_sign_negative())
    {
        return Err(invalid(format!(
            "item {index} has invalid financial values"
        )));
    }
    if order.order_type.trim().is_empty()
        || order.status.trim().is_empty()
        || order.post_only.trim().is_empty()
        || order.tx_hash_type.trim().is_empty()
    {
        return Err(invalid(format!(
            "item {index} has an empty venue enum value"
        )));
    }
    if order
        .tx_hash
        .strip_prefix("0x")
        .and_then(decode_spot_pair_identity)
        .is_none()
    {
        return Err(invalid(format!(
            "item {index} has an invalid transaction hash"
        )));
    }
    let created = order
        .create_time
        .parse::<jiff::Timestamp>()
        .map_err(|e| invalid(format!("item {index} has invalid createTime: {e}")))?;
    if let Some(updated_time) = &order.updated_time {
        let updated = updated_time
            .parse::<jiff::Timestamp>()
            .map_err(|e| invalid(format!("item {index} has invalid updatedTime: {e}")))?;
        if updated < created {
            return Err(invalid(format!(
                "item {index} update timestamp precedes creation"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum PerpOrderPageKind {
    Open,
    History,
    Position,
}

fn validate_perp_order_page(
    page: &DeepXAccountPage<DeepXPerpOrderRecord>,
    endpoint: &'static str,
    owner: &str,
    market_id: Option<u64>,
    side: Option<bool>,
    sort: Option<DeepXAccountSortOrder>,
    kind: PerpOrderPageKind,
) -> Result<(), DeepXRestError> {
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut order_ids = HashSet::new();
    let mut previous_time = None;
    let mut previous_position = None;
    for (index, order) in page.items.iter().enumerate() {
        validate_perp_order_record_scope(endpoint, index, order, owner, market_id)?;
        if side.is_some_and(|expected| order.is_long != expected) {
            return Err(invalid(format!(
                "item {index} does not match the requested side"
            )));
        }
        if matches!(kind, PerpOrderPageKind::Open) {
            validate_perp_open_order_state(index, order)?;
        }
        if matches!(kind, PerpOrderPageKind::Position) {
            let order_id = order
                .order_id
                .parse::<u64>()
                .map_err(|_| invalid(format!("item {index} has an invalid decimal order ID")))?;
            let key = (order_id, order.is_long);
            if previous_position.is_some_and(|previous| key <= previous) {
                return Err(invalid(format!(
                    "item {index} violates ascending order ID/side order"
                )));
            }
            previous_position = Some(key);
        } else {
            let created = order
                .create_time
                .parse::<jiff::Timestamp>()
                .map_err(|e| invalid(format!("item {index} has invalid createTime: {e}")))?;
            let out_of_order = previous_time.is_some_and(|previous| match sort {
                Some(DeepXAccountSortOrder::Ascending) => created < previous,
                Some(DeepXAccountSortOrder::Descending) => created > previous,
                None => false,
            });
            if out_of_order {
                return Err(invalid(format!(
                    "item {index} violates the requested time order"
                )));
            }
            if !order_ids.insert(&order.order_id) {
                return Err(invalid(format!("item {index} repeats order ID")));
            }
            previous_time = Some(created);
        }
    }
    Ok(())
}

/// Tracks perpetual open and history order identities and time order across pages.
#[derive(Debug, Default)]
pub struct DeepXPerpOrderContinuity {
    order_ids: HashSet<String>,
    previous: Option<jiff::Timestamp>,
}

impl DeepXPerpOrderContinuity {
    /// Checks one validated open-order page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid order, repeated order ID, or inconsistent time order.
    pub fn observe_open_page(
        &mut self,
        page: &DeepXAccountPage<DeepXPerpOrderRecord>,
        request: &DeepXPerpOpenOrdersRequest,
    ) -> Result<(), DeepXRestError> {
        validate_perp_order_page(
            page,
            "perp open orders",
            &request.subaccount,
            request.market_id,
            request.is_long,
            Some(request.sort),
            PerpOrderPageKind::Open,
        )?;
        self.observe_page(page, "perp open orders", request.sort)
    }

    /// Checks one validated history-order page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid order, repeated order ID, or inconsistent time order.
    pub fn observe_history_page(
        &mut self,
        page: &DeepXAccountPage<DeepXPerpOrderRecord>,
        request: &DeepXPerpHistoryOrdersRequest,
    ) -> Result<(), DeepXRestError> {
        validate_perp_order_page(
            page,
            "perp history orders",
            &request.subaccount,
            request.market_id,
            None,
            Some(request.sort),
            PerpOrderPageKind::History,
        )?;
        self.observe_page(page, "perp history orders", request.sort)
    }

    fn observe_page(
        &mut self,
        page: &DeepXAccountPage<DeepXPerpOrderRecord>,
        endpoint: &'static str,
        sort: DeepXAccountSortOrder,
    ) -> Result<(), DeepXRestError> {
        let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
        for (index, order) in page.items.iter().enumerate() {
            let created = order
                .create_time
                .parse::<jiff::Timestamp>()
                .map_err(|e| invalid(format!("item {index} has invalid createTime: {e}")))?;
            if self.previous.is_some_and(|previous| match sort {
                DeepXAccountSortOrder::Ascending => created < previous,
                DeepXAccountSortOrder::Descending => created > previous,
            }) {
                return Err(invalid(format!(
                    "item {index} violates the requested time order"
                )));
            }
            if !self.order_ids.insert(order.order_id.clone()) {
                return Err(invalid(format!("item {index} repeats order ID")));
            }
            self.previous = Some(created);
        }
        Ok(())
    }
}

/// Tracks perpetual position-order identity order across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXPerpPositionOrderContinuity {
    previous: Option<(u64, bool)>,
}

impl DeepXPerpPositionOrderContinuity {
    /// Checks one validated position-order page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid order or nonascending order ID and side identity.
    pub fn observe_page(
        &mut self,
        page: &DeepXAccountPage<DeepXPerpOrderRecord>,
        request: &DeepXPerpPositionOrdersRequest,
    ) -> Result<(), DeepXRestError> {
        const ENDPOINT: &str = "perp position orders";
        for (index, order) in page.items.iter().enumerate() {
            validate_perp_order_record_scope(ENDPOINT, index, order, &request.subaccount, None)?;
            let order_id = order.order_id.parse::<u64>().map_err(|_| {
                DeepXRestError::InvalidHistoryResponse {
                    endpoint: ENDPOINT,
                    message: "invalid decimal order ID".to_string(),
                }
            })?;
            let key = (order_id, order.is_long);
            if self.previous.is_some_and(|previous| key <= previous) {
                return Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint: ENDPOINT,
                    message: format!("item {index} violates ascending order ID/side order"),
                });
            }
            self.previous = Some(key);
        }
        Ok(())
    }
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

/// Documented DeepX REST transact actions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXRestAction {
    PerpPlace,
    PerpCancel,
    PerpClose,
    PerpProfitAndLossPoint,
    SpotPlace,
    SpotCancel,
}

impl DeepXRestAction {
    const fn labels(self) -> (&'static str, &'static str) {
        match self {
            Self::PerpPlace => ("Perp", "PlaceOrder"),
            Self::PerpCancel => ("Perp", "CancelOrder"),
            Self::PerpClose => ("Perp", "ClosePosition"),
            Self::PerpProfitAndLossPoint => ("Perp", "SetProfitAndLossPoint"),
            Self::SpotPlace => ("Spot", "PlaceOrder"),
            Self::SpotCancel => ("Spot", "CancelOrder"),
        }
    }
}

/// Venue transaction acknowledgement before the caller verifies the returned hash.
#[derive(Debug)]
pub struct DeepXRestTransactionAcknowledgement {
    pub tx_hash: String,
    pub data: Value,
}

/// Encodes one documented transaction action and signed extrinsic for REST submission.
///
/// # Errors
///
/// Returns an error when JSON serialization fails.
pub fn encode_transaction_request(
    action: DeepXRestAction,
    signed_extrinsic: &[u8],
) -> serde_json::Result<Vec<u8>> {
    let (market_type, action) = action.labels();
    serde_json::to_vec(&json!({
        "marketType": market_type,
        "action": action,
        "signedExtrinsic": format!("0x{}", hex::encode(signed_extrinsic)),
    }))
}

/// Decodes a successful REST transact acknowledgement without inferring finality.
///
/// The caller must verify `tx_hash` against the exact submitted extrinsic.
///
/// # Errors
///
/// Returns an error for malformed JSON, backend failure, or a missing hash.
pub fn decode_transaction_acknowledgement(
    body: &[u8],
) -> anyhow::Result<DeepXRestTransactionAcknowledgement> {
    let response: ApiEnvelope = serde_json::from_slice(body)?;
    anyhow::ensure!(
        !response.fail && response.code == DeepXApiCode::Api(200),
        "backend code {}: {}",
        response.code,
        response.msg,
    );
    let data: Value = serde_json::from_str(response.data.get())?;
    let tx_hash = data
        .get("tx_hash")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing transaction hash"))?
        .to_string();
    Ok(DeepXRestTransactionAcknowledgement { tx_hash, data })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TransactionStatusQuery<'a> {
    tx_hash: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PerpOrderByIdQuery<'a> {
    user: &'a str,
    market_id: u64,
    oid: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OrderByTxQuery<'a> {
    tx_hash: &'a str,
}

impl DeepXRestTransport {
    /// Creates a transport with the configured timeout and proxy.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be constructed.
    pub fn new(
        timeout_secs: Option<u64>,
        proxy_url: Option<String>,
    ) -> Result<Self, DeepXRestError> {
        install_tls_provider();
        let mut read_builder = Client::builder();
        let mut transaction_builder = Client::builder().redirect(Policy::none());
        if let Some(proxy_url) = proxy_url {
            let proxy =
                Proxy::all(&proxy_url).map_err(|e| DeepXRestError::Transport(e.to_string()))?;
            read_builder = read_builder.proxy(proxy.clone());
            transaction_builder = transaction_builder.proxy(proxy);
        }
        let client = read_builder
            .build()
            .map_err(|e| DeepXRestError::Transport(e.to_string()))?;
        let transaction_client = transaction_builder
            .build()
            .map_err(|e| DeepXRestError::Transport(e.to_string()))?;
        Ok(Self {
            client,
            transaction_client,
            timeout_secs,
        })
    }

    /// Submits one signed extrinsic without redirects or automatic retry.
    ///
    /// # Errors
    ///
    /// Returns a transport error; the caller must classify ambiguous delivery.
    pub async fn post_transaction_once(
        &self,
        base_url: &str,
        body: Vec<u8>,
    ) -> Result<DeepXRestResponse, DeepXRestError> {
        let base_url = normalize_base_url(base_url.to_string())?;
        let request = self
            .transaction_client
            .post(format!("{base_url}/internal/v1/chain/tx/transact"))
            .header("Content-Type", "application/json")
            .body(body);
        send_request(with_timeout(request, self.timeout_secs)).await
    }

    /// Queries one transaction status without redirects or automatic retry.
    ///
    /// # Errors
    ///
    /// Returns a transport error; response status and body remain available to the caller.
    pub async fn get_transaction_status_once(
        &self,
        base_url: &str,
        tx_hash: &str,
    ) -> Result<DeepXRestResponse, DeepXRestError> {
        let base_url = normalize_base_url(base_url.to_string())?;
        let request = self
            .transaction_client
            .get(format!("{base_url}/internal/v1/chain/tx/status"))
            .query(&TransactionStatusQuery { tx_hash });
        send_request(with_timeout(request, self.timeout_secs)).await
    }

    /// Queries one transaction status and returns a hash-bound backend observation.
    ///
    /// This performs one request without redirects or retries. A backend observation is not
    /// evidence of canonical finality.
    ///
    /// # Errors
    ///
    /// Returns a transport, HTTP, API, decoding, or invalid-status response error.
    pub async fn get_transaction_status(
        &self,
        base_url: &str,
        extrinsic_hash: [u8; 32],
    ) -> Result<status::DeepXRestTransactionStatusData, DeepXRestError> {
        let tx_hash = format!("0x{}", hex::encode(extrinsic_hash));
        let response = self.get_transaction_status_once(base_url, &tx_hash).await?;
        decode_transaction_status_response(&response, extrinsic_hash)
    }

    /// Sends one GET and decodes a JSON response.
    ///
    /// # Errors
    ///
    /// Returns a path, transport, HTTP status, or JSON decoding error.
    pub async fn get_json<R: DeserializeOwned>(
        &self,
        base_url: &str,
        path: &str,
    ) -> Result<R, DeepXRestError> {
        validate_path(path)?;
        let base_url = normalize_base_url(base_url.to_string())?;
        let request = self.client.get(format!("{base_url}{path}"));
        let response = send_request(with_timeout(request, self.timeout_secs)).await?;
        decode_response(response.status.as_u16(), &response.body)
    }

    /// Sends one GET with typed query parameters and decodes a JSON response.
    ///
    /// # Errors
    ///
    /// Returns a path, transport, HTTP status, or JSON decoding error.
    pub async fn get_json_with_query<R: DeserializeOwned, Q: Serialize>(
        &self,
        base_url: &str,
        path: &str,
        query: &Q,
    ) -> Result<R, DeepXRestError> {
        validate_path(path)?;
        let base_url = normalize_base_url(base_url.to_string())?;
        let request = self.client.get(format!("{base_url}{path}")).query(query);
        let response = send_request(with_timeout(request, self.timeout_secs)).await?;
        decode_response(response.status.as_u16(), &response.body)
    }

    /// Reads and decodes the `data` field from a DeepX API response.
    ///
    /// # Errors
    ///
    /// Returns a transport, HTTP, venue API, or decoding error.
    pub async fn get_api_data<T: DeserializeOwned>(
        &self,
        base_url: &str,
        path: &str,
    ) -> Result<T, DeepXRestError> {
        let response: ApiEnvelope = self.get_json(base_url, path).await?;
        decode_api_data(response)
    }

    /// Reads and decodes a DeepX API response with typed query parameters.
    ///
    /// # Errors
    ///
    /// Returns a transport, HTTP, venue API, or decoding error.
    pub async fn get_api_data_with_query<T: DeserializeOwned, Q: Serialize>(
        &self,
        base_url: &str,
        path: &str,
        query: &Q,
    ) -> Result<T, DeepXRestError> {
        let response: ApiEnvelope = self.get_json_with_query(base_url, path, query).await?;
        decode_api_data(response)
    }

    /// Returns the validated subaccount directory for one wallet.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or directory validation error.
    pub async fn get_wallet_subaccounts(
        &self,
        base_url: &str,
        wallet: &str,
    ) -> Result<DeepXWalletSubaccounts, DeepXRestError> {
        #[derive(Serialize)]
        struct Query<'a> {
            address: &'a str,
        }

        if !is_account_address(wallet) {
            return Err(DeepXRestError::InvalidRequest(
                "wallet subaccounts requires a 20-byte hex wallet address".to_string(),
            ));
        }
        let addresses: Vec<String> = self
            .get_api_data_with_query(
                base_url,
                WALLET_SUBACCOUNTS_PATH,
                &Query { address: wallet },
            )
            .await?;
        validate_wallet_subaccounts(&addresses)?;
        Ok(DeepXWalletSubaccounts::new(wallet.to_string(), addresses))
    }

    /// Returns the validated perpetual order selected by account, market, and order ID.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-record error.
    pub async fn get_perp_order_by_id(
        &self,
        base_url: &str,
        subaccount: &str,
        market_id: u64,
        order_id: &str,
    ) -> Result<DeepXPerpOrderRecord, DeepXRestError> {
        if parse_canonical_order_id(order_id).is_none() {
            return Err(DeepXRestError::InvalidRequest(
                "perp-order-by-id requires a decimal u64 order_id".to_string(),
            ));
        }
        let raw = self
            .get_perp_order_by_id_raw(base_url, subaccount, market_id, order_id)
            .await?;
        let order: DeepXPerpOrderRecord = serde_json::from_str(raw.get()).map_err(|e| {
            DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp order by ID",
                message: format!("record failed decoding: {e}"),
            }
        })?;
        validate_perp_order_record_scope(
            "perp order by ID",
            0,
            &order,
            subaccount,
            Some(market_id),
        )?;
        if order.order_id != order_id {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp order by ID",
                message: "record belongs to another order ID".to_string(),
            });
        }
        Ok(order)
    }

    /// Returns the validated perpetual order selected by its extrinsic hash.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-record error.
    pub async fn get_perp_order_by_tx(
        &self,
        base_url: &str,
        tx_hash: &str,
    ) -> Result<DeepXPerpOrderRecord, DeepXRestError> {
        let raw = self.get_perp_order_by_tx_raw(base_url, tx_hash).await?;
        let order: DeepXPerpOrderRecord = serde_json::from_str(raw.get())?;
        validate_perp_order_record_scope(
            "perp order by transaction hash",
            0,
            &order,
            &order.owner,
            Some(order.market_id),
        )?;
        let expected = tx_hash
            .strip_prefix("0x")
            .and_then(decode_spot_pair_identity);
        let received = order
            .tx_hash
            .strip_prefix("0x")
            .and_then(decode_spot_pair_identity);
        if received != expected || order.tx_hash_type != "EXTRINSIC_HASH" {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp order by transaction hash",
                message: "record does not identify the requested extrinsic hash".to_string(),
            });
        }
        Ok(order)
    }

    /// Returns the validated Spot order selected by account and order ID.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-record error.
    pub async fn get_spot_order_by_id(
        &self,
        base_url: &str,
        request: &DeepXSpotOrderByIdRequest,
    ) -> Result<DeepXSpotOrderRecord, DeepXRestError> {
        let raw = self.get_spot_order_by_id_raw(base_url, request).await?;
        let order: DeepXSpotOrderRecord = serde_json::from_str(raw.get()).map_err(|e| {
            DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot order by ID",
                message: format!("record failed decoding: {e}"),
            }
        })?;
        validate_spot_order_record(
            "spot order by ID",
            0,
            &order,
            &request.subaccount,
            request.name.as_deref(),
            request.pair.as_deref(),
            Some(request.order_side),
        )?;
        if order.order_id != request.order_id {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot order by ID",
                message: "record belongs to another order ID".to_string(),
            });
        }
        Ok(order)
    }

    /// Returns the validated Spot order selected by its extrinsic hash.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-record error.
    pub async fn get_spot_order_by_tx(
        &self,
        base_url: &str,
        tx_hash: &str,
    ) -> Result<DeepXSpotOrderRecord, DeepXRestError> {
        let raw = self.get_spot_order_by_tx_raw(base_url, tx_hash).await?;
        let order: DeepXSpotOrderRecord = serde_json::from_str(raw.get())?;
        validate_spot_order_record(
            "spot order by transaction hash",
            0,
            &order,
            &order.maker,
            Some(&order.pair_name),
            None,
            None,
        )?;
        let expected = tx_hash
            .strip_prefix("0x")
            .and_then(decode_spot_pair_identity);
        let received = order
            .tx_hash
            .strip_prefix("0x")
            .and_then(decode_spot_pair_identity);
        if received != expected || order.tx_hash_type != "EXTRINSIC_HASH" {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot order by transaction hash",
                message: "record does not identify the requested extrinsic hash".to_string(),
            });
        }
        Ok(order)
    }

    /// Returns one typed page of open perpetual orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, or record decoding error.
    pub async fn get_perp_open_orders(
        &self,
        base_url: &str,
        request: &DeepXPerpOpenOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpOrderRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_perp_open_orders_raw(base_url, request).await?,
            "perp open orders",
            request.page_size,
        )?;
        validate_perp_order_page(
            &page,
            "perp open orders",
            &request.subaccount,
            request.market_id,
            request.is_long,
            Some(request.sort),
            PerpOrderPageKind::Open,
        )?;
        Ok(page)
    }

    /// Returns one typed page of historical perpetual orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, or record decoding error.
    pub async fn get_perp_history_orders(
        &self,
        base_url: &str,
        request: &DeepXPerpHistoryOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpOrderRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_perp_history_orders_raw(base_url, request).await?,
            "perp history orders",
            request.page_size,
        )?;
        validate_perp_order_page(
            &page,
            "perp history orders",
            &request.subaccount,
            request.market_id,
            None,
            Some(request.sort),
            PerpOrderPageKind::History,
        )?;
        Ok(page)
    }

    /// Returns one typed page of perpetual position orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, or record decoding error.
    pub async fn get_perp_position_orders(
        &self,
        base_url: &str,
        request: &DeepXPerpPositionOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpOrderRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_perp_position_orders_raw(base_url, request).await?,
            "perp position orders",
            request.page_size,
        )?;
        validate_perp_order_page(
            &page,
            "perp position orders",
            &request.subaccount,
            None,
            None,
            None,
            PerpOrderPageKind::Position,
        )?;
        Ok(page)
    }

    /// Returns one validated typed page of open Spot orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, decoding, or invalid-record error.
    pub async fn get_spot_open_orders(
        &self,
        base_url: &str,
        request: &DeepXSpotOpenOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXSpotOrderRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_spot_open_orders_raw(base_url, request).await?,
            "spot open orders",
            request.page_size,
        )?;
        validate_spot_order_page(
            &page,
            SpotOrderScope {
                endpoint: "spot open orders",
                owner: &request.subaccount,
                name: request.name.as_deref(),
                pair: request.pair.as_deref(),
                side: request.order_side,
                sort: request.sort,
                active: true,
            },
        )?;
        Ok(page)
    }

    /// Returns one validated typed page of historical Spot orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, decoding, or invalid-record error.
    pub async fn get_spot_history_orders(
        &self,
        base_url: &str,
        request: &DeepXSpotHistoryOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXSpotOrderRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_spot_history_orders_raw(base_url, request).await?,
            "spot history orders",
            request.page_size,
        )?;
        validate_spot_order_page(
            &page,
            SpotOrderScope {
                endpoint: "spot history orders",
                owner: &request.subaccount,
                name: request.name.as_deref(),
                pair: request.pair.as_deref(),
                side: request.order_side,
                sort: request.sort,
                active: false,
            },
        )?;
        Ok(page)
    }

    /// Returns one validated typed page of perpetual account trades.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, decoding, or invalid-record error.
    pub async fn get_perp_account_trades(
        &self,
        base_url: &str,
        request: &DeepXPerpAccountTradesRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpAccountTradeRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_perp_account_trades_raw(base_url, request).await?,
            "perp account trades",
            request.page_size,
        )?;
        validate_perp_account_trades_page(&page, request)?;
        Ok(page)
    }

    /// Returns one validated typed page of Spot account trades.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, decoding, or invalid-record error.
    pub async fn get_spot_account_trades(
        &self,
        base_url: &str,
        request: &DeepXSpotAccountTradesRequest,
    ) -> Result<DeepXAccountPage<DeepXSpotAccountTradeRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_spot_account_trades_raw(base_url, request).await?,
            "spot account trades",
            request.page_size,
        )?;
        validate_spot_account_trades_page(&page, request)?;
        Ok(page)
    }

    /// Returns one validated typed page of perpetual funding fees.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, decoding, or invalid-record error.
    pub async fn get_perp_funding_fees(
        &self,
        base_url: &str,
        request: &DeepXPerpFundingFeeRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpFundingFeeRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_perp_funding_fees_raw(base_url, request).await?,
            "perp funding fees",
            request.page_size,
        )?;
        validate_funding_fee_page(
            &page,
            "perp funding fees",
            Some(&request.subaccount),
            request.market_id,
            request.start_ms,
            request.end_ms,
        )?;
        Ok(page)
    }

    /// Returns one validated typed page of wallet funding fees.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, decoding, or invalid-record error.
    pub async fn get_wallet_funding_fees(
        &self,
        base_url: &str,
        request: &DeepXWalletFundingFeeRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpFundingFeeRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_wallet_funding_fees_raw(base_url, request).await?,
            "wallet funding fees",
            request.page_size,
        )?;
        validate_funding_fee_page(
            &page,
            "wallet funding fees",
            None,
            request.market_id,
            request.start_ms,
            request.end_ms,
        )?;
        Ok(page)
    }

    /// Returns one validated typed page of perpetual positions.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, decoding, or invalid-record error.
    pub async fn get_perp_positions(
        &self,
        base_url: &str,
        request: &DeepXPerpPositionsRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpPositionRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_perp_positions_raw(base_url, request).await?,
            "perp positions",
            request.page_size,
        )?;
        validate_perp_positions_page(&page, request)?;
        Ok(page)
    }

    /// Returns one validated typed page of balance changes.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, decoding, or invalid-record error.
    pub async fn get_balance_changes(
        &self,
        base_url: &str,
        request: &DeepXBalanceChangesRequest,
    ) -> Result<DeepXAccountPage<DeepXBalanceChangeRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_balance_changes_raw(base_url, request).await?,
            "balance changes",
            request.page_size,
        )?;
        validate_balance_changes_page(&page, request)?;
        Ok(page)
    }

    /// Returns the uninterpreted payload for one perpetual order identity.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_order_by_id_raw(
        &self,
        base_url: &str,
        subaccount: &str,
        market_id: u64,
        order_id: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_perp_order_by_id(subaccount, market_id, order_id)?;
        self.get_api_data_with_query(
            base_url,
            PERP_ORDER_BY_ID_PATH,
            &PerpOrderByIdQuery {
                user: subaccount,
                market_id,
                oid: order_id,
            },
        )
        .await
    }

    /// Returns the uninterpreted perpetual order associated with one extrinsic hash.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_order_by_tx_raw(
        &self,
        base_url: &str,
        tx_hash: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        self.get_order_by_tx_raw(base_url, PERP_ORDER_BY_TX_PATH, tx_hash, "perp-order-by-tx")
            .await
    }

    /// Returns one uninterpreted page of open perpetual orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_perp_open_orders_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpOpenOrdersRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            PERP_OPEN_ORDERS_PATH,
            &request.as_query(),
            "perp open orders",
        )
        .await
    }

    /// Returns one uninterpreted page of historical perpetual orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_perp_history_orders_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpHistoryOrdersRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            PERP_HISTORY_ORDERS_PATH,
            &request.as_query(),
            "perp history orders",
        )
        .await
    }

    /// Returns one uninterpreted page of perpetual position orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_perp_position_orders_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpPositionOrdersRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            PERP_POSITION_ORDERS_PATH,
            &request.as_query(),
            "perp position orders",
        )
        .await
    }

    /// Returns one uninterpreted page of open Spot orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_spot_open_orders_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotOpenOrdersRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            SPOT_OPEN_ORDERS_PATH,
            &request.as_query(),
            "spot open orders",
        )
        .await
    }

    /// Returns one uninterpreted page of historical Spot orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_spot_history_orders_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotHistoryOrdersRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            SPOT_HISTORY_ORDERS_PATH,
            &request.as_query(),
            "spot history orders",
        )
        .await
    }

    /// Returns one uninterpreted page of perpetual account trades.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_perp_account_trades_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpAccountTradesRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            PERP_ACCOUNT_TRADES_PATH,
            &request.as_query(),
            "perp account trades",
        )
        .await
    }

    /// Returns one uninterpreted page of Spot account trades.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_spot_account_trades_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotAccountTradesRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            SPOT_ACCOUNT_TRADES_PATH,
            &request.as_query(),
            "spot account trades",
        )
        .await
    }

    /// Returns one uninterpreted page of perpetual funding fees.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_perp_funding_fees_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpFundingFeeRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            PERP_FUNDING_FEE_PATH,
            &request.as_query(),
            "perp funding fees",
        )
        .await
    }

    /// Returns one uninterpreted page of wallet funding fees.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_wallet_funding_fees_raw(
        &self,
        base_url: &str,
        request: &DeepXWalletFundingFeeRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            WALLET_FUNDING_FEE_PATH,
            &request.as_query(),
            "wallet funding fees",
        )
        .await
    }

    /// Returns one uninterpreted page of perpetual positions.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_perp_positions_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpPositionsRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            PERP_POSITIONS_PATH,
            &request.as_query(),
            "perp positions",
        )
        .await
    }

    /// Returns one uninterpreted page of balance changes.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_balance_changes_raw(
        &self,
        base_url: &str,
        request: &DeepXBalanceChangesRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            BALANCE_CHANGES_PATH,
            &request.as_query(),
            "balance changes",
        )
        .await
    }

    /// Returns the uninterpreted payload for one Spot order identity.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_order_by_id_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotOrderByIdRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_ORDER_BY_ID_PATH, &request.as_query())
            .await
    }

    /// Returns the uninterpreted Spot order associated with one extrinsic hash.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_order_by_tx_raw(
        &self,
        base_url: &str,
        tx_hash: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        self.get_order_by_tx_raw(base_url, SPOT_ORDER_BY_TX_PATH, tx_hash, "spot-order-by-tx")
            .await
    }

    async fn get_order_by_tx_raw(
        &self,
        base_url: &str,
        path: &str,
        tx_hash: &str,
        endpoint: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_order_tx_hash(tx_hash, endpoint)?;
        self.get_api_data_with_query(base_url, path, &OrderByTxQuery { tx_hash })
            .await
    }

    async fn get_account_page<Q: Serialize>(
        &self,
        base_url: &str,
        path: &str,
        query: &Q,
        endpoint: &'static str,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        let page: DeepXRawAccountPage = self.get_api_data_with_query(base_url, path, query).await?;
        validate_cursor_page(endpoint, page.has_next, page.next_cursor.as_deref())?;
        Ok(page)
    }
}

fn validate_order_tx_hash(tx_hash: &str, endpoint: &str) -> Result<(), DeepXRestError> {
    if !tx_hash.strip_prefix("0x").is_some_and(|value| {
        value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
    }) {
        return Err(DeepXRestError::InvalidRequest(format!(
            "{endpoint} requires a 0x-prefixed 32-byte tx_hash",
        )));
    }
    Ok(())
}

fn validate_perp_order_by_id(
    subaccount: &str,
    market_id: u64,
    order_id: &str,
) -> Result<(), DeepXRestError> {
    if !is_account_address(subaccount) || market_id == 0 || order_id.trim().is_empty() {
        return Err(DeepXRestError::InvalidRequest(
            "perp-order-by-id requires a 20-byte hex subaccount, positive market_id, and nonempty order_id"
                .to_string(),
        ));
    }
    Ok(())
}

/// Validates the identities and uniqueness of a wallet's subaccount directory.
///
/// # Errors
///
/// Returns an error when a subaccount address is malformed or repeated.
pub fn validate_wallet_subaccounts(addresses: &[String]) -> Result<(), DeepXRestError> {
    let mut seen = HashSet::new();
    for (index, address) in addresses.iter().enumerate() {
        if !is_account_address(address) {
            return Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "wallet subaccounts",
                message: format!("item {index} is not a 20-byte hex subaccount address"),
            });
        }
        if !seen.insert(address.to_ascii_lowercase()) {
            return Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "wallet subaccounts",
                message: format!("item {index} repeats a subaccount address"),
            });
        }
    }
    Ok(())
}

/// Validates a 20-byte hex account address for a named REST endpoint role.
///
/// # Errors
///
/// Returns an invalid-request error for a malformed address.
pub fn validate_account_address(
    endpoint: &str,
    role: &str,
    address: &str,
) -> Result<(), DeepXRestError> {
    if is_account_address(address) {
        return Ok(());
    }
    Err(DeepXRestError::InvalidRequest(format!(
        "{endpoint} requires a 20-byte hex {role} address"
    )))
}

fn is_account_address(value: &str) -> bool {
    decode_account_id20(value).is_some()
}

fn decode_transaction_status_response(
    response: &DeepXRestResponse,
    extrinsic_hash: [u8; 32],
) -> Result<status::DeepXRestTransactionStatusData, DeepXRestError> {
    let envelope: ApiEnvelope = decode_response(response.status.as_u16(), &response.body)?;
    let data: Box<RawValue> = decode_api_data(envelope)?;
    status::parse_transaction_status(data, extrinsic_hash).map_err(|e| {
        DeepXRestError::InvalidTransactionStatus {
            message: e.to_string(),
        }
    })
}

fn decode_api_data<T: DeserializeOwned>(response: ApiEnvelope) -> Result<T, DeepXRestError> {
    if response.fail || response.code != DeepXApiCode::Api(200) {
        return Err(DeepXRestError::Api {
            code: response.code,
            message: response.msg,
        });
    }
    Ok(serde_json::from_str(response.data.get())?)
}

fn with_timeout(
    request: reqwest::RequestBuilder,
    timeout_secs: Option<u64>,
) -> reqwest::RequestBuilder {
    match timeout_secs {
        Some(seconds) => request.timeout(Duration::from_secs(seconds)),
        None => request,
    }
}

async fn send_request(
    request: reqwest::RequestBuilder,
) -> Result<DeepXRestResponse, DeepXRestError> {
    let response = request
        .send()
        .await
        .map_err(|e| DeepXRestError::Transport(format!("{:#}", anyhow::Error::new(e))))?;
    let status = response.status();
    let body = read_bounded_body(response)
        .await
        .map_err(|e| DeepXRestError::Transport(e.to_string()))?;
    Ok(DeepXRestResponse { status, body })
}

/// Normalizes and validates a DeepX REST base URL.
///
/// # Errors
///
/// Returns an error for a malformed URL or unsupported scheme.
pub fn normalize_base_url(base_url: String) -> Result<String, DeepXRestError> {
    let normalized = base_url.trim_end_matches('/').to_string();
    let Ok(url) = reqwest::Url::parse(&normalized) else {
        return Err(DeepXRestError::InvalidBaseUrl(base_url));
    };
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DeepXRestError::InvalidBaseUrl(base_url));
    }
    Ok(normalized)
}

/// Checks that a request path cannot override or escape the configured base URL.
///
/// # Errors
///
/// Returns an error for an authority, query, fragment, or parent segment.
pub fn validate_path(path: &str) -> Result<(), DeepXRestError> {
    if !path.starts_with('/')
        || path.starts_with("//")
        || path.contains('?')
        || path.contains('#')
        || path.contains("://")
        || path.split('/').any(|segment| segment == "..")
    {
        return Err(DeepXRestError::InvalidPath(path.to_string()));
    }
    Ok(())
}

fn decode_response<R: DeserializeOwned>(status: u16, body: &[u8]) -> Result<R, DeepXRestError> {
    if !(200..300).contains(&status) {
        return Err(DeepXRestError::Http {
            status,
            message: String::from_utf8_lossy(body)
                .chars()
                .take(MAX_ERROR_BODY_CHARS)
                .collect(),
        });
    }
    Ok(serde_json::from_slice(body)?)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;

    const PERP_FUNDING_FEES_ACCOUNT_RESPONSE: &str =
        include_str!("../test_data/http/testnet/perp_funding_fees_account_market_3.json");
    const PERP_POSITIONS_ACCOUNT_RESPONSE: &str =
        include_str!("../test_data/http/testnet/perp_positions_account_market_3.json");
    const PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE: &str =
        include_str!("../test_data/http/testnet/perp_account_trades_account_market_3.json");
    const SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE: &str =
        include_str!("../test_data/http/testnet/spot_account_trades_eth_usdc_page_1.json");
    const SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE: &str =
        include_str!("../test_data/http/testnet/spot_history_orders_eth_usdc_page_1.json");

    #[rstest]
    #[case("0x0123456789abcdef0123456789abcdef01234567", true)]
    #[case("0X0123456789abcdef0123456789abcdef01234567", false)]
    #[case("0x0123", false)]
    #[case("0123456789abcdef0123456789abcdef01234567", false)]
    #[case("0x0123456789abcdef0123456789abcdef0123456g", false)]
    fn account_address_validation_preserves_request_errors(
        #[case] address: &str,
        #[case] valid: bool,
    ) {
        let result = validate_account_address("wallet subaccounts", "wallet", address);
        assert_eq!(result.is_ok(), valid);
        if !valid {
            assert!(matches!(
                result,
                Err(DeepXRestError::InvalidRequest(message))
                    if message == "wallet subaccounts requires a 20-byte hex wallet address",
            ));
        }
    }

    #[rstest]
    #[case(
        "0xABCDEF1111111111111111111111111111111111",
        "repeats a subaccount address"
    )]
    #[case("invalid", "is not a 20-byte hex subaccount address")]
    fn wallet_subaccount_directory_rejects_duplicate_and_malformed_addresses(
        #[case] second_address: &str,
        #[case] expected_message: &str,
    ) {
        let addresses = vec![
            "0xabcdef1111111111111111111111111111111111".to_string(),
            second_address.to_string(),
        ];

        assert!(matches!(
            validate_wallet_subaccounts(&addresses),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "wallet subaccounts",
                message,
            }) if message == format!("item 1 {expected_message}")
        ));
    }

    #[test]
    fn transaction_status_response_preserves_exact_data_and_hash_binding() {
        let hash = [0xab; 32];
        let body = format!(
            "{{\"code\":200,\"msg\":\"success\",\"fail\":false,\"data\":{{\"tx_hash\":\"0x{}\",\"confirmation\":\"best\",\"order_id\":\"42\",\"extra\":9.550000000000001e-6}}}}",
            hex::encode(hash),
        );
        let response = DeepXRestResponse {
            status: StatusCode::OK,
            body: body.into_bytes(),
        };
        let status = decode_transaction_status_response(&response, hash).unwrap();
        assert_eq!(status.extrinsic_hash, hash);
        assert_eq!(status.order_id, Some(42));
        assert!(status.data.get().contains("9.550000000000001e-6"));
    }

    #[test]
    fn transaction_status_response_preserves_http_api_and_schema_errors() {
        let hash = [0xab; 32];
        let response = |status, body: String| DeepXRestResponse {
            status,
            body: body.into_bytes(),
        };
        assert!(matches!(
            decode_transaction_status_response(
                &response(StatusCode::SERVICE_UNAVAILABLE, "unavailable".to_string()),
                hash,
            ),
            Err(DeepXRestError::Http { status: 503, .. })
        ));
        assert!(matches!(
            decode_transaction_status_response(
                &response(
                    StatusCode::OK,
                    r#"{"code":10020,"msg":"not found","fail":true,"data":null}"#.to_string(),
                ),
                hash,
            ),
            Err(DeepXRestError::Api {
                code: DeepXApiCode::Api(10020),
                ..
            })
        ));
        assert!(matches!(
            decode_transaction_status_response(
                &response(
                    StatusCode::OK,
                    r#"{"code":200,"msg":"success","fail":false,"data":{"tx_hash":"0x0000000000000000000000000000000000000000000000000000000000000000","confirmation":"best","order_id":""}}"#.to_string(),
                ),
                hash,
            ),
            Err(DeepXRestError::InvalidTransactionStatus { .. })
        ));
    }

    #[rstest]
    #[case("owner")]
    #[case("pair")]
    #[case("pair-scope")]
    #[case("name")]
    #[case("order-id")]
    #[case("side")]
    #[case("price")]
    #[case("base")]
    #[case("base-remaining")]
    #[case("quote")]
    #[case("quote-remaining")]
    #[case("block")]
    #[case("enum")]
    #[case("cancel-reason")]
    #[case("cancel-height")]
    #[case("hash")]
    #[case("time")]
    fn spot_order_rejects_invalid_record_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXSpotOrderRecord>> =
            serde_json::from_str(SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE).unwrap();
        let mut order = response.data.items[0].clone();
        match mutation {
            "owner" => order.maker = "0x1111111111111111111111111111111111111111".to_string(),
            "pair" => order.pair = "0x01".to_string(),
            "pair-scope" => {}
            "name" => order.pair_name = "SOL/USDC".to_string(),
            "order-id" => order.order_id = "bad".to_string(),
            "side" => order.order_side = "Long".to_string(),
            "price" => order.price = Decimal::NEGATIVE_ONE,
            "base" => order.base_amount = Decimal::ZERO,
            "base-remaining" => order.base_remaining_amount = order.base_amount + Decimal::ONE,
            "quote" => order.quote_amount = Decimal::ZERO,
            "quote-remaining" => order.quote_remaining_amount = order.quote_amount + Decimal::ONE,
            "block" => order.block_number = 0,
            "enum" => order.status.clear(),
            "cancel-reason" => order.cancel_reason = Some(String::new()),
            "cancel-height" => order.cancel_height = Some(0),
            "hash" => order.tx_hash = "0x01".to_string(),
            "time" => order.create_time = "invalid".to_string(),
            _ => unreachable!(),
        }
        let requested_name = (mutation != "pair-scope").then_some("ETH/USDC");
        let requested_pair = (mutation == "pair-scope")
            .then_some("0x1111111111111111111111111111111111111111111111111111111111111111");
        assert!(matches!(
            validate_spot_order_record(
                "spot history orders",
                0,
                &order,
                "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c",
                requested_name,
                requested_pair,
                None,
            ),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot history orders",
                ..
            })
        ));
    }

    #[rstest]
    #[case("Open", "0.8849", "null", true)]
    #[case("PartiallyFilled", "0.4", "2456.05", true)]
    #[case("Filled", "0", "2456.05", false)]
    #[case("Canceled", "0.8849", "null", false)]
    #[case("Rejected", "0.8849", "null", false)]
    #[case("Expired", "0.8849", "null", false)]
    #[case("Unknown", "0.8849", "null", false)]
    #[case("Open", "0.4", "2456.05", false)]
    #[case("Open", "0.8849", "2456.05", false)]
    #[case("PartiallyFilled", "0.8849", "2456.05", false)]
    #[case("PartiallyFilled", "0", "2456.05", false)]
    #[case("PartiallyFilled", "0.4", "null", false)]
    fn spot_open_orders_require_active_status_and_remaining_quantity(
        #[case] status: &str,
        #[case] remaining: &str,
        #[case] average: &str,
        #[case] expected: bool,
    ) {
        let fixture: Value = serde_json::from_str(SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE).unwrap();
        let mut item = fixture["data"]["items"][0].clone();
        item["status"] = json!(status);
        item["baseRemainingAmount"] = serde_json::from_str(remaining).unwrap();
        item["avgFillPrice"] = serde_json::from_str(average).unwrap();
        let order: DeepXSpotOrderRecord = serde_json::from_value(item).unwrap();
        assert_eq!(validate_spot_open_order_state(0, &order).is_ok(), expected);
    }

    #[rstest]
    fn spot_history_orders_reject_duplicates_ordering_and_oversized_pages() {
        let mut fixture: Value =
            serde_json::from_str(SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE).unwrap();
        let page: DeepXAccountPage<DeepXSpotOrderRecord> =
            serde_json::from_value(fixture["data"].clone()).unwrap();
        let scope = |sort| SpotOrderScope {
            endpoint: "spot history orders",
            owner: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c",
            name: Some("ETH/USDC"),
            pair: None,
            side: None,
            sort,
            active: false,
        };
        validate_spot_order_page(&page, scope(DeepXAccountSortOrder::Descending)).unwrap();
        assert!(matches!(
            validate_spot_order_page(&page, scope(DeepXAccountSortOrder::Ascending)),
            Err(DeepXRestError::InvalidHistoryResponse { .. })
        ));

        fixture["data"]["items"][1]["orderId"] = fixture["data"]["items"][0]["orderId"].clone();
        let duplicate: DeepXAccountPage<DeepXSpotOrderRecord> =
            serde_json::from_value(fixture["data"].clone()).unwrap();
        assert!(matches!(
            validate_spot_order_page(&duplicate, scope(DeepXAccountSortOrder::Descending)),
            Err(DeepXRestError::InvalidHistoryResponse { .. })
        ));

        let raw: DeepXRawAccountPage =
            serde_json::from_str::<DeepXApiResponse<DeepXRawAccountPage>>(
                SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE,
            )
            .unwrap()
            .data;
        assert!(matches!(
            decode_account_history_page::<DeepXSpotOrderRecord>(
                raw,
                "spot history orders",
                Some(2)
            ),
            Err(DeepXRestError::InvalidHistoryResponse { .. })
        ));
    }

    #[rstest]
    fn spot_order_continuity_rejects_repeated_and_out_of_order_pages() {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXSpotOrderRecord>> =
            serde_json::from_str(SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE).unwrap();
        let page = response.data;
        let history_request = DeepXSpotHistoryOrdersRequest {
            subaccount: page.items[0].maker.clone(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(3),
        };
        let open_request = DeepXSpotOpenOrdersRequest {
            subaccount: history_request.subaccount.clone(),
            name: history_request.name.clone(),
            pair: None,
            order_side: None,
            cursor: None,
            sort: history_request.sort,
            page_size: history_request.page_size,
        };
        let mut repeated = page.clone();
        repeated.items.drain(..2);
        let mut continuity = DeepXSpotOrderContinuity::default();
        continuity.observe_open_page(&page, &open_request).unwrap();
        assert!(matches!(
            continuity.observe_open_page(&repeated, &open_request),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("repeats order ID"),
        ));

        let mut later = repeated;
        later.items[0].order_id = "1789627378656".to_string();
        later.items[0].create_time = "2099-01-01T00:00:00Z".to_string();
        let mut continuity = DeepXSpotOrderContinuity::default();
        continuity
            .observe_history_page(&page, &history_request)
            .unwrap();
        assert!(matches!(
            continuity.observe_history_page(&later, &history_request),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("requested time order"),
        ));
    }

    fn spot_account_trades_request() -> DeepXSpotAccountTradesRequest {
        DeepXSpotAccountTradesRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            order_id: None,
            order_side: None,
            name: Some("ETH/USDC".to_string()),
            pair: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(3),
        }
    }

    #[rstest]
    #[case("id")]
    #[case("order-id")]
    #[case("order-filter")]
    #[case("side")]
    #[case("pair")]
    #[case("pair-scope")]
    #[case("name")]
    #[case("price")]
    #[case("base")]
    #[case("quote")]
    #[case("taker")]
    #[case("time")]
    #[case("range")]
    #[case("ordering")]
    fn spot_account_trade_rejects_invalid_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXSpotAccountTradeRecord>> =
            serde_json::from_str(SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE).unwrap();
        let mut trade = response.data.items[0].clone();
        let mut request = spot_account_trades_request();
        match mutation {
            "id" => trade.id = 0,
            "order-id" => trade.order_id = "bad".to_string(),
            "order-filter" => request.order_id = Some("1".to_string()),
            "side" => trade.order_side = "Long".to_string(),
            "pair" => trade.pair = "0x01".to_string(),
            "pair-scope" => {
                request.name = None;
                request.pair = Some(format!("0x{}", "11".repeat(32)));
            }
            "name" => trade.pair_name = "SOL/USDC".to_string(),
            "price" => trade.price = Decimal::ZERO,
            "base" => trade.base_amount = Decimal::ZERO,
            "quote" => trade.quote_amount = Decimal::ZERO,
            "taker" => trade.taker.clear(),
            "time" => trade.created_at = "invalid".to_string(),
            "range" => request.start_ms = Some(1_789_627_350_354),
            "ordering" => {}
            _ => unreachable!(),
        }
        let mut previous = (mutation == "ordering").then(|| {
            "2026-09-17T06:42:29.353Z"
                .parse::<jiff::Timestamp>()
                .unwrap()
        });
        assert!(matches!(
            validate_spot_account_trade_record(0, &trade, &request, &mut previous),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot account trades",
                ..
            })
        ));
    }

    #[rstest]
    fn spot_account_trade_page_rejects_duplicate_ids() {
        let mut response: Value =
            serde_json::from_str(SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE).unwrap();
        let request = spot_account_trades_request();
        let original: DeepXAccountPage<DeepXSpotAccountTradeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        validate_spot_account_trades_page(&original, &request).unwrap();
        response["data"]["items"][1]["id"] = response["data"]["items"][0]["id"].clone();
        let page: DeepXAccountPage<DeepXSpotAccountTradeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        assert!(matches!(
            validate_spot_account_trades_page(&page, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot account trades",
                ..
            })
        ));
    }

    #[rstest]
    fn spot_account_trade_continuity_rejects_repeated_and_out_of_order_pages() {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXSpotAccountTradeRecord>> =
            serde_json::from_str(SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE).unwrap();
        let page = response.data;
        let request = spot_account_trades_request();
        let mut repeated = page.clone();
        repeated.items = vec![page.items.last().unwrap().clone()];
        let mut continuity = DeepXSpotAccountTradeContinuity::default();
        continuity.observe_page(&page, &request).unwrap();
        assert!(matches!(
            continuity.observe_page(&repeated, &request),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("repeats trade ID"),
        ));

        let mut later = repeated;
        later.items[0].id += 1;
        later.items[0].created_at = "2099-01-01T00:00:00Z".to_string();
        let mut continuity = DeepXSpotAccountTradeContinuity::default();
        continuity.observe_page(&page, &request).unwrap();
        assert!(matches!(
            continuity.observe_page(&later, &request),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("requested time order"),
        ));
    }

    fn perp_account_trades_request() -> DeepXPerpAccountTradesRequest {
        DeepXPerpAccountTradesRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            order_id: None,
            market_id: Some(3),
            is_long: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(10),
        }
    }

    #[rstest]
    #[case("id")]
    #[case("market")]
    #[case("order-id")]
    #[case("price")]
    #[case("size")]
    #[case("leverage")]
    #[case("taker")]
    #[case("direction")]
    #[case("created-at")]
    #[case("duplicate-id")]
    #[case("order")]
    fn perp_account_trades_page_rejects_invalid_records(#[case] mutation: &str) {
        let mut response: Value =
            serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        let request = perp_account_trades_request();
        let original: DeepXAccountPage<DeepXPerpAccountTradeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        validate_perp_account_trades_page(&original, &request).unwrap();
        match mutation {
            "id" => response["data"]["items"][0]["id"] = json!(0),
            "market" => response["data"]["items"][0]["marketId"] = json!(4),
            "order-id" => response["data"]["items"][0]["orderId"] = json!("01"),
            "price" => response["data"]["items"][0]["price"] = json!(0),
            "size" => response["data"]["items"][0]["size"] = json!(0),
            "leverage" => response["data"]["items"][0]["leverage"] = json!(0),
            "taker" => response["data"]["items"][0]["taker"] = json!(" "),
            "direction" => response["data"]["items"][0]["filledDirection"] = json!(" "),
            "created-at" => response["data"]["items"][0]["createdAt"] = json!("invalid"),
            "duplicate-id" => {
                response["data"]["items"][1]["id"] = response["data"]["items"][0]["id"].clone();
            }
            "order" => response["data"]["items"].as_array_mut().unwrap().swap(0, 1),
            _ => unreachable!(),
        }
        let page: DeepXAccountPage<DeepXPerpAccountTradeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        assert!(matches!(
            validate_perp_account_trades_page(&page, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp account trades",
                ..
            })
        ));
    }

    #[rstest]
    fn perp_account_trade_page_boundaries_preserve_requested_time_order() {
        let response: Value = serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        let page: DeepXAccountPage<DeepXPerpAccountTradeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        let newer = &page.items[0];
        let older = &page.items[1];
        let mut request = perp_account_trades_request();
        let mut previous = None;
        validate_perp_account_trade_record(0, newer, &request, &mut previous).unwrap();
        validate_perp_account_trade_record(0, older, &request, &mut previous).unwrap();
        let boundary = previous;
        assert!(matches!(
            validate_perp_account_trade_record(0, newer, &request, &mut previous),
            Err(DeepXRestError::InvalidHistoryResponse { .. })
        ));
        assert_eq!(previous, boundary);

        request.sort = DeepXAccountSortOrder::Ascending;
        previous = None;
        validate_perp_account_trade_record(0, older, &request, &mut previous).unwrap();
        validate_perp_account_trade_record(0, newer, &request, &mut previous).unwrap();
        assert!(matches!(
            validate_perp_account_trade_record(0, older, &request, &mut previous),
            Err(DeepXRestError::InvalidHistoryResponse { .. })
        ));
    }

    #[rstest]
    fn perp_account_trade_continuity_rejects_repeated_and_out_of_order_pages() {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpAccountTradeRecord>> =
            serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        let page = response.data;
        let request = perp_account_trades_request();
        let mut repeated = page.clone();
        repeated.items = vec![page.items.last().unwrap().clone()];
        let mut continuity = DeepXPerpAccountTradeContinuity::default();
        continuity.observe_page(&page, &request).unwrap();
        assert!(matches!(
            continuity.observe_page(&repeated, &request),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("repeats trade ID"),
        ));

        let mut later = repeated;
        later.items[0].id += 1;
        later.items[0].created_at = "2099-01-01T00:00:00Z".to_string();
        let mut continuity = DeepXPerpAccountTradeContinuity::default();
        continuity.observe_page(&page, &request).unwrap();
        assert!(matches!(
            continuity.observe_page(&later, &request),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("requested time order"),
        ));
    }

    #[rstest]
    #[case("owner")]
    #[case("market")]
    #[case("base-amount")]
    #[case("entry-price")]
    #[case("status")]
    #[case("updated-before-created")]
    #[case("closed-before-open")]
    #[case("duplicate-id")]
    fn perp_positions_page_rejects_invalid_records(#[case] mutation: &str) {
        let mut response: Value = serde_json::from_str(PERP_POSITIONS_ACCOUNT_RESPONSE).unwrap();
        let request = DeepXPerpPositionsRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_id: Some(3),
            only_closed: None,
            cursor: None,
            page_size: Some(10),
        };
        let original: DeepXAccountPage<DeepXPerpPositionRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        validate_perp_positions_page(&original, &request).unwrap();
        match mutation {
            "owner" => {
                response["data"]["items"][0]["owner"] =
                    json!("0x1111111111111111111111111111111111111111");
            }
            "market" => response["data"]["items"][0]["marketId"] = json!(4),
            "base-amount" => response["data"]["items"][0]["baseAssetAmount"] = json!(0),
            "entry-price" => response["data"]["items"][0]["entryPrice"] = json!(0),
            "status" => response["data"]["items"][0]["status"] = json!(" "),
            "updated-before-created" => {
                response["data"]["items"][0]["updatedAt"] = json!("2026-09-15T04:06:32Z");
            }
            "closed-before-open" => {
                response["data"]["items"][1]["closeTime"] = json!("2026-09-15T04:04:14Z");
            }
            "duplicate-id" => {
                response["data"]["items"][1]["id"] = response["data"]["items"][0]["id"].clone();
            }
            _ => unreachable!(),
        }
        let page: DeepXAccountPage<DeepXPerpPositionRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        assert!(matches!(
            validate_perp_positions_page(&page, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp positions",
                ..
            })
        ));
    }

    #[rstest]
    fn perp_position_continuity_rejects_repeated_id_across_pages() {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpPositionRecord>> =
            serde_json::from_str(PERP_POSITIONS_ACCOUNT_RESPONSE).unwrap();
        let page = response.data;
        let mut later = page.clone();
        later.items = vec![page.items.last().unwrap().clone()];
        let mut continuity = DeepXPerpPositionContinuity::default();
        continuity.observe_page(&page).unwrap();
        assert!(matches!(
            continuity.observe_page(&later),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp positions",
                message,
            }) if message.contains("repeats position ID"),
        ));
    }

    #[rstest]
    #[case("owner", json!("0x1111111111111111111111111111111111111111"))]
    #[case("market", json!(4))]
    #[case("positionSize", json!(0))]
    #[case("height", json!(0))]
    #[case("createdAt", json!("invalid"))]
    fn funding_fee_page_rejects_invalid_records(
        #[case] field: &str,
        #[case] value: serde_json::Value,
    ) {
        let mut response: Value = serde_json::from_str(PERP_FUNDING_FEES_ACCOUNT_RESPONSE).unwrap();
        let request = DeepXPerpFundingFeeRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_id: Some(3),
            start_ms: None,
            end_ms: None,
            cursor: None,
            page_size: Some(10),
        };
        let original: DeepXAccountPage<DeepXPerpFundingFeeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        validate_funding_fee_page(
            &original,
            "perp funding fees",
            Some(&request.subaccount),
            request.market_id,
            request.start_ms,
            request.end_ms,
        )
        .unwrap();
        response["data"]["items"][0][field] = value;
        let page: DeepXAccountPage<DeepXPerpFundingFeeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        assert!(matches!(
            validate_funding_fee_page(
                &page,
                "perp funding fees",
                Some(&request.subaccount),
                request.market_id,
                request.start_ms,
                request.end_ms,
            ),
            Err(DeepXRestError::InvalidHistoryResponse { .. })
        ));
    }

    #[rstest]
    fn funding_fee_page_rejects_duplicate_events() {
        let response: Value = serde_json::from_str(PERP_FUNDING_FEES_ACCOUNT_RESPONSE).unwrap();
        let mut page: DeepXAccountPage<DeepXPerpFundingFeeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        page.items.push(page.items[0].clone());
        assert!(matches!(
            validate_funding_fee_page(&page, "wallet funding fees", None, Some(3), None, None),
            Err(DeepXRestError::InvalidHistoryResponse { .. })
        ));
    }

    #[rstest]
    fn funding_fee_continuity_rejects_repeated_events_across_pages() {
        let response: Value = serde_json::from_str(PERP_FUNDING_FEES_ACCOUNT_RESPONSE).unwrap();
        let page: DeepXAccountPage<DeepXPerpFundingFeeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        let mut continuity = DeepXFundingFeeContinuity::default();
        continuity.observe_page("perp funding fees", &page).unwrap();
        assert!(matches!(
            continuity.observe_page("perp funding fees", &page),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp funding fees",
                message,
            }) if message.contains("repeats event identity"),
        ));
    }

    #[rstest]
    fn wallet_funding_fee_page_accepts_distinct_owner() {
        let response: Value = serde_json::from_str(PERP_FUNDING_FEES_ACCOUNT_RESPONSE).unwrap();
        let page: DeepXAccountPage<DeepXPerpFundingFeeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        validate_funding_fee_page(&page, "wallet funding fees", None, Some(3), None, None).unwrap();
        assert!(matches!(
            validate_funding_fee_page(
                &page,
                "perp funding fees",
                Some("0x1111111111111111111111111111111111111111"),
                Some(3),
                None,
                None,
            ),
            Err(DeepXRestError::InvalidHistoryResponse { .. })
        ));
    }

    #[rstest]
    fn perp_order_by_id_preserves_opaque_order_id_and_exact_query_keys() {
        validate_perp_order_by_id(
            "0x1111111111111111111111111111111111111111",
            3,
            "id&opaque=1",
        )
        .unwrap();
        let query = PerpOrderByIdQuery {
            user: "0x1111111111111111111111111111111111111111",
            market_id: 3,
            oid: "id&opaque=1",
        };
        assert_eq!(
            serde_json::to_value(query).unwrap(),
            json!({
                "user": "0x1111111111111111111111111111111111111111",
                "marketId": 3,
                "oid": "id&opaque=1",
            }),
        );
        assert!(validate_perp_order_by_id("0x1234", 3, "1").is_err());
        assert!(
            validate_perp_order_by_id("0x1111111111111111111111111111111111111111", 0, "1",)
                .is_err()
        );
    }

    #[rstest]
    fn perp_order_scope_keeps_wallet_zero_size_policy() {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
            serde_json::from_str(include_str!(
                "../test_data/http/testnet/perp_history_orders_account_market_3.json"
            ))
            .unwrap();
        let mut order = response.data.items[0].clone();
        let owner = order.owner.clone();
        let market_id = order.market_id;

        validate_perp_order_record_scope("perp history orders", 0, &order, &owner, Some(market_id))
            .unwrap();
        order.size = Decimal::ZERO;
        order.size_filled = Decimal::ZERO;
        order.size_remain = Decimal::ZERO;
        assert!(
            validate_perp_order_record_scope(
                "perp history orders",
                0,
                &order,
                &owner,
                Some(market_id),
            )
            .is_err()
        );
        validate_perp_wallet_order_record_scope(
            "perp wallet orders",
            0,
            &order,
            &owner,
            Some(market_id),
        )
        .unwrap();
        order.size = Decimal::NEGATIVE_ONE;
        assert!(
            validate_perp_wallet_order_record_scope(
                "perp wallet orders",
                0,
                &order,
                &owner,
                Some(market_id),
            )
            .is_err()
        );
    }

    #[rstest]
    #[case("Open", "0", "0.3", None, true)]
    #[case("PartiallyFilled", "0.1", "0.2", Some("2499.6"), true)]
    #[case("Filled", "0.3", "0", Some("2499.6"), false)]
    #[case("Canceled", "0", "0.3", None, false)]
    #[case("Rejected", "0", "0.3", None, false)]
    #[case("Expired", "0", "0.3", None, false)]
    #[case("Unknown", "0", "0.3", None, false)]
    #[case("Open", "0.1", "0.2", None, false)]
    #[case("Open", "0.1", "0.2", Some("2499.6"), false)]
    #[case("Open", "0", "0.3", Some("2499.6"), false)]
    #[case("PartiallyFilled", "0.3", "0", Some("2499.6"), false)]
    #[case("PartiallyFilled", "0.1", "0.3", Some("2499.6"), false)]
    #[case("PartiallyFilled", "0.1", "0.1", Some("2499.6"), false)]
    #[case("PartiallyFilled", "0.1", "0.2", None, false)]
    fn perp_open_orders_require_active_state_and_exact_size_accounting(
        #[case] status: &str,
        #[case] filled: &str,
        #[case] remaining: &str,
        #[case] average: Option<&str>,
        #[case] expected: bool,
    ) {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
            serde_json::from_str(include_str!(
                "../test_data/http/testnet/perp_history_orders_account_market_3.json"
            ))
            .unwrap();
        let mut order = response.data.items[0].clone();
        order.status = status.to_string();
        order.size_filled = filled.parse().unwrap();
        order.size_remain = remaining.parse().unwrap();
        order.avg_fill_price = average.map(|value| value.parse().unwrap());
        assert_eq!(validate_perp_open_order_state(0, &order).is_ok(), expected);
    }

    #[rstest]
    fn perp_order_pages_validate_scope_sequence_and_state() {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
            serde_json::from_str(include_str!(
                "../test_data/http/testnet/perp_history_orders_account_market_3.json"
            ))
            .unwrap();
        let mut page = response.data;
        let owner = page.items[0].owner.clone();
        validate_perp_order_page(
            &page,
            "perp history orders",
            &owner,
            Some(3),
            None,
            Some(DeepXAccountSortOrder::Descending),
            PerpOrderPageKind::History,
        )
        .unwrap();

        page.items.reverse();
        assert!(
            validate_perp_order_page(
                &page,
                "perp history orders",
                &owner,
                Some(3),
                None,
                Some(DeepXAccountSortOrder::Descending),
                PerpOrderPageKind::History,
            )
            .is_err()
        );
        validate_perp_order_page(
            &page,
            "perp position orders",
            &owner,
            None,
            None,
            None,
            PerpOrderPageKind::Position,
        )
        .unwrap();
        page.items[1].order_id = page.items[0].order_id.clone();
        assert!(
            validate_perp_order_page(
                &page,
                "perp position orders",
                &owner,
                None,
                None,
                None,
                PerpOrderPageKind::Position,
            )
            .is_err()
        );

        page.items.truncate(1);
        let order = &mut page.items[0];
        order.status = "Open".to_string();
        order.size_filled = Decimal::ZERO;
        order.size_remain = order.size;
        order.avg_fill_price = None;
        validate_perp_order_page(
            &page,
            "perp open orders",
            &owner,
            Some(3),
            None,
            Some(DeepXAccountSortOrder::Descending),
            PerpOrderPageKind::Open,
        )
        .unwrap();
        page.items[0].status = "Filled".to_string();
        assert!(
            validate_perp_order_page(
                &page,
                "perp open orders",
                &owner,
                Some(3),
                None,
                Some(DeepXAccountSortOrder::Descending),
                PerpOrderPageKind::Open,
            )
            .is_err()
        );
    }

    #[rstest]
    fn perp_position_order_continuity_rejects_repeated_boundary_identity() {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
            serde_json::from_str(include_str!(
                "../test_data/http/testnet/perp_history_orders_account_market_3.json"
            ))
            .unwrap();
        let mut page = response.data;
        page.items.reverse();
        let request = DeepXPerpPositionOrdersRequest {
            subaccount: page.items[0].owner.clone(),
            position_id: 1,
            cursor: None,
            page_size: Some(2),
        };
        let mut later = page.clone();
        later.items.drain(..1);
        page.items.truncate(2);
        let mut continuity = DeepXPerpPositionOrderContinuity::default();
        continuity.observe_page(&page, &request).unwrap();
        assert!(matches!(
            continuity.observe_page(&later, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp position orders",
                message,
            }) if message.contains("ascending order ID/side order"),
        ));
    }

    #[rstest]
    fn perp_history_order_continuity_rejects_repeated_and_out_of_order_pages() {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
            serde_json::from_str(include_str!(
                "../test_data/http/testnet/perp_history_orders_account_market_3.json"
            ))
            .unwrap();
        let page = response.data;
        let request = DeepXPerpHistoryOrdersRequest {
            subaccount: page.items[0].owner.clone(),
            market_id: Some(page.items[0].market_id),
            cursor: None,
            page_size: Some(3),
            sort: DeepXAccountSortOrder::Descending,
        };
        let mut repeated = page.clone();
        repeated.items.drain(..2);
        let mut continuity = DeepXPerpOrderContinuity::default();
        continuity.observe_history_page(&page, &request).unwrap();
        assert!(matches!(
            continuity.observe_history_page(&repeated, &request),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("repeats order ID"),
        ));

        let mut later = repeated;
        later.items[0].order_id = "1789627378656".to_string();
        later.items[0].create_time = "2099-01-01T00:00:00Z".to_string();
        later.items[0].updated_time = Some("2099-01-01T00:00:00Z".to_string());
        let mut continuity = DeepXPerpOrderContinuity::default();
        continuity.observe_history_page(&page, &request).unwrap();
        assert!(matches!(
            continuity.observe_history_page(&later, &request),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("requested time order"),
        ));
    }

    #[rstest]
    fn perp_open_order_continuity_rejects_side_mismatch_and_repeated_id() {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
            serde_json::from_str(include_str!(
                "../test_data/http/testnet/perp_history_orders_account_market_3.json"
            ))
            .unwrap();
        let mut page = response.data;
        page.items.truncate(1);
        let order = &mut page.items[0];
        order.status = "Open".to_string();
        order.size_filled = Decimal::ZERO;
        order.size_remain = order.size;
        order.avg_fill_price = None;
        let mut request = DeepXPerpOpenOrdersRequest {
            subaccount: order.owner.clone(),
            market_id: Some(order.market_id),
            is_long: Some(!order.is_long),
            cursor: None,
            page_size: Some(1),
            sort: DeepXAccountSortOrder::Descending,
        };
        assert!(matches!(
            DeepXPerpOrderContinuity::default().observe_open_page(&page, &request),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("requested side"),
        ));
        request.is_long = None;
        let mut continuity = DeepXPerpOrderContinuity::default();
        continuity.observe_open_page(&page, &request).unwrap();
        assert!(matches!(
            continuity.observe_open_page(&page, &request),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("repeats order ID"),
        ));

        let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
            serde_json::from_str(include_str!(
                "../test_data/http/testnet/perp_history_orders_account_market_3.json"
            ))
            .unwrap();
        let mut reversed = response.data;
        for order in &mut reversed.items {
            order.status = "Open".to_string();
            order.size_filled = Decimal::ZERO;
            order.size_remain = order.size;
            order.avg_fill_price = None;
        }
        reversed.items.reverse();
        request.page_size = Some(3);
        assert!(matches!(
            DeepXPerpOrderContinuity::default().observe_open_page(&reversed, &request),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("requested time order"),
        ));
    }

    #[rstest]
    fn spot_wallet_trade_record_validates_group_and_time_order() {
        let response: Value = serde_json::from_str(include_str!(
            "../test_data/http/testnet/spot_wallet_trades_eth_usdc_page_1.json"
        ))
        .unwrap();
        let trade: DeepXSpotAccountTradeRecord = serde_json::from_value(
            response["data"][0]["subaccounts"][0]["trades"]["items"][0].clone(),
        )
        .unwrap();
        let request = DeepXSpotWalletTradesRequest {
            wallet: "0x1111111111111111111111111111111111111111".to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        let pair = decode_spot_pair_identity(&trade.pair).unwrap();
        let created =
            validate_spot_wallet_trade_record(0, &trade, &request, "ETH/USDC", pair, None).unwrap();
        assert!(
            validate_spot_wallet_trade_record(0, &trade, &request, "SOL/USDC", pair, None,)
                .is_err()
        );
        let earlier: DeepXSpotAccountTradeRecord = serde_json::from_value(
            response["data"][0]["subaccounts"][0]["trades"]["items"][1].clone(),
        )
        .unwrap();
        let earlier = earlier.created_at.parse::<jiff::Timestamp>().unwrap();
        assert!(earlier < created);
        assert!(
            validate_spot_wallet_trade_record(
                0,
                &trade,
                &request,
                "ETH/USDC",
                pair,
                Some(&earlier),
            )
            .is_err()
        );
    }

    #[rstest]
    fn perp_wallet_trade_record_preserves_zero_size_policy() {
        let response: Value = serde_json::from_str(include_str!(
            "../test_data/http/testnet/perp_wallet_trades_page.json"
        ))
        .unwrap();
        let mut trade: DeepXPerpAccountTradeRecord = serde_json::from_value(
            response["data"][1]["subaccounts"][0]["trades"]["items"][2].clone(),
        )
        .unwrap();
        let request = DeepXPerpWalletTradesRequest {
            wallet: "0x1111111111111111111111111111111111111111".to_string(),
            market_name: None,
            market_id: Some(4),
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        assert_eq!(trade.size, Decimal::ZERO);
        validate_perp_wallet_trade_record(0, &trade, &request, 4, &mut None).unwrap();
        trade.size = Decimal::NEGATIVE_ONE;
        assert!(validate_perp_wallet_trade_record(0, &trade, &request, 4, &mut None).is_err());
    }

    #[rstest]
    fn perp_wallet_trade_continuity_rejects_repeated_and_out_of_order_pages() {
        let response: Value = serde_json::from_str(include_str!(
            "../test_data/http/testnet/perp_wallet_trades_page.json"
        ))
        .unwrap();
        let page: DeepXAccountPage<DeepXPerpAccountTradeRecord> =
            serde_json::from_value(response["data"][1]["subaccounts"][0]["trades"].clone())
                .unwrap();
        let request = DeepXPerpWalletTradesRequest {
            wallet: "0x1111111111111111111111111111111111111111".to_string(),
            market_name: None,
            market_id: Some(4),
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        let mut repeated = page.clone();
        repeated.items = vec![page.items.last().unwrap().clone()];
        let mut continuity = DeepXPerpWalletTradeContinuity::default();
        continuity.observe_page(&page, &request, 4).unwrap();
        assert!(matches!(
            continuity.observe_page(&repeated, &request, 4),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("repeats trade ID"),
        ));

        let mut later = repeated;
        later.items[0].id += 1;
        later.items[0].created_at = "2099-01-01T00:00:00Z".to_string();
        let mut continuity = DeepXPerpWalletTradeContinuity::default();
        continuity.observe_page(&page, &request, 4).unwrap();
        assert!(matches!(
            continuity.observe_page(&later, &request, 4),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("requested time order"),
        ));
    }

    #[rstest]
    fn raw_account_page_preserves_numeric_lexemes() {
        let page: DeepXRawAccountPage = serde_json::from_str(
            r#"{"items":[{"size":0.000000000000000001}],"nextCursor":null,"hasNext":false}"#,
        )
        .unwrap();
        assert_eq!(page.items[0].get(), r#"{"size":0.000000000000000001}"#);
        assert!(!page.has_next);
    }

    #[rstest]
    fn typed_account_page_preserves_exact_records_and_pagination() {
        let response: DeepXApiResponse<DeepXRawAccountPage> = serde_json::from_str(include_str!(
            "../test_data/http/testnet/perp_history_orders_account_market_3.json"
        ))
        .unwrap();
        let raw = response.data;
        let expected_cursor = raw.next_cursor.clone();
        let expected_has_next = raw.has_next;
        let page: DeepXAccountPage<DeepXPerpOrderRecord> = raw.decode().unwrap();

        assert_eq!(page.items[0].order_id, "1789445193480");
        assert_eq!(page.items[0].price, Decimal::new(2_523_683_573, 6));
        assert_eq!(page.next_cursor, expected_cursor);
        assert_eq!(page.has_next, expected_has_next);

        let malformed: DeepXRawAccountPage = serde_json::from_str(
            r#"{"items":[{"orderId":"1"}],"nextCursor":"next","hasNext":true}"#,
        )
        .unwrap();
        assert!(malformed.decode::<DeepXPerpOrderRecord>().is_err());
    }

    #[rstest]
    fn captured_perp_account_pages_apply_scope_filters_and_exact_pnl() {
        let orders: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
            serde_json::from_str(include_str!(
                "../test_data/http/testnet/perp_history_orders_account_market_3.json"
            ))
            .unwrap();
        assert_eq!(orders.data.items.len(), 3);
        assert!(matches!(
            validate_perp_order_record_scope(
                "perp history orders",
                0,
                &orders.data.items[0],
                "0x1111111111111111111111111111111111111111",
                Some(3),
            ),
            Err(DeepXRestError::InvalidHistoryResponse { .. })
        ));

        let trades: DeepXApiResponse<DeepXAccountPage<DeepXPerpAccountTradeRecord>> =
            serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        assert_eq!(trades.data.items.len(), 3);
        let mut request = perp_account_trades_request();
        request.start_ms = Some(1_789_445_193_758);
        assert!(matches!(
            validate_perp_account_trades_page(&trades.data, &request),
            Err(DeepXRestError::InvalidHistoryResponse { .. })
        ));

        let positions: DeepXApiResponse<DeepXAccountPage<DeepXPerpPositionRecord>> =
            serde_json::from_str(PERP_POSITIONS_ACCOUNT_RESPONSE).unwrap();
        assert_eq!(positions.data.items.len(), 2);
        assert_eq!(
            positions.data.items[0].pnl,
            Decimal::new(-4_018_472_999_999_176, 16)
        );
        let request = DeepXPerpPositionsRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_id: Some(3),
            only_closed: Some(true),
            cursor: None,
            page_size: Some(10),
        };
        assert!(matches!(
            validate_perp_positions_page(&positions.data, &request),
            Err(DeepXRestError::InvalidHistoryResponse { .. })
        ));
    }

    #[rstest]
    fn account_history_decode_identifies_invalid_item_and_page_size() {
        let raw = r#"{"items":[{"orderId":"1"}],"nextCursor":null,"hasNext":false}"#;
        let malformed: DeepXRawAccountPage = serde_json::from_str(raw).unwrap();
        assert!(matches!(
            decode_account_history_page::<DeepXPerpOrderRecord>(
                malformed,
                "perp history orders",
                Some(1),
            ),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp history orders",
                message,
            }) if message.contains("item 0 failed decoding")
        ));
        let oversized: DeepXRawAccountPage = serde_json::from_str(raw).unwrap();
        assert!(matches!(
            decode_account_history_page::<DeepXPerpOrderRecord>(
                oversized,
                "perp history orders",
                Some(0),
            ),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp history orders",
                message,
            }) if message == "page exceeds requested page size"
        ));
    }

    #[rstest]
    #[case("id")]
    #[case("height")]
    #[case("asset")]
    #[case("time")]
    #[case("type")]
    #[case("tx-identity")]
    #[case("source")]
    #[case("position")]
    #[case("duplicate")]
    #[case("page-size")]
    fn balance_changes_reject_invalid_record_and_page_semantics(#[case] mutation: &str) {
        let mut response: Value = serde_json::from_str(include_str!(
            "../test_data/http/testnet/wallet_balance_changes_page_2.json"
        ))
        .unwrap();
        let mut request = DeepXBalanceChangesRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            start_ms: Some(1_789_498_000_000),
            end_ms: Some(1_789_523_000_000),
            change_types: vec![
                crate::query::DeepXBalanceChangeType::FundingFee,
                crate::query::DeepXBalanceChangeType::Settlement,
            ],
            cursor: None,
            page_size: Some(2),
        };
        let original: DeepXAccountPage<DeepXBalanceChangeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        validate_balance_changes_page(&original, &request).unwrap();
        match mutation {
            "id" => response["data"]["items"][0]["id"] = json!(0),
            "height" => response["data"]["items"][0]["height"] = json!(0),
            "asset" => response["data"]["items"][0]["asset"] = json!(" "),
            "time" => response["data"]["items"][0]["time"] = json!(1),
            "type" => request.change_types = vec![crate::query::DeepXBalanceChangeType::Deposit],
            "tx-identity" => response["data"]["items"][0]["txHashType"] = json!("EXTRINSIC_HASH"),
            "source" => response["data"]["items"][0]["from"] = json!("invalid"),
            "position" => response["data"]["items"][0]["position"]["owner"] = json!("invalid"),
            "duplicate" => {
                response["data"]["items"][1]["id"] = response["data"]["items"][0]["id"].clone();
            }
            "page-size" => request.page_size = Some(1),
            _ => unreachable!(),
        }
        let page: DeepXAccountPage<DeepXBalanceChangeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        let error = if mutation == "page-size" {
            let raw: DeepXRawAccountPage =
                serde_json::from_value(response["data"].clone()).unwrap();
            decode_account_history_page::<DeepXBalanceChangeRecord>(
                raw,
                "balance changes",
                request.page_size,
            )
            .unwrap_err()
        } else {
            validate_balance_changes_page(&page, &request).unwrap_err()
        };
        assert!(matches!(
            error,
            DeepXRestError::InvalidHistoryResponse {
                endpoint: "balance changes",
                ..
            }
        ));
    }

    #[rstest]
    fn balance_change_continuity_rejects_repeated_id_across_pages() {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXBalanceChangeRecord>> =
            serde_json::from_str(include_str!(
                "../test_data/http/testnet/wallet_balance_changes_page_2.json"
            ))
            .unwrap();
        let page = response.data;
        let mut later = page.clone();
        later.items = vec![page.items.last().unwrap().clone()];
        let mut continuity = DeepXBalanceChangeContinuity::default();
        continuity.observe_page(&page).unwrap();
        assert!(matches!(
            continuity.observe_page(&later),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "balance changes",
                message,
            }) if message.contains("repeats record ID"),
        ));
    }

    #[rstest]
    fn order_by_tx_query_requires_exact_extrinsic_hash() {
        let hash = format!("0x{}", "ab".repeat(32));
        validate_order_tx_hash(&hash, "perp-order-by-tx").unwrap();
        assert_eq!(
            serde_json::to_value(OrderByTxQuery { tx_hash: &hash }).unwrap(),
            json!({"txHash": hash}),
        );
        assert!(validate_order_tx_hash("0x1234", "spot-order-by-tx").is_err());
        assert!(
            validate_order_tx_hash(&format!("0x{}", "zz".repeat(32)), "spot-order-by-tx").is_err()
        );
    }

    #[rstest]
    #[case(DeepXRestAction::PerpPlace, "Perp", "PlaceOrder")]
    #[case(DeepXRestAction::PerpCancel, "Perp", "CancelOrder")]
    #[case(DeepXRestAction::PerpClose, "Perp", "ClosePosition")]
    #[case(
        DeepXRestAction::PerpProfitAndLossPoint,
        "Perp",
        "SetProfitAndLossPoint"
    )]
    #[case(DeepXRestAction::SpotPlace, "Spot", "PlaceOrder")]
    #[case(DeepXRestAction::SpotCancel, "Spot", "CancelOrder")]
    fn encodes_documented_transaction_action(
        #[case] action: DeepXRestAction,
        #[case] market_type: &str,
        #[case] action_name: &str,
    ) {
        let body = encode_transaction_request(action, &[0x01, 0xab]).unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["marketType"], market_type);
        assert_eq!(body["action"], action_name);
        assert_eq!(body["signedExtrinsic"], "0x01ab");
    }

    #[rstest]
    fn decodes_acknowledgement_without_discarding_action_data() {
        let data = json!({"tx_hash": "0x1234", "order_id": "18446744073709551615"});
        let body = serde_json::to_vec(&json!({
            "code": 200, "fail": false, "msg": "success", "data": data,
        }))
        .unwrap();
        let acknowledgement = decode_transaction_acknowledgement(&body).unwrap();
        assert_eq!(acknowledgement.tx_hash, "0x1234");
        assert_eq!(acknowledgement.data, data);
    }

    #[rstest]
    #[case(json!({"code": 500, "fail": true, "msg": "rejected", "data": null}))]
    #[case(json!({"code": 200, "fail": false, "msg": "success", "data": {}}))]
    fn rejects_failed_or_hashless_acknowledgements(#[case] response: Value) {
        let body = serde_json::to_vec(&response).unwrap();
        assert!(decode_transaction_acknowledgement(&body).is_err());
    }
}
