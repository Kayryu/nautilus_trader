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

//! Typed and raw account state and directory REST queries.

use std::collections::{HashMap, HashSet};

use alloy::primitives::U256;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::value::RawValue;

use super::{
    DeepXAccountPage, DeepXRawAccountPage, DeepXRestError, DeepXRestTransport,
    decode_account_history_page, decode_spot_pair_identity, is_account_address,
    validate_perp_wallet_order_record_scope, validate_perp_wallet_trade_record,
    validate_spot_order_record, validate_spot_wallet_trade_record,
};
use crate::{
    pagination::validate_cursor_page,
    query::{
        DeepXAccountSortOrder, DeepXAllSubaccountsRequest, DeepXHourlyUnsettledFundingRequest,
        DeepXPerpWalletOrdersRequest, DeepXPerpWalletTradesRequest, DeepXSpotWalletOrdersRequest,
        DeepXSpotWalletTradesRequest,
    },
    rest::models::{
        DeepXDelegateAccount, DeepXDelegateWallets, DeepXHourlyUnsettledFundingRecord,
        DeepXInvitationStatus, DeepXInvitationStatusWire, DeepXPerpWalletOrderMarket,
        DeepXPerpWalletOrdersPage, DeepXPerpWalletTradeMarket, DeepXSpotWalletOrderMarket,
        DeepXSpotWalletOrdersPage, DeepXSpotWalletTradeMarket, DeepXSpotWalletTradesPage,
        DeepXSubaccountBalances, DeepXSubaccountDirectoryRecord, DeepXSubaccountEquity,
        DeepXSubaccountMarginRatio, DeepXSubaccountProfile, DeepXUserStats,
        DeepXWalletDelegateAccounts, validate_subaccount_balances,
    },
};

const ALL_SUBACCOUNTS_PATH: &str = "/internal/v1/account/subaccounts/all";
const SUBACCOUNT_INFO_PATH: &str = "/internal/v1/account/subaccount-info";
const SUBACCOUNT_BALANCES_PATH: &str = "/internal/v1/account/balances";
const SUBACCOUNT_EQUITY_PATH: &str = "/internal/v1/account/equity";
const SUBACCOUNT_MARGIN_RATIO_PATH: &str = "/internal/v1/account/margin-ratio";
const DELEGATE_ACCOUNTS_PATH: &str = "/internal/v1/account/delegate-accounts";
const DELEGATOR_ACCOUNTS_PATH: &str = "/internal/v1/account/delegator-accounts";
const SPOT_WALLET_ORDERS_PATH: &str = "/internal/v1/account/spot/orders-by-wallet";
const SPOT_WALLET_TRADES_PATH: &str = "/internal/v1/account/spot/trades-by-wallet";
const PERP_WALLET_ORDERS_PATH: &str = "/internal/v1/account/perp/orders-by-wallet";
const PERP_WALLET_TRADES_PATH: &str = "/internal/v1/account/perp/trades-by-wallet";
const HOURLY_UNSETTLED_FUNDING_PATH: &str = "/internal/v1/account/hourly-unsettled-funding";
const INVITATION_CHECK_PATH: &str = "/internal/v1/invitation/check";
const INVITATION_CODE_PATH: &str = "/internal/v1/invitation/invitation-code";
const USER_STATS_PATH: &str = "/internal/v1/account/user-stats";

#[derive(Serialize)]
struct AddressQuery<'a> {
    address: &'a str,
}

#[derive(Serialize)]
struct SubaccountQuery<'a> {
    subaccount: &'a str,
}

impl DeepXRestTransport {
    /// Returns one validated typed page of the global subaccount directory.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, decoding, or invalid-directory error.
    pub async fn get_all_subaccounts(
        &self,
        base_url: &str,
        request: &DeepXAllSubaccountsRequest,
    ) -> Result<DeepXAccountPage<DeepXSubaccountDirectoryRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_all_subaccounts_raw(base_url, request).await?,
            "all subaccounts",
            request.page_size,
        )?;
        validate_all_subaccounts_page(&page, request)?;
        Ok(page)
    }

    /// Returns the typed invitation status for a wallet.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or mismatched-wallet error.
    pub async fn get_invitation_status(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<DeepXInvitationStatus, DeepXRestError> {
        let raw = self.get_invitation_status_raw(base_url, address).await?;
        let status = serde_json::from_str(raw.get())?;
        invitation_status_for_wallet(status, address)
    }

    /// Returns invitation codes for a wallet.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-code error.
    pub async fn get_invitation_codes(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Vec<String>, DeepXRestError> {
        let raw = self.get_invitation_codes_raw(base_url, address).await?;
        let codes: Vec<String> = serde_json::from_str(raw.get())?;
        validate_invitation_codes(&codes)?;
        Ok(codes)
    }

    /// Returns validated Spot wallet orders grouped by market and subaccount.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-page response error.
    pub async fn get_spot_wallet_orders(
        &self,
        base_url: &str,
        request: &DeepXSpotWalletOrdersRequest,
    ) -> Result<DeepXSpotWalletOrdersPage, DeepXRestError> {
        let raw = self.get_spot_wallet_orders_raw(base_url, request).await?;
        let markets = serde_json::from_str(raw.get())?;
        let page = spot_wallet_orders_page(markets)?;
        validate_spot_wallet_order_page(&page, request)?;
        Ok(page)
    }

    /// Returns validated Spot wallet trades grouped by market and subaccount.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-page response error.
    pub async fn get_spot_wallet_trades(
        &self,
        base_url: &str,
        request: &DeepXSpotWalletTradesRequest,
    ) -> Result<DeepXSpotWalletTradesPage, DeepXRestError> {
        let raw = self.get_spot_wallet_trades_raw(base_url, request).await?;
        let markets = serde_json::from_str(raw.get())?;
        let page = spot_wallet_trades_page(markets)?;
        validate_spot_wallet_trade_page(&page, request)?;
        Ok(page)
    }

    /// Returns validated perpetual wallet orders grouped by market and subaccount.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-page response error.
    pub async fn get_perp_wallet_orders(
        &self,
        base_url: &str,
        request: &DeepXPerpWalletOrdersRequest,
    ) -> Result<DeepXPerpWalletOrdersPage, DeepXRestError> {
        let raw = self.get_perp_wallet_orders_raw(base_url, request).await?;
        let markets = serde_json::from_str(raw.get())?;
        let page = perp_wallet_orders_page(markets)?;
        validate_perp_wallet_order_page(&page, request)?;
        Ok(page)
    }

    /// Returns validated perpetual wallet trades grouped by market and subaccount.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-group response error.
    pub async fn get_perp_wallet_trades(
        &self,
        base_url: &str,
        request: &DeepXPerpWalletTradesRequest,
    ) -> Result<Vec<DeepXPerpWalletTradeMarket>, DeepXRestError> {
        let raw = self.get_perp_wallet_trades_raw(base_url, request).await?;
        let groups: Vec<DeepXPerpWalletTradeMarket> = serde_json::from_str(raw.get())?;
        validate_perp_wallet_trade_groups(&groups, request)?;
        Ok(groups)
    }

    /// Returns validated exact hourly unsettled funding records.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-record error.
    pub async fn get_hourly_unsettled_funding(
        &self,
        base_url: &str,
        request: &DeepXHourlyUnsettledFundingRequest,
    ) -> Result<Vec<DeepXHourlyUnsettledFundingRecord>, DeepXRestError> {
        let raw = self
            .get_hourly_unsettled_funding_raw(base_url, request)
            .await?;
        let records: Vec<DeepXHourlyUnsettledFundingRecord> = serde_json::from_str(raw.get())?;
        validate_hourly_unsettled_funding_page(&records, request)?;
        Ok(records)
    }

    /// Returns typed delegate configurations bound to the queried wallet.
    ///
    /// This mutable REST observation does not establish current chain authorization.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-directory error.
    pub async fn get_delegate_accounts(
        &self,
        base_url: &str,
        wallet: &str,
    ) -> Result<DeepXWalletDelegateAccounts, DeepXRestError> {
        let raw = self.get_delegate_accounts_raw(base_url, wallet).await?;
        let accounts: Vec<DeepXDelegateAccount> = serde_json::from_str(raw.get())?;
        validate_delegate_accounts(&accounts)?;
        Ok(DeepXWalletDelegateAccounts::new(
            wallet.to_string(),
            accounts,
        ))
    }

    /// Returns typed wallet addresses bound to the queried delegate.
    ///
    /// This mutable REST observation does not establish current chain authorization.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-directory error.
    pub async fn get_delegator_accounts(
        &self,
        base_url: &str,
        delegate: &str,
    ) -> Result<DeepXDelegateWallets, DeepXRestError> {
        let raw = self.get_delegator_accounts_raw(base_url, delegate).await?;
        let wallets: Vec<String> = serde_json::from_str(raw.get())?;
        validate_delegator_wallets(&wallets)?;
        Ok(DeepXDelegateWallets::new(delegate.to_string(), wallets))
    }

    /// Returns typed wallet statistics.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-statistics error.
    pub async fn get_user_stats(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<DeepXUserStats, DeepXRestError> {
        let raw = self.get_user_stats_raw(base_url, address).await?;
        let stats = serde_json::from_str(raw.get())?;
        validate_user_stats(&stats)?;
        Ok(stats)
    }

    /// Returns the typed subaccount profile.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-profile error.
    pub async fn get_subaccount_info(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<DeepXSubaccountProfile, DeepXRestError> {
        let raw = self.get_subaccount_info_raw(base_url, address).await?;
        let profile = serde_json::from_str(raw.get())?;
        validate_subaccount_profile(&profile, address)?;
        Ok(profile)
    }

    /// Returns typed subaccount balances with exact decimal values.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-balance error.
    pub async fn get_subaccount_balances(
        &self,
        base_url: &str,
        subaccount: &str,
    ) -> Result<DeepXSubaccountBalances, DeepXRestError> {
        let raw = self
            .get_subaccount_balances_raw(base_url, subaccount)
            .await?;
        let balances = serde_json::from_str(raw.get())?;
        validate_subaccount_balances(&balances, subaccount).map_err(|e| {
            DeepXRestError::InvalidAccountResponse {
                endpoint: "subaccount balances",
                message: e.to_string(),
            }
        })?;
        Ok(balances)
    }

    /// Returns typed subaccount equity with exact decimal values.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-equity error.
    pub async fn get_subaccount_equity(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<DeepXSubaccountEquity, DeepXRestError> {
        let raw = self.get_subaccount_equity_raw(base_url, address).await?;
        let equity = serde_json::from_str(raw.get())?;
        validate_subaccount_equity(&equity, address)?;
        Ok(equity)
    }

    /// Returns the typed subaccount margin ratio with exact decimal values.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-margin error.
    pub async fn get_subaccount_margin_ratio(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<DeepXSubaccountMarginRatio, DeepXRestError> {
        let raw = self
            .get_subaccount_margin_ratio_raw(base_url, address)
            .await?;
        let margin = serde_json::from_str(raw.get())?;
        validate_subaccount_margin_ratio(&margin)?;
        Ok(margin)
    }

    /// Returns uninterpreted wallet statistics.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_user_stats_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("user stats", address)?;
        self.get_api_data_with_query(base_url, USER_STATS_PATH, &AddressQuery { address })
            .await
    }

    /// Returns the uninterpreted invitation status for a wallet.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_invitation_status_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("invitation status", address)?;
        self.get_api_data_with_query(base_url, INVITATION_CHECK_PATH, &AddressQuery { address })
            .await
    }

    /// Returns uninterpreted invitation codes for a wallet.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_invitation_codes_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("invitation codes", address)?;
        self.get_api_data_with_query(base_url, INVITATION_CODE_PATH, &AddressQuery { address })
            .await
    }

    /// Returns uninterpreted grouped spot wallet orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_wallet_orders_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotWalletOrdersRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_WALLET_ORDERS_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted grouped spot wallet trades.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_wallet_trades_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotWalletTradesRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_WALLET_TRADES_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted grouped perpetual wallet orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_wallet_orders_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpWalletOrdersRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_WALLET_ORDERS_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted grouped perpetual wallet trades.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_wallet_trades_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpWalletTradesRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_WALLET_TRADES_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted hourly unsettled funding records.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_hourly_unsettled_funding_raw(
        &self,
        base_url: &str,
        request: &DeepXHourlyUnsettledFundingRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, HOURLY_UNSETTLED_FUNDING_PATH, &request.as_query())
            .await
    }

    /// Returns one raw page of the global subaccount directory.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_all_subaccounts_raw(
        &self,
        base_url: &str,
        request: &DeepXAllSubaccountsRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            ALL_SUBACCOUNTS_PATH,
            &request.as_query(),
            "all subaccounts",
        )
        .await
    }

    /// Returns an uninterpreted subaccount profile.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_subaccount_info_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("subaccount info", address)?;
        self.get_api_data_with_query(base_url, SUBACCOUNT_INFO_PATH, &AddressQuery { address })
            .await
    }

    /// Returns uninterpreted subaccount balances.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_subaccount_balances_raw(
        &self,
        base_url: &str,
        subaccount: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("subaccount balances", subaccount)?;
        self.get_api_data_with_query(
            base_url,
            SUBACCOUNT_BALANCES_PATH,
            &SubaccountQuery { subaccount },
        )
        .await
    }

    /// Returns uninterpreted subaccount equity.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_subaccount_equity_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("subaccount equity", address)?;
        self.get_api_data_with_query(base_url, SUBACCOUNT_EQUITY_PATH, &AddressQuery { address })
            .await
    }

    /// Returns an uninterpreted subaccount margin ratio.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_subaccount_margin_ratio_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("subaccount margin ratio", address)?;
        self.get_api_data_with_query(
            base_url,
            SUBACCOUNT_MARGIN_RATIO_PATH,
            &AddressQuery { address },
        )
        .await
    }

    /// Returns uninterpreted wallet delegate accounts.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_delegate_accounts_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("delegate accounts", address)?;
        self.get_api_data_with_query(base_url, DELEGATE_ACCOUNTS_PATH, &AddressQuery { address })
            .await
    }

    /// Returns uninterpreted wallets bound to a delegate account.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_delegator_accounts_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("delegator accounts", address)?;
        self.get_api_data_with_query(base_url, DELEGATOR_ACCOUNTS_PATH, &AddressQuery { address })
            .await
    }
}

pub(super) fn validate_hourly_unsettled_funding_page(
    records: &[DeepXHourlyUnsettledFundingRecord],
    request: &DeepXHourlyUnsettledFundingRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "hourly unsettled funding";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if records.len() > request.page_size.unwrap_or(20) as usize {
        return Err(invalid(
            "response exceeds the requested page size".to_string(),
        ));
    }
    let mut event_ids = HashSet::new();
    let mut previous = request.cursor.as_ref().map(|cursor| {
        (
            cursor.boundary_timestamp_ms,
            cursor.market_id,
            cursor.subaccount.to_ascii_lowercase(),
            cursor.event_id.clone(),
        )
    });
    for (index, record) in records.iter().enumerate() {
        if !is_account_address(&record.subaccount)
            || request
                .subaccount
                .as_deref()
                .is_some_and(|value| !record.subaccount.eq_ignore_ascii_case(value))
        {
            return Err(invalid(format!(
                "item {index} has an invalid or unexpected subaccount"
            )));
        }
        if record.market_id == 0
            || request
                .market_id
                .is_some_and(|value| record.market_id != value)
        {
            return Err(invalid(format!("item {index} has an unexpected market ID")));
        }
        if record.signed_position_size_raw == 0
            || record.mark_price_raw == 0
            || record.boundary_block == 0
            || record.boundary_event_id.trim().is_empty()
            || record
                .cumulative_index_raw
                .checked_sub(record.baseline_index_raw)
                != Some(record.delta_index_raw)
        {
            return Err(invalid(format!(
                "item {index} has invalid raw financial or boundary values"
            )));
        }
        if i64::try_from(record.boundary_timestamp_ms)
            .ok()
            .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
            .is_none()
            || request
                .start_ms
                .is_some_and(|value| record.boundary_timestamp_ms < value)
            || request
                .end_ms
                .is_some_and(|value| record.boundary_timestamp_ms > value)
        {
            return Err(invalid(format!(
                "item {index} has an invalid or out-of-range boundary timestamp"
            )));
        }
        if !event_ids.insert(record.boundary_event_id.clone()) {
            return Err(invalid(format!("item {index} repeats a boundary event ID")));
        }
        let key = (
            record.boundary_timestamp_ms,
            record.market_id,
            record.subaccount.to_ascii_lowercase(),
            record.boundary_event_id.clone(),
        );
        if previous
            .as_ref()
            .is_some_and(|previous| match request.sort {
                DeepXAccountSortOrder::Ascending => &key <= previous,
                DeepXAccountSortOrder::Descending => &key >= previous,
            })
        {
            return Err(invalid(format!(
                "item {index} violates the requested keyset order"
            )));
        }
        previous = Some(key);
    }
    Ok(())
}

fn validate_all_subaccounts_page(
    page: &DeepXAccountPage<DeepXSubaccountDirectoryRecord>,
    request: &DeepXAllSubaccountsRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "all subaccounts";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if request
        .page_size
        .is_some_and(|limit| page.items.len() > limit as usize)
    {
        return Err(invalid("page exceeds requested page size".to_string()));
    }
    let mut subaccounts = HashSet::new();
    let mut previous = None;
    for (index, record) in page.items.iter().enumerate() {
        let created = validate_subaccount_directory_record(index, record)?;
        if previous.is_some_and(|previous| created > previous) {
            return Err(invalid(format!(
                "item {index} violates descending creation-time order"
            )));
        }
        previous = Some(created);
        if !subaccounts.insert(record.subaccount.to_ascii_lowercase()) {
            return Err(invalid(format!("item {index} repeats subaccount identity")));
        }
    }
    Ok(())
}

/// Tracks global subaccount directory order and identities across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXSubaccountDirectoryContinuity {
    subaccounts: HashSet<String>,
    previous: Option<jiff::Timestamp>,
}

impl DeepXSubaccountDirectoryContinuity {
    /// Checks one validated page against all directory records previously observed.
    ///
    /// # Errors
    ///
    /// Returns an error for a repeated subaccount or an increase in creation time.
    pub fn observe_page(
        &mut self,
        page: &DeepXAccountPage<DeepXSubaccountDirectoryRecord>,
    ) -> Result<(), DeepXRestError> {
        let endpoint = "all subaccounts";
        let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
        for (index, record) in page.items.iter().enumerate() {
            let created = validate_subaccount_directory_record(index, record)?;
            if self.previous.is_some_and(|previous| created > previous) {
                return Err(invalid(format!(
                    "item {index} violates descending creation-time order"
                )));
            }
            self.previous = Some(created);
            if !self
                .subaccounts
                .insert(record.subaccount.to_ascii_lowercase())
            {
                return Err(invalid(format!("item {index} repeats subaccount identity")));
            }
        }
        Ok(())
    }
}

/// Tracks Spot wallet order identities and per-group time order across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXSpotWalletOrderContinuity {
    order_ids: HashSet<(String, [u8; 32], String, String)>,
    previous: HashMap<([u8; 32], String), jiff::Timestamp>,
}

impl DeepXSpotWalletOrderContinuity {
    /// Checks one validated wallet order page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid pair, repeated order identity, or inconsistent group order.
    pub fn observe_page(
        &mut self,
        page: &DeepXSpotWalletOrdersPage,
        sort: DeepXAccountSortOrder,
    ) -> Result<(), DeepXRestError> {
        let endpoint = "spot wallet orders";
        let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
        for market in &page.markets {
            let pair = decode_spot_pair_identity(&market.pair)
                .ok_or_else(|| invalid("response has an invalid market pair".to_string()))?;
            for subaccount in &market.subaccounts {
                let owner = subaccount.subaccount.to_ascii_lowercase();
                let group = (pair, owner.clone());
                for (index, order) in subaccount.orders.items.iter().enumerate() {
                    let created = order.create_time.parse::<jiff::Timestamp>().map_err(|e| {
                        invalid(format!("item {index} has invalid createTime: {e}"))
                    })?;
                    if self.previous.get(&group).is_some_and(|last| match sort {
                        DeepXAccountSortOrder::Ascending => created < *last,
                        DeepXAccountSortOrder::Descending => created > *last,
                    }) {
                        return Err(invalid(format!(
                            "item {index} violates its group time order"
                        )));
                    }
                    self.previous.insert(group.clone(), created);
                    if !self.order_ids.insert((
                        owner.clone(),
                        pair,
                        order.order_id.clone(),
                        order.order_side.clone(),
                    )) {
                        return Err(invalid(format!(
                            "item {index} repeats wallet order identity"
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Tracks Spot wallet trade IDs and per-group time order across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXSpotWalletTradeContinuity {
    trade_ids: HashSet<u64>,
    previous: HashMap<([u8; 32], String), jiff::Timestamp>,
}

impl DeepXSpotWalletTradeContinuity {
    /// Checks one validated wallet trade page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid pair, repeated trade ID, or inconsistent group order.
    pub fn observe_page(
        &mut self,
        page: &DeepXSpotWalletTradesPage,
        sort: DeepXAccountSortOrder,
    ) -> Result<(), DeepXRestError> {
        let endpoint = "spot wallet trades";
        let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
        for market in &page.markets {
            let pair = decode_spot_pair_identity(&market.pair)
                .ok_or_else(|| invalid("response has an invalid market pair".to_string()))?;
            for subaccount in &market.subaccounts {
                let group = (pair, subaccount.subaccount.to_ascii_lowercase());
                for (index, trade) in subaccount.trades.items.iter().enumerate() {
                    let created = trade
                        .created_at
                        .parse::<jiff::Timestamp>()
                        .map_err(|e| invalid(format!("item {index} has invalid createdAt: {e}")))?;
                    if self.previous.get(&group).is_some_and(|last| match sort {
                        DeepXAccountSortOrder::Ascending => created < *last,
                        DeepXAccountSortOrder::Descending => created > *last,
                    }) {
                        return Err(invalid(format!(
                            "item {index} violates its group time order"
                        )));
                    }
                    self.previous.insert(group.clone(), created);
                    if !self.trade_ids.insert(trade.id) {
                        return Err(invalid(format!("item {index} repeats trade ID")));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Tracks perpetual wallet order identities and per-group time order across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXPerpWalletOrderContinuity {
    order_ids: HashSet<(String, u64, String, bool)>,
    previous: HashMap<(u64, String), jiff::Timestamp>,
}

impl DeepXPerpWalletOrderContinuity {
    /// Checks one validated wallet order page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error for a repeated order identity or inconsistent group order.
    pub fn observe_page(
        &mut self,
        page: &DeepXPerpWalletOrdersPage,
        sort: DeepXAccountSortOrder,
    ) -> Result<(), DeepXRestError> {
        let endpoint = "perp wallet orders";
        let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
        for market in &page.markets {
            for subaccount in &market.subaccounts {
                let owner = subaccount.subaccount.to_ascii_lowercase();
                let group = (market.market_id, owner.clone());
                for (index, order) in subaccount.orders.items.iter().enumerate() {
                    let created = order.create_time.parse::<jiff::Timestamp>().map_err(|e| {
                        invalid(format!("item {index} has invalid createTime: {e}"))
                    })?;
                    if self.previous.get(&group).is_some_and(|last| match sort {
                        DeepXAccountSortOrder::Ascending => created < *last,
                        DeepXAccountSortOrder::Descending => created > *last,
                    }) {
                        return Err(invalid(format!(
                            "item {index} violates its group time order"
                        )));
                    }
                    self.previous.insert(group.clone(), created);
                    if !self.order_ids.insert((
                        owner.clone(),
                        market.market_id,
                        order.order_id.clone(),
                        order.is_long,
                    )) {
                        return Err(invalid(format!(
                            "item {index} repeats wallet order identity"
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_subaccount_directory_record(
    index: usize,
    record: &DeepXSubaccountDirectoryRecord,
) -> Result<jiff::Timestamp, DeepXRestError> {
    let endpoint = "all subaccounts";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if !is_account_address(&record.owner)
        || !is_account_address(&record.subaccount)
        || record.owner.eq_ignore_ascii_case(&record.subaccount)
    {
        return Err(invalid(format!(
            "item {index} has invalid owner or subaccount identity"
        )));
    }
    if record.name.trim().is_empty()
        || record
            .status
            .as_deref()
            .is_some_and(|status| status.trim().is_empty())
        || record.height == 0
    {
        return Err(invalid(format!(
            "item {index} has invalid directory metadata"
        )));
    }
    let created = record
        .created_at
        .parse::<jiff::Timestamp>()
        .map_err(|e| invalid(format!("item {index} has invalid createdAt: {e}")))?;
    u64::try_from(created.as_millisecond())
        .map_err(|_| invalid(format!("item {index} timestamp is before the Unix epoch")))?;
    Ok(created)
}

fn validate_address(endpoint: &str, address: &str) -> Result<(), DeepXRestError> {
    if !is_account_address(address) {
        return Err(DeepXRestError::InvalidRequest(format!(
            "{endpoint} requires a 20-byte hex address"
        )));
    }
    Ok(())
}

fn validate_spot_wallet_order_page(
    page: &DeepXSpotWalletOrdersPage,
    request: &DeepXSpotWalletOrdersRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "spot wallet orders";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut markets = HashSet::new();
    let mut order_ids = HashSet::new();
    let mut previous = HashMap::new();
    let mut item_count = 0_usize;
    for market in &page.markets {
        let pair = decode_spot_pair_identity(&market.pair)
            .ok_or_else(|| invalid("response has an invalid market pair".to_string()))?;
        if market.name.trim().is_empty()
            || !markets.insert(pair)
            || request
                .pair
                .as_deref()
                .and_then(decode_spot_pair_identity)
                .is_some_and(|expected| pair != expected)
            || request
                .name
                .as_deref()
                .is_some_and(|name| market.name != name)
        {
            return Err(invalid(
                "response has a duplicate or unexpected market group".to_string(),
            ));
        }
        let mut subaccounts = HashSet::new();
        for subaccount in &market.subaccounts {
            if !is_account_address(&subaccount.subaccount) {
                return Err(DeepXRestError::InvalidRequest(
                    "spot wallet orders requires a 20-byte hex subaccount address".to_string(),
                ));
            }
            let normalized_subaccount = subaccount.subaccount.to_ascii_lowercase();
            if !subaccounts.insert(normalized_subaccount.clone()) {
                return Err(invalid(format!(
                    "market {} contains duplicate subaccount {}",
                    market.name, subaccount.subaccount
                )));
            }
            let order_key = (pair, normalized_subaccount.clone());
            for (index, order) in subaccount.orders.items.iter().enumerate() {
                item_count = item_count
                    .checked_add(1)
                    .ok_or_else(|| invalid("wallet order page item count overflows".to_string()))?;
                let created = validate_spot_order_record(
                    endpoint,
                    index,
                    order,
                    &subaccount.subaccount,
                    Some(&market.name),
                    Some(&market.pair),
                    request.order_side,
                )?;
                let created_ms = u64::try_from(created.as_millisecond()).map_err(|_| {
                    invalid(format!("item {index} timestamp is before the Unix epoch"))
                })?;
                if request.start_ms.is_some_and(|start| created_ms < start)
                    || request.end_ms.is_some_and(|end| created_ms > end)
                {
                    return Err(invalid(format!(
                        "item {index} is outside the requested time range"
                    )));
                }
                if previous
                    .get(&order_key)
                    .is_some_and(|last| match request.sort {
                        DeepXAccountSortOrder::Ascending => created < *last,
                        DeepXAccountSortOrder::Descending => created > *last,
                    })
                {
                    return Err(invalid(format!(
                        "item {index} violates its group time order"
                    )));
                }
                previous.insert(order_key.clone(), created);
                if !order_ids.insert((
                    normalized_subaccount.clone(),
                    pair,
                    order.order_id.clone(),
                    order.order_side.clone(),
                )) {
                    return Err(invalid(format!(
                        "item {index} repeats wallet order identity"
                    )));
                }
            }
        }
    }
    if request
        .page_size
        .is_some_and(|limit| item_count > limit as usize)
    {
        return Err(invalid(
            "grouped response exceeds requested global page size".to_string(),
        ));
    }
    Ok(())
}

fn spot_wallet_orders_page(
    markets: Vec<DeepXSpotWalletOrderMarket>,
) -> Result<DeepXSpotWalletOrdersPage, DeepXRestError> {
    let (has_next, next_cursor) = normalize_global_metadata(
        "spot wallet orders",
        "order",
        markets.iter().flat_map(|market| {
            market.subaccounts.iter().map(|subaccount| {
                (
                    subaccount.subaccount.as_str(),
                    subaccount.orders.items.len(),
                    subaccount.orders.has_next,
                    subaccount.orders.next_cursor.as_deref(),
                )
            })
        }),
    )?;
    Ok(DeepXSpotWalletOrdersPage {
        markets,
        next_cursor,
        has_next,
    })
}

fn validate_perp_wallet_trade_groups(
    groups: &[DeepXPerpWalletTradeMarket],
    request: &DeepXPerpWalletTradesRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "perp wallet trades";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut markets = HashSet::new();
    let mut trade_ids = HashSet::new();
    for group in groups {
        if group.market_id == 0
            || group.market_name.trim().is_empty()
            || !markets.insert(group.market_id)
            || request
                .market_id
                .is_some_and(|market_id| group.market_id != market_id)
            || request
                .market_name
                .as_deref()
                .is_some_and(|name| group.market_name != name)
        {
            return Err(invalid(
                "response has an invalid, duplicate, or unexpected market group".to_string(),
            ));
        }
        if group.subaccounts.is_empty() {
            return Err(invalid(format!(
                "market {} has no subaccount groups",
                group.market_id
            )));
        }
        let mut subaccounts = HashSet::new();
        for subaccount_group in &group.subaccounts {
            if !is_account_address(&subaccount_group.subaccount) {
                return Err(DeepXRestError::InvalidRequest(
                    "perp wallet trades requires a 20-byte hex subaccount address".to_string(),
                ));
            }
            if !subaccounts.insert(subaccount_group.subaccount.to_ascii_lowercase()) {
                return Err(invalid(format!(
                    "market {} contains duplicate subaccount {}",
                    group.market_id, subaccount_group.subaccount
                )));
            }
            validate_cursor_page(
                endpoint,
                subaccount_group.trades.has_next,
                subaccount_group.trades.next_cursor.as_deref(),
            )?;
            if request
                .page_size
                .is_some_and(|limit| subaccount_group.trades.items.len() > limit as usize)
            {
                return Err(invalid(format!(
                    "market {} subaccount {} exceeds requested page size",
                    group.market_id, subaccount_group.subaccount
                )));
            }
            let mut previous = None;
            for (index, trade) in subaccount_group.trades.items.iter().enumerate() {
                validate_perp_wallet_trade_record(
                    index,
                    trade,
                    request,
                    group.market_id,
                    &mut previous,
                )?;
                if !trade_ids.insert(trade.id) {
                    return Err(invalid(format!("item {index} repeats trade ID")));
                }
            }
        }
    }
    Ok(())
}

fn validate_spot_wallet_trade_page(
    page: &DeepXSpotWalletTradesPage,
    request: &DeepXSpotWalletTradesRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "spot wallet trades";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut markets = HashSet::new();
    let mut trade_ids = HashSet::new();
    let mut previous = HashMap::new();
    let mut item_count = 0_usize;
    for market in &page.markets {
        let pair = decode_spot_pair_identity(&market.pair)
            .ok_or_else(|| invalid("response has an invalid market pair".to_string()))?;
        if market.name.trim().is_empty()
            || !markets.insert(pair)
            || request
                .pair
                .as_deref()
                .and_then(decode_spot_pair_identity)
                .is_some_and(|expected| pair != expected)
            || request
                .name
                .as_deref()
                .is_some_and(|name| market.name != name)
        {
            return Err(invalid(
                "response has a duplicate or unexpected market group".to_string(),
            ));
        }
        let mut subaccounts = HashSet::new();
        for subaccount in &market.subaccounts {
            if !is_account_address(&subaccount.subaccount) {
                return Err(DeepXRestError::InvalidRequest(
                    "spot wallet trades requires a 20-byte hex subaccount address".to_string(),
                ));
            }
            let normalized_subaccount = subaccount.subaccount.to_ascii_lowercase();
            if !subaccounts.insert(normalized_subaccount.clone()) {
                return Err(invalid(format!(
                    "market {} contains duplicate subaccount {}",
                    market.name, subaccount.subaccount
                )));
            }
            let trade_key = (pair, normalized_subaccount);
            for (index, trade) in subaccount.trades.items.iter().enumerate() {
                item_count = item_count
                    .checked_add(1)
                    .ok_or_else(|| invalid("wallet trade page item count overflows".to_string()))?;
                let created = validate_spot_wallet_trade_record(
                    index,
                    trade,
                    request,
                    &market.name,
                    pair,
                    previous.get(&trade_key),
                )?;
                previous.insert(trade_key.clone(), created);
                if !trade_ids.insert(trade.id) {
                    return Err(invalid(format!("item {index} repeats trade ID")));
                }
            }
        }
    }
    if request
        .page_size
        .is_some_and(|limit| item_count > limit as usize)
    {
        return Err(invalid(
            "grouped response exceeds requested global page size".to_string(),
        ));
    }
    Ok(())
}

fn spot_wallet_trades_page(
    markets: Vec<DeepXSpotWalletTradeMarket>,
) -> Result<DeepXSpotWalletTradesPage, DeepXRestError> {
    let (has_next, next_cursor) = normalize_global_metadata(
        "spot wallet trades",
        "trade",
        markets.iter().flat_map(|market| {
            market.subaccounts.iter().map(|subaccount| {
                (
                    subaccount.subaccount.as_str(),
                    subaccount.trades.items.len(),
                    subaccount.trades.has_next,
                    subaccount.trades.next_cursor.as_deref(),
                )
            })
        }),
    )?;
    Ok(DeepXSpotWalletTradesPage {
        markets,
        next_cursor,
        has_next,
    })
}

fn normalize_global_metadata<'a>(
    endpoint: &'static str,
    kind: &'static str,
    groups: impl Iterator<Item = (&'a str, usize, bool, Option<&'a str>)>,
) -> Result<(bool, Option<String>), DeepXRestError> {
    let invalid = |message| DeepXRestError::InvalidAccountResponse { endpoint, message };
    let mut metadata: Option<(bool, Option<String>)> = None;
    for (subaccount, count, has_next, next_cursor) in groups {
        if count == 0 {
            if has_next || next_cursor.is_some_and(|cursor| !cursor.is_empty()) {
                return Err(invalid(format!(
                    "subaccount {subaccount} has an empty nonterminal {kind} group"
                )));
            }
            continue;
        }
        validate_cursor_page(endpoint, has_next, next_cursor)?;
        let observed = (has_next, next_cursor.map(str::to_string));
        if metadata
            .as_ref()
            .is_some_and(|expected| expected != &observed)
        {
            return Err(invalid(
                "nonempty groups disagree on global pagination metadata".to_string(),
            ));
        }
        metadata.get_or_insert(observed);
    }
    Ok(metadata.unwrap_or((false, None)))
}

fn validate_perp_wallet_order_page(
    page: &DeepXPerpWalletOrdersPage,
    request: &DeepXPerpWalletOrdersRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "perp wallet orders";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut markets = HashSet::new();
    let mut order_ids = HashSet::new();
    let mut previous = HashMap::new();
    let mut item_count = 0_usize;
    for market in &page.markets {
        if market.market_id == 0
            || market.market_name.trim().is_empty()
            || !markets.insert(market.market_id)
            || request
                .market_id
                .is_some_and(|market_id| market.market_id != market_id)
            || request
                .market_name
                .as_deref()
                .is_some_and(|name| market.market_name != name)
        {
            return Err(invalid(
                "response has an invalid, duplicate, or unexpected market group".to_string(),
            ));
        }
        let mut subaccounts = HashSet::new();
        for subaccount in &market.subaccounts {
            if !is_account_address(&subaccount.subaccount) {
                return Err(DeepXRestError::InvalidRequest(
                    "perp wallet orders requires a 20-byte hex subaccount address".to_string(),
                ));
            }
            let normalized_subaccount = subaccount.subaccount.to_ascii_lowercase();
            if !subaccounts.insert(normalized_subaccount.clone()) {
                return Err(invalid(format!(
                    "market {} contains duplicate subaccount {}",
                    market.market_id, subaccount.subaccount
                )));
            }
            let order_key = (market.market_id, normalized_subaccount.clone());
            for (index, order) in subaccount.orders.items.iter().enumerate() {
                item_count = item_count
                    .checked_add(1)
                    .ok_or_else(|| invalid("wallet order page item count overflows".to_string()))?;
                validate_perp_wallet_order_record_scope(
                    endpoint,
                    index,
                    order,
                    &subaccount.subaccount,
                    Some(market.market_id),
                )?;
                if request
                    .is_long
                    .is_some_and(|is_long| order.is_long != is_long)
                {
                    return Err(invalid(format!(
                        "item {index} does not match the requested side"
                    )));
                }
                let created = order
                    .create_time
                    .parse::<jiff::Timestamp>()
                    .map_err(|e| invalid(format!("item {index} has invalid createTime: {e}")))?;
                let created_ms = u64::try_from(created.as_millisecond()).map_err(|_| {
                    invalid(format!("item {index} timestamp is before the Unix epoch"))
                })?;
                if request.start_ms.is_some_and(|start| created_ms < start)
                    || request.end_ms.is_some_and(|end| created_ms > end)
                {
                    return Err(invalid(format!(
                        "item {index} is outside the requested time range"
                    )));
                }
                if previous
                    .get(&order_key)
                    .is_some_and(|last| match request.sort {
                        DeepXAccountSortOrder::Ascending => created < *last,
                        DeepXAccountSortOrder::Descending => created > *last,
                    })
                {
                    return Err(invalid(format!(
                        "item {index} violates its group time order"
                    )));
                }
                previous.insert(order_key.clone(), created);
                if !order_ids.insert((
                    normalized_subaccount.clone(),
                    market.market_id,
                    order.order_id.clone(),
                    order.is_long,
                )) {
                    return Err(invalid(format!(
                        "item {index} repeats wallet order identity"
                    )));
                }
            }
        }
    }
    if request
        .page_size
        .is_some_and(|limit| item_count > limit as usize)
    {
        return Err(invalid(
            "grouped response exceeds requested global page size".to_string(),
        ));
    }
    Ok(())
}

fn perp_wallet_orders_page(
    markets: Vec<DeepXPerpWalletOrderMarket>,
) -> Result<DeepXPerpWalletOrdersPage, DeepXRestError> {
    let endpoint = "perp wallet orders";
    let invalid = |message| DeepXRestError::InvalidAccountResponse { endpoint, message };
    let mut metadata: Option<(bool, Option<String>)> = None;
    for market in &markets {
        if market.subaccounts.is_empty() {
            return Err(invalid(format!(
                "market {} has no subaccount groups",
                market.market_id
            )));
        }
        for subaccount in &market.subaccounts {
            if subaccount.orders.items.is_empty() {
                return Err(invalid(format!(
                    "market {} subaccount {} has an empty order group",
                    market.market_id, subaccount.subaccount
                )));
            }
            validate_cursor_page(
                endpoint,
                subaccount.orders.has_next,
                subaccount.orders.next_cursor.as_deref(),
            )?;
            let observed = (
                subaccount.orders.has_next,
                subaccount.orders.next_cursor.clone(),
            );
            if metadata
                .as_ref()
                .is_some_and(|expected| expected != &observed)
            {
                return Err(invalid(
                    "nested groups disagree on global pagination metadata".to_string(),
                ));
            }
            metadata.get_or_insert(observed);
        }
    }
    let (has_next, next_cursor) = metadata.unwrap_or((false, None));
    Ok(DeepXPerpWalletOrdersPage {
        markets,
        next_cursor,
        has_next,
    })
}

fn validate_delegate_accounts(accounts: &[DeepXDelegateAccount]) -> Result<(), DeepXRestError> {
    let endpoint = "delegate accounts";
    let mut addresses = HashSet::new();
    for (index, account) in accounts.iter().enumerate() {
        let message = if !is_account_address(&account.delegate_address) {
            Some(format!("item {index} has an invalid delegate address"))
        } else if !addresses.insert(account.delegate_address.to_ascii_lowercase()) {
            Some(format!("item {index} repeats a delegate address"))
        } else if account.valid_until != 0 && account.valid_until <= account.create_time {
            Some(format!(
                "item {index} expiry does not follow its creation timestamp"
            ))
        } else {
            None
        };
        if let Some(message) = message {
            return Err(DeepXRestError::InvalidAccountResponse { endpoint, message });
        }
    }
    Ok(())
}

fn validate_delegator_wallets(wallets: &[String]) -> Result<(), DeepXRestError> {
    let endpoint = "delegator accounts";
    let mut seen = HashSet::new();
    for (index, wallet) in wallets.iter().enumerate() {
        let message = if !is_account_address(wallet) {
            Some(format!("item {index} has an invalid wallet address"))
        } else if !seen.insert(wallet.to_ascii_lowercase()) {
            Some(format!("item {index} repeats a wallet address"))
        } else {
            None
        };
        if let Some(message) = message {
            return Err(DeepXRestError::InvalidAccountResponse { endpoint, message });
        }
    }
    Ok(())
}

fn validate_invitation_codes(codes: &[String]) -> Result<(), DeepXRestError> {
    let endpoint = "invitation codes";
    let mut unique = HashSet::with_capacity(codes.len());
    for code in codes {
        let message = if code.trim().is_empty() {
            Some("response contains an empty code")
        } else if !unique.insert(code) {
            Some("response contains a duplicate code")
        } else {
            None
        };
        if let Some(message) = message {
            return Err(DeepXRestError::InvalidAccountResponse {
                endpoint,
                message: message.to_string(),
            });
        }
    }
    Ok(())
}

fn invitation_status_for_wallet(
    status: DeepXInvitationStatusWire,
    address: &str,
) -> Result<DeepXInvitationStatus, DeepXRestError> {
    match status {
        DeepXInvitationStatusWire::Flag(is_bound) => Ok(DeepXInvitationStatus {
            address: address.to_string(),
            is_bound,
        }),
        DeepXInvitationStatusWire::Detailed(status)
            if status.address.eq_ignore_ascii_case(address) =>
        {
            Ok(status)
        }
        DeepXInvitationStatusWire::Detailed(_) => Err(DeepXRestError::InvalidAccountResponse {
            endpoint: "invitation status",
            message: "response belongs to another wallet".to_string(),
        }),
    }
}

fn validate_user_stats(stats: &DeepXUserStats) -> Result<(), DeepXRestError> {
    let endpoint = "user stats";
    let invalid = |message| DeepXRestError::InvalidAccountResponse { endpoint, message };
    let expected_count = u64::try_from(stats.subaccounts.len())
        .map_err(|e| invalid(format!("subaccount count overflow: {e}")))?;
    if stats.if_staked_quote_asset_amount < Decimal::ZERO {
        return Err(invalid(
            "Insurance Fund staked quote amount is negative".to_string(),
        ));
    }
    if stats.number_of_sub_accounts != expected_count
        || stats.number_of_sub_accounts_created < stats.number_of_sub_accounts
    {
        return Err(invalid("subaccount counters are inconsistent".to_string()));
    }
    let mut subaccounts = HashSet::new();
    for (index, address) in stats.subaccounts.iter().enumerate() {
        if !is_account_address(address) {
            return Err(invalid(format!(
                "item {index} is not a 20-byte hex subaccount address"
            )));
        }
        if !subaccounts.insert(address.to_ascii_lowercase()) {
            return Err(invalid(format!(
                "item {index} repeats a subaccount address"
            )));
        }
    }
    Ok(())
}

fn validate_subaccount_equity(
    equity: &DeepXSubaccountEquity,
    requested_address: &str,
) -> Result<(), DeepXRestError> {
    let endpoint = "subaccount equity";
    if !equity.subaccount.eq_ignore_ascii_case(requested_address) {
        return Err(DeepXRestError::InvalidAccountResponse {
            endpoint,
            message: "equity belongs to another subaccount".to_string(),
        });
    }
    if equity.total_deposits_usd.is_sign_negative() || equity.total_borrows_usd.is_sign_negative() {
        return Err(DeepXRestError::InvalidAccountResponse {
            endpoint,
            message: "equity has a negative deposit or borrow total".to_string(),
        });
    }
    Ok(())
}

fn validate_subaccount_profile(
    profile: &DeepXSubaccountProfile,
    requested_address: &str,
) -> Result<(), DeepXRestError> {
    let invalid = |message: &str| DeepXRestError::InvalidAccountResponse {
        endpoint: "subaccount info",
        message: message.to_string(),
    };
    if !profile.address.eq_ignore_ascii_case(requested_address) {
        return Err(invalid("profile belongs to another subaccount"));
    }
    if !is_account_address(&profile.authority) {
        return Err(invalid("profile has an invalid authority address"));
    }
    if !matches!(
        profile.status.as_str(),
        "Active" | "BeingLiquidated" | "Closed" | "Bankrupt"
    ) {
        return Err(invalid("profile has an unknown account status"));
    }
    if !matches!(profile.margin_strategy.as_str(), "Cross" | "Isolate") {
        return Err(invalid("profile has an unknown margin strategy"));
    }
    if profile.height == 0
        || i64::try_from(profile.created_at)
            .ok()
            .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
            .is_none()
    {
        return Err(invalid(
            "profile has an invalid height or creation timestamp",
        ));
    }
    Ok(())
}

fn validate_subaccount_margin_ratio(
    margin: &DeepXSubaccountMarginRatio,
) -> Result<(), DeepXRestError> {
    let invalid = |message| DeepXRestError::InvalidAccountResponse {
        endpoint: "subaccount margin ratio",
        message,
    };
    if margin.collateral.is_sign_negative()
        || margin.margin_required.is_sign_negative()
        || margin
            .margin_ratio
            .is_some_and(|ratio| ratio.is_sign_negative())
    {
        return Err(invalid(
            "margin ratio response has negative financial values".to_string(),
        ));
    }
    let exact = margin
        .exact_margin
        .as_ref()
        .ok_or_else(|| invalid("margin ratio response lacks exact margin values".to_string()))?;
    if exact.total_collateral.is_sign_negative()
        || exact.maint_margin_req.is_sign_negative()
        || exact.uni_mmr.is_some_and(|ratio| ratio.is_sign_negative())
        || exact.uni_mmr.is_none() != exact.maint_margin_req.is_zero()
    {
        return Err(invalid(
            "margin ratio response has invalid exact margin values".to_string(),
        ));
    }
    if let Some(ratio) = exact.uni_mmr {
        let scaled = |value: Decimal, places| {
            let mut result = U256::from(u128::try_from(value.mantissa()).ok()?);
            for _ in 0..places {
                result = result.checked_mul(U256::from(10))?;
            }
            Some::<U256>(result)
        };
        let normalized = ratio.normalize();
        let expected = scaled(exact.total_collateral, exact.maint_margin_req.scale() + 8)
            .zip(scaled(
                exact.maint_margin_req,
                exact.total_collateral.scale(),
            ))
            .map(|(numerator, denominator)| numerator / denominator);
        let observed = (normalized.scale() <= 8)
            .then(|| scaled(normalized, 8 - normalized.scale()))
            .flatten();
        if expected.is_none() || expected != observed {
            return Err(invalid(
                "exact margin ratio disagrees with collateral and maintenance requirement"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, routing::get};
    use rstest::rstest;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;
    use crate::rest::DeepXApiResponse;

    const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
    const SPOT_ORDERS: &str =
        include_str!("../../test_data/http/testnet/spot_wallet_orders_eth_usdc_page_1.json");
    const SPOT_TRADES: &str =
        include_str!("../../test_data/http/testnet/spot_wallet_trades_eth_usdc_page_1.json");
    const PERP_ORDERS: &str =
        include_str!("../../test_data/http/testnet/perp_wallet_orders_page_1.json");
    const PERP_TRADES: &str =
        include_str!("../../test_data/http/testnet/perp_wallet_trades_page.json");
    const USER_STATS: &str = include_str!("../../test_data/http/testnet/wallet_user_stats.json");
    const SUBACCOUNT_BALANCES: &str =
        include_str!("../../test_data/http/testnet/subaccount_balances.json");
    const SUBACCOUNT_PROFILE: &str =
        include_str!("../../test_data/http/testnet/subaccount_info.json");
    const SUBACCOUNT_EQUITY: &str =
        include_str!("../../test_data/http/testnet/subaccount_equity.json");
    const SUBACCOUNT_MARGIN_RATIO: &str =
        include_str!("../../test_data/http/testnet/subaccount_margin_ratio_exact.json");
    const ALL_SUBACCOUNTS: &str =
        include_str!("../../test_data/http/testnet/all_subaccounts_page_1.json");
    const WALLET_HOURLY_FUNDING_PAGE_1: &str = include_str!(
        "../../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_1.json"
    );

    #[rstest]
    #[case("duplicate-market")]
    #[case("invalid-subaccount")]
    #[case("duplicate-subaccount")]
    #[case("divergent-cursor")]
    #[case("empty-nonterminal")]
    #[case("foreign-owner")]
    #[case("wrong-market")]
    #[case("duplicate-order")]
    #[case("wrong-order")]
    #[case("oversized-page")]
    #[tokio::test]
    async fn spot_wallet_order_transport_rejects_invalid_group_semantics(#[case] mutation: &str) {
        let mut response: Value = serde_json::from_str(SPOT_ORDERS).unwrap();
        match mutation {
            "duplicate-market" => {
                let group = response["data"][0].clone();
                response["data"].as_array_mut().unwrap().push(group);
            }
            "invalid-subaccount" => {
                response["data"][0]["subaccounts"][0]["subaccount"] = json!("invalid");
            }
            "duplicate-subaccount" | "divergent-cursor" => {
                let mut group = response["data"][0]["subaccounts"][0].clone();
                if mutation == "divergent-cursor" {
                    group["subaccount"] = json!("0x1111111111111111111111111111111111111111");
                    group["orders"]["nextCursor"] = json!("other");
                }
                response["data"][0]["subaccounts"]
                    .as_array_mut()
                    .unwrap()
                    .push(group);
            }
            "empty-nonterminal" => {
                response["data"][0]["subaccounts"][0]["orders"]["items"] = json!([]);
            }
            "foreign-owner" => {
                response["data"][0]["subaccounts"][0]["orders"]["items"][0]["maker"] =
                    json!("0x1111111111111111111111111111111111111111");
            }
            "wrong-market" => {
                response["data"][0]["subaccounts"][0]["orders"]["items"][0]["pairName"] =
                    json!("SOL/USDC");
            }
            "duplicate-order" => {
                response["data"][0]["subaccounts"][0]["orders"]["items"][1] =
                    response["data"][0]["subaccounts"][0]["orders"]["items"][0].clone();
            }
            "wrong-order" => {
                response["data"][0]["subaccounts"][0]["orders"]["items"][1]["createTime"] =
                    json!("2026-09-17T08:00:00Z");
            }
            "oversized-page" => {
                let item = response["data"][0]["subaccounts"][0]["orders"]["items"][0].clone();
                response["data"][0]["subaccounts"][0]["orders"]["items"]
                    .as_array_mut()
                    .unwrap()
                    .push(item);
            }
            _ => unreachable!(),
        }
        let router = Router::new().route(
            SPOT_WALLET_ORDERS_PATH,
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRestTransport::new(Some(2), None).unwrap();
        let request = DeepXSpotWalletOrdersRequest {
            wallet: Some("0x89bb0946046588f4f257ffc71bd3231aba13474a".to_string()),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        assert!(
            client
                .get_spot_wallet_orders(&base_url, &request)
                .await
                .is_err()
        );
        server.abort();
    }

    #[rstest]
    #[case("duplicate-market")]
    #[case("invalid-subaccount")]
    #[case("duplicate-subaccount")]
    #[case("divergent-cursor")]
    #[case("empty-nonterminal")]
    #[case("wrong-market")]
    #[case("duplicate-trade")]
    #[case("wrong-order")]
    #[case("oversized-page")]
    #[tokio::test]
    async fn spot_wallet_trade_transport_rejects_invalid_group_semantics(#[case] mutation: &str) {
        let mut response: Value = serde_json::from_str(SPOT_TRADES).unwrap();
        match mutation {
            "duplicate-market" => {
                let group = response["data"][0].clone();
                response["data"].as_array_mut().unwrap().push(group);
            }
            "invalid-subaccount" => {
                response["data"][0]["subaccounts"][0]["subaccount"] = json!("invalid");
            }
            "duplicate-subaccount" | "divergent-cursor" => {
                let mut group = response["data"][0]["subaccounts"][0].clone();
                if mutation == "divergent-cursor" {
                    group["subaccount"] = json!("0x1111111111111111111111111111111111111111");
                    group["trades"]["nextCursor"] = json!("other");
                }
                response["data"][0]["subaccounts"]
                    .as_array_mut()
                    .unwrap()
                    .push(group);
            }
            "empty-nonterminal" => {
                response["data"][0]["subaccounts"][0]["trades"]["items"] = json!([]);
            }
            "wrong-market" => {
                response["data"][0]["subaccounts"][0]["trades"]["items"][0]["pairName"] =
                    json!("SOL/USDC");
            }
            "duplicate-trade" => {
                response["data"][0]["subaccounts"][0]["trades"]["items"][1]["id"] =
                    response["data"][0]["subaccounts"][0]["trades"]["items"][0]["id"].clone();
            }
            "wrong-order" => {
                response["data"][0]["subaccounts"][0]["trades"]["items"][1]["createdAt"] =
                    json!("2026-09-17T08:00:00Z");
            }
            "oversized-page" => {
                let item = response["data"][0]["subaccounts"][0]["trades"]["items"][0].clone();
                response["data"][0]["subaccounts"][0]["trades"]["items"]
                    .as_array_mut()
                    .unwrap()
                    .push(item);
            }
            _ => unreachable!(),
        }
        let router = Router::new().route(
            SPOT_WALLET_TRADES_PATH,
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRestTransport::new(Some(2), None).unwrap();
        let request = DeepXSpotWalletTradesRequest {
            wallet: "0x89bb0946046588f4f257ffc71bd3231aba13474a".to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        assert!(
            client
                .get_spot_wallet_trades(&base_url, &request)
                .await
                .is_err()
        );
        server.abort();
    }

    #[rstest]
    #[case("divergent-cursor")]
    #[case("divergent-has-next")]
    #[case("empty-group")]
    #[case("foreign-owner")]
    #[case("wrong-market")]
    #[case("duplicate-order")]
    #[case("wrong-order")]
    #[case("oversized-page")]
    #[tokio::test]
    async fn perp_wallet_order_transport_rejects_invalid_group_semantics(#[case] mutation: &str) {
        let mut response: Value = serde_json::from_str(PERP_ORDERS).unwrap();
        match mutation {
            "divergent-cursor" => {
                response["data"][0]["subaccounts"][1]["orders"]["nextCursor"] = json!("other");
            }
            "divergent-has-next" => {
                response["data"][0]["subaccounts"][1]["orders"]["hasNext"] = json!(false);
            }
            "empty-group" => {
                response["data"][0]["subaccounts"][0]["orders"]["items"] = json!([]);
            }
            "foreign-owner" => {
                response["data"][0]["subaccounts"][0]["orders"]["items"][0]["owner"] =
                    json!("0x1111111111111111111111111111111111111111");
            }
            "wrong-market" => {
                response["data"][0]["subaccounts"][0]["orders"]["items"][0]["marketId"] = json!(4);
            }
            "duplicate-order" => {
                let duplicate = response["data"][0]["subaccounts"][0]["orders"]["items"][0].clone();
                response["data"][0]["subaccounts"][0]["orders"]["items"][1] = duplicate;
            }
            "wrong-order" => {
                response["data"][0]["subaccounts"][0]["orders"]["items"][1]["createTime"] =
                    json!("2026-09-17T00:00:00Z");
                response["data"][0]["subaccounts"][0]["orders"]["items"][1]["updatedTime"] =
                    json!("2026-09-17T00:00:00Z");
            }
            "oversized-page" => {
                let item = response["data"][0]["subaccounts"][0]["orders"]["items"][0].clone();
                response["data"][0]["subaccounts"][0]["orders"]["items"]
                    .as_array_mut()
                    .unwrap()
                    .push(item);
            }
            _ => unreachable!(),
        }
        let router = Router::new().route(
            PERP_WALLET_ORDERS_PATH,
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRestTransport::new(Some(5), None).unwrap();
        let request = DeepXPerpWalletOrdersRequest {
            wallet: WALLET.to_string(),
            market_name: None,
            market_id: None,
            is_long: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        assert!(
            client
                .get_perp_wallet_orders(&base_url, &request)
                .await
                .is_err()
        );
        server.abort();
    }

    #[rstest]
    #[case("duplicate-market")]
    #[case("empty-subaccounts")]
    #[case("invalid-subaccount")]
    #[case("duplicate-subaccount")]
    #[case("missing-cursor")]
    #[case("oversized-page")]
    #[case("wrong-market")]
    #[case("zero-id")]
    #[case("invalid-order-id")]
    #[case("negative-size")]
    #[case("invalid-enum")]
    #[case("invalid-timestamp")]
    #[case("wrong-order")]
    #[case("duplicate-trade")]
    #[tokio::test]
    async fn perp_wallet_trade_transport_rejects_invalid_group_semantics(#[case] mutation: &str) {
        let mut response: Value = serde_json::from_str(PERP_TRADES).unwrap();
        match mutation {
            "duplicate-market" => {
                response["data"][1]["marketId"] = response["data"][0]["marketId"].clone();
            }
            "empty-subaccounts" => response["data"][0]["subaccounts"] = json!([]),
            "invalid-subaccount" => {
                response["data"][0]["subaccounts"][0]["subaccount"] = json!("invalid");
            }
            "duplicate-subaccount" => {
                response["data"][0]["subaccounts"][1]["subaccount"] =
                    response["data"][0]["subaccounts"][0]["subaccount"].clone();
            }
            "missing-cursor" => {
                response["data"][0]["subaccounts"][2]["trades"]["nextCursor"] = Value::Null;
            }
            "oversized-page" => {
                let item = response["data"][0]["subaccounts"][0]["trades"]["items"][0].clone();
                response["data"][0]["subaccounts"][0]["trades"]["items"]
                    .as_array_mut()
                    .unwrap()
                    .push(item);
            }
            "wrong-market" => {
                response["data"][0]["subaccounts"][0]["trades"]["items"][0]["marketId"] = json!(4);
            }
            "zero-id" => {
                response["data"][0]["subaccounts"][0]["trades"]["items"][0]["id"] = json!(0);
            }
            "invalid-order-id" => {
                response["data"][0]["subaccounts"][0]["trades"]["items"][0]["orderId"] =
                    json!("invalid");
            }
            "negative-size" => {
                response["data"][0]["subaccounts"][0]["trades"]["items"][0]["size"] = json!(-1);
            }
            "invalid-enum" => {
                response["data"][0]["subaccounts"][0]["trades"]["items"][0]["taker"] = json!(" ");
            }
            "invalid-timestamp" => {
                response["data"][0]["subaccounts"][0]["trades"]["items"][0]["createdAt"] =
                    json!("invalid");
            }
            "wrong-order" => {
                response["data"][0]["subaccounts"][0]["trades"]["items"][1]["createdAt"] =
                    json!("2026-09-17T00:00:00Z");
            }
            "duplicate-trade" => {
                response["data"][0]["subaccounts"][1]["trades"]["items"][0]["id"] =
                    response["data"][0]["subaccounts"][0]["trades"]["items"][0]["id"].clone();
            }
            _ => unreachable!(),
        }
        let router = Router::new().route(
            PERP_WALLET_TRADES_PATH,
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRestTransport::new(Some(5), None).unwrap();
        let request = DeepXPerpWalletTradesRequest {
            wallet: WALLET.to_string(),
            market_name: None,
            market_id: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        assert!(
            client
                .get_perp_wallet_trades(&base_url, &request)
                .await
                .is_err()
        );
        server.abort();
    }

    #[rstest]
    #[case("duplicate-market")]
    #[case("empty-subaccounts")]
    #[case("invalid-subaccount")]
    #[case("duplicate-subaccount")]
    #[case("missing-cursor")]
    #[case("oversized-page")]
    #[case("wrong-market")]
    #[case("duplicate-trade")]
    fn perp_wallet_trade_groups_reject_invalid_protocol_data(#[case] mutation: &str) {
        let mut groups: Vec<DeepXPerpWalletTradeMarket> =
            serde_json::from_str::<DeepXApiResponse<_>>(PERP_TRADES)
                .unwrap()
                .data;
        let request = DeepXPerpWalletTradesRequest {
            wallet: WALLET.to_string(),
            market_name: None,
            market_id: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        validate_perp_wallet_trade_groups(&groups, &request).unwrap();
        match mutation {
            "duplicate-market" => groups[1].market_id = groups[0].market_id,
            "empty-subaccounts" => groups[0].subaccounts.clear(),
            "invalid-subaccount" => groups[0].subaccounts[0].subaccount = "invalid".to_string(),
            "duplicate-subaccount" => {
                groups[0].subaccounts[1].subaccount = groups[0].subaccounts[0].subaccount.clone();
            }
            "missing-cursor" => groups[0].subaccounts[2].trades.next_cursor = None,
            "oversized-page" => {
                let trade = groups[0].subaccounts[0].trades.items[0].clone();
                groups[0].subaccounts[0].trades.items.resize(6, trade);
            }
            "wrong-market" => groups[0].subaccounts[0].trades.items[0].market_id = 4,
            "duplicate-trade" => {
                groups[0].subaccounts[1].trades.items[0].id =
                    groups[0].subaccounts[0].trades.items[0].id;
            }
            _ => unreachable!(),
        }
        assert!(validate_perp_wallet_trade_groups(&groups, &request).is_err());
    }

    #[rstest]
    #[case("arithmetic")]
    #[case("market")]
    #[case("timestamp")]
    #[case("order")]
    #[case("duplicate-event")]
    #[case("zero-position")]
    #[case("empty-event")]
    #[case("oversized-page")]
    fn hourly_unsettled_funding_rejects_invalid_page(#[case] mutation: &str) {
        let mut response: serde_json::Value =
            serde_json::from_str(WALLET_HOURLY_FUNDING_PAGE_1).unwrap();
        let request = DeepXHourlyUnsettledFundingRequest {
            subaccount: None,
            wallet: Some(WALLET.to_string()),
            market_id: Some(3),
            start_ms: None,
            end_ms: None,
            cursor: None,
            page_size: Some(5),
            sort: DeepXAccountSortOrder::Descending,
        };
        let original: Vec<DeepXHourlyUnsettledFundingRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        validate_hourly_unsettled_funding_page(&original, &request).unwrap();
        match mutation {
            "arithmetic" => response["data"][0]["deltaIndexRaw"] = serde_json::json!("1"),
            "market" => response["data"][0]["marketId"] = serde_json::json!(4),
            "timestamp" => {
                response["data"][0]["boundaryTimestampMs"] = serde_json::json!(u64::MAX);
            }
            "order" => response["data"].as_array_mut().unwrap().swap(0, 1),
            "duplicate-event" => {
                response["data"][1]["boundaryEventId"] =
                    response["data"][0]["boundaryEventId"].clone();
            }
            "zero-position" => {
                response["data"][0]["signedPositionSizeRaw"] = serde_json::json!("0");
            }
            "empty-event" => response["data"][0]["boundaryEventId"] = serde_json::json!(" "),
            "oversized-page" => {
                let record = response["data"][4].clone();
                response["data"].as_array_mut().unwrap().push(record);
            }
            _ => unreachable!(),
        }
        let records: Vec<DeepXHourlyUnsettledFundingRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        assert!(matches!(
            validate_hourly_unsettled_funding_page(&records, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "hourly unsettled funding",
                ..
            })
        ));
    }

    #[rstest]
    #[case("owner")]
    #[case("subaccount")]
    #[case("same-address")]
    #[case("name")]
    #[case("status")]
    #[case("height")]
    #[case("timestamp")]
    fn all_subaccounts_reject_invalid_record_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXSubaccountDirectoryRecord>> =
            serde_json::from_str(ALL_SUBACCOUNTS).unwrap();
        let mut record = response.data.items[0].clone();
        match mutation {
            "owner" => record.owner = "invalid".to_string(),
            "subaccount" => record.subaccount = "invalid".to_string(),
            "same-address" => record.subaccount = record.owner.clone(),
            "name" => record.name.clear(),
            "status" => record.status = Some(" ".to_string()),
            "height" => record.height = 0,
            "timestamp" => record.created_at = "invalid".to_string(),
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_subaccount_directory_record(0, &record),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "all subaccounts",
                ..
            })
        ));
    }

    #[rstest]
    #[case("duplicate")]
    #[case("ordering")]
    #[case("oversized")]
    fn all_subaccounts_reject_invalid_page_semantics(#[case] mutation: &str) {
        let mut response: serde_json::Value = serde_json::from_str(ALL_SUBACCOUNTS).unwrap();
        let mut request = DeepXAllSubaccountsRequest {
            cursor: None,
            page_size: Some(5),
        };
        match mutation {
            "duplicate" => {
                response["data"]["items"][1]["subaccount"] =
                    response["data"]["items"][0]["subaccount"].clone();
            }
            "ordering" => response["data"]["items"].as_array_mut().unwrap().swap(0, 1),
            "oversized" => request.page_size = Some(4),
            _ => unreachable!(),
        }
        let page: DeepXAccountPage<DeepXSubaccountDirectoryRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        assert!(validate_all_subaccounts_page(&page, &request).is_err());
    }

    #[rstest]
    fn all_subaccounts_continuity_rejects_cross_page_duplicates_and_ordering() {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXSubaccountDirectoryRecord>> =
            serde_json::from_str(ALL_SUBACCOUNTS).unwrap();
        let mut first = response.data;
        let later = first.items.split_off(1);
        assert!(!later.is_empty());
        let mut second = DeepXAccountPage {
            items: later,
            next_cursor: None,
            has_next: false,
        };
        let mut continuity = DeepXSubaccountDirectoryContinuity::default();
        continuity.observe_page(&first).unwrap();
        continuity.observe_page(&second).unwrap();

        let original = second.items[0].clone();
        second.items[0].subaccount = first.items[0].subaccount.clone();
        let mut continuity = DeepXSubaccountDirectoryContinuity::default();
        continuity.observe_page(&first).unwrap();
        assert!(matches!(
            continuity.observe_page(&second),
            Err(DeepXRestError::InvalidHistoryResponse { .. }),
        ));

        second.items[0] = original;
        second.items[0].created_at = "2026-09-12T00:00:00Z".to_string();
        let mut continuity = DeepXSubaccountDirectoryContinuity::default();
        continuity.observe_page(&first).unwrap();
        assert!(matches!(
            continuity.observe_page(&second),
            Err(DeepXRestError::InvalidHistoryResponse { .. }),
        ));
    }

    #[rstest]
    fn spot_wallet_order_continuity_rejects_repeated_and_out_of_order_pages() {
        let markets: Vec<DeepXSpotWalletOrderMarket> =
            serde_json::from_str::<DeepXApiResponse<_>>(SPOT_ORDERS)
                .unwrap()
                .data;
        let page = spot_wallet_orders_page(markets).unwrap();
        let mut single = page.clone();
        for market in &mut single.markets {
            for subaccount in &mut market.subaccounts {
                subaccount.orders.items.truncate(1);
            }
        }
        let mut continuity = DeepXSpotWalletOrderContinuity::default();
        continuity
            .observe_page(&single, DeepXAccountSortOrder::Descending)
            .unwrap();
        assert!(matches!(
            continuity.observe_page(&single, DeepXAccountSortOrder::Descending),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("repeats wallet order identity"),
        ));

        let mut later = page.clone();
        let first = later
            .markets
            .iter_mut()
            .flat_map(|market| &mut market.subaccounts)
            .flat_map(|subaccount| &mut subaccount.orders.items)
            .next()
            .unwrap();
        first.create_time = "2099-01-01T00:00:00Z".to_string();
        let mut continuity = DeepXSpotWalletOrderContinuity::default();
        continuity
            .observe_page(&page, DeepXAccountSortOrder::Descending)
            .unwrap();
        assert!(matches!(
            continuity.observe_page(&later, DeepXAccountSortOrder::Descending),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("group time order"),
        ));
    }

    #[rstest]
    fn spot_wallet_trade_continuity_rejects_repeated_and_out_of_order_pages() {
        let markets: Vec<DeepXSpotWalletTradeMarket> =
            serde_json::from_str::<DeepXApiResponse<_>>(SPOT_TRADES)
                .unwrap()
                .data;
        let page = spot_wallet_trades_page(markets).unwrap();
        let mut single = page.clone();
        for market in &mut single.markets {
            for subaccount in &mut market.subaccounts {
                subaccount.trades.items.truncate(1);
            }
        }
        let mut continuity = DeepXSpotWalletTradeContinuity::default();
        continuity
            .observe_page(&single, DeepXAccountSortOrder::Descending)
            .unwrap();
        assert!(matches!(
            continuity.observe_page(&single, DeepXAccountSortOrder::Descending),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("repeats trade ID"),
        ));

        let mut later = page.clone();
        let first = later
            .markets
            .iter_mut()
            .flat_map(|market| &mut market.subaccounts)
            .flat_map(|subaccount| &mut subaccount.trades.items)
            .next()
            .unwrap();
        first.created_at = "2099-01-01T00:00:00Z".to_string();
        let mut continuity = DeepXSpotWalletTradeContinuity::default();
        continuity
            .observe_page(&page, DeepXAccountSortOrder::Descending)
            .unwrap();
        assert!(matches!(
            continuity.observe_page(&later, DeepXAccountSortOrder::Descending),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("group time order"),
        ));
    }

    #[rstest]
    fn perp_wallet_order_continuity_rejects_repeated_and_out_of_order_pages() {
        let markets: Vec<DeepXPerpWalletOrderMarket> =
            serde_json::from_str::<DeepXApiResponse<_>>(PERP_ORDERS)
                .unwrap()
                .data;
        let page = perp_wallet_orders_page(markets).unwrap();
        let mut single = page.clone();
        for market in &mut single.markets {
            for subaccount in &mut market.subaccounts {
                subaccount.orders.items.truncate(1);
            }
        }
        let mut continuity = DeepXPerpWalletOrderContinuity::default();
        continuity
            .observe_page(&single, DeepXAccountSortOrder::Descending)
            .unwrap();
        assert!(matches!(
            continuity.observe_page(&single, DeepXAccountSortOrder::Descending),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("repeats wallet order identity"),
        ));

        let mut later = page.clone();
        let first = later
            .markets
            .iter_mut()
            .flat_map(|market| &mut market.subaccounts)
            .flat_map(|subaccount| &mut subaccount.orders.items)
            .next()
            .unwrap();
        first.create_time = "2099-01-01T00:00:00Z".to_string();
        let mut continuity = DeepXPerpWalletOrderContinuity::default();
        continuity
            .observe_page(&page, DeepXAccountSortOrder::Descending)
            .unwrap();
        assert!(matches!(
            continuity.observe_page(&later, DeepXAccountSortOrder::Descending),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("group time order"),
        ));
    }

    #[rstest]
    fn spot_order_page_normalizes_captured_cursor_and_rejects_conflicting_groups() {
        let markets: Vec<DeepXSpotWalletOrderMarket> =
            serde_json::from_str::<DeepXApiResponse<_>>(SPOT_ORDERS)
                .unwrap()
                .data;
        let page = spot_wallet_orders_page(markets.clone()).unwrap();
        assert!(page.has_next);
        assert!(page.next_cursor.is_some());

        let mut conflicting = markets;
        let group = conflicting[0].subaccounts[0].clone();
        conflicting[0].subaccounts.push(group);
        conflicting[0].subaccounts[1].orders.next_cursor = Some("another-cursor".to_string());
        assert!(spot_wallet_orders_page(conflicting).is_err());
    }

    #[rstest]
    #[case("duplicate-market")]
    #[case("invalid-subaccount")]
    #[case("duplicate-subaccount")]
    #[case("foreign-owner")]
    #[case("wrong-market")]
    #[case("duplicate-order")]
    #[case("wrong-order")]
    #[case("oversized-page")]
    fn spot_wallet_order_page_rejects_invalid_protocol_data(#[case] mutation: &str) {
        let markets: Vec<DeepXSpotWalletOrderMarket> =
            serde_json::from_str::<DeepXApiResponse<_>>(SPOT_ORDERS)
                .unwrap()
                .data;
        let mut page = spot_wallet_orders_page(markets).unwrap();
        let request = DeepXSpotWalletOrdersRequest {
            wallet: Some("0x89bb0946046588f4f257ffc71bd3231aba13474a".to_string()),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        validate_spot_wallet_order_page(&page, &request).unwrap();
        match mutation {
            "duplicate-market" => page.markets.push(page.markets[0].clone()),
            "invalid-subaccount" => {
                page.markets[0].subaccounts[0].subaccount = "invalid".to_string();
            }
            "duplicate-subaccount" => {
                let subaccount = page.markets[0].subaccounts[0].clone();
                page.markets[0].subaccounts.push(subaccount);
            }
            "foreign-owner" => {
                page.markets[0].subaccounts[0].orders.items[0].maker =
                    "0x1111111111111111111111111111111111111111".to_string();
            }
            "wrong-market" => {
                page.markets[0].subaccounts[0].orders.items[0].pair_name = "SOL/USDC".to_string();
            }
            "duplicate-order" => {
                let order = page.markets[0].subaccounts[0].orders.items[0].clone();
                page.markets[0].subaccounts[0].orders.items[1] = order;
            }
            "wrong-order" => {
                page.markets[0].subaccounts[0].orders.items[1].create_time =
                    "2026-09-17T08:00:00Z".to_string();
            }
            "oversized-page" => {
                let order = page.markets[0].subaccounts[0].orders.items[0].clone();
                page.markets[0].subaccounts[0].orders.items.push(order);
            }
            _ => unreachable!(),
        }
        assert!(validate_spot_wallet_order_page(&page, &request).is_err());
    }

    #[rstest]
    fn spot_trade_page_normalizes_captured_cursor_and_rejects_empty_continuation() {
        let markets: Vec<DeepXSpotWalletTradeMarket> =
            serde_json::from_str::<DeepXApiResponse<_>>(SPOT_TRADES)
                .unwrap()
                .data;
        let page = spot_wallet_trades_page(markets.clone()).unwrap();
        assert!(page.has_next);
        assert!(page.next_cursor.is_some());

        let mut invalid = markets;
        invalid[0].subaccounts[0].trades.items.clear();
        assert!(spot_wallet_trades_page(invalid).is_err());
    }

    #[rstest]
    #[case("duplicate-market")]
    #[case("invalid-subaccount")]
    #[case("duplicate-subaccount")]
    #[case("wrong-market")]
    #[case("duplicate-trade")]
    #[case("wrong-order")]
    #[case("oversized-page")]
    fn spot_wallet_trade_page_rejects_invalid_protocol_data(#[case] mutation: &str) {
        let markets: Vec<DeepXSpotWalletTradeMarket> =
            serde_json::from_str::<DeepXApiResponse<_>>(SPOT_TRADES)
                .unwrap()
                .data;
        let mut page = spot_wallet_trades_page(markets).unwrap();
        let request = DeepXSpotWalletTradesRequest {
            wallet: "0x89bb0946046588f4f257ffc71bd3231aba13474a".to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        validate_spot_wallet_trade_page(&page, &request).unwrap();
        match mutation {
            "duplicate-market" => page.markets.push(page.markets[0].clone()),
            "invalid-subaccount" => {
                page.markets[0].subaccounts[0].subaccount = "invalid".to_string();
            }
            "duplicate-subaccount" => {
                let subaccount = page.markets[0].subaccounts[0].clone();
                page.markets[0].subaccounts.push(subaccount);
            }
            "wrong-market" => {
                page.markets[0].subaccounts[0].trades.items[0].pair_name = "SOL/USDC".to_string();
            }
            "duplicate-trade" => {
                page.markets[0].subaccounts[0].trades.items[1].id =
                    page.markets[0].subaccounts[0].trades.items[0].id;
            }
            "wrong-order" => {
                page.markets[0].subaccounts[0].trades.items[1].created_at =
                    "2026-09-17T08:00:00Z".to_string();
            }
            "oversized-page" => {
                let trade = page.markets[0].subaccounts[0].trades.items[0].clone();
                page.markets[0].subaccounts[0].trades.items.push(trade);
            }
            _ => unreachable!(),
        }
        assert!(validate_spot_wallet_trade_page(&page, &request).is_err());
    }

    #[rstest]
    fn perp_order_page_normalizes_captured_cursor_and_rejects_conflicting_groups() {
        let markets: Vec<DeepXPerpWalletOrderMarket> =
            serde_json::from_str::<DeepXApiResponse<_>>(PERP_ORDERS)
                .unwrap()
                .data;
        let page = perp_wallet_orders_page(markets.clone()).unwrap();
        assert!(page.has_next);
        assert!(page.next_cursor.is_some());

        let mut conflicting = markets;
        conflicting[0].subaccounts[1].orders.next_cursor = Some("another-cursor".to_string());
        assert!(perp_wallet_orders_page(conflicting).is_err());
    }

    #[rstest]
    #[case("duplicate-market")]
    #[case("invalid-subaccount")]
    #[case("duplicate-subaccount")]
    #[case("foreign-owner")]
    #[case("wrong-market")]
    #[case("duplicate-order")]
    #[case("wrong-order")]
    #[case("oversized-page")]
    fn perp_wallet_order_page_rejects_invalid_protocol_data(#[case] mutation: &str) {
        let markets: Vec<DeepXPerpWalletOrderMarket> =
            serde_json::from_str::<DeepXApiResponse<_>>(PERP_ORDERS)
                .unwrap()
                .data;
        let mut page = perp_wallet_orders_page(markets).unwrap();
        let request = DeepXPerpWalletOrdersRequest {
            wallet: WALLET.to_string(),
            market_name: None,
            market_id: None,
            is_long: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        validate_perp_wallet_order_page(&page, &request).unwrap();
        match mutation {
            "duplicate-market" => page.markets.push(page.markets[0].clone()),
            "invalid-subaccount" => {
                page.markets[0].subaccounts[0].subaccount = "invalid".to_string();
            }
            "duplicate-subaccount" => {
                let subaccount = page.markets[0].subaccounts[0].clone();
                page.markets[0].subaccounts.push(subaccount);
            }
            "foreign-owner" => {
                page.markets[0].subaccounts[0].orders.items[0].owner =
                    "0x1111111111111111111111111111111111111111".to_string();
            }
            "wrong-market" => page.markets[0].subaccounts[0].orders.items[0].market_id = 4,
            "duplicate-order" => {
                let order = page.markets[0].subaccounts[0].orders.items[0].clone();
                page.markets[0].subaccounts[0].orders.items[1] = order;
            }
            "wrong-order" => {
                page.markets[0].subaccounts[0].orders.items[1].create_time =
                    "2026-09-17T00:00:00Z".to_string();
                page.markets[0].subaccounts[0].orders.items[1].updated_time =
                    Some("2026-09-17T00:00:00Z".to_string());
            }
            "oversized-page" => {
                let order = page.markets[0].subaccounts[0].orders.items[0].clone();
                page.markets[0].subaccounts[0].orders.items.push(order);
            }
            _ => unreachable!(),
        }
        assert!(validate_perp_wallet_order_page(&page, &request).is_err());
    }

    fn account() -> DeepXDelegateAccount {
        serde_json::from_str(
            r#"{"delegateAddress":"0x1b856b9bf1d0ceeb0927a081c759813cd703a54f","delegateName":"One-Click Trading","validUntil":1804997046158,"mode":"PlaceOrCancelOrder","createTime":1789445050397}"#,
        )
        .unwrap()
    }

    #[rstest]
    #[case("address")]
    #[case("duplicate")]
    #[case("timestamp")]
    fn delegate_directory_rejects_invalid_records(#[case] mutation: &str) {
        let mut accounts = vec![account()];
        match mutation {
            "address" => accounts[0].delegate_address = "invalid".to_string(),
            "duplicate" => accounts.push(accounts[0].clone()),
            "timestamp" => accounts[0].valid_until = accounts[0].create_time,
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_delegate_accounts(&accounts),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "delegate accounts",
                ..
            })
        ));
    }

    #[rstest]
    fn delegate_directory_accepts_legacy_nonexpiring_record() {
        let mut account = account();
        account.valid_until = 0;
        validate_delegate_accounts(&[account]).unwrap();
    }

    #[rstest]
    fn delegate_directory_rejects_unknown_mode_and_invalid_reverse_directory() {
        assert!(serde_json::from_str::<DeepXDelegateAccount>(
            r#"{"delegateAddress":"0x1b856b9bf1d0ceeb0927a081c759813cd703a54f","delegateName":"One-Click Trading","validUntil":1804997046158,"mode":"FutureMode","createTime":1789445050397}"#,
        ).is_err());
        assert!(validate_delegator_wallets(&["invalid".to_string()]).is_err());
        let duplicate = format!("0x{}", WALLET[2..].to_ascii_uppercase());
        assert!(validate_delegator_wallets(&[WALLET.to_string(), duplicate]).is_err());
    }

    #[rstest]
    #[case("address")]
    #[case("duplicate")]
    #[case("current-count")]
    #[case("created-count")]
    #[case("negative-amount")]
    fn user_stats_reject_invalid_responses(#[case] mutation: &str) {
        let mut response: serde_json::Value = serde_json::from_str(USER_STATS).unwrap();
        match mutation {
            "address" => response["data"]["subaccounts"][0] = serde_json::json!("invalid"),
            "duplicate" => {
                response["data"]["subaccounts"][1] = response["data"]["subaccounts"][0].clone();
            }
            "current-count" => response["data"]["numberOfSubAccounts"] = serde_json::json!(3),
            "created-count" => {
                response["data"]["numberOfSubAccountsCreated"] = serde_json::json!(3);
            }
            "negative-amount" => {
                response["data"]["ifStakedQuoteAssetAmount"] = serde_json::json!(-1);
            }
            _ => unreachable!(),
        }
        let stats: DeepXUserStats = serde_json::from_value(response["data"].clone()).unwrap();
        assert!(matches!(
            validate_user_stats(&stats),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "user stats",
                ..
            })
        ));
    }

    #[rstest]
    fn user_stats_accept_captured_response() {
        let response: DeepXApiResponse<DeepXUserStats> = serde_json::from_str(USER_STATS).unwrap();
        validate_user_stats(&response.data).unwrap();
    }

    #[rstest]
    fn invitation_codes_preserve_values_and_reject_empty_or_duplicate_codes() {
        let codes = vec![" First ".to_string(), "first".to_string()];
        validate_invitation_codes(&codes).unwrap();
        assert_eq!(codes[0], " First ");

        for invalid in [vec!["  ".to_string()], vec!["same".to_string(); 2]] {
            assert!(matches!(
                validate_invitation_codes(&invalid),
                Err(DeepXRestError::InvalidAccountResponse {
                    endpoint: "invitation codes",
                    ..
                })
            ));
        }
    }

    #[rstest]
    fn invitation_status_binds_both_response_shapes_to_queried_wallet() {
        let flag =
            invitation_status_for_wallet(DeepXInvitationStatusWire::Flag(true), WALLET).unwrap();
        assert_eq!(flag.address, WALLET);
        assert!(flag.is_bound);

        let upper = format!("0x{}", WALLET[2..].to_ascii_uppercase());
        let detailed = DeepXInvitationStatusWire::Detailed(DeepXInvitationStatus {
            address: upper.clone(),
            is_bound: false,
        });
        assert_eq!(
            invitation_status_for_wallet(detailed, WALLET)
                .unwrap()
                .address,
            upper
        );

        let other = DeepXInvitationStatusWire::Detailed(DeepXInvitationStatus {
            address: "0x1111111111111111111111111111111111111111".to_string(),
            is_bound: true,
        });
        assert!(matches!(
            invitation_status_for_wallet(other, WALLET),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "invitation status",
                ..
            })
        ));
    }

    #[rstest]
    fn subaccount_balances_reject_duplicate_assets_and_negative_borrowing() {
        let mut balances: DeepXSubaccountBalances =
            serde_json::from_str::<DeepXApiResponse<_>>(SUBACCOUNT_BALANCES)
                .unwrap()
                .data;
        let address = balances.address.clone();
        validate_subaccount_balances(&balances, &address).unwrap();

        balances.assets[1].symbol = "usdc".to_string();
        assert!(validate_subaccount_balances(&balances, &address).is_err());
        balances.assets[1].symbol = "ETH".to_string();
        balances.assets[1].balance_borrowed = Decimal::NEGATIVE_ONE;
        assert!(validate_subaccount_balances(&balances, &address).is_err());
    }

    #[rstest]
    #[case("address")]
    #[case("authority")]
    #[case("status")]
    #[case("strategy")]
    #[case("height")]
    #[case("created-at")]
    fn subaccount_profile_rejects_invalid_response(#[case] mutation: &str) {
        let mut profile: DeepXSubaccountProfile =
            serde_json::from_str::<DeepXApiResponse<_>>(SUBACCOUNT_PROFILE)
                .unwrap()
                .data;
        let address = profile.address.clone();
        validate_subaccount_profile(&profile, &address).unwrap();
        match mutation {
            "address" => profile.address = "0x1111111111111111111111111111111111111111".to_string(),
            "authority" => profile.authority = "invalid".to_string(),
            "status" => profile.status = "FutureStatus".to_string(),
            "strategy" => profile.margin_strategy = "FutureStrategy".to_string(),
            "height" => profile.height = 0,
            "created-at" => profile.created_at = u64::MAX,
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_subaccount_profile(&profile, &address),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "subaccount info",
                ..
            })
        ));
    }

    #[rstest]
    fn subaccount_equity_rejects_wrong_account_and_negative_borrowing() {
        let mut equity: DeepXSubaccountEquity =
            serde_json::from_str::<DeepXApiResponse<_>>(SUBACCOUNT_EQUITY)
                .unwrap()
                .data;
        let address = equity.subaccount.clone();
        validate_subaccount_equity(&equity, &address).unwrap();
        assert!(matches!(
            validate_subaccount_equity(&equity, WALLET),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "subaccount equity",
                ..
            })
        ));
        equity.total_borrows_usd = Decimal::NEGATIVE_ONE;
        assert!(validate_subaccount_equity(&equity, &address).is_err());
    }

    #[rstest]
    fn subaccount_margin_ratio_checks_exact_arithmetic_and_missing_values() {
        let mut margin: DeepXSubaccountMarginRatio =
            serde_json::from_str::<DeepXApiResponse<_>>(SUBACCOUNT_MARGIN_RATIO)
                .unwrap()
                .data;
        validate_subaccount_margin_ratio(&margin).unwrap();

        margin.margin_required = Decimal::NEGATIVE_ONE;
        assert!(validate_subaccount_margin_ratio(&margin).is_err());
        margin.margin_required = Decimal::ZERO;
        margin.exact_margin.as_mut().unwrap().maint_margin_req = Decimal::ONE;
        assert!(validate_subaccount_margin_ratio(&margin).is_err());
        margin.exact_margin.as_mut().unwrap().total_collateral = Decimal::ZERO;
        margin.exact_margin.as_mut().unwrap().uni_mmr = Some(Decimal::ZERO);
        validate_subaccount_margin_ratio(&margin).unwrap();
        margin.exact_margin.as_mut().unwrap().total_collateral = Decimal::ONE;
        margin.exact_margin.as_mut().unwrap().maint_margin_req = Decimal::new(3, 0);
        margin.exact_margin.as_mut().unwrap().uni_mmr = Some(Decimal::new(33_333_333, 8));
        validate_subaccount_margin_ratio(&margin).unwrap();
        margin.exact_margin.as_mut().unwrap().total_collateral = Decimal::new(101, 2);
        margin.exact_margin.as_mut().unwrap().uni_mmr = Some(Decimal::new(33_666_666, 8));
        validate_subaccount_margin_ratio(&margin).unwrap();
        margin.exact_margin.as_mut().unwrap().uni_mmr = Some(Decimal::new(33_666_667, 8));
        assert!(validate_subaccount_margin_ratio(&margin).is_err());
        margin.exact_margin.as_mut().unwrap().maint_margin_req = Decimal::ZERO;
        assert!(validate_subaccount_margin_ratio(&margin).is_err());
        margin.exact_margin.as_mut().unwrap().uni_mmr = None;
        margin.exact_margin.as_mut().unwrap().total_collateral = Decimal::NEGATIVE_ONE;
        assert!(validate_subaccount_margin_ratio(&margin).is_err());
        margin.exact_margin = None;
        assert!(matches!(
            validate_subaccount_margin_ratio(&margin),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "subaccount margin ratio",
                ..
            })
        ));
    }
}
