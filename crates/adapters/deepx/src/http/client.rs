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

//! Read-only HTTP transport for the DeepX testnet API.

#[cfg(test)]
use std::collections::HashMap;
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[cfg(test)]
use deepx_rust_sdk::query::DEEPX_MAX_HISTORY_PAGE_SIZE;
use deepx_rust_sdk::rest::{
    DeepXLiquidationExecutionRoute, DeepXRestError, DeepXRestResponse, DeepXRestTransport,
    collect_all_subaccount_pages, collect_all_subaccount_pages_raw, collect_balance_change_pages,
    collect_balance_change_pages_raw, collect_hourly_unsettled_funding_pages,
    collect_liquidation_adl_record_pages_raw, collect_liquidation_execution_record_pages_raw,
    collect_liquidation_record_pages, collect_liquidation_record_pages_raw,
    collect_perp_account_trade_pages, collect_perp_account_trade_pages_raw,
    collect_perp_candle_history, collect_perp_funding_fee_pages,
    collect_perp_funding_fee_pages_raw, collect_perp_funding_rates_history_limited,
    collect_perp_history_order_pages, collect_perp_history_order_pages_raw,
    collect_perp_long_short_ratios_history_limited, collect_perp_open_interest_history,
    collect_perp_open_order_pages, collect_perp_open_order_pages_raw,
    collect_perp_position_order_pages, collect_perp_position_order_pages_raw,
    collect_perp_position_pages, collect_perp_position_pages_raw, collect_perp_trade_history,
    collect_perp_wallet_order_pages, collect_perp_wallet_trade_pages_for_subaccount,
    collect_quota_history_pages, collect_spot_account_trade_pages,
    collect_spot_account_trade_pages_raw, collect_spot_candle_history,
    collect_spot_history_order_pages, collect_spot_history_order_pages_raw,
    collect_spot_open_order_pages, collect_spot_trade_pages, collect_spot_trades_limited,
    collect_spot_wallet_order_pages, collect_spot_wallet_trade_pages,
    collect_wallet_funding_fee_pages, collect_wallet_funding_fee_pages_raw,
    validate_account_address,
};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_network::retry::{RetryConfig, RetryManager};

use super::{
    error::{DeepXHttpError, Result},
    models::{
        DeepXAccountPage, DeepXBalanceChangeRecord, DeepXDelegateAccount, DeepXDelegateWallets,
        DeepXFundingRatePage, DeepXFundingRateRecord, DeepXHourlyUnsettledFundingRecord,
        DeepXInvitationStatus, DeepXLendingAsset, DeepXLendingInterestRateHistory,
        DeepXLendingInterestRateParams, DeepXLendingStatusHistory, DeepXLiquidationRecord,
        DeepXLongShortRatioPage, DeepXLongShortRatioRecord, DeepXOpenInterestPage,
        DeepXPerpAccountTradeRecord, DeepXPerpCandlesPage, DeepXPerpFundingFeeRecord,
        DeepXPerpLastPrice, DeepXPerpLiquidationPrice, DeepXPerpMarket, DeepXPerpMarketLookup,
        DeepXPerpOrderBook, DeepXPerpOrderRecord, DeepXPerpPositionRecord, DeepXPerpTrade,
        DeepXPerpTradesPage, DeepXPerpVolume, DeepXPerpWalletOrdersPage,
        DeepXPerpWalletTradeMarket, DeepXQuotaHistoryRecord, DeepXQuotaSummary,
        DeepXRawAccountPage, DeepXSpotAccountTradeRecord, DeepXSpotAdlCandidates,
        DeepXSpotCandlesPage, DeepXSpotLastPrice, DeepXSpotMarket, DeepXSpotOrderBook,
        DeepXSpotOrderRecord, DeepXSpotTrade, DeepXSpotTradesPage, DeepXSpotVolume,
        DeepXSpotWalletOrdersPage, DeepXSpotWalletTradesPage, DeepXSubaccountBalances,
        DeepXSubaccountDirectoryRecord, DeepXSubaccountEquity, DeepXSubaccountMarginRatio,
        DeepXSubaccountPerpPositions, DeepXSubaccountProfile, DeepXSubaccountSnapshot,
        DeepXUserStats, DeepXWalletAccountSnapshot, DeepXWalletDelegateAccounts,
        DeepXWalletPerpPositions, DeepXWalletSubaccounts, wrap_account_page,
    },
    retry::{deepx_http_retry_config, should_retry_http_error},
};
use crate::{
    account::{
        DeepXAccountOwnershipError, DeepXAccountOwnershipProof, verify_account_ownership,
        verify_delegated_account_ownership,
    },
    common::DeepXPrivateKey,
    signing::derive_signer_account_id,
};
#[cfg(test)]
use deepx_rust_sdk::query::DeepXAccountSortOrder;
use deepx_rust_sdk::query::{
    DeepXAllSubaccountsRequest, DeepXBalanceChangesRequest, DeepXFundingRateRequest,
    DeepXHourlyUnsettledFundingRequest, DeepXLendingHistoryRequest, DeepXLendingMarketRequest,
    DeepXLiquidationAdlRecordsRequest, DeepXLiquidationExecutionRecordsRequest,
    DeepXLiquidationRecordsRequest, DeepXLongShortRatioRequest, DeepXOpenInterestRequest,
    DeepXPerpAccountTradesRequest, DeepXPerpCandlesRequest, DeepXPerpFundingFeeRequest,
    DeepXPerpHistoryOrdersRequest, DeepXPerpLastPriceRequest, DeepXPerpLiquidationPriceRequest,
    DeepXPerpMarkPriceRequest, DeepXPerpOpenOrdersRequest, DeepXPerpOraclePriceRequest,
    DeepXPerpOrderBookRequest, DeepXPerpPositionOrdersRequest, DeepXPerpPositionsRequest,
    DeepXPerpTradesHistoryRequest, DeepXPerpTradesQuery, DeepXPerpTradesRequest,
    DeepXPerpVolumeRequest, DeepXPerpWalletOrdersRequest, DeepXPerpWalletTradesRequest,
    DeepXQuotaHistoryRequest, DeepXSpotAccountTradesRequest, DeepXSpotAdlCandidatesRequest,
    DeepXSpotCandlesRequest, DeepXSpotHistoryOrdersRequest, DeepXSpotLastPriceRequest,
    DeepXSpotOpenOrdersRequest, DeepXSpotOrderBookRequest, DeepXSpotOrderByIdRequest,
    DeepXSpotTradesRequest, DeepXSpotVolumeRequest, DeepXSpotWalletOrdersRequest,
    DeepXSpotWalletTradesRequest, DeepXWalletFundingFeeRequest, DeepXWalletPerpPositionsRequest,
};

#[cfg(test)]
const SPOT_MARKETS_PATH: &str = "/internal/v1/market/spot/markets";
#[cfg(test)]
const PERP_FUNDING_RATE_PATH: &str = "/internal/v1/market/perp/funding_rate";
#[cfg(test)]
const PERP_LONG_SHORT_RATIO_PATH: &str = "/internal/v1/market/perp/long_short_ratio";
#[cfg(test)]
const PERP_OPEN_INTEREST_PATH: &str = "/internal/v1/market/perp/open_interest";
#[cfg(test)]
const PERP_TRADES_PATH: &str = "/internal/v1/market/perp/trades";
#[cfg(test)]
const PERP_CANDLES_PATH: &str = "/internal/v1/market/perp/candles";
#[cfg(test)]
const CANDLE_PAGE_SIZE: u32 = DEEPX_MAX_HISTORY_PAGE_SIZE;
#[cfg(test)]
const PERP_MARK_PRICE_PATH: &str = "/internal/v1/market/perp/mark_price";
#[cfg(test)]
const PERP_ORACLE_PRICE_PATH: &str = "/internal/v1/market/perp/oracle_price";
#[cfg(test)]
const SPOT_TRADES_PATH: &str = "/internal/v1/market/spot/trades";
#[cfg(test)]
const SPOT_CANDLES_PATH: &str = "/internal/v1/market/spot/candles";
#[cfg(test)]
const PERP_OPEN_ORDERS_PATH: &str = "/internal/v1/account/perp/open-orders";
#[cfg(test)]
const PERP_HISTORY_ORDERS_PATH: &str = "/internal/v1/account/perp/history-orders";
#[cfg(test)]
const SPOT_OPEN_ORDERS_PATH: &str = "/internal/v1/account/spot/open-orders";
#[cfg(test)]
const SPOT_HISTORY_ORDERS_PATH: &str = "/internal/v1/account/spot/history-orders";
#[cfg(test)]
const SPOT_ACCOUNT_TRADES_PATH: &str = "/internal/v1/account/spot/trades";
#[cfg(test)]
const SPOT_WALLET_ORDERS_PATH: &str = "/internal/v1/account/spot/orders-by-wallet";
#[cfg(test)]
const SPOT_WALLET_TRADES_PATH: &str = "/internal/v1/account/spot/trades-by-wallet";
#[cfg(test)]
const PERP_ACCOUNT_TRADES_PATH: &str = "/internal/v1/account/perp/trades";
#[cfg(test)]
const PERP_WALLET_ORDERS_PATH: &str = "/internal/v1/account/perp/orders-by-wallet";
#[cfg(test)]
const PERP_WALLET_TRADES_PATH: &str = "/internal/v1/account/perp/trades-by-wallet";
#[cfg(test)]
const WALLET_FUNDING_FEE_PATH: &str = "/internal/v1/account/funding-fee-by-wallet";
#[cfg(test)]
const HOURLY_UNSETTLED_FUNDING_PATH: &str = "/internal/v1/account/hourly-unsettled-funding";
#[cfg(test)]
const PERP_POSITIONS_PATH: &str = "/internal/v1/account/position";
#[cfg(test)]
const BALANCE_CHANGES_PATH: &str = "/internal/v1/account/balance-changes";
#[cfg(test)]
const LIQUIDATION_RECORDS_PATH: &str = "/internal/v1/account/subaccounts/liquidation-records";
#[cfg(test)]
const LIQUIDATION_EXECUTION_RECORDS_PATH: &str = "/internal/v1/liquidation/records";
#[cfg(test)]
const USER_STATS_PATH: &str = "/internal/v1/account/user-stats";
#[cfg(test)]
const QUOTA_HISTORY_PATH: &str = "/internal/v1/account/quota/history";
#[cfg(test)]
const ALL_SUBACCOUNTS_PATH: &str = "/internal/v1/account/subaccounts/all";
#[cfg(test)]
const SUBACCOUNT_INFO_PATH: &str = "/internal/v1/account/subaccount-info";
#[cfg(test)]
const SUBACCOUNT_BALANCES_PATH: &str = "/internal/v1/account/balances";
#[cfg(test)]
const SUBACCOUNT_EQUITY_PATH: &str = "/internal/v1/account/equity";
#[cfg(test)]
const SUBACCOUNT_MARGIN_RATIO_PATH: &str = "/internal/v1/account/margin-ratio";
#[cfg(test)]
const DELEGATE_ACCOUNTS_PATH: &str = "/internal/v1/account/delegate-accounts";
#[cfg(test)]
const DELEGATOR_ACCOUNTS_PATH: &str = "/internal/v1/account/delegator-accounts";

/// Raw client for unauthenticated, idempotent DeepX HTTP reads.
#[derive(Clone, Debug)]
pub struct DeepXHttpClient {
    client: DeepXRestTransport,
    base_urls: Arc<[String]>,
    retry_manager: Arc<RetryManager<DeepXHttpError>>,
}

impl DeepXHttpClient {
    /// Creates a public read-only client from validated network and retry configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the network configuration or HTTP client construction is invalid.
    pub fn from_network_config(
        config: &crate::config::DeepXNetworkConfig,
        timeout_secs: Option<u64>,
        proxy_url: Option<String>,
    ) -> Result<Self> {
        let base_urls = config
            .rest_urls()
            .map_err(|error| DeepXHttpError::InvalidRequest(error.to_string()))?;
        let retry_config = config
            .http_read_retry
            .to_retry_config()
            .map_err(|error| DeepXHttpError::InvalidRequest(error.to_string()))?;
        Self::new_with_endpoints(base_urls, timeout_secs, proxy_url, retry_config)
    }

    /// Creates a public read-only DeepX HTTP client.
    ///
    /// # Errors
    ///
    /// Returns [`DeepXHttpError::Transport`] when the shared HTTP client cannot be constructed.
    pub fn new(
        base_url: impl Into<String>,
        timeout_secs: Option<u64>,
        proxy_url: Option<String>,
    ) -> Result<Self> {
        Self::new_with_endpoints(
            [base_url.into()],
            timeout_secs,
            proxy_url,
            deepx_http_retry_config(),
        )
    }

    /// Creates a public client with ordered testnet failover endpoints.
    ///
    /// The first endpoint is primary. Retryable reads rotate through subsequent endpoints and
    /// return to the primary only after exhausting the configured list.
    ///
    /// # Errors
    ///
    /// Returns an error when no endpoint is provided, an endpoint is not an HTTP base URL, or the
    /// shared HTTP client cannot be constructed.
    pub fn new_with_endpoints(
        base_urls: impl IntoIterator<Item = String>,
        timeout_secs: Option<u64>,
        proxy_url: Option<String>,
        retry_config: RetryConfig,
    ) -> Result<Self> {
        let base_urls = base_urls
            .into_iter()
            .map(deepx_rust_sdk::rest::normalize_base_url)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if base_urls.is_empty() {
            return Err(DeepXHttpError::InvalidBaseUrl(String::new()));
        }
        let client = DeepXRestTransport::new(timeout_secs, proxy_url)?;
        Ok(Self {
            client,
            base_urls: base_urls.into(),
            retry_manager: Arc::new(RetryManager::new(retry_config)),
        })
    }

    /// Returns the configured base URL without a trailing slash.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_urls[0]
    }

    pub(crate) async fn post_transaction_once(
        &self,
        body: Vec<u8>,
    ) -> std::result::Result<DeepXRestResponse, DeepXHttpError> {
        Ok(self
            .client
            .post_transaction_once(self.base_url(), body)
            .await?)
    }

    pub(crate) async fn get_transaction_status_once(
        &self,
        extrinsic_hash: [u8; 32],
    ) -> Result<deepx_rust_sdk::rest::status::DeepXRestTransactionStatusData> {
        Ok(self
            .client
            .get_transaction_status(self.base_url(), extrinsic_hash)
            .await?)
    }

    /// Returns all configured base URLs in failover order.
    #[must_use]
    pub fn base_urls(&self) -> &[String] {
        &self.base_urls
    }

    /// Returns all validated Spot market metadata.
    ///
    /// # Errors
    ///
    /// Returns an error for transport or venue failures, malformed metadata, invalid identities or
    /// financial fields, or duplicate market identities.
    pub async fn get_spot_markets(&self) -> Result<Vec<DeepXSpotMarket>> {
        self.get_sdk_read_with_retry(|index| self.client.get_spot_markets(&self.base_urls[index]))
            .await
    }

    pub(crate) async fn get_spot_market_entries(&self) -> Result<Vec<DeepXSpotMarket>> {
        self.get_sdk_read_with_retry(|index| {
            self.client.get_spot_market_entries(&self.base_urls[index])
        })
        .await
    }

    /// Returns validated Spot market metadata selected by its venue name.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty name, transport or venue failures, malformed metadata, an
    /// identity mismatch, or invalid financial fields.
    pub async fn get_spot_market_by_name(&self, name: &str) -> Result<DeepXSpotMarket> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_market_by_name(&self.base_urls[index], name)
        })
        .await
    }

    /// Returns validated Spot market metadata selected by its bytes32 pair identity.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid pair, transport or venue failures, malformed metadata, an
    /// identity mismatch, or invalid financial fields.
    pub async fn get_spot_market_by_pair(&self, pair: &str) -> Result<DeepXSpotMarket> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_market_by_pair(&self.base_urls[index], pair)
        })
        .await
    }

    /// Returns all available perpetual market metadata.
    ///
    /// # Errors
    ///
    /// Returns a typed transport, HTTP status, response decoding, or venue API error.
    pub async fn get_perp_markets(&self) -> Result<Vec<DeepXPerpMarket>> {
        self.get_sdk_read_with_retry(|index| self.client.get_perp_markets(&self.base_urls[index]))
            .await
    }

    /// Returns validated perpetual market details selected by the deployment market ID.
    ///
    /// The response intentionally omits fields required for instrument construction. Use
    /// [`Self::get_perp_markets`] for complete directory metadata.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero market ID, transport or venue failures, malformed metadata, an
    /// identity mismatch, or invalid financial fields.
    pub async fn get_perp_market_by_id(&self, market_id: u64) -> Result<DeepXPerpMarketLookup> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_market_by_id(&self.base_urls[index], market_id)
        })
        .await
    }

    /// Returns validated perpetual market details selected by its venue name.
    ///
    /// The response intentionally omits fields required for instrument construction. Use
    /// [`Self::get_perp_markets`] for complete directory metadata.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty name, transport or venue failures, malformed metadata, an
    /// identity mismatch, or invalid financial fields.
    pub async fn get_perp_market_by_name(&self, name: &str) -> Result<DeepXPerpMarketLookup> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_market_by_name(&self.base_urls[index], name)
        })
        .await
    }

    /// Returns the validated public lending asset directory.
    ///
    /// Asset precision, pool status, and instrument semantics are not inferred.
    ///
    /// # Errors
    ///
    /// Returns an error for transport or venue failures, malformed responses, invalid fields, or
    /// duplicate market/asset identities.
    pub async fn get_lending_assets(&self) -> Result<Vec<DeepXLendingAsset>> {
        let assets = self
            .get_sdk_read_with_retry(|index| self.client.get_lending_assets(&self.base_urls[index]))
            .await?;
        Ok(assets.into_iter().map(Into::into).collect())
    }

    /// Returns validated exact borrow-rate curves under optional market and asset filters.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid filters, transport or venue failures, malformed responses,
    /// filter mismatches, invalid curve nodes, or duplicate market/asset identities.
    pub async fn get_lending_interest_rate_params(
        &self,
        request: &DeepXLendingMarketRequest,
    ) -> Result<Vec<DeepXLendingInterestRateParams>> {
        let params = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_lending_interest_rate_params(&self.base_urls[index], request)
            })
            .await?;
        Ok(params.into_iter().map(Into::into).collect())
    }

    /// Returns one validated lending supply and borrow APR history response.
    ///
    /// No compounding, payment, yield, or framework event semantics are inferred.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid filters or bounds, transport or venue failures, malformed
    /// responses, filter/range/order mismatches, invalid values, or duplicate buckets.
    pub async fn get_lending_interest_rate_history(
        &self,
        request: &DeepXLendingHistoryRequest,
    ) -> Result<DeepXLendingInterestRateHistory> {
        let history = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_lending_interest_rate_history(&self.base_urls[index], request)
            })
            .await?;
        Ok(
            deepx_rust_sdk::rest::models::DeepXLendingInterestRateHistory {
                details: history.details.into_iter().map(Into::into).collect(),
            },
        )
    }

    /// Returns one validated lending pool-status history response.
    ///
    /// Asset units and framework market-status semantics are not inferred.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid filters or bounds, transport or venue failures, malformed
    /// responses, filter/range/order mismatches, invalid values, or duplicate buckets.
    pub async fn get_lending_status_history(
        &self,
        request: &DeepXLendingHistoryRequest,
    ) -> Result<DeepXLendingStatusHistory> {
        let history = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_lending_status_history(&self.base_urls[index], request)
            })
            .await?;
        Ok(deepx_rust_sdk::rest::models::DeepXLendingStatusHistory {
            details: history.details.into_iter().map(Into::into).collect(),
        })
    }

    /// Returns one ascending page of perpetual funding-rate history.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, transport or HTTP failures, malformed responses, or a
    /// venue-level failure envelope.
    pub async fn get_perp_funding_rates(
        &self,
        request: &DeepXFundingRateRequest,
    ) -> Result<DeepXFundingRatePage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_funding_rates(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns up to `limit` most recent one-minute funding-rate samples in descending order.
    ///
    /// Requires a fixed upper bound and a nonzero page budget. Retains the range on every page
    /// and rejects duplicate buckets, ordering violations, malformed cursors, and oversized pages.
    /// These are sampled rates, not funding payments or a payment schedule.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds/page sizes, HTTP failures, invalid response identity,
    /// bucket/order/range violations, or exhausted cursor budgets.
    pub async fn get_perp_funding_rates_history_limited(
        &self,
        request: &DeepXFundingRateRequest,
        limit: std::num::NonZeroUsize,
        max_pages: usize,
    ) -> Result<Vec<DeepXFundingRateRecord>> {
        collect_perp_funding_rates_history_limited(
            request,
            limit,
            max_pages,
            |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_perp_funding_rates_descending(&self.base_urls[index], &page_request)
                })
                .await
            },
        )
        .await
    }

    /// Returns one ascending page of perpetual long-short ratio history.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, transport or HTTP failures, malformed responses, or a
    /// venue-level failure envelope.
    pub async fn get_perp_long_short_ratios(
        &self,
        request: &DeepXLongShortRatioRequest,
    ) -> Result<DeepXLongShortRatioPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_long_short_ratios(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns up to `limit` ascending long-short ratio observations under a page budget.
    ///
    /// Requires a fixed upper bound. Time filters remain on every cursor page, and market
    /// identity, range, ordering, response size, and cursor progress are validated.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds or budgets, HTTP failures, invalid response identity,
    /// out-of-range or unordered observations, oversized pages, or exhausted cursor budgets.
    pub async fn get_perp_long_short_ratios_history_limited(
        &self,
        request: &DeepXLongShortRatioRequest,
        limit: std::num::NonZeroUsize,
        max_pages: usize,
    ) -> Result<Vec<DeepXLongShortRatioRecord>> {
        collect_perp_long_short_ratios_history_limited(
            request,
            limit,
            max_pages,
            |page_request| async move { self.get_perp_long_short_ratios(&page_request).await },
        )
        .await
    }

    /// Returns one validated ascending page of perpetual open-interest history.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, transport or HTTP failures, malformed responses, or a
    /// venue-level failure envelope.
    pub async fn get_perp_open_interest(
        &self,
        request: &DeepXOpenInterestRequest,
    ) -> Result<DeepXOpenInterestPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_open_interest(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns complete bounded ascending open-interest history using one-minute time cursors.
    ///
    /// The venue rounds bounds down to minutes and excludes the `end` minute. The collector rounds
    /// the requested interval outward to return only buckets within the caller's inclusive bounds.
    /// Each continuation starts at the minute after the preceding page's final observation.
    /// No partial result is returned on budget exhaustion.
    ///
    /// # Errors
    ///
    /// Returns an error for missing bounds, invalid pages, changed market identity, repeated or
    /// out-of-range buckets, or a history exceeding the page budget.
    pub async fn get_perp_open_interest_history(
        &self,
        request: &DeepXOpenInterestRequest,
        max_pages: usize,
    ) -> Result<DeepXOpenInterestPage> {
        collect_perp_open_interest_history(request, max_pages, |page_request| async move {
            self.get_perp_open_interest(&page_request).await
        })
        .await
    }

    /// Returns one descending page of raw perpetual trades.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request parameters, transport or HTTP failures, malformed
    /// responses, or a venue-level failure envelope.
    pub async fn get_perp_trades(
        &self,
        request: &DeepXPerpTradesRequest,
    ) -> Result<DeepXPerpTradesPage> {
        request.validate()?;
        let query = request.as_query();
        self.get_perp_trades_page(&query).await
    }

    async fn get_perp_trades_page(
        &self,
        query: &DeepXPerpTradesQuery<'_>,
    ) -> Result<DeepXPerpTradesPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client.get_perp_trades(&self.base_urls[index], query)
        })
        .await
    }

    /// Returns complete raw perpetual trade history in descending order within inclusive bounds.
    ///
    /// Time filters are retained on every cursor page. Page-budget exhaustion, duplicate IDs,
    /// invalid timestamps, out-of-range records, and ascending page boundaries fail the whole
    /// operation. Equal timestamps with distinct IDs and empty terminal pages are accepted.
    /// No financial units or taker roles are inferred and no framework events are emitted.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid budgets or bounds, HTTP failures, malformed responses,
    /// market mismatches, invalid history records, or cursor progress failures.
    pub async fn get_perp_trades_history(
        &self,
        request: &DeepXPerpTradesHistoryRequest,
    ) -> Result<Vec<DeepXPerpTrade>> {
        self.get_perp_trades_history_inner(request, None).await
    }

    /// Returns up to `limit` most recent raw trades in the inclusive requested range.
    ///
    /// Unlike the complete reader, reaching the explicit record limit succeeds without reading
    /// the remaining pages. The same identity, ordering, cursor, and page-budget checks apply.
    ///
    /// # Errors
    ///
    /// Returns the same validation, transport, and history errors as the complete reader.
    pub async fn get_perp_trades_history_limited(
        &self,
        request: &DeepXPerpTradesHistoryRequest,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<DeepXPerpTrade>> {
        self.get_perp_trades_history_inner(request, Some(limit))
            .await
    }

    async fn get_perp_trades_history_inner(
        &self,
        request: &DeepXPerpTradesHistoryRequest,
        limit: Option<std::num::NonZeroUsize>,
    ) -> Result<Vec<DeepXPerpTrade>> {
        collect_perp_trade_history(request, limit, |page_request, cursor| async move {
            let query = page_request.as_query(cursor.as_deref());
            self.get_perp_trades_page(&query).await
        })
        .await
    }

    /// Returns one validated globally ordered page of exact raw Spot executions.
    ///
    /// The optional wallet filter remains a venue observation because this request does not join
    /// returned counterparties to a separately mutable wallet directory. No quantity precision,
    /// fee asset, aggressor-side framework semantics, or [`nautilus_model::data::TradeTick`] is
    /// inferred.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request parameters, transport or HTTP failures, malformed
    /// responses, cursor metadata, market-scope mismatches, or invalid trade records.
    pub async fn get_spot_trades(
        &self,
        request: &DeepXSpotTradesRequest,
    ) -> Result<DeepXSpotTradesPage> {
        self.get_spot_trades_page(request).await
    }

    /// Returns all validated Spot trade pages within an explicit page budget.
    ///
    /// Request filters and global venue order are retained across pages. Duplicate identities,
    /// ordering violations, cursor failures, or budget exhaustion return no partial collection.
    ///
    /// # Errors
    ///
    /// Returns the single-page errors or an error for an invalid page budget, empty continuation,
    /// repeated cursor, or budget exhaustion.
    pub async fn get_spot_trade_pages(
        &self,
        request: &DeepXSpotTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXSpotTradesPage>> {
        collect_spot_trade_pages(request, max_pages, |page_request| async move {
            self.get_spot_trades_page(&page_request).await
        })
        .await
    }

    /// Returns up to `limit` validated raw Spot trades while preserving global order checks.
    ///
    /// Reaching the explicit record limit succeeds without fetching remaining pages. The same
    /// market, timestamp, identity, cursor, and page-budget checks apply.
    pub async fn get_spot_trades_limited(
        &self,
        request: &DeepXSpotTradesRequest,
        limit: std::num::NonZeroUsize,
        max_pages: usize,
    ) -> Result<Vec<DeepXSpotTrade>> {
        collect_spot_trades_limited(request, limit, max_pages, |page_request| async move {
            self.get_spot_trades_page(&page_request).await
        })
        .await
    }

    async fn get_spot_trades_page(
        &self,
        request: &DeepXSpotTradesRequest,
    ) -> Result<DeepXSpotTradesPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client.get_spot_trades(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns one validated ascending page of exact raw Spot candles.
    ///
    /// The venue bucket timestamp is retained without inferring whether it identifies the open or
    /// close of a complete bucket. Pair-selected responses echo only the market name, so this
    /// method cannot independently bind that response to the requested bytes32 identity.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request parameters, transport or venue failures, malformed
    /// responses, market-name mismatches, invalid OHLC values, ordering, bounds, or page size.
    pub async fn get_spot_candles(
        &self,
        request: &DeepXSpotCandlesRequest,
    ) -> Result<DeepXSpotCandlesPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_candles(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns a complete bounded Spot candle history using ascending time cursors.
    ///
    /// An explicit request limit caps the total result. Without one, the collector reads at most
    /// `max_pages` pages of 5000 candles. No partial result is returned on budget exhaustion.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero page budget, invalid pages, market identity changes, cursor
    /// overflow, or a history exceeding the page budget.
    pub async fn get_spot_candles_history_limited(
        &self,
        request: &DeepXSpotCandlesRequest,
        max_pages: usize,
    ) -> Result<DeepXSpotCandlesPage> {
        request.validate()?;
        let client = self;
        let request = request.clone();
        collect_spot_candle_history(
            "spot candle history",
            request.start_ms,
            request.end_ms,
            request.limit,
            max_pages,
            move |start_ms, limit| {
                let mut page_request = request.clone();
                page_request.start_ms = start_ms;
                page_request.limit = Some(limit);
                async move { client.get_spot_candles(&page_request).await }
            },
        )
        .await
    }

    /// Returns one validated Spot volume-statistics window.
    ///
    /// The response does not echo its market selector. Volume units, window-boundary inclusion,
    /// and freshness remain venue observations.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request parameters, transport or venue failures, malformed
    /// responses, negative volume, or inconsistent timestamps.
    pub async fn get_spot_volume(
        &self,
        request: &DeepXSpotVolumeRequest,
    ) -> Result<DeepXSpotVolume> {
        self.get_sdk_read_with_retry(|index| {
            self.client.get_spot_volume(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns the validated raw Spot last price without observation-time semantics.
    ///
    /// The scalar response does not echo its market selector or observation timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request parameters, transport or venue failures, malformed
    /// responses, or a negative price.
    pub async fn get_spot_last_price(
        &self,
        request: &DeepXSpotLastPriceRequest,
    ) -> Result<DeepXSpotLastPrice> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_last_price(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns one validated exact Spot order-book snapshot.
    ///
    /// The optional positive tick is server-side price aggregation. Aggregated bid and ask buckets
    /// can overlap, and server notionals can be rounded independently, so neither uncrossed-book
    /// nor exact `price * quantity` invariants are invented here. This one-shot snapshot is
    /// mutable and does not imply continuity with the framework Spot book subscription.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request parameters, transport or venue failures, malformed
    /// responses, identity mismatches, invalid levels, duplicates, or side ordering violations.
    pub async fn get_spot_order_book(
        &self,
        request: &DeepXSpotOrderBookRequest,
    ) -> Result<DeepXSpotOrderBook> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_order_book(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns one ascending page of raw perpetual candles.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request parameters, transport or HTTP failures, malformed
    /// responses, or a venue-level failure envelope.
    pub async fn get_perp_candles(
        &self,
        request: &DeepXPerpCandlesRequest,
    ) -> Result<DeepXPerpCandlesPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_candles(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns a complete bounded perpetual candle history using ascending time cursors.
    ///
    /// An explicit request limit caps the total result. Without one, the collector reads at most
    /// `max_pages` pages of 5000 candles. No partial result is returned on budget exhaustion.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero page budget, invalid pages, market identity changes, cursor
    /// overflow, or a history exceeding the page budget.
    pub async fn get_perp_candles_history_limited(
        &self,
        request: &DeepXPerpCandlesRequest,
        max_pages: usize,
    ) -> Result<DeepXPerpCandlesPage> {
        request.validate()?;
        let client = self;
        let request = request.clone();
        collect_perp_candle_history(
            "perp candle history",
            request.start_ms,
            request.end_ms,
            request.limit,
            max_pages,
            move |start_ms, limit| {
                let mut page_request = request.clone();
                page_request.start_ms = start_ms;
                page_request.limit = Some(limit);
                async move { client.get_perp_candles(&page_request).await }
            },
        )
        .await
    }

    /// Returns one ascending page of raw perpetual mark-price history.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request parameters, transport or HTTP failures, malformed
    /// responses, or a venue-level failure envelope.
    pub async fn get_perp_mark_prices(
        &self,
        request: &DeepXPerpMarkPriceRequest,
    ) -> Result<DeepXPerpCandlesPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_mark_prices(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns a complete bounded perpetual mark-price history using ascending time cursors.
    ///
    /// An explicit request limit caps the total result. Without one, the collector reads at most
    /// `max_pages` pages of 5000 observations and never returns partial history.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero page budget, invalid pages, market identity changes, cursor
    /// overflow, or a history exceeding the page budget.
    pub async fn get_perp_mark_prices_history_limited(
        &self,
        request: &DeepXPerpMarkPriceRequest,
        max_pages: usize,
    ) -> Result<DeepXPerpCandlesPage> {
        request.validate()?;
        let client = self;
        let request = request.clone();
        collect_perp_candle_history(
            "perp mark-price history",
            request.start_ms,
            request.end_ms,
            request.limit,
            max_pages,
            move |start_ms, limit| {
                let mut page_request = request.clone();
                page_request.start_ms = start_ms;
                page_request.limit = Some(limit);
                async move { client.get_perp_mark_prices(&page_request).await }
            },
        )
        .await
    }

    /// Returns one ascending page of raw perpetual oracle-price history.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request parameters, transport or HTTP failures, malformed
    /// responses, or a venue-level failure envelope.
    pub async fn get_perp_oracle_prices(
        &self,
        request: &DeepXPerpOraclePriceRequest,
    ) -> Result<DeepXPerpCandlesPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_oracle_prices(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns a complete bounded perpetual oracle-price history using ascending time cursors.
    ///
    /// An explicit request limit caps the total result. Without one, the collector reads at most
    /// `max_pages` pages of 5000 observations and never returns partial history.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero page budget, invalid pages, market identity changes, cursor
    /// overflow, or a history exceeding the page budget.
    pub async fn get_perp_oracle_prices_history_limited(
        &self,
        request: &DeepXPerpOraclePriceRequest,
        max_pages: usize,
    ) -> Result<DeepXPerpCandlesPage> {
        request.validate()?;
        let client = self;
        let request = request.clone();
        collect_perp_candle_history(
            "perp oracle-price history",
            request.start_ms,
            request.end_ms,
            request.limit,
            max_pages,
            move |start_ms, limit| {
                let mut page_request = request.clone();
                page_request.start_ms = start_ms;
                page_request.limit = Some(limit);
                async move { client.get_perp_oracle_prices(&page_request).await }
            },
        )
        .await
    }

    /// Returns one perpetual volume-statistics window.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid market ID, transport or HTTP failures, malformed responses,
    /// or a venue-level failure envelope.
    pub async fn get_perp_volume(
        &self,
        request: &DeepXPerpVolumeRequest,
    ) -> Result<DeepXPerpVolume> {
        self.get_sdk_read_with_retry(|index| {
            self.client.get_perp_volume(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns the raw perpetual last price without assigning observation-time semantics.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid market ID, transport or HTTP failures, malformed responses,
    /// or a venue-level failure envelope.
    pub async fn get_perp_last_price(
        &self,
        request: &DeepXPerpLastPriceRequest,
    ) -> Result<DeepXPerpLastPrice> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_last_price(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns one exact, potentially price-aggregated perpetual order-book snapshot.
    ///
    /// Server notionals and price observations are retained without recomputation or tick
    /// quantization. This mutable REST view is not treated as full exchange depth or connected to
    /// the framework order-book pipeline.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request parameters, transport or venue failures, malformed
    /// responses, identity mismatches, invalid levels, duplicates, or side ordering violations.
    pub async fn get_perp_order_book(
        &self,
        request: &DeepXPerpOrderBookRequest,
    ) -> Result<DeepXPerpOrderBook> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_order_book(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns the uninterpreted payload of a perpetual order-by-ID lookup.
    ///
    /// The OpenAPI defines only the response envelope, so this does not produce an order report
    /// or assign status, quantity, timestamp, or finality semantics to the payload.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid subaccount address, zero market ID, empty order ID,
    /// transport or HTTP failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_perp_order_by_id_raw(
        &self,
        user: &str,
        market_id: u64,
        order_id: &str,
    ) -> Result<Box<serde_json::value::RawValue>> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_order_by_id_raw(&self.base_urls[index], user, market_id, order_id)
        })
        .await
    }

    /// Returns one validated typed perpetual order for an exact subaccount, market, and order ID.
    ///
    /// Venue enum strings remain uninterpreted and this method does not construct a framework
    /// report. Use the raw reader when forward-compatible unknown fields must be retained.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for a non-decimal requested order ID, malformed
    /// record fields, ownership, market or order identity mismatches, invalid financial values, or
    /// timestamps.
    pub async fn get_perp_order_by_id(
        &self,
        user: &str,
        market_id: u64,
        order_id: &str,
    ) -> Result<DeepXPerpOrderRecord> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_order_by_id(&self.base_urls[index], user, market_id, order_id)
        })
        .await
        .map(Into::into)
    }

    /// Returns one validated typed perpetual order associated with an exact transaction hash.
    ///
    /// This mutable REST association is not proof of canonical inclusion or finality and does not
    /// construct a framework order report.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid transaction hash, transport or venue failures, malformed
    /// record fields, or a response that does not identify the requested extrinsic hash.
    pub async fn get_perp_order_by_tx(&self, tx_hash: &str) -> Result<DeepXPerpOrderRecord> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_order_by_tx(&self.base_urls[index], tx_hash)
        })
        .await
        .map(Into::into)
    }

    /// Returns the uninterpreted payload of one exact Spot order-by-ID lookup.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request fields, transport or HTTP failures, malformed
    /// responses, or a venue-level failure envelope.
    pub async fn get_spot_order_by_id_raw(
        &self,
        request: &DeepXSpotOrderByIdRequest,
    ) -> Result<Box<serde_json::value::RawValue>> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_order_by_id_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns one validated typed Spot order for an exact subaccount, market, side, and order ID.
    ///
    /// Venue lifecycle strings remain uninterpreted and this mutable REST observation does not
    /// construct a framework order report.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed record fields, ownership, market,
    /// side or order identity mismatches, invalid financial values, hashes, or timestamps.
    pub async fn get_spot_order_by_id(
        &self,
        request: &DeepXSpotOrderByIdRequest,
    ) -> Result<DeepXSpotOrderRecord> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_order_by_id(&self.base_urls[index], request)
        })
        .await
        .map(Into::into)
    }

    /// Returns one validated typed Spot order associated with an exact transaction hash.
    ///
    /// This mutable association is not proof of canonical inclusion or finality and does not
    /// construct a framework order report.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid transaction hash, transport or venue failures, malformed
    /// record fields, or a response that does not identify the requested extrinsic hash.
    pub async fn get_spot_order_by_tx(&self, tx_hash: &str) -> Result<DeepXSpotOrderRecord> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_order_by_tx(&self.base_urls[index], tx_hash)
        })
        .await
        .map(Into::into)
    }

    /// Returns one page of uninterpreted open perpetual orders for a subaccount.
    ///
    /// The OpenAPI exposes only a generic order payload schema. Individual items retain their
    /// exact JSON representation and are not interpreted as Nautilus order status reports.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, inconsistent cursor metadata, transport or HTTP
    /// failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_perp_open_orders_raw(
        &self,
        request: &DeepXPerpOpenOrdersRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_open_orders_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all uninterpreted active perpetual-order pages within an explicit page budget.
    ///
    /// Page boundaries and raw item payloads are preserved. Reaching a terminal page succeeds;
    /// exhausting the budget while the venue advertises more data fails without returning a
    /// partial result.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid request or page budget, transport or venue failures,
    /// malformed pages, empty continuation pages, missing or repeated cursors, or budget
    /// exhaustion.
    pub async fn get_perp_open_order_pages_raw(
        &self,
        request: &DeepXPerpOpenOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_perp_open_order_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_open_orders_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated typed page of active perpetual orders for a subaccount and market.
    ///
    /// Venue enum strings remain uninterpreted and this method does not construct framework
    /// reports. Use the raw reader when forward-compatible unknown fields must be retained.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed record fields, ownership, market or
    /// side mismatches, terminal or inconsistent order state, invalid financial values or
    /// timestamps, ordering violations, duplicates, or an oversized page.
    pub async fn get_perp_open_orders(
        &self,
        request: &DeepXPerpOpenOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpOrderRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_open_orders(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated typed active-order pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector errors or a typed record validation error. No partial typed
    /// result is returned when any page or record is invalid, and invalid pages stop pagination.
    pub async fn get_perp_open_order_pages(
        &self,
        request: &DeepXPerpOpenOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpOrderRecord>>> {
        let pages = collect_perp_open_order_pages(request, max_pages, |page_request| async move {
            self.get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_open_orders(&self.base_urls[index], &page_request)
            })
            .await
        })
        .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns one page of uninterpreted historical perpetual orders for a subaccount.
    ///
    /// The OpenAPI exposes only a generic order payload schema. Individual items retain their
    /// exact JSON representation and are not interpreted as Nautilus order status reports.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, inconsistent cursor metadata, transport or HTTP
    /// failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_perp_history_orders_raw(
        &self,
        request: &DeepXPerpHistoryOrdersRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_history_orders_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all historical perpetual order pages within an explicit page budget.
    ///
    /// Page boundaries and raw item payloads are preserved. Reaching a terminal page succeeds;
    /// exhausting the budget while the venue advertises more data fails without returning a
    /// partial result.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid request or page budget, transport or venue failures,
    /// malformed pages, empty continuation pages, missing or repeated cursors, or budget
    /// exhaustion.
    pub async fn get_perp_history_order_pages_raw(
        &self,
        request: &DeepXPerpHistoryOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_perp_history_order_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_history_orders_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated typed page of historical perpetual orders for a subaccount.
    ///
    /// Venue enum strings remain uninterpreted and this method does not construct framework
    /// reports. Use the raw reader when forward-compatible unknown fields must be retained.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed record fields, ownership or market
    /// mismatches, invalid financial values, timestamps, or an oversized page.
    pub async fn get_perp_history_orders(
        &self,
        request: &DeepXPerpHistoryOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpOrderRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_history_orders(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated typed historical-order pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector errors or a typed record validation error. No partial typed
    /// result is returned when any page or record is invalid.
    pub async fn get_perp_history_order_pages(
        &self,
        request: &DeepXPerpHistoryOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpOrderRecord>>> {
        let pages =
            collect_perp_history_order_pages(request, max_pages, |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_perp_history_orders(&self.base_urls[index], &page_request)
                })
                .await
            })
            .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns one raw page of orders associated with a position lifecycle.
    ///
    /// The endpoint includes orders across statuses and both directions. Filled orders may
    /// appear in every lifecycle affected by their fills, while placement-only orders remain
    /// provisional until filled.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request fields, transport or HTTP failures, malformed page
    /// metadata, or a venue-level failure envelope.
    pub async fn get_perp_position_orders_raw(
        &self,
        request: &DeepXPerpPositionOrdersRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_position_orders_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all raw position-order pages under an explicit page budget.
    ///
    /// Cursor expiration or budget exhaustion returns an error rather than partial coverage.
    ///
    /// # Errors
    ///
    /// Returns the single-page errors or an error for invalid page budgets, empty continuation
    /// pages, missing or repeated cursors, or budget exhaustion.
    pub async fn get_perp_position_order_pages_raw(
        &self,
        request: &DeepXPerpPositionOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_perp_position_order_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_position_orders_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated typed page of orders associated with a position lifecycle.
    ///
    /// Venue enum values remain uninterpreted and no framework reports are constructed.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed fields, ownership mismatches,
    /// invalid financial values, timestamps, or duplicate order IDs.
    pub async fn get_perp_position_orders(
        &self,
        request: &DeepXPerpPositionOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpOrderRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_position_orders(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated typed position-order pages under an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector errors or a typed record validation error. No partial typed
    /// result is returned when any page or record is invalid.
    pub async fn get_perp_position_order_pages(
        &self,
        request: &DeepXPerpPositionOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpOrderRecord>>> {
        let pages =
            collect_perp_position_order_pages(request, max_pages, |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_perp_position_orders(&self.base_urls[index], &page_request)
                })
                .await
            })
            .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns one page of uninterpreted active Spot orders for a subaccount and market.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, inconsistent cursor metadata, transport or HTTP
    /// failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_spot_open_orders_raw(
        &self,
        request: &DeepXSpotOpenOrdersRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_open_orders_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns one validated typed page of active Spot orders for a subaccount and market.
    ///
    /// Venue enum strings remain uninterpreted and this method does not construct framework
    /// reports. Use the raw reader when forward-compatible unknown fields must be retained.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed record fields, ownership or market
    /// mismatches, terminal or inconsistent order state, invalid financial values, timestamps,
    /// duplicates, ordering, or an oversized page.
    pub async fn get_spot_open_orders(
        &self,
        request: &DeepXSpotOpenOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXSpotOrderRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_spot_open_orders(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated active Spot-order pages within an explicit page budget.
    ///
    /// Cursor cycles, duplicate order IDs, ordering violations, and budget exhaustion return an
    /// error without exposing a partial collection.
    ///
    /// # Errors
    ///
    /// Returns the single-page reader errors or an error for an invalid page budget, empty
    /// continuation page, missing or repeated cursor, duplicate order, ordering violation, or
    /// exhausted page budget.
    pub async fn get_spot_open_order_pages(
        &self,
        request: &DeepXSpotOpenOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXSpotOrderRecord>>> {
        let pages = collect_spot_open_order_pages(request, max_pages, |page_request| async move {
            self.get_sdk_read_with_retry(|index| {
                self.client
                    .get_spot_open_orders(&self.base_urls[index], &page_request)
            })
            .await
        })
        .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns one page of uninterpreted historical Spot orders for a subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, inconsistent cursor metadata, transport or HTTP
    /// failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_spot_history_orders_raw(
        &self,
        request: &DeepXSpotHistoryOrdersRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_history_orders_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all raw historical Spot order pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid request or page budget, transport or venue failures,
    /// malformed pages, empty continuation pages, missing or repeated cursors, or budget
    /// exhaustion. No partial result is returned.
    pub async fn get_spot_history_order_pages_raw(
        &self,
        request: &DeepXSpotHistoryOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_spot_history_order_pages_raw(request, max_pages, |page_request| async move {
            self.get_spot_history_orders_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated typed page of historical Spot orders for a subaccount.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed record fields, ownership or market
    /// mismatches, invalid financial values, timestamps, duplicates, ordering, or an oversized
    /// page.
    pub async fn get_spot_history_orders(
        &self,
        request: &DeepXSpotHistoryOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXSpotOrderRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_spot_history_orders(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated typed historical Spot order pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector errors or a typed record validation error. No partial typed
    /// result is returned when any page or record is invalid.
    pub async fn get_spot_history_order_pages(
        &self,
        request: &DeepXSpotHistoryOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXSpotOrderRecord>>> {
        let pages =
            collect_spot_history_order_pages(request, max_pages, |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_spot_history_orders(&self.base_urls[index], &page_request)
                })
                .await
            })
            .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns one page of uninterpreted Spot trades selected for a subaccount.
    ///
    /// The response does not echo the requested subaccount, so ownership cannot be independently
    /// rebound from an individual record.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, inconsistent cursor metadata, transport or HTTP
    /// failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_spot_account_trades_raw(
        &self,
        request: &DeepXSpotAccountTradesRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_account_trades_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all raw Spot account-trade pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid request or page budget, transport or venue failures,
    /// malformed pages, empty continuation pages, missing or repeated cursors, or budget
    /// exhaustion. No partial result is returned.
    pub async fn get_spot_account_trade_pages_raw(
        &self,
        request: &DeepXSpotAccountTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_spot_account_trade_pages_raw(request, max_pages, |page_request| async move {
            self.get_spot_account_trades_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated typed page of Spot trades selected for a subaccount.
    ///
    /// Taker, fee ownership, and framework execution semantics remain uninterpreted. The venue
    /// does not echo the requested subaccount on each record.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed record fields, market or filter
    /// mismatches, invalid financial values, timestamps, duplicates, ordering, or an oversized
    /// page.
    pub async fn get_spot_account_trades(
        &self,
        request: &DeepXSpotAccountTradesRequest,
    ) -> Result<DeepXAccountPage<DeepXSpotAccountTradeRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_spot_account_trades(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated typed Spot account-trade pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector errors or a typed record validation error. No partial typed
    /// result is returned when any page or record is invalid.
    pub async fn get_spot_account_trade_pages(
        &self,
        request: &DeepXSpotAccountTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXSpotAccountTradeRecord>>> {
        let pages =
            collect_spot_account_trade_pages(request, max_pages, |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_spot_account_trades(&self.base_urls[index], &page_request)
                })
                .await
            })
            .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns one validated globally paginated Spot order page grouped by market and subaccount.
    ///
    /// The response does not echo the requested wallet and is not joined to the separately mutable
    /// ownership directory. Empty terminal groups are preserved. Nonempty groups must agree on
    /// one global cursor, and each order maker must match its enclosing subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, transport or venue failures, malformed groups,
    /// inconsistent pagination metadata, scope mismatches, invalid records, duplicates, ordering
    /// violations, or an oversized page.
    pub async fn get_spot_wallet_orders(
        &self,
        request: &DeepXSpotWalletOrdersRequest,
    ) -> Result<DeepXSpotWalletOrdersPage> {
        self.get_spot_wallet_orders_page(request).await
    }

    /// Returns all validated Spot wallet-order pages within an explicit page budget.
    ///
    /// Cursor disagreement, duplicate composite identities, ordering violations, or budget
    /// exhaustion returns no partial result. The mutable endpoint is not a block-pinned snapshot.
    ///
    /// # Errors
    ///
    /// Returns the single-page errors or an error for an invalid page budget, empty continuation,
    /// repeated cursor, or budget exhaustion.
    pub async fn get_spot_wallet_order_pages(
        &self,
        request: &DeepXSpotWalletOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXSpotWalletOrdersPage>> {
        let pages =
            collect_spot_wallet_order_pages(request, max_pages, |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_spot_wallet_orders(&self.base_urls[index], &page_request)
                })
                .await
            })
            .await?;
        Ok(pages.into_iter().map(Into::into).collect())
    }

    async fn get_spot_wallet_orders_page(
        &self,
        request: &DeepXSpotWalletOrdersRequest,
    ) -> Result<DeepXSpotWalletOrdersPage> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_spot_wallet_orders(&self.base_urls[index], request)
            })
            .await?;
        Ok(page.into())
    }

    /// Returns one validated globally paginated Spot trade page grouped by market and subaccount.
    ///
    /// The venue does not echo wallet or subaccount ownership on trade records, so this method
    /// preserves the grouping without claiming an independently rebound owner. Taker, side, and
    /// fee semantics remain uninterpreted, and no framework fill report is constructed.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, transport or venue failures, malformed groups,
    /// inconsistent pagination metadata, scope mismatches, invalid records, duplicates, ordering
    /// violations, or an oversized page.
    pub async fn get_spot_wallet_trades(
        &self,
        request: &DeepXSpotWalletTradesRequest,
    ) -> Result<DeepXSpotWalletTradesPage> {
        self.get_spot_wallet_trades_page(request).await
    }

    /// Returns all validated Spot wallet-trade pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the single-page errors or an error for an invalid page budget, empty continuation,
    /// repeated cursor, or budget exhaustion. No partial result is returned.
    pub async fn get_spot_wallet_trade_pages(
        &self,
        request: &DeepXSpotWalletTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXSpotWalletTradesPage>> {
        let pages =
            collect_spot_wallet_trade_pages(request, max_pages, |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_spot_wallet_trades(&self.base_urls[index], &page_request)
                })
                .await
            })
            .await?;
        Ok(pages.into_iter().map(Into::into).collect())
    }

    async fn get_spot_wallet_trades_page(
        &self,
        request: &DeepXSpotWalletTradesRequest,
    ) -> Result<DeepXSpotWalletTradesPage> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_spot_wallet_trades(&self.base_urls[index], request)
            })
            .await?;
        Ok(page.into())
    }

    /// Returns one page of uninterpreted perpetual trades for a subaccount.
    ///
    /// The OpenAPI exposes only a generic trade payload schema. Individual items retain their
    /// exact JSON representation and are not interpreted as Nautilus fill reports.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, inconsistent cursor metadata, transport or HTTP
    /// failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_perp_account_trades_raw(
        &self,
        request: &DeepXPerpAccountTradesRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_account_trades_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all perpetual account trade pages within an explicit page budget.
    ///
    /// Page boundaries and raw item payloads are preserved. Reaching a terminal page succeeds;
    /// exhausting the budget while the venue advertises more data fails without returning a
    /// partial result.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid request or page budget, transport or venue failures,
    /// malformed pages, empty continuation pages, missing or repeated cursors, or budget
    /// exhaustion.
    pub async fn get_perp_account_trade_pages_raw(
        &self,
        request: &DeepXPerpAccountTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_perp_account_trade_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_account_trades_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated typed perpetual account-trade page for a subaccount.
    ///
    /// Taker and fill-direction strings remain uninterpreted and no framework fill report is
    /// constructed.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed record fields, market or filter
    /// mismatches, invalid financial values, timestamps, or an oversized page.
    pub async fn get_perp_account_trades(
        &self,
        request: &DeepXPerpAccountTradesRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpAccountTradeRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_account_trades(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated typed account-trade pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector errors or a typed record validation error. No partial typed
    /// result is returned when any page or record is invalid.
    pub async fn get_perp_account_trade_pages(
        &self,
        request: &DeepXPerpAccountTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpAccountTradeRecord>>> {
        let pages =
            collect_perp_account_trade_pages(request, max_pages, |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_perp_account_trades(&self.base_urls[index], &page_request)
                })
                .await
            })
            .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns one validated globally paginated perpetual order page for a wallet.
    ///
    /// The venue repeats one global cursor in every nested market/subaccount group. This reader
    /// requires all nested pagination metadata to agree before exposing the normalized page.
    /// Returned order owners must match their enclosing subaccount, but the response does not echo
    /// the wallet and is not joined to the separately mutable ownership directory.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, transport or venue failures, inconsistent nested
    /// pagination metadata, oversized pages, malformed groups, scope mismatches, invalid order
    /// records, duplicates, or ordering violations.
    pub async fn get_perp_wallet_orders(
        &self,
        request: &DeepXPerpWalletOrdersRequest,
    ) -> Result<DeepXPerpWalletOrdersPage> {
        self.get_perp_wallet_orders_page(request).await
    }

    /// Returns all validated wallet-order pages within an explicit page budget.
    ///
    /// Every page retains its market/subaccount grouping. Cursor disagreement, duplicate composite
    /// identities, per-group ordering violations, or budget exhaustion returns no partial result.
    /// The mutable endpoint is not treated as a block-pinned snapshot.
    ///
    /// # Errors
    ///
    /// Returns the single-page errors or an error for an invalid page budget, empty continuation,
    /// repeated cursor, or budget exhaustion.
    pub async fn get_perp_wallet_order_pages(
        &self,
        request: &DeepXPerpWalletOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXPerpWalletOrdersPage>> {
        let pages =
            collect_perp_wallet_order_pages(request, max_pages, |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_perp_wallet_orders(&self.base_urls[index], &page_request)
                })
                .await
            })
            .await?;
        Ok(pages.into_iter().map(Into::into).collect())
    }

    async fn get_perp_wallet_orders_page(
        &self,
        request: &DeepXPerpWalletOrdersRequest,
    ) -> Result<DeepXPerpWalletOrdersPage> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_wallet_orders(&self.base_urls[index], request)
            })
            .await?;
        Ok(page.into())
    }

    /// Returns one validated grouped perpetual trade snapshot for a wallet.
    ///
    /// Each subaccount group retains its independent cursor metadata. The endpoint can return
    /// different continuation cursors for different groups, so this method deliberately does not
    /// infer one aggregate next page or claim a complete wallet history. The response does not
    /// echo the wallet identity, and no separately mutable ownership directory is joined.
    /// Zero-sized historical records are preserved because they occur in captured venue data;
    /// no framework fill report is constructed.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, transport or venue failures, malformed groups,
    /// market-scope mismatches, invalid identities or financial values, out-of-range or unordered
    /// timestamps, duplicate trade IDs, oversized nested pages, or invalid cursor metadata.
    pub async fn get_perp_wallet_trades(
        &self,
        request: &DeepXPerpWalletTradesRequest,
    ) -> Result<Vec<DeepXPerpWalletTradeMarket>> {
        Ok(self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_wallet_trades(&self.base_urls[index], request)
            })
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Returns bounded trade pages for one subaccount in a wallet-grouped perpetual market.
    ///
    /// The venue supplies an independent cursor for each subaccount group. A continuation request
    /// can repeat other groups, so only the selected group's cursor and records advance this
    /// collector. The response does not prove wallet ownership or a stable historical snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid request, missing selected group, malformed or repeated
    /// trades, a market name differing from `expected_market_name`, time-order violations,
    /// cursor failures, or page-budget exhaustion. No partial pages are returned.
    pub async fn get_perp_wallet_trade_pages_for_subaccount(
        &self,
        request: &DeepXPerpWalletTradesRequest,
        subaccount: &str,
        expected_market_name: &str,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpAccountTradeRecord>>> {
        let pages = collect_perp_wallet_trade_pages_for_subaccount(
            request,
            subaccount,
            expected_market_name,
            max_pages,
            |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_perp_wallet_trades(&self.base_urls[index], &page_request)
                })
                .await
            },
        )
        .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns one page of uninterpreted perpetual funding fees for a subaccount.
    ///
    /// Individual items retain their exact JSON representation and are not interpreted as
    /// account-state, profit-and-loss, or funding-payment events.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, inconsistent cursor metadata, transport or HTTP
    /// failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_perp_funding_fees_raw(
        &self,
        request: &DeepXPerpFundingFeeRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_funding_fees_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all perpetual funding-fee pages within an explicit page budget.
    ///
    /// Page boundaries and raw item payloads are preserved. Reaching a terminal page succeeds;
    /// exhausting the budget while the venue advertises more data fails without returning a
    /// partial result.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid request or page budget, transport or venue failures,
    /// malformed pages, empty continuation pages, missing or repeated cursors, or budget
    /// exhaustion.
    pub async fn get_perp_funding_fee_pages_raw(
        &self,
        request: &DeepXPerpFundingFeeRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_perp_funding_fee_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_funding_fees_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated typed perpetual funding-fee page for a subaccount.
    ///
    /// Signed fee/rate values and settlement state remain venue observations. This method does not
    /// infer payment currency, settlement cadence, or framework account semantics.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed record fields, ownership, market or
    /// time-range mismatches, invalid financial values, timestamps, duplicate identities, or an
    /// oversized page.
    pub async fn get_perp_funding_fees(
        &self,
        request: &DeepXPerpFundingFeeRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpFundingFeeRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_funding_fees(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated typed funding-fee pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector errors or a typed record validation error. No partial typed
    /// result is returned when any page or record is invalid.
    pub async fn get_perp_funding_fee_pages(
        &self,
        request: &DeepXPerpFundingFeeRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpFundingFeeRecord>>> {
        let pages = collect_perp_funding_fee_pages(request, max_pages, |page_request| async move {
            self.get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_funding_fees(&self.base_urls[index], &page_request)
            })
            .await
        })
        .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns one raw globally ordered page of perpetual funding fees for a wallet.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, inconsistent cursor metadata, transport or HTTP
    /// failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_wallet_funding_fees_raw(
        &self,
        request: &DeepXWalletFundingFeeRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_wallet_funding_fees_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all raw wallet funding-fee pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the single-page errors or an error for an invalid page budget, empty continuation,
    /// repeated cursor, or budget exhaustion. No partial collection is returned.
    pub async fn get_wallet_funding_fee_pages_raw(
        &self,
        request: &DeepXWalletFundingFeeRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_wallet_funding_fee_pages_raw(request, max_pages, |page_request| async move {
            self.get_wallet_funding_fees_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated typed globally ordered funding-fee page for a wallet.
    ///
    /// Record owners are validated as AccountId20 values but are not independently resolved
    /// against the mutable wallet directory. Signed values and settlement state remain venue
    /// observations and no framework event is constructed.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed fields, market or time-range
    /// mismatches, invalid values, duplicate event identities, or an oversized page.
    pub async fn get_wallet_funding_fees(
        &self,
        request: &DeepXWalletFundingFeeRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpFundingFeeRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_wallet_funding_fees(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated wallet funding-fee pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector or typed-record errors. Duplicate event identities across pages
    /// fail without returning a partial typed collection.
    pub async fn get_wallet_funding_fee_pages(
        &self,
        request: &DeepXWalletFundingFeeRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpFundingFeeRecord>>> {
        let pages =
            collect_wallet_funding_fee_pages(request, max_pages, |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_wallet_funding_fees(&self.base_urls[index], &page_request)
                })
                .await
            })
            .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns one validated page of complete hourly unsettled-funding boundaries.
    ///
    /// All amounts remain exact on-chain integer units. This method does not infer token precision,
    /// current account state, settlement, or framework PnL events.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid account scope, bounds or cursor fields, transport or venue
    /// failures, malformed records, scope mismatches, invalid arithmetic, duplicates, ordering
    /// violations, or an oversized page.
    pub async fn get_hourly_unsettled_funding(
        &self,
        request: &DeepXHourlyUnsettledFundingRequest,
    ) -> Result<Vec<DeepXHourlyUnsettledFundingRecord>> {
        let records = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_hourly_unsettled_funding(&self.base_urls[index], request)
            })
            .await?;
        Ok(records.into_iter().map(Into::into).collect())
    }

    /// Returns all hourly unsettled-funding pages within an explicit page budget.
    ///
    /// A full page advances with the exact four-field keyset boundary from its final record. A
    /// short or empty page is terminal. Budget exhaustion, repeated cursors or duplicate event IDs
    /// fail without returning a partial collection.
    ///
    /// # Errors
    ///
    /// Returns the single-page errors, an invalid page-budget error, or a pagination progress or
    /// budget error.
    pub async fn get_hourly_unsettled_funding_pages(
        &self,
        request: &DeepXHourlyUnsettledFundingRequest,
        max_pages: usize,
    ) -> Result<Vec<Vec<DeepXHourlyUnsettledFundingRecord>>> {
        let pages =
            collect_hourly_unsettled_funding_pages(request, max_pages, |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_hourly_unsettled_funding(&self.base_urls[index], &page_request)
                })
                .await
            })
            .await?;
        Ok(pages
            .into_iter()
            .map(|page| page.into_iter().map(Into::into).collect())
            .collect())
    }

    /// Returns one page of uninterpreted perpetual position lifecycles for a subaccount.
    ///
    /// The request explicitly selects subaccount scope. Individual items retain their exact JSON
    /// representation and are not interpreted as Nautilus position status reports.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, inconsistent cursor metadata, transport or HTTP
    /// failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_perp_positions_raw(
        &self,
        request: &DeepXPerpPositionsRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_perp_positions_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all perpetual position lifecycle pages within an explicit page budget.
    ///
    /// Page boundaries and raw item payloads are preserved. Reaching a terminal page succeeds;
    /// exhausting the budget while the venue advertises more data fails without returning a
    /// partial result.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid request or page budget, transport or venue failures,
    /// malformed pages, empty continuation pages, missing or repeated cursors, or budget
    /// exhaustion.
    pub async fn get_perp_position_pages_raw(
        &self,
        request: &DeepXPerpPositionsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_perp_position_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_positions_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated typed perpetual position-lifecycle page for a subaccount.
    ///
    /// Lifecycle status strings remain uninterpreted and no framework position report is
    /// constructed.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed record fields, ownership, market or
    /// filter mismatches, invalid financial values, timestamps, or an oversized page.
    pub async fn get_perp_positions(
        &self,
        request: &DeepXPerpPositionsRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpPositionRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_positions(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated typed position-lifecycle pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector errors or a typed record validation error. No partial typed
    /// result is returned when any page or record is invalid.
    pub async fn get_perp_position_pages(
        &self,
        request: &DeepXPerpPositionsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpPositionRecord>>> {
        let pages = collect_perp_position_pages(request, max_pages, |page_request| async move {
            self.get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_positions(&self.base_urls[index], &page_request)
            })
            .await
        })
        .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns the uninterpreted lending deposit and borrow balance payload for a subaccount.
    ///
    /// The OpenAPI defines only a generic payload schema. This read preserves exact JSON
    /// lexemes and does not construct Nautilus account balances or prove account initialization.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid subaccount address, transport or HTTP failures,
    /// malformed responses, or a venue-level failure envelope.
    pub async fn get_subaccount_balances_raw(
        &self,
        subaccount: &str,
    ) -> Result<Box<serde_json::value::RawValue>> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_subaccount_balances_raw(&self.base_urls[index], subaccount)
        })
        .await
    }

    /// Returns one raw page of balance changes for one subaccount or wallet.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid scope, bounds, filters, cursor metadata, transport or HTTP
    /// failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_balance_changes_raw(
        &self,
        request: &DeepXBalanceChangesRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_balance_changes_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all raw balance-change pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the single-page errors or an error for an invalid page budget, empty continuation,
    /// repeated cursor, or budget exhaustion. No partial collection is returned.
    pub async fn get_balance_change_pages_raw(
        &self,
        request: &DeepXBalanceChangesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_balance_change_pages_raw(request, max_pages, |page_request| async move {
            self.get_balance_changes_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated typed page of exact balance changes.
    ///
    /// Signed changes retain their venue sign and cross-chain extensions remain raw. This reader
    /// does not construct Nautilus account balances or infer free/locked amounts.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed, duplicate, out-of-scope, or
    /// out-of-bounds records.
    pub async fn get_balance_changes(
        &self,
        request: &DeepXBalanceChangesRequest,
    ) -> Result<DeepXAccountPage<DeepXBalanceChangeRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_balance_changes(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated typed balance-change pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector or typed-record errors. Duplicate identities across pages fail
    /// without returning a partial typed collection. Invalid pages stop pagination immediately.
    pub async fn get_balance_change_pages(
        &self,
        request: &DeepXBalanceChangesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXBalanceChangeRecord>>> {
        let pages = collect_balance_change_pages(request, max_pages, |page_request| async move {
            self.get_sdk_read_with_retry(|index| {
                self.client
                    .get_balance_changes(&self.base_urls[index], &page_request)
            })
            .await
        })
        .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns the uninterpreted response data for spot ADL candidate discovery.
    ///
    /// The OpenAPI specifies only a generic response envelope. Raw amounts and candidate
    /// ranking are therefore left uninterpreted; this read does not establish execution
    /// eligibility or submit an ADL transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request fields, transport or HTTP failures, malformed
    /// responses, or a venue-level failure envelope.
    pub async fn get_spot_adl_candidates_raw(
        &self,
        request: &DeepXSpotAdlCandidatesRequest,
    ) -> Result<Box<serde_json::value::RawValue>> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_adl_candidates_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns validated spot ADL candidate data while preserving raw integer amounts.
    ///
    /// Candidate order and amount units remain venue observations; this method infers no ranking,
    /// financial scale, eligibility, or execution semantics.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request fields, transport or venue failures, malformed data,
    /// identity mismatches, invalid raw integer strings, or duplicate candidate subaccounts.
    pub async fn get_spot_adl_candidates(
        &self,
        request: &DeepXSpotAdlCandidatesRequest,
    ) -> Result<DeepXSpotAdlCandidates> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_spot_adl_candidates(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns one raw page of wallet- or subaccount-scoped ADL records.
    ///
    /// Record payloads remain uninterpreted because OpenAPI provides an example but no stable
    /// item schema. The cursor and page metadata are validated.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, inconsistent cursor metadata, transport or HTTP
    /// failures, malformed responses, or a venue-level failure envelope.
    pub async fn get_liquidation_adl_records_raw(
        &self,
        request: &DeepXLiquidationAdlRecordsRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_liquidation_adl_records_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all raw ADL record pages within an explicit page budget.
    ///
    /// Reaching a terminal page succeeds. Exhausting the budget while the venue advertises more
    /// records fails without returning a partial collection.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid request or page budget, transport or venue failures,
    /// malformed pages, empty continuation pages, repeated cursors, or budget exhaustion.
    pub async fn get_liquidation_adl_record_pages_raw(
        &self,
        request: &DeepXLiquidationAdlRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_liquidation_adl_record_pages_raw(request, max_pages, |page_request| async move {
            self.get_liquidation_adl_records_raw(&page_request).await
        })
        .await
    }

    /// Returns one raw page of ordinary liquidation execution records.
    ///
    /// Item fields remain uninterpreted because OpenAPI provides an example rather than a stable
    /// schema. This mutable read does not prove chain inclusion or finality.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid filters or cursor metadata, transport or HTTP failures,
    /// malformed responses, or a venue-level failure envelope.
    pub async fn get_liquidation_execution_records_raw(
        &self,
        request: &DeepXLiquidationExecutionRecordsRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_liquidation_execution_records_raw_at(
            DeepXLiquidationExecutionRoute::Ordinary,
            request,
        )
        .await
    }

    /// Returns all ordinary liquidation execution pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid filters or page budget, malformed pages, repeated cursors,
    /// transport or venue failures, or history exceeding the budget.
    pub async fn get_liquidation_execution_record_pages_raw(
        &self,
        request: &DeepXLiquidationExecutionRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        self.get_liquidation_execution_record_pages_raw_at(
            DeepXLiquidationExecutionRoute::Ordinary,
            request,
            max_pages,
        )
        .await
    }

    /// Returns one raw page of cranker execution records.
    ///
    /// Record fields remain uninterpreted because OpenAPI documents examples rather than a stable
    /// item schema. The read does not prove chain inclusion or finality.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid filters or cursor metadata, transport or HTTP failures,
    /// malformed responses, or a venue-level failure envelope.
    pub async fn get_liquidation_cranker_records_raw(
        &self,
        request: &DeepXLiquidationExecutionRecordsRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_liquidation_execution_records_raw_at(
            DeepXLiquidationExecutionRoute::Cranker,
            request,
        )
        .await
    }

    /// Returns all raw cranker execution pages within an explicit page budget.
    pub async fn get_liquidation_cranker_record_pages_raw(
        &self,
        request: &DeepXLiquidationExecutionRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        self.get_liquidation_execution_record_pages_raw_at(
            DeepXLiquidationExecutionRoute::Cranker,
            request,
            max_pages,
        )
        .await
    }

    /// Returns one raw page of TP/SL execution records.
    ///
    /// Record fields remain uninterpreted because OpenAPI provides no stable item schema. The
    /// read does not prove chain inclusion or finality.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid filters or cursor metadata, transport or HTTP failures,
    /// malformed responses, or a venue-level failure envelope.
    pub async fn get_liquidation_tp_sl_records_raw(
        &self,
        request: &DeepXLiquidationExecutionRecordsRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_liquidation_execution_records_raw_at(DeepXLiquidationExecutionRoute::TpSl, request)
            .await
    }

    /// Returns all raw TP/SL execution pages within an explicit page budget.
    pub async fn get_liquidation_tp_sl_record_pages_raw(
        &self,
        request: &DeepXLiquidationExecutionRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        self.get_liquidation_execution_record_pages_raw_at(
            DeepXLiquidationExecutionRoute::TpSl,
            request,
            max_pages,
        )
        .await
    }

    async fn get_liquidation_execution_records_raw_at(
        &self,
        route: DeepXLiquidationExecutionRoute,
        request: &DeepXLiquidationExecutionRecordsRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client.get_liquidation_execution_records_raw(
                &self.base_urls[index],
                route,
                request,
            )
        })
        .await
    }

    async fn get_liquidation_execution_record_pages_raw_at(
        &self,
        route: DeepXLiquidationExecutionRoute,
        request: &DeepXLiquidationExecutionRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_liquidation_execution_record_pages_raw(
            route,
            request,
            max_pages,
            |page_request| async move {
                self.get_liquidation_execution_records_raw_at(route, &page_request)
                    .await
            },
        )
        .await
    }

    /// Returns raw durable liquidation keeper state for one subaccount.
    ///
    /// The endpoint has phase-dependent data and no stable OpenAPI item schema, so the returned
    /// JSON remains uninterpreted. Reading this state does not claim a liquidation action or
    /// establish finality for a pending transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid subaccount or chain ID, transport or HTTP failures,
    /// malformed responses, or a venue-level failure envelope.
    pub async fn get_liquidation_keeper_state_raw(
        &self,
        subaccount: &str,
        chain_id: Option<&str>,
    ) -> Result<Box<serde_json::value::RawValue>> {
        self.get_sdk_read_with_retry(|index| {
            self.client.get_liquidation_keeper_state_raw(
                &self.base_urls[index],
                subaccount,
                chain_id,
            )
        })
        .await
    }

    /// Returns one raw page of account liquidation records for one subaccount or wallet.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid scope, filters or cursor metadata, transport or HTTP failures,
    /// malformed responses, or a venue-level failure envelope.
    pub async fn get_liquidation_records_raw(
        &self,
        request: &DeepXLiquidationRecordsRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_liquidation_records_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all raw liquidation-record pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the single-page errors or an error for an invalid page budget, empty continuation,
    /// repeated cursor, or budget exhaustion. No partial collection is returned.
    pub async fn get_liquidation_record_pages_raw(
        &self,
        request: &DeepXLiquidationRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_liquidation_record_pages_raw(request, max_pages, |page_request| async move {
            self.get_liquidation_records_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated page of exact raw-unit account liquidation records.
    ///
    /// Embedded chain details and canceled-order objects are validated JSON but remain
    /// uninterpreted. This reader performs no asset-unit conversion or framework event mapping.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed, duplicate, out-of-scope,
    /// incorrectly ordered, or filter-mismatched records.
    pub async fn get_liquidation_records(
        &self,
        request: &DeepXLiquidationRecordsRequest,
    ) -> Result<DeepXAccountPage<DeepXLiquidationRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_liquidation_records(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated liquidation-record pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector or typed-record errors. Duplicate identities or ordering
    /// violations across page boundaries fail without returning a partial typed collection.
    /// Invalid pages stop pagination immediately.
    pub async fn get_liquidation_record_pages(
        &self,
        request: &DeepXLiquidationRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXLiquidationRecord>>> {
        let pages =
            collect_liquidation_record_pages(request, max_pages, |page_request| async move {
                self.get_sdk_read_with_retry(|index| {
                    self.client
                        .get_liquidation_records(&self.base_urls[index], &page_request)
                })
                .await
            })
            .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns a validated nullable liquidation price for one subaccount and perpetual market.
    ///
    /// This preserves the venue observation without inferring liquidation methodology, freshness,
    /// price units, position state, or framework risk semantics.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request fields, transport or venue failures, malformed
    /// responses, identity mismatches, or a nonpositive present price.
    pub async fn get_perp_liquidation_price(
        &self,
        request: &DeepXPerpLiquidationPriceRequest,
    ) -> Result<DeepXPerpLiquidationPrice> {
        let price = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_perp_liquidation_price(&self.base_urls[index], request)
            })
            .await?;
        Ok(DeepXPerpLiquidationPrice(price))
    }

    /// Returns validated aggregated directory statistics for a wallet.
    ///
    /// The Insurance Fund staked quote amount remains an exact venue value with uninterpreted units
    /// and is not converted into a Nautilus account balance.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid wallet address, transport or venue failures, malformed
    /// responses, invalid or duplicate subaccounts, inconsistent counters, or a negative amount.
    pub async fn get_user_stats(&self, address: &str) -> Result<DeepXUserStats> {
        let stats = self
            .get_sdk_read_with_retry(|index| {
                self.client.get_user_stats(&self.base_urls[index], address)
            })
            .await?;
        Ok(DeepXUserStats(stats))
    }

    /// Returns whether one wallet is bound to an invitation code.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid wallet address, transport or venue failures, malformed
    /// data, or a response for another address.
    pub async fn get_invitation_status(&self, address: &str) -> Result<DeepXInvitationStatus> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_invitation_status(&self.base_urls[index], address)
        })
        .await
    }

    /// Returns the invitation codes associated with one wallet.
    ///
    /// The API documents a string array but does not specify code format. Values are therefore
    /// preserved exactly, while empty and duplicate entries are rejected.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid wallet, transport or HTTP failures, malformed responses,
    /// empty or duplicate codes, or a venue-level failure envelope.
    pub async fn get_invitation_codes(&self, address: &str) -> Result<Vec<String>> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_invitation_codes(&self.base_urls[index], address)
        })
        .await
    }

    /// Returns the validated wallet-level quota and trading-volume aggregate.
    ///
    /// This reader does not establish claim eligibility, history completeness, or mutation
    /// authority and does not submit a quota claim.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid wallet address, transport or venue failures, malformed
    /// responses, identity or timestamp mismatches, negative volumes, or an inconsistent total.
    pub async fn get_quota_summary(&self, wallet: &str) -> Result<DeepXQuotaSummary> {
        let summary = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_quota_summary(&self.base_urls[index], wallet)
            })
            .await?;
        Ok(DeepXQuotaSummary(summary))
    }

    /// Returns the raw backend state for one asynchronous quota claim task.
    ///
    /// OpenAPI documents a generic response envelope and an example, but no stable claim item
    /// schema. Only the returned claim ID is bound to the request; the task state remains
    /// uninterpreted and is not chain-finality evidence.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero claim ID, transport or HTTP failures, malformed responses,
    /// a venue-level failure envelope, or a response for a different claim.
    pub async fn get_quota_claim_raw(
        &self,
        claim_id: u64,
    ) -> Result<Box<serde_json::value::RawValue>> {
        let raw = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_quota_claim(&self.base_urls[index], claim_id)
            })
            .await?;
        Ok(raw.into_raw())
    }

    /// Returns one raw chain-confirmed wallet quota-history page.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid filters, transport or venue failures, malformed pages, or
    /// inconsistent cursor metadata.
    pub async fn get_quota_history_raw(
        &self,
        request: &DeepXQuotaHistoryRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_quota_history_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns one validated typed wallet quota-history page.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for invalid scope, buyer metadata, chain fields,
    /// timestamps, duplicates, ordering, or page size.
    pub async fn get_quota_history(
        &self,
        request: &DeepXQuotaHistoryRequest,
    ) -> Result<DeepXAccountPage<DeepXQuotaHistoryRecord>> {
        let page = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_quota_history(&self.base_urls[index], request)
            })
            .await?;
        Ok(wrap_account_page(page))
    }

    /// Returns all validated wallet quota-history pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the single-page errors or an error for invalid pagination progress or budget
    /// exhaustion. No partial collection is returned.
    pub async fn get_quota_history_pages(
        &self,
        request: &DeepXQuotaHistoryRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXQuotaHistoryRecord>>> {
        let pages = collect_quota_history_pages(request, max_pages, |page_request| async move {
            self.get_sdk_read_with_retry(|index| {
                self.client
                    .get_quota_history(&self.base_urls[index], &page_request)
            })
            .await
        })
        .await?;
        Ok(pages.into_iter().map(wrap_account_page).collect())
    }

    /// Returns one raw page from the public global subaccount directory.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid query fields, transport or venue failures, malformed pages,
    /// or inconsistent cursor metadata.
    pub async fn get_all_subaccounts_raw(
        &self,
        request: &DeepXAllSubaccountsRequest,
    ) -> Result<DeepXRawAccountPage> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_all_subaccounts_raw(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all raw global subaccount-directory pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the single-page errors or an error for an invalid page budget, empty continuation,
    /// repeated cursor, or budget exhaustion. No partial collection is returned.
    pub async fn get_all_subaccount_pages_raw(
        &self,
        request: &DeepXAllSubaccountsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>> {
        collect_all_subaccount_pages_raw(request, max_pages, |page_request| async move {
            self.get_all_subaccounts_raw(&page_request).await
        })
        .await
    }

    /// Returns one validated typed page from the public global subaccount directory.
    ///
    /// The directory is mutable and does not prove current control of either account key. The
    /// observed RFC 3339 creation timestamp is retained without accepting the conflicting integer
    /// representation shown by the OpenAPI example.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for invalid identities, metadata, timestamps,
    /// duplicates, ordering violations, or an oversized page.
    pub async fn get_all_subaccounts(
        &self,
        request: &DeepXAllSubaccountsRequest,
    ) -> Result<DeepXAccountPage<DeepXSubaccountDirectoryRecord>> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_all_subaccounts(&self.base_urls[index], request)
        })
        .await
    }

    /// Returns all validated global subaccount-directory pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector errors or a typed record validation error. Cross-page duplicate
    /// identities and ordering violations fail without returning a partial typed collection.
    pub async fn get_all_subaccount_pages(
        &self,
        request: &DeepXAllSubaccountsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXSubaccountDirectoryRecord>>> {
        collect_all_subaccount_pages(request, max_pages, |page_request| async move {
            self.get_sdk_read_with_retry(|index| {
                self.client
                    .get_all_subaccounts(&self.base_urls[index], &page_request)
            })
            .await
        })
        .await
    }

    /// Returns the validated subaccounts owned by a wallet address.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid wallet address, transport or HTTP failures, malformed
    /// responses, venue-level failures, invalid returned addresses, or duplicate subaccounts.
    pub async fn get_wallet_subaccounts(&self, address: &str) -> Result<DeepXWalletSubaccounts> {
        let directory = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_wallet_subaccounts(&self.base_urls[index], address)
            })
            .await?;
        Ok(DeepXWalletSubaccounts(directory))
    }

    /// Returns validated directory, statistics, and account reads for every wallet subaccount.
    ///
    /// Statistics must report the same subaccount set as the directory. Subaccounts retain
    /// directory order. Each profile must identify the requested wallet as its authority, and
    /// every balance and equity payload must identify the corresponding subaccount.
    /// A final directory read rejects membership changes during collection.
    /// No partial collection is returned if any request or validation fails. The REST responses
    /// are not block-pinned, so this does not assert cross-request venue snapshot atomicity.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid wallet, inconsistent directory or statistics, or any failed
    /// or inconsistent subaccount profile, balance, equity, or margin response.
    pub async fn get_wallet_account_snapshot(
        &self,
        address: &str,
    ) -> Result<DeepXWalletAccountSnapshot> {
        let directory = self.get_wallet_subaccounts(address).await?;
        let stats = self.get_user_stats(address).await?;
        let validation =
            deepx_rust_sdk::rest::DeepXWalletCollectionValidation::for_account_snapshot(&directory);
        validation.validate_stats(&stats)?;
        let mut subaccounts = Vec::with_capacity(directory.addresses.len());
        for subaccount in &directory.addresses {
            let profile = self.get_subaccount_info(subaccount, None).await?;
            validation.validate_profile(&profile)?;
            let balances = self.get_subaccount_balances(subaccount).await?;
            let equity = self.get_subaccount_equity(subaccount).await?;
            let margin_ratio = self.get_subaccount_margin_ratio(subaccount).await?;
            subaccounts.push(DeepXSubaccountSnapshot(
                deepx_rust_sdk::rest::models::DeepXSubaccountSnapshot {
                    profile,
                    balances,
                    equity,
                    margin_ratio,
                },
            ));
        }
        let final_directory = self.get_wallet_subaccounts(address).await?;
        validation.validate_final_directory(&final_directory)?;
        Ok(DeepXWalletAccountSnapshot(
            deepx_rust_sdk::rest::models::DeepXWalletAccountSnapshot {
                directory,
                stats,
                subaccounts,
            },
        ))
    }

    /// Returns bounded position lifecycle pages for every validated wallet subaccount.
    ///
    /// The directory and statistics must agree, every profile must bind the requested wallet,
    /// and position IDs must be unique across all pages and subaccounts. A final directory read
    /// catches a changed membership during collection. These mutable REST reads do not establish
    /// a block-pinned snapshot or finalized position state.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid limits or filters, inconsistent account identity, duplicate
    /// position IDs, failed pages, or changed directory membership. No partial result is returned.
    pub async fn get_wallet_perp_positions(
        &self,
        wallet: &str,
        max_subaccounts: usize,
        max_pages_per_subaccount: usize,
        market_id: Option<u64>,
        only_closed: Option<bool>,
        page_size: Option<u32>,
    ) -> Result<DeepXWalletPerpPositions> {
        DeepXWalletPerpPositionsRequest {
            wallet: wallet.to_string(),
            max_subaccounts,
            max_pages_per_subaccount,
            market_id,
            only_closed,
            page_size,
        }
        .validate()?;
        let request_template = DeepXPerpPositionsRequest {
            subaccount: wallet.to_string(),
            market_id,
            only_closed,
            cursor: None,
            page_size,
        };
        let directory = self.get_wallet_subaccounts(wallet).await?;
        let stats = self.get_user_stats(wallet).await?;
        let validation =
            deepx_rust_sdk::rest::DeepXWalletCollectionValidation::for_perp_positions(&directory);
        validation.validate_stats(&stats)?;
        if directory.addresses.len() > max_subaccounts {
            return Err(DeepXHttpError::InvalidRequest(format!(
                "wallet directory exceeds max_subaccounts limit of {max_subaccounts}"
            )));
        }
        let mut subaccounts = Vec::with_capacity(directory.addresses.len());
        let mut position_ids = deepx_rust_sdk::rest::DeepXWalletPerpPositionContinuity::default();
        for address in &directory.addresses {
            let profile = self.get_subaccount_info(address, None).await?;
            validation.validate_profile(&profile)?;
            let request = DeepXPerpPositionsRequest {
                subaccount: address.clone(),
                ..request_template.clone()
            };
            let pages = self
                .get_perp_position_pages(&request, max_pages_per_subaccount)
                .await?;
            position_ids.observe_ids(
                pages
                    .iter()
                    .flat_map(|page| page.items.iter().map(|position| position.id)),
            )?;
            subaccounts.push(DeepXSubaccountPerpPositions(
                deepx_rust_sdk::rest::models::DeepXSubaccountPerpPositions { profile, pages },
            ));
        }
        let final_directory = self.get_wallet_subaccounts(wallet).await?;
        validation.validate_final_directory(&final_directory)?;
        Ok(DeepXWalletPerpPositions(
            deepx_rust_sdk::rest::models::DeepXWalletPerpPositions {
                directory,
                stats,
                subaccounts,
            },
        ))
    }

    /// Returns a validated subaccount profile with optional expected-owner verification.
    ///
    /// `expected_authority` should be the wallet address independently derived from the signer
    /// when this read is used as ownership evidence.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid addresses, transport or HTTP failures, malformed responses,
    /// venue-level failures, identity mismatches, or invalid account metadata.
    pub async fn get_subaccount_info(
        &self,
        address: &str,
        expected_authority: Option<&str>,
    ) -> Result<DeepXSubaccountProfile> {
        if let Some(authority) = expected_authority {
            validate_account_address("subaccount info", "expected authority", authority)?;
        }
        let profile = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_subaccount_info(&self.base_urls[index], address)
            })
            .await?;
        if expected_authority
            .is_some_and(|expected| !profile.authority.eq_ignore_ascii_case(expected))
        {
            return Err(invalid_account_state_response(
                "subaccount info",
                "profile authority does not match the expected wallet".to_string(),
            ));
        }
        Ok(DeepXSubaccountProfile(profile))
    }

    /// Queries and verifies signer authority over one configured subaccount.
    ///
    /// A direct signer must own the profile's wallet. A delegated signer must appear in both the
    /// wallet's delegate directory and the delegate's reverse directory with current order
    /// permission. Directory and profile reads are repeated before returning the proof.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid key or subaccount, failed REST reads, inconsistent account
    /// directory or profile identities, changed membership, invalid delegate permission, duplicate
    /// subaccounts, or an inactive profile.
    pub async fn get_account_ownership_proof(
        &self,
        key: &DeepXPrivateKey,
        subaccount: &str,
    ) -> std::result::Result<DeepXAccountOwnershipProof, DeepXAccountOwnershipError> {
        let signer = derive_signer_account_id(key)?;
        let signer_address = format!("0x{}", nautilus_core::hex::encode(signer));
        let profile = self.get_subaccount_info(subaccount, None).await?;
        let wallet = profile.authority.clone();
        let directory = self.get_wallet_subaccounts(&wallet).await?;
        let delegated = !wallet.eq_ignore_ascii_case(&signer_address);
        let (delegates, delegators) = if delegated {
            (
                Some(self.get_delegate_accounts(&wallet).await?),
                Some(self.get_delegator_accounts(&signer_address).await?),
            )
        } else {
            (None, None)
        };
        let verify = |directory: &DeepXWalletSubaccounts,
                      profile: &DeepXSubaccountProfile,
                      delegates: Option<&DeepXWalletDelegateAccounts>,
                      delegators: Option<&DeepXDelegateWallets>| {
            if let (Some(delegates), Some(delegators)) = (delegates, delegators) {
                verify_delegated_account_ownership(
                    key,
                    subaccount,
                    directory,
                    profile,
                    delegates,
                    delegators,
                    get_atomic_clock_realtime().get_time_ms(),
                )
            } else {
                verify_account_ownership(key, subaccount, directory, profile)
            }
        };
        let proof = verify(
            &directory,
            &profile,
            delegates.as_ref(),
            delegators.as_ref(),
        )?;
        let final_directory = self.get_wallet_subaccounts(&wallet).await?;
        let final_profile = self.get_subaccount_info(subaccount, Some(&wallet)).await?;
        let initial_set: HashSet<_> = directory
            .addresses
            .iter()
            .map(|address| address.to_ascii_lowercase())
            .collect();
        let final_set: HashSet<_> = final_directory
            .addresses
            .iter()
            .map(|address| address.to_ascii_lowercase())
            .collect();
        if initial_set != final_set
            || !profile
                .authority
                .eq_ignore_ascii_case(&final_profile.authority)
            || !profile.address.eq_ignore_ascii_case(&final_profile.address)
            || profile.status != final_profile.status
            || profile.height != final_profile.height
            || profile.created_at != final_profile.created_at
        {
            return Err(DeepXAccountOwnershipError::DirectoryChanged);
        }
        if let (Some(delegates), Some(delegators)) = (delegates.as_ref(), delegators.as_ref()) {
            let final_delegates = self.get_delegate_accounts(&wallet).await?;
            let final_delegators = self.get_delegator_accounts(&signer_address).await?;
            if delegates != &final_delegates || delegators != &final_delegators {
                return Err(DeepXAccountOwnershipError::DirectoryChanged);
            }
            verify(
                &final_directory,
                &final_profile,
                Some(&final_delegates),
                Some(&final_delegators),
            )?;
        }
        Ok(proof)
    }

    /// Returns validated exact lending balances for a subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid subaccount address, transport or HTTP failures, malformed
    /// responses, venue-level failures, identity mismatches, duplicate assets, or invalid values.
    pub async fn get_subaccount_balances(
        &self,
        subaccount: &str,
    ) -> Result<DeepXSubaccountBalances> {
        let balances = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_subaccount_balances(&self.base_urls[index], subaccount)
            })
            .await?;
        Ok(DeepXSubaccountBalances(balances))
    }

    /// Returns a validated exact equity summary for a subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid subaccount address, transport or HTTP failures, malformed
    /// responses, venue-level failures, identity mismatches, or invalid nonnegative totals.
    pub async fn get_subaccount_equity(&self, address: &str) -> Result<DeepXSubaccountEquity> {
        let equity = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_subaccount_equity(&self.base_urls[index], address)
            })
            .await?;
        Ok(DeepXSubaccountEquity(equity))
    }

    /// Returns account-wide displays and exact margin data for a subaccount.
    ///
    /// The legacy numeric fields are rounded displays. The current response's exact fields identify
    /// a maintenance requirement, but do not establish initial margin or free collateral.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid subaccount address, transport or HTTP failures, malformed
    /// responses, venue-level failures, missing exact values, or inconsistent financial values.
    pub async fn get_subaccount_margin_ratio(
        &self,
        address: &str,
    ) -> Result<DeepXSubaccountMarginRatio> {
        let margin_ratio = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_subaccount_margin_ratio(&self.base_urls[index], address)
            })
            .await?;
        Ok(DeepXSubaccountMarginRatio(margin_ratio))
    }

    /// Returns the uninterpreted delegate configuration payload for a wallet owner.
    ///
    /// The generic OpenAPI response does not prove delegate permissions, expiry, or ownership.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid wallet address, transport or HTTP failures, malformed
    /// responses, or a venue-level failure envelope.
    pub async fn get_delegate_accounts_raw(
        &self,
        address: &str,
    ) -> Result<Box<serde_json::value::RawValue>> {
        self.get_sdk_read_with_retry(|index| {
            self.client
                .get_delegate_accounts_raw(&self.base_urls[index], address)
        })
        .await
    }

    /// Returns the validated wallet-level delegate configurations for one wallet.
    ///
    /// The response is a mutable REST observation and does not grant signing or mutation authority.
    /// Callers must independently establish current chain authorization before using a delegate.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid wallet address, transport or venue failures, malformed
    /// responses, invalid or duplicate delegate addresses, or inconsistent timestamps.
    pub async fn get_delegate_accounts(&self, wallet: &str) -> Result<DeepXWalletDelegateAccounts> {
        let sdk_directory = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_delegate_accounts(&self.base_urls[index], wallet)
            })
            .await?;
        let directory = DeepXWalletDelegateAccounts::new(
            sdk_directory.wallet().to_string(),
            sdk_directory
                .accounts
                .into_iter()
                .map(DeepXDelegateAccount)
                .collect(),
        );
        Ok(directory)
    }

    /// Returns the validated wallet addresses bound to one delegate account.
    ///
    /// The reverse directory is a mutable REST observation and does not prove current chain
    /// authorization or ordering semantics.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid delegate address, transport or venue failures, malformed
    /// responses, invalid returned wallet addresses, or duplicate wallets.
    pub async fn get_delegator_accounts(&self, delegate: &str) -> Result<DeepXDelegateWallets> {
        let directory = self
            .get_sdk_read_with_retry(|index| {
                self.client
                    .get_delegator_accounts(&self.base_urls[index], delegate)
            })
            .await?;
        let directory = DeepXDelegateWallets(directory);
        Ok(directory)
    }

    async fn get_sdk_read_with_retry<T, F, Fut>(&self, read: F) -> Result<T>
    where
        F: Fn(usize) -> Fut,
        Fut: Future<Output = std::result::Result<T, DeepXRestError>>,
    {
        let attempt = AtomicUsize::new(0);
        self.retry_manager
            .execute_with_retry(
                "DeepX public HTTP GET",
                || {
                    let index = attempt.fetch_add(1, Ordering::Relaxed) % self.base_urls.len();
                    let read = &read;
                    async move { read(index).await.map_err(DeepXHttpError::from) }
                },
                should_retry_http_error,
                DeepXHttpError::from,
            )
            .await
    }
}

fn invalid_account_state_response(endpoint: &'static str, message: String) -> DeepXHttpError {
    DeepXHttpError::InvalidAccountResponse { endpoint, message }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        Json, Router,
        extract::{Query, RawQuery},
        http::StatusCode,
        routing::get,
    };
    use nautilus_network::retry::RetryConfig;
    use rstest::rstest;
    use rust_decimal::Decimal;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;
    use crate::http::DeepXPerpCandleInterval;

    const SPOT_TRADES_ETH_USDC_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/spot_trades_eth_usdc_page_1.json"
    );
    const SPOT_MARKETS_SPEC369_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/spot_markets_spec369.json"
    );
    const PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/perp_history_orders_account_market_3.json"
    );

    fn active_perp_open_order_fixture() -> serde_json::Value {
        let mut response: serde_json::Value =
            serde_json::from_str(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE).unwrap();
        for item in response["data"]["items"].as_array_mut().unwrap() {
            item["status"] = json!("Open");
            item["sizeFilled"] = json!(0);
            item["sizeRemain"] = item["size"].clone();
            item["avgFillPrice"] = serde_json::Value::Null;
        }
        response
    }
    const SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/spot_history_orders_eth_usdc_page_1.json"
    );
    const SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/spot_account_trades_eth_usdc_page_1.json"
    );
    const SPOT_WALLET_ORDERS_ETH_USDC_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/spot_wallet_orders_eth_usdc_page_1.json"
    );
    const SPOT_WALLET_TRADES_ETH_USDC_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/spot_wallet_trades_eth_usdc_page_1.json"
    );
    const PERP_WALLET_TRADES_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/perp_wallet_trades_page.json"
    );
    const PERP_WALLET_ORDERS_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/perp_wallet_orders_page_1.json"
    );
    const WALLET_FUNDING_FEES_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/wallet_funding_fees_page_2.json"
    );
    const WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_1.json"
    );
    const WALLET_HOURLY_FUNDING_PAGE_2_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_2.json"
    );
    const PERP_POSITIONS_ACCOUNT_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/perp_positions_account_market_3.json"
    );
    const ALL_SUBACCOUNTS_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/all_subaccounts_page_1.json"
    );
    const SUBACCOUNT_INFO_RESPONSE: &str =
        include_str!("../../../../../deepx-rust-sdk/test_data/http/testnet/subaccount_info.json");
    const SUBACCOUNT_BALANCES_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/subaccount_balances.json"
    );
    const SUBACCOUNT_EQUITY_RESPONSE: &str =
        include_str!("../../../../../deepx-rust-sdk/test_data/http/testnet/subaccount_equity.json");
    const SUBACCOUNT_MARGIN_RATIO_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/subaccount_margin_ratio_exact.json"
    );
    const WALLET_BALANCE_CHANGES_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/wallet_balance_changes_page_2.json"
    );
    const WALLET_LIQUIDATION_RECORDS_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/wallet_liquidation_records_page_1.json"
    );
    const WALLET_USER_STATS_RESPONSE: &str =
        include_str!("../../../../../deepx-rust-sdk/test_data/http/testnet/wallet_user_stats.json");
    const QUOTA_HISTORY_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/quota_history_purchase_page.json"
    );

    fn balance_changes_request() -> DeepXBalanceChangesRequest {
        DeepXBalanceChangesRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            start_ms: Some(1_789_498_000_000),
            end_ms: Some(1_789_523_000_000),
            change_types: vec![
                crate::http::DeepXBalanceChangeType::FundingFee,
                crate::http::DeepXBalanceChangeType::Settlement,
            ],
            cursor: None,
            page_size: Some(2),
        }
    }

    fn all_subaccounts_request() -> DeepXAllSubaccountsRequest {
        DeepXAllSubaccountsRequest {
            cursor: None,
            page_size: Some(5),
        }
    }

    fn quota_history_request() -> DeepXQuotaHistoryRequest {
        DeepXQuotaHistoryRequest {
            wallet: "0x0a40c3efbc3b3bebdf1fb6f0f8c612eb336b25ae".to_string(),
            buyer_address: None,
            history_type: None,
            cursor: None,
            limit: Some(5),
        }
    }

    fn spot_trades_request() -> DeepXSpotTradesRequest {
        DeepXSpotTradesRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
            wallet: None,
            start_ms: None,
            end_ms: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(2),
        }
    }

    fn perp_wallet_trades_request() -> DeepXPerpWalletTradesRequest {
        DeepXPerpWalletTradesRequest {
            wallet: "0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string(),
            market_name: None,
            market_id: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        }
    }

    fn spot_wallet_orders_request() -> DeepXSpotWalletOrdersRequest {
        DeepXSpotWalletOrdersRequest {
            wallet: Some("0x89bb0946046588f4f257ffc71bd3231aba13474a".to_string()),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        }
    }

    fn spot_wallet_trades_request() -> DeepXSpotWalletTradesRequest {
        DeepXSpotWalletTradesRequest {
            wallet: "0x89bb0946046588f4f257ffc71bd3231aba13474a".to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        }
    }

    fn perp_wallet_orders_request() -> DeepXPerpWalletOrdersRequest {
        DeepXPerpWalletOrdersRequest {
            wallet: "0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string(),
            market_name: None,
            market_id: None,
            is_long: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        }
    }

    fn liquidation_records_request() -> DeepXLiquidationRecordsRequest {
        DeepXLiquidationRecordsRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            liquidation_types: Vec::new(),
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(5),
        }
    }

    fn wallet_funding_fee_request() -> DeepXWalletFundingFeeRequest {
        DeepXWalletFundingFeeRequest {
            wallet: "0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string(),
            market_name: None,
            market_id: Some(3),
            start_ms: Some(1_789_498_000_000),
            end_ms: Some(1_789_523_000_000),
            cursor: None,
            page_size: Some(2),
        }
    }

    fn hourly_unsettled_funding_request() -> DeepXHourlyUnsettledFundingRequest {
        DeepXHourlyUnsettledFundingRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            market_id: Some(3),
            start_ms: None,
            end_ms: None,
            cursor: None,
            page_size: Some(5),
            sort: DeepXAccountSortOrder::Descending,
        }
    }

    #[tokio::test]
    async fn perp_open_order_pages_follow_cursor_through_adapter() {
        let captured = active_perp_open_order_fixture();
        let first_item = captured["data"]["items"][0].clone();
        let second_item = captured["data"]["items"][1].clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let app = Router::new().route(
            PERP_OPEN_ORDERS_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let call = observed.fetch_add(1, Ordering::Relaxed);
                    let first_item = first_item.clone();
                    let second_item = second_item.clone();
                    async move {
                        assert_eq!(query["marketId"], "3");
                        assert_eq!(query["pageSize"], "1");
                        assert_eq!(query["sort"], "DESC");
                        let (item, has_next, next_cursor) = match call {
                            0 => {
                                assert!(!query.contains_key("cursor"));
                                (first_item, true, Some("next"))
                            }
                            1 => {
                                assert_eq!(query["cursor"], "next");
                                (second_item, false, None)
                            }
                            _ => panic!("unexpected perpetual open-order page request"),
                        };
                        Json(json!({
                            "code": 200,
                            "msg": "success",
                            "fail": false,
                            "data": {
                                "items": [item],
                                "hasNext": has_next,
                                "nextCursor": next_cursor,
                            },
                        }))
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let request = DeepXPerpOpenOrdersRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_id: Some(3),
            is_long: None,
            cursor: None,
            page_size: Some(1),
            sort: DeepXAccountSortOrder::Descending,
        };

        let pages = client.get_perp_open_order_pages(&request, 2).await.unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items.len(), 1);
        assert_eq!(pages[1].items.len(), 1);

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        server.abort();
    }

    #[tokio::test]
    async fn spot_history_order_pages_follow_the_opaque_cursor() {
        const CURSOR: &str = concat!(
            "MTc4OTYyNzM3OTA1MzoxODg1NzM3ODQ6MTc6MHg5MDY4ZDRhYzg5MWExNDc4NGMx",
            "Nzg3N2ViNzRiZDg0ODliMzM2N2M3MWQ3Mjc2NmRiZmE0ZGZiZmI2NjJmYTM3OjE3",
            "ODk2MjczNzg2NTc6U0VMTA",
        );
        const TERMINAL: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":[],"#,
            r#""hasNext":false,"nextCursor":null}}"#,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let app = Router::new().route(
            SPOT_HISTORY_ORDERS_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let call = observed.fetch_add(1, Ordering::Relaxed);
                    async move {
                        assert_eq!(query["name"], "ETH/USDC");
                        assert_eq!(query["pageSize"], "3");
                        match call {
                            0 => {
                                assert!(!query.contains_key("cursor"));
                                SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE
                            }
                            1 => {
                                assert_eq!(query["cursor"], CURSOR);
                                TERMINAL
                            }
                            _ => panic!("unexpected Spot history page request"),
                        }
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let pages = client
            .get_spot_history_order_pages(
                &DeepXSpotHistoryOrdersRequest {
                    subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
                    name: Some("ETH/USDC".to_string()),
                    pair: None,
                    order_side: None,
                    cursor: None,
                    sort: DeepXAccountSortOrder::Descending,
                    page_size: Some(3),
                },
                2,
            )
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items.len(), 3);
        assert!(pages[1].items.is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn spot_open_order_pages_follow_cursor_and_validate_across_pages() {
        const CURSOR: &str = concat!(
            "MTc4OTYyNzM3OTA1MzoxODg1NzM3ODQ6MTc6MHg5MDY4ZDRhYzg5MWExNDc4NGMx",
            "Nzg3N2ViNzRiZDg0ODliMzM2N2M3MWQ3Mjc2NmRiZmE0ZGZiZmI2NjJmYTM3OjE3",
            "ODk2MjczNzg2NTc6U0VMTA",
        );
        const TERMINAL: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":[],"#,
            r#""hasNext":false,"nextCursor":null}}"#,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let app = Router::new().route(
            SPOT_OPEN_ORDERS_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let call = observed.fetch_add(1, Ordering::Relaxed);
                    async move {
                        assert_eq!(query["name"], "ETH/USDC");
                        assert_eq!(query["pageSize"], "3");
                        match call {
                            0 => {
                                assert!(!query.contains_key("cursor"));
                                SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE
                            }
                            1 => {
                                assert_eq!(query["cursor"], CURSOR);
                                TERMINAL
                            }
                            _ => panic!("unexpected Spot open-order page request"),
                        }
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let pages = client
            .get_spot_open_order_pages(
                &DeepXSpotOpenOrdersRequest {
                    subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
                    name: Some("ETH/USDC".to_string()),
                    pair: None,
                    order_side: None,
                    cursor: None,
                    sort: DeepXAccountSortOrder::Descending,
                    page_size: Some(3),
                },
                2,
            )
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items.len(), 3);
        assert!(pages[1].items.is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn spot_account_trade_pages_follow_cursor() {
        const CURSOR: &str = "MTc4OTYyNzM1MDM1MzoxODg1NzMzNzQwMDAwODA";
        const TERMINAL: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":[],"#,
            r#""hasNext":false,"nextCursor":null}}"#,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let app = Router::new().route(
            SPOT_ACCOUNT_TRADES_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let call = observed.fetch_add(1, Ordering::Relaxed);
                    async move {
                        match call {
                            0 => {
                                assert!(!query.contains_key("cursor"));
                                SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE
                            }
                            1 => {
                                assert_eq!(query["cursor"], CURSOR);
                                TERMINAL
                            }
                            _ => panic!("unexpected Spot account-trade page request"),
                        }
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let request = DeepXSpotAccountTradesRequest {
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
        };
        let pages = client
            .get_spot_account_trade_pages(&request, 2)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items.len(), 3);

        server.abort();
    }

    #[tokio::test]
    async fn spot_wallet_order_collector_forwards_cursor_through_adapter() {
        let first: serde_json::Value =
            serde_json::from_str(SPOT_WALLET_ORDERS_ETH_USDC_RESPONSE).unwrap();
        let cursor = first["data"][0]["subaccounts"][0]["orders"]["nextCursor"]
            .as_str()
            .unwrap()
            .to_string();
        let mut terminal = first.clone();
        let mut item = first["data"][0]["subaccounts"][0]["orders"]["items"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .clone();
        item["orderId"] = json!("1789631508485");
        terminal["data"][0]["subaccounts"][0]["orders"] = json!({
            "items": [item],
            "nextCursor": null,
            "hasNext": false
        });
        let app = Router::new().route(
            SPOT_WALLET_ORDERS_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    let cursor = cursor.clone();
                    async move {
                        if let Some(received) = query.get("cursor") {
                            assert_eq!(received, &cursor);
                            Json(terminal)
                        } else {
                            Json(first)
                        }
                    }
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();

        let pages = client
            .get_spot_wallet_order_pages(&spot_wallet_orders_request(), 2)
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert!(!pages[1].has_next);
    }

    #[tokio::test]
    async fn spot_wallet_trade_collector_forwards_cursor_through_adapter() {
        let first: serde_json::Value =
            serde_json::from_str(SPOT_WALLET_TRADES_ETH_USDC_RESPONSE).unwrap();
        let cursor = first["data"][0]["subaccounts"][0]["trades"]["nextCursor"]
            .as_str()
            .unwrap()
            .to_string();
        let mut terminal = first.clone();
        let mut item = first["data"][0]["subaccounts"][0]["trades"]["items"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .clone();
        item["id"] = json!(188_632_780_999_999_u64);
        terminal["data"][0]["subaccounts"][0]["trades"] = json!({
            "items": [item],
            "nextCursor": null,
            "hasNext": false
        });
        let app = Router::new().route(
            SPOT_WALLET_TRADES_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    let cursor = cursor.clone();
                    async move {
                        if let Some(received) = query.get("cursor") {
                            assert_eq!(received, &cursor);
                            Json(terminal)
                        } else {
                            Json(first)
                        }
                    }
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();

        let pages = client
            .get_spot_wallet_trade_pages(&spot_wallet_trades_request(), 2)
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert!(!pages[1].has_next);
    }

    #[tokio::test]
    async fn subaccount_profile_checks_expected_authority() {
        let app = Router::new().route(
            SUBACCOUNT_INFO_PATH,
            get(|| async { SUBACCOUNT_INFO_RESPONSE }),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();
        let wallet = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
        let subaccount = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";

        let profile = client
            .get_subaccount_info(subaccount, Some(wallet))
            .await
            .unwrap();
        assert_eq!(profile.authority, wallet);
        assert!(matches!(
            client
                .get_subaccount_info(
                    subaccount,
                    Some("0x1111111111111111111111111111111111111111"),
                )
                .await,
            Err(DeepXHttpError::InvalidAccountResponse {
                endpoint: "subaccount info",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn all_subaccount_collector_wraps_sdk_pages() {
        let first: serde_json::Value = serde_json::from_str(ALL_SUBACCOUNTS_RESPONSE).unwrap();
        let cursor = first["data"]["nextCursor"].as_str().unwrap().to_string();
        let mut terminal = first.clone();
        let mut item = first["data"]["items"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .clone();
        item["subaccount"] = json!("0x1111111111111111111111111111111111111111");
        item["createdAt"] = json!("2026-09-08T10:03:25.573Z");
        terminal["data"] = json!({
            "items": [item],
            "nextCursor": null,
            "hasNext": false
        });
        let app = Router::new().route(
            ALL_SUBACCOUNTS_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    let cursor = cursor.clone();
                    async move {
                        if let Some(received) = query.get("cursor") {
                            assert_eq!(received, &cursor);
                            Json(terminal)
                        } else {
                            Json(first)
                        }
                    }
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();

        let result = client
            .get_all_subaccount_pages(&all_subaccounts_request(), 2)
            .await;

        let pages = result.unwrap();
        assert_eq!(pages.len(), 2);
        assert!(!pages[1].has_next);
    }

    #[tokio::test]
    async fn quota_history_collector_wraps_sdk_pages() {
        let mut first: serde_json::Value = serde_json::from_str(QUOTA_HISTORY_RESPONSE).unwrap();
        first["data"]["hasNext"] = json!(true);
        first["data"]["nextCursor"] = json!("next-page");
        let mut terminal = first.clone();
        let mut item = first["data"]["items"][0].clone();
        item["id"] = json!("purchase:other");
        item["blockNumber"] = json!(181_248_893);
        item["eventIndex"] = json!(48);
        item["createdAt"] = json!("2026-09-11T08:16:37.475Z");
        terminal["data"] = json!({"items": [item], "nextCursor": null, "hasNext": false});
        let app = Router::new().route(
            QUOTA_HISTORY_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    async move {
                        if query
                            .get("cursor")
                            .is_some_and(|value| value == "next-page")
                        {
                            Json(terminal)
                        } else {
                            Json(first)
                        }
                    }
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();
        let result = client
            .get_quota_history_pages(&quota_history_request(), 2)
            .await;
        assert_eq!(result.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn balance_change_collection_wraps_sdk_pages() {
        let first: serde_json::Value =
            serde_json::from_str(WALLET_BALANCE_CHANGES_RESPONSE).unwrap();
        let mut terminal = first.clone();
        terminal["data"]["items"] = json!([first["data"]["items"][1].clone()]);
        terminal["data"]["hasNext"] = json!(false);
        terminal["data"]["nextCursor"] = serde_json::Value::Null;
        terminal["data"]["items"][0]["id"] = json!(1_867_287_170_002_001_u64);
        let app = Router::new().route(
            BALANCE_CHANGES_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    async move {
                        if query.contains_key("cursor") {
                            Json(terminal)
                        } else {
                            Json(first)
                        }
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let result = client
            .get_balance_change_pages(&balance_changes_request(), 2)
            .await;

        let pages = result.unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages.iter().map(|page| page.items.len()).sum::<usize>(), 3);
        server.abort();
    }

    #[tokio::test]
    async fn liquidation_record_collection_wraps_sdk_pages() {
        let first: serde_json::Value =
            serde_json::from_str(WALLET_LIQUIDATION_RECORDS_RESPONSE).unwrap();
        let cursor = first["data"]["nextCursor"].as_str().unwrap().to_string();
        let mut terminal = first.clone();
        terminal["data"]["items"] = json!([first["data"]["items"][4].clone()]);
        terminal["data"]["hasNext"] = json!(false);
        terminal["data"]["nextCursor"] = serde_json::Value::Null;
        terminal["data"]["items"][0]["id"] = json!(1_747);
        terminal["data"]["items"][0]["createdAt"] = json!("2026-05-27T15:30:29.000Z");
        let app = Router::new().route(
            LIQUIDATION_RECORDS_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    let cursor = cursor.clone();
                    async move {
                        if let Some(received) = query.get("cursor") {
                            assert_eq!(received, &cursor);
                            Json(terminal)
                        } else {
                            Json(first)
                        }
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let result = client
            .get_liquidation_record_pages(&liquidation_records_request(), 2)
            .await;

        let pages = result.unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages.iter().map(|page| page.items.len()).sum::<usize>(), 6);
        server.abort();
    }

    #[tokio::test]
    async fn ordinary_liquidation_execution_collector_forwards_cursor_and_budget() {
        const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
        let app = Router::new().route(
            LIQUIDATION_EXECUTION_RECORDS_PATH,
            get(
                |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    if let Some(cursor) = query.get("cursor") {
                        assert_eq!(cursor, "next-1");
                        Json(json!({
                            "code": 200,
                            "msg": "success",
                            "fail": false,
                            "data": {
                                "items": [{"id": 2, "taskType": "liquidation"}],
                                "hasNext": false,
                                "nextCursor": null,
                            },
                        }))
                    } else {
                        Json(json!({
                            "code": 200,
                            "msg": "success",
                            "fail": false,
                            "data": {
                                "items": [{
                                    "id": 1,
                                    "taskType": "liquidation",
                                }],
                                "hasNext": true,
                                "nextCursor": "next-1",
                            },
                        }))
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let request = DeepXLiquidationExecutionRecordsRequest {
            wallet: Some(WALLET.to_string()),
            subaccount: None,
            status: None,
            reason: None,
            tx_hash: None,
            extrinsic_hash: None,
            trigger_block_from: None,
            trigger_block_to: None,
            start_ms: None,
            end_ms: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(1),
        };

        let pages = client
            .get_liquidation_execution_record_pages_raw(&request, 2)
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[1].items.len(), 1);
        assert!(matches!(
            client
                .get_liquidation_execution_record_pages_raw(&request, 1)
                .await,
            Err(DeepXHttpError::PaginationLimitExceeded { max_pages: 1 })
        ));
        server.abort();
    }

    #[tokio::test]
    async fn perp_wallet_order_collector_forwards_cursor_through_adapter() {
        let first: serde_json::Value = serde_json::from_str(PERP_WALLET_ORDERS_RESPONSE).unwrap();
        let cursor = first["data"][0]["subaccounts"][0]["orders"]["nextCursor"]
            .as_str()
            .unwrap()
            .to_string();
        let mut terminal = first.clone();
        let mut item = first["data"][0]["subaccounts"][1]["orders"]["items"][0].clone();
        item["orderId"] = json!("32");
        item["createTime"] = json!("2026-05-29T08:15:11.756Z");
        item["updatedTime"] = json!("2026-05-29T08:15:11.756Z");
        terminal["data"][0]["subaccounts"] = json!([{
            "subaccount": "0x977654069311fa41b88f7901dc65fae9a76fd60b",
            "orders": {
                "items": [item],
                "nextCursor": null,
                "hasNext": false
            }
        }]);
        let app = Router::new().route(
            PERP_WALLET_ORDERS_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    let cursor = cursor.clone();
                    async move {
                        if let Some(received) = query.get("cursor") {
                            assert_eq!(received, &cursor);
                            Json(terminal)
                        } else {
                            Json(first)
                        }
                    }
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();

        let pages = client
            .get_perp_wallet_order_pages(&perp_wallet_orders_request(), 2)
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(
            pages[1].markets[0].subaccounts[0].orders.items[0].order_id,
            "32"
        );
        assert!(!pages[1].has_next);
    }

    #[tokio::test]
    async fn perp_wallet_trade_collector_follows_selected_group_cursor_through_adapter() {
        let mut first: serde_json::Value =
            serde_json::from_str(PERP_WALLET_TRADES_RESPONSE).unwrap();
        first["data"].as_array_mut().unwrap().truncate(1);
        let selected = "0x977654069311fa41b88f7901dc65fae9a76fd60b";
        let cursor = first["data"][0]["subaccounts"][2]["trades"]["nextCursor"]
            .as_str()
            .unwrap()
            .to_string();
        let mut terminal = first.clone();
        let group = &mut terminal["data"][0]["subaccounts"][2];
        for (index, trade) in group["trades"]["items"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
        {
            trade["id"] = json!(52_780_136_000_283_u64 - index as u64 * 9);
        }
        group["trades"]["nextCursor"] = serde_json::Value::Null;
        group["trades"]["hasNext"] = json!(false);
        let app = Router::new().route(
            PERP_WALLET_TRADES_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    let cursor = cursor.clone();
                    async move {
                        if let Some(received) = query.get("cursor") {
                            assert_eq!(received, &cursor);
                            Json(terminal)
                        } else {
                            Json(first)
                        }
                    }
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();
        let mut request = perp_wallet_trades_request();
        request.market_id = Some(3);
        let pages = client
            .get_perp_wallet_trade_pages_for_subaccount(&request, selected, "ETH-USDC", 2)
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items.len(), 5);
        assert_eq!(pages[1].items.len(), 5);
        assert_eq!(pages[1].items[0].id, 52_780_136_000_283);
        assert!(!pages[1].has_next);
    }

    #[tokio::test]
    async fn wallet_funding_fee_collection_wraps_sdk_pages() {
        let first: serde_json::Value = serde_json::from_str(WALLET_FUNDING_FEES_RESPONSE).unwrap();
        let mut terminal = first.clone();
        terminal["data"]["items"] = json!([first["data"]["items"][1].clone()]);
        terminal["data"]["hasNext"] = json!(false);
        terminal["data"]["nextCursor"] = serde_json::Value::Null;
        terminal["data"]["items"][0]["eventIdx"] = json!(22);
        let app = Router::new().route(
            WALLET_FUNDING_FEE_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    async move {
                        if query.contains_key("cursor") {
                            Json(terminal)
                        } else {
                            Json(first)
                        }
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let result = client
            .get_wallet_funding_fee_pages(&wallet_funding_fee_request(), 2)
            .await;

        let pages = result.unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages.iter().map(|page| page.items.len()).sum::<usize>(), 3);
        server.abort();
    }

    #[tokio::test]
    async fn hourly_unsettled_funding_collection_forwards_complete_cursor() {
        let first: serde_json::Value =
            serde_json::from_str(WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE).unwrap();
        let mut terminal: serde_json::Value =
            serde_json::from_str(WALLET_HOURLY_FUNDING_PAGE_2_RESPONSE).unwrap();
        terminal["data"].as_array_mut().unwrap().truncate(2);
        let app = Router::new().route(
            HOURLY_UNSETTLED_FUNDING_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    async move {
                        if query.contains_key("cursorTimestamp") {
                            assert_eq!(query["cursorTimestamp"], "1789506862596");
                            assert_eq!(query["cursorMarketId"], "3");
                            assert_eq!(
                                query["cursorSubaccount"],
                                "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
                            );
                            assert_eq!(
                                query["cursorEventId"],
                                "4846:186852294:0x3516695ba7257d3dc2988b2c31435991ba7e111f68962fcdadca6b1664b77ecf:none:26"
                            );
                            Json(terminal)
                        } else {
                            Json(first)
                        }
                    }
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();

        let pages = client
            .get_hourly_unsettled_funding_pages(&hourly_unsettled_funding_request(), 2)
            .await
            .unwrap();

        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].len(), 5);
        assert_eq!(pages[1].len(), 2);
        assert_eq!(
            pages[0].last().unwrap().baseline_index_raw,
            pages[1][0].cumulative_index_raw
        );
    }

    #[tokio::test]
    async fn wallet_perp_positions_reject_duplicate_ids_across_subaccounts() {
        const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
        const FIRST: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
        const SECOND: &str = "0x1111111111111111111111111111111111111111";

        let directory =
            json!({"code": 200, "msg": "success", "data": [FIRST, SECOND], "fail": false});
        let mut stats: serde_json::Value =
            serde_json::from_str(WALLET_USER_STATS_RESPONSE).unwrap();
        stats["data"]["subaccounts"] = json!([FIRST, SECOND]);
        stats["data"]["numberOfSubAccounts"] = json!(2);
        let profile: serde_json::Value = serde_json::from_str(SUBACCOUNT_INFO_RESPONSE).unwrap();
        let positions: serde_json::Value =
            serde_json::from_str(PERP_POSITIONS_ACCOUNT_RESPONSE).unwrap();
        let app = Router::new()
            .route(
                deepx_rust_sdk::rest::WALLET_SUBACCOUNTS_PATH,
                get(move || {
                    let directory = directory.clone();
                    async move { Json(directory) }
                }),
            )
            .route(
                USER_STATS_PATH,
                get(move || {
                    let stats = stats.clone();
                    async move { Json(stats) }
                }),
            )
            .route(
                SUBACCOUNT_INFO_PATH,
                get(
                    move |Query(query): Query<std::collections::HashMap<String, String>>| {
                        let mut profile = profile.clone();
                        async move {
                            profile["data"]["address"] = json!(query["address"]);
                            Json(profile)
                        }
                    },
                ),
            )
            .route(
                PERP_POSITIONS_PATH,
                get(
                    move |Query(query): Query<std::collections::HashMap<String, String>>| {
                        let mut positions = positions.clone();
                        async move {
                            if query["user"] == SECOND {
                                positions["data"]["items"]
                                    .as_array_mut()
                                    .unwrap()
                                    .truncate(1);
                                positions["data"]["items"][0]["owner"] = json!(SECOND);
                            }
                            Json(positions)
                        }
                    },
                ),
            );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();

        assert!(matches!(
            client
                .get_wallet_perp_positions(WALLET, 2, 2, Some(3), None, Some(5))
                .await,
            Err(DeepXHttpError::InvalidAccountResponse {
                endpoint: "wallet perp positions",
                message,
            }) if message == "position ID 1659167 repeats across subaccounts"
        ));
    }

    #[rstest]
    #[case("valid")]
    #[case("changed-directory")]
    #[case("foreign-stats")]
    #[case("foreign-authority")]
    #[tokio::test]
    async fn wallet_account_snapshot_is_complete_or_returns_no_value(#[case] scenario: &str) {
        let wallet = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
        let subaccount = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
        let directory = json!({
            "code": 200,
            "msg": "success",
            "data": [subaccount],
            "fail": false,
        });
        let directory_reads = Arc::new(AtomicUsize::new(0));
        let changed_directory = scenario == "changed-directory";
        let mut stats: serde_json::Value =
            serde_json::from_str(WALLET_USER_STATS_RESPONSE).unwrap();
        stats["data"]["subaccounts"] = json!([if scenario == "foreign-stats" {
            "0x1111111111111111111111111111111111111111"
        } else {
            subaccount
        }]);
        stats["data"]["numberOfSubAccounts"] = json!(1);
        let mut profile: serde_json::Value =
            serde_json::from_str(SUBACCOUNT_INFO_RESPONSE).unwrap();
        if scenario == "foreign-authority" {
            profile["data"]["authority"] = json!("0x1111111111111111111111111111111111111111");
        }
        let app = Router::new()
            .route(
                deepx_rust_sdk::rest::WALLET_SUBACCOUNTS_PATH,
                get(move || {
                    let directory = directory.clone();
                    let directory_reads = directory_reads.clone();
                    async move {
                        let mut response = directory;
                        if changed_directory && directory_reads.fetch_add(1, Ordering::SeqCst) > 0 {
                            response["data"] = json!([]);
                        }
                        Json(response)
                    }
                }),
            )
            .route(
                USER_STATS_PATH,
                get(move || {
                    let stats = stats.clone();
                    async move { Json(stats) }
                }),
            )
            .route(
                SUBACCOUNT_INFO_PATH,
                get(move || {
                    let profile = profile.clone();
                    async move { Json(profile) }
                }),
            )
            .route(
                SUBACCOUNT_BALANCES_PATH,
                get(|| async { SUBACCOUNT_BALANCES_RESPONSE }),
            )
            .route(
                SUBACCOUNT_EQUITY_PATH,
                get(|| async { SUBACCOUNT_EQUITY_RESPONSE }),
            )
            .route(
                SUBACCOUNT_MARGIN_RATIO_PATH,
                get(|| async { SUBACCOUNT_MARGIN_RATIO_RESPONSE }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let result = client.get_wallet_account_snapshot(wallet).await;

        match scenario {
            "changed-directory" | "foreign-stats" => assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidAccountResponse {
                    endpoint: "wallet account snapshot",
                    ..
                })
            )),
            "foreign-authority" => assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidAccountResponse {
                    endpoint: "subaccount info",
                    ..
                })
            )),
            _ => {
                let snapshot = result.unwrap();
                assert_eq!(snapshot.directory.wallet(), wallet);
                assert_eq!(snapshot.directory.addresses, [subaccount]);
                assert_eq!(snapshot.stats.subaccounts, [subaccount]);
                assert_eq!(snapshot.subaccounts.len(), 1);
                assert_eq!(snapshot.subaccounts[0].profile.address, subaccount);
                assert_eq!(snapshot.subaccounts[0].balances.address, subaccount);
                assert_eq!(snapshot.subaccounts[0].equity.subaccount, subaccount);
                assert_eq!(
                    snapshot.subaccounts[0].margin_ratio.collateral,
                    Decimal::new(97_096, 2)
                );
            }
        }
        server.abort();
    }

    #[rstest]
    #[case::stable(false)]
    #[case::changed(true)]
    #[tokio::test]
    async fn account_ownership_proof_queries_the_derived_wallet_and_configured_subaccount(
        #[case] changed: bool,
    ) {
        let key = crate::common::DeepXPrivateKey::new(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &crate::common::DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let signer = derive_signer_account_id(&key).unwrap();
        let wallet = format!("0x{}", nautilus_core::hex::encode(signer));
        let subaccount = "0x1111111111111111111111111111111111111111".to_string();
        let directory_wallet = wallet.clone();
        let directory_subaccount = subaccount.clone();
        let directory_reads = Arc::new(AtomicUsize::new(0));
        let observed_directory_reads = directory_reads.clone();
        let profile_wallet = wallet.clone();
        let profile_subaccount = subaccount.clone();
        let app = Router::new()
            .route(
                deepx_rust_sdk::rest::WALLET_SUBACCOUNTS_PATH,
                get(
                    move |Query(query): Query<std::collections::HashMap<String, String>>| {
                        let wallet = directory_wallet.clone();
                        let subaccount = directory_subaccount.clone();
                        let directory_reads = directory_reads.clone();
                        async move {
                            assert_eq!(query.get("address"), Some(&wallet));
                            let read_index = directory_reads.fetch_add(1, Ordering::SeqCst);
                            let addresses = if changed && read_index > 0 {
                                vec![]
                            } else {
                                vec![subaccount]
                            };
                            Json(json!({
                                "code": 200,
                                "msg": "success",
                                "data": addresses,
                                "fail": false,
                            }))
                        }
                    },
                ),
            )
            .route(
                SUBACCOUNT_INFO_PATH,
                get(
                    move |Query(query): Query<std::collections::HashMap<String, String>>| {
                        let wallet = profile_wallet.clone();
                        let subaccount = profile_subaccount.clone();
                        async move {
                            assert_eq!(query.get("address"), Some(&subaccount));
                            Json(json!({
                                "code": 200,
                                "msg": "success",
                                "data": {
                                    "authority": wallet,
                                    "address": subaccount,
                                    "name": "test",
                                    "status": "Active",
                                    "spotPositions": [],
                                    "nextOrderId": 1,
                                    "spotMarginTradingEnabled": false,
                                    "marginStrategy": "Cross",
                                    "height": 1,
                                    "createdAt": 1,
                                },
                                "fail": false,
                            }))
                        }
                    },
                ),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let result = client.get_account_ownership_proof(&key, &subaccount).await;

        if changed {
            assert!(matches!(
                result,
                Err(DeepXAccountOwnershipError::DirectoryChanged)
            ));
        } else {
            let proof = result.unwrap();
            assert_eq!(proof.signer(), signer);
            assert_eq!(proof.subaccount(), [0x11; 20]);
        }
        assert_eq!(observed_directory_reads.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[rstest]
    #[case::stable(false)]
    #[case::revoked_during_read(true)]
    #[tokio::test]
    async fn account_ownership_proof_validates_current_delegate_directories(#[case] revoked: bool) {
        let key = crate::common::DeepXPrivateKey::new(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &crate::common::DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let signer = format!(
            "0x{}",
            nautilus_core::hex::encode(derive_signer_account_id(&key).unwrap())
        );
        let wallet = "0x3333333333333333333333333333333333333333";
        let subaccount = "0x1111111111111111111111111111111111111111";
        let delegate_reads = Arc::new(AtomicUsize::new(0));
        let counted_reads = Arc::clone(&delegate_reads);
        let forward_signer = signer.clone();
        let app = Router::new()
            .route(
                SUBACCOUNT_INFO_PATH,
                get(move || async move {
                    Json(json!({
                        "code": 200, "msg": "success", "fail": false,
                        "data": {
                            "authority": wallet,
                            "address": subaccount,
                            "name": "test",
                            "status": "Active",
                            "spotPositions": [],
                            "nextOrderId": 1,
                            "spotMarginTradingEnabled": false,
                            "marginStrategy": "Cross",
                            "height": 1,
                            "createdAt": 1,
                        },
                    }))
                }),
            )
            .route(
                deepx_rust_sdk::rest::WALLET_SUBACCOUNTS_PATH,
                get(move || async move {
                    Json(json!({
                        "code": 200, "msg": "success", "fail": false,
                        "data": [subaccount],
                    }))
                }),
            )
            .route(
                DELEGATE_ACCOUNTS_PATH,
                get(move || {
                    let signer = forward_signer.clone();
                    let reads = Arc::clone(&counted_reads);
                    async move {
                        let index = reads.fetch_add(1, Ordering::SeqCst);
                        let mode = if revoked && index > 0 {
                            "Disable"
                        } else {
                            "PlaceOrCancelOrder"
                        };
                        Json(json!({
                            "code": 200, "msg": "success", "fail": false,
                            "data": [{
                                "delegateAddress": signer,
                                "delegateName": "order bot",
                                "validUntil": 4_000_000_000_000_u64,
                                "mode": mode,
                                "createTime": 1,
                            }],
                        }))
                    }
                }),
            )
            .route(
                DELEGATOR_ACCOUNTS_PATH,
                get(move || async move {
                    Json(json!({
                        "code": 200, "msg": "success", "fail": false,
                        "data": [wallet],
                    }))
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let result = client.get_account_ownership_proof(&key, subaccount).await;

        if revoked {
            assert!(matches!(
                result,
                Err(DeepXAccountOwnershipError::DirectoryChanged)
            ));
        } else {
            let proof = result.unwrap();
            assert_eq!(proof.signer(), derive_signer_account_id(&key).unwrap());
            assert_eq!(proof.wallet(), [0x33; 20]);
            assert_eq!(proof.subaccount(), [0x11; 20]);
        }
        assert_eq!(delegate_reads.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[tokio::test]
    async fn historical_account_order_pages_follow_cursor_to_terminal_page() {
        const FIRST: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":["#,
            r#"{"orderId":"2"}],"hasNext":true,"nextCursor":"next-page"}}"#,
        );
        const SECOND: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":["#,
            r#"{"orderId":"1"}],"hasNext":false,"nextCursor":null}}"#,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let app = Router::new().route(
            PERP_HISTORY_ORDERS_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let call = observed.fetch_add(1, Ordering::Relaxed);
                    async move {
                        match call {
                            0 => {
                                assert!(!query.contains_key("cursor"));
                                FIRST
                            }
                            1 => {
                                assert_eq!(query["cursor"], "next-page");
                                SECOND
                            }
                            _ => panic!("unexpected history page request"),
                        }
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let pages = client
            .get_perp_history_order_pages_raw(
                &DeepXPerpHistoryOrdersRequest {
                    subaccount: "0x1111111111111111111111111111111111111111".to_string(),
                    market_id: Some(3),
                    cursor: None,
                    page_size: Some(1),
                    sort: crate::http::DeepXAccountSortOrder::Descending,
                },
                2,
            )
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items[0].get(), r#"{"orderId":"2"}"#);
        assert_eq!(pages[1].items[0].get(), r#"{"orderId":"1"}"#);
        server.abort();
    }

    #[tokio::test]
    async fn account_trade_page_budget_and_initial_cursor_fail_closed() {
        const PAGE: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":["#,
            r#"{"id":1}],"hasNext":true,"nextCursor":"next-page"}}"#,
        );
        let app = Router::new().route(PERP_ACCOUNT_TRADES_PATH, get(|| async { PAGE }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let mut request = DeepXPerpAccountTradesRequest {
            subaccount: "0x1111111111111111111111111111111111111111".to_string(),
            order_id: None,
            market_id: Some(3),
            is_long: None,
            cursor: None,
            sort: crate::http::DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(1),
        };

        assert!(matches!(
            client.get_perp_account_trade_pages_raw(&request, 1).await,
            Err(DeepXHttpError::PaginationLimitExceeded { max_pages: 1 })
        ));
        request.cursor = Some("next-page".to_string());
        assert!(matches!(
            client.get_perp_account_trade_pages_raw(&request, 2).await,
            Err(DeepXHttpError::RepeatedPaginationCursor { .. })
        ));
        assert!(matches!(
            client.get_perp_account_trade_pages_raw(&request, 0).await,
            Err(DeepXHttpError::InvalidPaginationLimit)
        ));
        server.abort();
    }

    #[tokio::test]
    async fn perp_position_pages_are_subaccount_scoped_and_cursor_bounded() {
        const FIRST: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":["#,
            r#"{"id":9007199254740993,"baseAssetAmount":0.004000000000000001,"#,
            r#""fundingPayment":-0.000000000000000001,"future":"retained"}],"#,
            r#""hasNext":true,"nextCursor":"next-page"}}"#,
        );
        const FIRST_ITEM: &str = concat!(
            r#"{"id":9007199254740993,"baseAssetAmount":0.004000000000000001,"#,
            r#""fundingPayment":-0.000000000000000001,"future":"retained"}"#,
        );
        const SECOND: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":[],"#,
            r#""hasNext":false,"nextCursor":null}}"#,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let app = Router::new().route(
            PERP_POSITIONS_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let call = observed.fetch_add(1, Ordering::Relaxed);
                    async move {
                        assert_eq!(query["user"], "0x1111111111111111111111111111111111111111");
                        assert_eq!(query["marketId"], "3");
                        assert_eq!(query["onlyClosed"], "true");
                        assert_eq!(query["addressType"], "subaccount");
                        assert_eq!(query["pageSize"], "1");
                        match call {
                            0 => {
                                assert!(!query.contains_key("cursor"));
                                FIRST
                            }
                            1 => {
                                assert_eq!(query["cursor"], "next-page");
                                SECOND
                            }
                            _ => panic!("unexpected position page request"),
                        }
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let request = DeepXPerpPositionsRequest {
            subaccount: "0x1111111111111111111111111111111111111111".to_string(),
            market_id: Some(3),
            only_closed: Some(true),
            cursor: None,
            page_size: Some(1),
        };
        let pages = client
            .get_perp_position_pages_raw(&request, 2)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items[0].get(), FIRST_ITEM);
        assert!(pages[1].items.is_empty());
        server.abort();
    }

    #[derive(Debug, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct OpenInterestQuery {
        market_id: u64,
        time_frame: String,
        start: u64,
        end: u64,
        limit: u32,
        sort: String,
    }

    #[derive(Debug, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct PerpCandlesQuery {
        market_id: u64,
        time_frame: String,
        start: u64,
        end: u64,
        limit: u32,
        sort: String,
        trade_view: bool,
    }

    fn immediate_retry_config(max_retries: u32) -> RetryConfig {
        RetryConfig {
            max_retries,
            initial_delay_ms: 1,
            max_delay_ms: 1,
            backoff_factor: 1.0,
            jitter_ms: 0,
            operation_timeout_ms: Some(1_000),
            immediate_first: true,
            max_elapsed_ms: Some(5_000),
        }
    }

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn collects_perp_long_short_ratio_history_across_bounded_cursor_pages() {
        let router = Router::new().route(
            PERP_LONG_SHORT_RATIO_PATH,
            get(|RawQuery(raw_query): RawQuery| async move {
                let query = raw_query.unwrap_or_default();
                assert!(query.contains("marketId=3"));
                assert!(query.contains("start=100"));
                assert!(query.contains("end=300"));
                assert!(query.contains("limit=1"));
                if query.contains("cursor=next") {
                    Json(json!({
                        "code": 200,
                        "msg": "success",
                        "data": {
                            "marketId": 3,
                            "details": [{"longShortRatio": "1.25", "time": 200}],
                            "nextCursor": null,
                            "hasNext": false
                        },
                        "fail": false
                    }))
                } else {
                    Json(json!({
                        "code": 200,
                        "msg": "success",
                        "data": {
                            "marketId": 3,
                            "details": [{"longShortRatio": "1.5", "time": 100}],
                            "nextCursor": "next",
                            "hasNext": true
                        },
                        "fail": false
                    }))
                }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let request = DeepXLongShortRatioRequest {
            market_id: 3,
            start_ms: 100,
            end_ms: Some(300),
            limit: Some(1),
            cursor: None,
        };

        let records = client
            .get_perp_long_short_ratios_history_limited(
                &request,
                std::num::NonZeroUsize::new(2).unwrap(),
                2,
            )
            .await
            .unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].time, 100);
        assert_eq!(records[1].time, 200);
    }

    #[tokio::test]
    async fn spot_trade_collection_forwards_cursor_through_adapter() {
        let first: serde_json::Value = serde_json::from_str(SPOT_TRADES_ETH_USDC_RESPONSE).unwrap();
        let cursor = first["data"]["nextCursor"].as_str().unwrap().to_string();
        let mut terminal = first.clone();
        terminal["data"]["items"] = json!([first["data"]["items"][1].clone()]);
        terminal["data"]["hasNext"] = json!(false);
        terminal["data"]["nextCursor"] = serde_json::Value::Null;
        terminal["data"]["items"][0]["id"] = json!(188_405_949_000_001_u64);
        terminal["data"]["items"][0]["tradeTime"] = json!("2026-09-17T03:27:09.999Z");
        let router = Router::new().route(
            SPOT_TRADES_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    let cursor = cursor.clone();
                    async move {
                        if let Some(received) = query.get("cursor") {
                            assert_eq!(received, &cursor);
                            Json(terminal)
                        } else {
                            Json(first)
                        }
                    }
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();

        let pages = client
            .get_spot_trade_pages(&spot_trades_request(), 2)
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages.iter().map(|page| page.items.len()).sum::<usize>(), 3);
    }

    #[rstest]
    #[case(1, 1, true)]
    #[case(3, 2, true)]
    #[case(3, 1, false)]
    #[tokio::test]
    async fn funding_history_record_limit_and_page_budget(
        #[case] limit: usize,
        #[case] budget: usize,
        #[case] succeeds: bool,
    ) {
        let router = Router::new().route(
            PERP_FUNDING_RATE_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    assert_eq!(query["start"], "60000");
                    assert_eq!(query["end"], "180000");
                    assert_eq!(query["sort"], "DESC");
                    assert_eq!(query["interval"], "1m");
                    let first = !query.contains_key("cursor");
                    let page_size: usize = query["limit"].parse().unwrap();
                    let details = if first {
                        assert_eq!(page_size, limit.min(2));
                        vec![
                            json!({"time": 180_000, "fundingRate": "0.000012500000000000001"}),
                            json!({"time": 120_000, "fundingRate": "-0.00005"}),
                        ]
                        .into_iter()
                        .take(page_size)
                        .collect::<Vec<_>>()
                    } else {
                        assert_eq!(query["cursor"], "opaque+/cursor=");
                        assert_eq!(page_size, 1);
                        vec![json!({"time": 60_000, "fundingRate": "0"})]
                    };
                    Json(json!({"code": 200, "msg": "success", "fail": false,
                    "data": {"marketId": 3, "details": details, "hasNext": first,
                        "nextCursor": if first { Some("opaque+/cursor=") } else { None }}}))
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let request = DeepXFundingRateRequest {
            market_id: 3,
            start_ms: 60_000,
            end_ms: Some(180_000),
            limit: Some(2),
            cursor: None,
        };
        let result = client
            .get_perp_funding_rates_history_limited(
                &request,
                std::num::NonZeroUsize::new(limit).unwrap(),
                budget,
            )
            .await;
        if succeeds {
            let samples = result.unwrap();
            assert_eq!(samples.len(), limit);
            assert_eq!(
                samples[0].funding_rate.to_string(),
                "0.000012500000000000001"
            );
        } else {
            assert!(matches!(
                result,
                Err(DeepXHttpError::PaginationLimitExceeded { max_pages: 1 })
            ));
        }
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TradesHistoryQuery {
        market_id: u64,
        start: u64,
        end: u64,
        page_size: u32,
        cursor: Option<String>,
        sort: String,
    }

    fn history_trade(id: u64, timestamp_ms: i64) -> serde_json::Value {
        json!({
            "id": id, "marketId": 3, "buyerOrderId": "65043", "buyer": "0x11",
            "sellerOrderId": "65044", "seller": "0x22", "price": "1792.600000000000001",
            "size": "0.004000000000000001", "buyerLeverage": 25.0, "sellerLeverage": "12.500000000000000001",
            "createdAt": jiff::Timestamp::from_millisecond(timestamp_ms).unwrap().to_string(),
            "filledDirection": "Short", "taker": "Seller", "takerFee": "0.000716",
            "makerFee": "-0.000716"
        })
    }

    fn trades_history_request() -> DeepXPerpTradesHistoryRequest {
        DeepXPerpTradesHistoryRequest {
            market_id: 3,
            start_ms: 1_000,
            end_ms: 3_000,
            page_size: 2,
            max_pages: 2,
        }
    }

    #[rstest]
    #[case(1, 1, true, 1)]
    #[case(3, 2, true, 2)]
    #[case(3, 1, false, 1)]
    #[tokio::test]
    async fn limited_trade_history_shrinks_pages_and_honors_budget(
        #[case] limit: usize,
        #[case] budget: usize,
        #[case] succeeds: bool,
        #[case] expected_calls: usize,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let router = Router::new().route(
            PERP_TRADES_PATH,
            get(move |Query(query): Query<TradesHistoryQuery>| {
                let calls = Arc::clone(&handler_calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(query.start, 1_000);
                    assert_eq!(query.end, 3_000);
                    let first = query.cursor.is_none();
                    let items = if first {
                        assert_eq!(query.page_size, limit.min(2) as u32);
                        vec![history_trade(3, 3_000), history_trade(2, 2_000)]
                            .into_iter()
                            .take(query.page_size as usize)
                            .collect::<Vec<_>>()
                    } else {
                        assert_eq!(query.page_size, 1);
                        assert_eq!(query.cursor.as_deref(), Some("next"));
                        vec![history_trade(1, 1_000)]
                    };
                    Json(json!({"code": 200, "msg": "success", "fail": false,
                        "data": {"items": items, "hasNext": first,
                            "nextCursor": if first { Some("next") } else { None }}}))
                }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let mut request = trades_history_request();
        request.max_pages = budget;
        let result = client
            .get_perp_trades_history_limited(&request, std::num::NonZeroUsize::new(limit).unwrap())
            .await;
        if succeeds {
            let trades = result.unwrap();
            assert_eq!(trades.len(), limit);
            assert_eq!(trades[0].id, 3);
        } else {
            assert!(matches!(
                result,
                Err(DeepXHttpError::PaginationLimitExceeded { max_pages: 1 })
            ));
        }
        assert_eq!(calls.load(Ordering::Relaxed), expected_calls);
    }

    #[rstest]
    #[tokio::test]
    async fn bounded_trade_history_bridges_sdk_pages() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let router = Router::new().route(
            PERP_TRADES_PATH,
            get(move |Query(query): Query<TradesHistoryQuery>| {
                let calls = Arc::clone(&handler_calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(query.market_id, 3);
                    assert_eq!(query.start, 1_000);
                    assert_eq!(query.end, 3_000);
                    assert_eq!(query.page_size, 2);
                    assert_eq!(query.sort, "DESC");
                    let first = query.cursor.is_none();
                    if !first {
                        assert_eq!(query.cursor.as_deref(), Some("opaque+/cursor="));
                    }
                    let items = vec![history_trade(
                        if first { 2 } else { 1 },
                        if first { 3_000 } else { 1_000 },
                    )];
                    Json(json!({"code": 200, "fail": false, "msg": "success",
                        "data": {"items": items, "hasNext": first,
                            "nextCursor": if first { Some("opaque+/cursor=") } else { None }}}))
                }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let trades = client
            .get_perp_trades_history(&trades_history_request())
            .await
            .unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].id, 2);
        assert_eq!(trades[1].id, 1);
        assert_eq!(trades[0].price.to_string(), "1792.600000000000001");
        assert_eq!(trades[0].size.to_string(), "0.004000000000000001");
        assert_eq!(trades[0].buyer_leverage, Decimal::from(25));
        assert_eq!(
            trades[0].seller_leverage.to_string(),
            "12.500000000000000001"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn collects_perp_candle_history_across_bounded_time_pages() {
        let calls = Arc::new(AtomicUsize::new(0));
        let route_calls = Arc::clone(&calls);
        let router = Router::new().route(
            PERP_CANDLES_PATH,
            get(move |Query(query): Query<PerpCandlesQuery>| {
                let calls = Arc::clone(&route_calls);
                async move {
                    let page = calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(query.market_id, 3);
                    assert_eq!(query.limit, CANDLE_PAGE_SIZE);
                    assert_eq!(query.sort, "ASC");
                    let (start, count) = match page {
                        0 => {
                            assert_eq!(query.start, 1_000);
                            (1_000, CANDLE_PAGE_SIZE)
                        }
                        1 => {
                            assert_eq!(query.start, 6_000);
                            (6_000, 2)
                        }
                        _ => panic!("unexpected candle history page"),
                    };
                    let details = (0..count)
                        .map(|offset| {
                            json!({
                                "volume": 1,
                                "high": 1,
                                "low": 1,
                                "open": 1,
                                "close": 1,
                                "time": start + u64::from(offset),
                            })
                        })
                        .collect::<Vec<_>>();
                    Json(json!({
                        "code": 200,
                        "msg": "success",
                        "data": {"pair": "ETH-USDC", "details": details},
                        "fail": false,
                    }))
                }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let request = DeepXPerpCandlesRequest {
            market_id: 3,
            interval: DeepXPerpCandleInterval::OneMinute,
            start_ms: 1_000,
            end_ms: Some(6_002),
            limit: None,
        };

        let history = client
            .get_perp_candles_history_limited(&request, 2)
            .await
            .unwrap();

        assert_eq!(history.pair, "ETH-USDC");
        assert_eq!(history.details.len(), 5_002);
        assert_eq!(history.details[0].time, 1_000);
        assert_eq!(history.details[4_999].time, 5_999);
        assert_eq!(history.details[5_000].time, 6_000);
        assert_eq!(history.details[5_001].time, 6_001);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn collects_spot_candle_history_across_bounded_time_pages() {
        let calls = Arc::new(AtomicUsize::new(0));
        let route_calls = Arc::clone(&calls);
        let router = Router::new().route(
            SPOT_CANDLES_PATH,
            get(move |Query(query): Query<HashMap<String, String>>| {
                let calls = Arc::clone(&route_calls);
                async move {
                    let page = calls.fetch_add(1, Ordering::Relaxed);
                    let requested_start = query["start"].parse::<u64>().unwrap();
                    assert_eq!(query["name"], "ETH/USDC");
                    assert_eq!(query["limit"], CANDLE_PAGE_SIZE.to_string());
                    assert_eq!(query["sort"], "ASC");
                    let (start, count) = match page {
                        0 => {
                            assert_eq!(requested_start, 1_000);
                            (1_000, CANDLE_PAGE_SIZE)
                        }
                        1 => {
                            assert_eq!(requested_start, 6_000);
                            (6_000, 2)
                        }
                        _ => panic!("unexpected Spot candle history page"),
                    };
                    let details = (0..count)
                        .map(|offset| {
                            json!({
                                "volume": 1,
                                "high": 1,
                                "low": 1,
                                "open": 1,
                                "close": 1,
                                "time": start + u64::from(offset),
                            })
                        })
                        .collect::<Vec<_>>();
                    Json(json!({
                        "code": 200,
                        "msg": "success",
                        "data": {"pair": "ETH/USDC", "details": details},
                        "fail": false,
                    }))
                }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let request = DeepXSpotCandlesRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
            interval: DeepXPerpCandleInterval::OneMinute,
            start_ms: 1_000,
            end_ms: Some(6_002),
            limit: None,
        };

        let history = client
            .get_spot_candles_history_limited(&request, 2)
            .await
            .unwrap();

        assert_eq!(history.pair, "ETH/USDC");
        assert_eq!(history.details.len(), 5_002);
        assert_eq!(history.details[4_999].time, 5_999);
        assert_eq!(history.details[5_000].time, 6_000);
        assert_eq!(history.details[5_001].time, 6_001);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[rstest]
    #[case(PERP_CANDLES_PATH)]
    #[case(PERP_MARK_PRICE_PATH)]
    #[case(PERP_ORACLE_PRICE_PATH)]
    #[tokio::test]
    async fn history_endpoints_reject_invalid_decoded_pages(#[case] path: &'static str) {
        let router = Router::new().route(
            path,
            get(|| async {
                let candle = json!({"volume": "1", "high": "3", "low": "1",
                    "open": "2", "close": "3", "time": 60000});
                Json(json!({"code": 200, "msg": "success", "fail": false,
                    "data": {"pair": "ETH-USDC", "details": [candle.clone(), candle]}}))
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let result = match path {
            PERP_CANDLES_PATH => {
                client
                    .get_perp_candles(&DeepXPerpCandlesRequest {
                        market_id: 3,
                        interval: DeepXPerpCandleInterval::OneMinute,
                        start_ms: 60_000,
                        end_ms: Some(120_000),
                        limit: Some(2),
                    })
                    .await
            }
            PERP_MARK_PRICE_PATH => {
                client
                    .get_perp_mark_prices(&DeepXPerpMarkPriceRequest {
                        market_id: 3,
                        interval: DeepXPerpCandleInterval::OneMinute,
                        start_ms: 60_000,
                        end_ms: Some(120_000),
                        limit: Some(2),
                    })
                    .await
            }
            _ => {
                client
                    .get_perp_oracle_prices(&DeepXPerpOraclePriceRequest {
                        market_id: 3,
                        interval: DeepXPerpCandleInterval::OneMinute,
                        start_ms: 60_000,
                        end_ms: Some(120_000),
                        limit: Some(2),
                    })
                    .await
            }
        };
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            DeepXHttpError::InvalidHistoryResponse { .. }
        ));
        assert!(!should_retry_http_error(&error));
    }

    #[tokio::test]
    async fn collects_open_interest_with_minute_aligned_continuation() {
        const START: u64 = 1_788_251_280_000;
        let router = Router::new().route(
            PERP_OPEN_INTEREST_PATH,
            get(|Query(query): Query<OpenInterestQuery>| async move {
                assert_eq!(query.market_id, 3);
                assert_eq!(query.end, START + 240_000);
                assert_eq!(query.limit, 2);
                assert_eq!(query.sort, "ASC");
                let times = match query.start {
                    START => vec![START, START + 60_000],
                    value if value == START + 120_000 => {
                        vec![START + 120_000, START + 180_000]
                    }
                    value => panic!("unexpected open-interest continuation {value}"),
                };
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "pair": "ETH-USDC",
                        "details": times.into_iter().map(|statistic_time| json!({
                            "total_oi": "3393.618",
                            "long_short_ratio": "1.8456014362657092",
                            "long_position_count": 2056,
                            "short_position_count": 1114,
                            "statistic_time": statistic_time,
                        })).collect::<Vec<_>>(),
                    },
                    "fail": false,
                }))
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let request = DeepXOpenInterestRequest {
            market_id: 3,
            start_ms: START,
            end_ms: Some(START + 180_000),
            limit: Some(2),
        };

        let history = client
            .get_perp_open_interest_history(&request, 2)
            .await
            .unwrap();
        assert_eq!(history.pair, "ETH-USDC");
        assert_eq!(
            history
                .details
                .iter()
                .map(|record| record.statistic_time)
                .collect::<Vec<_>>(),
            vec![START, START + 60_000, START + 120_000, START + 180_000]
        );
        assert_eq!(
            history.details[0].total_oi,
            "3393.618".parse::<Decimal>().unwrap()
        );
        assert!(matches!(
            client.get_perp_open_interest_history(&request, 1).await,
            Err(DeepXHttpError::InvalidRequest(message))
                if message == "open-interest history page budget exceeded"
        ));
    }

    #[tokio::test]
    async fn retries_transient_api_failure_before_returning_data() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = Arc::clone(&attempts);
        let router = Router::new().route(
            SPOT_MARKETS_PATH,
            get(move || {
                let server_attempts = Arc::clone(&server_attempts);
                async move {
                    if server_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Json(json!({
                            "code": 10010,
                            "msg": "rate limit exceeded",
                            "data": [],
                            "fail": true
                        }))
                    } else {
                        Json(json!({
                            "code": 200,
                            "msg": "success",
                            "data": [],
                            "fail": false
                        }))
                    }
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new_with_endpoints(
            [base_url],
            Some(5),
            None,
            immediate_retry_config(1),
        )
        .unwrap();

        let markets = client.get_spot_markets().await.unwrap();

        assert!(markets.is_empty());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn fails_over_to_next_endpoint_after_retryable_status() {
        let primary = spawn_server(Router::new().route(
            SPOT_MARKETS_PATH,
            get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "unavailable") }),
        ))
        .await;
        let secondary = spawn_server(Router::new().route(
            SPOT_MARKETS_PATH,
            get(|| async { SPOT_MARKETS_SPEC369_RESPONSE }),
        ))
        .await;
        let client = DeepXHttpClient::new_with_endpoints(
            [format!("{primary}/"), secondary],
            Some(1),
            None,
            immediate_retry_config(1),
        )
        .unwrap();

        let markets = client.get_spot_markets().await.unwrap();

        assert_eq!(client.base_url(), primary);
        assert_eq!(markets.len(), 2);
    }

    #[tokio::test]
    async fn network_configured_zero_retries_sends_one_request() {
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_clone = Arc::clone(&requests);
        let base_url = spawn_server(Router::new().route(
            SPOT_MARKETS_PATH,
            get(move || {
                let requests = Arc::clone(&requests_clone);
                async move {
                    requests.fetch_add(1, Ordering::SeqCst);
                    (StatusCode::SERVICE_UNAVAILABLE, "unavailable")
                }
            }),
        ))
        .await;
        let config = crate::config::DeepXNetworkConfig {
            base_url_rest: Some(base_url),
            http_read_retry: crate::config::DeepXHttpReadRetryConfig {
                max_retries: 0,
                initial_delay_ms: 1,
                max_delay_ms: 1,
                jitter_ms: 0,
                operation_timeout_ms: 1_000,
                max_elapsed_ms: 5_000,
            },
            ..Default::default()
        };
        let client = DeepXHttpClient::from_network_config(&config, Some(1), None).unwrap();

        let error = client.get_spot_markets().await.unwrap_err();

        assert!(matches!(error, DeepXHttpError::Http { status: 503, .. }));
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fails_over_to_next_endpoint_after_transport_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable = format!("http://{}", listener.local_addr().unwrap());
        let failed_server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
        });
        let secondary = spawn_server(Router::new().route(
            SPOT_MARKETS_PATH,
            get(|| async { SPOT_MARKETS_SPEC369_RESPONSE }),
        ))
        .await;
        let client = DeepXHttpClient::new_with_endpoints(
            [unavailable, secondary],
            Some(1),
            None,
            immediate_retry_config(1),
        )
        .unwrap();

        let markets = client.get_spot_markets().await.unwrap();

        failed_server.await.unwrap();
        assert_eq!(markets.len(), 2);
    }

    #[tokio::test]
    async fn does_not_fail_over_after_terminal_status() {
        let secondary_requests = Arc::new(AtomicUsize::new(0));
        let primary = spawn_server(Router::new().route(
            SPOT_MARKETS_PATH,
            get(|| async { (StatusCode::BAD_REQUEST, "invalid request") }),
        ))
        .await;
        let secondary_requests_clone = Arc::clone(&secondary_requests);
        let secondary = spawn_server(Router::new().route(
            SPOT_MARKETS_PATH,
            get(move || {
                let secondary_requests = Arc::clone(&secondary_requests_clone);
                async move {
                    secondary_requests.fetch_add(1, Ordering::SeqCst);
                    SPOT_MARKETS_SPEC369_RESPONSE
                }
            }),
        ))
        .await;
        let client = DeepXHttpClient::new_with_endpoints(
            [primary, secondary],
            Some(1),
            None,
            immediate_retry_config(1),
        )
        .unwrap();

        let error = client.get_spot_markets().await.unwrap_err();

        assert!(matches!(error, DeepXHttpError::Http { status: 400, .. }));
        assert_eq!(secondary_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn does_not_fail_over_after_decode_failure() {
        let secondary_requests = Arc::new(AtomicUsize::new(0));
        let primary =
            spawn_server(Router::new().route(SPOT_MARKETS_PATH, get(|| async { "not json" })))
                .await;
        let secondary_requests_clone = Arc::clone(&secondary_requests);
        let secondary = spawn_server(Router::new().route(
            SPOT_MARKETS_PATH,
            get(move || {
                let secondary_requests = Arc::clone(&secondary_requests_clone);
                async move {
                    secondary_requests.fetch_add(1, Ordering::SeqCst);
                    SPOT_MARKETS_SPEC369_RESPONSE
                }
            }),
        ))
        .await;
        let client = DeepXHttpClient::new_with_endpoints(
            [primary, secondary],
            Some(1),
            None,
            immediate_retry_config(1),
        )
        .unwrap();

        let error = client.get_spot_markets().await.unwrap_err();

        assert!(matches!(error, DeepXHttpError::Decode(_)));
        assert_eq!(secondary_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn preserves_final_error_after_retry_exhaustion() {
        let primary = spawn_server(Router::new().route(
            SPOT_MARKETS_PATH,
            get(|| async { (StatusCode::BAD_GATEWAY, "primary") }),
        ))
        .await;
        let secondary = spawn_server(Router::new().route(
            SPOT_MARKETS_PATH,
            get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "secondary") }),
        ))
        .await;
        let client = DeepXHttpClient::new_with_endpoints(
            [primary, secondary],
            Some(1),
            None,
            immediate_retry_config(1),
        )
        .unwrap();

        let error = client.get_spot_markets().await.unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::Http { status: 503, message } if message == "secondary"
        ));
    }

    #[rstest]
    #[case(Vec::new())]
    #[case(vec!["ftp://rest-api-testnet.deepx.fi".to_string()])]
    #[case(vec!["not a URL".to_string()])]
    fn rejects_invalid_endpoint_lists(#[case] base_urls: Vec<String>) {
        let error = DeepXHttpClient::new_with_endpoints(
            base_urls,
            Some(1),
            None,
            immediate_retry_config(0),
        )
        .unwrap_err();

        assert!(matches!(error, DeepXHttpError::InvalidBaseUrl(_)));
    }
}
