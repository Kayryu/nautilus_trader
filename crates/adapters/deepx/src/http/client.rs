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

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use nautilus_network::{
    http::{HttpClient, HttpRedirectPolicy, Method},
    retry::{RetryConfig, RetryManager},
};
use rust_decimal::Decimal;
use serde::{Serialize, de::DeserializeOwned};

use super::{
    error::{DeepXHttpError, Result},
    models::{
        DeepXAccountPage, DeepXApiResponse, DeepXBalanceChangeRecord, DeepXDelegateAccount,
        DeepXDelegateWallets, DeepXFundingRatePage, DeepXFundingRateRecord,
        DeepXHourlyUnsettledFundingRecord, DeepXLendingAsset, DeepXLendingInterestRateHistory,
        DeepXLendingInterestRateParams, DeepXLendingInterestRateRecord, DeepXLendingStatusHistory,
        DeepXLendingStatusRecord, DeepXLiquidationRecord, DeepXLongShortRatioPage,
        DeepXOpenInterestPage, DeepXPerpAccountTradeRecord, DeepXPerpCandlesPage,
        DeepXPerpFundingFeeRecord, DeepXPerpLastPrice, DeepXPerpLiquidationPrice, DeepXPerpMarket,
        DeepXPerpMarketLookup, DeepXPerpOrderBook, DeepXPerpOrderBookLevel, DeepXPerpOrderRecord,
        DeepXPerpPositionRecord, DeepXPerpTrade, DeepXPerpTradesPage, DeepXPerpVolume,
        DeepXPerpWalletOrderMarket, DeepXPerpWalletOrdersPage, DeepXPerpWalletTradeMarket,
        DeepXQuotaHistoryRecord, DeepXQuotaSummary, DeepXRawAccountPage,
        DeepXSpotAccountTradeRecord, DeepXSpotCandlesPage, DeepXSpotLastPrice, DeepXSpotMarket,
        DeepXSpotOrderBook, DeepXSpotOrderBookLevel, DeepXSpotOrderRecord, DeepXSpotTrade,
        DeepXSpotTradesPage, DeepXSpotVolume, DeepXSpotWalletOrderMarket,
        DeepXSpotWalletOrdersPage, DeepXSpotWalletTradeMarket, DeepXSpotWalletTradesPage,
        DeepXSubaccountBalances, DeepXSubaccountDirectoryRecord, DeepXSubaccountEquity,
        DeepXSubaccountMarginRatio, DeepXSubaccountProfile, DeepXSubaccountSnapshot,
        DeepXUserStats, DeepXWalletAccountSnapshot, DeepXWalletDelegateAccounts,
        DeepXWalletSubaccounts,
    },
    pagination::{CursorPagination, PaginationDecision, validate_cursor_page},
    query::{
        DeepXAccountSortOrder, DeepXAllSubaccountsRequest, DeepXBalanceChangesRequest,
        DeepXFundingRateRequest, DeepXHourlyUnsettledFundingCursor,
        DeepXHourlyUnsettledFundingRequest, DeepXLendingHistoryRequest, DeepXLendingMarketRequest,
        DeepXLiquidationRecordsRequest, DeepXLongShortRatioRequest, DeepXOpenInterestRequest,
        DeepXPerpAccountTradesRequest, DeepXPerpCandlesRequest, DeepXPerpFundingFeeRequest,
        DeepXPerpHistoryOrdersRequest, DeepXPerpLastPriceRequest, DeepXPerpLiquidationPriceRequest,
        DeepXPerpMarkPriceRequest, DeepXPerpOpenOrdersRequest, DeepXPerpOraclePriceRequest,
        DeepXPerpOrderBookRequest, DeepXPerpPositionsRequest, DeepXPerpTradesHistoryRequest,
        DeepXPerpTradesQuery, DeepXPerpTradesRequest, DeepXPerpVolumeRequest,
        DeepXPerpWalletOrdersRequest, DeepXPerpWalletTradesRequest, DeepXQuotaHistoryRequest,
        DeepXQuotaHistoryType, DeepXSpotAccountTradesRequest, DeepXSpotCandlesRequest,
        DeepXSpotHistoryOrdersRequest, DeepXSpotLastPriceRequest, DeepXSpotOpenOrdersRequest,
        DeepXSpotOrderBookRequest, DeepXSpotOrderByIdRequest, DeepXSpotOrderSide,
        DeepXSpotTradesRequest, DeepXSpotVolumeRequest, DeepXSpotWalletOrdersRequest,
        DeepXSpotWalletTradesRequest, DeepXWalletFundingFeeRequest,
    },
    retry::{deepx_http_retry_config, should_retry_http_error},
};
use crate::{
    account::{DeepXAccountOwnershipError, DeepXAccountOwnershipProof, verify_account_ownership},
    common::DeepXPrivateKey,
    signing::derive_signer_account_id,
};

const SPOT_MARKETS_PATH: &str = "/internal/v1/market/spot/markets";
const SPOT_MARKET_BY_NAME_PATH: &str = "/internal/v1/market/spot/market-by-name";
const SPOT_MARKET_BY_PAIR_PATH: &str = "/internal/v1/market/spot/market-by-pair";
const PERP_MARKETS_PATH: &str = "/internal/v1/market/perp/markets";
const PERP_MARKET_BY_ID_PATH: &str = "/internal/v1/market/perp/market-by-id";
const PERP_MARKET_BY_NAME_PATH: &str = "/internal/v1/market/perp/market-by-name";
const PERP_FUNDING_RATE_PATH: &str = "/internal/v1/market/perp/funding_rate";
const PERP_LONG_SHORT_RATIO_PATH: &str = "/internal/v1/market/perp/long_short_ratio";
const PERP_OPEN_INTEREST_PATH: &str = "/internal/v1/market/perp/open_interest";
const PERP_TRADES_PATH: &str = "/internal/v1/market/perp/trades";
const PERP_CANDLES_PATH: &str = "/internal/v1/market/perp/candles";
const PERP_MARK_PRICE_PATH: &str = "/internal/v1/market/perp/mark_price";
const PERP_ORACLE_PRICE_PATH: &str = "/internal/v1/market/perp/oracle_price";
const PERP_VOLUME_PATH: &str = "/internal/v1/market/perp/volume";
const PERP_LAST_PRICE_PATH: &str = "/internal/v1/market/perp/last_price";
const PERP_ORDER_BOOK_PATH: &str = "/internal/v1/market/perp/order-books";
const SPOT_TRADES_PATH: &str = "/internal/v1/market/spot/trades";
const SPOT_CANDLES_PATH: &str = "/internal/v1/market/spot/candles";
const SPOT_LAST_PRICE_PATH: &str = "/internal/v1/market/spot/last_price";
const SPOT_VOLUME_PATH: &str = "/internal/v1/market/spot/volume";
const SPOT_ORDER_BOOK_PATH: &str = "/internal/v1/market/spot/order-books";
const LENDING_ASSETS_PATH: &str = "/internal/v1/market/lending/assets";
const LENDING_INTEREST_RATE_PATH: &str = "/internal/v1/market/lending/interest-rate";
const LENDING_INTEREST_RATE_HISTORY_PATH: &str =
    "/internal/v1/market/lending/interest-rate-history";
const LENDING_STATUS_HISTORY_PATH: &str = "/internal/v1/market/lending/status-history";
const PERP_OPEN_ORDERS_PATH: &str = "/internal/v1/account/perp/open-orders";
const PERP_HISTORY_ORDERS_PATH: &str = "/internal/v1/account/perp/history-orders";
const SPOT_OPEN_ORDERS_PATH: &str = "/internal/v1/account/spot/open-orders";
const SPOT_HISTORY_ORDERS_PATH: &str = "/internal/v1/account/spot/history-orders";
const SPOT_ORDER_BY_ID_PATH: &str = "/internal/v1/account/spot/order-by-id";
const SPOT_ORDER_BY_TX_PATH: &str = "/internal/v1/account/spot/order-by-tx";
const SPOT_ACCOUNT_TRADES_PATH: &str = "/internal/v1/account/spot/trades";
const SPOT_WALLET_ORDERS_PATH: &str = "/internal/v1/account/spot/orders-by-wallet";
const SPOT_WALLET_TRADES_PATH: &str = "/internal/v1/account/spot/trades-by-wallet";
const PERP_ACCOUNT_TRADES_PATH: &str = "/internal/v1/account/perp/trades";
const PERP_WALLET_ORDERS_PATH: &str = "/internal/v1/account/perp/orders-by-wallet";
const PERP_WALLET_TRADES_PATH: &str = "/internal/v1/account/perp/trades-by-wallet";
const PERP_FUNDING_FEE_PATH: &str = "/internal/v1/account/funding-fee";
const WALLET_FUNDING_FEE_PATH: &str = "/internal/v1/account/funding-fee-by-wallet";
const HOURLY_UNSETTLED_FUNDING_PATH: &str = "/internal/v1/account/hourly-unsettled-funding";
const PERP_POSITIONS_PATH: &str = "/internal/v1/account/position";
const BALANCE_CHANGES_PATH: &str = "/internal/v1/account/balance-changes";
const LIQUIDATION_RECORDS_PATH: &str = "/internal/v1/account/subaccounts/liquidation-records";
const PERP_LIQUIDATION_PRICE_PATH: &str = "/internal/v1/account/perp/liquidate-price";
const USER_STATS_PATH: &str = "/internal/v1/account/user-stats";
const QUOTA_SUMMARY_PATH: &str = "/internal/v1/account/quota/summary";
const QUOTA_HISTORY_PATH: &str = "/internal/v1/account/quota/history";
const WALLET_SUBACCOUNTS_PATH: &str = "/internal/v1/account/subaccounts";
const ALL_SUBACCOUNTS_PATH: &str = "/internal/v1/account/subaccounts/all";
const SUBACCOUNT_INFO_PATH: &str = "/internal/v1/account/subaccount-info";
const SUBACCOUNT_BALANCES_PATH: &str = "/internal/v1/account/balances";
const SUBACCOUNT_EQUITY_PATH: &str = "/internal/v1/account/equity";
const SUBACCOUNT_MARGIN_RATIO_PATH: &str = "/internal/v1/account/margin-ratio";
const DELEGATE_ACCOUNTS_PATH: &str = "/internal/v1/account/delegate-accounts";
const DELEGATOR_ACCOUNTS_PATH: &str = "/internal/v1/account/delegator-accounts";
const PERP_ORDER_BY_TX_PATH: &str = "/internal/v1/account/perp/order-by-tx";

const MAX_ERROR_BODY_CHARS: usize = 1_024;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TransactionStatusQuery {
    tx_hash: String,
}

/// Raw client for unauthenticated, idempotent DeepX HTTP reads.
#[derive(Clone, Debug)]
pub struct DeepXHttpClient {
    client: HttpClient,
    transaction_client: HttpClient,
    base_urls: Arc<[String]>,
    timeout_secs: Option<u64>,
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
            .map(normalize_base_url)
            .collect::<Result<Vec<_>>>()?;
        if base_urls.is_empty() {
            return Err(DeepXHttpError::InvalidBaseUrl(String::new()));
        }
        let client = HttpClient::builder()
            .maybe_timeout_secs(timeout_secs)
            .maybe_proxy_url(proxy_url.clone())
            .build()?;
        let transaction_client = HttpClient::builder()
            .maybe_timeout_secs(timeout_secs)
            .maybe_proxy_url(proxy_url)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .build()?;
        Ok(Self {
            client,
            transaction_client,
            base_urls: base_urls.into(),
            timeout_secs,
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
    ) -> std::result::Result<nautilus_network::http::HttpResponse, DeepXHttpError> {
        Ok(self
            .transaction_client
            .post(
                format!("{}/internal/v1/chain/tx/transact", self.base_url()),
                None,
                Some(std::collections::HashMap::from([(
                    "Content-Type".to_string(),
                    "application/json".to_string(),
                )])),
                Some(body),
                self.timeout_secs,
                None,
            )
            .await?)
    }

    pub(crate) async fn get_transaction_status_once_raw(
        &self,
        extrinsic_hash: [u8; 32],
    ) -> Result<Box<serde_json::value::RawValue>> {
        let query = TransactionStatusQuery {
            tx_hash: format!("0x{}", nautilus_core::hex::encode(extrinsic_hash)),
        };
        let response = self
            .transaction_client
            .request_with_params(
                Method::GET,
                format!("{}/internal/v1/chain/tx/status", self.base_url()),
                Some(&query),
                None,
                None,
                self.timeout_secs,
                None,
            )
            .await?;

        if !response.status.is_success() {
            return Err(DeepXHttpError::Http {
                status: response.status.as_u16(),
                message: bounded_body(&response.body),
            });
        }
        into_api_data(serde_json::from_slice(&response.body)?)
    }

    /// Returns all configured base URLs in failover order.
    #[must_use]
    pub fn base_urls(&self) -> &[String] {
        &self.base_urls
    }

    /// Sends an unauthenticated GET and decodes a successful JSON response.
    ///
    /// The path must start with one `/` and cannot contain a URI authority, query, or fragment.
    /// Query parameters will be added through typed endpoint methods in later milestones.
    ///
    /// # Errors
    ///
    /// Returns a typed path, transport, HTTP status, or response decoding error.
    pub async fn get_json<R>(&self, path: &str) -> Result<R>
    where
        R: DeserializeOwned,
    {
        validate_path(path)?;
        let attempt = AtomicUsize::new(0);
        self.retry_manager
            .execute_with_retry(
                "DeepX public HTTP GET",
                || {
                    let index = attempt.fetch_add(1, Ordering::Relaxed) % self.base_urls.len();
                    self.get_json_once(&self.base_urls[index], path)
                },
                should_retry_http_error,
                DeepXHttpError::from,
            )
            .await
    }

    /// Returns all validated Spot market metadata.
    ///
    /// # Errors
    ///
    /// Returns an error for transport or venue failures, malformed metadata, invalid identities or
    /// financial fields, or duplicate market identities.
    pub async fn get_spot_markets(&self) -> Result<Vec<DeepXSpotMarket>> {
        let markets = self.get_spot_market_entries().await?;
        validate_spot_markets(&markets)?;
        Ok(markets)
    }

    pub(crate) async fn get_spot_market_entries(&self) -> Result<Vec<DeepXSpotMarket>> {
        let markets = self.get_market_data(SPOT_MARKETS_PATH).await?;
        for market in &markets {
            validate_spot_market("spot markets", market)?;
        }
        Ok(markets)
    }

    /// Returns validated Spot market metadata selected by its venue name.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty name, transport or venue failures, malformed metadata, an
    /// identity mismatch, or invalid financial fields.
    pub async fn get_spot_market_by_name(&self, name: &str) -> Result<DeepXSpotMarket> {
        #[derive(Serialize)]
        struct MarketNameQuery<'a> {
            name: &'a str,
        }

        if name.trim().is_empty() {
            return Err(DeepXHttpError::InvalidRequest(
                "spot-market-by-name name must not be empty".to_string(),
            ));
        }
        let market: DeepXSpotMarket = self
            .get_json_with_query(SPOT_MARKET_BY_NAME_PATH, &MarketNameQuery { name })
            .await?;
        validate_spot_market("spot market by name", &market)?;
        if market.name != name {
            return Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot market by name",
                message: "response does not match the requested market name".to_string(),
            });
        }
        Ok(market)
    }

    /// Returns validated Spot market metadata selected by its bytes32 pair identity.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid pair, transport or venue failures, malformed metadata, an
    /// identity mismatch, or invalid financial fields.
    pub async fn get_spot_market_by_pair(&self, pair: &str) -> Result<DeepXSpotMarket> {
        #[derive(Serialize)]
        struct MarketPairQuery<'a> {
            pair: &'a str,
        }

        let pair_identity = parse_bytes32_identity("spot-market-by-pair", "pair", pair)?;
        let pair = format!("0x{}", nautilus_core::hex::encode(pair_identity));
        let market: DeepXSpotMarket = self
            .get_json_with_query(SPOT_MARKET_BY_PAIR_PATH, &MarketPairQuery { pair: &pair })
            .await?;
        validate_spot_market("spot market by pair", &market)?;
        if decode_bytes32_identity(&market.pair) != Some(pair_identity) {
            return Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot market by pair",
                message: "response does not match the requested pair identity".to_string(),
            });
        }
        Ok(market)
    }

    /// Returns all available perpetual market metadata.
    ///
    /// # Errors
    ///
    /// Returns a typed transport, HTTP status, response decoding, or venue API error.
    pub async fn get_perp_markets(&self) -> Result<Vec<DeepXPerpMarket>> {
        self.get_market_data(PERP_MARKETS_PATH).await
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
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct MarketIdQuery {
            market_id: u64,
        }

        if market_id == 0 {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-market-by-id market ID must be positive".to_string(),
            ));
        }
        let market: DeepXPerpMarketLookup = self
            .get_json_with_query(PERP_MARKET_BY_ID_PATH, &MarketIdQuery { market_id })
            .await?;
        validate_perp_market_lookup("perp market by id", &market)?;
        if market.id != market_id {
            return Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "perp market by id",
                message: "response does not match the requested market ID".to_string(),
            });
        }
        Ok(market)
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
        #[derive(Serialize)]
        struct MarketNameQuery<'a> {
            name: &'a str,
        }

        if name.trim().is_empty() {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-market-by-name name must not be empty".to_string(),
            ));
        }
        let market: DeepXPerpMarketLookup = self
            .get_json_with_query(PERP_MARKET_BY_NAME_PATH, &MarketNameQuery { name })
            .await?;
        validate_perp_market_lookup("perp market by name", &market)?;
        if market.name != name {
            return Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "perp market by name",
                message: "response does not match the requested market name".to_string(),
            });
        }
        Ok(market)
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
        let assets: Vec<DeepXLendingAsset> = self.get_market_data(LENDING_ASSETS_PATH).await?;
        validate_lending_assets(&assets)?;
        Ok(assets)
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
        request.validate()?;
        let params: Vec<DeepXLendingInterestRateParams> = self
            .get_json_with_query(LENDING_INTEREST_RATE_PATH, &request.as_query())
            .await?;
        validate_lending_interest_rate_params(&params, request)?;
        Ok(params)
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
        request.validate()?;
        let history: DeepXLendingInterestRateHistory = self
            .get_json_with_query(LENDING_INTEREST_RATE_HISTORY_PATH, &request.as_query())
            .await?;
        validate_lending_interest_rate_history(&history.details, request)?;
        Ok(history)
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
        request.validate()?;
        let history: DeepXLendingStatusHistory = self
            .get_json_with_query(LENDING_STATUS_HISTORY_PATH, &request.as_query())
            .await?;
        validate_lending_status_history(&history.details, request)?;
        Ok(history)
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
        request.validate()?;
        let query = request.as_query();
        let page: DeepXFundingRatePage = self
            .get_json_with_query(PERP_FUNDING_RATE_PATH, &query)
            .await?;
        validate_response_market_id("perp funding-rate", request.market_id, page.market_id)?;
        validate_cursor_page(
            "perp funding-rate",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
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
        request.validate()?;
        let end = request.end_ms.ok_or_else(|| {
            DeepXHttpError::InvalidRequest(
                "funding-rate history requires a fixed end_ms".to_string(),
            )
        })?;
        let page_size = request.limit.unwrap_or(100);
        if page_size > 5_000 {
            return Err(DeepXHttpError::InvalidRequest(
                "funding-rate page size exceeds 5000".to_string(),
            ));
        }
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut page_request = request.clone();
        let mut records = Vec::new();
        let mut previous = None;
        let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse {
            endpoint: "perp funding-rate",
            message,
        };
        loop {
            let current_limit =
                page_size.min(u32::try_from(limit.get() - records.len()).unwrap_or(u32::MAX));
            page_request.limit = Some(current_limit);
            let query = page_request.as_descending_query();
            let page: DeepXFundingRatePage = self
                .get_json_with_query(PERP_FUNDING_RATE_PATH, &query)
                .await?;
            validate_response_market_id("perp funding-rate", request.market_id, page.market_id)?;
            validate_cursor_page(
                "perp funding-rate",
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            if page.details.len() > current_limit as usize {
                return Err(invalid("page exceeds requested limit".to_string()));
            }
            for record in &page.details {
                if !record.time.is_multiple_of(60_000) {
                    return Err(invalid(
                        "funding sample is not a UTC minute bucket".to_string(),
                    ));
                }
                if record.time < request.start_ms || record.time > end {
                    return Err(invalid(
                        "funding sample is outside requested range".to_string(),
                    ));
                }
                if previous.is_some_and(|time| record.time >= time) {
                    return Err(invalid(
                        "funding sample buckets are not strictly descending".to_string(),
                    ));
                }
                previous = Some(record.time);
            }
            if records.len() + page.details.len() == limit.get() {
                records.extend(page.details);
                return Ok(records);
            }
            let decision = pagination.observe_response_page(
                "perp funding-rate",
                page.details.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            records.extend(page.details);
            match decision {
                PaginationDecision::Complete => return Ok(records),
                PaginationDecision::Continue(cursor) => page_request.cursor = Some(cursor),
            }
        }
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
        request.validate()?;
        let query = request.as_query();
        let page: DeepXLongShortRatioPage = self
            .get_json_with_query(PERP_LONG_SHORT_RATIO_PATH, &query)
            .await?;
        validate_response_market_id("perp long-short-ratio", request.market_id, page.market_id)?;
        validate_cursor_page(
            "perp long-short-ratio",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
    }

    /// Returns one ascending page of perpetual open-interest history.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, transport or HTTP failures, malformed responses, or a
    /// venue-level failure envelope.
    pub async fn get_perp_open_interest(
        &self,
        request: &DeepXOpenInterestRequest,
    ) -> Result<DeepXOpenInterestPage> {
        request.validate()?;
        let query = request.as_query();
        self.get_json_with_query(PERP_OPEN_INTEREST_PATH, &query)
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
        self.get_perp_trades_page(&query, request.market_id, request.page_size)
            .await
    }

    async fn get_perp_trades_page(
        &self,
        query: &DeepXPerpTradesQuery<'_>,
        market_id: u64,
        page_size: Option<u32>,
    ) -> Result<DeepXPerpTradesPage> {
        let page: DeepXPerpTradesPage = self.get_json_with_query(PERP_TRADES_PATH, &query).await?;
        for trade in &page.items {
            validate_response_market_id("perp trades", market_id, trade.market_id)?;
        }
        validate_cursor_page("perp trades", page.has_next, page.next_cursor.as_deref())?;
        if page_size.is_some_and(|limit| page.items.len() > limit as usize) {
            return Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "perp trades",
                message: "page exceeds requested page size".to_string(),
            });
        }
        Ok(page)
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
        self.get_perp_trades_history_inner(request, Some(limit.get()))
            .await
    }

    async fn get_perp_trades_history_inner(
        &self,
        request: &DeepXPerpTradesHistoryRequest,
        limit: Option<usize>,
    ) -> Result<Vec<DeepXPerpTrade>> {
        request.validate()?;
        let start = jiff::Timestamp::from_millisecond(request.start_ms as i64)
            .map_err(|error| DeepXHttpError::InvalidRequest(error.to_string()))?;
        let end = jiff::Timestamp::from_millisecond(request.end_ms as i64)
            .map_err(|error| DeepXHttpError::InvalidRequest(error.to_string()))?;
        let mut pagination = CursorPagination::new(request.max_pages)?;
        let mut cursor = None;
        let mut trades = Vec::new();
        let mut ids = HashSet::new();
        let mut previous = None;
        let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse {
            endpoint: "perp trades",
            message,
        };
        loop {
            let mut page_request = request.clone();
            if let Some(limit) = limit {
                page_request.page_size = request
                    .page_size
                    .min(u32::try_from(limit - trades.len()).unwrap_or(u32::MAX));
            }
            let query = page_request.as_query(cursor.as_deref());
            let page = self
                .get_perp_trades_page(&query, request.market_id, Some(page_request.page_size))
                .await?;
            for trade in &page.items {
                let timestamp = trade
                    .created_at
                    .parse::<jiff::Timestamp>()
                    .map_err(|error| {
                        invalid(format!("trade {} has invalid timestamp: {error}", trade.id))
                    })?;
                if timestamp < start || timestamp > end {
                    return Err(invalid(format!(
                        "trade {} is outside the requested range",
                        trade.id
                    )));
                }
                if previous.is_some_and(|time| timestamp > time) {
                    return Err(invalid(format!(
                        "trade {} timestamp is not descending",
                        trade.id
                    )));
                }
                if !ids.insert(trade.id) {
                    return Err(invalid(format!("duplicate trade ID {}", trade.id)));
                }
                previous = Some(timestamp);
            }
            if limit.is_some_and(|limit| trades.len() + page.items.len() == limit) {
                trades.extend(page.items);
                return Ok(trades);
            }
            let decision = pagination.observe_response_page(
                "perp trades",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            trades.extend(page.items);
            match decision {
                PaginationDecision::Complete => return Ok(trades),
                PaginationDecision::Continue(next) => cursor = Some(next),
            }
        }
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
        request.validate()?;
        let page = self.get_spot_trades_page(request).await?;
        let mut ids = HashSet::new();
        let mut previous = None;
        for (index, trade) in page.items.iter().enumerate() {
            validate_spot_trade(index, trade, request, &mut previous)?;
            validate_unique_account_identity("spot trades", index, &mut ids, trade.id, "trade ID")?;
        }
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut ids = HashSet::new();
        let mut previous = None;
        let mut pages = Vec::new();
        loop {
            let page = self.get_spot_trades_page(&next_request).await?;
            for (index, trade) in page.items.iter().enumerate() {
                validate_spot_trade(index, trade, request, &mut previous)?;
                validate_unique_account_identity(
                    "spot trades",
                    index,
                    &mut ids,
                    trade.id,
                    "trade ID",
                )?;
            }
            let decision = pagination.observe_response_page(
                "spot trades",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
    }

    async fn get_spot_trades_page(
        &self,
        request: &DeepXSpotTradesRequest,
    ) -> Result<DeepXSpotTradesPage> {
        let page: DeepXSpotTradesPage = self
            .get_json_with_query(SPOT_TRADES_PATH, &request.as_query())
            .await?;
        validate_cursor_page("spot trades", page.has_next, page.next_cursor.as_deref())?;
        if request
            .page_size
            .is_some_and(|limit| page.items.len() > limit as usize)
        {
            return Err(invalid_account_response(
                "spot trades",
                "page exceeds requested page size".to_string(),
            ));
        }
        if page.total < u64::try_from(page.items.len()).unwrap_or(u64::MAX) {
            return Err(invalid_account_response(
                "spot trades",
                "page contains more records than the reported total".to_string(),
            ));
        }
        Ok(page)
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
        request.validate()?;
        let page = self
            .get_json_with_query(SPOT_CANDLES_PATH, &request.as_query())
            .await?;
        validate_spot_candle_page(&page, request)?;
        Ok(page)
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
        request.validate()?;
        let volume = self
            .get_json_with_query(SPOT_VOLUME_PATH, &request.as_query())
            .await?;
        validate_spot_volume(&volume)?;
        Ok(volume)
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
        request.validate()?;
        let price: DeepXSpotLastPrice = self
            .get_json_with_query(SPOT_LAST_PRICE_PATH, &request.as_query())
            .await?;
        if price.0.is_sign_negative() {
            return Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot last price",
                message: "price must not be negative".to_string(),
            });
        }
        Ok(price)
    }

    /// Returns one validated exact Spot order-book snapshot.
    ///
    /// The optional positive tick is server-side price aggregation. Aggregated bid and ask buckets
    /// can overlap, and server notionals can be rounded independently, so neither uncrossed-book
    /// nor exact `price * quantity` invariants are invented here. This snapshot is mutable and not
    /// connected to the framework Spot instrument or book pipeline.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request parameters, transport or venue failures, malformed
    /// responses, identity mismatches, invalid levels, duplicates, or side ordering violations.
    pub async fn get_spot_order_book(
        &self,
        request: &DeepXSpotOrderBookRequest,
    ) -> Result<DeepXSpotOrderBook> {
        request.validate()?;
        let book = self
            .get_json_with_query(SPOT_ORDER_BOOK_PATH, &request.as_query())
            .await?;
        validate_spot_order_book(&book, request)?;
        Ok(book)
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
        request.validate()?;
        let query = request.as_query();
        let page = self.get_json_with_query(PERP_CANDLES_PATH, &query).await?;
        validate_candle_page(
            "perp candles",
            &page,
            request.start_ms,
            request.end_ms,
            request.limit,
        )?;
        Ok(page)
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
        request.validate()?;
        let query = request.as_query();
        let page = self
            .get_json_with_query(PERP_MARK_PRICE_PATH, &query)
            .await?;
        validate_candle_page(
            "perp mark-price",
            &page,
            request.start_ms,
            request.end_ms,
            request.limit,
        )?;
        Ok(page)
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
        request.validate()?;
        let query = request.as_query();
        let page = self
            .get_json_with_query(PERP_ORACLE_PRICE_PATH, &query)
            .await?;
        validate_candle_page(
            "perp oracle-price",
            &page,
            request.start_ms,
            request.end_ms,
            request.limit,
        )?;
        Ok(page)
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
        request.validate()?;
        let query = request.as_query();
        self.get_json_with_query(PERP_VOLUME_PATH, &query).await
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
        request.validate()?;
        let query = request.as_query();
        self.get_json_with_query(PERP_LAST_PRICE_PATH, &query).await
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
        request.validate()?;
        let book = self
            .get_json_with_query(PERP_ORDER_BOOK_PATH, &request.as_query())
            .await?;
        validate_perp_order_book(&book, request)?;
        Ok(book)
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
        if user.len() != 42
            || !user.starts_with("0x")
            || !user.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
            || market_id == 0
            || order_id.trim().is_empty()
        {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-order-by-id requires a 20-byte hex subaccount, positive market_id, and nonempty order_id"
                    .to_string(),
            ));
        }
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct OrderQuery<'a> {
            user: &'a str,
            market_id: u64,
            oid: &'a str,
        }
        self.get_json_with_query(
            "/internal/v1/account/perp/order-by-id",
            &OrderQuery {
                user,
                market_id,
                oid: order_id,
            },
        )
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
        if order_id.is_empty()
            || !order_id.bytes().all(|value| value.is_ascii_digit())
            || order_id.parse::<u64>().is_err()
        {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-order-by-id requires a decimal u64 order_id".to_string(),
            ));
        }
        let raw = self
            .get_perp_order_by_id_raw(user, market_id, order_id)
            .await?;
        let order: DeepXPerpOrderRecord = serde_json::from_str(raw.get()).map_err(|e| {
            invalid_account_response("perp order by ID", format!("record failed decoding: {e}"))
        })?;
        validate_perp_order_record_scope("perp order by ID", 0, &order, user, Some(market_id))?;
        if order.order_id != order_id {
            return Err(invalid_account_response(
                "perp order by ID",
                "record belongs to another order ID".to_string(),
            ));
        }
        Ok(order)
    }

    /// Returns one validated typed perpetual order associated with an exact transaction hash.
    ///
    /// This mutable REST association is not proof of canonical inclusion or finality and does not
    /// construct a framework order report.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid transaction hash, transport or venue failures, malformed
    /// record fields, or a response transaction hash that differs from the request.
    pub async fn get_perp_order_by_tx(&self, tx_hash: &str) -> Result<DeepXPerpOrderRecord> {
        let expected_hash = tx_hash
            .strip_prefix("0x")
            .and_then(|value| nautilus_core::hex::decode_array::<32>(value).ok())
            .ok_or_else(|| {
                DeepXHttpError::InvalidRequest(
                    "perp-order-by-tx requires a 0x-prefixed 32-byte tx_hash".to_string(),
                )
            })?;
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct OrderByTxQuery<'a> {
            tx_hash: &'a str,
        }
        let order: DeepXPerpOrderRecord = self
            .get_json_with_query(PERP_ORDER_BY_TX_PATH, &OrderByTxQuery { tx_hash })
            .await?;
        validate_perp_order_record_scope(
            "perp order by transaction hash",
            0,
            &order,
            &order.owner,
            Some(order.market_id),
        )?;
        let received_hash = order
            .tx_hash
            .strip_prefix("0x")
            .and_then(|value| nautilus_core::hex::decode_array::<32>(value).ok())
            .ok_or_else(|| {
                invalid_account_response(
                    "perp order by transaction hash",
                    "record has an invalid transaction hash".to_string(),
                )
            })?;
        if received_hash != expected_hash {
            return Err(invalid_account_response(
                "perp order by transaction hash",
                "record belongs to another transaction hash".to_string(),
            ));
        }
        Ok(order)
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
        request.validate()?;
        self.get_json_with_query(SPOT_ORDER_BY_ID_PATH, &request.as_query())
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
        let raw = self.get_spot_order_by_id_raw(request).await?;
        let order: DeepXSpotOrderRecord = serde_json::from_str(raw.get()).map_err(|e| {
            invalid_account_response("spot order by ID", format!("record failed decoding: {e}"))
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
            return Err(invalid_account_response(
                "spot order by ID",
                "record belongs to another order ID".to_string(),
            ));
        }
        Ok(order)
    }

    /// Returns one validated typed Spot order associated with an exact transaction hash.
    ///
    /// This mutable association is not proof of canonical inclusion or finality and does not
    /// construct a framework order report.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid transaction hash, transport or venue failures, malformed
    /// record fields, or a response transaction hash that differs from the request.
    pub async fn get_spot_order_by_tx(&self, tx_hash: &str) -> Result<DeepXSpotOrderRecord> {
        let expected_hash = tx_hash
            .strip_prefix("0x")
            .and_then(|value| nautilus_core::hex::decode_array::<32>(value).ok())
            .ok_or_else(|| {
                DeepXHttpError::InvalidRequest(
                    "spot-order-by-tx requires a 0x-prefixed 32-byte tx_hash".to_string(),
                )
            })?;
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct OrderByTxQuery<'a> {
            tx_hash: &'a str,
        }
        let order: DeepXSpotOrderRecord = self
            .get_json_with_query(SPOT_ORDER_BY_TX_PATH, &OrderByTxQuery { tx_hash })
            .await?;
        validate_spot_order_record(
            "spot order by transaction hash",
            0,
            &order,
            &order.maker,
            Some(&order.pair_name),
            None,
            None,
        )?;
        let received_hash = order
            .tx_hash
            .strip_prefix("0x")
            .and_then(|value| nautilus_core::hex::decode_array::<32>(value).ok())
            .ok_or_else(|| {
                invalid_account_response(
                    "spot order by transaction hash",
                    "record has an invalid transaction hash".to_string(),
                )
            })?;
        if received_hash != expected_hash {
            return Err(invalid_account_response(
                "spot order by transaction hash",
                "record belongs to another transaction hash".to_string(),
            ));
        }
        Ok(order)
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(PERP_OPEN_ORDERS_PATH, &request.as_query())
            .await?;
        validate_cursor_page(
            "perp open orders",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut pages = Vec::new();
        loop {
            let page = self.get_perp_open_orders_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "perp open orders",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
    }

    /// Returns one validated typed page of active perpetual orders for a subaccount and market.
    ///
    /// Venue enum strings remain uninterpreted and this method does not construct framework
    /// reports. Use the raw reader when forward-compatible unknown fields must be retained.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed record fields, ownership, market or
    /// side mismatches, invalid financial values or timestamps, ordering violations, duplicates,
    /// or an oversized page.
    pub async fn get_perp_open_orders(
        &self,
        request: &DeepXPerpOpenOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXPerpOrderRecord>> {
        let page = self.get_perp_open_orders_raw(request).await?;
        let mut order_ids = HashSet::new();
        let mut previous = None;
        decode_account_page(
            "perp open orders",
            page,
            request.page_size,
            |index, order| {
                validate_perp_open_order_record(index, order, request, &mut previous)?;
                validate_unique_account_identity(
                    "perp open orders",
                    index,
                    &mut order_ids,
                    order.order_id.clone(),
                    "order ID",
                )
            },
        )
    }

    /// Returns all validated typed active-order pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector errors or a typed record validation error. No partial typed
    /// result is returned when any page or record is invalid.
    pub async fn get_perp_open_order_pages(
        &self,
        request: &DeepXPerpOpenOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpOrderRecord>>> {
        let mut order_ids = HashSet::new();
        let mut previous = None;
        self.get_perp_open_order_pages_raw(request, max_pages)
            .await?
            .into_iter()
            .map(|page| {
                decode_account_page(
                    "perp open orders",
                    page,
                    request.page_size,
                    |index, order| {
                        validate_perp_open_order_record(index, order, request, &mut previous)?;
                        validate_unique_account_identity(
                            "perp open orders",
                            index,
                            &mut order_ids,
                            order.order_id.clone(),
                            "order ID",
                        )
                    },
                )
            })
            .collect()
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(PERP_HISTORY_ORDERS_PATH, &request.as_query())
            .await?;
        validate_cursor_page(
            "perp history orders",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut pages = Vec::new();
        loop {
            let page = self.get_perp_history_orders_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "perp history orders",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
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
        let page = self.get_perp_history_orders_raw(request).await?;
        let mut order_ids = HashSet::new();
        let mut previous = None;
        decode_account_page(
            "perp history orders",
            page,
            request.page_size,
            |index, order| {
                validate_perp_history_order_record(index, order, request, &mut previous)?;
                validate_unique_account_identity(
                    "perp history orders",
                    index,
                    &mut order_ids,
                    order.order_id.clone(),
                    "order ID",
                )
            },
        )
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
        let mut order_ids = HashSet::new();
        let mut previous = None;
        self.get_perp_history_order_pages_raw(request, max_pages)
            .await?
            .into_iter()
            .map(|page| {
                decode_account_page(
                    "perp history orders",
                    page,
                    request.page_size,
                    |index, order| {
                        validate_perp_history_order_record(index, order, request, &mut previous)?;
                        validate_unique_account_identity(
                            "perp history orders",
                            index,
                            &mut order_ids,
                            order.order_id.clone(),
                            "order ID",
                        )
                    },
                )
            })
            .collect()
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(SPOT_OPEN_ORDERS_PATH, &request.as_query())
            .await?;
        validate_cursor_page(
            "spot open orders",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
    }

    /// Returns one validated typed page of active Spot orders for a subaccount and market.
    ///
    /// Venue enum strings remain uninterpreted and this method does not construct framework
    /// reports. Use the raw reader when forward-compatible unknown fields must be retained.
    ///
    /// # Errors
    ///
    /// Returns the raw reader errors or an error for malformed record fields, ownership or market
    /// mismatches, invalid financial values, timestamps, duplicates, ordering, or an oversized
    /// page.
    pub async fn get_spot_open_orders(
        &self,
        request: &DeepXSpotOpenOrdersRequest,
    ) -> Result<DeepXAccountPage<DeepXSpotOrderRecord>> {
        let page = self.get_spot_open_orders_raw(request).await?;
        let mut order_ids = HashSet::new();
        let mut previous = None;
        decode_account_page(
            "spot open orders",
            page,
            request.page_size,
            |index, order| {
                let created = validate_spot_order_record(
                    "spot open orders",
                    index,
                    order,
                    &request.subaccount,
                    request.name.as_deref(),
                    request.pair.as_deref(),
                    request.order_side,
                )?;
                validate_spot_order_sequence(
                    "spot open orders",
                    index,
                    &order.order_id,
                    created,
                    request.sort,
                    &mut previous,
                    &mut order_ids,
                )
            },
        )
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(SPOT_HISTORY_ORDERS_PATH, &request.as_query())
            .await?;
        validate_cursor_page(
            "spot history orders",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut pages = Vec::new();
        loop {
            let page = self.get_spot_history_orders_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "spot history orders",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
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
        let page = self.get_spot_history_orders_raw(request).await?;
        let mut order_ids = HashSet::new();
        let mut previous = None;
        decode_spot_order_page(
            "spot history orders",
            page,
            request,
            &mut order_ids,
            &mut previous,
        )
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
        let mut order_ids = HashSet::new();
        let mut previous = None;
        self.get_spot_history_order_pages_raw(request, max_pages)
            .await?
            .into_iter()
            .map(|page| {
                decode_spot_order_page(
                    "spot history orders",
                    page,
                    request,
                    &mut order_ids,
                    &mut previous,
                )
            })
            .collect()
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(SPOT_ACCOUNT_TRADES_PATH, &request.as_query())
            .await?;
        validate_cursor_page(
            "spot account trades",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut pages = Vec::new();
        loop {
            let page = self.get_spot_account_trades_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "spot account trades",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
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
        let page = self.get_spot_account_trades_raw(request).await?;
        let mut trade_ids = HashSet::new();
        let mut previous = None;
        decode_spot_account_trade_page(page, request, &mut trade_ids, &mut previous)
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
        let mut trade_ids = HashSet::new();
        let mut previous = None;
        self.get_spot_account_trade_pages_raw(request, max_pages)
            .await?
            .into_iter()
            .map(|page| {
                decode_spot_account_trade_page(page, request, &mut trade_ids, &mut previous)
            })
            .collect()
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
        request.validate()?;
        let page = self.get_spot_wallet_orders_page(request).await?;
        let mut order_ids = HashSet::new();
        let mut previous = HashMap::new();
        validate_spot_wallet_order_page(&page, request, &mut order_ids, &mut previous)?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut order_ids = HashSet::new();
        let mut previous = HashMap::new();
        let mut pages = Vec::new();
        loop {
            let page = self.get_spot_wallet_orders_page(&next_request).await?;
            validate_spot_wallet_order_page(&page, request, &mut order_ids, &mut previous)?;
            let item_count = page
                .markets
                .iter()
                .flat_map(|market| &market.subaccounts)
                .map(|subaccount| subaccount.orders.items.len())
                .sum();
            let decision = pagination.observe_response_page(
                "spot wallet orders",
                item_count,
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
    }

    async fn get_spot_wallet_orders_page(
        &self,
        request: &DeepXSpotWalletOrdersRequest,
    ) -> Result<DeepXSpotWalletOrdersPage> {
        let markets: Vec<DeepXSpotWalletOrderMarket> = self
            .get_json_with_query(SPOT_WALLET_ORDERS_PATH, &request.as_query())
            .await?;
        let (has_next, next_cursor) = normalize_spot_wallet_order_metadata(&markets)?;
        Ok(DeepXSpotWalletOrdersPage {
            markets,
            next_cursor,
            has_next,
        })
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
        request.validate()?;
        let page = self.get_spot_wallet_trades_page(request).await?;
        let mut trade_ids = HashSet::new();
        let mut previous = HashMap::new();
        validate_spot_wallet_trade_page(&page, request, &mut trade_ids, &mut previous)?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut trade_ids = HashSet::new();
        let mut previous = HashMap::new();
        let mut pages = Vec::new();
        loop {
            let page = self.get_spot_wallet_trades_page(&next_request).await?;
            validate_spot_wallet_trade_page(&page, request, &mut trade_ids, &mut previous)?;
            let item_count = page
                .markets
                .iter()
                .flat_map(|market| &market.subaccounts)
                .map(|subaccount| subaccount.trades.items.len())
                .sum();
            let decision = pagination.observe_response_page(
                "spot wallet trades",
                item_count,
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
    }

    async fn get_spot_wallet_trades_page(
        &self,
        request: &DeepXSpotWalletTradesRequest,
    ) -> Result<DeepXSpotWalletTradesPage> {
        let markets: Vec<DeepXSpotWalletTradeMarket> = self
            .get_json_with_query(SPOT_WALLET_TRADES_PATH, &request.as_query())
            .await?;
        let (has_next, next_cursor) = normalize_spot_wallet_trade_metadata(&markets)?;
        Ok(DeepXSpotWalletTradesPage {
            markets,
            next_cursor,
            has_next,
        })
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(PERP_ACCOUNT_TRADES_PATH, &request.as_query())
            .await?;
        validate_cursor_page(
            "perp account trades",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut pages = Vec::new();
        loop {
            let page = self.get_perp_account_trades_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "perp account trades",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
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
        let page = self.get_perp_account_trades_raw(request).await?;
        let mut trade_ids = HashSet::new();
        decode_account_page(
            "perp account trades",
            page,
            request.page_size,
            |index, trade| {
                validate_perp_account_trade_record(index, trade, request)?;
                validate_unique_account_identity(
                    "perp account trades",
                    index,
                    &mut trade_ids,
                    trade.id,
                    "trade ID",
                )
            },
        )
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
        let mut trade_ids = HashSet::new();
        self.get_perp_account_trade_pages_raw(request, max_pages)
            .await?
            .into_iter()
            .map(|page| {
                decode_account_page(
                    "perp account trades",
                    page,
                    request.page_size,
                    |index, trade| {
                        validate_perp_account_trade_record(index, trade, request)?;
                        validate_unique_account_identity(
                            "perp account trades",
                            index,
                            &mut trade_ids,
                            trade.id,
                            "trade ID",
                        )
                    },
                )
            })
            .collect()
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
        request.validate()?;
        let page = self.get_perp_wallet_orders_page(request).await?;
        let mut order_ids = HashSet::new();
        let mut previous = HashMap::new();
        validate_perp_wallet_order_page(&page, request, &mut order_ids, &mut previous)?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut order_ids = HashSet::new();
        let mut previous = HashMap::new();
        let mut pages = Vec::new();
        loop {
            let page = self.get_perp_wallet_orders_page(&next_request).await?;
            validate_perp_wallet_order_page(&page, request, &mut order_ids, &mut previous)?;
            let item_count = page
                .markets
                .iter()
                .flat_map(|market| &market.subaccounts)
                .map(|subaccount| subaccount.orders.items.len())
                .sum();
            let decision = pagination.observe_response_page(
                "perp wallet orders",
                item_count,
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
    }

    async fn get_perp_wallet_orders_page(
        &self,
        request: &DeepXPerpWalletOrdersRequest,
    ) -> Result<DeepXPerpWalletOrdersPage> {
        let markets: Vec<DeepXPerpWalletOrderMarket> = self
            .get_json_with_query(PERP_WALLET_ORDERS_PATH, &request.as_query())
            .await?;
        let mut metadata: Option<(bool, Option<String>)> = None;
        for market in &markets {
            if market.subaccounts.is_empty() {
                return Err(invalid_account_response(
                    "perp wallet orders",
                    format!("market {} has no subaccount groups", market.market_id),
                ));
            }
            for subaccount in &market.subaccounts {
                if subaccount.orders.items.is_empty() {
                    return Err(invalid_account_response(
                        "perp wallet orders",
                        format!(
                            "market {} subaccount {} has an empty order group",
                            market.market_id, subaccount.subaccount
                        ),
                    ));
                }
                validate_cursor_page(
                    "perp wallet orders",
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
                    return Err(invalid_account_response(
                        "perp wallet orders",
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
        request.validate()?;
        let groups: Vec<DeepXPerpWalletTradeMarket> = self
            .get_json_with_query(PERP_WALLET_TRADES_PATH, &request.as_query())
            .await?;
        let mut markets = HashSet::new();
        let mut trade_ids = HashSet::new();
        for group in &groups {
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
                return Err(invalid_account_response(
                    "perp wallet trades",
                    "response has an invalid, duplicate, or unexpected market group".to_string(),
                ));
            }
            if group.subaccounts.is_empty() {
                return Err(invalid_account_response(
                    "perp wallet trades",
                    format!("market {} has no subaccount groups", group.market_id),
                ));
            }
            let mut subaccounts = HashSet::new();
            for subaccount_group in &group.subaccounts {
                validate_account_address(
                    "perp wallet trades",
                    "subaccount",
                    &subaccount_group.subaccount,
                )?;
                if !subaccounts.insert(subaccount_group.subaccount.to_ascii_lowercase()) {
                    return Err(invalid_account_response(
                        "perp wallet trades",
                        format!(
                            "market {} contains duplicate subaccount {}",
                            group.market_id, subaccount_group.subaccount
                        ),
                    ));
                }
                validate_cursor_page(
                    "perp wallet trades",
                    subaccount_group.trades.has_next,
                    subaccount_group.trades.next_cursor.as_deref(),
                )?;
                if request
                    .page_size
                    .is_some_and(|limit| subaccount_group.trades.items.len() > limit as usize)
                {
                    return Err(invalid_account_response(
                        "perp wallet trades",
                        format!(
                            "market {} subaccount {} exceeds requested page size",
                            group.market_id, subaccount_group.subaccount
                        ),
                    ));
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
                    validate_unique_account_identity(
                        "perp wallet trades",
                        index,
                        &mut trade_ids,
                        trade.id,
                        "trade ID",
                    )?;
                }
            }
        }
        Ok(groups)
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(PERP_FUNDING_FEE_PATH, &request.as_query())
            .await?;
        validate_cursor_page(
            "perp funding fees",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut pages = Vec::new();
        loop {
            let page = self.get_perp_funding_fees_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "perp funding fees",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
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
        let page = self.get_perp_funding_fees_raw(request).await?;
        let mut event_ids = HashSet::new();
        decode_account_page(
            "perp funding fees",
            page,
            request.page_size,
            |index, fee| {
                validate_perp_funding_fee_record(index, fee, request)?;
                validate_unique_account_identity(
                    "perp funding fees",
                    index,
                    &mut event_ids,
                    (fee.height, fee.event_idx),
                    "event identity",
                )
            },
        )
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
        let mut event_ids = HashSet::new();
        self.get_perp_funding_fee_pages_raw(request, max_pages)
            .await?
            .into_iter()
            .map(|page| {
                decode_account_page(
                    "perp funding fees",
                    page,
                    request.page_size,
                    |index, fee| {
                        validate_perp_funding_fee_record(index, fee, request)?;
                        validate_unique_account_identity(
                            "perp funding fees",
                            index,
                            &mut event_ids,
                            (fee.height, fee.event_idx),
                            "event identity",
                        )
                    },
                )
            })
            .collect()
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(WALLET_FUNDING_FEE_PATH, &request.as_query())
            .await?;
        validate_cursor_page(
            "wallet funding fees",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut pages = Vec::new();
        loop {
            let page = self.get_wallet_funding_fees_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "wallet funding fees",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
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
        let page = self.get_wallet_funding_fees_raw(request).await?;
        let mut event_ids = HashSet::new();
        decode_account_page(
            "wallet funding fees",
            page,
            request.page_size,
            |index, fee| {
                validate_wallet_funding_fee_record(index, fee, request)?;
                validate_unique_account_identity(
                    "wallet funding fees",
                    index,
                    &mut event_ids,
                    (fee.height, fee.event_idx),
                    "event identity",
                )
            },
        )
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
        let mut event_ids = HashSet::new();
        self.get_wallet_funding_fee_pages_raw(request, max_pages)
            .await?
            .into_iter()
            .map(|page| {
                decode_account_page(
                    "wallet funding fees",
                    page,
                    request.page_size,
                    |index, fee| {
                        validate_wallet_funding_fee_record(index, fee, request)?;
                        validate_unique_account_identity(
                            "wallet funding fees",
                            index,
                            &mut event_ids,
                            (fee.height, fee.event_idx),
                            "event identity",
                        )
                    },
                )
            })
            .collect()
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
        request.validate()?;
        let records: Vec<DeepXHourlyUnsettledFundingRecord> = self
            .get_json_with_query(HOURLY_UNSETTLED_FUNDING_PATH, &request.as_query())
            .await?;
        validate_hourly_unsettled_funding_page(&records, request)?;
        Ok(records)
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
        request.validate()?;
        let initial_cursor = request.cursor.as_ref().map(hourly_funding_cursor_token);
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, initial_cursor.as_deref())?;
        let mut next_request = request.clone();
        let page_size = request.page_size.unwrap_or(20) as usize;
        let mut event_ids = HashSet::new();
        let mut pages = Vec::new();
        loop {
            let page = self.get_hourly_unsettled_funding(&next_request).await?;
            for (index, record) in page.iter().enumerate() {
                if !event_ids.insert(record.boundary_event_id.clone()) {
                    return Err(invalid_account_response(
                        "hourly unsettled funding",
                        format!("item {index} repeats a boundary event ID across pages"),
                    ));
                }
            }
            let next_cursor = page.last().map(hourly_funding_cursor);
            let cursor_token = next_cursor.as_ref().map(hourly_funding_cursor_token);
            let decision = pagination.observe_page(
                page.len(),
                (page.len() == page_size)
                    .then_some(cursor_token.as_deref())
                    .flatten(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(_) => {
                    next_request.cursor = next_cursor;
                }
            }
        }
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(PERP_POSITIONS_PATH, &request.as_query())
            .await?;
        validate_cursor_page("perp positions", page.has_next, page.next_cursor.as_deref())?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut pages = Vec::new();
        loop {
            let page = self.get_perp_positions_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "perp positions",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
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
        let page = self.get_perp_positions_raw(request).await?;
        let mut position_ids = HashSet::new();
        decode_account_page(
            "perp positions",
            page,
            request.page_size,
            |index, position| {
                validate_perp_position_record(index, position, request)?;
                validate_unique_account_identity(
                    "perp positions",
                    index,
                    &mut position_ids,
                    position.id,
                    "position ID",
                )
            },
        )
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
        let mut position_ids = HashSet::new();
        self.get_perp_position_pages_raw(request, max_pages)
            .await?
            .into_iter()
            .map(|page| {
                decode_account_page(
                    "perp positions",
                    page,
                    request.page_size,
                    |index, position| {
                        validate_perp_position_record(index, position, request)?;
                        validate_unique_account_identity(
                            "perp positions",
                            index,
                            &mut position_ids,
                            position.id,
                            "position ID",
                        )
                    },
                )
            })
            .collect()
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
        validate_account_address("subaccount balances", "subaccount", subaccount)?;
        #[derive(Serialize)]
        struct BalancesQuery<'a> {
            subaccount: &'a str,
        }
        self.get_json_with_query(SUBACCOUNT_BALANCES_PATH, &BalancesQuery { subaccount })
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(BALANCE_CHANGES_PATH, &request.as_query())
            .await?;
        validate_cursor_page(
            "balance changes",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut pages = Vec::new();
        loop {
            let page = self.get_balance_changes_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "balance changes",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
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
        let page = self.get_balance_changes_raw(request).await?;
        let mut ids = HashSet::new();
        decode_account_page(
            "balance changes",
            page,
            request.page_size,
            |index, record| {
                validate_balance_change_record(index, record, request)?;
                validate_unique_account_identity(
                    "balance changes",
                    index,
                    &mut ids,
                    record.id,
                    "record ID",
                )
            },
        )
    }

    /// Returns all validated typed balance-change pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector or typed-record errors. Duplicate identities across pages fail
    /// without returning a partial typed collection.
    pub async fn get_balance_change_pages(
        &self,
        request: &DeepXBalanceChangesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXBalanceChangeRecord>>> {
        let mut ids = HashSet::new();
        self.get_balance_change_pages_raw(request, max_pages)
            .await?
            .into_iter()
            .map(|page| {
                decode_account_page(
                    "balance changes",
                    page,
                    request.page_size,
                    |index, record| {
                        validate_balance_change_record(index, record, request)?;
                        validate_unique_account_identity(
                            "balance changes",
                            index,
                            &mut ids,
                            record.id,
                            "record ID",
                        )
                    },
                )
            })
            .collect()
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(LIQUIDATION_RECORDS_PATH, &request.as_query())
            .await?;
        validate_cursor_page(
            "liquidation records",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut pages = Vec::new();
        loop {
            let page = self.get_liquidation_records_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "liquidation records",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
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
        let page = self.get_liquidation_records_raw(request).await?;
        let mut ids = HashSet::new();
        let mut previous = None;
        decode_account_page(
            "liquidation records",
            page,
            request.page_size,
            |index, record| {
                validate_liquidation_record(index, record, request, &mut previous)?;
                validate_unique_account_identity(
                    "liquidation records",
                    index,
                    &mut ids,
                    record.id,
                    "record ID",
                )
            },
        )
    }

    /// Returns all validated liquidation-record pages within an explicit page budget.
    ///
    /// # Errors
    ///
    /// Returns the raw collector or typed-record errors. Duplicate identities or ordering
    /// violations across page boundaries fail without returning a partial typed collection.
    pub async fn get_liquidation_record_pages(
        &self,
        request: &DeepXLiquidationRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXLiquidationRecord>>> {
        let mut ids = HashSet::new();
        let mut previous = None;
        self.get_liquidation_record_pages_raw(request, max_pages)
            .await?
            .into_iter()
            .map(|page| {
                decode_account_page(
                    "liquidation records",
                    page,
                    request.page_size,
                    |index, record| {
                        validate_liquidation_record(index, record, request, &mut previous)?;
                        validate_unique_account_identity(
                            "liquidation records",
                            index,
                            &mut ids,
                            record.id,
                            "record ID",
                        )
                    },
                )
            })
            .collect()
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
        request.validate()?;
        let price: DeepXPerpLiquidationPrice = self
            .get_json_with_query(PERP_LIQUIDATION_PRICE_PATH, &request.as_query())
            .await?;
        validate_perp_liquidation_price(&price, request)?;
        Ok(price)
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
        validate_account_address("user stats", "wallet", address)?;
        #[derive(Serialize)]
        struct UserStatsQuery<'a> {
            address: &'a str,
        }
        let stats: DeepXUserStats = self
            .get_json_with_query(USER_STATS_PATH, &UserStatsQuery { address })
            .await?;
        validate_user_stats(&stats)?;
        Ok(stats)
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
        validate_account_address("quota summary", "wallet", wallet)?;
        #[derive(Serialize)]
        struct QuotaSummaryQuery<'a> {
            wallet: &'a str,
        }
        let summary: DeepXQuotaSummary = self
            .get_json_with_query(QUOTA_SUMMARY_PATH, &QuotaSummaryQuery { wallet })
            .await?;
        validate_quota_summary(&summary, wallet)?;
        Ok(summary)
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(QUOTA_HISTORY_PATH, &request.as_query())
            .await?;
        validate_cursor_page("quota history", page.has_next, page.next_cursor.as_deref())?;
        Ok(page)
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
        let page = self.get_quota_history_raw(request).await?;
        let mut identities = QuotaHistoryIdentities::default();
        let mut previous = None;
        decode_quota_history_page(page, request, &mut identities, &mut previous)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut identities = QuotaHistoryIdentities::default();
        let mut previous = None;
        let mut pages = Vec::new();
        loop {
            let raw = self.get_quota_history_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "quota history",
                raw.items.len(),
                raw.has_next,
                raw.next_cursor.as_deref(),
            )?;
            pages.push(decode_quota_history_page(
                raw,
                request,
                &mut identities,
                &mut previous,
            )?);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
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
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_json_with_query(ALL_SUBACCOUNTS_PATH, &request.as_query())
            .await?;
        validate_cursor_page(
            "all subaccounts",
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
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
        request.validate()?;
        let mut pagination =
            CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref())?;
        let mut next_request = request.clone();
        let mut pages = Vec::new();
        loop {
            let page = self.get_all_subaccounts_raw(&next_request).await?;
            let decision = pagination.observe_response_page(
                "all subaccounts",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )?;
            pages.push(page);
            match decision {
                PaginationDecision::Complete => return Ok(pages),
                PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
            }
        }
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
        let page = self.get_all_subaccounts_raw(request).await?;
        let mut subaccounts = HashSet::new();
        let mut previous = None;
        decode_all_subaccounts_page(page, request, &mut subaccounts, &mut previous)
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
        let mut subaccounts = HashSet::new();
        let mut previous = None;
        self.get_all_subaccount_pages_raw(request, max_pages)
            .await?
            .into_iter()
            .map(|page| decode_all_subaccounts_page(page, request, &mut subaccounts, &mut previous))
            .collect()
    }

    /// Returns the validated subaccounts owned by a wallet address.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid wallet address, transport or HTTP failures, malformed
    /// responses, venue-level failures, invalid returned addresses, or duplicate subaccounts.
    pub async fn get_wallet_subaccounts(&self, address: &str) -> Result<DeepXWalletSubaccounts> {
        validate_account_address("wallet subaccounts", "wallet", address)?;
        #[derive(Serialize)]
        struct SubaccountsQuery<'a> {
            address: &'a str,
        }
        let addresses: Vec<String> = self
            .get_json_with_query(WALLET_SUBACCOUNTS_PATH, &SubaccountsQuery { address })
            .await?;
        let subaccounts = DeepXWalletSubaccounts::new(address.to_string(), addresses);
        validate_wallet_subaccounts(&subaccounts)?;
        Ok(subaccounts)
    }

    /// Returns validated profile, balance, and equity reads for every wallet subaccount.
    ///
    /// Subaccounts retain directory order. Each profile must identify the requested wallet as its
    /// authority, and every balance and equity payload must identify the corresponding subaccount.
    /// No partial collection is returned if any request or validation fails. The REST responses
    /// are not block-pinned, so this does not assert cross-request venue snapshot atomicity.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid wallet, directory failure, or any failed or inconsistent
    /// subaccount profile, balance, or equity response.
    pub async fn get_wallet_account_snapshot(
        &self,
        address: &str,
    ) -> Result<DeepXWalletAccountSnapshot> {
        let directory = self.get_wallet_subaccounts(address).await?;
        let mut subaccounts = Vec::with_capacity(directory.addresses.len());
        for subaccount in &directory.addresses {
            let profile = self.get_subaccount_info(subaccount, Some(address)).await?;
            let balances = self.get_subaccount_balances(subaccount).await?;
            let equity = self.get_subaccount_equity(subaccount).await?;
            let margin_ratio = self.get_subaccount_margin_ratio(subaccount).await?;
            subaccounts.push(DeepXSubaccountSnapshot {
                profile,
                balances,
                equity,
                margin_ratio,
            });
        }
        Ok(DeepXWalletAccountSnapshot {
            directory,
            subaccounts,
        })
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
        validate_account_address("subaccount info", "subaccount", address)?;
        if let Some(authority) = expected_authority {
            validate_account_address("subaccount info", "expected authority", authority)?;
        }
        #[derive(Serialize)]
        struct SubaccountInfoQuery<'a> {
            address: &'a str,
        }
        let profile: DeepXSubaccountProfile = self
            .get_json_with_query(SUBACCOUNT_INFO_PATH, &SubaccountInfoQuery { address })
            .await?;
        validate_subaccount_profile(&profile, address, expected_authority)?;
        Ok(profile)
    }

    /// Queries and verifies signer ownership of one configured subaccount.
    ///
    /// The wallet directory query is derived from `key`; the returned profile must bind the same
    /// wallet authority and exact configured subaccount before an opaque proof is returned.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid key or subaccount, failed REST reads, inconsistent account
    /// directory or profile identities, duplicate subaccounts, or an inactive profile.
    pub async fn get_account_ownership_proof(
        &self,
        key: &DeepXPrivateKey,
        subaccount: &str,
    ) -> std::result::Result<DeepXAccountOwnershipProof, DeepXAccountOwnershipError> {
        let signer = derive_signer_account_id(key)?;
        let wallet = format!("0x{}", nautilus_core::hex::encode(signer));
        let directory = self.get_wallet_subaccounts(&wallet).await?;
        let profile = self.get_subaccount_info(subaccount, Some(&wallet)).await?;
        verify_account_ownership(key, subaccount, &directory, &profile)
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
        validate_account_address("subaccount balances", "subaccount", subaccount)?;
        #[derive(Serialize)]
        struct BalancesQuery<'a> {
            subaccount: &'a str,
        }
        let balances: DeepXSubaccountBalances = self
            .get_json_with_query(SUBACCOUNT_BALANCES_PATH, &BalancesQuery { subaccount })
            .await?;
        validate_subaccount_balances(&balances, subaccount)?;
        Ok(balances)
    }

    /// Returns a validated exact equity summary for a subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid subaccount address, transport or HTTP failures, malformed
    /// responses, venue-level failures, identity mismatches, or invalid nonnegative totals.
    pub async fn get_subaccount_equity(&self, address: &str) -> Result<DeepXSubaccountEquity> {
        validate_account_address("subaccount equity", "subaccount", address)?;
        #[derive(Serialize)]
        struct EquityQuery<'a> {
            address: &'a str,
        }
        let equity: DeepXSubaccountEquity = self
            .get_json_with_query(SUBACCOUNT_EQUITY_PATH, &EquityQuery { address })
            .await?;
        validate_subaccount_equity(&equity, address)?;
        Ok(equity)
    }

    /// Returns exact account-wide collateral and margin-ratio data for a subaccount.
    ///
    /// The endpoint does not identify whether its margin requirement uses initial or maintenance
    /// weights. This method therefore preserves the venue fields without constructing a Nautilus
    /// margin balance or deriving free collateral.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid subaccount address, transport or HTTP failures, malformed
    /// responses, venue-level failures, or negative financial values.
    pub async fn get_subaccount_margin_ratio(
        &self,
        address: &str,
    ) -> Result<DeepXSubaccountMarginRatio> {
        validate_account_address("subaccount margin ratio", "subaccount", address)?;
        #[derive(Serialize)]
        struct MarginRatioQuery<'a> {
            address: &'a str,
        }
        let margin_ratio: DeepXSubaccountMarginRatio = self
            .get_json_with_query(SUBACCOUNT_MARGIN_RATIO_PATH, &MarginRatioQuery { address })
            .await?;
        validate_subaccount_margin_ratio(&margin_ratio)?;
        Ok(margin_ratio)
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
        validate_account_address("delegate accounts", "wallet", address)?;
        #[derive(Serialize)]
        struct DelegateQuery<'a> {
            address: &'a str,
        }
        self.get_json_with_query(DELEGATE_ACCOUNTS_PATH, &DelegateQuery { address })
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
        validate_account_address("delegate accounts", "wallet", wallet)?;
        #[derive(Serialize)]
        struct DelegateQuery<'a> {
            address: &'a str,
        }
        let accounts: Vec<DeepXDelegateAccount> = self
            .get_json_with_query(DELEGATE_ACCOUNTS_PATH, &DelegateQuery { address: wallet })
            .await?;
        let directory = DeepXWalletDelegateAccounts::new(wallet.to_string(), accounts);
        validate_wallet_delegate_accounts(&directory)?;
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
        validate_account_address("delegator accounts", "delegate", delegate)?;
        #[derive(Serialize)]
        struct DelegatorQuery<'a> {
            address: &'a str,
        }
        let wallets: Vec<String> = self
            .get_json_with_query(
                DELEGATOR_ACCOUNTS_PATH,
                &DelegatorQuery { address: delegate },
            )
            .await?;
        let directory = DeepXDelegateWallets::new(delegate.to_string(), wallets);
        validate_delegate_wallets(&directory)?;
        Ok(directory)
    }

    async fn get_market_data<T>(&self, path: &str) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        self.get_api_json(path).await
    }

    async fn get_api_json<T>(&self, path: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        validate_path(path)?;
        let attempt = AtomicUsize::new(0);
        self.retry_manager
            .execute_with_retry(
                "DeepX public HTTP GET",
                || {
                    let index = attempt.fetch_add(1, Ordering::Relaxed) % self.base_urls.len();
                    async move {
                        let response = self
                            .get_json_once::<DeepXApiResponse<T>>(&self.base_urls[index], path)
                            .await?;
                        into_api_data(response)
                    }
                },
                should_retry_http_error,
                DeepXHttpError::from,
            )
            .await
    }

    async fn get_json_with_query<T, Q>(&self, path: &str, query: &Q) -> Result<T>
    where
        T: DeserializeOwned,
        Q: Serialize + Sync,
    {
        validate_path(path)?;
        let attempt = AtomicUsize::new(0);
        self.retry_manager
            .execute_with_retry(
                "DeepX public HTTP GET",
                || {
                    let index = attempt.fetch_add(1, Ordering::Relaxed) % self.base_urls.len();
                    async move {
                        let response = self
                            .get_json_once_with_query::<DeepXApiResponse<T>, _>(
                                &self.base_urls[index],
                                path,
                                query,
                            )
                            .await?;
                        into_api_data(response)
                    }
                },
                should_retry_http_error,
                DeepXHttpError::from,
            )
            .await
    }

    async fn get_json_once<R>(&self, base_url: &str, path: &str) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let response = self
            .client
            .get(
                format!("{base_url}{path}"),
                None,
                None,
                self.timeout_secs,
                None,
            )
            .await?;
        if !response.status.is_success() {
            return Err(DeepXHttpError::Http {
                status: response.status.as_u16(),
                message: bounded_body(&response.body),
            });
        }
        Ok(serde_json::from_slice(&response.body)?)
    }

    async fn get_json_once_with_query<R, Q>(
        &self,
        base_url: &str,
        path: &str,
        query: &Q,
    ) -> Result<R>
    where
        R: DeserializeOwned,
        Q: Serialize,
    {
        let response = self
            .client
            .request_with_params(
                Method::GET,
                format!("{base_url}{path}"),
                Some(query),
                None,
                None,
                self.timeout_secs,
                None,
            )
            .await?;
        if !response.status.is_success() {
            return Err(DeepXHttpError::Http {
                status: response.status.as_u16(),
                message: bounded_body(&response.body),
            });
        }
        Ok(serde_json::from_slice(&response.body)?)
    }
}

fn decode_account_page<T, F>(
    endpoint: &'static str,
    page: DeepXRawAccountPage,
    page_size: Option<u32>,
    mut validate: F,
) -> Result<DeepXAccountPage<T>>
where
    T: DeserializeOwned,
    F: FnMut(usize, &T) -> Result<()>,
{
    if page_size.is_some_and(|limit| page.items.len() > limit as usize) {
        return Err(invalid_account_response(
            endpoint,
            "page exceeds requested page size".to_string(),
        ));
    }
    let mut items = Vec::with_capacity(page.items.len());
    for (index, raw) in page.items.into_iter().enumerate() {
        let item = serde_json::from_str(raw.get()).map_err(|e| {
            invalid_account_response(endpoint, format!("item {index} failed decoding: {e}"))
        })?;
        validate(index, &item)?;
        items.push(item);
    }
    Ok(DeepXAccountPage {
        items,
        next_cursor: page.next_cursor,
        has_next: page.has_next,
    })
}

fn decode_spot_order_page(
    endpoint: &'static str,
    page: DeepXRawAccountPage,
    request: &DeepXSpotHistoryOrdersRequest,
    order_ids: &mut HashSet<String>,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<DeepXAccountPage<DeepXSpotOrderRecord>> {
    decode_account_page(endpoint, page, request.page_size, |index, order| {
        let created = validate_spot_order_record(
            endpoint,
            index,
            order,
            &request.subaccount,
            request.name.as_deref(),
            request.pair.as_deref(),
            request.order_side,
        )?;
        validate_spot_order_sequence(
            endpoint,
            index,
            &order.order_id,
            created,
            request.sort,
            previous,
            order_ids,
        )
    })
}

fn decode_spot_account_trade_page(
    page: DeepXRawAccountPage,
    request: &DeepXSpotAccountTradesRequest,
    trade_ids: &mut HashSet<u64>,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<DeepXAccountPage<DeepXSpotAccountTradeRecord>> {
    decode_account_page(
        "spot account trades",
        page,
        request.page_size,
        |index, trade| {
            validate_spot_account_trade_record(index, trade, request, previous)?;
            validate_unique_account_identity(
                "spot account trades",
                index,
                trade_ids,
                trade.id,
                "trade ID",
            )
        },
    )
}

fn decode_all_subaccounts_page(
    page: DeepXRawAccountPage,
    request: &DeepXAllSubaccountsRequest,
    subaccounts: &mut HashSet<String>,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<DeepXAccountPage<DeepXSubaccountDirectoryRecord>> {
    decode_account_page(
        "all subaccounts",
        page,
        request.page_size,
        |index, record| {
            let created = validate_subaccount_directory_record(index, record)?;
            if previous
                .as_ref()
                .is_some_and(|previous| created > *previous)
            {
                return Err(invalid_account_response(
                    "all subaccounts",
                    format!("item {index} violates descending creation-time order"),
                ));
            }
            *previous = Some(created);
            validate_unique_account_identity(
                "all subaccounts",
                index,
                subaccounts,
                record.subaccount.to_ascii_lowercase(),
                "subaccount identity",
            )
        },
    )
}

fn decode_quota_history_page(
    page: DeepXRawAccountPage,
    request: &DeepXQuotaHistoryRequest,
    identities: &mut QuotaHistoryIdentities,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<DeepXAccountPage<DeepXQuotaHistoryRecord>> {
    decode_account_page("quota history", page, request.limit, |index, record| {
        let created = validate_quota_history_record(index, record, request)?;
        if previous
            .as_ref()
            .is_some_and(|previous| created > *previous)
        {
            return Err(invalid_account_response(
                "quota history",
                format!("item {index} violates descending creation-time order"),
            ));
        }
        *previous = Some(created);
        validate_unique_account_identity(
            "quota history",
            index,
            &mut identities.ids,
            record.id.clone(),
            "quota history identity",
        )?;
        validate_unique_account_identity(
            "quota history",
            index,
            &mut identities.events,
            (record.block_number, record.event_index),
            "quota chain event identity",
        )
    })
}

#[derive(Default)]
struct QuotaHistoryIdentities {
    ids: HashSet<String>,
    events: HashSet<(u64, u64)>,
}

fn validate_quota_history_record(
    index: usize,
    record: &DeepXQuotaHistoryRecord,
    request: &DeepXQuotaHistoryRequest,
) -> Result<jiff::Timestamp> {
    let endpoint = "quota history";
    let invalid = |message| invalid_account_response(endpoint, message);
    if record.id.trim().is_empty()
        || validate_account_address(endpoint, "owner", &record.owner_address).is_err()
        || !record.owner_address.eq_ignore_ascii_case(&request.wallet)
        || request
            .history_type
            .is_some_and(|expected| record.history_type != expected)
    {
        return Err(invalid(format!(
            "item {index} has invalid scope or identity"
        )));
    }
    match (
        record.history_type,
        record.buyer_type,
        &record.buyer_address,
    ) {
        (DeepXQuotaHistoryType::Purchase, Some(_), Some(buyer))
            if validate_account_address(endpoint, "buyer", buyer).is_ok()
                && request
                    .buyer_address
                    .as_deref()
                    .is_none_or(|expected| buyer.eq_ignore_ascii_case(expected)) => {}
        (DeepXQuotaHistoryType::Activate | DeepXQuotaHistoryType::Free, None, None)
            if request.buyer_address.is_none() => {}
        _ => return Err(invalid(format!("item {index} has invalid buyer metadata"))),
    }
    if record.quota == 0
        || record.block_number == 0
        || record.tx_hash_type.trim().is_empty()
        || record
            .tx_hash
            .strip_prefix("0x")
            .and_then(|value| nautilus_core::hex::decode_array::<32>(value).ok())
            .is_none()
    {
        return Err(invalid(format!("item {index} has invalid chain metadata")));
    }
    let created = parse_account_timestamp(endpoint, index, "createdAt", &record.created_at)?;
    u64::try_from(created.as_millisecond())
        .map_err(|_| invalid(format!("item {index} timestamp is before the Unix epoch")))?;
    Ok(created)
}

fn validate_subaccount_directory_record(
    index: usize,
    record: &DeepXSubaccountDirectoryRecord,
) -> Result<jiff::Timestamp> {
    let endpoint = "all subaccounts";
    let invalid = |message| invalid_account_response(endpoint, message);
    if validate_account_address(endpoint, "owner", &record.owner).is_err()
        || validate_account_address(endpoint, "subaccount", &record.subaccount).is_err()
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
    let created = parse_account_timestamp(endpoint, index, "createdAt", &record.created_at)?;
    u64::try_from(created.as_millisecond())
        .map_err(|_| invalid(format!("item {index} timestamp is before the Unix epoch")))?;
    Ok(created)
}

fn validate_perp_order_record(
    index: usize,
    order: &DeepXPerpOrderRecord,
    request: &DeepXPerpHistoryOrdersRequest,
) -> Result<()> {
    validate_perp_order_record_scope(
        "perp history orders",
        index,
        order,
        &request.subaccount,
        request.market_id,
    )
}

fn validate_perp_history_order_record(
    index: usize,
    order: &DeepXPerpOrderRecord,
    request: &DeepXPerpHistoryOrdersRequest,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<()> {
    validate_perp_order_record(index, order, request)?;
    validate_perp_order_time_order("perp history orders", index, order, request.sort, previous)
}

fn validate_perp_open_order_record(
    index: usize,
    order: &DeepXPerpOrderRecord,
    request: &DeepXPerpOpenOrdersRequest,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<()> {
    let endpoint = "perp open orders";
    validate_perp_order_record_scope(
        endpoint,
        index,
        order,
        &request.subaccount,
        request.market_id,
    )?;
    if request
        .is_long
        .is_some_and(|is_long| order.is_long != is_long)
    {
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} does not match the requested side"),
        ));
    }
    validate_perp_order_time_order(endpoint, index, order, request.sort, previous)
}

fn validate_perp_order_time_order(
    endpoint: &'static str,
    index: usize,
    order: &DeepXPerpOrderRecord,
    sort: DeepXAccountSortOrder,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<()> {
    let created = parse_account_timestamp(endpoint, index, "createTime", &order.create_time)?;
    let out_of_order = previous.is_some_and(|previous| match sort {
        DeepXAccountSortOrder::Ascending => created < previous,
        DeepXAccountSortOrder::Descending => created > previous,
    });
    if out_of_order {
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} violates the requested time order"),
        ));
    }
    *previous = Some(created);
    Ok(())
}

fn validate_spot_order_record(
    endpoint: &'static str,
    index: usize,
    order: &DeepXSpotOrderRecord,
    requested_owner: &str,
    requested_name: Option<&str>,
    requested_pair: Option<&str>,
    requested_side: Option<DeepXSpotOrderSide>,
) -> Result<jiff::Timestamp> {
    let invalid = |message| invalid_account_response(endpoint, message);
    if validate_account_address(endpoint, "maker", &order.maker).is_err()
        || !order.maker.eq_ignore_ascii_case(requested_owner)
    {
        return Err(invalid(format!(
            "item {index} belongs to another subaccount"
        )));
    }
    let pair = decode_bytes32_identity(&order.pair)
        .ok_or_else(|| invalid(format!("item {index} has an invalid pair identity")))?;
    if requested_pair
        .and_then(decode_bytes32_identity)
        .is_some_and(|expected| pair != expected)
        || order.pair_name.trim().is_empty()
        || requested_name.is_some_and(|expected| order.pair_name != expected)
    {
        return Err(invalid(format!(
            "item {index} belongs to another Spot market"
        )));
    }
    validate_decimal_order_id(endpoint, index, &order.order_id)?;
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
        .and_then(|value| nautilus_core::hex::decode_array::<32>(value).ok())
        .is_none()
    {
        return Err(invalid(format!(
            "item {index} has an invalid transaction hash"
        )));
    }
    let created = parse_account_timestamp(endpoint, index, "createTime", &order.create_time)?;
    u64::try_from(created.as_millisecond())
        .map_err(|_| invalid(format!("item {index} timestamp is before the Unix epoch")))?;
    Ok(created)
}

fn validate_spot_order_sequence(
    endpoint: &'static str,
    index: usize,
    order_id: &str,
    created: jiff::Timestamp,
    sort: DeepXAccountSortOrder,
    previous: &mut Option<jiff::Timestamp>,
    order_ids: &mut HashSet<String>,
) -> Result<()> {
    let invalid = |message| invalid_account_response(endpoint, message);
    let out_of_order = previous.as_ref().is_some_and(|previous| match sort {
        DeepXAccountSortOrder::Ascending => created < *previous,
        DeepXAccountSortOrder::Descending => created > *previous,
    });
    if out_of_order {
        return Err(invalid(format!(
            "item {index} violates the requested time order"
        )));
    }
    validate_unique_account_identity(endpoint, index, order_ids, order_id.to_string(), "order ID")?;
    *previous = Some(created);
    Ok(())
}

fn validate_spot_account_trade_record(
    index: usize,
    trade: &DeepXSpotAccountTradeRecord,
    request: &DeepXSpotAccountTradesRequest,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<()> {
    let endpoint = "spot account trades";
    let invalid = |message| invalid_account_response(endpoint, message);
    if trade.id == 0 {
        return Err(invalid(format!("item {index} has an invalid trade ID")));
    }
    validate_decimal_order_id(endpoint, index, &trade.order_id)?;
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
    let pair = decode_bytes32_identity(&trade.pair)
        .ok_or_else(|| invalid(format!("item {index} has an invalid pair identity")))?;
    if request
        .pair
        .as_deref()
        .and_then(decode_bytes32_identity)
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
    let created = parse_account_timestamp(endpoint, index, "createdAt", &trade.created_at)?;
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

fn normalize_spot_wallet_order_metadata(
    markets: &[DeepXSpotWalletOrderMarket],
) -> Result<(bool, Option<String>)> {
    let endpoint = "spot wallet orders";
    let mut metadata: Option<(bool, Option<String>)> = None;
    for subaccount in markets.iter().flat_map(|market| &market.subaccounts) {
        let page = &subaccount.orders;
        if page.items.is_empty() {
            if page.has_next
                || page
                    .next_cursor
                    .as_deref()
                    .is_some_and(|cursor| !cursor.is_empty())
            {
                return Err(invalid_account_response(
                    endpoint,
                    format!(
                        "subaccount {} has an empty nonterminal order group",
                        subaccount.subaccount
                    ),
                ));
            }
            continue;
        }
        validate_cursor_page(endpoint, page.has_next, page.next_cursor.as_deref())?;
        let observed = (page.has_next, page.next_cursor.clone());
        if metadata
            .as_ref()
            .is_some_and(|expected| expected != &observed)
        {
            return Err(invalid_account_response(
                endpoint,
                "nonempty groups disagree on global pagination metadata".to_string(),
            ));
        }
        metadata.get_or_insert(observed);
    }
    Ok(metadata.unwrap_or((false, None)))
}

fn normalize_spot_wallet_trade_metadata(
    markets: &[DeepXSpotWalletTradeMarket],
) -> Result<(bool, Option<String>)> {
    let endpoint = "spot wallet trades";
    let mut metadata: Option<(bool, Option<String>)> = None;
    for subaccount in markets.iter().flat_map(|market| &market.subaccounts) {
        let page = &subaccount.trades;
        if page.items.is_empty() {
            if page.has_next
                || page
                    .next_cursor
                    .as_deref()
                    .is_some_and(|cursor| !cursor.is_empty())
            {
                return Err(invalid_account_response(
                    endpoint,
                    format!(
                        "subaccount {} has an empty nonterminal trade group",
                        subaccount.subaccount
                    ),
                ));
            }
            continue;
        }
        validate_cursor_page(endpoint, page.has_next, page.next_cursor.as_deref())?;
        let observed = (page.has_next, page.next_cursor.clone());
        if metadata
            .as_ref()
            .is_some_and(|expected| expected != &observed)
        {
            return Err(invalid_account_response(
                endpoint,
                "nonempty groups disagree on global pagination metadata".to_string(),
            ));
        }
        metadata.get_or_insert(observed);
    }
    Ok(metadata.unwrap_or((false, None)))
}

fn validate_spot_wallet_order_page(
    page: &DeepXSpotWalletOrdersPage,
    request: &DeepXSpotWalletOrdersRequest,
    order_ids: &mut HashSet<(String, [u8; 32], String, String)>,
    previous: &mut HashMap<([u8; 32], String), jiff::Timestamp>,
) -> Result<()> {
    let endpoint = "spot wallet orders";
    let invalid = |message| invalid_account_response(endpoint, message);
    let mut markets = HashSet::new();
    let mut item_count = 0_usize;
    for market in &page.markets {
        let pair = decode_bytes32_identity(&market.pair)
            .ok_or_else(|| invalid("response has an invalid market pair".to_string()))?;
        if market.name.trim().is_empty()
            || !markets.insert(pair)
            || request
                .pair
                .as_deref()
                .and_then(decode_bytes32_identity)
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
            validate_account_address(endpoint, "subaccount", &subaccount.subaccount)?;
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
                let out_of_order =
                    previous
                        .get(&order_key)
                        .is_some_and(|previous| match request.sort {
                            DeepXAccountSortOrder::Ascending => created < *previous,
                            DeepXAccountSortOrder::Descending => created > *previous,
                        });
                if out_of_order {
                    return Err(invalid(format!(
                        "item {index} violates its group time order"
                    )));
                }
                previous.insert(order_key.clone(), created);
                validate_unique_account_identity(
                    endpoint,
                    index,
                    order_ids,
                    (
                        normalized_subaccount.clone(),
                        pair,
                        order.order_id.clone(),
                        order.order_side.clone(),
                    ),
                    "wallet order identity",
                )?;
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

fn validate_spot_wallet_trade_page(
    page: &DeepXSpotWalletTradesPage,
    request: &DeepXSpotWalletTradesRequest,
    trade_ids: &mut HashSet<u64>,
    previous: &mut HashMap<([u8; 32], String), jiff::Timestamp>,
) -> Result<()> {
    let endpoint = "spot wallet trades";
    let invalid = |message| invalid_account_response(endpoint, message);
    let mut markets = HashSet::new();
    let mut item_count = 0_usize;
    for market in &page.markets {
        let pair = decode_bytes32_identity(&market.pair)
            .ok_or_else(|| invalid("response has an invalid market pair".to_string()))?;
        if market.name.trim().is_empty()
            || !markets.insert(pair)
            || request
                .pair
                .as_deref()
                .and_then(decode_bytes32_identity)
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
            validate_account_address(endpoint, "subaccount", &subaccount.subaccount)?;
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
                validate_unique_account_identity(endpoint, index, trade_ids, trade.id, "trade ID")?;
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

fn validate_spot_wallet_trade_record(
    index: usize,
    trade: &DeepXSpotAccountTradeRecord,
    request: &DeepXSpotWalletTradesRequest,
    group_name: &str,
    group_pair: [u8; 32],
    previous: Option<&jiff::Timestamp>,
) -> Result<jiff::Timestamp> {
    let endpoint = "spot wallet trades";
    let invalid = |message| invalid_account_response(endpoint, message);
    if trade.id == 0 {
        return Err(invalid(format!("item {index} has an invalid trade ID")));
    }
    validate_decimal_order_id(endpoint, index, &trade.order_id)?;
    if !matches!(trade.order_side.as_str(), "Buy" | "Sell") {
        return Err(invalid(format!("item {index} has an invalid order side")));
    }
    let pair = decode_bytes32_identity(&trade.pair)
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
    let created = parse_account_timestamp(endpoint, index, "createdAt", &trade.created_at)?;
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

fn validate_perp_order_record_scope(
    endpoint: &'static str,
    index: usize,
    order: &DeepXPerpOrderRecord,
    requested_owner: &str,
    requested_market_id: Option<u64>,
) -> Result<()> {
    validate_perp_order_record_scope_with_size_policy(
        endpoint,
        index,
        order,
        requested_owner,
        requested_market_id,
        false,
    )
}

fn validate_perp_wallet_order_record_scope(
    endpoint: &'static str,
    index: usize,
    order: &DeepXPerpOrderRecord,
    requested_owner: &str,
    requested_market_id: Option<u64>,
) -> Result<()> {
    validate_perp_order_record_scope_with_size_policy(
        endpoint,
        index,
        order,
        requested_owner,
        requested_market_id,
        true,
    )
}

fn validate_perp_order_record_scope_with_size_policy(
    endpoint: &'static str,
    index: usize,
    order: &DeepXPerpOrderRecord,
    requested_owner: &str,
    requested_market_id: Option<u64>,
    allow_zero_size: bool,
) -> Result<()> {
    validate_account_record_scope(
        endpoint,
        index,
        &order.owner,
        order.market_id,
        requested_owner,
        requested_market_id,
    )?;
    validate_decimal_order_id(endpoint, index, &order.order_id)?;
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
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} has invalid financial values"),
        ));
    }
    if order.order_type.trim().is_empty()
        || order.status.trim().is_empty()
        || order.post_only.trim().is_empty()
        || order.tx_hash_type.trim().is_empty()
    {
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} has an empty venue enum value"),
        ));
    }
    if order
        .tx_hash
        .strip_prefix("0x")
        .and_then(|value| nautilus_core::hex::decode_array::<32>(value).ok())
        .is_none()
    {
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} has an invalid transaction hash"),
        ));
    }
    let created = parse_account_timestamp(endpoint, index, "createTime", &order.create_time)?;
    if let Some(updated_time) = &order.updated_time {
        let updated = parse_account_timestamp(endpoint, index, "updatedTime", updated_time)?;
        if updated < created {
            return Err(invalid_account_response(
                endpoint,
                format!("item {index} update timestamp precedes creation"),
            ));
        }
    }
    Ok(())
}

fn validate_perp_wallet_order_page(
    page: &DeepXPerpWalletOrdersPage,
    request: &DeepXPerpWalletOrdersRequest,
    order_ids: &mut HashSet<(String, u64, String, bool)>,
    previous: &mut HashMap<(u64, String), jiff::Timestamp>,
) -> Result<()> {
    let endpoint = "perp wallet orders";
    let invalid = |message| invalid_account_response(endpoint, message);
    let mut markets = HashSet::new();
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
            validate_account_address(endpoint, "subaccount", &subaccount.subaccount)?;
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
                let created =
                    parse_account_timestamp(endpoint, index, "createTime", &order.create_time)?;
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
                let out_of_order =
                    previous
                        .get(&order_key)
                        .is_some_and(|previous| match request.sort {
                            DeepXAccountSortOrder::Ascending => created < *previous,
                            DeepXAccountSortOrder::Descending => created > *previous,
                        });
                if out_of_order {
                    return Err(invalid(format!(
                        "item {index} violates its group time order"
                    )));
                }
                previous.insert(order_key.clone(), created);
                validate_unique_account_identity(
                    endpoint,
                    index,
                    order_ids,
                    (
                        normalized_subaccount.clone(),
                        market.market_id,
                        order.order_id.clone(),
                        order.is_long,
                    ),
                    "wallet order identity",
                )?;
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

fn validate_perp_account_trade_record(
    index: usize,
    trade: &DeepXPerpAccountTradeRecord,
    request: &DeepXPerpAccountTradesRequest,
) -> Result<()> {
    if trade.market_id == 0
        || request
            .market_id
            .is_some_and(|market_id| trade.market_id != market_id)
    {
        return Err(invalid_account_response(
            "perp account trades",
            format!("item {index} has an unexpected market ID"),
        ));
    }
    validate_decimal_order_id("perp account trades", index, &trade.order_id)?;
    if request
        .order_id
        .as_ref()
        .is_some_and(|order_id| trade.order_id != *order_id)
        || request
            .is_long
            .is_some_and(|is_long| trade.is_long != is_long)
    {
        return Err(invalid_account_response(
            "perp account trades",
            format!("item {index} does not match the requested order filter"),
        ));
    }
    if trade.price <= Decimal::ZERO
        || trade.size <= Decimal::ZERO
        || trade.leverage <= Decimal::ZERO
    {
        return Err(invalid_account_response(
            "perp account trades",
            format!("item {index} has invalid financial values"),
        ));
    }
    if trade.taker.trim().is_empty() || trade.filled_direction.trim().is_empty() {
        return Err(invalid_account_response(
            "perp account trades",
            format!("item {index} has an empty venue enum value"),
        ));
    }
    let created =
        parse_account_timestamp("perp account trades", index, "createdAt", &trade.created_at)?;
    let created_ms = u64::try_from(created.as_millisecond()).map_err(|_| {
        invalid_account_response(
            "perp account trades",
            format!("item {index} timestamp is before the Unix epoch"),
        )
    })?;
    if request.start_ms.is_some_and(|start| created_ms < start)
        || request.end_ms.is_some_and(|end| created_ms > end)
    {
        return Err(invalid_account_response(
            "perp account trades",
            format!("item {index} is outside the requested time range"),
        ));
    }
    Ok(())
}

fn validate_perp_wallet_trade_record(
    index: usize,
    trade: &DeepXPerpAccountTradeRecord,
    request: &DeepXPerpWalletTradesRequest,
    group_market_id: u64,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<()> {
    let endpoint = "perp wallet trades";
    let invalid = |message| invalid_account_response(endpoint, message);
    if trade.id == 0 || trade.market_id != group_market_id {
        return Err(invalid(format!(
            "item {index} has an invalid trade ID or mismatched market ID"
        )));
    }
    validate_decimal_order_id(endpoint, index, &trade.order_id)?;
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
    let timestamp = parse_account_timestamp(endpoint, index, "createdAt", &trade.created_at)?;
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

fn validate_spot_trade(
    index: usize,
    trade: &DeepXSpotTrade,
    request: &DeepXSpotTradesRequest,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<()> {
    let endpoint = "spot trades";
    let invalid = |message| invalid_account_response(endpoint, message);
    if trade.id == 0 || trade.height == 0 {
        return Err(invalid(format!(
            "item {index} has an invalid trade ID or block height"
        )));
    }
    validate_decimal_order_id(endpoint, index, &trade.sell_id)?;
    validate_decimal_order_id(endpoint, index, &trade.buy_id)?;
    if validate_account_address(endpoint, "seller", &trade.seller).is_err()
        || validate_account_address(endpoint, "buyer", &trade.buyer).is_err()
    {
        return Err(invalid(format!(
            "item {index} has an invalid counterparty identity"
        )));
    }
    let valid_pair = trade.pair.len() == 66
        && trade.pair.starts_with("0x")
        && trade.pair.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit);
    if trade.pair_name.trim().is_empty()
        || !valid_pair
        || request
            .name
            .as_deref()
            .is_some_and(|expected| trade.pair_name != expected)
        || request
            .pair
            .as_deref()
            .is_some_and(|expected| !trade.pair.eq_ignore_ascii_case(expected))
    {
        return Err(invalid(format!(
            "item {index} has an invalid or unexpected market identity"
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
    let timestamp = parse_account_timestamp(endpoint, index, "tradeTime", &trade.trade_time)?;
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

fn validate_perp_funding_fee_record(
    index: usize,
    fee: &DeepXPerpFundingFeeRecord,
    request: &DeepXPerpFundingFeeRequest,
) -> Result<()> {
    validate_funding_fee_record(
        "perp funding fees",
        index,
        fee,
        Some(&request.subaccount),
        request.market_id,
        request.start_ms,
        request.end_ms,
    )
}

fn validate_wallet_funding_fee_record(
    index: usize,
    fee: &DeepXPerpFundingFeeRecord,
    request: &DeepXWalletFundingFeeRequest,
) -> Result<()> {
    validate_funding_fee_record(
        "wallet funding fees",
        index,
        fee,
        None,
        request.market_id,
        request.start_ms,
        request.end_ms,
    )
}

fn validate_funding_fee_record(
    endpoint: &'static str,
    index: usize,
    fee: &DeepXPerpFundingFeeRecord,
    expected_owner: Option<&str>,
    expected_market_id: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
) -> Result<()> {
    if validate_account_address(endpoint, "owner", &fee.owner).is_err()
        || expected_owner.is_some_and(|owner| !fee.owner.eq_ignore_ascii_case(owner))
    {
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} has an invalid or unexpected owner"),
        ));
    }
    if fee.market == 0 || expected_market_id.is_some_and(|market| fee.market != market) {
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} has an unexpected market ID"),
        ));
    }
    if fee.position_size <= Decimal::ZERO || fee.height == 0 {
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} has invalid financial or event values"),
        ));
    }
    let created = parse_account_timestamp(endpoint, index, "createdAt", &fee.created_at)?;
    let created_ms = u64::try_from(created.as_millisecond()).map_err(|_| {
        invalid_account_response(
            endpoint,
            format!("item {index} timestamp is before the Unix epoch"),
        )
    })?;
    if start_ms.is_some_and(|start| created_ms < start)
        || end_ms.is_some_and(|end| created_ms > end)
    {
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} is outside the requested time range"),
        ));
    }
    Ok(())
}

fn validate_hourly_unsettled_funding_page(
    records: &[DeepXHourlyUnsettledFundingRecord],
    request: &DeepXHourlyUnsettledFundingRequest,
) -> Result<()> {
    let endpoint = "hourly unsettled funding";
    if records.len() > request.page_size.unwrap_or(20) as usize {
        return Err(invalid_account_response(
            endpoint,
            "response exceeds the requested page size".to_string(),
        ));
    }
    let mut event_ids = HashSet::new();
    let mut previous = request.cursor.as_ref().map(hourly_funding_cursor_key);
    for (index, record) in records.iter().enumerate() {
        if validate_account_address(endpoint, "subaccount", &record.subaccount).is_err()
            || request
                .subaccount
                .as_deref()
                .is_some_and(|value| !record.subaccount.eq_ignore_ascii_case(value))
        {
            return Err(invalid_account_response(
                endpoint,
                format!("item {index} has an invalid or unexpected subaccount"),
            ));
        }
        if record.market_id == 0
            || request
                .market_id
                .is_some_and(|value| record.market_id != value)
        {
            return Err(invalid_account_response(
                endpoint,
                format!("item {index} has an unexpected market ID"),
            ));
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
            return Err(invalid_account_response(
                endpoint,
                format!("item {index} has invalid raw financial or boundary values"),
            ));
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
            return Err(invalid_account_response(
                endpoint,
                format!("item {index} has an invalid or out-of-range boundary timestamp"),
            ));
        }
        if !event_ids.insert(record.boundary_event_id.clone()) {
            return Err(invalid_account_response(
                endpoint,
                format!("item {index} repeats a boundary event ID"),
            ));
        }
        let key = hourly_funding_record_key(record);
        if previous
            .as_ref()
            .is_some_and(|previous| match request.sort {
                DeepXAccountSortOrder::Ascending => &key <= previous,
                DeepXAccountSortOrder::Descending => &key >= previous,
            })
        {
            return Err(invalid_account_response(
                endpoint,
                format!("item {index} violates the requested keyset order"),
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

fn hourly_funding_record_key(
    record: &DeepXHourlyUnsettledFundingRecord,
) -> (u64, u64, String, String) {
    (
        record.boundary_timestamp_ms,
        record.market_id,
        record.subaccount.to_ascii_lowercase(),
        record.boundary_event_id.clone(),
    )
}

fn hourly_funding_cursor_key(
    cursor: &DeepXHourlyUnsettledFundingCursor,
) -> (u64, u64, String, String) {
    (
        cursor.boundary_timestamp_ms,
        cursor.market_id,
        cursor.subaccount.to_ascii_lowercase(),
        cursor.event_id.clone(),
    )
}

fn hourly_funding_cursor(
    record: &DeepXHourlyUnsettledFundingRecord,
) -> DeepXHourlyUnsettledFundingCursor {
    DeepXHourlyUnsettledFundingCursor {
        boundary_timestamp_ms: record.boundary_timestamp_ms,
        market_id: record.market_id,
        subaccount: record.subaccount.clone(),
        event_id: record.boundary_event_id.clone(),
    }
}

fn hourly_funding_cursor_token(cursor: &DeepXHourlyUnsettledFundingCursor) -> String {
    format!(
        "{}:{}:{}:{}",
        cursor.boundary_timestamp_ms,
        cursor.market_id,
        cursor.subaccount.to_ascii_lowercase(),
        cursor.event_id,
    )
}

fn validate_perp_position_record(
    index: usize,
    position: &DeepXPerpPositionRecord,
    request: &DeepXPerpPositionsRequest,
) -> Result<()> {
    validate_account_record_scope(
        "perp positions",
        index,
        &position.owner,
        position.market_id,
        &request.subaccount,
        request.market_id,
    )?;
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
        return Err(invalid_account_response(
            "perp positions",
            format!("item {index} has invalid financial values"),
        ));
    }
    if position.status.trim().is_empty()
        || request.only_closed == Some(true) && position.status != "Closed"
    {
        return Err(invalid_account_response(
            "perp positions",
            format!("item {index} does not match the requested lifecycle filter"),
        ));
    }
    let opened = parse_account_timestamp("perp positions", index, "openTime", &position.open_time)?;
    let created =
        parse_account_timestamp("perp positions", index, "createdAt", &position.created_at)?;
    let updated =
        parse_account_timestamp("perp positions", index, "updatedAt", &position.updated_at)?;
    if updated < created {
        return Err(invalid_account_response(
            "perp positions",
            format!("item {index} update timestamp precedes creation"),
        ));
    }
    if let Some(closed_at) = &position.close_time {
        let closed = parse_account_timestamp("perp positions", index, "closeTime", closed_at)?;
        if closed < opened {
            return Err(invalid_account_response(
                "perp positions",
                format!("item {index} close timestamp precedes opening"),
            ));
        }
    }
    Ok(())
}

fn validate_account_record_scope(
    endpoint: &'static str,
    index: usize,
    owner: &str,
    market_id: u64,
    requested_owner: &str,
    requested_market_id: Option<u64>,
) -> Result<()> {
    if !owner.eq_ignore_ascii_case(requested_owner) {
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} belongs to another subaccount"),
        ));
    }
    if market_id == 0 || requested_market_id.is_some_and(|expected| market_id != expected) {
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} has an unexpected market ID"),
        ));
    }
    Ok(())
}

fn validate_decimal_order_id(endpoint: &'static str, index: usize, order_id: &str) -> Result<()> {
    if order_id.is_empty()
        || !order_id.bytes().all(|value| value.is_ascii_digit())
        || order_id.parse::<u64>().is_err()
    {
        return Err(invalid_account_response(
            endpoint,
            format!("item {index} has an invalid decimal order ID"),
        ));
    }
    Ok(())
}

fn validate_unique_account_identity<T>(
    endpoint: &'static str,
    index: usize,
    identities: &mut HashSet<T>,
    identity: T,
    name: &str,
) -> Result<()>
where
    T: std::hash::Hash + Eq,
{
    if identities.insert(identity) {
        return Ok(());
    }
    Err(invalid_account_response(
        endpoint,
        format!("item {index} repeats {name}"),
    ))
}

fn parse_account_timestamp(
    endpoint: &'static str,
    index: usize,
    field: &str,
    value: &str,
) -> Result<jiff::Timestamp> {
    value.parse::<jiff::Timestamp>().map_err(|e| {
        invalid_account_response(endpoint, format!("item {index} has invalid {field}: {e}"))
    })
}

fn validate_account_address(endpoint: &str, role: &str, address: &str) -> Result<()> {
    if address.len() == 42
        && address.starts_with("0x")
        && address.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
    {
        return Ok(());
    }
    Err(DeepXHttpError::InvalidRequest(format!(
        "{endpoint} requires a 20-byte hex {role} address"
    )))
}

fn parse_bytes32_identity(endpoint: &str, role: &str, identity: &str) -> Result<[u8; 32]> {
    decode_bytes32_identity(identity).ok_or_else(|| {
        DeepXHttpError::InvalidRequest(format!("{endpoint} requires a 32-byte hex {role} identity"))
    })
}

fn decode_bytes32_identity(identity: &str) -> Option<[u8; 32]> {
    nautilus_core::hex::decode_array::<32>(identity.strip_prefix("0x").unwrap_or(identity)).ok()
}

fn validate_wallet_subaccounts(subaccounts: &DeepXWalletSubaccounts) -> Result<()> {
    let mut addresses = HashSet::new();
    for (index, address) in subaccounts.addresses.iter().enumerate() {
        if validate_account_address("wallet subaccounts", "subaccount", address).is_err() {
            return Err(invalid_account_state_response(
                "wallet subaccounts",
                format!("item {index} is not a 20-byte hex subaccount address"),
            ));
        }
        if !addresses.insert(address.to_ascii_lowercase()) {
            return Err(invalid_account_state_response(
                "wallet subaccounts",
                format!("item {index} repeats a subaccount address"),
            ));
        }
    }
    Ok(())
}

fn validate_wallet_delegate_accounts(directory: &DeepXWalletDelegateAccounts) -> Result<()> {
    let endpoint = "delegate accounts";
    let mut addresses = HashSet::new();
    for (index, account) in directory.accounts.iter().enumerate() {
        if validate_account_address(endpoint, "delegate", &account.delegate_address).is_err() {
            return Err(invalid_account_state_response(
                endpoint,
                format!("item {index} has an invalid delegate address"),
            ));
        }
        if !addresses.insert(account.delegate_address.to_ascii_lowercase()) {
            return Err(invalid_account_state_response(
                endpoint,
                format!("item {index} repeats a delegate address"),
            ));
        }
        if account.valid_until != 0 && account.valid_until <= account.create_time {
            return Err(invalid_account_state_response(
                endpoint,
                format!("item {index} expiry does not follow its creation timestamp"),
            ));
        }
    }
    Ok(())
}

fn validate_delegate_wallets(directory: &DeepXDelegateWallets) -> Result<()> {
    let endpoint = "delegator accounts";
    let mut wallets = HashSet::new();
    for (index, wallet) in directory.wallets.iter().enumerate() {
        if validate_account_address(endpoint, "wallet", wallet).is_err() {
            return Err(invalid_account_state_response(
                endpoint,
                format!("item {index} has an invalid wallet address"),
            ));
        }
        if !wallets.insert(wallet.to_ascii_lowercase()) {
            return Err(invalid_account_state_response(
                endpoint,
                format!("item {index} repeats a wallet address"),
            ));
        }
    }
    Ok(())
}

fn validate_user_stats(stats: &DeepXUserStats) -> Result<()> {
    let expected_count = u64::try_from(stats.subaccounts.len()).map_err(|e| {
        invalid_account_state_response("user stats", format!("subaccount count overflow: {e}"))
    })?;
    if stats.if_staked_quote_asset_amount < Decimal::ZERO {
        return Err(invalid_account_state_response(
            "user stats",
            "Insurance Fund staked quote amount is negative".to_string(),
        ));
    }
    if stats.number_of_sub_accounts != expected_count
        || stats.number_of_sub_accounts_created < stats.number_of_sub_accounts
    {
        return Err(invalid_account_state_response(
            "user stats",
            "subaccount counters are inconsistent".to_string(),
        ));
    }
    let mut subaccounts = HashSet::new();
    for (index, address) in stats.subaccounts.iter().enumerate() {
        if validate_account_address("user stats", "subaccount", address).is_err() {
            return Err(invalid_account_state_response(
                "user stats",
                format!("item {index} is not a 20-byte hex subaccount address"),
            ));
        }
        if !subaccounts.insert(address.to_ascii_lowercase()) {
            return Err(invalid_account_state_response(
                "user stats",
                format!("item {index} repeats a subaccount address"),
            ));
        }
    }
    Ok(())
}

fn validate_quota_summary(summary: &DeepXQuotaSummary, wallet: &str) -> Result<()> {
    let endpoint = "quota summary";
    let invalid = |message| invalid_account_state_response(endpoint, message);
    if validate_account_address(endpoint, "owner", &summary.owner).is_err()
        || !summary.owner.eq_ignore_ascii_case(wallet)
    {
        return Err(invalid(
            "response owner does not match the requested wallet".to_string(),
        ));
    }
    if summary.spot_volume_usd < Decimal::ZERO
        || summary.perp_volume_usd < Decimal::ZERO
        || summary.total_volume_usd < Decimal::ZERO
        || summary.spot_volume_usd.checked_add(summary.perp_volume_usd)
            != Some(summary.total_volume_usd)
    {
        return Err(invalid(
            "USD volume aggregates are inconsistent".to_string(),
        ));
    }
    let parse_bound =
        |name: &str, value: Option<&str>, milliseconds: Option<u64>| match (value, milliseconds) {
            (None, None) => Ok(None),
            (Some(value), Some(milliseconds)) => {
                let timestamp = value
                    .parse::<jiff::Timestamp>()
                    .map_err(|e| invalid(format!("{name} timestamp is not valid RFC 3339: {e}")))?;
                if u64::try_from(timestamp.as_millisecond()).ok() != Some(milliseconds) {
                    return Err(invalid(format!(
                        "{name} timestamp representations do not match"
                    )));
                }
                Ok(Some(milliseconds))
            }
            _ => Err(invalid(format!(
                "{name} timestamp representations are incomplete"
            ))),
        };
    let first = parse_bound(
        "first trade",
        summary.first_trade_at.as_deref(),
        summary.first_trade_ts_ms,
    )?;
    let last = parse_bound(
        "last trade",
        summary.last_trade_at.as_deref(),
        summary.last_trade_ts_ms,
    )?;
    if first.is_some() != last.is_some()
        || first.zip(last).is_some_and(|(first, last)| first > last)
    {
        return Err(invalid(
            "trade timestamp bounds are inconsistent".to_string(),
        ));
    }
    if let Some(updated_at) = &summary.updated_at {
        updated_at
            .parse::<jiff::Timestamp>()
            .map_err(|e| invalid(format!("updatedAt timestamp is not valid RFC 3339: {e}")))?;
    }
    Ok(())
}

fn validate_perp_liquidation_price(
    price: &DeepXPerpLiquidationPrice,
    request: &DeepXPerpLiquidationPriceRequest,
) -> Result<()> {
    let invalid = |message| invalid_account_state_response("perp liquidation price", message);
    if validate_account_address("perp liquidation price", "subaccount", &price.address).is_err()
        || !price.address.eq_ignore_ascii_case(&request.subaccount)
    {
        return Err(invalid(
            "response belongs to another or invalid subaccount".to_string(),
        ));
    }
    if price.market_id == 0
        || request
            .market_id
            .is_some_and(|market_id| price.market_id != market_id)
        || price.market_name.trim().is_empty()
        || request
            .market_name
            .as_deref()
            .is_some_and(|name| price.market_name != name)
    {
        return Err(invalid(
            "response has an unexpected market identity".to_string(),
        ));
    }
    if price
        .liquidate_price
        .is_some_and(|value| value <= Decimal::ZERO)
    {
        return Err(invalid(
            "response has a nonpositive liquidation price".to_string(),
        ));
    }
    Ok(())
}

fn validate_liquidation_record(
    index: usize,
    record: &DeepXLiquidationRecord,
    request: &DeepXLiquidationRecordsRequest,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<()> {
    let endpoint = "liquidation records";
    let invalid = |message| invalid_account_response(endpoint, message);
    if record.id == 0 || record.height == 0 {
        return Err(invalid(format!(
            "item {index} has an invalid record ID or block height"
        )));
    }
    if validate_account_address(endpoint, "target subaccount", &record.target_account).is_err()
        || validate_account_address(endpoint, "liquidator", &record.liquidator).is_err()
        || request
            .subaccount
            .as_deref()
            .is_some_and(|expected| !record.target_account.eq_ignore_ascii_case(expected))
    {
        return Err(invalid(format!(
            "item {index} has an invalid or unexpected account identity"
        )));
    }
    if !request.liquidation_types.is_empty()
        && !request.liquidation_types.contains(&record.liquidation_type)
    {
        return Err(invalid(format!(
            "item {index} does not match the requested liquidation types"
        )));
    }
    if record.market_index == Some(0) {
        return Err(invalid(format!("item {index} has an invalid market index")));
    }
    if record.liquidation_type.is_bankruptcy() && !record.bankrupt {
        return Err(invalid(format!(
            "item {index} has an inconsistent bankruptcy classification"
        )));
    }
    if record.tx_hash.as_deref().is_some_and(|value| {
        value
            .strip_prefix("0x")
            .and_then(|hex| nautilus_core::hex::decode_array::<32>(hex).ok())
            .is_none()
    }) {
        return Err(invalid(format!(
            "item {index} has an invalid transaction hash"
        )));
    }
    let detail: serde_json::Value =
        serde_json::from_str(&record.liquidation_detail).map_err(|e| {
            invalid(format!(
                "item {index} has invalid liquidation detail JSON: {e}"
            ))
        })?;
    let detail = detail
        .as_object()
        .ok_or_else(|| invalid(format!("item {index} liquidation detail is not an object")))?;
    if detail.len() != 1 || !detail.contains_key(record.liquidation_type.detail_key()) {
        return Err(invalid(format!(
            "item {index} liquidation detail does not match its type"
        )));
    }
    if serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(
        &record.canceled_order_ids,
    )
    .is_err()
    {
        return Err(invalid(format!(
            "item {index} has invalid canceled-order JSON"
        )));
    }
    let created = parse_account_timestamp(endpoint, index, "createdAt", &record.created_at)?;
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

fn validate_balance_change_record(
    index: usize,
    record: &DeepXBalanceChangeRecord,
    request: &DeepXBalanceChangesRequest,
) -> Result<()> {
    let invalid = |message| invalid_account_response("balance changes", message);
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
            && validate_account_address("balance changes", role, address).is_err()
        {
            return Err(invalid(format!(
                "item {index} has an invalid {role} address"
            )));
        }
    }
    if let Some(position) = &record.position
        && (position.id == 0
            || position.market_id == 0
            || validate_account_address("balance changes", "position owner", &position.owner)
                .is_err()
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

fn validate_subaccount_profile(
    profile: &DeepXSubaccountProfile,
    requested_address: &str,
    expected_authority: Option<&str>,
) -> Result<()> {
    if !profile.address.eq_ignore_ascii_case(requested_address) {
        return Err(invalid_account_state_response(
            "subaccount info",
            "profile belongs to another subaccount".to_string(),
        ));
    }
    if validate_account_address("subaccount info", "authority", &profile.authority).is_err() {
        return Err(invalid_account_state_response(
            "subaccount info",
            "profile has an invalid authority address".to_string(),
        ));
    }
    if expected_authority.is_some_and(|expected| !profile.authority.eq_ignore_ascii_case(expected))
    {
        return Err(invalid_account_state_response(
            "subaccount info",
            "profile authority does not match the expected wallet".to_string(),
        ));
    }
    if !matches!(
        profile.status.as_str(),
        "Active" | "BeingLiquidated" | "Closed" | "Bankrupt"
    ) {
        return Err(invalid_account_state_response(
            "subaccount info",
            "profile has an unknown account status".to_string(),
        ));
    }
    if !matches!(profile.margin_strategy.as_str(), "Cross" | "Isolate") {
        return Err(invalid_account_state_response(
            "subaccount info",
            "profile has an unknown margin strategy".to_string(),
        ));
    }
    if profile.height == 0
        || i64::try_from(profile.created_at)
            .ok()
            .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
            .is_none()
    {
        return Err(invalid_account_state_response(
            "subaccount info",
            "profile has an invalid height or creation timestamp".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_subaccount_balances(
    balances: &DeepXSubaccountBalances,
    requested_address: &str,
) -> Result<()> {
    if !balances.address.eq_ignore_ascii_case(requested_address) {
        return Err(invalid_account_state_response(
            "subaccount balances",
            "balances belong to another subaccount".to_string(),
        ));
    }
    let mut symbols = HashSet::new();
    for (index, asset) in balances.assets.iter().enumerate() {
        let symbol = asset.symbol.trim();
        if symbol.is_empty() || !symbols.insert(symbol.to_ascii_uppercase()) {
            return Err(invalid_account_state_response(
                "subaccount balances",
                format!("asset {index} has an empty or duplicate symbol"),
            ));
        }
        if asset.decimals > 28
            || asset.price.is_sign_negative()
            || asset.balance.is_sign_negative()
            || asset.balance_usd.is_sign_negative()
            || asset.balance_borrowed.is_sign_negative()
            || asset.balance_borrowed_usd.is_sign_negative()
            || asset.borrow_interest.is_sign_negative()
            || asset.borrow_interest_usd.is_sign_negative()
        {
            return Err(invalid_account_state_response(
                "subaccount balances",
                format!("asset {index} has invalid precision or financial values"),
            ));
        }
    }
    Ok(())
}

fn validate_subaccount_equity(
    equity: &DeepXSubaccountEquity,
    requested_address: &str,
) -> Result<()> {
    if !equity.subaccount.eq_ignore_ascii_case(requested_address) {
        return Err(invalid_account_state_response(
            "subaccount equity",
            "equity belongs to another subaccount".to_string(),
        ));
    }
    if equity.total_deposits_usd.is_sign_negative() || equity.total_borrows_usd.is_sign_negative() {
        return Err(invalid_account_state_response(
            "subaccount equity",
            "equity has a negative deposit or borrow total".to_string(),
        ));
    }
    Ok(())
}

fn validate_subaccount_margin_ratio(margin: &DeepXSubaccountMarginRatio) -> Result<()> {
    if margin.collateral.is_sign_negative()
        || margin.margin_required.is_sign_negative()
        || margin
            .margin_ratio
            .is_some_and(|ratio| ratio.is_sign_negative())
    {
        return Err(invalid_account_state_response(
            "subaccount margin ratio",
            "margin ratio response has negative financial values".to_string(),
        ));
    }
    Ok(())
}

fn invalid_account_response(endpoint: &'static str, message: String) -> DeepXHttpError {
    DeepXHttpError::InvalidHistoryResponse { endpoint, message }
}

fn invalid_account_state_response(endpoint: &'static str, message: String) -> DeepXHttpError {
    DeepXHttpError::InvalidAccountResponse { endpoint, message }
}

fn into_api_data<T>(response: DeepXApiResponse<T>) -> Result<T> {
    if response.fail || !response.code.is_success() {
        return Err(DeepXHttpError::Api {
            code: response.code,
            message: response.msg,
        });
    }
    Ok(response.data)
}

fn validate_candle_page(
    endpoint: &'static str,
    page: &DeepXPerpCandlesPage,
    start_ms: u64,
    end_ms: Option<u64>,
    limit: Option<u32>,
) -> Result<()> {
    let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    if page.pair.trim().is_empty() {
        return Err(invalid("missing pair identity".to_string()));
    }
    if page.details.len() > 5_000 {
        return Err(invalid("page exceeds venue limit of 5000".to_string()));
    }
    if limit.is_some_and(|limit| page.details.len() > limit as usize) {
        return Err(invalid("page exceeds requested limit".to_string()));
    }
    let mut previous = None;
    for (index, candle) in page.details.iter().enumerate() {
        if candle.time < start_ms || end_ms.is_some_and(|end_ms| candle.time > end_ms) {
            return Err(invalid(format!(
                "candle {index} timestamp is outside the requested bounds"
            )));
        }
        if previous.is_some_and(|time| candle.time <= time) {
            return Err(invalid(format!(
                "candle {index} timestamp is not strictly ascending"
            )));
        }
        if candle.low > candle.high
            || candle.open < candle.low
            || candle.open > candle.high
            || candle.close < candle.low
            || candle.close > candle.high
        {
            return Err(invalid(format!(
                "candle {index} has inconsistent OHLC values"
            )));
        }
        if candle.volume.is_sign_negative() && !candle.volume.is_zero() {
            return Err(invalid(format!("candle {index} has negative volume")));
        }
        previous = Some(candle.time);
    }
    Ok(())
}

fn validate_spot_candle_page(
    page: &DeepXSpotCandlesPage,
    request: &DeepXSpotCandlesRequest,
) -> Result<()> {
    let endpoint = "spot candles";
    let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    if page.pair.trim().is_empty() || request.name.as_ref().is_some_and(|name| page.pair != *name) {
        return Err(invalid(
            "response has a missing or unexpected market name".to_string(),
        ));
    }
    if page.details.len() > 5_000
        || request
            .limit
            .is_some_and(|limit| page.details.len() > limit as usize)
    {
        return Err(invalid("response exceeds its candle limit".to_string()));
    }
    let mut previous = None;
    for (index, candle) in page.details.iter().enumerate() {
        if candle.time < request.start_ms || request.end_ms.is_some_and(|end| candle.time > end) {
            return Err(invalid(format!(
                "candle {index} timestamp is outside the requested bounds"
            )));
        }
        if previous.is_some_and(|time| candle.time <= time) {
            return Err(invalid(format!(
                "candle {index} timestamp is not strictly ascending"
            )));
        }
        if candle.low <= Decimal::ZERO
            || candle.low > candle.high
            || candle.open < candle.low
            || candle.open > candle.high
            || candle.close < candle.low
            || candle.close > candle.high
            || candle.volume.is_sign_negative()
        {
            return Err(invalid(format!("candle {index} has invalid OHLCV values")));
        }
        previous = Some(candle.time);
    }
    Ok(())
}

fn validate_spot_markets(markets: &[DeepXSpotMarket]) -> Result<()> {
    let endpoint = "spot markets";
    let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    let mut names = HashSet::new();
    let mut pairs = HashSet::new();
    for (index, market) in markets.iter().enumerate() {
        validate_spot_market(endpoint, market)?;
        if !names.insert(market.name.to_ascii_lowercase()) {
            return Err(invalid(format!("market {index} repeats a market name")));
        }
        let pair = market.pair.strip_prefix("0x").unwrap_or(&market.pair);
        if !pairs.insert(pair.to_ascii_lowercase()) {
            return Err(invalid(format!("market {index} repeats a pair identity")));
        }
    }
    Ok(())
}

fn validate_spot_market(endpoint: &'static str, market: &DeepXSpotMarket) -> Result<()> {
    let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    let mut name_parts = market.name.split('/');
    let name_base = name_parts.next().unwrap_or_default();
    let name_quote = name_parts.next().unwrap_or_default();
    if name_base.is_empty()
        || name_quote.is_empty()
        || name_parts.next().is_some()
        || market.base_symbol.trim().is_empty()
        || market.quote_symbol.trim().is_empty()
        || !name_base.eq_ignore_ascii_case(&market.base_symbol)
        || !name_quote.eq_ignore_ascii_case(&market.quote_symbol)
        || decode_bytes32_identity(&market.pair).is_none()
        || validate_account_address(endpoint, "base asset", &market.base_address).is_err()
        || validate_account_address(endpoint, "quote asset", &market.quote_address).is_err()
        || market
            .base_address
            .eq_ignore_ascii_case(&market.quote_address)
        || market
            .base_symbol
            .eq_ignore_ascii_case(&market.quote_symbol)
    {
        return Err(invalid("market has invalid identity metadata".to_string()));
    }
    if market.price.is_sign_negative()
        || market.tick_size <= Decimal::ZERO
        || market.max_deviation_bps.is_sign_negative()
        || market.limit_order_guard_limit_long <= Decimal::ZERO
        || market.limit_order_guard_limit_short <= Decimal::ZERO
    {
        return Err(invalid("market has invalid financial metadata".to_string()));
    }
    Ok(())
}

fn validate_perp_market_lookup(
    endpoint: &'static str,
    market: &DeepXPerpMarketLookup,
) -> Result<()> {
    let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    let mut name_parts = market.name.split('-');
    let name_base = name_parts.next().unwrap_or_default();
    let name_quote = name_parts.next().unwrap_or_default();
    if market.id == 0
        || market.quote_market_id == 0
        || market.id == market.quote_market_id
        || name_base.is_empty()
        || name_quote.is_empty()
        || name_parts.next().is_some()
        || market.base_symbol.trim().is_empty()
        || market.quote_symbol.trim().is_empty()
        || !name_base.eq_ignore_ascii_case(&market.base_symbol)
        || !name_quote.eq_ignore_ascii_case(&market.quote_symbol)
        || validate_account_address(endpoint, "base asset", &market.base_address).is_err()
        || validate_account_address(endpoint, "quote asset", &market.quote_address).is_err()
        || market
            .base_address
            .eq_ignore_ascii_case(&market.quote_address)
        || market
            .base_symbol
            .eq_ignore_ascii_case(&market.quote_symbol)
    {
        return Err(invalid("market has invalid identity metadata".to_string()));
    }

    let has_deployer_metadata = market.deployer_delegate.is_some()
        || market.deployer_fee_recipient.is_some()
        || market.deployer_builder_fee_bps.is_some()
        || market.deployer_isolated_margin_only.is_some();
    if market
        .deployer
        .as_deref()
        .is_some_and(|value| validate_account_address(endpoint, "deployer", value).is_err())
        || market.deployer.is_none() && has_deployer_metadata
        || market.deployer_delegate.as_deref().is_some_and(|value| {
            validate_account_address(endpoint, "deployer delegate", value).is_err()
        })
        || market
            .deployer_fee_recipient
            .as_deref()
            .is_some_and(|value| {
                validate_account_address(endpoint, "deployer fee recipient", value).is_err()
            })
        || market
            .deployer_builder_fee_bps
            .is_some_and(|value| value.is_sign_negative())
    {
        return Err(invalid("market has invalid deployer metadata".to_string()));
    }

    if market.oracle_price <= Decimal::ZERO
        || market.mark_price <= Decimal::ZERO
        || market.max_deviation_bps.is_sign_negative()
        || market.initial_margin_ratio <= Decimal::ZERO
        || market.initial_margin_ratio > Decimal::ONE
        || market.maintenance_margin_ratio.is_sign_negative()
        || market.maintenance_margin_ratio > market.initial_margin_ratio
        || market.order_spec_min_qty <= Decimal::ZERO
        || market.order_spec_min_notional <= Decimal::ZERO
        || market.limit_order_guard_limit_long <= Decimal::ZERO
        || market.limit_order_guard_limit_short <= Decimal::ZERO
        || market.impact_margin_value <= Decimal::ZERO
        || market.funding_rate_clamp_lower_bound > market.funding_rate_clamp_upper_bound
        || market.liquidation_duration == 0
        || market.liquidity_bucket_slippage_step == 0
        || market.liquidity_bucket_slippage_limit == 0
        || market.liquidity_bucket_slippage_step > market.liquidity_bucket_slippage_limit
        || market.liquidation_dust_value.is_sign_negative()
        || market.liquidator_share_fee_rate.is_sign_negative()
        || market.insurance_fund_share_fee_rate.is_sign_negative()
    {
        return Err(invalid("market has invalid financial metadata".to_string()));
    }

    if market.last_calc_funding_rate_time == 0
        || market.last_funding_rate_time == 0
        || market.last_calc_funding_rate_time > market.last_funding_rate_time
        || [
            market.last_calc_funding_rate_time,
            market.last_funding_rate_time,
        ]
        .into_iter()
        .any(|value| {
            i64::try_from(value)
                .ok()
                .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
                .is_none()
        })
    {
        return Err(invalid("market has invalid funding timestamps".to_string()));
    }
    Ok(())
}

fn validate_spot_volume(volume: &DeepXSpotVolume) -> Result<()> {
    let endpoint = "spot volume";
    let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    if volume.total_volume.is_sign_negative() {
        return Err(invalid("total volume must not be negative".to_string()));
    }
    if volume.start_time > volume.end_time || volume.end_time > volume.statistic_time {
        return Err(invalid(
            "volume timestamps are not monotonically ordered".to_string(),
        ));
    }
    for (name, value) in [
        ("startTime", volume.start_time),
        ("endTime", volume.end_time),
        ("statisticTime", volume.statistic_time),
    ] {
        if i64::try_from(value)
            .ok()
            .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
            .is_none()
        {
            return Err(invalid(format!("{name} is outside the timestamp range")));
        }
    }
    Ok(())
}

fn validate_spot_order_book(
    book: &DeepXSpotOrderBook,
    request: &DeepXSpotOrderBookRequest,
) -> Result<()> {
    let endpoint = "spot order book";
    let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    if book
        .pair
        .strip_prefix("0x")
        .and_then(|value| nautilus_core::hex::decode_array::<32>(value).ok())
        .is_none()
        || book.pair_name.trim().is_empty()
        || request
            .name
            .as_ref()
            .is_some_and(|name| book.pair_name != *name)
        || request
            .pair
            .as_ref()
            .is_some_and(|pair| !book.pair.eq_ignore_ascii_case(pair))
    {
        return Err(invalid(
            "response has a missing or unexpected market identity".to_string(),
        ));
    }
    if book.last_update_id == 0
        || i64::try_from(book.engine_time)
            .ok()
            .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
            .is_none()
    {
        return Err(invalid(
            "response has an invalid sequence or engine time".to_string(),
        ));
    }
    if book.latest_price.is_sign_negative() || book.mid_price.is_sign_negative() {
        return Err(invalid(
            "response has a negative price observation".to_string(),
        ));
    }
    validate_spot_order_book_side(endpoint, "bid", &book.order_buy_list, false)?;
    validate_spot_order_book_side(endpoint, "ask", &book.order_sell_list, true)
}

fn validate_perp_order_book(
    book: &DeepXPerpOrderBook,
    request: &DeepXPerpOrderBookRequest,
) -> Result<()> {
    let endpoint = "perp order book";
    let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    if book.market_id == 0 || book.market_id != request.market_id {
        return Err(invalid(
            "response has a missing or unexpected market identity".to_string(),
        ));
    }
    if book.last_update_id == 0
        || i64::try_from(book.engine_time)
            .ok()
            .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
            .is_none()
    {
        return Err(invalid(
            "response has an invalid sequence or engine time".to_string(),
        ));
    }
    if book.latest_price.is_sign_negative() || book.mid_price.is_sign_negative() {
        return Err(invalid(
            "response has a negative price observation".to_string(),
        ));
    }
    validate_perp_order_book_side(endpoint, "bid", book.market_id, &book.order_buy_list, false)?;
    validate_perp_order_book_side(endpoint, "ask", book.market_id, &book.order_sell_list, true)
}

fn validate_perp_order_book_side(
    endpoint: &'static str,
    side: &str,
    market_id: u64,
    levels: &[DeepXPerpOrderBookLevel],
    ascending: bool,
) -> Result<()> {
    let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    let mut prices = HashSet::new();
    let mut previous = None;
    for (index, level) in levels.iter().enumerate() {
        if level.market_id != market_id
            || level.price <= Decimal::ZERO
            || level.qty <= Decimal::ZERO
            || level.value.is_sign_negative()
            || !prices.insert(level.price)
        {
            return Err(invalid(format!(
                "{side} level {index} has invalid values or market identity"
            )));
        }
        let out_of_order = previous.is_some_and(|previous| {
            if ascending {
                level.price <= previous
            } else {
                level.price >= previous
            }
        });
        if out_of_order {
            return Err(invalid(format!(
                "{side} level {index} is not strictly price ordered"
            )));
        }
        previous = Some(level.price);
    }
    Ok(())
}

fn validate_spot_order_book_side(
    endpoint: &'static str,
    side: &str,
    levels: &[DeepXSpotOrderBookLevel],
    ascending: bool,
) -> Result<()> {
    let invalid = |message: String| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    let mut prices = HashSet::new();
    let mut previous = None;
    for (index, level) in levels.iter().enumerate() {
        if level.price <= Decimal::ZERO
            || level.qty <= Decimal::ZERO
            || level.value.is_sign_negative()
            || !prices.insert(level.price)
        {
            return Err(invalid(format!("{side} level {index} has invalid values")));
        }
        let out_of_order = previous.is_some_and(|previous| {
            if ascending {
                level.price <= previous
            } else {
                level.price >= previous
            }
        });
        if out_of_order {
            return Err(invalid(format!(
                "{side} level {index} is not strictly price ordered"
            )));
        }
        previous = Some(level.price);
    }
    Ok(())
}

fn validate_lending_assets(assets: &[DeepXLendingAsset]) -> Result<()> {
    let endpoint = "lending assets";
    let invalid = |message| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    let mut identities = HashSet::new();
    for (index, asset) in assets.iter().enumerate() {
        if asset.market_id == 0 || asset.asset.trim().is_empty() || asset.height == 0 {
            return Err(invalid(format!(
                "item {index} has invalid identity or height"
            )));
        }
        asset
            .created_at
            .parse::<jiff::Timestamp>()
            .map_err(|e| invalid(format!("item {index} has invalid createdAt timestamp: {e}")))?;
        if !identities.insert((asset.market_id, asset.asset.to_ascii_lowercase())) {
            return Err(invalid(format!(
                "item {index} repeats a market/asset identity"
            )));
        }
    }
    Ok(())
}

fn validate_lending_interest_rate_params(
    params: &[DeepXLendingInterestRateParams],
    request: &DeepXLendingMarketRequest,
) -> Result<()> {
    let endpoint = "lending interest-rate parameters";
    let invalid = |message| DeepXHttpError::InvalidHistoryResponse { endpoint, message };
    let mut identities = HashSet::new();
    for (index, curve) in params.iter().enumerate() {
        validate_lending_scope(
            endpoint,
            index,
            curve.market_id,
            &curve.asset,
            request.market_id,
            request.asset.as_deref(),
        )?;
        if !identities.insert((curve.market_id, curve.asset.to_ascii_lowercase())) {
            return Err(invalid(format!(
                "item {index} repeats a market/asset identity"
            )));
        }
        if curve.u4.is_some() != curve.r4.is_some()
            || curve.u5.is_some() != curve.r5.is_some()
            || curve.u5.is_some() && curve.u4.is_none()
        {
            return Err(invalid(format!(
                "item {index} has incomplete optional curve nodes"
            )));
        }
        let utilizations = [
            Some(curve.u1),
            Some(curve.u2),
            Some(curve.u3),
            curve.u4,
            curve.u5,
        ];
        let rates = [
            Some(curve.r1),
            Some(curve.r2),
            Some(curve.r3),
            curve.r4,
            curve.r5,
        ];
        let mut previous_utilization = Decimal::ZERO;
        if curve.r_min < Decimal::ZERO
            || curve.r_max < Decimal::ZERO
            || curve.rho < Decimal::ZERO
            || rates.into_iter().flatten().any(|rate| rate < Decimal::ZERO)
        {
            return Err(invalid(format!("item {index} has invalid curve bounds")));
        }
        for utilization in utilizations.into_iter().flatten() {
            if utilization <= previous_utilization || utilization > Decimal::ONE {
                return Err(invalid(format!("item {index} has unordered curve nodes")));
            }
            previous_utilization = utilization;
        }
    }
    Ok(())
}

fn validate_lending_interest_rate_history(
    records: &[DeepXLendingInterestRateRecord],
    request: &DeepXLendingHistoryRequest,
) -> Result<()> {
    let endpoint = "lending interest-rate history";
    validate_lending_history_len(endpoint, records.len(), request.limit)?;
    let mut identities = HashSet::new();
    let mut previous = None;
    for (index, record) in records.iter().enumerate() {
        validate_lending_history_scope(
            endpoint,
            index,
            record.market_id,
            &record.asset,
            record.statistic_time,
            request,
            &mut previous,
        )?;
        if record.supply_apr < Decimal::ZERO || record.borrow_apr < Decimal::ZERO {
            return Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint,
                message: format!("item {index} has a negative APR"),
            });
        }
        if !identities.insert((
            record.market_id,
            record.asset.to_ascii_lowercase(),
            record.statistic_time,
        )) {
            return Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint,
                message: format!("item {index} repeats a market/asset/time bucket"),
            });
        }
    }
    Ok(())
}

fn validate_lending_status_history(
    records: &[DeepXLendingStatusRecord],
    request: &DeepXLendingHistoryRequest,
) -> Result<()> {
    let endpoint = "lending status history";
    validate_lending_history_len(endpoint, records.len(), request.limit)?;
    let mut identities = HashSet::new();
    let mut previous = None;
    for (index, record) in records.iter().enumerate() {
        validate_lending_history_scope(
            endpoint,
            index,
            record.market_id,
            &record.asset,
            record.statistic_time,
            request,
            &mut previous,
        )?;
        if record.index_price <= Decimal::ZERO
            || record.total_supplied < Decimal::ZERO
            || record.total_borrowed < Decimal::ZERO
            || record.utilization_rate < Decimal::ZERO
            || record.utilization_rate > Decimal::ONE
            || record.supply_apr < Decimal::ZERO
            || record.borrow_apr < Decimal::ZERO
        {
            return Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint,
                message: format!("item {index} has invalid financial values"),
            });
        }
        if !identities.insert((
            record.market_id,
            record.asset.to_ascii_lowercase(),
            record.statistic_time,
        )) {
            return Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint,
                message: format!("item {index} repeats a market/asset/time bucket"),
            });
        }
    }
    Ok(())
}

fn validate_lending_scope(
    endpoint: &'static str,
    index: usize,
    market_id: u64,
    asset: &str,
    expected_market_id: Option<u64>,
    expected_asset: Option<&str>,
) -> Result<()> {
    if market_id == 0
        || expected_market_id.is_some_and(|expected| market_id != expected)
        || asset.trim().is_empty()
        || expected_asset.is_some_and(|expected| !asset.eq_ignore_ascii_case(expected))
    {
        return Err(DeepXHttpError::InvalidHistoryResponse {
            endpoint,
            message: format!("item {index} does not match the requested market/asset scope"),
        });
    }
    Ok(())
}

fn validate_lending_history_scope(
    endpoint: &'static str,
    index: usize,
    market_id: u64,
    asset: &str,
    statistic_time: u64,
    request: &DeepXLendingHistoryRequest,
    previous: &mut Option<u64>,
) -> Result<()> {
    validate_lending_scope(
        endpoint,
        index,
        market_id,
        asset,
        request.market_id,
        request.asset.as_deref(),
    )?;
    let valid_time = i64::try_from(statistic_time)
        .ok()
        .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
        .is_some();
    if !valid_time
        || statistic_time < request.start_ms
        || request.end_ms.is_some_and(|end| statistic_time > end)
    {
        return Err(DeepXHttpError::InvalidHistoryResponse {
            endpoint,
            message: format!("item {index} has an invalid or out-of-range timestamp"),
        });
    }
    if previous.is_some_and(|previous| match request.sort {
        DeepXAccountSortOrder::Ascending => statistic_time < previous,
        DeepXAccountSortOrder::Descending => statistic_time > previous,
    }) {
        return Err(DeepXHttpError::InvalidHistoryResponse {
            endpoint,
            message: format!("item {index} violates the requested time order"),
        });
    }
    *previous = Some(statistic_time);
    Ok(())
}

fn validate_lending_history_len(
    endpoint: &'static str,
    len: usize,
    limit: Option<u32>,
) -> Result<()> {
    if limit.is_some_and(|limit| len > limit as usize) {
        return Err(DeepXHttpError::InvalidHistoryResponse {
            endpoint,
            message: "response exceeds the requested limit".to_string(),
        });
    }
    Ok(())
}

fn validate_response_market_id(endpoint: &'static str, expected: u64, received: u64) -> Result<()> {
    if received != expected {
        return Err(DeepXHttpError::ResponseMarketMismatch {
            endpoint,
            expected,
            received,
        });
    }
    Ok(())
}

fn normalize_base_url(base_url: String) -> Result<String> {
    let normalized = base_url.trim_end_matches('/').to_string();
    let Ok(url) = reqwest::Url::parse(&normalized) else {
        return Err(DeepXHttpError::InvalidBaseUrl(base_url));
    };
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DeepXHttpError::InvalidBaseUrl(base_url));
    }
    Ok(normalized)
}

fn validate_path(path: &str) -> Result<()> {
    if !path.starts_with('/')
        || path.starts_with("//")
        || path.contains('?')
        || path.contains('#')
        || path.contains("://")
        || path.split('/').any(|segment| segment == "..")
    {
        return Err(DeepXHttpError::InvalidPath(path.to_string()));
    }
    Ok(())
}

fn bounded_body(body: &[u8]) -> String {
    String::from_utf8_lossy(body)
        .chars()
        .take(MAX_ERROR_BODY_CHARS)
        .collect()
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
    use crate::http::{DeepXPerpCandleInterval, DeepXPerpVolumePeriod, models::DeepXResponseCode};

    const PERP_VOLUME_1H_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_volume_1h.json");
    const SPOT_TRADES_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_trades_eth_usdc_page_1.json");
    const SPOT_CANDLES_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_candles_eth_usdc_1m.json");
    const SPOT_LAST_PRICE_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_last_price_eth_usdc.json");
    const SPOT_VOLUME_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_volume_eth_usdc_1h.json");
    const SPOT_ORDER_BOOK_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_order_book_eth_usdc_tick_001.json");
    const PERP_ORDER_BOOK_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_order_book_eth_usdc_tick_001.json");
    const SPOT_MARKETS_SPEC369_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_markets_spec369.json");
    const SPOT_MARKET_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_market_eth_usdc_by_pair.json");
    const PERP_MARKET_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_market_eth_usdc_by_id.json");
    const PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_history_orders_account_market_3.json");
    const SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_history_orders_eth_usdc_page_1.json");
    const SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_account_trades_eth_usdc_page_1.json");
    const SPOT_WALLET_ORDERS_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_wallet_orders_eth_usdc_page_1.json");
    const SPOT_WALLET_TRADES_ETH_USDC_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_wallet_trades_eth_usdc_page_1.json");
    const SPOT_ORDER_BY_ID_CANCELED_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/spot_order_by_id_canceled.json");
    const PERP_ORDER_BY_TX_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_order_by_tx.json");
    const PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_account_trades_account_market_3.json");
    const PERP_WALLET_TRADES_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_wallet_trades_page.json");
    const PERP_WALLET_ORDERS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_wallet_orders_page_1.json");
    const PERP_FUNDING_FEES_ACCOUNT_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_funding_fees_account_market_3.json");
    const WALLET_FUNDING_FEES_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_funding_fees_page_2.json");
    const WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE: &str = include_str!(
        "../../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_1.json"
    );
    const WALLET_HOURLY_FUNDING_PAGE_2_RESPONSE: &str = include_str!(
        "../../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_2.json"
    );
    const PERP_POSITIONS_ACCOUNT_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_positions_account_market_3.json");
    const WALLET_SUBACCOUNTS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_subaccounts.json");
    const ALL_SUBACCOUNTS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/all_subaccounts_page_1.json");
    const WALLET_DELEGATE_ACCOUNTS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_delegate_accounts.json");
    const DELEGATE_DELEGATOR_ACCOUNTS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/delegate_delegator_accounts.json");
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
    const WALLET_USER_STATS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_user_stats.json");
    const PERP_LIQUIDATION_PRICE_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_liquidation_price_account_market_3.json");
    const WALLET_QUOTA_SUMMARY_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_quota_summary.json");
    const QUOTA_HISTORY_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/quota_history_purchase_page.json");
    const LENDING_ASSETS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/lending_assets.json");
    const LENDING_INTEREST_RATE_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/lending_interest_rate_usdc.json");
    const LENDING_INTEREST_RATE_HISTORY_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/lending_interest_rate_history_usdc_1h.json");
    const LENDING_STATUS_HISTORY_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/lending_status_history_usdc_1h.json");

    fn raw_account_fixture(response: &str) -> DeepXRawAccountPage {
        serde_json::from_str::<DeepXApiResponse<DeepXRawAccountPage>>(response)
            .unwrap()
            .data
    }

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

    fn lending_history_request() -> DeepXLendingHistoryRequest {
        DeepXLendingHistoryRequest {
            market_id: Some(1),
            asset: Some("usdc".to_string()),
            interval: DeepXPerpCandleInterval::OneHour,
            start_ms: 1_789_430_000_000,
            end_ms: None,
            limit: Some(3),
            sort: DeepXAccountSortOrder::Descending,
        }
    }

    #[tokio::test]
    async fn decodes_lending_fixtures_through_client() {
        let router = Router::new()
            .route(
                LENDING_ASSETS_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(query, None);
                    LENDING_ASSETS_RESPONSE
                }),
            )
            .route(
                LENDING_INTEREST_RATE_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(query.as_deref(), Some("marketId=1&asset=USDC"));
                    LENDING_INTEREST_RATE_RESPONSE
                }),
            )
            .route(
                LENDING_INTEREST_RATE_HISTORY_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(
                        query.as_deref(),
                        Some(concat!(
                            "marketId=1&asset=usdc&timeFrame=1h&start=1789430000000",
                            "&limit=3&sort=DESC",
                        )),
                    );
                    LENDING_INTEREST_RATE_HISTORY_RESPONSE
                }),
            )
            .route(
                LENDING_STATUS_HISTORY_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(
                        query.as_deref(),
                        Some(concat!(
                            "marketId=1&asset=usdc&timeFrame=1h&start=1789430000000",
                            "&limit=3&sort=DESC",
                        )),
                    );
                    LENDING_STATUS_HISTORY_RESPONSE
                }),
            );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();

        let assets = client.get_lending_assets().await.unwrap();
        assert_eq!(assets.len(), 3);
        assert_eq!(assets[2].asset, "usdc");

        let curves = client
            .get_lending_interest_rate_params(&DeepXLendingMarketRequest {
                market_id: Some(1),
                asset: Some("USDC".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(curves[0].r_max, Decimal::new(15, 1));
        assert_eq!(curves[0].u4, None);

        let request = lending_history_request();
        let rates = client
            .get_lending_interest_rate_history(&request)
            .await
            .unwrap();
        assert_eq!(rates.details.len(), 3);
        assert_eq!(
            rates.details[0].borrow_apr,
            "0.001327604517817316".parse::<Decimal>().unwrap(),
        );

        let statuses = client.get_lending_status_history(&request).await.unwrap();
        assert_eq!(statuses.details.len(), 3);
        assert_eq!(statuses.details[0].index_price, Decimal::ONE);
    }

    #[rstest]
    #[case("identity")]
    #[case("height")]
    #[case("timestamp")]
    #[case("duplicate")]
    fn rejects_invalid_lending_assets(#[case] mutation: &str) {
        let mut assets = serde_json::from_str::<DeepXApiResponse<Vec<DeepXLendingAsset>>>(
            LENDING_ASSETS_RESPONSE,
        )
        .unwrap()
        .data;
        match mutation {
            "identity" => assets[0].asset = " ".to_string(),
            "height" => assets[0].height = 0,
            "timestamp" => assets[0].created_at = "invalid".to_string(),
            "duplicate" => assets[1] = assets[0].clone(),
            _ => unreachable!(),
        }

        assert!(validate_lending_assets(&assets).is_err());
    }

    #[rstest]
    #[case("filter")]
    #[case("pair")]
    #[case("gap")]
    #[case("utilization")]
    #[case("rate")]
    #[case("rho")]
    #[case("duplicate")]
    fn rejects_invalid_lending_curve(#[case] mutation: &str) {
        let mut curves = serde_json::from_str::<
            DeepXApiResponse<Vec<DeepXLendingInterestRateParams>>,
        >(LENDING_INTEREST_RATE_RESPONSE)
        .unwrap()
        .data;
        match mutation {
            "filter" => curves[0].market_id = 2,
            "pair" => curves[0].r4 = Some(Decimal::ONE),
            "gap" => {
                curves[0].u5 = Some(Decimal::new(995, 3));
                curves[0].r5 = Some(Decimal::ONE);
            }
            "utilization" => curves[0].u2 = curves[0].u1,
            "rate" => curves[0].r2 = Decimal::NEGATIVE_ONE,
            "rho" => curves[0].rho = Decimal::NEGATIVE_ONE,
            "duplicate" => curves.push(curves[0].clone()),
            _ => unreachable!(),
        }
        let request = DeepXLendingMarketRequest {
            market_id: Some(1),
            asset: Some("usdc".to_string()),
        };

        assert!(validate_lending_interest_rate_params(&curves, &request).is_err());
    }

    #[rstest]
    fn accepts_non_monotonic_lending_rates_supported_by_chain_curve() {
        let mut curves = serde_json::from_str::<
            DeepXApiResponse<Vec<DeepXLendingInterestRateParams>>,
        >(LENDING_INTEREST_RATE_RESPONSE)
        .unwrap()
        .data;
        curves[0].r2 = Decimal::new(1, 2);
        curves[0].r_max = Decimal::new(5, 3);
        let request = DeepXLendingMarketRequest {
            market_id: Some(1),
            asset: Some("usdc".to_string()),
        };

        validate_lending_interest_rate_params(&curves, &request).unwrap();
    }

    #[rstest]
    #[case("filter")]
    #[case("apr")]
    #[case("order")]
    #[case("timestamp")]
    #[case("duplicate")]
    #[case("limit")]
    fn rejects_invalid_lending_interest_rate_history(#[case] mutation: &str) {
        let mut records =
            serde_json::from_str::<DeepXApiResponse<DeepXLendingInterestRateHistory>>(
                LENDING_INTEREST_RATE_HISTORY_RESPONSE,
            )
            .unwrap()
            .data
            .details;
        let mut request = lending_history_request();
        match mutation {
            "filter" => records[0].asset = "eth".to_string(),
            "apr" => records[0].supply_apr = Decimal::NEGATIVE_ONE,
            "order" => records.swap(0, 2),
            "timestamp" => records[0].statistic_time = request.start_ms - 1,
            "duplicate" => records[1] = records[0].clone(),
            "limit" => request.limit = Some(2),
            _ => unreachable!(),
        }

        assert!(validate_lending_interest_rate_history(&records, &request).is_err());
    }

    #[rstest]
    #[case("price")]
    #[case("supplied")]
    #[case("borrowed")]
    #[case("utilization")]
    #[case("apr")]
    #[case("filter")]
    #[case("order")]
    #[case("timestamp")]
    #[case("duplicate")]
    #[case("limit")]
    fn rejects_invalid_lending_status_history(#[case] mutation: &str) {
        let mut records = serde_json::from_str::<DeepXApiResponse<DeepXLendingStatusHistory>>(
            LENDING_STATUS_HISTORY_RESPONSE,
        )
        .unwrap()
        .data
        .details;
        let mut request = lending_history_request();
        match mutation {
            "price" => records[0].index_price = Decimal::ZERO,
            "supplied" => records[0].total_supplied = Decimal::NEGATIVE_ONE,
            "borrowed" => records[0].total_borrowed = Decimal::NEGATIVE_ONE,
            "utilization" => records[0].utilization_rate = Decimal::new(101, 2),
            "apr" => records[0].borrow_apr = Decimal::NEGATIVE_ONE,
            "filter" => records[0].market_id = 2,
            "order" => records.swap(0, 2),
            "timestamp" => records[0].statistic_time = request.start_ms - 1,
            "duplicate" => records[1] = records[0].clone(),
            "limit" => request.limit = Some(2),
            _ => unreachable!(),
        }

        assert!(validate_lending_status_history(&records, &request).is_err());
    }

    #[tokio::test]
    async fn test_perp_order_by_id_raw() {
        let user = "0x1111111111111111111111111111111111111111";
        let app = Router::new().route(
            "/internal/v1/account/perp/order-by-id",
            get(move |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                assert_eq!(query.len(), 3);
                assert_eq!(query["user"], user);
                assert_eq!(query["marketId"], "3");
                match query["oid"].as_str() {
                    "id&opaque=1" => r#"{"code":200,"msg":"success","fail":false,"data":{"price":0.1234567890123456789012345678,"orderId":"id&opaque=1"}}"#,
                    "missing" => r#"{"code":10008,"msg":"not found","fail":true,"data":null}"#,
                    _ => r#"{"code":200,"msg":"success","fail":false}"#,
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let payload = client
            .get_perp_order_by_id_raw(user, 3, "id&opaque=1")
            .await
            .unwrap();
        assert_eq!(
            payload.get(),
            r#"{"price":0.1234567890123456789012345678,"orderId":"id&opaque=1"}"#
        );
        assert!(matches!(
            client.get_perp_order_by_id_raw(user, 3, "missing").await,
            Err(DeepXHttpError::Api {
                code: DeepXResponseCode::Api(10008),
                ..
            })
        ));
        assert!(
            client
                .get_perp_order_by_id_raw(user, 3, "malformed")
                .await
                .is_err()
        );
        for (address, market, oid) in [("invalid", 3, "1"), (user, 0, "1"), (user, 3, " ")] {
            assert!(matches!(
                client.get_perp_order_by_id_raw(address, market, oid).await,
                Err(DeepXHttpError::InvalidRequest(_))
            ));
        }
        server.abort();
    }

    #[tokio::test]
    async fn typed_perp_order_by_id_validates_identity_and_nullable_fields() {
        let fixture: serde_json::Value =
            serde_json::from_str(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE).unwrap();
        let filled = fixture["data"]["items"][0].clone();
        let mut canceled = filled.clone();
        canceled["orderId"] = json!("69544");
        canceled["avgFillPrice"] = serde_json::Value::Null;
        canceled.as_object_mut().unwrap().remove("updatedTime");
        canceled["status"] = json!("Canceled");
        canceled["sizeFilled"] = json!(0);
        canceled["sizeRemain"] = canceled["size"].clone();
        let app = Router::new().route(
            "/internal/v1/account/perp/order-by-id",
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let data = if query["oid"] == "69544" {
                        canceled.clone()
                    } else {
                        filled.clone()
                    };
                    async move {
                        assert_eq!(query["user"], "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc");
                        assert_eq!(query["marketId"], "3");
                        Json(json!({
                            "code": 200,
                            "msg": "success",
                            "fail": false,
                            "data": data,
                        }))
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let user = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";

        let order = client
            .get_perp_order_by_id(user, 3, "1789445193480")
            .await
            .unwrap();
        assert_eq!(order.order_id, "1789445193480");
        assert_eq!(order.avg_fill_price, Some(Decimal::new(24_996, 1)));
        assert!(order.updated_time.is_some());

        let canceled = client.get_perp_order_by_id(user, 3, "69544").await.unwrap();
        assert_eq!(canceled.avg_fill_price, None);
        assert_eq!(canceled.updated_time, None);

        assert!(matches!(
            client.get_perp_order_by_id(user, 3, "1").await,
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "perp order by ID",
                ..
            })
        ));
        assert!(matches!(
            client
                .get_perp_order_by_id(user, 3, "18446744073709551616")
                .await,
            Err(DeepXHttpError::InvalidRequest(_))
        ));
        server.abort();
    }

    #[tokio::test]
    async fn typed_perp_order_by_tx_encodes_query_and_decodes_fixture() {
        const TX_HASH: &str = "0x247ef3967338c6985714731a49752455ebe877d8678e0b261b3638892cfae554";
        let fixture: serde_json::Value = serde_json::from_str(PERP_ORDER_BY_TX_RESPONSE).unwrap();
        let app = Router::new().route(
            "/internal/v1/account/perp/order-by-tx",
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let fixture = fixture.clone();
                    async move {
                        assert_eq!(query.len(), 1);
                        assert_eq!(query["txHash"], TX_HASH);
                        Json(fixture)
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let order = client.get_perp_order_by_tx(TX_HASH).await.unwrap();

        assert_eq!(order.order_id, "1789445193480");
        assert_eq!(order.size, Decimal::new(3, 1));
        assert_eq!(order.avg_fill_price, Some(Decimal::new(24_996, 1)));
        assert_eq!(order.fee, Decimal::new(-149_921, 6));
        assert_eq!(order.tx_hash, TX_HASH);
        server.abort();
    }

    #[tokio::test]
    async fn typed_perp_order_by_tx_rejects_invalid_input_and_response_identity() {
        const MISMATCH_HASH: &str =
            "0x1111111111111111111111111111111111111111111111111111111111111111";
        const INVALID_RESPONSE_HASH_REQUEST: &str =
            "0x2222222222222222222222222222222222222222222222222222222222222222";
        const EMPTY_TYPE_HASH: &str =
            "0x3333333333333333333333333333333333333333333333333333333333333333";
        let fixture: serde_json::Value = serde_json::from_str(PERP_ORDER_BY_TX_RESPONSE).unwrap();
        let app = Router::new().route(
            "/internal/v1/account/perp/order-by-tx",
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let mut response = fixture.clone();
                    match query["txHash"].as_str() {
                        INVALID_RESPONSE_HASH_REQUEST => response["data"]["txHash"] = json!("0x12"),
                        EMPTY_TYPE_HASH => {
                            response["data"]["txHash"] = json!(EMPTY_TYPE_HASH);
                            response["data"]["txHashType"] = json!(" ");
                        }
                        MISMATCH_HASH => {}
                        _ => unreachable!(),
                    }
                    async move { Json(response) }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        for tx_hash in ["", "247ef396", "0x12", "0xzz"] {
            assert!(matches!(
                client.get_perp_order_by_tx(tx_hash).await,
                Err(DeepXHttpError::InvalidRequest(_))
            ));
        }
        assert!(matches!(
            client.get_perp_order_by_tx(MISMATCH_HASH).await,
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "perp order by transaction hash",
                ..
            })
        ));
        for tx_hash in [INVALID_RESPONSE_HASH_REQUEST, EMPTY_TYPE_HASH] {
            assert!(matches!(
                client.get_perp_order_by_tx(tx_hash).await,
                Err(DeepXHttpError::InvalidHistoryResponse {
                    endpoint: "perp order by transaction hash",
                    ..
                })
            ));
        }
        server.abort();
    }

    #[tokio::test]
    async fn test_perp_account_order_pages_preserve_raw_items_and_queries() {
        const OPEN_QUERY: &str = concat!(
            "user=0x1111111111111111111111111111111111111111&marketId=3&isLong=false",
            "&cursor=opaque%26cursor&sort=ASC&pageSize=7",
        );
        const OPEN_RESPONSE: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":["#,
            r#"{"orderId":"9007199254740993","price":0.1234567890123456789012345678,"#,
            r#""unknown":"retained"}],"hasNext":true,"nextCursor":"next-page"}}"#,
        );
        const OPEN_ITEM: &str = concat!(
            r#"{"orderId":"9007199254740993","price":0.1234567890123456789012345678,"#,
            r#""unknown":"retained"}"#,
        );
        const HISTORY_RESPONSE: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":[],"#,
            r#""hasNext":false,"nextCursor":null}}"#,
        );
        let subaccount = "0x1111111111111111111111111111111111111111";
        let open_app = get(move |RawQuery(query): RawQuery| async move {
            assert_eq!(query.as_deref(), Some(OPEN_QUERY));
            OPEN_RESPONSE
        });
        let history_app = get(move |RawQuery(query): RawQuery| async move {
            assert_eq!(
                query.as_deref(),
                Some("user=0x1111111111111111111111111111111111111111&sort=DESC")
            );
            HISTORY_RESPONSE
        });
        let app = Router::new()
            .route(PERP_OPEN_ORDERS_PATH, open_app)
            .route(PERP_HISTORY_ORDERS_PATH, history_app);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let open_page = client
            .get_perp_open_orders_raw(&DeepXPerpOpenOrdersRequest {
                subaccount: subaccount.to_string(),
                market_id: Some(3),
                is_long: Some(false),
                cursor: Some("opaque&cursor".to_string()),
                page_size: Some(7),
                sort: crate::http::DeepXAccountSortOrder::Ascending,
            })
            .await
            .unwrap();
        assert!(open_page.has_next);
        assert_eq!(open_page.next_cursor.as_deref(), Some("next-page"));
        assert_eq!(open_page.items[0].get(), OPEN_ITEM);

        let history_page = client
            .get_perp_history_orders_raw(&DeepXPerpHistoryOrdersRequest {
                subaccount: subaccount.to_string(),
                market_id: None,
                cursor: None,
                page_size: None,
                sort: crate::http::DeepXAccountSortOrder::Descending,
            })
            .await
            .unwrap();
        assert!(history_page.items.is_empty());
        assert!(!history_page.has_next);
        assert_eq!(history_page.next_cursor, None);
        server.abort();
    }

    #[tokio::test]
    async fn perp_open_order_pages_follow_cursor_and_reject_cross_page_duplicates() {
        let captured: serde_json::Value =
            serde_json::from_str(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE).unwrap();
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
                            0 | 2 => {
                                assert!(!query.contains_key("cursor"));
                                (first_item, true, Some("next"))
                            }
                            1 => {
                                assert_eq!(query["cursor"], "next");
                                (second_item, false, None)
                            }
                            3 => {
                                assert_eq!(query["cursor"], "next");
                                (first_item, false, None)
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

        assert!(matches!(
            client.get_perp_open_order_pages(&request, 2).await,
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "perp open orders",
                ..
            })
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 4);
        server.abort();
    }

    #[rstest]
    fn perp_order_pages_reject_side_and_time_order_mismatches() {
        let request = DeepXPerpOpenOrdersRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_id: Some(3),
            is_long: Some(false),
            cursor: None,
            page_size: Some(10),
            sort: DeepXAccountSortOrder::Descending,
        };
        let mut previous = None;
        let side_error = decode_account_page::<DeepXPerpOrderRecord, _>(
            "perp open orders",
            raw_account_fixture(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE),
            request.page_size,
            |index, order| validate_perp_open_order_record(index, order, &request, &mut previous),
        )
        .unwrap_err();
        assert!(matches!(
            side_error,
            DeepXHttpError::InvalidHistoryResponse {
                endpoint: "perp open orders",
                ..
            }
        ));

        let mut reversed: serde_json::Value =
            serde_json::from_str(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE).unwrap();
        reversed["data"]["items"].as_array_mut().unwrap().reverse();
        let reversed_page =
            serde_json::from_value::<DeepXRawAccountPage>(reversed["data"].clone()).unwrap();
        let mut unrestricted = request;
        unrestricted.is_long = None;
        let mut previous = None;
        let order_error = decode_account_page::<DeepXPerpOrderRecord, _>(
            "perp open orders",
            reversed_page,
            unrestricted.page_size,
            |index, order| {
                validate_perp_open_order_record(index, order, &unrestricted, &mut previous)
            },
        )
        .unwrap_err();
        assert!(matches!(
            order_error,
            DeepXHttpError::InvalidHistoryResponse {
                endpoint: "perp open orders",
                ..
            }
        ));

        let history_request = DeepXPerpHistoryOrdersRequest {
            subaccount: unrestricted.subaccount,
            market_id: unrestricted.market_id,
            cursor: None,
            page_size: unrestricted.page_size,
            sort: unrestricted.sort,
        };
        let mut previous = None;
        let history_error = decode_account_page::<DeepXPerpOrderRecord, _>(
            "perp history orders",
            raw_account_fixture(
                &serde_json::to_string(&reversed).expect("reversed fixture should serialize"),
            ),
            history_request.page_size,
            |index, order| {
                validate_perp_history_order_record(index, order, &history_request, &mut previous)
            },
        )
        .unwrap_err();
        assert!(matches!(
            history_error,
            DeepXHttpError::InvalidHistoryResponse {
                endpoint: "perp history orders",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn spot_order_readers_preserve_queries_and_decode_exact_records() {
        let pair = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
        let subaccount = "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c";
        let app = Router::new()
            .route(
                SPOT_OPEN_ORDERS_PATH,
                get(
                    move |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                        assert_eq!(query["pair"], pair);
                        assert_eq!(query["user"], subaccount);
                        assert_eq!(query["orderSide"], "Sell");
                        assert_eq!(query["sort"], "DESC");
                        assert_eq!(query["pageSize"], "3");
                        assert_eq!(query.len(), 5);
                        SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE
                    },
                ),
            )
            .route(
                SPOT_HISTORY_ORDERS_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(
                        query.as_deref(),
                        Some(concat!(
                            "name=ETH%2FUSDC&user=0x08bdb660e03c75954e0fcaa1e7ead150a92c891c",
                            "&sort=DESC&pageSize=3",
                        ))
                    );
                    SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let open = client
            .get_spot_open_orders(&DeepXSpotOpenOrdersRequest {
                subaccount: subaccount.to_string(),
                name: None,
                pair: Some(pair.to_string()),
                order_side: Some(DeepXSpotOrderSide::Sell),
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                page_size: Some(3),
            })
            .await
            .unwrap();
        let history = client
            .get_spot_history_orders(&DeepXSpotHistoryOrdersRequest {
                subaccount: subaccount.to_string(),
                name: Some("ETH/USDC".to_string()),
                pair: None,
                order_side: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                page_size: Some(3),
            })
            .await
            .unwrap();

        assert_eq!(open.items, history.items);
        assert_eq!(history.items.len(), 3);
        assert!(history.has_next);
        assert_eq!(history.items[0].price, Decimal::new(245_605, 2));
        assert_eq!(history.items[0].base_amount, Decimal::new(8_849, 4));
        assert_eq!(history.items[0].fee, Decimal::ZERO);
        server.abort();
    }

    #[tokio::test]
    async fn spot_order_lookups_bind_exact_id_side_owner_market_and_tx_hash() {
        const TX_HASH: &str = "0x437279a6138febacf1a416ad24e3377f8c1ecd4ee0f9cdd6702f11f20d9e1750";
        let app = Router::new()
            .route(
                SPOT_ORDER_BY_ID_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(
                        query.as_deref(),
                        Some(concat!(
                            "name=ETH%2FUSDC&user=0x08bdb660e03c75954e0fcaa1e7ead150a92c891c",
                            "&oid=1789627379350&orderSide=Sell",
                        ))
                    );
                    SPOT_ORDER_BY_ID_CANCELED_RESPONSE
                }),
            )
            .route(
                SPOT_ORDER_BY_TX_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(query.as_deref(), Some(format!("txHash={TX_HASH}").as_str()));
                    SPOT_ORDER_BY_ID_CANCELED_RESPONSE
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let request = DeepXSpotOrderByIdRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            order_id: "1789627379350".to_string(),
            order_side: DeepXSpotOrderSide::Sell,
        };

        let by_id = client.get_spot_order_by_id(&request).await.unwrap();
        let by_tx = client.get_spot_order_by_tx(TX_HASH).await.unwrap();

        assert_eq!(by_id, by_tx);
        assert_eq!(by_id.status, "Canceled");
        assert_eq!(by_id.cancel_reason.as_deref(), Some("UserCanceled"));
        assert_eq!(by_id.cancel_height, Some(188_574_100));
        server.abort();
    }

    #[tokio::test]
    async fn spot_order_lookups_reject_invalid_inputs_and_response_identity() {
        const TX_HASH: &str = "0x437279a6138febacf1a416ad24e3377f8c1ecd4ee0f9cdd6702f11f20d9e1750";
        const OTHER_HASH: &str =
            "0x1111111111111111111111111111111111111111111111111111111111111111";
        let app = Router::new()
            .route(
                SPOT_ORDER_BY_ID_PATH,
                get(|| async { SPOT_ORDER_BY_ID_CANCELED_RESPONSE }),
            )
            .route(
                SPOT_ORDER_BY_TX_PATH,
                get(|| async { SPOT_ORDER_BY_ID_CANCELED_RESPONSE }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let mismatched_id = DeepXSpotOrderByIdRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            order_id: "1789627379351".to_string(),
            order_side: DeepXSpotOrderSide::Sell,
        };

        assert!(matches!(
            client.get_spot_order_by_id(&mismatched_id).await,
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot order by ID",
                ..
            })
        ));
        assert!(matches!(
            client.get_spot_order_by_tx(OTHER_HASH).await,
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot order by transaction hash",
                ..
            })
        ));
        assert!(matches!(
            client.get_spot_order_by_tx("bad").await,
            Err(DeepXHttpError::InvalidRequest(_))
        ));
        assert_ne!(TX_HASH, OTHER_HASH);
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
    async fn spot_account_trade_reader_preserves_query_and_exact_values() {
        let app = Router::new().route(
            SPOT_ACCOUNT_TRADES_PATH,
            get(|RawQuery(query): RawQuery| async move {
                assert_eq!(
                    query.as_deref(),
                    Some(concat!(
                        "name=ETH%2FUSDC&user=0x08bdb660e03c75954e0fcaa1e7ead150a92c891c",
                        "&sort=DESC&pageSize=3",
                    ))
                );
                SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let page = client
            .get_spot_account_trades(&DeepXSpotAccountTradesRequest {
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
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 3);
        assert!(page.has_next);
        assert_eq!(page.items[0].id, 188_573_374_000_099);
        assert_eq!(
            page.items[0].base_amount,
            "0.33079181307615474".parse::<Decimal>().unwrap()
        );
        assert_eq!(page.items[0].fee, Decimal::new(-323_216, 6));
        assert_eq!(page.items[0].fee_asset, "");
        server.abort();
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
    fn spot_account_trade_validation_rejects_invalid_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXSpotAccountTradeRecord>> =
            serde_json::from_str(SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE).unwrap();
        let mut trade = response.data.items[0].clone();
        let mut request = DeepXSpotAccountTradesRequest {
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
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot account trades",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn spot_account_trade_pages_follow_cursor_and_reject_duplicate_ids() {
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

        let mut duplicate: serde_json::Value =
            serde_json::from_str(SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE).unwrap();
        duplicate["data"]["items"][1]["id"] = duplicate["data"]["items"][0]["id"].clone();
        let duplicate_page =
            serde_json::from_value::<DeepXRawAccountPage>(duplicate["data"].clone()).unwrap();
        let mut ids = HashSet::new();
        let mut previous = None;
        assert!(
            decode_spot_account_trade_page(duplicate_page, &request, &mut ids, &mut previous,)
                .is_err()
        );
        server.abort();
    }

    #[tokio::test]
    async fn spot_wallet_readers_normalize_global_cursor_and_preserve_exact_queries() {
        let app = Router::new()
            .route(
                SPOT_WALLET_ORDERS_PATH,
                get(
                    |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                        assert_eq!(
                            query["address"],
                            "0x89bb0946046588f4f257ffc71bd3231aba13474a"
                        );
                        assert_eq!(query["name"], "ETH/USDC");
                        assert_eq!(query["sort"], "DESC");
                        assert_eq!(query["pageSize"], "5");
                        assert_eq!(query.len(), 4);
                        SPOT_WALLET_ORDERS_ETH_USDC_RESPONSE
                    },
                ),
            )
            .route(
                SPOT_WALLET_TRADES_PATH,
                get(
                    |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                        assert_eq!(
                            query["address"],
                            "0x89bb0946046588f4f257ffc71bd3231aba13474a"
                        );
                        assert_eq!(query["name"], "ETH/USDC");
                        assert_eq!(query["sort"], "DESC");
                        assert_eq!(query["pageSize"], "5");
                        assert_eq!(query.len(), 4);
                        SPOT_WALLET_TRADES_ETH_USDC_RESPONSE
                    },
                ),
            );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();

        let orders = client
            .get_spot_wallet_orders(&spot_wallet_orders_request())
            .await
            .unwrap();
        let trades = client
            .get_spot_wallet_trades(&spot_wallet_trades_request())
            .await
            .unwrap();

        assert_eq!(orders.markets[0].subaccounts[0].orders.items.len(), 5);
        assert!(orders.has_next);
        assert_eq!(
            orders.next_cursor,
            orders.markets[0].subaccounts[0].orders.next_cursor
        );
        assert_eq!(trades.markets[0].subaccounts[0].trades.items.len(), 5);
        assert!(trades.has_next);
        assert_eq!(
            trades.markets[0].subaccounts[0].trades.items[0].base_amount,
            "0.26400108068721884".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            trades.next_cursor,
            trades.markets[0].subaccounts[0].trades.next_cursor
        );
    }

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
    async fn spot_wallet_orders_reject_invalid_group_semantics(#[case] mutation: &str) {
        let mut response: serde_json::Value =
            serde_json::from_str(SPOT_WALLET_ORDERS_ETH_USDC_RESPONSE).unwrap();
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
        let app = Router::new().route(
            SPOT_WALLET_ORDERS_PATH,
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();

        assert!(
            client
                .get_spot_wallet_orders(&spot_wallet_orders_request())
                .await
                .is_err()
        );
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
    async fn spot_wallet_trades_reject_invalid_group_semantics(#[case] mutation: &str) {
        let mut response: serde_json::Value =
            serde_json::from_str(SPOT_WALLET_TRADES_ETH_USDC_RESPONSE).unwrap();
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
        let app = Router::new().route(
            SPOT_WALLET_TRADES_PATH,
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();

        assert!(
            client
                .get_spot_wallet_trades(&spot_wallet_trades_request())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn spot_wallet_readers_accept_empty_terminal_groups() {
        const PAIR: &str = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
        const SUBACCOUNT: &str = "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c";
        let orders = json!({
            "code": 200,
            "msg": "success",
            "fail": false,
            "data": [{
                "pair": PAIR,
                "name": "ETH/USDC",
                "subaccounts": [{
                    "subaccount": SUBACCOUNT,
                    "orders": {"items": [], "nextCursor": null, "hasNext": false}
                }]
            }]
        });
        let trades = json!({
            "code": 200,
            "msg": "success",
            "fail": false,
            "data": [{
                "pair": PAIR,
                "name": "ETH/USDC",
                "subaccounts": [{
                    "subaccount": SUBACCOUNT,
                    "trades": {"items": [], "nextCursor": null, "hasNext": false}
                }]
            }]
        });
        let app = Router::new()
            .route(
                SPOT_WALLET_ORDERS_PATH,
                get(move || {
                    let response = orders.clone();
                    async move { Json(response) }
                }),
            )
            .route(
                SPOT_WALLET_TRADES_PATH,
                get(move || {
                    let response = trades.clone();
                    async move { Json(response) }
                }),
            );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();

        let orders = client
            .get_spot_wallet_orders(&spot_wallet_orders_request())
            .await
            .unwrap();
        let trades = client
            .get_spot_wallet_trades(&spot_wallet_trades_request())
            .await
            .unwrap();

        assert!(!orders.has_next);
        assert_eq!(orders.next_cursor, None);
        assert!(!trades.has_next);
        assert_eq!(trades.next_cursor, None);
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn spot_wallet_order_collector_forwards_cursor_and_rejects_duplicates(
        #[case] duplicate: bool,
    ) {
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
        if !duplicate {
            item["orderId"] = json!("1789631508485");
        }
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

        let result = client
            .get_spot_wallet_order_pages(&spot_wallet_orders_request(), 2)
            .await;

        if duplicate {
            assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidHistoryResponse {
                    endpoint: "spot wallet orders",
                    ..
                })
            ));
        } else {
            let pages = result.unwrap();
            assert_eq!(pages.len(), 2);
            assert!(!pages[1].has_next);
        }
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn spot_wallet_trade_collector_forwards_cursor_and_rejects_duplicates(
        #[case] duplicate: bool,
    ) {
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
        if !duplicate {
            item["id"] = json!(188_632_780_999_999_u64);
        }
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

        let result = client
            .get_spot_wallet_trade_pages(&spot_wallet_trades_request(), 2)
            .await;

        if duplicate {
            assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidHistoryResponse {
                    endpoint: "spot wallet trades",
                    ..
                })
            ));
        } else {
            let pages = result.unwrap();
            assert_eq!(pages.len(), 2);
            assert!(!pages[1].has_next);
        }
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
    fn spot_order_validation_rejects_invalid_record_semantics(#[case] mutation: &str) {
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
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot history orders",
                ..
            })
        ));
    }

    #[rstest]
    fn spot_history_orders_reject_duplicates_ordering_and_oversized_pages() {
        let request = DeepXSpotHistoryOrdersRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(3),
        };
        let page = raw_account_fixture(SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE);
        let mut ids = HashSet::new();
        let mut previous = None;
        assert_eq!(
            decode_spot_order_page(
                "spot history orders",
                page,
                &request,
                &mut ids,
                &mut previous,
            )
            .unwrap()
            .items
            .len(),
            3
        );

        let mut ascending = request.clone();
        ascending.sort = DeepXAccountSortOrder::Ascending;
        let mut ids = HashSet::new();
        let mut previous = None;
        assert!(
            decode_spot_order_page(
                "spot history orders",
                raw_account_fixture(SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE),
                &ascending,
                &mut ids,
                &mut previous,
            )
            .is_err()
        );

        let mut duplicate: serde_json::Value =
            serde_json::from_str(SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE).unwrap();
        duplicate["data"]["items"][1]["orderId"] = duplicate["data"]["items"][0]["orderId"].clone();
        let duplicate_page =
            serde_json::from_value::<DeepXRawAccountPage>(duplicate["data"].clone()).unwrap();
        let mut ids = HashSet::new();
        let mut previous = None;
        assert!(
            decode_spot_order_page(
                "spot history orders",
                duplicate_page,
                &request,
                &mut ids,
                &mut previous,
            )
            .is_err()
        );

        let mut too_small = request.clone();
        too_small.page_size = Some(2);
        let mut ids = HashSet::new();
        let mut previous = None;
        assert!(
            decode_spot_order_page(
                "spot history orders",
                raw_account_fixture(SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE),
                &too_small,
                &mut ids,
                &mut previous,
            )
            .is_err()
        );
    }

    #[rstest]
    fn captured_account_pages_decode_with_strict_scope_and_financial_validation() {
        let subaccount = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
        let orders_request = DeepXPerpHistoryOrdersRequest {
            subaccount: subaccount.to_string(),
            market_id: Some(3),
            cursor: None,
            page_size: Some(10),
            sort: crate::http::DeepXAccountSortOrder::Descending,
        };
        let trades_request = DeepXPerpAccountTradesRequest {
            subaccount: subaccount.to_string(),
            order_id: None,
            market_id: Some(3),
            is_long: None,
            cursor: None,
            sort: crate::http::DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(10),
        };
        let positions_request = DeepXPerpPositionsRequest {
            subaccount: subaccount.to_string(),
            market_id: Some(3),
            only_closed: None,
            cursor: None,
            page_size: Some(10),
        };

        let orders = decode_account_page(
            "perp history orders",
            raw_account_fixture(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE),
            orders_request.page_size,
            |index, order| validate_perp_order_record(index, order, &orders_request),
        )
        .unwrap();
        let trades = decode_account_page(
            "perp account trades",
            raw_account_fixture(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE),
            trades_request.page_size,
            |index, trade| validate_perp_account_trade_record(index, trade, &trades_request),
        )
        .unwrap();
        let positions = decode_account_page(
            "perp positions",
            raw_account_fixture(PERP_POSITIONS_ACCOUNT_RESPONSE),
            positions_request.page_size,
            |index, position| validate_perp_position_record(index, position, &positions_request),
        )
        .unwrap();

        assert_eq!(orders.items.len(), 3);
        assert_eq!(trades.items.len(), 3);
        assert_eq!(positions.items.len(), 2);
        assert_eq!(orders.items[0].order_id, trades.items[0].order_id);
        assert_eq!(
            positions.items[0].pnl,
            Decimal::new(-4_018_472_999_999_176, 16)
        );
    }

    #[rstest]
    fn captured_account_pages_reject_scope_filter_and_page_size_mismatches() {
        let subaccount = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
        let orders_request = DeepXPerpHistoryOrdersRequest {
            subaccount: "0x1111111111111111111111111111111111111111".to_string(),
            market_id: Some(3),
            cursor: None,
            page_size: Some(10),
            sort: crate::http::DeepXAccountSortOrder::Descending,
        };
        let order_error = decode_account_page::<DeepXPerpOrderRecord, _>(
            "perp history orders",
            raw_account_fixture(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE),
            orders_request.page_size,
            |index, order| validate_perp_order_record(index, order, &orders_request),
        )
        .unwrap_err();
        assert!(matches!(
            order_error,
            DeepXHttpError::InvalidHistoryResponse { .. }
        ));

        let trades_request = DeepXPerpAccountTradesRequest {
            subaccount: subaccount.to_string(),
            order_id: None,
            market_id: Some(3),
            is_long: None,
            cursor: None,
            sort: crate::http::DeepXAccountSortOrder::Descending,
            start_ms: Some(1_789_445_193_758),
            end_ms: None,
            page_size: Some(10),
        };
        let trade_error = decode_account_page::<DeepXPerpAccountTradeRecord, _>(
            "perp account trades",
            raw_account_fixture(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE),
            trades_request.page_size,
            |index, trade| validate_perp_account_trade_record(index, trade, &trades_request),
        )
        .unwrap_err();
        assert!(matches!(
            trade_error,
            DeepXHttpError::InvalidHistoryResponse { .. }
        ));

        let positions_request = DeepXPerpPositionsRequest {
            subaccount: subaccount.to_string(),
            market_id: Some(3),
            only_closed: Some(true),
            cursor: None,
            page_size: Some(10),
        };
        let position_error = decode_account_page::<DeepXPerpPositionRecord, _>(
            "perp positions",
            raw_account_fixture(PERP_POSITIONS_ACCOUNT_RESPONSE),
            positions_request.page_size,
            |index, position| validate_perp_position_record(index, position, &positions_request),
        )
        .unwrap_err();
        assert!(matches!(
            position_error,
            DeepXHttpError::InvalidHistoryResponse { .. }
        ));

        let oversized_error = decode_account_page::<DeepXPerpOrderRecord, _>(
            "perp history orders",
            raw_account_fixture(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE),
            Some(2),
            |_, _| Ok(()),
        )
        .unwrap_err();
        assert!(matches!(
            oversized_error,
            DeepXHttpError::InvalidHistoryResponse { .. }
        ));

        let mut duplicate: serde_json::Value =
            serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        duplicate["data"]["items"][1] = duplicate["data"]["items"][0].clone();
        let duplicate_page =
            serde_json::from_value::<DeepXRawAccountPage>(duplicate["data"].clone()).unwrap();
        let mut trade_ids = HashSet::new();
        let duplicate_error = decode_account_page::<DeepXPerpAccountTradeRecord, _>(
            "perp account trades",
            duplicate_page,
            Some(10),
            |index, trade| {
                validate_perp_account_trade_record(
                    index,
                    trade,
                    &DeepXPerpAccountTradesRequest {
                        subaccount: subaccount.to_string(),
                        order_id: None,
                        market_id: Some(3),
                        is_long: None,
                        cursor: None,
                        sort: crate::http::DeepXAccountSortOrder::Descending,
                        start_ms: None,
                        end_ms: None,
                        page_size: Some(10),
                    },
                )?;
                validate_unique_account_identity(
                    "perp account trades",
                    index,
                    &mut trade_ids,
                    trade.id,
                    "trade ID",
                )
            },
        )
        .unwrap_err();
        assert!(matches!(
            duplicate_error,
            DeepXHttpError::InvalidHistoryResponse { .. }
        ));
    }

    #[tokio::test]
    async fn typed_account_readers_validate_captured_transport_responses() {
        let app = Router::new()
            .route(
                PERP_OPEN_ORDERS_PATH,
                get(|| async { PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE }),
            )
            .route(
                PERP_HISTORY_ORDERS_PATH,
                get(|| async { PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE }),
            )
            .route(
                PERP_ACCOUNT_TRADES_PATH,
                get(|| async { PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE }),
            )
            .route(
                PERP_FUNDING_FEE_PATH,
                get(|| async { PERP_FUNDING_FEES_ACCOUNT_RESPONSE }),
            )
            .route(
                PERP_POSITIONS_PATH,
                get(|| async { PERP_POSITIONS_ACCOUNT_RESPONSE }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let subaccount = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";

        let open_orders = client
            .get_perp_open_orders(&DeepXPerpOpenOrdersRequest {
                subaccount: subaccount.to_string(),
                market_id: Some(3),
                is_long: None,
                cursor: None,
                page_size: Some(10),
                sort: crate::http::DeepXAccountSortOrder::Descending,
            })
            .await
            .unwrap();
        let orders = client
            .get_perp_history_orders(&DeepXPerpHistoryOrdersRequest {
                subaccount: subaccount.to_string(),
                market_id: Some(3),
                cursor: None,
                page_size: Some(10),
                sort: crate::http::DeepXAccountSortOrder::Descending,
            })
            .await
            .unwrap();
        assert_eq!(open_orders.items, orders.items);
        let trades = client
            .get_perp_account_trades(&DeepXPerpAccountTradesRequest {
                subaccount: subaccount.to_string(),
                order_id: None,
                market_id: Some(3),
                is_long: None,
                cursor: None,
                sort: crate::http::DeepXAccountSortOrder::Descending,
                start_ms: None,
                end_ms: None,
                page_size: Some(10),
            })
            .await
            .unwrap();
        let funding_fees = client
            .get_perp_funding_fees(&DeepXPerpFundingFeeRequest {
                subaccount: subaccount.to_string(),
                market_id: Some(3),
                start_ms: None,
                end_ms: None,
                cursor: None,
                page_size: Some(10),
            })
            .await
            .unwrap();
        let positions = client
            .get_perp_positions(&DeepXPerpPositionsRequest {
                subaccount: subaccount.to_string(),
                market_id: Some(3),
                only_closed: None,
                cursor: None,
                page_size: Some(10),
            })
            .await
            .unwrap();

        assert_eq!(orders.items.len(), 3);
        assert_eq!(trades.items.len(), 3);
        assert_eq!(funding_fees.items.len(), 4);
        assert_eq!(positions.items.len(), 2);
        server.abort();
    }

    #[tokio::test]
    async fn typed_account_state_readers_validate_captured_transport_responses() {
        let app = Router::new()
            .route(
                WALLET_SUBACCOUNTS_PATH,
                get(|| async { WALLET_SUBACCOUNTS_RESPONSE }),
            )
            .route(
                SUBACCOUNT_INFO_PATH,
                get(|| async { SUBACCOUNT_INFO_RESPONSE }),
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
                get(
                    |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                        assert_eq!(query.len(), 1);
                        assert_eq!(
                            query["address"],
                            "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
                        );
                        SUBACCOUNT_MARGIN_RATIO_RESPONSE
                    },
                ),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let wallet = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
        let subaccount = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";

        let subaccounts = client.get_wallet_subaccounts(wallet).await.unwrap();
        let profile = client
            .get_subaccount_info(subaccount, Some(wallet))
            .await
            .unwrap();
        let balances = client.get_subaccount_balances(subaccount).await.unwrap();
        let equity = client.get_subaccount_equity(subaccount).await.unwrap();
        let margin = client
            .get_subaccount_margin_ratio(subaccount)
            .await
            .unwrap();

        assert_eq!(subaccounts.wallet(), wallet);
        assert!(
            subaccounts
                .addresses
                .iter()
                .any(|value| value == subaccount)
        );
        assert_eq!(profile.authority, wallet);
        assert_eq!(balances.assets[0].balance, Decimal::new(999_783_258, 6));
        assert_eq!(equity.unrealized_pnl_usd, Decimal::new(-273, 2));
        assert_eq!(margin.collateral, Decimal::new(97_096, 2));
        assert_eq!(margin.margin_required, Decimal::ZERO);
        assert_eq!(margin.margin_ratio, None);
        server.abort();
    }

    #[tokio::test]
    async fn all_subaccounts_reader_preserves_query_and_validates_fixture() {
        let app = Router::new().route(
            ALL_SUBACCOUNTS_PATH,
            get(|RawQuery(query): RawQuery| async move {
                assert_eq!(query.as_deref(), Some("pageSize=5"));
                ALL_SUBACCOUNTS_RESPONSE
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();

        let page = client
            .get_all_subaccounts(&all_subaccounts_request())
            .await
            .unwrap();

        assert_eq!(page.items.len(), 5);
        assert!(page.has_next);
        assert_eq!(page.items[0].name, "Subaccount11");
        assert_eq!(page.items[0].status, None);
        assert_eq!(page.items[0].height, 181_249_057);
        assert_eq!(page.items[0].created_at, "2026-09-11T08:16:49.885Z");
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
            serde_json::from_str(ALL_SUBACCOUNTS_RESPONSE).unwrap();
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
            Err(DeepXHttpError::InvalidHistoryResponse {
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
        let mut response: serde_json::Value =
            serde_json::from_str(ALL_SUBACCOUNTS_RESPONSE).unwrap();
        let mut request = all_subaccounts_request();
        match mutation {
            "duplicate" => {
                response["data"]["items"][1]["subaccount"] =
                    response["data"]["items"][0]["subaccount"].clone();
            }
            "ordering" => response["data"]["items"].as_array_mut().unwrap().swap(0, 1),
            "oversized" => request.page_size = Some(4),
            _ => unreachable!(),
        }
        let page: DeepXRawAccountPage = serde_json::from_value(response["data"].clone()).unwrap();
        let mut subaccounts = HashSet::new();
        let mut previous = None;

        assert!(
            decode_all_subaccounts_page(page, &request, &mut subaccounts, &mut previous).is_err()
        );
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn all_subaccount_collector_forwards_cursor_and_rejects_duplicates(
        #[case] duplicate: bool,
    ) {
        let first: serde_json::Value = serde_json::from_str(ALL_SUBACCOUNTS_RESPONSE).unwrap();
        let cursor = first["data"]["nextCursor"].as_str().unwrap().to_string();
        let mut terminal = first.clone();
        let mut item = first["data"]["items"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .clone();
        if !duplicate {
            item["subaccount"] = json!("0x1111111111111111111111111111111111111111");
            item["createdAt"] = json!("2026-09-08T10:03:25.573Z");
        }
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

        if duplicate {
            assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidHistoryResponse {
                    endpoint: "all subaccounts",
                    ..
                })
            ));
        } else {
            let pages = result.unwrap();
            assert_eq!(pages.len(), 2);
            assert!(!pages[1].has_next);
        }
    }

    #[tokio::test]
    async fn typed_user_stats_and_liquidation_price_validate_captured_responses() {
        let app = Router::new()
            .route(
                USER_STATS_PATH,
                get(
                    |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                        assert_eq!(query.len(), 1);
                        assert_eq!(
                            query["address"],
                            "0x781ed35b167068c93dfadab41dfb680edaca4e50"
                        );
                        WALLET_USER_STATS_RESPONSE
                    },
                ),
            )
            .route(
                PERP_LIQUIDATION_PRICE_PATH,
                get(
                    |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                        assert_eq!(query.len(), 2);
                        assert_eq!(
                            query["address"],
                            "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
                        );
                        assert_eq!(query["marketId"], "3");
                        assert!(!query.contains_key("name"));
                        PERP_LIQUIDATION_PRICE_RESPONSE
                    },
                ),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let stats = client
            .get_user_stats("0x781ed35b167068c93dfadab41dfb680edaca4e50")
            .await
            .unwrap();
        let price = client
            .get_perp_liquidation_price(&DeepXPerpLiquidationPriceRequest {
                subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
                market_name: None,
                market_id: Some(3),
            })
            .await
            .unwrap();

        assert_eq!(stats.subaccounts.len(), 4);
        assert_eq!(stats.if_staked_quote_asset_amount, Decimal::ZERO);
        assert_eq!(price.market_name, "ETH-USDC");
        assert_eq!(price.liquidate_price, None);
        server.abort();
    }

    #[tokio::test]
    async fn typed_quota_summary_validates_captured_response() {
        let app = Router::new().route(
            QUOTA_SUMMARY_PATH,
            get(
                |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    assert_eq!(query.len(), 1);
                    assert_eq!(
                        query["wallet"],
                        "0x781ed35b167068c93dfadab41dfb680edaca4e50"
                    );
                    WALLET_QUOTA_SUMMARY_RESPONSE
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let summary = client
            .get_quota_summary("0x781ed35b167068c93dfadab41dfb680edaca4e50")
            .await
            .unwrap();

        assert_eq!(summary.subaccount_count, 4);
        assert_eq!(summary.spot_volume_usd, Decimal::ZERO);
        assert_eq!(summary.quota_earned, 2_970);
        assert_eq!(summary.quota_pending, 2_970);
        server.abort();
    }

    #[tokio::test]
    async fn quota_history_reader_validates_captured_purchase() {
        let app = Router::new().route(
            QUOTA_HISTORY_PATH,
            get(|RawQuery(query): RawQuery| async move {
                assert_eq!(
                    query.as_deref(),
                    Some("wallet=0x0a40c3efbc3b3bebdf1fb6f0f8c612eb336b25ae&limit=5")
                );
                QUOTA_HISTORY_RESPONSE
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(2), None).unwrap();
        let page = client
            .get_quota_history(&quota_history_request())
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].quota, 10_000);
        assert_eq!(page.items[0].event_index, 49);
    }

    #[rstest]
    #[case("id")]
    #[case("owner")]
    #[case("type-filter")]
    #[case("buyer-missing")]
    #[case("buyer-filter")]
    #[case("quota")]
    #[case("block")]
    #[case("hash")]
    #[case("hash-type")]
    #[case("timestamp")]
    fn quota_history_rejects_invalid_record_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXQuotaHistoryRecord>> =
            serde_json::from_str(QUOTA_HISTORY_RESPONSE).unwrap();
        let mut record = response.data.items[0].clone();
        let mut request = quota_history_request();
        match mutation {
            "id" => record.id.clear(),
            "owner" => {
                record.owner_address = "0x1111111111111111111111111111111111111111".to_string()
            }
            "type-filter" => request.history_type = Some(DeepXQuotaHistoryType::Free),
            "buyer-missing" => record.buyer_address = None,
            "buyer-filter" => {
                request.buyer_address =
                    Some("0x1111111111111111111111111111111111111111".to_string())
            }
            "quota" => record.quota = 0,
            "block" => record.block_number = 0,
            "hash" => record.tx_hash = "0x12".to_string(),
            "hash-type" => record.tx_hash_type.clear(),
            "timestamp" => record.created_at = "invalid".to_string(),
            _ => unreachable!(),
        }
        assert!(validate_quota_history_record(0, &record, &request).is_err());
    }

    #[rstest]
    #[case("duplicate-id")]
    #[case("duplicate-event")]
    #[case("ordering")]
    #[case("oversized")]
    fn quota_history_rejects_invalid_page_semantics(#[case] mutation: &str) {
        let mut response: serde_json::Value = serde_json::from_str(QUOTA_HISTORY_RESPONSE).unwrap();
        let mut second = response["data"]["items"][0].clone();
        second["id"] = json!("purchase:other");
        second["blockNumber"] = json!(181_248_893);
        second["eventIndex"] = json!(48);
        second["createdAt"] = json!("2026-09-11T08:16:37.475Z");
        match mutation {
            "duplicate-id" => second["id"] = response["data"]["items"][0]["id"].clone(),
            "duplicate-event" => {
                second["blockNumber"] = response["data"]["items"][0]["blockNumber"].clone();
                second["eventIndex"] = response["data"]["items"][0]["eventIndex"].clone();
            }
            "ordering" => second["createdAt"] = json!("2026-09-11T08:16:39.475Z"),
            "oversized" => {}
            _ => unreachable!(),
        }
        response["data"]["items"]
            .as_array_mut()
            .unwrap()
            .push(second);
        let page: DeepXRawAccountPage = serde_json::from_value(response["data"].clone()).unwrap();
        let mut request = quota_history_request();
        if mutation == "oversized" {
            request.limit = Some(1);
        }
        assert!(
            decode_quota_history_page(
                page,
                &request,
                &mut QuotaHistoryIdentities::default(),
                &mut None,
            )
            .is_err()
        );
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn quota_history_collector_forwards_cursor_and_rejects_duplicates(
        #[case] duplicate: bool,
    ) {
        let mut first: serde_json::Value = serde_json::from_str(QUOTA_HISTORY_RESPONSE).unwrap();
        first["data"]["hasNext"] = json!(true);
        first["data"]["nextCursor"] = json!("next-page");
        let mut terminal = first.clone();
        let mut item = first["data"]["items"][0].clone();
        if !duplicate {
            item["id"] = json!("purchase:other");
            item["blockNumber"] = json!(181_248_893);
            item["eventIndex"] = json!(48);
            item["createdAt"] = json!("2026-09-11T08:16:37.475Z");
        }
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
        if duplicate {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap().len(), 2);
        }
    }

    #[tokio::test]
    async fn quota_summary_rejects_invalid_wallet_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();

        let error = client.get_quota_summary("invalid").await.unwrap_err();

        assert!(matches!(error, DeepXHttpError::InvalidRequest(_)));
    }

    #[rstest]
    fn quota_summary_accepts_absent_trade_times_for_empty_volume() {
        let mut response: serde_json::Value =
            serde_json::from_str(WALLET_QUOTA_SUMMARY_RESPONSE).unwrap();
        for field in [
            "firstTradeAt",
            "firstTradeTsMs",
            "lastTradeAt",
            "lastTradeTsMs",
            "updatedAt",
        ] {
            response["data"].as_object_mut().unwrap().remove(field);
        }
        response["data"]["perpVolumeUsd"] = json!("0");
        response["data"]["totalVolumeUsd"] = json!("0");
        response["data"]["quotaEarned"] = json!(0);
        response["data"]["quotaPending"] = json!(0);
        let summary: DeepXQuotaSummary = serde_json::from_value(response["data"].clone()).unwrap();

        validate_quota_summary(&summary, "0x781ed35b167068c93dfadab41dfb680edaca4e50").unwrap();
    }

    #[rstest]
    #[case("owner")]
    #[case("spot-volume")]
    #[case("perp-volume")]
    #[case("total-volume")]
    #[case("missing-first-ms")]
    #[case("missing-last")]
    #[case("first-mismatch")]
    #[case("inverted-time")]
    #[case("updated-at")]
    fn quota_summary_rejects_invalid_responses(#[case] mutation: &str) {
        let mut response: serde_json::Value =
            serde_json::from_str(WALLET_QUOTA_SUMMARY_RESPONSE).unwrap();
        match mutation {
            "owner" => {
                response["data"]["owner"] = json!("0x1111111111111111111111111111111111111111")
            }
            "spot-volume" => response["data"]["spotVolumeUsd"] = json!("-1"),
            "perp-volume" => response["data"]["perpVolumeUsd"] = json!("-1"),
            "total-volume" => response["data"]["totalVolumeUsd"] = json!("1"),
            "missing-first-ms" => {
                response["data"]
                    .as_object_mut()
                    .unwrap()
                    .remove("firstTradeTsMs");
            }
            "missing-last" => {
                response["data"]
                    .as_object_mut()
                    .unwrap()
                    .remove("lastTradeAt");
                response["data"]
                    .as_object_mut()
                    .unwrap()
                    .remove("lastTradeTsMs");
            }
            "first-mismatch" => response["data"]["firstTradeTsMs"] = json!(1),
            "inverted-time" => {
                response["data"]["firstTradeAt"] = response["data"]["lastTradeAt"].clone();
                response["data"]["firstTradeTsMs"] = response["data"]["lastTradeTsMs"].clone();
                response["data"]["lastTradeAt"] = json!("2026-09-15T04:04:15.646Z");
                response["data"]["lastTradeTsMs"] = json!(1_789_445_055_646_u64);
            }
            "updated-at" => response["data"]["updatedAt"] = json!("invalid"),
            _ => unreachable!(),
        }
        let summary: DeepXQuotaSummary = serde_json::from_value(response["data"].clone()).unwrap();

        assert!(matches!(
            validate_quota_summary(&summary, "0x781ed35b167068c93dfadab41dfb680edaca4e50"),
            Err(DeepXHttpError::InvalidAccountResponse {
                endpoint: "quota summary",
                ..
            })
        ));
    }

    #[rstest]
    #[case("address")]
    #[case("duplicate")]
    #[case("current-count")]
    #[case("created-count")]
    #[case("negative-amount")]
    fn user_stats_reject_invalid_responses(#[case] mutation: &str) {
        let mut response: serde_json::Value =
            serde_json::from_str(WALLET_USER_STATS_RESPONSE).unwrap();
        match mutation {
            "address" => response["data"]["subaccounts"][0] = json!("invalid"),
            "duplicate" => {
                response["data"]["subaccounts"][1] = response["data"]["subaccounts"][0].clone();
            }
            "current-count" => response["data"]["numberOfSubAccounts"] = json!(3),
            "created-count" => response["data"]["numberOfSubAccountsCreated"] = json!(3),
            "negative-amount" => response["data"]["ifStakedQuoteAssetAmount"] = json!(-1),
            _ => unreachable!(),
        }
        let stats: DeepXUserStats = serde_json::from_value(response["data"].clone()).unwrap();

        assert!(matches!(
            validate_user_stats(&stats),
            Err(DeepXHttpError::InvalidAccountResponse {
                endpoint: "user stats",
                ..
            })
        ));
    }

    #[rstest]
    #[case("address")]
    #[case("market-id")]
    #[case("market-name")]
    #[case("price")]
    fn liquidation_price_rejects_invalid_responses(#[case] mutation: &str) {
        let mut response: serde_json::Value =
            serde_json::from_str(PERP_LIQUIDATION_PRICE_RESPONSE).unwrap();
        match mutation {
            "address" => {
                response["data"]["address"] = json!("0x1111111111111111111111111111111111111111")
            }
            "market-id" => response["data"]["marketId"] = json!(4),
            "market-name" => response["data"]["marketName"] = json!(""),
            "price" => response["data"]["liquidatePrice"] = json!(0),
            _ => unreachable!(),
        }
        let price: DeepXPerpLiquidationPrice =
            serde_json::from_value(response["data"].clone()).unwrap();
        let request = DeepXPerpLiquidationPriceRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_name: None,
            market_id: Some(3),
        };

        assert!(matches!(
            validate_perp_liquidation_price(&price, &request),
            Err(DeepXHttpError::InvalidAccountResponse {
                endpoint: "perp liquidation price",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn typed_balance_changes_preserve_exact_captured_records_and_query() {
        let app = Router::new().route(
            BALANCE_CHANGES_PATH,
            get(
                |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    assert_eq!(query.len(), 5);
                    assert_eq!(
                        query["wallet"],
                        "0x781ed35b167068c93dfadab41dfb680edaca4e50"
                    );
                    assert_eq!(query["startTime"], "1789498000000");
                    assert_eq!(query["endTime"], "1789523000000");
                    assert_eq!(query["changeType"], "FUNDING_FEE,SETTLEMENT");
                    assert_eq!(query["pageSize"], "2");
                    assert!(!query.contains_key("user"));
                    WALLET_BALANCE_CHANGES_RESPONSE
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let page = client
            .get_balance_changes(&balance_changes_request())
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].balance_change, Decimal::new(-17, 5));
        assert_eq!(
            page.items[0].change_type,
            crate::http::DeepXBalanceChangeType::FundingFee
        );
        assert_eq!(
            page.items[0].position.as_ref().unwrap().owner,
            "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
        );
        assert_eq!(page.items[1].balance_change, Decimal::new(-10_235_558, 6));
        assert_eq!(
            page.items[1].tx_hash_type.as_deref(),
            Some("EXTRINSIC_HASH")
        );
        assert!(page.has_next);
        assert!(page.next_cursor.is_some());
        server.abort();
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn balance_change_collection_is_bounded_and_rejects_cross_page_duplicates(
        #[case] duplicate: bool,
    ) {
        let first: serde_json::Value =
            serde_json::from_str(WALLET_BALANCE_CHANGES_RESPONSE).unwrap();
        let mut terminal = first.clone();
        terminal["data"]["items"] = json!([first["data"]["items"][1].clone()]);
        terminal["data"]["hasNext"] = json!(false);
        terminal["data"]["nextCursor"] = serde_json::Value::Null;
        if !duplicate {
            terminal["data"]["items"][0]["id"] = json!(1_867_287_170_002_001_u64);
        }
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

        if duplicate {
            assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidHistoryResponse {
                    endpoint: "balance changes",
                    ..
                })
            ));
        } else {
            let pages = result.unwrap();
            assert_eq!(pages.len(), 2);
            assert_eq!(pages.iter().map(|page| page.items.len()).sum::<usize>(), 3);
        }
        server.abort();
    }

    #[tokio::test]
    async fn typed_liquidation_records_preserve_exact_captured_records_and_query() {
        let app = Router::new().route(
            LIQUIDATION_RECORDS_PATH,
            get(
                |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    assert_eq!(query.len(), 3);
                    assert_eq!(
                        query["wallet"],
                        "0x781ed35b167068c93dfadab41dfb680edaca4e50"
                    );
                    assert_eq!(query["sort"], "DESC");
                    assert_eq!(query["pageSize"], "5");
                    assert!(!query.contains_key("subaccount"));
                    assert!(!query.contains_key("liquidationType"));
                    WALLET_LIQUIDATION_RECORDS_RESPONSE
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let page = client
            .get_liquidation_records(&liquidation_records_request())
            .await
            .unwrap();

        assert_eq!(page.items.len(), 5);
        assert_eq!(page.items[0].id, 1_763);
        assert_eq!(page.items[0].margin_shortage, 2_771);
        assert_eq!(page.items[0].margin_freed, 16_917);
        assert_eq!(page.items[0].liquidator_fee, Some(16_911));
        assert_eq!(page.items[0].liquidate_base_amount, Some(20_000_000));
        assert_eq!(page.items[0].oracle_price, Some(84_566_331));
        assert!(page.has_next);
        assert_eq!(
            page.next_cursor.as_deref(),
            Some("MTc3OTg5NTgyOTI0MDo1MDcyNjAwODoxMDU6MTc1NA")
        );
        server.abort();
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn liquidation_record_collection_forwards_cursor_and_rejects_duplicates(
        #[case] duplicate: bool,
    ) {
        let first: serde_json::Value =
            serde_json::from_str(WALLET_LIQUIDATION_RECORDS_RESPONSE).unwrap();
        let cursor = first["data"]["nextCursor"].as_str().unwrap().to_string();
        let mut terminal = first.clone();
        terminal["data"]["items"] = json!([first["data"]["items"][4].clone()]);
        terminal["data"]["hasNext"] = json!(false);
        terminal["data"]["nextCursor"] = serde_json::Value::Null;
        if !duplicate {
            terminal["data"]["items"][0]["id"] = json!(1_747);
            terminal["data"]["items"][0]["createdAt"] = json!("2026-05-27T15:30:29.000Z");
        }
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

        if duplicate {
            assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidHistoryResponse {
                    endpoint: "liquidation records",
                    ..
                })
            ));
        } else {
            let pages = result.unwrap();
            assert_eq!(pages.len(), 2);
            assert_eq!(pages.iter().map(|page| page.items.len()).sum::<usize>(), 6);
        }
        server.abort();
    }

    #[tokio::test]
    async fn invalid_liquidation_record_request_fails_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let mut request = liquidation_records_request();
        request.wallet = None;

        assert!(matches!(
            client.get_liquidation_records_raw(&request).await,
            Err(DeepXHttpError::InvalidRequest(_))
        ));
    }

    #[rstest]
    #[case("zero-id")]
    #[case("zero-height")]
    #[case("invalid-target")]
    #[case("foreign-target")]
    #[case("filter-mismatch")]
    #[case("zero-market")]
    #[case("bankruptcy")]
    #[case("invalid-tx-hash")]
    #[case("malformed-detail")]
    #[case("mismatched-detail")]
    #[case("malformed-canceled-orders")]
    #[case("invalid-timestamp")]
    #[case("wrong-order")]
    fn liquidation_records_reject_invalid_response_semantics(#[case] mutation: &str) {
        let response: serde_json::Value =
            serde_json::from_str(WALLET_LIQUIDATION_RECORDS_RESPONSE).unwrap();
        let mut record: DeepXLiquidationRecord =
            serde_json::from_value(response["data"]["items"][0].clone()).unwrap();
        let mut request = liquidation_records_request();
        let mut previous = None;
        match mutation {
            "zero-id" => record.id = 0,
            "zero-height" => record.height = 0,
            "invalid-target" => record.target_account = "invalid".to_string(),
            "foreign-target" => {
                request.subaccount = Some("0x1111111111111111111111111111111111111111".to_string());
                request.wallet = None;
            }
            "filter-mismatch" => {
                request.liquidation_types = vec![crate::http::DeepXLiquidationType::LiquidateSpot];
            }
            "zero-market" => record.market_index = Some(0),
            "bankruptcy" => {
                record.liquidation_type = crate::http::DeepXLiquidationType::PerpBankruptcy;
                record.liquidation_detail = r#"{"perpBankruptcy":{}}"#.to_string();
            }
            "invalid-tx-hash" => record.tx_hash = Some("0x1234".to_string()),
            "malformed-detail" => record.liquidation_detail = "{".to_string(),
            "mismatched-detail" => {
                record.liquidation_detail = r#"{"liquidateSpot":{}}"#.to_string();
            }
            "malformed-canceled-orders" => record.canceled_order_ids = "[]".to_string(),
            "invalid-timestamp" => record.created_at = "invalid".to_string(),
            "wrong-order" => {
                previous = Some("2026-05-27T15:00:00Z".parse().unwrap());
            }
            _ => unreachable!(),
        }

        assert!(matches!(
            validate_liquidation_record(0, &record, &request, &mut previous),
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "liquidation records",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn typed_wallet_funding_fees_preserve_exact_captured_records_and_query() {
        let app = Router::new().route(
            WALLET_FUNDING_FEE_PATH,
            get(
                |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    assert_eq!(query.len(), 5);
                    assert_eq!(
                        query["address"],
                        "0x781ed35b167068c93dfadab41dfb680edaca4e50"
                    );
                    assert_eq!(query["marketId"], "3");
                    assert_eq!(query["start"], "1789498000000");
                    assert_eq!(query["end"], "1789523000000");
                    assert_eq!(query["pageSize"], "2");
                    assert!(!query.contains_key("name"));
                    WALLET_FUNDING_FEES_RESPONSE
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let page = client
            .get_wallet_funding_fees(&wallet_funding_fee_request())
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].position_size, Decimal::new(238, 3));
        assert_eq!(page.items[0].fee, Decimal::new(-17, 5));
        assert_eq!(
            page.items[1].fee_rate,
            "0.000469620169921581".parse::<Decimal>().unwrap()
        );
        assert!(page.has_next);
        assert!(page.next_cursor.is_some());
        server.abort();
    }

    #[tokio::test]
    async fn perp_wallet_orders_normalize_global_cursor_and_exact_query() {
        let app = Router::new().route(
            PERP_WALLET_ORDERS_PATH,
            get(
                |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    assert_eq!(query.len(), 3);
                    assert_eq!(
                        query["address"],
                        "0x781ed35b167068c93dfadab41dfb680edaca4e50"
                    );
                    assert_eq!(query["sort"], "DESC");
                    assert_eq!(query["pageSize"], "5");
                    assert!(!query.contains_key("wallet"));
                    PERP_WALLET_ORDERS_RESPONSE
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();

        let page = client
            .get_perp_wallet_orders(&perp_wallet_orders_request())
            .await
            .unwrap();

        assert_eq!(page.markets.len(), 1);
        assert_eq!(page.markets[0].subaccounts.len(), 2);
        assert_eq!(
            page.markets[0]
                .subaccounts
                .iter()
                .map(|group| group.orders.items.len())
                .sum::<usize>(),
            5
        );
        assert!(page.has_next);
        assert_eq!(
            page.next_cursor,
            page.markets[0].subaccounts[0].orders.next_cursor
        );
        assert_eq!(
            page.markets[0].subaccounts[0].orders.items[0].avg_fill_price,
            Some("2403.9885999999997".parse::<Decimal>().unwrap())
        );
    }

    #[tokio::test]
    async fn perp_wallet_orders_preserve_zero_size_legacy_history() {
        let mut response: serde_json::Value =
            serde_json::from_str(PERP_WALLET_ORDERS_RESPONSE).unwrap();
        let subaccount = "0x7af9d31794004cc061b239ac098e998e47095de1";
        let orders = &mut response["data"][0]["subaccounts"];
        let mut order = orders[0]["orders"]["items"][0].clone();
        response["data"][0]["marketId"] = json!(4);
        response["data"][0]["marketName"] = json!("SOL-USDC");
        order["marketId"] = json!(4);
        order["owner"] = json!(subaccount);
        order["orderId"] = json!("13");
        order["size"] = json!(0.0);
        order["sizeFilled"] = json!(0.0);
        order["sizeRemain"] = json!(0.0);
        order["price"] = json!(0.0);
        order["avgFillPrice"] = serde_json::Value::Null;
        order["leverage"] = json!(25);
        order["slippage"] = json!(0.0);
        order["status"] = json!("Filled");
        response["data"][0]["subaccounts"] = json!([{
            "subaccount": subaccount,
            "orders": {
                "items": [order],
                "nextCursor": null,
                "hasNext": false
            }
        }]);
        let app = Router::new().route(
            PERP_WALLET_ORDERS_PATH,
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();

        let page = client
            .get_perp_wallet_orders(&perp_wallet_orders_request())
            .await
            .unwrap();
        let order = &page.markets[0].subaccounts[0].orders.items[0];

        assert_eq!(order.market_id, 4);
        assert_eq!(order.owner, subaccount);
        assert_eq!(order.order_id, "13");
        assert_eq!(order.size, Decimal::ZERO);
        assert_eq!(order.size_filled, Decimal::ZERO);
        assert_eq!(order.size_remain, Decimal::ZERO);
        assert_eq!(order.price, Decimal::ZERO);
        assert_eq!(order.avg_fill_price, None);
        assert!(
            validate_perp_order_record_scope("perp history orders", 0, order, subaccount, Some(4),)
                .is_err()
        );
        let mut negative_size = order.clone();
        negative_size.size = Decimal::NEGATIVE_ONE;
        assert!(
            validate_perp_wallet_order_record_scope(
                "perp wallet orders",
                0,
                &negative_size,
                subaccount,
                Some(4),
            )
            .is_err()
        );
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
    async fn perp_wallet_orders_reject_invalid_group_semantics(#[case] mutation: &str) {
        let mut response: serde_json::Value =
            serde_json::from_str(PERP_WALLET_ORDERS_RESPONSE).unwrap();
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
        let app = Router::new().route(
            PERP_WALLET_ORDERS_PATH,
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();

        assert!(
            client
                .get_perp_wallet_orders(&perp_wallet_orders_request())
                .await
                .is_err()
        );
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn perp_wallet_order_collector_forwards_cursor_and_rejects_duplicates(
        #[case] duplicate: bool,
    ) {
        let first: serde_json::Value = serde_json::from_str(PERP_WALLET_ORDERS_RESPONSE).unwrap();
        let cursor = first["data"][0]["subaccounts"][0]["orders"]["nextCursor"]
            .as_str()
            .unwrap()
            .to_string();
        let mut terminal = first.clone();
        let mut item = first["data"][0]["subaccounts"][1]["orders"]["items"][0].clone();
        if !duplicate {
            item["orderId"] = json!("32");
            item["createTime"] = json!("2026-05-29T08:15:11.756Z");
            item["updatedTime"] = json!("2026-05-29T08:15:11.756Z");
        }
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

        let result = client
            .get_perp_wallet_order_pages(&perp_wallet_orders_request(), 2)
            .await;

        if duplicate {
            assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidHistoryResponse {
                    endpoint: "perp wallet orders",
                    ..
                })
            ));
        } else {
            let pages = result.unwrap();
            assert_eq!(pages.len(), 2);
            assert_eq!(
                pages[1].markets[0].subaccounts[0].orders.items[0].order_id,
                "32"
            );
            assert!(!pages[1].has_next);
        }
    }

    #[tokio::test]
    async fn perp_wallet_trades_preserve_group_cursors_zero_size_and_exact_query() {
        let app = Router::new().route(
            PERP_WALLET_TRADES_PATH,
            get(
                |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    assert_eq!(query.len(), 3);
                    assert_eq!(
                        query["address"],
                        "0x781ed35b167068c93dfadab41dfb680edaca4e50"
                    );
                    assert_eq!(query["sort"], "DESC");
                    assert_eq!(query["pageSize"], "5");
                    assert!(!query.contains_key("wallet"));
                    PERP_WALLET_TRADES_RESPONSE
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();

        let groups = client
            .get_perp_wallet_trades(&perp_wallet_trades_request())
            .await
            .unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].market_id, 3);
        assert_eq!(groups[1].market_name, "SOL-USDC");
        assert_eq!(groups[0].subaccounts.len(), 3);
        assert_eq!(groups[1].subaccounts.len(), 2);
        assert_eq!(groups[1].subaccounts[0].trades.items[2].size, Decimal::ZERO);
        assert_ne!(
            groups[1].subaccounts[0].trades.next_cursor,
            groups[1].subaccounts[1].trades.next_cursor,
        );
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
    async fn perp_wallet_trades_reject_invalid_group_semantics(#[case] mutation: &str) {
        let mut response: serde_json::Value =
            serde_json::from_str(PERP_WALLET_TRADES_RESPONSE).unwrap();
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
                response["data"][0]["subaccounts"][2]["trades"]["nextCursor"] =
                    serde_json::Value::Null;
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
        let app = Router::new().route(
            PERP_WALLET_TRADES_PATH,
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();

        assert!(
            client
                .get_perp_wallet_trades(&perp_wallet_trades_request())
                .await
                .is_err()
        );
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn wallet_funding_fee_collection_rejects_cross_page_duplicates(#[case] duplicate: bool) {
        let first: serde_json::Value = serde_json::from_str(WALLET_FUNDING_FEES_RESPONSE).unwrap();
        let mut terminal = first.clone();
        terminal["data"]["items"] = json!([first["data"]["items"][1].clone()]);
        terminal["data"]["hasNext"] = json!(false);
        terminal["data"]["nextCursor"] = serde_json::Value::Null;
        if !duplicate {
            terminal["data"]["items"][0]["eventIdx"] = json!(22);
        }
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

        if duplicate {
            assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidHistoryResponse {
                    endpoint: "wallet funding fees",
                    ..
                })
            ));
        } else {
            let pages = result.unwrap();
            assert_eq!(pages.len(), 2);
            assert_eq!(pages.iter().map(|page| page.items.len()).sum::<usize>(), 3);
        }
        server.abort();
    }

    #[tokio::test]
    async fn hourly_unsettled_funding_decodes_exact_first_page_query() {
        let app = Router::new().route(
            HOURLY_UNSETTLED_FUNDING_PATH,
            get(
                |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    assert_eq!(query.len(), 4);
                    assert_eq!(
                        query["wallet"],
                        "0x781ed35b167068c93dfadab41dfb680edaca4e50"
                    );
                    assert_eq!(query["marketId"], "3");
                    assert_eq!(query["pageSize"], "5");
                    assert_eq!(query["sort"], "DESC");
                    assert!(!query.contains_key("subaccount"));
                    WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();

        let records = client
            .get_hourly_unsettled_funding(&hourly_unsettled_funding_request())
            .await
            .unwrap();

        assert_eq!(records.len(), 5);
        assert_eq!(records[0].signed_position_size_raw, 300_000_000_000_000_000);
        assert_eq!(records[0].payment_raw, -8_991);
        assert_eq!(records[4].boundary_timestamp_ms, 1_789_506_862_596);
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
    async fn hourly_unsettled_funding_collection_rejects_page_budget_exhaustion() {
        let app = Router::new().route(
            HOURLY_UNSETTLED_FUNDING_PATH,
            get(|| async { WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE }),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();

        let result = client
            .get_hourly_unsettled_funding_pages(&hourly_unsettled_funding_request(), 1)
            .await;

        assert!(matches!(
            result,
            Err(DeepXHttpError::PaginationLimitExceeded { max_pages: 1 })
        ));
    }

    #[tokio::test]
    async fn hourly_unsettled_funding_collection_rejects_cross_page_duplicate_event() {
        let first: serde_json::Value =
            serde_json::from_str(WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE).unwrap();
        let duplicate_event_id = first["data"][0]["boundaryEventId"].clone();
        let mut terminal: serde_json::Value =
            serde_json::from_str(WALLET_HOURLY_FUNDING_PAGE_2_RESPONSE).unwrap();
        terminal["data"].as_array_mut().unwrap().truncate(1);
        terminal["data"][0]["boundaryEventId"] = duplicate_event_id;
        let app = Router::new().route(
            HOURLY_UNSETTLED_FUNDING_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    async move {
                        Json(if query.contains_key("cursorTimestamp") {
                            terminal
                        } else {
                            first
                        })
                    }
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();

        let result = client
            .get_hourly_unsettled_funding_pages(&hourly_unsettled_funding_request(), 2)
            .await;

        assert!(matches!(
            result,
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "hourly unsettled funding",
                ..
            })
        ));
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
    #[tokio::test]
    async fn hourly_unsettled_funding_rejects_invalid_page(#[case] mutation: &str) {
        let mut response: serde_json::Value =
            serde_json::from_str(WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE).unwrap();
        match mutation {
            "arithmetic" => response["data"][0]["deltaIndexRaw"] = json!("1"),
            "market" => response["data"][0]["marketId"] = json!(4),
            "timestamp" => response["data"][0]["boundaryTimestampMs"] = json!(u64::MAX),
            "order" => response["data"].as_array_mut().unwrap().swap(0, 1),
            "duplicate-event" => {
                response["data"][1]["boundaryEventId"] =
                    response["data"][0]["boundaryEventId"].clone();
            }
            "zero-position" => response["data"][0]["signedPositionSizeRaw"] = json!("0"),
            "empty-event" => response["data"][0]["boundaryEventId"] = json!(" "),
            "oversized-page" => {
                let record = response["data"][4].clone();
                response["data"].as_array_mut().unwrap().push(record);
            }
            _ => unreachable!(),
        }
        let app = Router::new().route(
            HOURLY_UNSETTLED_FUNDING_PATH,
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(app).await, Some(5), None).unwrap();

        let result = client
            .get_hourly_unsettled_funding(&hourly_unsettled_funding_request())
            .await;

        assert!(matches!(
            result,
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "hourly unsettled funding",
                ..
            })
        ));
    }

    #[rstest]
    #[case("valid")]
    #[case("foreign-equity")]
    #[case("negative-margin")]
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
        let mut equity: serde_json::Value =
            serde_json::from_str(SUBACCOUNT_EQUITY_RESPONSE).unwrap();
        if scenario == "foreign-equity" {
            equity["data"]["subaccount"] = json!("0x1111111111111111111111111111111111111111");
        }
        let mut margin: serde_json::Value =
            serde_json::from_str(SUBACCOUNT_MARGIN_RATIO_RESPONSE).unwrap();
        if scenario == "negative-margin" {
            margin["data"]["marginRequired"] = json!(-1);
        }
        let app = Router::new()
            .route(
                WALLET_SUBACCOUNTS_PATH,
                get(move || {
                    let directory = directory.clone();
                    async move { Json(directory) }
                }),
            )
            .route(
                SUBACCOUNT_INFO_PATH,
                get(|| async { SUBACCOUNT_INFO_RESPONSE }),
            )
            .route(
                SUBACCOUNT_BALANCES_PATH,
                get(|| async { SUBACCOUNT_BALANCES_RESPONSE }),
            )
            .route(
                SUBACCOUNT_EQUITY_PATH,
                get(move || {
                    let equity = equity.clone();
                    async move { Json(equity) }
                }),
            )
            .route(
                SUBACCOUNT_MARGIN_RATIO_PATH,
                get(move || {
                    let margin = margin.clone();
                    async move { Json(margin) }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let result = client.get_wallet_account_snapshot(wallet).await;

        if scenario == "foreign-equity" {
            assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidAccountResponse {
                    endpoint: "subaccount equity",
                    ..
                })
            ));
        } else if scenario == "negative-margin" {
            assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidAccountResponse {
                    endpoint: "subaccount margin ratio",
                    ..
                })
            ));
        } else {
            let snapshot = result.unwrap();
            assert_eq!(snapshot.directory.wallet(), wallet);
            assert_eq!(snapshot.directory.addresses, [subaccount]);
            assert_eq!(snapshot.subaccounts.len(), 1);
            assert_eq!(snapshot.subaccounts[0].profile.address, subaccount);
            assert_eq!(snapshot.subaccounts[0].balances.address, subaccount);
            assert_eq!(snapshot.subaccounts[0].equity.subaccount, subaccount);
            assert_eq!(
                snapshot.subaccounts[0].margin_ratio.collateral,
                Decimal::new(97_096, 2)
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn account_ownership_proof_queries_the_derived_wallet_and_configured_subaccount() {
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
        let profile_wallet = wallet.clone();
        let profile_subaccount = subaccount.clone();
        let app = Router::new()
            .route(
                WALLET_SUBACCOUNTS_PATH,
                get(
                    move |Query(query): Query<std::collections::HashMap<String, String>>| {
                        let wallet = directory_wallet.clone();
                        let subaccount = directory_subaccount.clone();
                        async move {
                            assert_eq!(query.get("address"), Some(&wallet));
                            Json(json!({
                                "code": 200,
                                "msg": "success",
                                "data": [subaccount],
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

        let proof = client
            .get_account_ownership_proof(&key, &subaccount)
            .await
            .unwrap();

        assert_eq!(proof.signer(), signer);
        assert_eq!(proof.subaccount(), [0x11; 20]);
        server.abort();
    }

    #[rstest]
    fn typed_account_state_validation_rejects_inconsistent_responses() {
        let wallet = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
        let subaccount = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";

        let mut subaccounts = DeepXWalletSubaccounts::new(
            wallet.to_string(),
            vec![
                "0xabcdef1111111111111111111111111111111111".to_string(),
                "0xABCDEF1111111111111111111111111111111111".to_string(),
            ],
        );
        assert!(matches!(
            validate_wallet_subaccounts(&subaccounts),
            Err(DeepXHttpError::InvalidAccountResponse { .. })
        ));
        subaccounts.addresses[1] = "invalid".to_string();
        assert!(matches!(
            validate_wallet_subaccounts(&subaccounts),
            Err(DeepXHttpError::InvalidAccountResponse { .. })
        ));

        let mut profile = serde_json::from_str::<DeepXApiResponse<DeepXSubaccountProfile>>(
            SUBACCOUNT_INFO_RESPONSE,
        )
        .unwrap()
        .data;
        assert!(matches!(
            validate_subaccount_profile(
                &profile,
                subaccount,
                Some("0x1111111111111111111111111111111111111111")
            ),
            Err(DeepXHttpError::InvalidAccountResponse { .. })
        ));
        profile.status = "FutureStatus".to_string();
        assert!(matches!(
            validate_subaccount_profile(&profile, subaccount, Some(wallet)),
            Err(DeepXHttpError::InvalidAccountResponse { .. })
        ));

        let mut balances = serde_json::from_str::<DeepXApiResponse<DeepXSubaccountBalances>>(
            SUBACCOUNT_BALANCES_RESPONSE,
        )
        .unwrap()
        .data;
        balances.assets[1].symbol = "usdc".to_string();
        assert!(matches!(
            validate_subaccount_balances(&balances, subaccount),
            Err(DeepXHttpError::InvalidAccountResponse { .. })
        ));
        balances.assets[1].symbol = "ETH".to_string();
        balances.assets[1].balance_borrowed = Decimal::NEGATIVE_ONE;
        assert!(matches!(
            validate_subaccount_balances(&balances, subaccount),
            Err(DeepXHttpError::InvalidAccountResponse { .. })
        ));

        let mut equity = serde_json::from_str::<DeepXApiResponse<DeepXSubaccountEquity>>(
            SUBACCOUNT_EQUITY_RESPONSE,
        )
        .unwrap()
        .data;
        equity.total_borrows_usd = Decimal::NEGATIVE_ONE;
        assert!(matches!(
            validate_subaccount_equity(&equity, subaccount),
            Err(DeepXHttpError::InvalidAccountResponse { .. })
        ));

        let mut margin = serde_json::from_str::<DeepXApiResponse<DeepXSubaccountMarginRatio>>(
            SUBACCOUNT_MARGIN_RATIO_RESPONSE,
        )
        .unwrap()
        .data;
        margin.margin_required = Decimal::NEGATIVE_ONE;
        assert!(matches!(
            validate_subaccount_margin_ratio(&margin),
            Err(DeepXHttpError::InvalidAccountResponse { .. })
        ));
    }

    #[tokio::test]
    async fn typed_account_trade_collector_rejects_cross_page_duplicate_identity() {
        let fixture: serde_json::Value =
            serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
        let trade = fixture["data"]["items"][0].clone();
        let first = json!({
            "code": 200,
            "msg": "success",
            "fail": false,
            "data": {"items": [trade.clone()], "hasNext": true, "nextCursor": "next"},
        });
        let terminal = json!({
            "code": 200,
            "msg": "success",
            "fail": false,
            "data": {"items": [trade], "hasNext": false, "nextCursor": null},
        });
        let app = Router::new().route(
            PERP_ACCOUNT_TRADES_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    async move {
                        if query.get("cursor").is_some_and(|cursor| cursor == "next") {
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
        let request = DeepXPerpAccountTradesRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
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
            client.get_perp_account_trade_pages(&request, 2).await,
            Err(DeepXHttpError::InvalidHistoryResponse { .. })
        ));
        server.abort();
    }

    #[tokio::test]
    async fn invalid_perp_account_order_requests_fail_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let valid_subaccount = "0x1111111111111111111111111111111111111111";
        for request in [
            DeepXPerpOpenOrdersRequest {
                subaccount: "invalid".to_string(),
                market_id: None,
                is_long: None,
                cursor: None,
                page_size: None,
                sort: crate::http::DeepXAccountSortOrder::Descending,
            },
            DeepXPerpOpenOrdersRequest {
                subaccount: valid_subaccount.to_string(),
                market_id: Some(0),
                is_long: None,
                cursor: None,
                page_size: None,
                sort: crate::http::DeepXAccountSortOrder::Descending,
            },
            DeepXPerpOpenOrdersRequest {
                subaccount: valid_subaccount.to_string(),
                market_id: None,
                is_long: None,
                cursor: None,
                page_size: None,
                sort: crate::http::DeepXAccountSortOrder::Descending,
            },
            DeepXPerpOpenOrdersRequest {
                subaccount: valid_subaccount.to_string(),
                market_id: Some(3),
                is_long: None,
                cursor: Some(String::new()),
                page_size: None,
                sort: crate::http::DeepXAccountSortOrder::Descending,
            },
            DeepXPerpOpenOrdersRequest {
                subaccount: valid_subaccount.to_string(),
                market_id: Some(3),
                is_long: None,
                cursor: None,
                page_size: Some(0),
                sort: crate::http::DeepXAccountSortOrder::Descending,
            },
        ] {
            assert!(matches!(
                client.get_perp_open_orders_raw(&request).await,
                Err(DeepXHttpError::InvalidRequest(_))
            ));
        }
        let history = DeepXPerpHistoryOrdersRequest {
            subaccount: valid_subaccount.to_string(),
            market_id: None,
            cursor: Some(String::new()),
            page_size: None,
            sort: crate::http::DeepXAccountSortOrder::Descending,
        };
        assert!(matches!(
            client.get_perp_history_orders_raw(&history).await,
            Err(DeepXHttpError::InvalidRequest(_))
        ));
    }

    #[tokio::test]
    async fn perp_account_order_pages_require_usable_continuation_cursor() {
        const INVALID_PAGE: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":[],"#,
            r#""hasNext":true,"nextCursor":null}}"#,
        );
        let app = Router::new().route(PERP_HISTORY_ORDERS_PATH, get(|| async { INVALID_PAGE }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let request = DeepXPerpHistoryOrdersRequest {
            subaccount: "0x1111111111111111111111111111111111111111".to_string(),
            market_id: Some(3),
            cursor: None,
            page_size: Some(1),
            sort: crate::http::DeepXAccountSortOrder::Descending,
        };

        assert!(matches!(
            client.get_perp_history_orders_raw(&request).await,
            Err(DeepXHttpError::MissingPaginationCursor { .. })
        ));
        server.abort();
    }

    #[tokio::test]
    async fn perp_account_trade_page_preserves_raw_items_and_conditional_filters() {
        const RESPONSE: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":["#,
            r#"{"id":9007199254740993,"orderId":"18446744073709551615","#,
            r#""fee":0.000000000000000000123456789,"future":"retained"}],"#,
            r#""hasNext":true,"nextCursor":"next-page"}}"#,
        );
        const ITEM: &str = concat!(
            r#"{"id":9007199254740993,"orderId":"18446744073709551615","#,
            r#""fee":0.000000000000000000123456789,"future":"retained"}"#,
        );
        let app = Router::new().route(
            PERP_ACCOUNT_TRADES_PATH,
            get(
                |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    assert_eq!(query.len(), 9);
                    assert_eq!(query["orderId"], "18446744073709551615");
                    assert_eq!(query["isLong"], "false");
                    assert_eq!(query["user"], "0x1111111111111111111111111111111111111111");
                    assert_eq!(query["marketId"], "3");
                    assert_eq!(query["cursor"], "opaque&cursor");
                    assert_eq!(query["sort"], "ASC");
                    assert_eq!(query["start"], "1000");
                    assert_eq!(query["end"], "2000");
                    assert_eq!(query["pageSize"], "7");
                    RESPONSE
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let page = client
            .get_perp_account_trades_raw(&DeepXPerpAccountTradesRequest {
                subaccount: "0x1111111111111111111111111111111111111111".to_string(),
                order_id: Some(u64::MAX.to_string()),
                market_id: Some(3),
                is_long: Some(false),
                cursor: Some("opaque&cursor".to_string()),
                sort: crate::http::DeepXAccountSortOrder::Ascending,
                start_ms: Some(1_000),
                end_ms: Some(2_000),
                page_size: Some(7),
            })
            .await
            .unwrap();

        assert_eq!(page.items[0].get(), ITEM);
        assert!(page.has_next);
        assert_eq!(page.next_cursor.as_deref(), Some("next-page"));
        server.abort();
    }

    #[tokio::test]
    async fn invalid_perp_account_trade_filters_fail_before_transport() {
        fn request() -> DeepXPerpAccountTradesRequest {
            DeepXPerpAccountTradesRequest {
                subaccount: "0x1111111111111111111111111111111111111111".to_string(),
                order_id: None,
                market_id: None,
                is_long: None,
                cursor: None,
                sort: crate::http::DeepXAccountSortOrder::Descending,
                start_ms: None,
                end_ms: None,
                page_size: None,
            }
        }

        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let mut invalid = Vec::new();
        let mut missing_market = request();
        missing_market.order_id = Some("1".to_string());
        missing_market.is_long = Some(true);
        invalid.push(missing_market);
        let mut missing_side = request();
        missing_side.order_id = Some("1".to_string());
        missing_side.market_id = Some(3);
        invalid.push(missing_side);
        let mut side_without_order = request();
        side_without_order.is_long = Some(false);
        invalid.push(side_without_order);
        let mut invalid_order = request();
        invalid_order.order_id = Some("18446744073709551616".to_string());
        invalid_order.market_id = Some(3);
        invalid_order.is_long = Some(true);
        invalid.push(invalid_order);
        let mut inverted_time = request();
        inverted_time.start_ms = Some(2);
        inverted_time.end_ms = Some(1);
        invalid.push(inverted_time);
        let mut invalid_time = request();
        invalid_time.end_ms = Some(u64::MAX);
        invalid.push(invalid_time);

        for request in invalid {
            assert!(matches!(
                client.get_perp_account_trades_raw(&request).await,
                Err(DeepXHttpError::InvalidRequest(_))
            ));
        }
    }

    #[tokio::test]
    async fn perp_funding_fee_page_preserves_raw_items_and_query_scope() {
        let app = Router::new().route(
            PERP_FUNDING_FEE_PATH,
            get(|RawQuery(query): RawQuery| async move {
                assert_eq!(
                    query.as_deref(),
                    Some(concat!(
                        "user=0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc&marketId=3",
                        "&start=1789459670946&end=1789522187243&cursor=opaque%26cursor",
                        "&pageSize=10",
                    ))
                );
                PERP_FUNDING_FEES_ACCOUNT_RESPONSE
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let page = client
            .get_perp_funding_fees_raw(&DeepXPerpFundingFeeRequest {
                subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
                market_id: Some(3),
                start_ms: Some(1_789_459_670_946),
                end_ms: Some(1_789_522_187_243),
                cursor: Some("opaque&cursor".to_string()),
                page_size: Some(10),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 4);
        assert!(page.items[0].get().contains("0.000489097868208476"));
        assert!(!page.has_next);
        server.abort();
    }

    #[tokio::test]
    async fn invalid_perp_funding_fee_requests_fail_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let request = || DeepXPerpFundingFeeRequest {
            subaccount: "0x1111111111111111111111111111111111111111".to_string(),
            market_id: None,
            start_ms: None,
            end_ms: None,
            cursor: None,
            page_size: None,
        };
        let mut invalid = Vec::new();
        let mut address = request();
        address.subaccount = "invalid".to_string();
        invalid.push(address);
        let mut market = request();
        market.market_id = Some(0);
        invalid.push(market);
        let mut cursor = request();
        cursor.cursor = Some(String::new());
        invalid.push(cursor);
        let mut page_size = request();
        page_size.page_size = Some(0);
        invalid.push(page_size);
        let mut inverted = request();
        inverted.start_ms = Some(2);
        inverted.end_ms = Some(1);
        invalid.push(inverted);
        let mut out_of_range = request();
        out_of_range.end_ms = Some(u64::MAX);
        invalid.push(out_of_range);

        for request in invalid {
            assert!(matches!(
                client.get_perp_funding_fees_raw(&request).await,
                Err(DeepXHttpError::InvalidRequest(_))
            ));
        }
    }

    #[rstest]
    #[case("owner", json!("0x1111111111111111111111111111111111111111"))]
    #[case("market", json!(4))]
    #[case("positionSize", json!(0))]
    #[case("height", json!(0))]
    #[case("createdAt", json!("invalid"))]
    fn typed_perp_funding_fees_reject_invalid_records(
        #[case] field: &str,
        #[case] value: serde_json::Value,
    ) {
        let mut response: serde_json::Value =
            serde_json::from_str(PERP_FUNDING_FEES_ACCOUNT_RESPONSE).unwrap();
        response["data"]["items"][0][field] = value;
        let page: DeepXRawAccountPage = serde_json::from_value(response["data"].clone()).unwrap();
        let request = DeepXPerpFundingFeeRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_id: Some(3),
            start_ms: None,
            end_ms: None,
            cursor: None,
            page_size: Some(10),
        };

        let error = decode_account_page::<DeepXPerpFundingFeeRecord, _>(
            "perp funding fees",
            page,
            request.page_size,
            |index, fee| validate_perp_funding_fee_record(index, fee, &request),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::InvalidHistoryResponse { .. }
        ));
    }

    #[tokio::test]
    async fn typed_perp_funding_fee_pages_reject_duplicate_event_identity() {
        let fixture: serde_json::Value =
            serde_json::from_str(PERP_FUNDING_FEES_ACCOUNT_RESPONSE).unwrap();
        let item = fixture["data"]["items"][0].clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let app = Router::new().route(
            PERP_FUNDING_FEE_PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let call = observed.fetch_add(1, Ordering::Relaxed);
                    let item = item.clone();
                    async move {
                        if call == 0 {
                            assert!(!query.contains_key("cursor"));
                            Json(json!({
                                "code": 200,
                                "msg": "success",
                                "fail": false,
                                "data": {
                                    "items": [item],
                                    "hasNext": true,
                                    "nextCursor": "next-page",
                                },
                            }))
                        } else {
                            assert_eq!(query["cursor"], "next-page");
                            Json(json!({
                                "code": 200,
                                "msg": "success",
                                "fail": false,
                                "data": {
                                    "items": [item],
                                    "hasNext": false,
                                    "nextCursor": null,
                                },
                            }))
                        }
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let request = DeepXPerpFundingFeeRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_id: Some(3),
            start_ms: None,
            end_ms: None,
            cursor: None,
            page_size: Some(1),
        };

        assert!(matches!(
            client.get_perp_funding_fee_pages(&request, 2).await,
            Err(DeepXHttpError::InvalidHistoryResponse { .. })
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
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

    #[tokio::test]
    async fn invalid_perp_position_requests_fail_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let valid_subaccount = "0x1111111111111111111111111111111111111111";
        for request in [
            DeepXPerpPositionsRequest {
                subaccount: "invalid".to_string(),
                market_id: None,
                only_closed: None,
                cursor: None,
                page_size: None,
            },
            DeepXPerpPositionsRequest {
                subaccount: valid_subaccount.to_string(),
                market_id: Some(0),
                only_closed: None,
                cursor: None,
                page_size: None,
            },
            DeepXPerpPositionsRequest {
                subaccount: valid_subaccount.to_string(),
                market_id: None,
                only_closed: None,
                cursor: Some(String::new()),
                page_size: None,
            },
            DeepXPerpPositionsRequest {
                subaccount: valid_subaccount.to_string(),
                market_id: None,
                only_closed: None,
                cursor: None,
                page_size: Some(0),
            },
        ] {
            assert!(matches!(
                client.get_perp_positions_raw(&request).await,
                Err(DeepXHttpError::InvalidRequest(_))
            ));
        }
        let request = DeepXPerpPositionsRequest {
            subaccount: valid_subaccount.to_string(),
            market_id: None,
            only_closed: None,
            cursor: None,
            page_size: None,
        };
        assert!(matches!(
            client.get_perp_position_pages_raw(&request, 0).await,
            Err(DeepXHttpError::InvalidPaginationLimit)
        ));
    }

    #[tokio::test]
    async fn test_subaccount_balances_raw() {
        let subaccount = "0xABCDEF1111111111111111111111111111111111";
        let payload = r#"{"address":"opaque&account=1","assets":[{"balance":0.1234567890123456789012345678,"balanceBorrowed":"9007199254740993.000000","assetId":"000001"}]}"#;
        let bodies = [
            format!(r#"{{"code":200,"msg":"success","fail":false,"data":{payload}}}"#),
            r#"{"code":10009,"msg":"not found","fail":true,"data":null}"#.to_string(),
            r#"{"code":200,"msg":"success","fail":false}"#.to_string(),
            r#"{"code":200,"msg":"success","data":{}}"#.to_string(),
            "not json".to_string(),
        ];
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let app = Router::new().route(
            "/internal/v1/account/balances",
            get(move |RawQuery(query): RawQuery| {
                let body = bodies[observed.fetch_add(1, Ordering::Relaxed)].clone();
                async move {
                    assert_eq!(query.unwrap(), format!("subaccount={subaccount}"));
                    body
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        for invalid in [
            "",
            "0x",
            "invalid",
            "0x111111111111111111111111111111111111111g",
            "0X1111111111111111111111111111111111111111",
            " 0x1111111111111111111111111111111111111111",
            "0x1111111111111111111111111111111111111111&extra=1",
        ] {
            assert!(matches!(
                client.get_subaccount_balances_raw(invalid).await,
                Err(DeepXHttpError::InvalidRequest(_))
            ));
        }
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            client
                .get_subaccount_balances_raw(subaccount)
                .await
                .unwrap()
                .get(),
            payload
        );
        assert!(matches!(
            client.get_subaccount_balances_raw(subaccount).await,
            Err(DeepXHttpError::Api {
                code: DeepXResponseCode::Api(10009),
                ..
            })
        ));
        for _ in 0..3 {
            assert!(matches!(
                client.get_subaccount_balances_raw(subaccount).await,
                Err(DeepXHttpError::Decode(_))
            ));
        }
        assert_eq!(calls.load(Ordering::Relaxed), 5);
        server.abort();
    }

    #[tokio::test]
    async fn test_delegate_accounts_raw() {
        let wallet = "0xABCDEF1111111111111111111111111111111111";
        let payload = r#"[{"delegateAddress":"opaque&delegate=0001","validUntil":9007199254740993,"unknownFee":0.1234567890123456789012345678,"mode":"future-mode"}]"#;
        let bodies = [
            format!(r#"{{"code":200,"msg":"success","fail":false,"data":{payload}}}"#),
            r#"{"code":10014,"msg":"invalid address","fail":true,"data":null}"#.to_string(),
            r#"{"code":200,"msg":"failure","fail":true,"data":[]}"#.to_string(),
            r#"{"code":200,"msg":"success","fail":false}"#.to_string(),
            r#"{"code":200,"msg":"success","data":[]}"#.to_string(),
            "not json".to_string(),
        ];
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let app = Router::new().route(
            "/internal/v1/account/delegate-accounts",
            get(move |RawQuery(query): RawQuery| {
                let body = bodies[observed.fetch_add(1, Ordering::Relaxed)].clone();
                async move {
                    assert_eq!(query.unwrap(), format!("address={wallet}"));
                    body
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        for invalid in [
            "",
            "0x",
            "invalid",
            "0x111111111111111111111111111111111111111g",
            "0X1111111111111111111111111111111111111111",
            " 0x1111111111111111111111111111111111111111",
            "0x1111111111111111111111111111111111111111&extra=1",
            "0x1111111111111111111111111111111111111111\u{e9}",
        ] {
            assert!(matches!(
                client.get_delegate_accounts_raw(invalid).await,
                Err(DeepXHttpError::InvalidRequest(_))
            ));
        }
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            client
                .get_delegate_accounts_raw(wallet)
                .await
                .unwrap()
                .get(),
            payload
        );
        assert!(matches!(
            client.get_delegate_accounts_raw(wallet).await,
            Err(DeepXHttpError::Api {
                code: DeepXResponseCode::Api(10014),
                ..
            })
        ));
        assert!(matches!(
            client.get_delegate_accounts_raw(wallet).await,
            Err(DeepXHttpError::Api { .. })
        ));
        for _ in 0..3 {
            assert!(matches!(
                client.get_delegate_accounts_raw(wallet).await,
                Err(DeepXHttpError::Decode(_))
            ));
        }
        assert_eq!(calls.load(Ordering::Relaxed), 6);
        server.abort();
    }

    #[tokio::test]
    async fn typed_delegate_directories_validate_captured_responses() {
        const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
        const DELEGATE: &str = "0x1b856b9bf1d0ceeb0927a081c759813cd703a54f";
        let app = Router::new()
            .route(
                DELEGATE_ACCOUNTS_PATH,
                get(
                    |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                        assert_eq!(query.len(), 1);
                        assert_eq!(query["address"], WALLET);
                        WALLET_DELEGATE_ACCOUNTS_RESPONSE
                    },
                ),
            )
            .route(
                DELEGATOR_ACCOUNTS_PATH,
                get(
                    |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                        assert_eq!(query.len(), 1);
                        assert_eq!(query["address"], DELEGATE);
                        DELEGATE_DELEGATOR_ACCOUNTS_RESPONSE
                    },
                ),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();

        let delegates = client.get_delegate_accounts(WALLET).await.unwrap();
        let delegators = client.get_delegator_accounts(DELEGATE).await.unwrap();

        assert_eq!(delegates.wallet(), WALLET);
        assert_eq!(delegates.accounts.len(), 1);
        assert_eq!(delegates.accounts[0].delegate_address, DELEGATE);
        assert_eq!(delegates.accounts[0].delegate_name, "One-Click Trading");
        assert_eq!(delegates.accounts[0].valid_until, 1_804_997_046_158);
        assert_eq!(delegates.accounts[0].create_time, 1_789_445_050_397);
        assert_eq!(delegates.accounts[0].mode.as_str(), "PlaceOrCancelOrder");
        assert!(delegates.accounts[0].active);
        assert_eq!(delegators.delegate(), DELEGATE);
        assert_eq!(delegators.wallets, [WALLET]);
        server.abort();
    }

    #[tokio::test]
    async fn typed_delegate_directories_reject_invalid_input_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();

        assert!(matches!(
            client.get_delegate_accounts("invalid").await,
            Err(DeepXHttpError::InvalidRequest(_))
        ));
        assert!(matches!(
            client.get_delegator_accounts("invalid").await,
            Err(DeepXHttpError::InvalidRequest(_))
        ));
    }

    #[rstest]
    #[case("address")]
    #[case("duplicate")]
    #[case("timestamp")]
    fn typed_delegate_directory_rejects_invalid_records(#[case] mutation: &str) {
        let mut accounts = serde_json::from_str::<DeepXApiResponse<Vec<DeepXDelegateAccount>>>(
            WALLET_DELEGATE_ACCOUNTS_RESPONSE,
        )
        .unwrap()
        .data;
        match mutation {
            "address" => accounts[0].delegate_address = "invalid".to_string(),
            "duplicate" => accounts.push(accounts[0].clone()),
            "timestamp" => accounts[0].valid_until = accounts[0].create_time,
            _ => unreachable!(),
        }
        let directory = DeepXWalletDelegateAccounts::new(
            "0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string(),
            accounts,
        );

        assert!(matches!(
            validate_wallet_delegate_accounts(&directory),
            Err(DeepXHttpError::InvalidAccountResponse {
                endpoint: "delegate accounts",
                ..
            })
        ));
    }

    #[rstest]
    fn typed_delegate_directory_accepts_legacy_nonexpiring_record() {
        let mut accounts = serde_json::from_str::<DeepXApiResponse<Vec<DeepXDelegateAccount>>>(
            WALLET_DELEGATE_ACCOUNTS_RESPONSE,
        )
        .unwrap()
        .data;
        accounts[0].valid_until = 0;
        let directory = DeepXWalletDelegateAccounts::new(
            "0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string(),
            accounts,
        );

        validate_wallet_delegate_accounts(&directory).unwrap();
    }

    #[rstest]
    fn typed_delegate_directories_reject_unknown_mode_and_invalid_reverse_directory() {
        let mut response: serde_json::Value =
            serde_json::from_str(WALLET_DELEGATE_ACCOUNTS_RESPONSE).unwrap();
        response["data"][0]["mode"] = json!("FutureMode");
        assert!(
            serde_json::from_value::<DeepXApiResponse<Vec<DeepXDelegateAccount>>>(response)
                .is_err()
        );

        let invalid = DeepXDelegateWallets::new(
            "0x1b856b9bf1d0ceeb0927a081c759813cd703a54f".to_string(),
            vec!["invalid".to_string()],
        );
        assert!(validate_delegate_wallets(&invalid).is_err());
        let duplicate = DeepXDelegateWallets::new(
            "0x1b856b9bf1d0ceeb0927a081c759813cd703a54f".to_string(),
            vec![
                "0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string(),
                "0x781ED35B167068C93DFADAB41DFB680EDACA4E50".to_string(),
            ],
        );
        assert!(validate_delegate_wallets(&duplicate).is_err());
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct HealthResponse {
        status: String,
    }

    #[derive(Debug, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct FundingRateQuery {
        market_id: u64,
        start: u64,
        end: u64,
        limit: u32,
        cursor: String,
        interval: String,
        sort: String,
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
    struct LongShortRatioQuery {
        market_id: u64,
        start: u64,
        end: u64,
        limit: u32,
        cursor: String,
        interval: String,
        sort: String,
    }

    #[derive(Debug, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct PerpTradesQuery {
        market_id: u64,
        page_size: u32,
        cursor: String,
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

    #[derive(Debug, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct PerpVolumeQuery {
        market_id: u64,
        period: String,
    }

    #[derive(Debug, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct PerpLastPriceQuery {
        market_id: u64,
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

    async fn mock_client() -> DeepXHttpClient {
        let router = Router::new()
            .route("/health", get(|| async { Json(json!({ "status": "ok" })) }))
            .route(
                "/unavailable",
                get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "temporarily unavailable") }),
            )
            .route("/invalid-json", get(|| async { "not json" }))
            .route(
                SPOT_MARKETS_PATH,
                get(|| async {
                    Json(json!({
                        "code": 200,
                        "msg": "success",
                        "data": [{
                            "name": "ETH/USDC",
                            "pair": "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37",
                            "quoteAddress": "0x9eb03d8ac62ae18398ced13c033db78b905ad8c9",
                            "quoteDecimal": 6,
                            "quoteSymbol": "usdc",
                            "baseAddress": "0x123ae070eb84068b5fed9f5b99f236507c44c880",
                            "baseDecimal": 18,
                            "baseSymbol": "eth",
                            "takerFeeRate": 0.0004,
                            "makerFeeRate": 0.0001,
                            "price": 2449.68,
                            "tickSize": 0.01,
                            "isPaused": false,
                            "maxDeviationBps": 0.1,
                            "limitOrderGuardLimitLong": 0.2,
                            "limitOrderGuardLimitShort": 5,
                            "last24hPriceChangeRate": null
                        }],
                        "fail": false
                    }))
                }),
            )
            .route(
                PERP_MARKETS_PATH,
                get(|| async { PERP_MARKETS_RESPONSE }),
            );
        let base_url = spawn_server(router).await;
        DeepXHttpClient::new(format!("{base_url}/"), Some(5), None).unwrap()
    }

    const PERP_MARKETS_RESPONSE: &str = r#"{
        "code": 200,
        "msg": "success",
        "data": [{
            "id": 3,
            "name": "ETH-USDC",
            "baseSymbol": "eth",
            "baseAddress": "0x123ae070eb84068b5fed9f5b99f236507c44c880",
            "baseDecimal": 18,
            "quoteMarketId": 1,
            "quoteSymbol": "usdc",
            "quoteAddress": "0x9eb03d8ac62ae18398ced13c033db78b905ad8c9",
            "quoteDecimal": 6,
            "network": "",
            "height": 64839897,
            "fundingRate": 0.000395485070947924,
            "cumulativeFundingIndex": 0.05836447860920836,
            "lastFundingRateTime": 1788251686460,
            "lastCaclFundingRateTime": 1788250451100,
            "oraclePrice": 2449.12,
            "markPrice": 2449.68,
            "last24hPriceChangeRate": 0.51,
            "maxDeviationBps": 0.1,
            "initialMarginRatio": 0.04,
            "maintenanceMarginRatio": 0.02,
            "maxActiveOrders": 128,
            "takerFeeRate": 0.0002,
            "makerFeeRate": -0.0001,
            "orderSpecMinQty": "0.0010",
            "orderSpecTickSize": "0.0100",
            "orderSpecStepSize": "0.0010",
            "orderSpecMinNotional": "1",
            "limitOrderGuardLimitLong": 0.2,
            "limitOrderGuardLimitShort": 5,
            "openInterest": 12242.54,
            "longOpenPosNum": "14",
            "shortOpenPosNum": "6",
            "baseInterestRate": 0.0001,
            "impactMarginValue": 100,
            "fundingRateChangeCap": 0.002,
            "fundingRateChangeFloor": 0.002,
            "fundingRateClampUpperBound": 0.0005,
            "fundingRateClampLowerBound": -0.0005,
            "liquidationDuration": 100,
            "liquidityBucketSlippageStep": 10000,
            "liquidityBucketSlippageLimit": 100000,
            "liquidationDustValue": "50000000",
            "liquidatorShareFeeRate": "5000",
            "insuranceFundShareFeeRate": "5000",
            "deployer": null,
            "deployerDelegate": null,
            "deployerFeeRecipient": null,
            "deployerBuilderFeeBps": null,
            "deployerIsolatedMarginOnly": null,
            "isPaused": false,
            "isDeleted": false,
            "unknownFutureField": "accepted"
        }],
        "fail": false
    }"#;

    #[tokio::test]
    async fn decodes_successful_json_response() {
        let client = mock_client().await;

        let response = client.get_json::<HealthResponse>("/health").await.unwrap();

        assert_eq!(
            response,
            HealthResponse {
                status: "ok".to_string()
            }
        );
        assert!(!client.base_url().ends_with('/'));
    }

    #[tokio::test]
    async fn preserves_non_success_status_and_body() {
        let client = mock_client().await;

        let error = client
            .get_json::<HealthResponse>("/unavailable")
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::Http { status: 503, message }
                if message == "temporarily unavailable"
        ));
    }

    #[tokio::test]
    async fn classifies_invalid_success_body_as_decode_error() {
        let client = mock_client().await;

        let error = client
            .get_json::<HealthResponse>("/invalid-json")
            .await
            .unwrap_err();

        assert!(matches!(error, DeepXHttpError::Decode(_)));
    }

    #[tokio::test]
    async fn decodes_spot_market_metadata_without_floating_point() {
        let markets = mock_client().await.get_spot_markets().await.unwrap();

        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].name, "ETH/USDC");
        assert_eq!(markets[0].tick_size, Decimal::new(1, 2));
        assert_eq!(markets[0].last_24h_price_change_rate, None);
    }

    #[tokio::test]
    async fn decodes_perp_market_metadata_and_ignores_unmodeled_fields() {
        let markets = mock_client().await.get_perp_markets().await.unwrap();

        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].id, 3);
        assert_eq!(markets[0].order_spec_min_qty, Decimal::new(10, 4));
        assert_eq!(markets[0].maker_fee_rate, Decimal::new(-1, 4));
        assert_eq!(
            markets[0].funding_rate,
            "0.000395485070947924".parse::<Decimal>().unwrap()
        );
        assert_eq!(markets[0].deployer, None);
    }

    #[tokio::test]
    async fn encodes_and_decodes_perp_funding_rate_page_exactly() {
        let router = Router::new().route(
            PERP_FUNDING_RATE_PATH,
            get(|Query(query): Query<FundingRateQuery>| async move {
                assert_eq!(query.market_id, 3);
                assert_eq!(query.start, 1_788_168_000_000);
                assert_eq!(query.end, 1_788_254_400_000);
                assert_eq!(query.limit, 5);
                assert_eq!(query.cursor, "opaque+/cursor=");
                assert_eq!(query.interval, "1m");
                assert_eq!(query.sort, "ASC");
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "marketId": 3,
                        "details": [{
                            "fundingRate": "0.000012500000000001",
                            "time": 1_788_168_000_000_u64
                        }],
                        "nextCursor": "next-page",
                        "hasNext": true
                    },
                    "fail": false
                }))
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXFundingRateRequest {
            market_id: 3,
            start_ms: 1_788_168_000_000,
            end_ms: Some(1_788_254_400_000),
            limit: Some(5),
            cursor: Some("opaque+/cursor=".to_string()),
        };

        let page = client.get_perp_funding_rates(&request).await.unwrap();

        assert_eq!(page.market_id, 3);
        assert_eq!(page.details.len(), 1);
        assert_eq!(
            page.details[0].funding_rate,
            "0.000012500000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(page.details[0].time, 1_788_168_000_000);
        assert_eq!(page.next_cursor.as_deref(), Some("next-page"));
        assert!(page.has_next);
    }

    #[tokio::test]
    async fn rejects_funding_rate_response_for_another_market() {
        let router = Router::new().route(
            PERP_FUNDING_RATE_PATH,
            get(|| async {
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "marketId": 4,
                        "details": [],
                        "nextCursor": null,
                        "hasNext": false
                    },
                    "fail": false
                }))
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let request = DeepXFundingRateRequest {
            market_id: 3,
            start_ms: 1,
            end_ms: Some(2),
            limit: Some(3),
            cursor: None,
        };

        let error = client.get_perp_funding_rates(&request).await.unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::ResponseMarketMismatch {
                endpoint: "perp funding-rate",
                expected: 3,
                received: 4,
            }
        ));
    }

    #[tokio::test]
    async fn encodes_and_decodes_perp_long_short_ratio_page_exactly() {
        let router = Router::new().route(
            PERP_LONG_SHORT_RATIO_PATH,
            get(|Query(query): Query<LongShortRatioQuery>| async move {
                assert_eq!(query.market_id, 3);
                assert_eq!(query.start, 1_788_251_280_000);
                assert_eq!(query.end, 1_788_254_880_000);
                assert_eq!(query.limit, 3);
                assert_eq!(query.cursor, "opaque+/cursor=");
                assert_eq!(query.interval, "1m");
                assert_eq!(query.sort, "ASC");
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "marketId": 3,
                        "details": [{
                            "longShortRatio": "0.439024390244000001",
                            "time": 1_788_251_280_000_u64
                        }],
                        "nextCursor": "next-page",
                        "hasNext": true
                    },
                    "fail": false
                }))
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXLongShortRatioRequest {
            market_id: 3,
            start_ms: 1_788_251_280_000,
            end_ms: Some(1_788_254_880_000),
            limit: Some(3),
            cursor: Some("opaque+/cursor=".to_string()),
        };

        let page = client.get_perp_long_short_ratios(&request).await.unwrap();

        assert_eq!(page.market_id, 3);
        assert_eq!(page.details.len(), 1);
        assert_eq!(
            page.details[0].long_short_ratio,
            "0.439024390244000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(page.details[0].time, 1_788_251_280_000);
        assert_eq!(page.next_cursor.as_deref(), Some("next-page"));
        assert!(page.has_next);
    }

    #[tokio::test]
    async fn rejects_long_short_ratio_response_for_another_market() {
        let router = Router::new().route(
            PERP_LONG_SHORT_RATIO_PATH,
            get(|| async {
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "marketId": 4,
                        "details": [],
                        "nextCursor": null,
                        "hasNext": false
                    },
                    "fail": false
                }))
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let request = DeepXLongShortRatioRequest {
            market_id: 3,
            start_ms: 1,
            end_ms: Some(2),
            limit: Some(3),
            cursor: None,
        };

        let error = client
            .get_perp_long_short_ratios(&request)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::ResponseMarketMismatch {
                endpoint: "perp long-short-ratio",
                expected: 3,
                received: 4,
            }
        ));
    }

    #[tokio::test]
    async fn rejects_empty_long_short_ratio_cursor_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let request = DeepXLongShortRatioRequest {
            market_id: 3,
            start_ms: 1,
            end_ms: Some(2),
            limit: Some(3),
            cursor: Some(String::new()),
        };

        let error = client
            .get_perp_long_short_ratios(&request)
            .await
            .unwrap_err();

        assert!(
            matches!(error, DeepXHttpError::InvalidRequest(message) if message.contains("cursor"))
        );
    }

    #[tokio::test]
    async fn rejects_long_short_ratio_continuation_without_cursor() {
        let router = Router::new().route(
            PERP_LONG_SHORT_RATIO_PATH,
            get(|| async {
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "marketId": 3,
                        "details": [],
                        "nextCursor": "",
                        "hasNext": true
                    },
                    "fail": false
                }))
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXLongShortRatioRequest {
            market_id: 3,
            start_ms: 1,
            end_ms: Some(2),
            limit: Some(3),
            cursor: None,
        };

        let error = client
            .get_perp_long_short_ratios(&request)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::MissingPaginationCursor {
                endpoint: "perp long-short-ratio"
            }
        ));
    }

    #[rstest]
    #[case(None)]
    #[case(Some(""))]
    fn rejects_next_page_without_usable_cursor(#[case] next_cursor: Option<&str>) {
        let error = validate_cursor_page("perp trades", true, next_cursor).unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::MissingPaginationCursor {
                endpoint: "perp trades"
            }
        ));
    }

    #[rstest]
    #[case(false, None)]
    #[case(false, Some("unused"))]
    #[case(true, Some("next-page"))]
    fn accepts_cursor_pages_that_can_terminate_or_continue(
        #[case] has_next: bool,
        #[case] next_cursor: Option<&str>,
    ) {
        validate_cursor_page("perp trades", has_next, next_cursor).unwrap();
    }

    #[tokio::test]
    async fn rejects_funding_rate_continuation_without_cursor() {
        let router = Router::new().route(
            PERP_FUNDING_RATE_PATH,
            get(|| async {
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "marketId": 3,
                        "details": [],
                        "nextCursor": null,
                        "hasNext": true
                    },
                    "fail": false
                }))
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXFundingRateRequest {
            market_id: 3,
            start_ms: 1,
            end_ms: Some(2),
            limit: Some(3),
            cursor: None,
        };

        let error = client.get_perp_funding_rates(&request).await.unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::MissingPaginationCursor {
                endpoint: "perp funding-rate"
            }
        ));
    }

    #[tokio::test]
    async fn typed_spot_trades_preserve_exact_captured_records_and_query() {
        let router = Router::new().route(
            SPOT_TRADES_PATH,
            get(
                |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    assert_eq!(query.len(), 3);
                    assert_eq!(query["name"], "ETH/USDC");
                    assert_eq!(query["sort"], "DESC");
                    assert_eq!(query["pageSize"], "2");
                    assert!(!query.contains_key("pair"));
                    assert!(!query.contains_key("wallet"));
                    SPOT_TRADES_ETH_USDC_RESPONSE
                },
            ),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();

        let page = client
            .get_spot_trades(&spot_trades_request())
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.total, 11_437_005);
        assert_eq!(page.items[0].id, 188_405_950_000_036);
        assert_eq!(page.items[0].price, Decimal::new(243_436, 2));
        assert_eq!(page.items[0].base_amount, Decimal::new(7_039, 4));
        assert_eq!(page.items[0].maker_fee, Decimal::new(7_039, 8));
        assert_eq!(page.items[1].maker_fee, Decimal::new(955, 8));
        assert_eq!(page.items[0].trade_time, page.items[1].trade_time);
        assert!(page.has_next);
        assert!(page.next_cursor.is_some());
    }

    #[tokio::test]
    async fn spot_market_observations_preserve_exact_captured_values_and_queries() {
        let router = Router::new()
            .route(
                SPOT_CANDLES_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(
                        query.as_deref(),
                        Some(concat!(
                            "name=ETH%2FUSDC&timeFrame=1m&start=1789616280000&",
                            "end=1789619880000&limit=3&sort=ASC&tradeView=false"
                        ))
                    );
                    SPOT_CANDLES_ETH_USDC_RESPONSE
                }),
            )
            .route(
                SPOT_LAST_PRICE_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(query.as_deref(), Some("name=ETH%2FUSDC"));
                    SPOT_LAST_PRICE_ETH_USDC_RESPONSE
                }),
            )
            .route(
                SPOT_VOLUME_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(query.as_deref(), Some("name=ETH%2FUSDC&period=1h"));
                    SPOT_VOLUME_ETH_USDC_RESPONSE
                }),
            );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();

        let candles = client
            .get_spot_candles(&DeepXSpotCandlesRequest {
                name: Some("ETH/USDC".to_string()),
                pair: None,
                interval: crate::http::DeepXSpotCandleInterval::OneMinute,
                start_ms: 1_789_616_280_000,
                end_ms: Some(1_789_619_880_000),
                limit: Some(3),
            })
            .await
            .unwrap();
        let last_price = client
            .get_spot_last_price(&DeepXSpotLastPriceRequest {
                name: Some("ETH/USDC".to_string()),
                pair: None,
            })
            .await
            .unwrap();
        let volume = client
            .get_spot_volume(&DeepXSpotVolumeRequest {
                name: Some("ETH/USDC".to_string()),
                pair: None,
                period: crate::http::DeepXSpotVolumePeriod::OneHour,
            })
            .await
            .unwrap();

        assert_eq!(candles.details.len(), 3);
        assert_eq!(
            candles.details[0].volume,
            "5.6965012112303635".parse::<Decimal>().unwrap()
        );
        assert_eq!(candles.details[2].close, Decimal::new(243_569, 2));
        assert_eq!(last_price.0, Decimal::new(242_894, 2));
        assert_eq!(
            volume.total_volume,
            "293.50766491243".parse::<Decimal>().unwrap()
        );
        assert_eq!(volume.trade_count, 1_186);
    }

    #[tokio::test]
    async fn spot_market_directory_and_lookups_validate_exact_captured_metadata() {
        let router = Router::new()
            .route(SPOT_MARKETS_PATH, get(|| async { SPOT_MARKETS_SPEC369_RESPONSE }))
            .route(
                SPOT_MARKET_BY_NAME_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(query.as_deref(), Some("name=ETH%2FUSDC"));
                    SPOT_MARKET_ETH_USDC_RESPONSE
                }),
            )
            .route(
                SPOT_MARKET_BY_PAIR_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(
                        query.as_deref(),
                        Some(concat!(
                            "pair=0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"
                        ))
                    );
                    SPOT_MARKET_ETH_USDC_RESPONSE
                }),
            );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();

        let markets = client.get_spot_markets().await.unwrap();
        let by_name = client.get_spot_market_by_name("ETH/USDC").await.unwrap();
        let by_pair = client
            .get_spot_market_by_pair(
                "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37",
            )
            .await
            .unwrap();
        let by_pair_without_prefix = client
            .get_spot_market_by_pair(
                "9068D4AC891A14784C17877EB74BD8489B3367C71D72766DBFA4DFBFB662FA37",
            )
            .await
            .unwrap();

        assert_eq!(markets.len(), 2);
        assert_eq!(markets[0].name, "SOL/USDC");
        assert_eq!(markets[1].name, "ETH/USDC");
        assert_eq!(by_name, by_pair);
        assert_eq!(by_pair_without_prefix, by_pair);
        assert_eq!(by_pair.tick_size, Decimal::new(1, 2));
        assert_eq!(by_pair.base_decimal, 18);
        assert_eq!(by_pair.quote_decimal, 6);
        assert_eq!(by_pair.last_24h_price_change_rate, None);
    }

    #[tokio::test]
    async fn perp_market_lookups_validate_exact_captured_metadata_and_queries() {
        let router = Router::new()
            .route(
                PERP_MARKET_BY_ID_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(query.as_deref(), Some("marketId=3"));
                    PERP_MARKET_ETH_USDC_RESPONSE
                }),
            )
            .route(
                PERP_MARKET_BY_NAME_PATH,
                get(|RawQuery(query): RawQuery| async move {
                    assert_eq!(query.as_deref(), Some("name=ETH-USDC"));
                    PERP_MARKET_ETH_USDC_RESPONSE
                }),
            );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();

        let by_id = client.get_perp_market_by_id(3).await.unwrap();
        let by_name = client.get_perp_market_by_name("ETH-USDC").await.unwrap();

        assert_eq!(by_id, by_name);
        assert_eq!(by_id.id, 3);
        assert_eq!(by_id.mark_price, Decimal::new(2_438_185_111, 6));
        assert_eq!(by_id.order_spec_min_qty, Decimal::new(1, 3));
        assert_eq!(by_id.liquidation_dust_value, Decimal::new(50, 0));
        assert_eq!(by_id.deployer, None);
    }

    #[rstest]
    #[case("id")]
    #[case("name")]
    #[case("address")]
    #[case("deployer")]
    #[case("price")]
    #[case("margin")]
    #[case("quantity")]
    #[case("clamp")]
    #[case("liquidation")]
    #[case("timestamp")]
    fn perp_market_lookup_rejects_invalid_metadata(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXPerpMarketLookup> =
            serde_json::from_str(PERP_MARKET_ETH_USDC_RESPONSE).unwrap();
        let mut market = response.data;
        match mutation {
            "id" => market.id = 0,
            "name" => market.name = "ETH/USDC".to_string(),
            "address" => market.base_address = "invalid".to_string(),
            "deployer" => {
                market.deployer_delegate =
                    Some("0x123ae070eb84068b5fed9f5b99f236507c44c880".to_string());
            }
            "price" => market.mark_price = Decimal::ZERO,
            "margin" => market.maintenance_margin_ratio = Decimal::ONE,
            "quantity" => market.order_spec_min_qty = Decimal::ZERO,
            "clamp" => {
                market.funding_rate_clamp_lower_bound = Decimal::ONE;
                market.funding_rate_clamp_upper_bound = Decimal::ZERO;
            }
            "liquidation" => market.liquidity_bucket_slippage_step = 100_001,
            "timestamp" => market.last_calc_funding_rate_time = u64::MAX,
            _ => unreachable!(),
        }

        assert!(matches!(
            validate_perp_market_lookup("perp market", &market),
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "perp market",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn perp_market_lookups_reject_invalid_requests_and_scope_mismatches() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        assert!(client.get_perp_market_by_id(0).await.is_err());
        assert!(client.get_perp_market_by_name(" ").await.is_err());

        let router = Router::new()
            .route(
                PERP_MARKET_BY_ID_PATH,
                get(|| async { PERP_MARKET_ETH_USDC_RESPONSE }),
            )
            .route(
                PERP_MARKET_BY_NAME_PATH,
                get(|| async { PERP_MARKET_ETH_USDC_RESPONSE }),
            );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();

        assert!(client.get_perp_market_by_id(4).await.is_err());
        assert!(client.get_perp_market_by_name("BTC-USDC").await.is_err());
    }

    #[rstest]
    #[case("name")]
    #[case("symbol")]
    #[case("pair")]
    #[case("address")]
    #[case("same-address")]
    #[case("same-symbol")]
    #[case("price")]
    #[case("tick")]
    #[case("deviation")]
    #[case("guard")]
    fn spot_market_rejects_invalid_metadata(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXSpotMarket> =
            serde_json::from_str(SPOT_MARKET_ETH_USDC_RESPONSE).unwrap();
        let mut market = response.data;
        match mutation {
            "name" => market.name = "ETH-USDC".to_string(),
            "symbol" => market.base_symbol = "btc".to_string(),
            "pair" => market.pair = "0x1234".to_string(),
            "address" => market.base_address = "invalid".to_string(),
            "same-address" => market.base_address = market.quote_address.clone(),
            "same-symbol" => market.base_symbol = market.quote_symbol.clone(),
            "price" => market.price = Decimal::NEGATIVE_ONE,
            "tick" => market.tick_size = Decimal::ZERO,
            "deviation" => market.max_deviation_bps = Decimal::NEGATIVE_ONE,
            "guard" => market.limit_order_guard_limit_long = Decimal::ZERO,
            _ => unreachable!(),
        }

        assert!(matches!(
            validate_spot_market("spot market", &market),
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot market",
                ..
            })
        ));
    }

    #[rstest]
    #[case("name")]
    #[case("pair")]
    #[case("pair-encoding")]
    fn spot_market_directory_rejects_duplicate_identities(#[case] identity: &str) {
        let response: DeepXApiResponse<Vec<DeepXSpotMarket>> =
            serde_json::from_str(SPOT_MARKETS_SPEC369_RESPONSE).unwrap();
        let mut markets = response.data;
        if identity == "name" {
            markets[1].name = markets[0].name.clone();
            markets[1].base_symbol = markets[0].base_symbol.clone();
        } else if identity == "pair" {
            markets[1].pair = markets[0].pair.clone();
        } else {
            markets[1].pair = markets[0]
                .pair
                .strip_prefix("0x")
                .unwrap()
                .to_ascii_uppercase();
        }

        assert!(matches!(
            validate_spot_markets(&markets),
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot markets",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn spot_market_lookups_reject_invalid_requests_and_scope_mismatches() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        assert!(client.get_spot_market_by_name(" ").await.is_err());
        assert!(client.get_spot_market_by_pair("0x1234").await.is_err());

        let router = Router::new()
            .route(
                SPOT_MARKET_BY_NAME_PATH,
                get(|| async { SPOT_MARKET_ETH_USDC_RESPONSE }),
            )
            .route(
                SPOT_MARKET_BY_PAIR_PATH,
                get(|| async { SPOT_MARKET_ETH_USDC_RESPONSE }),
            );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let other_pair = format!("0x{}", "11".repeat(32));

        assert!(client.get_spot_market_by_name("SOL/USDC").await.is_err());
        assert!(client.get_spot_market_by_pair(&other_pair).await.is_err());
    }

    #[tokio::test]
    async fn spot_order_book_preserves_exact_captured_levels_and_query() {
        let router = Router::new().route(
            SPOT_ORDER_BOOK_PATH,
            get(|RawQuery(query): RawQuery| async move {
                assert_eq!(
                    query.as_deref(),
                    Some(concat!(
                        "pair=0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37&",
                        "tickSize=0.01"
                    ))
                );
                SPOT_ORDER_BOOK_ETH_USDC_RESPONSE
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();

        let book = client
            .get_spot_order_book(&DeepXSpotOrderBookRequest {
                name: None,
                pair: Some(
                    "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"
                        .to_string(),
                ),
                tick_size: Some(Decimal::new(1, 2)),
            })
            .await
            .unwrap();

        assert_eq!(book.order_buy_list.len(), 20);
        assert_eq!(book.order_sell_list.len(), 20);
        assert_eq!(book.order_buy_list[0].price, Decimal::new(243_567, 2));
        assert_eq!(book.order_sell_list[0].qty, Decimal::new(16_055, 4));
        assert_eq!(book.mid_price, Decimal::new(2_435_745, 3));
        assert_eq!(book.latest_price, Decimal::ZERO);
    }

    #[tokio::test]
    async fn perp_order_book_preserves_exact_captured_levels_and_query() {
        let router = Router::new().route(
            PERP_ORDER_BOOK_PATH,
            get(|RawQuery(query): RawQuery| async move {
                assert_eq!(query.as_deref(), Some("marketId=3&tickSize=0.01"));
                PERP_ORDER_BOOK_ETH_USDC_RESPONSE
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();

        let book = client
            .get_perp_order_book(&DeepXPerpOrderBookRequest {
                market_id: 3,
                tick_size: Some(Decimal::new(1, 2)),
            })
            .await
            .unwrap();

        assert_eq!(book.market_id, 3);
        assert_eq!(book.order_buy_list.len(), 20);
        assert_eq!(book.order_sell_list.len(), 20);
        assert_eq!(book.order_buy_list[0].price, Decimal::new(24_339, 1));
        assert_eq!(book.order_sell_list[0].qty, Decimal::new(577, 3));
        assert_eq!(book.mid_price, Decimal::new(2_434_035, 3));
        assert_eq!(book.latest_price, Decimal::ZERO);
    }

    #[rstest]
    #[case("market")]
    #[case("level-market")]
    #[case("sequence")]
    #[case("time")]
    #[case("observation")]
    #[case("bid-price")]
    #[case("bid-quantity")]
    #[case("bid-value")]
    #[case("bid-order")]
    #[case("ask-order")]
    fn perp_order_book_rejects_invalid_response_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXPerpOrderBook> =
            serde_json::from_str(PERP_ORDER_BOOK_ETH_USDC_RESPONSE).unwrap();
        let mut book = response.data;
        let request = DeepXPerpOrderBookRequest {
            market_id: 3,
            tick_size: Some(Decimal::new(1, 2)),
        };
        match mutation {
            "market" => book.market_id = 4,
            "level-market" => book.order_buy_list[0].market_id = 4,
            "sequence" => book.last_update_id = 0,
            "time" => book.engine_time = u64::MAX,
            "observation" => book.mid_price = Decimal::NEGATIVE_ONE,
            "bid-price" => book.order_buy_list[0].price = Decimal::ZERO,
            "bid-quantity" => book.order_buy_list[0].qty = Decimal::ZERO,
            "bid-value" => book.order_buy_list[0].value = Decimal::NEGATIVE_ONE,
            "bid-order" => book.order_buy_list[1].price = book.order_buy_list[0].price,
            "ask-order" => book.order_sell_list[1].price = book.order_sell_list[0].price,
            _ => unreachable!(),
        }

        assert!(matches!(
            validate_perp_order_book(&book, &request),
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "perp order book",
                ..
            })
        ));
    }

    #[rstest]
    #[case("pair")]
    #[case("name")]
    #[case("sequence")]
    #[case("time")]
    #[case("observation")]
    #[case("bid-price")]
    #[case("bid-quantity")]
    #[case("bid-value")]
    #[case("bid-order")]
    #[case("ask-order")]
    fn spot_order_book_rejects_invalid_response_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXSpotOrderBook> =
            serde_json::from_str(SPOT_ORDER_BOOK_ETH_USDC_RESPONSE).unwrap();
        let mut book = response.data;
        let mut request = DeepXSpotOrderBookRequest {
            name: None,
            pair: Some(
                "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37".to_string(),
            ),
            tick_size: Some(Decimal::new(1, 2)),
        };
        match mutation {
            "pair" => book.pair = format!("0x{}", "11".repeat(32)),
            "name" => {
                request.name = Some("SOL/USDC".to_string());
                request.pair = None;
            }
            "sequence" => book.last_update_id = 0,
            "time" => book.engine_time = u64::MAX,
            "observation" => book.latest_price = Decimal::NEGATIVE_ONE,
            "bid-price" => book.order_buy_list[0].price = Decimal::ZERO,
            "bid-quantity" => book.order_buy_list[0].qty = Decimal::ZERO,
            "bid-value" => book.order_buy_list[0].value = Decimal::NEGATIVE_ONE,
            "bid-order" => book.order_buy_list[1].price = book.order_buy_list[0].price,
            "ask-order" => book.order_sell_list[1].price = book.order_sell_list[0].price,
            _ => unreachable!(),
        }

        assert!(matches!(
            validate_spot_order_book(&book, &request),
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot order book",
                ..
            })
        ));
    }

    #[rstest]
    #[case("market")]
    #[case("limit")]
    #[case("order")]
    #[case("price")]
    #[case("volume")]
    fn spot_candles_reject_invalid_response_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXSpotCandlesPage> =
            serde_json::from_str(SPOT_CANDLES_ETH_USDC_RESPONSE).unwrap();
        let mut page = response.data;
        let mut request = DeepXSpotCandlesRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
            interval: crate::http::DeepXSpotCandleInterval::OneMinute,
            start_ms: 1_789_616_280_000,
            end_ms: Some(1_789_619_880_000),
            limit: Some(3),
        };
        match mutation {
            "market" => page.pair = "SOL/USDC".to_string(),
            "limit" => request.limit = Some(2),
            "order" => page.details[1].time = page.details[0].time,
            "price" => page.details[0].low = Decimal::ZERO,
            "volume" => page.details[0].volume = Decimal::NEGATIVE_ONE,
            _ => unreachable!(),
        }

        assert!(matches!(
            validate_spot_candle_page(&page, &request),
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot candles",
                ..
            })
        ));
    }

    #[rstest]
    #[case("volume")]
    #[case("order")]
    #[case("range")]
    fn spot_volume_rejects_invalid_response_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXSpotVolume> =
            serde_json::from_str(SPOT_VOLUME_ETH_USDC_RESPONSE).unwrap();
        let mut volume = response.data;
        match mutation {
            "volume" => volume.total_volume = Decimal::NEGATIVE_ONE,
            "order" => volume.start_time = volume.end_time + 1,
            "range" => volume.statistic_time = u64::MAX,
            _ => unreachable!(),
        }

        assert!(matches!(
            validate_spot_volume(&volume),
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot volume",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn spot_last_price_rejects_negative_response() {
        let router = Router::new().route(
            SPOT_LAST_PRICE_PATH,
            get(|| async { r#"{"code":200,"msg":"success","data":-0.01,"fail":false}"# }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();

        let error = client
            .get_spot_last_price(&DeepXSpotLastPriceRequest {
                name: Some("ETH/USDC".to_string()),
                pair: None,
            })
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot last price",
                ..
            }
        ));
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn spot_trade_collection_forwards_cursor_and_rejects_duplicates(#[case] duplicate: bool) {
        let first: serde_json::Value = serde_json::from_str(SPOT_TRADES_ETH_USDC_RESPONSE).unwrap();
        let cursor = first["data"]["nextCursor"].as_str().unwrap().to_string();
        let mut terminal = first.clone();
        terminal["data"]["items"] = json!([first["data"]["items"][1].clone()]);
        terminal["data"]["hasNext"] = json!(false);
        terminal["data"]["nextCursor"] = serde_json::Value::Null;
        if !duplicate {
            terminal["data"]["items"][0]["id"] = json!(188_405_949_000_001_u64);
            terminal["data"]["items"][0]["tradeTime"] = json!("2026-09-17T03:27:09.999Z");
        }
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

        let result = client.get_spot_trade_pages(&spot_trades_request(), 2).await;

        if duplicate {
            assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidHistoryResponse {
                    endpoint: "spot trades",
                    ..
                })
            ));
        } else {
            let pages = result.unwrap();
            assert_eq!(pages.len(), 2);
            assert_eq!(pages.iter().map(|page| page.items.len()).sum::<usize>(), 3);
        }
    }

    #[tokio::test]
    async fn invalid_spot_trade_request_fails_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let mut request = spot_trades_request();
        request.name = Some(" ".to_string());

        assert!(matches!(
            client.get_spot_trades(&request).await,
            Err(DeepXHttpError::InvalidRequest(_))
        ));
    }

    #[rstest]
    #[case("zero-id")]
    #[case("zero-height")]
    #[case("sell-id")]
    #[case("buy-id")]
    #[case("seller")]
    #[case("buyer")]
    #[case("pair")]
    #[case("foreign-name")]
    #[case("foreign-pair")]
    #[case("price")]
    #[case("base-amount")]
    #[case("quote-amount")]
    #[case("taker")]
    #[case("timestamp")]
    #[case("range")]
    #[case("order")]
    fn spot_trades_reject_invalid_response_semantics(#[case] mutation: &str) {
        let response: serde_json::Value =
            serde_json::from_str(SPOT_TRADES_ETH_USDC_RESPONSE).unwrap();
        let mut trade: DeepXSpotTrade =
            serde_json::from_value(response["data"]["items"][0].clone()).unwrap();
        let mut request = spot_trades_request();
        let mut previous = None;
        match mutation {
            "zero-id" => trade.id = 0,
            "zero-height" => trade.height = 0,
            "sell-id" => trade.sell_id = "invalid".to_string(),
            "buy-id" => trade.buy_id = u128::MAX.to_string(),
            "seller" => trade.seller = "invalid".to_string(),
            "buyer" => trade.buyer = "invalid".to_string(),
            "pair" => trade.pair = "0x1234".to_string(),
            "foreign-name" => trade.pair_name = "OTHER/USDC".to_string(),
            "foreign-pair" => {
                request.name = None;
                request.pair = Some(
                    "0x1111111111111111111111111111111111111111111111111111111111111111"
                        .to_string(),
                );
            }
            "price" => trade.price = Decimal::ZERO,
            "base-amount" => trade.base_amount = Decimal::ZERO,
            "quote-amount" => trade.quote_amount = Decimal::ZERO,
            "taker" => trade.taker = " ".to_string(),
            "timestamp" => trade.trade_time = "invalid".to_string(),
            "range" => request.start_ms = Some(1_789_615_631_000),
            "order" => previous = Some("2026-09-17T03:27:09Z".parse().unwrap()),
            _ => unreachable!(),
        }

        assert!(matches!(
            validate_spot_trade(0, &trade, &request, &mut previous),
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot trades",
                ..
            })
        ));
    }

    #[rstest]
    #[case("page-size")]
    #[case("total")]
    #[case("cursor")]
    #[tokio::test]
    async fn spot_trades_reject_invalid_page_metadata(#[case] mutation: &str) {
        let mut response: serde_json::Value =
            serde_json::from_str(SPOT_TRADES_ETH_USDC_RESPONSE).unwrap();
        let mut request = spot_trades_request();
        match mutation {
            "page-size" => request.page_size = Some(1),
            "total" => response["data"]["total"] = json!(1),
            "cursor" => response["data"]["nextCursor"] = serde_json::Value::Null,
            _ => unreachable!(),
        }
        let router = Router::new().route(
            SPOT_TRADES_PATH,
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();

        let error = client.get_spot_trades(&request).await.unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::InvalidHistoryResponse {
                endpoint: "spot trades",
                ..
            } | DeepXHttpError::MissingPaginationCursor {
                endpoint: "spot trades"
            }
        ));
    }

    #[tokio::test]
    async fn encodes_and_decodes_perp_trades_page_exactly() {
        let router = Router::new().route(
            PERP_TRADES_PATH,
            get(|Query(query): Query<PerpTradesQuery>| async move {
                assert_eq!(query.market_id, 3);
                assert_eq!(query.page_size, 3);
                assert_eq!(query.cursor, "opaque+/cursor=");
                assert_eq!(query.sort, "DESC");
                r#"{
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "items": [{
                            "id": 51431900000019,
                            "marketId": 3,
                            "buyerOrderId": "65043",
                            "buyer": "0x40116fee7389f89df3b716a27a38a929a51f2c4b",
                            "sellerOrderId": "65044",
                            "seller": "0xf1a9b15cf875ba3b58f78eb4ce39b74e27507465",
                            "price": 1792.600000000000001,
                            "size": 0.004000000000000001,
                            "buyerLeverage": 2,
                            "sellerLeverage": 2,
                            "createdAt": "2026-06-17T03:44:23.664Z",
                            "filledDirection": "Short",
                            "taker": "Seller",
                            "takerFee": 0.000716000000000001,
                            "makerFee": -0.000716000000000001
                        }],
                        "nextCursor": "next-page",
                        "hasNext": true
                    },
                    "fail": false
                }"#
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXPerpTradesRequest {
            market_id: 3,
            page_size: Some(3),
            cursor: Some("opaque+/cursor=".to_string()),
        };

        let page = client.get_perp_trades(&request).await.unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].id, 51_431_900_000_019);
        assert_eq!(page.items[0].buyer_leverage, Decimal::from(2));
        assert_eq!(page.items[0].seller_leverage, Decimal::from(2));
        assert_eq!(
            page.items[0].price,
            "1792.600000000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            page.items[0].size,
            "0.004000000000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            page.items[0].maker_fee,
            "-0.000716000000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(page.items[0].created_at, "2026-06-17T03:44:23.664Z");
        assert_eq!(page.next_cursor.as_deref(), Some("next-page"));
        assert!(page.has_next);
    }

    #[rstest]
    #[case("market")]
    #[case("range")]
    #[case("missing-end")]
    #[case("zero-page-size")]
    #[case("large-page-size")]
    #[case("empty-cursor")]
    #[case("page-budget")]
    #[tokio::test]
    async fn funding_history_rejects_invalid_inputs_before_transport(#[case] invalid: &str) {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let mut request = DeepXFundingRateRequest {
            market_id: 3,
            start_ms: 60_000,
            end_ms: Some(180_000),
            limit: Some(2),
            cursor: None,
        };
        let mut budget = 2;
        match invalid {
            "market" => request.market_id = 0,
            "range" => request.start_ms = 240_000,
            "missing-end" => request.end_ms = None,
            "zero-page-size" => request.limit = Some(0),
            "large-page-size" => request.limit = Some(5_001),
            "empty-cursor" => request.cursor = Some(String::new()),
            "page-budget" => budget = 0,
            _ => unreachable!(),
        }
        let error = client
            .get_perp_funding_rates_history_limited(
                &request,
                std::num::NonZeroUsize::new(3).unwrap(),
                budget,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DeepXHttpError::InvalidRequest(_) | DeepXHttpError::InvalidPaginationLimit
        ));
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
    #[case("valid")]
    #[case("equal-times")]
    #[case("empty-terminal")]
    #[case("stale-terminal-cursor")]
    #[case("duplicate")]
    #[case("ascending")]
    #[case("before-start")]
    #[case("after-end")]
    #[case("invalid-time")]
    #[case("foreign-market")]
    #[case("oversized")]
    #[case("repeat-cursor")]
    #[case("missing-cursor")]
    #[case("no-progress")]
    #[case("page-budget")]
    #[tokio::test]
    async fn bounded_trade_history_checks_every_page(#[case] scenario: &'static str) {
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
                    let mut items = vec![history_trade(
                        if first { 2 } else { 1 },
                        if first {
                            if scenario == "ascending" {
                                2_000
                            } else {
                                3_000
                            }
                        } else {
                            1_000
                        },
                    )];
                    let mut has_next = first;
                    let mut cursor = if first { Some("opaque+/cursor=") } else { None };
                    if !first {
                        match scenario {
                            "equal-times" => items[0] = history_trade(1, 3_000),
                            "empty-terminal" => items.clear(),
                            "stale-terminal-cursor" => cursor = Some("opaque+/cursor="),
                            "duplicate" => items[0] = history_trade(2, 1_000),
                            "ascending" => items[0] = history_trade(1, 2_500),
                            "before-start" => items[0] = history_trade(1, 999),
                            "after-end" => items[0] = history_trade(1, 3_001),
                            "invalid-time" => items[0]["createdAt"] = json!("not-a-timestamp"),
                            "foreign-market" => items[0]["marketId"] = json!(4),
                            "oversized" => {
                                items.push(history_trade(3, 1_000));
                                items.push(history_trade(4, 1_000));
                            }
                            "repeat-cursor" => {
                                has_next = true;
                                cursor = Some("opaque+/cursor=");
                            }
                            "missing-cursor" => has_next = true,
                            "no-progress" => {
                                items.clear();
                                has_next = true;
                                cursor = Some("next");
                            }
                            _ => {}
                        }
                    }
                    Json(json!({"code": 200, "fail": false, "msg": "success",
                        "data": {"items": items, "hasNext": has_next, "nextCursor": cursor}}))
                }
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let mut request = trades_history_request();
        if scenario == "page-budget" {
            request.max_pages = 1;
        }
        let result = client.get_perp_trades_history(&request).await;
        match scenario {
            "valid" | "equal-times" | "empty-terminal" | "stale-terminal-cursor" => {
                let trades = result.unwrap();
                assert_eq!(
                    trades.len(),
                    if scenario == "empty-terminal" { 1 } else { 2 }
                );
                assert_eq!(trades[0].id, 2);
                assert_eq!(trades[0].price.to_string(), "1792.600000000000001");
                assert_eq!(trades[0].size.to_string(), "0.004000000000000001");
                assert_eq!(trades[0].buyer_leverage, Decimal::from(25));
                assert_eq!(
                    trades[0].seller_leverage.to_string(),
                    "12.500000000000000001"
                );
                if trades.len() == 2 {
                    assert_eq!(trades[1].id, 1);
                }
            }
            "repeat-cursor" => assert!(matches!(
                result,
                Err(DeepXHttpError::RepeatedPaginationCursor { .. })
            )),
            "missing-cursor" => assert!(matches!(
                result,
                Err(DeepXHttpError::MissingPaginationCursor { .. })
            )),
            "no-progress" => assert!(matches!(
                result,
                Err(DeepXHttpError::PaginationNoProgress { .. })
            )),
            "page-budget" => assert!(matches!(
                result,
                Err(DeepXHttpError::PaginationLimitExceeded { max_pages: 1 })
            )),
            "foreign-market" => assert!(matches!(
                result,
                Err(DeepXHttpError::ResponseMarketMismatch {
                    expected: 3,
                    received: 4,
                    ..
                })
            )),
            _ => assert!(matches!(
                result,
                Err(DeepXHttpError::InvalidHistoryResponse { .. })
            )),
        }
        assert_eq!(
            calls.load(Ordering::Relaxed),
            if scenario == "page-budget" { 1 } else { 2 }
        );
    }

    #[rstest]
    #[case("market")]
    #[case("range")]
    #[case("page-size")]
    #[case("page-budget")]
    #[case("overflow")]
    #[case("timestamp")]
    #[tokio::test]
    async fn trade_history_rejects_invalid_bounds_and_budgets_before_transport(
        #[case] invalid: &str,
    ) {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let mut request = trades_history_request();
        match invalid {
            "market" => request.market_id = 0,
            "range" => request.start_ms = 4_000,
            "page-size" => request.page_size = 0,
            "page-budget" => request.max_pages = 0,
            "overflow" => request.max_pages = usize::MAX,
            "timestamp" => request.end_ms = u64::MAX,
            _ => unreachable!(),
        }
        let error = client.get_perp_trades_history(&request).await.unwrap_err();
        assert!(matches!(
            error,
            DeepXHttpError::InvalidRequest(_) | DeepXHttpError::InvalidPaginationLimit
        ));
    }

    #[tokio::test]
    async fn rejects_perp_trade_response_for_another_market() {
        let router = Router::new().route(
            PERP_TRADES_PATH,
            get(|| async {
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "items": [{
                            "id": 1,
                            "marketId": 3,
                            "buyerOrderId": "2",
                            "buyer": "0x01",
                            "sellerOrderId": "3",
                            "seller": "0x02",
                            "price": "1",
                            "size": "1",
                            "buyerLeverage": 1,
                            "sellerLeverage": 1,
                            "createdAt": "2026-09-10T00:00:00Z",
                            "filledDirection": "Long",
                            "taker": "Buyer",
                            "takerFee": "0",
                            "makerFee": "0"
                        }, {
                            "id": 2,
                            "marketId": 4,
                            "buyerOrderId": "2",
                            "buyer": "0x01",
                            "sellerOrderId": "3",
                            "seller": "0x02",
                            "price": "1",
                            "size": "1",
                            "buyerLeverage": 1,
                            "sellerLeverage": 1,
                            "createdAt": "2026-09-10T00:00:00Z",
                            "filledDirection": "Long",
                            "taker": "Buyer",
                            "takerFee": "0",
                            "makerFee": "0"
                        }],
                        "nextCursor": null,
                        "hasNext": false
                    },
                    "fail": false
                }))
            }),
        );
        let client = DeepXHttpClient::new(spawn_server(router).await, Some(5), None).unwrap();
        let request = DeepXPerpTradesRequest {
            market_id: 3,
            page_size: Some(3),
            cursor: None,
        };

        let error = client.get_perp_trades(&request).await.unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::ResponseMarketMismatch {
                endpoint: "perp trades",
                expected: 3,
                received: 4,
            }
        ));
    }

    #[tokio::test]
    async fn rejects_invalid_perp_trades_request_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let request = DeepXPerpTradesRequest {
            market_id: 3,
            page_size: Some(0),
            cursor: None,
        };

        let error = client.get_perp_trades(&request).await.unwrap_err();

        assert!(
            matches!(error, DeepXHttpError::InvalidRequest(message) if message.contains("page_size"))
        );
    }

    #[tokio::test]
    async fn rejects_perp_trades_continuation_without_cursor() {
        let router = Router::new().route(
            PERP_TRADES_PATH,
            get(|| async {
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "items": [],
                        "nextCursor": null,
                        "hasNext": true
                    },
                    "fail": false
                }))
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXPerpTradesRequest {
            market_id: 3,
            page_size: Some(3),
            cursor: None,
        };

        let error = client.get_perp_trades(&request).await.unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::MissingPaginationCursor {
                endpoint: "perp trades"
            }
        ));
    }

    #[tokio::test]
    async fn encodes_and_decodes_perp_candles_page_exactly() {
        let router = Router::new().route(
            PERP_CANDLES_PATH,
            get(|Query(query): Query<PerpCandlesQuery>| async move {
                assert_eq!(query.market_id, 3);
                assert_eq!(query.time_frame, "3m");
                assert_eq!(query.start, 1_788_254_100_000);
                assert_eq!(query.end, 1_788_257_700_000);
                assert_eq!(query.limit, 3);
                assert_eq!(query.sort, "ASC");
                assert!(!query.trade_view);
                r#"{
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "pair": "ETH-USDC",
                        "details": [{
                            "volume": 35.986000000000000001,
                            "high": 2445.310000000000001,
                            "low": 2444.920000000000001,
                            "open": 2445.310000000000001,
                            "close": 2444.920000000000001,
                            "time": 1788254100000
                        }]
                    },
                    "fail": false
                }"#
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXPerpCandlesRequest {
            market_id: 3,
            interval: DeepXPerpCandleInterval::ThreeMinutes,
            start_ms: 1_788_254_100_000,
            end_ms: Some(1_788_257_700_000),
            limit: Some(3),
        };

        let page = client.get_perp_candles(&request).await.unwrap();

        assert_eq!(page.pair, "ETH-USDC");
        assert_eq!(page.details.len(), 1);
        assert_eq!(
            page.details[0].volume,
            "35.986000000000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            page.details[0].close,
            "2444.920000000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(page.details[0].time, 1_788_254_100_000);
    }

    fn candle_page() -> DeepXPerpCandlesPage {
        serde_json::from_value(json!({
            "pair": "ETH-USDC",
            "details": [{"volume": "1.000000000000000001", "high": "3",
                "low": "1", "open": "2", "close": "3", "time": 60000}]
        }))
        .unwrap()
    }

    #[rstest]
    #[case("pair")]
    #[case("limit")]
    #[case("venue-limit")]
    #[case("duplicate")]
    #[case("descending")]
    #[case("range")]
    #[case("open")]
    #[case("close")]
    #[case("volume")]
    fn rejects_invalid_candle_page(#[case] corruption: &str) {
        let mut page = candle_page();
        let mut limit = Some(2);
        match corruption {
            "pair" => page.pair = " ".to_string(),
            "limit" => limit = Some(0),
            "venue-limit" => {
                page.details.resize(5_001, page.details[0].clone());
                limit = None;
            }
            "duplicate" => page.details.push(page.details[0].clone()),
            "descending" => {
                let mut candle = page.details[0].clone();
                candle.time -= 1;
                page.details.push(candle);
            }
            "range" => page.details[0].low = Decimal::from(4),
            "open" => page.details[0].open = Decimal::ZERO,
            "close" => page.details[0].close = Decimal::from(4),
            "volume" => page.details[0].volume = Decimal::from(-1),
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_candle_page("test history", &page, 60_000, Some(240_000), limit),
            Err(DeepXHttpError::InvalidHistoryResponse {
                endpoint: "test history",
                ..
            })
        ));
    }

    #[test]
    fn accepts_empty_sparse_and_exact_candle_pages() {
        let mut page = candle_page();
        let mut candle = page.details[0].clone();
        candle.time += 180_000;
        candle.volume = Decimal::ZERO;
        page.details.push(candle);
        validate_candle_page("test history", &page, 60_000, Some(240_000), Some(2)).unwrap();
        assert_eq!(page.details[0].volume.to_string(), "1.000000000000000001");
        page.details.clear();
        validate_candle_page("test history", &page, 60_000, Some(240_000), None).unwrap();
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
    async fn rejects_excessive_perp_candle_limit_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let request = DeepXPerpCandlesRequest {
            market_id: 3,
            interval: DeepXPerpCandleInterval::OneMinute,
            start_ms: 1,
            end_ms: Some(2),
            limit: Some(5_001),
        };

        let error = client.get_perp_candles(&request).await.unwrap_err();

        assert!(
            matches!(error, DeepXHttpError::InvalidRequest(message) if message.contains("5000"))
        );
    }

    #[tokio::test]
    async fn encodes_and_decodes_perp_mark_price_page_exactly() {
        let router = Router::new().route(
            PERP_MARK_PRICE_PATH,
            get(|Query(query): Query<PerpCandlesQuery>| async move {
                assert_eq!(query.market_id, 3);
                assert_eq!(query.time_frame, "8h");
                assert_eq!(query.start, 1_788_254_100_000);
                assert_eq!(query.end, 1_788_257_700_000);
                assert_eq!(query.limit, 3);
                assert_eq!(query.sort, "ASC");
                assert!(!query.trade_view);
                r#"{
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "pair": "ETH-USDC",
                        "details": [{
                            "volume": 35.986000000000000001,
                            "high": 2444.134646000000000001,
                            "low": 2442.917618000000000001,
                            "open": 2443.781181000000000001,
                            "close": 2442.933709000000000001,
                            "time": 1788254100000
                        }]
                    },
                    "fail": false
                }"#
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXPerpMarkPriceRequest {
            market_id: 3,
            interval: DeepXPerpCandleInterval::EightHours,
            start_ms: 1_788_254_100_000,
            end_ms: Some(1_788_257_700_000),
            limit: Some(3),
        };

        let page = client.get_perp_mark_prices(&request).await.unwrap();

        assert_eq!(page.pair, "ETH-USDC");
        assert_eq!(page.details.len(), 1);
        assert_eq!(
            page.details[0].open,
            "2443.781181000000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            page.details[0].close,
            "2442.933709000000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(page.details[0].time, 1_788_254_100_000);
    }

    #[tokio::test]
    async fn rejects_excessive_perp_mark_price_limit_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let request = DeepXPerpMarkPriceRequest {
            market_id: 3,
            interval: DeepXPerpCandleInterval::OneMinute,
            start_ms: 1,
            end_ms: Some(2),
            limit: Some(5_001),
        };

        let error = client.get_perp_mark_prices(&request).await.unwrap_err();

        assert!(
            matches!(error, DeepXHttpError::InvalidRequest(message) if message.contains("5000"))
        );
    }

    #[tokio::test]
    async fn encodes_and_decodes_perp_oracle_price_page_exactly() {
        let router = Router::new().route(
            PERP_ORACLE_PRICE_PATH,
            get(|Query(query): Query<PerpCandlesQuery>| async move {
                assert_eq!(query.market_id, 3);
                assert_eq!(query.time_frame, "1M");
                assert_eq!(query.start, 1_788_255_060_000);
                assert_eq!(query.end, 1_788_258_660_000);
                assert_eq!(query.limit, 3);
                assert_eq!(query.sort, "ASC");
                assert!(!query.trade_view);
                r#"{
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "pair": "ETH-USDC",
                        "details": [{
                            "volume": 40.288000000000000001,
                            "high": 2451.350920760000000001,
                            "low": 2451.312364720000000001,
                            "open": 2451.312364720000000001,
                            "close": 2451.312365970000000001,
                            "time": 1788255060000
                        }]
                    },
                    "fail": false
                }"#
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXPerpOraclePriceRequest {
            market_id: 3,
            interval: DeepXPerpCandleInterval::OneMonth,
            start_ms: 1_788_255_060_000,
            end_ms: Some(1_788_258_660_000),
            limit: Some(3),
        };

        let page = client.get_perp_oracle_prices(&request).await.unwrap();

        assert_eq!(page.pair, "ETH-USDC");
        assert_eq!(page.details.len(), 1);
        assert_eq!(
            page.details[0].open,
            "2451.312364720000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            page.details[0].close,
            "2451.312365970000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(page.details[0].time, 1_788_255_060_000);
    }

    #[tokio::test]
    async fn rejects_excessive_perp_oracle_price_limit_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let request = DeepXPerpOraclePriceRequest {
            market_id: 3,
            interval: DeepXPerpCandleInterval::OneMinute,
            start_ms: 1,
            end_ms: Some(2),
            limit: Some(5_001),
        };

        let error = client.get_perp_oracle_prices(&request).await.unwrap_err();

        assert!(
            matches!(error, DeepXHttpError::InvalidRequest(message) if message.contains("5000"))
        );
    }

    #[tokio::test]
    async fn encodes_and_decodes_perp_volume_exactly() {
        let router = Router::new().route(
            PERP_VOLUME_PATH,
            get(|Query(query): Query<PerpVolumeQuery>| async move {
                assert_eq!(query.market_id, 3);
                assert_eq!(query.period, "1h");
                r#"{
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "totalVolume": 2066.304000000000000001,
                        "tradeCount": 2421,
                        "startTime": 1788255664844,
                        "endTime": 1788259264844,
                        "statisticTime": 1788259264844
                    },
                    "fail": false
                }"#
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXPerpVolumeRequest {
            market_id: 3,
            period: DeepXPerpVolumePeriod::OneHour,
        };

        let volume = client.get_perp_volume(&request).await.unwrap();

        assert_eq!(
            volume.total_volume,
            "2066.304000000000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(volume.trade_count, 2_421);
        assert_eq!(volume.end_time - volume.start_time, 3_600_000);
        assert_eq!(volume.statistic_time, volume.end_time);
    }

    #[tokio::test]
    async fn decodes_perp_volume_fixture_through_client() {
        let router = Router::new().route(
            PERP_VOLUME_PATH,
            get(|RawQuery(raw_query): RawQuery| async move {
                assert_eq!(raw_query.as_deref(), Some("marketId=3&period=1h"));
                PERP_VOLUME_1H_RESPONSE
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXPerpVolumeRequest {
            market_id: 3,
            period: DeepXPerpVolumePeriod::OneHour,
        };

        let volume = client.get_perp_volume(&request).await.unwrap();

        assert_eq!(volume.total_volume, Decimal::new(2_117_975, 3));
        assert_eq!(volume.trade_count, 2_492);
        assert_eq!(volume.start_time, 1_788_256_255_843);
        assert_eq!(volume.end_time, 1_788_259_855_843);
        assert_eq!(volume.statistic_time, 1_788_259_855_843);
    }

    #[tokio::test]
    async fn rejects_invalid_perp_volume_market_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let request = DeepXPerpVolumeRequest {
            market_id: 0,
            period: DeepXPerpVolumePeriod::TwentyFourHours,
        };

        let error = client.get_perp_volume(&request).await.unwrap_err();

        assert!(
            matches!(error, DeepXHttpError::InvalidRequest(message) if message.contains("market_id"))
        );
    }

    #[tokio::test]
    async fn encodes_and_decodes_perp_last_price_exactly() {
        let router = Router::new().route(
            PERP_LAST_PRICE_PATH,
            get(|Query(query): Query<PerpLastPriceQuery>| async move {
                assert_eq!(query.market_id, 3);
                r#"{
                    "code": 200,
                    "msg": "success",
                    "data": 2453.980000000000001,
                    "fail": false
                }"#
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXPerpLastPriceRequest { market_id: 3 };

        let last_price = client.get_perp_last_price(&request).await.unwrap();

        assert_eq!(
            last_price.0,
            "2453.980000000000001".parse::<Decimal>().unwrap()
        );
    }

    #[tokio::test]
    async fn rejects_invalid_perp_last_price_market_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let request = DeepXPerpLastPriceRequest { market_id: 0 };

        let error = client.get_perp_last_price(&request).await.unwrap_err();

        assert!(
            matches!(error, DeepXHttpError::InvalidRequest(message) if message.contains("market_id"))
        );
    }

    #[tokio::test]
    async fn rejects_invalid_funding_rate_bounds_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let request = DeepXFundingRateRequest {
            market_id: 3,
            start_ms: 2,
            end_ms: Some(1),
            limit: Some(5),
            cursor: None,
        };

        let error = client.get_perp_funding_rates(&request).await.unwrap_err();

        assert!(
            matches!(error, DeepXHttpError::InvalidRequest(message) if message.contains("start_ms"))
        );
    }

    #[tokio::test]
    async fn encodes_and_decodes_perp_open_interest_page_exactly() {
        let router = Router::new().route(
            PERP_OPEN_INTEREST_PATH,
            get(|Query(query): Query<OpenInterestQuery>| async move {
                assert_eq!(query.market_id, 3);
                assert_eq!(query.time_frame, "1m");
                assert_eq!(query.start, 1_788_251_280_000);
                assert_eq!(query.end, 1_788_254_880_000);
                assert_eq!(query.limit, 5);
                assert_eq!(query.sort, "ASC");
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "data": {
                        "pair": "ETH-USDC",
                        "details": [{
                            "total_oi": "3393.618000000000001",
                            "long_short_ratio": "1.8456014362657092",
                            "long_position_count": 2056,
                            "short_position_count": 1114,
                            "statistic_time": 1_788_251_280_000_u64
                        }]
                    },
                    "fail": false
                }))
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(base_url, Some(5), None).unwrap();
        let request = DeepXOpenInterestRequest {
            market_id: 3,
            start_ms: 1_788_251_280_000,
            end_ms: Some(1_788_254_880_000),
            limit: Some(5),
        };

        let page = client.get_perp_open_interest(&request).await.unwrap();

        assert_eq!(page.pair, "ETH-USDC");
        assert_eq!(page.details.len(), 1);
        assert_eq!(
            page.details[0].total_oi,
            "3393.618000000000001".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            page.details[0].long_short_ratio,
            "1.8456014362657092".parse::<Decimal>().unwrap()
        );
        assert_eq!(page.details[0].long_position_count, 2056);
        assert_eq!(page.details[0].short_position_count, 1114);
        assert_eq!(page.details[0].statistic_time, 1_788_251_280_000);
    }

    #[tokio::test]
    async fn rejects_invalid_open_interest_market_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let request = DeepXOpenInterestRequest {
            market_id: 0,
            start_ms: 1,
            end_ms: Some(2),
            limit: Some(5),
        };

        let error = client.get_perp_open_interest(&request).await.unwrap_err();

        assert!(
            matches!(error, DeepXHttpError::InvalidRequest(message) if message.contains("market_id"))
        );
    }

    #[tokio::test]
    async fn rejects_venue_failure_in_successful_http_response() {
        let router = Router::new().route(
            SPOT_MARKETS_PATH,
            get(|| async {
                Json(json!({
                    "code": 10010,
                    "msg": "rate limit exceeded",
                    "data": [],
                    "fail": true
                }))
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(format!("{base_url}/"), Some(5), None).unwrap();

        let error = client.get_spot_markets().await.unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::Api {
                code: DeepXResponseCode::Api(10010),
                message,
            } if message == "rate limit exceeded"
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
    async fn preserves_on_chain_failure_code_in_successful_http_response() {
        let router = Router::new().route(
            SPOT_MARKETS_PATH,
            get(|| async {
                Json(json!({
                    "code": "19_0",
                    "msg": "Subaccount not initialized",
                    "data": [],
                    "fail": true
                }))
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXHttpClient::new(format!("{base_url}/"), Some(5), None).unwrap();

        let error = client.get_spot_markets().await.unwrap_err();

        assert!(matches!(
            error,
            DeepXHttpError::Api {
                code: DeepXResponseCode::Pallet(code),
                message,
            } if code == "19_0" && message == "Subaccount not initialized"
        ));
    }

    #[tokio::test]
    async fn fails_over_to_next_endpoint_after_retryable_status() {
        let primary = spawn_server(Router::new().route(
            "/health",
            get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "unavailable") }),
        ))
        .await;
        let secondary = spawn_server(
            Router::new().route("/health", get(|| async { Json(json!({ "status": "ok" })) })),
        )
        .await;
        let client = DeepXHttpClient::new_with_endpoints(
            [primary, secondary],
            Some(1),
            None,
            immediate_retry_config(1),
        )
        .unwrap();

        let response = client.get_json::<HealthResponse>("/health").await.unwrap();

        assert_eq!(response.status, "ok");
    }

    #[tokio::test]
    async fn network_configured_zero_retries_sends_one_request() {
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_clone = Arc::clone(&requests);
        let base_url = spawn_server(Router::new().route(
            "/health",
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

        let error = client
            .get_json::<HealthResponse>("/health")
            .await
            .unwrap_err();

        assert!(matches!(error, DeepXHttpError::Http { status: 503, .. }));
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fails_over_to_next_endpoint_after_transport_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let secondary = spawn_server(
            Router::new().route("/health", get(|| async { Json(json!({ "status": "ok" })) })),
        )
        .await;
        let client = DeepXHttpClient::new_with_endpoints(
            [unavailable, secondary],
            Some(1),
            None,
            immediate_retry_config(1),
        )
        .unwrap();

        let response = client.get_json::<HealthResponse>("/health").await.unwrap();

        assert_eq!(response.status, "ok");
    }

    #[tokio::test]
    async fn does_not_fail_over_after_terminal_status() {
        let secondary_requests = Arc::new(AtomicUsize::new(0));
        let primary = spawn_server(Router::new().route(
            "/health",
            get(|| async { (StatusCode::BAD_REQUEST, "invalid request") }),
        ))
        .await;
        let secondary_requests_clone = Arc::clone(&secondary_requests);
        let secondary = spawn_server(Router::new().route(
            "/health",
            get(move || {
                let secondary_requests = Arc::clone(&secondary_requests_clone);
                async move {
                    secondary_requests.fetch_add(1, Ordering::SeqCst);
                    Json(json!({ "status": "ok" }))
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

        let error = client
            .get_json::<HealthResponse>("/health")
            .await
            .unwrap_err();

        assert!(matches!(error, DeepXHttpError::Http { status: 400, .. }));
        assert_eq!(secondary_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn does_not_fail_over_after_decode_failure() {
        let secondary_requests = Arc::new(AtomicUsize::new(0));
        let primary =
            spawn_server(Router::new().route("/health", get(|| async { "not json" }))).await;
        let secondary_requests_clone = Arc::clone(&secondary_requests);
        let secondary = spawn_server(Router::new().route(
            "/health",
            get(move || {
                let secondary_requests = Arc::clone(&secondary_requests_clone);
                async move {
                    secondary_requests.fetch_add(1, Ordering::SeqCst);
                    Json(json!({ "status": "ok" }))
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

        let error = client
            .get_json::<HealthResponse>("/health")
            .await
            .unwrap_err();

        assert!(matches!(error, DeepXHttpError::Decode(_)));
        assert_eq!(secondary_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn preserves_final_error_after_retry_exhaustion() {
        let primary = spawn_server(Router::new().route(
            "/health",
            get(|| async { (StatusCode::BAD_GATEWAY, "primary") }),
        ))
        .await;
        let secondary = spawn_server(Router::new().route(
            "/health",
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

        let error = client
            .get_json::<HealthResponse>("/health")
            .await
            .unwrap_err();

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

    #[rstest]
    #[case("health")]
    #[case("//other-host/health")]
    #[case("/health?token=secret")]
    #[case("https://other-host/health")]
    #[case("/internal/v1/../health")]
    fn rejects_paths_outside_the_configured_base(#[case] path: &str) {
        assert!(matches!(
            validate_path(path),
            Err(DeepXHttpError::InvalidPath(_)),
        ));
    }
}
