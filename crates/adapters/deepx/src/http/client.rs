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

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use nautilus_network::{
    http::{HttpClient, Method},
    retry::{RetryConfig, RetryManager},
};
use serde::{Serialize, de::DeserializeOwned};

use super::{
    error::{DeepXHttpError, Result},
    models::{
        DeepXApiResponse, DeepXFundingRatePage, DeepXLongShortRatioPage, DeepXOpenInterestPage,
        DeepXPerpCandlesPage, DeepXPerpMarket, DeepXPerpTradesPage, DeepXPerpVolume,
        DeepXSpotMarket,
    },
    query::{
        DeepXFundingRateRequest, DeepXLongShortRatioRequest, DeepXOpenInterestRequest,
        DeepXPerpCandlesRequest, DeepXPerpMarkPriceRequest, DeepXPerpOraclePriceRequest,
        DeepXPerpTradesRequest, DeepXPerpVolumeRequest,
    },
    retry::{deepx_http_retry_config, should_retry_http_error},
};

const SPOT_MARKETS_PATH: &str = "/internal/v1/market/spot/markets";
const PERP_MARKETS_PATH: &str = "/internal/v1/market/perp/markets";
const PERP_FUNDING_RATE_PATH: &str = "/internal/v1/market/perp/funding_rate";
const PERP_LONG_SHORT_RATIO_PATH: &str = "/internal/v1/market/perp/long_short_ratio";
const PERP_OPEN_INTEREST_PATH: &str = "/internal/v1/market/perp/open_interest";
const PERP_TRADES_PATH: &str = "/internal/v1/market/perp/trades";
const PERP_CANDLES_PATH: &str = "/internal/v1/market/perp/candles";
const PERP_MARK_PRICE_PATH: &str = "/internal/v1/market/perp/mark_price";
const PERP_ORACLE_PRICE_PATH: &str = "/internal/v1/market/perp/oracle_price";
const PERP_VOLUME_PATH: &str = "/internal/v1/market/perp/volume";

const MAX_ERROR_BODY_CHARS: usize = 1_024;

/// Raw client for unauthenticated, idempotent DeepX HTTP reads.
#[derive(Clone, Debug)]
pub struct DeepXHttpClient {
    client: HttpClient,
    base_urls: Arc<[String]>,
    timeout_secs: Option<u64>,
    retry_manager: Arc<RetryManager<DeepXHttpError>>,
}

impl DeepXHttpClient {
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
            .maybe_proxy_url(proxy_url)
            .build()?;
        Ok(Self {
            client,
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

    /// Returns all available Spot market metadata.
    ///
    /// # Errors
    ///
    /// Returns a typed transport, HTTP status, response decoding, or venue API error.
    pub async fn get_spot_markets(&self) -> Result<Vec<DeepXSpotMarket>> {
        self.get_market_data(SPOT_MARKETS_PATH).await
    }

    /// Returns all available perpetual market metadata.
    ///
    /// # Errors
    ///
    /// Returns a typed transport, HTTP status, response decoding, or venue API error.
    pub async fn get_perp_markets(&self) -> Result<Vec<DeepXPerpMarket>> {
        self.get_market_data(PERP_MARKETS_PATH).await
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
        let response: DeepXApiResponse<DeepXFundingRatePage> = self
            .get_json_with_query(PERP_FUNDING_RATE_PATH, &query)
            .await?;
        into_api_data(response)
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
        let response: DeepXApiResponse<DeepXLongShortRatioPage> = self
            .get_json_with_query(PERP_LONG_SHORT_RATIO_PATH, &query)
            .await?;
        into_api_data(response)
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
        let response: DeepXApiResponse<DeepXOpenInterestPage> = self
            .get_json_with_query(PERP_OPEN_INTEREST_PATH, &query)
            .await?;
        into_api_data(response)
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
        let response: DeepXApiResponse<DeepXPerpTradesPage> =
            self.get_json_with_query(PERP_TRADES_PATH, &query).await?;
        into_api_data(response)
    }

    /// Returns one ascending page of raw one-minute perpetual candles.
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
        let response: DeepXApiResponse<DeepXPerpCandlesPage> =
            self.get_json_with_query(PERP_CANDLES_PATH, &query).await?;
        into_api_data(response)
    }

    /// Returns one ascending page of raw one-minute perpetual mark-price history.
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
        let response: DeepXApiResponse<DeepXPerpCandlesPage> = self
            .get_json_with_query(PERP_MARK_PRICE_PATH, &query)
            .await?;
        into_api_data(response)
    }

    /// Returns one ascending page of raw one-minute perpetual oracle-price history.
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
        let response: DeepXApiResponse<DeepXPerpCandlesPage> = self
            .get_json_with_query(PERP_ORACLE_PRICE_PATH, &query)
            .await?;
        into_api_data(response)
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
        let response: DeepXApiResponse<DeepXPerpVolume> =
            self.get_json_with_query(PERP_VOLUME_PATH, &query).await?;
        into_api_data(response)
    }

    async fn get_market_data<T>(&self, path: &str) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        into_api_data(self.get_json(path).await?)
    }

    async fn get_json_with_query<R, Q>(&self, path: &str, query: &Q) -> Result<R>
    where
        R: DeserializeOwned,
        Q: Serialize + Sync,
    {
        validate_path(path)?;
        let attempt = AtomicUsize::new(0);
        self.retry_manager
            .execute_with_retry(
                "DeepX public HTTP GET",
                || {
                    let index = attempt.fetch_add(1, Ordering::Relaxed) % self.base_urls.len();
                    self.get_json_once_with_query(&self.base_urls[index], path, query)
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

fn into_api_data<T>(response: DeepXApiResponse<T>) -> Result<T> {
    if response.fail || response.code != 200 {
        return Err(DeepXHttpError::Api {
            code: response.code,
            message: response.msg,
        });
    }
    Ok(response.data)
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

    use axum::{Json, Router, extract::Query, http::StatusCode, routing::get};
    use nautilus_network::retry::RetryConfig;
    use rstest::rstest;
    use rust_decimal::Decimal;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;
    use crate::http::DeepXPerpVolumePeriod;

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
    async fn encodes_and_decodes_perp_candles_page_exactly() {
        let router = Router::new().route(
            PERP_CANDLES_PATH,
            get(|Query(query): Query<PerpCandlesQuery>| async move {
                assert_eq!(query.market_id, 3);
                assert_eq!(query.time_frame, "1m");
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

    #[tokio::test]
    async fn rejects_excessive_perp_candle_limit_before_transport() {
        let client = DeepXHttpClient::new("http://127.0.0.1:1", Some(1), None).unwrap();
        let request = DeepXPerpCandlesRequest {
            market_id: 3,
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
                assert_eq!(query.time_frame, "1m");
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
                assert_eq!(query.time_frame, "1m");
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
                    "code": 429,
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
            DeepXHttpError::Api { code: 429, message } if message == "rate limit exceeded"
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
