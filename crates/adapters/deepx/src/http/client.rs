// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Raw HTTP client for DeepX REST endpoints.

use std::collections::HashMap;

use nautilus_core::consts::NAUTILUS_USER_AGENT;
use nautilus_network::http::{HttpClient, HttpResponse, Method, USER_AGENT};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{
    error::{DeepXHttpError, DeepXHttpResult},
    models::{
        DeepXChainTxResponse, DeepXOrderBookSnapshot, DeepXPerpetualMarket,
        DeepXRawAccountSnapshot, DeepXSignedExtrinsicRequest,
    },
    query::{
        DeepXBalanceEventQuery, DeepXCandleQuery, DeepXHistoryQuery, DeepXLiquidationQuery,
        DeepXOrderBookQuery, DeepXOrderHistoryQuery, DeepXPerpMarketHistoryQuery, DeepXSymbolQuery,
    },
};
use crate::common::{enums::DeepXEnvironment, urls};
use crate::execution::{DeepXCancelPerpOrder, DeepXExtrinsicSigner, DeepXPlacePerpOrder};

const PLACE_PERP_ORDER_PATH: &str = "/v1/chain/tx/placePerpOrder";
const CANCEL_PERP_ORDER_PATH: &str = "/v1/chain/tx/cancelPerpOrder";

#[derive(Deserialize)]
#[serde(untagged)]
enum DeepXRelayResponse<T> {
    Envelope { data: T },
    Direct(T),
}

impl<T> DeepXRelayResponse<T> {
    fn into_inner(self) -> T {
        match self {
            Self::Envelope { data } | Self::Direct(data) => data,
        }
    }
}

/// Low-level DeepX REST client returning venue JSON without assuming response schemas.
#[derive(Clone, Debug)]
pub struct DeepXRawHttpClient {
    base_url: String,
    client: HttpClient,
}

impl DeepXRawHttpClient {
    /// Creates a DeepX REST client.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying HTTP client cannot be built.
    pub fn new(
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> DeepXHttpResult<Self> {
        let base_url =
            base_url.unwrap_or_else(|| urls::rest_url(DeepXEnvironment::default()).to_string());
        let headers = HashMap::from([(USER_AGENT.to_string(), NAUTILUS_USER_AGENT.to_string())]);
        let client = HttpClient::builder()
            .headers(headers)
            .timeout_secs(timeout_secs)
            .maybe_proxy_url(proxy_url)
            .build()
            .map_err(|e| DeepXHttpError::Validation(e.to_string()))?;

        Ok(Self { base_url, client })
    }

    /// Returns the configured REST base URL.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns current perpetual positions for a subaccount.
    pub async fn get_perp_positions(
        &self,
        address: &str,
        query: &DeepXSymbolQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        self.get(
            &format!("/v1/account/subaccounts/{address}/perp/positions"),
            Some(query),
        )
        .await
    }

    /// Returns perpetual position history for a subaccount.
    pub async fn get_perp_position_history(
        &self,
        address: &str,
        query: &DeepXHistoryQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        query.validate()?;
        self.get(
            &format!("/v1/account/subaccounts/{address}/perp/positions/history"),
            Some(query),
        )
        .await
    }

    /// Returns perpetual order history for a subaccount.
    pub async fn get_perp_orders(
        &self,
        address: &str,
        query: &DeepXOrderHistoryQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        query.validate()?;
        self.get(
            &format!("/v1/account/subaccounts/{address}/perp/orders"),
            Some(query),
        )
        .await
    }

    /// Returns open perpetual orders for a subaccount.
    pub async fn get_open_perp_orders(
        &self,
        address: &str,
        query: &DeepXSymbolQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        self.get(
            &format!("/v1/account/subaccounts/{address}/perp/orders/open"),
            Some(query),
        )
        .await
    }

    /// Returns one perpetual order by subaccount address and order ID.
    pub async fn get_perp_order_by_id(
        &self,
        address: &str,
        order_id: u64,
    ) -> DeepXHttpResult<serde_json::Value> {
        self.get::<serde_json::Value, ()>(
            &format!("/v1/account/subaccounts/{address}/perp/orders/{order_id}"),
            None,
        )
        .await
    }

    /// Returns one perpetual order by transaction hash.
    pub async fn get_perp_order_by_tx(&self, tx_hash: &str) -> DeepXHttpResult<serde_json::Value> {
        self.get::<serde_json::Value, ()>(&format!("/v1/account/perp/orders/tx/{tx_hash}"), None)
            .await
    }

    /// Returns perpetual trades for a subaccount.
    pub async fn get_perp_trades(
        &self,
        address: &str,
        query: &DeepXHistoryQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        query.validate()?;
        self.get(
            &format!("/v1/account/subaccounts/{address}/perp/trades"),
            Some(query),
        )
        .await
    }

    /// Returns perpetual funding payments for a subaccount.
    pub async fn get_perp_funding_payments(
        &self,
        address: &str,
        query: &DeepXHistoryQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        query.validate()?;
        self.get(
            &format!("/v1/account/subaccounts/{address}/perp/funding-payments"),
            Some(query),
        )
        .await
    }

    /// Returns balances for a subaccount.
    pub async fn get_balances(&self, address: &str) -> DeepXHttpResult<serde_json::Value> {
        self.get::<serde_json::Value, ()>(
            &format!("/v1/account/subaccounts/{address}/balances"),
            None,
        )
        .await
    }

    /// Returns portfolio metrics for a subaccount.
    pub async fn get_portfolio(&self, address: &str) -> DeepXHttpResult<serde_json::Value> {
        self.get::<serde_json::Value, ()>(
            &format!("/v1/account/subaccounts/{address}/portfolio"),
            None,
        )
        .await
    }

    /// Collects schema-neutral responses from the current-account endpoints.
    ///
    /// The requests execute concurrently and do not represent an atomic account snapshot. The
    /// returned payloads must not be interpreted as authoritative reconciliation without matching
    /// private response schemas and sequencing guarantees.
    pub async fn get_raw_account_snapshot(
        &self,
        address: &str,
        symbol_query: &DeepXSymbolQuery,
        history_query: &DeepXHistoryQuery,
    ) -> DeepXHttpResult<DeepXRawAccountSnapshot> {
        history_query.validate()?;
        let order_query = DeepXOrderHistoryQuery {
            history: history_query.clone(),
            side: None,
        };
        let (balances, portfolio, positions, open_orders, order_history, trades) = tokio::try_join!(
            self.get_balances(address),
            self.get_portfolio(address),
            self.get_perp_positions(address, symbol_query),
            self.get_open_perp_orders(address, symbol_query),
            self.get_perp_orders(address, &order_query),
            self.get_perp_trades(address, history_query),
        )?;

        Ok(DeepXRawAccountSnapshot {
            balances,
            portfolio,
            positions,
            open_orders,
            order_history,
            trades,
        })
    }

    /// Returns balance events for a subaccount.
    pub async fn get_balance_events(
        &self,
        address: &str,
        query: &DeepXBalanceEventQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        query.validate()?;
        self.get(
            &format!("/v1/account/subaccounts/{address}/balance-events"),
            Some(query),
        )
        .await
    }

    /// Returns liquidation history for a subaccount.
    pub async fn get_liquidations(
        &self,
        address: &str,
        query: &DeepXLiquidationQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        query.validate()?;
        self.get(
            &format!("/v1/account/subaccounts/{address}/liquidations"),
            Some(query),
        )
        .await
    }

    /// Returns all public perpetual markets.
    pub async fn get_perp_markets(&self) -> DeepXHttpResult<Vec<DeepXPerpetualMarket>> {
        self.get::<Vec<DeepXPerpetualMarket>, ()>("/v1/perp/markets", None)
            .await
    }

    /// Returns one public perpetual market by symbol.
    pub async fn get_perp_market(&self, symbol: &str) -> DeepXHttpResult<DeepXPerpetualMarket> {
        self.get::<DeepXPerpetualMarket, ()>(&format!("/v1/perp/markets/{symbol}"), None)
            .await
    }

    /// Returns perpetual candles for a market symbol.
    pub async fn get_perp_candles(
        &self,
        symbol: &str,
        query: &DeepXCandleQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        query.validate()?;
        self.get(&format!("/v1/perp/markets/{symbol}/candles"), Some(query))
            .await
    }

    /// Returns public perpetual trades for a market symbol.
    pub async fn get_public_perp_trades(
        &self,
        symbol: &str,
        query: &DeepXHistoryQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        query.validate()?;
        self.get(&format!("/v1/perp/markets/{symbol}/trades"), Some(query))
            .await
    }

    /// Returns a perpetual order book snapshot.
    pub async fn get_perp_order_book(
        &self,
        symbol: &str,
        query: &DeepXOrderBookQuery,
    ) -> DeepXHttpResult<DeepXOrderBookSnapshot> {
        query.validate()?;
        self.get(&format!("/v1/perp/markets/{symbol}/orderbook"), Some(query))
            .await
    }

    /// Returns current open interest for a perpetual market.
    pub async fn get_perp_open_interest(&self, symbol: &str) -> DeepXHttpResult<serde_json::Value> {
        self.get::<serde_json::Value, ()>(&format!("/v1/perp/markets/{symbol}/open-interest"), None)
            .await
    }

    /// Returns open interest history for a perpetual market.
    pub async fn get_perp_open_interest_history(
        &self,
        symbol: &str,
        query: &DeepXPerpMarketHistoryQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        query.validate(5_000)?;
        self.get(
            &format!("/v1/perp/markets/{symbol}/open-interest/history"),
            Some(query),
        )
        .await
    }

    /// Returns current funding rate for a perpetual market.
    pub async fn get_perp_funding_rate(&self, symbol: &str) -> DeepXHttpResult<serde_json::Value> {
        self.get::<serde_json::Value, ()>(&format!("/v1/perp/markets/{symbol}/funding-rate"), None)
            .await
    }

    /// Returns funding rate history for a perpetual market.
    pub async fn get_perp_funding_rate_history(
        &self,
        symbol: &str,
        query: &DeepXPerpMarketHistoryQuery,
    ) -> DeepXHttpResult<serde_json::Value> {
        query.validate(5_000)?;
        self.get(
            &format!("/v1/perp/markets/{symbol}/funding-rate/history"),
            Some(query),
        )
        .await
    }

    /// Returns the current long/short ratio for a perpetual market.
    pub async fn get_perp_long_short_ratio(
        &self,
        symbol: &str,
    ) -> DeepXHttpResult<serde_json::Value> {
        self.get::<serde_json::Value, ()>(
            &format!("/v1/perp/markets/{symbol}/long-short-ratio"),
            None,
        )
        .await
    }

    /// Returns long/short ratio history for a perpetual market.
    pub async fn get_perp_long_short_ratio_history(
        &self,
        symbol: &str,
        query: Option<&DeepXPerpMarketHistoryQuery>,
    ) -> DeepXHttpResult<serde_json::Value> {
        let default_query = DeepXPerpMarketHistoryQuery {
            interval: "1m".to_string(),
            limit: None,
            start_time: None,
            end_time: None,
            sort: None,
        };
        let query = query.unwrap_or(&default_query);
        query.validate(5_000)?;
        self.get(
            &format!("/v1/perp/markets/{symbol}/long-short-ratio/history"),
            Some(query),
        )
        .await
    }

    /// Submits an already signed order placement extrinsic.
    pub async fn submit_place_perp_order(
        &self,
        signed_extrinsic: &str,
    ) -> DeepXHttpResult<DeepXChainTxResponse> {
        self.submit_signed_extrinsic(PLACE_PERP_ORDER_PATH, signed_extrinsic)
            .await
    }

    /// Submits an already signed order cancellation extrinsic.
    pub async fn submit_cancel_perp_order(
        &self,
        signed_extrinsic: &str,
    ) -> DeepXHttpResult<DeepXChainTxResponse> {
        self.submit_signed_extrinsic(CANCEL_PERP_ORDER_PATH, signed_extrinsic)
            .await
    }

    /// Signs and submits `PerpMarket.place_order` entirely in Rust.
    ///
    /// # Errors
    ///
    /// Returns an error when runtime constraints reject the order, or when querying metadata,
    /// runtime encoding, signing, or REST submission fails.
    pub async fn place_perp_order(
        &self,
        signer: &DeepXExtrinsicSigner,
        request: &DeepXPlacePerpOrder,
        nonce: u64,
    ) -> DeepXHttpResult<DeepXChainTxResponse> {
        let constraints = signer.perp_market_constraints(request.market_id).await?;
        constraints.validate_limit_order(request.size, request.price)?;
        let signed_extrinsic = signer.sign_place_perp_order(request, nonce).await?;
        self.submit_place_perp_order(&signed_extrinsic).await
    }

    /// Signs and submits `PerpMarket.cancel_order` entirely in Rust.
    ///
    /// # Errors
    ///
    /// Returns an error when runtime encoding, signing, or REST submission fails.
    pub async fn cancel_perp_order(
        &self,
        signer: &DeepXExtrinsicSigner,
        request: &DeepXCancelPerpOrder,
        nonce: u64,
    ) -> DeepXHttpResult<DeepXChainTxResponse> {
        let signed_extrinsic = signer.sign_cancel_perp_order(request, nonce).await?;
        self.submit_cancel_perp_order(&signed_extrinsic).await
    }

    async fn get<T, P>(&self, path: &str, query: Option<&P>) -> DeepXHttpResult<T>
    where
        T: DeserializeOwned,
        P: Serialize,
    {
        let response = self
            .client
            .request_with_params(Method::GET, self.url(path), query, None, None, None, None)
            .await?;
        Self::parse_response(&response)
    }

    async fn submit_signed_extrinsic<T>(
        &self,
        path: &str,
        signed_extrinsic: &str,
    ) -> DeepXHttpResult<T>
    where
        T: DeserializeOwned,
    {
        let payload = DeepXSignedExtrinsicRequest {
            signed_extrinsic: signed_extrinsic.to_string(),
        };
        let body = serde_json::to_vec(&payload)?;
        let headers = HashMap::from([("Content-Type".to_string(), "application/json".to_string())]);
        let response = self
            .client
            .request(
                Method::POST,
                self.url(path),
                None,
                Some(headers),
                Some(body),
                None,
                None,
            )
            .await?;
        Self::parse_response::<DeepXRelayResponse<T>>(&response).map(DeepXRelayResponse::into_inner)
    }

    fn parse_response<T: DeserializeOwned>(response: &HttpResponse) -> DeepXHttpResult<T> {
        if !response.status.is_success() {
            return Err(DeepXHttpError::Http {
                status: response.status.as_u16(),
                body: String::from_utf8_lossy(&response.body).to_string(),
            });
        }
        Ok(serde_json::from_slice(&response.body)?)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url.trim_end_matches('/'))
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        Json, Router,
        http::{StatusCode, Uri},
        routing::{get, post},
    };

    use super::*;
    use crate::{
        common::credential::DEEPX_TESTNET_SUBACCOUNT_ADDRESS,
        http::query::{DeepXOrderSide, DeepXSortDirection},
    };

    #[rstest::rstest]
    fn constructs_with_default_url() {
        let client = DeepXRawHttpClient::new(None, 5, None).unwrap();

        assert_eq!(client.base_url(), urls::rest_url(DeepXEnvironment::Testnet));
    }

    #[tokio::test]
    async fn requests_order_history_with_camel_case_query() {
        async fn handler(uri: Uri) -> Json<serde_json::Value> {
            let query = uri.query().unwrap();
            assert!(query.contains("fromId=9"));
            assert!(query.contains("startTime=100"));
            assert!(query.contains("sort=DESC"));
            assert!(query.contains("side=Buy"));
            Json(serde_json::json!({"orders": [{"id": 9}]}))
        }

        let router = Router::new().route("/v1/account/subaccounts/0xsub/perp/orders", get(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRawHttpClient::new(Some(format!("http://{address}")), 5, None).unwrap();
        let query = DeepXOrderHistoryQuery {
            history: DeepXHistoryQuery {
                from_id: Some(9),
                start_time: Some(100),
                sort: Some(DeepXSortDirection::Desc),
                ..Default::default()
            },
            side: Some(DeepXOrderSide::Buy),
        };

        let response = client.get_perp_orders("0xsub", &query).await.unwrap();

        assert_eq!(response["orders"][0]["id"], 9);
    }

    #[tokio::test]
    async fn collects_raw_account_snapshot_without_interpreting_payloads() {
        async fn handler(uri: Uri) -> Json<serde_json::Value> {
            match uri.path() {
                "/v1/account/subaccounts/0xsub/balances" => {
                    Json(serde_json::json!({"rawBalances": [1]}))
                }
                "/v1/account/subaccounts/0xsub/portfolio" => {
                    Json(serde_json::json!({"rawPortfolio": [2]}))
                }
                "/v1/account/subaccounts/0xsub/perp/positions" => {
                    assert_eq!(uri.query(), Some("symbol=ETH-USDC"));
                    Json(serde_json::json!({"rawPositions": [3]}))
                }
                "/v1/account/subaccounts/0xsub/perp/orders/open" => {
                    assert_eq!(uri.query(), Some("symbol=ETH-USDC"));
                    Json(serde_json::json!({"rawOpenOrders": [4]}))
                }
                "/v1/account/subaccounts/0xsub/perp/orders" => {
                    let query = uri.query().unwrap();
                    assert!(query.contains("symbol=ETH-USDC"));
                    assert!(query.contains("limit=100"));
                    assert!(query.contains("sort=DESC"));
                    Json(serde_json::json!({"rawOrderHistory": [5]}))
                }
                "/v1/account/subaccounts/0xsub/perp/trades" => {
                    let query = uri.query().unwrap();
                    assert!(query.contains("symbol=ETH-USDC"));
                    assert!(query.contains("limit=100"));
                    assert!(query.contains("sort=DESC"));
                    Json(serde_json::json!({"rawTrades": [6]}))
                }
                path => panic!("unexpected account snapshot path: {path}"),
            }
        }

        let router = Router::new().fallback(get(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRawHttpClient::new(Some(format!("http://{address}")), 5, None).unwrap();
        let query = DeepXSymbolQuery {
            symbol: Some("ETH-USDC".to_string()),
        };
        let history_query = DeepXHistoryQuery {
            symbol: Some("ETH-USDC".to_string()),
            limit: Some(100),
            sort: Some(DeepXSortDirection::Desc),
            ..Default::default()
        };

        let snapshot = client
            .get_raw_account_snapshot("0xsub", &query, &history_query)
            .await
            .unwrap();

        assert_eq!(snapshot.balances, serde_json::json!({"rawBalances": [1]}));
        assert_eq!(snapshot.portfolio, serde_json::json!({"rawPortfolio": [2]}));
        assert_eq!(snapshot.positions, serde_json::json!({"rawPositions": [3]}));
        assert_eq!(
            snapshot.open_orders,
            serde_json::json!({"rawOpenOrders": [4]})
        );
        assert_eq!(
            snapshot.order_history,
            serde_json::json!({"rawOrderHistory": [5]})
        );
        assert_eq!(snapshot.trades, serde_json::json!({"rawTrades": [6]}));
    }

    #[tokio::test]
    async fn raw_account_snapshot_fails_when_any_endpoint_fails() {
        async fn handler(uri: Uri) -> Result<Json<serde_json::Value>, StatusCode> {
            if uri.path().ends_with("/portfolio") {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            Ok(Json(serde_json::json!({"raw": true})))
        }

        let router = Router::new().fallback(get(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRawHttpClient::new(Some(format!("http://{address}")), 5, None).unwrap();

        let result = client
            .get_raw_account_snapshot(
                "0xsub",
                &DeepXSymbolQuery::default(),
                &DeepXHistoryQuery::default(),
            )
            .await;

        assert!(matches!(
            result,
            Err(DeepXHttpError::Http { status: 503, .. })
        ));
    }

    #[tokio::test]
    #[ignore = "requires a DeepX testnet subaccount"]
    async fn captures_raw_testnet_account_snapshot_without_signing() {
        let subaccount = std::env::var(DEEPX_TESTNET_SUBACCOUNT_ADDRESS).unwrap_or_else(|_| {
            panic!("missing environment variable `{DEEPX_TESTNET_SUBACCOUNT_ADDRESS}`")
        });
        let client = DeepXRawHttpClient::new(None, 10, None).unwrap();
        let history_query = DeepXHistoryQuery {
            limit: Some(500),
            sort: Some(DeepXSortDirection::Desc),
            ..Default::default()
        };

        let snapshot = client
            .get_raw_account_snapshot(&subaccount, &DeepXSymbolQuery::default(), &history_query)
            .await
            .unwrap();

        println!(
            "{}",
            serde_json::to_string_pretty(&snapshot.redacted()).unwrap()
        );
    }

    #[tokio::test]
    async fn requests_typed_perp_order_book_with_sdk_query() {
        async fn handler(uri: Uri) -> Json<serde_json::Value> {
            let query = uri.query().unwrap();
            assert!(query.contains("limit=50"));
            assert!(query.contains("mergeLevel=2"));
            Json(serde_json::json!({
                "asks": [["2453.76", "3.282"]],
                "bids": [["2453.75", "0.842"]],
                "engineTime": 1787561622066_u64,
                "lastUpdateId": 62430652_u64,
                "serverTime": 1787561622211_u64
            }))
        }

        let router = Router::new().route("/v1/perp/markets/ETH-USDC/orderbook", get(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRawHttpClient::new(Some(format!("http://{address}")), 5, None).unwrap();
        let query = DeepXOrderBookQuery {
            limit: Some(50),
            merge_level: Some(2),
        };

        let response = client
            .get_perp_order_book("ETH-USDC", &query)
            .await
            .unwrap();

        assert_eq!(response.last_update_id, 62_430_652);
    }

    #[tokio::test]
    async fn defaults_long_short_ratio_history_interval_to_one_minute() {
        async fn handler(uri: Uri) -> Json<serde_json::Value> {
            assert_eq!(uri.query(), Some("interval=1m"));
            Json(serde_json::json!([{"longShortRatio": "1.25"}]))
        }

        let router = Router::new().route(
            "/v1/perp/markets/ETH-USDC/long-short-ratio/history",
            get(handler),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRawHttpClient::new(Some(format!("http://{address}")), 5, None).unwrap();

        let response = client
            .get_perp_long_short_ratio_history("ETH-USDC", None)
            .await
            .unwrap();

        assert_eq!(response[0]["longShortRatio"], "1.25");
    }

    #[tokio::test]
    async fn submits_signed_extrinsic_with_sdk_wire_name() {
        async fn handler(Json(body): Json<serde_json::Value>) -> Json<serde_json::Value> {
            assert_eq!(body, serde_json::json!({"signedExtrinsic": "0x1234"}));
            Json(serde_json::json!({
                "data": {
                    "orderId": 1781757000123_u64,
                    "txHash": "0x5a8f"
                }
            }))
        }

        let router = Router::new().route("/v1/chain/tx/placePerpOrder", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRawHttpClient::new(Some(format!("http://{address}")), 5, None).unwrap();

        let response = client.submit_place_perp_order("0x1234").await.unwrap();

        assert_eq!(response.order_id, 1_781_757_000_123_u64);
        assert_eq!(response.tx_hash, "0x5a8f");
    }
}
