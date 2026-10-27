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
pub mod status;

mod account;
mod liquidation;
mod market;
mod quota;

use std::{collections::HashSet, time::Duration};

pub use liquidation::DeepXLiquidationExecutionRoute;
use reqwest::{Client, Proxy, StatusCode, redirect::Policy};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json, value::RawValue};
use thiserror::Error;

use crate::{
    pagination::validate_cursor_page,
    query::{
        DeepXBalanceChangesRequest, DeepXPerpAccountTradesRequest, DeepXPerpFundingFeeRequest,
        DeepXPerpHistoryOrdersRequest, DeepXPerpOpenOrdersRequest, DeepXPerpPositionOrdersRequest,
        DeepXPerpPositionsRequest, DeepXSpotAccountTradesRequest, DeepXSpotHistoryOrdersRequest,
        DeepXSpotOpenOrdersRequest, DeepXSpotOrderByIdRequest, DeepXWalletFundingFeeRequest,
    },
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
    ) -> Result<Vec<String>, DeepXRestError> {
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
        Ok(addresses)
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

fn is_account_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
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

    use super::*;

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
    fn raw_account_page_preserves_numeric_lexemes() {
        let page: DeepXRawAccountPage = serde_json::from_str(
            r#"{"items":[{"size":0.000000000000000001}],"nextCursor":null,"hasNext":false}"#,
        )
        .unwrap();
        assert_eq!(page.items[0].get(), r#"{"size":0.000000000000000001}"#);
        assert!(!page.has_next);
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
