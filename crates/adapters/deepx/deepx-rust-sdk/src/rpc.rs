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

//! Bounded reads from a DeepX JSON-RPC endpoint.

pub mod bridge;
pub mod chain_decode;
pub mod endpoint_identity;
pub mod event_decode;
pub mod event_storage;
pub mod events;
pub mod lending_positions;
pub mod leverage;
pub mod margin;
pub mod market_spec;
pub mod onboarding;
pub mod order_ticket;
pub mod perp_modify;
pub mod perp_place;
pub mod perp_position;
pub mod precompile;
pub mod quota_purchase;
pub mod spot_cancel;
pub mod spot_margin;
pub mod spot_market_spec;
pub mod spot_modify;
pub mod spot_place;
pub mod storage;
pub mod storage_decode;
pub mod submission;
pub mod system_account;
pub mod timestamp;
pub mod trade_event_decode;
pub mod trade_match;

use std::{collections::BTreeSet, fmt::Debug, net::IpAddr, time::Duration};

use anyhow::Context;
use reqwest::{Client, Proxy, redirect::Policy};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::json;

use crate::transport::{install_tls_provider, read_bounded_body};

/// JSON-RPC method for one signed extrinsic submission.
pub const SUBMIT_EXTRINSIC_METHOD: &str = "author_submitExtrinsic";

/// Runtime method for initial or maintenance margin aggregates.
pub const DEEPX_MARGIN_RUNTIME_API_METHOD: &str =
    "PerpRuntimeRpcApi_total_collateral_and_margin_required_for";
/// Runtime method for lending position shares.
pub const DEEPX_LENDING_POSITIONS_RUNTIME_API_METHOD: &str =
    "LendingRuntimeRpcApi_batch_position_for";
/// Runtime method for V2 lending pool accounting.
pub const DEEPX_LENDING_POOL_RUNTIME_API_METHOD: &str = "LendingRuntimeRpcApi_pool_accounting_v2";
/// Runtime method for effective lending deposits.
pub const DEEPX_LENDING_DEPOSIT_RUNTIME_API_METHOD: &str =
    "LendingRuntimeRpcApi_effective_deposit_v2";
/// Runtime method for effective lending borrows.
pub const DEEPX_LENDING_BORROW_RUNTIME_API_METHOD: &str =
    "LendingRuntimeRpcApi_effective_borrow_v2";

/// Margin weighting selected for the DeepX runtime calculation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXMarginDirection {
    /// Initial collateral weights and order/position requirements.
    Initial,
    /// Maintenance collateral weights, requirements, unrealized PnL, and unsettled funding.
    Maintenance,
}

impl DeepXMarginDirection {
    /// Returns the direction parameter encoded for the runtime API.
    #[must_use]
    pub const fn as_runtime_u8(self) -> u8 {
        match self {
            Self::Initial => 0,
            Self::Maintenance => 1,
        }
    }
}

/// Raw collateral and margin requirement returned by the DeepX runtime API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXRawMarginRequirement {
    /// Weighted collateral in runtime quote precision.
    pub total_collateral: u128,
    /// Required margin in runtime quote precision.
    pub total_margin_required: u128,
}

/// One JSON-RPC response, including an error envelope when the node rejects a call.
#[derive(Debug, Deserialize)]
pub struct DeepXRpcResponse<T>
where
    T: DeserializeOwned,
{
    pub jsonrpc: Option<String>,
    pub id: Option<u64>,
    #[serde(bound(deserialize = ""))]
    pub result: Option<T>,
    pub error: Option<DeepXRpcError>,
    pub code: Option<i32>,
    pub message: Option<String>,
}

/// Error returned in a JSON-RPC response.
#[derive(Debug, Deserialize)]
pub struct DeepXRpcError {
    pub code: i32,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

/// Block header returned by `chain_getHeader`.
#[derive(Debug, Deserialize)]
pub struct DeepXRpcBlockHeader {
    pub number: String,
}

/// Runtime version returned by `state_getRuntimeVersion`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXRpcRuntimeVersion {
    pub spec_version: u32,
    pub transaction_version: u32,
}

/// Block response returned by `chain_getBlock`.
#[derive(Debug, Deserialize)]
pub struct DeepXRpcBlockResponse {
    pub block: DeepXRpcBlock,
}

/// Header and extrinsics from one observed block.
#[derive(Debug, Deserialize)]
pub struct DeepXRpcBlock {
    pub header: DeepXRpcBlockHeader,
    pub extrinsics: Vec<String>,
}

/// Storage changes observed at one block hash.
#[derive(Debug, Deserialize)]
pub struct DeepXRpcStorageChangeSet {
    pub block: String,
    pub changes: Vec<(String, Option<String>)>,
}

/// HTTP transport for one DeepX JSON-RPC endpoint.
pub struct DeepXRpcClient {
    url: String,
    client: Client,
}

impl Debug for DeepXRpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXRpcClient))
            .field("url", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl DeepXRpcClient {
    /// Queries the current sequential account nonce once.
    ///
    /// The result is not block-pinned. Callers must reconcile concurrent transactions before
    /// signing or submitting a nonce-based pallet call.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn get_account_next_index(&self, account: [u8; 20]) -> anyhow::Result<u64> {
        self.execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "system_accountNextIndex",
            "params": [format!("0x{}", hex::encode(account))],
        }))
        .await
    }

    /// Queries the current best block hash once.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn get_best_block_hash_once(&self) -> anyhow::Result<String> {
        self.execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [],
        }))
        .await
    }

    /// Queries the current best block hash with bounded retries.
    ///
    /// # Errors
    ///
    /// Returns the last transport, decoding, or JSON-RPC error.
    pub async fn get_best_block_hash_with_retry(&self) -> anyhow::Result<String> {
        execute_rpc_read_with_retry(
            self,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "chain_getBlockHash",
                "params": [],
            }),
        )
        .await
    }

    /// Queries a block header once at an explicit hash.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn get_header_once<T: DeserializeOwned>(
        &self,
        block_hash: &str,
    ) -> anyhow::Result<T> {
        self.execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getHeader",
            "params": [block_hash],
        }))
        .await
    }

    /// Creates a client for one RPC endpoint.
    ///
    /// # Panics
    ///
    /// Panics if the platform cannot construct an HTTP client.
    #[must_use]
    pub fn new(url: String) -> Self {
        Self::new_with_proxy(url, None).expect("failed to construct DeepX RPC client")
    }

    /// Creates an RPC client with an optional explicit HTTP proxy.
    ///
    /// # Errors
    ///
    /// Returns an error when proxy configuration or client construction fails.
    pub fn new_with_proxy(url: String, proxy_url: Option<&str>) -> anyhow::Result<Self> {
        install_tls_provider();
        let mut builder = Client::builder().redirect(Policy::none());
        if let Some(proxy_url) = proxy_url {
            builder = builder.proxy(Proxy::all(proxy_url)?);
        }
        if reqwest::Url::parse(&url)
            .ok()
            .and_then(|parsed| parsed.host_str().map(str::to_owned))
            .is_some_and(|host| {
                host.eq_ignore_ascii_case("localhost")
                    || host
                        .parse::<IpAddr>()
                        .is_ok_and(|address| address.is_loopback())
            })
        {
            builder = builder.no_proxy();
        }
        Ok(Self {
            url,
            client: builder.build()?,
        })
    }

    /// Sends a JSON-RPC request and decodes its successful result.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn execute_rpc_call<T: DeserializeOwned>(
        &self,
        request: serde_json::Value,
    ) -> anyhow::Result<T> {
        let response: DeepXRpcResponse<T> = self.execute_rpc_response(request).await?;
        if response.jsonrpc.is_none()
            && let (Some(code), Some(message)) = (response.code, response.message)
        {
            anyhow::bail!("RPC provider error {code}: {message}");
        }
        if let Some(error) = response.error {
            return Err(rpc_error(error));
        }
        response
            .result
            .context("RPC response missing a non-null result")
    }

    /// Submits one encoded extrinsic without automatic retry.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error. Failure after sending may be ambiguous.
    pub async fn submit_encoded_extrinsic(&self, encoded: &str) -> anyhow::Result<String> {
        self.execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": SUBMIT_EXTRINSIC_METHOD,
            "params": [encoded],
        }))
        .await
    }

    /// Returns the extrinsics currently advertised in the submission pool.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn get_pending_extrinsics(&self) -> anyhow::Result<Vec<String>> {
        self.execute_rpc_call(pending_extrinsics_request()).await
    }

    /// Reads the submission pool with the bounded read retry policy.
    ///
    /// # Errors
    ///
    /// Returns the last transport, decoding, or JSON-RPC error.
    pub async fn get_pending_extrinsics_with_retry(&self) -> anyhow::Result<Vec<String>> {
        execute_rpc_read_with_retry(self, pending_extrinsics_request()).await
    }

    /// Queries storage changes at an explicit block hash.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn query_storage_at<T: DeserializeOwned>(
        &self,
        keys: &[String],
        block_hash: &str,
    ) -> anyhow::Result<T> {
        self.execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_queryStorageAt",
            "params": [keys, block_hash],
        }))
        .await
    }

    /// Queries a block at an explicit block hash.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn get_block<T: DeserializeOwned>(&self, block_hash: &str) -> anyhow::Result<T> {
        self.execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlock",
            "params": [block_hash],
        }))
        .await
    }

    /// Queries a non-null block with the bounded read retry policy.
    ///
    /// # Errors
    ///
    /// Returns the last transport, decoding, or JSON-RPC error.
    pub async fn get_block_with_retry<T: DeserializeOwned>(
        &self,
        block_hash: &str,
    ) -> anyhow::Result<T> {
        execute_rpc_read_with_retry(
            self,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "chain_getBlock",
                "params": [block_hash],
            }),
        )
        .await
    }

    /// Queries a block that may not yet be available at an explicit block hash.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn get_block_optional<T: DeserializeOwned>(
        &self,
        block_hash: &str,
    ) -> anyhow::Result<Option<T>> {
        self.execute_optional_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlock",
            "params": [block_hash],
        }))
        .await
    }

    /// Queries a nullable block with the bounded read retry policy.
    ///
    /// # Errors
    ///
    /// Returns the last transport, decoding, or JSON-RPC error.
    pub async fn get_block_optional_with_retry<T: DeserializeOwned>(
        &self,
        block_hash: &str,
    ) -> anyhow::Result<Option<T>> {
        execute_optional_rpc_read_with_retry(
            self,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "chain_getBlock",
                "params": [block_hash],
            }),
        )
        .await
    }

    /// Queries a block header that may not yet be available.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn get_header_optional<T: DeserializeOwned>(
        &self,
        block_hash: &str,
    ) -> anyhow::Result<Option<T>> {
        self.execute_optional_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getHeader",
            "params": [block_hash],
        }))
        .await
    }

    /// Queries a nullable block header with the bounded read retry policy.
    ///
    /// # Errors
    ///
    /// Returns the last transport, decoding, or JSON-RPC error.
    pub async fn get_header_optional_with_retry<T: DeserializeOwned>(
        &self,
        block_hash: &str,
    ) -> anyhow::Result<Option<T>> {
        execute_optional_rpc_read_with_retry(
            self,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "chain_getHeader",
                "params": [block_hash],
            }),
        )
        .await
    }

    /// Queries the latest finalized block hash once.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn get_finalized_head_once(&self) -> anyhow::Result<String> {
        self.execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getFinalizedHead",
            "params": [],
        }))
        .await
    }

    /// Queries one storage value at an explicit block hash.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn get_storage(&self, key: &str, block_hash: &str) -> anyhow::Result<Option<String>> {
        self.execute_optional_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [key, block_hash],
        }))
        .await
    }

    /// Decodes a non-null storage RPC result as the caller's wire type.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error, including a null result.
    pub async fn get_storage_result<T: DeserializeOwned>(
        &self,
        key: &str,
        block_hash: &str,
    ) -> anyhow::Result<T> {
        self.execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [key, block_hash],
        }))
        .await
    }

    /// Reads a non-null storage result with bounded retries.
    ///
    /// # Errors
    ///
    /// Returns the last transport, decoding, or JSON-RPC error.
    pub async fn get_storage_required_with_retry(
        &self,
        key: &str,
        block_hash: &str,
    ) -> anyhow::Result<String> {
        execute_rpc_read_with_retry(
            self,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "state_getStorage",
                "params": [key, block_hash],
            }),
        )
        .await
    }

    /// Returns the full storage response for block-selector rejection checks.
    ///
    /// # Errors
    ///
    /// Returns a transport or decoding error.
    pub async fn get_storage_response<T: DeserializeOwned>(
        &self,
        key: &str,
        block_hash: &str,
    ) -> anyhow::Result<DeepXRpcResponse<T>> {
        self.execute_rpc_response(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [key, block_hash],
        }))
        .await
    }

    /// Returns the full storage response with bounded retries.
    ///
    /// # Errors
    ///
    /// Returns the last transport or decoding error.
    pub async fn get_storage_response_with_retry<T: DeserializeOwned>(
        &self,
        key: &str,
        block_hash: &str,
    ) -> anyhow::Result<DeepXRpcResponse<T>> {
        execute_rpc_response_with_retry(
            self,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "state_getStorage",
                "params": [key, block_hash],
            }),
        )
        .await
    }

    /// Queries the canonical block hash at one height.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn get_block_hash(&self, block_number: u32) -> anyhow::Result<Option<String>> {
        self.get_block_hash_at_height(u64::from(block_number)).await
    }

    /// Queries the canonical block hash at a `u64` height.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn get_block_hash_at_height(
        &self,
        block_number: u64,
    ) -> anyhow::Result<Option<String>> {
        self.execute_optional_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [block_number],
        }))
        .await
    }

    /// Queries a non-null canonical block hash once.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error, including a null result.
    pub async fn get_block_hash_required_once(&self, block_number: u64) -> anyhow::Result<String> {
        self.execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [block_number],
        }))
        .await
    }

    /// Queries a nullable block hash with the bounded read retry policy.
    ///
    /// # Errors
    ///
    /// Returns the last transport, decoding, or JSON-RPC error.
    pub async fn get_block_hash_at_height_with_retry(
        &self,
        block_number: u64,
    ) -> anyhow::Result<Option<String>> {
        execute_optional_rpc_read_with_retry(
            self,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "chain_getBlockHash",
                "params": [block_number],
            }),
        )
        .await
    }

    /// Queries a non-null canonical block hash with bounded retries.
    ///
    /// # Errors
    ///
    /// Returns the last transport, decoding, or JSON-RPC error.
    pub async fn get_block_hash_required_with_retry(
        &self,
        block_number: u64,
    ) -> anyhow::Result<String> {
        execute_rpc_read_with_retry(
            self,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "chain_getBlockHash",
                "params": [block_number],
            }),
        )
        .await
    }

    /// Calls a runtime API at an explicit block hash.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn state_call(
        &self,
        method: &str,
        input: Vec<u8>,
        block_hash: &str,
    ) -> anyhow::Result<String> {
        self.execute_rpc_call(state_call_request(method, &input, block_hash))
            .await
    }

    /// Returns the complete response for a runtime API call, including a rejection envelope.
    ///
    /// # Errors
    ///
    /// Returns a transport or decoding error.
    pub async fn state_call_response<T: DeserializeOwned>(
        &self,
        method: &str,
        input: Vec<u8>,
        block_hash: &str,
    ) -> anyhow::Result<DeepXRpcResponse<T>> {
        self.execute_rpc_response(state_call_request(method, &input, block_hash))
            .await
    }

    /// Returns the full runtime-version response with bounded retries.
    ///
    /// # Errors
    ///
    /// Returns the last transport or decoding error.
    pub async fn get_runtime_version_response_with_retry<T: DeserializeOwned>(
        &self,
        block_hash: &str,
    ) -> anyhow::Result<DeepXRpcResponse<T>> {
        execute_rpc_response_with_retry(
            self,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "state_getRuntimeVersion",
                "params": [block_hash],
            }),
        )
        .await
    }

    /// Calls an EVM precompile at an explicit block selector.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error.
    pub async fn eth_call(&self, to: &str, data: &[u8], block: &str) -> anyhow::Result<String> {
        self.execute_rpc_call(eth_call_request(to, data, block))
            .await
    }

    /// Returns the complete EVM call response, including a rejection envelope.
    ///
    /// # Errors
    ///
    /// Returns a transport or decoding error.
    pub async fn eth_call_response<T: DeserializeOwned>(
        &self,
        to: &str,
        data: &[u8],
        block: &str,
    ) -> anyhow::Result<DeepXRpcResponse<T>> {
        self.execute_rpc_response(eth_call_request(to, data, block))
            .await
    }

    /// Sends a JSON-RPC request and returns its full response envelope.
    ///
    /// # Errors
    ///
    /// Returns a transport or decoding error.
    pub async fn execute_rpc_response<T: DeserializeOwned>(
        &self,
        request: serde_json::Value,
    ) -> anyhow::Result<DeepXRpcResponse<T>> {
        let body = self.send_rpc_bytes(request).await?;
        serde_json::from_slice(&body).context("failed to decode DeepX RPC response")
    }

    /// Decodes a JSON-RPC result that may legitimately be `null`.
    ///
    /// # Errors
    ///
    /// Returns a transport, decoding, or JSON-RPC error, including a missing result field.
    pub async fn execute_optional_rpc_call<T: DeserializeOwned>(
        &self,
        request: serde_json::Value,
    ) -> anyhow::Result<Option<T>> {
        let body = self.send_rpc_bytes(request).await?;
        let raw: serde_json::Value =
            serde_json::from_slice(&body).context("failed to decode DeepX RPC response")?;
        let has_result = raw
            .as_object()
            .is_some_and(|response| response.contains_key("result"));
        let response: DeepXRpcResponse<T> =
            serde_json::from_slice(&body).context("failed to decode DeepX RPC response")?;
        if response.jsonrpc.is_none()
            && let (Some(code), Some(message)) = (response.code, response.message)
        {
            anyhow::bail!("RPC provider error {code}: {message}");
        }
        if let Some(error) = response.error {
            return Err(rpc_error(error));
        }
        if !has_result {
            anyhow::bail!("RPC response missing result and error fields");
        }
        Ok(response.result)
    }

    async fn send_rpc_bytes(&self, request: serde_json::Value) -> anyhow::Result<Vec<u8>> {
        let response = self
            .client
            .post(&self.url)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .context("failed to send DeepX RPC request")?;
        if response.status().is_redirection() {
            anyhow::bail!("DeepX RPC redirect response rejected");
        }
        read_bounded_body(response).await
    }
}

fn state_call_request(method: &str, input: &[u8], block_hash: &str) -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "state_call",
        "params": [method, format!("0x{}", hex::encode(input)), block_hash],
    })
}

fn eth_call_request(to: &str, data: &[u8], block: &str) -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": to, "data": format!("0x{}", hex::encode(data))}, block],
    })
}

fn rpc_error(error: DeepXRpcError) -> anyhow::Error {
    match error.data {
        Some(data) => anyhow::anyhow!("RPC error {}: {} ({data})", error.code, error.message),
        None => anyhow::anyhow!("RPC error {}: {}", error.code, error.message),
    }
}

fn pending_extrinsics_request() -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "author_pendingExtrinsics",
        "params": [],
    })
}

#[derive(Debug, Deserialize)]
struct RpcMethods {
    methods: BTreeSet<String>,
}

/// Queries the genesis block hash reported by one RPC endpoint.
///
/// # Errors
///
/// Returns a transport, JSON-RPC, or decoding error after bounded retries.
pub async fn get_genesis_block_hash(url: &str) -> anyhow::Result<String> {
    let client = DeepXRpcClient::new(url.to_string());
    execute_rpc_read_with_retry(
        &client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [0],
        }),
    )
    .await
}

/// Queries the RPC methods advertised by one endpoint.
///
/// # Errors
///
/// Returns a transport, JSON-RPC, or decoding error after bounded retries.
pub async fn get_rpc_methods(url: &str) -> anyhow::Result<BTreeSet<String>> {
    let client = DeepXRpcClient::new(url.to_string());
    let response: RpcMethods = execute_rpc_read_with_retry(
        &client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "rpc_methods",
            "params": [],
        }),
    )
    .await?;
    Ok(response.methods)
}

/// Queries the latest finalized block hash.
///
/// # Errors
///
/// Returns a transport, JSON-RPC, or decoding error after bounded retries.
pub async fn get_finalized_head(client: &DeepXRpcClient) -> anyhow::Result<String> {
    execute_rpc_read_with_retry(
        client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getFinalizedHead",
            "params": [],
        }),
    )
    .await
}

/// Queries a block header at an explicit block hash.
///
/// # Errors
///
/// Returns a transport, JSON-RPC, or decoding error after bounded retries.
pub async fn get_header<T: DeserializeOwned>(
    client: &DeepXRpcClient,
    block_hash: &str,
) -> anyhow::Result<T> {
    execute_rpc_read_with_retry(
        client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getHeader",
            "params": [block_hash],
        }),
    )
    .await
}

/// Queries the runtime version at an explicit block hash.
///
/// # Errors
///
/// Returns a transport, JSON-RPC, or decoding error after bounded retries.
pub async fn get_runtime_version<T: DeserializeOwned>(
    client: &DeepXRpcClient,
    block_hash: &str,
) -> anyhow::Result<T> {
    execute_rpc_read_with_retry(
        client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getRuntimeVersion",
            "params": [block_hash],
        }),
    )
    .await
}

/// Queries SCALE metadata at an explicit block hash.
///
/// # Errors
///
/// Returns a transport, JSON-RPC, or decoding error after bounded retries.
pub async fn get_metadata(client: &DeepXRpcClient, block_hash: &str) -> anyhow::Result<String> {
    execute_rpc_read_with_retry(
        client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getMetadata",
            "params": [block_hash],
        }),
    )
    .await
}

/// Reads a decoded RPC result with the established bounded retry schedule.
///
/// # Errors
///
/// Returns the last transport, JSON-RPC, or decoding error.
pub async fn execute_rpc_read_with_retry<T: DeserializeOwned>(
    client: &DeepXRpcClient,
    request: serde_json::Value,
) -> anyhow::Result<T> {
    for delay_ms in [250, 500] {
        match client.execute_rpc_call(request.clone()).await {
            Ok(value) => return Ok(value),
            Err(_) => tokio::time::sleep(Duration::from_millis(delay_ms)).await,
        }
    }
    client.execute_rpc_call(request).await
}

/// Reads a complete RPC response with the established bounded retry schedule.
///
/// # Errors
///
/// Returns the last transport, JSON-RPC, or decoding error.
pub async fn execute_rpc_response_with_retry<T: DeserializeOwned>(
    client: &DeepXRpcClient,
    request: serde_json::Value,
) -> anyhow::Result<DeepXRpcResponse<T>> {
    for delay_ms in [250, 500] {
        match client.execute_rpc_response(request.clone()).await {
            Ok(value) => return Ok(value),
            Err(_) => tokio::time::sleep(Duration::from_millis(delay_ms)).await,
        }
    }
    client.execute_rpc_response(request).await
}

/// Retries a nullable RPC read until it returns a value or its bounded budget is exhausted.
///
/// # Errors
///
/// Returns the last transport, JSON-RPC, or decoding error.
pub async fn execute_optional_rpc_read_with_retry<T: DeserializeOwned>(
    client: &DeepXRpcClient,
    request: serde_json::Value,
) -> anyhow::Result<Option<T>> {
    for delay_ms in [250, 500] {
        match client.execute_optional_rpc_call(request.clone()).await {
            Ok(Some(value)) => return Ok(Some(value)),
            Ok(None) | Err(_) => tokio::time::sleep(Duration::from_millis(delay_ms)).await,
        }
    }
    client.execute_optional_rpc_call(request).await
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::{
        DeepXMarginDirection, DeepXRpcError, eth_call_request, rpc_error, state_call_request,
    };

    #[rstest]
    fn margin_direction_uses_runtime_parameters() {
        assert_eq!(DeepXMarginDirection::Initial.as_runtime_u8(), 0);
        assert_eq!(DeepXMarginDirection::Maintenance.as_runtime_u8(), 1);
    }

    #[rstest]
    fn state_call_encodes_exact_method_input_and_block() {
        assert_eq!(
            state_call_request("MarginApi_calculate", &[0x01, 0xab], "0x1234"),
            serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "state_call",
                "params": ["MarginApi_calculate", "0x01ab", "0x1234"],
            }),
        );
    }

    #[rstest]
    fn eth_call_encodes_exact_precompile_data_and_block() {
        assert_eq!(
            eth_call_request("0x1234", &[0x01, 0xab], "0xff"),
            serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "eth_call",
                "params": [{"to": "0x1234", "data": "0x01ab"}, "0xff"],
            }),
        );
    }

    #[rstest]
    fn rpc_error_preserves_provider_data() {
        let error: DeepXRpcError = serde_json::from_str(
            r#"{"code":1010,"message":"Invalid Transaction","data":"Unknown(IndeterminateImplicit)"}"#,
        )
        .unwrap();
        assert_eq!(
            rpc_error(error).to_string(),
            "RPC error 1010: Invalid Transaction (\"Unknown(IndeterminateImplicit)\")",
        );
    }
}
