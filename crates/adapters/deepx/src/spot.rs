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

//! Raw Spot market specification retrieval from the DeepX EVM precompile.

use std::sync::Arc;

use alloy::{
    primitives::{Address, FixedBytes},
    sol,
    sol_types::SolCall,
};
use nautilus_blockchain::{
    contracts::base::BaseContract,
    rpc::{error::BlockchainRpcClientError, http::BlockchainHttpRpcClient},
};
use nautilus_core::hex;
use thiserror::Error;

sol! {
    struct SpotMarketSpec {
        uint128 minOrderSize;
        uint128 tickSize;
        uint128 stepSize;
    }

    function getSpotMarketSpec(bytes32 pair) external view returns (SpotMarketSpec);
}

/// Raw integer Spot order constraints returned by the DeepX precompile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSpotMarketSpec {
    /// Raw minimum order size.
    pub min_order_size: u128,
    /// Raw price tick size.
    pub tick_size: u128,
    /// Raw quantity step size.
    pub step_size: u128,
}

/// Errors raised while retrieving a raw DeepX Spot market specification.
#[derive(Debug, Error)]
pub enum DeepXSpotMarketSpecError {
    /// The deployment pair identity is not exactly one bytes32 value.
    #[error("invalid DeepX Spot pair '{0}': expected 32-byte hexadecimal identity")]
    InvalidPair(String),
    /// The EVM RPC call or ABI response decoding failed.
    #[error(transparent)]
    Rpc(#[from] BlockchainRpcClientError),
}

/// Reads raw Spot order constraints from a configured DeepX precompile deployment.
#[derive(Debug)]
pub struct DeepXSpotMarketSpecClient {
    contract: BaseContract,
    precompile_address: Address,
}

impl DeepXSpotMarketSpecClient {
    /// Creates a reader for an explicit RPC client and precompile address.
    #[must_use]
    pub fn new(client: Arc<BlockchainHttpRpcClient>, precompile_address: Address) -> Self {
        Self {
            contract: BaseContract::new(client),
            precompile_address,
        }
    }

    /// Returns the unscaled integer constraints for a deployment-provided bytes32 pair ID.
    ///
    /// # Errors
    ///
    /// Returns an error when `pair` is not exactly 32 hexadecimal bytes, the RPC call fails, or
    /// the precompile response does not match the verified `(uint128,uint128,uint128)` ABI tuple.
    pub async fn get_market_spec(
        &self,
        pair: &str,
    ) -> Result<DeepXSpotMarketSpec, DeepXSpotMarketSpecError> {
        let pair_bytes = hex::decode_array::<32>(pair.strip_prefix("0x").unwrap_or(pair))
            .map_err(|_| DeepXSpotMarketSpecError::InvalidPair(pair.to_string()))?;
        let call_data = getSpotMarketSpecCall {
            pair: FixedBytes::from(pair_bytes),
        }
        .abi_encode();
        let response = self
            .contract
            .execute_call(&self.precompile_address, &call_data, None)
            .await?;
        let decoded = getSpotMarketSpecCall::abi_decode_returns(&response)
            .map_err(|error| BlockchainRpcClientError::AbiDecodingError(error.to_string()))?;

        Ok(DeepXSpotMarketSpec {
            min_order_size: decoded.minOrderSize,
            tick_size: decoded.tickSize,
            step_size: decoded.stepSize,
        })
    }
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, routing::post};
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;

    const PAIR: &str = "9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
    const PRECOMPILE: Address = Address::new([
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x4d,
    ]);

    #[tokio::test]
    async fn retrieves_raw_spot_market_spec() {
        let router = Router::new().route(
            "/",
            post(|Json(request): Json<Value>| async move {
                assert_eq!(request["method"], "eth_call");
                assert_eq!(request["params"][0]["to"], PRECOMPILE.to_string());
                assert_eq!(request["params"][1], "latest");
                let calldata = hex::decode(
                    request["params"][0]["data"]
                        .as_str()
                        .unwrap()
                        .trim_start_matches("0x"),
                )
                .unwrap();
                let signature_hash = alloy::primitives::keccak256("getSpotMarketSpec(bytes32)");
                assert_eq!(&calldata[..4], &signature_hash[..4]);
                assert_eq!(&calldata[4..], &hex::decode_array::<32>(PAIR).unwrap());

                Json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": format!("0x{:064x}{:064x}{:064x}", 11, 22, 33)
                }))
            }),
        );
        let rpc_url = spawn_server(router).await;
        let rpc = Arc::new(BlockchainHttpRpcClient::new(rpc_url, None, None));
        let client = DeepXSpotMarketSpecClient::new(rpc, PRECOMPILE);

        let spec = client.get_market_spec(&format!("0x{PAIR}")).await.unwrap();

        assert_eq!(
            spec,
            DeepXSpotMarketSpec {
                min_order_size: 11,
                tick_size: 22,
                step_size: 33,
            }
        );
    }

    #[tokio::test]
    async fn rejects_invalid_pair_before_rpc_call() {
        let rpc = Arc::new(BlockchainHttpRpcClient::new(
            "http://127.0.0.1:1".to_string(),
            None,
            None,
        ));
        let client = DeepXSpotMarketSpecClient::new(rpc, PRECOMPILE);

        let error = client.get_market_spec("ETH-USDC").await.unwrap_err();

        assert!(matches!(error, DeepXSpotMarketSpecError::InvalidPair(_)));
    }

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }
}
