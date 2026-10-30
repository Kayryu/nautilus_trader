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
//  See the GNU Lesser General Public License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Spot market specification precompile ABI.

use alloy::{
    primitives::{Address, FixedBytes},
    sol,
    sol_types::SolCall,
};
use thiserror::Error;

use super::DeepXRpcClient;
use crate::symbol::decode_spot_pair_identity;

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

/// Errors returned by a Spot market-specification read.
#[derive(Debug, Error)]
pub enum DeepXSpotMarketSpecError {
    /// The deployment pair identity is not exactly one bytes32 value.
    #[error("invalid DeepX Spot pair '{0}': expected 32-byte hexadecimal identity")]
    InvalidPair(String),
    /// The RPC endpoint failed to return a successful response.
    #[error("DeepX Spot market-spec RPC failed: {0}")]
    Rpc(#[source] anyhow::Error),
    /// The RPC result was not a hexadecimal byte string.
    #[error("DeepX Spot market-spec RPC returned invalid hexadecimal data")]
    InvalidHex,
    /// The precompile response did not match the expected ABI tuple.
    #[error("DeepX Spot market-spec precompile response is invalid: {0}")]
    Decode(#[source] anyhow::Error),
}

/// Reads raw Spot order constraints from an explicit DeepX precompile deployment.
#[derive(Debug)]
pub struct DeepXSpotMarketSpecClient {
    rpc: DeepXRpcClient,
    precompile_address: Address,
}

impl DeepXSpotMarketSpecClient {
    /// Creates a reader with an SDK RPC client and an explicit precompile address.
    #[must_use]
    pub const fn new(rpc: DeepXRpcClient, precompile_address: Address) -> Self {
        Self {
            rpc,
            precompile_address,
        }
    }

    /// Returns unscaled integer constraints for a deployment-provided pair ID.
    ///
    /// # Errors
    ///
    /// Returns a pair, RPC, hexadecimal, or ABI decoding error.
    pub async fn get_market_spec(
        &self,
        pair: &str,
    ) -> Result<DeepXSpotMarketSpec, DeepXSpotMarketSpecError> {
        self.rpc
            .get_spot_market_spec(self.precompile_address, pair)
            .await
    }
}

/// Parses a deployment-provided Spot pair into its exact bytes32 identity.
///
/// # Errors
///
/// Returns an error when the pair is not exactly 32 hexadecimal bytes.
pub fn parse_spot_pair(pair: &str) -> Result<[u8; 32], DeepXSpotMarketSpecError> {
    decode_spot_pair_identity(pair)
        .ok_or_else(|| DeepXSpotMarketSpecError::InvalidPair(pair.to_string()))
}

impl DeepXRpcClient {
    /// Reads raw Spot order constraints from an explicit precompile at the current block.
    ///
    /// # Errors
    ///
    /// Returns a pair, RPC, hexadecimal, or ABI decoding error.
    pub async fn get_spot_market_spec(
        &self,
        precompile_address: Address,
        pair: &str,
    ) -> Result<DeepXSpotMarketSpec, DeepXSpotMarketSpecError> {
        let pair = parse_spot_pair(pair)?;
        let calldata = encode_spot_market_spec_call(pair);
        let response = self
            .eth_call(&precompile_address.to_string(), &calldata, "latest")
            .await
            .map_err(DeepXSpotMarketSpecError::Rpc)?;
        let response = response
            .strip_prefix("0x")
            .and_then(|value| hex::decode(value).ok())
            .ok_or(DeepXSpotMarketSpecError::InvalidHex)?;
        decode_spot_market_spec_return(&response).map_err(DeepXSpotMarketSpecError::Decode)
    }
}

/// Encodes a Spot market specification query for one deployment pair.
#[must_use]
pub fn encode_spot_market_spec_call(pair: [u8; 32]) -> Vec<u8> {
    getSpotMarketSpecCall {
        pair: FixedBytes::from(pair),
    }
    .abi_encode()
}

/// Decodes the raw Spot market specification returned by the precompile.
///
/// # Errors
///
/// Returns an error if the response does not match the verified ABI tuple.
pub fn decode_spot_market_spec_return(response: &[u8]) -> anyhow::Result<DeepXSpotMarketSpec> {
    let decoded = getSpotMarketSpecCall::abi_decode_returns(response)?;
    Ok(DeepXSpotMarketSpec {
        min_order_size: decoded.minOrderSize,
        tick_size: decoded.tickSize,
        step_size: decoded.stepSize,
    })
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, routing::post};
    use rstest::rstest;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;

    #[rstest]
    fn spot_market_spec_abi_preserves_integer_fields() {
        let pair = [0x42; 32];
        let call = encode_spot_market_spec_call(pair);
        let selector = alloy::primitives::keccak256("getSpotMarketSpec(bytes32)");
        assert_eq!(&call[..4], &selector[..4]);
        assert_eq!(&call[4..], &pair);

        let mut response = [0_u8; 96];
        response[31] = 11;
        response[63] = 22;
        response[95] = 33;
        assert_eq!(
            decode_spot_market_spec_return(&response).unwrap(),
            DeepXSpotMarketSpec {
                min_order_size: 11,
                tick_size: 22,
                step_size: 33,
            }
        );
        assert!(decode_spot_market_spec_return(&response[..95]).is_err());
    }

    #[tokio::test]
    async fn sdk_reads_typed_spot_market_spec_from_precompile() {
        const PAIR: &str = "9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
        const PRECOMPILE: Address = Address::new([
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x4d,
        ]);
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
                        .strip_prefix("0x")
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(
                    calldata,
                    encode_spot_market_spec_call(parse_spot_pair(PAIR).unwrap())
                );

                Json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": format!("0x{:064x}{:064x}{:064x}", 11, 22, 33)
                }))
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXSpotMarketSpecClient::new(DeepXRpcClient::new(rpc_url), PRECOMPILE);

        assert_eq!(
            client.get_market_spec(&format!("0x{PAIR}")).await.unwrap(),
            DeepXSpotMarketSpec {
                min_order_size: 11,
                tick_size: 22,
                step_size: 33,
            }
        );
    }

    #[tokio::test]
    async fn invalid_pair_is_rejected_before_rpc() {
        let client = DeepXSpotMarketSpecClient::new(
            DeepXRpcClient::new("http://127.0.0.1:1".to_string()),
            Address::ZERO,
        );

        assert!(matches!(
            client.get_market_spec("ETH-USDC").await,
            Err(DeepXSpotMarketSpecError::InvalidPair(_)),
        ));
    }

    #[tokio::test]
    async fn malformed_rpc_hex_is_rejected() {
        let router = Router::new().route(
            "/",
            post(|| async {
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": "0xzz"
                }))
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRpcClient::new(rpc_url);

        assert!(matches!(
            client
                .get_spot_market_spec(Address::ZERO, &format!("0x{}", hex::encode([0x11; 32])))
                .await,
            Err(DeepXSpotMarketSpecError::InvalidHex),
        ));
    }
}
