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

//! Exact decoding of the DeepX margin runtime API response.

use std::collections::BTreeMap;

use parity_scale_codec::{Decode, Encode};
use thiserror::Error;

use super::{
    DEEPX_MARGIN_RUNTIME_API_METHOD, DeepXMarginDirection, DeepXRawMarginRequirement,
    DeepXRpcClient,
};

/// Errors while reading both margin directions at one block hash.
#[derive(Debug, Error)]
pub enum DeepXMarginRequirementsReadError {
    /// One direction-specific runtime API request failed.
    #[error("DeepX {direction:?} margin RPC failed: {source}")]
    Rpc {
        /// Margin direction whose request failed.
        direction: DeepXMarginDirection,
        /// Underlying RPC error.
        #[source]
        source: anyhow::Error,
    },
    /// A direction-specific runtime response was malformed or out of scope.
    #[error("DeepX {0:?} margin response is invalid")]
    InvalidEvidence(DeepXMarginDirection),
}

/// Reads initial and maintenance margin requirements at one block hash.
///
/// # Errors
///
/// Returns an error for a direction-specific RPC failure or invalid SCALE response.
pub async fn get_margin_requirements_at_block(
    client: &DeepXRpcClient,
    block_hash: [u8; 32],
    subaccount: [u8; 20],
) -> Result<(DeepXRawMarginRequirement, DeepXRawMarginRequirement), DeepXMarginRequirementsReadError>
{
    let block_hash = format!("0x{}", hex::encode(block_hash));
    let observe = |direction: DeepXMarginDirection| {
        let input = encode_margin_requirement_request(subaccount, direction);
        client.state_call(DEEPX_MARGIN_RUNTIME_API_METHOD, input, &block_hash)
    };
    let (initial, maintenance) = tokio::join!(
        observe(DeepXMarginDirection::Initial),
        observe(DeepXMarginDirection::Maintenance),
    );
    let initial = initial.map_err(|source| DeepXMarginRequirementsReadError::Rpc {
        direction: DeepXMarginDirection::Initial,
        source,
    })?;
    let maintenance = maintenance.map_err(|source| DeepXMarginRequirementsReadError::Rpc {
        direction: DeepXMarginDirection::Maintenance,
        source,
    })?;
    Ok((
        decode_margin_requirement(&initial, subaccount).map_err(|_| {
            DeepXMarginRequirementsReadError::InvalidEvidence(DeepXMarginDirection::Initial)
        })?,
        decode_margin_requirement(&maintenance, subaccount).map_err(|_| {
            DeepXMarginRequirementsReadError::InvalidEvidence(DeepXMarginDirection::Maintenance)
        })?,
    ))
}

/// Encodes one account and margin direction for the runtime API.
#[must_use]
pub fn encode_margin_requirement_request(
    subaccount: [u8; 20],
    direction: DeepXMarginDirection,
) -> Vec<u8> {
    (vec![subaccount], direction.as_runtime_u8()).encode()
}

/// A margin response with invalid encoding, account identity, or trailing data.
#[derive(Debug, Error)]
#[error("invalid DeepX margin requirement evidence")]
pub struct DeepXMarginDecodeError;

/// Decodes a complete one-account margin runtime API response.
///
/// # Errors
///
/// Returns an error unless the response is a prefixed SCALE map with exactly the requested
/// subaccount and no trailing bytes.
pub fn decode_margin_requirement(
    encoded: &str,
    subaccount: [u8; 20],
) -> Result<DeepXRawMarginRequirement, DeepXMarginDecodeError> {
    let bytes = encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())
        .ok_or(DeepXMarginDecodeError)?;
    let mut input = bytes.as_slice();
    let values = BTreeMap::<[u8; 20], (u128, u128)>::decode(&mut input)
        .map_err(|_| DeepXMarginDecodeError)?;
    if !input.is_empty() || values.len() != 1 {
        return Err(DeepXMarginDecodeError);
    }
    let (total_collateral, total_margin_required) = values
        .get(&subaccount)
        .copied()
        .ok_or(DeepXMarginDecodeError)?;
    Ok(DeepXRawMarginRequirement {
        total_collateral,
        total_margin_required,
    })
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, routing::post};
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;

    const SUBACCOUNT: [u8; 20] = [0x11; 20];

    #[rstest]
    #[case::valid(false)]
    #[case::invalid_maintenance(true)]
    #[tokio::test]
    async fn reads_both_directions_at_the_same_block(#[case] invalid_maintenance: bool) {
        let block_hash = [0x22; 32];
        let app = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| async move {
                assert_eq!(request["method"], "state_call");
                assert_eq!(request["params"][0], DEEPX_MARGIN_RUNTIME_API_METHOD);
                assert_eq!(
                    request["params"][2],
                    format!("0x{}", hex::encode(block_hash))
                );
                let initial = format!(
                    "0x{}",
                    hex::encode(encode_margin_requirement_request(
                        SUBACCOUNT,
                        DeepXMarginDirection::Initial,
                    ))
                );
                let maintenance = format!(
                    "0x{}",
                    hex::encode(encode_margin_requirement_request(
                        SUBACCOUNT,
                        DeepXMarginDirection::Maintenance,
                    ))
                );
                let result = if request["params"][1] == initial {
                    format!(
                        "0x{}",
                        hex::encode(BTreeMap::from([(SUBACCOUNT, (100_u128, 10_u128))]).encode())
                    )
                } else {
                    assert_eq!(request["params"][1], maintenance);
                    if invalid_maintenance {
                        "0xzz".to_string()
                    } else {
                        format!(
                            "0x{}",
                            hex::encode(
                                BTreeMap::from([(SUBACCOUNT, (100_u128, 20_u128))]).encode()
                            )
                        )
                    }
                };
                Json(json!({"jsonrpc": "2.0", "id": 1, "result": result}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let result =
            get_margin_requirements_at_block(&DeepXRpcClient::new(url), block_hash, SUBACCOUNT)
                .await;
        if invalid_maintenance {
            assert!(matches!(
                result,
                Err(DeepXMarginRequirementsReadError::InvalidEvidence(
                    DeepXMarginDirection::Maintenance
                ))
            ));
        } else {
            let (initial, maintenance) = result.unwrap();
            assert_eq!(initial.total_margin_required, 10);
            assert_eq!(maintenance.total_margin_required, 20);
        }
        server.abort();
    }

    #[rstest]
    #[case(DeepXMarginDirection::Initial, 0)]
    #[case(DeepXMarginDirection::Maintenance, 1)]
    fn encodes_margin_runtime_request(#[case] direction: DeepXMarginDirection, #[case] code: u8) {
        let mut expected = vec![4];
        expected.extend_from_slice(&SUBACCOUNT);
        expected.push(code);
        assert_eq!(
            encode_margin_requirement_request(SUBACCOUNT, direction),
            expected
        );
    }

    #[rstest]
    fn decodes_exact_one_account_response() {
        let encoded = BTreeMap::from([(SUBACCOUNT, (u128::MAX, 42_u128))]).encode();
        let result =
            decode_margin_requirement(&format!("0x{}", hex::encode(encoded)), SUBACCOUNT).unwrap();
        assert_eq!(result.total_collateral, u128::MAX);
        assert_eq!(result.total_margin_required, 42);
    }

    #[rstest]
    #[case(BTreeMap::<[u8; 20], (u128, u128)>::new().encode())]
    #[case(BTreeMap::from([([0x22; 20], (1_u128, 2_u128))]).encode())]
    #[case({
        let mut encoded = BTreeMap::from([(SUBACCOUNT, (1_u128, 2_u128))]).encode();
        encoded.push(0);
        encoded
    })]
    #[case(BTreeMap::from([(SUBACCOUNT, (1_u128, 2_u128)), ([0x22; 20], (3, 4))]).encode())]
    fn rejects_missing_foreign_trailing_or_multiple_accounts(#[case] encoded: Vec<u8>) {
        assert!(
            decode_margin_requirement(&format!("0x{}", hex::encode(encoded)), SUBACCOUNT).is_err()
        );
    }

    #[rstest]
    #[case("deadbeef")]
    #[case("0xyz")]
    #[case("0x")]
    fn rejects_invalid_hex(#[case] encoded: &str) {
        assert!(decode_margin_requirement(encoded, SUBACCOUNT).is_err());
    }
}
