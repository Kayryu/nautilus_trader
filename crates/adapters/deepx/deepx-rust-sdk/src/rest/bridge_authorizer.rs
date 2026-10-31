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

//! Bridge authorizer request for the pinned EVM-to-EVM example.

use std::{net::IpAddr, time::Duration};

use alloy::primitives::{Address, B256, Bytes, FixedBytes, Signature, U256};
use anyhow::{Context, ensure};
use reqwest::{Client, Url};
use serde_json::{Value, json};

use crate::transport::{install_tls_provider, read_bounded_body};

/// Exact EIP-712 bridge terms sent to the off-chain authorizer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXBridgeAuthorizationTerms {
    /// Source EVM chain ID checked against its RPC endpoint.
    pub source_chain_id: u64,
    /// Source bridge contract used as the EIP-712 verifying contract.
    pub bridge_contract: Address,
    /// Destination EVM chain ID.
    pub destination_chain_id: u32,
    /// Registered bridge token ID.
    pub token_id: U256,
    /// Source amount in smallest token units.
    pub amount: U256,
    /// Source transaction sender.
    pub sender: Address,
    /// Destination recipient encoded as `bytes32`.
    pub destination_recipient: FixedBytes<32>,
    /// Source refund address.
    pub refund_address: Address,
    /// Fresh one-use bridge salt.
    pub salt: FixedBytes<32>,
    /// Optional relayer payload.
    pub custom_data: Bytes,
}

impl DeepXBridgeAuthorizationTerms {
    /// Returns the exact JSON body expected by the `bytes32` authorizer route.
    #[must_use]
    pub fn request_body(&self) -> Value {
        json!({
            "domain": {
                "name": "Channel",
                "version": "1",
                "chain_id": self.source_chain_id,
                "verifying_contract": self.bridge_contract.to_string(),
            },
            "params": {
                "dst_chain_id": self.destination_chain_id,
                "token_id": self.token_id.to_string(),
                "amount": self.amount.to_string(),
                "sender": self.sender.to_string(),
                "dst_recipient": format!("0x{}", hex::encode(self.destination_recipient)),
                "refund_address": self.refund_address.to_string(),
                "salt": format!("0x{}", hex::encode(self.salt)),
                "custom_data_hex": format!("0x{}", hex::encode(&self.custom_data)),
            },
        })
    }
}

/// Signature and optional digest returned by the bridge authorizer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXBridgeAuthorization {
    /// Contract-call signature bytes.
    pub signature: Bytes,
    /// EIP-712 digest reported by the authorizer, when present.
    pub digest: Option<B256>,
}

/// Requests an authorization for the exact bridge terms once.
///
/// When `expected_authorizer` is supplied, the response must include a digest and its signature
/// must recover that address. The contract independently verifies authorization on-chain.
///
/// # Errors
///
/// Returns an error for invalid URL, HTTP failure, malformed response, or signer mismatch.
pub async fn request_bridge_authorization(
    client: &Client,
    base_url: &str,
    terms: &DeepXBridgeAuthorizationTerms,
    expected_authorizer: Option<Address>,
) -> anyhow::Result<DeepXBridgeAuthorization> {
    let url = authorizer_url(base_url)?;
    install_tls_provider();
    let response = client
        .post(url)
        .timeout(Duration::from_secs(30))
        .json(&terms.request_body())
        .send()
        .await
        .context("bridge authorizer request failed")?;
    ensure!(
        response.status().is_success(),
        "bridge authorizer returned HTTP {}",
        response.status()
    );
    let body = read_bounded_body(response).await?;
    let payload: Value = serde_json::from_slice(&body).context("invalid bridge authorizer JSON")?;
    ensure!(
        payload.get("matches_authorizer") != Some(&Value::Bool(false)),
        "bridge authorizer rejected the signature"
    );
    let signature_hex = payload["signature"]
        .as_str()
        .and_then(|value| value.strip_prefix("0x"))
        .context("bridge authorizer response is missing a hex signature")?;
    let signature = hex::decode(signature_hex).context("invalid bridge signature hex")?;
    let parsed =
        Signature::try_from(signature.as_slice()).context("invalid bridge signature bytes")?;
    let digest = payload["digest"]
        .as_str()
        .map(str::parse::<B256>)
        .transpose()
        .context("invalid bridge authorization digest")?;
    if let Some(expected) = expected_authorizer {
        let digest = digest.context("bridge authorizer response has no digest")?;
        let recovered = parsed
            .recover_address_from_prehash(&digest)
            .context("unable to recover bridge authorizer")?;
        ensure!(recovered == expected, "bridge authorizer signer mismatch");
    }
    Ok(DeepXBridgeAuthorization {
        signature: Bytes::from(signature),
        digest,
    })
}

fn authorizer_url(base_url: &str) -> anyhow::Result<Url> {
    let mut url = Url::parse(base_url).context("invalid bridge authorizer URL")?;
    let loopback = url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    ensure!(
        (url.scheme() == "https" || (url.scheme() == "http" && loopback))
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "bridge authorizer URL requires HTTPS, a host, and no credentials, query, or fragment"
    );
    let path = format!(
        "{}/internal/v1/sign-bridge/sign-bridge-out/bytes32",
        url.path().trim_end_matches('/')
    );
    url.set_path(&path);
    Ok(url)
}

#[cfg(test)]
mod tests {
    use alloy::{
        primitives::keccak256,
        signers::{SignerSync, local::PrivateKeySigner},
    };
    use axum::{Json, Router, routing::post};
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn authorizer_body_matches_exact_bytes32_route() {
        let terms = DeepXBridgeAuthorizationTerms {
            source_chain_id: 4856,
            bridge_contract: Address::from([0x11; 20]),
            destination_chain_id: 11_155_111,
            token_id: U256::from(3),
            amount: U256::from(1_000_000),
            sender: Address::from([0x22; 20]),
            destination_recipient: FixedBytes::from([0x33; 32]),
            refund_address: Address::from([0x22; 20]),
            salt: FixedBytes::from([0x44; 32]),
            custom_data: Bytes::new(),
        };
        let body = terms.request_body();
        assert_eq!(body["domain"]["chain_id"], 4856);
        assert_eq!(body["params"]["token_id"], "3");
        assert_eq!(body["params"]["amount"], "1000000");
        assert_eq!(body["params"]["custom_data_hex"], "0x");
        assert_eq!(
            authorizer_url("https://api.example.com/").unwrap().as_str(),
            "https://api.example.com/internal/v1/sign-bridge/sign-bridge-out/bytes32"
        );
        assert!(authorizer_url("http://api.example.com").is_err());
        assert!(authorizer_url("https://user@api.example.com").is_err());
    }

    #[tokio::test]
    async fn authorizer_response_recovers_expected_signer() {
        let signer = PrivateKeySigner::from_slice(&[0x11; 32]).unwrap();
        let digest = keccak256(b"bridge terms");
        let signature = signer.sign_hash_sync(&digest).unwrap();
        let app = Router::new().route(
            "/internal/v1/sign-bridge/sign-bridge-out/bytes32",
            post(move |Json(body): Json<Value>| async move {
                assert_eq!(body["domain"]["chain_id"], 4856);
                assert_eq!(body["params"]["token_id"], "3");
                Json(json!({
                    "signature": format!("0x{}", hex::encode(signature.as_bytes())),
                    "digest": digest,
                    "matches_authorizer": true,
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let terms = DeepXBridgeAuthorizationTerms {
            source_chain_id: 4856,
            bridge_contract: Address::from([0x22; 20]),
            destination_chain_id: 11_155_111,
            token_id: U256::from(3),
            amount: U256::from(1_000_000),
            sender: signer.address(),
            destination_recipient: FixedBytes::from([0x33; 32]),
            refund_address: signer.address(),
            salt: FixedBytes::from([0x44; 32]),
            custom_data: Bytes::new(),
        };
        let base = format!("http://{address}");
        let authorized =
            request_bridge_authorization(&Client::new(), &base, &terms, Some(signer.address()))
                .await
                .unwrap();
        assert_eq!(authorized.digest, Some(digest));
        assert_eq!(authorized.signature.as_ref(), signature.as_bytes());
        assert!(
            request_bridge_authorization(
                &Client::new(),
                &base,
                &terms,
                Some(Address::from([0x55; 20])),
            )
            .await
            .is_err()
        );
        server.abort();
    }
}
