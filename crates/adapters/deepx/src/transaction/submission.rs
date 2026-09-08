// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Hash-verified DeepX transaction submission boundary.

use nautilus_blockchain::rpc::http::BlockchainHttpRpcClient;
use nautilus_core::hex;
use serde_json::json;
use subxt_core::config::{Hasher, substrate::BlakeTwo256};
use thiserror::Error;

use crate::signing::SignedPalletExtrinsic;

/// JSON-RPC method used to submit a signed DeepX extrinsic exactly once.
const SUBMIT_EXTRINSIC_METHOD: &str = "author_submitExtrinsic";

/// Errors raised while submitting one signed DeepX extrinsic.
#[derive(Debug, Error)]
pub enum DeepXSubmissionError {
    /// The request or JSON-RPC response failed.
    #[error("failed to submit DeepX extrinsic via `{method}`: {source}")]
    Rpc {
        /// Method which failed.
        method: &'static str,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The submission response was not a single hex-encoded hash string.
    #[error("DeepX submission response was not a 0x-prefixed 32-byte hash: {value}")]
    InvalidResponse {
        /// Raw JSON value returned by the node.
        value: String,
    },
    /// The node-returned hash did not match the submitted extrinsic.
    #[error(
        "DeepX submission hash mismatch: node {}, extrinsic {}, recomputed {}",
        hash_to_hex(node_hash),
        hash_to_hex(extrinsic_hash),
        hash_to_hex(recomputed_hash)
    )]
    HashMismatch {
        /// Blake2-256 hash returned by the submission node.
        node_hash: [u8; 32],
        /// Blake2-256 hash recorded with the signed extrinsic.
        extrinsic_hash: [u8; 32],
        /// Blake2-256 hash recomputed from the submitted extrinsic bytes.
        recomputed_hash: [u8; 32],
    },
}

/// Renders a 32-byte hash as a fixed-width hex string for error messages.
fn hash_to_hex(hash: &[u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in hash {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Verifies that the submission node's hash matches the submitted extrinsic exactly.
///
/// The recomputed hash is derived from the exact submitted bytes, and both the recorded extrinsic
/// hash and the node-returned hash must equal it. This function performs no network I/O.
///
/// # Errors
///
/// Returns [`DeepXSubmissionError::HashMismatch`] when any pair of the three hashes differs.
pub fn verify_submission_hash(
    extrinsic_bytes: &[u8],
    expected_hash: [u8; 32],
    node_hash: [u8; 32],
) -> Result<(), DeepXSubmissionError> {
    let recomputed_hash = BlakeTwo256.hash(extrinsic_bytes).0;
    if recomputed_hash == expected_hash && recomputed_hash == node_hash {
        Ok(())
    } else {
        Err(DeepXSubmissionError::HashMismatch {
            node_hash,
            extrinsic_hash: expected_hash,
            recomputed_hash,
        })
    }
}

/// Evidence produced by one hash-verified extrinsic submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSubmittedExtrinsic {
    node_hash: [u8; 32],
    extrinsic_hash: [u8; 32],
}

impl DeepXSubmittedExtrinsic {
    /// Blake2-256 hash returned by the submission node for the accepted extrinsic.
    #[must_use]
    pub const fn node_hash(&self) -> [u8; 32] {
        self.node_hash
    }

    /// Blake2-256 hash recorded with the signed extrinsic before submission.
    #[must_use]
    pub const fn extrinsic_hash(&self) -> [u8; 32] {
        self.extrinsic_hash
    }
}

/// Submits one signed extrinsic and verifies the node's hash before returning evidence.
///
/// The call is attempted exactly once with no retry. A successful return proves only that the
/// submission node accepted the extrinsic into its pool and echoed its exact Blake2-256 hash; pool
/// acceptance is not inclusion, finality, or business success. The underlying client surfaces node
/// JSON-RPC error objects and missing results as [`DeepXSubmissionError::Rpc`] failures without
/// automatic ambiguity classification, and any hash inconsistency as
/// [`DeepXSubmissionError::HashMismatch`]. The caller owns lifecycle advancement.
///
/// # Errors
///
/// Returns an error when the RPC call fails, the response is not a single hex-encoded hash, or
/// the returned hash does not match the submitted extrinsic.
pub async fn submit_extrinsic_once(
    submission_url: &str,
    extrinsic: &SignedPalletExtrinsic,
) -> Result<DeepXSubmittedExtrinsic, DeepXSubmissionError> {
    let client = BlockchainHttpRpcClient::new(submission_url.to_string(), None, None);
    let encoded_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": SUBMIT_EXTRINSIC_METHOD,
            "params": [format!("0x{}", hex::encode(extrinsic.bytes()))],
        }))
        .await
        .map_err(|source| DeepXSubmissionError::Rpc {
            method: SUBMIT_EXTRINSIC_METHOD,
            source,
        })?;
    let node_hash = decode_node_hash(&encoded_hash)?;
    verify_submission_hash(extrinsic.bytes(), extrinsic.extrinsic_hash(), node_hash)?;

    Ok(DeepXSubmittedExtrinsic {
        node_hash,
        extrinsic_hash: extrinsic.extrinsic_hash(),
    })
}

fn decode_node_hash(encoded: &str) -> Result<[u8; 32], DeepXSubmissionError> {
    encoded
        .strip_prefix("0x")
        .ok_or_else(|| DeepXSubmissionError::InvalidResponse {
            value: encoded.to_string(),
        })
        .and_then(|value| {
            hex::decode_array::<32>(value).map_err(|_| DeepXSubmissionError::InvalidResponse {
                value: encoded.to_string(),
            })
        })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{Json, Router, routing::post};
    use rstest::rstest;
    use serde_json::Value;
    use tokio::net::TcpListener;

    use super::*;
    use crate::signing::{DeepXRuntimeSnapshotService, SigningError, sign_dynamic_pallet_call};

    fn signed_remark() -> Result<SignedPalletExtrinsic, SigningError> {
        let metadata: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json",
        )))
        .unwrap();
        let bytes = hex::decode(
            metadata["result"]
                .as_str()
                .unwrap()
                .trim_start_matches("0x"),
        )
        .unwrap();
        let snapshot = crate::signing::RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            hex::decode_array("86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b")
                .unwrap(),
            366,
            1,
            &bytes,
        )
        .unwrap();
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        sign_dynamic_pallet_call(
            &permit,
            &crate::common::DeepXPrivateKey::new(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                &crate::common::DeepXKeyScheme::Secp256k1,
            )
            .unwrap(),
            "System",
            "remark",
            vec![subxt_core::dynamic::Value::from_bytes(
                b"deepx-submission-hash-check",
            )],
            1_725_000_000_123,
        )
    }

    async fn spawn_submission_server(responses: Arc<Mutex<Vec<Value>>>) -> String {
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let responses = Arc::clone(&responses);
                async move {
                    assert_eq!(request["method"], "author_submitExtrinsic");
                    assert_eq!(
                        request["params"]
                            .as_array()
                            .and_then(|params| params.first())
                            .and_then(Value::as_str)
                            .and_then(|encoded| encoded.strip_prefix("0x"))
                            .map(|stripped| stripped.len() % 2),
                        Some(0)
                    );
                    let response = responses
                        .lock()
                        .unwrap()
                        .pop()
                        .expect("no scripted submission response");
                    Json(response)
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        url
    }

    fn ok_response(extrinsic: &SignedPalletExtrinsic) -> Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": format!("0x{}", hex::encode(extrinsic.extrinsic_hash())),
        })
    }

    #[tokio::test]
    async fn matching_node_hash_returns_verified_evidence() {
        let extrinsic = signed_remark().unwrap();
        let url =
            spawn_submission_server(Arc::new(Mutex::new(vec![ok_response(&extrinsic)]))).await;

        let submitted = submit_extrinsic_once(&url, &extrinsic).await.unwrap();

        assert_eq!(submitted.node_hash(), extrinsic.extrinsic_hash());
        assert_eq!(submitted.extrinsic_hash(), extrinsic.extrinsic_hash());
    }

    #[tokio::test]
    async fn different_node_hash_is_rejected() {
        let extrinsic = signed_remark().unwrap();
        let mut response = ok_response(&extrinsic);
        response["result"] = serde_json::json!(format!(
            "0x{}",
            hex::encode(BlakeTwo256.hash(b"different extrinsic").0)
        ));
        let url = spawn_submission_server(Arc::new(Mutex::new(vec![response]))).await;

        let error = submit_extrinsic_once(&url, &extrinsic).await.unwrap_err();

        assert!(matches!(error, DeepXSubmissionError::HashMismatch { .. }));
    }

    #[tokio::test]
    async fn tampered_extrinsic_hash_is_rejected_against_recomputed_hash() {
        let mut extrinsic = signed_remark().unwrap();
        let node_hash = extrinsic.extrinsic_hash();
        let real_hash = BlakeTwo256.hash(extrinsic.bytes()).0;
        let tampered_hash = BlakeTwo256.hash(b"tampered").0;
        extrinsic.extrinsic_hash = tampered_hash;
        let url = spawn_submission_server(Arc::new(Mutex::new(vec![serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": format!("0x{}", hex::encode(node_hash)),
        })])))
        .await;

        let error = submit_extrinsic_once(&url, &extrinsic).await.unwrap_err();

        assert!(matches!(
            error,
            DeepXSubmissionError::HashMismatch {
                extrinsic_hash: tampered,
                recomputed_hash: real,
                ..
            } if tampered == tampered_hash && real == real_hash
        ));
    }

    #[rstest]
    #[case(serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": "0x00"}))]
    #[case(serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": "0xyz"}))]
    #[case(serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": "deadbeef"}))]
    #[tokio::test]
    async fn malformed_submission_responses_are_rejected(#[case] response: Value) {
        let extrinsic = signed_remark().unwrap();
        let url = spawn_submission_server(Arc::new(Mutex::new(vec![response]))).await;

        let error = submit_extrinsic_once(&url, &extrinsic).await.unwrap_err();

        assert!(matches!(
            error,
            DeepXSubmissionError::InvalidResponse { .. }
        ));
    }

    #[rstest]
    #[case(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": { "code": 1010, "message": "Invalid Transaction" },
    }))]
    #[case(serde_json::json!({ "jsonrpc": "2.0", "id": 1 }))]
    #[case(serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": 42 }))]
    #[tokio::test]
    async fn node_reported_failures_surface_as_rpc_errors(#[case] response: Value) {
        let extrinsic = signed_remark().unwrap();
        let url = spawn_submission_server(Arc::new(Mutex::new(vec![response]))).await;

        let error = submit_extrinsic_once(&url, &extrinsic).await.unwrap_err();

        assert!(matches!(
            error,
            DeepXSubmissionError::Rpc {
                method: "author_submitExtrinsic",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn transport_failure_reports_submission_method() {
        let extrinsic = signed_remark().unwrap();

        let error = submit_extrinsic_once("http://127.0.0.1:1", &extrinsic)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXSubmissionError::Rpc {
                method: "author_submitExtrinsic",
                ..
            }
        ));
    }

    fn blake2_of(bytes: &[u8]) -> [u8; 32] {
        BlakeTwo256.hash(bytes).0
    }

    #[rstest]
    fn verify_submission_hash_accepts_exact_match() {
        let bytes = b"deepx-verify-exact";
        let hash = blake2_of(bytes);

        assert!(verify_submission_hash(bytes, hash, hash).is_ok());
    }

    #[rstest]
    #[case::wrong_expected(vec![0xde, 0xad])]
    #[case::wrong_node(b"bytes".to_vec())]
    fn verify_submission_hash_rejects_any_mismatch(#[case] bytes: Vec<u8>) {
        let true_hash = blake2_of(&bytes);
        let wrong_hash = BlakeTwo256.hash(b"wrong-hash").0;

        assert!(verify_submission_hash(&bytes, wrong_hash, true_hash).is_err());
        assert!(verify_submission_hash(&bytes, true_hash, wrong_hash).is_err());
        assert!(
            verify_submission_hash(&bytes, wrong_hash, wrong_hash).is_err()
                || wrong_hash == true_hash
        );
    }
}
