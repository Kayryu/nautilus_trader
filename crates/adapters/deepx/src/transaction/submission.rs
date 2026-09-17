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

use std::{future::Future, num::NonZeroU32};

use nautilus_blockchain::rpc::http::BlockchainHttpRpcClient;
use nautilus_core::hex;
use serde_json::json;
use subxt_core::config::{Hasher, substrate::BlakeTwo256};
use thiserror::Error;

use super::{DeepXSubmissionFailure, DeepXSubmissionPermit};
use crate::signing::SignedPalletExtrinsic;

/// REST acknowledgement, not verified inclusion, finality, or business success.
#[derive(Debug)]
pub struct DeepXRestSubmissionAcknowledgement {
    /// Hash-verified acceptance of the exact permitted extrinsic.
    pub submitted: DeepXSubmittedExtrinsic,
    /// Backend action result retained without inventing confirmation semantics.
    pub data: serde_json::Value,
}

/// REST submission failures which never authorize automatic replay or nonce reuse.
#[derive(Debug, Error)]
pub enum DeepXRestSubmissionError {
    /// Local preparation could not produce a supported request, before HTTP transmission.
    #[error("DeepX REST submission was not sent: {0}")]
    NotSent(String),
    /// Delivery or execution cannot be established from the backend response.
    #[error("DeepX REST submission requires reconciliation: {0}")]
    ReconciliationRequired(String),
}

fn rest_action(operation: &super::DeepXTransactionOperation) -> (&'static str, &'static str) {
    use super::DeepXTransactionOperation;
    match operation {
        DeepXTransactionOperation::PerpPlace { .. } => ("Perp", "PlaceOrder"),
        DeepXTransactionOperation::PerpCancel { .. } => ("Perp", "CancelOrder"),
        DeepXTransactionOperation::PerpClose { .. } => ("Perp", "ClosePosition"),
        DeepXTransactionOperation::PerpProfitAndLossPoint { .. } => {
            ("Perp", "SetProfitAndLossPoint")
        }
        DeepXTransactionOperation::SpotPlace { .. } => ("Spot", "PlaceOrder"),
        DeepXTransactionOperation::SpotCancel { .. } => ("Spot", "CancelOrder"),
    }
}

fn verify_rest_acknowledgement(
    body: &[u8],
    bytes: &[u8],
    expected_hash: [u8; 32],
) -> Result<DeepXRestSubmissionAcknowledgement, DeepXRestSubmissionError> {
    use crate::http::models::DeepXApiResponse;
    let uncertain = |e: String| DeepXRestSubmissionError::ReconciliationRequired(e);
    let response: DeepXApiResponse<serde_json::Value> =
        serde_json::from_slice(body).map_err(|e| uncertain(e.to_string()))?;
    if response.fail || !response.code.is_success() {
        // Runtime failures may already be included and consume the timestamp nonce
        return Err(uncertain(format!(
            "backend code {}: {}",
            response.code, response.msg
        )));
    }
    let encoded = response
        .data
        .get("tx_hash")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| uncertain("missing transaction hash".to_string()))?;
    let node_hash = decode_node_hash(encoded).map_err(|e| uncertain(e.to_string()))?;
    verify_submission_hash(bytes, expected_hash, node_hash)
        .map_err(|e| uncertain(e.to_string()))?;
    Ok(DeepXRestSubmissionAcknowledgement {
        submitted: DeepXSubmittedExtrinsic {
            node_hash,
            extrinsic_hash: expected_hash,
        },
        data: response.data,
    })
}

/// Consumes durably prepared submission and sends exactly one REST request to the primary URL.
///
/// Routing comes from the verified durable operation, not caller-supplied action labels. There is
/// no read retry or endpoint failover. Pending acknowledgements require status polling, never
/// resubmission. Backend action data is not canonical-chain evidence and must not finalize records.
///
/// # Errors
///
/// Returns `NotSent` for local validation failures and `ReconciliationRequired` for every failure
/// after starting the HTTP request, including runtime reverts and invalid returned hashes.
pub async fn submit_rest_transaction_once(
    client: &crate::http::client::DeepXHttpClient,
    prepared: super::DeepXPreparedSubmission,
) -> Result<DeepXRestSubmissionAcknowledgement, DeepXRestSubmissionError> {
    let operation = prepared.record().identity().operation().ok_or_else(|| {
        DeepXRestSubmissionError::NotSent("missing durable operation".to_string())
    })?;
    let (market_type, action) = rest_action(operation);
    let (bytes, expected_hash) = prepared.into_permit().into_payload();
    verify_submission_hash(&bytes, expected_hash, expected_hash)
        .map_err(|e| DeepXRestSubmissionError::NotSent(e.to_string()))?;
    let body = serde_json::to_vec(&json!({
        "marketType": market_type,
        "action": action,
        "signedExtrinsic": format!("0x{}", hex::encode(&bytes)),
    }))
    .map_err(|e| DeepXRestSubmissionError::NotSent(e.to_string()))?;
    let response = client
        .post_transaction_once(body)
        .await
        .map_err(|e| DeepXRestSubmissionError::ReconciliationRequired(e.to_string()))?;
    if !response.status.is_success() {
        return Err(DeepXRestSubmissionError::ReconciliationRequired(format!(
            "HTTP {}",
            response.status.as_u16(),
        )));
    }
    verify_rest_acknowledgement(&response.body, &bytes, expected_hash)
}

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

/// Terminal result of a bounded initial-submission attempt sequence.
#[derive(Debug, Error)]
pub enum DeepXSubmissionRetryError {
    /// Local evidence proved that the terminal attempt never started transmission.
    #[error("DeepX submission was not sent after {attempts} attempt(s): {reason}")]
    NotSent {
        /// Number of attempts made, including the terminal attempt.
        attempts: u32,
        /// Local delivery evidence.
        reason: String,
    },
    /// The submission node authoritatively rejected the signed extrinsic.
    #[error("DeepX submission was rejected after {attempts} attempt(s): {reason}")]
    VenueRejected {
        /// Number of attempts made, including the terminal attempt.
        attempts: u32,
        /// Authoritative rejection evidence.
        reason: String,
    },
    /// Every permitted attempt ended without authoritative delivery evidence.
    #[error("DeepX submission remained ambiguous after {attempts} attempt(s): {reason}")]
    AmbiguousExhausted {
        /// Number of attempts made.
        attempts: u32,
        /// Most recent ambiguity evidence.
        reason: String,
    },
    /// Accepted response evidence did not identify the exact permitted payload.
    #[error(transparent)]
    Hash(#[from] DeepXSubmissionError),
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

/// Consumes one durable submission permit and retries only explicitly ambiguous outcomes.
///
/// Every attempt receives a fresh copy of the exact bytes and hash released by the same permit.
/// `NotSent` and `VenueRejected` stop immediately; only `Ambiguous` consumes the remaining bounded
/// attempt budget. A successful attempt is accepted only when its node hash matches the permitted
/// bytes. This coordinator does not classify transport or JSON-RPC errors and applies no lifecycle
/// mutation; callers must supply protocol-proven delivery evidence and commit the result.
///
/// # Errors
///
/// Returns a terminal delivery classification when submission cannot continue, or a hash error
/// when accepted response evidence does not match the exact permitted payload.
pub async fn submit_with_bounded_ambiguity_retry<F, Fut>(
    permit: DeepXSubmissionPermit,
    max_attempts: NonZeroU32,
    mut submit: F,
) -> Result<DeepXSubmittedExtrinsic, DeepXSubmissionRetryError>
where
    F: FnMut(Vec<u8>, [u8; 32]) -> Fut,
    Fut: Future<Output = Result<[u8; 32], DeepXSubmissionFailure>>,
{
    let (bytes, extrinsic_hash) = permit.into_payload();
    verify_submission_hash(&bytes, extrinsic_hash, extrinsic_hash)?;

    for attempt in 1..=max_attempts.get() {
        match submit(bytes.clone(), extrinsic_hash).await {
            Ok(node_hash) => {
                verify_submission_hash(&bytes, extrinsic_hash, node_hash)?;
                return Ok(DeepXSubmittedExtrinsic {
                    node_hash,
                    extrinsic_hash,
                });
            }
            Err(DeepXSubmissionFailure::NotSent(reason)) => {
                return Err(DeepXSubmissionRetryError::NotSent {
                    attempts: attempt,
                    reason,
                });
            }
            Err(DeepXSubmissionFailure::VenueRejected(reason)) => {
                return Err(DeepXSubmissionRetryError::VenueRejected {
                    attempts: attempt,
                    reason,
                });
            }
            Err(DeepXSubmissionFailure::Ambiguous(reason)) if attempt == max_attempts.get() => {
                return Err(DeepXSubmissionRetryError::AmbiguousExhausted {
                    attempts: attempt,
                    reason,
                });
            }
            Err(DeepXSubmissionFailure::Ambiguous(_)) => {}
        }
    }

    unreachable!("NonZeroU32 guarantees at least one submission attempt")
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
    use std::{
        collections::VecDeque,
        future::ready,
        sync::{Arc, Mutex},
    };

    use axum::{Json, Router, routing::post};
    use rstest::rstest;
    use serde_json::Value;
    use tokio::net::TcpListener;

    use super::*;
    use crate::signing::{DeepXRuntimeSnapshotService, SigningError, sign_dynamic_pallet_call};

    #[rstest]
    #[case(None)]
    #[case(Some("pending"))]
    #[case(Some("best"))]
    #[case(Some("decode_failed"))]
    fn rest_acknowledgement_preserves_backend_data(#[case] confirmation: Option<&str>) {
        let bytes = b"exact durable extrinsic";
        let hash = BlakeTwo256.hash(bytes).0;
        let data = json!({
            "tx_hash": format!("0x{}", hex::encode(hash)),
            "order_id": "18446744073709551615",
            "confirmation": confirmation,
        });
        let body = serde_json::to_vec(&json!({
            "code": 200, "fail": false, "msg": "success", "data": data,
        }))
        .unwrap();
        let acknowledgement = verify_rest_acknowledgement(&body, bytes, hash).unwrap();
        assert_eq!(acknowledgement.submitted.node_hash(), hash);
        assert_eq!(acknowledgement.data, data);
    }

    #[rstest]
    #[case(json!(null))]
    #[case(json!({}))]
    #[case(json!({"tx_hash": 42}))]
    #[case(json!({"tx_hash": "0x1234"}))]
    #[case(json!({"tx_hash": "0000000000000000000000000000000000000000000000000000000000000000"}))]
    #[case(json!({"tx_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"}))]
    fn rest_invalid_hash_requires_reconciliation(#[case] data: Value) {
        let bytes = b"durable";
        let body = serde_json::to_vec(&json!({
            "code": 200, "fail": false, "msg": "success", "data": data,
        }))
        .unwrap();
        assert!(matches!(
            verify_rest_acknowledgement(&body, bytes, BlakeTwo256.hash(bytes).0),
            Err(DeepXRestSubmissionError::ReconciliationRequired(_))
        ));
    }

    #[rstest]
    #[case(json!("19_0"), true)]
    #[case(json!(500), true)]
    #[case(json!(200), true)]
    #[case(json!(500), false)]
    fn rest_backend_failure_never_proves_not_sent(#[case] code: Value, #[case] fail: bool) {
        let body = serde_json::to_vec(&json!({
            "code": code, "fail": fail, "msg": "runtime failure", "data": null,
        }))
        .unwrap();
        assert!(matches!(
            verify_rest_acknowledgement(&body, b"durable", [0; 32]),
            Err(DeepXRestSubmissionError::ReconciliationRequired(_))
        ));
    }

    #[rstest]
    #[case(503)]
    #[case(307)]
    #[case(308)]
    #[tokio::test]
    async fn rest_post_uses_exact_body_once_without_read_failover(#[case] status: u16) {
        use axum::http::StatusCode;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let expected =
            json!({"marketType": "Perp", "action": "PlaceOrder", "signedExtrinsic": "0x0102"});
        let body = serde_json::to_vec(&expected).unwrap();
        let router = Router::new().route(
            "/internal/v1/chain/tx/transact",
            post(move |Json(value): Json<Value>| {
                let calls = observed.clone();
                let expected = expected.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(value, expected);
                    (
                        StatusCode::from_u16(status).unwrap(),
                        [("Location", "/internal/v1/chain/tx/transact")],
                    )
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = crate::http::client::DeepXHttpClient::new_with_endpoints(
            [url, "http://127.0.0.1:1".to_string()],
            Some(2),
            None,
            crate::http::retry::deepx_http_retry_config(),
        )
        .unwrap();
        let response = client.post_transaction_once(body).await.unwrap();
        assert_eq!(response.status.as_u16(), status);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        server.abort();
    }

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

    fn submission_permit(extrinsic: &SignedPalletExtrinsic) -> DeepXSubmissionPermit {
        DeepXSubmissionPermit {
            bytes: extrinsic.bytes().to_vec(),
            extrinsic_hash: extrinsic.extrinsic_hash(),
        }
    }

    #[tokio::test]
    async fn bounded_retry_reuses_exact_permitted_payload_after_ambiguity() {
        let extrinsic = signed_remark().unwrap();
        let expected_bytes = extrinsic.bytes().to_vec();
        let expected_hash = extrinsic.extrinsic_hash();
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&attempts);
        let mut outcomes = VecDeque::from([
            Err(DeepXSubmissionFailure::ambiguous("response lost")),
            Ok(expected_hash),
        ]);

        let submitted = submit_with_bounded_ambiguity_retry(
            submission_permit(&extrinsic),
            NonZeroU32::new(3).unwrap(),
            move |bytes, hash| {
                captured.lock().unwrap().push((bytes, hash));
                ready(outcomes.pop_front().unwrap())
            },
        )
        .await
        .unwrap();

        assert_eq!(submitted.extrinsic_hash(), expected_hash);
        assert_eq!(
            attempts.lock().unwrap().as_slice(),
            [
                (expected_bytes.clone(), expected_hash),
                (expected_bytes, expected_hash),
            ]
        );
    }

    #[tokio::test]
    async fn bounded_retry_stops_on_authoritative_rejection() {
        let extrinsic = signed_remark().unwrap();
        let mut outcomes = VecDeque::from([
            Err(DeepXSubmissionFailure::ambiguous("response lost")),
            Err(DeepXSubmissionFailure::venue_rejected(
                "invalid transaction",
            )),
            Ok(extrinsic.extrinsic_hash()),
        ]);

        let error = submit_with_bounded_ambiguity_retry(
            submission_permit(&extrinsic),
            NonZeroU32::new(3).unwrap(),
            move |_, _| ready(outcomes.pop_front().unwrap()),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXSubmissionRetryError::VenueRejected { attempts: 2, reason }
                if reason == "invalid transaction"
        ));
    }

    #[tokio::test]
    async fn bounded_retry_stops_when_transmission_provably_did_not_start() {
        let extrinsic = signed_remark().unwrap();
        let attempts = Arc::new(Mutex::new(0_u32));
        let captured = Arc::clone(&attempts);

        let error = submit_with_bounded_ambiguity_retry(
            submission_permit(&extrinsic),
            NonZeroU32::new(3).unwrap(),
            move |_, _| {
                *captured.lock().unwrap() += 1;
                ready(Err(DeepXSubmissionFailure::not_sent("send canceled")))
            },
        )
        .await
        .unwrap_err();

        assert_eq!(*attempts.lock().unwrap(), 1);
        assert!(matches!(
            error,
            DeepXSubmissionRetryError::NotSent { attempts: 1, reason }
                if reason == "send canceled"
        ));
    }

    #[tokio::test]
    async fn bounded_retry_preserves_ambiguity_when_budget_is_exhausted() {
        let extrinsic = signed_remark().unwrap();
        let attempts = Arc::new(Mutex::new(0_u32));
        let captured = Arc::clone(&attempts);

        let error = submit_with_bounded_ambiguity_retry(
            submission_permit(&extrinsic),
            NonZeroU32::new(2).unwrap(),
            move |_, _| {
                *captured.lock().unwrap() += 1;
                ready(Err(DeepXSubmissionFailure::ambiguous("still unknown")))
            },
        )
        .await
        .unwrap_err();

        assert_eq!(*attempts.lock().unwrap(), 2);
        assert!(matches!(
            error,
            DeepXSubmissionRetryError::AmbiguousExhausted { attempts: 2, reason }
                if reason == "still unknown"
        ));
    }

    #[tokio::test]
    async fn bounded_retry_rejects_mismatched_success_hash_without_retry() {
        let extrinsic = signed_remark().unwrap();
        let attempts = Arc::new(Mutex::new(0_u32));
        let captured = Arc::clone(&attempts);
        let wrong_hash = BlakeTwo256.hash(b"different accepted extrinsic").0;

        let error = submit_with_bounded_ambiguity_retry(
            submission_permit(&extrinsic),
            NonZeroU32::new(3).unwrap(),
            move |_, _| {
                *captured.lock().unwrap() += 1;
                ready(Ok(wrong_hash))
            },
        )
        .await
        .unwrap_err();

        assert_eq!(*attempts.lock().unwrap(), 1);
        assert!(matches!(
            error,
            DeepXSubmissionRetryError::Hash(DeepXSubmissionError::HashMismatch { .. })
        ));
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
