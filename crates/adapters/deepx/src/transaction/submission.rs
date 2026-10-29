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

pub use deepx_rust_sdk::rpc::submission::{
    DeepXRestSubmissionAcknowledgement, DeepXSubmissionError, DeepXSubmittedExtrinsic,
    verify_submission_hash,
};
use deepx_rust_sdk::{
    rest::{DeepXRestAction, encode_transaction_request},
    rpc::{
        DeepXRpcClient, submission::verify_rest_acknowledgement as sdk_verify_rest_acknowledgement,
    },
};
use thiserror::Error;

use super::{DeepXSubmissionFailure, DeepXSubmissionPermit};
use crate::signing::{DeepXRuntimeSnapshotPermit, SignedPalletExtrinsic};

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

fn rest_action(operation: &super::DeepXTransactionOperation) -> Option<DeepXRestAction> {
    use super::DeepXTransactionOperation;
    Some(match operation {
        DeepXTransactionOperation::PerpPlace { .. } => DeepXRestAction::PerpPlace,
        DeepXTransactionOperation::PerpCancel { .. } => DeepXRestAction::PerpCancel,
        DeepXTransactionOperation::PerpModify { .. } => return None,
        DeepXTransactionOperation::PerpClose { .. } => DeepXRestAction::PerpClose,
        DeepXTransactionOperation::PerpProfitAndLossPoint { .. } => {
            DeepXRestAction::PerpProfitAndLossPoint
        }
        DeepXTransactionOperation::SpotPlace { .. } => DeepXRestAction::SpotPlace,
        DeepXTransactionOperation::SpotCancel { .. } => DeepXRestAction::SpotCancel,
    })
}

fn verify_rest_acknowledgement(
    body: &[u8],
    bytes: &[u8],
    expected_hash: [u8; 32],
) -> Result<DeepXRestSubmissionAcknowledgement, DeepXRestSubmissionError> {
    sdk_verify_rest_acknowledgement(body, bytes, expected_hash)
        .map_err(|e| DeepXRestSubmissionError::ReconciliationRequired(e.to_string()))
}

/// Consumes durably prepared submission and sends exactly one REST request to the primary URL.
///
/// Routing comes from the verified durable operation, not caller-supplied action labels. There is
/// no read retry or endpoint failover. Pending acknowledgements require status polling, never
/// resubmission. Backend action data is not canonical-chain evidence and must not finalize records.
///
/// # Errors
///
/// Returns `NotSent` for local validation failures, including a mismatched or revoked runtime
/// permit, and `ReconciliationRequired` for every failure after starting the HTTP request,
/// including runtime reverts and invalid returned hashes.
pub async fn submit_rest_transaction_once(
    client: &crate::http::client::DeepXHttpClient,
    prepared: super::DeepXPreparedSubmission,
    runtime_permit: &DeepXRuntimeSnapshotPermit,
) -> Result<DeepXRestSubmissionAcknowledgement, DeepXRestSubmissionError> {
    if prepared.record().identity().runtime()
        != &crate::transaction::DeepXDirectRuntimeIdentity::from(
            runtime_permit.snapshot().identity(),
        )
    {
        return Err(DeepXRestSubmissionError::NotSent(
            "runtime permit does not match the durable transaction".to_string(),
        ));
    }
    let operation = prepared.record().identity().operation().ok_or_else(|| {
        DeepXRestSubmissionError::NotSent("missing durable operation".to_string())
    })?;
    let action = rest_action(operation).ok_or_else(|| {
        DeepXRestSubmissionError::NotSent(
            "perpetual modification has no documented REST transact action".to_string(),
        )
    })?;
    let (bytes, expected_hash) = prepared.into_permit().into_payload();
    verify_submission_hash(&bytes, expected_hash, expected_hash)
        .map_err(|e| DeepXRestSubmissionError::NotSent(e.to_string()))?;
    let body = encode_transaction_request(action, &bytes)
        .map_err(|e| DeepXRestSubmissionError::NotSent(e.to_string()))?;
    runtime_permit
        .ensure_active()
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

/// A one-shot submission failure retained with its durable reconciliation context.
#[derive(Debug, Error)]
pub enum DeepXSubmissionAttemptFailure {
    /// Local evidence proves the transport did not start transmission.
    #[error("DeepX submission was not sent: {0}")]
    NotSent(String),
    /// The submission endpoint explicitly rejected the payload.
    #[error("DeepX submission endpoint rejected the payload: {0}")]
    VenueRejected(String),
    /// Transmission may have started and requires reconciliation.
    #[error("DeepX submission outcome is ambiguous: {0}")]
    Ambiguous(String),
    /// Claimed acceptance did not identify the exact permitted payload.
    #[error(transparent)]
    Hash(#[from] DeepXSubmissionError),
}

impl From<DeepXSubmissionFailure> for DeepXSubmissionAttemptFailure {
    fn from(value: DeepXSubmissionFailure) -> Self {
        match value {
            DeepXSubmissionFailure::NotSent(reason) => Self::NotSent(reason),
            DeepXSubmissionFailure::VenueRejected(reason) => Self::VenueRejected(reason),
            DeepXSubmissionFailure::Ambiguous(reason) => Self::Ambiguous(reason),
        }
    }
}

/// Result of consuming one permit while preserving caller-owned reconciliation context.
#[derive(Debug)]
pub enum DeepXSubmissionAttemptOutcome<C> {
    /// The transport returned the exact permitted extrinsic hash.
    Accepted {
        /// Durable context retained for the acceptance commit.
        context: C,
        /// Hash-verified node acceptance evidence.
        submitted: DeepXSubmittedExtrinsic,
    },
    /// Acceptance was not proven and the context must remain available for reconciliation.
    Unresolved {
        /// Durable context retained after the permit was consumed.
        context: C,
        /// Delivery classification or hash-verification failure.
        failure: DeepXSubmissionAttemptFailure,
    },
}

/// Consumes one prepared permit through exactly one classified transport attempt.
///
/// This coordinator never retries. It preserves `context` in every outcome so consuming the permit
/// cannot discard the durable evidence required to commit acceptance or reconcile ambiguity.
///
/// # Errors
///
/// Delivery and hash failures are returned inside
/// [`DeepXSubmissionAttemptOutcome::Unresolved`] together with the original context.
pub async fn submit_once_preserving_context<C, F, Fut>(
    context: C,
    permit: DeepXSubmissionPermit,
    submit: F,
) -> DeepXSubmissionAttemptOutcome<C>
where
    F: FnOnce(Vec<u8>, [u8; 32]) -> Fut,
    Fut: Future<Output = Result<[u8; 32], DeepXSubmissionFailure>>,
{
    let (bytes, expected_hash) = permit.into_payload();
    match submit(bytes.clone(), expected_hash).await {
        Ok(node_hash) => {
            match DeepXSubmittedExtrinsic::verified(&bytes, expected_hash, node_hash) {
                Ok(submitted) => DeepXSubmissionAttemptOutcome::Accepted { context, submitted },
                Err(error) => DeepXSubmissionAttemptOutcome::Unresolved {
                    context,
                    failure: error.into(),
                },
            }
        }
        Err(failure) => DeepXSubmissionAttemptOutcome::Unresolved {
            context,
            failure: failure.into(),
        },
    }
}

/// Rechecks a retained runtime permit before one classified submission attempt.
///
/// A revoked permit returns `NotSent` with the original durable context and never invokes the
/// transport. This check occurs immediately before the transport begins; an observation racing
/// after that point remains an in-flight attempt requiring normal reconciliation.
pub(crate) async fn submit_once_with_runtime_guard<C, F, Fut>(
    context: C,
    permit: DeepXSubmissionPermit,
    runtime_permit: &DeepXRuntimeSnapshotPermit,
    submit: F,
) -> DeepXSubmissionAttemptOutcome<C>
where
    F: FnOnce(Vec<u8>, [u8; 32]) -> Fut,
    Fut: Future<Output = Result<[u8; 32], DeepXSubmissionFailure>>,
{
    submit_once_preserving_context(context, permit, move |bytes, expected_hash| async move {
        runtime_permit
            .ensure_active()
            .map_err(|e| DeepXSubmissionFailure::NotSent(e.to_string()))?;
        submit(bytes, expected_hash).await
    })
    .await
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
                return Ok(DeepXSubmittedExtrinsic::verified(
                    &bytes,
                    extrinsic_hash,
                    node_hash,
                )?);
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
    DeepXRpcClient::new(submission_url.to_string())
        .submit_verified_extrinsic_once(extrinsic.bytes(), extrinsic.extrinsic_hash())
        .await
}

/// Submits exact prevalidated direct-pallet bytes once with conservative delivery classification.
///
/// Empty bytes or a local Blake2 hash mismatch prove that transmission was not attempted. Once the
/// RPC future starts, every transport, RPC response, decoding, or returned-hash failure is
/// ambiguous and requires reconciliation. This function never retries.
///
/// # Errors
///
/// Returns `NotSent` only for local payload validation and `Ambiguous` for every failure after the
/// `author_submitExtrinsic` request starts. It never returns `VenueRejected` because the shared RPC
/// transport does not expose evidence sufficient to distinguish rejection from uncertain delivery.
pub async fn submit_direct_pallet_once_classified(
    submission_url: &str,
    bytes: Vec<u8>,
    expected_hash: [u8; 32],
) -> Result<[u8; 32], DeepXSubmissionFailure> {
    if bytes.is_empty() {
        return Err(DeepXSubmissionFailure::not_sent(
            "signed extrinsic payload is empty",
        ));
    }
    if let Err(error) = verify_submission_hash(&bytes, expected_hash, expected_hash) {
        return Err(DeepXSubmissionFailure::not_sent(error.to_string()));
    }
    let submitted = DeepXRpcClient::new(submission_url.to_string())
        .submit_verified_extrinsic_once(&bytes, expected_hash)
        .await
        .map_err(|error| DeepXSubmissionFailure::ambiguous(error.to_string()))?;
    Ok(submitted.node_hash())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        future::ready,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{Json, Router, routing::post};
    use deepx_rust_sdk::rpc::SUBMIT_EXTRINSIC_METHOD;
    use nautilus_core::hex;
    use rstest::rstest;
    use serde_json::{Value, json};
    use subxt_core::config::{Hasher, substrate::BlakeTwo256};
    use tokio::net::TcpListener;

    use super::*;
    use crate::signing::{
        DeepXReadOnlyRuntimeSnapshot, DeepXRuntimeSnapshotService, RuntimeSnapshot, SigningError,
        sign_dynamic_pallet_call,
    };

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
        use std::sync::atomic::{AtomicUsize, Ordering};

        use axum::http::StatusCode;
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

    fn runtime_service() -> DeepXRuntimeSnapshotService {
        let metadata: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/deepx-rust-sdk/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        let bytes = hex::decode(
            metadata["result"]
                .as_str()
                .unwrap()
                .trim_start_matches("0x"),
        )
        .unwrap();
        let read_only = DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            hex::decode_array("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap(),
            376,
            1,
            &bytes,
        )
        .unwrap();
        DeepXRuntimeSnapshotService::new(RuntimeSnapshot::from_read_only_for_test(read_only))
    }

    fn signed_remark() -> Result<SignedPalletExtrinsic, SigningError> {
        let service = runtime_service();
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

    async fn spawn_counted_submission_server(
        response: Value,
        calls: Arc<AtomicUsize>,
        expected_bytes: Vec<u8>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&calls);
                let response = response.clone();
                let expected_bytes = expected_bytes.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(request["method"], SUBMIT_EXTRINSIC_METHOD);
                    assert_eq!(
                        request["params"],
                        json!([format!("0x{}", hex::encode(expected_bytes))])
                    );
                    Json(response)
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        (url, server)
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

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn revoked_runtime_permit_retains_context_without_calling_transport(
        #[case] client_retired: bool,
    ) {
        let service = runtime_service();
        let runtime_permit = service.acquire().unwrap();
        let extrinsic = signed_remark().unwrap();
        if client_retired {
            service.retire_client().unwrap();
        } else {
            service.halt_for_inconsistent_evidence().unwrap();
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);

        let outcome = submit_once_with_runtime_guard(
            "durable-context",
            submission_permit(&extrinsic),
            &runtime_permit,
            move |_, _| {
                observed.fetch_add(1, Ordering::Relaxed);
                ready(Ok(extrinsic.extrinsic_hash()))
            },
        )
        .await;

        assert!(matches!(
            outcome,
            DeepXSubmissionAttemptOutcome::Unresolved {
                context: "durable-context",
                failure: DeepXSubmissionAttemptFailure::NotSent(_),
            },
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn one_shot_submission_acceptance_preserves_context_and_exact_payload() {
        let extrinsic = signed_remark().unwrap();
        let expected_bytes = extrinsic.bytes().to_vec();
        let expected_hash = extrinsic.extrinsic_hash();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&calls);

        let outcome = submit_once_preserving_context(
            "durable-context",
            submission_permit(&extrinsic),
            move |bytes, hash| {
                observed.lock().unwrap().push((bytes, hash));
                ready(Ok(expected_hash))
            },
        )
        .await;

        assert!(matches!(
            outcome,
            DeepXSubmissionAttemptOutcome::Accepted { context, submitted }
                if context == "durable-context"
                    && submitted.node_hash() == expected_hash
                    && submitted.extrinsic_hash() == expected_hash
        ));
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            [(expected_bytes, expected_hash)]
        );
    }

    #[rstest]
    #[case(DeepXSubmissionFailure::not_sent("not sent"), 0)]
    #[case(DeepXSubmissionFailure::venue_rejected("rejected"), 1)]
    #[case(DeepXSubmissionFailure::ambiguous("unknown"), 2)]
    #[tokio::test]
    async fn one_shot_submission_failure_preserves_context_without_retry(
        #[case] delivery: DeepXSubmissionFailure,
        #[case] expected: u8,
    ) {
        let extrinsic = signed_remark().unwrap();
        let calls = Arc::new(Mutex::new(0_u32));
        let observed = Arc::clone(&calls);

        let outcome = submit_once_preserving_context(
            "durable-context",
            submission_permit(&extrinsic),
            move |_, _| {
                *observed.lock().unwrap() += 1;
                ready(Err(delivery))
            },
        )
        .await;

        assert_eq!(*calls.lock().unwrap(), 1);
        assert!(matches!(
            (&outcome, expected),
            (
                DeepXSubmissionAttemptOutcome::Unresolved {
                    context: "durable-context",
                    failure: DeepXSubmissionAttemptFailure::NotSent(_),
                },
                0,
            ) | (
                DeepXSubmissionAttemptOutcome::Unresolved {
                    context: "durable-context",
                    failure: DeepXSubmissionAttemptFailure::VenueRejected(_),
                },
                1,
            ) | (
                DeepXSubmissionAttemptOutcome::Unresolved {
                    context: "durable-context",
                    failure: DeepXSubmissionAttemptFailure::Ambiguous(_),
                },
                2,
            )
        ));
    }

    #[tokio::test]
    async fn one_shot_submission_hash_mismatch_preserves_context_without_retry() {
        let extrinsic = signed_remark().unwrap();
        let calls = Arc::new(Mutex::new(0_u32));
        let observed = Arc::clone(&calls);
        let wrong_hash = BlakeTwo256.hash(b"different accepted extrinsic").0;

        let outcome = submit_once_preserving_context(
            "durable-context",
            submission_permit(&extrinsic),
            move |_, _| {
                *observed.lock().unwrap() += 1;
                ready(Ok(wrong_hash))
            },
        )
        .await;

        assert_eq!(*calls.lock().unwrap(), 1);
        assert!(matches!(
            outcome,
            DeepXSubmissionAttemptOutcome::Unresolved {
                context: "durable-context",
                failure: DeepXSubmissionAttemptFailure::Hash(
                    DeepXSubmissionError::HashMismatch { .. }
                ),
            }
        ));
    }

    #[rstest]
    #[case(Vec::new(), BlakeTwo256.hash(b"empty").0)]
    #[case(b"signed payload".to_vec(), BlakeTwo256.hash(b"different payload").0)]
    #[tokio::test]
    async fn classified_direct_submission_rejects_local_payload_without_request(
        #[case] bytes: Vec<u8>,
        #[case] expected_hash: [u8; 32],
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let (url, server) = spawn_counted_submission_server(
            json!({"jsonrpc": "2.0", "id": 1, "result": "unreachable"}),
            Arc::clone(&calls),
            bytes.clone(),
        )
        .await;

        let failure = submit_direct_pallet_once_classified(&url, bytes, expected_hash)
            .await
            .unwrap_err();

        assert!(matches!(failure, DeepXSubmissionFailure::NotSent(_)));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        server.abort();
    }

    #[tokio::test]
    async fn classified_direct_submission_accepts_exact_hash_once() {
        let extrinsic = signed_remark().unwrap();
        let bytes = extrinsic.bytes().to_vec();
        let expected_hash = extrinsic.extrinsic_hash();
        let calls = Arc::new(AtomicUsize::new(0));
        let (url, server) = spawn_counted_submission_server(
            ok_response(&extrinsic),
            Arc::clone(&calls),
            bytes.clone(),
        )
        .await;

        let node_hash = submit_direct_pallet_once_classified(&url, bytes, expected_hash)
            .await
            .unwrap();

        assert_eq!(node_hash, expected_hash);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        server.abort();
    }

    #[rstest]
    #[case(json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": {"code": 1010, "message": "Invalid Transaction"},
    }))]
    #[case(json!({"jsonrpc": "2.0", "id": 1}))]
    #[case(json!({"jsonrpc": "2.0", "id": 1, "result": 42}))]
    #[case(json!({"jsonrpc": "2.0", "id": 1, "result": "0x00"}))]
    #[tokio::test]
    async fn classified_direct_submission_treats_rpc_failures_as_ambiguous(
        #[case] response: Value,
    ) {
        let extrinsic = signed_remark().unwrap();
        let bytes = extrinsic.bytes().to_vec();
        let calls = Arc::new(AtomicUsize::new(0));
        let (url, server) =
            spawn_counted_submission_server(response, Arc::clone(&calls), bytes.clone()).await;

        let failure = submit_direct_pallet_once_classified(&url, bytes, extrinsic.extrinsic_hash())
            .await
            .unwrap_err();

        assert!(matches!(failure, DeepXSubmissionFailure::Ambiguous(_)));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        server.abort();
    }

    #[tokio::test]
    async fn classified_direct_submission_treats_mismatched_node_hash_as_ambiguous() {
        let extrinsic = signed_remark().unwrap();
        let bytes = extrinsic.bytes().to_vec();
        let calls = Arc::new(AtomicUsize::new(0));
        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": format!(
                "0x{}",
                hex::encode(BlakeTwo256.hash(b"different extrinsic").0)
            ),
        });
        let (url, server) =
            spawn_counted_submission_server(response, Arc::clone(&calls), bytes.clone()).await;

        let failure = submit_direct_pallet_once_classified(&url, bytes, extrinsic.extrinsic_hash())
            .await
            .unwrap_err();

        assert!(matches!(failure, DeepXSubmissionFailure::Ambiguous(_)));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        server.abort();
    }

    #[tokio::test]
    async fn classified_direct_submission_treats_transport_failure_as_ambiguous() {
        let extrinsic = signed_remark().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (connection, _) = listener.accept().await.unwrap();
            drop(connection);
        });

        let failure = submit_direct_pallet_once_classified(
            &url,
            extrinsic.bytes().to_vec(),
            extrinsic.extrinsic_hash(),
        )
        .await
        .unwrap_err();

        assert!(matches!(failure, DeepXSubmissionFailure::Ambiguous(_)));
        server.await.unwrap();
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
}
