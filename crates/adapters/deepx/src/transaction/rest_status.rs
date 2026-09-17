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

//! Bounded REST observation of already-submitted transactions without replay authorization.

use std::{num::NonZeroU32, time::Duration};

use nautilus_core::hex;
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio_util::sync::CancellationToken;

use crate::http::{DeepXHttpClient, DeepXHttpError, Result, should_retry_http_error};

/// Documented backend confirmation states, none of which proves canonical finality.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeepXRestTransactionConfirmation {
    /// Backend accepted the transaction but is still reconciling its best-block events.
    Pending,
    /// Backend decoded an action result from its current best-chain observation.
    Best,
    /// Backend reports successful execution but exhausted action-result decoding retries.
    DecodeFailed,
}

/// Hash-bound backend observation, not canonical inclusion or business-event evidence.
#[derive(Clone, Debug)]
pub struct DeepXRestTransactionStatus {
    extrinsic_hash: [u8; 32],
    confirmation: DeepXRestTransactionConfirmation,
    order_id: Option<u64>,
    backend_status: Option<String>,
    data: Box<RawValue>,
}

impl DeepXRestTransactionStatus {
    /// Returns the hash checked against the queried extrinsic identity.
    #[must_use]
    pub const fn extrinsic_hash(&self) -> [u8; 32] {
        self.extrinsic_hash
    }

    /// Returns the backend confirmation state without promoting it to chain finality.
    #[must_use]
    pub const fn confirmation(&self) -> DeepXRestTransactionConfirmation {
        self.confirmation
    }

    /// Returns the exact backend order identifier; an empty or absent result is `None`.
    #[must_use]
    pub const fn order_id(&self) -> Option<u64> {
        self.order_id
    }

    /// Returns the optional backend status string without inventing additional state semantics.
    #[must_use]
    pub fn backend_status(&self) -> Option<&str> {
        self.backend_status.as_deref()
    }

    /// Returns the original payload, including uninterpreted fields and exact numeric lexemes.
    #[must_use]
    pub fn data(&self) -> &RawValue {
        &self.data
    }
}

/// Explicit limits for polling one already-submitted extrinsic.
#[derive(Clone, Copy, Debug)]
pub struct DeepXRestStatusPollPolicy {
    /// Maximum actual GET attempts, including transient HTTP failures.
    pub max_attempts: NonZeroU32,
    /// Delay between attempts; zero is rejected to prevent a busy polling loop.
    pub interval: Duration,
    /// Overall deadline covering requests and delays, independent of client request timeout.
    pub timeout: Duration,
}

/// Reason status polling stopped; no variant authorizes replay or nonce reuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXRestStatusPollTermination {
    /// Backend decoded a best-chain result, not necessarily a finalized result.
    Best,
    /// Backend cannot decode the action result; operator investigation is required.
    DecodeFailed,
    /// Pending observations or transient errors exhausted the attempt budget.
    AttemptLimit,
    /// The overall time budget expired, possibly with a request still unresolved.
    Timeout,
    /// Cancellation stopped polling, possibly with a request still unresolved.
    Cancelled,
    /// A nonretryable query or protocol error stopped polling.
    Failed,
}

/// Retained observations when a bounded status poll stops, with no lifecycle mutation.
#[derive(Debug)]
pub struct DeepXRestStatusPollResult {
    /// Reason polling stopped, not an order execution status.
    pub termination: DeepXRestStatusPollTermination,
    /// Number of GET attempts started; an interrupted in-flight attempt is included.
    pub attempts: u32,
    /// Latest successfully decoded hash-matching backend observation, if any.
    pub last_observation: Option<DeepXRestTransactionStatus>,
    /// Most recent query failure, cleared by a later successful observation.
    pub last_error: Option<DeepXHttpError>,
}

/// Queries the primary backend once for an already-submitted extrinsic's status.
///
/// No submission, redirect, automatic retry, endpoint failover, or lifecycle mutation occurs.
/// Not-found responses are backend lookup failures, not evidence that the extrinsic was unsent
/// or absent from the chain. Unknown/missing confirmation states fail closed.
///
/// # Errors
///
/// Returns a transport/API error or a schema/hash validation error. None authorizes replay.
pub async fn get_rest_transaction_status(
    client: &DeepXHttpClient,
    extrinsic_hash: [u8; 32],
) -> Result<DeepXRestTransactionStatus> {
    let data = client
        .get_transaction_status_once_raw(extrinsic_hash)
        .await?;
    parse_transaction_status(data, extrinsic_hash)
}

/// Polls status with bounded GET attempts, an overall deadline, and cancellation.
///
/// Only `pending` or retryable idempotent-read errors continue polling. `best`, `decode_failed`,
/// malformed results, hash mismatch, and nonretryable errors stop immediately. The result retains
/// the last valid observation when subsequent reads fail, time out, or are cancelled. This API
/// never transmits an extrinsic or advances transaction/order lifecycle state.
///
/// # Errors
///
/// Returns an invalid-request error before any GET for zero or unrepresentable timing limits.
pub async fn poll_rest_transaction_status(
    client: &DeepXHttpClient,
    extrinsic_hash: [u8; 32],
    policy: DeepXRestStatusPollPolicy,
    cancellation: &CancellationToken,
) -> Result<DeepXRestStatusPollResult> {
    if policy.interval.is_zero() || policy.timeout.is_zero() {
        return Err(DeepXHttpError::InvalidRequest(
            "transaction status polling requires positive interval and timeout".to_string(),
        ));
    }
    let deadline = tokio::time::Instant::now()
        .checked_add(policy.timeout)
        .ok_or_else(|| {
            DeepXHttpError::InvalidRequest("status poll deadline overflows".to_string())
        })?;
    let mut result = DeepXRestStatusPollResult {
        termination: DeepXRestStatusPollTermination::AttemptLimit,
        attempts: 0,
        last_observation: None,
        last_error: None,
    };
    result.termination = tokio::select! {
        biased;
        () = cancellation.cancelled() => DeepXRestStatusPollTermination::Cancelled,
        () = tokio::time::sleep_until(deadline) => DeepXRestStatusPollTermination::Timeout,
        termination = poll_status_attempts(client, extrinsic_hash, policy, deadline, &mut result) => termination,
    };
    Ok(result)
}

async fn poll_status_attempts(
    client: &DeepXHttpClient,
    extrinsic_hash: [u8; 32],
    policy: DeepXRestStatusPollPolicy,
    deadline: tokio::time::Instant,
    result: &mut DeepXRestStatusPollResult,
) -> DeepXRestStatusPollTermination {
    for attempt in 1..=policy.max_attempts.get() {
        result.attempts = attempt;

        match get_rest_transaction_status(client, extrinsic_hash).await {
            Ok(observation) => {
                let confirmation = observation.confirmation();
                result.last_observation = Some(observation);
                result.last_error = None;

                match confirmation {
                    DeepXRestTransactionConfirmation::Best => {
                        return DeepXRestStatusPollTermination::Best;
                    }
                    DeepXRestTransactionConfirmation::DecodeFailed => {
                        return DeepXRestStatusPollTermination::DecodeFailed;
                    }
                    DeepXRestTransactionConfirmation::Pending => {}
                }
            }
            Err(e) => {
                let retryable = should_retry_http_error(&e);
                result.last_error = Some(e);

                if !retryable {
                    return DeepXRestStatusPollTermination::Failed;
                }
            }
        }

        if attempt < policy.max_attempts.get() {
            let next = tokio::time::Instant::now()
                .checked_add(policy.interval)
                .unwrap_or(deadline);
            tokio::time::sleep_until(next.min(deadline)).await;
        }
    }
    DeepXRestStatusPollTermination::AttemptLimit
}

#[derive(Deserialize)]
struct TransactionStatusData {
    tx_hash: String,
    #[serde(default)]
    order_id: String,
    confirmation: DeepXRestTransactionConfirmation,
    status: Option<String>,
}

fn parse_transaction_status(
    data: Box<RawValue>,
    expected_hash: [u8; 32],
) -> Result<DeepXRestTransactionStatus> {
    let invalid = |message: String| DeepXHttpError::InvalidTransactionStatus { message };
    let decoded: TransactionStatusData =
        serde_json::from_str(data.get()).map_err(|e| invalid(e.to_string()))?;
    let extrinsic_hash = decoded
        .tx_hash
        .strip_prefix("0x")
        .and_then(|value| hex::decode_array::<32>(value).ok())
        .ok_or_else(|| invalid("tx_hash must be a 0x-prefixed 32-byte hash".to_string()))?;

    if extrinsic_hash != expected_hash {
        return Err(invalid(format!(
            "transaction hash mismatch: expected 0x{}, received {}",
            hex::encode(expected_hash),
            decoded.tx_hash,
        )));
    }
    let order_id = if decoded.order_id.is_empty() {
        None
    } else {
        if !decoded.order_id.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid(
                "order_id must be empty or a decimal u64 string".to_string(),
            ));
        }
        Some(
            decoded
                .order_id
                .parse::<u64>()
                .map_err(|e| invalid(e.to_string()))?,
        )
    };

    if decoded.confirmation != DeepXRestTransactionConfirmation::Best && order_id.is_some() {
        return Err(invalid(
            "pending/decode_failed result must have an empty order_id".to_string(),
        ));
    }
    Ok(DeepXRestTransactionStatus {
        extrinsic_hash,
        confirmation: decoded.confirmation,
        order_id,
        backend_status: decoded.status,
        data,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        Json, Router,
        extract::Query,
        http::StatusCode,
        routing::{get, post},
    };
    use nautilus_network::retry::RetryConfig;
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;
    use crate::http::models::DeepXResponseCode;

    const HASH: [u8; 32] = [0xab; 32];

    fn status_data(confirmation: &str, order_id: &str) -> Value {
        json!({
            "tx_hash": format!("0x{}", hex::encode(HASH)),
            "confirmation": confirmation,
            "order_id": order_id,
            "status": "included",
        })
    }

    fn success_body(confirmation: &str, order_id: &str) -> String {
        json!({
            "code": 200, "msg": "success", "fail": false,
            "data": status_data(confirmation, order_id),
        })
        .to_string()
    }

    fn raw(data: &Value) -> Box<RawValue> {
        RawValue::from_string(data.to_string()).unwrap()
    }

    fn policy(attempts: u32) -> DeepXRestStatusPollPolicy {
        DeepXRestStatusPollPolicy {
            max_attempts: NonZeroU32::new(attempts).unwrap(),
            interval: Duration::from_millis(1),
            timeout: Duration::from_secs(10),
        }
    }

    struct StatusServer {
        url: String,
        gets: Arc<AtomicUsize>,
        posts: Arc<AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for StatusServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn status_server(responses: Vec<(u16, String)>) -> StatusServer {
        let gets = Arc::new(AtomicUsize::new(0));
        let posts = Arc::new(AtomicUsize::new(0));
        let observed_gets = gets.clone();
        let observed_posts = posts.clone();
        let responses = Arc::new(responses);
        let app = Router::new()
            .route(
                "/internal/v1/chain/tx/status",
                get(
                    move |Query(query): Query<std::collections::HashMap<String, String>>| {
                        let responses = responses.clone();
                        let index = observed_gets.fetch_add(1, Ordering::Relaxed);
                        async move {
                            assert_eq!(query.len(), 1);
                            assert_eq!(query["txHash"], format!("0x{}", hex::encode(HASH)));
                            let (status, body) = &responses[index];
                            (
                                StatusCode::from_u16(*status).unwrap(),
                                [("Location", "/internal/v1/chain/tx/status")],
                                body.clone(),
                            )
                        }
                    },
                ),
            )
            .route(
                "/internal/v1/chain/tx/transact",
                post(move || {
                    observed_posts.fetch_add(1, Ordering::Relaxed);
                    async { StatusCode::INTERNAL_SERVER_ERROR }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        StatusServer {
            url,
            gets,
            posts,
            task,
        }
    }

    fn client(server: &StatusServer) -> DeepXHttpClient {
        DeepXHttpClient::new_with_endpoints(
            [server.url.clone(), "http://127.0.0.1:1".to_string()],
            Some(2),
            None,
            RetryConfig {
                max_retries: 9,
                ..RetryConfig::default()
            },
        )
        .unwrap()
    }

    #[rstest]
    #[case("pending", "", DeepXRestTransactionConfirmation::Pending, None)]
    #[case("best", "", DeepXRestTransactionConfirmation::Best, None)]
    #[case(
        "best",
        "18446744073709551615",
        DeepXRestTransactionConfirmation::Best,
        Some(u64::MAX)
    )]
    #[case("best", "0", DeepXRestTransactionConfirmation::Best, Some(0))]
    #[case("best", "00042", DeepXRestTransactionConfirmation::Best, Some(42))]
    #[case(
        "decode_failed",
        "",
        DeepXRestTransactionConfirmation::DecodeFailed,
        None
    )]
    fn parses_documented_backend_states(
        #[case] confirmation: &str,
        #[case] id: &str,
        #[case] expected: DeepXRestTransactionConfirmation,
        #[case] order_id: Option<u64>,
    ) {
        let status = parse_transaction_status(raw(&status_data(confirmation, id)), HASH).unwrap();
        assert_eq!(status.extrinsic_hash(), HASH);
        assert_eq!(status.confirmation(), expected);
        assert_eq!(status.order_id(), order_id);
        assert_eq!(status.backend_status(), Some("included"));
    }

    #[rstest]
    fn preserves_original_numeric_lexemes_and_optional_status() {
        let body = format!(
            r#"{{"tx_hash":"0x{}","confirmation":"best","order_id":"42","extra":0.1234567890123456789012345678}}"#,
            hex::encode(HASH)
        );
        let status =
            parse_transaction_status(RawValue::from_string(body.clone()).unwrap(), HASH).unwrap();
        assert_eq!(status.data().get(), body);
        assert_eq!(status.backend_status(), None);
    }

    #[rstest]
    #[case("confirmation", json!("finalized"))]
    #[case("confirmation", json!("unknown"))]
    #[case("confirmation", json!("Best"))]
    #[case("confirmation", json!(null))]
    #[case("tx_hash", json!("0x1234"))]
    #[case("tx_hash", json!("abababababababababababababababababababababababababababababababab"))]
    #[case("tx_hash", json!("0x0000000000000000000000000000000000000000000000000000000000000000"))]
    #[case("tx_hash", json!(42))]
    #[case("order_id", json!("18446744073709551616"))]
    #[case("order_id", json!("-1"))]
    #[case("order_id", json!("+1"))]
    #[case("order_id", json!("1.0"))]
    #[case("order_id", json!(" 1"))]
    #[case("order_id", json!("1 "))]
    #[case("order_id", json!(42))]
    #[case("order_id", json!(null))]
    #[case("status", json!(42))]
    fn malformed_observations_fail_closed(#[case] field: &str, #[case] value: Value) {
        let mut data = status_data("best", "42");
        data[field] = value;
        assert!(matches!(
            parse_transaction_status(raw(&data), HASH),
            Err(DeepXHttpError::InvalidTransactionStatus { .. })
        ));
    }

    #[rstest]
    #[case("confirmation")]
    #[case("tx_hash")]
    fn missing_required_status_fields_fail_closed(#[case] field: &str) {
        let mut data = status_data("best", "42");
        data.as_object_mut().unwrap().remove(field);
        assert!(matches!(
            parse_transaction_status(raw(&data), HASH),
            Err(DeepXHttpError::InvalidTransactionStatus { .. })
        ));
    }

    #[rstest]
    #[case("pending")]
    #[case("best")]
    #[case("decode_failed")]
    fn absent_order_id_does_not_invent_an_action_result(#[case] confirmation: &str) {
        let mut data = status_data(confirmation, "");
        data.as_object_mut().unwrap().remove("order_id");
        let status = parse_transaction_status(raw(&data), HASH).unwrap();
        assert_eq!(status.order_id(), None);
        assert_eq!(status.data().get(), data.to_string());
    }

    #[rstest]
    #[case("pending")]
    #[case("decode_failed")]
    fn unresolved_order_result_must_remain_empty(#[case] confirmation: &str) {
        assert!(matches!(
            parse_transaction_status(raw(&status_data(confirmation, "42")), HASH),
            Err(DeepXHttpError::InvalidTransactionStatus { .. })
        ));
    }

    #[rstest]
    #[case("best", "18446744073709551615", DeepXRestStatusPollTermination::Best)]
    #[case("decode_failed", "", DeepXRestStatusPollTermination::DecodeFailed)]
    #[tokio::test]
    async fn pending_poll_stops_on_documented_backend_result(
        #[case] confirmation: &str,
        #[case] order_id: &str,
        #[case] termination: DeepXRestStatusPollTermination,
    ) {
        let server = status_server(vec![
            (200, success_body("pending", "")),
            (200, success_body(confirmation, order_id)),
        ])
        .await;
        let result = poll_rest_transaction_status(
            &client(&server),
            HASH,
            policy(9),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(result.termination, termination);
        assert_eq!(result.attempts, 2);
        assert_eq!(result.last_observation.unwrap().extrinsic_hash(), HASH);
        assert!(result.last_error.is_none());
        assert_eq!(server.gets.load(Ordering::Relaxed), 2);
        assert_eq!(server.posts.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn pending_poll_exhausts_without_replay() {
        let server = status_server(vec![(200, success_body("pending", "")); 3]).await;
        let result = poll_rest_transaction_status(
            &client(&server),
            HASH,
            policy(3),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            result.termination,
            DeepXRestStatusPollTermination::AttemptLimit
        );
        assert_eq!(result.attempts, 3);
        assert_eq!(
            result.last_observation.unwrap().confirmation(),
            DeepXRestTransactionConfirmation::Pending
        );
        assert!(result.last_error.is_none());
        assert_eq!(server.gets.load(Ordering::Relaxed), 3);
        assert_eq!(server.posts.load(Ordering::Relaxed), 0);
    }

    #[rstest]
    #[case(503, "unavailable".to_string())]
    #[case(429, "rate limited".to_string())]
    #[case(200, json!({"code":10012,"fail":true,"msg":"unavailable","data":null}).to_string())]
    #[case(200, json!({"code":10010,"fail":true,"msg":"rate limited","data":null}).to_string())]
    #[tokio::test]
    async fn transient_reads_consume_explicit_poll_budget(
        #[case] status: u16,
        #[case] body: String,
    ) {
        let server = status_server(vec![
            (status, body),
            (200, success_body("pending", "")),
            (200, success_body("best", "42")),
        ])
        .await;
        let result = poll_rest_transaction_status(
            &client(&server),
            HASH,
            policy(3),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(result.termination, DeepXRestStatusPollTermination::Best);
        assert_eq!(result.attempts, 3);
        assert_eq!(result.last_observation.unwrap().order_id(), Some(42));
        assert!(result.last_error.is_none());
        assert_eq!(server.gets.load(Ordering::Relaxed), 3);
        assert_eq!(server.posts.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn transient_failure_retains_last_valid_pending_observation() {
        let server = status_server(vec![
            (200, success_body("pending", "")),
            (503, "unavailable".to_string()),
        ])
        .await;
        let result = poll_rest_transaction_status(
            &client(&server),
            HASH,
            policy(2),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            result.termination,
            DeepXRestStatusPollTermination::AttemptLimit
        );
        assert_eq!(result.attempts, 2);
        assert_eq!(
            result.last_observation.unwrap().confirmation(),
            DeepXRestTransactionConfirmation::Pending
        );
        assert!(matches!(
            result.last_error,
            Some(DeepXHttpError::Http { status: 503, .. })
        ));
        assert_eq!(server.gets.load(Ordering::Relaxed), 2);
        assert_eq!(server.posts.load(Ordering::Relaxed), 0);
    }

    #[rstest]
    #[case(200, json!({"code":10020,"fail":true,"msg":"not found","data":null}).to_string())]
    #[case(200, json!({"code":10018,"fail":true,"msg":"unauthorized","data":null}).to_string())]
    #[case(200, json!({"code":"19_0","fail":true,"msg":"runtime failure","data":null}).to_string())]
    #[case(200, success_body("finalized", "42"))]
    #[case(200, "not json".to_string())]
    #[case(200, json!({"code":200,"fail":false,"msg":"success","data":null}).to_string())]
    #[case(200, json!({"code":200,"fail":false,"msg":"success"}).to_string())]
    #[case(401, "unauthorized".to_string())]
    #[case(404, "not found".to_string())]
    #[case(307, "redirect".to_string())]
    #[case(308, "redirect".to_string())]
    #[tokio::test]
    async fn nonretryable_status_failure_stops_without_replay(
        #[case] status: u16,
        #[case] body: String,
    ) {
        let server = status_server(vec![(status, body)]).await;
        let result = poll_rest_transaction_status(
            &client(&server),
            HASH,
            policy(9),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(result.termination, DeepXRestStatusPollTermination::Failed);
        assert_eq!(result.attempts, 1);
        assert!(result.last_observation.is_none());
        assert!(result.last_error.is_some());
        assert_eq!(server.gets.load(Ordering::Relaxed), 1);
        assert_eq!(server.posts.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn mismatched_hash_does_not_replace_last_valid_observation() {
        let mut body: Value = serde_json::from_str(&success_body("best", "42")).unwrap();
        body["data"]["tx_hash"] = json!(format!("0x{}", hex::encode([0; 32])));
        let server = status_server(vec![
            (200, success_body("pending", "")),
            (200, body.to_string()),
        ])
        .await;
        let result = poll_rest_transaction_status(
            &client(&server),
            HASH,
            policy(9),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(result.termination, DeepXRestStatusPollTermination::Failed);
        assert_eq!(result.attempts, 2);
        assert_eq!(
            result.last_observation.unwrap().confirmation(),
            DeepXRestTransactionConfirmation::Pending
        );
        assert!(matches!(
            result.last_error,
            Some(DeepXHttpError::InvalidTransactionStatus { .. })
        ));
        assert_eq!(server.posts.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn unknown_hash_is_a_lookup_error_not_submission_evidence() {
        let server = status_server(vec![(
            200,
            json!({"code":10020,"fail":true,"msg":"Resource not found.","data":null}).to_string(),
        )])
        .await;
        assert!(matches!(
            get_rest_transaction_status(&client(&server), HASH).await,
            Err(DeepXHttpError::Api {
                code: DeepXResponseCode::Api(10020),
                ..
            })
        ));
        assert_eq!(server.gets.load(Ordering::Relaxed), 1);
        assert_eq!(server.posts.load(Ordering::Relaxed), 0);
    }

    #[rstest]
    #[case(Duration::ZERO, Duration::from_secs(1))]
    #[case(Duration::from_secs(1), Duration::ZERO)]
    #[case(Duration::from_secs(1), Duration::MAX)]
    #[tokio::test]
    async fn invalid_poll_timing_never_queries(
        #[case] interval: Duration,
        #[case] timeout: Duration,
    ) {
        let server = status_server(vec![]).await;
        let policy = DeepXRestStatusPollPolicy {
            interval,
            timeout,
            ..policy(1)
        };
        assert!(matches!(
            poll_rest_transaction_status(&client(&server), HASH, policy, &CancellationToken::new())
                .await,
            Err(DeepXHttpError::InvalidRequest(_))
        ));
        assert_eq!(server.gets.load(Ordering::Relaxed), 0);
        assert_eq!(server.posts.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn precancelled_poll_never_queries() {
        let server = status_server(vec![]).await;
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let result = poll_rest_transaction_status(&client(&server), HASH, policy(9), &cancellation)
            .await
            .unwrap();
        assert_eq!(
            result.termination,
            DeepXRestStatusPollTermination::Cancelled
        );
        assert_eq!(result.attempts, 0);
        assert!(result.last_observation.is_none());
        assert!(result.last_error.is_none());
        assert_eq!(server.gets.load(Ordering::Relaxed), 0);
        assert_eq!(server.posts.load(Ordering::Relaxed), 0);
    }

    #[rstest]
    #[case(false, DeepXRestStatusPollTermination::Timeout)]
    #[case(true, DeepXRestStatusPollTermination::Cancelled)]
    #[tokio::test]
    async fn interrupted_request_retains_previous_pending_observation(
        #[case] cancel: bool,
        #[case] termination: DeepXRestStatusPollTermination,
    ) {
        let gets = Arc::new(AtomicUsize::new(0));
        let requests = gets.clone();
        let second_request = Arc::new(tokio::sync::Notify::new());
        let notify = second_request.clone();
        let app = Router::new().route(
            "/internal/v1/chain/tx/status",
            get(move || {
                let attempt = requests.fetch_add(1, Ordering::Relaxed);
                let notify = notify.clone();
                async move {
                    if attempt == 0 {
                        Json(serde_json::from_str::<Value>(&success_body("pending", "")).unwrap())
                    } else {
                        notify.notify_one();
                        std::future::pending::<Json<Value>>().await
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = DeepXHttpClient::new(
            format!("http://{}", listener.local_addr().unwrap()),
            None,
            None,
        )
        .unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let cancellation = CancellationToken::new();
        let worker_cancel = cancellation.clone();
        let policy = DeepXRestStatusPollPolicy {
            timeout: Duration::from_secs(3600),
            ..policy(9)
        };
        let worker = tokio::spawn(async move {
            poll_rest_transaction_status(&client, HASH, policy, &worker_cancel)
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(5), second_request.notified())
            .await
            .unwrap();

        if cancel {
            cancellation.cancel();
        } else {
            tokio::time::pause();
            tokio::time::advance(policy.timeout).await;
        }
        let result = worker.await.unwrap();
        assert_eq!(result.termination, termination);
        assert_eq!(result.attempts, 2);
        assert_eq!(
            result.last_observation.unwrap().confirmation(),
            DeepXRestTransactionConfirmation::Pending
        );
        assert!(result.last_error.is_none());
        assert_eq!(gets.load(Ordering::Relaxed), 2);
        server.abort();
    }
}
