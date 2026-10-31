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

//! Typed and raw wallet quota REST queries and claim submission.

use std::{collections::HashSet, time::Duration};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::time::{Instant, sleep};

use super::{
    DeepXAccountPage, DeepXRawAccountPage, DeepXRestError, DeepXRestResponse, DeepXRestTransport,
    decode_account_history_page, decode_response, is_account_address, normalize_base_url,
    send_request, with_timeout,
};
use crate::{
    query::{DeepXQuotaHistoryRequest, DeepXQuotaHistoryType},
    rest::models::{
        DeepXQuotaClaim, DeepXQuotaClaimIdentity, DeepXQuotaClaimSubmission,
        DeepXQuotaHistoryRecord, DeepXQuotaSummary, DeepXWalletQuota,
    },
    signing::DeepXQuotaClaimSignature,
};

const QUOTA_SUMMARY_PATH: &str = "/internal/v1/account/quota/summary";
const QUOTA_CLAIM_PATH: &str = "/internal/v1/account/quota/claim";
const QUOTA_HISTORY_PATH: &str = "/internal/v1/account/quota/history";

#[derive(Serialize)]
struct WalletQuery<'a> {
    wallet: &'a str,
}

#[derive(Serialize)]
struct ClaimQuery {
    id: u64,
}

#[derive(Deserialize)]
struct QuotaClaimSubmissionWire {
    status: String,
    created: bool,
    claim: Option<Box<RawValue>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuotaClaimTaskState {
    status: String,
    last_error: Option<String>,
}

impl DeepXRestTransport {
    /// Returns the public wallet quota amounts without an API envelope.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, HTTP, or decoding error.
    pub async fn get_wallet_quota(
        &self,
        base_url: &str,
        wallet: &str,
    ) -> Result<DeepXWalletQuota, DeepXRestError> {
        if !is_account_address(wallet) {
            return Err(DeepXRestError::InvalidRequest(
                "wallet quota requires a 20-byte hex wallet address".to_string(),
            ));
        }
        self.get_json(base_url, &format!("/v1/account/wallets/{wallet}/quota"))
            .await
    }

    /// Submits one signed quota claim and decodes the public response.
    ///
    /// A transport error leaves delivery uncertain. The caller retains the idempotency key
    /// and reconciles the outcome before another submission.
    ///
    /// # Errors
    ///
    /// Returns a URL, transport, HTTP, decoding, or invalid-response error.
    pub async fn post_quota_claim(
        &self,
        base_url: &str,
        signed: &DeepXQuotaClaimSignature,
    ) -> Result<DeepXQuotaClaimSubmission, DeepXRestError> {
        let response = self.post_quota_claim_once(base_url, signed).await?;
        let raw = decode_response(response.status.as_u16(), &response.body)?;
        decode_quota_claim_submission(raw)
    }

    /// Submits one signed wallet quota claim without redirects or automatic retries.
    ///
    /// The response retains its HTTP status and body because a transport failure can leave
    /// submission uncertain. The caller owns the idempotency key and claim reconciliation.
    ///
    /// # Errors
    ///
    /// Returns a URL, serialization, or transport error; the caller must reconcile uncertain
    /// delivery before issuing another claim.
    pub async fn post_quota_claim_once(
        &self,
        base_url: &str,
        signed: &DeepXQuotaClaimSignature,
    ) -> Result<DeepXRestResponse, DeepXRestError> {
        let base_url = normalize_base_url(base_url.to_string())?;
        let path = format!("/v1/account/wallets/{}/quota/claims", signed.wallet());
        let body = serde_json::to_vec(signed)?;
        let request = self
            .transaction_client
            .post(format!("{base_url}{path}"))
            .header("Content-Type", "application/json")
            .body(body);
        send_request(with_timeout(request, self.timeout_secs)).await
    }

    /// Returns the exact quota summary for a wallet.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-aggregate error.
    pub async fn get_quota_summary(
        &self,
        base_url: &str,
        wallet: &str,
    ) -> Result<DeepXQuotaSummary, DeepXRestError> {
        let raw = self.get_quota_summary_raw(base_url, wallet).await?;
        let summary = serde_json::from_str(raw.get())?;
        validate_quota_summary(&summary, wallet)?;
        Ok(summary)
    }

    /// Returns one validated typed quota history page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, decoding, or invalid-record error.
    pub async fn get_quota_history(
        &self,
        base_url: &str,
        request: &DeepXQuotaHistoryRequest,
    ) -> Result<DeepXAccountPage<DeepXQuotaHistoryRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_quota_history_raw(base_url, request).await?,
            "quota history",
            request.limit,
        )?;
        validate_quota_history_page(&page, request)?;
        Ok(page)
    }

    /// Returns one quota claim with a validated request identity and exact backend payload.
    ///
    /// The task state remains uninterpreted because the venue does not define a stable schema.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or identity mismatch error.
    pub async fn get_quota_claim(
        &self,
        base_url: &str,
        claim_id: u64,
    ) -> Result<DeepXQuotaClaim, DeepXRestError> {
        let raw = self.get_quota_claim_raw(base_url, claim_id).await?;
        decode_quota_claim(raw, claim_id)
    }

    /// Polls one claim until it is confirmed, fails, or exceeds the total deadline.
    ///
    /// Transport errors, rate limits, and server errors are retried within the deadline.
    /// Other HTTP, API, or decoding failures stop polling. No new claim is submitted.
    ///
    /// # Errors
    ///
    /// Returns an invalid-request, claim-failed, timeout, or non-transient REST error.
    pub async fn wait_quota_claim(
        &self,
        base_url: &str,
        claim_id: u64,
        timeout: Duration,
        interval: Duration,
    ) -> Result<DeepXQuotaClaim, DeepXRestError> {
        if claim_id == 0 || timeout.is_zero() || interval.is_zero() {
            return Err(DeepXRestError::InvalidRequest(
                "quota claim polling requires a positive ID, timeout, and interval".to_string(),
            ));
        }
        let deadline = Instant::now() + timeout;
        let mut last_status = "unknown".to_string();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(DeepXRestError::QuotaClaimTimeout {
                    claim_id,
                    last_status,
                });
            }
            match tokio::time::timeout(remaining, self.get_quota_claim(base_url, claim_id)).await {
                Ok(Ok(claim)) => {
                    let state: QuotaClaimTaskState = serde_json::from_str(claim.data().get())?;
                    let status = state.status.to_ascii_lowercase();
                    match status.as_str() {
                        "confirmed" => return Ok(claim),
                        "failed" => {
                            return Err(DeepXRestError::QuotaClaimFailed {
                                claim_id,
                                message: state.last_error.unwrap_or_default(),
                            });
                        }
                        _ => last_status = status,
                    }
                }
                Ok(Err(
                    DeepXRestError::Transport(_)
                    | DeepXRestError::Http {
                        status: 429 | 500..=599,
                        ..
                    },
                )) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(DeepXRestError::QuotaClaimTimeout {
                        claim_id,
                        last_status,
                    });
                }
            }
            sleep(interval.min(deadline.saturating_duration_since(Instant::now()))).await;
        }
    }

    /// Returns the uninterpreted quota summary for a wallet.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_quota_summary_raw(
        &self,
        base_url: &str,
        wallet: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        if !is_account_address(wallet) {
            return Err(DeepXRestError::InvalidRequest(
                "quota summary requires a 20-byte hex wallet address".to_string(),
            ));
        }
        self.get_api_data_with_query(base_url, QUOTA_SUMMARY_PATH, &WalletQuery { wallet })
            .await
    }

    /// Returns the uninterpreted quota claim task state.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_quota_claim_raw(
        &self,
        base_url: &str,
        claim_id: u64,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        if claim_id == 0 {
            return Err(DeepXRestError::InvalidRequest(
                "quota-claim id must be greater than zero".to_string(),
            ));
        }
        self.get_api_data_with_query(base_url, QUOTA_CLAIM_PATH, &ClaimQuery { id: claim_id })
            .await
    }

    /// Returns one uninterpreted quota history page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_quota_history_raw(
        &self,
        base_url: &str,
        request: &DeepXQuotaHistoryRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            QUOTA_HISTORY_PATH,
            &request.as_query(),
            "quota history",
        )
        .await
    }
}

fn decode_quota_claim(
    raw: Box<RawValue>,
    claim_id: u64,
) -> Result<DeepXQuotaClaim, DeepXRestError> {
    let identity: DeepXQuotaClaimIdentity = serde_json::from_str(raw.get())?;
    if identity.id != claim_id {
        return Err(DeepXRestError::InvalidAccountResponse {
            endpoint: "quota claim",
            message: format!(
                "claim ID mismatch: expected {claim_id}, received {}",
                identity.id
            ),
        });
    }
    Ok(DeepXQuotaClaim::new(identity.id, raw))
}

fn decode_quota_claim_submission(
    raw: Box<RawValue>,
) -> Result<DeepXQuotaClaimSubmission, DeepXRestError> {
    let wire: QuotaClaimSubmissionWire = serde_json::from_str(raw.get())?;
    let claim = wire
        .claim
        .map(|claim| {
            let identity: DeepXQuotaClaimIdentity = serde_json::from_str(claim.get())?;
            if identity.id == 0 {
                return Err(DeepXRestError::InvalidAccountResponse {
                    endpoint: "quota claim submission",
                    message: "claim ID must be greater than zero".to_string(),
                });
            }
            Ok(DeepXQuotaClaim::new(identity.id, claim))
        })
        .transpose()?;
    let noop = wire.status.eq_ignore_ascii_case("noop");
    if wire.status.is_empty()
        || (noop && (wire.created || claim.is_some()))
        || (!noop && claim.is_none())
    {
        return Err(DeepXRestError::InvalidAccountResponse {
            endpoint: "quota claim submission",
            message: "claim status and task are inconsistent".to_string(),
        });
    }
    Ok(DeepXQuotaClaimSubmission::new(
        wire.status,
        wire.created,
        claim,
        raw,
    ))
}

fn validate_quota_history_page(
    page: &DeepXAccountPage<DeepXQuotaHistoryRecord>,
    request: &DeepXQuotaHistoryRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "quota history";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if request
        .limit
        .is_some_and(|limit| page.items.len() > limit as usize)
    {
        return Err(invalid("page exceeds requested page size".to_string()));
    }
    let mut ids = HashSet::new();
    let mut events = HashSet::new();
    let mut previous = None;
    for (index, record) in page.items.iter().enumerate() {
        let created = validate_quota_history_record(index, record, request)?;
        if previous.is_some_and(|previous| created > previous) {
            return Err(invalid(format!(
                "item {index} violates descending creation-time order"
            )));
        }
        previous = Some(created);
        if !ids.insert(&record.id) {
            return Err(invalid(format!(
                "item {index} repeats quota history identity"
            )));
        }
        if !events.insert((record.block_number, record.event_index)) {
            return Err(invalid(format!(
                "item {index} repeats quota chain event identity"
            )));
        }
    }
    Ok(())
}

/// Tracks quota history ordering and unique identities across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXQuotaHistoryContinuity {
    ids: HashSet<String>,
    events: HashSet<(u64, u64)>,
    previous: Option<jiff::Timestamp>,
}

impl DeepXQuotaHistoryContinuity {
    /// Checks one page against all quota history records previously observed.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign record, repeated identity, or increasing creation time.
    pub fn observe_page(
        &mut self,
        page: &DeepXAccountPage<DeepXQuotaHistoryRecord>,
        request: &DeepXQuotaHistoryRequest,
    ) -> Result<(), DeepXRestError> {
        let endpoint = "quota history";
        let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
        for (index, record) in page.items.iter().enumerate() {
            let created = validate_quota_history_record(index, record, request)?;
            if self.previous.is_some_and(|previous| created > previous) {
                return Err(invalid(format!(
                    "item {index} violates descending creation-time order"
                )));
            }
            self.previous = Some(created);
            if !self.ids.insert(record.id.clone()) {
                return Err(invalid(format!(
                    "item {index} repeats quota history identity"
                )));
            }
            if !self
                .events
                .insert((record.block_number, record.event_index))
            {
                return Err(invalid(format!(
                    "item {index} repeats quota chain event identity"
                )));
            }
        }
        Ok(())
    }
}

fn validate_quota_history_record(
    index: usize,
    record: &DeepXQuotaHistoryRecord,
    request: &DeepXQuotaHistoryRequest,
) -> Result<jiff::Timestamp, DeepXRestError> {
    let endpoint = "quota history";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if record.id.trim().is_empty()
        || !is_account_address(&record.owner_address)
        || !record.owner_address.eq_ignore_ascii_case(&request.wallet)
        || request
            .history_type
            .is_some_and(|expected| record.history_type != expected)
    {
        return Err(invalid(format!(
            "item {index} has invalid scope or identity"
        )));
    }
    match (
        record.history_type,
        record.buyer_type,
        &record.buyer_address,
    ) {
        (DeepXQuotaHistoryType::Purchase, Some(_), Some(buyer))
            if is_account_address(buyer)
                && request
                    .buyer_address
                    .as_deref()
                    .is_none_or(|expected| buyer.eq_ignore_ascii_case(expected)) => {}
        (DeepXQuotaHistoryType::Activate | DeepXQuotaHistoryType::Free, None, None)
            if request.buyer_address.is_none() => {}
        _ => return Err(invalid(format!("item {index} has invalid buyer metadata"))),
    }
    if record.quota == 0
        || record.block_number == 0
        || record.tx_hash_type.trim().is_empty()
        || !record.tx_hash.strip_prefix("0x").is_some_and(|hash| {
            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return Err(invalid(format!("item {index} has invalid chain metadata")));
    }
    let created = record
        .created_at
        .parse::<jiff::Timestamp>()
        .map_err(|e| invalid(format!("item {index} has invalid createdAt: {e}")))?;
    u64::try_from(created.as_millisecond())
        .map_err(|_| invalid(format!("item {index} timestamp is before the Unix epoch")))?;
    Ok(created)
}

fn validate_quota_summary(summary: &DeepXQuotaSummary, wallet: &str) -> Result<(), DeepXRestError> {
    let endpoint = "quota summary";
    let invalid = |message| DeepXRestError::InvalidAccountResponse { endpoint, message };
    if !is_account_address(&summary.owner) || !summary.owner.eq_ignore_ascii_case(wallet) {
        return Err(invalid(
            "response owner does not match the requested wallet".to_string(),
        ));
    }
    if summary.spot_volume_usd < Decimal::ZERO
        || summary.perp_volume_usd < Decimal::ZERO
        || summary.total_volume_usd < Decimal::ZERO
        || summary.spot_volume_usd.checked_add(summary.perp_volume_usd)
            != Some(summary.total_volume_usd)
    {
        return Err(invalid(
            "USD volume aggregates are inconsistent".to_string(),
        ));
    }
    let parse_bound =
        |name: &str, value: Option<&str>, milliseconds: Option<u64>| match (value, milliseconds) {
            (None, None) => Ok(None),
            (Some(value), Some(milliseconds)) => {
                let timestamp = value
                    .parse::<jiff::Timestamp>()
                    .map_err(|e| invalid(format!("{name} timestamp is not valid RFC 3339: {e}")))?;
                if u64::try_from(timestamp.as_millisecond()).ok() != Some(milliseconds) {
                    return Err(invalid(format!(
                        "{name} timestamp representations do not match"
                    )));
                }
                Ok(Some(milliseconds))
            }
            _ => Err(invalid(format!(
                "{name} timestamp representations are incomplete"
            ))),
        };
    let first = parse_bound(
        "first trade",
        summary.first_trade_at.as_deref(),
        summary.first_trade_ts_ms,
    )?;
    let last = parse_bound(
        "last trade",
        summary.last_trade_at.as_deref(),
        summary.last_trade_ts_ms,
    )?;
    if first.is_some() != last.is_some()
        || first.zip(last).is_some_and(|(first, last)| first > last)
    {
        return Err(invalid(
            "trade timestamp bounds are inconsistent".to_string(),
        ));
    }
    if let Some(updated_at) = &summary.updated_at {
        updated_at
            .parse::<jiff::Timestamp>()
            .map_err(|e| invalid(format!("updatedAt timestamp is not valid RFC 3339: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        Json, Router,
        extract::State,
        http::StatusCode,
        routing::{get, post},
    };
    use rstest::rstest;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        credential::{DeepXKeyScheme, DeepXPrivateKey},
        rest::DeepXApiResponse,
        signing::sign_quota_claim,
    };

    const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
    const RESPONSE: &str = include_str!("../../test_data/http/testnet/wallet_quota_summary.json");
    const HISTORY_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/quota_history_purchase_page.json");

    fn history_request() -> DeepXQuotaHistoryRequest {
        DeepXQuotaHistoryRequest {
            wallet: "0x0a40c3efbc3b3bebdf1fb6f0f8c612eb336b25ae".to_string(),
            buyer_address: None,
            history_type: None,
            cursor: None,
            limit: Some(5),
        }
    }

    #[tokio::test]
    async fn quota_claim_posts_signed_fields_once_and_preserves_response() {
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let signed = sign_quota_claim(&key, WALLET, "claim-1").unwrap();
        let expected = serde_json::to_value(&signed).unwrap();
        let path = format!("/v1/account/wallets/{WALLET}/quota/claims");
        let router = Router::new().route(
            &path,
            post(move |Json(body): Json<Value>| {
                let expected = expected.clone();
                async move {
                    assert_eq!(body, expected);
                    (
                        StatusCode::ACCEPTED,
                        r#"{"status":"pending","created":true,"claim":{"id":42,"quotaAmount":5}}"#,
                    )
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let transport = DeepXRestTransport::new(Some(5), None).unwrap();
        let response = transport
            .post_quota_claim_once(&format!("http://{address}"), &signed)
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::ACCEPTED);
        assert_eq!(
            response.body,
            br#"{"status":"pending","created":true,"claim":{"id":42,"quotaAmount":5}}"#
        );
        server.abort();
    }

    #[rstest]
    #[case(r#"{"status":"noop","created":false,"claim":null}"#, true)]
    #[case(
        r#"{"status":"pending","created":true,"claim":{"id":42,"quotaAmount":5}}"#,
        true
    )]
    #[case(r#"{"status":"noop","created":true,"claim":null}"#, false)]
    #[case(r#"{"status":"pending","created":true,"claim":null}"#, false)]
    #[case(r#"{"status":"pending","created":true,"claim":{"id":0}}"#, false)]
    fn quota_claim_submission_decodes_documented_outcomes(
        #[case] payload: &str,
        #[case] valid: bool,
    ) {
        let raw: Box<RawValue> = serde_json::from_str(payload).unwrap();
        let result = decode_quota_claim_submission(raw);
        assert_eq!(result.is_ok(), valid);
        if let Ok(submission) = result {
            assert_eq!(submission.data().get(), payload);
            if let Some(claim) = submission.claim {
                assert_eq!(claim.id, 42);
                assert_eq!(claim.data().get(), r#"{"id":42,"quotaAmount":5}"#);
            }
        }
    }

    #[rstest]
    fn wallet_quota_decodes_live_public_shape() {
        let quota: DeepXWalletQuota =
            serde_json::from_str(r#"{"pendingGrant":0,"remaining":9832,"unclaimed":157155}"#)
                .unwrap();
        assert_eq!(quota.pending_grant, 0);
        assert_eq!(quota.remaining, 9832);
        assert_eq!(quota.unclaimed, 157155);
    }

    #[tokio::test]
    async fn quota_claim_poll_recovers_from_rate_limit_and_waits_for_confirmation() {
        let requests = Arc::new(AtomicUsize::new(0));
        let router = Router::new()
            .route(
                QUOTA_CLAIM_PATH,
                get(|State(requests): State<Arc<AtomicUsize>>| async move {
                    match requests.fetch_add(1, Ordering::SeqCst) {
                        0 => (StatusCode::TOO_MANY_REQUESTS, Json(json!({"error": "rate limit"}))),
                        1 => (StatusCode::OK, Json(json!({"code":200,"msg":"success","fail":false,"data":{"id":42,"status":"pending"}}))),
                        _ => (StatusCode::OK, Json(json!({"code":200,"msg":"success","fail":false,"data":{"id":42,"status":"confirmed","txHash":"0xabc"}}))),
                    }
                }),
            )
            .with_state(requests.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let transport = DeepXRestTransport::new(Some(5), None).unwrap();
        let claim = transport
            .wait_quota_claim(
                &format!("http://{address}"),
                42,
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .await
            .unwrap();
        assert_eq!(claim.id, 42);
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn quota_claim_poll_reports_terminal_failure() {
        let router = Router::new().route(
            QUOTA_CLAIM_PATH,
            get(|| async {
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "fail": false,
                    "data": {"id": 42, "status": "failed", "lastError": "grant rejected"}
                }))
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let transport = DeepXRestTransport::new(Some(5), None).unwrap();
        let result = transport
            .wait_quota_claim(
                &format!("http://{address}"),
                42,
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .await;
        assert!(matches!(
            result,
            Err(DeepXRestError::QuotaClaimFailed { claim_id: 42, message })
                if message == "grant rejected"
        ));
        server.abort();
    }

    #[tokio::test]
    async fn quota_claim_poll_times_out_with_last_status() {
        let router = Router::new().route(
            QUOTA_CLAIM_PATH,
            get(|| async {
                Json(json!({
                    "code": 200,
                    "msg": "success",
                    "fail": false,
                    "data": {"id": 42, "status": "submitted"}
                }))
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let transport = DeepXRestTransport::new(Some(5), None).unwrap();
        let result = transport
            .wait_quota_claim(
                &format!("http://{address}"),
                42,
                Duration::from_millis(30),
                Duration::from_millis(2),
            )
            .await;
        assert!(matches!(
            result,
            Err(DeepXRestError::QuotaClaimTimeout { claim_id: 42, last_status })
                if last_status == "submitted"
        ));
        server.abort();
    }

    #[rstest]
    #[case(r#"{"id":42,"status":"submitted"}"#, "valid")]
    #[case(r#"{"id":43,"status":"submitted"}"#, "mismatch")]
    #[case(r#"{"status":"submitted"}"#, "decode")]
    #[case(r#"{"id":"42","status":"submitted"}"#, "decode")]
    fn quota_claim_binds_request_and_preserves_payload(
        #[case] payload: &str,
        #[case] outcome: &str,
    ) {
        let raw: Box<RawValue> = serde_json::from_str(payload).unwrap();
        let result = decode_quota_claim(raw, 42);
        match outcome {
            "valid" => {
                let claim = result.unwrap();
                assert_eq!(claim.id, 42);
                assert_eq!(claim.data().get(), payload);
                assert_eq!(claim.into_raw().get(), payload);
            }
            "mismatch" => assert!(matches!(
                result,
                Err(DeepXRestError::InvalidAccountResponse {
                    endpoint: "quota claim",
                    ..
                })
            )),
            "decode" => assert!(matches!(result, Err(DeepXRestError::Decode(_)))),
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn zero_quota_claim_id_fails_before_transport() {
        let client = DeepXRestTransport::new(Some(1), None).unwrap();
        assert!(matches!(
            client.get_quota_claim("http://127.0.0.1:1", 0).await,
            Err(DeepXRestError::InvalidRequest(message))
                if message == "quota-claim id must be greater than zero"
        ));
    }

    #[rstest]
    #[case("id")]
    #[case("owner")]
    #[case("type-filter")]
    #[case("buyer-missing")]
    #[case("buyer-filter")]
    #[case("quota")]
    #[case("block")]
    #[case("hash")]
    #[case("hash-type")]
    #[case("timestamp")]
    fn quota_history_rejects_invalid_record_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXQuotaHistoryRecord>> =
            serde_json::from_str(HISTORY_RESPONSE).unwrap();
        let mut record = response.data.items[0].clone();
        let mut request = history_request();
        match mutation {
            "id" => record.id.clear(),
            "owner" => {
                record.owner_address = "0x1111111111111111111111111111111111111111".to_string();
            }
            "type-filter" => request.history_type = Some(DeepXQuotaHistoryType::Free),
            "buyer-missing" => record.buyer_address = None,
            "buyer-filter" => {
                request.buyer_address =
                    Some("0x1111111111111111111111111111111111111111".to_string());
            }
            "quota" => record.quota = 0,
            "block" => record.block_number = 0,
            "hash" => record.tx_hash = "0x12".to_string(),
            "hash-type" => record.tx_hash_type.clear(),
            "timestamp" => record.created_at = "invalid".to_string(),
            _ => unreachable!(),
        }
        assert!(validate_quota_history_record(0, &record, &request).is_err());
    }

    #[rstest]
    #[case("duplicate-id")]
    #[case("duplicate-event")]
    #[case("ordering")]
    #[case("oversized")]
    fn quota_history_rejects_invalid_page_semantics(#[case] mutation: &str) {
        let mut response: Value = serde_json::from_str(HISTORY_RESPONSE).unwrap();
        let mut second = response["data"]["items"][0].clone();
        second["id"] = json!("purchase:other");
        second["blockNumber"] = json!(181_248_893);
        second["eventIndex"] = json!(48);
        second["createdAt"] = json!("2026-09-11T08:16:37.475Z");
        match mutation {
            "duplicate-id" => second["id"] = response["data"]["items"][0]["id"].clone(),
            "duplicate-event" => {
                second["blockNumber"] = response["data"]["items"][0]["blockNumber"].clone();
                second["eventIndex"] = response["data"]["items"][0]["eventIndex"].clone();
            }
            "ordering" => second["createdAt"] = json!("2026-09-11T08:16:39.475Z"),
            "oversized" => {}
            _ => unreachable!(),
        }
        response["data"]["items"]
            .as_array_mut()
            .unwrap()
            .push(second);
        let page: DeepXAccountPage<DeepXQuotaHistoryRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        let mut request = history_request();
        if mutation == "oversized" {
            request.limit = Some(1);
        }
        assert!(validate_quota_history_page(&page, &request).is_err());
    }

    #[rstest]
    #[case("duplicate-id")]
    #[case("duplicate-event")]
    #[case("ordering")]
    fn quota_history_continuity_rejects_cross_page_conflicts(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXQuotaHistoryRecord>> =
            serde_json::from_str(HISTORY_RESPONSE).unwrap();
        let first = response.data;
        let mut second = first.items[0].clone();
        second.id = "purchase:other".to_string();
        second.block_number += 1;
        second.event_index += 1;
        second.created_at = "2026-09-11T08:16:37.475Z".to_string();
        match mutation {
            "duplicate-id" => second.id = first.items[0].id.clone(),
            "duplicate-event" => {
                second.block_number = first.items[0].block_number;
                second.event_index = first.items[0].event_index;
            }
            "ordering" => second.created_at = "2026-09-12T00:00:00Z".to_string(),
            _ => unreachable!(),
        }
        let second = DeepXAccountPage {
            items: vec![second],
            next_cursor: None,
            has_next: false,
        };
        let request = history_request();
        let mut continuity = DeepXQuotaHistoryContinuity::default();
        continuity.observe_page(&first, &request).unwrap();
        assert!(matches!(
            continuity.observe_page(&second, &request),
            Err(DeepXRestError::InvalidHistoryResponse { .. }),
        ));
    }

    fn summary(response: &Value) -> DeepXQuotaSummary {
        serde_json::from_value(response["data"].clone()).unwrap()
    }

    #[rstest]
    fn quota_summary_accepts_absent_trade_times_for_empty_volume() {
        let mut response: Value = serde_json::from_str(RESPONSE).unwrap();
        for field in [
            "firstTradeAt",
            "firstTradeTsMs",
            "lastTradeAt",
            "lastTradeTsMs",
            "updatedAt",
        ] {
            response["data"].as_object_mut().unwrap().remove(field);
        }
        response["data"]["perpVolumeUsd"] = json!("0");
        response["data"]["totalVolumeUsd"] = json!("0");
        response["data"]["quotaEarned"] = json!(0);
        response["data"]["quotaPending"] = json!(0);
        validate_quota_summary(&summary(&response), WALLET).unwrap();
    }

    #[rstest]
    #[case("owner")]
    #[case("spot-volume")]
    #[case("perp-volume")]
    #[case("total-volume")]
    #[case("missing-first-ms")]
    #[case("missing-last")]
    #[case("first-mismatch")]
    #[case("inverted-time")]
    #[case("updated-at")]
    fn quota_summary_rejects_invalid_responses(#[case] mutation: &str) {
        let mut response: Value = serde_json::from_str(RESPONSE).unwrap();
        match mutation {
            "owner" => {
                response["data"]["owner"] = json!("0x1111111111111111111111111111111111111111");
            }
            "spot-volume" => {
                response["data"]["spotVolumeUsd"] = json!("-1");
            }
            "perp-volume" => {
                response["data"]["perpVolumeUsd"] = json!("-1");
            }
            "total-volume" => {
                response["data"]["totalVolumeUsd"] = json!("1");
            }
            "missing-first-ms" => {
                response["data"]
                    .as_object_mut()
                    .unwrap()
                    .remove("firstTradeTsMs");
            }
            "missing-last" => {
                response["data"]
                    .as_object_mut()
                    .unwrap()
                    .remove("lastTradeAt");
                response["data"]
                    .as_object_mut()
                    .unwrap()
                    .remove("lastTradeTsMs");
            }
            "first-mismatch" => {
                response["data"]["firstTradeTsMs"] = json!(1);
            }
            "inverted-time" => {
                response["data"]["firstTradeAt"] = response["data"]["lastTradeAt"].clone();
                response["data"]["firstTradeTsMs"] = response["data"]["lastTradeTsMs"].clone();
                response["data"]["lastTradeAt"] = json!("2026-09-15T04:04:15.646Z");
                response["data"]["lastTradeTsMs"] = json!(1_789_445_055_646_u64);
            }
            "updated-at" => {
                response["data"]["updatedAt"] = json!("invalid");
            }
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_quota_summary(&summary(&response), WALLET),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "quota summary",
                ..
            })
        ));
    }
}
