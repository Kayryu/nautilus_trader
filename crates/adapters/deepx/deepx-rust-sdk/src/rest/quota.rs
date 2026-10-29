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

//! Typed and raw wallet quota REST queries.

use std::collections::HashSet;

use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::value::RawValue;

use super::{
    DeepXAccountPage, DeepXRawAccountPage, DeepXRestError, DeepXRestTransport,
    decode_account_history_page, is_account_address,
};
use crate::{
    query::{DeepXQuotaHistoryRequest, DeepXQuotaHistoryType},
    rest::models::{
        DeepXQuotaClaim, DeepXQuotaClaimIdentity, DeepXQuotaHistoryRecord, DeepXQuotaSummary,
    },
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

impl DeepXRestTransport {
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
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;
    use crate::rest::DeepXApiResponse;

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
