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

//! Typed and raw liquidation and ADL REST queries.

use std::collections::HashSet;

use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::value::RawValue;

use super::{
    DeepXAccountPage, DeepXRawAccountPage, DeepXRestError, DeepXRestTransport,
    decode_account_history_page, is_account_address,
};
use crate::{
    pagination::validate_cursor_page,
    query::{
        DeepXAccountSortOrder, DeepXLiquidationAdlRecordsRequest,
        DeepXLiquidationExecutionRecordsRequest, DeepXLiquidationRecordsRequest,
        DeepXPerpLiquidationPriceRequest, DeepXSpotAdlCandidatesRequest,
    },
    rest::models::{DeepXLiquidationRecord, DeepXPerpLiquidationPrice, DeepXSpotAdlCandidates},
};

const SPOT_ADL_CANDIDATES_PATH: &str = "/internal/v1/adl/spot/candidates";
const LIQUIDATION_ADL_RECORDS_PATH: &str = "/internal/v1/liquidation/adl-records";
const LIQUIDATION_KEEPER_STATE_PATH: &str = "/internal/v1/liquidation/keeper-state";
const LIQUIDATION_RECORDS_PATH: &str = "/internal/v1/account/subaccounts/liquidation-records";
const PERP_LIQUIDATION_PRICE_PATH: &str = "/internal/v1/account/perp/liquidate-price";

/// DeepX liquidation execution-record endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXLiquidationExecutionRoute {
    Ordinary,
    Cranker,
    TpSl,
}

impl DeepXLiquidationExecutionRoute {
    const fn path(self) -> &'static str {
        match self {
            Self::Ordinary => "/internal/v1/liquidation/records",
            Self::Cranker => "/internal/v1/liquidation/cranker-records",
            Self::TpSl => "/internal/v1/liquidation/tp-sl-records",
        }
    }

    /// Returns the endpoint name used in validation errors and pagination state.
    #[must_use]
    pub const fn endpoint(self) -> &'static str {
        match self {
            Self::Ordinary => "liquidation execution records",
            Self::Cranker => "liquidation cranker records",
            Self::TpSl => "liquidation TP/SL records",
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct KeeperStateQuery<'a> {
    subaccount: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_id: Option<&'a str>,
}

impl DeepXRestTransport {
    /// Returns the exact perpetual liquidation-price response.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-price response error.
    pub async fn get_perp_liquidation_price(
        &self,
        base_url: &str,
        request: &DeepXPerpLiquidationPriceRequest,
    ) -> Result<DeepXPerpLiquidationPrice, DeepXRestError> {
        let raw = self
            .get_perp_liquidation_price_raw(base_url, request)
            .await?;
        let price = serde_json::from_str(raw.get())?;
        validate_perp_liquidation_price(&price, request)?;
        Ok(price)
    }

    /// Returns typed Spot ADL candidates with exact raw integer amounts.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-candidate response error.
    pub async fn get_spot_adl_candidates(
        &self,
        base_url: &str,
        request: &DeepXSpotAdlCandidatesRequest,
    ) -> Result<DeepXSpotAdlCandidates, DeepXRestError> {
        let raw = self.get_spot_adl_candidates_raw(base_url, request).await?;
        let candidates = serde_json::from_str(raw.get())?;
        validate_spot_adl_candidates(&candidates, request)?;
        Ok(candidates)
    }

    /// Returns one validated account liquidation-record page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, pagination, decoding, or invalid-record error.
    pub async fn get_liquidation_records(
        &self,
        base_url: &str,
        request: &DeepXLiquidationRecordsRequest,
    ) -> Result<DeepXAccountPage<DeepXLiquidationRecord>, DeepXRestError> {
        let page = decode_account_history_page(
            self.get_liquidation_records_raw(base_url, request).await?,
            "liquidation records",
            request.page_size,
        )?;
        validate_liquidation_records_page(&page, request)?;
        Ok(page)
    }

    /// Returns the uninterpreted perpetual liquidation-price response.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_liquidation_price_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpLiquidationPriceRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_LIQUIDATION_PRICE_PATH, &request.as_query())
            .await
    }

    /// Returns the uninterpreted Spot ADL candidate response.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_adl_candidates_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotAdlCandidatesRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_ADL_CANDIDATES_PATH, &request.as_query())
            .await
    }

    /// Returns one uninterpreted page of ADL records.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_liquidation_adl_records_raw(
        &self,
        base_url: &str,
        request: &DeepXLiquidationAdlRecordsRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            LIQUIDATION_ADL_RECORDS_PATH,
            &request.as_query(),
            "liquidation ADL records",
        )
        .await
    }

    /// Returns one uninterpreted execution-record page from the selected route.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, page-size, or pagination error.
    pub async fn get_liquidation_execution_records_raw(
        &self,
        base_url: &str,
        route: DeepXLiquidationExecutionRoute,
        request: &DeepXLiquidationExecutionRecordsRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_api_data_with_query(base_url, route.path(), &request.as_query())
            .await?;
        validate_execution_page(&page, route, request.page_size.unwrap_or(20))?;
        Ok(page)
    }

    /// Returns the uninterpreted liquidation keeper state for one subaccount.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_liquidation_keeper_state_raw(
        &self,
        base_url: &str,
        subaccount: &str,
        chain_id: Option<&str>,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        if !is_account_address(subaccount) {
            return Err(DeepXRestError::InvalidRequest(
                "liquidation keeper state requires a 20-byte hex subaccount address".to_string(),
            ));
        }
        if chain_id.is_some_and(|value| value.trim().is_empty()) {
            return Err(DeepXRestError::InvalidRequest(
                "liquidation keeper state chain_id must not be empty".to_string(),
            ));
        }
        self.get_api_data_with_query(
            base_url,
            LIQUIDATION_KEEPER_STATE_PATH,
            &KeeperStateQuery {
                subaccount,
                chain_id,
            },
        )
        .await
    }

    /// Returns one uninterpreted page of account liquidation records.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_liquidation_records_raw(
        &self,
        base_url: &str,
        request: &DeepXLiquidationRecordsRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            LIQUIDATION_RECORDS_PATH,
            &request.as_query(),
            "liquidation records",
        )
        .await
    }
}

fn validate_liquidation_records_page(
    page: &DeepXAccountPage<DeepXLiquidationRecord>,
    request: &DeepXLiquidationRecordsRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "liquidation records";
    if request
        .page_size
        .is_some_and(|limit| page.items.len() > limit as usize)
    {
        return Err(DeepXRestError::InvalidHistoryResponse {
            endpoint,
            message: "page exceeds requested page size".to_string(),
        });
    }
    let mut ids = HashSet::new();
    let mut previous = None;
    for (index, record) in page.items.iter().enumerate() {
        validate_liquidation_record(index, record, request, &mut previous)?;
        if !ids.insert(record.id) {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint,
                message: format!("item {index} repeats record ID"),
            });
        }
    }
    Ok(())
}

/// Tracks liquidation record order and identifiers across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXLiquidationRecordContinuity {
    ids: HashSet<u64>,
    previous: Option<jiff::Timestamp>,
}

impl DeepXLiquidationRecordContinuity {
    /// Checks one page against all liquidation records previously observed.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign record, repeated ID, or inconsistent time order.
    pub fn observe_page(
        &mut self,
        page: &DeepXAccountPage<DeepXLiquidationRecord>,
        request: &DeepXLiquidationRecordsRequest,
    ) -> Result<(), DeepXRestError> {
        let endpoint = "liquidation records";
        for (index, record) in page.items.iter().enumerate() {
            validate_liquidation_record(index, record, request, &mut self.previous)?;
            if !self.ids.insert(record.id) {
                return Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint,
                    message: format!("item {index} repeats record ID"),
                });
            }
        }
        Ok(())
    }
}

fn validate_liquidation_record(
    index: usize,
    record: &DeepXLiquidationRecord,
    request: &DeepXLiquidationRecordsRequest,
    previous: &mut Option<jiff::Timestamp>,
) -> Result<(), DeepXRestError> {
    let endpoint = "liquidation records";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if record.id == 0 || record.height == 0 {
        return Err(invalid(format!(
            "item {index} has an invalid record ID or block height"
        )));
    }
    if !is_account_address(&record.target_account)
        || !is_account_address(&record.liquidator)
        || request
            .subaccount
            .as_deref()
            .is_some_and(|expected| !record.target_account.eq_ignore_ascii_case(expected))
    {
        return Err(invalid(format!(
            "item {index} has an invalid or unexpected account identity"
        )));
    }
    if !request.liquidation_types.is_empty()
        && !request.liquidation_types.contains(&record.liquidation_type)
    {
        return Err(invalid(format!(
            "item {index} does not match the requested liquidation types"
        )));
    }
    if record.market_index == Some(0) {
        return Err(invalid(format!("item {index} has an invalid market index")));
    }
    if record.liquidation_type.is_bankruptcy() && !record.bankrupt {
        return Err(invalid(format!(
            "item {index} has an inconsistent bankruptcy classification"
        )));
    }
    if record.tx_hash.as_deref().is_some_and(|value| {
        !value.strip_prefix("0x").is_some_and(|hash| {
            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    }) {
        return Err(invalid(format!(
            "item {index} has an invalid transaction hash"
        )));
    }
    let detail: serde_json::Value =
        serde_json::from_str(&record.liquidation_detail).map_err(|e| {
            invalid(format!(
                "item {index} has invalid liquidation detail JSON: {e}"
            ))
        })?;
    let detail = detail
        .as_object()
        .ok_or_else(|| invalid(format!("item {index} liquidation detail is not an object")))?;
    if detail.len() != 1 || !detail.contains_key(record.liquidation_type.detail_key()) {
        return Err(invalid(format!(
            "item {index} liquidation detail does not match its type"
        )));
    }
    if serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(
        &record.canceled_order_ids,
    )
    .is_err()
    {
        return Err(invalid(format!(
            "item {index} has invalid canceled-order JSON"
        )));
    }
    let created = record
        .created_at
        .parse::<jiff::Timestamp>()
        .map_err(|e| invalid(format!("item {index} has invalid createdAt: {e}")))?;
    let out_of_order = previous
        .as_ref()
        .is_some_and(|previous| match request.sort {
            DeepXAccountSortOrder::Ascending => created < *previous,
            DeepXAccountSortOrder::Descending => created > *previous,
        });
    if out_of_order {
        return Err(invalid(format!(
            "item {index} violates the requested time order"
        )));
    }
    *previous = Some(created);
    Ok(())
}

fn validate_execution_page(
    page: &DeepXRawAccountPage,
    route: DeepXLiquidationExecutionRoute,
    page_size: u32,
) -> Result<(), DeepXRestError> {
    if page.items.len() > page_size as usize {
        return Err(DeepXRestError::InvalidHistoryResponse {
            endpoint: route.endpoint(),
            message: "page exceeds requested page size".to_string(),
        });
    }
    validate_cursor_page(route.endpoint(), page.has_next, page.next_cursor.as_deref())
}

fn validate_perp_liquidation_price(
    price: &DeepXPerpLiquidationPrice,
    request: &DeepXPerpLiquidationPriceRequest,
) -> Result<(), DeepXRestError> {
    let invalid = |message| DeepXRestError::InvalidAccountResponse {
        endpoint: "perp liquidation price",
        message,
    };
    if !is_account_address(&price.address)
        || !price.address.eq_ignore_ascii_case(&request.subaccount)
    {
        return Err(invalid(
            "response belongs to another or invalid subaccount".to_string(),
        ));
    }
    if price.market_id == 0
        || request
            .market_id
            .is_some_and(|market_id| price.market_id != market_id)
        || price.market_name.trim().is_empty()
        || request
            .market_name
            .as_deref()
            .is_some_and(|name| price.market_name != name)
    {
        return Err(invalid(
            "response has an unexpected market identity".to_string(),
        ));
    }
    if price
        .liquidate_price
        .is_some_and(|value| value <= Decimal::ZERO)
    {
        return Err(invalid(
            "response has a nonpositive liquidation price".to_string(),
        ));
    }
    Ok(())
}

fn validate_spot_adl_candidates(
    response: &DeepXSpotAdlCandidates,
    request: &DeepXSpotAdlCandidatesRequest,
) -> Result<(), DeepXRestError> {
    let invalid = |message: &str| DeepXRestError::InvalidHistoryResponse {
        endpoint: "spot ADL candidates",
        message: message.to_string(),
    };
    if !response
        .target_subaccount
        .eq_ignore_ascii_case(&request.target_subaccount)
        || response.asset != request.asset
        || response.liability != request.liability
        || normalize_raw_integer(&response.min_cover_amount)
            != normalize_raw_integer(&request.min_cover_amount)
    {
        return Err(invalid(
            "response does not match the requested target or assets",
        ));
    }
    if !is_account_address(&response.target_subaccount) {
        return Err(invalid("response target is not a 20-byte hex address"));
    }
    for value in [
        response.min_cover_amount.as_str(),
        response.covered_amount.as_str(),
    ] {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid("response contains a non-integer raw amount"));
        }
    }
    let mut candidates = HashSet::new();
    for candidate in &response.candidates {
        if !is_account_address(&candidate.subaccount) {
            return Err(invalid("candidate has an invalid subaccount address"));
        }
        if !candidates.insert(candidate.subaccount.to_ascii_lowercase()) {
            return Err(invalid(
                "response contains a duplicate candidate subaccount",
            ));
        }
        for value in [&candidate.available_amount, &candidate.borrow_amount] {
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid("candidate contains a non-integer raw amount"));
            }
        }
    }
    Ok(())
}

fn normalize_raw_integer(value: &str) -> &str {
    let normalized = value.trim_start_matches('0');
    if normalized.is_empty() {
        "0"
    } else {
        normalized
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::rest::{DeepXApiResponse, models::DeepXSpotAdlCandidate};

    const PERP_LIQUIDATION_PRICE_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_liquidation_price_account_market_3.json");
    const LIQUIDATION_RECORDS_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_liquidation_records_page_1.json");

    fn liquidation_records_request() -> DeepXLiquidationRecordsRequest {
        DeepXLiquidationRecordsRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            liquidation_types: Vec::new(),
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(5),
        }
    }

    #[rstest]
    #[case("zero-id")]
    #[case("zero-height")]
    #[case("invalid-target")]
    #[case("foreign-target")]
    #[case("filter-mismatch")]
    #[case("zero-market")]
    #[case("bankruptcy")]
    #[case("invalid-tx-hash")]
    #[case("malformed-detail")]
    #[case("mismatched-detail")]
    #[case("malformed-canceled-orders")]
    #[case("invalid-timestamp")]
    #[case("wrong-order")]
    fn liquidation_records_reject_invalid_response_semantics(#[case] mutation: &str) {
        let response: serde_json::Value =
            serde_json::from_str(LIQUIDATION_RECORDS_RESPONSE).unwrap();
        let mut record: DeepXLiquidationRecord =
            serde_json::from_value(response["data"]["items"][0].clone()).unwrap();
        let mut request = liquidation_records_request();
        let mut previous = None;
        match mutation {
            "zero-id" => record.id = 0,
            "zero-height" => record.height = 0,
            "invalid-target" => record.target_account = "invalid".to_string(),
            "foreign-target" => {
                request.subaccount = Some("0x1111111111111111111111111111111111111111".to_string());
                request.wallet = None;
            }
            "filter-mismatch" => {
                request.liquidation_types = vec![crate::query::DeepXLiquidationType::LiquidateSpot];
            }
            "zero-market" => record.market_index = Some(0),
            "bankruptcy" => {
                record.liquidation_type = crate::query::DeepXLiquidationType::PerpBankruptcy;
                record.liquidation_detail = r#"{"perpBankruptcy":{}}"#.to_string();
            }
            "invalid-tx-hash" => record.tx_hash = Some("0x1234".to_string()),
            "malformed-detail" => record.liquidation_detail = "{".to_string(),
            "mismatched-detail" => {
                record.liquidation_detail = r#"{"liquidateSpot":{}}"#.to_string();
            }
            "malformed-canceled-orders" => record.canceled_order_ids = "[]".to_string(),
            "invalid-timestamp" => record.created_at = "invalid".to_string(),
            "wrong-order" => previous = Some("2026-05-27T15:00:00Z".parse().unwrap()),
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_liquidation_record(0, &record, &request, &mut previous),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "liquidation records",
                ..
            })
        ));
    }

    #[rstest]
    #[case("duplicate")]
    #[case("oversized")]
    fn liquidation_page_rejects_duplicate_and_oversized_records(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXLiquidationRecord>> =
            serde_json::from_str(LIQUIDATION_RECORDS_RESPONSE).unwrap();
        let mut page = response.data;
        let mut request = liquidation_records_request();
        validate_liquidation_records_page(&page, &request).unwrap();
        match mutation {
            "duplicate" => {
                page.items[1].id = page.items[0].id;
            }
            "oversized" => request.page_size = Some(1),
            _ => unreachable!(),
        }
        let error = validate_liquidation_records_page(&page, &request).unwrap_err();
        assert!(matches!(
            error,
            DeepXRestError::InvalidHistoryResponse {
                endpoint: "liquidation records",
                message,
            } if message.contains(match mutation {
                "duplicate" => "repeats record ID",
                "oversized" => "page exceeds requested page size",
                _ => unreachable!(),
            })
        ));
    }

    #[rstest]
    #[case("duplicate")]
    #[case("ordering")]
    fn liquidation_continuity_rejects_cross_page_conflicts(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXAccountPage<DeepXLiquidationRecord>> =
            serde_json::from_str(LIQUIDATION_RECORDS_RESPONSE).unwrap();
        let mut first = response.data;
        let later = first.items.split_off(1);
        let mut second = DeepXAccountPage {
            items: later,
            next_cursor: None,
            has_next: false,
        };
        let request = liquidation_records_request();
        let mut continuity = DeepXLiquidationRecordContinuity::default();
        continuity.observe_page(&first, &request).unwrap();
        continuity.observe_page(&second, &request).unwrap();
        match mutation {
            "duplicate" => second.items[0].id = first.items[0].id,
            "ordering" => second.items[0].created_at = "2026-09-12T00:00:00Z".to_string(),
            _ => unreachable!(),
        }
        let mut continuity = DeepXLiquidationRecordContinuity::default();
        continuity.observe_page(&first, &request).unwrap();
        assert!(matches!(
            continuity.observe_page(&second, &request),
            Err(DeepXRestError::InvalidHistoryResponse { .. }),
        ));
    }

    #[rstest]
    fn execution_routes_map_to_distinct_paths_and_names() {
        let routes = [
            DeepXLiquidationExecutionRoute::Ordinary,
            DeepXLiquidationExecutionRoute::Cranker,
            DeepXLiquidationExecutionRoute::TpSl,
        ];
        let paths = routes.map(DeepXLiquidationExecutionRoute::path);
        assert_eq!(paths.len(), std::collections::HashSet::from(paths).len());
        assert_eq!(routes[0].endpoint(), "liquidation execution records");
        assert_eq!(routes[1].endpoint(), "liquidation cranker records");
        assert_eq!(routes[2].endpoint(), "liquidation TP/SL records");
    }

    #[rstest]
    fn execution_page_checks_size_before_cursor() {
        let page: DeepXRawAccountPage = serde_json::from_value(json!({
            "items": [{"id": 1}, {"id": 2}], "hasNext": true, "nextCursor": null,
        }))
        .unwrap();
        assert!(matches!(
            validate_execution_page(&page, DeepXLiquidationExecutionRoute::Ordinary, 1),
            Err(DeepXRestError::InvalidHistoryResponse { .. }),
        ));
    }

    #[rstest]
    #[case("address")]
    #[case("market-id")]
    #[case("market-name")]
    #[case("price")]
    fn liquidation_price_rejects_invalid_responses(#[case] mutation: &str) {
        let mut response: serde_json::Value =
            serde_json::from_str(PERP_LIQUIDATION_PRICE_RESPONSE).unwrap();
        match mutation {
            "address" => {
                response["data"]["address"] = json!("0x1111111111111111111111111111111111111111");
            }
            "market-id" => response["data"]["marketId"] = json!(4),
            "market-name" => response["data"]["marketName"] = json!(""),
            "price" => response["data"]["liquidatePrice"] = json!(0),
            _ => unreachable!(),
        }
        let price: DeepXPerpLiquidationPrice =
            serde_json::from_value(response["data"].clone()).unwrap();
        let request = DeepXPerpLiquidationPriceRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_name: None,
            market_id: Some(3),
        };

        assert!(matches!(
            validate_perp_liquidation_price(&price, &request),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "perp liquidation price",
                ..
            })
        ));
    }

    #[rstest]
    fn liquidation_price_accepts_captured_response() {
        let response: DeepXApiResponse<DeepXPerpLiquidationPrice> =
            serde_json::from_str(PERP_LIQUIDATION_PRICE_RESPONSE).unwrap();
        let request = DeepXPerpLiquidationPriceRequest {
            subaccount: response.data.address.clone(),
            market_name: None,
            market_id: Some(response.data.market_id),
        };
        validate_perp_liquidation_price(&response.data, &request).unwrap();
    }

    #[rstest]
    #[case("target")]
    #[case("asset")]
    #[case("amount")]
    #[case("covered")]
    #[case("candidate-address")]
    #[case("duplicate")]
    #[case("candidate-amount")]
    fn spot_adl_candidates_reject_invalid_responses(#[case] mutation: &str) {
        let request = DeepXSpotAdlCandidatesRequest {
            target_subaccount: "0x1111111111111111111111111111111111111111".to_string(),
            asset: "USDC".to_string(),
            liability: "ETH".to_string(),
            min_cover_amount: "10".to_string(),
            limit: None,
        };
        let candidate = DeepXSpotAdlCandidate {
            subaccount: "0x2222222222222222222222222222222222222222".to_string(),
            available_amount: "20".to_string(),
            borrow_amount: "5".to_string(),
        };
        let mut response = DeepXSpotAdlCandidates {
            target_subaccount: request.target_subaccount.clone(),
            asset: request.asset.clone(),
            liability: request.liability.clone(),
            min_cover_amount: "0010".to_string(),
            covered_amount: "10".to_string(),
            candidates: vec![candidate],
        };
        validate_spot_adl_candidates(&response, &request).unwrap();
        match mutation {
            "target" => response.target_subaccount = "invalid".to_string(),
            "asset" => response.asset = "BTC".to_string(),
            "amount" => response.min_cover_amount = "11".to_string(),
            "covered" => response.covered_amount = "-1".to_string(),
            "candidate-address" => response.candidates[0].subaccount = "invalid".to_string(),
            "duplicate" => response.candidates.push(response.candidates[0].clone()),
            "candidate-amount" => response.candidates[0].borrow_amount = "1.5".to_string(),
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_spot_adl_candidates(&response, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot ADL candidates",
                ..
            })
        ));
    }
}
