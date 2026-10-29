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

//! Complete cursor pagination for liquidation records.

use std::future::Future;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::{
        DeepXLiquidationAdlRecordsRequest, DeepXLiquidationExecutionRecordsRequest,
        DeepXLiquidationRecordsRequest,
    },
    rest::{
        DeepXAccountPage, DeepXLiquidationExecutionRoute, DeepXLiquidationRecordContinuity,
        DeepXRawAccountPage, DeepXRestError, DeepXRestTransport, models::DeepXLiquidationRecord,
    },
};

/// Collects raw ADL record pages within a page budget.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_liquidation_adl_record_pages_raw<E, F, Fut>(
    request: &DeepXLiquidationAdlRecordsRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXLiquidationAdlRecordsRequest) -> Fut,
    Fut: Future<Output = Result<DeepXRawAccountPage, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        let decision = pagination
            .observe_response_page(
                "liquidation ADL records",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        pages.push(page);
        match decision {
            PaginationDecision::Complete => return Ok(pages),
            PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
        }
    }
}

/// Collects raw liquidation execution pages from the specified route.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_liquidation_execution_record_pages_raw<E, F, Fut>(
    route: DeepXLiquidationExecutionRoute,
    request: &DeepXLiquidationExecutionRecordsRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXLiquidationExecutionRecordsRequest) -> Fut,
    Fut: Future<Output = Result<DeepXRawAccountPage, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        let decision = pagination
            .observe_response_page(
                route.endpoint(),
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        pages.push(page);
        match decision {
            PaginationDecision::Complete => return Ok(pages),
            PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
        }
    }
}

/// Collects raw account liquidation-record pages within a page budget.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_liquidation_record_pages_raw<E, F, Fut>(
    request: &DeepXLiquidationRecordsRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXLiquidationRecordsRequest) -> Fut,
    Fut: Future<Output = Result<DeepXRawAccountPage, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        let decision = pagination
            .observe_response_page(
                "liquidation records",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        pages.push(page);
        match decision {
            PaginationDecision::Complete => return Ok(pages),
            PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
        }
    }
}

/// Collects validated account liquidation records with cross-page continuity.
///
/// # Errors
///
/// Returns a request, page-read, pagination, or record-continuity error.
pub async fn collect_liquidation_record_pages<E, F, Fut>(
    request: &DeepXLiquidationRecordsRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXLiquidationRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXLiquidationRecordsRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXLiquidationRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXLiquidationRecordContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        let decision = pagination
            .observe_response_page(
                "liquidation records",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        continuity.observe_page(&page, request).map_err(E::from)?;
        pages.push(page);
        match decision {
            PaginationDecision::Complete => return Ok(pages),
            PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
        }
    }
}

impl DeepXRestTransport {
    /// Returns complete raw ADL record pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_liquidation_adl_record_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXLiquidationAdlRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_liquidation_adl_record_pages_raw(request, max_pages, |page_request| async move {
            self.get_liquidation_adl_records_raw(base_url, &page_request)
                .await
        })
        .await
    }

    /// Returns complete raw execution-record pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_liquidation_execution_record_pages_raw(
        &self,
        base_url: &str,
        route: DeepXLiquidationExecutionRoute,
        request: &DeepXLiquidationExecutionRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_liquidation_execution_record_pages_raw(
            route,
            request,
            max_pages,
            |page_request| async move {
                self.get_liquidation_execution_records_raw(base_url, route, &page_request)
                    .await
            },
        )
        .await
    }

    /// Returns complete raw account liquidation-record pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_liquidation_record_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXLiquidationRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_liquidation_record_pages_raw(request, max_pages, |page_request| async move {
            self.get_liquidation_records_raw(base_url, &page_request)
                .await
        })
        .await
    }

    /// Returns complete validated account liquidation-record pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_liquidation_record_pages(
        &self,
        base_url: &str,
        request: &DeepXLiquidationRecordsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXLiquidationRecord>>, DeepXRestError> {
        collect_liquidation_record_pages(request, max_pages, |page_request| async move {
            self.get_liquidation_records(base_url, &page_request).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::Value;

    use super::*;
    use crate::query::DeepXAccountSortOrder;

    const RESPONSE: &str =
        include_str!("../../test_data/http/testnet/wallet_liquidation_records_page_1.json");

    fn liquidation_request() -> DeepXLiquidationRecordsRequest {
        DeepXLiquidationRecordsRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            liquidation_types: Vec::new(),
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(1),
        }
    }

    fn execution_request() -> DeepXLiquidationExecutionRecordsRequest {
        DeepXLiquidationExecutionRecordsRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            status: None,
            reason: None,
            tx_hash: None,
            extrinsic_hash: None,
            trigger_block_from: None,
            trigger_block_to: None,
            start_ms: None,
            end_ms: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(1),
        }
    }

    #[rstest]
    #[tokio::test]
    async fn typed_pages_reject_repeated_liquidation_identity() {
        let response: Value = serde_json::from_str(RESPONSE).unwrap();
        let mut first: DeepXAccountPage<DeepXLiquidationRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        first.items.truncate(1);
        first.has_next = true;
        first.next_cursor = Some("next-page".to_string());
        let mut second = first.clone();
        second.has_next = false;
        second.next_cursor = None;
        let mut pages = [first, second].into_iter();
        let mut cursors = Vec::new();
        let result = collect_liquidation_record_pages(&liquidation_request(), 2, |request| {
            cursors.push(request.cursor);
            std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
        })
        .await;
        assert_eq!(cursors, [None, Some("next-page".to_string())]);
        assert!(matches!(
            result,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "liquidation records",
                ..
            })
        ));
    }

    #[rstest]
    #[case(DeepXLiquidationExecutionRoute::Ordinary)]
    #[case(DeepXLiquidationExecutionRoute::Cranker)]
    #[case(DeepXLiquidationExecutionRoute::TpSl)]
    #[tokio::test]
    async fn execution_routes_follow_cursor_and_keep_raw_items(
        #[case] route: DeepXLiquidationExecutionRoute,
    ) {
        let first: DeepXRawAccountPage = serde_json::from_str(
            r#"{"items":[{"rawAmount":9007199254740993}],"hasNext":true,"nextCursor":"next-page"}"#,
        )
        .unwrap();
        let second: DeepXRawAccountPage =
            serde_json::from_str(r#"{"items":[],"hasNext":false,"nextCursor":null}"#).unwrap();
        let mut pages = [first, second].into_iter();
        let mut cursors = Vec::new();
        let result = collect_liquidation_execution_record_pages_raw(
            route,
            &execution_request(),
            2,
            |request| {
                cursors.push(request.cursor);
                std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
            },
        )
        .await
        .unwrap();
        assert_eq!(cursors, [None, Some("next-page".to_string())]);
        assert_eq!(
            result[0].items[0].get(),
            r#"{"rawAmount":9007199254740993}"#
        );
    }
}
