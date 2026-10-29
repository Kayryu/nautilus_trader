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

//! Complete cursor pagination for perpetual position lifecycles.

use std::future::Future;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::DeepXPerpPositionsRequest,
    rest::{
        DeepXAccountPage, DeepXPerpPositionContinuity, DeepXRawAccountPage, DeepXRestError,
        DeepXRestTransport, models::DeepXPerpPositionRecord,
    },
};

/// Collects raw perpetual position pages without decoding their item payloads.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_perp_position_pages_raw<E, F, Fut>(
    request: &DeepXPerpPositionsRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpPositionsRequest) -> Fut,
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
                "perp positions",
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

/// Collects validated perpetual position pages with cross-page position identity checks.
///
/// # Errors
///
/// Returns a request, page-read, position-continuity, or pagination error.
pub async fn collect_perp_position_pages<E, F, Fut>(
    request: &DeepXPerpPositionsRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXPerpPositionRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpPositionsRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXPerpPositionRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXPerpPositionContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        continuity.observe_page(&page).map_err(E::from)?;
        let decision = pagination
            .observe_response_page(
                "perp positions",
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

impl DeepXRestTransport {
    /// Returns complete raw perpetual position pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_position_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpPositionsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_perp_position_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_positions_raw(base_url, &page_request).await
        })
        .await
    }

    /// Returns complete validated perpetual position pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_position_pages(
        &self,
        base_url: &str,
        request: &DeepXPerpPositionsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpPositionRecord>>, DeepXRestError> {
        collect_perp_position_pages(request, max_pages, |page_request| async move {
            self.get_perp_positions(base_url, &page_request).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::Value;

    use super::*;

    const RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_positions_account_market_3.json");

    fn request() -> DeepXPerpPositionsRequest {
        DeepXPerpPositionsRequest {
            subaccount: "0x1111111111111111111111111111111111111111".to_string(),
            market_id: Some(3),
            only_closed: None,
            cursor: None,
            page_size: Some(1),
        }
    }

    #[rstest]
    #[tokio::test]
    async fn typed_pages_reject_repeated_position_ids() {
        let response: Value = serde_json::from_str(RESPONSE).unwrap();
        let mut first: DeepXAccountPage<DeepXPerpPositionRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        first.items.truncate(1);
        first.has_next = true;
        first.next_cursor = Some("next-page".to_string());
        let mut second = first.clone();
        second.has_next = false;
        second.next_cursor = None;
        let mut pages = [first, second].into_iter();
        let mut cursors = Vec::new();
        let result = collect_perp_position_pages(&request(), 2, |page_request| {
            cursors.push(page_request.cursor);
            std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
        })
        .await;
        assert_eq!(cursors, [None, Some("next-page".to_string())]);
        assert!(matches!(
            result,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp positions",
                ..
            })
        ));
    }

    #[rstest]
    #[tokio::test]
    async fn raw_pages_keep_exact_payloads() {
        let first: DeepXRawAccountPage = serde_json::from_str(
            r#"{"items":[{"id":9007199254740993,"amount":0.004000000000000001}],"hasNext":true,"nextCursor":"next-page"}"#,
        )
        .unwrap();
        let second: DeepXRawAccountPage =
            serde_json::from_str(r#"{"items":[],"hasNext":false,"nextCursor":null}"#).unwrap();
        let mut pages = [first, second].into_iter();
        let result = collect_perp_position_pages_raw(&request(), 2, |_| {
            std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
        })
        .await
        .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].items[0].get(),
            r#"{"id":9007199254740993,"amount":0.004000000000000001}"#
        );
    }
}
