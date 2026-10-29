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

//! Complete cursor pagination for the global subaccount directory.

use std::future::Future;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::DeepXAllSubaccountsRequest,
    rest::{
        DeepXAccountPage, DeepXRawAccountPage, DeepXRestError, DeepXRestTransport,
        DeepXSubaccountDirectoryContinuity, models::DeepXSubaccountDirectoryRecord,
    },
};

/// Collects raw subaccount-directory pages without decoding their item payloads.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_all_subaccount_pages_raw<E, F, Fut>(
    request: &DeepXAllSubaccountsRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXAllSubaccountsRequest) -> Fut,
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
                "all subaccounts",
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

/// Collects validated subaccount-directory pages with cross-page continuity.
///
/// # Errors
///
/// Returns a request, page-read, pagination, or directory-continuity error.
pub async fn collect_all_subaccount_pages<E, F, Fut>(
    request: &DeepXAllSubaccountsRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXSubaccountDirectoryRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXAllSubaccountsRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXSubaccountDirectoryRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXSubaccountDirectoryContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        let decision = pagination
            .observe_response_page(
                "all subaccounts",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        continuity.observe_page(&page).map_err(E::from)?;
        pages.push(page);
        match decision {
            PaginationDecision::Complete => return Ok(pages),
            PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
        }
    }
}

impl DeepXRestTransport {
    /// Returns complete raw subaccount-directory pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_all_subaccount_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXAllSubaccountsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_all_subaccount_pages_raw(request, max_pages, |page_request| async move {
            self.get_all_subaccounts_raw(base_url, &page_request).await
        })
        .await
    }

    /// Returns complete validated subaccount-directory pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_all_subaccount_pages(
        &self,
        base_url: &str,
        request: &DeepXAllSubaccountsRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXSubaccountDirectoryRecord>>, DeepXRestError> {
        collect_all_subaccount_pages(request, max_pages, |page_request| async move {
            self.get_all_subaccounts(base_url, &page_request).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::Value;

    use super::*;

    const RESPONSE: &str = include_str!("../../test_data/http/testnet/all_subaccounts_page_1.json");

    #[rstest]
    #[tokio::test]
    async fn rejects_repeated_subaccount_across_pages() {
        let response: Value = serde_json::from_str(RESPONSE).unwrap();
        let mut first: DeepXAccountPage<DeepXSubaccountDirectoryRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        first.items.truncate(1);
        first.has_next = true;
        first.next_cursor = Some("next-page".to_string());
        let mut second = first.clone();
        second.has_next = false;
        second.next_cursor = None;
        let mut pages = [first, second].into_iter();
        let request = DeepXAllSubaccountsRequest {
            cursor: None,
            page_size: Some(1),
        };
        assert!(matches!(
            collect_all_subaccount_pages(&request, 2, |_| {
                std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
            })
            .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "all subaccounts",
                ..
            })
        ));
    }

    #[rstest]
    #[tokio::test]
    async fn raw_pages_preserve_payloads_and_cursor() {
        let first: DeepXRawAccountPage = serde_json::from_str(
            r#"{"items":[{"id":9007199254740993}],"hasNext":true,"nextCursor":"next-page"}"#,
        )
        .unwrap();
        let second: DeepXRawAccountPage =
            serde_json::from_str(r#"{"items":[],"hasNext":false,"nextCursor":null}"#).unwrap();
        let mut pages = [first, second].into_iter();
        let mut cursors = Vec::new();
        let request = DeepXAllSubaccountsRequest {
            cursor: None,
            page_size: Some(1),
        };
        let result = collect_all_subaccount_pages_raw(&request, 2, |page_request| {
            cursors.push(page_request.cursor);
            std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
        })
        .await
        .unwrap();
        assert_eq!(cursors, [None, Some("next-page".to_string())]);
        assert_eq!(result[0].items[0].get(), r#"{"id":9007199254740993}"#);
    }
}
