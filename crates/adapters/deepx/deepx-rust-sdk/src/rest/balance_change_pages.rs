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

//! Complete cursor pagination for account balance changes.

use std::future::Future;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::DeepXBalanceChangesRequest,
    rest::{
        DeepXAccountPage, DeepXBalanceChangeContinuity, DeepXRawAccountPage, DeepXRestError,
        DeepXRestTransport, models::DeepXBalanceChangeRecord,
    },
};

/// Collects raw balance-change pages without decoding their item payloads.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_balance_change_pages_raw<E, F, Fut>(
    request: &DeepXBalanceChangesRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXBalanceChangesRequest) -> Fut,
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
                "balance changes",
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

/// Collects validated balance-change pages with cross-page record identity checks.
///
/// # Errors
///
/// Returns a request, page-read, pagination, or record-continuity error.
pub async fn collect_balance_change_pages<E, F, Fut>(
    request: &DeepXBalanceChangesRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXBalanceChangeRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXBalanceChangesRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXBalanceChangeRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXBalanceChangeContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        let decision = pagination
            .observe_response_page(
                "balance changes",
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
    /// Returns complete raw balance-change pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_balance_change_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXBalanceChangesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_balance_change_pages_raw(request, max_pages, |page_request| async move {
            self.get_balance_changes_raw(base_url, &page_request).await
        })
        .await
    }

    /// Returns complete validated balance-change pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_balance_change_pages(
        &self,
        base_url: &str,
        request: &DeepXBalanceChangesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXBalanceChangeRecord>>, DeepXRestError> {
        collect_balance_change_pages(request, max_pages, |page_request| async move {
            self.get_balance_changes(base_url, &page_request).await
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
        include_str!("../../test_data/http/testnet/wallet_balance_changes_page_2.json");

    fn request() -> DeepXBalanceChangesRequest {
        DeepXBalanceChangesRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            start_ms: None,
            end_ms: None,
            change_types: Vec::new(),
            cursor: None,
            page_size: Some(1),
        }
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn typed_pages_check_record_ids_across_pages(#[case] duplicate: bool) {
        let response: Value = serde_json::from_str(RESPONSE).unwrap();
        let mut first: DeepXAccountPage<DeepXBalanceChangeRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        first.items.truncate(1);
        first.has_next = true;
        first.next_cursor = Some("next-page".to_string());
        let mut second = first.clone();
        second.has_next = false;
        second.next_cursor = None;
        if !duplicate {
            second.items[0].id += 1;
        }
        let mut pages = [first, second].into_iter();
        let result = collect_balance_change_pages(&request(), 2, |_| {
            std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
        })
        .await;
        if duplicate {
            assert!(matches!(
                result,
                Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint: "balance changes",
                    ..
                })
            ));
        } else {
            assert_eq!(result.unwrap().len(), 2);
        }
    }
}
