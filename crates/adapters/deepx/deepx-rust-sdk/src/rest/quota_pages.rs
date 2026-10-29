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

//! Complete cursor pagination for wallet quota history.

use std::future::Future;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::DeepXQuotaHistoryRequest,
    rest::{
        DeepXAccountPage, DeepXQuotaHistoryContinuity, DeepXRestError, DeepXRestTransport,
        models::DeepXQuotaHistoryRecord,
    },
};

/// Collects validated quota-history pages with cross-page continuity.
///
/// # Errors
///
/// Returns a request, page-read, pagination, or record-continuity error.
pub async fn collect_quota_history_pages<E, F, Fut>(
    request: &DeepXQuotaHistoryRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXQuotaHistoryRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXQuotaHistoryRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXQuotaHistoryRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXQuotaHistoryContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        let decision = pagination
            .observe_response_page(
                "quota history",
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
    /// Returns complete validated wallet quota-history pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_quota_history_pages(
        &self,
        base_url: &str,
        request: &DeepXQuotaHistoryRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXQuotaHistoryRecord>>, DeepXRestError> {
        collect_quota_history_pages(request, max_pages, |page_request| async move {
            self.get_quota_history(base_url, &page_request).await
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
        include_str!("../../test_data/http/testnet/quota_history_purchase_page.json");

    fn request() -> DeepXQuotaHistoryRequest {
        DeepXQuotaHistoryRequest {
            wallet: "0x0a40c3efbc3b3bebdf1fb6f0f8c612eb336b25ae".to_string(),
            buyer_address: None,
            history_type: None,
            cursor: None,
            limit: Some(1),
        }
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_repeated_quota_identity_across_pages() {
        let response: Value = serde_json::from_str(RESPONSE).unwrap();
        let mut first: DeepXAccountPage<DeepXQuotaHistoryRecord> =
            serde_json::from_value(response["data"].clone()).unwrap();
        first.items.truncate(1);
        first.has_next = true;
        first.next_cursor = Some("next-page".to_string());
        let mut second = first.clone();
        second.has_next = false;
        second.next_cursor = None;
        let mut pages = [first, second].into_iter();
        let mut cursors = Vec::new();
        let result = collect_quota_history_pages(&request(), 2, |page_request| {
            cursors.push(page_request.cursor);
            std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
        })
        .await;
        assert_eq!(cursors, [None, Some("next-page".to_string())]);
        assert!(matches!(
            result,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "quota history",
                ..
            })
        ));
    }
}
