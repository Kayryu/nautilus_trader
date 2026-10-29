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

//! Complete keyset pagination for hourly unsettled-funding boundaries.

use std::{collections::HashSet, future::Future};

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::DeepXHourlyUnsettledFundingRequest,
    rest::{
        DeepXRestError, DeepXRestTransport, account::validate_hourly_unsettled_funding_page,
        models::DeepXHourlyUnsettledFundingRecord,
    },
};

/// Collects complete hourly unsettled-funding pages using caller-provided page reads.
///
/// Full pages advance with the exact four-field keyset boundary. Short or empty pages terminate.
/// Cross-page duplicate event IDs, repeated cursors, and budget exhaustion return no partial result.
///
/// # Errors
///
/// Returns a request, page-read, history-continuity, or pagination error.
pub async fn collect_hourly_unsettled_funding_pages<E, F, Fut>(
    request: &DeepXHourlyUnsettledFundingRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<Vec<DeepXHourlyUnsettledFundingRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXHourlyUnsettledFundingRequest) -> Fut,
    Fut: Future<Output = Result<Vec<DeepXHourlyUnsettledFundingRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let initial_cursor = request.cursor.as_ref().map(|cursor| cursor.token());
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, initial_cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let page_size = request.page_size.unwrap_or(20) as usize;
    let mut event_ids = HashSet::new();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        validate_hourly_unsettled_funding_page(&page, &next_request).map_err(E::from)?;
        for (index, record) in page.iter().enumerate() {
            if !event_ids.insert(record.boundary_event_id.clone()) {
                return Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint: "hourly unsettled funding",
                    message: format!("item {index} repeats a boundary event ID across pages"),
                }
                .into());
            }
        }
        let next_cursor = page.last().map(DeepXHourlyUnsettledFundingRecord::cursor);
        let cursor_token = next_cursor.as_ref().map(|cursor| cursor.token());
        let decision = pagination
            .observe_page(
                page.len(),
                (page.len() == page_size)
                    .then_some(cursor_token.as_deref())
                    .flatten(),
            )
            .map_err(E::from)?;
        pages.push(page);
        match decision {
            PaginationDecision::Complete => return Ok(pages),
            PaginationDecision::Continue(_) => next_request.cursor = next_cursor,
        }
    }
}

impl DeepXRestTransport {
    /// Returns all validated hourly unsettled-funding pages using one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_hourly_unsettled_funding_pages(
        &self,
        base_url: &str,
        request: &DeepXHourlyUnsettledFundingRequest,
        max_pages: usize,
    ) -> Result<Vec<Vec<DeepXHourlyUnsettledFundingRecord>>, DeepXRestError> {
        collect_hourly_unsettled_funding_pages(request, max_pages, |page_request| async move {
            self.get_hourly_unsettled_funding(base_url, &page_request)
                .await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use axum::{Router, extract::Query, response::IntoResponse, routing::get};
    use tokio::net::TcpListener;

    use super::*;
    use crate::query::DeepXAccountSortOrder;

    const FIRST_PAGE: &str = include_str!(
        "../../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_1.json"
    );
    const SECOND_PAGE: &str = include_str!(
        "../../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_2.json"
    );
    const PATH: &str = "/internal/v1/account/hourly-unsettled-funding";

    fn request() -> DeepXHourlyUnsettledFundingRequest {
        DeepXHourlyUnsettledFundingRequest {
            subaccount: None,
            wallet: Some("0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string()),
            market_id: Some(3),
            start_ms: None,
            end_ms: None,
            cursor: None,
            page_size: Some(5),
            sort: DeepXAccountSortOrder::Descending,
        }
    }

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn complete_history_forwards_the_full_keyset_cursor() {
        let first: serde_json::Value = serde_json::from_str(FIRST_PAGE).unwrap();
        let mut terminal: serde_json::Value = serde_json::from_str(SECOND_PAGE).unwrap();
        terminal["data"].as_array_mut().unwrap().truncate(2);
        let router = Router::new().route(
            PATH,
            get(move |Query(query): Query<std::collections::HashMap<String, String>>| {
                let first = first.clone();
                let terminal = terminal.clone();
                async move {
                    if query.contains_key("cursorTimestamp") {
                        assert_eq!(query["cursorTimestamp"], "1789506862596");
                        assert_eq!(query["cursorMarketId"], "3");
                        assert_eq!(
                            query["cursorSubaccount"],
                            "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
                        );
                        assert_eq!(
                            query["cursorEventId"],
                            "4846:186852294:0x3516695ba7257d3dc2988b2c31435991ba7e111f68962fcdadca6b1664b77ecf:none:26"
                        );
                        axum::Json(terminal).into_response()
                    } else {
                        axum::Json(first).into_response()
                    }
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();

        let pages = client
            .get_hourly_unsettled_funding_pages(&base_url, &request(), 2)
            .await
            .unwrap();

        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].len(), 5);
        assert_eq!(pages[1].len(), 2);
    }

    #[tokio::test]
    async fn full_page_exhausts_the_budget_without_partial_result() {
        let router = Router::new().route(PATH, get(|| async { FIRST_PAGE }));
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();

        assert!(matches!(
            client
                .get_hourly_unsettled_funding_pages(&base_url, &request(), 1)
                .await,
            Err(DeepXRestError::PaginationLimitExceeded { max_pages: 1 }),
        ));
    }

    #[tokio::test]
    async fn cross_page_duplicate_event_is_rejected() {
        let first: serde_json::Value = serde_json::from_str(FIRST_PAGE).unwrap();
        let mut terminal: serde_json::Value = serde_json::from_str(SECOND_PAGE).unwrap();
        terminal["data"].as_array_mut().unwrap().truncate(1);
        terminal["data"][0]["boundaryEventId"] = first["data"][0]["boundaryEventId"].clone();
        let router = Router::new().route(
            PATH,
            get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let first = first.clone();
                    let terminal = terminal.clone();
                    async move {
                        axum::Json(if query.contains_key("cursorTimestamp") {
                            terminal
                        } else {
                            first
                        })
                    }
                },
            ),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();

        assert!(matches!(
            client
                .get_hourly_unsettled_funding_pages(&base_url, &request(), 2)
                .await,
            Err(DeepXRestError::InvalidHistoryResponse { endpoint: "hourly unsettled funding", message })
                if message.contains("across pages"),
        ));
    }
}
