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

//! Cursor pagination and bounded collection for public Spot executions.

use std::{future::Future, num::NonZeroUsize};

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::DeepXSpotTradesRequest,
    rest::{
        DeepXRestError, DeepXRestTransport, DeepXSpotTradeContinuity,
        market::validate_spot_trades_page,
        models::{DeepXSpotTrade, DeepXSpotTradesPage},
    },
};

/// Collects complete validated Spot trade pages using caller-provided page reads.
///
/// # Errors
///
/// Returns a request, page-read, history-continuity, or pagination error.
pub async fn collect_spot_trade_pages<E, F, Fut>(
    request: &DeepXSpotTradesRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXSpotTradesPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXSpotTradesRequest) -> Fut,
    Fut: Future<Output = Result<DeepXSpotTradesPage, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXSpotTradeContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        validate_spot_trades_page(&page, &next_request).map_err(E::from)?;
        continuity
            .observe_page(&page, request.sort)
            .map_err(E::from)?;
        let decision = pagination
            .observe_response_page(
                "spot trades",
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

/// Collects up to `limit` validated Spot trades using caller-provided page reads.
///
/// # Errors
///
/// Returns a request, page-read, history-continuity, or pagination error.
pub async fn collect_spot_trades_limited<E, F, Fut>(
    request: &DeepXSpotTradesRequest,
    limit: NonZeroUsize,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXSpotTrade>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXSpotTradesRequest) -> Fut,
    Fut: Future<Output = Result<DeepXSpotTradesPage, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut trades = Vec::new();
    let mut continuity = DeepXSpotTradeContinuity::default();
    loop {
        let remaining = limit.get() - trades.len();
        let page_size = request
            .page_size
            .unwrap_or(100)
            .min(u32::try_from(remaining).unwrap_or(u32::MAX));
        next_request.page_size = Some(page_size);
        let page = fetch_page(next_request.clone()).await?;
        validate_spot_trades_page(&page, &next_request).map_err(E::from)?;
        continuity
            .observe_page(&page, request.sort)
            .map_err(E::from)?;
        if trades.len() + page.items.len() >= limit.get() {
            trades.extend(page.items.into_iter().take(remaining));
            return Ok(trades);
        }
        let decision = pagination
            .observe_response_page(
                "spot trades",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        trades.extend(page.items);
        match decision {
            PaginationDecision::Complete => return Ok(trades),
            PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
        }
    }
}

impl DeepXRestTransport {
    /// Returns complete validated Spot trade pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_spot_trade_pages(
        &self,
        base_url: &str,
        request: &DeepXSpotTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXSpotTradesPage>, DeepXRestError> {
        collect_spot_trade_pages(request, max_pages, |page_request| async move {
            self.get_spot_trades(base_url, &page_request).await
        })
        .await
    }

    /// Returns up to `limit` validated Spot trades from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_spot_trades_limited(
        &self,
        base_url: &str,
        request: &DeepXSpotTradesRequest,
        limit: NonZeroUsize,
        max_pages: usize,
    ) -> Result<Vec<DeepXSpotTrade>, DeepXRestError> {
        collect_spot_trades_limited(request, limit, max_pages, |page_request| async move {
            self.get_spot_trades(base_url, &page_request).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{Json, Router, extract::Query, routing::get};
    use rstest::rstest;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;
    use crate::query::DeepXAccountSortOrder;

    const FIRST_PAGE: &str =
        include_str!("../../test_data/http/testnet/spot_trades_eth_usdc_page_1.json");
    const PATH: &str = "/internal/v1/market/spot/trades";

    fn request() -> DeepXSpotTradesRequest {
        DeepXSpotTradesRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
            wallet: None,
            start_ms: None,
            end_ms: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(2),
        }
    }

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    #[rstest]
    #[case("valid")]
    #[case("duplicate")]
    #[case("wrong-order")]
    #[tokio::test]
    async fn pages_forward_cursor_and_reject_cross_page_drift(#[case] mutation: &str) {
        let first: Value = serde_json::from_str(FIRST_PAGE).unwrap();
        let cursor = first["data"]["nextCursor"].as_str().unwrap().to_string();
        let mut terminal = first.clone();
        terminal["data"]["items"] = json!([first["data"]["items"][1].clone()]);
        terminal["data"]["hasNext"] = json!(false);
        terminal["data"]["nextCursor"] = Value::Null;
        if mutation != "duplicate" {
            terminal["data"]["items"][0]["id"] = json!(188_405_949_000_001_u64);
            terminal["data"]["items"][0]["tradeTime"] = json!("2026-09-17T03:27:09.999Z");
        }
        if mutation == "wrong-order" {
            terminal["data"]["items"][0]["tradeTime"] = json!("2026-09-17T03:27:11.999Z");
        }
        let router = Router::new().route(
            PATH,
            get(move |Query(query): Query<HashMap<String, String>>| {
                let first = first.clone();
                let terminal = terminal.clone();
                let cursor = cursor.clone();
                async move {
                    if let Some(received) = query.get("cursor") {
                        assert_eq!(received, &cursor);
                        Json(terminal)
                    } else {
                        Json(first)
                    }
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();

        let result = client.get_spot_trade_pages(&base_url, &request(), 2).await;

        if mutation == "valid" {
            let pages = result.unwrap();
            assert_eq!(pages.len(), 2);
            assert_eq!(pages.iter().map(|page| page.items.len()).sum::<usize>(), 3);
        } else {
            assert!(matches!(
                result,
                Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint: "spot trades",
                    ..
                }),
            ));
        }
    }

    #[tokio::test]
    async fn full_page_exhausts_budget_without_partial_history() {
        let router = Router::new().route(PATH, get(|| async { FIRST_PAGE }));
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();

        assert!(matches!(
            client.get_spot_trade_pages(&base_url, &request(), 1).await,
            Err(DeepXRestError::PaginationLimitExceeded { max_pages: 1 }),
        ));
    }

    #[tokio::test]
    async fn explicit_trade_limit_stops_after_one_page() {
        let calls = Arc::new(AtomicUsize::new(0));
        let route_calls = Arc::clone(&calls);
        let router = Router::new().route(
            PATH,
            get(move || {
                route_calls.fetch_add(1, Ordering::Relaxed);
                async { FIRST_PAGE }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();

        let trades = client
            .get_spot_trades_limited(&base_url, &request(), NonZeroUsize::new(2).unwrap(), 1)
            .await
            .unwrap();

        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].id, 188_405_950_000_036);
        assert_eq!(trades[1].id, 188_405_950_000_026);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}
