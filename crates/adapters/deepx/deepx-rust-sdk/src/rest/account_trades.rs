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

//! Complete cursor pagination for subaccount perpetual and Spot trades.

use std::future::Future;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::{DeepXPerpAccountTradesRequest, DeepXSpotAccountTradesRequest},
    rest::{
        DeepXAccountPage, DeepXPerpAccountTradeContinuity, DeepXRawAccountPage, DeepXRestError,
        DeepXRestTransport, DeepXSpotAccountTradeContinuity,
        models::{DeepXPerpAccountTradeRecord, DeepXSpotAccountTradeRecord},
    },
};

/// Collects raw perpetual account-trade pages without decoding item payloads.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_perp_account_trade_pages_raw<E, F, Fut>(
    request: &DeepXPerpAccountTradesRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpAccountTradesRequest) -> Fut,
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
                "perp account trades",
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

/// Collects validated perpetual account-trade pages with cross-page continuity.
///
/// # Errors
///
/// Returns a request, page-read, trade-continuity, or pagination error.
pub async fn collect_perp_account_trade_pages<E, F, Fut>(
    request: &DeepXPerpAccountTradesRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXPerpAccountTradeRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpAccountTradesRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXPerpAccountTradeRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXPerpAccountTradeContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        continuity.observe_page(&page, request).map_err(E::from)?;
        let decision = pagination
            .observe_response_page(
                "perp account trades",
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

/// Collects raw Spot account-trade pages without decoding item payloads.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_spot_account_trade_pages_raw<E, F, Fut>(
    request: &DeepXSpotAccountTradesRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXSpotAccountTradesRequest) -> Fut,
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
                "spot account trades",
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

/// Collects validated Spot account-trade pages with cross-page continuity.
///
/// # Errors
///
/// Returns a request, page-read, trade-continuity, or pagination error.
pub async fn collect_spot_account_trade_pages<E, F, Fut>(
    request: &DeepXSpotAccountTradesRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXSpotAccountTradeRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXSpotAccountTradesRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXSpotAccountTradeRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXSpotAccountTradeContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        continuity.observe_page(&page, request).map_err(E::from)?;
        let decision = pagination
            .observe_response_page(
                "spot account trades",
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
    /// Returns complete raw perpetual account-trade pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_account_trade_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpAccountTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_perp_account_trade_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_account_trades_raw(base_url, &page_request)
                .await
        })
        .await
    }

    /// Returns complete validated perpetual account-trade pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_account_trade_pages(
        &self,
        base_url: &str,
        request: &DeepXPerpAccountTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpAccountTradeRecord>>, DeepXRestError> {
        collect_perp_account_trade_pages(request, max_pages, |page_request| async move {
            self.get_perp_account_trades(base_url, &page_request).await
        })
        .await
    }

    /// Returns complete raw Spot account-trade pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_spot_account_trade_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotAccountTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_spot_account_trade_pages_raw(request, max_pages, |page_request| async move {
            self.get_spot_account_trades_raw(base_url, &page_request)
                .await
        })
        .await
    }

    /// Returns complete validated Spot account-trade pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_spot_account_trade_pages(
        &self,
        base_url: &str,
        request: &DeepXSpotAccountTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXSpotAccountTradeRecord>>, DeepXRestError> {
        collect_spot_account_trade_pages(request, max_pages, |page_request| async move {
            self.get_spot_account_trades(base_url, &page_request).await
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
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;
    use crate::query::DeepXAccountSortOrder;

    const PERP_PATH: &str = "/internal/v1/account/perp/trades";
    const SPOT_PATH: &str = "/internal/v1/account/spot/trades";
    const PERP_FIXTURE: &str =
        include_str!("../../test_data/http/testnet/perp_account_trades_account_market_3.json");
    const SPOT_FIXTURE: &str =
        include_str!("../../test_data/http/testnet/spot_account_trades_eth_usdc_page_1.json");

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    fn perp_request() -> DeepXPerpAccountTradesRequest {
        DeepXPerpAccountTradesRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_id: Some(3),
            order_id: None,
            is_long: None,
            cursor: None,
            page_size: Some(1),
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
        }
    }

    fn spot_request() -> DeepXSpotAccountTradesRequest {
        DeepXSpotAccountTradesRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            order_id: None,
            order_side: None,
            name: Some("ETH/USDC".to_string()),
            pair: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(1),
        }
    }

    #[tokio::test]
    async fn raw_perp_pages_preserve_items_and_forward_cursor() {
        const FIRST: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":["#,
            r#"{"exact":0.004000000000000001}],"hasNext":true,"nextCursor":"next"}}"#,
        );
        const SECOND: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":[],"#,
            r#""hasNext":false,"nextCursor":null}}"#,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let router = Router::new().route(
            PERP_PATH,
            get(move |Query(query): Query<HashMap<String, String>>| {
                let call = observed.fetch_add(1, Ordering::Relaxed);
                async move {
                    match call {
                        0 => {
                            assert!(!query.contains_key("cursor"));
                            FIRST
                        }
                        1 => {
                            assert_eq!(query["cursor"], "next");
                            SECOND
                        }
                        _ => panic!("unexpected raw perpetual account-trade page request"),
                    }
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();

        let pages = client
            .get_perp_account_trade_pages_raw(&base_url, &perp_request(), 2)
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items[0].get(), r#"{"exact":0.004000000000000001}"#);
        assert!(pages[1].items.is_empty());
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn typed_perp_pages_reject_cross_page_duplicate_id() {
        let fixture: Value = serde_json::from_str(PERP_FIXTURE).unwrap();
        let item = fixture["data"]["items"][0].clone();
        let router = Router::new().route(
            PERP_PATH,
            get(move |Query(query): Query<HashMap<String, String>>| {
                let item = item.clone();
                async move {
                    let first = !query.contains_key("cursor");
                    if !first {
                        assert_eq!(query["cursor"], "next");
                    }
                    Json(json!({
                        "code": 200, "msg": "success", "fail": false,
                        "data": {
                            "items": [item], "hasNext": first,
                            "nextCursor": first.then_some("next"),
                        },
                    }))
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();

        assert!(matches!(
            client
                .get_perp_account_trade_pages(&base_url, &perp_request(), 2)
                .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp account trades",
                ..
            }),
        ));
    }

    #[tokio::test]
    async fn typed_spot_pages_reject_cross_page_duplicate_id() {
        let fixture: Value = serde_json::from_str(SPOT_FIXTURE).unwrap();
        let item = fixture["data"]["items"][0].clone();
        let router = Router::new().route(
            SPOT_PATH,
            get(move |Query(query): Query<HashMap<String, String>>| {
                let item = item.clone();
                async move {
                    let first = !query.contains_key("cursor");
                    if !first {
                        assert_eq!(query["cursor"], "next");
                    }
                    Json(json!({
                        "code": 200, "msg": "success", "fail": false,
                        "data": {
                            "items": [item], "hasNext": first,
                            "nextCursor": first.then_some("next"),
                        },
                    }))
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();

        assert!(matches!(
            client
                .get_spot_account_trade_pages(&base_url, &spot_request(), 2)
                .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot account trades",
                ..
            }),
        ));
    }
}
