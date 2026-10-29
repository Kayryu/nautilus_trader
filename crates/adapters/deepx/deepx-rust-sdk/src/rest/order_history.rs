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

//! Complete cursor pagination for perpetual order history and position orders.

use std::future::Future;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::{
        DeepXPerpHistoryOrdersRequest, DeepXPerpPositionOrdersRequest,
        DeepXSpotHistoryOrdersRequest,
    },
    rest::{
        DeepXAccountPage, DeepXPerpOrderContinuity, DeepXPerpPositionOrderContinuity,
        DeepXRawAccountPage, DeepXRestError, DeepXRestTransport, DeepXSpotOrderContinuity,
        models::{DeepXPerpOrderRecord, DeepXSpotOrderRecord},
    },
};

/// Collects raw historical perpetual-order pages without decoding their item payloads.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_perp_history_order_pages_raw<E, F, Fut>(
    request: &DeepXPerpHistoryOrdersRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpHistoryOrdersRequest) -> Fut,
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
                "perp history orders",
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

/// Collects validated historical perpetual-order pages with cross-page continuity.
///
/// # Errors
///
/// Returns a request, page-read, order-continuity, or pagination error.
pub async fn collect_perp_history_order_pages<E, F, Fut>(
    request: &DeepXPerpHistoryOrdersRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXPerpOrderRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpHistoryOrdersRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXPerpOrderRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXPerpOrderContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        continuity
            .observe_history_page(&page, request)
            .map_err(E::from)?;
        let decision = pagination
            .observe_response_page(
                "perp history orders",
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

/// Collects raw perpetual position-order pages without decoding their item payloads.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_perp_position_order_pages_raw<E, F, Fut>(
    request: &DeepXPerpPositionOrdersRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpPositionOrdersRequest) -> Fut,
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
                "perp position orders",
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

/// Collects validated perpetual position-order pages with cross-page continuity.
///
/// # Errors
///
/// Returns a request, page-read, order-continuity, or pagination error.
pub async fn collect_perp_position_order_pages<E, F, Fut>(
    request: &DeepXPerpPositionOrdersRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXPerpOrderRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpPositionOrdersRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXPerpOrderRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXPerpPositionOrderContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        continuity.observe_page(&page, request).map_err(E::from)?;
        let decision = pagination
            .observe_response_page(
                "perp position orders",
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

/// Collects raw historical Spot-order pages without decoding their item payloads.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_spot_history_order_pages_raw<E, F, Fut>(
    request: &DeepXSpotHistoryOrdersRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXSpotHistoryOrdersRequest) -> Fut,
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
                "spot history orders",
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

/// Collects validated historical Spot-order pages with cross-page continuity.
///
/// # Errors
///
/// Returns a request, page-read, order-continuity, or pagination error.
pub async fn collect_spot_history_order_pages<E, F, Fut>(
    request: &DeepXSpotHistoryOrdersRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXSpotOrderRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXSpotHistoryOrdersRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXSpotOrderRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXSpotOrderContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        continuity
            .observe_history_page(&page, request)
            .map_err(E::from)?;
        let decision = pagination
            .observe_response_page(
                "spot history orders",
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
    /// Returns complete raw historical Spot-order pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_spot_history_order_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotHistoryOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_spot_history_order_pages_raw(request, max_pages, |page_request| async move {
            self.get_spot_history_orders_raw(base_url, &page_request)
                .await
        })
        .await
    }

    /// Returns complete validated historical Spot-order pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_spot_history_order_pages(
        &self,
        base_url: &str,
        request: &DeepXSpotHistoryOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXSpotOrderRecord>>, DeepXRestError> {
        collect_spot_history_order_pages(request, max_pages, |page_request| async move {
            self.get_spot_history_orders(base_url, &page_request).await
        })
        .await
    }

    /// Returns complete raw historical perpetual-order pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_history_order_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpHistoryOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_perp_history_order_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_history_orders_raw(base_url, &page_request)
                .await
        })
        .await
    }

    /// Returns complete validated historical perpetual-order pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_history_order_pages(
        &self,
        base_url: &str,
        request: &DeepXPerpHistoryOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpOrderRecord>>, DeepXRestError> {
        collect_perp_history_order_pages(request, max_pages, |page_request| async move {
            self.get_perp_history_orders(base_url, &page_request).await
        })
        .await
    }

    /// Returns complete raw perpetual position-order pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_position_order_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpPositionOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_perp_position_order_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_position_orders_raw(base_url, &page_request)
                .await
        })
        .await
    }

    /// Returns complete validated perpetual position-order pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_position_order_pages(
        &self,
        base_url: &str,
        request: &DeepXPerpPositionOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpOrderRecord>>, DeepXRestError> {
        collect_perp_position_order_pages(request, max_pages, |page_request| async move {
            self.get_perp_position_orders(base_url, &page_request).await
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

    const HISTORY_PATH: &str = "/internal/v1/account/perp/history-orders";
    const POSITION_PATH: &str = "/internal/v1/account/perp/position-orders";
    const SPOT_HISTORY_PATH: &str = "/internal/v1/account/spot/history-orders";
    const HISTORY_FIXTURE: &str =
        include_str!("../../test_data/http/testnet/perp_history_orders_account_market_3.json");
    const SPOT_HISTORY_FIXTURE: &str =
        include_str!("../../test_data/http/testnet/spot_history_orders_eth_usdc_page_1.json");
    const SUBACCOUNT: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn raw_history_pages_preserve_items_and_forward_cursor() {
        const FIRST: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":["#,
            r#"{"exact":123456789012345678}],"hasNext":true,"nextCursor":"next"}}"#,
        );
        const SECOND: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":["#,
            r#"{"orderId":"1"}],"hasNext":false,"nextCursor":null}}"#,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let router = Router::new().route(
            HISTORY_PATH,
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
                        _ => panic!("unexpected historical order page request"),
                    }
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();
        let request = DeepXPerpHistoryOrdersRequest {
            subaccount: SUBACCOUNT.to_string(),
            market_id: Some(3),
            cursor: None,
            page_size: Some(1),
            sort: DeepXAccountSortOrder::Descending,
        };

        let pages = client
            .get_perp_history_order_pages_raw(&base_url, &request, 2)
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items[0].get(), r#"{"exact":123456789012345678}"#);
        assert_eq!(pages[1].items[0].get(), r#"{"orderId":"1"}"#);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn typed_history_pages_reject_cross_page_duplicate() {
        let fixture: Value = serde_json::from_str(HISTORY_FIXTURE).unwrap();
        let item = fixture["data"]["items"][0].clone();
        let router = Router::new().route(
            HISTORY_PATH,
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
        let request = DeepXPerpHistoryOrdersRequest {
            subaccount: SUBACCOUNT.to_string(),
            market_id: Some(3),
            cursor: None,
            page_size: Some(1),
            sort: DeepXAccountSortOrder::Descending,
        };

        assert!(matches!(
            client
                .get_perp_history_order_pages(&base_url, &request, 2)
                .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp history orders",
                ..
            }),
        ));
    }

    #[tokio::test]
    async fn position_order_pages_enforce_ascending_identity_across_pages() {
        let fixture: Value = serde_json::from_str(HISTORY_FIXTURE).unwrap();
        let first_item = fixture["data"]["items"][1].clone();
        let second_item = fixture["data"]["items"][0].clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let router = Router::new().route(
            POSITION_PATH,
            get(move |Query(query): Query<HashMap<String, String>>| {
                let call = observed.fetch_add(1, Ordering::Relaxed);
                let first_item = first_item.clone();
                let second_item = second_item.clone();
                async move {
                    assert_eq!(query["positionId"], "42");
                    let (item, has_next, cursor) = match call {
                        0 | 2 => {
                            assert!(!query.contains_key("cursor"));
                            (first_item, true, Some("next"))
                        }
                        1 => {
                            assert_eq!(query["cursor"], "next");
                            (second_item, false, None)
                        }
                        3 => {
                            assert_eq!(query["cursor"], "next");
                            (first_item, false, None)
                        }
                        _ => panic!("unexpected position-order page request"),
                    };
                    Json(json!({
                        "code": 200, "msg": "success", "fail": false,
                        "data": {
                            "items": [item], "hasNext": has_next, "nextCursor": cursor,
                        },
                    }))
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();
        let request = DeepXPerpPositionOrdersRequest {
            subaccount: SUBACCOUNT.to_string(),
            position_id: 42,
            cursor: None,
            page_size: Some(1),
        };

        let pages = client
            .get_perp_position_order_pages(&base_url, &request, 2)
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items.len(), 1);
        assert_eq!(pages[1].items.len(), 1);
        assert!(matches!(
            client
                .get_perp_position_order_pages(&base_url, &request, 2)
                .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp position orders",
                ..
            }),
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn typed_spot_history_pages_reject_cross_page_duplicate() {
        let fixture: Value = serde_json::from_str(SPOT_HISTORY_FIXTURE).unwrap();
        let item = fixture["data"]["items"][0].clone();
        let router = Router::new().route(
            SPOT_HISTORY_PATH,
            get(move |Query(query): Query<HashMap<String, String>>| {
                let item = item.clone();
                async move {
                    assert_eq!(query["name"], "ETH/USDC");
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
        let request = DeepXSpotHistoryOrdersRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(1),
        };

        assert!(matches!(
            client
                .get_spot_history_order_pages(&base_url, &request, 2)
                .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot history orders",
                ..
            }),
        ));
    }
}
