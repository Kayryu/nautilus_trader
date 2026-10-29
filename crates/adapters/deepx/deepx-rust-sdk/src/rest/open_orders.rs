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

//! Complete cursor pagination for active perpetual and Spot orders.

use std::future::Future;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::{DeepXPerpOpenOrdersRequest, DeepXSpotOpenOrdersRequest},
    rest::{
        DeepXAccountPage, DeepXPerpOrderContinuity, DeepXRawAccountPage, DeepXRestError,
        DeepXRestTransport, DeepXSpotOrderContinuity,
        models::{DeepXPerpOrderRecord, DeepXSpotOrderRecord},
    },
};

/// Collects raw active perpetual-order pages without decoding their item payloads.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_perp_open_order_pages_raw<E, F, Fut>(
    request: &DeepXPerpOpenOrdersRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpOpenOrdersRequest) -> Fut,
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
                "perp open orders",
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

/// Collects validated active perpetual-order pages using caller-provided page reads.
///
/// # Errors
///
/// Returns a request, page-read, order-continuity, or pagination error.
pub async fn collect_perp_open_order_pages<E, F, Fut>(
    request: &DeepXPerpOpenOrdersRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXPerpOrderRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpOpenOrdersRequest) -> Fut,
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
        let decision = pagination
            .observe_response_page(
                "perp open orders",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        continuity
            .observe_open_page(&page, request)
            .map_err(E::from)?;
        pages.push(page);
        match decision {
            PaginationDecision::Complete => return Ok(pages),
            PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
        }
    }
}

/// Collects validated active Spot-order pages using caller-provided page reads.
///
/// # Errors
///
/// Returns a request, page-read, order-continuity, or pagination error.
pub async fn collect_spot_open_order_pages<E, F, Fut>(
    request: &DeepXSpotOpenOrdersRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXSpotOrderRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXSpotOpenOrdersRequest) -> Fut,
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
        let decision = pagination
            .observe_response_page(
                "spot open orders",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        continuity
            .observe_open_page(&page, request)
            .map_err(E::from)?;
        pages.push(page);
        match decision {
            PaginationDecision::Complete => return Ok(pages),
            PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
        }
    }
}

impl DeepXRestTransport {
    /// Returns complete raw active perpetual-order pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_open_order_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpOpenOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_perp_open_order_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_open_orders_raw(base_url, &page_request).await
        })
        .await
    }

    /// Returns complete validated active perpetual-order pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_open_order_pages(
        &self,
        base_url: &str,
        request: &DeepXPerpOpenOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpOrderRecord>>, DeepXRestError> {
        collect_perp_open_order_pages(request, max_pages, |page_request| async move {
            self.get_perp_open_orders(base_url, &page_request).await
        })
        .await
    }

    /// Returns complete validated active Spot-order pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_spot_open_order_pages(
        &self,
        base_url: &str,
        request: &DeepXSpotOpenOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXSpotOrderRecord>>, DeepXRestError> {
        collect_spot_open_order_pages(request, max_pages, |page_request| async move {
            self.get_spot_open_orders(base_url, &page_request).await
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

    const PERP_HISTORY: &str =
        include_str!("../../test_data/http/testnet/perp_history_orders_account_market_3.json");
    const SPOT_HISTORY: &str =
        include_str!("../../test_data/http/testnet/spot_history_orders_eth_usdc_page_1.json");
    const PERP_PATH: &str = "/internal/v1/account/perp/open-orders";
    const SPOT_PATH: &str = "/internal/v1/account/spot/open-orders";

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn raw_perp_pages_preserve_items_and_forward_cursor() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let router = Router::new().route(
            PERP_PATH,
            get(move |Query(query): Query<HashMap<String, String>>| {
                let call = observed.fetch_add(1, Ordering::Relaxed);
                async move {
                    assert_eq!(query["pageSize"], "1");
                    match call {
                        0 => {
                            assert!(!query.contains_key("cursor"));
                            Json(json!({
                                "code": 200, "msg": "success", "fail": false,
                                "data": {
                                    "items": [{"exact": 123456789012345678_u64}],
                                    "hasNext": true, "nextCursor": "next",
                                },
                            }))
                        }
                        1 => {
                            assert_eq!(query["cursor"], "next");
                            Json(json!({
                                "code": 200, "msg": "success", "fail": false,
                                "data": {"items": [], "hasNext": false, "nextCursor": null},
                            }))
                        }
                        _ => panic!("unexpected raw perpetual open-order page request"),
                    }
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();
        let request = DeepXPerpOpenOrdersRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_id: Some(3),
            is_long: None,
            cursor: None,
            page_size: Some(1),
            sort: DeepXAccountSortOrder::Descending,
        };

        let pages = client
            .get_perp_open_order_pages_raw(&base_url, &request, 2)
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items[0].get(), r#"{"exact":123456789012345678}"#);
        assert!(pages[1].items.is_empty());
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn perp_pages_forward_cursor_and_reject_cross_page_duplicate() {
        let mut captured: Value = serde_json::from_str(PERP_HISTORY).unwrap();
        for item in captured["data"]["items"].as_array_mut().unwrap() {
            item["status"] = json!("Open");
            item["sizeFilled"] = json!(0);
            item["sizeRemain"] = item["size"].clone();
            item["avgFillPrice"] = Value::Null;
        }
        let first_item = captured["data"]["items"][0].clone();
        let second_item = captured["data"]["items"][1].clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let router = Router::new().route(
            PERP_PATH,
            get(move |Query(query): Query<HashMap<String, String>>| {
                let call = observed.fetch_add(1, Ordering::Relaxed);
                let first_item = first_item.clone();
                let second_item = second_item.clone();
                async move {
                    assert_eq!(query["marketId"], "3");
                    assert_eq!(query["pageSize"], "1");
                    assert_eq!(query["sort"], "DESC");
                    let (item, has_next, next_cursor) = match call {
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
                        _ => panic!("unexpected perpetual open-order page request"),
                    };
                    Json(json!({
                        "code": 200,
                        "msg": "success",
                        "fail": false,
                        "data": {"items": [item], "hasNext": has_next, "nextCursor": next_cursor},
                    }))
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();
        let request = DeepXPerpOpenOrdersRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_id: Some(3),
            is_long: None,
            cursor: None,
            page_size: Some(1),
            sort: DeepXAccountSortOrder::Descending,
        };

        let pages = client
            .get_perp_open_order_pages(&base_url, &request, 2)
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items.len(), 1);
        assert_eq!(pages[1].items.len(), 1);

        assert!(matches!(
            client
                .get_perp_open_order_pages(&base_url, &request, 2)
                .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp open orders",
                ..
            }),
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn spot_pages_forward_cursor_and_accept_empty_terminal_page() {
        const CURSOR: &str = concat!(
            "MTc4OTYyNzM3OTA1MzoxODg1NzM3ODQ6MTc6MHg5MDY4ZDRhYzg5MWExNDc4NGMx",
            "Nzg3N2ViNzRiZDg0ODliMzM2N2M3MWQ3Mjc2NmRiZmE0ZGZiZmI2NjJmYTM3OjE3",
            "ODk2MjczNzg2NTc6U0VMTA",
        );
        const TERMINAL: &str = concat!(
            r#"{"code":200,"msg":"success","fail":false,"data":{"items":[],"#,
            r#""hasNext":false,"nextCursor":null}}"#,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let router = Router::new().route(
            SPOT_PATH,
            get(move |Query(query): Query<HashMap<String, String>>| {
                let call = observed.fetch_add(1, Ordering::Relaxed);
                async move {
                    assert_eq!(query["name"], "ETH/USDC");
                    assert_eq!(query["pageSize"], "3");
                    match call {
                        0 => {
                            assert!(!query.contains_key("cursor"));
                            SPOT_HISTORY
                        }
                        1 => {
                            assert_eq!(query["cursor"], CURSOR);
                            TERMINAL
                        }
                        _ => panic!("unexpected Spot open-order page request"),
                    }
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();
        let request = DeepXSpotOpenOrdersRequest {
            subaccount: "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c".to_string(),
            name: Some("ETH/USDC".to_string()),
            pair: None,
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(3),
        };

        let pages = client
            .get_spot_open_order_pages(&base_url, &request, 2)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].items.len(), 3);
        assert!(pages[1].items.is_empty());
    }
}
