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

//! Global cursor pagination for wallet-grouped orders and Spot trades.

use std::future::Future;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::{
        DeepXPerpWalletOrdersRequest, DeepXPerpWalletTradesRequest, DeepXSpotWalletOrdersRequest,
        DeepXSpotWalletTradesRequest,
    },
    rest::{
        DeepXAccountPage, DeepXPerpWalletOrderContinuity, DeepXPerpWalletTradeContinuity,
        DeepXRestError, DeepXRestTransport, DeepXSpotWalletOrderContinuity,
        DeepXSpotWalletTradeContinuity,
        models::{
            DeepXPerpAccountTradeRecord, DeepXPerpWalletOrdersPage, DeepXPerpWalletTradeMarket,
            DeepXSpotWalletOrdersPage, DeepXSpotWalletTradesPage,
        },
        validate_account_address,
    },
};

/// Collects validated Spot wallet-order pages under one global cursor.
///
/// # Errors
///
/// Returns a request, page-read, order-continuity, or pagination error.
pub async fn collect_spot_wallet_order_pages<E, F, Fut>(
    request: &DeepXSpotWalletOrdersRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXSpotWalletOrdersPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXSpotWalletOrdersRequest) -> Fut,
    Fut: Future<Output = Result<DeepXSpotWalletOrdersPage, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXSpotWalletOrderContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        continuity
            .observe_page(&page, request.sort)
            .map_err(E::from)?;
        let item_count = page
            .markets
            .iter()
            .flat_map(|market| &market.subaccounts)
            .map(|subaccount| subaccount.orders.items.len())
            .sum();
        let decision = pagination
            .observe_response_page(
                "spot wallet orders",
                item_count,
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

/// Collects validated Spot wallet-trade pages under one global cursor.
///
/// # Errors
///
/// Returns a request, page-read, trade-continuity, or pagination error.
pub async fn collect_spot_wallet_trade_pages<E, F, Fut>(
    request: &DeepXSpotWalletTradesRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXSpotWalletTradesPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXSpotWalletTradesRequest) -> Fut,
    Fut: Future<Output = Result<DeepXSpotWalletTradesPage, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXSpotWalletTradeContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        continuity
            .observe_page(&page, request.sort)
            .map_err(E::from)?;
        let item_count = page
            .markets
            .iter()
            .flat_map(|market| &market.subaccounts)
            .map(|subaccount| subaccount.trades.items.len())
            .sum();
        let decision = pagination
            .observe_response_page(
                "spot wallet trades",
                item_count,
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

/// Collects validated perpetual wallet-order pages under one global cursor.
///
/// # Errors
///
/// Returns a request, page-read, order-continuity, or pagination error.
pub async fn collect_perp_wallet_order_pages<E, F, Fut>(
    request: &DeepXPerpWalletOrdersRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXPerpWalletOrdersPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpWalletOrdersRequest) -> Fut,
    Fut: Future<Output = Result<DeepXPerpWalletOrdersPage, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXPerpWalletOrderContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        continuity
            .observe_page(&page, request.sort)
            .map_err(E::from)?;
        let item_count = page
            .markets
            .iter()
            .flat_map(|market| &market.subaccounts)
            .map(|subaccount| subaccount.orders.items.len())
            .sum();
        let decision = pagination
            .observe_response_page(
                "perp wallet orders",
                item_count,
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

/// Collects one selected subaccount's perpetual wallet-trade pages.
///
/// Other groups may have independent cursors and are not advanced by this collector.
///
/// # Errors
///
/// Returns a request, missing-group, page-read, trade-continuity, or pagination error.
pub async fn collect_perp_wallet_trade_pages_for_subaccount<E, F, Fut>(
    request: &DeepXPerpWalletTradesRequest,
    subaccount: &str,
    expected_market_name: &str,
    max_pages: usize,
    mut fetch_groups: F,
) -> Result<Vec<DeepXAccountPage<DeepXPerpAccountTradeRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpWalletTradesRequest) -> Fut,
    Fut: Future<Output = Result<Vec<DeepXPerpWalletTradeMarket>, E>>,
{
    request.validate().map_err(E::from)?;
    validate_account_address("perp wallet trades", "subaccount", subaccount).map_err(E::from)?;
    let market_id = request.market_id.ok_or_else(|| {
        E::from(DeepXRestError::InvalidRequest(
            "perp wallet trade pagination requires a market ID".to_string(),
        ))
    })?;
    if expected_market_name.trim().is_empty() {
        return Err(E::from(DeepXRestError::InvalidRequest(
            "perp wallet trade pagination requires a market name".to_string(),
        )));
    }
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXPerpWalletTradeContinuity::default();
    let mut pages = Vec::new();
    loop {
        let markets = fetch_groups(next_request.clone()).await?;
        let market = markets
            .iter()
            .find(|market| market.market_id == market_id)
            .ok_or_else(|| {
                E::from(DeepXRestError::InvalidHistoryResponse {
                    endpoint: "perp wallet trades",
                    message: "selected market group is missing".to_string(),
                })
            })?;
        if market.market_name != expected_market_name {
            return Err(E::from(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp wallet trades",
                message: "selected market name differs from expected identity".to_string(),
            }));
        }
        let group = market
            .subaccounts
            .iter()
            .find(|group| group.subaccount.eq_ignore_ascii_case(subaccount))
            .ok_or_else(|| {
                E::from(DeepXRestError::InvalidHistoryResponse {
                    endpoint: "perp wallet trades",
                    message: "selected subaccount group is missing".to_string(),
                })
            })?;
        continuity
            .observe_page(&group.trades, &next_request, market_id)
            .map_err(E::from)?;
        let decision = pagination
            .observe_response_page(
                "perp wallet trades",
                group.trades.items.len(),
                group.trades.has_next,
                group.trades.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        pages.push(group.trades.clone());
        match decision {
            PaginationDecision::Complete => return Ok(pages),
            PaginationDecision::Continue(cursor) => next_request.cursor = Some(cursor),
        }
    }
}

impl DeepXRestTransport {
    /// Returns complete validated trade pages for one wallet subaccount and perpetual market.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, selection, or pagination error.
    pub async fn get_perp_wallet_trade_pages_for_subaccount(
        &self,
        base_url: &str,
        request: &DeepXPerpWalletTradesRequest,
        subaccount: &str,
        expected_market_name: &str,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpAccountTradeRecord>>, DeepXRestError> {
        collect_perp_wallet_trade_pages_for_subaccount(
            request,
            subaccount,
            expected_market_name,
            max_pages,
            |page_request| async move {
                self.get_perp_wallet_trades(base_url, &page_request).await
            },
        )
        .await
    }

    /// Returns complete validated Spot wallet-order pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_spot_wallet_order_pages(
        &self,
        base_url: &str,
        request: &DeepXSpotWalletOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXSpotWalletOrdersPage>, DeepXRestError> {
        collect_spot_wallet_order_pages(request, max_pages, |page_request| async move {
            self.get_spot_wallet_orders(base_url, &page_request).await
        })
        .await
    }

    /// Returns complete validated Spot wallet-trade pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_spot_wallet_trade_pages(
        &self,
        base_url: &str,
        request: &DeepXSpotWalletTradesRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXSpotWalletTradesPage>, DeepXRestError> {
        collect_spot_wallet_trade_pages(request, max_pages, |page_request| async move {
            self.get_spot_wallet_trades(base_url, &page_request).await
        })
        .await
    }

    /// Returns complete validated perpetual wallet-order pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_wallet_order_pages(
        &self,
        base_url: &str,
        request: &DeepXPerpWalletOrdersRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXPerpWalletOrdersPage>, DeepXRestError> {
        collect_perp_wallet_order_pages(request, max_pages, |page_request| async move {
            self.get_perp_wallet_orders(base_url, &page_request).await
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

    const SPOT_ORDERS: &str =
        include_str!("../../test_data/http/testnet/spot_wallet_orders_eth_usdc_page_1.json");
    const SPOT_TRADES: &str =
        include_str!("../../test_data/http/testnet/spot_wallet_trades_eth_usdc_page_1.json");
    const PERP_ORDERS: &str =
        include_str!("../../test_data/http/testnet/perp_wallet_orders_page_1.json");
    const PERP_TRADES: &str =
        include_str!("../../test_data/http/testnet/perp_wallet_trades_page.json");

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    #[rstest]
    #[case::spot_orders(
        "/internal/v1/account/spot/orders-by-wallet",
        SPOT_ORDERS,
        "orders",
        "spot wallet orders",
        "duplicate"
    )]
    #[case::spot_orders_wrong_order(
        "/internal/v1/account/spot/orders-by-wallet",
        SPOT_ORDERS,
        "orders",
        "spot wallet orders",
        "wrong-order"
    )]
    #[case::spot_trades(
        "/internal/v1/account/spot/trades-by-wallet",
        SPOT_TRADES,
        "trades",
        "spot wallet trades",
        "duplicate"
    )]
    #[case::spot_trades_wrong_order(
        "/internal/v1/account/spot/trades-by-wallet",
        SPOT_TRADES,
        "trades",
        "spot wallet trades",
        "wrong-order"
    )]
    #[case::perp_orders(
        "/internal/v1/account/perp/orders-by-wallet",
        PERP_ORDERS,
        "orders",
        "perp wallet orders",
        "duplicate"
    )]
    #[case::perp_orders_wrong_order(
        "/internal/v1/account/perp/orders-by-wallet",
        PERP_ORDERS,
        "orders",
        "perp wallet orders",
        "wrong-order"
    )]
    #[tokio::test]
    async fn wallet_pages_reject_cross_page_duplicate_or_wrong_order(
        #[case] path: &'static str,
        #[case] fixture: &str,
        #[case] group_key: &str,
        #[case] endpoint: &str,
        #[case] mutation: &str,
    ) {
        let first: Value = serde_json::from_str(fixture).unwrap();
        let cursor = first["data"][0]["subaccounts"][0][group_key]["nextCursor"]
            .as_str()
            .unwrap()
            .to_string();
        let mut terminal = first.clone();
        terminal["data"].as_array_mut().unwrap().truncate(1);
        terminal["data"][0]["subaccounts"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        let group = &mut terminal["data"][0]["subaccounts"][0][group_key];
        group["items"].as_array_mut().unwrap().truncate(1);
        if mutation == "wrong-order" {
            let item = &mut group["items"][0];
            match endpoint {
                "spot wallet orders" => {
                    item["orderId"] = json!("1789631508485");
                    item["createTime"] = json!("2026-09-18T08:00:00Z");
                    item["updatedTime"] = json!("2026-09-18T08:00:00Z");
                }
                "spot wallet trades" => {
                    item["id"] = json!(188_632_780_999_999_u64);
                    item["createdAt"] = json!("2026-09-18T08:00:00Z");
                }
                "perp wallet orders" => {
                    item["orderId"] = json!("32");
                    item["createTime"] = json!("2026-09-17T08:20:11.756Z");
                    item["updatedTime"] = json!("2026-09-17T08:20:11.756Z");
                }
                _ => unreachable!(),
            }
        }
        group["hasNext"] = json!(false);
        group["nextCursor"] = Value::Null;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let router = Router::new().route(
            path,
            get(move |Query(query): Query<HashMap<String, String>>| {
                let call = observed.fetch_add(1, Ordering::Relaxed);
                let first = first.clone();
                let terminal = terminal.clone();
                let cursor = cursor.clone();
                async move {
                    match call {
                        0 => {
                            assert!(!query.contains_key("cursor"));
                            Json(first)
                        }
                        1 => {
                            assert_eq!(query["cursor"], cursor);
                            Json(terminal)
                        }
                        _ => panic!("unexpected wallet page request"),
                    }
                }
            }),
        );
        let base_url = spawn_server(router).await;
        let client = DeepXRestTransport::new(Some(5), None).unwrap();
        let sort = DeepXAccountSortOrder::Descending;

        let result = match endpoint {
            "spot wallet orders" => client
                .get_spot_wallet_order_pages(
                    &base_url,
                    &DeepXSpotWalletOrdersRequest {
                        wallet: Some("0x89bb0946046588f4f257ffc71bd3231aba13474a".to_string()),
                        name: Some("ETH/USDC".to_string()),
                        pair: None,
                        order_side: None,
                        cursor: None,
                        sort,
                        start_ms: None,
                        end_ms: None,
                        page_size: Some(5),
                    },
                    2,
                )
                .await
                .map(|_| ()),
            "spot wallet trades" => client
                .get_spot_wallet_trade_pages(
                    &base_url,
                    &DeepXSpotWalletTradesRequest {
                        wallet: "0x89bb0946046588f4f257ffc71bd3231aba13474a".to_string(),
                        name: Some("ETH/USDC".to_string()),
                        pair: None,
                        cursor: None,
                        sort,
                        start_ms: None,
                        end_ms: None,
                        page_size: Some(5),
                    },
                    2,
                )
                .await
                .map(|_| ()),
            "perp wallet orders" => client
                .get_perp_wallet_order_pages(
                    &base_url,
                    &DeepXPerpWalletOrdersRequest {
                        wallet: "0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string(),
                        market_name: None,
                        market_id: None,
                        is_long: None,
                        cursor: None,
                        sort,
                        start_ms: None,
                        end_ms: None,
                        page_size: Some(5),
                    },
                    2,
                )
                .await
                .map(|_| ()),
            _ => unreachable!(),
        };
        assert!(matches!(
            result,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: returned_endpoint,
                ..
            }) if returned_endpoint == endpoint
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[rstest]
    #[case::complete("complete")]
    #[case::duplicate("duplicate")]
    #[case::reversed_time("reversed-time")]
    #[case::changed_market_name("changed-market-name")]
    #[case::missing_group("missing-group")]
    #[case::repeated_cursor("repeated-cursor")]
    #[case::budget("budget")]
    #[tokio::test]
    async fn perp_wallet_trade_pages_follow_only_selected_group(#[case] outcome: &str) {
        let mut first: Value = serde_json::from_str(PERP_TRADES).unwrap();
        first["data"].as_array_mut().unwrap().truncate(1);
        let selected = "0x977654069311fa41b88f7901dc65fae9a76fd60b";
        let cursor = first["data"][0]["subaccounts"][2]["trades"]["nextCursor"]
            .as_str()
            .unwrap()
            .to_string();
        let mut terminal = first.clone();
        let group = &mut terminal["data"][0]["subaccounts"][2];
        for (index, trade) in group["trades"]["items"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
        {
            trade["id"] = json!(52_780_136_000_283_u64 - index as u64 * 9);
        }
        group["trades"]["nextCursor"] = Value::Null;
        group["trades"]["hasNext"] = json!(false);
        match outcome {
            "duplicate" => {
                group["trades"]["items"][0]["id"] =
                    first["data"][0]["subaccounts"][2]["trades"]["items"][4]["id"].clone();
            }
            "reversed-time" => {
                group["trades"]["items"][0]["createdAt"] = json!("2026-06-01T00:00:00Z");
            }
            "changed-market-name" => {
                terminal["data"][0]["marketName"] = json!("OTHER-USDC");
            }
            "missing-group" => {
                terminal["data"][0]["subaccounts"]
                    .as_array_mut()
                    .unwrap()
                    .remove(2);
            }
            "repeated-cursor" => group["trades"]["nextCursor"] = json!(cursor.clone()),
            "complete" | "budget" => {}
            _ => unreachable!(),
        }
        if outcome == "repeated-cursor" {
            terminal["data"][0]["subaccounts"][2]["trades"]["hasNext"] = json!(true);
        }
        let router = Router::new().route(
            "/internal/v1/account/perp/trades-by-wallet",
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
        let request = DeepXPerpWalletTradesRequest {
            wallet: "0x781ed35b167068c93dfadab41dfb680edaca4e50".to_string(),
            market_name: None,
            market_id: Some(3),
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        };
        let result = client
            .get_perp_wallet_trade_pages_for_subaccount(
                &base_url,
                &request,
                selected,
                "ETH-USDC",
                if outcome == "budget" { 1 } else { 2 },
            )
            .await;
        match outcome {
            "complete" => {
                let pages = result.unwrap();
                assert_eq!(pages.len(), 2);
                assert_eq!(pages[0].items.len(), 5);
                assert_eq!(pages[1].items.len(), 5);
                assert_eq!(pages[1].items[0].id, 52_780_136_000_283);
                assert!(!pages[1].has_next);
            }
            "repeated-cursor" => assert!(matches!(
                result,
                Err(DeepXRestError::RepeatedPaginationCursor { .. })
            )),
            "budget" => assert!(matches!(
                result,
                Err(DeepXRestError::PaginationLimitExceeded { .. })
            )),
            _ => assert!(matches!(
                result,
                Err(DeepXRestError::InvalidHistoryResponse { .. })
            )),
        }
    }
}
