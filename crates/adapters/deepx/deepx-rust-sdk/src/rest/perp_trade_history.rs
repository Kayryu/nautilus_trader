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

//! Complete and record-limited perpetual trade history.

use std::{collections::HashSet, future::Future, num::NonZeroUsize};

use jiff::Timestamp;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::DeepXPerpTradesHistoryRequest,
    rest::{
        DeepXRestError, DeepXRestTransport,
        models::{DeepXPerpTrade, DeepXPerpTradesPage},
    },
};

/// Collects perpetual trades in descending order within the requested inclusive bounds.
///
/// An optional record limit succeeds without reading later pages.
///
/// # Errors
///
/// Returns a request, page-read, trade-validation, or pagination error.
pub async fn collect_perp_trade_history<E, F, Fut>(
    request: &DeepXPerpTradesHistoryRequest,
    limit: Option<NonZeroUsize>,
    mut fetch_page: F,
) -> Result<Vec<DeepXPerpTrade>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpTradesHistoryRequest, Option<String>) -> Fut,
    Fut: Future<Output = Result<DeepXPerpTradesPage, E>>,
{
    request.validate().map_err(E::from)?;
    let start = Timestamp::from_millisecond(request.start_ms as i64)
        .map_err(|e| E::from(DeepXRestError::InvalidRequest(e.to_string())))?;
    let end = Timestamp::from_millisecond(request.end_ms as i64)
        .map_err(|e| E::from(DeepXRestError::InvalidRequest(e.to_string())))?;
    let mut pagination = CursorPagination::new(request.max_pages).map_err(E::from)?;
    let mut cursor = None;
    let mut trades = Vec::new();
    let mut ids = HashSet::new();
    let mut previous = None;
    let invalid = |message: String| {
        E::from(DeepXRestError::InvalidHistoryResponse {
            endpoint: "perp trades",
            message,
        })
    };
    loop {
        let mut page_request = request.clone();
        if let Some(limit) = limit {
            page_request.page_size = request
                .page_size
                .min(u32::try_from(limit.get() - trades.len()).unwrap_or(u32::MAX));
        }
        let page = fetch_page(page_request, cursor.clone()).await?;
        for trade in &page.items {
            let timestamp = trade
                .created_at
                .parse::<Timestamp>()
                .map_err(|e| invalid(format!("trade {} has invalid timestamp: {e}", trade.id)))?;
            if timestamp < start || timestamp > end {
                return Err(invalid(format!(
                    "trade {} is outside the requested range",
                    trade.id
                )));
            }
            if previous.is_some_and(|time| timestamp > time) {
                return Err(invalid(format!(
                    "trade {} timestamp is not descending",
                    trade.id
                )));
            }
            if !ids.insert(trade.id) {
                return Err(invalid(format!("duplicate trade ID {}", trade.id)));
            }
            previous = Some(timestamp);
        }
        if limit.is_some_and(|limit| trades.len() + page.items.len() == limit.get()) {
            trades.extend(page.items);
            return Ok(trades);
        }
        let decision = pagination
            .observe_response_page(
                "perp trades",
                page.items.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        trades.extend(page.items);
        match decision {
            PaginationDecision::Complete => return Ok(trades),
            PaginationDecision::Continue(next) => cursor = Some(next),
        }
    }
}

impl DeepXRestTransport {
    /// Returns complete perpetual trade history from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, trade-validation, or pagination error.
    pub async fn get_perp_trades_history(
        &self,
        base_url: &str,
        request: &DeepXPerpTradesHistoryRequest,
    ) -> Result<Vec<DeepXPerpTrade>, DeepXRestError> {
        collect_perp_trade_history(request, None, |page_request, cursor| async move {
            let query = page_request.as_query(cursor.as_deref());
            self.get_perp_trades(base_url, &query).await
        })
        .await
    }

    /// Returns record-limited perpetual trade history from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, trade-validation, or pagination error.
    pub async fn get_perp_trades_history_limited(
        &self,
        base_url: &str,
        request: &DeepXPerpTradesHistoryRequest,
        limit: NonZeroUsize,
    ) -> Result<Vec<DeepXPerpTrade>, DeepXRestError> {
        collect_perp_trade_history(request, Some(limit), |page_request, cursor| async move {
            let query = page_request.as_query(cursor.as_deref());
            self.get_perp_trades(base_url, &query).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::{Json, Router, extract::Query, routing::get};
    use rstest::rstest;
    use rust_decimal::Decimal;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;

    fn request() -> DeepXPerpTradesHistoryRequest {
        DeepXPerpTradesHistoryRequest {
            market_id: 3,
            start_ms: 1_000,
            end_ms: 3_000,
            page_size: 2,
            max_pages: 2,
        }
    }

    fn trade(id: u64, timestamp_ms: i64) -> Value {
        json!({
            "id": id,
            "marketId": 3,
            "buyerOrderId": "65043",
            "buyer": "0x11",
            "sellerOrderId": "65044",
            "seller": "0x22",
            "price": "1792.600000000000001",
            "size": "0.004000000000000001",
            "buyerLeverage": "25",
            "sellerLeverage": "12.500000000000000001",
            "createdAt": Timestamp::from_millisecond(timestamp_ms).unwrap().to_string(),
            "filledDirection": "Short",
            "taker": "Seller",
            "takerFee": "0.000716",
            "makerFee": "-0.000716"
        })
    }

    fn page(items: &[Value], has_next: bool) -> DeepXPerpTradesPage {
        serde_json::from_value(json!({
            "items": items,
            "hasNext": has_next,
            "nextCursor": if has_next { Some("next-page") } else { None },
        }))
        .unwrap()
    }

    #[rstest]
    #[tokio::test]
    async fn limited_history_shrinks_next_page_and_preserves_exact_prices() {
        let mut pages = [
            page(&[trade(3, 3_000), trade(2, 2_000)], true),
            page(&[trade(1, 1_000)], false),
        ]
        .into_iter();
        let mut requests = Vec::new();
        let trades = collect_perp_trade_history(
            &request(),
            Some(NonZeroUsize::new(3).unwrap()),
            |page_request, cursor| {
                requests.push((page_request.page_size, cursor));
                std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
            },
        )
        .await
        .unwrap();
        assert_eq!(requests, [(2, None), (1, Some("next-page".to_string()))]);
        assert_eq!(trades.len(), 3);
        assert_eq!(
            trades[0].price,
            "1792.600000000000001".parse::<Decimal>().unwrap()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn complete_history_rejects_duplicate_id_across_pages() {
        let mut pages = [
            page(&[trade(2, 3_000)], true),
            page(&[trade(2, 2_000)], false),
        ]
        .into_iter();
        assert!(matches!(
            collect_perp_trade_history(&request(), None, |_, _| {
                std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
            })
            .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp trades",
                ..
            })
        ));
    }

    #[rstest]
    #[tokio::test]
    async fn complete_history_fails_when_page_budget_is_exhausted() {
        let mut request = request();
        request.max_pages = 1;
        let first = page(&[trade(2, 3_000)], true);
        assert!(matches!(
            collect_perp_trade_history(&request, None, |_, _| {
                std::future::ready(Ok::<_, DeepXRestError>(first.clone()))
            })
            .await,
            Err(DeepXRestError::PaginationLimitExceeded { max_pages: 1 })
        ));
    }

    #[rstest]
    #[case("market")]
    #[case("range")]
    #[case("page-size")]
    #[case("page-budget")]
    #[case("overflow")]
    #[case("timestamp")]
    #[tokio::test]
    async fn history_rejects_invalid_bounds_and_budgets_before_transport(#[case] invalid: &str) {
        let calls = std::cell::Cell::new(0);
        let mut request = request();
        match invalid {
            "market" => request.market_id = 0,
            "range" => request.start_ms = 4_000,
            "page-size" => request.page_size = 0,
            "page-budget" => request.max_pages = 0,
            "overflow" => request.max_pages = usize::MAX,
            "timestamp" => request.end_ms = u64::MAX,
            _ => unreachable!(),
        }
        let error = collect_perp_trade_history(&request, None, |_, _| {
            calls.set(calls.get() + 1);
            std::future::ready(Ok::<_, DeepXRestError>(page(&[], false)))
        })
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            DeepXRestError::InvalidRequest(_) | DeepXRestError::InvalidPaginationLimit
        ));
        assert_eq!(calls.get(), 0);
    }

    #[rstest]
    #[case("valid")]
    #[case("equal-times")]
    #[case("empty-terminal")]
    #[case("stale-terminal-cursor")]
    #[case("duplicate")]
    #[case("ascending")]
    #[case("before-start")]
    #[case("after-end")]
    #[case("invalid-time")]
    #[case("foreign-market")]
    #[case("oversized")]
    #[case("repeat-cursor")]
    #[case("missing-cursor")]
    #[case("no-progress")]
    #[case("page-budget")]
    #[tokio::test]
    async fn transport_history_checks_every_page(#[case] scenario: &'static str) {
        let router = Router::new().route(
            "/internal/v1/market/perp/trades",
            get(
                move |Query(query): Query<HashMap<String, String>>| async move {
                    assert_eq!(query["marketId"], "3");
                    assert_eq!(query["start"], "1000");
                    assert_eq!(query["end"], "3000");
                    assert_eq!(query["pageSize"], "2");
                    assert_eq!(query["sort"], "DESC");
                    let first = !query.contains_key("cursor");
                    if !first {
                        assert_eq!(query["cursor"], "opaque+/cursor=");
                    }
                    let mut items = vec![trade(
                        if first { 2 } else { 1 },
                        if first {
                            if scenario == "ascending" {
                                2_000
                            } else {
                                3_000
                            }
                        } else {
                            1_000
                        },
                    )];
                    let mut has_next = first;
                    let mut cursor = first.then_some("opaque+/cursor=");
                    if !first {
                        match scenario {
                            "equal-times" => items[0] = trade(1, 3_000),
                            "empty-terminal" => items.clear(),
                            "stale-terminal-cursor" => cursor = Some("opaque+/cursor="),
                            "duplicate" => items[0] = trade(2, 1_000),
                            "ascending" => items[0] = trade(1, 2_500),
                            "before-start" => items[0] = trade(1, 999),
                            "after-end" => items[0] = trade(1, 3_001),
                            "invalid-time" => items[0]["createdAt"] = json!("not-a-timestamp"),
                            "foreign-market" => items[0]["marketId"] = json!(4),
                            "oversized" => {
                                items.push(trade(3, 1_000));
                                items.push(trade(4, 1_000));
                            }
                            "repeat-cursor" => {
                                has_next = true;
                                cursor = Some("opaque+/cursor=");
                            }
                            "missing-cursor" => has_next = true,
                            "no-progress" => {
                                items.clear();
                                has_next = true;
                                cursor = Some("next");
                            }
                            _ => {}
                        }
                    }
                    Json(json!({
                        "code": 200,
                        "fail": false,
                        "msg": "success",
                        "data": {"items": items, "hasNext": has_next, "nextCursor": cursor},
                    }))
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRestTransport::new(Some(5), None).unwrap();
        let mut request = request();
        if scenario == "page-budget" {
            request.max_pages = 1;
        }

        let result = client.get_perp_trades_history(&base_url, &request).await;
        match scenario {
            "valid" | "equal-times" | "empty-terminal" | "stale-terminal-cursor" => {
                let trades = result.unwrap();
                assert_eq!(
                    trades.len(),
                    if scenario == "empty-terminal" { 1 } else { 2 }
                );
                assert_eq!(trades[0].id, 2);
                assert_eq!(
                    trades[0].price,
                    "1792.600000000000001".parse::<Decimal>().unwrap()
                );
            }
            "repeat-cursor" => assert!(matches!(
                result,
                Err(DeepXRestError::RepeatedPaginationCursor { .. })
            )),
            "missing-cursor" => assert!(matches!(
                result,
                Err(DeepXRestError::MissingPaginationCursor { .. })
            )),
            "no-progress" => assert!(matches!(
                result,
                Err(DeepXRestError::PaginationNoProgress { .. })
            )),
            "page-budget" => assert!(matches!(
                result,
                Err(DeepXRestError::PaginationLimitExceeded { max_pages: 1 })
            )),
            "foreign-market" => assert!(matches!(
                result,
                Err(DeepXRestError::ResponseMarketMismatch {
                    expected: 3,
                    received: 4,
                    ..
                })
            )),
            _ => assert!(matches!(
                result,
                Err(DeepXRestError::InvalidHistoryResponse { .. })
            )),
        }
        server.abort();
    }
}
