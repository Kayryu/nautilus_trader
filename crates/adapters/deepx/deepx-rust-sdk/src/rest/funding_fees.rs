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

//! Complete cursor pagination for perpetual and wallet funding fees.

use std::future::Future;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::{DeepXPerpFundingFeeRequest, DeepXWalletFundingFeeRequest},
    rest::{
        DeepXAccountPage, DeepXFundingFeeContinuity, DeepXRawAccountPage, DeepXRestError,
        DeepXRestTransport, models::DeepXPerpFundingFeeRecord,
    },
};

/// Collects raw perpetual funding-fee pages without decoding their item payloads.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_perp_funding_fee_pages_raw<E, F, Fut>(
    request: &DeepXPerpFundingFeeRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpFundingFeeRequest) -> Fut,
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
                "perp funding fees",
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

/// Collects validated perpetual funding-fee pages with cross-page event identity checks.
///
/// # Errors
///
/// Returns a request, page-read, event-continuity, or pagination error.
pub async fn collect_perp_funding_fee_pages<E, F, Fut>(
    request: &DeepXPerpFundingFeeRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXPerpFundingFeeRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXPerpFundingFeeRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXPerpFundingFeeRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXFundingFeeContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        continuity
            .observe_page("perp funding fees", &page)
            .map_err(E::from)?;
        let decision = pagination
            .observe_response_page(
                "perp funding fees",
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

/// Collects raw wallet funding-fee pages without decoding their item payloads.
///
/// # Errors
///
/// Returns a request, page-read, or pagination error.
pub async fn collect_wallet_funding_fee_pages_raw<E, F, Fut>(
    request: &DeepXWalletFundingFeeRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXRawAccountPage>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXWalletFundingFeeRequest) -> Fut,
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
                "wallet funding fees",
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

/// Collects validated wallet funding-fee pages with cross-page event identity checks.
///
/// # Errors
///
/// Returns a request, page-read, event-continuity, or pagination error.
pub async fn collect_wallet_funding_fee_pages<E, F, Fut>(
    request: &DeepXWalletFundingFeeRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXAccountPage<DeepXPerpFundingFeeRecord>>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXWalletFundingFeeRequest) -> Fut,
    Fut: Future<Output = Result<DeepXAccountPage<DeepXPerpFundingFeeRecord>, E>>,
{
    request.validate().map_err(E::from)?;
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut next_request = request.clone();
    let mut continuity = DeepXFundingFeeContinuity::default();
    let mut pages = Vec::new();
    loop {
        let page = fetch_page(next_request.clone()).await?;
        continuity
            .observe_page("wallet funding fees", &page)
            .map_err(E::from)?;
        let decision = pagination
            .observe_response_page(
                "wallet funding fees",
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
    /// Returns complete raw perpetual funding-fee pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_funding_fee_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpFundingFeeRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_perp_funding_fee_pages_raw(request, max_pages, |page_request| async move {
            self.get_perp_funding_fees_raw(base_url, &page_request)
                .await
        })
        .await
    }

    /// Returns complete validated perpetual funding-fee pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_perp_funding_fee_pages(
        &self,
        base_url: &str,
        request: &DeepXPerpFundingFeeRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpFundingFeeRecord>>, DeepXRestError> {
        collect_perp_funding_fee_pages(request, max_pages, |page_request| async move {
            self.get_perp_funding_fees(base_url, &page_request).await
        })
        .await
    }

    /// Returns complete raw wallet funding-fee pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_wallet_funding_fee_pages_raw(
        &self,
        base_url: &str,
        request: &DeepXWalletFundingFeeRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXRawAccountPage>, DeepXRestError> {
        collect_wallet_funding_fee_pages_raw(request, max_pages, |page_request| async move {
            self.get_wallet_funding_fees_raw(base_url, &page_request)
                .await
        })
        .await
    }

    /// Returns complete validated wallet funding-fee pages from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or pagination error.
    pub async fn get_wallet_funding_fee_pages(
        &self,
        base_url: &str,
        request: &DeepXWalletFundingFeeRequest,
        max_pages: usize,
    ) -> Result<Vec<DeepXAccountPage<DeepXPerpFundingFeeRecord>>, DeepXRestError> {
        collect_wallet_funding_fee_pages(request, max_pages, |page_request| async move {
            self.get_wallet_funding_fees(base_url, &page_request).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::Value;

    use super::*;

    const PERP_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_funding_fees_account_market_3.json");

    fn perp_request() -> DeepXPerpFundingFeeRequest {
        DeepXPerpFundingFeeRequest {
            subaccount: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_id: Some(3),
            start_ms: None,
            end_ms: None,
            cursor: None,
            page_size: Some(1),
        }
    }

    fn wallet_request() -> DeepXWalletFundingFeeRequest {
        DeepXWalletFundingFeeRequest {
            wallet: "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc".to_string(),
            market_name: None,
            market_id: Some(3),
            start_ms: None,
            end_ms: None,
            cursor: None,
            page_size: Some(1),
        }
    }

    fn funding_page() -> DeepXAccountPage<DeepXPerpFundingFeeRecord> {
        let response: Value = serde_json::from_str(PERP_RESPONSE).unwrap();
        serde_json::from_value(response["data"].clone()).unwrap()
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn perp_collector_checks_event_identity_across_pages(#[case] duplicate: bool) {
        let mut first = funding_page();
        first.items.truncate(1);
        first.has_next = true;
        first.next_cursor = Some("next-page".to_string());
        let mut second = first.clone();
        second.has_next = false;
        second.next_cursor = None;
        if !duplicate {
            second.items[0].event_idx += 1;
        }
        let mut pages = [first, second].into_iter();
        let mut cursors = Vec::new();
        let result = collect_perp_funding_fee_pages(&perp_request(), 2, |request| {
            cursors.push(request.cursor);
            std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
        })
        .await;
        assert_eq!(cursors, [None, Some("next-page".to_string())]);
        if duplicate {
            assert!(matches!(
                result,
                Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint: "perp funding fees",
                    ..
                })
            ));
        } else {
            assert_eq!(result.unwrap().len(), 2);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn wallet_collector_rejects_repeated_event_identity() {
        let mut first = funding_page();
        first.items.truncate(1);
        first.has_next = true;
        first.next_cursor = Some("next-page".to_string());
        let mut second = first.clone();
        second.has_next = false;
        second.next_cursor = None;
        let mut pages = [first, second].into_iter();
        assert!(matches!(
            collect_wallet_funding_fee_pages(&wallet_request(), 2, |_| {
                std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
            })
            .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "wallet funding fees",
                ..
            })
        ));
    }

    #[rstest]
    #[tokio::test]
    async fn raw_collector_preserves_numeric_lexemes_and_cursor() {
        let first: DeepXRawAccountPage = serde_json::from_str(
            r#"{"items":[{"fee":1.2300}],"hasNext":true,"nextCursor":"next-page"}"#,
        )
        .unwrap();
        let second: DeepXRawAccountPage =
            serde_json::from_str(r#"{"items":[{"fee":2.0000}],"hasNext":false,"nextCursor":null}"#)
                .unwrap();
        let mut pages = [first, second].into_iter();
        let result = collect_wallet_funding_fee_pages_raw(&wallet_request(), 2, |request| {
            assert_eq!(request.cursor.is_some(), pages.len() == 1);
            std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
        })
        .await
        .unwrap();
        assert_eq!(result[0].items[0].get(), r#"{"fee":1.2300}"#);
        assert_eq!(result[1].items[0].get(), r#"{"fee":2.0000}"#);
    }
}
