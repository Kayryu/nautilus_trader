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

//! Bounded perpetual funding-rate and long-short ratio history.

use std::{future::Future, num::NonZeroUsize};

use rust_decimal::Decimal;

use crate::{
    pagination::{CursorPagination, PaginationDecision},
    query::{
        DEEPX_MAX_HISTORY_PAGE_SIZE, DeepXFundingRateRequest, DeepXLongShortRatioRequest,
        DeepXOpenInterestRequest,
    },
    rest::{
        DeepXRestError, DeepXRestTransport,
        models::{
            DeepXFundingRatePage, DeepXFundingRateRecord, DeepXLongShortRatioPage,
            DeepXLongShortRatioRecord, DeepXOpenInterestPage,
        },
    },
};

/// Collects descending one-minute funding rates within an explicit record and page budget.
///
/// # Errors
///
/// Returns a request, page-read, record-validation, or pagination error.
pub async fn collect_perp_funding_rates_history_limited<E, F, Fut>(
    request: &DeepXFundingRateRequest,
    limit: NonZeroUsize,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXFundingRateRecord>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXFundingRateRequest) -> Fut,
    Fut: Future<Output = Result<DeepXFundingRatePage, E>>,
{
    request.validate().map_err(E::from)?;
    let end = request.end_ms.ok_or_else(|| {
        E::from(DeepXRestError::InvalidRequest(
            "funding-rate history requires a fixed end_ms".to_string(),
        ))
    })?;
    let page_size = request.limit.unwrap_or(100);
    if page_size > DEEPX_MAX_HISTORY_PAGE_SIZE {
        return Err(E::from(DeepXRestError::InvalidRequest(
            "funding-rate page size exceeds 5000".to_string(),
        )));
    }
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut page_request = request.clone();
    let mut records = Vec::new();
    let mut previous = None;
    let invalid = |message: String| {
        E::from(DeepXRestError::InvalidHistoryResponse {
            endpoint: "perp funding-rate",
            message,
        })
    };
    loop {
        let current_limit =
            page_size.min(u32::try_from(limit.get() - records.len()).unwrap_or(u32::MAX));
        page_request.limit = Some(current_limit);
        let page = fetch_page(page_request.clone()).await?;
        if page.details.len() > current_limit as usize {
            return Err(invalid("page exceeds requested limit".to_string()));
        }
        for record in &page.details {
            if !record.time.is_multiple_of(60_000) {
                return Err(invalid(
                    "funding sample is not a UTC minute bucket".to_string(),
                ));
            }
            if record.time < request.start_ms || record.time > end {
                return Err(invalid(
                    "funding sample is outside requested range".to_string(),
                ));
            }
            if previous.is_some_and(|time| record.time >= time) {
                return Err(invalid(
                    "funding sample buckets are not strictly descending".to_string(),
                ));
            }
            previous = Some(record.time);
        }
        if records.len() + page.details.len() == limit.get() {
            records.extend(page.details);
            return Ok(records);
        }
        let decision = pagination
            .observe_response_page(
                "perp funding-rate",
                page.details.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        records.extend(page.details);
        match decision {
            PaginationDecision::Complete => return Ok(records),
            PaginationDecision::Continue(cursor) => page_request.cursor = Some(cursor),
        }
    }
}

/// Collects ascending long-short ratios within an explicit record and page budget.
///
/// # Errors
///
/// Returns a request, page-read, record-validation, or pagination error.
pub async fn collect_perp_long_short_ratios_history_limited<E, F, Fut>(
    request: &DeepXLongShortRatioRequest,
    limit: NonZeroUsize,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Vec<DeepXLongShortRatioRecord>, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXLongShortRatioRequest) -> Fut,
    Fut: Future<Output = Result<DeepXLongShortRatioPage, E>>,
{
    request.validate().map_err(E::from)?;
    let end = request.end_ms.ok_or_else(|| {
        E::from(DeepXRestError::InvalidRequest(
            "long-short-ratio history requires a fixed end_ms".to_string(),
        ))
    })?;
    let page_size = request.limit.unwrap_or(100);
    if page_size > DEEPX_MAX_HISTORY_PAGE_SIZE {
        return Err(E::from(DeepXRestError::InvalidRequest(
            "long-short-ratio page size exceeds 5000".to_string(),
        )));
    }
    let mut pagination =
        CursorPagination::new_with_cursor(max_pages, request.cursor.as_deref()).map_err(E::from)?;
    let mut page_request = request.clone();
    let mut records = Vec::new();
    let mut previous = None;
    let invalid = |message: String| {
        E::from(DeepXRestError::InvalidHistoryResponse {
            endpoint: "perp long-short-ratio",
            message,
        })
    };
    loop {
        let current_limit =
            page_size.min(u32::try_from(limit.get() - records.len()).unwrap_or(u32::MAX));
        page_request.limit = Some(current_limit);
        let page = fetch_page(page_request.clone()).await?;
        if page.details.len() > current_limit as usize {
            return Err(invalid("page exceeds requested limit".to_string()));
        }
        for record in &page.details {
            if record.time < request.start_ms || record.time > end {
                return Err(invalid(
                    "observation is outside requested range".to_string(),
                ));
            }
            if previous.is_some_and(|time| record.time <= time) {
                return Err(invalid(
                    "observation times are not strictly ascending".to_string(),
                ));
            }
            previous = Some(record.time);
        }
        if records.len() + page.details.len() == limit.get() {
            records.extend(page.details);
            return Ok(records);
        }
        let decision = pagination
            .observe_response_page(
                "perp long-short-ratio",
                page.details.len(),
                page.has_next,
                page.next_cursor.as_deref(),
            )
            .map_err(E::from)?;
        records.extend(page.details);
        match decision {
            PaginationDecision::Complete => return Ok(records),
            PaginationDecision::Continue(cursor) => page_request.cursor = Some(cursor),
        }
    }
}

/// Collects complete ascending open-interest history using one-minute time cursors.
///
/// # Errors
///
/// Returns a request, page-read, record-validation, or page-budget error.
pub async fn collect_perp_open_interest_history<E, F, Fut>(
    request: &DeepXOpenInterestRequest,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<DeepXOpenInterestPage, E>
where
    E: From<DeepXRestError>,
    F: FnMut(DeepXOpenInterestRequest) -> Fut,
    Fut: Future<Output = Result<DeepXOpenInterestPage, E>>,
{
    request.validate().map_err(E::from)?;
    let end_ms = request.end_ms.ok_or_else(|| {
        E::from(DeepXRestError::InvalidRequest(
            "open-interest history requires end_ms".to_string(),
        ))
    })?;
    if max_pages == 0 {
        return Err(E::from(DeepXRestError::InvalidRequest(
            "open-interest history requires a positive page budget".to_string(),
        )));
    }
    let page_size = request.limit.unwrap_or(100) as usize;
    if page_size > DEEPX_MAX_HISTORY_PAGE_SIZE as usize {
        return Err(E::from(DeepXRestError::InvalidRequest(
            "open-interest page size exceeds 5000".to_string(),
        )));
    }
    let next_minute = |timestamp: u64| {
        timestamp
            .checked_div(60_000)
            .and_then(|minute| minute.checked_add(1))
            .and_then(|minute| minute.checked_mul(60_000))
            .ok_or_else(|| {
                E::from(DeepXRestError::InvalidRequest(
                    "open-interest time bound overflows minute range".to_string(),
                ))
            })
    };
    let first_minute = if request.start_ms.is_multiple_of(60_000) {
        request.start_ms
    } else {
        next_minute(request.start_ms)?
    };
    if first_minute > end_ms {
        return Err(E::from(DeepXRestError::InvalidRequest(
            "open-interest history range contains no minute bucket".to_string(),
        )));
    }
    let venue_end = next_minute(end_ms)?;
    let mut page_request = request.clone();
    page_request.start_ms = first_minute;
    page_request.end_ms = Some(venue_end);
    page_request.limit = Some(page_size as u32);
    let mut pair = None;
    let mut details = Vec::new();
    let mut previous_time = None;
    for _ in 0..max_pages {
        let page = fetch_page(page_request.clone()).await?;
        let invalid = |message| {
            E::from(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp open-interest",
                message,
            })
        };
        if page.pair.trim().is_empty()
            || pair.as_ref().is_some_and(|expected| expected != &page.pair)
        {
            return Err(invalid("market pair changed or is empty".to_string()));
        }
        if page.details.len() > page_size {
            return Err(invalid("page exceeds requested limit".to_string()));
        }
        if pair.is_none() {
            pair = Some(page.pair);
        }
        for record in &page.details {
            if !record.statistic_time.is_multiple_of(60_000)
                || record.statistic_time < page_request.start_ms
                || record.statistic_time < request.start_ms
                || record.statistic_time > end_ms
                || previous_time.is_some_and(|previous| record.statistic_time <= previous)
                || record.total_oi < Decimal::ZERO
                || record.long_short_ratio < Decimal::ZERO
            {
                return Err(invalid(
                    "invalid or out-of-order open-interest record".to_string(),
                ));
            }
            previous_time = Some(record.statistic_time);
        }
        let page_len = page.details.len();
        let last_time = page.details.last().map(|record| record.statistic_time);
        details.extend(page.details);
        if page_len < page_size || last_time == Some(end_ms) {
            return Ok(DeepXOpenInterestPage {
                pair: pair.ok_or_else(|| invalid("missing market pair".to_string()))?,
                details,
            });
        }
        let next_start = last_time
            .ok_or_else(|| invalid("full page has no last record".to_string()))?
            .checked_add(60_000)
            .ok_or_else(|| invalid("open-interest cursor overflows".to_string()))?;
        if next_start > end_ms {
            return Ok(DeepXOpenInterestPage {
                pair: pair.ok_or_else(|| invalid("missing market pair".to_string()))?,
                details,
            });
        }
        page_request.start_ms = next_start;
    }
    Err(E::from(DeepXRestError::InvalidRequest(
        "open-interest history page budget exceeded".to_string(),
    )))
}

impl DeepXRestTransport {
    /// Returns bounded descending funding-rate history from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, record-validation, or pagination error.
    pub async fn get_perp_funding_rates_history_limited(
        &self,
        base_url: &str,
        request: &DeepXFundingRateRequest,
        limit: NonZeroUsize,
        max_pages: usize,
    ) -> Result<Vec<DeepXFundingRateRecord>, DeepXRestError> {
        collect_perp_funding_rates_history_limited(
            request,
            limit,
            max_pages,
            |page_request| async move {
                self.get_perp_funding_rates_descending(base_url, &page_request)
                    .await
            },
        )
        .await
    }

    /// Returns bounded ascending long-short ratio history from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, record-validation, or pagination error.
    pub async fn get_perp_long_short_ratios_history_limited(
        &self,
        base_url: &str,
        request: &DeepXLongShortRatioRequest,
        limit: NonZeroUsize,
        max_pages: usize,
    ) -> Result<Vec<DeepXLongShortRatioRecord>, DeepXRestError> {
        collect_perp_long_short_ratios_history_limited(
            request,
            limit,
            max_pages,
            |page_request| async move {
                self.get_perp_long_short_ratios(base_url, &page_request)
                    .await
            },
        )
        .await
    }

    /// Returns complete ascending open-interest history from one REST endpoint.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, record-validation, or page-budget error.
    pub async fn get_perp_open_interest_history(
        &self,
        base_url: &str,
        request: &DeepXOpenInterestRequest,
        max_pages: usize,
    ) -> Result<DeepXOpenInterestPage, DeepXRestError> {
        collect_perp_open_interest_history(request, max_pages, |page_request| async move {
            self.get_perp_open_interest(base_url, &page_request).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::*;

    fn funding_request() -> DeepXFundingRateRequest {
        DeepXFundingRateRequest {
            market_id: 3,
            start_ms: 60_000,
            end_ms: Some(180_000),
            limit: Some(2),
            cursor: None,
        }
    }

    fn ratio_request() -> DeepXLongShortRatioRequest {
        DeepXLongShortRatioRequest {
            market_id: 3,
            start_ms: 60_000,
            end_ms: Some(180_000),
            limit: Some(2),
            cursor: None,
        }
    }

    #[rstest]
    #[tokio::test]
    async fn funding_history_follows_cursor_and_preserves_exact_rates() {
        let first: DeepXFundingRatePage = serde_json::from_str(
            r#"{"marketId":3,"details":[{"time":180000,"fundingRate":"0.000012500000000000001"},{"time":120000,"fundingRate":"-0.00005"}],"hasNext":true,"nextCursor":"next-page"}"#,
        )
        .unwrap();
        let second: DeepXFundingRatePage = serde_json::from_str(
            r#"{"marketId":3,"details":[{"time":60000,"fundingRate":"0"}],"hasNext":false,"nextCursor":null}"#,
        )
        .unwrap();
        let mut pages = [first, second].into_iter();
        let mut requests = Vec::new();
        let records = collect_perp_funding_rates_history_limited(
            &funding_request(),
            NonZeroUsize::new(3).unwrap(),
            2,
            |request| {
                requests.push((request.cursor, request.limit));
                std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
            },
        )
        .await
        .unwrap();
        assert_eq!(
            requests,
            [(None, Some(2)), (Some("next-page".to_string()), Some(1))]
        );
        assert_eq!(records.len(), 3);
        assert_eq!(
            records[0].funding_rate,
            "0.000012500000000000001".parse::<Decimal>().unwrap()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn funding_history_rejects_non_minute_bucket() {
        let page: DeepXFundingRatePage = serde_json::from_str(
            r#"{"marketId":3,"details":[{"time":120001,"fundingRate":"0.1"}],"hasNext":false,"nextCursor":null}"#,
        )
        .unwrap();
        assert!(matches!(
            collect_perp_funding_rates_history_limited(
                &funding_request(),
                NonZeroUsize::new(1).unwrap(),
                1,
                |_| std::future::ready(Ok::<_, DeepXRestError>(page.clone())),
            )
            .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp funding-rate",
                ..
            })
        ));
    }

    #[rstest]
    #[tokio::test]
    async fn ratio_history_rejects_non_ascending_boundary() {
        let first: DeepXLongShortRatioPage = serde_json::from_str(
            r#"{"marketId":3,"details":[{"time":60000,"longShortRatio":"1.000000000000000001"}],"hasNext":true,"nextCursor":"next-page"}"#,
        )
        .unwrap();
        let mut second = first.clone();
        second.has_next = false;
        second.next_cursor = None;
        let mut pages = [first, second].into_iter();
        assert!(matches!(
            collect_perp_long_short_ratios_history_limited(
                &ratio_request(),
                NonZeroUsize::new(2).unwrap(),
                2,
                |_| std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap())),
            )
            .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp long-short-ratio",
                ..
            })
        ));
    }

    fn open_interest_page(pair: &str, times: &[u64]) -> DeepXOpenInterestPage {
        serde_json::from_value(json!({
            "pair": pair,
            "details": times.iter().map(|time| json!({
                "total_oi": "3393.618",
                "long_short_ratio": "1.8456014362657092",
                "long_position_count": 2056,
                "short_position_count": 1114,
                "statistic_time": time,
            })).collect::<Vec<_>>(),
        }))
        .unwrap()
    }

    #[rstest]
    #[tokio::test]
    async fn open_interest_history_advances_minute_cursor_and_keeps_decimals() {
        let request = DeepXOpenInterestRequest {
            market_id: 3,
            start_ms: 60_000,
            end_ms: Some(240_000),
            limit: Some(2),
        };
        let mut pages = [
            open_interest_page("ETH-USDC", &[60_000, 120_000]),
            open_interest_page("ETH-USDC", &[180_000, 240_000]),
        ]
        .into_iter();
        let mut bounds = Vec::new();
        let history = collect_perp_open_interest_history(&request, 2, |page_request| {
            bounds.push((page_request.start_ms, page_request.end_ms));
            std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
        })
        .await
        .unwrap();
        assert_eq!(bounds, [(60_000, Some(300_000)), (180_000, Some(300_000))]);
        assert_eq!(history.details.len(), 4);
        assert_eq!(history.details[0].total_oi, Decimal::new(3_393_618, 3));
    }

    #[rstest]
    #[tokio::test]
    async fn open_interest_history_rejects_pair_change() {
        let request = DeepXOpenInterestRequest {
            market_id: 3,
            start_ms: 60_000,
            end_ms: Some(240_000),
            limit: Some(2),
        };
        let mut pages = [
            open_interest_page("ETH-USDC", &[60_000, 120_000]),
            open_interest_page("BTC-USDC", &[180_000]),
        ]
        .into_iter();
        assert!(matches!(
            collect_perp_open_interest_history(&request, 2, |_| {
                std::future::ready(Ok::<_, DeepXRestError>(pages.next().unwrap()))
            })
            .await,
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp open-interest",
                ..
            })
        ));
    }
}
