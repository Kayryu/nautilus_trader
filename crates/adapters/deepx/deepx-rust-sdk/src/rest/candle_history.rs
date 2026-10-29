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

//! Bounded time-cursor collection for DeepX candle and price histories.

use std::future::Future;

use crate::{
    query::DEEPX_MAX_HISTORY_PAGE_SIZE,
    rest::{
        DeepXRestError,
        models::{DeepXPerpCandle, DeepXPerpCandlesPage, DeepXSpotCandle, DeepXSpotCandlesPage},
    },
};

trait CandleHistoryRecord {
    fn time_ms(&self) -> u64;
}

impl CandleHistoryRecord for DeepXPerpCandle {
    fn time_ms(&self) -> u64 {
        self.time
    }
}

impl CandleHistoryRecord for DeepXSpotCandle {
    fn time_ms(&self) -> u64 {
        self.time
    }
}

trait CandleHistoryPage<Candle>: Sized {
    fn pair(&self) -> &str;
    fn into_parts(self) -> (String, Vec<Candle>);
    fn from_parts(pair: String, details: Vec<Candle>) -> Self;
}

impl CandleHistoryPage<DeepXPerpCandle> for DeepXPerpCandlesPage {
    fn pair(&self) -> &str {
        &self.pair
    }

    fn into_parts(self) -> (String, Vec<DeepXPerpCandle>) {
        (self.pair, self.details)
    }

    fn from_parts(pair: String, details: Vec<DeepXPerpCandle>) -> Self {
        Self { pair, details }
    }
}

impl CandleHistoryPage<DeepXSpotCandle> for DeepXSpotCandlesPage {
    fn pair(&self) -> &str {
        &self.pair
    }

    fn into_parts(self) -> (String, Vec<DeepXSpotCandle>) {
        (self.pair, self.details)
    }

    fn from_parts(pair: String, details: Vec<DeepXSpotCandle>) -> Self {
        Self { pair, details }
    }
}

async fn collect<Candle, Page, E, F, Fut>(
    endpoint: &'static str,
    start_ms: u64,
    end_ms: Option<u64>,
    total_limit: Option<u32>,
    max_pages: usize,
    mut fetch_page: F,
) -> Result<Page, E>
where
    Candle: CandleHistoryRecord,
    Page: CandleHistoryPage<Candle>,
    E: From<DeepXRestError>,
    F: FnMut(u64, u32) -> Fut,
    Fut: Future<Output = Result<Page, E>>,
{
    if max_pages == 0 {
        return Err(DeepXRestError::InvalidPaginationLimit.into());
    }

    let page_limit = total_limit.unwrap_or(DEEPX_MAX_HISTORY_PAGE_SIZE);
    let mut next_start = start_ms;
    let mut pair = None;
    let mut details = Vec::new();
    for page_index in 0..max_pages {
        let remaining = total_limit.map_or(usize::MAX, |limit| limit as usize - details.len());
        if remaining == 0 {
            break;
        }
        let current_limit = page_limit.min(u32::try_from(remaining).unwrap_or(u32::MAX));
        let page = fetch_page(next_start, current_limit).await?;
        if let Some(expected_pair) = pair.as_ref() {
            if page.pair() != expected_pair {
                return Err(DeepXRestError::InvalidHistoryResponse {
                    endpoint,
                    message: "market identity changed between pages".to_string(),
                }
                .into());
            }
        } else {
            pair = Some(page.pair().to_string());
        }
        let (_, page_details) = page.into_parts();
        let page_len = page_details.len();
        let last_time = page_details.last().map(|candle| candle.time_ms());
        details.extend(page_details);
        if page_len < current_limit as usize
            || total_limit.is_some_and(|limit| details.len() >= limit as usize)
        {
            break;
        }
        let last_time = last_time.ok_or_else(|| DeepXRestError::InvalidHistoryResponse {
            endpoint,
            message: "page made no timestamp cursor progress".to_string(),
        })?;
        next_start =
            last_time
                .checked_add(1)
                .ok_or_else(|| DeepXRestError::InvalidHistoryResponse {
                    endpoint,
                    message: "timestamp cursor overflowed".to_string(),
                })?;
        if end_ms.is_some_and(|end| next_start >= end) {
            break;
        }
        if page_index + 1 == max_pages {
            return Err(DeepXRestError::PaginationLimitExceeded { max_pages }.into());
        }
    }

    Ok(Page::from_parts(pair.unwrap_or_default(), details))
}

/// Collects a bounded Spot candle history through caller-provided page reads.
///
/// # Errors
///
/// Returns a page-read error or a pagination error for invalid continuity or exhausted budgets.
pub async fn collect_spot_candle_history<E, F, Fut>(
    endpoint: &'static str,
    start_ms: u64,
    end_ms: Option<u64>,
    total_limit: Option<u32>,
    max_pages: usize,
    fetch_page: F,
) -> Result<DeepXSpotCandlesPage, E>
where
    E: From<DeepXRestError>,
    F: FnMut(u64, u32) -> Fut,
    Fut: Future<Output = Result<DeepXSpotCandlesPage, E>>,
{
    collect(
        endpoint,
        start_ms,
        end_ms,
        total_limit,
        max_pages,
        fetch_page,
    )
    .await
}

/// Collects a bounded perpetual candle or price history through caller-provided page reads.
///
/// # Errors
///
/// Returns a page-read error or a pagination error for invalid continuity or exhausted budgets.
pub async fn collect_perp_candle_history<E, F, Fut>(
    endpoint: &'static str,
    start_ms: u64,
    end_ms: Option<u64>,
    total_limit: Option<u32>,
    max_pages: usize,
    fetch_page: F,
) -> Result<DeepXPerpCandlesPage, E>
where
    E: From<DeepXRestError>,
    F: FnMut(u64, u32) -> Fut,
    Fut: Future<Output = Result<DeepXPerpCandlesPage, E>>,
{
    collect(
        endpoint,
        start_ms,
        end_ms,
        total_limit,
        max_pages,
        fetch_page,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rust_decimal::Decimal;

    use super::*;

    #[tokio::test]
    async fn exclusive_candle_end_stops_after_a_full_final_page() {
        let calls = AtomicUsize::new(0);
        let page = DeepXPerpCandlesPage {
            pair: "ETH-USDC".to_string(),
            details: (1_000..6_000)
                .map(|time| DeepXPerpCandle {
                    volume: Decimal::ONE,
                    high: Decimal::ONE,
                    low: Decimal::ONE,
                    open: Decimal::ONE,
                    close: Decimal::ONE,
                    time,
                })
                .collect(),
        };
        let fetched = collect_perp_candle_history::<DeepXRestError, _, _>(
            "perp candle history",
            1_000,
            Some(6_000),
            None,
            1,
            |start, limit| {
                calls.fetch_add(1, Ordering::Relaxed);
                assert_eq!(start, 1_000);
                assert_eq!(limit, DEEPX_MAX_HISTORY_PAGE_SIZE);
                let page = page.clone();
                async move { Ok(page) }
            },
        )
        .await
        .unwrap();
        assert_eq!(fetched.details.len(), DEEPX_MAX_HISTORY_PAGE_SIZE as usize);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}
