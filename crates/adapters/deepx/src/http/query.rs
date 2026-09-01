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

//! Typed query parameters for verified DeepX public endpoints.

use serde::Serialize;

use super::{DeepXHttpError, Result};

/// Supported aggregation periods for perpetual volume statistics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum DeepXPerpVolumePeriod {
    /// One-hour window.
    #[serde(rename = "1h")]
    OneHour,
    /// Twenty-four-hour window.
    #[serde(rename = "24h")]
    TwentyFourHours,
    /// Seven-day window.
    #[serde(rename = "7d")]
    SevenDays,
    /// Thirty-day window.
    #[serde(rename = "30d")]
    ThirtyDays,
}

/// Request for one perpetual volume-statistics window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpVolumeRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Venue-defined aggregation period.
    pub period: DeepXPerpVolumePeriod,
}

impl DeepXPerpVolumeRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.market_id == 0 {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-volume market_id must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_query(&self) -> DeepXPerpVolumeQuery {
        DeepXPerpVolumeQuery {
            market_id: self.market_id,
            period: self.period,
        }
    }
}

/// Request for one page of perpetual funding-rate history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFundingRateRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of rows requested from the venue.
    pub limit: Option<u32>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
}

/// Request for one page of perpetual open-interest history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXOpenInterestRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of rows requested from the venue.
    pub limit: Option<u32>,
}

/// Request for one page of perpetual long-short ratio history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXLongShortRatioRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of rows requested from the venue.
    pub limit: Option<u32>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
}

/// Request for one descending page of raw perpetual trades.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpTradesRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Number of trades requested from the venue.
    pub page_size: Option<u32>,
    /// Opaque venue cursor returned by the preceding page.
    pub cursor: Option<String>,
}

/// Request for one ascending page of raw one-minute perpetual candles.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpCandlesRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of candles requested from the venue.
    pub limit: Option<u32>,
}

/// Request for one ascending page of raw one-minute perpetual mark-price history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpMarkPriceRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of observations requested from the venue.
    pub limit: Option<u32>,
}

/// Request for one ascending page of raw one-minute perpetual oracle-price history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpOraclePriceRequest {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Lower timestamp bound in Unix milliseconds.
    pub start_ms: u64,
    /// Upper timestamp bound in Unix milliseconds.
    pub end_ms: Option<u64>,
    /// Maximum number of observations requested from the venue.
    pub limit: Option<u32>,
}

impl DeepXPerpOraclePriceRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_bounded_candle_request(
            "perp-oracle-price",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXPerpCandlesQuery {
        DeepXPerpCandlesQuery::new(self.market_id, self.start_ms, self.end_ms, self.limit)
    }
}

impl DeepXPerpMarkPriceRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_bounded_candle_request(
            "perp-mark-price",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXPerpCandlesQuery {
        DeepXPerpCandlesQuery::new(self.market_id, self.start_ms, self.end_ms, self.limit)
    }
}

impl DeepXPerpCandlesRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_bounded_candle_request(
            "perp-candles",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXPerpCandlesQuery {
        DeepXPerpCandlesQuery::new(self.market_id, self.start_ms, self.end_ms, self.limit)
    }
}

impl DeepXPerpTradesRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.market_id == 0 {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-trades market_id must be greater than zero".to_string(),
            ));
        }
        if self.page_size == Some(0) {
            return Err(DeepXHttpError::InvalidRequest(
                "perp-trades page_size must be greater than zero".to_string(),
            ));
        }
        validate_cursor("perp-trades", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXPerpTradesQuery<'_> {
        DeepXPerpTradesQuery {
            market_id: self.market_id,
            page_size: self.page_size,
            cursor: self.cursor.as_deref(),
            sort: "DESC",
        }
    }
}

impl DeepXLongShortRatioRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_history_request(
            "long-short-ratio",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )?;
        validate_cursor("long-short-ratio", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXLongShortRatioQuery<'_> {
        DeepXLongShortRatioQuery {
            market_id: self.market_id,
            start: self.start_ms,
            end: self.end_ms,
            limit: self.limit,
            cursor: self.cursor.as_deref(),
            interval: "1m",
            sort: "ASC",
        }
    }
}

impl DeepXOpenInterestRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_history_request(
            "open-interest",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )
    }

    pub(crate) fn as_query(&self) -> DeepXOpenInterestQuery {
        DeepXOpenInterestQuery {
            market_id: self.market_id,
            time_frame: "1m",
            start: self.start_ms,
            end: self.end_ms,
            limit: self.limit,
            sort: "ASC",
        }
    }
}

impl DeepXFundingRateRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_history_request(
            "funding-rate",
            self.market_id,
            self.start_ms,
            self.end_ms,
            self.limit,
        )?;
        validate_cursor("funding-rate", self.cursor.as_deref())
    }

    pub(crate) fn as_query(&self) -> DeepXFundingRateQuery<'_> {
        DeepXFundingRateQuery {
            market_id: self.market_id,
            start: self.start_ms,
            end: self.end_ms,
            limit: self.limit,
            cursor: self.cursor.as_deref(),
            interval: "1m",
            sort: "ASC",
        }
    }
}

fn validate_history_request(
    endpoint: &str,
    market_id: u64,
    start_ms: u64,
    end_ms: Option<u64>,
    limit: Option<u32>,
) -> Result<()> {
    if market_id == 0 {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} market_id must be greater than zero"
        )));
    }
    if end_ms.is_some_and(|end_ms| start_ms > end_ms) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} start_ms must not exceed end_ms"
        )));
    }
    if limit == Some(0) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} limit must be greater than zero"
        )));
    }
    Ok(())
}

fn validate_bounded_candle_request(
    endpoint: &str,
    market_id: u64,
    start_ms: u64,
    end_ms: Option<u64>,
    limit: Option<u32>,
) -> Result<()> {
    validate_history_request(endpoint, market_id, start_ms, end_ms, limit)?;
    if limit.is_some_and(|limit| limit > 5_000) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} limit must not exceed 5000"
        )));
    }
    Ok(())
}

fn validate_cursor(endpoint: &str, cursor: Option<&str>) -> Result<()> {
    if cursor.is_some_and(str::is_empty) {
        return Err(DeepXHttpError::InvalidRequest(format!(
            "{endpoint} cursor must not be empty"
        )));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXFundingRateQuery<'a> {
    market_id: u64,
    start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    interval: &'static str,
    sort: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXLongShortRatioQuery<'a> {
    market_id: u64,
    start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    interval: &'static str,
    sort: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXOpenInterestQuery {
    market_id: u64,
    time_frame: &'static str,
    start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    sort: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpTradesQuery<'a> {
    market_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    sort: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpCandlesQuery {
    market_id: u64,
    time_frame: &'static str,
    start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    sort: &'static str,
    trade_view: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeepXPerpVolumeQuery {
    market_id: u64,
    period: DeepXPerpVolumePeriod,
}

impl DeepXPerpCandlesQuery {
    fn new(market_id: u64, start: u64, end: Option<u64>, limit: Option<u32>) -> Self {
        Self {
            market_id,
            time_frame: "1m",
            start,
            end,
            limit,
            sort: "ASC",
            trade_view: false,
        }
    }
}
