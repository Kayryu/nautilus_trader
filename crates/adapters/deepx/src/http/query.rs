// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Query parameters for DeepX account and historical REST endpoints.

use serde::{Deserialize, Serialize};

use super::error::{DeepXHttpError, DeepXHttpResult};

/// Sort direction accepted by DeepX historical endpoints.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum DeepXSortDirection {
    #[serde(rename = "ASC")]
    Asc,
    #[serde(rename = "DESC")]
    Desc,
}

/// Order side accepted by DeepX order history endpoints.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum DeepXOrderSide {
    Buy,
    Sell,
}

/// Shared pagination and time filters for DeepX account history.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXHistoryQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<DeepXSortDirection>,
}

impl DeepXHistoryQuery {
    /// Validates limits and time bounds documented by the DeepX API.
    pub fn validate(&self) -> DeepXHttpResult<()> {
        if self.limit.is_some_and(|limit| !(1..=500).contains(&limit)) {
            return Err(DeepXHttpError::Validation(
                "limit must be between 1 and 500".to_string(),
            ));
        }
        if self
            .start_time
            .zip(self.end_time)
            .is_some_and(|(start, end)| start > end)
        {
            return Err(DeepXHttpError::Validation(
                "start_time must not exceed end_time".to_string(),
            ));
        }
        Ok(())
    }
}

/// Filters accepted by the DeepX perpetual order history endpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXOrderHistoryQuery {
    #[serde(flatten)]
    pub history: DeepXHistoryQuery,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<DeepXOrderSide>,
}

impl DeepXOrderHistoryQuery {
    /// Validates the shared historical filters.
    pub fn validate(&self) -> DeepXHttpResult<()> {
        self.history.validate()
    }
}

/// Symbol filter accepted by current-position and open-order endpoints.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSymbolQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

/// Filters accepted by the subaccount balance event endpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXBalanceEventQuery {
    #[serde(flatten)]
    pub history: DeepXHistoryQuery,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_type: Option<String>,
}

impl DeepXBalanceEventQuery {
    /// Validates the shared historical filters.
    pub fn validate(&self) -> DeepXHttpResult<()> {
        self.history.validate()
    }
}

/// Filters accepted by the subaccount liquidation endpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXLiquidationQuery {
    #[serde(flatten)]
    pub history: DeepXHistoryQuery,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liquidator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liquidation_type: Option<String>,
}

impl DeepXLiquidationQuery {
    /// Validates the shared historical filters.
    pub fn validate(&self) -> DeepXHttpResult<()> {
        self.history.validate()
    }
}

/// Query parameters for DeepX candle history.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXCandleQuery {
    pub interval: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_type: Option<String>,
}

impl DeepXCandleQuery {
    /// Validates the interval, limit, and time range.
    pub fn validate(&self) -> DeepXHttpResult<()> {
        if self.interval.trim().is_empty() {
            return Err(DeepXHttpError::Validation(
                "interval must not be empty".to_string(),
            ));
        }
        validate_limit_and_time(self.limit, 500, self.start_time, self.end_time)
    }
}

/// Query parameters for a DeepX order book snapshot.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXOrderBookQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merge_level: Option<u8>,
}

impl DeepXOrderBookQuery {
    /// Validates the snapshot depth and price merge level.
    pub fn validate(&self) -> DeepXHttpResult<()> {
        if self.limit.is_some_and(|limit| !(1..=500).contains(&limit)) {
            return Err(DeepXHttpError::Validation(
                "limit must be between 1 and 500".to_string(),
            ));
        }
        if self.merge_level.is_some_and(|level| level > 3) {
            return Err(DeepXHttpError::Validation(
                "merge_level must be between 0 and 3".to_string(),
            ));
        }
        Ok(())
    }
}

/// Query parameters for interval-based perpetual market history.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpMarketHistoryQuery {
    pub interval: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<DeepXSortDirection>,
}

impl DeepXPerpMarketHistoryQuery {
    /// Validates the interval, limit, and time range.
    pub fn validate(&self, maximum_limit: u16) -> DeepXHttpResult<()> {
        if self.interval.trim().is_empty() {
            return Err(DeepXHttpError::Validation(
                "interval must not be empty".to_string(),
            ));
        }
        validate_limit_and_time(self.limit, maximum_limit, self.start_time, self.end_time)
    }
}

fn validate_limit_and_time(
    limit: Option<u16>,
    maximum_limit: u16,
    start_time: Option<u64>,
    end_time: Option<u64>,
) -> DeepXHttpResult<()> {
    if limit.is_some_and(|limit| !(1..=maximum_limit).contains(&limit)) {
        return Err(DeepXHttpError::Validation(format!(
            "limit must be between 1 and {maximum_limit}",
        )));
    }
    if start_time
        .zip(end_time)
        .is_some_and(|(start, end)| start > end)
    {
        return Err(DeepXHttpError::Validation(
            "start_time must not exceed end_time".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn serializes_wire_names_and_values() {
        let query = DeepXOrderHistoryQuery {
            history: DeepXHistoryQuery {
                symbol: Some("ETH-USDC".to_string()),
                limit: Some(10),
                from_id: Some(9),
                start_time: Some(100),
                end_time: Some(200),
                sort: Some(DeepXSortDirection::Desc),
                ..Default::default()
            },
            side: Some(DeepXOrderSide::Buy),
        };

        let value = serde_json::to_value(query).unwrap();

        assert_eq!(value["fromId"], 9);
        assert_eq!(value["startTime"], 100);
        assert_eq!(value["side"], "Buy");
        assert_eq!(value["sort"], "DESC");
    }

    #[rstest]
    fn rejects_invalid_history_bounds() {
        let query = DeepXHistoryQuery {
            start_time: Some(200),
            end_time: Some(100),
            ..Default::default()
        };

        assert!(matches!(
            query.validate(),
            Err(DeepXHttpError::Validation(_))
        ));
    }

    #[rstest]
    #[case(None)]
    #[case(Some(0))]
    #[case(Some(501))]
    fn rejects_invalid_history_limits(#[case] limit: Option<u16>) {
        let query = DeepXHistoryQuery {
            limit,
            ..Default::default()
        };

        assert_eq!(query.validate().is_ok(), limit.is_none());
    }

    #[rstest]
    fn serializes_market_history_query() {
        let query = DeepXPerpMarketHistoryQuery {
            interval: "1h".to_string(),
            limit: Some(100),
            start_time: Some(1_000),
            end_time: Some(2_000),
            sort: Some(DeepXSortDirection::Asc),
        };

        let value = serde_json::to_value(query).unwrap();

        assert_eq!(value["startTime"], 1_000);
        assert_eq!(value["endTime"], 2_000);
        assert_eq!(value["sort"], "ASC");
    }

    #[rstest]
    fn serializes_and_validates_order_book_query() {
        let query = DeepXOrderBookQuery {
            limit: Some(50),
            merge_level: Some(2),
        };

        let value = serde_json::to_value(&query).unwrap();

        assert_eq!(value["limit"], 50);
        assert_eq!(value["mergeLevel"], 2);
        assert!(query.validate().is_ok());
    }

    #[rstest]
    #[case(Some(0), None)]
    #[case(Some(501), None)]
    #[case(None, Some(4))]
    fn rejects_invalid_order_book_query(
        #[case] limit: Option<u16>,
        #[case] merge_level: Option<u8>,
    ) {
        let query = DeepXOrderBookQuery { limit, merge_level };

        assert!(query.validate().is_err());
    }

    #[rstest]
    #[case("", Some(1), 500)]
    #[case("   ", Some(1), 500)]
    #[case("1m", Some(0), 500)]
    #[case("1m", Some(101), 100)]
    fn rejects_invalid_market_history_query(
        #[case] interval: &str,
        #[case] limit: Option<u16>,
        #[case] maximum_limit: u16,
    ) {
        let query = DeepXPerpMarketHistoryQuery {
            interval: interval.to_string(),
            limit,
            start_time: None,
            end_time: None,
            sort: None,
        };

        assert!(query.validate(maximum_limit).is_err());
    }

    #[rstest]
    fn rejects_invalid_candle_time_range() {
        let query = DeepXCandleQuery {
            interval: "1m".to_string(),
            limit: Some(500),
            start_time: Some(2_000),
            end_time: Some(1_000),
            price_type: None,
        };

        assert!(query.validate().is_err());
    }
}
