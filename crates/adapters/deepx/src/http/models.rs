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

//! Wire models for verified DeepX public market responses.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, de::Error as _};
use serde_json::value::RawValue;

fn parse_decimal(raw: &str) -> Result<Decimal, rust_decimal::Error> {
    Decimal::from_str(raw).or_else(|_| Decimal::from_scientific(raw))
}

mod exact_decimal {
    use super::*;

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };

        parse_decimal(&value).map_err(D::Error::custom)
    }
}

mod optional_exact_decimal {
    use super::*;

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        if raw.get() == "null" {
            return Ok(None);
        }
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };

        parse_decimal(&value).map(Some).map_err(D::Error::custom)
    }
}

/// Standard successful DeepX API response envelope.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXApiResponse<T> {
    /// Venue response code.
    pub code: u16,
    /// Human-readable venue response message.
    pub msg: String,
    /// Response payload.
    pub data: T,
    /// Venue failure indicator.
    pub fail: bool,
}

/// One exact perpetual funding-rate observation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXFundingRateRecord {
    /// Funding rate represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate: Decimal,
    /// Observation timestamp in Unix milliseconds.
    pub time: u64,
}

/// One cursor-paginated perpetual funding-rate response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXFundingRatePage {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Funding-rate observations in venue response order.
    pub details: Vec<DeepXFundingRateRecord>,
    /// Opaque cursor for the next page when present.
    pub next_cursor: Option<String>,
    /// Whether the venue reports another page.
    pub has_next: bool,
}

/// One exact perpetual long-short ratio observation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXLongShortRatioRecord {
    /// Venue-reported long-to-short position ratio.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub long_short_ratio: Decimal,
    /// UTC interval bucket timestamp in Unix milliseconds.
    pub time: u64,
}

/// One cursor-paginated perpetual long-short ratio response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXLongShortRatioPage {
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Ratio observations in venue response order.
    pub details: Vec<DeepXLongShortRatioRecord>,
    /// Opaque cursor for the next page when present.
    pub next_cursor: Option<String>,
    /// Whether the venue reports another page.
    pub has_next: bool,
}

/// One exact perpetual open-interest observation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXOpenInterestRecord {
    /// Venue-reported total open interest with units left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_oi: Decimal,
    /// Venue-reported long-to-short ratio.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub long_short_ratio: Decimal,
    /// Number of long positions.
    pub long_position_count: u64,
    /// Number of short positions.
    pub short_position_count: u64,
    /// Observation timestamp in Unix milliseconds.
    pub statistic_time: u64,
}

/// One perpetual open-interest response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXOpenInterestPage {
    /// Venue pair name.
    pub pair: String,
    /// Open-interest observations in venue response order.
    pub details: Vec<DeepXOpenInterestRecord>,
}

/// One raw perpetual trade in venue response form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpTrade {
    /// Venue trade identity.
    pub id: u64,
    /// Deployment-provided perpetual market ID.
    pub market_id: u64,
    /// Buyer order identity.
    pub buyer_order_id: String,
    /// Buyer account address.
    pub buyer: String,
    /// Seller order identity.
    pub seller_order_id: String,
    /// Seller account address.
    pub seller: String,
    /// Execution price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Execution quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub size: Decimal,
    /// Buyer leverage reported by the venue.
    pub buyer_leverage: u64,
    /// Seller leverage reported by the venue.
    pub seller_leverage: u64,
    /// Venue timestamp preserved without interpretation.
    pub created_at: String,
    /// Venue fill-direction value preserved without interpretation.
    pub filled_direction: String,
    /// Venue taker-role value preserved without interpretation.
    pub taker: String,
    /// Taker fee represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub taker_fee: Decimal,
    /// Maker fee represented without floating point.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maker_fee: Decimal,
}

/// One cursor-paginated raw perpetual trades response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpTradesPage {
    /// Trades in venue response order.
    pub items: Vec<DeepXPerpTrade>,
    /// Opaque cursor for the next page when present.
    pub next_cursor: Option<String>,
    /// Whether the venue reports another page.
    pub has_next: bool,
}

/// One raw one-minute perpetual candle in venue response form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXPerpCandle {
    /// Traded quantity reported for the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub volume: Decimal,
    /// Highest execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub high: Decimal,
    /// Lowest execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub low: Decimal,
    /// First execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub open: Decimal,
    /// Last execution price in the bucket.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub close: Decimal,
    /// Venue bucket timestamp in Unix milliseconds.
    pub time: u64,
}

/// One raw perpetual candle response page.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXPerpCandlesPage {
    /// Venue pair name.
    pub pair: String,
    /// One-minute candles in venue response order.
    pub details: Vec<DeepXPerpCandle>,
}

/// One perpetual market volume-statistics window.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpVolume {
    /// Venue-reported aggregate volume with units left uninterpreted.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub total_volume: Decimal,
    /// Venue-reported number of trades in the window.
    pub trade_count: u64,
    /// Inclusive or exclusive window start in Unix milliseconds, left uninterpreted.
    pub start_time: u64,
    /// Inclusive or exclusive window end in Unix milliseconds, left uninterpreted.
    pub end_time: u64,
    /// Venue statistics timestamp in Unix milliseconds.
    pub statistic_time: u64,
}

/// DeepX Spot market metadata.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSpotMarket {
    /// Venue market name.
    pub name: String,
    /// Deployment-provided bytes32 pair identity.
    pub pair: String,
    /// Quote asset address.
    pub quote_address: String,
    /// Quote asset decimal precision.
    pub quote_decimal: u8,
    /// Quote asset symbol.
    pub quote_symbol: String,
    /// Base asset address.
    pub base_address: String,
    /// Base asset decimal precision.
    pub base_decimal: u8,
    /// Base asset symbol.
    pub base_symbol: String,
    /// Taker fee rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub taker_fee_rate: Decimal,
    /// Maker fee rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maker_fee_rate: Decimal,
    /// Current market price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Minimum price increment.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub tick_size: Decimal,
    /// Whether trading is paused.
    pub is_paused: bool,
    /// Maximum permitted price deviation.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub max_deviation_bps: Decimal,
    /// Long-side limit-order guard.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub limit_order_guard_limit_long: Decimal,
    /// Short-side limit-order guard.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub limit_order_guard_limit_short: Decimal,
    /// Latest 24-hour price change rate when available.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub last_24h_price_change_rate: Option<Decimal>,
}

/// DeepX perpetual market metadata required for instrument construction.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpMarket {
    /// Deployment-provided market identity.
    pub id: u64,
    /// Venue market name.
    pub name: String,
    /// Base asset symbol.
    pub base_symbol: String,
    /// Base asset address.
    pub base_address: String,
    /// Base asset decimal precision.
    pub base_decimal: u8,
    /// Quote market identity.
    pub quote_market_id: u64,
    /// Quote asset symbol.
    pub quote_symbol: String,
    /// Quote asset address.
    pub quote_address: String,
    /// Quote asset decimal precision.
    pub quote_decimal: u8,
    /// Venue network label.
    pub network: String,
    /// Venue-reported market height.
    pub height: u64,
    /// Current funding rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate: Decimal,
    /// Cumulative funding index.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub cumulative_funding_index: Decimal,
    /// Latest funding-rate timestamp in milliseconds.
    pub last_funding_rate_time: u64,
    /// Latest funding calculation timestamp in milliseconds.
    #[serde(rename = "lastCaclFundingRateTime")]
    pub last_calc_funding_rate_time: u64,
    /// Current oracle price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub oracle_price: Decimal,
    /// Current mark price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub mark_price: Decimal,
    /// Latest 24-hour price change rate when available.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub last_24h_price_change_rate: Option<Decimal>,
    /// Maximum permitted price deviation.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub max_deviation_bps: Decimal,
    /// Initial margin ratio.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub initial_margin_ratio: Decimal,
    /// Maintenance margin ratio.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maintenance_margin_ratio: Decimal,
    /// Maximum number of active orders.
    pub max_active_orders: u32,
    /// Taker fee rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub taker_fee_rate: Decimal,
    /// Maker fee rate.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub maker_fee_rate: Decimal,
    /// Minimum order quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub order_spec_min_qty: Decimal,
    /// Minimum price increment.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub order_spec_tick_size: Decimal,
    /// Minimum quantity increment.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub order_spec_step_size: Decimal,
    /// Minimum order notional.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub order_spec_min_notional: Decimal,
    /// Long-side limit-order guard.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub limit_order_guard_limit_long: Decimal,
    /// Short-side limit-order guard.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub limit_order_guard_limit_short: Decimal,
    /// Current open interest.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub open_interest: Decimal,
    /// Number of open long positions.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub long_open_pos_num: Decimal,
    /// Number of open short positions.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub short_open_pos_num: Decimal,
    /// Base interest rate used for funding.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub base_interest_rate: Decimal,
    /// Impact margin value.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub impact_margin_value: Decimal,
    /// Funding-rate change cap.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate_change_cap: Decimal,
    /// Funding-rate change floor.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate_change_floor: Decimal,
    /// Funding-rate clamp upper bound.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate_clamp_upper_bound: Decimal,
    /// Funding-rate clamp lower bound.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub funding_rate_clamp_lower_bound: Decimal,
    /// Liquidation duration.
    pub liquidation_duration: u64,
    /// Liquidity-bucket slippage step.
    pub liquidity_bucket_slippage_step: u64,
    /// Liquidity-bucket slippage limit.
    pub liquidity_bucket_slippage_limit: u64,
    /// Liquidation dust value in venue units.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub liquidation_dust_value: Decimal,
    /// Liquidator fee share in venue units.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub liquidator_share_fee_rate: Decimal,
    /// Insurance-fund fee share in venue units.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub insurance_fund_share_fee_rate: Decimal,
    /// Optional deployer address.
    pub deployer: Option<String>,
    /// Optional deployer delegate address.
    pub deployer_delegate: Option<String>,
    /// Optional deployer fee-recipient address.
    pub deployer_fee_recipient: Option<String>,
    /// Optional deployer builder fee in basis points.
    #[serde(default, deserialize_with = "optional_exact_decimal::deserialize")]
    pub deployer_builder_fee_bps: Option<Decimal>,
    /// Whether the market is restricted to isolated margin by its deployer.
    pub deployer_isolated_margin_only: Option<bool>,
    /// Whether trading is paused.
    pub is_paused: bool,
    /// Whether the market is deleted.
    pub is_deleted: bool,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const PERP_VOLUME_1H_RESPONSE: &str =
        include_str!("../../test_data/http/testnet/perp_volume_1h.json");

    #[rstest]
    fn preserves_exact_json_number_lexeme() {
        #[derive(Deserialize)]
        struct ExactPrice {
            #[serde(deserialize_with = "exact_decimal::deserialize")]
            price: Decimal,
        }

        let value: ExactPrice = serde_json::from_str(r#"{"price":2453.980000000000001}"#).unwrap();

        assert_eq!(
            value.price,
            "2453.980000000000001".parse::<Decimal>().unwrap()
        );
    }

    #[rstest]
    fn decodes_sanitized_perp_volume_fixture() {
        let response: DeepXApiResponse<DeepXPerpVolume> =
            serde_json::from_str(PERP_VOLUME_1H_RESPONSE).unwrap();

        assert_eq!(response.code, 200);
        assert!(!response.fail);
        assert_eq!(response.data.total_volume, Decimal::new(2_117_975, 3));
        assert_eq!(response.data.trade_count, 2_492);
        assert_eq!(response.data.end_time - response.data.start_time, 3_600_000);
        assert_eq!(response.data.statistic_time, response.data.end_time);
    }
}
