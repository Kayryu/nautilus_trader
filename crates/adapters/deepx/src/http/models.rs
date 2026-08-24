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

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// DeepX perpetual market metadata returned by `GET /v1/perp/markets`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpetualMarket {
    pub market_id: u32,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub status: String,
    pub tick_size: Decimal,
    pub step_size: Decimal,
    pub min_qty: Decimal,
    pub min_notional: Decimal,
    pub maker_fee_rate: Decimal,
    pub taker_fee_rate: Decimal,
    pub max_open_orders: u32,
    pub order_types: Vec<String>,
}

/// DeepX aggregated price level encoded as `[price, size]`.
pub type DeepXOrderBookLevel = (Decimal, Decimal);

/// DeepX perpetual order book snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXOrderBookSnapshot {
    pub asks: Vec<DeepXOrderBookLevel>,
    pub bids: Vec<DeepXOrderBookLevel>,
    pub engine_time: u64,
    pub last_update_id: u64,
    pub server_time: u64,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::rstest;

    use super::*;

    #[rstest]
    fn parses_perpetual_market_without_losing_decimal_precision() {
        let payload = r#"{
            "baseAsset":"ETH","makerFeeRate":"-0.0001","marketId":3,
            "maxOpenOrders":128,"minNotional":"1","minQty":"0.001",
            "orderTypes":["LIMIT","MARKET"],"quoteAsset":"USDC",
            "status":"TRADING","stepSize":"0.001","symbol":"ETH-USDC",
            "takerFeeRate":"0.0002","tickSize":"0.01"
        }"#;

        let market: DeepXPerpetualMarket = serde_json::from_str(payload).unwrap();

        assert_eq!(market.market_id, 3);
        assert_eq!(market.tick_size, Decimal::from_str("0.01").unwrap());
        assert_eq!(market.step_size, Decimal::from_str("0.001").unwrap());
        assert_eq!(market.maker_fee_rate, Decimal::from_str("-0.0001").unwrap());
    }

    #[rstest]
    fn parses_order_book_snapshot_and_sequence() {
        let payload = r#"{
            "asks":[["2453.76","3.282"]],"bids":[["2453.75","0.842"]],
            "engineTime":1787561622066,"lastUpdateId":62430652,
            "serverTime":1787561622211
        }"#;

        let snapshot: DeepXOrderBookSnapshot = serde_json::from_str(payload).unwrap();

        assert_eq!(snapshot.last_update_id, 62_430_652);
        assert_eq!(snapshot.asks[0].0, Decimal::from_str("2453.76").unwrap());
        assert_eq!(snapshot.bids[0].1, Decimal::from_str("0.842").unwrap());
    }
}
