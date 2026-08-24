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

pub use super::enums::{DeepXBookUpdateType, DeepXTakerSide};
use crate::common::models::DeepXOrderBookLevel;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeepXWsMessage<T> {
    pub channel: String,
    pub data: T,
}

/// DeepX order book update. Delta sizes are absolute and zero removes a level.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXOrderBookUpdate {
    pub asks: Vec<DeepXOrderBookLevel>,
    pub bids: Vec<DeepXOrderBookLevel>,
    pub engine_time: u64,
    pub last_update_id: u64,
    pub prev_last_update_id: Option<u64>,
    pub server_time: u64,
    pub symbol: String,
    pub update_type: DeepXBookUpdateType,
}

impl DeepXOrderBookUpdate {
    /// Returns whether this update can initialize or continue a book at `last_update_id`.
    #[must_use]
    pub fn follows(&self, last_update_id: Option<u64>) -> bool {
        match self.update_type {
            DeepXBookUpdateType::Snapshot => self.prev_last_update_id.is_none(),
            DeepXBookUpdateType::Delta => {
                self.prev_last_update_id.is_some() && self.prev_last_update_id == last_update_id
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXTrade {
    pub buy_order_id: String,
    pub id: String,
    pub maker_fee: Decimal,
    pub market_id: u32,
    pub price: Decimal,
    pub qty: Decimal,
    pub quote_qty: Decimal,
    pub sell_order_id: String,
    pub symbol: String,
    pub taker_fee: Decimal,
    pub taker_side: DeepXTakerSide,
    pub time: u64,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::rstest;

    use super::*;

    #[rstest]
    fn parses_and_sequences_order_book_updates() {
        let snapshot_payload = r#"{"channel":"perp@orderbook","data":{"asks":[["2457.02","0.859"]],"bids":[["2456.81","0.291"]],"engineTime":1787562160783,"lastUpdateId":62493922,"prevLastUpdateId":null,"serverTime":1787562160833,"symbol":"ETH-USDC","updateType":"snapshot"}}"#;
        let delta_payload = r#"{"channel":"perp@orderbook","data":{"asks":[["2457.02","0"],["2457.12","1.468"]],"bids":[],"engineTime":1787562162051,"lastUpdateId":62494052,"prevLastUpdateId":62493922,"serverTime":1787562162052,"symbol":"ETH-USDC","updateType":"delta"}}"#;

        let snapshot: DeepXWsMessage<DeepXOrderBookUpdate> =
            serde_json::from_str(snapshot_payload).unwrap();
        let delta: DeepXWsMessage<DeepXOrderBookUpdate> =
            serde_json::from_str(delta_payload).unwrap();

        assert!(snapshot.data.follows(None));
        assert!(delta.data.follows(Some(snapshot.data.last_update_id)));
        assert!(!delta.data.follows(Some(1)));
        assert_eq!(delta.data.asks[0].1, Decimal::ZERO);
    }

    #[rstest]
    fn parses_public_trade_without_losing_decimal_precision() {
        let payload = r#"{"channel":"perp@trades","data":{"buyOrderId":"6652970","id":"159078803000024","makerFee":"-0.02529","marketId":3,"price":"2455.7","qty":"0.103","quoteQty":"252.9371","sellOrderId":"2263022","symbol":"ETH-USDC","takerFee":"0.050581","takerSide":"SELL","time":1787562119833}}"#;

        let message: DeepXWsMessage<DeepXTrade> = serde_json::from_str(payload).unwrap();

        assert_eq!(message.data.taker_side, DeepXTakerSide::Sell);
        assert_eq!(message.data.price, Decimal::from_str("2455.7").unwrap());
        assert_eq!(
            message.data.quote_qty,
            Decimal::from_str("252.9371").unwrap()
        );
    }
}
