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
//  See the GNU Lesser General Public License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Spot account trade-history subscription protocol.

use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, value::RawValue};

use super::{account::DeepXWsUserBalancesRequest, spot_trades::DeepXWsSpotMarket};
use crate::{
    parse::parse_canonical_order_id,
    rest::{
        DeepXAccountPage,
        models::{DeepXSpotAccountTradeRecord, DeepXSpotMarket},
    },
};

/// One Spot market and subaccount selected from the current market directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsSpotTradeHistoryRequest {
    market_name: String,
    pair: String,
    subaccount: String,
}

impl DeepXWsSpotTradeHistoryRequest {
    /// Creates an exact market- and subaccount-bound read-only subscription.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed market identity or AccountId20.
    pub fn new(market: &DeepXSpotMarket, subaccount: &str) -> Result<Self> {
        ensure!(
            !market.name.trim().is_empty(),
            "DeepX Spot market name is empty"
        );
        ensure!(
            market.pair.len() == 66
                && market.pair.starts_with("0x")
                && hex::decode(&market.pair[2..]).is_ok(),
            "DeepX Spot market pair is invalid"
        );
        Ok(Self {
            market_name: market.name.clone(),
            pair: market.pair.to_ascii_lowercase(),
            subaccount: DeepXWsUserBalancesRequest::new(subaccount)?
                .subaccount()
                .to_string(),
        })
    }

    /// Returns the selected market name.
    #[must_use]
    pub fn market_name(&self) -> &str {
        &self.market_name
    }

    /// Returns the selected pair identity.
    #[must_use]
    pub fn pair(&self) -> &str {
        &self.pair
    }

    /// Returns the selected subaccount.
    #[must_use]
    pub fn subaccount(&self) -> &str {
        &self.subaccount
    }

    /// Serializes the documented uncompressed subscription.
    #[must_use]
    pub fn to_text(&self) -> String {
        json!({
            "action": "subscribe",
            "market": {"type": "spot", "name": self.market_name},
            "subscriptions": [{
                "channel": "user_spot_trade_history",
                "address": self.subaccount,
            }],
            "options": {"compress": false},
        })
        .to_string()
    }

    /// Validates the exact market identity echoed by a frame.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign market name or pair.
    pub fn validate_market(&self, market: &DeepXWsSpotMarket) -> Result<()> {
        ensure!(
            market.kind == "spot"
                && market.name == self.market_name
                && market
                    .pair
                    .as_ref()
                    .is_none_or(|pair| pair.eq_ignore_ascii_case(&self.pair)),
            "foreign DeepX Spot trade-history market"
        );
        Ok(())
    }
}

/// Parses and validates one Spot trade item for the subscribed market.
///
/// # Errors
///
/// Returns an error for malformed venue fields, an invalid timestamp, or a foreign market.
pub fn parse_spot_trade_history_item(
    raw: &RawValue,
    request: &DeepXWsSpotTradeHistoryRequest,
) -> Result<DeepXSpotAccountTradeRecord> {
    let record: DeepXSpotAccountTradeRecord = serde_json::from_str(raw.get())?;
    ensure!(
        parse_canonical_order_id(&record.order_id).is_some(),
        "invalid DeepX Spot trade order ID"
    );
    let created_at = record.created_at.parse::<jiff::Timestamp>()?;
    ensure!(
        record.id != 0
            && record.pair.eq_ignore_ascii_case(request.pair())
            && record.pair_name == request.market_name()
            && matches!(record.order_side.as_str(), "Buy" | "Sell")
            && matches!(record.taker.as_str(), "Buyer" | "Seller")
            && record.price > Decimal::ZERO
            && record.base_amount > Decimal::ZERO
            && record.quote_amount > Decimal::ZERO
            && created_at.as_nanosecond() >= 0,
        "foreign or invalid DeepX Spot trade-history item"
    );
    Ok(record)
}

/// One unconfirmed Spot trade item with its original JSON.
#[derive(Debug)]
pub struct DeepXWsSpotTradeHistoryItem {
    record: DeepXSpotAccountTradeRecord,
    raw: Box<RawValue>,
}

impl DeepXWsSpotTradeHistoryItem {
    /// Returns the exact venue record.
    #[must_use]
    pub const fn record(&self) -> &DeepXSpotAccountTradeRecord {
        &self.record
    }

    /// Returns the original JSON with numeric lexemes preserved.
    #[must_use]
    pub fn raw(&self) -> &RawValue {
        &self.raw
    }

    /// Consumes the item into its record and original JSON.
    #[must_use]
    pub fn into_parts(self) -> (DeepXSpotAccountTradeRecord, Box<RawValue>) {
        (self.record, self.raw)
    }
}

/// One incremental Spot trade batch for the selected account and market.
#[derive(Debug)]
pub struct DeepXWsSpotTradeHistoryBatch {
    items: Vec<DeepXWsSpotTradeHistoryItem>,
    timestamp_ms: u64,
}

impl DeepXWsSpotTradeHistoryBatch {
    /// Returns trade observations in venue order.
    #[must_use]
    pub fn items(&self) -> &[DeepXWsSpotTradeHistoryItem] {
        &self.items
    }

    /// Returns the venue frame timestamp in Unix milliseconds.
    #[must_use]
    pub const fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    /// Consumes the batch into its items and timestamp.
    #[must_use]
    pub fn into_parts(self) -> (Vec<DeepXWsSpotTradeHistoryItem>, u64) {
        (self.items, self.timestamp_ms)
    }
}

/// One frame admitted for an exact Spot trade-history subscription.
#[derive(Debug)]
pub enum DeepXWsSpotTradeHistoryFrame {
    /// The selected market and subaccount were acknowledged.
    Subscribed,
    /// One mutable incremental trade batch.
    TradeBatch(DeepXWsSpotTradeHistoryBatch),
    /// Application heartbeat response.
    Pong,
}

impl DeepXWsSpotTradeHistoryFrame {
    /// Parses one frame under the selected market and subaccount identity.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, foreign, or unsupported frames and invalid trade records.
    pub fn parse(text: &str, request: &DeepXWsSpotTradeHistoryRequest) -> Result<Self> {
        #[derive(Deserialize)]
        struct WireFrame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsSpotMarket>,
            subscriptions: Option<Vec<Subscription>>,
            channel: Option<String>,
            data: Option<Box<RawValue>>,
            timestamp: Option<u64>,
        }
        #[derive(Deserialize)]
        struct Subscription {
            channel: String,
            address: String,
        }
        #[derive(Deserialize)]
        struct Data {
            address: String,
            trades: DeepXAccountPage<Box<RawValue>>,
        }

        let wire: WireFrame = serde_json::from_str(text)?;
        match wire.kind.as_str() {
            "pong" => Ok(Self::Pong),
            "subscribed" => {
                request.validate_market(
                    &wire
                        .market
                        .context("missing DeepX Spot trade-history market")?,
                )?;
                let subscriptions = wire
                    .subscriptions
                    .context("missing DeepX Spot trade-history subscriptions")?;
                ensure!(
                    subscriptions.len() == 1
                        && subscriptions[0].channel == "user_spot_trade_history"
                        && subscriptions[0]
                            .address
                            .eq_ignore_ascii_case(request.subaccount()),
                    "foreign DeepX Spot trade-history acknowledgement"
                );
                Ok(Self::Subscribed)
            }
            "data" => {
                request.validate_market(
                    &wire
                        .market
                        .context("missing DeepX Spot trade-history market")?,
                )?;
                ensure!(
                    wire.channel.as_deref() == Some("user_spot_trade_history"),
                    "foreign DeepX Spot trade-history channel"
                );
                let data: Data = serde_json::from_str(
                    wire.data
                        .context("missing DeepX Spot trade-history data")?
                        .get(),
                )?;
                ensure!(
                    data.address.eq_ignore_ascii_case(request.subaccount())
                        && !data.trades.has_next
                        && data.trades.next_cursor.is_none(),
                    "foreign or incomplete DeepX Spot trade-history batch"
                );
                let mut items = Vec::with_capacity(data.trades.items.len());
                for raw in data.trades.items {
                    let record = parse_spot_trade_history_item(&raw, request)?;
                    items.push(DeepXWsSpotTradeHistoryItem { record, raw });
                }
                let timestamp_ms = wire
                    .timestamp
                    .context("missing DeepX Spot trade-history timestamp")?;
                ensure!(
                    timestamp_ms > 0 && i64::try_from(timestamp_ms).is_ok(),
                    "invalid DeepX Spot trade-history timestamp"
                );
                Ok(Self::TradeBatch(DeepXWsSpotTradeHistoryBatch {
                    items,
                    timestamp_ms,
                }))
            }
            _ => anyhow::bail!("unsupported DeepX Spot trade-history frame"),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::rest::DeepXApiResponse;

    const ADDRESS: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
    const PAIR: &str = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
    const DATA: &str = r#"{"type":"data","channel":"user_spot_trade_history","market":{"type":"spot","name":"ETH/USDC","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","trades":{"items":[{"id":53219022000005,"orderId":"2524638","orderSide":"Sell","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37","pairName":"ETH/USDC","baseAmount":0.0011644,"quoteAmount":2,"price":1717.64,"fee":0.0002,"taker":"Buyer","createdAt":"2026-06-18T15:13:14.739Z"}],"nextCursor":null,"hasNext":false}},"timestamp":1781795594739}"#;

    fn request() -> DeepXWsSpotTradeHistoryRequest {
        let response: DeepXApiResponse<Vec<DeepXSpotMarket>> = serde_json::from_str(include_str!(
            "../../test_data/http/testnet/spot_markets.json"
        ))
        .unwrap();
        let market = response
            .data
            .iter()
            .find(|market| market.name == "ETH/USDC")
            .unwrap();
        assert_eq!(market.pair, PAIR);
        DeepXWsSpotTradeHistoryRequest::new(market, ADDRESS).unwrap()
    }

    #[rstest]
    fn spot_batch_preserves_exact_item_and_scope() {
        let DeepXWsSpotTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsSpotTradeHistoryFrame::parse(DATA, &request()).unwrap()
        else {
            panic!("expected Spot trade batch");
        };
        assert_eq!(batch.items().len(), 1);
        assert_eq!(batch.items()[0].record().id, 53_219_022_000_005);
        assert!(batch.items()[0].raw().get().contains("0.0011644"));
        assert_eq!(batch.timestamp_ms(), 1_781_795_594_739);
    }

    #[rstest]
    fn spot_batch_rejects_foreign_account_and_continuation() {
        let foreign = DATA.replace(ADDRESS, "0x977654069311fa41b88f7901dc65fae9a76fd60b");
        assert!(DeepXWsSpotTradeHistoryFrame::parse(&foreign, &request()).is_err());
        let continued = DATA.replace("\"hasNext\":false", "\"hasNext\":true");
        assert!(DeepXWsSpotTradeHistoryFrame::parse(&continued, &request()).is_err());
    }
}
