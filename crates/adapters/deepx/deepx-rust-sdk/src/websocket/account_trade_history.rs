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

//! Perpetual account trade-history subscription and acknowledgement protocol.

use std::collections::HashSet;

use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::{account::DeepXWsUserBalancesRequest, public::DeepXWsPerpMarket};
use crate::{decimal::exact_decimal, parse::parse_canonical_order_id, rest::DeepXAccountPage};

/// One market-bound, address-scoped perpetual trade-history subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsPerpTradeHistoryRequest {
    market_id: u16,
    address: String,
    expected_subaccounts: Option<HashSet<String>>,
}

impl DeepXWsPerpTradeHistoryRequest {
    /// Creates a read-only subscription for one subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero market ID or invalid AccountId20.
    pub fn new(market_id: u16, subaccount: &str) -> Result<Self> {
        ensure!(market_id != 0, "DeepX perpetual market ID must be positive");
        let address = DeepXWsUserBalancesRequest::new(subaccount)?
            .subaccount()
            .to_string();
        Ok(Self {
            market_id,
            address,
            expected_subaccounts: None,
        })
    }

    /// Creates a wallet subscription bound to an independently read subaccount directory.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, duplicated, or invalid subaccount set.
    pub fn for_wallet(market_id: u16, wallet: &str, subaccounts: &[String]) -> Result<Self> {
        let mut request = Self::new(market_id, wallet)?;
        ensure!(
            !subaccounts.is_empty(),
            "DeepX perpetual wallet trade subscription requires subaccounts"
        );
        let mut expected = HashSet::with_capacity(subaccounts.len());
        for subaccount in subaccounts {
            let address = DeepXWsUserBalancesRequest::new(subaccount)?
                .subaccount()
                .to_string();
            ensure!(
                expected.insert(address),
                "duplicate DeepX perpetual wallet trade subaccount"
            );
        }
        request.expected_subaccounts = Some(expected);
        Ok(request)
    }

    /// Returns the subscribed perpetual market ID.
    #[must_use]
    pub const fn market_id(&self) -> u16 {
        self.market_id
    }

    /// Returns whether the request subscribes to a wallet's subaccount directory.
    #[must_use]
    pub const fn is_wallet(&self) -> bool {
        self.expected_subaccounts.is_some()
    }

    /// Returns whether a canonical subaccount belongs to the subscribed scope.
    #[must_use]
    pub fn accepts_subaccount(&self, subaccount: &str) -> bool {
        self.expected_subaccounts
            .as_ref()
            .map_or(subaccount == self.address, |expected| {
                expected.contains(subaccount)
            })
    }

    /// Serializes the observed read-only subscription request.
    ///
    /// # Errors
    ///
    /// Returns an error if the validated request cannot be serialized.
    pub fn to_text(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Market {
            #[serde(rename = "type")]
            kind: &'static str,
            id: u16,
        }
        #[derive(Serialize)]
        struct Subscription<'a> {
            channel: &'static str,
            address: &'a str,
            #[serde(rename = "addressType", skip_serializing_if = "Option::is_none")]
            address_type: Option<&'static str>,
        }
        #[derive(Serialize)]
        struct Options {
            compress: bool,
        }
        #[derive(Serialize)]
        struct Request<'a> {
            action: &'static str,
            market: Market,
            subscriptions: [Subscription<'a>; 1],
            options: Options,
        }

        Ok(serde_json::to_string(&Request {
            action: "subscribe",
            market: Market {
                kind: "perp",
                id: self.market_id,
            },
            subscriptions: [Subscription {
                channel: "user_perp_trade_history",
                address: &self.address,
                address_type: self.expected_subaccounts.as_ref().map(|_| "wallet"),
            }],
            options: Options { compress: false },
        })?)
    }

    /// Validates the exact market and account scope of an acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error for a malformed or foreign acknowledgement.
    pub fn validate_ack(&self, text: &str) -> Result<()> {
        #[derive(Deserialize)]
        struct Subscription {
            channel: String,
            address: String,
        }
        #[derive(Deserialize)]
        struct Ack {
            #[serde(rename = "type")]
            kind: String,
            market: DeepXWsPerpMarket,
            subscriptions: Vec<Subscription>,
        }

        let ack: Ack = serde_json::from_str(text)?;
        ensure!(
            ack.kind == "subscribed",
            "unexpected DeepX trade-history frame"
        );
        ensure!(
            ack.market.kind == "perp" && ack.market.id == self.market_id,
            "foreign DeepX perpetual trade-history market"
        );
        let mut observed = HashSet::with_capacity(ack.subscriptions.len());
        for subscription in ack.subscriptions {
            ensure!(
                subscription.channel == "user_perp_trade_history"
                    && observed.insert(subscription.address.to_ascii_lowercase()),
                "foreign or duplicate DeepX trade-history subscription"
            );
        }
        if let Some(expected) = &self.expected_subaccounts {
            ensure!(
                &observed == expected,
                "DeepX wallet trade-history acknowledgement differs from directory"
            );
        } else {
            ensure!(
                observed.len() == 1 && observed.contains(&self.address),
                "foreign DeepX trade-history subscription"
            );
        }
        Ok(())
    }
}

/// One typed but unconfirmed incremental perpetual trade observation.
#[derive(Debug)]
pub struct DeepXWsPerpTradeHistoryItem {
    /// Venue trade ID, absent from the documented WebSocket example.
    pub id: Option<u64>,
    /// Perpetual market ID bound to the enclosing subscription.
    pub market_id: u64,
    /// Exact decimal venue order ID.
    pub order_id: String,
    /// Venue long-side flag without framework-side interpretation.
    pub is_long: bool,
    /// Venue taker role.
    pub taker: String,
    /// Exact execution price.
    pub price: Decimal,
    /// Exact execution quantity.
    pub size: Decimal,
    /// Exact leverage.
    pub leverage: Decimal,
    /// Venue creation timestamp, validated as RFC 3339.
    pub created_at: String,
    /// Venue fill-direction value.
    pub filled_direction: String,
    /// Exact signed venue fee.
    pub fee: Decimal,
    /// Venue fee asset when supplied.
    pub fee_asset: Option<String>,
    raw: Box<RawValue>,
}

impl DeepXWsPerpTradeHistoryItem {
    /// Parses an exact trade item for the subscribed perpetual market.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed or invalid venue fields or a foreign market ID.
    pub fn parse(raw: Box<RawValue>, market_id: u16) -> Result<Self> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct WireTrade {
            id: Option<u64>,
            market_id: u64,
            order_id: String,
            is_long: bool,
            taker: String,
            #[serde(deserialize_with = "exact_decimal::deserialize")]
            price: Decimal,
            #[serde(deserialize_with = "exact_decimal::deserialize")]
            size: Decimal,
            #[serde(deserialize_with = "exact_decimal::deserialize")]
            leverage: Decimal,
            created_at: String,
            filled_direction: String,
            #[serde(deserialize_with = "exact_decimal::deserialize")]
            fee: Decimal,
            fee_asset: Option<String>,
        }

        let trade: WireTrade = serde_json::from_str(raw.get())?;
        ensure!(
            parse_canonical_order_id(&trade.order_id).is_some(),
            "invalid DeepX trade-history order ID"
        );
        let created_at = trade.created_at.parse::<jiff::Timestamp>()?;
        ensure!(
            trade.market_id == u64::from(market_id)
                && trade.id.is_none_or(|id| id != 0)
                && matches!(trade.taker.as_str(), "Buyer" | "Seller")
                && matches!(trade.filled_direction.as_str(), "Long" | "Short" | "Both")
                && trade.price > Decimal::ZERO
                && trade.size >= Decimal::ZERO
                && trade.leverage > Decimal::ZERO
                && created_at.as_nanosecond() >= 0,
            "foreign or invalid DeepX perpetual trade-history item"
        );
        Ok(Self {
            id: trade.id,
            market_id: trade.market_id,
            order_id: trade.order_id,
            is_long: trade.is_long,
            taker: trade.taker,
            price: trade.price,
            size: trade.size,
            leverage: trade.leverage,
            created_at: trade.created_at,
            filled_direction: trade.filled_direction,
            fee: trade.fee,
            fee_asset: trade.fee_asset,
            raw,
        })
    }

    /// Returns the complete source item with numeric JSON lexemes preserved.
    #[must_use]
    pub fn raw(&self) -> &RawValue {
        &self.raw
    }
}

/// One address-bound incremental perpetual trade batch.
#[derive(Debug)]
pub struct DeepXWsPerpTradeHistoryBatch {
    subaccount: String,
    items: Vec<DeepXWsPerpTradeHistoryItem>,
    timestamp_ms: u64,
}

impl DeepXWsPerpTradeHistoryBatch {
    /// Returns the acknowledged subaccount carrying this batch.
    #[must_use]
    pub fn subaccount(&self) -> &str {
        &self.subaccount
    }

    /// Returns typed trade observations retaining their original JSON.
    #[must_use]
    pub fn items(&self) -> &[DeepXWsPerpTradeHistoryItem] {
        &self.items
    }

    /// Returns the venue frame timestamp in Unix milliseconds.
    #[must_use]
    pub const fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }
}

/// One frame admitted for an exact perpetual trade-history subscription.
#[derive(Debug)]
pub enum DeepXWsPerpTradeHistoryFrame {
    /// The requested market and address set were acknowledged.
    Subscribed,
    /// One typed account-scoped incremental batch, without fill interpretation.
    TradeBatch(DeepXWsPerpTradeHistoryBatch),
    /// Application heartbeat response.
    Pong,
}

impl DeepXWsPerpTradeHistoryFrame {
    /// Parses and binds one documented trade-history envelope to the request.
    ///
    /// Items retain their raw JSON alongside exact typed fields. The documented example omits a
    /// stable venue trade ID, so this parser cannot deduplicate batches or authorize framework
    /// execution events.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, foreign, nonterminal, or unsupported frames.
    pub fn parse(text: &str, request: &DeepXWsPerpTradeHistoryRequest) -> Result<Self> {
        #[derive(Deserialize)]
        struct WireFrame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsPerpMarket>,
            channel: Option<String>,
            data: Option<Box<RawValue>>,
            timestamp: Option<u64>,
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
                request.validate_ack(text)?;
                Ok(Self::Subscribed)
            }
            "data" => {
                let market = wire.market.context("missing DeepX trade-history market")?;
                ensure!(
                    market.kind == "perp" && market.id == request.market_id(),
                    "foreign DeepX perpetual trade-history market"
                );
                ensure!(
                    wire.channel.as_deref() == Some("user_perp_trade_history"),
                    "foreign DeepX perpetual trade-history channel"
                );
                let data: Data = serde_json::from_str(
                    wire.data.context("missing DeepX trade-history data")?.get(),
                )?;
                let subaccount = DeepXWsUserBalancesRequest::new(&data.address)?
                    .subaccount()
                    .to_string();
                if request.is_wallet() {
                    ensure!(
                        request.accepts_subaccount(&subaccount),
                        "foreign DeepX wallet trade-history subaccount"
                    );
                } else {
                    ensure!(
                        request.accepts_subaccount(&subaccount),
                        "foreign DeepX trade-history subaccount"
                    );
                }
                ensure!(
                    !data.trades.has_next && data.trades.next_cursor.is_none(),
                    "DeepX incremental trade-history batch has a continuation cursor"
                );
                let mut items = Vec::with_capacity(data.trades.items.len());
                for raw in data.trades.items {
                    items.push(DeepXWsPerpTradeHistoryItem::parse(
                        raw,
                        request.market_id(),
                    )?);
                }
                let timestamp_ms = wire
                    .timestamp
                    .context("missing DeepX trade-history timestamp")?;
                ensure!(
                    timestamp_ms > 0 && i64::try_from(timestamp_ms).is_ok(),
                    "invalid DeepX trade-history timestamp"
                );
                Ok(Self::TradeBatch(DeepXWsPerpTradeHistoryBatch {
                    subaccount,
                    items,
                    timestamp_ms,
                }))
            }
            _ => anyhow::bail!("unsupported DeepX trade-history frame"),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const ADDRESS: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
    const DATA: &str = r#"{"type":"data","channel":"user_perp_trade_history","market":{"type":"perp","id":3},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","trades":{"items":[{"marketId":3,"orderId":"87561","isLong":true,"taker":"Buyer","price":2209.7600,"size":0.01,"leverage":10,"createdAt":"2026-02-06T14:52:25.613Z","filledDirection":"Long","fee":0.02}],"nextCursor":null,"hasNext":false}},"timestamp":1791475653766}"#;

    #[rstest]
    fn trade_batch_binds_account_and_preserves_numeric_lexemes() {
        let request = DeepXWsPerpTradeHistoryRequest::new(3, ADDRESS).unwrap();
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsPerpTradeHistoryFrame::parse(DATA, &request).unwrap()
        else {
            panic!("expected trade batch");
        };

        assert_eq!(batch.subaccount(), ADDRESS);
        assert_eq!(batch.items().len(), 1);
        assert!(batch.items()[0].raw().get().contains("2209.7600"));
        assert_eq!(batch.timestamp_ms(), 1_791_475_653_766);
    }

    #[rstest]
    fn trade_batch_rejects_foreign_account_and_continuation() {
        let request = DeepXWsPerpTradeHistoryRequest::new(3, ADDRESS).unwrap();
        let foreign = DATA.replace(ADDRESS, "0x977654069311fa41b88f7901dc65fae9a76fd60b");
        assert!(DeepXWsPerpTradeHistoryFrame::parse(&foreign, &request).is_err());

        let continued = DATA.replace("\"hasNext\":false", "\"hasNext\":true");
        assert!(DeepXWsPerpTradeHistoryFrame::parse(&continued, &request).is_err());
    }
}
