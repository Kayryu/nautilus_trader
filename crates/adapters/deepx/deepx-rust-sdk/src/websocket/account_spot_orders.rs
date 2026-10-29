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

//! Spot open-order subscription and raw page protocol.

use std::collections::HashSet;

use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::{json, value::RawValue};

use super::{
    account::DeepXWsUserBalancesRequest,
    spot_trades::{DeepXWsSpotMarket, DeepXWsSpotTradeRequest},
};
use crate::rest::models::DeepXSpotMarket;

/// One subaccount- or wallet-scoped Spot open-order subscription for a verified market.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsSpotOpenOrdersRequest {
    market_name: String,
    pair: String,
    address: String,
    expected_subaccounts: Option<HashSet<String>>,
}

impl DeepXWsSpotOpenOrdersRequest {
    /// Creates a read-only subscription for one Spot market and subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid market identity or AccountId20 address.
    pub fn new(market: &DeepXSpotMarket, subaccount: &str) -> Result<Self> {
        DeepXWsSpotTradeRequest::subscribe(market)?;
        let address = DeepXWsUserBalancesRequest::new(subaccount)?
            .subaccount()
            .to_string();
        Ok(Self {
            market_name: market.name.clone(),
            pair: market.pair.clone(),
            address,
            expected_subaccounts: None,
        })
    }

    /// Creates a wallet-scoped subscription bound to a separately observed subaccount directory.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, duplicated, or invalid subaccount set.
    pub fn for_wallet(
        market: &DeepXSpotMarket,
        wallet: &str,
        subaccounts: &[String],
    ) -> Result<Self> {
        let mut request = Self::new(market, wallet)?;
        ensure!(
            !subaccounts.is_empty(),
            "DeepX Spot wallet subscription requires subaccounts"
        );
        let mut expected = HashSet::with_capacity(subaccounts.len());
        for subaccount in subaccounts {
            let address = DeepXWsUserBalancesRequest::new(subaccount)?
                .subaccount()
                .to_string();
            ensure!(
                expected.insert(address),
                "duplicate DeepX Spot wallet subaccount"
            );
        }
        request.expected_subaccounts = Some(expected);
        Ok(request)
    }

    /// Returns the subscribed market name.
    #[must_use]
    pub fn market_name(&self) -> &str {
        &self.market_name
    }

    /// Returns the subscribed subaccount or wallet address.
    #[must_use]
    pub fn subaccount(&self) -> &str {
        &self.address
    }

    /// Returns whether this request targets a wallet directory.
    #[must_use]
    pub fn is_wallet(&self) -> bool {
        self.expected_subaccounts.is_some()
    }

    /// Returns the number of initial pages required by this subscription.
    #[must_use]
    pub fn expected_page_count(&self) -> usize {
        self.expected_subaccounts.as_ref().map_or(1, HashSet::len)
    }

    /// Serializes the documented, uncompressed subscription.
    #[must_use]
    pub fn to_text(&self) -> String {
        let subscription = if self.expected_subaccounts.is_some() {
            json!({
                "channel": "user_spot_open_orders",
                "address": self.address,
                "addressType": "wallet",
            })
        } else {
            json!({"channel": "user_spot_open_orders", "address": self.address})
        };
        json!({
            "action": "subscribe",
            "market": {"type": "spot", "name": self.market_name},
            "subscriptions": [subscription],
            "options": {"compress": false},
        })
        .to_string()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotPage {
    has_next: bool,
    has_prev: bool,
    items: Vec<Box<RawValue>>,
    page_no: u64,
    page_size: u64,
    total_count: u64,
    total_page: u64,
}

/// One complete initial page of raw Spot orders, without order-report interpretation.
#[derive(Debug, Deserialize)]
pub struct DeepXWsSpotOpenOrdersPage {
    address: String,
    orders: SnapshotPage,
}

impl DeepXWsSpotOpenOrdersPage {
    /// Returns exact raw order items.
    #[must_use]
    pub fn items(&self) -> &[Box<RawValue>] {
        &self.orders.items
    }

    /// Returns the subaccount bound to this page.
    #[must_use]
    pub fn subaccount(&self) -> &str {
        &self.address
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderIdentity {
    order_id: String,
    maker: String,
    pair: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Subscription {
    channel: String,
    address: String,
}

/// A frame admitted for one exact Spot open-order subscription.
#[derive(Debug)]
pub enum DeepXWsSpotOpenOrdersFrame {
    /// The venue acknowledged exactly the requested channel and address scope.
    Subscribed,
    /// The venue sent one complete initial page for the requested identity.
    OrderPage {
        /// Raw page, without framework order-state meaning.
        page: DeepXWsSpotOpenOrdersPage,
        /// Venue frame timestamp in Unix milliseconds.
        timestamp_ms: u64,
    },
    /// Application heartbeat response.
    Pong,
}

impl DeepXWsSpotOpenOrdersFrame {
    /// Parses and binds one frame to a specific Spot subscription request.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, unsupported, foreign, or incomplete frames.
    pub fn parse(text: &str, request: &DeepXWsSpotOpenOrdersRequest) -> Result<Self> {
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

        let wire: WireFrame = serde_json::from_str(text)?;
        match wire.kind.as_str() {
            "pong" => Ok(Self::Pong),
            "subscribed" | "data" => {
                let market = wire
                    .market
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX Spot market"))?;
                ensure!(
                    market.kind == "spot"
                        && market.name == request.market_name
                        && market
                            .pair
                            .as_deref()
                            .is_some_and(|pair| pair.eq_ignore_ascii_case(&request.pair)),
                    "foreign DeepX Spot order market"
                );
                if wire.kind == "subscribed" {
                    let subscriptions = wire
                        .subscriptions
                        .ok_or_else(|| anyhow::anyhow!("missing DeepX Spot subscriptions"))?;
                    let mut observed = HashSet::with_capacity(subscriptions.len());
                    for subscription in subscriptions {
                        ensure!(
                            subscription.channel == "user_spot_open_orders"
                                && observed.insert(subscription.address.to_ascii_lowercase()),
                            "foreign or duplicate DeepX Spot order subscription"
                        );
                    }
                    if let Some(expected) = &request.expected_subaccounts {
                        ensure!(
                            &observed == expected,
                            "DeepX Spot wallet order acknowledgement differs from directory"
                        );
                    } else {
                        ensure!(
                            observed.len() == 1 && observed.contains(&request.address),
                            "foreign DeepX Spot order subscription"
                        );
                    }
                    return Ok(Self::Subscribed);
                }
                ensure!(
                    wire.channel.as_deref() == Some("user_spot_open_orders"),
                    "unsupported DeepX Spot order channel"
                );
                let data = wire
                    .data
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX Spot order data"))?;
                let page: DeepXWsSpotOpenOrdersPage = serde_json::from_str(data.get())?;
                let page_address = page.address.to_ascii_lowercase();
                if let Some(expected) = &request.expected_subaccounts {
                    ensure!(
                        expected.contains(&page_address),
                        "foreign DeepX Spot wallet order page"
                    );
                } else {
                    ensure!(
                        page_address == request.address,
                        "foreign DeepX Spot order page"
                    );
                }
                let orders = &page.orders;
                ensure!(
                    !orders.has_next
                        && !orders.has_prev
                        && orders.page_no == 1
                        && orders.page_size > 0
                        && orders.items.len() as u64 <= orders.page_size
                        && orders.total_count == orders.items.len() as u64
                        && orders.total_page == u64::from(!orders.items.is_empty()),
                    "incomplete DeepX Spot order page"
                );
                let mut order_ids = HashSet::new();
                for item in &orders.items {
                    let identity: OrderIdentity = serde_json::from_str(item.get())?;
                    let order_id = crate::parse::parse_canonical_order_id(&identity.order_id)
                        .ok_or_else(|| anyhow::anyhow!("invalid DeepX Spot order ID"))?;
                    ensure!(
                        order_ids.insert(order_id)
                            && identity.maker.eq_ignore_ascii_case(&page.address)
                            && identity.pair.eq_ignore_ascii_case(&request.pair),
                        "foreign or duplicate DeepX Spot open order"
                    );
                }
                let timestamp_ms = wire
                    .timestamp
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX Spot order timestamp"))?;
                ensure!(
                    timestamp_ms > 0 && i64::try_from(timestamp_ms).is_ok(),
                    "invalid DeepX Spot order timestamp"
                );
                Ok(Self::OrderPage { page, timestamp_ms })
            }
            _ => anyhow::bail!("unsupported DeepX Spot order frame"),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const ADDRESS: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
    const OTHER: &str = "0x977654069311fa41b88f7901dc65fae9a76fd60b";
    const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
    const PAIR: &str = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
    const ACK: &str = r#"{"type":"subscribed","market":{"type":"spot","name":"ETH/USDC","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"},"subscriptions":[{"channel":"user_spot_open_orders","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"}],"message":"Successfully subscribed to 1 channels"}"#;
    const SNAPSHOT: &str = r#"{"type":"data","channel":"user_spot_open_orders","market":{"type":"spot","name":"ETH/USDC","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","orders":{"hasNext":false,"hasPrev":false,"items":[],"pageNo":1,"pageSize":100,"totalCount":0,"totalPage":0}},"timestamp":1791491471048}"#;
    const MARKETS: &str = include_str!("../../test_data/http/testnet/spot_markets.json");

    fn market() -> DeepXSpotMarket {
        let response: crate::rest::DeepXApiResponse<Vec<DeepXSpotMarket>> =
            serde_json::from_str(MARKETS).unwrap();
        response
            .data
            .into_iter()
            .find(|market| market.name == "ETH/USDC")
            .unwrap()
    }

    fn request() -> DeepXWsSpotOpenOrdersRequest {
        DeepXWsSpotOpenOrdersRequest::new(&market(), ADDRESS).unwrap()
    }

    fn wallet_request() -> DeepXWsSpotOpenOrdersRequest {
        DeepXWsSpotOpenOrdersRequest::for_wallet(
            &market(),
            WALLET,
            &[ADDRESS.to_string(), OTHER.to_string()],
        )
        .unwrap()
    }

    #[rstest]
    fn parses_observed_ack_and_empty_snapshot() {
        let request = request();
        let encoded: serde_json::Value = serde_json::from_str(&request.to_text()).unwrap();
        assert_eq!(encoded["market"]["name"], "ETH/USDC");
        assert_eq!(encoded["subscriptions"][0]["address"], ADDRESS);
        assert_eq!(encoded["options"]["compress"], false);
        assert!(matches!(
            DeepXWsSpotOpenOrdersFrame::parse(ACK, &request).unwrap(),
            DeepXWsSpotOpenOrdersFrame::Subscribed
        ));
        let DeepXWsSpotOpenOrdersFrame::OrderPage { page, timestamp_ms } =
            DeepXWsSpotOpenOrdersFrame::parse(SNAPSHOT, &request).unwrap()
        else {
            panic!("expected initial Spot order page");
        };
        assert!(page.items().is_empty());
        assert_eq!(page.subaccount(), ADDRESS);
        assert_eq!(timestamp_ms, 1_791_491_471_048);
    }

    #[rstest]
    fn preserves_nonempty_raw_item_with_bound_identity() {
        let item = format!(
            "{{\"orderId\":\"123\",\"maker\":\"{ADDRESS}\",\"pair\":\"{PAIR}\",\"price\":1.2300}}"
        );
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":[{item}]"))
            .replace("\"totalCount\":0", "\"totalCount\":1")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        let DeepXWsSpotOpenOrdersFrame::OrderPage { page, .. } =
            DeepXWsSpotOpenOrdersFrame::parse(&frame, &request()).unwrap()
        else {
            panic!("expected initial Spot order page");
        };
        assert_eq!(page.items()[0].get(), item);
    }

    #[rstest]
    #[case("00", ADDRESS, PAIR)]
    #[case("1.0", ADDRESS, PAIR)]
    #[case("1", "0x0000000000000000000000000000000000000000", PAIR)]
    #[case(
        "1",
        ADDRESS,
        "0x0000000000000000000000000000000000000000000000000000000000000000"
    )]
    fn rejects_invalid_order_identity(
        #[case] order_id: &str,
        #[case] maker: &str,
        #[case] pair: &str,
    ) {
        let item =
            format!("{{\"orderId\":\"{order_id}\",\"maker\":\"{maker}\",\"pair\":\"{pair}\"}}");
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":[{item}]"))
            .replace("\"totalCount\":0", "\"totalCount\":1")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        assert!(DeepXWsSpotOpenOrdersFrame::parse(&frame, &request()).is_err());
    }

    #[rstest]
    fn rejects_duplicate_numeric_order_ids() {
        let items = format!(
            "[{{\"orderId\":\"1\",\"maker\":\"{ADDRESS}\",\"pair\":\"{PAIR}\"}},{{\"orderId\":\"01\",\"maker\":\"{ADDRESS}\",\"pair\":\"{PAIR}\"}}]"
        );
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":{items}"))
            .replace("\"totalCount\":0", "\"totalCount\":2")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        assert!(DeepXWsSpotOpenOrdersFrame::parse(&frame, &request()).is_err());
    }

    #[rstest]
    fn accepts_first_generated_order_id_zero() {
        let item = format!("{{\"orderId\":\"0\",\"maker\":\"{ADDRESS}\",\"pair\":\"{PAIR}\"}}");
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":[{item}]"))
            .replace("\"totalCount\":0", "\"totalCount\":1")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        let DeepXWsSpotOpenOrdersFrame::OrderPage { page, .. } =
            DeepXWsSpotOpenOrdersFrame::parse(&frame, &request()).unwrap()
        else {
            panic!("expected zero-ID Spot order page");
        };
        assert_eq!(page.items()[0].get(), item);
    }

    #[rstest]
    #[case(
        "pair",
        "\"pair\":\"0x0000000000000000000000000000000000000000000000000000000000000000\""
    )]
    #[case(
        "address",
        "\"address\":\"0x0000000000000000000000000000000000000000\""
    )]
    #[case("channel", "\"channel\":\"user_spot_order_history\"")]
    #[case("pagination", "\"hasNext\":true")]
    fn rejects_foreign_or_incomplete_snapshot(#[case] field: &str, #[case] replacement: &str) {
        let original = match field {
            "pair" => format!("\"pair\":\"{PAIR}\""),
            "address" => format!("\"address\":\"{ADDRESS}\""),
            "channel" => "\"channel\":\"user_spot_open_orders\"".to_string(),
            "pagination" => "\"hasNext\":false".to_string(),
            _ => unreachable!(),
        };
        let changed = SNAPSHOT.replacen(&original, replacement, 1);
        assert_ne!(changed, SNAPSHOT);
        assert!(DeepXWsSpotOpenOrdersFrame::parse(&changed, &request()).is_err());
    }

    #[rstest]
    fn wallet_request_requires_valid_unique_subaccounts() {
        assert!(DeepXWsSpotOpenOrdersRequest::for_wallet(&market(), WALLET, &[]).is_err());
        assert!(
            DeepXWsSpotOpenOrdersRequest::for_wallet(
                &market(),
                WALLET,
                &[ADDRESS.to_string(), ADDRESS.to_string()],
            )
            .is_err()
        );
        assert!(
            DeepXWsSpotOpenOrdersRequest::for_wallet(&market(), WALLET, &["invalid".to_string()],)
                .is_err()
        );
        let encoded: serde_json::Value = serde_json::from_str(&wallet_request().to_text()).unwrap();
        assert_eq!(encoded["subscriptions"][0]["address"], WALLET);
        assert_eq!(encoded["subscriptions"][0]["addressType"], "wallet");
    }
}
