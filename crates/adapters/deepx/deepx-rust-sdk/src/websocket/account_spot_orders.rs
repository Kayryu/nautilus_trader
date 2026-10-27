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
