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

//! Perpetual open-order subscription and raw page protocol.

use std::collections::HashSet;

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::{account::DeepXWsUserBalancesRequest, public::DeepXWsPerpMarket};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Subscription {
    channel: String,
    address: String,
}

/// One address-scoped perpetual open-order subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsPerpOpenOrdersRequest {
    market_id: u16,
    address: String,
    expected_subaccounts: Option<HashSet<String>>,
}

impl DeepXWsPerpOpenOrdersRequest {
    /// Creates a read-only subscription for one market and subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero market ID or invalid AccountId20 address.
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
            "DeepX perpetual wallet subscription requires subaccounts"
        );
        let mut expected = HashSet::with_capacity(subaccounts.len());
        for subaccount in subaccounts {
            let address = DeepXWsUserBalancesRequest::new(subaccount)?
                .subaccount()
                .to_string();
            ensure!(
                expected.insert(address),
                "duplicate DeepX perpetual wallet subaccount"
            );
        }
        request.expected_subaccounts = Some(expected);
        Ok(request)
    }

    /// Returns the subscribed market ID.
    #[must_use]
    pub const fn market_id(&self) -> u16 {
        self.market_id
    }

    /// Returns the canonical subscribed subaccount or wallet.
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
        struct Options {
            compress: bool,
        }
        #[derive(Serialize)]
        struct Request<'a> {
            action: &'static str,
            market: Market,
            subscriptions: [SubscriptionRef<'a>; 1],
            options: Options,
        }
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct SubscriptionRef<'a> {
            channel: &'static str,
            address: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            address_type: Option<&'static str>,
        }

        Ok(serde_json::to_string(&Request {
            action: "subscribe",
            market: Market {
                kind: "perp",
                id: self.market_id,
            },
            subscriptions: [SubscriptionRef {
                channel: "user_perp_open_orders",
                address: &self.address,
                address_type: self.expected_subaccounts.as_ref().map(|_| "wallet"),
            }],
            options: Options { compress: false },
        })?)
    }
}

/// One raw open-order page, without execution-report interpretation.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXWsPerpOpenOrdersPage {
    address: String,
    orders: SnapshotPage,
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderIdentity {
    order_id: String,
    owner: String,
    market_id: u64,
}

impl DeepXWsPerpOpenOrdersPage {
    /// Returns raw order items; no order-state meaning is assigned.
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

/// A frame admitted for one exact perpetual open-order subscription.
#[derive(Debug)]
pub enum DeepXWsPerpOpenOrdersFrame {
    /// The venue acknowledged exactly the requested channel and address.
    Subscribed,
    /// The venue sent one complete order page for the requested identity.
    OrderPage {
        /// Raw page, without order-report interpretation.
        page: DeepXWsPerpOpenOrdersPage,
        /// Venue frame timestamp in Unix milliseconds.
        timestamp_ms: u64,
    },
    /// Application heartbeat response.
    Pong,
}

impl DeepXWsPerpOpenOrdersFrame {
    /// Parses and binds one frame to a specific subscription request.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, unsupported, foreign, or incomplete frames.
    pub fn parse(text: &str, request: &DeepXWsPerpOpenOrdersRequest) -> Result<Self> {
        #[derive(Deserialize)]
        struct WireFrame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsPerpMarket>,
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
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX market"))?;
                ensure!(
                    market.kind == "perp" && market.id == request.market_id,
                    "foreign DeepX perpetual market"
                );
                if wire.kind == "subscribed" {
                    let subscriptions = wire
                        .subscriptions
                        .ok_or_else(|| anyhow::anyhow!("missing DeepX subscriptions"))?;
                    let mut observed = HashSet::with_capacity(subscriptions.len());
                    for subscription in subscriptions {
                        ensure!(
                            subscription.channel == "user_perp_open_orders"
                                && observed.insert(subscription.address.to_ascii_lowercase()),
                            "foreign or duplicate DeepX perpetual order subscription"
                        );
                    }
                    if let Some(expected) = &request.expected_subaccounts {
                        ensure!(
                            &observed == expected,
                            "DeepX perpetual wallet order acknowledgement differs from directory"
                        );
                    } else {
                        ensure!(
                            observed.len() == 1 && observed.contains(&request.address),
                            "foreign DeepX perpetual order subscription"
                        );
                    }
                    return Ok(Self::Subscribed);
                }
                ensure!(
                    wire.channel.as_deref() == Some("user_perp_open_orders"),
                    "unsupported DeepX perpetual order channel"
                );
                let data = wire
                    .data
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX order data"))?;
                let page: DeepXWsPerpOpenOrdersPage = serde_json::from_str(data.get())?;
                let page_address = page.address.to_ascii_lowercase();
                if let Some(expected) = &request.expected_subaccounts {
                    ensure!(
                        expected.contains(&page_address),
                        "foreign DeepX perpetual wallet order page"
                    );
                } else {
                    ensure!(
                        page_address == request.address,
                        "foreign DeepX perpetual order page"
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
                    "incomplete DeepX perpetual order page"
                );
                let mut order_ids = HashSet::new();
                for item in &orders.items {
                    let identity: OrderIdentity = serde_json::from_str(item.get())?;
                    let order_id = crate::parse::parse_canonical_order_id(&identity.order_id)
                        .ok_or_else(|| anyhow::anyhow!("invalid DeepX perpetual order ID"))?;
                    ensure!(
                        order_ids.insert(order_id)
                            && identity.owner.eq_ignore_ascii_case(&page.address)
                            && identity.market_id == u64::from(request.market_id),
                        "foreign or duplicate DeepX perpetual open order"
                    );
                }
                let timestamp_ms = wire
                    .timestamp
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX order timestamp"))?;
                ensure!(
                    timestamp_ms > 0 && i64::try_from(timestamp_ms).is_ok(),
                    "invalid DeepX perpetual order timestamp"
                );
                Ok(Self::OrderPage { page, timestamp_ms })
            }
            _ => anyhow::bail!("unsupported DeepX perpetual order frame"),
        }
    }
}
