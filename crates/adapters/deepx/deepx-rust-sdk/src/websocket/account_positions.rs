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

//! Perpetual open-position subscription and raw page protocol.

use std::collections::HashSet;

use anyhow::{Result, ensure};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

use super::{account::DeepXWsUserBalancesRequest, public::DeepXWsPerpMarket};

/// One subaccount- or wallet-scoped perpetual open-position subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsPerpOpenPositionsRequest {
    market_id: u16,
    address: String,
    expected_subaccounts: Option<HashSet<String>>,
}

impl DeepXWsPerpOpenPositionsRequest {
    /// Creates a subscription for one market and subaccount.
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
            "DeepX perpetual position wallet subscription requires subaccounts"
        );
        let mut expected = HashSet::with_capacity(subaccounts.len());
        for subaccount in subaccounts {
            let address = DeepXWsUserBalancesRequest::new(subaccount)?
                .subaccount()
                .to_string();
            ensure!(
                expected.insert(address),
                "duplicate DeepX perpetual position wallet subaccount"
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

    /// Serializes the documented uncompressed, read-only subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_text(&self) -> Result<String> {
        let mut subscription = serde_json::json!({
            "channel": "user_perp_positions",
            "address": self.address,
            "status": "open",
        });
        if self.expected_subaccounts.is_some() {
            subscription["addressType"] = serde_json::json!("wallet");
        }
        Ok(serde_json::json!({
            "action": "subscribe",
            "market": {"type": "perp", "id": self.market_id},
            "subscriptions": [subscription],
            "options": {"compress": false},
        })
        .to_string())
    }
}

/// One complete initial page of raw positions, without position-report interpretation.
#[derive(Debug, Deserialize)]
pub struct DeepXWsPerpOpenPositionsPage {
    address: String,
    positions: SnapshotPage,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotPage {
    has_next: bool,
    has_prev: bool,
    #[serde(deserialize_with = "null_items_as_empty")]
    items: Vec<Box<RawValue>>,
    page_no: u64,
    page_size: u64,
    total_count: u64,
    total_page: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionIdentity {
    id: u64,
    owner: String,
    market_id: u64,
    status: String,
}

fn null_items_as_empty<'de, D>(deserializer: D) -> Result<Vec<Box<RawValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<Vec<Box<RawValue>>>::deserialize(deserializer)?.unwrap_or_default())
}

impl DeepXWsPerpOpenPositionsPage {
    /// Returns exact raw position items. A venue `null` item list is empty.
    #[must_use]
    pub fn items(&self) -> &[Box<RawValue>] {
        &self.positions.items
    }

    /// Returns the subaccount bound to this page.
    #[must_use]
    pub fn subaccount(&self) -> &str {
        &self.address
    }

    /// Returns whether two pages carry the same identity, pagination, and raw items.
    #[must_use]
    pub fn same_contents(&self, other: &Self) -> bool {
        let left = &self.positions;
        let right = &other.positions;
        self.address.eq_ignore_ascii_case(&other.address)
            && left.has_next == right.has_next
            && left.has_prev == right.has_prev
            && left.page_no == right.page_no
            && left.page_size == right.page_size
            && left.total_count == right.total_count
            && left.total_page == right.total_page
            && left.items.len() == right.items.len()
            && left
                .items
                .iter()
                .zip(&right.items)
                .all(|(a, b)| a.get() == b.get())
    }
}

/// A frame admitted for one exact open-position subscription.
#[derive(Debug)]
pub enum DeepXWsPerpOpenPositionsFrame {
    /// The venue acknowledged the requested market, address, and open status.
    Subscribed,
    /// The venue sent one complete initial page for the requested identity.
    PositionPage {
        /// Raw positions without account or lifecycle interpretation.
        page: DeepXWsPerpOpenPositionsPage,
        /// Venue frame timestamp in Unix milliseconds.
        timestamp_ms: u64,
    },
    /// Application heartbeat response.
    Pong,
}

impl DeepXWsPerpOpenPositionsFrame {
    /// Parses and binds one frame to the requested subscription.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, foreign, or incomplete frames.
    pub fn parse(text: &str, request: &DeepXWsPerpOpenPositionsRequest) -> Result<Self> {
        #[derive(Deserialize)]
        struct Subscription {
            channel: String,
            address: String,
            status: Option<String>,
            #[serde(rename = "onlyClosed")]
            only_closed: Option<bool>,
        }
        #[derive(Deserialize)]
        struct Frame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsPerpMarket>,
            subscriptions: Option<Vec<Subscription>>,
            channel: Option<String>,
            data: Option<Box<RawValue>>,
            timestamp: Option<u64>,
        }

        let frame: Frame = serde_json::from_str(text)?;
        if frame.kind == "pong" {
            return Ok(Self::Pong);
        }
        let market = frame
            .market
            .ok_or_else(|| anyhow::anyhow!("missing DeepX position market"))?;
        ensure!(
            market.kind == "perp" && market.id == request.market_id,
            "foreign DeepX position market"
        );
        match frame.kind.as_str() {
            "subscribed" => {
                let subscriptions = frame
                    .subscriptions
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX position subscriptions"))?;
                let mut observed = HashSet::with_capacity(subscriptions.len());
                for subscription in subscriptions {
                    ensure!(
                        subscription.channel == "user_perp_positions"
                            && subscription.status.as_deref() == Some("open")
                            && subscription.only_closed.is_none()
                            && observed.insert(subscription.address.to_ascii_lowercase()),
                        "foreign or duplicate DeepX position subscription"
                    );
                }
                if let Some(expected) = &request.expected_subaccounts {
                    ensure!(
                        &observed == expected,
                        "DeepX perpetual position wallet acknowledgement differs from directory"
                    );
                } else {
                    ensure!(
                        observed.len() == 1 && observed.contains(&request.address),
                        "foreign DeepX position subscription"
                    );
                }
                Ok(Self::Subscribed)
            }
            "data" => {
                ensure!(
                    frame.channel.as_deref() == Some("user_perp_positions"),
                    "foreign DeepX position channel"
                );
                let data = frame
                    .data
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX position data"))?;
                let page: DeepXWsPerpOpenPositionsPage = serde_json::from_str(data.get())?;
                let page_address = page.address.to_ascii_lowercase();
                if let Some(expected) = &request.expected_subaccounts {
                    ensure!(
                        expected.contains(&page_address),
                        "foreign DeepX perpetual position wallet page"
                    );
                } else {
                    ensure!(
                        page_address == request.address,
                        "foreign DeepX position page"
                    );
                }
                let positions = &page.positions;
                ensure!(
                    !positions.has_next
                        && !positions.has_prev
                        && positions.page_no == 1
                        && positions.page_size > 0
                        && page.items().len() as u64 <= positions.page_size
                        && positions.total_count == page.items().len() as u64
                        && positions.total_page == u64::from(!page.items().is_empty()),
                    "incomplete DeepX position page"
                );
                let mut ids = HashSet::new();
                for item in page.items() {
                    let identity: PositionIdentity = serde_json::from_str(item.get())?;
                    ensure!(
                        identity.id != 0
                            && ids.insert(identity.id)
                            && identity.owner.eq_ignore_ascii_case(&page.address)
                            && identity.market_id == u64::from(request.market_id)
                            && identity.status == "Open",
                        "foreign or duplicate DeepX open position"
                    );
                }
                let timestamp_ms = frame
                    .timestamp
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX position timestamp"))?;
                ensure!(
                    timestamp_ms > 0 && i64::try_from(timestamp_ms).is_ok(),
                    "invalid DeepX position timestamp"
                );
                Ok(Self::PositionPage { page, timestamp_ms })
            }
            _ => anyhow::bail!("unsupported DeepX position frame"),
        }
    }
}
