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

//! Address-scoped incremental order-history subscription protocol.

use anyhow::{Result, ensure};
use serde::Deserialize;

use super::{
    account::DeepXWsUserBalancesRequest,
    public::DeepXWsPerpMarket,
    spot_trades::{DeepXWsSpotMarket, DeepXWsSpotTradeRequest},
};
use crate::rest::models::DeepXSpotMarket;

/// One market- and subaccount-bound incremental order-history subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsPerpOrderHistoryRequest {
    market_id: u16,
    subaccount: String,
}

impl DeepXWsPerpOrderHistoryRequest {
    /// Creates a read-only subscription for one market and subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero market ID or invalid AccountId20.
    pub fn new(market_id: u16, subaccount: &str) -> Result<Self> {
        ensure!(market_id != 0, "DeepX perpetual market ID must be positive");
        Ok(Self {
            market_id,
            subaccount: DeepXWsUserBalancesRequest::new(subaccount)?
                .subaccount()
                .to_string(),
        })
    }

    /// Returns the selected market ID.
    #[must_use]
    pub const fn market_id(&self) -> u16 {
        self.market_id
    }

    /// Returns the canonical selected subaccount.
    #[must_use]
    pub fn subaccount(&self) -> &str {
        &self.subaccount
    }

    /// Serializes the documented uncompressed subscription.
    #[must_use]
    pub fn to_text(&self) -> String {
        serde_json::json!({
            "action": "subscribe",
            "market": {"type": "perp", "id": self.market_id},
            "subscriptions": [{
                "channel": "user_perp_order_history",
                "address": self.subaccount,
            }],
            "options": {"compress": false},
        })
        .to_string()
    }

    /// Validates an acknowledgement against this request.
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
            ack.kind == "subscribed"
                && ack.market.kind == "perp"
                && ack.market.id == self.market_id
                && ack.subscriptions.len() == 1,
            "foreign DeepX perpetual order-history acknowledgement"
        );
        let subscription = &ack.subscriptions[0];
        ensure!(
            subscription.channel == "user_perp_order_history"
                && subscription.address.eq_ignore_ascii_case(&self.subaccount),
            "foreign DeepX perpetual order-history subscription"
        );
        Ok(())
    }
}

/// One Spot market- and subaccount-bound incremental order-history subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsSpotOrderHistoryRequest {
    market_name: String,
    pair: String,
    subaccount: String,
}

impl DeepXWsSpotOrderHistoryRequest {
    /// Creates a read-only subscription for one verified Spot market and subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid market identity or AccountId20.
    pub fn new(market: &DeepXSpotMarket, subaccount: &str) -> Result<Self> {
        DeepXWsSpotTradeRequest::subscribe(market)?;
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

    /// Returns the canonical selected subaccount.
    #[must_use]
    pub fn subaccount(&self) -> &str {
        &self.subaccount
    }

    /// Serializes the documented uncompressed subscription.
    #[must_use]
    pub fn to_text(&self) -> String {
        serde_json::json!({
            "action": "subscribe",
            "market": {"type": "spot", "name": self.market_name},
            "subscriptions": [{
                "channel": "user_spot_order_history",
                "address": self.subaccount,
            }],
            "options": {"compress": false},
        })
        .to_string()
    }

    /// Validates an acknowledgement against this request.
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
            market: DeepXWsSpotMarket,
            subscriptions: Vec<Subscription>,
        }

        let ack: Ack = serde_json::from_str(text)?;
        ensure!(
            ack.kind == "subscribed"
                && ack.market.kind == "spot"
                && ack.market.name == self.market_name
                && ack
                    .market
                    .pair
                    .as_deref()
                    .is_some_and(|pair| pair.eq_ignore_ascii_case(&self.pair))
                && ack.subscriptions.len() == 1,
            "foreign DeepX Spot order-history acknowledgement"
        );
        let subscription = &ack.subscriptions[0];
        ensure!(
            subscription.channel == "user_spot_order_history"
                && subscription.address.eq_ignore_ascii_case(&self.subaccount),
            "foreign DeepX Spot order-history subscription"
        );
        Ok(())
    }
}
