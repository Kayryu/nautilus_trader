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

/// Control frames accepted while awaiting an order-history subscription acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeepXWsOrderHistoryControlFrame {
    /// The requested market and subaccount were acknowledged.
    Subscribed,
    /// An application heartbeat arrived before the acknowledgement.
    Pong,
}

fn parse_control_frame(
    text: &str,
    validate_ack: impl FnOnce(&str) -> Result<()>,
) -> Result<DeepXWsOrderHistoryControlFrame> {
    #[derive(Deserialize)]
    struct FrameKind {
        #[serde(rename = "type")]
        kind: String,
    }

    if serde_json::from_str::<FrameKind>(text)?.kind == "pong" {
        return Ok(DeepXWsOrderHistoryControlFrame::Pong);
    }
    validate_ack(text)?;
    Ok(DeepXWsOrderHistoryControlFrame::Subscribed)
}

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

    /// Parses an acknowledgement or application heartbeat under this request's scope.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, foreign, or unsupported frames.
    pub fn parse_control_frame(&self, text: &str) -> Result<DeepXWsOrderHistoryControlFrame> {
        parse_control_frame(text, |text| self.validate_ack(text))
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

    /// Parses an acknowledgement or application heartbeat under this request's scope.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, foreign, or unsupported frames.
    pub fn parse_control_frame(&self, text: &str) -> Result<DeepXWsOrderHistoryControlFrame> {
        parse_control_frame(text, |text| self.validate_ack(text))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const SUBACCOUNT: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
    const PERP_ACK: &str = r#"{"type":"subscribed","market":{"type":"perp","id":3,"name":"ETH-USDC"},"subscriptions":[{"channel":"user_perp_order_history","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"}],"message":"Successfully subscribed to 1 channels"}"#;
    const SPOT_PAIR: &str = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
    const SPOT_ACK: &str = r#"{"type":"subscribed","market":{"type":"spot","name":"ETH/USDC","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"},"subscriptions":[{"channel":"user_spot_order_history","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"}],"message":"Successfully subscribed to 1 channels"}"#;

    #[rstest]
    fn binds_perp_ack_to_exact_scope() {
        let request = DeepXWsPerpOrderHistoryRequest::new(3, SUBACCOUNT).unwrap();
        let encoded: serde_json::Value = serde_json::from_str(&request.to_text()).unwrap();
        assert_eq!(encoded["market"]["id"], 3);
        assert_eq!(encoded["subscriptions"][0]["address"], SUBACCOUNT);
        assert_eq!(
            encoded["subscriptions"][0]["channel"],
            "user_perp_order_history"
        );
        request.validate_ack(PERP_ACK).unwrap();
        assert_eq!(
            request.parse_control_frame(PERP_ACK).unwrap(),
            DeepXWsOrderHistoryControlFrame::Subscribed
        );
        assert_eq!(
            request.parse_control_frame(r#"{"type":"pong"}"#).unwrap(),
            DeepXWsOrderHistoryControlFrame::Pong
        );

        for foreign in [
            PERP_ACK.replace("\"id\":3", "\"id\":4"),
            PERP_ACK.replace("user_perp_order_history", "user_perp_open_orders"),
            PERP_ACK.replace(SUBACCOUNT, "0x0000000000000000000000000000000000000000"),
            PERP_ACK.replace("\"type\":\"subscribed\"", "\"type\":\"data\""),
        ] {
            assert!(request.validate_ack(&foreign).is_err());
            assert!(request.parse_control_frame(&foreign).is_err());
        }
        assert!(request.parse_control_frame(r#"{"type":"data"}"#).is_err());
    }

    #[rstest]
    fn binds_spot_ack_to_exact_scope() {
        let response: serde_json::Value = serde_json::from_str(include_str!(
            "../../test_data/http/testnet/spot_markets.json"
        ))
        .unwrap();
        let markets: Vec<DeepXSpotMarket> =
            serde_json::from_value(response["data"].clone()).unwrap();
        let market = markets
            .iter()
            .find(|market| market.name == "ETH/USDC")
            .unwrap();
        let request = DeepXWsSpotOrderHistoryRequest::new(market, SUBACCOUNT).unwrap();
        let encoded: serde_json::Value = serde_json::from_str(&request.to_text()).unwrap();
        assert_eq!(encoded["market"]["name"], "ETH/USDC");
        assert_eq!(encoded["subscriptions"][0]["address"], SUBACCOUNT);
        assert_eq!(
            encoded["subscriptions"][0]["channel"],
            "user_spot_order_history"
        );
        request.validate_ack(SPOT_ACK).unwrap();
        assert_eq!(
            request.parse_control_frame(SPOT_ACK).unwrap(),
            DeepXWsOrderHistoryControlFrame::Subscribed
        );
        assert_eq!(
            request.parse_control_frame(r#"{"type":"pong"}"#).unwrap(),
            DeepXWsOrderHistoryControlFrame::Pong
        );

        for foreign in [
            SPOT_ACK.replace("ETH/USDC", "SOL/USDC"),
            SPOT_ACK.replace(SPOT_PAIR, &format!("0x{}", "00".repeat(32))),
            SPOT_ACK.replace("user_spot_order_history", "user_spot_open_orders"),
            SPOT_ACK.replace(SUBACCOUNT, "0x0000000000000000000000000000000000000000"),
        ] {
            assert!(request.validate_ack(&foreign).is_err());
            assert!(request.parse_control_frame(&foreign).is_err());
        }
        assert!(request.parse_control_frame(r#"{"type":"data"}"#).is_err());
    }
}
