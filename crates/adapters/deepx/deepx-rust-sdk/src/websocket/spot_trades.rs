// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Public Spot trade subscription request and frame protocol.

use serde::Deserialize;
use serde_json::{json, value::RawValue};

use crate::rest::models::DeepXSpotMarket;

/// One concrete Spot market echoed by a public WebSocket frame.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct DeepXWsSpotMarket {
    /// Market kind; consumers require `spot`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Canonical market name.
    pub name: String,
    /// Optional bytes32 pair identity echoed by the venue.
    pub pair: Option<String>,
}

/// A raw, market-bound public Spot trade frame.
#[derive(Debug)]
pub enum DeepXWsSpotTradeFrame {
    /// Subscription acknowledgement.
    Subscribed {
        /// Echoed Spot market identity.
        market: DeepXWsSpotMarket,
        /// Channels acknowledged by the server.
        subscriptions: Vec<String>,
    },
    /// Spot trade page; decimal values retain their original JSON lexemes.
    Data {
        /// Echoed Spot market identity.
        market: DeepXWsSpotMarket,
        /// Unmodified venue payload.
        data: Box<RawValue>,
    },
    /// Application heartbeat response.
    Pong,
    /// Venue protocol error.
    Error {
        /// Venue error code.
        code: String,
        /// Venue error message.
        message: String,
    },
}

impl DeepXWsSpotTradeFrame {
    /// Parses one observed Spot public frame and rejects other market kinds or channels.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed frames, missing fields, or unsupported frame types.
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct WireFrame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsSpotMarket>,
            subscriptions: Option<Vec<String>>,
            message: Option<String>,
            channel: Option<String>,
            data: Option<Box<RawValue>>,
            timestamp: Option<u64>,
            code: Option<String>,
        }

        fn required<T>(value: Option<T>, name: &str) -> anyhow::Result<T> {
            value.ok_or_else(|| anyhow::anyhow!("DeepX Spot frame missing {name}"))
        }

        let wire: WireFrame = serde_json::from_str(text)?;
        let frame = match wire.kind.as_str() {
            "subscribed" => {
                let market = required(wire.market, "market")?;
                let subscriptions = required(wire.subscriptions, "subscriptions")?;
                let _ = required(wire.message, "message")?;
                Self::Subscribed {
                    market,
                    subscriptions,
                }
            }
            "data" => {
                anyhow::ensure!(
                    wire.channel.as_deref() == Some("trades"),
                    "unsupported DeepX Spot data channel"
                );
                let _ = required(wire.timestamp, "timestamp")?;
                Self::Data {
                    market: required(wire.market, "market")?,
                    data: required(wire.data, "data")?,
                }
            }
            "pong" => {
                let _ = required(wire.timestamp, "timestamp")?;
                Self::Pong
            }
            "error" => Self::Error {
                code: required(wire.code, "code")?,
                message: required(wire.message, "message")?,
            },
            _ => anyhow::bail!("unsupported DeepX Spot frame type"),
        };
        let market = match &frame {
            Self::Subscribed { market, .. } | Self::Data { market, .. } => Some(market),
            Self::Pong | Self::Error { .. } => None,
        };
        anyhow::ensure!(
            market.is_none_or(|market| market.kind == "spot" && !market.name.is_empty()),
            "DeepX public frame is not a Spot market"
        );
        Ok(frame)
    }
}

/// One typed public Spot trade subscription request.
#[derive(Clone, Debug)]
pub struct DeepXWsSpotTradeRequest {
    market_name: String,
    pair: String,
}

impl DeepXWsSpotTradeRequest {
    /// Creates a Spot trade subscription request from finalized market metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the market name is empty or the pair is not a bytes32 identity.
    pub fn subscribe(market: &DeepXSpotMarket) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !market.name.trim().is_empty(),
            "DeepX Spot market name is empty"
        );
        anyhow::ensure!(
            market.pair.len() == 66
                && market.pair.starts_with("0x")
                && market.pair.as_bytes()[2..]
                    .iter()
                    .all(u8::is_ascii_hexdigit),
            "DeepX Spot market pair is invalid"
        );
        Ok(Self {
            market_name: market.name.clone(),
            pair: market.pair.clone(),
        })
    }

    /// Returns whether an echoed frame market matches the verified Spot market.
    #[must_use]
    pub fn matches_market(&self, market: &DeepXWsSpotMarket) -> bool {
        market.kind == "spot"
            && market.name == self.market_name
            && market
                .pair
                .as_deref()
                .is_some_and(|pair| pair.eq_ignore_ascii_case(&self.pair))
    }

    /// Validates the market and channels echoed by a subscription acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error if the acknowledgement differs from this request.
    pub fn validate_ack(
        &self,
        market: &DeepXWsSpotMarket,
        subscriptions: &[String],
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.matches_market(market) && subscriptions == ["trades"],
            "DeepX Spot trade acknowledgement conflicts with subscription"
        );
        Ok(())
    }

    /// Serializes the uncompressed public Spot trade subscription.
    #[must_use]
    pub fn to_text(&self) -> String {
        json!({
            "action": "subscribe",
            "market": {"type": "spot", "name": self.market_name},
            "subscriptions": ["trades"],
            "options": {"compress": false},
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::Value;

    use super::*;

    fn request() -> DeepXWsSpotTradeRequest {
        let response: Value = serde_json::from_str(include_str!(
            "../../test_data/http/testnet/spot_market_eth_usdc_by_pair.json"
        ))
        .unwrap();
        let market: DeepXSpotMarket = serde_json::from_value(response["data"].clone()).unwrap();
        DeepXWsSpotTradeRequest::subscribe(&market).unwrap()
    }

    #[rstest]
    fn acknowledgement_and_data_require_exact_spot_market() {
        let request = request();
        let pair = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
        let market = DeepXWsSpotMarket {
            kind: "spot".to_string(),
            name: "ETH/USDC".to_string(),
            pair: Some(format!("0x{}", pair[2..].to_ascii_uppercase())),
        };
        assert!(request.matches_market(&market));
        assert!(
            request
                .validate_ack(&market, &["trades".to_string()])
                .is_ok()
        );
        assert!(request.validate_ack(&market, &[]).is_err());
        assert!(
            request
                .validate_ack(&market, &["trades".to_string(), "trades".to_string()])
                .is_err()
        );

        let mut foreign = market.clone();
        foreign.name = "SOL/USDC".to_string();
        assert!(!request.matches_market(&foreign));
        assert!(
            request
                .validate_ack(&foreign, &["trades".to_string()])
                .is_err()
        );
        foreign = market;
        foreign.pair = None;
        assert!(!request.matches_market(&foreign));
        foreign.pair = Some(format!("0x{}", "0".repeat(64)));
        assert!(!request.matches_market(&foreign));
    }

    #[rstest]
    fn data_frame_preserves_numeric_lexemes() {
        let text = r#"{"type":"data","market":{"type":"spot","name":"ETH-USDC"},"channel":"trades","data":{"price":1.2300},"timestamp":1}"#;
        let DeepXWsSpotTradeFrame::Data { market, data } =
            DeepXWsSpotTradeFrame::parse(text).unwrap()
        else {
            panic!("expected Spot trade data frame");
        };
        assert_eq!(market.name, "ETH-USDC");
        assert!(data.get().contains("1.2300"));
    }

    #[rstest]
    #[case("perp", "trades")]
    #[case("spot", "orders")]
    fn rejects_foreign_market_or_channel(#[case] market: &str, #[case] channel: &str) {
        let text = format!(
            "{{\"type\":\"data\",\"market\":{{\"type\":\"{market}\",\"name\":\"ETH-USDC\"}},\"channel\":\"{channel}\",\"data\":{{}},\"timestamp\":1}}"
        );
        assert!(DeepXWsSpotTradeFrame::parse(&text).is_err());
    }
}
