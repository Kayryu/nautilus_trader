// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Documented public perpetual subscription messages and frames.

use std::{collections::BTreeSet, num::NonZeroUsize};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue};

use crate::decimal::exact_decimal;

/// Public perpetual channels with documented simple subscription names.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeepXWsPublicChannel {
    /// Price-level book snapshots and deltas.
    Orderbook,
    /// Public executed trades.
    Trades,
    /// Last execution price.
    LatestPrice,
    /// Oracle price observations.
    OraclePrice,
    /// Mark price observations.
    MarkPrice,
    /// Funding rate observations.
    FundingRate,
    /// Public open-interest observations.
    OpenInterest,
}

/// Read-only application commands, deliberately excluding private account subscriptions.
#[derive(Clone, Debug)]
pub enum DeepXWsPublicRequest {
    /// Subscribes to a configured public price-level book without compression.
    SubscribeBook {
        market_id: u16,
        depth: NonZeroUsize,
        price_size: rust_decimal::Decimal,
    },
    /// Subscribes to simple public channels for one perpetual market, disabling compression.
    Subscribe {
        market_id: u16,
        channels: Vec<DeepXWsPublicChannel>,
    },
    /// Removes public channels for one perpetual market.
    Unsubscribe {
        market_id: u16,
        channels: Vec<DeepXWsPublicChannel>,
    },
    /// Requests the documented application-level heartbeat response.
    Ping,
}

impl DeepXWsPublicRequest {
    /// Serializes the documented envelope without inventing a numeric request correlator.
    ///
    /// # Errors
    ///
    /// Returns an error for empty or duplicate subscription channels.
    pub fn to_text(&self) -> anyhow::Result<String> {
        if let Self::SubscribeBook {
            market_id,
            depth,
            price_size,
        } = self
        {
            #[derive(Serialize)]
            struct Options {
                compress: bool,
                orderbook_depth: usize,
                orderbook_price_size: Box<RawValue>,
            }
            #[derive(Serialize)]
            struct Request {
                action: &'static str,
                market: DeepXWsPerpMarket,
                subscriptions: [DeepXWsPublicChannel; 1],
                options: Options,
            }
            anyhow::ensure!(
                *price_size > rust_decimal::Decimal::ZERO,
                "DeepX book price size must be positive"
            );
            return Ok(serde_json::to_string(&Request {
                action: "subscribe",
                market: DeepXWsPerpMarket {
                    kind: "perp".into(),
                    id: *market_id,
                    name: None,
                },
                subscriptions: [DeepXWsPublicChannel::Orderbook],
                options: Options {
                    compress: false,
                    orderbook_depth: depth.get(),
                    orderbook_price_size: RawValue::from_string(price_size.to_string())?,
                },
            })?);
        }
        let (action, market_id, channels) = match self {
            Self::Subscribe {
                market_id,
                channels,
            } => ("subscribe", market_id, channels),
            Self::Unsubscribe {
                market_id,
                channels,
            } => ("unsubscribe", market_id, channels),
            Self::Ping => return Ok(json!({"action": "ping"}).to_string()),
            Self::SubscribeBook { .. } => unreachable!("book request handled above"),
        };
        anyhow::ensure!(
            !channels.is_empty(),
            "DeepX public subscriptions must be nonempty"
        );
        anyhow::ensure!(
            channels.iter().collect::<BTreeSet<_>>().len() == channels.len(),
            "DeepX public subscription channels must be unique"
        );
        let mut request = json!({"action": action, "market": {"type": "perp", "id": market_id}, "subscriptions": channels});
        if matches!(self, Self::Subscribe { .. }) {
            request["options"] = json!({"compress": false});
        }
        Ok(request.to_string())
    }
}

/// Concrete perpetual market identity on a public message.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct DeepXWsPerpMarket {
    /// Market kind; public consumers require this to equal `perp`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Concrete perpetual market ID.
    pub id: u16,
    /// Optional display name; never used as an alternative to ID binding.
    pub name: Option<String>,
}

/// Schema-conservative public server envelope, preserving the payload's numeric lexemes.
#[derive(Debug)]
pub enum DeepXWsPublicFrame {
    /// Subscription acknowledgement; callers still bind market and channels to pending intent.
    Subscribed {
        market: DeepXWsPerpMarket,
        subscriptions: Vec<DeepXWsPublicChannel>,
        message: String,
    },
    /// Subscription-set replacement acknowledgement.
    SubscriptionsChanged {
        market: DeepXWsPerpMarket,
        subscriptions: Vec<DeepXWsPublicChannel>,
        message: String,
    },
    /// Public data; payload semantics are channel-specific and not inferred here.
    Data {
        market: DeepXWsPerpMarket,
        channel: DeepXWsPublicChannel,
        data: Box<RawValue>,
        timestamp: u64,
    },
    /// Application heartbeat response; timestamp units are not inferred.
    Pong { timestamp: u64 },
    /// Server protocol error, not a trading rejection or proof of non-delivery.
    Error { code: String, message: String },
}

impl DeepXWsPublicFrame {
    /// Parses a public envelope and rejects foreign market kinds.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed/unsupported frames or non-perpetual market identity.
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        // Direct struct decoding preserves RawValue without internally-tagged enum buffering
        #[derive(Deserialize)]
        struct WireFrame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsPerpMarket>,
            subscriptions: Option<Vec<DeepXWsPublicChannel>>,
            message: Option<String>,
            channel: Option<DeepXWsPublicChannel>,
            data: Option<Box<RawValue>>,
            timestamp: Option<u64>,
            code: Option<String>,
        }
        fn required<T>(value: Option<T>, name: &str) -> anyhow::Result<T> {
            value.ok_or_else(|| anyhow::anyhow!("DeepX public frame missing {name}"))
        }
        let wire: WireFrame = serde_json::from_str(text)?;
        let frame = match wire.kind.as_str() {
            "subscribed" => Self::Subscribed {
                market: required(wire.market, "market")?,
                subscriptions: required(wire.subscriptions, "subscriptions")?,
                message: required(wire.message, "message")?,
            },
            "subscriptions_changed" => Self::SubscriptionsChanged {
                market: required(wire.market, "market")?,
                subscriptions: required(wire.subscriptions, "subscriptions")?,
                message: required(wire.message, "message")?,
            },
            "data" => Self::Data {
                market: required(wire.market, "market")?,
                channel: required(wire.channel, "channel")?,
                data: required(wire.data, "data")?,
                timestamp: required(wire.timestamp, "timestamp")?,
            },
            "pong" => Self::Pong {
                timestamp: required(wire.timestamp, "timestamp")?,
            },
            "error" => Self::Error {
                code: required(wire.code, "code")?,
                message: required(wire.message, "message")?,
            },
            _ => anyhow::bail!("unsupported DeepX public frame type"),
        };
        let market = match &frame {
            Self::Subscribed { market, .. }
            | Self::SubscriptionsChanged { market, .. }
            | Self::Data { market, .. } => Some(market),
            _ => None,
        };
        anyhow::ensure!(
            market.is_none_or(|market| market.kind == "perp"),
            "DeepX public frame is not perpetual"
        );
        Ok(frame)
    }
}

/// Parses an exact positive scalar price from a public price channel.
///
/// # Errors
///
/// Returns an error for a string, malformed number, unrepresentable decimal, or nonpositive price.
pub fn parse_public_price(raw: &RawValue) -> anyhow::Result<Decimal> {
    #[derive(Deserialize)]
    #[serde(transparent)]
    struct ExactNumber(#[serde(deserialize_with = "exact_decimal::deserialize")] Decimal);

    anyhow::ensure!(
        !raw.get().starts_with('"'),
        "DeepX public price payload is not a scalar JSON number"
    );
    let value = serde_json::from_str::<ExactNumber>(raw.get())?.0;
    anyhow::ensure!(value > Decimal::ZERO, "DeepX public price must be positive");
    Ok(value)
}

/// Parses an exact funding rate after validating the venue's funding timestamps.
///
/// # Errors
///
/// Returns an error for malformed values or funding timestamps later than the frame timestamp.
pub fn parse_public_funding_rate(raw: &RawValue, timestamp_ms: u64) -> anyhow::Result<Decimal> {
    #[derive(Deserialize)]
    struct FundingPayload {
        #[serde(deserialize_with = "exact_decimal::deserialize")]
        funding_rate: Decimal,
        last_cacl_funding_rate_time: u64,
        last_funding_rate_time: u64,
    }

    let payload: FundingPayload = serde_json::from_str(raw.get())?;
    anyhow::ensure!(
        payload.last_cacl_funding_rate_time <= payload.last_funding_rate_time
            && payload.last_funding_rate_time <= timestamp_ms,
        "DeepX funding timestamps are inconsistent"
    );
    Ok(payload.funding_rate)
}

/// Validates that a subscription acknowledgement matches the pending market and channels.
pub fn validate_ack(
    frame: &DeepXWsPublicFrame,
    market_id: u16,
    expected: &BTreeSet<DeepXWsPublicChannel>,
) -> anyhow::Result<()> {
    let DeepXWsPublicFrame::Subscribed {
        market,
        subscriptions,
        ..
    } = frame
    else {
        anyhow::bail!("DeepX public response is not a subscription acknowledgement");
    };
    let actual: BTreeSet<_> = subscriptions.iter().copied().collect();
    anyhow::ensure!(
        market.kind == "perp"
            && market.id == market_id
            && subscriptions.len() == actual.len()
            && &actual == expected,
        "DeepX public subscription acknowledgement conflicts with pending intent"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    fn price_payload_preserves_exact_scientific_value() {
        let raw = RawValue::from_string("2.384511399e3".to_string()).unwrap();

        assert_eq!(
            parse_public_price(&raw).unwrap(),
            Decimal::from_str_exact("2384.511399").unwrap()
        );
        for invalid in [
            "0",
            "-1",
            "\"2384.51\"",
            "2384.51000000000000000000000000001",
        ] {
            let raw = RawValue::from_string(invalid.to_string()).unwrap();
            assert!(parse_public_price(&raw).is_err());
        }
    }

    #[rstest]
    fn funding_payload_requires_ordered_timestamps() {
        let raw = RawValue::from_string(
            r#"{"funding_rate":0.000108085430235316,"last_cacl_funding_rate_time":10,"last_funding_rate_time":12}"#.to_string(),
        )
        .unwrap();
        assert_eq!(
            parse_public_funding_rate(&raw, 12).unwrap(),
            Decimal::from_str_exact("0.000108085430235316").unwrap()
        );
        assert!(parse_public_funding_rate(&raw, 11).is_err());
        let invalid = RawValue::from_string(
            r#"{"funding_rate":0.1,"last_cacl_funding_rate_time":13,"last_funding_rate_time":12}"#
                .to_string(),
        )
        .unwrap();
        assert!(parse_public_funding_rate(&invalid, 13).is_err());
    }

    #[rstest]
    #[case(2, vec![DeepXWsPublicChannel::Trades], true)]
    #[case(3, vec![DeepXWsPublicChannel::Trades], false)]
    #[case(2, vec![DeepXWsPublicChannel::Orderbook], false)]
    #[case(2, vec![DeepXWsPublicChannel::Trades; 2], false)]
    fn acknowledgements_bind_exact_market_and_channels(
        #[case] id: u16,
        #[case] subscriptions: Vec<DeepXWsPublicChannel>,
        #[case] valid: bool,
    ) {
        let frame = DeepXWsPublicFrame::Subscribed {
            market: DeepXWsPerpMarket {
                kind: "perp".to_string(),
                id,
                name: None,
            },
            subscriptions,
            message: "Successfully subscribed to 1 channels".to_string(),
        };
        assert_eq!(
            validate_ack(&frame, 2, &BTreeSet::from([DeepXWsPublicChannel::Trades])).is_ok(),
            valid
        );
    }

    #[rstest]
    fn explicit_book_options_preserve_numeric_decimal_lexeme() {
        let price_size: rust_decimal::Decimal = "0.000000000000000000123456789".parse().unwrap();
        let text = DeepXWsPublicRequest::SubscribeBook {
            market_id: 2,
            depth: std::num::NonZeroUsize::new(40).unwrap(),
            price_size,
        }
        .to_text()
        .unwrap();
        assert!(text.contains("\"orderbook_price_size\":0.000000000000000000123456789"));
        let request: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(request["options"]["compress"], false);
        assert_eq!(request["options"]["orderbook_depth"], 40);
        assert!(
            DeepXWsPublicRequest::SubscribeBook {
                market_id: 2,
                depth: std::num::NonZeroUsize::new(40).unwrap(),
                price_size: rust_decimal::Decimal::ZERO,
            }
            .to_text()
            .is_err()
        );
    }

    #[rstest]
    fn public_request_uses_documented_envelope_and_disables_compression() {
        let request = DeepXWsPublicRequest::Subscribe {
            market_id: 2,
            channels: vec![DeepXWsPublicChannel::Trades],
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&request.to_text().unwrap()).unwrap(),
            json!({"action":"subscribe","market":{"type":"perp","id":2},"subscriptions":["trades"],"options":{"compress":false}})
        );
        assert_eq!(
            DeepXWsPublicRequest::Ping.to_text().unwrap(),
            "{\"action\":\"ping\"}"
        );
        let request = DeepXWsPublicRequest::Unsubscribe {
            market_id: 2,
            channels: vec![DeepXWsPublicChannel::Trades],
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&request.to_text().unwrap()).unwrap(),
            json!({"action":"unsubscribe","market":{"type":"perp","id":2},"subscriptions":["trades"]})
        );
        for channels in [vec![], vec![DeepXWsPublicChannel::Trades; 2]] {
            assert!(
                DeepXWsPublicRequest::Subscribe {
                    market_id: 2,
                    channels
                }
                .to_text()
                .is_err()
            );
        }
    }

    #[rstest]
    fn data_payload_preserves_exact_numeric_lexemes() {
        let frame = DeepXWsPublicFrame::parse(r#"{"type":"data","channel":"trades","market":{"type":"perp","id":2},"data":{"price":123.1234567890123456789012345678},"timestamp":1789378785219}"#).unwrap();
        let DeepXWsPublicFrame::Data { data, .. } = frame else {
            panic!();
        };
        assert_eq!(data.get(), r#"{"price":123.1234567890123456789012345678}"#);
    }

    #[rstest]
    #[case(r#"{"type":"data","channel":"trades","market":{"type":"spot","id":2},"data":{},"timestamp":1}"#)]
    #[case(r#"{"type":"data","channel":"trades","market":{"type":"perp","id":65536},"data":{},"timestamp":1}"#)]
    #[case(r#"{"type":"data","channel":"user_balances","market":{"type":"perp","id":2},"data":{},"timestamp":1}"#)]
    #[case(r#"{"type":"data","channel":"trades","market":{"type":"perp","id":2},"data":{},"timestamp":-1}"#)]
    #[case(r#"{"type":"data","channel":"trades","market":{"type":"perp","id":2},"data":{},"timestamp":null}"#)]
    fn unsupported_public_envelopes_are_rejected(#[case] text: &str) {
        assert!(DeepXWsPublicFrame::parse(text).is_err());
    }
}
