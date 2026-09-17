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

//! Documented public perpetual subscription messages; no account or transaction commands.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroUsize,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue};

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

/// Single-owner public connection with acknowledgement-gated data delivery.
#[derive(Debug)]
pub struct DeepXWsPublicConnection {
    transport: super::transport::DeepXWsReadConnection,
    confirmed: BTreeMap<u16, BTreeSet<DeepXWsPublicChannel>>,
    buffered: VecDeque<DeepXWsPublicFrame>,
    buffer_capacity: NonZeroUsize,
    timeout: Duration,
    poisoned: bool,
}

impl DeepXWsPublicConnection {
    /// Opens one public connection with a bounded pre-acknowledgement data buffer.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, upgrade failure or connection timeout.
    pub async fn connect(
        network: &crate::config::DeepXNetworkConfig,
        proxy_url: Option<&str>,
        timeout: Duration,
        buffer_capacity: NonZeroUsize,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            transport: super::transport::DeepXWsReadConnection::connect(
                network, proxy_url, timeout,
            )
            .await?,
            confirmed: BTreeMap::new(),
            buffered: VecDeque::new(),
            buffer_capacity,
            timeout,
            poisoned: false,
        })
    }

    /// Sends a public subscription once and waits for its exact market/channel acknowledgement.
    ///
    /// No wire correlator exists, so one mutable owner serializes requests. Data arriving before
    /// acknowledgement is buffered, never delivered until the matching acknowledgement arrives.
    /// Timeout or conflicting evidence poisons the connection; callers must close, not retry it.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid intent, terminal connection, send/ack failure or buffer overflow.
    pub async fn subscribe(
        &mut self,
        market_id: u16,
        channels: Vec<DeepXWsPublicChannel>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(!self.poisoned, "DeepX public connection is terminal");
        let request = DeepXWsPublicRequest::Subscribe {
            market_id,
            channels: channels.clone(),
        };
        self.subscribe_request(request, market_id, channels).await
    }

    /// Subscribes to a book with explicit depth and exact price aggregation size.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid options or acknowledgement/transport failure.
    pub async fn subscribe_book(
        &mut self,
        market_id: u16,
        depth: NonZeroUsize,
        price_size: rust_decimal::Decimal,
    ) -> anyhow::Result<()> {
        self.subscribe_request(
            DeepXWsPublicRequest::SubscribeBook {
                market_id,
                depth,
                price_size,
            },
            market_id,
            vec![DeepXWsPublicChannel::Orderbook],
        )
        .await
    }

    async fn subscribe_request(
        &mut self,
        request: DeepXWsPublicRequest,
        market_id: u16,
        channels: Vec<DeepXWsPublicChannel>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(!self.poisoned, "DeepX public connection is terminal");
        request.to_text()?;
        let expected: BTreeSet<_> = channels.into_iter().collect();
        let result = tokio::time::timeout(
            self.timeout,
            self.subscribe_inner(&request, market_id, &expected),
        )
        .await;
        match result {
            Ok(Ok(())) => {
                self.confirmed
                    .entry(market_id)
                    .or_default()
                    .extend(expected);
                Ok(())
            }
            Ok(Err(e)) => {
                self.poisoned = true;
                self.buffered.clear();
                Err(e)
            }
            Err(_) => {
                self.poisoned = true;
                self.buffered.clear();
                anyhow::bail!("DeepX public subscription acknowledgement timed out");
            }
        }
    }

    async fn subscribe_inner(
        &mut self,
        request: &DeepXWsPublicRequest,
        market_id: u16,
        expected: &BTreeSet<DeepXWsPublicChannel>,
    ) -> anyhow::Result<()> {
        self.transport.send_public_request(request).await?;
        loop {
            let frame = self.read_frame().await?;
            match &frame {
                DeepXWsPublicFrame::Subscribed { .. } => {
                    return validate_ack(&frame, market_id, expected);
                }
                DeepXWsPublicFrame::Data {
                    market, channel, ..
                } => {
                    anyhow::ensure!(
                        (market.id == market_id && expected.contains(channel))
                            || self
                                .confirmed
                                .get(&market.id)
                                .is_some_and(|set| set.contains(channel)),
                        "DeepX public data does not match subscription intent"
                    );
                    anyhow::ensure!(
                        self.buffered.len() < self.buffer_capacity.get(),
                        "DeepX public pre-ack buffer overflow"
                    );
                    self.buffered.push_back(frame);
                }
                DeepXWsPublicFrame::Pong { .. } => {}
                _ => anyhow::bail!("unexpected DeepX public subscription response"),
            }
        }
    }

    /// Returns confirmed public data or a heartbeat; foreign/unconfirmed messages fail closed.
    ///
    /// # Errors
    ///
    /// Returns an error for terminal connection, malformed/foreign data or bounded read failure.
    pub async fn next_frame(&mut self) -> anyhow::Result<DeepXWsPublicFrame> {
        anyhow::ensure!(!self.poisoned, "DeepX public connection is terminal");
        if let Some(frame) = self.buffered.pop_front() {
            return Ok(frame);
        }
        let result = tokio::time::timeout(self.timeout, self.read_frame()).await;
        let frame = match result {
            Ok(Ok(frame)) => frame,
            Ok(Err(e)) => {
                if !matches!(
                    e.downcast_ref::<super::DeepXWsError>(),
                    Some(super::DeepXWsError::ReceiveTimeout)
                ) {
                    self.poisoned = true;
                }
                return Err(e);
            }
            Err(_) => return Err(super::DeepXWsError::ReceiveTimeout.into()),
        };
        let valid = match &frame {
            DeepXWsPublicFrame::Data {
                market, channel, ..
            } => self
                .confirmed
                .get(&market.id)
                .is_some_and(|set| set.contains(channel)),
            DeepXWsPublicFrame::Pong { .. } => true,
            _ => false,
        };
        if !valid {
            self.poisoned = true;
            anyhow::bail!("unexpected or unconfirmed DeepX public frame");
        }
        Ok(frame)
    }

    async fn read_frame(&mut self) -> anyhow::Result<DeepXWsPublicFrame> {
        loop {
            match self.transport.receive().await? {
                Some(nautilus_network::transport::Message::Text(bytes)) => {
                    return DeepXWsPublicFrame::parse(std::str::from_utf8(&bytes)?);
                }
                Some(
                    nautilus_network::transport::Message::Ping(_)
                    | nautilus_network::transport::Message::Pong(_),
                ) => {}
                _ => anyhow::bail!(
                    "DeepX public connection closed or received unsupported binary data"
                ),
            }
        }
    }

    /// Sends the documented application heartbeat once on this connection.
    ///
    /// # Errors
    ///
    /// Returns an error for a terminal connection or bounded send failure; failure is terminal.
    pub async fn ping(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.poisoned, "DeepX public connection is terminal");
        let result = self
            .transport
            .send_public_request(&DeepXWsPublicRequest::Ping)
            .await;
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Closes the transport and discards all connection-owned subscription evidence.
    ///
    /// # Errors
    ///
    /// Returns an error if the bounded transport close fails.
    pub async fn close(&mut self) -> anyhow::Result<()> {
        self.poisoned = true;
        self.confirmed.clear();
        self.buffered.clear();
        self.transport.close().await
    }
}

fn validate_ack(
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
    #[rstest::rstest]
    fn explicit_book_options_preserve_numeric_decimal_lexeme() {
        let price_size: rust_decimal::Decimal = "0.000000000000000000123456789".parse().unwrap();
        let text = super::DeepXWsPublicRequest::SubscribeBook {
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
            super::DeepXWsPublicRequest::SubscribeBook {
                market_id: 2,
                depth: std::num::NonZeroUsize::new(40).unwrap(),
                price_size: rust_decimal::Decimal::ZERO,
            }
            .to_text()
            .is_err()
        );
    }

    use super::*;
    use rstest::rstest;

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

    struct Server(tokio::task::JoinHandle<()>);
    impl Drop for Server {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[tokio::test]
    async fn public_connection_gates_data_on_matching_ack(#[case] scenario: u8) {
        use axum::{
            Router,
            extract::ws::{Message, WebSocketUpgrade},
            routing::get,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/internal/v1/ws", get(move |upgrade: WebSocketUpgrade| async move {
            upgrade.on_upgrade(move |mut socket| async move {
                let Some(Ok(Message::Text(request))) = socket.recv().await else { panic!(); };
                let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                assert_eq!(request["market"]["id"], 2);
                assert_eq!(request["options"]["compress"], false);
                let data = Message::Text(r#"{"type":"data","channel":"trades","market":{"type":"perp","id":2},"data":{"items":[]},"timestamp":1}"#.into());
                if scenario != 0 { socket.send(data.clone()).await.unwrap(); }
                if scenario == 3 { socket.send(data.clone()).await.unwrap(); }
                let ack = json!({"type":"subscribed","market":{"type":"perp","id":if scenario == 2 {3} else {2}},"subscriptions":["trades"],"message":"Subscribed"});
                let _ = socket.send(Message::Text(ack.to_string().into())).await;
                if scenario == 0 { socket.send(data).await.unwrap(); }
                let _ = tokio::time::timeout(Duration::from_secs(2), socket.recv()).await;
            })
        }));
        let _server = Server(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let mut connection = DeepXWsPublicConnection::connect(
            &network,
            None,
            Duration::from_secs(1),
            NonZeroUsize::new(1).unwrap(),
        )
        .await
        .unwrap();
        let result = connection
            .subscribe(2, vec![DeepXWsPublicChannel::Trades])
            .await;
        if scenario < 2 {
            result.unwrap();
            assert!(matches!(
                connection.next_frame().await.unwrap(),
                DeepXWsPublicFrame::Data { .. }
            ));
        } else {
            assert!(result.is_err());
            assert!(connection.next_frame().await.is_err());
            assert!(
                connection
                    .subscribe(2, vec![DeepXWsPublicChannel::Trades])
                    .await
                    .is_err()
            );
        }
        connection.close().await.unwrap();
    }
}
