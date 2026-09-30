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

//! Acknowledged public Spot trade subscriptions and observed trade-page envelopes.

use std::{collections::VecDeque, num::NonZeroUsize, time::Duration};

use serde::Deserialize;
use serde_json::{json, value::RawValue};

use crate::{config::DeepXNetworkConfig, http::DeepXSpotMarket};

/// One concrete Spot market echoed by a public WebSocket frame.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct DeepXWsSpotMarket {
    /// Market kind; consumers require `spot`.
    #[serde(rename = "type")]
    pub(crate) kind: String,
    /// Canonical market name.
    pub(crate) name: String,
    /// Optional bytes32 pair identity echoed by the venue.
    pub(crate) pair: Option<String>,
}

/// A raw, market-bound public Spot trade frame.
#[derive(Debug)]
pub(crate) enum DeepXWsSpotTradeFrame {
    /// Subscription acknowledgement.
    Subscribed {
        market: DeepXWsSpotMarket,
        subscriptions: Vec<String>,
    },
    /// Spot trade page; decimal values retain their original JSON lexemes.
    Data {
        market: DeepXWsSpotMarket,
        data: Box<RawValue>,
    },
    /// Application heartbeat response.
    Pong,
    /// Venue protocol error.
    Error { code: String, message: String },
}

impl DeepXWsSpotTradeFrame {
    /// Parses one observed Spot public frame and rejects other market kinds or channels.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed frames, missing fields, or unsupported frame types.
    pub(crate) fn parse(text: &str) -> anyhow::Result<Self> {
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
pub(crate) struct DeepXWsSpotTradeRequest {
    market_name: String,
}

impl DeepXWsSpotTradeRequest {
    /// Creates a Spot trade subscription request from finalized market metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the market name is empty or the pair is not a bytes32 identity.
    pub(crate) fn subscribe(market: &DeepXSpotMarket) -> anyhow::Result<Self> {
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
        })
    }

    pub(crate) fn to_text(&self) -> String {
        json!({
            "action": "subscribe",
            "market": {"type": "spot", "name": self.market_name},
            "subscriptions": ["trades"],
            "options": {"compress": false},
        })
        .to_string()
    }
}

/// A single-owner Spot connection with bounded pre-acknowledgement data buffering.
#[derive(Debug)]
pub(crate) struct DeepXWsSpotTradeConnection {
    transport: super::transport::DeepXWsReadConnection,
    market: DeepXSpotMarket,
    buffered: VecDeque<DeepXWsSpotTradeFrame>,
    buffer_capacity: NonZeroUsize,
    timeout: Duration,
    subscribed: bool,
    poisoned: bool,
}

impl DeepXWsSpotTradeConnection {
    /// Connects and acknowledges one Spot `trades` subscription.
    ///
    /// # Errors
    ///
    /// Returns an error for connection, acknowledgement, frame, or bounded-buffer failures.
    pub(crate) async fn connect(
        network: &DeepXNetworkConfig,
        proxy_url: Option<&str>,
        timeout: Duration,
        buffer_capacity: NonZeroUsize,
        market: DeepXSpotMarket,
    ) -> anyhow::Result<Self> {
        let mut connection = Self {
            transport: super::transport::DeepXWsReadConnection::connect(
                network, proxy_url, timeout,
            )
            .await?,
            market,
            buffered: VecDeque::new(),
            buffer_capacity,
            timeout,
            subscribed: false,
            poisoned: false,
        };
        connection.subscribe().await?;
        Ok(connection)
    }

    async fn subscribe(&mut self) -> anyhow::Result<()> {
        let request = DeepXWsSpotTradeRequest::subscribe(&self.market)?;
        self.transport.send_spot_trade_request(&request).await?;
        let result = tokio::time::timeout(self.timeout, async {
            loop {
                let frame = self.read_frame().await?;
                match &frame {
                    DeepXWsSpotTradeFrame::Subscribed {
                        market,
                        subscriptions,
                    } => {
                        anyhow::ensure!(
                            market.name == self.market.name
                                && market.pair.as_deref().is_some_and(
                                    |pair| pair.eq_ignore_ascii_case(&self.market.pair)
                                )
                                && subscriptions.as_slice() == ["trades"],
                            "DeepX Spot trade acknowledgement conflicts with subscription"
                        );
                        self.subscribed = true;
                        return Ok(());
                    }
                    DeepXWsSpotTradeFrame::Data { market, .. } => {
                        anyhow::ensure!(
                            self.matches_market(market),
                            "DeepX Spot pre-ack trade identity mismatch"
                        );
                        anyhow::ensure!(
                            self.buffered.len() < self.buffer_capacity.get(),
                            "DeepX Spot pre-ack trade buffer overflow"
                        );
                        self.buffered.push_back(frame);
                    }
                    DeepXWsSpotTradeFrame::Pong => {}
                    DeepXWsSpotTradeFrame::Error { code, message } => {
                        anyhow::bail!("DeepX Spot WebSocket error {code}: {message}");
                    }
                }
            }
        })
        .await;
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                self.poisoned = true;
                self.buffered.clear();
                Err(e)
            }
            Err(_) => {
                self.poisoned = true;
                self.buffered.clear();
                anyhow::bail!("DeepX Spot trade acknowledgement timed out");
            }
        }
    }

    /// Returns the next acknowledged Spot trade page or heartbeat.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign frame or bounded transport failure.
    pub(crate) async fn next_frame(&mut self) -> anyhow::Result<DeepXWsSpotTradeFrame> {
        anyhow::ensure!(
            self.subscribed && !self.poisoned,
            "DeepX Spot trade connection is terminal"
        );
        if let Some(frame) = self.buffered.pop_front() {
            return Ok(frame);
        }
        let frame = self.read_frame().await?;
        match &frame {
            DeepXWsSpotTradeFrame::Data { market, .. } => {
                anyhow::ensure!(
                    self.matches_market(market),
                    "DeepX Spot trade market mismatch"
                );
            }
            DeepXWsSpotTradeFrame::Pong => {}
            DeepXWsSpotTradeFrame::Error { code, message } => {
                self.poisoned = true;
                anyhow::bail!("DeepX Spot WebSocket error {code}: {message}");
            }
            DeepXWsSpotTradeFrame::Subscribed { .. } => {
                self.poisoned = true;
                anyhow::bail!("unexpected DeepX Spot subscription acknowledgement");
            }
        }
        Ok(frame)
    }

    /// Sends the documented public application heartbeat.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection is terminal or the send fails.
    pub(crate) async fn ping(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.poisoned, "DeepX Spot trade connection is terminal");
        self.transport
            .send_public_request(&super::public::DeepXWsPublicRequest::Ping)
            .await
    }

    /// Closes the connection and discards buffered evidence.
    ///
    /// # Errors
    ///
    /// Returns an error if the bounded transport close fails.
    pub(crate) async fn close(&mut self) -> anyhow::Result<()> {
        self.poisoned = true;
        self.buffered.clear();
        self.transport.close().await
    }

    fn matches_market(&self, market: &DeepXWsSpotMarket) -> bool {
        market.kind == "spot"
            && market.name == self.market.name
            && market
                .pair
                .as_deref()
                .is_some_and(|pair| pair.eq_ignore_ascii_case(&self.market.pair))
    }

    async fn read_frame(&mut self) -> anyhow::Result<DeepXWsSpotTradeFrame> {
        loop {
            match self.transport.receive().await? {
                Some(nautilus_network::transport::Message::Text(bytes)) => {
                    return DeepXWsSpotTradeFrame::parse(std::str::from_utf8(&bytes)?);
                }
                Some(
                    nautilus_network::transport::Message::Ping(_)
                    | nautilus_network::transport::Message::Pong(_),
                ) => {}
                _ => anyhow::bail!("DeepX Spot WebSocket closed or sent unsupported data"),
            }
        }
    }
}
