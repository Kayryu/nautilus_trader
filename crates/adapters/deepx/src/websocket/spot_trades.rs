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

pub(crate) use deepx_rust_sdk::websocket::spot_trades::{
    DeepXWsSpotTradeFrame, DeepXWsSpotTradeRequest,
};

use crate::{config::DeepXNetworkConfig, http::DeepXSpotMarket};

/// A single-owner Spot connection with bounded pre-acknowledgement data buffering.
#[derive(Debug)]
pub(crate) struct DeepXWsSpotTradeConnection {
    transport: super::transport::DeepXWsReadConnection,
    request: DeepXWsSpotTradeRequest,
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
        let request = DeepXWsSpotTradeRequest::subscribe(&market)?;
        let mut connection = Self {
            transport: super::transport::DeepXWsReadConnection::connect(
                network, proxy_url, timeout,
            )
            .await?,
            request,
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
        self.transport
            .send_spot_trade_request(&self.request)
            .await?;
        let result = tokio::time::timeout(self.timeout, async {
            loop {
                let frame = self.read_frame().await?;
                match &frame {
                    DeepXWsSpotTradeFrame::Subscribed {
                        market,
                        subscriptions,
                    } => {
                        self.request.validate_ack(market, subscriptions)?;
                        self.subscribed = true;
                        return Ok(());
                    }
                    DeepXWsSpotTradeFrame::Data { market, .. } => {
                        anyhow::ensure!(
                            self.request.matches_market(market),
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
                    self.request.matches_market(market),
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
