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

use std::{num::NonZeroUsize, time::Duration};

use deepx_rust_sdk::websocket::spot_trades::DeepXWsSpotTradeSession;
pub(crate) use deepx_rust_sdk::websocket::spot_trades::{
    DeepXWsSpotTradeFrame, DeepXWsSpotTradeRequest,
};

use crate::{config::DeepXNetworkConfig, http::DeepXSpotMarket};

/// A single-owner Spot connection with bounded pre-acknowledgement data buffering.
#[derive(Debug)]
pub(crate) struct DeepXWsSpotTradeConnection {
    transport: super::transport::DeepXWsReadConnection,
    session: DeepXWsSpotTradeSession,
    timeout: Duration,
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
            session: DeepXWsSpotTradeSession::new(request, buffer_capacity),
            timeout,
            poisoned: false,
        };
        connection.subscribe().await?;
        Ok(connection)
    }

    async fn subscribe(&mut self) -> anyhow::Result<()> {
        self.transport
            .send_spot_trade_request(self.session.request())
            .await?;
        let result = tokio::time::timeout(self.timeout, async {
            loop {
                let frame = self.read_frame().await?;
                if self.session.accept_before_ack(frame)? {
                    return Ok(());
                }
            }
        })
        .await;
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                self.poisoned = true;
                self.session.clear();
                Err(e)
            }
            Err(_) => {
                self.poisoned = true;
                self.session.clear();
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
            self.session.is_subscribed() && !self.poisoned,
            "DeepX Spot trade connection is terminal"
        );
        if let Some(frame) = self.session.pop_buffered() {
            return Ok(frame);
        }
        let frame = self.read_frame().await?;
        if let Err(e) = self.session.validate_live_frame(&frame) {
            if !matches!(frame, DeepXWsSpotTradeFrame::Data { .. }) {
                self.poisoned = true;
            }
            return Err(e);
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
        self.session.clear();
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

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        extract::ws::{Message, WebSocketUpgrade},
        routing::get,
    };
    use rstest::rstest;

    use super::*;

    const MARKET_RESPONSE: &str = include_str!(
        "../../../../../deepx-rust-sdk/test_data/http/testnet/spot_market_eth_usdc_by_pair.json"
    );

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn connection_releases_only_acknowledged_market_data(#[case] foreign: bool) {
        let response: serde_json::Value = serde_json::from_str(MARKET_RESPONSE).unwrap();
        let market: DeepXSpotMarket = serde_json::from_value(response["data"].clone()).unwrap();
        let pair = if foreign {
            format!("0x{}", "0".repeat(64))
        } else {
            market.pair.clone()
        };
        let data = format!(
            r#"{{"type":"data","market":{{"type":"spot","name":"ETH/USDC","pair":"{pair}"}},"channel":"trades","data":{{"items":[]}},"timestamp":1}}"#
        );
        let ack = format!(
            r#"{{"type":"subscribed","market":{{"type":"spot","name":"ETH/USDC","pair":"{}"}},"subscriptions":["trades"],"message":"Subscribed"}}"#,
            market.pair
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| {
                let data = data.clone();
                let ack = ack.clone();
                async move {
                    upgrade.on_upgrade(move |mut socket| async move {
                        let Some(Ok(Message::Text(_))) = socket.recv().await else {
                            panic!("expected Spot trade subscription");
                        };
                        socket.send(Message::Text(data.into())).await.unwrap();
                        if !foreign {
                            socket.send(Message::Text(ack.into())).await.unwrap();
                        }
                        let _ = tokio::time::timeout(Duration::from_secs(1), socket.recv()).await;
                    })
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let connection = DeepXWsSpotTradeConnection::connect(
            &network,
            None,
            Duration::from_secs(1),
            NonZeroUsize::new(1).unwrap(),
            market,
        )
        .await;
        if foreign {
            assert!(connection.is_err());
        } else {
            let mut connection = connection.unwrap();
            assert!(matches!(
                connection.next_frame().await.unwrap(),
                DeepXWsSpotTradeFrame::Data { .. }
            ));
            connection.close().await.unwrap();
        }
        server.abort();
    }
}
