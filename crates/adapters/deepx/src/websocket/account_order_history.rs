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

//! Read-only perpetual order-history subscription acknowledgement.

use std::time::Duration;

use anyhow::{Result, ensure};
use deepx_rust_sdk::websocket::account_order_history::DeepXWsOrderHistoryControlFrame;
pub use deepx_rust_sdk::websocket::account_order_history::{
    DeepXWsPerpOrderHistoryRequest, DeepXWsSpotOrderHistoryRequest,
};
use nautilus_network::transport::Message;

use super::transport::DeepXWsReadConnection;
#[cfg(test)]
use crate::http::DeepXSpotMarket;

/// Observes one exact acknowledgement, without interpreting incremental order updates.
///
/// An acknowledgement proves only that the endpoint accepted this market and subaccount scope.
/// The channel has no initial snapshot and does not establish complete order history.
///
/// # Errors
///
/// Returns an error for invalid timeout, transport failure, data before acknowledgement,
/// malformed or foreign acknowledgement, or deadline expiry.
pub async fn observe_perp_order_history_ack(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsPerpOrderHistoryRequest,
) -> Result<()> {
    observe_order_history_ack(network, proxy_url, timeout, request.to_text(), |text| {
        request.parse_control_frame(text)
    })
    .await
}

/// Observes one exact Spot acknowledgement without interpreting incremental order updates.
///
/// The channel has no initial snapshot and does not establish complete order history.
///
/// # Errors
///
/// Returns an error for invalid timeout, transport failure, data before acknowledgement,
/// malformed or foreign acknowledgement, or deadline expiry.
pub async fn observe_spot_order_history_ack(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsSpotOrderHistoryRequest,
) -> Result<()> {
    observe_order_history_ack(network, proxy_url, timeout, request.to_text(), |text| {
        request.parse_control_frame(text)
    })
    .await
}

async fn observe_order_history_ack(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request_text: String,
    parse_frame: impl Fn(&str) -> Result<DeepXWsOrderHistoryControlFrame>,
) -> Result<()> {
    ensure!(
        !timeout.is_zero(),
        "DeepX order-history timeout must be positive"
    );
    let mut connection = DeepXWsReadConnection::connect(network, proxy_url, timeout).await?;
    if let Err(e) = connection.send_order_history_request(request_text).await {
        let _ = connection.close().await;
        return Err(e);
    }
    let result = tokio::time::timeout(timeout, async {
        loop {
            let Some(message) = connection.receive().await? else {
                anyhow::bail!("DeepX order-history stream closed before acknowledgement");
            };
            match message {
                Message::Text(bytes) => {
                    let text = std::str::from_utf8(&bytes)?;
                    match parse_frame(text)? {
                        DeepXWsOrderHistoryControlFrame::Pong => continue,
                        DeepXWsOrderHistoryControlFrame::Subscribed => break Ok(()),
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => anyhow::bail!("unsupported DeepX order-history stream message"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        Err(anyhow::anyhow!(
            "DeepX order-history acknowledgement deadline exceeded"
        ))
    });
    let close = connection.close().await;
    result.and(close)
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        extract::ws::{Message as ServerMessage, WebSocketUpgrade},
        routing::get,
    };
    use rstest::rstest;

    use super::*;

    const SUBACCOUNT: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";

    fn request() -> DeepXWsPerpOrderHistoryRequest {
        DeepXWsPerpOrderHistoryRequest::new(3, SUBACCOUNT).unwrap()
    }

    fn spot_request() -> DeepXWsSpotOrderHistoryRequest {
        let response: crate::http::DeepXApiResponse<Vec<DeepXSpotMarket>> = serde_json::from_str(
            include_str!("../../deepx-rust-sdk/test_data/http/testnet/spot_markets.json"),
        )
        .unwrap();
        let market = response
            .data
            .into_iter()
            .find(|market| market.name == "ETH/USDC")
            .unwrap();
        DeepXWsSpotOrderHistoryRequest::new(&market, SUBACCOUNT).unwrap()
    }

    #[rstest]
    #[tokio::test]
    async fn observer_requires_ack_before_data() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(|upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(|mut socket| async move {
                    let Some(Ok(ServerMessage::Text(_))) = socket.recv().await else {
                        panic!("expected order-history subscription");
                    };
                    let _ = socket
                        .send(ServerMessage::Text(
                            r#"{"type":"data","channel":"user_perp_order_history"}"#.into(),
                        ))
                        .await;
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        assert!(
            observe_perp_order_history_ack(&network, None, Duration::from_secs(2), &request())
                .await
                .is_err()
        );
        server.abort();
    }

    #[rstest]
    #[tokio::test]
    async fn spot_observer_requires_ack_before_data() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(|upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(|mut socket| async move {
                    let Some(Ok(ServerMessage::Text(_))) = socket.recv().await else {
                        panic!("expected Spot order-history subscription");
                    };
                    let _ = socket
                        .send(ServerMessage::Text(
                            r#"{"type":"data","channel":"user_spot_order_history"}"#.into(),
                        ))
                        .await;
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        assert!(
            observe_spot_order_history_ack(&network, None, Duration::from_secs(2), &spot_request())
                .await
                .is_err()
        );
        server.abort();
    }
}
