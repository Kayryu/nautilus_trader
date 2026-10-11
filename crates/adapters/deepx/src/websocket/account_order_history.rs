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
use nautilus_network::transport::Message;
use serde::Deserialize;

use super::{
    account::DeepXWsUserBalancesRequest, public::DeepXWsPerpMarket, spot_trades::DeepXWsSpotMarket,
    transport::DeepXWsReadConnection,
};
use crate::http::DeepXSpotMarket;

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

    fn validate_ack(&self, text: &str) -> Result<()> {
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
        ensure!(
            !market.name.trim().is_empty(),
            "DeepX Spot market name is empty"
        );
        ensure!(
            market.pair.len() == 66
                && market.pair.starts_with("0x")
                && nautilus_core::hex::decode_array::<32>(&market.pair[2..]).is_ok(),
            "DeepX Spot market pair is invalid"
        );
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

    fn validate_ack(&self, text: &str) -> Result<()> {
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
        request.validate_ack(text)
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
        request.validate_ack(text)
    })
    .await
}

async fn observe_order_history_ack(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request_text: String,
    validate_ack: impl Fn(&str) -> Result<()>,
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
                    let frame: serde_json::Value = serde_json::from_str(text)?;
                    if frame.get("type").and_then(serde_json::Value::as_str) == Some("pong") {
                        continue;
                    }
                    validate_ack(text)?;
                    break Ok::<_, anyhow::Error>(());
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
    const ACK: &str = r#"{"type":"subscribed","market":{"type":"perp","id":3,"name":"ETH-USDC"},"subscriptions":[{"channel":"user_perp_order_history","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"}],"message":"Successfully subscribed to 1 channels"}"#;
    const SPOT_PAIR: &str = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
    const SPOT_ACK: &str = r#"{"type":"subscribed","market":{"type":"spot","name":"ETH/USDC","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"},"subscriptions":[{"channel":"user_spot_order_history","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"}],"message":"Successfully subscribed to 1 channels"}"#;

    fn request() -> DeepXWsPerpOrderHistoryRequest {
        DeepXWsPerpOrderHistoryRequest::new(3, SUBACCOUNT).unwrap()
    }

    fn spot_request() -> DeepXWsSpotOrderHistoryRequest {
        let response: crate::http::DeepXApiResponse<Vec<DeepXSpotMarket>> = serde_json::from_str(
            include_str!("../../test_data/http/testnet/spot_markets.json"),
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
    fn binds_observed_ack_to_exact_scope() {
        let request = request();
        let encoded: serde_json::Value = serde_json::from_str(&request.to_text()).unwrap();
        assert_eq!(encoded["market"]["id"], 3);
        assert_eq!(encoded["subscriptions"][0]["address"], SUBACCOUNT);
        assert_eq!(
            encoded["subscriptions"][0]["channel"],
            "user_perp_order_history"
        );
        request.validate_ack(ACK).unwrap();

        for foreign in [
            ACK.replace("\"id\":3", "\"id\":4"),
            ACK.replace("user_perp_order_history", "user_perp_open_orders"),
            ACK.replace(SUBACCOUNT, "0x0000000000000000000000000000000000000000"),
            ACK.replace("\"type\":\"subscribed\"", "\"type\":\"data\""),
        ] {
            assert!(request.validate_ack(&foreign).is_err());
        }
    }

    #[rstest]
    fn binds_observed_spot_ack_to_exact_scope() {
        let request = spot_request();
        let encoded: serde_json::Value = serde_json::from_str(&request.to_text()).unwrap();
        assert_eq!(encoded["market"]["name"], "ETH/USDC");
        assert_eq!(encoded["subscriptions"][0]["address"], SUBACCOUNT);
        assert_eq!(
            encoded["subscriptions"][0]["channel"],
            "user_spot_order_history"
        );
        request.validate_ack(SPOT_ACK).unwrap();

        for foreign in [
            SPOT_ACK.replace("ETH/USDC", "SOL/USDC"),
            SPOT_ACK.replace(SPOT_PAIR, &format!("0x{}", "00".repeat(32))),
            SPOT_ACK.replace("user_spot_order_history", "user_spot_open_orders"),
            SPOT_ACK.replace(SUBACCOUNT, "0x0000000000000000000000000000000000000000"),
        ] {
            assert!(request.validate_ack(&foreign).is_err());
        }
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
