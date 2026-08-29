// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! DeepX public WebSocket transport and subscription lifecycle.

use std::collections::HashMap;

use nautilus_common::live::runtime::get_runtime;
use nautilus_network::{
    Message, RECONNECTED,
    websocket::{MessageHandler, TransportBackend, WebSocketClient, WebSocketConfig},
};
use serde::Deserialize;

use super::{
    messages::{DeepXOrderBookUpdate, DeepXTrade, DeepXWsMessage},
    requests::{DeepXWsMethod, DeepXWsParams, DeepXWsRequest},
};

const ORDER_BOOK_CHANNEL: &str = "perp@orderbook";
const TRADES_CHANNEL: &str = "perp@trades";

/// A parsed message emitted by the DeepX public WebSocket client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeepXWebSocketMessage {
    SubscriptionConfirmed {
        channel: String,
        symbol: Option<String>,
    },
    UnsubscriptionConfirmed {
        channel: String,
        symbol: Option<String>,
    },
    OrderBook(DeepXOrderBookUpdate),
    Trade(DeepXTrade),
    Reconnected,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SubscriptionKey {
    channel: String,
    symbol: Option<String>,
}

impl From<&DeepXWsParams> for SubscriptionKey {
    fn from(params: &DeepXWsParams) -> Self {
        Self {
            channel: params.channel.clone(),
            symbol: params.symbol.clone(),
        }
    }
}

#[derive(Debug)]
enum Command {
    Subscribe(DeepXWsParams),
    Unsubscribe(DeepXWsParams),
    Disconnect,
}

#[derive(Debug, Deserialize)]
struct SubscriptionResponseData {
    id: u64,
    method: DeepXWsMethod,
    params: SubscriptionResponseParams,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionResponseParams {
    channel: String,
    symbol: Option<String>,
}

/// Public DeepX WebSocket client.
#[derive(Debug)]
pub struct DeepXWebSocketClient {
    url: String,
    timeout_secs: u64,
    proxy_url: Option<String>,
    command_tx: Option<tokio::sync::mpsc::UnboundedSender<Command>>,
    output_rx: Option<tokio::sync::mpsc::UnboundedReceiver<DeepXWebSocketMessage>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl DeepXWebSocketClient {
    #[must_use]
    pub const fn new(url: String, timeout_secs: u64, proxy_url: Option<String>) -> Self {
        Self {
            url,
            timeout_secs,
            proxy_url,
            command_tx: None,
            output_rx: None,
            task: None,
        }
    }

    /// Connects to DeepX and starts the protocol handler.
    pub async fn connect(&mut self) -> anyhow::Result<()> {
        if self.task.is_some() {
            return Ok(());
        }

        let (raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel();
        let message_handler: MessageHandler = std::sync::Arc::new(move |message| {
            let _ = raw_tx.send(message);
        });
        let config = WebSocketConfig {
            url: self.url.clone(),
            headers: vec![],
            heartbeat_interval_secs: None,
            heartbeat_payload: None,
            connect_timeout_ms: Some(self.timeout_secs.saturating_mul(1_000)),
            reconnect_delay_initial_ms: Some(250),
            reconnect_delay_max_ms: Some(5_000),
            reconnect_backoff_factor: Some(2.0),
            reconnect_jitter_ms: Some(200),
            reconnect_max_attempts: None,
            heartbeat_timeout_secs: None,
            idle_timeout_ms: None,
            backend: TransportBackend::default(),
            proxy_url: self.proxy_url.clone(),
        };
        let client = WebSocketClient::builder()
            .config(config)
            .message_handler(message_handler)
            .connect()
            .await?;
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        let (output_tx, output_rx) = tokio::sync::mpsc::unbounded_channel();

        self.command_tx = Some(command_tx);
        self.output_rx = Some(output_rx);
        self.task = Some(get_runtime().spawn(run_handler(client, command_rx, raw_rx, output_tx)));
        Ok(())
    }

    /// Returns the output receiver once.
    pub fn take_receiver(
        &mut self,
    ) -> anyhow::Result<tokio::sync::mpsc::UnboundedReceiver<DeepXWebSocketMessage>> {
        self.output_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("DeepX WebSocket output receiver is unavailable"))
    }

    /// Subscribes to perpetual order book updates.
    pub fn subscribe_order_book(&self, symbol: impl Into<String>) -> anyhow::Result<()> {
        self.send_command(Command::Subscribe(DeepXWsParams::perpetual_order_book(
            symbol,
        )))
    }

    /// Subscribes to perpetual public trades.
    pub fn subscribe_trades(&self, symbol: impl Into<String>) -> anyhow::Result<()> {
        self.send_command(Command::Subscribe(DeepXWsParams::perpetual_trades(symbol)))
    }

    /// Unsubscribes from perpetual order book updates.
    pub fn unsubscribe_order_book(&self, symbol: impl Into<String>) -> anyhow::Result<()> {
        self.send_command(Command::Unsubscribe(DeepXWsParams::perpetual_order_book(
            symbol,
        )))
    }

    /// Unsubscribes from perpetual public trades.
    pub fn unsubscribe_trades(&self, symbol: impl Into<String>) -> anyhow::Result<()> {
        self.send_command(Command::Unsubscribe(DeepXWsParams::perpetual_trades(
            symbol,
        )))
    }

    /// Disconnects and joins the protocol handler.
    pub async fn disconnect(&mut self) {
        self.request_disconnect();
        if let Some(task) = self.task.take() {
            let abort_handle = task.abort_handle();
            match tokio::time::timeout(std::time::Duration::from_secs(5), task).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => log::warn!("Failed to join DeepX WebSocket task: {e}"),
                Err(_) => {
                    log::warn!("DeepX WebSocket task did not stop within timeout, aborting");
                    abort_handle.abort();
                }
            }
        }
        self.output_rx = None;
    }

    /// Requests transport shutdown without waiting for the handler to finish.
    pub fn request_disconnect(&mut self) {
        if let Some(command_tx) = self.command_tx.take() {
            let _ = command_tx.send(Command::Disconnect);
        }
    }

    fn send_command(&self, command: Command) -> anyhow::Result<()> {
        self.command_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("DeepX WebSocket is not connected"))?
            .send(command)
            .map_err(|e| anyhow::anyhow!("failed to send DeepX WebSocket command: {e}"))
    }
}

async fn run_handler(
    client: WebSocketClient,
    mut command_rx: tokio::sync::mpsc::UnboundedReceiver<Command>,
    mut raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
    output_tx: tokio::sync::mpsc::UnboundedSender<DeepXWebSocketMessage>,
) {
    let mut desired = HashMap::<SubscriptionKey, DeepXWsParams>::new();
    let mut pending = HashMap::<u64, (DeepXWsMethod, SubscriptionKey)>::new();
    let mut next_request_id = 1_u64;

    loop {
        tokio::select! {
            Some(command) = command_rx.recv() => match command {
                Command::Subscribe(params) => {
                    let key = SubscriptionKey::from(&params);
                    desired.insert(key, params.clone());
                    if let Err(e) = send_request(
                        &client,
                        DeepXWsMethod::Subscribe,
                        params,
                        &mut next_request_id,
                        &mut pending,
                    ).await {
                        log::warn!("Failed to subscribe to DeepX WebSocket: {e}");
                    }
                }
                Command::Unsubscribe(params) => {
                    desired.remove(&SubscriptionKey::from(&params));
                    if let Err(e) = send_request(
                        &client,
                        DeepXWsMethod::Unsubscribe,
                        params,
                        &mut next_request_id,
                        &mut pending,
                    ).await {
                        log::warn!("Failed to unsubscribe from DeepX WebSocket: {e}");
                    }
                }
                Command::Disconnect => {
                    client.disconnect().await;
                    break;
                }
            },
            Some(message) = raw_rx.recv() => {
                if !handle_message(
                    message,
                    &client,
                    &desired,
                    &mut pending,
                    &mut next_request_id,
                    &output_tx,
                ).await {
                    break;
                }
            }
            else => break,
        }
    }
}

async fn send_request(
    client: &WebSocketClient,
    method: DeepXWsMethod,
    params: DeepXWsParams,
    next_request_id: &mut u64,
    pending: &mut HashMap<u64, (DeepXWsMethod, SubscriptionKey)>,
) -> anyhow::Result<()> {
    let request_id = *next_request_id;
    *next_request_id = next_request_id
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("DeepX WebSocket request ID overflow"))?;
    let request = match method {
        DeepXWsMethod::Subscribe => DeepXWsRequest::subscribe(request_id, params.clone()),
        DeepXWsMethod::Unsubscribe => DeepXWsRequest::unsubscribe(request_id, params.clone()),
        DeepXWsMethod::Ping | DeepXWsMethod::Pong => {
            anyhow::bail!("invalid DeepX subscription method {method:?}")
        }
    };
    client
        .send_text(serde_json::to_string(&request)?, None)
        .await?;
    pending.insert(request_id, (method, SubscriptionKey::from(&params)));
    Ok(())
}

async fn handle_message(
    message: Message,
    client: &WebSocketClient,
    desired: &HashMap<SubscriptionKey, DeepXWsParams>,
    pending: &mut HashMap<u64, (DeepXWsMethod, SubscriptionKey)>,
    next_request_id: &mut u64,
    output_tx: &tokio::sync::mpsc::UnboundedSender<DeepXWebSocketMessage>,
) -> bool {
    let Message::Text(bytes) = message else {
        return !matches!(message, Message::Close(_));
    };
    let Ok(text) = std::str::from_utf8(&bytes) else {
        log::warn!("Received non-UTF-8 DeepX WebSocket text frame");
        return true;
    };
    if text == RECONNECTED {
        pending.clear();
        for params in desired.values().cloned() {
            if let Err(e) = send_request(
                client,
                DeepXWsMethod::Subscribe,
                params,
                next_request_id,
                pending,
            )
            .await
            {
                log::warn!("Failed to restore DeepX WebSocket subscription: {e}");
            }
        }
        return output_tx.send(DeepXWebSocketMessage::Reconnected).is_ok();
    }

    let Ok(envelope) = serde_json::from_slice::<DeepXWsMessage<serde_json::Value>>(&bytes) else {
        log::warn!("Failed to parse DeepX WebSocket envelope");
        return true;
    };
    let output = match envelope.channel.as_str() {
        "subscriptionResponse" => parse_subscription_response(envelope.data, pending),
        ORDER_BOOK_CHANNEL => serde_json::from_value(envelope.data)
            .map(DeepXWebSocketMessage::OrderBook)
            .map(Some)
            .map_err(anyhow::Error::from),
        TRADES_CHANNEL => serde_json::from_value(envelope.data)
            .map(DeepXWebSocketMessage::Trade)
            .map(Some)
            .map_err(anyhow::Error::from),
        channel => {
            log::debug!("Ignoring unsupported DeepX WebSocket channel: {channel}");
            return true;
        }
    };
    match output {
        Ok(Some(message)) => output_tx.send(message).is_ok(),
        Ok(None) => true,
        Err(e) => {
            log::warn!("Failed to parse DeepX WebSocket message: {e}");
            true
        }
    }
}

fn parse_subscription_response(
    data: serde_json::Value,
    pending: &mut HashMap<u64, (DeepXWsMethod, SubscriptionKey)>,
) -> anyhow::Result<Option<DeepXWebSocketMessage>> {
    let response: SubscriptionResponseData = serde_json::from_value(data)?;
    let Some((method, key)) = pending.remove(&response.id) else {
        log::debug!(
            "Ignoring unknown DeepX subscription response ID: {}",
            response.id
        );
        return Ok(None);
    };
    anyhow::ensure!(
        response.method == method,
        "DeepX subscription method mismatch"
    );
    anyhow::ensure!(
        response.params.channel == key.channel && response.params.symbol == key.symbol,
        "DeepX subscription parameters mismatch"
    );
    Ok(Some(match method {
        DeepXWsMethod::Subscribe => DeepXWebSocketMessage::SubscriptionConfirmed {
            channel: key.channel,
            symbol: key.symbol,
        },
        DeepXWsMethod::Unsubscribe => DeepXWebSocketMessage::UnsubscriptionConfirmed {
            channel: key.channel,
            symbol: key.symbol,
        },
        DeepXWsMethod::Ping | DeepXWsMethod::Pong => unreachable!(),
    }))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn parses_verified_subscription_response() {
        let data = serde_json::json!({
            "id": 1,
            "method": "subscribe",
            "params": {
                "asset": null,
                "channel": "perp@orderbook",
                "interval": null,
                "priceType": null,
                "subaccount": null,
                "symbol": "ETH-USDC",
                "wallet": null
            }
        });
        let key = SubscriptionKey {
            channel: ORDER_BOOK_CHANNEL.to_string(),
            symbol: Some("ETH-USDC".to_string()),
        };
        let mut pending = HashMap::from([(1, (DeepXWsMethod::Subscribe, key))]);

        assert_eq!(
            parse_subscription_response(data, &mut pending).unwrap(),
            Some(DeepXWebSocketMessage::SubscriptionConfirmed {
                channel: ORDER_BOOK_CHANNEL.to_string(),
                symbol: Some("ETH-USDC".to_string()),
            })
        );
        assert!(pending.is_empty());
    }

    #[rstest]
    fn parses_verified_unsubscription_response() {
        let data = serde_json::json!({
            "id": 2,
            "method": "unsubscribe",
            "params": {
                "channel": "perp@trades",
                "symbol": "ETH-USDC"
            }
        });
        let key = SubscriptionKey {
            channel: TRADES_CHANNEL.to_string(),
            symbol: Some("ETH-USDC".to_string()),
        };
        let mut pending = HashMap::from([(2, (DeepXWsMethod::Unsubscribe, key))]);

        assert_eq!(
            parse_subscription_response(data, &mut pending).unwrap(),
            Some(DeepXWebSocketMessage::UnsubscriptionConfirmed {
                channel: TRADES_CHANNEL.to_string(),
                symbol: Some("ETH-USDC".to_string()),
            })
        );
        assert!(pending.is_empty());
    }

    #[rstest]
    fn rejects_mismatched_subscription_response() {
        let data = serde_json::json!({
            "id": 2,
            "method": "subscribe",
            "params": {"channel": "perp@trades", "symbol": "BTC-USDC"}
        });
        let key = SubscriptionKey {
            channel: TRADES_CHANNEL.to_string(),
            symbol: Some("ETH-USDC".to_string()),
        };
        let mut pending = HashMap::from([(2, (DeepXWsMethod::Subscribe, key))]);

        assert!(parse_subscription_response(data, &mut pending).is_err());
    }
}
