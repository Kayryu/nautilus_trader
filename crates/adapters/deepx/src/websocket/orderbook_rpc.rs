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
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Typed messages for the node's full-snapshot order-book JSON-RPC subscription.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use crate::config::{DeepXNetworkConfig, DeepXRpcRole};

/// Resolves the configured Watch RPC endpoint to its WebSocket transport scheme.
pub fn websocket_url(network: &DeepXNetworkConfig) -> anyhow::Result<String> {
    let mut url = reqwest::Url::parse(&network.rpc_url_for(DeepXRpcRole::Watch)?)
        .map_err(|_| anyhow::anyhow!("invalid DeepX Watch RPC URL"))?;
    match url.scheme() {
        "http" => url
            .set_scheme("ws")
            .map_err(|()| anyhow::anyhow!("invalid DeepX Watch RPC WebSocket scheme"))?,
        "https" => url
            .set_scheme("wss")
            .map_err(|()| anyhow::anyhow!("invalid DeepX Watch RPC WebSocket scheme"))?,
        "ws" | "wss" => {}
        _ => anyhow::bail!("DeepX Watch RPC URL must use HTTP or WebSocket"),
    }
    anyhow::ensure!(
        url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none(),
        "invalid DeepX Watch RPC WebSocket URL"
    );
    Ok(url.to_string())
}

/// One chain price level encoded as hexadecimal atomic values.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXOrderbookRpcLevel {
    /// Runtime price in 1e6 units.
    pub price: String,
    /// Base-token amount in base currency atomic units.
    pub amount: String,
    /// Number of orders aggregated into this level.
    pub order_count: u32,
}

/// One full pair snapshot from the custom node RPC.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXOrderbookRpcSide {
    /// Best bid first.
    pub bids: Vec<DeepXOrderbookRpcLevel>,
    /// Best ask first.
    pub asks: Vec<DeepXOrderbookRpcLevel>,
}

/// Full snapshot payload sent by `orderbook_subscribe`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXOrderbookRpcSnapshot {
    /// Spot books keyed by the 32-byte pair identity.
    pub spot: BTreeMap<String, DeepXOrderbookRpcSide>,
    /// Perpetual books keyed by market ID.
    pub perp: BTreeMap<String, DeepXOrderbookRpcSide>,
}

/// Parses and validates the response that acknowledges a JSON-RPC subscription request.
pub fn parse_subscribe_ack(value: &Value, request_id: u64) -> anyhow::Result<String> {
    anyhow::ensure!(
        value["jsonrpc"] == "2.0",
        "invalid DeepX orderbook RPC version"
    );
    anyhow::ensure!(
        value["id"] == request_id,
        "DeepX orderbook RPC request ID mismatch"
    );
    if let Some(error) = value.get("error") {
        anyhow::bail!("DeepX orderbook subscription rejected: {error}");
    }
    let subscription = value["result"]
        .as_str()
        .filter(|subscription| !subscription.is_empty())
        .ok_or_else(|| anyhow::anyhow!("DeepX orderbook subscription ID is missing"))?;
    Ok(subscription.to_string())
}

/// Parses one full-snapshot notification and binds it to the acknowledged subscription.
pub fn parse_notification(
    value: &Value,
    subscription_id: &str,
) -> anyhow::Result<DeepXOrderbookRpcSnapshot> {
    anyhow::ensure!(
        value["jsonrpc"] == "2.0" && value["method"] == "orderbook_subscribe",
        "unexpected DeepX orderbook RPC notification"
    );
    anyhow::ensure!(
        value["params"]["subscription"] == subscription_id,
        "DeepX orderbook notification subscription ID mismatch"
    );
    let snapshot: DeepXOrderbookRpcSnapshot =
        serde_json::from_value(value["params"]["result"].clone())?;
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn parses_subscription_ack_and_full_snapshot() {
        let ack = serde_json::json!({"jsonrpc":"2.0","result":"book-1","id":1});
        let subscription = parse_subscribe_ack(&ack, 1).unwrap();
        let notification = serde_json::json!({
            "jsonrpc":"2.0",
            "method":"orderbook_subscribe",
            "params":{
                "subscription":"book-1",
                "result":{
                    "spot":{"0x0102":{"bids":[{"price":"0x2710","amount":"0x5f5e100","order_count":2}],"asks":[]}},
                    "perp":{}
                }
            }
        });

        let snapshot = parse_notification(&notification, &subscription).unwrap();

        assert_eq!(snapshot.spot["0x0102"].bids[0].price, "0x2710");
        assert_eq!(snapshot.spot["0x0102"].bids[0].amount, "0x5f5e100");
        assert!(snapshot.spot["0x0102"].asks.is_empty());
    }

    #[rstest]
    #[case(serde_json::json!({"jsonrpc":"2.0","result":"book-1","id":2}), 1)]
    #[case(serde_json::json!({"jsonrpc":"2.0","error":{"code":-32601,"message":"missing"},"id":1}), 1)]
    fn rejects_invalid_subscription_ack(#[case] ack: Value, #[case] request_id: u64) {
        assert!(parse_subscribe_ack(&ack, request_id).is_err());
    }

    #[rstest]
    fn rejects_notification_from_another_subscription() {
        let notification = serde_json::json!({
            "jsonrpc":"2.0",
            "method":"orderbook_subscribe",
            "params":{"subscription":"other","result":{"spot":{},"perp":{}}}
        });
        assert!(parse_notification(&notification, "book-1").is_err());
    }
}
