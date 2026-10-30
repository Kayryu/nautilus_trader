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

/// Encodes a Spot order-book JSON-RPC subscription request.
#[must_use]
pub fn encode_subscribe_request(pair: &str, depth: usize, request_id: u64) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "orderbook_subscribe",
        "params": [[pair], [], depth],
    })
    .to_string()
}

/// Encodes an order-book JSON-RPC unsubscription request.
#[must_use]
pub fn encode_unsubscribe_request(subscription_id: &str, request_id: u64) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "orderbook_unsubscribe",
        "params": [subscription_id],
    })
    .to_string()
}

/// Returns whether the node reports that the requested order-book method is unavailable.
#[must_use]
pub fn is_method_unavailable_response(value: &Value, request_id: u64) -> bool {
    value["jsonrpc"] == "2.0" && value["id"] == request_id && value["error"]["code"] == -32601
}

/// Converts a Watch RPC endpoint to its WebSocket transport scheme.
pub fn websocket_url(rpc_url: &str) -> anyhow::Result<String> {
    let mut url =
        reqwest::Url::parse(rpc_url).map_err(|_| anyhow::anyhow!("invalid DeepX Watch RPC URL"))?;
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
    fn encodes_spot_orderbook_subscription_and_unsubscription() {
        let subscribe: Value =
            serde_json::from_str(&encode_subscribe_request("0xAB12", 10, 1)).unwrap();
        assert_eq!(
            subscribe,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "orderbook_subscribe",
                "params": [["0xAB12"], [], 10],
            }),
        );

        let unsubscribe: Value =
            serde_json::from_str(&encode_unsubscribe_request("book-1", 2)).unwrap();
        assert_eq!(
            unsubscribe,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "orderbook_unsubscribe",
                "params": ["book-1"],
            }),
        );
    }

    #[rstest]
    #[case(serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":-32601}}), true)]
    #[case(serde_json::json!({"jsonrpc":"2.0","id":2,"error":{"code":-32601}}), false)]
    #[case(serde_json::json!({"jsonrpc":"1.0","id":1,"error":{"code":-32601}}), false)]
    #[case(serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000}}), false)]
    fn method_unavailable_response_requires_matching_rpc_envelope(
        #[case] response: Value,
        #[case] expected: bool,
    ) {
        assert_eq!(is_method_unavailable_response(&response, 1), expected);
    }

    #[rstest]
    #[case("https://rpc1-testnet.deepx.fi", "wss://rpc1-testnet.deepx.fi/")]
    #[case("http://localhost:9944", "ws://localhost:9944/")]
    fn rpc_url_uses_websocket_scheme(#[case] rpc_url: &str, #[case] expected: &str) {
        assert_eq!(websocket_url(rpc_url).unwrap(), expected);
    }

    #[rstest]
    #[case("ftp://rpc1-testnet.deepx.fi")]
    #[case("https://user@rpc1-testnet.deepx.fi")]
    #[case("https://rpc1-testnet.deepx.fi/#fragment")]
    fn rpc_url_rejects_unsupported_or_unsafe_endpoints(#[case] rpc_url: &str) {
        assert!(websocket_url(rpc_url).is_err());
    }

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
