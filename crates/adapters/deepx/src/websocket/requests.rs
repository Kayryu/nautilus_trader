// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeepXWsMethod {
    Subscribe,
    Unsubscribe,
    Ping,
    Pong,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DeepXWsRequest {
    pub method: DeepXWsMethod,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<DeepXWsParams>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXWsParams {
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subaccount: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wallet: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset: Option<String>,
}

impl DeepXWsRequest {
    #[must_use]
    pub fn subscribe(id: u64, params: DeepXWsParams) -> Self {
        Self {
            method: DeepXWsMethod::Subscribe,
            id: Some(id),
            params: Some(params),
        }
    }

    #[must_use]
    pub fn unsubscribe(id: u64, params: DeepXWsParams) -> Self {
        Self {
            method: DeepXWsMethod::Unsubscribe,
            id: Some(id),
            params: Some(params),
        }
    }

    #[must_use]
    pub const fn ping() -> Self {
        Self {
            method: DeepXWsMethod::Ping,
            id: None,
            params: None,
        }
    }

    #[must_use]
    pub const fn pong() -> Self {
        Self {
            method: DeepXWsMethod::Pong,
            id: None,
            params: None,
        }
    }
}

impl DeepXWsParams {
    #[must_use]
    pub fn perpetual_order_book(symbol: impl Into<String>) -> Self {
        Self::market("perp@orderbook", symbol)
    }

    #[must_use]
    pub fn perpetual_trades(symbol: impl Into<String>) -> Self {
        Self::market("perp@trades", symbol)
    }

    fn market(channel: &str, symbol: impl Into<String>) -> Self {
        Self {
            channel: channel.to_owned(),
            symbol: Some(symbol.into()),
            interval: None,
            subaccount: None,
            wallet: None,
            asset: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    fn serializes_perpetual_subscription_like_sdk() {
        let request =
            DeepXWsRequest::subscribe(27, DeepXWsParams::perpetual_order_book("ETH-USDC"));

        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({
                "method": "subscribe",
                "id": 27,
                "params": {"channel": "perp@orderbook", "symbol": "ETH-USDC"},
            }),
        );
    }

    #[rstest]
    fn serializes_unsubscribe_and_ping_without_null_fields() {
        let unsubscribe =
            DeepXWsRequest::unsubscribe(28, DeepXWsParams::perpetual_trades("ETH-USDC"));

        assert_eq!(
            serde_json::to_value(unsubscribe).unwrap(),
            json!({
                "method": "unsubscribe",
                "id": 28,
                "params": {"channel": "perp@trades", "symbol": "ETH-USDC"},
            }),
        );
        assert_eq!(
            serde_json::to_value(DeepXWsRequest::ping()).unwrap(),
            json!({"method": "ping"})
        );
    }
}
