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

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub use crate::common::models::DeepXOrderBookLevel;

/// Request body used to submit a signed DeepX chain extrinsic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXSignedExtrinsicRequest {
    pub signed_extrinsic: String,
}

/// Correlation evidence returned after the DeepX relay accepts an order transaction.
///
/// This response does not prove that the transaction executed. Callers must reconcile the
/// transaction hash or order ID against an authoritative account source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXChainTxResponse {
    #[serde(alias = "order_id")]
    pub order_id: u64,
    #[serde(alias = "tx_hash")]
    pub tx_hash: String,
}

/// Schema-neutral responses collected from the DeepX current-account endpoints.
///
/// The requests are concurrent and therefore do not represent an atomic account snapshot. Each
/// payload remains raw until authoritative private response schemas are available.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXRawAccountSnapshot {
    pub balances: serde_json::Value,
    pub portfolio: serde_json::Value,
    pub positions: serde_json::Value,
    pub open_orders: serde_json::Value,
    pub order_history: serde_json::Value,
    pub trades: serde_json::Value,
}

impl DeepXRawAccountSnapshot {
    /// Returns a copy with all 20-byte hexadecimal account addresses redacted.
    #[must_use]
    pub fn redacted(&self) -> Self {
        let mut snapshot = self.clone();
        redact_account_addresses(&mut snapshot.balances);
        redact_account_addresses(&mut snapshot.portfolio);
        redact_account_addresses(&mut snapshot.positions);
        redact_account_addresses(&mut snapshot.open_orders);
        redact_account_addresses(&mut snapshot.order_history);
        redact_account_addresses(&mut snapshot.trades);
        snapshot
    }
}

fn redact_account_addresses(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                redact_account_addresses(value);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                redact_account_addresses(value);
            }
        }
        serde_json::Value::String(text) if is_account_address(text) => {
            *text = "<redacted-address>".to_string();
        }
        _ => {}
    }
}

fn is_account_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// DeepX perpetual market metadata returned by `GET /v1/perp/markets`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXPerpetualMarket {
    pub market_id: u32,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub status: String,
    pub tick_size: Decimal,
    pub step_size: Decimal,
    pub min_qty: Decimal,
    pub min_notional: Decimal,
    pub maker_fee_rate: Decimal,
    pub taker_fee_rate: Decimal,
    pub max_open_orders: u32,
    pub order_types: Vec<String>,
}

/// DeepX perpetual order book snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXOrderBookSnapshot {
    pub asks: Vec<DeepXOrderBookLevel>,
    pub bids: Vec<DeepXOrderBookLevel>,
    pub engine_time: u64,
    pub last_update_id: u64,
    pub server_time: u64,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::rstest;

    use super::*;

    #[rstest]
    fn parses_perpetual_market_without_losing_decimal_precision() {
        let payload = r#"{
            "baseAsset":"ETH","makerFeeRate":"-0.0001","marketId":3,
            "maxOpenOrders":128,"minNotional":"1","minQty":"0.001",
            "orderTypes":["LIMIT","MARKET"],"quoteAsset":"USDC",
            "status":"TRADING","stepSize":"0.001","symbol":"ETH-USDC",
            "takerFeeRate":"0.0002","tickSize":"0.01"
        }"#;

        let market: DeepXPerpetualMarket = serde_json::from_str(payload).unwrap();

        assert_eq!(market.market_id, 3);
        assert_eq!(market.tick_size, Decimal::from_str("0.01").unwrap());
        assert_eq!(market.step_size, Decimal::from_str("0.001").unwrap());
        assert_eq!(market.maker_fee_rate, Decimal::from_str("-0.0001").unwrap());
    }

    #[rstest]
    fn parses_order_book_snapshot_and_sequence() {
        let payload = r#"{
            "asks":[["2453.76","3.282"]],"bids":[["2453.75","0.842"]],
            "engineTime":1787561622066,"lastUpdateId":62430652,
            "serverTime":1787561622211
        }"#;

        let snapshot: DeepXOrderBookSnapshot = serde_json::from_str(payload).unwrap();

        assert_eq!(snapshot.last_update_id, 62_430_652);
        assert_eq!(snapshot.asks[0].0, Decimal::from_str("2453.76").unwrap());
        assert_eq!(snapshot.bids[0].1, Decimal::from_str("0.842").unwrap());
    }

    #[rstest]
    fn parses_chain_transaction_correlation_evidence() {
        let payload = r#"{
            "orderId":1781757000123,
            "txHash":"0x5a8f"
        }"#;

        let response: DeepXChainTxResponse = serde_json::from_str(payload).unwrap();

        assert_eq!(response.order_id, 1_781_757_000_123);
        assert_eq!(response.tx_hash, "0x5a8f");
    }

    #[rstest]
    fn rejects_chain_transaction_response_without_transaction_hash() {
        let payload = r#"{"orderId":1781757000123}"#;

        let result = serde_json::from_str::<DeepXChainTxResponse>(payload);

        assert!(result.is_err());
    }

    #[rstest]
    fn redacts_account_addresses_without_losing_schema_or_transaction_hashes() {
        let address = "0x1111111111111111111111111111111111111111";
        let transaction_hash = "0x2222222222222222222222222222222222222222222222222222222222222222";
        let snapshot = DeepXRawAccountSnapshot {
            balances: serde_json::json!({"address": address, "available": "12.34"}),
            portfolio: serde_json::json!({"nested": [{"wallet": address}]}),
            positions: serde_json::json!([]),
            open_orders: serde_json::json!({"txHash": transaction_hash}),
            order_history: serde_json::json!({"owner": address, "orderId": 42}),
            trades: serde_json::json!([]),
        };

        let redacted = snapshot.redacted();

        assert_eq!(redacted.balances["address"], "<redacted-address>");
        assert_eq!(redacted.balances["available"], "12.34");
        assert_eq!(
            redacted.portfolio["nested"][0]["wallet"],
            "<redacted-address>"
        );
        assert_eq!(redacted.open_orders["txHash"], transaction_hash);
        assert_eq!(redacted.order_history["orderId"], 42);
    }
}
