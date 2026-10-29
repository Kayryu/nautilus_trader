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

//! Hash-bound decoding of DeepX REST transaction-status responses.

use serde::Deserialize;
use serde_json::value::RawValue;
use thiserror::Error;

/// Documented backend confirmation states, none of which proves canonical finality.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeepXRestTransactionConfirmation {
    Pending,
    Best,
    DecodeFailed,
}

/// Decoded backend status bound to the requested extrinsic hash.
#[derive(Clone, Debug)]
pub struct DeepXRestTransactionStatusData {
    pub extrinsic_hash: [u8; 32],
    pub confirmation: DeepXRestTransactionConfirmation,
    pub order_id: Option<u64>,
    pub backend_status: Option<String>,
    pub data: Box<RawValue>,
}

/// An invalid transaction-status schema or identity.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct DeepXRestTransactionStatusError(pub String);

#[derive(Deserialize)]
struct TransactionStatusData {
    tx_hash: String,
    #[serde(default)]
    order_id: String,
    confirmation: DeepXRestTransactionConfirmation,
    status: Option<String>,
}

/// Decodes status only when it belongs to the exact requested extrinsic.
///
/// # Errors
///
/// Returns an error for malformed data, a different hash, or an order ID in an unconfirmed state.
pub fn parse_transaction_status(
    data: Box<RawValue>,
    expected_hash: [u8; 32],
) -> Result<DeepXRestTransactionStatusData, DeepXRestTransactionStatusError> {
    let invalid = |message: String| DeepXRestTransactionStatusError(message);
    let decoded: TransactionStatusData =
        serde_json::from_str(data.get()).map_err(|e| invalid(e.to_string()))?;
    let extrinsic_hash = decoded
        .tx_hash
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| invalid("tx_hash must be a 0x-prefixed 32-byte hash".to_string()))?;

    if extrinsic_hash != expected_hash {
        return Err(invalid(format!(
            "transaction hash mismatch: expected 0x{}, received {}",
            hex::encode(expected_hash),
            decoded.tx_hash,
        )));
    }
    let order_id = if decoded.order_id.is_empty() {
        None
    } else {
        if !decoded.order_id.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid(
                "order_id must be empty or a decimal u64 string".to_string(),
            ));
        }
        Some(
            decoded
                .order_id
                .parse::<u64>()
                .map_err(|e| invalid(e.to_string()))?,
        )
    };

    if decoded.confirmation != DeepXRestTransactionConfirmation::Best && order_id.is_some() {
        return Err(invalid(
            "pending/decode_failed result must have an empty order_id".to_string(),
        ));
    }
    Ok(DeepXRestTransactionStatusData {
        extrinsic_hash,
        confirmation: decoded.confirmation,
        order_id,
        backend_status: decoded.status,
        data,
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;

    const HASH: [u8; 32] = [0xab; 32];

    fn raw_status(confirmation: &str, order_id: &str) -> Box<RawValue> {
        serde_json::value::to_raw_value(&json!({
            "tx_hash": format!("0x{}", hex::encode(HASH)),
            "confirmation": confirmation,
            "order_id": order_id,
            "status": "observed",
        }))
        .unwrap()
    }

    fn status_data(confirmation: &str, order_id: &str) -> Value {
        json!({
            "tx_hash": format!("0x{}", hex::encode(HASH)),
            "confirmation": confirmation,
            "order_id": order_id,
            "status": "included",
        })
    }

    fn raw(data: &Value) -> Box<RawValue> {
        RawValue::from_string(data.to_string()).unwrap()
    }

    #[rstest]
    fn parses_best_status_with_exact_order_id() {
        let status =
            parse_transaction_status(raw_status("best", "18446744073709551615"), HASH).unwrap();
        assert_eq!(status.extrinsic_hash, HASH);
        assert_eq!(status.confirmation, DeepXRestTransactionConfirmation::Best);
        assert_eq!(status.order_id, Some(u64::MAX));
        assert_eq!(status.backend_status.as_deref(), Some("observed"));
    }

    #[rstest]
    fn rejects_status_for_another_hash() {
        assert!(parse_transaction_status(raw_status("best", "1"), [0; 32]).is_err());
    }

    #[rstest]
    fn rejects_order_id_before_best_confirmation() {
        assert!(parse_transaction_status(raw_status("pending", "1"), HASH).is_err());
    }

    #[rstest]
    #[case("pending", "", DeepXRestTransactionConfirmation::Pending, None)]
    #[case("best", "", DeepXRestTransactionConfirmation::Best, None)]
    #[case("best", "0", DeepXRestTransactionConfirmation::Best, Some(0))]
    #[case("best", "00042", DeepXRestTransactionConfirmation::Best, Some(42))]
    #[case(
        "decode_failed",
        "",
        DeepXRestTransactionConfirmation::DecodeFailed,
        None
    )]
    fn parses_documented_backend_states(
        #[case] confirmation: &str,
        #[case] id: &str,
        #[case] expected: DeepXRestTransactionConfirmation,
        #[case] order_id: Option<u64>,
    ) {
        let status = parse_transaction_status(raw(&status_data(confirmation, id)), HASH).unwrap();
        assert_eq!(status.extrinsic_hash, HASH);
        assert_eq!(status.confirmation, expected);
        assert_eq!(status.order_id, order_id);
        assert_eq!(status.backend_status.as_deref(), Some("included"));
    }

    #[test]
    fn preserves_original_numeric_lexemes_and_optional_status() {
        let body = format!(
            r#"{{"tx_hash":"0x{}","confirmation":"best","order_id":"42","extra":0.1234567890123456789012345678}}"#,
            hex::encode(HASH)
        );
        let status =
            parse_transaction_status(RawValue::from_string(body.clone()).unwrap(), HASH).unwrap();
        assert_eq!(status.data.get(), body);
        assert_eq!(status.backend_status, None);
    }

    #[rstest]
    #[case("confirmation", json!("finalized"))]
    #[case("confirmation", json!("unknown"))]
    #[case("confirmation", json!("Best"))]
    #[case("confirmation", json!(null))]
    #[case("tx_hash", json!("0x1234"))]
    #[case("tx_hash", json!("abababababababababababababababababababababababababababababababab"))]
    #[case("tx_hash", json!("0x0000000000000000000000000000000000000000000000000000000000000000"))]
    #[case("tx_hash", json!(42))]
    #[case("order_id", json!("18446744073709551616"))]
    #[case("order_id", json!("-1"))]
    #[case("order_id", json!("+1"))]
    #[case("order_id", json!("1.0"))]
    #[case("order_id", json!(" 1"))]
    #[case("order_id", json!("1 "))]
    #[case("order_id", json!(42))]
    #[case("order_id", json!(null))]
    #[case("status", json!(42))]
    fn malformed_observations_fail_closed(#[case] field: &str, #[case] value: Value) {
        let mut data = status_data("best", "42");
        data[field] = value;
        assert!(parse_transaction_status(raw(&data), HASH).is_err());
    }

    #[rstest]
    #[case("confirmation")]
    #[case("tx_hash")]
    fn missing_required_status_fields_fail_closed(#[case] field: &str) {
        let mut data = status_data("best", "42");
        data.as_object_mut().unwrap().remove(field);
        assert!(parse_transaction_status(raw(&data), HASH).is_err());
    }

    #[rstest]
    #[case("pending")]
    #[case("best")]
    #[case("decode_failed")]
    fn absent_order_id_does_not_invent_an_action_result(#[case] confirmation: &str) {
        let mut data = status_data(confirmation, "");
        data.as_object_mut().unwrap().remove("order_id");
        let status = parse_transaction_status(raw(&data), HASH).unwrap();
        assert_eq!(status.order_id, None);
        assert_eq!(status.data.get(), data.to_string());
    }

    #[rstest]
    #[case("pending")]
    #[case("decode_failed")]
    fn unresolved_order_result_must_remain_empty(#[case] confirmation: &str) {
        assert!(parse_transaction_status(raw(&status_data(confirmation, "42")), HASH).is_err());
    }
}
