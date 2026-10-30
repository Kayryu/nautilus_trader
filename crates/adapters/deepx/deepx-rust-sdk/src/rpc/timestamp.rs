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

//! Hash-pinned reads and SCALE decoding for `Timestamp.Now`.

use parity_scale_codec::{Compact, Decode};
use serde_json::json;
use thiserror::Error;

use super::{DeepXRpcClient, DeepXRpcStorageChangeSet};

/// Errors from a hash-pinned `Timestamp.Now` read.
#[derive(Debug, Error)]
pub enum DeepXTimestampReadError {
    /// A JSON-RPC request or response failed.
    #[error("failed to query DeepX Timestamp.Now storage: {0}")]
    Rpc(#[source] anyhow::Error),
    /// The storage value was not one exact non-zero SCALE `u64`.
    #[error("DeepX Timestamp.Now storage is invalid")]
    InvalidStorage,
}

/// Reads `Timestamp.Now` at an explicit block hash.
///
/// # Errors
///
/// Returns an RPC error or an invalid-storage error for malformed SCALE data.
pub async fn get_timestamp_now(
    client: &DeepXRpcClient,
    block_hash: [u8; 32],
) -> Result<u64, DeepXTimestampReadError> {
    let encoded: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [timestamp_now_storage_key(), format!("0x{}", hex::encode(block_hash))],
        }))
        .await
        .map_err(DeepXTimestampReadError::Rpc)?;
    decode_timestamp_now(&encoded).ok_or(DeepXTimestampReadError::InvalidStorage)
}

/// Reads and validates one `Timestamp.Now` storage change at a fixed block hash.
///
/// # Errors
///
/// Returns an RPC error or an invalid-storage error for a mismatched or malformed change set.
pub async fn query_timestamp_now_at_block(
    client: &DeepXRpcClient,
    block_hash: [u8; 32],
) -> Result<u64, DeepXTimestampReadError> {
    let block_hash = format!("0x{}", hex::encode(block_hash));
    let key = timestamp_now_storage_key();
    let mut sets: Vec<DeepXRpcStorageChangeSet> = client
        .query_storage_at(std::slice::from_ref(&key), &block_hash)
        .await
        .map_err(DeepXTimestampReadError::Rpc)?;
    if sets.len() != 1 || sets[0].block != block_hash {
        return Err(DeepXTimestampReadError::InvalidStorage);
    }
    let changes = std::mem::take(&mut sets[0].changes);
    let [(returned_key, Some(encoded))] = <[(String, Option<String>); 1]>::try_from(changes)
        .map_err(|_| DeepXTimestampReadError::InvalidStorage)?
    else {
        return Err(DeepXTimestampReadError::InvalidStorage);
    };
    if returned_key != key {
        return Err(DeepXTimestampReadError::InvalidStorage);
    }
    decode_timestamp_now(&encoded).ok_or(DeepXTimestampReadError::InvalidStorage)
}

/// Computes the `Timestamp.Now` storage key.
#[must_use]
pub fn timestamp_now_storage_key() -> String {
    let mut encoded = Vec::with_capacity(32);
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(b"Timestamp"));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(b"Now"));
    format!("0x{}", hex::encode(encoded))
}

/// Decodes one exact non-zero SCALE `u64` from a hex storage value.
#[must_use]
pub fn decode_timestamp_now(encoded: &str) -> Option<u64> {
    let bytes = hex::decode(encoded.strip_prefix("0x")?).ok()?;
    let bytes: [u8; 8] = bytes.try_into().ok()?;
    let timestamp_ms = u64::from_le_bytes(bytes);
    (timestamp_ms != 0).then_some(timestamp_ms)
}

/// Decodes the unique unsigned `Timestamp.set` inherent in a block.
#[must_use]
pub fn decode_timestamp_set_inherent(extrinsics: &[String]) -> Option<u64> {
    let mut block_ms = None;
    for encoded in extrinsics {
        let bytes = hex::decode(encoded.strip_prefix("0x")?).ok()?;
        let mut input = bytes.as_slice();
        let length = Compact::<u32>::decode(&mut input).ok()?;
        if input.len() != length.0 as usize {
            return None;
        }
        if input.starts_with(&[4, 2, 0]) {
            let mut timestamp_input = &input[3..];
            let timestamp = Compact::<u64>::decode(&mut timestamp_input).ok()?;
            if timestamp.0 == 0
                || !timestamp_input.is_empty()
                || block_ms.replace(timestamp.0).is_some()
            {
                return None;
            }
        }
    }
    block_ms
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, routing::post};
    use parity_scale_codec::{Compact, Encode};
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::{
        DeepXRpcClient, DeepXTimestampReadError, decode_timestamp_now,
        decode_timestamp_set_inherent, query_timestamp_now_at_block, timestamp_now_storage_key,
    };

    #[rstest]
    #[case::valid(0)]
    #[case::wrong_block(1)]
    #[case::duplicate_key(2)]
    #[case::missing_value(3)]
    #[case::malformed_value(4)]
    #[tokio::test]
    async fn timestamp_query_requires_exact_change_set(#[case] scenario: u8) {
        let block_hash = [0x22; 32];
        let encoded_hash = format!("0x{}", hex::encode(block_hash));
        let key = timestamp_now_storage_key();
        let app = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let key = key.clone();
                let encoded_hash = encoded_hash.clone();
                async move {
                    assert_eq!(request["method"], "state_queryStorageAt");
                    assert_eq!(request["params"], json!([[key], encoded_hash]));
                    let block = if scenario == 1 { "0x00" } else { &encoded_hash };
                    let value = match scenario {
                        3 => Value::Null,
                        4 => json!("0xzz"),
                        _ => json!("0x0100000000000000"),
                    };
                    let changes = if scenario == 2 {
                        json!([[key, value], [key, value]])
                    } else {
                        json!([[key, value]])
                    };
                    Json(json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": [{"block": block, "changes": changes}],
                    }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let result = query_timestamp_now_at_block(&DeepXRpcClient::new(url), block_hash).await;
        if scenario == 0 {
            assert_eq!(result.unwrap(), 1);
        } else {
            assert!(matches!(
                result,
                Err(DeepXTimestampReadError::InvalidStorage)
            ));
        }
        server.abort();
    }

    #[rstest]
    fn timestamp_key_matches_twox_storage_prefix() {
        assert_eq!(
            timestamp_now_storage_key(),
            "0xf0c365c3cf59d671eb72da0e7a4113c49f1f0515f462cdcf84e0f1d6045dfcbb",
        );
    }

    #[rstest]
    #[case("", None)]
    #[case("42", None)]
    #[case("0x", None)]
    #[case("0x0100000000000000", Some(1))]
    #[case("0x0000000000000000", None)]
    #[case("0x01000000000000", None)]
    #[case("0x010000000000000000", None)]
    #[case("0100000000000000", None)]
    #[case("0xzz", None)]
    fn timestamp_requires_one_nonzero_scale_u64(
        #[case] encoded: &str,
        #[case] expected: Option<u64>,
    ) {
        assert_eq!(decode_timestamp_now(encoded), expected);
    }

    #[rstest]
    fn timestamp_inherent_requires_one_exact_nonzero_value() {
        let inherent = |timestamp: u64| {
            let mut call = vec![4, 2, 0];
            call.extend(Compact(timestamp).encode());
            let mut extrinsic = Compact(u32::try_from(call.len()).unwrap()).encode();
            extrinsic.extend(call);
            format!("0x{}", hex::encode(extrinsic))
        };
        let valid = inherent(123);
        assert_eq!(
            decode_timestamp_set_inherent(std::slice::from_ref(&valid)),
            Some(123)
        );
        assert_eq!(decode_timestamp_set_inherent(&[inherent(0)]), None);
        assert_eq!(decode_timestamp_set_inherent(&[valid.clone(), valid]), None);
        assert_eq!(decode_timestamp_set_inherent(&["0x00".to_string()]), None);
    }

    #[rstest]
    fn captured_timestamp_inherent_decodes_exact_milliseconds() {
        let captured = "0x280402000bf4a65c1ba101".to_string();
        assert_eq!(
            decode_timestamp_set_inherent(std::slice::from_ref(&captured)),
            Some(1_791_460_419_316),
        );
        assert_eq!(
            decode_timestamp_set_inherent(&[captured.clone(), captured]),
            None,
        );
        assert_eq!(
            decode_timestamp_set_inherent(&["0x280402000bf4a65c1ba1".to_string()]),
            None,
        );
    }
}
