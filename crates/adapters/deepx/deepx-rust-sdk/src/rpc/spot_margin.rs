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

//! Hash-pinned SubaccountInfo Spot-margin reads and SCALE decoding.

use serde::Deserialize;
use subxt_core::ext::scale_value::{Composite, Value, ValueDef, scale::decode_as_type};
use thiserror::Error;

use super::DeepXRpcClient;

#[derive(Debug, Deserialize)]
struct StorageChangeSet {
    block: String,
    changes: Vec<(String, Option<String>)>,
}

/// Errors from a hash-pinned SubaccountInfo read.
#[derive(Debug, Error)]
pub enum DeepXSpotMarginQueryError {
    /// A JSON-RPC request or response failed.
    #[error("failed to query DeepX Spot-margin storage: {0}")]
    Rpc(#[source] anyhow::Error),
    /// The storage response or SCALE value was missing, malformed, or out of scope.
    #[error("DeepX SubaccountInfo storage is invalid or belongs to another account")]
    InvalidStorage,
    /// The two hash-pinned storage methods disagreed.
    #[error("DeepX SubaccountInfo storage methods returned different values")]
    InconsistentStorage,
    /// The requested height no longer resolves to its observed hash.
    #[error("DeepX Spot-margin block is no longer canonical")]
    CanonicalHashMismatch,
}

/// Reads an exact subaccount's Spot-margin setting at one canonical block.
///
/// The expected authority must be established independently by the caller. All storage reads are
/// pinned to `block_hash`, and the block's canonical identity is checked before returning.
///
/// # Errors
///
/// Returns an error for an RPC failure, missing or conflicting storage, a different account
/// identity, malformed SCALE data, or a changed canonical block hash.
pub async fn get_spot_margin_at_block(
    client: &DeepXRpcClient,
    block_hash: [u8; 32],
    block_number: u32,
    metadata: &subxt_core::Metadata,
    subaccount: [u8; 20],
    expected_authority: [u8; 20],
) -> Result<bool, DeepXSpotMarginQueryError> {
    let block_hash = format!("0x{}", hex::encode(block_hash));
    let key = subaccount_info_storage_key(subaccount);
    let sets: Vec<StorageChangeSet> = client
        .query_storage_at(std::slice::from_ref(&key), &block_hash)
        .await
        .map_err(DeepXSpotMarginQueryError::Rpc)?;
    let [set] = <[_; 1]>::try_from(sets).map_err(|_| DeepXSpotMarginQueryError::InvalidStorage)?;
    if set.block != block_hash {
        return Err(DeepXSpotMarginQueryError::InvalidStorage);
    }
    let [(observed_key, Some(encoded))] =
        <[_; 1]>::try_from(set.changes).map_err(|_| DeepXSpotMarginQueryError::InvalidStorage)?
    else {
        return Err(DeepXSpotMarginQueryError::InvalidStorage);
    };
    if observed_key != key {
        return Err(DeepXSpotMarginQueryError::InvalidStorage);
    }
    let direct = client
        .get_storage(&key, &block_hash)
        .await
        .map_err(DeepXSpotMarginQueryError::Rpc)?;
    if direct.as_deref() != Some(encoded.as_str()) {
        return Err(DeepXSpotMarginQueryError::InconsistentStorage);
    }
    let enabled = decode_spot_margin(metadata, &encoded, subaccount, expected_authority)
        .ok_or(DeepXSpotMarginQueryError::InvalidStorage)?;
    let canonical = client
        .get_block_hash(block_number)
        .await
        .map_err(DeepXSpotMarginQueryError::Rpc)?;
    if canonical.as_deref() != Some(block_hash.as_str()) {
        return Err(DeepXSpotMarginQueryError::CanonicalHashMismatch);
    }
    Ok(enabled)
}

/// Computes the SubaccountInfo storage key for an AccountId20.
#[must_use]
pub fn subaccount_info_storage_key(subaccount: [u8; 20]) -> String {
    let mut key = Vec::with_capacity(52);
    key.extend_from_slice(&sp_crypto_hashing::twox_128(b"Subaccount"));
    key.extend_from_slice(&sp_crypto_hashing::twox_128(b"SubaccountInfo"));
    key.extend_from_slice(&subaccount);
    format!("0x{}", hex::encode(key))
}

/// Decodes a SubaccountInfo value only when both account identities match.
#[must_use]
pub fn decode_spot_margin(
    metadata: &subxt_core::Metadata,
    encoded: &str,
    subaccount: [u8; 20],
    expected_authority: [u8; 20],
) -> Option<bool> {
    let value_type = metadata
        .pallet_by_name("Subaccount")?
        .storage()?
        .entry_by_name("SubaccountInfo")?
        .entry_type()
        .value_ty();
    let bytes = hex::decode(encoded.strip_prefix("0x")?).ok()?;
    if bytes.is_empty() || bytes.len() > 4096 {
        return None;
    }
    let mut input = bytes.as_slice();
    let info = decode_as_type(&mut input, value_type, metadata.types()).ok()?;
    if !input.is_empty() {
        return None;
    }
    let ValueDef::Composite(Composite::Named(fields)) = &info.value else {
        return None;
    };
    let field = |name| {
        fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value)
    };
    if scale_account_id20(field("authority")?)? != expected_authority
        || scale_account_id20(field("address")?)? != subaccount
    {
        return None;
    }
    field("spot_margin_trading_enabled")?.as_bool()
}

fn scale_account_id20(value: &Value<u32>) -> Option<[u8; 20]> {
    let ValueDef::Composite(Composite::Unnamed(items)) = &value.value else {
        return None;
    };
    if let [inner] = items.as_slice() {
        return scale_account_id20(inner);
    }
    let bytes = items
        .iter()
        .map(|item| u8::try_from(item.as_u128()?).ok())
        .collect::<Option<Vec<_>>>()?;
    bytes.try_into().ok()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const WALLET: [u8; 20] = [
        0x78, 0x1e, 0xd3, 0x5b, 0x16, 0x70, 0x68, 0xc9, 0x3d, 0xfa, 0xda, 0xb4, 0x1d, 0xfb, 0x68,
        0x0e, 0xda, 0xca, 0x4e, 0x50,
    ];
    const SUBACCOUNT: [u8; 20] = [
        0x4d, 0xed, 0x31, 0xcb, 0x63, 0x94, 0x9b, 0x52, 0xf9, 0xdf, 0xc9, 0xbc, 0xfa, 0xde, 0x4e,
        0xab, 0x70, 0x17, 0xea, 0xdc,
    ];
    const STORAGE: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e504ded31cb63949b52f9dfc9bcfade4eab7017eadc146b617a656501000100000000000100000000";

    fn metadata() -> subxt_core::Metadata {
        let fixture = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        ));
        let fixture: serde_json::Value = serde_json::from_str(fixture).unwrap();
        let encoded = fixture["result"]
            .as_str()
            .unwrap()
            .strip_prefix("0x")
            .unwrap();
        let bytes = hex::decode(encoded).unwrap();
        subxt_core::metadata::decode_from(&bytes).unwrap()
    }

    #[rstest]
    fn subaccount_info_key_uses_identity_account_id20() {
        assert_eq!(
            subaccount_info_storage_key(SUBACCOUNT),
            "0x7a722acdbb19d1db692588a0209d14499ac3b2b9666a5eaa79e0ca457b2ea3a94ded31cb63949b52f9dfc9bcfade4eab7017eadc"
        );
    }

    #[rstest]
    fn decodes_captured_spot_margin_and_rejects_foreign_identity() {
        let metadata = metadata();
        assert_eq!(
            decode_spot_margin(&metadata, STORAGE, SUBACCOUNT, WALLET),
            Some(false)
        );
        assert_eq!(
            decode_spot_margin(&metadata, STORAGE, [0; 20], WALLET),
            None
        );
        assert_eq!(
            decode_spot_margin(&metadata, STORAGE, SUBACCOUNT, [0; 20]),
            None
        );
        assert_eq!(
            decode_spot_margin(&metadata, &format!("{STORAGE}00"), SUBACCOUNT, WALLET),
            None
        );
    }
}
