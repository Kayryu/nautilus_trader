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

//! Exact decoding of DeepX chain identity fields returned by JSON-RPC.

use std::num::ParseIntError;

use thiserror::Error;

/// Invalid hexadecimal integer returned by a DeepX RPC method.
#[derive(Debug, Error)]
pub enum DeepXRpcHexIntegerError {
    /// The integer is missing its hexadecimal prefix.
    #[error("hexadecimal prefix is missing")]
    MissingPrefix,
    /// The integer has no digits.
    #[error("hexadecimal integer is empty")]
    Empty,
    /// The integer contains invalid digits or exceeds `u128`.
    #[error(transparent)]
    Invalid(#[from] ParseIntError),
}

/// Decodes one 0x-prefixed 32-byte chain hash.
#[must_use]
pub fn decode_prefixed_hash(encoded: &str) -> Option<[u8; 32]> {
    let bytes = hex::decode(encoded.strip_prefix("0x")?).ok()?;
    bytes.try_into().ok()
}

/// Decodes one 0x-prefixed hexadecimal block number within `u64`.
#[must_use]
pub fn decode_block_number_u64(encoded: &str) -> Option<u64> {
    let value = encoded
        .strip_prefix("0x")
        .filter(|value| !value.is_empty())?;
    u64::from_str_radix(value, 16).ok()
}

/// Decodes one 0x-prefixed hexadecimal block number within `u32`.
#[must_use]
pub fn decode_block_number_u32(encoded: &str) -> Option<u32> {
    decode_block_number_u64(encoded).and_then(|value| u32::try_from(value).ok())
}

/// Decodes nonempty 0x-prefixed runtime metadata bytes.
#[must_use]
pub fn decode_runtime_metadata(encoded: &str) -> Option<Vec<u8>> {
    let value = encoded
        .strip_prefix("0x")
        .filter(|value| !value.is_empty())?;
    hex::decode(value).ok()
}

/// Decodes a 0x- or 0X-prefixed hexadecimal integer within `u128`.
///
/// # Errors
///
/// Returns an error for a missing prefix, empty value, invalid digit, or overflow.
pub fn decode_prefixed_u128(encoded: &str) -> Result<u128, DeepXRpcHexIntegerError> {
    let value = encoded
        .strip_prefix("0x")
        .or_else(|| encoded.strip_prefix("0X"))
        .ok_or(DeepXRpcHexIntegerError::MissingPrefix)?;
    if value.is_empty() {
        return Err(DeepXRpcHexIntegerError::Empty);
    }
    Ok(u128::from_str_radix(value, 16)?)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn prefixed_hash_requires_exact_bytes() {
        assert_eq!(
            decode_prefixed_hash(&format!("0x{}", "11".repeat(32))),
            Some([0x11; 32])
        );
        for encoded in [
            "0x".to_string(),
            "0x11".to_string(),
            "11".repeat(32),
            "0xzz".to_string(),
        ] {
            assert_eq!(decode_prefixed_hash(&encoded), None);
        }
    }

    #[rstest]
    fn block_number_rejects_missing_prefix_and_overflow() {
        assert_eq!(decode_block_number_u64("0x2a"), Some(42));
        assert_eq!(decode_block_number_u32("0x2a"), Some(42));
        assert_eq!(decode_block_number_u64("0x100000000"), Some(4_294_967_296));
        assert_eq!(decode_block_number_u32("0x100000000"), None);
        for encoded in ["", "0x", "42", "0xgg", "0xzz", "0x10000000000000000"] {
            assert_eq!(decode_block_number_u64(encoded), None);
            assert_eq!(decode_block_number_u32(encoded), None);
        }
    }

    #[rstest]
    fn runtime_metadata_requires_nonempty_prefixed_hex() {
        assert_eq!(
            decode_runtime_metadata("0x6d657461"),
            Some(b"meta".to_vec())
        );
        for encoded in ["", "0x", "6d657461", "0x6d65746", "0xzz"] {
            assert_eq!(decode_runtime_metadata(encoded), None);
        }
    }

    #[rstest]
    fn prefixed_u128_accepts_both_prefixes_and_rejects_invalid_values() {
        assert_eq!(decode_prefixed_u128("0x2a").unwrap(), 42);
        assert_eq!(decode_prefixed_u128("0X2A").unwrap(), 42);
        assert!(matches!(
            decode_prefixed_u128("2a"),
            Err(DeepXRpcHexIntegerError::MissingPrefix)
        ));
        assert!(matches!(
            decode_prefixed_u128("0x"),
            Err(DeepXRpcHexIntegerError::Empty)
        ));
        assert!(matches!(
            decode_prefixed_u128("0xzz"),
            Err(DeepXRpcHexIntegerError::Invalid(_))
        ));
        assert!(matches!(
            decode_prefixed_u128("0x100000000000000000000000000000000"),
            Err(DeepXRpcHexIntegerError::Invalid(_))
        ));
    }
}
