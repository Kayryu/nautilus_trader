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
}
