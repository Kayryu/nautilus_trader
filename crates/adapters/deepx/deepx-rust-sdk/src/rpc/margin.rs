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

//! Exact decoding of the DeepX margin runtime API response.

use std::collections::BTreeMap;

use parity_scale_codec::Decode;
use thiserror::Error;

use super::DeepXRawMarginRequirement;

/// A margin response with invalid encoding, account identity, or trailing data.
#[derive(Debug, Error)]
#[error("invalid DeepX margin requirement evidence")]
pub struct DeepXMarginDecodeError;

/// Decodes a complete one-account margin runtime API response.
///
/// # Errors
///
/// Returns an error unless the response is a prefixed SCALE map with exactly the requested
/// subaccount and no trailing bytes.
pub fn decode_margin_requirement(
    encoded: &str,
    subaccount: [u8; 20],
) -> Result<DeepXRawMarginRequirement, DeepXMarginDecodeError> {
    let bytes = encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())
        .ok_or(DeepXMarginDecodeError)?;
    let mut input = bytes.as_slice();
    let values = BTreeMap::<[u8; 20], (u128, u128)>::decode(&mut input)
        .map_err(|_| DeepXMarginDecodeError)?;
    if !input.is_empty() || values.len() != 1 {
        return Err(DeepXMarginDecodeError);
    }
    let (total_collateral, total_margin_required) = values
        .get(&subaccount)
        .copied()
        .ok_or(DeepXMarginDecodeError)?;
    Ok(DeepXRawMarginRequirement {
        total_collateral,
        total_margin_required,
    })
}

#[cfg(test)]
mod tests {
    use parity_scale_codec::Encode;
    use rstest::rstest;

    use super::*;

    const SUBACCOUNT: [u8; 20] = [0x11; 20];

    #[rstest]
    fn decodes_exact_one_account_response() {
        let encoded = BTreeMap::from([(SUBACCOUNT, (u128::MAX, 42_u128))]).encode();
        let result =
            decode_margin_requirement(&format!("0x{}", hex::encode(encoded)), SUBACCOUNT).unwrap();
        assert_eq!(result.total_collateral, u128::MAX);
        assert_eq!(result.total_margin_required, 42);
    }

    #[rstest]
    #[case(BTreeMap::<[u8; 20], (u128, u128)>::new().encode())]
    #[case(BTreeMap::from([([0x22; 20], (1_u128, 2_u128))]).encode())]
    #[case({
        let mut encoded = BTreeMap::from([(SUBACCOUNT, (1_u128, 2_u128))]).encode();
        encoded.push(0);
        encoded
    })]
    #[case(BTreeMap::from([(SUBACCOUNT, (1_u128, 2_u128)), ([0x22; 20], (3, 4))]).encode())]
    fn rejects_missing_foreign_trailing_or_multiple_accounts(#[case] encoded: Vec<u8>) {
        assert!(
            decode_margin_requirement(&format!("0x{}", hex::encode(encoded)), SUBACCOUNT).is_err()
        );
    }

    #[rstest]
    #[case("deadbeef")]
    #[case("0xyz")]
    #[case("0x")]
    fn rejects_invalid_hex(#[case] encoded: &str) {
        assert!(decode_margin_requirement(encoded, SUBACCOUNT).is_err());
    }
}
