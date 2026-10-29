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
//  See the GNU Lesser General Public License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Bounded SCALE decoding for lending position runtime API responses.

use parity_scale_codec::{Compact, Decode, DecodeAll};

use super::storage::DeepXLendingPoolStorage;

const MAX_LENDING_ASSET_SYMBOL_BYTES: usize = 32;
const MAX_LENDING_POSITION_ASSETS: usize = 64;

/// One asset position returned by the DeepX lending runtime API.
#[derive(Debug, PartialEq, Eq)]
pub struct DeepXRawLendingPosition {
    pub market_id: u8,
    pub asset: Vec<u8>,
    pub scaled_deposit: u128,
    pub scaled_borrow: u128,
}

/// Decodes one hex-encoded lending pool response without trailing SCALE data.
#[must_use]
pub fn decode_lending_pool_response(encoded: &str) -> Option<Option<DeepXLendingPoolStorage>> {
    decode_lending_asset_response(encoded)
}

/// Decodes one hex-encoded effective lending balance without trailing SCALE data.
#[must_use]
pub fn decode_lending_amount_response(encoded: &str) -> Option<Option<u128>> {
    decode_lending_asset_response(encoded)
}

fn decode_lending_asset_response<T: Decode>(encoded: &str) -> Option<T> {
    let bytes = hex::decode(encoded.strip_prefix("0x")?).ok()?;
    T::decode_all(&mut &bytes[..]).ok()
}

/// Decodes one canonical, account-scoped lending position response.
#[must_use]
pub fn decode_lending_positions(
    encoded: &str,
    subaccount: [u8; 20],
) -> Option<Vec<DeepXRawLendingPosition>> {
    let bytes = hex::decode(encoded.strip_prefix("0x")?).ok()?;
    let mut input = bytes.as_slice();
    let account_count = decode_bounded_compact_len(&mut input, 1)?;
    if account_count == 0 {
        return input.is_empty().then(Vec::new);
    }
    let account = <[u8; 20]>::decode(&mut input).ok()?;
    if account != subaccount {
        return None;
    }
    let market_count = decode_bounded_compact_len(&mut input, usize::from(u8::MAX) + 1)
        .filter(|count| *count > 0)?;
    let mut positions = Vec::new();
    let mut previous_market = None;
    for _ in 0..market_count {
        let market_id = u8::decode(&mut input).ok()?;
        if previous_market.is_some_and(|previous| market_id <= previous) {
            return None;
        }
        previous_market = Some(market_id);
        let asset_count = decode_bounded_compact_len(&mut input, MAX_LENDING_POSITION_ASSETS)
            .filter(|count| *count > 0)?;
        let mut previous_asset: Option<Vec<u8>> = None;
        for _ in 0..asset_count {
            let asset = decode_bounded_bytes(&mut input, MAX_LENDING_ASSET_SYMBOL_BYTES)
                .filter(|value| !value.is_empty())?;
            if previous_asset
                .as_ref()
                .is_some_and(|previous| asset <= *previous)
            {
                return None;
            }
            previous_asset = Some(asset.clone());
            let scaled_deposit = u128::decode(&mut input).ok()?;
            let scaled_borrow = u128::decode(&mut input).ok()?;
            if scaled_deposit == 0 && scaled_borrow == 0 {
                return None;
            }
            positions.push(DeepXRawLendingPosition {
                market_id,
                asset,
                scaled_deposit,
                scaled_borrow,
            });
        }
    }
    input.is_empty().then_some(positions)
}

fn decode_bounded_compact_len(input: &mut &[u8], maximum: usize) -> Option<usize> {
    let Compact(value) = Compact::<u32>::decode(input).ok()?;
    let value = usize::try_from(value).ok()?;
    (value <= maximum).then_some(value)
}

fn decode_bounded_bytes(input: &mut &[u8], maximum: usize) -> Option<Vec<u8>> {
    let len = decode_bounded_compact_len(input, maximum)?;
    if input.len() < len {
        return None;
    }
    let (value, remaining) = input.split_at(len);
    *input = remaining;
    Some(value.to_vec())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use parity_scale_codec::Encode;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn lending_asset_response_requires_complete_scale_value() {
        assert_eq!(decode_lending_amount_response("0x00"), Some(None));
        assert_eq!(decode_lending_amount_response("0x0000"), None);
        assert_eq!(decode_lending_amount_response("0xzz"), None);
        assert_eq!(decode_lending_amount_response("00"), None);
        assert!(matches!(decode_lending_pool_response("0x00"), Some(None)));
        assert!(decode_lending_pool_response("0x0000").is_none());
    }

    #[rstest]
    fn lending_positions_decode_exact_scoped_shares() {
        let account = [0x11; 20];
        let encoded = BTreeMap::from([(
            account,
            BTreeMap::from([(
                1_u8,
                BTreeMap::from([(b"usdc".to_vec(), (100_u128, 2_u128))]),
            )]),
        )])
        .encode();
        let text = format!("0x{}", hex::encode(&encoded));

        assert_eq!(
            decode_lending_positions(&text, account),
            Some(vec![DeepXRawLendingPosition {
                market_id: 1,
                asset: b"usdc".to_vec(),
                scaled_deposit: 100,
                scaled_borrow: 2,
            }])
        );
        assert_eq!(decode_lending_positions(&text, [0x22; 20]), None);
        assert_eq!(
            decode_lending_positions(&format!("{text}00"), account),
            None
        );
    }

    #[rstest]
    #[case("foreign")]
    #[case("zero")]
    #[case("overlong")]
    #[case("trailing")]
    fn lending_positions_reject_invalid_account_evidence(#[case] scenario: &str) {
        let account = if scenario == "foreign" {
            [0x22; 20]
        } else {
            [0x11; 20]
        };
        let asset = if scenario == "overlong" {
            vec![b'x'; 33]
        } else {
            b"usdc".to_vec()
        };
        let amounts = if scenario == "zero" {
            (0_u128, 0_u128)
        } else {
            (1, 0)
        };
        let mut encoded = BTreeMap::from([(
            account,
            BTreeMap::from([(1_u8, BTreeMap::from([(asset, amounts)]))]),
        )])
        .encode();
        if scenario == "trailing" {
            encoded.push(0);
        }

        assert_eq!(
            decode_lending_positions(&format!("0x{}", hex::encode(encoded)), [0x11; 20]),
            None,
        );
    }

    #[rstest]
    fn lending_positions_accept_canonical_empty_account_evidence() {
        let encoded =
            BTreeMap::<[u8; 20], BTreeMap<u8, BTreeMap<Vec<u8>, (u128, u128)>>>::new().encode();

        assert_eq!(
            decode_lending_positions(&format!("0x{}", hex::encode(encoded)), [0x11; 20]),
            Some(Vec::new()),
        );
    }
}
