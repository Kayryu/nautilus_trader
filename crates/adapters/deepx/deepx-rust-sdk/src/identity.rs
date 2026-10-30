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

//! Exact DeepX wire identities shared by REST, WebSocket, and adapter boundaries.

/// Decodes a `0x`-prefixed AccountId20 address.
#[must_use]
pub fn decode_account_id20(value: &str) -> Option<[u8; 20]> {
    let encoded = value.strip_prefix("0x")?;
    let mut address = [0; 20];
    hex::decode_to_slice(encoded, &mut address).ok()?;
    Some(address)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc", true)]
    #[case("0x4DED31CB63949B52F9DFC9BCFADE4EAB7017EADC", true)]
    #[case("4ded31cb63949b52f9dfc9bcfade4eab7017eadc", false)]
    #[case("0X4ded31cb63949b52f9dfc9bcfade4eab7017eadc", false)]
    #[case("0x00", false)]
    #[case("0x4ded31cb63949b52f9dfc9bcfade4eab7017eadg", false)]
    fn account_id20_requires_exact_prefixed_hex(#[case] value: &str, #[case] valid: bool) {
        assert_eq!(decode_account_id20(value).is_some(), valid);
    }
}
