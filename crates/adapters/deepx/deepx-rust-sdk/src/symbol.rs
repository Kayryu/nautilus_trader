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

//! Canonical DeepX pair and product symbol mapping.

use crate::{
    enums::DeepXProductType,
    error::{DeepXError, Result},
};

const PERPETUAL_SUFFIX: &str = "-PERP";

/// Builds a canonical DeepX symbol from a venue pair and product type.
///
/// # Errors
///
/// Returns an error for malformed pairs or unsupported product types.
pub fn format_product_symbol(venue_pair: &str, product_type: &DeepXProductType) -> Result<String> {
    let pair = canonical_pair(venue_pair)?;
    match product_type {
        DeepXProductType::Spot => Ok(pair),
        DeepXProductType::Perpetual => Ok(format!("{pair}{PERPETUAL_SUFFIX}")),
        DeepXProductType::Unknown(value) => Err(DeepXError::UnsupportedProduct(value.clone())),
    }
}

/// Parses a canonical DeepX symbol into a venue pair and product type.
///
/// # Errors
///
/// Returns an error if the symbol does not contain exactly one base and quote pair.
pub fn parse_product_symbol(symbol: &str) -> Result<(String, DeepXProductType)> {
    let (pair, product_type) = match symbol.strip_suffix(PERPETUAL_SUFFIX) {
        Some(pair) => (pair, DeepXProductType::Perpetual),
        None => (symbol, DeepXProductType::Spot),
    };
    Ok((canonical_pair(pair)?, product_type))
}

/// Normalizes a Spot pair for comparing REST records with market metadata.
///
/// This does not validate the pair identity.
#[must_use]
pub fn normalize_spot_pair(pair: &str) -> String {
    pair.strip_prefix("0x")
        .or_else(|| pair.strip_prefix("0X"))
        .unwrap_or(pair)
        .to_ascii_lowercase()
}

/// Decodes a deployment-provided Spot pair as an exact bytes32 identity.
pub(crate) fn decode_spot_pair_identity(pair: &str) -> Option<[u8; 32]> {
    let mut bytes = [0; 32];
    hex::decode_to_slice(pair.strip_prefix("0x").unwrap_or(pair), &mut bytes).ok()?;
    Some(bytes)
}

fn canonical_pair(value: &str) -> Result<String> {
    let pair = value.trim().to_ascii_uppercase();
    let mut components = pair.split('-');
    let base = components.next().unwrap_or_default();
    let quote = components.next().unwrap_or_default();
    if base.is_empty() || quote.is_empty() || components.next().is_some() {
        return Err(DeepXError::InvalidSymbol(format!(
            "expected BASE-QUOTE pair, received {value:?}",
        )));
    }
    Ok(format!("{base}-{quote}"))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(DeepXProductType::Spot, "ETH-USDC")]
    #[case(DeepXProductType::Perpetual, "ETH-USDC-PERP")]
    fn symbols_round_trip(#[case] product_type: DeepXProductType, #[case] expected: &str) {
        let symbol = format_product_symbol(" eth-usdc ", &product_type).unwrap();
        assert_eq!(symbol, expected);
        assert_eq!(
            parse_product_symbol(&symbol).unwrap(),
            ("ETH-USDC".to_string(), product_type),
        );
    }

    #[rstest]
    #[case("")]
    #[case("ETH")]
    #[case("ETH-")]
    #[case("-USDC")]
    #[case("ETH-USDC-PERP")]
    fn malformed_pairs_are_rejected(#[case] pair: &str) {
        assert!(format_product_symbol(pair, &DeepXProductType::Spot).is_err());
    }

    #[rstest]
    fn unknown_products_are_rejected() {
        assert_eq!(
            format_product_symbol("ETH-USDC", &DeepXProductType::Unknown("option".to_string())),
            Err(DeepXError::UnsupportedProduct("option".to_string())),
        );
    }

    #[rstest]
    #[case("0xAB12", "ab12")]
    #[case("0XAB12", "ab12")]
    #[case("AB12", "ab12")]
    #[case("not-a-pair", "not-a-pair")]
    fn spot_pair_normalization_preserves_comparison_semantics(
        #[case] pair: &str,
        #[case] expected: &str,
    ) {
        assert_eq!(normalize_spot_pair(pair), expected);
    }

    #[rstest]
    #[case(
        "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37",
        true
    )]
    #[case(
        "9068D4AC891A14784C17877EB74BD8489B3367C71D72766DBFA4DFBFB662FA37",
        true
    )]
    #[case("0x00", false)]
    #[case(
        "0X9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37",
        false
    )]
    fn spot_pair_identity_requires_exact_hex_bytes(#[case] pair: &str, #[case] valid: bool) {
        assert_eq!(decode_spot_pair_identity(pair).is_some(), valid);
    }
}
