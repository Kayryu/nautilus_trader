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

//! Bidirectional DeepX product-aware symbol mapping.

use nautilus_model::identifiers::{InstrumentId, Symbol};

use super::{
    consts::DEEPX_VENUE,
    enums::DeepXProductType,
    error::{DeepXError, Result},
};

const PERPETUAL_SUFFIX: &str = "-PERP";

/// Builds a canonical DeepX [`InstrumentId`] from a venue pair and product type.
///
/// # Errors
///
/// Returns an error when the pair is malformed or the product type is unsupported.
pub fn format_instrument_id(
    venue_pair: &str,
    product_type: &DeepXProductType,
) -> Result<InstrumentId> {
    let pair = canonical_pair(venue_pair)?;
    let symbol = match product_type {
        DeepXProductType::Spot => pair,
        DeepXProductType::Perpetual => format!("{pair}{PERPETUAL_SUFFIX}"),
        DeepXProductType::Unknown(value) => {
            return Err(DeepXError::UnsupportedProduct(value.clone()));
        }
    };

    Ok(InstrumentId::new(
        Symbol::from_str_unchecked(symbol),
        *DEEPX_VENUE,
    ))
}

/// Parses a canonical DeepX [`InstrumentId`] into its venue pair and product type.
///
/// # Errors
///
/// Returns an error when the venue or symbol shape is not canonical.
pub fn parse_instrument_id(instrument_id: &InstrumentId) -> Result<(String, DeepXProductType)> {
    if instrument_id.venue != *DEEPX_VENUE {
        return Err(DeepXError::InvalidSymbol(format!(
            "expected DEEPX venue, received {}",
            instrument_id.venue,
        )));
    }

    let symbol = instrument_id.symbol.as_str();
    let (pair, product_type) = match symbol.strip_suffix(PERPETUAL_SUFFIX) {
        Some(pair) => (pair, DeepXProductType::Perpetual),
        None => (symbol, DeepXProductType::Spot),
    };
    let pair = canonical_pair(pair)?;
    Ok((pair, product_type))
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
    use nautilus_model::identifiers::{Symbol, Venue};
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(DeepXProductType::Spot, "ETH-USDC.DEEPX")]
    #[case(DeepXProductType::Perpetual, "ETH-USDC-PERP.DEEPX")]
    fn symbol_round_trips(#[case] product_type: DeepXProductType, #[case] expected: &str) {
        let instrument_id = format_instrument_id(" eth-usdc ", &product_type).unwrap();

        assert_eq!(instrument_id.to_string(), expected);
        assert_eq!(
            parse_instrument_id(&instrument_id).unwrap(),
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
        assert!(format_instrument_id(pair, &DeepXProductType::Spot).is_err());
    }

    #[rstest]
    fn unknown_products_are_rejected() {
        assert_eq!(
            format_instrument_id("ETH-USDC", &DeepXProductType::Unknown("option".to_string()),),
            Err(DeepXError::UnsupportedProduct("option".to_string())),
        );
    }

    #[rstest]
    fn foreign_venues_are_rejected() {
        let instrument_id = InstrumentId::new(Symbol::new("ETH-USDC"), Venue::new("OTHER"));

        assert!(parse_instrument_id(&instrument_id).is_err());
    }
}
