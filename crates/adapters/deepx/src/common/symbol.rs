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

use deepx_rust_sdk::symbol::{format_product_symbol, parse_product_symbol};
use nautilus_model::identifiers::{InstrumentId, Symbol};

use super::{
    DeepXProductType,
    consts::DEEPX_VENUE,
    error::{DeepXError, Result},
};

/// Builds a canonical DeepX [`InstrumentId`] from a venue pair and product type.
///
/// # Errors
///
/// Returns an error when the pair is malformed or the product type is unsupported.
pub fn format_instrument_id(
    venue_pair: &str,
    product_type: &DeepXProductType,
) -> Result<InstrumentId> {
    let symbol = format_product_symbol(venue_pair, product_type)?;

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

    parse_product_symbol(instrument_id.symbol.as_str())
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
    fn foreign_venues_are_rejected() {
        let instrument_id = InstrumentId::new(Symbol::new("ETH-USDC"), Venue::new("OTHER"));

        assert!(parse_instrument_id(&instrument_id).is_err());
    }
}
