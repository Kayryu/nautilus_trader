// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! DeepX symbol conversion utilities.

use nautilus_model::identifiers::{InstrumentId, Symbol};

use super::{consts::DEEPX_VENUE, enums::DeepXProductType};

/// Converts a raw DeepX market symbol to a product-aware Nautilus instrument ID.
///
/// # Errors
///
/// Returns an error if the resulting Nautilus symbol is invalid.
pub fn instrument_id_from_raw(
    raw_symbol: &str,
    product_type: DeepXProductType,
) -> anyhow::Result<InstrumentId> {
    let symbol = match product_type {
        DeepXProductType::Spot => raw_symbol.to_string(),
        DeepXProductType::Perpetual => format!("{raw_symbol}-PERP"),
    };

    Ok(InstrumentId::new(
        Symbol::new_checked(symbol)?,
        *DEEPX_VENUE,
    ))
}

/// Converts a DeepX Nautilus instrument ID back to the raw venue symbol.
///
/// # Errors
///
/// Returns an error if the instrument belongs to another venue or its product type is ambiguous.
pub fn raw_symbol_from_instrument_id(
    instrument_id: InstrumentId,
    product_type: DeepXProductType,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        instrument_id.venue == *DEEPX_VENUE,
        "instrument `{instrument_id}` is not a DeepX instrument"
    );

    let symbol = instrument_id.symbol.as_str();
    match product_type {
        DeepXProductType::Spot => {
            anyhow::ensure!(
                !symbol.ends_with("-PERP"),
                "instrument `{instrument_id}` is not a DeepX spot instrument"
            );
            Ok(symbol.to_string())
        }
        DeepXProductType::Perpetual => symbol
            .strip_suffix("-PERP")
            .map(ToString::to_string)
            .ok_or_else(|| {
                anyhow::anyhow!("instrument `{instrument_id}` is not a DeepX perpetual")
            }),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(DeepXProductType::Spot, "ETH-USDC.DEEPX")]
    #[case(DeepXProductType::Perpetual, "ETH-USDC-PERP.DEEPX")]
    fn creates_product_aware_instrument_ids(
        #[case] product_type: DeepXProductType,
        #[case] expected: &str,
    ) {
        let instrument_id = instrument_id_from_raw("ETH-USDC", product_type).unwrap();

        assert_eq!(instrument_id.to_string(), expected);
    }

    #[rstest]
    #[case(DeepXProductType::Spot, "ETH-USDC.DEEPX")]
    #[case(DeepXProductType::Perpetual, "ETH-USDC-PERP.DEEPX")]
    fn round_trips_raw_symbols(
        #[case] product_type: DeepXProductType,
        #[case] instrument_id: &str,
    ) {
        let raw_symbol =
            raw_symbol_from_instrument_id(InstrumentId::from(instrument_id), product_type).unwrap();

        assert_eq!(raw_symbol, "ETH-USDC");
    }

    #[rstest]
    fn rejects_product_type_mismatch() {
        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");

        let result = raw_symbol_from_instrument_id(instrument_id, DeepXProductType::Spot);

        assert!(result.is_err());
    }
}
