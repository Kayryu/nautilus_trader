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

//! Exact conversion helpers for DeepX discrete financial values.

use rust_decimal::Decimal;

use super::error::{DeepXError, Result};

/// Converts a scaled integer into a [`Decimal`] without floating-point arithmetic.
///
/// # Errors
///
/// Returns an error when the scale exceeds the [`Decimal`] representation.
pub fn scaled_i128_to_decimal(value: i128, scale: u32) -> Result<Decimal> {
    Decimal::try_from_i128_with_scale(value, scale).map_err(|e| {
        DeepXError::DecimalOverflow(format!(
            "cannot represent integer {value} with scale {scale}: {e}",
        ))
    })
}

/// Converts a [`Decimal`] into an integer at the requested scale exactly.
///
/// # Errors
///
/// Returns an error when conversion would discard precision or overflow `i128`.
pub fn decimal_to_scaled_i128(value: Decimal, scale: u32) -> Result<i128> {
    if scale > Decimal::MAX_SCALE {
        return Err(DeepXError::DecimalOverflow(format!(
            "scale {scale} exceeds maximum {}",
            Decimal::MAX_SCALE,
        )));
    }

    let normalized = value.normalize();
    if normalized.scale() > scale {
        return Err(DeepXError::InexactDecimal(format!(
            "value {value} has scale {}, target scale is {scale}",
            normalized.scale(),
        )));
    }

    let exponent = scale - normalized.scale();
    let factor = 10_i128
        .checked_pow(exponent)
        .ok_or_else(|| DeepXError::DecimalOverflow(format!("10^{exponent} exceeds i128")))?;
    normalized.mantissa().checked_mul(factor).ok_or_else(|| {
        DeepXError::DecimalOverflow(format!("value {value} exceeds i128 at scale {scale}",))
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("123.45", 2, 12_345)]
    #[case("123.45", 4, 1_234_500)]
    #[case("-0.000001", 6, -1)]
    #[case("0", 18, 0)]
    fn decimal_integer_round_trip(#[case] raw: &str, #[case] scale: u32, #[case] expected: i128) {
        let value = Decimal::from_str(raw).unwrap();

        assert_eq!(decimal_to_scaled_i128(value, scale).unwrap(), expected);
        assert_eq!(scaled_i128_to_decimal(expected, scale).unwrap(), value);
    }

    #[rstest]
    fn conversion_rejects_precision_loss() {
        let value = Decimal::from_str("1.001").unwrap();

        assert!(matches!(
            decimal_to_scaled_i128(value, 2),
            Err(DeepXError::InexactDecimal(_)),
        ));
    }

    #[rstest]
    fn conversion_rejects_unsupported_scale() {
        assert!(matches!(
            scaled_i128_to_decimal(1, Decimal::MAX_SCALE + 1),
            Err(DeepXError::DecimalOverflow(_)),
        ));
    }
}
