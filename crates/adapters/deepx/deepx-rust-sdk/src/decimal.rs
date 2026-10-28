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

//! Exact decoding of DeepX wire-level decimal values.

use rust_decimal::Decimal;
use serde::{Deserialize, de::Error as _};
use serde_json::value::RawValue;

pub fn parse_decimal(raw: &str) -> Result<Decimal, String> {
    let Some((mantissa, exponent)) = raw.split_once(['e', 'E']) else {
        return Decimal::from_str_exact(raw).map_err(|e| e.to_string());
    };
    let exponent = exponent.parse::<i32>().map_err(|e| e.to_string())?;
    let mantissa = mantissa.strip_prefix('-').unwrap_or(mantissa);
    let (whole, fractional) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty()
        || !whole.bytes().all(|digit| digit.is_ascii_digit())
        || !fractional.bytes().all(|digit| digit.is_ascii_digit())
        || (mantissa.contains('.') && fractional.is_empty())
    {
        return Err("invalid scientific decimal mantissa".to_string());
    }
    let digits = format!("{whole}{fractional}");
    let Some(first_nonzero) = digits.bytes().position(|digit| digit != b'0') else {
        return Ok(Decimal::ZERO);
    };
    let significant = digits[first_nonzero..].trim_end_matches('0');
    let point = i64::try_from(whole.len())
        .ok()
        .and_then(|whole_len| whole_len.checked_add(i64::from(exponent)))
        .and_then(|point| point.checked_sub(i64::try_from(first_nonzero).ok()?))
        .ok_or_else(|| "scientific decimal exponent is out of range".to_string())?;
    let length = i64::try_from(significant.len())
        .map_err(|_| "scientific decimal mantissa is out of range".to_string())?;
    if point > 29 || length - point > 28 {
        return Err("scientific decimal is not exactly representable".to_string());
    }
    let mut plain = String::from(if raw.starts_with('-') { "-" } else { "" });
    if point <= 0 {
        plain.push_str("0.");
        plain.extend(std::iter::repeat_n('0', (-point) as usize));
        plain.push_str(significant);
    } else if point >= length {
        plain.push_str(significant);
        plain.extend(std::iter::repeat_n('0', (point - length) as usize));
    } else {
        let split = point as usize;
        plain.push_str(&significant[..split]);
        plain.push('.');
        plain.push_str(&significant[split..]);
    }
    Decimal::from_str_exact(&plain).map_err(|e| e.to_string())
}

pub mod exact_decimal {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };

        parse_decimal(&value).map_err(D::Error::custom)
    }
}

pub mod optional_exact_decimal {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        if raw.get() == "null" {
            return Ok(None);
        }
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };

        parse_decimal(&value).map(Some).map_err(D::Error::custom)
    }
}

pub mod optional_none_decimal {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        if raw.get() == "null" {
            return Ok(None);
        }
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };
        if value == "none" {
            return Ok(None);
        }

        parse_decimal(&value).map(Some).map_err(D::Error::custom)
    }
}

pub mod exact_u128 {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };
        value.parse().map_err(D::Error::custom)
    }
}

pub mod exact_i128 {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i128, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };
        value.parse().map_err(D::Error::custom)
    }
}

pub mod optional_exact_u128 {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u128>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        if raw.get() == "null" {
            return Ok(None);
        }
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };
        value.parse().map(Some).map_err(D::Error::custom)
    }
}

pub mod optional_exact_u64 {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        if raw.get() == "null" {
            return Ok(None);
        }
        let value = if raw.get().starts_with('"') {
            serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?
        } else {
            raw.get().to_string()
        };
        value.parse().map(Some).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde::Deserialize;

    use super::*;

    #[rstest]
    fn preserves_exact_json_number_lexeme() {
        #[derive(Deserialize)]
        struct ExactPrice {
            #[serde(deserialize_with = "exact_decimal::deserialize")]
            price: Decimal,
        }

        let value: ExactPrice = serde_json::from_str(r#"{"price":2453.980000000000001}"#).unwrap();

        assert_eq!(
            value.price,
            "2453.980000000000001".parse::<Decimal>().unwrap()
        );
    }

    #[rstest]
    #[case("1.2867e1", "12.867")]
    #[case("1.2127547341372958e-6", "0.0000012127547341372958")]
    #[case("1e-28", "0.0000000000000000000000000001")]
    #[case("1.00e-28", "0.0000000000000000000000000001")]
    #[case("7.9228162514264337593543950335e28", "79228162514264337593543950335")]
    fn parses_exact_scientific_decimal(#[case] raw: &str, #[case] expected: &str) {
        assert_eq!(
            parse_decimal(raw).unwrap(),
            Decimal::from_str_exact(expected).unwrap()
        );
    }

    #[rstest]
    #[case("12.867999999999999999999999999999")]
    #[case("1.2867999999999999999999999999999e1")]
    #[case("1e-29")]
    #[case("1e29")]
    #[case("1e2147483647")]
    #[case("1e-2147483648")]
    fn rejects_inexact_or_out_of_range_decimal(#[case] raw: &str) {
        assert!(parse_decimal(raw).is_err());
    }
}
