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

//! Conversion of verified DeepX market metadata into Nautilus instruments.

use anyhow::{Context, Result};
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    identifiers::Symbol,
    instruments::{CryptoPerpetual, CurrencyPair, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use serde_json::Value;

use crate::{
    common::{DeepXProductType, format_instrument_id},
    http::{DeepXPerpMarket, DeepXSpotMarket},
    rpc::{DeepXCurrentSpotMarketSpec, DeepXFinalizedSpotMarketSpecEvidence},
};

/// Converts verified perpetual market metadata into a Nautilus instrument.
///
/// The deployment market ID and protocol addresses remain available in the instrument `info`.
///
/// # Errors
///
/// Returns an error when an identity, increment, quantity, notional, or instrument invariant is
/// invalid.
pub fn parse_perpetual_instrument(
    market: &DeepXPerpMarket,
    ts_init: UnixNanos,
) -> Result<InstrumentAny> {
    let pair = format!("{}-{}", market.base_symbol, market.quote_symbol);
    let instrument_id = format_instrument_id(&pair, &DeepXProductType::Perpetual)?;
    let raw_symbol = Symbol::new(&market.name);
    let base_currency = Currency::get_or_create_crypto(market.base_symbol.to_ascii_uppercase());
    let quote_currency = Currency::get_or_create_crypto(market.quote_symbol.to_ascii_uppercase());
    let price_increment = Price::from_decimal(market.order_spec_tick_size)
        .context("invalid DeepX order_spec_tick_size")?;
    let size_increment = Quantity::from_decimal(market.order_spec_step_size)
        .context("invalid DeepX order_spec_step_size")?;
    let min_quantity = Quantity::from_decimal(market.order_spec_min_qty)
        .context("invalid DeepX order_spec_min_qty")?;
    let min_notional = Money::from_decimal(market.order_spec_min_notional, quote_currency)
        .context("invalid DeepX order_spec_min_notional")?;

    let instrument = CryptoPerpetual::builder()
        .instrument_id(instrument_id)
        .raw_symbol(raw_symbol)
        .base_currency(base_currency)
        .quote_currency(quote_currency)
        .settlement_currency(quote_currency)
        .is_inverse(false)
        .price_precision(price_increment.precision)
        .size_precision(size_increment.precision)
        .price_increment(price_increment)
        .size_increment(size_increment)
        .multiplier(Quantity::from(1))
        .lot_size(size_increment)
        .min_quantity(min_quantity)
        .min_notional(min_notional)
        .margin_init(market.initial_margin_ratio)
        .margin_maint(market.maintenance_margin_ratio)
        .maker_fee(market.maker_fee_rate)
        .taker_fee(market.taker_fee_rate)
        .info(perpetual_info(market))
        .ts_event(ts_init)
        .ts_init(ts_init)
        .build()
        .context("invalid DeepX perpetual instrument")?;

    Ok(InstrumentAny::CryptoPerpetual(instrument))
}

/// Converts Spot market metadata using exact constraints from a finalized runtime checkpoint.
///
/// The runtime stores tick sizes at `1e6` price scale and quantity constraints in base-token
/// atomic units. The REST tick is required to match the finalized runtime tick exactly.
///
/// # Errors
///
/// Returns an error for a malformed identity, mismatched pair or tick, unsupported currency
/// precision, financial values outside the model decimal domain, or an invalid instrument.
pub fn parse_spot_instrument(
    market: &DeepXSpotMarket,
    spec: &DeepXFinalizedSpotMarketSpecEvidence,
    ts_init: UnixNanos,
) -> Result<InstrumentAny> {
    let pair = nautilus_core::hex::decode_array::<32>(
        market.pair.strip_prefix("0x").unwrap_or(&market.pair),
    )
    .context("invalid DeepX Spot pair identity")?;
    anyhow::ensure!(
        pair == spec.pair(),
        "DeepX Spot market pair differs from finalized spec"
    );
    parse_spot_instrument_with_spec(
        market,
        spec.min_order_size(),
        spec.tick_size(),
        spec.step_size(),
        ts_init,
    )
}

pub(crate) fn parse_current_spot_instrument(
    market: &DeepXSpotMarket,
    spec: &DeepXCurrentSpotMarketSpec,
    ts_init: UnixNanos,
) -> Result<InstrumentAny> {
    let pair = nautilus_core::hex::decode_array::<32>(
        market.pair.strip_prefix("0x").unwrap_or(&market.pair),
    )
    .context("invalid DeepX Spot pair identity")?;
    anyhow::ensure!(
        pair == spec.pair(),
        "DeepX Spot market pair differs from current spec"
    );
    parse_spot_instrument_with_spec(
        market,
        spec.min_order_size(),
        spec.tick_size(),
        spec.step_size(),
        ts_init,
    )
}

fn parse_spot_instrument_with_spec(
    market: &DeepXSpotMarket,
    min_order_size: u128,
    tick_size: u128,
    step_size: u128,
    ts_init: UnixNanos,
) -> Result<InstrumentAny> {
    anyhow::ensure!(
        market.base_decimal <= 28 && market.quote_decimal <= 28,
        "DeepX Spot currency precision exceeds the Decimal domain",
    );
    let mut name_parts = market.name.split('/');
    let name_base = name_parts.next().unwrap_or_default();
    let name_quote = name_parts.next().unwrap_or_default();
    anyhow::ensure!(
        !name_base.is_empty()
            && !name_quote.is_empty()
            && name_parts.next().is_none()
            && name_base.eq_ignore_ascii_case(&market.base_symbol)
            && name_quote.eq_ignore_ascii_case(&market.quote_symbol)
            && !market
                .base_symbol
                .eq_ignore_ascii_case(&market.quote_symbol),
        "DeepX Spot market name and currency symbols are inconsistent",
    );

    let price_increment = scaled_runtime_decimal(tick_size, 6)
        .context("invalid DeepX Spot tick size")?
        .normalize();
    anyhow::ensure!(
        price_increment == market.tick_size,
        "DeepX Spot REST tick differs from the runtime tick",
    );
    let size_increment =
        Quantity::from_decimal(scaled_runtime_decimal(step_size, market.base_decimal)?.normalize())
            .context("invalid DeepX Spot quantity step")?;
    let min_quantity = Quantity::from_decimal(
        scaled_runtime_decimal(min_order_size, market.base_decimal)?.normalize(),
    )
    .context("invalid DeepX Spot minimum quantity")?;
    let price_increment =
        Price::from_decimal(price_increment).context("invalid DeepX Spot price increment")?;
    let base_currency = Currency::new_checked(
        market.base_symbol.to_ascii_uppercase(),
        market.base_decimal,
        0,
        market.base_symbol.to_ascii_uppercase(),
        nautilus_model::enums::CurrencyType::Crypto,
    )
    .context("invalid DeepX Spot base currency")?;
    let quote_currency = Currency::new_checked(
        market.quote_symbol.to_ascii_uppercase(),
        market.quote_decimal,
        0,
        market.quote_symbol.to_ascii_uppercase(),
        nautilus_model::enums::CurrencyType::Crypto,
    )
    .context("invalid DeepX Spot quote currency")?;
    let pair_symbol = format!("{}-{}", market.base_symbol, market.quote_symbol);
    let instrument_id = format_instrument_id(&pair_symbol, &DeepXProductType::Spot)?;
    let instrument = CurrencyPair::builder()
        .instrument_id(instrument_id)
        .raw_symbol(Symbol::new(&market.name))
        .base_currency(base_currency)
        .quote_currency(quote_currency)
        .price_precision(price_increment.precision)
        .size_precision(size_increment.precision)
        .price_increment(price_increment)
        .size_increment(size_increment)
        .maybe_lot_size(Some(size_increment))
        .maybe_min_quantity(Some(min_quantity))
        .maker_fee(market.maker_fee_rate)
        .taker_fee(market.taker_fee_rate)
        .info(spot_info(market))
        .ts_event(ts_init)
        .ts_init(ts_init)
        .build()
        .context("invalid DeepX Spot instrument")?;

    Ok(InstrumentAny::CurrencyPair(instrument))
}

fn scaled_runtime_decimal(value: u128, scale: u8) -> Result<rust_decimal::Decimal> {
    let value = i128::try_from(value).context("runtime value exceeds Decimal integer range")?;
    Ok(rust_decimal::Decimal::from_i128_with_scale(
        value,
        u32::from(scale),
    ))
}

fn spot_info(market: &DeepXSpotMarket) -> Params {
    let mut info = Params::new();
    info.insert("pair".to_string(), Value::from(market.pair.clone()));
    info.insert(
        "baseAddress".to_string(),
        Value::from(market.base_address.clone()),
    );
    info.insert(
        "quoteAddress".to_string(),
        Value::from(market.quote_address.clone()),
    );
    info.insert("isPaused".to_string(), Value::from(market.is_paused));
    info
}

fn perpetual_info(market: &DeepXPerpMarket) -> Params {
    let mut info = Params::new();
    info.insert("marketId".to_string(), Value::from(market.id));
    info.insert("name".to_string(), Value::from(market.name.clone()));
    info.insert(
        "baseAddress".to_string(),
        Value::from(market.base_address.clone()),
    );
    info.insert(
        "quoteAddress".to_string(),
        Value::from(market.quote_address.clone()),
    );
    info.insert(
        "quoteMarketId".to_string(),
        Value::from(market.quote_market_id),
    );
    info
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;
    use crate::http::DeepXApiResponse;

    const PERP_RESPONSE: &str =
        include_str!("../deepx-rust-sdk/test_data/http/testnet/perp_markets.json");
    const SPOT_RESPONSE: &str =
        include_str!("../deepx-rust-sdk/test_data/http/testnet/spot_markets.json");

    #[rstest]
    fn parses_fixture_perpetual_without_losing_protocol_values() {
        let response: DeepXApiResponse<Vec<DeepXPerpMarket>> =
            serde_json::from_str(PERP_RESPONSE).unwrap();
        let market = response.data.into_iter().next().unwrap();
        let ts_init = UnixNanos::from(1_788_250_000_000_000_000);

        let instrument = parse_perpetual_instrument(&market, ts_init).unwrap();
        let InstrumentAny::CryptoPerpetual(perpetual) = instrument else {
            panic!("expected CryptoPerpetual");
        };

        assert_eq!(perpetual.id.to_string(), "ETH-USDC-PERP.DEEPX");
        assert_eq!(perpetual.raw_symbol.as_str(), "ETH-USDC");
        assert_eq!(perpetual.price_increment.as_decimal(), Decimal::new(1, 2));
        assert_eq!(perpetual.size_increment.as_decimal(), Decimal::new(1, 3));
        assert_eq!(
            perpetual.min_quantity.unwrap().as_decimal(),
            Decimal::new(1, 3)
        );
        assert_eq!(perpetual.min_notional.unwrap().as_decimal(), Decimal::ONE);
        assert_eq!(perpetual.margin_init, Decimal::new(4, 2));
        assert_eq!(perpetual.margin_maint, Decimal::new(2, 2));
        assert_eq!(perpetual.maker_fee, Decimal::new(-1, 4));
        assert_eq!(perpetual.taker_fee, Decimal::new(2, 4));
        assert_eq!(perpetual.ts_event, ts_init);
        assert_eq!(perpetual.ts_init, ts_init);
        assert_eq!(
            perpetual.info.as_ref().unwrap().get_u64("marketId"),
            Some(3)
        );
        assert_eq!(
            perpetual.info.as_ref().unwrap().get_str("baseAddress"),
            Some("0x123ae070eb84068b5fed9f5b99f236507c44c880"),
        );
    }

    #[rstest]
    fn rejects_non_positive_perpetual_step_size() {
        let response: DeepXApiResponse<Vec<DeepXPerpMarket>> =
            serde_json::from_str(PERP_RESPONSE).unwrap();
        let mut market = response.data.into_iter().next().unwrap();
        market.order_spec_step_size = Decimal::ZERO;

        let error = parse_perpetual_instrument(&market, UnixNanos::default()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("invalid DeepX perpetual instrument")
        );
    }

    #[rstest]
    fn parses_spot_instrument_from_runtime_scaled_constraints() {
        let response: DeepXApiResponse<Vec<DeepXSpotMarket>> =
            serde_json::from_str(SPOT_RESPONSE).unwrap();
        let market = response.data.into_iter().next().unwrap();

        let instrument = parse_spot_instrument_with_spec(
            &market,
            1_000_000_000_000_000,
            10_000,
            1_000_000_000_000_000,
            UnixNanos::from(1_788_250_000_000_000_000),
        )
        .unwrap();
        let InstrumentAny::CurrencyPair(pair) = instrument else {
            panic!("expected CurrencyPair");
        };

        assert_eq!(pair.id.to_string(), "ETH-USDC.DEEPX");
        assert_eq!(pair.raw_symbol.as_str(), "ETH/USDC");
        assert_eq!(pair.base_currency.precision, 18);
        assert_eq!(pair.quote_currency.precision, 6);
        assert_eq!(pair.price_precision, 2);
        assert_eq!(pair.size_precision, 3);
        assert_eq!(
            pair.price_increment.as_decimal(),
            rust_decimal::Decimal::new(1, 2)
        );
        assert_eq!(
            pair.size_increment.as_decimal(),
            rust_decimal::Decimal::new(1, 3)
        );
        assert_eq!(
            pair.min_quantity.unwrap().as_decimal(),
            rust_decimal::Decimal::new(1, 3),
        );
        assert_eq!(pair.maker_fee, rust_decimal::Decimal::new(1, 4));
        assert_eq!(pair.taker_fee, rust_decimal::Decimal::new(4, 4));
    }

    #[rstest]
    fn spot_instrument_fails_closed_for_unreconciled_constraints() {
        let response: DeepXApiResponse<Vec<DeepXSpotMarket>> =
            serde_json::from_str(SPOT_RESPONSE).unwrap();
        let market = response.data.into_iter().next().unwrap();

        let tick_mismatch =
            parse_spot_instrument_with_spec(&market, 1_000, 1, 1_000, UnixNanos::default())
                .unwrap_err();
        assert!(format!("{tick_mismatch:#}").contains("REST tick"));

        assert!(
            parse_spot_instrument_with_spec(&market, 1_000, 10_000, 0, UnixNanos::default())
                .is_err()
        );

        assert!(
            parse_spot_instrument_with_spec(
                &market,
                u128::MAX,
                10_000,
                1_000,
                UnixNanos::default(),
            )
            .is_err()
        );
    }
}
