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
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use serde_json::Value;

use crate::{
    common::{DeepXProductType, format_instrument_id},
    http::DeepXPerpMarket,
};

/// Converts verified perpetual market metadata into a Nautilus instrument.
///
/// The deployment market ID and protocol addresses remain available in the instrument `info`.
/// Spot conversion remains unsupported because the market-list response does not expose a verified
/// order quantity increment.
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

    const PERP_RESPONSE: &str = include_str!("../test_data/http/testnet/perp_markets.json");

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
}
