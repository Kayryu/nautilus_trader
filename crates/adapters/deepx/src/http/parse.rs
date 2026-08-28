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

//! Parsers from DeepX REST payloads to Nautilus domain types.

use std::str::FromStr;

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::Symbol,
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};

use crate::{
    common::{enums::DeepXProductType, symbol::instrument_id_from_raw},
    http::models::DeepXPerpetualMarket,
};

/// Parses DeepX perpetual market metadata into a Nautilus instrument.
///
/// # Errors
///
/// Returns an error if the symbol, currencies, increments, or limits are invalid.
pub fn parse_perpetual_instrument(
    market: &DeepXPerpetualMarket,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    let instrument_id = instrument_id_from_raw(&market.symbol, DeepXProductType::Perpetual)?;
    let raw_symbol = Symbol::new_checked(&market.symbol).context("invalid DeepX market symbol")?;
    let base_currency =
        Currency::from_str(&market.base_asset).context("invalid DeepX base asset")?;
    let quote_currency =
        Currency::from_str(&market.quote_asset).context("invalid DeepX quote asset")?;
    let price_precision = decimal_precision(market.tick_size, "tick size")?;
    let size_precision = decimal_precision(market.step_size, "step size")?;
    let price_increment = Price::from_decimal_dp(market.tick_size, price_precision)
        .map_err(|e| anyhow::anyhow!("invalid DeepX tick size: {e}"))?;
    let size_increment = Quantity::from_decimal_dp(market.step_size, size_precision)
        .map_err(|e| anyhow::anyhow!("invalid DeepX step size: {e}"))?;
    let min_quantity = Quantity::from_decimal_dp(market.min_qty, size_precision)
        .map_err(|e| anyhow::anyhow!("invalid DeepX minimum quantity: {e}"))?;
    let min_notional = Money::from_decimal(market.min_notional, quote_currency)
        .map_err(|e| anyhow::anyhow!("invalid DeepX minimum notional: {e}"))?;

    let instrument = CryptoPerpetual::builder()
        .instrument_id(instrument_id)
        .raw_symbol(raw_symbol)
        .base_currency(base_currency)
        .quote_currency(quote_currency)
        .settlement_currency(quote_currency)
        .is_inverse(false)
        .price_precision(price_precision)
        .size_precision(size_precision)
        .price_increment(price_increment)
        .size_increment(size_increment)
        .min_quantity(min_quantity)
        .min_notional(min_notional)
        .maker_fee(market.maker_fee_rate)
        .taker_fee(market.taker_fee_rate)
        .ts_event(ts_init)
        .ts_init(ts_init)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to construct DeepX perpetual instrument: {e}"))?;

    Ok(InstrumentAny::CryptoPerpetual(instrument))
}

fn decimal_precision(value: rust_decimal::Decimal, field: &str) -> anyhow::Result<u8> {
    anyhow::ensure!(
        value > rust_decimal::Decimal::ZERO,
        "non-positive DeepX {field} `{value}`"
    );
    u8::try_from(value.normalize().scale())
        .with_context(|| format!("DeepX {field} precision exceeds u8"))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use nautilus_model::instruments::Instrument;
    use rstest::{fixture, rstest};
    use rust_decimal::Decimal;

    use super::*;

    #[fixture]
    fn market() -> DeepXPerpetualMarket {
        serde_json::from_str(
            r#"{
                "baseAsset":"ETH","makerFeeRate":"-0.0001","marketId":3,
                "maxOpenOrders":128,"minNotional":"1","minQty":"0.001",
                "orderTypes":["LIMIT","MARKET"],"quoteAsset":"USDC",
                "status":"TRADING","stepSize":"0.001","symbol":"ETH-USDC",
                "takerFeeRate":"0.0002","tickSize":"0.01"
            }"#,
        )
        .unwrap()
    }

    #[rstest]
    fn parses_perpetual_market(market: DeepXPerpetualMarket) {
        let ts_init = UnixNanos::from(1_787_561_622_211_000_000);
        let instrument = parse_perpetual_instrument(&market, ts_init).unwrap();

        assert_eq!(instrument.id().to_string(), "ETH-USDC-PERP.DEEPX");
        assert_eq!(instrument.raw_symbol().as_str(), "ETH-USDC");
        assert_eq!(instrument.base_currency().unwrap().code.as_str(), "ETH");
        assert_eq!(instrument.quote_currency().code.as_str(), "USDC");
        assert_eq!(instrument.settlement_currency().code.as_str(), "USDC");
        assert_eq!(instrument.price_precision(), 2);
        assert_eq!(instrument.size_precision(), 3);
        assert_eq!(instrument.price_increment().to_string(), "0.01");
        assert_eq!(instrument.size_increment().to_string(), "0.001");
        assert_eq!(instrument.min_quantity().unwrap().to_string(), "0.001");
        assert_eq!(
            instrument.min_notional().unwrap().as_decimal(),
            Decimal::ONE
        );
        assert_eq!(
            instrument.min_notional().unwrap().currency.code.as_str(),
            "USDC"
        );
        assert_eq!(
            instrument.maker_fee(),
            Decimal::from_str("-0.0001").unwrap()
        );
        assert_eq!(instrument.taker_fee(), Decimal::from_str("0.0002").unwrap());
        assert_eq!(instrument.ts_init(), ts_init);
    }

    #[rstest]
    fn rejects_non_positive_increment(mut market: DeepXPerpetualMarket) {
        market.tick_size = Decimal::ZERO;

        let result = parse_perpetual_instrument(&market, UnixNanos::default());

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("non-positive DeepX tick size")
        );
    }
}
