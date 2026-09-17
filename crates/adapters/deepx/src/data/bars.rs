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
//  See the GNU Lesser General Public License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Exact conversion of venue candle history without inferring bucket-close semantics.

use anyhow::{Context, Result, ensure};
use nautilus_core::{UnixNanos, datetime::NANOSECONDS_IN_MILLISECOND};
use nautilus_model::{
    data::{Bar, BarSpecification, BarType},
    enums::{BarAggregation, PriceType},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

use crate::http::{DeepXPerpCandle, DeepXPerpCandleInterval};

pub(super) fn candle_interval(spec: BarSpecification) -> Result<DeepXPerpCandleInterval> {
    ensure!(
        spec.price_type == PriceType::Last,
        "DeepX candle history only supports last-price bars"
    );
    match (spec.step.get(), spec.aggregation) {
        (1, BarAggregation::Minute) => Ok(DeepXPerpCandleInterval::OneMinute),
        (3, BarAggregation::Minute) => Ok(DeepXPerpCandleInterval::ThreeMinutes),
        (5, BarAggregation::Minute) => Ok(DeepXPerpCandleInterval::FiveMinutes),
        (15, BarAggregation::Minute) => Ok(DeepXPerpCandleInterval::FifteenMinutes),
        (30, BarAggregation::Minute) => Ok(DeepXPerpCandleInterval::ThirtyMinutes),
        (1, BarAggregation::Hour) => Ok(DeepXPerpCandleInterval::OneHour),
        (2, BarAggregation::Hour) => Ok(DeepXPerpCandleInterval::TwoHours),
        (4, BarAggregation::Hour) => Ok(DeepXPerpCandleInterval::FourHours),
        (8, BarAggregation::Hour) => Ok(DeepXPerpCandleInterval::EightHours),
        (12, BarAggregation::Hour) => Ok(DeepXPerpCandleInterval::TwelveHours),
        (1, BarAggregation::Day) => Ok(DeepXPerpCandleInterval::OneDay),
        (3, BarAggregation::Day) => Ok(DeepXPerpCandleInterval::ThreeDays),
        (1, BarAggregation::Week) => Ok(DeepXPerpCandleInterval::OneWeek),
        (1, BarAggregation::Month) => Ok(DeepXPerpCandleInterval::OneMonth),
        _ => anyhow::bail!("DeepX candle interval is unsupported: {spec}"),
    }
}

pub(super) fn parse_bar(
    candle: &DeepXPerpCandle,
    bar_type: BarType,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> Result<Bar> {
    ensure!(
        candle.open > Decimal::ZERO,
        "DeepX candle open must be positive"
    );
    ensure!(
        candle.high > Decimal::ZERO,
        "DeepX candle high must be positive"
    );
    ensure!(
        candle.low > Decimal::ZERO,
        "DeepX candle low must be positive"
    );
    ensure!(
        candle.close > Decimal::ZERO,
        "DeepX candle close must be positive"
    );

    let price_precision = instrument.price_precision();
    let open = exact_price(candle.open, price_precision, "open")?;
    let high = exact_price(candle.high, price_precision, "high")?;
    let low = exact_price(candle.low, price_precision, "low")?;
    let close = exact_price(candle.close, price_precision, "close")?;
    let volume = Quantity::from_decimal_dp(candle.volume, instrument.size_precision())
        .context("DeepX candle volume cannot be represented")?;
    ensure!(
        volume.as_decimal() == candle.volume,
        "DeepX candle volume would lose precision"
    );
    let timestamp_ns = candle
        .time
        .checked_mul(NANOSECONDS_IN_MILLISECOND)
        .ok_or_else(|| anyhow::anyhow!("DeepX candle timestamp overflows UnixNanos"))?;
    Bar::new_checked(
        bar_type,
        open,
        high,
        low,
        close,
        volume,
        UnixNanos::from(timestamp_ns),
        ts_init,
    )
}

fn exact_price(value: Decimal, precision: u8, field: &str) -> Result<Price> {
    let price = Price::from_decimal_dp(value, precision)
        .with_context(|| format!("DeepX candle {field} cannot be represented"))?;
    ensure!(
        price.as_decimal() == value,
        "DeepX candle {field} would lose precision"
    );
    Ok(price)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(1, BarAggregation::Minute, DeepXPerpCandleInterval::OneMinute)]
    #[case(3, BarAggregation::Minute, DeepXPerpCandleInterval::ThreeMinutes)]
    #[case(5, BarAggregation::Minute, DeepXPerpCandleInterval::FiveMinutes)]
    #[case(15, BarAggregation::Minute, DeepXPerpCandleInterval::FifteenMinutes)]
    #[case(30, BarAggregation::Minute, DeepXPerpCandleInterval::ThirtyMinutes)]
    #[case(1, BarAggregation::Hour, DeepXPerpCandleInterval::OneHour)]
    #[case(2, BarAggregation::Hour, DeepXPerpCandleInterval::TwoHours)]
    #[case(4, BarAggregation::Hour, DeepXPerpCandleInterval::FourHours)]
    #[case(8, BarAggregation::Hour, DeepXPerpCandleInterval::EightHours)]
    #[case(12, BarAggregation::Hour, DeepXPerpCandleInterval::TwelveHours)]
    #[case(1, BarAggregation::Day, DeepXPerpCandleInterval::OneDay)]
    #[case(3, BarAggregation::Day, DeepXPerpCandleInterval::ThreeDays)]
    #[case(1, BarAggregation::Week, DeepXPerpCandleInterval::OneWeek)]
    #[case(1, BarAggregation::Month, DeepXPerpCandleInterval::OneMonth)]
    fn maps_only_documented_candle_intervals(
        #[case] step: usize,
        #[case] aggregation: BarAggregation,
        #[case] expected: DeepXPerpCandleInterval,
    ) {
        let spec = BarSpecification::new(step, aggregation, PriceType::Last);
        assert_eq!(candle_interval(spec).unwrap(), expected);
    }

    #[rstest]
    #[case(2, BarAggregation::Minute, PriceType::Last)]
    #[case(1, BarAggregation::Second, PriceType::Last)]
    #[case(1, BarAggregation::Minute, PriceType::Bid)]
    fn rejects_unsupported_candle_specifications(
        #[case] step: usize,
        #[case] aggregation: BarAggregation,
        #[case] price_type: PriceType,
    ) {
        let spec = BarSpecification::new(step, aggregation, price_type);
        assert!(candle_interval(spec).is_err());
    }
}
