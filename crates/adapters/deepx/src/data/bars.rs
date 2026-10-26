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

use crate::http::{DeepXPerpCandle, DeepXPerpCandleInterval, DeepXSpotCandle};

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
    parse_bar_values(
        RawCandleValues {
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
            time: candle.time,
        },
        bar_type,
        instrument,
        ts_init,
    )
}

pub(super) fn parse_spot_bar(
    candle: &DeepXSpotCandle,
    bar_type: BarType,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> Result<Bar> {
    parse_bar_values(
        RawCandleValues {
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
            time: candle.time,
        },
        bar_type,
        instrument,
        ts_init,
    )
}

#[derive(Clone, Copy)]
struct RawCandleValues {
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    time: u64,
}

fn parse_bar_values(
    values: RawCandleValues,
    bar_type: BarType,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> Result<Bar> {
    let RawCandleValues {
        open,
        high,
        low,
        close,
        volume: raw_volume,
        time,
    } = values;
    ensure!(open > Decimal::ZERO, "DeepX candle open must be positive");
    ensure!(high > Decimal::ZERO, "DeepX candle high must be positive");
    ensure!(low > Decimal::ZERO, "DeepX candle low must be positive");
    ensure!(close > Decimal::ZERO, "DeepX candle close must be positive");

    let price_precision = instrument.price_precision();
    let open = exact_price(open, price_precision, "open")?;
    let high = exact_price(high, price_precision, "high")?;
    let low = exact_price(low, price_precision, "low")?;
    let close = exact_price(close, price_precision, "close")?;
    let volume = Quantity::from_decimal_dp(raw_volume, instrument.size_precision())
        .context("DeepX candle volume cannot be represented")?;
    ensure!(
        volume.as_decimal() == raw_volume,
        "DeepX candle volume would lose precision"
    );
    ensure!(
        volume.as_decimal() % instrument.size_increment().as_decimal() == Decimal::ZERO,
        "DeepX candle volume is not a multiple of the instrument step"
    );
    for (field, price) in [
        ("open", open),
        ("high", high),
        ("low", low),
        ("close", close),
    ] {
        ensure!(
            price.as_decimal() % instrument.price_increment().as_decimal() == Decimal::ZERO,
            "DeepX candle {field} is not a multiple of the instrument tick"
        );
    }
    let timestamp_ns = time
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
        (price.as_decimal() - value).abs() <= Decimal::new(1, 12),
        "DeepX candle {field} would lose precision"
    );
    Ok(price)
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{AggregationSource, CurrencyType},
        identifiers::{InstrumentId, Symbol},
        types::Currency,
    };
    use rstest::rstest;

    use super::*;

    const SPOT_CANDLES: &str =
        include_str!("../../test_data/http/testnet/spot_candles_eth_usdc_1m.json");

    fn spot_instrument() -> InstrumentAny {
        let base = Currency::new_checked("ETH", 18, 0, "ETH", CurrencyType::Crypto).unwrap();
        let quote = Currency::new_checked("USDC", 6, 0, "USDC", CurrencyType::Crypto).unwrap();
        InstrumentAny::CurrencyPair(
            nautilus_model::instruments::CurrencyPair::builder()
                .instrument_id(InstrumentId::from("ETH-USDC.DEEPX"))
                .raw_symbol(Symbol::new("ETH/USDC"))
                .base_currency(base)
                .quote_currency(quote)
                .price_precision(2)
                .size_precision(4)
                .price_increment(Price::from("0.01"))
                .size_increment(Quantity::from("0.0001"))
                .multiplier(Quantity::from(1))
                .ts_event(UnixNanos::default())
                .ts_init(UnixNanos::default())
                .build()
                .unwrap(),
        )
    }

    fn spot_bar_type() -> BarType {
        BarType::new(
            InstrumentId::from("ETH-USDC.DEEPX"),
            BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
            AggregationSource::External,
        )
    }

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

    #[rstest::rstest]
    fn parses_spot_candle_at_verified_instrument_precision() {
        let instrument = spot_instrument();
        let bar_type = spot_bar_type();
        let candle = DeepXSpotCandle {
            volume: Decimal::new(12_345, 4),
            high: Decimal::new(243_555, 2),
            low: Decimal::new(243_486, 2),
            open: Decimal::new(243_554, 2),
            close: Decimal::new(243_486, 2),
            time: 1_789_616_280_000,
        };

        let bar = parse_spot_bar(&candle, bar_type, &instrument, UnixNanos::from(10)).unwrap();

        assert_eq!(bar.open.as_decimal(), candle.open);
        assert_eq!(bar.close.as_decimal(), candle.close);
        assert_eq!(bar.volume.as_decimal(), candle.volume);
        assert_eq!(bar.ts_event.as_millis(), candle.time);
    }

    #[rstest::rstest]
    fn rejects_spot_candle_volume_precision_loss() {
        let instrument = spot_instrument();
        let bar_type = spot_bar_type();
        let candle = DeepXSpotCandle {
            volume: Decimal::new(123_456, 5),
            high: Decimal::new(243_555, 2),
            low: Decimal::new(243_486, 2),
            open: Decimal::new(243_554, 2),
            close: Decimal::new(243_486, 2),
            time: 1_789_616_280_000,
        };

        assert!(parse_spot_bar(&candle, bar_type, &instrument, UnixNanos::default()).is_err());
    }

    #[rstest::rstest]
    fn rejects_captured_spot_candle_float_artifacts() {
        let response: crate::http::DeepXApiResponse<crate::http::DeepXSpotCandlesPage> =
            serde_json::from_str(SPOT_CANDLES).unwrap();
        let candle = response.data.details.first().unwrap();

        let error = parse_spot_bar(
            candle,
            spot_bar_type(),
            &spot_instrument(),
            UnixNanos::default(),
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("volume would lose precision"));
    }

    #[rstest::rstest]
    fn rejects_spot_candle_values_off_instrument_grid() {
        let mut instrument = spot_instrument();
        let InstrumentAny::CurrencyPair(pair) = &mut instrument else {
            unreachable!();
        };
        pair.size_increment = Quantity::from("0.0003");
        let bar_type = spot_bar_type();
        let candle = DeepXSpotCandle {
            volume: Decimal::new(12_346, 4),
            high: Decimal::new(243_555, 2),
            low: Decimal::new(243_486, 2),
            open: Decimal::new(243_554, 2),
            close: Decimal::new(243_486, 2),
            time: 1_789_616_280_000,
        };

        let error =
            parse_spot_bar(&candle, bar_type, &instrument, UnixNanos::default()).unwrap_err();

        assert!(format!("{error:#}").contains("not a multiple of the instrument step"));
    }

    #[rstest::rstest]
    fn rejects_spot_candle_prices_off_instrument_grid() {
        let mut instrument = spot_instrument();
        let InstrumentAny::CurrencyPair(pair) = &mut instrument else {
            unreachable!();
        };
        pair.price_increment = Price::from("0.05");
        let candle = DeepXSpotCandle {
            volume: Decimal::new(12_345, 4),
            high: Decimal::new(243_555, 2),
            low: Decimal::new(243_486, 2),
            open: Decimal::new(243_554, 2),
            close: Decimal::new(243_486, 2),
            time: 1_789_616_280_000,
        };

        let error = parse_spot_bar(&candle, spot_bar_type(), &instrument, UnixNanos::default())
            .unwrap_err();

        assert!(format!("{error:#}").contains("not a multiple of the instrument tick"));
    }

    #[rstest::rstest]
    fn normalizes_candle_price_float_artifact() {
        let value = Decimal::from_str_exact("2481.3799999999997").unwrap();

        let price = exact_price(value, 2, "close").unwrap();

        assert_eq!(
            price.as_decimal(),
            Decimal::from_str_exact("2481.38").unwrap()
        );
    }

    #[rstest::rstest]
    fn rejects_candle_price_beyond_float_artifact_tolerance() {
        let value = Decimal::from_str_exact("2481.379999999").unwrap();

        let error = exact_price(value, 2, "close").unwrap_err();

        assert!(format!("{error:#}").contains("close would lose precision"));
    }
}
