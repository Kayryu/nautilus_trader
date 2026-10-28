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

//! Strict conversion of public perpetual execution records, not position fill directions.

use anyhow::{Context, Result, ensure};
use nautilus_core::{UnixNanos, datetime::try_datetime_to_unix_nanos};
use nautilus_model::{
    data::TradeTick,
    enums::AggressorSide,
    identifiers::TradeId,
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

use crate::http::{DeepXPerpTrade, DeepXSpotMarket, DeepXSpotTrade};

pub(crate) fn parse_trade_tick(
    trade: &DeepXPerpTrade,
    instrument: &InstrumentAny,
    market_id: u64,
    ts_init: UnixNanos,
) -> Result<TradeTick> {
    ensure!(
        trade.market_id == market_id,
        "DeepX trade market ID mismatch"
    );
    ensure!(trade.id != 0, "DeepX trade ID must be nonzero");
    ensure!(
        trade.price > Decimal::ZERO,
        "DeepX execution price must be positive"
    );
    ensure!(
        trade.size > Decimal::ZERO,
        "DeepX execution size must be positive"
    );
    let price = Price::from_decimal_dp(trade.price, instrument.price_precision())
        .context("DeepX execution price cannot be represented")?;
    let size = Quantity::from_decimal_dp(trade.size, instrument.size_precision())
        .context("DeepX execution size cannot be represented")?;
    ensure!(
        price.as_decimal() == trade.price,
        "DeepX execution price would lose precision"
    );
    ensure!(
        size.as_decimal() == trade.size,
        "DeepX execution size would lose precision"
    );
    let aggressor = match trade.taker.as_str() {
        "Buyer" => AggressorSide::Buy,
        "Seller" => AggressorSide::Sell,
        _ => anyhow::bail!("unknown DeepX trade taker: {}", trade.taker),
    };
    let timestamp: jiff::Timestamp = trade
        .created_at
        .parse()
        .context("invalid DeepX execution timestamp")?;
    ensure!(
        jiff::Timestamp::from_millisecond(timestamp.as_millisecond())? == timestamp,
        "DeepX execution timestamp is not millisecond aligned"
    );
    let ts_event = try_datetime_to_unix_nanos(timestamp)?;
    TradeTick::new_checked(
        instrument.id(),
        price,
        size,
        aggressor,
        TradeId::new(trade.id.to_string()),
        ts_event,
        ts_init,
    )
}

pub(crate) fn parse_spot_trade_tick(
    trade: &DeepXSpotTrade,
    market: &DeepXSpotMarket,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> Result<TradeTick> {
    ensure!(trade.id != 0, "DeepX Spot trade ID must be nonzero");
    ensure!(trade.height != 0, "DeepX Spot trade height must be nonzero");
    ensure!(
        trade.pair_name == market.name,
        "DeepX Spot trade name mismatch"
    );
    let expected_pair = nautilus_core::hex::decode_array::<32>(
        market.pair.strip_prefix("0x").unwrap_or(&market.pair),
    )
    .context("invalid DeepX Spot market pair")?;
    let observed_pair = nautilus_core::hex::decode_array::<32>(
        trade.pair.strip_prefix("0x").unwrap_or(&trade.pair),
    )
    .context("invalid DeepX Spot trade pair")?;
    ensure!(
        expected_pair == observed_pair,
        "DeepX Spot trade pair mismatch"
    );
    ensure!(
        trade.price > Decimal::ZERO && trade.base_amount > Decimal::ZERO,
        "DeepX Spot trade price and amount must be positive"
    );
    let price = Price::from_decimal_dp(trade.price, instrument.price_precision())
        .context("DeepX Spot trade price cannot be represented")?;
    let exact_amount = trade.base_amount.normalize();
    ensure!(
        exact_amount.scale() <= u32::from(market.base_decimal),
        "DeepX Spot trade amount exceeds base token precision"
    );
    let size = Quantity::from_decimal(exact_amount)
        .context("DeepX Spot trade amount cannot be represented")?;
    ensure!(
        price.as_decimal() == trade.price,
        "DeepX Spot trade price would lose precision"
    );
    ensure!(
        size.as_decimal() == trade.base_amount,
        "DeepX Spot trade amount would lose precision"
    );
    ensure!(
        price.as_decimal() % instrument.price_increment().as_decimal() == Decimal::ZERO,
        "DeepX Spot trade price is not a multiple of the instrument tick"
    );
    let aggressor = match trade.taker.as_str() {
        "Buyer" => AggressorSide::Buy,
        "Seller" => AggressorSide::Sell,
        _ => anyhow::bail!("unknown DeepX Spot trade taker: {}", trade.taker),
    };
    let timestamp: jiff::Timestamp = trade
        .trade_time
        .parse()
        .context("invalid DeepX Spot trade timestamp")?;
    ensure!(
        jiff::Timestamp::from_millisecond(timestamp.as_millisecond())? == timestamp,
        "DeepX Spot trade timestamp is not millisecond aligned"
    );
    let ts_event = try_datetime_to_unix_nanos(timestamp)?;
    TradeTick::new_checked(
        instrument.id(),
        price,
        size,
        aggressor,
        TradeId::new(trade.id.to_string()),
        ts_event,
        ts_init,
    )
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::CurrencyType,
        identifiers::{InstrumentId, Symbol},
        instruments::{CurrencyPair, InstrumentAny},
        types::{Currency, Price, Quantity},
    };

    use super::*;
    use crate::http::{DeepXApiResponse, DeepXSpotMarket};

    const SPOT_MARKETS: &str =
        include_str!("../../deepx-rust-sdk/test_data/http/testnet/spot_markets.json");
    const SPOT_TRADES: &str = include_str!(
        "../../deepx-rust-sdk/test_data/http/testnet/spot_trades_eth_usdc_page_1.json"
    );

    fn spot_instrument() -> InstrumentAny {
        let base = Currency::new_checked("ETH", 18, 0, "ETH", CurrencyType::Crypto).unwrap();
        let quote = Currency::new_checked("USDC", 6, 0, "USDC", CurrencyType::Crypto).unwrap();
        InstrumentAny::CurrencyPair(
            CurrencyPair::builder()
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

    fn fixture() -> (DeepXSpotMarket, DeepXSpotTrade) {
        let markets: DeepXApiResponse<Vec<DeepXSpotMarket>> =
            serde_json::from_str(SPOT_MARKETS).unwrap();
        let trades: DeepXApiResponse<crate::http::DeepXSpotTradesPage> =
            serde_json::from_str(SPOT_TRADES).unwrap();
        (
            markets
                .data
                .into_iter()
                .find(|market| market.name == "ETH/USDC")
                .unwrap(),
            trades.data.items.into_iter().next().unwrap(),
        )
    }

    #[rstest::rstest]
    fn converts_spot_taker_and_exact_values_to_trade_tick() {
        let (market, trade) = fixture();

        let tick = parse_spot_trade_tick(&trade, &market, &spot_instrument(), UnixNanos::from(10))
            .unwrap();

        assert_eq!(tick.price.as_decimal(), trade.price);
        assert_eq!(tick.size.as_decimal(), trade.base_amount);
        assert_eq!(tick.aggressor_side, AggressorSide::Sell);
        assert_eq!(tick.trade_id.to_string(), trade.id.to_string());
        let timestamp: jiff::Timestamp = trade.trade_time.parse().unwrap();
        assert_eq!(tick.ts_event.as_millis(), timestamp.as_millisecond() as u64);
    }

    #[rstest::rstest]
    fn rejects_unknown_spot_taker() {
        let (market, mut trade) = fixture();
        trade.taker = "Unknown".to_string();

        let error =
            parse_spot_trade_tick(&trade, &market, &spot_instrument(), UnixNanos::default())
                .unwrap_err();

        assert!(error.to_string().contains("unknown DeepX Spot trade taker"));
    }

    #[rstest::rstest]
    fn preserves_spot_fill_below_order_step() {
        let (market, mut trade) = fixture();
        trade.base_amount = Decimal::from_str_exact("0.00001184918359125").unwrap();

        let tick = parse_spot_trade_tick(&trade, &market, &spot_instrument(), UnixNanos::default())
            .unwrap();

        assert_eq!(tick.size.as_decimal(), trade.base_amount);
        assert_eq!(tick.size.precision, 17);
    }

    #[rstest::rstest]
    fn rejects_spot_fill_beyond_base_token_precision() {
        let (market, mut trade) = fixture();
        trade.base_amount = Decimal::from_str_exact("0.0000000000000000001").unwrap();

        let error =
            parse_spot_trade_tick(&trade, &market, &spot_instrument(), UnixNanos::default())
                .unwrap_err();

        assert!(format!("{error:#}").contains("exceeds base token precision"));
    }
}
