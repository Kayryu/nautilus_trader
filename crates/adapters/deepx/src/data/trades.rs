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

use crate::http::DeepXPerpTrade;

pub(super) fn parse_trade_tick(
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
