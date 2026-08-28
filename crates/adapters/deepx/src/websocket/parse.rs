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

//! Parsers from DeepX WebSocket payloads to Nautilus domain types.

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BookOrder, OrderBookDelta, OrderBookDeltas, TradeTick},
    enums::{AggressorSide, BookAction, OrderSide, RecordFlag},
    identifiers::TradeId,
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};

use crate::{
    common::models::DeepXOrderBookLevel,
    websocket::{
        enums::{DeepXBookUpdateType, DeepXTakerSide},
        messages::{DeepXOrderBookUpdate, DeepXTrade},
    },
};

/// Parses a DeepX public trade into a Nautilus [`TradeTick`].
///
/// # Errors
///
/// Returns an error if the price, quantity, identifier, or timestamp is invalid.
pub fn parse_trade_tick(
    trade: &DeepXTrade,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let price = Price::from_decimal_dp(trade.price, instrument.price_precision())
        .map_err(|e| anyhow::anyhow!("invalid DeepX trade price: {e}"))?;
    let size = Quantity::from_decimal_dp(trade.qty, instrument.size_precision())
        .map_err(|e| anyhow::anyhow!("invalid DeepX trade quantity: {e}"))?;
    let aggressor_side = match trade.taker_side {
        DeepXTakerSide::Buy => AggressorSide::Buy,
        DeepXTakerSide::Sell => AggressorSide::Sell,
    };
    let trade_id = TradeId::new_checked(&trade.id).context("invalid DeepX trade identifier")?;
    let ts_event = millis_to_nanos(trade.time)?;

    TradeTick::new_checked(
        instrument.id(),
        price,
        size,
        aggressor_side,
        trade_id,
        ts_event,
        ts_init,
    )
    .context("failed to construct TradeTick from DeepX trade")
}

/// Parses a DeepX order book message into Nautilus deltas.
///
/// Snapshot messages begin with a clear action. Delta quantities are absolute, and a zero quantity
/// removes the corresponding level.
///
/// # Errors
///
/// Returns an error if a level, timestamp, or resulting delta batch is invalid.
pub fn parse_order_book_deltas(
    update: &DeepXOrderBookUpdate,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    let is_snapshot = update.update_type == DeepXBookUpdateType::Snapshot;
    let total_levels = update.bids.len() + update.asks.len();
    anyhow::ensure!(
        is_snapshot || total_levels > 0,
        "empty DeepX order book delta"
    );

    let ts_event = millis_to_nanos(update.engine_time)?;
    let mut deltas = Vec::with_capacity(total_levels + usize::from(is_snapshot));

    if is_snapshot {
        let mut clear =
            OrderBookDelta::clear(instrument.id(), update.last_update_id, ts_event, ts_init);
        if total_levels == 0 {
            clear.flags |= RecordFlag::F_LAST as u8;
        }
        deltas.push(clear);
    }

    let mut processed = 0_usize;
    for level in &update.bids {
        processed += 1;
        deltas.push(parse_level(
            level,
            OrderSide::Buy,
            instrument,
            update.last_update_id,
            ts_event,
            ts_init,
            book_flags(is_snapshot, processed, total_levels),
        )?);
    }
    for level in &update.asks {
        processed += 1;
        deltas.push(parse_level(
            level,
            OrderSide::Sell,
            instrument,
            update.last_update_id,
            ts_event,
            ts_init,
            book_flags(is_snapshot, processed, total_levels),
        )?);
    }

    OrderBookDeltas::new_checked(instrument.id(), deltas)
        .context("failed to construct OrderBookDeltas from DeepX update")
}

#[allow(clippy::too_many_arguments)]
fn parse_level(
    level: &DeepXOrderBookLevel,
    side: OrderSide,
    instrument: &InstrumentAny,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
    flags: u8,
) -> anyhow::Result<OrderBookDelta> {
    let price = Price::from_decimal_dp(level.0, instrument.price_precision())
        .map_err(|e| anyhow::anyhow!("invalid DeepX book price: {e}"))?;
    let size = Quantity::from_decimal_dp(level.1, instrument.size_precision())
        .map_err(|e| anyhow::anyhow!("invalid DeepX book quantity: {e}"))?;
    let action = if flags & RecordFlag::F_SNAPSHOT as u8 != 0 {
        BookAction::Add
    } else if size.is_zero() {
        BookAction::Delete
    } else {
        BookAction::Update
    };

    OrderBookDelta::new_checked(
        instrument.id(),
        action,
        BookOrder::new(side, price, size, 0),
        flags,
        sequence,
        ts_event,
        ts_init,
    )
    .context("failed to construct DeepX order book delta")
}

fn book_flags(is_snapshot: bool, processed: usize, total_levels: usize) -> u8 {
    let mut flags = if is_snapshot {
        RecordFlag::F_SNAPSHOT as u8
    } else {
        0
    };
    if processed == total_levels {
        flags |= RecordFlag::F_LAST as u8;
    }
    flags
}

fn millis_to_nanos(timestamp_ms: u64) -> anyhow::Result<UnixNanos> {
    timestamp_ms
        .checked_mul(1_000_000)
        .map(UnixNanos::from)
        .context("DeepX millisecond timestamp overflows nanoseconds")
}

#[cfg(test)]
mod tests {
    use nautilus_model::enums::BookAction;
    use rstest::{fixture, rstest};

    use super::*;
    use crate::{
        http::{models::DeepXPerpetualMarket, parse::parse_perpetual_instrument},
        websocket::messages::DeepXWsMessage,
    };

    #[fixture]
    fn instrument() -> InstrumentAny {
        let market: DeepXPerpetualMarket = serde_json::from_str(
            r#"{"baseAsset":"ETH","makerFeeRate":"-0.0001","marketId":3,"maxOpenOrders":128,"minNotional":"1","minQty":"0.001","orderTypes":["LIMIT","MARKET"],"quoteAsset":"USDC","status":"TRADING","stepSize":"0.001","symbol":"ETH-USDC","takerFeeRate":"0.0002","tickSize":"0.01"}"#,
        )
        .unwrap();
        parse_perpetual_instrument(&market, UnixNanos::default()).unwrap()
    }

    #[rstest]
    fn parses_public_trade(instrument: InstrumentAny) {
        let message: DeepXWsMessage<DeepXTrade> = serde_json::from_str(
            r#"{"channel":"perp@trades","data":{"buyOrderId":"6652970","id":"159078803000024","makerFee":"-0.02529","marketId":3,"price":"2455.7","qty":"0.103","quoteQty":"252.9371","sellOrderId":"2263022","symbol":"ETH-USDC","takerFee":"0.050581","takerSide":"SELL","time":1787562119833}}"#,
        )
        .unwrap();

        let tick = parse_trade_tick(&message.data, &instrument, UnixNanos::from(42)).unwrap();

        assert_eq!(tick.price.to_string(), "2455.70");
        assert_eq!(tick.size.to_string(), "0.103");
        assert_eq!(tick.aggressor_side, AggressorSide::Sell);
        assert_eq!(tick.trade_id.as_str(), "159078803000024");
        assert_eq!(tick.ts_event, UnixNanos::from(1_787_562_119_833_000_000));
    }

    #[rstest]
    fn parses_snapshot_with_clear_and_flags(instrument: InstrumentAny) {
        let message: DeepXWsMessage<DeepXOrderBookUpdate> = serde_json::from_str(
            r#"{"channel":"perp@orderbook","data":{"asks":[["2457.02","0.859"]],"bids":[["2456.81","0.291"]],"engineTime":1787562160783,"lastUpdateId":62493922,"prevLastUpdateId":null,"serverTime":1787562160833,"symbol":"ETH-USDC","updateType":"snapshot"}}"#,
        )
        .unwrap();

        let deltas =
            parse_order_book_deltas(&message.data, &instrument, UnixNanos::default()).unwrap();

        assert_eq!(deltas.deltas.len(), 3);
        assert_eq!(deltas.deltas[0].action, BookAction::Clear);
        assert_eq!(deltas.deltas[1].action, BookAction::Add);
        assert_eq!(deltas.deltas[2].action, BookAction::Add);
        assert_eq!(
            deltas.deltas[2].flags,
            RecordFlag::F_SNAPSHOT as u8 | RecordFlag::F_LAST as u8
        );
        assert_eq!(deltas.deltas[2].sequence, 62_493_922);
    }

    #[rstest]
    fn parses_zero_quantity_as_delete(instrument: InstrumentAny) {
        let message: DeepXWsMessage<DeepXOrderBookUpdate> = serde_json::from_str(
            r#"{"channel":"perp@orderbook","data":{"asks":[["2457.02","0"]],"bids":[],"engineTime":1787562162051,"lastUpdateId":62494052,"prevLastUpdateId":62493922,"serverTime":1787562162052,"symbol":"ETH-USDC","updateType":"delta"}}"#,
        )
        .unwrap();

        let deltas =
            parse_order_book_deltas(&message.data, &instrument, UnixNanos::default()).unwrap();

        assert_eq!(deltas.deltas.len(), 1);
        assert_eq!(deltas.deltas[0].action, BookAction::Delete);
        assert_eq!(deltas.deltas[0].flags, RecordFlag::F_LAST as u8);
    }
}
