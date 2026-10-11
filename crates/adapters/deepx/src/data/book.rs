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

//! Atomic conversion and one-shot retrieval of validated aggregated L2 books.

use std::{num::NonZeroUsize, time::Duration};

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BookOrder, DEPTH10_LEN, OrderBookDelta, OrderBookDeltas, OrderBookDepth10, QuoteTick},
    enums::{BookAction, BookType, OrderSide, RecordFlag},
    instruments::{Instrument, InstrumentAny},
    orderbook::OrderBook,
    types::{Price, Quantity},
};

use crate::{
    config::DeepXNetworkConfig,
    http::{DeepXSpotMarket, DeepXSpotOrderBook},
    websocket::{
        DeepXWsError,
        book::{DeepXWsBookSnapshot, DeepXWsBookStream},
        orderbook_rpc::DeepXOrderbookRpcSnapshot,
        public::{DeepXWsPublicConnection, DeepXWsPublicFrame},
    },
};

pub(super) async fn request_book_snapshot(
    network: DeepXNetworkConfig,
    proxy_url: Option<String>,
    timeout: Duration,
    market_id: u16,
    depth: NonZeroUsize,
    instrument: InstrumentAny,
) -> anyhow::Result<OrderBook> {
    let (snapshot, ts_init) = request_raw_book_snapshot(
        network,
        proxy_url,
        timeout,
        market_id,
        depth,
        instrument.price_increment().as_decimal(),
    )
    .await?;
    let deltas = parse_book_deltas(None, &snapshot, &instrument, ts_init)?
        .ok_or_else(|| anyhow::anyhow!("DeepX book snapshot conversion was empty"))?;
    let mut book = OrderBook::new(instrument.id(), BookType::L2_MBP);
    book.apply_deltas(&deltas)?;
    Ok(book)
}

pub(super) async fn request_book_depth10(
    network: DeepXNetworkConfig,
    proxy_url: Option<String>,
    timeout: Duration,
    market_id: u16,
    depth: NonZeroUsize,
    instrument: InstrumentAny,
) -> anyhow::Result<OrderBookDepth10> {
    let (snapshot, ts_init) = request_raw_book_snapshot(
        network,
        proxy_url,
        timeout,
        market_id,
        depth,
        instrument.price_increment().as_decimal(),
    )
    .await?;
    parse_book_depth10(&snapshot, &instrument, ts_init)
}

async fn request_raw_book_snapshot(
    network: DeepXNetworkConfig,
    proxy_url: Option<String>,
    timeout: Duration,
    market_id: u16,
    depth: NonZeroUsize,
    price_increment: rust_decimal::Decimal,
) -> anyhow::Result<(DeepXWsBookSnapshot, UnixNanos)> {
    let mut connection = DeepXWsPublicConnection::connect(
        &network,
        proxy_url.as_deref(),
        timeout,
        NonZeroUsize::new(32).expect("nonzero capacity"),
    )
    .await?;
    let result = async {
        connection
            .subscribe_book(market_id, depth, price_increment)
            .await?;
        let capacity = NonZeroUsize::new(depth.get() * 2).expect("nonzero book capacity");
        let mut stream = DeepXWsBookStream::new(market_id, capacity);
        tokio::time::timeout(timeout, async {
            loop {
                let frame = match connection.next_frame().await {
                    Ok(frame) => frame,
                    Err(e)
                        if matches!(
                            e.downcast_ref::<DeepXWsError>(),
                            Some(DeepXWsError::ReceiveTimeout)
                        ) =>
                    {
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                if matches!(frame, DeepXWsPublicFrame::Pong { .. }) {
                    continue;
                }
                anyhow::ensure!(
                    stream.ingest(&frame)?,
                    "DeepX book snapshot request received a delta before a snapshot"
                );
                let snapshot = stream
                    .snapshot()
                    .ok_or_else(|| anyhow::anyhow!("DeepX book snapshot is missing"))?;
                anyhow::ensure!(
                    snapshot.bids.len() <= depth.get() && snapshot.asks.len() <= depth.get(),
                    "DeepX book snapshot exceeds requested per-side depth"
                );
                let ts_init = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
                return Ok((snapshot.clone(), ts_init));
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("DeepX book snapshot initial data deadline exceeded"))?
    }
    .await;
    let close = connection.close().await;
    result.and_then(|snapshot| close.map(|()| snapshot))
}

pub(super) fn parse_spot_order_book(
    snapshot: &DeepXSpotOrderBook,
    market: &DeepXSpotMarket,
    instrument: &InstrumentAny,
    depth: NonZeroUsize,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBook> {
    anyhow::ensure!(
        snapshot.pair_name == market.name,
        "DeepX Spot book name does not match instrument metadata"
    );
    let expected_pair = nautilus_core::hex::decode_array::<32>(
        market.pair.strip_prefix("0x").unwrap_or(&market.pair),
    )?;
    let observed_pair = nautilus_core::hex::decode_array::<32>(
        snapshot.pair.strip_prefix("0x").unwrap_or(&snapshot.pair),
    )?;
    anyhow::ensure!(
        expected_pair == observed_pair,
        "DeepX Spot book pair does not match instrument metadata"
    );
    anyhow::ensure!(
        snapshot.last_update_id > 0,
        "DeepX Spot book sequence must be nonzero"
    );
    let ts_event = UnixNanos::from(
        snapshot
            .engine_time
            .checked_mul(1_000_000)
            .ok_or_else(|| anyhow::anyhow!("DeepX Spot book timestamp overflows UnixNanos"))?,
    );
    let mut deltas = vec![OrderBookDelta::clear(
        instrument.id(),
        snapshot.last_update_id,
        ts_event,
        ts_init,
    )];
    for (side, levels) in [
        (OrderSide::Buy, &snapshot.order_buy_list),
        (OrderSide::Sell, &snapshot.order_sell_list),
    ] {
        for level in levels.iter().take(depth.get()) {
            let price = Price::from_decimal_dp(level.price, instrument.price_precision())?;
            let size = Quantity::from_decimal_dp(level.qty, instrument.size_precision())?;
            anyhow::ensure!(
                price.as_decimal() == level.price
                    && size.as_decimal() == level.qty
                    && price.as_decimal() % instrument.price_increment().as_decimal()
                        == rust_decimal::Decimal::ZERO
                    && size.as_decimal() % instrument.size_increment().as_decimal()
                        == rust_decimal::Decimal::ZERO,
                "DeepX Spot book level is not exactly representable on the instrument grid"
            );
            deltas.push(OrderBookDelta::new_checked(
                instrument.id(),
                BookAction::Add,
                BookOrder::new(side, price, size, 0),
                RecordFlag::F_SNAPSHOT as u8,
                snapshot.last_update_id,
                ts_event,
                ts_init,
            )?);
        }
    }
    if let Some(last) = deltas.last_mut() {
        last.flags |= RecordFlag::F_LAST as u8;
    }
    let deltas = OrderBookDeltas::new_checked(instrument.id(), deltas)?;
    let mut book = OrderBook::new(instrument.id(), BookType::L2_MBP);
    book.apply_deltas(&deltas)?;
    Ok(book)
}

pub(super) fn parse_spot_order_book_quote_tick(
    snapshot: &DeepXSpotOrderBook,
    market: &DeepXSpotMarket,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<QuoteTick>> {
    let book = parse_spot_order_book(
        snapshot,
        market,
        instrument,
        NonZeroUsize::new(1).expect("nonzero quote depth"),
        ts_init,
    )?;
    let ts_event = UnixNanos::from(
        snapshot
            .engine_time
            .checked_mul(1_000_000)
            .ok_or_else(|| anyhow::anyhow!("DeepX Spot book timestamp overflows UnixNanos"))?,
    );
    quote_tick_from_book(&book, instrument, ts_event, ts_init)
}

pub(super) fn quote_tick_from_book(
    book: &OrderBook,
    instrument: &InstrumentAny,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<QuoteTick>> {
    let (Some(bid_price), Some(bid_size), Some(ask_price), Some(ask_size)) = (
        book.best_bid_price(),
        book.best_bid_size(),
        book.best_ask_price(),
        book.best_ask_size(),
    ) else {
        return Ok(None);
    };
    Ok(Some(QuoteTick::new_checked(
        instrument.id(),
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        ts_event,
        ts_init,
    )?))
}

pub(super) fn parse_spot_rpc_book_deltas(
    snapshot: &DeepXOrderbookRpcSnapshot,
    market: &DeepXSpotMarket,
    instrument: &InstrumentAny,
    depth: NonZeroUsize,
    sequence: u64,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    anyhow::ensure!(sequence > 0, "DeepX Spot book sequence must be nonzero");
    let book = spot_rpc_pair(snapshot, market)?;
    let ts_event = ts_init;
    let mut deltas = vec![OrderBookDelta::clear(
        instrument.id(),
        sequence,
        ts_event,
        ts_init,
    )];
    for (side, levels) in [(OrderSide::Buy, &book.bids), (OrderSide::Sell, &book.asks)] {
        for level in levels.iter().take(depth.get()) {
            let (price, size) = parse_spot_rpc_level(level, market, instrument)?;
            deltas.push(OrderBookDelta::new_checked(
                instrument.id(),
                BookAction::Add,
                BookOrder::new(side, price, size, 0),
                RecordFlag::F_SNAPSHOT as u8,
                sequence,
                ts_event,
                ts_init,
            )?);
        }
    }
    if let Some(last) = deltas.last_mut() {
        last.flags |= RecordFlag::F_LAST as u8;
    }
    OrderBookDeltas::new_checked(instrument.id(), deltas)
}

pub(super) fn parse_spot_rpc_book_depth10(
    snapshot: &DeepXOrderbookRpcSnapshot,
    market: &DeepXSpotMarket,
    instrument: &InstrumentAny,
    sequence: u64,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDepth10> {
    anyhow::ensure!(sequence > 0, "DeepX Spot book sequence must be nonzero");
    let book = spot_rpc_pair(snapshot, market)?;
    let mut bids = [BookOrder::default(); DEPTH10_LEN];
    let mut asks = [BookOrder::default(); DEPTH10_LEN];
    let mut bid_counts = [0; DEPTH10_LEN];
    let mut ask_counts = [0; DEPTH10_LEN];
    for (index, level) in book.bids.iter().take(DEPTH10_LEN).enumerate() {
        let (price, size) = parse_spot_rpc_level(level, market, instrument)?;
        bids[index] = BookOrder::new(OrderSide::Buy, price, size, 0);
        bid_counts[index] = level.order_count;
    }
    for (index, level) in book.asks.iter().take(DEPTH10_LEN).enumerate() {
        let (price, size) = parse_spot_rpc_level(level, market, instrument)?;
        asks[index] = BookOrder::new(OrderSide::Sell, price, size, 0);
        ask_counts[index] = level.order_count;
    }
    for bid in bids.iter_mut().skip(book.bids.len().min(DEPTH10_LEN)) {
        *bid = BookOrder::new(
            OrderSide::Buy,
            Price::zero(instrument.price_precision()),
            Quantity::zero(instrument.size_precision()),
            0,
        );
    }
    for ask in asks.iter_mut().skip(book.asks.len().min(DEPTH10_LEN)) {
        *ask = BookOrder::new(
            OrderSide::Sell,
            Price::zero(instrument.price_precision()),
            Quantity::zero(instrument.size_precision()),
            0,
        );
    }
    Ok(OrderBookDepth10::new(
        instrument.id(),
        bids,
        asks,
        bid_counts,
        ask_counts,
        RecordFlag::F_SNAPSHOT as u8,
        sequence,
        ts_init,
        ts_init,
    ))
}

pub(super) fn parse_spot_rpc_quote_tick(
    snapshot: &DeepXOrderbookRpcSnapshot,
    market: &DeepXSpotMarket,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<QuoteTick>> {
    let book = spot_rpc_pair(snapshot, market)?;
    let (Some(bid), Some(ask)) = (book.bids.first(), book.asks.first()) else {
        return Ok(None);
    };
    let (bid_price, bid_size) = parse_spot_rpc_level(bid, market, instrument)?;
    let (ask_price, ask_size) = parse_spot_rpc_level(ask, market, instrument)?;
    Ok(Some(QuoteTick::new_checked(
        instrument.id(),
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        ts_init,
        ts_init,
    )?))
}

pub(super) fn spot_rpc_pair<'a>(
    snapshot: &'a DeepXOrderbookRpcSnapshot,
    market: &DeepXSpotMarket,
) -> anyhow::Result<&'a crate::websocket::orderbook_rpc::DeepXOrderbookRpcSide> {
    let expected_pair = nautilus_core::hex::decode_array::<32>(
        market.pair.strip_prefix("0x").unwrap_or(&market.pair),
    )?;
    let mut pair_book = None;
    for (pair, book) in &snapshot.spot {
        let observed_pair =
            nautilus_core::hex::decode_array::<32>(pair.strip_prefix("0x").unwrap_or(pair))?;
        anyhow::ensure!(
            observed_pair == expected_pair,
            "DeepX Spot RPC snapshot contains an unexpected pair"
        );
        anyhow::ensure!(
            pair_book.replace(book).is_none(),
            "duplicate DeepX Spot pair book"
        );
    }
    pair_book.ok_or_else(|| anyhow::anyhow!("DeepX Spot RPC snapshot is missing pair"))
}

fn parse_spot_rpc_level(
    level: &crate::websocket::orderbook_rpc::DeepXOrderbookRpcLevel,
    market: &DeepXSpotMarket,
    instrument: &InstrumentAny,
) -> anyhow::Result<(Price, Quantity)> {
    let raw_price = rpc_atomic_decimal(&level.price, 6)?;
    let raw_size = rpc_atomic_decimal(&level.amount, market.base_decimal)?;
    let price = Price::from_decimal_dp(raw_price, instrument.price_precision())?;
    let size = Quantity::from_decimal_dp(raw_size, instrument.size_precision())?;
    anyhow::ensure!(
        price.as_decimal() == raw_price
            && size.as_decimal() == raw_size
            && price.as_decimal() > rust_decimal::Decimal::ZERO
            && size.as_decimal() > rust_decimal::Decimal::ZERO
            && price.as_decimal() % instrument.price_increment().as_decimal()
                == rust_decimal::Decimal::ZERO
            && size.as_decimal() % instrument.size_increment().as_decimal()
                == rust_decimal::Decimal::ZERO,
        "DeepX Spot RPC level is invalid or outside the instrument grid"
    );
    Ok((price, size))
}

fn rpc_atomic_decimal(value: &str, scale: u8) -> anyhow::Result<rust_decimal::Decimal> {
    let value = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .ok_or_else(|| anyhow::anyhow!("DeepX Spot RPC integer lacks hexadecimal prefix"))?;
    anyhow::ensure!(!value.is_empty(), "DeepX Spot RPC integer is empty");
    let value = u128::from_str_radix(value, 16)
        .map_err(|e| anyhow::anyhow!("invalid DeepX Spot RPC integer: {e}"))?;
    let value = i128::try_from(value)
        .map_err(|_| anyhow::anyhow!("DeepX Spot RPC integer exceeds Decimal range"))?;
    Ok(rust_decimal::Decimal::from_i128_with_scale(value, u32::from(scale)).normalize())
}

pub(super) fn parse_book_deltas(
    previous: Option<&DeepXWsBookSnapshot>,
    next: &DeepXWsBookSnapshot,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<OrderBookDeltas>> {
    let ts_event = UnixNanos::from(
        next.engine_time
            .checked_mul(1_000_000)
            .ok_or_else(|| anyhow::anyhow!("DeepX book engine timestamp overflows nanoseconds"))?,
    );
    let mut deltas = Vec::new();
    let flags = if previous.is_none() {
        RecordFlag::F_SNAPSHOT as u8
    } else {
        0
    };
    if previous.is_none() {
        deltas.push(OrderBookDelta::clear(
            instrument.id(),
            next.last_update_id,
            ts_event,
            ts_init,
        ));
    }
    for (side, levels, old) in [
        (OrderSide::Buy, &next.bids, previous.map(|book| &book.bids)),
        (OrderSide::Sell, &next.asks, previous.map(|book| &book.asks)),
    ] {
        if let Some(old) = old {
            for (price, level) in old {
                if !levels.contains_key(price) {
                    let price = Price::from_decimal_dp(level.price, instrument.price_precision())?;
                    deltas.push(OrderBookDelta::new_checked(
                        instrument.id(),
                        BookAction::Delete,
                        BookOrder::new(side, price, Quantity::zero(instrument.size_precision()), 0),
                        flags,
                        next.last_update_id,
                        ts_event,
                        ts_init,
                    )?);
                }
            }
        }
        for (key, level) in levels {
            let old_level = old.and_then(|levels| levels.get(key));
            let price = Price::from_decimal_dp(level.price, instrument.price_precision())?;
            let size = Quantity::from_decimal_dp(level.qty, instrument.size_precision())?;
            anyhow::ensure!(
                price.as_decimal() == level.price && size.as_decimal() == level.qty,
                "DeepX book level would lose precision"
            );
            if old_level.is_some_and(|old| old.price == level.price && old.qty == level.qty) {
                continue;
            }
            let action = if old_level.is_some() {
                BookAction::Update
            } else {
                BookAction::Add
            };
            deltas.push(OrderBookDelta::new_checked(
                instrument.id(),
                action,
                BookOrder::new(side, price, size, 0),
                flags,
                next.last_update_id,
                ts_event,
                ts_init,
            )?);
        }
    }
    if let Some(last) = deltas.last_mut() {
        last.flags |= RecordFlag::F_LAST as u8;
        return Ok(Some(OrderBookDeltas::new_checked(instrument.id(), deltas)?));
    }
    Ok(None)
}

pub(super) fn parse_quote_tick(
    previous: Option<&DeepXWsBookSnapshot>,
    next: &DeepXWsBookSnapshot,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<QuoteTick>> {
    let Some((_, bid)) = next.bids.last_key_value() else {
        return Ok(None);
    };
    let Some((_, ask)) = next.asks.first_key_value() else {
        return Ok(None);
    };
    let bid_price = Price::from_decimal_dp(bid.price, instrument.price_precision())?;
    let ask_price = Price::from_decimal_dp(ask.price, instrument.price_precision())?;
    let bid_size = Quantity::from_decimal_dp(bid.qty, instrument.size_precision())?;
    let ask_size = Quantity::from_decimal_dp(ask.qty, instrument.size_precision())?;
    anyhow::ensure!(
        bid_price.as_decimal() == bid.price
            && ask_price.as_decimal() == ask.price
            && bid_size.as_decimal() == bid.qty
            && ask_size.as_decimal() == ask.qty,
        "DeepX quote would lose precision"
    );
    if previous.is_some_and(|old| {
        old.bids
            .last_key_value()
            .is_some_and(|(_, old_bid)| old_bid.price == bid.price && old_bid.qty == bid.qty)
            && old
                .asks
                .first_key_value()
                .is_some_and(|(_, old_ask)| old_ask.price == ask.price && old_ask.qty == ask.qty)
    }) {
        return Ok(None);
    }
    let ts_event =
        UnixNanos::from(next.engine_time.checked_mul(1_000_000).ok_or_else(|| {
            anyhow::anyhow!("DeepX quote engine timestamp overflows nanoseconds")
        })?);
    Ok(Some(QuoteTick::new_checked(
        instrument.id(),
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        ts_event,
        ts_init,
    )?))
}

pub(super) fn parse_book_depth10(
    snapshot: &DeepXWsBookSnapshot,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDepth10> {
    let ts_event = UnixNanos::from(
        snapshot
            .engine_time
            .checked_mul(1_000_000)
            .ok_or_else(|| anyhow::anyhow!("DeepX book engine timestamp overflows nanoseconds"))?,
    );
    let mut bids = [BookOrder::default(); DEPTH10_LEN];
    let mut asks = [BookOrder::default(); DEPTH10_LEN];
    let mut bid_counts = [0; DEPTH10_LEN];
    let mut ask_counts = [0; DEPTH10_LEN];

    for (index, level) in snapshot.bids.values().rev().take(DEPTH10_LEN).enumerate() {
        bids[index] = depth_order(OrderSide::Buy, level, instrument)?;
        bid_counts[index] = 1;
    }
    for (index, level) in snapshot.asks.values().take(DEPTH10_LEN).enumerate() {
        asks[index] = depth_order(OrderSide::Sell, level, instrument)?;
        ask_counts[index] = 1;
    }
    for bid in bids.iter_mut().skip(snapshot.bids.len().min(DEPTH10_LEN)) {
        *bid = BookOrder::new(
            OrderSide::Buy,
            Price::zero(instrument.price_precision()),
            Quantity::zero(instrument.size_precision()),
            0,
        );
    }
    for ask in asks.iter_mut().skip(snapshot.asks.len().min(DEPTH10_LEN)) {
        *ask = BookOrder::new(
            OrderSide::Sell,
            Price::zero(instrument.price_precision()),
            Quantity::zero(instrument.size_precision()),
            0,
        );
    }

    Ok(OrderBookDepth10::new(
        instrument.id(),
        bids,
        asks,
        bid_counts,
        ask_counts,
        RecordFlag::F_SNAPSHOT as u8,
        snapshot.last_update_id,
        ts_event,
        ts_init,
    ))
}

fn depth_order(
    side: OrderSide,
    level: &crate::websocket::book::DeepXWsBookLevel,
    instrument: &InstrumentAny,
) -> anyhow::Result<BookOrder> {
    let price = Price::from_decimal_dp(level.price, instrument.price_precision())?;
    let size = Quantity::from_decimal_dp(level.qty, instrument.size_precision())?;
    anyhow::ensure!(
        price.as_decimal() == level.price && size.as_decimal() == level.qty,
        "DeepX book depth would lose precision"
    );
    Ok(BookOrder::new(side, price, size, 0))
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::CurrencyType,
        identifiers::{InstrumentId, Symbol},
        instruments::CurrencyPair,
        types::Currency,
    };
    use rstest::rstest;

    use super::*;
    use crate::websocket::{
        book::DeepXWsBookLevel,
        orderbook_rpc::{DeepXOrderbookRpcLevel, DeepXOrderbookRpcSide},
    };

    const SPOT_MARKETS: &str = include_str!("../../test_data/http/testnet/spot_markets.json");
    const SPOT_BOOK: &str =
        include_str!("../../test_data/http/testnet/spot_order_book_eth_usdc_tick_001.json");

    fn spot_fixture() -> (
        crate::http::DeepXSpotMarket,
        DeepXSpotOrderBook,
        InstrumentAny,
    ) {
        let markets: crate::http::DeepXApiResponse<Vec<crate::http::DeepXSpotMarket>> =
            serde_json::from_str(SPOT_MARKETS).unwrap();
        let book: crate::http::DeepXApiResponse<DeepXSpotOrderBook> =
            serde_json::from_str(SPOT_BOOK).unwrap();
        let market = markets
            .data
            .into_iter()
            .find(|market| market.name == "ETH/USDC")
            .unwrap();
        let instrument = InstrumentAny::CurrencyPair(
            CurrencyPair::builder()
                .instrument_id(InstrumentId::from("ETH-USDC.DEEPX"))
                .raw_symbol(Symbol::new("ETH/USDC"))
                .base_currency(
                    Currency::new_checked("ETH", 18, 0, "ETH", CurrencyType::Crypto).unwrap(),
                )
                .quote_currency(
                    Currency::new_checked("USDC", 6, 0, "USDC", CurrencyType::Crypto).unwrap(),
                )
                .price_precision(2)
                .size_precision(4)
                .price_increment(Price::from("0.01"))
                .size_increment(Quantity::from("0.0001"))
                .ts_event(UnixNanos::default())
                .ts_init(UnixNanos::default())
                .build()
                .unwrap(),
        );
        (market, book.data, instrument)
    }

    fn spot_rpc_snapshot(market: &crate::http::DeepXSpotMarket) -> DeepXOrderbookRpcSnapshot {
        let level = |price: u128, amount: u128| DeepXOrderbookRpcLevel {
            price: format!("0x{price:x}"),
            amount: format!("0x{amount:x}"),
            order_count: 2,
        };
        DeepXOrderbookRpcSnapshot {
            spot: std::collections::BTreeMap::from([(
                market.pair.clone(),
                DeepXOrderbookRpcSide {
                    bids: vec![level(2_435_670_000, 903_000_000_000_000_000)],
                    asks: vec![level(2_435_820_000, 1_605_500_000_000_000_000)],
                },
            )]),
            perp: std::collections::BTreeMap::new(),
        }
    }

    fn snapshot() -> DeepXWsBookSnapshot {
        let mut book = DeepXWsBookSnapshot {
            last_update_id: 10,
            engine_time: 2000,
            bids: Default::default(),
            asks: Default::default(),
        };
        for (side, price, qty) in [
            (true, "1792.60", "0.004"),
            (true, "1792.50", "0.005"),
            (false, "1792.80", "0.006"),
            (false, "1792.90", "0.007"),
        ] {
            let level = DeepXWsBookLevel {
                price: price.parse().unwrap(),
                qty: qty.parse().unwrap(),
                value: rust_decimal::Decimal::ZERO,
            };
            if side {
                book.bids.insert(level.price, level);
            } else {
                book.asks.insert(level.price, level);
            }
        }
        book
    }

    #[rstest]
    fn quote_uses_best_levels_and_exact_engine_time() {
        let instrument = InstrumentAny::CryptoPerpetual(
            nautilus_model::instruments::stubs::crypto_perpetual_ethusdt(),
        );
        let book = snapshot();
        let quote = parse_quote_tick(None, &book, &instrument, UnixNanos::from(123))
            .unwrap()
            .unwrap();
        assert_eq!(quote.bid_price.as_decimal().to_string(), "1792.60");
        assert_eq!(quote.ask_price.as_decimal().to_string(), "1792.80");
        assert_eq!(quote.bid_size.as_decimal().to_string(), "0.004");
        assert_eq!(quote.ask_size.as_decimal().to_string(), "0.006");
        assert_eq!(quote.ts_event.as_millis(), 2000);
        assert_eq!(quote.ts_init.as_u64(), 123);
        let mut unchanged = book.clone();
        unchanged.engine_time += 1;
        unchanged.bids.first_entry().unwrap().get_mut().qty = rust_decimal::Decimal::ONE;
        assert!(
            parse_quote_tick(Some(&book), &unchanged, &instrument, Default::default())
                .unwrap()
                .is_none()
        );
        unchanged.bids.last_entry().unwrap().get_mut().qty = rust_decimal::Decimal::ONE;
        assert!(
            parse_quote_tick(Some(&book), &unchanged, &instrument, Default::default())
                .unwrap()
                .is_some()
        );
    }

    #[rstest]
    #[case("bids")]
    #[case("asks")]
    fn missing_side_never_fabricates_quotes(#[case] side: &str) {
        let instrument = InstrumentAny::CryptoPerpetual(
            nautilus_model::instruments::stubs::crypto_perpetual_ethusdt(),
        );
        let mut book = snapshot();
        if side == "bids" {
            book.bids.clear();
        } else {
            book.asks.clear();
        }
        assert!(
            parse_quote_tick(None, &book, &instrument, Default::default())
                .unwrap()
                .is_none()
        );
    }

    #[rstest]
    #[case("price")]
    #[case("quantity")]
    #[case("timestamp")]
    fn quote_conversion_rejects_precision_loss_and_overflow(#[case] mutation: &str) {
        let instrument = InstrumentAny::CryptoPerpetual(
            nautilus_model::instruments::stubs::crypto_perpetual_ethusdt(),
        );
        let mut book = snapshot();
        match mutation {
            "price" => {
                book.asks.first_entry().unwrap().get_mut().price = "1792.801".parse().unwrap();
            }
            "quantity" => {
                book.asks.first_entry().unwrap().get_mut().qty = "0.00001".parse().unwrap();
            }
            _ => book.engine_time = u64::MAX,
        }
        assert!(parse_quote_tick(None, &book, &instrument, Default::default()).is_err());
    }

    #[rstest]
    fn depth10_uses_best_first_levels_and_preserves_metadata() {
        let instrument = InstrumentAny::CryptoPerpetual(
            nautilus_model::instruments::stubs::crypto_perpetual_ethusdt(),
        );
        let depth = parse_book_depth10(&snapshot(), &instrument, UnixNanos::from(123)).unwrap();
        assert_eq!(depth.bids[0].price.as_decimal().to_string(), "1792.60");
        assert_eq!(depth.bids[1].price.as_decimal().to_string(), "1792.50");
        assert_eq!(depth.asks[0].price.as_decimal().to_string(), "1792.80");
        assert_eq!(depth.asks[1].price.as_decimal().to_string(), "1792.90");
        assert_eq!(depth.bid_counts[..2], [1, 1]);
        assert_eq!(depth.ask_counts[..2], [1, 1]);
        assert!(depth.bids[2..].iter().all(|order| order.price.is_zero()));
        assert!(depth.asks[2..].iter().all(|order| order.price.is_zero()));
        assert_eq!(depth.sequence, 10);
        assert_eq!(depth.ts_event.as_millis(), 2000);
        assert_eq!(depth.ts_init.as_u64(), 123);
        assert!(RecordFlag::F_SNAPSHOT.matches(depth.flags));
    }

    #[rstest]
    #[case("price")]
    #[case("quantity")]
    #[case("timestamp")]
    fn depth10_rejects_precision_loss_and_overflow(#[case] mutation: &str) {
        let instrument = InstrumentAny::CryptoPerpetual(
            nautilus_model::instruments::stubs::crypto_perpetual_ethusdt(),
        );
        let mut book = snapshot();
        match mutation {
            "price" => {
                book.bids.last_entry().unwrap().get_mut().price = "1792.601".parse().unwrap();
            }
            "quantity" => {
                book.asks.first_entry().unwrap().get_mut().qty = "0.00001".parse().unwrap();
            }
            _ => book.engine_time = u64::MAX,
        }
        assert!(parse_book_depth10(&book, &instrument, Default::default()).is_err());
    }

    #[rstest]
    fn spot_rest_snapshot_converts_exact_best_levels_and_depth() {
        let (market, snapshot, instrument) = spot_fixture();
        let book = parse_spot_order_book(
            &snapshot,
            &market,
            &instrument,
            NonZeroUsize::new(2).unwrap(),
            UnixNanos::from(123),
        )
        .unwrap();

        assert_eq!(
            book.best_bid_price().unwrap().as_decimal().to_string(),
            "2435.67"
        );
        assert_eq!(
            book.best_ask_price().unwrap().as_decimal().to_string(),
            "2435.82"
        );
        assert_eq!(book.bids(None).count(), 2);
        assert_eq!(book.asks(None).count(), 2);
        assert_eq!(book.ts_last.as_u64(), 1_789_620_906_510_000_000);
    }

    #[rstest]
    fn spot_rest_snapshot_converts_current_quote() {
        let (market, snapshot, instrument) = spot_fixture();
        let quote =
            parse_spot_order_book_quote_tick(&snapshot, &market, &instrument, UnixNanos::from(123))
                .unwrap()
                .unwrap();

        assert_eq!(quote.bid_price.to_string(), "2435.67");
        assert_eq!(quote.bid_size.to_string(), "0.9030");
        assert_eq!(quote.ask_price.to_string(), "2435.82");
        assert_eq!(quote.ask_size.to_string(), "1.6055");
        assert_eq!(quote.ts_event.as_u64(), 1_789_620_906_510_000_000);
        assert_eq!(quote.ts_init.as_u64(), 123);
    }

    #[rstest]
    #[case("bids")]
    #[case("asks")]
    fn spot_rest_snapshot_with_empty_side_has_no_quote(#[case] empty_side: &str) {
        let (market, mut snapshot, instrument) = spot_fixture();
        match empty_side {
            "bids" => snapshot.order_buy_list.clear(),
            "asks" => snapshot.order_sell_list.clear(),
            _ => unreachable!(),
        }

        assert!(
            parse_spot_order_book_quote_tick(
                &snapshot,
                &market,
                &instrument,
                UnixNanos::from(123),
            )
            .unwrap()
            .is_none()
        );
    }

    #[rstest]
    fn spot_rest_snapshot_rejects_off_grid_levels() {
        let (market, snapshot, mut instrument) = spot_fixture();
        let InstrumentAny::CurrencyPair(pair) = &mut instrument else {
            unreachable!();
        };
        pair.size_increment = Quantity::from("0.0007");

        assert!(
            parse_spot_order_book(
                &snapshot,
                &market,
                &instrument,
                NonZeroUsize::new(2).unwrap(),
                UnixNanos::default(),
            )
            .is_err()
        );
    }

    #[rstest]
    fn spot_rpc_snapshot_converts_exact_snapshot_deltas() {
        let (market, _, instrument) = spot_fixture();
        let snapshot = spot_rpc_snapshot(&market);
        let deltas = parse_spot_rpc_book_deltas(
            &snapshot,
            &market,
            &instrument,
            NonZeroUsize::new(1).unwrap(),
            7,
            UnixNanos::from(123),
        )
        .unwrap();
        let mut book = OrderBook::new(instrument.id(), BookType::L2_MBP);
        book.apply_deltas(&deltas).unwrap();

        assert_eq!(deltas.deltas.len(), 3);
        assert_eq!(book.best_bid_price().unwrap(), Price::from("2435.67"));
        assert_eq!(book.best_ask_price().unwrap(), Price::from("2435.82"));
        assert_eq!(book.ts_last.as_u64(), 123);
    }

    #[rstest]
    fn spot_rpc_snapshot_converts_depth10_and_best_quote() {
        let (market, _, instrument) = spot_fixture();
        let snapshot = spot_rpc_snapshot(&market);
        let depth =
            parse_spot_rpc_book_depth10(&snapshot, &market, &instrument, 8, UnixNanos::from(124))
                .unwrap();
        let quote =
            parse_spot_rpc_quote_tick(&snapshot, &market, &instrument, UnixNanos::from(124))
                .unwrap()
                .unwrap();

        assert_eq!(depth.bids[0].price, Price::from("2435.67"));
        assert_eq!(depth.asks[0].size, Quantity::from("1.6055"));
        assert_eq!(depth.bid_counts[0], 2);
        assert_eq!(quote.bid_price.as_decimal().to_string(), "2435.67");
        assert_eq!(quote.ask_price.as_decimal().to_string(), "2435.82");
    }

    #[rstest]
    fn spot_rpc_snapshot_rejects_off_grid_quantities() {
        let (market, _, mut instrument) = spot_fixture();
        let InstrumentAny::CurrencyPair(pair) = &mut instrument else {
            unreachable!();
        };
        pair.size_increment = Quantity::from("0.0007");
        let snapshot = spot_rpc_snapshot(&market);

        assert!(
            parse_spot_rpc_book_deltas(
                &snapshot,
                &market,
                &instrument,
                NonZeroUsize::new(1).unwrap(),
                1,
                UnixNanos::default(),
            )
            .is_err()
        );
    }
}
