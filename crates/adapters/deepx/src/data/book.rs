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
    websocket::{
        DeepXWsError,
        book::{DeepXWsBookSnapshot, DeepXWsBookStream},
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
    let mut connection = DeepXWsPublicConnection::connect(
        &network,
        proxy_url.as_deref(),
        timeout,
        NonZeroUsize::new(32).expect("nonzero capacity"),
    )
    .await?;
    let result = async {
        connection
            .subscribe_book(market_id, depth, instrument.price_increment().as_decimal())
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
                let deltas = parse_book_deltas(None, snapshot, &instrument, ts_init)?
                    .ok_or_else(|| anyhow::anyhow!("DeepX book snapshot conversion was empty"))?;
                let mut book = OrderBook::new(instrument.id(), BookType::L2_MBP);
                book.apply_deltas(&deltas)?;
                return Ok(book);
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("DeepX book snapshot initial data deadline exceeded"))?
    }
    .await;
    let close = connection.close().await;
    result.and_then(|book| close.map(|()| book))
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
    use super::*;
    use crate::websocket::book::DeepXWsBookLevel;
    use rstest::rstest;

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
                book.asks.first_entry().unwrap().get_mut().price = "1792.801".parse().unwrap()
            }
            "quantity" => {
                book.asks.first_entry().unwrap().get_mut().qty = "0.00001".parse().unwrap()
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
                book.bids.last_entry().unwrap().get_mut().price = "1792.601".parse().unwrap()
            }
            "quantity" => {
                book.asks.first_entry().unwrap().get_mut().qty = "0.00001".parse().unwrap()
            }
            _ => book.engine_time = u64::MAX,
        }
        assert!(parse_book_depth10(&book, &instrument, Default::default()).is_err());
    }
}
