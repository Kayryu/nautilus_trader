// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Read-only verification of framework live trade subscriptions and shutdown.

use std::{cell::RefCell, collections::BTreeSet, rc::Rc, time::Duration};

use nautilus_common::{
    cache::Cache,
    clients::DataClient,
    live::{clock::LiveClock, runner::set_data_event_sender},
    messages::{
        DataEvent, DataResponse,
        data::{
            RequestBookDepth, RequestBookSnapshot, SubscribeBars, SubscribeBookDeltas,
            SubscribeBookDepth10, SubscribeFundingRates, SubscribeIndexPrices,
            SubscribeInstrumentStatus, SubscribeMarkPrices, SubscribeQuotes, SubscribeTrades,
            UnsubscribeBars, UnsubscribeBookDeltas, UnsubscribeBookDepth10,
            UnsubscribeFundingRates, UnsubscribeIndexPrices, UnsubscribeInstrumentStatus,
            UnsubscribeMarkPrices, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_deepx::{config::DeepXDataClientConfig, data::DeepXDataClient};
use nautilus_model::{
    data::{BarSpecification, BarType, Data},
    enums::{AggregationSource, BarAggregation, MarketStatusAction, PriceType, RecordFlag},
    identifiers::{ClientId, InstrumentId},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    set_data_event_sender(sender);
    let mut client = DeepXDataClient::new(
        ClientId::from("DEEPX-LIVE-TRADE-CHECK"),
        DeepXDataClientConfig::builder()
            .perpetual_only(!std::env::args().any(|arg| arg == "--spot"))
            .build(),
        Rc::new(RefCell::new(Cache::default())).into(),
        Rc::new(RefCell::new(LiveClock::default())),
    )?;
    client.connect().await?;
    while receiver.try_recv().is_ok() {}
    if std::env::args().any(|arg| arg == "--status") {
        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");
        client.subscribe_instrument_status(SubscribeInstrumentStatus::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        let event = tokio::time::timeout(Duration::from_secs(45), receiver.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("framework event channel closed"))?;
        let DataEvent::InstrumentStatus(status) = event else {
            anyhow::bail!("expected framework instrument status, received {event:?}");
        };
        let trading = matches!(status.action, MarketStatusAction::Trading);
        anyhow::ensure!(
            status.instrument_id == instrument_id
                && status.ts_event.as_u64() > 0
                && status.ts_event == status.ts_init
                && status.is_trading == Some(trading)
                && status.is_quoting == Some(trading),
            "framework instrument status identity or state mismatch"
        );
        println!(
            "DeepX framework instrument status action={} is_trading={} timestamp={}",
            status.action, trading, status.ts_event,
        );
        client.unsubscribe_instrument_status(&UnsubscribeInstrumentStatus::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        client.disconnect().await?;
        while receiver.try_recv().is_ok() {}
        anyhow::ensure!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err(),
            "instrument status was published after unsubscribe/disconnect"
        );
        println!(
            "DeepX framework status subscription retired; no credentials or transactions sent"
        );
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--bars") {
        let bar_type = BarType::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
            AggregationSource::External,
        );
        client.subscribe_bars(SubscribeBars::new(
            bar_type,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        let event = tokio::time::timeout(Duration::from_secs(180), receiver.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("framework event channel closed"))?;
        let DataEvent::Data(Data::Bar(bar)) = event else {
            anyhow::bail!("expected framework one-minute bar, received {event:?}");
        };
        anyhow::ensure!(
            bar.bar_type == bar_type && bar.ts_event.as_millis().is_multiple_of(60_000),
            "framework bar identity or bucket timestamp mismatch"
        );
        println!(
            "DeepX framework one-minute bar time={} open={} high={} low={} close={} volume={}",
            bar.ts_event, bar.open, bar.high, bar.low, bar.close, bar.volume,
        );
        client.unsubscribe_bars(&UnsubscribeBars::new(
            bar_type,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        client.disconnect().await?;
        while receiver.try_recv().is_ok() {}
        anyhow::ensure!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err(),
            "bar was published after unsubscribe/disconnect"
        );
        println!("DeepX framework bar subscription retired; no credentials or transactions sent");
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--depth-request") {
        let instrument_id = InstrumentId::from("BTC-USDC-PERP.DEEPX");
        let request_id = UUID4::new();
        client.request_book_depth(RequestBookDepth::new(
            instrument_id,
            None,
            None,
            std::num::NonZeroUsize::new(1),
            std::num::NonZeroUsize::new(10),
            Some(client.client_id()),
            request_id,
            UnixNanos::default(),
            None,
        ))?;
        let response = tokio::time::timeout(Duration::from_secs(30), receiver.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("framework event channel closed"))?;
        let DataEvent::Response(DataResponse::BookDepth(response)) = response else {
            anyhow::bail!("expected framework book depth response");
        };
        anyhow::ensure!(
            response.correlation_id == request_id
                && response.instrument_id == instrument_id
                && response.data.len() == 1,
            "book depth response identity mismatch"
        );
        let depth = &response.data[0];
        anyhow::ensure!(
            depth.sequence > 0
                && depth.ts_event.as_u64() > 0
                && !depth.bids[0].price.is_zero()
                && !depth.asks[0].price.is_zero()
                && depth.bid_counts[0] == 1
                && depth.ask_counts[0] == 1
                && depth.bid_counts.iter().all(|count| *count <= 1)
                && depth.ask_counts.iter().all(|count| *count <= 1)
                && RecordFlag::F_SNAPSHOT.matches(depth.flags),
            "book depth snapshot metadata is invalid"
        );
        println!(
            "DeepX framework depth request sequence={} timestamp={} bid={} ask={}; no private accounts or transactions",
            depth.sequence, depth.ts_event, depth.bids[0].price, depth.asks[0].price,
        );
        client.disconnect().await?;
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--snapshot") {
        let instrument_id = InstrumentId::from("BTC-USDC-PERP.DEEPX");
        client.request_book_snapshot(RequestBookSnapshot::new(
            instrument_id,
            std::num::NonZeroUsize::new(20),
            Some(client.client_id()),
            UUID4::new(),
            UnixNanos::default(),
            None,
        ))?;
        let response = tokio::time::timeout(Duration::from_secs(30), receiver.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("framework event channel closed"))?;
        let DataEvent::Response(DataResponse::Book(response)) = response else {
            anyhow::bail!("expected framework book snapshot response");
        };
        anyhow::ensure!(
            response.instrument_id == instrument_id && response.data.instrument_id == instrument_id,
            "book snapshot instrument mismatch"
        );
        anyhow::ensure!(
            response.data.bids(None).count() <= 20 && response.data.asks(None).count() <= 20,
            "book snapshot exceeded requested depth"
        );
        println!(
            "DeepX framework book snapshot sequence={} timestamp={} bids={} asks={} best_bid={:?} best_ask={:?}",
            response.data.sequence,
            response.data.ts_last,
            response.data.bids(None).count(),
            response.data.asks(None).count(),
            response.data.best_bid_price(),
            response.data.best_ask_price(),
        );
        client.disconnect().await?;
        while receiver.try_recv().is_ok() {}
        anyhow::ensure!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err(),
            "book snapshot response was published after disconnect"
        );
        println!(
            "DeepX framework book snapshot request/disconnect verified; no private accounts or transactions"
        );
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--prices") {
        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");
        client.subscribe_mark_prices(SubscribeMarkPrices::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        client.subscribe_index_prices(SubscribeIndexPrices::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        client.subscribe_funding_rates(SubscribeFundingRates::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut mark = false;
            let mut index = false;
            let mut funding = false;
            while !(mark && index && funding) {
                match receiver.recv().await {
                    Some(DataEvent::Data(Data::MarkPrice(update))) => {
                        anyhow::ensure!(
                            update.instrument_id == instrument_id,
                            "mark price instrument mismatch"
                        );
                        println!(
                            "DeepX framework mark price={} timestamp={}",
                            update.value, update.ts_event
                        );
                        mark = true;
                    }
                    Some(DataEvent::Data(Data::IndexPrice(update))) => {
                        anyhow::ensure!(
                            update.instrument_id == instrument_id,
                            "index price instrument mismatch"
                        );
                        println!(
                            "DeepX framework index price={} timestamp={}",
                            update.value, update.ts_event
                        );
                        index = true;
                    }
                    Some(DataEvent::FundingRate(update)) => {
                        anyhow::ensure!(
                            update.instrument_id == instrument_id,
                            "funding rate instrument mismatch"
                        );
                        anyhow::ensure!(
                            update.interval.is_none() && update.next_funding_ns.is_none(),
                            "DeepX funding schedule was inferred"
                        );
                        println!(
                            "DeepX framework funding rate={} timestamp={}",
                            update.rate, update.ts_event
                        );
                        funding = true;
                    }
                    Some(event) => anyhow::bail!("unexpected framework event: {event:?}"),
                    None => anyhow::bail!("framework event channel closed"),
                }
            }
            anyhow::Ok(())
        })
        .await??;
        client.unsubscribe_mark_prices(&UnsubscribeMarkPrices::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        client.unsubscribe_index_prices(&UnsubscribeIndexPrices::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        client.unsubscribe_funding_rates(&UnsubscribeFundingRates::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        client.disconnect().await?;
        while receiver.try_recv().is_ok() {}
        anyhow::ensure!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err(),
            "public price event was published after disconnect"
        );
        println!(
            "DeepX framework price/funding unsubscribe and disconnect verified; no private accounts or transactions"
        );
        return Ok(());
    }
    let instrument_id = InstrumentId::from("BTC-USDC-PERP.DEEPX");
    if std::env::args().any(|arg| arg == "--quotes") {
        client.subscribe_quotes(SubscribeQuotes::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        tokio::time::timeout(Duration::from_secs(30), async {
            for _ in 0..3 {
                let Some(DataEvent::Data(Data::Quote(quote))) = receiver.recv().await else {
                    anyhow::bail!("expected framework quote");
                };
                anyhow::ensure!(
                    quote.instrument_id == instrument_id,
                    "quote instrument mismatch"
                );
                println!(
                    "DeepX framework quote bid={} x {} ask={} x {}",
                    quote.bid_price, quote.bid_size, quote.ask_price, quote.ask_size
                );
            }
            anyhow::Ok(())
        })
        .await??;
        client.unsubscribe_quotes(&UnsubscribeQuotes::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        client.disconnect().await?;
        while receiver.try_recv().is_ok() {}
        anyhow::ensure!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err(),
            "quote published after disconnect"
        );
        println!(
            "DeepX framework quote unsubscribe/disconnect verified; no private accounts or transactions"
        );
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--book") {
        client.subscribe_book_deltas(SubscribeBookDeltas::new(
            instrument_id,
            nautilus_model::enums::BookType::L2_MBP,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            std::num::NonZeroUsize::new(20),
            true,
            None,
            None,
        ))?;
        tokio::time::timeout(Duration::from_secs(30), async {
            for _ in 0..3 {
                let Some(DataEvent::Data(Data::Deltas(batch))) = receiver.recv().await else {
                    anyhow::bail!("expected framework book deltas");
                };
                anyhow::ensure!(
                    batch.instrument_id == instrument_id,
                    "book instrument mismatch"
                );
                anyhow::ensure!(
                    nautilus_model::enums::RecordFlag::F_LAST.matches(
                        batch
                            .deltas
                            .last()
                            .ok_or_else(|| anyhow::anyhow!("empty book batch"))?
                            .flags
                    ),
                    "book batch is not terminated"
                );
                println!(
                    "DeepX framework book sequence={} levels={} snapshot={}",
                    batch.sequence,
                    batch.deltas.len(),
                    nautilus_model::enums::RecordFlag::F_SNAPSHOT.matches(batch.deltas[0].flags)
                );
            }
            anyhow::Ok(())
        })
        .await??;
        client.unsubscribe_book_deltas(&UnsubscribeBookDeltas::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        client.disconnect().await?;
        while receiver.try_recv().is_ok() {}
        anyhow::ensure!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err(),
            "book event was published after disconnect"
        );
        println!(
            "DeepX framework book unsubscribe/disconnect verified; no private accounts or transactions"
        );
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--depth10") {
        client.subscribe_book_depth10(SubscribeBookDepth10::new(
            instrument_id,
            nautilus_model::enums::BookType::L2_MBP,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            std::num::NonZeroUsize::new(10),
            true,
            None,
            None,
        ))?;
        let event = tokio::time::timeout(Duration::from_secs(30), receiver.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("framework event channel closed"))?;
        let DataEvent::Data(Data::Depth10(depth)) = event else {
            anyhow::bail!("expected framework depth10 snapshot");
        };
        anyhow::ensure!(
            depth.instrument_id == instrument_id,
            "depth10 instrument mismatch"
        );
        anyhow::ensure!(
            nautilus_model::enums::RecordFlag::F_SNAPSHOT.matches(depth.flags),
            "depth10 event is not a snapshot"
        );
        println!(
            "DeepX framework depth10 sequence={} timestamp={} best_bid={} x {} best_ask={} x {}",
            depth.sequence,
            depth.ts_event,
            depth.bids[0].price,
            depth.bids[0].size,
            depth.asks[0].price,
            depth.asks[0].size,
        );
        client.unsubscribe_book_depth10(&UnsubscribeBookDepth10::new(
            instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        client.disconnect().await?;
        while receiver.try_recv().is_ok() {}
        anyhow::ensure!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err(),
            "depth10 event was published after disconnect"
        );
        println!(
            "DeepX framework depth10 unsubscribe/disconnect verified; no private accounts or transactions"
        );
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--spot") {
        let spot_instrument_id = InstrumentId::from("ETH-USDC.DEEPX");
        client.subscribe_trades(SubscribeTrades::new(
            spot_instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        let spot_trade_result = tokio::time::timeout(Duration::from_secs(30), async {
            let mut ids = BTreeSet::new();
            let mut previous = None;
            while ids.is_empty() {
                let Some(DataEvent::Data(Data::Trade(tick))) = receiver.recv().await else {
                    anyhow::bail!("expected a framework Spot TradeTick");
                };
                anyhow::ensure!(
                    tick.instrument_id == spot_instrument_id,
                    "Spot live trade instrument mismatch"
                );
                anyhow::ensure!(
                    ids.insert(tick.trade_id.to_string()),
                    "duplicate Spot live trade ID"
                );
                anyhow::ensure!(
                    previous.is_none_or(|time| tick.ts_event >= time),
                    "Spot live trade timestamps moved backwards"
                );
                previous = Some(tick.ts_event);
                println!(
                    "DeepX framework live Spot trade ID={} price={} size={}",
                    tick.trade_id, tick.price, tick.size
                );
            }
            anyhow::Ok(())
        })
        .await;
        client.unsubscribe_trades(&UnsubscribeTrades::new(
            spot_instrument_id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        client.disconnect().await?;
        while receiver.try_recv().is_ok() {}
        anyhow::ensure!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err(),
            "Spot live trade was published after disconnect"
        );
        match spot_trade_result {
            Ok(result) => result?,
            Err(_) => println!("No novel Spot TradeTick arrived during the observation window"),
        }
        println!(
            "DeepX framework Spot trade unsubscribe/disconnect verified; no private accounts or transactions"
        );
        return Ok(());
    }
    client.subscribe_trades(SubscribeTrades::new(
        instrument_id,
        Some(client.client_id()),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    ))?;
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut ids = BTreeSet::new();
        let mut previous = None;
        while ids.len() < 3 {
            let Some(DataEvent::Data(Data::Trade(tick))) = receiver.recv().await else {
                anyhow::bail!("expected a framework live TradeTick");
            };
            anyhow::ensure!(
                tick.instrument_id == instrument_id,
                "live trade instrument mismatch"
            );
            anyhow::ensure!(
                ids.insert(tick.trade_id.to_string()),
                "duplicate live trade ID"
            );
            anyhow::ensure!(
                previous.is_none_or(|time| tick.ts_event >= time),
                "live trade timestamps moved backwards"
            );
            previous = Some(tick.ts_event);
            println!(
                "DeepX framework live trade ID={} price={} size={}",
                tick.trade_id, tick.price, tick.size
            );
        }
        anyhow::Ok(())
    })
    .await??;
    client.unsubscribe_trades(&UnsubscribeTrades::new(
        instrument_id,
        Some(client.client_id()),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    ))?;
    client.disconnect().await?;
    while receiver.try_recv().is_ok() {}
    anyhow::ensure!(
        tokio::time::timeout(Duration::from_millis(100), receiver.recv())
            .await
            .is_err(),
        "live trade was published after disconnect"
    );
    println!(
        "DeepX framework unsubscribe/disconnect verified; no private accounts or transactions"
    );
    Ok(())
}
