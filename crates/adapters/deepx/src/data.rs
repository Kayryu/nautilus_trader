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

//! Live data client for DeepX.

use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::{runner::get_data_event_sender, runtime::get_runtime},
    messages::{
        DataEvent, DataResponse,
        data::{
            BookResponse, InstrumentResponse, InstrumentsResponse, RequestBars, RequestBookDeltas,
            RequestBookDepth, RequestBookSnapshot, RequestCustomData, RequestForwardPrices,
            RequestFundingRates, RequestInstrument, RequestInstruments, RequestQuotes,
            RequestTrades, SubscribeBars, SubscribeBookDeltas, SubscribeBookDepth10,
            SubscribeCustomData, SubscribeFundingRates, SubscribeIndexPrices, SubscribeInstrument,
            SubscribeInstrumentClose, SubscribeInstrumentStatus, SubscribeInstruments,
            SubscribeMarkPrices, SubscribeOptionGreeks, SubscribeQuotes, SubscribeTrades,
            UnsubscribeBars, UnsubscribeBookDeltas, UnsubscribeBookDepth10, UnsubscribeCustomData,
            UnsubscribeFundingRates, UnsubscribeIndexPrices, UnsubscribeInstrument,
            UnsubscribeInstrumentClose, UnsubscribeInstrumentStatus, UnsubscribeInstruments,
            UnsubscribeMarkPrices, UnsubscribeOptionGreeks, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
    providers::InstrumentProvider,
};
use nautilus_core::{datetime::datetime_to_unix_nanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::Data,
    enums::BookType,
    identifiers::{ClientId, Venue},
    instruments::{Instrument, InstrumentAny},
    orderbook::OrderBook,
};

use crate::{
    common::{consts::DEEPX_VENUE, enums::DeepXProductType, symbol::raw_symbol_from_instrument_id},
    config::DeepXDataClientConfig,
    http::{
        client::DeepXRawHttpClient, parse::parse_perpetual_instrument, query::DeepXOrderBookQuery,
    },
    providers::DeepXInstrumentProvider,
    websocket::{
        book_sync::{DeepXBookSync, DeepXBookSyncOutcome},
        client::{DeepXWebSocketClient, DeepXWebSocketMessage},
        enums::DeepXBookUpdateType,
        messages::DeepXOrderBookUpdate,
        parse::{parse_order_book_deltas, parse_trade_tick},
    },
};

/// DeepX data client for verified perpetual instruments and public streaming.
#[derive(Debug)]
pub struct DeepXDataClient {
    client_id: ClientId,
    http_client: DeepXRawHttpClient,
    provider: DeepXInstrumentProvider,
    update_instruments_interval_mins: u64,
    instrument_refresh_task: Option<tokio::task::JoinHandle<()>>,
    websocket_client: DeepXWebSocketClient,
    websocket_task: Option<tokio::task::JoinHandle<()>>,
    is_connected: AtomicBool,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
}

impl DeepXDataClient {
    /// Creates a new [`DeepXDataClient`] instance.
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be initialized.
    pub fn new(client_id: ClientId, config: DeepXDataClientConfig) -> anyhow::Result<Self> {
        let websocket_client = DeepXWebSocketClient::new(
            config.ws_url(),
            config.ws_timeout_secs,
            config.proxy_url.clone(),
        );
        let http_client = DeepXRawHttpClient::new(
            Some(config.rest_url()),
            config.http_timeout_secs,
            config.proxy_url,
        )?;

        Ok(Self {
            client_id,
            provider: DeepXInstrumentProvider::new(http_client.clone()),
            http_client,
            update_instruments_interval_mins: config.update_instruments_interval_mins,
            instrument_refresh_task: None,
            websocket_client,
            websocket_task: None,
            is_connected: AtomicBool::new(false),
            data_sender: get_data_event_sender(),
        })
    }

    fn spawn_instrument_refresh(&mut self) {
        let minutes = self.update_instruments_interval_mins;
        if minutes == 0 || self.instrument_refresh_task.is_some() {
            return;
        }

        let interval = Duration::from_secs(minutes.saturating_mul(60));
        let http_client = self.http_client.clone();
        let data_sender = self.data_sender.clone();
        let client_id = self.client_id;

        self.instrument_refresh_task = Some(get_runtime().spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                let result = async {
                    let markets = http_client.get_perp_markets().await?;
                    let ts_init = get_atomic_clock_realtime().get_time_ns();
                    let instruments = markets
                        .iter()
                        .map(|market| parse_perpetual_instrument(market, ts_init))
                        .collect::<anyhow::Result<Vec<_>>>()?;

                    for instrument in instruments {
                        data_sender
                            .send(DataEvent::Instrument(instrument))
                            .map_err(|e| {
                                anyhow::anyhow!("failed to publish DeepX instrument: {e}")
                            })?;
                    }

                    anyhow::Ok(markets.len())
                }
                .await;

                match result {
                    Ok(count) => log::debug!(
                        "DeepX instruments refreshed: client_id={client_id}, count={count}"
                    ),
                    Err(e) => log::warn!(
                        "Failed to refresh DeepX instruments: client_id={client_id}, error={e:?}"
                    ),
                }
            }
        }));
    }

    fn cancel_instrument_refresh(&mut self) {
        if let Some(task) = self.instrument_refresh_task.take() {
            task.abort();
        }
    }

    fn cancel_websocket_task(&mut self) {
        if let Some(task) = self.websocket_task.take() {
            task.abort();
        }
    }

    fn unsupported(operation: &str) -> anyhow::Result<()> {
        anyhow::bail!("DeepX {operation} is not supported")
    }
}

fn spawn_websocket_task(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<DeepXWebSocketMessage>,
    http_client: DeepXRawHttpClient,
    instruments: std::collections::HashMap<String, InstrumentAny>,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
) -> tokio::task::JoinHandle<()> {
    get_runtime().spawn(async move {
        let mut book_sync = DeepXBookSync::default();
        let (recovery_tx, mut recovery_rx) = tokio::sync::mpsc::unbounded_channel();

        loop {
            tokio::select! {
                Some(message) = receiver.recv() => match message {
                    DeepXWebSocketMessage::OrderBook(update) => {
                        match book_sync.validate(&update) {
                            DeepXBookSyncOutcome::Accept => {
                                publish_book_update(&update, &instruments, &data_sender);
                            }
                            DeepXBookSyncOutcome::Recover => {
                                spawn_book_recovery(
                                    update.symbol.clone(),
                                    http_client.clone(),
                                    recovery_tx.clone(),
                                );
                            }
                            DeepXBookSyncOutcome::Suppress => {}
                        }
                    }
                    DeepXWebSocketMessage::Trade(trade) => {
                        let Some(instrument) = instruments.get(&trade.symbol) else {
                            log::error!("No DeepX instrument cached for trade symbol {}", trade.symbol);
                            continue;
                        };
                        let ts_init = get_atomic_clock_realtime().get_time_ns();
                        match parse_trade_tick(&trade, instrument, ts_init) {
                            Ok(tick) => {
                                let _ = data_sender.send(DataEvent::Data(Data::Trade(tick)));
                            }
                            Err(e) => log::error!("Failed to parse DeepX trade: {e}"),
                        }
                    }
                    DeepXWebSocketMessage::Reconnected => book_sync.reset_all(),
                    DeepXWebSocketMessage::SubscriptionConfirmed { .. }
                    | DeepXWebSocketMessage::UnsubscriptionConfirmed { .. } => {}
                },
                Some((symbol, result)) = recovery_rx.recv() => match result {
                    Ok(snapshot) => match book_sync.recover(&snapshot) {
                        Ok(replay) => {
                            publish_book_update(&snapshot, &instruments, &data_sender);
                            for update in replay {
                                publish_book_update(&update, &instruments, &data_sender);
                            }
                        }
                        Err(e) => {
                            log::warn!("DeepX book recovery did not converge for {symbol}: {e}");
                            spawn_book_recovery(
                                symbol,
                                http_client.clone(),
                                recovery_tx.clone(),
                            );
                        }
                    },
                    Err(e) => {
                        log::warn!("Failed to fetch DeepX book recovery snapshot for {symbol}: {e}");
                        spawn_book_recovery(
                            symbol,
                            http_client.clone(),
                            recovery_tx.clone(),
                        );
                    }
                },
                else => break,
            }
        }
    })
}

fn spawn_book_recovery(
    symbol: String,
    http_client: DeepXRawHttpClient,
    recovery_tx: tokio::sync::mpsc::UnboundedSender<(String, anyhow::Result<DeepXOrderBookUpdate>)>,
) {
    get_runtime().spawn(async move {
        let query = DeepXOrderBookQuery {
            limit: None,
            merge_level: None,
        };
        let result = http_client
            .get_perp_order_book(&symbol, &query)
            .await
            .map(|snapshot| DeepXOrderBookUpdate {
                asks: snapshot.asks,
                bids: snapshot.bids,
                engine_time: snapshot.engine_time,
                last_update_id: snapshot.last_update_id,
                prev_last_update_id: None,
                server_time: snapshot.server_time,
                symbol: symbol.clone(),
                update_type: DeepXBookUpdateType::Snapshot,
            })
            .map_err(anyhow::Error::from);
        let _ = recovery_tx.send((symbol, result));
    });
}

fn publish_book_update(
    update: &DeepXOrderBookUpdate,
    instruments: &std::collections::HashMap<String, InstrumentAny>,
    data_sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
) {
    let Some(instrument) = instruments.get(&update.symbol) else {
        log::error!(
            "No DeepX instrument cached for order book symbol {}",
            update.symbol
        );
        return;
    };
    let ts_init = get_atomic_clock_realtime().get_time_ns();
    match parse_order_book_deltas(update, instrument, ts_init) {
        Ok(deltas) => {
            let _ = data_sender.send(DataEvent::Data(Data::Deltas(Box::new(deltas))));
        }
        Err(e) => log::error!("Failed to parse DeepX order book update: {e}"),
    }
}

#[async_trait(?Send)]
impl DataClient for DeepXDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*DEEPX_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.cancel_instrument_refresh();
        self.websocket_client.request_disconnect();
        self.cancel_websocket_task();
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.cancel_instrument_refresh();
        self.websocket_client.request_disconnect();
        self.cancel_websocket_task();
        self.provider.store_mut().clear();
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }

        self.provider.load_all(None).await?;
        for instrument in self.provider.store().list_all() {
            self.data_sender
                .send(DataEvent::Instrument(instrument.clone()))
                .map_err(|e| anyhow::anyhow!("failed to publish DeepX instrument: {e}"))?;
        }
        self.websocket_client.connect().await?;
        let receiver = self.websocket_client.take_receiver()?;
        let instruments = self
            .provider
            .store()
            .list_all()
            .into_iter()
            .map(|instrument| (instrument.raw_symbol().to_string(), instrument.clone()))
            .collect();
        self.websocket_task = Some(spawn_websocket_task(
            receiver,
            self.http_client.clone(),
            instruments,
            self.data_sender.clone(),
        ));
        self.is_connected.store(true, Ordering::Release);
        self.spawn_instrument_refresh();

        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.cancel_instrument_refresh();
        self.websocket_client.disconnect().await;
        if let Some(task) = self.websocket_task.take() {
            task.abort();
            if let Err(e) = task.await
                && !e.is_cancelled()
            {
                log::warn!("Failed to join DeepX data stream task: {e}");
            }
        }
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn subscribe(&mut self, _cmd: SubscribeCustomData) -> anyhow::Result<()> {
        Self::unsupported("custom data subscriptions")
    }

    fn subscribe_instruments(&mut self, _cmd: SubscribeInstruments) -> anyhow::Result<()> {
        Self::unsupported("instrument subscriptions")
    }

    fn subscribe_instrument(&mut self, _cmd: SubscribeInstrument) -> anyhow::Result<()> {
        Self::unsupported("instrument definition subscriptions")
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.book_type == BookType::L2_MBP,
            "DeepX only supports L2_MBP order book deltas"
        );
        let symbol = raw_symbol_from_instrument_id(cmd.instrument_id, DeepXProductType::Perpetual)?;
        self.websocket_client.subscribe_order_book(symbol)
    }

    fn subscribe_book_depth10(&mut self, _cmd: SubscribeBookDepth10) -> anyhow::Result<()> {
        Self::unsupported("order book depth subscriptions")
    }

    fn subscribe_quotes(&mut self, _cmd: SubscribeQuotes) -> anyhow::Result<()> {
        Self::unsupported("quote subscriptions")
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        let symbol = raw_symbol_from_instrument_id(cmd.instrument_id, DeepXProductType::Perpetual)?;
        self.websocket_client.subscribe_trades(symbol)
    }

    fn subscribe_mark_prices(&mut self, _cmd: SubscribeMarkPrices) -> anyhow::Result<()> {
        Self::unsupported("mark price subscriptions")
    }

    fn subscribe_index_prices(&mut self, _cmd: SubscribeIndexPrices) -> anyhow::Result<()> {
        Self::unsupported("index price subscriptions")
    }

    fn subscribe_funding_rates(&mut self, _cmd: SubscribeFundingRates) -> anyhow::Result<()> {
        Self::unsupported("funding rate subscriptions")
    }

    fn subscribe_bars(&mut self, _cmd: SubscribeBars) -> anyhow::Result<()> {
        Self::unsupported("bar subscriptions")
    }

    fn subscribe_instrument_status(
        &mut self,
        _cmd: SubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        Self::unsupported("instrument status subscriptions")
    }

    fn subscribe_instrument_close(&mut self, _cmd: SubscribeInstrumentClose) -> anyhow::Result<()> {
        Self::unsupported("instrument close subscriptions")
    }

    fn subscribe_option_greeks(&mut self, _cmd: SubscribeOptionGreeks) -> anyhow::Result<()> {
        Self::unsupported("option greek subscriptions")
    }

    fn unsubscribe(&mut self, _cmd: &UnsubscribeCustomData) -> anyhow::Result<()> {
        Self::unsupported("custom data unsubscriptions")
    }

    fn unsubscribe_instruments(&mut self, _cmd: &UnsubscribeInstruments) -> anyhow::Result<()> {
        Self::unsupported("instrument unsubscriptions")
    }

    fn unsubscribe_instrument(&mut self, _cmd: &UnsubscribeInstrument) -> anyhow::Result<()> {
        Self::unsupported("instrument definition unsubscriptions")
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        let symbol = raw_symbol_from_instrument_id(cmd.instrument_id, DeepXProductType::Perpetual)?;
        self.websocket_client.unsubscribe_order_book(symbol)
    }

    fn unsubscribe_book_depth10(&mut self, _cmd: &UnsubscribeBookDepth10) -> anyhow::Result<()> {
        Self::unsupported("order book depth unsubscriptions")
    }

    fn unsubscribe_quotes(&mut self, _cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        Self::unsupported("quote unsubscriptions")
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        let symbol = raw_symbol_from_instrument_id(cmd.instrument_id, DeepXProductType::Perpetual)?;
        self.websocket_client.unsubscribe_trades(symbol)
    }

    fn unsubscribe_mark_prices(&mut self, _cmd: &UnsubscribeMarkPrices) -> anyhow::Result<()> {
        Self::unsupported("mark price unsubscriptions")
    }

    fn unsubscribe_index_prices(&mut self, _cmd: &UnsubscribeIndexPrices) -> anyhow::Result<()> {
        Self::unsupported("index price unsubscriptions")
    }

    fn unsubscribe_funding_rates(&mut self, _cmd: &UnsubscribeFundingRates) -> anyhow::Result<()> {
        Self::unsupported("funding rate unsubscriptions")
    }

    fn unsubscribe_bars(&mut self, _cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        Self::unsupported("bar unsubscriptions")
    }

    fn unsubscribe_instrument_status(
        &mut self,
        _cmd: &UnsubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        Self::unsupported("instrument status unsubscriptions")
    }

    fn unsubscribe_instrument_close(
        &mut self,
        _cmd: &UnsubscribeInstrumentClose,
    ) -> anyhow::Result<()> {
        Self::unsupported("instrument close unsubscriptions")
    }

    fn unsubscribe_option_greeks(&mut self, _cmd: &UnsubscribeOptionGreeks) -> anyhow::Result<()> {
        Self::unsupported("option greek unsubscriptions")
    }

    fn request_data(&self, _request: RequestCustomData) -> anyhow::Result<()> {
        Self::unsupported("custom data requests")
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let data_sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start = datetime_to_unix_nanos(request.start);
        let end = datetime_to_unix_nanos(request.end);

        get_runtime().spawn(async move {
            let result = async {
                let markets = http_client.get_perp_markets().await?;
                let ts_init = get_atomic_clock_realtime().get_time_ns();
                let instruments = markets
                    .iter()
                    .map(|market| parse_perpetual_instrument(market, ts_init))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let response = InstrumentsResponse::new(
                    request.request_id,
                    client_id,
                    *DEEPX_VENUE,
                    instruments,
                    start,
                    end,
                    ts_init,
                    request.params,
                );
                data_sender.send(DataEvent::Response(DataResponse::Instruments(response)))?;
                anyhow::Ok(())
            }
            .await;

            if let Err(error) = result {
                log::error!("Failed to request DeepX instruments: {error}");
            }
        });

        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let raw_symbol =
            raw_symbol_from_instrument_id(request.instrument_id, DeepXProductType::Perpetual)?;
        let http_client = self.http_client.clone();
        let data_sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start = datetime_to_unix_nanos(request.start);
        let end = datetime_to_unix_nanos(request.end);

        get_runtime().spawn(async move {
            let result = async {
                let market = http_client.get_perp_market(&raw_symbol).await?;
                let ts_init = get_atomic_clock_realtime().get_time_ns();
                let instrument = parse_perpetual_instrument(&market, ts_init)?;
                anyhow::ensure!(
                    instrument.id() == request.instrument_id,
                    "DeepX returned instrument `{}` for requested `{}`",
                    instrument.id(),
                    request.instrument_id,
                );
                let response = InstrumentResponse::new(
                    request.request_id,
                    client_id,
                    request.instrument_id,
                    instrument,
                    start,
                    end,
                    ts_init,
                    request.params,
                );
                data_sender.send(DataEvent::Response(DataResponse::Instrument(Box::new(
                    response,
                ))))?;
                anyhow::Ok(())
            }
            .await;

            if let Err(error) = result {
                log::error!(
                    "Failed to request DeepX instrument {}: {error}",
                    request.instrument_id,
                );
            }
        });

        Ok(())
    }

    fn request_book_snapshot(&self, request: RequestBookSnapshot) -> anyhow::Result<()> {
        let raw_symbol =
            raw_symbol_from_instrument_id(request.instrument_id, DeepXProductType::Perpetual)?;
        let limit = request
            .depth
            .map(|depth| u16::try_from(depth.get()))
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid DeepX order book depth: {e}"))?;
        let query = DeepXOrderBookQuery {
            limit,
            merge_level: None,
        };
        query.validate()?;

        let http_client = self.http_client.clone();
        let data_sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);

        get_runtime().spawn(async move {
            let result = async {
                let market = http_client.get_perp_market(&raw_symbol).await?;
                let snapshot = http_client.get_perp_order_book(&raw_symbol, &query).await?;
                let ts_init = get_atomic_clock_realtime().get_time_ns();
                let instrument = parse_perpetual_instrument(&market, ts_init)?;
                anyhow::ensure!(
                    instrument.id() == request.instrument_id,
                    "DeepX returned instrument `{}` for requested `{}`",
                    instrument.id(),
                    request.instrument_id,
                );
                let update = DeepXOrderBookUpdate {
                    asks: snapshot.asks,
                    bids: snapshot.bids,
                    engine_time: snapshot.engine_time,
                    last_update_id: snapshot.last_update_id,
                    prev_last_update_id: None,
                    server_time: snapshot.server_time,
                    symbol: raw_symbol,
                    update_type: DeepXBookUpdateType::Snapshot,
                };
                let deltas = parse_order_book_deltas(&update, &instrument, ts_init)?;
                let mut book = OrderBook::new(request.instrument_id, BookType::L2_MBP);
                book.apply_deltas(&deltas)?;
                let response = BookResponse::new(
                    request.request_id,
                    client_id,
                    request.instrument_id,
                    book,
                    None,
                    None,
                    ts_init,
                    request.params,
                );
                data_sender.send(DataEvent::Response(DataResponse::Book(response)))?;
                anyhow::Ok(())
            }
            .await;

            if let Err(error) = result {
                log::error!(
                    "Failed to request DeepX order book snapshot for {}: {error}",
                    request.instrument_id,
                );
            }
        });

        Ok(())
    }

    fn request_quotes(&self, _request: RequestQuotes) -> anyhow::Result<()> {
        Self::unsupported("historical quote requests")
    }

    fn request_trades(&self, _request: RequestTrades) -> anyhow::Result<()> {
        Self::unsupported("historical trade requests")
    }

    fn request_funding_rates(&self, _request: RequestFundingRates) -> anyhow::Result<()> {
        Self::unsupported("historical funding rate requests")
    }

    fn request_forward_prices(&self, _request: RequestForwardPrices) -> anyhow::Result<()> {
        Self::unsupported("forward price requests")
    }

    fn request_bars(&self, _request: RequestBars) -> anyhow::Result<()> {
        Self::unsupported("historical bar requests")
    }

    fn request_book_depth(&self, _request: RequestBookDepth) -> anyhow::Result<()> {
        Self::unsupported("historical order book depth requests")
    }

    fn request_book_deltas(&self, _request: RequestBookDeltas) -> anyhow::Result<()> {
        Self::unsupported("historical order book delta requests")
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        Json, Router,
        extract::ws::{WebSocket, WebSocketUpgrade},
        http::StatusCode,
        response::Response,
        routing::get,
    };
    use nautilus_common::{
        clients::DataClient,
        live::runner::replace_data_event_sender,
        messages::{
            DataEvent, DataResponse,
            data::{
                RequestBookSnapshot, RequestInstrument, RequestInstruments, SubscribeInstruments,
            },
        },
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        data::Data,
        enums::{AggressorSide, BookAction, RecordFlag},
        identifiers::{ClientId, InstrumentId},
        instruments::Instrument,
    };
    use rstest::rstest;

    use super::*;
    use crate::{
        http::models::DeepXPerpetualMarket,
        websocket::{
            client::DeepXWebSocketMessage,
            messages::{DeepXOrderBookUpdate, DeepXTrade},
        },
    };

    fn market_json() -> serde_json::Value {
        serde_json::json!({
            "baseAsset": "ETH",
            "makerFeeRate": "-0.0001",
            "marketId": 3,
            "maxOpenOrders": 128,
            "minNotional": "1",
            "minQty": "0.001",
            "orderTypes": ["LIMIT", "MARKET"],
            "quoteAsset": "USDC",
            "status": "TRADING",
            "stepSize": "0.001",
            "symbol": "ETH-USDC",
            "takerFeeRate": "0.0002",
            "tickSize": "0.01"
        })
    }

    async fn websocket(upgrade: WebSocketUpgrade) -> Response {
        upgrade.on_upgrade(hold_websocket)
    }

    async fn hold_websocket(mut socket: WebSocket) {
        while socket.recv().await.is_some() {}
    }

    #[rstest]
    #[tokio::test]
    async fn publishes_public_trade_as_typed_data_event() {
        let market: DeepXPerpetualMarket = serde_json::from_value(market_json()).unwrap();
        let instrument = parse_perpetual_instrument(&market, UnixNanos::default()).unwrap();
        let instruments = std::collections::HashMap::from([(market.symbol.clone(), instrument)]);
        let http_client =
            DeepXRawHttpClient::new(Some("http://127.0.0.1:1".to_string()), 1, None).unwrap();
        let (message_tx, message_rx) = tokio::sync::mpsc::unbounded_channel();
        let (data_tx, mut data_rx) = tokio::sync::mpsc::unbounded_channel();
        let task = spawn_websocket_task(message_rx, http_client, instruments, data_tx);
        let trade: DeepXTrade = serde_json::from_value(serde_json::json!({
            "buyOrderId": "6652970",
            "id": "159078803000024",
            "makerFee": "-0.02529",
            "marketId": 3,
            "price": "2455.7",
            "qty": "0.103",
            "quoteQty": "252.9371",
            "sellOrderId": "2263022",
            "symbol": "ETH-USDC",
            "takerFee": "0.050581",
            "takerSide": "SELL",
            "time": 1787562119833_u64
        }))
        .unwrap();

        message_tx
            .send(DeepXWebSocketMessage::Trade(trade))
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), data_rx.recv())
            .await
            .expect("DeepX trade publication timed out")
            .expect("DeepX data stream closed before publishing the trade");

        let DataEvent::Data(Data::Trade(tick)) = event else {
            panic!("expected trade data event")
        };
        assert_eq!(
            tick.instrument_id,
            InstrumentId::from("ETH-USDC-PERP.DEEPX")
        );
        assert_eq!(tick.price.to_string(), "2455.70");
        assert_eq!(tick.size.to_string(), "0.103");
        assert_eq!(tick.aggressor_side, AggressorSide::Sell);
        assert_eq!(tick.trade_id.as_str(), "159078803000024");
        assert_eq!(tick.ts_event, UnixNanos::from(1_787_562_119_833_000_000));

        task.abort();
    }

    #[rstest]
    #[tokio::test]
    async fn publishes_snapshot_and_contiguous_delta_as_typed_data_events() {
        let market: DeepXPerpetualMarket = serde_json::from_value(market_json()).unwrap();
        let instrument = parse_perpetual_instrument(&market, UnixNanos::default()).unwrap();
        let instruments = std::collections::HashMap::from([(market.symbol.clone(), instrument)]);
        let http_client =
            DeepXRawHttpClient::new(Some("http://127.0.0.1:1".to_string()), 1, None).unwrap();
        let (message_tx, message_rx) = tokio::sync::mpsc::unbounded_channel();
        let (data_tx, mut data_rx) = tokio::sync::mpsc::unbounded_channel();
        let task = spawn_websocket_task(message_rx, http_client, instruments, data_tx);
        let snapshot: DeepXOrderBookUpdate = serde_json::from_str(
            r#"{"asks":[["2457.02","0.859"]],"bids":[["2456.81","0.291"]],"engineTime":1787562160783,"lastUpdateId":62493922,"prevLastUpdateId":null,"serverTime":1787562160833,"symbol":"ETH-USDC","updateType":"snapshot"}"#,
        )
        .unwrap();
        let delta: DeepXOrderBookUpdate = serde_json::from_str(
            r#"{"asks":[["2457.02","0"]],"bids":[],"engineTime":1787562162051,"lastUpdateId":62494052,"prevLastUpdateId":62493922,"serverTime":1787562162052,"symbol":"ETH-USDC","updateType":"delta"}"#,
        )
        .unwrap();

        message_tx
            .send(DeepXWebSocketMessage::OrderBook(snapshot))
            .unwrap();
        message_tx
            .send(DeepXWebSocketMessage::OrderBook(delta))
            .unwrap();

        let snapshot_event = tokio::time::timeout(Duration::from_secs(1), data_rx.recv())
            .await
            .expect("DeepX snapshot publication timed out")
            .expect("DeepX data stream closed before publishing the snapshot");
        let DataEvent::Data(Data::Deltas(snapshot_deltas)) = snapshot_event else {
            panic!("expected snapshot deltas data event")
        };
        assert_eq!(
            snapshot_deltas.instrument_id,
            InstrumentId::from("ETH-USDC-PERP.DEEPX")
        );
        assert_eq!(snapshot_deltas.deltas.len(), 3);
        assert_eq!(snapshot_deltas.deltas[0].action, BookAction::Clear);
        assert_eq!(snapshot_deltas.deltas[2].sequence, 62_493_922);
        assert_eq!(
            snapshot_deltas.deltas[2].flags,
            RecordFlag::F_SNAPSHOT as u8 | RecordFlag::F_LAST as u8
        );

        let delta_event = tokio::time::timeout(Duration::from_secs(1), data_rx.recv())
            .await
            .expect("DeepX delta publication timed out")
            .expect("DeepX data stream closed before publishing the delta");
        let DataEvent::Data(Data::Deltas(delta_deltas)) = delta_event else {
            panic!("expected delta data event")
        };
        assert_eq!(delta_deltas.deltas.len(), 1);
        assert_eq!(delta_deltas.deltas[0].action, BookAction::Delete);
        assert_eq!(delta_deltas.deltas[0].sequence, 62_494_052);
        assert_eq!(delta_deltas.deltas[0].flags, RecordFlag::F_LAST as u8);

        task.abort();
    }

    #[rstest]
    #[tokio::test]
    async fn recovers_order_book_gap_with_rest_snapshot_and_replays_buffered_delta() {
        let router = Router::new().route(
            "/v1/perp/markets/ETH-USDC/orderbook",
            get(|| async {
                Json(serde_json::json!({
                    "asks": [["2457.02", "0.859"]],
                    "bids": [["2456.81", "0.291"]],
                    "engineTime": 1787562160783_u64,
                    "lastUpdateId": 100_u64,
                    "serverTime": 1787562160833_u64
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let market: DeepXPerpetualMarket = serde_json::from_value(market_json()).unwrap();
        let instrument = parse_perpetual_instrument(&market, UnixNanos::default()).unwrap();
        let instruments = std::collections::HashMap::from([(market.symbol.clone(), instrument)]);
        let http_client =
            DeepXRawHttpClient::new(Some(format!("http://{address}")), 1, None).unwrap();
        let (message_tx, message_rx) = tokio::sync::mpsc::unbounded_channel();
        let (data_tx, mut data_rx) = tokio::sync::mpsc::unbounded_channel();
        let task = spawn_websocket_task(message_rx, http_client, instruments, data_tx);
        let delta: DeepXOrderBookUpdate = serde_json::from_str(
            r#"{"asks":[["2457.02","0"]],"bids":[],"engineTime":1787562162051,"lastUpdateId":101,"prevLastUpdateId":100,"serverTime":1787562162052,"symbol":"ETH-USDC","updateType":"delta"}"#,
        )
        .unwrap();

        message_tx
            .send(DeepXWebSocketMessage::OrderBook(delta))
            .unwrap();

        let snapshot_event = tokio::time::timeout(Duration::from_secs(1), data_rx.recv())
            .await
            .expect("DeepX recovery snapshot publication timed out")
            .expect("DeepX data stream closed before publishing the recovery snapshot");
        let DataEvent::Data(Data::Deltas(snapshot_deltas)) = snapshot_event else {
            panic!("expected recovery snapshot deltas data event")
        };
        assert_eq!(snapshot_deltas.deltas.last().unwrap().sequence, 100);

        let replay_event = tokio::time::timeout(Duration::from_secs(1), data_rx.recv())
            .await
            .expect("DeepX buffered delta replay timed out")
            .expect("DeepX data stream closed before replaying the buffered delta");
        let DataEvent::Data(Data::Deltas(replay_deltas)) = replay_event else {
            panic!("expected replayed deltas data event")
        };
        assert_eq!(replay_deltas.deltas.len(), 1);
        assert_eq!(replay_deltas.deltas[0].action, BookAction::Delete);
        assert_eq!(replay_deltas.deltas[0].sequence, 101);

        task.abort();
    }

    async fn client(
        router: Router,
    ) -> (
        DeepXDataClient,
        tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = router.route("/ws", get(websocket));
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(sender);
        let client = DeepXDataClient::new(
            ClientId::from("DEEPX"),
            DeepXDataClientConfig::builder()
                .base_url_rest(format!("http://{address}"))
                .base_url_ws(format!("ws://{address}/ws"))
                .build(),
        )
        .unwrap();

        (client, receiver)
    }

    #[rstest]
    #[tokio::test]
    async fn connects_after_loading_perpetual_instruments() {
        let router = Router::new().route(
            "/v1/perp/markets",
            get(|| async { Json(vec![market_json()]) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = router.route("/ws", get(websocket));
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(sender);
        let mut client = DeepXDataClient::new(
            ClientId::from("DEEPX"),
            DeepXDataClientConfig::builder()
                .base_url_rest(format!("http://{address}"))
                .base_url_ws(format!("ws://{address}/ws"))
                .build(),
        )
        .unwrap();

        client.connect().await.unwrap();

        assert!(client.is_connected());
        assert_eq!(client.client_id(), ClientId::from("DEEPX"));
        assert_eq!(client.venue(), Some(*DEEPX_VENUE));
        let event = receiver.try_recv().unwrap();
        let DataEvent::Instrument(instrument) = event else {
            panic!("expected instrument event")
        };
        assert_eq!(instrument.id(), InstrumentId::from("ETH-USDC-PERP.DEEPX"));
    }

    #[rstest]
    #[tokio::test]
    async fn connect_is_idempotent() {
        let router = Router::new().route(
            "/v1/perp/markets",
            get(|| async { Json(vec![market_json()]) }),
        );
        let (mut client, _receiver) = client(router).await;

        client.connect().await.unwrap();
        client.connect().await.unwrap();

        assert!(client.is_connected());
    }

    #[rstest]
    #[tokio::test]
    async fn instrument_refresh_is_disabled_when_interval_is_zero() {
        let router = Router::new().route(
            "/v1/perp/markets",
            get(|| async { Json(vec![market_json()]) }),
        );
        let (mut client, _receiver) = client(router).await;
        client.update_instruments_interval_mins = 0;

        client.connect().await.unwrap();

        assert!(client.instrument_refresh_task.is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn disconnect_cancels_instrument_refresh() {
        let router = Router::new().route(
            "/v1/perp/markets",
            get(|| async { Json(vec![market_json()]) }),
        );
        let (mut client, _receiver) = client(router).await;

        tokio::time::timeout(Duration::from_secs(10), client.connect())
            .await
            .expect("DeepX test connection timed out")
            .unwrap();
        assert!(client.instrument_refresh_task.is_some());

        tokio::time::timeout(Duration::from_secs(10), client.disconnect())
            .await
            .expect("DeepX test disconnection timed out")
            .unwrap();

        assert!(client.instrument_refresh_task.is_none());
        assert!(client.is_disconnected());
    }

    #[rstest]
    #[tokio::test]
    async fn failed_bootstrap_leaves_client_disconnected() {
        let router = Router::new().route(
            "/v1/perp/markets",
            get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        );
        let (mut client, _receiver) = client(router).await;

        let result = client.connect().await;

        assert!(result.is_err());
        assert!(client.is_disconnected());
    }

    #[rstest]
    #[tokio::test]
    async fn reset_clears_connection_state() {
        let (mut client, _receiver) = client(Router::new()).await;
        client.is_connected.store(true, Ordering::Release);

        client.reset().unwrap();

        assert!(client.is_disconnected());
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_unsupported_instrument_subscription() {
        let (mut client, _receiver) = client(Router::new()).await;
        let result = client.subscribe_instruments(SubscribeInstruments::new(
            Some(ClientId::from("DEEPX")),
            *DEEPX_VENUE,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ));

        assert_eq!(
            result.unwrap_err().to_string(),
            "DeepX instrument subscriptions is not supported"
        );
        assert!(client.is_disconnected());
    }

    #[rstest]
    #[tokio::test]
    async fn requests_fresh_perpetual_instruments() {
        let router = Router::new().route(
            "/v1/perp/markets",
            get(|| async { Json(vec![market_json()]) }),
        );
        let (client, mut receiver) = client(router).await;
        let request_id = UUID4::new();

        client
            .request_instruments(RequestInstruments::new(
                None,
                None,
                Some(ClientId::from("DEEPX")),
                None,
                request_id,
                UnixNanos::default(),
                None,
            ))
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let DataEvent::Response(DataResponse::Instruments(response)) = event else {
            panic!("expected instruments response")
        };
        assert_eq!(response.correlation_id, request_id);
        assert_eq!(response.data.len(), 1);
        assert_eq!(
            response.data[0].id(),
            InstrumentId::from("ETH-USDC-PERP.DEEPX")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn requests_fresh_perpetual_instrument() {
        let router = Router::new().route(
            "/v1/perp/markets/ETH-USDC",
            get(|| async { Json(market_json()) }),
        );
        let (client, mut receiver) = client(router).await;
        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");
        let request_id = UUID4::new();

        client
            .request_instrument(RequestInstrument::new(
                instrument_id,
                None,
                None,
                None,
                request_id,
                UnixNanos::default(),
                None,
            ))
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let DataEvent::Response(DataResponse::Instrument(response)) = event else {
            panic!("expected instrument response")
        };
        assert_eq!(response.correlation_id, request_id);
        assert_eq!(response.instrument_id, instrument_id);
        assert_eq!(response.data.id(), instrument_id);
    }

    #[rstest]
    #[tokio::test]
    async fn requests_perpetual_book_snapshot() {
        let router = Router::new()
            .route(
                "/v1/perp/markets/ETH-USDC",
                get(|| async { Json(market_json()) }),
            )
            .route(
                "/v1/perp/markets/ETH-USDC/orderbook",
                get(
                    |axum::extract::Query(query): axum::extract::Query<DeepXOrderBookQuery>| async move {
                        assert_eq!(query.limit, Some(50));
                        Json(serde_json::json!({
                            "asks": [["2453.76", "3.282"]],
                            "bids": [["2453.75", "0.842"]],
                            "engineTime": 1787561622066_u64,
                            "lastUpdateId": 62430652_u64,
                            "serverTime": 1787561622211_u64
                        }))
                    },
                ),
            );
        let (client, mut receiver) = client(router).await;
        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");
        let request_id = UUID4::new();

        client
            .request_book_snapshot(RequestBookSnapshot::new(
                instrument_id,
                std::num::NonZeroUsize::new(50),
                None,
                request_id,
                UnixNanos::default(),
                None,
            ))
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let DataEvent::Response(DataResponse::Book(response)) = event else {
            panic!("expected book response")
        };
        assert_eq!(response.correlation_id, request_id);
        assert_eq!(response.instrument_id, instrument_id);
        assert_eq!(
            response.data.best_bid_price().unwrap().to_string(),
            "2453.75"
        );
        assert_eq!(response.data.best_bid_size().unwrap().to_string(), "0.842");
        assert_eq!(
            response.data.best_ask_price().unwrap().to_string(),
            "2453.76"
        );
        assert_eq!(response.data.best_ask_size().unwrap().to_string(), "3.282");
        assert_eq!(response.data.sequence, 62_430_652);
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_spot_book_snapshot_request() {
        let (client, _receiver) = client(Router::new()).await;

        let result = client.request_book_snapshot(RequestBookSnapshot::new(
            InstrumentId::from("ETH-USDC.DEEPX"),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        ));

        assert!(result.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_book_snapshot_depth_above_protocol_limit() {
        let (client, _receiver) = client(Router::new()).await;

        let result = client.request_book_snapshot(RequestBookSnapshot::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            std::num::NonZeroUsize::new(501),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        ));

        assert!(result.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_spot_instrument_request() {
        let (client, _receiver) = client(Router::new()).await;

        let result = client.request_instrument(RequestInstrument::new(
            InstrumentId::from("ETH-USDC.DEEPX"),
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        ));

        assert!(result.is_err());
    }
}
