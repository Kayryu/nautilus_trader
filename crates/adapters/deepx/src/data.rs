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

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::{runner::get_data_event_sender, runtime::get_runtime},
    messages::{
        DataEvent, DataResponse,
        data::{
            BookResponse, InstrumentResponse, InstrumentsResponse, RequestBookSnapshot,
            RequestInstrument, RequestInstruments,
        },
    },
    providers::InstrumentProvider,
};
use nautilus_core::{datetime::datetime_to_unix_nanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    enums::BookType,
    identifiers::{ClientId, Venue},
    instruments::Instrument,
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
        enums::DeepXBookUpdateType, messages::DeepXOrderBookUpdate, parse::parse_order_book_deltas,
    },
};

/// DeepX data client for verified perpetual instrument metadata.
///
/// Public streaming remains unavailable until the WebSocket lifecycle and
/// subscription acknowledgment schemas are verified.
#[derive(Debug)]
pub struct DeepXDataClient {
    client_id: ClientId,
    http_client: DeepXRawHttpClient,
    provider: DeepXInstrumentProvider,
    is_connected: AtomicBool,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
}

impl DeepXDataClient {
    /// Creates a new [`DeepXDataClient`] instance.
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be initialized.
    pub fn new(client_id: ClientId, config: DeepXDataClientConfig) -> anyhow::Result<Self> {
        let http_client = DeepXRawHttpClient::new(
            Some(config.rest_url()),
            config.http_timeout_secs,
            config.proxy_url,
        )?;

        Ok(Self {
            client_id,
            provider: DeepXInstrumentProvider::new(http_client.clone()),
            http_client,
            is_connected: AtomicBool::new(false),
            data_sender: get_data_event_sender(),
        })
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
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
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
        self.is_connected.store(true, Ordering::Release);

        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.is_connected.store(false, Ordering::Release);
        Ok(())
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
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, http::StatusCode, routing::get};
    use nautilus_common::{
        clients::DataClient,
        live::runner::replace_data_event_sender,
        messages::{
            DataEvent, DataResponse,
            data::{RequestBookSnapshot, RequestInstrument, RequestInstruments},
        },
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        identifiers::{ClientId, InstrumentId},
        instruments::Instrument,
    };
    use rstest::rstest;

    use super::*;

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

    async fn client(
        router: Router,
    ) -> (
        DeepXDataClient,
        tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(sender);
        let client = DeepXDataClient::new(
            ClientId::from("DEEPX"),
            DeepXDataClientConfig::builder()
                .base_url_rest(format!("http://{address}"))
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
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(sender);
        let mut client = DeepXDataClient::new(
            ClientId::from("DEEPX"),
            DeepXDataClientConfig::builder()
                .base_url_rest(format!("http://{address}"))
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
