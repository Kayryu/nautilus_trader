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

//! Spot book snapshots streamed by the node's orderbook JSON-RPC subscription.

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use nautilus_common::messages::DataEvent;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    data::{Data, OrderBookDelta, OrderBookDeltas, OrderBookDepth10},
    enums::RecordFlag,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::transport::Message;
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

use crate::{
    config::DeepXNetworkConfig,
    data::{book, live_trades::TradeSubscription},
    http::DeepXSpotMarket,
    websocket::{DeepXWsError, orderbook_rpc, transport::DeepXWsReadConnection},
};

const MAX_RECONNECT_ATTEMPTS: usize = 5;
const MAX_RPC_DEPTH: usize = 1000;
const SUBSCRIBE_REQUEST_ID: u64 = 1;
const UNSUBSCRIBE_REQUEST_ID: u64 = 2;

struct SubscriptionGuard(TradeSubscription);

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.0.retire();
    }
}

pub(super) async fn request_spot_book_depth10(
    network: DeepXNetworkConfig,
    proxy_url: Option<String>,
    timeout: Duration,
    market: DeepXSpotMarket,
    instrument: InstrumentAny,
    depth: NonZeroUsize,
) -> anyhow::Result<OrderBookDepth10> {
    let url = orderbook_rpc::websocket_url(&network)?;
    let mut connection =
        DeepXWsReadConnection::connect_url(&url, proxy_url.as_deref(), timeout).await?;
    let result = async {
        let request = json!({
            "jsonrpc": "2.0",
            "id": SUBSCRIBE_REQUEST_ID,
            "method": "orderbook_subscribe",
            "params": [[market.pair], [], depth.get()],
        });
        connection.send_json_rpc_text(request.to_string()).await?;
        tokio::time::timeout(timeout, async {
            let mut subscription_id = None;
            loop {
                let message = match connection.receive().await {
                    Err(e)
                        if e.downcast_ref::<DeepXWsError>()
                            == Some(&DeepXWsError::ReceiveTimeout) =>
                    {
                        continue;
                    }
                    other => other?,
                };
                let message =
                    message.ok_or_else(|| anyhow::anyhow!("DeepX Spot depth connection closed"))?;
                if matches!(message, Message::Ping(_) | Message::Pong(_)) {
                    continue;
                }
                let text = message
                    .as_text()
                    .ok_or_else(|| anyhow::anyhow!("DeepX Spot depth received non-text data"))?;
                let value: Value = serde_json::from_str(text)?;
                if subscription_id.is_none() {
                    anyhow::ensure!(
                        value.get("id").is_some(),
                        "DeepX Spot depth notification preceded its acknowledgement"
                    );
                    subscription_id = Some(orderbook_rpc::parse_subscribe_ack(
                        &value,
                        SUBSCRIBE_REQUEST_ID,
                    )?);
                    continue;
                }
                let snapshot = orderbook_rpc::parse_notification(
                    &value,
                    subscription_id.as_deref().expect("acknowledged above"),
                )?;
                let pair = book::spot_rpc_pair(&snapshot, &market)?;
                anyhow::ensure!(
                    pair.bids.len() <= depth.get() && pair.asks.len() <= depth.get(),
                    "DeepX Spot depth snapshot exceeds requested depth"
                );
                let ts_init = get_atomic_clock_realtime().get_time_ns();
                return book::parse_spot_rpc_book_depth10(
                    &snapshot,
                    &market,
                    &instrument,
                    1,
                    ts_init,
                );
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("DeepX Spot depth initial data deadline exceeded"))?
    }
    .await;
    let close = connection.close().await;
    result.and_then(|depth| close.map(|()| depth))
}

#[derive(Clone, Copy)]
pub(super) enum SpotBookOutputKind {
    Deltas,
    Depth10,
    Quotes,
}

pub(super) struct SpotBookTask {
    pub instrument: InstrumentAny,
    pub market: Box<DeepXSpotMarket>,
    pub depth: NonZeroUsize,
    pub output: SpotBookOutputKind,
    pub network: DeepXNetworkConfig,
    pub proxy_url: Option<String>,
    pub timeout: Duration,
    pub sender: UnboundedSender<DataEvent>,
    pub connection_epoch: Arc<Mutex<bool>>,
    pub subscription: TradeSubscription,
}

impl SpotBookTask {
    pub(super) async fn run(self) {
        let _guard = SubscriptionGuard(self.subscription.clone());
        let cancellation = self.subscription.cancellation_token();
        for attempt in 0..=MAX_RECONNECT_ATTEMPTS {
            let result = self.run_connection(&cancellation).await;
            if cancellation.is_cancelled() || self.sender.is_closed() {
                return;
            }
            if let Err(e) = result {
                if let Err(clear_error) = self.publish_clear() {
                    log::error!("DeepX Spot book invalidation failed: {clear_error}");
                    return;
                }
                if attempt == MAX_RECONNECT_ATTEMPTS {
                    log::error!("DeepX Spot book stream stopped after bounded recovery: {e}");
                    return;
                }
                log::warn!(
                    "DeepX Spot book stream interrupted; reconnecting from a full snapshot: {e}"
                );
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return,
                    () = self.sender.closed() => return,
                    () = tokio::time::sleep(Duration::from_millis(250 * (1 << attempt))) => {}
                }
            }
        }
    }

    async fn run_connection(&self, cancellation: &CancellationToken) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.depth.get() <= MAX_RPC_DEPTH,
            "DeepX Spot book depth exceeds node RPC limit"
        );
        let url = orderbook_rpc::websocket_url(&self.network)?;
        let mut connection = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            () = self.sender.closed() => return Ok(()),
            result = DeepXWsReadConnection::connect_url(&url, self.proxy_url.as_deref(), self.timeout) => result?,
        };
        let pair = self
            .market
            .pair
            .strip_prefix("0x")
            .unwrap_or(&self.market.pair);
        let request = json!({
            "jsonrpc": "2.0",
            "id": SUBSCRIBE_REQUEST_ID,
            "method": "orderbook_subscribe",
            "params": [[format!("0x{pair}")], [], self.depth.get()],
        });
        connection.send_json_rpc_text(request.to_string()).await?;

        let result = self.read_subscription(&mut connection, cancellation).await;
        let close = connection.close().await;
        result.and(close)
    }

    async fn read_subscription(
        &self,
        connection: &mut DeepXWsReadConnection,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<()> {
        let mut subscription_id = None;
        loop {
            let message = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    if let Some(subscription_id) = subscription_id.as_deref() {
                        self.unsubscribe(connection, subscription_id).await;
                    }
                    return Ok(());
                }
                () = self.sender.closed() => return Ok(()),
                result = tokio::time::timeout(self.timeout, connection.receive()) => {
                    match result {
                        Err(_) => anyhow::bail!("DeepX Spot book RPC receive timed out"),
                        Ok(Err(e)) if e.downcast_ref::<DeepXWsError>()
                            == Some(&DeepXWsError::ReceiveTimeout) => continue,
                        Ok(result) => result?,
                    }
                }
            };
            let Some(message) = message else {
                anyhow::bail!("DeepX Spot book RPC connection closed");
            };
            let Some(text) = message.as_text() else {
                continue;
            };
            let value: Value =
                serde_json::from_str(text).context("invalid DeepX Spot book RPC JSON")?;
            if subscription_id.is_none() {
                if value.get("id").is_some() {
                    subscription_id = Some(orderbook_rpc::parse_subscribe_ack(
                        &value,
                        SUBSCRIBE_REQUEST_ID,
                    )?);
                }
                continue;
            }
            if value.get("method").is_none() {
                continue;
            }
            let snapshot = orderbook_rpc::parse_notification(
                &value,
                subscription_id
                    .as_deref()
                    .expect("subscription acknowledged"),
            )?;
            let sequence = self.subscription.next_book_sequence()?;
            let ts_init = get_atomic_clock_realtime().get_time_ns();
            let events = match self.output {
                SpotBookOutputKind::Deltas => vec![DataEvent::Data(Data::from(
                    book::parse_spot_rpc_book_deltas(
                        &snapshot,
                        &self.market,
                        &self.instrument,
                        self.depth,
                        sequence,
                        ts_init,
                    )?,
                ))],
                SpotBookOutputKind::Depth10 => vec![DataEvent::Data(Data::Depth10(Box::new(
                    book::parse_spot_rpc_book_depth10(
                        &snapshot,
                        &self.market,
                        &self.instrument,
                        sequence,
                        ts_init,
                    )?,
                )))],
                SpotBookOutputKind::Quotes => book::parse_spot_rpc_quote_tick(
                    &snapshot,
                    &self.market,
                    &self.instrument,
                    ts_init,
                )?
                .map(|quote| vec![DataEvent::Data(Data::Quote(quote))])
                .unwrap_or_default(),
            };
            self.subscription
                .publish(&self.connection_epoch, &self.sender, events)?;
        }
    }

    async fn unsubscribe(&self, connection: &mut DeepXWsReadConnection, subscription: &str) {
        let request = json!({
            "jsonrpc": "2.0",
            "id": UNSUBSCRIBE_REQUEST_ID,
            "method": "orderbook_unsubscribe",
            "params": [subscription],
        });
        if let Err(e) = connection.send_json_rpc_text(request.to_string()).await {
            log::debug!("DeepX Spot book unsubscribe failed during retirement: {e}");
        }
    }

    fn publish_clear(&self) -> anyhow::Result<()> {
        let ts = get_atomic_clock_realtime().get_time_ns();
        let sequence = self.subscription.next_book_sequence()?;
        let mut clear = OrderBookDelta::clear(self.instrument.id(), sequence, ts, ts);
        clear.flags |= RecordFlag::F_LAST as u8;
        self.subscription.publish(
            &self.connection_epoch,
            &self.sender,
            vec![DataEvent::Data(Data::from(OrderBookDeltas::new(
                Instrument::id(&self.instrument),
                vec![clear],
            )))],
        )
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::CurrencyType,
        identifiers::{InstrumentId, Symbol},
        instruments::CurrencyPair,
        types::{Currency, Price, Quantity},
    };
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    const SPOT_MARKETS: &str = include_str!("../../test_data/http/testnet/spot_markets.json");

    struct Server(tokio::task::JoinHandle<()>);

    impl Drop for Server {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

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
                .ts_event(nautilus_core::UnixNanos::default())
                .ts_init(nautilus_core::UnixNanos::default())
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn subscribes_publishes_full_spot_book_and_unsubscribes() {
        use axum::{
            Router,
            extract::ws::{Message as ServerMessage, WebSocketUpgrade},
            routing::get,
        };

        let response: crate::http::DeepXApiResponse<Vec<DeepXSpotMarket>> =
            serde_json::from_str(SPOT_MARKETS).unwrap();
        let market = Box::new(
            response
                .data
                .into_iter()
                .find(|market| market.name == "ETH/USDC")
                .unwrap(),
        );
        let pair = market.pair.clone();
        let (unsubscribe_sender, unsubscribe_receiver) = tokio::sync::oneshot::channel();
        let unsubscribe_sender = Arc::new(tokio::sync::Mutex::new(Some(unsubscribe_sender)));
        let app = Router::new().route(
            "/rpc",
            get(move |upgrade: WebSocketUpgrade| {
                let pair = pair.clone();
                let unsubscribe_sender = Arc::clone(&unsubscribe_sender);
                async move {
                    upgrade.on_upgrade(move |mut socket| async move {
                        let Some(Ok(ServerMessage::Text(request))) = socket.recv().await else {
                            return;
                        };
                        let request: Value = serde_json::from_str(&request).unwrap();
                        assert_eq!(request["method"], "orderbook_subscribe");
                        assert_eq!(request["params"][0][0], pair);
                        assert_eq!(request["params"][1], json!([]));
                        assert_eq!(request["params"][2], 2);
                        socket
                            .send(ServerMessage::Text(
                                json!({"jsonrpc":"2.0","id":1,"result":"spot-book-1"})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .unwrap();
                        socket
                            .send(ServerMessage::Text(
                                json!({
                                    "jsonrpc":"2.0",
                                    "method":"orderbook_subscribe",
                                    "params":{
                                        "subscription":"spot-book-1",
                                        "result":{
                                            "spot":{
                                                (pair):{
                                                    "bids":[{"price":"0x912d5ff0","amount":"0xc8819b7382d8000","order_count":3}],
                                                    "asks":[{"price":"0x912fa9e0","amount":"0x1647e1be62e3c000","order_count":4}]
                                                }
                                            },
                                            "perp":{}
                                        }
                                    }
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                        if let Some(Ok(ServerMessage::Text(unsubscribe))) = socket.recv().await {
                            let request: Value = serde_json::from_str(&unsubscribe).unwrap();
                            if request["method"] == "orderbook_unsubscribe"
                                && let Some(sender) = unsubscribe_sender.lock().await.take()
                            {
                                let _ = sender.send(());
                            }
                        }
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = Server(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let network = DeepXNetworkConfig {
            base_url_rpc_watch: Some(format!("http://{address}/rpc")),
            ..Default::default()
        };
        let subscription = TradeSubscription::new();
        let (sender, mut receiver) = unbounded_channel();
        let task = SpotBookTask {
            instrument: spot_instrument(),
            market,
            depth: NonZeroUsize::new(2).unwrap(),
            output: SpotBookOutputKind::Deltas,
            network,
            proxy_url: None,
            timeout: Duration::from_secs(2),
            sender,
            connection_epoch: Arc::new(Mutex::new(true)),
            subscription: subscription.clone(),
        };
        let task = tokio::spawn(task.run());

        let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let DataEvent::Data(Data::Deltas(deltas)) = event else {
            panic!("expected Spot L2 deltas");
        };
        assert_eq!(deltas.deltas.len(), 3);
        subscription.retire();
        tokio::time::timeout(Duration::from_secs(2), unsubscribe_receiver)
            .await
            .unwrap()
            .unwrap();
        task.await.unwrap();
        drop(server);
    }

    #[tokio::test]
    async fn idle_spot_book_stops_when_receiver_closes() {
        use axum::{
            Router,
            extract::ws::{Message as ServerMessage, WebSocketUpgrade},
            routing::get,
        };

        let response: crate::http::DeepXApiResponse<Vec<DeepXSpotMarket>> =
            serde_json::from_str(SPOT_MARKETS).unwrap();
        let market = response
            .data
            .into_iter()
            .find(|market| market.name == "ETH/USDC")
            .unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let entered_on_connect = Arc::clone(&entered);
        let app = Router::new().route(
            "/rpc",
            get(move |upgrade: WebSocketUpgrade| {
                let entered = Arc::clone(&entered_on_connect);
                async move {
                    upgrade.on_upgrade(move |mut socket| async move {
                        let Some(Ok(ServerMessage::Text(_))) = socket.recv().await else {
                            return;
                        };
                        entered.notify_one();
                        while socket.recv().await.is_some() {}
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = Server(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let (sender, receiver) = unbounded_channel();
        let subscription = TradeSubscription::new();
        let task = tokio::spawn(
            SpotBookTask {
                instrument: spot_instrument(),
                market: Box::new(market),
                depth: NonZeroUsize::new(2).unwrap(),
                output: SpotBookOutputKind::Deltas,
                network: DeepXNetworkConfig {
                    base_url_rpc_watch: Some(format!("http://{address}/rpc")),
                    ..Default::default()
                },
                proxy_url: None,
                timeout: Duration::from_secs(30),
                sender,
                connection_epoch: Arc::new(Mutex::new(true)),
                subscription: subscription.clone(),
            }
            .run(),
        );
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();

        drop(receiver);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(!subscription.is_active());
        drop(server);
    }

    #[rstest::rstest]
    #[case(1, false, false)]
    #[case(1, true, false)]
    #[case(1, false, true)]
    #[case(10, false, false)]
    #[case(10, true, false)]
    #[tokio::test]
    async fn one_shot_depth_requires_ack_and_preserves_order_counts(
        #[case] requested_depth: usize,
        #[case] premature: bool,
        #[case] oversized: bool,
    ) {
        use axum::{
            Router,
            extract::ws::{Message as ServerMessage, WebSocketUpgrade},
            routing::get,
        };

        let response: crate::http::DeepXApiResponse<Vec<DeepXSpotMarket>> =
            serde_json::from_str(SPOT_MARKETS).unwrap();
        let market = response
            .data
            .into_iter()
            .find(|market| market.name == "ETH/USDC")
            .unwrap();
        let pair = market.pair.clone();
        let app = Router::new().route(
            "/rpc",
            get(move |upgrade: WebSocketUpgrade| {
                let pair = pair.clone();
                async move {
                    upgrade.on_upgrade(move |mut socket| async move {
                        let Some(Ok(ServerMessage::Text(request))) = socket.recv().await else {
                            return;
                        };
                        let request: Value = serde_json::from_str(&request).unwrap();
                        assert_eq!(request["method"], "orderbook_subscribe");
                        assert_eq!(request["params"][0][0], pair);
                        assert_eq!(request["params"][2], requested_depth);
                        let ack = json!({"jsonrpc":"2.0","id":1,"result":"depth-1"});
                        let mut snapshot = json!({
                            "jsonrpc":"2.0",
                            "method":"orderbook_subscribe",
                            "params":{
                                "subscription":"depth-1",
                                "result":{
                                    "spot":{
                                        (pair.clone()):{
                                            "bids":[{"price":"0x912d5ff0","amount":"0xc8819b7382d8000","order_count":3}],
                                            "asks":[{"price":"0x912fa9e0","amount":"0x1647e1be62e3c000","order_count":4}]
                                        }
                                    },
                                    "perp":{}
                                }
                            }
                        });
                        if oversized {
                            let bid = snapshot["params"]["result"]["spot"][&pair]["bids"][0]
                                .clone();
                            snapshot["params"]["result"]["spot"][&pair]["bids"]
                                .as_array_mut()
                                .unwrap()
                                .push(bid);
                        }
                        if !premature {
                            socket
                                .send(ServerMessage::Ping(vec![1].into()))
                                .await
                                .unwrap();
                        }
                        for value in if premature {
                            [snapshot, ack]
                        } else {
                            [ack, snapshot]
                        } {
                            let _ = socket
                                .send(ServerMessage::Text(value.to_string().into()))
                                .await;
                        }
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let _server = Server(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let network = DeepXNetworkConfig {
            base_url_rpc_watch: Some(format!("http://{address}/rpc")),
            ..Default::default()
        };
        let result = request_spot_book_depth10(
            network,
            None,
            Duration::from_secs(2),
            market,
            spot_instrument(),
            NonZeroUsize::new(requested_depth).unwrap(),
        )
        .await;
        if premature || oversized {
            assert!(result.is_err());
        } else {
            let depth = result.unwrap();
            assert_eq!(depth.bid_counts[0], 3);
            assert_eq!(depth.ask_counts[0], 4);
            assert_eq!(depth.bids[0].price, Price::from("2435.67"));
            assert_eq!(depth.asks[0].price, Price::from("2435.82"));
        }
    }
}
