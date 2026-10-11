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

//! REST-polled instrument status observations for DeepX markets.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use nautilus_common::messages::DataEvent;
use nautilus_core::{MUTEX_POISONED, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::InstrumentStatus, enums::MarketStatusAction, identifiers::InstrumentId,
};
use tokio::sync::{Notify, mpsc::UnboundedSender};
use tokio_util::sync::CancellationToken;

use crate::{
    http::DeepXHttpClient,
    providers::{DeepXMarketMetadata, DeepXMarketProvider},
};

const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct MarketStatusSnapshot {
    action: MarketStatusAction,
    is_trading: bool,
}

pub(super) type StatusSubscriptions = Arc<Mutex<BTreeSet<InstrumentId>>>;
pub(super) type StatusSnapshots = Arc<Mutex<BTreeMap<InstrumentId, MarketStatusSnapshot>>>;
pub(super) type StatusWakeup = Arc<Notify>;

fn market_status(market: &DeepXMarketMetadata) -> MarketStatusSnapshot {
    let (is_paused, is_deleted) = match market {
        DeepXMarketMetadata::Spot(market) => (market.is_paused, false),
        DeepXMarketMetadata::Perpetual(market) => (market.is_paused, market.is_deleted),
    };
    let action = if is_deleted {
        MarketStatusAction::NotAvailableForTrading
    } else if is_paused {
        MarketStatusAction::Pause
    } else {
        MarketStatusAction::Trading
    };
    MarketStatusSnapshot {
        action,
        is_trading: !is_paused && !is_deleted,
    }
}

fn unavailable_status() -> MarketStatusSnapshot {
    MarketStatusSnapshot {
        action: MarketStatusAction::NotAvailableForTrading,
        is_trading: false,
    }
}

pub(super) async fn poll_market_status(
    http: DeepXHttpClient,
    subscriptions: StatusSubscriptions,
    snapshots: StatusSnapshots,
    wakeup: StatusWakeup,
    sender: UnboundedSender<DataEvent>,
    connection_epoch: Arc<Mutex<bool>>,
    cancellation: CancellationToken,
) {
    loop {
        if cancellation.is_cancelled() || sender.is_closed() {
            return;
        }
        if !*connection_epoch.lock().expect(MUTEX_POISONED) {
            return;
        }
        let subscribed = subscriptions.lock().expect(MUTEX_POISONED).clone();
        if !subscribed.is_empty() {
            let mut provider = DeepXMarketProvider::new(http.clone());
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                () = sender.closed() => return,
                result = provider.load_all() => result,
            };
            if let Err(e) = result {
                log::warn!("DeepX instrument status polling failed: {e:#}");
            } else {
                for instrument_id in subscribed {
                    let status = if let Some(market) = provider.market(&instrument_id) {
                        market_status(market)
                    } else {
                        log::warn!(
                            "DeepX market status response omitted subscribed instrument {instrument_id}"
                        );
                        unavailable_status()
                    };
                    let changed = {
                        let mut snapshots = snapshots.lock().expect(MUTEX_POISONED);
                        snapshots.insert(instrument_id, status) != Some(status)
                    };
                    if !changed {
                        continue;
                    }
                    let ts_observed = get_atomic_clock_realtime().get_time_ns();
                    let event = InstrumentStatus::new(
                        instrument_id,
                        status.action,
                        ts_observed,
                        ts_observed,
                        None,
                        None,
                        Some(status.is_trading),
                        Some(status.is_trading),
                        None,
                    );
                    let subscribed = subscriptions.lock().expect(MUTEX_POISONED);
                    if !subscribed.contains(&instrument_id) {
                        snapshots
                            .lock()
                            .expect(MUTEX_POISONED)
                            .remove(&instrument_id);
                        continue;
                    }
                    let active = connection_epoch.lock().expect(MUTEX_POISONED);
                    if *active && let Err(e) = sender.send(DataEvent::InstrumentStatus(event)) {
                        log::error!("DeepX instrument status dispatch failed: {e}");
                        return;
                    }
                }
            }
        }
        tokio::select! {
            () = cancellation.cancelled() => return,
            () = sender.closed() => return,
            () = wakeup.notified() => {}
            () = tokio::time::sleep(STATUS_POLL_INTERVAL) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU8, Ordering};

    use axum::{Json, Router, routing::get};
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    const PERP: &str = include_str!("../../test_data/http/testnet/perp_markets.json");
    const EMPTY: &str = r#"{"code":200,"msg":"success","data":[],"fail":false}"#;
    const FAILED: &str =
        r#"{"code":10012,"msg":"Service temporarily unavailable","data":null,"fail":true}"#;

    #[rstest]
    #[tokio::test]
    async fn idle_poller_stops_when_receiver_closes() {
        let http = DeepXHttpClient::new("http://127.0.0.1:1".to_string(), Some(1), None).unwrap();
        let (sender, receiver) = unbounded_channel();
        let task = tokio::spawn(poll_market_status(
            http,
            Arc::default(),
            Arc::default(),
            Arc::new(Notify::new()),
            sender,
            Arc::new(Mutex::new(true)),
            CancellationToken::new(),
        ));

        drop(receiver);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn failed_catalog_preserves_status_and_restoration_recovers() {
        let mode = Arc::new(AtomicU8::new(0));
        let mode_on_request = Arc::clone(&mode);
        let app = Router::new()
            .route("/internal/v1/market/spot/markets", get(|| async { EMPTY }))
            .route(
                "/internal/v1/market/perp/markets",
                get(move || {
                    let mode = mode_on_request.load(Ordering::Relaxed);
                    async move {
                        let response = match mode {
                            1 => EMPTY,
                            2 => FAILED,
                            _ => PERP,
                        };
                        Json(serde_json::from_str::<serde_json::Value>(response).unwrap())
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = DeepXHttpClient::new(format!("http://{address}"), Some(2), None).unwrap();
        let mut provider = DeepXMarketProvider::new(http.clone());
        provider.load_all().await.unwrap();
        let instrument_id: InstrumentId = provider.instrument_ids()[0];
        let subscriptions = Arc::new(Mutex::new(BTreeSet::from([instrument_id])));
        let snapshots = Arc::new(Mutex::new(BTreeMap::new()));
        let wakeup = Arc::new(Notify::new());
        let (sender, mut receiver) = unbounded_channel();
        let epoch = Arc::new(Mutex::new(true));
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(poll_market_status(
            http,
            subscriptions,
            snapshots,
            Arc::clone(&wakeup),
            sender,
            epoch,
            cancellation.clone(),
        ));

        for (catalog_mode, expected) in [
            (0, Some(MarketStatusAction::Trading)),
            (1, Some(MarketStatusAction::NotAvailableForTrading)),
            (0, Some(MarketStatusAction::Trading)),
            (2, None),
            (1, Some(MarketStatusAction::NotAvailableForTrading)),
            (0, Some(MarketStatusAction::Trading)),
        ] {
            mode.store(catalog_mode, Ordering::Relaxed);
            wakeup.notify_one();
            if expected.is_none() {
                assert!(
                    tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                        .await
                        .is_err()
                );
                continue;
            }
            let event = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
                .await
                .unwrap()
                .unwrap();
            let DataEvent::InstrumentStatus(status) = event else {
                panic!("expected DeepX instrument status");
            };
            assert_eq!(status.instrument_id, instrument_id);
            assert_eq!(Some(status.action), expected);
            assert_eq!(status.is_trading, Some(catalog_mode == 0));
        }
        cancellation.cancel();
        task.await.unwrap();
        server.abort();
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn blocked_catalog_read_retires_on_cancel_or_closed_receiver(
        #[case] close_receiver: bool,
    ) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let app = Router::new()
            .route("/internal/v1/market/spot/markets", get(|| async { EMPTY }))
            .route(
                "/internal/v1/market/perp/markets",
                get({
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    move || {
                        let entered = Arc::clone(&entered);
                        let release = Arc::clone(&release);
                        async move {
                            entered.notify_one();
                            release.notified().await;
                            PERP
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = DeepXHttpClient::new(format!("http://{address}"), Some(10), None).unwrap();
        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");
        let subscriptions = Arc::new(Mutex::new(BTreeSet::from([instrument_id])));
        let snapshots = Arc::new(Mutex::new(BTreeMap::new()));
        let (sender, receiver) = unbounded_channel();
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(poll_market_status(
            http,
            subscriptions,
            snapshots,
            Arc::new(Notify::new()),
            sender,
            Arc::new(Mutex::new(true)),
            cancellation.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        if close_receiver {
            drop(receiver);
        } else {
            cancellation.cancel();
        }
        tokio::time::timeout(Duration::from_millis(500), task)
            .await
            .unwrap()
            .unwrap();
        release.notify_one();
        server.abort();
    }
}
