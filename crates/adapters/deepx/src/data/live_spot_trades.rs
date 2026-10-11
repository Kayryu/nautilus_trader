// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Acknowledged live Spot trades with bounded REST recovery and publication fencing.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use nautilus_common::messages::DataEvent;
use nautilus_core::{MUTEX_POISONED, time::get_atomic_clock_realtime};
use nautilus_model::{data::Data, instruments::InstrumentAny};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

use crate::{
    config::DeepXNetworkConfig,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXSpotMarket, DeepXSpotTradesRequest},
    websocket::{
        DeepXWsError, spot_trades::DeepXWsSpotTradeConnection, trades::DeepXWsSpotTradeStream,
    },
};

#[derive(Clone, Debug)]
pub(super) struct SpotTradeSubscription {
    active: Arc<Mutex<bool>>,
    cancellation: CancellationToken,
    state: Arc<Mutex<Option<DeepXWsSpotTradeStream>>>,
}

impl SpotTradeSubscription {
    pub(super) fn new() -> Self {
        Self {
            active: Arc::new(Mutex::new(true)),
            cancellation: CancellationToken::new(),
            state: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) fn is_active(&self) -> bool {
        *self.active.lock().expect(MUTEX_POISONED)
    }

    pub(super) fn retire(&self) {
        *self.active.lock().expect(MUTEX_POISONED) = false;
        self.cancellation.cancel();
    }

    pub(super) fn resume(&self) -> Self {
        Self {
            active: Arc::new(Mutex::new(true)),
            cancellation: CancellationToken::new(),
            state: Arc::clone(&self.state),
        }
    }
}

struct SubscriptionGuard(SpotTradeSubscription);

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.0.retire();
    }
}

pub(super) struct SpotTradeTask {
    pub instrument: InstrumentAny,
    pub market: DeepXSpotMarket,
    pub network: DeepXNetworkConfig,
    pub http: DeepXHttpClient,
    pub proxy_url: Option<String>,
    pub timeout: Duration,
    pub sender: UnboundedSender<DataEvent>,
    pub connection_epoch: Arc<Mutex<bool>>,
    pub subscription: SpotTradeSubscription,
}

impl SpotTradeTask {
    pub(super) async fn run(self) {
        let _guard = SubscriptionGuard(self.subscription.clone());
        let mut stream = self
            .subscription
            .state
            .lock()
            .expect(MUTEX_POISONED)
            .clone()
            .unwrap_or_else(|| {
                DeepXWsSpotTradeStream::new(
                    &self.market,
                    std::num::NonZeroUsize::new(1024).expect("nonzero capacity"),
                )
            });
        let resuming = stream.is_initialized();
        for attempt in 0..=5 {
            let mut retry_transport = false;
            let Err(e) = self
                .run_inner(&mut stream, attempt > 0 || resuming, &mut retry_transport)
                .await
            else {
                return;
            };
            if self.subscription.cancellation.is_cancelled() || self.sender.is_closed() {
                return;
            }
            if !retry_transport || attempt == 5 {
                log::error!(
                    "DeepX Spot trade subscription stopped; explicit recovery required: {e}"
                );
                return;
            }
            log::warn!("DeepX Spot trade stream interrupted; reconnecting with REST recovery: {e}");
            tokio::select! {
                biased;
                () = self.subscription.cancellation.cancelled() => return,
                () = self.sender.closed() => return,
                () = tokio::time::sleep(Duration::from_millis(250 * (1 << attempt))) => {},
            }
        }
    }

    async fn run_inner(
        &self,
        stream: &mut DeepXWsSpotTradeStream,
        recovering: bool,
        retry_transport: &mut bool,
    ) -> anyhow::Result<()> {
        let cancellation = &self.subscription.cancellation;
        let mut connection = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            () = self.sender.closed() => return Ok(()),
            result = DeepXWsSpotTradeConnection::connect(
                &self.network,
                self.proxy_url.as_deref(),
                self.timeout,
                std::num::NonZeroUsize::new(32).expect("nonzero capacity"),
                self.market.clone(),
            ) => match result {
                Ok(connection) => connection,
                Err(e) => { *retry_transport = true; return Err(e); }
            },
        };
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => Ok(()),
            () = self.sender.closed() => Ok(()),
            result = async {
                let mut first_data_frame = true;
                let snapshot_deadline = tokio::time::Instant::now() + self.timeout;
                let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    let frame = tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return Ok(()),
                        () = tokio::time::sleep_until(snapshot_deadline), if first_data_frame => {
                            *retry_transport = true;
                            anyhow::bail!("DeepX Spot initial trade data deadline exceeded");
                        }
                        _ = heartbeat.tick() => {
                            if let Err(e) = connection.ping().await {
                                *retry_transport = true;
                                return Err(e);
                            }
                            continue;
                        }
                        result = connection.next_frame() => match result {
                            Ok(frame) => frame,
                            Err(e) if matches!(e.downcast_ref::<DeepXWsError>(), Some(DeepXWsError::ReceiveTimeout)) => continue,
                            Err(e) => { *retry_transport = true; return Err(e); }
                        },
                    };
                    if matches!(frame, crate::websocket::spot_trades::DeepXWsSpotTradeFrame::Pong) {
                        continue;
                    }
                    let mut next_stream = stream.clone();
                    let trades = if recovering && first_data_frame && stream.is_initialized() {
                        let (start_ms, end_ms) = stream.recovery_window(&frame)?;
                        let request = DeepXSpotTradesRequest {
                            name: Some(self.market.name.clone()),
                            pair: None,
                            wallet: None,
                            start_ms: Some(start_ms),
                            end_ms: Some(end_ms),
                            cursor: None,
                            sort: DeepXAccountSortOrder::Descending,
                            page_size: Some(100),
                        };
                        let pages = tokio::time::timeout(
                            self.timeout,
                            self.http.get_spot_trade_pages(&request, 100),
                        )
                        .await??;
                        let history = pages.into_iter().flat_map(|page| page.items).collect::<Vec<_>>();
                        next_stream.reconcile_history(&history, &frame)?
                    } else {
                        next_stream.ingest(&frame)?
                    };
                    let ts_init = get_atomic_clock_realtime().get_time_ns();
                    let events = trades
                        .iter()
                        .map(|trade| {
                            super::trades::parse_spot_trade_tick(
                                trade,
                                &self.market,
                                &self.instrument,
                                ts_init,
                            )
                            .map(Data::Trade)
                            .map(DataEvent::Data)
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    let epoch = self.connection_epoch.lock().expect(MUTEX_POISONED);
                    let active = self.subscription.active.lock().expect(MUTEX_POISONED);
                    if !*epoch || !*active {
                        return Ok(());
                    }
                    for event in events {
                        self.sender.send(event)?;
                    }
                    *stream = next_stream;
                    *self.subscription.state.lock().expect(MUTEX_POISONED) = Some(stream.clone());
                    first_data_frame = false;
                }
            } => result,
        };
        let close = connection.close().await;
        result.and(close)
    }
}
