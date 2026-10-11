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

//! One-minute perpetual bars from superseded public candle buckets.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use nautilus_common::messages::DataEvent;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    data::{BarType, Data},
    instruments::InstrumentAny,
};
use nautilus_network::transport::Message;
use tokio::sync::mpsc::UnboundedSender;

use super::{bars, live_trades::TradeSubscription};
use crate::{
    config::DeepXNetworkConfig,
    websocket::{
        DeepXWsCandleStream, DeepXWsError, public::DeepXWsPublicRequest,
        transport::DeepXWsReadConnection,
    },
};

const BAR_PROGRESS_DEADLINE: Duration = Duration::from_secs(180);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

struct SubscriptionGuard(TradeSubscription);

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.0.retire();
    }
}

pub(super) struct BarTask {
    pub instrument: InstrumentAny,
    pub bar_type: BarType,
    pub market_id: u16,
    pub pair: String,
    pub network: DeepXNetworkConfig,
    pub proxy_url: Option<String>,
    pub timeout: Duration,
    pub sender: UnboundedSender<DataEvent>,
    pub connection_epoch: Arc<Mutex<bool>>,
    pub subscription: TradeSubscription,
}

impl BarTask {
    pub(super) async fn run(self) {
        let _guard = SubscriptionGuard(self.subscription.clone());
        if let Err(e) = self.run_inner().await
            && !self.subscription.cancellation_token().is_cancelled()
        {
            log::error!("DeepX one-minute bar subscription stopped: {e:#}");
        }
    }

    async fn run_inner(&self) -> anyhow::Result<()> {
        let cancellation = self.subscription.cancellation_token();
        let mut connection = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            () = self.sender.closed() => return Ok(()),
            result = DeepXWsReadConnection::connect(
                &self.network,
                self.proxy_url.as_deref(),
                self.timeout,
            ) => result?,
        };
        let mut stream = DeepXWsCandleStream::new(self.market_id, self.pair.clone())?;
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => Ok(()),
            () = self.sender.closed() => Ok(()),
            result = connection.send_candle_request(&stream) => result,
        };
        if result.is_ok() && !cancellation.is_cancelled() {
            let mut next_bar_deadline = tokio::time::Instant::now() + BAR_PROGRESS_DEADLINE;
            let mut heartbeat = tokio::time::interval_at(
                tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
                HEARTBEAT_INTERVAL,
            );
            let result = loop {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break Ok(()),
                    () = self.sender.closed() => break Ok(()),
                    () = tokio::time::sleep_until(next_bar_deadline) => {
                        break Err(anyhow::anyhow!("DeepX one-minute bar progress deadline exceeded"));
                    }
                    _ = heartbeat.tick() => {
                        if let Err(e) = connection.send_public_request(&DeepXWsPublicRequest::Ping).await {
                            break Err(e);
                        }
                    }
                    message = connection.receive() => {
                        let message = match message {
                            Ok(Some(message)) => message,
                            Ok(None) => break Err(anyhow::anyhow!("DeepX candle connection closed")),
                            Err(e) if matches!(e.downcast_ref::<DeepXWsError>(), Some(DeepXWsError::ReceiveTimeout)) => continue,
                            Err(e) => break Err(e),
                        };
                        let Message::Text(bytes) = message else {
                            if matches!(message, Message::Ping(_) | Message::Pong(_)) {
                                continue;
                            }
                            break Err(anyhow::anyhow!("unsupported DeepX candle message"));
                        };
                        let completed = match std::str::from_utf8(&bytes)
                            .map_err(anyhow::Error::from)
                            .and_then(|text| stream.apply(text))
                        {
                            Ok(completed) => completed,
                            Err(e) => break Err(e),
                        };
                        let Some(completed) = completed else { continue };
                        let ts_init = get_atomic_clock_realtime().get_time_ns();
                        let bar = match bars::parse_bar(
                            &completed.candle,
                            self.bar_type,
                            &self.instrument,
                            ts_init,
                        ) {
                            Ok(bar) => bar,
                            Err(e) => break Err(e),
                        };
                        if let Err(e) = self.subscription.publish(
                            &self.connection_epoch,
                            &self.sender,
                            vec![DataEvent::Data(Data::Bar(bar))],
                        ) {
                            break Err(e);
                        }
                        next_bar_deadline = tokio::time::Instant::now() + BAR_PROGRESS_DEADLINE;
                        if completed.gap_after {
                            break Err(anyhow::anyhow!(
                                "DeepX one-minute candle stream has a missing bucket"
                            ));
                        }
                    }
                }
            };
            let _ = connection.close().await;
            return result;
        }
        let _ = connection.close().await;
        result
    }
}
