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
            match provider.load_all().await {
                Ok(()) => {
                    for instrument_id in subscribed {
                        let Some(market) = provider.market(&instrument_id) else {
                            log::warn!(
                                "DeepX market status response omitted subscribed instrument {instrument_id}"
                            );
                            continue;
                        };
                        let status = market_status(market);
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
                        if *active
                            && let Err(error) = sender.send(DataEvent::InstrumentStatus(event))
                        {
                            log::error!("DeepX instrument status dispatch failed: {error}");
                            return;
                        }
                    }
                }
                Err(error) => log::warn!("DeepX instrument status polling failed: {error:#}"),
            }
        }
        tokio::select! {
            () = cancellation.cancelled() => return,
            () = wakeup.notified() => {}
            () = tokio::time::sleep(STATUS_POLL_INTERVAL) => {}
        }
    }
}
