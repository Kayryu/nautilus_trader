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

//! Connection-owned live trade subscriptions with synchronous publication fencing.

use std::{
    num::NonZeroUsize,
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
    http::{DeepXHttpClient, DeepXPerpTradesHistoryRequest},
    websocket::{
        DeepXWsError,
        book::DeepXWsBookStream,
        public::{DeepXWsPublicChannel, DeepXWsPublicConnection, DeepXWsPublicFrame},
        trades::DeepXWsTradeStream,
    },
};

#[derive(Clone, Debug)]
pub(super) struct TradeSubscription {
    active: Arc<Mutex<bool>>,
    cancellation: CancellationToken,
    trade_state: Arc<Mutex<Option<DeepXWsTradeStream>>>,
}

impl TradeSubscription {
    pub(super) fn new() -> Self {
        Self {
            active: Arc::new(Mutex::new(true)),
            cancellation: CancellationToken::new(),
            trade_state: Arc::new(Mutex::new(None)),
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
            trade_state: Arc::clone(&self.trade_state),
        }
    }
}

struct SubscriptionGuard(TradeSubscription);
impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.0.retire();
    }
}

pub(super) struct TradeTask {
    pub kind: PublicSubscriptionKind,
    pub instrument: InstrumentAny,
    pub market_id: u16,
    pub network: DeepXNetworkConfig,
    pub http: DeepXHttpClient,
    pub proxy_url: Option<String>,
    pub timeout: Duration,
    pub sender: UnboundedSender<DataEvent>,
    pub connection_epoch: Arc<Mutex<bool>>,
    pub subscription: TradeSubscription,
}

#[derive(Clone, Copy)]
pub(super) enum PublicSubscriptionKind {
    Trades,
    Book { depth: NonZeroUsize },
    Depth10,
    Quotes,
    MarkPrice,
    IndexPrice,
    FundingRate,
}

impl PublicSubscriptionKind {
    fn is_book(self) -> bool {
        matches!(self, Self::Book { .. } | Self::Depth10 | Self::Quotes)
    }

    fn channel(self) -> Option<DeepXWsPublicChannel> {
        match self {
            Self::Trades => Some(DeepXWsPublicChannel::Trades),
            Self::MarkPrice => Some(DeepXWsPublicChannel::MarkPrice),
            Self::IndexPrice => Some(DeepXWsPublicChannel::OraclePrice),
            Self::FundingRate => Some(DeepXWsPublicChannel::FundingRate),
            Self::Book { .. } | Self::Depth10 | Self::Quotes => None,
        }
    }
}

impl TradeTask {
    pub(super) async fn run(self) {
        let _guard = SubscriptionGuard(self.subscription.clone());
        let mut stream = self
            .subscription
            .trade_state
            .lock()
            .expect(MUTEX_POISONED)
            .clone()
            .unwrap_or_else(|| {
                DeepXWsTradeStream::new(
                    self.market_id,
                    NonZeroUsize::new(1024).expect("nonzero capacity"),
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
            if self.subscription.cancellation.is_cancelled() {
                return;
            }
            if (!self.kind.is_book() && !retry_transport) || attempt == 5 {
                log::error!("DeepX public subscription stopped; explicit recovery required: {e}");
                return;
            }
            log::warn!(
                "DeepX public stream interrupted; reconnecting with explicit state reconciliation: {e}"
            );
            tokio::select! {
                biased;
                () = self.subscription.cancellation.cancelled() => return,
                () = tokio::time::sleep(Duration::from_millis(250 * (1 << attempt))) => {},
            }
        }
    }

    async fn run_inner(
        &self,
        stream: &mut DeepXWsTradeStream,
        recovering: bool,
        retry_transport: &mut bool,
    ) -> anyhow::Result<()> {
        let cancellation = &self.subscription.cancellation;
        let mut connection = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            result = DeepXWsPublicConnection::connect(&self.network, self.proxy_url.as_deref(),
                self.timeout, NonZeroUsize::new(32).expect("nonzero capacity")) => match result {
                    Ok(connection) => connection,
                    Err(e) => { *retry_transport = true; return Err(e); }
                },
        };
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => Ok(()),
            result = async {
                let acknowledgement = match self.kind.channel() {
                    Some(channel) => connection.subscribe(self.market_id, vec![channel]).await,
                    None => match self.kind {
                    PublicSubscriptionKind::Book { depth } => connection.subscribe_book(self.market_id, depth,
                        nautilus_model::instruments::Instrument::price_increment(&self.instrument).as_decimal()).await,
                    PublicSubscriptionKind::Depth10 => connection.subscribe_book(self.market_id,
                        NonZeroUsize::new(10).expect("nonzero depth10 depth"),
                        nautilus_model::instruments::Instrument::price_increment(&self.instrument).as_decimal()).await,
                    PublicSubscriptionKind::Quotes => connection.subscribe_book(self.market_id,
                        NonZeroUsize::new(1).expect("nonzero quote depth"),
                        nautilus_model::instruments::Instrument::price_increment(&self.instrument).as_decimal()).await,
                    _ => unreachable!("simple channel handled above"),
                    },
                };
                if let Err(e) = acknowledgement { *retry_transport = true; return Err(e); }
                let mut first_data_frame = true;
                let mut book = DeepXWsBookStream::new(self.market_id, NonZeroUsize::new(8192).expect("nonzero capacity"));
                let snapshot_deadline = tokio::time::Instant::now() + self.timeout;
                let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    let frame = tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return Ok(()),
                        () = tokio::time::sleep_until(snapshot_deadline), if
                            (self.kind.is_book() && book.snapshot().is_none()) ||
                            (!self.kind.is_book() && first_data_frame) => {
                            *retry_transport = true;
                            anyhow::bail!("DeepX public initial data deadline exceeded");
                        }
                        _ = heartbeat.tick() => {
                            if let Err(e) = connection.ping().await { *retry_transport = true; return Err(e); }
                            continue;
                        }
                        result = connection.next_frame() => match result {
                            Ok(frame) => frame,
                            Err(e) if matches!(e.downcast_ref::<DeepXWsError>(), Some(DeepXWsError::ReceiveTimeout)) => continue,
                            Err(e) => { *retry_transport = true; return Err(e); },
                        },
                    };
                    if matches!(frame, DeepXWsPublicFrame::Pong { .. }) { continue; }
                    let ts_init = get_atomic_clock_realtime().get_time_ns();
                    let mut next_stream = stream.clone();
                    let events = match self.kind {
                        PublicSubscriptionKind::Trades => {
                            let raw = if recovering && first_data_frame && stream.is_initialized() {
                                let (start_ms, end_ms) = stream.recovery_window(&frame)?;
                                let request = DeepXPerpTradesHistoryRequest { market_id: u64::from(self.market_id),
                                    start_ms, end_ms, page_size: 100, max_pages: 100 };
                                let history = tokio::time::timeout(self.timeout, self.http.get_perp_trades_history(&request)).await??;
                                next_stream.reconcile_history(&history, &frame)?
                            } else { next_stream.ingest(&frame)? };
                            let ts_init = get_atomic_clock_realtime().get_time_ns();
                            raw.iter().map(|trade| super::trades::parse_trade_tick(trade, &self.instrument,
                                u64::from(self.market_id), ts_init).map(Data::Trade).map(DataEvent::Data)).collect::<anyhow::Result<Vec<_>>>()?
                        },
                        PublicSubscriptionKind::Book { .. }
                        | PublicSubscriptionKind::Depth10
                        | PublicSubscriptionKind::Quotes => {
                            let previous = book.snapshot().cloned();
                            let is_snapshot = book.ingest(&frame)?;
                            let previous = if is_snapshot { None } else { previous.as_ref() };
                            let next = book.snapshot().ok_or_else(|| anyhow::anyhow!("DeepX book snapshot missing"))?;
                            if matches!(self.kind, PublicSubscriptionKind::Quotes) {
                                super::book::parse_quote_tick(previous, next, &self.instrument, ts_init)?
                                    .into_iter().map(Data::Quote).map(DataEvent::Data).collect()
                            } else if matches!(self.kind, PublicSubscriptionKind::Depth10) {
                                vec![DataEvent::Data(Data::Depth10(Box::new(
                                    super::book::parse_book_depth10(next, &self.instrument, ts_init)?,
                                )))]
                            } else {
                                super::book::parse_book_deltas(previous, next, &self.instrument, ts_init)?
                                    .into_iter().map(Data::from).map(DataEvent::Data).collect()
                            }
                        },
                        PublicSubscriptionKind::MarkPrice
                        | PublicSubscriptionKind::IndexPrice
                        | PublicSubscriptionKind::FundingRate => vec![
                            super::public_prices::parse_public_price_event(
                                &frame,
                                self.kind.channel().expect("public price channel"),
                                self.market_id,
                                &self.instrument,
                                ts_init,
                            )?,
                        ],
                    };
                    // Lock order matches global retirement followed by per-subscription retirement
                    let epoch = self.connection_epoch.lock().expect(MUTEX_POISONED);
                    let active = self.subscription.active.lock().expect(MUTEX_POISONED);
                    if !*epoch || !*active { return Ok(()); }
                    for event in events { self.sender.send(event)?; }
                    *stream = next_stream;
                    if matches!(self.kind, PublicSubscriptionKind::Trades) {
                        *self.subscription.trade_state.lock().expect(MUTEX_POISONED) = Some(stream.clone());
                    }
                    first_data_frame = false;
                }
            } => result,
        };
        if result.is_err() && matches!(self.kind, PublicSubscriptionKind::Book { .. }) {
            // Invalidate before waiting for the remote close handshake
            self.invalidate_book()?;
        }
        let close = connection.close().await;
        result.and(close)
    }

    fn invalidate_book(&self) -> anyhow::Result<()> {
        let ts_init = get_atomic_clock_realtime().get_time_ns();
        let mut clear = nautilus_model::data::OrderBookDelta::clear(
            nautilus_model::instruments::Instrument::id(&self.instrument),
            0,
            ts_init,
            ts_init,
        );
        clear.flags |= nautilus_model::enums::RecordFlag::F_LAST as u8;
        let epoch = self.connection_epoch.lock().expect(MUTEX_POISONED);
        let active = self.subscription.active.lock().expect(MUTEX_POISONED);
        if *epoch && *active {
            self.sender.send(DataEvent::Data(Data::from(
                nautilus_model::data::OrderBookDeltas::new(clear.instrument_id, vec![clear]),
            )))?;
        }
        Ok(())
    }
}
