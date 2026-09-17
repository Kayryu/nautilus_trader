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

//! Read-only REST instrument discovery, historical bars, trades, and funding for DeepX testnet.

mod bars;
mod book;
mod funding;
mod live_trades;
mod public_prices;
mod trades;

use std::{
    cell::RefCell,
    collections::BTreeMap,
    num::NonZeroUsize,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    live::runner::try_get_data_event_sender,
    messages::data::{
        BarsResponse, BookResponse, FundingRatesResponse, InstrumentResponse, InstrumentsResponse,
        RequestBars, RequestBookDeltas, RequestBookDepth, RequestBookSnapshot, RequestCustomData,
        RequestForwardPrices, RequestFundingRates, RequestInstrument, RequestInstruments,
        RequestQuotes, RequestTrades, SubscribeBars, SubscribeBookDeltas, SubscribeBookDepth10,
        SubscribeCustomData, SubscribeFundingRates, SubscribeIndexPrices, SubscribeInstrument,
        SubscribeInstrumentClose, SubscribeInstrumentStatus, SubscribeInstruments,
        SubscribeMarkPrices, SubscribeOptionGreeks, SubscribeQuotes, SubscribeTrades,
        TradesResponse, UnsubscribeBars, UnsubscribeBookDeltas, UnsubscribeBookDepth10,
        UnsubscribeCustomData, UnsubscribeFundingRates, UnsubscribeIndexPrices,
        UnsubscribeInstrument, UnsubscribeInstrumentClose, UnsubscribeInstrumentStatus,
        UnsubscribeInstruments, UnsubscribeMarkPrices, UnsubscribeOptionGreeks, UnsubscribeQuotes,
        UnsubscribeTrades,
    },
    messages::{DataEvent, DataResponse},
    providers::InstrumentProvider,
};
use nautilus_core::{
    MUTEX_POISONED, datetime::try_datetime_to_unix_nanos, time::get_atomic_clock_realtime,
};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    common::{DeepXError, consts::DEEPX_VENUE},
    config::DeepXDataClientConfig,
    http::{
        DeepXFundingRateRequest, DeepXHttpClient, DeepXPerpCandlesRequest,
        DeepXPerpTradesHistoryRequest,
    },
    providers::{DeepXInstrumentProvider, DeepXMarketMetadata, DeepXMarketProvider},
};

fn unsupported(capability: &'static str) -> anyhow::Result<()> {
    Err(DeepXError::UnsupportedCapability(capability).into())
}

macro_rules! unsupported_owned_commands {
    ($($method:ident($command:ident: $command_type:ty) => $capability:literal;)+) => {
        $(
            fn $method(&mut self, $command: $command_type) -> anyhow::Result<()> {
                let _ = $command;
                unsupported($capability)
            }
        )+
    };
}

macro_rules! unsupported_borrowed_commands {
    ($($method:ident($command:ident: $command_type:ty) => $capability:literal;)+) => {
        $(
            fn $method(&mut self, $command: &$command_type) -> anyhow::Result<()> {
                let _ = $command;
                unsupported($capability)
            }
        )+
    };
}

macro_rules! unsupported_requests {
    ($($method:ident($request:ident: $request_type:ty) => $capability:literal;)+) => {
        $(
            fn $method(&self, $request: $request_type) -> anyhow::Result<()> {
                let _ = $request;
                unsupported($capability)
            }
        )+
    };
}

/// DeepX read-only REST and public market-data client, created disconnected.
///
/// Connection loads and publishes verified perpetual instruments. Trade subscriptions lazily open
/// acknowledgement-gated public WebSockets. L2 book subscriptions publish atomic delta batches or
/// complete depth-10 snapshots and request fresh snapshots on bounded recovery. Spot instrument
/// construction remains unsupported.
/// Historical perpetual bars preserve venue timestamps and require exact instrument precision.
/// Historical perpetual trades are delivered as bounded correlated framework responses.
/// Historical funding samples preserve minute bucket times without inferring a payment schedule.
/// Live mark, index, and funding observations preserve venue values and envelope timestamps.
pub struct DeepXDataClient {
    client_id: ClientId,
    config: DeepXDataClientConfig,
    cache: CacheView,
    clock: Rc<RefCell<dyn Clock>>,
    provider: DeepXInstrumentProvider,
    http: DeepXHttpClient,
    tasks: TaskGroup,
    request_epoch: Arc<Mutex<bool>>,
    sender: Option<UnboundedSender<DataEvent>>,
    connected: bool,
    trade_subscriptions: BTreeMap<InstrumentId, live_trades::TradeSubscription>,
    book_subscriptions: BTreeMap<InstrumentId, (NonZeroUsize, live_trades::TradeSubscription)>,
    depth10_subscriptions: BTreeMap<InstrumentId, live_trades::TradeSubscription>,
    quote_subscriptions: BTreeMap<InstrumentId, live_trades::TradeSubscription>,
    mark_price_subscriptions: BTreeMap<InstrumentId, live_trades::TradeSubscription>,
    index_price_subscriptions: BTreeMap<InstrumentId, live_trades::TradeSubscription>,
    funding_rate_subscriptions: BTreeMap<InstrumentId, live_trades::TradeSubscription>,
}

impl std::fmt::Debug for DeepXDataClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXDataClient))
            .field("client_id", &self.client_id)
            .field("config", &self.config)
            .field("cache", &"<cache-view>")
            .field("clock", &"<clock>")
            .field("connected", &self.connected)
            .finish()
    }
}

impl DeepXDataClient {
    /// Creates a validated disconnected DeepX data client.
    ///
    /// # Errors
    ///
    /// Returns an error when the data client configuration is invalid.
    pub fn new(
        client_id: ClientId,
        config: DeepXDataClientConfig,
        cache: CacheView,
        clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let http = DeepXHttpClient::from_network_config(
            &config.network,
            Some(config.http_timeout_secs),
            config.proxy_url.clone(),
        )?;
        Ok(Self {
            client_id,
            config,
            cache,
            clock,
            provider: DeepXInstrumentProvider::new(DeepXMarketProvider::new(http.clone())),
            http,
            tasks: TaskGroup::default(),
            request_epoch: Arc::new(Mutex::new(false)),
            sender: None,
            connected: false,
            trade_subscriptions: BTreeMap::new(),
            book_subscriptions: BTreeMap::new(),
            depth10_subscriptions: BTreeMap::new(),
            quote_subscriptions: BTreeMap::new(),
            mark_price_subscriptions: BTreeMap::new(),
            index_price_subscriptions: BTreeMap::new(),
            funding_rate_subscriptions: BTreeMap::new(),
        })
    }

    /// Returns the validated client configuration.
    #[must_use]
    pub const fn config(&self) -> &DeepXDataClientConfig {
        &self.config
    }

    /// Returns the read-only platform cache view.
    #[must_use]
    pub const fn cache(&self) -> &CacheView {
        &self.cache
    }

    /// Returns the framework clock.
    #[must_use]
    pub const fn clock(&self) -> &Rc<RefCell<dyn Clock>> {
        &self.clock
    }

    fn require_connected(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.connected, "DeepX REST data client is disconnected");
        anyhow::ensure!(
            self.sender
                .as_ref()
                .is_some_and(|sender| !sender.is_closed()),
            "DeepX data event receiver is unavailable",
        );
        Ok(())
    }

    fn send(&self, event: DataEvent) -> anyhow::Result<()> {
        self.require_connected()?;
        self.sender
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("DeepX data event sender is unavailable"))?
            .send(event)
            .map_err(|_| anyhow::anyhow!("DeepX data event receiver is unavailable"))
    }

    fn validate_request(
        &self,
        client_id: Option<ClientId>,
        venue: Option<Venue>,
        has_time_bounds: bool,
        has_params: bool,
    ) -> anyhow::Result<()> {
        self.require_connected()?;
        anyhow::ensure!(
            client_id.is_none_or(|id| id == self.client_id),
            "DeepX request client ID mismatch"
        );
        anyhow::ensure!(
            venue.is_none_or(|venue| venue == *DEEPX_VENUE),
            "DeepX request venue mismatch"
        );
        anyhow::ensure!(!has_time_bounds, "DeepX instrument history is unsupported");
        anyhow::ensure!(
            !has_params,
            "DeepX instrument request parameters are unsupported"
        );
        Ok(())
    }

    fn history_instrument(
        &self,
        instrument_id: InstrumentId,
        client_id: Option<ClientId>,
        has_params: bool,
    ) -> anyhow::Result<(InstrumentAny, u64)> {
        self.require_connected()?;
        anyhow::ensure!(
            client_id.is_none_or(|id| id == self.client_id),
            "DeepX request client ID mismatch"
        );
        anyhow::ensure!(
            instrument_id.venue == *DEEPX_VENUE,
            "DeepX request venue mismatch"
        );
        anyhow::ensure!(
            !has_params,
            "DeepX history request parameters are unsupported"
        );
        let instrument = self
            .provider
            .store()
            .find(&instrument_id)
            .ok_or_else(|| {
                anyhow::anyhow!("DeepX instrument is unknown or unsupported: {instrument_id}")
            })?
            .clone();
        let Some(DeepXMarketMetadata::Perpetual(market)) =
            self.provider.catalog().market(&instrument_id)
        else {
            return Err(DeepXError::UnsupportedCapability("Spot history").into());
        };
        Ok((instrument, market.id))
    }

    fn spawn_data_request<F>(&self, capability: &'static str, future: F) -> anyhow::Result<()>
    where
        F: std::future::Future<Output = anyhow::Result<DataResponse>> + Send + 'static,
    {
        let sender = self.sender.as_ref().expect("connected sender").clone();
        let epoch = Arc::clone(&self.request_epoch);
        self.tasks.spawn(async move {
            match future.await {
                Ok(response) => {
                    // Serialize emission with epoch retirement, including on another worker thread.
                    let active = epoch.lock().expect(MUTEX_POISONED);
                    if *active && let Err(error) = sender.send(DataEvent::Response(response)) {
                        log::error!("DeepX {capability} response dispatch failed: {error}");
                    }
                }
                Err(error) => log::error!("DeepX {capability} request failed: {error}"),
            }
        })?;
        Ok(())
    }

    fn clear_connection(&mut self) {
        *self.request_epoch.lock().expect(MUTEX_POISONED) = false;
        for subscription in self.trade_subscriptions.values() {
            subscription.retire();
        }
        self.trade_subscriptions.clear();
        for (_, subscription) in self.book_subscriptions.values() {
            subscription.retire();
        }
        self.book_subscriptions.clear();
        for subscription in self.depth10_subscriptions.values() {
            subscription.retire();
        }
        self.depth10_subscriptions.clear();
        for subscription in self.quote_subscriptions.values() {
            subscription.retire();
        }
        self.quote_subscriptions.clear();
        for subscription in self.mark_price_subscriptions.values() {
            subscription.retire();
        }
        self.mark_price_subscriptions.clear();
        for subscription in self.index_price_subscriptions.values() {
            subscription.retire();
        }
        self.index_price_subscriptions.clear();
        for subscription in self.funding_rate_subscriptions.values() {
            subscription.retire();
        }
        self.funding_rate_subscriptions.clear();
        self.tasks.abort();
        self.connected = false;
        self.sender = None;
        self.provider.store_mut().clear();
    }

    fn subscribe_public_price(
        &mut self,
        instrument_id: InstrumentId,
        client_id: Option<ClientId>,
        venue: Option<Venue>,
        has_params: bool,
        kind: live_trades::PublicSubscriptionKind,
    ) -> anyhow::Result<()> {
        let (instrument, market_id) =
            self.history_instrument(instrument_id, client_id, has_params)?;
        anyhow::ensure!(
            venue.is_none_or(|venue| venue == *DEEPX_VENUE),
            "DeepX subscription venue mismatch"
        );
        let market_id = u16::try_from(market_id)?;
        self.config.network.ws_connection_url()?;
        let is_active = match kind {
            live_trades::PublicSubscriptionKind::MarkPrice => self
                .mark_price_subscriptions
                .get(&instrument_id)
                .is_some_and(live_trades::TradeSubscription::is_active),
            live_trades::PublicSubscriptionKind::IndexPrice => self
                .index_price_subscriptions
                .get(&instrument_id)
                .is_some_and(live_trades::TradeSubscription::is_active),
            live_trades::PublicSubscriptionKind::FundingRate => self
                .funding_rate_subscriptions
                .get(&instrument_id)
                .is_some_and(live_trades::TradeSubscription::is_active),
            _ => unreachable!("public price subscription kind"),
        };
        if is_active {
            return Ok(());
        }
        let subscription = live_trades::TradeSubscription::new();
        let task = live_trades::TradeTask {
            kind,
            instrument,
            market_id,
            network: self.config.network.clone(),
            http: self.http.clone(),
            proxy_url: self.config.proxy_url.clone(),
            timeout: Duration::from_secs(self.config.websocket_timeout_secs),
            sender: self
                .sender
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("DeepX data event sender unavailable"))?
                .clone(),
            connection_epoch: Arc::clone(&self.request_epoch),
            subscription: subscription.clone(),
        };
        self.tasks.spawn(task.run())?;
        match kind {
            live_trades::PublicSubscriptionKind::MarkPrice => {
                self.mark_price_subscriptions
                    .insert(instrument_id, subscription);
            }
            live_trades::PublicSubscriptionKind::IndexPrice => {
                self.index_price_subscriptions
                    .insert(instrument_id, subscription);
            }
            live_trades::PublicSubscriptionKind::FundingRate => {
                self.funding_rate_subscriptions
                    .insert(instrument_id, subscription);
            }
            _ => unreachable!("public price subscription kind"),
        }
        Ok(())
    }

    fn unsubscribe_public_price(
        &mut self,
        instrument_id: InstrumentId,
        client_id: Option<ClientId>,
        venue: Option<Venue>,
        has_params: bool,
        kind: live_trades::PublicSubscriptionKind,
    ) -> anyhow::Result<()> {
        self.history_instrument(instrument_id, client_id, has_params)?;
        anyhow::ensure!(
            venue.is_none_or(|venue| venue == *DEEPX_VENUE),
            "DeepX subscription venue mismatch"
        );
        let subscription = match kind {
            live_trades::PublicSubscriptionKind::MarkPrice => {
                self.mark_price_subscriptions.remove(&instrument_id)
            }
            live_trades::PublicSubscriptionKind::IndexPrice => {
                self.index_price_subscriptions.remove(&instrument_id)
            }
            live_trades::PublicSubscriptionKind::FundingRate => {
                self.funding_rate_subscriptions.remove(&instrument_id)
            }
            _ => unreachable!("public price subscription kind"),
        };
        if let Some(subscription) = subscription {
            subscription.retire();
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl DataClient for DeepXDataClient {
    fn subscribe_quotes(&mut self, command: SubscribeQuotes) -> anyhow::Result<()> {
        let (instrument, market_id) = self.history_instrument(
            command.instrument_id,
            command.client_id,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        anyhow::ensure!(
            command.venue.is_none_or(|venue| venue == *DEEPX_VENUE),
            "DeepX subscription venue mismatch"
        );
        let market_id = u16::try_from(market_id)?;
        self.config.network.ws_connection_url()?;
        if self
            .quote_subscriptions
            .get(&command.instrument_id)
            .is_some_and(live_trades::TradeSubscription::is_active)
        {
            return Ok(());
        }
        let subscription = live_trades::TradeSubscription::new();
        let task = live_trades::TradeTask {
            kind: live_trades::PublicSubscriptionKind::Quotes,
            instrument,
            market_id,
            network: self.config.network.clone(),
            http: self.http.clone(),
            proxy_url: self.config.proxy_url.clone(),
            timeout: Duration::from_secs(self.config.websocket_timeout_secs),
            sender: self
                .sender
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("DeepX data event sender unavailable"))?
                .clone(),
            connection_epoch: Arc::clone(&self.request_epoch),
            subscription: subscription.clone(),
        };
        self.tasks.spawn(task.run())?;
        self.quote_subscriptions
            .insert(command.instrument_id, subscription);
        Ok(())
    }

    fn unsubscribe_quotes(&mut self, command: &UnsubscribeQuotes) -> anyhow::Result<()> {
        self.history_instrument(
            command.instrument_id,
            command.client_id,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        anyhow::ensure!(
            command.venue.is_none_or(|venue| venue == *DEEPX_VENUE),
            "DeepX subscription venue mismatch"
        );
        if let Some(subscription) = self.quote_subscriptions.remove(&command.instrument_id) {
            subscription.retire();
        }
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, command: SubscribeBookDeltas) -> anyhow::Result<()> {
        let (instrument, market_id) = self.history_instrument(
            command.instrument_id,
            command.client_id,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        anyhow::ensure!(
            command.venue.is_none_or(|venue| venue == *DEEPX_VENUE),
            "DeepX subscription venue mismatch"
        );
        anyhow::ensure!(
            command.book_type == nautilus_model::enums::BookType::L2_MBP,
            "DeepX only supports price-level L2 books"
        );
        let depth = command
            .depth
            .unwrap_or(NonZeroUsize::new(20).expect("nonzero default depth"));
        anyhow::ensure!(
            depth.get() <= 4096,
            "DeepX book depth exceeds retained capacity"
        );
        let market_id = u16::try_from(market_id)?;
        self.config.network.ws_connection_url()?;
        if let Some((active_depth, subscription)) =
            self.book_subscriptions.get(&command.instrument_id)
            && subscription.is_active()
        {
            anyhow::ensure!(
                *active_depth == depth,
                "DeepX active book subscription depth mismatch"
            );
            return Ok(());
        }
        let subscription = live_trades::TradeSubscription::new();
        let task = live_trades::TradeTask {
            kind: live_trades::PublicSubscriptionKind::Book { depth },
            instrument,
            market_id,
            network: self.config.network.clone(),
            http: self.http.clone(),
            proxy_url: self.config.proxy_url.clone(),
            timeout: Duration::from_secs(self.config.websocket_timeout_secs),
            sender: self
                .sender
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("DeepX data event sender unavailable"))?
                .clone(),
            connection_epoch: Arc::clone(&self.request_epoch),
            subscription: subscription.clone(),
        };
        self.tasks.spawn(task.run())?;
        self.book_subscriptions
            .insert(command.instrument_id, (depth, subscription));
        Ok(())
    }

    fn unsubscribe_book_deltas(&mut self, command: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        self.history_instrument(
            command.instrument_id,
            command.client_id,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        anyhow::ensure!(
            command.venue.is_none_or(|venue| venue == *DEEPX_VENUE),
            "DeepX subscription venue mismatch"
        );
        if let Some((_, subscription)) = self.book_subscriptions.remove(&command.instrument_id) {
            subscription.retire();
        }
        Ok(())
    }

    fn subscribe_book_depth10(&mut self, command: SubscribeBookDepth10) -> anyhow::Result<()> {
        let (instrument, market_id) = self.history_instrument(
            command.instrument_id,
            command.client_id,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        anyhow::ensure!(
            command.venue.is_none_or(|venue| venue == *DEEPX_VENUE),
            "DeepX subscription venue mismatch"
        );
        anyhow::ensure!(
            command.book_type == nautilus_model::enums::BookType::L2_MBP,
            "DeepX only supports price-level L2 books"
        );
        anyhow::ensure!(
            command.depth.is_none_or(|depth| depth.get() == 10),
            "DeepX depth10 subscription depth must be 10"
        );
        let market_id = u16::try_from(market_id)?;
        self.config.network.ws_connection_url()?;
        if self
            .depth10_subscriptions
            .get(&command.instrument_id)
            .is_some_and(live_trades::TradeSubscription::is_active)
        {
            return Ok(());
        }
        let subscription = live_trades::TradeSubscription::new();
        let task = live_trades::TradeTask {
            kind: live_trades::PublicSubscriptionKind::Depth10,
            instrument,
            market_id,
            network: self.config.network.clone(),
            http: self.http.clone(),
            proxy_url: self.config.proxy_url.clone(),
            timeout: Duration::from_secs(self.config.websocket_timeout_secs),
            sender: self
                .sender
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("DeepX data event sender unavailable"))?
                .clone(),
            connection_epoch: Arc::clone(&self.request_epoch),
            subscription: subscription.clone(),
        };
        self.tasks.spawn(task.run())?;
        self.depth10_subscriptions
            .insert(command.instrument_id, subscription);
        Ok(())
    }

    fn unsubscribe_book_depth10(&mut self, command: &UnsubscribeBookDepth10) -> anyhow::Result<()> {
        self.history_instrument(
            command.instrument_id,
            command.client_id,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        anyhow::ensure!(
            command.venue.is_none_or(|venue| venue == *DEEPX_VENUE),
            "DeepX subscription venue mismatch"
        );
        if let Some(subscription) = self.depth10_subscriptions.remove(&command.instrument_id) {
            subscription.retire();
        }
        Ok(())
    }

    fn subscribe_trades(&mut self, command: SubscribeTrades) -> anyhow::Result<()> {
        let (instrument, market_id) = self.history_instrument(
            command.instrument_id,
            command.client_id,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        anyhow::ensure!(
            command.venue.is_none_or(|venue| venue == *DEEPX_VENUE),
            "DeepX subscription venue mismatch"
        );
        let market_id = u16::try_from(market_id)?;
        self.config.network.ws_connection_url()?;
        if self
            .trade_subscriptions
            .get(&command.instrument_id)
            .is_some_and(live_trades::TradeSubscription::is_active)
        {
            return Ok(());
        }
        let subscription = self
            .trade_subscriptions
            .get(&command.instrument_id)
            .map_or_else(
                live_trades::TradeSubscription::new,
                live_trades::TradeSubscription::resume,
            );
        let task = live_trades::TradeTask {
            kind: live_trades::PublicSubscriptionKind::Trades,
            instrument,
            market_id,
            network: self.config.network.clone(),
            http: self.http.clone(),
            proxy_url: self.config.proxy_url.clone(),
            timeout: Duration::from_secs(self.config.websocket_timeout_secs),
            sender: self
                .sender
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("DeepX data event sender unavailable"))?
                .clone(),
            connection_epoch: Arc::clone(&self.request_epoch),
            subscription: subscription.clone(),
        };
        self.tasks.spawn(task.run())?;
        self.trade_subscriptions
            .insert(command.instrument_id, subscription);
        Ok(())
    }

    fn unsubscribe_trades(&mut self, command: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.history_instrument(
            command.instrument_id,
            command.client_id,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        anyhow::ensure!(
            command.venue.is_none_or(|venue| venue == *DEEPX_VENUE),
            "DeepX subscription venue mismatch"
        );
        if let Some(subscription) = self.trade_subscriptions.remove(&command.instrument_id) {
            subscription.retire();
        }
        Ok(())
    }

    fn subscribe_mark_prices(&mut self, command: SubscribeMarkPrices) -> anyhow::Result<()> {
        self.subscribe_public_price(
            command.instrument_id,
            command.client_id,
            command.venue,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
            live_trades::PublicSubscriptionKind::MarkPrice,
        )
    }

    fn unsubscribe_mark_prices(&mut self, command: &UnsubscribeMarkPrices) -> anyhow::Result<()> {
        self.unsubscribe_public_price(
            command.instrument_id,
            command.client_id,
            command.venue,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
            live_trades::PublicSubscriptionKind::MarkPrice,
        )
    }

    fn subscribe_index_prices(&mut self, command: SubscribeIndexPrices) -> anyhow::Result<()> {
        self.subscribe_public_price(
            command.instrument_id,
            command.client_id,
            command.venue,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
            live_trades::PublicSubscriptionKind::IndexPrice,
        )
    }

    fn unsubscribe_index_prices(&mut self, command: &UnsubscribeIndexPrices) -> anyhow::Result<()> {
        self.unsubscribe_public_price(
            command.instrument_id,
            command.client_id,
            command.venue,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
            live_trades::PublicSubscriptionKind::IndexPrice,
        )
    }

    fn subscribe_funding_rates(&mut self, command: SubscribeFundingRates) -> anyhow::Result<()> {
        self.subscribe_public_price(
            command.instrument_id,
            command.client_id,
            command.venue,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
            live_trades::PublicSubscriptionKind::FundingRate,
        )
    }

    fn unsubscribe_funding_rates(
        &mut self,
        command: &UnsubscribeFundingRates,
    ) -> anyhow::Result<()> {
        self.unsubscribe_public_price(
            command.instrument_id,
            command.client_id,
            command.venue,
            command
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
            live_trades::PublicSubscriptionKind::FundingRate,
        )
    }

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
        self.clear_connection();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.clear_connection();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.clear_connection();
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
            && self
                .sender
                .as_ref()
                .is_some_and(|sender| !sender.is_closed())
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        self.clear_connection();
        self.tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
            .await?;
        self.tasks.start_generation()?;
        let sender = try_get_data_event_sender()
            .ok_or_else(|| anyhow::anyhow!("DeepX data event sender is unavailable"))?;
        anyhow::ensure!(
            !sender.is_closed(),
            "DeepX data event receiver is unavailable"
        );
        let mut provider = DeepXInstrumentProvider::new(self.provider.catalog().clone());
        provider.load_all(None).await?;
        let instruments = provider.store().list_all();
        anyhow::ensure!(
            !instruments.is_empty(),
            "DeepX has no supported perpetual instruments"
        );
        for instrument in instruments {
            sender
                .send(DataEvent::Instrument(instrument.clone()))
                .map_err(|_| anyhow::anyhow!("DeepX data event receiver is unavailable"))?;
        }
        self.provider = provider;
        self.sender = Some(sender);
        self.request_epoch = Arc::new(Mutex::new(true));
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.clear_connection();
        self.tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
            .await?;
        Ok(())
    }

    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let (instrument, market_id) = self.history_instrument(
            request.instrument_id,
            request.client_id,
            request
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        let start = request.start.map(try_datetime_to_unix_nanos).transpose()?;
        let end = request.end.map(try_datetime_to_unix_nanos).transpose()?;
        let lower = start.unwrap_or_default();
        let upper = end.unwrap_or_else(|| self.clock.borrow().timestamp_ns());
        anyhow::ensure!(lower <= upper, "DeepX trade request start exceeds end");
        let limit = request
            .limit
            .unwrap_or(NonZeroUsize::new(1_000).expect("nonzero default"));
        anyhow::ensure!(
            limit.get() <= 10_000,
            "DeepX trade request limit exceeds 10000"
        );
        let history = DeepXPerpTradesHistoryRequest {
            market_id,
            start_ms: lower.as_millis(),
            end_ms: upper.as_millis(),
            page_size: 100,
            max_pages: 100,
        };
        history.validate()?;
        let http = self.http.clone();
        let client_id = self.client_id;
        self.spawn_data_request("historical trades", async move {
            let raw = http
                .get_perp_trades_history_limited(&history, limit)
                .await?;
            let ts_init = get_atomic_clock_realtime().get_time_ns();
            let mut ticks = raw
                .iter()
                .map(|trade| trades::parse_trade_tick(trade, &instrument, market_id, ts_init))
                .collect::<anyhow::Result<Vec<_>>>()?;
            ticks.retain(|trade| trade.ts_event >= lower && trade.ts_event <= upper);
            ticks.reverse();
            Ok(DataResponse::Trades(TradesResponse::new(
                request.request_id,
                client_id,
                instrument.id(),
                ticks,
                start,
                end,
                ts_init,
                request.params,
            )))
        })
    }

    fn request_funding_rates(&self, request: RequestFundingRates) -> anyhow::Result<()> {
        let (instrument, market_id) = self.history_instrument(
            request.instrument_id,
            request.client_id,
            request
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        let start = request.start.map(try_datetime_to_unix_nanos).transpose()?;
        let end = request.end.map(try_datetime_to_unix_nanos).transpose()?;
        let lower = start.unwrap_or_default();
        let upper = end.unwrap_or_else(|| self.clock.borrow().timestamp_ns());
        anyhow::ensure!(lower <= upper, "DeepX funding request start exceeds end");
        let limit = request
            .limit
            .unwrap_or(NonZeroUsize::new(1_000).expect("nonzero default"));
        anyhow::ensure!(
            limit.get() <= 10_000,
            "DeepX funding request limit exceeds 10000"
        );
        let history = DeepXFundingRateRequest {
            market_id,
            start_ms: lower.as_millis() / 60_000 * 60_000,
            end_ms: Some(upper.as_millis()),
            limit: Some(100),
            cursor: None,
        };
        history.validate()?;
        let http = self.http.clone();
        let client_id = self.client_id;
        self.spawn_data_request("historical funding rates", async move {
            let raw = http
                .get_perp_funding_rates_history_limited(&history, limit, 100)
                .await?;
            let ts_init = get_atomic_clock_realtime().get_time_ns();
            let mut samples = raw
                .iter()
                .map(|record| funding::parse_funding_sample(record, instrument.id(), ts_init))
                .collect::<anyhow::Result<Vec<_>>>()?;
            samples.retain(|sample| sample.ts_event >= lower && sample.ts_event <= upper);
            samples.reverse();
            Ok(DataResponse::FundingRates(FundingRatesResponse::new(
                request.request_id,
                client_id,
                instrument.id(),
                samples,
                start,
                end,
                ts_init,
                request.params,
            )))
        })
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        anyhow::ensure!(
            request.bar_type.is_standard(),
            "DeepX candle history requires a standard bar type"
        );
        anyhow::ensure!(
            request.bar_type.is_externally_aggregated(),
            "DeepX candle history requires externally aggregated bars"
        );
        let bar_type = request.bar_type;
        let interval = bars::candle_interval(bar_type.spec())?;
        let (instrument, market_id) = self.history_instrument(
            bar_type.instrument_id(),
            request.client_id,
            request
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        let start = request.start.map(try_datetime_to_unix_nanos).transpose()?;
        let end = request.end.map(try_datetime_to_unix_nanos).transpose()?;
        let lower = start.unwrap_or_default();
        let upper = end.unwrap_or_else(|| self.clock.borrow().timestamp_ns());
        anyhow::ensure!(lower <= upper, "DeepX candle request start exceeds end");
        let limit = request.limit.map(|limit| limit.get());
        anyhow::ensure!(
            limit.is_none_or(|limit| limit <= 5_000),
            "DeepX candle request limit exceeds 5000"
        );
        let history = DeepXPerpCandlesRequest {
            market_id,
            interval,
            start_ms: lower.as_millis(),
            end_ms: Some(upper.as_millis()),
            limit: limit.map(u32::try_from).transpose()?,
        };
        history.validate()?;
        let http = self.http.clone();
        let client_id = self.client_id;
        self.spawn_data_request("historical bars", async move {
            let page = http.get_perp_candles(&history).await?;
            anyhow::ensure!(
                page.pair == instrument.raw_symbol().as_str(),
                "DeepX candle pair identity mismatch: expected {}, received {}",
                instrument.raw_symbol(),
                page.pair,
            );
            let ts_init = get_atomic_clock_realtime().get_time_ns();
            let mut parsed = page
                .details
                .iter()
                .map(|candle| bars::parse_bar(candle, bar_type, &instrument, ts_init))
                .collect::<anyhow::Result<Vec<_>>>()?;
            parsed.retain(|bar| bar.ts_event >= lower && bar.ts_event <= upper);
            Ok(DataResponse::Bars(BarsResponse::new(
                request.request_id,
                client_id,
                bar_type,
                parsed,
                start,
                end,
                ts_init,
                request.params,
            )))
        })
    }

    fn request_book_snapshot(&self, request: RequestBookSnapshot) -> anyhow::Result<()> {
        let (instrument, market_id) = self.history_instrument(
            request.instrument_id,
            request.client_id,
            request
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        let depth = request
            .depth
            .unwrap_or(NonZeroUsize::new(20).expect("nonzero default depth"));
        anyhow::ensure!(
            depth.get() <= 4096,
            "DeepX book snapshot depth exceeds retained capacity"
        );
        let market_id = u16::try_from(market_id)?;
        self.config.network.ws_connection_url()?;
        let network = self.config.network.clone();
        let proxy_url = self.config.proxy_url.clone();
        let timeout = Duration::from_secs(self.config.websocket_timeout_secs);
        let client_id = self.client_id;
        self.spawn_data_request("book snapshot", async move {
            let book = book::request_book_snapshot(
                network, proxy_url, timeout, market_id, depth, instrument,
            )
            .await?;
            let ts_init = get_atomic_clock_realtime().get_time_ns();
            Ok(DataResponse::Book(BookResponse::new(
                request.request_id,
                client_id,
                request.instrument_id,
                book,
                None,
                None,
                ts_init,
                request.params,
            )))
        })
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        self.validate_request(
            request.client_id,
            request.venue,
            request.start.is_some() || request.end.is_some(),
            request
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        let instruments = self
            .provider
            .store()
            .list_all()
            .into_iter()
            .cloned()
            .collect();
        self.send(DataEvent::Response(DataResponse::Instruments(
            InstrumentsResponse::new(
                request.request_id,
                self.client_id,
                *DEEPX_VENUE,
                instruments,
                None,
                None,
                self.clock.borrow().timestamp_ns(),
                request.params,
            ),
        )))
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        self.validate_request(
            request.client_id,
            Some(request.instrument_id.venue),
            request.start.is_some() || request.end.is_some(),
            request
                .params
                .as_ref()
                .is_some_and(|params| !params.is_empty()),
        )?;
        let instrument = self
            .provider
            .store()
            .find(&request.instrument_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "DeepX instrument is unknown or unsupported: {}",
                    request.instrument_id
                )
            })?;
        self.send(DataEvent::Response(DataResponse::Instrument(Box::new(
            InstrumentResponse::new(
                request.request_id,
                self.client_id,
                request.instrument_id,
                instrument.clone(),
                None,
                None,
                self.clock.borrow().timestamp_ns(),
                request.params,
            ),
        ))))
    }

    unsupported_owned_commands! {
        subscribe(command: SubscribeCustomData) => "custom data subscriptions";
        subscribe_instruments(command: SubscribeInstruments) => "instrument subscriptions";
        subscribe_instrument(command: SubscribeInstrument) => "instrument subscriptions";
        subscribe_bars(command: SubscribeBars) => "bar subscriptions";
        subscribe_instrument_status(command: SubscribeInstrumentStatus) => "instrument status subscriptions";
        subscribe_instrument_close(command: SubscribeInstrumentClose) => "instrument close subscriptions";
        subscribe_option_greeks(command: SubscribeOptionGreeks) => "option greeks subscriptions";
    }

    unsupported_borrowed_commands! {
        unsubscribe(command: UnsubscribeCustomData) => "custom data subscriptions";
        unsubscribe_instruments(command: UnsubscribeInstruments) => "instrument subscriptions";
        unsubscribe_instrument(command: UnsubscribeInstrument) => "instrument subscriptions";
        unsubscribe_bars(command: UnsubscribeBars) => "bar subscriptions";
        unsubscribe_instrument_status(command: UnsubscribeInstrumentStatus) => "instrument status subscriptions";
        unsubscribe_instrument_close(command: UnsubscribeInstrumentClose) => "instrument close subscriptions";
        unsubscribe_option_greeks(command: UnsubscribeOptionGreeks) => "option greeks subscriptions";
    }

    unsupported_requests! {
        request_data(request: RequestCustomData) => "custom data requests";
        request_quotes(request: RequestQuotes) => "quote requests";
        request_forward_prices(request: RequestForwardPrices) => "forward price requests";
        request_book_depth(request: RequestBookDepth) => "order book depth requests";
        request_book_deltas(request: RequestBookDeltas) => "order book delta requests";
    }
}

impl Drop for DeepXDataClient {
    fn drop(&mut self) {
        self.clear_connection();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{Router, routing::get};
    use nautilus_common::{
        cache::Cache, clock::TestClock, live::runner::replace_data_event_sender,
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        data::{BarSpecification, BarType},
        enums::{AggregationSource, BarAggregation, PriceType},
        identifiers::InstrumentId,
        instruments::Instrument,
    };
    use rstest::rstest;
    use tokio::{
        net::TcpListener,
        sync::mpsc::{UnboundedReceiver, unbounded_channel},
        task::JoinHandle,
    };

    use super::*;

    const SPOT: &str = include_str!("../test_data/http/testnet/spot_markets.json");
    const PERP: &str = include_str!("../test_data/http/testnet/perp_markets.json");
    const CAPTURED_PERP: &str = include_str!("../test_data/http/testnet/perp_markets_spec369.json");

    static DATA_EVENT_SENDER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct Server {
        task: JoinHandle<()>,
        _data_event_sender_guard: tokio::sync::MutexGuard<'static, ()>,
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn client(
        perp_response: serde_json::Value,
    ) -> (
        DeepXDataClient,
        UnboundedReceiver<DataEvent>,
        Arc<AtomicUsize>,
        Server,
    ) {
        client_with_history(perp_response, Router::new()).await
    }

    async fn client_with_history(
        perp_response: serde_json::Value,
        history_router: Router,
    ) -> (
        DeepXDataClient,
        UnboundedReceiver<DataEvent>,
        Arc<AtomicUsize>,
        Server,
    ) {
        let data_event_sender_guard = DATA_EVENT_SENDER_LOCK.lock().await;
        let requests = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&requests);
        let router = Router::new()
            .route("/internal/v1/market/spot/markets", get(|| async { SPOT }))
            .route(
                "/internal/v1/market/perp/markets",
                get(move || {
                    count.fetch_add(1, Ordering::Relaxed);
                    let response = perp_response.clone();
                    async move { axum::Json(response) }
                }),
            )
            .merge(history_router);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = Server {
            task: tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            }),
            _data_event_sender_guard: data_event_sender_guard,
        };
        let clock = Rc::new(RefCell::new(TestClock::new()));
        clock.borrow_mut().set_time(UnixNanos::from(123u64));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut client = DeepXDataClient::new(
            ClientId::from("DEEPX-DATA"),
            DeepXDataClientConfig::default(),
            cache.into(),
            clock,
        )
        .unwrap();
        client.http = DeepXHttpClient::new(format!("http://{address}"), Some(5), None).unwrap();
        client.provider =
            DeepXInstrumentProvider::new(DeepXMarketProvider::new(client.http.clone()));
        let (sender, receiver) = unbounded_channel();
        replace_data_event_sender(sender);
        (client, receiver, requests, server)
    }

    fn all_request(client: &DeepXDataClient) -> RequestInstruments {
        RequestInstruments::new(
            None,
            None,
            Some(client.client_id),
            Some(*DEEPX_VENUE),
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    fn one_request(client: &DeepXDataClient, id: &str) -> RequestInstrument {
        RequestInstrument::new(
            InstrumentId::from(id),
            None,
            None,
            Some(client.client_id),
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    fn trade_request() -> RequestTrades {
        RequestTrades::new(
            nautilus_model::identifiers::InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            Some(jiff::Timestamp::from_millisecond(1_000).unwrap()),
            Some(jiff::Timestamp::from_millisecond(3_000).unwrap()),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    fn funding_request() -> RequestFundingRates {
        RequestFundingRates::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            Some(jiff::Timestamp::from_millisecond(60_000).unwrap()),
            Some(jiff::Timestamp::from_millisecond(180_000).unwrap()),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    fn bar_request() -> RequestBars {
        RequestBars::new(
            BarType::new(
                InstrumentId::from("ETH-USDC-PERP.DEEPX"),
                BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
                AggregationSource::External,
            ),
            Some(jiff::Timestamp::from_millisecond(60_000).unwrap()),
            Some(jiff::Timestamp::from_millisecond(180_000).unwrap()),
            NonZeroUsize::new(3),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    fn candle_router(
        scenario: &'static str,
        calls: Arc<AtomicUsize>,
        gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    ) -> Router {
        Router::new().route(
            "/internal/v1/market/perp/candles",
            get(
                move |axum::extract::Query(query): axum::extract::Query<
                    std::collections::HashMap<String, String>,
                >| {
                    let calls = Arc::clone(&calls);
                    let gate = gate.clone();
                    async move {
                        let call = calls.fetch_add(1, Ordering::Relaxed);
                        if call == 0
                            && let Some((entered, release)) = gate
                        {
                            entered.notify_one();
                            release.notified().await;
                        }
                        assert_eq!(query["marketId"], "3");
                        assert_eq!(query["timeFrame"], "1m");
                        assert_eq!(query["sort"], "ASC");
                        assert_eq!(query["tradeView"], "false");
                        let start: u64 = query["start"].parse().unwrap();
                        let end: u64 = query["end"].parse().unwrap();
                        let limit = query
                            .get("limit")
                            .map(|limit| limit.parse().unwrap())
                            .unwrap_or(5_000);
                        let mut details = [60_000, 120_000, 180_000]
                            .into_iter()
                            .filter(|time| *time >= start && *time <= end)
                            .take(limit)
                            .map(|time| {
                                serde_json::json!({
                                    "volume": "0.0040",
                                    "high": "1800.1000",
                                    "low": "1790.1000",
                                    "open": "1792.6000",
                                    "close": "1798.1000",
                                    "time": time,
                                })
                            })
                            .collect::<Vec<_>>();
                        let pair = if scenario == "foreign-pair" {
                            "BTC-USDC"
                        } else {
                            "ETH-USDC"
                        };
                        if let Some(candle) = details.first_mut() {
                            match scenario {
                                "price-precision" => {
                                    candle["open"] = serde_json::json!("1792.60001");
                                }
                                "volume-precision" => {
                                    candle["volume"] = serde_json::json!("0.00401");
                                }
                                "nonpositive-price" => {
                                    candle["low"] = serde_json::json!("0");
                                }
                                "out-of-range" => {
                                    candle["time"] = serde_json::json!(240_000);
                                }
                                _ => {}
                            }
                        }
                        axum::Json(serde_json::json!({
                            "code": 200,
                            "msg": "success",
                            "fail": false,
                            "data": {"pair": pair, "details": details},
                        }))
                    }
                },
            ),
        )
    }

    #[rstest]
    #[case("full", 3)]
    #[case("precise-bounds", 1)]
    #[case("empty-params", 3)]
    #[case("no-limit", 3)]
    #[tokio::test]
    async fn candle_history_returns_exact_correlated_chronological_bars(
        #[case] scenario: &str,
        #[case] count: usize,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            candle_router("valid", Arc::clone(&calls), None),
        )
        .await;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        let mut request = bar_request();
        match scenario {
            "precise-bounds" => {
                request.start = Some(jiff::Timestamp::from_nanosecond(60_000_000_001).unwrap());
                request.end = Some(jiff::Timestamp::from_nanosecond(179_999_999_999).unwrap());
            }
            "empty-params" => request.params = Some(nautilus_core::Params::new()),
            "no-limit" => request.limit = None,
            _ => {}
        }
        let expected = request.clone();
        client.request_bars(request).unwrap();
        let event = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let DataEvent::Response(DataResponse::Bars(response)) = event else {
            panic!("expected bars response")
        };
        assert_eq!(response.correlation_id, expected.request_id);
        assert_eq!(response.client_id, client.client_id);
        assert_eq!(response.bar_type, expected.bar_type);
        assert_eq!(
            response.start,
            expected
                .start
                .map(try_datetime_to_unix_nanos)
                .transpose()
                .unwrap()
        );
        assert_eq!(
            response.end,
            expected
                .end
                .map(try_datetime_to_unix_nanos)
                .transpose()
                .unwrap()
        );
        assert_eq!(response.data.len(), count);
        assert!(
            response
                .data
                .windows(2)
                .all(|pair| pair[0].ts_event < pair[1].ts_event)
        );
        for bar in &response.data {
            assert_eq!(bar.ts_init, response.ts_init);
            assert_eq!(bar.bar_type, response.bar_type);
        }
        if scenario == "full" {
            let bar = &response.data[0];
            assert_eq!(bar.open.as_decimal().to_string(), "1792.6000");
            assert_eq!(bar.high.as_decimal().to_string(), "1800.1000");
            assert_eq!(bar.low.as_decimal().to_string(), "1790.1000");
            assert_eq!(bar.close.as_decimal().to_string(), "1798.1000");
            assert_eq!(bar.volume.as_decimal().to_string(), "0.0040");
            assert_eq!(bar.ts_event.as_millis(), 60_000);
        }
        finish_requests(&client).await;
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    #[case("foreign-pair")]
    #[case("price-precision")]
    #[case("volume-precision")]
    #[case("nonpositive-price")]
    #[case("out-of-range")]
    #[tokio::test]
    async fn invalid_candle_history_never_emits_partial_response(#[case] scenario: &'static str) {
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            candle_router(scenario, Arc::new(AtomicUsize::new(0)), None),
        )
        .await;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.request_bars(bar_request()).unwrap();
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    #[case("disconnected")]
    #[case("client")]
    #[case("venue")]
    #[case("spot")]
    #[case("params")]
    #[case("range")]
    #[case("negative-time")]
    #[case("limit")]
    #[case("source")]
    #[case("price-type")]
    #[case("interval")]
    #[case("composite")]
    #[tokio::test]
    async fn invalid_candle_request_fails_before_transport(#[case] invalid: &str) {
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            candle_router("valid", Arc::clone(&calls), None),
        )
        .await;
        if invalid != "disconnected" {
            client.connect().await.unwrap();
        }
        while receiver.try_recv().is_ok() {}
        let mut request = bar_request();
        match invalid {
            "client" => request.client_id = Some(ClientId::from("OTHER")),
            "venue" => {
                request.bar_type = BarType::new(
                    InstrumentId::from("ETH-USDC-PERP.OTHER"),
                    request.bar_type.spec(),
                    AggregationSource::External,
                );
            }
            "spot" => {
                request.bar_type = BarType::new(
                    InstrumentId::from("ETH-USDC.DEEPX"),
                    request.bar_type.spec(),
                    AggregationSource::External,
                );
            }
            "params" => {
                let mut params = nautilus_core::Params::new();
                params.insert("unsupported".to_string(), serde_json::json!(true));
                request.params = Some(params);
            }
            "range" => {
                request.start = Some(jiff::Timestamp::from_millisecond(240_000).unwrap());
            }
            "negative-time" => {
                request.start = Some(jiff::Timestamp::from_millisecond(-1).unwrap());
            }
            "limit" => request.limit = NonZeroUsize::new(5_001),
            "source" => {
                request.bar_type = BarType::new(
                    request.bar_type.instrument_id(),
                    request.bar_type.spec(),
                    AggregationSource::Internal,
                );
            }
            "price-type" => {
                request.bar_type = BarType::new(
                    request.bar_type.instrument_id(),
                    BarSpecification::new(1, BarAggregation::Minute, PriceType::Bid),
                    AggregationSource::External,
                );
            }
            "interval" => {
                request.bar_type = BarType::new(
                    request.bar_type.instrument_id(),
                    BarSpecification::new(2, BarAggregation::Minute, PriceType::Last),
                    AggregationSource::External,
                );
            }
            "composite" => {
                request.bar_type = BarType::new_composite(
                    request.bar_type.instrument_id(),
                    request.bar_type.spec(),
                    AggregationSource::External,
                    1,
                    BarAggregation::Minute,
                    AggregationSource::External,
                );
            }
            _ => {}
        }
        assert!(client.request_bars(request).is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn candle_tasks_cannot_emit_into_reconnected_epoch() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            candle_router(
                "valid",
                Arc::clone(&calls),
                Some((Arc::clone(&entered), Arc::clone(&release))),
            ),
        )
        .await;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.request_bars(bar_request()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        client.disconnect().await.unwrap();
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        release.notify_one();
        let request = bar_request();
        let expected = request.request_id;
        client.request_bars(request).unwrap();
        let event = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let DataEvent::Response(DataResponse::Bars(response)) = event else {
            panic!("expected bars response")
        };
        assert_eq!(response.correlation_id, expected);
        finish_requests(&client).await;
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert!(receiver.try_recv().is_err());
    }

    fn funding_router(
        scenario: &'static str,
        calls: Arc<AtomicUsize>,
        gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    ) -> Router {
        Router::new().route("/internal/v1/market/perp/funding_rate", get(
            move |axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>| {
                let calls = Arc::clone(&calls);
                let gate = gate.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::Relaxed);
                    if call == 0 && let Some((entered, release)) = gate {
                        entered.notify_one();
                        release.notified().await;
                    }
                    assert_eq!(query["marketId"], "3");
                    assert_eq!(query["sort"], "DESC");
                    assert_eq!(query["interval"], "1m");
                    let start: u64 = query["start"].parse().unwrap();
                    let end: u64 = query["end"].parse().unwrap();
                    let limit: usize = query["limit"].parse().unwrap();
                    let offset: usize = query.get("cursor").map_or(0, |cursor| cursor.parse().unwrap());
                    let rows = [(180_000, "0.000012500000000000001"), (120_000, "-0.00005"), (60_000, "0")]
                        .into_iter().filter(|(time, _)| *time >= start && *time <= end)
                        .map(|(time, rate)| serde_json::json!({"time": time, "fundingRate": rate}))
                        .collect::<Vec<_>>();
                    let mut details = rows.iter().skip(offset).take(limit.min(2)).cloned().collect::<Vec<_>>();
                    let next = offset + details.len();
                    let mut has_next = next < rows.len();
                    let mut cursor = has_next.then(|| next.to_string());
                    let market = if scenario == "foreign-market" { 4 } else { 3 };
                    if offset != 0 {
                        match scenario {
                            "duplicate" => details[0]["time"] = serde_json::json!(120_000),
                            "ascending" => details[0]["time"] = serde_json::json!(180_000),
                            "unaligned" => details[0]["time"] = serde_json::json!(60_001),
                            "range" => details[0]["time"] = serde_json::json!(240_000),
                            "bad-rate" => details[0]["fundingRate"] = serde_json::json!("NaN"),
                            "repeat-cursor" => { has_next = true; cursor = Some(offset.to_string()); }
                            "missing-cursor" => { has_next = true; cursor = None; }
                            "empty-continuation" => { details.clear(); has_next = true; cursor = Some("3".to_string()); }
                            _ => {},
                        }
                    }
                    if scenario == "oversized" { details.resize(limit + 1, serde_json::json!({"time": 60_000, "fundingRate": "0"})); }
                    axum::Json(serde_json::json!({"code": 200, "msg": "success", "fail": false,
                        "data": {"marketId": market, "details": details, "hasNext": has_next, "nextCursor": cursor}}))
                }
            }
        ))
    }

    #[rstest]
    #[case("full", 3, 2)]
    #[case("limited", 1, 1)]
    #[case("precise-bounds", 1, 1)]
    #[case("empty", 0, 1)]
    #[tokio::test]
    async fn funding_history_returns_exact_correlated_chronological_samples(
        #[case] scenario: &str,
        #[case] count: usize,
        #[case] pages: usize,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            funding_router("valid", Arc::clone(&calls), None),
        )
        .await;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        let mut request = funding_request();
        match scenario {
            "limited" => request.limit = NonZeroUsize::new(1),
            "precise-bounds" => {
                request.start = Some(jiff::Timestamp::from_nanosecond(60_000_000_001).unwrap());
                request.end = Some(jiff::Timestamp::from_nanosecond(179_999_999_999).unwrap());
            }
            "empty" => {
                request.start = Some(jiff::Timestamp::from_millisecond(240_000).unwrap());
                request.end = Some(jiff::Timestamp::from_millisecond(300_000).unwrap());
            }
            _ => {}
        }
        let expected = request.clone();
        client.request_funding_rates(request).unwrap();
        let event = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let DataEvent::Response(DataResponse::FundingRates(response)) = event else {
            panic!("expected funding response")
        };
        assert_eq!(response.correlation_id, expected.request_id);
        assert_eq!(response.client_id, client.client_id);
        assert_eq!(response.instrument_id, expected.instrument_id);
        assert_eq!(
            response.start,
            expected
                .start
                .map(try_datetime_to_unix_nanos)
                .transpose()
                .unwrap()
        );
        assert_eq!(
            response.end,
            expected
                .end
                .map(try_datetime_to_unix_nanos)
                .transpose()
                .unwrap()
        );
        assert_eq!(response.data.len(), count);
        assert!(
            response
                .data
                .windows(2)
                .all(|pair| pair[0].ts_event < pair[1].ts_event)
        );
        for sample in &response.data {
            assert_eq!(sample.interval, None);
            assert_eq!(sample.next_funding_ns, None);
            assert_eq!(sample.ts_init, response.ts_init);
        }
        if scenario == "full" {
            assert_eq!(response.data[0].rate.to_string(), "0");
            assert_eq!(response.data[1].rate.to_string(), "-0.00005");
            assert_eq!(response.data[2].rate.to_string(), "0.000012500000000000001");
        }
        if scenario == "limited" {
            assert_eq!(response.data[0].ts_event.as_millis(), 180_000);
        }
        finish_requests(&client).await;
        assert_eq!(calls.load(Ordering::Relaxed), pages);
    }

    #[rstest]
    #[case("foreign-market")]
    #[case("duplicate")]
    #[case("ascending")]
    #[case("unaligned")]
    #[case("range")]
    #[case("bad-rate")]
    #[case("repeat-cursor")]
    #[case("missing-cursor")]
    #[case("empty-continuation")]
    #[case("oversized")]
    #[tokio::test]
    async fn malformed_funding_history_never_emits_partial_response(
        #[case] scenario: &'static str,
    ) {
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            funding_router(scenario, Arc::new(AtomicUsize::new(0)), None),
        )
        .await;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.request_funding_rates(funding_request()).unwrap();
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn funding_tasks_cannot_emit_into_reconnected_epoch() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            funding_router(
                "valid",
                Arc::new(AtomicUsize::new(0)),
                Some((Arc::clone(&entered), Arc::clone(&release))),
            ),
        )
        .await;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.request_funding_rates(funding_request()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        client.disconnect().await.unwrap();
        assert!(client.request_funding_rates(funding_request()).is_err());
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        release.notify_one();
        let request = funding_request();
        let expected = request.request_id;
        client.request_funding_rates(request).unwrap();
        let event = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let DataEvent::Response(DataResponse::FundingRates(response)) = event else {
            panic!("expected funding response")
        };
        assert_eq!(response.correlation_id, expected);
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    #[case("client")]
    #[case("venue")]
    #[case("spot")]
    #[case("params")]
    #[case("range")]
    #[case("negative-time")]
    #[case("limit")]
    #[tokio::test]
    async fn invalid_funding_request_fails_before_transport(#[case] invalid: &str) {
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            funding_router("valid", Arc::clone(&calls), None),
        )
        .await;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        let mut request = funding_request();
        match invalid {
            "client" => request.client_id = Some(ClientId::from("OTHER")),
            "venue" => request.instrument_id = InstrumentId::from("ETH-USDC-PERP.OTHER"),
            "spot" => request.instrument_id = InstrumentId::from("ETH-USDC.DEEPX"),
            "params" => {
                let mut params = nautilus_core::Params::new();
                params.insert("unsupported".to_string(), serde_json::json!(true));
                request.params = Some(params);
            }
            "range" => request.start = Some(jiff::Timestamp::from_millisecond(240_000).unwrap()),
            "negative-time" => request.start = Some(jiff::Timestamp::from_millisecond(-1).unwrap()),
            "limit" => request.limit = NonZeroUsize::new(10_001),
            _ => unreachable!(),
        }
        assert!(client.request_funding_rates(request).is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    fn history_router(
        scenario: &'static str,
        calls: Arc<AtomicUsize>,
        gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    ) -> Router {
        Router::new().route("/internal/v1/market/perp/trades", get(
            move |axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>| {
                let calls = Arc::clone(&calls);
                let gate = gate.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::Relaxed);
                    if call == 0 && let Some((entered, release)) = gate {
                        entered.notify_one();
                        release.notified().await;
                    }
                    assert_eq!(query["marketId"], "3");
                    assert_eq!(query["sort"], "DESC");
                    let start: i64 = query["start"].parse().unwrap();
                    let end: i64 = query["end"].parse().unwrap();
                    let page_size: usize = query["pageSize"].parse().unwrap();
                    let offset: usize = query.get("cursor").map_or(0, |cursor| cursor.parse().unwrap());
                    let rows = [3_000, 2_000, 1_500, 1_000].into_iter().enumerate()
                        .filter(|(_, time)| *time >= start && *time <= end)
                        .map(|(index, time)| serde_json::json!({
                            "id": 4 - index, "marketId": 3, "buyerOrderId": "1", "buyer": "0x11",
                            "sellerOrderId": "2", "seller": "0x22", "price": "1792.60", "size": "0.004",
                            "buyerLeverage": 25.0, "sellerLeverage": 12.5,
                            "createdAt": jiff::Timestamp::from_millisecond(time).unwrap().to_string(),
                            "filledDirection": "Both", "taker": if index % 2 == 0 { "Buyer" } else { "Seller" },
                            "takerFee": "0", "makerFee": "0"
                        })).collect::<Vec<_>>();
                    let mut items = rows.iter().skip(offset).take(page_size.min(2)).cloned().collect::<Vec<_>>();
                    let next_offset = offset + items.len();
                    let has_next = next_offset < rows.len();
                    let cursor = has_next.then(|| next_offset.to_string());
                    if let Some(trade) = items.first_mut() {
                        match scenario {
                            "unknown-taker" => trade["taker"] = serde_json::json!("Unknown"),
                            "price-precision" => trade["price"] = serde_json::json!("1792.60001"),
                            "size-precision" => trade["size"] = serde_json::json!("0.00401"),
                            "zero-size" => trade["size"] = serde_json::json!("0"),
                            "negative-size" => trade["size"] = serde_json::json!("-1"),
                            "negative-price" => trade["price"] = serde_json::json!("-1"),
                            "invalid-time" => trade["createdAt"] = serde_json::json!("invalid"),
                            "submillisecond" => trade["createdAt"] = serde_json::json!("1970-01-01T00:00:02.000001Z"),
                            "zero-id" => trade["id"] = serde_json::json!(0),
                            "foreign-market" => trade["marketId"] = serde_json::json!(4),
                            _ => {},
                        }
                    }
                    axum::Json(serde_json::json!({"code": 200, "fail": false, "msg": "success",
                        "data": {"items": items, "hasNext": has_next, "nextCursor": cursor}}))
                }
            }
        ))
    }

    fn quote_subscribe() -> SubscribeQuotes {
        SubscribeQuotes::new(
            live_subscribe().instrument_id,
            None,
            Some(*DEEPX_VENUE),
            UUID4::new(),
            Default::default(),
            None,
            None,
        )
    }

    fn quote_unsubscribe() -> UnsubscribeQuotes {
        UnsubscribeQuotes::new(
            live_subscribe().instrument_id,
            None,
            Some(*DEEPX_VENUE),
            UUID4::new(),
            Default::default(),
            None,
            None,
        )
    }

    fn quote_router(
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        calls: Arc<AtomicUsize>,
        scenario: &'static str,
    ) -> Router {
        use axum::extract::ws::{Message, WebSocketUpgrade};
        Router::new().route("/internal/v1/ws", get(move |upgrade: WebSocketUpgrade| {
            let entered = Arc::clone(&entered); let release = Arc::clone(&release); let calls = Arc::clone(&calls);
            async move { upgrade.on_upgrade(move |mut socket| async move {
                let Some(Ok(Message::Text(request))) = socket.recv().await else { return; };
                let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                assert_eq!(request["subscriptions"], serde_json::json!(["orderbook"]));
                assert_eq!(request["options"]["orderbook_depth"], 1);
                let call = calls.fetch_add(1, Ordering::Relaxed);
                let ack = serde_json::json!({"type":"subscribed","market":{"type":"perp","id":3},"subscriptions":["orderbook"],"message":"Subscribed"});
                if socket.send(Message::Text(ack.to_string().into())).await.is_err() { return; }
                let frame = |kind: &str, prev: u64, id: u64, size: &str, empty: bool| Message::Text(serde_json::json!({
                    "type":"data","channel":"orderbook","market":{"type":"perp","id":3},"timestamp":999999,
                    "data":{"updateType":kind,"prevLastUpdateId":prev,"lastUpdateId":id,"engineTime":id*100,
                        "bids":[{"price":"1792.60","qty":size,"value":"1"}],
                        "asks":if empty { serde_json::json!([]) } else { serde_json::json!([{"price":"1792.80","qty":"0.005","value":"1"}]) }}}).to_string().into());
                if socket.send(frame("snapshot",0,10,"0.004",scenario == "empty")).await.is_err() { return; }
                if call == 0 {
                    entered.notify_one(); release.notified().await;
                    if socket.send(frame("delta", if scenario == "gap" {9} else {10},20,
                        if scenario == "precision" {"0.000000001"} else {"0.006"},false)).await.is_err() { return; }
                    if socket.send(frame("delta",20,30,"0.006",false)).await.is_err() { return; }
                }
                while let Some(Ok(message)) = socket.recv().await {
                    match message {
                        Message::Close(_) => break,
                        Message::Ping(payload) => { if socket.send(Message::Pong(payload)).await.is_err() { break; } }
                        _ => {},
                    }
                }
            }) }
        }))
    }

    #[rstest]
    #[case("normal")]
    #[case("empty")]
    #[case("gap")]
    #[case("precision")]
    #[tokio::test]
    async fn live_quotes_publish_only_verified_best_levels(#[case] scenario: &'static str) {
        use nautilus_model::data::Data;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            quote_router(
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::clone(&calls),
                scenario,
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.subscribe_quotes(quote_subscribe()).unwrap();
        client.subscribe_quotes(quote_subscribe()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        if scenario != "empty" {
            let DataEvent::Data(Data::Quote(initial)) =
                tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                    .await
                    .unwrap()
                    .unwrap()
            else {
                panic!("expected initial quote");
            };
            assert_eq!(
                initial.bid_price.as_decimal(),
                "1792.60".parse::<rust_decimal::Decimal>().unwrap()
            );
            assert_eq!(
                initial.ask_price.as_decimal(),
                "1792.80".parse::<rust_decimal::Decimal>().unwrap()
            );
            assert_eq!(initial.ts_event.as_millis(), 1000);
        } else {
            assert!(receiver.try_recv().is_err());
        }
        release.notify_one();
        let DataEvent::Data(Data::Quote(next)) =
            tokio::time::timeout(Duration::from_secs(3), receiver.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected changed or recovered quote, not book deltas");
        };
        let recovering = scenario == "gap" || scenario == "precision";
        assert_eq!(
            next.bid_size.as_decimal(),
            if recovering { "0.004" } else { "0.006" }
                .parse::<rust_decimal::Decimal>()
                .unwrap()
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            if recovering { 2 } else { 1 }
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), receiver.recv())
                .await
                .is_err()
        );
        client.unsubscribe_quotes(&quote_unsubscribe()).unwrap();
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
        client.disconnect().await.unwrap();
    }

    #[rstest]
    #[case("unsubscribe")]
    #[case("disconnect")]
    #[case("stop")]
    #[case("reset")]
    #[case("dispose")]
    #[case("drop")]
    #[tokio::test]
    async fn live_quotes_retirement_fences_pending_updates(#[case] shutdown: &str) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            quote_router(
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::new(AtomicUsize::new(0)),
                "normal",
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.subscribe_quotes(quote_subscribe()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        match shutdown {
            "unsubscribe" => client.unsubscribe_quotes(&quote_unsubscribe()).unwrap(),
            "disconnect" => client.disconnect().await.unwrap(),
            "stop" => client.stop().unwrap(),
            "reset" => client.reset().unwrap(),
            "dispose" => client.dispose().unwrap(),
            "drop" => {
                drop(client);
                release.notify_one();
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                        .await
                        .is_err()
                );
                return;
            }
            _ => unreachable!(),
        }
        release.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err()
        );
    }

    fn book_subscribe() -> SubscribeBookDeltas {
        SubscribeBookDeltas::new(
            live_subscribe().instrument_id,
            nautilus_model::enums::BookType::L2_MBP,
            None,
            Some(*DEEPX_VENUE),
            nautilus_core::UUID4::new(),
            Default::default(),
            NonZeroUsize::new(20),
            true,
            None,
            None,
        )
    }

    fn book_unsubscribe() -> UnsubscribeBookDeltas {
        UnsubscribeBookDeltas::new(
            live_subscribe().instrument_id,
            None,
            Some(*DEEPX_VENUE),
            nautilus_core::UUID4::new(),
            Default::default(),
            None,
            None,
        )
    }

    fn depth10_subscribe() -> SubscribeBookDepth10 {
        SubscribeBookDepth10::new(
            live_subscribe().instrument_id,
            nautilus_model::enums::BookType::L2_MBP,
            None,
            Some(*DEEPX_VENUE),
            nautilus_core::UUID4::new(),
            Default::default(),
            NonZeroUsize::new(10),
            true,
            None,
            None,
        )
    }

    fn depth10_unsubscribe() -> UnsubscribeBookDepth10 {
        UnsubscribeBookDepth10::new(
            live_subscribe().instrument_id,
            None,
            Some(*DEEPX_VENUE),
            nautilus_core::UUID4::new(),
            Default::default(),
            None,
            None,
        )
    }

    fn live_book_router(
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        calls: Arc<AtomicUsize>,
        scenario: &'static str,
        expected_depth: usize,
    ) -> Router {
        use axum::extract::ws::{Message, WebSocketUpgrade};
        Router::new().route("/internal/v1/ws", get(move |upgrade: WebSocketUpgrade| {
            let entered = Arc::clone(&entered); let release = Arc::clone(&release); let calls = Arc::clone(&calls);
            async move { upgrade.on_upgrade(move |mut socket| async move {
                let Some(Ok(Message::Text(request))) = socket.recv().await else { return; };
                let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                assert_eq!(request["subscriptions"], serde_json::json!(["orderbook"]));
                assert_eq!(request["options"]["orderbook_depth"], expected_depth);
                assert_eq!(request["options"]["orderbook_price_size"], 0.01);
                let call = calls.fetch_add(1, Ordering::Relaxed);
                let ack = serde_json::json!({"type":"subscribed","market":{"type":"perp","id":3},"subscriptions":["orderbook"],"message":"Subscribed"});
                if socket.send(Message::Text(ack.to_string().into())).await.is_err() { return; }
                if call == 0 { entered.notify_one(); release.notified().await; }
                let frame = |kind: &str, prev: u64, id: u64, bids: serde_json::Value| Message::Text(serde_json::json!({
                    "type":"data","channel":"orderbook","market":{"type":"perp","id":3},"timestamp":2000,
                    "data":{"updateType":kind,"prevLastUpdateId":prev,"lastUpdateId":id,"engineTime":2000,
                        "bids":bids,"asks":[]}}).to_string().into());
                if socket.send(frame("snapshot", 0, 10, serde_json::json!([
                    {"price":"1792.60","qty":"0.004","value":"7.1704"},
                    {"price":"1792.50","qty":"0.005","value":"8.9625"}]))).await.is_err() { return; }
                if call == 0 {
                    let mut levels = serde_json::json!([
                        {"price":"1792.50","qty":"0","value":"0"},
                        {"price":"1792.60","qty":"0.006","value":"10.7556"},
                        {"price":"1792.70","qty":"0.007","value":"12.5489"}]);
                    if scenario == "precision" { levels[2]["qty"] = serde_json::json!("0.000000001"); }
                    if socket.send(frame("delta", if scenario == "gap" { 9 } else { 10 }, 20, levels)).await.is_err() { return; }
                }
                while let Some(Ok(message)) = socket.recv().await {
                    match message {
                        Message::Close(_) => break,
                        Message::Ping(payload) => { if socket.send(Message::Pong(payload)).await.is_err() { break; } }
                        _ => {},
                    }
                }
            }) }
        }))
    }

    #[rstest]
    #[case("normal")]
    #[case("gap")]
    #[case("precision")]
    #[tokio::test]
    async fn live_book_framework_batches_and_fresh_snapshot_recovery(
        #[case] scenario: &'static str,
    ) {
        use nautilus_model::{
            data::Data,
            enums::{BookAction, RecordFlag},
        };
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            live_book_router(
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::clone(&calls),
                scenario,
                20,
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.subscribe_book_deltas(book_subscribe()).unwrap();
        client.subscribe_book_deltas(book_subscribe()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        release.notify_one();
        let DataEvent::Data(Data::Deltas(initial)) =
            tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected book snapshot");
        };
        assert_eq!(initial.deltas.len(), 3);
        assert_eq!(initial.deltas[0].action, BookAction::Clear);
        assert!(
            initial
                .deltas
                .iter()
                .all(|delta| RecordFlag::F_SNAPSHOT.matches(delta.flags))
        );
        assert!(RecordFlag::F_LAST.matches(initial.deltas.last().unwrap().flags));
        let DataEvent::Data(Data::Deltas(batch)) =
            tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected delta batch or invalidation");
        };
        if scenario == "normal" {
            assert_eq!(
                batch
                    .deltas
                    .iter()
                    .map(|delta| delta.action)
                    .collect::<Vec<_>>(),
                vec![BookAction::Delete, BookAction::Update, BookAction::Add]
            );
            assert!(
                batch
                    .deltas
                    .iter()
                    .all(|delta| delta.sequence == 20 && delta.ts_event.as_millis() == 2000)
            );
            assert_eq!(calls.load(Ordering::Relaxed), 1);
        } else {
            assert_eq!(batch.deltas.len(), 1);
            assert_eq!(batch.deltas[0].action, BookAction::Clear);
            let DataEvent::Data(Data::Deltas(restored)) =
                tokio::time::timeout(Duration::from_secs(3), receiver.recv())
                    .await
                    .unwrap()
                    .unwrap()
            else {
                panic!("expected recovery snapshot");
            };
            assert_eq!(restored.deltas.len(), 3);
            assert_eq!(restored.deltas[0].action, BookAction::Clear);
            assert_eq!(calls.load(Ordering::Relaxed), 2);
        }
        client.unsubscribe_book_deltas(&book_unsubscribe()).unwrap();
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
        client.disconnect().await.unwrap();
    }

    #[rstest]
    #[case("unsubscribe")]
    #[case("disconnect")]
    #[case("stop")]
    #[case("reset")]
    #[case("dispose")]
    #[case("drop")]
    #[tokio::test]
    async fn live_book_retirement_fences_pending_snapshot(#[case] shutdown: &str) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            live_book_router(
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::new(AtomicUsize::new(0)),
                "normal",
                20,
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.subscribe_book_deltas(book_subscribe()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        match shutdown {
            "unsubscribe" => client.unsubscribe_book_deltas(&book_unsubscribe()).unwrap(),
            "disconnect" => client.disconnect().await.unwrap(),
            "stop" => client.stop().unwrap(),
            "reset" => client.reset().unwrap(),
            "dispose" => client.dispose().unwrap(),
            "drop" => {
                drop(client);
                release.notify_one();
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                        .await
                        .is_err()
                );
                return;
            }
            _ => unreachable!(),
        }
        release.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn live_depth10_publishes_exact_full_snapshots() {
        use nautilus_model::{data::Data, enums::RecordFlag};
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            live_book_router(
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::clone(&calls),
                "normal",
                10,
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.subscribe_book_depth10(depth10_subscribe()).unwrap();
        client.subscribe_book_depth10(depth10_subscribe()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        release.notify_one();
        let DataEvent::Data(Data::Depth10(initial)) =
            tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected depth10 snapshot");
        };
        assert_eq!(initial.bids[0].price.to_string(), "1792.6000");
        assert_eq!(initial.bids[0].size.to_string(), "0.0040");
        assert_eq!(initial.bids[1].price.to_string(), "1792.5000");
        assert_eq!(initial.bid_counts[..2], [1, 1]);
        assert!(initial.bids[2..].iter().all(|order| order.price.is_zero()));
        assert_eq!(initial.sequence, 10);
        assert_eq!(initial.ts_event.as_millis(), 2000);
        assert!(RecordFlag::F_SNAPSHOT.matches(initial.flags));
        let DataEvent::Data(Data::Depth10(updated)) =
            tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected updated depth10 snapshot");
        };
        assert_eq!(updated.bids[0].price.to_string(), "1792.7000");
        assert_eq!(updated.bids[1].price.to_string(), "1792.6000");
        assert_eq!(updated.bids[1].size.to_string(), "0.0060");
        assert_eq!(updated.sequence, 20);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        client
            .unsubscribe_book_depth10(&depth10_unsubscribe())
            .unwrap();
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
        client.disconnect().await.unwrap();
    }

    #[rstest]
    #[case("client")]
    #[case("venue")]
    #[case("instrument")]
    #[case("params")]
    #[case("book-type")]
    #[case("depth")]
    #[case("ws-url")]
    #[case("disconnected")]
    #[tokio::test]
    async fn invalid_depth10_admission_never_creates_tasks(#[case] scenario: &str) {
        let (mut client, _receiver, _, _server) =
            client_with_history(serde_json::from_str(PERP).unwrap(), Router::new()).await;
        if scenario != "disconnected" {
            client.connect().await.unwrap();
        }
        let mut command = depth10_subscribe();
        match scenario {
            "client" => command.client_id = Some(ClientId::from("OTHER")),
            "venue" => command.venue = Some(Venue::from("OTHER")),
            "instrument" => command.instrument_id = InstrumentId::from("UNKNOWN.DEEPX"),
            "params" => {
                let mut params = nautilus_core::Params::new();
                params.insert("unsupported".into(), serde_json::json!(true));
                command.params = Some(params);
            }
            "book-type" => command.book_type = nautilus_model::enums::BookType::L3_MBO,
            "depth" => command.depth = NonZeroUsize::new(5),
            "ws-url" => client.config.network.base_url_ws = Some("http://localhost".into()),
            "disconnected" => {}
            _ => unreachable!(),
        }
        assert!(client.subscribe_book_depth10(command).is_err());
        assert!(client.depth10_subscriptions.is_empty());
        assert!(client.tasks.is_empty());
    }

    #[rstest]
    #[case("unsubscribe")]
    #[case("disconnect")]
    #[tokio::test]
    async fn depth10_retirement_fences_pending_snapshot(#[case] action: &str) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            live_book_router(
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::new(AtomicUsize::new(0)),
                "normal",
                10,
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.subscribe_book_depth10(depth10_subscribe()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        if action == "unsubscribe" {
            client
                .unsubscribe_book_depth10(&depth10_unsubscribe())
                .unwrap();
        } else {
            client.disconnect().await.unwrap();
        }
        release.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err()
        );
    }

    fn book_snapshot_request() -> RequestBookSnapshot {
        RequestBookSnapshot::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            NonZeroUsize::new(2),
            Some(ClientId::from("DEEPX-DATA")),
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    fn book_snapshot_router(
        scenario: &'static str,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        calls: Arc<AtomicUsize>,
    ) -> Router {
        use axum::extract::ws::{Message, WebSocketUpgrade};
        Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                let calls = Arc::clone(&calls);
                async move {
                    upgrade.on_upgrade(move |mut socket| async move {
                        let Some(Ok(Message::Text(request))) = socket.recv().await else {
                            return;
                        };
                        let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                        assert_eq!(request["subscriptions"], serde_json::json!(["orderbook"]));
                        assert_eq!(request["options"]["orderbook_depth"], 2);
                        assert_eq!(request["options"]["orderbook_price_size"], 0.01);
                        calls.fetch_add(1, Ordering::Relaxed);
                        let ack = serde_json::json!({"type":"subscribed","market":{"type":"perp","id":3},
                            "subscriptions":["orderbook"],"message":"Subscribed"});
                        if socket
                            .send(Message::Text(ack.to_string().into()))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        entered.notify_one();
                        release.notified().await;
                        let mut bids = serde_json::json!([
                            {"price":"1792.60","qty":"0.004","value":"7.1704"},
                            {"price":"1792.50","qty":"0.005","value":"8.9625"}
                        ]);
                        if scenario == "precision" {
                            bids[0]["qty"] = serde_json::json!("0.00001");
                        }
                        if scenario == "side-depth" {
                            bids.as_array_mut().unwrap().push(serde_json::json!(
                                {"price":"1792.40","qty":"0.008","value":"14.3392"}
                            ));
                        }
                        let frame = serde_json::json!({
                            "type":"data","channel":"orderbook",
                            "market":{"type":"perp","id":3},"timestamp":2001,
                            "data":{"updateType":if scenario == "delta" {"delta"} else {"snapshot"},
                                "prevLastUpdateId":if scenario == "delta" {serde_json::json!(9)} else {serde_json::Value::Null},
                                "lastUpdateId":10,"engineTime":2000,
                                "bids":bids,
                                "asks":[
                                    {"price":"1792.80","qty":"0.006","value":"10.7568"},
                                    {"price":"1792.90","qty":"0.007","value":"12.5503"}
                                ]}
                        });
                        if socket
                            .send(Message::Text(frame.to_string().into()))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        let _ = tokio::time::timeout(Duration::from_secs(2), socket.recv()).await;
                    })
                }
            }),
        )
    }

    #[tokio::test]
    async fn book_snapshot_request_returns_correlated_exact_l2_book() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            book_snapshot_router(
                "valid",
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::clone(&calls),
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        let mut request = book_snapshot_request();
        request.params = Some(nautilus_core::Params::new());
        let expected_id = request.request_id;
        client.request_book_snapshot(request).unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        release.notify_one();
        let DataEvent::Response(DataResponse::Book(response)) =
            tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected book response");
        };
        assert_eq!(response.correlation_id, expected_id);
        assert_eq!(response.client_id, client.client_id);
        assert_eq!(
            response.instrument_id,
            book_snapshot_request().instrument_id
        );
        assert_eq!(response.data.sequence, 10);
        assert_eq!(response.data.ts_last.as_millis(), 2000);
        assert_eq!(response.data.bids(None).count(), 2);
        assert_eq!(response.data.asks(None).count(), 2);
        assert_eq!(
            response.data.best_bid_price().unwrap().to_string(),
            "1792.6000"
        );
        assert_eq!(
            response.data.best_ask_price().unwrap().to_string(),
            "1792.8000"
        );
        assert_eq!(response.start, None);
        assert_eq!(response.end, None);
        assert!(response.params.unwrap().is_empty());
        finish_requests(&client).await;
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    #[case("precision")]
    #[case("delta")]
    #[case("side-depth")]
    #[tokio::test]
    async fn invalid_book_snapshot_never_emits_partial_response(#[case] scenario: &'static str) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            book_snapshot_router(
                scenario,
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::new(AtomicUsize::new(0)),
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client
            .request_book_snapshot(book_snapshot_request())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        release.notify_one();
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    #[case("client")]
    #[case("instrument")]
    #[case("params")]
    #[case("depth")]
    #[case("ws-url")]
    #[case("disconnected")]
    #[tokio::test]
    async fn invalid_book_snapshot_request_fails_before_task_creation(#[case] invalid: &str) {
        let (mut client, _receiver, _, _server) = client(serde_json::from_str(PERP).unwrap()).await;
        if invalid != "disconnected" {
            client.connect().await.unwrap();
        }
        let mut request = book_snapshot_request();
        match invalid {
            "client" => request.client_id = Some(ClientId::from("FOREIGN")),
            "instrument" => request.instrument_id = InstrumentId::from("UNKNOWN.DEEPX"),
            "params" => {
                let mut params = nautilus_core::Params::new();
                params.insert("unsupported".to_string(), serde_json::json!(true));
                request.params = Some(params);
            }
            "depth" => request.depth = NonZeroUsize::new(4097),
            "ws-url" => client.config.network.base_url_ws = Some("http://localhost".into()),
            _ => {}
        }
        assert!(client.request_book_snapshot(request).is_err());
        assert!(client.tasks.is_empty());
    }

    #[rstest]
    #[case("stop")]
    #[case("reset")]
    #[case("dispose")]
    #[case("disconnect")]
    #[tokio::test]
    async fn book_snapshot_response_is_fenced_on_synchronous_retirement(#[case] action: &str) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            book_snapshot_router(
                "valid",
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::new(AtomicUsize::new(0)),
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client
            .request_book_snapshot(book_snapshot_request())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        match action {
            "stop" => client.stop().unwrap(),
            "reset" => client.reset().unwrap(),
            "dispose" => client.dispose().unwrap(),
            _ => client.disconnect().await.unwrap(),
        }
        release.notify_one();
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn book_snapshot_request_has_bounded_initial_data_deadline() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            book_snapshot_router(
                "valid",
                Arc::clone(&entered),
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(AtomicUsize::new(0)),
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.config.websocket_timeout_secs = 1;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client
            .request_book_snapshot(book_snapshot_request())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    #[case("client")]
    #[case("venue")]
    #[case("instrument")]
    #[case("params")]
    #[case("book-type")]
    #[case("depth")]
    #[case("ws-url")]
    #[case("disconnected")]
    #[tokio::test]
    async fn invalid_live_book_admission_never_creates_tasks(#[case] scenario: &str) {
        let (mut client, _receiver, _, _server) =
            client_with_history(serde_json::from_str(PERP).unwrap(), Router::new()).await;
        if scenario != "disconnected" {
            client.connect().await.unwrap();
        }
        let mut command = book_subscribe();
        match scenario {
            "client" => command.client_id = Some(ClientId::from("OTHER")),
            "venue" => command.venue = Some(Venue::from("OTHER")),
            "instrument" => command.instrument_id = InstrumentId::from("UNKNOWN.DEEPX"),
            "params" => {
                let mut params = nautilus_core::Params::new();
                params.insert("unsupported".into(), serde_json::json!(true));
                command.params = Some(params);
            }
            "book-type" => command.book_type = nautilus_model::enums::BookType::L3_MBO,
            "depth" => command.depth = NonZeroUsize::new(4097),
            "ws-url" => client.config.network.base_url_ws = Some("http://localhost".into()),
            "disconnected" => {}
            _ => unreachable!(),
        }
        assert!(client.subscribe_book_deltas(command).is_err());
        assert!(client.book_subscriptions.is_empty());
        assert!(client.tasks.is_empty());
    }

    fn live_trade(id: u64, time: i64) -> serde_json::Value {
        serde_json::json!({"id":id,"marketId":3,"buyerOrderId":"1","buyer":"0x11",
            "sellerOrderId":"2","seller":"0x22","price":"1792.60","size":"0.004",
            "buyerLeverage":"25.0","sellerLeverage":"12.5",
            "createdAt":jiff::Timestamp::from_millisecond(time).unwrap().to_string(),
            "filledDirection":"Both","taker":"Buyer","takerFee":"0","makerFee":"0"})
    }

    fn recovering_trade_router(
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        sockets: Arc<AtomicUsize>,
        reads: Arc<AtomicUsize>,
        scenario: &'static str,
    ) -> Router {
        use axum::extract::ws::{Message, WebSocketUpgrade};
        let ws = get(move |upgrade: WebSocketUpgrade| {
            let sockets = Arc::clone(&sockets);
            async move {
                upgrade.on_upgrade(move |mut socket| async move {
                let Some(Ok(Message::Text(_))) = socket.recv().await else { return; };
                let call = sockets.fetch_add(1, Ordering::Relaxed);
                let ack = serde_json::json!({"type":"subscribed","market":{"type":"perp","id":3},"subscriptions":["trades"],"message":"Subscribed"});
                if socket.send(Message::Text(ack.to_string().into())).await.is_err() { return; }
                let frame = |ids: &[u64]| Message::Text(serde_json::json!({"type":"data","channel":"trades",
                    "market":{"type":"perp","id":3},"timestamp":999999,
                    "data":{"items":ids.iter().map(|id| live_trade(*id, (*id * 1000) as i64)).collect::<Vec<_>>(),"hasNext":true}}).to_string().into());
                if call == 0 && scenario != "initial-timeout" {
                    if socket.send(frame(&[1])).await.is_err() { return; }
                    if socket.send(frame(&[2,1])).await.is_err() { return; }
                    let _ = socket.send(Message::Close(None)).await;
                    return;
                }
                if call != 0 {
                    if socket.send(frame(&[5,4,2,1])).await.is_err() { return; }
                    if socket.send(frame(&[6,5,4])).await.is_err() { return; }
                }
                while let Some(Ok(message)) = socket.recv().await {
                    match message {
                        Message::Close(_) => break,
                        Message::Ping(payload) => { if socket.send(Message::Pong(payload)).await.is_err() { break; } }
                        _ => {},
                    }
                }
            })
            }
        });
        Router::new().route("/internal/v1/ws", ws).route("/internal/v1/market/perp/trades",
            get(move |axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String,String>>| {
                let entered = Arc::clone(&entered); let release = Arc::clone(&release); let reads = Arc::clone(&reads);
                async move {
                    let call = reads.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(query["start"], "2000"); assert_eq!(query["end"], "5000");
                    assert_eq!(query["marketId"], "3"); assert_eq!(query["sort"], "DESC");
                    if call == 0 { entered.notify_one(); release.notified().await; }
                    let cursor = query.get("cursor");
                    let mut rows = if cursor.is_none() { vec![live_trade(5,5000),live_trade(4,4000)] }
                        else { assert_eq!(cursor.unwrap(), "second"); vec![live_trade(3,3000),live_trade(2,2000)] };
                    if cursor.is_some() {
                        if scenario == "missing" || (scenario == "readmit" && call < 2) { rows.pop(); }
                        if scenario == "precision" { rows[0]["size"] = serde_json::json!("0.000000001"); }
                    }
                    axum::Json(serde_json::json!({"code":200,"fail":false,"msg":"success",
                        "data":{"items":rows,"hasNext":cursor.is_none(),"nextCursor":if cursor.is_none() { Some("second") } else { None }}}))
                }
            }))
    }

    #[rstest]
    #[case("valid")]
    #[case("missing")]
    #[case("precision")]
    #[case("readmit")]
    #[case("unsubscribe")]
    #[case("disconnect")]
    #[tokio::test]
    async fn live_trade_reconnect_repairs_gap_before_buffered_live_publication(
        #[case] scenario: &'static str,
    ) {
        use nautilus_model::data::Data;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let sockets = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            recovering_trade_router(
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::clone(&sockets),
                Arc::clone(&reads),
                scenario,
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.subscribe_trades(live_subscribe()).unwrap();
        let DataEvent::Data(Data::Trade(first)) =
            tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected initial live execution");
        };
        assert_eq!(first.trade_id.to_string(), "2");
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        assert!(receiver.try_recv().is_err());
        if scenario == "unsubscribe" {
            client.unsubscribe_trades(&live_unsubscribe()).unwrap();
        }
        if scenario == "disconnect" {
            client.disconnect().await.unwrap();
        }
        release.notify_one();
        if scenario == "readmit" {
            finish_requests(&client).await;
            assert!(receiver.try_recv().is_err());
            assert!(
                !client
                    .trade_subscriptions
                    .values()
                    .next()
                    .unwrap()
                    .is_active()
            );
            client.subscribe_trades(live_subscribe()).unwrap();
        }
        if scenario == "valid" || scenario == "readmit" {
            for id in [3, 4, 5, 6] {
                let DataEvent::Data(Data::Trade(tick)) =
                    tokio::time::timeout(Duration::from_secs(3), receiver.recv())
                        .await
                        .unwrap()
                        .unwrap()
                else {
                    panic!("expected recovered/live execution");
                };
                assert_eq!(tick.trade_id.to_string(), id.to_string());
                assert_eq!(tick.ts_event.as_millis(), id * 1000);
            }
            client.unsubscribe_trades(&live_unsubscribe()).unwrap();
        }
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
        assert_eq!(
            sockets.load(Ordering::Relaxed),
            if scenario == "readmit" { 3 } else { 2 }
        );
        if scenario != "unsubscribe" && scenario != "disconnect" {
            assert_eq!(
                reads.load(Ordering::Relaxed),
                if scenario == "readmit" { 4 } else { 2 }
            );
        }
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn live_trade_initial_data_deadline_retries_without_inventing_a_boundary() {
        let sockets = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            recovering_trade_router(
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(tokio::sync::Notify::new()),
                Arc::clone(&sockets),
                Arc::clone(&reads),
                "initial-timeout",
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.config.websocket_timeout_secs = 1;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.subscribe_trades(live_subscribe()).unwrap();
        let DataEvent::Data(nautilus_model::data::Data::Trade(tick)) =
            tokio::time::timeout(Duration::from_secs(4), receiver.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected live execution after initial-data timeout");
        };
        assert_eq!(tick.trade_id.to_string(), "6");
        assert_eq!(sockets.load(Ordering::Relaxed), 2);
        assert_eq!(reads.load(Ordering::Relaxed), 0);
        client.unsubscribe_trades(&live_unsubscribe()).unwrap();
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
        client.disconnect().await.unwrap();
    }

    fn live_trade_router(
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        calls: Arc<AtomicUsize>,
        invalid: bool,
    ) -> Router {
        use axum::extract::ws::{Message, WebSocketUpgrade};
        Router::new().route("/internal/v1/ws", get(move |upgrade: WebSocketUpgrade| {
            let entered = Arc::clone(&entered); let release = Arc::clone(&release); let calls = Arc::clone(&calls);
            async move { upgrade.on_upgrade(move |mut socket| async move {
                let Some(Ok(Message::Text(request))) = socket.recv().await else { return; };
                let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                assert_eq!(request["action"], "subscribe");
                assert_eq!(request["market"]["id"], 3);
                assert_eq!(request["subscriptions"], serde_json::json!(["trades"]));
                calls.fetch_add(1, Ordering::Relaxed);
                let ack = serde_json::json!({"type":"subscribed","market":{"type":"perp","id":3},"subscriptions":["trades"],"message":"Subscribed"});
                if socket.send(Message::Text(ack.to_string().into())).await.is_err() { return; }
                let frame = |items| Message::Text(serde_json::json!({"type":"data","channel":"trades",
                    "market":{"type":"perp","id":3},"timestamp":3000_u64,
                    "data":{"items":serde_json::Value::Array(items),"hasNext":true}}).to_string().into());
                if socket.send(frame(vec![live_trade(1, 1000)])).await.is_err() { return; }
                entered.notify_one();
                loop {
                    tokio::select! {
                        () = release.notified() => break,
                        message = socket.recv() => {
                            match message {
                                Some(Ok(Message::Text(text))) if text.contains("ping") => {
                                    if socket.send(Message::Text("{\"type\":\"pong\",\"timestamp\":1}".into())).await.is_err() { return; }
                                }
                                _ => return,
                            }
                        }
                    }
                }
                let mut newest = live_trade(3, 3000);
                if invalid { newest["price"] = serde_json::json!("1792.60001"); }
                let push = frame(vec![newest, live_trade(2, 2000), live_trade(1, 1000)]);
                if socket.send(push.clone()).await.is_err() { return; }
                let _ = socket.send(push).await;
                let _ = tokio::time::timeout(Duration::from_secs(2), socket.recv()).await;
            }) }
        }))
    }

    fn live_subscribe() -> SubscribeTrades {
        SubscribeTrades::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            None,
            Some(*DEEPX_VENUE),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    fn live_unsubscribe() -> UnsubscribeTrades {
        UnsubscribeTrades::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            None,
            Some(*DEEPX_VENUE),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    fn mark_price_subscribe() -> SubscribeMarkPrices {
        SubscribeMarkPrices::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            None,
            Some(*DEEPX_VENUE),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    fn mark_price_unsubscribe() -> UnsubscribeMarkPrices {
        UnsubscribeMarkPrices::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            None,
            Some(*DEEPX_VENUE),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    fn index_price_subscribe() -> SubscribeIndexPrices {
        SubscribeIndexPrices::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            None,
            Some(*DEEPX_VENUE),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    fn index_price_unsubscribe() -> UnsubscribeIndexPrices {
        UnsubscribeIndexPrices::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            None,
            Some(*DEEPX_VENUE),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    fn funding_rate_subscribe() -> SubscribeFundingRates {
        SubscribeFundingRates::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            None,
            Some(*DEEPX_VENUE),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    fn funding_rate_unsubscribe() -> UnsubscribeFundingRates {
        UnsubscribeFundingRates::new(
            InstrumentId::from("ETH-USDC-PERP.DEEPX"),
            None,
            Some(*DEEPX_VENUE),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    fn public_price_router(
        channel: &'static str,
        data: serde_json::Value,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        calls: Arc<AtomicUsize>,
    ) -> Router {
        use axum::extract::ws::{Message, WebSocketUpgrade};
        Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| {
                let data = data.clone();
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                let calls = Arc::clone(&calls);
                async move {
                    upgrade.on_upgrade(move |mut socket| async move {
                        let Some(Ok(Message::Text(request))) = socket.recv().await else {
                            return;
                        };
                        let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                        assert_eq!(request["action"], "subscribe");
                        assert_eq!(request["market"]["id"], 3);
                        assert_eq!(request["subscriptions"], serde_json::json!([channel]));
                        calls.fetch_add(1, Ordering::Relaxed);
                        let ack = serde_json::json!({"type":"subscribed","market":{"type":"perp","id":3},
                            "subscriptions":[channel],"message":"Subscribed"});
                        if socket
                            .send(Message::Text(ack.to_string().into()))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        let frame = Message::Text(
                            serde_json::json!({"type":"data","channel":channel,
                                "market":{"type":"perp","id":3},"timestamp":1_789_546_201_652_u64,
                                "data":data})
                            .to_string()
                            .into(),
                        );
                        if socket.send(frame.clone()).await.is_err() {
                            return;
                        }
                        entered.notify_one();
                        release.notified().await;
                        let _ = socket.send(frame).await;
                        let _ = tokio::time::timeout(Duration::from_secs(2), socket.recv()).await;
                    })
                }
            }),
        )
    }

    #[rstest]
    #[case("mark_price")]
    #[case("oracle_price")]
    #[case("funding_rate")]
    #[tokio::test]
    async fn public_price_subscriptions_emit_exact_events_and_fence_unsubscribe(
        #[case] channel: &'static str,
    ) {
        use nautilus_model::data::Data;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let data = match channel {
            "mark_price" => serde_json::json!(2384.511399),
            "oracle_price" => serde_json::json!(2387.095),
            _ => serde_json::json!({"funding_rate":0.000108085430235316,
                "last_cacl_funding_rate_time":1789542862949_u64,
                "last_funding_rate_time":1789546196629_u64}),
        };
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            public_price_router(
                channel,
                data,
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::clone(&calls),
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        match channel {
            "mark_price" => {
                client
                    .subscribe_mark_prices(mark_price_subscribe())
                    .unwrap();
                client
                    .subscribe_mark_prices(mark_price_subscribe())
                    .unwrap();
            }
            "oracle_price" => {
                client
                    .subscribe_index_prices(index_price_subscribe())
                    .unwrap();
                client
                    .subscribe_index_prices(index_price_subscribe())
                    .unwrap();
            }
            _ => {
                client
                    .subscribe_funding_rates(funding_rate_subscribe())
                    .unwrap();
                client
                    .subscribe_funding_rates(funding_rate_subscribe())
                    .unwrap();
            }
        }
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        match (channel, event) {
            ("mark_price", DataEvent::Data(Data::MarkPrice(update))) => {
                assert_eq!(update.value.as_decimal().to_string(), "2384.511399");
                assert_eq!(update.ts_event.as_millis(), 1_789_546_201_652);
            }
            ("oracle_price", DataEvent::Data(Data::IndexPrice(update))) => {
                assert_eq!(update.value.as_decimal().to_string(), "2387.095");
                assert_eq!(update.ts_event.as_millis(), 1_789_546_201_652);
            }
            ("funding_rate", DataEvent::FundingRate(update)) => {
                assert_eq!(update.rate.to_string(), "0.000108085430235316");
                assert_eq!(update.interval, None);
                assert_eq!(update.next_funding_ns, None);
            }
            (_, event) => panic!("unexpected public price event: {event:?}"),
        }
        match channel {
            "mark_price" => client
                .unsubscribe_mark_prices(&mark_price_unsubscribe())
                .unwrap(),
            "oracle_price" => client
                .unsubscribe_index_prices(&index_price_unsubscribe())
                .unwrap(),
            _ => client
                .unsubscribe_funding_rates(&funding_rate_unsubscribe())
                .unwrap(),
        }
        release.notify_one();
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        client.disconnect().await.unwrap();
    }

    #[rstest]
    #[case("client")]
    #[case("venue")]
    #[case("instrument")]
    #[case("params")]
    #[case("ws-url")]
    #[case("disconnected")]
    #[tokio::test]
    async fn invalid_public_price_admission_never_creates_tasks(#[case] invalid: &str) {
        let (mut client, _receiver, _, _server) = client(serde_json::from_str(PERP).unwrap()).await;
        if invalid != "disconnected" {
            client.connect().await.unwrap();
        }
        let mut command = mark_price_subscribe();
        match invalid {
            "client" => command.client_id = Some(ClientId::from("FOREIGN")),
            "venue" => command.venue = Some(Venue::from("FOREIGN")),
            "instrument" => command.instrument_id = InstrumentId::from("UNKNOWN.DEEPX"),
            "params" => {
                let mut params = nautilus_core::Params::default();
                params.insert("unsupported".to_string(), serde_json::json!(true));
                command.params = Some(params);
            }
            "ws-url" => {
                client.config.network.base_url_ws = Some("https://example.invalid".to_string());
            }
            _ => {}
        }
        assert!(client.subscribe_mark_prices(command).is_err());
        assert!(client.mark_price_subscriptions.is_empty());
        assert!(client.tasks.is_empty());
    }

    #[tokio::test]
    async fn stop_synchronously_retires_all_public_price_subscriptions() {
        let (mut client, _receiver, _, _server) = client(serde_json::from_str(PERP).unwrap()).await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        client
            .subscribe_mark_prices(mark_price_subscribe())
            .unwrap();
        client
            .subscribe_index_prices(index_price_subscribe())
            .unwrap();
        client
            .subscribe_funding_rates(funding_rate_subscribe())
            .unwrap();
        let mark = client
            .mark_price_subscriptions
            .values()
            .next()
            .unwrap()
            .clone();
        let index = client
            .index_price_subscriptions
            .values()
            .next()
            .unwrap()
            .clone();
        let funding = client
            .funding_rate_subscriptions
            .values()
            .next()
            .unwrap()
            .clone();
        client.stop().unwrap();
        assert!(!mark.is_active());
        assert!(!index.is_active());
        assert!(!funding.is_active());
        assert!(client.mark_price_subscriptions.is_empty());
        assert!(client.index_price_subscriptions.is_empty());
        assert!(client.funding_rate_subscriptions.is_empty());
        finish_requests(&client).await;
        assert!(client.tasks.is_empty());
    }

    #[rstest]
    #[case("client")]
    #[case("venue")]
    #[case("instrument")]
    #[case("params")]
    #[case("ws-url")]
    #[case("disconnected")]
    #[tokio::test]
    async fn invalid_live_trade_subscriptions_fail_before_task_or_socket_creation(
        #[case] invalid: &str,
    ) {
        let (mut client, _receiver, _, _server) = client(serde_json::from_str(PERP).unwrap()).await;
        if invalid != "disconnected" {
            client.connect().await.unwrap();
        }
        let mut command = live_subscribe();
        match invalid {
            "client" => command.client_id = Some(ClientId::from("FOREIGN")),
            "venue" => command.venue = Some(Venue::from("FOREIGN")),
            "instrument" => command.instrument_id = InstrumentId::from("UNKNOWN.DEEPX"),
            "params" => {
                let mut params = nautilus_core::Params::default();
                params.insert("unsupported".to_string(), serde_json::json!(true));
                command.params = Some(params);
            }
            "ws-url" => {
                client.config.network.base_url_ws = Some("https://example.invalid".to_string());
            }
            _ => {}
        }
        assert!(client.subscribe_trades(command).is_err());
        assert!(client.trade_subscriptions.is_empty());
        assert!(client.tasks.is_empty());
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn live_trade_subscription_emits_atomic_chronological_deduplicated_ticks(
        #[case] invalid: bool,
    ) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            live_trade_router(
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::clone(&calls),
                invalid,
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.subscribe_trades(live_subscribe()).unwrap();
        client.subscribe_trades(live_subscribe()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), receiver.recv())
                .await
                .is_err()
        );
        release.notify_one();
        if invalid {
            finish_requests(&client).await;
            assert!(receiver.try_recv().is_err());
            assert!(
                !client
                    .trade_subscriptions
                    .values()
                    .next()
                    .unwrap()
                    .is_active()
            );
        } else {
            for (id, time) in [("2", 2000_u64), ("3", 3000)] {
                let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                    .await
                    .unwrap()
                    .unwrap();
                let DataEvent::Data(nautilus_model::data::Data::Trade(tick)) = event else {
                    panic!("expected live trade");
                };
                assert_eq!(tick.trade_id.to_string(), id);
                assert_eq!(tick.ts_event.as_millis(), time);
                assert_eq!(tick.instrument_id, live_subscribe().instrument_id);
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(30), receiver.recv())
                    .await
                    .is_err()
            );
            client.unsubscribe_trades(&live_unsubscribe()).unwrap();
            finish_requests(&client).await;
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        client.disconnect().await.unwrap();
    }

    #[rstest]
    #[case("unsubscribe")]
    #[case("disconnect")]
    #[case("stop")]
    #[case("reset")]
    #[case("dispose")]
    #[case("drop")]
    #[tokio::test]
    async fn live_trade_publication_is_fenced_on_retirement(#[case] shutdown: &str) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            live_trade_router(
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::new(AtomicUsize::new(0)),
                false,
            ),
        )
        .await;
        client.config.network.base_url_ws =
            Some(client.http.base_url().replacen("http://", "ws://", 1));
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.subscribe_trades(live_subscribe()).unwrap();
        let subscription = client.trade_subscriptions.values().next().unwrap().clone();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        match shutdown {
            "unsubscribe" => client.unsubscribe_trades(&live_unsubscribe()).unwrap(),
            "disconnect" => client.disconnect().await.unwrap(),
            "stop" => client.stop().unwrap(),
            "reset" => client.reset().unwrap(),
            "dispose" => client.dispose().unwrap(),
            _ => drop(client),
        }
        assert!(!subscription.is_active());
        release.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), receiver.recv())
                .await
                .is_err()
        );
    }

    async fn finish_requests(client: &DeepXDataClient) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !client.tasks.is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }

    #[rstest]
    #[case("full", 4, 2)]
    #[case("limited", 1, 1)]
    #[case("nanosecond-bounds", 1, 1)]
    #[case("empty", 0, 1)]
    #[case("default-bounds", 0, 1)]
    #[case("empty-params", 4, 2)]
    #[tokio::test]
    async fn historical_trade_response_is_correlated_and_chronological(
        #[case] scenario: &str,
        #[case] count: usize,
        #[case] pages: usize,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            history_router("valid", Arc::clone(&calls), None),
        )
        .await;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        let mut request = trade_request();
        match scenario {
            "limited" => request.limit = NonZeroUsize::new(1),
            "nanosecond-bounds" => {
                request.start = Some(jiff::Timestamp::from_nanosecond(1_500_000_001).unwrap());
                request.end = Some(jiff::Timestamp::from_nanosecond(2_999_999_999).unwrap());
            }
            "empty" => {
                request.start = Some(jiff::Timestamp::from_millisecond(4_000).unwrap());
                request.end = Some(jiff::Timestamp::from_millisecond(5_000).unwrap());
            }
            "default-bounds" => {
                request.start = None;
                request.end = None;
            }
            "empty-params" => request.params = Some(nautilus_core::Params::new()),
            _ => {}
        }
        let expected = request.clone();
        client.request_trades(request).unwrap();
        let event = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let DataEvent::Response(DataResponse::Trades(response)) = event else {
            panic!("expected trades response")
        };
        assert_eq!(response.correlation_id, expected.request_id);
        assert_eq!(response.client_id, client.client_id);
        assert_eq!(response.instrument_id, expected.instrument_id);
        assert_eq!(
            response.start,
            expected
                .start
                .map(try_datetime_to_unix_nanos)
                .transpose()
                .unwrap()
        );
        assert_eq!(
            response.end,
            expected
                .end
                .map(try_datetime_to_unix_nanos)
                .transpose()
                .unwrap()
        );
        assert_eq!(response.data.len(), count);
        assert!(
            response
                .data
                .windows(2)
                .all(|pair| pair[0].ts_event <= pair[1].ts_event)
        );
        for trade in &response.data {
            assert_eq!(trade.ts_init, response.ts_init);
        }
        if scenario == "full" {
            assert_eq!(response.data[0].trade_id.to_string(), "1");
            assert_eq!(response.data[3].trade_id.to_string(), "4");
            assert_eq!(
                response.data[0].aggressor_side,
                nautilus_model::enums::AggressorSide::Sell
            );
            assert_eq!(
                response.data[3].aggressor_side,
                nautilus_model::enums::AggressorSide::Buy
            );
            assert_eq!(response.data[0].price.as_decimal().to_string(), "1792.6000");
        }
        if scenario == "limited" {
            assert_eq!(response.data[0].trade_id.to_string(), "4");
        }
        finish_requests(&client).await;
        assert_eq!(calls.load(Ordering::Relaxed), pages);
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    #[case("unknown-taker")]
    #[case("price-precision")]
    #[case("size-precision")]
    #[case("zero-size")]
    #[case("negative-size")]
    #[case("negative-price")]
    #[case("invalid-time")]
    #[case("submillisecond")]
    #[case("zero-id")]
    #[case("foreign-market")]
    #[tokio::test]
    async fn invalid_historical_trade_never_emits_partial_response(#[case] scenario: &'static str) {
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            history_router(scenario, calls, None),
        )
        .await;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.request_trades(trade_request()).unwrap();
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    #[case("stop")]
    #[case("reset")]
    #[case("dispose")]
    #[case("disconnect")]
    #[tokio::test]
    async fn historical_trade_tasks_are_fenced_across_reconnect(#[case] shutdown: &str) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            history_router(
                "valid",
                Arc::clone(&calls),
                Some((Arc::clone(&entered), Arc::clone(&release))),
            ),
        )
        .await;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        client.request_trades(trade_request()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        match shutdown {
            "stop" => client.stop().unwrap(),
            "reset" => client.reset().unwrap(),
            "dispose" => client.dispose().unwrap(),
            "disconnect" => client.disconnect().await.unwrap(),
            _ => unreachable!(),
        }
        assert!(client.request_trades(trade_request()).is_err());
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        release.notify_one();
        let request = trade_request();
        let expected_id = request.request_id;
        client.request_trades(request).unwrap();
        let event = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let DataEvent::Response(DataResponse::Trades(response)) = event else {
            panic!("expected trades response")
        };
        assert_eq!(response.correlation_id, expected_id);
        finish_requests(&client).await;
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    #[case("disconnected")]
    #[case("client")]
    #[case("venue")]
    #[case("spot")]
    #[case("unknown")]
    #[case("params")]
    #[case("range")]
    #[case("negative-time")]
    #[case("limit")]
    #[tokio::test]
    async fn historical_trade_validation_fails_before_transport(#[case] invalid: &str) {
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            history_router("valid", Arc::clone(&calls), None),
        )
        .await;
        if invalid != "disconnected" {
            client.connect().await.unwrap();
        }
        while receiver.try_recv().is_ok() {}
        let mut request = trade_request();
        match invalid {
            "client" => request.client_id = Some(ClientId::from("OTHER")),
            "venue" => request.instrument_id = InstrumentId::from("ETH-USDC-PERP.OTHER"),
            "spot" => request.instrument_id = InstrumentId::from("ETH-USDC.DEEPX"),
            "unknown" => request.instrument_id = InstrumentId::from("UNKNOWN-USDC-PERP.DEEPX"),
            "params" => {
                let mut params = nautilus_core::Params::new();
                params.insert("unsupported".to_string(), serde_json::json!(true));
                request.params = Some(params);
            }
            "range" => request.start = Some(jiff::Timestamp::from_millisecond(4_000).unwrap()),
            "negative-time" => request.start = Some(jiff::Timestamp::from_millisecond(-1).unwrap()),
            "limit" => request.limit = NonZeroUsize::new(10_001),
            _ => {}
        }
        assert!(client.request_trades(request).is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn dropping_data_client_retires_pending_trade_response_epoch() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (mut client, mut receiver, _, _server) = client_with_history(
            serde_json::from_str(PERP).unwrap(),
            history_router(
                "valid",
                Arc::new(AtomicUsize::new(0)),
                Some((Arc::clone(&entered), Arc::clone(&release))),
            ),
        )
        .await;
        client.connect().await.unwrap();
        while receiver.try_recv().is_ok() {}
        let epoch = Arc::clone(&client.request_epoch);
        client.request_trades(trade_request()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        drop(client);
        assert!(!*epoch.lock().unwrap());
        release.notify_one();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn captured_spec369_markets_load_and_publish_every_perpetual() {
        let manifest: serde_json::Value = serde_json::from_str(include_str!(
            "../test_data/http/testnet/perp_markets_spec369.manifest.json"
        ))
        .unwrap();
        let digest =
            aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, CAPTURED_PERP.as_bytes());
        assert_eq!(
            nautilus_core::hex::encode(digest.as_ref()),
            manifest["response"]["payload_sha256"].as_str().unwrap()
        );
        let (mut client, mut receiver, requests, _server) =
            client(serde_json::from_str(CAPTURED_PERP).unwrap()).await;
        client.connect().await.unwrap();
        let mut ids = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            let DataEvent::Instrument(instrument) = event else {
                panic!("expected instrument")
            };
            ids.push(instrument.id());
        }
        assert_eq!(ids.len(), 9);
        assert_eq!(
            ids,
            client
                .provider
                .store()
                .list_all()
                .iter()
                .map(|instrument| instrument.id())
                .collect::<Vec<_>>()
        );
        assert_eq!(requests.load(Ordering::Relaxed), 1);
        assert!(client.is_connected());
        assert!(ids.contains(&InstrumentId::from("BTC-USDC-PERP.DEEPX")));
        assert!(ids.contains(&InstrumentId::from("ETH-USDC-PERP.DEEPX")));
    }

    #[tokio::test]
    async fn rest_connect_and_instrument_requests_reach_framework() {
        let (mut client, mut receiver, requests, _server) =
            client(serde_json::from_str(PERP).unwrap()).await;
        assert!(client.is_disconnected());
        assert!(client.request_instruments(all_request(&client)).is_err());
        client.connect().await.unwrap();
        assert!(client.is_connected());
        let DataEvent::Instrument(instrument) = receiver.try_recv().unwrap() else {
            panic!("expected instrument event")
        };
        assert_eq!(instrument.id(), InstrumentId::from("ETH-USDC-PERP.DEEPX"));
        assert!(receiver.try_recv().is_err());
        client.connect().await.unwrap();
        assert_eq!(requests.load(Ordering::Relaxed), 1);
        assert!(receiver.try_recv().is_err());

        let request = all_request(&client);
        let id = request.request_id;
        client.request_instruments(request).unwrap();
        let DataEvent::Response(DataResponse::Instruments(response)) = receiver.try_recv().unwrap()
        else {
            panic!("expected instruments response")
        };
        assert_eq!(response.correlation_id, id);
        assert_eq!(response.client_id, client.client_id);
        assert_eq!(response.venue, *DEEPX_VENUE);
        assert_eq!(response.data.len(), 1);
        assert_eq!(response.ts_init, UnixNanos::from(123u64));
        assert_eq!(response.start, None);
        assert_eq!(response.end, None);

        let request = one_request(&client, "ETH-USDC-PERP.DEEPX");
        let id = request.request_id;
        client.request_instrument(request).unwrap();
        let DataEvent::Response(DataResponse::Instrument(response)) = receiver.try_recv().unwrap()
        else {
            panic!("expected instrument response")
        };
        assert_eq!(response.correlation_id, id);
        assert_eq!(response.instrument_id, instrument.id());
        assert_eq!(response.data, instrument);
        assert_eq!(response.ts_init, UnixNanos::from(123u64));
        assert_eq!(requests.load(Ordering::Relaxed), 1);
    }

    #[rstest]
    #[case::disconnect(0)]
    #[case::stop(1)]
    #[case::reset(2)]
    #[case::dispose(3)]
    #[tokio::test]
    async fn rest_lifecycle_clears_readiness_and_reloads(#[case] action: u8) {
        let (mut client, mut receiver, requests, _server) =
            client(serde_json::from_str(PERP).unwrap()).await;
        client.connect().await.unwrap();
        receiver.try_recv().unwrap();
        match action {
            0 => client.disconnect().await.unwrap(),
            1 => client.stop().unwrap(),
            2 => client.reset().unwrap(),
            _ => client.dispose().unwrap(),
        }
        assert!(client.is_disconnected());
        assert!(!client.provider.store().is_initialized());
        assert!(client.provider.store().is_empty());
        assert!(client.request_instruments(all_request(&client)).is_err());
        client.connect().await.unwrap();
        assert!(client.is_connected());
        assert_eq!(requests.load(Ordering::Relaxed), 2);
        assert!(matches!(receiver.try_recv(), Ok(DataEvent::Instrument(_))));
    }

    #[rstest]
    #[case::wrong_client(0)]
    #[case::wrong_venue(1)]
    #[case::history(2)]
    #[case::params(3)]
    #[case::spot(4)]
    #[case::unknown(5)]
    #[tokio::test]
    async fn rest_requests_reject_unproven_scope_without_emission(#[case] mutation: u8) {
        let (mut client, mut receiver, requests, _server) =
            client(serde_json::from_str(PERP).unwrap()).await;
        client.connect().await.unwrap();
        receiver.try_recv().unwrap();
        if mutation < 4 {
            let mut request = all_request(&client);
            match mutation {
                0 => request.client_id = Some(ClientId::from("OTHER")),
                1 => request.venue = Some(Venue::from("OTHER")),
                2 => request.start = Some(jiff::Timestamp::from_second(1).unwrap()),
                _ => {
                    let mut params = nautilus_core::Params::new();
                    params.insert("filter".to_string(), serde_json::json!("unknown"));
                    request.params = Some(params);
                }
            }
            assert!(client.request_instruments(request).is_err());
        } else {
            let id = if mutation == 4 {
                "ETH-USDC-SPOT.DEEPX"
            } else {
                "BTC-USDC-PERP.DEEPX"
            };
            assert!(client.request_instrument(one_request(&client, id)).is_err());
        }
        assert!(receiver.try_recv().is_err());
        assert_eq!(requests.load(Ordering::Relaxed), 1);
    }

    #[rstest]
    #[case::invalid_increment(0)]
    #[case::empty_perpetuals(1)]
    #[case::venue_failure(2)]
    #[case::invalid_second_market(3)]
    #[tokio::test]
    async fn rest_connect_failure_emits_no_partial_instruments(#[case] mutation: u8) {
        let mut response: serde_json::Value = serde_json::from_str(PERP).unwrap();
        match mutation {
            0 => response["data"][0]["orderSpecStepSize"] = serde_json::json!("0"),
            1 => response["data"] = serde_json::json!([]),
            2 => {
                response["fail"] = serde_json::json!(true);
                response["code"] = serde_json::json!(10014);
            }
            _ => {
                let mut invalid = response["data"][0].clone();
                invalid["id"] = serde_json::json!(4);
                invalid["baseSymbol"] = serde_json::json!("sol");
                invalid["name"] = serde_json::json!("SOL-USDC");
                invalid["orderSpecStepSize"] = serde_json::json!("0");
                response["data"].as_array_mut().unwrap().push(invalid);
            }
        }
        let (mut client, mut receiver, _, _server) = client(response).await;
        assert!(client.connect().await.is_err());
        assert!(client.is_disconnected());
        assert!(client.provider.store().is_empty());
        assert!(!client.provider.store().is_initialized());
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn closed_event_receiver_revokes_readiness() {
        let (mut client, receiver, requests, _server) =
            client(serde_json::from_str(PERP).unwrap()).await;
        client.connect().await.unwrap();
        drop(receiver);
        assert!(client.is_disconnected());
        assert!(client.request_instruments(all_request(&client)).is_err());
        assert!(client.connect().await.is_err());
        assert_eq!(requests.load(Ordering::Relaxed), 1);
    }
}
