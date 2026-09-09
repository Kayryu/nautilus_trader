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

//! Fail-closed DeepX data client framework boundary.

use std::{cell::RefCell, rc::Rc};

use async_trait::async_trait;
use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    messages::data::{
        RequestBars, RequestBookDeltas, RequestBookDepth, RequestBookSnapshot, RequestCustomData,
        RequestForwardPrices, RequestFundingRates, RequestInstrument, RequestInstruments,
        RequestQuotes, RequestTrades, SubscribeBars, SubscribeBookDeltas, SubscribeBookDepth10,
        SubscribeCustomData, SubscribeFundingRates, SubscribeIndexPrices, SubscribeInstrument,
        SubscribeInstrumentClose, SubscribeInstrumentStatus, SubscribeInstruments,
        SubscribeMarkPrices, SubscribeOptionGreeks, SubscribeQuotes, SubscribeTrades,
        UnsubscribeBars, UnsubscribeBookDeltas, UnsubscribeBookDepth10, UnsubscribeCustomData,
        UnsubscribeFundingRates, UnsubscribeIndexPrices, UnsubscribeInstrument,
        UnsubscribeInstrumentClose, UnsubscribeInstrumentStatus, UnsubscribeInstruments,
        UnsubscribeMarkPrices, UnsubscribeOptionGreeks, UnsubscribeQuotes, UnsubscribeTrades,
    },
};
use nautilus_model::identifiers::{ClientId, Venue};

use crate::{
    common::{DeepXError, consts::DEEPX_VENUE},
    config::DeepXDataClientConfig,
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

/// Disconnected DeepX data client.
///
/// Network startup remains disabled until fixture evidence proves public WebSocket subscription
/// and replay semantics.
pub struct DeepXDataClient {
    client_id: ClientId,
    config: DeepXDataClientConfig,
    cache: CacheView,
    clock: Rc<RefCell<dyn Clock>>,
}

impl std::fmt::Debug for DeepXDataClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXDataClient))
            .field("client_id", &self.client_id)
            .field("config", &self.config)
            .field("cache", &"<cache-view>")
            .field("clock", &"<clock>")
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
        Ok(Self {
            client_id,
            config,
            cache,
            clock,
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
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        false
    }

    fn is_disconnected(&self) -> bool {
        true
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        Err(DeepXError::UnsupportedCapability(
            "data client network startup requires fixture-proven public WebSocket semantics",
        )
        .into())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    unsupported_owned_commands! {
        subscribe(command: SubscribeCustomData) => "custom data subscriptions";
        subscribe_instruments(command: SubscribeInstruments) => "instrument subscriptions";
        subscribe_instrument(command: SubscribeInstrument) => "instrument subscriptions";
        subscribe_book_deltas(command: SubscribeBookDeltas) => "order book delta subscriptions";
        subscribe_book_depth10(command: SubscribeBookDepth10) => "order book depth subscriptions";
        subscribe_quotes(command: SubscribeQuotes) => "quote subscriptions";
        subscribe_trades(command: SubscribeTrades) => "trade subscriptions";
        subscribe_mark_prices(command: SubscribeMarkPrices) => "mark price subscriptions";
        subscribe_index_prices(command: SubscribeIndexPrices) => "index price subscriptions";
        subscribe_funding_rates(command: SubscribeFundingRates) => "funding rate subscriptions";
        subscribe_bars(command: SubscribeBars) => "bar subscriptions";
        subscribe_instrument_status(command: SubscribeInstrumentStatus) => "instrument status subscriptions";
        subscribe_instrument_close(command: SubscribeInstrumentClose) => "instrument close subscriptions";
        subscribe_option_greeks(command: SubscribeOptionGreeks) => "option greeks subscriptions";
    }

    unsupported_borrowed_commands! {
        unsubscribe(command: UnsubscribeCustomData) => "custom data subscriptions";
        unsubscribe_instruments(command: UnsubscribeInstruments) => "instrument subscriptions";
        unsubscribe_instrument(command: UnsubscribeInstrument) => "instrument subscriptions";
        unsubscribe_book_deltas(command: UnsubscribeBookDeltas) => "order book delta subscriptions";
        unsubscribe_book_depth10(command: UnsubscribeBookDepth10) => "order book depth subscriptions";
        unsubscribe_quotes(command: UnsubscribeQuotes) => "quote subscriptions";
        unsubscribe_trades(command: UnsubscribeTrades) => "trade subscriptions";
        unsubscribe_mark_prices(command: UnsubscribeMarkPrices) => "mark price subscriptions";
        unsubscribe_index_prices(command: UnsubscribeIndexPrices) => "index price subscriptions";
        unsubscribe_funding_rates(command: UnsubscribeFundingRates) => "funding rate subscriptions";
        unsubscribe_bars(command: UnsubscribeBars) => "bar subscriptions";
        unsubscribe_instrument_status(command: UnsubscribeInstrumentStatus) => "instrument status subscriptions";
        unsubscribe_instrument_close(command: UnsubscribeInstrumentClose) => "instrument close subscriptions";
        unsubscribe_option_greeks(command: UnsubscribeOptionGreeks) => "option greeks subscriptions";
    }

    unsupported_requests! {
        request_data(request: RequestCustomData) => "custom data requests";
        request_instruments(request: RequestInstruments) => "instrument requests";
        request_instrument(request: RequestInstrument) => "instrument requests";
        request_book_snapshot(request: RequestBookSnapshot) => "order book snapshot requests";
        request_quotes(request: RequestQuotes) => "quote requests";
        request_trades(request: RequestTrades) => "trade requests";
        request_funding_rates(request: RequestFundingRates) => "funding rate requests";
        request_forward_prices(request: RequestForwardPrices) => "forward price requests";
        request_bars(request: RequestBars) => "bar requests";
        request_book_depth(request: RequestBookDepth) => "order book depth requests";
        request_book_deltas(request: RequestBookDeltas) => "order book delta requests";
    }
}
