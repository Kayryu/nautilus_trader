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

//! Verify testnet REST instrument discovery through the Nautilus data client.
//! No account access, signing, WebSocket subscriptions, or transaction submission.

use std::{cell::RefCell, rc::Rc};

use anyhow::ensure;
use nautilus_common::{
    cache::Cache,
    clients::DataClient,
    live::{clock::LiveClock, runner::set_data_event_sender},
    messages::{DataEvent, DataResponse, data::RequestInstruments},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_deepx::{
    common::consts::DEEPX_VENUE,
    config::DeepXDataClientConfig,
    data::DeepXDataClient,
    http::{DeepXHttpClient, DeepXPerpMarketLookup},
};
use nautilus_model::{identifiers::ClientId, instruments::Instrument};
use tokio::sync::mpsc::unbounded_channel;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = DeepXDataClientConfig::default();
    let http_client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let markets = http_client.get_perp_markets().await?;
    let by_id = http_client.get_perp_market_by_id(3).await?;
    let by_name = http_client.get_perp_market_by_name("ETH-USDC").await?;
    let directory_market = markets
        .iter()
        .find(|market| market.id == 3)
        .ok_or_else(|| anyhow::anyhow!("perpetual market 3 is absent from the directory"))?;
    verify_lookup(directory_market, &by_id)?;
    verify_lookup(directory_market, &by_name)?;

    let (sender, mut receiver) = unbounded_channel();
    set_data_event_sender(sender);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let clock = Rc::new(RefCell::new(LiveClock::default()));
    let mut client = DeepXDataClient::new(
        ClientId::from("DEEPX-REST-CHECK"),
        config,
        cache.into(),
        clock,
    )?;
    client.connect().await?;
    let mut published = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        let DataEvent::Instrument(instrument) = event else {
            anyhow::bail!("unexpected startup event");
        };
        published.push(instrument.id());
    }
    let request_id = UUID4::new();
    client.request_instruments(RequestInstruments::new(
        None,
        None,
        Some(client.client_id()),
        Some(*DEEPX_VENUE),
        request_id,
        UnixNanos::default(),
        None,
    ))?;
    let DataEvent::Response(DataResponse::Instruments(response)) = receiver.try_recv()? else {
        anyhow::bail!("expected instruments response");
    };
    ensure!(
        response.correlation_id == request_id,
        "request correlation mismatch"
    );
    ensure!(
        response.data.iter().map(Instrument::id).collect::<Vec<_>>() == published,
        "startup/query instrument mismatch"
    );
    for instrument in response.data {
        println!("{}", instrument.id());
    }
    client.disconnect().await?;
    ensure!(client.is_disconnected(), "client did not disconnect");
    println!(
        "REST instrument discovery verified markets={} lookup={}; no trading or streaming activated",
        markets.len(),
        by_id.name,
    );
    Ok(())
}

fn verify_lookup(
    directory: &nautilus_deepx::http::DeepXPerpMarket,
    lookup: &DeepXPerpMarketLookup,
) -> anyhow::Result<()> {
    ensure!(lookup.id == directory.id, "market ID mismatch");
    ensure!(lookup.name == directory.name, "market name mismatch");
    ensure!(
        lookup.base_symbol == directory.base_symbol
            && lookup.quote_symbol == directory.quote_symbol,
        "market symbol mismatch"
    );
    ensure!(
        lookup.base_address == directory.base_address
            && lookup.quote_address == directory.quote_address,
        "market address mismatch"
    );
    ensure!(
        lookup.base_decimal == directory.base_decimal
            && lookup.quote_decimal == directory.quote_decimal,
        "market precision mismatch"
    );
    ensure!(
        lookup.initial_margin_ratio == directory.initial_margin_ratio
            && lookup.maintenance_margin_ratio == directory.maintenance_margin_ratio,
        "market margin mismatch"
    );
    ensure!(
        lookup.taker_fee_rate == directory.taker_fee_rate
            && lookup.maker_fee_rate == directory.maker_fee_rate,
        "market fee mismatch"
    );
    ensure!(
        lookup.order_spec_min_qty == directory.order_spec_min_qty
            && lookup.order_spec_min_notional == directory.order_spec_min_notional,
        "market minimum-order mismatch"
    );
    Ok(())
}
