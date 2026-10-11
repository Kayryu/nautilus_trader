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
//  See the GNU Lesser General Public License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live read-only verification of an initial Spot open-order WebSocket snapshot.

use std::time::Duration;

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::DeepXHttpClient,
    websocket::{
        DeepXWsSpotOpenOrdersRequest, observe_initial_spot_open_orders_page,
        observe_initial_spot_wallet_open_orders_pages,
    },
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let first = args.next().context(
        "usage: deepx-verify-ws-spot-open-orders [--wallet] <account-id20> <market-name>",
    )?;
    let wallet_mode = first == "--wallet";
    let address = if wallet_mode {
        args.next().context("missing wallet address")?
    } else {
        first
    };
    let market_name = args.next().context("missing Spot market name")?;
    ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let market = client
        .get_spot_markets()
        .await?
        .into_iter()
        .find(|market| market.name == market_name)
        .context("Spot market is absent from the current directory")?;
    if wallet_mode {
        let directory = client.get_wallet_subaccounts(&address).await?;
        let request =
            DeepXWsSpotOpenOrdersRequest::for_wallet(&market, &address, &directory.addresses)?;
        let pages = observe_initial_spot_wallet_open_orders_pages(
            &config.network,
            config.proxy_url.as_deref(),
            Duration::from_secs(35),
            &request,
        )
        .await?;
        for (page, timestamp_ms) in &pages {
            println!(
                "DeepX initial Spot order page subaccount={} market={} raw_orders={} timestamp_ms={}",
                page.subaccount(),
                request.market_name(),
                page.items().len(),
                timestamp_ms,
            );
        }
        println!(
            "DeepX wallet Spot order snapshot verified wallet={} subaccounts={}; mutable read-only observation, no credentials or transactions sent",
            directory.wallet(),
            pages.len(),
        );
    } else {
        let request = DeepXWsSpotOpenOrdersRequest::new(&market, &address)?;
        let (page, timestamp_ms) = observe_initial_spot_open_orders_page(
            &config.network,
            config.proxy_url.as_deref(),
            Duration::from_secs(35),
            &request,
        )
        .await?;
        println!(
            "DeepX initial Spot order snapshot verified subaccount={} market={} raw_orders={} timestamp_ms={}; no credentials or transactions sent",
            page.subaccount(),
            request.market_name(),
            page.items().len(),
            timestamp_ms,
        );
    }
    Ok(())
}
