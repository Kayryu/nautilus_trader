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

//! Live read-only verification of an initial perpetual open-order WebSocket snapshot.

use std::time::Duration;

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::DeepXHttpClient,
    websocket::{
        DeepXWsPerpOpenOrdersRequest, observe_initial_perp_open_orders_page,
        observe_initial_perp_wallet_open_orders_pages,
    },
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let first = args
        .next()
        .context("usage: deepx-verify-ws-perp-open-orders [--wallet] <account-id20> <market-id>")?;
    let wallet_mode = first == "--wallet";
    let address = if wallet_mode {
        args.next().context("missing wallet address")?
    } else {
        first
    };
    let market_id = args
        .next()
        .context("missing perpetual market ID")?
        .parse::<u16>()
        .context("perpetual market ID must be a u16")?;
    ensure!(args.next().is_none(), "unexpected extra argument");
    let config = DeepXDataClientConfig::default();
    if wallet_mode {
        let client = DeepXHttpClient::from_network_config(
            &config.network,
            Some(config.http_timeout_secs),
            config.proxy_url.clone(),
        )?;
        let directory = client.get_wallet_subaccounts(&address).await?;
        let request =
            DeepXWsPerpOpenOrdersRequest::for_wallet(market_id, &address, &directory.addresses)?;
        let pages = observe_initial_perp_wallet_open_orders_pages(
            &config.network,
            config.proxy_url.as_deref(),
            Duration::from_secs(35),
            &request,
        )
        .await?;
        for (page, timestamp_ms) in &pages {
            println!(
                "DeepX initial perpetual order page subaccount={} market_id={} raw_orders={} timestamp_ms={}",
                page.subaccount(),
                request.market_id(),
                page.items().len(),
                timestamp_ms,
            );
        }
        println!(
            "DeepX wallet perpetual order snapshot verified wallet={} subaccounts={}; mutable read-only observation, no credentials or transactions sent",
            directory.wallet(),
            pages.len(),
        );
    } else {
        let request = DeepXWsPerpOpenOrdersRequest::new(market_id, &address)?;
        let (page, timestamp_ms) = observe_initial_perp_open_orders_page(
            &config.network,
            config.proxy_url.as_deref(),
            Duration::from_secs(35),
            &request,
        )
        .await?;
        println!(
            "DeepX initial perpetual order snapshot verified subaccount={} market_id={} raw_orders={} timestamp_ms={}; no credentials or transactions sent",
            page.subaccount(),
            request.market_id(),
            page.items().len(),
            timestamp_ms,
        );
    }
    Ok(())
}
