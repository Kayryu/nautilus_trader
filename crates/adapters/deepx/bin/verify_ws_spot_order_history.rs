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

//! Live read-only verification of Spot order-history subscription acknowledgement.

use std::time::Duration;

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::DeepXHttpClient,
    websocket::{DeepXWsSpotOrderHistoryRequest, observe_spot_order_history_ack},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-ws-spot-order-history <wallet-account-id20> <market-name>")?;
    let market_name = args.next().context("missing Spot market name")?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let http = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let directory = http.get_wallet_subaccounts(&wallet).await?;
    anyhow::ensure!(
        !directory.addresses.is_empty(),
        "wallet directory has no subaccounts"
    );
    let market = http
        .get_spot_markets()
        .await?
        .into_iter()
        .find(|market| market.name == market_name)
        .context("Spot market is absent from the current directory")?;

    for subaccount in &directory.addresses {
        let request = DeepXWsSpotOrderHistoryRequest::new(&market, subaccount)?;
        observe_spot_order_history_ack(
            &config.network,
            config.proxy_url.as_deref(),
            Duration::from_secs(config.websocket_timeout_secs),
            &request,
        )
        .await?;
    }
    println!(
        "DeepX Spot order-history subscriptions acknowledged wallet={} market={} subaccounts={}; no order data decoded or transactions sent",
        directory.wallet(),
        market.name,
        directory.addresses.len(),
    );
    Ok(())
}
