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

//! Live read-only verification of perpetual order-history subscription acknowledgement.

use std::time::Duration;

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::DeepXHttpClient,
    websocket::{DeepXWsPerpOrderHistoryRequest, observe_perp_order_history_ack},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-ws-perp-order-history <wallet-account-id20> <market-id>")?;
    let market_id = args
        .next()
        .context("missing perpetual market ID")?
        .parse::<u16>()
        .context("perpetual market ID must be a u16")?;
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
    let markets = http.get_perp_markets().await?;
    anyhow::ensure!(
        markets
            .iter()
            .any(|market| market.id == u64::from(market_id)),
        "perpetual market is absent from the current directory"
    );

    for subaccount in &directory.addresses {
        let request = DeepXWsPerpOrderHistoryRequest::new(market_id, subaccount)?;
        observe_perp_order_history_ack(
            &config.network,
            config.proxy_url.as_deref(),
            Duration::from_secs(config.websocket_timeout_secs),
            &request,
        )
        .await?;
    }
    println!(
        "DeepX perpetual order-history subscriptions acknowledged wallet={} market_id={} subaccounts={}; no order data decoded or transactions sent",
        directory.wallet(),
        market_id,
        directory.addresses.len(),
    );
    Ok(())
}
