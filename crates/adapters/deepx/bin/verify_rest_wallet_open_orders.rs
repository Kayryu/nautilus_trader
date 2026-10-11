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

//! Read-only, bounded perpetual open-order query for every subaccount in a wallet directory.

use std::collections::HashSet;

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpOpenOrdersRequest},
};

const PAGE_SIZE: u32 = 100;
const MAX_PAGES_PER_SUBACCOUNT: usize = 20;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-rest-wallet-open-orders <wallet-account-id20> <market-id>")?;
    let market_id = args
        .next()
        .context("missing perpetual market ID")?
        .parse::<u64>()
        .context("perpetual market ID must be an unsigned integer")?;
    ensure!(market_id > 0, "perpetual market ID must be positive");
    ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let directory = client.get_wallet_subaccounts(&wallet).await?;
    let mut counts = Vec::with_capacity(directory.addresses.len());
    for subaccount in &directory.addresses {
        let pages = client
            .get_perp_open_order_pages(
                &DeepXPerpOpenOrdersRequest {
                    subaccount: subaccount.clone(),
                    market_id: Some(market_id),
                    is_long: None,
                    cursor: None,
                    page_size: Some(PAGE_SIZE),
                    sort: DeepXAccountSortOrder::Descending,
                },
                MAX_PAGES_PER_SUBACCOUNT,
            )
            .await
            .with_context(|| format!("open-order query failed for subaccount {subaccount}"))?;
        let orders = pages.iter().map(|page| page.items.len()).sum::<usize>();
        counts.push((subaccount, pages.len(), orders));
    }
    let final_directory = client.get_wallet_subaccounts(&wallet).await?;
    let initial_addresses: HashSet<_> = directory
        .addresses
        .iter()
        .map(|address| address.to_ascii_lowercase())
        .collect();
    let final_addresses: HashSet<_> = final_directory
        .addresses
        .iter()
        .map(|address| address.to_ascii_lowercase())
        .collect();
    ensure!(
        initial_addresses == final_addresses,
        "wallet subaccount directory changed during open-order query"
    );

    for (subaccount, pages, orders) in &counts {
        println!(
            "subaccount={subaccount} market_id={market_id} pages={pages} open_orders={orders}"
        );
    }
    println!(
        "DeepX wallet REST open orders verified wallet={} subaccounts={} open_orders={}; mutable read-only observations, no credentials or transactions sent",
        directory.wallet(),
        counts.len(),
        counts.iter().map(|(_, _, orders)| orders).sum::<usize>(),
    );
    Ok(())
}
