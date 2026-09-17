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

//! Live read-only verification of bounded DeepX wallet order history.

use std::collections::HashSet;

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpWalletOrdersRequest},
};

const PAGE_SIZE: u32 = 5;
const MAX_PAGES: usize = 100;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-rest-wallet-orders <wallet-account-id20> [market-id]")?;
    let market_id = args
        .next()
        .map(|value| {
            value
                .parse::<u64>()
                .context("market-id must be an unsigned integer")
        })
        .transpose()?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let pages = client
        .get_perp_wallet_order_pages(
            &DeepXPerpWalletOrdersRequest {
                wallet: wallet.clone(),
                market_name: None,
                market_id,
                is_long: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                start_ms: None,
                end_ms: None,
                page_size: Some(PAGE_SIZE),
            },
            MAX_PAGES,
        )
        .await?;
    let records = pages
        .iter()
        .flat_map(|page| &page.markets)
        .flat_map(|market| &market.subaccounts)
        .map(|subaccount| subaccount.orders.items.len())
        .sum::<usize>();
    anyhow::ensure!(records > 0, "wallet order history is empty");
    let markets = pages
        .iter()
        .flat_map(|page| &page.markets)
        .map(|market| market.market_id)
        .collect::<HashSet<_>>();
    let subaccounts = pages
        .iter()
        .flat_map(|page| &page.markets)
        .flat_map(|market| &market.subaccounts)
        .map(|subaccount| subaccount.subaccount.as_str())
        .collect::<HashSet<_>>();

    println!(
        concat!(
            "DeepX wallet orders verified wallet={} market={} pages={} records={} ",
            "markets={} subaccounts={} page_size={} max_pages={}"
        ),
        wallet,
        market_id.map_or_else(|| "all".to_string(), |value| value.to_string()),
        pages.len(),
        records,
        markets.len(),
        subaccounts.len(),
        PAGE_SIZE,
        MAX_PAGES,
    );
    println!("DeepX wallet order verification completed; no credentials or transactions sent");
    Ok(())
}
