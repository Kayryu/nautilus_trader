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

//! Live read-only verification of one grouped DeepX wallet trade snapshot.

use std::collections::HashSet;

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpWalletTradesRequest},
};
use rust_decimal::Decimal;

const PAGE_SIZE: u32 = 5;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-rest-wallet-trades <wallet-account-id20> [market-id [subaccount max-pages]]")?;
    let market_id = args
        .next()
        .map(|value| {
            value
                .parse::<u64>()
                .context("market-id must be an unsigned integer")
        })
        .transpose()?;
    let subaccount = args.next();
    let max_pages = args
        .next()
        .map(|value| {
            value
                .parse::<usize>()
                .context("max-pages must be a positive integer")
        })
        .transpose()?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");
    anyhow::ensure!(
        subaccount.is_some() == max_pages.is_some(),
        "subaccount and max-pages must be supplied together"
    );
    anyhow::ensure!(
        subaccount.is_none() || market_id.is_some(),
        "subaccount pagination requires a market ID"
    );

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let request = DeepXPerpWalletTradesRequest {
        wallet: wallet.clone(),
        market_name: None,
        market_id,
        cursor: None,
        sort: DeepXAccountSortOrder::Descending,
        start_ms: None,
        end_ms: None,
        page_size: Some(PAGE_SIZE),
    };
    let groups = client.get_perp_wallet_trades(&request).await?;
    anyhow::ensure!(!groups.is_empty(), "wallet trade snapshot is empty");

    let markets = groups.len();
    let subaccount_groups = groups
        .iter()
        .map(|group| group.subaccounts.len())
        .sum::<usize>();
    let subaccounts = groups
        .iter()
        .flat_map(|group| &group.subaccounts)
        .map(|group| group.subaccount.to_ascii_lowercase())
        .collect::<HashSet<_>>()
        .len();
    let trades = groups
        .iter()
        .flat_map(|group| &group.subaccounts)
        .map(|group| group.trades.items.len())
        .sum::<usize>();
    let zero_size = groups
        .iter()
        .flat_map(|group| &group.subaccounts)
        .flat_map(|group| &group.trades.items)
        .filter(|trade| trade.size == Decimal::ZERO)
        .count();
    let continuation_cursors = groups
        .iter()
        .flat_map(|group| &group.subaccounts)
        .filter(|group| group.trades.has_next)
        .filter_map(|group| group.trades.next_cursor.as_deref())
        .collect::<HashSet<_>>();

    println!(
        concat!(
            "DeepX wallet trades verified wallet={} market={} markets={} ",
            "subaccounts={} subaccount_groups={} trades={} zero_size={} continuation_groups={} ",
            "distinct_cursors={} page_size={}"
        ),
        wallet,
        market_id.map_or_else(|| "all".to_string(), |value| value.to_string()),
        markets,
        subaccounts,
        subaccount_groups,
        trades,
        zero_size,
        groups
            .iter()
            .flat_map(|group| &group.subaccounts)
            .filter(|group| group.trades.has_next)
            .count(),
        continuation_cursors.len(),
        PAGE_SIZE,
    );
    if let (Some(subaccount), Some(max_pages)) = (subaccount, max_pages) {
        let mut pagination_request = request.clone();
        pagination_request.page_size = Some(100);
        let expected_market_name = groups
            .iter()
            .find(|group| Some(group.market_id) == market_id)
            .context("selected wallet trade market is absent")?
            .market_name
            .as_str();
        let pages = client
            .get_perp_wallet_trade_pages_for_subaccount(
                &pagination_request,
                &subaccount,
                expected_market_name,
                max_pages,
            )
            .await?;
        let count = pages.iter().map(|page| page.items.len()).sum::<usize>();
        println!(
            "DeepX wallet trade continuation verified subaccount={} pages={} trades={} terminal={} page_size=100",
            subaccount,
            pages.len(),
            count,
            pages.last().is_some_and(|page| !page.has_next),
        );
    }
    println!("DeepX wallet trade verification completed; no credentials or transactions sent");
    Ok(())
}
