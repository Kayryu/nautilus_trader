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
        .context("usage: deepx-verify-rest-wallet-trades <wallet-account-id20> [market-id]")?;
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
    let groups = client
        .get_perp_wallet_trades(&DeepXPerpWalletTradesRequest {
            wallet: wallet.clone(),
            market_name: None,
            market_id,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(PAGE_SIZE),
        })
        .await?;
    anyhow::ensure!(!groups.is_empty(), "wallet trade snapshot is empty");

    let markets = groups.len();
    let subaccounts = groups
        .iter()
        .map(|group| group.subaccounts.len())
        .sum::<usize>();
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
            "subaccounts={} trades={} zero_size={} continuation_groups={} ",
            "distinct_cursors={} page_size={}"
        ),
        wallet,
        market_id.map_or_else(|| "all".to_string(), |value| value.to_string()),
        markets,
        subaccounts,
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
    println!(
        "DeepX wallet trade verification completed; no credentials, pagination, or transactions sent"
    );
    Ok(())
}
