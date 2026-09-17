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

//! Live read-only verification of one typed DeepX perpetual account order.

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpOpenOrdersRequest},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let first = args.next().context(concat!(
        "usage: deepx-verify-rest-account-order <subaccount> <market-id> <order-id>\n",
        "       deepx-verify-rest-account-order --tx <tx-hash>\n",
        "       deepx-verify-rest-account-order --open <subaccount> <market-id>",
    ))?;

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    if first == "--open" {
        let subaccount = args.next().context("missing subaccount")?;
        let market_id = args
            .next()
            .context("missing market-id")?
            .parse::<u64>()
            .context("market-id must be an unsigned integer")?;
        anyhow::ensure!(args.next().is_none(), "unexpected extra argument");
        let pages = client
            .get_perp_open_order_pages(
                &DeepXPerpOpenOrdersRequest {
                    subaccount: subaccount.clone(),
                    market_id: Some(market_id),
                    is_long: None,
                    cursor: None,
                    page_size: Some(100),
                    sort: DeepXAccountSortOrder::Descending,
                },
                100,
            )
            .await?;
        let count: usize = pages.iter().map(|page| page.items.len()).sum();
        println!(
            "DeepX active account orders verified owner={subaccount} market_id={market_id} pages={} records={count}",
            pages.len(),
        );
        println!("DeepX account-order verification completed; no credentials or transactions sent");
        return Ok(());
    }

    let order = if first == "--tx" {
        let tx_hash = args.next().context("missing tx-hash")?;
        anyhow::ensure!(args.next().is_none(), "unexpected extra argument");
        client.get_perp_order_by_tx(&tx_hash).await?
    } else {
        let market_id = args
            .next()
            .context("missing market-id")?
            .parse::<u64>()
            .context("market-id must be an unsigned integer")?;
        let order_id = args.next().context("missing order-id")?;
        anyhow::ensure!(args.next().is_none(), "unexpected extra argument");
        client
            .get_perp_order_by_id(&first, market_id, &order_id)
            .await?
    };

    println!(
        "DeepX account order verified owner={} market_id={} order_id={} tx_hash={} tx_hash_type={} side={} type={} status={} size={} filled={} remaining={} created_at={} updated_at={}",
        order.owner,
        order.market_id,
        order.order_id,
        order.tx_hash,
        order.tx_hash_type,
        if order.is_long { "buy" } else { "sell" },
        order.order_type,
        order.status,
        order.size,
        order.size_filled,
        order.size_remain,
        order.create_time,
        order.updated_time.as_deref().unwrap_or("unreported"),
    );
    Ok(())
}
