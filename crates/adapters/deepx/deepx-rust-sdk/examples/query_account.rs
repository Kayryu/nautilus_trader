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

//! Read-only account and chain query using the standalone DeepX SDK.

use deepx_rust_sdk::{
    deployment::{DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL},
    query::{DeepXAccountSortOrder, DeepXPerpHistoryOrdersRequest, DeepXPerpOpenOrdersRequest},
    rest::DeepXRestTransport,
    rpc::DeepXRpcClient,
};
use rust_decimal::Decimal;

#[derive(Clone, Copy)]
enum OrderQuery {
    Open(u64),
    Recent(u64),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: deepx-sdk-query-account <wallet-account-id20>"))?;
    let order_query = match args.next().as_deref() {
        None => None,
        Some("--open-perp-orders") => Some(OrderQuery::Open(
            args.next()
                .ok_or_else(|| anyhow::anyhow!("missing market ID"))?
                .parse::<u64>()?,
        )),
        Some("--recent-perp-orders") => Some(OrderQuery::Recent(
            args.next()
                .ok_or_else(|| anyhow::anyhow!("missing market ID"))?
                .parse::<u64>()?,
        )),
        Some(_) => anyhow::bail!("unexpected option"),
    };
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let proxy_url = std::env::var("DEEPX_PROXY_URL").ok();
    let transport = DeepXRestTransport::new(Some(30), proxy_url.clone())?;
    let subaccounts = transport
        .get_wallet_subaccounts(DEEPX_TESTNET_REST_URL, &wallet)
        .await?;
    let rpc =
        DeepXRpcClient::new_with_proxy(DEEPX_TESTNET_RPC_URL.to_string(), proxy_url.as_deref())?;
    let genesis = rpc
        .get_block_hash(0)
        .await?
        .ok_or_else(|| anyhow::anyhow!("DeepX RPC returned no genesis hash"))?;

    println!(
        "wallet={wallet} subaccounts={}",
        subaccounts.addresses.len()
    );
    for subaccount in &subaccounts.addresses {
        println!("subaccount={subaccount}");
    }
    println!("genesis={genesis}");
    if let Some(OrderQuery::Open(market_id)) = order_query {
        for subaccount in &subaccounts.addresses {
            let request = DeepXPerpOpenOrdersRequest {
                subaccount: subaccount.clone(),
                market_id: Some(market_id),
                is_long: None,
                cursor: None,
                page_size: Some(100),
                sort: DeepXAccountSortOrder::Descending,
            };
            let pages = transport
                .get_perp_open_order_pages(DEEPX_TESTNET_REST_URL, &request, 20)
                .await?;
            let count: usize = pages.iter().map(|page| page.items.len()).sum();
            println!("subaccount={subaccount} market_id={market_id} open_orders={count}");
        }
    } else if let Some(OrderQuery::Recent(market_id)) = order_query {
        for subaccount in &subaccounts.addresses {
            let request = DeepXPerpHistoryOrdersRequest {
                subaccount: subaccount.clone(),
                market_id: Some(market_id),
                cursor: None,
                page_size: Some(100),
                sort: DeepXAccountSortOrder::Descending,
            };
            let page = transport
                .get_perp_history_orders(DEEPX_TESTNET_REST_URL, &request)
                .await?;
            println!(
                "subaccount={subaccount} market_id={market_id} recent_orders={} has_next={}",
                page.items.len(),
                page.has_next,
            );
            for order in page.items {
                if order.status != "Canceled" || order.size_filled != Decimal::ZERO {
                    continue;
                }
                println!(
                    "canceled_unfilled order_id={} post_only={} tx_hash={} created_at={}",
                    order.order_id, order.post_only, order.tx_hash, order.create_time,
                );
            }
        }
    }
    Ok(())
}
