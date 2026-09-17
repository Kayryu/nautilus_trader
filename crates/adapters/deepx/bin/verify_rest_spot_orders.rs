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

//! Live read-only verification of DeepX Spot active and historical order pages.

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountSortOrder, DeepXHttpClient, DeepXSpotHistoryOrdersRequest,
        DeepXSpotOpenOrdersRequest,
    },
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let subaccount = args
        .next()
        .context("usage: deepx-verify-rest-spot-orders <subaccount> <market-name>")?;
    let market_name = args
        .next()
        .context("usage: deepx-verify-rest-spot-orders <subaccount> <market-name>")?;
    ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let open = client
        .get_spot_open_orders(&DeepXSpotOpenOrdersRequest {
            subaccount: subaccount.clone(),
            name: Some(market_name.clone()),
            pair: None,
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(5),
        })
        .await?;
    let history = client
        .get_spot_history_orders(&DeepXSpotHistoryOrdersRequest {
            subaccount: subaccount.clone(),
            name: Some(market_name.clone()),
            pair: None,
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(5),
        })
        .await?;
    ensure!(
        !open.items.is_empty() || !history.items.is_empty(),
        "both Spot order pages are empty for the requested subaccount and market"
    );

    println!(
        "DeepX Spot orders verified subaccount={} market={} open={} open_has_next={} history={} history_has_next={}",
        subaccount,
        market_name,
        open.items.len(),
        open.has_next,
        history.items.len(),
        history.has_next,
    );
    if let Some(order) = open.items.first().or_else(|| history.items.first()) {
        println!(
            "latest observed order id={} side={} status={} price={} base_amount={} base_remaining={} created_at={}",
            order.order_id,
            order.order_side,
            order.status,
            order.price,
            order.base_amount,
            order.base_remaining_amount,
            order.create_time,
        );
    }
    println!("DeepX Spot order verification completed; no credentials or transactions sent");
    Ok(())
}
