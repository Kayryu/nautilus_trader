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

//! Live read-only verification of one DeepX Spot account-trade page.

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXSpotAccountTradesRequest},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let subaccount = args
        .next()
        .context("usage: deepx-verify-rest-spot-account-trades <subaccount> <market-name>")?;
    let market_name = args
        .next()
        .context("usage: deepx-verify-rest-spot-account-trades <subaccount> <market-name>")?;
    ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let page = client
        .get_spot_account_trades(&DeepXSpotAccountTradesRequest {
            subaccount: subaccount.clone(),
            order_id: None,
            order_side: None,
            name: Some(market_name.clone()),
            pair: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(5),
        })
        .await?;
    ensure!(
        !page.items.is_empty(),
        "Spot account-trade page is empty for the requested subaccount and market"
    );

    let latest = &page.items[0];
    println!(
        "DeepX Spot account trades verified requested_subaccount={} market={} trades={} has_next={} latest_id={} order_id={} side={} taker={} price={} base_amount={} quote_amount={} fee={} fee_asset={:?} created_at={}",
        subaccount,
        market_name,
        page.items.len(),
        page.has_next,
        latest.id,
        latest.order_id,
        latest.order_side,
        latest.taker,
        latest.price,
        latest.base_amount,
        latest.quote_amount,
        latest.fee,
        latest.fee_asset,
        latest.created_at,
    );
    println!(
        "DeepX Spot account-trade verification completed; response records do not echo ownership; no credentials or transactions sent"
    );
    Ok(())
}
