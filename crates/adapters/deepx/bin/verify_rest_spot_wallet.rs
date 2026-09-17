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

//! Live read-only verification of DeepX Spot wallet-grouped orders and trades.

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountSortOrder, DeepXHttpClient, DeepXSpotWalletOrdersRequest,
        DeepXSpotWalletTradesRequest,
    },
};

const PAGE_SIZE: u32 = 5;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-rest-spot-wallet <wallet-account-id20> <market-name>")?;
    let market_name = args
        .next()
        .context("usage: deepx-verify-rest-spot-wallet <wallet-account-id20> <market-name>")?;
    ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let orders = client
        .get_spot_wallet_orders(&DeepXSpotWalletOrdersRequest {
            wallet: Some(wallet.clone()),
            name: Some(market_name.clone()),
            pair: None,
            order_side: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(PAGE_SIZE),
        })
        .await?;
    let trades = client
        .get_spot_wallet_trades(&DeepXSpotWalletTradesRequest {
            wallet: wallet.clone(),
            name: Some(market_name.clone()),
            pair: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(PAGE_SIZE),
        })
        .await?;
    let order_count = orders
        .markets
        .iter()
        .flat_map(|market| &market.subaccounts)
        .map(|subaccount| subaccount.orders.items.len())
        .sum::<usize>();
    let trade_count = trades
        .markets
        .iter()
        .flat_map(|market| &market.subaccounts)
        .map(|subaccount| subaccount.trades.items.len())
        .sum::<usize>();
    ensure!(order_count > 0, "Spot wallet order page is empty");
    ensure!(trade_count > 0, "Spot wallet trade page is empty");

    println!(
        concat!(
            "DeepX Spot wallet groups verified wallet={} market={} orders={} trades={} ",
            "order_has_next={} trade_has_next={}"
        ),
        wallet, market_name, order_count, trade_count, orders.has_next, trades.has_next,
    );
    println!("DeepX Spot wallet verification completed; no credentials or transactions sent");
    Ok(())
}
