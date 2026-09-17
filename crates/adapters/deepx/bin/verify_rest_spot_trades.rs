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

//! Live read-only verification of one DeepX Spot market-trade page.

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXSpotTradesRequest},
};

const PAGE_SIZE: u32 = 100;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let name = args
        .next()
        .context("usage: deepx-verify-rest-spot-trades <market-name> [wallet-account-id20]")?;
    let wallet = args.next();
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let page = client
        .get_spot_trades(&DeepXSpotTradesRequest {
            name: Some(name.clone()),
            pair: None,
            wallet: wallet.clone(),
            start_ms: None,
            end_ms: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(PAGE_SIZE),
        })
        .await?;
    println!(
        "DeepX Spot trades verified name={name} wallet={} records={} total={} has_next={} page_size={PAGE_SIZE}",
        wallet.as_deref().unwrap_or("all"),
        page.items.len(),
        page.total,
        page.has_next,
    );
    for trade in page.items {
        println!(
            concat!(
                "id={} pair={} pair_name={} sell_id={} seller={} buy_id={} buyer={} ",
                "price={} base_amount={} quote_amount={} taker_fee={} maker_fee={} ",
                "token_value={} taker={} height={} trade_time={}"
            ),
            trade.id,
            trade.pair,
            trade.pair_name,
            trade.sell_id,
            trade.seller,
            trade.buy_id,
            trade.buyer,
            trade.price,
            trade.base_amount,
            trade.quote_amount,
            trade.taker_fee,
            trade.maker_fee,
            trade.token_value,
            trade.taker,
            trade.height,
            trade.trade_time,
        );
    }
    println!("DeepX Spot trade verification completed; no credentials or transactions sent");
    Ok(())
}
