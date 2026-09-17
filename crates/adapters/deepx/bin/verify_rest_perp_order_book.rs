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

//! Live read-only verification of one DeepX perpetual REST order-book snapshot.

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXHttpClient, DeepXPerpOrderBookRequest},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let market_id = args
        .next()
        .context("usage: deepx-verify-rest-perp-order-book <market-id>")?
        .parse::<u64>()
        .context("market-id must be an unsigned integer")?;
    ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let market = client
        .get_perp_markets()
        .await?
        .into_iter()
        .find(|market| market.id == market_id)
        .context("requested market is absent from the perpetual directory")?;
    let book = client
        .get_perp_order_book(&DeepXPerpOrderBookRequest {
            market_id,
            tick_size: Some(market.order_spec_tick_size),
        })
        .await?;
    ensure!(
        !book.order_buy_list.is_empty() && !book.order_sell_list.is_empty(),
        "perpetual REST order book has an empty side"
    );

    println!(
        "DeepX perpetual REST book verified market={} id={} tick_size={} bids={} asks={} sequence={} engine_time={} best_bid={} best_ask={} latest_price={} mid_price={}",
        market.name,
        market.id,
        market.order_spec_tick_size,
        book.order_buy_list.len(),
        book.order_sell_list.len(),
        book.last_update_id,
        book.engine_time,
        book.order_buy_list[0].price,
        book.order_sell_list[0].price,
        book.latest_price,
        book.mid_price,
    );
    println!(
        "DeepX perpetual REST book verification completed; no credentials or transactions sent"
    );
    Ok(())
}
