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

//! Live read-only verification of DeepX Spot order lookup by ID and transaction hash.

use anyhow::{Context, bail, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXHttpClient, DeepXSpotOrderByIdRequest, DeepXSpotOrderSide},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let subaccount = args.next().context(concat!(
        "usage: deepx-verify-rest-spot-order-lookup ",
        "<subaccount> <market-name> <order-id> <Buy|Sell> <tx-hash>",
    ))?;
    let market_name = args.next().context("missing market-name")?;
    let order_id = args.next().context("missing order-id")?;
    let order_side = match args.next().as_deref() {
        Some("Buy") => DeepXSpotOrderSide::Buy,
        Some("Sell") => DeepXSpotOrderSide::Sell,
        _ => bail!("order side must be exactly Buy or Sell"),
    };
    let tx_hash = args.next().context("missing tx-hash")?;
    ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let by_id = client
        .get_spot_order_by_id(&DeepXSpotOrderByIdRequest {
            subaccount: subaccount.clone(),
            name: Some(market_name.clone()),
            pair: None,
            order_id: order_id.clone(),
            order_side,
        })
        .await?;
    let by_tx = client.get_spot_order_by_tx(&tx_hash).await?;
    ensure!(by_id == by_tx, "ID and transaction lookups disagree");

    println!(
        "DeepX Spot order lookup verified subaccount={} market={} order_id={} side={} status={} price={} base_amount={} base_remaining={} tx_hash={} cancel_reason={:?} cancel_height={:?}",
        by_id.maker,
        by_id.pair_name,
        by_id.order_id,
        by_id.order_side,
        by_id.status,
        by_id.price,
        by_id.base_amount,
        by_id.base_remaining_amount,
        by_id.tx_hash,
        by_id.cancel_reason,
        by_id.cancel_height,
    );
    println!("DeepX Spot order lookup verification completed; no credentials or transactions sent");
    Ok(())
}
