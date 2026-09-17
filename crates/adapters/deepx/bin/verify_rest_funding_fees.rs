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

//! Live read-only verification of bounded DeepX perpetual funding-fee history.

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXHttpClient, DeepXPerpFundingFeeRequest, DeepXWalletFundingFeeRequest},
};

const PAGE_SIZE: u32 = 100;
const MAX_PAGES: usize = 100;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let first = args
        .next()
        .context("usage: deepx-verify-rest-funding-fees [--wallet] <address> [market-id]")?;
    let (scope, address) = if first == "--wallet" {
        (
            "wallet",
            args.next().context(
                "usage: deepx-verify-rest-funding-fees [--wallet] <address> [market-id]",
            )?,
        )
    } else {
        ("subaccount", first)
    };
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
    let pages = if scope == "wallet" {
        client
            .get_wallet_funding_fee_pages(
                &DeepXWalletFundingFeeRequest {
                    wallet: address.clone(),
                    market_name: None,
                    market_id,
                    start_ms: None,
                    end_ms: None,
                    cursor: None,
                    page_size: Some(PAGE_SIZE),
                },
                MAX_PAGES,
            )
            .await?
    } else {
        client
            .get_perp_funding_fee_pages(
                &DeepXPerpFundingFeeRequest {
                    subaccount: address.clone(),
                    market_id,
                    start_ms: None,
                    end_ms: None,
                    cursor: None,
                    page_size: Some(PAGE_SIZE),
                },
                MAX_PAGES,
            )
            .await?
    };
    let records = pages.iter().map(|page| page.items.len()).sum::<usize>();
    println!(
        "DeepX funding fees verified scope={scope} address={address} market={} pages={} records={} page_size={} max_pages={}",
        market_id.map_or_else(|| "all".to_string(), |value| value.to_string()),
        pages.len(),
        records,
        PAGE_SIZE,
        MAX_PAGES,
    );
    for fee in pages.into_iter().flat_map(|page| page.items) {
        println!(
            "height={} event_idx={} market={} side={} position_size={} fee={} rate={} settled={} created_at={}",
            fee.height,
            fee.event_idx,
            fee.market,
            if fee.is_long { "long" } else { "short" },
            fee.position_size,
            fee.fee,
            fee.fee_rate,
            fee.is_settled,
            fee.created_at,
        );
    }
    Ok(())
}
