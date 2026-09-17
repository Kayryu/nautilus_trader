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

//! Live read-only verification of bounded DeepX balance-change history.

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXBalanceChangesRequest, DeepXHttpClient},
};

const PAGE_SIZE: u32 = 100;
const MAX_PAGES: usize = 100;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let scope = args
        .next()
        .context("usage: deepx-verify-rest-balance-changes <wallet|subaccount> <account-id20>")?;
    let address = args
        .next()
        .context("usage: deepx-verify-rest-balance-changes <wallet|subaccount> <account-id20>")?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let (subaccount, wallet) = match scope.as_str() {
        "subaccount" => (Some(address.clone()), None),
        "wallet" => (None, Some(address.clone())),
        _ => anyhow::bail!("scope must be 'wallet' or 'subaccount'"),
    };
    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let pages = client
        .get_balance_change_pages(
            &DeepXBalanceChangesRequest {
                subaccount,
                wallet,
                start_ms: None,
                end_ms: None,
                change_types: Vec::new(),
                cursor: None,
                page_size: Some(PAGE_SIZE),
            },
            MAX_PAGES,
        )
        .await?;
    let records = pages.iter().map(|page| page.items.len()).sum::<usize>();
    println!(
        "DeepX balance changes verified scope={scope} address={address} pages={} records={} page_size={} max_pages={}",
        pages.len(),
        records,
        PAGE_SIZE,
        MAX_PAGES,
    );
    for change in pages.into_iter().flat_map(|page| page.items) {
        println!(
            "id={} type={:?} asset={} amount={} time={} height={} event_idx={} position_owner={}",
            change.id,
            change.change_type,
            change.asset,
            change.balance_change,
            change.time,
            change.height,
            change.event_idx,
            change
                .position
                .as_ref()
                .map_or("none", |position| position.owner.as_str()),
        );
    }
    println!("DeepX balance-change verification completed; no credentials or transactions sent");
    Ok(())
}
