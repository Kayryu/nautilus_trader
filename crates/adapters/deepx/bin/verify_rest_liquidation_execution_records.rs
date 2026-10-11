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

//! Live read-only verification of raw DeepX liquidation executor records.

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXLiquidationExecutionRecordsRequest},
};

const PAGE_SIZE: u32 = 100;
const MAX_PAGES: usize = 100;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let kind = args.next().context(
        "usage: deepx-verify-rest-liquidation-execution-records <liquidation|cranker|tp-sl> <wallet|subaccount> <account-id20>",
    )?;
    let scope = args.next().context("missing scope: wallet or subaccount")?;
    let address = args.next().context("missing account-id20")?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let (wallet, subaccount) = match scope.as_str() {
        "wallet" => (Some(address.clone()), None),
        "subaccount" => (None, Some(address.clone())),
        _ => anyhow::bail!("scope must be 'wallet' or 'subaccount'"),
    };
    let request = DeepXLiquidationExecutionRecordsRequest {
        wallet,
        subaccount,
        status: None,
        reason: None,
        tx_hash: None,
        extrinsic_hash: None,
        trigger_block_from: None,
        trigger_block_to: None,
        start_ms: None,
        end_ms: None,
        cursor: None,
        sort: DeepXAccountSortOrder::Descending,
        page_size: Some(PAGE_SIZE),
    };
    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let pages = match kind.as_str() {
        "liquidation" => {
            client
                .get_liquidation_execution_record_pages_raw(&request, MAX_PAGES)
                .await?
        }
        "cranker" => {
            client
                .get_liquidation_cranker_record_pages_raw(&request, MAX_PAGES)
                .await?
        }
        "tp-sl" => {
            client
                .get_liquidation_tp_sl_record_pages_raw(&request, MAX_PAGES)
                .await?
        }
        _ => anyhow::bail!("kind must be 'liquidation', 'cranker', or 'tp-sl'"),
    };
    let records = pages.iter().map(|page| page.items.len()).sum::<usize>();
    println!(
        "DeepX liquidation execution records observed kind={kind} scope={scope} address={address} pages={} records={records}",
        pages.len(),
    );
    for item in pages.into_iter().flat_map(|page| page.items) {
        println!("record={item}");
    }
    println!(
        "Record payloads remain raw observations and do not establish chain inclusion or finality. No credentials or transactions were used"
    );
    Ok(())
}
