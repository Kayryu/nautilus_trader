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

//! Live read-only verification of bounded DeepX account liquidation history.

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXLiquidationRecordsRequest},
};

const PAGE_SIZE: u32 = 100;
const MAX_PAGES: usize = 100;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let scope = args.next().context(
        "usage: deepx-verify-rest-liquidation-records <wallet|subaccount> <account-id20>",
    )?;
    let address = args.next().context(
        "usage: deepx-verify-rest-liquidation-records <wallet|subaccount> <account-id20>",
    )?;
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
        .get_liquidation_record_pages(
            &DeepXLiquidationRecordsRequest {
                subaccount,
                wallet,
                liquidation_types: Vec::new(),
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                page_size: Some(PAGE_SIZE),
            },
            MAX_PAGES,
        )
        .await?;
    let records = pages.iter().map(|page| page.items.len()).sum::<usize>();
    println!(
        "DeepX liquidation records verified scope={scope} address={address} pages={} records={} page_size={} max_pages={}",
        pages.len(),
        records,
        PAGE_SIZE,
        MAX_PAGES,
    );
    for record in pages.into_iter().flat_map(|page| page.items) {
        println!(
            "id={} liquidation_id={} type={:?} target={} liquidator={} market={:?} margin_shortage={} margin_freed={} liquidator_fee={:?} if_fee={} liquidate_base_amount={:?} oracle_price={:?} liquidator_order_id={:?} target_order_id={:?} borrow_amount={:?} bankrupt={} height={} event_idx={} created_at={} tx_hash={}",
            record.id,
            record.liquidation_id,
            record.liquidation_type,
            record.target_account,
            record.liquidator,
            record.market_index,
            record.margin_shortage,
            record.margin_freed,
            record.liquidator_fee,
            record.if_fee,
            record.liquidate_base_amount,
            record.oracle_price,
            record.liquidator_order_id,
            record.target_account_order_id,
            record.borrow_amount,
            record.bankrupt,
            record.height,
            record.event_idx,
            record.created_at,
            record.tx_hash.as_deref().unwrap_or("none"),
        );
    }
    println!("DeepX liquidation verification completed; no credentials or transactions sent");
    Ok(())
}
