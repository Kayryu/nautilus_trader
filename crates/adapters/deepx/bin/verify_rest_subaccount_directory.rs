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

//! Live read-only verification of one DeepX global subaccount-directory page.

use anyhow::ensure;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXAllSubaccountsRequest, DeepXHttpClient},
};

const PAGE_SIZE: u32 = 5;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    ensure!(
        std::env::args().len() == 1,
        "usage: deepx-verify-rest-subaccount-directory"
    );
    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let page = client
        .get_all_subaccounts(&DeepXAllSubaccountsRequest {
            cursor: None,
            page_size: Some(PAGE_SIZE),
        })
        .await?;
    ensure!(
        !page.items.is_empty(),
        "global subaccount directory is empty"
    );

    println!(
        "DeepX subaccount directory verified records={} has_next={} page_size={}",
        page.items.len(),
        page.has_next,
        PAGE_SIZE,
    );
    for record in &page.items {
        println!(
            "subaccount={} owner={} name={} status={:?} height={} created_at={}",
            record.subaccount,
            record.owner,
            record.name,
            record.status,
            record.height,
            record.created_at,
        );
    }
    println!(
        "DeepX subaccount directory verification completed; no credentials or transactions sent"
    );
    Ok(())
}
