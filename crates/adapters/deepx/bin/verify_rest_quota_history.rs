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

//! Live read-only verification of DeepX wallet quota history.

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXHttpClient, DeepXQuotaHistoryRequest},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-rest-quota-history <wallet-account-id20>")?;
    ensure!(args.next().is_none(), "unexpected extra argument");
    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let page = client
        .get_quota_history(&DeepXQuotaHistoryRequest {
            wallet: wallet.clone(),
            buyer_address: None,
            history_type: None,
            cursor: None,
            limit: Some(5),
        })
        .await?;
    ensure!(!page.items.is_empty(), "wallet quota history is empty");
    println!(
        "DeepX quota history verified wallet={} records={} has_next={}",
        wallet,
        page.items.len(),
        page.has_next,
    );
    for record in page.items {
        println!(
            "id={} type={:?} quota={} block={} event={} created_at={}",
            record.id,
            record.history_type,
            record.quota,
            record.block_number,
            record.event_index,
            record.created_at,
        );
    }
    println!("DeepX quota history verification completed; no credentials or transactions sent");
    Ok(())
}
