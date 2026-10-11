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

//! Live read-only verification of a DeepX wallet quota summary.

use anyhow::Context;
use nautilus_deepx::{config::DeepXDataClientConfig, http::DeepXHttpClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-rest-quota-summary <wallet-account-id20>")?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let summary = client.get_quota_summary(&wallet).await?;
    println!(
        "DeepX quota summary verified owner={} subaccounts={} spot_volume_usd={} perp_volume_usd={} total_volume_usd={} earned={} granted={} reserved={} pending={}",
        summary.owner,
        summary.subaccount_count,
        summary.spot_volume_usd,
        summary.perp_volume_usd,
        summary.total_volume_usd,
        summary.quota_earned,
        summary.quota_granted,
        summary.quota_reserved,
        summary.quota_pending,
    );
    println!(
        "first_trade_ts_ms={:?} last_trade_ts_ms={:?} first_trade_at={:?} last_trade_at={:?} updated_at={:?}",
        summary.first_trade_ts_ms,
        summary.last_trade_ts_ms,
        summary.first_trade_at,
        summary.last_trade_at,
        summary.updated_at,
    );
    println!("Read-only query; no credentials or transactions were used");
    Ok(())
}
