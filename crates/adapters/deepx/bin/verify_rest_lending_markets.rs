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

//! Live read-only verification of DeepX public lending market observations.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountSortOrder, DeepXHttpClient, DeepXLendingHistoryInterval,
        DeepXLendingHistoryRequest, DeepXLendingMarketRequest,
    },
};

const HISTORY_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let assets = client.get_lending_assets().await?;
    ensure!(!assets.is_empty(), "lending asset directory is empty");
    let curves = client
        .get_lending_interest_rate_params(&DeepXLendingMarketRequest::default())
        .await?;
    ensure!(
        !curves.is_empty(),
        "lending interest-rate directory is empty"
    );

    for asset in &assets {
        println!(
            "DeepX lending asset verified market_id={} asset={} height={} created_at={}",
            asset.market_id, asset.asset, asset.height, asset.created_at,
        );
    }
    for curve in &curves {
        println!(
            "DeepX lending curve verified market_id={} asset={} r_min={} r_max={} rho={}",
            curve.market_id, curve.asset, curve.r_min, curve.r_max, curve.rho,
        );
    }

    let usdc = assets
        .iter()
        .find(|asset| asset.asset.eq_ignore_ascii_case("usdc"))
        .context("USDC lending asset is absent from the directory")?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    let start_ms = now_ms
        .saturating_sub(HISTORY_WINDOW.as_millis())
        .try_into()
        .context("lending history timestamp exceeds u64")?;
    let request = DeepXLendingHistoryRequest {
        market_id: Some(usdc.market_id),
        asset: Some(usdc.asset.clone()),
        interval: DeepXLendingHistoryInterval::OneHour,
        start_ms,
        end_ms: None,
        limit: Some(3),
        sort: DeepXAccountSortOrder::Descending,
    };
    let rates = client.get_lending_interest_rate_history(&request).await?;
    let statuses = client.get_lending_status_history(&request).await?;
    ensure!(
        !rates.details.is_empty(),
        "recent lending interest-rate history is empty"
    );
    ensure!(
        !statuses.details.is_empty(),
        "recent lending status history is empty"
    );

    println!(
        "DeepX lending histories verified market_id={} asset={} rates={} statuses={}",
        usdc.market_id,
        usdc.asset,
        rates.details.len(),
        statuses.details.len(),
    );
    println!("DeepX lending verification completed; no credentials or transactions sent");
    Ok(())
}
