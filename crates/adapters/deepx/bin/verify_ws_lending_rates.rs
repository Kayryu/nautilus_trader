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
//  See the GNU Lesser General Public License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live read-only verification of one public lending-rate WebSocket batch.

use std::time::Duration;

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountSortOrder, DeepXHttpClient, DeepXLendingHistoryInterval,
        DeepXLendingHistoryRequest,
    },
    websocket::{
        DeepXWsLendingRatesRequest, match_lending_rate_batch_to_rest, observe_first_lending_rates,
    },
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let reconcile = match args.next().as_deref() {
        None => false,
        Some("--reconcile") => true,
        Some(_) => anyhow::bail!("usage: deepx-verify-ws-lending-rates [--reconcile]"),
    };
    ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let http = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let directory = http.get_lending_assets().await?;
    let request = DeepXWsLendingRatesRequest::new(&directory)?;
    let batch = observe_first_lending_rates(
        &config.network,
        config.proxy_url.as_deref(),
        Duration::from_secs(config.websocket_timeout_secs),
        &request,
    )
    .await?;
    if reconcile {
        let end_ms = batch
            .statistic_time_ms
            .checked_add(60_000)
            .context("lending statistic time exceeds one-minute range")?;
        let mut records = Vec::with_capacity(batch.rates.len());
        for rate in &batch.rates {
            let request = DeepXLendingHistoryRequest {
                market_id: Some(rate.market_id),
                asset: Some(rate.asset.clone()),
                interval: DeepXLendingHistoryInterval::OneMinute,
                start_ms: batch.statistic_time_ms,
                end_ms: Some(end_ms),
                limit: Some(2),
                sort: DeepXAccountSortOrder::Ascending,
            };
            let history = http.get_lending_interest_rate_history(&request).await?;
            records.extend(history.details);
        }
        match_lending_rate_batch_to_rest(&batch, &records)?;
    }
    for rate in &batch.rates {
        println!(
            "market_id={} asset={} borrow_apr={} supply_apr={} statistic_time_ms={} timestamp_ms={}",
            rate.market_id,
            rate.asset,
            rate.borrow_apr,
            rate.supply_apr,
            batch.statistic_time_ms,
            batch.timestamp_ms,
        );
    }
    println!(
        "DeepX lending-rate batch observed assets={} rest_reconciled={reconcile}; no credentials or transactions sent",
        batch.rates.len(),
    );
    Ok(())
}
