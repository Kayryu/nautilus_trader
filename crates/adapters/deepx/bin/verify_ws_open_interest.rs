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

//! Live read-only observation of one public perpetual open-interest bucket.

use std::time::Duration;

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXHttpClient, DeepXOpenInterestRequest},
    websocket::{match_open_interest_to_rest, observe_first_open_interest},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let market_id = args
        .next()
        .context("usage: deepx-verify-ws-open-interest <market-id> [--reconcile]")?
        .parse::<u16>()
        .context("invalid market ID")?;
    let reconcile = match args.next().as_deref() {
        None => false,
        Some("--reconcile") => true,
        Some(_) => anyhow::bail!("usage: deepx-verify-ws-open-interest <market-id> [--reconcile]"),
    };
    ensure!(args.next().is_none(), "unexpected extra argument");
    ensure!(market_id != 0, "market ID must be positive");

    let config = DeepXDataClientConfig::default();
    let http = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let markets = http.get_perp_markets().await?;
    let market = markets
        .iter()
        .find(|market| market.id == u64::from(market_id))
        .context("market ID is absent from the validated REST directory")?;
    let observation = observe_first_open_interest(
        &config.network,
        config.proxy_url.as_deref(),
        Duration::from_secs(config.websocket_timeout_secs),
        Duration::from_secs(75),
        market_id,
        &market.name,
    )
    .await?;
    if reconcile {
        let request = DeepXOpenInterestRequest {
            market_id: u64::from(market_id),
            start_ms: observation.record.statistic_time,
            end_ms: Some(observation.record.statistic_time),
            limit: Some(1),
        };
        let rest = http.get_perp_open_interest_history(&request, 1).await?;
        match_open_interest_to_rest(&observation, &rest)?;
    }
    println!(
        "market={} pair={} statistic_time_ms={} timestamp_ms={} total_oi={} long_short_ratio={} long_position_count={} short_position_count={} rest_reconciled={reconcile}",
        observation.market_id,
        observation.pair,
        observation.record.statistic_time,
        observation.timestamp_ms,
        observation.record.total_oi,
        observation.record.long_short_ratio,
        observation.record.long_position_count,
        observation.record.short_position_count,
    );
    println!("DeepX public open interest observed; no credentials or transactions sent");
    Ok(())
}
