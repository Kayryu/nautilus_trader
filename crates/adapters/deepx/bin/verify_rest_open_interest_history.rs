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
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
//  the specific language governing permissions and limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Read-only verification of bounded public perpetual open-interest history.

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXHttpClient, DeepXOpenInterestRequest},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let market_id = args
        .next()
        .context("usage: deepx-verify-rest-open-interest-history <market-id> <start-ms> <end-ms> [page-size] [max-pages]")?
        .parse::<u64>()
        .context("invalid market ID")?;
    let start_ms = args
        .next()
        .context("missing start timestamp")?
        .parse::<u64>()
        .context("invalid start timestamp")?;
    let end_ms = args
        .next()
        .context("missing end timestamp")?
        .parse::<u64>()
        .context("invalid end timestamp")?;
    let page_size = args
        .next()
        .map(|value| value.parse::<u32>())
        .transpose()
        .context("invalid page size")?
        .unwrap_or(100);
    let max_pages = args
        .next()
        .map(|value| value.parse::<usize>())
        .transpose()
        .context("invalid page budget")?
        .unwrap_or(10);
    ensure!(args.next().is_none(), "unexpected extra argument");
    ensure!(max_pages <= 100, "page budget exceeds 100");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let request = DeepXOpenInterestRequest {
        market_id,
        start_ms,
        end_ms: Some(end_ms),
        limit: Some(page_size),
    };
    let history = client
        .get_perp_open_interest_history(&request, max_pages)
        .await?;
    println!(
        "DeepX open-interest history verified pair={} records={} first_ms={:?} last_ms={:?}; read-only public market data",
        history.pair,
        history.details.len(),
        history.details.first().map(|record| record.statistic_time),
        history.details.last().map(|record| record.statistic_time),
    );
    Ok(())
}
