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

//! Live read-only verification of bounded DeepX hourly unsettled-funding history.

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXAccountSortOrder, DeepXHourlyUnsettledFundingRequest, DeepXHttpClient},
};

const CAPTURE_START_MS: u64 = 1_789_488_862_441;
const CAPTURE_END_MS: u64 = 1_789_521_262_677;
const PAGE_SIZE: u32 = 5;
const MAX_PAGES: usize = 3;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-rest-hourly-unsettled-funding <wallet> <market-id>")?;
    let market_id = args
        .next()
        .context("usage: deepx-verify-rest-hourly-unsettled-funding <wallet> <market-id>")?
        .parse::<u64>()
        .context("market-id must be an unsigned integer")?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let pages = client
        .get_hourly_unsettled_funding_pages(
            &DeepXHourlyUnsettledFundingRequest {
                subaccount: None,
                wallet: Some(wallet.clone()),
                market_id: Some(market_id),
                start_ms: Some(CAPTURE_START_MS),
                end_ms: Some(CAPTURE_END_MS),
                cursor: None,
                page_size: Some(PAGE_SIZE),
                sort: DeepXAccountSortOrder::Descending,
            },
            MAX_PAGES,
        )
        .await?;
    let record_count = pages.iter().map(Vec::len).sum::<usize>();
    anyhow::ensure!(record_count > 0, "bounded verification returned no records");

    println!(
        "DeepX hourly unsettled funding verified wallet={wallet} market={market_id} pages={} records={record_count} range={CAPTURE_START_MS}..={CAPTURE_END_MS}",
        pages.len(),
    );
    for record in pages.into_iter().flatten() {
        println!(
            "subaccount={} market={} position_version={} signed_position_size_raw={} baseline_index_raw={} cumulative_index_raw={} delta_index_raw={} mark_price_raw={} payment_raw={} boundary_timestamp_ms={} boundary_block={} boundary_event_index={} boundary_event_id={}",
            record.subaccount,
            record.market_id,
            record.position_version,
            record.signed_position_size_raw,
            record.baseline_index_raw,
            record.cumulative_index_raw,
            record.delta_index_raw,
            record.mark_price_raw,
            record.payment_raw,
            record.boundary_timestamp_ms,
            record.boundary_block,
            record.boundary_event_index,
            record.boundary_event_id,
        );
    }
    println!("DeepX hourly funding verification completed; no credentials or transactions sent");
    Ok(())
}
