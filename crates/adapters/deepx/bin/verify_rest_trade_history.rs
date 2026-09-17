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

//! Verify bounded raw testnet trade history without private accounts or chain submission.

use anyhow::{Context, ensure};
use nautilus_common::{
    cache::Cache,
    clients::DataClient,
    live::{clock::LiveClock, runner::set_data_event_sender},
    messages::{
        DataEvent, DataResponse,
        data::{RequestFundingRates, RequestTrades},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    data::DeepXDataClient,
    http::{
        DeepXFundingRateRequest, DeepXHttpClient, DeepXPerpTradesHistoryRequest,
        DeepXPerpTradesRequest,
    },
};
use nautilus_model::identifiers::{ClientId, InstrumentId};
use std::{cell::RefCell, num::NonZeroUsize, rc::Rc, time::Duration};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let market = client
        .get_perp_markets()
        .await?
        .into_iter()
        .find(|market| market.name == "ETH-USDC")
        .context("ETH-USDC perpetual market was not advertised by testnet")?;
    let recent = client
        .get_perp_trades(&DeepXPerpTradesRequest {
            market_id: market.id,
            page_size: Some(1),
            cursor: None,
        })
        .await?;
    let latest = recent
        .items
        .first()
        .context("testnet has no ETH-USDC trade to verify")?;
    let timestamp: jiff::Timestamp = latest.created_at.parse()?;
    let end_ms = u64::try_from(timestamp.as_millisecond())?;
    ensure!(
        jiff::Timestamp::from_millisecond(i64::try_from(end_ms)?)? == timestamp,
        "latest trade does not have millisecond-aligned time"
    );
    let request = DeepXPerpTradesHistoryRequest {
        market_id: market.id,
        start_ms: end_ms.saturating_sub(1_000),
        end_ms,
        page_size: 1,
        max_pages: 100,
    };
    let trades = client.get_perp_trades_history(&request).await?;
    ensure!(
        trades.iter().any(|trade| trade.id == latest.id),
        "inclusive upper bound did not retain the observed latest trade"
    );
    println!(
        "{}: {} raw trades in inclusive range {}..={}; page size 1, budget 100",
        market.name,
        trades.len(),
        request.start_ms,
        request.end_ms
    );
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    set_data_event_sender(sender);
    let mut data = DeepXDataClient::new(
        ClientId::from("DEEPX-HISTORY-CHECK"),
        config,
        Rc::new(RefCell::new(Cache::default())).into(),
        Rc::new(RefCell::new(LiveClock::default())),
    )?;
    data.connect().await?;
    while receiver.try_recv().is_ok() {}
    let request_id = UUID4::new();
    data.request_trades(RequestTrades::new(
        InstrumentId::from("ETH-USDC-PERP.DEEPX"),
        Some(jiff::Timestamp::from_millisecond(i64::try_from(
            request.start_ms,
        )?)?),
        Some(timestamp),
        NonZeroUsize::new(100),
        Some(data.client_id()),
        request_id,
        UnixNanos::default(),
        None,
    ))?;
    let event = tokio::time::timeout(Duration::from_secs(60), receiver.recv())
        .await?
        .context("data event channel closed")?;
    let DataEvent::Response(DataResponse::Trades(response)) = event else {
        anyhow::bail!("expected framework trades response");
    };
    ensure!(
        response.correlation_id == request_id,
        "trade correlation mismatch"
    );
    ensure!(
        response
            .data
            .windows(2)
            .all(|pair| pair[0].ts_event <= pair[1].ts_event),
        "framework trades are not chronological"
    );
    ensure!(
        response
            .data
            .iter()
            .any(|trade| trade.trade_id.to_string() == latest.id.to_string()
                && trade.price.as_decimal() == latest.price
                && trade.size.as_decimal() == latest.size),
        "framework response lost or rounded the observed latest trade"
    );
    println!(
        "Framework request_trades verified: {} ticks, correlated and chronological",
        response.data.len()
    );
    let funding_end = end_ms / 60_000 * 60_000;
    let funding_start = funding_end.saturating_sub(120_000);
    let raw_funding = client
        .get_perp_funding_rates_history_limited(
            &DeepXFundingRateRequest {
                market_id: market.id,
                start_ms: funding_start,
                end_ms: Some(funding_end),
                limit: Some(1),
                cursor: None,
            },
            NonZeroUsize::new(3).expect("nonzero limit"),
            10,
        )
        .await?;
    ensure!(
        !raw_funding.is_empty(),
        "no recent funding samples to verify"
    );
    let funding_id = UUID4::new();
    data.request_funding_rates(RequestFundingRates::new(
        InstrumentId::from("ETH-USDC-PERP.DEEPX"),
        Some(jiff::Timestamp::from_millisecond(i64::try_from(
            funding_start,
        )?)?),
        Some(jiff::Timestamp::from_millisecond(i64::try_from(
            funding_end,
        )?)?),
        NonZeroUsize::new(3),
        Some(data.client_id()),
        funding_id,
        UnixNanos::default(),
        None,
    ))?;
    let event = tokio::time::timeout(Duration::from_secs(60), receiver.recv())
        .await?
        .context("funding event channel closed")?;
    let DataEvent::Response(DataResponse::FundingRates(response)) = event else {
        anyhow::bail!("expected framework funding response");
    };
    ensure!(
        response.correlation_id == funding_id,
        "funding correlation mismatch"
    );
    ensure!(
        response.data.len() == raw_funding.len(),
        "raw/framework funding count mismatch"
    );
    ensure!(
        response
            .data
            .windows(2)
            .all(|pair| pair[0].ts_event < pair[1].ts_event),
        "funding samples are not chronological"
    );
    for sample in &response.data {
        ensure!(
            sample.interval.is_none() && sample.next_funding_ns.is_none(),
            "funding sample invented a payment schedule"
        );
        ensure!(
            raw_funding
                .iter()
                .any(|raw| raw.time == sample.ts_event.as_millis()
                    && raw.funding_rate == sample.rate),
            "raw/framework funding time or exact rate mismatch"
        );
    }
    println!(
        "Framework request_funding_rates verified: {} exact chronological samples; no inferred payment schedule",
        response.data.len()
    );
    data.disconnect().await?;
    ensure!(data.is_disconnected(), "data client did not disconnect");
    println!("Bounded REST and framework history verified; no streaming or trading activated");
    Ok(())
}
