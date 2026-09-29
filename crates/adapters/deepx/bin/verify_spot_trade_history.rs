// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Verify bounded Spot trade history without private accounts or chain submission.

use std::{cell::RefCell, num::NonZeroUsize, rc::Rc, time::Duration};

use anyhow::{Context, ensure};
use nautilus_common::{
    cache::Cache,
    clients::DataClient,
    live::{clock::LiveClock, runner::set_data_event_sender},
    messages::{DataEvent, DataResponse, data::RequestTrades},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    data::DeepXDataClient,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXSpotTradesRequest},
};
use nautilus_model::identifiers::{ClientId, InstrumentId};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = DeepXDataClientConfig::default();
    let http = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let latest_page = http
        .get_spot_trades(&DeepXSpotTradesRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
            wallet: None,
            start_ms: None,
            end_ms: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(1),
        })
        .await?;
    let latest = latest_page
        .items
        .first()
        .context("testnet has no ETH/USDC Spot trade to verify")?;
    let timestamp: jiff::Timestamp = latest.trade_time.parse()?;
    let end_ms = u64::try_from(timestamp.as_millisecond())?;
    ensure!(
        jiff::Timestamp::from_millisecond(i64::try_from(end_ms)?)? == timestamp,
        "latest Spot trade does not have millisecond-aligned time"
    );
    let start_ms = end_ms.saturating_sub(1_000);
    let raw = http
        .get_spot_trades_limited(
            &DeepXSpotTradesRequest {
                name: Some("ETH/USDC".to_string()),
                pair: None,
                wallet: None,
                start_ms: Some(start_ms),
                end_ms: Some(end_ms),
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                page_size: Some(1),
            },
            NonZeroUsize::new(10).expect("nonzero limit"),
            100,
        )
        .await?;
    ensure!(
        raw.iter().any(|trade| trade.id == latest.id),
        "inclusive Spot history range omitted the latest trade"
    );

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    set_data_event_sender(sender);
    let mut data = DeepXDataClient::new(
        ClientId::from("DEEPX-SPOT-HISTORY-CHECK"),
        config,
        Rc::new(RefCell::new(Cache::default())).into(),
        Rc::new(RefCell::new(LiveClock::default())),
    )?;
    data.connect().await?;
    while receiver.try_recv().is_ok() {}
    let request_id = UUID4::new();
    data.request_trades(RequestTrades::new(
        InstrumentId::from("ETH-USDC.DEEPX"),
        Some(jiff::Timestamp::from_millisecond(i64::try_from(start_ms)?)?),
        Some(timestamp),
        NonZeroUsize::new(10),
        Some(data.client_id()),
        request_id,
        UnixNanos::default(),
        None,
    ))?;
    let event = tokio::time::timeout(Duration::from_secs(60), receiver.recv())
        .await?
        .context("data event channel closed")?;
    let DataEvent::Response(DataResponse::Trades(response)) = event else {
        anyhow::bail!("expected framework Spot trades response");
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
        "framework Spot trades are not chronological"
    );
    ensure!(
        response.data.iter().any(|trade| {
            trade.trade_id.to_string() == latest.id.to_string()
                && trade.price.as_decimal() == latest.price
                && trade.size.as_decimal() == latest.base_amount
        }),
        "framework response lost or rounded the observed Spot trade"
    );
    println!(
        "DeepX framework Spot history verified market=ETH/USDC raw={} ticks={} correlated and chronological; no credentials or transactions sent",
        raw.len(),
        response.data.len(),
    );
    data.disconnect().await?;
    ensure!(data.is_disconnected(), "data client did not disconnect");
    Ok(())
}
