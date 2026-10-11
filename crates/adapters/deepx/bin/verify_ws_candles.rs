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

//! Live read-only verification of a superseded one-minute perpetual candle.

use std::time::Duration;

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::{DeepXDataClientConfig, DeepXNetworkConfig},
    http::{DeepXHttpClient, DeepXPerpCandleInterval, DeepXPerpCandlesRequest},
    websocket::{DeepXWsCandleStream, observe_first_superseded_perp_candle},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let market_id = args
        .next()
        .context("usage: deepx-verify-ws-candles <market-id> <pair>")?
        .parse::<u16>()
        .context("perpetual market ID must be a u16")?;
    let pair = args.next().context("missing perpetual pair")?;
    ensure!(args.next().is_none(), "unexpected extra argument");

    let stream = DeepXWsCandleStream::new(market_id, pair.clone())?;
    let superseded = observe_first_superseded_perp_candle(
        &DeepXNetworkConfig::default(),
        None,
        Duration::from_secs(35),
        Duration::from_secs(180),
        stream,
    )
    .await?;
    let candle = superseded.candle;
    let config = DeepXDataClientConfig::default();
    let http = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let request = DeepXPerpCandlesRequest {
        market_id: u64::from(market_id),
        interval: DeepXPerpCandleInterval::OneMinute,
        start_ms: candle.time,
        end_ms: Some(
            candle
                .time
                .checked_add(60_000)
                .context("candle time overflow")?,
        ),
        limit: Some(1),
    };
    let rest_checks = tokio::time::timeout(Duration::from_secs(90), async {
        let mut checks = 0;
        loop {
            let page = http.get_perp_candles(&request).await?;
            checks += 1;
            ensure!(page.pair == pair, "REST candle pair identity mismatch");
            ensure!(
                page.details.len() <= 1,
                "REST returned multiple candle buckets"
            );
            if page.details.first() == Some(&candle) {
                break Ok::<_, anyhow::Error>(checks);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .context("REST candle did not converge with WebSocket within 90 seconds")??;
    println!(
        "DeepX superseded 1m candle matches REST market_id={market_id} time_ms={} open={} high={} low={} close={} volume={} gap_after={} rest_checks={rest_checks}; no credentials or transactions sent",
        candle.time,
        candle.open,
        candle.high,
        candle.low,
        candle.close,
        candle.volume,
        superseded.gap_after,
    );
    Ok(())
}
