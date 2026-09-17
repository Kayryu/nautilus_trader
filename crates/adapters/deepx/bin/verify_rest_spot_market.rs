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

//! Live read-only verification of DeepX public Spot market observations.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{
        DeepXHttpClient, DeepXSpotCandleInterval, DeepXSpotCandlesRequest,
        DeepXSpotLastPriceRequest, DeepXSpotMarket, DeepXSpotOrderBookRequest,
        DeepXSpotVolumePeriod, DeepXSpotVolumeRequest,
    },
};
use rust_decimal::Decimal;

const CANDLE_WINDOW: Duration = Duration::from_secs(60 * 60);
const CANDLE_LIMIT: u32 = 3;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let name = args
        .next()
        .context("usage: deepx-verify-rest-spot-market <market-name>")?;
    ensure!(args.next().is_none(), "unexpected extra argument");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    let end_ms = u64::try_from(now.as_millis()).context("current timestamp exceeds u64")?;
    let start_ms = end_ms.saturating_sub(
        u64::try_from(CANDLE_WINDOW.as_millis()).context("candle window exceeds u64")?,
    );
    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let markets = client.get_spot_markets().await?;
    let directory_market = markets
        .iter()
        .find(|market| market.name == name)
        .context("requested market is absent from the Spot directory")?;
    let by_name = client.get_spot_market_by_name(&name).await?;
    let by_pair = client
        .get_spot_market_by_pair(&directory_market.pair)
        .await?;
    ensure!(
        same_static_metadata(directory_market, &by_name)
            && same_static_metadata(directory_market, &by_pair),
        "Spot directory and lookup metadata disagree"
    );
    let candles = client
        .get_spot_candles(&DeepXSpotCandlesRequest {
            name: Some(name.clone()),
            pair: None,
            interval: DeepXSpotCandleInterval::OneMinute,
            start_ms,
            end_ms: Some(end_ms),
            limit: Some(CANDLE_LIMIT),
        })
        .await?;
    ensure!(!candles.details.is_empty(), "recent Spot candles are empty");
    let last_price = client
        .get_spot_last_price(&DeepXSpotLastPriceRequest {
            name: Some(name.clone()),
            pair: None,
        })
        .await?;
    let volume = client
        .get_spot_volume(&DeepXSpotVolumeRequest {
            name: Some(name.clone()),
            pair: None,
            period: DeepXSpotVolumePeriod::OneHour,
        })
        .await?;
    let book = client
        .get_spot_order_book(&DeepXSpotOrderBookRequest {
            name: Some(name.clone()),
            pair: None,
            tick_size: Some(Decimal::new(1, 2)),
        })
        .await?;
    ensure!(
        !book.order_buy_list.is_empty() && !book.order_sell_list.is_empty(),
        "Spot order book has an empty side"
    );

    println!(
        concat!(
            "DeepX Spot market verified name={} candles={} first_time={} last_time={} ",
            "last_price={} volume_1h={} trades_1h={} bids={} asks={} book_sequence={} ",
            "markets={} pair={} tick_size={}"
        ),
        name,
        candles.details.len(),
        candles.details.first().unwrap().time,
        candles.details.last().unwrap().time,
        last_price.0,
        volume.total_volume,
        volume.trade_count,
        book.order_buy_list.len(),
        book.order_sell_list.len(),
        book.last_update_id,
        markets.len(),
        directory_market.pair,
        directory_market.tick_size,
    );
    println!("DeepX Spot market verification completed; no credentials or transactions sent");
    Ok(())
}

fn same_static_metadata(lhs: &DeepXSpotMarket, rhs: &DeepXSpotMarket) -> bool {
    lhs.name == rhs.name
        && lhs.pair.eq_ignore_ascii_case(&rhs.pair)
        && lhs.quote_address.eq_ignore_ascii_case(&rhs.quote_address)
        && lhs.quote_decimal == rhs.quote_decimal
        && lhs.quote_symbol.eq_ignore_ascii_case(&rhs.quote_symbol)
        && lhs.base_address.eq_ignore_ascii_case(&rhs.base_address)
        && lhs.base_decimal == rhs.base_decimal
        && lhs.base_symbol.eq_ignore_ascii_case(&rhs.base_symbol)
        && lhs.taker_fee_rate == rhs.taker_fee_rate
        && lhs.maker_fee_rate == rhs.maker_fee_rate
        && lhs.tick_size == rhs.tick_size
        && lhs.is_paused == rhs.is_paused
        && lhs.max_deviation_bps == rhs.max_deviation_bps
        && lhs.limit_order_guard_limit_long == rhs.limit_order_guard_limit_long
        && lhs.limit_order_guard_limit_short == rhs.limit_order_guard_limit_short
}
