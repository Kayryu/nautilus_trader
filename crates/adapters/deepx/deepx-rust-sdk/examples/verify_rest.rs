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

//! Read-only public REST checks using the standalone DeepX SDK.

use std::{
    collections::BTreeSet,
    num::NonZeroUsize,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, ensure};
use deepx_rust_sdk::{
    deployment::{DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL},
    enums::DeepXEnvironment,
    query::{
        DeepXAccountSortOrder, DeepXFundingRateRequest, DeepXLendingHistoryInterval,
        DeepXLendingHistoryRequest, DeepXLendingMarketRequest, DeepXOpenInterestRequest,
        DeepXPerpOrderBookRequest, DeepXPerpTradesHistoryRequest, DeepXPerpTradesRequest,
        DeepXSpotCandleInterval, DeepXSpotCandlesRequest, DeepXSpotLastPriceRequest,
        DeepXSpotOrderBookRequest, DeepXSpotTradesRequest, DeepXSpotVolumePeriod,
        DeepXSpotVolumeRequest,
    },
    rest::{DeepXRestTransport, models::DeepXSpotMarket},
    rpc::{
        DeepXRpcBlockHeader, DeepXRpcClient, DeepXRpcRuntimeVersion,
        chain_decode::{decode_block_number_u64, decode_prefixed_hash},
        get_metadata, get_runtime_version,
        market_spec::get_spot_market_storage_spec,
        spot_market_spec::parse_spot_pair,
        storage_decode::spot_market_storage_key,
    },
    signing::DeepXReadOnlyRuntimeSnapshot,
};
use rust_decimal::Decimal;

const BASE_URL: &str = DEEPX_TESTNET_REST_URL;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().context(
        "usage: deepx-sdk-verify-rest <instruments|perp-market|spot-market|lending|spot-trades|spot-trade-history|perp-book|perp-trade-history|open-interest> [arguments]",
    )?;
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    match mode.as_str() {
        "instruments" => {
            ensure!(args.next().is_none(), "unexpected argument");
            verify_instruments(&transport).await?;
        }
        "perp-market" => {
            ensure!(args.next().is_none(), "unexpected argument");
            verify_perp_market(&transport).await?;
        }
        "spot-market" => {
            let name = args.next().context("spot-market requires a market name")?;
            ensure!(args.next().is_none(), "unexpected argument");
            verify_spot_market(&transport, &name).await?;
        }
        "lending" => {
            ensure!(args.next().is_none(), "unexpected argument");
            verify_lending(&transport).await?;
        }
        "spot-trades" => {
            let name = args.next().context("spot-trades requires a market name")?;
            let wallet = args.next();
            ensure!(args.next().is_none(), "unexpected argument");
            verify_spot_trades(&transport, name, wallet).await?;
        }
        "spot-trade-history" => {
            ensure!(args.next().is_none(), "unexpected argument");
            verify_spot_trade_history(&transport).await?;
        }
        "perp-book" => {
            let market_id = args
                .next()
                .context("perp-book requires a market ID")?
                .parse::<u64>()?;
            ensure!(args.next().is_none(), "unexpected argument");
            verify_perp_book(&transport, market_id).await?;
        }
        "perp-trade-history" => {
            ensure!(args.next().is_none(), "unexpected argument");
            verify_perp_trade_history(&transport).await?;
        }
        "open-interest" => {
            let market_id = args.next().context("missing market ID")?.parse::<u64>()?;
            let start_ms = args
                .next()
                .context("missing start timestamp")?
                .parse::<u64>()?;
            let end_ms = args
                .next()
                .context("missing end timestamp")?
                .parse::<u64>()?;
            let page_size = args
                .next()
                .map(|value| value.parse::<u32>())
                .transpose()?
                .unwrap_or(100);
            let max_pages = args
                .next()
                .map(|value| value.parse::<usize>())
                .transpose()?
                .unwrap_or(10);
            ensure!(args.next().is_none(), "unexpected argument");
            verify_open_interest(
                &transport, market_id, start_ms, end_ms, page_size, max_pages,
            )
            .await?;
        }
        _ => anyhow::bail!("unknown REST verification mode: {mode}"),
    }
    Ok(())
}

async fn verify_instruments(transport: &DeepXRestTransport) -> anyhow::Result<()> {
    let spot = transport.get_spot_markets(BASE_URL).await?;
    let perpetual = transport.get_perp_markets(BASE_URL).await?;
    ensure!(!spot.is_empty(), "no current Spot markets were advertised");
    ensure!(
        !perpetual.is_empty(),
        "no current perpetual markets were advertised"
    );
    ensure!(
        spot.len() <= 256,
        "Spot market spec batch exceeds 256 pairs"
    );
    let pairs = spot
        .iter()
        .map(|market| parse_spot_pair(&market.pair).map_err(anyhow::Error::from))
        .collect::<anyhow::Result<Vec<_>>>()?;
    ensure!(
        pairs.iter().copied().collect::<BTreeSet<_>>().len() == pairs.len(),
        "duplicate Spot market pair"
    );
    for market in &perpetual {
        ensure!(
            market.id > 0
                && !market.base_symbol.is_empty()
                && !market.quote_symbol.is_empty()
                && market.order_spec_tick_size > Decimal::ZERO
                && market.order_spec_step_size > Decimal::ZERO
                && market.order_spec_min_qty > Decimal::ZERO
                && market.order_spec_min_notional > Decimal::ZERO,
            "perpetual market {} has invalid instrument constraints",
            market.name
        );
    }
    let rpc = DeepXRpcClient::new_with_proxy(
        DEEPX_TESTNET_RPC_URL.to_string(),
        std::env::var("DEEPX_PROXY_URL").ok().as_deref(),
    )?;
    let genesis = rpc.get_block_hash_required_once(0).await?;
    ensure!(
        genesis.eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
        "RPC endpoint genesis differs from approved DeepX testnet"
    );
    let block_hash = rpc.get_best_block_hash_once().await?;
    let header: DeepXRpcBlockHeader = rpc.get_header_once(&block_hash).await?;
    let block_number =
        decode_block_number_u64(&header.number).context("invalid instrument block number")?;
    let version: DeepXRpcRuntimeVersion = get_runtime_version(&rpc, &block_hash).await?;
    let encoded_metadata = get_metadata(&rpc, &block_hash).await?;
    let metadata = hex::decode(
        encoded_metadata
            .strip_prefix("0x")
            .context("runtime metadata is not hex encoded")?,
    )?;
    let snapshot = DeepXReadOnlyRuntimeSnapshot::approved_testnet(
        &DeepXEnvironment::Testnet,
        decode_prefixed_hash(&genesis).context("invalid testnet genesis hash")?,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )?;
    let rejected = rpc
        .get_storage_response::<String>(
            &spot_market_storage_key(pairs[0]),
            &format!("0x{}", "00".repeat(32)),
        )
        .await?;
    ensure!(
        rejected.jsonrpc.as_deref() == Some("2.0")
            && rejected.id == Some(1)
            && rejected.result.is_none()
            && rejected.error.is_some(),
        "RPC endpoint ignored an invalid Spot market block hash"
    );
    for (market, pair) in spot.iter().zip(pairs) {
        let mut symbols = market.name.split('/');
        ensure!(
            symbols
                .next()
                .is_some_and(|base| base.eq_ignore_ascii_case(&market.base_symbol))
                && symbols
                    .next()
                    .is_some_and(|quote| quote.eq_ignore_ascii_case(&market.quote_symbol))
                && symbols.next().is_none()
                && !market
                    .base_symbol
                    .eq_ignore_ascii_case(&market.quote_symbol),
            "Spot market {} name and currency symbols disagree",
            market.name
        );
        let spec = get_spot_market_storage_spec(&rpc, &block_hash, snapshot.metadata(), pair)
            .await
            .with_context(|| format!("invalid runtime Spot spec for {}", market.name))?;
        ensure!(
            spec.min_order_size > 0 && spec.tick_size > 0 && spec.step_size > 0,
            "Spot market {} has nonpositive order constraints",
            market.name
        );
        let tick = Decimal::try_from_i128_with_scale(
            i128::try_from(spec.tick_size).context("Spot tick exceeds Decimal range")?,
            6,
        )?;
        ensure!(
            tick == market.tick_size,
            "Spot market {} REST tick differs from runtime tick",
            market.name
        );
        for quantity in [spec.min_order_size, spec.step_size] {
            Decimal::try_from_i128_with_scale(
                i128::try_from(quantity).context("Spot quantity exceeds Decimal range")?,
                u32::from(market.base_decimal),
            )?;
        }
    }
    let canonical = rpc.get_block_hash_required_once(block_number).await?;
    ensure!(
        canonical.eq_ignore_ascii_case(&block_hash),
        "instrument block changed during Spot spec verification"
    );
    println!(
        "market directories and Spot order specs verified: spot={} perpetual={}",
        spot.len(),
        perpetual.len()
    );
    Ok(())
}

async fn verify_perp_market(transport: &DeepXRestTransport) -> anyhow::Result<()> {
    let markets = transport.get_perp_markets(BASE_URL).await?;
    let directory = markets
        .iter()
        .find(|market| market.id == 3)
        .context("perpetual market 3 is absent from the directory")?;
    let by_id = transport.get_perp_market_by_id(BASE_URL, 3).await?;
    let by_name = transport
        .get_perp_market_by_name(BASE_URL, "ETH-USDC")
        .await?;
    for lookup in [&by_id, &by_name] {
        ensure!(
            lookup.id == directory.id && lookup.name == directory.name,
            "market identity mismatch"
        );
        ensure!(
            lookup.base_symbol == directory.base_symbol
                && lookup.quote_symbol == directory.quote_symbol
                && lookup.base_address == directory.base_address
                && lookup.quote_address == directory.quote_address
                && lookup.base_decimal == directory.base_decimal
                && lookup.quote_decimal == directory.quote_decimal
                && lookup.initial_margin_ratio == directory.initial_margin_ratio
                && lookup.maintenance_margin_ratio == directory.maintenance_margin_ratio
                && lookup.taker_fee_rate == directory.taker_fee_rate
                && lookup.maker_fee_rate == directory.maker_fee_rate
                && lookup.order_spec_min_qty == directory.order_spec_min_qty
                && lookup.order_spec_min_notional == directory.order_spec_min_notional
                && lookup.is_paused == directory.is_paused,
            "perpetual directory and lookup metadata disagree",
        );
    }
    println!(
        "perpetual market verified: {} ({} markets) paused={} deleted={}",
        directory.name,
        markets.len(),
        directory.is_paused,
        directory.is_deleted
    );
    Ok(())
}

async fn verify_spot_market(transport: &DeepXRestTransport, name: &str) -> anyhow::Result<()> {
    let markets = transport.get_spot_markets(BASE_URL).await?;
    let directory = markets
        .iter()
        .find(|market| market.name == name)
        .context("requested Spot market is absent from the directory")?;
    let by_name = transport.get_spot_market_by_name(BASE_URL, name).await?;
    let by_pair = transport
        .get_spot_market_by_pair(BASE_URL, &directory.pair)
        .await?;
    ensure!(
        same_spot_metadata(directory, &by_name) && same_spot_metadata(directory, &by_pair),
        "Spot directory and lookup metadata disagree"
    );
    let end_ms = now_ms()?;
    let start_ms = end_ms.saturating_sub(Duration::from_secs(60 * 60).as_millis() as u64);
    let candles = transport
        .get_spot_candles(
            BASE_URL,
            &DeepXSpotCandlesRequest {
                name: Some(name.to_string()),
                pair: None,
                interval: DeepXSpotCandleInterval::OneMinute,
                start_ms,
                end_ms: Some(end_ms),
                limit: Some(3),
            },
        )
        .await?;
    ensure!(!candles.details.is_empty(), "recent Spot candles are empty");
    let last_price = transport
        .get_spot_last_price(
            BASE_URL,
            &DeepXSpotLastPriceRequest {
                name: Some(name.to_string()),
                pair: None,
            },
        )
        .await?;
    let volume = transport
        .get_spot_volume(
            BASE_URL,
            &DeepXSpotVolumeRequest {
                name: Some(name.to_string()),
                pair: None,
                period: DeepXSpotVolumePeriod::OneHour,
            },
        )
        .await?;
    let book = transport
        .get_spot_order_book(
            BASE_URL,
            &DeepXSpotOrderBookRequest {
                name: Some(name.to_string()),
                pair: None,
                tick_size: Some(Decimal::new(1, 2)),
            },
        )
        .await?;
    ensure!(
        !book.order_buy_list.is_empty() && !book.order_sell_list.is_empty(),
        "Spot order book has an empty side"
    );
    println!(
        "Spot market verified: {name} candles={} price={} volume={} bids={} asks={}",
        candles.details.len(),
        last_price.0,
        volume.total_volume,
        book.order_buy_list.len(),
        book.order_sell_list.len()
    );
    Ok(())
}

fn same_spot_metadata(lhs: &DeepXSpotMarket, rhs: &DeepXSpotMarket) -> bool {
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

async fn verify_lending(transport: &DeepXRestTransport) -> anyhow::Result<()> {
    let assets = transport.get_lending_assets(BASE_URL).await?;
    ensure!(!assets.is_empty(), "lending asset directory is empty");
    let curves = transport
        .get_lending_interest_rate_params(BASE_URL, &DeepXLendingMarketRequest::default())
        .await?;
    ensure!(
        !curves.is_empty(),
        "lending interest-rate directory is empty"
    );
    let usdc = assets
        .iter()
        .find(|asset| asset.asset.eq_ignore_ascii_case("usdc"))
        .context("USDC lending asset is absent from the directory")?;
    let request = DeepXLendingHistoryRequest {
        market_id: Some(usdc.market_id),
        asset: Some(usdc.asset.clone()),
        interval: DeepXLendingHistoryInterval::OneHour,
        start_ms: now_ms()?.saturating_sub(Duration::from_secs(24 * 60 * 60).as_millis() as u64),
        end_ms: None,
        limit: Some(3),
        sort: DeepXAccountSortOrder::Descending,
    };
    let rates = transport
        .get_lending_interest_rate_history(BASE_URL, &request)
        .await?;
    let statuses = transport
        .get_lending_status_history(BASE_URL, &request)
        .await?;
    ensure!(
        !rates.details.is_empty() && !statuses.details.is_empty(),
        "recent lending history is empty"
    );
    println!(
        "lending verified: assets={} curves={} rates={} statuses={}",
        assets.len(),
        curves.len(),
        rates.details.len(),
        statuses.details.len()
    );
    Ok(())
}

async fn verify_spot_trades(
    transport: &DeepXRestTransport,
    name: String,
    wallet: Option<String>,
) -> anyhow::Result<()> {
    let page = transport
        .get_spot_trades(
            BASE_URL,
            &DeepXSpotTradesRequest {
                name: Some(name.clone()),
                pair: None,
                wallet,
                start_ms: None,
                end_ms: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                page_size: Some(100),
            },
        )
        .await?;
    println!(
        "Spot trades verified: {name} records={} total={} has_next={}",
        page.items.len(),
        page.total,
        page.has_next
    );
    for trade in page.items {
        println!(
            "id={} pair={} price={} base_amount={} quote_amount={} height={}",
            trade.id, trade.pair, trade.price, trade.base_amount, trade.quote_amount, trade.height
        );
    }
    Ok(())
}

async fn verify_perp_book(transport: &DeepXRestTransport, market_id: u64) -> anyhow::Result<()> {
    let markets = transport.get_perp_markets(BASE_URL).await?;
    let market = markets
        .iter()
        .find(|market| market.id == market_id)
        .context("requested market is absent from the perpetual directory")?;
    let book = transport
        .get_perp_order_book(
            BASE_URL,
            &DeepXPerpOrderBookRequest {
                market_id,
                tick_size: Some(market.order_spec_tick_size),
            },
        )
        .await?;
    ensure!(
        !book.order_buy_list.is_empty() && !book.order_sell_list.is_empty(),
        "perpetual REST order book has an empty side"
    );
    println!(
        "perpetual book verified: {} bids={} asks={} sequence={}",
        market.name,
        book.order_buy_list.len(),
        book.order_sell_list.len(),
        book.last_update_id
    );
    Ok(())
}

async fn verify_open_interest(
    transport: &DeepXRestTransport,
    market_id: u64,
    start_ms: u64,
    end_ms: u64,
    page_size: u32,
    max_pages: usize,
) -> anyhow::Result<()> {
    ensure!(max_pages <= 100, "page budget exceeds 100");
    let history = transport
        .get_perp_open_interest_history(
            BASE_URL,
            &DeepXOpenInterestRequest {
                market_id,
                start_ms,
                end_ms: Some(end_ms),
                limit: Some(page_size),
            },
            max_pages,
        )
        .await?;
    println!(
        "open-interest history verified: pair={} records={} first_ms={:?} last_ms={:?}",
        history.pair,
        history.details.len(),
        history.details.first().map(|record| record.statistic_time),
        history.details.last().map(|record| record.statistic_time)
    );
    Ok(())
}

async fn verify_perp_trade_history(transport: &DeepXRestTransport) -> anyhow::Result<()> {
    let market = transport
        .get_perp_markets(BASE_URL)
        .await?
        .into_iter()
        .find(|market| market.name == "ETH-USDC")
        .context("ETH-USDC perpetual market was not advertised by testnet")?;
    let recent_request = DeepXPerpTradesRequest {
        market_id: market.id,
        page_size: Some(1),
        cursor: None,
    };
    let recent = transport
        .get_perp_trades(BASE_URL, &recent_request.as_query())
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
    let trades = transport
        .get_perp_trades_history(BASE_URL, &request)
        .await?;
    ensure!(
        trades.iter().any(|trade| trade.id == latest.id),
        "inclusive upper bound did not retain the observed latest trade"
    );
    let funding_end = end_ms / 60_000 * 60_000;
    let funding_start = funding_end.saturating_sub(120_000);
    let funding = transport
        .get_perp_funding_rates_history_limited(
            BASE_URL,
            &DeepXFundingRateRequest {
                market_id: market.id,
                start_ms: funding_start,
                end_ms: Some(funding_end),
                limit: Some(1),
                cursor: None,
            },
            NonZeroUsize::new(3).context("funding record limit must be positive")?,
            10,
        )
        .await?;
    ensure!(!funding.is_empty(), "no recent funding samples to verify");
    println!(
        "perp trade and funding history verified: market={} trades={} funding_samples={} range={}..={}",
        market.name,
        trades.len(),
        funding.len(),
        request.start_ms,
        request.end_ms
    );
    Ok(())
}

async fn verify_spot_trade_history(transport: &DeepXRestTransport) -> anyhow::Result<()> {
    let recent = transport
        .get_spot_trades(
            BASE_URL,
            &DeepXSpotTradesRequest {
                name: Some("ETH/USDC".to_string()),
                pair: None,
                wallet: None,
                start_ms: None,
                end_ms: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                page_size: Some(1),
            },
        )
        .await?;
    let latest = recent
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
    let trades = transport
        .get_spot_trades_limited(
            BASE_URL,
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
            NonZeroUsize::new(10).context("trade limit must be positive")?,
            100,
        )
        .await?;
    ensure!(
        trades.iter().any(|trade| trade.id == latest.id),
        "inclusive Spot history range omitted the latest trade"
    );
    println!(
        "Spot trade history verified: market=ETH/USDC trades={} range={start_ms}..={end_ms}",
        trades.len()
    );
    Ok(())
}

fn now_ms() -> anyhow::Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    u64::try_from(now.as_millis()).context("current timestamp exceeds u64")
}
