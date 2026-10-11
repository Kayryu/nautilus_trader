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

//! Read-only bounded Spot trade-history observation for every wallet subaccount.

use std::{
    collections::{BTreeSet, HashSet},
    time::Duration,
};

use anyhow::{Context, ensure};
use nautilus_core::hex;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountSortOrder, DeepXHttpClient, DeepXSpotAccountTradeRecord,
        DeepXSpotAccountTradesRequest, DeepXSpotMarket,
    },
    observe_and_validate_rpc_endpoint_identities, observe_and_validate_rpc_method_capabilities,
    observe_finalized_spot_trades_at,
    websocket::{
        DeepXWsSpotFinalizedTradeScope, DeepXWsSpotTradeHistoryBatch,
        DeepXWsSpotTradeHistoryRequest, match_spot_trade_history_batch_to_finalized,
        match_spot_trade_history_batch_to_rest, observe_spot_trade_history_batches,
    },
};

const MAX_RECONCILE_BATCHES: usize = 20;
const MAX_RECONCILE_ITEMS: usize = 100;
const MAX_RECONCILE_BLOCKS: usize = 20;
const MAX_REST_PAGES: usize = 10;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Observe,
    Rest,
    Finalized,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-ws-spot-trade-history <wallet-account-id20> <market-name> [--reconcile|--finalized]")?;
    let market_name = args.next().context("missing Spot market name")?;
    let mode = match args.next().as_deref() {
        None => Mode::Observe,
        Some("--reconcile") => Mode::Rest,
        Some("--finalized") => Mode::Finalized,
        Some(_) => anyhow::bail!("unexpected Spot trade-history option"),
    };
    ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let market = client
        .get_spot_markets()
        .await?
        .into_iter()
        .find(|market| market.name == market_name)
        .context("Spot market is absent from the current directory")?;
    let directory = client.get_wallet_subaccounts(&wallet).await?;
    ensure!(
        !directory.addresses.is_empty(),
        "wallet directory has no subaccounts"
    );

    let mut observations = Vec::with_capacity(directory.addresses.len());
    for subaccount in &directory.addresses {
        let request = DeepXWsSpotTradeHistoryRequest::new(&market, subaccount)?;
        let batches = observe_spot_trade_history_batches(
            &config.network,
            config.proxy_url.as_deref(),
            Duration::from_secs(10),
            20,
            100,
            &request,
        )
        .await?;
        observations.push((request, batches));
    }
    if mode != Mode::Observe {
        let observed_batches = observations
            .iter()
            .try_fold(0_usize, |count, (_, batches)| {
                count
                    .checked_add(batches.len())
                    .context("DeepX Spot trade batch count overflowed")
            })?;
        let candidates = collect_rest_matches(&client, &market.pair, &observations).await?;
        let lines = if mode == Mode::Finalized {
            reconcile_finalized(&config, &market, &candidates).await?
        } else {
            candidates
                .iter()
                .flat_map(|candidate| {
                    candidate.matches.iter().map(|record| {
                        format!(
                            "subaccount={} market={} rest_trade_id={} order_id={}",
                            candidate.subscription.subaccount(),
                            candidate.subscription.market_name(),
                            record.id,
                            record.order_id,
                        )
                    })
                })
                .collect()
        };
        let matched_trades = lines.len();
        for line in lines {
            println!("{line}");
        }
        println!(
            "DeepX wallet Spot trade-history {} completed wallet={} subaccounts={} observed_batches={} matched_trades={}; no transactions sent",
            if mode == Mode::Finalized {
                "finalized match"
            } else {
                "REST candidate match"
            },
            directory.wallet(),
            observations.len(),
            observed_batches,
            matched_trades,
        );
        return Ok(());
    }
    for (request, batches) in &observations {
        for batch in batches {
            for item in batch.items() {
                println!(
                    "subaccount={} market={} timestamp_ms={} trade={}",
                    request.subaccount(),
                    request.market_name(),
                    batch.timestamp_ms(),
                    item.raw().get(),
                );
            }
        }
        println!(
            "subaccount={} observed_spot_trade_batches={}",
            request.subaccount(),
            batches.len(),
        );
    }
    println!(
        "DeepX wallet Spot trade-history observation completed wallet={} subaccounts={}; mutable incremental data only, no transactions sent",
        directory.wallet(),
        observations.len(),
    );
    Ok(())
}

struct SpotRestCandidate<'a> {
    subscription: &'a DeepXWsSpotTradeHistoryRequest,
    batch: &'a DeepXWsSpotTradeHistoryBatch,
    request: DeepXSpotAccountTradesRequest,
    records: Vec<DeepXSpotAccountTradeRecord>,
    matches: Vec<DeepXSpotAccountTradeRecord>,
}

async fn collect_rest_matches<'a>(
    client: &DeepXHttpClient,
    pair: &str,
    observations: &'a [(
        DeepXWsSpotTradeHistoryRequest,
        Vec<DeepXWsSpotTradeHistoryBatch>,
    )],
) -> anyhow::Result<Vec<SpotRestCandidate<'a>>> {
    let batch_count = observations
        .iter()
        .try_fold(0_usize, |count, (_, batches)| {
            count
                .checked_add(batches.len())
                .context("DeepX Spot trade batch count overflowed")
        })?;
    let item_count = observations
        .iter()
        .try_fold(0_usize, |count, (_, batches)| {
            batches.iter().try_fold(count, |count, batch| {
                count
                    .checked_add(batch.items().len())
                    .context("DeepX Spot trade item count overflowed")
            })
        })?;
    ensure!(
        batch_count <= MAX_RECONCILE_BATCHES && item_count <= MAX_RECONCILE_ITEMS,
        "DeepX Spot trade reconciliation budget exceeded"
    );
    let mut seen_ids = HashSet::with_capacity(item_count);
    let mut candidates = Vec::with_capacity(batch_count);
    for (subscription, batches) in observations {
        for batch in batches {
            if batch.items().is_empty() {
                continue;
            }
            let timestamps = batch
                .items()
                .iter()
                .map(|item| {
                    item.record()
                        .created_at
                        .parse::<jiff::Timestamp>()
                        .ok()
                        .and_then(|timestamp| u64::try_from(timestamp.as_millisecond()).ok())
                        .context("invalid observed Spot trade timestamp")
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let request = DeepXSpotAccountTradesRequest {
                subaccount: subscription.subaccount().to_string(),
                order_id: None,
                order_side: None,
                name: None,
                pair: Some(pair.to_string()),
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                start_ms: timestamps.iter().min().copied(),
                end_ms: timestamps.iter().max().copied(),
                page_size: Some(100),
            };
            let records = client
                .get_spot_account_trade_pages(&request, MAX_REST_PAGES)
                .await?
                .into_iter()
                .flat_map(|page| page.items)
                .collect::<Vec<DeepXSpotAccountTradeRecord>>();
            let matches =
                match_spot_trade_history_batch_to_rest(batch, subscription, &request, &records)?;
            for record in &matches {
                ensure!(
                    seen_ids.insert(record.id),
                    "duplicate DeepX Spot trade across observed WebSocket batches"
                );
            }
            let matched_records = matches.into_iter().cloned().collect();
            candidates.push(SpotRestCandidate {
                subscription,
                batch,
                request,
                records,
                matches: matched_records,
            });
        }
    }
    Ok(candidates)
}

async fn reconcile_finalized(
    config: &DeepXDataClientConfig,
    market: &DeepXSpotMarket,
    candidates: &[SpotRestCandidate<'_>],
) -> anyhow::Result<Vec<String>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let mut block_numbers = BTreeSet::new();
    for candidate in candidates {
        for record in &candidate.matches {
            let block_number = record.id / 1_000_000;
            ensure!(block_number > 0, "invalid DeepX Spot trade block identity");
            block_numbers.insert(block_number);
        }
    }
    ensure!(
        block_numbers.len() <= MAX_RECONCILE_BLOCKS,
        "DeepX Spot trade finalized block budget exceeded"
    );
    let endpoints = observe_and_validate_rpc_endpoint_identities(&config.network).await?;
    let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints).await?;
    let mut events = Vec::new();
    for block_number in block_numbers {
        events.extend(
            observe_finalized_spot_trades_at(
                &config.network.environment,
                &endpoints,
                &capabilities,
                block_number,
            )
            .await?,
        );
    }
    let mut lines = Vec::new();
    for candidate in candidates {
        let address = candidate
            .subscription
            .subaccount()
            .strip_prefix("0x")
            .context("invalid observed Spot subaccount prefix")?;
        let scope = DeepXWsSpotFinalizedTradeScope {
            subaccount: hex::decode_array(address)?,
            base_decimal: market.base_decimal,
            quote_decimal: market.quote_decimal,
            base_symbol: &market.base_symbol,
            quote_symbol: &market.quote_symbol,
        };
        for (record, trade) in match_spot_trade_history_batch_to_finalized(
            candidate.batch,
            candidate.subscription,
            &candidate.request,
            &candidate.records,
            &events,
            scope,
        )? {
            lines.push(format!(
                "subaccount={} market={} rest_trade_id={} block={} event={}",
                candidate.subscription.subaccount(),
                candidate.subscription.market_name(),
                record.id,
                trade.block_number,
                trade.event_index,
            ));
        }
    }
    Ok(lines)
}
