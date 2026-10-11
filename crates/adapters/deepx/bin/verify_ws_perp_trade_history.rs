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

//! Live read-only verification of a wallet perpetual trade-history subscription.

use std::{
    collections::{BTreeSet, HashSet},
    time::Duration,
};

use anyhow::{Context, ensure};
use nautilus_core::hex;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpAccountTradeRecord,
        DeepXPerpAccountTradesRequest,
    },
    observe_and_validate_rpc_endpoint_identities, observe_and_validate_rpc_method_capabilities,
    observe_finalized_perp_trades_at,
    websocket::{
        DeepXWsPerpFinalizedTradeScope, DeepXWsPerpTradeHistoryBatch,
        DeepXWsPerpTradeHistoryRequest, match_perp_trade_history_batch_to_finalized,
        match_perp_trade_history_batch_to_rest, observe_perp_trade_history_ack,
        observe_perp_trade_history_batches,
    },
};

const MAX_RECONCILE_BATCHES: usize = 20;
const MAX_RECONCILE_ITEMS: usize = 100;
const MAX_RECONCILE_BLOCKS: usize = 20;
const MAX_REST_PAGES: usize = 10;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Ack,
    Observe,
    Reconcile,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args.next().context(
        "usage: deepx-verify-ws-perp-trade-history <wallet-account-id20> <market-id> [--observe|--reconcile]",
    )?;
    let market_id = args
        .next()
        .context("missing perpetual market ID")?
        .parse::<u16>()
        .context("perpetual market ID must be a u16")?;
    let mode = match args.next().as_deref() {
        None => Mode::Ack,
        Some("--observe") => Mode::Observe,
        Some("--reconcile") => Mode::Reconcile,
        Some(_) => anyhow::bail!("unexpected trade-history option"),
    };
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let http = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let directory = http.get_wallet_subaccounts(&wallet).await?;
    let request =
        DeepXWsPerpTradeHistoryRequest::for_wallet(market_id, &wallet, &directory.addresses)?;
    if mode == Mode::Ack {
        observe_perp_trade_history_ack(
            &config.network,
            config.proxy_url.as_deref(),
            Duration::from_secs(30),
            &request,
        )
        .await?;
        println!(
            "DeepX perpetual trade-history subscription acknowledged wallet={} market_id={} subaccounts={}; no trade data decoded or transactions sent",
            directory.wallet(),
            market_id,
            directory.addresses.len(),
        );
    } else {
        let batches = observe_perp_trade_history_batches(
            &config.network,
            config.proxy_url.as_deref(),
            Duration::from_secs(30),
            if mode == Mode::Reconcile {
                MAX_RECONCILE_BATCHES
            } else {
                100
            },
            if mode == Mode::Reconcile {
                MAX_RECONCILE_ITEMS
            } else {
                10_000
            },
            &request,
        )
        .await?;
        if mode == Mode::Reconcile {
            let lines = reconcile_batches(&http, &config, market_id, &batches).await?;
            for line in lines {
                println!("{line}");
            }
            println!(
                "reconciled_trade_batches={}; no transactions sent",
                batches.len()
            );
        } else {
            for batch in &batches {
                for item in batch.items() {
                    println!(
                        "subaccount={} timestamp_ms={} trade={}",
                        batch.subaccount(),
                        batch.timestamp_ms(),
                        item.raw().get(),
                    );
                }
            }
            println!(
                "observed_trade_batches={}; no transactions sent",
                batches.len()
            );
        }
    }
    Ok(())
}

async fn reconcile_batches(
    http: &DeepXHttpClient,
    config: &DeepXDataClientConfig,
    market_id: u16,
    batches: &[DeepXWsPerpTradeHistoryBatch],
) -> anyhow::Result<Vec<String>> {
    let item_count = batches.iter().try_fold(0_usize, |count, batch| {
        count
            .checked_add(batch.items().len())
            .context("DeepX observed trade count overflowed")
    })?;
    ensure!(
        item_count <= MAX_RECONCILE_ITEMS,
        "DeepX observed trade count exceeds reconciliation budget"
    );
    if item_count == 0 {
        return Ok(Vec::new());
    }
    let mut pages_by_batch = Vec::with_capacity(batches.len());
    let mut block_numbers = BTreeSet::new();
    let mut seen_ids = HashSet::new();
    for batch in batches {
        if batch.items().is_empty() {
            pages_by_batch.push(None);
            continue;
        }
        let timestamps = batch
            .items()
            .iter()
            .map(|item| {
                item.created_at
                    .parse::<jiff::Timestamp>()
                    .ok()
                    .and_then(|timestamp| u64::try_from(timestamp.as_millisecond()).ok())
                    .context("invalid observed trade timestamp")
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let request = DeepXPerpAccountTradesRequest {
            subaccount: batch.subaccount().to_string(),
            order_id: None,
            market_id: Some(u64::from(market_id)),
            is_long: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: timestamps.iter().min().copied(),
            end_ms: timestamps.iter().max().copied(),
            page_size: Some(100),
        };
        let records = http
            .get_perp_account_trade_pages(&request, MAX_REST_PAGES)
            .await?
            .into_iter()
            .flat_map(|page| page.items)
            .collect::<Vec<DeepXPerpAccountTradeRecord>>();
        admit_rest_matches(batch, &request, &records, &mut seen_ids, &mut block_numbers)?;
        pages_by_batch.push(Some((request, records)));
    }
    let endpoints = observe_and_validate_rpc_endpoint_identities(&config.network).await?;
    let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints).await?;
    let mut trades = Vec::new();
    for block_number in block_numbers {
        trades.extend(
            observe_finalized_perp_trades_at(
                &config.network.environment,
                &endpoints,
                &capabilities,
                block_number,
            )
            .await?,
        );
    }
    let market = http.get_perp_market_by_id(u64::from(market_id)).await?;
    ensure!(
        market.id == u64::from(market_id),
        "foreign DeepX market metadata"
    );
    let mut lines = Vec::new();
    for (batch, page) in batches.iter().zip(pages_by_batch) {
        let Some((request, records)) = page else {
            continue;
        };
        let address = batch
            .subaccount()
            .strip_prefix("0x")
            .context("invalid observed subaccount prefix")?;
        let scope = DeepXWsPerpFinalizedTradeScope {
            subaccount: hex::decode_array(address)?,
            base_decimal: market.base_decimal,
            quote_decimal: market.quote_decimal,
            quote_symbol: &market.quote_symbol,
        };
        for (record, trade) in
            match_perp_trade_history_batch_to_finalized(batch, &request, &records, &trades, scope)?
        {
            lines.push(format!(
                "subaccount={} rest_trade_id={} block={} event={}",
                batch.subaccount(),
                record.id,
                trade.block_number,
                trade.event_index,
            ));
        }
    }
    Ok(lines)
}

fn admit_rest_matches(
    batch: &DeepXWsPerpTradeHistoryBatch,
    request: &DeepXPerpAccountTradesRequest,
    records: &[DeepXPerpAccountTradeRecord],
    seen_ids: &mut HashSet<u64>,
    block_numbers: &mut BTreeSet<u64>,
) -> anyhow::Result<()> {
    let matched = match_perp_trade_history_batch_to_rest(batch, request, records)?;
    ensure!(
        matched.iter().all(|record| !seen_ids.contains(&record.id)),
        "duplicate DeepX trade across observed WebSocket batches"
    );
    let new_blocks = matched
        .iter()
        .map(|record| record.id / 1_000_000)
        .collect::<BTreeSet<_>>();
    ensure!(
        block_numbers.union(&new_blocks).count() <= MAX_RECONCILE_BLOCKS,
        "DeepX observed trades exceed finalized block budget"
    );
    for record in matched {
        seen_ids.insert(record.id);
        block_numbers.insert(record.id / 1_000_000);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nautilus_deepx::websocket::DeepXWsPerpTradeHistoryFrame;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn duplicate_observed_trade_fails_before_finalized_reads() {
        let wallet: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/http/testnet/perp_wallet_trades_page.json",
        )))
        .unwrap();
        let address = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
        let item = wallet["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|market| market["marketId"] == 3)
            .unwrap()["subaccounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|group| group["subaccount"] == address)
            .unwrap()["trades"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"] == 187_071_212_000_014_u64)
            .unwrap()
            .clone();
        let record: DeepXPerpAccountTradeRecord = serde_json::from_value(item.clone()).unwrap();
        let ws_request = DeepXWsPerpTradeHistoryRequest::new(3, address).unwrap();
        let frame = serde_json::json!({
            "type": "data",
            "channel": "user_perp_trade_history",
            "market": {"type": "perp", "id": 3},
            "data": {"address": address, "trades": {
                "items": [item], "nextCursor": null, "hasNext": false,
            }},
            "timestamp": 1_789_522_187_243_u64,
        });
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsPerpTradeHistoryFrame::parse(&frame.to_string(), &ws_request).unwrap()
        else {
            panic!("expected synthesized trade batch");
        };
        let request = DeepXPerpAccountTradesRequest {
            subaccount: address.to_string(),
            order_id: None,
            market_id: Some(3),
            is_long: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(100),
        };
        let mut seen_ids = HashSet::new();
        let mut block_numbers = BTreeSet::new();
        admit_rest_matches(
            &batch,
            &request,
            std::slice::from_ref(&record),
            &mut seen_ids,
            &mut block_numbers,
        )
        .unwrap();
        assert_eq!(seen_ids, HashSet::from([record.id]));
        assert_eq!(block_numbers, BTreeSet::from([187_071_212]));
        assert!(
            admit_rest_matches(
                &batch,
                &request,
                &[record],
                &mut seen_ids,
                &mut block_numbers,
            )
            .is_err()
        );
        assert_eq!(seen_ids.len(), 1);
        assert_eq!(block_numbers.len(), 1);
    }
}
