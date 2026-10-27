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

//! Read-only observation of perpetual trade events at one finalized testnet block.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use nautilus_core::hex;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountPage, DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpWalletSubaccountTrades,
        DeepXPerpWalletTradeMarket, DeepXPerpWalletTradesRequest,
    },
    observe_and_validate_rpc_endpoint_identities, observe_and_validate_rpc_method_capabilities,
    observe_finalized_perp_trades_at,
    transaction::{DeepXFinalizedPerpTrade, match_finalized_wallet_perp_trades},
};

const MAX_RELEVANT_GROUPS: usize = 20;
const MAX_PAGES_PER_GROUP: usize = 10;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let block_number: u64 = args
        .next()
        .context("usage: deepx-verify-finalized-perp-trades <block-number> [wallet-account-id20]")?
        .parse()
        .context("invalid block number")?;
    let wallet = args.next();
    let verify_wallet = wallet.is_some();
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let endpoints = observe_and_validate_rpc_endpoint_identities(&config.network).await?;
    let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints).await?;
    let trades = observe_finalized_perp_trades_at(
        &config.network.environment,
        &endpoints,
        &capabilities,
        block_number,
    )
    .await?;
    let event_lines = trades.iter().map(|trade| {
        format!(
            "block={} hash=0x{} event={} market={} buyer_order={} buyer=0x{} seller_order={} seller=0x{} price={} size={} taker={} taker_fee={} maker_fee={} fee_asset=0x{}",
            trade.block_number,
            hex::encode(trade.block_hash),
            trade.event_index,
            trade.market_id,
            trade.buyer_order_id,
            hex::encode(trade.buyer),
            trade.seller_order_id,
            hex::encode(trade.seller),
            trade.price,
            trade.size,
            trade.taker,
            trade.taker_fee,
            trade.maker_fee,
            hex::encode(&trade.fee_asset),
        )
    }).collect::<Vec<_>>();

    let mut wallet_lines = Vec::new();

    if let Some(wallet) = wallet {
        let http = DeepXHttpClient::from_network_config(
            &config.network,
            Some(config.http_timeout_secs),
            config.proxy_url,
        )?;
        let directory = http.get_wallet_subaccounts(&wallet).await?;
        let subaccounts = directory
            .addresses
            .iter()
            .map(|address| {
                let encoded = address
                    .strip_prefix("0x")
                    .context("wallet subaccount is not 0x-prefixed")?;
                hex::decode_array::<20>(encoded).context("invalid wallet subaccount address")
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(!subaccounts.is_empty(), "wallet has no subaccounts");

        let groups =
            collect_relevant_wallet_trade_groups(&http, &wallet, &trades, &subaccounts).await?;
        let mut market_metadata = Vec::new();
        for group in &groups {
            market_metadata.push(http.get_perp_market_by_id(group.market_id).await?);
        }
        let matched =
            match_finalized_wallet_perp_trades(&trades, &groups, &subaccounts, &market_metadata)?;
        anyhow::ensure!(
            !matched.is_empty(),
            "no finalized trades involve wallet subaccounts"
        );
        wallet_lines = matched
            .iter()
            .map(|entry| {
                format!(
                    "wallet={} subaccount=0x{} rest_trade_id={} event={}",
                    wallet,
                    hex::encode(entry.subaccount),
                    entry.record.id,
                    entry.trade.event_index,
                )
            })
            .collect();
    }
    for line in event_lines {
        println!("{line}");
    }
    println!("finalized_perp_trades={}", trades.len());
    for line in &wallet_lines {
        println!("{line}");
    }
    if verify_wallet {
        println!("wallet_finalized_matches={}", wallet_lines.len());
    }
    Ok(())
}

async fn collect_relevant_wallet_trade_groups(
    http: &DeepXHttpClient,
    wallet: &str,
    trades: &[DeepXFinalizedPerpTrade],
    subaccounts: &[[u8; 20]],
) -> anyhow::Result<Vec<DeepXPerpWalletTradeMarket>> {
    let mut relevant = BTreeMap::<u64, BTreeSet<String>>::new();
    for trade in trades {
        for subaccount in subaccounts {
            if trade.buyer == *subaccount || trade.seller == *subaccount {
                relevant
                    .entry(u64::from(trade.market_id))
                    .or_default()
                    .insert(format!("0x{}", hex::encode(subaccount)));
            }
        }
    }
    let group_count = relevant.values().map(BTreeSet::len).sum::<usize>();
    anyhow::ensure!(
        group_count > 0,
        "no finalized trades involve wallet subaccounts"
    );
    anyhow::ensure!(
        group_count <= MAX_RELEVANT_GROUPS,
        "finalized wallet trade group budget exceeded"
    );

    let mut complete = Vec::with_capacity(relevant.len());
    for (market_id, addresses) in relevant {
        let request = DeepXPerpWalletTradesRequest {
            wallet: wallet.to_string(),
            market_name: None,
            market_id: Some(market_id),
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(100),
        };
        let snapshot = http.get_perp_wallet_trades(&request).await?;
        let market = snapshot
            .into_iter()
            .find(|market| market.market_id == market_id)
            .context("relevant perpetual wallet market is absent")?;
        let mut subaccount_groups = Vec::with_capacity(addresses.len());
        for address in addresses {
            let first = market
                .subaccounts
                .iter()
                .find(|group| group.subaccount.eq_ignore_ascii_case(&address))
                .context("relevant perpetual wallet subaccount is absent")?;
            let trades = if first.trades.has_next {
                let pages = http
                    .get_perp_wallet_trade_pages_for_subaccount(
                        &request,
                        &address,
                        &market.market_name,
                        MAX_PAGES_PER_GROUP,
                    )
                    .await?;
                anyhow::ensure!(
                    pages.first() == Some(&first.trades),
                    "perpetual wallet first page changed during traversal"
                );
                DeepXAccountPage {
                    items: pages.into_iter().flat_map(|page| page.items).collect(),
                    next_cursor: None,
                    has_next: false,
                }
            } else {
                first.trades.clone()
            };
            subaccount_groups.push(DeepXPerpWalletSubaccountTrades(
                deepx_rust_sdk::rest::models::DeepXPerpWalletSubaccountTrades {
                    subaccount: address,
                    trades,
                },
            ));
        }
        let latest = http.get_perp_wallet_trades(&request).await?;
        let latest_market = latest
            .iter()
            .find(|candidate| candidate.market_id == market_id)
            .context("relevant perpetual wallet market disappeared during traversal")?;
        anyhow::ensure!(
            latest_market.market_name == market.market_name,
            "perpetual wallet market name changed during traversal"
        );
        for group in &subaccount_groups {
            let initial = market
                .subaccounts
                .iter()
                .find(|candidate| candidate.subaccount.eq_ignore_ascii_case(&group.subaccount))
                .context("initial perpetual wallet subaccount disappeared")?;
            let current = latest_market
                .subaccounts
                .iter()
                .find(|candidate| candidate.subaccount.eq_ignore_ascii_case(&group.subaccount))
                .context("perpetual wallet subaccount disappeared during traversal")?;
            anyhow::ensure!(
                current.trades == initial.trades,
                "perpetual wallet first page changed during traversal"
            );
        }
        complete.push(DeepXPerpWalletTradeMarket(
            deepx_rust_sdk::rest::models::DeepXPerpWalletTradeMarket {
                market_id,
                market_name: market.0.market_name,
                subaccounts: subaccount_groups,
            },
        ));
    }
    Ok(complete)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{Json, Router, extract::Query, routing::get};
    use rstest::rstest;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;

    #[rstest]
    #[case("stable")]
    #[case("changed-first-page")]
    #[case("changed-market-name")]
    #[case("changed-after-traversal")]
    #[tokio::test]
    async fn selected_wallet_group_requires_stable_identity(#[case] outcome: &str) {
        let mut first: Value = serde_json::from_str(include_str!(
            "../test_data/http/testnet/perp_wallet_trades_page.json"
        ))
        .unwrap();
        first["data"].as_array_mut().unwrap().truncate(1);
        let selected = "0x977654069311fa41b88f7901dc65fae9a76fd60b";
        let cursor = first["data"][0]["subaccounts"][2]["trades"]["nextCursor"]
            .as_str()
            .unwrap()
            .to_string();
        let mut changed_first = first.clone();
        changed_first["data"][0]["subaccounts"][2]["trades"]["items"][0]["id"] =
            json!(52_780_136_999_999_u64);
        let mut changed_name = first.clone();
        changed_name["data"][0]["marketName"] = json!("OTHER-USDC");
        let mut last = first.clone();
        let trades = &mut last["data"][0]["subaccounts"][2]["trades"];
        let mut item = trades["items"][4].clone();
        item["id"] = json!(52_780_136_000_001_u64);
        item["createdAt"] = json!("2026-05-28T00:00:00Z");
        trades["items"] = json!([item]);
        trades["hasNext"] = json!(false);
        trades["nextCursor"] = Value::Null;
        let changed_first_page = outcome == "changed-first-page";
        let changed_market_name = outcome == "changed-market-name";
        let changed_after_traversal = outcome == "changed-after-traversal";
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&requests);
        let app = Router::new().route(
            "/internal/v1/account/perp/trades-by-wallet",
            get(move |Query(query): Query<BTreeMap<String, String>>| {
                let first = first.clone();
                let changed_first = changed_first.clone();
                let changed_name = changed_name.clone();
                let last = last.clone();
                let cursor = cursor.clone();
                let observed = Arc::clone(&observed);
                async move {
                    assert_eq!(query["marketId"], "3");
                    assert_eq!(query["pageSize"], "100");
                    let call = observed.fetch_add(1, Ordering::Relaxed);
                    if let Some(value) = query.get("cursor") {
                        assert_eq!(value, &cursor);
                        Json(last)
                    } else if call == 3 && changed_after_traversal {
                        Json(changed_first)
                    } else if call == 1 {
                        if changed_first_page {
                            Json(changed_first)
                        } else if changed_market_name {
                            Json(changed_name)
                        } else {
                            Json(first)
                        }
                    } else {
                        Json(first)
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = DeepXHttpClient::new(base_url, Some(2), None).unwrap();
        let account = hex::decode_array::<20>(selected.trim_start_matches("0x")).unwrap();
        let event = DeepXFinalizedPerpTrade {
            block_hash: [0; 32],
            block_number: 1,
            event_index: 0,
            market_id: 3,
            buyer_order_id: 0,
            buyer: account,
            seller_order_id: 0,
            seller: [0; 20],
            price: 0,
            size: 0,
            buyer_leverage: 0,
            seller_leverage: 0,
            filled_direction: String::new(),
            taker: String::new(),
            taker_fee: 0,
            maker_fee: 0,
            fee_asset: Vec::new(),
        };

        let result = collect_relevant_wallet_trade_groups(
            &http,
            "0x781ed35b167068c93dfadab41dfb680edaca4e50",
            &[event],
            &[account],
        )
        .await;

        if outcome == "stable" {
            let groups = result.unwrap();
            assert_eq!(groups.len(), 1);
            assert_eq!(groups[0].market_name, "ETH-USDC");
            assert_eq!(groups[0].subaccounts.len(), 1);
            assert_eq!(groups[0].subaccounts[0].trades.items.len(), 6);
            assert!(!groups[0].subaccounts[0].trades.has_next);
            assert_eq!(
                groups[0].subaccounts[0].trades.items[5].id,
                52_780_136_000_001
            );
        } else {
            let message = result.unwrap_err().to_string();
            assert!(
                message.contains(match outcome {
                    "changed-first-page" => "first page changed",
                    "changed-market-name" => "market name differs from expected identity",
                    "changed-after-traversal" => "first page changed",
                    _ => unreachable!(),
                }),
                "{message}"
            );
        }
        server.abort();
    }
}
