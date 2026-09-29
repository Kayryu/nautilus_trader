// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Live read-only verification of finalized DeepX Spot order storage.

use anyhow::Context;
use nautilus_core::hex;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXSpotHistoryOrdersRequest},
    observe_and_validate_rpc_endpoint_identities, observe_and_validate_rpc_method_capabilities,
    observe_finalized_spot_orders_by_id_evidence,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let encoded_subaccount = args.next().context(
        "usage: deepx-verify-finalized-spot-orders <subaccount-account-id20> [--history | order-id ...]",
    )?;
    let subaccount = hex::decode_array::<20>(
        encoded_subaccount
            .strip_prefix("0x")
            .context("subaccount must be 0x-prefixed")?,
    )
    .context("subaccount must contain exactly 20 bytes of hex")?;
    let config = DeepXDataClientConfig::default();
    let remaining_args = args.collect::<Vec<_>>();
    let (order_ids, history_count) = if remaining_args.as_slice() == ["--history"] {
        let http = DeepXHttpClient::from_network_config(
            &config.network,
            Some(config.http_timeout_secs),
            config.proxy_url.clone(),
        )?;
        let pages = http
            .get_spot_history_order_pages(
                &DeepXSpotHistoryOrdersRequest {
                    subaccount: encoded_subaccount,
                    name: None,
                    pair: None,
                    order_side: None,
                    cursor: None,
                    sort: DeepXAccountSortOrder::Ascending,
                    page_size: Some(100),
                },
                100,
            )
            .await?;
        let records = pages
            .into_iter()
            .flat_map(|page| page.items)
            .collect::<Vec<_>>();
        let ids = records
            .iter()
            .map(|record| {
                record
                    .order_id
                    .parse::<u64>()
                    .context("Spot order ID is invalid")
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        (ids, Some(records.len()))
    } else {
        let ids = remaining_args
            .iter()
            .map(|value| {
                value
                    .parse::<u64>()
                    .context("order ID must be a decimal u64")
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        (ids, None)
    };
    let endpoints = observe_and_validate_rpc_endpoint_identities(&config.network).await?;
    let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints).await?;
    let evidence = observe_finalized_spot_orders_by_id_evidence(
        &config.network.environment,
        &endpoints,
        &capabilities,
        subaccount,
        &order_ids,
    )
    .await?;
    anyhow::ensure!(
        evidence.subaccount() == subaccount,
        "finalized Spot order evidence belongs to another subaccount"
    );

    let checkpoint = evidence.checkpoint();
    println!(
        "DeepX finalized Spot orders verified subaccount=0x{} block={} hash=0x{} spec_version={} transaction_version={} history_records={:?} requested_orders={} active_orders={}",
        hex::encode(evidence.subaccount()),
        checkpoint.block_number(),
        hex::encode(checkpoint.block_hash()),
        evidence.runtime_identity().spec_version,
        evidence.runtime_identity().transaction_version,
        history_count,
        evidence.orders().len(),
        evidence.active_order_ids().len(),
    );
    for order in evidence.orders() {
        println!(
            "order_id={} owner=0x{} pair=0x{} base_amount_le=0x{} quote_amount_le=0x{} price_le={:?} status={:?} order_type={:?} is_buy={} reduce_only={} post_only={:?}",
            order.order_id(),
            hex::encode(order.owner()),
            hex::encode(order.pair()),
            hex::encode(order.base_amount()),
            hex::encode(order.quote_amount()),
            order.price().map(hex::encode),
            order.status(),
            order.order_type(),
            order.is_buy(),
            order.reduce_only(),
            order.post_only(),
        );
    }
    println!(
        "Values are raw finalized runtime storage evidence; REST history, when requested, supplies query IDs only; no credentials, signing, or transactions were used"
    );
    Ok(())
}
