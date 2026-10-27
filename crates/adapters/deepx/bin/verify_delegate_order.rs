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

//! Verify a delegated testnet signer with a small post-only order and cancellation.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, ensure};
use deepx_rust_sdk::rpc::{
    DeepXRpcClient, get_finalized_head, get_header,
    timestamp::{decode_timestamp_set_inherent, get_timestamp_now},
};
use nautilus_core::hex;
use nautilus_deepx::{
    DeepXValidatedRpcMethodCapabilities,
    common::{DeepXEnvironment, DeepXKeyScheme, DeepXPrivateKey},
    config::{DeepXDataClientConfig, DeepXRpcRole, DeepXValidatedRpcEndpoints},
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpOpenOrdersRequest},
    observe_and_validate_rpc_endpoint_identities, observe_and_validate_rpc_method_capabilities,
    observe_current_runtime_snapshot_for_endpoints,
    signing::{
        DeepXPerpCancelParams, DeepXPerpOrderType, DeepXPerpPlaceParams, DeepXPostOnlyParam,
        DeepXRuntimeSnapshotService, DeepXTimeInForce, RuntimeSnapshot, SignedPalletExtrinsic,
        sign_perp_cancel, sign_perp_place_order,
    },
    transaction::{
        DeepXDispatchOutcome, DeepXFinalizedTransactionLocationObservation,
        observe_finalized_system_dispatch, observe_finalized_transaction_location,
        submit_extrinsic_once,
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Deserialize;
use serde_json::json;

const MAX_CLOCK_DRIFT_MS: u64 = 10_000;
const FINALITY_TIMEOUT: Duration = Duration::from_secs(120);
const FINALITY_POLL_INTERVAL: Duration = Duration::from_secs(3);
const MARKET_ID: u16 = 3;

#[derive(Deserialize)]
struct BlockResponse {
    block: Block,
}

#[derive(Deserialize)]
struct Block {
    header: Header,
    extrinsics: Vec<String>,
}

#[derive(Deserialize)]
struct Header {
    number: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-delegate-order <wallet> <subaccount> [--submit]")?;
    let subaccount = args.next().context("missing subaccount")?;
    let option = args.next();
    let recovery = match option.as_deref() {
        None => None,
        Some("--submit") => None,
        Some("--cancel-existing") => {
            let order_id = args.next().context("missing existing order ID")?;
            let hash = args.next().context("missing existing extrinsic hash")?;
            Some((order_id, hash))
        }
        Some(_) => anyhow::bail!("unexpected option"),
    };
    let submit = option.as_deref() == Some("--submit");
    ensure!(args.next().is_none(), "unexpected extra argument");

    let key = DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
    let data_config = DeepXDataClientConfig::default();
    let http = DeepXHttpClient::from_network_config(
        &data_config.network,
        Some(data_config.http_timeout_secs),
        data_config.proxy_url.clone(),
    )?;
    let ownership = http.get_account_ownership_proof(&key, &subaccount).await?;
    ensure!(
        format!("0x{}", hex::encode(ownership.wallet())).eq_ignore_ascii_case(&wallet),
        "delegated signer does not control a subaccount of the requested wallet",
    );
    if let Some((order_id, hash)) = recovery {
        return cancel_existing_order(&http, &data_config, &key, &subaccount, &order_id, &hash)
            .await;
    }
    let subaccount_bytes = hex::decode_array::<20>(
        subaccount
            .strip_prefix("0x")
            .context("subaccount lacks 0x prefix")?,
    )?;
    let market = http
        .get_perp_markets()
        .await?
        .into_iter()
        .find(|market| market.id == u64::from(MARKET_ID))
        .context("ETH-USDC perpetual market is unavailable")?;
    ensure!(
        market.name == "ETH-USDC" && !market.is_paused,
        "market is unavailable"
    );
    ensure!(
        market.base_decimal <= 18 && market.quote_decimal == 6,
        "market scale changed"
    );
    let pages = http
        .get_perp_open_order_pages(
            &DeepXPerpOpenOrdersRequest {
                subaccount: subaccount.clone(),
                market_id: Some(u64::from(MARKET_ID)),
                is_long: None,
                cursor: None,
                page_size: Some(100),
                sort: DeepXAccountSortOrder::Descending,
            },
            2,
        )
        .await?;
    ensure!(
        pages.iter().all(|page| page.items.is_empty()),
        "subaccount already has open orders in the test market",
    );
    let quantity = market.order_spec_min_qty;
    ensure!(
        quantity > Decimal::ZERO && market.order_spec_step_size > Decimal::ZERO,
        "market minimum quantity is invalid",
    );
    ensure!(
        quantity % market.order_spec_step_size == Decimal::ZERO,
        "market minimum quantity is not step aligned",
    );
    let tick = market.order_spec_tick_size;
    ensure!(
        tick > Decimal::ZERO && market.mark_price > Decimal::ZERO,
        "market price is invalid"
    );
    let price = ((market.mark_price * Decimal::new(9, 1)) / tick).floor() * tick;
    ensure!(
        price > Decimal::ZERO
            && price < market.mark_price
            && price * quantity >= market.order_spec_min_notional
            && price >= market.oracle_price * (Decimal::ONE - market.limit_order_guard_limit_long),
        "post-only test price violates current market constraints",
    );
    let size_raw = quantity
        .checked_mul(Decimal::from(10_u64.pow(u32::from(market.base_decimal))))
        .and_then(|value| value.to_u128())
        .context("minimum quantity cannot be represented in runtime units")?;
    let price_raw = price
        .checked_mul(Decimal::from(1_000_000_u64))
        .and_then(|value| value.to_u128())
        .context("test price cannot be represented in runtime units")?;

    let network = data_config.network;
    let endpoints = observe_and_validate_rpc_endpoint_identities(&network).await?;
    let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints).await?;
    let snapshot =
        observe_current_runtime_snapshot_for_endpoints(&network.environment, &endpoints).await?;
    let service = DeepXRuntimeSnapshotService::new(snapshot.clone());
    let watch = DeepXRpcClient::new(endpoints.url_for(DeepXRpcRole::Watch).to_string());
    let (place_nonce, block_hash) = observe_nonce(&watch, 0).await?;
    let permit = service.acquire()?;
    let placed = sign_perp_place_order(
        &permit,
        &key,
        DeepXPerpPlaceParams {
            subaccount: subaccount_bytes,
            market_id: MARKET_ID,
            is_long: true,
            size: size_raw,
            price: price_raw,
            order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXPostOnlyParam::MustPostOnly,
        },
        place_nonce,
    )?;
    let place_dry_run = dry_run(&watch, &placed, &block_hash).await?;
    ensure!(
        place_dry_run == "accepted",
        "delegated place dry run did not prove validity",
    );
    println!(
        "Delegated post-only place dry_run={} subaccount={} market={} quantity={} price={} order_id={} hash=0x{}",
        place_dry_run,
        subaccount,
        MARKET_ID,
        quantity,
        price,
        place_nonce,
        hex::encode(placed.extrinsic_hash()),
    );
    if !submit {
        return Ok(());
    }

    let current =
        observe_current_runtime_snapshot_for_endpoints(&network.environment, &endpoints).await?;
    ensure!(
        current.identity() == placed.runtime(),
        "runtime changed after place signing"
    );
    submit_and_verify(
        &endpoints,
        &capabilities,
        &snapshot,
        &watch,
        &placed,
        "place",
    )
    .await?;

    let placed_order = http
        .get_perp_order_by_id(&subaccount, u64::from(MARKET_ID), &place_nonce.to_string())
        .await;
    match placed_order {
        Ok(order) => {
            if order.size != quantity || order.price != price {
                println!(
                    "Delegated order REST size or price disagrees with signed terms; continuing to cancellation"
                );
            }
            println!(
                "Delegated order observed order_id={} status={} filled={}",
                order.order_id, order.status, order.size_filled,
            );
        }
        Err(e) => {
            println!(
                "Delegated order REST lookup is unavailable after finality: {e}; continuing to cancellation"
            );
        }
    }

    let (cancel_nonce, block_hash) = observe_nonce(
        &watch,
        place_nonce.checked_add(1).context("nonce overflow")?,
    )
    .await?;
    let cancel = sign_perp_cancel(
        &permit,
        &key,
        DeepXPerpCancelParams {
            subaccount: subaccount_bytes,
            order_id: place_nonce,
            market_id: MARKET_ID,
            fast_cancel: false,
        },
        cancel_nonce,
    )?;
    let cancel_dry_run = dry_run(&watch, &cancel, &block_hash).await?;
    println!(
        "Delegated cancel dry_run={} order_id={} hash=0x{}",
        cancel_dry_run,
        place_nonce,
        hex::encode(cancel.extrinsic_hash()),
    );
    let current =
        observe_current_runtime_snapshot_for_endpoints(&network.environment, &endpoints).await?;
    ensure!(
        current.identity() == cancel.runtime(),
        "runtime changed after cancel signing"
    );
    submit_and_verify(
        &endpoints,
        &capabilities,
        &snapshot,
        &watch,
        &cancel,
        "cancel",
    )
    .await?;
    let order = http
        .get_perp_order_by_id(&subaccount, u64::from(MARKET_ID), &place_nonce.to_string())
        .await?;
    ensure!(
        order.status == "Canceled" && order.size_filled == Decimal::ZERO,
        "test order was not fully canceled without fills: status={} filled={}",
        order.status,
        order.size_filled,
    );
    println!("Delegated post-only order placed and canceled without fills order_id={place_nonce}");
    Ok(())
}

async fn cancel_existing_order(
    http: &DeepXHttpClient,
    data_config: &DeepXDataClientConfig,
    key: &DeepXPrivateKey,
    subaccount: &str,
    order_id: &str,
    expected_hash: &str,
) -> anyhow::Result<()> {
    let order = http
        .get_perp_order_by_id(subaccount, u64::from(MARKET_ID), order_id)
        .await?;
    ensure!(
        order.status == "Open" && order.size_filled == Decimal::ZERO,
        "existing order is not open and unfilled: status={} filled={}",
        order.status,
        order.size_filled,
    );
    ensure!(
        order.tx_hash_type == "EXTRINSIC_HASH"
            && order.post_only == "MustPostOnly"
            && order.order_type == "Limit",
        "existing order does not match the delegated post-only placement",
    );
    ensure!(
        order.tx_hash.eq_ignore_ascii_case(expected_hash),
        "existing order transaction hash differs from the submitted hash",
    );
    let order_id = order_id
        .parse::<u64>()
        .context("invalid existing order ID")?;
    let subaccount_bytes = hex::decode_array::<20>(
        subaccount
            .strip_prefix("0x")
            .context("subaccount lacks 0x prefix")?,
    )?;
    let network = &data_config.network;
    let endpoints = observe_and_validate_rpc_endpoint_identities(network).await?;
    let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints).await?;
    let snapshot =
        observe_current_runtime_snapshot_for_endpoints(&network.environment, &endpoints).await?;
    let service = DeepXRuntimeSnapshotService::new(snapshot.clone());
    let watch = DeepXRpcClient::new(endpoints.url_for(DeepXRpcRole::Watch).to_string());
    let minimum_nonce = order_id.checked_add(1).context("order ID overflow")?;
    let (nonce, block_hash) = observe_nonce(&watch, minimum_nonce).await?;
    let permit = service.acquire()?;
    let cancel = sign_perp_cancel(
        &permit,
        key,
        DeepXPerpCancelParams {
            subaccount: subaccount_bytes,
            order_id,
            market_id: MARKET_ID,
            fast_cancel: false,
        },
        nonce,
    )?;
    ensure!(
        dry_run(&watch, &cancel, &block_hash).await? == "accepted",
        "existing order cancellation dry run did not prove validity",
    );
    let current =
        observe_current_runtime_snapshot_for_endpoints(&network.environment, &endpoints).await?;
    ensure!(
        current.identity() == cancel.runtime(),
        "runtime changed after cancellation signing",
    );
    println!(
        "Existing order cancel dry run accepted order_id={order_id} hash=0x{}",
        hex::encode(cancel.extrinsic_hash()),
    );
    submit_and_verify(
        &endpoints,
        &capabilities,
        &snapshot,
        &watch,
        &cancel,
        "cancel",
    )
    .await?;
    let order = http
        .get_perp_order_by_id(subaccount, u64::from(MARKET_ID), &order_id.to_string())
        .await?;
    ensure!(
        order.status == "Canceled" && order.size_filled == Decimal::ZERO,
        "existing order cancellation did not complete: status={} filled={}",
        order.status,
        order.size_filled,
    );
    println!("Existing delegated order canceled without fills order_id={order_id}");
    Ok(())
}

async fn observe_nonce(watch: &DeepXRpcClient, minimum: u64) -> anyhow::Result<(u64, String)> {
    let block_hash: String = watch
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "chain_getBlockHash", "params": [],
        }))
        .await?;
    let decoded_hash = hex::decode_array::<32>(
        block_hash
            .strip_prefix("0x")
            .context("current block hash lacks 0x prefix")?,
    )
    .context("current block hash is not 32 bytes")?;
    let block: BlockResponse = watch.get_block(&block_hash).await?;
    let block_number = parse_block_number(&block.block.header.number)?;
    let storage_ms = get_timestamp_now(watch, decoded_hash).await?;
    ensure!(
        decode_timestamp_set_inherent(&block.block.extrinsics) == Some(storage_ms),
        "current timestamp storage and block inherent disagree",
    );
    ensure!(
        watch.get_block_hash(block_number).await?.as_deref() == Some(block_hash.as_str()),
        "current block is no longer canonical",
    );
    let local_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    ensure!(
        local_ms.abs_diff(storage_ms) <= MAX_CLOCK_DRIFT_MS,
        "local clock differs too far from the DeepX chain",
    );
    Ok((local_ms.max(storage_ms).max(minimum), block_hash))
}

async fn dry_run(
    watch: &DeepXRpcClient,
    signed: &SignedPalletExtrinsic,
    block_hash: &str,
) -> anyhow::Result<&'static str> {
    let encoded = format!("0x{}", hex::encode(signed.bytes()));
    let result: String = watch
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "system_dryRunAt",
            "params": [encoded, block_hash],
        }))
        .await?;
    ensure!(
        result == "0x0000" || result == "0x01000b",
        "DeepX order dry run failed: {result}",
    );
    Ok(if result == "0x0000" {
        "accepted"
    } else {
        "inconclusive"
    })
}

async fn submit_and_verify(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    watch: &DeepXRpcClient,
    signed: &SignedPalletExtrinsic,
    operation: &str,
) -> anyhow::Result<()> {
    let encoded = format!("0x{}", hex::encode(signed.bytes()));
    let finalized_before = get_finalized_head(watch).await?;
    let finalized_header: Header = get_header(watch, &finalized_before).await?;
    let mut next_height = parse_block_number(&finalized_header.number)?
        .checked_add(1)
        .context("finalized block height overflow")?;
    let submitted =
        submit_extrinsic_once(endpoints.url_for(DeepXRpcRole::Submission), signed).await?;
    println!(
        "Delegated {operation} submitted hash=0x{}; awaiting finalized dispatch",
        hex::encode(submitted.node_hash()),
    );
    let deadline = Instant::now() + FINALITY_TIMEOUT;
    loop {
        let finalized_hash = get_finalized_head(watch).await?;
        let finalized_header: Header = get_header(watch, &finalized_hash).await?;
        let finalized_height = parse_block_number(&finalized_header.number)?;
        for height in next_height..=finalized_height {
            let hash = watch
                .get_block_hash(height)
                .await?
                .context("finalized block has no canonical hash")?;
            let candidate: BlockResponse = watch.get_block(&hash).await?;
            ensure!(
                parse_block_number(&candidate.block.header.number)? == height,
                "finalized block height mismatch",
            );
            if !candidate
                .block
                .extrinsics
                .iter()
                .any(|extrinsic| extrinsic.eq_ignore_ascii_case(&encoded))
            {
                continue;
            }
            let location = observe_finalized_transaction_location(
                endpoints,
                capabilities,
                signed.extrinsic_hash(),
                u64::from(height),
            )
            .await?;
            let DeepXFinalizedTransactionLocationObservation::Finalized(location) = location else {
                anyhow::bail!("target block lost finality during verification");
            };
            let dispatch = observe_finalized_system_dispatch(endpoints, snapshot, location).await?;
            ensure!(
                dispatch == DeepXDispatchOutcome::Success,
                "finalized DeepX {operation} dispatch failed",
            );
            println!(
                "Delegated {operation} finalized block={} index={} hash=0x{} dispatch=success",
                location.block_number(),
                location.extrinsic_index(),
                hex::encode(location.extrinsic_hash()),
            );
            return Ok(());
        }
        next_height = finalized_height
            .checked_add(1)
            .context("finalized block height overflow")?;
        ensure!(
            Instant::now() < deadline,
            "DeepX {operation} finality was not observed within the bounded wait; reconcile hash=0x{}",
            hex::encode(signed.extrinsic_hash()),
        );
        tokio::time::sleep(FINALITY_POLL_INTERVAL).await;
    }
}

fn parse_block_number(encoded: &str) -> anyhow::Result<u32> {
    u32::from_str_radix(
        encoded
            .strip_prefix("0x")
            .context("block number lacks 0x prefix")?,
        16,
    )
    .context("block number is invalid or exceeds u32")
}
