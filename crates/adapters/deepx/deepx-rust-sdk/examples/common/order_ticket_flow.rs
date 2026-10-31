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

//! Shared command flow for the blocking and asynchronous order-ticket examples.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, ensure};
use deepx_rust_sdk::{
    credential::{DeepXKeyScheme, DeepXPrivateKey},
    deployment::{DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_RPC_URL},
    enums::DeepXEnvironment,
    identity::decode_account_id20,
    rpc::{
        DeepXRpcBlockResponse, DeepXRpcClient, DeepXRpcRuntimeVersion,
        chain_decode::{decode_block_number_u64, decode_prefixed_hash},
        get_metadata, get_runtime_version,
        order_ticket::{
            DeepXPerpOrderMonitor, DeepXPerpOrderTicket, DeepXPerpOrderTicketState,
            DeepXPreparedPerpOrder,
        },
        storage::{
            DeepXStoredOrderStatus, DeepXStoredPerpOrderType, DeepXStoredPostOnlyParam,
            DeepXStoredTimeInForce,
        },
        storage_decode::get_perp_orders_by_id_at_block,
        timestamp::{decode_timestamp_set_inherent, get_timestamp_now},
    },
    signing::{
        DeepXPerpOrderType, DeepXPerpPlaceParams, DeepXPostOnlyParam, DeepXRuntimeSnapshotService,
        DeepXTimeInForce, RuntimeSnapshot,
    },
};
use serde_json::json;

const MAX_CLOCK_DRIFT_MS: u64 = 10_000;
const EXECUTION_TIMEOUT: Duration = Duration::from_secs(120);
const FINALITY_TIMEOUT: Duration = Duration::from_secs(120);

struct OrderArguments {
    params: DeepXPerpPlaceParams,
    requested_nonce: Option<u64>,
    submit: bool,
}

pub(crate) async fn run(monitoring: bool) -> anyhow::Result<()> {
    let args = parse_args(std::env::args().skip(1))?;
    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let rpc = DeepXRpcClient::new_with_proxy(DEEPX_TESTNET_RPC_URL.to_string(), proxy.as_deref())?;
    ensure!(
        rpc.get_block_hash_required_once(0)
            .await?
            .eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
        "RPC endpoint is not the approved DeepX testnet"
    );
    let key = DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
    let (suggested_nonce, block_hash) = observe_nonce(&rpc).await?;
    let nonce = args.requested_nonce.unwrap_or(suggested_nonce);
    ensure!(
        nonce >= suggested_nonce,
        "requested timestamp nonce precedes current chain time"
    );
    let service = DeepXRuntimeSnapshotService::new(read_runtime(&rpc, &block_hash).await?);
    let permit = service.acquire()?;
    let prepared = DeepXPreparedPerpOrder::sign(&permit, &key, args.params, nonce)?;
    let dry_run: String = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "system_dryRunAt",
            "params": [format!("0x{}", hex::encode(prepared.bytes())), block_hash],
        }))
        .await?;
    ensure!(dry_run == "0x0000", "order dry run failed: {dry_run}");
    println!(
        "order dry run accepted: order_id={nonce} hash=0x{}",
        hex::encode(prepared.extrinsic_hash())
    );
    if !args.submit {
        return Ok(());
    }
    let finalized = if monitoring {
        let mut monitor = DeepXPerpOrderMonitor::new(|event| {
            println!(
                "listener: hash=0x{} nonce={} state={:?} error={:?}",
                hex::encode(event.tx_hash),
                event.nonce,
                event.state,
                event.error
            );
        });
        let hash = monitor.submit(&rpc, prepared).await?;
        ensure!(
            monitor.get(&hash).is_some(),
            "accepted order is not tracked by hash"
        );
        println!("tracked: hash=0x{}", hex::encode(hash));
        monitor.executed(&rpc, &hash, EXECUTION_TIMEOUT).await?;
        let finalized = monitor.finalized(&rpc, &hash, FINALITY_TIMEOUT).await?;
        ensure!(
            matches!(
                monitor.get(&hash).map(DeepXPerpOrderTicket::state),
                Some(DeepXPerpOrderTicketState::Finalized(_))
            ),
            "tracked order did not reach finality"
        );
        finalized
    } else {
        let mut ticket = DeepXPerpOrderTicket::submit(&rpc, prepared).await?;
        println!(
            "accepted: state={:?} order_id={} hash=0x{}",
            ticket.state(),
            ticket.nonce(),
            hex::encode(ticket.tx_hash())
        );
        let executed = ticket.executed(&rpc, EXECUTION_TIMEOUT).await?;
        println!(
            "executed: state={:?} block={} index={}",
            ticket.state(),
            executed.block_number,
            executed.extrinsic_index
        );
        let finalized = ticket.finalized(&rpc, FINALITY_TIMEOUT).await?;
        ensure!(
            matches!(ticket.state(), DeepXPerpOrderTicketState::Finalized(_)),
            "order ticket did not reach finality"
        );
        finalized
    };
    let hash =
        decode_prefixed_hash(&finalized.block_hash).context("invalid finalized block hash")?;
    let orders =
        get_perp_orders_by_id_at_block(&rpc, hash, args.params.subaccount, &[finalized.order_id])
            .await?;
    let order = orders.first().context("finalized order is missing")?;
    ensure!(
        order.order_id == finalized.order_id
            && order.owner == args.params.subaccount
            && order.market_id == args.params.market_id
            && order.is_long == args.params.is_long
            && order.size == args.params.size
            && order.price == args.params.price
            && order.status == DeepXStoredOrderStatus::Open
            && order.order_type == DeepXStoredPerpOrderType::Limit(DeepXStoredTimeInForce::Gtc)
            && order.post_only == DeepXStoredPostOnlyParam::MustPostOnly
            && !order.reduce_only
            && order.take_profit.is_none()
            && order.stop_loss.is_none()
            && order.size_filled == 0
            && order.size_remain == args.params.size,
        "finalized order state differs from signed terms"
    );
    println!(
        "finalized: order_id={} block={}; cancel or reconcile this resting order before another run",
        finalized.order_id, finalized.block_number
    );
    Ok(())
}

fn parse_args(args: impl Iterator<Item = String>) -> anyhow::Result<OrderArguments> {
    let mut args = args;
    let subaccount = args.next().context(
        "usage: <subaccount> <market-id> <long|short> <size-raw> <price-raw> [--nonce-ms N] [--submit]",
    )?;
    let subaccount = decode_account_id20(&subaccount).context("invalid subaccount AccountId20")?;
    let market_id = args.next().context("missing market ID")?.parse()?;
    let is_long = match args.next().as_deref() {
        Some("long") => true,
        Some("short") => false,
        _ => anyhow::bail!("perpetual side must be long or short"),
    };
    let size = args.next().context("missing raw size")?.parse::<u128>()?;
    let price = args.next().context("missing raw price")?.parse::<u128>()?;
    ensure!(size > 0 && price > 0, "size and price must be positive");
    let (requested_nonce, submit) = match args.next().as_deref() {
        None => (None, false),
        Some("--submit") => (None, true),
        Some("--nonce-ms") => {
            let nonce = args.next().context("missing timestamp nonce")?.parse()?;
            let submit = match args.next().as_deref() {
                None => false,
                Some("--submit") => true,
                Some(_) => anyhow::bail!("expected --submit after timestamp nonce"),
            };
            (Some(nonce), submit)
        }
        Some(_) => anyhow::bail!("expected --nonce-ms or --submit"),
    };
    ensure!(args.next().is_none(), "unexpected argument");
    Ok(OrderArguments {
        params: DeepXPerpPlaceParams {
            subaccount,
            market_id,
            is_long,
            size,
            price,
            order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXPostOnlyParam::MustPostOnly,
        },
        requested_nonce,
        submit,
    })
}

async fn observe_nonce(rpc: &DeepXRpcClient) -> anyhow::Result<(u64, String)> {
    let block_hash = rpc.get_best_block_hash_once().await?;
    let block: DeepXRpcBlockResponse = rpc.get_block(&block_hash).await?;
    let height = decode_block_number_u64(&block.block.header.number)
        .context("invalid current block height")?;
    let block_id = decode_prefixed_hash(&block_hash).context("invalid current block hash")?;
    let chain_ms = get_timestamp_now(rpc, block_id).await?;
    ensure!(
        decode_timestamp_set_inherent(&block.block.extrinsics) == Some(chain_ms),
        "current timestamp storage and block inherent disagree"
    );
    ensure!(
        rpc.get_block_hash_required_once(height)
            .await?
            .eq_ignore_ascii_case(&block_hash),
        "current block is no longer canonical"
    );
    let local_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    ensure!(
        local_ms.abs_diff(chain_ms) <= MAX_CLOCK_DRIFT_MS,
        "local clock differs too far from the DeepX chain"
    );
    Ok((local_ms.max(chain_ms), block_hash))
}

async fn read_runtime(rpc: &DeepXRpcClient, block_hash: &str) -> anyhow::Result<RuntimeSnapshot> {
    let version: DeepXRpcRuntimeVersion = get_runtime_version(rpc, block_hash).await?;
    let metadata = get_metadata(rpc, block_hash).await?;
    let metadata = hex::decode(
        metadata
            .strip_prefix("0x")
            .context("invalid metadata hex")?,
    )?;
    let genesis = decode_prefixed_hash(DEEPX_TESTNET_GENESIS_HASH)
        .context("invalid approved genesis hash")?;
    RuntimeSnapshot::approved_testnet(
        &DeepXEnvironment::Testnet,
        genesis,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn parses_exact_order_ticket_arguments() {
        let args = parse_args(
            [
                format!("0x{}", hex::encode([0x11; 20])),
                "3".to_string(),
                "short".to_string(),
                "123".to_string(),
                "456".to_string(),
                "--nonce-ms".to_string(),
                "789".to_string(),
                "--submit".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(args.params.subaccount, [0x11; 20]);
        assert_eq!(args.params.market_id, 3);
        assert!(!args.params.is_long);
        assert_eq!(args.params.size, 123);
        assert_eq!(args.params.price, 456);
        assert_eq!(args.requested_nonce, Some(789));
        assert!(args.submit);
    }
}
