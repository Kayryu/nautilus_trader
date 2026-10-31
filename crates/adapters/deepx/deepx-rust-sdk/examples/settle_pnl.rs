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

//! Signs and dry-runs permissionless Perp PnL settlement on DeepX testnet.

use std::time::{Duration, Instant};

use anyhow::{Context, ensure};
use deepx_rust_sdk::{
    credential::{DeepXKeyScheme, DeepXPrivateKey},
    deployment::{DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_RPC_URL},
    enums::DeepXEnvironment,
    identity::decode_account_id20,
    rpc::{
        DeepXRpcBlockHeader, DeepXRpcBlockResponse, DeepXRpcClient, DeepXRpcRuntimeVersion,
        chain_decode::{decode_block_number_u64, decode_prefixed_hash},
        event_storage::get_system_events,
        get_metadata, get_runtime_version,
        perp_position::{
            DeepXPerpPositionInclusionOutcome, DeepXPerpSettleExpected, DeepXPerpSettlement,
            decode_perp_settle_inclusion,
        },
        submission::find_extrinsic_by_hash,
    },
    signing::{
        DeepXPermittedPalletExtrinsic, DeepXPerpSettlePnlParams, DeepXRuntimeSnapshotService,
        RuntimeSnapshot, derive_signer_account_id, sign_perp_settle_pnl,
    },
};
use serde_json::json;

const FINALITY_TIMEOUT: Duration = Duration::from_secs(120);
const FINALITY_POLL_INTERVAL: Duration = Duration::from_secs(3);
const MAX_FINALIZED_BLOCKS: u64 = 256;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let subaccount = args
        .next()
        .context("usage: deepx-sdk-settle-pnl <subaccount> [market-id] [--submit|--repeat]")?;
    let subaccount = decode_account_id20(&subaccount).context("invalid subaccount AccountId20")?;
    let (market_id, mode) = match args.next().as_deref() {
        None => (None, None),
        Some("--submit") => (None, Some("--submit")),
        Some("--repeat") => anyhow::bail!("--repeat requires one market ID"),
        Some(value) => {
            let mode = match args.next().as_deref() {
                None => None,
                Some("--submit") => Some("--submit"),
                Some("--repeat") => Some("--repeat"),
                Some(_) => anyhow::bail!("expected --submit or --repeat after market ID"),
            };
            (Some(value.parse::<u16>()?), mode)
        }
    };
    ensure!(args.next().is_none(), "unexpected argument");

    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let rpc = DeepXRpcClient::new_with_proxy(DEEPX_TESTNET_RPC_URL.to_string(), proxy.as_deref())?;
    let observed_genesis = rpc.get_block_hash_required_once(0).await?;
    ensure!(
        observed_genesis.eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
        "RPC endpoint is not the approved DeepX testnet"
    );
    let key = DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
    let signed = sign_and_dry_run(&rpc, &key, subaccount, market_id).await?;
    if mode.is_some() {
        let first = submit_and_verify_settlement(&rpc, &signed, subaccount, market_id).await?;
        if mode == Some("--repeat") {
            let repeated = sign_and_dry_run(&rpc, &key, subaccount, market_id).await?;
            ensure!(
                repeated.nonce() > signed.nonce(),
                "repeat settlement did not acquire a fresh nonce"
            );
            let second =
                submit_and_verify_settlement(&rpc, &repeated, subaccount, market_id).await?;
            println!(
                "repeat settlement: first_total={} second_total={} quote_base_units",
                first
                    .first()
                    .context("first market settlement event is missing")?
                    .total,
                second
                    .first()
                    .context("second market settlement event is missing")?
                    .total,
            );
        }
    }
    Ok(())
}

async fn sign_and_dry_run(
    rpc: &DeepXRpcClient,
    key: &DeepXPrivateKey,
    subaccount: [u8; 20],
    market_id: Option<u16>,
) -> anyhow::Result<DeepXPermittedPalletExtrinsic> {
    let block_hash = rpc.get_best_block_hash_once().await?;
    let snapshot = read_runtime(rpc, &block_hash).await?;
    let service = DeepXRuntimeSnapshotService::new(snapshot);
    let permit = service.acquire()?;
    let signer = derive_signer_account_id(key)?;
    let nonce = rpc.get_account_next_index(signer).await?;
    let signed = sign_perp_settle_pnl(
        &permit,
        key,
        DeepXPerpSettlePnlParams {
            subaccount,
            market_id,
        },
        nonce,
    )?;
    let dry_run: String = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "system_dryRunAt",
            "params": [format!("0x{}", hex::encode(signed.bytes())), block_hash],
        }))
        .await?;
    ensure!(
        dry_run == "0x0000",
        "settlement dry run was not accepted: {dry_run}"
    );
    println!(
        "settlement dry run accepted: subaccount=0x{} market={market_id:?} signer=0x{} nonce={nonce} hash=0x{}",
        hex::encode(subaccount),
        hex::encode(signer),
        hex::encode(signed.extrinsic_hash()),
    );
    Ok(signed)
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

async fn submit_and_verify_settlement(
    rpc: &DeepXRpcClient,
    signed: &DeepXPermittedPalletExtrinsic,
    subaccount: [u8; 20],
    market_id: Option<u16>,
) -> anyhow::Result<Vec<DeepXPerpSettlement>> {
    let expected_hash = format!("0x{}", hex::encode(signed.extrinsic_hash()));
    let encoded = format!("0x{}", hex::encode(signed.bytes()));
    let current_nonce = rpc.get_account_next_index(signed.signer()).await?;
    ensure!(
        current_nonce == signed.nonce(),
        "sequential account nonce changed before settlement submission"
    );
    let current_hash = rpc.get_best_block_hash_once().await?;
    ensure!(
        read_runtime(rpc, &current_hash).await?.identity() == signed.runtime(),
        "runtime changed after settlement signing"
    );
    ensure!(
        !rpc.get_pending_extrinsics()
            .await?
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&encoded)),
        "settlement extrinsic is already pending"
    );
    let finalized_before = rpc.get_finalized_head_once().await?;
    let header: DeepXRpcBlockHeader = rpc.get_header_once(&finalized_before).await?;
    let mut next_height = decode_block_number_u64(&header.number)
        .context("invalid finalized block number")?
        .checked_add(1)
        .context("finalized block number overflow")?;
    let submitted_hash = rpc
        .submit_encoded_extrinsic(&encoded)
        .await
        .with_context(|| {
            format!("settlement submission uncertain; reconcile hash={expected_hash}")
        })?;
    ensure!(
        submitted_hash.eq_ignore_ascii_case(&expected_hash),
        "submission returned another hash; reconcile expected hash={expected_hash}"
    );
    println!("settlement submitted: hash={expected_hash}; awaiting finality");
    let deadline = Instant::now() + FINALITY_TIMEOUT;
    loop {
        ensure!(
            Instant::now() < deadline,
            "settlement finality timed out; reconcile hash={expected_hash}"
        );
        let finalized_hash = rpc.get_finalized_head_once().await?;
        let header: DeepXRpcBlockHeader = rpc.get_header_once(&finalized_hash).await?;
        let finalized_height =
            decode_block_number_u64(&header.number).context("invalid finalized block number")?;
        ensure!(
            finalized_height >= next_height.saturating_sub(1),
            "finalized height regressed; reconcile hash={expected_hash}"
        );
        ensure!(
            finalized_height.saturating_sub(next_height) < MAX_FINALIZED_BLOCKS,
            "settlement finality scan exceeded its block limit; reconcile hash={expected_hash}"
        );
        for height in next_height..=finalized_height {
            let block_hash = rpc.get_block_hash_required_once(height).await?;
            let block: DeepXRpcBlockResponse = rpc.get_block(&block_hash).await?;
            ensure!(
                decode_block_number_u64(&block.block.header.number) == Some(height),
                "finalized block height mismatch; reconcile hash={expected_hash}"
            );
            let Some(index) =
                find_extrinsic_by_hash(&block.block.extrinsics, signed.extrinsic_hash())?
            else {
                continue;
            };
            ensure!(
                block.block.extrinsics[index as usize].eq_ignore_ascii_case(&encoded),
                "finalized extrinsic bytes differ; reconcile hash={expected_hash}"
            );
            let events = get_system_events(rpc, &block_hash, height).await?;
            ensure!(
                events == get_system_events(rpc, &block_hash, height).await?,
                "finalized event storage changed; reconcile hash={expected_hash}"
            );
            let runtime = read_runtime(rpc, &block_hash).await?;
            ensure!(
                runtime.identity() == signed.runtime(),
                "inclusion runtime differs from signing runtime; reconcile hash={expected_hash}"
            );
            let evidence = decode_perp_settle_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                DeepXPerpSettleExpected {
                    params: DeepXPerpSettlePnlParams {
                        subaccount,
                        market_id,
                    },
                    extrinsic_index: index,
                },
                &events,
            )?;
            ensure!(
                rpc.get_block_hash_required_once(height)
                    .await?
                    .eq_ignore_ascii_case(&block_hash),
                "finalized block changed; reconcile hash={expected_hash}"
            );
            ensure!(
                evidence.outcome == DeepXPerpPositionInclusionOutcome::Success,
                "settlement dispatch failed; hash={expected_hash}"
            );
            println!(
                "settlement finalized: block={height} index={index} hash={expected_hash} events={}",
                evidence.settlements.len()
            );
            for settlement in &evidence.settlements {
                println!(
                    "market={} unrealized={} funding={} total={} quote_base_units",
                    settlement.market_id,
                    settlement.unrealized,
                    settlement.funding,
                    settlement.total,
                );
            }
            return Ok(evidence.settlements);
        }
        next_height = finalized_height
            .checked_add(1)
            .context("finalized block number overflow")?;
        tokio::time::sleep(FINALITY_POLL_INTERVAL).await;
    }
}
