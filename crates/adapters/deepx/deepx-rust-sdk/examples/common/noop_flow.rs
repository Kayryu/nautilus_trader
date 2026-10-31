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

//! Consumes a timestamp nonce or replaces a pending extrinsic with a no-op.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, ensure};
use deepx_rust_sdk::{
    credential::{DeepXKeyScheme, DeepXPrivateKey},
    deployment::{DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_RPC_URL},
    enums::DeepXEnvironment,
    identity::decode_account_id20,
    rpc::{
        DeepXRpcBlockHeader, DeepXRpcBlockResponse, DeepXRpcClient, DeepXRpcRuntimeVersion,
        chain_decode::{decode_block_number_u64, decode_prefixed_hash},
        event_decode::{DeepXSystemDispatchOutcome, decode_indexed_system_dispatch},
        event_storage::get_system_events,
        get_metadata, get_runtime_version,
        submission::{decode_signed_extrinsic_identity, find_extrinsic_by_hash},
        timestamp::{decode_timestamp_set_inherent, get_timestamp_now},
    },
    signing::{
        DeepXPermittedPalletExtrinsic, DeepXRuntimeSnapshotService, RuntimeSnapshot,
        derive_signer_account_id, sign_no_op,
    },
};
use serde_json::json;

const FINALITY_TIMEOUT: Duration = Duration::from_secs(120);
const FINALITY_POLL_INTERVAL: Duration = Duration::from_secs(3);
const MAX_FINALIZED_BLOCKS: u64 = 256;
const MAX_CLOCK_DRIFT_MS: u64 = 30_000;

enum NonceSource {
    Fresh,
    Explicit(u64),
    Pending([u8; 32]),
}

pub(crate) async fn run_example(replacement: bool) -> anyhow::Result<()> {
    let arguments = route_arguments(replacement, std::env::args().skip(1))?;
    let mut args = arguments.into_iter();
    let wallet = args.next().context(
        "usage: deepx-sdk-noop <wallet> [--nonce-ms N|--replace PENDING_HASH] [--submit]",
    )?;
    let signer = decode_account_id20(&wallet).context("invalid wallet AccountId20")?;
    let source = match args.next().as_deref() {
        Some("--nonce-ms") => NonceSource::Explicit(
            args.next()
                .context("--nonce-ms requires a value")?
                .parse()?,
        ),
        Some("--replace") => NonceSource::Pending(
            decode_prefixed_hash(&args.next().context("--replace requires a pending hash")?)
                .context("invalid pending extrinsic hash")?,
        ),
        Some("--submit") => {
            ensure!(args.next().is_none(), "unexpected argument");
            return execute(signer, NonceSource::Fresh, true).await;
        }
        None => NonceSource::Fresh,
        Some(_) => anyhow::bail!("expected --nonce-ms, --replace, or --submit"),
    };
    let submit = match args.next().as_deref() {
        None => false,
        Some("--submit") => true,
        Some(_) => anyhow::bail!("expected --submit"),
    };
    ensure!(args.next().is_none(), "unexpected argument");
    execute(signer, source, submit).await
}

fn route_arguments(
    replacement: bool,
    arguments: impl Iterator<Item = String>,
) -> anyhow::Result<Vec<String>> {
    let mut arguments: Vec<String> = arguments.collect();

    if replacement {
        ensure!(
            arguments.len() >= 2,
            "usage: <wallet> <pending-hash> [--submit]"
        );
        ensure!(
            arguments.get(1).is_none_or(|value| value != "--replace"),
            "--replace is implicit in this example"
        );
        arguments.insert(1, "--replace".to_string());
    }
    Ok(arguments)
}

async fn execute(signer: [u8; 20], source: NonceSource, submit: bool) -> anyhow::Result<()> {
    let key = DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
    ensure!(
        derive_signer_account_id(&key)? == signer,
        "no-op requires the wallet's signing key"
    );
    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let rpc = DeepXRpcClient::new_with_proxy(DEEPX_TESTNET_RPC_URL.to_string(), proxy.as_deref())?;
    ensure!(
        rpc.get_block_hash_required_once(0)
            .await?
            .eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
        "RPC endpoint is not the approved DeepX testnet"
    );
    let block_hash = rpc.get_best_block_hash_once().await?;
    let snapshot = read_runtime(&rpc, &block_hash).await?;
    let original_hash = match source {
        NonceSource::Pending(hash) => Some(hash),
        _ => None,
    };
    let nonce = match source {
        NonceSource::Fresh => fresh_nonce(&rpc, &block_hash).await?,
        NonceSource::Explicit(nonce) => nonce,
        NonceSource::Pending(hash) => {
            pending_nonce(&rpc, snapshot.metadata(), signer, hash).await?
        }
    };
    let permit = DeepXRuntimeSnapshotService::new(snapshot).acquire()?;
    let signed = sign_no_op(&permit, &key, nonce)?;
    let dry_run: String = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "system_dryRunAt",
            "params": [format!("0x{}", hex::encode(signed.bytes())), block_hash],
        }))
        .await?;
    let dry_run_state = match dry_run.as_str() {
        "0x0000" => "accepted",
        "0x01000b" if original_hash.is_some() => "inconclusive",
        _ => anyhow::bail!("no-op dry run was rejected: {dry_run}"),
    };
    println!(
        "no-op dry_run={dry_run_state}: signer=0x{} nonce={nonce} hash=0x{}",
        hex::encode(signer),
        hex::encode(signed.extrinsic_hash())
    );

    if submit {
        submit_and_verify(&rpc, &signed, original_hash).await?;
    }
    Ok(())
}

async fn fresh_nonce(rpc: &DeepXRpcClient, block_hash: &str) -> anyhow::Result<u64> {
    let block: DeepXRpcBlockResponse = rpc.get_block(block_hash).await?;
    let block_number = decode_block_number_u64(&block.block.header.number)
        .context("invalid current block number")?;
    let block_id = decode_prefixed_hash(block_hash).context("invalid current block hash")?;
    let chain_ms = get_timestamp_now(rpc, block_id).await?;
    ensure!(
        decode_timestamp_set_inherent(&block.block.extrinsics) == Some(chain_ms),
        "current timestamp storage and block inherent disagree"
    );
    ensure!(
        rpc.get_block_hash_required_once(block_number)
            .await?
            .eq_ignore_ascii_case(block_hash),
        "current block is no longer canonical"
    );
    let local_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    ensure!(
        local_ms.abs_diff(chain_ms) <= MAX_CLOCK_DRIFT_MS,
        "local clock differs too far from DeepX chain"
    );
    Ok(local_ms.max(chain_ms))
}

async fn pending_nonce(
    rpc: &DeepXRpcClient,
    metadata: &subxt_core::metadata::Metadata,
    signer: [u8; 20],
    original_hash: [u8; 32],
) -> anyhow::Result<u64> {
    let pending = rpc.get_pending_extrinsics().await?;
    let index = find_extrinsic_by_hash(&pending, original_hash)?
        .context("target extrinsic is not in this RPC endpoint's pending pool")?;
    let encoded = &pending[index as usize];
    let bytes = hex::decode(
        encoded
            .strip_prefix("0x")
            .context("invalid pending extrinsic hex")?,
    )?;
    let identity = decode_signed_extrinsic_identity(metadata, &bytes)?;
    ensure!(
        identity.signer == signer,
        "pending extrinsic belongs to another signer"
    );
    println!(
        "pending extrinsic matched: hash=0x{} nonce={}",
        hex::encode(original_hash),
        identity.nonce
    );
    Ok(identity.nonce)
}

async fn read_runtime(rpc: &DeepXRpcClient, block_hash: &str) -> anyhow::Result<RuntimeSnapshot> {
    let version: DeepXRpcRuntimeVersion = get_runtime_version(rpc, block_hash).await?;
    let metadata = get_metadata(rpc, block_hash).await?;
    let metadata = hex::decode(
        metadata
            .strip_prefix("0x")
            .context("invalid metadata hex")?,
    )?;
    RuntimeSnapshot::approved_testnet(
        &DeepXEnvironment::Testnet,
        decode_prefixed_hash(DEEPX_TESTNET_GENESIS_HASH).context("invalid genesis hash")?,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )
    .map_err(Into::into)
}

async fn submit_and_verify(
    rpc: &DeepXRpcClient,
    signed: &DeepXPermittedPalletExtrinsic,
    original_hash: Option<[u8; 32]>,
) -> anyhow::Result<()> {
    let expected_hash = format!("0x{}", hex::encode(signed.extrinsic_hash()));
    let encoded = format!("0x{}", hex::encode(signed.bytes()));
    let current_hash = rpc.get_best_block_hash_once().await?;
    ensure!(
        read_runtime(rpc, &current_hash).await?.identity() == signed.runtime(),
        "runtime changed after no-op signing"
    );
    ensure!(
        !rpc.get_pending_extrinsics()
            .await?
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&encoded)),
        "no-op is already pending"
    );
    let finalized_before = rpc.get_finalized_head_once().await?;
    let header: DeepXRpcBlockHeader = rpc.get_header_once(&finalized_before).await?;
    let mut next_height = decode_block_number_u64(&header.number)
        .context("invalid finalized block number")?
        .checked_add(1)
        .context("finalized block number overflow")?;

    if let Some(hash) = original_hash {
        ensure!(
            pending_nonce(
                rpc,
                read_runtime(rpc, &current_hash).await?.metadata(),
                signed.signer(),
                hash
            )
            .await?
                == signed.nonce(),
            "pending target nonce changed before no-op submission"
        );
    }
    rpc.submit_verified_extrinsic_once(signed.bytes(), signed.extrinsic_hash())
        .await
        .with_context(|| format!("no-op submission uncertain; reconcile hash={expected_hash}"))?;
    println!("no-op submitted: hash={expected_hash}; awaiting finality");
    let deadline = Instant::now() + FINALITY_TIMEOUT;

    loop {
        ensure!(
            Instant::now() < deadline,
            "no-op finality timed out; reconcile hash={expected_hash}"
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
            "no-op scan exceeded block limit; reconcile hash={expected_hash}"
        );

        for height in next_height..=finalized_height {
            let block_hash = rpc.get_block_hash_required_once(height).await?;
            let block: DeepXRpcBlockResponse = rpc.get_block(&block_hash).await?;
            ensure!(
                decode_block_number_u64(&block.block.header.number) == Some(height),
                "finalized block height mismatch; reconcile hash={expected_hash}"
            );

            if let Some(hash) = original_hash {
                ensure!(
                    find_extrinsic_by_hash(&block.block.extrinsics, hash)?.is_none(),
                    "original transaction finalized before the no-op; reconcile original hash=0x{}",
                    hex::encode(hash)
                );
            }
            let Some(index) =
                find_extrinsic_by_hash(&block.block.extrinsics, signed.extrinsic_hash())?
            else {
                continue;
            };
            ensure!(
                block.block.extrinsics[index as usize].eq_ignore_ascii_case(&encoded),
                "finalized no-op bytes differ; reconcile hash={expected_hash}"
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
            let dispatch = decode_indexed_system_dispatch(runtime.metadata(), index, &events)?;
            ensure!(
                rpc.get_block_hash_required_once(height)
                    .await?
                    .eq_ignore_ascii_case(&block_hash),
                "finalized block changed; reconcile hash={expected_hash}"
            );
            ensure!(
                dispatch.outcome == DeepXSystemDispatchOutcome::Success,
                "no-op dispatch failed; reconcile hash={expected_hash}"
            );
            println!(
                "no-op finalized: block={height} index={index} nonce={} hash={expected_hash}",
                signed.nonce()
            );

            if let Some(hash) = original_hash {
                println!(
                    "original pending hash=0x{} was not finalized before the no-op",
                    hex::encode(hash)
                );
            }
            return Ok(());
        }
        next_height = finalized_height
            .checked_add(1)
            .context("finalized block number overflow")?;
        tokio::time::sleep(FINALITY_POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn replacement_entry_point_routes_hash_before_submit_flag() {
        let wallet = "0x1111111111111111111111111111111111111111".to_string();
        let hash = format!("0x{}", hex::encode([0x22; 32]));
        let arguments = vec![wallet.clone(), hash.clone(), "--submit".to_string()];
        assert_eq!(
            route_arguments(true, arguments.clone().into_iter()).unwrap(),
            [
                wallet,
                "--replace".to_string(),
                hash,
                "--submit".to_string()
            ]
        );
        assert_eq!(
            route_arguments(false, arguments.clone().into_iter()).unwrap(),
            arguments
        );
        assert!(route_arguments(true, ["wallet".to_string()].into_iter()).is_err());
    }
}
