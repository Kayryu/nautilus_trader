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

//! Reads and optionally changes perpetual leverage caps on DeepX testnet.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, ensure};
use deepx_rust_sdk::{
    credential::{DeepXKeyScheme, DeepXPrivateKey},
    deployment::{DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL},
    enums::DeepXEnvironment,
    identity::decode_account_id20,
    rest::{
        DeepXRestTransport,
        ownership::{
            verify_account_ownership_evidence, verify_delegated_account_ownership_evidence,
        },
    },
    rpc::{
        DeepXRpcBlockHeader, DeepXRpcBlockResponse, DeepXRpcClient, DeepXRpcRuntimeVersion,
        chain_decode::{decode_block_number_u64, decode_prefixed_hash},
        event_decode::DeepXSystemDispatchOutcome,
        event_storage::get_system_events,
        get_metadata, get_runtime_version,
        leverage::{
            DeepXPerpLeverageExpected, decode_perp_leverage_inclusion, get_perp_leverage_at_block,
        },
        submission::find_extrinsic_by_hash,
    },
    signing::{
        DeepXPermittedPalletExtrinsic, DeepXRuntimeSnapshotService, DeepXSetGlobalLeverageParams,
        DeepXSetPerMarketLeverageParams, RuntimeSnapshot, derive_signer_account_id,
        sign_set_global_leverage, sign_set_per_market_leverage,
    },
};
use serde_json::json;

const FINALITY_TIMEOUT: Duration = Duration::from_secs(120);
const FINALITY_POLL_INTERVAL: Duration = Duration::from_secs(3);
const MAX_FINALIZED_BLOCKS: u64 = 256;

#[derive(Clone, Copy)]
enum LeverageAction {
    SetGlobal(u64),
    ClearGlobal,
    SetMarket(u64),
    ClearMarket,
}

impl LeverageAction {
    fn expected(self, subaccount: [u8; 20], market_id: u16) -> DeepXPerpLeverageExpected {
        match self {
            Self::SetGlobal(max_leverage) => DeepXPerpLeverageExpected {
                subaccount,
                market_id: None,
                max_leverage: Some(max_leverage),
                extrinsic_index: 0,
            },
            Self::ClearGlobal => DeepXPerpLeverageExpected {
                subaccount,
                market_id: None,
                max_leverage: None,
                extrinsic_index: 0,
            },
            Self::SetMarket(max_leverage) => DeepXPerpLeverageExpected {
                subaccount,
                market_id: Some(market_id),
                max_leverage: Some(max_leverage),
                extrinsic_index: 0,
            },
            Self::ClearMarket => DeepXPerpLeverageExpected {
                subaccount,
                market_id: Some(market_id),
                max_leverage: None,
                extrinsic_index: 0,
            },
        }
    }

    fn is_global(self) -> bool {
        matches!(self, Self::SetGlobal(_) | Self::ClearGlobal)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let subaccount = args
        .next()
        .context("usage: deepx-sdk-leverage <subaccount> <market-id> [--set-global N|--clear-global|--set-market N|--clear-market] [--submit]")?;
    let subaccount_id =
        decode_account_id20(&subaccount).context("invalid subaccount AccountId20")?;
    let market_id = args.next().context("missing market ID")?.parse::<u16>()?;
    let action = match args.next().as_deref() {
        None => None,
        Some("--set-global") => Some(LeverageAction::SetGlobal(parse_leverage(args.next())?)),
        Some("--clear-global") => Some(LeverageAction::ClearGlobal),
        Some("--set-market") => Some(LeverageAction::SetMarket(parse_leverage(args.next())?)),
        Some("--clear-market") => Some(LeverageAction::ClearMarket),
        Some(_) => anyhow::bail!("expected a leverage action"),
    };
    let submit = match args.next().as_deref() {
        None => false,
        Some("--submit") if action.is_some() => true,
        Some(_) => anyhow::bail!("expected --submit after a leverage action"),
    };
    ensure!(args.next().is_none(), "unexpected argument");

    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let rpc = DeepXRpcClient::new_with_proxy(DEEPX_TESTNET_RPC_URL.to_string(), proxy.as_deref())?;
    let genesis = rpc.get_block_hash_required_once(0).await?;
    ensure!(
        genesis.eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
        "RPC endpoint is not the approved DeepX testnet"
    );
    show_leverage(&rpc, subaccount_id, market_id).await?;
    let Some(action) = action else {
        return Ok(());
    };

    let key = DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
    let signer = derive_signer_account_id(&key)?;
    verify_authority(&subaccount, signer, action.is_global()).await?;
    let block_hash = rpc.get_best_block_hash_once().await?;
    let snapshot = read_runtime(&rpc, &block_hash).await?;
    let service = DeepXRuntimeSnapshotService::new(snapshot);
    let permit = service.acquire()?;
    let nonce = rpc.get_account_next_index(signer).await?;
    let expected = action.expected(subaccount_id, market_id);
    let signed = if action.is_global() {
        sign_set_global_leverage(
            &permit,
            &key,
            DeepXSetGlobalLeverageParams {
                subaccount: subaccount_id,
                max_leverage: expected.max_leverage,
            },
            nonce,
        )?
    } else {
        sign_set_per_market_leverage(
            &permit,
            &key,
            DeepXSetPerMarketLeverageParams {
                subaccount: subaccount_id,
                market_id,
                max_leverage: expected.max_leverage,
            },
            nonce,
        )?
    };
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
        "leverage dry run was not accepted: {dry_run}"
    );
    println!(
        "leverage dry run accepted: signer=0x{} nonce={nonce} hash=0x{}",
        hex::encode(signer),
        hex::encode(signed.extrinsic_hash()),
    );
    if submit {
        submit_and_verify(&rpc, &signed, expected, market_id).await?;
    }
    Ok(())
}

fn parse_leverage(value: Option<String>) -> anyhow::Result<u64> {
    let value = value
        .context("setting leverage requires a value")?
        .parse::<u64>()?;
    ensure!(
        value > 0 && value % 1_000 == 0,
        "leverage must be a positive multiple of 1000"
    );
    Ok(value)
}

async fn show_leverage(
    rpc: &DeepXRpcClient,
    subaccount: [u8; 20],
    market_id: u16,
) -> anyhow::Result<()> {
    let finalized_hash = rpc.get_finalized_head_once().await?;
    let header: DeepXRpcBlockHeader = rpc.get_header_once(&finalized_hash).await?;
    let block_number = decode_block_number_u64(&header.number).context("invalid block number")?;
    let view = get_perp_leverage_at_block(rpc, subaccount, market_id, block_number).await?;
    ensure!(
        rpc.get_block_hash_required_once(block_number)
            .await?
            .eq_ignore_ascii_case(&finalized_hash),
        "finalized block changed during leverage query"
    );
    println!(
        "subaccount=0x{} market_id={market_id} block={block_number} global={} per_market={} effective={} scale=1000",
        hex::encode(subaccount),
        view.global_max_leverage,
        view.per_market_max_leverage,
        view.effective_leverage,
    );
    Ok(())
}

async fn verify_authority(subaccount: &str, signer: [u8; 20], global: bool) -> anyhow::Result<()> {
    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let rest = DeepXRestTransport::new(Some(15), proxy)?;
    let profile = rest
        .get_subaccount_info(DEEPX_TESTNET_REST_URL, subaccount)
        .await?;
    let directory = rest
        .get_wallet_subaccounts(DEEPX_TESTNET_REST_URL, &profile.authority)
        .await?;
    if global || decode_account_id20(&profile.authority) == Some(signer) {
        verify_account_ownership_evidence(signer, subaccount, &directory, &profile)?;
    } else {
        let signer_address = format!("0x{}", hex::encode(signer));
        let delegates = rest
            .get_delegate_accounts(DEEPX_TESTNET_REST_URL, &profile.authority)
            .await?;
        let delegators = rest
            .get_delegator_accounts(DEEPX_TESTNET_REST_URL, &signer_address)
            .await?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_millis();
        verify_delegated_account_ownership_evidence(
            signer,
            subaccount,
            &directory,
            &profile,
            &delegates,
            &delegators,
            u64::try_from(now_ms).context("current timestamp exceeds u64")?,
        )?;
    }
    Ok(())
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

async fn submit_and_verify(
    rpc: &DeepXRpcClient,
    signed: &DeepXPermittedPalletExtrinsic,
    expected: DeepXPerpLeverageExpected,
    market_id: u16,
) -> anyhow::Result<()> {
    let expected_hash = format!("0x{}", hex::encode(signed.extrinsic_hash()));
    let encoded = format!("0x{}", hex::encode(signed.bytes()));
    ensure!(
        rpc.get_account_next_index(signed.signer()).await? == signed.nonce(),
        "sequential account nonce changed before leverage submission"
    );
    let current_hash = rpc.get_best_block_hash_once().await?;
    ensure!(
        read_runtime(rpc, &current_hash).await?.identity() == signed.runtime(),
        "runtime changed after leverage signing"
    );
    ensure!(
        !rpc.get_pending_extrinsics()
            .await?
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&encoded)),
        "leverage extrinsic is already pending"
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
            format!("leverage submission uncertain; reconcile hash={expected_hash}")
        })?;
    ensure!(
        submitted_hash.eq_ignore_ascii_case(&expected_hash),
        "submission returned another hash; reconcile expected hash={expected_hash}"
    );
    println!("leverage submitted: hash={expected_hash}; awaiting finality");
    let deadline = Instant::now() + FINALITY_TIMEOUT;
    loop {
        ensure!(
            Instant::now() < deadline,
            "leverage finality timed out; reconcile hash={expected_hash}"
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
            "leverage finality scan exceeded its block limit; reconcile hash={expected_hash}"
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
            let evidence = decode_perp_leverage_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                DeepXPerpLeverageExpected {
                    extrinsic_index: index,
                    ..expected
                },
                &events,
            )?;
            ensure!(
                evidence.outcome == DeepXSystemDispatchOutcome::Success,
                "leverage dispatch failed; hash={expected_hash}"
            );
            let view =
                get_perp_leverage_at_block(rpc, expected.subaccount, market_id, height).await?;
            let actual = if expected.market_id.is_some() {
                view.per_market_max_leverage
            } else {
                view.global_max_leverage
            };
            ensure!(
                actual == expected.max_leverage.unwrap_or(0),
                "finalized leverage view differs from submitted terms; hash={expected_hash}"
            );
            ensure!(
                rpc.get_block_hash_required_once(height)
                    .await?
                    .eq_ignore_ascii_case(&block_hash),
                "finalized block changed; reconcile hash={expected_hash}"
            );
            println!(
                "leverage finalized: block={height} index={index} hash={expected_hash} global={} per_market={} effective={}",
                view.global_max_leverage, view.per_market_max_leverage, view.effective_leverage
            );
            return Ok(());
        }
        next_height = finalized_height
            .checked_add(1)
            .context("finalized block number overflow")?;
        tokio::time::sleep(FINALITY_POLL_INTERVAL).await;
    }
}
