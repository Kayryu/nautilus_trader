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

//! Reads wallet quota and optionally claims or purchases quota.

use std::time::{Duration, Instant};

use anyhow::{Context, ensure};
use deepx_rust_sdk::{
    credential::{DeepXKeyScheme, DeepXPrivateKey},
    deployment::{DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL},
    enums::DeepXEnvironment,
    identity::decode_account_id20,
    rest::DeepXRestTransport,
    rpc::{
        DeepXRpcBlockHeader, DeepXRpcBlockResponse, DeepXRpcClient, DeepXRpcRuntimeVersion,
        chain_decode::{decode_block_number_u64, decode_prefixed_hash},
        event_storage::get_system_events,
        get_metadata, get_runtime_version,
        quota_purchase::{
            DeepXBuyQuotaExpected, DeepXBuyQuotaInclusionOutcome, decode_buy_quota_inclusion,
            get_quote_amount_per_quota_at_block,
        },
        submission::find_extrinsic_by_hash,
        system_account::get_system_account_at_block_number,
    },
    signing::{
        DeepXBuyQuotaParams, DeepXPermittedPalletExtrinsic, DeepXRuntimeSnapshotService,
        RuntimeSnapshot, derive_signer_account_id, sign_buy_quota, sign_quota_claim,
    },
};
use serde_json::json;

const FINALITY_TIMEOUT: Duration = Duration::from_secs(120);
const FINALITY_POLL_INTERVAL: Duration = Duration::from_secs(3);
const MAX_FINALIZED_BLOCKS: u64 = 256;

enum QuotaAction {
    Read,
    ChainOnly,
    Claim(String),
    Buy {
        quota: u32,
        from_subaccount: Option<[u8; 20]>,
        submit: bool,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-sdk-quota <wallet> [--chain-only|--claim KEY|--buy COUNT [--from-subaccount ADDRESS] [--submit]]")?;
    let wallet_id = decode_account_id20(&wallet).context("invalid wallet AccountId20")?;
    let action = match args.next().as_deref() {
        None => QuotaAction::Read,
        Some("--chain-only") => QuotaAction::ChainOnly,
        Some("--claim") => {
            QuotaAction::Claim(args.next().context("--claim requires an idempotency key")?)
        }
        Some("--buy") => {
            let quota = args
                .next()
                .context("--buy requires a quota count")?
                .parse()?;
            ensure!(quota > 0, "quota purchase count must be positive");
            let (from_subaccount, submit) = match args.next().as_deref() {
                None => (None, false),
                Some("--submit") => (None, true),
                Some("--from-subaccount") => {
                    let source = args.next().context("missing source subaccount")?;
                    let source = decode_account_id20(&source)
                        .context("invalid source subaccount AccountId20")?;
                    let submit = match args.next().as_deref() {
                        None => false,
                        Some("--submit") => true,
                        Some(_) => anyhow::bail!("expected --submit after source subaccount"),
                    };
                    (Some(source), submit)
                }
                Some(_) => anyhow::bail!("expected --from-subaccount or --submit"),
            };
            QuotaAction::Buy {
                quota,
                from_subaccount,
                submit,
            }
        }
        Some(_) => anyhow::bail!("expected --chain-only, --claim, or --buy"),
    };
    ensure!(args.next().is_none(), "unexpected argument");

    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let transport = DeepXRestTransport::new(Some(15), proxy.clone())?;
    if matches!(action, QuotaAction::Read | QuotaAction::Claim(_)) {
        let public = transport
            .get_wallet_quota(DEEPX_TESTNET_REST_URL, &wallet)
            .await?;
        let summary = transport
            .get_quota_summary(DEEPX_TESTNET_REST_URL, &wallet)
            .await?;
        println!(
            "wallet={wallet} remaining={} unclaimed={} pending_grant={} earned={} granted={} reserved={} pending={}",
            public.remaining,
            public.unclaimed,
            public.pending_grant,
            summary.quota_earned,
            summary.quota_granted,
            summary.quota_reserved,
            summary.quota_pending,
        );
    }

    let rpc = DeepXRpcClient::new_with_proxy(DEEPX_TESTNET_RPC_URL.to_string(), proxy.as_deref())?;
    let genesis = rpc.get_block_hash_required_once(0).await?;
    ensure!(
        genesis.eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
        "RPC endpoint is not the approved DeepX testnet"
    );
    let finalized_hash = rpc.get_finalized_head_once().await?;
    let header: DeepXRpcBlockHeader = rpc.get_header_once(&finalized_hash).await?;
    let height =
        decode_block_number_u64(&header.number).context("invalid finalized block number")?;
    let finalized_runtime = read_runtime(&rpc, &finalized_hash).await?;
    let chain_account = get_system_account_at_block_number(&rpc, height, wallet_id).await?;
    let quote_amount_per_quota =
        get_quote_amount_per_quota_at_block(&rpc, finalized_runtime.metadata(), &finalized_hash)
            .await?;
    ensure!(
        rpc.get_block_hash_required_once(height)
            .await?
            .eq_ignore_ascii_case(&finalized_hash),
        "finalized block changed during System account query"
    );
    println!(
        "on-chain quota={} account_exists={} block={} nonce={} time_nonce_window={} quote_amount_per_quota_raw={}",
        chain_account.quota,
        chain_account.is_exist,
        height,
        chain_account.nonce,
        chain_account.time_nonce.len(),
        quote_amount_per_quota,
    );

    if let QuotaAction::Claim(ref idempotency_key) = action {
        let key =
            DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
        ensure!(
            derive_signer_account_id(&key)? == wallet_id,
            "quota claim requires the target wallet's signing key"
        );
        let signed = sign_quota_claim(&key, &wallet, idempotency_key)?;
        let submission = transport
            .post_quota_claim(DEEPX_TESTNET_REST_URL, &signed)
            .await
            .context("claim submission outcome may be uncertain; reconcile the idempotency key")?;
        println!(
            "claim status={} created={}",
            submission.status, submission.created
        );
        if let Some(claim) = submission.claim {
            let confirmed = transport
                .wait_quota_claim(
                    DEEPX_TESTNET_REST_URL,
                    claim.id,
                    Duration::from_secs(120),
                    Duration::from_secs(2),
                )
                .await?;
            println!(
                "claim confirmed: id={} data={}",
                confirmed.id,
                confirmed.data()
            );
        }
    }
    if let QuotaAction::Buy {
        quota,
        from_subaccount,
        submit,
    } = action
    {
        let key =
            DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
        let signer = derive_signer_account_id(&key)?;
        let block_hash = rpc.get_best_block_hash_once().await?;
        let snapshot = read_runtime(&rpc, &block_hash).await?;
        let purchase_price =
            get_quote_amount_per_quota_at_block(&rpc, snapshot.metadata(), &block_hash).await?;
        let cost = purchase_price
            .checked_mul(u128::from(quota))
            .context("quota purchase cost exceeds runtime integer range")?;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire()?;
        let nonce = rpc.get_account_next_index(signer).await?;
        let params = DeepXBuyQuotaParams {
            address: wallet_id,
            quota,
            from_subaccount,
        };
        let signed = sign_buy_quota(&permit, &key, params, nonce)?;
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
            "quota purchase dry run failed: {dry_run}"
        );
        println!(
            "quota purchase dry run accepted: count={quota} quoted_cost_raw={cost} payer={from_subaccount:?} nonce={nonce} hash=0x{}",
            hex::encode(signed.extrinsic_hash())
        );
        if submit {
            submit_and_verify_purchase(&rpc, &signed, params, purchase_price).await?;
        }
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

async fn submit_and_verify_purchase(
    rpc: &DeepXRpcClient,
    signed: &DeepXPermittedPalletExtrinsic,
    params: DeepXBuyQuotaParams,
    purchase_price: u128,
) -> anyhow::Result<()> {
    let expected_hash = format!("0x{}", hex::encode(signed.extrinsic_hash()));
    let encoded = format!("0x{}", hex::encode(signed.bytes()));
    ensure!(
        rpc.get_account_next_index(signed.signer()).await? == signed.nonce(),
        "account nonce changed before quota purchase submission"
    );
    let current_hash = rpc.get_best_block_hash_once().await?;
    let current_runtime = read_runtime(rpc, &current_hash).await?;
    ensure!(
        current_runtime.identity() == signed.runtime(),
        "runtime changed after quota purchase signing"
    );
    ensure!(
        get_quote_amount_per_quota_at_block(rpc, current_runtime.metadata(), &current_hash).await?
            == purchase_price,
        "quota purchase price changed after dry run"
    );
    ensure!(
        !rpc.get_pending_extrinsics()
            .await?
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&encoded)),
        "quota purchase extrinsic is already pending"
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
            format!("quota purchase submission uncertain; reconcile hash={expected_hash}")
        })?;
    ensure!(
        submitted_hash.eq_ignore_ascii_case(&expected_hash),
        "submission returned another hash; reconcile expected hash={expected_hash}"
    );
    println!("quota purchase submitted: hash={expected_hash}; awaiting finality");
    let deadline = Instant::now() + FINALITY_TIMEOUT;
    loop {
        ensure!(
            Instant::now() < deadline,
            "quota purchase finality timed out; reconcile hash={expected_hash}"
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
            "quota purchase finality scan exceeded its block limit; reconcile hash={expected_hash}"
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
            let outcome = decode_buy_quota_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                DeepXBuyQuotaExpected {
                    params,
                    signer: signed.signer(),
                    extrinsic_index: index,
                },
                &events,
            )?;
            ensure!(
                outcome == DeepXBuyQuotaInclusionOutcome::Success,
                "quota purchase dispatch failed; hash={expected_hash}"
            );
            let account = get_system_account_at_block_number(rpc, height, params.address).await?;
            ensure!(
                rpc.get_block_hash_required_once(height)
                    .await?
                    .eq_ignore_ascii_case(&block_hash),
                "finalized block changed during quota purchase verification; reconcile hash={expected_hash}"
            );
            println!(
                "quota purchase finalized: block={height} index={index} hash={expected_hash} on_chain_quota={}",
                account.quota,
            );
            return Ok(());
        }
        next_height = finalized_height
            .checked_add(1)
            .context("finalized block number overflow")?;
        tokio::time::sleep(FINALITY_POLL_INTERVAL).await;
    }
}
