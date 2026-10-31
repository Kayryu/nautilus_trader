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

//! Creates or deletes a testnet subaccount using its owner's key.

use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use deepx_rust_sdk::{
    credential::{DeepXKeyScheme, DeepXPrivateKey},
    deployment::{DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL},
    enums::DeepXEnvironment,
    identity::decode_account_id20,
    query::{DeepXAccountSortOrder, DeepXPerpOpenOrdersRequest},
    rest::{DeepXRestTransport, models::DeepXWalletSubaccounts},
    rpc::{
        DeepXRpcBlockHeader, DeepXRpcBlockResponse, DeepXRpcClient, DeepXRpcRuntimeVersion,
        chain_decode::{decode_block_number_u64, decode_prefixed_hash},
        event_decode::DeepXSystemDispatchOutcome,
        event_storage::get_system_events,
        get_metadata, get_runtime_version,
        onboarding::{DeepXOnboardingExpected, decode_onboarding_inclusion},
        submission::find_extrinsic_by_hash,
    },
    signing::{
        DeepXDeleteSubaccountParams, DeepXInitializeSubaccountParams,
        DeepXPermittedPalletExtrinsic, DeepXRuntimeSnapshotService, RuntimeSnapshot,
        derive_signer_account_id, sign_delete_subaccount, sign_initialize_subaccount,
    },
};
use serde_json::json;

const FINALITY_TIMEOUT: Duration = Duration::from_secs(120);
const FINALITY_POLL_INTERVAL: Duration = Duration::from_secs(3);
const MAX_FINALIZED_BLOCKS: u64 = 256;
const DIRECTORY_TIMEOUT: Duration = Duration::from_secs(30);
const DIRECTORY_POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAX_ORDER_PAGES: usize = 100;

enum Action {
    Create(Vec<u8>),
    Delete([u8; 20]),
}

struct OnboardingContext<'a> {
    rpc: &'a DeepXRpcClient,
    rest: &'a DeepXRestTransport,
    key: &'a DeepXPrivateKey,
    owner: [u8; 20],
    wallet: &'a str,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args.next().context(
        "usage: deepx-sdk-onboarding <wallet> [--info SUBACCOUNT|--orders [MARKET_ID]|--create NAME|--delete SUBACCOUNT|--lifecycle NAME --submit] [--submit]",
    )?;
    let owner = decode_account_id20(&wallet).context("invalid wallet AccountId20")?;
    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let rest = DeepXRestTransport::new(Some(15), proxy.clone())?;
    let directory = rest
        .get_wallet_subaccounts(DEEPX_TESTNET_REST_URL, &wallet)
        .await?;
    println!("wallet={wallet} subaccounts={}", directory.addresses.len());
    for address in &directory.addresses {
        println!("subaccount={address}");
    }
    let Some(operation) = args.next() else {
        return Ok(());
    };
    if operation == "--info" {
        let address = args.next().context("--info requires a subaccount")?;
        ensure!(args.next().is_none(), "unexpected argument");
        ensure!(
            directory
                .addresses
                .iter()
                .any(|item| item.eq_ignore_ascii_case(&address)),
            "subaccount is not in the wallet directory"
        );
        let profile = rest
            .get_subaccount_info(DEEPX_TESTNET_REST_URL, &address)
            .await?;
        ensure!(
            decode_account_id20(&profile.authority) == Some(owner)
                && decode_account_id20(&profile.address) == decode_account_id20(&address),
            "subaccount profile conflicts with wallet directory"
        );
        println!(
            "name={} status={} next_order_id={} profile_height={}",
            profile.name, profile.status, profile.next_order_id, profile.height
        );
        return Ok(());
    }
    if operation == "--orders" {
        let selected_market = args.next().map(|value| value.parse::<u64>()).transpose()?;
        ensure!(args.next().is_none(), "unexpected argument");
        let market_ids = if let Some(market_id) = selected_market {
            vec![market_id]
        } else {
            let markets = rest.get_perp_markets(DEEPX_TESTNET_REST_URL).await?;
            let ids = markets
                .iter()
                .map(|market| market.id)
                .collect::<BTreeSet<_>>();
            ensure!(!ids.is_empty(), "perpetual market directory is empty");
            ensure!(
                ids.len() == markets.len(),
                "perpetual market directory has duplicate IDs"
            );
            ids.into_iter().collect()
        };
        let mut total = 0_usize;
        for market_id in market_ids {
            total = total
                .checked_add(show_wallet_open_orders(&rest, &directory, market_id).await?)
                .context("wallet open order count overflow")?;
        }
        println!("wallet={wallet} total_open_perp_orders={total}");
        return Ok(());
    }
    let lifecycle = operation == "--lifecycle";
    let action = match operation.as_str() {
        "--create" | "--lifecycle" => Action::Create(
            args.next()
                .context("creation requires a name")?
                .into_bytes(),
        ),
        "--delete" => Action::Delete(
            decode_account_id20(&args.next().context("--delete requires a subaccount")?)
                .context("invalid subaccount AccountId20")?,
        ),
        _ => anyhow::bail!("expected --create, --delete, or --lifecycle"),
    };
    let submit = match args.next().as_deref() {
        None => false,
        Some("--submit") => true,
        Some(_) => anyhow::bail!("expected --submit"),
    };
    ensure!(args.next().is_none(), "unexpected argument");
    ensure!(!lifecycle || submit, "--lifecycle requires --submit");
    if let Action::Delete(subaccount) = &action {
        ensure!(
            directory
                .addresses
                .iter()
                .any(|address| decode_account_id20(address) == Some(*subaccount)),
            "subaccount is not in the wallet directory"
        );
    }

    let key = DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
    ensure!(
        derive_signer_account_id(&key)? == owner,
        "onboarding requires the wallet owner's signing key"
    );
    let rpc = DeepXRpcClient::new_with_proxy(DEEPX_TESTNET_RPC_URL.to_string(), proxy.as_deref())?;
    ensure!(
        rpc.get_block_hash_required_once(0)
            .await?
            .eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
        "RPC endpoint is not the approved DeepX testnet"
    );
    let context = OnboardingContext {
        rpc: &rpc,
        rest: &rest,
        key: &key,
        owner,
        wallet: &wallet,
    };
    let result = run_action(&context, &action, submit, None).await?;
    if lifecycle {
        let (created, nonce) = result.context("lifecycle creation did not finalize")?;
        let deletion = Action::Delete(created);
        let (deleted, _) = run_action(&context, &deletion, true, Some(nonce))
            .await?
            .context("lifecycle deletion did not finalize")?;
        ensure!(deleted == created, "lifecycle deleted another subaccount");
    }
    Ok(())
}

async fn run_action(
    context: &OnboardingContext<'_>,
    action: &Action,
    submit: bool,
    previous_nonce: Option<u64>,
) -> anyhow::Result<Option<([u8; 20], u64)>> {
    let block_hash = context.rpc.get_best_block_hash_once().await?;
    let snapshot = read_runtime(context.rpc, &block_hash).await?;
    let permit = DeepXRuntimeSnapshotService::new(snapshot).acquire()?;
    let nonce = context.rpc.get_account_next_index(context.owner).await?;
    ensure!(
        previous_nonce.is_none_or(|previous| nonce > previous),
        "onboarding lifecycle did not acquire a fresh nonce"
    );
    let signed = match action {
        Action::Create(name) => sign_initialize_subaccount(
            &permit,
            context.key,
            DeepXInitializeSubaccountParams { name: name.clone() },
            nonce,
        )?,
        Action::Delete(subaccount) => sign_delete_subaccount(
            &permit,
            context.key,
            DeepXDeleteSubaccountParams {
                subaccount: *subaccount,
            },
            nonce,
        )?,
    };
    let dry_run: String = context
        .rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "system_dryRunAt",
            "params": [format!("0x{}", hex::encode(signed.bytes())), block_hash],
        }))
        .await?;
    ensure!(
        dry_run == "0x0000",
        "onboarding dry run was not accepted: {dry_run}"
    );
    println!(
        "onboarding dry run accepted: nonce={nonce} hash=0x{}",
        hex::encode(signed.extrinsic_hash())
    );
    if submit {
        let subaccount = submit_and_verify(context.rpc, &signed, context.owner, action).await?;
        println!("finalized subaccount=0x{}", hex::encode(subaccount));
        wait_for_directory(context.rest, context.wallet, subaccount, action).await?;
        return Ok(Some((subaccount, nonce)));
    }
    Ok(None)
}

async fn show_wallet_open_orders(
    rest: &DeepXRestTransport,
    directory: &DeepXWalletSubaccounts,
    market_id: u64,
) -> anyhow::Result<usize> {
    let mut count = 0_usize;
    for subaccount in &directory.addresses {
        let request = DeepXPerpOpenOrdersRequest {
            subaccount: subaccount.clone(),
            market_id: Some(market_id),
            is_long: None,
            cursor: None,
            page_size: Some(100),
            sort: DeepXAccountSortOrder::Descending,
        };
        let pages = rest
            .get_perp_open_order_pages(DEEPX_TESTNET_REST_URL, &request, MAX_ORDER_PAGES)
            .await?;
        for page in pages {
            for order in page.items {
                println!(
                    "subaccount={} order_id={} market_id={} is_long={} size={} price={} status={}",
                    subaccount,
                    order.order_id,
                    order.market_id,
                    order.is_long,
                    order.size,
                    order.price,
                    order.status
                );
                count = count.checked_add(1).context("open order count overflow")?;
            }
        }
    }
    let final_directory = rest
        .get_wallet_subaccounts(DEEPX_TESTNET_REST_URL, directory.wallet())
        .await?;
    ensure!(
        directory.has_same_membership(&final_directory.addresses),
        "wallet directory changed during open-order collection"
    );
    println!(
        "wallet={} market_id={market_id} open_perp_orders={count}",
        directory.wallet()
    );
    Ok(count)
}

async fn wait_for_directory(
    rest: &DeepXRestTransport,
    wallet: &str,
    subaccount: [u8; 20],
    action: &Action,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + DIRECTORY_TIMEOUT;
    loop {
        ensure!(
            Instant::now() < deadline,
            "finalized onboarding is not yet reflected in the wallet directory; reconcile before continuing"
        );
        let directory = rest
            .get_wallet_subaccounts(DEEPX_TESTNET_REST_URL, wallet)
            .await?;
        let present = directory
            .addresses
            .iter()
            .any(|address| decode_account_id20(address) == Some(subaccount));
        match action {
            Action::Create(name) if present => {
                let address = format!("0x{}", hex::encode(subaccount));
                let profile = rest
                    .get_subaccount_info(DEEPX_TESTNET_REST_URL, &address)
                    .await?;
                ensure!(
                    decode_account_id20(&profile.authority) == decode_account_id20(wallet)
                        && decode_account_id20(&profile.address) == Some(subaccount)
                        && profile.name.as_bytes() == name,
                    "created subaccount profile conflicts with finalized event"
                );
                println!(
                    "subaccount name={} status={} next_order_id={}",
                    profile.name, profile.status, profile.next_order_id
                );
                return Ok(());
            }
            Action::Delete(_) if !present => {
                println!("deleted subaccount is absent from the wallet directory");
                return Ok(());
            }
            _ => {}
        }
        tokio::time::sleep(DIRECTORY_POLL_INTERVAL).await;
    }
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
        decode_prefixed_hash(DEEPX_TESTNET_GENESIS_HASH)
            .context("invalid approved genesis hash")?,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )
    .map_err(Into::into)
}

async fn submit_and_verify(
    rpc: &DeepXRpcClient,
    signed: &DeepXPermittedPalletExtrinsic,
    owner: [u8; 20],
    action: &Action,
) -> anyhow::Result<[u8; 20]> {
    let expected_hash = format!("0x{}", hex::encode(signed.extrinsic_hash()));
    let encoded = format!("0x{}", hex::encode(signed.bytes()));
    ensure!(
        rpc.get_account_next_index(owner).await? == signed.nonce(),
        "nonce changed before onboarding submission"
    );
    let current_hash = rpc.get_best_block_hash_once().await?;
    ensure!(
        read_runtime(rpc, &current_hash).await?.identity() == signed.runtime(),
        "runtime changed after onboarding signing"
    );
    ensure!(
        !rpc.get_pending_extrinsics()
            .await?
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&encoded)),
        "onboarding extrinsic is already pending"
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
            format!("onboarding submission uncertain; reconcile hash={expected_hash}")
        })?;
    ensure!(
        submitted_hash.eq_ignore_ascii_case(&expected_hash),
        "submission returned another hash; reconcile expected hash={expected_hash}"
    );
    println!("onboarding submitted: hash={expected_hash}; awaiting finality");
    let deadline = Instant::now() + FINALITY_TIMEOUT;
    loop {
        ensure!(
            Instant::now() < deadline,
            "onboarding finality timed out; reconcile hash={expected_hash}"
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
            "onboarding finality scan exceeded its block limit; reconcile hash={expected_hash}"
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
            let expected = match action {
                Action::Create(name) => DeepXOnboardingExpected::Create { owner, name },
                Action::Delete(subaccount) => DeepXOnboardingExpected::Delete {
                    subaccount: *subaccount,
                },
            };
            let evidence = decode_onboarding_inclusion(
                runtime.interfaces(),
                runtime.metadata(),
                index,
                expected,
                &events,
            )?;
            ensure!(
                rpc.get_block_hash_required_once(height)
                    .await?
                    .eq_ignore_ascii_case(&block_hash),
                "finalized block changed; reconcile hash={expected_hash}"
            );
            ensure!(
                evidence.outcome == DeepXSystemDispatchOutcome::Success,
                "onboarding dispatch failed: hash={expected_hash}"
            );
            println!("onboarding finalized: block={height} index={index} hash={expected_hash}");
            return evidence
                .subaccount
                .context("successful onboarding emitted no subaccount");
        }
        next_height = finalized_height
            .checked_add(1)
            .context("finalized block number overflow")?;
        tokio::time::sleep(FINALITY_POLL_INTERVAL).await;
    }
}
