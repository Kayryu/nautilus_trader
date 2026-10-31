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

//! Lists wallet delegates and optionally runs owner-signed management operations.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, ensure};
use deepx_rust_sdk::{
    credential::{DeepXKeyScheme, DeepXPrivateKey},
    deployment::{DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL},
    enums::DeepXEnvironment,
    identity::decode_account_id20,
    rest::{DeepXRestError, DeepXRestTransport, models::DeepXDelegateMode},
    rpc::{
        DeepXRpcBlockHeader, DeepXRpcBlockResponse, DeepXRpcClient, DeepXRpcRuntimeVersion,
        chain_decode::{decode_block_number_u64, decode_prefixed_hash},
        event_decode::{
            DeepXDelegateAccountExpected, DeepXDelegateModeExpected, DeepXFinalizedEventLocation,
            verify_finalized_delegate_account_change_events,
            verify_finalized_delegate_mode_change_events,
        },
        event_storage::get_system_events,
        events::DeepXDelegateAccountChangeKind,
        get_metadata, get_runtime_version,
        submission::find_extrinsic_by_hash,
    },
    signing::{
        DeepXPermittedPalletExtrinsic, DeepXRemoveDelegateAccountParams,
        DeepXRuntimeSnapshotService, DeepXSetDelegateAccountParams, DeepXUpdateDelegateModeParams,
        DeepXWritableDelegateMode, RuntimeSnapshot, derive_signer_account_id,
        sign_remove_delegate_account, sign_set_delegate_account, sign_update_delegate_mode,
    },
};
use serde_json::json;

const FINALITY_TIMEOUT: Duration = Duration::from_secs(120);
const FINALITY_POLL_INTERVAL: Duration = Duration::from_secs(3);
const DIRECTORY_TIMEOUT: Duration = Duration::from_secs(30);
const DIRECTORY_POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAX_FINALIZED_BLOCKS: u64 = 256;

enum DelegateAction {
    Set { name: String, valid_until: u64 },
    Mode(DeepXWritableDelegateMode),
    Remove,
}

struct DelegateContext<'a> {
    rpc: &'a DeepXRpcClient,
    rest: &'a DeepXRestTransport,
    key: &'a DeepXPrivateKey,
    owner: [u8; 20],
    wallet: &'a str,
    delegate: [u8; 20],
    submit: bool,
}

impl DelegateAction {
    fn name(&self) -> &'static str {
        match self {
            Self::Set { .. } => "set",
            Self::Mode(DeepXWritableDelegateMode::Disable) => "disable",
            Self::Mode(DeepXWritableDelegateMode::PlaceOrCancelOrder) => "enable",
            Self::Remove => "remove",
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args.next().context(
        "usage: deepx-sdk-delegate <wallet> [<delegate> <--set NAME|--disable|--enable|--remove|--lifecycle NAME> [--submit]]",
    )?;
    let owner = decode_account_id20(&wallet).context("invalid wallet AccountId20")?;
    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let rest = DeepXRestTransport::new(Some(15), proxy.clone())?;
    show_delegates(&rest, &wallet).await?;
    let Some(delegate_address) = args.next() else {
        return Ok(());
    };
    let delegate =
        decode_account_id20(&delegate_address).context("invalid delegate AccountId20")?;
    let operation = args.next().context("missing delegate operation")?;
    let mut lifecycle = false;
    let actions = match operation.as_str() {
        "--set" => vec![DelegateAction::Set {
            name: args.next().context("--set requires a name")?,
            valid_until: expiry_in_one_day()?,
        }],
        "--disable" => vec![DelegateAction::Mode(DeepXWritableDelegateMode::Disable)],
        "--enable" => vec![DelegateAction::Mode(
            DeepXWritableDelegateMode::PlaceOrCancelOrder,
        )],
        "--remove" => vec![DelegateAction::Remove],
        "--lifecycle" => {
            lifecycle = true;
            vec![
                DelegateAction::Set {
                    name: args.next().context("--lifecycle requires a name")?,
                    valid_until: expiry_in_one_day()?,
                },
                DelegateAction::Mode(DeepXWritableDelegateMode::Disable),
                DelegateAction::Mode(DeepXWritableDelegateMode::PlaceOrCancelOrder),
                DelegateAction::Remove,
            ]
        }
        _ => anyhow::bail!("unknown delegate operation"),
    };
    let submit = match args.next().as_deref() {
        None if !lifecycle => false,
        Some("--submit") => true,
        _ => anyhow::bail!("expected --submit; lifecycle requires explicit submission"),
    };
    ensure!(args.next().is_none(), "unexpected argument");

    if lifecycle {
        let directory = rest
            .get_delegate_accounts(DEEPX_TESTNET_REST_URL, &wallet)
            .await?;
        ensure!(
            !directory.accounts.iter().any(|account| {
                decode_account_id20(&account.delegate_address) == Some(delegate)
            }),
            "lifecycle target already exists; use individual operations to manage it"
        );
    }
    let key = DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
    ensure!(
        derive_signer_account_id(&key)? == owner,
        "delegate management requires the wallet owner's signing key"
    );
    let rpc = DeepXRpcClient::new_with_proxy(DEEPX_TESTNET_RPC_URL.to_string(), proxy.as_deref())?;
    ensure!(
        rpc.get_block_hash_required_once(0)
            .await?
            .eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
        "RPC endpoint is not the approved DeepX testnet"
    );
    let context = DelegateContext {
        rpc: &rpc,
        rest: &rest,
        key: &key,
        owner,
        wallet: &wallet,
        delegate,
        submit,
    };
    for action in &actions {
        run_action(&context, action).await?;
    }
    Ok(())
}

fn expiry_in_one_day() -> anyhow::Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    u64::try_from(now)
        .context("current timestamp exceeds u64")?
        .checked_add(86_400_000)
        .context("delegate expiry overflows u64")
}

async fn show_delegates(rest: &DeepXRestTransport, wallet: &str) -> anyhow::Result<()> {
    let directory = rest
        .get_delegate_accounts(DEEPX_TESTNET_REST_URL, wallet)
        .await?;
    println!("wallet={wallet} delegates={}", directory.accounts.len());
    for account in directory.accounts {
        println!(
            "delegate={} name={} mode={} active={} valid_until={}",
            account.delegate_address,
            account.delegate_name,
            account.mode.as_str(),
            account.active,
            account.valid_until,
        );
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

async fn run_action(context: &DelegateContext<'_>, action: &DelegateAction) -> anyhow::Result<()> {
    let block_hash = context.rpc.get_best_block_hash_once().await?;
    let snapshot = read_runtime(context.rpc, &block_hash).await?;
    let service = DeepXRuntimeSnapshotService::new(snapshot);
    let permit = service.acquire()?;
    let nonce = context.rpc.get_account_next_index(context.owner).await?;
    let signed = match action {
        DelegateAction::Set { name, valid_until } => sign_set_delegate_account(
            &permit,
            context.key,
            DeepXSetDelegateAccountParams {
                delegate: context.delegate,
                name: name.as_bytes().to_vec(),
                valid_until: *valid_until,
            },
            nonce,
        )?,
        DelegateAction::Mode(new_mode) => sign_update_delegate_mode(
            &permit,
            context.key,
            DeepXUpdateDelegateModeParams {
                address: context.delegate,
                new_mode: *new_mode,
            },
            nonce,
        )?,
        DelegateAction::Remove => sign_remove_delegate_account(
            &permit,
            context.key,
            DeepXRemoveDelegateAccountParams {
                delegate: context.delegate,
            },
            nonce,
        )?,
    };
    let dry_run: String = context
        .rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "system_dryRunAt",
            "params": [format!("0x{}", hex::encode(signed.bytes())), block_hash],
        }))
        .await?;
    ensure!(
        dry_run == "0x0000",
        "delegate {} dry run was not accepted: {dry_run}",
        action.name()
    );
    println!(
        "delegate {} dry run accepted: nonce={nonce} hash=0x{}",
        action.name(),
        hex::encode(signed.extrinsic_hash())
    );
    if context.submit {
        submit_and_verify(
            context.rpc,
            &signed,
            context.owner,
            context.delegate,
            action,
        )
        .await?;
        wait_for_directory(context.rest, context.wallet, context.delegate, action).await?;
    }
    Ok(())
}

async fn submit_and_verify(
    rpc: &DeepXRpcClient,
    signed: &DeepXPermittedPalletExtrinsic,
    owner: [u8; 20],
    delegate: [u8; 20],
    action: &DelegateAction,
) -> anyhow::Result<()> {
    let expected_hash = format!("0x{}", hex::encode(signed.extrinsic_hash()));
    let encoded = format!("0x{}", hex::encode(signed.bytes()));
    ensure!(
        rpc.get_account_next_index(owner).await? == signed.nonce(),
        "sequential nonce changed before delegate submission"
    );
    let current_hash = rpc.get_best_block_hash_once().await?;
    ensure!(
        read_runtime(rpc, &current_hash).await?.identity() == signed.runtime(),
        "runtime changed after delegate signing"
    );
    ensure!(
        !rpc.get_pending_extrinsics()
            .await?
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&encoded)),
        "delegate extrinsic is already pending"
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
            format!(
                "delegate {} submission uncertain; reconcile hash={expected_hash}",
                action.name()
            )
        })?;
    ensure!(
        submitted_hash.eq_ignore_ascii_case(&expected_hash),
        "submission returned another hash; reconcile expected hash={expected_hash}"
    );
    println!(
        "delegate {} submitted: hash={expected_hash}; awaiting finality",
        action.name()
    );
    let deadline = Instant::now() + FINALITY_TIMEOUT;
    loop {
        ensure!(
            Instant::now() < deadline,
            "delegate {} finality timed out; reconcile hash={expected_hash}",
            action.name()
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
            "delegate finality scan exceeded its block limit; reconcile hash={expected_hash}"
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
            let location = DeepXFinalizedEventLocation {
                block_hash: decode_prefixed_hash(&block_hash).context("invalid block hash")?,
                block_number: height,
                extrinsic_index: index,
            };
            match action {
                DelegateAction::Set { .. } | DelegateAction::Remove => {
                    let kind = if matches!(action, DelegateAction::Remove) {
                        DeepXDelegateAccountChangeKind::Removed
                    } else {
                        DeepXDelegateAccountChangeKind::Set
                    };
                    verify_finalized_delegate_account_change_events(
                        runtime.interfaces(),
                        runtime.metadata(),
                        location,
                        &events,
                        DeepXDelegateAccountExpected {
                            owner,
                            delegate,
                            kind,
                        },
                    )?;
                }
                DelegateAction::Mode(new_mode) => {
                    let mode = match new_mode {
                        DeepXWritableDelegateMode::PlaceOrCancelOrder => {
                            DeepXDelegateMode::PlaceOrCancelOrder
                        }
                        DeepXWritableDelegateMode::Disable => DeepXDelegateMode::Disable,
                    };
                    verify_finalized_delegate_mode_change_events(
                        runtime.interfaces(),
                        runtime.metadata(),
                        location,
                        &events,
                        DeepXDelegateModeExpected {
                            owner,
                            delegate,
                            mode,
                        },
                    )?;
                }
            }
            ensure!(
                rpc.get_block_hash_required_once(height)
                    .await?
                    .eq_ignore_ascii_case(&block_hash),
                "finalized block changed; reconcile hash={expected_hash}"
            );
            println!(
                "delegate {} finalized: block={height} index={index} hash={expected_hash}",
                action.name()
            );
            return Ok(());
        }
        next_height = finalized_height
            .checked_add(1)
            .context("finalized block number overflow")?;
        tokio::time::sleep(FINALITY_POLL_INTERVAL).await;
    }
}

async fn wait_for_directory(
    rest: &DeepXRestTransport,
    wallet: &str,
    delegate: [u8; 20],
    action: &DelegateAction,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + DIRECTORY_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "delegate {} finalized but wallet directory did not converge; reconcile before continuing",
            action.name()
        );
        let directory = match tokio::time::timeout(
            remaining,
            rest.get_delegate_accounts(DEEPX_TESTNET_REST_URL, wallet),
        )
        .await
        {
            Ok(Ok(directory)) => Some(directory),
            Ok(Err(
                DeepXRestError::Transport(_)
                | DeepXRestError::Http {
                    status: 429 | 500..=599,
                    ..
                },
            )) => None,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => None,
        };
        let record = directory.as_ref().and_then(|directory| {
            directory
                .accounts
                .iter()
                .find(|record| decode_account_id20(&record.delegate_address) == Some(delegate))
        });
        let matched = match (action, record) {
            (DelegateAction::Set { name, valid_until }, Some(record)) => {
                record.delegate_name == *name && record.valid_until == *valid_until
            }
            (DelegateAction::Mode(DeepXWritableDelegateMode::Disable), Some(record)) => {
                record.mode == DeepXDelegateMode::Disable
            }
            (DelegateAction::Mode(DeepXWritableDelegateMode::PlaceOrCancelOrder), Some(record)) => {
                record.mode == DeepXDelegateMode::PlaceOrCancelOrder && record.active
            }
            (DelegateAction::Remove, None) if directory.is_some() => true,
            _ => false,
        };
        if matched {
            println!("delegate {} reflected in wallet directory", action.name());
            return Ok(());
        }
        tokio::time::sleep(
            DIRECTORY_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
        )
        .await;
    }
}
