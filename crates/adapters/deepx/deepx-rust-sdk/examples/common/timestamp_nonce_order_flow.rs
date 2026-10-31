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

//! Signs a timestamp-nonce order and optionally places then cancels it on testnet.

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
        event_decode::{
            DeepXPerpCancelExpected, DeepXPerpCancelInclusionOutcome, decode_perp_cancel_inclusion,
        },
        event_storage::get_system_events,
        get_metadata, get_runtime_version,
        perp_modify::{
            DeepXPerpModifyExpected, DeepXPerpModifyInclusionOutcome, decode_perp_modify_inclusion,
        },
        perp_place::{
            DeepXPerpPlaceExpected, DeepXPerpPlaceInclusionOutcome, decode_perp_place_inclusion,
        },
        spot_cancel::{
            DeepXSpotCancelExpected, DeepXSpotCancelInclusionOutcome, decode_spot_cancel_inclusion,
        },
        spot_modify::{
            DeepXSpotModifyExpected, DeepXSpotModifyInclusionOutcome, decode_spot_modify_inclusion,
        },
        spot_place::{
            DeepXSpotPlaceExpected, DeepXSpotPlaceInclusionOutcome, decode_spot_place_inclusion,
        },
        storage::{
            DeepXPerpOrderStorage, DeepXSpotOrderStorage, DeepXStoredOrderStatus,
            DeepXStoredPerpOrderType, DeepXStoredPostOnlyParam, DeepXStoredSpotOrderType,
            DeepXStoredTimeInForce,
        },
        storage_decode::{get_perp_orders_by_id_at_block, get_spot_orders_by_id_at_block},
        submission::{
            DeepXSignedExtrinsicDecodeError, decode_signed_extrinsic_identity,
            find_extrinsic_by_hash,
        },
        timestamp::{decode_timestamp_set_inherent, get_timestamp_now},
    },
    signing::{
        DeepXPermittedPalletExtrinsic, DeepXPerpCancelParams, DeepXPerpModifyParams,
        DeepXPerpOrderType, DeepXPerpPlaceParams, DeepXPostOnlyParam, DeepXRuntimeSnapshotService,
        DeepXSpotCancelParams, DeepXSpotModifyParams, DeepXSpotOrderType, DeepXSpotPlaceParams,
        DeepXTimeInForce, RuntimeSnapshot, sign_perp_cancel, sign_perp_modify_order,
        sign_perp_place_order, sign_spot_cancel, sign_spot_modify_order, sign_spot_place_order,
    },
};
use serde_json::json;

const MAX_CLOCK_DRIFT_MS: u64 = 10_000;
const FINALITY_TIMEOUT: Duration = Duration::from_secs(120);
const FINALITY_POLL_INTERVAL: Duration = Duration::from_secs(3);
const MAX_FINALIZED_BLOCKS: u64 = 256;

#[derive(Clone, Copy)]
enum Market {
    Perp(DeepXPerpPlaceParams),
    Spot(DeepXSpotPlaceParams),
}

impl Market {
    fn subaccount(self) -> [u8; 20] {
        match self {
            Self::Perp(params) => params.subaccount,
            Self::Spot(params) => params.subaccount,
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    PerpPlace(DeepXPerpPlaceParams),
    PerpCancel(DeepXPerpCancelParams),
    PerpModify(DeepXPerpModifyParams),
    SpotPlace(DeepXSpotPlaceParams),
    SpotCancel(DeepXSpotCancelParams),
    SpotModify(DeepXSpotModifyParams),
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::PerpPlace(_) | Self::SpotPlace(_) => "place",
            Self::PerpCancel(_) | Self::SpotCancel(_) => "cancel",
            Self::PerpModify(_) | Self::SpotModify(_) => "modify",
        }
    }
}

pub(crate) async fn run(mode: Option<&str>) -> anyhow::Result<()> {
    let arguments = route_arguments(mode, std::env::args().skip(1))?;
    let mut args = arguments.into_iter();
    let kind = args.next().context("usage: deepx-sdk-timestamp-nonce-orders <perp|spot> <subaccount> <market-id|pair> <long|short|buy|sell> <size|quote> <price|base> [--ioc] [--nonce-ms N | --cancel-existing ORDER_ID | --modify-existing ORDER_ID NEW_AMOUNT NEW_PRICE_OR_BASE] [--submit]")?;
    let subaccount = args.next().context("missing subaccount")?;
    let subaccount = decode_account_id20(&subaccount).context("invalid subaccount AccountId20")?;
    let identity = args.next().context("missing market ID or pair")?;
    let side = args.next().context("missing order side")?;
    let first_amount: u128 = args.next().context("missing first raw amount")?.parse()?;
    let second_amount: u128 = args.next().context("missing second raw amount")?.parse()?;
    let mut option = args.next();
    let ioc = option.as_deref() == Some("--ioc");
    if ioc {
        option = args.next();
    }
    let (requested_nonce, recovery_order_id, modification, submit) = match option.as_deref() {
        None => (None, None, None, false),
        Some("--submit") => (None, None, None, true),
        Some("--nonce-ms") => {
            let nonce = args.next().context("missing timestamp nonce")?.parse()?;
            let submit = match args.next().as_deref() {
                None => false,
                Some("--submit") => true,
                Some(_) => anyhow::bail!("expected --submit after timestamp nonce"),
            };
            (Some(nonce), None, None, submit)
        }
        Some("--cancel-existing") => {
            let order_id = args
                .next()
                .context("missing existing order ID")?
                .parse::<u64>()?;
            ensure!(order_id > 0, "existing order ID must be positive");
            let submit = match args.next().as_deref() {
                None => false,
                Some("--submit") => true,
                Some(_) => anyhow::bail!("expected --submit after existing order ID"),
            };
            (None, Some(order_id), None, submit)
        }
        Some("--modify-existing") => {
            let order_id = args
                .next()
                .context("missing existing order ID")?
                .parse::<u64>()?;
            let first = args
                .next()
                .context("missing replacement size or quote amount")?
                .parse()?;
            let second = args
                .next()
                .context("missing replacement price or base amount")?
                .parse()?;
            ensure!(order_id > 0, "existing order ID must be positive");
            let submit = match args.next().as_deref() {
                None => false,
                Some("--submit") => true,
                Some(_) => anyhow::bail!("expected --submit after replacement amounts"),
            };
            (None, None, Some((order_id, first, second)), submit)
        }
        Some(_) => {
            anyhow::bail!("expected --nonce-ms, --cancel-existing, --modify-existing, or --submit")
        }
    };
    ensure!(args.next().is_none(), "unexpected argument");
    ensure!(
        !ioc || (recovery_order_id.is_none() && modification.is_none()),
        "IOC orders cannot use open-order cancellation or modification"
    );
    let market = parse_market(
        &kind,
        subaccount,
        &identity,
        &side,
        first_amount,
        second_amount,
        ioc,
    )?;

    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let rpc = DeepXRpcClient::new_with_proxy(DEEPX_TESTNET_RPC_URL.to_string(), proxy.as_deref())?;
    ensure!(
        rpc.get_block_hash_required_once(0)
            .await?
            .eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
        "RPC endpoint is not the approved DeepX testnet"
    );
    let key = DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;

    if let Some((order_id, first, second)) = modification {
        match market {
            Market::Perp(old) => {
                ensure!(second > 0, "replacement perpetual price must be positive");
                let replacement = DeepXPerpPlaceParams {
                    size: first,
                    price: second,
                    ..old
                };
                modify_perp_order(&rpc, &key, old, replacement, order_id, submit).await?;
            }
            Market::Spot(old) => {
                let Market::Spot(replacement) =
                    parse_market(&kind, subaccount, &identity, &side, first, second, false)?
                else {
                    unreachable!("the original market is Spot");
                };
                modify_spot_order(&rpc, &key, old, replacement, order_id, submit).await?;
            }
        }
        return Ok(());
    }

    if let Some(order_id) = recovery_order_id {
        cancel_order(&rpc, &key, market, order_id, 0, submit).await?;
        return Ok(());
    }
    let (suggested_nonce, block_hash) = observe_nonce(&rpc, 0).await?;
    let nonce = requested_nonce.unwrap_or(suggested_nonce);
    ensure!(
        nonce >= suggested_nonce,
        "requested timestamp nonce precedes current chain time"
    );
    let snapshot = read_runtime(&rpc, &block_hash).await?;
    let service = DeepXRuntimeSnapshotService::new(snapshot);
    let permit = service.acquire()?;
    let (signed, operation) = match market {
        Market::Perp(params) => (
            sign_perp_place_order(&permit, &key, params, nonce)?,
            Operation::PerpPlace(params),
        ),
        Market::Spot(params) => (
            sign_spot_place_order(&permit, &key, params, nonce)?,
            Operation::SpotPlace(params),
        ),
    };
    dry_run(&rpc, &signed, &block_hash).await?;
    println!(
        "order dry run accepted: order_id={nonce} hash=0x{}",
        hex::encode(signed.extrinsic_hash())
    );

    if !submit {
        return Ok(());
    }
    let (_, placed_block) = submit_and_verify(&rpc, &signed, operation).await?;
    if ioc {
        verify_ioc_order_state(&rpc, market, nonce, &placed_block).await?;
        return Ok(());
    }
    verify_order_state(&rpc, market, nonce, &placed_block, true).await?;

    cancel_order(&rpc, &key, market, nonce, 0, true).await?;
    println!("post-only order placed and canceled: order_id={nonce}");
    Ok(())
}

fn route_arguments(
    mode: Option<&str>,
    arguments: impl Iterator<Item = String>,
) -> anyhow::Result<Vec<String>> {
    let mut arguments: Vec<String> = arguments.collect();
    let inserted = match mode {
        None => return Ok(arguments),
        Some("--ioc") => "--ioc",
        Some("--modify-existing") => "--modify-existing",
        Some(_) => anyhow::bail!("unsupported order example mode"),
    };
    ensure!(arguments.len() >= 6, "missing order arguments");
    ensure!(
        !arguments.iter().any(|argument| argument == inserted),
        "{inserted} is implicit in this example"
    );
    arguments.insert(6, inserted.to_string());
    Ok(arguments)
}

async fn modify_perp_order(
    rpc: &DeepXRpcClient,
    key: &DeepXPrivateKey,
    old: DeepXPerpPlaceParams,
    mut replacement: DeepXPerpPlaceParams,
    old_order_id: u64,
    submit: bool,
) -> anyhow::Result<()> {
    let finalized = rpc.get_finalized_head_once().await?;
    let hash = decode_prefixed_hash(&finalized).context("invalid finalized block hash")?;
    let orders = get_perp_orders_by_id_at_block(rpc, hash, old.subaccount, &[old_order_id]).await?;
    let order = orders
        .first()
        .context("existing perpetual order is missing")?;
    ensure!(
        matches_perp_order(order, old, old_order_id) && order.status.is_active(),
        "existing perpetual order identity or active state differs from supplied terms"
    );
    let filled_size = order.size_filled;
    let target_size = replacement.size;
    let Some(remaining_size) = replacement_size(target_size, filled_size)? else {
        cancel_order(
            rpc,
            key,
            Market::Perp(old),
            old_order_id,
            filled_size,
            submit,
        )
        .await?;
        return Ok(());
    };
    replacement.size = remaining_size;
    let minimum = old_order_id.checked_add(1).context("order ID overflow")?;
    let (new_order_id, block_hash) = observe_nonce(rpc, minimum).await?;
    let service = DeepXRuntimeSnapshotService::new(read_runtime(rpc, &block_hash).await?);
    let permit = service.acquire()?;
    let params = DeepXPerpModifyParams {
        cancel: DeepXPerpCancelParams {
            subaccount: old.subaccount,
            order_id: old_order_id,
            market_id: old.market_id,
            fast_cancel: false,
        },
        place: replacement,
    };
    let signed = sign_perp_modify_order(&permit, key, params, new_order_id)?;
    dry_run(rpc, &signed, &block_hash).await?;
    println!(
        "perpetual modify dry run accepted: old_order_id={old_order_id} filled_raw={filled_size} new_order_id={new_order_id} replacement_size_raw={remaining_size} hash=0x{}",
        hex::encode(signed.extrinsic_hash())
    );

    if !submit {
        return Ok(());
    }
    let (_, modified_block) =
        submit_and_verify(rpc, &signed, Operation::PerpModify(params)).await?;
    verify_perp_order_fill_state(
        rpc,
        old,
        old_order_id,
        &modified_block,
        DeepXStoredOrderStatus::Canceled,
        filled_size,
    )
    .await?;
    verify_order_state(
        rpc,
        Market::Perp(replacement),
        new_order_id,
        &modified_block,
        true,
    )
    .await?;
    cancel_order(rpc, key, Market::Perp(replacement), new_order_id, 0, true).await?;
    println!(
        "perpetual order modified and replacement canceled: old_order_id={old_order_id} new_order_id={new_order_id}"
    );
    Ok(())
}

fn replacement_size(target_size: u128, filled_size: u128) -> anyhow::Result<Option<u128>> {
    let remaining = target_size
        .checked_sub(filled_size)
        .context("new total quantity is below the already-filled size")?;
    Ok((remaining != 0).then_some(remaining))
}

async fn modify_spot_order(
    rpc: &DeepXRpcClient,
    key: &DeepXPrivateKey,
    old: DeepXSpotPlaceParams,
    replacement: DeepXSpotPlaceParams,
    old_order_id: u64,
    submit: bool,
) -> anyhow::Result<()> {
    let finalized = rpc.get_finalized_head_once().await?;
    verify_order_state(rpc, Market::Spot(old), old_order_id, &finalized, true).await?;
    let minimum = old_order_id.checked_add(1).context("order ID overflow")?;
    let (new_order_id, block_hash) = observe_nonce(rpc, minimum).await?;
    let service = DeepXRuntimeSnapshotService::new(read_runtime(rpc, &block_hash).await?);
    let permit = service.acquire()?;
    let params = spot_modify_params(old, replacement, old_order_id)?;
    let signed = sign_spot_modify_order(&permit, key, params, new_order_id)?;
    dry_run(rpc, &signed, &block_hash).await?;
    println!(
        "Spot modify dry run accepted: old_order_id={old_order_id} new_order_id={new_order_id} hash=0x{}",
        hex::encode(signed.extrinsic_hash())
    );

    if !submit {
        return Ok(());
    }
    let (_, modified_block) =
        submit_and_verify(rpc, &signed, Operation::SpotModify(params)).await?;
    verify_order_state(rpc, Market::Spot(old), old_order_id, &modified_block, false).await?;
    verify_order_state(
        rpc,
        Market::Spot(replacement),
        new_order_id,
        &modified_block,
        true,
    )
    .await?;
    cancel_order(rpc, key, Market::Spot(replacement), new_order_id, 0, true).await?;
    println!(
        "Spot order modified and replacement canceled: old_order_id={old_order_id} new_order_id={new_order_id}"
    );
    Ok(())
}

fn spot_modify_params(
    old: DeepXSpotPlaceParams,
    replacement: DeepXSpotPlaceParams,
    old_order_id: u64,
) -> anyhow::Result<DeepXSpotModifyParams> {
    ensure!(
        old.subaccount == replacement.subaccount
            && old.pair == replacement.pair
            && old.is_buy == replacement.is_buy,
        "Spot modify legs must use the same subaccount, pair, and side"
    );
    Ok(DeepXSpotModifyParams {
        cancel: DeepXSpotCancelParams {
            subaccount: old.subaccount,
            pair: old.pair,
            order_id: old_order_id,
            is_buy: old.is_buy,
            fast_cancel: false,
        },
        place: replacement,
    })
}

async fn cancel_order(
    rpc: &DeepXRpcClient,
    key: &DeepXPrivateKey,
    market: Market,
    order_id: u64,
    expected_filled: u128,
    submit: bool,
) -> anyhow::Result<()> {
    let finalized = rpc.get_finalized_head_once().await?;
    if expected_filled == 0 {
        verify_order_state(rpc, market, order_id, &finalized, true).await?;
    } else {
        let Market::Perp(params) = market else {
            anyhow::bail!("Spot cancellation cannot use perpetual fill evidence");
        };
        verify_perp_order_fill_state(
            rpc,
            params,
            order_id,
            &finalized,
            DeepXStoredOrderStatus::PartiallyFilled,
            expected_filled,
        )
        .await?;
    }
    let minimum = order_id.checked_add(1).context("order ID overflow")?;
    let (cancel_nonce, cancel_block) = observe_nonce(rpc, minimum).await?;
    let cancel_snapshot = read_runtime(rpc, &cancel_block).await?;
    let cancel_service = DeepXRuntimeSnapshotService::new(cancel_snapshot);
    let cancel_permit = cancel_service.acquire()?;
    let (cancel, cancel_operation) = match market {
        Market::Perp(params) => {
            let cancel_params = DeepXPerpCancelParams {
                subaccount: params.subaccount,
                order_id,
                market_id: params.market_id,
                fast_cancel: false,
            };
            (
                sign_perp_cancel(&cancel_permit, key, cancel_params, cancel_nonce)?,
                Operation::PerpCancel(cancel_params),
            )
        }
        Market::Spot(params) => {
            let cancel_params = DeepXSpotCancelParams {
                subaccount: params.subaccount,
                pair: params.pair,
                order_id,
                is_buy: params.is_buy,
                fast_cancel: false,
            };
            (
                sign_spot_cancel(&cancel_permit, key, cancel_params, cancel_nonce)?,
                Operation::SpotCancel(cancel_params),
            )
        }
    };
    dry_run(rpc, &cancel, &cancel_block)
        .await
        .with_context(|| format!("order {order_id} may remain open; reconcile before retry"))?;
    println!(
        "existing order cancel dry run accepted: order_id={order_id} hash=0x{}",
        hex::encode(cancel.extrinsic_hash())
    );

    if !submit {
        return Ok(());
    }
    let (_, canceled_block) = submit_and_verify(rpc, &cancel, cancel_operation).await?;
    if expected_filled == 0 {
        verify_order_state(rpc, market, order_id, &canceled_block, false).await?;
    } else {
        let Market::Perp(params) = market else {
            unreachable!("Spot cancellation rejected perpetual fill evidence");
        };
        verify_perp_order_fill_state(
            rpc,
            params,
            order_id,
            &canceled_block,
            DeepXStoredOrderStatus::Canceled,
            expected_filled,
        )
        .await?;
    }
    println!("post-only order canceled: order_id={order_id}");
    Ok(())
}

fn parse_market(
    kind: &str,
    subaccount: [u8; 20],
    identity: &str,
    side: &str,
    first_amount: u128,
    second_amount: u128,
    ioc: bool,
) -> anyhow::Result<Market> {
    ensure!(
        first_amount > 0 && second_amount > 0,
        "order amounts must be positive"
    );
    let time_in_force = if ioc {
        DeepXTimeInForce::Ioc
    } else {
        DeepXTimeInForce::Gtc
    };
    let post_only = if ioc {
        DeepXPostOnlyParam::None
    } else {
        DeepXPostOnlyParam::MustPostOnly
    };
    Ok(match kind {
        "perp" => Market::Perp(DeepXPerpPlaceParams {
            subaccount,
            market_id: identity.parse()?,
            is_long: match side {
                "long" => true,
                "short" => false,
                _ => anyhow::bail!("perpetual side must be long or short"),
            },
            size: first_amount,
            price: second_amount,
            order_type: DeepXPerpOrderType::Limit(time_in_force),
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only,
        }),
        "spot" => {
            let pair: [u8; 32] =
                hex::decode(identity.strip_prefix("0x").context("invalid pair hex")?)?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Spot pair must contain 32 bytes"))?;
            let mut quote_amount = [0_u8; 32];
            quote_amount[..16].copy_from_slice(&first_amount.to_le_bytes());
            let mut base_amount = [0_u8; 32];
            base_amount[..16].copy_from_slice(&second_amount.to_le_bytes());
            Market::Spot(DeepXSpotPlaceParams {
                subaccount,
                pair,
                is_buy: match side {
                    "buy" => true,
                    "sell" => false,
                    _ => anyhow::bail!("Spot side must be buy or sell"),
                },
                quote_amount,
                base_amount,
                order_type: DeepXSpotOrderType::Limit(time_in_force),
                post_only,
                reduce_only: false,
            })
        }
        _ => anyhow::bail!("market must be perp or spot"),
    })
}

async fn observe_nonce(rpc: &DeepXRpcClient, minimum: u64) -> anyhow::Result<(u64, String)> {
    let block_hash = rpc.get_best_block_hash_once().await?;
    let block: DeepXRpcBlockResponse = rpc.get_block(&block_hash).await?;
    let block_number = decode_block_number_u64(&block.block.header.number)
        .context("invalid current block number")?;
    let block_id = decode_prefixed_hash(&block_hash).context("invalid current block hash")?;
    let storage_ms = get_timestamp_now(rpc, block_id).await?;
    ensure!(
        decode_timestamp_set_inherent(&block.block.extrinsics) == Some(storage_ms),
        "current timestamp storage and block inherent disagree"
    );
    ensure!(
        rpc.get_block_hash_required_once(block_number)
            .await?
            .eq_ignore_ascii_case(&block_hash),
        "current block is no longer canonical"
    );
    let local_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    ensure!(
        local_ms.abs_diff(storage_ms) <= MAX_CLOCK_DRIFT_MS,
        "local clock differs too far from the DeepX chain"
    );
    Ok((local_ms.max(storage_ms).max(minimum), block_hash))
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

async fn dry_run(
    rpc: &DeepXRpcClient,
    signed: &DeepXPermittedPalletExtrinsic,
    block_hash: &str,
) -> anyhow::Result<()> {
    let result: String = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "system_dryRunAt",
            "params": [format!("0x{}", hex::encode(signed.bytes())), block_hash],
        }))
        .await?;
    ensure!(result == "0x0000", "order dry run failed: {result}");
    Ok(())
}

async fn submit_and_verify(
    rpc: &DeepXRpcClient,
    signed: &DeepXPermittedPalletExtrinsic,
    operation: Operation,
) -> anyhow::Result<(u64, String)> {
    let name = operation.name();
    let expected_hash = format!("0x{}", hex::encode(signed.extrinsic_hash()));
    let encoded = format!("0x{}", hex::encode(signed.bytes()));
    let current_hash = rpc.get_best_block_hash_once().await?;
    let current_runtime = read_runtime(rpc, &current_hash).await?;
    ensure!(
        current_runtime.identity() == signed.runtime(),
        "runtime changed after {name} signing"
    );

    for pending in rpc.get_pending_extrinsics().await? {
        ensure!(
            !pending.eq_ignore_ascii_case(&encoded),
            "{name} extrinsic is already pending; reconcile hash={expected_hash}"
        );
        let bytes = hex::decode(
            pending
                .strip_prefix("0x")
                .context("invalid pending extrinsic hex")?,
        )?;

        match decode_signed_extrinsic_identity(current_runtime.metadata(), &bytes) {
            Ok(identity) => ensure!(
                identity.signer != signed.signer() || identity.nonce != signed.nonce(),
                "another extrinsic with this signer and nonce is pending; reconcile before {name} submission"
            ),
            Err(DeepXSignedExtrinsicDecodeError::MissingIdentity) => {}
            Err(e) => return Err(e.into()),
        }
    }
    let finalized_before = rpc.get_finalized_head_once().await?;
    let header: DeepXRpcBlockHeader = rpc.get_header_once(&finalized_before).await?;
    let mut next_height = decode_block_number_u64(&header.number)
        .context("invalid finalized block number")?
        .checked_add(1)
        .context("finalized block number overflow")?;
    let returned_hash = rpc
        .submit_encoded_extrinsic(&encoded)
        .await
        .with_context(|| format!("{name} submission uncertain; reconcile hash={expected_hash}"))?;
    ensure!(
        returned_hash.eq_ignore_ascii_case(&expected_hash),
        "{name} submission returned another hash; reconcile expected hash={expected_hash}"
    );
    println!("{name} submitted: hash={expected_hash}; awaiting finality");
    let deadline = Instant::now() + FINALITY_TIMEOUT;

    loop {
        ensure!(
            Instant::now() < deadline,
            "{name} finality timed out; reconcile hash={expected_hash}"
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
            "{name} finality scan exceeded its block limit; reconcile hash={expected_hash}"
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
            let success = match operation {
                Operation::PerpPlace(params) => {
                    decode_perp_place_inclusion(
                        runtime.interfaces(),
                        runtime.metadata(),
                        DeepXPerpPlaceExpected {
                            params,
                            order_id: signed.nonce(),
                            block_number: height,
                            extrinsic_index: index,
                        },
                        &events,
                    )?
                    .outcome
                        == DeepXPerpPlaceInclusionOutcome::Success
                }
                Operation::PerpCancel(params) => {
                    decode_perp_cancel_inclusion(
                        runtime.interfaces(),
                        runtime.metadata(),
                        DeepXPerpCancelExpected {
                            subaccount: params.subaccount,
                            order_id: params.order_id,
                            extrinsic_index: index,
                        },
                        false,
                        &events,
                    )?
                    .outcome
                        == DeepXPerpCancelInclusionOutcome::Success
                }
                Operation::PerpModify(params) => {
                    decode_perp_modify_inclusion(
                        runtime.interfaces(),
                        runtime.metadata(),
                        DeepXPerpModifyExpected {
                            params,
                            new_order_id: signed.nonce(),
                            block_number: height,
                            extrinsic_index: index,
                        },
                        &events,
                    )? == DeepXPerpModifyInclusionOutcome::Success
                }
                Operation::SpotPlace(params) => {
                    decode_spot_place_inclusion(
                        runtime.interfaces(),
                        runtime.metadata(),
                        DeepXSpotPlaceExpected {
                            params,
                            order_id: signed.nonce(),
                            extrinsic_index: index,
                        },
                        &events,
                    )?
                    .outcome
                        == DeepXSpotPlaceInclusionOutcome::Success
                }
                Operation::SpotCancel(params) => {
                    decode_spot_cancel_inclusion(
                        runtime.interfaces(),
                        runtime.metadata(),
                        DeepXSpotCancelExpected {
                            subaccount: params.subaccount,
                            pair: params.pair,
                            order_id: params.order_id,
                            is_buy: params.is_buy,
                            extrinsic_index: index,
                        },
                        &events,
                    )?
                    .outcome
                        == DeepXSpotCancelInclusionOutcome::Success
                }
                Operation::SpotModify(params) => {
                    decode_spot_modify_inclusion(
                        runtime.interfaces(),
                        runtime.metadata(),
                        DeepXSpotModifyExpected {
                            params,
                            new_order_id: signed.nonce(),
                            extrinsic_index: index,
                        },
                        &events,
                    )? == DeepXSpotModifyInclusionOutcome::Success
                }
            };
            ensure!(
                success,
                "{name} dispatch failed; reconcile hash={expected_hash}"
            );
            ensure!(
                rpc.get_block_hash_required_once(height)
                    .await?
                    .eq_ignore_ascii_case(&block_hash),
                "finalized block changed; reconcile hash={expected_hash}"
            );
            println!("{name} finalized: block={height} index={index} hash={expected_hash}");
            return Ok((height, block_hash));
        }
        next_height = finalized_height
            .checked_add(1)
            .context("finalized block number overflow")?;
        tokio::time::sleep(FINALITY_POLL_INTERVAL).await;
    }
}

async fn verify_order_state(
    rpc: &DeepXRpcClient,
    market: Market,
    order_id: u64,
    block_hash: &str,
    expect_open: bool,
) -> anyhow::Result<()> {
    let hash = decode_prefixed_hash(block_hash).context("invalid finalized block hash")?;

    match market {
        Market::Perp(params) => {
            let orders =
                get_perp_orders_by_id_at_block(rpc, hash, market.subaccount(), &[order_id]).await?;
            let order = orders
                .first()
                .context("finalized perpetual order is missing")?;
            ensure!(
                matches_perp_order(order, params, order_id),
                "perpetual order identity differs from supplied terms: order_id={order_id}"
            );
            ensure!(
                order.status
                    == if expect_open {
                        DeepXStoredOrderStatus::Open
                    } else {
                        DeepXStoredOrderStatus::Canceled
                    }
                    && order.size_filled == 0
                    && (!expect_open || order.size_remain == params.size),
                "perpetual order state disagrees with post-only lifecycle: order_id={order_id}"
            );
        }
        Market::Spot(params) => {
            let observed =
                get_spot_orders_by_id_at_block(rpc, hash, market.subaccount(), &[order_id]).await?;
            let order = observed
                .orders
                .first()
                .context("finalized Spot order is missing")?;
            ensure!(
                matches_spot_order(order, params, order_id),
                "Spot order identity differs from supplied terms: order_id={order_id}"
            );
            ensure!(
                order.status
                    == if expect_open {
                        DeepXStoredOrderStatus::Open
                    } else {
                        DeepXStoredOrderStatus::Canceled
                    }
                    && observed.active_order_ids.contains(&order_id) == expect_open
                    && observed
                        .active_order_pairs
                        .contains(&(order_id, params.pair))
                        == expect_open,
                "Spot order state disagrees with post-only lifecycle: order_id={order_id}"
            );
        }
    }
    Ok(())
}

async fn verify_perp_order_fill_state(
    rpc: &DeepXRpcClient,
    params: DeepXPerpPlaceParams,
    order_id: u64,
    block_hash: &str,
    status: DeepXStoredOrderStatus,
    filled_size: u128,
) -> anyhow::Result<()> {
    let hash = decode_prefixed_hash(block_hash).context("invalid finalized block hash")?;
    let orders = get_perp_orders_by_id_at_block(rpc, hash, params.subaccount, &[order_id]).await?;
    let order = orders
        .first()
        .context("finalized perpetual order is missing")?;
    ensure!(
        matches_perp_order(order, params, order_id)
            && order.status == status
            && order.size_filled == filled_size,
        "perpetual order identity, status, or filled size differs from signed terms: order_id={order_id}"
    );
    Ok(())
}

async fn verify_ioc_order_state(
    rpc: &DeepXRpcClient,
    market: Market,
    order_id: u64,
    block_hash: &str,
) -> anyhow::Result<()> {
    let hash = decode_prefixed_hash(block_hash).context("invalid finalized block hash")?;

    match market {
        Market::Perp(params) => {
            let orders =
                get_perp_orders_by_id_at_block(rpc, hash, params.subaccount, &[order_id]).await?;
            let order = orders
                .first()
                .context("finalized perpetual IOC order is missing")?;
            ensure!(
                matches_perp_order(order, params, order_id) && !order.status.is_active(),
                "perpetual IOC order identity or terminal state differs from signed terms: order_id={order_id}"
            );
            println!(
                "perpetual IOC finalized: order_id={order_id} status={:?} filled_raw={}",
                order.status, order.size_filled
            );
        }
        Market::Spot(params) => {
            let observed =
                get_spot_orders_by_id_at_block(rpc, hash, params.subaccount, &[order_id]).await?;
            let order = observed
                .orders
                .first()
                .context("finalized Spot IOC order is missing")?;
            ensure!(
                matches_spot_order(order, params, order_id)
                    && !order.status.is_active()
                    && !observed.active_order_ids.contains(&order_id),
                "Spot IOC order identity or terminal state differs from signed terms: order_id={order_id}"
            );
            println!(
                "Spot IOC finalized: order_id={order_id} status={:?}",
                order.status
            );
        }
    }
    Ok(())
}

fn matches_perp_order(
    order: &DeepXPerpOrderStorage,
    params: DeepXPerpPlaceParams,
    order_id: u64,
) -> bool {
    let expected_time_in_force = match params.order_type {
        DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc) => DeepXStoredTimeInForce::Gtc,
        DeepXPerpOrderType::Limit(DeepXTimeInForce::Ioc) => DeepXStoredTimeInForce::Ioc,
        DeepXPerpOrderType::Limit(DeepXTimeInForce::Fok) => DeepXStoredTimeInForce::Fok,
        _ => return false,
    };
    let expected_post_only = match params.post_only {
        DeepXPostOnlyParam::None => DeepXStoredPostOnlyParam::None,
        DeepXPostOnlyParam::MustPostOnly => DeepXStoredPostOnlyParam::MustPostOnly,
        DeepXPostOnlyParam::Adaptive => DeepXStoredPostOnlyParam::Adaptive,
    };
    order.order_id == order_id
        && order.owner == params.subaccount
        && order.market_id == params.market_id
        && order.is_long == params.is_long
        && order.size == params.size
        && order.price == params.price
        && order.order_type == DeepXStoredPerpOrderType::Limit(expected_time_in_force)
        && order.take_profit.is_none()
        && order.stop_loss.is_none()
        && !order.reduce_only
        && order.post_only == expected_post_only
}

fn matches_spot_order(
    order: &DeepXSpotOrderStorage,
    params: DeepXSpotPlaceParams,
    order_id: u64,
) -> bool {
    let expected_time_in_force = match params.order_type {
        DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc) => DeepXStoredTimeInForce::Gtc,
        DeepXSpotOrderType::Limit(DeepXTimeInForce::Ioc) => DeepXStoredTimeInForce::Ioc,
        DeepXSpotOrderType::Limit(DeepXTimeInForce::Fok) => DeepXStoredTimeInForce::Fok,
        _ => return false,
    };
    let expected_post_only = match params.post_only {
        DeepXPostOnlyParam::None => DeepXStoredPostOnlyParam::None,
        DeepXPostOnlyParam::MustPostOnly => DeepXStoredPostOnlyParam::MustPostOnly,
        DeepXPostOnlyParam::Adaptive => DeepXStoredPostOnlyParam::Adaptive,
    };
    order.order_id == order_id
        && order.owner == params.subaccount
        && order.pair == params.pair
        && order.is_buy == params.is_buy
        && order.quote_amount == params.quote_amount
        && order.base_amount == params.base_amount
        && order.order_type == DeepXStoredSpotOrderType::Limit(expected_time_in_force)
        && !order.reduce_only
        && order.post_only == expected_post_only
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn named_order_examples_insert_their_mode_before_options() {
        let base = [
            "perp",
            "0x1111111111111111111111111111111111111111",
            "3",
            "long",
            "123",
            "456",
        ]
        .map(str::to_string);
        let ioc = route_arguments(
            Some("--ioc"),
            base.clone().into_iter().chain(["--submit".to_string()]),
        )
        .unwrap();
        assert_eq!(&ioc[6..], ["--ioc", "--submit"]);
        let modify = route_arguments(
            Some("--modify-existing"),
            base.clone().into_iter().chain([
                "789".to_string(),
                "234".to_string(),
                "567".to_string(),
                "--submit".to_string(),
            ]),
        )
        .unwrap();
        assert_eq!(
            &modify[6..],
            ["--modify-existing", "789", "234", "567", "--submit"]
        );
        assert_eq!(
            route_arguments(None, base.clone().into_iter()).unwrap(),
            base
        );
        assert!(route_arguments(Some("--ioc"), ["--ioc".to_string()].into_iter()).is_err());
        assert!(route_arguments(Some("--unknown"), base.into_iter()).is_err());
    }

    #[rstest]
    fn parses_exact_perpetual_order_terms() {
        let market = parse_market("perp", [0x11; 20], "3", "short", 123, 456, false).unwrap();
        let Market::Perp(params) = market else {
            panic!("expected perpetual market");
        };
        assert_eq!(params.subaccount, [0x11; 20]);
        assert_eq!(params.market_id, 3);
        assert!(!params.is_long);
        assert_eq!(params.size, 123);
        assert_eq!(params.price, 456);
        assert_eq!(
            params.order_type,
            DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc)
        );
        assert_eq!(params.post_only, DeepXPostOnlyParam::MustPostOnly);
    }

    #[rstest]
    fn parses_exact_spot_order_terms() {
        let pair = format!("0x{}", hex::encode([0x22; 32]));
        let market = parse_market("spot", [0x11; 20], &pair, "sell", 123, 456, false).unwrap();
        let Market::Spot(params) = market else {
            panic!("expected Spot market");
        };
        let mut expected_quote = [0_u8; 32];
        expected_quote[..16].copy_from_slice(&123_u128.to_le_bytes());
        let mut expected_base = [0_u8; 32];
        expected_base[..16].copy_from_slice(&456_u128.to_le_bytes());
        assert_eq!(params.subaccount, [0x11; 20]);
        assert_eq!(params.pair, [0x22; 32]);
        assert!(!params.is_buy);
        assert_eq!(params.quote_amount, expected_quote);
        assert_eq!(params.base_amount, expected_base);
        assert_eq!(
            params.order_type,
            DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc)
        );
        assert_eq!(params.post_only, DeepXPostOnlyParam::MustPostOnly);
    }

    #[rstest]
    fn rejects_ambiguous_or_zero_order_terms() {
        for (kind, identity, side, first, second) in [
            ("perp", "3", "buy", 1, 1),
            ("perp", "3", "long", 0, 1),
            ("spot", "0x1234", "buy", 1, 1),
            ("spot", "0x1234", "hold", 1, 1),
            ("spot", "0x1234", "sell", 1, 0),
        ] {
            assert!(parse_market(kind, [0x11; 20], identity, side, first, second, false).is_err());
        }
    }

    #[rstest]
    fn recovery_requires_exact_perpetual_order_terms() {
        let Market::Perp(params) =
            parse_market("perp", [0x11; 20], "3", "long", 123, 456, false).unwrap()
        else {
            panic!("expected perpetual market");
        };
        let mut order = DeepXPerpOrderStorage {
            order_id: 789,
            owner: params.subaccount,
            market_id: params.market_id,
            is_long: params.is_long,
            size: params.size,
            price: params.price,
            order_type: DeepXStoredPerpOrderType::Limit(DeepXStoredTimeInForce::Gtc),
            create_time: 0,
            leverage: 1,
            status: DeepXStoredOrderStatus::Open,
            size_filled: 0,
            size_remain: params.size,
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXStoredPostOnlyParam::MustPostOnly,
        };
        assert!(matches_perp_order(&order, params, 789));
        order.price += 1;
        assert!(!matches_perp_order(&order, params, 789));
        order.price = params.price;
        order.market_id += 1;
        assert!(!matches_perp_order(&order, params, 789));

        let Market::Perp(ioc) =
            parse_market("perp", [0x11; 20], "3", "long", 123, 456, true).unwrap()
        else {
            panic!("expected perpetual market");
        };
        order.market_id = params.market_id;
        order.order_type = DeepXStoredPerpOrderType::Limit(DeepXStoredTimeInForce::Ioc);
        order.post_only = DeepXStoredPostOnlyParam::None;
        assert!(matches_perp_order(&order, ioc, 789));
        assert!(!matches_perp_order(&order, params, 789));
    }

    #[rstest]
    fn recovery_requires_exact_spot_order_terms() {
        let pair = format!("0x{}", hex::encode([0x22; 32]));
        let Market::Spot(params) =
            parse_market("spot", [0x11; 20], &pair, "buy", 123, 456, false).unwrap()
        else {
            panic!("expected Spot market");
        };
        let mut order = DeepXSpotOrderStorage {
            order_id: 789,
            owner: params.subaccount,
            pair: params.pair,
            price: None,
            quote_amount: params.quote_amount,
            base_amount: params.base_amount,
            create_time: 0,
            status: DeepXStoredOrderStatus::Open,
            order_type: DeepXStoredSpotOrderType::Limit(DeepXStoredTimeInForce::Gtc),
            post_only: DeepXStoredPostOnlyParam::MustPostOnly,
            reduce_only: false,
            is_buy: params.is_buy,
        };
        assert!(matches_spot_order(&order, params, 789));
        order.base_amount[0] ^= 1;
        assert!(!matches_spot_order(&order, params, 789));
        order.base_amount = params.base_amount;
        order.is_buy = false;
        assert!(!matches_spot_order(&order, params, 789));

        let Market::Spot(ioc) =
            parse_market("spot", [0x11; 20], &pair, "buy", 123, 456, true).unwrap()
        else {
            panic!("expected Spot market");
        };
        order.is_buy = params.is_buy;
        order.order_type = DeepXStoredSpotOrderType::Limit(DeepXStoredTimeInForce::Ioc);
        order.post_only = DeepXStoredPostOnlyParam::None;
        assert!(matches_spot_order(&order, ioc, 789));
        assert!(!matches_spot_order(&order, params, 789));
    }

    #[rstest]
    fn spot_modify_cancels_old_order_and_uses_explicit_replacement() {
        let pair = format!("0x{}", hex::encode([0x22; 32]));
        let Market::Spot(old) =
            parse_market("spot", [0x11; 20], &pair, "buy", 123, 456, false).unwrap()
        else {
            panic!("expected Spot market");
        };
        let Market::Spot(replacement) =
            parse_market("spot", [0x11; 20], &pair, "buy", 234, 456, false).unwrap()
        else {
            panic!("expected Spot market");
        };
        let params = spot_modify_params(old, replacement, 789).unwrap();
        assert_eq!(params.cancel.order_id, 789);
        assert_eq!(params.cancel.subaccount, old.subaccount);
        assert_eq!(params.cancel.pair, old.pair);
        assert_eq!(params.cancel.is_buy, old.is_buy);
        assert!(!params.cancel.fast_cancel);
        assert_eq!(params.place, replacement);

        let mut other_side = replacement;
        other_side.is_buy = false;
        assert!(spot_modify_params(old, other_side, 789).is_err());
    }

    #[rstest]
    fn perpetual_total_quantity_uses_exact_filled_size() {
        assert_eq!(replacement_size(300, 100).unwrap(), Some(200));
        assert_eq!(replacement_size(100, 100).unwrap(), None);
        assert_eq!(replacement_size(0, 0).unwrap(), None);
        assert!(replacement_size(99, 100).is_err());
        assert_eq!(replacement_size(u128::MAX, 1).unwrap(), Some(u128::MAX - 1));
    }
}
