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

//! Read-only block-pinned JSON-RPC checks using the standalone DeepX SDK.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
};

use alloy::primitives::{Signature, U256, keccak256};
use anyhow::{Context, ensure};
use aws_lc_rs::digest::{self, SHA256};
use deepx_rust_sdk::{
    credential::{DeepXKeyScheme, DeepXPrivateKey},
    deployment::{DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL},
    enums::DeepXEnvironment,
    identity::decode_account_id20,
    query::{
        DeepXAccountSortOrder, DeepXPerpHistoryOrdersRequest, DeepXPerpWalletTradesRequest,
        DeepXSpotHistoryOrdersRequest,
    },
    rest::{
        DeepXAccountPage, DeepXRestTransport,
        models::{
            DeepXPerpOrderRecord, DeepXPerpWalletSubaccountTrades, DeepXPerpWalletTradeMarket,
        },
    },
    rpc::{
        DEEPX_MARGIN_RUNTIME_API_METHOD, DeepXMarginDirection, DeepXRpcBlockHeader, DeepXRpcClient,
        DeepXRpcRuntimeVersion,
        chain_decode::{decode_block_number_u64, decode_prefixed_hash},
        endpoint_identity::{
            DEEPX_RECOVERY_RPC_METHODS, DEEPX_SUBMISSION_RPC_METHODS, DEEPX_WATCH_RPC_METHODS,
            DeepXObservedRpcEndpoint, DeepXRpcRole, validate_rpc_endpoint_identities,
        },
        event_storage::{
            SYSTEM_EVENTS_STORAGE_KEY, combine_events_map_batches, decode_event_storage_value,
            decode_event_threads, get_system_events, system_events_map_storage_key,
            system_threads_storage_key,
        },
        events::DeepXFinalizedPerpTrade,
        get_finalized_head, get_metadata, get_runtime_version,
        lending_positions::{
            get_lending_asset_at_block, get_lending_positions_at_block,
            lending_asset_reads_match_position,
        },
        margin::{encode_margin_requirement_request, get_margin_requirements_at_block},
        precompile::get_max_withdrawal_at_block_number,
        storage::{DeepXStoredOrderStatus, DeepXStoredPerpOrderType, DeepXStoredPostOnlyParam},
        storage_decode::{
            get_active_perp_orders_at_block, get_perp_orders_by_id_at_block,
            get_perp_positions_at_block, get_spot_orders_by_id_at_block,
        },
        timestamp::{
            decode_timestamp_set_inherent, get_timestamp_now, query_timestamp_now_at_block,
        },
        trade_event_decode::{
            decode_finalized_perp_trade_events, decode_finalized_spot_trade_events,
        },
        trade_match::match_finalized_wallet_perp_trades,
    },
    signing::{
        DeepXEvmTransactionKind, DeepXOfflineEvmTransactionParams, DeepXOfflinePerpPnlEvmParams,
        DeepXPerpOrderType, DeepXPerpPlaceParams, DeepXPerpProfitAndLossPointParams,
        DeepXPostOnlyParam, DeepXReadOnlyRuntimeSnapshot, DeepXRuntimeSnapshotService,
        DeepXTimeInForce, RuntimeSnapshot, sign_no_op, sign_offline_evm_transact_extrinsic,
        sign_permitted_dynamic_pallet_call, sign_perp_place_order, validate_testnet_runtime,
    },
};
use parity_scale_codec::{Compact, Decode};
use serde_json::{Value, json};
use subxt_core::dynamic::Value as DynamicValue;

const MAX_RELEVANT_GROUPS: usize = 20;
const MAX_PAGES_PER_GROUP: usize = 10;
const ETH_CALL_PROBE_ADDRESS: &str = "0x0000000000000000000000000000000000000001";

struct PreparedPerpHistory {
    market_id: u64,
    base_decimal: u8,
    records: Vec<DeepXPerpOrderRecord>,
    ids: Vec<u64>,
}

#[derive(serde::Deserialize)]
struct TimestampBlockResponse {
    block: TimestampBlock,
}

#[derive(serde::Deserialize)]
struct TimestampBlock {
    header: DeepXRpcBlockHeader,
    extrinsics: Vec<String>,
}

async fn verify_account_checkpoint(
    client: &DeepXRpcClient,
    block_hash: &str,
    block: [u8; 32],
    block_number: u64,
) -> anyhow::Result<u64> {
    let selected = format!("0x{block_number:x}");
    ensure!(
        client
            .eth_call(ETH_CALL_PROBE_ADDRESS, &[], &selected)
            .await?
            == "0x",
        "block-selected eth_call probe returned unexpected data"
    );
    let rejection = client
        .eth_call_response::<String>(ETH_CALL_PROBE_ADDRESS, &[], "0xffffffff")
        .await?;
    ensure!(
        rejection.jsonrpc.as_deref() == Some("2.0")
            && rejection.id == Some(1)
            && rejection.result.is_none()
            && rejection.error.is_some(),
        "RPC endpoint ignored an invalid eth_call block selection"
    );
    let storage_ms = get_timestamp_now(client, block).await?;
    let response: TimestampBlockResponse = client.get_block(block_hash).await?;
    ensure!(
        decode_block_number_u64(&response.block.header.number) == Some(block_number),
        "timestamp block number differs from the checkpoint"
    );
    let inherent_ms = decode_timestamp_set_inherent(&response.block.extrinsics)
        .context("invalid Timestamp.set block inherent")?;
    ensure!(
        storage_ms == inherent_ms,
        "block inherent timestamp differs from storage"
    );
    ensure!(
        query_timestamp_now_at_block(client, block).await? == storage_ms,
        "storage query timestamp differs from the block inherent"
    );
    Ok(storage_ms)
}

async fn verify_read_only_runtime_at(
    client: &DeepXRpcClient,
    block_hash: &str,
) -> anyhow::Result<()> {
    let version: DeepXRpcRuntimeVersion = get_runtime_version(client, block_hash).await?;
    let encoded = get_metadata(client, block_hash).await?;
    let metadata = hex::decode(
        encoded
            .strip_prefix("0x")
            .context("runtime metadata is not hex encoded")?,
    )?;
    let genesis = decode_prefixed_hash(DEEPX_TESTNET_GENESIS_HASH)
        .context("invalid approved testnet genesis hash")?;
    let snapshot = DeepXReadOnlyRuntimeSnapshot::approved_testnet(
        &DeepXEnvironment::Testnet,
        genesis,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )?;
    println!(
        "read-only runtime verified: block_hash={block_hash} spec={} transaction={} metadata_sha256=0x{}",
        snapshot.identity().spec_version,
        snapshot.identity().transaction_version,
        hex::encode(snapshot.identity().metadata_sha256)
    );
    Ok(())
}

async fn verify_rpc_roles(check_methods: bool) -> anyhow::Result<()> {
    let url = DEEPX_TESTNET_RPC_URL.to_string();
    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let clients = [
        DeepXRpcClient::new_with_proxy(url.clone(), proxy.as_deref())?,
        DeepXRpcClient::new_with_proxy(url.clone(), proxy.as_deref())?,
        DeepXRpcClient::new_with_proxy(url.clone(), proxy.as_deref())?,
    ];
    let (submission, watch, recovery) = tokio::try_join!(
        clients[0].get_block_hash_required_once(0),
        clients[1].get_block_hash_required_once(0),
        clients[2].get_block_hash_required_once(0),
    )?;
    let observed = [
        (DeepXRpcRole::Submission, submission),
        (DeepXRpcRole::Watch, watch),
        (DeepXRpcRole::Recovery, recovery),
    ]
    .into_iter()
    .map(|(role, encoded)| {
        let genesis = decode_prefixed_hash(&encoded).context("invalid role genesis hash")?;
        anyhow::Ok(DeepXObservedRpcEndpoint::new(role, url.clone(), genesis))
    })
    .collect::<anyhow::Result<Vec<_>>>()?;
    let endpoints = validate_rpc_endpoint_identities(&url, &url, &url, observed)?;
    if check_methods {
        let probes = [
            (DeepXRpcRole::Submission, DEEPX_SUBMISSION_RPC_METHODS),
            (DeepXRpcRole::Watch, DEEPX_WATCH_RPC_METHODS),
            (DeepXRpcRole::Recovery, DEEPX_RECOVERY_RPC_METHODS),
        ];
        for (client, (role, required)) in clients.iter().zip(probes) {
            let result: Value = client
                .execute_rpc_call(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "rpc_methods",
                    "params": [],
                }))
                .await
                .with_context(|| format!("RPC method probe failed for {role:?}"))?;
            let advertised = result["methods"]
                .as_array()
                .with_context(|| format!("RPC methods missing for {role:?}"))?;
            for method in required {
                ensure!(
                    advertised
                        .iter()
                        .any(|value| value.as_str() == Some(method)),
                    "RPC endpoint for {role:?} does not advertise {method}"
                );
            }
        }
    }
    println!(
        "RPC roles verified: submission, watch, recovery; genesis=0x{}; methods_checked={check_methods}",
        hex::encode(endpoints.genesis_hash())
    );
    Ok(())
}

fn verify_signing_vectors() -> anyhow::Result<()> {
    const NO_OP: &str = "850184fcad0b19bb29d4674531d6f115237e16afce377ccd76f6b9583b6dfd829576bc6888f00fa985b12eb69488d9215ad76133b8792113e341e9f2dc4a0e073af90495f8926c9d4c9e817f29ee384e83fb3dd18d78f600000b7c2203a2910100131c";
    const PERP_PLACE: &str = "b90284fcad0b19bb29d4674531d6f115237e16afce377c6476e0a03c23f5e2d94326d35a5d1db1a3b03e19e7270548a4781c5ffcd9c63f13f98e79b13089f1010035607eff80c9990aa7dcbbeb35fee0fb7bae24df0b2401000b7d2203a291010016021111111111111111111111111111111111111111070001ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff000001ffffffffffffffffffffffffffffffff000000";
    const REMARK: &str = "f50184fcad0b19bb29d4674531d6f115237e16afce377cdd2abfbe77653eff8459e3105b30feb8295341991ff6cd015b5a94ca0ae27bf30f2be47c10c8a8f926f0ba538bd72d5ddb9a6eb38549961dfb0fa8e96433bc6f01000b7b2203a291010000006c64656570782d6f66666c696e652d7369676e696e672d636865636b";
    let fixture: Value = serde_json::from_str(include_str!(
        "../test_data/runtime/testnet/genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json"
    ))?;
    let encoded = fixture["result"]
        .as_str()
        .context("runtime metadata fixture has no result")?;
    let metadata = hex::decode(
        encoded
            .strip_prefix("0x")
            .context("invalid metadata fixture")?,
    )?;
    let genesis = decode_prefixed_hash(DEEPX_TESTNET_GENESIS_HASH)
        .context("invalid approved genesis hash")?;
    let snapshot =
        RuntimeSnapshot::approved_testnet(&DeepXEnvironment::Testnet, genesis, 376, 1, &metadata)?;
    let service = DeepXRuntimeSnapshotService::new(snapshot);
    let permit = service.acquire()?;
    let key = DeepXPrivateKey::new(
        "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        &DeepXKeyScheme::Secp256k1,
    )?;
    let no_op = sign_no_op(&permit, &key, 1_725_000_000_124)?;
    ensure!(
        hex::encode(no_op.bytes()) == NO_OP,
        "spec-376 no-op signing vector differs"
    );
    let place = sign_perp_place_order(
        &permit,
        &key,
        DeepXPerpPlaceParams {
            subaccount: [0x11; 20],
            market_id: 7,
            is_long: true,
            size: u128::MAX,
            price: u128::MAX,
            order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
            take_profit: Some(u128::MAX),
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXPostOnlyParam::None,
        },
        1_725_000_000_125,
    )?;
    ensure!(
        hex::encode(place.bytes()) == PERP_PLACE,
        "spec-376 place signing vector differs"
    );
    let remark = sign_permitted_dynamic_pallet_call(
        &permit,
        &key,
        "System",
        "remark",
        vec![DynamicValue::from_bytes(b"deepx-offline-signing-check")],
        1_725_000_000_123,
    )?;
    ensure!(
        hex::encode(remark.bytes()) == REMARK,
        "spec-376 remark signing vector differs"
    );
    verify_evm_signing_vectors(permit.snapshot(), &key)?;
    verify_finalized_signing_context()?;
    println!(
        "offline signing vectors verified: no_op=0x{} perp_place=0x{} remark=0x{}",
        hex::encode(no_op.extrinsic_hash()),
        hex::encode(place.extrinsic_hash()),
        hex::encode(remark.extrinsic_hash())
    );
    Ok(())
}

fn verify_evm_signing_vectors(
    snapshot: &RuntimeSnapshot,
    key: &DeepXPrivateKey,
) -> anyhow::Result<()> {
    const EIP1559_TX: &str = "02f86f8212ee860191a203228c80808307a120941111111111111111111111111111111111111111808412345678c080a0639ada905c91501e47eced7b49deeed8cf16f17fd620b857bd5aee2cbb8218e3a0425d15bea327d1df586898c7fde06f5b6005b946860691a0c1c0389233f3eaf1";
    const LEGACY_TX: &str = "f86c860191a203228c808307a120941111111111111111111111111111111111111111808412345678822600a0833e8851cce34f8f0f1280e4d037df20b397c048a71df05e8019acefd34eca75a044126e0458821c52e1dbb4a08f1a9eb41247f831a17366c5c27045183bbfffee";
    const EIP1559_WRAPPER: &str = "710404290002ee120000000000008c2203a2910100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020a1070000000000000000000000000000000000000000000000000000000000001111111111111111111111111111111111111111000000000000000000000000000000000000000000000000000000000000000010123456780000639ada905c91501e47eced7b49deeed8cf16f17fd620b857bd5aee2cbb8218e3425d15bea327d1df586898c7fde06f5b6005b946860691a0c1c0389233f3eaf1fcad0b19bb29d4674531d6f115237e16afce377c";
    const LEGACY_WRAPPER: &str = "e903042900008c2203a291010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020a1070000000000000000000000000000000000000000000000000000000000001111111111111111111111111111111111111111000000000000000000000000000000000000000000000000000000000000000010123456780100000000000000833e8851cce34f8f0f1280e4d037df20b397c048a71df05e8019acefd34eca7544126e0458821c52e1dbb4a08f1a9eb41247f831a17366c5c27045183bbfffeefcad0b19bb29d4674531d6f115237e16afce377c";
    for (kind, expected_tx, expected_hash, expected_wrapper, expected_pnl, expected_pnl_wrapper) in [
        (
            DeepXEvmTransactionKind::Eip1559,
            EIP1559_TX,
            "34ed9fbf57152e47a2efbf4beb6e0f6ddf53571ed36c0b1c59a29a7b546dfa62",
            EIP1559_WRAPPER,
            "2ab5f44c34d307b8e33b6967968fda3e6fb7ff21c7326cd9c5208e7c3b0ead4f",
            "1cb8be646af7de0e8ce0a779df69db5eccd543fa7383707c106c9322368f82d7",
        ),
        (
            DeepXEvmTransactionKind::Legacy,
            LEGACY_TX,
            "b8b6810903e60203709d97d61641fb7c110b035f4102c7d99d24740b366075d3",
            LEGACY_WRAPPER,
            "72ee4be55ce21d3a857c3d59ed064a75557ba83edeee30a552af3a39d68d2435",
            "af40605ce99f7d31b18662700704ec99c58a6d54369a41a6e85e3d9f48e296db",
        ),
    ] {
        let params = DeepXOfflineEvmTransactionParams {
            kind,
            chain_id: 0x12ee,
            nonce: 1_725_000_000_140,
            gas_limit: 500_000,
            precompile_address: [0x11; 20],
            value: U256::ZERO,
            calldata: vec![0x12, 0x34, 0x56, 0x78],
        };
        let signed = sign_offline_evm_transact_extrinsic(
            snapshot.metadata(),
            snapshot.interfaces(),
            key,
            &params,
        )?;
        ensure!(
            hex::encode(&signed.evm.bytes) == expected_tx,
            "offline EVM envelope differs: {kind:?}"
        );
        ensure!(
            hex::encode(signed.evm.tx_hash) == expected_hash,
            "offline EVM transaction hash differs: {kind:?}"
        );
        ensure!(
            hex::encode(&signed.bytes) == expected_wrapper,
            "offline EVM runtime wrapper differs: {kind:?}"
        );
        let pnl = DeepXOfflinePerpPnlEvmParams {
            kind,
            nonce: 1_725_000_000_140,
            gas_limit: 500_000,
            call: DeepXPerpProfitAndLossPointParams {
                subaccount: [0x11; 20],
                market_id: 7,
                take_profit_point: 123,
                stop_loss_point: 456,
            },
        }
        .into_transaction_params();
        let signed = sign_offline_evm_transact_extrinsic(
            snapshot.metadata(),
            snapshot.interfaces(),
            key,
            &pnl,
        )?;
        ensure!(
            hex::encode(signed.evm.tx_hash) == expected_pnl
                && hex::encode(signed.extrinsic_hash) == expected_pnl_wrapper,
            "offline Perp precompile vector differs: {kind:?}"
        );
        println!("offline EVM and Perp precompile vectors verified: {kind:?}");
    }
    Ok(())
}

fn verify_finalized_signing_context() -> anyhow::Result<()> {
    const FINALIZED_CANCEL: &str = concat!(
        "050284bb3d6fdc376a153ead60bc2ab97dd0be7cd55960065f5de9b395a3c3155f8837b88532",
        "03f944ce34754e59ee71b1b514b96f9e420222a084aa4374aac260da37b6acb697514a27657a75",
        "aa1aafe5d278e5ce591801000b0dc99734a101001603a13c7ab8decf4d32cf80c371647c93d0e2",
        "f4183e60982d21a101000002000000"
    );
    let encoded = hex::decode(FINALIZED_CANCEL)?;
    let mut input = encoded.as_slice();
    let Compact(length) = Compact::<u32>::decode(&mut input)?;
    ensure!(
        encoded.len() - input.len() == 2
            && input.len() == usize::try_from(length)?
            && encoded.len() >= 97,
        "finalized cancel extrinsic SCALE length or structure differs"
    );
    ensure!(
        encoded.get(2) == Some(&0x84),
        "finalized cancel extrinsic is not signed version 4"
    );
    let account: [u8; 20] = encoded[3..23].try_into()?;
    let signature = Signature::try_from(&encoded[23..88])?;
    let extra = &encoded[88..97];
    let call = &encoded[97..];
    ensure!(
        extra.len() == 9 && extra[0] == 0 && extra[1] == 0x0b && extra[8] == 0,
        "finalized cancel extrinsic has an unexpected immortal extension"
    );
    ensure!(
        call.starts_with(&[22, 3]) && call.len() == 34,
        "finalized extrinsic is not the captured Perp cancel"
    );
    let genesis = decode_prefixed_hash(DEEPX_TESTNET_GENESIS_HASH)
        .context("invalid approved genesis hash")?;
    let recover = |spec_version: u32, implicit_era: [u8; 32]| -> anyhow::Result<[u8; 20]> {
        let mut payload = Vec::with_capacity(call.len() + extra.len() + 72);
        payload.extend_from_slice(call);
        payload.extend_from_slice(extra);
        payload.extend_from_slice(&spec_version.to_le_bytes());
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.extend_from_slice(&genesis);
        payload.extend_from_slice(&implicit_era);
        Ok(signature
            .recover_address_from_prehash(&keccak256(payload))?
            .into_array())
    };
    ensure!(
        recover(376, genesis)? == account
            && recover(375, genesis)? != account
            && recover(376, [0; 32])? != account,
        "finalized cancel signature does not bind spec 376 and immortal genesis"
    );
    println!("finalized spec-376 Perp cancel signature context verified");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().context(
        "usage: deepx-sdk-verify-rpc <signing-vectors|runtime|event-storage|capture-events|finalized-trades|current-margin|finalized-margin|finalized-positions|finalized-orders|finalized-perp-history|finalized-spot-orders|finalized-spot-history|finalized-spot-ids|finalized-lending|finalized-account|finalized-wallet> [argument]; capture-events <block-number> <absolute-output-root>",
    )?;
    let event_block = if matches!(
        mode.as_str(),
        "event-storage" | "capture-events" | "finalized-trades"
    ) {
        Some(
            args.next()
                .context("missing block number")?
                .parse::<u64>()?,
        )
    } else {
        None
    };
    let capture_root = if mode == "capture-events" {
        let root = PathBuf::from(args.next().context("missing absolute output root")?);
        ensure!(root.is_absolute(), "capture output root must be absolute");
        Some(root)
    } else {
        None
    };
    let wallet = if mode == "finalized-wallet" {
        Some(args.next().context("missing wallet")?)
    } else {
        None
    };
    let event_wallet = if mode == "finalized-trades" {
        args.next()
    } else {
        None
    };
    let subaccount = match mode.as_str() {
        "signing-vectors" | "runtime" | "event-storage" | "capture-events" | "finalized-trades"
        | "finalized-wallet" => None,
        "current-margin"
        | "finalized-margin"
        | "finalized-positions"
        | "finalized-orders"
        | "finalized-perp-history"
        | "finalized-spot-orders"
        | "finalized-spot-history"
        | "finalized-spot-ids"
        | "finalized-lending"
        | "finalized-account" => {
            let address = args.next().context("missing subaccount")?;
            Some(decode_account_id20(&address).context("invalid subaccount AccountId20")?)
        }
        _ => anyhow::bail!("unknown RPC verification mode: {mode}"),
    };
    let history_market = if mode == "finalized-perp-history" {
        Some(
            args.next()
                .context("missing perpetual market ID")?
                .parse::<u64>()?,
        )
    } else {
        None
    };
    let spot_ids = if mode == "finalized-spot-ids" {
        args.by_ref()
            .map(|value| value.parse::<u64>())
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    ensure!(args.next().is_none(), "unexpected argument");
    if mode == "signing-vectors" {
        return verify_signing_vectors();
    }
    if mode == "runtime" || mode == "finalized-trades" || subaccount.is_some() || wallet.is_some() {
        verify_rpc_roles(mode != "runtime").await?;
    }
    let rpc_url = if mode == "capture-events" {
        std::env::var("DEEPX_TESTNET_RPC_URL").unwrap_or_else(|_| DEEPX_TESTNET_RPC_URL.to_string())
    } else {
        DEEPX_TESTNET_RPC_URL.to_string()
    };
    let client = DeepXRpcClient::new_with_proxy(
        rpc_url.clone(),
        std::env::var("DEEPX_PROXY_URL").ok().as_deref(),
    )?;
    let genesis = client.get_block_hash_required_once(0).await?;
    ensure!(
        genesis.eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
        "RPC endpoint genesis hash differs from approved DeepX testnet"
    );
    if let Some(block_number) = event_block {
        if let Some(root) = capture_root {
            return capture_events(&client, &rpc_url, block_number, &root).await;
        }
        return verify_event_storage(
            &client,
            block_number,
            mode == "finalized-trades",
            event_wallet.as_deref(),
        )
        .await;
    }
    if let Some(wallet) = wallet {
        return verify_wallet(&client, &wallet).await;
    }
    let perp_history = if mode == "finalized-perp-history" {
        Some(
            prepare_perp_history(
                subaccount.context("missing subaccount")?,
                history_market.context("missing perpetual market ID")?,
            )
            .await?,
        )
    } else {
        None
    };
    let spot_history_ids = if mode == "finalized-spot-history" {
        Some(prepare_spot_history(subaccount.context("missing subaccount")?).await?)
    } else {
        None
    };
    let block_hash = if mode == "current-margin" {
        client.get_best_block_hash_once().await?
    } else {
        get_finalized_head(&client).await?
    };
    let block = decode_prefixed_hash(&block_hash).context("invalid block hash")?;
    if let Some(subaccount) = subaccount {
        if mode != "current-margin" {
            verify_read_only_runtime_at(&client, &block_hash).await?;
        }
        let header: DeepXRpcBlockHeader = client.get_header_once(&block_hash).await?;
        let block_number =
            decode_block_number_u64(&header.number).context("invalid block height")?;
        match mode.as_str() {
            "current-margin" => {
                verify_current_margin(&client, block, subaccount).await?;
            }
            "finalized-margin" => {
                verify_margin(&client, block, subaccount).await?;
            }
            "finalized-positions" => verify_positions(&client, block, subaccount).await?,
            "finalized-orders" => verify_orders(&client, block, subaccount).await?,
            "finalized-perp-history" => {
                verify_perp_history(
                    &client,
                    block,
                    subaccount,
                    perp_history.as_ref().context("missing perpetual history")?,
                )
                .await?;
            }
            "finalized-spot-orders" => verify_spot_orders(&client, block, subaccount).await?,
            "finalized-spot-history" | "finalized-spot-ids" => {
                verify_spot_history(
                    &client,
                    block,
                    subaccount,
                    spot_history_ids.as_deref().unwrap_or(&spot_ids),
                )
                .await?;
            }
            "finalized-lending" => {
                verify_lending(&client, block, block_number, subaccount).await?;
            }
            "finalized-account" => {
                let timestamp_ms =
                    verify_account_checkpoint(&client, &block_hash, block, block_number).await?;
                verify_margin(&client, block, subaccount).await?;
                verify_positions(&client, block, subaccount).await?;
                verify_orders(&client, block, subaccount).await?;
                verify_spot_orders(&client, block, subaccount).await?;
                verify_lending(&client, block, block_number, subaccount).await?;
                ensure!(
                    get_timestamp_now(&client, block).await? == timestamp_ms,
                    "account timestamp changed at the pinned block"
                );
            }
            _ => unreachable!(),
        }
        let canonical = client.get_block_hash_required_once(block_number).await?;
        ensure!(
            canonical.eq_ignore_ascii_case(&block_hash),
            "block hash changed during the read"
        );
        println!(
            "{mode} raw reads verified: subaccount=0x{} block_hash={block_hash}",
            hex::encode(subaccount)
        );
    } else {
        let version: DeepXRpcRuntimeVersion = get_runtime_version(&client, &block_hash).await?;
        let encoded_metadata = get_metadata(&client, &block_hash).await?;
        let metadata = hex::decode(
            encoded_metadata
                .strip_prefix("0x")
                .context("runtime metadata is not hex encoded")?,
        )?;
        ensure!(!metadata.is_empty(), "runtime metadata is empty");
        let genesis_id = decode_prefixed_hash(&genesis).context("invalid testnet genesis hash")?;
        let snapshot = RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            genesis_id,
            version.spec_version,
            version.transaction_version,
            &metadata,
        )?;
        println!(
            "runtime verified: genesis={genesis} block_hash={block_hash} spec={} transaction={} metadata_sha256=0x{}",
            snapshot.identity().spec_version,
            snapshot.identity().transaction_version,
            hex::encode(snapshot.identity().metadata_sha256)
        );
    }
    Ok(())
}

async fn verify_margin(
    client: &DeepXRpcClient,
    block: [u8; 32],
    subaccount: [u8; 20],
) -> anyhow::Result<()> {
    let (initial, maintenance) =
        get_margin_requirements_at_block(client, block, subaccount).await?;
    println!(
        "margin raw: initial_collateral={} initial_required={} maintenance_collateral={} maintenance_required={}",
        initial.total_collateral,
        initial.total_margin_required,
        maintenance.total_collateral,
        maintenance.total_margin_required
    );
    Ok(())
}

async fn verify_current_margin(
    client: &DeepXRpcClient,
    block: [u8; 32],
    subaccount: [u8; 20],
) -> anyhow::Result<()> {
    let rejected = client
        .state_call_response::<String>(
            DEEPX_MARGIN_RUNTIME_API_METHOD,
            encode_margin_requirement_request(subaccount, DeepXMarginDirection::Initial),
            &format!("0x{}", "00".repeat(32)),
        )
        .await?;
    ensure!(
        rejected.jsonrpc.as_deref() == Some("2.0")
            && rejected.id == Some(1)
            && rejected.result.is_none()
            && rejected.error.is_some(),
        "Watch RPC ignored an invalid margin block hash"
    );
    let first = get_margin_requirements_at_block(client, block, subaccount).await?;
    let second = get_margin_requirements_at_block(client, block, subaccount).await?;
    ensure!(first == second, "margin changed at a fixed block hash");
    println!(
        "current margin raw: initial_collateral={} initial_required={} maintenance_collateral={} maintenance_required={}",
        first.0.total_collateral,
        first.0.total_margin_required,
        first.1.total_collateral,
        first.1.total_margin_required
    );
    Ok(())
}

async fn verify_event_storage(
    client: &DeepXRpcClient,
    block_number: u64,
    decode_trades: bool,
    wallet: Option<&str>,
) -> anyhow::Result<()> {
    let finalized_hash = get_finalized_head(client).await?;
    let finalized_header: DeepXRpcBlockHeader = client.get_header_once(&finalized_hash).await?;
    let finalized_number = decode_block_number_u64(&finalized_header.number)
        .context("invalid finalized head number")?;
    ensure!(
        block_number <= finalized_number,
        "requested block is above the finalized head"
    );
    let block_hash = client.get_block_hash_required_once(block_number).await?;
    let header: DeepXRpcBlockHeader = client.get_header_once(&block_hash).await?;
    ensure!(
        decode_block_number_u64(&header.number) == Some(block_number),
        "canonical header number mismatch"
    );
    let first = get_system_events(client, &block_hash, block_number).await?;
    let second = get_system_events(client, &block_hash, block_number).await?;
    ensure!(
        first == second,
        "event storage changed at a fixed block hash"
    );
    let count = Compact::<u32>::decode(&mut first.as_slice())?.0;
    let trades = if decode_trades {
        let version: DeepXRpcRuntimeVersion = get_runtime_version(client, &block_hash).await?;
        let encoded = get_metadata(client, &block_hash).await?;
        let metadata = hex::decode(encoded.strip_prefix("0x").context("invalid metadata hex")?)?;
        let genesis = decode_prefixed_hash(DEEPX_TESTNET_GENESIS_HASH)
            .context("invalid approved testnet genesis hash")?;
        let runtime = validate_testnet_runtime(
            &DeepXEnvironment::Testnet,
            genesis,
            version.spec_version,
            version.transaction_version,
            &metadata,
        )?;
        let block = decode_prefixed_hash(&block_hash).context("invalid event block hash")?;
        let perp = decode_finalized_perp_trade_events(
            runtime.interfaces(),
            runtime.metadata(),
            block,
            block_number,
            &first,
        )?;
        let spot = decode_finalized_spot_trade_events(
            runtime.interfaces(),
            runtime.metadata(),
            block,
            block_number,
            &first,
        )?;
        let wallet_matches = if let Some(wallet) = wallet {
            Some(verify_finalized_wallet_trades(wallet, &perp).await?)
        } else {
            None
        };
        Some((perp, spot, version.spec_version, wallet_matches))
    } else {
        None
    };
    let canonical = client.get_block_hash_required_once(block_number).await?;
    ensure!(
        canonical.eq_ignore_ascii_case(&block_hash),
        "canonical event block hash changed during the read"
    );
    if let Some((perp, spot, spec_version, wallet_matches)) = trades {
        for trade in &perp {
            println!("perp_trade={trade:?}");
        }
        for trade in &spot {
            println!("spot_trade={trade:?}");
        }
        println!(
            "finalized trades decoded: block={block_number} perp={} spot={} spec={spec_version}",
            perp.len(),
            spot.len()
        );
        if let (Some(wallet), Some(matched)) = (wallet, wallet_matches) {
            println!("finalized wallet perpetual trades matched: wallet={wallet} count={matched}");
        }
    }
    let hash = digest::digest(&SHA256, &first);
    println!(
        "finalized event storage verified: block={block_number} hash={block_hash} events={count} bytes={} sha256=0x{}",
        first.len(),
        hex::encode(hash.as_ref())
    );
    Ok(())
}

async fn capture_events(
    client: &DeepXRpcClient,
    rpc_url: &str,
    block_number: u64,
    output_root: &Path,
) -> anyhow::Result<()> {
    const SPEC_VERSION: u32 = 376;
    const TRANSACTION_VERSION: u32 = 1;
    const METADATA_SHA256: &str =
        "57f1bedf1e04522176a2b422af9249d5c990693fb92d11327870d8ef751a8447";
    let finalized_hash = get_finalized_head(client).await?;
    let finalized_header: DeepXRpcBlockHeader = client.get_header_once(&finalized_hash).await?;
    let finalized_height = decode_block_number_u64(&finalized_header.number)
        .context("invalid finalized head number")?;
    ensure!(
        block_number <= finalized_height,
        "capture block is not finalized"
    );
    let block_hash = client.get_block_hash_required_once(block_number).await?;
    let header: DeepXRpcBlockHeader = client.get_header_once(&block_hash).await?;
    ensure!(
        decode_block_number_u64(&header.number) == Some(block_number),
        "capture block header number mismatch"
    );
    let version: DeepXRpcRuntimeVersion = get_runtime_version(client, &block_hash).await?;
    let encoded_metadata = get_metadata(client, &block_hash).await?;
    let metadata = hex::decode(
        encoded_metadata
            .strip_prefix("0x")
            .context("invalid runtime metadata hex")?,
    )?;
    let genesis = decode_prefixed_hash(DEEPX_TESTNET_GENESIS_HASH)
        .context("invalid approved genesis hash")?;
    let runtime = validate_testnet_runtime(
        &DeepXEnvironment::Testnet,
        genesis,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )?;
    ensure!(
        version.spec_version == SPEC_VERSION
            && version.transaction_version == TRANSACTION_VERSION
            && hex::encode(runtime.identity().metadata_sha256) == METADATA_SHA256,
        "capture block differs from the reviewed spec-376 runtime"
    );

    let mut payloads = Vec::<(String, Value)>::new();
    let standard = capture_storage_rpc(client, SYSTEM_EVENTS_STORAGE_KEY, &block_hash).await?;
    let standard_value = standard["result"].as_str().map(str::to_owned);
    payloads.push(("system_events.json".to_string(), standard));
    let (events, layout, threads_key, map_keys) = if let Some(encoded) = standard_value.as_deref() {
        let events = decode_event_storage_value(encoded).context("invalid System.Events value")?;
        capture_storage_repeat(
            client,
            SYSTEM_EVENTS_STORAGE_KEY,
            &block_hash,
            encoded,
            "system_events",
            &mut payloads,
        )
        .await?;
        (events, "System.Events", None, Vec::new())
    } else {
        let threads_key = system_threads_storage_key(block_number);
        let threads = capture_storage_rpc(client, &threads_key, &block_hash).await?;
        let encoded = threads["result"]
            .as_str()
            .context("System.Threads unavailable")?;
        let last_thread = decode_event_threads(encoded).context("invalid System.Threads value")?;
        capture_storage_repeat(
            client,
            &threads_key,
            &block_hash,
            encoded,
            "system_threads",
            &mut payloads,
        )
        .await?;
        payloads.push(("system_threads.json".to_string(), threads));
        let mut batches = Vec::with_capacity(usize::from(last_thread) + 1);
        let mut map_keys = Vec::with_capacity(batches.capacity());
        for thread in 0..=last_thread {
            let key = system_events_map_storage_key(block_number, thread);
            let response = capture_storage_rpc(client, &key, &block_hash).await?;
            let encoded = response["result"]
                .as_str()
                .context("System.EventsMap unavailable")?;
            let base = format!("events_map_thread_{thread}");
            capture_storage_repeat(client, &key, &block_hash, encoded, &base, &mut payloads)
                .await?;
            batches.push(decode_event_storage_value(encoded).context("invalid EventsMap batch")?);
            map_keys.push(key);
            payloads.push((format!("{base}.json"), response));
        }
        (
            combine_events_map_batches(&batches).context("invalid EventsMap assembly")?,
            "System.Threads+System.EventsMap",
            Some(threads_key),
            map_keys,
        )
    };
    let count = Compact::<u32>::decode(&mut events.as_slice())?.0;
    ensure!(
        events == get_system_events(client, &block_hash, block_number).await?,
        "captured storage differs from SDK event assembly"
    );
    let canonical = client.get_block_hash_required_once(block_number).await?;
    ensure!(
        canonical.eq_ignore_ascii_case(&block_hash),
        "capture block hash changed"
    );

    let fixture_dir = output_root.join(format!("finalized-events-{block_number}-spec376"));
    ensure!(
        !fixture_dir.exists(),
        "event fixture already exists: {}",
        fixture_dir.display()
    );
    let staging_dir = fixture_dir.with_extension(format!("partial-{}", std::process::id()));
    ensure!(
        !staging_dir.exists(),
        "event fixture staging directory already exists"
    );
    fs::create_dir_all(&staging_dir)?;
    for (name, payload) in &payloads {
        write_capture_json(&staging_dir.join(name), payload)?;
    }
    fs::write(
        staging_dir.join("reassembled_events.hex"),
        format!("0x{}\n", hex::encode(&events)),
    )?;
    let event_hash = hex::encode(digest::digest(&SHA256, &events).as_ref());
    let manifest = json!({
        "captured_at": jiff::Timestamp::now().to_string(),
        "deployment": "testnet",
        "endpoint_role": "finalized_event_storage",
        "rpc_url": rpc_url,
        "block_hash": block_hash,
        "block_number": block_number,
        "finalized_head_hash_at_capture": finalized_hash,
        "finalized_head_number_at_capture": finalized_height,
        "runtime_identity": {
            "genesis_hash": DEEPX_TESTNET_GENESIS_HASH,
            "metadata_sha256": METADATA_SHA256,
            "spec_version": SPEC_VERSION,
            "transaction_version": TRANSACTION_VERSION,
        },
        "storage": {
            "layout": layout,
            "standard_events_key": SYSTEM_EVENTS_STORAGE_KEY,
            "threads_key": threads_key,
            "events_map_keys": map_keys,
            "event_count": count,
            "reassembled_bytes": events.len(),
            "reassembled_sha256": event_hash,
        },
        "rpc_payloads": payloads.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        "reassembled_payload_path": "reassembled_events.hex",
    });
    write_capture_json(&staging_dir.join("manifest.json"), &manifest)?;
    fs::rename(&staging_dir, &fixture_dir)?;
    println!("captured finalized events: {}", fixture_dir.display());
    Ok(())
}

async fn capture_storage_rpc(
    client: &DeepXRpcClient,
    key: &str,
    block_hash: &str,
) -> anyhow::Result<Value> {
    capture_rpc(client, "state_getStorage", json!([key, block_hash])).await
}

async fn capture_storage_repeat(
    client: &DeepXRpcClient,
    key: &str,
    block_hash: &str,
    expected: &str,
    base: &str,
    payloads: &mut Vec<(String, Value)>,
) -> anyhow::Result<()> {
    let repeated = capture_storage_rpc(client, key, block_hash).await?;
    ensure!(
        repeated["result"] == expected,
        "repeated storage read disagreed"
    );
    let query = capture_rpc(client, "state_queryStorageAt", json!([[key], block_hash])).await?;
    ensure!(
        query["result"][0]["block"] == block_hash
            && query["result"][0]["changes"][0][0] == key
            && query["result"][0]["changes"][0][1] == expected,
        "state_queryStorageAt disagreed with state_getStorage"
    );
    payloads.push((format!("{base}_repeated.json"), repeated));
    payloads.push((format!("{base}_query_storage_at.json"), query));
    Ok(())
}

async fn capture_rpc(
    client: &DeepXRpcClient,
    method: &str,
    params: Value,
) -> anyhow::Result<Value> {
    let response = client
        .execute_rpc_response::<Value>(json!({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params,
        }))
        .await?;
    ensure!(
        response.jsonrpc.as_deref() == Some("2.0")
            && response.id == Some(1)
            && response.error.is_none()
            && response.code.is_none(),
        "invalid JSON-RPC response for {method}"
    );
    Ok(json!({"jsonrpc": "2.0", "id": 1, "result": response.result}))
}

fn write_capture_json(path: &Path, value: &Value) -> anyhow::Result<()> {
    fs::write(path, format!("{}\n", serde_json::to_string_pretty(value)?))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, routing::post};

    use super::*;

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn captures_complete_event_fixture(#[case] sharded: bool) {
        let metadata: Value = serde_json::from_str(include_str!(
            "../test_data/runtime/testnet/genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json"
        )).unwrap();
        let version: Value = serde_json::from_str(include_str!(
            "../test_data/runtime/testnet/genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/runtime_version.json"
        )).unwrap();
        let storage: Value = serde_json::from_str(include_str!(
            "../test_data/runtime/testnet/finalized-events-19592700-spec376/system_events.json"
        ))
        .unwrap();
        let manifest: Value = serde_json::from_str(include_str!(
            "../test_data/runtime/testnet/finalized-events-19592700-spec376/manifest.json"
        ))
        .unwrap();
        let block_hash = manifest["block_hash"].as_str().unwrap().to_string();
        let event_hex = storage["result"].as_str().unwrap().to_string();
        let threads_key = system_threads_storage_key(19_592_700);
        let map_key = system_events_map_storage_key(19_592_700, 0);
        let app = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let metadata = metadata.clone();
                let version = version.clone();
                let block_hash = block_hash.clone();
                let event_hex = event_hex.clone();
                let threads_key = threads_key.clone();
                let map_key = map_key.clone();
                async move {
                    let key = request["params"][0]
                        .as_str()
                        .or_else(|| request["params"][0][0].as_str())
                        .unwrap_or_default();
                    let stored = if key == SYSTEM_EVENTS_STORAGE_KEY && !sharded
                        || key == map_key && sharded
                    {
                        json!(event_hex)
                    } else if key == threads_key && sharded {
                        json!("0x00")
                    } else {
                        Value::Null
                    };
                    let result = match request["method"].as_str().unwrap() {
                        "chain_getFinalizedHead" => json!(block_hash),
                        "chain_getHeader" => json!({"number": "0x12af5fc"}),
                        "chain_getBlockHash" => json!(block_hash),
                        "state_getRuntimeVersion" => version["result"].clone(),
                        "state_getMetadata" => metadata["result"].clone(),
                        "state_getStorage" => stored.clone(),
                        "state_queryStorageAt" => json!([{
                            "block": block_hash,
                            "changes": [[request["params"][0][0], stored]],
                        }]),
                        method => panic!("unexpected RPC method: {method}"),
                    };
                    Json(json!({"jsonrpc": "2.0", "id": 1, "result": result}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let root = std::env::temp_dir().join(format!(
            "deepx-sdk-capture-test-{sharded}-{}-{}",
            std::process::id(),
            jiff::Timestamp::now().as_nanosecond()
        ));
        let rpc_url = format!("http://{address}");
        let client = DeepXRpcClient::new(rpc_url.clone());
        let result = capture_events(&client, &rpc_url, 19_592_700, &root).await;
        server.abort();
        result.unwrap();
        let dir = root.join("finalized-events-19592700-spec376");
        let captured: Value =
            serde_json::from_slice(&fs::read(dir.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(captured["rpc_url"], rpc_url);
        assert_eq!(
            captured["storage"]["event_count"],
            manifest["storage"]["event_count"]
        );
        assert_eq!(
            captured["storage"]["reassembled_sha256"],
            manifest["storage"]["reassembled_sha256"]
        );
        if sharded {
            assert_eq!(
                captured["storage"]["layout"],
                "System.Threads+System.EventsMap"
            );
            assert_eq!(
                captured["storage"]["events_map_keys"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(captured["rpc_payloads"].as_array().unwrap().len(), 7);
        } else {
            assert_eq!(captured["storage"], manifest["storage"]);
            assert_eq!(captured["rpc_payloads"], manifest["rpc_payloads"]);
        }
        assert_eq!(
            fs::read_to_string(dir.join("reassembled_events.hex")).unwrap(),
            include_str!(
                "../test_data/runtime/testnet/finalized-events-19592700-spec376/reassembled_events.hex"
            )
        );
        fs::remove_dir_all(root).unwrap();
    }
}

async fn verify_wallet(client: &DeepXRpcClient, wallet: &str) -> anyhow::Result<()> {
    decode_account_id20(wallet).context("invalid wallet AccountId20")?;
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let directory = transport
        .get_wallet_subaccounts(DEEPX_TESTNET_REST_URL, wallet)
        .await?;
    ensure!(
        directory.wallet().eq_ignore_ascii_case(wallet),
        "wallet directory belongs to another wallet"
    );
    ensure!(!directory.addresses.is_empty(), "wallet directory is empty");
    let subaccounts = directory
        .addresses
        .iter()
        .map(|address| decode_account_id20(address).context("invalid directory subaccount"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let unique = subaccounts.iter().copied().collect::<HashSet<_>>();
    ensure!(
        unique.len() == subaccounts.len(),
        "duplicate directory subaccount"
    );

    let block_hash = get_finalized_head(client).await?;
    verify_read_only_runtime_at(client, &block_hash).await?;
    let header: DeepXRpcBlockHeader = client.get_header_once(&block_hash).await?;
    let block_number =
        decode_block_number_u64(&header.number).context("invalid finalized height")?;
    let block = decode_prefixed_hash(&block_hash).context("invalid finalized hash")?;
    let timestamp_ms = verify_account_checkpoint(client, &block_hash, block, block_number).await?;
    for subaccount in subaccounts {
        verify_margin(client, block, subaccount).await?;
        verify_positions(client, block, subaccount).await?;
        verify_orders(client, block, subaccount).await?;
        verify_spot_orders(client, block, subaccount).await?;
        verify_lending(client, block, block_number, subaccount).await?;
        println!(
            "finalized-wallet raw subaccount=0x{}",
            hex::encode(subaccount)
        );
    }
    ensure!(
        get_timestamp_now(client, block).await? == timestamp_ms,
        "wallet account timestamp changed at the pinned block"
    );
    let final_directory = transport
        .get_wallet_subaccounts(DEEPX_TESTNET_REST_URL, wallet)
        .await?;
    let final_subaccounts = final_directory
        .addresses
        .iter()
        .map(|address| decode_account_id20(address).context("invalid final directory subaccount"))
        .collect::<anyhow::Result<HashSet<_>>>()?;
    ensure!(
        final_directory.wallet().eq_ignore_ascii_case(wallet)
            && final_subaccounts.len() == final_directory.addresses.len()
            && final_subaccounts == unique,
        "wallet directory changed during finalized reads"
    );
    let canonical = client.get_block_hash_required_once(block_number).await?;
    ensure!(
        canonical.eq_ignore_ascii_case(&block_hash),
        "finalized wallet block hash changed during the read"
    );
    println!(
        "finalized-wallet raw reads verified: wallet={wallet} subaccounts={} block={block_number} hash={block_hash}",
        unique.len()
    );
    Ok(())
}

async fn verify_finalized_wallet_trades(
    wallet: &str,
    trades: &[DeepXFinalizedPerpTrade],
) -> anyhow::Result<usize> {
    decode_account_id20(wallet).context("invalid wallet AccountId20")?;
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let directory = transport
        .get_wallet_subaccounts(DEEPX_TESTNET_REST_URL, wallet)
        .await?;
    ensure!(
        directory.wallet().eq_ignore_ascii_case(wallet),
        "wallet directory belongs to another wallet"
    );
    let subaccounts = directory
        .addresses
        .iter()
        .map(|address| decode_account_id20(address).context("invalid wallet subaccount"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let unique = subaccounts.iter().copied().collect::<HashSet<_>>();
    ensure!(
        !unique.is_empty() && unique.len() == subaccounts.len(),
        "wallet directory is empty or contains duplicates"
    );

    let mut relevant = BTreeMap::<u64, BTreeSet<String>>::new();
    for trade in trades {
        for subaccount in &subaccounts {
            if trade.buyer == *subaccount || trade.seller == *subaccount {
                relevant
                    .entry(u64::from(trade.market_id))
                    .or_default()
                    .insert(format!("0x{}", hex::encode(subaccount)));
            }
        }
    }
    let group_count = relevant.values().map(BTreeSet::len).sum::<usize>();
    ensure!(group_count > 0, "no finalized trades involve the wallet");
    ensure!(
        group_count <= MAX_RELEVANT_GROUPS,
        "finalized wallet trade group budget exceeded"
    );

    let mut complete = Vec::with_capacity(relevant.len());
    let mut metadata = Vec::with_capacity(relevant.len());
    for (market_id, addresses) in relevant {
        let request = DeepXPerpWalletTradesRequest {
            wallet: wallet.to_string(),
            market_name: None,
            market_id: Some(market_id),
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(100),
        };
        let snapshot = transport
            .get_perp_wallet_trades(DEEPX_TESTNET_REST_URL, &request)
            .await?;
        let market = snapshot
            .into_iter()
            .find(|market| market.market_id == market_id)
            .context("relevant perpetual wallet market is absent")?;
        let mut groups = Vec::with_capacity(addresses.len());
        for address in addresses {
            let first = market
                .subaccounts
                .iter()
                .find(|group| group.subaccount.eq_ignore_ascii_case(&address))
                .context("relevant perpetual wallet subaccount is absent")?;
            let page = if first.trades.has_next {
                let pages = transport
                    .get_perp_wallet_trade_pages_for_subaccount(
                        DEEPX_TESTNET_REST_URL,
                        &request,
                        &address,
                        &market.market_name,
                        MAX_PAGES_PER_GROUP,
                    )
                    .await?;
                ensure!(
                    pages.first() == Some(&first.trades),
                    "perpetual wallet first page changed during traversal"
                );
                DeepXAccountPage {
                    items: pages.into_iter().flat_map(|page| page.items).collect(),
                    next_cursor: None,
                    has_next: false,
                }
            } else {
                first.trades.clone()
            };
            groups.push(DeepXPerpWalletSubaccountTrades {
                subaccount: address,
                trades: page,
            });
        }
        let latest = transport
            .get_perp_wallet_trades(DEEPX_TESTNET_REST_URL, &request)
            .await?;
        let latest_market = latest
            .iter()
            .find(|candidate| candidate.market_id == market_id)
            .context("relevant perpetual wallet market disappeared")?;
        ensure!(
            latest_market.market_name == market.market_name,
            "perpetual wallet market name changed during traversal"
        );
        for group in &groups {
            let initial = market
                .subaccounts
                .iter()
                .find(|candidate| candidate.subaccount.eq_ignore_ascii_case(&group.subaccount))
                .context("initial perpetual wallet subaccount disappeared")?;
            let current = latest_market
                .subaccounts
                .iter()
                .find(|candidate| candidate.subaccount.eq_ignore_ascii_case(&group.subaccount))
                .context("perpetual wallet subaccount disappeared during traversal")?;
            ensure!(
                current.trades == initial.trades,
                "perpetual wallet first page changed during traversal"
            );
        }
        metadata.push(
            transport
                .get_perp_market_by_id(DEEPX_TESTNET_REST_URL, market_id)
                .await?,
        );
        complete.push(DeepXPerpWalletTradeMarket {
            market_id,
            market_name: market.market_name,
            subaccounts: groups,
        });
    }

    let final_directory = transport
        .get_wallet_subaccounts(DEEPX_TESTNET_REST_URL, wallet)
        .await?;
    let final_subaccounts = final_directory
        .addresses
        .iter()
        .map(|address| decode_account_id20(address).context("invalid final wallet subaccount"))
        .collect::<anyhow::Result<HashSet<_>>>()?;
    ensure!(
        final_directory.wallet().eq_ignore_ascii_case(wallet)
            && final_subaccounts.len() == final_directory.addresses.len()
            && final_subaccounts == unique,
        "wallet directory changed during finalized trade comparison"
    );
    let matched = match_finalized_wallet_perp_trades(trades, &complete, &subaccounts, &metadata)?;
    Ok(matched.len())
}

async fn verify_positions(
    client: &DeepXRpcClient,
    block: [u8; 32],
    subaccount: [u8; 20],
) -> anyhow::Result<()> {
    let (maximum, positions) = get_perp_positions_at_block(client, block, subaccount).await?;
    println!(
        "perpetual positions raw: runtime_max={maximum} count={}",
        positions.len()
    );
    for position in positions {
        println!(
            "market={} owner=0x{} long={} base_amount={} entry_price={} leverage={} version={}",
            position.market_id,
            hex::encode(position.owner),
            position.is_long,
            position.base_asset_amount,
            position.entry_price,
            position.leverage,
            position.version
        );
    }
    Ok(())
}

async fn verify_orders(
    client: &DeepXRpcClient,
    block: [u8; 32],
    subaccount: [u8; 20],
) -> anyhow::Result<()> {
    let orders = get_active_perp_orders_at_block(client, block, subaccount).await?;
    println!("active perpetual orders raw: count={}", orders.len());
    for order in orders {
        println!(
            "market={} order_id={} owner=0x{} long={} size={} price={} status={:?}",
            order.market_id,
            order.order_id,
            hex::encode(order.owner),
            order.is_long,
            order.size,
            order.price,
            order.status
        );
    }
    Ok(())
}

async fn verify_spot_orders(
    client: &DeepXRpcClient,
    block: [u8; 32],
    subaccount: [u8; 20],
) -> anyhow::Result<()> {
    let index = get_spot_orders_by_id_at_block(client, block, subaccount, &[]).await?;
    let orders =
        get_spot_orders_by_id_at_block(client, block, subaccount, &index.active_order_ids).await?;
    ensure!(
        index.active_order_ids == orders.active_order_ids,
        "Spot active-order index changed at a fixed block hash"
    );
    println!("active Spot orders raw: count={}", orders.orders.len());
    for order in orders.orders {
        println!("spot_order={order:?}");
    }
    Ok(())
}

async fn prepare_perp_history(
    subaccount: [u8; 20],
    market_id: u64,
) -> anyhow::Result<PreparedPerpHistory> {
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let market = transport
        .get_perp_markets(DEEPX_TESTNET_REST_URL)
        .await?
        .into_iter()
        .find(|market| market.id == market_id)
        .context("perpetual market is absent from REST directory")?;
    let address = format!("0x{}", hex::encode(subaccount));
    let records = transport
        .get_perp_history_order_pages(
            DEEPX_TESTNET_REST_URL,
            &DeepXPerpHistoryOrdersRequest {
                subaccount: address,
                market_id: Some(market_id),
                cursor: None,
                page_size: Some(100),
                sort: DeepXAccountSortOrder::Ascending,
            },
            100,
        )
        .await?
        .into_iter()
        .flat_map(|page| page.items)
        .collect::<Vec<_>>();
    let ids = records
        .iter()
        .map(|record| {
            record
                .order_id
                .parse::<u64>()
                .context("invalid perpetual order ID")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(PreparedPerpHistory {
        market_id,
        base_decimal: market.base_decimal,
        records,
        ids,
    })
}

async fn verify_perp_history(
    client: &DeepXRpcClient,
    block: [u8; 32],
    subaccount: [u8; 20],
    history: &PreparedPerpHistory,
) -> anyhow::Result<()> {
    let PreparedPerpHistory {
        market_id,
        base_decimal,
        records,
        ids,
    } = history;
    let orders = get_perp_orders_by_id_at_block(client, block, subaccount, ids).await?;
    ensure!(
        orders.len() == records.len(),
        "perpetual REST and chain order counts differ"
    );
    for record in records {
        let id = record.order_id.parse::<u64>()?;
        let order = orders
            .iter()
            .find(|order| order.order_id == id)
            .context("REST perpetual order is absent from finalized storage")?;
        ensure!(
            order.owner == subaccount
                && u64::from(order.market_id) == record.market_id
                && order.create_time == record.height,
            "REST and finalized perpetual order identity differs: {id}"
        );
        let decimal = |value: u128, scale: u32| -> anyhow::Result<rust_decimal::Decimal> {
            Ok(rust_decimal::Decimal::from_i128_with_scale(
                i128::try_from(value)?,
                scale,
            ))
        };
        ensure!(
            record.size == decimal(order.size, u32::from(*base_decimal))?
                && record.size_filled == decimal(order.size_filled, u32::from(*base_decimal))?
                && record.size_remain == decimal(order.size_remain, u32::from(*base_decimal))?
                && record.reduce_only == order.reduce_only
                && record.is_long == order.is_long,
            "REST and finalized perpetual order quantities or flags differ: {id}"
        );
        ensure!(
            record.leverage
                == rust_decimal::Decimal::from_i128_with_scale(i128::from(order.leverage), 3),
            "REST and finalized leverage differ: {id}"
        );
        let (kind, slippage) = match &order.order_type {
            DeepXStoredPerpOrderType::Limit(_) => ("Limit", rust_decimal::Decimal::ZERO),
            DeepXStoredPerpOrderType::Market(slippage) => (
                "Market",
                rust_decimal::Decimal::from(slippage.unwrap_or_default()),
            ),
            DeepXStoredPerpOrderType::Stop => ("Stop", rust_decimal::Decimal::ZERO),
        };
        ensure!(
            record.order_type == kind && record.slippage == slippage,
            "REST and finalized order type or slippage differ: {id}"
        );
        let status = match order.status {
            DeepXStoredOrderStatus::Open => "Open",
            DeepXStoredOrderStatus::PartiallyFilled => "PartiallyFilled",
            DeepXStoredOrderStatus::Filled => "Filled",
            DeepXStoredOrderStatus::Canceled => "Canceled",
            DeepXStoredOrderStatus::Rejected => "Rejected",
            DeepXStoredOrderStatus::Expired => "Expired",
        };
        let post_only = match order.post_only {
            DeepXStoredPostOnlyParam::None => "None",
            DeepXStoredPostOnlyParam::MustPostOnly => "MustPostOnly",
            DeepXStoredPostOnlyParam::Adaptive => "Adaptive",
        };
        ensure!(
            record.status == status && record.post_only == post_only,
            "REST and finalized lifecycle fields differ: {id}"
        );
        ensure!(
            record.take_profit
                == order
                    .take_profit
                    .map(|value| decimal(value, 6))
                    .transpose()?
                && record.stop_loss
                    == order.stop_loss.map(|value| decimal(value, 6)).transpose()?,
            "REST and finalized trigger fields differ: {id}"
        );
        if matches!(order.order_type, DeepXStoredPerpOrderType::Limit(_)) {
            ensure!(
                record.price == decimal(order.price, 6)?,
                "REST and finalized limit price differs: {id}"
            );
        }
        println!("perp_history_order={order:?}");
    }
    println!(
        "finalized perpetual history verified: market={market_id} records={}",
        orders.len()
    );
    Ok(())
}

async fn prepare_spot_history(subaccount: [u8; 20]) -> anyhow::Result<Vec<u64>> {
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let address = format!("0x{}", hex::encode(subaccount));
    transport
        .get_spot_history_order_pages(
            DEEPX_TESTNET_REST_URL,
            &DeepXSpotHistoryOrdersRequest {
                subaccount: address,
                name: None,
                pair: None,
                order_side: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Ascending,
                page_size: Some(100),
            },
            100,
        )
        .await?
        .into_iter()
        .flat_map(|page| page.items)
        .map(|record| {
            record
                .order_id
                .parse::<u64>()
                .context("invalid Spot order ID")
        })
        .collect::<anyhow::Result<Vec<_>>>()
}

async fn verify_spot_history(
    client: &DeepXRpcClient,
    block: [u8; 32],
    subaccount: [u8; 20],
    ids: &[u64],
) -> anyhow::Result<()> {
    let evidence = get_spot_orders_by_id_at_block(client, block, subaccount, ids).await?;
    ensure!(
        evidence.orders.len() == ids.len(),
        "Spot REST and chain order counts differ"
    );
    println!(
        "finalized Spot order IDs verified: requested={} active={}",
        evidence.orders.len(),
        evidence.active_order_ids.len()
    );
    for order in evidence.orders {
        println!("spot_history_order={order:?}");
    }
    Ok(())
}

async fn verify_lending(
    client: &DeepXRpcClient,
    block: [u8; 32],
    block_number: u64,
    subaccount: [u8; 20],
) -> anyhow::Result<()> {
    let block_number = u32::try_from(block_number).context("finalized height exceeds u32")?;
    let positions = get_lending_positions_at_block(client, block, subaccount).await?;
    println!("lending positions raw: count={}", positions.len());
    for position in positions {
        let asset = get_lending_asset_at_block(
            client,
            block,
            subaccount,
            position.market_id,
            &position.asset,
        )
        .await?;
        let max_withdrawal = if position.scaled_deposit == 0 {
            None
        } else {
            Some(
                get_max_withdrawal_at_block_number(
                    client,
                    block_number,
                    subaccount,
                    position.market_id,
                    &position.asset,
                )
                .await?,
            )
        };
        ensure!(
            lending_asset_reads_match_position(&position, &asset, max_withdrawal),
            "lending asset reads conflict with the account position"
        );
        println!(
            "market={} asset=0x{} scaled_deposit={} scaled_borrow={} effective_deposit={:?} effective_borrow={:?} max_withdrawal={:?}",
            position.market_id,
            hex::encode(position.asset),
            position.scaled_deposit,
            position.scaled_borrow,
            asset.effective_deposit,
            asset.effective_borrow,
            max_withdrawal
        );
    }
    Ok(())
}
