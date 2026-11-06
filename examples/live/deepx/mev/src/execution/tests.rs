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

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::{Json, Router, extract::State as RpcState, routing::post};
use parity_scale_codec::{Compact, Encode};
use rstest::rstest;
use subxt_core::ext::scale_value::{Composite, ValueDef, scale::encode_as_type};

use super::*;

static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "deepx-mev-test-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn read_checkpoint(path: &std::path::Path) -> State {
    let checkpoint: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    serde_json::from_value(checkpoint["state"].clone()).unwrap()
}

fn metadata_hex() -> String {
    let response: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../../deepx-rust-sdk/test_data/runtime/testnet/",
        "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
    )))
    .unwrap();
    response["result"].as_str().unwrap().to_owned()
}

fn native_metadata_hex() -> String {
    let response: Value =
        serde_json::from_str(include_str!("../../test_data/spec380_metadata.json")).unwrap();
    response["result"].as_str().unwrap().to_owned()
}

fn runtime() -> RuntimeSnapshot {
    runtime_for(false)
}

fn runtime_for(modern: bool) -> RuntimeSnapshot {
    RuntimeSnapshot::approved_testnet(
        &DeepXEnvironment::Testnet,
        super::super::parse_hash(deepx_rust_sdk::deployment::DEEPX_TESTNET_GENESIS_HASH).unwrap(),
        if modern { 380 } else { 376 },
        1,
        &decode_hex(&if modern {
            native_metadata_hex()
        } else {
            metadata_hex()
        })
        .unwrap(),
    )
    .unwrap()
}

fn set_field(value: &mut ScaleValue<u32>, name: &str, replacement: ScaleValue) {
    let ValueDef::Composite(Composite::Named(fields)) = &mut value.value else {
        panic!("named value required")
    };
    let (_, field) = fields.iter_mut().find(|(field, _)| field == name).unwrap();
    *field = replacement.map_context(|()| field.context);
}

fn market_storage() -> String {
    market_storage_for(false)
}

fn market_storage_for(modern: bool) -> String {
    let runtime = runtime();
    let metadata = runtime.metadata();
    let response: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),"/../../../../deepx-rust-sdk/test_data/runtime/testnet/",
        "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/spot_market_eth_usdc.json",
    ))).unwrap();
    let bytes = decode_hex(response["result"].as_str().unwrap()).unwrap();
    let ty = metadata
        .pallet_by_name("SpotMarket")
        .unwrap()
        .storage()
        .unwrap()
        .entry_by_name("SpotMarkets")
        .unwrap()
        .entry_type()
        .value_ty();
    let mut market = decode_as_type(&mut &bytes[..], ty, metadata.types()).unwrap();
    for (name, value) in [
        ("base_decimal", 3),
        ("quote_decimal", 6),
        ("taker_fee_rate", 10_000),
    ] {
        set_field(&mut market, name, ScaleValue::u128(value));
    }
    set_field(&mut market, "pair", ScaleValue::from_bytes([3; 32]));
    set_field(&mut market, "is_paused", ScaleValue::bool(false));
    let spec = ScaleValue::named_composite([
        ("min_qty", ScaleValue::u128(1)),
        ("step_size", ScaleValue::u128(1)),
        ("tick_size", ScaleValue::u128(1_000)),
        ("min_notional", ScaleValue::u128(1)),
    ]);
    set_field(&mut market, "order_spec", spec);
    let mut encoded_bytes = Vec::new();
    let destination = runtime_for(modern);
    let destination_type = destination
        .metadata()
        .pallet_by_name("SpotMarket")
        .unwrap()
        .storage()
        .unwrap()
        .entry_by_name("SpotMarkets")
        .unwrap()
        .entry_type()
        .value_ty();
    encode_as_type(
        &market,
        destination_type,
        destination.metadata().types(),
        &mut encoded_bytes,
    )
    .unwrap();
    encoded(encoded_bytes)
}

fn account_storage(signer: [u8; 20]) -> String {
    account_storage_for(signer, false)
}

fn account_storage_for(signer: [u8; 20], modern: bool) -> String {
    let runtime = runtime_for(modern);
    let metadata = runtime.metadata();
    let address = dynamic::storage("System", "Account", vec![ScaleValue::from_bytes(signer)]);
    let mut account = storage::default_value(&address, metadata)
        .unwrap()
        .to_value()
        .unwrap();
    set_field(&mut account, "quota", ScaleValue::u128(100));
    let mut bytes = Vec::new();
    encode_as_type(&account, account.context, metadata.types(), &mut bytes).unwrap();
    encoded(bytes)
}

fn depth(after_entry: bool) -> String {
    let level = |price: u128, size: u128| {
        (
            U256::from(price).to_le_bytes::<32>(),
            U256::from(size).to_le_bytes::<32>(),
            1_u32,
        )
    };
    let bids = vec![level(if after_entry { 110_000 } else { 90_000 }, 10_000)];
    let asks = if after_entry {
        Vec::new()
    } else {
        vec![level(100_000, 1_000)]
    };
    encoded(std::collections::BTreeMap::from([([3_u8; 32], (bids, asks))]).encode())
}

fn fixture(directory: &Directory) -> (Execution, State) {
    fixture_for(directory, false)
}

fn fixture_for(directory: &Directory, modern: bool) -> (Execution, State) {
    let runtime = || runtime_for(modern);
    let key = DeepXPrivateKey::new(&"11".repeat(32), &DeepXKeyScheme::Secp256k1).unwrap();
    let signer = derive_signer_account_id(&key).unwrap();
    let execution = Execution {
        journal: Journal::open(&directory.0.join("journal.json")).unwrap(),
        settings: Settings {
            journal: directory.0.join("journal.json"),
            subaccount: [1; 20],
            wallet: None,
            maximum_loss: 500,
            exit_after_blocks: 3,
        },
        key,
        signer,
        genesis: runtime().identity().genesis_hash,
        pair: [3; 32],
        _account_lock: lock_account(&directory.0.join("account.lock")).unwrap(),
        _wallet_lock: None,
    };
    let entry = execution
        .sign(runtime(), true, 1_000, 100_000, 1_725_000_000_001)
        .unwrap();
    let target_key = DeepXPrivateKey::new(&"22".repeat(32), &DeepXKeyScheme::Secp256k1).unwrap();
    let mut target_params = execution.params(true, 10_000, 1_100_000);
    target_params.subaccount = [2; 20];
    target_params.order_type = DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc);
    let service = DeepXRuntimeSnapshotService::new(runtime());
    let target = sign_spot_place_order(
        &service.acquire().unwrap(),
        &target_key,
        target_params,
        entry.nonce + 1,
    )
    .unwrap();
    let state = State {
        version: 1,
        genesis: encoded(execution.genesis),
        signer: encoded(signer),
        wallet: None,
        subaccount: encoded([1; 20]),
        pair: encoded([3; 32]),
        runtime_code_hash: encoded([9; 32]),
        target_bytes: encoded(target.bytes()),
        target_hash: encoded(target.extrinsic_hash()),
        target_nonce: entry.nonce + 1,
        target_subaccount: encoded([2; 20]),
        target_outcome: None,
        target_first_trade: None,
        entry_first_trade: None,
        entry_last_trade: None,
        cursor_number: 100,
        cursor_hash: encoded([100; 32]),
        exit_after_blocks: 3,
        maximum_loss: "500".into(),
        cost_budget: "100".into(),
        minimum_profit: "1".into(),
        exit_price: "110000".into(),
        base_scale: "1000".into(),
        base_symbol: b"ETH".to_vec(),
        quote_symbol: b"USDC".to_vec(),
        acquired_base: "0".into(),
        spent_quote: "0".into(),
        sold_base: "0".into(),
        received_quote: "0".into(),
        entry,
        exits: Vec::new(),
        complete: false,
        result: None,
    };
    (execution, state)
}

fn inclusion(success: bool) -> Inclusion {
    Inclusion {
        number: 101,
        hash: encoded([101; 32]),
        index: 0,
        success,
    }
}

fn event_record(
    metadata: &Metadata,
    pallet: &str,
    name: &str,
    fields: ScaleValue,
    index: u32,
) -> Vec<u8> {
    let pallet = metadata.pallet_by_name(pallet).unwrap();
    let event = pallet
        .event_variants()
        .unwrap()
        .iter()
        .find(|event| event.name == name)
        .unwrap();
    let ValueDef::Composite(Composite::Named(values)) = fields.value else {
        panic!("named event fields required")
    };
    let mut bytes = Phase::ApplyExtrinsic(index).encode();
    bytes.extend([pallet.index(), event.index]);
    for field in &event.fields {
        let (_, value) = values
            .iter()
            .find(|(name, _)| Some(name) == field.name.as_ref())
            .unwrap();
        encode_as_type(value, field.ty.id, metadata.types(), &mut bytes).unwrap();
    }
    Vec::<[u8; 32]>::new().encode_to(&mut bytes);
    bytes
}

fn dispatch_record(metadata: &Metadata, index: u32, success: bool) -> Vec<u8> {
    let info = ScaleValue::named_composite([
        (
            "weight",
            ScaleValue::named_composite([
                ("ref_time", ScaleValue::u128(0)),
                ("proof_size", ScaleValue::u128(0)),
            ]),
        ),
        (
            "call_type",
            ScaleValue::unnamed_variant("Timestamp", [ScaleValue::u128(0)]),
        ),
        (
            "priority",
            ScaleValue::unnamed_composite([ScaleValue::u128(0), ScaleValue::u128(80)]),
        ),
        ("class", ScaleValue::unnamed_variant("Normal", [])),
        ("pays_fee", ScaleValue::unnamed_variant("Yes", [])),
    ]);
    let mut fields = vec![("dispatch_info", info)];
    if !success {
        fields.push(("dispatch_error", ScaleValue::unnamed_variant("Other", [])));
    }
    event_record(
        metadata,
        "System",
        if success {
            "ExtrinsicSuccess"
        } else {
            "ExtrinsicFailed"
        },
        ScaleValue::named_composite(fields),
        index,
    )
}

fn u256(value: u128) -> ScaleValue {
    ScaleValue::unnamed_composite([ScaleValue::unnamed_composite([
        ScaleValue::u128(value & u128::from(u64::MAX)),
        ScaleValue::u128(value >> 64),
        ScaleValue::u128(0),
        ScaleValue::u128(0),
    ])])
}

fn place_record(
    metadata: &Metadata,
    execution: &Execution,
    operation: &Operation,
    is_buy: bool,
    index: u32,
) -> Vec<u8> {
    let mut order = ScaleValue::named_composite([
        ("id", ScaleValue::u128(u128::from(operation.nonce))),
        (
            "maker",
            ScaleValue::from_bytes(execution.settings.subaccount),
        ),
        ("pair", ScaleValue::from_bytes(execution.pair)),
        (
            "price",
            ScaleValue::unnamed_variant("Some", [u256(100_000)]),
        ),
        ("quote_amount", u256(amount(&operation.quote).unwrap())),
        ("base_amount", u256(amount(&operation.base).unwrap())),
        ("create_time", ScaleValue::u128(42)),
        ("status", ScaleValue::unnamed_variant("Open", [])),
        (
            "order_type",
            ScaleValue::unnamed_variant("Limit", [ScaleValue::unnamed_variant("IOC", [])]),
        ),
        ("post_only", ScaleValue::unnamed_variant("None", [])),
        ("reduce_only", ScaleValue::bool(false)),
        ("is_buy", ScaleValue::bool(is_buy)),
    ]);
    if native::uses_pair_storage(metadata).unwrap() {
        let ValueDef::Composite(Composite::Named(fields)) = &mut order.value else {
            panic!("named order required")
        };
        fields.push(("stp_mode".into(), ScaleValue::unnamed_variant("None", [])));
    }
    event_record(
        metadata,
        "SpotMarket",
        if is_buy {
            "StateOrderBuy"
        } else {
            "StateOrderSell"
        },
        ScaleValue::named_composite([("order", order)]),
        index,
    )
}

fn event_bytes(records: Vec<Vec<u8>>) -> Vec<u8> {
    let mut bytes = Compact(u32::try_from(records.len()).unwrap()).encode();
    for record in records {
        bytes.extend(record);
    }
    bytes
}

#[rstest]
fn signed_ioc_and_inclusion_bind_exact_terms_and_dispatch_outcomes() {
    let directory = Directory::new();
    let (execution, mut state) = fixture(&directory);
    let runtime = runtime();
    validate_storage_interface(runtime.metadata()).unwrap();
    let decoded = crate::decode_order(
        runtime.metadata(),
        decode_hex(&state.entry.bytes).unwrap(),
        &crate::Market::Spot(execution.pair),
        1,
    )
    .unwrap()
    .unwrap();
    assert_eq!(decoded["time_in_force"], "IOC");
    assert_eq!(decoded["signer"], encoded(execution.signer));
    assert_eq!(decoded["raw_base_size"], "1000");
    let events = event_bytes(vec![
        place_record(runtime.metadata(), &execution, &state.entry, true, 0),
        dispatch_record(runtime.metadata(), 0, true),
        dispatch_record(runtime.metadata(), 1, false),
    ]);
    let extrinsics = vec![state.entry.bytes.clone(), state.target_bytes.clone()];
    execution
        .bind_inclusions(
            &mut state,
            &runtime,
            &extrinsics,
            &events,
            101,
            &encoded([101; 32]),
        )
        .unwrap();
    assert!(state.entry.included.as_ref().unwrap().success);
    assert_eq!(state.target_outcome, Some(false));
    assert!(
        execution
            .bind_inclusions(
                &mut state,
                &runtime,
                &extrinsics,
                &events,
                101,
                &encoded([101; 32])
            )
            .is_err()
    );
    let (_, mut conflict) = fixture(&Directory::new());
    conflict.entry.quote = "99999".into();
    assert!(
        execution
            .bind_inclusions(
                &mut conflict,
                &runtime,
                &extrinsics,
                &events,
                101,
                &encoded([101; 32])
            )
            .is_err()
    );
}

fn trade(
    state: &State,
    buy: bool,
    base: u128,
    quote: u128,
    fee: i128,
    index: u32,
) -> DeepXFinalizedSpotTrade {
    DeepXFinalizedSpotTrade {
        block_hash: [101; 32],
        block_number: 101,
        event_index: index,
        pair: [3; 32],
        buyer: if buy { [1; 20] } else { [2; 20] },
        buy_id: if buy {
            state.entry.nonce
        } else {
            state.target_nonce
        },
        seller: if buy { [4; 20] } else { [1; 20] },
        sell_id: if buy {
            40
        } else {
            state.exits.last().unwrap().nonce
        },
        trade_amount_u: U256::from(quote).to_le_bytes(),
        trade_amount_b: U256::from(base).to_le_bytes(),
        taker: if buy { "Buyer" } else { "Seller" }.into(),
        taker_fee: fee,
        taker_fee_asset: if buy {
            b"ETH".to_vec()
        } else {
            b"USDC".to_vec()
        },
        maker_fee: 0,
        maker_fee_asset: if buy {
            b"USDC".to_vec()
        } else {
            b"ETH".to_vec()
        },
        price: U256::from(110_000).to_le_bytes(),
    }
}

#[rstest]
fn partial_fills_use_net_inventory_and_exact_realized_fees() {
    let directory = Directory::new();
    let (execution, mut state) = fixture(&directory);
    state.entry.included = Some(inclusion(true));
    for (base, quote, fee, index) in [(400, 40_000, 4, 4), (600, 60_000, 6, 5)] {
        let fill = trade(&state, true, base, quote, fee, index);
        apply_trade(&mut state, &fill).unwrap();
    }
    assert_eq!(state.remaining().unwrap(), 990);
    assert_eq!(state.spent_quote, "100000");
    state.exits.push(
        execution
            .sign(runtime(), false, 990, 108_900, state.target_nonce + 1)
            .unwrap(),
    );
    state.exits[0].included = Some(inclusion(true));
    for (base, quote, fee, index) in [(400, 44_000, 44, 6), (590, 64_900, 64, 7)] {
        let fill = trade(&state, false, base, quote, fee, index);
        apply_trade(&mut state, &fill).unwrap();
    }
    assert_eq!(state.remaining().unwrap(), 0);
    assert_eq!(state.sold_base, "990");
    assert_eq!(state.received_quote, "108792");
    assert_eq!(state.profit().unwrap(), "8692");
    assert_eq!(state.entry_first_trade, Some((101, 4)));
    assert_eq!(state.target_first_trade, Some((101, 6)));
}

#[rstest]
fn trade_evidence_rejects_fee_asset_conflict_and_overselling() {
    let directory = Directory::new();
    let (execution, mut state) = fixture(&directory);
    let fill = trade(&state, true, 1_000, 100_000, 10, 4);
    assert!(apply_trade(&mut state, &fill).is_err());
    state.entry.included = Some(inclusion(true));
    let mut invalid = fill.clone();
    invalid.taker_fee_asset = b"USDC".to_vec();
    assert!(apply_trade(&mut state, &invalid).is_err());
    apply_trade(&mut state, &fill).unwrap();
    state.exits.push(
        execution
            .sign(runtime(), false, 990, 108_900, state.target_nonce + 1)
            .unwrap(),
    );
    state.exits[0].included = Some(inclusion(true));
    let sell = trade(&state, false, 991, 109_010, 10, 6);
    assert!(apply_trade(&mut state, &sell).is_err());
}

#[rstest]
fn financial_bounds_and_signed_fee_extremes_are_exact() {
    assert_eq!(exact_quote(1_000, 110_000, 1_000).unwrap(), 110_000);
    assert!(exact_quote(999, 3, 1_000).is_err());
    assert_eq!(
        exact_quote(u128::MAX, u128::MAX, u128::MAX).unwrap(),
        u128::MAX
    );
    assert!(exact_quote(u128::MAX, u128::MAX, 1).is_err());
    assert_eq!(loss_floor(101, 30, 1_000, 100).unwrap(), 3_400);
    assert_eq!(loss_floor(0, 30, 1_000, 100).unwrap(), 100);
    assert_eq!(net_received(1_000, 10).unwrap(), 990);
    assert_eq!(net_received(1_000, -10).unwrap(), 1_010);
    assert!(net_received(9, 10).is_err());
    assert!(net_received(u128::MAX, i128::MIN).is_err());
}

#[rstest]
fn journal_restart_preserves_signed_bytes_and_rejects_parallel_writer() {
    let directory = Directory::new();
    let (execution, mut state) = fixture(&directory);
    state.entry.attempted = true;
    state.entry.submission_error = Some("delivery unknown".into());
    execution.journal.store(&state).unwrap();
    assert!(Journal::open(&execution.settings.journal).is_err());
    assert!(lock_account(&directory.0.join("account.lock")).is_err());
    let path = execution.settings.journal.clone();
    drop(execution);
    let journal = Journal::open(&path).unwrap();
    let loaded = journal.load().unwrap().unwrap();
    assert_eq!(loaded.entry.bytes, state.entry.bytes);
    assert_eq!(loaded.entry.hash, state.entry.hash);
    assert_eq!(loaded.entry.nonce, state.entry.nonce);
    assert!(loaded.entry.attempted);
    assert_eq!(
        loaded.entry.submission_error.as_deref(),
        Some("delivery unknown")
    );
    let mut corrupt: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    corrupt["state"]["acquired_base"] = json!("999999");
    std::fs::write(&path, serde_json::to_vec(&corrupt).unwrap()).unwrap();
    assert!(journal.load().unwrap_err().to_string().contains("checksum"));
    std::fs::write(&path, b"{broken").unwrap();
    assert!(journal.load().is_err());
}

#[derive(Clone)]
struct Mock {
    state: State,
    journal_path: PathBuf,
    submissions: Arc<AtomicUsize>,
    outcome: &'static str,
    target_pending: bool,
    progress: Option<Arc<AtomicUsize>>,
    modern: bool,
}

async fn rpc_handler(RpcState(mock): RpcState<Mock>, Json(request): Json<Value>) -> Json<Value> {
    if mock.outcome.starts_with("idle_") {
        assert_eq!(request["method"], "state_getStorage");
        let snapshot = runtime_for(mock.modern);
        let metadata = snapshot.metadata();
        let directory = dynamic::storage("SpotMarket", "PairList", Vec::<ScaleValue>::new());
        let result = if request["params"][0]
            == encoded(storage::get_address_bytes(&directory, metadata).unwrap())
        {
            let value = ScaleValue::unnamed_composite([ScaleValue::named_composite([
                ("pair", ScaleValue::from_bytes([3; 32])),
                ("base_market_id", ScaleValue::u128(1)),
                ("base_symbol", ScaleValue::from_bytes(b"ETH")),
                ("quote_market_id", ScaleValue::u128(2)),
                ("quote_symbol", ScaleValue::from_bytes(b"USDC")),
            ])]);
            let ty = metadata
                .pallet_by_name("SpotMarket")
                .unwrap()
                .storage()
                .unwrap()
                .entry_by_name("PairList")
                .unwrap()
                .entry_type()
                .value_ty();
            let mut bytes = Vec::new();
            encode_as_type(&value, ty, metadata.types(), &mut bytes).unwrap();
            json!(encoded(bytes))
        } else {
            assert_eq!(request["params"][0], native_order_key(metadata));
            match mock.outcome {
                "idle_absent" => Value::Null,
                "idle_empty" => json!("0x00"),
                "idle_existing" => json!(native_active_storage(&mock.state, mock.modern)),
                _ => panic!("unexpected idle outcome"),
            }
        };
        return Json(json!({"jsonrpc":"2.0","id":1,"result":result}));
    }
    if mock.progress.is_some() {
        return progressing_rpc(mock, request).await;
    }
    let result = match request["method"].as_str().unwrap() {
        "chain_getFinalizedHead" => json!(mock.state.cursor_hash),
        "chain_getBlockHash" => {
            if mock.outcome == "reorg" && request["params"][0].is_number() {
                json!(encoded([99; 32]))
            } else {
                json!(mock.state.cursor_hash)
            }
        }
        "chain_getHeader" => json!({"number":"0x64"}),
        "state_getStorageHash" => {
            if mock.outcome == "runtime_changed" {
                json!(encoded([8; 32]))
            } else {
                json!(mock.state.runtime_code_hash)
            }
        }
        "state_getMetadata" => json!(metadata_hex()),
        "state_getStorage" => {
            if request["params"][0]
                == deepx_rust_sdk::rpc::storage_decode::spot_market_storage_key([3; 32])
            {
                json!(market_storage())
            } else {
                json!(account_storage(parse_account(&mock.state.signer).unwrap()))
            }
        }
        "state_call" => json!(depth(false)),
        "state_queryStorageAt" => json!([{"block":request["params"][1],"changes":[[
            deepx_rust_sdk::rpc::timestamp::timestamp_now_storage_key(),encoded(1_725_000_000_000_u64.to_le_bytes())
        ]]}]),
        "author_pendingExtrinsics" => {
            if mock.target_pending {
                json!([mock.state.target_bytes])
            } else {
                json!([])
            }
        }
        "author_submitExtrinsic" => {
            let durable = read_checkpoint(&mock.journal_path);
            assert!(durable.entry.attempted);
            assert_eq!(request["params"][0], durable.entry.bytes);
            mock.submissions.fetch_add(1, Ordering::SeqCst);
            match mock.outcome {
                "accepted" => json!(durable.entry.hash),
                "wrong_hash" => json!(encoded([42; 32])),
                "rpc_error" => {
                    return Json(
                        json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"connection to upstream lost"}}),
                    );
                }
                _ => panic!("unknown submission outcome"),
            }
        }
        method => panic!("unexpected RPC method {method}"),
    };
    Json(json!({"jsonrpc":"2.0","id":1,"result":result}))
}

async fn mock_rpc(mock: Mock) -> (DeepXRpcClient, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().route("/", post(rpc_handler)).with_state(mock);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        DeepXRpcClient::new_with_proxy(format!("http://{address}"), None).unwrap(),
        task,
    )
}

#[rstest]
#[case("accepted")]
#[case("wrong_hash")]
#[case("rpc_error")]
#[tokio::test]
async fn submission_is_durable_and_never_repeated_after_restart(#[case] outcome: &'static str) {
    let directory = Directory::new();
    let (execution, mut state) = fixture(&directory);
    execution.journal.store(&state).unwrap();
    let submissions = Arc::new(AtomicUsize::new(0));
    let mock = Mock {
        state: state.clone(),
        journal_path: execution.settings.journal.clone(),
        submissions: submissions.clone(),
        outcome,
        target_pending: true,
        progress: None,
        modern: false,
    };
    let (rpc, task) = mock_rpc(mock).await;
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        execution.run_inner(&rpc, &mut state, 1),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(submissions.load(Ordering::SeqCst), 1);
    let mut restarted = execution.journal.load().unwrap().unwrap();
    assert!(restarted.entry.attempted);
    assert_eq!(
        restarted.entry.submission_error.is_some(),
        outcome != "accepted"
    );
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        execution.run_inner(&rpc, &mut restarted, 1),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(submissions.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test]
async fn disappeared_target_aborts_before_submission() {
    let directory = Directory::new();
    let (execution, mut state) = fixture(&directory);
    execution.journal.store(&state).unwrap();
    let submissions = Arc::new(AtomicUsize::new(0));
    let mock = Mock {
        state: state.clone(),
        journal_path: execution.settings.journal.clone(),
        submissions: submissions.clone(),
        outcome: "accepted",
        target_pending: false,
        progress: None,
        modern: false,
    };
    let (rpc, task) = mock_rpc(mock).await;
    execution.run_inner(&rpc, &mut state, 1).await.unwrap();
    assert!(state.complete);
    assert_eq!(
        state.result.as_deref(),
        Some("target_left_pool_before_entry")
    );
    assert_eq!(submissions.load(Ordering::SeqCst), 0);
    task.abort();
}

#[rstest]
#[case("idle_absent", true)]
#[case("idle_empty", true)]
#[case("idle_existing", false)]
#[tokio::test]
async fn native_pair_directory_checks_orders_independently_of_legacy_positions(
    #[case] outcome: &'static str,
    #[case] idle: bool,
) {
    let directory = Directory::new();
    let (execution, state) = fixture_for(&directory, true);
    let mock = Mock {
        state,
        journal_path: execution.settings.journal.clone(),
        submissions: Arc::new(AtomicUsize::new(0)),
        outcome,
        target_pending: false,
        progress: None,
        modern: true,
    };
    let (rpc, task) = mock_rpc(mock).await;
    let snapshot = runtime_for(true);
    assert_eq!(
        native::require_idle(&rpc, snapshot.metadata(), &encoded([100; 32]), [1; 20])
            .await
            .is_ok(),
        idle
    );
    task.abort();
}

fn match_record(metadata: &Metadata, trade: &DeepXFinalizedSpotTrade) -> Vec<u8> {
    let fields = ScaleValue::named_composite([
        ("pair", ScaleValue::from_bytes(trade.pair)),
        ("buyer", ScaleValue::from_bytes(trade.buyer)),
        ("seller", ScaleValue::from_bytes(trade.seller)),
        ("buy_id", ScaleValue::u128(u128::from(trade.buy_id))),
        ("sell_id", ScaleValue::u128(u128::from(trade.sell_id))),
        (
            "trade_amount_u",
            u256(narrow(U256::from_le_bytes(trade.trade_amount_u)).unwrap()),
        ),
        (
            "trade_amount_b",
            u256(narrow(U256::from_le_bytes(trade.trade_amount_b)).unwrap()),
        ),
        (
            "taker",
            ScaleValue::unnamed_variant(trade.taker.clone(), []),
        ),
        ("taker_fee", ScaleValue::i128(trade.taker_fee)),
        (
            "taker_fee_asset",
            ScaleValue::from_bytes(&trade.taker_fee_asset),
        ),
        ("maker_fee", ScaleValue::i128(trade.maker_fee)),
        (
            "maker_fee_asset",
            ScaleValue::from_bytes(&trade.maker_fee_asset),
        ),
        (
            "price",
            u256(narrow(U256::from_le_bytes(trade.price)).unwrap()),
        ),
    ]);
    event_record(metadata, "SpotMarket", "OrderMatched", fields, 0)
}

fn native_order_key(metadata: &Metadata) -> String {
    let address = dynamic::storage(
        "SpotMarket",
        "ActiveSpotOrdersFor",
        vec![
            ScaleValue::from_bytes([1; 20]),
            ScaleValue::from_bytes([3; 32]),
        ],
    );
    encoded(storage::get_address_bytes(&address, metadata).unwrap())
}

fn delegation_storage(mock: &Mock, key: &str, after_entry: bool) -> Option<Value> {
    let wallet = parse_account(mock.state.wallet.as_deref()?).unwrap();
    let signer = parse_account(&mock.state.signer).unwrap();
    let snapshot = runtime_for(mock.modern);
    let metadata = snapshot.metadata();
    for (pallet, item, account, value) in [
        (
            "Subaccount",
            "SubaccountInfo",
            [1; 20],
            ScaleValue::named_composite([
                ("authority", ScaleValue::from_bytes(wallet)),
                ("address", ScaleValue::from_bytes([1; 20])),
                ("name", ScaleValue::from_bytes(b"test")),
                ("status", ScaleValue::unnamed_variant("Active", [])),
                ("spot_positions", ScaleValue::unnamed_composite([])),
                ("next_order_id", ScaleValue::u128(1)),
                ("spot_margin_trading_enabled", ScaleValue::bool(false)),
                (
                    "liquidation_start_at",
                    ScaleValue::unnamed_variant("None", []),
                ),
                ("next_liquidation_id", ScaleValue::u128(1)),
                ("margin_strategy", ScaleValue::unnamed_variant("Cross", [])),
            ]),
        ),
        (
            "Subaccount",
            "DelegateAccountsFor",
            wallet,
            ScaleValue::unnamed_composite([ScaleValue::named_composite([
                ("delegate_address", ScaleValue::from_bytes(signer)),
                ("delegate_name", ScaleValue::from_bytes(b"test")),
                ("valid_until", ScaleValue::u128(1_725_003_600_000)),
                (
                    "mode",
                    ScaleValue::unnamed_variant(
                        if mock.outcome == "delegate_revoked" && after_entry {
                            "Disable"
                        } else {
                            "PlaceOrCancelOrder"
                        },
                        [],
                    ),
                ),
                ("create_time", ScaleValue::u128(1)),
            ])]),
        ),
        (
            "Subaccount",
            "DelegatorAccountsFor",
            signer,
            ScaleValue::unnamed_composite([ScaleValue::from_bytes(wallet)]),
        ),
    ] {
        let address = dynamic::storage(pallet, item, vec![ScaleValue::from_bytes(account)]);
        if encoded(storage::get_address_bytes(&address, metadata).unwrap()) == key {
            let ty = metadata
                .pallet_by_name(pallet)
                .unwrap()
                .storage()
                .unwrap()
                .entry_by_name(item)
                .unwrap()
                .entry_type()
                .value_ty();
            let mut bytes = Vec::new();
            encode_as_type(&value, ty, metadata.types(), &mut bytes).unwrap();
            return Some(json!(encoded(bytes)));
        }
    }
    let address = dynamic::storage("System", "Account", vec![ScaleValue::from_bytes(wallet)]);
    if encoded(storage::get_address_bytes(&address, metadata).unwrap()) == key {
        let raw = account_storage_for(wallet, mock.modern);
        let durable = read_checkpoint(&mock.journal_path);
        if mock.outcome == "owner_nonce_consumed"
            && let Some(exit) = durable.exits.last()
        {
            let bytes = decode_hex(&raw).unwrap();
            let ty = metadata
                .pallet_by_name("System")
                .unwrap()
                .storage()
                .unwrap()
                .entry_by_name("Account")
                .unwrap()
                .entry_type()
                .value_ty();
            let mut account = decode_as_type(&mut &bytes[..], ty, metadata.types()).unwrap();
            set_field(
                &mut account,
                "time_nonce",
                ScaleValue::unnamed_composite([ScaleValue::unnamed_composite([ScaleValue::u128(
                    u128::from(exit.nonce),
                )])]),
            );
            let mut bytes = Vec::new();
            encode_as_type(&account, ty, metadata.types(), &mut bytes).unwrap();
            return Some(json!(encoded(bytes)));
        }
        return Some(json!(raw));
    }
    let signer_address =
        dynamic::storage("System", "Account", vec![ScaleValue::from_bytes(signer)]);
    assert_ne!(
        encoded(storage::get_address_bytes(&signer_address, metadata).unwrap()),
        key,
        "delegated nonce/quota must be read from the wallet, not signer"
    );
    None
}

fn native_active_storage(state: &State, modern: bool) -> String {
    let directory = Directory::new();
    let (execution, _) = fixture_for(&directory, modern);
    let runtime = runtime_for(modern);
    let bytes = event_bytes(vec![place_record(
        runtime.metadata(),
        &execution,
        &state.entry,
        true,
        0,
    )]);
    let events =
        Events::<crate::DeepXRuntimeConfig>::decode_from(bytes, runtime.metadata().clone());
    let event = events.iter().next().unwrap().unwrap();
    let fields = event.field_values().unwrap();
    let order = fields.at("order").unwrap().clone().map_context(|_| ());
    let ty = runtime
        .metadata()
        .pallet_by_name("SpotMarket")
        .unwrap()
        .storage()
        .unwrap()
        .entry_by_name("ActiveSpotOrdersFor")
        .unwrap()
        .entry_type()
        .value_ty();
    let mut encoded_bytes = Vec::new();
    encode_as_type(
        &ScaleValue::unnamed_composite([order]),
        ty,
        runtime.metadata().types(),
        &mut encoded_bytes,
    )
    .unwrap();
    encoded(encoded_bytes)
}

async fn progressing_rpc(mock: Mock, request: Value) -> Json<Value> {
    let progress = mock.progress.as_ref().unwrap();
    let stage = progress.load(Ordering::SeqCst);
    let durable = read_checkpoint(&mock.journal_path);
    let number = 100 + u8::try_from(stage).unwrap();
    let hash = encoded([number; 32]);
    let result = match request["method"].as_str().unwrap() {
        "chain_getFinalizedHead" => json!(hash),
        "chain_getBlockHash" => {
            let n = request["params"][0]
                .as_u64()
                .map_or(number, |n| u8::try_from(n).unwrap());
            json!(encoded([n; 32]))
        }
        "chain_getHeader" => {
            let n = crate::parse_hash(request["params"][0].as_str().unwrap()).unwrap()[0];
            json!({"number":format!("0x{n:x}"),"parentHash":encoded([n-1;32])})
        }
        "chain_getBlock" => {
            let n = crate::parse_hash(request["params"][0].as_str().unwrap()).unwrap()[0];
            let extrinsics = if n == 101 {
                vec![durable.entry.bytes.clone(), durable.target_bytes.clone()]
            } else {
                vec![durable.exits[usize::from(n - 102)].bytes.clone()]
            };
            json!({"block":{"header":{"number":format!("0x{n:x}"),"parentHash":encoded([n-1;32])},"extrinsics":extrinsics}})
        }
        "state_getStorageHash" => json!(durable.runtime_code_hash),
        "state_getRuntimeVersion" => {
            if request["params"][0] == encoded([0; 32]) {
                return Json(
                    json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"unknown block"}}),
                );
            }
            json!({"specVersion":if mock.modern {380} else {376},"transactionVersion":1})
        }
        "state_getMetadata" => json!(if mock.modern {
            native_metadata_hex()
        } else {
            metadata_hex()
        }),
        "author_pendingExtrinsics" => {
            if stage == 0 {
                json!([durable.target_bytes])
            } else {
                json!([])
            }
        }
        "state_call" => json!(depth(stage > 0)),
        "state_getStorage" => {
            let key = request["params"][0].as_str().unwrap();
            if let Some(value) = delegation_storage(&mock, key, stage > 0) {
                value
            } else if key == deepx_rust_sdk::rpc::event_storage::SYSTEM_EVENTS_STORAGE_KEY {
                let n = crate::parse_hash(request["params"][1].as_str().unwrap()).unwrap()[0];
                let directory = Directory::new();
                let (execution, _) = fixture_for(&directory, mock.modern);
                let runtime = runtime_for(mock.modern);
                let records = if n == 101 {
                    let mut records = vec![
                        place_record(runtime.metadata(), &execution, &durable.entry, true, 0),
                        dispatch_record(runtime.metadata(), 0, true),
                        dispatch_record(runtime.metadata(), 1, mock.outcome != "target_failed"),
                    ];
                    if !matches!(mock.outcome, "unfilled" | "active_ioc") {
                        let (base, quote, fee) = if mock.outcome == "partial" {
                            (500, 50_000, 5)
                        } else {
                            (1_000, 100_000, 10)
                        };
                        records.push(match_record(
                            runtime.metadata(),
                            &trade(&durable, true, base, quote, fee, 3),
                        ));
                    }
                    records
                } else {
                    let op = &durable.exits[usize::from(n - 102)];
                    let base = if mock.outcome == "partial_exit" && n == 102 {
                        333
                    } else {
                        amount(&op.base).unwrap()
                    };
                    let quote = base * 110;
                    let mut fill = trade(&durable, false, base, quote, (quote / 100) as i128, 2);
                    if mock.outcome == "target_failed" {
                        fill.buyer = [5; 20];
                        fill.buy_id = 50;
                    }
                    vec![
                        place_record(runtime.metadata(), &execution, op, false, 0),
                        dispatch_record(runtime.metadata(), 0, true),
                        match_record(runtime.metadata(), &fill),
                    ]
                };
                json!(encoded(event_bytes(records)))
            } else if key == deepx_rust_sdk::rpc::storage_decode::spot_market_storage_key([3; 32]) {
                json!(market_storage_for(mock.modern))
            } else if key == native_order_key(runtime_for(mock.modern).metadata()) {
                if mock.outcome == "active_ioc" {
                    json!(native_active_storage(&durable, mock.modern))
                } else {
                    Value::Null
                }
            } else {
                json!(account_storage_for(
                    parse_account(&durable.signer).unwrap(),
                    mock.modern
                ))
            }
        }
        "state_queryStorageAt" => {
            let n = crate::parse_hash(request["params"][1].as_str().unwrap()).unwrap()[0];
            json!([{"block":request["params"][1],"changes":[[
                deepx_rust_sdk::rpc::timestamp::timestamp_now_storage_key(),encoded((1_725_000_000_000_u64+u64::from(n-100)*10).to_le_bytes())
            ]]}])
        }
        "author_submitExtrinsic" => {
            let op = if stage == 0 {
                &durable.entry
            } else {
                durable.exits.last().unwrap()
            };
            assert!(op.attempted);
            assert_eq!(request["params"][0], op.bytes);
            mock.submissions.fetch_add(1, Ordering::SeqCst);
            progress.fetch_add(1, Ordering::SeqCst);
            json!(op.hash)
        }
        method => panic!("unexpected progressive RPC method {method}"),
    };
    Json(json!({"jsonrpc":"2.0","id":1,"result":result}))
}

#[rstest]
#[case("accepted", "7711", false, false)]
#[case("accepted", "7711", true, false)]
#[case("partial", "3806", false, false)]
#[case("partial", "3806", true, false)]
#[case("target_failed", "7711", false, false)]
#[case("target_failed", "7711", true, false)]
#[case("unfilled", "-100", false, false)]
#[case("unfilled", "-100", true, false)]
#[case("partial_exit", "7712", false, false)]
#[case("partial_exit", "7712", true, false)]
#[case("accepted", "7711", true, true)]
#[case("partial_exit", "7712", true, true)]
#[tokio::test]
async fn execution_reconciles_both_finalized_legs_and_target_before_reporting_profit(
    #[case] outcome: &'static str,
    #[case] profit: &str,
    #[case] modern: bool,
    #[case] delegated: bool,
) {
    let directory = Directory::new();
    let (mut execution, mut state) = fixture_for(&directory, modern);
    if delegated {
        execution.settings.wallet = Some([7; 20]);
        state.wallet = Some(encoded([7; 20]));
    }
    execution.journal.store(&state).unwrap();
    let submissions = Arc::new(AtomicUsize::new(0));
    let mock = Mock {
        state: state.clone(),
        journal_path: execution.settings.journal.clone(),
        submissions: submissions.clone(),
        outcome,
        target_pending: true,
        progress: Some(Arc::new(AtomicUsize::new(0))),
        modern,
    };
    let (rpc, task) = mock_rpc(mock).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        execution.run_inner(&rpc, &mut state, 1),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(state.complete);
    let unfilled = outcome == "unfilled";
    assert_eq!(
        state.result.as_deref(),
        Some(if unfilled { "entry_unfilled" } else { "flat" })
    );
    assert_eq!(
        state.cursor_number,
        if unfilled {
            101
        } else if outcome == "partial_exit" {
            103
        } else {
            102
        }
    );
    assert_eq!(state.entry.included.as_ref().unwrap().number, 101);
    if !unfilled {
        assert_eq!(state.exits[0].included.as_ref().unwrap().number, 102);
    }
    if outcome == "partial_exit" {
        assert_eq!(state.exits[1].included.as_ref().unwrap().number, 103);
    }
    assert_eq!(state.target_outcome, Some(outcome != "target_failed"));
    assert_eq!(state.remaining().unwrap(), 0);
    assert_eq!(state.profit().unwrap(), profit);
    assert_eq!(
        state.entry_first_trade,
        if unfilled { None } else { Some((101, 3)) }
    );
    assert_eq!(
        state.target_first_trade,
        if unfilled || outcome == "target_failed" {
            None
        } else {
            Some((102, 2))
        }
    );
    assert_eq!(
        submissions.load(Ordering::SeqCst),
        if unfilled {
            1
        } else if outcome == "partial_exit" {
            3
        } else {
            2
        }
    );
    let checkpoint = execution.journal.load().unwrap().unwrap();
    assert!(checkpoint.complete);
    assert_eq!(checkpoint.profit().unwrap(), profit);
    task.abort();
}

#[rstest]
#[case("delegate_revoked")]
#[case("owner_nonce_consumed")]
#[tokio::test]
async fn delegated_inventory_is_retained_when_authorization_or_wallet_nonce_changes(
    #[case] outcome: &'static str,
) {
    let directory = Directory::new();
    let (mut execution, mut state) = fixture_for(&directory, true);
    execution.settings.wallet = Some([7; 20]);
    state.wallet = Some(encoded([7; 20]));
    execution.journal.store(&state).unwrap();
    let submissions = Arc::new(AtomicUsize::new(0));
    let mock = Mock {
        state: state.clone(),
        journal_path: execution.settings.journal.clone(),
        submissions: submissions.clone(),
        outcome,
        target_pending: true,
        progress: Some(Arc::new(AtomicUsize::new(0))),
        modern: true,
    };
    let (rpc, task) = mock_rpc(mock).await;
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        execution.run_inner(&rpc, &mut state, 1),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains(if outcome == "delegate_revoked" {
                "order delegation"
            } else {
                "nonce is no longer available"
            }),
        "{error:#}"
    );
    assert_eq!(submissions.load(Ordering::SeqCst), 1);
    let saved = execution.journal.load().unwrap().unwrap();
    assert!(!saved.complete);
    assert_eq!(saved.remaining().unwrap(), 990);
    assert!(saved.exits.iter().all(|exit| !exit.attempted));
    task.abort();
}

#[test]
fn delegated_journal_binds_wallet_and_keeps_legacy_direct_identity() {
    let directory = Directory::new();
    let (mut execution, mut state) = fixture(&directory);
    execution.validate_identity(&state).unwrap();
    assert!(
        serde_json::to_value(&state)
            .unwrap()
            .get("wallet")
            .is_none()
    );
    execution.settings.wallet = Some([7; 20]);
    assert!(execution.validate_identity(&state).is_err());
    state.wallet = Some(encoded([7; 20]));
    execution.validate_identity(&state).unwrap();
    execution.journal.store(&state).unwrap();
    let restored = execution.journal.load().unwrap().unwrap();
    execution.validate_identity(&restored).unwrap();
    execution.settings.wallet = Some([8; 20]);
    assert!(execution.validate_identity(&restored).is_err());
}

#[test]
fn shared_wallet_pending_check_rejects_owner_and_other_delegates() {
    let directory = Directory::new();
    let (execution, state) = fixture(&directory);
    let snapshot = runtime();
    let foreign_key = DeepXPrivateKey::new(&"33".repeat(32), &DeepXKeyScheme::Secp256k1).unwrap();
    let foreign_signer = derive_signer_account_id(&foreign_key).unwrap();
    let service = DeepXRuntimeSnapshotService::new(runtime());
    let foreign = sign_spot_place_order(
        &service.acquire().unwrap(),
        &foreign_key,
        execution.params(true, 1000, 100_000),
        state.entry.nonce + 20,
    )
    .unwrap();
    let related = std::collections::BTreeSet::from([execution.signer, foreign_signer]);
    for bytes in [state.entry.bytes, encoded(foreign.bytes())] {
        assert!(
            delegation::require_no_shared_pending(snapshot.metadata(), &[bytes], &related).is_err()
        );
    }
    delegation::require_no_shared_pending(snapshot.metadata(), &[state.target_bytes], &related)
        .unwrap();
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn successful_placement_with_active_ioc_is_not_treated_as_unfilled_or_flat(
    #[case] modern: bool,
) {
    let directory = Directory::new();
    let (execution, mut state) = fixture_for(&directory, modern);
    execution.journal.store(&state).unwrap();
    let submissions = Arc::new(AtomicUsize::new(0));
    let mock = Mock {
        state: state.clone(),
        journal_path: execution.settings.journal.clone(),
        submissions: submissions.clone(),
        outcome: "active_ioc",
        target_pending: true,
        progress: Some(Arc::new(AtomicUsize::new(0))),
        modern,
    };
    let (rpc, task) = mock_rpc(mock).await;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        execution.run_inner(&rpc, &mut state, 1),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(state.cursor_number, 101);
    assert!(state.entry.included.as_ref().unwrap().success);
    assert!(!state.entry.settled());
    assert!(!state.complete);
    assert!(state.exits.is_empty());
    assert_eq!(submissions.load(Ordering::SeqCst), 1);
    task.abort();
}

#[rstest]
#[case("reorg", "checkpoint")]
#[case("runtime_changed", "runtime changed")]
#[tokio::test]
async fn contradictory_chain_evidence_stops_before_submission(
    #[case] outcome: &'static str,
    #[case] message: &str,
) {
    let directory = Directory::new();
    let (execution, mut state) = fixture(&directory);
    execution.journal.store(&state).unwrap();
    let submissions = Arc::new(AtomicUsize::new(0));
    let mock = Mock {
        state: state.clone(),
        journal_path: execution.settings.journal.clone(),
        submissions: submissions.clone(),
        outcome,
        target_pending: true,
        progress: None,
        modern: false,
    };
    let (rpc, task) = mock_rpc(mock).await;
    let e = execution.run_inner(&rpc, &mut state, 1).await.unwrap_err();
    assert!(e.to_string().contains(message), "{e}");
    assert_eq!(submissions.load(Ordering::SeqCst), 0);
    assert!(!execution.journal.load().unwrap().unwrap().entry.attempted);
    task.abort();
}
