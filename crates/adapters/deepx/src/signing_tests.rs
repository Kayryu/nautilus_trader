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

//! Adapter integration tests for SDK signing compatibility.

use super::*;
use crate::common::{DeepXEnvironment, DeepXKeyScheme};
use nautilus_core::hex;
use rstest::rstest;
use serde::Deserialize;
use subxt_core::config::{Hasher, substrate::BlakeTwo256};

const SPEC376_METADATA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/deepx-rust-sdk/test_data/runtime/testnet/",
    "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
));

#[derive(Deserialize)]
struct RpcResponse {
    result: String,
}

fn runtime() -> RuntimeSnapshot {
    let response: RpcResponse = serde_json::from_str(SPEC376_METADATA).unwrap();
    let metadata = hex::decode(response.result.trim_start_matches("0x")).unwrap();
    RuntimeSnapshot::approved_testnet(
        &DeepXEnvironment::Testnet,
        hex::decode_array("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
            .unwrap(),
        376,
        1,
        &metadata,
    )
    .unwrap()
}

fn key() -> DeepXPrivateKey {
    DeepXPrivateKey::new(
        "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        &DeepXKeyScheme::Secp256k1,
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires live DeepX testnet Watch RPC"]
async fn current_runtime_signs_calls_offline_with_synthetic_key() {
    let network = crate::config::DeepXNetworkConfig::default();
    let endpoints = crate::rpc::observe_and_validate_rpc_endpoint_identities(&network)
        .await
        .unwrap();
    let snapshot = crate::rpc::observe_current_runtime_snapshot_for_endpoints(
        &network.environment,
        &endpoints,
    )
    .await
    .unwrap();
    let service = DeepXRuntimeSnapshotService::new(snapshot.clone());
    let permit = service.acquire().unwrap();
    let key = key();
    let signed = sign_no_op(&permit, &key, 0).unwrap();

    assert_eq!(signed.runtime, *snapshot.identity());
    assert_eq!(signed.signer, derive_signer_account_id(&key).unwrap());
    assert_eq!(signed.extrinsic_hash, BlakeTwo256.hash(&signed.bytes).0);
    assert!(!signed.bytes.is_empty());

    let params = DeepXPerpPlaceParams {
        subaccount: [0x11; 20],
        market_id: 3,
        is_long: true,
        size: 1_000_000,
        price: 2_500_000_000,
        order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
        take_profit: None,
        stop_loss: None,
        reduce_only: false,
        post_only: DeepXPostOnlyParam::None,
    };
    let direct = sign_dynamic_pallet_call(
        &permit,
        &key,
        "PerpMarket",
        "place_order",
        vec![params.into_dynamic_value()],
        1,
    )
    .unwrap();
    let placement = sign_perp_place_order(&permit, &key, params, 1).unwrap();
    assert_eq!(placement.bytes, direct.bytes);
    assert_eq!(placement.runtime, *snapshot.identity());
    assert_eq!(placement.signer, signed.signer);
    assert_eq!(
        placement.extrinsic_hash,
        BlakeTwo256.hash(&placement.bytes).0
    );
    assert_ne!(placement.extrinsic_hash, signed.extrinsic_hash);

    let spot_params = DeepXSpotPlaceParams {
        subaccount: [0x11; 20],
        pair: [0x22; 32],
        is_buy: true,
        quote_amount: [0x33; 32],
        base_amount: [0x44; 32],
        order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc),
        post_only: DeepXPostOnlyParam::None,
        reduce_only: false,
    };
    let spot = sign_spot_place_order(&permit, &key, spot_params, 2).unwrap();
    assert_eq!(spot.runtime, *snapshot.identity());
    assert_eq!(spot.signer, signed.signer);
    assert_eq!(spot.extrinsic_hash, BlakeTwo256.hash(&spot.bytes).0);
    assert_ne!(spot.extrinsic_hash, placement.extrinsic_hash);

    let cancel = DeepXPerpCancelParams {
        subaccount: params.subaccount,
        order_id: 42,
        market_id: params.market_id,
        fast_cancel: false,
    };
    let perp_cancel = sign_perp_cancel(&permit, &key, cancel, 3).unwrap();
    assert_eq!(perp_cancel.runtime, *snapshot.identity());
    assert_eq!(
        perp_cancel.extrinsic_hash,
        BlakeTwo256.hash(&perp_cancel.bytes).0
    );

    let modify = sign_perp_modify_order(
        &permit,
        &key,
        DeepXPerpModifyParams {
            cancel,
            place: params,
        },
        4,
    )
    .unwrap();
    assert_eq!(modify.runtime, *snapshot.identity());
    assert_eq!(modify.extrinsic_hash, BlakeTwo256.hash(&modify.bytes).0);

    let spot_cancel = sign_spot_cancel(
        &permit,
        &key,
        DeepXSpotCancelParams {
            subaccount: spot_params.subaccount,
            pair: spot_params.pair,
            order_id: 42,
            is_buy: spot_params.is_buy,
            fast_cancel: false,
        },
        5,
    )
    .unwrap();
    assert_eq!(spot_cancel.runtime, *snapshot.identity());
    assert_eq!(
        spot_cancel.extrinsic_hash,
        BlakeTwo256.hash(&spot_cancel.bytes).0
    );
}

#[rstest]
fn signed_extrinsic_retains_approved_runtime_identity() {
    let snapshot = runtime();
    let service = DeepXRuntimeSnapshotService::new(snapshot.clone());
    let permit = service.acquire().unwrap();
    let signed = sign_no_op(&permit, &key(), 1_725_000_000_124).unwrap();
    assert!(signed.has_valid_hash());
    assert_eq!(signed.runtime(), snapshot.identity());
}
