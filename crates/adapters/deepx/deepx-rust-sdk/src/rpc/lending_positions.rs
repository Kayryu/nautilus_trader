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
//  See the GNU Lesser General Public License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Bounded SCALE decoding for lending position runtime API responses.

use parity_scale_codec::{Compact, Decode, DecodeAll, Encode};
use thiserror::Error;

use super::{
    DEEPX_LENDING_BORROW_RUNTIME_API_METHOD, DEEPX_LENDING_DEPOSIT_RUNTIME_API_METHOD,
    DEEPX_LENDING_POOL_RUNTIME_API_METHOD, DEEPX_LENDING_POSITIONS_RUNTIME_API_METHOD,
    DeepXRpcClient, storage::DeepXLendingPoolStorage,
};

const MAX_LENDING_ASSET_SYMBOL_BYTES: usize = 32;
const MAX_LENDING_POSITION_ASSETS: usize = 64;

/// Encodes the complete market-ID range for one-account lending position reads.
#[must_use]
pub fn encode_lending_positions_request(subaccount: [u8; 20]) -> Vec<u8> {
    let market_ids = (u8::MIN..=u8::MAX).collect::<Vec<_>>();
    (vec![subaccount], market_ids).encode()
}

/// Encodes an asset-scoped lending pool read.
#[must_use]
pub fn encode_lending_pool_request(market_id: u8, asset: &[u8]) -> Vec<u8> {
    (market_id, asset.to_vec()).encode()
}

/// Encodes an account and asset-scoped lending balance read.
#[must_use]
pub fn encode_lending_balance_request(
    subaccount: [u8; 20],
    market_id: u8,
    asset: &[u8],
) -> Vec<u8> {
    (subaccount, market_id, asset.to_vec()).encode()
}

/// One asset position returned by the DeepX lending runtime API.
#[derive(Debug, PartialEq, Eq)]
pub struct DeepXRawLendingPosition {
    pub market_id: u8,
    pub asset: Vec<u8>,
    pub scaled_deposit: u128,
    pub scaled_borrow: u128,
}

/// Errors while reading lending Runtime API responses.
#[derive(Debug, Error)]
pub enum DeepXLendingReadError {
    /// A Runtime API request failed.
    #[error("DeepX lending Runtime API {method} request failed: {source}")]
    Rpc {
        method: &'static str,
        #[source]
        source: anyhow::Error,
    },
    /// The response was malformed or inconsistent with its request.
    #[error("DeepX lending Runtime API response is invalid")]
    InvalidResponse,
}

/// One asset's pool and effective balances at one block.
#[derive(Debug)]
pub struct DeepXLendingAssetRead {
    pub pool: DeepXLendingPoolStorage,
    pub effective_deposit: Option<u128>,
    pub effective_borrow: Option<u128>,
}

/// Checks one lending position against its asset reads and maximum withdrawal.
#[must_use]
pub fn lending_asset_reads_match_position(
    position: &DeepXRawLendingPosition,
    asset_read: &DeepXLendingAssetRead,
    max_withdraw_amount: Option<u128>,
) -> bool {
    asset_read.pool.market_id == position.market_id
        && asset_read.pool.asset == position.asset
        && (position.scaled_deposit == 0) == asset_read.effective_deposit.is_none()
        && (position.scaled_borrow == 0) == asset_read.effective_borrow.is_none()
        && (position.scaled_deposit == 0) == max_withdraw_amount.is_none()
        && max_withdraw_amount.is_none_or(|amount| {
            amount <= asset_read.effective_deposit.unwrap_or_default()
                && amount <= asset_read.pool.available_liquidity
        })
}

/// Reads all lending positions for one account at a fixed block.
///
/// # Errors
///
/// Returns an error for RPC failure or an invalid account-scoped response.
pub async fn get_lending_positions_at_block(
    client: &DeepXRpcClient,
    block_hash: [u8; 32],
    subaccount: [u8; 20],
) -> Result<Vec<DeepXRawLendingPosition>, DeepXLendingReadError> {
    let hash = format!("0x{}", hex::encode(block_hash));
    let encoded: String = client
        .state_call(
            DEEPX_LENDING_POSITIONS_RUNTIME_API_METHOD,
            encode_lending_positions_request(subaccount),
            &hash,
        )
        .await
        .map_err(|source| DeepXLendingReadError::Rpc {
            method: DEEPX_LENDING_POSITIONS_RUNTIME_API_METHOD,
            source,
        })?;
    decode_lending_positions(&encoded, subaccount).ok_or(DeepXLendingReadError::InvalidResponse)
}

/// Reads one asset's pool and effective balances at a fixed block.
///
/// # Errors
///
/// Returns an error for RPC failure or malformed pool or balance responses.
pub async fn get_lending_asset_at_block(
    client: &DeepXRpcClient,
    block_hash: [u8; 32],
    subaccount: [u8; 20],
    market_id: u8,
    asset: &[u8],
) -> Result<DeepXLendingAssetRead, DeepXLendingReadError> {
    let hash = format!("0x{}", hex::encode(block_hash));
    let call = |method: &'static str, input: Vec<u8>| {
        let hash = hash.clone();
        async move {
            client
                .state_call(method, input, &hash)
                .await
                .map_err(|source| DeepXLendingReadError::Rpc { method, source })
        }
    };
    let balance_input = encode_lending_balance_request(subaccount, market_id, asset);
    let (pool, deposit, borrow) = tokio::join!(
        call(
            DEEPX_LENDING_POOL_RUNTIME_API_METHOD,
            encode_lending_pool_request(market_id, asset),
        ),
        call(
            DEEPX_LENDING_DEPOSIT_RUNTIME_API_METHOD,
            balance_input.clone()
        ),
        call(DEEPX_LENDING_BORROW_RUNTIME_API_METHOD, balance_input),
    );
    let pool = decode_lending_pool_response(&pool?)
        .flatten()
        .ok_or(DeepXLendingReadError::InvalidResponse)?;
    if pool.market_id != market_id || pool.asset != asset {
        return Err(DeepXLendingReadError::InvalidResponse);
    }
    let effective_deposit =
        decode_lending_amount_response(&deposit?).ok_or(DeepXLendingReadError::InvalidResponse)?;
    let effective_borrow =
        decode_lending_amount_response(&borrow?).ok_or(DeepXLendingReadError::InvalidResponse)?;
    Ok(DeepXLendingAssetRead {
        pool,
        effective_deposit,
        effective_borrow,
    })
}

/// Decodes one hex-encoded lending pool response without trailing SCALE data.
#[must_use]
pub fn decode_lending_pool_response(encoded: &str) -> Option<Option<DeepXLendingPoolStorage>> {
    decode_lending_asset_response(encoded)
}

/// Decodes one hex-encoded effective lending balance without trailing SCALE data.
#[must_use]
pub fn decode_lending_amount_response(encoded: &str) -> Option<Option<u128>> {
    decode_lending_asset_response(encoded)
}

fn decode_lending_asset_response<T: Decode>(encoded: &str) -> Option<T> {
    let bytes = hex::decode(encoded.strip_prefix("0x")?).ok()?;
    T::decode_all(&mut &bytes[..]).ok()
}

/// Decodes one canonical, account-scoped lending position response.
#[must_use]
pub fn decode_lending_positions(
    encoded: &str,
    subaccount: [u8; 20],
) -> Option<Vec<DeepXRawLendingPosition>> {
    let bytes = hex::decode(encoded.strip_prefix("0x")?).ok()?;
    let mut input = bytes.as_slice();
    let account_count = decode_bounded_compact_len(&mut input, 1)?;
    if account_count == 0 {
        return input.is_empty().then(Vec::new);
    }
    let account = <[u8; 20]>::decode(&mut input).ok()?;
    if account != subaccount {
        return None;
    }
    let market_count = decode_bounded_compact_len(&mut input, usize::from(u8::MAX) + 1)
        .filter(|count| *count > 0)?;
    let mut positions = Vec::new();
    let mut previous_market = None;
    for _ in 0..market_count {
        let market_id = u8::decode(&mut input).ok()?;
        if previous_market.is_some_and(|previous| market_id <= previous) {
            return None;
        }
        previous_market = Some(market_id);
        let asset_count = decode_bounded_compact_len(&mut input, MAX_LENDING_POSITION_ASSETS)
            .filter(|count| *count > 0)?;
        let mut previous_asset: Option<Vec<u8>> = None;
        for _ in 0..asset_count {
            let asset = decode_bounded_bytes(&mut input, MAX_LENDING_ASSET_SYMBOL_BYTES)
                .filter(|value| !value.is_empty())?;
            if previous_asset
                .as_ref()
                .is_some_and(|previous| asset <= *previous)
            {
                return None;
            }
            previous_asset = Some(asset.clone());
            let scaled_deposit = u128::decode(&mut input).ok()?;
            let scaled_borrow = u128::decode(&mut input).ok()?;
            if scaled_deposit == 0 && scaled_borrow == 0 {
                return None;
            }
            positions.push(DeepXRawLendingPosition {
                market_id,
                asset,
                scaled_deposit,
                scaled_borrow,
            });
        }
    }
    input.is_empty().then_some(positions)
}

fn decode_bounded_compact_len(input: &mut &[u8], maximum: usize) -> Option<usize> {
    let Compact(value) = Compact::<u32>::decode(input).ok()?;
    let value = usize::try_from(value).ok()?;
    (value <= maximum).then_some(value)
}

fn decode_bounded_bytes(input: &mut &[u8], maximum: usize) -> Option<Vec<u8>> {
    let len = decode_bounded_compact_len(input, maximum)?;
    if input.len() < len {
        return None;
    }
    let (value, remaining) = input.split_at(len);
    *input = remaining;
    Some(value.to_vec())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::{Json, Router, routing::post};
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;
    use crate::rpc::storage::DeepXLendingPoolStatus;

    fn pool(market_id: u8, asset: &[u8]) -> DeepXLendingPoolStorage {
        DeepXLendingPoolStorage {
            market_id,
            asset: asset.to_vec(),
            decimal: 6,
            cash_balance: 2_000,
            locked_liquidity: 100,
            available_liquidity: 1_900,
            total_scaled_deposits: 1_500,
            total_scaled_borrows: 500,
            total_effective_deposits: 1_600,
            total_effective_borrows: 550,
            deposit_index: 1,
            borrow_index: 1,
            protocol_reserve: 10,
            bad_debt: 0,
            accounting_dust: 0,
            internal_transfer_dust: 0,
            status: DeepXLendingPoolStatus::Active,
            apr_borrow: 0,
            apr_lend: 0,
        }
    }

    #[rstest]
    #[case::valid(0)]
    #[case::foreign_market(1)]
    #[case::foreign_asset(2)]
    #[case::malformed_balance(3)]
    #[tokio::test]
    async fn lending_asset_read_checks_requested_pool_identity(#[case] scenario: u8) {
        let account = [0x11; 20];
        let block_hash = [0x22; 32];
        let encoded_hash = format!("0x{}", hex::encode(block_hash));
        let market_id = if scenario == 1 { 2 } else { 1 };
        let asset = if scenario == 2 { b"usdt" } else { b"usdc" };
        let encoded_pool = format!("0x{}", hex::encode(Some(pool(market_id, asset)).encode()));
        let app = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let encoded_hash = encoded_hash.clone();
                let encoded_pool = encoded_pool.clone();
                async move {
                    assert_eq!(request["method"], "state_call");
                    assert_eq!(request["params"][2], encoded_hash);
                    let method = request["params"][0].as_str().unwrap();
                    let input = hex::decode(
                        request["params"][1]
                            .as_str()
                            .unwrap()
                            .strip_prefix("0x")
                            .unwrap(),
                    )
                    .unwrap();
                    let response = match method {
                        DEEPX_LENDING_POOL_RUNTIME_API_METHOD => {
                            assert_eq!(input, encode_lending_pool_request(1, b"usdc"));
                            encoded_pool
                        }
                        DEEPX_LENDING_DEPOSIT_RUNTIME_API_METHOD => {
                            assert_eq!(input, encode_lending_balance_request(account, 1, b"usdc"));
                            if scenario == 3 {
                                "0xzz".to_string()
                            } else {
                                format!("0x{}", hex::encode(Some(500_u128).encode()))
                            }
                        }
                        DEEPX_LENDING_BORROW_RUNTIME_API_METHOD => {
                            assert_eq!(input, encode_lending_balance_request(account, 1, b"usdc"));
                            "0x00".to_string()
                        }
                        _ => panic!("unexpected lending method"),
                    };
                    Json(json!({"jsonrpc": "2.0", "id": 1, "result": response}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let result =
            get_lending_asset_at_block(&DeepXRpcClient::new(url), block_hash, account, 1, b"usdc")
                .await;
        if scenario == 0 {
            let read = result.unwrap();
            assert_eq!(read.pool, pool(1, b"usdc"));
            assert_eq!(read.effective_deposit, Some(500));
            assert_eq!(read.effective_borrow, None);
        } else {
            assert!(matches!(
                result,
                Err(DeepXLendingReadError::InvalidResponse)
            ));
        }
        server.abort();
    }

    #[rstest]
    fn encodes_complete_lending_position_market_range() {
        let subaccount = [0x11; 20];
        let encoded = encode_lending_positions_request(subaccount);
        let (accounts, markets) =
            <(Vec<[u8; 20]>, Vec<u8>)>::decode_all(&mut &encoded[..]).unwrap();
        assert_eq!(accounts, vec![subaccount]);
        assert_eq!(markets, (u8::MIN..=u8::MAX).collect::<Vec<_>>());
    }

    #[rstest]
    fn encodes_lending_asset_request_shapes() {
        let subaccount = [0x11; 20];
        assert_eq!(encode_lending_pool_request(3, b"usdc"), b"\x03\x10usdc");
        let mut expected_balance = subaccount.to_vec();
        expected_balance.extend_from_slice(b"\x03\x10usdc");
        assert_eq!(
            encode_lending_balance_request(subaccount, 3, b"usdc"),
            expected_balance,
        );
    }

    #[rstest]
    fn lending_asset_response_requires_complete_scale_value() {
        assert_eq!(decode_lending_amount_response("0x00"), Some(None));
        assert_eq!(decode_lending_amount_response("0x0000"), None);
        assert_eq!(decode_lending_amount_response("0xzz"), None);
        assert_eq!(decode_lending_amount_response("00"), None);
        assert!(matches!(decode_lending_pool_response("0x00"), Some(None)));
        assert!(decode_lending_pool_response("0x0000").is_none());
    }

    #[rstest]
    fn lending_positions_decode_exact_scoped_shares() {
        let account = [0x11; 20];
        let encoded = BTreeMap::from([(
            account,
            BTreeMap::from([(
                1_u8,
                BTreeMap::from([(b"usdc".to_vec(), (100_u128, 2_u128))]),
            )]),
        )])
        .encode();
        let text = format!("0x{}", hex::encode(&encoded));

        assert_eq!(
            decode_lending_positions(&text, account),
            Some(vec![DeepXRawLendingPosition {
                market_id: 1,
                asset: b"usdc".to_vec(),
                scaled_deposit: 100,
                scaled_borrow: 2,
            }])
        );
        assert_eq!(decode_lending_positions(&text, [0x22; 20]), None);
        assert_eq!(
            decode_lending_positions(&format!("{text}00"), account),
            None
        );
    }

    #[rstest]
    #[case("foreign")]
    #[case("zero")]
    #[case("overlong")]
    #[case("trailing")]
    fn lending_positions_reject_invalid_account_evidence(#[case] scenario: &str) {
        let account = if scenario == "foreign" {
            [0x22; 20]
        } else {
            [0x11; 20]
        };
        let asset = if scenario == "overlong" {
            vec![b'x'; 33]
        } else {
            b"usdc".to_vec()
        };
        let amounts = if scenario == "zero" {
            (0_u128, 0_u128)
        } else {
            (1, 0)
        };
        let mut encoded = BTreeMap::from([(
            account,
            BTreeMap::from([(1_u8, BTreeMap::from([(asset, amounts)]))]),
        )])
        .encode();
        if scenario == "trailing" {
            encoded.push(0);
        }

        assert_eq!(
            decode_lending_positions(&format!("0x{}", hex::encode(encoded)), [0x11; 20]),
            None,
        );
    }

    #[rstest]
    fn lending_positions_accept_canonical_empty_account_evidence() {
        let encoded =
            BTreeMap::<[u8; 20], BTreeMap<u8, BTreeMap<Vec<u8>, (u128, u128)>>>::new().encode();

        assert_eq!(
            decode_lending_positions(&format!("0x{}", hex::encode(encoded)), [0x11; 20]),
            Some(Vec::new()),
        );
    }
}
