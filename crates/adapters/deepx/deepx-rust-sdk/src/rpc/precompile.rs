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

//! DeepX lending and perpetual-market precompile ABI.

use alloy::{
    primitives::{Address, Bytes},
    sol,
    sol_types::{SolCall, SolValue},
};
use thiserror::Error;

use super::DeepXRpcClient;

/// Lending V2 precompile deployed by DeepX.
pub const DEEPX_LENDING_PRECOMPILE_ADDRESS: Address = Address::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x50,
]);

/// Perpetual market precompile deployed by DeepX.
pub const DEEPX_PERP_MARKET_PRECOMPILE_ADDRESS: Address = Address::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x4e,
]);

sol! {
    function maxWithdrawAmountFor(
        address subaccount,
        uint8 lendingMarket,
        bytes asset
    ) external view returns (uint128);

    struct DeepXPerpOrderSpecAbi {
        uint128 min_order_size;
        uint128 tick_size;
        uint128 step_size;
    }

    struct DeepXPerpMarketAbi {
        uint16 id;
        bytes name;
        bytes base_symbol;
        int32 base_decimal;
        uint16 quote_market_id;
        bytes network;
        uint64 height;
        int128 funding_rate;
        uint64 last_cacl_funding_rate_time;
        uint128 oracle_price;
        uint128 mark_price;
        uint64 max_deviation_bps;
        uint128 maintenance_margin_ratio;
        uint32 taker_fee_rate;
        int32 maker_fee_rate;
        DeepXPerpOrderSpecAbi order_spec;
        uint128 open_interest;
        uint128 long_open_pos_num;
        uint128 short_open_pos_num;
        int128 base_interest_rate;
        uint128 impact_margin_value;
        int128 funding_rate_change_cap;
        int128 funding_rate_change_floor;
        int128 cumulative_funding_index;
    }

    function perpMarkets(uint16 market_id) external view returns (DeepXPerpMarketAbi);
}

/// Encodes the lending precompile's maximum-withdrawal query.
#[must_use]
pub fn encode_max_withdraw_call(subaccount: [u8; 20], market_id: u8, asset: &[u8]) -> Vec<u8> {
    maxWithdrawAmountForCall {
        subaccount: Address::from(subaccount),
        lendingMarket: market_id,
        asset: Bytes::copy_from_slice(asset),
    }
    .abi_encode()
}

/// Decodes a canonical `uint128` maximum-withdrawal return value.
#[must_use]
pub fn decode_max_withdraw_return(response: &[u8]) -> Option<u128> {
    let raw = <[u8; 16]>::try_from(response.get(16..)?).ok()?;
    (response.len() == 32 && response[..16].iter().all(|byte| *byte == 0))
        .then_some(u128::from_be_bytes(raw))
}

/// Decodes a hex-encoded canonical maximum-withdrawal return value.
#[must_use]
pub fn decode_max_withdraw_hex(encoded: &str) -> Option<u128> {
    let bytes = encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())?;
    decode_max_withdraw_return(&bytes)
}

/// Errors while reading the lending maximum-withdrawal precompile.
#[derive(Debug, Error)]
pub enum DeepXMaxWithdrawReadError {
    /// The EVM RPC request failed.
    #[error("DeepX maximum-withdrawal RPC failed: {0}")]
    Rpc(#[source] anyhow::Error),
    /// The precompile response was not one canonical `uint128`.
    #[error("DeepX maximum-withdrawal response is invalid")]
    InvalidResponse,
}

/// Reads one lending asset's maximum withdrawal at a selected EVM block number.
///
/// # Errors
///
/// Returns an error for RPC failure or a malformed ABI response.
pub async fn get_max_withdrawal_at_block_number(
    client: &DeepXRpcClient,
    block_number: u32,
    subaccount: [u8; 20],
    market_id: u8,
    asset: &[u8],
) -> Result<u128, DeepXMaxWithdrawReadError> {
    let encoded: String = client
        .eth_call(
            &DEEPX_LENDING_PRECOMPILE_ADDRESS.to_string(),
            &encode_max_withdraw_call(subaccount, market_id, asset),
            &format!("0x{block_number:x}"),
        )
        .await
        .map_err(DeepXMaxWithdrawReadError::Rpc)?;
    decode_max_withdraw_hex(&encoded).ok_or(DeepXMaxWithdrawReadError::InvalidResponse)
}

/// Encodes a perpetual-market precompile query.
#[must_use]
pub fn encode_perp_market_call(market_id: u16) -> Vec<u8> {
    perpMarketsCall { market_id }.abi_encode()
}

/// Decodes a canonical perpetual-market precompile return value.
#[must_use]
pub fn decode_perp_market_return(response: &[u8]) -> Option<DeepXPerpMarketAbi> {
    let market = perpMarketsCall::abi_decode_returns(response).ok()?;
    (market.abi_encode() == response).then_some(market)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn precompile_addresses_and_calls_match_deployment_abi() {
        assert_eq!(
            DEEPX_LENDING_PRECOMPILE_ADDRESS.as_slice()[18..],
            [0x04, 0x50]
        );
        assert_eq!(
            DEEPX_PERP_MARKET_PRECOMPILE_ADDRESS.as_slice()[18..],
            [0x04, 0x4e]
        );

        let withdraw = encode_max_withdraw_call([0x11; 20], 1, b"usdc");
        let withdraw_selector =
            alloy::primitives::keccak256("maxWithdrawAmountFor(address,uint8,bytes)");
        assert_eq!(&withdraw[..4], &withdraw_selector[..4]);

        let market = encode_perp_market_call(3);
        let market_selector = alloy::primitives::keccak256("perpMarkets(uint16)");
        assert_eq!(&market[..4], &market_selector[..4]);
        assert_eq!(market[35], 3);
    }

    #[rstest]
    fn max_withdraw_return_rejects_noncanonical_uint128() {
        let mut response = [0_u8; 32];
        response[31] = 42;
        assert_eq!(decode_max_withdraw_return(&response), Some(42));
        assert_eq!(decode_max_withdraw_return(&response[..31]), None);
        response[0] = 1;
        assert_eq!(decode_max_withdraw_return(&response), None);
    }

    #[rstest]
    fn max_withdraw_hex_rejects_noncanonical_envelopes() {
        let mut valid = [0_u8; 32];
        valid[31] = 42;
        assert_eq!(
            decode_max_withdraw_hex(&format!("0x{}", hex::encode(valid))),
            Some(42)
        );
        for encoded in [
            "0x".to_string(),
            "0x01".to_string(),
            format!("0x01{}", "00".repeat(31)),
            format!("0x{}00", "00".repeat(32)),
        ] {
            assert_eq!(decode_max_withdraw_hex(&encoded), None);
        }
    }
}
