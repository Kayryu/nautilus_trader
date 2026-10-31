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

//! System precompile account state at a selected block.

use alloy::{
    primitives::Address,
    sol,
    sol_types::{SolCall, SolValue},
};
use thiserror::Error;

use super::DeepXRpcClient;

/// System precompile deployed by DeepX.
pub const DEEPX_SYSTEM_PRECOMPILE_ADDRESS: Address = Address::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x52,
]);

sol! {
    struct DeepXSystemAccountAbi {
        uint64 nonce;
        uint64 update;
        uint64[] time_nonce;
        uint32 quota;
        bool is_exist;
    }

    function systemAccountV2(address account) external view returns (DeepXSystemAccountAbi);
}

/// Exact account state returned by the System V2 precompile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSystemAccount {
    /// Sequential account nonce.
    pub nonce: u64,
    /// Last account update value.
    pub update: u64,
    /// Timestamp nonce window.
    pub time_nonce: Vec<u64>,
    /// Available on-chain quota.
    pub quota: u32,
    /// Whether the runtime account exists.
    pub is_exist: bool,
}

/// Encodes one exact `systemAccountV2(address)` read.
#[must_use]
pub fn encode_system_account_call(account: [u8; 20]) -> Vec<u8> {
    systemAccountV2Call {
        account: Address::from(account),
    }
    .abi_encode()
}

/// Decodes a canonical System V2 precompile result.
#[must_use]
pub fn decode_system_account_return(response: &[u8]) -> Option<DeepXSystemAccount> {
    let account = systemAccountV2Call::abi_decode_returns(response).ok()?;
    if account.abi_encode() != response {
        return None;
    }
    Some(DeepXSystemAccount {
        nonce: account.nonce,
        update: account.update,
        time_nonce: account.time_nonce,
        quota: account.quota,
        is_exist: account.is_exist,
    })
}

/// Errors while reading an account from the System precompile.
#[derive(Debug, Error)]
pub enum DeepXSystemAccountReadError {
    /// The pinned-block EVM call failed.
    #[error("DeepX System account RPC failed: {0}")]
    Rpc(#[source] anyhow::Error),
    /// The precompile response was not a canonical V2 account tuple.
    #[error("DeepX System account V2 response is invalid")]
    InvalidResponse,
}

/// Reads one System account at an EVM block number corresponding to a finalized chain block.
///
/// The caller establishes the canonical finalized block and endpoint identity.
///
/// # Errors
///
/// Returns an error for RPC failure or malformed V2 ABI data.
pub async fn get_system_account_at_block_number(
    client: &DeepXRpcClient,
    block_number: u64,
    account: [u8; 20],
) -> Result<DeepXSystemAccount, DeepXSystemAccountReadError> {
    let encoded: String = client
        .eth_call(
            &DEEPX_SYSTEM_PRECOMPILE_ADDRESS.to_string(),
            &encode_system_account_call(account),
            &format!("0x{block_number:x}"),
        )
        .await
        .map_err(DeepXSystemAccountReadError::Rpc)?;
    let response = encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())
        .ok_or(DeepXSystemAccountReadError::InvalidResponse)?;
    decode_system_account_return(&response).ok_or(DeepXSystemAccountReadError::InvalidResponse)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn system_account_v2_abi_is_canonical() {
        let account = [0x11; 20];
        let encoded = encode_system_account_call(account);
        let selector = alloy::primitives::keccak256("systemAccountV2(address)");
        assert_eq!(&encoded[..4], &selector[..4]);
        assert_eq!(&encoded[4..16], &[0; 12]);
        assert_eq!(&encoded[16..], &account);

        let result = DeepXSystemAccountAbi {
            nonce: 7,
            update: 8,
            time_nonce: vec![10, 11],
            quota: 12,
            is_exist: true,
        }
        .abi_encode();
        assert_eq!(
            decode_system_account_return(&result),
            Some(DeepXSystemAccount {
                nonce: 7,
                update: 8,
                time_nonce: vec![10, 11],
                quota: 12,
                is_exist: true,
            })
        );
        assert_eq!(
            decode_system_account_return(&result[..result.len() - 1]),
            None
        );
        let mut noncanonical = result;
        noncanonical.extend_from_slice(&[0; 32]);
        assert_eq!(decode_system_account_return(&noncanonical), None);
    }
}
