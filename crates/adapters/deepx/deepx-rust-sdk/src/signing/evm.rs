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

//! Explicit-parameter offline EVM transaction signing for DeepX.

use alloy::{
    consensus::{SignableTransaction, TxEip1559, TxLegacy},
    primitives::{Address, Bytes, Signature, TxKind, U256, keccak256},
    signers::{SignerSync, local::PrivateKeySigner},
    sol,
    sol_types::SolCall,
};
use thiserror::Error;

use super::pallet::DeepXPerpProfitAndLossPointParams;
use crate::{
    credential::DeepXPrivateKey, deployment::DEEPX_TESTNET_CHAIN_ID,
    rpc::precompile::DEEPX_PERP_MARKET_PRECOMPILE_ADDRESS,
};

sol! {
    function setProfitAndLossPoint(
        address subaccount,
        uint16 marketId,
        uint128 takeProfitPoint,
        uint128 stopLossPoint
    ) external;
}

/// EVM envelope mode selected explicitly by the caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeepXEvmTransactionKind {
    /// EIP-1559 envelope with zero fee caps, as in the pinned SDK.
    Eip1559,
    /// EIP-155 legacy envelope with zero gas price, as in the pinned SDK.
    Legacy,
}

/// Explicit inputs for an offline DeepX EVM precompile transaction.
///
/// Callers supply an independently verified precompile address, ABI calldata, nonce, gas limit,
/// and chain ID. This type does not discover or validate venue business semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXOfflineEvmTransactionParams {
    /// EVM envelope mode.
    pub kind: DeepXEvmTransactionKind,
    /// Chain ID included in the EVM signature.
    pub chain_id: u64,
    /// Explicit transaction nonce.
    pub nonce: u64,
    /// Explicit gas limit; no RPC gas estimation occurs.
    pub gas_limit: u64,
    /// Precompile destination address.
    pub precompile_address: [u8; 20],
    /// Wei transferred to the precompile.
    pub value: U256,
    /// Complete ABI calldata bytes.
    pub calldata: Vec<u8>,
}

/// Explicit envelope and call inputs for offline perpetual profit/loss-point management.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeepXOfflinePerpPnlEvmParams {
    /// EVM envelope mode.
    pub kind: DeepXEvmTransactionKind,
    /// Explicit timestamp nonce; reservation is owned by the caller.
    pub nonce: u64,
    /// Explicit gas limit; no RPC estimate occurs.
    pub gas_limit: u64,
    /// Exact runtime profit/loss-point arguments.
    pub call: DeepXPerpProfitAndLossPointParams,
}

impl DeepXOfflinePerpPnlEvmParams {
    /// Encodes the approved testnet precompile call and explicit EVM envelope inputs.
    #[must_use]
    pub fn into_transaction_params(self) -> DeepXOfflineEvmTransactionParams {
        let calldata = setProfitAndLossPointCall {
            subaccount: Address::from(self.call.subaccount),
            marketId: self.call.market_id,
            takeProfitPoint: self.call.take_profit_point,
            stopLossPoint: self.call.stop_loss_point,
        }
        .abi_encode();
        DeepXOfflineEvmTransactionParams {
            kind: self.kind,
            chain_id: u64::from(DEEPX_TESTNET_CHAIN_ID),
            nonce: self.nonce,
            gas_limit: self.gas_limit,
            precompile_address: DEEPX_PERP_MARKET_PRECOMPILE_ADDRESS.into_array(),
            value: U256::ZERO,
            calldata,
        }
    }
}

/// Signed EVM envelope and independently computed identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXOfflineEvmTransaction {
    /// Complete signed EVM transaction bytes.
    pub bytes: Vec<u8>,
    /// Keccak-256 hash of the complete signed EVM transaction.
    pub tx_hash: [u8; 32],
    /// Ethereum AccountId20 derived from the signing key.
    pub signer: [u8; 20],
    signature: Signature,
}

impl DeepXOfflineEvmTransaction {
    /// Returns the signature for the runtime's unsigned `Ethereum.transact` wrapper.
    #[must_use]
    pub const fn signature(&self) -> &Signature {
        &self.signature
    }
}

/// Signed EVM transaction embedded in an unsigned Substrate v4 extrinsic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXOfflineEvmExtrinsic {
    /// Signed Ethereum transaction and its Keccak identity.
    pub evm: DeepXOfflineEvmTransaction,
    /// Complete SCALE-encoded unsigned `Ethereum.transact` extrinsic.
    pub bytes: Vec<u8>,
    /// Blake2-256 hash of the complete SCALE extrinsic.
    pub extrinsic_hash: [u8; 32],
}

/// Errors before an offline EVM envelope is available.
#[derive(Debug, Error)]
pub enum DeepXOfflineEvmSignError {
    /// The transaction parameters cannot identify a valid explicit EVM envelope.
    #[error("invalid DeepX offline EVM transaction parameters")]
    InvalidParams,
    /// The private scalar is invalid.
    #[error("invalid DeepX secp256k1 signing key")]
    InvalidKey,
    /// The local EVM signer failed.
    #[error("DeepX offline EVM signing failed")]
    Sign,
}

/// Signs an explicit EVM envelope entirely offline, with the pinned SDK's zero-fee fields.
///
/// This primitive does not encode an ABI call, construct an unsigned `Ethereum.transact` wrapper,
/// allocate a nonce, inspect chain state, or submit a transaction.
///
/// # Errors
///
/// Returns an error for invalid inputs, key material, or signature construction.
pub fn sign_offline_evm_transaction(
    key: &DeepXPrivateKey,
    params: &DeepXOfflineEvmTransactionParams,
) -> Result<DeepXOfflineEvmTransaction, DeepXOfflineEvmSignError> {
    if params.chain_id == 0 || params.gas_limit == 0 || params.precompile_address == [0; 20] {
        return Err(DeepXOfflineEvmSignError::InvalidParams);
    }
    let signer = PrivateKeySigner::from_slice(key.as_bytes())
        .map_err(|_| DeepXOfflineEvmSignError::InvalidKey)?;
    let to = TxKind::Call(params.precompile_address.into());
    let input = Bytes::copy_from_slice(&params.calldata);
    let mut bytes = Vec::new();
    let signature = match params.kind {
        DeepXEvmTransactionKind::Eip1559 => {
            let tx = TxEip1559 {
                chain_id: params.chain_id,
                nonce: params.nonce,
                gas_limit: params.gas_limit,
                max_fee_per_gas: 0,
                max_priority_fee_per_gas: 0,
                to,
                value: params.value,
                input,
                ..Default::default()
            };
            let signature = signer
                .sign_hash_sync(&tx.signature_hash())
                .map_err(|_| DeepXOfflineEvmSignError::Sign)?;
            tx.into_signed(signature).eip2718_encode(&mut bytes);
            signature
        }
        DeepXEvmTransactionKind::Legacy => {
            let tx = TxLegacy {
                chain_id: Some(params.chain_id),
                nonce: params.nonce,
                gas_price: 0,
                gas_limit: params.gas_limit,
                to,
                value: params.value,
                input,
            };
            let signature = signer
                .sign_hash_sync(&tx.signature_hash())
                .map_err(|_| DeepXOfflineEvmSignError::Sign)?;
            tx.into_signed(signature).rlp_encode(&mut bytes);
            signature
        }
    };
    Ok(DeepXOfflineEvmTransaction {
        tx_hash: keccak256(&bytes).0,
        signer: signer.address().into_array(),
        bytes,
        signature,
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::credential::DeepXKeyScheme;

    #[rstest]
    #[case(
        DeepXEvmTransactionKind::Eip1559,
        "02f86f8212ee860191a203228c80808307a120941111111111111111111111111111111111111111808412345678c080a0639ada905c91501e47eced7b49deeed8cf16f17fd620b857bd5aee2cbb8218e3a0425d15bea327d1df586898c7fde06f5b6005b946860691a0c1c0389233f3eaf1",
        "34ed9fbf57152e47a2efbf4beb6e0f6ddf53571ed36c0b1c59a29a7b546dfa62"
    )]
    #[case(
        DeepXEvmTransactionKind::Legacy,
        "f86c860191a203228c808307a120941111111111111111111111111111111111111111808412345678822600a0833e8851cce34f8f0f1280e4d037df20b397c048a71df05e8019acefd34eca75a044126e0458821c52e1dbb4a08f1a9eb41247f831a17366c5c27045183bbfffee",
        "b8b6810903e60203709d97d61641fb7c110b035f4102c7d99d24740b366075d3"
    )]
    fn matches_pinned_explicit_parameter_vectors(
        #[case] kind: DeepXEvmTransactionKind,
        #[case] expected_bytes: &str,
        #[case] expected_hash: &str,
    ) {
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let params = DeepXOfflineEvmTransactionParams {
            kind,
            chain_id: 0x12ee,
            nonce: 1_725_000_000_140,
            gas_limit: 500_000,
            precompile_address: [0x11; 20],
            value: U256::ZERO,
            calldata: vec![0x12, 0x34, 0x56, 0x78],
        };
        let signed = sign_offline_evm_transaction(&key, &params).unwrap();
        assert_eq!(hex::encode(signed.bytes), expected_bytes);
        assert_eq!(hex::encode(signed.tx_hash), expected_hash);
        assert_eq!(
            hex::encode(signed.signer),
            "fcad0b19bb29d4674531d6f115237e16afce377c"
        );
    }
}
