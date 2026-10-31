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
use subxt_core::{config::substrate::BlakeTwo256, dynamic::Value, metadata::Metadata, tx};
use thiserror::Error;

use super::{
    DeepXRuntimeSnapshotPermit, SigningError,
    pallet::DeepXPerpProfitAndLossPointParams,
    runtime::DeepXRuntimeConfig,
    runtime_interface::{DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError},
};
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

/// Explicit EIP-1559 envelope for a bridge contract on a selected EVM chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXBridgeEvmTransactionParams {
    /// Chain ID independently verified against the source RPC endpoint.
    pub chain_id: u64,
    /// Sequential EVM account nonce reserved by the caller.
    pub nonce: u64,
    /// Explicit gas limit.
    pub gas_limit: u64,
    /// Maximum fee per gas in wei.
    pub max_fee_per_gas: u128,
    /// Maximum priority fee per gas in wei.
    pub max_priority_fee_per_gas: u128,
    /// Verified token or bridge contract address.
    pub contract_address: [u8; 20],
    /// Wei sent with the contract call.
    pub value: U256,
    /// Complete ABI calldata for approval or bridge-out.
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

/// Errors before an offline EVM transaction can be wrapped in a runtime extrinsic.
#[derive(Debug, Error)]
pub enum DeepXOfflineEvmWrapperSignError {
    /// The requested chain ID does not match the DeepX testnet EVM deployment.
    #[error("DeepX testnet EVM chain ID must be 4846")]
    ChainId,
    /// The EVM signing inputs or signature were invalid.
    #[error(transparent)]
    Evm(#[from] DeepXOfflineEvmSignError),
    /// The runtime does not expose the expected Ethereum call.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The runtime metadata could not encode the unsigned wrapper.
    #[error("unable to encode DeepX EVM wrapper: {0}")]
    Encode(#[source] Box<subxt_core::Error>),
}

/// Errors before a permit-bound offline EVM wrapper is available.
#[derive(Debug, Error)]
pub enum DeepXOfflineEvmWrapperError {
    /// The requested chain ID does not match the approved DeepX testnet EVM deployment.
    #[error("DeepX testnet EVM chain ID must be 4846")]
    ChainId,
    /// The EVM signing inputs or signature were invalid.
    #[error(transparent)]
    Evm(#[from] DeepXOfflineEvmSignError),
    /// The approved runtime permit or SCALE metadata rejected the wrapper.
    #[error(transparent)]
    Runtime(#[from] SigningError),
}

impl From<DeepXOfflineEvmWrapperSignError> for DeepXOfflineEvmWrapperError {
    fn from(value: DeepXOfflineEvmWrapperSignError) -> Self {
        match value {
            DeepXOfflineEvmWrapperSignError::ChainId => Self::ChainId,
            DeepXOfflineEvmWrapperSignError::Evm(e) => Self::Evm(e),
            DeepXOfflineEvmWrapperSignError::RuntimeInterface(e) => {
                Self::Runtime(SigningError::RuntimeInterface(e))
            }
            DeepXOfflineEvmWrapperSignError::Encode(e) => Self::Runtime(SigningError::Encode(e)),
        }
    }
}

impl From<subxt_core::Error> for DeepXOfflineEvmWrapperSignError {
    fn from(value: subxt_core::Error) -> Self {
        Self::Encode(Box::new(value))
    }
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

/// Signs a fee-aware bridge EVM transaction without submitting it.
///
/// The caller verifies the chain ID, contract address, gas estimate, account nonce, and
/// authorization before signing. The returned hash identifies the raw EVM transaction.
///
/// # Errors
///
/// Returns an error for invalid envelope inputs or a failed secp256k1 signature.
pub fn sign_bridge_evm_transaction(
    key: &DeepXPrivateKey,
    params: &DeepXBridgeEvmTransactionParams,
) -> Result<DeepXOfflineEvmTransaction, DeepXOfflineEvmSignError> {
    if params.chain_id == 0
        || params.gas_limit == 0
        || params.contract_address == [0; 20]
        || params.max_priority_fee_per_gas > params.max_fee_per_gas
    {
        return Err(DeepXOfflineEvmSignError::InvalidParams);
    }
    let signer = PrivateKeySigner::from_slice(key.as_bytes())
        .map_err(|_| DeepXOfflineEvmSignError::InvalidKey)?;
    let transaction = TxEip1559 {
        chain_id: params.chain_id,
        nonce: params.nonce,
        gas_limit: params.gas_limit,
        max_fee_per_gas: params.max_fee_per_gas,
        max_priority_fee_per_gas: params.max_priority_fee_per_gas,
        to: TxKind::Call(params.contract_address.into()),
        value: params.value,
        input: Bytes::copy_from_slice(&params.calldata),
        ..Default::default()
    };
    let signature = signer
        .sign_hash_sync(&transaction.signature_hash())
        .map_err(|_| DeepXOfflineEvmSignError::Sign)?;
    let mut bytes = Vec::new();
    transaction
        .into_signed(signature)
        .eip2718_encode(&mut bytes);
    Ok(DeepXOfflineEvmTransaction {
        tx_hash: keccak256(&bytes).0,
        signer: signer.address().into_array(),
        bytes,
        signature,
    })
}

/// Signs an EVM envelope and embeds it in a DeepX unsigned runtime extrinsic offline.
///
/// The caller validates runtime identity and metadata before supplying them. This function
/// performs no nonce reservation, authorization, submission, or finality verification.
///
/// # Errors
///
/// Returns an error for an unexpected chain ID, invalid EVM inputs, an unavailable runtime call,
/// or failed SCALE encoding.
pub fn sign_offline_evm_transact_extrinsic(
    metadata: &Metadata,
    interfaces: &DeepXRuntimeInterfaceCatalog,
    key: &DeepXPrivateKey,
    params: &DeepXOfflineEvmTransactionParams,
) -> Result<DeepXOfflineEvmExtrinsic, DeepXOfflineEvmWrapperSignError> {
    if params.chain_id != u64::from(DEEPX_TESTNET_CHAIN_ID) {
        return Err(DeepXOfflineEvmWrapperSignError::ChainId);
    }
    interfaces.call("Ethereum", "transact")?;
    let evm = sign_offline_evm_transaction(key, params)?;
    let signature = evm.signature();
    let action = Value::unnamed_variant("Call", [Value::from_bytes(params.precompile_address)]);
    let r = Value::from_bytes(signature.r().to_be_bytes::<32>());
    let s = Value::from_bytes(signature.s().to_be_bytes::<32>());
    let transaction = match params.kind {
        DeepXEvmTransactionKind::Eip1559 => Value::unnamed_variant(
            "EIP1559",
            [Value::named_composite([
                ("chain_id", Value::u128(u128::from(params.chain_id))),
                ("nonce", runtime_u256(U256::from(params.nonce))),
                ("max_priority_fee_per_gas", runtime_u256(U256::ZERO)),
                ("max_fee_per_gas", runtime_u256(U256::ZERO)),
                ("gas_limit", runtime_u256(U256::from(params.gas_limit))),
                ("action", action),
                ("value", runtime_u256(params.value)),
                ("input", Value::from_bytes(&params.calldata)),
                ("access_list", Value::unnamed_composite(Vec::<Value>::new())),
                ("odd_y_parity", Value::bool(signature.v())),
                ("r", r),
                ("s", s),
            ])],
        ),
        DeepXEvmTransactionKind::Legacy => Value::unnamed_variant(
            "Legacy",
            [Value::named_composite([
                ("nonce", runtime_u256(U256::from(params.nonce))),
                ("gas_price", runtime_u256(U256::ZERO)),
                ("gas_limit", runtime_u256(U256::from(params.gas_limit))),
                ("action", action),
                ("value", runtime_u256(params.value)),
                ("input", Value::from_bytes(&params.calldata)),
                (
                    "signature",
                    Value::named_composite([
                        ("v", Value::u128(u128::from(signature.v()))),
                        ("r", r),
                        ("s", s),
                    ]),
                ),
            ])],
        ),
    };
    let payload = subxt_core::dynamic::tx(
        "Ethereum",
        "transact",
        vec![transaction, Value::from_bytes(evm.signer)],
    );
    let wrapper = tx::create_v4_unsigned::<DeepXRuntimeConfig, _>(&payload, metadata)?;
    let extrinsic_hash = wrapper.hash_with(BlakeTwo256).0;
    Ok(DeepXOfflineEvmExtrinsic {
        evm,
        bytes: wrapper.into_encoded(),
        extrinsic_hash,
    })
}

/// Signs an EVM envelope and embeds it in an active runtime's unsigned wrapper offline.
///
/// # Errors
///
/// Returns an error if the chain ID, signer, active permit, or runtime encoding is invalid.
pub fn sign_permitted_evm_transact_extrinsic(
    permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: &DeepXOfflineEvmTransactionParams,
) -> Result<DeepXOfflineEvmExtrinsic, DeepXOfflineEvmWrapperError> {
    if params.chain_id != u64::from(DEEPX_TESTNET_CHAIN_ID) {
        return Err(DeepXOfflineEvmWrapperError::ChainId);
    }
    permit.ensure_active().map_err(SigningError::from)?;
    let snapshot = permit.snapshot();
    Ok(sign_offline_evm_transact_extrinsic(
        snapshot.metadata(),
        snapshot.interfaces(),
        key,
        params,
    )?)
}

/// Signs a testnet Perp profit/loss-point precompile call and its runtime wrapper offline.
///
/// # Errors
///
/// Returns an error if the active permit, key, or envelope inputs are invalid.
pub fn sign_permitted_perp_pnl_evm_transact_extrinsic(
    permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXOfflinePerpPnlEvmParams,
) -> Result<DeepXOfflineEvmExtrinsic, DeepXOfflineEvmWrapperError> {
    sign_permitted_evm_transact_extrinsic(permit, key, &params.into_transaction_params())
}

fn runtime_u256(value: U256) -> Value {
    let limbs = value.into_limbs().map(|limb| Value::u128(u128::from(limb)));
    Value::unnamed_composite([Value::unnamed_composite(limbs)])
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde::Deserialize;
    use subxt_core::metadata;

    use super::*;
    use crate::{
        credential::DeepXKeyScheme,
        enums::DeepXEnvironment,
        signing::{DeepXRuntimeSnapshotService, RuntimeSnapshot},
    };

    const SPEC376_METADATA: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test_data/runtime/testnet/",
        "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
    ));

    #[derive(Deserialize)]
    struct RpcResponse {
        result: String,
    }

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

    #[rstest]
    #[case(
        DeepXEvmTransactionKind::Eip1559,
        "710404290002ee120000000000008c2203a2910100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020a1070000000000000000000000000000000000000000000000000000000000001111111111111111111111111111111111111111000000000000000000000000000000000000000000000000000000000000000010123456780000639ada905c91501e47eced7b49deeed8cf16f17fd620b857bd5aee2cbb8218e3425d15bea327d1df586898c7fde06f5b6005b946860691a0c1c0389233f3eaf1fcad0b19bb29d4674531d6f115237e16afce377c"
    )]
    #[case(
        DeepXEvmTransactionKind::Legacy,
        "e903042900008c2203a291010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020a1070000000000000000000000000000000000000000000000000000000000001111111111111111111111111111111111111111000000000000000000000000000000000000000000000000000000000000000010123456780100000000000000833e8851cce34f8f0f1280e4d037df20b397c048a71df05e8019acefd34eca7544126e0458821c52e1dbb4a08f1a9eb41247f831a17366c5c27045183bbfffeefcad0b19bb29d4674531d6f115237e16afce377c"
    )]
    fn matches_captured_unsigned_wrapper_vectors(
        #[case] kind: DeepXEvmTransactionKind,
        #[case] expected_bytes: &str,
    ) {
        let response: RpcResponse = serde_json::from_str(SPEC376_METADATA).unwrap();
        let metadata_bytes = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let metadata = metadata::decode_from(&metadata_bytes).unwrap();
        let interfaces = DeepXRuntimeInterfaceCatalog::from_metadata(&metadata).unwrap();
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
        let signed =
            sign_offline_evm_transact_extrinsic(&metadata, &interfaces, &key, &params).unwrap();

        assert_eq!(hex::encode(signed.bytes), expected_bytes);

        let mut foreign_chain = params;
        foreign_chain.chain_id = 1;
        assert!(matches!(
            sign_offline_evm_transact_extrinsic(&metadata, &interfaces, &key, &foreign_chain),
            Err(DeepXOfflineEvmWrapperSignError::ChainId)
        ));
    }

    #[rstest]
    fn evm_envelope_rejects_invalid_scope_and_binds_inputs() {
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let params = DeepXOfflineEvmTransactionParams {
            kind: DeepXEvmTransactionKind::Eip1559,
            chain_id: 0x12ee,
            nonce: 1_725_000_000_140,
            gas_limit: 500_000,
            precompile_address: [0x11; 20],
            value: U256::ZERO,
            calldata: vec![0x12, 0x34, 0x56, 0x78],
        };
        let mut invalid = params.clone();
        invalid.chain_id = 0;
        assert!(sign_offline_evm_transaction(&key, &invalid).is_err());
        invalid = params.clone();
        invalid.gas_limit = 0;
        assert!(sign_offline_evm_transaction(&key, &invalid).is_err());
        invalid = params.clone();
        invalid.precompile_address = [0; 20];
        assert!(sign_offline_evm_transaction(&key, &invalid).is_err());

        let baseline = sign_offline_evm_transaction(&key, &params).unwrap();
        let mut changed = params.clone();
        changed.calldata.push(0x42);
        assert_ne!(
            sign_offline_evm_transaction(&key, &changed)
                .unwrap()
                .tx_hash,
            baseline.tx_hash,
        );
        changed = params;
        changed.nonce += 1;
        assert_ne!(
            sign_offline_evm_transaction(&key, &changed)
                .unwrap()
                .tx_hash,
            baseline.tx_hash,
        );
    }

    #[rstest]
    #[case(
        DeepXEvmTransactionKind::Eip1559,
        "2ab5f44c34d307b8e33b6967968fda3e6fb7ff21c7326cd9c5208e7c3b0ead4f",
        "1cb8be646af7de0e8ce0a779df69db5eccd543fa7383707c106c9322368f82d7"
    )]
    #[case(
        DeepXEvmTransactionKind::Legacy,
        "72ee4be55ce21d3a857c3d59ed064a75557ba83edeee30a552af3a39d68d2435",
        "af40605ce99f7d31b18662700704ec99c58a6d54369a41a6e85e3d9f48e296db"
    )]
    fn perp_pnl_precompile_matches_captured_hashes(
        #[case] kind: DeepXEvmTransactionKind,
        #[case] expected_evm_hash: &str,
        #[case] expected_wrapper_hash: &str,
    ) {
        let response: RpcResponse = serde_json::from_str(SPEC376_METADATA).unwrap();
        let metadata_bytes = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let metadata = metadata::decode_from(&metadata_bytes).unwrap();
        let interfaces = DeepXRuntimeInterfaceCatalog::from_metadata(&metadata).unwrap();
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let params = DeepXOfflinePerpPnlEvmParams {
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
        let signed =
            sign_offline_evm_transact_extrinsic(&metadata, &interfaces, &key, &params).unwrap();
        assert!(
            signed
                .evm
                .bytes
                .windows(4)
                .any(|bytes| bytes == [0xad, 0xc0, 0x86, 0xbe])
        );
        assert_eq!(hex::encode(signed.evm.tx_hash), expected_evm_hash);
        assert_eq!(hex::encode(signed.extrinsic_hash), expected_wrapper_hash);
    }

    #[rstest]
    fn permitted_wrapper_requires_testnet_chain_and_active_runtime() {
        let response: RpcResponse = serde_json::from_str(SPEC376_METADATA).unwrap();
        let metadata = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let genesis =
            hex::decode("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap();
        let snapshot = RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            genesis.try_into().unwrap(),
            376,
            1,
            &metadata,
        )
        .unwrap();
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let mut params = DeepXOfflineEvmTransactionParams {
            kind: DeepXEvmTransactionKind::Eip1559,
            chain_id: 1,
            nonce: 1_725_000_000_140,
            gas_limit: 500_000,
            precompile_address: [0x11; 20],
            value: U256::ZERO,
            calldata: vec![0x12, 0x34, 0x56, 0x78],
        };

        assert!(matches!(
            sign_permitted_evm_transact_extrinsic(&permit, &key, &params),
            Err(DeepXOfflineEvmWrapperError::ChainId),
        ));
        params.chain_id = u64::from(DEEPX_TESTNET_CHAIN_ID);
        let baseline = sign_permitted_evm_transact_extrinsic(&permit, &key, &params).unwrap();
        params.value = U256::from(123_u64);
        let changed = sign_permitted_evm_transact_extrinsic(&permit, &key, &params).unwrap();
        assert_ne!(baseline.bytes, changed.bytes);
        assert_ne!(baseline.extrinsic_hash, changed.extrinsic_hash);

        service.halt_for_inconsistent_evidence().unwrap();
        assert!(matches!(
            sign_permitted_evm_transact_extrinsic(&permit, &key, &params),
            Err(DeepXOfflineEvmWrapperError::Runtime(
                SigningError::RuntimePermit(_)
            )),
        ));
    }

    #[rstest]
    fn bridge_signer_binds_chain_nonce_fees_and_calldata() {
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let mut params = DeepXBridgeEvmTransactionParams {
            chain_id: 4856,
            nonce: 7,
            gas_limit: 200_000,
            max_fee_per_gas: 20_000_000_000,
            max_priority_fee_per_gas: 1_000_000_000,
            contract_address: [0x11; 20],
            value: U256::from(123_u64),
            calldata: vec![0x12, 0x34],
        };
        let baseline = sign_bridge_evm_transaction(&key, &params).unwrap();
        assert_eq!(baseline.tx_hash, keccak256(&baseline.bytes).0);
        assert_eq!(baseline.bytes[0], 0x02);
        params.max_fee_per_gas += 1;
        assert_ne!(
            sign_bridge_evm_transaction(&key, &params).unwrap().tx_hash,
            baseline.tx_hash
        );
        params.max_fee_per_gas -= 1;
        params.nonce += 1;
        assert_ne!(
            sign_bridge_evm_transaction(&key, &params).unwrap().tx_hash,
            baseline.tx_hash
        );
        params.nonce -= 1;
        params.chain_id += 1;
        assert_ne!(
            sign_bridge_evm_transaction(&key, &params).unwrap().tx_hash,
            baseline.tx_hash
        );
        params.chain_id -= 1;
        params.calldata.push(0x56);
        assert_ne!(
            sign_bridge_evm_transaction(&key, &params).unwrap().tx_hash,
            baseline.tx_hash
        );
        params.max_priority_fee_per_gas = params.max_fee_per_gas + 1;
        assert!(matches!(
            sign_bridge_evm_transaction(&key, &params),
            Err(DeepXOfflineEvmSignError::InvalidParams)
        ));
    }
}
