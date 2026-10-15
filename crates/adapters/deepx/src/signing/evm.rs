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

//! Offline EVM transaction envelopes matching the pinned DeepX SDK's explicit-parameter path.

use alloy::{
    consensus::{SignableTransaction, TxEip1559, TxLegacy},
    primitives::{Address, Bytes, Signature, TxKind, U256, keccak256},
    signers::{SignerSync, local::PrivateKeySigner},
    sol,
    sol_types::SolCall,
};
use subxt_core::{config::substrate::BlakeTwo256, dynamic::Value, tx};
use thiserror::Error;

use super::{
    DeepXPerpProfitAndLossPointParams, DeepXRuntimeConfig, DeepXRuntimeSnapshotPermit, SigningError,
};
use crate::common::DeepXPrivateKey;

const DEEPX_TESTNET_PERP_PRECOMPILE: [u8; 20] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x4e,
];

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

/// Signed EVM envelope and independently computed identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXOfflineEvmTransaction {
    /// Complete signed EVM transaction bytes.
    pub bytes: Vec<u8>,
    /// Keccak-256 hash of the complete signed EVM transaction.
    pub tx_hash: [u8; 32],
    /// Ethereum AccountId20 derived from the signing key.
    pub signer: [u8; 20],
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

/// Errors before an offline legacy EVM wrapper is available.
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
    sign_offline_evm_transaction_parts(key, params).map(|(transaction, _)| transaction)
}

fn sign_offline_evm_transaction_parts(
    key: &DeepXPrivateKey,
    params: &DeepXOfflineEvmTransactionParams,
) -> Result<(DeepXOfflineEvmTransaction, Signature), DeepXOfflineEvmSignError> {
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
    Ok((
        DeepXOfflineEvmTransaction {
            tx_hash: keccak256(&bytes).0,
            signer: signer.address().into_array(),
            bytes,
        },
        signature,
    ))
}

/// Signs an EVM envelope and embeds it in the approved runtime's unsigned wrapper offline.
///
/// The caller still owns precompile discovery, ABI encoding, nonce reservation, persistence,
/// authorization, submission, and reconciliation. No network operation occurs here.
///
/// # Errors
///
/// Returns an error if the chain ID, signer, active snapshot, or runtime encoding is invalid.
pub fn sign_offline_evm_transact_extrinsic(
    permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: &DeepXOfflineEvmTransactionParams,
) -> Result<DeepXOfflineEvmExtrinsic, DeepXOfflineEvmWrapperError> {
    if params.chain_id != 4846 {
        return Err(DeepXOfflineEvmWrapperError::ChainId);
    }
    permit.ensure_active().map_err(SigningError::from)?;
    let snapshot = permit.snapshot();
    snapshot
        .interfaces()
        .call("Ethereum", "transact")
        .map_err(SigningError::from)?;
    let (evm, signature) = sign_offline_evm_transaction_parts(key, params)?;
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
    let wrapper = tx::create_v4_unsigned::<DeepXRuntimeConfig, _>(&payload, snapshot.metadata())
        .map_err(SigningError::from)?;
    let extrinsic_hash = wrapper.hash_with(BlakeTwo256).0;
    Ok(DeepXOfflineEvmExtrinsic {
        evm,
        bytes: wrapper.into_encoded(),
        extrinsic_hash,
    })
}

/// Signs a testnet Perp profit/loss-point precompile call and its unsigned runtime wrapper.
///
/// This matches the chain source's `0x044e` precompile and its four-argument ABI. It performs no
/// nonce reservation, persistence, authorization, network access, or submission.
///
/// # Errors
///
/// Returns an error if the active runtime permit, key, or envelope inputs are invalid.
pub fn sign_offline_perp_pnl_evm_transact_extrinsic(
    permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXOfflinePerpPnlEvmParams,
) -> Result<DeepXOfflineEvmExtrinsic, DeepXOfflineEvmWrapperError> {
    let calldata = setProfitAndLossPointCall {
        subaccount: Address::from(params.call.subaccount),
        marketId: params.call.market_id,
        takeProfitPoint: params.call.take_profit_point,
        stopLossPoint: params.call.stop_loss_point,
    }
    .abi_encode();
    sign_offline_evm_transact_extrinsic(
        permit,
        key,
        &DeepXOfflineEvmTransactionParams {
            kind: params.kind,
            chain_id: 4846,
            nonce: params.nonce,
            gas_limit: params.gas_limit,
            precompile_address: DEEPX_TESTNET_PERP_PRECOMPILE,
            value: U256::ZERO,
            calldata,
        },
    )
}

fn runtime_u256(value: U256) -> Value {
    let limbs = value.into_limbs().map(|limb| Value::u128(u128::from(limb)));
    Value::unnamed_composite([Value::unnamed_composite(limbs)])
}

#[cfg(test)]
mod tests {
    use nautilus_core::hex;
    use rstest::rstest;
    use serde::Deserialize;

    use super::*;
    use crate::{
        common::{DeepXEnvironment, DeepXKeyScheme},
        signing::{DeepXReadOnlyRuntimeSnapshot, DeepXRuntimeSnapshotService, RuntimeSnapshot},
    };

    #[derive(Deserialize)]
    struct RpcResponse {
        result: String,
    }

    fn snapshot() -> RuntimeSnapshot {
        let metadata: RpcResponse = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        let read_only = DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap(),
            376,
            1,
            &hex::decode(metadata.result.trim_start_matches("0x")).unwrap(),
        )
        .unwrap();
        RuntimeSnapshot::from_read_only_for_test(read_only)
    }

    fn key() -> DeepXPrivateKey {
        DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap()
    }

    fn params(kind: DeepXEvmTransactionKind) -> DeepXOfflineEvmTransactionParams {
        DeepXOfflineEvmTransactionParams {
            kind,
            chain_id: 0x12ee,
            nonce: 1_725_000_000_140,
            gas_limit: 500_000,
            precompile_address: [0x11; 20],
            value: U256::ZERO,
            calldata: vec![0x12, 0x34, 0x56, 0x78],
        }
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
    fn matches_pinned_sdk_explicit_parameter_vectors(
        #[case] kind: DeepXEvmTransactionKind,
        #[case] expected_bytes: &str,
        #[case] expected_hash: &str,
    ) {
        let signed = sign_offline_evm_transaction(&key(), &params(kind)).unwrap();
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
    fn matches_pinned_sdk_unsigned_wrapper_vectors(
        #[case] kind: DeepXEvmTransactionKind,
        #[case] expected_bytes: &str,
    ) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let wrapper = sign_offline_evm_transact_extrinsic(&permit, &key(), &params(kind)).unwrap();
        assert_eq!(hex::encode(wrapper.bytes), expected_bytes);
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
    fn perp_pnl_precompile_matches_sdk_envelope_and_wrapper_hashes(
        #[case] kind: DeepXEvmTransactionKind,
        #[case] expected_evm_hash: &str,
        #[case] expected_wrapper_hash: &str,
    ) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let signed = sign_offline_perp_pnl_evm_transact_extrinsic(
            &permit,
            &key(),
            DeepXOfflinePerpPnlEvmParams {
                kind,
                nonce: 1_725_000_000_140,
                gas_limit: 500_000,
                call: DeepXPerpProfitAndLossPointParams {
                    subaccount: [0x11; 20],
                    market_id: 7,
                    take_profit_point: 123,
                    stop_loss_point: 456,
                },
            },
        )
        .unwrap();
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
    fn wrapper_requires_testnet_chain_and_active_runtime_permit() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut request = params(DeepXEvmTransactionKind::Eip1559);
        request.chain_id = 1;
        assert!(matches!(
            sign_offline_evm_transact_extrinsic(&permit, &key(), &request),
            Err(DeepXOfflineEvmWrapperError::ChainId)
        ));

        request.chain_id = 4846;
        let baseline = sign_offline_evm_transact_extrinsic(&permit, &key(), &request).unwrap();
        request.value = U256::from(123_u64);
        let changed = sign_offline_evm_transact_extrinsic(&permit, &key(), &request).unwrap();
        assert_ne!(baseline.bytes, changed.bytes);
        assert_ne!(baseline.extrinsic_hash, changed.extrinsic_hash);

        service.halt_for_inconsistent_evidence().unwrap();
        assert!(matches!(
            sign_offline_evm_transact_extrinsic(&permit, &key(), &request),
            Err(DeepXOfflineEvmWrapperError::Runtime(
                SigningError::RuntimePermit(_)
            ))
        ));
    }

    #[rstest]
    fn rejects_invalid_envelope_scope_and_binds_every_input() {
        let mut request = params(DeepXEvmTransactionKind::Eip1559);
        request.chain_id = 0;
        assert!(sign_offline_evm_transaction(&key(), &request).is_err());
        request = params(DeepXEvmTransactionKind::Eip1559);
        request.gas_limit = 0;
        assert!(sign_offline_evm_transaction(&key(), &request).is_err());
        request = params(DeepXEvmTransactionKind::Eip1559);
        request.precompile_address = [0; 20];
        assert!(sign_offline_evm_transaction(&key(), &request).is_err());

        let baseline =
            sign_offline_evm_transaction(&key(), &params(DeepXEvmTransactionKind::Eip1559))
                .unwrap();
        let mut changed = params(DeepXEvmTransactionKind::Eip1559);
        changed.calldata.push(0x42);
        assert_ne!(
            sign_offline_evm_transaction(&key(), &changed)
                .unwrap()
                .tx_hash,
            baseline.tx_hash
        );
        changed = params(DeepXEvmTransactionKind::Eip1559);
        changed.nonce += 1;
        assert_ne!(
            sign_offline_evm_transaction(&key(), &changed)
                .unwrap()
                .tx_hash,
            baseline.tx_hash
        );
    }
}
