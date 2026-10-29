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

use deepx_rust_sdk::signing::{self as sdk_signing, DeepXOfflineEvmWrapperSignError};
pub use deepx_rust_sdk::signing::{
    DeepXEvmTransactionKind, DeepXOfflineEvmExtrinsic, DeepXOfflineEvmSignError,
    DeepXOfflineEvmTransaction, DeepXOfflineEvmTransactionParams, DeepXOfflinePerpPnlEvmParams,
    sign_offline_evm_transaction,
};
use thiserror::Error;

use super::{DeepXRuntimeSnapshotPermit, SigningError};
use crate::common::DeepXPrivateKey;

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
    if params.chain_id != u64::from(crate::common::consts::DEEPX_TESTNET_CHAIN_ID) {
        return Err(DeepXOfflineEvmWrapperError::ChainId);
    }
    permit.ensure_active().map_err(SigningError::from)?;
    let snapshot = permit.snapshot();
    Ok(sdk_signing::sign_offline_evm_transact_extrinsic(
        snapshot.metadata(),
        snapshot.interfaces(),
        key,
        params,
    )?)
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
    sign_offline_evm_transact_extrinsic(permit, key, &params.into_transaction_params())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
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
            "/deepx-rust-sdk/test_data/runtime/testnet/",
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
}
