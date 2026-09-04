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

//! Fixture-gated DeepX runtime snapshots and offline pallet signing.

mod snapshot;

pub use snapshot::{
    ApprovedRuntimeIdentity, DeepXRuntimeChangeDecision, DeepXRuntimeInterfaceCatalog,
    DeepXRuntimeInterfaceError, DeepXRuntimePalletInterface, DeepXRuntimeSnapshotPermit,
    DeepXRuntimeSnapshotService, DeepXRuntimeSnapshotServiceError, DeepXRuntimeSnapshotUpdate,
    DeepXRuntimeVariantIdentity, RuntimeSnapshot, SnapshotError,
};
use subxt_core::{
    Config,
    config::{DefaultExtrinsicParamsBuilder, Hasher, substrate::BlakeTwo256},
    dynamic::Value,
    tx,
    utils::AccountId20,
};
use subxt_signer::eth::{Keypair, Signature};
use thiserror::Error;

use crate::common::DeepXPrivateKey;

/// DeepX runtime types required for AccountId20 Ethereum-compatible signatures.
#[derive(Clone, Copy, Debug)]
pub enum DeepXRuntimeConfig {}

impl Config for DeepXRuntimeConfig {
    type AccountId = AccountId20;
    type Address = AccountId20;
    type Signature = Signature;
    type Hasher = BlakeTwo256;
    type Header = subxt_core::config::substrate::SubstrateHeader<u32, Self::Hasher>;
    type ExtrinsicParams = subxt_core::config::SubstrateExtrinsicParams<Self>;
    type AssetId = u32;
}

/// Errors produced before an extrinsic can be signed offline.
#[derive(Debug, Error)]
pub enum SigningError {
    /// The private scalar was rejected by the pinned DeepX signer implementation.
    #[error("invalid DeepX secp256k1 signing key")]
    InvalidKey,
    /// The dynamic call or transaction extensions could not be SCALE encoded.
    #[error("unable to encode DeepX pallet extrinsic: {0}")]
    Encode(#[source] Box<subxt_core::Error>),
}

impl From<subxt_core::Error> for SigningError {
    fn from(value: subxt_core::Error) -> Self {
        Self::Encode(Box::new(value))
    }
}

/// A signed SCALE extrinsic and its deterministic identities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedPalletExtrinsic {
    /// Complete compact-length-prefixed SCALE extrinsic bytes.
    pub(crate) bytes: Vec<u8>,
    /// Blake2-256 hash of the complete extrinsic bytes.
    pub(crate) extrinsic_hash: [u8; 32],
    /// Ethereum AccountId20 derived from the signing key.
    pub(crate) signer: [u8; 20],
    /// Explicit nonce encoded in the signed extensions.
    pub(crate) nonce: u64,
    /// Approved runtime identity used to encode and sign the extrinsic.
    pub(crate) runtime: ApprovedRuntimeIdentity,
}

impl SignedPalletExtrinsic {
    /// Returns the complete SCALE extrinsic bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the Blake2-256 hash of the complete extrinsic bytes.
    #[must_use]
    pub const fn extrinsic_hash(&self) -> [u8; 32] {
        self.extrinsic_hash
    }

    /// Returns the signing AccountId20.
    #[must_use]
    pub const fn signer(&self) -> [u8; 20] {
        self.signer
    }

    /// Returns the nonce encoded in the signed extensions.
    #[must_use]
    pub const fn nonce(&self) -> u64 {
        self.nonce
    }

    /// Returns the approved runtime identity used for signing.
    #[must_use]
    pub const fn runtime(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime
    }

    pub(crate) fn has_valid_hash(&self) -> bool {
        BlakeTwo256.hash(&self.bytes).0 == self.extrinsic_hash
    }
}

/// Signs a metadata-driven DeepX pallet call without submitting it.
///
/// The caller supplies an explicit nonce and a runtime snapshot permit. Requiring the permit binds
/// public signing to the runtime-change quiescence boundary. This function performs no nonce
/// allocation, persistence, network access, retry, or submission, so it cannot make a trading
/// capability operational.
///
/// # Errors
///
/// Returns an error when the key is invalid or the call cannot be encoded against the approved
/// runtime snapshot.
pub fn sign_dynamic_pallet_call(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    pallet: &str,
    call: &str,
    arguments: Vec<Value>,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call_with_snapshot(
        snapshot_permit.snapshot(),
        key,
        pallet,
        call,
        arguments,
        nonce,
    )
}

fn sign_dynamic_pallet_call_with_snapshot(
    snapshot: &RuntimeSnapshot,
    key: &DeepXPrivateKey,
    pallet: &str,
    call: &str,
    arguments: Vec<Value>,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    let signer = Keypair::from_secret_key(*key.as_bytes()).map_err(|_| SigningError::InvalidKey)?;
    let account_id = signer.public_key().to_account_id();
    let payload = subxt_core::dynamic::tx(pallet, call, arguments);
    let params = DefaultExtrinsicParamsBuilder::<DeepXRuntimeConfig>::new()
        .nonce(nonce)
        .build();
    let transaction =
        tx::create_v4_signed(&payload, snapshot.client_state(), params)?.sign(&signer);
    let hash = transaction.hash_with(BlakeTwo256);

    Ok(SignedPalletExtrinsic {
        bytes: transaction.into_encoded(),
        extrinsic_hash: hash.0,
        signer: account_id.0,
        nonce,
        runtime: snapshot.identity().clone(),
    })
}

#[cfg(test)]
mod tests {
    use nautilus_core::hex;
    use rstest::rstest;
    use serde::Deserialize;

    use super::*;
    use crate::common::DeepXKeyScheme;

    #[derive(Deserialize)]
    struct RpcResponse {
        result: String,
    }

    fn snapshot() -> RuntimeSnapshot {
        let metadata: RpcResponse = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json",
        )))
        .unwrap();
        let bytes = hex::decode(metadata.result.trim_start_matches("0x")).unwrap();

        RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            hex::decode_array("86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b")
                .unwrap(),
            366,
            1,
            &bytes,
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

    #[rstest]
    fn dynamic_signing_is_deterministic_for_a_fixed_snapshot_and_nonce() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let first = sign_dynamic_pallet_call(
            &permit,
            &key(),
            "System",
            "remark",
            vec![Value::from_bytes(b"deepx-offline-signing-check")],
            1_725_000_000_123,
        )
        .unwrap();
        let second = sign_dynamic_pallet_call(
            &permit,
            &key(),
            "System",
            "remark",
            vec![Value::from_bytes(b"deepx-offline-signing-check")],
            1_725_000_000_123,
        )
        .unwrap();

        assert_eq!(first, second);
        assert!(!first.bytes().is_empty());
        assert_eq!(first.signer().len(), 20);
    }

    #[rstest]
    fn unknown_dynamic_call_is_rejected_without_panic() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let result = sign_dynamic_pallet_call(
            &permit,
            &key(),
            "UnknownPallet",
            "unknown_call",
            Vec::new(),
            7,
        );

        assert!(matches!(result, Err(SigningError::Encode(_))));
    }
}
