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
    /// The pallet call is absent from the approved runtime interface.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The dynamic call or transaction extensions could not be SCALE encoded.
    #[error("unable to encode DeepX pallet extrinsic: {0}")]
    Encode(#[source] Box<subxt_core::Error>),
}

impl From<subxt_core::Error> for SigningError {
    fn from(value: subxt_core::Error) -> Self {
        Self::Encode(Box::new(value))
    }
}

/// Derives the Ethereum-compatible DeepX account identity for a signing key.
///
/// # Errors
///
/// Returns an error if the pinned signer implementation rejects the private scalar.
pub fn derive_signer_account_id(key: &DeepXPrivateKey) -> Result<[u8; 20], SigningError> {
    let signer = Keypair::from_secret_key(*key.as_bytes()).map_err(|_| SigningError::InvalidKey)?;
    Ok(signer.public_key().to_account_id().0)
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

/// Exact arguments for a user-requested DeepX perpetual order cancellation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpCancelParams {
    /// DeepX subaccount which owns the order.
    pub subaccount: [u8; 20],
    /// Runtime-assigned order identifier.
    pub order_id: u64,
    /// Perpetual market identifier.
    pub market_id: u16,
    /// Whether the runtime should use its high-priority cancellation path.
    pub fast_cancel: bool,
}

impl DeepXPerpCancelParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::named_composite([(
            "params",
            Value::named_composite([
                ("subaccount", Value::from_bytes(self.subaccount)),
                ("order_id", Value::u128(u128::from(self.order_id))),
                ("market_id", Value::u128(u128::from(self.market_id))),
                (
                    "cancel_reason",
                    Value::unnamed_variant("UserCanceled", Vec::<Value>::new()),
                ),
                ("fast_cancel", Value::bool(self.fast_cancel)),
            ]),
        )])]
    }
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

/// Signs a perpetual order cancellation without submitting it.
///
/// The cancellation reason is fixed to the runtime's `UserCanceled` variant. As with
/// [`sign_dynamic_pallet_call`], the caller owns nonce reservation and persistence.
///
/// # Errors
///
/// Returns an error when the key is invalid or the call cannot be encoded against the approved
/// runtime snapshot.
pub fn sign_perp_cancel(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXPerpCancelParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "cancel_order",
        params.into_dynamic_arguments(),
        nonce,
    )
}

pub(crate) fn sign_dynamic_pallet_call_with_snapshot(
    snapshot: &RuntimeSnapshot,
    key: &DeepXPrivateKey,
    pallet: &str,
    call: &str,
    arguments: Vec<Value>,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    snapshot.interfaces().call(pallet, call)?;

    let signer = Keypair::from_secret_key(*key.as_bytes()).map_err(|_| SigningError::InvalidKey)?;
    let account_id = derive_signer_account_id(key)?;
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
        signer: account_id,
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
    fn dynamic_signing_matches_fixed_testnet_regression_vector() {
        let snapshot = snapshot();
        let payload = subxt_core::dynamic::tx(
            "System",
            "remark",
            vec![Value::from_bytes(b"deepx-offline-signing-check")],
        );
        let params = DefaultExtrinsicParamsBuilder::<DeepXRuntimeConfig>::new()
            .nonce(1_725_000_000_123)
            .build();
        let signer_payload = tx::create_v4_signed(&payload, snapshot.client_state(), params)
            .unwrap()
            .signer_payload();
        assert_eq!(
            hex::encode(signer_payload),
            "00006c64656570782d6f66666c696e652d7369676e696e672d636865636b000b7b2203a29101006e0100000100000086604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b",
        );

        let service = DeepXRuntimeSnapshotService::new(snapshot);
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
        assert_eq!(
            hex::encode(first.signer()),
            "fcad0b19bb29d4674531d6f115237e16afce377c",
        );
        assert_eq!(first.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(
            hex::encode(first.bytes()),
            "f50184fcad0b19bb29d4674531d6f115237e16afce377ca524ebcf1d41cd1079a3bee1eb1b25d2ac0473e5546faa6e8fb828389f795ee933fece23b7435b4b58435d775b7d97e7430485a1ff25eca05b3ab6626f43cb9c01000b7b2203a291010000006c64656570782d6f66666c696e652d7369676e696e672d636865636b",
        );
        assert_eq!(
            hex::encode(first.extrinsic_hash()),
            "9695ee4aa7ac14ad58b7e5fd850bf6f0648df811a653c5486ba6146365f7de19",
        );
    }

    #[rstest]
    fn no_op_signing_matches_fixed_testnet_regression_vector() {
        let snapshot = snapshot();
        let payload = subxt_core::dynamic::tx("Subaccount", "no_op", Vec::<Value>::new());
        let params = DefaultExtrinsicParamsBuilder::<DeepXRuntimeConfig>::new()
            .nonce(1_725_000_000_124)
            .build();
        let signer_payload = tx::create_v4_signed(&payload, snapshot.client_state(), params)
            .unwrap()
            .signer_payload();
        assert_eq!(
            hex::encode(signer_payload),
            "131c000b7c2203a29101006e0100000100000086604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b",
        );

        let signed = sign_dynamic_pallet_call_with_snapshot(
            &snapshot,
            &key(),
            "Subaccount",
            "no_op",
            Vec::new(),
            1_725_000_000_124,
        )
        .unwrap();
        assert_eq!(
            hex::encode(signed.signer()),
            "fcad0b19bb29d4674531d6f115237e16afce377c",
        );
        assert_eq!(
            hex::encode(signed.bytes()),
            "850184fcad0b19bb29d4674531d6f115237e16afce377c96c788579cf2cdda606129a7fe59b763031ddc0d19056af3c1585674b0a64d4928e9e1c787b283a9467c2207afa5c6338dd1d671df0b3aec5edb5128e79981e301000b7c2203a2910100131c",
        );
        assert_eq!(
            hex::encode(signed.extrinsic_hash()),
            "c2e2837a583ddf1e94fd0a5a177c4b6e471ac24b82a7e9fd32d600c0f674d28a",
        );
    }

    #[rstest]
    fn approved_testnet_catalog_contains_direct_market_calls() {
        let snapshot = snapshot();
        let interfaces = snapshot.interfaces();

        for (pallet, call) in [
            ("Subaccount", "no_op"),
            ("SpotMarket", "place_order"),
            ("SpotMarket", "cancel_order"),
            ("PerpMarket", "place_order"),
            ("PerpMarket", "cancel_order"),
        ] {
            interfaces.call(pallet, call).unwrap();
        }
    }

    #[rstest]
    fn perp_cancel_arguments_encode_against_approved_testnet_metadata() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let signed = sign_perp_cancel(
            &permit,
            &key(),
            DeepXPerpCancelParams {
                subaccount: [0x11; 20],
                order_id: 1_725_000_000_001,
                market_id: 7,
                fast_cancel: false,
            },
            1_725_000_000_125,
        )
        .unwrap();

        assert!(signed.has_valid_hash());
        assert_eq!(
            hex::encode(signed.bytes()),
            "050284fcad0b19bb29d4674531d6f115237e16afce377c91034ea2dd5af54cb435089b4ddd3cca24035b700655e0a59997f0a36d25a4175a3fdad77ba793855e1961a5a6d9c9c0f9a4ce3b54ece8b92c56d2cf6ed469d300000b7d2203a291010016031111111111111111111111111111111111111111012203a29101000007000000",
        );
        assert_eq!(
            hex::encode(signed.extrinsic_hash()),
            "4bf41e0b660b46dbf14be59a261b2ded7ea510b712f27063ace34a6d6d6e94c8",
        );
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

        assert!(matches!(
            result,
            Err(SigningError::RuntimeInterface(
                DeepXRuntimeInterfaceError::PalletUnavailable(pallet),
            )) if pallet == "UnknownPallet"
        ));
    }
}
