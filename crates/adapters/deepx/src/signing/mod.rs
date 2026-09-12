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

/// Exact raw runtime arguments for a DeepX perpetual position close.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpCloseParams {
    /// DeepX subaccount identity.
    pub subaccount: [u8; 20],
    /// Runtime perpetual market identifier.
    pub market_id: u16,
    /// Exact runtime price integer, without inferred financial scaling.
    pub price: u128,
    /// Optional exact runtime slippage integer, without inferred units.
    pub slippage: Option<u64>,
}

impl DeepXPerpCloseParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.subaccount),
            Value::u128(u128::from(self.market_id)),
            Value::u128(self.price),
            match self.slippage {
                Some(value) => Value::unnamed_variant("Some", [Value::u128(u128::from(value))]),
                None => Value::unnamed_variant("None", Vec::<Value>::new()),
            },
        ]
    }
}

/// Exact raw runtime arguments for perpetual profit and loss point management.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpProfitAndLossPointParams {
    /// DeepX subaccount identity.
    pub subaccount: [u8; 20],
    /// Runtime perpetual market identifier.
    pub market_id: u16,
    /// Exact runtime integer, without inferred scaling or trigger semantics.
    pub take_profit_point: u128,
    /// Exact runtime integer, without inferred scaling or trigger semantics.
    pub stop_loss_point: u128,
}

impl DeepXPerpProfitAndLossPointParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.subaccount),
            Value::u128(u128::from(self.market_id)),
            Value::u128(self.take_profit_point),
            Value::u128(self.stop_loss_point),
        ]
    }
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

/// Exact arguments for a user-requested DeepX Spot order cancellation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSpotCancelParams {
    /// DeepX subaccount which owns the order.
    pub subaccount: [u8; 20],
    /// Deployment-provided bytes32 Spot pair identifier.
    pub pair: [u8; 32],
    /// Runtime-assigned order identifier.
    pub order_id: u64,
    /// Whether the canceled order is a buy.
    pub is_buy: bool,
    /// Whether the runtime should use its high-priority cancellation path.
    pub fast_cancel: bool,
}

impl DeepXSpotCancelParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::named_composite([(
            "params",
            Value::named_composite([
                ("subaccount", Value::from_bytes(self.subaccount)),
                ("pair", Value::from_bytes(self.pair)),
                ("order_id", Value::u128(u128::from(self.order_id))),
                ("is_buy", Value::bool(self.is_buy)),
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

/// Signs a Spot order cancellation without submitting it.
///
/// The pair is the deployment's bytes32 identity, not a symbol or perpetual market ID.
/// The cancellation reason is fixed to `UserCanceled`. The caller owns nonce reservation
/// and persistence; this offline function does not authorize operational trading.
///
/// # Errors
///
/// Returns an error when the key is invalid or the call cannot be encoded against the approved
/// runtime snapshot.
pub fn sign_spot_cancel(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSpotCancelParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "SpotMarket",
        "cancel_order",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Signs a metadata-driven perpetual position close without submitting it.
///
/// Price and slippage are raw runtime integers. This function establishes no financial unit
/// semantics, authorization, SDK parity, nonce reservation, or operational close capability.
///
/// # Errors
///
/// Returns an error if the key or the permitted snapshot cannot encode the close call.
pub fn sign_perp_close(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXPerpCloseParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "close_position",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Signs perpetual profit and loss point management without submitting it.
///
/// Both points are exact runtime integers. This offline API performs no unit conversion,
/// nonce reservation, persistence, or network access, and proves no authorization,
/// trigger semantics, independent SDK parity, or operational management capability.
///
/// # Errors
///
/// Returns an error if the key or permitted snapshot cannot encode the management call.
pub fn sign_perp_set_profit_and_loss_point(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXPerpProfitAndLossPointParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "set_profit_and_loss_point",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Exact raw arguments for subaccount deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXDeleteSubaccountParams {
    /// Exact runtime subaccount identity, without inferred authorization.
    pub subaccount: [u8; 20],
}

impl DeepXDeleteSubaccountParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::from_bytes(self.subaccount)]
    }
}

/// Signs subaccount deletion without submitting it.
///
/// This offline API infers no ownership, eligibility, or deletion semantics and performs no
/// network access, nonce allocation, persistence, replay, or live activation.
///
/// # Errors
///
/// Returns an error if the key or permitted snapshot cannot encode the call.
pub fn sign_delete_subaccount(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXDeleteSubaccountParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "delete_subaccount",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Exact raw arguments for wallet delegate removal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXRemoveDelegateAccountParams {
    /// Exact runtime delegate identity, without inferred authorization.
    pub delegate: [u8; 20],
}

impl DeepXRemoveDelegateAccountParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::from_bytes(self.delegate)]
    }
}

/// Signs wallet delegate removal without submitting it.
///
/// This offline API infers no authorization or revocation semantics and performs no network
/// access, nonce reservation, persistence, or live activation.
///
/// # Errors
///
/// Returns an error if the key or permitted snapshot cannot encode the call.
pub fn sign_remove_delegate_account(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXRemoveDelegateAccountParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "remove_delegate_account",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Signs a metadata-driven Subaccount no-op without submitting it.
///
/// The call has no arguments. The caller supplies the exact signed-extension nonce and owns
/// reservation and persistence. This offline API proves no pool replacement, nonce-domain,
/// authorization, inclusion, or business semantics and grants no recovery or replay authority.
///
/// # Errors
///
/// Returns an error if the key or permitted runtime cannot encode the no-op call.
pub fn sign_no_op(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "no_op",
        Vec::new(),
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
    fn delete_subaccount_metadata_contract() {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet.call_variant_by_name("delete_subaccount").unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 1);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| (field.name.as_deref(), field.type_name.as_deref()))
                .collect::<Vec<_>>(),
            [(Some("subaccount"), Some("H160"))]
        );
    }

    #[rstest]
    #[case([0; 20], 0)]
    #[case([0xff; 20], u64::MAX)]
    #[case([0x11; 20], 63)]
    #[case([0x22; 20], 64)]
    fn delete_subaccount_exact_scale(#[case] subaccount: [u8; 20], #[case] nonce: u64) {
        let snapshot = snapshot();
        let params = DeepXDeleteSubaccountParams { subaccount };
        let payload = subxt_core::dynamic::tx(
            "Subaccount",
            "delete_subaccount",
            params.into_dynamic_arguments(),
        );
        let mut expected = vec![19, 1];
        expected.extend_from_slice(&subaccount);
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
            expected
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_delete_subaccount(&permit, &key(), params, nonce).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(
            signed,
            sign_delete_subaccount(&permit, &key(), params, nonce).unwrap()
        );
        let mut changed = params;
        changed.subaccount[19] ^= 1;
        let other_key = DeepXPrivateKey::new(
            "0x1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        for altered in [
            sign_delete_subaccount(&permit, &key(), changed, nonce).unwrap(),
            sign_delete_subaccount(&permit, &key(), params, nonce ^ 1).unwrap(),
            sign_delete_subaccount(&permit, &other_key, params, nonce).unwrap(),
        ] {
            assert_ne!(signed.bytes(), altered.bytes());
            assert_ne!(signed.extrinsic_hash(), altered.extrinsic_hash());
        }
    }

    #[rstest]
    #[case(vec![])]
    #[case(vec![Value::from_bytes([0; 19])])]
    #[case(vec![Value::from_bytes([0; 21])])]
    #[case(vec![Value::bool(true)])]
    #[case(vec![Value::from_bytes([0; 20]), Value::from_bytes([0; 20])])]
    fn delete_subaccount_malformed_arguments(#[case] arguments: Vec<Value>) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        assert!(matches!(
            sign_dynamic_pallet_call(
                &permit,
                &key(),
                "Subaccount",
                "delete_subaccount",
                arguments,
                0
            ),
            Err(SigningError::Encode(_))
        ));
    }

    #[rstest]
    fn delete_subaccount_runtime_change_blocks_new_permit() {
        let snapshot = snapshot();
        let mut changed = snapshot.identity().clone();
        changed.spec_version += 1;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        service.observe_runtime_identity(changed).unwrap();
        assert!(service.acquire().is_err());
        assert!(
            sign_delete_subaccount(
                &permit,
                &key(),
                DeepXDeleteSubaccountParams {
                    subaccount: [0x11; 20]
                },
                0,
            )
            .is_ok()
        );
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

        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_no_op(&permit, &key(), 1_725_000_000_124).unwrap();
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
    #[case([0; 20], 0)]
    #[case([0xff; 20], u64::MAX)]
    #[case([0x11; 20], 63)]
    #[case([0x22; 20], 64)]
    fn remove_delegate_exact_scale(#[case] delegate: [u8; 20], #[case] nonce: u64) {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet
            .call_variant_by_name("remove_delegate_account")
            .unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 29);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| (field.name.as_deref(), field.type_name.as_deref()))
                .collect::<Vec<_>>(),
            [(Some("delegate"), Some("H160"))]
        );
        let params = DeepXRemoveDelegateAccountParams { delegate };
        let payload = subxt_core::dynamic::tx(
            "Subaccount",
            "remove_delegate_account",
            params.into_dynamic_arguments(),
        );
        let mut expected = vec![19, 29];
        expected.extend_from_slice(&delegate);
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
            expected
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_remove_delegate_account(&permit, &key(), params, nonce).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(
            signed,
            sign_remove_delegate_account(&permit, &key(), params, nonce).unwrap()
        );
        let mut changed = params;
        changed.delegate[19] ^= 1;
        let other_key = DeepXPrivateKey::new(
            "0x1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        for altered in [
            sign_remove_delegate_account(&permit, &key(), changed, nonce).unwrap(),
            sign_remove_delegate_account(&permit, &key(), params, nonce ^ 1).unwrap(),
            sign_remove_delegate_account(&permit, &other_key, params, nonce).unwrap(),
        ] {
            assert_ne!(signed.bytes(), altered.bytes());
            assert_ne!(signed.extrinsic_hash(), altered.extrinsic_hash());
        }
    }

    #[rstest]
    #[case(Value::from_bytes([0; 19]))]
    #[case(Value::from_bytes([0; 21]))]
    #[case(Value::bool(true))]
    fn remove_delegate_malformed_arguments(#[case] value: Value) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        assert!(matches!(
            sign_dynamic_pallet_call(
                &permit,
                &key(),
                "Subaccount",
                "remove_delegate_account",
                vec![value],
                0,
            ),
            Err(SigningError::Encode(_))
        ));
    }

    #[rstest]
    fn remove_delegate_runtime_change_blocks_new_permit() {
        let snapshot = snapshot();
        let mut changed = snapshot.identity().clone();
        changed.transaction_version += 1;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        service.observe_runtime_identity(changed).unwrap();
        assert!(service.acquire().is_err());
        assert!(
            sign_remove_delegate_account(
                &permit,
                &key(),
                DeepXRemoveDelegateAccountParams {
                    delegate: [0x11; 20]
                },
                0,
            )
            .is_ok()
        );
    }

    #[rstest]
    fn perp_close_runtime_change_blocks_new_permit() {
        let snapshot = snapshot();
        let mut changed = snapshot.identity().clone();
        changed.spec_version += 1;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        service.observe_runtime_identity(changed).unwrap();
        assert!(service.acquire().is_err());
    }

    #[rstest]
    #[case(0, 0, 0)]
    #[case(u16::MAX, u128::MAX, u128::MAX)]
    #[case(7, 123, 456)]
    fn perp_points_exact_scale(
        #[case] market_id: u16,
        #[case] take_profit_point: u128,
        #[case] stop_loss_point: u128,
    ) {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("PerpMarket").unwrap();
        let call = pallet
            .call_variant_by_name("set_profit_and_loss_point")
            .unwrap();
        assert_eq!(pallet.index(), 22);
        assert_eq!(call.index, 13);
        assert!(pallet.call_variant_by_name("modify_order").is_none());
        assert_eq!(
            call.fields
                .iter()
                .map(|field| (field.name.as_deref(), field.type_name.as_deref()))
                .collect::<Vec<_>>(),
            [
                (Some("subaccount"), Some("H160")),
                (Some("market_id"), Some("u16")),
                (Some("take_profit_point"), Some("u128")),
                (Some("stop_loss_point"), Some("u128")),
            ]
        );
        let params = DeepXPerpProfitAndLossPointParams {
            subaccount: [0x11; 20],
            market_id,
            take_profit_point,
            stop_loss_point,
        };
        let mut expected = vec![22, 13];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&market_id.to_le_bytes());
        expected.extend_from_slice(&take_profit_point.to_le_bytes());
        expected.extend_from_slice(&stop_loss_point.to_le_bytes());
        let payload = subxt_core::dynamic::tx(
            "PerpMarket",
            "set_profit_and_loss_point",
            params.into_dynamic_arguments(),
        );
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
            expected
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed =
            sign_perp_set_profit_and_loss_point(&permit, &key(), params, u64::MAX).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), u64::MAX);
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(
            signed,
            sign_perp_set_profit_and_loss_point(&permit, &key(), params, u64::MAX).unwrap()
        );
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    fn perp_points_signed_identity_sensitivity(#[case] mutation: u8) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut params = DeepXPerpProfitAndLossPointParams {
            subaccount: [0x11; 20],
            market_id: 7,
            take_profit_point: 123,
            stop_loss_point: 456,
        };
        let original = sign_perp_set_profit_and_loss_point(&permit, &key(), params, 125).unwrap();
        let mut nonce = 125;
        match mutation {
            0 => params.subaccount[19] ^= 1,
            1 => params.market_id += 1,
            2 => params.take_profit_point += 1,
            3 => params.stop_loss_point += 1,
            4 => nonce += 1,
            _ => unreachable!(),
        }
        let changed = sign_perp_set_profit_and_loss_point(&permit, &key(), params, nonce).unwrap();
        assert_ne!(original.bytes(), changed.bytes());
        assert_ne!(original.extrinsic_hash(), changed.extrinsic_hash());
    }

    #[rstest]
    #[case(0, Value::from_bytes([0x11; 19]))]
    #[case(1, Value::u128(u128::from(u16::MAX) + 1))]
    #[case(2, Value::bool(true))]
    #[case(3, Value::unnamed_variant("None", Vec::<Value>::new()))]
    fn perp_points_malformed_arguments(#[case] index: usize, #[case] value: Value) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut arguments = DeepXPerpProfitAndLossPointParams {
            subaccount: [0x11; 20],
            market_id: 7,
            take_profit_point: 123,
            stop_loss_point: 456,
        }
        .into_dynamic_arguments();
        arguments[index] = value;
        assert!(matches!(
            sign_dynamic_pallet_call(
                &permit,
                &key(),
                "PerpMarket",
                "set_profit_and_loss_point",
                arguments,
                125
            ),
            Err(SigningError::Encode(_))
        ));
    }

    #[rstest]
    fn perp_points_runtime_change_blocks_new_permit() {
        let snapshot = snapshot();
        let mut changed = snapshot.identity().clone();
        changed.spec_version += 1;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        service.observe_runtime_identity(changed).unwrap();
        assert!(service.acquire().is_err());
        let params = DeepXPerpProfitAndLossPointParams {
            subaccount: [0x11; 20],
            market_id: 7,
            take_profit_point: 123,
            stop_loss_point: 456,
        };
        assert!(sign_perp_set_profit_and_loss_point(&permit, &key(), params, 125).is_ok());
    }

    #[rstest]
    #[case(0, 0, None)]
    #[case(u16::MAX, u128::MAX, Some(u64::MAX))]
    #[case(7, 123, Some(0))]
    fn perp_close_exact_scale(
        #[case] market_id: u16,
        #[case] price: u128,
        #[case] slippage: Option<u64>,
    ) {
        let snapshot = snapshot();
        let metadata = snapshot.metadata();
        let pallet = metadata.pallet_by_name("PerpMarket").unwrap();
        let call = pallet.call_variant_by_name("close_position").unwrap();
        assert_eq!(call.index, 14);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| (field.name.as_deref(), field.type_name.as_deref()))
                .collect::<Vec<_>>(),
            [
                (Some("subaccount"), Some("H160")),
                (Some("market_id"), Some("u16")),
                (Some("price"), Some("u128")),
                (Some("slippage"), Some("Option<u64>"))
            ]
        );
        let params = DeepXPerpCloseParams {
            subaccount: [0x11; 20],
            market_id,
            price,
            slippage,
        };
        let mut expected = vec![pallet.index(), call.index];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&market_id.to_le_bytes());
        expected.extend_from_slice(&price.to_le_bytes());
        expected.push(u8::from(slippage.is_some()));
        if let Some(value) = slippage {
            expected.extend_from_slice(&value.to_le_bytes());
        }
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_perp_close(&permit, &key(), params, u64::MAX).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(signed.nonce(), u64::MAX);
        assert_eq!(
            signed,
            sign_perp_close(&permit, &key(), params, u64::MAX).unwrap()
        );
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    fn perp_close_signed_identity_sensitivity(#[case] mutation: u8) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut params = DeepXPerpCloseParams {
            subaccount: [0x11; 20],
            market_id: 7,
            price: 123,
            slippage: None,
        };
        let original = sign_perp_close(&permit, &key(), params, 125).unwrap();
        let mut nonce = 125;
        match mutation {
            0 => params.subaccount[19] ^= 1,
            1 => params.market_id += 1,
            2 => params.price += 1,
            3 => params.slippage = Some(0),
            4 => nonce += 1,
            _ => unreachable!(),
        }
        let changed = sign_perp_close(&permit, &key(), params, nonce).unwrap();
        assert_ne!(original.bytes(), changed.bytes());
        assert_ne!(original.extrinsic_hash(), changed.extrinsic_hash());
    }

    #[rstest]
    #[case(0, Value::from_bytes([0x11; 19]))]
    #[case(1, Value::u128(u128::from(u16::MAX) + 1))]
    #[case(2, Value::bool(true))]
    #[case(3, Value::unnamed_variant("Some", [Value::u128(u128::from(u64::MAX) + 1)]))]
    #[case(3, Value::unnamed_variant("Unknown", Vec::<Value>::new()))]
    fn perp_close_malformed_arguments(#[case] index: usize, #[case] value: Value) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut arguments = DeepXPerpCloseParams {
            subaccount: [0x11; 20],
            market_id: 7,
            price: 123,
            slippage: None,
        }
        .into_dynamic_arguments();
        arguments[index] = value;
        assert!(matches!(
            sign_dynamic_pallet_call(
                &permit,
                &key(),
                "PerpMarket",
                "close_position",
                arguments,
                125
            ),
            Err(SigningError::Encode(_))
        ));
    }

    #[rstest]
    #[case(0)]
    #[case(63)]
    #[case(64)]
    #[case(u64::MAX)]
    fn no_op_exact_metadata_contract(#[case] nonce: u64) {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet.call_variant_by_name("no_op").unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 28);
        assert!(call.fields.is_empty());
        let payload = subxt_core::dynamic::tx("Subaccount", "no_op", Vec::<Value>::new());
        let encoded =
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap();
        assert_eq!(encoded, [19, 28]);
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_no_op(&permit, &key(), nonce).unwrap();
        assert!(signed.bytes().ends_with(&encoded));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(signed, sign_no_op(&permit, &key(), nonce).unwrap());
        let changed = sign_no_op(&permit, &key(), nonce ^ 1).unwrap();
        assert_ne!(signed.bytes(), changed.bytes());
        assert_ne!(signed.extrinsic_hash(), changed.extrinsic_hash());
    }

    #[rstest]
    fn no_op_runtime_change_blocks_new_signing_permit() {
        let snapshot = snapshot();
        let mut changed = snapshot.identity().clone();
        changed.transaction_version += 1;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        service.observe_runtime_identity(changed).unwrap();
        assert!(service.acquire().is_err());
        assert!(sign_no_op(&permit, &key(), 1).is_ok());
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

    #[rstest]
    #[case::subaccount(0)]
    #[case::pair(1)]
    #[case::order(2)]
    #[case::side(3)]
    #[case::fast(4)]
    #[case::nonce(5)]
    fn spot_cancel_changes_signed_identity(#[case] mutation: u8) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut params = DeepXSpotCancelParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            order_id: 7,
            is_buy: false,
            fast_cancel: false,
        };
        let original = sign_spot_cancel(&permit, &key(), params, 125).unwrap();
        let mut nonce = 125;
        match mutation {
            0 => params.subaccount[19] ^= 1,
            1 => params.pair[31] ^= 1,
            2 => params.order_id += 1,
            3 => params.is_buy = true,
            4 => params.fast_cancel = true,
            5 => nonce += 1,
            _ => unreachable!(),
        }
        let changed = sign_spot_cancel(&permit, &key(), params, nonce).unwrap();
        assert_ne!(original.bytes(), changed.bytes());
        assert_ne!(original.extrinsic_hash(), changed.extrinsic_hash());
        assert_eq!(original.runtime(), changed.runtime());
        assert_eq!(original.signer(), changed.signer());
    }

    #[rstest]
    #[case::short_subaccount("subaccount", Value::from_bytes([0x11; 19]))]
    #[case::short_pair("pair", Value::from_bytes([0x22; 31]))]
    #[case::long_pair("pair", Value::from_bytes([0x22; 33]))]
    #[case::order_overflow("order_id", Value::u128(u128::from(u64::MAX) + 1))]
    #[case::wrong_side_type("is_buy", Value::u128(1))]
    #[case::unknown_reason("cancel_reason", Value::unnamed_variant("UnrecognizedCancelReason", Vec::<Value>::new()))]
    fn spot_cancel_rejects_malformed_dynamic_binding(#[case] field: &str, #[case] value: Value) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut fields = vec![
            ("subaccount", Value::from_bytes([0x11; 20])),
            ("pair", Value::from_bytes([0x22; 32])),
            ("order_id", Value::u128(7)),
            ("is_buy", Value::bool(false)),
            (
                "cancel_reason",
                Value::unnamed_variant("UserCanceled", Vec::<Value>::new()),
            ),
            ("fast_cancel", Value::bool(false)),
        ];
        fields
            .iter_mut()
            .find(|(name, _)| *name == field)
            .unwrap()
            .1 = value;
        let result = sign_dynamic_pallet_call(
            &permit,
            &key(),
            "SpotMarket",
            "cancel_order",
            vec![Value::named_composite([(
                "params",
                Value::named_composite(fields),
            )])],
            125,
        );
        assert!(matches!(result, Err(SigningError::Encode(_))));
    }

    #[rstest]
    #[case(0, false, false)]
    #[case(u64::MAX, true, false)]
    #[case(1_725_000_000_001, false, true)]
    #[case(1, true, true)]
    fn spot_cancel_binds_spec366_schema(
        #[case] order_id: u64,
        #[case] is_buy: bool,
        #[case] fast_cancel: bool,
    ) {
        let snapshot = snapshot();
        let metadata = snapshot.metadata();
        let pallet = metadata.pallet_by_name("SpotMarket").unwrap();
        let call = pallet.call_variant_by_name("cancel_order").unwrap();
        assert_eq!(call.fields.len(), 1);
        assert_eq!(call.fields[0].name.as_deref(), Some("params"));
        let params_type = metadata.types().resolve(call.fields[0].ty.id).unwrap();
        let scale_info::TypeDef::Composite(composite) = &params_type.type_def else {
            panic!("Spot cancel params must be a composite");
        };
        assert_eq!(
            composite
                .fields
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            [
                "subaccount",
                "pair",
                "order_id",
                "is_buy",
                "cancel_reason",
                "fast_cancel"
            ],
        );
        let params = DeepXSpotCancelParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            order_id,
            is_buy,
            fast_cancel,
        };
        let mut expected = vec![pallet.index(), call.index];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&params.pair);
        expected.extend_from_slice(&order_id.to_le_bytes());
        expected.extend_from_slice(&[u8::from(is_buy), 0, u8::from(fast_cancel)]);
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_spot_cancel(&permit, &key(), params, 1_725_000_000_125).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(signed.nonce(), 1_725_000_000_125);
        assert_eq!(signed.runtime(), permit.snapshot().identity());
    }
}
