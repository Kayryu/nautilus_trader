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

mod evm;
mod snapshot;

use deepx_rust_sdk::signing::{
    self as sdk_signing, DeepXOfflinePalletCall, DeepXPalletSigningError,
};
pub use deepx_rust_sdk::signing::{
    CheckNonceEra, DeepXBuyQuotaParams, DeepXDeleteSubaccountParams,
    DeepXInitializeSubaccountParams, DeepXNonceEra, DeepXPerpCancelParams, DeepXPerpCloseParams,
    DeepXPerpModifyParams, DeepXPerpOrderType, DeepXPerpPlaceParams,
    DeepXPerpProfitAndLossPointParams, DeepXPostOnlyParam, DeepXQuotaClaimSignature,
    DeepXQuotaClaimSigningError, DeepXRemoveDelegateAccountParams, DeepXRenameSubaccountParams,
    DeepXRuntimeConfig, DeepXSetDelegateAccountParams, DeepXSetGlobalLeverageParams,
    DeepXSetPerMarketLeverageParams, DeepXSetSpotMarginParams, DeepXSpotCancelParams,
    DeepXSpotOrderType, DeepXSpotPlaceParams, DeepXTimeInForce, DeepXUpdateDelegateModeParams,
    DeepXWritableDelegateMode, sign_quota_claim,
};
pub use evm::{
    DeepXEvmTransactionKind, DeepXOfflineEvmExtrinsic, DeepXOfflineEvmSignError,
    DeepXOfflineEvmTransaction, DeepXOfflineEvmTransactionParams, DeepXOfflineEvmWrapperError,
    DeepXOfflinePerpPnlEvmParams, sign_offline_evm_transact_extrinsic,
    sign_offline_evm_transaction, sign_offline_perp_pnl_evm_transact_extrinsic,
};
pub use snapshot::{
    ApprovedRuntimeIdentity, DeepXReadOnlyRuntimeSnapshot, DeepXRuntimeChangeDecision,
    DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError, DeepXRuntimePalletInterface,
    DeepXRuntimeSnapshotPermit, DeepXRuntimeSnapshotService, DeepXRuntimeSnapshotServiceError,
    DeepXRuntimeSnapshotUpdate, DeepXRuntimeVariantIdentity, RuntimeSnapshot, SnapshotError,
};
use subxt_core::{
    client::ClientState,
    config::{Hasher, substrate::BlakeTwo256},
    dynamic::Value,
};
use thiserror::Error;

use crate::common::DeepXPrivateKey;

/// Errors produced before an extrinsic can be signed offline.
#[derive(Debug, Error)]
pub enum SigningError {
    /// The private scalar was rejected by the pinned DeepX signer implementation.
    #[error("invalid DeepX secp256k1 signing key")]
    InvalidKey,
    /// The runtime snapshot permit was revoked before signing.
    #[error(transparent)]
    RuntimePermit(#[from] DeepXRuntimeSnapshotServiceError),
    /// The pallet call is absent from the approved runtime interface.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The dynamic call or transaction extensions could not be SCALE encoded.
    #[error("unable to encode DeepX pallet extrinsic: {0}")]
    Encode(#[source] Box<subxt_core::Error>),
    /// The cancel and place legs of a perpetual modify target different runtime scopes.
    #[error("DeepX perpetual modify legs must target the same subaccount and market")]
    ModifyScopeMismatch,
    /// The runtime accepts at most 32 subaccount name bytes.
    #[error("DeepX subaccount name exceeds the runtime limit of 32 bytes: {length}")]
    SubaccountNameTooLong { length: usize },
}

impl From<subxt_core::Error> for SigningError {
    fn from(value: subxt_core::Error) -> Self {
        Self::Encode(Box::new(value))
    }
}

impl From<DeepXPalletSigningError> for SigningError {
    fn from(value: DeepXPalletSigningError) -> Self {
        match value {
            DeepXPalletSigningError::InvalidKey => Self::InvalidKey,
            DeepXPalletSigningError::RuntimeInterface(e) => Self::RuntimeInterface(e),
            DeepXPalletSigningError::ModifyScopeMismatch => Self::ModifyScopeMismatch,
            DeepXPalletSigningError::SubaccountNameTooLong { length } => {
                Self::SubaccountNameTooLong { length }
            }
            DeepXPalletSigningError::Encode(e) => Self::Encode(e),
        }
    }
}

/// Derives the Ethereum-compatible DeepX account identity for a signing key.
///
/// # Errors
///
/// Returns an error if the pinned signer implementation rejects the private scalar.
pub fn derive_signer_account_id(key: &DeepXPrivateKey) -> Result<[u8; 20], SigningError> {
    Ok(sdk_signing::derive_signer_account_id(key)?)
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
/// Returns an error when the permit was revoked, the key is invalid, or the call cannot be encoded
/// against the approved runtime snapshot.
pub fn sign_dynamic_pallet_call(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    pallet: &str,
    call: &str,
    arguments: Vec<Value>,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    snapshot_permit.ensure_active()?;
    sign_dynamic_pallet_call_with_snapshot(
        snapshot_permit.snapshot(),
        key,
        pallet,
        call,
        arguments,
        nonce,
    )
}

/// Signs a perpetual order placement without submitting it.
///
/// All financial values are exact runtime integers. For the timestamp-nonce call in the captured
/// runtime, the caller-provided nonce also becomes the runtime order ID. This offline function
/// performs no nonce reservation, persistence, authorization, submission, or event verification
/// and does not make perpetual trading operational.
///
/// # Errors
///
/// Returns an error when the key is invalid or the call cannot be encoded against the approved
/// runtime snapshot.
pub fn sign_perp_place_order(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXPerpPlaceParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::PerpPlaceOrder(params),
        nonce,
    )
}

/// Signs a Spot order placement without submitting it.
///
/// Amounts are exact little-endian runtime `U256` values and the pair is the deployment's bytes32
/// identity. For the timestamp-nonce call in the captured runtime, the caller-provided nonce also
/// becomes the runtime order ID. This function performs no unit conversion, nonce reservation,
/// persistence, authorization, submission, or event verification.
///
/// # Errors
///
/// Returns an error when the key is invalid or the call cannot be encoded against the approved
/// runtime snapshot.
pub fn sign_spot_place_order(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSpotPlaceParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::SpotPlaceOrder(params),
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
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::PerpCancel(params),
        nonce,
    )
}

/// Signs a perpetual order modification as one transactional cancel-and-place call.
///
/// The cancel and replacement must target the same subaccount and market. The caller supplies an
/// explicit nonce, which the runtime uses as the replacement order ID. This function performs no
/// nonce allocation, persistence, authorization, submission, or event recovery.
///
/// # Errors
///
/// Returns an error when the operation scopes differ, the key is invalid, or the approved
/// runtime snapshot cannot encode `Subaccount.modify_orders`.
pub fn sign_perp_modify_order(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXPerpModifyParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::PerpModifyOrder(params),
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
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::SpotCancel(params),
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
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::PerpClose(params),
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
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::PerpSetProfitAndLossPoint(params),
        nonce,
    )
}

/// Signs a global perpetual leverage setting without submitting it.
///
/// The runtime checks ownership and leverage bounds. This offline API does not establish either
/// condition, reserve a nonce, persist a transaction, or activate a framework command.
///
/// # Errors
///
/// Returns an error if the key or permitted runtime cannot encode the call.
pub fn sign_set_global_leverage(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSetGlobalLeverageParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::SetGlobalLeverage(params),
        nonce,
    )
}

/// Signs a per-market perpetual leverage setting without submitting it.
///
/// The runtime checks ownership, market eligibility, and leverage bounds. This offline API does
/// not establish those conditions, reserve a nonce, persist a transaction, or activate a command.
///
/// # Errors
///
/// Returns an error if the key or permitted runtime cannot encode the call.
pub fn sign_set_per_market_leverage(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSetPerMarketLeverageParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::SetPerMarketLeverage(params),
        nonce,
    )
}

/// Signs subaccount initialization without submitting it.
///
/// This offline API does not infer the resulting address, owner eligibility, name uniqueness, or
/// business success. It performs no network access, nonce allocation, persistence, or submission.
///
/// # Errors
///
/// Returns an error if the name exceeds the runtime bound or the key or permitted snapshot cannot
/// encode the call.
pub fn sign_initialize_subaccount(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXInitializeSubaccountParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::InitializeSubaccount(params),
        nonce,
    )
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
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::DeleteSubaccount(params),
        nonce,
    )
}

/// Signs subaccount renaming without submitting it.
///
/// This offline API does not establish ownership, name uniqueness, or business success. It
/// performs no network access, nonce allocation, persistence, or submission.
///
/// # Errors
///
/// Returns an error if the name exceeds the runtime bound or the key or permitted snapshot cannot
/// encode the call.
pub fn sign_rename_subaccount(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXRenameSubaccountParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::RenameSubaccount(params),
        nonce,
    )
}

/// Signs a subaccount Spot margin change without submitting it.
///
/// The runtime requires ownership and no open Spot orders. This offline API does not establish
/// either condition, reserve a nonce, persist a transaction, or activate a framework command.
///
/// # Errors
///
/// Returns an error if the key or permitted runtime cannot encode the call.
pub fn sign_set_spot_margin(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSetSpotMarginParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::SetSpotMargin(params),
        nonce,
    )
}

/// Signs wallet delegate creation or renewal without submitting it.
///
/// This offline API performs no authorization, clock, ownership, or quota checks and does not
/// reserve a nonce, persist a transaction, or enable a management command.
///
/// # Errors
///
/// Returns an error if the key or permitted snapshot cannot encode the call.
pub fn sign_set_delegate_account(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSetDelegateAccountParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::SetDelegateAccount(params),
        nonce,
    )
}

/// Signs a wallet delegate mode update without submitting it.
///
/// This offline API does not check account existence, owner authorization, expiry, or business
/// success. It reserves no nonce and performs no network access, persistence, or submission.
///
/// # Errors
///
/// Returns an error if the key or permitted snapshot cannot encode the call.
pub fn sign_update_delegate_mode(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXUpdateDelegateModeParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::UpdateDelegateMode(params),
        nonce,
    )
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
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::RemoveDelegateAccount(params),
        nonce,
    )
}

/// Signs a Lending quota purchase call without submitting it.
///
/// This offline API does not establish payer authorization, quota pricing, fund availability,
/// nonce reservation, transaction persistence, or purchase finality.
///
/// # Errors
///
/// Returns an error if the key or permitted runtime cannot encode the call.
pub fn sign_buy_quota(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXBuyQuotaParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_typed_pallet_call(
        snapshot_permit,
        key,
        DeepXOfflinePalletCall::BuyQuota(params),
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
    sign_typed_pallet_call(snapshot_permit, key, DeepXOfflinePalletCall::NoOp, nonce)
}

fn sign_typed_pallet_call(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    operation: DeepXOfflinePalletCall,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    snapshot_permit.ensure_active()?;
    let snapshot = snapshot_permit.snapshot();
    let signed = sdk_signing::sign_pallet_call(
        snapshot.interfaces(),
        snapshot.client_state(),
        key,
        operation,
        nonce,
    )?;
    Ok(SignedPalletExtrinsic {
        extrinsic_hash: signed.extrinsic_hash(),
        signer: signed.signer(),
        nonce: signed.nonce(),
        runtime: snapshot.identity().clone(),
        bytes: signed.into_bytes(),
    })
}

pub(crate) fn sign_dynamic_pallet_call_with_snapshot(
    snapshot: &RuntimeSnapshot,
    key: &DeepXPrivateKey,
    pallet: &str,
    call: &str,
    arguments: Vec<Value>,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call_with_runtime(
        RuntimeSigningContext {
            identity: snapshot.identity(),
            interfaces: snapshot.interfaces(),
            client_state: snapshot.client_state(),
        },
        key,
        pallet,
        call,
        arguments,
        nonce,
    )
}

#[derive(Clone, Copy)]
struct RuntimeSigningContext<'a> {
    identity: &'a ApprovedRuntimeIdentity,
    interfaces: &'a DeepXRuntimeInterfaceCatalog,
    client_state: &'a ClientState<DeepXRuntimeConfig>,
}

fn sign_dynamic_pallet_call_with_runtime(
    runtime: RuntimeSigningContext<'_>,
    key: &DeepXPrivateKey,
    pallet: &str,
    call: &str,
    arguments: Vec<Value>,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    let signed = sdk_signing::sign_dynamic_pallet_call(
        runtime.interfaces,
        runtime.client_state,
        key,
        pallet,
        call,
        arguments,
        nonce,
    )?;

    Ok(SignedPalletExtrinsic {
        extrinsic_hash: signed.extrinsic_hash(),
        signer: signed.signer(),
        nonce,
        runtime: runtime.identity.clone(),
        bytes: signed.into_bytes(),
    })
}

#[cfg(test)]
mod tests {
    use nautilus_core::hex;
    use rstest::rstest;
    use serde::Deserialize;
    use subxt_core::utils::H256;

    use super::*;
    use crate::common::{DeepXEnvironment, DeepXKeyScheme};

    const SPEC376_METADATA: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/deepx-rust-sdk/test_data/runtime/testnet/",
        "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
    ));

    #[derive(Deserialize)]
    struct RpcResponse {
        result: String,
    }

    fn runtime() -> (
        DeepXReadOnlyRuntimeSnapshot,
        ClientState<DeepXRuntimeConfig>,
    ) {
        let response: RpcResponse = serde_json::from_str(SPEC376_METADATA).unwrap();
        let metadata = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let snapshot = DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap(),
            376,
            1,
            &metadata,
        )
        .unwrap();
        let client_state = ClientState::<DeepXRuntimeConfig> {
            metadata: snapshot.metadata().clone(),
            genesis_hash: H256::from(snapshot.identity().genesis_hash),
            runtime_version: subxt_core::client::RuntimeVersion {
                spec_version: snapshot.identity().spec_version,
                transaction_version: snapshot.identity().transaction_version,
            },
        };
        (snapshot, client_state)
    }

    fn key() -> DeepXPrivateKey {
        DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap()
    }

    #[tokio::test]
    #[ignore = "requires live DeepX testnet Watch RPC"]
    async fn current_runtime_signs_calls_offline_with_synthetic_key() {
        let network = crate::config::DeepXNetworkConfig::default();
        let endpoints = crate::rpc::observe_and_validate_rpc_endpoint_identities(&network)
            .await
            .unwrap();
        let snapshot = crate::rpc::observe_current_runtime_snapshot_for_endpoints(
            &network.environment,
            &endpoints,
        )
        .await
        .unwrap();
        let service = DeepXRuntimeSnapshotService::new(snapshot.clone());
        let permit = service.acquire().unwrap();
        let key = key();
        let signed = sign_no_op(&permit, &key, 0).unwrap();

        assert_eq!(signed.runtime, *snapshot.identity());
        assert_eq!(signed.signer, derive_signer_account_id(&key).unwrap());
        assert_eq!(signed.extrinsic_hash, BlakeTwo256.hash(&signed.bytes).0);
        assert!(!signed.bytes.is_empty());

        let params = DeepXPerpPlaceParams {
            subaccount: [0x11; 20],
            market_id: 3,
            is_long: true,
            size: 1_000_000,
            price: 2_500_000_000,
            order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXPostOnlyParam::None,
        };
        let direct = sign_dynamic_pallet_call(
            &permit,
            &key,
            "PerpMarket",
            "place_order",
            vec![params.into_dynamic_value()],
            1,
        )
        .unwrap();
        let placement = sign_perp_place_order(&permit, &key, params, 1).unwrap();
        assert_eq!(placement.bytes, direct.bytes);
        assert_eq!(placement.runtime, *snapshot.identity());
        assert_eq!(placement.signer, signed.signer);
        assert_eq!(
            placement.extrinsic_hash,
            BlakeTwo256.hash(&placement.bytes).0
        );
        assert_ne!(placement.extrinsic_hash, signed.extrinsic_hash);

        let spot_params = DeepXSpotPlaceParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            is_buy: true,
            quote_amount: [0x33; 32],
            base_amount: [0x44; 32],
            order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc),
            post_only: DeepXPostOnlyParam::None,
            reduce_only: false,
        };
        let spot = sign_spot_place_order(&permit, &key, spot_params, 2).unwrap();
        assert_eq!(spot.runtime, *snapshot.identity());
        assert_eq!(spot.signer, signed.signer);
        assert_eq!(spot.extrinsic_hash, BlakeTwo256.hash(&spot.bytes).0);
        assert_ne!(spot.extrinsic_hash, placement.extrinsic_hash);

        let cancel = DeepXPerpCancelParams {
            subaccount: params.subaccount,
            order_id: 42,
            market_id: params.market_id,
            fast_cancel: false,
        };
        let perp_cancel = sign_perp_cancel(&permit, &key, cancel, 3).unwrap();
        assert_eq!(perp_cancel.runtime, *snapshot.identity());
        assert_eq!(
            perp_cancel.extrinsic_hash,
            BlakeTwo256.hash(&perp_cancel.bytes).0
        );

        let modify = sign_perp_modify_order(
            &permit,
            &key,
            DeepXPerpModifyParams {
                cancel,
                place: params,
            },
            4,
        )
        .unwrap();
        assert_eq!(modify.runtime, *snapshot.identity());
        assert_eq!(modify.extrinsic_hash, BlakeTwo256.hash(&modify.bytes).0);

        let spot_cancel = sign_spot_cancel(
            &permit,
            &key,
            DeepXSpotCancelParams {
                subaccount: spot_params.subaccount,
                pair: spot_params.pair,
                order_id: 42,
                is_buy: spot_params.is_buy,
                fast_cancel: false,
            },
            5,
        )
        .unwrap();
        assert_eq!(spot_cancel.runtime, *snapshot.identity());
        assert_eq!(
            spot_cancel.extrinsic_hash,
            BlakeTwo256.hash(&spot_cancel.bytes).0
        );
    }

    fn sign(
        snapshot: &DeepXReadOnlyRuntimeSnapshot,
        client_state: &ClientState<DeepXRuntimeConfig>,
        pallet: &str,
        call: &str,
        arguments: Vec<Value>,
        nonce: u64,
    ) -> SignedPalletExtrinsic {
        sign_dynamic_pallet_call_with_runtime(
            RuntimeSigningContext {
                identity: snapshot.identity(),
                interfaces: snapshot.interfaces(),
                client_state,
            },
            &key(),
            pallet,
            call,
            arguments,
            nonce,
        )
        .unwrap()
    }

    #[rstest]
    fn signed_extrinsic_retains_approved_runtime_identity() {
        let (snapshot, client_state) = runtime();
        let signed = sign(
            &snapshot,
            &client_state,
            "Subaccount",
            "no_op",
            Vec::new(),
            1_725_000_000_124,
        );
        assert!(signed.has_valid_hash());
        assert_eq!(signed.runtime(), snapshot.identity());
    }
}
