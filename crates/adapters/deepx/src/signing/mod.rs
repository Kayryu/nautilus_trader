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

pub use deepx_rust_sdk::signing::{
    DeepXBuyQuotaParams, DeepXDeleteSubaccountParams, DeepXInitializeSubaccountParams,
    DeepXNonceEra, DeepXPerpCancelParams, DeepXPerpCloseParams, DeepXPerpModifyParams,
    DeepXPerpOrderType, DeepXPerpPlaceParams, DeepXPerpProfitAndLossPointParams,
    DeepXPostOnlyParam, DeepXQuotaClaimSignature, DeepXQuotaClaimSigningError,
    DeepXRemoveDelegateAccountParams, DeepXRenameSubaccountParams, DeepXSetDelegateAccountParams,
    DeepXSetGlobalLeverageParams, DeepXSetPerMarketLeverageParams, DeepXSetSpotMarginParams,
    DeepXSpotCancelParams, DeepXSpotOrderType, DeepXSpotPlaceParams, DeepXTimeInForce,
    DeepXUpdateDelegateModeParams, DeepXWritableDelegateMode, sign_quota_claim,
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
    Config,
    client::ClientState,
    config::{
        DefaultExtrinsicParamsBuilder, ExtrinsicParams, ExtrinsicParamsEncoder, Hasher,
        TransactionExtension, substrate::BlakeTwo256, transaction_extensions,
    },
    dynamic::Value,
    error::ExtrinsicParamsError,
    tx,
    utils::{AccountId20, H256, Static},
};
use subxt_signer::eth::{Keypair, Signature};
use thiserror::Error;

use crate::common::DeepXPrivateKey;

/// DeepX runtime types required for AccountId20 Ethereum-compatible signatures.
#[derive(Clone, Copy, Debug)]
pub enum DeepXRuntimeConfig {}

#[doc(hidden)]
#[derive(Debug)]
pub struct CheckNonceEra {
    genesis_hash: H256,
}

impl ExtrinsicParams<DeepXRuntimeConfig> for CheckNonceEra {
    type Params = ();

    fn new(client: &ClientState<DeepXRuntimeConfig>, (): ()) -> Result<Self, ExtrinsicParamsError> {
        Ok(Self {
            genesis_hash: client.genesis_hash,
        })
    }
}

impl ExtrinsicParamsEncoder for CheckNonceEra {
    fn encode_value_to(&self, bytes: &mut Vec<u8>) {
        // The captured CheckNonceEra extra encodes Immortal as variant index zero
        bytes.push(0);
    }

    fn encode_implicit_to(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.genesis_hash.0);
    }
}

impl TransactionExtension<DeepXRuntimeConfig> for CheckNonceEra {
    type Decoded = Static<DeepXNonceEra>;

    fn matches(identifier: &str, _: u32, _: &scale_info::PortableRegistry) -> bool {
        identifier == "CheckNonceEra"
    }
}

impl Config for DeepXRuntimeConfig {
    type AccountId = AccountId20;
    type Address = AccountId20;
    type Signature = Signature;
    type Hasher = BlakeTwo256;
    type Header = subxt_core::config::substrate::SubstrateHeader<u32, Self::Hasher>;
    type ExtrinsicParams = transaction_extensions::AnyOf<
        Self,
        (
            transaction_extensions::VerifySignature<Self>,
            transaction_extensions::CheckSpecVersion,
            transaction_extensions::CheckTxVersion,
            transaction_extensions::CheckNonce,
            transaction_extensions::CheckGenesis<Self>,
            transaction_extensions::CheckMortality<Self>,
            transaction_extensions::ChargeAssetTxPayment<Self>,
            transaction_extensions::ChargeTransactionPayment,
            transaction_extensions::CheckMetadataHash,
            CheckNonceEra,
        ),
    >;
    type AssetId = u32;
}

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
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "place_order",
        params.into_dynamic_arguments(),
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
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "SpotMarket",
        "place_order",
        params.into_dynamic_arguments(),
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
    if params.cancel.subaccount != params.place.subaccount
        || params.cancel.market_id != params.place.market_id
    {
        return Err(SigningError::ModifyScopeMismatch);
    }
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "modify_orders",
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
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "set_global_leverage",
        params.into_dynamic_arguments(),
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
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "set_per_market_leverage",
        params.into_dynamic_arguments(),
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
    if params.name.len() > 32 {
        return Err(SigningError::SubaccountNameTooLong {
            length: params.name.len(),
        });
    }
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "initialize_subaccount",
        params.into_dynamic_arguments(),
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
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "delete_subaccount",
        params.into_dynamic_arguments(),
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
    if params.new_name.len() > 32 {
        return Err(SigningError::SubaccountNameTooLong {
            length: params.new_name.len(),
        });
    }
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "rename_subaccount",
        params.into_dynamic_arguments(),
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
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "set_spot_margin",
        params.into_dynamic_arguments(),
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
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "set_delegate_account",
        params.into_dynamic_arguments(),
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
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "update_delegate_mode",
        params.into_dynamic_arguments(),
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
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "remove_delegate_account",
        params.into_dynamic_arguments(),
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
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Lending",
        "buy_quota",
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

fn immortal_params(nonce: u64) -> subxt_core::config::ParamsFor<DeepXRuntimeConfig> {
    let (
        signature,
        spec,
        tx_version,
        nonce_param,
        genesis,
        mortality,
        asset_payment,
        payment,
        metadata_hash,
    ) = DefaultExtrinsicParamsBuilder::<DeepXRuntimeConfig>::new()
        .nonce(nonce)
        .immortal()
        .build();
    (
        signature,
        spec,
        tx_version,
        nonce_param,
        genesis,
        mortality,
        asset_payment,
        payment,
        metadata_hash,
        (),
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
    runtime.interfaces.call(pallet, call)?;

    let signer = Keypair::from_secret_key(*key.as_bytes()).map_err(|_| SigningError::InvalidKey)?;
    let account_id = derive_signer_account_id(key)?;
    let payload = subxt_core::dynamic::tx(pallet, call, arguments);
    let transaction =
        tx::create_v4_signed(&payload, runtime.client_state, immortal_params(nonce))?.sign(&signer);
    let hash = transaction.hash_with(BlakeTwo256);

    Ok(SignedPalletExtrinsic {
        bytes: transaction.into_encoded(),
        extrinsic_hash: hash.0,
        signer: account_id,
        nonce,
        runtime: runtime.identity.clone(),
    })
}

#[cfg(test)]
mod tests {
    use nautilus_core::hex;
    use rstest::rstest;
    use serde::Deserialize;

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

    fn encode_call(
        snapshot: &DeepXReadOnlyRuntimeSnapshot,
        pallet: &str,
        call: &str,
        arguments: Vec<Value>,
    ) -> Vec<u8> {
        let payload = subxt_core::dynamic::tx(pallet, call, arguments);
        tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap()
    }

    #[rstest]
    fn spec376_immortal_extrinsics_match_pinned_sdk() {
        let (snapshot, client_state) = runtime();
        let cases = [
            (
                "Subaccount",
                "no_op",
                Vec::new(),
                1_725_000_000_124,
                "850184fcad0b19bb29d4674531d6f115237e16afce377ccd76f6b9583b6dfd829576bc6888f00fa985b12eb69488d9215ad76133b8792113e341e9f2dc4a0e073af90495f8926c9d4c9e817f29ee384e83fb3dd18d78f600000b7c2203a2910100131c",
            ),
            (
                "System",
                "remark",
                vec![Value::from_bytes(b"deepx-offline-signing-check")],
                1_725_000_000_123,
                "f50184fcad0b19bb29d4674531d6f115237e16afce377cdd2abfbe77653eff8459e3105b30feb8295341991ff6cd015b5a94ca0ae27bf30f2be47c10c8a8f926f0ba538bd72d5ddb9a6eb38549961dfb0fa8e96433bc6f01000b7b2203a291010000006c64656570782d6f66666c696e652d7369676e696e672d636865636b",
            ),
            (
                "PerpMarket",
                "place_order",
                DeepXPerpPlaceParams {
                    subaccount: [0x11; 20],
                    market_id: 7,
                    is_long: true,
                    size: u128::MAX,
                    price: u128::MAX,
                    order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                    take_profit: Some(u128::MAX),
                    stop_loss: None,
                    reduce_only: false,
                    post_only: DeepXPostOnlyParam::None,
                }
                .into_dynamic_arguments(),
                1_725_000_000_125,
                "b90284fcad0b19bb29d4674531d6f115237e16afce377c6476e0a03c23f5e2d94326d35a5d1db1a3b03e19e7270548a4781c5ffcd9c63f13f98e79b13089f1010035607eff80c9990aa7dcbbeb35fee0fb7bae24df0b2401000b7d2203a291010016021111111111111111111111111111111111111111070001ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff000001ffffffffffffffffffffffffffffffff000000",
            ),
        ];

        for (pallet, call, arguments, nonce, expected) in cases {
            let signed = sign(&snapshot, &client_state, pallet, call, arguments, nonce);
            assert_eq!(signed.bytes(), hex::decode(expected).unwrap());
            assert!(signed.has_valid_hash());
            assert_eq!(signed.runtime(), snapshot.identity());
        }
    }

    #[rstest]
    #[case("Subaccount", "initialize_subaccount", 19, 0, &["name"])]
    #[case("Subaccount", "delete_subaccount", 19, 1, &["subaccount"])]
    #[case("Subaccount", "set_delegate_account", 19, 2, &["delegate", "name", "valid_until"])]
    #[case("Subaccount", "update_delegate_mode", 19, 4, &["address", "new_mode"])]
    #[case("Subaccount", "set_spot_margin", 19, 6, &["address", "enable_spot_margin"])]
    #[case("Subaccount", "rename_subaccount", 19, 7, &["subaccount", "new_name"])]
    #[case("Subaccount", "no_op", 19, 28, &[])]
    #[case("Subaccount", "remove_delegate_account", 19, 29, &["delegate"])]
    #[case("SpotMarket", "place_order", 20, 1, &["params"])]
    #[case("SpotMarket", "cancel_order", 20, 3, &["params"])]
    #[case("PerpMarket", "place_order", 22, 2, &["params"])]
    #[case("PerpMarket", "cancel_order", 22, 3, &["params"])]
    #[case("PerpMarket", "set_profit_and_loss_point", 22, 13, &["subaccount", "market_id", "take_profit_point", "stop_loss_point"])]
    #[case("PerpMarket", "close_position", 22, 14, &["subaccount", "market_id", "price", "slippage"])]
    #[case("PerpMarket", "set_global_leverage", 22, 43, &["subaccount", "max_leverage"])]
    #[case("PerpMarket", "set_per_market_leverage", 22, 44, &["subaccount", "market_id", "max_leverage"])]
    #[case("Lending", "buy_quota", 24, 12, &["address", "quota", "from_subaccount"])]
    fn spec376_metadata_call_contracts(
        #[case] pallet_name: &str,
        #[case] call_name: &str,
        #[case] pallet_index: u8,
        #[case] call_index: u8,
        #[case] fields: &[&str],
    ) {
        let (snapshot, _) = runtime();
        let pallet = snapshot.metadata().pallet_by_name(pallet_name).unwrap();
        let call = pallet.call_variant_by_name(call_name).unwrap();

        assert_eq!(pallet.index(), pallet_index);
        assert_eq!(call.index, call_index);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            fields,
        );
        snapshot.interfaces().call(pallet_name, call_name).unwrap();
    }

    #[rstest]
    fn spec376_perp_place_scale_encoding_is_exact() {
        let (snapshot, client_state) = runtime();
        let params = DeepXPerpPlaceParams {
            subaccount: [0x11; 20],
            market_id: 7,
            is_long: true,
            size: 123,
            price: 456,
            order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Ioc),
            take_profit: Some(789),
            stop_loss: None,
            reduce_only: true,
            post_only: DeepXPostOnlyParam::MustPostOnly,
        };
        let mut expected = vec![22, 2];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&params.market_id.to_le_bytes());
        expected.push(1);
        expected.extend_from_slice(&params.size.to_le_bytes());
        expected.extend_from_slice(&params.price.to_le_bytes());
        expected.extend_from_slice(&[0, 1, 1]);
        expected.extend_from_slice(&789_u128.to_le_bytes());
        expected.extend_from_slice(&[0, 1, 1]);

        let arguments = params.into_dynamic_arguments();
        assert_eq!(
            encode_call(&snapshot, "PerpMarket", "place_order", arguments.clone()),
            expected,
        );
        let signed = sign(
            &snapshot,
            &client_state,
            "PerpMarket",
            "place_order",
            arguments,
            42,
        );
        assert!(signed.bytes().ends_with(&expected));
    }

    #[rstest]
    fn spec376_spot_place_scale_encoding_is_exact() {
        let (snapshot, client_state) = runtime();
        let params = DeepXSpotPlaceParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            is_buy: true,
            quote_amount: [0x33; 32],
            base_amount: [0x44; 32],
            order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc),
            post_only: DeepXPostOnlyParam::None,
            reduce_only: false,
        };
        let mut expected = vec![20, 1];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&params.pair);
        expected.push(1);
        expected.extend_from_slice(&params.quote_amount);
        expected.extend_from_slice(&params.base_amount);
        expected.extend_from_slice(&[0, 0, 0, 0]);

        let arguments = params.into_dynamic_arguments();
        assert_eq!(
            encode_call(&snapshot, "SpotMarket", "place_order", arguments.clone()),
            expected,
        );
        let signed = sign(
            &snapshot,
            &client_state,
            "SpotMarket",
            "place_order",
            arguments,
            42,
        );
        assert!(signed.bytes().ends_with(&expected));
    }

    #[rstest]
    fn spec376_spot_cancel_scale_encoding_is_exact() {
        let (snapshot, client_state) = runtime();
        let params = DeepXSpotCancelParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            order_id: u64::MAX,
            is_buy: true,
            fast_cancel: true,
        };
        let mut expected = vec![20, 3];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&params.pair);
        expected.extend_from_slice(&params.order_id.to_le_bytes());
        expected.extend_from_slice(&[1, 0, 1]);

        let arguments = params.into_dynamic_arguments();
        assert_eq!(
            encode_call(&snapshot, "SpotMarket", "cancel_order", arguments.clone()),
            expected,
        );
        let signed = sign(
            &snapshot,
            &client_state,
            "SpotMarket",
            "cancel_order",
            arguments,
            64,
        );
        assert!(signed.bytes().ends_with(&expected));
    }

    #[rstest]
    #[case::subaccount(0)]
    #[case::market_id(1)]
    #[case::size(2)]
    #[case::order_type(3)]
    #[case::nonce(4)]
    fn spec376_signing_is_sensitive_to_arguments_and_nonce(#[case] mutation: u8) {
        let (snapshot, client_state) = runtime();
        let mut params = DeepXPerpPlaceParams {
            subaccount: [0x11; 20],
            market_id: 7,
            is_long: true,
            size: 123,
            price: 456,
            order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXPostOnlyParam::None,
        };
        let mut nonce = 42;
        let original = sign(
            &snapshot,
            &client_state,
            "PerpMarket",
            "place_order",
            params.into_dynamic_arguments(),
            nonce,
        );
        match mutation {
            0 => params.subaccount[19] ^= 1,
            1 => params.market_id += 1,
            2 => params.size += 1,
            3 => params.order_type = DeepXPerpOrderType::Market(None),
            4 => nonce += 1,
            _ => unreachable!(),
        }
        let changed = sign(
            &snapshot,
            &client_state,
            "PerpMarket",
            "place_order",
            params.into_dynamic_arguments(),
            nonce,
        );

        assert_ne!(original.bytes(), changed.bytes());
        assert_ne!(original.extrinsic_hash(), changed.extrinsic_hash());
        assert_eq!(original.runtime(), changed.runtime());
        assert_eq!(original.signer(), changed.signer());
    }

    #[rstest]
    fn spec376_metadata_rejects_malformed_arguments() {
        let (snapshot, client_state) = runtime();
        let result = sign_dynamic_pallet_call_with_runtime(
            RuntimeSigningContext {
                identity: snapshot.identity(),
                interfaces: snapshot.interfaces(),
                client_state: &client_state,
            },
            &key(),
            "SpotMarket",
            "cancel_order",
            vec![Value::named_composite([(
                "params",
                Value::named_composite([
                    ("subaccount", Value::from_bytes([0x11; 19])),
                    ("pair", Value::from_bytes([0x22; 32])),
                    ("order_id", Value::u128(7)),
                    ("is_buy", Value::bool(false)),
                    (
                        "cancel_reason",
                        Value::unnamed_variant("UserCanceled", Vec::<Value>::new()),
                    ),
                    ("fast_cancel", Value::bool(false)),
                ]),
            )])],
            42,
        );

        assert!(matches!(result, Err(SigningError::Encode(_))));
    }

    #[rstest]
    fn spec376_interface_rejects_unknown_call() {
        let (snapshot, client_state) = runtime();
        let result = sign_dynamic_pallet_call_with_runtime(
            RuntimeSigningContext {
                identity: snapshot.identity(),
                interfaces: snapshot.interfaces(),
                client_state: &client_state,
            },
            &key(),
            "UnknownPallet",
            "unknown_call",
            Vec::new(),
            42,
        );

        assert!(matches!(
            result,
            Err(SigningError::RuntimeInterface(
                DeepXRuntimeInterfaceError::PalletUnavailable(pallet),
            )) if pallet == "UnknownPallet"
        ));
    }
}
