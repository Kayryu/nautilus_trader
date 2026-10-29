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

//! Compatibility facade for SDK signing and Nautilus transaction persistence.

use deepx_rust_sdk::signing as sdk_signing;
pub use deepx_rust_sdk::signing::{
    ApprovedRuntimeIdentity, DeepXReadOnlyRuntimeSnapshot, DeepXRuntimeChangeDecision,
    DeepXRuntimeSnapshotPermit, DeepXRuntimeSnapshotService, DeepXRuntimeSnapshotServiceError,
    DeepXRuntimeSnapshotUpdate, RuntimeSnapshot, SnapshotError,
    runtime_interface::{
        DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError, DeepXRuntimePalletInterface,
        DeepXRuntimeVariantIdentity,
    },
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
    DeepXWritableDelegateMode, SigningError, sign_quota_claim,
};
pub use deepx_rust_sdk::signing::{
    DeepXEvmTransactionKind, DeepXOfflineEvmExtrinsic, DeepXOfflineEvmSignError,
    DeepXOfflineEvmTransaction, DeepXOfflineEvmTransactionParams, DeepXOfflineEvmWrapperError,
    DeepXOfflinePerpPnlEvmParams, sign_offline_evm_transaction,
    sign_permitted_evm_transact_extrinsic as sign_offline_evm_transact_extrinsic,
    sign_permitted_perp_pnl_evm_transact_extrinsic as sign_offline_perp_pnl_evm_transact_extrinsic,
};
use subxt_core::dynamic::Value;

use crate::common::DeepXPrivateKey;

/// Derives the Ethereum-compatible DeepX account identity for a signing key.
///
/// # Errors
///
/// Returns an error if the pinned signer implementation rejects the private scalar.
pub fn derive_signer_account_id(key: &DeepXPrivateKey) -> Result<[u8; 20], SigningError> {
    Ok(sdk_signing::derive_signer_account_id(key)?)
}

pub use crate::transaction::SignedPalletExtrinsic;

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
    sdk_signing::sign_permitted_dynamic_pallet_call(
        snapshot_permit,
        key,
        pallet,
        call,
        arguments,
        nonce,
    )
    .map(Into::into)
}

macro_rules! adapter_signer {
    ($($name:ident($params:ty): $description:literal;)+) => {
        $(
            #[doc = $description]
            ///
            /// # Errors
            ///
            /// Returns an error if the permit is inactive or the call cannot be signed.
            pub fn $name(
                snapshot_permit: &DeepXRuntimeSnapshotPermit,
                key: &DeepXPrivateKey,
                params: $params,
                nonce: u64,
            ) -> Result<SignedPalletExtrinsic, SigningError> {
                sdk_signing::$name(snapshot_permit, key, params, nonce).map(Into::into)
            }
        )+
    };
}

adapter_signer! {
    sign_perp_place_order(DeepXPerpPlaceParams): "Signs a perpetual order placement without submitting it.";
    sign_spot_place_order(DeepXSpotPlaceParams): "Signs a Spot order placement without submitting it.";
    sign_perp_cancel(DeepXPerpCancelParams): "Signs a perpetual order cancellation without submitting it.";
    sign_perp_modify_order(DeepXPerpModifyParams): "Signs a perpetual order modification as one transactional cancel-and-place call.";
    sign_spot_cancel(DeepXSpotCancelParams): "Signs a Spot order cancellation without submitting it.";
    sign_perp_close(DeepXPerpCloseParams): "Signs a metadata-driven perpetual position close without submitting it.";
    sign_perp_set_profit_and_loss_point(DeepXPerpProfitAndLossPointParams): "Signs perpetual profit and loss point management without submitting it.";
    sign_set_global_leverage(DeepXSetGlobalLeverageParams): "Signs a global perpetual leverage setting without submitting it.";
    sign_set_per_market_leverage(DeepXSetPerMarketLeverageParams): "Signs a per-market perpetual leverage setting without submitting it.";
    sign_initialize_subaccount(DeepXInitializeSubaccountParams): "Signs subaccount initialization without submitting it.";
    sign_delete_subaccount(DeepXDeleteSubaccountParams): "Signs subaccount deletion without submitting it.";
    sign_rename_subaccount(DeepXRenameSubaccountParams): "Signs subaccount renaming without submitting it.";
    sign_set_spot_margin(DeepXSetSpotMarginParams): "Signs a subaccount Spot margin change without submitting it.";
    sign_set_delegate_account(DeepXSetDelegateAccountParams): "Signs wallet delegate creation or renewal without submitting it.";
    sign_update_delegate_mode(DeepXUpdateDelegateModeParams): "Signs a wallet delegate mode update without submitting it.";
    sign_remove_delegate_account(DeepXRemoveDelegateAccountParams): "Signs wallet delegate removal without submitting it.";
    sign_buy_quota(DeepXBuyQuotaParams): "Signs a Lending quota purchase call without submitting it.";
}

/// Signs a Subaccount no-op without submitting it.
///
/// # Errors
///
/// Returns an error if the permit is inactive or the call cannot be signed.
pub fn sign_no_op(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sdk_signing::sign_no_op(snapshot_permit, key, nonce).map(Into::into)
}

#[cfg(test)]
#[path = "signing_tests.rs"]
mod tests;
