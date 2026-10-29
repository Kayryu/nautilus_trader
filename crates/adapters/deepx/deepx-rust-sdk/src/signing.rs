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

//! Offline signing for DeepX requests.

use parity_scale_codec::Decode;

pub mod runtime_interface;

mod error;
mod evm;
mod guard;
mod management;
mod named;
mod operation;
mod pallet;
mod quota;
mod runtime;
mod snapshot;

pub use error::SigningError;
pub use evm::{
    DeepXEvmTransactionKind, DeepXOfflineEvmExtrinsic, DeepXOfflineEvmSignError,
    DeepXOfflineEvmTransaction, DeepXOfflineEvmTransactionParams, DeepXOfflineEvmWrapperError,
    DeepXOfflineEvmWrapperSignError, DeepXOfflinePerpPnlEvmParams,
    sign_offline_evm_transact_extrinsic, sign_offline_evm_transaction,
    sign_permitted_evm_transact_extrinsic, sign_permitted_perp_pnl_evm_transact_extrinsic,
};
pub use guard::{
    DeepXReadOnlyRuntimeSnapshot, DeepXRuntimeChangeDecision, DeepXRuntimeSnapshotPermit,
    DeepXRuntimeSnapshotService, DeepXRuntimeSnapshotServiceError, DeepXRuntimeSnapshotUpdate,
    RuntimeSnapshot,
};
pub use management::{
    DeepXBuyQuotaParams, DeepXDeleteSubaccountParams, DeepXInitializeSubaccountParams,
    DeepXRemoveDelegateAccountParams, DeepXRenameSubaccountParams, DeepXSetDelegateAccountParams,
    DeepXSetGlobalLeverageParams, DeepXSetPerMarketLeverageParams, DeepXSetSpotMarginParams,
    DeepXUpdateDelegateModeParams, DeepXWritableDelegateMode,
};
pub use named::{
    sign_buy_quota, sign_delete_subaccount, sign_initialize_subaccount, sign_no_op,
    sign_perp_cancel, sign_perp_close, sign_perp_modify_order, sign_perp_place_order,
    sign_perp_set_profit_and_loss_point, sign_remove_delegate_account, sign_rename_subaccount,
    sign_set_delegate_account, sign_set_global_leverage, sign_set_per_market_leverage,
    sign_set_spot_margin, sign_spot_cancel, sign_spot_place_order, sign_update_delegate_mode,
};
pub use operation::{DeepXDirectRuntimeIdentity, DeepXNonceReservation, DeepXTransactionOperation};
pub use pallet::{
    DeepXPerpCancelParams, DeepXPerpCloseParams, DeepXPerpModifyParams, DeepXPerpOrderType,
    DeepXPerpPlaceParams, DeepXPerpProfitAndLossPointParams, DeepXPostOnlyParam,
    DeepXSpotCancelParams, DeepXSpotOrderType, DeepXSpotPlaceParams, DeepXTimeInForce,
};
pub use quota::{DeepXQuotaClaimSignature, DeepXQuotaClaimSigningError, sign_quota_claim};
pub use runtime::{
    CheckNonceEra, DeepXEncodedPalletExtrinsic, DeepXOfflinePalletCall, DeepXPalletSigningError,
    DeepXPermittedPalletExtrinsic, DeepXRuntimeConfig, derive_signer_account_id,
    sign_dynamic_pallet_call, sign_pallet_call, sign_permitted_dynamic_pallet_call,
    sign_permitted_pallet_call, sign_validated_dynamic_pallet_call, sign_validated_pallet_call,
};
pub use snapshot::{
    ApprovedRuntimeIdentity, DeepXValidatedRuntime, SnapshotError, validate_testnet_runtime,
};

/// Nonce-era extension decoded from DeepX runtime extrinsics.
#[doc(hidden)]
#[derive(Debug, Decode)]
pub enum DeepXNonceEra {
    Immortal,
    Mortal(u64),
}
