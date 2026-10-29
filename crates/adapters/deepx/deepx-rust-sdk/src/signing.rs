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

mod evm;
mod management;
mod operation;
mod pallet;
mod quota;
mod runtime;
mod snapshot;

pub use evm::{
    DeepXEvmTransactionKind, DeepXOfflineEvmExtrinsic, DeepXOfflineEvmSignError,
    DeepXOfflineEvmTransaction, DeepXOfflineEvmTransactionParams, DeepXOfflineEvmWrapperSignError,
    DeepXOfflinePerpPnlEvmParams, sign_offline_evm_transact_extrinsic,
    sign_offline_evm_transaction,
};
pub use management::{
    DeepXBuyQuotaParams, DeepXDeleteSubaccountParams, DeepXInitializeSubaccountParams,
    DeepXRemoveDelegateAccountParams, DeepXRenameSubaccountParams, DeepXSetDelegateAccountParams,
    DeepXSetGlobalLeverageParams, DeepXSetPerMarketLeverageParams, DeepXSetSpotMarginParams,
    DeepXUpdateDelegateModeParams, DeepXWritableDelegateMode,
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
    DeepXRuntimeConfig, derive_signer_account_id, sign_dynamic_pallet_call, sign_pallet_call,
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
