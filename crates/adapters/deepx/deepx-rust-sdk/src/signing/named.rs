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

//! Named permit-bound offline pallet signing entry points.

use super::{
    DeepXBuyQuotaParams, DeepXDeleteSubaccountParams, DeepXInitializeSubaccountParams,
    DeepXOfflinePalletCall, DeepXPermittedPalletExtrinsic, DeepXPerpCancelParams,
    DeepXPerpCloseParams, DeepXPerpModifyParams, DeepXPerpPlaceParams,
    DeepXPerpProfitAndLossPointParams, DeepXPerpSettlePnlParams, DeepXRemoveDelegateAccountParams,
    DeepXRenameSubaccountParams, DeepXRuntimeSnapshotPermit, DeepXSetDelegateAccountParams,
    DeepXSetGlobalLeverageParams, DeepXSetPerMarketLeverageParams, DeepXSetSpotMarginParams,
    DeepXSpotCancelParams, DeepXSpotModifyParams, DeepXSpotPlaceParams,
    DeepXUpdateDelegateModeParams, SigningError, sign_permitted_pallet_call,
};
use crate::credential::DeepXPrivateKey;

macro_rules! named_signer {
    ($( $(#[$attribute:meta])* $name:ident($params:ty) => $variant:ident; )+) => {
        $(
            $(#[$attribute])*
            pub fn $name(
                permit: &DeepXRuntimeSnapshotPermit,
                key: &DeepXPrivateKey,
                params: $params,
                nonce: u64,
            ) -> Result<DeepXPermittedPalletExtrinsic, SigningError> {
                sign_permitted_pallet_call(
                    permit,
                    key,
                    DeepXOfflinePalletCall::$variant(params),
                    nonce,
                )
            }
        )+
    };
}

named_signer! {
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
    sign_perp_place_order(DeepXPerpPlaceParams) => PerpPlaceOrder;

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
    sign_spot_place_order(DeepXSpotPlaceParams) => SpotPlaceOrder;

    /// Signs a perpetual order cancellation without submitting it.
    ///
    /// The cancellation reason is fixed to the runtime's `UserCanceled` variant. As with
    /// [`super::sign_permitted_dynamic_pallet_call`], the caller owns nonce reservation and persistence.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is invalid or the call cannot be encoded against the approved
    /// runtime snapshot.
    sign_perp_cancel(DeepXPerpCancelParams) => PerpCancel;

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
    sign_perp_modify_order(DeepXPerpModifyParams) => PerpModifyOrder;

    /// Signs an atomic Spot order cancellation and replacement.
    ///
    /// The two legs must target the same subaccount, pair, and side. The supplied nonce becomes
    /// the replacement order ID. This offline signer does not submit or establish success.
    ///
    /// # Errors
    ///
    /// Returns an error for conflicting scopes or a failed approved-runtime encoding.
    sign_spot_modify_order(DeepXSpotModifyParams) => SpotModifyOrder;

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
    sign_spot_cancel(DeepXSpotCancelParams) => SpotCancel;

    /// Signs a metadata-driven perpetual position close without submitting it.
    ///
    /// Price and slippage are raw runtime integers. This function establishes no financial unit
    /// semantics, authorization, SDK parity, nonce reservation, or operational close capability.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or the permitted snapshot cannot encode the close call.
    sign_perp_close(DeepXPerpCloseParams) => PerpClose;

    /// Signs perpetual profit and loss point management without submitting it.
    ///
    /// Both points are exact runtime integers. This offline API performs no unit conversion,
    /// nonce reservation, persistence, or network access, and proves no authorization,
    /// trigger semantics, independent SDK parity, or operational management capability.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or permitted snapshot cannot encode the management call.
    sign_perp_set_profit_and_loss_point(DeepXPerpProfitAndLossPointParams) => PerpSetProfitAndLossPoint;

    /// Signs permissionless settlement of one or all perpetual markets without submitting it.
    ///
    /// The caller owns nonce reservation, submission, finality, and `SettlePnl` event verification.
    /// A second settlement can succeed without emitting a nonzero settlement event.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or permitted runtime cannot encode the call.
    sign_perp_settle_pnl(DeepXPerpSettlePnlParams) => PerpSettlePnl;

    /// Signs a global perpetual leverage setting without submitting it.
    ///
    /// The runtime checks ownership and leverage bounds. This offline API does not establish either
    /// condition, reserve a nonce, persist a transaction, or activate a framework command.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or permitted runtime cannot encode the call.
    sign_set_global_leverage(DeepXSetGlobalLeverageParams) => SetGlobalLeverage;

    /// Signs a per-market perpetual leverage setting without submitting it.
    ///
    /// The runtime checks ownership, market eligibility, and leverage bounds. This offline API does
    /// not establish those conditions, reserve a nonce, persist a transaction, or activate a command.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or permitted runtime cannot encode the call.
    sign_set_per_market_leverage(DeepXSetPerMarketLeverageParams) => SetPerMarketLeverage;

    /// Signs subaccount initialization without submitting it.
    ///
    /// This offline API does not infer the resulting address, owner eligibility, name uniqueness, or
    /// business success. It performs no network access, nonce allocation, persistence, or submission.
    ///
    /// # Errors
    ///
    /// Returns an error if the name exceeds the runtime bound or the key or permitted snapshot cannot
    /// encode the call.
    sign_initialize_subaccount(DeepXInitializeSubaccountParams) => InitializeSubaccount;

    /// Signs subaccount deletion without submitting it.
    ///
    /// This offline API infers no ownership, eligibility, or deletion semantics and performs no
    /// network access, nonce allocation, persistence, replay, or live activation.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or permitted snapshot cannot encode the call.
    sign_delete_subaccount(DeepXDeleteSubaccountParams) => DeleteSubaccount;

    /// Signs subaccount renaming without submitting it.
    ///
    /// This offline API does not establish ownership, name uniqueness, or business success. It
    /// performs no network access, nonce allocation, persistence, or submission.
    ///
    /// # Errors
    ///
    /// Returns an error if the name exceeds the runtime bound or the key or permitted snapshot cannot
    /// encode the call.
    sign_rename_subaccount(DeepXRenameSubaccountParams) => RenameSubaccount;

    /// Signs a subaccount Spot margin change without submitting it.
    ///
    /// The runtime requires ownership and no open Spot orders. This offline API does not establish
    /// either condition, reserve a nonce, persist a transaction, or activate a framework command.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or permitted runtime cannot encode the call.
    sign_set_spot_margin(DeepXSetSpotMarginParams) => SetSpotMargin;

    /// Signs wallet delegate creation or renewal without submitting it.
    ///
    /// This offline API performs no authorization, clock, ownership, or quota checks and does not
    /// reserve a nonce, persist a transaction, or enable a management command.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or permitted snapshot cannot encode the call.
    sign_set_delegate_account(DeepXSetDelegateAccountParams) => SetDelegateAccount;

    /// Signs a wallet delegate mode update without submitting it.
    ///
    /// This offline API does not check account existence, owner authorization, expiry, or business
    /// success. It reserves no nonce and performs no network access, persistence, or submission.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or permitted snapshot cannot encode the call.
    sign_update_delegate_mode(DeepXUpdateDelegateModeParams) => UpdateDelegateMode;

    /// Signs wallet delegate removal without submitting it.
    ///
    /// This offline API infers no authorization or revocation semantics and performs no network
    /// access, nonce reservation, persistence, or live activation.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or permitted snapshot cannot encode the call.
    sign_remove_delegate_account(DeepXRemoveDelegateAccountParams) => RemoveDelegateAccount;

    /// Signs a Lending quota purchase call without submitting it.
    ///
    /// This offline API does not establish payer authorization, quota pricing, fund availability,
    /// nonce reservation, transaction persistence, or purchase finality.
    ///
    /// # Errors
    ///
    /// Returns an error if the key or permitted runtime cannot encode the call.
    sign_buy_quota(DeepXBuyQuotaParams) => BuyQuota;
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
    permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    nonce: u64,
) -> Result<DeepXPermittedPalletExtrinsic, SigningError> {
    sign_permitted_pallet_call(permit, key, DeepXOfflinePalletCall::NoOp, nonce)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{
        credential::DeepXKeyScheme,
        signing::{DeepXRuntimeSnapshotService, RuntimeSnapshot},
    };

    #[rstest]
    fn named_signing_matches_typed_operation_and_observes_permit_revocation() {
        let service =
            DeepXRuntimeSnapshotService::new(RuntimeSnapshot::built_in_testnet().unwrap());
        let permit = service.acquire().unwrap();
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let params = DeepXPerpCancelParams {
            subaccount: [0x11; 20],
            order_id: 42,
            market_id: 7,
            fast_cancel: false,
        };

        let named = sign_perp_cancel(&permit, &key, params, 1_725_000_000_125).unwrap();
        let typed = sign_permitted_pallet_call(
            &permit,
            &key,
            DeepXOfflinePalletCall::PerpCancel(params),
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(named, typed);
        assert_eq!(named.runtime(), permit.snapshot().identity());

        service.halt_for_inconsistent_evidence().unwrap();
        assert!(matches!(
            sign_perp_cancel(&permit, &key, params, 1_725_000_000_126),
            Err(SigningError::RuntimePermit(_)),
        ));
    }

    #[rstest]
    fn settle_pnl_signs_one_market_and_all_markets_with_exact_scale_arguments() {
        let service =
            DeepXRuntimeSnapshotService::new(RuntimeSnapshot::built_in_testnet().unwrap());
        let permit = service.acquire().unwrap();
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let account = [0x11; 20];
        let nonce = 1_725_000_000_125;
        let one = sign_perp_settle_pnl(
            &permit,
            &key,
            DeepXPerpSettlePnlParams {
                subaccount: account,
                market_id: Some(3),
            },
            nonce,
        )
        .unwrap();
        let all = sign_perp_settle_pnl(
            &permit,
            &key,
            DeepXPerpSettlePnlParams {
                subaccount: account,
                market_id: None,
            },
            nonce,
        )
        .unwrap();

        let mut one_call = vec![22, 45];
        one_call.extend_from_slice(&account);
        one_call.extend_from_slice(&[1, 3, 0]);
        let mut all_call = vec![22, 45];
        all_call.extend_from_slice(&account);
        all_call.push(0);
        assert!(one.bytes().ends_with(&one_call));
        assert!(all.bytes().ends_with(&all_call));
        assert_ne!(one.extrinsic_hash(), all.extrinsic_hash());
    }
}
