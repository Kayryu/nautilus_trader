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

//! Account-scoped offline dispatch for operation-specific direct-pallet records.

use std::fmt;
use thiserror::Error;

use super::{
    DeepXBusinessCallBindingError, DeepXBusinessCallVerifier, DeepXCommittedTransactionRecord,
    DeepXDirectRuntimeIdentity, DeepXDurableSignedExtrinsic, DeepXNonceReservation,
    DeepXPerpCancelCallVerifier, DeepXPerpCloseCallVerifier, DeepXPerpPlaceCallVerifier,
    DeepXPerpProfitAndLossPointCallVerifier, DeepXPreparedSignedTransaction,
    DeepXRestoredTransactionRecord, DeepXSignedTransactionPreparationError, DeepXSignerLease,
    DeepXSpotCancelCallVerifier, DeepXSpotPlaceCallVerifier, DeepXTransactionIdentity,
    DeepXTransactionOperation, DeepXTransactionPersistenceError, DeepXTransactionRecord,
    DeepXTransactionStore, load_verified_committed_for_signer,
    prepare_signed_perp_cancel_transaction, prepare_signed_perp_close_transaction,
    prepare_signed_perp_place_transaction, prepare_signed_perp_profit_and_loss_point_transaction,
    prepare_signed_spot_cancel_transaction, prepare_signed_spot_place_transaction,
};
use crate::{
    common::DeepXPrivateKey,
    signing::{
        DeepXRuntimeSnapshotPermit, RuntimeSnapshot, SigningError, derive_signer_account_id,
    },
};

/// Canonical verifier for all supported direct-pallet order operations in one account scope.
///
/// Uses the existing operation-specific verifiers; no dynamic call or legacy record fallback is
/// allowed. Canonical byte equality proves identity binding, not subaccount authorization,
/// financial units, business success, or permission to submit a restored transaction.
#[derive(Clone)]
pub struct DeepXDirectPalletCallVerifier {
    snapshot: RuntimeSnapshot,
    key: DeepXPrivateKey,
    signer: [u8; 20],
    subaccount: [u8; 20],
}

impl DeepXDirectPalletCallVerifier {
    /// Binds mixed-operation verification and offline preparation to one runtime and account.
    ///
    /// # Errors
    ///
    /// Returns an error when the signing key cannot derive an AccountId20.
    pub fn new(
        snapshot: RuntimeSnapshot,
        key: DeepXPrivateKey,
        subaccount: [u8; 20],
    ) -> Result<Self, SigningError> {
        let signer = derive_signer_account_id(&key)?;
        Ok(Self {
            snapshot,
            key,
            signer,
            subaccount,
        })
    }

    fn validate_identity<'a>(
        &self,
        identity: &'a DeepXTransactionIdentity,
    ) -> Result<&'a DeepXTransactionOperation, DeepXBusinessCallBindingError> {
        if identity.signer() != self.signer
            || *identity.runtime() != DeepXDirectRuntimeIdentity::from(self.snapshot.identity())
        {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "direct-pallet signer or runtime differs from account scope".to_string(),
            ));
        }
        let operation = identity.operation().ok_or_else(|| {
            DeepXBusinessCallBindingError::Unsupported(
                "direct-pallet record has no operation-specific identity".to_string(),
            )
        })?;
        if !matches!(
            identity.nonce(),
            DeepXNonceReservation::TimestampOrderId { .. }
        ) {
            return Err(DeepXBusinessCallBindingError::Unsupported(
                "sequential account nonce domain remains unproven".to_string(),
            ));
        }
        let subaccount = match operation {
            DeepXTransactionOperation::PerpPlace { subaccount, .. }
            | DeepXTransactionOperation::PerpClose { subaccount, .. }
            | DeepXTransactionOperation::PerpProfitAndLossPoint { subaccount, .. }
            | DeepXTransactionOperation::PerpCancel { subaccount, .. }
            | DeepXTransactionOperation::SpotPlace { subaccount, .. }
            | DeepXTransactionOperation::SpotCancel { subaccount, .. } => subaccount,
        };
        if *subaccount != self.subaccount {
            return Err(DeepXBusinessCallBindingError::Mismatch(
                "direct-pallet subaccount differs from account scope".to_string(),
            ));
        }
        Ok(operation)
    }
}

/// Failure while restoring operation-specific transactions in a configured account scope.
#[derive(Debug, Error)]
pub enum DeepXDirectPalletRestoreError {
    /// Lease ownership or exact committed-record integrity could not be proven.
    #[error(transparent)]
    Persistence(#[from] DeepXTransactionPersistenceError),
    /// Account scope or canonical signed call binding could not be proven.
    #[error(transparent)]
    Binding(#[from] DeepXBusinessCallBindingError),
}

/// Loads the entire signer record set, verifying every operation and retained signed payload.
///
/// Validation is failure-atomic: no partial result is returned. Created records must carry an
/// operation in the configured account scope; signed records additionally require exact canonical
/// byte equality. Restoration never authorizes submission or replay.
///
/// # Errors
///
/// Returns an error for a foreign or stale lease, invalid durable acknowledgement, legacy or
/// foreign-account identity, unsupported nonce domain, or noncanonical retained signed bytes.
pub async fn load_verified_direct_pallet_for_signer<S>(
    store: &S,
    lease: &S::Lease,
    verifier: &DeepXDirectPalletCallVerifier,
) -> Result<Vec<DeepXRestoredTransactionRecord>, DeepXDirectPalletRestoreError>
where
    S: DeepXTransactionStore,
{
    if lease.signer() != verifier.signer {
        return Err(DeepXTransactionPersistenceError::LeaseMismatch.into());
    }
    let restored = load_verified_committed_for_signer(store, lease).await?;
    for item in &restored {
        let record = item.record();
        verifier.validate_identity(record.identity())?;
        if let Some(signed) = record.signed_extrinsic() {
            verifier.verify(record.identity(), signed)?;
        }
    }
    Ok(restored)
}

impl fmt::Debug for DeepXDirectPalletCallVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeepXDirectPalletCallVerifier")
            .field("snapshot", &self.snapshot.identity())
            .field("signer", &self.signer)
            .field("subaccount", &self.subaccount)
            .field("key", &"<redacted>")
            .finish()
    }
}

impl DeepXBusinessCallVerifier for DeepXDirectPalletCallVerifier {
    fn verify(
        &self,
        identity: &DeepXTransactionIdentity,
        signed_extrinsic: &DeepXDurableSignedExtrinsic,
    ) -> Result<(), DeepXBusinessCallBindingError> {
        macro_rules! verify_with {
            ($verifier:ty) => {
                <$verifier>::new(self.snapshot.clone(), self.key.clone())
                    .map_err(|error| DeepXBusinessCallBindingError::Unsupported(error.to_string()))?
                    .verify(identity, signed_extrinsic)
            };
        }
        match self.validate_identity(identity)? {
            DeepXTransactionOperation::PerpPlace { .. } => verify_with!(DeepXPerpPlaceCallVerifier),
            DeepXTransactionOperation::PerpClose { .. } => verify_with!(DeepXPerpCloseCallVerifier),
            DeepXTransactionOperation::PerpProfitAndLossPoint { .. } => {
                verify_with!(DeepXPerpProfitAndLossPointCallVerifier)
            }
            DeepXTransactionOperation::PerpCancel { .. } => {
                verify_with!(DeepXPerpCancelCallVerifier)
            }
            DeepXTransactionOperation::SpotPlace { .. } => verify_with!(DeepXSpotPlaceCallVerifier),
            DeepXTransactionOperation::SpotCancel { .. } => {
                verify_with!(DeepXSpotCancelCallVerifier)
            }
        }
    }
}

/// Signs and durably commits a Created record using its exact operation-specific signer.
///
/// Verifies the configured account scope before delegating to the existing lease, runtime permit,
/// nonce, canonical call binding, and compare-and-set boundaries. Does not submit any bytes.
///
/// # Errors
///
/// Returns an error for foreign account scopes, missing operations, invalid Created evidence,
/// stale permits, invalid call inputs, signing failures, or unproven durable acknowledgements.
pub async fn prepare_signed_direct_pallet_transaction<S>(
    store: &S,
    lease: &S::Lease,
    committed_created: &DeepXCommittedTransactionRecord,
    record: &DeepXTransactionRecord,
    permit: &DeepXRuntimeSnapshotPermit,
    verifier: &DeepXDirectPalletCallVerifier,
) -> Result<DeepXPreparedSignedTransaction, DeepXSignedTransactionPreparationError>
where
    S: DeepXTransactionStore,
{
    macro_rules! prepare_with {
        ($prepare:ident) => {
            $prepare(
                store,
                lease,
                committed_created,
                record,
                permit,
                &verifier.key,
            )
            .await
        };
    }
    match verifier.validate_identity(record.identity())? {
        DeepXTransactionOperation::PerpPlace { .. } => {
            prepare_with!(prepare_signed_perp_place_transaction)
        }
        DeepXTransactionOperation::PerpClose { .. } => {
            prepare_with!(prepare_signed_perp_close_transaction)
        }
        DeepXTransactionOperation::PerpProfitAndLossPoint { .. } => {
            prepare_with!(prepare_signed_perp_profit_and_loss_point_transaction)
        }
        DeepXTransactionOperation::PerpCancel { .. } => {
            prepare_with!(prepare_signed_perp_cancel_transaction)
        }
        DeepXTransactionOperation::SpotPlace { .. } => {
            prepare_with!(prepare_signed_spot_place_transaction)
        }
        DeepXTransactionOperation::SpotCancel { .. } => {
            prepare_with!(prepare_signed_spot_cancel_transaction)
        }
    }
}
