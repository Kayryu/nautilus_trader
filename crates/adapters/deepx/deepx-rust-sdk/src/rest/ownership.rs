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

//! REST directory and profile evidence for DeepX account ownership.

use std::{borrow::Borrow, collections::HashSet};

use thiserror::Error;

use super::models::{
    DeepXDelegateAccount, DeepXDelegateMode, DeepXDelegateWallets, DeepXSubaccountProfile,
    DeepXWalletDelegateAccounts, DeepXWalletSubaccounts,
};
use crate::identity::decode_account_id20;

/// Errors raised while checking REST ownership evidence for a signer.
#[derive(Debug, Error)]
pub enum DeepXOwnershipEvidenceError {
    /// An address in the supplied identity evidence is not an exact AccountId20.
    #[error("invalid DeepX {field} address")]
    InvalidAddress { field: &'static str },
    /// The account directory was queried for a wallet other than the signer.
    #[error("DeepX account directory wallet does not match the signing identity")]
    DirectoryWalletMismatch,
    /// The account directory contains a repeated subaccount identity.
    #[error("DeepX account directory contains a duplicate subaccount")]
    DuplicateSubaccount,
    /// The configured subaccount is absent from the signer's account directory.
    #[error("configured DeepX subaccount is not owned by the signing identity")]
    SubaccountNotOwned,
    /// The profile belongs to a subaccount other than the configured identity.
    #[error("DeepX subaccount profile address does not match the configured subaccount")]
    ProfileAddressMismatch,
    /// The profile authority is not the configured signing identity.
    #[error("DeepX subaccount profile authority does not match the signing identity")]
    ProfileAuthorityMismatch,
    /// The configured subaccount is not active and cannot pass execution startup.
    #[error("DeepX subaccount profile is not active: {0}")]
    InactiveSubaccount(String),
    /// The delegate's reverse directory does not contain the profile authority.
    #[error("DeepX delegate directory does not contain the subaccount authority")]
    DelegatorUnavailable,
    /// The authority's delegate directory does not contain the signer.
    #[error("DeepX subaccount authority has not configured the signing delegate")]
    DelegateUnavailable,
    /// The configured delegate has no order permission.
    #[error("DeepX signing delegate is not permitted to place or cancel orders")]
    DelegateModeUnsupported,
    /// The configured delegate has expired or the backend marked it inactive.
    #[error("DeepX signing delegate is inactive or expired")]
    DelegateInactive,
    /// A delegate directory belongs to an address other than the queried identity.
    #[error("DeepX delegate directory identity mismatch")]
    DelegateDirectoryMismatch,
}

/// Complete REST-backed ownership evidence for one signer and configured subaccount.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXAccountOwnershipProof {
    signer: [u8; 20],
    wallet: [u8; 20],
    subaccount: [u8; 20],
    profile_height: u64,
    profile_created_at: u64,
}

impl DeepXAccountOwnershipProof {
    /// Returns the signer AccountId20.
    #[must_use]
    pub const fn signer(&self) -> [u8; 20] {
        self.signer
    }

    /// Returns the wallet that owns the configured subaccount.
    #[must_use]
    pub const fn wallet(&self) -> [u8; 20] {
        self.wallet
    }

    /// Returns the owned subaccount AccountId20.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns the profile observation block height reported by REST.
    #[must_use]
    pub const fn profile_height(&self) -> u64 {
        self.profile_height
    }

    /// Returns the subaccount creation timestamp in Unix milliseconds.
    #[must_use]
    pub const fn profile_created_at(&self) -> u64 {
        self.profile_created_at
    }
}

/// Checks that REST directory and profile evidence bind a subaccount to the signer.
///
/// The proof does not establish freshness, private-stream authentication, or trading authority.
///
/// # Errors
///
/// Returns an error for malformed or inconsistent identity evidence.
pub fn verify_account_ownership_evidence(
    signer: [u8; 20],
    configured_subaccount: &str,
    directory: &DeepXWalletSubaccounts,
    profile: &DeepXSubaccountProfile,
) -> Result<DeepXAccountOwnershipProof, DeepXOwnershipEvidenceError> {
    verify_for_wallet(signer, signer, configured_subaccount, directory, profile)
}

/// Checks both delegate directories and the active order permission before proving ownership.
///
/// # Errors
///
/// Returns an error for malformed or inconsistent identity or delegate evidence.
pub fn verify_delegated_account_ownership_evidence<T>(
    signer: [u8; 20],
    configured_subaccount: &str,
    directory: &DeepXWalletSubaccounts,
    profile: &DeepXSubaccountProfile,
    delegates: &DeepXWalletDelegateAccounts<T>,
    delegators: &DeepXDelegateWallets,
    now_ms: u64,
) -> Result<DeepXAccountOwnershipProof, DeepXOwnershipEvidenceError>
where
    T: Borrow<DeepXDelegateAccount>,
{
    let wallet = parse_account_id20(&profile.authority, "profile authority")?;
    if parse_account_id20(delegates.wallet(), "delegate directory wallet")? != wallet
        || parse_account_id20(delegators.delegate(), "delegator directory delegate")? != signer
    {
        return Err(DeepXOwnershipEvidenceError::DelegateDirectoryMismatch);
    }
    let mut observed_wallets = HashSet::new();
    for address in &delegators.wallets {
        let address = parse_account_id20(address, "delegator wallet")?;
        if !observed_wallets.insert(address) {
            return Err(DeepXOwnershipEvidenceError::DelegateDirectoryMismatch);
        }
    }
    if !observed_wallets.contains(&wallet) {
        return Err(DeepXOwnershipEvidenceError::DelegatorUnavailable);
    }
    let mut observed_delegates = HashSet::new();
    let mut delegate = None;
    for account in &delegates.accounts {
        let account = account.borrow();
        let address = parse_account_id20(&account.delegate_address, "delegate address")?;
        if !observed_delegates.insert(address) {
            return Err(DeepXOwnershipEvidenceError::DelegateDirectoryMismatch);
        }
        if address == signer {
            delegate = Some(account);
        }
    }
    let delegate = delegate.ok_or(DeepXOwnershipEvidenceError::DelegateUnavailable)?;
    if delegate.mode != DeepXDelegateMode::PlaceOrCancelOrder {
        return Err(DeepXOwnershipEvidenceError::DelegateModeUnsupported);
    }
    if !delegate.active || (delegate.valid_until != 0 && delegate.valid_until <= now_ms) {
        return Err(DeepXOwnershipEvidenceError::DelegateInactive);
    }
    verify_for_wallet(signer, wallet, configured_subaccount, directory, profile)
}

fn verify_for_wallet(
    signer: [u8; 20],
    wallet: [u8; 20],
    configured_subaccount: &str,
    directory: &DeepXWalletSubaccounts,
    profile: &DeepXSubaccountProfile,
) -> Result<DeepXAccountOwnershipProof, DeepXOwnershipEvidenceError> {
    let subaccount = parse_account_id20(configured_subaccount, "configured subaccount")?;
    let directory_wallet = parse_account_id20(directory.wallet(), "account directory wallet")?;
    if directory_wallet != wallet {
        return Err(DeepXOwnershipEvidenceError::DirectoryWalletMismatch);
    }

    let mut observed = HashSet::new();
    let mut owns_subaccount = false;
    for address in &directory.addresses {
        let address = parse_account_id20(address, "account directory subaccount")?;
        if !observed.insert(address) {
            return Err(DeepXOwnershipEvidenceError::DuplicateSubaccount);
        }
        owns_subaccount |= address == subaccount;
    }
    if !owns_subaccount {
        return Err(DeepXOwnershipEvidenceError::SubaccountNotOwned);
    }

    if parse_account_id20(&profile.address, "profile subaccount")? != subaccount {
        return Err(DeepXOwnershipEvidenceError::ProfileAddressMismatch);
    }
    if parse_account_id20(&profile.authority, "profile authority")? != wallet {
        return Err(DeepXOwnershipEvidenceError::ProfileAuthorityMismatch);
    }
    if profile.status != "Active" {
        return Err(DeepXOwnershipEvidenceError::InactiveSubaccount(
            profile.status.clone(),
        ));
    }

    Ok(DeepXAccountOwnershipProof {
        signer,
        wallet,
        subaccount,
        profile_height: profile.height,
        profile_created_at: profile.created_at,
    })
}

fn parse_account_id20(
    value: &str,
    field: &'static str,
) -> Result<[u8; 20], DeepXOwnershipEvidenceError> {
    decode_account_id20(value).ok_or(DeepXOwnershipEvidenceError::InvalidAddress { field })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const SIGNER: [u8; 20] = [0xaa; 20];

    fn evidence() -> (DeepXWalletSubaccounts, DeepXSubaccountProfile, String) {
        let wallet = format!("0x{}", hex::encode(SIGNER));
        let subaccount = format!("0x{}", hex::encode([0x11; 20]));
        let directory = DeepXWalletSubaccounts::new(
            wallet.clone(),
            vec![subaccount.clone(), format!("0x{}", hex::encode([0x22; 20]))],
        );
        let profile = DeepXSubaccountProfile {
            authority: wallet,
            address: subaccount.clone(),
            name: "test".to_string(),
            status: "Active".to_string(),
            spot_positions: Vec::new(),
            next_order_id: 1,
            spot_margin_trading_enabled: false,
            margin_strategy: "Cross".to_string(),
            height: 50_070_126,
            created_at: 1_779_848_876_302,
        };
        (directory, profile, subaccount)
    }

    fn delegated_evidence() -> (
        DeepXWalletSubaccounts,
        DeepXSubaccountProfile,
        DeepXWalletDelegateAccounts,
        DeepXDelegateWallets,
        String,
    ) {
        let (directory, mut profile, subaccount) = evidence();
        let wallet = format!("0x{}", hex::encode([0x33; 20]));
        let signer = format!("0x{}", hex::encode(SIGNER));
        profile.authority = wallet.clone();
        let directory = DeepXWalletSubaccounts::new(wallet.clone(), directory.addresses);
        let delegates = DeepXWalletDelegateAccounts::new(
            wallet.clone(),
            vec![DeepXDelegateAccount {
                delegate_address: signer.clone(),
                delegate_name: "order bot".to_string(),
                valid_until: 2_000,
                mode: DeepXDelegateMode::PlaceOrCancelOrder,
                create_time: 100,
                active: true,
            }],
        );
        let delegators = DeepXDelegateWallets::new(signer, vec![wallet]);
        (directory, profile, delegates, delegators, subaccount)
    }

    #[rstest]
    fn proof_binds_signer_directory_and_profile() {
        let (directory, profile, subaccount) = evidence();

        let proof =
            verify_account_ownership_evidence(SIGNER, &subaccount, &directory, &profile).unwrap();

        assert_eq!(proof.signer(), SIGNER);
        assert_eq!(proof.wallet(), SIGNER);
        assert_eq!(proof.subaccount(), [0x11; 20]);
        assert_eq!(proof.profile_height(), profile.height);
        assert_eq!(proof.profile_created_at(), profile.created_at);
    }

    #[rstest]
    fn delegated_ownership_requires_both_directories_and_order_permission() {
        let (directory, profile, mut delegates, mut delegators, subaccount) = delegated_evidence();
        let verify =
            |delegates: &DeepXWalletDelegateAccounts, delegators: &DeepXDelegateWallets, now_ms| {
                verify_delegated_account_ownership_evidence(
                    SIGNER,
                    &subaccount,
                    &directory,
                    &profile,
                    delegates,
                    delegators,
                    now_ms,
                )
            };

        let proof = verify(&delegates, &delegators, 1_999).unwrap();
        assert_eq!(proof.wallet(), [0x33; 20]);
        assert_eq!(proof.signer(), SIGNER);
        assert_eq!(proof.subaccount(), [0x11; 20]);

        assert!(matches!(
            verify(&delegates, &delegators, 2_000),
            Err(DeepXOwnershipEvidenceError::DelegateInactive),
        ));
        delegates.accounts[0].mode = DeepXDelegateMode::Disable;
        assert!(matches!(
            verify(&delegates, &delegators, 1_999),
            Err(DeepXOwnershipEvidenceError::DelegateModeUnsupported),
        ));
        delegates.accounts[0].mode = DeepXDelegateMode::PlaceOrCancelOrder;
        delegators.wallets.clear();
        assert!(matches!(
            verify(&delegates, &delegators, 1_999),
            Err(DeepXOwnershipEvidenceError::DelegatorUnavailable),
        ));
    }

    #[rstest]
    fn rejects_foreign_directory_wallet() {
        let (directory, profile, subaccount) = evidence();
        let directory = DeepXWalletSubaccounts::new(
            format!("0x{}", hex::encode([0x33; 20])),
            directory.addresses,
        );

        assert!(matches!(
            verify_account_ownership_evidence(SIGNER, &subaccount, &directory, &profile),
            Err(DeepXOwnershipEvidenceError::DirectoryWalletMismatch),
        ));
    }

    #[rstest]
    fn rejects_missing_and_duplicate_subaccounts() {
        let (directory, profile, subaccount) = evidence();
        let missing = DeepXWalletSubaccounts::new(
            directory.wallet().to_string(),
            vec![format!("0x{}", hex::encode([0x22; 20]))],
        );
        assert!(matches!(
            verify_account_ownership_evidence(SIGNER, &subaccount, &missing, &profile),
            Err(DeepXOwnershipEvidenceError::SubaccountNotOwned),
        ));

        let duplicate = DeepXWalletSubaccounts::new(
            directory.wallet().to_string(),
            vec![subaccount.clone(), subaccount.clone()],
        );
        assert!(matches!(
            verify_account_ownership_evidence(SIGNER, &subaccount, &duplicate, &profile),
            Err(DeepXOwnershipEvidenceError::DuplicateSubaccount),
        ));
    }

    #[rstest]
    fn rejects_profile_identity_and_status_mismatches() {
        let (directory, mut profile, subaccount) = evidence();
        profile.address = format!("0x{}", hex::encode([0x44; 20]));
        assert!(matches!(
            verify_account_ownership_evidence(SIGNER, &subaccount, &directory, &profile),
            Err(DeepXOwnershipEvidenceError::ProfileAddressMismatch),
        ));

        let (_, mut profile, _) = evidence();
        profile.authority = format!("0x{}", hex::encode([0x44; 20]));
        assert!(matches!(
            verify_account_ownership_evidence(SIGNER, &subaccount, &directory, &profile),
            Err(DeepXOwnershipEvidenceError::ProfileAuthorityMismatch),
        ));

        let (_, mut profile, _) = evidence();
        profile.status = "Closed".to_string();
        assert!(matches!(
            verify_account_ownership_evidence(SIGNER, &subaccount, &directory, &profile),
            Err(DeepXOwnershipEvidenceError::InactiveSubaccount(status)) if status == "Closed",
        ));
    }

    #[rstest]
    fn rejects_malformed_account_id20() {
        let (directory, profile, _) = evidence();

        assert!(matches!(
            verify_account_ownership_evidence(SIGNER, "subaccount", &directory, &profile),
            Err(DeepXOwnershipEvidenceError::InvalidAddress {
                field: "configured subaccount"
            }),
        ));
    }
}
