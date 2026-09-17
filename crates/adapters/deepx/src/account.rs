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

//! Account ownership validation across signing and REST identity evidence.

use std::collections::HashSet;

use nautilus_core::hex;
use thiserror::Error;

use crate::{
    common::DeepXPrivateKey,
    http::{DeepXHttpError, DeepXSubaccountProfile, DeepXWalletSubaccounts},
    signing::{SigningError, derive_signer_account_id},
};

/// Errors raised while proving signer ownership of a configured subaccount.
#[derive(Debug, Error)]
pub enum DeepXAccountOwnershipError {
    /// The authoritative REST identity query failed.
    #[error(transparent)]
    Http(#[from] DeepXHttpError),
    /// The configured signing identity could not be derived.
    #[error(transparent)]
    Signing(#[from] SigningError),
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
}

/// Complete REST-backed ownership evidence for one signer and configured subaccount.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXAccountOwnershipProof {
    signer: [u8; 20],
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

/// Verifies that REST directory and profile evidence bind a configured subaccount to a signer.
///
/// This proves consistency of the supplied point-in-time REST observations with the locally held
/// private key. It does not prove freshness, private-stream authentication, or trading authority.
///
/// # Errors
///
/// Returns an error for an invalid signing key or address, duplicate directory entries, an absent
/// configured subaccount, signer/profile mismatches, or an inactive profile.
pub(crate) fn verify_account_ownership(
    key: &DeepXPrivateKey,
    configured_subaccount: &str,
    directory: &DeepXWalletSubaccounts,
    profile: &DeepXSubaccountProfile,
) -> Result<DeepXAccountOwnershipProof, DeepXAccountOwnershipError> {
    let signer = derive_signer_account_id(key)?;
    let subaccount = parse_account_id20(configured_subaccount, "configured subaccount")?;
    let directory_wallet = parse_account_id20(directory.wallet(), "account directory wallet")?;
    if directory_wallet != signer {
        return Err(DeepXAccountOwnershipError::DirectoryWalletMismatch);
    }

    let mut observed = HashSet::new();
    let mut owns_subaccount = false;
    for address in &directory.addresses {
        let address = parse_account_id20(address, "account directory subaccount")?;
        if !observed.insert(address) {
            return Err(DeepXAccountOwnershipError::DuplicateSubaccount);
        }
        owns_subaccount |= address == subaccount;
    }
    if !owns_subaccount {
        return Err(DeepXAccountOwnershipError::SubaccountNotOwned);
    }

    if parse_account_id20(&profile.address, "profile subaccount")? != subaccount {
        return Err(DeepXAccountOwnershipError::ProfileAddressMismatch);
    }
    if parse_account_id20(&profile.authority, "profile authority")? != signer {
        return Err(DeepXAccountOwnershipError::ProfileAuthorityMismatch);
    }
    if profile.status != "Active" {
        return Err(DeepXAccountOwnershipError::InactiveSubaccount(
            profile.status.clone(),
        ));
    }

    Ok(DeepXAccountOwnershipProof {
        signer,
        subaccount,
        profile_height: profile.height,
        profile_created_at: profile.created_at,
    })
}

fn parse_account_id20(
    value: &str,
    field: &'static str,
) -> Result<[u8; 20], DeepXAccountOwnershipError> {
    let value = value
        .strip_prefix("0x")
        .ok_or(DeepXAccountOwnershipError::InvalidAddress { field })?;
    hex::decode_array::<20>(value).map_err(|_| DeepXAccountOwnershipError::InvalidAddress { field })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{common::DeepXKeyScheme, http::DeepXSubaccountProfile};

    const PRIVATE_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn key() -> DeepXPrivateKey {
        DeepXPrivateKey::new(PRIVATE_KEY, &DeepXKeyScheme::Secp256k1).unwrap()
    }

    fn evidence() -> (DeepXWalletSubaccounts, DeepXSubaccountProfile, String) {
        let signer = derive_signer_account_id(&key()).unwrap();
        let wallet = format!("0x{}", hex::encode(signer));
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

    #[rstest]
    fn ownership_proof_binds_signer_directory_and_profile() {
        let (directory, profile, subaccount) = evidence();

        let proof = verify_account_ownership(&key(), &subaccount, &directory, &profile).unwrap();

        assert_eq!(proof.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(proof.subaccount(), [0x11; 20]);
        assert_eq!(proof.profile_height(), profile.height);
        assert_eq!(proof.profile_created_at(), profile.created_at);
    }

    #[rstest]
    fn ownership_proof_rejects_foreign_directory_wallet() {
        let (directory, profile, subaccount) = evidence();
        let directory = DeepXWalletSubaccounts::new(
            format!("0x{}", hex::encode([0x33; 20])),
            directory.addresses,
        );

        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &directory, &profile),
            Err(DeepXAccountOwnershipError::DirectoryWalletMismatch),
        ));
    }

    #[rstest]
    fn ownership_proof_rejects_missing_and_duplicate_subaccounts() {
        let (directory, profile, subaccount) = evidence();
        let missing = DeepXWalletSubaccounts::new(
            directory.wallet().to_string(),
            vec![format!("0x{}", hex::encode([0x22; 20]))],
        );
        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &missing, &profile),
            Err(DeepXAccountOwnershipError::SubaccountNotOwned),
        ));

        let duplicate = DeepXWalletSubaccounts::new(
            directory.wallet().to_string(),
            vec![subaccount.clone(), subaccount.clone()],
        );
        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &duplicate, &profile),
            Err(DeepXAccountOwnershipError::DuplicateSubaccount),
        ));
    }

    #[rstest]
    fn ownership_proof_rejects_profile_identity_and_status_mismatches() {
        let (directory, mut profile, subaccount) = evidence();
        profile.address = format!("0x{}", hex::encode([0x44; 20]));
        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &directory, &profile),
            Err(DeepXAccountOwnershipError::ProfileAddressMismatch),
        ));

        let (_, mut profile, _) = evidence();
        profile.authority = format!("0x{}", hex::encode([0x44; 20]));
        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &directory, &profile),
            Err(DeepXAccountOwnershipError::ProfileAuthorityMismatch),
        ));

        let (_, mut profile, _) = evidence();
        profile.status = "Closed".to_string();
        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &directory, &profile),
            Err(DeepXAccountOwnershipError::InactiveSubaccount(status)) if status == "Closed",
        ));
    }

    #[rstest]
    fn ownership_proof_rejects_malformed_account_id20() {
        let (directory, profile, _) = evidence();

        assert!(matches!(
            verify_account_ownership(&key(), "subaccount", &directory, &profile),
            Err(DeepXAccountOwnershipError::InvalidAddress {
                field: "configured subaccount"
            }),
        ));
    }
}
