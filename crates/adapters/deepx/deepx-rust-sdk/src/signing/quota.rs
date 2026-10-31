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

//! Offline EIP-191 signing for a DeepX wallet quota claim.

use std::fmt::{Debug, Formatter};

use alloy::signers::{SignerSync, local::PrivateKeySigner};
use serde::Serialize;
use thiserror::Error;

use crate::credential::DeepXPrivateKey;

/// Exact signed fields for the documented quota-claim request.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepXQuotaClaimSignature {
    /// Exact wallet address used in the signed message.
    wallet: String,
    /// Caller-owned retry key, retained byte-for-byte.
    idempotency_key: String,
    /// Exact three-line EIP-191 message.
    message: String,
    /// Hex-encoded 65-byte personal-sign signature.
    signature: String,
}

impl DeepXQuotaClaimSignature {
    /// Returns the wallet address exactly as signed.
    #[must_use]
    pub fn wallet(&self) -> &str {
        &self.wallet
    }

    /// Returns the caller-owned retry key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Returns the exact signed message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the EIP-191 signature.
    #[must_use]
    pub fn signature(&self) -> &str {
        &self.signature
    }
}

impl Debug for DeepXQuotaClaimSignature {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeepXQuotaClaimSignature")
            .field("wallet", &self.wallet)
            .field("idempotency_key", &"**redacted**")
            .field("message", &"**redacted**")
            .field("signature", &"**redacted**")
            .finish()
    }
}

/// Local quota-claim signing failure.
#[derive(Debug, Error)]
pub enum DeepXQuotaClaimSigningError {
    /// The wallet is not a 20-byte hexadecimal address.
    #[error("quota claim wallet must be a 0x-prefixed 20-byte address")]
    InvalidWallet,
    /// The retry key cannot form an unambiguous three-line message.
    #[error("quota claim idempotency key must contain 1 to 160 bytes without control characters")]
    InvalidIdempotencyKey,
    /// The private key could not produce an EIP-191 signature.
    #[error("quota claim personal signing failed")]
    SigningFailed,
}

/// Signs a quota-claim request offline with a caller-supplied key.
///
/// This only creates request fields. The caller owns durable idempotency and must establish
/// signer authorization independently; no claim is sent or treated as granted.
///
/// # Errors
///
/// Returns an error for malformed wallet or retry-key fields, or a signing failure.
pub fn sign_quota_claim(
    key: &DeepXPrivateKey,
    wallet: &str,
    idempotency_key: &str,
) -> Result<DeepXQuotaClaimSignature, DeepXQuotaClaimSigningError> {
    let wallet_hex = wallet
        .strip_prefix("0x")
        .ok_or(DeepXQuotaClaimSigningError::InvalidWallet)?;
    let mut wallet_bytes = [0_u8; 20];
    hex::decode_to_slice(wallet_hex, &mut wallet_bytes)
        .map_err(|_| DeepXQuotaClaimSigningError::InvalidWallet)?;
    if idempotency_key.is_empty()
        || idempotency_key.len() > 160
        || idempotency_key.chars().any(char::is_control)
    {
        return Err(DeepXQuotaClaimSigningError::InvalidIdempotencyKey);
    }

    let message =
        format!("DeepX quota claim\nWallet: {wallet}\nIdempotency-Key: {idempotency_key}");
    let signer = PrivateKeySigner::from_slice(key.as_bytes())
        .map_err(|_| DeepXQuotaClaimSigningError::SigningFailed)?;
    let signature = signer
        .sign_message_sync(message.as_bytes())
        .map_err(|_| DeepXQuotaClaimSigningError::SigningFailed)?;

    Ok(DeepXQuotaClaimSignature {
        wallet: wallet.to_string(),
        idempotency_key: idempotency_key.to_string(),
        message,
        signature: format!("0x{}", hex::encode(signature.as_bytes())),
    })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Signature, eip191_hash_message};
    use rstest::rstest;

    use super::*;
    use crate::credential::DeepXKeyScheme;

    fn test_key() -> DeepXPrivateKey {
        DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap()
    }

    #[rstest]
    fn signs_exact_documented_message_with_independent_vector() {
        let signed = sign_quota_claim(
            &test_key(),
            "0xfcad0b19bb29d4674531d6f115237e16afce377c",
            "018f52bc-0f0b-7cc4-b356-a91f62c72e3f",
        )
        .unwrap();
        assert_eq!(
            signed.wallet(),
            "0xfcad0b19bb29d4674531d6f115237e16afce377c"
        );
        assert_eq!(
            signed.message(),
            "DeepX quota claim\nWallet: 0xfcad0b19bb29d4674531d6f115237e16afce377c\nIdempotency-Key: 018f52bc-0f0b-7cc4-b356-a91f62c72e3f"
        );
        assert_eq!(
            signed.signature(),
            "0xa4e69e7f62d53e212c46c0e7cce04d4523f8dc377a7f019c613357fb856d12632271e1665d1da5d463663d7295353570ed8347a450b39b866cccb0f56d79df171c"
        );
        let payload = serde_json::to_value(&signed).unwrap();
        assert_eq!(payload["wallet"], signed.wallet());
        assert_eq!(payload["idempotencyKey"], signed.idempotency_key());
        assert_eq!(payload["message"], signed.message());
        assert_eq!(payload["signature"], signed.signature());
        assert_eq!(payload.as_object().unwrap().len(), 4);
        let signature = Signature::try_from(
            hex::decode(signed.signature().trim_start_matches("0x"))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let recovered = signature
            .recover_address_from_prehash(&eip191_hash_message(signed.message().as_bytes()))
            .unwrap();
        assert_eq!(
            recovered.as_slice(),
            hex::decode(&signed.wallet()[2..]).unwrap()
        );
    }

    #[rstest]
    fn preserves_explicit_wallet_case_in_signed_message() {
        let wallet = "0xFCAD0B19BB29D4674531D6F115237E16AFCE377C";
        let signed = sign_quota_claim(&test_key(), wallet, "claim-1").unwrap();
        assert_eq!(signed.wallet(), wallet);
        assert_eq!(
            signed.message(),
            "DeepX quota claim\nWallet: 0xFCAD0B19BB29D4674531D6F115237E16AFCE377C\nIdempotency-Key: claim-1"
        );
        let signature = Signature::try_from(
            hex::decode(signed.signature().trim_start_matches("0x"))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let recovered = signature
            .recover_address_from_prehash(&eip191_hash_message(signed.message().as_bytes()))
            .unwrap();
        assert_eq!(
            recovered.as_slice(),
            hex::decode(&wallet[2..]).unwrap().as_slice()
        );
    }

    #[rstest]
    fn signs_wallet_claim_with_distinct_signer_using_independent_vector() {
        let signed = sign_quota_claim(
            &test_key(),
            "0x781ed35b167068c93dfadab41dfb680edaca4e50",
            "delegate-retry-key",
        )
        .unwrap();
        assert_eq!(
            signed.message(),
            "DeepX quota claim\nWallet: 0x781ed35b167068c93dfadab41dfb680edaca4e50\nIdempotency-Key: delegate-retry-key"
        );
        assert_eq!(
            signed.signature(),
            "0xfd7d3b438c0148080c86bb05aef1d60419d70d3250f137438b8109ef00c5ec3363a85a2ff55d291eccf0a080545adfed56acc9bdc7bb62fdfb06ecb7ab4e776c1b"
        );
        assert_eq!(
            hex::encode(eip191_hash_message(signed.message().as_bytes())),
            "6202df1d5ee9898e4e1432bb5c40f6a91285e459482f85e4e4ca1013c143b34f"
        );
        let signature = Signature::try_from(
            hex::decode(signed.signature().trim_start_matches("0x"))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let recovered = signature
            .recover_address_from_prehash(&eip191_hash_message(signed.message().as_bytes()))
            .unwrap();
        assert_eq!(
            hex::encode(recovered),
            "fcad0b19bb29d4674531d6f115237e16afce377c"
        );
        assert_ne!(
            recovered.as_slice(),
            hex::decode(&signed.wallet()[2..]).unwrap().as_slice()
        );
    }

    #[rstest]
    fn debug_redacts_replayable_claim_fields() {
        let signed = sign_quota_claim(
            &test_key(),
            "0xfcad0b19bb29d4674531d6f115237e16afce377c",
            "retry-key",
        )
        .unwrap();
        let debug = format!("{signed:?}");
        assert!(debug.contains(signed.wallet()));
        assert!(!debug.contains(signed.signature()));
        assert!(!debug.contains(signed.idempotency_key()));
        assert!(!debug.contains(signed.message()));
    }

    #[rstest]
    #[case("", DeepXQuotaClaimSigningError::InvalidIdempotencyKey)]
    #[case("line\nextra", DeepXQuotaClaimSigningError::InvalidIdempotencyKey)]
    #[case("line\rbreak", DeepXQuotaClaimSigningError::InvalidIdempotencyKey)]
    fn rejects_invalid_idempotency_key(
        #[case] value: &str,
        #[case] expected: DeepXQuotaClaimSigningError,
    ) {
        assert_eq!(
            sign_quota_claim(
                &test_key(),
                "0xfcad0b19bb29d4674531d6f115237e16afce377c",
                value
            )
            .unwrap_err()
            .to_string(),
            expected.to_string()
        );
    }

    #[rstest]
    fn rejects_oversized_key_and_invalid_wallet() {
        assert!(matches!(
            sign_quota_claim(
                &test_key(),
                "0xfcad0b19bb29d4674531d6f115237e16afce377c",
                &"x".repeat(161),
            ),
            Err(DeepXQuotaClaimSigningError::InvalidIdempotencyKey)
        ));
        assert!(matches!(
            sign_quota_claim(&test_key(), "0x1234", "retry-key"),
            Err(DeepXQuotaClaimSigningError::InvalidWallet)
        ));
    }
}
