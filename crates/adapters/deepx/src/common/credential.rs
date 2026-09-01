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

//! DeepX private-key storage and environment resolution.

use std::{
    fmt::{Debug, Display, Formatter},
    str::FromStr,
};

use alloy::signers::local::PrivateKeySigner;
use nautilus_core::hex;
use zeroize::{Zeroize, ZeroizeOnDrop};

use super::{
    enums::DeepXEnvironment,
    error::{DeepXError, Result},
};

const DEEPX_TESTNET_PRIVATE_KEY_VAR: &str = "DEEPX_TESTNET_PRIVATE_KEY";

/// DeepX transaction-signing key scheme.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeepXKeyScheme {
    /// ECDSA over the secp256k1 curve.
    Secp256k1,
    /// Unrecognized key scheme retained for explicit rejection.
    Unknown(String),
}

impl Display for DeepXKeyScheme {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Secp256k1 => f.write_str("secp256k1"),
            Self::Unknown(value) => f.write_str(value),
        }
    }
}

/// Returns the private-key environment variable for a supported deployment.
///
/// # Errors
///
/// Returns an error when the environment is not the verified DeepX testnet.
pub fn credential_env_var(environment: &DeepXEnvironment) -> Result<&'static str> {
    match environment {
        DeepXEnvironment::Testnet => Ok(DEEPX_TESTNET_PRIVATE_KEY_VAR),
        environment => Err(DeepXError::UnsupportedEnvironment(environment.to_string())),
    }
}

/// Secure wrapper for a DeepX secp256k1 private key.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct DeepXPrivateKey {
    bytes: [u8; 32],
}

impl DeepXPrivateKey {
    /// Creates a private key for the selected signing scheme.
    ///
    /// Accepts a 32-byte hexadecimal scalar with an optional `0x` prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the scheme is unsupported or the key is not a valid
    /// secp256k1 private scalar.
    pub fn new(value: &str, scheme: &DeepXKeyScheme) -> Result<Self> {
        if let DeepXKeyScheme::Unknown(value) = scheme {
            return Err(DeepXError::UnsupportedKeyScheme(value.clone()));
        }

        let value = value.trim();
        let hex_value = value.strip_prefix("0x").unwrap_or(value);
        let bytes = hex::decode_array::<32>(hex_value).map_err(|_| {
            DeepXError::InvalidCredential(
                "private key must be exactly 32 bytes of hexadecimal".to_string(),
            )
        })?;

        let normalized = hex::encode_prefixed(bytes);
        PrivateKeySigner::from_str(&normalized).map_err(|_| {
            DeepXError::InvalidCredential(
                "private key must be a valid non-zero secp256k1 scalar".to_string(),
            )
        })?;

        Ok(Self { bytes })
    }

    /// Loads a private key from the environment for the selected deployment.
    ///
    /// # Errors
    ///
    /// Returns an error when the environment is unsupported, the variable is
    /// unavailable, or the value is not a valid private key.
    pub fn from_env(environment: &DeepXEnvironment, scheme: &DeepXKeyScheme) -> Result<Self> {
        Self::from_env_with(environment, scheme, |variable| std::env::var(variable))
    }

    fn from_env_with<F>(
        environment: &DeepXEnvironment,
        scheme: &DeepXKeyScheme,
        resolver: F,
    ) -> Result<Self>
    where
        F: FnOnce(&str) -> std::result::Result<String, std::env::VarError>,
    {
        let variable = credential_env_var(environment)?;
        let value = resolver(variable).map_err(|_| DeepXError::MissingCredential(variable))?;
        Self::new(&value, scheme)
    }

    /// Returns the decoded private-key bytes for controlled signing operations.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl Debug for DeepXPrivateKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeepXPrivateKey(**redacted**)")
    }
}

impl Display for DeepXPrivateKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeepXPrivateKey(**redacted**)")
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const VALID_PRIVATE_KEY: &str =
        "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[rstest]
    #[case(VALID_PRIVATE_KEY)]
    #[case("0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF")]
    fn valid_secp256k1_keys_are_accepted(#[case] value: &str) {
        let key = DeepXPrivateKey::new(value, &DeepXKeyScheme::Secp256k1).unwrap();

        assert_eq!(key.as_bytes().len(), 32);
    }

    #[rstest]
    #[case("")]
    #[case("0x01")]
    #[case("zz23456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")]
    fn malformed_keys_are_rejected_without_echoing_input(#[case] value: &str) {
        let error = DeepXPrivateKey::new(value, &DeepXKeyScheme::Secp256k1).unwrap_err();

        assert!(!error.to_string().contains(value) || value.is_empty());
    }

    #[rstest]
    fn zero_scalar_is_rejected() {
        let error = DeepXPrivateKey::new(
            "0000000000000000000000000000000000000000000000000000000000000000",
            &DeepXKeyScheme::Secp256k1,
        );

        assert!(matches!(error, Err(DeepXError::InvalidCredential(_))));
    }

    #[rstest]
    fn unsupported_key_scheme_is_rejected() {
        let error = DeepXPrivateKey::new(
            VALID_PRIVATE_KEY,
            &DeepXKeyScheme::Unknown("ed25519".to_string()),
        );

        assert!(matches!(
            error,
            Err(DeepXError::UnsupportedKeyScheme(value)) if value == "ed25519",
        ));
    }

    #[rstest]
    fn formatting_is_redacted() {
        let key = DeepXPrivateKey::new(VALID_PRIVATE_KEY, &DeepXKeyScheme::Secp256k1).unwrap();

        assert_eq!(format!("{key:?}"), "DeepXPrivateKey(**redacted**)");
        assert_eq!(key.to_string(), "DeepXPrivateKey(**redacted**)");
        assert!(!format!("{key:?}").contains("0123456789abcdef"));
    }

    #[rstest]
    fn environment_names_are_testnet_only() {
        assert_eq!(
            credential_env_var(&DeepXEnvironment::Testnet).unwrap(),
            DEEPX_TESTNET_PRIVATE_KEY_VAR,
        );
        assert!(credential_env_var(&DeepXEnvironment::Mainnet).is_err());
        assert!(credential_env_var(&DeepXEnvironment::Unknown("staging".to_string())).is_err());
    }

    #[rstest]
    fn environment_loading_resolves_and_validates_the_key() {
        let key = DeepXPrivateKey::from_env_with(
            &DeepXEnvironment::Testnet,
            &DeepXKeyScheme::Secp256k1,
            |variable| {
                assert_eq!(variable, DEEPX_TESTNET_PRIVATE_KEY_VAR);
                Ok(VALID_PRIVATE_KEY.to_string())
            },
        )
        .unwrap();

        assert_eq!(key.as_bytes().len(), 32);
    }

    #[rstest]
    fn missing_environment_variable_is_typed() {
        let result = DeepXPrivateKey::from_env_with(
            &DeepXEnvironment::Testnet,
            &DeepXKeyScheme::Secp256k1,
            |_| Err(std::env::VarError::NotPresent),
        );

        assert_eq!(
            result.unwrap_err(),
            DeepXError::MissingCredential(DEEPX_TESTNET_PRIVATE_KEY_VAR),
        );
    }
}
