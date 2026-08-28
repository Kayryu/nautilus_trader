// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! DeepX credential parsing and identity validation.

use std::fmt::Debug;

use nautilus_core::{env::get_or_env_var_opt, string::secret::REDACTED};
use subxt_signer::eth::Keypair;
use zeroize::Zeroizing;

use crate::{common::enums::DeepXEnvironment, config::DeepXExecutionClientConfig};

/// Testnet private key environment variable.
pub const DEEPX_TESTNET_PRIVATE_KEY: &str = "DEEPX_TESTNET_PRIVATE_KEY";
/// Testnet wallet address environment variable.
pub const DEEPX_TESTNET_WALLET_ADDRESS: &str = "DEEPX_TESTNET_WALLET_ADDRESS";
/// Testnet subaccount address environment variable.
pub const DEEPX_TESTNET_SUBACCOUNT_ADDRESS: &str = "DEEPX_TESTNET_SUBACCOUNT_ADDRESS";

/// Returns the credential environment variables for a DeepX environment.
#[must_use]
pub const fn credential_env_vars(
    environment: DeepXEnvironment,
) -> (&'static str, &'static str, &'static str) {
    match environment {
        DeepXEnvironment::Testnet => (
            DEEPX_TESTNET_PRIVATE_KEY,
            DEEPX_TESTNET_WALLET_ADDRESS,
            DEEPX_TESTNET_SUBACCOUNT_ADDRESS,
        ),
    }
}

/// Locally validated DeepX signing credentials for one wallet and one subaccount.
#[derive(Clone)]
pub struct DeepXCredential {
    keypair: Keypair,
    wallet_address: String,
    subaccount: [u8; 20],
}

impl DeepXCredential {
    /// Resolves and validates credentials from an execution client configuration.
    ///
    /// Explicit configuration values take precedence over environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, malformed, or the configured wallet does not
    /// match the wallet derived from the private key.
    pub fn resolve(config: &DeepXExecutionClientConfig) -> anyhow::Result<Self> {
        let (private_key_var, wallet_var, subaccount_var) = credential_env_vars(config.environment);
        let private_key = Zeroizing::new(
            get_or_env_var_opt(config.private_key.clone(), private_key_var)
                .ok_or_else(|| anyhow::anyhow!("missing DeepX private key `{private_key_var}`"))?,
        );
        let configured_wallet = get_or_env_var_opt(config.wallet_address.clone(), wallet_var);
        let subaccount = get_or_env_var_opt(config.subaccount_address.clone(), subaccount_var)
            .ok_or_else(|| anyhow::anyhow!("missing DeepX subaccount `{subaccount_var}`"))?;

        Self::new(&private_key, configured_wallet.as_deref(), &subaccount)
    }

    /// Creates credentials from explicit values.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid private key, wallet address, subaccount, or wallet mismatch.
    pub fn new(
        private_key: &str,
        wallet_address: Option<&str>,
        subaccount_address: &str,
    ) -> anyhow::Result<Self> {
        let secret_key = decode_hex_array::<32>(private_key, "private key")?;
        let keypair = Keypair::from_secret_key(secret_key)
            .map_err(|error| anyhow::anyhow!("invalid DeepX private key: {error}"))?;
        let derived_wallet = account_id(&keypair);

        if let Some(wallet_address) = wallet_address {
            let configured_wallet = normalize_address(wallet_address, "wallet address")?;
            anyhow::ensure!(
                configured_wallet == derived_wallet,
                "configured DeepX wallet `{configured_wallet}` does not match derived wallet `{derived_wallet}`"
            );
        }

        Ok(Self {
            keypair,
            wallet_address: derived_wallet,
            subaccount: decode_hex_array(subaccount_address, "subaccount address")?,
        })
    }

    /// Returns the wallet address derived from the private key.
    #[must_use]
    pub fn wallet_address(&self) -> &str {
        &self.wallet_address
    }

    /// Returns the configured subaccount as raw address bytes.
    #[must_use]
    pub const fn subaccount(&self) -> &[u8; 20] {
        &self.subaccount
    }

    /// Returns the configured subaccount as normalized hexadecimal.
    #[must_use]
    pub fn subaccount_address(&self) -> String {
        format!("0x{}", hex::encode(self.subaccount))
    }

    pub(crate) const fn keypair(&self) -> &Keypair {
        &self.keypair
    }
}

impl Debug for DeepXCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXCredential))
            .field("wallet_address", &self.wallet_address)
            .field("subaccount_address", &self.subaccount_address())
            .field("keypair", &REDACTED)
            .finish()
    }
}

fn account_id(keypair: &Keypair) -> String {
    let account_id = keypair.public_key().to_account_id();
    format!("0x{}", hex::encode(account_id.0))
}

fn normalize_address(value: &str, field: &str) -> anyhow::Result<String> {
    Ok(format!(
        "0x{}",
        hex::encode(decode_hex_array::<20>(value, field)?)
    ))
}

fn decode_hex_array<const N: usize>(value: &str, field: &str) -> anyhow::Result<[u8; N]> {
    let bytes = hex::decode(value.strip_prefix("0x").unwrap_or(value))
        .map_err(|error| anyhow::anyhow!("invalid DeepX {field}: {error}"))?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        anyhow::anyhow!(
            "invalid DeepX {field}: expected {N} bytes, received {}",
            bytes.len()
        )
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const PRIVATE_KEY_ONE: &str =
        "0000000000000000000000000000000000000000000000000000000000000001";
    const WALLET_ONE: &str = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf";
    const SUBACCOUNT: &str = "0x1111111111111111111111111111111111111111";

    #[rstest]
    fn derives_and_validates_wallet_identity() {
        let credential =
            DeepXCredential::new(PRIVATE_KEY_ONE, Some(WALLET_ONE), SUBACCOUNT).unwrap();

        assert_eq!(credential.wallet_address(), WALLET_ONE);
        assert_eq!(credential.subaccount_address(), SUBACCOUNT);
    }

    #[rstest]
    fn rejects_wallet_mismatch() {
        let result = DeepXCredential::new(
            PRIVATE_KEY_ONE,
            Some("0x2222222222222222222222222222222222222222"),
            SUBACCOUNT,
        );

        assert!(result.unwrap_err().to_string().contains("does not match"));
    }

    #[rstest]
    fn rejects_invalid_subaccount() {
        let result = DeepXCredential::new(PRIVATE_KEY_ONE, None, "0x1234");

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expected 20 bytes")
        );
    }

    #[rstest]
    fn debug_redacts_keypair() {
        let credential = DeepXCredential::new(PRIVATE_KEY_ONE, None, SUBACCOUNT).unwrap();
        let debug = format!("{credential:?}");

        assert!(debug.contains(REDACTED));
        assert!(!debug.contains(PRIVATE_KEY_ONE));
    }
}
