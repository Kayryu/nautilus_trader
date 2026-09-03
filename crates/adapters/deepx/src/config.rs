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

//! Configuration for DeepX network access.

use std::fmt::{Debug, Formatter};

use nautilus_core::hex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::common::{DeepXEnvironment, Result, consts::DEEPX_TESTNET_GENESIS_HASH, urls};

const REDACTED: &str = "<redacted>";

/// Role assigned to a DeepX Substrate JSON-RPC endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXRpcRole {
    /// Submits signed transaction bytes.
    Submission,
    /// Observes best and finalized heads and transaction inclusion.
    Watch,
    /// Performs bounded canonical recovery scans and pool checks.
    Recovery,
}

/// Identity observed directly from one configured DeepX JSON-RPC endpoint.
#[derive(Clone, PartialEq, Eq)]
pub struct DeepXObservedRpcEndpoint {
    role: DeepXRpcRole,
    url: String,
    genesis_hash: [u8; 32],
}

impl Debug for DeepXObservedRpcEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXObservedRpcEndpoint))
            .field("role", &self.role)
            .field("url", &REDACTED)
            .field("genesis_hash", &self.genesis_hash)
            .finish()
    }
}

impl DeepXObservedRpcEndpoint {
    /// Creates identity evidence returned by one endpoint assigned to `role`.
    #[must_use]
    pub fn new(role: DeepXRpcRole, url: String, genesis_hash: [u8; 32]) -> Self {
        Self {
            role,
            url,
            genesis_hash,
        }
    }

    /// Returns the role assigned to the observed endpoint.
    #[must_use]
    pub const fn role(&self) -> DeepXRpcRole {
        self.role
    }
}

/// Errors raised when RPC endpoint identity evidence is incomplete or inconsistent.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXRpcEndpointValidationError {
    /// The built-in approved testnet genesis hash is invalid.
    #[error("invalid built-in DeepX testnet genesis hash")]
    InvalidApprovedGenesisHash,
    /// No identity evidence was supplied for a configured role.
    #[error("missing DeepX RPC endpoint identity for role {0:?}")]
    MissingRole(DeepXRpcRole),
    /// More than one identity observation was supplied for a role.
    #[error("duplicate DeepX RPC endpoint identity for role {0:?}")]
    DuplicateRole(DeepXRpcRole),
    /// The observation did not identify the endpoint selected by the configuration.
    #[error("DeepX RPC endpoint URL does not match configured role {0:?}")]
    UrlMismatch(DeepXRpcRole),
    /// The endpoint belongs to a chain other than the approved DeepX testnet.
    #[error("DeepX RPC endpoint genesis hash is not approved for role {0:?}")]
    GenesisHashMismatch(DeepXRpcRole),
    /// The network configuration itself is unsupported.
    #[error(transparent)]
    Configuration(#[from] crate::common::DeepXError),
}

/// Complete identity-validated endpoint selection for every DeepX RPC role.
#[derive(Clone, PartialEq, Eq)]
pub struct DeepXValidatedRpcEndpoints {
    submission_url: String,
    watch_url: String,
    recovery_url: String,
    genesis_hash: [u8; 32],
}

impl Debug for DeepXValidatedRpcEndpoints {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXValidatedRpcEndpoints))
            .field("submission_url", &REDACTED)
            .field("watch_url", &REDACTED)
            .field("recovery_url", &REDACTED)
            .field("genesis_hash", &self.genesis_hash)
            .finish()
    }
}

impl DeepXValidatedRpcEndpoints {
    /// Returns the identity-validated URL assigned to `role`.
    #[must_use]
    pub fn url_for(&self, role: DeepXRpcRole) -> &str {
        match role {
            DeepXRpcRole::Submission => &self.submission_url,
            DeepXRpcRole::Watch => &self.watch_url,
            DeepXRpcRole::Recovery => &self.recovery_url,
        }
    }

    /// Returns the approved genesis hash observed from every role endpoint.
    #[must_use]
    pub const fn genesis_hash(&self) -> [u8; 32] {
        self.genesis_hash
    }
}

/// Read-only DeepX network configuration.
#[derive(Clone, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeepXNetworkConfig {
    /// DeepX deployment environment.
    pub environment: DeepXEnvironment,
    /// Optional REST API base URL override.
    pub base_url_rest: Option<String>,
    /// Optional WebSocket API URL override.
    pub base_url_ws: Option<String>,
    /// Optional Substrate JSON-RPC URL override.
    pub base_url_rpc: Option<String>,
    /// Optional transaction-submission JSON-RPC URL override.
    pub base_url_rpc_submission: Option<String>,
    /// Optional best/finalized-head watch JSON-RPC URL override.
    pub base_url_rpc_watch: Option<String>,
    /// Optional recovery-scan JSON-RPC URL override.
    pub base_url_rpc_recovery: Option<String>,
}

impl Debug for DeepXNetworkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXNetworkConfig))
            .field("environment", &self.environment)
            .field(
                "base_url_rest",
                &self.base_url_rest.as_ref().map(|_| REDACTED),
            )
            .field("base_url_ws", &self.base_url_ws.as_ref().map(|_| REDACTED))
            .field(
                "base_url_rpc",
                &self.base_url_rpc.as_ref().map(|_| REDACTED),
            )
            .field(
                "base_url_rpc_submission",
                &self.base_url_rpc_submission.as_ref().map(|_| REDACTED),
            )
            .field(
                "base_url_rpc_watch",
                &self.base_url_rpc_watch.as_ref().map(|_| REDACTED),
            )
            .field(
                "base_url_rpc_recovery",
                &self.base_url_rpc_recovery.as_ref().map(|_| REDACTED),
            )
            .finish()
    }
}

impl DeepXNetworkConfig {
    /// Validates that this configuration targets the supported deployment.
    pub fn validate(&self) -> Result<()> {
        urls::rest_url(&self.environment)?;
        Ok(())
    }

    /// Returns the configured REST API URL.
    pub fn rest_url(&self) -> Result<String> {
        self.validate()?;
        match &self.base_url_rest {
            Some(url) => Ok(url.clone()),
            None => Ok(urls::rest_url(&self.environment)?.to_string()),
        }
    }

    /// Returns the configured WebSocket API URL.
    pub fn ws_url(&self) -> Result<String> {
        self.validate()?;
        match &self.base_url_ws {
            Some(url) => Ok(url.clone()),
            None => Ok(urls::ws_url(&self.environment)?.to_string()),
        }
    }

    /// Returns the configured Substrate JSON-RPC URL.
    pub fn rpc_url(&self) -> Result<String> {
        self.validate()?;
        match &self.base_url_rpc {
            Some(url) => Ok(url.clone()),
            None => Ok(urls::rpc_url(&self.environment)?.to_string()),
        }
    }

    /// Returns the configured Substrate JSON-RPC URL for `role`.
    pub fn rpc_url_for(&self, role: DeepXRpcRole) -> Result<String> {
        self.validate()?;
        let role_override = match role {
            DeepXRpcRole::Submission => &self.base_url_rpc_submission,
            DeepXRpcRole::Watch => &self.base_url_rpc_watch,
            DeepXRpcRole::Recovery => &self.base_url_rpc_recovery,
        };
        match role_override {
            Some(url) => Ok(url.clone()),
            None => self.rpc_url(),
        }
    }
}

/// Validates complete chain-identity evidence for all configured RPC roles.
///
/// This function performs no network I/O. Callers must obtain each genesis hash directly from the
/// endpoint URL selected for that role. A result proves only endpoint selection and chain identity;
/// it does not prove support for role-specific RPC methods.
///
/// # Errors
///
/// Returns an error unless every role appears exactly once with its configured URL and the approved
/// DeepX testnet genesis hash.
pub fn validate_rpc_endpoint_identities(
    config: &DeepXNetworkConfig,
    observations: impl IntoIterator<Item = DeepXObservedRpcEndpoint>,
) -> std::result::Result<DeepXValidatedRpcEndpoints, DeepXRpcEndpointValidationError> {
    let approved_genesis_hash = hex::decode_array::<32>(
        DEEPX_TESTNET_GENESIS_HASH
            .strip_prefix("0x")
            .unwrap_or(DEEPX_TESTNET_GENESIS_HASH),
    )
    .map_err(|_| DeepXRpcEndpointValidationError::InvalidApprovedGenesisHash)?;
    let mut submission_url = None;
    let mut watch_url = None;
    let mut recovery_url = None;

    for observation in observations {
        let expected_url = config.rpc_url_for(observation.role)?;
        if observation.url != expected_url {
            return Err(DeepXRpcEndpointValidationError::UrlMismatch(
                observation.role,
            ));
        }
        if observation.genesis_hash != approved_genesis_hash {
            return Err(DeepXRpcEndpointValidationError::GenesisHashMismatch(
                observation.role,
            ));
        }
        let role_url = match observation.role {
            DeepXRpcRole::Submission => &mut submission_url,
            DeepXRpcRole::Watch => &mut watch_url,
            DeepXRpcRole::Recovery => &mut recovery_url,
        };
        if role_url.replace(observation.url).is_some() {
            return Err(DeepXRpcEndpointValidationError::DuplicateRole(
                observation.role,
            ));
        }
    }

    Ok(DeepXValidatedRpcEndpoints {
        submission_url: submission_url.ok_or(DeepXRpcEndpointValidationError::MissingRole(
            DeepXRpcRole::Submission,
        ))?,
        watch_url: watch_url.ok_or(DeepXRpcEndpointValidationError::MissingRole(
            DeepXRpcRole::Watch,
        ))?,
        recovery_url: recovery_url.ok_or(DeepXRpcEndpointValidationError::MissingRole(
            DeepXRpcRole::Recovery,
        ))?,
        genesis_hash: approved_genesis_hash,
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::{
        DeepXError,
        consts::{
            DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL,
            DEEPX_TESTNET_WS_URL,
        },
    };

    fn approved_genesis_hash() -> [u8; 32] {
        hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.strip_prefix("0x").unwrap()).unwrap()
    }

    fn observations(config: &DeepXNetworkConfig) -> [DeepXObservedRpcEndpoint; 3] {
        [
            DeepXObservedRpcEndpoint::new(
                DeepXRpcRole::Submission,
                config.rpc_url_for(DeepXRpcRole::Submission).unwrap(),
                approved_genesis_hash(),
            ),
            DeepXObservedRpcEndpoint::new(
                DeepXRpcRole::Watch,
                config.rpc_url_for(DeepXRpcRole::Watch).unwrap(),
                approved_genesis_hash(),
            ),
            DeepXObservedRpcEndpoint::new(
                DeepXRpcRole::Recovery,
                config.rpc_url_for(DeepXRpcRole::Recovery).unwrap(),
                approved_genesis_hash(),
            ),
        ]
    }

    #[rstest]
    fn defaults_target_verified_testnet() {
        let config = DeepXNetworkConfig::default();

        assert_eq!(config.rest_url().unwrap(), DEEPX_TESTNET_REST_URL);
        assert_eq!(config.ws_url().unwrap(), DEEPX_TESTNET_WS_URL);
        assert_eq!(config.rpc_url().unwrap(), DEEPX_TESTNET_RPC_URL);
        assert_eq!(
            config.rpc_url_for(DeepXRpcRole::Submission).unwrap(),
            DEEPX_TESTNET_RPC_URL,
        );
        assert_eq!(
            config.rpc_url_for(DeepXRpcRole::Watch).unwrap(),
            DEEPX_TESTNET_RPC_URL,
        );
        assert_eq!(
            config.rpc_url_for(DeepXRpcRole::Recovery).unwrap(),
            DEEPX_TESTNET_RPC_URL,
        );
    }

    #[rstest]
    fn mainnet_is_rejected_before_url_resolution() {
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Mainnet,
            base_url_rest: Some("https://example.invalid".to_string()),
            ..Default::default()
        };

        assert_eq!(
            config.rest_url(),
            Err(DeepXError::UnsupportedEnvironment("mainnet".to_string())),
        );
    }

    #[rstest]
    fn unknown_fields_are_rejected() {
        let result = serde_json::from_str::<DeepXNetworkConfig>(
            r#"{"environment":"testnet","unsupported":true}"#,
        );

        assert!(result.is_err());
    }

    #[rstest]
    fn rpc_roles_support_independent_endpoint_overrides() {
        let config = DeepXNetworkConfig {
            base_url_rpc: Some("https://common.example.invalid".to_string()),
            base_url_rpc_submission: Some("https://submit.example.invalid".to_string()),
            base_url_rpc_watch: Some("https://watch.example.invalid".to_string()),
            base_url_rpc_recovery: Some("https://recovery.example.invalid".to_string()),
            ..Default::default()
        };

        assert_eq!(
            config.rpc_url_for(DeepXRpcRole::Submission).unwrap(),
            "https://submit.example.invalid",
        );
        assert_eq!(
            config.rpc_url_for(DeepXRpcRole::Watch).unwrap(),
            "https://watch.example.invalid",
        );
        assert_eq!(
            config.rpc_url_for(DeepXRpcRole::Recovery).unwrap(),
            "https://recovery.example.invalid",
        );
    }

    #[rstest]
    fn rpc_role_override_does_not_bypass_testnet_validation() {
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Mainnet,
            base_url_rpc_submission: Some("https://example.invalid".to_string()),
            ..Default::default()
        };

        assert_eq!(
            config.rpc_url_for(DeepXRpcRole::Submission),
            Err(DeepXError::UnsupportedEnvironment("mainnet".to_string())),
        );
    }

    #[rstest]
    fn debug_redacts_all_endpoint_overrides() {
        const SECRET: &str = "deepx-endpoint-secret";
        let endpoint = format!("https://rpc.example.invalid/{SECRET}?api_key={SECRET}");
        let config = DeepXNetworkConfig {
            base_url_rest: Some(endpoint.clone()),
            base_url_ws: Some(endpoint.clone()),
            base_url_rpc: Some(endpoint.clone()),
            base_url_rpc_submission: Some(endpoint.clone()),
            base_url_rpc_watch: Some(endpoint.clone()),
            base_url_rpc_recovery: Some(endpoint.clone()),
            ..Default::default()
        };

        let debug = format!("{config:?}");

        assert!(debug.contains("environment: Testnet"));
        assert_eq!(debug.matches(REDACTED).count(), 6);
        assert!(!debug.contains(SECRET));
        assert!(!debug.contains(&endpoint));
    }

    #[rstest]
    fn complete_rpc_endpoint_identity_evidence_is_validated() {
        let config = DeepXNetworkConfig {
            base_url_rpc_submission: Some("https://submit.example.invalid/secret".to_string()),
            base_url_rpc_watch: Some("https://watch.example.invalid/secret".to_string()),
            base_url_rpc_recovery: Some("https://recovery.example.invalid/secret".to_string()),
            ..Default::default()
        };

        let validated = validate_rpc_endpoint_identities(&config, observations(&config)).unwrap();

        assert_eq!(
            validated.url_for(DeepXRpcRole::Submission),
            "https://submit.example.invalid/secret",
        );
        assert_eq!(validated.genesis_hash(), approved_genesis_hash());
        assert!(!format!("{validated:?}").contains("secret"));
    }

    #[rstest]
    fn common_rpc_fallback_is_validated_for_every_role() {
        let config = DeepXNetworkConfig {
            base_url_rpc: Some("https://common.example.invalid".to_string()),
            ..Default::default()
        };

        let validated = validate_rpc_endpoint_identities(&config, observations(&config)).unwrap();

        for role in [
            DeepXRpcRole::Submission,
            DeepXRpcRole::Watch,
            DeepXRpcRole::Recovery,
        ] {
            assert_eq!(validated.url_for(role), "https://common.example.invalid");
        }
    }

    #[rstest]
    #[case::submission(DeepXRpcRole::Submission)]
    #[case::watch(DeepXRpcRole::Watch)]
    #[case::recovery(DeepXRpcRole::Recovery)]
    fn wrong_chain_is_rejected_for_each_rpc_role(#[case] role: DeepXRpcRole) {
        let config = DeepXNetworkConfig::default();
        let mut observations = observations(&config);
        observations
            .iter_mut()
            .find(|observation| observation.role() == role)
            .unwrap()
            .genesis_hash = [9; 32];

        assert_eq!(
            validate_rpc_endpoint_identities(&config, observations),
            Err(DeepXRpcEndpointValidationError::GenesisHashMismatch(role)),
        );
    }

    #[rstest]
    fn missing_or_duplicate_rpc_role_is_rejected() {
        let config = DeepXNetworkConfig::default();
        let [submission, watch, recovery] = observations(&config);

        assert_eq!(
            validate_rpc_endpoint_identities(&config, [submission.clone(), recovery]),
            Err(DeepXRpcEndpointValidationError::MissingRole(
                DeepXRpcRole::Watch
            )),
        );
        assert_eq!(
            validate_rpc_endpoint_identities(&config, [submission.clone(), submission, watch]),
            Err(DeepXRpcEndpointValidationError::DuplicateRole(
                DeepXRpcRole::Submission
            )),
        );
    }

    #[rstest]
    fn rpc_endpoint_observation_must_match_configured_url() {
        let config = DeepXNetworkConfig::default();
        let mut observed_endpoints = observations(&config);
        observed_endpoints[0].url = "https://other.example.invalid/secret".to_string();

        assert_eq!(
            validate_rpc_endpoint_identities(&config, observed_endpoints),
            Err(DeepXRpcEndpointValidationError::UrlMismatch(
                DeepXRpcRole::Submission
            )),
        );
        assert!(!format!("{:?}", observations(&config)[0]).contains(DEEPX_TESTNET_RPC_URL));
    }
}
