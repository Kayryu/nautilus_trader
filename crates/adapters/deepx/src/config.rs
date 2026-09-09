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

//! Configuration for DeepX network, data, and execution access.

use std::fmt::{Debug, Formatter};

use nautilus_core::hex;
use nautilus_model::identifiers::AccountId;
use nautilus_network::retry::RetryConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::common::{
    DeepXEnvironment, DeepXKeyScheme, DeepXPrivateKey, Result,
    consts::{DEEPX_TESTNET_GENESIS_HASH, DEEPX_VENUE},
    urls,
};

const REDACTED: &str = "<redacted>";
const DEFAULT_RECOVERY_BLOCKS_PER_RANGE: u64 = 100;
const DEFAULT_TIMESTAMP_NONCE_MAX_CLOCK_DRIFT_MS: u64 = 5_000;

/// Bounded retry configuration for idempotent DeepX HTTP reads only.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeepXHttpReadRetryConfig {
    /// Maximum retries after the initial read attempt.
    pub max_retries: u32,
    /// Initial delay between attempts in milliseconds.
    pub initial_delay_ms: u64,
    /// Maximum delay between attempts in milliseconds.
    pub max_delay_ms: u64,
    /// Maximum random jitter added to each delay in milliseconds.
    pub jitter_ms: u64,
    /// Timeout for each read attempt in milliseconds.
    pub operation_timeout_ms: u64,
    /// Maximum total elapsed time across all attempts in milliseconds.
    pub max_elapsed_ms: u64,
}

impl Default for DeepXHttpReadRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 250,
            max_delay_ms: 2_000,
            jitter_ms: 100,
            operation_timeout_ms: 30_000,
            max_elapsed_ms: 60_000,
        }
    }
}

impl DeepXHttpReadRetryConfig {
    /// Validates bounded read-retry timing.
    pub fn validate(&self) -> Result<()> {
        if self.initial_delay_ms == 0 {
            return Err(crate::common::DeepXError::InvalidConfiguration(
                "HTTP read retry initial delay must be non-zero".to_string(),
            ));
        }
        if self.max_delay_ms < self.initial_delay_ms {
            return Err(crate::common::DeepXError::InvalidConfiguration(
                "HTTP read retry maximum delay must not be less than the initial delay".to_string(),
            ));
        }
        if self.operation_timeout_ms == 0 || self.max_elapsed_ms == 0 {
            return Err(crate::common::DeepXError::InvalidConfiguration(
                "HTTP read retry timeouts must be non-zero".to_string(),
            ));
        }
        Ok(())
    }

    /// Converts this adapter configuration to the shared retry policy.
    pub fn to_retry_config(&self) -> Result<RetryConfig> {
        self.validate()?;
        Ok(RetryConfig {
            max_retries: self.max_retries,
            initial_delay_ms: self.initial_delay_ms,
            max_delay_ms: self.max_delay_ms,
            backoff_factor: 2.0,
            jitter_ms: self.jitter_ms,
            operation_timeout_ms: Some(self.operation_timeout_ms),
            immediate_first: false,
            max_elapsed_ms: Some(self.max_elapsed_ms),
        })
    }
}

/// Explicit DeepX transaction execution backend.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeepXExecutionBackend {
    /// Metadata-driven direct pallet extrinsics.
    #[default]
    DirectPallet,
    /// Legacy EVM-precompile transactions wrapped in a Substrate extrinsic.
    LegacyEvm,
}

/// Configuration for a fail-closed DeepX data client.
#[derive(Clone, Debug, Deserialize, Serialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct DeepXDataClientConfig {
    /// Testnet network, REST failover, and read-retry configuration.
    #[builder(default)]
    pub network: DeepXNetworkConfig,
    /// Optional proxy URL for future HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// HTTP operation timeout in seconds.
    #[builder(default = 30)]
    pub http_timeout_secs: u64,
    /// WebSocket operation timeout in seconds.
    #[builder(default = 30)]
    pub websocket_timeout_secs: u64,
}

impl Default for DeepXDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl DeepXDataClientConfig {
    /// Validates the testnet-only data client configuration.
    pub fn validate(&self) -> Result<()> {
        self.network.validate()?;
        if self.http_timeout_secs == 0 {
            return Err(crate::common::DeepXError::InvalidConfiguration(
                "HTTP timeout must be non-zero".to_string(),
            ));
        }
        if self.websocket_timeout_secs == 0 {
            return Err(crate::common::DeepXError::InvalidConfiguration(
                "WebSocket timeout must be non-zero".to_string(),
            ));
        }
        Ok(())
    }
}

/// Configuration for a fail-closed DeepX execution client.
#[derive(Clone, Deserialize, Serialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct DeepXExecutionClientConfig {
    /// Account identifier for the execution client.
    #[builder(default = AccountId::from("DEEPX-001"))]
    pub account_id: AccountId,
    /// Explicit DeepX subaccount identity.
    pub subaccount_id: Option<String>,
    /// secp256k1 private key, loaded from `DEEPX_TESTNET_PRIVATE_KEY` when unset.
    pub private_key: Option<String>,
    /// Transaction encoding and submission backend.
    #[builder(default)]
    pub execution_backend: DeepXExecutionBackend,
    /// Maximum finalized blocks requested in one canonical recovery scan range.
    #[builder(default = DEFAULT_RECOVERY_BLOCKS_PER_RANGE)]
    pub recovery_blocks_per_range: u64,
    /// Maximum accepted difference between local and chain time for timestamp nonce allocation.
    #[builder(default = DEFAULT_TIMESTAMP_NONCE_MAX_CLOCK_DRIFT_MS)]
    pub timestamp_nonce_max_clock_drift_ms: u64,
    /// Testnet network and RPC-role configuration.
    #[builder(default)]
    pub network: DeepXNetworkConfig,
}

impl Default for DeepXExecutionClientConfig {
    fn default() -> Self {
        Self {
            account_id: AccountId::from("DEEPX-001"),
            subaccount_id: None,
            private_key: None,
            execution_backend: DeepXExecutionBackend::default(),
            recovery_blocks_per_range: DEFAULT_RECOVERY_BLOCKS_PER_RANGE,
            timestamp_nonce_max_clock_drift_ms: DEFAULT_TIMESTAMP_NONCE_MAX_CLOCK_DRIFT_MS,
            network: DeepXNetworkConfig::default(),
        }
    }
}

impl Debug for DeepXExecutionClientConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXExecutionClientConfig))
            .field("account_id", &self.account_id)
            .field("subaccount_id", &self.subaccount_id)
            .field("private_key", &self.private_key.as_ref().map(|_| REDACTED))
            .field("execution_backend", &self.execution_backend)
            .field("recovery_blocks_per_range", &self.recovery_blocks_per_range)
            .field(
                "timestamp_nonce_max_clock_drift_ms",
                &self.timestamp_nonce_max_clock_drift_ms,
            )
            .field("network", &self.network)
            .finish()
    }
}

impl DeepXExecutionClientConfig {
    /// Validates the execution identity and supported deployment.
    pub fn validate(&self) -> Result<()> {
        self.network.validate()?;
        if self.account_id.get_issuer() != *DEEPX_VENUE {
            return Err(crate::common::DeepXError::InvalidConfiguration(format!(
                "DeepX account ID issuer must be {}",
                *DEEPX_VENUE,
            )));
        }
        if self.recovery_blocks_per_range == 0 {
            return Err(crate::common::DeepXError::InvalidConfiguration(
                "DeepX recovery blocks per range must be greater than zero".to_string(),
            ));
        }
        if self.timestamp_nonce_max_clock_drift_ms == 0 {
            return Err(crate::common::DeepXError::InvalidConfiguration(
                "DeepX timestamp nonce maximum clock drift must be greater than zero".to_string(),
            ));
        }
        match self.subaccount_id.as_deref() {
            Some(value) if !value.trim().is_empty() => Ok(()),
            _ => Err(crate::common::DeepXError::InvalidConfiguration(
                "DeepX subaccount identity must be explicitly configured".to_string(),
            )),
        }
    }

    /// Resolves and validates the configured testnet signing credential.
    pub fn resolve_private_key(&self) -> Result<DeepXPrivateKey> {
        self.validate()?;
        match &self.private_key {
            Some(value) => DeepXPrivateKey::new(value, &DeepXKeyScheme::Secp256k1),
            None => {
                DeepXPrivateKey::from_env(&self.network.environment, &DeepXKeyScheme::Secp256k1)
            }
        }
    }
}

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
    /// Optional ordered REST API base URL overrides for read failover.
    pub base_urls_rest: Option<Vec<String>>,
    /// Retry policy for idempotent HTTP reads.
    pub http_read_retry: DeepXHttpReadRetryConfig,
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
            .field(
                "base_urls_rest",
                &self
                    .base_urls_rest
                    .as_ref()
                    .map(|urls| vec![REDACTED; urls.len()]),
            )
            .field("http_read_retry", &self.http_read_retry)
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
        if self.base_urls_rest.as_ref().is_some_and(Vec::is_empty) {
            return Err(crate::common::DeepXError::InvalidConfiguration(
                "REST failover endpoints cannot be empty".to_string(),
            ));
        }
        self.http_read_retry.validate()?;
        Ok(())
    }

    /// Returns the configured REST API URL.
    pub fn rest_url(&self) -> Result<String> {
        Ok(self.rest_urls()?.remove(0))
    }

    /// Returns the configured REST API URLs in read-failover order.
    pub fn rest_urls(&self) -> Result<Vec<String>> {
        self.validate()?;
        if let Some(urls) = &self.base_urls_rest {
            return Ok(urls.clone());
        }
        match &self.base_url_rest {
            Some(url) => Ok(vec![url.clone()]),
            None => Ok(vec![urls::rest_url(&self.environment)?.to_string()]),
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
    fn execution_config_requires_explicit_deepx_subaccount() {
        let config = DeepXExecutionClientConfig::default();

        assert!(matches!(
            config.validate(),
            Err(DeepXError::InvalidConfiguration(message))
                if message.contains("subaccount identity"),
        ));
    }

    #[rstest]
    fn execution_config_rejects_non_deepx_account() {
        let config = DeepXExecutionClientConfig {
            account_id: AccountId::from("OTHER-001"),
            subaccount_id: Some("subaccount-1".to_string()),
            ..Default::default()
        };

        assert!(matches!(
            config.validate(),
            Err(DeepXError::InvalidConfiguration(message)) if message.contains("issuer"),
        ));
    }

    #[rstest]
    fn execution_config_rejects_mainnet_before_credentials() {
        let config = DeepXExecutionClientConfig {
            subaccount_id: Some("subaccount-1".to_string()),
            private_key: Some(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            ),
            network: DeepXNetworkConfig {
                environment: DeepXEnvironment::Mainnet,
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(matches!(
            config.resolve_private_key(),
            Err(DeepXError::UnsupportedEnvironment(environment)) if environment == "mainnet",
        ));
    }

    #[rstest]
    fn execution_config_debug_redacts_private_key() {
        const PRIVATE_KEY: &str =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let config = DeepXExecutionClientConfig {
            subaccount_id: Some("subaccount-1".to_string()),
            private_key: Some(PRIVATE_KEY.to_string()),
            ..Default::default()
        };

        let debug = format!("{config:?}");

        assert!(debug.contains("DirectPallet"));
        assert!(debug.contains(REDACTED));
        assert!(!debug.contains(PRIVATE_KEY));
    }

    #[rstest]
    fn execution_backend_is_explicitly_serialized() {
        let config: DeepXExecutionClientConfig = serde_json::from_str(
            r#"{"subaccount_id":"subaccount-1","execution_backend":"legacy_evm","recovery_blocks_per_range":25}"#,
        )
        .unwrap();

        assert_eq!(config.execution_backend, DeepXExecutionBackend::LegacyEvm,);
        assert_eq!(config.recovery_blocks_per_range, 25);
        assert!(config.validate().is_ok());
    }

    #[rstest]
    fn execution_config_defaults_to_bounded_recovery_ranges() {
        let config = DeepXExecutionClientConfig::default();

        assert_eq!(
            config.recovery_blocks_per_range,
            DEFAULT_RECOVERY_BLOCKS_PER_RANGE
        );
        assert_eq!(
            config.timestamp_nonce_max_clock_drift_ms,
            DEFAULT_TIMESTAMP_NONCE_MAX_CLOCK_DRIFT_MS,
        );
    }

    #[rstest]
    fn execution_config_rejects_empty_recovery_ranges() {
        let config = DeepXExecutionClientConfig {
            subaccount_id: Some("subaccount-1".to_string()),
            recovery_blocks_per_range: 0,
            ..Default::default()
        };

        assert!(matches!(
            config.validate(),
            Err(DeepXError::InvalidConfiguration(message))
                if message.contains("recovery blocks per range"),
        ));
    }

    #[rstest]
    fn execution_config_rejects_zero_timestamp_nonce_clock_drift() {
        let config = DeepXExecutionClientConfig {
            subaccount_id: Some("subaccount-1".to_string()),
            timestamp_nonce_max_clock_drift_ms: 0,
            ..Default::default()
        };

        assert!(matches!(
            config.validate(),
            Err(DeepXError::InvalidConfiguration(message))
                if message.contains("timestamp nonce maximum clock drift"),
        ));
    }

    #[rstest]
    fn data_config_defaults_to_strict_testnet_network() {
        let config = DeepXDataClientConfig::default();

        assert_eq!(config.network.environment, DeepXEnvironment::Testnet);
        assert_eq!(config.http_timeout_secs, 30);
        assert_eq!(config.websocket_timeout_secs, 30);
        assert!(config.validate().is_ok());
    }

    #[rstest]
    #[case(0, 30)]
    #[case(30, 0)]
    fn data_config_rejects_zero_timeouts(
        #[case] http_timeout_secs: u64,
        #[case] websocket_timeout_secs: u64,
    ) {
        let config = DeepXDataClientConfig {
            http_timeout_secs,
            websocket_timeout_secs,
            ..Default::default()
        };

        assert!(matches!(
            config.validate(),
            Err(DeepXError::InvalidConfiguration(_)),
        ));
    }

    #[rstest]
    fn data_config_rejects_mainnet_even_with_url_overrides() {
        let config = DeepXDataClientConfig {
            network: DeepXNetworkConfig {
                environment: DeepXEnvironment::Mainnet,
                base_url_rest: Some("https://example.invalid".to_string()),
                base_url_ws: Some("wss://example.invalid".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(matches!(
            config.validate(),
            Err(DeepXError::UnsupportedEnvironment(environment)) if environment == "mainnet",
        ));
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
    fn rest_failover_endpoints_take_precedence_over_single_override() {
        let config = DeepXNetworkConfig {
            base_url_rest: Some("https://single.example.invalid".to_string()),
            base_urls_rest: Some(vec![
                "https://primary.example.invalid".to_string(),
                "https://secondary.example.invalid".to_string(),
            ]),
            ..Default::default()
        };

        assert_eq!(
            config.rest_urls().unwrap(),
            [
                "https://primary.example.invalid",
                "https://secondary.example.invalid"
            ],
        );
        assert_eq!(
            config.rest_url().unwrap(),
            "https://primary.example.invalid",
        );
    }

    #[rstest]
    fn empty_rest_failover_endpoints_are_rejected() {
        let config = DeepXNetworkConfig {
            base_urls_rest: Some(Vec::new()),
            ..Default::default()
        };

        assert!(matches!(
            config.rest_urls(),
            Err(DeepXError::InvalidConfiguration(message))
                if message.contains("REST failover endpoints"),
        ));
    }

    #[rstest]
    fn http_read_retry_config_converts_to_bounded_shared_policy() {
        let config = DeepXHttpReadRetryConfig {
            max_retries: 2,
            initial_delay_ms: 10,
            max_delay_ms: 40,
            jitter_ms: 3,
            operation_timeout_ms: 100,
            max_elapsed_ms: 250,
        };

        let retry = config.to_retry_config().unwrap();

        assert_eq!(retry.max_retries, 2);
        assert_eq!(retry.initial_delay_ms, 10);
        assert_eq!(retry.max_delay_ms, 40);
        assert_eq!(retry.backoff_factor, 2.0);
        assert_eq!(retry.jitter_ms, 3);
        assert_eq!(retry.operation_timeout_ms, Some(100));
        assert!(!retry.immediate_first);
        assert_eq!(retry.max_elapsed_ms, Some(250));
    }

    #[rstest]
    #[case(0, 10, 100, 200)]
    #[case(20, 10, 100, 200)]
    #[case(10, 20, 0, 200)]
    #[case(10, 20, 100, 0)]
    fn invalid_http_read_retry_timing_is_rejected(
        #[case] initial_delay_ms: u64,
        #[case] max_delay_ms: u64,
        #[case] operation_timeout_ms: u64,
        #[case] max_elapsed_ms: u64,
    ) {
        let config = DeepXHttpReadRetryConfig {
            initial_delay_ms,
            max_delay_ms,
            operation_timeout_ms,
            max_elapsed_ms,
            ..Default::default()
        };

        assert!(matches!(
            config.validate(),
            Err(DeepXError::InvalidConfiguration(_)),
        ));
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
            base_urls_rest: Some(vec![endpoint.clone(), endpoint.clone()]),
            base_url_ws: Some(endpoint.clone()),
            base_url_rpc: Some(endpoint.clone()),
            base_url_rpc_submission: Some(endpoint.clone()),
            base_url_rpc_watch: Some(endpoint.clone()),
            base_url_rpc_recovery: Some(endpoint.clone()),
            ..Default::default()
        };

        let debug = format!("{config:?}");

        assert!(debug.contains("environment: Testnet"));
        assert_eq!(debug.matches(REDACTED).count(), 8);
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
