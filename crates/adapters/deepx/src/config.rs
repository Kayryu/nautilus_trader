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

use serde::{Deserialize, Serialize};

use crate::common::{DeepXEnvironment, Result, urls};

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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::{
        DeepXError,
        consts::{DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL, DEEPX_TESTNET_WS_URL},
    };

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
}
