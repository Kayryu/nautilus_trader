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

use serde::{Deserialize, Serialize};

use crate::common::{DeepXEnvironment, Result, urls};

/// Read-only DeepX network configuration.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
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
}
