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

//! Configuration structures for the DeepX adapter.

use std::fmt::Debug;

use nautilus_core::string::secret::REDACTED;
use nautilus_model::identifiers::AccountId;
use serde::{Deserialize, Serialize};

use crate::common::{enums::DeepXEnvironment, urls};

/// Configuration for the DeepX data client.
#[derive(Clone, Debug, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXDataClientConfig {
    /// DeepX environment to connect to.
    #[builder(default)]
    pub environment: DeepXEnvironment,
    /// Optional REST API base URL override.
    pub base_url_rest: Option<String>,
    /// Optional public WebSocket URL override.
    pub base_url_ws: Option<String>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// HTTP request timeout in seconds.
    #[builder(default = 10)]
    pub http_timeout_secs: u64,
    /// WebSocket connection and request timeout in seconds.
    #[builder(default = 30)]
    pub ws_timeout_secs: u64,
    /// Instrument metadata refresh interval in minutes.
    #[builder(default = 60)]
    pub update_instruments_interval_mins: u64,
}

impl Default for DeepXDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl DeepXDataClientConfig {
    /// Creates a new configuration with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the resolved REST API base URL.
    #[must_use]
    pub fn rest_url(&self) -> String {
        self.base_url_rest
            .clone()
            .unwrap_or_else(|| urls::rest_url(self.environment).to_string())
    }

    /// Returns the resolved public WebSocket URL.
    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| urls::ws_url(self.environment).to_string())
    }
}

/// Configuration for the DeepX execution client.
#[derive(Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct DeepXExecutionClientConfig {
    /// DeepX environment to connect to.
    #[builder(default)]
    pub environment: DeepXEnvironment,
    /// Nautilus account identifier for the execution client.
    #[builder(default = AccountId::from("DEEPX-001"))]
    pub account_id: AccountId,
    /// Wallet address controlling the configured subaccount.
    pub wallet_address: Option<String>,
    /// Subaccount address used for account state and trading.
    pub subaccount_address: Option<String>,
    /// Hex-encoded private key used to sign DeepX extrinsics.
    pub private_key: Option<String>,
    /// Optional REST API base URL override.
    pub base_url_rest: Option<String>,
    /// Optional account WebSocket URL override.
    pub base_url_ws: Option<String>,
    /// Optional Substrate WebSocket URL override used to load runtime metadata.
    pub base_url_substrate_ws: Option<String>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// HTTP request timeout in seconds.
    #[builder(default = 10)]
    pub http_timeout_secs: u64,
    /// WebSocket connection and request timeout in seconds.
    #[builder(default = 30)]
    pub ws_timeout_secs: u64,
}

impl Default for DeepXExecutionClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl DeepXExecutionClientConfig {
    /// Creates a new configuration with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the resolved REST API base URL.
    #[must_use]
    pub fn rest_url(&self) -> String {
        self.base_url_rest
            .clone()
            .unwrap_or_else(|| urls::rest_url(self.environment).to_string())
    }

    /// Returns the resolved account WebSocket URL.
    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| urls::ws_url(self.environment).to_string())
    }

    /// Returns the resolved Substrate WebSocket URL.
    #[must_use]
    pub fn substrate_ws_url(&self) -> String {
        self.base_url_substrate_ws
            .clone()
            .unwrap_or_else(|| urls::substrate_ws_url(self.environment).to_string())
    }
}

impl Debug for DeepXExecutionClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXExecutionClientConfig))
            .field("environment", &self.environment)
            .field("account_id", &self.account_id)
            .field("wallet_address", &self.wallet_address)
            .field("subaccount_address", &self.subaccount_address)
            .field("private_key", &self.private_key.as_ref().map(|_| REDACTED))
            .field("base_url_rest", &self.base_url_rest)
            .field("base_url_ws", &self.base_url_ws)
            .field("base_url_substrate_ws", &self.base_url_substrate_ws)
            .field("proxy_url", &self.proxy_url)
            .field("http_timeout_secs", &self.http_timeout_secs)
            .field("ws_timeout_secs", &self.ws_timeout_secs)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::consts::{
        DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_SUBSTRATE_WS_URL, DEEPX_TESTNET_WS_URL,
    };

    #[rstest]
    fn data_config_resolves_testnet_defaults() {
        let config = DeepXDataClientConfig::default();

        assert_eq!(config.rest_url(), DEEPX_TESTNET_REST_URL);
        assert_eq!(config.ws_url(), DEEPX_TESTNET_WS_URL);
    }

    #[rstest]
    fn execution_config_resolves_testnet_defaults() {
        let config = DeepXExecutionClientConfig::default();

        assert_eq!(config.rest_url(), DEEPX_TESTNET_REST_URL);
        assert_eq!(config.ws_url(), DEEPX_TESTNET_WS_URL);
        assert_eq!(config.substrate_ws_url(), DEEPX_TESTNET_SUBSTRATE_WS_URL);
    }

    #[rstest]
    fn execution_config_redacts_private_key() {
        let config = DeepXExecutionClientConfig::builder()
            .private_key("0xsecret".to_string())
            .build();

        let debug = format!("{config:?}");

        assert!(debug.contains(REDACTED));
        assert!(!debug.contains("0xsecret"));
    }
}
