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

//! Python bindings for DeepX configuration.

use nautilus_core::python::to_pyvalue_err;
use nautilus_infrastructure::sql::pg::PostgresConnectOptions;
use nautilus_model::identifiers::AccountId;
use pyo3::prelude::*;

use crate::{
    common::DeepXEnvironment,
    config::{
        DeepXDataClientConfig, DeepXExecutionBackend, DeepXExecutionClientConfig,
        DeepXHttpReadRetryConfig, DeepXNetworkConfig,
    },
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXHttpReadRetryConfig {
    /// Bounded retry configuration for idempotent DeepX HTTP reads.
    #[new]
    #[pyo3(signature = (
        max_retries = None,
        initial_delay_ms = None,
        max_delay_ms = None,
        jitter_ms = None,
        operation_timeout_ms = None,
        max_elapsed_ms = None,
    ))]
    fn py_new(
        max_retries: Option<u32>,
        initial_delay_ms: Option<u64>,
        max_delay_ms: Option<u64>,
        jitter_ms: Option<u64>,
        operation_timeout_ms: Option<u64>,
        max_elapsed_ms: Option<u64>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let config = Self {
            max_retries: max_retries.unwrap_or(defaults.max_retries),
            initial_delay_ms: initial_delay_ms.unwrap_or(defaults.initial_delay_ms),
            max_delay_ms: max_delay_ms.unwrap_or(defaults.max_delay_ms),
            jitter_ms: jitter_ms.unwrap_or(defaults.jitter_ms),
            operation_timeout_ms: operation_timeout_ms.unwrap_or(defaults.operation_timeout_ms),
            max_elapsed_ms: max_elapsed_ms.unwrap_or(defaults.max_elapsed_ms),
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

nautilus_core::impl_pyo3_config_getters!(DeepXHttpReadRetryConfig {
    max_retries: u32,
    initial_delay_ms: u64,
    max_delay_ms: u64,
    jitter_ms: u64,
    operation_timeout_ms: u64,
    max_elapsed_ms: u64,
});

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXNetworkConfig {
    /// Testnet-only DeepX network configuration.
    #[new]
    #[pyo3(signature = (
        environment = None,
        base_url_rest = None,
        base_urls_rest = None,
        http_read_retry = None,
        base_url_ws = None,
        base_url_rpc = None,
        base_url_rpc_submission = None,
        base_url_rpc_watch = None,
        base_url_rpc_recovery = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        environment: Option<String>,
        base_url_rest: Option<String>,
        base_urls_rest: Option<Vec<String>>,
        http_read_retry: Option<DeepXHttpReadRetryConfig>,
        base_url_ws: Option<String>,
        base_url_rpc: Option<String>,
        base_url_rpc_submission: Option<String>,
        base_url_rpc_watch: Option<String>,
        base_url_rpc_recovery: Option<String>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let config = Self {
            environment: environment.map_or(defaults.environment, DeepXEnvironment::from),
            base_url_rest,
            base_urls_rest,
            http_read_retry: http_read_retry.unwrap_or(defaults.http_read_retry),
            base_url_ws,
            base_url_rpc,
            base_url_rpc_submission,
            base_url_rpc_watch,
            base_url_rpc_recovery,
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    #[getter]
    fn environment(&self) -> &str {
        self.environment.as_str()
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

nautilus_core::impl_pyo3_config_getters!(DeepXNetworkConfig {
    http_read_retry: DeepXHttpReadRetryConfig,
});

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXDataClientConfig {
    /// Configuration for the fail-closed DeepX data client.
    #[new]
    #[pyo3(signature = (
        network = None,
        proxy_url = None,
        http_timeout_secs = None,
        websocket_timeout_secs = None,
    ))]
    fn py_new(
        network: Option<DeepXNetworkConfig>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        websocket_timeout_secs: Option<u64>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let config = Self {
            network: network.unwrap_or(defaults.network),
            proxy_url,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            websocket_timeout_secs: websocket_timeout_secs
                .unwrap_or(defaults.websocket_timeout_secs),
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    #[getter]
    const fn has_proxy_url(&self) -> bool {
        self.proxy_url.is_some()
    }

    fn __repr__(&self) -> String {
        stringify!(DeepXDataClientConfig).to_string()
    }
}

nautilus_core::impl_pyo3_config_getters!(DeepXDataClientConfig {
    network: DeepXNetworkConfig,
    http_timeout_secs: u64,
    websocket_timeout_secs: u64,
});

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXExecutionClientConfig {
    /// Configuration for the fail-closed DeepX execution client.
    #[new]
    #[pyo3(signature = (
        account_id = None,
        subaccount_id = None,
        private_key = None,
        proxy_url = None,
        http_timeout_secs = None,
        execution_backend = None,
        recovery_blocks_per_range = None,
        timestamp_nonce_max_clock_drift_ms = None,
        postgres_cache_database_config = None,
        network = None,
    ))]
    fn py_new(
        account_id: Option<AccountId>,
        subaccount_id: Option<String>,
        private_key: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        execution_backend: Option<String>,
        recovery_blocks_per_range: Option<u64>,
        timestamp_nonce_max_clock_drift_ms: Option<u64>,
        #[gen_stub(
            override_type(
                type_repr = "typing.Optional[nautilus_trader.infrastructure.PostgresConnectOptions]",
                imports = ("typing", "nautilus_trader.infrastructure"),
            ),
        )]
        postgres_cache_database_config: Option<PostgresConnectOptions>,
        network: Option<DeepXNetworkConfig>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let execution_backend = match execution_backend {
            None => DeepXExecutionBackend::DirectPallet,
            Some(value) if value == "direct_pallet" => DeepXExecutionBackend::DirectPallet,
            Some(value) if value == "legacy_evm" => DeepXExecutionBackend::LegacyEvm,
            Some(value) => {
                return Err(to_pyvalue_err(format!(
                    "unsupported DeepX execution backend: {value}"
                )));
            }
        };
        let config = Self {
            account_id: account_id.unwrap_or(defaults.account_id),
            subaccount_id,
            private_key,
            proxy_url,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            execution_backend,
            recovery_blocks_per_range: recovery_blocks_per_range
                .unwrap_or(defaults.recovery_blocks_per_range),
            timestamp_nonce_max_clock_drift_ms: timestamp_nonce_max_clock_drift_ms
                .unwrap_or(defaults.timestamp_nonce_max_clock_drift_ms),
            postgres_cache_database_config,
            network: network.unwrap_or(defaults.network),
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    #[getter]
    const fn has_private_key(&self) -> bool {
        self.private_key.is_some()
    }

    #[getter]
    const fn has_proxy_url(&self) -> bool {
        self.proxy_url.is_some()
    }

    #[getter]
    const fn has_postgres_cache_database_config(&self) -> bool {
        self.postgres_cache_database_config.is_some()
    }

    #[getter]
    const fn execution_backend(&self) -> &str {
        match self.execution_backend {
            DeepXExecutionBackend::DirectPallet => "direct_pallet",
            DeepXExecutionBackend::LegacyEvm => "legacy_evm",
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

nautilus_core::impl_pyo3_config_getters!(DeepXExecutionClientConfig {
    account_id: AccountId,
    subaccount_id: Option<String>,
    http_timeout_secs: u64,
    recovery_blocks_per_range: u64,
    timestamp_nonce_max_clock_drift_ms: u64,
    network: DeepXNetworkConfig,
});

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn retry_config_py_new_uses_validated_defaults() {
        let config = DeepXHttpReadRetryConfig::py_new(None, None, None, None, None, None).unwrap();

        assert_eq!(config, DeepXHttpReadRetryConfig::default());
    }

    #[rstest]
    fn network_config_py_new_rejects_mainnet() {
        Python::initialize();
        let error = DeepXNetworkConfig::py_new(
            Some("mainnet".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();

        assert!(error.to_string().contains("mainnet"));
    }

    #[rstest]
    fn execution_config_py_new_rejects_unknown_backend() {
        Python::initialize();
        let error = DeepXExecutionClientConfig::py_new(
            None,
            Some("0x1111111111111111111111111111111111111111".to_string()),
            None,
            None,
            None,
            Some("automatic".to_string()),
            None,
            None,
            None,
            None,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unsupported DeepX execution backend")
        );
    }

    #[rstest]
    fn execution_config_repr_redacts_sensitive_values() {
        let private_key = "0000000000000000000000000000000000000000000000000000000000000001";
        let proxy_url = "https://user:secret@proxy.example.invalid";
        let postgres_password = "postgres-secret";
        let config = DeepXExecutionClientConfig::py_new(
            None,
            Some("0x1111111111111111111111111111111111111111".to_string()),
            Some(private_key.to_string()),
            Some(proxy_url.to_string()),
            None,
            None,
            None,
            None,
            Some(PostgresConnectOptions::new(
                "localhost".to_string(),
                5432,
                "nautilus".to_string(),
                postgres_password.to_string(),
                "nautilus".to_string(),
            )),
            None,
        )
        .unwrap();

        let representation = config.__repr__();
        assert!(config.has_private_key());
        assert!(config.has_proxy_url());
        assert!(config.has_postgres_cache_database_config());
        assert!(representation.contains("<redacted>"));
        assert!(!representation.contains(private_key));
        assert!(!representation.contains(proxy_url));
        assert!(!representation.contains(postgres_password));
    }
}
