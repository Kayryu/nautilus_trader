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

//! Read-only Python delegate-directory bindings.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXDelegateAccount, DeepXDelegateWallets, DeepXHttpClient, DeepXWalletDelegateAccounts,
    },
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXDelegateAccount {
    #[getter]
    fn delegate_address(&self) -> &str {
        &self.delegate_address
    }

    #[getter]
    fn delegate_name(&self) -> &str {
        &self.delegate_name
    }

    #[getter]
    fn valid_until(&self) -> u64 {
        self.valid_until
    }

    #[getter]
    fn mode(&self) -> &'static str {
        self.mode.as_str()
    }

    #[getter]
    fn create_time(&self) -> u64 {
        self.create_time
    }

    #[getter]
    fn active(&self) -> bool {
        self.active
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXWalletDelegateAccounts {
    #[getter("wallet")]
    fn py_wallet(&self) -> &str {
        self.wallet()
    }

    #[getter]
    fn accounts(&self) -> Vec<DeepXDelegateAccount> {
        self.accounts.clone()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXDelegateWallets {
    #[getter("delegate")]
    fn py_delegate(&self) -> &str {
        self.delegate()
    }

    #[getter]
    fn wallets(&self) -> Vec<String> {
        self.wallets.clone()
    }
}

/// Returns the validated public delegate directory for one wallet.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_delegate_accounts", signature = (wallet, config=None))]
pub(super) fn py_get_deepx_delegate_accounts(
    py: Python<'_>,
    wallet: String,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXWalletDelegateAccounts> {
    let config = config.unwrap_or_default();
    config.validate().map_err(to_pyvalue_err)?;
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )
    .map_err(to_pyvalue_err)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)?;
    py.detach(move || runtime.block_on(client.get_delegate_accounts(&wallet)))
        .map_err(to_pyvalue_err)
}

/// Returns the validated public wallet directory for one delegate.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_delegator_accounts", signature = (delegate, config=None))]
pub(super) fn py_get_deepx_delegator_accounts(
    py: Python<'_>,
    delegate: String,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXDelegateWallets> {
    let config = config.unwrap_or_default();
    config.validate().map_err(to_pyvalue_err)?;
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )
    .map_err(to_pyvalue_err)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)?;
    py.detach(move || runtime.block_on(client.get_delegator_accounts(&delegate)))
        .map_err(to_pyvalue_err)
}
