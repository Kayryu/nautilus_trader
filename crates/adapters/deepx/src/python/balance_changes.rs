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

//! Read-only Python account balance-change bindings.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountPage, DeepXBalanceChangePosition, DeepXBalanceChangeRecord,
        DeepXBalanceChangeType, DeepXBalanceChangesRequest, DeepXHttpClient,
    },
};

/// One validated page of account balance changes.
#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXBalanceChangesPage {
    items: Vec<DeepXBalanceChangeRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXBalanceChangesPage {
    #[getter]
    fn items(&self) -> Vec<DeepXBalanceChangeRecord> {
        self.items.clone()
    }

    #[getter]
    fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    #[getter]
    fn has_next(&self) -> bool {
        self.has_next
    }
}

impl From<DeepXAccountPage<DeepXBalanceChangeRecord>> for DeepXBalanceChangesPage {
    fn from(page: DeepXAccountPage<DeepXBalanceChangeRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXBalanceChangePosition {
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    #[getter]
    fn owner(&self) -> &str {
        &self.owner
    }

    #[getter]
    fn market_id(&self) -> u64 {
        self.market_id
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXBalanceChangeRecord {
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    #[getter]
    fn asset(&self) -> &str {
        &self.asset
    }

    #[getter]
    fn balance_change(&self) -> String {
        self.balance_change.to_string()
    }

    #[getter]
    fn change_type(&self) -> &'static str {
        self.change_type.as_str()
    }

    #[getter]
    fn time(&self) -> u64 {
        self.time
    }

    #[getter]
    fn height(&self) -> u64 {
        self.height
    }

    #[getter]
    fn event_idx(&self) -> u64 {
        self.event_idx
    }

    #[getter]
    fn tx_hash(&self) -> Option<&str> {
        self.tx_hash.as_deref()
    }

    #[getter]
    fn tx_hash_type(&self) -> Option<&str> {
        self.tx_hash_type.as_deref()
    }

    #[getter("from_address")]
    fn py_from_address(&self) -> Option<&str> {
        self.from.as_deref()
    }

    #[getter]
    fn to_address(&self) -> Option<&str> {
        self.to.as_deref()
    }

    #[getter]
    fn cross_chain_json(&self) -> Option<&str> {
        self.cross_chain
            .as_deref()
            .map(serde_json::value::RawValue::get)
    }

    #[getter]
    fn position(&self) -> Option<DeepXBalanceChangePosition> {
        self.position.clone().map(DeepXBalanceChangePosition)
    }
}

/// Returns one validated account balance-change page without credentials.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_balance_changes", signature = (*, wallet=None, subaccount=None, start_ms=None, end_ms=None, change_types=None, cursor=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_balance_changes(
    py: Python<'_>,
    wallet: Option<String>,
    subaccount: Option<String>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    change_types: Option<Vec<String>>,
    cursor: Option<String>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXBalanceChangesPage> {
    let change_types = change_types
        .unwrap_or_default()
        .into_iter()
        .map(|value| {
            serde_json::from_value::<DeepXBalanceChangeType>(serde_json::Value::String(value))
                .map_err(|_| to_pyvalue_err("unknown balance change type"))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let request = DeepXBalanceChangesRequest {
        subaccount,
        wallet,
        start_ms,
        end_ms,
        change_types,
        cursor,
        page_size,
    };
    request.validate().map_err(to_pyvalue_err)?;
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
    py.detach(move || runtime.block_on(client.get_balance_changes(&request)))
        .map(DeepXBalanceChangesPage::from)
        .map_err(to_pyvalue_err)
}

/// Returns bounded validated balance-change pages without credentials.
///
/// The constituent REST pages are mutable and do not establish a block-pinned snapshot.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_balance_change_pages", signature = (*, max_pages, wallet=None, subaccount=None, start_ms=None, end_ms=None, change_types=None, cursor=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_balance_change_pages(
    py: Python<'_>,
    max_pages: usize,
    wallet: Option<String>,
    subaccount: Option<String>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    change_types: Option<Vec<String>>,
    cursor: Option<String>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXBalanceChangesPage>> {
    let change_types = change_types
        .unwrap_or_default()
        .into_iter()
        .map(|value| {
            serde_json::from_value::<DeepXBalanceChangeType>(serde_json::Value::String(value))
                .map_err(|_| to_pyvalue_err("unknown balance change type"))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let request = DeepXBalanceChangesRequest {
        subaccount,
        wallet,
        start_ms,
        end_ms,
        change_types,
        cursor,
        page_size,
    };
    request.validate().map_err(to_pyvalue_err)?;
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
    py.detach(move || runtime.block_on(client.get_balance_change_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}
