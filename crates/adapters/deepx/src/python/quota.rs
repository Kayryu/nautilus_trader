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

//! Read-only Python quota summary bindings.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountPage, DeepXHttpClient, DeepXQuotaBuyerType, DeepXQuotaHistoryRecord,
        DeepXQuotaHistoryRequest, DeepXQuotaHistoryType, DeepXQuotaSummary,
    },
};

/// One validated page of wallet quota history.
#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXQuotaHistoryPage {
    items: Vec<DeepXQuotaHistoryRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXQuotaHistoryPage {
    #[getter]
    fn items(&self) -> Vec<DeepXQuotaHistoryRecord> {
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

impl From<DeepXAccountPage<DeepXQuotaHistoryRecord>> for DeepXQuotaHistoryPage {
    fn from(page: DeepXAccountPage<DeepXQuotaHistoryRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXQuotaHistoryRecord {
    #[getter]
    fn id(&self) -> &str {
        &self.id
    }

    #[getter]
    fn history_type(&self) -> &'static str {
        match self.history_type {
            DeepXQuotaHistoryType::Purchase => "purchase",
            DeepXQuotaHistoryType::Activate => "activate",
            DeepXQuotaHistoryType::Free => "free",
        }
    }

    #[getter]
    fn owner_address(&self) -> &str {
        &self.owner_address
    }

    #[getter]
    fn buyer_type(&self) -> Option<&'static str> {
        self.buyer_type.map(|buyer_type| match buyer_type {
            DeepXQuotaBuyerType::Wallet => "wallet",
            DeepXQuotaBuyerType::Subaccount => "subaccount",
        })
    }

    #[getter]
    fn buyer_address(&self) -> Option<&str> {
        self.buyer_address.as_deref()
    }

    #[getter]
    fn quota(&self) -> u64 {
        self.quota
    }

    #[getter]
    fn tx_hash(&self) -> &str {
        &self.tx_hash
    }

    #[getter]
    fn tx_hash_type(&self) -> &str {
        &self.tx_hash_type
    }

    #[getter]
    fn block_number(&self) -> u64 {
        self.block_number
    }

    #[getter]
    fn event_index(&self) -> u64 {
        self.event_index
    }

    #[getter]
    fn created_at(&self) -> &str {
        &self.created_at
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXQuotaSummary {
    #[getter]
    fn owner(&self) -> &str {
        &self.owner
    }

    #[getter]
    fn subaccount_count(&self) -> u64 {
        self.subaccount_count
    }

    #[getter]
    fn spot_volume_usd(&self) -> String {
        self.spot_volume_usd.to_string()
    }

    #[getter]
    fn perp_volume_usd(&self) -> String {
        self.perp_volume_usd.to_string()
    }

    #[getter]
    fn total_volume_usd(&self) -> String {
        self.total_volume_usd.to_string()
    }

    #[getter]
    fn quota_earned(&self) -> u64 {
        self.quota_earned
    }

    #[getter]
    fn quota_granted(&self) -> u64 {
        self.quota_granted
    }

    #[getter]
    fn quota_reserved(&self) -> u64 {
        self.quota_reserved
    }

    #[getter]
    fn quota_pending(&self) -> u64 {
        self.quota_pending
    }

    #[getter]
    fn first_trade_ts_ms(&self) -> Option<u64> {
        self.first_trade_ts_ms
    }

    #[getter]
    fn last_trade_ts_ms(&self) -> Option<u64> {
        self.last_trade_ts_ms
    }

    #[getter]
    fn first_trade_at(&self) -> Option<&str> {
        self.first_trade_at.as_deref()
    }

    #[getter]
    fn last_trade_at(&self) -> Option<&str> {
        self.last_trade_at.as_deref()
    }

    #[getter]
    fn updated_at(&self) -> Option<&str> {
        self.updated_at.as_deref()
    }
}

/// Returns a validated public wallet quota summary without credentials or mutation requests.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_quota_summary", signature = (wallet, config=None))]
pub(super) fn py_get_deepx_quota_summary(
    py: Python<'_>,
    wallet: String,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXQuotaSummary> {
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
    py.detach(move || runtime.block_on(client.get_quota_summary(&wallet)))
        .map_err(to_pyvalue_err)
}

/// Returns one validated chain-confirmed wallet quota-history page without credentials.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_quota_history", signature = (wallet, buyer_address=None, history_type=None, cursor=None, limit=None, config=None))]
pub(super) fn py_get_deepx_quota_history(
    py: Python<'_>,
    wallet: String,
    buyer_address: Option<String>,
    history_type: Option<&str>,
    cursor: Option<String>,
    limit: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXQuotaHistoryPage> {
    let request = quota_history_request(wallet, buyer_address, history_type, cursor, limit)?;
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
    py.detach(move || runtime.block_on(client.get_quota_history(&request)))
        .map(DeepXQuotaHistoryPage::from)
        .map_err(to_pyvalue_err)
}

/// Returns bounded validated wallet quota-history pages without credentials.
///
/// The constituent REST pages are mutable and do not establish a block-pinned snapshot.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_quota_history_pages", signature = (wallet, max_pages, buyer_address=None, history_type=None, cursor=None, limit=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_quota_history_pages(
    py: Python<'_>,
    wallet: String,
    max_pages: usize,
    buyer_address: Option<String>,
    history_type: Option<&str>,
    cursor: Option<String>,
    limit: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXQuotaHistoryPage>> {
    let request = quota_history_request(wallet, buyer_address, history_type, cursor, limit)?;
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
    py.detach(move || runtime.block_on(client.get_quota_history_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}

fn quota_history_request(
    wallet: String,
    buyer_address: Option<String>,
    history_type: Option<&str>,
    cursor: Option<String>,
    limit: Option<u32>,
) -> PyResult<DeepXQuotaHistoryRequest> {
    let history_type = match history_type {
        None => None,
        Some("purchase") => Some(DeepXQuotaHistoryType::Purchase),
        Some("activate") => Some(DeepXQuotaHistoryType::Activate),
        Some("free") => Some(DeepXQuotaHistoryType::Free),
        Some(_) => {
            return Err(to_pyvalue_err(
                "quota history type must be purchase, activate, or free",
            ));
        }
    };
    Ok(DeepXQuotaHistoryRequest {
        wallet,
        buyer_address,
        history_type,
        cursor,
        limit,
    })
}
