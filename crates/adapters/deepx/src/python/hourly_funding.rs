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

//! Read-only Python hourly unsettled-funding boundaries.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountSortOrder, DeepXHourlyUnsettledFundingRecord,
        DeepXHourlyUnsettledFundingRequest, DeepXHttpClient,
    },
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXHourlyUnsettledFundingRecord {
    #[getter]
    fn subaccount(&self) -> &str {
        &self.subaccount
    }

    #[getter]
    fn market_id(&self) -> u64 {
        self.market_id
    }

    #[getter]
    fn position_version(&self) -> u64 {
        self.position_version
    }

    #[getter]
    fn signed_position_size_raw(&self) -> String {
        self.signed_position_size_raw.to_string()
    }

    #[getter]
    fn baseline_index_raw(&self) -> String {
        self.baseline_index_raw.to_string()
    }

    #[getter]
    fn cumulative_index_raw(&self) -> String {
        self.cumulative_index_raw.to_string()
    }

    #[getter]
    fn delta_index_raw(&self) -> String {
        self.delta_index_raw.to_string()
    }

    #[getter]
    fn mark_price_raw(&self) -> String {
        self.mark_price_raw.to_string()
    }

    #[getter]
    fn payment_raw(&self) -> String {
        self.payment_raw.to_string()
    }

    #[getter]
    fn boundary_timestamp_ms(&self) -> u64 {
        self.boundary_timestamp_ms
    }

    #[getter]
    fn boundary_block(&self) -> u64 {
        self.boundary_block
    }

    #[getter]
    fn boundary_event_index(&self) -> u64 {
        self.boundary_event_index
    }

    #[getter]
    fn boundary_event_id(&self) -> &str {
        &self.boundary_event_id
    }
}

/// Returns bounded validated hourly funding boundaries without credentials.
///
/// Amounts remain exact raw on-chain integers and do not establish settled or current PnL.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_hourly_unsettled_funding_pages", signature = (*, max_pages, wallet=None, subaccount=None, market_id=None, start_ms=None, end_ms=None, page_size=None, sort="DESC", config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_hourly_unsettled_funding_pages(
    py: Python<'_>,
    max_pages: usize,
    wallet: Option<String>,
    subaccount: Option<String>,
    market_id: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    sort: &str,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<Vec<DeepXHourlyUnsettledFundingRecord>>> {
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXHourlyUnsettledFundingRequest {
        subaccount,
        wallet,
        market_id,
        start_ms,
        end_ms,
        cursor: None,
        page_size,
        sort,
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
    py.detach(move || {
        runtime.block_on(client.get_hourly_unsettled_funding_pages(&request, max_pages))
    })
    .map_err(to_pyvalue_err)
}
