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

//! Read-only Python liquidation history with exact raw chain integers.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountPage, DeepXAccountSortOrder, DeepXHttpClient, DeepXLiquidationRecord,
        DeepXLiquidationRecordsRequest, DeepXLiquidationType,
    },
};

/// One validated page of raw-unit liquidation records.
#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXLiquidationRecordsPage {
    items: Vec<DeepXLiquidationRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXLiquidationRecordsPage {
    #[getter]
    fn items(&self) -> Vec<DeepXLiquidationRecord> {
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

impl From<DeepXAccountPage<DeepXLiquidationRecord>> for DeepXLiquidationRecordsPage {
    fn from(page: DeepXAccountPage<DeepXLiquidationRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXLiquidationRecord {
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    #[getter]
    fn liquidation_id(&self) -> u64 {
        self.liquidation_id
    }

    #[getter]
    fn liquidation_type(&self) -> &'static str {
        self.liquidation_type.as_str()
    }

    #[getter]
    fn liquidation_detail_json(&self) -> &str {
        &self.liquidation_detail
    }

    #[getter]
    fn tx_hash(&self) -> Option<&str> {
        self.tx_hash.as_deref()
    }

    #[getter]
    fn target_account(&self) -> &str {
        &self.target_account
    }

    #[getter]
    fn liquidator(&self) -> &str {
        &self.liquidator
    }

    #[getter]
    fn margin_shortage(&self) -> String {
        self.margin_shortage.to_string()
    }

    #[getter]
    fn canceled_order_ids_json(&self) -> &str {
        &self.canceled_order_ids
    }

    #[getter]
    fn margin_freed(&self) -> String {
        self.margin_freed.to_string()
    }

    #[getter]
    fn market_index(&self) -> Option<u64> {
        self.market_index
    }

    #[getter]
    fn liquidator_fee(&self) -> Option<String> {
        self.liquidator_fee.map(|value| value.to_string())
    }

    #[getter]
    fn if_fee(&self) -> String {
        self.if_fee.to_string()
    }

    #[getter]
    fn liquidate_base_amount(&self) -> Option<String> {
        self.liquidate_base_amount.map(|value| value.to_string())
    }

    #[getter]
    fn oracle_price(&self) -> Option<String> {
        self.oracle_price.map(|value| value.to_string())
    }

    #[getter]
    fn liquidator_order_id(&self) -> Option<u64> {
        self.liquidator_order_id
    }

    #[getter]
    fn target_account_order_id(&self) -> Option<u64> {
        self.target_account_order_id
    }

    #[getter]
    fn borrow_amount(&self) -> Option<String> {
        self.borrow_amount.map(|value| value.to_string())
    }

    #[getter]
    fn bankrupt(&self) -> bool {
        self.bankrupt
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
    fn created_at(&self) -> &str {
        &self.created_at
    }
}

fn liquidation_request(
    wallet: Option<String>,
    subaccount: Option<String>,
    liquidation_types: Option<Vec<String>>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
) -> PyResult<DeepXLiquidationRecordsRequest> {
    let liquidation_types = liquidation_types
        .unwrap_or_default()
        .into_iter()
        .map(|value| {
            serde_json::from_value::<DeepXLiquidationType>(serde_json::Value::String(value))
                .map_err(|_| to_pyvalue_err("unknown liquidation type"))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXLiquidationRecordsRequest {
        subaccount,
        wallet,
        liquidation_types,
        cursor,
        sort,
        page_size,
    };
    request.validate().map_err(to_pyvalue_err)?;
    Ok(request)
}

/// Returns one validated liquidation-history page without credentials.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_liquidation_records", signature = (*, wallet=None, subaccount=None, liquidation_types=None, cursor=None, sort="DESC", page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_liquidation_records(
    py: Python<'_>,
    wallet: Option<String>,
    subaccount: Option<String>,
    liquidation_types: Option<Vec<String>>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXLiquidationRecordsPage> {
    let request = liquidation_request(
        wallet,
        subaccount,
        liquidation_types,
        cursor,
        sort,
        page_size,
    )?;
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
    py.detach(move || runtime.block_on(client.get_liquidation_records(&request)))
        .map(Into::into)
        .map_err(to_pyvalue_err)
}

/// Returns all validated liquidation-history pages within an explicit budget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_liquidation_record_pages", signature = (*, max_pages, wallet=None, subaccount=None, liquidation_types=None, cursor=None, sort="DESC", page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_liquidation_record_pages(
    py: Python<'_>,
    max_pages: usize,
    wallet: Option<String>,
    subaccount: Option<String>,
    liquidation_types: Option<Vec<String>>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXLiquidationRecordsPage>> {
    let request = liquidation_request(
        wallet,
        subaccount,
        liquidation_types,
        cursor,
        sort,
        page_size,
    )?;
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
    py.detach(move || runtime.block_on(client.get_liquidation_record_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}
