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

//! Read-only Python wallet-grouped perpetual order observations.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountPage, DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpHistoryOrdersRequest,
        DeepXPerpOpenOrdersRequest, DeepXPerpOrderRecord, DeepXPerpWalletOrderMarket,
        DeepXPerpWalletOrdersPage, DeepXPerpWalletOrdersRequest, DeepXPerpWalletSubaccountOrders,
    },
};

#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXPerpOpenOrdersPage {
    items: Vec<DeepXPerpOrderRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpOpenOrdersPage {
    #[getter]
    fn items(&self) -> Vec<DeepXPerpOrderRecord> {
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

impl From<DeepXAccountPage<DeepXPerpOrderRecord>> for DeepXPerpOpenOrdersPage {
    fn from(page: DeepXAccountPage<DeepXPerpOrderRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXPerpHistoryOrdersPage {
    items: Vec<DeepXPerpOrderRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpHistoryOrdersPage {
    #[getter]
    fn items(&self) -> Vec<DeepXPerpOrderRecord> {
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

impl From<DeepXAccountPage<DeepXPerpOrderRecord>> for DeepXPerpHistoryOrdersPage {
    fn from(page: DeepXAccountPage<DeepXPerpOrderRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXPerpWalletOrderItemsPage {
    items: Vec<DeepXPerpOrderRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpWalletOrderItemsPage {
    #[getter]
    fn items(&self) -> Vec<DeepXPerpOrderRecord> {
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

impl From<DeepXAccountPage<DeepXPerpOrderRecord>> for DeepXPerpWalletOrderItemsPage {
    fn from(page: DeepXAccountPage<DeepXPerpOrderRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpOrderRecord {
    #[getter]
    fn order_id(&self) -> &str {
        &self.order_id
    }
    #[getter]
    fn owner(&self) -> &str {
        &self.owner
    }
    #[getter]
    fn market_id(&self) -> u64 {
        self.market_id
    }
    #[getter]
    fn is_long(&self) -> bool {
        self.is_long
    }
    #[getter]
    fn size(&self) -> String {
        self.size.to_string()
    }
    #[getter]
    fn price(&self) -> String {
        self.price.to_string()
    }
    #[getter]
    fn avg_fill_price(&self) -> Option<String> {
        self.avg_fill_price.map(|value| value.to_string())
    }
    #[getter]
    fn order_type(&self) -> &str {
        &self.order_type
    }
    #[getter]
    fn create_time(&self) -> &str {
        &self.create_time
    }
    #[getter]
    fn updated_time(&self) -> Option<&str> {
        self.updated_time.as_deref()
    }
    #[getter]
    fn leverage(&self) -> String {
        self.leverage.to_string()
    }
    #[getter]
    fn slippage(&self) -> String {
        self.slippage.to_string()
    }
    #[getter]
    fn status(&self) -> &str {
        &self.status
    }
    #[getter]
    fn size_filled(&self) -> String {
        self.size_filled.to_string()
    }
    #[getter]
    fn size_remain(&self) -> String {
        self.size_remain.to_string()
    }
    #[getter]
    fn take_profit(&self) -> Option<String> {
        self.take_profit.map(|value| value.to_string())
    }
    #[getter]
    fn stop_loss(&self) -> Option<String> {
        self.stop_loss.map(|value| value.to_string())
    }
    #[getter]
    fn reduce_only(&self) -> bool {
        self.reduce_only
    }
    #[getter]
    fn post_only(&self) -> &str {
        &self.post_only
    }
    #[getter]
    fn height(&self) -> u64 {
        self.height
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
    fn cancel_reason(&self) -> &str {
        &self.cancel_reason
    }
    #[getter]
    fn cancel_height(&self) -> Option<u64> {
        self.cancel_height
    }
    #[getter]
    fn fee(&self) -> String {
        self.fee.to_string()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpWalletSubaccountOrders {
    #[getter]
    fn subaccount(&self) -> &str {
        &self.subaccount
    }

    #[getter]
    fn orders(&self) -> DeepXPerpWalletOrderItemsPage {
        self.orders.clone().into()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpWalletOrderMarket {
    #[getter]
    fn market_id(&self) -> u64 {
        self.market_id
    }

    #[getter]
    fn market_name(&self) -> &str {
        &self.market_name
    }

    #[getter]
    fn subaccounts(&self) -> Vec<DeepXPerpWalletSubaccountOrders> {
        self.subaccounts.clone()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpWalletOrdersPage {
    #[getter]
    fn markets(&self) -> Vec<DeepXPerpWalletOrderMarket> {
        self.markets.clone()
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

#[expect(clippy::too_many_arguments)]
fn wallet_orders_request(
    wallet: String,
    market_name: Option<String>,
    market_id: Option<u64>,
    is_long: Option<bool>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
) -> PyResult<DeepXPerpWalletOrdersRequest> {
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXPerpWalletOrdersRequest {
        wallet,
        market_name,
        market_id,
        is_long,
        cursor,
        sort,
        start_ms,
        end_ms,
        page_size,
    };
    request.validate().map_err(to_pyvalue_err)?;
    Ok(request)
}

fn client_and_runtime(
    config: Option<DeepXDataClientConfig>,
) -> PyResult<(DeepXHttpClient, tokio::runtime::Runtime)> {
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
    Ok((client, runtime))
}

fn open_orders_request(
    subaccount: String,
    market_id: u64,
    is_long: Option<bool>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
) -> PyResult<DeepXPerpOpenOrdersRequest> {
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXPerpOpenOrdersRequest {
        subaccount,
        market_id: Some(market_id),
        is_long,
        cursor,
        page_size,
        sort,
    };
    request.validate().map_err(to_pyvalue_err)?;
    Ok(request)
}

fn history_orders_request(
    subaccount: String,
    market_id: Option<u64>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
) -> PyResult<DeepXPerpHistoryOrdersRequest> {
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXPerpHistoryOrdersRequest {
        subaccount,
        market_id,
        cursor,
        page_size,
        sort,
    };
    request.validate().map_err(to_pyvalue_err)?;
    Ok(request)
}

/// Returns one validated, mutable perpetual history-order page for a subaccount.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_history_orders", signature = (subaccount, market_id=None, cursor=None, sort="DESC", page_size=None, config=None))]
pub(super) fn py_get_deepx_perp_history_orders(
    py: Python<'_>,
    subaccount: String,
    market_id: Option<u64>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXPerpHistoryOrdersPage> {
    let request = history_orders_request(subaccount, market_id, cursor, sort, page_size)?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_history_orders(&request)))
        .map(Into::into)
        .map_err(to_pyvalue_err)
}

/// Returns validated perpetual history-order pages within an explicit page budget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_history_order_pages", signature = (subaccount, max_pages, market_id=None, cursor=None, sort="DESC", page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_history_order_pages(
    py: Python<'_>,
    subaccount: String,
    max_pages: usize,
    market_id: Option<u64>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXPerpHistoryOrdersPage>> {
    let request = history_orders_request(subaccount, market_id, cursor, sort, page_size)?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_history_order_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}

/// Returns one validated, mutable perpetual open-order page for a subaccount and market.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_open_orders", signature = (subaccount, market_id, is_long=None, cursor=None, sort="DESC", page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_open_orders(
    py: Python<'_>,
    subaccount: String,
    market_id: u64,
    is_long: Option<bool>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXPerpOpenOrdersPage> {
    let request = open_orders_request(subaccount, market_id, is_long, cursor, sort, page_size)?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_open_orders(&request)))
        .map(Into::into)
        .map_err(to_pyvalue_err)
}

/// Returns validated perpetual open-order pages within an explicit page budget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_open_order_pages", signature = (subaccount, market_id, max_pages, is_long=None, cursor=None, sort="DESC", page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_open_order_pages(
    py: Python<'_>,
    subaccount: String,
    market_id: u64,
    max_pages: usize,
    is_long: Option<bool>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXPerpOpenOrdersPage>> {
    let request = open_orders_request(subaccount, market_id, is_long, cursor, sort, page_size)?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_open_order_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}

/// Returns one validated grouped perpetual wallet-order page without credentials.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_wallet_orders", signature = (wallet, market_name=None, market_id=None, is_long=None, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_wallet_orders(
    py: Python<'_>,
    wallet: String,
    market_name: Option<String>,
    market_id: Option<u64>,
    is_long: Option<bool>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXPerpWalletOrdersPage> {
    let request = wallet_orders_request(
        wallet,
        market_name,
        market_id,
        is_long,
        cursor,
        sort,
        start_ms,
        end_ms,
        page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_wallet_orders(&request)))
        .map_err(to_pyvalue_err)
}

/// Returns bounded validated grouped perpetual wallet-order pages.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_wallet_order_pages", signature = (wallet, max_pages, market_name=None, market_id=None, is_long=None, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_wallet_order_pages(
    py: Python<'_>,
    wallet: String,
    max_pages: usize,
    market_name: Option<String>,
    market_id: Option<u64>,
    is_long: Option<bool>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXPerpWalletOrdersPage>> {
    let request = wallet_orders_request(
        wallet,
        market_name,
        market_id,
        is_long,
        cursor,
        sort,
        start_ms,
        end_ms,
        page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_wallet_order_pages(&request, max_pages)))
        .map_err(to_pyvalue_err)
}
