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

//! Read-only Python wallet-grouped Spot order observations.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountPage, DeepXAccountSortOrder, DeepXHttpClient, DeepXSpotHistoryOrdersRequest,
        DeepXSpotOpenOrdersRequest, DeepXSpotOrderRecord, DeepXSpotOrderSide,
        DeepXSpotWalletOrderMarket, DeepXSpotWalletOrdersPage, DeepXSpotWalletOrdersRequest,
        DeepXSpotWalletSubaccountOrders,
    },
};

#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXSpotOpenOrdersPage {
    items: Vec<DeepXSpotOrderRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotOpenOrdersPage {
    #[getter]
    fn items(&self) -> Vec<DeepXSpotOrderRecord> {
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

impl From<DeepXAccountPage<DeepXSpotOrderRecord>> for DeepXSpotOpenOrdersPage {
    fn from(page: DeepXAccountPage<DeepXSpotOrderRecord>) -> Self {
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
pub(super) struct DeepXSpotHistoryOrdersPage {
    items: Vec<DeepXSpotOrderRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotHistoryOrdersPage {
    #[getter]
    fn items(&self) -> Vec<DeepXSpotOrderRecord> {
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

impl From<DeepXAccountPage<DeepXSpotOrderRecord>> for DeepXSpotHistoryOrdersPage {
    fn from(page: DeepXAccountPage<DeepXSpotOrderRecord>) -> Self {
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
pub(super) struct DeepXSpotWalletOrderItemsPage {
    items: Vec<DeepXSpotOrderRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotWalletOrderItemsPage {
    #[getter]
    fn items(&self) -> Vec<DeepXSpotOrderRecord> {
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

impl From<DeepXAccountPage<DeepXSpotOrderRecord>> for DeepXSpotWalletOrderItemsPage {
    fn from(page: DeepXAccountPage<DeepXSpotOrderRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotOrderRecord {
    #[getter]
    fn pair(&self) -> &str {
        &self.pair
    }
    #[getter]
    fn pair_name(&self) -> &str {
        &self.pair_name
    }
    #[getter]
    fn order_id(&self) -> &str {
        &self.order_id
    }
    #[getter]
    fn order_side(&self) -> &str {
        &self.order_side
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
    fn slippage(&self) -> String {
        self.slippage.to_string()
    }
    #[getter]
    fn create_time(&self) -> &str {
        &self.create_time
    }
    #[getter]
    fn block_number(&self) -> u64 {
        self.block_number
    }
    #[getter]
    fn maker(&self) -> &str {
        &self.maker
    }
    #[getter]
    fn status(&self) -> &str {
        &self.status
    }
    #[getter]
    fn price_type(&self) -> &str {
        &self.price_type
    }
    #[getter]
    fn base_amount(&self) -> String {
        self.base_amount.to_string()
    }
    #[getter]
    fn base_remaining_amount(&self) -> String {
        self.base_remaining_amount.to_string()
    }
    #[getter]
    fn quote_amount(&self) -> String {
        self.quote_amount.to_string()
    }
    #[getter]
    fn quote_remaining_amount(&self) -> String {
        self.quote_remaining_amount.to_string()
    }
    #[getter]
    fn post_only(&self) -> &str {
        &self.post_only
    }
    #[getter]
    fn reduce_only(&self) -> bool {
        self.reduce_only
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
    fn fee(&self) -> String {
        self.fee.to_string()
    }
    #[getter]
    fn cancel_reason(&self) -> Option<&str> {
        self.cancel_reason.as_deref()
    }
    #[getter]
    fn cancel_height(&self) -> Option<u64> {
        self.cancel_height
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotWalletSubaccountOrders {
    #[getter]
    fn subaccount(&self) -> &str {
        &self.subaccount
    }

    #[getter]
    fn orders(&self) -> DeepXSpotWalletOrderItemsPage {
        self.orders.clone().into()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotWalletOrderMarket {
    #[getter]
    fn pair(&self) -> &str {
        &self.pair
    }
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }
    #[getter]
    fn subaccounts(&self) -> Vec<DeepXSpotWalletSubaccountOrders> {
        self.subaccounts.clone()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotWalletOrdersPage {
    #[getter]
    fn markets(&self) -> Vec<DeepXSpotWalletOrderMarket> {
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
    name: Option<String>,
    pair: Option<String>,
    order_side: Option<&str>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
) -> PyResult<DeepXSpotWalletOrdersRequest> {
    let order_side = match order_side {
        None => None,
        Some("Buy") => Some(DeepXSpotOrderSide::Buy),
        Some("Sell") => Some(DeepXSpotOrderSide::Sell),
        _ => return Err(to_pyvalue_err("order_side must be Buy or Sell")),
    };
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXSpotWalletOrdersRequest {
        wallet: Some(wallet),
        name,
        pair,
        order_side,
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
    name: Option<String>,
    pair: Option<String>,
    order_side: Option<&str>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
) -> PyResult<DeepXSpotOpenOrdersRequest> {
    let order_side = match order_side {
        None => None,
        Some("Buy") => Some(DeepXSpotOrderSide::Buy),
        Some("Sell") => Some(DeepXSpotOrderSide::Sell),
        _ => return Err(to_pyvalue_err("order_side must be Buy or Sell")),
    };
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXSpotOpenOrdersRequest {
        subaccount,
        name,
        pair,
        order_side,
        cursor,
        sort,
        page_size,
    };
    request.validate().map_err(to_pyvalue_err)?;
    Ok(request)
}

fn history_orders_request(
    subaccount: String,
    name: Option<String>,
    pair: Option<String>,
    order_side: Option<&str>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
) -> PyResult<DeepXSpotHistoryOrdersRequest> {
    let order_side = match order_side {
        None => None,
        Some("Buy") => Some(DeepXSpotOrderSide::Buy),
        Some("Sell") => Some(DeepXSpotOrderSide::Sell),
        _ => return Err(to_pyvalue_err("order_side must be Buy or Sell")),
    };
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXSpotHistoryOrdersRequest {
        subaccount,
        name,
        pair,
        order_side,
        cursor,
        sort,
        page_size,
    };
    request.validate().map_err(to_pyvalue_err)?;
    Ok(request)
}

/// Returns one validated, mutable Spot history-order page for a subaccount.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_spot_history_orders", signature = (subaccount, name=None, pair=None, order_side=None, cursor=None, sort="DESC", page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_spot_history_orders(
    py: Python<'_>,
    subaccount: String,
    name: Option<String>,
    pair: Option<String>,
    order_side: Option<&str>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXSpotHistoryOrdersPage> {
    let request =
        history_orders_request(subaccount, name, pair, order_side, cursor, sort, page_size)?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_spot_history_orders(&request)))
        .map(Into::into)
        .map_err(to_pyvalue_err)
}

/// Returns validated Spot history-order pages within an explicit page budget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_spot_history_order_pages", signature = (subaccount, max_pages, name=None, pair=None, order_side=None, cursor=None, sort="DESC", page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_spot_history_order_pages(
    py: Python<'_>,
    subaccount: String,
    max_pages: usize,
    name: Option<String>,
    pair: Option<String>,
    order_side: Option<&str>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXSpotHistoryOrdersPage>> {
    let request =
        history_orders_request(subaccount, name, pair, order_side, cursor, sort, page_size)?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_spot_history_order_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}

/// Returns one validated, mutable Spot open-order page for a subaccount and market.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_spot_open_orders", signature = (subaccount, name=None, pair=None, order_side=None, cursor=None, sort="DESC", page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_spot_open_orders(
    py: Python<'_>,
    subaccount: String,
    name: Option<String>,
    pair: Option<String>,
    order_side: Option<&str>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXSpotOpenOrdersPage> {
    let request = open_orders_request(subaccount, name, pair, order_side, cursor, sort, page_size)?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_spot_open_orders(&request)))
        .map(Into::into)
        .map_err(to_pyvalue_err)
}

/// Returns validated Spot open-order pages within an explicit page budget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_spot_open_order_pages", signature = (subaccount, max_pages, name=None, pair=None, order_side=None, cursor=None, sort="DESC", page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_spot_open_order_pages(
    py: Python<'_>,
    subaccount: String,
    max_pages: usize,
    name: Option<String>,
    pair: Option<String>,
    order_side: Option<&str>,
    cursor: Option<String>,
    sort: &str,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXSpotOpenOrdersPage>> {
    let request = open_orders_request(subaccount, name, pair, order_side, cursor, sort, page_size)?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_spot_open_order_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}

/// Returns one validated grouped Spot wallet-order page without credentials.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_spot_wallet_orders", signature = (wallet, name=None, pair=None, order_side=None, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_spot_wallet_orders(
    py: Python<'_>,
    wallet: String,
    name: Option<String>,
    pair: Option<String>,
    order_side: Option<&str>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXSpotWalletOrdersPage> {
    let request = wallet_orders_request(
        wallet, name, pair, order_side, cursor, sort, start_ms, end_ms, page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_spot_wallet_orders(&request)))
        .map_err(to_pyvalue_err)
}

/// Returns complete validated grouped Spot wallet-order pages within a page budget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_spot_wallet_order_pages", signature = (wallet, max_pages, name=None, pair=None, order_side=None, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_spot_wallet_order_pages(
    py: Python<'_>,
    wallet: String,
    max_pages: usize,
    name: Option<String>,
    pair: Option<String>,
    order_side: Option<&str>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXSpotWalletOrdersPage>> {
    let request = wallet_orders_request(
        wallet, name, pair, order_side, cursor, sort, start_ms, end_ms, page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_spot_wallet_order_pages(&request, max_pages)))
        .map_err(to_pyvalue_err)
}
