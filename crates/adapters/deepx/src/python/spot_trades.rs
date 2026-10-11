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

//! Read-only Python Spot account and wallet trade observations.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountPage, DeepXAccountSortOrder, DeepXHttpClient, DeepXSpotAccountTradeRecord,
        DeepXSpotAccountTradesRequest, DeepXSpotOrderSide, DeepXSpotWalletSubaccountTrades,
        DeepXSpotWalletTradeMarket, DeepXSpotWalletTradesPage, DeepXSpotWalletTradesRequest,
    },
};

#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXSpotAccountTradesPage {
    items: Vec<DeepXSpotAccountTradeRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotAccountTradesPage {
    #[getter]
    fn items(&self) -> Vec<DeepXSpotAccountTradeRecord> {
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

impl From<DeepXAccountPage<DeepXSpotAccountTradeRecord>> for DeepXSpotAccountTradesPage {
    fn from(page: DeepXAccountPage<DeepXSpotAccountTradeRecord>) -> Self {
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
pub(super) struct DeepXSpotWalletTradeItemsPage {
    items: Vec<DeepXSpotAccountTradeRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotWalletTradeItemsPage {
    #[getter]
    fn items(&self) -> Vec<DeepXSpotAccountTradeRecord> {
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

impl From<DeepXAccountPage<DeepXSpotAccountTradeRecord>> for DeepXSpotWalletTradeItemsPage {
    fn from(page: DeepXAccountPage<DeepXSpotAccountTradeRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotAccountTradeRecord {
    #[getter]
    fn id(&self) -> u64 {
        self.id
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
    fn pair(&self) -> &str {
        &self.pair
    }

    #[getter]
    fn pair_name(&self) -> &str {
        &self.pair_name
    }

    #[getter]
    fn base_amount(&self) -> String {
        self.base_amount.to_string()
    }

    #[getter]
    fn quote_amount(&self) -> String {
        self.quote_amount.to_string()
    }

    #[getter]
    fn price(&self) -> String {
        self.price.to_string()
    }

    #[getter]
    fn fee(&self) -> String {
        self.fee.to_string()
    }

    #[getter]
    fn fee_asset(&self) -> &str {
        &self.fee_asset
    }

    #[getter]
    fn taker(&self) -> &str {
        &self.taker
    }

    #[getter]
    fn created_at(&self) -> &str {
        &self.created_at
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotWalletSubaccountTrades {
    #[getter]
    fn subaccount(&self) -> &str {
        &self.subaccount
    }

    #[getter]
    fn trades(&self) -> DeepXSpotWalletTradeItemsPage {
        self.trades.clone().into()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotWalletTradeMarket {
    #[getter]
    fn pair(&self) -> &str {
        &self.pair
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn subaccounts(&self) -> Vec<DeepXSpotWalletSubaccountTrades> {
        self.subaccounts.clone()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSpotWalletTradesPage {
    #[getter]
    fn markets(&self) -> Vec<DeepXSpotWalletTradeMarket> {
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
fn spot_wallet_trades_request(
    wallet: String,
    name: Option<String>,
    pair: Option<String>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
) -> PyResult<DeepXSpotWalletTradesRequest> {
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXSpotWalletTradesRequest {
        wallet,
        name,
        pair,
        cursor,
        sort,
        start_ms,
        end_ms,
        page_size,
    };
    request.validate().map_err(to_pyvalue_err)?;
    Ok(request)
}

#[expect(clippy::too_many_arguments)]
fn spot_account_trades_request(
    subaccount: String,
    order_id: Option<String>,
    order_side: Option<&str>,
    name: Option<String>,
    pair: Option<String>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
) -> PyResult<DeepXSpotAccountTradesRequest> {
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
    let request = DeepXSpotAccountTradesRequest {
        subaccount,
        order_id,
        order_side,
        name,
        pair,
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

/// Returns one validated, mutable Spot trade page for a subaccount.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_spot_account_trades", signature = (subaccount, order_id=None, order_side=None, name=None, pair=None, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_spot_account_trades(
    py: Python<'_>,
    subaccount: String,
    order_id: Option<String>,
    order_side: Option<&str>,
    name: Option<String>,
    pair: Option<String>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXSpotAccountTradesPage> {
    let request = spot_account_trades_request(
        subaccount, order_id, order_side, name, pair, cursor, sort, start_ms, end_ms, page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_spot_account_trades(&request)))
        .map(Into::into)
        .map_err(to_pyvalue_err)
}

/// Returns validated Spot account-trade pages within an explicit page budget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_spot_account_trade_pages", signature = (subaccount, max_pages, order_id=None, order_side=None, name=None, pair=None, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_spot_account_trade_pages(
    py: Python<'_>,
    subaccount: String,
    max_pages: usize,
    order_id: Option<String>,
    order_side: Option<&str>,
    name: Option<String>,
    pair: Option<String>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXSpotAccountTradesPage>> {
    let request = spot_account_trades_request(
        subaccount, order_id, order_side, name, pair, cursor, sort, start_ms, end_ms, page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_spot_account_trade_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}

/// Returns one validated grouped Spot wallet-trade page without credentials.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_spot_wallet_trades", signature = (wallet, name=None, pair=None, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_spot_wallet_trades(
    py: Python<'_>,
    wallet: String,
    name: Option<String>,
    pair: Option<String>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXSpotWalletTradesPage> {
    let request = spot_wallet_trades_request(
        wallet, name, pair, cursor, sort, start_ms, end_ms, page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_spot_wallet_trades(&request)))
        .map_err(to_pyvalue_err)
}

/// Returns validated grouped Spot wallet-trade pages within a page budget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_spot_wallet_trade_pages", signature = (wallet, max_pages, name=None, pair=None, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_spot_wallet_trade_pages(
    py: Python<'_>,
    wallet: String,
    max_pages: usize,
    name: Option<String>,
    pair: Option<String>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXSpotWalletTradesPage>> {
    let request = spot_wallet_trades_request(
        wallet, name, pair, cursor, sort, start_ms, end_ms, page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_spot_wallet_trade_pages(&request, max_pages)))
        .map_err(to_pyvalue_err)
}
