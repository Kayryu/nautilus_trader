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

//! Read-only Python perpetual account and wallet trade observations.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountPage, DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpAccountTradeRecord,
        DeepXPerpAccountTradesRequest, DeepXPerpWalletSubaccountTrades, DeepXPerpWalletTradeMarket,
        DeepXPerpWalletTradesRequest,
    },
};

#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXPerpAccountTradesPage {
    items: Vec<DeepXPerpAccountTradeRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpAccountTradesPage {
    #[getter]
    fn items(&self) -> Vec<DeepXPerpAccountTradeRecord> {
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

impl From<DeepXAccountPage<DeepXPerpAccountTradeRecord>> for DeepXPerpAccountTradesPage {
    fn from(page: DeepXAccountPage<DeepXPerpAccountTradeRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

/// One validated page of perpetual account trades.
#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXPerpWalletTradesPage {
    items: Vec<DeepXPerpAccountTradeRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpWalletTradesPage {
    #[getter]
    fn items(&self) -> Vec<DeepXPerpAccountTradeRecord> {
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

impl From<DeepXAccountPage<DeepXPerpAccountTradeRecord>> for DeepXPerpWalletTradesPage {
    fn from(page: DeepXAccountPage<DeepXPerpAccountTradeRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpAccountTradeRecord {
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    #[getter]
    fn market_id(&self) -> u64 {
        self.market_id
    }

    #[getter]
    fn order_id(&self) -> &str {
        &self.order_id
    }

    #[getter]
    fn is_long(&self) -> bool {
        self.is_long
    }

    #[getter]
    fn taker(&self) -> &str {
        &self.taker
    }

    #[getter]
    fn price(&self) -> String {
        self.price.to_string()
    }

    #[getter]
    fn size(&self) -> String {
        self.size.to_string()
    }

    #[getter]
    fn leverage(&self) -> String {
        self.leverage.to_string()
    }

    #[getter]
    fn created_at(&self) -> &str {
        &self.created_at
    }

    #[getter]
    fn filled_direction(&self) -> &str {
        &self.filled_direction
    }

    #[getter]
    fn fee(&self) -> String {
        self.fee.to_string()
    }

    #[getter]
    fn fee_asset(&self) -> &str {
        &self.fee_asset
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpWalletSubaccountTrades {
    #[getter]
    fn subaccount(&self) -> &str {
        &self.subaccount
    }

    #[getter]
    fn trades(&self) -> DeepXPerpWalletTradesPage {
        self.trades.clone().into()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpWalletTradeMarket {
    #[getter]
    fn market_id(&self) -> u64 {
        self.market_id
    }

    #[getter]
    fn market_name(&self) -> &str {
        &self.market_name
    }

    #[getter]
    fn subaccounts(&self) -> Vec<DeepXPerpWalletSubaccountTrades> {
        self.subaccounts.clone()
    }
}

#[expect(clippy::too_many_arguments)]
fn wallet_trades_request(
    wallet: String,
    market_name: Option<String>,
    market_id: Option<u64>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
) -> PyResult<DeepXPerpWalletTradesRequest> {
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXPerpWalletTradesRequest {
        wallet,
        market_name,
        market_id,
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
fn account_trades_request(
    subaccount: String,
    order_id: Option<String>,
    market_id: Option<u64>,
    is_long: Option<bool>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
) -> PyResult<DeepXPerpAccountTradesRequest> {
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXPerpAccountTradesRequest {
        subaccount,
        order_id,
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

/// Returns one validated, mutable perpetual account-trade page for a subaccount.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_account_trades", signature = (subaccount, order_id=None, market_id=None, is_long=None, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_account_trades(
    py: Python<'_>,
    subaccount: String,
    order_id: Option<String>,
    market_id: Option<u64>,
    is_long: Option<bool>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXPerpAccountTradesPage> {
    let request = account_trades_request(
        subaccount, order_id, market_id, is_long, cursor, sort, start_ms, end_ms, page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_account_trades(&request)))
        .map(Into::into)
        .map_err(to_pyvalue_err)
}

/// Returns validated perpetual account-trade pages within an explicit page budget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_account_trade_pages", signature = (subaccount, max_pages, order_id=None, market_id=None, is_long=None, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_account_trade_pages(
    py: Python<'_>,
    subaccount: String,
    max_pages: usize,
    order_id: Option<String>,
    market_id: Option<u64>,
    is_long: Option<bool>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXPerpAccountTradesPage>> {
    let request = account_trades_request(
        subaccount, order_id, market_id, is_long, cursor, sort, start_ms, end_ms, page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_account_trade_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}

/// Returns one validated market-grouped wallet trade response without credentials.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_wallet_trades", signature = (wallet, market_name=None, market_id=None, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_wallet_trades(
    py: Python<'_>,
    wallet: String,
    market_name: Option<String>,
    market_id: Option<u64>,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXPerpWalletTradeMarket>> {
    let request = wallet_trades_request(
        wallet,
        market_name,
        market_id,
        cursor,
        sort,
        start_ms,
        end_ms,
        page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_wallet_trades(&request)))
        .map_err(to_pyvalue_err)
}

/// Returns bounded trade pages for one wallet market and subaccount group.
///
/// The constituent REST pages are mutable and do not establish a block-pinned snapshot.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_wallet_trade_pages", signature = (wallet, subaccount, market_id, market_name, max_pages, cursor=None, sort="DESC", start_ms=None, end_ms=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_wallet_trade_pages(
    py: Python<'_>,
    wallet: String,
    subaccount: String,
    market_id: u64,
    market_name: String,
    max_pages: usize,
    cursor: Option<String>,
    sort: &str,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXPerpWalletTradesPage>> {
    let request = wallet_trades_request(
        wallet,
        None,
        Some(market_id),
        cursor,
        sort,
        start_ms,
        end_ms,
        page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || {
        runtime.block_on(client.get_perp_wallet_trade_pages_for_subaccount(
            &request,
            &subaccount,
            &market_name,
            max_pages,
        ))
    })
    .map(|pages| pages.into_iter().map(Into::into).collect())
    .map_err(to_pyvalue_err)
}
