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

//! Read-only Python perpetual position lifecycle observations.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountPage, DeepXHttpClient, DeepXPerpPositionRecord, DeepXPerpPositionsRequest,
        DeepXSubaccountPerpPositions, DeepXSubaccountProfile, DeepXUserStats,
        DeepXWalletPerpPositions, DeepXWalletSubaccounts,
    },
};

/// One validated page of perpetual position lifecycles.
#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXPerpPositionsPage {
    items: Vec<DeepXPerpPositionRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpPositionsPage {
    #[getter]
    fn items(&self) -> Vec<DeepXPerpPositionRecord> {
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

impl From<DeepXAccountPage<DeepXPerpPositionRecord>> for DeepXPerpPositionsPage {
    fn from(page: DeepXAccountPage<DeepXPerpPositionRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSubaccountPerpPositions {
    #[getter]
    fn profile(&self) -> DeepXSubaccountProfile {
        self.profile.clone()
    }

    #[getter]
    fn pages(&self) -> Vec<DeepXPerpPositionsPage> {
        self.pages.iter().cloned().map(Into::into).collect()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXWalletPerpPositions {
    #[getter]
    fn directory(&self) -> DeepXWalletSubaccounts {
        self.directory.clone()
    }

    #[getter]
    fn stats(&self) -> DeepXUserStats {
        self.stats.clone()
    }

    #[getter]
    fn subaccounts(&self) -> Vec<DeepXSubaccountPerpPositions> {
        self.subaccounts.clone()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpPositionRecord {
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

    #[getter]
    fn is_long(&self) -> bool {
        self.is_long
    }

    #[getter]
    fn base_asset_amount(&self) -> String {
        self.base_asset_amount.to_string()
    }

    #[getter]
    fn entry_price(&self) -> String {
        self.entry_price.to_string()
    }

    #[getter]
    fn leverage(&self) -> String {
        self.leverage.to_string()
    }

    #[getter]
    fn last_funding_rate(&self) -> String {
        self.last_funding_rate.to_string()
    }

    #[getter]
    fn version(&self) -> u64 {
        self.version
    }

    #[getter]
    fn realized_pnl(&self) -> String {
        self.realized_pnl.to_string()
    }

    #[getter]
    fn funding_payment(&self) -> String {
        self.funding_payment.to_string()
    }

    #[getter]
    fn take_profit(&self) -> String {
        self.take_profit.to_string()
    }

    #[getter]
    fn stop_loss(&self) -> String {
        self.stop_loss.to_string()
    }

    #[getter]
    fn last_settle_price(&self) -> String {
        self.last_settle_price.to_string()
    }

    #[getter]
    fn pnl(&self) -> String {
        self.pnl.to_string()
    }

    #[getter]
    fn open_block_num(&self) -> u64 {
        self.open_block_num
    }

    #[getter]
    fn open_time(&self) -> &str {
        &self.open_time
    }

    #[getter]
    fn open_event_idx(&self) -> u64 {
        self.open_event_idx
    }

    #[getter]
    fn close_price(&self) -> Option<String> {
        self.close_price.map(|value| value.to_string())
    }

    #[getter]
    fn close_block_num(&self) -> Option<u64> {
        self.close_block_num
    }

    #[getter]
    fn close_time(&self) -> Option<&str> {
        self.close_time.as_deref()
    }

    #[getter]
    fn close_event_idx(&self) -> Option<u64> {
        self.close_event_idx
    }

    #[getter]
    fn status(&self) -> &str {
        &self.status
    }

    #[getter]
    fn total_fee(&self) -> String {
        self.total_fee.to_string()
    }

    #[getter]
    fn liquidate_price(&self) -> Option<String> {
        self.liquidate_price.map(|value| value.to_string())
    }

    #[getter]
    fn created_at(&self) -> &str {
        &self.created_at
    }

    #[getter]
    fn updated_at(&self) -> &str {
        &self.updated_at
    }
}

/// Returns one validated position-lifecycle page for a subaccount.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_positions", signature = (subaccount, market_id=None, only_closed=None, cursor=None, page_size=None, config=None))]
pub(super) fn py_get_deepx_perp_positions(
    py: Python<'_>,
    subaccount: String,
    market_id: Option<u64>,
    only_closed: Option<bool>,
    cursor: Option<String>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXPerpPositionsPage> {
    let config = config.unwrap_or_default();
    config.validate().map_err(to_pyvalue_err)?;
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )
    .map_err(to_pyvalue_err)?;
    let request = DeepXPerpPositionsRequest {
        subaccount,
        market_id,
        only_closed,
        cursor,
        page_size,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)?;
    py.detach(move || runtime.block_on(client.get_perp_positions(&request)))
        .map(Into::into)
        .map_err(to_pyvalue_err)
}

/// Returns all validated position-lifecycle pages within an explicit page budget.
///
/// Returns no partial collection if pagination or record validation fails.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_position_pages", signature = (subaccount, max_pages, market_id=None, only_closed=None, cursor=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_position_pages(
    py: Python<'_>,
    subaccount: String,
    max_pages: usize,
    market_id: Option<u64>,
    only_closed: Option<bool>,
    cursor: Option<String>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXPerpPositionsPage>> {
    let config = config.unwrap_or_default();
    config.validate().map_err(to_pyvalue_err)?;
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )
    .map_err(to_pyvalue_err)?;
    let request = DeepXPerpPositionsRequest {
        subaccount,
        market_id,
        only_closed,
        cursor,
        page_size,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)?;
    py.detach(move || runtime.block_on(client.get_perp_position_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}

/// Returns bounded validated position lifecycles for every wallet subaccount.
///
/// The constituent REST pages are mutable and do not establish a block-pinned snapshot.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_wallet_perp_positions", signature = (wallet, max_subaccounts, max_pages_per_subaccount, market_id=None, only_closed=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_wallet_perp_positions(
    py: Python<'_>,
    wallet: String,
    max_subaccounts: usize,
    max_pages_per_subaccount: usize,
    market_id: Option<u64>,
    only_closed: Option<bool>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXWalletPerpPositions> {
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
        runtime.block_on(client.get_wallet_perp_positions(
            &wallet,
            max_subaccounts,
            max_pages_per_subaccount,
            market_id,
            only_closed,
            page_size,
        ))
    })
    .map_err(to_pyvalue_err)
}
