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

//! Read-only Python subaccount and wallet funding-fee observations.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountPage, DeepXHttpClient, DeepXPerpFundingFeeRecord, DeepXPerpFundingFeeRequest,
        DeepXWalletFundingFeeRequest,
    },
};

#[derive(Clone)]
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXPerpFundingFeesPage {
    items: Vec<DeepXPerpFundingFeeRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpFundingFeesPage {
    #[getter]
    fn items(&self) -> Vec<DeepXPerpFundingFeeRecord> {
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

impl From<DeepXAccountPage<DeepXPerpFundingFeeRecord>> for DeepXPerpFundingFeesPage {
    fn from(page: DeepXAccountPage<DeepXPerpFundingFeeRecord>) -> Self {
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
pub(super) struct DeepXWalletFundingFeesPage {
    items: Vec<DeepXPerpFundingFeeRecord>,
    next_cursor: Option<String>,
    has_next: bool,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXWalletFundingFeesPage {
    #[getter]
    fn items(&self) -> Vec<DeepXPerpFundingFeeRecord> {
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

impl From<DeepXAccountPage<DeepXPerpFundingFeeRecord>> for DeepXWalletFundingFeesPage {
    fn from(page: DeepXAccountPage<DeepXPerpFundingFeeRecord>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpFundingFeeRecord {
    #[getter]
    fn owner(&self) -> &str {
        &self.owner
    }

    #[getter]
    fn market(&self) -> u64 {
        self.market
    }

    #[getter]
    fn position_size(&self) -> String {
        self.position_size.to_string()
    }

    #[getter]
    fn is_long(&self) -> bool {
        self.is_long
    }

    #[getter]
    fn fee(&self) -> String {
        self.fee.to_string()
    }

    #[getter]
    fn fee_rate(&self) -> String {
        self.fee_rate.to_string()
    }

    #[getter]
    fn created_at(&self) -> &str {
        &self.created_at
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
    fn is_settled(&self) -> bool {
        self.is_settled
    }
}

fn wallet_funding_fee_request(
    wallet: String,
    market_name: Option<String>,
    market_id: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    cursor: Option<String>,
    page_size: Option<u32>,
) -> PyResult<DeepXWalletFundingFeeRequest> {
    let request = DeepXWalletFundingFeeRequest {
        wallet,
        market_name,
        market_id,
        start_ms,
        end_ms,
        cursor,
        page_size,
    };
    request.validate().map_err(to_pyvalue_err)?;
    Ok(request)
}

fn perp_funding_fee_request(
    subaccount: String,
    market_id: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    cursor: Option<String>,
    page_size: Option<u32>,
) -> PyResult<DeepXPerpFundingFeeRequest> {
    let request = DeepXPerpFundingFeeRequest {
        subaccount,
        market_id,
        start_ms,
        end_ms,
        cursor,
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

/// Returns one validated, mutable funding-fee page for a subaccount.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_funding_fees", signature = (subaccount, market_id=None, start_ms=None, end_ms=None, cursor=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_funding_fees(
    py: Python<'_>,
    subaccount: String,
    market_id: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    cursor: Option<String>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXPerpFundingFeesPage> {
    let request =
        perp_funding_fee_request(subaccount, market_id, start_ms, end_ms, cursor, page_size)?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_funding_fees(&request)))
        .map(Into::into)
        .map_err(to_pyvalue_err)
}

/// Returns validated subaccount funding-fee pages within an explicit page budget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_funding_fee_pages", signature = (subaccount, max_pages, market_id=None, start_ms=None, end_ms=None, cursor=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_perp_funding_fee_pages(
    py: Python<'_>,
    subaccount: String,
    max_pages: usize,
    market_id: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    cursor: Option<String>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXPerpFundingFeesPage>> {
    let request =
        perp_funding_fee_request(subaccount, market_id, start_ms, end_ms, cursor, page_size)?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_funding_fee_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}

/// Returns one validated wallet funding-fee page without credentials.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_wallet_funding_fees", signature = (wallet, market_name=None, market_id=None, start_ms=None, end_ms=None, cursor=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_wallet_funding_fees(
    py: Python<'_>,
    wallet: String,
    market_name: Option<String>,
    market_id: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    cursor: Option<String>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXWalletFundingFeesPage> {
    let request = wallet_funding_fee_request(
        wallet,
        market_name,
        market_id,
        start_ms,
        end_ms,
        cursor,
        page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_wallet_funding_fees(&request)))
        .map(Into::into)
        .map_err(to_pyvalue_err)
}

/// Returns complete validated wallet funding-fee pages within a page budget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_wallet_funding_fee_pages", signature = (wallet, max_pages, market_name=None, market_id=None, start_ms=None, end_ms=None, cursor=None, page_size=None, config=None))]
#[expect(clippy::too_many_arguments)]
pub(super) fn py_get_deepx_wallet_funding_fee_pages(
    py: Python<'_>,
    wallet: String,
    max_pages: usize,
    market_name: Option<String>,
    market_id: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    cursor: Option<String>,
    page_size: Option<u32>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<Vec<DeepXWalletFundingFeesPage>> {
    let request = wallet_funding_fee_request(
        wallet,
        market_name,
        market_id,
        start_ms,
        end_ms,
        cursor,
        page_size,
    )?;
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_wallet_funding_fee_pages(&request, max_pages)))
        .map(|pages| pages.into_iter().map(Into::into).collect())
        .map_err(to_pyvalue_err)
}
