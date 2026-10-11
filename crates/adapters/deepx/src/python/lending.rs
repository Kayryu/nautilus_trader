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

//! Read-only public lending market service for Python.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXAccountSortOrder, DeepXHttpClient, DeepXLendingAsset, DeepXLendingHistoryRequest,
        DeepXLendingInterestRateParams, DeepXLendingInterestRateRecord, DeepXLendingMarketRequest,
        DeepXLendingStatusRecord, DeepXPerpCandleInterval,
    },
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXLendingAsset {
    #[getter]
    fn market_id(&self) -> u64 {
        self.market_id
    }

    #[getter]
    fn asset(&self) -> &str {
        &self.asset
    }

    #[getter]
    fn height(&self) -> u64 {
        self.height
    }

    #[getter]
    fn created_at(&self) -> &str {
        &self.created_at
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXLendingInterestRateParams {
    #[getter]
    fn market_id(&self) -> u64 {
        self.market_id
    }

    #[getter]
    fn asset(&self) -> &str {
        &self.asset
    }

    #[getter]
    fn r_min(&self) -> String {
        self.r_min.to_string()
    }

    #[getter]
    fn u1(&self) -> String {
        self.u1.to_string()
    }

    #[getter]
    fn u2(&self) -> String {
        self.u2.to_string()
    }

    #[getter]
    fn u3(&self) -> String {
        self.u3.to_string()
    }

    #[getter]
    fn u4(&self) -> Option<String> {
        self.u4.map(|value| value.to_string())
    }

    #[getter]
    fn u5(&self) -> Option<String> {
        self.u5.map(|value| value.to_string())
    }

    #[getter]
    fn r1(&self) -> String {
        self.r1.to_string()
    }

    #[getter]
    fn r2(&self) -> String {
        self.r2.to_string()
    }

    #[getter]
    fn r3(&self) -> String {
        self.r3.to_string()
    }

    #[getter]
    fn r4(&self) -> Option<String> {
        self.r4.map(|value| value.to_string())
    }

    #[getter]
    fn r5(&self) -> Option<String> {
        self.r5.map(|value| value.to_string())
    }

    #[getter]
    fn r_max(&self) -> String {
        self.r_max.to_string()
    }

    #[getter]
    fn rho(&self) -> String {
        self.rho.to_string()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXLendingInterestRateRecord {
    #[getter]
    fn market_id(&self) -> u64 {
        self.market_id
    }

    #[getter]
    fn asset(&self) -> &str {
        &self.asset
    }

    #[getter]
    fn supply_apr(&self) -> String {
        self.supply_apr.to_string()
    }

    #[getter]
    fn borrow_apr(&self) -> String {
        self.borrow_apr.to_string()
    }

    #[getter]
    fn statistic_time(&self) -> u64 {
        self.statistic_time
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXLendingStatusRecord {
    #[getter]
    fn market_id(&self) -> u64 {
        self.market_id
    }

    #[getter]
    fn asset(&self) -> &str {
        &self.asset
    }

    #[getter]
    fn index_price(&self) -> String {
        self.index_price.to_string()
    }

    #[getter]
    fn total_supplied(&self) -> String {
        self.total_supplied.to_string()
    }

    #[getter]
    fn total_borrowed(&self) -> String {
        self.total_borrowed.to_string()
    }

    #[getter]
    fn utilization_rate(&self) -> String {
        self.utilization_rate.to_string()
    }

    #[getter]
    fn supply_apr(&self) -> String {
        self.supply_apr.to_string()
    }

    #[getter]
    fn borrow_apr(&self) -> String {
        self.borrow_apr.to_string()
    }

    #[getter]
    fn statistic_time(&self) -> u64 {
        self.statistic_time
    }
}

/// Reusable read-only client for validated public lending market observations.
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXLendingService {
    client: DeepXHttpClient,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXLendingService {
    #[new]
    #[pyo3(signature = (config=None))]
    fn py_new(config: Option<DeepXDataClientConfig>) -> PyResult<Self> {
        let config = config.unwrap_or_default();
        config.validate().map_err(to_pyvalue_err)?;
        let client = DeepXHttpClient::from_network_config(
            &config.network,
            Some(config.http_timeout_secs),
            config.proxy_url,
        )
        .map_err(to_pyvalue_err)?;
        Ok(Self { client })
    }

    /// Returns the validated public lending asset directory.
    fn get_assets(&self, py: Python<'_>) -> PyResult<Vec<DeepXLendingAsset>> {
        let client = self.client.clone();
        let runtime = runtime()?;
        py.detach(move || runtime.block_on(client.get_lending_assets()))
            .map_err(to_pyvalue_err)
    }

    /// Returns exact borrow-rate curve parameters under optional filters.
    #[pyo3(signature = (*, market_id=None, asset=None))]
    fn get_interest_rate_params(
        &self,
        py: Python<'_>,
        market_id: Option<u64>,
        asset: Option<String>,
    ) -> PyResult<Vec<DeepXLendingInterestRateParams>> {
        let request = DeepXLendingMarketRequest { market_id, asset };
        request.validate().map_err(to_pyvalue_err)?;
        let client = self.client.clone();
        let runtime = runtime()?;
        py.detach(move || runtime.block_on(client.get_lending_interest_rate_params(&request)))
            .map_err(to_pyvalue_err)
    }

    /// Returns validated exact APR observations within the requested bounds.
    #[pyo3(signature = (*, start_ms, interval="1h", market_id=None, asset=None, end_ms=None, limit=None, sort="DESC"))]
    #[expect(clippy::too_many_arguments)]
    fn get_interest_rate_history(
        &self,
        py: Python<'_>,
        start_ms: u64,
        interval: &str,
        market_id: Option<u64>,
        asset: Option<String>,
        end_ms: Option<u64>,
        limit: Option<u32>,
        sort: &str,
    ) -> PyResult<Vec<DeepXLendingInterestRateRecord>> {
        let request = history_request(start_ms, interval, market_id, asset, end_ms, limit, sort)?;
        let client = self.client.clone();
        let runtime = runtime()?;
        py.detach(move || runtime.block_on(client.get_lending_interest_rate_history(&request)))
            .map(|history| history.details)
            .map_err(to_pyvalue_err)
    }

    /// Returns validated exact pool observations within the requested bounds.
    #[pyo3(signature = (*, start_ms, interval="1h", market_id=None, asset=None, end_ms=None, limit=None, sort="DESC"))]
    #[expect(clippy::too_many_arguments)]
    fn get_status_history(
        &self,
        py: Python<'_>,
        start_ms: u64,
        interval: &str,
        market_id: Option<u64>,
        asset: Option<String>,
        end_ms: Option<u64>,
        limit: Option<u32>,
        sort: &str,
    ) -> PyResult<Vec<DeepXLendingStatusRecord>> {
        let request = history_request(start_ms, interval, market_id, asset, end_ms, limit, sort)?;
        let client = self.client.clone();
        let runtime = runtime()?;
        py.detach(move || runtime.block_on(client.get_lending_status_history(&request)))
            .map(|history| history.details)
            .map_err(to_pyvalue_err)
    }
}

fn runtime() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)
}

fn history_request(
    start_ms: u64,
    interval: &str,
    market_id: Option<u64>,
    asset: Option<String>,
    end_ms: Option<u64>,
    limit: Option<u32>,
    sort: &str,
) -> PyResult<DeepXLendingHistoryRequest> {
    let interval = match interval {
        "1m" => DeepXPerpCandleInterval::OneMinute,
        "3m" => DeepXPerpCandleInterval::ThreeMinutes,
        "5m" => DeepXPerpCandleInterval::FiveMinutes,
        "15m" => DeepXPerpCandleInterval::FifteenMinutes,
        "30m" => DeepXPerpCandleInterval::ThirtyMinutes,
        "1h" => DeepXPerpCandleInterval::OneHour,
        "2h" => DeepXPerpCandleInterval::TwoHours,
        "4h" => DeepXPerpCandleInterval::FourHours,
        "8h" => DeepXPerpCandleInterval::EightHours,
        "12h" => DeepXPerpCandleInterval::TwelveHours,
        "1d" => DeepXPerpCandleInterval::OneDay,
        "3d" => DeepXPerpCandleInterval::ThreeDays,
        "1w" => DeepXPerpCandleInterval::OneWeek,
        "1M" => DeepXPerpCandleInterval::OneMonth,
        _ => return Err(to_pyvalue_err("unsupported lending history interval")),
    };
    let sort = match sort {
        "ASC" => DeepXAccountSortOrder::Ascending,
        "DESC" => DeepXAccountSortOrder::Descending,
        _ => return Err(to_pyvalue_err("sort must be ASC or DESC")),
    };
    let request = DeepXLendingHistoryRequest {
        market_id,
        asset,
        interval,
        start_ms,
        end_ms,
        limit,
        sort,
    };
    request.validate().map_err(to_pyvalue_err)?;
    Ok(request)
}
