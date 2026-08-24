// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use pyo3::prelude::*;
use rust_decimal::Decimal;

use super::parse_json;
use crate::http::models::{DeepXOrderBookSnapshot, DeepXPerpetualMarket};

#[derive(Clone, Debug)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
#[pyclass(
    name = "DeepXPerpetualMarket",
    module = "nautilus_trader.adapters.deepx",
    frozen,
    skip_from_py_object
)]
pub struct PyDeepXPerpetualMarket(DeepXPerpetualMarket);

#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyDeepXPerpetualMarket {
    #[staticmethod]
    #[pyo3(name = "from_json")]
    fn py_from_json(value: &str) -> PyResult<Self> {
        parse_json(value).map(Self)
    }

    #[getter]
    fn market_id(&self) -> u32 {
        self.0.market_id
    }

    #[getter]
    fn symbol(&self) -> &str {
        &self.0.symbol
    }

    #[getter]
    fn base_asset(&self) -> &str {
        &self.0.base_asset
    }

    #[getter]
    fn quote_asset(&self) -> &str {
        &self.0.quote_asset
    }

    #[getter]
    fn status(&self) -> &str {
        &self.0.status
    }

    #[getter]
    fn tick_size(&self) -> Decimal {
        self.0.tick_size
    }

    #[getter]
    fn step_size(&self) -> Decimal {
        self.0.step_size
    }

    #[getter]
    fn min_qty(&self) -> Decimal {
        self.0.min_qty
    }

    #[getter]
    fn min_notional(&self) -> Decimal {
        self.0.min_notional
    }

    #[getter]
    fn maker_fee_rate(&self) -> Decimal {
        self.0.maker_fee_rate
    }

    #[getter]
    fn taker_fee_rate(&self) -> Decimal {
        self.0.taker_fee_rate
    }

    #[getter]
    fn max_open_orders(&self) -> u32 {
        self.0.max_open_orders
    }

    #[getter]
    fn order_types(&self) -> Vec<String> {
        self.0.order_types.clone()
    }
}

#[derive(Clone, Debug)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
#[pyclass(
    name = "DeepXOrderBookSnapshot",
    module = "nautilus_trader.adapters.deepx",
    frozen,
    skip_from_py_object
)]
pub struct PyDeepXOrderBookSnapshot(DeepXOrderBookSnapshot);

#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyDeepXOrderBookSnapshot {
    #[staticmethod]
    #[pyo3(name = "from_json")]
    fn py_from_json(value: &str) -> PyResult<Self> {
        parse_json(value).map(Self)
    }

    #[getter]
    fn asks(&self) -> Vec<(Decimal, Decimal)> {
        self.0.asks.clone()
    }

    #[getter]
    fn bids(&self) -> Vec<(Decimal, Decimal)> {
        self.0.bids.clone()
    }

    #[getter]
    fn engine_time(&self) -> u64 {
        self.0.engine_time
    }

    #[getter]
    fn last_update_id(&self) -> u64 {
        self.0.last_update_id
    }

    #[getter]
    fn server_time(&self) -> u64 {
        self.0.server_time
    }
}
