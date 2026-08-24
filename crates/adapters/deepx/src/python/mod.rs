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

//! Python bindings from `pyo3` for DeepX protocol models.

use pyo3::{exceptions::PyValueError, prelude::*};
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;

use crate::{
    common::consts::{DEEPX, DEEPX_CLIENT_ID, DEEPX_VENUE},
    http::models::{DeepXOrderBookSnapshot, DeepXPerpetualMarket},
    websocket::messages::{
        DeepXBookUpdateType, DeepXTakerSide, DeepXTrade, DeepXWsMessage,
        DeepXOrderBookUpdate,
    },
};

fn parse_json<T: DeserializeOwned>(value: &str) -> PyResult<T> {
    serde_json::from_str(value).map_err(|e| PyValueError::new_err(e.to_string()))
}

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

#[derive(Clone, Debug)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
#[pyclass(
    name = "DeepXOrderBookUpdate",
    module = "nautilus_trader.adapters.deepx",
    frozen,
    skip_from_py_object
)]
pub struct PyDeepXOrderBookUpdate(DeepXOrderBookUpdate);

#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyDeepXOrderBookUpdate {
    #[staticmethod]
    #[pyo3(name = "from_json")]
    fn py_from_json(value: &str) -> PyResult<Self> {
        let message: DeepXWsMessage<DeepXOrderBookUpdate> = parse_json(value)?;
        Ok(Self(message.data))
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
    fn last_update_id(&self) -> u64 {
        self.0.last_update_id
    }

    #[getter]
    fn prev_last_update_id(&self) -> Option<u64> {
        self.0.prev_last_update_id
    }

    #[getter]
    fn symbol(&self) -> &str {
        &self.0.symbol
    }

    #[getter]
    fn update_type(&self) -> &str {
        match self.0.update_type {
            DeepXBookUpdateType::Snapshot => "snapshot",
            DeepXBookUpdateType::Delta => "delta",
        }
    }

    fn follows(&self, last_update_id: Option<u64>) -> bool {
        self.0.follows(last_update_id)
    }
}

#[derive(Clone, Debug)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
#[pyclass(
    name = "DeepXTrade",
    module = "nautilus_trader.adapters.deepx",
    frozen,
    skip_from_py_object
)]
pub struct PyDeepXTrade(DeepXTrade);

#[pyo3_stub_gen::derive::gen_stub_pymethods]
#[pymethods]
impl PyDeepXTrade {
    #[staticmethod]
    #[pyo3(name = "from_json")]
    fn py_from_json(value: &str) -> PyResult<Self> {
        let message: DeepXWsMessage<DeepXTrade> = parse_json(value)?;
        Ok(Self(message.data))
    }

    #[getter]
    fn id(&self) -> &str {
        &self.0.id
    }

    #[getter]
    fn market_id(&self) -> u32 {
        self.0.market_id
    }

    #[getter]
    fn price(&self) -> Decimal {
        self.0.price
    }

    #[getter]
    fn qty(&self) -> Decimal {
        self.0.qty
    }

    #[getter]
    fn quote_qty(&self) -> Decimal {
        self.0.quote_qty
    }

    #[getter]
    fn symbol(&self) -> &str {
        &self.0.symbol
    }

    #[getter]
    fn taker_side(&self) -> &str {
        match self.0.taker_side {
            DeepXTakerSide::Buy => "BUY",
            DeepXTakerSide::Sell => "SELL",
        }
    }

    #[getter]
    fn time(&self) -> u64 {
        self.0.time
    }
}

/// Exposed through `nautilus_trader.adapters.deepx`.
///
/// # Errors
///
/// Returns an error if any binding fails to register with the Python module.
#[pymodule]
pub fn deepx(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(stringify!(DEEPX), DEEPX)?;
    m.add(stringify!(DEEPX_CLIENT_ID), *DEEPX_CLIENT_ID)?;
    m.add(stringify!(DEEPX_VENUE), *DEEPX_VENUE)?;
    m.add_class::<PyDeepXPerpetualMarket>()?;
    m.add_class::<PyDeepXOrderBookSnapshot>()?;
    m.add_class::<PyDeepXOrderBookUpdate>()?;
    m.add_class::<PyDeepXTrade>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn parses_order_book_sequence_for_python() {
        let snapshot = PyDeepXOrderBookUpdate::py_from_json(
            r#"{"channel":"perp@orderbook","data":{"asks":[],"bids":[],"engineTime":1,"lastUpdateId":10,"prevLastUpdateId":null,"serverTime":2,"symbol":"ETH-USDC","updateType":"snapshot"}}"#,
        )
        .unwrap();
        let delta = PyDeepXOrderBookUpdate::py_from_json(
            r#"{"channel":"perp@orderbook","data":{"asks":[],"bids":[],"engineTime":3,"lastUpdateId":11,"prevLastUpdateId":10,"serverTime":4,"symbol":"ETH-USDC","updateType":"delta"}}"#,
        )
        .unwrap();

        assert!(snapshot.follows(None));
        assert!(delta.follows(Some(snapshot.last_update_id())));
    }
}