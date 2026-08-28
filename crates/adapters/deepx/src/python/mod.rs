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

use nautilus_common::factories::{ClientConfig, DataClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::{exceptions::PyValueError, prelude::*};
use serde::de::DeserializeOwned;

use crate::{
    common::{
        consts::{DEEPX, DEEPX_CLIENT_ID, DEEPX_VENUE},
        enums::DeepXEnvironment,
    },
    config::DeepXDataClientConfig,
    factories::DeepXDataClientFactory,
};

pub mod config;
pub mod factories;
pub mod http;
pub mod websocket;

pub use http::{PyDeepXOrderBookSnapshot, PyDeepXPerpetualMarket};
pub use websocket::{PyDeepXOrderBookUpdate, PyDeepXTrade};

fn parse_json<T: DeserializeOwned>(value: &str) -> PyResult<T> {
    serde_json::from_str(value).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[expect(clippy::needless_pass_by_value)]
fn extract_deepx_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<DeepXDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract DeepXDataClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_deepx_data_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<DeepXDataClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract DeepXDataClientConfig: {e}"
        ))),
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
    m.add_class::<DeepXEnvironment>()?;
    m.add_class::<DeepXDataClientConfig>()?;
    m.add_class::<DeepXDataClientFactory>()?;
    m.add_class::<PyDeepXPerpetualMarket>()?;
    m.add_class::<PyDeepXOrderBookSnapshot>()?;
    m.add_class::<PyDeepXOrderBookUpdate>()?;
    m.add_class::<PyDeepXTrade>()?;

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor(DEEPX.to_string(), extract_deepx_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register DeepX data factory extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "DeepXDataClientConfig".to_string(),
        extract_deepx_data_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register DeepX data config extractor: {e}"
        )));
    }

    Ok(())
}
