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

//! Python bindings from `pyo3`.

pub mod config;
pub mod factories;

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::consts::{DEEPX, DEEPX_CLIENT_ID, DEEPX_VENUE},
    config::{
        DeepXDataClientConfig, DeepXExecutionClientConfig, DeepXHttpReadRetryConfig,
        DeepXNetworkConfig,
    },
    factories::{DeepXDataClientFactory, DeepXExecutionClientFactory},
};

#[expect(clippy::needless_pass_by_value)]
fn extract_deepx_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    factory
        .extract::<DeepXDataClientFactory>(py)
        .map(|factory| Box::new(factory) as Box<dyn DataClientFactory>)
        .map_err(|error| {
            to_pyvalue_err(format!("Failed to extract DeepXDataClientFactory: {error}"))
        })
}

#[expect(clippy::needless_pass_by_value)]
fn extract_deepx_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    factory
        .extract::<DeepXExecutionClientFactory>(py)
        .map(|factory| Box::new(factory) as Box<dyn ExecutionClientFactory>)
        .map_err(|error| {
            to_pyvalue_err(format!(
                "Failed to extract DeepXExecutionClientFactory: {error}"
            ))
        })
}

#[expect(clippy::needless_pass_by_value)]
fn extract_deepx_data_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    config
        .extract::<DeepXDataClientConfig>(py)
        .map(|config| Box::new(config) as Box<dyn ClientConfig>)
        .map_err(|error| {
            to_pyvalue_err(format!("Failed to extract DeepXDataClientConfig: {error}"))
        })
}

#[expect(clippy::needless_pass_by_value)]
fn extract_deepx_exec_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    config
        .extract::<DeepXExecutionClientConfig>(py)
        .map(|config| Box::new(config) as Box<dyn ClientConfig>)
        .map_err(|error| {
            to_pyvalue_err(format!(
                "Failed to extract DeepXExecutionClientConfig: {error}"
            ))
        })
}

/// Exposed through `nautilus_trader.adapters.deepx`.
///
/// # Errors
///
/// Returns an error if any bindings fail to register with the Python module.
#[pymodule]
pub fn deepx(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(stringify!(DEEPX), DEEPX)?;
    m.add(stringify!(DEEPX_CLIENT_ID), *DEEPX_CLIENT_ID)?;
    m.add(stringify!(DEEPX_VENUE), *DEEPX_VENUE)?;
    m.add_class::<DeepXHttpReadRetryConfig>()?;
    m.add_class::<DeepXNetworkConfig>()?;
    m.add_class::<DeepXDataClientConfig>()?;
    m.add_class::<DeepXDataClientFactory>()?;
    m.add_class::<DeepXExecutionClientConfig>()?;
    m.add_class::<DeepXExecutionClientFactory>()?;

    let registry = get_global_pyo3_registry();
    registry
        .register_factory_extractor(DEEPX.to_string(), extract_deepx_data_factory)
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register DeepX data factory extractor: {error}"
            ))
        })?;
    registry
        .register_exec_factory_extractor(DEEPX.to_string(), extract_deepx_exec_factory)
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register DeepX exec factory extractor: {error}"
            ))
        })?;
    registry
        .register_config_extractor(
            stringify!(DeepXDataClientConfig).to_string(),
            extract_deepx_data_config,
        )
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register DeepX data config extractor: {error}"
            ))
        })?;
    registry
        .register_config_extractor(
            stringify!(DeepXExecutionClientConfig).to_string(),
            extract_deepx_exec_config,
        )
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register DeepX exec config extractor: {error}"
            ))
        })?;

    Ok(())
}
