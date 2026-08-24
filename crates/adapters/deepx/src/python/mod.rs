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
use serde::de::DeserializeOwned;

use crate::common::consts::{DEEPX, DEEPX_CLIENT_ID, DEEPX_VENUE};

pub mod http;
pub mod websocket;

pub use http::{PyDeepXOrderBookSnapshot, PyDeepXPerpetualMarket};
pub use websocket::{PyDeepXOrderBookUpdate, PyDeepXTrade};

fn parse_json<T: DeserializeOwned>(value: &str) -> PyResult<T> {
    serde_json::from_str(value).map_err(|e| PyValueError::new_err(e.to_string()))
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
