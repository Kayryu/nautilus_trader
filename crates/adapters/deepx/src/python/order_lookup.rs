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

//! Read-only Python order lookup by exact transaction hash.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{DeepXHttpClient, DeepXPerpOrderRecord, DeepXSpotOrderRecord},
};

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

/// Returns one validated perpetual order associated with a transaction hash.
///
/// The mutable REST association does not establish canonical inclusion or finality.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_order_by_tx", signature = (tx_hash, config=None))]
pub(super) fn py_get_deepx_perp_order_by_tx(
    py: Python<'_>,
    tx_hash: String,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXPerpOrderRecord> {
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_perp_order_by_tx(&tx_hash)))
        .map_err(to_pyvalue_err)
}

/// Returns one validated Spot order associated with a transaction hash.
///
/// The mutable REST association does not establish canonical inclusion or finality.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_spot_order_by_tx", signature = (tx_hash, config=None))]
pub(super) fn py_get_deepx_spot_order_by_tx(
    py: Python<'_>,
    tx_hash: String,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXSpotOrderRecord> {
    let (client, runtime) = client_and_runtime(config)?;
    py.detach(move || runtime.block_on(client.get_spot_order_by_tx(&tx_hash)))
        .map_err(to_pyvalue_err)
}
