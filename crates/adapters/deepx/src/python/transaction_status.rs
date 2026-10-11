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

//! One-shot, read-only Python transaction-status observation.

use std::{num::NonZeroU32, time::Duration};

use nautilus_core::{hex, python::to_pyvalue_err};
use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::{
    config::DeepXDataClientConfig,
    http::DeepXHttpClient,
    transaction::{
        DeepXRestStatusPollPolicy, DeepXRestStatusPollResult as CorePollResult,
        DeepXRestStatusPollTermination, DeepXRestTransactionConfirmation,
        DeepXRestTransactionStatus, get_rest_transaction_status, poll_rest_transaction_status,
    },
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXRestTransactionStatus {
    #[getter]
    fn tx_hash(&self) -> String {
        format!("0x{}", hex::encode(self.extrinsic_hash()))
    }

    #[getter("confirmation")]
    fn py_confirmation(&self) -> &'static str {
        match self.confirmation() {
            DeepXRestTransactionConfirmation::Pending => "pending",
            DeepXRestTransactionConfirmation::Best => "best",
            DeepXRestTransactionConfirmation::DecodeFailed => "decode_failed",
        }
    }

    #[getter("order_id")]
    fn py_order_id(&self) -> Option<u64> {
        self.order_id()
    }

    #[getter("backend_status")]
    fn py_backend_status(&self) -> Option<&str> {
        self.backend_status()
    }

    #[getter]
    fn data_json(&self) -> &str {
        self.data().get()
    }
}

/// Retained result of bounded, read-only transaction-status polling.
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXRestStatusPollResult {
    termination: &'static str,
    attempts: u32,
    last_observation: Option<DeepXRestTransactionStatus>,
    last_error: Option<String>,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXRestStatusPollResult {
    #[getter]
    fn termination(&self) -> &'static str {
        self.termination
    }

    #[getter]
    fn attempts(&self) -> u32 {
        self.attempts
    }

    #[getter]
    fn last_observation(&self) -> Option<DeepXRestTransactionStatus> {
        self.last_observation.clone()
    }

    #[getter]
    fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

impl From<CorePollResult> for DeepXRestStatusPollResult {
    fn from(result: CorePollResult) -> Self {
        let termination = match result.termination {
            DeepXRestStatusPollTermination::Best => "best",
            DeepXRestStatusPollTermination::DecodeFailed => "decode_failed",
            DeepXRestStatusPollTermination::AttemptLimit => "attempt_limit",
            DeepXRestStatusPollTermination::Timeout => "timeout",
            DeepXRestStatusPollTermination::Cancelled => "cancelled",
            DeepXRestStatusPollTermination::Failed => "failed",
        };
        Self {
            termination,
            attempts: result.attempts,
            last_observation: result.last_observation,
            last_error: result.last_error.map(|e| e.to_string()),
        }
    }
}

/// One owner for bounded status polling of already-submitted hashes.
///
/// Cancellation is sticky and stops all polls owned by this instance.
#[pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")]
pub(super) struct DeepXRestStatusPoller {
    client: DeepXHttpClient,
    cancellation: CancellationToken,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXRestStatusPoller {
    #[new]
    #[pyo3(signature = (config=None))]
    fn py_new(config: Option<DeepXDataClientConfig>) -> PyResult<Self> {
        Ok(Self {
            client: client_from_config(config)?,
            cancellation: CancellationToken::new(),
        })
    }

    /// Stops any active or future poll owned by this instance.
    fn cancel(&self) {
        self.cancellation.cancel();
    }

    #[getter]
    fn cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Polls one hash within explicit attempt and time limits without submitting bytes.
    #[pyo3(signature = (tx_hash, *, max_attempts, interval_ms, timeout_ms))]
    fn poll(
        &self,
        py: Python<'_>,
        tx_hash: &str,
        max_attempts: u32,
        interval_ms: u64,
        timeout_ms: u64,
    ) -> PyResult<DeepXRestStatusPollResult> {
        let hash = parse_hash(tx_hash)?;
        let max_attempts = NonZeroU32::new(max_attempts)
            .ok_or_else(|| to_pyvalue_err("max_attempts must be greater than zero"))?;
        if interval_ms == 0 || timeout_ms == 0 {
            return Err(to_pyvalue_err(
                "interval_ms and timeout_ms must be greater than zero",
            ));
        }
        let policy = DeepXRestStatusPollPolicy {
            max_attempts,
            interval: Duration::from_millis(interval_ms),
            timeout: Duration::from_millis(timeout_ms),
        };
        let client = self.client.clone();
        let cancellation = self.cancellation.clone();
        let runtime = runtime()?;
        py.detach(move || {
            runtime.block_on(poll_rest_transaction_status(
                &client,
                hash,
                policy,
                &cancellation,
            ))
        })
        .map(Into::into)
        .map_err(to_pyvalue_err)
    }
}

/// Returns one hash-bound backend status without submitting or polling.
///
/// A `best` response is not canonical finality. Backend not-found does not prove non-inclusion.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_rest_transaction_status", signature = (tx_hash, config=None))]
pub(super) fn py_get_deepx_rest_transaction_status(
    py: Python<'_>,
    tx_hash: &str,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXRestTransactionStatus> {
    let hash = parse_hash(tx_hash)?;
    let client = client_from_config(config)?;
    let runtime = runtime()?;
    py.detach(move || runtime.block_on(get_rest_transaction_status(&client, hash)))
        .map_err(to_pyvalue_err)
}

fn parse_hash(tx_hash: &str) -> PyResult<[u8; 32]> {
    tx_hash
        .strip_prefix("0x")
        .and_then(|value| hex::decode_array::<32>(value).ok())
        .ok_or_else(|| to_pyvalue_err("tx_hash must be a 0x-prefixed 32-byte hash"))
}

fn client_from_config(config: Option<DeepXDataClientConfig>) -> PyResult<DeepXHttpClient> {
    let config = config.unwrap_or_default();
    config.validate().map_err(to_pyvalue_err)?;
    DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )
    .map_err(to_pyvalue_err)
}

fn runtime() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)
}
