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

//! HTTP errors returned by the DeepX REST client.

use nautilus_network::http::HttpClientError;
use thiserror::Error;

use crate::execution::{DeepXOrderValidationError, DeepXSignerError};

/// Result alias for DeepX HTTP operations.
pub type DeepXHttpResult<T> = Result<T, DeepXHttpError>;

/// Errors emitted by the DeepX HTTP client.
#[derive(Clone, Debug, Error)]
pub enum DeepXHttpError {
    /// Request validation failed before network submission.
    #[error("validation error: {0}")]
    Validation(String),
    /// Network-level request failure.
    #[error("network error: {0}")]
    Network(String),
    /// DeepX returned a non-success HTTP response.
    #[error("HTTP {status}: {body}")]
    Http { status: u16, body: String },
    /// DeepX returned an invalid JSON response.
    #[error("parse error: {0}")]
    Parse(String),
    /// A Substrate extrinsic could not be constructed or signed.
    #[error("signing error: {0}")]
    Signing(String),
}

impl From<HttpClientError> for DeepXHttpError {
    fn from(error: HttpClientError) -> Self {
        Self::Network(error.to_string())
    }
}

impl From<serde_json::Error> for DeepXHttpError {
    fn from(error: serde_json::Error) -> Self {
        Self::Parse(error.to_string())
    }
}

impl From<DeepXSignerError> for DeepXHttpError {
    fn from(error: DeepXSignerError) -> Self {
        Self::Signing(error.to_string())
    }
}

impl From<DeepXOrderValidationError> for DeepXHttpError {
    fn from(error: DeepXOrderValidationError) -> Self {
        Self::Validation(error.to_string())
    }
}
