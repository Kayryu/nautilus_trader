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

//! Error types for the DeepX HTTP transport.

use nautilus_network::{http::HttpClientError, retry::RetryError};
use thiserror::Error;

/// Result alias for DeepX HTTP operations.
pub type Result<T> = std::result::Result<T, DeepXHttpError>;

/// Errors raised by the DeepX HTTP transport.
#[derive(Debug, Error)]
pub enum DeepXHttpError {
    /// The request could not be completed at the transport layer.
    #[error("DeepX HTTP transport error: {0}")]
    Transport(String),
    /// The venue returned a non-success HTTP status.
    #[error("DeepX HTTP {status}: {message}")]
    Http { status: u16, message: String },
    /// A successful response could not be decoded into its expected schema.
    #[error("failed to decode DeepX HTTP response: {0}")]
    Decode(#[from] serde_json::Error),
    /// A successful HTTP response carried a venue-level failure envelope.
    #[error("DeepX API {code}: {message}")]
    Api { code: u16, message: String },
    /// Typed endpoint parameters violate a local request invariant.
    #[error("invalid DeepX HTTP request: {0}")]
    InvalidRequest(String),
    /// A request path could escape the configured DeepX base URL.
    #[error("invalid DeepX HTTP path '{0}': expected an absolute-path reference")]
    InvalidPath(String),
    /// A configured endpoint is not a valid HTTP base URL.
    #[error("invalid DeepX HTTP base URL '{0}'")]
    InvalidBaseUrl(String),
    /// A cursor paginator was configured without a page budget.
    #[error("DeepX HTTP pagination max_pages must be greater than zero")]
    InvalidPaginationLimit,
    /// A cursor paginator exhausted its local page budget.
    #[error("DeepX HTTP pagination exceeded its limit of {max_pages} pages")]
    PaginationLimitExceeded { max_pages: usize },
    /// A response supplied a cursor without any records and therefore made no progress.
    #[error("DeepX HTTP pagination returned an empty page with cursor '{cursor}'")]
    PaginationNoProgress { cursor: String },
    /// A response repeated a cursor already observed by this paginator.
    #[error("DeepX HTTP pagination repeated cursor '{cursor}'")]
    RepeatedPaginationCursor { cursor: String },
    /// The shared retry machinery could not continue the operation.
    #[error("DeepX HTTP retry control error: {0}")]
    RetryControl(#[source] RetryError),
}

impl DeepXHttpError {
    /// Returns whether the request failed before an authoritative venue response was available.
    #[must_use]
    pub fn is_transport_error(&self) -> bool {
        matches!(self, Self::Transport(_))
    }
}

impl From<HttpClientError> for DeepXHttpError {
    fn from(value: HttpClientError) -> Self {
        Self::Transport(value.to_string())
    }
}

impl From<RetryError> for DeepXHttpError {
    fn from(value: RetryError) -> Self {
        match value {
            RetryError::OperationTimeout { .. } => Self::Transport(value.to_string()),
            _ => Self::RetryControl(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn maps_shared_client_errors_to_transport_failures() {
        let error: DeepXHttpError = HttpClientError::Error("connection reset".to_string()).into();

        assert!(error.is_transport_error());
        assert!(error.to_string().contains("connection reset"));
    }

    #[rstest]
    fn venue_http_errors_are_authoritative_responses() {
        let error = DeepXHttpError::Http {
            status: 503,
            message: "service unavailable".to_string(),
        };

        assert!(!error.is_transport_error());
    }
}
