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

//! Retry policy for DeepX HTTP reads.

use deepx_rust_sdk::rest::should_retry_read_http_status;
use nautilus_network::retry::RetryConfig;

use super::DeepXHttpError;

/// Builds the default bounded retry configuration for idempotent DeepX HTTP reads.
#[must_use]
pub const fn deepx_http_retry_config() -> RetryConfig {
    RetryConfig {
        max_retries: 3,
        initial_delay_ms: 250,
        max_delay_ms: 2_000,
        backoff_factor: 2.0,
        jitter_ms: 100,
        operation_timeout_ms: Some(30_000),
        immediate_first: false,
        max_elapsed_ms: Some(60_000),
    }
}

/// Returns whether an idempotent DeepX HTTP read may retry after this failure.
#[must_use]
pub fn should_retry_http_error(error: &DeepXHttpError) -> bool {
    match error {
        DeepXHttpError::Transport(_) => true,
        DeepXHttpError::Http { status, .. } => should_retry_read_http_status(*status),
        DeepXHttpError::Api { code, .. } => code.is_retryable_read_failure(),
        DeepXHttpError::Decode(_)
        | DeepXHttpError::InvalidRequest(_)
        | DeepXHttpError::QuotaClaimFailed { .. }
        | DeepXHttpError::QuotaClaimTimeout { .. }
        | DeepXHttpError::InvalidHistoryResponse { .. }
        | DeepXHttpError::InvalidAccountResponse { .. }
        | DeepXHttpError::InvalidTransactionStatus { .. }
        | DeepXHttpError::ResponseMarketMismatch { .. }
        | DeepXHttpError::InvalidPath(_)
        | DeepXHttpError::InvalidBaseUrl(_)
        | DeepXHttpError::InvalidPaginationLimit
        | DeepXHttpError::PaginationLimitExceeded { .. }
        | DeepXHttpError::PaginationNoProgress { .. }
        | DeepXHttpError::MissingPaginationCursor { .. }
        | DeepXHttpError::RepeatedPaginationCursor { .. }
        | DeepXHttpError::RetryControl(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use nautilus_network::retry::RetryError;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn retries_transport_failures() {
        assert!(should_retry_http_error(&DeepXHttpError::Transport(
            "connection reset".to_string(),
        )));
    }

    #[rstest]
    fn does_not_retry_quota_claim_outcomes() {
        assert!(!should_retry_http_error(
            &DeepXHttpError::QuotaClaimFailed {
                claim_id: 42,
                message: "grant rejected".to_string(),
            }
        ));
        assert!(!should_retry_http_error(
            &DeepXHttpError::QuotaClaimTimeout {
                claim_id: 42,
                last_status: "submitted".to_string(),
            }
        ));
    }

    #[rstest]
    fn does_not_retry_local_or_decode_failures() {
        let control = DeepXHttpError::RetryControl(RetryError::Canceled);
        let path = DeepXHttpError::InvalidPath("health".to_string());
        let pagination = DeepXHttpError::PaginationLimitExceeded { max_pages: 10 };
        let identity = DeepXHttpError::ResponseMarketMismatch {
            endpoint: "perp trades",
            expected: 3,
            received: 4,
        };
        let decode =
            DeepXHttpError::Decode(serde_json::from_str::<serde_json::Value>("{").unwrap_err());

        assert!(!should_retry_http_error(&control));
        assert!(!should_retry_http_error(&path));
        assert!(!should_retry_http_error(&pagination));
        assert!(!should_retry_http_error(&identity));
        assert!(!should_retry_http_error(&decode));
    }
}
