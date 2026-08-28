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

//! Execution orchestration for signed DeepX extrinsics.

use crate::{
    execution::{
        DeepXCancelPerpOrder, DeepXChainTimeCalibration, DeepXExecutionState, DeepXExtrinsicSigner,
        DeepXNonceError, DeepXPlacePerpOrder, DeepXTimestampNonceAllocator,
    },
    http::{
        client::DeepXRawHttpClient,
        error::{DeepXHttpError, DeepXHttpResult},
        models::DeepXChainTxResponse,
    },
};

/// Nonterminal result of attempting to submit a DeepX extrinsic through the relay.
#[derive(Clone, Debug)]
pub struct DeepXSubmissionOutcome {
    pub state: DeepXExecutionState,
    pub nonce: u64,
    pub correlation: Option<DeepXChainTxResponse>,
    pub error: Option<DeepXHttpError>,
}

impl DeepXSubmissionOutcome {
    fn submitted(nonce: u64, correlation: DeepXChainTxResponse) -> Self {
        Self {
            state: DeepXExecutionState::Submitting,
            nonce,
            correlation: Some(correlation),
            error: None,
        }
    }

    fn action_required(nonce: u64, error: DeepXHttpError) -> Self {
        Self {
            state: DeepXExecutionState::ActionRequired,
            nonce,
            correlation: None,
            error: Some(error),
        }
    }
}

/// Coordinates timestamp allocation, signing, and relay submission.
#[derive(Clone, Debug)]
pub struct DeepXExecutionCoordinator {
    calibration: DeepXChainTimeCalibration,
    nonce_allocator: DeepXTimestampNonceAllocator,
}

impl DeepXExecutionCoordinator {
    /// Creates an execution coordinator from an authoritative chain-time calibration.
    #[must_use]
    pub fn new(calibration: DeepXChainTimeCalibration) -> Self {
        Self {
            calibration,
            nonce_allocator: DeepXTimestampNonceAllocator::new(),
        }
    }

    /// Validates, signs, and submits a perpetual order.
    ///
    /// A relay response is correlation evidence only, so the returned state remains `Submitting`.
    /// Any error after submission begins is ambiguous and returns `ActionRequired`.
    ///
    /// # Errors
    ///
    /// Returns an error when nonce allocation, runtime validation, or signing fails before relay
    /// submission begins.
    pub async fn place_perp_order(
        &self,
        client: &DeepXRawHttpClient,
        signer: &DeepXExtrinsicSigner,
        request: &DeepXPlacePerpOrder,
    ) -> Result<DeepXSubmissionOutcome, DeepXExecutionCoordinatorError> {
        let nonce = self.reserve_nonce()?;
        let prepared: DeepXHttpResult<String> = async {
            let constraints = signer
                .perp_market_constraints(request.market_id)
                .await
                .map_err(DeepXHttpError::from)?;
            constraints
                .validate_limit_order(request.size, request.price)
                .map_err(DeepXHttpError::from)?;
            signer
                .sign_place_perp_order(request, nonce)
                .await
                .map_err(DeepXHttpError::from)
        }
        .await;

        let signed_extrinsic = match prepared {
            Ok(signed_extrinsic) => signed_extrinsic,
            Err(error) => {
                self.nonce_allocator.release(nonce)?;
                return Err(DeepXExecutionCoordinatorError::Http(error));
            }
        };

        Ok(classify_submission(
            nonce,
            client.submit_place_perp_order(&signed_extrinsic).await,
        ))
    }

    /// Signs and submits a perpetual-order cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error when nonce allocation or signing fails before relay submission begins.
    pub async fn cancel_perp_order(
        &self,
        client: &DeepXRawHttpClient,
        signer: &DeepXExtrinsicSigner,
        request: &DeepXCancelPerpOrder,
    ) -> Result<DeepXSubmissionOutcome, DeepXExecutionCoordinatorError> {
        let nonce = self.reserve_nonce()?;
        let signed_extrinsic = match signer.sign_cancel_perp_order(request, nonce).await {
            Ok(signed_extrinsic) => signed_extrinsic,
            Err(error) => {
                self.nonce_allocator.release(nonce)?;
                return Err(DeepXExecutionCoordinatorError::Http(error.into()));
            }
        };

        Ok(classify_submission(
            nonce,
            client.submit_cancel_perp_order(&signed_extrinsic).await,
        ))
    }

    fn reserve_nonce(&self) -> Result<u64, DeepXNonceError> {
        self.nonce_allocator
            .next(self.calibration.estimated_chain_time_ms()?)
    }
}

/// Errors which prove that relay submission did not begin.
#[derive(Clone, Debug, thiserror::Error)]
pub enum DeepXExecutionCoordinatorError {
    #[error(transparent)]
    Nonce(#[from] DeepXNonceError),
    #[error(transparent)]
    Http(#[from] DeepXHttpError),
}

fn classify_submission(
    nonce: u64,
    result: DeepXHttpResult<DeepXChainTxResponse>,
) -> DeepXSubmissionOutcome {
    match result {
        Ok(correlation) => DeepXSubmissionOutcome::submitted(nonce, correlation),
        Err(error) => DeepXSubmissionOutcome::action_required(nonce, error),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn relay_response_remains_submitting() {
        let outcome = classify_submission(
            1_000,
            Ok(DeepXChainTxResponse {
                order_id: 42,
                tx_hash: "0x1234".to_string(),
            }),
        );

        assert_eq!(outcome.state, DeepXExecutionState::Submitting);
        assert_eq!(outcome.nonce, 1_000);
        assert_eq!(outcome.correlation.unwrap().order_id, 42);
        assert!(outcome.error.is_none());
    }

    #[rstest]
    fn relay_error_requires_authoritative_reconciliation() {
        let outcome = classify_submission(
            1_000,
            Err(DeepXHttpError::Network("connection reset".to_string())),
        );

        assert_eq!(outcome.state, DeepXExecutionState::ActionRequired);
        assert_eq!(outcome.nonce, 1_000);
        assert!(outcome.correlation.is_none());
        assert!(matches!(outcome.error, Some(DeepXHttpError::Network(_))));
    }
}
