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

//! Finalized, read-only SubaccountInfo Spot-margin observation.

use deepx_rust_sdk::rpc::{
    DeepXRpcClient,
    spot_margin::{DeepXSpotMarginQueryError, get_spot_margin_at_block},
};
use thiserror::Error;

use super::{
    DeepXFinalizedCheckpoint, DeepXRpcRole, DeepXRuntimeSnapshotObservationError,
    DeepXValidatedRpcMethodCapabilities, observe_approved_finalized_read_only_runtime_snapshot_at,
};
use crate::{
    common::DeepXEnvironment, config::DeepXValidatedRpcEndpoints, signing::ApprovedRuntimeIdentity,
};

/// Spot-margin setting decoded from SubaccountInfo at one finalized checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedSpotMarginEvidence {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
    authority: [u8; 20],
    enabled: bool,
}

impl DeepXFinalizedSpotMarginEvidence {
    /// Returns the finalized block used for the storage observation.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the approved historical runtime identity.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the exact observed subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns the authority encoded in the finalized account state.
    #[must_use]
    pub const fn authority(&self) -> [u8; 20] {
        self.authority
    }

    /// Returns the finalized Spot-margin setting.
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }
}

/// Errors from a finalized SubaccountInfo Spot-margin observation.
#[derive(Debug, Error)]
pub enum DeepXFinalizedSpotMarginError {
    /// Watch capability evidence is missing or belongs to another endpoint.
    #[error("DeepX Watch RPC capabilities do not authorize finalized Spot-margin reads")]
    RpcCapabilitiesMismatch,
    /// The finalized runtime does not match an approved read-only snapshot.
    #[error(transparent)]
    Runtime(#[from] DeepXRuntimeSnapshotObservationError),
    /// A hash-pinned RPC request or response failed.
    #[error("failed to query DeepX finalized Spot-margin storage: {0}")]
    Rpc(#[source] anyhow::Error),
    /// The storage response or SCALE value was missing, malformed, or out of scope.
    #[error("DeepX finalized SubaccountInfo storage is invalid or belongs to another account")]
    InvalidStorage,
    /// The two hash-pinned storage methods disagreed.
    #[error("DeepX finalized SubaccountInfo storage methods returned different values")]
    InconsistentStorage,
    /// The requested finalized height no longer resolves to its observed hash.
    #[error("DeepX finalized Spot-margin block is no longer canonical")]
    CanonicalHashMismatch,
}

/// Observes an exact subaccount Spot-margin setting at an approved finalized checkpoint.
///
/// The expected authority must come from the caller's independent wallet identity. The observer
/// makes only read-only RPC requests and does not establish transaction inclusion or authorization.
///
/// # Errors
///
/// Returns an error for missing Watch capabilities, an unapproved runtime, malformed or conflicting
/// storage, a foreign account identity, or a changed canonical block hash.
pub async fn observe_finalized_spot_margin_evidence(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    subaccount: [u8; 20],
    expected_authority: [u8; 20],
) -> Result<DeepXFinalizedSpotMarginEvidence, DeepXFinalizedSpotMarginError> {
    let watch = capabilities.for_role(DeepXRpcRole::Watch);
    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    if watch.role() != DeepXRpcRole::Watch
        || watch.endpoint_url() != watch_url
        || ![
            "chain_getBlockHash",
            "chain_getFinalizedHead",
            "chain_getHeader",
            "state_getStorage",
            "state_queryStorageAt",
        ]
        .iter()
        .all(|method| watch.methods().contains(*method))
    {
        return Err(DeepXFinalizedSpotMarginError::RpcCapabilitiesMismatch);
    }
    let observation = observe_approved_finalized_read_only_runtime_snapshot_at(
        environment,
        watch_url.to_string(),
    )
    .await?;
    let client = DeepXRpcClient::new(watch_url.to_string());
    let enabled = get_spot_margin_at_block(
        &client,
        observation.checkpoint().block_hash(),
        observation.checkpoint().block_number(),
        observation.snapshot().metadata(),
        subaccount,
        expected_authority,
    )
    .await
    .map_err(|e| match e {
        DeepXSpotMarginQueryError::Rpc(e) => DeepXFinalizedSpotMarginError::Rpc(e),
        DeepXSpotMarginQueryError::InvalidStorage => DeepXFinalizedSpotMarginError::InvalidStorage,
        DeepXSpotMarginQueryError::InconsistentStorage => {
            DeepXFinalizedSpotMarginError::InconsistentStorage
        }
        DeepXSpotMarginQueryError::CanonicalHashMismatch => {
            DeepXFinalizedSpotMarginError::CanonicalHashMismatch
        }
    })?;
    Ok(DeepXFinalizedSpotMarginEvidence {
        checkpoint: observation.checkpoint(),
        runtime_identity: observation.snapshot().identity().clone(),
        subaccount,
        authority: expected_authority,
        enabled,
    })
}
