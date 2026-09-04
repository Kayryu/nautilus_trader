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

//! Read-only DeepX Substrate RPC identity collection.

use std::collections::BTreeSet;

use nautilus_blockchain::rpc::http::BlockchainHttpRpcClient;
use nautilus_core::hex;
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;

use crate::{
    common::{DeepXEnvironment, DeepXError},
    config::{
        DeepXNetworkConfig, DeepXObservedRpcEndpoint, DeepXRpcEndpointValidationError,
        DeepXRpcRole, DeepXValidatedRpcEndpoints, validate_rpc_endpoint_identities,
    },
    signing::{
        ApprovedRuntimeIdentity, DeepXRuntimeSnapshotService, DeepXRuntimeSnapshotServiceError,
        DeepXRuntimeSnapshotUpdate, RuntimeSnapshot, SnapshotError,
    },
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ObservedRuntimeVersion {
    spec_version: u32,
    transaction_version: u32,
}

#[derive(Debug, Deserialize)]
struct ObservedBlockHeader {
    number: String,
}

#[derive(Debug, Deserialize)]
struct ObservedRpcMethods {
    methods: BTreeSet<String>,
}

/// Evidence that one identity-validated endpoint advertised every requested RPC method.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXRpcMethodCapabilities {
    role: DeepXRpcRole,
    methods: BTreeSet<String>,
}

impl DeepXRpcMethodCapabilities {
    /// Returns the role whose endpoint supplied this evidence.
    #[must_use]
    pub const fn role(&self) -> DeepXRpcRole {
        self.role
    }

    /// Returns the required methods advertised by the endpoint at observation time.
    #[must_use]
    pub const fn methods(&self) -> &BTreeSet<String> {
        &self.methods
    }
}

/// Finalized block identity which pins one observed DeepX runtime snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedCheckpoint {
    block_hash: [u8; 32],
    block_number: u32,
}

impl DeepXFinalizedCheckpoint {
    /// Returns the finalized block hash used for all pinned state queries.
    #[must_use]
    pub const fn block_hash(&self) -> [u8; 32] {
        self.block_hash
    }

    /// Returns the decoded finalized block number.
    #[must_use]
    pub const fn block_number(&self) -> u32 {
        self.block_number
    }
}

/// Approved runtime metadata paired with its observed finalized checkpoint.
#[derive(Clone, Debug)]
pub struct DeepXObservedRuntimeSnapshot {
    snapshot: RuntimeSnapshot,
    checkpoint: DeepXFinalizedCheckpoint,
}

impl DeepXObservedRuntimeSnapshot {
    /// Returns the fixture-approved immutable runtime snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> &RuntimeSnapshot {
        &self.snapshot
    }

    /// Returns the finalized checkpoint used to collect the snapshot.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Consumes the observation and returns its runtime snapshot.
    #[must_use]
    pub fn into_snapshot(self) -> RuntimeSnapshot {
        self.snapshot
    }
}

/// Result of one approved finalized runtime observation and snapshot application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXAppliedRuntimeSnapshot {
    checkpoint: DeepXFinalizedCheckpoint,
    update: DeepXRuntimeSnapshotUpdate,
    identity: ApprovedRuntimeIdentity,
}

impl DeepXAppliedRuntimeSnapshot {
    /// Returns the finalized checkpoint used for the approved observation.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns whether the validated snapshot was unchanged or installed.
    #[must_use]
    pub const fn update(&self) -> DeepXRuntimeSnapshotUpdate {
        self.update
    }

    /// Returns the approved runtime identity observed and applied at the finalized checkpoint.
    #[must_use]
    pub const fn identity(&self) -> &ApprovedRuntimeIdentity {
        &self.identity
    }
}

/// Errors raised while collecting DeepX RPC endpoint identity.
#[derive(Debug, Error)]
pub enum DeepXRpcIdentityError {
    /// The network configuration is unsupported.
    #[error(transparent)]
    Configuration(#[from] DeepXError),
    /// The endpoint request or JSON-RPC response failed.
    #[error("failed to query DeepX RPC endpoint identity: {0}")]
    Rpc(#[source] anyhow::Error),
    /// The endpoint returned a genesis hash other than one prefixed 32-byte value.
    #[error("DeepX RPC endpoint returned an invalid genesis hash")]
    InvalidGenesisHash,
}

/// Errors raised while probing role-specific RPC method availability.
#[derive(Debug, Error)]
pub enum DeepXRpcMethodCapabilityError {
    /// No methods were supplied for the role probe.
    #[error("no required DeepX RPC methods supplied for role {0:?}")]
    EmptyRequirements(DeepXRpcRole),
    /// The endpoint request or JSON-RPC response failed.
    #[error("failed to query DeepX RPC methods for role {role:?}: {source}")]
    Rpc {
        /// Role whose identity-validated endpoint was queried.
        role: DeepXRpcRole,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The endpoint did not advertise one required method.
    #[error("DeepX RPC endpoint for role {role:?} does not advertise required method {method}")]
    MissingMethod {
        /// Role whose endpoint lacks the method.
        role: DeepXRpcRole,
        /// Required method absent from the advertised method set.
        method: String,
    },
}

/// Errors raised while collecting and validating every DeepX RPC role endpoint.
#[derive(Debug, Error)]
pub enum DeepXRpcEndpointIdentityError {
    /// Identity collection failed for one configured role endpoint.
    #[error("failed to observe DeepX RPC endpoint identity for role {role:?}: {source}")]
    Observation {
        /// Role whose configured endpoint could not be observed.
        role: DeepXRpcRole,
        /// Underlying collection error.
        #[source]
        source: DeepXRpcIdentityError,
    },
    /// Complete observations failed endpoint identity validation.
    #[error(transparent)]
    Validation(#[from] DeepXRpcEndpointValidationError),
}

/// Errors raised while collecting an approved finalized DeepX runtime snapshot.
#[derive(Debug, Error)]
pub enum DeepXRuntimeSnapshotObservationError {
    /// The network configuration is unsupported.
    #[error(transparent)]
    Configuration(#[from] DeepXError),
    /// An endpoint request or JSON-RPC response failed.
    #[error("failed to query DeepX runtime snapshot method {method}: {source}")]
    Rpc {
        /// RPC method which failed.
        method: &'static str,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The endpoint returned a hash other than one prefixed 32-byte value.
    #[error("DeepX runtime snapshot returned an invalid {0}")]
    InvalidHash(&'static str),
    /// The finalized header returned a malformed or out-of-range block number.
    #[error("DeepX runtime snapshot returned an invalid finalized block number: {0}")]
    InvalidBlockNumber(String),
    /// The endpoint returned metadata other than one prefixed hexadecimal value.
    #[error("DeepX runtime snapshot returned invalid metadata")]
    InvalidMetadata,
    /// The observed runtime snapshot is not approved for use.
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
}

/// Errors raised by one approved runtime snapshot observation and application.
#[derive(Debug, Error)]
pub enum DeepXRuntimeSnapshotRefreshError {
    /// The finalized runtime observation failed before service state was changed.
    #[error(transparent)]
    Observation(#[from] DeepXRuntimeSnapshotObservationError),
    /// The validated snapshot could not be applied to the snapshot service.
    #[error(transparent)]
    Application(#[from] DeepXRuntimeSnapshotServiceError),
}

/// Collects the genesis hash directly from the configured endpoint for `role`.
///
/// This observation proves only the endpoint URL and reported chain identity. It does not prove
/// support for submission, watch, finality, pool, or recovery methods.
///
/// # Errors
///
/// Returns an error for unsupported configuration, transport or JSON-RPC failure, or a genesis
/// hash that is not exactly one `0x`-prefixed 32-byte hexadecimal value.
pub async fn observe_rpc_endpoint_identity(
    config: &DeepXNetworkConfig,
    role: DeepXRpcRole,
) -> Result<DeepXObservedRpcEndpoint, DeepXRpcIdentityError> {
    let url = config.rpc_url_for(role)?;
    let client = BlockchainHttpRpcClient::new(url.clone(), None, None);
    let encoded_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [0],
        }))
        .await
        .map_err(DeepXRpcIdentityError::Rpc)?;
    let hash = encoded_hash
        .strip_prefix("0x")
        .ok_or(DeepXRpcIdentityError::InvalidGenesisHash)
        .and_then(|value| {
            hex::decode_array::<32>(value).map_err(|_| DeepXRpcIdentityError::InvalidGenesisHash)
        })?;

    Ok(DeepXObservedRpcEndpoint::new(role, url, hash))
}

/// Collects and validates chain identity for every configured DeepX RPC role endpoint.
///
/// All three read-only observations complete before errors are evaluated in submission, watch,
/// then recovery order. Validated endpoints are returned only after every observation succeeds and
/// the complete set matches the configured URLs and approved testnet genesis hash.
///
/// This result does not prove support for role-specific RPC methods.
///
/// # Errors
///
/// Returns the role-specific observation failure or complete endpoint validation failure without
/// releasing a partial endpoint set.
pub async fn observe_and_validate_rpc_endpoint_identities(
    config: &DeepXNetworkConfig,
) -> Result<DeepXValidatedRpcEndpoints, DeepXRpcEndpointIdentityError> {
    let (submission, watch, recovery) = tokio::join!(
        observe_rpc_endpoint_identity(config, DeepXRpcRole::Submission),
        observe_rpc_endpoint_identity(config, DeepXRpcRole::Watch),
        observe_rpc_endpoint_identity(config, DeepXRpcRole::Recovery),
    );
    let submission = submission.map_err(|source| DeepXRpcEndpointIdentityError::Observation {
        role: DeepXRpcRole::Submission,
        source,
    })?;
    let watch = watch.map_err(|source| DeepXRpcEndpointIdentityError::Observation {
        role: DeepXRpcRole::Watch,
        source,
    })?;
    let recovery = recovery.map_err(|source| DeepXRpcEndpointIdentityError::Observation {
        role: DeepXRpcRole::Recovery,
        source,
    })?;

    validate_rpc_endpoint_identities(config, [submission, watch, recovery]).map_err(Into::into)
}

/// Probes required methods on one identity-validated role endpoint.
///
/// This evidence proves only that `rpc_methods` advertised each requested name at observation
/// time. It does not prove method semantics, authorize requests, or enable transaction operations.
///
/// # Errors
///
/// Returns an error when no methods are requested, the probe fails, or any required method is not
/// advertised by the endpoint.
pub async fn observe_rpc_method_capabilities(
    endpoints: &DeepXValidatedRpcEndpoints,
    role: DeepXRpcRole,
    required_methods: impl IntoIterator<Item = impl Into<String>>,
) -> Result<DeepXRpcMethodCapabilities, DeepXRpcMethodCapabilityError> {
    let required_methods: BTreeSet<String> = required_methods.into_iter().map(Into::into).collect();
    if required_methods.is_empty() {
        return Err(DeepXRpcMethodCapabilityError::EmptyRequirements(role));
    }

    let client = BlockchainHttpRpcClient::new(endpoints.url_for(role).to_string(), None, None);
    let observed: ObservedRpcMethods = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "rpc_methods",
            "params": [],
        }))
        .await
        .map_err(|source| DeepXRpcMethodCapabilityError::Rpc { role, source })?;

    for method in &required_methods {
        if !observed.methods.contains(method) {
            return Err(DeepXRpcMethodCapabilityError::MissingMethod {
                role,
                method: method.clone(),
            });
        }
    }

    Ok(DeepXRpcMethodCapabilities {
        role,
        methods: required_methods,
    })
}

/// Collects an approved runtime snapshot pinned to one finalized DeepX block.
///
/// The ordinary configured RPC endpoint is used without asserting role-specific method support.
/// Runtime version and metadata reads are both pinned to the finalized hash returned by the same
/// endpoint. This function does not install the snapshot or authorize signing.
///
/// # Errors
///
/// Returns an error for unsupported configuration, request or response failure, malformed hashes
/// or metadata, or a runtime snapshot which differs from the approved testnet fixture identity.
pub async fn observe_approved_finalized_runtime_snapshot(
    config: &DeepXNetworkConfig,
) -> Result<DeepXObservedRuntimeSnapshot, DeepXRuntimeSnapshotObservationError> {
    let url = config.rpc_url()?;
    observe_approved_finalized_runtime_snapshot_at(&config.environment, url).await
}

/// Observes and atomically applies an approved finalized runtime snapshot.
///
/// The observation uses only the chain-identity-validated Watch endpoint. Observation and fixture
/// validation complete before the snapshot service is changed. This one-shot operation does not
/// poll for runtime upgrades, select mortality parameters, or authorize signing or submission.
///
/// # Errors
///
/// Returns an error when runtime observation or fixture validation fails, another runtime identity
/// is pending, old signing permits remain, or snapshot service state is unavailable.
pub async fn observe_and_apply_approved_finalized_runtime_snapshot(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    service: &DeepXRuntimeSnapshotService,
) -> Result<DeepXAppliedRuntimeSnapshot, DeepXRuntimeSnapshotRefreshError> {
    let observation = observe_approved_finalized_runtime_snapshot_at(
        environment,
        endpoints.url_for(DeepXRpcRole::Watch).to_string(),
    )
    .await?;
    let update = service.apply_validated(observation.snapshot())?;

    Ok(DeepXAppliedRuntimeSnapshot {
        checkpoint: observation.checkpoint(),
        update,
        identity: observation.snapshot().identity().clone(),
    })
}

async fn observe_approved_finalized_runtime_snapshot_at(
    environment: &DeepXEnvironment,
    url: String,
) -> Result<DeepXObservedRuntimeSnapshot, DeepXRuntimeSnapshotObservationError> {
    let client = BlockchainHttpRpcClient::new(url, None, None);
    let encoded_genesis: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [0],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "chain_getBlockHash",
            source,
        })?;
    let genesis_hash = decode_prefixed_hash(&encoded_genesis, "genesis hash")?;
    let encoded_finalized_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getFinalizedHead",
            "params": [],
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "chain_getFinalizedHead",
            source,
        })?;
    let finalized_hash = decode_prefixed_hash(&encoded_finalized_hash, "finalized hash")?;
    let block_params = json!([encoded_finalized_hash]);
    let finalized_header: ObservedBlockHeader = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getHeader",
            "params": block_params.clone(),
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "chain_getHeader",
            source,
        })?;
    let finalized_block_number = decode_block_number(&finalized_header.number)?;
    let runtime_version: ObservedRuntimeVersion = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getRuntimeVersion",
            "params": block_params.clone(),
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "state_getRuntimeVersion",
            source,
        })?;
    let encoded_metadata: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getMetadata",
            "params": block_params,
        }))
        .await
        .map_err(|source| DeepXRuntimeSnapshotObservationError::Rpc {
            method: "state_getMetadata",
            source,
        })?;
    let metadata = encoded_metadata
        .strip_prefix("0x")
        .ok_or(DeepXRuntimeSnapshotObservationError::InvalidMetadata)
        .and_then(|value| {
            hex::decode(value).map_err(|_| DeepXRuntimeSnapshotObservationError::InvalidMetadata)
        })?;

    let snapshot = RuntimeSnapshot::approved_testnet(
        environment,
        genesis_hash,
        runtime_version.spec_version,
        runtime_version.transaction_version,
        &metadata,
    )?;

    Ok(DeepXObservedRuntimeSnapshot {
        snapshot,
        checkpoint: DeepXFinalizedCheckpoint {
            block_hash: finalized_hash,
            block_number: finalized_block_number,
        },
    })
}

fn decode_prefixed_hash(
    encoded_hash: &str,
    name: &'static str,
) -> Result<[u8; 32], DeepXRuntimeSnapshotObservationError> {
    encoded_hash
        .strip_prefix("0x")
        .ok_or(DeepXRuntimeSnapshotObservationError::InvalidHash(name))
        .and_then(|value| {
            hex::decode_array::<32>(value)
                .map_err(|_| DeepXRuntimeSnapshotObservationError::InvalidHash(name))
        })
}

fn decode_block_number(encoded_number: &str) -> Result<u32, DeepXRuntimeSnapshotObservationError> {
    let value = encoded_number
        .strip_prefix("0x")
        .filter(|value| !value.is_empty());
    value
        .and_then(|value| u32::from_str_radix(value, 16).ok())
        .ok_or_else(|| {
            DeepXRuntimeSnapshotObservationError::InvalidBlockNumber(encoded_number.to_string())
        })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{Json, Router, routing::post};
    use serde_json::Value;
    use tokio::net::TcpListener;

    use super::*;
    use crate::{DeepXEnvironment, common::consts::DEEPX_TESTNET_GENESIS_HASH};

    const GENESIS_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
         genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/\
         genesis_hash.json"
    );
    const FINALIZED_HEAD_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
         genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/\
         finalized_head.json"
    );
    const RUNTIME_VERSION_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
         genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/\
         runtime_version.json"
    );
    const METADATA_FIXTURE: &str = include_str!(
        "../test_data/runtime/testnet/\
         genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/\
         metadata.json"
    );
    const FINALIZED_HASH: &str =
        "0x03e29c08d90b26697535dacbcfa940c8d2ae08653e4b4760ac1dd4a281ced7c6";

    #[tokio::test]
    async fn observes_and_validates_all_rpc_role_identities() {
        let response: Value = serde_json::from_str(GENESIS_FIXTURE).unwrap();
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let response = response.clone();
                async move {
                    assert_eq!(request["jsonrpc"], "2.0");
                    assert_eq!(request["id"], 1);
                    assert_eq!(request["method"], "chain_getBlockHash");
                    assert_eq!(request["params"], json!([0]));
                    Json(response)
                }
            }),
        );
        let rpc_url = spawn_server(router).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc_submission: Some(rpc_url.clone()),
            base_url_rpc_watch: Some(rpc_url.clone()),
            base_url_rpc_recovery: Some(rpc_url),
            ..Default::default()
        };
        let validated = observe_and_validate_rpc_endpoint_identities(&config)
            .await
            .unwrap();

        assert_eq!(
            validated.url_for(DeepXRpcRole::Submission),
            config.rpc_url_for(DeepXRpcRole::Submission).unwrap()
        );
    }

    #[tokio::test]
    async fn observes_each_configured_rpc_role_endpoint() {
        let submission_calls = Arc::new(AtomicUsize::new(0));
        let watch_calls = Arc::new(AtomicUsize::new(0));
        let recovery_calls = Arc::new(AtomicUsize::new(0));
        let submission_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&submission_calls)).await;
        let watch_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&watch_calls)).await;
        let recovery_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&recovery_calls)).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc: Some("http://127.0.0.1:1".to_string()),
            base_url_rpc_submission: Some(submission_url.clone()),
            base_url_rpc_watch: Some(watch_url.clone()),
            base_url_rpc_recovery: Some(recovery_url.clone()),
            ..Default::default()
        };

        let validated = observe_and_validate_rpc_endpoint_identities(&config)
            .await
            .unwrap();

        assert_eq!(submission_calls.load(Ordering::Relaxed), 1);
        assert_eq!(watch_calls.load(Ordering::Relaxed), 1);
        assert_eq!(recovery_calls.load(Ordering::Relaxed), 1);
        assert_eq!(validated.url_for(DeepXRpcRole::Submission), submission_url);
        assert_eq!(validated.url_for(DeepXRpcRole::Watch), watch_url);
        assert_eq!(validated.url_for(DeepXRpcRole::Recovery), recovery_url);
    }

    #[tokio::test]
    async fn rejects_complete_observations_when_one_role_has_wrong_chain_identity() {
        const WRONG_GENESIS_HASH: &str =
            "0x9999999999999999999999999999999999999999999999999999999999999999";
        let submission_calls = Arc::new(AtomicUsize::new(0));
        let watch_calls = Arc::new(AtomicUsize::new(0));
        let recovery_calls = Arc::new(AtomicUsize::new(0));
        let submission_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&submission_calls)).await;
        let watch_url = spawn_identity_server(WRONG_GENESIS_HASH, Arc::clone(&watch_calls)).await;
        let recovery_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&recovery_calls)).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc_submission: Some(submission_url),
            base_url_rpc_watch: Some(watch_url),
            base_url_rpc_recovery: Some(recovery_url),
            ..Default::default()
        };

        let error = observe_and_validate_rpc_endpoint_identities(&config)
            .await
            .unwrap_err();

        assert_eq!(submission_calls.load(Ordering::Relaxed), 1);
        assert_eq!(watch_calls.load(Ordering::Relaxed), 1);
        assert_eq!(recovery_calls.load(Ordering::Relaxed), 1);
        assert!(matches!(
            error,
            DeepXRpcEndpointIdentityError::Validation(
                DeepXRpcEndpointValidationError::GenesisHashMismatch(DeepXRpcRole::Watch)
            )
        ));
    }

    #[tokio::test]
    async fn attributes_observation_failure_after_all_role_requests_complete() {
        let submission_calls = Arc::new(AtomicUsize::new(0));
        let watch_calls = Arc::new(AtomicUsize::new(0));
        let recovery_calls = Arc::new(AtomicUsize::new(0));
        let submission_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&submission_calls)).await;
        let watch_url =
            spawn_identity_server(DEEPX_TESTNET_GENESIS_HASH, Arc::clone(&watch_calls)).await;
        let recovery_url = spawn_identity_server("malformed", Arc::clone(&recovery_calls)).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc_submission: Some(submission_url),
            base_url_rpc_watch: Some(watch_url),
            base_url_rpc_recovery: Some(recovery_url),
            ..Default::default()
        };

        let error = observe_and_validate_rpc_endpoint_identities(&config)
            .await
            .unwrap_err();

        assert_eq!(submission_calls.load(Ordering::Relaxed), 1);
        assert_eq!(watch_calls.load(Ordering::Relaxed), 1);
        assert_eq!(recovery_calls.load(Ordering::Relaxed), 1);
        assert!(matches!(
            error,
            DeepXRpcEndpointIdentityError::Observation {
                role: DeepXRpcRole::Recovery,
                source: DeepXRpcIdentityError::InvalidGenesisHash,
            }
        ));
    }

    #[tokio::test]
    async fn observes_required_rpc_methods_for_validated_role_endpoint() {
        let rpc_url = spawn_rpc_methods_server(&["author_submitExtrinsic", "system_health"]).await;
        let endpoints = validated_endpoints(&rpc_url);

        let capabilities = observe_rpc_method_capabilities(
            &endpoints,
            DeepXRpcRole::Submission,
            ["author_submitExtrinsic"],
        )
        .await
        .unwrap();

        assert_eq!(capabilities.role(), DeepXRpcRole::Submission);
        assert_eq!(
            capabilities.methods(),
            &BTreeSet::from(["author_submitExtrinsic".to_string()])
        );
    }

    #[tokio::test]
    async fn rejects_rpc_role_endpoint_missing_required_method() {
        let rpc_url = spawn_rpc_methods_server(&["chain_getFinalizedHead"]).await;
        let endpoints = validated_endpoints(&rpc_url);

        let error = observe_rpc_method_capabilities(
            &endpoints,
            DeepXRpcRole::Watch,
            ["chain_getFinalizedHead", "chain_getHeader"],
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXRpcMethodCapabilityError::MissingMethod {
                role: DeepXRpcRole::Watch,
                method,
            } if method == "chain_getHeader"
        ));
    }

    #[tokio::test]
    async fn rejects_empty_rpc_method_requirements_without_network_io() {
        let endpoints = validated_endpoints("http://127.0.0.1:1");

        let error = observe_rpc_method_capabilities(
            &endpoints,
            DeepXRpcRole::Recovery,
            std::iter::empty::<&str>(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXRpcMethodCapabilityError::EmptyRequirements(DeepXRpcRole::Recovery)
        ));
    }

    #[tokio::test]
    async fn observes_approved_finalized_runtime_snapshot() {
        let calls = Arc::new(AtomicUsize::new(0));
        let rpc_url = spawn_runtime_snapshot_server(366, Arc::clone(&calls)).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc: Some(rpc_url),
            ..Default::default()
        };

        let observation = observe_approved_finalized_runtime_snapshot(&config)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 5);
        assert_eq!(
            observation.snapshot().identity().environment,
            DeepXEnvironment::Testnet
        );
        assert_eq!(observation.snapshot().identity().spec_version, 366);
        assert_eq!(observation.snapshot().identity().transaction_version, 1);
        assert_eq!(
            observation.checkpoint().block_hash(),
            hex::decode_array(FINALIZED_HASH.trim_start_matches("0x")).unwrap()
        );
        assert_eq!(observation.checkpoint().block_number(), 42);
    }

    #[tokio::test]
    async fn observes_and_applies_snapshot_from_validated_watch_endpoint() {
        let calls = Arc::new(AtomicUsize::new(0));
        let watch_url = spawn_runtime_snapshot_server(366, Arc::clone(&calls)).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let service = DeepXRuntimeSnapshotService::new(approved_runtime_snapshot());

        let applied = observe_and_apply_approved_finalized_runtime_snapshot(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &service,
        )
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 5);
        assert_eq!(applied.update(), DeepXRuntimeSnapshotUpdate::Unchanged);
        assert_eq!(applied.checkpoint().block_number(), 42);
    }

    #[tokio::test]
    async fn failed_snapshot_observation_does_not_block_snapshot_service() {
        let calls = Arc::new(AtomicUsize::new(0));
        let watch_url = spawn_runtime_snapshot_server(367, Arc::clone(&calls)).await;
        let endpoints =
            validated_role_endpoints("http://127.0.0.1:1", &watch_url, "http://127.0.0.1:2");
        let service = DeepXRuntimeSnapshotService::new(approved_runtime_snapshot());

        let error = observe_and_apply_approved_finalized_runtime_snapshot(
            &DeepXEnvironment::Testnet,
            &endpoints,
            &service,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXRuntimeSnapshotRefreshError::Observation(
                DeepXRuntimeSnapshotObservationError::Snapshot(
                    SnapshotError::RuntimeIdentityMismatch
                )
            )
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 5);
        assert!(service.acquire().is_ok());
    }

    #[tokio::test]
    async fn rejects_unapproved_finalized_runtime_snapshot() {
        let calls = Arc::new(AtomicUsize::new(0));
        let rpc_url = spawn_runtime_snapshot_server(367, Arc::clone(&calls)).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc: Some(rpc_url),
            ..Default::default()
        };

        let error = observe_approved_finalized_runtime_snapshot(&config)
            .await
            .unwrap_err();

        assert_eq!(calls.load(Ordering::Relaxed), 5);
        assert!(matches!(
            error,
            DeepXRuntimeSnapshotObservationError::Snapshot(SnapshotError::RuntimeIdentityMismatch)
        ));
    }

    #[tokio::test]
    async fn rejects_invalid_genesis_hash() {
        let router = Router::new().route(
            "/",
            post(|| async {
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": "86604388"
                }))
            }),
        );
        let rpc_url = spawn_server(router).await;
        let config = DeepXNetworkConfig {
            environment: DeepXEnvironment::Testnet,
            base_url_rpc_watch: Some(rpc_url),
            ..Default::default()
        };

        let error = observe_rpc_endpoint_identity(&config, DeepXRpcRole::Watch)
            .await
            .unwrap_err();

        assert!(matches!(error, DeepXRpcIdentityError::InvalidGenesisHash));
    }

    #[rstest::rstest]
    #[case("")]
    #[case("42")]
    #[case("0x")]
    #[case("0xgg")]
    #[case("0x100000000")]
    fn rejects_invalid_finalized_block_number(#[case] value: &str) {
        assert!(matches!(
            decode_block_number(value),
            Err(DeepXRuntimeSnapshotObservationError::InvalidBlockNumber(_))
        ));
    }

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    async fn spawn_identity_server(encoded_hash: &'static str, calls: Arc<AtomicUsize>) -> String {
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(request["method"], "chain_getBlockHash");
                    assert_eq!(request["params"], json!([0]));
                    Json(json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": encoded_hash
                    }))
                }
            }),
        );
        spawn_server(router).await
    }

    async fn spawn_rpc_methods_server(methods: &'static [&'static str]) -> String {
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| async move {
                assert_eq!(request["method"], "rpc_methods");
                assert_eq!(request["params"], json!([]));
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": { "methods": methods }
                }))
            }),
        );
        spawn_server(router).await
    }

    fn validated_endpoints(rpc_url: &str) -> DeepXValidatedRpcEndpoints {
        validated_role_endpoints(rpc_url, rpc_url, rpc_url)
    }

    fn validated_role_endpoints(
        submission_url: &str,
        watch_url: &str,
        recovery_url: &str,
    ) -> DeepXValidatedRpcEndpoints {
        let config = DeepXNetworkConfig {
            base_url_rpc_submission: Some(submission_url.to_string()),
            base_url_rpc_watch: Some(watch_url.to_string()),
            base_url_rpc_recovery: Some(recovery_url.to_string()),
            ..Default::default()
        };
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let observations = [
            DeepXObservedRpcEndpoint::new(
                DeepXRpcRole::Submission,
                submission_url.to_string(),
                genesis_hash,
            ),
            DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, watch_url.to_string(), genesis_hash),
            DeepXObservedRpcEndpoint::new(
                DeepXRpcRole::Recovery,
                recovery_url.to_string(),
                genesis_hash,
            ),
        ];
        validate_rpc_endpoint_identities(&config, observations).unwrap()
    }

    fn approved_runtime_snapshot() -> RuntimeSnapshot {
        let metadata: Value = serde_json::from_str(METADATA_FIXTURE).unwrap();
        let encoded_metadata = metadata["result"].as_str().unwrap();
        RuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap(),
            366,
            1,
            &hex::decode(encoded_metadata.trim_start_matches("0x")).unwrap(),
        )
        .unwrap()
    }

    async fn spawn_runtime_snapshot_server(spec_version: u32, calls: Arc<AtomicUsize>) -> String {
        let genesis: Value = serde_json::from_str(GENESIS_FIXTURE).unwrap();
        let finalized_head: Value = serde_json::from_str(FINALIZED_HEAD_FIXTURE).unwrap();
        let finalized_header = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "number": "0x2a"
            }
        });
        let mut runtime_version: Value = serde_json::from_str(RUNTIME_VERSION_FIXTURE).unwrap();
        runtime_version["result"]["specVersion"] = json!(spec_version);
        let metadata: Value = serde_json::from_str(METADATA_FIXTURE).unwrap();
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let calls = Arc::clone(&calls);
                let genesis = genesis.clone();
                let finalized_head = finalized_head.clone();
                let finalized_header = finalized_header.clone();
                let runtime_version = runtime_version.clone();
                let metadata = metadata.clone();
                async move {
                    let index = calls.fetch_add(1, Ordering::Relaxed);
                    let method = request["method"].as_str().unwrap();
                    let response = match index {
                        0 => {
                            assert_eq!(method, "chain_getBlockHash");
                            assert_eq!(request["params"], json!([0]));
                            genesis
                        }
                        1 => {
                            assert_eq!(method, "chain_getFinalizedHead");
                            assert_eq!(request["params"], json!([]));
                            finalized_head
                        }
                        2 => {
                            assert_eq!(method, "chain_getHeader");
                            assert_eq!(request["params"], json!([FINALIZED_HASH]));
                            finalized_header
                        }
                        3 => {
                            assert_eq!(method, "state_getRuntimeVersion");
                            assert_eq!(request["params"], json!([FINALIZED_HASH]));
                            runtime_version
                        }
                        4 => {
                            assert_eq!(method, "state_getMetadata");
                            assert_eq!(request["params"], json!([FINALIZED_HASH]));
                            metadata
                        }
                        _ => panic!("unexpected DeepX runtime snapshot request"),
                    };
                    Json(response)
                }
            }),
        );
        spawn_server(router).await
    }
}
