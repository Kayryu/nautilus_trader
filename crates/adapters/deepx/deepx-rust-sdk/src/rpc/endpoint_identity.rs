// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Chain identity validation for role-specific DeepX JSON-RPC endpoints.

use std::{
    collections::BTreeSet,
    fmt::{Debug, Formatter},
};

use thiserror::Error;

use super::get_rpc_methods;
use crate::{deployment::DEEPX_TESTNET_GENESIS_HASH, error::DeepXError};

const REDACTED: &str = "<redacted>";

/// RPC methods required from a transaction submission endpoint.
pub const DEEPX_SUBMISSION_RPC_METHODS: &[&str] =
    &["author_pendingExtrinsics", "author_submitExtrinsic"];

/// RPC methods required from a finalized runtime watch endpoint.
pub const DEEPX_WATCH_RPC_METHODS: &[&str] = &[
    "chain_getBlock",
    "chain_getBlockHash",
    "chain_getFinalizedHead",
    "chain_getHeader",
    "eth_call",
    "state_getStorage",
    "state_queryStorageAt",
    "state_call",
];

/// RPC methods required from a transaction recovery endpoint.
pub const DEEPX_RECOVERY_RPC_METHODS: &[&str] = &[
    "chain_getBlock",
    "chain_getBlockHash",
    "chain_getFinalizedHead",
    "chain_getHeader",
    "state_getStorage",
];

/// Role assigned to a DeepX Substrate JSON-RPC endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXRpcRole {
    /// Submits signed transaction bytes.
    Submission,
    /// Observes best and finalized heads and transaction inclusion.
    Watch,
    /// Performs bounded canonical recovery scans and pool checks.
    Recovery,
}

/// Identity observed directly from one configured DeepX JSON-RPC endpoint.
#[derive(Clone, PartialEq, Eq)]
pub struct DeepXObservedRpcEndpoint {
    role: DeepXRpcRole,
    url: String,
    genesis_hash: [u8; 32],
}

impl Debug for DeepXObservedRpcEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXObservedRpcEndpoint))
            .field("role", &self.role)
            .field("url", &REDACTED)
            .field("genesis_hash", &self.genesis_hash)
            .finish()
    }
}

impl DeepXObservedRpcEndpoint {
    /// Creates identity evidence returned by one endpoint assigned to `role`.
    #[must_use]
    pub fn new(role: DeepXRpcRole, url: String, genesis_hash: [u8; 32]) -> Self {
        Self {
            role,
            url,
            genesis_hash,
        }
    }

    /// Returns the role assigned to the observed endpoint.
    #[must_use]
    pub const fn role(&self) -> DeepXRpcRole {
        self.role
    }
}

/// Errors raised when RPC endpoint identity evidence is incomplete or inconsistent.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXRpcEndpointValidationError {
    /// The built-in approved testnet genesis hash is invalid.
    #[error("invalid built-in DeepX testnet genesis hash")]
    InvalidApprovedGenesisHash,
    /// No identity evidence was supplied for a configured role.
    #[error("missing DeepX RPC endpoint identity for role {0:?}")]
    MissingRole(DeepXRpcRole),
    /// More than one identity observation was supplied for a role.
    #[error("duplicate DeepX RPC endpoint identity for role {0:?}")]
    DuplicateRole(DeepXRpcRole),
    /// The observation did not identify the endpoint selected by the configuration.
    #[error("DeepX RPC endpoint URL does not match configured role {0:?}")]
    UrlMismatch(DeepXRpcRole),
    /// The endpoint belongs to a chain other than the approved DeepX testnet.
    #[error("DeepX RPC endpoint genesis hash is not approved for role {0:?}")]
    GenesisHashMismatch(DeepXRpcRole),
    /// The network configuration itself is unsupported.
    #[error(transparent)]
    Configuration(#[from] DeepXError),
}

/// Complete identity-validated endpoint selection for every DeepX RPC role.
#[derive(Clone, PartialEq, Eq)]
pub struct DeepXValidatedRpcEndpoints {
    submission_url: String,
    watch_url: String,
    recovery_url: String,
    genesis_hash: [u8; 32],
}

impl Debug for DeepXValidatedRpcEndpoints {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXValidatedRpcEndpoints))
            .field("submission_url", &REDACTED)
            .field("watch_url", &REDACTED)
            .field("recovery_url", &REDACTED)
            .field("genesis_hash", &self.genesis_hash)
            .finish()
    }
}

impl DeepXValidatedRpcEndpoints {
    /// Returns the identity-validated URL assigned to `role`.
    #[must_use]
    pub fn url_for(&self, role: DeepXRpcRole) -> &str {
        match role {
            DeepXRpcRole::Submission => &self.submission_url,
            DeepXRpcRole::Watch => &self.watch_url,
            DeepXRpcRole::Recovery => &self.recovery_url,
        }
    }

    /// Returns the approved genesis hash observed from every role endpoint.
    #[must_use]
    pub const fn genesis_hash(&self) -> [u8; 32] {
        self.genesis_hash
    }
}

/// Required RPC methods advertised by one identity-validated role endpoint.
#[derive(Clone, PartialEq, Eq)]
pub struct DeepXRpcMethodCapabilities {
    role: DeepXRpcRole,
    endpoint_url: String,
    methods: BTreeSet<String>,
}

impl Debug for DeepXRpcMethodCapabilities {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXRpcMethodCapabilities))
            .field("role", &self.role)
            .field("endpoint_url", &REDACTED)
            .field("methods", &self.methods)
            .finish()
    }
}

impl DeepXRpcMethodCapabilities {
    /// Returns the role whose endpoint supplied this evidence.
    #[must_use]
    pub const fn role(&self) -> DeepXRpcRole {
        self.role
    }

    /// Returns the identity-validated endpoint which supplied this evidence.
    #[must_use]
    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    /// Returns required methods advertised by the endpoint at observation time.
    #[must_use]
    pub const fn methods(&self) -> &BTreeSet<String> {
        &self.methods
    }
}

/// Complete role-specific RPC method evidence for one validated endpoint set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXValidatedRpcMethodCapabilities {
    submission: DeepXRpcMethodCapabilities,
    watch: DeepXRpcMethodCapabilities,
    recovery: DeepXRpcMethodCapabilities,
}

impl DeepXValidatedRpcMethodCapabilities {
    /// Returns the capabilities observed for `role`.
    #[must_use]
    pub const fn for_role(&self, role: DeepXRpcRole) -> &DeepXRpcMethodCapabilities {
        match role {
            DeepXRpcRole::Submission => &self.submission,
            DeepXRpcRole::Watch => &self.watch,
            DeepXRpcRole::Recovery => &self.recovery,
        }
    }
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

/// Errors raised while collecting complete role-specific RPC method capabilities.
#[derive(Debug, Error)]
#[error("failed to observe DeepX RPC method capabilities for role {role:?}: {source}")]
pub struct DeepXRpcMethodCapabilitiesError {
    /// Role whose identity-validated endpoint was queried.
    pub role: DeepXRpcRole,
    /// Underlying capability observation error.
    #[source]
    pub source: DeepXRpcMethodCapabilityError,
}

/// Probes required methods on one identity-validated role endpoint.
///
/// This proves only that `rpc_methods` advertised each requested name at observation time.
///
/// # Errors
///
/// Returns an error for empty requirements, request failure, or a missing advertised method.
pub async fn observe_rpc_method_capabilities(
    endpoints: &DeepXValidatedRpcEndpoints,
    role: DeepXRpcRole,
    required_methods: impl IntoIterator<Item = impl Into<String>>,
) -> Result<DeepXRpcMethodCapabilities, DeepXRpcMethodCapabilityError> {
    let required_methods: BTreeSet<String> = required_methods.into_iter().map(Into::into).collect();
    if required_methods.is_empty() {
        return Err(DeepXRpcMethodCapabilityError::EmptyRequirements(role));
    }
    let observed = get_rpc_methods(endpoints.url_for(role))
        .await
        .map_err(|source| DeepXRpcMethodCapabilityError::Rpc { role, source })?;
    for method in &required_methods {
        if !observed.contains(method) {
            return Err(DeepXRpcMethodCapabilityError::MissingMethod {
                role,
                method: method.clone(),
            });
        }
    }
    Ok(DeepXRpcMethodCapabilities {
        role,
        endpoint_url: endpoints.url_for(role).to_string(),
        methods: required_methods,
    })
}

/// Collects the required RPC method capabilities for all endpoint roles.
///
/// All three probes complete before errors are evaluated in submission, watch, recovery order.
///
/// # Errors
///
/// Returns a role-attributed error unless every validated endpoint advertises its required methods.
pub async fn observe_and_validate_rpc_method_capabilities(
    endpoints: &DeepXValidatedRpcEndpoints,
) -> Result<DeepXValidatedRpcMethodCapabilities, DeepXRpcMethodCapabilitiesError> {
    let (submission, watch, recovery) = tokio::join!(
        observe_rpc_method_capabilities(
            endpoints,
            DeepXRpcRole::Submission,
            DEEPX_SUBMISSION_RPC_METHODS.iter().copied(),
        ),
        observe_rpc_method_capabilities(
            endpoints,
            DeepXRpcRole::Watch,
            DEEPX_WATCH_RPC_METHODS.iter().copied(),
        ),
        observe_rpc_method_capabilities(
            endpoints,
            DeepXRpcRole::Recovery,
            DEEPX_RECOVERY_RPC_METHODS.iter().copied(),
        ),
    );
    let submission = submission.map_err(|source| DeepXRpcMethodCapabilitiesError {
        role: DeepXRpcRole::Submission,
        source,
    })?;
    let watch = watch.map_err(|source| DeepXRpcMethodCapabilitiesError {
        role: DeepXRpcRole::Watch,
        source,
    })?;
    let recovery = recovery.map_err(|source| DeepXRpcMethodCapabilitiesError {
        role: DeepXRpcRole::Recovery,
        source,
    })?;
    Ok(DeepXValidatedRpcMethodCapabilities {
        submission,
        watch,
        recovery,
    })
}

/// Validates complete chain-identity evidence for all configured RPC roles.
///
/// Callers must obtain each genesis hash directly from the endpoint selected for that role.
/// A result proves chain identity and URL selection, not role-specific RPC method support.
///
/// # Errors
///
/// Returns an error unless every role appears exactly once with its selected URL and all roles
/// observe the approved DeepX testnet genesis hash.
pub fn validate_rpc_endpoint_identities(
    submission_url: &str,
    watch_url: &str,
    recovery_url: &str,
    observations: impl IntoIterator<Item = DeepXObservedRpcEndpoint>,
) -> Result<DeepXValidatedRpcEndpoints, DeepXRpcEndpointValidationError> {
    let mut approved_genesis_hash = [0; 32];
    hex::decode_to_slice(
        DEEPX_TESTNET_GENESIS_HASH
            .strip_prefix("0x")
            .unwrap_or(DEEPX_TESTNET_GENESIS_HASH),
        &mut approved_genesis_hash,
    )
    .map_err(|_| DeepXRpcEndpointValidationError::InvalidApprovedGenesisHash)?;

    let mut submission = None;
    let mut watch = None;
    let mut recovery = None;
    let mut observed_genesis_hash = None;

    for observation in observations {
        let expected_url = match observation.role {
            DeepXRpcRole::Submission => submission_url,
            DeepXRpcRole::Watch => watch_url,
            DeepXRpcRole::Recovery => recovery_url,
        };
        if observation.url != expected_url {
            return Err(DeepXRpcEndpointValidationError::UrlMismatch(
                observation.role,
            ));
        }
        if observation.genesis_hash != approved_genesis_hash
            || observed_genesis_hash.is_some_and(|hash| hash != observation.genesis_hash)
        {
            return Err(DeepXRpcEndpointValidationError::GenesisHashMismatch(
                observation.role,
            ));
        }
        observed_genesis_hash = Some(observation.genesis_hash);
        let role_url = match observation.role {
            DeepXRpcRole::Submission => &mut submission,
            DeepXRpcRole::Watch => &mut watch,
            DeepXRpcRole::Recovery => &mut recovery,
        };
        if role_url.replace(observation.url).is_some() {
            return Err(DeepXRpcEndpointValidationError::DuplicateRole(
                observation.role,
            ));
        }
    }

    Ok(DeepXValidatedRpcEndpoints {
        submission_url: submission.ok_or(DeepXRpcEndpointValidationError::MissingRole(
            DeepXRpcRole::Submission,
        ))?,
        watch_url: watch.ok_or(DeepXRpcEndpointValidationError::MissingRole(
            DeepXRpcRole::Watch,
        ))?,
        recovery_url: recovery.ok_or(DeepXRpcEndpointValidationError::MissingRole(
            DeepXRpcRole::Recovery,
        ))?,
        genesis_hash: observed_genesis_hash.ok_or(DeepXRpcEndpointValidationError::MissingRole(
            DeepXRpcRole::Submission,
        ))?,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{Json, Router, routing::post};
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;

    fn approved_hash() -> [u8; 32] {
        let mut hash = [0; 32];
        hex::decode_to_slice(&DEEPX_TESTNET_GENESIS_HASH[2..], &mut hash).unwrap();
        hash
    }

    fn observations() -> [DeepXObservedRpcEndpoint; 3] {
        [
            DeepXObservedRpcEndpoint::new(
                DeepXRpcRole::Submission,
                "https://submit.example.invalid/secret".to_string(),
                approved_hash(),
            ),
            DeepXObservedRpcEndpoint::new(
                DeepXRpcRole::Watch,
                "https://watch.example.invalid/secret".to_string(),
                approved_hash(),
            ),
            DeepXObservedRpcEndpoint::new(
                DeepXRpcRole::Recovery,
                "https://recover.example.invalid/secret".to_string(),
                approved_hash(),
            ),
        ]
    }

    fn validate(
        observations: impl IntoIterator<Item = DeepXObservedRpcEndpoint>,
    ) -> Result<DeepXValidatedRpcEndpoints, DeepXRpcEndpointValidationError> {
        validate_rpc_endpoint_identities(
            "https://submit.example.invalid/secret",
            "https://watch.example.invalid/secret",
            "https://recover.example.invalid/secret",
            observations,
        )
    }

    fn validated_local_endpoint(url: &str) -> DeepXValidatedRpcEndpoints {
        let observations = [
            DeepXObservedRpcEndpoint::new(
                DeepXRpcRole::Submission,
                url.to_string(),
                approved_hash(),
            ),
            DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, url.to_string(), approved_hash()),
            DeepXObservedRpcEndpoint::new(DeepXRpcRole::Recovery, url.to_string(), approved_hash()),
        ];
        validate_rpc_endpoint_identities(url, url, url, observations).unwrap()
    }

    async fn methods_server(methods: BTreeSet<String>, calls: Arc<AtomicUsize>) -> String {
        let app = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let methods = methods.clone();
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(request["method"], "rpc_methods");
                    Json(json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {"methods": methods},
                    }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    #[rstest]
    #[tokio::test]
    async fn method_probe_rejects_empty_requirements_before_network_access() {
        let endpoints = validated_local_endpoint("http://127.0.0.1:1");
        assert!(matches!(
            observe_rpc_method_capabilities(
                &endpoints,
                DeepXRpcRole::Recovery,
                std::iter::empty::<&str>(),
            )
            .await,
            Err(DeepXRpcMethodCapabilityError::EmptyRequirements(
                DeepXRpcRole::Recovery
            ))
        ));
    }

    #[rstest]
    #[tokio::test]
    async fn method_probe_returns_typed_role_evidence_and_rejects_missing_method() {
        let calls = Arc::new(AtomicUsize::new(0));
        let url = methods_server(
            BTreeSet::from(["chain_getHeader".to_string()]),
            Arc::clone(&calls),
        )
        .await;
        let endpoints = validated_local_endpoint(&url);
        let observed =
            observe_rpc_method_capabilities(&endpoints, DeepXRpcRole::Watch, ["chain_getHeader"])
                .await
                .unwrap();
        assert_eq!(observed.role(), DeepXRpcRole::Watch);
        assert_eq!(observed.endpoint_url(), url);
        assert_eq!(
            observed.methods(),
            &BTreeSet::from(["chain_getHeader".to_string()])
        );
        assert!(!format!("{observed:?}").contains(&url));
        assert!(matches!(
            observe_rpc_method_capabilities(
                &endpoints,
                DeepXRpcRole::Watch,
                ["chain_getHeader", "state_call"],
            )
            .await,
            Err(DeepXRpcMethodCapabilityError::MissingMethod {
                role: DeepXRpcRole::Watch,
                method,
            }) if method == "state_call"
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[rstest]
    #[tokio::test]
    async fn shared_endpoint_is_probed_for_each_role() {
        let methods = DEEPX_SUBMISSION_RPC_METHODS
            .iter()
            .chain(DEEPX_WATCH_RPC_METHODS)
            .chain(DEEPX_RECOVERY_RPC_METHODS)
            .map(|method| (*method).to_string())
            .collect();
        let calls = Arc::new(AtomicUsize::new(0));
        let url = methods_server(methods, Arc::clone(&calls)).await;
        let endpoints = validated_local_endpoint(&url);
        let observed = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        for (role, required) in [
            (DeepXRpcRole::Submission, DEEPX_SUBMISSION_RPC_METHODS),
            (DeepXRpcRole::Watch, DEEPX_WATCH_RPC_METHODS),
            (DeepXRpcRole::Recovery, DEEPX_RECOVERY_RPC_METHODS),
        ] {
            assert_eq!(observed.for_role(role).role(), role);
            assert_eq!(observed.for_role(role).endpoint_url(), url);
            assert_eq!(observed.for_role(role).methods().len(), required.len());
        }
    }

    #[rstest]
    fn complete_evidence_returns_redacted_endpoints() {
        let endpoints = validate(observations()).unwrap();
        assert_eq!(
            endpoints.url_for(DeepXRpcRole::Watch),
            "https://watch.example.invalid/secret"
        );
        assert_eq!(endpoints.genesis_hash(), approved_hash());
        assert!(!format!("{endpoints:?}").contains("secret"));
        assert!(!format!("{:?}", observations()[0]).contains("secret"));
    }

    #[rstest]
    #[case::submission(DeepXRpcRole::Submission)]
    #[case::watch(DeepXRpcRole::Watch)]
    #[case::recovery(DeepXRpcRole::Recovery)]
    fn wrong_chain_is_rejected_for_each_role(#[case] role: DeepXRpcRole) {
        let mut observed = observations();
        observed
            .iter_mut()
            .find(|item| item.role == role)
            .unwrap()
            .genesis_hash = [9; 32];
        assert_eq!(
            validate(observed),
            Err(DeepXRpcEndpointValidationError::GenesisHashMismatch(role))
        );
    }

    #[rstest]
    fn missing_duplicate_and_foreign_urls_are_rejected() {
        let [submission, watch, recovery] = observations();
        assert_eq!(
            validate([submission.clone(), recovery.clone()]),
            Err(DeepXRpcEndpointValidationError::MissingRole(
                DeepXRpcRole::Watch
            ))
        );
        assert_eq!(
            validate([submission.clone(), submission.clone(), watch]),
            Err(DeepXRpcEndpointValidationError::DuplicateRole(
                DeepXRpcRole::Submission
            ))
        );
        assert_eq!(
            validate([
                DeepXObservedRpcEndpoint::new(
                    DeepXRpcRole::Submission,
                    "https://other.example.invalid".to_string(),
                    approved_hash(),
                ),
                submission,
                recovery,
            ]),
            Err(DeepXRpcEndpointValidationError::UrlMismatch(
                DeepXRpcRole::Submission
            ))
        );
    }
}
