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
    let enabled = observe_spot_margin_with_client(
        &client,
        observation.checkpoint(),
        observation.snapshot().metadata(),
        subaccount,
        expected_authority,
    )
    .await?;
    Ok(DeepXFinalizedSpotMarginEvidence {
        checkpoint: observation.checkpoint(),
        runtime_identity: observation.snapshot().identity().clone(),
        subaccount,
        authority: expected_authority,
        enabled,
    })
}

async fn observe_spot_margin_with_client(
    client: &DeepXRpcClient,
    checkpoint: DeepXFinalizedCheckpoint,
    metadata: &subxt_core::Metadata,
    subaccount: [u8; 20],
    expected_authority: [u8; 20],
) -> Result<bool, DeepXFinalizedSpotMarginError> {
    get_spot_margin_at_block(
        client,
        checkpoint.block_hash(),
        checkpoint.block_number(),
        metadata,
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
    })
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, routing::post};
    use deepx_rust_sdk::rpc::spot_margin::{decode_spot_margin, subaccount_info_storage_key};
    use frame_metadata::{
        RuntimeMetadata, RuntimeMetadataPrefixed,
        v14::{StorageEntryModifier, StorageEntryType},
    };
    use nautilus_core::hex;
    use parity_scale_codec::Decode;
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;
    use crate::DeepXReadOnlyRuntimeSnapshot;

    const WALLET: [u8; 20] = [
        0x78, 0x1e, 0xd3, 0x5b, 0x16, 0x70, 0x68, 0xc9, 0x3d, 0xfa, 0xda, 0xb4, 0x1d, 0xfb, 0x68,
        0x0e, 0xda, 0xca, 0x4e, 0x50,
    ];
    const SUBACCOUNT: [u8; 20] = [
        0x4d, 0xed, 0x31, 0xcb, 0x63, 0x94, 0x9b, 0x52, 0xf9, 0xdf, 0xc9, 0xbc, 0xfa, 0xde, 0x4e,
        0xab, 0x70, 0x17, 0xea, 0xdc,
    ];
    const STORAGE: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e504ded31cb63949b52f9dfc9bcfade4eab7017eadc146b617a656501000100000000000100000000";
    const BLOCK_HASH: &str = "0xbb48406ad92b4a214f64b71b9ac95052df793113ef82b5e7df744dd44546f0e4";
    const BLOCK_NUMBER: u32 = 217_394_490;

    fn metadata_bytes() -> Vec<u8> {
        let fixture = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        ));
        let fixture: serde_json::Value = serde_json::from_str(fixture).unwrap();
        hex::decode(
            fixture["result"]
                .as_str()
                .unwrap()
                .strip_prefix("0x")
                .unwrap(),
        )
        .unwrap()
    }

    fn metadata() -> subxt_core::Metadata {
        DeepXReadOnlyRuntimeSnapshot::built_in_testnet()
            .unwrap()
            .metadata()
            .clone()
    }

    #[rstest::rstest]
    fn finalized_spot_margin_key_uses_identity_account_id20() {
        assert_eq!(
            subaccount_info_storage_key(SUBACCOUNT),
            "0x7a722acdbb19d1db692588a0209d14499ac3b2b9666a5eaa79e0ca457b2ea3a94ded31cb63949b52f9dfc9bcfade4eab7017eadc"
        );
    }

    #[rstest::rstest]
    fn retained_runtime_uses_identity_subaccount_info_storage() {
        let bytes = metadata_bytes();
        let RuntimeMetadataPrefixed(_, RuntimeMetadata::V14(metadata)) =
            RuntimeMetadataPrefixed::decode(&mut bytes.as_slice()).unwrap()
        else {
            panic!("approved runtime metadata must be V14");
        };
        let pallet = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.name == "Subaccount")
            .unwrap();
        let storage = pallet.storage.as_ref().unwrap();
        assert_eq!(storage.prefix, "Subaccount");
        let entry = storage
            .entries
            .iter()
            .find(|entry| entry.name == "SubaccountInfo")
            .unwrap();
        assert_eq!(entry.modifier, StorageEntryModifier::Optional);
        let StorageEntryType::Map {
            hashers,
            key,
            value,
        } = &entry.ty
        else {
            panic!("SubaccountInfo must be a storage map");
        };
        assert!(matches!(
            hashers.as_slice(),
            [frame_metadata::v14::StorageHasher::Identity]
        ));
        let account_id = metadata.types.resolve(key.id).unwrap();
        assert_eq!(
            account_id.path.segments.last().map(String::as_str),
            Some("H160")
        );
        let account_info = metadata.types.resolve(value.id).unwrap();
        assert_eq!(
            account_info.path.segments.last().map(String::as_str),
            Some("AccountInfo")
        );
    }

    #[rstest::rstest]
    fn decodes_captured_finalized_subaccount_spot_margin() {
        let metadata = metadata();
        assert_eq!(
            decode_spot_margin(&metadata, STORAGE, SUBACCOUNT, WALLET),
            Some(false)
        );
        let mut enabled = hex::decode(STORAGE.strip_prefix("0x").unwrap()).unwrap();
        enabled[52] = 1;
        assert_eq!(
            decode_spot_margin(
                &metadata,
                &format!("0x{}", hex::encode(&enabled)),
                SUBACCOUNT,
                WALLET,
            ),
            Some(true)
        );
    }

    #[rstest::rstest]
    fn rejects_foreign_or_malformed_subaccount_state() {
        let metadata = metadata();
        assert_eq!(
            decode_spot_margin(&metadata, STORAGE, [0; 20], WALLET),
            None
        );
        assert_eq!(
            decode_spot_margin(&metadata, STORAGE, SUBACCOUNT, [0; 20]),
            None
        );
        assert_eq!(
            decode_spot_margin(&metadata, &format!("{STORAGE}00"), SUBACCOUNT, WALLET),
            None
        );
        assert_eq!(
            decode_spot_margin(&metadata, &STORAGE[..STORAGE.len() - 2], SUBACCOUNT, WALLET),
            None
        );
    }

    #[rstest::rstest]
    #[case::consistent(Some(STORAGE), Some(BLOCK_HASH), None)]
    #[case::changed_storage(Some("0x00"), Some(BLOCK_HASH), Some("storage"))]
    #[case::missing_storage(None, Some(BLOCK_HASH), Some("storage"))]
    #[case::missing_canonical_hash(Some(STORAGE), None, Some("canonical"))]
    #[case::changed_canonical_hash(
        Some(STORAGE),
        Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
        Some("canonical")
    )]
    #[tokio::test]
    async fn finalized_spot_margin_reads_consistent_storage_at_one_hash(
        #[case] direct_storage: Option<&str>,
        #[case] canonical_hash: Option<&str>,
        #[case] failure: Option<&str>,
    ) {
        let key = subaccount_info_storage_key(SUBACCOUNT);
        let direct_storage = direct_storage.map(str::to_string);
        let canonical_hash = canonical_hash.map(str::to_string);
        let router = Router::new().route(
            "/",
            post(move |Json(request): Json<serde_json::Value>| {
                let key = key.clone();
                let direct_storage = direct_storage.clone();
                let canonical_hash = canonical_hash.clone();
                async move {
                    assert_eq!(request["jsonrpc"], "2.0");
                    assert_eq!(request["id"], 1);
                    let result = match request["method"].as_str().unwrap() {
                        "state_queryStorageAt" => {
                            assert_eq!(request["params"], json!([[key], BLOCK_HASH]));
                            json!([{
                                "block": BLOCK_HASH,
                                "changes": [[subaccount_info_storage_key(SUBACCOUNT), STORAGE]],
                            }])
                        }
                        "state_getStorage" => {
                            assert_eq!(request["params"], json!([key, BLOCK_HASH]));
                            json!(direct_storage)
                        }
                        "chain_getBlockHash" => {
                            assert_eq!(request["params"], json!([BLOCK_NUMBER]));
                            json!(canonical_hash)
                        }
                        other => panic!("unexpected RPC method: {other}"),
                    };
                    Json(json!({"jsonrpc": "2.0", "id": 1, "result": result}))
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRpcClient::new(format!("http://{address}"));
        let checkpoint = DeepXFinalizedCheckpoint {
            block_hash: hex::decode_array(BLOCK_HASH.strip_prefix("0x").unwrap()).unwrap(),
            block_number: BLOCK_NUMBER,
        };

        let result =
            observe_spot_margin_with_client(&client, checkpoint, &metadata(), SUBACCOUNT, WALLET)
                .await;
        match failure {
            None => assert!(!result.unwrap()),
            Some("storage") => assert!(matches!(
                result,
                Err(DeepXFinalizedSpotMarginError::InconsistentStorage)
            )),
            Some("canonical") => assert!(matches!(
                result,
                Err(DeepXFinalizedSpotMarginError::CanonicalHashMismatch)
            )),
            _ => unreachable!(),
        }
        server.abort();
    }
}
