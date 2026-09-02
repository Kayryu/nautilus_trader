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

//! Immutable runtime identity and metadata used by offline signing.

use super::DeepXRuntimeConfig;
use aws_lc_rs::digest::{SHA256, digest};
use nautilus_core::hex;
use scale_info::TypeDef;
use subxt_core::{
    client::{ClientState, RuntimeVersion},
    metadata,
    metadata::Metadata,
    utils::H256,
};
use thiserror::Error;

const TESTNET_GENESIS_HASH: &str =
    "86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b";
const TESTNET_METADATA_SHA256: &str =
    "e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37";
const TESTNET_SPEC_VERSION: u32 = 366;
const TESTNET_TRANSACTION_VERSION: u32 = 1;
const TESTNET_SIGNED_EXTENSIONS: &[&str] = &[
    "CheckNonZeroSender",
    "CheckSpecVersion",
    "CheckTxVersion",
    "CheckGenesis",
    "CheckMortality",
    "CheckNonce",
    "CheckWeight",
    "ChargeTransactionPayment",
    "CheckPriority",
];

/// Runtime identity approved by the captured DeepX testnet fixture set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovedRuntimeIdentity {
    /// Testnet genesis hash.
    pub genesis_hash: [u8; 32],
    /// SHA-256 of the complete SCALE metadata bytes.
    pub metadata_sha256: [u8; 32],
    /// Runtime specification version.
    pub spec_version: u32,
    /// Runtime transaction version.
    pub transaction_version: u32,
    /// Signed extensions in metadata order.
    pub signed_extensions: Vec<String>,
}

/// Errors produced while validating an immutable runtime snapshot.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// The pinned identity constant could not be decoded.
    #[error("invalid built-in DeepX runtime identity")]
    InvalidApprovedIdentity,
    /// The SCALE metadata cannot be decoded by the pinned DeepX Subxt fork.
    #[error("invalid DeepX runtime metadata: {0}")]
    InvalidMetadata(#[from] parity_scale_codec::Error),
    /// The metadata does not match the approved testnet SHA-256.
    #[error("DeepX runtime metadata hash is not approved")]
    MetadataHashMismatch,
    /// The observed deployment or runtime version differs from the approved fixture.
    #[error("DeepX genesis or runtime version is not approved")]
    RuntimeIdentityMismatch,
    /// The signed-extension sequence differs from the approved fixture.
    #[error("DeepX runtime signed-extension sequence is not approved")]
    SignedExtensionsMismatch,
    /// An unknown non-empty transaction extension would make signing ambiguous.
    #[error("unsupported non-empty DeepX transaction extension: {0}")]
    UnsupportedTransactionExtension(String),
}

/// An immutable metadata and runtime-version snapshot for deterministic signing.
#[derive(Clone, Debug)]
pub struct RuntimeSnapshot {
    identity: ApprovedRuntimeIdentity,
    client_state: ClientState<DeepXRuntimeConfig>,
}

impl RuntimeSnapshot {
    /// Builds the only runtime identity currently approved for DeepX testnet signing.
    ///
    /// # Errors
    ///
    /// Returns an error unless the observed genesis, runtime versions, metadata hash, extension
    /// order, and extension encodings match the captured finalized fixture.
    pub fn approved_testnet(
        observed_genesis_hash: [u8; 32],
        observed_spec_version: u32,
        observed_transaction_version: u32,
        metadata_bytes: &[u8],
    ) -> Result<Self, SnapshotError> {
        let genesis_hash = decode_32(TESTNET_GENESIS_HASH)?;
        if observed_genesis_hash != genesis_hash
            || observed_spec_version != TESTNET_SPEC_VERSION
            || observed_transaction_version != TESTNET_TRANSACTION_VERSION
        {
            return Err(SnapshotError::RuntimeIdentityMismatch);
        }
        let approved_metadata_hash = decode_32(TESTNET_METADATA_SHA256)?;
        let actual_metadata_hash: [u8; 32] = digest(&SHA256, metadata_bytes)
            .as_ref()
            .try_into()
            .map_err(|_| SnapshotError::InvalidApprovedIdentity)?;
        if actual_metadata_hash != approved_metadata_hash {
            return Err(SnapshotError::MetadataHashMismatch);
        }

        let metadata = metadata::decode_from(metadata_bytes)?;
        let extension_metadata = metadata
            .extrinsic()
            .transaction_extensions_to_use_for_encoding()
            .collect::<Vec<_>>();
        let signed_extensions = extension_metadata
            .iter()
            .map(|extension| extension.identifier().to_string())
            .collect::<Vec<_>>();
        if signed_extensions != TESTNET_SIGNED_EXTENSIONS {
            return Err(SnapshotError::SignedExtensionsMismatch);
        }
        validate_unknown_extensions(&metadata)?;

        let identity = ApprovedRuntimeIdentity {
            genesis_hash,
            metadata_sha256: actual_metadata_hash,
            spec_version: TESTNET_SPEC_VERSION,
            transaction_version: TESTNET_TRANSACTION_VERSION,
            signed_extensions,
        };
        let client_state = ClientState {
            metadata,
            genesis_hash: H256::from(genesis_hash),
            runtime_version: RuntimeVersion {
                spec_version: TESTNET_SPEC_VERSION,
                transaction_version: TESTNET_TRANSACTION_VERSION,
            },
        };

        Ok(Self {
            identity,
            client_state,
        })
    }

    /// Returns the approved identity associated with this snapshot.
    #[must_use]
    pub const fn identity(&self) -> &ApprovedRuntimeIdentity {
        &self.identity
    }

    pub(super) const fn client_state(&self) -> &ClientState<DeepXRuntimeConfig> {
        &self.client_state
    }
}

fn validate_unknown_extensions(metadata: &Metadata) -> Result<(), SnapshotError> {
    const SUPPORTED: &[&str] = &[
        "CheckSpecVersion",
        "CheckTxVersion",
        "CheckNonce",
        "CheckGenesis",
        "CheckMortality",
        "ChargeAssetTxPayment",
        "ChargeTransactionPayment",
        "CheckMetadataHash",
    ];

    for extension in metadata
        .extrinsic()
        .transaction_extensions_to_use_for_encoding()
    {
        if !SUPPORTED.contains(&extension.identifier())
            && (!is_empty_type(extension.extra_ty(), metadata)
                || !is_empty_type(extension.additional_ty(), metadata))
        {
            return Err(SnapshotError::UnsupportedTransactionExtension(
                extension.identifier().to_string(),
            ));
        }
    }
    Ok(())
}

fn is_empty_type(type_id: u32, metadata: &Metadata) -> bool {
    let Some(ty) = metadata.types().resolve(type_id) else {
        return false;
    };
    match &ty.type_def {
        TypeDef::Composite(value) => value
            .fields
            .iter()
            .all(|field| is_empty_type(field.ty.id, metadata)),
        TypeDef::Array(value) => value.len == 0 || is_empty_type(value.type_param.id, metadata),
        TypeDef::Tuple(value) => value
            .fields
            .iter()
            .all(|field| is_empty_type(field.id, metadata)),
        TypeDef::BitSequence(_)
        | TypeDef::Variant(_)
        | TypeDef::Sequence(_)
        | TypeDef::Compact(_)
        | TypeDef::Primitive(_) => false,
    }
}

fn decode_32(value: &str) -> Result<[u8; 32], SnapshotError> {
    hex::decode_array(value).map_err(|_| SnapshotError::InvalidApprovedIdentity)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde::Deserialize;

    use super::*;

    #[derive(Deserialize)]
    struct RpcResponse {
        result: String,
    }

    fn metadata_bytes() -> Vec<u8> {
        let response: RpcResponse = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json",
        )))
        .unwrap();
        hex::decode(response.result.trim_start_matches("0x")).unwrap()
    }

    #[rstest]
    fn accepts_the_approved_finalized_testnet_metadata() {
        let snapshot = RuntimeSnapshot::approved_testnet(
            decode_32(TESTNET_GENESIS_HASH).unwrap(),
            TESTNET_SPEC_VERSION,
            TESTNET_TRANSACTION_VERSION,
            &metadata_bytes(),
        )
        .unwrap();

        assert_eq!(snapshot.identity().spec_version, 366);
        assert_eq!(snapshot.identity().transaction_version, 1);
        assert_eq!(
            snapshot.identity().signed_extensions,
            TESTNET_SIGNED_EXTENSIONS
        );
    }

    #[rstest]
    fn rejects_metadata_outside_the_approved_fixture_identity() {
        let mut bytes = metadata_bytes();
        bytes[100] ^= 1;

        assert!(matches!(
            RuntimeSnapshot::approved_testnet(
                decode_32(TESTNET_GENESIS_HASH).unwrap(),
                TESTNET_SPEC_VERSION,
                TESTNET_TRANSACTION_VERSION,
                &bytes,
            ),
            Err(SnapshotError::MetadataHashMismatch),
        ));
    }

    #[rstest]
    fn rejects_runtime_versions_outside_the_approved_fixture_identity() {
        assert!(matches!(
            RuntimeSnapshot::approved_testnet(
                decode_32(TESTNET_GENESIS_HASH).unwrap(),
                TESTNET_SPEC_VERSION + 1,
                TESTNET_TRANSACTION_VERSION,
                &metadata_bytes(),
            ),
            Err(SnapshotError::RuntimeIdentityMismatch),
        ));
    }
}
