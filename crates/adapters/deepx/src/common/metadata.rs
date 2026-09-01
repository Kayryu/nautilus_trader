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

//! Structured extraction of signing metadata from DeepX runtime metadata.

use frame_metadata::{META_RESERVED, RuntimeMetadata, RuntimeMetadataPrefixed};
use parity_scale_codec::DecodeAll;
use thiserror::Error;

/// Errors produced while extracting signed-extension metadata.
#[derive(Debug, Error)]
pub enum MetadataError {
    /// The SCALE metadata could not be decoded completely.
    #[error("invalid SCALE runtime metadata: {0}")]
    Decode(#[from] parity_scale_codec::Error),
    /// The runtime metadata version is not supported by this adapter.
    #[error("unsupported runtime metadata version {0}; expected version 14")]
    UnsupportedVersion(u32),
    /// The metadata prefix does not contain the expected magic value.
    #[error("invalid runtime metadata prefix {0:#010x}")]
    InvalidPrefix(u32),
    /// A signed extension has no usable identifier.
    #[error("runtime metadata contains a blank signed-extension identifier at index {0}")]
    BlankIdentifier(usize),
}

/// Extracts signed-extension identifiers in the order declared by V14 runtime metadata.
pub fn signed_extension_identifiers(metadata_bytes: &[u8]) -> Result<Vec<String>, MetadataError> {
    decode_v14(metadata_bytes)?
        .extrinsic
        .signed_extensions
        .into_iter()
        .enumerate()
        .map(|(index, extension)| {
            let identifier = extension.identifier;
            if identifier.trim().is_empty() {
                Err(MetadataError::BlankIdentifier(index))
            } else {
                Ok(identifier)
            }
        })
        .collect::<Result<_, _>>()
}

fn decode_v14(
    metadata_bytes: &[u8],
) -> Result<frame_metadata::v14::RuntimeMetadataV14, MetadataError> {
    let RuntimeMetadataPrefixed(prefix, metadata) =
        RuntimeMetadataPrefixed::decode_all(&mut &metadata_bytes[..])?;
    if prefix != META_RESERVED {
        return Err(MetadataError::InvalidPrefix(prefix));
    }
    let RuntimeMetadata::V14(metadata) = metadata else {
        return Err(MetadataError::UnsupportedVersion(metadata.version()));
    };
    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use frame_metadata::OpaqueMetadata;
    use nautilus_core::hex;
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde::Deserialize;

    use super::*;

    const FINALIZED_METADATA: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test_data/runtime/testnet/",
        "genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json",
    ));

    #[derive(Deserialize)]
    struct MetadataResponse {
        result: String,
    }

    fn finalized_metadata_bytes() -> Vec<u8> {
        let response: MetadataResponse = serde_json::from_str(FINALIZED_METADATA).unwrap();
        hex::decode(response.result.trim_start_matches("0x")).unwrap()
    }

    #[rstest]
    fn extracts_ordered_signed_extensions_from_finalized_fixture() {
        let identifiers = signed_extension_identifiers(&finalized_metadata_bytes()).unwrap();

        assert_eq!(
            identifiers,
            [
                "CheckNonZeroSender",
                "CheckSpecVersion",
                "CheckTxVersion",
                "CheckGenesis",
                "CheckMortality",
                "CheckNonce",
                "CheckWeight",
                "ChargeTransactionPayment",
                "CheckPriority",
            ],
        );
    }

    #[rstest]
    fn rejects_malformed_metadata() {
        assert!(matches!(
            signed_extension_identifiers(&[0x6d, 0x65]),
            Err(MetadataError::Decode(_)),
        ));
    }

    #[rstest]
    fn rejects_trailing_metadata_bytes() {
        let mut metadata = finalized_metadata_bytes();
        metadata.push(0);

        assert!(matches!(
            signed_extension_identifiers(&metadata),
            Err(MetadataError::Decode(_)),
        ));
    }

    #[rstest]
    fn rejects_invalid_metadata_prefix() {
        let mut metadata = finalized_metadata_bytes();
        metadata[0] ^= 0xff;

        assert!(matches!(
            signed_extension_identifiers(&metadata),
            Err(MetadataError::InvalidPrefix(_)),
        ));
    }

    #[rstest]
    fn rejects_unsupported_metadata_version() {
        let metadata = RuntimeMetadataPrefixed(
            META_RESERVED,
            RuntimeMetadata::V13(OpaqueMetadata(Vec::new())),
        )
        .encode();

        assert!(matches!(
            signed_extension_identifiers(&metadata),
            Err(MetadataError::UnsupportedVersion(13)),
        ));
    }
}
