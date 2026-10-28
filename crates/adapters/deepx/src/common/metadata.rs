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

pub use deepx_rust_sdk::metadata::{MetadataError, signed_extension_identifiers};

#[cfg(test)]
mod tests {
    use frame_metadata::{META_RESERVED, OpaqueMetadata, RuntimeMetadata, RuntimeMetadataPrefixed};
    use nautilus_core::hex;
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde::Deserialize;

    use super::*;

    const FINALIZED_METADATA: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/deepx-rust-sdk/test_data/runtime/testnet/",
        "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
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
                "CheckNonceEra",
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
