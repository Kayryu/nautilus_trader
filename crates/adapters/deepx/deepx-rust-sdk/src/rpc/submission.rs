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

//! One-shot extrinsic submission with exact payload identity verification.

use parity_scale_codec::{Compact, Decode};
use serde_json::Value;
use subxt_core::config::{Hasher, substrate::BlakeTwo256};
use thiserror::Error;

use super::{DeepXRpcClient, SUBMIT_EXTRINSIC_METHOD};
use crate::rest::decode_transaction_acknowledgement;

/// Errors raised while submitting one signed DeepX extrinsic.
#[derive(Debug, Error)]
pub enum DeepXSubmissionError {
    /// The request or JSON-RPC response failed.
    #[error("failed to submit DeepX extrinsic via `{method}`: {source}")]
    Rpc {
        /// Method which failed.
        method: &'static str,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The submission response was not a single hex-encoded hash string.
    #[error("DeepX submission response was not a 0x-prefixed 32-byte hash: {value}")]
    InvalidResponse {
        /// Raw JSON value returned by the node.
        value: String,
    },
    /// The node-returned hash did not match the submitted extrinsic.
    #[error(
        "DeepX submission hash mismatch: node {}, extrinsic {}, recomputed {}",
        hex::encode(node_hash),
        hex::encode(extrinsic_hash),
        hex::encode(recomputed_hash)
    )]
    HashMismatch {
        /// Blake2-256 hash returned by the submission node.
        node_hash: [u8; 32],
        /// Blake2-256 hash recorded with the signed extrinsic.
        extrinsic_hash: [u8; 32],
        /// Blake2-256 hash recomputed from the submitted extrinsic bytes.
        recomputed_hash: [u8; 32],
    },
}

/// Failures while scanning a complete block or pending-pool extrinsic list.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeepXExtrinsicLookupError {
    /// An entry was not one complete hex-encoded, length-prefixed SCALE extrinsic.
    #[error("invalid DeepX extrinsic at index {index}")]
    InvalidEntry { index: usize },
    /// The exact target extrinsic appeared more than once.
    #[error("duplicate DeepX target extrinsic")]
    DuplicateTarget,
}

/// Searches a complete list for one exact Blake2-256 extrinsic hash.
///
/// Every entry is checked before absence is returned; a malformed entry cannot establish absence.
///
/// # Errors
///
/// Returns an error for malformed SCALE length framing or duplicate target entries.
pub fn find_extrinsic_by_hash(
    encoded_extrinsics: &[String],
    target_hash: [u8; 32],
) -> Result<Option<u32>, DeepXExtrinsicLookupError> {
    let mut matched = None;
    for (index, encoded) in encoded_extrinsics.iter().enumerate() {
        let bytes = encoded
            .strip_prefix("0x")
            .and_then(|value| hex::decode(value).ok())
            .ok_or(DeepXExtrinsicLookupError::InvalidEntry { index })?;
        let mut remaining = bytes.as_slice();
        let payload_length = Compact::<u32>::decode(&mut remaining)
            .map_err(|_| DeepXExtrinsicLookupError::InvalidEntry { index })?
            .0;
        if usize::try_from(payload_length).ok() != Some(remaining.len()) {
            return Err(DeepXExtrinsicLookupError::InvalidEntry { index });
        }
        if BlakeTwo256.hash(&bytes).0 == target_hash {
            let index = u32::try_from(index)
                .map_err(|_| DeepXExtrinsicLookupError::InvalidEntry { index })?;
            if matched.replace(index).is_some() {
                return Err(DeepXExtrinsicLookupError::DuplicateTarget);
            }
        }
    }
    Ok(matched)
}

/// Evidence that a node accepted the exact submitted extrinsic into its pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSubmittedExtrinsic {
    node_hash: [u8; 32],
    extrinsic_hash: [u8; 32],
}

/// Hash-verified REST acknowledgement with backend action data retained verbatim.
///
/// This proves acceptance of the submitted bytes, not inclusion or finality.
#[derive(Debug)]
pub struct DeepXRestSubmissionAcknowledgement {
    /// Hash-verified acceptance of the exact submitted extrinsic.
    pub submitted: DeepXSubmittedExtrinsic,
    /// Backend action result without inferred confirmation semantics.
    pub data: Value,
}

/// Verifies a successful REST acknowledgement against the exact signed extrinsic.
///
/// # Errors
///
/// Returns an error for a backend failure, malformed or missing hash, or hash mismatch.
pub fn verify_rest_acknowledgement(
    body: &[u8],
    bytes: &[u8],
    expected_hash: [u8; 32],
) -> anyhow::Result<DeepXRestSubmissionAcknowledgement> {
    let response = decode_transaction_acknowledgement(body)?;
    let node_hash = decode_submission_hash(&response.tx_hash)?;
    Ok(DeepXRestSubmissionAcknowledgement {
        submitted: DeepXSubmittedExtrinsic::verified(bytes, expected_hash, node_hash)?,
        data: response.data,
    })
}

impl DeepXSubmittedExtrinsic {
    /// Verifies the submitted bytes against both recorded and node-returned hashes.
    ///
    /// # Errors
    ///
    /// Returns a hash mismatch if any of the three identities differ.
    pub fn verified(
        bytes: &[u8],
        extrinsic_hash: [u8; 32],
        node_hash: [u8; 32],
    ) -> Result<Self, DeepXSubmissionError> {
        verify_submission_hash(bytes, extrinsic_hash, node_hash)?;
        Ok(Self {
            node_hash,
            extrinsic_hash,
        })
    }

    /// Blake2-256 hash returned by the submission node.
    #[must_use]
    pub const fn node_hash(&self) -> [u8; 32] {
        self.node_hash
    }

    /// Blake2-256 hash recorded with the signed extrinsic.
    #[must_use]
    pub const fn extrinsic_hash(&self) -> [u8; 32] {
        self.extrinsic_hash
    }
}

/// Verifies both claimed hashes against the exact submitted bytes.
///
/// # Errors
///
/// Returns a hash mismatch if any of the three identities differ.
pub fn verify_submission_hash(
    extrinsic_bytes: &[u8],
    expected_hash: [u8; 32],
    node_hash: [u8; 32],
) -> Result<(), DeepXSubmissionError> {
    let recomputed_hash = BlakeTwo256.hash(extrinsic_bytes).0;
    if recomputed_hash == expected_hash && recomputed_hash == node_hash {
        Ok(())
    } else {
        Err(DeepXSubmissionError::HashMismatch {
            node_hash,
            extrinsic_hash: expected_hash,
            recomputed_hash,
        })
    }
}

/// Decodes a node-returned 0x-prefixed 32-byte hash.
///
/// # Errors
///
/// Returns an invalid-response error for a malformed hash.
pub fn decode_submission_hash(encoded: &str) -> Result<[u8; 32], DeepXSubmissionError> {
    encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| DeepXSubmissionError::InvalidResponse {
            value: encoded.to_string(),
        })
}

impl DeepXRpcClient {
    /// Submits exact extrinsic bytes once and verifies the returned hash.
    ///
    /// This proves node pool acceptance only, not inclusion or finality. No retry is attempted.
    ///
    /// # Errors
    ///
    /// Returns a local hash mismatch before sending, or a transport, response, or returned-hash
    /// error after the request starts. Callers must reconcile uncertain delivery.
    pub async fn submit_verified_extrinsic_once(
        &self,
        bytes: &[u8],
        expected_hash: [u8; 32],
    ) -> Result<DeepXSubmittedExtrinsic, DeepXSubmissionError> {
        verify_submission_hash(bytes, expected_hash, expected_hash)?;
        let encoded_hash: String = self
            .submit_encoded_extrinsic(&format!("0x{}", hex::encode(bytes)))
            .await
            .map_err(|source| DeepXSubmissionError::Rpc {
                method: SUBMIT_EXTRINSIC_METHOD,
                source,
            })?;
        let node_hash = decode_submission_hash(&encoded_hash)?;
        DeepXSubmittedExtrinsic::verified(bytes, expected_hash, node_hash)
    }
}

#[cfg(test)]
mod tests {
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    fn encoded_extrinsic(payload: &[u8]) -> String {
        let mut bytes = Compact(u32::try_from(payload.len()).unwrap()).encode();
        bytes.extend_from_slice(payload);
        format!("0x{}", hex::encode(bytes))
    }

    #[rstest]
    #[case(None)]
    #[case(Some("pending"))]
    #[case(Some("best"))]
    #[case(Some("decode_failed"))]
    fn rest_acknowledgement_preserves_backend_data(#[case] confirmation: Option<&str>) {
        let bytes = b"exact durable extrinsic";
        let hash = BlakeTwo256.hash(bytes).0;
        let data = json!({
            "tx_hash": format!("0x{}", hex::encode(hash)),
            "order_id": "18446744073709551615",
            "confirmation": confirmation,
        });
        let body = serde_json::to_vec(&json!({
            "code": 200, "fail": false, "msg": "success", "data": data,
        }))
        .unwrap();
        let acknowledgement = verify_rest_acknowledgement(&body, bytes, hash).unwrap();
        assert_eq!(acknowledgement.submitted.node_hash(), hash);
        assert_eq!(acknowledgement.data, data);
    }

    #[rstest]
    fn exact_extrinsic_scan_requires_all_entries_and_unique_target() {
        let target = encoded_extrinsic(&[1, 2]);
        let other = encoded_extrinsic(&[3]);
        let target_bytes = hex::decode(target.strip_prefix("0x").unwrap()).unwrap();
        let target_hash = BlakeTwo256.hash(&target_bytes).0;
        assert_eq!(
            find_extrinsic_by_hash(&[other.clone(), target.clone()], target_hash),
            Ok(Some(1))
        );
        assert_eq!(find_extrinsic_by_hash(&[other], target_hash), Ok(None));
        assert_eq!(
            find_extrinsic_by_hash(&[target.clone(), target], target_hash),
            Err(DeepXExtrinsicLookupError::DuplicateTarget),
        );
    }

    #[rstest]
    fn malformed_extrinsic_prevents_absence_evidence() {
        let target_hash = [9; 32];
        for encoded in ["not-hex", "0xzz", "0x100102", "0x"] {
            assert_eq!(
                find_extrinsic_by_hash(&[encoded.to_string()], target_hash),
                Err(DeepXExtrinsicLookupError::InvalidEntry { index: 0 }),
            );
        }
        let valid = encoded_extrinsic(&[1]);
        assert_eq!(
            find_extrinsic_by_hash(&[valid, "0xzz".to_string()], target_hash),
            Err(DeepXExtrinsicLookupError::InvalidEntry { index: 1 }),
        );
    }

    #[rstest]
    fn exact_hash_is_accepted() {
        let bytes = b"deepx-verify-exact";
        let hash = BlakeTwo256.hash(bytes).0;
        let submitted = DeepXSubmittedExtrinsic::verified(bytes, hash, hash).unwrap();
        assert_eq!(submitted.node_hash(), hash);
        assert_eq!(submitted.extrinsic_hash(), hash);
    }

    #[rstest]
    #[case(vec![0xde, 0xad])]
    #[case(b"bytes".to_vec())]
    fn any_hash_mismatch_is_rejected(#[case] bytes: Vec<u8>) {
        let true_hash = BlakeTwo256.hash(&bytes).0;
        let wrong_hash = BlakeTwo256.hash(b"wrong-hash").0;
        assert!(verify_submission_hash(&bytes, wrong_hash, true_hash).is_err());
        assert!(verify_submission_hash(&bytes, true_hash, wrong_hash).is_err());
        assert!(verify_submission_hash(&bytes, wrong_hash, wrong_hash).is_err());
    }

    #[rstest]
    #[case("0x00")]
    #[case("0xyz")]
    #[case("deadbeef")]
    fn malformed_node_hash_is_rejected(#[case] value: &str) {
        assert!(matches!(
            decode_submission_hash(value),
            Err(DeepXSubmissionError::InvalidResponse { .. })
        ));
    }
}
