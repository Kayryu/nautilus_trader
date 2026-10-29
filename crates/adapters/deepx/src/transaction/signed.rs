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

//! Nautilus transaction envelope for a DeepX signed pallet extrinsic.

use deepx_rust_sdk::signing::{
    self as sdk_signing, ApprovedRuntimeIdentity, DeepXPermittedPalletExtrinsic, RuntimeSnapshot,
    SigningError,
};
use subxt_core::{
    config::{Hasher, substrate::BlakeTwo256},
    dynamic::Value,
};

use crate::common::DeepXPrivateKey;

/// A signed SCALE extrinsic and its deterministic identities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedPalletExtrinsic {
    /// Complete compact-length-prefixed SCALE extrinsic bytes.
    pub(crate) bytes: Vec<u8>,
    /// Blake2-256 hash of the complete extrinsic bytes.
    pub(crate) extrinsic_hash: [u8; 32],
    /// Ethereum AccountId20 derived from the signing key.
    pub(crate) signer: [u8; 20],
    /// Explicit nonce encoded in the signed extensions.
    pub(crate) nonce: u64,
    /// Approved runtime identity used to encode and sign the extrinsic.
    pub(crate) runtime: ApprovedRuntimeIdentity,
}

impl SignedPalletExtrinsic {
    /// Returns the complete SCALE extrinsic bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the Blake2-256 hash of the complete extrinsic bytes.
    #[must_use]
    pub const fn extrinsic_hash(&self) -> [u8; 32] {
        self.extrinsic_hash
    }

    /// Returns the signing AccountId20.
    #[must_use]
    pub const fn signer(&self) -> [u8; 20] {
        self.signer
    }

    /// Returns the nonce encoded in the signed extensions.
    #[must_use]
    pub const fn nonce(&self) -> u64 {
        self.nonce
    }

    /// Returns the approved runtime identity used for signing.
    #[must_use]
    pub const fn runtime(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime
    }

    pub(crate) fn has_valid_hash(&self) -> bool {
        BlakeTwo256.hash(&self.bytes).0 == self.extrinsic_hash
    }
}

impl From<DeepXPermittedPalletExtrinsic> for SignedPalletExtrinsic {
    fn from(signed: DeepXPermittedPalletExtrinsic) -> Self {
        Self {
            extrinsic_hash: signed.extrinsic_hash(),
            signer: signed.signer(),
            nonce: signed.nonce(),
            runtime: signed.runtime().clone(),
            bytes: signed.into_bytes(),
        }
    }
}

pub(crate) fn sign_dynamic_pallet_call_with_snapshot(
    snapshot: &RuntimeSnapshot,
    key: &DeepXPrivateKey,
    pallet: &str,
    call: &str,
    arguments: Vec<Value>,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    let signed = sdk_signing::sign_validated_dynamic_pallet_call(
        snapshot.validated(),
        key,
        pallet,
        call,
        arguments,
        nonce,
    )?;

    Ok(SignedPalletExtrinsic {
        extrinsic_hash: signed.extrinsic_hash(),
        signer: signed.signer(),
        nonce,
        runtime: snapshot.identity().clone(),
        bytes: signed.into_bytes(),
    })
}
