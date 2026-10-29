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

//! Errors for permit-bound offline signing.

use thiserror::Error;

use super::{
    DeepXPalletSigningError, DeepXRuntimeSnapshotServiceError,
    runtime_interface::DeepXRuntimeInterfaceError,
};

/// Errors produced before an extrinsic can be signed offline.
#[derive(Debug, Error)]
pub enum SigningError {
    /// The private scalar was rejected by the pinned DeepX signer implementation.
    #[error("invalid DeepX secp256k1 signing key")]
    InvalidKey,
    /// The runtime snapshot permit was revoked before signing.
    #[error(transparent)]
    RuntimePermit(#[from] DeepXRuntimeSnapshotServiceError),
    /// The pallet call is absent from the approved runtime interface.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The dynamic call or transaction extensions could not be SCALE encoded.
    #[error("unable to encode DeepX pallet extrinsic: {0}")]
    Encode(#[source] Box<subxt_core::Error>),
    /// The cancel and place legs of a perpetual modify target different runtime scopes.
    #[error("DeepX perpetual modify legs must target the same subaccount and market")]
    ModifyScopeMismatch,
    /// The runtime accepts at most 32 subaccount name bytes.
    #[error("DeepX subaccount name exceeds the runtime limit of 32 bytes: {length}")]
    SubaccountNameTooLong { length: usize },
}

impl From<subxt_core::Error> for SigningError {
    fn from(value: subxt_core::Error) -> Self {
        Self::Encode(Box::new(value))
    }
}

impl From<DeepXPalletSigningError> for SigningError {
    fn from(value: DeepXPalletSigningError) -> Self {
        match value {
            DeepXPalletSigningError::InvalidKey => Self::InvalidKey,
            DeepXPalletSigningError::RuntimeInterface(e) => Self::RuntimeInterface(e),
            DeepXPalletSigningError::ModifyScopeMismatch => Self::ModifyScopeMismatch,
            DeepXPalletSigningError::SubaccountNameTooLong { length } => {
                Self::SubaccountNameTooLong { length }
            }
            DeepXPalletSigningError::Encode(e) => Self::Encode(e),
        }
    }
}
