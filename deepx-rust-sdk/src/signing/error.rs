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
    /// The cancel and place legs of a Spot modify target different runtime scopes.
    #[error("DeepX Spot modify legs must target the same subaccount, pair, and side")]
    SpotModifyScopeMismatch,
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
            DeepXPalletSigningError::SpotModifyScopeMismatch => Self::SpotModifyScopeMismatch,
            DeepXPalletSigningError::SubaccountNameTooLong { length } => {
                Self::SubaccountNameTooLong { length }
            }
            DeepXPalletSigningError::Encode(e) => Self::Encode(e),
        }
    }
}
