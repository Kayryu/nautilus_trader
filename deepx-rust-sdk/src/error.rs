//! Shared DeepX errors.

use thiserror::Error;

/// Result alias for DeepX SDK operations.
pub type Result<T> = std::result::Result<T, DeepXError>;

/// Errors surfaced by shared DeepX SDK types and utilities.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXError {
    /// The configured environment is not supported by this SDK release.
    #[error("unsupported DeepX environment: {0}")]
    UnsupportedEnvironment(String),

    /// The configuration violates an SDK invariant.
    #[error("invalid DeepX configuration: {0}")]
    InvalidConfiguration(String),

    /// A required credential environment variable is unavailable.
    #[error("DeepX credential environment variable '{0}' must be set")]
    MissingCredential(&'static str),

    /// A credential cannot be used by the configured signing scheme.
    #[error("invalid DeepX credential: {0}")]
    InvalidCredential(String),

    /// A key scheme is not supported by this SDK release.
    #[error("unsupported DeepX key scheme: {0}")]
    UnsupportedKeyScheme(String),

    /// A symbol does not satisfy the canonical DeepX format.
    #[error("invalid DeepX symbol: {0}")]
    InvalidSymbol(String),

    /// A product has no supported DeepX symbol representation.
    #[error("unsupported DeepX product type: {0}")]
    UnsupportedProduct(String),

    /// A capability is not enabled by the available protocol evidence.
    #[error("unsupported DeepX capability: {0}")]
    UnsupportedCapability(&'static str),

    /// A discrete financial value cannot be represented exactly.
    #[error("inexact DeepX decimal conversion: {0}")]
    InexactDecimal(String),

    /// A discrete financial value exceeds the supported representation.
    #[error("DeepX decimal conversion overflow: {0}")]
    DecimalOverflow(String),
}
