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

//! Adapter-level DeepX errors.

use thiserror::Error;

/// Result alias for DeepX adapter operations.
pub type Result<T> = std::result::Result<T, DeepXError>;

/// Errors surfaced at the DeepX adapter boundary.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXError {
    /// The configured environment is not supported by this adapter release.
    #[error("unsupported DeepX environment: {0}")]
    UnsupportedEnvironment(String),

    /// The configuration violates an adapter invariant.
    #[error("invalid DeepX configuration: {0}")]
    InvalidConfiguration(String),

    /// A required credential environment variable is unavailable.
    #[error("DeepX credential environment variable '{0}' must be set")]
    MissingCredential(&'static str),

    /// A credential cannot be used by the configured signing scheme.
    #[error("invalid DeepX credential: {0}")]
    InvalidCredential(String),

    /// A key scheme is not supported by this adapter release.
    #[error("unsupported DeepX key scheme: {0}")]
    UnsupportedKeyScheme(String),

    /// A symbol does not satisfy the canonical DeepX adapter format.
    #[error("invalid DeepX symbol: {0}")]
    InvalidSymbol(String),

    /// A product cannot be mapped to a supported Nautilus instrument identity.
    #[error("unsupported DeepX product type: {0}")]
    UnsupportedProduct(String),

    /// A discrete financial value cannot be represented exactly.
    #[error("inexact DeepX decimal conversion: {0}")]
    InexactDecimal(String),

    /// A discrete financial value exceeds the supported representation.
    #[error("DeepX decimal conversion overflow: {0}")]
    DecimalOverflow(String),
}
