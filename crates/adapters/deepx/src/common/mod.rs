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

//! Shared DeepX protocol types and deployment constants.

pub mod consts;
pub mod credential;
pub mod enums;
pub mod error;
pub mod metadata;
pub mod parse;
pub mod symbol;
pub mod urls;

pub use credential::{DeepXKeyScheme, DeepXPrivateKey, credential_env_var};
pub use enums::{DeepXEnvironment, DeepXProductType};
pub use error::{DeepXError, Result};
pub use metadata::{MetadataError, signed_extension_identifiers};
pub use parse::{decimal_to_scaled_i128, scaled_i128_to_decimal};
pub use symbol::{format_instrument_id, parse_instrument_id};
