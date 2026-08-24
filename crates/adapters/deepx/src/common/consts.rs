// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

/// Venue identifier string.
pub const DEEPX: &str = "DEEPX";

/// Static venue instance.
pub static DEEPX_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(DEEPX)));

/// Static client ID instance.
pub static DEEPX_CLIENT_ID: LazyLock<ClientId> = LazyLock::new(|| ClientId::new(Ustr::from(DEEPX)));

pub const DEEPX_TESTNET_REST_URL: &str = "https://rest-api-testnet.deepx.fi";
pub const DEEPX_TESTNET_WS_URL: &str = "wss://ws-api-testnet.deepx.fi/v1/ws";
pub const DEEPX_TESTNET_SUBSTRATE_WS_URL: &str = "wss://rpc-testnet.deepx.fi";
pub const DEEPX_TESTNET_EVM_RPC_URL: &str = "https://rpc-testnet.deepx.fi";
