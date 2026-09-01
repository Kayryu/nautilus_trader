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

//! Verified DeepX testnet deployment constants.

use std::sync::LazyLock;

use nautilus_model::identifiers::Venue;

pub const DEEPX: &str = "DEEPX";
pub static DEEPX_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(DEEPX));

pub const DEEPX_TESTNET_REST_URL: &str = "https://rest-api-testnet.deepx.fi";
pub const DEEPX_TESTNET_WS_URL: &str = "wss://ws-api-testnet.deepx.fi";
pub const DEEPX_TESTNET_RPC_URL: &str = "https://rpc-testnet.deepx.fi";
pub const DEEPX_TESTNET_CHAIN_ID: u32 = 4_846;
pub const DEEPX_TESTNET_GENESIS_HASH: &str =
    "0x86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b";
