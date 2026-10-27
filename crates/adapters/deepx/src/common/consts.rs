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

pub use deepx_rust_sdk::deployment::{
    DEEPX_TESTNET_CHAIN_ID, DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_REST_URL,
    DEEPX_TESTNET_RPC_URL, DEEPX_TESTNET_WS_URL,
};
use nautilus_model::identifiers::{ClientId, Venue};

pub const DEEPX: &str = deepx_rust_sdk::deployment::DEEPX;

pub static DEEPX_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(DEEPX));
pub static DEEPX_CLIENT_ID: LazyLock<ClientId> = LazyLock::new(|| ClientId::new(DEEPX));
