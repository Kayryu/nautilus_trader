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

//! Adapter configuration for the DeepX order-book JSON-RPC subscription.

pub use deepx_rust_sdk::websocket::orderbook_rpc::{
    DeepXOrderbookRpcLevel, DeepXOrderbookRpcSide, DeepXOrderbookRpcSnapshot, parse_notification,
    parse_subscribe_ack,
};

use crate::config::{DeepXNetworkConfig, DeepXRpcRole};

/// Resolves the configured Watch RPC endpoint to its WebSocket transport scheme.
pub fn websocket_url(network: &DeepXNetworkConfig) -> anyhow::Result<String> {
    deepx_rust_sdk::websocket::orderbook_rpc::websocket_url(
        &network.rpc_url_for(DeepXRpcRole::Watch)?,
    )
}
