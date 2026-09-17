// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Read-only live upgrade verification. Sends no application or authentication requests.

use std::time::Duration;

use nautilus_deepx::{config::DeepXNetworkConfig, websocket::transport::DeepXWsReadConnection};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut connection = DeepXWsReadConnection::connect(
        &DeepXNetworkConfig::default(),
        None,
        Duration::from_secs(10),
    )
    .await?;
    println!("DeepX public WebSocket upgrade succeeded; no application requests sent");
    connection.close().await?;
    println!("DeepX public WebSocket transport closed");
    Ok(())
}
