// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live read-only verification of one address-scoped DeepX user-balances subscription.

use std::{num::NonZeroUsize, time::Duration};

use anyhow::Context;
use nautilus_deepx::{config::DeepXNetworkConfig, websocket::DeepXWsAccountConnection};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subaccount = std::env::args()
        .nth(1)
        .context("usage: deepx-verify-ws-account-balances <subaccount>")?;
    let mut connection = DeepXWsAccountConnection::connect(
        &DeepXNetworkConfig::default(),
        None,
        Duration::from_secs(12),
        NonZeroUsize::new(8).unwrap(),
    )
    .await?;
    let subscription = connection.subscribe_user_balances(&subaccount).await?;
    println!("DeepX user-balances acknowledgement verified for {subaccount}");
    let frame = tokio::time::timeout(
        Duration::from_secs(35),
        connection.next_balances(subscription),
    )
    .await
    .context("DeepX user-balances snapshot deadline exceeded")??;
    println!(
        "DeepX user-balances snapshot verified address={} assets={} timestamp_ms={}",
        frame.balances().address,
        frame.balances().assets.len(),
        frame.timestamp(),
    );
    connection.close().await?;
    println!("DeepX account connection closed; no credentials or transactions sent");
    Ok(())
}
