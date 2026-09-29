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

//! Live read-only verification of raw DeepX liquidation keeper state.

use anyhow::Context;
use nautilus_deepx::{config::DeepXDataClientConfig, http::DeepXHttpClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let subaccount = args
        .next()
        .context("usage: deepx-verify-rest-liquidation-keeper-state <subaccount> [chain-id]")?;
    let chain_id = args.next();
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let state = client
        .get_liquidation_keeper_state_raw(&subaccount, chain_id.as_deref())
        .await?;

    println!("DeepX liquidation keeper state observed subaccount={subaccount}");
    println!("data={state}");
    println!(
        "Keeper state is uninterpreted endpoint data; it does not establish liquidation execution or chain finality. No credentials or transactions were used"
    );
    Ok(())
}
