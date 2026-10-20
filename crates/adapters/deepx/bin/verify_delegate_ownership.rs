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

//! Read-only live verification of signer authority over one DeepX subaccount.

use anyhow::Context;
use nautilus_core::hex;
use nautilus_deepx::{
    common::{DeepXEnvironment, DeepXKeyScheme, DeepXPrivateKey},
    config::DeepXDataClientConfig,
    http::DeepXHttpClient,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let subaccount = args
        .next()
        .context("usage: deepx-verify-delegate-ownership <subaccount-account-id20>")?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let key = DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let proof = client
        .get_account_ownership_proof(&key, &subaccount)
        .await?;
    println!(
        "signer=0x{} wallet=0x{} subaccount=0x{} profile_height={}",
        hex::encode(proof.signer()),
        hex::encode(proof.wallet()),
        hex::encode(proof.subaccount()),
        proof.profile_height(),
    );
    Ok(())
}
