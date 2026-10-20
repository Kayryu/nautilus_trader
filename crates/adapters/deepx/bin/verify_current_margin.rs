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

//! Read-only live verification of hash-pinned current DeepX margin.

use anyhow::Context;
use nautilus_core::hex;
use nautilus_deepx::{
    config::DeepXNetworkConfig, observe_and_validate_rpc_endpoint_identities,
    observe_and_validate_rpc_method_capabilities, observe_current_margin_evidence,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let address = args
        .next()
        .context("usage: deepx-verify-current-margin <subaccount-account-id20>")?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");
    let subaccount = address
        .strip_prefix("0x")
        .and_then(|value| hex::decode_array::<20>(value).ok())
        .context("invalid subaccount AccountId20")?;

    let endpoints =
        observe_and_validate_rpc_endpoint_identities(&DeepXNetworkConfig::default()).await?;
    let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints).await?;
    let evidence = observe_current_margin_evidence(&endpoints, &capabilities, subaccount).await?;
    println!(
        "subaccount=0x{} block={} hash=0x{} initial_collateral={} initial_required={} maintenance_collateral={} maintenance_required={}",
        hex::encode(evidence.subaccount()),
        evidence.block_number(),
        hex::encode(evidence.block_hash()),
        evidence.initial().total_collateral(),
        evidence.initial().total_margin_required(),
        evidence.maintenance().total_collateral(),
        evidence.maintenance().total_margin_required(),
    );
    Ok(())
}
