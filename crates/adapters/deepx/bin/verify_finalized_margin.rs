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

//! Live read-only verification of finalized DeepX margin aggregates.

use anyhow::Context;
use nautilus_core::hex;
use nautilus_deepx::{
    config::DeepXDataClientConfig, map_finalized_margin_balance,
    observe_and_validate_rpc_endpoint_identities, observe_and_validate_rpc_method_capabilities,
    observe_finalized_margin_evidence,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let encoded_subaccount = args
        .next()
        .context("usage: deepx-verify-finalized-margin <subaccount-account-id20>")?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");
    let subaccount = hex::decode_array::<20>(
        encoded_subaccount
            .strip_prefix("0x")
            .context("subaccount must be 0x-prefixed")?,
    )
    .context("subaccount must contain exactly 20 bytes of hex")?;

    let config = DeepXDataClientConfig::default();
    let endpoints = observe_and_validate_rpc_endpoint_identities(&config.network).await?;
    let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints).await?;
    let evidence = observe_finalized_margin_evidence(
        &config.network.environment,
        &endpoints,
        &capabilities,
        subaccount,
    )
    .await?;
    anyhow::ensure!(
        evidence.subaccount() == subaccount,
        "finalized margin evidence belongs to another subaccount"
    );
    let margin = map_finalized_margin_balance(&evidence)?;

    let checkpoint = evidence.checkpoint();
    println!(
        "DeepX finalized margin verified subaccount=0x{} block={} hash=0x{} spec_version={} transaction_version={}",
        hex::encode(evidence.subaccount()),
        checkpoint.block_number(),
        hex::encode(checkpoint.block_hash()),
        evidence.runtime_identity().spec_version,
        evidence.runtime_identity().transaction_version,
    );
    println!(
        "initial total_collateral={} total_margin_required={}",
        evidence.initial().total_collateral(),
        evidence.initial().total_margin_required(),
    );
    println!(
        "maintenance total_collateral={} total_margin_required={}",
        evidence.maintenance().total_collateral(),
        evidence.maintenance().total_margin_required(),
    );
    println!(
        "framework_margin initial={} maintenance={} currency={}",
        margin.initial, margin.maintenance, margin.currency,
    );
    println!(
        "Raw values use DeepX six-decimal USDC quote units; no balances, AccountState, credentials, or transactions were used"
    );
    Ok(())
}
