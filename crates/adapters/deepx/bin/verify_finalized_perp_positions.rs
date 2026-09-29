// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License. You may obtain a copy of the
//  License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live read-only verification of finalized DeepX perpetual-position storage.

use anyhow::Context;
use nautilus_core::hex;
use nautilus_deepx::{
    config::DeepXDataClientConfig, observe_and_validate_rpc_endpoint_identities,
    observe_and_validate_rpc_method_capabilities, observe_finalized_perp_positions_evidence,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let encoded_subaccount = args
        .next()
        .context("usage: deepx-verify-finalized-perp-positions <subaccount-account-id20>")?;
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
    let evidence = observe_finalized_perp_positions_evidence(
        &config.network.environment,
        &endpoints,
        &capabilities,
        subaccount,
    )
    .await?;
    anyhow::ensure!(
        evidence.subaccount() == subaccount,
        "finalized position evidence belongs to another subaccount"
    );

    let checkpoint = evidence.checkpoint();
    println!(
        "DeepX finalized perpetual positions verified subaccount=0x{} block={} hash=0x{} spec_version={} transaction_version={} runtime_max_positions={} positions={}",
        hex::encode(evidence.subaccount()),
        checkpoint.block_number(),
        hex::encode(checkpoint.block_hash()),
        evidence.runtime_identity().spec_version,
        evidence.runtime_identity().transaction_version,
        evidence.runtime_max_positions(),
        evidence.positions().len(),
    );
    for position in evidence.positions() {
        println!(
            "market={} is_long={} base_asset_amount={} entry_price={} leverage={} last_funding_rate={} version={} realized_pnl={} funding_payment={} owner=0x{} take_profit={:?} stop_loss={:?} last_settle_price={}",
            position.market_id(),
            position.is_long(),
            position.base_asset_amount(),
            position.entry_price(),
            position.leverage(),
            position.last_funding_rate(),
            position.version(),
            position.realized_pnl(),
            position.funding_payment(),
            hex::encode(position.owner()),
            position.take_profit(),
            position.stop_loss(),
            position.last_settle_price(),
        );
    }
    println!(
        "Values are raw finalized runtime storage evidence; no REST position lifecycle, framework report, credentials, or transactions were used"
    );
    Ok(())
}
