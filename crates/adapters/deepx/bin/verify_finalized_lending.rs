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

//! Live read-only verification of finalized DeepX V2 lending evidence.

use anyhow::Context;
use nautilus_core::hex;
use nautilus_deepx::{
    config::DeepXDataClientConfig, observe_and_validate_rpc_endpoint_identities,
    observe_and_validate_rpc_method_capabilities, observe_finalized_lending_evidence,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let encoded_account = args
        .next()
        .context("usage: deepx-verify-finalized-lending <account-id20>")?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");
    let account = hex::decode_array::<20>(
        encoded_account
            .strip_prefix("0x")
            .context("account must be 0x-prefixed")?,
    )
    .context("account must contain exactly 20 bytes of hex")?;

    let config = DeepXDataClientConfig::default();
    let endpoints = observe_and_validate_rpc_endpoint_identities(&config.network).await?;
    let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints).await?;
    let evidence = observe_finalized_lending_evidence(
        &config.network.environment,
        &endpoints,
        &capabilities,
        account,
    )
    .await?;
    anyhow::ensure!(
        evidence.subaccount() == account,
        "finalized lending evidence belongs to another account"
    );

    let checkpoint = evidence.checkpoint();
    println!(
        "DeepX finalized lending verified account=0x{} block={} hash=0x{} spec_version={} transaction_version={} positions={}",
        hex::encode(evidence.subaccount()),
        checkpoint.block_number(),
        hex::encode(checkpoint.block_hash()),
        evidence.runtime_identity().spec_version,
        evidence.runtime_identity().transaction_version,
        evidence.positions().len(),
    );
    for position in evidence.positions() {
        println!(
            "market={} asset=0x{} scaled_deposit={} scaled_borrow={} effective_deposit={:?} effective_borrow={:?} max_withdraw_amount={:?} decimal={} deposit_index={} borrow_index={} pool_locked_liquidity={} pool_status={:?}",
            position.market_id(),
            hex::encode(position.asset()),
            position.scaled_deposit(),
            position.scaled_borrow(),
            position.effective_deposit(),
            position.effective_borrow(),
            position.max_withdraw_amount(),
            position.pool().decimal(),
            position.pool().deposit_index(),
            position.pool().borrow_index(),
            position.pool().locked_liquidity(),
            position.pool().status(),
        );
    }
    println!(
        "Values are raw finalized evidence; max_withdraw_amount is liquidity-capped, pool_locked_liquidity is not a user balance, and no AccountState, credentials, or transactions were used"
    );
    Ok(())
}
