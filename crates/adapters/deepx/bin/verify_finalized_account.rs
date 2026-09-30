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

//! Live read-only verification of complete finalized DeepX raw account evidence.

use anyhow::Context;
use nautilus_core::hex;
use nautilus_deepx::{
    config::DeepXDataClientConfig, map_finalized_flat_account_balances,
    map_finalized_margin_balance_with_evidence, observe_and_validate_rpc_endpoint_identities,
    observe_and_validate_rpc_method_capabilities, observe_finalized_account_evidence,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let encoded_subaccount = args
        .next()
        .context("usage: deepx-verify-finalized-account <subaccount-account-id20>")?;
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
    let evidence = observe_finalized_account_evidence(
        &config.network.environment,
        &endpoints,
        &capabilities,
        subaccount,
    )
    .await?;
    anyhow::ensure!(
        evidence.subaccount() == subaccount,
        "finalized account evidence belongs to another subaccount"
    );
    let margin = map_finalized_margin_balance_with_evidence(evidence.margin())?;
    anyhow::ensure!(
        margin.subaccount() == evidence.subaccount()
            && margin.checkpoint() == evidence.checkpoint()
            && margin.runtime_identity() == evidence.runtime_identity(),
        "mapped framework margin provenance differs from complete account evidence",
    );

    let checkpoint = evidence.checkpoint();
    println!(
        "DeepX finalized raw account evidence verified subaccount=0x{} block={} hash=0x{} timestamp_ms={} spec_version={} transaction_version={}",
        hex::encode(evidence.subaccount()),
        checkpoint.block_number(),
        hex::encode(checkpoint.block_hash()),
        evidence.timestamp_ms(),
        evidence.runtime_identity().spec_version,
        evidence.runtime_identity().transaction_version,
    );
    println!(
        "margin initial_collateral={} initial_required={} maintenance_collateral={} maintenance_required={}",
        evidence.margin().initial().total_collateral(),
        evidence.margin().initial().total_margin_required(),
        evidence.margin().maintenance().total_collateral(),
        evidence.margin().maintenance().total_margin_required(),
    );
    println!(
        "framework_margin initial={} maintenance={} currency={} block={}",
        margin.margin().initial,
        margin.margin().maintenance,
        margin.margin().currency,
        margin.checkpoint().block_number(),
    );
    println!(
        "lending_positions={} perp_positions={} runtime_max_perp_positions={} active_perp_orders={} active_spot_orders={}",
        evidence.lending().positions().len(),
        evidence.perp_positions().positions().len(),
        evidence.perp_positions().runtime_max_positions(),
        evidence.perp_orders().orders().len(),
        evidence.spot_orders().active_order_ids().len(),
    );
    for position in evidence.lending().positions() {
        println!(
            "lending market={} asset=0x{} scaled_deposit={} scaled_borrow={} effective_deposit={:?} effective_borrow={:?} max_withdraw_amount={:?}",
            position.market_id(),
            hex::encode(position.asset()),
            position.scaled_deposit(),
            position.scaled_borrow(),
            position.effective_deposit(),
            position.effective_borrow(),
            position.max_withdraw_amount(),
        );
    }
    for position in evidence.perp_positions().positions() {
        println!(
            "perp_position market={} owner=0x{} is_long={} base_asset_amount={} entry_price={} leverage={} last_settle_price={}",
            position.market_id(),
            hex::encode(position.owner()),
            position.is_long(),
            position.base_asset_amount(),
            position.entry_price(),
            position.leverage(),
            position.last_settle_price(),
        );
    }
    for order in evidence.perp_orders().orders() {
        println!(
            "perp_order market={} order_id={} owner=0x{} is_long={} size={} price={} status={:?} size_filled={} size_remain={}",
            order.market_id(),
            order.order_id(),
            hex::encode(order.owner()),
            order.is_long(),
            order.size(),
            order.price(),
            order.status(),
            order.size_filled(),
            order.size_remain(),
        );
    }
    for order in evidence.spot_orders().orders() {
        println!(
            "active_spot_order order_id={} pair=0x{} is_buy={} status={:?} order_type={:?} quote_amount_le=0x{} base_amount_le=0x{} price_le={:?} create_time={} reduce_only={} post_only={:?}",
            order.order_id(),
            hex::encode(order.pair()),
            order.is_buy(),
            order.status(),
            order.order_type(),
            hex::encode(order.quote_amount()),
            hex::encode(order.base_amount()),
            order.price().map(hex::encode),
            order.create_time(),
            order.reduce_only(),
            order.post_only(),
        );
    }
    match map_finalized_flat_account_balances(&evidence) {
        Ok(mapped) => {
            for balance in mapped.balances() {
                println!(
                    "framework_flat_balance currency={} total={} locked={} free={}",
                    balance.currency, balance.total, balance.locked, balance.free,
                );
            }
        }
        Err(error) => println!("framework_flat_balance unavailable: {error}"),
    }
    println!(
        "Values are raw same-checkpoint evidence; no framework AccountState, reports, credentials, signing, or transactions were used"
    );
    Ok(())
}
