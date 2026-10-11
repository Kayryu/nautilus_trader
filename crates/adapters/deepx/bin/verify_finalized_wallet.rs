// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live read-only verification of finalized account evidence for wallet subaccounts.

use anyhow::Context;
use nautilus_core::hex;
use nautilus_deepx::{
    config::DeepXDataClientConfig, http::DeepXHttpClient, map_finalized_flat_account_balances,
    observe_and_validate_rpc_endpoint_identities, observe_and_validate_rpc_method_capabilities,
    observe_finalized_accounts_evidence,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-finalized-wallet <wallet-account-id20>")?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let http = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let directory = http.get_wallet_subaccounts(&wallet).await?;
    anyhow::ensure!(
        !directory.addresses.is_empty(),
        "wallet subaccount directory is empty"
    );

    let endpoints = observe_and_validate_rpc_endpoint_identities(&config.network).await?;
    let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints).await?;
    println!(
        "DeepX wallet finalized evidence wallet={} directory_subaccounts={}",
        directory.wallet(),
        directory.addresses.len(),
    );

    let subaccounts = directory
        .addresses
        .iter()
        .map(|encoded| {
            let value = encoded
                .strip_prefix("0x")
                .context("directory subaccount must be 0x-prefixed")?;
            hex::decode_array::<20>(value)
                .context("directory subaccount must contain exactly 20 bytes of hex")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let evidence = observe_finalized_accounts_evidence(
        &config.network.environment,
        &endpoints,
        &capabilities,
        &subaccounts,
    )
    .await?;
    let shared_block = evidence
        .first()
        .context("wallet account batch returned no evidence")?
        .checkpoint()
        .block_number();
    for evidence in evidence {
        let checkpoint = evidence.checkpoint();
        println!(
            "subaccount=0x{} block={} hash=0x{} timestamp_ms={} initial_margin_required={} maintenance_margin_required={} lending_positions={} perp_positions={} active_perp_orders={} active_spot_orders={}",
            hex::encode(evidence.subaccount()),
            checkpoint.block_number(),
            hex::encode(checkpoint.block_hash()),
            evidence.timestamp_ms(),
            evidence.margin().initial().total_margin_required(),
            evidence.margin().maintenance().total_margin_required(),
            evidence.lending().positions().len(),
            evidence.perp_positions().positions().len(),
            evidence.perp_orders().orders().len(),
            evidence.spot_orders().active_order_ids().len(),
        );
        for position in evidence.lending().positions() {
            println!(
                "lending_position subaccount=0x{} market={} asset=0x{} scaled_deposit={} scaled_borrow={} effective_deposit={:?} effective_borrow={:?} max_withdraw_amount={:?} decimals={}",
                hex::encode(evidence.subaccount()),
                position.market_id(),
                hex::encode(position.asset()),
                position.scaled_deposit(),
                position.scaled_borrow(),
                position.effective_deposit(),
                position.effective_borrow(),
                position.max_withdraw_amount(),
                position.pool().decimal(),
            );
        }
        for position in evidence.perp_positions().positions() {
            println!(
                "perp_position subaccount=0x{} market={} owner=0x{} is_long={} base_asset_amount={} entry_price={} leverage={} realized_pnl={} funding_payment={} last_settle_price={}",
                hex::encode(evidence.subaccount()),
                position.market_id(),
                hex::encode(position.owner()),
                position.is_long(),
                position.base_asset_amount(),
                position.entry_price(),
                position.leverage(),
                position.realized_pnl(),
                position.funding_payment(),
                position.last_settle_price(),
            );
        }
        for order in evidence.perp_orders().orders() {
            println!(
                "active_perp_order subaccount=0x{} market={} order_id={} owner=0x{} is_long={} size={} price={} order_type={:?} status={:?} size_filled={} size_remain={} create_time_ms={} reduce_only={} post_only={:?}",
                hex::encode(evidence.subaccount()),
                order.market_id(),
                order.order_id(),
                hex::encode(order.owner()),
                order.is_long(),
                order.size(),
                order.price(),
                order.order_type(),
                order.status(),
                order.size_filled(),
                order.size_remain(),
                order.create_time(),
                order.reduce_only(),
                order.post_only(),
            );
        }
        for order in evidence.spot_orders().orders() {
            println!(
                "active_spot_order subaccount=0x{} order_id={} pair=0x{} is_buy={} status={:?} order_type={:?} quote_amount_le=0x{} base_amount_le=0x{} price_le={:?} create_time={} reduce_only={} post_only={:?}",
                hex::encode(evidence.subaccount()),
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
            Ok(balances) => {
                for balance in balances.balances() {
                    println!(
                        "framework_flat_balance subaccount=0x{} currency={} total={} locked={} free={}",
                        hex::encode(evidence.subaccount()),
                        balance.currency,
                        balance.total,
                        balance.locked,
                        balance.free,
                    );
                }
            }
            Err(error) => println!(
                "framework_flat_balance subaccount=0x{} unavailable: {error}",
                hex::encode(evidence.subaccount()),
            ),
        }
    }

    println!(
        "DeepX wallet finalized evidence completed observed={} block={shared_block}; directory associations are mutable REST observations and do not prove chain-level ownership. No credentials or transactions were sent",
        subaccounts.len(),
    );
    Ok(())
}
