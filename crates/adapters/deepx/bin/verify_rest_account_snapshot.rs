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

//! Live read-only verification of a DeepX wallet account snapshot.

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXHttpClient, DeepXPerpLiquidationPriceRequest},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-rest-account-snapshot <wallet-account-id20> [market-id]")?;
    let market_id = args
        .next()
        .map(|value| {
            value
                .parse::<u64>()
                .context("market-id must be an unsigned integer")
        })
        .transpose()?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let snapshot = client.get_wallet_account_snapshot(&wallet).await?;
    let stats = client.get_user_stats(&wallet).await?;
    let quota = client.get_quota_summary(&wallet).await?;
    let delegates = client.get_delegate_accounts(&wallet).await?;
    anyhow::ensure!(
        quota.subaccount_count == snapshot.subaccounts.len() as u64,
        "quota summary subaccount count does not match the wallet directory"
    );
    println!(
        "DeepX wallet account directory verified wallet={} subaccounts={} created={} if_staked_quote_asset_amount={}",
        snapshot.directory.wallet(),
        snapshot.subaccounts.len(),
        stats.number_of_sub_accounts_created,
        stats.if_staked_quote_asset_amount,
    );
    println!(
        "DeepX wallet quota verified owner={} subaccounts={} spot_volume_usd={} perp_volume_usd={} total_volume_usd={} earned={} granted={} reserved={} pending={}",
        quota.owner,
        quota.subaccount_count,
        quota.spot_volume_usd,
        quota.perp_volume_usd,
        quota.total_volume_usd,
        quota.quota_earned,
        quota.quota_granted,
        quota.quota_reserved,
        quota.quota_pending,
    );
    for delegate in delegates.accounts {
        let delegators = client
            .get_delegator_accounts(&delegate.delegate_address)
            .await?;
        anyhow::ensure!(
            delegators
                .wallets
                .iter()
                .any(|value| value.eq_ignore_ascii_case(&wallet)),
            "reverse delegator directory does not contain the queried wallet"
        );
        println!(
            "DeepX wallet delegate verified wallet={} delegate={} name={} mode={} active={} created_at_ms={} valid_until_ms={}",
            wallet,
            delegate.delegate_address,
            delegate.delegate_name,
            delegate.mode.as_str(),
            delegate.active,
            delegate.create_time,
            delegate.valid_until,
        );
    }

    for subaccount in snapshot.subaccounts {
        println!(
            "DeepX subaccount verified address={} name={} status={} height={} assets={} deposits_usd={} borrows_usd={} unrealized_pnl_usd={} equity_usd={} collateral={} margin_required={} margin_ratio={:?}",
            subaccount.profile.address,
            subaccount.profile.name,
            subaccount.profile.status,
            subaccount.profile.height,
            subaccount.balances.assets.len(),
            subaccount.equity.total_deposits_usd,
            subaccount.equity.total_borrows_usd,
            subaccount.equity.unrealized_pnl_usd,
            subaccount.equity.equity_usd,
            subaccount.margin_ratio.collateral,
            subaccount.margin_ratio.margin_required,
            subaccount.margin_ratio.margin_ratio,
        );
        if let Some(market_id) = market_id {
            let price = client
                .get_perp_liquidation_price(&DeepXPerpLiquidationPriceRequest {
                    subaccount: subaccount.profile.address.clone(),
                    market_name: None,
                    market_id: Some(market_id),
                })
                .await?;
            println!(
                "DeepX liquidation price verified address={} market_id={} market_name={} price={:?}",
                price.address, price.market_id, price.market_name, price.liquidate_price,
            );
        }
    }

    println!("DeepX account snapshot completed; no credentials or transactions sent");
    Ok(())
}
