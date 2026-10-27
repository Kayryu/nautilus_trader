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

//! Read-only Python wallet account observations.

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    http::{
        DeepXHttpClient, DeepXPerpLiquidationPrice, DeepXPerpLiquidationPriceRequest,
        DeepXSubaccountBalanceAsset, DeepXSubaccountBalances, DeepXSubaccountEquity,
        DeepXSubaccountMarginRatio, DeepXSubaccountProfile, DeepXSubaccountSnapshot,
        DeepXUserStats, DeepXWalletAccountSnapshot, DeepXWalletSubaccounts,
    },
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXWalletSubaccounts {
    #[getter("wallet")]
    fn py_wallet(&self) -> &str {
        self.wallet()
    }

    #[getter]
    fn addresses(&self) -> Vec<String> {
        self.addresses.clone()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXUserStats {
    #[getter]
    fn subaccounts(&self) -> Vec<String> {
        self.subaccounts.clone()
    }

    #[getter]
    fn if_staked_quote_asset_amount(&self) -> String {
        self.if_staked_quote_asset_amount.to_string()
    }

    #[getter]
    fn number_of_sub_accounts(&self) -> u64 {
        self.number_of_sub_accounts
    }

    #[getter]
    fn number_of_sub_accounts_created(&self) -> u64 {
        self.number_of_sub_accounts_created
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXPerpLiquidationPrice {
    #[getter]
    fn address(&self) -> &str {
        &self.address
    }

    #[getter]
    fn market_id(&self) -> u64 {
        self.market_id
    }

    #[getter]
    fn market_name(&self) -> &str {
        &self.market_name
    }

    #[getter]
    fn liquidate_price(&self) -> Option<String> {
        self.liquidate_price.map(|value| value.to_string())
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSubaccountProfile {
    #[getter]
    fn authority(&self) -> &str {
        &self.authority
    }

    #[getter]
    fn address(&self) -> &str {
        &self.address
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn status(&self) -> &str {
        &self.status
    }

    #[getter]
    fn spot_positions_json(&self) -> Vec<String> {
        self.spot_positions
            .iter()
            .map(|value| value.get().to_string())
            .collect()
    }

    #[getter]
    fn next_order_id(&self) -> u32 {
        self.next_order_id
    }

    #[getter]
    fn spot_margin_trading_enabled(&self) -> bool {
        self.spot_margin_trading_enabled
    }

    #[getter]
    fn margin_strategy(&self) -> &str {
        &self.margin_strategy
    }

    #[getter]
    fn height(&self) -> u64 {
        self.height
    }

    #[getter]
    fn created_at(&self) -> u64 {
        self.created_at
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSubaccountBalanceAsset {
    #[getter]
    fn symbol(&self) -> &str {
        &self.symbol
    }

    #[getter]
    fn decimals(&self) -> u8 {
        self.decimals
    }

    #[getter]
    fn price(&self) -> String {
        self.price.to_string()
    }

    #[getter]
    fn balance(&self) -> String {
        self.balance.to_string()
    }

    #[getter]
    fn balance_usd(&self) -> String {
        self.balance_usd.to_string()
    }

    #[getter]
    fn balance_borrowed(&self) -> String {
        self.balance_borrowed.to_string()
    }

    #[getter]
    fn balance_borrowed_usd(&self) -> String {
        self.balance_borrowed_usd.to_string()
    }

    #[getter]
    fn borrow_interest(&self) -> String {
        self.borrow_interest.to_string()
    }

    #[getter]
    fn borrow_interest_usd(&self) -> String {
        self.borrow_interest_usd.to_string()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSubaccountBalances {
    #[getter]
    fn address(&self) -> &str {
        &self.address
    }

    #[getter]
    fn assets(&self) -> Vec<DeepXSubaccountBalanceAsset> {
        self.assets
            .iter()
            .cloned()
            .map(DeepXSubaccountBalanceAsset)
            .collect()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSubaccountEquity {
    #[getter]
    fn subaccount(&self) -> &str {
        &self.subaccount
    }

    #[getter]
    fn total_deposits_usd(&self) -> String {
        self.total_deposits_usd.to_string()
    }

    #[getter]
    fn total_borrows_usd(&self) -> String {
        self.total_borrows_usd.to_string()
    }

    #[getter]
    fn unrealized_pnl_usd(&self) -> String {
        self.unrealized_pnl_usd.to_string()
    }

    #[getter]
    fn equity_usd(&self) -> String {
        self.equity_usd.to_string()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSubaccountMarginRatio {
    #[getter]
    fn collateral(&self) -> String {
        self.collateral.to_string()
    }

    #[getter]
    fn margin_required(&self) -> String {
        self.margin_required.to_string()
    }

    #[getter]
    fn margin_ratio(&self) -> Option<String> {
        self.margin_ratio.map(|value| value.to_string())
    }

    #[getter]
    fn exact_total_collateral(&self) -> Option<String> {
        self.exact_margin
            .as_ref()
            .map(|exact| exact.total_collateral.to_string())
    }

    #[getter]
    fn exact_maintenance_margin_required(&self) -> Option<String> {
        self.exact_margin
            .as_ref()
            .map(|exact| exact.maint_margin_req.to_string())
    }

    #[getter]
    fn exact_uni_mmr(&self) -> Option<String> {
        self.exact_margin
            .as_ref()
            .and_then(|exact| exact.uni_mmr.map(|value| value.to_string()))
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXSubaccountSnapshot {
    #[getter]
    fn profile(&self) -> DeepXSubaccountProfile {
        self.profile.clone()
    }

    #[getter]
    fn balances(&self) -> DeepXSubaccountBalances {
        self.balances.clone()
    }

    #[getter]
    fn equity(&self) -> DeepXSubaccountEquity {
        self.equity.clone()
    }

    #[getter]
    fn margin_ratio(&self) -> DeepXSubaccountMarginRatio {
        self.margin_ratio.clone()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl DeepXWalletAccountSnapshot {
    #[getter]
    fn directory(&self) -> DeepXWalletSubaccounts {
        self.directory.clone()
    }

    #[getter]
    fn stats(&self) -> DeepXUserStats {
        self.stats.clone()
    }

    #[getter]
    fn subaccounts(&self) -> Vec<DeepXSubaccountSnapshot> {
        self.subaccounts.clone()
    }
}

/// Returns the validated public subaccount directory for one wallet.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_wallet_subaccounts", signature = (wallet, config=None))]
pub(super) fn py_get_deepx_wallet_subaccounts(
    py: Python<'_>,
    wallet: String,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXWalletSubaccounts> {
    let config = config.unwrap_or_default();
    config.validate().map_err(to_pyvalue_err)?;
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )
    .map_err(to_pyvalue_err)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)?;
    py.detach(move || runtime.block_on(client.get_wallet_subaccounts(&wallet)))
        .map_err(to_pyvalue_err)
}

/// Returns the validated directory statistics for one wallet.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_user_stats", signature = (wallet, config=None))]
pub(super) fn py_get_deepx_user_stats(
    py: Python<'_>,
    wallet: String,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXUserStats> {
    let config = config.unwrap_or_default();
    config.validate().map_err(to_pyvalue_err)?;
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )
    .map_err(to_pyvalue_err)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)?;
    py.detach(move || runtime.block_on(client.get_user_stats(&wallet)))
        .map_err(to_pyvalue_err)
}

/// Returns a validated nullable perpetual liquidation price for one subaccount and market.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_perp_liquidation_price", signature = (subaccount, market_id=None, market_name=None, config=None))]
pub(super) fn py_get_deepx_perp_liquidation_price(
    py: Python<'_>,
    subaccount: String,
    market_id: Option<u64>,
    market_name: Option<String>,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXPerpLiquidationPrice> {
    let config = config.unwrap_or_default();
    config.validate().map_err(to_pyvalue_err)?;
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )
    .map_err(to_pyvalue_err)?;
    let request = DeepXPerpLiquidationPriceRequest {
        subaccount,
        market_name,
        market_id,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)?;
    py.detach(move || runtime.block_on(client.get_perp_liquidation_price(&request)))
        .map_err(to_pyvalue_err)
}

/// Returns all validated account REST observations for one wallet.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "get_deepx_wallet_account_snapshot", signature = (wallet, config=None))]
pub(super) fn py_get_deepx_wallet_account_snapshot(
    py: Python<'_>,
    wallet: String,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<DeepXWalletAccountSnapshot> {
    let config = config.unwrap_or_default();
    config.validate().map_err(to_pyvalue_err)?;
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )
    .map_err(to_pyvalue_err)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)?;
    py.detach(move || runtime.block_on(client.get_wallet_account_snapshot(&wallet)))
        .map_err(to_pyvalue_err)
}
