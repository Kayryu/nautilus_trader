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

//! Python bindings from `pyo3`.

pub mod config;
pub mod factories;

mod account;
mod balance_changes;
mod delegates;
mod funding_fees;
mod hourly_funding;
mod lending;
mod liquidations;
mod market_data;
mod order_lookup;
mod perp_orders;
mod positions;
mod quota;
mod spot_orders;
mod spot_trades;
mod trades;
mod transaction_status;

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::consts::{DEEPX, DEEPX_CLIENT_ID, DEEPX_VENUE},
    config::{
        DeepXDataClientConfig, DeepXExecutionClientConfig, DeepXHttpReadRetryConfig,
        DeepXNetworkConfig,
    },
    factories::{DeepXDataClientFactory, DeepXExecutionClientFactory},
    http::{
        DeepXBalanceChangePosition, DeepXBalanceChangeRecord, DeepXDelegateAccount,
        DeepXDelegateWallets, DeepXHourlyUnsettledFundingRecord, DeepXLendingAsset,
        DeepXLendingInterestRateParams, DeepXLendingInterestRateRecord, DeepXLendingStatusRecord,
        DeepXLiquidationRecord, DeepXPerpAccountTradeRecord, DeepXPerpFundingFeeRecord,
        DeepXPerpLiquidationPrice, DeepXPerpOrderRecord, DeepXPerpPositionRecord,
        DeepXPerpWalletOrderMarket, DeepXPerpWalletOrdersPage, DeepXPerpWalletSubaccountOrders,
        DeepXPerpWalletSubaccountTrades, DeepXPerpWalletTradeMarket, DeepXQuotaHistoryRecord,
        DeepXQuotaSummary, DeepXSpotAccountTradeRecord, DeepXSpotOrderRecord,
        DeepXSpotWalletOrderMarket, DeepXSpotWalletOrdersPage, DeepXSpotWalletSubaccountOrders,
        DeepXSpotWalletSubaccountTrades, DeepXSpotWalletTradeMarket, DeepXSpotWalletTradesPage,
        DeepXSubaccountBalanceAsset, DeepXSubaccountBalances, DeepXSubaccountEquity,
        DeepXSubaccountMarginRatio, DeepXSubaccountPerpPositions, DeepXSubaccountProfile,
        DeepXSubaccountSnapshot, DeepXUserStats, DeepXWalletAccountSnapshot,
        DeepXWalletDelegateAccounts, DeepXWalletPerpPositions, DeepXWalletSubaccounts,
    },
    transaction::DeepXRestTransactionStatus,
};

#[expect(clippy::needless_pass_by_value)]
fn extract_deepx_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    factory
        .extract::<DeepXDataClientFactory>(py)
        .map(|factory| Box::new(factory) as Box<dyn DataClientFactory>)
        .map_err(|error| {
            to_pyvalue_err(format!("Failed to extract DeepXDataClientFactory: {error}"))
        })
}

#[expect(clippy::needless_pass_by_value)]
fn extract_deepx_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    factory
        .extract::<DeepXExecutionClientFactory>(py)
        .map(|factory| Box::new(factory) as Box<dyn ExecutionClientFactory>)
        .map_err(|error| {
            to_pyvalue_err(format!(
                "Failed to extract DeepXExecutionClientFactory: {error}"
            ))
        })
}

#[expect(clippy::needless_pass_by_value)]
fn extract_deepx_data_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    config
        .extract::<DeepXDataClientConfig>(py)
        .map(|config| Box::new(config) as Box<dyn ClientConfig>)
        .map_err(|error| {
            to_pyvalue_err(format!("Failed to extract DeepXDataClientConfig: {error}"))
        })
}

#[expect(clippy::needless_pass_by_value)]
fn extract_deepx_exec_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    config
        .extract::<DeepXExecutionClientConfig>(py)
        .map(|config| Box::new(config) as Box<dyn ClientConfig>)
        .map_err(|error| {
            to_pyvalue_err(format!(
                "Failed to extract DeepXExecutionClientConfig: {error}"
            ))
        })
}

/// Exposed through `nautilus_trader.adapters.deepx`.
///
/// # Errors
///
/// Returns an error if any bindings fail to register with the Python module.
#[pymodule]
pub fn deepx(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(stringify!(DEEPX), DEEPX)?;
    m.add(stringify!(DEEPX_CLIENT_ID), *DEEPX_CLIENT_ID)?;
    m.add(stringify!(DEEPX_VENUE), *DEEPX_VENUE)?;
    m.add_class::<DeepXHttpReadRetryConfig>()?;
    m.add_class::<DeepXNetworkConfig>()?;
    m.add_class::<DeepXDataClientConfig>()?;
    m.add_class::<DeepXDataClientFactory>()?;
    m.add_class::<DeepXExecutionClientConfig>()?;
    m.add_class::<DeepXExecutionClientFactory>()?;
    m.add_class::<DeepXQuotaSummary>()?;
    m.add_class::<DeepXBalanceChangePosition>()?;
    m.add_class::<DeepXBalanceChangeRecord>()?;
    m.add_class::<balance_changes::DeepXBalanceChangesPage>()?;
    m.add_class::<DeepXWalletSubaccounts>()?;
    m.add_class::<DeepXUserStats>()?;
    m.add_class::<DeepXPerpLiquidationPrice>()?;
    m.add_class::<DeepXLiquidationRecord>()?;
    m.add_class::<liquidations::DeepXLiquidationRecordsPage>()?;
    m.add_class::<DeepXPerpPositionRecord>()?;
    m.add_class::<DeepXPerpOrderRecord>()?;
    m.add_class::<perp_orders::DeepXPerpHistoryOrdersPage>()?;
    m.add_class::<perp_orders::DeepXPerpOpenOrdersPage>()?;
    m.add_class::<perp_orders::DeepXPerpWalletOrderItemsPage>()?;
    m.add_class::<DeepXPerpWalletSubaccountOrders>()?;
    m.add_class::<DeepXPerpWalletOrderMarket>()?;
    m.add_class::<DeepXPerpWalletOrdersPage>()?;
    m.add_class::<DeepXPerpAccountTradeRecord>()?;
    m.add_class::<trades::DeepXPerpAccountTradesPage>()?;
    m.add_class::<DeepXPerpFundingFeeRecord>()?;
    m.add_class::<funding_fees::DeepXPerpFundingFeesPage>()?;
    m.add_class::<funding_fees::DeepXWalletFundingFeesPage>()?;
    m.add_class::<DeepXHourlyUnsettledFundingRecord>()?;
    m.add_class::<DeepXRestTransactionStatus>()?;
    m.add_class::<transaction_status::DeepXRestStatusPollResult>()?;
    m.add_class::<transaction_status::DeepXRestStatusPoller>()?;
    m.add_class::<DeepXLendingAsset>()?;
    m.add_class::<DeepXLendingInterestRateParams>()?;
    m.add_class::<DeepXLendingInterestRateRecord>()?;
    m.add_class::<DeepXLendingStatusRecord>()?;
    m.add_class::<lending::DeepXLendingService>()?;
    m.add_class::<DeepXSpotAccountTradeRecord>()?;
    m.add_class::<spot_trades::DeepXSpotAccountTradesPage>()?;
    m.add_class::<spot_trades::DeepXSpotWalletTradeItemsPage>()?;
    m.add_class::<DeepXSpotWalletSubaccountTrades>()?;
    m.add_class::<DeepXSpotWalletTradeMarket>()?;
    m.add_class::<DeepXSpotWalletTradesPage>()?;
    m.add_class::<DeepXSpotOrderRecord>()?;
    m.add_class::<spot_orders::DeepXSpotHistoryOrdersPage>()?;
    m.add_class::<spot_orders::DeepXSpotOpenOrdersPage>()?;
    m.add_class::<spot_orders::DeepXSpotWalletOrderItemsPage>()?;
    m.add_class::<DeepXSpotWalletSubaccountOrders>()?;
    m.add_class::<DeepXSpotWalletOrderMarket>()?;
    m.add_class::<DeepXSpotWalletOrdersPage>()?;
    m.add_class::<trades::DeepXPerpWalletTradesPage>()?;
    m.add_class::<DeepXPerpWalletSubaccountTrades>()?;
    m.add_class::<DeepXPerpWalletTradeMarket>()?;
    m.add_class::<positions::DeepXPerpPositionsPage>()?;
    m.add_class::<DeepXSubaccountPerpPositions>()?;
    m.add_class::<DeepXWalletPerpPositions>()?;
    m.add_class::<DeepXSubaccountProfile>()?;
    m.add_class::<DeepXSubaccountBalanceAsset>()?;
    m.add_class::<DeepXSubaccountBalances>()?;
    m.add_class::<DeepXSubaccountEquity>()?;
    m.add_class::<DeepXSubaccountMarginRatio>()?;
    m.add_class::<DeepXSubaccountSnapshot>()?;
    m.add_class::<DeepXWalletAccountSnapshot>()?;
    m.add_class::<DeepXQuotaHistoryRecord>()?;
    m.add_class::<quota::DeepXQuotaHistoryPage>()?;
    m.add_class::<DeepXDelegateAccount>()?;
    m.add_class::<DeepXWalletDelegateAccounts>()?;
    m.add_class::<DeepXDelegateWallets>()?;
    m.add_function(wrap_pyfunction!(quota::py_get_deepx_quota_summary, m)?)?;
    m.add_function(wrap_pyfunction!(
        market_data::py_load_deepx_perp_trade_ticks,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        market_data::py_load_deepx_spot_trade_ticks,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        balance_changes::py_get_deepx_balance_changes,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        balance_changes::py_get_deepx_balance_change_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        liquidations::py_get_deepx_liquidation_records,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        liquidations::py_get_deepx_liquidation_record_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(quota::py_get_deepx_quota_history, m)?)?;
    m.add_function(wrap_pyfunction!(
        quota::py_get_deepx_quota_history_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        account::py_get_deepx_wallet_subaccounts,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        account::py_get_deepx_wallet_account_snapshot,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(account::py_get_deepx_user_stats, m)?)?;
    m.add_function(wrap_pyfunction!(
        account::py_get_deepx_perp_liquidation_price,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(positions::py_get_deepx_perp_positions, m)?)?;
    m.add_function(wrap_pyfunction!(
        perp_orders::py_get_deepx_perp_open_orders,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        perp_orders::py_get_deepx_perp_open_order_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        perp_orders::py_get_deepx_perp_history_orders,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        perp_orders::py_get_deepx_perp_history_order_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        perp_orders::py_get_deepx_perp_wallet_orders,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        order_lookup::py_get_deepx_perp_order_by_tx,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        order_lookup::py_get_deepx_spot_order_by_tx,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        transaction_status::py_get_deepx_rest_transaction_status,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        perp_orders::py_get_deepx_perp_wallet_order_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        positions::py_get_deepx_perp_position_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        positions::py_get_deepx_wallet_perp_positions,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        trades::py_get_deepx_perp_account_trades,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        trades::py_get_deepx_perp_account_trade_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        trades::py_get_deepx_perp_wallet_trades,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        trades::py_get_deepx_perp_wallet_trade_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        funding_fees::py_get_deepx_perp_funding_fees,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        funding_fees::py_get_deepx_perp_funding_fee_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        funding_fees::py_get_deepx_wallet_funding_fees,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        funding_fees::py_get_deepx_wallet_funding_fee_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        hourly_funding::py_get_deepx_hourly_unsettled_funding_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        spot_trades::py_get_deepx_spot_account_trades,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        spot_trades::py_get_deepx_spot_account_trade_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        spot_trades::py_get_deepx_spot_wallet_trades,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        spot_trades::py_get_deepx_spot_wallet_trade_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        spot_orders::py_get_deepx_spot_history_orders,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        spot_orders::py_get_deepx_spot_history_order_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        spot_orders::py_get_deepx_spot_open_orders,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        spot_orders::py_get_deepx_spot_open_order_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        spot_orders::py_get_deepx_spot_wallet_orders,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        spot_orders::py_get_deepx_spot_wallet_order_pages,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        delegates::py_get_deepx_delegate_accounts,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        delegates::py_get_deepx_delegator_accounts,
        m
    )?)?;

    let registry = get_global_pyo3_registry();
    registry
        .register_factory_extractor(DEEPX.to_string(), extract_deepx_data_factory)
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register DeepX data factory extractor: {error}"
            ))
        })?;
    registry
        .register_exec_factory_extractor(DEEPX.to_string(), extract_deepx_exec_factory)
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register DeepX exec factory extractor: {error}"
            ))
        })?;
    registry
        .register_config_extractor(
            stringify!(DeepXDataClientConfig).to_string(),
            extract_deepx_data_config,
        )
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register DeepX data config extractor: {error}"
            ))
        })?;
    registry
        .register_config_extractor(
            stringify!(DeepXExecutionClientConfig).to_string(),
            extract_deepx_exec_config,
        )
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register DeepX exec config extractor: {error}"
            ))
        })?;

    Ok(())
}
