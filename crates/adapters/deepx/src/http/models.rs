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

//! Python-bound and adapter-facing DeepX REST models.

use deepx_rust_sdk::rest::models::{
    DeepXBalanceChangePosition as DeepXBalanceChangePositionWire,
    DeepXBalanceChangeRecord as DeepXBalanceChangeRecordWire,
    DeepXDelegateAccount as DeepXDelegateAccountWire,
    DeepXDelegateWallets as DeepXDelegateWalletsWire,
    DeepXHourlyUnsettledFundingRecord as DeepXHourlyUnsettledFundingRecordWire,
    DeepXLendingAsset as DeepXLendingAssetWire,
    DeepXLendingInterestRateHistory as DeepXLendingInterestRateHistoryWire,
    DeepXLendingInterestRateParams as DeepXLendingInterestRateParamsWire,
    DeepXLendingInterestRateRecord as DeepXLendingInterestRateRecordWire,
    DeepXLendingStatusHistory as DeepXLendingStatusHistoryWire,
    DeepXLendingStatusRecord as DeepXLendingStatusRecordWire,
    DeepXLiquidationRecord as DeepXLiquidationRecordWire,
    DeepXPerpAccountTradeRecord as DeepXPerpAccountTradeRecordWire,
    DeepXPerpFundingFeeRecord as DeepXPerpFundingFeeRecordWire,
    DeepXPerpLiquidationPrice as DeepXPerpLiquidationPriceWire,
    DeepXPerpOrderRecord as DeepXPerpOrderRecordWire,
    DeepXPerpPositionRecord as DeepXPerpPositionRecordWire,
    DeepXPerpWalletOrderMarket as DeepXPerpWalletOrderMarketWire,
    DeepXPerpWalletOrdersPage as DeepXPerpWalletOrdersPageWire,
    DeepXPerpWalletSubaccountOrders as DeepXPerpWalletSubaccountOrdersWire,
    DeepXPerpWalletSubaccountTrades as DeepXPerpWalletSubaccountTradesWire,
    DeepXPerpWalletTradeMarket as DeepXPerpWalletTradeMarketWire,
    DeepXQuotaHistoryRecord as DeepXQuotaHistoryRecordWire,
    DeepXQuotaSummary as DeepXQuotaSummaryWire,
    DeepXSpotAccountTradeRecord as DeepXSpotAccountTradeRecordWire,
    DeepXSpotOrderRecord as DeepXSpotOrderRecordWire,
    DeepXSpotWalletOrderMarket as DeepXSpotWalletOrderMarketWire,
    DeepXSpotWalletOrdersPage as DeepXSpotWalletOrdersPageWire,
    DeepXSpotWalletSubaccountOrders as DeepXSpotWalletSubaccountOrdersWire,
    DeepXSpotWalletSubaccountTrades as DeepXSpotWalletSubaccountTradesWire,
    DeepXSpotWalletTradeMarket as DeepXSpotWalletTradeMarketWire,
    DeepXSpotWalletTradesPage as DeepXSpotWalletTradesPageWire,
    DeepXSubaccountBalanceAsset as DeepXSubaccountBalanceAssetWire,
    DeepXSubaccountBalances as DeepXSubaccountBalancesWire,
    DeepXSubaccountEquity as DeepXSubaccountEquityWire,
    DeepXSubaccountMarginRatio as DeepXSubaccountMarginRatioWire,
    DeepXSubaccountPerpPositions as DeepXSubaccountPerpPositionsWire,
    DeepXSubaccountProfile as DeepXSubaccountProfileWire,
    DeepXSubaccountSnapshot as DeepXSubaccountSnapshotWire, DeepXUserStats as DeepXUserStatsWire,
    DeepXWalletAccountSnapshot as DeepXWalletAccountSnapshotWire,
    DeepXWalletDelegateAccounts as DeepXWalletDelegateAccountsWire,
    DeepXWalletPerpPositions as DeepXWalletPerpPositionsWire,
    DeepXWalletSubaccounts as DeepXWalletSubaccountsWire,
};
pub use deepx_rust_sdk::rest::{
    DeepXAccountPage, DeepXApiCode as DeepXResponseCode, DeepXApiResponse, DeepXRawAccountPage,
    models::{
        DeepXDelegateMode, DeepXExactMargin, DeepXFundingRatePage, DeepXFundingRateRecord,
        DeepXInvitationStatus, DeepXLongShortRatioPage, DeepXLongShortRatioRecord,
        DeepXOpenInterestPage, DeepXOpenInterestRecord, DeepXPerpCandle, DeepXPerpCandlesPage,
        DeepXPerpLastPrice, DeepXPerpMarket, DeepXPerpMarketLookup, DeepXPerpOrderBook,
        DeepXPerpOrderBookLevel, DeepXPerpTrade, DeepXPerpTradesPage, DeepXPerpVolume,
        DeepXSpotAdlCandidate, DeepXSpotAdlCandidates, DeepXSpotCandle, DeepXSpotCandlesPage,
        DeepXSpotLastPrice, DeepXSpotMarket, DeepXSpotOrderBook, DeepXSpotOrderBookLevel,
        DeepXSpotTrade, DeepXSpotTradesPage, DeepXSpotVolume, DeepXSubaccountDirectoryRecord,
    },
};
use serde::Deserialize;

macro_rules! impl_wire_deref {
    ($wrapper:ident => $wire:ty) => {
        impl From<$wire> for $wrapper {
            fn from(value: $wire) -> Self {
                Self(value)
            }
        }

        impl std::ops::Deref for $wrapper {
            type Target = $wire;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::ops::DerefMut for $wrapper {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

fn wrap_account_page<T, U: From<T>>(page: DeepXAccountPage<T>) -> DeepXAccountPage<U> {
    DeepXAccountPage {
        items: page.items.into_iter().map(Into::into).collect(),
        next_cursor: page.next_cursor,
        has_next: page.has_next,
    }
}

/// One lending pool asset identity exposed by the public market directory.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXLendingAsset(pub DeepXLendingAssetWire);
impl_wire_deref!(DeepXLendingAsset => DeepXLendingAssetWire);

/// Python-facing exact borrow-rate curve parameters for one lending pool asset.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXLendingInterestRateParams(pub DeepXLendingInterestRateParamsWire);
impl_wire_deref!(DeepXLendingInterestRateParams => DeepXLendingInterestRateParamsWire);

/// Python-facing exact lending supply and borrow APR observation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXLendingInterestRateRecord(pub DeepXLendingInterestRateRecordWire);
impl_wire_deref!(DeepXLendingInterestRateRecord => DeepXLendingInterestRateRecordWire);

impl std::borrow::Borrow<DeepXLendingInterestRateRecordWire> for DeepXLendingInterestRateRecord {
    fn borrow(&self) -> &DeepXLendingInterestRateRecordWire {
        &self.0
    }
}

/// Lending interest-rate history in venue response order.
pub type DeepXLendingInterestRateHistory =
    DeepXLendingInterestRateHistoryWire<DeepXLendingInterestRateRecord>;

/// Python-facing exact lending pool status observation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXLendingStatusRecord(pub DeepXLendingStatusRecordWire);
impl_wire_deref!(DeepXLendingStatusRecord => DeepXLendingStatusRecordWire);

/// Lending pool status history in venue response order.
pub type DeepXLendingStatusHistory = DeepXLendingStatusHistoryWire<DeepXLendingStatusRecord>;

/// Wallet-owned subaccounts returned by the account directory.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXWalletSubaccounts(pub DeepXWalletSubaccountsWire);
impl_wire_deref!(DeepXWalletSubaccounts => DeepXWalletSubaccountsWire);

/// One wallet-level delegate configuration in the observed REST wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXDelegateAccount(pub DeepXDelegateAccountWire);
impl_wire_deref!(DeepXDelegateAccount => DeepXDelegateAccountWire);

impl std::borrow::Borrow<DeepXDelegateAccountWire> for DeepXDelegateAccount {
    fn borrow(&self) -> &DeepXDelegateAccountWire {
        &self.0
    }
}

/// Request-scoped delegate configurations for one wallet.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXWalletDelegateAccounts {
    /// SDK-owned directory with Python-facing delegate records.
    pub(crate) inner: DeepXWalletDelegateAccountsWire<DeepXDelegateAccount>,
}

impl DeepXWalletDelegateAccounts {
    pub(crate) fn new(wallet: String, accounts: Vec<DeepXDelegateAccount>) -> Self {
        Self {
            inner: DeepXWalletDelegateAccountsWire::new(wallet, accounts),
        }
    }

    /// Returns the wallet address used for the delegate-directory query.
    #[must_use]
    pub fn wallet(&self) -> &str {
        self.inner.wallet()
    }
}

impl std::ops::Deref for DeepXWalletDelegateAccounts {
    type Target = DeepXWalletDelegateAccountsWire<DeepXDelegateAccount>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for DeepXWalletDelegateAccounts {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Request-scoped wallets bound to one delegate account.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXDelegateWallets(pub DeepXDelegateWalletsWire);
impl_wire_deref!(DeepXDelegateWallets => DeepXDelegateWalletsWire);

/// One fully validated point-in-time subaccount snapshot.
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSubaccountSnapshot(
    pub  DeepXSubaccountSnapshotWire<
        DeepXSubaccountProfile,
        DeepXSubaccountBalances,
        DeepXSubaccountEquity,
        DeepXSubaccountMarginRatio,
    >,
);
impl_wire_deref!(DeepXSubaccountSnapshot => DeepXSubaccountSnapshotWire<DeepXSubaccountProfile, DeepXSubaccountBalances, DeepXSubaccountEquity, DeepXSubaccountMarginRatio>);

/// Failure-atomic point-in-time snapshots for every subaccount in one wallet directory.
///
/// The constituent REST reads are not block-pinned and therefore do not form an atomic venue
/// snapshot. Failure atomicity means callers receive either every locally validated response or
/// an error, never a partial collection.
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXWalletAccountSnapshot(
    pub  DeepXWalletAccountSnapshotWire<
        DeepXWalletSubaccounts,
        DeepXUserStats,
        DeepXSubaccountSnapshot,
    >,
);
impl_wire_deref!(DeepXWalletAccountSnapshot => DeepXWalletAccountSnapshotWire<DeepXWalletSubaccounts, DeepXUserStats, DeepXSubaccountSnapshot>);

/// Validated position lifecycle pages for one wallet-owned subaccount.
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSubaccountPerpPositions(
    pub DeepXSubaccountPerpPositionsWire<DeepXSubaccountProfile, DeepXPerpPositionRecord>,
);
impl_wire_deref!(DeepXSubaccountPerpPositions => DeepXSubaccountPerpPositionsWire<DeepXSubaccountProfile, DeepXPerpPositionRecord>);

/// Bounded, failure-atomic wallet position lifecycle observations.
///
/// The constituent REST reads are mutable and do not form a block-pinned snapshot.
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXWalletPerpPositions(
    pub  DeepXWalletPerpPositionsWire<
        DeepXWalletSubaccounts,
        DeepXUserStats,
        DeepXSubaccountPerpPositions,
    >,
);
impl_wire_deref!(DeepXWalletPerpPositions => DeepXWalletPerpPositionsWire<DeepXWalletSubaccounts, DeepXUserStats, DeepXSubaccountPerpPositions>);

/// One subaccount profile in the observed account-info wire form.
#[derive(Clone, Debug, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSubaccountProfile(pub DeepXSubaccountProfileWire);
impl_wire_deref!(DeepXSubaccountProfile => DeepXSubaccountProfileWire);

/// One lending asset balance in the observed account-balance wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSubaccountBalanceAsset(pub DeepXSubaccountBalanceAssetWire);
impl_wire_deref!(DeepXSubaccountBalanceAsset => DeepXSubaccountBalanceAssetWire);

/// Exact lending balances for one subaccount.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSubaccountBalances(pub DeepXSubaccountBalancesWire);
impl_wire_deref!(DeepXSubaccountBalances => DeepXSubaccountBalancesWire);

/// Exact equity summary for one subaccount.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSubaccountEquity(pub DeepXSubaccountEquityWire);
impl_wire_deref!(DeepXSubaccountEquity => DeepXSubaccountEquityWire);

/// Account-wide margin displays and the exact values supplied by current REST responses.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSubaccountMarginRatio(pub DeepXSubaccountMarginRatioWire);
impl_wire_deref!(DeepXSubaccountMarginRatio => DeepXSubaccountMarginRatioWire);

/// Optional position identity attached to a balance change.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXBalanceChangePosition(pub DeepXBalanceChangePositionWire);
impl_wire_deref!(DeepXBalanceChangePosition => DeepXBalanceChangePositionWire);

/// One exact account balance-change record.
#[derive(Clone, Debug, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXBalanceChangeRecord(pub DeepXBalanceChangeRecordWire);
impl_wire_deref!(DeepXBalanceChangeRecord => DeepXBalanceChangeRecordWire);

/// One account liquidation record with exact raw chain integer values.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXLiquidationRecord(pub DeepXLiquidationRecordWire);
impl_wire_deref!(DeepXLiquidationRecord => DeepXLiquidationRecordWire);

/// Python-facing wallet directory counters returned by the user-stats endpoint.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXUserStats(pub DeepXUserStatsWire);
impl_wire_deref!(DeepXUserStats => DeepXUserStatsWire);

/// Python-facing wallet trading-volume and quota aggregate.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXQuotaSummary(pub DeepXQuotaSummaryWire);
impl_wire_deref!(DeepXQuotaSummary => DeepXQuotaSummaryWire);

/// Python-facing chain-confirmed wallet quota-history record.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXQuotaHistoryRecord(pub DeepXQuotaHistoryRecordWire);
impl_wire_deref!(DeepXQuotaHistoryRecord => DeepXQuotaHistoryRecordWire);

/// Python-facing liquidation-price observation for one subaccount and perpetual market.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXPerpLiquidationPrice(pub DeepXPerpLiquidationPriceWire);
impl_wire_deref!(DeepXPerpLiquidationPrice => DeepXPerpLiquidationPriceWire);

/// Python-facing perpetual account order record.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXPerpOrderRecord(pub DeepXPerpOrderRecordWire);
impl_wire_deref!(DeepXPerpOrderRecord => DeepXPerpOrderRecordWire);

/// Python-facing Spot account order record.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSpotOrderRecord(pub DeepXSpotOrderRecordWire);
impl_wire_deref!(DeepXSpotOrderRecord => DeepXSpotOrderRecordWire);

/// Python-facing Spot account trade record.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSpotAccountTradeRecord(pub DeepXSpotAccountTradeRecordWire);
impl_wire_deref!(DeepXSpotAccountTradeRecord => DeepXSpotAccountTradeRecordWire);

impl std::borrow::Borrow<DeepXSpotAccountTradeRecordWire> for DeepXSpotAccountTradeRecord {
    fn borrow(&self) -> &DeepXSpotAccountTradeRecordWire {
        &self.0
    }
}

/// Spot orders for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSpotWalletSubaccountOrders(
    pub DeepXSpotWalletSubaccountOrdersWire<DeepXSpotOrderRecord>,
);
impl_wire_deref!(
    DeepXSpotWalletSubaccountOrders => DeepXSpotWalletSubaccountOrdersWire<DeepXSpotOrderRecord>
);

/// One market group in a wallet-scoped Spot order response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSpotWalletOrderMarket(
    pub DeepXSpotWalletOrderMarketWire<DeepXSpotWalletSubaccountOrders>,
);
impl_wire_deref!(
    DeepXSpotWalletOrderMarket => DeepXSpotWalletOrderMarketWire<DeepXSpotWalletSubaccountOrders>
);

/// One normalized globally paginated Spot wallet-order response page.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSpotWalletOrdersPage(pub DeepXSpotWalletOrdersPageWire<DeepXSpotWalletOrderMarket>);
impl_wire_deref!(DeepXSpotWalletOrdersPage => DeepXSpotWalletOrdersPageWire<DeepXSpotWalletOrderMarket>);

impl From<DeepXSpotWalletOrdersPageWire> for DeepXSpotWalletOrdersPage {
    fn from(page: DeepXSpotWalletOrdersPageWire) -> Self {
        Self(DeepXSpotWalletOrdersPageWire {
            markets: page
                .markets
                .into_iter()
                .map(|market| {
                    DeepXSpotWalletOrderMarket(DeepXSpotWalletOrderMarketWire {
                        pair: market.pair,
                        name: market.name,
                        subaccounts: market
                            .subaccounts
                            .into_iter()
                            .map(|group| {
                                DeepXSpotWalletSubaccountOrders(
                                    DeepXSpotWalletSubaccountOrdersWire {
                                        subaccount: group.subaccount,
                                        orders: wrap_account_page(group.orders),
                                    },
                                )
                            })
                            .collect(),
                    })
                })
                .collect(),
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        })
    }
}

/// Spot trades for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSpotWalletSubaccountTrades(
    pub DeepXSpotWalletSubaccountTradesWire<DeepXSpotAccountTradeRecord>,
);
impl_wire_deref!(
    DeepXSpotWalletSubaccountTrades => DeepXSpotWalletSubaccountTradesWire<DeepXSpotAccountTradeRecord>
);

/// One market group in a wallet-scoped Spot trade response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSpotWalletTradeMarket(
    pub DeepXSpotWalletTradeMarketWire<DeepXSpotWalletSubaccountTrades>,
);
impl_wire_deref!(
    DeepXSpotWalletTradeMarket => DeepXSpotWalletTradeMarketWire<DeepXSpotWalletSubaccountTrades>
);

/// One normalized globally paginated Spot wallet-trade response page.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXSpotWalletTradesPage(pub DeepXSpotWalletTradesPageWire<DeepXSpotWalletTradeMarket>);
impl_wire_deref!(DeepXSpotWalletTradesPage => DeepXSpotWalletTradesPageWire<DeepXSpotWalletTradeMarket>);

impl From<DeepXSpotWalletTradesPageWire> for DeepXSpotWalletTradesPage {
    fn from(page: DeepXSpotWalletTradesPageWire) -> Self {
        Self(DeepXSpotWalletTradesPageWire {
            markets: page
                .markets
                .into_iter()
                .map(|market| {
                    DeepXSpotWalletTradeMarket(DeepXSpotWalletTradeMarketWire {
                        pair: market.pair,
                        name: market.name,
                        subaccounts: market
                            .subaccounts
                            .into_iter()
                            .map(|group| {
                                DeepXSpotWalletSubaccountTrades(
                                    DeepXSpotWalletSubaccountTradesWire {
                                        subaccount: group.subaccount,
                                        trades: wrap_account_page(group.trades),
                                    },
                                )
                            })
                            .collect(),
                    })
                })
                .collect(),
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        })
    }
}

/// Python-facing perpetual funding-fee record.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXPerpFundingFeeRecord(pub DeepXPerpFundingFeeRecordWire);
impl_wire_deref!(DeepXPerpFundingFeeRecord => DeepXPerpFundingFeeRecordWire);

/// Python-facing hourly unsettled-funding boundary.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXHourlyUnsettledFundingRecord(pub DeepXHourlyUnsettledFundingRecordWire);
impl_wire_deref!(DeepXHourlyUnsettledFundingRecord => DeepXHourlyUnsettledFundingRecordWire);

/// Python-facing perpetual account trade record.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXPerpAccountTradeRecord(pub DeepXPerpAccountTradeRecordWire);
impl_wire_deref!(DeepXPerpAccountTradeRecord => DeepXPerpAccountTradeRecordWire);

impl std::borrow::Borrow<DeepXPerpAccountTradeRecordWire> for DeepXPerpAccountTradeRecord {
    fn borrow(&self) -> &DeepXPerpAccountTradeRecordWire {
        &self.0
    }
}

/// Perpetual trades for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXPerpWalletSubaccountTrades(
    pub DeepXPerpWalletSubaccountTradesWire<DeepXPerpAccountTradeRecord>,
);
impl_wire_deref!(
    DeepXPerpWalletSubaccountTrades => DeepXPerpWalletSubaccountTradesWire<DeepXPerpAccountTradeRecord>
);

impl std::borrow::Borrow<DeepXPerpWalletSubaccountTradesWire<DeepXPerpAccountTradeRecord>>
    for DeepXPerpWalletSubaccountTrades
{
    fn borrow(&self) -> &DeepXPerpWalletSubaccountTradesWire<DeepXPerpAccountTradeRecord> {
        &self.0
    }
}

/// One market group in a wallet-scoped perpetual trade response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXPerpWalletTradeMarket(
    pub DeepXPerpWalletTradeMarketWire<DeepXPerpWalletSubaccountTrades>,
);
impl_wire_deref!(
    DeepXPerpWalletTradeMarket => DeepXPerpWalletTradeMarketWire<DeepXPerpWalletSubaccountTrades>
);

impl std::borrow::Borrow<DeepXPerpWalletTradeMarketWire<DeepXPerpWalletSubaccountTrades>>
    for DeepXPerpWalletTradeMarket
{
    fn borrow(&self) -> &DeepXPerpWalletTradeMarketWire<DeepXPerpWalletSubaccountTrades> {
        &self.0
    }
}

impl From<DeepXPerpWalletTradeMarketWire> for DeepXPerpWalletTradeMarket {
    fn from(market: DeepXPerpWalletTradeMarketWire) -> Self {
        Self(DeepXPerpWalletTradeMarketWire {
            market_id: market.market_id,
            market_name: market.market_name,
            subaccounts: market
                .subaccounts
                .into_iter()
                .map(|group| {
                    DeepXPerpWalletSubaccountTrades(DeepXPerpWalletSubaccountTradesWire {
                        subaccount: group.subaccount,
                        trades: wrap_account_page(group.trades),
                    })
                })
                .collect(),
        })
    }
}

/// Perpetual orders for one subaccount within a wallet-grouped response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXPerpWalletSubaccountOrders(
    pub DeepXPerpWalletSubaccountOrdersWire<DeepXPerpOrderRecord>,
);
impl_wire_deref!(
    DeepXPerpWalletSubaccountOrders => DeepXPerpWalletSubaccountOrdersWire<DeepXPerpOrderRecord>
);

/// One market group in a wallet-scoped perpetual order response.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXPerpWalletOrderMarket(
    pub DeepXPerpWalletOrderMarketWire<DeepXPerpWalletSubaccountOrders>,
);
impl_wire_deref!(
    DeepXPerpWalletOrderMarket => DeepXPerpWalletOrderMarketWire<DeepXPerpWalletSubaccountOrders>
);

/// One normalized globally paginated wallet-order response page.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXPerpWalletOrdersPage(pub DeepXPerpWalletOrdersPageWire<DeepXPerpWalletOrderMarket>);
impl_wire_deref!(DeepXPerpWalletOrdersPage => DeepXPerpWalletOrdersPageWire<DeepXPerpWalletOrderMarket>);

impl From<DeepXPerpWalletOrdersPageWire> for DeepXPerpWalletOrdersPage {
    fn from(page: DeepXPerpWalletOrdersPageWire) -> Self {
        Self(DeepXPerpWalletOrdersPageWire {
            markets: page
                .markets
                .into_iter()
                .map(|market| {
                    DeepXPerpWalletOrderMarket(DeepXPerpWalletOrderMarketWire {
                        market_id: market.market_id,
                        market_name: market.market_name,
                        subaccounts: market
                            .subaccounts
                            .into_iter()
                            .map(|group| {
                                DeepXPerpWalletSubaccountOrders(
                                    DeepXPerpWalletSubaccountOrdersWire {
                                        subaccount: group.subaccount,
                                        orders: wrap_account_page(group.orders),
                                    },
                                )
                            })
                            .collect(),
                    })
                })
                .collect(),
            next_cursor: page.next_cursor,
            has_next: page.has_next,
        })
    }
}

/// One perpetual position lifecycle in the observed account-position wire form.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", skip_from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXPerpPositionRecord(pub DeepXPerpPositionRecordWire);
impl_wire_deref!(DeepXPerpPositionRecord => DeepXPerpPositionRecordWire);

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn sdk_wallet_pages_preserve_records_and_cursors_in_adapter_wrappers() {
        let spot_orders: DeepXApiResponse<Vec<DeepXSpotWalletOrderMarketWire>> =
            serde_json::from_str(include_str!(
                "../../deepx-rust-sdk/test_data/http/testnet/spot_wallet_orders_eth_usdc_page_1.json"
            ))
            .unwrap();
        let cursor = spot_orders.data[0].subaccounts[0]
            .orders
            .next_cursor
            .clone();
        let first_order = spot_orders.data[0].subaccounts[0].orders.items[0].clone();
        let wrapped: DeepXSpotWalletOrdersPage = DeepXSpotWalletOrdersPageWire {
            markets: spot_orders.data,
            next_cursor: cursor.clone(),
            has_next: true,
        }
        .into();
        assert_eq!(wrapped.next_cursor, cursor);
        assert_eq!(
            wrapped.markets[0].subaccounts[0].orders.items[0].0,
            first_order
        );

        let spot_trades: DeepXApiResponse<Vec<DeepXSpotWalletTradeMarketWire>> =
            serde_json::from_str(include_str!(
                "../../deepx-rust-sdk/test_data/http/testnet/spot_wallet_trades_eth_usdc_page_1.json"
            ))
            .unwrap();
        let cursor = spot_trades.data[0].subaccounts[0]
            .trades
            .next_cursor
            .clone();
        let first_trade = spot_trades.data[0].subaccounts[0].trades.items[0].clone();
        let wrapped: DeepXSpotWalletTradesPage = DeepXSpotWalletTradesPageWire {
            markets: spot_trades.data,
            next_cursor: cursor.clone(),
            has_next: true,
        }
        .into();
        assert_eq!(wrapped.next_cursor, cursor);
        assert_eq!(
            wrapped.markets[0].subaccounts[0].trades.items[0].0,
            first_trade
        );

        let perp_orders: DeepXApiResponse<Vec<DeepXPerpWalletOrderMarketWire>> =
            serde_json::from_str(include_str!(
                "../../deepx-rust-sdk/test_data/http/testnet/perp_wallet_orders_page_1.json"
            ))
            .unwrap();
        let cursor = perp_orders.data[0].subaccounts[0]
            .orders
            .next_cursor
            .clone();
        let first_order = perp_orders.data[0].subaccounts[0].orders.items[0].clone();
        let wrapped: DeepXPerpWalletOrdersPage = DeepXPerpWalletOrdersPageWire {
            markets: perp_orders.data,
            next_cursor: cursor.clone(),
            has_next: true,
        }
        .into();
        assert_eq!(wrapped.next_cursor, cursor);
        assert_eq!(
            wrapped.markets[0].subaccounts[0].orders.items[0].0,
            first_order
        );
    }
}
