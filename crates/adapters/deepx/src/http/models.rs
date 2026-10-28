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
    DeepXPerpWalletSubaccountOrders as DeepXPerpWalletSubaccountOrdersWire,
    DeepXPerpWalletSubaccountTrades as DeepXPerpWalletSubaccountTradesWire,
    DeepXPerpWalletTradeMarket as DeepXPerpWalletTradeMarketWire,
    DeepXQuotaHistoryRecord as DeepXQuotaHistoryRecordWire,
    DeepXQuotaSummary as DeepXQuotaSummaryWire,
    DeepXSpotAccountTradeRecord as DeepXSpotAccountTradeRecordWire,
    DeepXSpotOrderRecord as DeepXSpotOrderRecordWire,
    DeepXSpotWalletOrderMarket as DeepXSpotWalletOrderMarketWire,
    DeepXSpotWalletSubaccountOrders as DeepXSpotWalletSubaccountOrdersWire,
    DeepXSpotWalletSubaccountTrades as DeepXSpotWalletSubaccountTradesWire,
    DeepXSpotWalletTradeMarket as DeepXSpotWalletTradeMarketWire,
    DeepXSubaccountBalanceAsset as DeepXSubaccountBalanceAssetWire,
    DeepXSubaccountBalances as DeepXSubaccountBalancesWire,
    DeepXSubaccountEquity as DeepXSubaccountEquityWire,
    DeepXSubaccountMarginRatio as DeepXSubaccountMarginRatioWire,
    DeepXSubaccountProfile as DeepXSubaccountProfileWire, DeepXUserStats as DeepXUserStatsWire,
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
pub struct DeepXWalletSubaccounts {
    /// Wallet address used for the directory query.
    wallet: String,
    /// Subaccount addresses in venue response order.
    pub addresses: Vec<String>,
}

impl DeepXWalletSubaccounts {
    pub(crate) fn new(wallet: String, addresses: Vec<String>) -> Self {
        Self { wallet, addresses }
    }

    /// Returns the wallet address used for the directory query.
    #[must_use]
    pub fn wallet(&self) -> &str {
        &self.wallet
    }
}

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
    /// Wallet address used for the delegate-directory query.
    wallet: String,
    /// Delegate configurations in venue response order.
    pub accounts: Vec<DeepXDelegateAccount>,
}

impl DeepXWalletDelegateAccounts {
    pub(crate) fn new(wallet: String, accounts: Vec<DeepXDelegateAccount>) -> Self {
        Self { wallet, accounts }
    }

    /// Returns the wallet address used for the delegate-directory query.
    #[must_use]
    pub fn wallet(&self) -> &str {
        &self.wallet
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
pub struct DeepXDelegateWallets {
    /// Delegate address used for the reverse-directory query.
    delegate: String,
    /// Wallet addresses in venue response order.
    pub wallets: Vec<String>,
}

impl DeepXDelegateWallets {
    pub(crate) fn new(delegate: String, wallets: Vec<String>) -> Self {
        Self { delegate, wallets }
    }

    /// Returns the delegate address used for the reverse-directory query.
    #[must_use]
    pub fn delegate(&self) -> &str {
        &self.delegate
    }
}

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
pub struct DeepXSubaccountSnapshot {
    /// Subaccount profile bound to the requested wallet authority.
    pub profile: DeepXSubaccountProfile,
    /// Exact lending balances bound to the profile address.
    pub balances: DeepXSubaccountBalances,
    /// Exact equity summary bound to the profile address.
    pub equity: DeepXSubaccountEquity,
    /// Request-scoped collateral and margin requirement summary.
    pub margin_ratio: DeepXSubaccountMarginRatio,
}

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
pub struct DeepXWalletAccountSnapshot {
    /// Validated wallet directory in venue response order.
    pub directory: DeepXWalletSubaccounts,
    /// Validated wallet statistics with a subaccount set matching the directory.
    pub stats: DeepXUserStats,
    /// Validated subaccount snapshots in the same order as `directory.addresses`.
    pub subaccounts: Vec<DeepXSubaccountSnapshot>,
}

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
pub struct DeepXSubaccountPerpPositions {
    /// Profile whose authority matches the requested wallet.
    pub profile: DeepXSubaccountProfile,
    /// Validated position pages in venue cursor order.
    pub pages: Vec<DeepXAccountPage<DeepXPerpPositionRecord>>,
}

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
pub struct DeepXWalletPerpPositions {
    /// Validated wallet directory in venue response order.
    pub directory: DeepXWalletSubaccounts,
    /// Validated statistics whose subaccount set matches the directory.
    pub stats: DeepXUserStats,
    /// Position pages in the same order as `directory.addresses`.
    pub subaccounts: Vec<DeepXSubaccountPerpPositions>,
}

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
pub struct DeepXSpotWalletOrdersPage {
    /// Market and subaccount groups in venue response order.
    pub markets: Vec<DeepXSpotWalletOrderMarket>,
    /// Opaque global cursor repeated consistently by every nonempty nested group.
    pub next_cursor: Option<String>,
    /// Whether every nonempty nested group reports another global page.
    pub has_next: bool,
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
pub struct DeepXSpotWalletTradesPage {
    /// Market and subaccount groups in venue response order.
    pub markets: Vec<DeepXSpotWalletTradeMarket>,
    /// Opaque global cursor repeated consistently by every nonempty nested group.
    pub next_cursor: Option<String>,
    /// Whether every nonempty nested group reports another global page.
    pub has_next: bool,
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
pub struct DeepXPerpWalletOrdersPage {
    /// Market and subaccount groups in venue response order.
    pub markets: Vec<DeepXPerpWalletOrderMarket>,
    /// Opaque global cursor repeated consistently by every nested group.
    pub next_cursor: Option<String>,
    /// Whether every nested group reports another global page.
    pub has_next: bool,
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
