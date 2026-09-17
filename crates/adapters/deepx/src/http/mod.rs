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

//! HTTP transport for the DeepX testnet API.

pub mod client;
pub mod error;
pub mod models;
pub mod pagination;
pub mod query;
pub mod retry;

pub use client::DeepXHttpClient;
pub use error::{DeepXHttpError, Result};
pub use models::{
    DeepXAccountPage, DeepXApiResponse, DeepXBalanceChangePosition, DeepXBalanceChangeRecord,
    DeepXDelegateAccount, DeepXDelegateMode, DeepXDelegateWallets, DeepXFundingRatePage,
    DeepXFundingRateRecord, DeepXHourlyUnsettledFundingRecord, DeepXLendingAsset,
    DeepXLendingInterestRateHistory, DeepXLendingInterestRateParams,
    DeepXLendingInterestRateRecord, DeepXLendingStatusHistory, DeepXLendingStatusRecord,
    DeepXLiquidationRecord, DeepXLongShortRatioPage, DeepXLongShortRatioRecord,
    DeepXOpenInterestPage, DeepXOpenInterestRecord, DeepXPerpAccountTradeRecord, DeepXPerpCandle,
    DeepXPerpCandlesPage, DeepXPerpFundingFeeRecord, DeepXPerpLastPrice, DeepXPerpLiquidationPrice,
    DeepXPerpMarket, DeepXPerpMarketLookup, DeepXPerpOrderBook, DeepXPerpOrderBookLevel,
    DeepXPerpOrderRecord, DeepXPerpPositionRecord, DeepXPerpTrade, DeepXPerpTradesPage,
    DeepXPerpVolume, DeepXPerpWalletOrderMarket, DeepXPerpWalletOrdersPage,
    DeepXPerpWalletSubaccountOrders, DeepXPerpWalletSubaccountTrades, DeepXPerpWalletTradeMarket,
    DeepXQuotaHistoryRecord, DeepXQuotaSummary, DeepXRawAccountPage, DeepXSpotAccountTradeRecord,
    DeepXSpotCandle, DeepXSpotCandlesPage, DeepXSpotLastPrice, DeepXSpotMarket, DeepXSpotOrderBook,
    DeepXSpotOrderBookLevel, DeepXSpotOrderRecord, DeepXSpotTrade, DeepXSpotTradesPage,
    DeepXSpotVolume, DeepXSpotWalletOrderMarket, DeepXSpotWalletOrdersPage,
    DeepXSpotWalletSubaccountOrders, DeepXSpotWalletSubaccountTrades, DeepXSpotWalletTradeMarket,
    DeepXSpotWalletTradesPage, DeepXSubaccountBalanceAsset, DeepXSubaccountBalances,
    DeepXSubaccountDirectoryRecord, DeepXSubaccountEquity, DeepXSubaccountMarginRatio,
    DeepXSubaccountProfile, DeepXSubaccountSnapshot, DeepXUserStats, DeepXWalletAccountSnapshot,
    DeepXWalletDelegateAccounts, DeepXWalletSubaccounts,
};
pub use pagination::{CursorPagination, PaginationDecision};
pub use query::{
    DeepXAccountSortOrder, DeepXAllSubaccountsRequest, DeepXBalanceChangeType,
    DeepXBalanceChangesRequest, DeepXFundingRateRequest, DeepXHourlyUnsettledFundingCursor,
    DeepXHourlyUnsettledFundingRequest, DeepXLendingHistoryInterval, DeepXLendingHistoryRequest,
    DeepXLendingMarketRequest, DeepXLiquidationRecordsRequest, DeepXLiquidationType,
    DeepXLongShortRatioRequest, DeepXOpenInterestRequest, DeepXPerpAccountTradesRequest,
    DeepXPerpCandleInterval, DeepXPerpCandlesRequest, DeepXPerpFundingFeeRequest,
    DeepXPerpHistoryOrdersRequest, DeepXPerpLastPriceRequest, DeepXPerpLiquidationPriceRequest,
    DeepXPerpMarkPriceRequest, DeepXPerpOpenOrdersRequest, DeepXPerpOraclePriceRequest,
    DeepXPerpOrderBookRequest, DeepXPerpPositionsRequest, DeepXPerpTradesHistoryRequest,
    DeepXPerpTradesRequest, DeepXPerpVolumePeriod, DeepXPerpVolumeRequest,
    DeepXPerpWalletOrdersRequest, DeepXPerpWalletTradesRequest, DeepXQuotaBuyerType,
    DeepXQuotaHistoryRequest, DeepXQuotaHistoryType, DeepXSpotAccountTradesRequest,
    DeepXSpotCandleInterval, DeepXSpotCandlesRequest, DeepXSpotHistoryOrdersRequest,
    DeepXSpotLastPriceRequest, DeepXSpotOpenOrdersRequest, DeepXSpotOrderBookRequest,
    DeepXSpotOrderByIdRequest, DeepXSpotOrderSide, DeepXSpotTradesRequest, DeepXSpotVolumePeriod,
    DeepXSpotVolumeRequest, DeepXSpotWalletOrdersRequest, DeepXSpotWalletTradesRequest,
    DeepXWalletFundingFeeRequest,
};
pub use retry::{deepx_http_retry_config, should_retry_http_error};
