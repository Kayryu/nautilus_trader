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

//! Typed and raw market REST queries.

use std::collections::HashSet;

use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::value::RawValue;

use super::{DeepXRestError, DeepXRestTransport, is_account_address};
use crate::{
    pagination::validate_cursor_page,
    parse::parse_canonical_order_id,
    query::{
        DeepXAccountSortOrder, DeepXFundingRateRequest, DeepXLendingHistoryRequest,
        DeepXLendingMarketRequest, DeepXLongShortRatioRequest, DeepXOpenInterestRequest,
        DeepXPerpCandlesRequest, DeepXPerpLastPriceRequest, DeepXPerpMarkPriceRequest,
        DeepXPerpOraclePriceRequest, DeepXPerpOrderBookRequest, DeepXPerpTradesQuery,
        DeepXPerpVolumeRequest, DeepXSpotCandlesRequest, DeepXSpotLastPriceRequest,
        DeepXSpotOrderBookRequest, DeepXSpotTradesRequest, DeepXSpotVolumeRequest,
    },
    rest::models::{
        DeepXFundingRatePage, DeepXLendingAsset, DeepXLendingInterestRateHistory,
        DeepXLendingInterestRateParams, DeepXLendingInterestRateRecord, DeepXLendingStatusHistory,
        DeepXLendingStatusRecord, DeepXLongShortRatioPage, DeepXOpenInterestPage,
        DeepXPerpCandlesPage, DeepXPerpLastPrice, DeepXPerpMarket, DeepXPerpMarketLookup,
        DeepXPerpOrderBook, DeepXPerpOrderBookLevel, DeepXPerpTradesPage, DeepXPerpVolume,
        DeepXSpotCandlesPage, DeepXSpotLastPrice, DeepXSpotMarket, DeepXSpotOrderBook,
        DeepXSpotOrderBookLevel, DeepXSpotTrade, DeepXSpotTradesPage, DeepXSpotVolume,
    },
    symbol::decode_spot_pair_identity,
};

const SPOT_MARKETS_PATH: &str = "/internal/v1/market/spot/markets";
const SPOT_MARKET_BY_NAME_PATH: &str = "/internal/v1/market/spot/market-by-name";
const SPOT_MARKET_BY_PAIR_PATH: &str = "/internal/v1/market/spot/market-by-pair";
const PERP_MARKETS_PATH: &str = "/internal/v1/market/perp/markets";
const PERP_MARKET_BY_ID_PATH: &str = "/internal/v1/market/perp/market-by-id";
const PERP_MARKET_BY_NAME_PATH: &str = "/internal/v1/market/perp/market-by-name";
const LENDING_ASSETS_PATH: &str = "/internal/v1/market/lending/assets";
const LENDING_INTEREST_RATE_PATH: &str = "/internal/v1/market/lending/interest-rate";
const LENDING_INTEREST_RATE_HISTORY_PATH: &str =
    "/internal/v1/market/lending/interest-rate-history";
const LENDING_STATUS_HISTORY_PATH: &str = "/internal/v1/market/lending/status-history";
const PERP_FUNDING_RATE_PATH: &str = "/internal/v1/market/perp/funding_rate";
const PERP_LONG_SHORT_RATIO_PATH: &str = "/internal/v1/market/perp/long_short_ratio";
const PERP_OPEN_INTEREST_PATH: &str = "/internal/v1/market/perp/open_interest";
const PERP_TRADES_PATH: &str = "/internal/v1/market/perp/trades";
const PERP_CANDLES_PATH: &str = "/internal/v1/market/perp/candles";
const PERP_MARK_PRICE_PATH: &str = "/internal/v1/market/perp/mark_price";
const PERP_ORACLE_PRICE_PATH: &str = "/internal/v1/market/perp/oracle_price";
const PERP_VOLUME_PATH: &str = "/internal/v1/market/perp/volume";
const PERP_LAST_PRICE_PATH: &str = "/internal/v1/market/perp/last_price";
const PERP_ORDER_BOOK_PATH: &str = "/internal/v1/market/perp/order-books";
const SPOT_TRADES_PATH: &str = "/internal/v1/market/spot/trades";
const SPOT_CANDLES_PATH: &str = "/internal/v1/market/spot/candles";
const SPOT_VOLUME_PATH: &str = "/internal/v1/market/spot/volume";
const SPOT_LAST_PRICE_PATH: &str = "/internal/v1/market/spot/last_price";
const SPOT_ORDER_BOOK_PATH: &str = "/internal/v1/market/spot/order-books";

/// Tracks Spot trade identities and global time order across cursor pages.
#[derive(Debug, Default)]
pub struct DeepXSpotTradeContinuity {
    ids: HashSet<u64>,
    previous: Option<jiff::Timestamp>,
}

impl DeepXSpotTradeContinuity {
    /// Checks one validated Spot trade page against previously observed pages.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid timestamp, repeated trade ID, or inconsistent time order.
    pub fn observe_page(
        &mut self,
        page: &DeepXSpotTradesPage,
        sort: DeepXAccountSortOrder,
    ) -> Result<(), DeepXRestError> {
        let endpoint = "spot trades";
        let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
        for (index, trade) in page.items.iter().enumerate() {
            let timestamp = trade
                .trade_time
                .parse::<jiff::Timestamp>()
                .map_err(|e| invalid(format!("item {index} has invalid tradeTime: {e}")))?;
            if self.previous.is_some_and(|last| match sort {
                DeepXAccountSortOrder::Ascending => timestamp < last,
                DeepXAccountSortOrder::Descending => timestamp > last,
            }) {
                return Err(invalid(format!(
                    "item {index} violates the requested time order"
                )));
            }
            self.previous = Some(timestamp);
            if !self.ids.insert(trade.id) {
                return Err(invalid(format!("item {index} repeats trade ID")));
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct NameQuery<'a> {
    name: &'a str,
}

#[derive(Serialize)]
struct PairQuery<'a> {
    pair: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MarketIdQuery {
    market_id: u64,
}

impl DeepXRestTransport {
    /// Returns the typed spot market directory.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, decoding, or invalid-market response error.
    pub async fn get_spot_markets(
        &self,
        base_url: &str,
    ) -> Result<Vec<DeepXSpotMarket>, DeepXRestError> {
        let markets = self.get_spot_market_entries(base_url).await?;
        validate_spot_markets(&markets)?;
        Ok(markets)
    }

    /// Returns individually validated Spot market entries in venue order.
    ///
    /// Duplicate market identities are preserved for callers that validate a complete catalog.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, decoding, or invalid-market response error.
    pub async fn get_spot_market_entries(
        &self,
        base_url: &str,
    ) -> Result<Vec<DeepXSpotMarket>, DeepXRestError> {
        let raw = self.get_spot_markets_raw(base_url).await?;
        let markets: Vec<DeepXSpotMarket> = serde_json::from_str(raw.get())?;
        for market in &markets {
            validate_spot_market("spot markets", market)?;
        }
        Ok(markets)
    }

    /// Returns the typed spot market selected by name.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or mismatched-market error.
    pub async fn get_spot_market_by_name(
        &self,
        base_url: &str,
        name: &str,
    ) -> Result<DeepXSpotMarket, DeepXRestError> {
        let raw = self.get_spot_market_by_name_raw(base_url, name).await?;
        let market = serde_json::from_str(raw.get())?;
        validate_spot_market("spot market by name", &market)?;
        if market.name != name {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot market by name",
                message: "response does not match the requested market name".to_string(),
            });
        }
        Ok(market)
    }

    /// Returns the typed spot market selected by bytes32 pair.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or mismatched-market error.
    pub async fn get_spot_market_by_pair(
        &self,
        base_url: &str,
        pair: &str,
    ) -> Result<DeepXSpotMarket, DeepXRestError> {
        let pair = normalize_spot_pair_identity(pair)?;
        let raw = self.get_spot_market_by_pair_raw(base_url, &pair).await?;
        let market = serde_json::from_str(raw.get())?;
        validate_spot_market("spot market by pair", &market)?;
        if decode_spot_pair_identity(&market.pair) != decode_spot_pair_identity(&pair) {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot market by pair",
                message: "response does not match the requested pair identity".to_string(),
            });
        }
        Ok(market)
    }

    /// Returns the typed perpetual market directory.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_perp_markets(
        &self,
        base_url: &str,
    ) -> Result<Vec<DeepXPerpMarket>, DeepXRestError> {
        let raw = self.get_perp_markets_raw(base_url).await?;
        Ok(serde_json::from_str(raw.get())?)
    }

    /// Returns the typed perpetual market selected by ID.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or mismatched-market error.
    pub async fn get_perp_market_by_id(
        &self,
        base_url: &str,
        market_id: u64,
    ) -> Result<DeepXPerpMarketLookup, DeepXRestError> {
        let raw = self.get_perp_market_by_id_raw(base_url, market_id).await?;
        let market = serde_json::from_str(raw.get())?;
        validate_perp_market_lookup("perp market by id", &market)?;
        if market.id != market_id {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp market by id",
                message: "response does not match the requested market ID".to_string(),
            });
        }
        Ok(market)
    }

    /// Returns the typed perpetual market selected by name.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or mismatched-market error.
    pub async fn get_perp_market_by_name(
        &self,
        base_url: &str,
        name: &str,
    ) -> Result<DeepXPerpMarketLookup, DeepXRestError> {
        let raw = self.get_perp_market_by_name_raw(base_url, name).await?;
        let market = serde_json::from_str(raw.get())?;
        validate_perp_market_lookup("perp market by name", &market)?;
        if market.name != name {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp market by name",
                message: "response does not match the requested market name".to_string(),
            });
        }
        Ok(market)
    }

    /// Returns the typed lending asset directory.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, decoding, or invalid-directory error.
    pub async fn get_lending_assets(
        &self,
        base_url: &str,
    ) -> Result<Vec<DeepXLendingAsset>, DeepXRestError> {
        let raw = self.get_lending_assets_raw(base_url).await?;
        let assets: Vec<DeepXLendingAsset> = serde_json::from_str(raw.get())?;
        validate_lending_assets(&assets)?;
        Ok(assets)
    }

    /// Returns exact lending interest-rate parameter curves.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-curve error.
    pub async fn get_lending_interest_rate_params(
        &self,
        base_url: &str,
        request: &DeepXLendingMarketRequest,
    ) -> Result<Vec<DeepXLendingInterestRateParams>, DeepXRestError> {
        let raw = self
            .get_lending_interest_rate_params_raw(base_url, request)
            .await?;
        let params: Vec<DeepXLendingInterestRateParams> = serde_json::from_str(raw.get())?;
        validate_lending_interest_rate_params(&params, request)?;
        Ok(params)
    }

    /// Returns exact lending interest-rate history.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-history error.
    pub async fn get_lending_interest_rate_history(
        &self,
        base_url: &str,
        request: &DeepXLendingHistoryRequest,
    ) -> Result<DeepXLendingInterestRateHistory, DeepXRestError> {
        let raw = self
            .get_lending_interest_rate_history_raw(base_url, request)
            .await?;
        let history: DeepXLendingInterestRateHistory = serde_json::from_str(raw.get())?;
        validate_lending_interest_rate_history(&history.details, request)?;
        Ok(history)
    }

    /// Returns exact lending pool-status history.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-history error.
    pub async fn get_lending_status_history(
        &self,
        base_url: &str,
        request: &DeepXLendingHistoryRequest,
    ) -> Result<DeepXLendingStatusHistory, DeepXRestError> {
        let raw = self
            .get_lending_status_history_raw(base_url, request)
            .await?;
        let history: DeepXLendingStatusHistory = serde_json::from_str(raw.get())?;
        validate_lending_status_history(&history.details, request)?;
        Ok(history)
    }

    /// Returns one validated perpetual funding-rate page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or response-identity error.
    pub async fn get_perp_funding_rates(
        &self,
        base_url: &str,
        request: &DeepXFundingRateRequest,
    ) -> Result<DeepXFundingRatePage, DeepXRestError> {
        let raw = self.get_perp_funding_rates_raw(base_url, request).await?;
        let page: DeepXFundingRatePage = serde_json::from_str(raw.get())?;
        validate_market_cursor_page(
            "perp funding-rate",
            request.market_id,
            page.market_id,
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
    }

    /// Returns one validated perpetual funding-rate page in descending order.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or response-identity error.
    pub async fn get_perp_funding_rates_descending(
        &self,
        base_url: &str,
        request: &DeepXFundingRateRequest,
    ) -> Result<DeepXFundingRatePage, DeepXRestError> {
        let raw = self
            .get_perp_funding_rates_descending_raw(base_url, request)
            .await?;
        let page: DeepXFundingRatePage = serde_json::from_str(raw.get())?;
        validate_market_cursor_page(
            "perp funding-rate",
            request.market_id,
            page.market_id,
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
    }

    /// Returns one validated perpetual long-short ratio page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or response-identity error.
    pub async fn get_perp_long_short_ratios(
        &self,
        base_url: &str,
        request: &DeepXLongShortRatioRequest,
    ) -> Result<DeepXLongShortRatioPage, DeepXRestError> {
        let raw = self
            .get_perp_long_short_ratios_raw(base_url, request)
            .await?;
        let page: DeepXLongShortRatioPage = serde_json::from_str(raw.get())?;
        validate_market_cursor_page(
            "perp long-short-ratio",
            request.market_id,
            page.market_id,
            page.has_next,
            page.next_cursor.as_deref(),
        )?;
        Ok(page)
    }

    /// Returns one validated perpetual open-interest page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-page response error.
    pub async fn get_perp_open_interest(
        &self,
        base_url: &str,
        request: &DeepXOpenInterestRequest,
    ) -> Result<DeepXOpenInterestPage, DeepXRestError> {
        let raw = self.get_perp_open_interest_raw(base_url, request).await?;
        let page = serde_json::from_str(raw.get())?;
        validate_open_interest_page(&page, request)?;
        Ok(page)
    }

    /// Returns one validated exact perpetual trade page.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, decoding, or invalid-page response error.
    pub async fn get_perp_trades(
        &self,
        base_url: &str,
        query: &DeepXPerpTradesQuery<'_>,
    ) -> Result<DeepXPerpTradesPage, DeepXRestError> {
        let raw = self.get_perp_trades_raw(base_url, query).await?;
        let page = serde_json::from_str(raw.get())?;
        validate_perp_trades_page(&page, query)?;
        Ok(page)
    }

    /// Returns one validated exact Spot trade page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-page response error.
    pub async fn get_spot_trades(
        &self,
        base_url: &str,
        request: &DeepXSpotTradesRequest,
    ) -> Result<DeepXSpotTradesPage, DeepXRestError> {
        let raw = self.get_spot_trades_raw(base_url, request).await?;
        let page = serde_json::from_str(raw.get())?;
        validate_spot_trades_page(&page, request)?;
        Ok(page)
    }

    /// Returns one validated exact Spot candle page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-page response error.
    pub async fn get_spot_candles(
        &self,
        base_url: &str,
        request: &DeepXSpotCandlesRequest,
    ) -> Result<DeepXSpotCandlesPage, DeepXRestError> {
        let raw = self.get_spot_candles_raw(base_url, request).await?;
        let page = serde_json::from_str(raw.get())?;
        validate_spot_candle_page(&page, request)?;
        Ok(page)
    }

    /// Returns exact Spot volume statistics.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-volume response error.
    pub async fn get_spot_volume(
        &self,
        base_url: &str,
        request: &DeepXSpotVolumeRequest,
    ) -> Result<DeepXSpotVolume, DeepXRestError> {
        let raw = self.get_spot_volume_raw(base_url, request).await?;
        let volume = serde_json::from_str(raw.get())?;
        validate_spot_volume(&volume)?;
        Ok(volume)
    }

    /// Returns the exact Spot last price.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-price response error.
    pub async fn get_spot_last_price(
        &self,
        base_url: &str,
        request: &DeepXSpotLastPriceRequest,
    ) -> Result<DeepXSpotLastPrice, DeepXRestError> {
        let raw = self.get_spot_last_price_raw(base_url, request).await?;
        let price: DeepXSpotLastPrice = serde_json::from_str(raw.get())?;
        validate_spot_last_price(&price)?;
        Ok(price)
    }

    /// Returns one validated exact Spot order-book snapshot.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-book response error.
    pub async fn get_spot_order_book(
        &self,
        base_url: &str,
        request: &DeepXSpotOrderBookRequest,
    ) -> Result<DeepXSpotOrderBook, DeepXRestError> {
        let raw = self.get_spot_order_book_raw(base_url, request).await?;
        let book = serde_json::from_str(raw.get())?;
        validate_spot_order_book(&book, request)?;
        Ok(book)
    }

    /// Returns one validated exact perpetual candle page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-page response error.
    pub async fn get_perp_candles(
        &self,
        base_url: &str,
        request: &DeepXPerpCandlesRequest,
    ) -> Result<DeepXPerpCandlesPage, DeepXRestError> {
        let raw = self.get_perp_candles_raw(base_url, request).await?;
        let page = serde_json::from_str(raw.get())?;
        validate_candle_page(
            "perp candles",
            &page,
            request.start_ms,
            request.end_ms,
            request.limit,
        )?;
        Ok(page)
    }

    /// Returns one validated exact perpetual mark-price candle page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-page response error.
    pub async fn get_perp_mark_prices(
        &self,
        base_url: &str,
        request: &DeepXPerpMarkPriceRequest,
    ) -> Result<DeepXPerpCandlesPage, DeepXRestError> {
        let raw = self.get_perp_mark_prices_raw(base_url, request).await?;
        let page = serde_json::from_str(raw.get())?;
        validate_candle_page(
            "perp mark-price",
            &page,
            request.start_ms,
            request.end_ms,
            request.limit,
        )?;
        Ok(page)
    }

    /// Returns one validated exact perpetual oracle-price candle page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-page response error.
    pub async fn get_perp_oracle_prices(
        &self,
        base_url: &str,
        request: &DeepXPerpOraclePriceRequest,
    ) -> Result<DeepXPerpCandlesPage, DeepXRestError> {
        let raw = self.get_perp_oracle_prices_raw(base_url, request).await?;
        let page = serde_json::from_str(raw.get())?;
        validate_candle_page(
            "perp oracle-price",
            &page,
            request.start_ms,
            request.end_ms,
            request.limit,
        )?;
        Ok(page)
    }

    /// Returns exact perpetual volume statistics.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_volume(
        &self,
        base_url: &str,
        request: &DeepXPerpVolumeRequest,
    ) -> Result<DeepXPerpVolume, DeepXRestError> {
        let raw = self.get_perp_volume_raw(base_url, request).await?;
        Ok(serde_json::from_str(raw.get())?)
    }

    /// Returns the exact perpetual last price.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_last_price(
        &self,
        base_url: &str,
        request: &DeepXPerpLastPriceRequest,
    ) -> Result<DeepXPerpLastPrice, DeepXRestError> {
        let raw = self.get_perp_last_price_raw(base_url, request).await?;
        Ok(serde_json::from_str(raw.get())?)
    }

    /// Returns one validated exact perpetual order-book snapshot.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or invalid-book response error.
    pub async fn get_perp_order_book(
        &self,
        base_url: &str,
        request: &DeepXPerpOrderBookRequest,
    ) -> Result<DeepXPerpOrderBook, DeepXRestError> {
        let raw = self.get_perp_order_book_raw(base_url, request).await?;
        let book = serde_json::from_str(raw.get())?;
        validate_perp_order_book(&book, request)?;
        Ok(book)
    }

    /// Returns uninterpreted perpetual funding-rate history.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_funding_rates_raw(
        &self,
        base_url: &str,
        request: &DeepXFundingRateRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_FUNDING_RATE_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted perpetual funding-rate history in descending order.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_funding_rates_descending_raw(
        &self,
        base_url: &str,
        request: &DeepXFundingRateRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(
            base_url,
            PERP_FUNDING_RATE_PATH,
            &request.as_descending_query(),
        )
        .await
    }

    /// Returns uninterpreted perpetual long-short ratio history.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_long_short_ratios_raw(
        &self,
        base_url: &str,
        request: &DeepXLongShortRatioRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_LONG_SHORT_RATIO_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted perpetual open-interest history.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_open_interest_raw(
        &self,
        base_url: &str,
        request: &DeepXOpenInterestRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_OPEN_INTEREST_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted perpetual trades for a prepared query.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_perp_trades_raw(
        &self,
        base_url: &str,
        query: &DeepXPerpTradesQuery<'_>,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        self.get_api_data_with_query(base_url, PERP_TRADES_PATH, query)
            .await
    }

    /// Returns uninterpreted spot trades.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_trades_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotTradesRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_TRADES_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted spot candles.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_candles_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotCandlesRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_CANDLES_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted spot volume.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_volume_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotVolumeRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_VOLUME_PATH, &request.as_query())
            .await
    }

    /// Returns the uninterpreted spot last price.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_last_price_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotLastPriceRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_LAST_PRICE_PATH, &request.as_query())
            .await
    }

    /// Returns an uninterpreted spot order-book snapshot.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_order_book_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotOrderBookRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_ORDER_BOOK_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted perpetual candles.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_candles_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpCandlesRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_CANDLES_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted perpetual mark-price candles.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_mark_prices_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpMarkPriceRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_MARK_PRICE_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted perpetual oracle-price candles.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_oracle_prices_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpOraclePriceRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_ORACLE_PRICE_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted perpetual volume.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_volume_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpVolumeRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_VOLUME_PATH, &request.as_query())
            .await
    }

    /// Returns the uninterpreted perpetual last price.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_last_price_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpLastPriceRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_LAST_PRICE_PATH, &request.as_query())
            .await
    }

    /// Returns an uninterpreted perpetual order-book snapshot.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_order_book_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpOrderBookRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_ORDER_BOOK_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted spot market metadata.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_spot_markets_raw(
        &self,
        base_url: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        self.get_api_data(base_url, SPOT_MARKETS_PATH).await
    }

    /// Returns uninterpreted spot market metadata selected by name.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_market_by_name_raw(
        &self,
        base_url: &str,
        name: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        if name.trim().is_empty() {
            return Err(DeepXRestError::InvalidRequest(
                "spot-market-by-name name must not be empty".to_string(),
            ));
        }
        self.get_api_data_with_query(base_url, SPOT_MARKET_BY_NAME_PATH, &NameQuery { name })
            .await
    }

    /// Returns uninterpreted spot market metadata selected by bytes32 pair.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_spot_market_by_pair_raw(
        &self,
        base_url: &str,
        pair: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        if !pair
            .strip_prefix("0x")
            .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err(DeepXRestError::InvalidRequest(
                "spot-market-by-pair requires a 0x-prefixed bytes32 pair".to_string(),
            ));
        }
        self.get_api_data_with_query(base_url, SPOT_MARKET_BY_PAIR_PATH, &PairQuery { pair })
            .await
    }

    /// Returns uninterpreted perpetual market metadata.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_perp_markets_raw(
        &self,
        base_url: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        self.get_api_data(base_url, PERP_MARKETS_PATH).await
    }

    /// Returns uninterpreted perpetual market metadata selected by ID.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_market_by_id_raw(
        &self,
        base_url: &str,
        market_id: u64,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        if market_id == 0 {
            return Err(DeepXRestError::InvalidRequest(
                "perp-market-by-id market ID must be positive".to_string(),
            ));
        }
        self.get_api_data_with_query(
            base_url,
            PERP_MARKET_BY_ID_PATH,
            &MarketIdQuery { market_id },
        )
        .await
    }

    /// Returns uninterpreted perpetual market metadata selected by name.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_market_by_name_raw(
        &self,
        base_url: &str,
        name: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        if name.trim().is_empty() {
            return Err(DeepXRestError::InvalidRequest(
                "perp-market-by-name name must not be empty".to_string(),
            ));
        }
        self.get_api_data_with_query(base_url, PERP_MARKET_BY_NAME_PATH, &NameQuery { name })
            .await
    }

    /// Returns uninterpreted lending asset metadata.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_lending_assets_raw(
        &self,
        base_url: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        self.get_api_data(base_url, LENDING_ASSETS_PATH).await
    }

    /// Returns uninterpreted lending interest-rate parameters.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_lending_interest_rate_params_raw(
        &self,
        base_url: &str,
        request: &DeepXLendingMarketRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, LENDING_INTEREST_RATE_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted lending interest-rate history.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_lending_interest_rate_history_raw(
        &self,
        base_url: &str,
        request: &DeepXLendingHistoryRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(
            base_url,
            LENDING_INTEREST_RATE_HISTORY_PATH,
            &request.as_query(),
        )
        .await
    }

    /// Returns uninterpreted lending pool-status history.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_lending_status_history_raw(
        &self,
        base_url: &str,
        request: &DeepXLendingHistoryRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, LENDING_STATUS_HISTORY_PATH, &request.as_query())
            .await
    }
}

fn normalize_spot_pair_identity(pair: &str) -> Result<String, DeepXRestError> {
    let identity = decode_spot_pair_identity(pair).ok_or_else(|| {
        DeepXRestError::InvalidRequest(
            "spot-market-by-pair requires a 32-byte hex pair identity".to_string(),
        )
    })?;
    Ok(format!("0x{}", hex::encode(identity)))
}

fn validate_market_cursor_page(
    endpoint: &'static str,
    expected: u64,
    received: u64,
    has_next: bool,
    next_cursor: Option<&str>,
) -> Result<(), DeepXRestError> {
    if received != expected {
        return Err(DeepXRestError::ResponseMarketMismatch {
            endpoint,
            expected,
            received,
        });
    }
    validate_cursor_page(endpoint, has_next, next_cursor)
}

fn validate_perp_trades_page(
    page: &DeepXPerpTradesPage,
    query: &DeepXPerpTradesQuery<'_>,
) -> Result<(), DeepXRestError> {
    for trade in &page.items {
        if trade.market_id != query.market_id {
            return Err(DeepXRestError::ResponseMarketMismatch {
                endpoint: "perp trades",
                expected: query.market_id,
                received: trade.market_id,
            });
        }
    }
    validate_cursor_page("perp trades", page.has_next, page.next_cursor.as_deref())?;
    if query
        .page_size
        .is_some_and(|limit| page.items.len() > limit as usize)
    {
        return Err(DeepXRestError::InvalidHistoryResponse {
            endpoint: "perp trades",
            message: "page exceeds requested page size".to_string(),
        });
    }
    Ok(())
}

pub(super) fn validate_spot_trades_page(
    page: &DeepXSpotTradesPage,
    request: &DeepXSpotTradesRequest,
) -> Result<(), DeepXRestError> {
    validate_cursor_page("spot trades", page.has_next, page.next_cursor.as_deref())?;
    if request
        .page_size
        .is_some_and(|limit| page.items.len() > limit as usize)
    {
        return Err(DeepXRestError::InvalidAccountResponse {
            endpoint: "spot trades",
            message: "page exceeds requested page size".to_string(),
        });
    }
    if page.total < u64::try_from(page.items.len()).unwrap_or(u64::MAX) {
        return Err(DeepXRestError::InvalidAccountResponse {
            endpoint: "spot trades",
            message: "page contains more records than the reported total".to_string(),
        });
    }
    let mut ids = HashSet::new();
    let mut previous = None;
    for (index, trade) in page.items.iter().enumerate() {
        let timestamp = validate_spot_trade_record(index, trade, request, previous)?;
        if !ids.insert(trade.id) {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot trades",
                message: format!("item {index} repeats trade ID"),
            });
        }
        previous = Some(timestamp);
    }
    Ok(())
}

fn validate_spot_trade_record(
    index: usize,
    trade: &DeepXSpotTrade,
    request: &DeepXSpotTradesRequest,
    previous: Option<jiff::Timestamp>,
) -> Result<jiff::Timestamp, DeepXRestError> {
    let endpoint = "spot trades";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if trade.id == 0 || trade.height == 0 {
        return Err(invalid(format!(
            "item {index} has an invalid trade ID or block height"
        )));
    }
    if parse_canonical_order_id(&trade.sell_id).is_none()
        || parse_canonical_order_id(&trade.buy_id).is_none()
    {
        return Err(invalid(format!(
            "item {index} has an invalid decimal order ID"
        )));
    }
    if !is_account_address(&trade.seller) || !is_account_address(&trade.buyer) {
        return Err(invalid(format!(
            "item {index} has an invalid counterparty identity"
        )));
    }
    if trade.pair_name.trim().is_empty()
        || !trade.pair.starts_with("0x")
        || decode_spot_pair_identity(&trade.pair).is_none()
        || request
            .name
            .as_deref()
            .is_some_and(|expected| trade.pair_name != expected)
        || request
            .pair
            .as_deref()
            .is_some_and(|expected| !trade.pair.eq_ignore_ascii_case(expected))
    {
        return Err(invalid(format!(
            "item {index} has an invalid or unexpected market identity"
        )));
    }
    if trade.price <= Decimal::ZERO
        || trade.base_amount <= Decimal::ZERO
        || trade.quote_amount <= Decimal::ZERO
        || trade.taker.trim().is_empty()
    {
        return Err(invalid(format!(
            "item {index} has invalid financial or taker values"
        )));
    }
    let timestamp = trade
        .trade_time
        .parse::<jiff::Timestamp>()
        .map_err(|e| invalid(format!("item {index} has invalid tradeTime: {e}")))?;
    let timestamp_ms = u64::try_from(timestamp.as_millisecond()).map_err(|_| {
        invalid(format!(
            "item {index} execution timestamp is before the Unix epoch"
        ))
    })?;
    if request.start_ms.is_some_and(|start| timestamp_ms < start)
        || request.end_ms.is_some_and(|end| timestamp_ms > end)
    {
        return Err(invalid(format!(
            "item {index} is outside the requested time range"
        )));
    }
    if previous.is_some_and(|last| match request.sort {
        DeepXAccountSortOrder::Ascending => timestamp < last,
        DeepXAccountSortOrder::Descending => timestamp > last,
    }) {
        return Err(invalid(format!(
            "item {index} violates the requested time order"
        )));
    }
    Ok(timestamp)
}

fn validate_spot_markets(markets: &[DeepXSpotMarket]) -> Result<(), DeepXRestError> {
    let endpoint = "spot markets";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut names = HashSet::new();
    let mut pairs = HashSet::new();
    for (index, market) in markets.iter().enumerate() {
        validate_spot_market(endpoint, market)?;
        if !names.insert(market.name.to_ascii_lowercase()) {
            return Err(invalid(format!("market {index} repeats a market name")));
        }
        let pair = decode_spot_pair_identity(&market.pair)
            .ok_or_else(|| invalid(format!("market {index} has an invalid pair identity")))?;
        if !pairs.insert(pair) {
            return Err(invalid(format!("market {index} repeats a pair identity")));
        }
    }
    Ok(())
}

fn validate_spot_market(
    endpoint: &'static str,
    market: &DeepXSpotMarket,
) -> Result<(), DeepXRestError> {
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut name_parts = market.name.split('/');
    let name_base = name_parts.next().unwrap_or_default();
    let name_quote = name_parts.next().unwrap_or_default();
    if name_base.is_empty()
        || name_quote.is_empty()
        || name_parts.next().is_some()
        || market.base_symbol.trim().is_empty()
        || market.quote_symbol.trim().is_empty()
        || !name_base.eq_ignore_ascii_case(&market.base_symbol)
        || !name_quote.eq_ignore_ascii_case(&market.quote_symbol)
        || decode_spot_pair_identity(&market.pair).is_none()
        || !is_account_address(&market.base_address)
        || !is_account_address(&market.quote_address)
        || market
            .base_address
            .eq_ignore_ascii_case(&market.quote_address)
        || market
            .base_symbol
            .eq_ignore_ascii_case(&market.quote_symbol)
    {
        return Err(invalid("market has invalid identity metadata".to_string()));
    }
    if market.price.is_sign_negative()
        || market.tick_size <= Decimal::ZERO
        || market.max_deviation_bps.is_sign_negative()
        || market.limit_order_guard_limit_long <= Decimal::ZERO
        || market.limit_order_guard_limit_short <= Decimal::ZERO
    {
        return Err(invalid("market has invalid financial metadata".to_string()));
    }
    Ok(())
}

fn validate_perp_market_lookup(
    endpoint: &'static str,
    market: &DeepXPerpMarketLookup,
) -> Result<(), DeepXRestError> {
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut name_parts = market.name.split('-');
    let name_base = name_parts.next().unwrap_or_default();
    let name_quote = name_parts.next().unwrap_or_default();
    if market.id == 0
        || market.quote_market_id == 0
        || market.id == market.quote_market_id
        || name_base.is_empty()
        || name_quote.is_empty()
        || name_parts.next().is_some()
        || market.base_symbol.trim().is_empty()
        || market.quote_symbol.trim().is_empty()
        || !name_base.eq_ignore_ascii_case(&market.base_symbol)
        || !name_quote.eq_ignore_ascii_case(&market.quote_symbol)
        || !is_account_address(&market.base_address)
        || !is_account_address(&market.quote_address)
        || market
            .base_address
            .eq_ignore_ascii_case(&market.quote_address)
        || market
            .base_symbol
            .eq_ignore_ascii_case(&market.quote_symbol)
    {
        return Err(invalid("market has invalid identity metadata".to_string()));
    }

    let has_deployer_metadata = market.deployer_delegate.is_some()
        || market.deployer_fee_recipient.is_some()
        || market.deployer_builder_fee_bps.is_some()
        || market.deployer_isolated_margin_only.is_some();
    if market
        .deployer
        .as_deref()
        .is_some_and(|value| !is_account_address(value))
        || market.deployer.is_none() && has_deployer_metadata
        || market
            .deployer_delegate
            .as_deref()
            .is_some_and(|value| !is_account_address(value))
        || market
            .deployer_fee_recipient
            .as_deref()
            .is_some_and(|value| !is_account_address(value))
        || market
            .deployer_builder_fee_bps
            .is_some_and(|value| value.is_sign_negative())
    {
        return Err(invalid("market has invalid deployer metadata".to_string()));
    }

    if market.oracle_price <= Decimal::ZERO
        || market.mark_price <= Decimal::ZERO
        || market.max_deviation_bps.is_sign_negative()
        || market.initial_margin_ratio <= Decimal::ZERO
        || market.initial_margin_ratio > Decimal::ONE
        || market.maintenance_margin_ratio.is_sign_negative()
        || market.maintenance_margin_ratio > market.initial_margin_ratio
        || market.order_spec_min_qty <= Decimal::ZERO
        || market.order_spec_min_notional <= Decimal::ZERO
        || market.limit_order_guard_limit_long <= Decimal::ZERO
        || market.limit_order_guard_limit_short <= Decimal::ZERO
        || market.impact_margin_value <= Decimal::ZERO
        || market.funding_rate_clamp_lower_bound > market.funding_rate_clamp_upper_bound
        || market.liquidation_duration == 0
        || market.liquidity_bucket_slippage_step == 0
        || market.liquidity_bucket_slippage_limit == 0
        || market.liquidity_bucket_slippage_step > market.liquidity_bucket_slippage_limit
        || market.liquidation_dust_value.is_sign_negative()
        || market.liquidator_share_fee_rate.is_sign_negative()
        || market.insurance_fund_share_fee_rate.is_sign_negative()
    {
        return Err(invalid("market has invalid financial metadata".to_string()));
    }

    if market.last_calc_funding_rate_time == 0
        || market.last_funding_rate_time == 0
        || market.last_calc_funding_rate_time > market.last_funding_rate_time
        || [
            market.last_calc_funding_rate_time,
            market.last_funding_rate_time,
        ]
        .into_iter()
        .any(|value| {
            i64::try_from(value)
                .ok()
                .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
                .is_none()
        })
    {
        return Err(invalid("market has invalid funding timestamps".to_string()));
    }
    Ok(())
}

fn validate_lending_assets(assets: &[DeepXLendingAsset]) -> Result<(), DeepXRestError> {
    let endpoint = "lending assets";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut identities = HashSet::new();
    for (index, asset) in assets.iter().enumerate() {
        if asset.market_id == 0 || asset.asset.trim().is_empty() || asset.height == 0 {
            return Err(invalid(format!(
                "item {index} has invalid identity or height"
            )));
        }
        asset
            .created_at
            .parse::<jiff::Timestamp>()
            .map_err(|e| invalid(format!("item {index} has invalid createdAt timestamp: {e}")))?;
        if !identities.insert((asset.market_id, asset.asset.to_ascii_lowercase())) {
            return Err(invalid(format!(
                "item {index} repeats a market/asset identity"
            )));
        }
    }
    Ok(())
}

fn validate_lending_interest_rate_params(
    params: &[DeepXLendingInterestRateParams],
    request: &DeepXLendingMarketRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "lending interest-rate parameters";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut identities = HashSet::new();
    for (index, curve) in params.iter().enumerate() {
        validate_lending_scope(
            endpoint,
            index,
            curve.market_id,
            &curve.asset,
            request.market_id,
            request.asset.as_deref(),
        )?;
        if !identities.insert((curve.market_id, curve.asset.to_ascii_lowercase())) {
            return Err(invalid(format!(
                "item {index} repeats a market/asset identity"
            )));
        }
        if curve.u4.is_some() != curve.r4.is_some()
            || curve.u5.is_some() != curve.r5.is_some()
            || curve.u5.is_some() && curve.u4.is_none()
        {
            return Err(invalid(format!(
                "item {index} has incomplete optional curve nodes"
            )));
        }
        let utilizations = [
            Some(curve.u1),
            Some(curve.u2),
            Some(curve.u3),
            curve.u4,
            curve.u5,
        ];
        let rates = [
            Some(curve.r1),
            Some(curve.r2),
            Some(curve.r3),
            curve.r4,
            curve.r5,
        ];
        let mut previous_utilization = Decimal::ZERO;
        if curve.r_min < Decimal::ZERO
            || curve.r_max < Decimal::ZERO
            || curve.rho < Decimal::ZERO
            || rates.into_iter().flatten().any(|rate| rate < Decimal::ZERO)
        {
            return Err(invalid(format!("item {index} has invalid curve bounds")));
        }
        for utilization in utilizations.into_iter().flatten() {
            if utilization <= previous_utilization || utilization > Decimal::ONE {
                return Err(invalid(format!("item {index} has unordered curve nodes")));
            }
            previous_utilization = utilization;
        }
    }
    Ok(())
}

fn validate_lending_scope(
    endpoint: &'static str,
    index: usize,
    market_id: u64,
    asset: &str,
    expected_market_id: Option<u64>,
    expected_asset: Option<&str>,
) -> Result<(), DeepXRestError> {
    if market_id == 0
        || expected_market_id.is_some_and(|expected| market_id != expected)
        || asset.trim().is_empty()
        || expected_asset.is_some_and(|expected| !asset.eq_ignore_ascii_case(expected))
    {
        return Err(DeepXRestError::InvalidHistoryResponse {
            endpoint,
            message: format!("item {index} does not match the requested market/asset scope"),
        });
    }
    Ok(())
}

fn validate_lending_interest_rate_history(
    records: &[DeepXLendingInterestRateRecord],
    request: &DeepXLendingHistoryRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "lending interest-rate history";
    validate_lending_history_len(endpoint, records.len(), request.limit)?;
    let mut identities = HashSet::new();
    let mut previous = None;
    for (index, record) in records.iter().enumerate() {
        validate_lending_history_scope(
            endpoint,
            index,
            record.market_id,
            &record.asset,
            record.statistic_time,
            request,
            &mut previous,
        )?;
        if record.supply_apr < Decimal::ZERO || record.borrow_apr < Decimal::ZERO {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint,
                message: format!("item {index} has a negative APR"),
            });
        }
        if !identities.insert((
            record.market_id,
            record.asset.to_ascii_lowercase(),
            record.statistic_time,
        )) {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint,
                message: format!("item {index} repeats a market/asset/time bucket"),
            });
        }
    }
    Ok(())
}

fn validate_lending_status_history(
    records: &[DeepXLendingStatusRecord],
    request: &DeepXLendingHistoryRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "lending status history";
    validate_lending_history_len(endpoint, records.len(), request.limit)?;
    let mut identities = HashSet::new();
    let mut previous = None;
    for (index, record) in records.iter().enumerate() {
        validate_lending_history_scope(
            endpoint,
            index,
            record.market_id,
            &record.asset,
            record.statistic_time,
            request,
            &mut previous,
        )?;
        if record.index_price <= Decimal::ZERO
            || record.total_supplied < Decimal::ZERO
            || record.total_borrowed < Decimal::ZERO
            || record.utilization_rate < Decimal::ZERO
            || record.utilization_rate > Decimal::ONE
            || record.supply_apr < Decimal::ZERO
            || record.borrow_apr < Decimal::ZERO
        {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint,
                message: format!("item {index} has invalid financial values"),
            });
        }
        if !identities.insert((
            record.market_id,
            record.asset.to_ascii_lowercase(),
            record.statistic_time,
        )) {
            return Err(DeepXRestError::InvalidHistoryResponse {
                endpoint,
                message: format!("item {index} repeats a market/asset/time bucket"),
            });
        }
    }
    Ok(())
}

fn validate_lending_history_scope(
    endpoint: &'static str,
    index: usize,
    market_id: u64,
    asset: &str,
    statistic_time: u64,
    request: &DeepXLendingHistoryRequest,
    previous: &mut Option<u64>,
) -> Result<(), DeepXRestError> {
    validate_lending_scope(
        endpoint,
        index,
        market_id,
        asset,
        request.market_id,
        request.asset.as_deref(),
    )?;
    let valid_time = i64::try_from(statistic_time)
        .ok()
        .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
        .is_some();
    if !valid_time
        || statistic_time < request.start_ms
        || request.end_ms.is_some_and(|end| statistic_time > end)
    {
        return Err(DeepXRestError::InvalidHistoryResponse {
            endpoint,
            message: format!("item {index} has an invalid or out-of-range timestamp"),
        });
    }
    if previous.is_some_and(|previous| match request.sort {
        DeepXAccountSortOrder::Ascending => statistic_time < previous,
        DeepXAccountSortOrder::Descending => statistic_time > previous,
    }) {
        return Err(DeepXRestError::InvalidHistoryResponse {
            endpoint,
            message: format!("item {index} violates the requested time order"),
        });
    }
    *previous = Some(statistic_time);
    Ok(())
}

fn validate_lending_history_len(
    endpoint: &'static str,
    len: usize,
    limit: Option<u32>,
) -> Result<(), DeepXRestError> {
    if limit.is_some_and(|limit| len > limit as usize) {
        return Err(DeepXRestError::InvalidHistoryResponse {
            endpoint,
            message: "response exceeds the requested limit".to_string(),
        });
    }
    Ok(())
}

fn validate_open_interest_page(
    page: &DeepXOpenInterestPage,
    request: &DeepXOpenInterestRequest,
) -> Result<(), DeepXRestError> {
    let invalid = |message: String| DeepXRestError::InvalidHistoryResponse {
        endpoint: "perp open-interest",
        message,
    };
    if page.pair.trim().is_empty() {
        return Err(invalid("missing pair identity".to_string()));
    }
    if page.details.len() > crate::query::DEEPX_MAX_HISTORY_PAGE_SIZE as usize
        || request
            .limit
            .is_some_and(|limit| page.details.len() > limit as usize)
    {
        return Err(invalid("page exceeds requested limit".to_string()));
    }
    let first_bucket = request.start_ms / 60_000 * 60_000;
    let end_bucket = request.end_ms.map(|end| end / 60_000 * 60_000);
    let mut previous = None;
    for (index, record) in page.details.iter().enumerate() {
        if !record.statistic_time.is_multiple_of(60_000)
            || record.statistic_time < first_bucket
            || end_bucket.is_some_and(|end| record.statistic_time >= end)
            || previous.is_some_and(|time| record.statistic_time <= time)
        {
            return Err(invalid(format!(
                "record {index} is outside the requested minute range or order"
            )));
        }
        if record.total_oi < Decimal::ZERO || record.long_short_ratio < Decimal::ZERO {
            return Err(invalid(format!(
                "record {index} has negative financial values"
            )));
        }
        previous = Some(record.statistic_time);
    }
    Ok(())
}

fn validate_candle_page(
    endpoint: &'static str,
    page: &DeepXPerpCandlesPage,
    start_ms: u64,
    end_ms: Option<u64>,
    limit: Option<u32>,
) -> Result<(), DeepXRestError> {
    let invalid = |message: String| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if page.pair.trim().is_empty() {
        return Err(invalid("missing pair identity".to_string()));
    }
    if page.details.len() > crate::query::DEEPX_MAX_HISTORY_PAGE_SIZE as usize {
        return Err(invalid("page exceeds venue limit of 5000".to_string()));
    }
    if limit.is_some_and(|limit| page.details.len() > limit as usize) {
        return Err(invalid("page exceeds requested limit".to_string()));
    }
    let mut previous = None;
    for (index, candle) in page.details.iter().enumerate() {
        if candle.time < start_ms || end_ms.is_some_and(|end_ms| candle.time >= end_ms) {
            return Err(invalid(format!(
                "candle {index} timestamp is outside the requested bounds"
            )));
        }
        if previous.is_some_and(|time| candle.time <= time) {
            return Err(invalid(format!(
                "candle {index} timestamp is not strictly ascending"
            )));
        }
        if candle.low > candle.high
            || candle.open < candle.low
            || candle.open > candle.high
            || candle.close < candle.low
            || candle.close > candle.high
        {
            return Err(invalid(format!(
                "candle {index} has inconsistent OHLC values"
            )));
        }
        if candle.volume.is_sign_negative() && !candle.volume.is_zero() {
            return Err(invalid(format!("candle {index} has negative volume")));
        }
        previous = Some(candle.time);
    }
    Ok(())
}

fn validate_spot_candle_page(
    page: &DeepXSpotCandlesPage,
    request: &DeepXSpotCandlesRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "spot candles";
    let invalid = |message: String| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if page.pair.trim().is_empty() || request.name.as_ref().is_some_and(|name| page.pair != *name) {
        return Err(invalid(
            "response has a missing or unexpected market name".to_string(),
        ));
    }
    if page.details.len() > crate::query::DEEPX_MAX_HISTORY_PAGE_SIZE as usize
        || request
            .limit
            .is_some_and(|limit| page.details.len() > limit as usize)
    {
        return Err(invalid("response exceeds its candle limit".to_string()));
    }
    let mut previous = None;
    for (index, candle) in page.details.iter().enumerate() {
        if candle.time < request.start_ms || request.end_ms.is_some_and(|end| candle.time >= end) {
            return Err(invalid(format!(
                "candle {index} timestamp is outside the requested bounds"
            )));
        }
        if previous.is_some_and(|time| candle.time <= time) {
            return Err(invalid(format!(
                "candle {index} timestamp is not strictly ascending"
            )));
        }
        if candle.low <= Decimal::ZERO
            || candle.low > candle.high
            || candle.open < candle.low
            || candle.open > candle.high
            || candle.close < candle.low
            || candle.close > candle.high
            || candle.volume.is_sign_negative()
        {
            return Err(invalid(format!("candle {index} has invalid OHLCV values")));
        }
        previous = Some(candle.time);
    }
    Ok(())
}

fn validate_spot_order_book(
    book: &DeepXSpotOrderBook,
    request: &DeepXSpotOrderBookRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "spot order book";
    let invalid = |message: String| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if book
        .pair
        .strip_prefix("0x")
        .and_then(decode_spot_pair_identity)
        .is_none()
        || book.pair_name.trim().is_empty()
        || request
            .name
            .as_ref()
            .is_some_and(|name| book.pair_name != *name)
        || request
            .pair
            .as_ref()
            .is_some_and(|pair| !book.pair.eq_ignore_ascii_case(pair))
    {
        return Err(invalid(
            "response has a missing or unexpected market identity".to_string(),
        ));
    }
    if book.last_update_id == 0
        || i64::try_from(book.engine_time)
            .ok()
            .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
            .is_none()
    {
        return Err(invalid(
            "response has an invalid sequence or engine time".to_string(),
        ));
    }
    if book.latest_price.is_sign_negative() || book.mid_price.is_sign_negative() {
        return Err(invalid(
            "response has a negative price observation".to_string(),
        ));
    }
    validate_spot_order_book_side(endpoint, "bid", &book.order_buy_list, false)?;
    validate_spot_order_book_side(endpoint, "ask", &book.order_sell_list, true)?;
    if let (Some(bid), Some(ask)) = (book.order_buy_list.first(), book.order_sell_list.first())
        && bid.price >= ask.price
    {
        return Err(invalid("response has a crossed or locked book".to_string()));
    }
    Ok(())
}

fn validate_perp_order_book(
    book: &DeepXPerpOrderBook,
    request: &DeepXPerpOrderBookRequest,
) -> Result<(), DeepXRestError> {
    let endpoint = "perp order book";
    let invalid = |message: String| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if book.market_id == 0 || book.market_id != request.market_id {
        return Err(invalid(
            "response has a missing or unexpected market identity".to_string(),
        ));
    }
    if book.last_update_id == 0
        || i64::try_from(book.engine_time)
            .ok()
            .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
            .is_none()
    {
        return Err(invalid(
            "response has an invalid sequence or engine time".to_string(),
        ));
    }
    if book.latest_price.is_sign_negative() || book.mid_price.is_sign_negative() {
        return Err(invalid(
            "response has a negative price observation".to_string(),
        ));
    }
    validate_perp_order_book_side(endpoint, "bid", book.market_id, &book.order_buy_list, false)?;
    validate_perp_order_book_side(endpoint, "ask", book.market_id, &book.order_sell_list, true)
}

fn validate_perp_order_book_side(
    endpoint: &'static str,
    side: &str,
    market_id: u64,
    levels: &[DeepXPerpOrderBookLevel],
    ascending: bool,
) -> Result<(), DeepXRestError> {
    let invalid = |message: String| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut prices = HashSet::new();
    let mut previous = None;
    for (index, level) in levels.iter().enumerate() {
        if level.market_id != market_id
            || level.price <= Decimal::ZERO
            || level.qty <= Decimal::ZERO
            || level.value.is_sign_negative()
            || !prices.insert(level.price)
        {
            return Err(invalid(format!(
                "{side} level {index} has invalid values or market identity"
            )));
        }
        let out_of_order = previous.is_some_and(|previous| {
            if ascending {
                level.price <= previous
            } else {
                level.price >= previous
            }
        });
        if out_of_order {
            return Err(invalid(format!(
                "{side} level {index} is not strictly price ordered"
            )));
        }
        previous = Some(level.price);
    }
    Ok(())
}

fn validate_spot_order_book_side(
    endpoint: &'static str,
    side: &str,
    levels: &[DeepXSpotOrderBookLevel],
    ascending: bool,
) -> Result<(), DeepXRestError> {
    let invalid = |message: String| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    let mut prices = HashSet::new();
    let mut previous = None;
    for (index, level) in levels.iter().enumerate() {
        if level.price <= Decimal::ZERO
            || level.qty <= Decimal::ZERO
            || level.value.is_sign_negative()
            || !prices.insert(level.price)
        {
            return Err(invalid(format!("{side} level {index} has invalid values")));
        }
        let out_of_order = previous.is_some_and(|previous| {
            if ascending {
                level.price <= previous
            } else {
                level.price >= previous
            }
        });
        if out_of_order {
            return Err(invalid(format!(
                "{side} level {index} is not strictly price ordered"
            )));
        }
        previous = Some(level.price);
    }
    Ok(())
}

fn validate_spot_volume(volume: &DeepXSpotVolume) -> Result<(), DeepXRestError> {
    let endpoint = "spot volume";
    let invalid = |message| DeepXRestError::InvalidHistoryResponse { endpoint, message };
    if volume.total_volume.is_sign_negative() {
        return Err(invalid("total volume must not be negative".to_string()));
    }
    if volume.start_time > volume.end_time || volume.end_time > volume.statistic_time {
        return Err(invalid(
            "volume timestamps are not monotonically ordered".to_string(),
        ));
    }
    for (name, value) in [
        ("startTime", volume.start_time),
        ("endTime", volume.end_time),
        ("statisticTime", volume.statistic_time),
    ] {
        if i64::try_from(value)
            .ok()
            .and_then(|value| jiff::Timestamp::from_millisecond(value).ok())
            .is_none()
        {
            return Err(invalid(format!("{name} is outside the timestamp range")));
        }
    }
    Ok(())
}

fn validate_spot_last_price(price: &DeepXSpotLastPrice) -> Result<(), DeepXRestError> {
    if price.0.is_sign_negative() {
        return Err(DeepXRestError::InvalidHistoryResponse {
            endpoint: "spot last price",
            message: "price must not be negative".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{
        query::{DeepXPerpTradesRequest, DeepXSpotTradesRequest},
        rest::{DeepXApiResponse, models::DeepXOpenInterestRecord},
    };

    const SPOT_VOLUME: &str =
        include_str!("../../test_data/http/testnet/spot_volume_eth_usdc_1h.json");
    const SPOT_CANDLES: &str =
        include_str!("../../test_data/http/testnet/spot_candles_eth_usdc_1m.json");
    const SPOT_ORDER_BOOK: &str =
        include_str!("../../test_data/http/testnet/spot_order_book_eth_usdc_tick_001.json");
    const PERP_ORDER_BOOK: &str =
        include_str!("../../test_data/http/testnet/perp_order_book_eth_usdc_tick_001.json");
    const SPOT_MARKETS: &str =
        include_str!("../../test_data/http/testnet/spot_markets_spec369.json");
    const SPOT_MARKET: &str =
        include_str!("../../test_data/http/testnet/spot_market_eth_usdc_by_pair.json");
    const PERP_MARKET: &str =
        include_str!("../../test_data/http/testnet/perp_market_eth_usdc_by_id.json");
    const LENDING_ASSETS: &str = include_str!("../../test_data/http/testnet/lending_assets.json");
    const LENDING_CURVES: &str =
        include_str!("../../test_data/http/testnet/lending_interest_rate_usdc.json");
    const LENDING_RATE_HISTORY: &str =
        include_str!("../../test_data/http/testnet/lending_interest_rate_history_usdc_1h.json");
    const LENDING_STATUS_HISTORY: &str =
        include_str!("../../test_data/http/testnet/lending_status_history_usdc_1h.json");
    const SPOT_TRADES: &str =
        include_str!("../../test_data/http/testnet/spot_trades_eth_usdc_page_1.json");

    #[test]
    fn perp_trade_page_checks_market_cursor_and_size() {
        let request = DeepXPerpTradesRequest {
            market_id: 3,
            page_size: Some(1),
            cursor: None,
        };
        let query = request.as_query();
        let trade = serde_json::from_value(serde_json::json!({
            "id": 1,
            "marketId": 3,
            "buyerOrderId": "1",
            "buyer": "0x01",
            "sellerOrderId": "2",
            "seller": "0x02",
            "price": "1",
            "size": "1",
            "buyerLeverage": "1",
            "sellerLeverage": "1",
            "createdAt": "2026-09-10T00:00:00Z",
            "filledDirection": "Long",
            "taker": "Buyer",
            "takerFee": "0",
            "makerFee": "0"
        }))
        .unwrap();
        let mut page = DeepXPerpTradesPage {
            items: vec![trade],
            next_cursor: None,
            has_next: false,
        };
        assert!(validate_perp_trades_page(&page, &query).is_ok());

        page.items[0].market_id = 4;
        assert!(matches!(
            validate_perp_trades_page(&page, &query),
            Err(DeepXRestError::ResponseMarketMismatch {
                endpoint: "perp trades",
                expected: 3,
                received: 4,
            })
        ));
        page.items[0].market_id = 3;
        page.has_next = true;
        assert!(matches!(
            validate_perp_trades_page(&page, &query),
            Err(DeepXRestError::MissingPaginationCursor {
                endpoint: "perp trades"
            })
        ));
        page.has_next = false;
        page.items.push(page.items[0].clone());
        assert!(matches!(
            validate_perp_trades_page(&page, &query),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp trades",
                ..
            })
        ));
    }

    #[test]
    fn spot_trade_page_checks_cursor_size_and_total() {
        let envelope: DeepXApiResponse<DeepXSpotTradesPage> =
            serde_json::from_str(SPOT_TRADES).unwrap();
        let mut page = envelope.data;
        let mut request = DeepXSpotTradesRequest {
            name: None,
            pair: None,
            wallet: None,
            start_ms: None,
            end_ms: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(2),
        };
        assert!(validate_spot_trades_page(&page, &request).is_ok());

        page.next_cursor = None;
        assert!(matches!(
            validate_spot_trades_page(&page, &request),
            Err(DeepXRestError::MissingPaginationCursor {
                endpoint: "spot trades"
            })
        ));
        page.next_cursor = Some("next".to_string());
        request.page_size = Some(1);
        assert!(matches!(
            validate_spot_trades_page(&page, &request),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "spot trades",
                ..
            })
        ));
        request.page_size = Some(2);
        page.total = 1;
        assert!(matches!(
            validate_spot_trades_page(&page, &request),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "spot trades",
                ..
            })
        ));
    }

    #[rstest]
    #[case("zero-id")]
    #[case("zero-height")]
    #[case("sell-id")]
    #[case("buy-id")]
    #[case("seller")]
    #[case("buyer")]
    #[case("pair")]
    #[case("foreign-name")]
    #[case("foreign-pair")]
    #[case("price")]
    #[case("base-amount")]
    #[case("quote-amount")]
    #[case("taker")]
    #[case("timestamp")]
    #[case("range")]
    #[case("order")]
    fn spot_trades_reject_invalid_response_semantics(#[case] mutation: &str) {
        let envelope: DeepXApiResponse<DeepXSpotTradesPage> =
            serde_json::from_str(SPOT_TRADES).unwrap();
        let mut trade = envelope.data.items[0].clone();
        let mut request = DeepXSpotTradesRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
            wallet: None,
            start_ms: None,
            end_ms: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(2),
        };
        let mut previous = None;
        match mutation {
            "zero-id" => trade.id = 0,
            "zero-height" => trade.height = 0,
            "sell-id" => trade.sell_id = "invalid".to_string(),
            "buy-id" => trade.buy_id = u128::MAX.to_string(),
            "seller" => trade.seller = "invalid".to_string(),
            "buyer" => trade.buyer = "invalid".to_string(),
            "pair" => trade.pair = "0x1234".to_string(),
            "foreign-name" => trade.pair_name = "OTHER/USDC".to_string(),
            "foreign-pair" => {
                request.name = None;
                request.pair = Some(
                    "0x1111111111111111111111111111111111111111111111111111111111111111"
                        .to_string(),
                );
            }
            "price" => trade.price = Decimal::ZERO,
            "base-amount" => trade.base_amount = Decimal::ZERO,
            "quote-amount" => trade.quote_amount = Decimal::ZERO,
            "taker" => trade.taker = " ".to_string(),
            "timestamp" => trade.trade_time = "invalid".to_string(),
            "range" => request.start_ms = Some(1_789_615_631_000),
            "order" => previous = Some("2026-09-17T03:27:09Z".parse().unwrap()),
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_spot_trade_record(0, &trade, &request, previous),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot trades",
                ..
            })
        ));
    }

    #[test]
    fn spot_trade_page_rejects_duplicate_ids() {
        let envelope: DeepXApiResponse<DeepXSpotTradesPage> =
            serde_json::from_str(SPOT_TRADES).unwrap();
        let mut page = envelope.data;
        page.items[1].id = page.items[0].id;
        let request = DeepXSpotTradesRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
            wallet: None,
            start_ms: None,
            end_ms: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            page_size: Some(2),
        };
        assert!(matches!(
            validate_spot_trades_page(&page, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot trades",
                ..
            })
        ));
    }

    #[rstest]
    fn spot_trade_continuity_rejects_repeated_and_out_of_order_pages() {
        let envelope: DeepXApiResponse<DeepXSpotTradesPage> =
            serde_json::from_str(SPOT_TRADES).unwrap();
        let page = envelope.data;
        let mut repeated = page.clone();
        repeated.items.remove(0);
        let mut continuity = DeepXSpotTradeContinuity::default();
        continuity
            .observe_page(&page, DeepXAccountSortOrder::Descending)
            .unwrap();
        assert!(matches!(
            continuity.observe_page(&repeated, DeepXAccountSortOrder::Descending),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("repeats trade ID"),
        ));

        let mut later = page.clone();
        later.items[0].id += 1;
        later.items[0].trade_time = "2099-01-01T00:00:00Z".to_string();
        let mut continuity = DeepXSpotTradeContinuity::default();
        continuity
            .observe_page(&page, DeepXAccountSortOrder::Descending)
            .unwrap();
        assert!(matches!(
            continuity.observe_page(&later, DeepXAccountSortOrder::Descending),
            Err(DeepXRestError::InvalidHistoryResponse { message, .. })
                if message.contains("requested time order"),
        ));
    }

    #[rstest]
    #[case(3, 3, false, None, false)]
    #[case(3, 4, false, None, true)]
    #[case(3, 3, true, None, true)]
    fn market_cursor_page_checks_identity_and_cursor(
        #[case] expected: u64,
        #[case] received: u64,
        #[case] has_next: bool,
        #[case] cursor: Option<&str>,
        #[case] rejects: bool,
    ) {
        assert_eq!(
            validate_market_cursor_page("perp funding-rate", expected, received, has_next, cursor,)
                .is_err(),
            rejects,
        );
    }

    #[rstest]
    #[case("-0.01", true)]
    #[case("0", false)]
    #[case("0.01", false)]
    fn spot_last_price_validates_sign(#[case] value: &str, #[case] invalid: bool) {
        let price: DeepXSpotLastPrice = serde_json::from_str(value).unwrap();
        assert_eq!(validate_spot_last_price(&price).is_err(), invalid);
    }

    #[rstest]
    #[case("pair")]
    #[case("duplicate")]
    #[case("descending")]
    #[case("before-start")]
    #[case("at-end")]
    #[case("negative-oi")]
    #[case("negative-ratio")]
    fn open_interest_page_rejects_invalid_scope_order_and_values(#[case] mutation: &str) {
        const START: u64 = 1_788_251_280_000;
        let request = DeepXOpenInterestRequest {
            market_id: 3,
            start_ms: START + 1,
            end_ms: Some(START + 180_001),
            limit: Some(2),
        };
        let mut page = DeepXOpenInterestPage {
            pair: "ETH-USDC".to_string(),
            details: [START + 60_000, START + 120_000]
                .into_iter()
                .map(|statistic_time| DeepXOpenInterestRecord {
                    total_oi: Decimal::ONE,
                    long_short_ratio: Decimal::ONE,
                    long_position_count: 1,
                    short_position_count: 1,
                    statistic_time,
                })
                .collect(),
        };
        assert!(validate_open_interest_page(&page, &request).is_ok());
        match mutation {
            "pair" => page.pair.clear(),
            "duplicate" => page.details[1].statistic_time = page.details[0].statistic_time,
            "descending" => page.details.swap(0, 1),
            "before-start" => page.details[0].statistic_time = START - 60_000,
            "at-end" => page.details[1].statistic_time = START + 180_000,
            "negative-oi" => page.details[0].total_oi = Decimal::NEGATIVE_ONE,
            "negative-ratio" => page.details[0].long_short_ratio = Decimal::NEGATIVE_ONE,
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_open_interest_page(&page, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp open-interest",
                ..
            })
        ));
    }

    fn candle_page() -> DeepXPerpCandlesPage {
        serde_json::from_value(serde_json::json!({
            "pair": "ETH-USDC",
            "details": [{"volume": "1.000000000000000001", "high": "3",
                "low": "1", "open": "2", "close": "3", "time": 60000}]
        }))
        .unwrap()
    }

    #[rstest]
    #[case("pair")]
    #[case("limit")]
    #[case("venue-limit")]
    #[case("duplicate")]
    #[case("descending")]
    #[case("range")]
    #[case("open")]
    #[case("close")]
    #[case("volume")]
    fn rejects_invalid_candle_page(#[case] corruption: &str) {
        let mut page = candle_page();
        let mut limit = Some(2);
        match corruption {
            "pair" => page.pair = " ".to_string(),
            "limit" => limit = Some(0),
            "venue-limit" => {
                page.details.resize(5_001, page.details[0].clone());
                limit = None;
            }
            "duplicate" => page.details.push(page.details[0].clone()),
            "descending" => {
                let mut candle = page.details[0].clone();
                candle.time -= 1;
                page.details.push(candle);
            }
            "range" => page.details[0].low = Decimal::from(4),
            "open" => page.details[0].open = Decimal::ZERO,
            "close" => page.details[0].close = Decimal::from(4),
            "volume" => page.details[0].volume = Decimal::from(-1),
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_candle_page("test history", &page, 60_000, Some(240_000), limit),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "test history",
                ..
            })
        ));
    }

    #[rstest]
    fn accepts_empty_sparse_and_exact_candle_pages() {
        let mut page = candle_page();
        let mut candle = page.details[0].clone();
        candle.time += 180_000;
        candle.volume = Decimal::ZERO;
        page.details.push(candle);
        validate_candle_page("test history", &page, 60_000, Some(240_001), Some(2)).unwrap();
        assert!(
            validate_candle_page("test history", &page, 60_000, Some(240_000), Some(2)).is_err()
        );
        assert_eq!(page.details[0].volume.to_string(), "1.000000000000000001");
        page.details.clear();
        validate_candle_page("test history", &page, 60_000, Some(240_000), None).unwrap();
    }

    #[rstest]
    #[case("market")]
    #[case("limit")]
    #[case("order")]
    #[case("price")]
    #[case("volume")]
    #[case("end")]
    fn spot_candles_reject_invalid_response_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXSpotCandlesPage> =
            serde_json::from_str(SPOT_CANDLES).unwrap();
        let mut page = response.data;
        let mut request = DeepXSpotCandlesRequest {
            name: Some("ETH/USDC".to_string()),
            pair: None,
            interval: crate::query::DeepXSpotCandleInterval::OneMinute,
            start_ms: 1_789_616_280_000,
            end_ms: Some(1_789_619_880_000),
            limit: Some(3),
        };
        match mutation {
            "market" => page.pair = "SOL/USDC".to_string(),
            "limit" => request.limit = Some(2),
            "order" => page.details[1].time = page.details[0].time,
            "price" => page.details[0].low = Decimal::ZERO,
            "volume" => page.details[0].volume = Decimal::NEGATIVE_ONE,
            "end" => request.end_ms = Some(page.details.last().unwrap().time),
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_spot_candle_page(&page, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot candles",
                ..
            })
        ));
    }

    #[rstest]
    #[case("market")]
    #[case("level-market")]
    #[case("sequence")]
    #[case("time")]
    #[case("observation")]
    #[case("bid-price")]
    #[case("bid-quantity")]
    #[case("bid-value")]
    #[case("bid-order")]
    #[case("ask-order")]
    fn perp_order_book_rejects_invalid_response_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXPerpOrderBook> =
            serde_json::from_str(PERP_ORDER_BOOK).unwrap();
        let mut book = response.data;
        let request = DeepXPerpOrderBookRequest {
            market_id: 3,
            tick_size: Some(Decimal::new(1, 2)),
        };
        match mutation {
            "market" => book.market_id = 4,
            "level-market" => book.order_buy_list[0].market_id = 4,
            "sequence" => book.last_update_id = 0,
            "time" => book.engine_time = u64::MAX,
            "observation" => book.mid_price = Decimal::NEGATIVE_ONE,
            "bid-price" => book.order_buy_list[0].price = Decimal::ZERO,
            "bid-quantity" => book.order_buy_list[0].qty = Decimal::ZERO,
            "bid-value" => book.order_buy_list[0].value = Decimal::NEGATIVE_ONE,
            "bid-order" => book.order_buy_list[1].price = book.order_buy_list[0].price,
            "ask-order" => book.order_sell_list[1].price = book.order_sell_list[0].price,
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_perp_order_book(&book, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp order book",
                ..
            })
        ));
    }

    #[rstest]
    #[case("pair")]
    #[case("name")]
    #[case("sequence")]
    #[case("time")]
    #[case("observation")]
    #[case("bid-price")]
    #[case("bid-quantity")]
    #[case("bid-value")]
    #[case("bid-order")]
    #[case("ask-order")]
    #[case("crossed")]
    fn spot_order_book_rejects_invalid_response_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXSpotOrderBook> =
            serde_json::from_str(SPOT_ORDER_BOOK).unwrap();
        let mut book = response.data;
        let mut request = DeepXSpotOrderBookRequest {
            name: None,
            pair: Some(
                "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37".to_string(),
            ),
            tick_size: Some(Decimal::new(1, 2)),
        };
        match mutation {
            "pair" => book.pair = format!("0x{}", "11".repeat(32)),
            "name" => {
                request.name = Some("SOL/USDC".to_string());
                request.pair = None;
            }
            "sequence" => book.last_update_id = 0,
            "time" => book.engine_time = u64::MAX,
            "observation" => book.latest_price = Decimal::NEGATIVE_ONE,
            "bid-price" => book.order_buy_list[0].price = Decimal::ZERO,
            "bid-quantity" => book.order_buy_list[0].qty = Decimal::ZERO,
            "bid-value" => book.order_buy_list[0].value = Decimal::NEGATIVE_ONE,
            "bid-order" => book.order_buy_list[1].price = book.order_buy_list[0].price,
            "ask-order" => book.order_sell_list[1].price = book.order_sell_list[0].price,
            "crossed" => book.order_buy_list[0].price = book.order_sell_list[0].price,
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_spot_order_book(&book, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot order book",
                ..
            })
        ));
    }

    fn lending_history_request() -> DeepXLendingHistoryRequest {
        DeepXLendingHistoryRequest {
            market_id: Some(1),
            asset: Some("usdc".to_string()),
            interval: crate::query::DeepXPerpCandleInterval::OneHour,
            start_ms: 1_789_430_000_000,
            end_ms: None,
            limit: Some(3),
            sort: DeepXAccountSortOrder::Descending,
        }
    }

    #[rstest]
    #[case("filter")]
    #[case("apr")]
    #[case("order")]
    #[case("timestamp")]
    #[case("duplicate")]
    #[case("limit")]
    fn lending_rate_history_rejects_invalid_responses(#[case] mutation: &str) {
        let mut records =
            serde_json::from_str::<DeepXApiResponse<DeepXLendingInterestRateHistory>>(
                LENDING_RATE_HISTORY,
            )
            .unwrap()
            .data
            .details;
        let mut request = lending_history_request();
        validate_lending_interest_rate_history(&records, &request).unwrap();
        match mutation {
            "filter" => records[0].asset = "eth".to_string(),
            "apr" => records[0].supply_apr = Decimal::NEGATIVE_ONE,
            "order" => records.swap(0, 2),
            "timestamp" => records[0].statistic_time = request.start_ms - 1,
            "duplicate" => records[1] = records[0].clone(),
            "limit" => request.limit = Some(2),
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_lending_interest_rate_history(&records, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "lending interest-rate history",
                ..
            })
        ));
    }

    #[rstest]
    #[case("price")]
    #[case("supplied")]
    #[case("borrowed")]
    #[case("utilization")]
    #[case("apr")]
    #[case("filter")]
    #[case("order")]
    #[case("timestamp")]
    #[case("duplicate")]
    #[case("limit")]
    fn lending_status_history_rejects_invalid_responses(#[case] mutation: &str) {
        let mut records = serde_json::from_str::<DeepXApiResponse<DeepXLendingStatusHistory>>(
            LENDING_STATUS_HISTORY,
        )
        .unwrap()
        .data
        .details;
        let mut request = lending_history_request();
        validate_lending_status_history(&records, &request).unwrap();
        match mutation {
            "price" => records[0].index_price = Decimal::ZERO,
            "supplied" => records[0].total_supplied = Decimal::NEGATIVE_ONE,
            "borrowed" => records[0].total_borrowed = Decimal::NEGATIVE_ONE,
            "utilization" => records[0].utilization_rate = Decimal::new(101, 2),
            "apr" => records[0].borrow_apr = Decimal::NEGATIVE_ONE,
            "filter" => records[0].market_id = 2,
            "order" => records.swap(0, 2),
            "timestamp" => records[0].statistic_time = request.start_ms - 1,
            "duplicate" => records[1] = records[0].clone(),
            "limit" => request.limit = Some(2),
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_lending_status_history(&records, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "lending status history",
                ..
            })
        ));
    }

    #[rstest]
    #[case("identity")]
    #[case("height")]
    #[case("timestamp")]
    #[case("duplicate")]
    fn lending_assets_reject_invalid_responses(#[case] mutation: &str) {
        let mut assets: Vec<DeepXLendingAsset> =
            serde_json::from_str::<DeepXApiResponse<_>>(LENDING_ASSETS)
                .unwrap()
                .data;
        validate_lending_assets(&assets).unwrap();
        match mutation {
            "identity" => assets[0].asset = " ".to_string(),
            "height" => assets[0].height = 0,
            "timestamp" => assets[0].created_at = "invalid".to_string(),
            "duplicate" => assets[1] = assets[0].clone(),
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_lending_assets(&assets),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "lending assets",
                ..
            })
        ));
    }

    #[rstest]
    #[case("filter")]
    #[case("pair")]
    #[case("gap")]
    #[case("utilization")]
    #[case("rate")]
    #[case("rho")]
    #[case("duplicate")]
    fn lending_curves_reject_invalid_responses(#[case] mutation: &str) {
        let mut curves: Vec<DeepXLendingInterestRateParams> =
            serde_json::from_str::<DeepXApiResponse<_>>(LENDING_CURVES)
                .unwrap()
                .data;
        let request = DeepXLendingMarketRequest {
            market_id: Some(1),
            asset: Some("usdc".to_string()),
        };
        validate_lending_interest_rate_params(&curves, &request).unwrap();
        match mutation {
            "filter" => curves[0].market_id = 2,
            "pair" => curves[0].r4 = Some(Decimal::ONE),
            "gap" => {
                curves[0].u5 = Some(Decimal::new(995, 3));
                curves[0].r5 = Some(Decimal::ONE);
            }
            "utilization" => curves[0].u2 = curves[0].u1,
            "rate" => curves[0].r2 = Decimal::NEGATIVE_ONE,
            "rho" => curves[0].rho = Decimal::NEGATIVE_ONE,
            "duplicate" => curves.push(curves[0].clone()),
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_lending_interest_rate_params(&curves, &request),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "lending interest-rate parameters",
                ..
            })
        ));
    }

    #[rstest]
    fn lending_curves_accept_nonmonotonic_rates_supported_by_chain() {
        let mut curves: Vec<DeepXLendingInterestRateParams> =
            serde_json::from_str::<DeepXApiResponse<_>>(LENDING_CURVES)
                .unwrap()
                .data;
        curves[0].r2 = Decimal::new(1, 2);
        curves[0].r_max = Decimal::new(5, 3);
        let request = DeepXLendingMarketRequest {
            market_id: Some(1),
            asset: Some("usdc".to_string()),
        };
        validate_lending_interest_rate_params(&curves, &request).unwrap();
    }

    #[rstest]
    #[case("id")]
    #[case("name")]
    #[case("address")]
    #[case("deployer")]
    #[case("price")]
    #[case("margin")]
    #[case("quantity")]
    #[case("clamp")]
    #[case("liquidation")]
    #[case("timestamp")]
    fn perp_market_lookup_rejects_invalid_metadata(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXPerpMarketLookup> =
            serde_json::from_str(PERP_MARKET).unwrap();
        let mut market = response.data;
        validate_perp_market_lookup("perp market", &market).unwrap();
        match mutation {
            "id" => market.id = 0,
            "name" => market.name = "ETH/USDC".to_string(),
            "address" => market.base_address = "invalid".to_string(),
            "deployer" => {
                market.deployer_delegate =
                    Some("0x123ae070eb84068b5fed9f5b99f236507c44c880".to_string());
            }
            "price" => market.mark_price = Decimal::ZERO,
            "margin" => market.maintenance_margin_ratio = Decimal::ONE,
            "quantity" => market.order_spec_min_qty = Decimal::ZERO,
            "clamp" => {
                market.funding_rate_clamp_lower_bound = Decimal::ONE;
                market.funding_rate_clamp_upper_bound = Decimal::ZERO;
            }
            "liquidation" => market.liquidity_bucket_slippage_step = 100_001,
            "timestamp" => market.last_calc_funding_rate_time = u64::MAX,
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_perp_market_lookup("perp market", &market),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "perp market",
                ..
            })
        ));
    }

    #[rstest]
    #[case("name")]
    #[case("symbol")]
    #[case("pair")]
    #[case("address")]
    #[case("same-address")]
    #[case("same-symbol")]
    #[case("price")]
    #[case("tick")]
    #[case("deviation")]
    #[case("guard")]
    fn spot_market_rejects_invalid_metadata(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXSpotMarket> =
            serde_json::from_str(SPOT_MARKET).unwrap();
        let mut market = response.data;
        validate_spot_market("spot market", &market).unwrap();
        match mutation {
            "name" => market.name = "ETH-USDC".to_string(),
            "symbol" => market.base_symbol = "btc".to_string(),
            "pair" => market.pair = "0x1234".to_string(),
            "address" => market.base_address = "invalid".to_string(),
            "same-address" => market.base_address = market.quote_address.clone(),
            "same-symbol" => market.base_symbol = market.quote_symbol.clone(),
            "price" => market.price = Decimal::NEGATIVE_ONE,
            "tick" => market.tick_size = Decimal::ZERO,
            "deviation" => market.max_deviation_bps = Decimal::NEGATIVE_ONE,
            "guard" => market.limit_order_guard_limit_long = Decimal::ZERO,
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_spot_market("spot market", &market),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot market",
                ..
            })
        ));
    }

    #[rstest]
    #[case("name")]
    #[case("pair")]
    #[case("pair-encoding")]
    fn spot_market_directory_rejects_duplicate_identities(#[case] identity: &str) {
        let response: DeepXApiResponse<Vec<DeepXSpotMarket>> =
            serde_json::from_str(SPOT_MARKETS).unwrap();
        let mut markets = response.data;
        validate_spot_markets(&markets).unwrap();
        match identity {
            "name" => {
                markets[1].name = markets[0].name.clone();
                markets[1].base_symbol = markets[0].base_symbol.clone();
            }
            "pair" => markets[1].pair = markets[0].pair.clone(),
            "pair-encoding" => {
                markets[1].pair = markets[0]
                    .pair
                    .strip_prefix("0x")
                    .unwrap()
                    .to_ascii_uppercase();
            }
            _ => unreachable!(),
        }
        for market in &markets {
            validate_spot_market("spot markets", market).unwrap();
        }
        assert!(matches!(
            validate_spot_markets(&markets),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot markets",
                ..
            })
        ));
    }

    #[rstest]
    #[case("0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37")]
    #[case("9068D4AC891A14784C17877EB74BD8489B3367C71D72766DBFA4DFBFB662FA37")]
    fn spot_pair_lookup_normalizes_hex_identity(#[case] pair: &str) {
        assert_eq!(
            normalize_spot_pair_identity(pair).unwrap(),
            "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"
        );
        assert!(matches!(
            normalize_spot_pair_identity("0x1234"),
            Err(DeepXRestError::InvalidRequest(_)),
        ));
    }

    #[rstest]
    #[case("volume")]
    #[case("order")]
    #[case("range")]
    fn spot_volume_rejects_invalid_response_semantics(#[case] mutation: &str) {
        let response: DeepXApiResponse<DeepXSpotVolume> =
            serde_json::from_str(SPOT_VOLUME).unwrap();
        let mut volume = response.data;
        validate_spot_volume(&volume).unwrap();
        match mutation {
            "volume" => volume.total_volume = rust_decimal::Decimal::NEGATIVE_ONE,
            "order" => volume.start_time = volume.end_time + 1,
            "range" => volume.statistic_time = u64::MAX,
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_spot_volume(&volume),
            Err(DeepXRestError::InvalidHistoryResponse {
                endpoint: "spot volume",
                ..
            })
        ));
    }
}
