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

//! Raw market directory and lending REST queries.

use serde::Serialize;
use serde_json::value::RawValue;

use super::{DeepXRestError, DeepXRestTransport};
use crate::query::{
    DeepXFundingRateRequest, DeepXLendingHistoryRequest, DeepXLendingMarketRequest,
    DeepXLongShortRatioRequest, DeepXOpenInterestRequest, DeepXPerpCandlesRequest,
    DeepXPerpLastPriceRequest, DeepXPerpMarkPriceRequest, DeepXPerpOraclePriceRequest,
    DeepXPerpOrderBookRequest, DeepXPerpTradesQuery, DeepXPerpVolumeRequest,
    DeepXSpotCandlesRequest, DeepXSpotLastPriceRequest, DeepXSpotOrderBookRequest,
    DeepXSpotTradesRequest, DeepXSpotVolumeRequest,
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
