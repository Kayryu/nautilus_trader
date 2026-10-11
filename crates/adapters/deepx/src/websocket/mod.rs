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

//! Transport-neutral WebSocket protocol state for DeepX testnet.

pub mod account;
pub mod account_metrics;
pub mod account_order_history;
pub mod account_orders;
pub mod account_positions;
pub mod account_spot_orders;
pub mod account_spot_trades;
pub mod account_trades;
pub mod book;
pub mod candles;
pub mod error;
pub mod frame;
pub mod handler;
pub mod lending_rates;
pub mod open_interest;
pub mod orderbook_rpc;
pub mod protocol;
pub mod public;
pub(crate) mod spot_trades;
pub mod task;
pub mod trades;
pub mod transport;

pub use account::{
    DeepXWsAccountConnection, DeepXWsAccountFrame, DeepXWsAllMarket,
    DeepXWsConfirmedAccountSubscription, DeepXWsConfirmedBalancesFrame, DeepXWsUserBalancesRequest,
    DeepXWsUserBalancesSubscription,
};
pub use account_metrics::{
    DeepXWsAccountMetric, DeepXWsAccountMetricChannel, DeepXWsAccountMetricsRequest,
    DeepXWsExactPortfolioMargin, observe_initial_account_metrics,
};
pub use account_order_history::{
    DeepXWsPerpOrderHistoryRequest, DeepXWsSpotOrderHistoryRequest, observe_perp_order_history_ack,
    observe_spot_order_history_ack,
};
pub use account_orders::{
    DeepXWsPerpOpenOrdersFrame, DeepXWsPerpOpenOrdersPage, DeepXWsPerpOpenOrdersRequest,
    observe_initial_perp_open_orders_page, observe_initial_perp_wallet_open_orders_pages,
};
pub use account_positions::{
    DeepXWsPerpOpenPositionsFrame, DeepXWsPerpOpenPositionsPage, DeepXWsPerpOpenPositionsRequest,
    observe_initial_perp_open_positions_page, observe_initial_perp_wallet_open_positions_pages,
};
pub use account_spot_orders::{
    DeepXWsSpotOpenOrdersFrame, DeepXWsSpotOpenOrdersPage, DeepXWsSpotOpenOrdersRequest,
    observe_initial_spot_open_orders_page, observe_initial_spot_wallet_open_orders_pages,
};
pub use account_spot_trades::{
    DeepXWsSpotFinalizedTradeScope, DeepXWsSpotTradeHistoryBatch, DeepXWsSpotTradeHistoryFrame,
    DeepXWsSpotTradeHistoryItem, DeepXWsSpotTradeHistoryMatchError, DeepXWsSpotTradeHistoryRequest,
    match_spot_trade_history_batch_to_finalized, match_spot_trade_history_batch_to_rest,
    observe_spot_trade_history_batches,
};
pub use account_trades::{
    DeepXWsPerpFinalizedTradeScope, DeepXWsPerpTradeHistoryBatch, DeepXWsPerpTradeHistoryFrame,
    DeepXWsPerpTradeHistoryItem, DeepXWsPerpTradeHistoryMatchError, DeepXWsPerpTradeHistoryRequest,
    match_perp_trade_history_batch_to_finalized, match_perp_trade_history_batch_to_rest,
    observe_perp_trade_history_ack, observe_perp_trade_history_batches,
};
pub use candles::{
    DeepXWsCandleStream, DeepXWsSupersededCandle, observe_first_superseded_perp_candle,
};
pub use error::DeepXWsError;
pub use frame::DeepXWsFrame;
pub use handler::{
    DeepXWsProtocolHandle, DeepXWsProtocolHandler, DeepXWsRegisteredAuthentication,
    DeepXWsRegisteredRequest, deepx_ws_protocol_handler,
};
pub use lending_rates::{
    DeepXWsLendingRate, DeepXWsLendingRatesBatch, DeepXWsLendingRatesRequest,
    match_lending_rate_batch_to_rest, observe_first_lending_rates,
};
pub use open_interest::{
    DeepXWsOpenInterestObservation, match_open_interest_to_rest, observe_first_open_interest,
};
pub use protocol::{
    DeepXWsAuthenticatedFrame, DeepXWsAuthenticatedSession, DeepXWsAuthenticationAttempt,
    DeepXWsProtocolCore, DeepXWsRequest, DeepXWsRequestId,
};
pub use task::{DeepXWsTaskHandles, DeepXWsTaskOutcome};
