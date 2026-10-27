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

//! DeepX WebSocket wire-level frames and request identifiers.

pub mod account;
pub mod account_metrics;
pub mod account_order_history;
pub mod account_orders;
pub mod account_positions;
pub mod account_spot_orders;
pub mod account_spot_trade_history;
pub mod account_trade_history;
pub mod book;
pub mod candles;
pub mod error;
pub mod frame;
pub mod lending_rates;
pub mod open_interest;
pub mod orderbook_rpc;
pub mod public;
pub mod spot_trades;
pub mod task;
pub mod trades;

pub use error::DeepXWsError;
pub use frame::DeepXWsFrame;

/// Numeric correlator carried by one DeepX WebSocket request and response.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DeepXWsRequestId(u64);

impl DeepXWsRequestId {
    /// Creates an ID decoded from a WebSocket response frame.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wire-level numeric request ID.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}
