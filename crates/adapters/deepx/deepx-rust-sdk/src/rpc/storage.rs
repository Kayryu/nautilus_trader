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

//! SCALE values stored with DeepX orders and lending pools.

use parity_scale_codec::{Decode, Encode};

/// Runtime status of one DeepX V2 lending pool.
#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXLendingPoolStatus {
    /// New lending activity is paused.
    Paused,
    /// The pool is undergoing a storage migration and is not readable as V2 state.
    Migrating,
    /// The pool is active.
    Active,
    /// The pool is frozen.
    Frozen,
}

/// Time-in-force encoded in finalized order storage.
#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXStoredTimeInForce {
    /// Good until canceled.
    Gtc,
    /// Immediate or cancel.
    Ioc,
    /// Fill or kill.
    Fok,
}

/// Order type encoded in finalized perpetual-order storage.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXStoredPerpOrderType {
    /// Limit order with its exact runtime time-in-force value.
    Limit(DeepXStoredTimeInForce),
    /// Market order with an optional raw slippage limit.
    Market(Option<u64>),
    /// System stop order.
    Stop,
}

/// Lifecycle state encoded in finalized order storage.
#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXStoredOrderStatus {
    /// Active on the order book.
    Open,
    /// Partially filled with a remainder still active.
    PartiallyFilled,
    /// Fully filled.
    Filled,
    /// Canceled by a user or the protocol.
    Canceled,
    /// Reverted or failed on-chain.
    Rejected,
    /// Canceled according to order-type rules or by the exchange.
    Expired,
}

impl DeepXStoredOrderStatus {
    /// Returns whether the order can remain in the active-order index.
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Open | Self::PartiallyFilled)
    }
}

/// Post-only behavior encoded in finalized order storage.
#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXStoredPostOnlyParam {
    /// No post-only constraint.
    None,
    /// The order must rest without immediately matching.
    MustPostOnly,
    /// The runtime may adapt the price to post the order.
    Adaptive,
}

/// Side encoded in the active perpetual-order index.
#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXStoredOrderSide {
    #[codec(index = 1)]
    Buy,
    #[codec(index = 0)]
    Sell,
}

/// Spot order type stored by the runtime.
#[derive(Clone, Copy, Debug, Decode, Encode, PartialEq, Eq)]
pub enum DeepXStoredSpotOrderType {
    Limit(DeepXStoredTimeInForce),
    Market(Option<u64>),
    Stop,
}

/// SCALE layout of one DeepX V2 lending pool.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub struct DeepXLendingPoolStorage {
    pub market_id: u8,
    pub asset: Vec<u8>,
    pub decimal: u32,
    pub cash_balance: u128,
    pub locked_liquidity: u128,
    pub available_liquidity: u128,
    pub total_scaled_deposits: u128,
    pub total_scaled_borrows: u128,
    pub total_effective_deposits: u128,
    pub total_effective_borrows: u128,
    pub deposit_index: u128,
    pub borrow_index: u128,
    pub protocol_reserve: u128,
    pub bad_debt: u128,
    pub accounting_dust: i128,
    pub internal_transfer_dust: i128,
    pub status: DeepXLendingPoolStatus,
    pub apr_borrow: u128,
    pub apr_lend: u128,
}

/// SCALE layout of one perpetual position.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub struct DeepXPerpPositionStorage {
    pub market_id: u16,
    pub is_long: bool,
    pub base_asset_amount: u128,
    pub entry_price: u128,
    pub leverage: u64,
    pub last_funding_rate: i128,
    pub version: u64,
    pub realized_pnl: i128,
    pub funding_payment: i128,
    pub owner: [u8; 20],
    pub take_profit: Option<u128>,
    pub stop_loss: Option<u128>,
    pub last_settle_price: u128,
}

/// SCALE layout of one active perpetual order index entry.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub struct DeepXActivePerpOrderIndexEntry {
    pub address: [u8; 20],
    pub market_id: u16,
    pub order_side: DeepXStoredOrderSide,
    pub order_type: DeepXStoredPerpOrderType,
    pub order_id: u64,
    pub price: u128,
    pub created_at: u64,
}

/// SCALE layout of one perpetual order.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub struct DeepXPerpOrderStorage {
    pub order_id: u64,
    pub owner: [u8; 20],
    pub market_id: u16,
    pub is_long: bool,
    pub size: u128,
    pub price: u128,
    pub order_type: DeepXStoredPerpOrderType,
    pub create_time: u64,
    pub leverage: u64,
    pub status: DeepXStoredOrderStatus,
    pub size_filled: u128,
    pub size_remain: u128,
    pub take_profit: Option<u128>,
    pub stop_loss: Option<u128>,
    pub reduce_only: bool,
    pub post_only: DeepXStoredPostOnlyParam,
}

/// SCALE layout of one Spot order.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub struct DeepXSpotOrderStorage {
    pub order_id: u64,
    pub owner: [u8; 20],
    pub pair: [u8; 32],
    pub price: Option<[u8; 32]>,
    pub quote_amount: [u8; 32],
    pub base_amount: [u8; 32],
    pub create_time: u64,
    pub status: DeepXStoredOrderStatus,
    pub order_type: DeepXStoredSpotOrderType,
    pub post_only: DeepXStoredPostOnlyParam,
    pub reduce_only: bool,
    pub is_buy: bool,
}

#[cfg(test)]
mod tests {
    use parity_scale_codec::{Decode, Encode};
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn stored_order_discriminants_match_runtime() {
        assert_eq!(DeepXStoredOrderSide::Buy.encode(), [1]);
        assert_eq!(DeepXStoredOrderSide::Sell.encode(), [0]);
        assert_eq!(DeepXStoredTimeInForce::Gtc.encode(), [0]);
        assert_eq!(DeepXStoredTimeInForce::Ioc.encode(), [1]);
        assert_eq!(DeepXStoredTimeInForce::Fok.encode(), [2]);
        assert_eq!(DeepXStoredOrderStatus::Open.encode(), [0]);
        assert_eq!(DeepXStoredOrderStatus::PartiallyFilled.encode(), [1]);
        assert_eq!(DeepXStoredOrderStatus::Filled.encode(), [2]);
        assert_eq!(DeepXStoredOrderStatus::Canceled.encode(), [3]);
        assert_eq!(DeepXStoredOrderStatus::Rejected.encode(), [4]);
        assert_eq!(DeepXStoredOrderStatus::Expired.encode(), [5]);
        assert_eq!(DeepXStoredPostOnlyParam::None.encode(), [0]);
        assert_eq!(DeepXStoredPostOnlyParam::MustPostOnly.encode(), [1]);
        assert_eq!(DeepXStoredPostOnlyParam::Adaptive.encode(), [2]);
        assert_eq!(DeepXStoredPerpOrderType::Stop.encode(), [2]);
        assert_eq!(DeepXStoredSpotOrderType::Stop.encode(), [2]);

        let mut encoded = &[1_u8][..];
        assert_eq!(
            DeepXStoredOrderSide::decode(&mut encoded).unwrap(),
            DeepXStoredOrderSide::Buy
        );
        assert!(encoded.is_empty());
    }

    #[rstest]
    fn lending_pool_status_discriminants_match_runtime() {
        assert_eq!(DeepXLendingPoolStatus::Paused.encode(), [0]);
        assert_eq!(DeepXLendingPoolStatus::Migrating.encode(), [1]);
        assert_eq!(DeepXLendingPoolStatus::Active.encode(), [2]);
        assert_eq!(DeepXLendingPoolStatus::Frozen.encode(), [3]);
    }
}
