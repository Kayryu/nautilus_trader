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

//! DeepX finalized runtime event data.

use crate::rest::models::DeepXDelegateMode;

/// Wallet delegate account operation reported by the runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeepXDelegateAccountChangeKind {
    /// A delegate was created or renewed; the event does not distinguish the two.
    Set,
    /// A delegate was removed.
    Removed,
}

/// One delegate account change decoded from complete finalized runtime event storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXFinalizedDelegateAccountChange {
    /// Finalized block identity supplied with the complete event storage value.
    pub block_hash: [u8; 32],
    pub block_number: u64,
    /// Zero-based position in the complete event sequence.
    pub event_index: u32,
    /// Extrinsic index attached to the runtime event.
    pub extrinsic_index: u32,
    /// Wallet owner and delegate identities emitted by the runtime.
    pub owner: [u8; 20],
    pub delegate: [u8; 20],
    /// Runtime account operation.
    pub kind: DeepXDelegateAccountChangeKind,
}

/// One wallet delegate mode change decoded from complete finalized runtime event storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXFinalizedDelegateModeChange {
    /// Finalized block identity supplied with the complete event storage value.
    pub block_hash: [u8; 32],
    pub block_number: u64,
    /// Zero-based position in the complete event sequence.
    pub event_index: u32,
    /// Extrinsic index attached to the runtime event.
    pub extrinsic_index: u32,
    /// Wallet owner and delegate identities emitted by the runtime.
    pub owner: [u8; 20],
    pub delegate: [u8; 20],
    /// Runtime mode after the update.
    pub mode: DeepXDelegateMode,
}

/// One perpetual execution decoded from complete finalized runtime event storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXFinalizedPerpTrade {
    /// Finalized block hash supplied with the complete event storage value.
    pub block_hash: [u8; 32],
    /// Finalized block number supplied with the complete event storage value.
    pub block_number: u64,
    /// Zero-based position in the complete `System.Events` value.
    pub event_index: u32,
    /// Runtime perpetual market ID.
    pub market_id: u16,
    /// Buyer order ID.
    pub buyer_order_id: u64,
    /// Buyer account.
    pub buyer: [u8; 20],
    /// Seller order ID.
    pub seller_order_id: u64,
    /// Seller account.
    pub seller: [u8; 20],
    /// Raw runtime price before market-specific scale conversion.
    pub price: u128,
    /// Raw runtime size before market-specific scale conversion.
    pub size: u128,
    /// Raw buyer leverage.
    pub buyer_leverage: u64,
    /// Raw seller leverage.
    pub seller_leverage: u64,
    /// Runtime filled-direction variant.
    pub filled_direction: String,
    /// Runtime taker variant.
    pub taker: String,
    /// Raw signed taker fee in the runtime quote asset's atomic units.
    pub taker_fee: i128,
    /// Raw signed maker fee in the runtime quote asset's atomic units.
    pub maker_fee: i128,
    /// Runtime fee asset bytes; both event fee assets must agree.
    pub fee_asset: Vec<u8>,
}

/// One Spot execution decoded from complete finalized runtime event storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXFinalizedSpotTrade {
    /// Finalized block hash supplied with complete event storage.
    pub block_hash: [u8; 32],
    /// Finalized block number supplied with complete event storage.
    pub block_number: u64,
    /// Zero-based index in complete `System.Events`.
    pub event_index: u32,
    /// Deployment-provided Spot pair identity.
    pub pair: [u8; 32],
    /// Buyer account and order ID.
    pub buyer: [u8; 20],
    pub buy_id: u64,
    /// Seller account and order ID.
    pub seller: [u8; 20],
    pub sell_id: u64,
    /// Gross quote and base quantities in raw atomic units.
    pub trade_amount_u: [u8; 32],
    pub trade_amount_b: [u8; 32],
    /// Buyer or seller taker role.
    pub taker: String,
    /// Signed taker and maker balance fees in their respective received assets.
    pub taker_fee: i128,
    pub taker_fee_asset: Vec<u8>,
    pub maker_fee: i128,
    pub maker_fee_asset: Vec<u8>,
    /// Runtime price in six-decimal quote units.
    pub price: [u8; 32],
}
