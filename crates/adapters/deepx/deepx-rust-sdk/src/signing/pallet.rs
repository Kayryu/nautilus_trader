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
//  See the GNU Lesser General Public License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! DeepX runtime order arguments and their metadata-driven encoding.

use serde::{Deserialize, Serialize};
use subxt_core::dynamic::Value;

/// Runtime time-in-force variants accepted by a DeepX perpetual limit order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeepXTimeInForce {
    /// Good until canceled.
    Gtc,
    /// Immediate or cancel.
    Ioc,
    /// Fill or kill.
    Fok,
}

impl DeepXTimeInForce {
    fn into_dynamic_value(self) -> Value {
        let variant = match self {
            Self::Gtc => "GTC",
            Self::Ioc => "IOC",
            Self::Fok => "FOK",
        };
        Value::unnamed_variant(variant, Vec::<Value>::new())
    }
}

/// Runtime order-type variants accepted by a DeepX perpetual order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeepXPerpOrderType {
    /// Limit order with an explicit time-in-force policy.
    Limit(DeepXTimeInForce),
    /// Market order with optional exact runtime slippage basis points.
    Market(Option<u64>),
    /// System stop order. This type only proves encoding and does not grant placement authority.
    Stop,
}

impl DeepXPerpOrderType {
    fn into_dynamic_value(self) -> Value {
        match self {
            Self::Limit(time_in_force) => {
                Value::unnamed_variant("Limit", [time_in_force.into_dynamic_value()])
            }
            Self::Market(slippage) => Value::unnamed_variant(
                "Market",
                [match slippage {
                    Some(value) => Value::unnamed_variant("Some", [Value::u128(u128::from(value))]),
                    None => Value::unnamed_variant("None", Vec::<Value>::new()),
                }],
            ),
            Self::Stop => Value::unnamed_variant("Stop", Vec::<Value>::new()),
        }
    }
}

/// Runtime post-only behavior accepted by a DeepX perpetual order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeepXPostOnlyParam {
    /// No post-only restriction.
    None,
    /// Reject the order unless it posts to the book.
    MustPostOnly,
    /// Allow the runtime to adjust the price so the order posts.
    Adaptive,
}

impl DeepXPostOnlyParam {
    fn into_dynamic_value(self) -> Value {
        let variant = match self {
            Self::None => "None",
            Self::MustPostOnly => "MustPostOnly",
            Self::Adaptive => "Adaptive",
        };
        Value::unnamed_variant(variant, Vec::<Value>::new())
    }
}

/// Exact raw runtime arguments for a DeepX perpetual order placement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpPlaceParams {
    /// DeepX subaccount identity.
    pub subaccount: [u8; 20],
    /// Runtime perpetual market identifier.
    pub market_id: u16,
    /// Whether the order opens or extends a long position.
    pub is_long: bool,
    /// Exact runtime size integer, without inferred financial scaling.
    pub size: u128,
    /// Exact runtime price integer, without inferred financial scaling.
    pub price: u128,
    /// Runtime order type and its exact associated data.
    pub order_type: DeepXPerpOrderType,
    /// Optional exact runtime take-profit integer.
    pub take_profit: Option<u128>,
    /// Optional exact runtime stop-loss integer.
    pub stop_loss: Option<u128>,
    /// Whether the order may only reduce an existing position.
    pub reduce_only: bool,
    /// Runtime post-only behavior.
    pub post_only: DeepXPostOnlyParam,
}

impl DeepXPerpPlaceParams {
    /// Returns the encoded DeepX perpetual placement parameter value.
    #[must_use]
    pub fn into_dynamic_value(self) -> Value {
        let optional_u128 = |value| match value {
            Some(value) => Value::unnamed_variant("Some", [Value::u128(value)]),
            None => Value::unnamed_variant("None", Vec::<Value>::new()),
        };
        Value::named_composite([
            ("subaccount", Value::from_bytes(self.subaccount)),
            ("market_id", Value::u128(u128::from(self.market_id))),
            ("is_long", Value::bool(self.is_long)),
            ("size", Value::u128(self.size)),
            ("price", Value::u128(self.price)),
            ("order_type", self.order_type.into_dynamic_value()),
            ("take_profit", optional_u128(self.take_profit)),
            ("stop_loss", optional_u128(self.stop_loss)),
            ("reduce_only", Value::bool(self.reduce_only)),
            ("post_only", self.post_only.into_dynamic_value()),
            (
                "stp_mode",
                Value::unnamed_variant("None", Vec::<Value>::new()),
            ),
        ])
    }

    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::named_composite([(
            "params",
            self.into_dynamic_value(),
        )])]
    }
}

/// Runtime order types accepted by a DeepX Spot order.
///
/// Spot and perpetual placement use the same runtime `OrderType` definition in the captured
/// metadata.
pub type DeepXSpotOrderType = DeepXPerpOrderType;

fn u256_from_le_bytes(value: [u8; 32]) -> Value {
    let limbs = std::array::from_fn::<_, 4, _>(|index| {
        let offset = index * 8;
        Value::u128(u128::from(u64::from_le_bytes(
            value[offset..offset + 8]
                .try_into()
                .expect("fixed U256 limb"),
        )))
    });
    Value::unnamed_composite([Value::unnamed_composite(limbs)])
}

/// Exact raw runtime arguments for a DeepX Spot order placement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSpotPlaceParams {
    /// DeepX subaccount identity.
    pub subaccount: [u8; 20],
    /// Deployment-provided bytes32 Spot pair identifier.
    pub pair: [u8; 32],
    /// Whether the order buys the base asset.
    pub is_buy: bool,
    /// Exact little-endian runtime `U256` quote amount.
    pub quote_amount: [u8; 32],
    /// Exact little-endian runtime `U256` base amount.
    pub base_amount: [u8; 32],
    /// Runtime order type and its exact associated data.
    pub order_type: DeepXSpotOrderType,
    /// Runtime post-only behavior.
    pub post_only: DeepXPostOnlyParam,
    /// Whether the order may only reduce an existing balance or liability.
    pub reduce_only: bool,
}

impl DeepXSpotPlaceParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::named_composite([
            ("subaccount", Value::from_bytes(self.subaccount)),
            ("pair", Value::from_bytes(self.pair)),
            ("is_buy", Value::bool(self.is_buy)),
            ("quote_amount", u256_from_le_bytes(self.quote_amount)),
            ("base_amount", u256_from_le_bytes(self.base_amount)),
            ("order_type", self.order_type.into_dynamic_value()),
            ("post_only", self.post_only.into_dynamic_value()),
            ("reduce_only", Value::bool(self.reduce_only)),
            (
                "stp_mode",
                Value::unnamed_variant("None", Vec::<Value>::new()),
            ),
        ])]
    }
}

/// Exact raw runtime arguments for a DeepX perpetual position close.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpCloseParams {
    /// DeepX subaccount identity.
    pub subaccount: [u8; 20],
    /// Runtime perpetual market identifier.
    pub market_id: u16,
    /// Exact runtime price integer, without inferred financial scaling.
    pub price: u128,
    /// Optional exact runtime slippage integer, without inferred units.
    pub slippage: Option<u64>,
}

impl DeepXPerpCloseParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.subaccount),
            Value::u128(u128::from(self.market_id)),
            Value::u128(self.price),
            match self.slippage {
                Some(value) => Value::unnamed_variant("Some", [Value::u128(u128::from(value))]),
                None => Value::unnamed_variant("None", Vec::<Value>::new()),
            },
        ]
    }
}

/// Exact raw runtime arguments for perpetual profit and loss point management.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpProfitAndLossPointParams {
    /// DeepX subaccount identity.
    pub subaccount: [u8; 20],
    /// Runtime perpetual market identifier.
    pub market_id: u16,
    /// Exact runtime integer, without inferred scaling or trigger semantics.
    pub take_profit_point: u128,
    /// Exact runtime integer, without inferred scaling or trigger semantics.
    pub stop_loss_point: u128,
}

impl DeepXPerpProfitAndLossPointParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.subaccount),
            Value::u128(u128::from(self.market_id)),
            Value::u128(self.take_profit_point),
            Value::u128(self.stop_loss_point),
        ]
    }
}

/// Exact arguments for a user-requested DeepX perpetual order cancellation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpCancelParams {
    /// DeepX subaccount which owns the order.
    pub subaccount: [u8; 20],
    /// Runtime-assigned order identifier.
    pub order_id: u64,
    /// Perpetual market identifier.
    pub market_id: u16,
    /// Whether the runtime should use its high-priority cancellation path.
    pub fast_cancel: bool,
}

impl DeepXPerpCancelParams {
    fn into_dynamic_value(self) -> Value {
        Value::named_composite([
            ("subaccount", Value::from_bytes(self.subaccount)),
            ("order_id", Value::u128(u128::from(self.order_id))),
            ("market_id", Value::u128(u128::from(self.market_id))),
            (
                "cancel_reason",
                Value::unnamed_variant("UserCanceled", Vec::<Value>::new()),
            ),
            ("fast_cancel", Value::bool(self.fast_cancel)),
        ])
    }

    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::named_composite([(
            "params",
            self.into_dynamic_value(),
        )])]
    }
}

/// Exact arguments for a transactional perpetual cancel-and-place modification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXPerpModifyParams {
    /// Existing order to cancel.
    pub cancel: DeepXPerpCancelParams,
    /// Replacement order to place using the modify extrinsic nonce as its order ID.
    pub place: DeepXPerpPlaceParams,
}

impl DeepXPerpModifyParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        let cancel = Value::unnamed_variant(
            "Cancel",
            [Value::unnamed_variant(
                "Perp",
                [self.cancel.into_dynamic_value()],
            )],
        );
        let place = Value::unnamed_variant(
            "Place",
            [Value::unnamed_variant(
                "Perp",
                [self.place.into_dynamic_value()],
            )],
        );
        vec![Value::unnamed_composite([cancel, place])]
    }
}

/// Exact arguments for a user-requested DeepX Spot order cancellation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSpotCancelParams {
    /// DeepX subaccount which owns the order.
    pub subaccount: [u8; 20],
    /// Deployment-provided bytes32 Spot pair identifier.
    pub pair: [u8; 32],
    /// Runtime-assigned order identifier.
    pub order_id: u64,
    /// Whether the canceled order is a buy.
    pub is_buy: bool,
    /// Whether the runtime should use its high-priority cancellation path.
    pub fast_cancel: bool,
}

impl DeepXSpotCancelParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::named_composite([(
            "params",
            Value::named_composite([
                ("subaccount", Value::from_bytes(self.subaccount)),
                ("pair", Value::from_bytes(self.pair)),
                ("order_id", Value::u128(u128::from(self.order_id))),
                ("is_buy", Value::bool(self.is_buy)),
                (
                    "cancel_reason",
                    Value::unnamed_variant("UserCanceled", Vec::<Value>::new()),
                ),
                ("fast_cancel", Value::bool(self.fast_cancel)),
            ]),
        )])]
    }
}
