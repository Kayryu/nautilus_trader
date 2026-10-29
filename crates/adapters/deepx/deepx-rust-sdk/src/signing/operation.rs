// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Exact DeepX transaction operation terms for durable caller records.

use serde::{Deserialize, Serialize};

use super::{ApprovedRuntimeIdentity, DeepXPerpOrderType, DeepXPostOnlyParam, DeepXSpotOrderType};

/// Immutable runtime identity attached to a direct-pallet reservation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeepXDirectRuntimeIdentity {
    /// Testnet genesis hash.
    pub genesis_hash: [u8; 32],
    /// SHA-256 of the complete SCALE metadata bytes.
    pub metadata_sha256: [u8; 32],
    /// Runtime specification version.
    pub spec_version: u32,
    /// Runtime transaction version.
    pub transaction_version: u32,
    /// Signed extensions in metadata order.
    pub signed_extensions: Vec<String>,
}

impl From<&ApprovedRuntimeIdentity> for DeepXDirectRuntimeIdentity {
    fn from(value: &ApprovedRuntimeIdentity) -> Self {
        Self {
            genesis_hash: value.genesis_hash,
            metadata_sha256: value.metadata_sha256,
            spec_version: value.spec_version,
            transaction_version: value.transaction_version,
            signed_extensions: value.signed_extensions.clone(),
        }
    }
}

/// A reserved nonce in one of DeepX's distinct nonce domains.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "domain", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DeepXNonceReservation {
    /// Timestamp-derived order identity used by the signed direct-pallet extrinsic.
    TimestampOrderId {
        /// Reserved timestamp nonce.
        value: u64,
    },
    /// Sequential account nonce reserved for protocols which prove that domain.
    SequentialAccount {
        /// Venue account index owning the sequence.
        account_index: u64,
        /// Reserved sequential nonce.
        nonce: u64,
    },
}

/// Protocol operation fields retained as part of a durable transaction identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DeepXTransactionOperation {
    /// Direct `PerpMarket.place_order` with all raw runtime arguments.
    PerpPlace {
        /// Venue subaccount placing the order.
        subaccount: [u8; 20],
        /// Venue perpetual market identifier.
        market_id: u16,
        /// Whether the order is long.
        is_long: bool,
        /// Exact runtime size without inferred scaling.
        #[serde(with = "perp_close_price")]
        size: u128,
        /// Exact runtime price without inferred scaling.
        #[serde(with = "perp_close_price")]
        price: u128,
        /// Runtime order type and associated data.
        order_type: DeepXPerpOrderType,
        /// Optional raw take-profit point.
        #[serde(with = "optional_perp_point")]
        take_profit: Option<u128>,
        /// Optional raw stop-loss point.
        #[serde(with = "optional_perp_point")]
        stop_loss: Option<u128>,
        /// Whether the order may only reduce a position.
        reduce_only: bool,
        /// Runtime post-only behavior.
        post_only: DeepXPostOnlyParam,
    },
    /// Direct `SpotMarket.place_order` operation with raw runtime integers.
    SpotPlace {
        /// Venue subaccount placing the order.
        subaccount: [u8; 20],
        /// Deployment-provided bytes32 pair identifier.
        pair: [u8; 32],
        /// Whether the order buys the base asset.
        is_buy: bool,
        /// Exact little-endian runtime `U256` quote amount.
        quote_amount: [u8; 32],
        /// Exact little-endian runtime `U256` base amount.
        base_amount: [u8; 32],
        /// Runtime order type and its exact associated data.
        order_type: DeepXSpotOrderType,
        /// Runtime post-only behavior.
        post_only: DeepXPostOnlyParam,
        /// Whether the order may only reduce an existing balance or liability.
        reduce_only: bool,
    },
    /// Direct `PerpMarket.close_position` with raw runtime integers.
    PerpClose {
        /// Venue subaccount whose position is closed.
        subaccount: [u8; 20],
        /// Venue perpetual market identifier.
        market_id: u16,
        /// Exact runtime price without inferred scaling.
        #[serde(with = "perp_close_price")]
        price: u128,
        /// Optional raw slippage without inferred units.
        slippage: Option<u64>,
    },
    /// Direct `PerpMarket.set_profit_and_loss_point` with raw runtime integers.
    PerpProfitAndLossPoint {
        /// Venue subaccount whose points are configured.
        subaccount: [u8; 20],
        /// Venue perpetual market identifier.
        market_id: u16,
        /// Exact runtime take-profit point without inferred scaling.
        #[serde(with = "perp_close_price")]
        take_profit_point: u128,
        /// Exact runtime stop-loss point without inferred scaling.
        #[serde(with = "perp_close_price")]
        stop_loss_point: u128,
    },
    /// Direct `SpotMarket.cancel_order` operation with fixed `UserCanceled` reason.
    SpotCancel {
        /// Venue subaccount whose order is canceled.
        subaccount: [u8; 20],
        /// Deployment-provided bytes32 pair identifier.
        pair: [u8; 32],
        /// Venue order identifier.
        order_id: u64,
        /// Whether the canceled order is a buy.
        is_buy: bool,
        /// Whether the runtime should use its fast-cancel path.
        fast_cancel: bool,
    },
    /// Direct `PerpMarket.cancel_order` operation.
    PerpCancel {
        /// Venue subaccount whose order is canceled.
        subaccount: [u8; 20],
        /// Venue order identifier.
        order_id: u64,
        /// Venue perpetual market identifier.
        market_id: u16,
        /// Whether the runtime should use its fast-cancel path.
        fast_cancel: bool,
    },
    /// Transactional `Subaccount.modify_orders` cancellation and replacement.
    PerpModify {
        /// Venue subaccount whose order is canceled and replaced.
        subaccount: [u8; 20],
        /// Subaccount specified by the replacement leg.
        place_subaccount: [u8; 20],
        /// Existing venue order identifier.
        cancel_order_id: u64,
        /// Venue perpetual market identifier.
        market_id: u16,
        /// Market specified by the replacement leg.
        place_market_id: u16,
        /// Whether the cancellation leg uses the runtime fast-cancel path.
        fast_cancel: bool,
        /// Replacement order direction.
        is_long: bool,
        /// Exact replacement runtime size.
        #[serde(with = "perp_close_price")]
        size: u128,
        /// Exact replacement runtime price.
        #[serde(with = "perp_close_price")]
        price: u128,
        /// Replacement runtime order type.
        order_type: DeepXPerpOrderType,
        /// Optional replacement take-profit point.
        #[serde(with = "optional_perp_point")]
        take_profit: Option<u128>,
        /// Optional replacement stop-loss point.
        #[serde(with = "optional_perp_point")]
        stop_loss: Option<u128>,
        /// Whether the replacement may only reduce a position.
        reduce_only: bool,
        /// Replacement runtime post-only behavior.
        post_only: DeepXPostOnlyParam,
    },
}

mod optional_perp_point {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

    pub(super) fn serialize<S: Serializer>(
        value: &Option<u128>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.map(|point| point.to_string()).serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u128>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|point| point.parse().map_err(D::Error::custom))
            .transpose()
    }
}

mod perp_close_price {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub(super) fn serialize<S: Serializer>(price: &u128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&price.to_string())
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<u128, D::Error> {
        let price = String::deserialize(deserializer)?;
        price.parse().map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;

    #[rstest]
    fn durable_runtime_identity_preserves_validated_runtime_fields() {
        let approved = ApprovedRuntimeIdentity {
            environment: crate::enums::DeepXEnvironment::Testnet,
            genesis_hash: [0x11; 32],
            metadata_sha256: [0x22; 32],
            spec_version: 376,
            transaction_version: 1,
            signed_extensions: vec!["CheckNonce".to_string()],
        };
        let durable = DeepXDirectRuntimeIdentity::from(&approved);
        let encoded = serde_json::to_value(&durable).unwrap();
        assert_eq!(encoded["genesis_hash"], json!(vec![0x11; 32]));
        assert_eq!(encoded["metadata_sha256"], json!(vec![0x22; 32]));
        assert_eq!(encoded["spec_version"], 376);
        assert_eq!(encoded["transaction_version"], 1);
        assert_eq!(
            serde_json::from_value::<DeepXDirectRuntimeIdentity>(encoded).unwrap(),
            durable,
        );
    }

    #[rstest]
    fn nonce_domains_have_distinct_complete_serialized_forms() {
        let timestamp = DeepXNonceReservation::TimestampOrderId { value: u64::MAX };
        let sequential = DeepXNonceReservation::SequentialAccount {
            account_index: 7,
            nonce: 9,
        };
        assert_eq!(
            serde_json::to_value(timestamp).unwrap(),
            json!({"domain": "timestamp-order-id", "value": u64::MAX}),
        );
        assert_eq!(
            serde_json::to_value(sequential).unwrap(),
            json!({"domain": "sequential-account", "account_index": 7, "nonce": 9}),
        );
        for value in [timestamp, sequential] {
            let encoded = serde_json::to_value(value).unwrap();
            assert_eq!(
                serde_json::from_value::<DeepXNonceReservation>(encoded).unwrap(),
                value,
            );
        }
        assert!(
            serde_json::from_value::<DeepXNonceReservation>(
                json!({"domain": "timestamp-order-id", "value": 7, "unexpected": true}),
            )
            .is_err(),
        );
    }

    #[rstest]
    fn perpetual_operation_preserves_full_width_runtime_integers() {
        let operation = DeepXTransactionOperation::PerpModify {
            subaccount: [0x11; 20],
            place_subaccount: [0x11; 20],
            cancel_order_id: 42,
            market_id: 3,
            place_market_id: 3,
            fast_cancel: false,
            is_long: true,
            size: u128::MAX,
            price: u128::MAX - 1,
            order_type: DeepXPerpOrderType::Stop,
            take_profit: Some(u128::MAX - 2),
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXPostOnlyParam::None,
        };
        let encoded = serde_json::to_value(&operation).unwrap();
        assert_eq!(encoded["size"], u128::MAX.to_string());
        assert_eq!(encoded["price"], (u128::MAX - 1).to_string());
        assert_eq!(encoded["take_profit"], (u128::MAX - 2).to_string());
        assert!(encoded["stop_loss"].is_null());
        assert_eq!(
            serde_json::from_value::<DeepXTransactionOperation>(encoded).unwrap(),
            operation,
        );
    }

    #[rstest]
    fn perpetual_operation_rejects_numeric_and_unknown_fields() {
        let operation = DeepXTransactionOperation::PerpClose {
            subaccount: [0x11; 20],
            market_id: 3,
            price: u128::MAX,
            slippage: None,
        };
        let mut encoded = serde_json::to_value(&operation).unwrap();
        assert_eq!(encoded["price"], u128::MAX.to_string());
        encoded["price"] = json!(3);
        assert!(serde_json::from_value::<DeepXTransactionOperation>(encoded.clone()).is_err());
        encoded["price"] = Value::String(u128::MAX.to_string());
        encoded["unexpected"] = json!(true);
        assert!(serde_json::from_value::<DeepXTransactionOperation>(encoded).is_err());
    }
}
