// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Metadata-driven signing for DeepX Substrate extrinsics.

use scale_decode::DecodeAsType;
use subxt::{
    ArcMetadata, OnlineClient,
    config::{
        Config, DefaultExtrinsicParamsBuilder, DefaultTransactionExtensions, HashFor,
        SubstrateConfig,
    },
    dynamic::{Value, storage, tx},
    utils::eth::{AccountId20, Signature},
};
use subxt_signer::eth::Keypair;
use thiserror::Error;

use crate::{common::credential::DeepXCredential, execution::nonce::DeepXChainTimeCalibration};

const PERP_MARKET_PALLET: &str = "PerpMarket";

#[derive(Clone, Debug, Default)]
struct DeepXConfig(SubstrateConfig);

impl Config for DeepXConfig {
    type AccountId = AccountId20;
    type Address = AccountId20;
    type Signature = Signature;
    type Hasher = <SubstrateConfig as Config>::Hasher;
    type Header = <SubstrateConfig as Config>::Header;
    type TransactionExtensions = DefaultTransactionExtensions<Self>;
    type AssetId = <SubstrateConfig as Config>::AssetId;

    fn genesis_hash(&self) -> Option<HashFor<Self>> {
        self.0.genesis_hash()
    }

    fn metadata_for_spec_version(&self, spec_version: u32) -> Option<ArcMetadata> {
        self.0.metadata_for_spec_version(spec_version)
    }

    fn set_metadata_for_spec_version(&self, spec_version: u32, metadata: ArcMetadata) {
        self.0.set_metadata_for_spec_version(spec_version, metadata);
    }

    fn spec_and_transaction_version_for_block_number(
        &self,
        block_number: u64,
    ) -> Option<(u32, u32)> {
        self.0
            .spec_and_transaction_version_for_block_number(block_number)
    }
}

/// Errors emitted while constructing and signing DeepX extrinsics.
#[derive(Debug, Error)]
pub enum DeepXSignerError {
    /// A hexadecimal key or address is malformed.
    #[error("invalid {field}: {reason}")]
    InvalidHex { field: &'static str, reason: String },
    /// The private key does not represent a valid secp256k1 scalar.
    #[error("invalid ECDSA private key: {0}")]
    InvalidPrivateKey(String),
    /// Runtime metadata is incompatible with the expected DeepX calls.
    #[error("incompatible DeepX runtime metadata: {0}")]
    IncompatibleRuntimeMetadata(String),
    /// Runtime metadata, RPC, SCALE encoding, or signing failed.
    #[error("Substrate transaction error: {0}")]
    Substrate(String),
}

/// Errors emitted when raw order values violate live runtime constraints.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DeepXOrderValidationError {
    #[error("order size must be positive")]
    ZeroSize,
    #[error("runtime {field} must be positive")]
    ZeroIncrement { field: &'static str },
    #[error("order size {size} is below minimum quantity {minimum}")]
    BelowMinimumQuantity { size: u128, minimum: u128 },
    #[error("order size {size} is not aligned to step size {step_size}")]
    InvalidStep { size: u128, step_size: u128 },
    #[error("order price {price} is not aligned to tick size {tick_size}")]
    InvalidTick { price: u128, tick_size: u128 },
    #[error("order notional is below raw minimum {minimum}")]
    BelowMinimumNotional { minimum: u128 },
    #[error("order notional comparison overflowed u128")]
    NotionalOverflow,
}

/// Raw perpetual market constraints decoded from live runtime metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeepXPerpMarketConstraints {
    pub base_decimal: u8,
    pub min_quantity: u128,
    pub tick_size: u128,
    pub step_size: u128,
    pub min_notional: Option<u128>,
}

impl DeepXPerpMarketConstraints {
    /// Validates raw limit-order size and price against runtime constraints.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid runtime increments, misaligned values, values below a
    /// runtime minimum, or arithmetic overflow during the exact notional comparison.
    pub fn validate_limit_order(
        &self,
        size: u128,
        price: u128,
    ) -> Result<(), DeepXOrderValidationError> {
        if size == 0 {
            return Err(DeepXOrderValidationError::ZeroSize);
        }
        if self.step_size == 0 {
            return Err(DeepXOrderValidationError::ZeroIncrement { field: "step_size" });
        }
        if self.tick_size == 0 {
            return Err(DeepXOrderValidationError::ZeroIncrement { field: "tick_size" });
        }
        if size < self.min_quantity {
            return Err(DeepXOrderValidationError::BelowMinimumQuantity {
                size,
                minimum: self.min_quantity,
            });
        }
        if !size.is_multiple_of(self.step_size) {
            return Err(DeepXOrderValidationError::InvalidStep {
                size,
                step_size: self.step_size,
            });
        }
        if !price.is_multiple_of(self.tick_size) {
            return Err(DeepXOrderValidationError::InvalidTick {
                price,
                tick_size: self.tick_size,
            });
        }
        if let Some(minimum) = self.min_notional {
            let base_scale = 10_u128
                .checked_pow(u32::from(self.base_decimal))
                .ok_or(DeepXOrderValidationError::NotionalOverflow)?;
            let actual = size
                .checked_mul(price)
                .ok_or(DeepXOrderValidationError::NotionalOverflow)?;
            let required = minimum
                .checked_mul(base_scale)
                .ok_or(DeepXOrderValidationError::NotionalOverflow)?;
            if actual < required {
                return Err(DeepXOrderValidationError::BelowMinimumNotional { minimum });
            }
        }

        Ok(())
    }
}

#[derive(Debug, DecodeAsType)]
struct RuntimePerpMarket {
    base_decimal: u8,
    order_spec: RuntimePerpOrderSpec,
}

#[derive(Debug, DecodeAsType)]
struct RuntimePerpOrderSpec {
    min_qty: u128,
    tick_size: u128,
    step_size: u128,
    min_notional: u128,
}

#[derive(Debug, DecodeAsType)]
struct LegacyRuntimePerpMarket {
    base_decimal: u8,
    order_spec: LegacyRuntimePerpOrderSpec,
}

#[derive(Debug, DecodeAsType)]
struct LegacyRuntimePerpOrderSpec {
    min_qty: u128,
    tick_size: u128,
    step_size: u128,
}

/// Perpetual order execution mode encoded from live runtime metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeepXOrderType {
    /// Good-til-cancelled limit order.
    Limit,
    /// Immediate-or-cancel limit order.
    ImmediateOrCancel,
    /// Market order with optional slippage in basis points.
    Market { slippage_bps: Option<u64> },
    /// Stop order.
    Stop,
}

/// DeepX post-only behavior.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DeepXPostOnly {
    /// Post-only handling is disabled.
    #[default]
    None,
    /// Reject the order unless it can rest on the book.
    MustPostOnly,
    /// Let the runtime adapt the order to rest on the book.
    Adaptive,
}

/// Parameters for `PerpMarket.place_order`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXPlacePerpOrder {
    pub subaccount: [u8; 20],
    pub market_id: u64,
    pub is_long: bool,
    pub size: u128,
    pub price: u128,
    pub order_type: DeepXOrderType,
    pub take_profit: Option<u128>,
    pub stop_loss: Option<u128>,
    pub reduce_only: bool,
    pub post_only: DeepXPostOnly,
}

impl DeepXPlacePerpOrder {
    /// Creates parameters after validating the 20-byte subaccount address.
    ///
    /// # Errors
    ///
    /// Returns an error when `subaccount` is not a 20-byte hexadecimal address.
    pub fn new(
        subaccount: &str,
        market_id: u64,
        is_long: bool,
        size: u128,
        price: u128,
    ) -> Result<Self, DeepXSignerError> {
        Ok(Self {
            subaccount: decode_hex_array(subaccount, "subaccount")?,
            market_id,
            is_long,
            size,
            price,
            order_type: DeepXOrderType::Limit,
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXPostOnly::None,
        })
    }

    fn value(&self) -> Value {
        Value::named_composite([
            ("subaccount", Value::from_bytes(self.subaccount)),
            ("market_id", Value::u128(u128::from(self.market_id))),
            ("is_long", Value::bool(self.is_long)),
            ("size", Value::u128(self.size)),
            ("price", Value::u128(self.price)),
            ("order_type", order_type_value(self.order_type)),
            ("take_profit", option_u128_value(self.take_profit)),
            ("stop_loss", option_u128_value(self.stop_loss)),
            ("reduce_only", Value::bool(self.reduce_only)),
            ("post_only", unit_variant(post_only_name(self.post_only))),
        ])
    }
}

/// Parameters for `PerpMarket.cancel_order`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXCancelPerpOrder {
    pub subaccount: [u8; 20],
    pub order_id: u64,
    pub market_id: u64,
    pub fast_cancel: bool,
}

impl DeepXCancelPerpOrder {
    /// Creates parameters after validating the 20-byte subaccount address.
    ///
    /// # Errors
    ///
    /// Returns an error when `subaccount` is not a 20-byte hexadecimal address.
    pub fn new(subaccount: &str, order_id: u64, market_id: u64) -> Result<Self, DeepXSignerError> {
        Ok(Self {
            subaccount: decode_hex_array(subaccount, "subaccount")?,
            order_id,
            market_id,
            fast_cancel: false,
        })
    }

    fn value(&self) -> Value {
        Value::named_composite([
            ("subaccount", Value::from_bytes(self.subaccount)),
            ("order_id", Value::u128(u128::from(self.order_id))),
            ("market_id", Value::u128(u128::from(self.market_id))),
            ("cancel_reason", unit_variant("UserCanceled")),
            ("fast_cancel", Value::bool(self.fast_cancel)),
        ])
    }
}

/// Builds signed DeepX extrinsics using metadata fetched from its Substrate RPC.
#[derive(Clone, Debug)]
pub struct DeepXExtrinsicSigner {
    client: OnlineClient<DeepXConfig>,
    keypair: Keypair,
}

impl DeepXExtrinsicSigner {
    /// Connects to DeepX and initializes an ECDSA signer from a raw 32-byte private key.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid key or when runtime metadata cannot be fetched.
    pub async fn connect(
        substrate_ws_url: &str,
        private_key: &str,
    ) -> Result<Self, DeepXSignerError> {
        let secret_key = decode_hex_array(private_key, "private key")?;
        let keypair = Keypair::from_secret_key(secret_key)
            .map_err(|error| DeepXSignerError::InvalidPrivateKey(error.to_string()))?;
        let client = OnlineClient::<DeepXConfig>::from_url(substrate_ws_url)
            .await
            .map_err(|error| DeepXSignerError::Substrate(error.to_string()))?;
        validate_runtime_metadata(&client).await?;
        Ok(Self { client, keypair })
    }

    /// Connects to DeepX using locally validated credentials.
    ///
    /// # Errors
    ///
    /// Returns an error when runtime metadata cannot be fetched.
    pub async fn connect_with_credential(
        substrate_ws_url: &str,
        credential: &DeepXCredential,
    ) -> Result<Self, DeepXSignerError> {
        let client = OnlineClient::<DeepXConfig>::from_url(substrate_ws_url)
            .await
            .map_err(|error| DeepXSignerError::Substrate(error.to_string()))?;
        validate_runtime_metadata(&client).await?;
        Ok(Self {
            client,
            keypair: credential.keypair().clone(),
        })
    }

    /// Returns the signer's 20-byte DeepX account ID as hexadecimal.
    #[must_use]
    pub fn account_id(&self) -> String {
        let account_id = self.keypair.public_key().to_account_id();
        format!("0x{}", hex::encode(account_id.0))
    }

    /// Calibrates timestamp nonces from `Timestamp.Now` at the current finalized block.
    ///
    /// # Errors
    ///
    /// Returns an error when the block or timestamp storage value cannot be fetched and decoded.
    pub async fn calibrate_chain_time(
        &self,
    ) -> Result<DeepXChainTimeCalibration, DeepXSignerError> {
        let at_block = self
            .client
            .at_current_block()
            .await
            .map_err(|error| DeepXSignerError::Substrate(error.to_string()))?;
        let timestamp = at_block
            .storage()
            .fetch(storage::<(), u64>("Timestamp", "Now"), ())
            .await
            .map_err(|error| DeepXSignerError::Substrate(error.to_string()))?
            .decode()
            .map_err(|error| DeepXSignerError::Substrate(error.to_string()))?;

        Ok(DeepXChainTimeCalibration::new(timestamp))
    }

    /// Returns raw order constraints for a perpetual market from `PerpMarkets` storage.
    ///
    /// # Errors
    ///
    /// Returns an error when the market is absent or runtime metadata cannot decode its fields.
    pub async fn perp_market_constraints(
        &self,
        market_id: u64,
    ) -> Result<DeepXPerpMarketConstraints, DeepXSignerError> {
        let at_block = self
            .client
            .at_current_block()
            .await
            .map_err(|error| DeepXSignerError::Substrate(error.to_string()))?;
        let market = at_block
            .storage()
            .fetch(
                storage::<(Value,), RuntimePerpMarket>(PERP_MARKET_PALLET, "PerpMarkets"),
                (Value::u128(u128::from(market_id)),),
            )
            .await
            .map_err(|error| DeepXSignerError::Substrate(error.to_string()))?;

        match market.decode() {
            Ok(market) => Ok(DeepXPerpMarketConstraints {
                base_decimal: market.base_decimal,
                min_quantity: market.order_spec.min_qty,
                tick_size: market.order_spec.tick_size,
                step_size: market.order_spec.step_size,
                min_notional: Some(market.order_spec.min_notional),
            }),
            Err(current_error) => {
                let market =
                    market
                        .decode_as::<LegacyRuntimePerpMarket>()
                        .map_err(|legacy_error| {
                            DeepXSignerError::IncompatibleRuntimeMetadata(format!(
                                "{PERP_MARKET_PALLET}.PerpMarkets[{market_id}]: current layout: \
                             {current_error}; legacy layout: {legacy_error}",
                            ))
                        })?;
                Ok(DeepXPerpMarketConstraints {
                    base_decimal: market.base_decimal,
                    min_quantity: market.order_spec.min_qty,
                    tick_size: market.order_spec.tick_size,
                    step_size: market.order_spec.step_size,
                    min_notional: None,
                })
            }
        }
    }

    /// Signs `PerpMarket.place_order` and returns the SCALE extrinsic as `0x` hex.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata encoding or signing fails.
    pub async fn sign_place_perp_order(
        &self,
        request: &DeepXPlacePerpOrder,
        nonce: u64,
    ) -> Result<String, DeepXSignerError> {
        self.sign("place_order", request.value(), nonce).await
    }

    /// Signs `PerpMarket.cancel_order` and returns the SCALE extrinsic as `0x` hex.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata encoding or signing fails.
    pub async fn sign_cancel_perp_order(
        &self,
        request: &DeepXCancelPerpOrder,
        nonce: u64,
    ) -> Result<String, DeepXSignerError> {
        self.sign("cancel_order", request.value(), nonce).await
    }

    async fn sign(
        &self,
        call: &str,
        params: Value,
        nonce: u64,
    ) -> Result<String, DeepXSignerError> {
        let payload = tx(PERP_MARKET_PALLET, call, vec![params]);
        let tx_params = DefaultExtrinsicParamsBuilder::<DeepXConfig>::new()
            .nonce(nonce)
            .build();
        let tx_client = self
            .client
            .tx()
            .await
            .map_err(|error| DeepXSignerError::Substrate(error.to_string()))?;
        let mut signable = tx_client
            .create_signable_offline(&payload, tx_params)
            .map_err(|error| DeepXSignerError::Substrate(error.to_string()))?;
        let signed = signable
            .sign(&self.keypair)
            .map_err(|error| DeepXSignerError::Substrate(error.to_string()))?;
        Ok(format!("0x{}", hex::encode(signed.into_encoded())))
    }
}

async fn validate_runtime_metadata(
    client: &OnlineClient<DeepXConfig>,
) -> Result<(), DeepXSignerError> {
    let place = DeepXPlacePerpOrder {
        subaccount: [0; 20],
        market_id: 0,
        is_long: false,
        size: 0,
        price: 0,
        order_type: DeepXOrderType::Limit,
        take_profit: None,
        stop_loss: None,
        reduce_only: false,
        post_only: DeepXPostOnly::None,
    };
    let cancel = DeepXCancelPerpOrder {
        subaccount: [0; 20],
        order_id: 0,
        market_id: 0,
        fast_cancel: false,
    };
    let tx_client = client
        .tx()
        .await
        .map_err(|error| DeepXSignerError::Substrate(error.to_string()))?;

    for (call, params) in [
        ("place_order", place.value()),
        ("cancel_order", cancel.value()),
    ] {
        let payload = tx(PERP_MARKET_PALLET, call, vec![params]);
        let tx_params = DefaultExtrinsicParamsBuilder::<DeepXConfig>::new()
            .nonce(0)
            .build();
        tx_client
            .create_signable_offline(&payload, tx_params)
            .map_err(|error| {
                DeepXSignerError::IncompatibleRuntimeMetadata(format!(
                    "{PERP_MARKET_PALLET}.{call}: {error}",
                ))
            })?;
    }

    Ok(())
}

fn decode_hex_array<const N: usize>(
    value: &str,
    field: &'static str,
) -> Result<[u8; N], DeepXSignerError> {
    let bytes = hex::decode(value.strip_prefix("0x").unwrap_or(value)).map_err(|error| {
        DeepXSignerError::InvalidHex {
            field,
            reason: error.to_string(),
        }
    })?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| DeepXSignerError::InvalidHex {
            field,
            reason: format!("expected {N} bytes, received {}", bytes.len()),
        })
}

fn unit_variant(name: &'static str) -> Value {
    Value::unnamed_variant(name, [])
}

fn option_u128_value(value: Option<u128>) -> Value {
    match value {
        Some(value) => Value::unnamed_variant("Some", [Value::u128(value)]),
        None => unit_variant("None"),
    }
}

fn order_type_value(order_type: DeepXOrderType) -> Value {
    match order_type {
        DeepXOrderType::Limit => Value::unnamed_variant("Limit", [unit_variant("GTC")]),
        DeepXOrderType::ImmediateOrCancel => Value::unnamed_variant("Limit", [unit_variant("IOC")]),
        DeepXOrderType::Market { slippage_bps } => Value::unnamed_variant(
            "Market",
            [match slippage_bps {
                Some(value) => Value::unnamed_variant("Some", [Value::u128(u128::from(value))]),
                None => unit_variant("None"),
            }],
        ),
        DeepXOrderType::Stop => unit_variant("Stop"),
    }
}

const fn post_only_name(post_only: DeepXPostOnly) -> &'static str {
    match post_only {
        DeepXPostOnly::None => "None",
        DeepXPostOnly::MustPostOnly => "MustPostOnly",
        DeepXPostOnly::Adaptive => "Adaptive",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::consts::DEEPX_TESTNET_SUBSTRATE_WS_URL;

    #[test]
    fn validates_hex_input_lengths() {
        let error = DeepXPlacePerpOrder::new("0x1234", 1, true, 10, 20).unwrap_err();
        assert!(error.to_string().contains("expected 20 bytes"));
    }

    #[test]
    fn derives_deepx_ecdsa_account_id() {
        let keypair = Keypair::from_secret_key([1; 32]).unwrap();
        let account_id = keypair.public_key().to_account_id();
        assert_eq!(account_id.0.len(), 20);
    }

    #[test]
    fn validates_raw_limit_order_constraints_exactly() {
        let constraints = DeepXPerpMarketConstraints {
            base_decimal: 18,
            min_quantity: 10_000_000_000_000_000,
            tick_size: 1_000,
            step_size: 5_000_000_000_000_000,
            min_notional: Some(10_000_000),
        };

        assert_eq!(
            constraints.validate_limit_order(10_000_000_000_000_000, 1_000_000_000),
            Ok(()),
        );
        assert_eq!(
            constraints.validate_limit_order(5_000_000_000_000_000, 2_000_000_000),
            Err(DeepXOrderValidationError::BelowMinimumQuantity {
                size: 5_000_000_000_000_000,
                minimum: 10_000_000_000_000_000,
            }),
        );
        assert_eq!(
            constraints.validate_limit_order(11_000_000_000_000_000, 1_000_000_000),
            Err(DeepXOrderValidationError::InvalidStep {
                size: 11_000_000_000_000_000,
                step_size: 5_000_000_000_000_000,
            }),
        );
        assert_eq!(
            constraints.validate_limit_order(10_000_000_000_000_000, 999_999_999),
            Err(DeepXOrderValidationError::InvalidTick {
                price: 999_999_999,
                tick_size: 1_000,
            }),
        );
        assert_eq!(
            constraints.validate_limit_order(10_000_000_000_000_000, 999_999_000),
            Err(DeepXOrderValidationError::BelowMinimumNotional {
                minimum: 10_000_000,
            }),
        );
    }

    #[test]
    fn rejects_raw_limit_order_validation_overflow() {
        let constraints = DeepXPerpMarketConstraints {
            base_decimal: 18,
            min_quantity: 1,
            tick_size: 1,
            step_size: 1,
            min_notional: Some(1),
        };

        assert_eq!(
            constraints.validate_limit_order(u128::MAX, 2),
            Err(DeepXOrderValidationError::NotionalOverflow),
        );
    }

    #[tokio::test]
    #[ignore = "requires DeepX testnet metadata"]
    async fn signs_perp_calls_from_live_metadata_without_submitting() {
        let signer = DeepXExtrinsicSigner::connect(
            DEEPX_TESTNET_SUBSTRATE_WS_URL,
            "0x0101010101010101010101010101010101010101010101010101010101010101",
        )
        .await
        .unwrap();
        let place =
            DeepXPlacePerpOrder::new("0x0000000000000000000000000000000000000000", 1, true, 1, 1)
                .unwrap();
        let cancel =
            DeepXCancelPerpOrder::new("0x0000000000000000000000000000000000000000", 1, 1).unwrap();

        let place_extrinsic = signer
            .sign_place_perp_order(&place, 1_781_757_000_123)
            .await
            .unwrap();

        println!("place_extrinsic: {place_extrinsic}");

        let cancel_extrinsic = signer
            .sign_cancel_perp_order(&cancel, 1_781_757_000_124)
            .await
            .unwrap();

        println!("cancel_extrinsic: {cancel_extrinsic}");
        assert!(place_extrinsic.starts_with("0x"));
        assert!(cancel_extrinsic.starts_with("0x"));
    }
}
