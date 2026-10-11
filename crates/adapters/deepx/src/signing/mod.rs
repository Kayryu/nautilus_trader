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

//! Fixture-gated DeepX runtime snapshots and offline pallet signing.

mod evm;
mod quota;
mod snapshot;

pub use evm::{
    DeepXEvmTransactionKind, DeepXOfflineEvmExtrinsic, DeepXOfflineEvmSignError,
    DeepXOfflineEvmTransaction, DeepXOfflineEvmTransactionParams, DeepXOfflineEvmWrapperError,
    DeepXOfflinePerpPnlEvmParams, sign_offline_evm_transact_extrinsic,
    sign_offline_evm_transaction, sign_offline_perp_pnl_evm_transact_extrinsic,
};
pub use quota::{DeepXQuotaClaimSignature, DeepXQuotaClaimSigningError, sign_quota_claim};
use serde::{Deserialize, Serialize};
pub use snapshot::{
    ApprovedRuntimeIdentity, DeepXReadOnlyRuntimeSnapshot, DeepXRuntimeChangeDecision,
    DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError, DeepXRuntimePalletInterface,
    DeepXRuntimeSnapshotPermit, DeepXRuntimeSnapshotService, DeepXRuntimeSnapshotServiceError,
    DeepXRuntimeSnapshotUpdate, DeepXRuntimeVariantIdentity, RuntimeSnapshot, SnapshotError,
};
use subxt_core::{
    Config,
    config::{DefaultExtrinsicParamsBuilder, Hasher, substrate::BlakeTwo256},
    dynamic::Value,
    tx,
    utils::AccountId20,
};
use subxt_signer::eth::{Keypair, Signature};
use thiserror::Error;

use crate::common::DeepXPrivateKey;

/// DeepX runtime types required for AccountId20 Ethereum-compatible signatures.
#[derive(Clone, Copy, Debug)]
pub enum DeepXRuntimeConfig {}

impl Config for DeepXRuntimeConfig {
    type AccountId = AccountId20;
    type Address = AccountId20;
    type Signature = Signature;
    type Hasher = BlakeTwo256;
    type Header = subxt_core::config::substrate::SubstrateHeader<u32, Self::Hasher>;
    type ExtrinsicParams = subxt_core::config::SubstrateExtrinsicParams<Self>;
    type AssetId = u32;
}

/// Errors produced before an extrinsic can be signed offline.
#[derive(Debug, Error)]
pub enum SigningError {
    /// The private scalar was rejected by the pinned DeepX signer implementation.
    #[error("invalid DeepX secp256k1 signing key")]
    InvalidKey,
    /// The runtime snapshot permit was revoked before signing.
    #[error(transparent)]
    RuntimePermit(#[from] DeepXRuntimeSnapshotServiceError),
    /// The pallet call is absent from the approved runtime interface.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The dynamic call or transaction extensions could not be SCALE encoded.
    #[error("unable to encode DeepX pallet extrinsic: {0}")]
    Encode(#[source] Box<subxt_core::Error>),
    /// The cancel and place legs of a perpetual modify target different runtime scopes.
    #[error("DeepX perpetual modify legs must target the same subaccount and market")]
    ModifyScopeMismatch,
    /// The runtime accepts at most 32 subaccount name bytes.
    #[error("DeepX subaccount name exceeds the runtime limit of 32 bytes: {length}")]
    SubaccountNameTooLong { length: usize },
}

impl From<subxt_core::Error> for SigningError {
    fn from(value: subxt_core::Error) -> Self {
        Self::Encode(Box::new(value))
    }
}

/// Derives the Ethereum-compatible DeepX account identity for a signing key.
///
/// # Errors
///
/// Returns an error if the pinned signer implementation rejects the private scalar.
pub fn derive_signer_account_id(key: &DeepXPrivateKey) -> Result<[u8; 20], SigningError> {
    let signer = Keypair::from_secret_key(*key.as_bytes()).map_err(|_| SigningError::InvalidKey)?;
    Ok(signer.public_key().to_account_id().0)
}

/// A signed SCALE extrinsic and its deterministic identities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedPalletExtrinsic {
    /// Complete compact-length-prefixed SCALE extrinsic bytes.
    pub(crate) bytes: Vec<u8>,
    /// Blake2-256 hash of the complete extrinsic bytes.
    pub(crate) extrinsic_hash: [u8; 32],
    /// Ethereum AccountId20 derived from the signing key.
    pub(crate) signer: [u8; 20],
    /// Explicit nonce encoded in the signed extensions.
    pub(crate) nonce: u64,
    /// Approved runtime identity used to encode and sign the extrinsic.
    pub(crate) runtime: ApprovedRuntimeIdentity,
}

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
    fn into_dynamic_value(self) -> Value {
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
        ])
    }

    fn into_dynamic_arguments(self) -> Vec<Value> {
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
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::named_composite([
            ("subaccount", Value::from_bytes(self.subaccount)),
            ("pair", Value::from_bytes(self.pair)),
            ("is_buy", Value::bool(self.is_buy)),
            ("quote_amount", u256_from_le_bytes(self.quote_amount)),
            ("base_amount", u256_from_le_bytes(self.base_amount)),
            ("order_type", self.order_type.into_dynamic_value()),
            ("post_only", self.post_only.into_dynamic_value()),
            ("reduce_only", Value::bool(self.reduce_only)),
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
    fn into_dynamic_arguments(self) -> Vec<Value> {
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
    fn into_dynamic_arguments(self) -> Vec<Value> {
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

    fn into_dynamic_arguments(self) -> Vec<Value> {
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
    fn into_dynamic_arguments(self) -> Vec<Value> {
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
    fn into_dynamic_arguments(self) -> Vec<Value> {
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

impl SignedPalletExtrinsic {
    /// Returns the complete SCALE extrinsic bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the Blake2-256 hash of the complete extrinsic bytes.
    #[must_use]
    pub const fn extrinsic_hash(&self) -> [u8; 32] {
        self.extrinsic_hash
    }

    /// Returns the signing AccountId20.
    #[must_use]
    pub const fn signer(&self) -> [u8; 20] {
        self.signer
    }

    /// Returns the nonce encoded in the signed extensions.
    #[must_use]
    pub const fn nonce(&self) -> u64 {
        self.nonce
    }

    /// Returns the approved runtime identity used for signing.
    #[must_use]
    pub const fn runtime(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime
    }

    pub(crate) fn has_valid_hash(&self) -> bool {
        BlakeTwo256.hash(&self.bytes).0 == self.extrinsic_hash
    }
}

/// Signs a metadata-driven DeepX pallet call without submitting it.
///
/// The caller supplies an explicit nonce and a runtime snapshot permit. Requiring the permit binds
/// public signing to the runtime-change quiescence boundary. This function performs no nonce
/// allocation, persistence, network access, retry, or submission, so it cannot make a trading
/// capability operational.
///
/// # Errors
///
/// Returns an error when the permit was revoked, the key is invalid, or the call cannot be encoded
/// against the approved runtime snapshot.
pub fn sign_dynamic_pallet_call(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    pallet: &str,
    call: &str,
    arguments: Vec<Value>,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    snapshot_permit.ensure_active()?;
    sign_dynamic_pallet_call_with_snapshot(
        snapshot_permit.snapshot(),
        key,
        pallet,
        call,
        arguments,
        nonce,
    )
}

/// Signs a perpetual order placement without submitting it.
///
/// All financial values are exact runtime integers. For the timestamp-nonce call in the captured
/// runtime, the caller-provided nonce also becomes the runtime order ID. This offline function
/// performs no nonce reservation, persistence, authorization, submission, or event verification
/// and does not make perpetual trading operational.
///
/// # Errors
///
/// Returns an error when the key is invalid or the call cannot be encoded against the approved
/// runtime snapshot.
pub fn sign_perp_place_order(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXPerpPlaceParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "place_order",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Signs a Spot order placement without submitting it.
///
/// Amounts are exact little-endian runtime `U256` values and the pair is the deployment's bytes32
/// identity. For the timestamp-nonce call in the captured runtime, the caller-provided nonce also
/// becomes the runtime order ID. This function performs no unit conversion, nonce reservation,
/// persistence, authorization, submission, or event verification.
///
/// # Errors
///
/// Returns an error when the key is invalid or the call cannot be encoded against the approved
/// runtime snapshot.
pub fn sign_spot_place_order(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSpotPlaceParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "SpotMarket",
        "place_order",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Signs a perpetual order cancellation without submitting it.
///
/// The cancellation reason is fixed to the runtime's `UserCanceled` variant. As with
/// [`sign_dynamic_pallet_call`], the caller owns nonce reservation and persistence.
///
/// # Errors
///
/// Returns an error when the key is invalid or the call cannot be encoded against the approved
/// runtime snapshot.
pub fn sign_perp_cancel(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXPerpCancelParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "cancel_order",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Signs a perpetual order modification as one transactional cancel-and-place call.
///
/// The cancel and replacement must target the same subaccount and market. The caller supplies an
/// explicit nonce, which the runtime uses as the replacement order ID. This function performs no
/// nonce allocation, persistence, authorization, submission, or event recovery.
///
/// # Errors
///
/// Returns an error when the operation scopes differ, the key is invalid, or the approved
/// runtime snapshot cannot encode `Subaccount.modify_orders`.
pub fn sign_perp_modify_order(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXPerpModifyParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    if params.cancel.subaccount != params.place.subaccount
        || params.cancel.market_id != params.place.market_id
    {
        return Err(SigningError::ModifyScopeMismatch);
    }
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "modify_orders",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Signs a Spot order cancellation without submitting it.
///
/// The pair is the deployment's bytes32 identity, not a symbol or perpetual market ID.
/// The cancellation reason is fixed to `UserCanceled`. The caller owns nonce reservation
/// and persistence; this offline function does not authorize operational trading.
///
/// # Errors
///
/// Returns an error when the key is invalid or the call cannot be encoded against the approved
/// runtime snapshot.
pub fn sign_spot_cancel(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSpotCancelParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "SpotMarket",
        "cancel_order",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Signs a metadata-driven perpetual position close without submitting it.
///
/// Price and slippage are raw runtime integers. This function establishes no financial unit
/// semantics, authorization, SDK parity, nonce reservation, or operational close capability.
///
/// # Errors
///
/// Returns an error if the key or the permitted snapshot cannot encode the close call.
pub fn sign_perp_close(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXPerpCloseParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "close_position",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Signs perpetual profit and loss point management without submitting it.
///
/// Both points are exact runtime integers. This offline API performs no unit conversion,
/// nonce reservation, persistence, or network access, and proves no authorization,
/// trigger semantics, independent SDK parity, or operational management capability.
///
/// # Errors
///
/// Returns an error if the key or permitted snapshot cannot encode the management call.
pub fn sign_perp_set_profit_and_loss_point(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXPerpProfitAndLossPointParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "set_profit_and_loss_point",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Exact raw arguments for an owner-signed global leverage setting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSetGlobalLeverageParams {
    /// Exact runtime subaccount identity, without inferred ownership.
    pub subaccount: [u8; 20],
    /// Raw runtime leverage, or `None` to clear the global setting.
    pub max_leverage: Option<u64>,
}

impl DeepXSetGlobalLeverageParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.subaccount),
            optional_leverage(self.max_leverage),
        ]
    }
}

/// Signs a global perpetual leverage setting without submitting it.
///
/// The runtime checks ownership and leverage bounds. This offline API does not establish either
/// condition, reserve a nonce, persist a transaction, or activate a framework command.
///
/// # Errors
///
/// Returns an error if the key or permitted runtime cannot encode the call.
pub fn sign_set_global_leverage(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSetGlobalLeverageParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "set_global_leverage",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Exact raw arguments for an owner-signed per-market leverage override.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSetPerMarketLeverageParams {
    /// Exact runtime subaccount identity, without inferred ownership.
    pub subaccount: [u8; 20],
    /// Runtime perpetual market identifier.
    pub market_id: u16,
    /// Raw runtime leverage, or `None` to remove this market override.
    pub max_leverage: Option<u64>,
}

impl DeepXSetPerMarketLeverageParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.subaccount),
            Value::u128(u128::from(self.market_id)),
            optional_leverage(self.max_leverage),
        ]
    }
}

/// Signs a per-market perpetual leverage setting without submitting it.
///
/// The runtime checks ownership, market eligibility, and leverage bounds. This offline API does
/// not establish those conditions, reserve a nonce, persist a transaction, or activate a command.
///
/// # Errors
///
/// Returns an error if the key or permitted runtime cannot encode the call.
pub fn sign_set_per_market_leverage(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSetPerMarketLeverageParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "PerpMarket",
        "set_per_market_leverage",
        params.into_dynamic_arguments(),
        nonce,
    )
}

fn optional_leverage(value: Option<u64>) -> Value {
    match value {
        Some(value) => Value::unnamed_variant("Some", [Value::u128(u128::from(value))]),
        None => Value::unnamed_variant("None", Vec::<Value>::new()),
    }
}

/// Exact raw arguments for subaccount initialization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXInitializeSubaccountParams {
    /// Opaque name bytes, bounded to 32 by the runtime call type.
    pub name: Vec<u8>,
}

impl DeepXInitializeSubaccountParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::from_bytes(self.name)]
    }
}

/// Signs subaccount initialization without submitting it.
///
/// This offline API does not infer the resulting address, owner eligibility, name uniqueness, or
/// business success. It performs no network access, nonce allocation, persistence, or submission.
///
/// # Errors
///
/// Returns an error if the name exceeds the runtime bound or the key or permitted snapshot cannot
/// encode the call.
pub fn sign_initialize_subaccount(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXInitializeSubaccountParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    if params.name.len() > 32 {
        return Err(SigningError::SubaccountNameTooLong {
            length: params.name.len(),
        });
    }
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "initialize_subaccount",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Exact raw arguments for subaccount deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXDeleteSubaccountParams {
    /// Exact runtime subaccount identity, without inferred authorization.
    pub subaccount: [u8; 20],
}

impl DeepXDeleteSubaccountParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::from_bytes(self.subaccount)]
    }
}

/// Signs subaccount deletion without submitting it.
///
/// This offline API infers no ownership, eligibility, or deletion semantics and performs no
/// network access, nonce allocation, persistence, replay, or live activation.
///
/// # Errors
///
/// Returns an error if the key or permitted snapshot cannot encode the call.
pub fn sign_delete_subaccount(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXDeleteSubaccountParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "delete_subaccount",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Exact raw arguments for subaccount renaming.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXRenameSubaccountParams {
    /// Exact runtime subaccount identity, without inferred ownership.
    pub subaccount: [u8; 20],
    /// Opaque new name bytes, bounded to 32 by the runtime call type.
    pub new_name: Vec<u8>,
}

impl DeepXRenameSubaccountParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.subaccount),
            Value::from_bytes(self.new_name),
        ]
    }
}

/// Signs subaccount renaming without submitting it.
///
/// This offline API does not establish ownership, name uniqueness, or business success. It
/// performs no network access, nonce allocation, persistence, or submission.
///
/// # Errors
///
/// Returns an error if the name exceeds the runtime bound or the key or permitted snapshot cannot
/// encode the call.
pub fn sign_rename_subaccount(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXRenameSubaccountParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    if params.new_name.len() > 32 {
        return Err(SigningError::SubaccountNameTooLong {
            length: params.new_name.len(),
        });
    }
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "rename_subaccount",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Exact raw arguments for a subaccount Spot margin setting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSetSpotMarginParams {
    /// Exact runtime subaccount identity, without inferred ownership.
    pub address: [u8; 20],
    /// Requested Spot margin setting.
    pub enable_spot_margin: bool,
}

impl DeepXSetSpotMarginParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.address),
            Value::bool(self.enable_spot_margin),
        ]
    }
}

/// Signs a subaccount Spot margin change without submitting it.
///
/// The runtime requires ownership and no open Spot orders. This offline API does not establish
/// either condition, reserve a nonce, persist a transaction, or activate a framework command.
///
/// # Errors
///
/// Returns an error if the key or permitted runtime cannot encode the call.
pub fn sign_set_spot_margin(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSetSpotMarginParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "set_spot_margin",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Exact raw arguments for wallet delegate creation or renewal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSetDelegateAccountParams {
    /// Exact runtime delegate identity, without inferred authorization.
    pub delegate: [u8; 20],
    /// Opaque runtime label bytes.
    pub name: Vec<u8>,
    /// Raw Unix millisecond expiry supplied by the caller.
    pub valid_until: u64,
}

impl DeepXSetDelegateAccountParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.delegate),
            Value::from_bytes(self.name),
            Value::u128(u128::from(self.valid_until)),
        ]
    }
}

/// Signs wallet delegate creation or renewal without submitting it.
///
/// This offline API performs no authorization, clock, ownership, or quota checks and does not
/// reserve a nonce, persist a transaction, or enable a management command.
///
/// # Errors
///
/// Returns an error if the key or permitted snapshot cannot encode the call.
pub fn sign_set_delegate_account(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXSetDelegateAccountParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "set_delegate_account",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Delegate modes accepted by the captured runtime's update call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXWritableDelegateMode {
    /// Allows order placement and cancellation.
    PlaceOrCancelOrder,
    /// Disables delegate permissions.
    Disable,
}

impl DeepXWritableDelegateMode {
    fn into_dynamic_value(self) -> Value {
        let variant = match self {
            Self::PlaceOrCancelOrder => "PlaceOrCancelOrder",
            Self::Disable => "Disable",
        };
        Value::unnamed_variant(variant, Vec::<Value>::new())
    }
}

/// Exact raw arguments for a wallet delegate mode update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXUpdateDelegateModeParams {
    /// Exact delegate identity, without inferred ownership or eligibility.
    pub address: [u8; 20],
    /// Mode accepted by the captured runtime dispatch logic.
    pub new_mode: DeepXWritableDelegateMode,
}

impl DeepXUpdateDelegateModeParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.address),
            self.new_mode.into_dynamic_value(),
        ]
    }
}

/// Signs a wallet delegate mode update without submitting it.
///
/// This offline API does not check account existence, owner authorization, expiry, or business
/// success. It reserves no nonce and performs no network access, persistence, or submission.
///
/// # Errors
///
/// Returns an error if the key or permitted snapshot cannot encode the call.
pub fn sign_update_delegate_mode(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXUpdateDelegateModeParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "update_delegate_mode",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Exact raw arguments for wallet delegate removal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXRemoveDelegateAccountParams {
    /// Exact runtime delegate identity, without inferred authorization.
    pub delegate: [u8; 20],
}

impl DeepXRemoveDelegateAccountParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::from_bytes(self.delegate)]
    }
}

/// Signs wallet delegate removal without submitting it.
///
/// This offline API infers no authorization or revocation semantics and performs no network
/// access, nonce reservation, persistence, or live activation.
///
/// # Errors
///
/// Returns an error if the key or permitted snapshot cannot encode the call.
pub fn sign_remove_delegate_account(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXRemoveDelegateAccountParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "remove_delegate_account",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Exact raw arguments for a Lending quota purchase call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXBuyQuotaParams {
    /// AccountId20 whose quota would be increased.
    pub address: [u8; 20],
    /// Raw runtime quota count.
    pub quota: u32,
    /// Optional source subaccount for the runtime purchase path.
    pub from_subaccount: Option<[u8; 20]>,
}

impl DeepXBuyQuotaParams {
    fn into_dynamic_arguments(self) -> Vec<Value> {
        let from_subaccount = match self.from_subaccount {
            Some(address) => Value::unnamed_variant("Some", [Value::from_bytes(address)]),
            None => Value::unnamed_variant("None", Vec::<Value>::new()),
        };
        vec![
            Value::from_bytes(self.address),
            Value::u128(u128::from(self.quota)),
            from_subaccount,
        ]
    }
}

/// Signs a Lending quota purchase call without submitting it.
///
/// This offline API does not establish payer authorization, quota pricing, fund availability,
/// nonce reservation, transaction persistence, or purchase finality.
///
/// # Errors
///
/// Returns an error if the key or permitted runtime cannot encode the call.
pub fn sign_buy_quota(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    params: DeepXBuyQuotaParams,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Lending",
        "buy_quota",
        params.into_dynamic_arguments(),
        nonce,
    )
}

/// Signs a metadata-driven Subaccount no-op without submitting it.
///
/// The call has no arguments. The caller supplies the exact signed-extension nonce and owns
/// reservation and persistence. This offline API proves no pool replacement, nonce-domain,
/// authorization, inclusion, or business semantics and grants no recovery or replay authority.
///
/// # Errors
///
/// Returns an error if the key or permitted runtime cannot encode the no-op call.
pub fn sign_no_op(
    snapshot_permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    sign_dynamic_pallet_call(
        snapshot_permit,
        key,
        "Subaccount",
        "no_op",
        Vec::new(),
        nonce,
    )
}

pub(crate) fn sign_dynamic_pallet_call_with_snapshot(
    snapshot: &RuntimeSnapshot,
    key: &DeepXPrivateKey,
    pallet: &str,
    call: &str,
    arguments: Vec<Value>,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    snapshot.interfaces().call(pallet, call)?;

    let signer = Keypair::from_secret_key(*key.as_bytes()).map_err(|_| SigningError::InvalidKey)?;
    let account_id = derive_signer_account_id(key)?;
    let payload = subxt_core::dynamic::tx(pallet, call, arguments);
    let params = DefaultExtrinsicParamsBuilder::<DeepXRuntimeConfig>::new()
        .nonce(nonce)
        .immortal()
        .build();
    let transaction =
        tx::create_v4_signed(&payload, snapshot.client_state(), params)?.sign(&signer);
    let hash = transaction.hash_with(BlakeTwo256);

    Ok(SignedPalletExtrinsic {
        bytes: transaction.into_encoded(),
        extrinsic_hash: hash.0,
        signer: account_id,
        nonce,
        runtime: snapshot.identity().clone(),
    })
}

#[cfg(test)]
mod tests {
    use nautilus_core::hex;
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use serde::Deserialize;

    use super::*;
    use crate::common::DeepXKeyScheme;

    #[derive(Deserialize)]
    struct RpcResponse {
        result: String,
    }

    fn snapshot() -> RuntimeSnapshot {
        let metadata: RpcResponse = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json",
        )))
        .unwrap();
        let bytes = hex::decode(metadata.result.trim_start_matches("0x")).unwrap();

        RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            hex::decode_array("86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b")
                .unwrap(),
            366,
            1,
            &bytes,
        )
        .unwrap()
    }

    fn snapshot_spec371() -> RuntimeSnapshot {
        let metadata: RpcResponse = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-86604388_metadata-4a620bad_spec-371_tx-1_finalized-64caaeda/metadata.json",
        )))
        .unwrap();
        let bytes = hex::decode(metadata.result.trim_start_matches("0x")).unwrap();
        RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            hex::decode_array("86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b")
                .unwrap(),
            371,
            1,
            &bytes,
        )
        .unwrap()
    }

    fn snapshot_spec369() -> RuntimeSnapshot {
        let metadata: RpcResponse = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-86604388_metadata-98136fdb_spec-369_tx-1_finalized-95febbff/metadata.json",
        )))
        .unwrap();
        let bytes = hex::decode(metadata.result.trim_start_matches("0x")).unwrap();
        RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            hex::decode_array("86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b")
                .unwrap(),
            369,
            1,
            &bytes,
        )
        .unwrap()
    }

    fn key() -> DeepXPrivateKey {
        DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap()
    }

    #[rstest]
    #[case::spec366(
        366,
        "810284fcad0b19bb29d4674531d6f115237e16afce377c57016d2af0940763ebbf413cc61b58196c3d7536629b0f105421694cd5e6224d5f9d5d0a39036fd0053d9d49549925d7bb2620e5ed01ce86474a8f9febaa771c00000b812203a2910100140311111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222802203a291010000000000"
    )]
    #[case::spec369(
        369,
        "810284fcad0b19bb29d4674531d6f115237e16afce377c1adc284d8e52364fafda797e3afb7c77ae93d9160bbec86017d07487adb18d513642f0b0cbf88dd840c70ae18c2180dc74b2134ad11b97bb4913856d3d769f6c01000b812203a2910100140311111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222802203a291010000000000"
    )]
    fn spot_cancel_matches_pinned_sdk_under_older_runtimes(
        #[case] spec_version: u32,
        #[case] expected: &str,
    ) {
        let snapshot = if spec_version == 366 {
            snapshot()
        } else {
            let response: RpcResponse = serde_json::from_str(include_str!(
                "../../test_data/runtime/testnet/genesis-86604388_metadata-98136fdb_spec-369_tx-1_finalized-95febbff/metadata.json"
            ))
            .unwrap();
            let bytes = hex::decode(response.result.trim_start_matches("0x")).unwrap();
            RuntimeSnapshot::approved_testnet(
                &crate::common::DeepXEnvironment::Testnet,
                snapshot().identity().genesis_hash,
                369,
                1,
                &bytes,
            )
            .unwrap()
        };
        let permit = DeepXRuntimeSnapshotService::new(snapshot)
            .acquire()
            .unwrap();
        let signed = sign_spot_cancel(
            &permit,
            &key(),
            DeepXSpotCancelParams {
                subaccount: [0x11; 20],
                pair: [0x22; 32],
                order_id: 1_725_000_000_128,
                is_buy: false,
                fast_cancel: false,
            },
            1_725_000_000_129,
        )
        .unwrap();
        assert_eq!(hex::encode(signed.bytes()), expected);
    }

    #[rstest]
    #[case::spec366(
        366,
        "050284fcad0b19bb29d4674531d6f115237e16afce377c3c5c2e45132c1228fc8504c0cf4c9ff82d82ed40f8d0d8937fc5e39d818957a52f1d00951f17adb7abbf78e5c9f73883cf0a38024d833eba5fbdcf68f113901a00000b7d2203a291010016031111111111111111111111111111111111111111012203a29101000007000001"
    )]
    #[case::spec369(
        369,
        "050284fcad0b19bb29d4674531d6f115237e16afce377cc270d5e80a810deafc00bcb03558628add687168876cce850d2fb7c17df27e0d2107ae85cf324b382d05828e0fe044b86c276ac720c9d52a9ed598618a8f103900000b7d2203a291010016031111111111111111111111111111111111111111012203a29101000007000001"
    )]
    #[case::spec371(
        371,
        "050284fcad0b19bb29d4674531d6f115237e16afce377c3adc81c5d2d7c2c4dca9724836dd45a34db253ddb95b1a81b3a3f5d80e55c3b05c26cc5a0764eda919ae0bb6d51f91d7d5155b2db6d7c3e951b353bdaed57ce901000b7d2203a291010016031111111111111111111111111111111111111111012203a29101000007000001"
    )]
    fn fast_perp_cancel_matches_pinned_sdk(#[case] spec_version: u32, #[case] expected: &str) {
        let snapshot = match spec_version {
            366 => snapshot(),
            369 => snapshot_spec369(),
            371 => snapshot_spec371(),
            _ => unreachable!(),
        };
        let permit = DeepXRuntimeSnapshotService::new(snapshot)
            .acquire()
            .unwrap();
        let signed = sign_perp_cancel(
            &permit,
            &key(),
            DeepXPerpCancelParams {
                subaccount: [0x11; 20],
                order_id: 1_725_000_000_001,
                market_id: 7,
                fast_cancel: true,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(hex::encode(signed.bytes()), expected);
        assert!(signed.has_valid_hash());
    }

    #[rstest]
    fn fast_spot_cancel_matches_pinned_sdk_spec371() {
        let permit = DeepXRuntimeSnapshotService::new(snapshot_spec371())
            .acquire()
            .unwrap();
        let signed = sign_spot_cancel(
            &permit,
            &key(),
            DeepXSpotCancelParams {
                subaccount: [0x11; 20],
                pair: [0x22; 32],
                order_id: 1_725_000_000_128,
                is_buy: false,
                fast_cancel: true,
            },
            1_725_000_000_129,
        )
        .unwrap();
        assert_eq!(
            hex::encode(signed.bytes()),
            "810284fcad0b19bb29d4674531d6f115237e16afce377cd53172d554595db1db1c8a0a238cd81649dad4acda6e69303c2b07a165a500dc1af1d04d2d8d761f2bc3511e3f307687d3ccb67358d1a51e89bdf75e71c2832600000b812203a2910100140311111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222802203a291010000000001",
        );
        assert!(signed.has_valid_hash());
    }

    #[rstest]
    fn old_permit_cannot_sign_after_runtime_evidence_is_inconsistent() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        service.halt_for_inconsistent_evidence().unwrap();

        assert!(matches!(
            sign_no_op(&permit, &key(), 42),
            Err(SigningError::RuntimePermit(
                DeepXRuntimeSnapshotServiceError::EvidenceInconsistent
            )),
        ));
    }

    #[rstest]
    #[case(
        DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
        None,
        Some(456),
        false,
        DeepXPostOnlyParam::None,
        vec![0, 0],
    )]
    #[case(
        DeepXPerpOrderType::Limit(DeepXTimeInForce::Ioc),
        Some(123),
        None,
        true,
        DeepXPostOnlyParam::MustPostOnly,
        vec![0, 1],
    )]
    #[case(
        DeepXPerpOrderType::Market(Some(u64::MAX)),
        None,
        None,
        false,
        DeepXPostOnlyParam::Adaptive,
        [vec![1, 1], u64::MAX.to_le_bytes().to_vec()].concat(),
    )]
    #[case(
        DeepXPerpOrderType::Stop,
        Some(u128::MAX),
        Some(0),
        true,
        DeepXPostOnlyParam::None,
        vec![2],
    )]
    #[case(
        DeepXPerpOrderType::Limit(DeepXTimeInForce::Fok),
        None,
        None,
        false,
        DeepXPostOnlyParam::None,
        vec![0, 2],
    )]
    #[case(
        DeepXPerpOrderType::Market(None),
        None,
        None,
        false,
        DeepXPostOnlyParam::None,
        vec![1, 0],
    )]
    fn perp_place_exact_scale_and_signing(
        #[case] order_type: DeepXPerpOrderType,
        #[case] take_profit: Option<u128>,
        #[case] stop_loss: Option<u128>,
        #[case] reduce_only: bool,
        #[case] post_only: DeepXPostOnlyParam,
        #[case] order_type_prefix: Vec<u8>,
    ) {
        let snapshot = snapshot();
        let metadata = snapshot.metadata();
        let pallet = metadata.pallet_by_name("PerpMarket").unwrap();
        let call = pallet.call_variant_by_name("place_order").unwrap();
        assert_eq!(pallet.index(), 22);
        assert_eq!(call.index, 2);
        assert_eq!(call.fields.len(), 1);
        assert_eq!(call.fields[0].name.as_deref(), Some("params"));
        let params_type = metadata.types().resolve(call.fields[0].ty.id).unwrap();
        let scale_info::TypeDef::Composite(composite) = &params_type.type_def else {
            panic!("Perp place params must be a composite");
        };
        assert_eq!(
            composite
                .fields
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            [
                "subaccount",
                "market_id",
                "is_long",
                "size",
                "price",
                "order_type",
                "take_profit",
                "stop_loss",
                "reduce_only",
                "post_only",
            ]
        );
        let params = DeepXPerpPlaceParams {
            subaccount: [0x11; 20],
            market_id: 7,
            is_long: true,
            size: 123,
            price: 456,
            order_type,
            take_profit,
            stop_loss,
            reduce_only,
            post_only,
        };
        let mut expected = vec![22, 2];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&params.market_id.to_le_bytes());
        expected.push(1);
        expected.extend_from_slice(&params.size.to_le_bytes());
        expected.extend_from_slice(&params.price.to_le_bytes());
        expected.extend_from_slice(&order_type_prefix);
        for value in [take_profit, stop_loss] {
            expected.push(u8::from(value.is_some()));
            if let Some(value) = value {
                expected.extend_from_slice(&value.to_le_bytes());
            }
        }
        expected.push(u8::from(reduce_only));
        expected.push(match post_only {
            DeepXPostOnlyParam::None => 0,
            DeepXPostOnlyParam::MustPostOnly => 1,
            DeepXPostOnlyParam::Adaptive => 2,
        });
        let payload =
            subxt_core::dynamic::tx("PerpMarket", "place_order", params.into_dynamic_arguments());
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, metadata).unwrap(),
            expected
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let nonce = 1_725_000_000_125;
        let signed = sign_perp_place_order(&permit, &key(), params, nonce).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(
            signed,
            sign_perp_place_order(&permit, &key(), params, nonce).unwrap()
        );
    }

    #[rstest]
    #[case::subaccount(0)]
    #[case::market_id(1)]
    #[case::side(2)]
    #[case::size(3)]
    #[case::price(4)]
    #[case::order_type(5)]
    #[case::take_profit(6)]
    #[case::stop_loss(7)]
    #[case::reduce_only(8)]
    #[case::post_only(9)]
    #[case::nonce(10)]
    fn perp_place_signed_identity_sensitivity(#[case] mutation: u8) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut params = DeepXPerpPlaceParams {
            subaccount: [0x11; 20],
            market_id: 7,
            is_long: true,
            size: 123,
            price: 456,
            order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXPostOnlyParam::None,
        };
        let mut nonce = 1_725_000_000_125;
        let original = sign_perp_place_order(&permit, &key(), params, nonce).unwrap();
        match mutation {
            0 => params.subaccount[19] ^= 1,
            1 => params.market_id += 1,
            2 => params.is_long = false,
            3 => params.size += 1,
            4 => params.price += 1,
            5 => params.order_type = DeepXPerpOrderType::Market(None),
            6 => params.take_profit = Some(0),
            7 => params.stop_loss = Some(0),
            8 => params.reduce_only = true,
            9 => params.post_only = DeepXPostOnlyParam::MustPostOnly,
            10 => nonce += 1,
            _ => unreachable!(),
        }
        let changed = sign_perp_place_order(&permit, &key(), params, nonce).unwrap();
        assert_ne!(original.bytes(), changed.bytes());
        assert_ne!(original.extrinsic_hash(), changed.extrinsic_hash());
        assert_eq!(original.runtime(), changed.runtime());
        assert_eq!(original.signer(), changed.signer());
    }

    #[rstest]
    fn perp_place_runtime_change_blocks_new_permit() {
        let snapshot = snapshot();
        let mut changed = snapshot.identity().clone();
        changed.spec_version += 1;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        service.observe_runtime_identity(changed).unwrap();
        assert!(service.acquire().is_err());
        assert!(
            sign_perp_place_order(
                &permit,
                &key(),
                DeepXPerpPlaceParams {
                    subaccount: [0x11; 20],
                    market_id: 7,
                    is_long: true,
                    size: 123,
                    price: 456,
                    order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                    take_profit: None,
                    stop_loss: None,
                    reduce_only: false,
                    post_only: DeepXPostOnlyParam::None,
                },
                1_725_000_000_125,
            )
            .is_ok()
        );
    }

    #[rstest]
    fn perp_place_rejects_malformed_dynamic_binding() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut arguments = DeepXPerpPlaceParams {
            subaccount: [0x11; 20],
            market_id: 7,
            is_long: true,
            size: 123,
            price: 456,
            order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXPostOnlyParam::None,
        }
        .into_dynamic_arguments();
        arguments[0] = Value::named_composite([(
            "params",
            Value::named_composite([("subaccount", Value::from_bytes([0x11; 19]))]),
        )]);
        assert!(matches!(
            sign_dynamic_pallet_call(&permit, &key(), "PerpMarket", "place_order", arguments, 125,),
            Err(SigningError::Encode(_))
        ));
    }

    #[rstest]
    #[case(
        DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc),
        DeepXPostOnlyParam::None,
        vec![0, 0],
    )]
    #[case(
        DeepXSpotOrderType::Limit(DeepXTimeInForce::Ioc),
        DeepXPostOnlyParam::MustPostOnly,
        vec![0, 1],
    )]
    #[case(
        DeepXSpotOrderType::Limit(DeepXTimeInForce::Fok),
        DeepXPostOnlyParam::Adaptive,
        vec![0, 2],
    )]
    #[case(
        DeepXSpotOrderType::Market(Some(u64::MAX)),
        DeepXPostOnlyParam::None,
        [vec![1, 1], u64::MAX.to_le_bytes().to_vec()].concat(),
    )]
    #[case(DeepXSpotOrderType::Market(None), DeepXPostOnlyParam::None, vec![1, 0])]
    #[case(DeepXSpotOrderType::Stop, DeepXPostOnlyParam::None, vec![2])]
    fn spot_place_exact_scale_and_signing(
        #[case] order_type: DeepXSpotOrderType,
        #[case] post_only: DeepXPostOnlyParam,
        #[case] order_type_bytes: Vec<u8>,
    ) {
        let snapshot = snapshot();
        let metadata = snapshot.metadata();
        let pallet = metadata.pallet_by_name("SpotMarket").unwrap();
        let call = pallet.call_variant_by_name("place_order").unwrap();
        assert_eq!(pallet.index(), 20);
        assert_eq!(call.index, 1);
        assert_eq!(call.fields.len(), 1);
        assert_eq!(call.fields[0].name.as_deref(), Some("params"));
        let params_type = metadata.types().resolve(call.fields[0].ty.id).unwrap();
        let scale_info::TypeDef::Composite(composite) = &params_type.type_def else {
            panic!("Spot place params must be a composite");
        };
        assert_eq!(
            composite
                .fields
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            [
                "subaccount",
                "pair",
                "is_buy",
                "quote_amount",
                "base_amount",
                "order_type",
                "post_only",
                "reduce_only",
            ]
        );
        let params = DeepXSpotPlaceParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            is_buy: true,
            quote_amount: [0x33; 32],
            base_amount: [0x44; 32],
            order_type,
            post_only,
            reduce_only: true,
        };
        let mut expected = vec![20, 1];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&params.pair);
        expected.push(1);
        expected.extend_from_slice(&params.quote_amount);
        expected.extend_from_slice(&params.base_amount);
        expected.extend_from_slice(&order_type_bytes);
        expected.push(match post_only {
            DeepXPostOnlyParam::None => 0,
            DeepXPostOnlyParam::MustPostOnly => 1,
            DeepXPostOnlyParam::Adaptive => 2,
        });
        expected.push(1);
        let payload =
            subxt_core::dynamic::tx("SpotMarket", "place_order", params.into_dynamic_arguments());
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, metadata).unwrap(),
            expected
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let nonce = 1_725_000_000_126;
        let signed = sign_spot_place_order(&permit, &key(), params, nonce).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(
            signed,
            sign_spot_place_order(&permit, &key(), params, nonce).unwrap()
        );
    }

    #[rstest]
    #[case(
        369,
        include_str!("../../test_data/runtime/testnet/genesis-86604388_metadata-98136fdb_spec-369_tx-1_finalized-95febbff/metadata.json"),
    )]
    #[case(
        371,
        include_str!("../../test_data/runtime/testnet/genesis-86604388_metadata-4a620bad_spec-371_tx-1_finalized-64caaeda/metadata.json"),
    )]
    fn spot_limit_ioc_and_market_sign_under_approved_runtime_snapshots(
        #[case] spec_version: u32,
        #[case] metadata_json: &str,
    ) {
        let response: RpcResponse = serde_json::from_str(metadata_json).unwrap();
        let bytes = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let genesis_hash = snapshot().identity().genesis_hash;
        let snapshot = RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            genesis_hash,
            spec_version,
            1,
            &bytes,
        )
        .unwrap();
        let params = DeepXSpotPlaceParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            is_buy: true,
            quote_amount: [0x33; 32],
            base_amount: [0x44; 32],
            order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Ioc),
            post_only: DeepXPostOnlyParam::None,
            reduce_only: false,
        };
        let pallet = snapshot.metadata().pallet_by_name("SpotMarket").unwrap();
        let call = pallet.call_variant_by_name("place_order").unwrap();
        let mut expected = vec![pallet.index(), call.index];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&params.pair);
        expected.push(1);
        expected.extend_from_slice(&params.quote_amount);
        expected.extend_from_slice(&params.base_amount);
        expected.extend_from_slice(&[0, 1, 0, 0]);
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_spot_place_order(&permit, &key(), params, 1_725_000_000_133).unwrap();

        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        let expected_bytes = match spec_version {
            369 => {
                "690384fcad0b19bb29d4674531d6f115237e16afce377cd0744615b40516ebf6f6bb3e9100588102f38dc40cee4fc75072c4dca810537158ce1442aadb13fce46568449c82ab43e10d1963f70b3f77fc938bf5f9813c9900000b852203a2910100140111111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222013333333333333333333333333333333333333333333333333333333333333333444444444444444444444444444444444444444444444444444444444444444400010000"
            }
            371 => {
                "690384fcad0b19bb29d4674531d6f115237e16afce377cb1e12ee3ed17790541d63f5ce75084bccb5575060a6c94f9dd2e4bb350aa52921911fd47d4643d8e42eabae1ccb4460c00c432a27bf7b014181ae5c5424aa87701000b852203a2910100140111111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222013333333333333333333333333333333333333333333333333333333333333333444444444444444444444444444444444444444444444444444444444444444400010000"
            }
            _ => unreachable!(),
        };
        assert_eq!(hex::encode(signed.bytes()), expected_bytes);

        let market = sign_spot_place_order(
            &permit,
            &key(),
            DeepXSpotPlaceParams {
                order_type: DeepXSpotOrderType::Market(None),
                ..params
            },
            1_725_000_000_135,
        )
        .unwrap();
        let expected_market_bytes = match spec_version {
            369 => {
                "690384fcad0b19bb29d4674531d6f115237e16afce377cd3626db8348883d5cee0e8aed25f1484600c9b7a5240761f4084f88c0dff780678e85770e5f524499e1a975876b48b8928db868052b8393a461e7077f2ecad8700000b872203a2910100140111111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222013333333333333333333333333333333333333333333333333333333333333333444444444444444444444444444444444444444444444444444444444444444401000000"
            }
            371 => {
                "690384fcad0b19bb29d4674531d6f115237e16afce377c4668857ab52a32b3932a39ceed8cffc213e91d85ba3f116f3fa21c26c73954435656869483ac74f1eb4e5cdaba173468f0ca34d184eae9e202f7385063064fb401000b872203a2910100140111111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222013333333333333333333333333333333333333333333333333333333333333333444444444444444444444444444444444444444444444444444444444444444401000000"
            }
            _ => unreachable!(),
        };
        assert_eq!(hex::encode(market.bytes()), expected_market_bytes);
    }

    #[rstest]
    #[case::subaccount(0)]
    #[case::pair(1)]
    #[case::side(2)]
    #[case::quote_amount(3)]
    #[case::base_amount(4)]
    #[case::order_type(5)]
    #[case::post_only(6)]
    #[case::reduce_only(7)]
    #[case::nonce(8)]
    fn spot_place_signed_identity_sensitivity(#[case] mutation: u8) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut params = DeepXSpotPlaceParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            is_buy: true,
            quote_amount: [0x33; 32],
            base_amount: [0x44; 32],
            order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc),
            post_only: DeepXPostOnlyParam::None,
            reduce_only: false,
        };
        let mut nonce = 1_725_000_000_126;
        let original = sign_spot_place_order(&permit, &key(), params, nonce).unwrap();
        match mutation {
            0 => params.subaccount[19] ^= 1,
            1 => params.pair[31] ^= 1,
            2 => params.is_buy = false,
            3 => params.quote_amount[31] ^= 1,
            4 => params.base_amount[31] ^= 1,
            5 => params.order_type = DeepXSpotOrderType::Market(None),
            6 => params.post_only = DeepXPostOnlyParam::MustPostOnly,
            7 => params.reduce_only = true,
            8 => nonce += 1,
            _ => unreachable!(),
        }
        let changed = sign_spot_place_order(&permit, &key(), params, nonce).unwrap();
        assert_ne!(original.bytes(), changed.bytes());
        assert_ne!(original.extrinsic_hash(), changed.extrinsic_hash());
        assert_eq!(original.runtime(), changed.runtime());
        assert_eq!(original.signer(), changed.signer());
    }

    #[rstest]
    fn delete_subaccount_metadata_contract() {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet.call_variant_by_name("delete_subaccount").unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 1);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| (field.name.as_deref(), field.type_name.as_deref()))
                .collect::<Vec<_>>(),
            [(Some("subaccount"), Some("H160"))]
        );
    }

    #[rstest]
    #[case([0; 20], 0)]
    #[case([0xff; 20], u64::MAX)]
    #[case([0x11; 20], 63)]
    #[case([0x22; 20], 64)]
    fn delete_subaccount_exact_scale(#[case] subaccount: [u8; 20], #[case] nonce: u64) {
        let snapshot = snapshot();
        let params = DeepXDeleteSubaccountParams { subaccount };
        let payload = subxt_core::dynamic::tx(
            "Subaccount",
            "delete_subaccount",
            params.into_dynamic_arguments(),
        );
        let mut expected = vec![19, 1];
        expected.extend_from_slice(&subaccount);
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
            expected
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_delete_subaccount(&permit, &key(), params, nonce).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(
            signed,
            sign_delete_subaccount(&permit, &key(), params, nonce).unwrap()
        );
        let mut changed = params;
        changed.subaccount[19] ^= 1;
        let other_key = DeepXPrivateKey::new(
            "0x1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        for altered in [
            sign_delete_subaccount(&permit, &key(), changed, nonce).unwrap(),
            sign_delete_subaccount(&permit, &key(), params, nonce ^ 1).unwrap(),
            sign_delete_subaccount(&permit, &other_key, params, nonce).unwrap(),
        ] {
            assert_ne!(signed.bytes(), altered.bytes());
            assert_ne!(signed.extrinsic_hash(), altered.extrinsic_hash());
        }
    }

    #[rstest]
    #[case(vec![])]
    #[case(vec![Value::from_bytes([0; 19])])]
    #[case(vec![Value::from_bytes([0; 21])])]
    #[case(vec![Value::bool(true)])]
    #[case(vec![Value::from_bytes([0; 20]), Value::from_bytes([0; 20])])]
    fn delete_subaccount_malformed_arguments(#[case] arguments: Vec<Value>) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        assert!(matches!(
            sign_dynamic_pallet_call(
                &permit,
                &key(),
                "Subaccount",
                "delete_subaccount",
                arguments,
                0
            ),
            Err(SigningError::Encode(_))
        ));
    }

    #[rstest]
    fn delete_subaccount_runtime_change_blocks_new_permit() {
        let snapshot = snapshot();
        let mut changed = snapshot.identity().clone();
        changed.spec_version += 1;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        service.observe_runtime_identity(changed).unwrap();
        assert!(service.acquire().is_err());
        assert!(
            sign_delete_subaccount(
                &permit,
                &key(),
                DeepXDeleteSubaccountParams {
                    subaccount: [0x11; 20]
                },
                0,
            )
            .is_ok()
        );
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn set_spot_margin_exact_scale(#[case] enabled: bool) {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet.call_variant_by_name("set_spot_margin").unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 6);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| (field.name.as_deref(), field.type_name.as_deref()))
                .collect::<Vec<_>>(),
            [
                (Some("address"), Some("H160")),
                (Some("enable_spot_margin"), Some("bool")),
            ]
        );
        let params = DeepXSetSpotMarginParams {
            address: [0x11; 20],
            enable_spot_margin: enabled,
        };
        let payload = subxt_core::dynamic::tx(
            "Subaccount",
            "set_spot_margin",
            params.into_dynamic_arguments(),
        );
        let mut expected = vec![19, 6];
        expected.extend_from_slice(&params.address);
        expected.push(u8::from(enabled));
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
            expected,
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_set_spot_margin(&permit, &key(), params, 1_725_000_000_138).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
    }

    #[rstest]
    #[case::spec366(
        366,
        "d90184fcad0b19bb29d4674531d6f115237e16afce377ceee83d340e6da5bb10103f064903ae2dc1606564f9685b1f621c9a312ef29e5f60b7e7711f52d27b0d07a9d8eb3c634f6b74414758fbdf2aa374ed8c82be5cf101000b8a2203a29101001306111111111111111111111111111111111111111101"
    )]
    #[case::spec369(
        369,
        "d90184fcad0b19bb29d4674531d6f115237e16afce377c319211b06b2255d91334722da7e9902603e55a51dd7f2260ec525250111c815100f98a80afd7832dcf8e52099d63741b407fb3e1fffc5a0ef6ce582da001ad4001000b8a2203a29101001306111111111111111111111111111111111111111101"
    )]
    #[case::spec371(
        371,
        "d90184fcad0b19bb29d4674531d6f115237e16afce377c47eaac364d88409a5942882b0e3fe9f4ff0efef9707c95662c9f0856ae529615339fcf2770576da96ea33c64114b4d81a47fb4c47e255eb78e98ebccd7cf4b3300000b8a2203a29101001306111111111111111111111111111111111111111101"
    )]
    fn set_spot_margin_matches_pinned_sdk_vectors(
        #[case] spec_version: u32,
        #[case] expected_signed: &str,
    ) {
        let snapshot = match spec_version {
            366 => snapshot(),
            369 => snapshot_spec369(),
            371 => snapshot_spec371(),
            _ => unreachable!(),
        };
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet.call_variant_by_name("set_spot_margin").unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 6);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["address", "enable_spot_margin"]
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_set_spot_margin(
            &permit,
            &key(),
            DeepXSetSpotMarginParams {
                address: [0x11; 20],
                enable_spot_margin: true,
            },
            1_725_000_000_138,
        )
        .unwrap();
        assert_eq!(hex::encode(signed.bytes()), expected_signed);
        assert!(signed.has_valid_hash());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
    }

    #[rstest]
    #[case::spec366_wallet(
        366,
        None,
        "e90184fcad0b19bb29d4674531d6f115237e16afce377cc7945d4e267d3853eeaeec6439f0bccc8e93dc75eb5bc4f203a655e5a4b3c2152e0bbff1f01455850f3077214a27b54d998e327f07dafbfa37dcee8f2d6d219e00000b962203a2910100180c11111111111111111111111111111111111111116400000000"
    )]
    #[case::spec366_subaccount(
        366,
        Some([0x22; 20]),
        "390284fcad0b19bb29d4674531d6f115237e16afce377c67b02573df5b229792fc96b55fb670c7bbe83be4e58cc3c133415adc265d3c5a3f859ec78158cead196dec4e76eeea7c62ef010e0c6287f0a20359a16da3d51a00000b962203a2910100180c111111111111111111111111111111111111111164000000012222222222222222222222222222222222222222"
    )]
    #[case::spec369_wallet(
        369,
        None,
        "e90184fcad0b19bb29d4674531d6f115237e16afce377c23cfcb65956f74d10f5a90bc4814e8f5837f7d5816392a1978850b74efe30d3600d5e27772d5f20486455354d785897c069246c6b868d900dd5c672acb60bdc000000b962203a2910100180c11111111111111111111111111111111111111116400000000"
    )]
    #[case::spec369_subaccount(
        369,
        Some([0x22; 20]),
        "390284fcad0b19bb29d4674531d6f115237e16afce377c6e2e8d78795fce845a6ec16f3084679002280db322c2d4fc08164ca714c8ebdd3ffc4cfd95b9b679023cbfd0e919d6887f9be28dd10233cfcd24622bec70ebe501000b962203a2910100180c111111111111111111111111111111111111111164000000012222222222222222222222222222222222222222"
    )]
    #[case::spec371_wallet(
        371,
        None,
        "e90184fcad0b19bb29d4674531d6f115237e16afce377c3a3d621cb24e0d67332f01ed109129ca7c4744838c792af38a80a4abd9fbb9623d226694251d6f1cdbfe4121bf0e13c35ba17b8f6112d76599c319ccc0f15d5900000b962203a2910100180c11111111111111111111111111111111111111116400000000"
    )]
    #[case::spec371_subaccount(
        371,
        Some([0x22; 20]),
        "390284fcad0b19bb29d4674531d6f115237e16afce377ca8ebd8efbf15201e241834dae51a9d8f4b573e4507ad83ea8b890af48972e97a06c827bc07863f749a7bdecba507f50ecf1fe832d1d7c53f0c29601ace41905f01000b962203a2910100180c111111111111111111111111111111111111111164000000012222222222222222222222222222222222222222"
    )]
    fn buy_quota_matches_pinned_sdk_vectors(
        #[case] spec_version: u32,
        #[case] from_subaccount: Option<[u8; 20]>,
        #[case] expected_signed: &str,
    ) {
        let snapshot = match spec_version {
            366 => snapshot(),
            369 => snapshot_spec369(),
            371 => snapshot_spec371(),
            _ => unreachable!(),
        };
        let pallet = snapshot.metadata().pallet_by_name("Lending").unwrap();
        let call = pallet.call_variant_by_name("buy_quota").unwrap();
        assert_eq!(
            call.fields
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["address", "quota", "from_subaccount"],
        );
        let permit = DeepXRuntimeSnapshotService::new(snapshot)
            .acquire()
            .unwrap();
        let signed = sign_buy_quota(
            &permit,
            &key(),
            DeepXBuyQuotaParams {
                address: [0x11; 20],
                quota: 100,
                from_subaccount,
            },
            1_725_000_000_150,
        )
        .unwrap();
        assert_eq!(hex::encode(signed.bytes()), expected_signed);
        assert!(signed.has_valid_hash());
    }

    #[rstest]
    #[case(None)]
    #[case(Some(20_000))]
    fn leverage_settings_encode_exact_runtime_options(#[case] max_leverage: Option<u64>) {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("PerpMarket").unwrap();
        assert_eq!(pallet.index(), 22);
        for (call_name, index, fields) in [
            (
                "set_global_leverage",
                43,
                vec!["subaccount", "max_leverage"],
            ),
            (
                "set_per_market_leverage",
                44,
                vec!["subaccount", "market_id", "max_leverage"],
            ),
        ] {
            let call = pallet.call_variant_by_name(call_name).unwrap();
            assert_eq!(call.index, index);
            assert_eq!(
                call.fields
                    .iter()
                    .map(|field| field.name.as_deref().unwrap())
                    .collect::<Vec<_>>(),
                fields,
            );
        }
        let mut option_bytes = vec![u8::from(max_leverage.is_some())];
        if let Some(value) = max_leverage {
            option_bytes.extend_from_slice(&value.to_le_bytes());
        }
        let global = DeepXSetGlobalLeverageParams {
            subaccount: [0x11; 20],
            max_leverage,
        };
        let per_market = DeepXSetPerMarketLeverageParams {
            subaccount: [0x11; 20],
            market_id: 3,
            max_leverage,
        };
        let mut global_expected = vec![22, 43];
        global_expected.extend_from_slice(&global.subaccount);
        global_expected.extend_from_slice(&option_bytes);
        let mut market_expected = vec![22, 44];
        market_expected.extend_from_slice(&per_market.subaccount);
        market_expected.extend_from_slice(&per_market.market_id.to_le_bytes());
        market_expected.extend_from_slice(&option_bytes);
        for (call_name, arguments, expected) in [
            (
                "set_global_leverage",
                global.into_dynamic_arguments(),
                &global_expected,
            ),
            (
                "set_per_market_leverage",
                per_market.into_dynamic_arguments(),
                &market_expected,
            ),
        ] {
            let payload = subxt_core::dynamic::tx("PerpMarket", call_name, arguments);
            assert_eq!(
                &tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
                expected,
            );
        }
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let global_signed = sign_set_global_leverage(&permit, &key(), global, 42).unwrap();
        let market_signed = sign_set_per_market_leverage(&permit, &key(), per_market, 42).unwrap();
        assert!(global_signed.bytes().ends_with(&global_expected));
        assert!(market_signed.bytes().ends_with(&market_expected));
        assert!(global_signed.has_valid_hash());
        assert!(market_signed.has_valid_hash());
        assert_ne!(global_signed.bytes(), market_signed.bytes());
    }

    #[rstest]
    fn leverage_settings_match_pinned_sdk_spec371_vectors() {
        let service = DeepXRuntimeSnapshotService::new(snapshot_spec371());
        let permit = service.acquire().unwrap();
        let global = sign_set_global_leverage(
            &permit,
            &key(),
            DeepXSetGlobalLeverageParams {
                subaccount: [0x11; 20],
                max_leverage: None,
            },
            1_725_000_000_140,
        )
        .unwrap();
        let per_market = sign_set_per_market_leverage(
            &permit,
            &key(),
            DeepXSetPerMarketLeverageParams {
                subaccount: [0x11; 20],
                market_id: 3,
                max_leverage: Some(20_000),
            },
            1_725_000_000_141,
        )
        .unwrap();
        assert_eq!(
            hex::encode(global.bytes()),
            "d90184fcad0b19bb29d4674531d6f115237e16afce377c3eb93fc436c1c92cbe80d40298ed6e69f73f72f3ebafb69c4b510d6aa0176f6a6c38fd402cea4a53fd290de1af32dccd1b74ce78325b4deb64ef8f7f2c63fed501000b8c2203a2910100162b111111111111111111111111111111111111111100",
        );
        assert_eq!(
            hex::encode(per_market.bytes()),
            "010284fcad0b19bb29d4674531d6f115237e16afce377c885f13b5768c298677c44b579428d4ef5bc2c275dc465763a88a4f1c4c6ab6f32e5243e3012562ea59e550ea1fd24840f0da0f96dde35194ef6f228ccf2165dc00000b8d2203a2910100162c1111111111111111111111111111111111111111030001204e000000000000",
        );
    }

    #[rstest]
    fn spec369_matches_independent_sdk_vectors() {
        let response: RpcResponse = serde_json::from_str(include_str!(
            "../../test_data/runtime/testnet/genesis-86604388_metadata-98136fdb_spec-369_tx-1_finalized-95febbff/metadata.json"
        )).unwrap();
        let bytes = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let old_snapshot = snapshot();
        let snapshot = RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            old_snapshot.identity().genesis_hash,
            369,
            1,
            &bytes,
        )
        .unwrap();
        assert_eq!(snapshot.identity().spec_version, 369);
        assert_eq!(
            hex::encode(snapshot.identity().metadata_sha256),
            "98136fdbab99332fa40828119c9d53a71a219f3e23155844cc0230cc663cba3c"
        );
        assert!(
            RuntimeSnapshot::approved_testnet(
                &crate::common::DeepXEnvironment::Testnet,
                old_snapshot.identity().genesis_hash,
                366,
                1,
                &bytes,
            )
            .is_err()
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let no_op = sign_no_op(&permit, &key(), 1_725_000_000_124).unwrap();
        assert_eq!(
            hex::encode(no_op.bytes()),
            "850184fcad0b19bb29d4674531d6f115237e16afce377cee66eb79a74f1ead51bd7f068610f3b1381a77f9ed0032dcdecf089f220e95c66442eba016e0c53cfc32eba2c0f6f646206f4f84681b82429dcb49b8df422b1b00000b7c2203a2910100131c",
        );
        let remark = sign_dynamic_pallet_call(
            &permit,
            &key(),
            "System",
            "remark",
            vec![Value::from_bytes(b"deepx-offline-signing-check")],
            1_725_000_000_123,
        )
        .unwrap();
        assert_eq!(
            hex::encode(remark.bytes()),
            "f50184fcad0b19bb29d4674531d6f115237e16afce377cc66f72cdc7c5e16824473501762652d8acd5d3ed2c6881563864a93d5a01a4a262e0c2f5fc2e2698e245e70775805742adf3cc4770446bb807177ff0df87799801000b7b2203a291010000006c64656570782d6f66666c696e652d7369676e696e672d636865636b",
        );
        let place = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: true,
                size: u128::MAX,
                price: u128::MAX,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                take_profit: Some(u128::MAX),
                stop_loss: None,
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(place.bytes()),
            "b90284fcad0b19bb29d4674531d6f115237e16afce377cbfb9294139140c58ea9fb2de1dd0727359f52ad0f969c5e0fcd042ce886cef677527ee2e5bade3c20164af2fe16976826a1a23eba0d47d081f085ec31746725b00000b7d2203a291010016021111111111111111111111111111111111111111070001ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff000001ffffffffffffffffffffffffffffffff000000",
        );
        let limit_ioc = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: true,
                size: 1_000_000_000_000_000,
                price: 2_499_050_000,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Ioc),
                take_profit: None,
                stop_loss: None,
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(limit_ioc.bytes()),
            "790284fcad0b19bb29d4674531d6f115237e16afce377cda154c8c33e393b013f3847bdc805d86005bc9f86c7136924940e9f4d5279c852026d42369f3f10720b404640f6b744d7001faa2278dc034e50bc43bd20cd2d801000b7d2203a2910100160211111111111111111111111111111111111111110700010080c6a47e8d03000000000000000000107af494000000000000000000000000000100000000",
        );
        let market_ioc = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: true,
                size: 1_000_000_000_000_000,
                price: 0,
                order_type: DeepXPerpOrderType::Market(None),
                take_profit: None,
                stop_loss: None,
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(market_ioc.bytes()),
            "790284fcad0b19bb29d4674531d6f115237e16afce377cde6e00cb63f5f822612a9bd055c3f3616e43479224ff4d7603e0d184e406f2c2797e3f719ff59fd06db69d28e7b51c38f4672a595d866ca30991b19f0a4053ad00000b7d2203a2910100160211111111111111111111111111111111111111110700010080c6a47e8d0300000000000000000000000000000000000000000000000000010000000000",
        );
        let post_only_reduce_only_sell = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: false,
                size: 1_000_000_000_000_000,
                price: 2_499_050_000,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                take_profit: None,
                stop_loss: None,
                reduce_only: true,
                post_only: DeepXPostOnlyParam::MustPostOnly,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(post_only_reduce_only_sell.bytes()),
            "790284fcad0b19bb29d4674531d6f115237e16afce377cc1f64aff3ef4886dff70093171d731a8b90dbc1b11a86707e65413cdbc23f8ed50bf261d74d5d495daa4517188fc412708791a3304818df3c973a9b54f3f699c01000b7d2203a2910100160211111111111111111111111111111111111111110700000080c6a47e8d03000000000000000000107af494000000000000000000000000000000000101",
        );
        let cancel = sign_perp_cancel(
            &permit,
            &key(),
            DeepXPerpCancelParams {
                subaccount: [0x11; 20],
                order_id: 1_725_000_000_001,
                market_id: 7,
                fast_cancel: false,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(cancel.bytes()),
            "050284fcad0b19bb29d4674531d6f115237e16afce377c152b54a48813785aa57c9baf3e9c735f6605547989f68154c0842e1ac0b027e00713afa4407c312135214709a88f0bc537ebf72727cb2df9a383ef953d9a894200000b7d2203a291010016031111111111111111111111111111111111111111012203a29101000007000000",
        );
        let modify = sign_perp_modify_order(
            &permit,
            &key(),
            DeepXPerpModifyParams {
                cancel: DeepXPerpCancelParams {
                    subaccount: [0x11; 20],
                    order_id: 123,
                    market_id: 7,
                    fast_cancel: false,
                },
                place: DeepXPerpPlaceParams {
                    subaccount: [0x11; 20],
                    market_id: 7,
                    is_long: true,
                    size: 1_000_000,
                    price: 2_500_000_000,
                    order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                    take_profit: None,
                    stop_loss: None,
                    reduce_only: false,
                    post_only: DeepXPostOnlyParam::None,
                },
            },
            456,
        )
        .unwrap();
        assert_eq!(
            hex::encode(modify.bytes()),
            "f90284fcad0b19bb29d4674531d6f115237e16afce377ca1593280341f7ed9fb1672ebaee9bf19d6393f1bdc3b77f9546027c045c6af225fb1a34a0e742060da3e8eecd5c33381485577b42faf7c0924f38330724c50790000210700131e08010111111111111111111111111111111111111111117b00000000000000070000000001111111111111111111111111111111111111111107000140420f0000000000000000000000000000f90295000000000000000000000000000000000000",
        );
        assert!(
            no_op.has_valid_hash()
                && remark.has_valid_hash()
                && place.has_valid_hash()
                && cancel.has_valid_hash()
                && modify.has_valid_hash()
        );
    }

    #[rstest]
    fn perp_modify_signs_the_metadata_defined_transactional_call() {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet.call_variant_by_name("modify_orders").unwrap();
        assert_eq!(call.fields.len(), 1);
        assert_eq!(call.fields[0].name.as_deref(), Some("ops"));

        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_perp_modify_order(
            &permit,
            &key(),
            DeepXPerpModifyParams {
                cancel: DeepXPerpCancelParams {
                    subaccount: [0x11; 20],
                    order_id: 123,
                    market_id: 7,
                    fast_cancel: false,
                },
                place: DeepXPerpPlaceParams {
                    subaccount: [0x11; 20],
                    market_id: 7,
                    is_long: true,
                    size: 1_000_000,
                    price: 2_500_000_000,
                    order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                    take_profit: None,
                    stop_loss: None,
                    reduce_only: false,
                    post_only: DeepXPostOnlyParam::None,
                },
            },
            456,
        )
        .unwrap();

        assert_eq!(signed.nonce(), 456);
        assert!(signed.has_valid_hash());
        assert_eq!(
            hex::encode(signed.bytes()),
            "f90284fcad0b19bb29d4674531d6f115237e16afce377c97c9295eba3f392e4c9dee063c7169e2192aca3e17ebb55ae41237398246dd727e144de8af8c21c0213de9ecadc21402dcdcb158b20d0ecd2a41f512ee8d87430000210700131e08010111111111111111111111111111111111111111117b00000000000000070000000001111111111111111111111111111111111111111107000140420f0000000000000000000000000000f90295000000000000000000000000000000000000",
        );
    }

    #[rstest]
    #[case([0x11; 20], 8)]
    #[case([0x22; 20], 7)]
    fn perp_modify_rejects_mixed_market_or_subaccount_before_signing(
        #[case] place_subaccount: [u8; 20],
        #[case] place_market_id: u16,
    ) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let result = sign_perp_modify_order(
            &permit,
            &key(),
            DeepXPerpModifyParams {
                cancel: DeepXPerpCancelParams {
                    subaccount: [0x11; 20],
                    order_id: 123,
                    market_id: 7,
                    fast_cancel: false,
                },
                place: DeepXPerpPlaceParams {
                    subaccount: place_subaccount,
                    market_id: place_market_id,
                    is_long: true,
                    size: 1_000_000,
                    price: 2_500_000_000,
                    order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                    take_profit: None,
                    stop_loss: None,
                    reduce_only: false,
                    post_only: DeepXPostOnlyParam::None,
                },
            },
            456,
        );

        assert!(matches!(result, Err(SigningError::ModifyScopeMismatch)));
    }

    #[rstest]
    fn spec371_matches_live_python_sdk_vectors() {
        let response: RpcResponse = serde_json::from_str(include_str!(
            "../../test_data/runtime/testnet/genesis-86604388_metadata-4a620bad_spec-371_tx-1_finalized-64caaeda/metadata.json"
        )).unwrap();
        let bytes = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let old_snapshot = snapshot();
        let snapshot = RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            old_snapshot.identity().genesis_hash,
            371,
            1,
            &bytes,
        )
        .unwrap();
        assert_eq!(snapshot.identity().spec_version, 371);
        assert_eq!(
            hex::encode(snapshot.identity().metadata_sha256),
            "4a620bad9f2482b5511f4637d03d5e7907b903f4bab5af439f664963724cda15"
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let remark = sign_dynamic_pallet_call(
            &permit,
            &key(),
            "System",
            "remark",
            vec![Value::from_bytes(b"deepx-offline-signing-check")],
            1_725_000_000_123,
        )
        .unwrap();
        assert_eq!(
            hex::encode(remark.bytes()),
            "f50184fcad0b19bb29d4674531d6f115237e16afce377cc68afede7c39a8c51c9b56c4318e88613a966c209d559e9fcf68ad4da6b9160d56739bde6bb50780464211f37eeca92e514feefdb8bd8c643f47e2cc03deebc200000b7b2203a291010000006c64656570782d6f66666c696e652d7369676e696e672d636865636b",
        );
        let no_op = sign_no_op(&permit, &key(), 1_725_000_000_124).unwrap();
        assert_eq!(
            hex::encode(no_op.bytes()),
            "850184fcad0b19bb29d4674531d6f115237e16afce377cbd583a84ae2b8912fc6a7760233a6c695d0e58e18fdbcef984101c616747430b4029fcc67ca9dc844d2c42e524f3f5c6b986f08e9a3d7a23ab3af3ca4897f8f901000b7c2203a2910100131c",
        );
        let place = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: true,
                size: u128::MAX,
                price: u128::MAX,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                take_profit: Some(u128::MAX),
                stop_loss: None,
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(place.bytes()),
            "b90284fcad0b19bb29d4674531d6f115237e16afce377cb4fdb632655a86ec12b30c06130762cfc7d4773a4c74561e5fa072046ce144997e42b89c0fee251d46196cd0d7526adfaed6831c1dcfe893c9e8e174c7fb942301000b7d2203a291010016021111111111111111111111111111111111111111070001ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff000001ffffffffffffffffffffffffffffffff000000",
        );
        let limit_ioc = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: true,
                size: 1_000_000_000_000_000,
                price: 2_499_050_000,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Ioc),
                take_profit: None,
                stop_loss: None,
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(limit_ioc.bytes()),
            "790284fcad0b19bb29d4674531d6f115237e16afce377c55f33a8bfbd8f3dc02eca6182adf273f25b50703d938041e4d2dc6018d11f68b50e97da64fc6b52f9165032b893621b643c383e4f26cc5719306d29b0b55918401000b7d2203a2910100160211111111111111111111111111111111111111110700010080c6a47e8d03000000000000000000107af494000000000000000000000000000100000000",
        );
        let market_ioc = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: true,
                size: 1_000_000_000_000_000,
                price: 0,
                order_type: DeepXPerpOrderType::Market(None),
                take_profit: None,
                stop_loss: None,
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(market_ioc.bytes()),
            "790284fcad0b19bb29d4674531d6f115237e16afce377cca6ad7b9fc3b9b85a7040e0c1eb50f4aeca0b45c75393c676e28993d21e3c00622cd442f7ab6e9ca61d9de5a1adf2cef5ceeafe308f3ddb9f5928cab648dd5bf01000b7d2203a2910100160211111111111111111111111111111111111111110700010080c6a47e8d0300000000000000000000000000000000000000000000000000010000000000",
        );
        let post_only_reduce_only_sell = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: false,
                size: 1_000_000_000_000_000,
                price: 2_499_050_000,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                take_profit: None,
                stop_loss: None,
                reduce_only: true,
                post_only: DeepXPostOnlyParam::MustPostOnly,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(post_only_reduce_only_sell.bytes()),
            "790284fcad0b19bb29d4674531d6f115237e16afce377cea59477d5fb34533252188655b6412650435b035aaa831a67d93b20f71ac07177ab647c50fd953eb0d9ae949dc6b455d72e06cea2a1378edda2214fc2a8a35cf00000b7d2203a2910100160211111111111111111111111111111111111111110700000080c6a47e8d03000000000000000000107af494000000000000000000000000000000000101",
        );
        let cancel = sign_perp_cancel(
            &permit,
            &key(),
            DeepXPerpCancelParams {
                subaccount: [0x11; 20],
                order_id: 1_725_000_000_001,
                market_id: 7,
                fast_cancel: false,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(cancel.bytes()),
            "050284fcad0b19bb29d4674531d6f115237e16afce377c452661dbf696e0d16df0acff2587aa76059c511610f557c4d35971617441e5f07f62600861ac45973dace2c47d48605d0373e7a99177e64f9112498f1838852e00000b7d2203a291010016031111111111111111111111111111111111111111012203a29101000007000000",
        );
        let modify = sign_perp_modify_order(
            &permit,
            &key(),
            DeepXPerpModifyParams {
                cancel: DeepXPerpCancelParams {
                    subaccount: [0x11; 20],
                    order_id: 123,
                    market_id: 7,
                    fast_cancel: false,
                },
                place: DeepXPerpPlaceParams {
                    subaccount: [0x11; 20],
                    market_id: 7,
                    is_long: true,
                    size: 1_000_000,
                    price: 2_500_000_000,
                    order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                    take_profit: None,
                    stop_loss: None,
                    reduce_only: false,
                    post_only: DeepXPostOnlyParam::None,
                },
            },
            456,
        )
        .unwrap();
        assert_eq!(
            hex::encode(modify.bytes()),
            "f90284fcad0b19bb29d4674531d6f115237e16afce377cc77b308e4ce36835792428b14c4df3c54e5c514f2bfd40bb740bb147aaf4450d134c46cb097628c8c057ca2d7d7c90c7b4853d5ae053c6c498a13ee21c966bca0000210700131e08010111111111111111111111111111111111111111117b00000000000000070000000001111111111111111111111111111111111111111107000140420f0000000000000000000000000000f90295000000000000000000000000000000000000",
        );
        assert!(
            remark.has_valid_hash()
                && no_op.has_valid_hash()
                && place.has_valid_hash()
                && cancel.has_valid_hash()
                && modify.has_valid_hash()
        );
    }

    #[rstest]
    fn additional_order_calls_match_live_sdk_vectors() {
        let response: RpcResponse = serde_json::from_str(include_str!(
            "../../test_data/runtime/testnet/genesis-86604388_metadata-4a620bad_spec-371_tx-1_finalized-64caaeda/metadata.json"
        ))
        .unwrap();
        let bytes = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let old_snapshot = snapshot();
        let runtime = RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            old_snapshot.identity().genesis_hash,
            371,
            1,
            &bytes,
        )
        .unwrap();
        let service = DeepXRuntimeSnapshotService::new(runtime);
        let permit = service.acquire().unwrap();
        let spot = sign_spot_place_order(
            &permit,
            &key(),
            DeepXSpotPlaceParams {
                subaccount: [0x11; 20],
                pair: [0x22; 32],
                is_buy: true,
                quote_amount: [0x33; 32],
                base_amount: [0x44; 32],
                order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc),
                post_only: DeepXPostOnlyParam::None,
                reduce_only: true,
            },
            1_725_000_000_126,
        )
        .unwrap();
        assert_eq!(
            hex::encode(spot.bytes()),
            "690384fcad0b19bb29d4674531d6f115237e16afce377cc9d77c74cceef6627f758f3d669608a4ee257d855eef9da477cb2626c90195765cebb8d76d8d39429399d10a6b88c2f179ccfa68f61dc0981ec647da1d4945ba01000b7e2203a2910100140111111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222013333333333333333333333333333333333333333333333333333333333333333444444444444444444444444444444444444444444444444444444444444444400000001",
        );
        let spot_ioc = sign_spot_place_order(
            &permit,
            &key(),
            DeepXSpotPlaceParams {
                subaccount: [0x11; 20],
                pair: [0x22; 32],
                is_buy: true,
                quote_amount: [0x33; 32],
                base_amount: [0x44; 32],
                order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Ioc),
                post_only: DeepXPostOnlyParam::None,
                reduce_only: false,
            },
            1_725_000_000_133,
        )
        .unwrap();
        assert_eq!(
            hex::encode(spot_ioc.bytes()),
            "690384fcad0b19bb29d4674531d6f115237e16afce377cb1e12ee3ed17790541d63f5ce75084bccb5575060a6c94f9dd2e4bb350aa52921911fd47d4643d8e42eabae1ccb4460c00c432a27bf7b014181ae5c5424aa87701000b852203a2910100140111111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222013333333333333333333333333333333333333333333333333333333333333333444444444444444444444444444444444444444444444444444444444444444400010000",
        );
        let spot_ioc_sell = sign_spot_place_order(
            &permit,
            &key(),
            DeepXSpotPlaceParams {
                subaccount: [0x11; 20],
                pair: [0x22; 32],
                is_buy: false,
                quote_amount: [0x33; 32],
                base_amount: [0x44; 32],
                order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Ioc),
                post_only: DeepXPostOnlyParam::None,
                reduce_only: false,
            },
            1_725_000_000_134,
        )
        .unwrap();
        assert_eq!(
            hex::encode(spot_ioc_sell.bytes()),
            "690384fcad0b19bb29d4674531d6f115237e16afce377c809a3ba096d4a9064b053da0307c139f0ca8ec9414f43695d2c836be4490e0896d6797530f89684230a4e0d321e0734aae345dcd4cf19794656da269500efa9500000b862203a2910100140111111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222003333333333333333333333333333333333333333333333333333333333333333444444444444444444444444444444444444444444444444444444444444444400010000"
        );
        let spot_market_sell = sign_spot_place_order(
            &permit,
            &key(),
            DeepXSpotPlaceParams {
                subaccount: [0x11; 20],
                pair: [0x22; 32],
                is_buy: false,
                quote_amount: [0x33; 32],
                base_amount: [0x44; 32],
                order_type: DeepXSpotOrderType::Market(Some(50)),
                post_only: DeepXPostOnlyParam::None,
                reduce_only: false,
            },
            1_725_000_000_136,
        )
        .unwrap();
        assert_eq!(
            hex::encode(spot_market_sell.bytes()),
            "890384fcad0b19bb29d4674531d6f115237e16afce377c66ca1877b72d5a2971b82f10a3b3a72da3c520ee4a0583a5f7e244f6fbd2898f104af68f07afc47698674e6632ef8872087696ab9452d0a9f8abc31bf71d779801000b882203a29101001401111111111111111111111111111111111111111122222222222222222222222222222222222222222222222222222222222222220033333333333333333333333333333333333333333333333333333333333333334444444444444444444444444444444444444444444444444444444444444444010132000000000000000000",
        );
        let close = sign_perp_close(
            &permit,
            &key(),
            DeepXPerpCloseParams {
                subaccount: [0x11; 20],
                market_id: 7,
                price: 123,
                slippage: None,
            },
            1_725_000_000_127,
        )
        .unwrap();
        assert_eq!(
            hex::encode(close.bytes()),
            "210284fcad0b19bb29d4674531d6f115237e16afce377c87a8b3c22a0abb14d596f04842477750059ada9eb78f0c8c39366e149abed37034d1dde1f881afda42f881fbad1bbe7dcc0ade6390114b80dd21445f5398e9e701000b7f2203a2910100160e111111111111111111111111111111111111111107007b00000000000000000000000000000000",
        );
        let spot_cancel = sign_spot_cancel(
            &permit,
            &key(),
            DeepXSpotCancelParams {
                subaccount: [0x11; 20],
                pair: [0x22; 32],
                order_id: 1_725_000_000_128,
                is_buy: false,
                fast_cancel: false,
            },
            1_725_000_000_129,
        )
        .unwrap();
        assert_eq!(
            hex::encode(spot_cancel.bytes()),
            "810284fcad0b19bb29d4674531d6f115237e16afce377c6123c984492ce7bbe3177a19d693d8a8284bb0f63b36dcb89cfb0e3a2e99bae00ab801a969ba7df9e492e63b7179d42b145c014a4c81e5f985967f76a2e22ec001000b812203a2910100140311111111111111111111111111111111111111112222222222222222222222222222222222222222222222222222222222222222802203a291010000000000",
        );
        let pnl = sign_perp_set_profit_and_loss_point(
            &permit,
            &key(),
            DeepXPerpProfitAndLossPointParams {
                subaccount: [0x11; 20],
                market_id: 7,
                take_profit_point: 123,
                stop_loss_point: 456,
            },
            1_725_000_000_130,
        )
        .unwrap();
        assert_eq!(
            hex::encode(pnl.bytes()),
            "5d0284fcad0b19bb29d4674531d6f115237e16afce377ce67839366830606287ce288fd27c3ef4fa159d63fffd4c41ab8269279504653361da97fdf4ac2b895ec7191d8e0e239463dadf50c23ff85f082a6e05f565a4c300000b822203a2910100160d111111111111111111111111111111111111111107007b000000000000000000000000000000c8010000000000000000000000000000",
        );
        let delete_subaccount = sign_delete_subaccount(
            &permit,
            &key(),
            DeepXDeleteSubaccountParams {
                subaccount: [0x11; 20],
            },
            1_725_000_000_131,
        )
        .unwrap();
        assert_eq!(
            hex::encode(delete_subaccount.bytes()),
            "d50184fcad0b19bb29d4674531d6f115237e16afce377ce536bcc04e8cdcd9561fe62a5be6dace7c75c16841b8ba8974cabcaffe284d032af1ee81bd37e840bca645ab9075c7a7183a5bae4b8c3ff307d26c0b29d2df7800000b832203a291010013011111111111111111111111111111111111111111",
        );
        let remove_delegate = sign_remove_delegate_account(
            &permit,
            &key(),
            DeepXRemoveDelegateAccountParams {
                delegate: [0x22; 20],
            },
            1_725_000_000_132,
        )
        .unwrap();
        assert_eq!(
            hex::encode(remove_delegate.bytes()),
            "d50184fcad0b19bb29d4674531d6f115237e16afce377ca03e70bf0001a60c4d12230b4571571df8a83495f3bbb1c0b0366561383e7d9c40748912805d2a7ae5aced740e1cc65bbdf66ea094e2041c65c77c3dd554382b00000b842203a2910100131d2222222222222222222222222222222222222222",
        );
        assert!(
            spot.has_valid_hash()
                && close.has_valid_hash()
                && spot_cancel.has_valid_hash()
                && pnl.has_valid_hash()
                && delete_subaccount.has_valid_hash()
                && remove_delegate.has_valid_hash()
        );
    }

    #[rstest]
    fn perp_place_matches_independent_sdk_vector() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let signed = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: true,
                size: u128::MAX,
                price: u128::MAX,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                take_profit: Some(u128::MAX),
                stop_loss: None,
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
            1_725_000_000_125,
        )
        .unwrap();
        // Generated independently by the pinned SDK builder with captured spec366 metadata.
        assert_eq!(
            hex::encode(signed.bytes()),
            "b90284fcad0b19bb29d4674531d6f115237e16afce377c20f3fc4c9d8b313e32298602a7336f137e453ac098c95812f909033f5df1dba341ef8e7b770ef911213478b6361a4223cc117f1b5ec9545621ff1c66f6ba031700000b7d2203a291010016021111111111111111111111111111111111111111070001ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff000001ffffffffffffffffffffffffffffffff000000",
        );
        let limit_ioc = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: true,
                size: 1_000_000_000_000_000,
                price: 2_499_050_000,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Ioc),
                take_profit: None,
                stop_loss: None,
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(limit_ioc.bytes()),
            "790284fcad0b19bb29d4674531d6f115237e16afce377cf6d5e54464c67cbccc28cb70bf6b231df7a4ffe3e5877db847dc289a7e83a364144a9ef7b823692163362914158c80b60e59e435196f83d95e2fe35040bb3f4a01000b7d2203a2910100160211111111111111111111111111111111111111110700010080c6a47e8d03000000000000000000107af494000000000000000000000000000100000000",
        );
        let market_ioc = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: true,
                size: 1_000_000_000_000_000,
                price: 0,
                order_type: DeepXPerpOrderType::Market(None),
                take_profit: None,
                stop_loss: None,
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(market_ioc.bytes()),
            "790284fcad0b19bb29d4674531d6f115237e16afce377cc874f7ea0d5c726894ff8b6521ca77a047f1642f909a6830bcb151b3e79bc942236059799d655fcc7030aaf6810693f7e6cdacbf454358b9a84425ebb0fd77c200000b7d2203a2910100160211111111111111111111111111111111111111110700010080c6a47e8d0300000000000000000000000000000000000000000000000000010000000000",
        );
        let post_only_reduce_only_sell = sign_perp_place_order(
            &permit,
            &key(),
            DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 7,
                is_long: false,
                size: 1_000_000_000_000_000,
                price: 2_499_050_000,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                take_profit: None,
                stop_loss: None,
                reduce_only: true,
                post_only: DeepXPostOnlyParam::MustPostOnly,
            },
            1_725_000_000_125,
        )
        .unwrap();
        assert_eq!(
            hex::encode(post_only_reduce_only_sell.bytes()),
            "790284fcad0b19bb29d4674531d6f115237e16afce377ccd5ab655383a2dc7cfbc91d192e7b96fdd96576430637b17d5a88ccf6ae6beb4215b2b0a966ac5e4f1a1ed1cd0193e4fe33bea511d1ae535de4bc545de7f181500000b7d2203a2910100160211111111111111111111111111111111111111110700000080c6a47e8d03000000000000000000107af494000000000000000000000000000000000101",
        );
        assert!(signed.has_valid_hash());
    }

    #[rstest]
    fn dynamic_signing_matches_fixed_testnet_regression_vector() {
        let snapshot = snapshot();
        let payload = subxt_core::dynamic::tx(
            "System",
            "remark",
            vec![Value::from_bytes(b"deepx-offline-signing-check")],
        );
        let params = DefaultExtrinsicParamsBuilder::<DeepXRuntimeConfig>::new()
            .nonce(1_725_000_000_123)
            .build();
        let signer_payload = tx::create_v4_signed(&payload, snapshot.client_state(), params)
            .unwrap()
            .signer_payload();
        assert_eq!(
            hex::encode(signer_payload),
            "00006c64656570782d6f66666c696e652d7369676e696e672d636865636b000b7b2203a29101006e0100000100000086604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b",
        );

        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let first = sign_dynamic_pallet_call(
            &permit,
            &key(),
            "System",
            "remark",
            vec![Value::from_bytes(b"deepx-offline-signing-check")],
            1_725_000_000_123,
        )
        .unwrap();
        let second = sign_dynamic_pallet_call(
            &permit,
            &key(),
            "System",
            "remark",
            vec![Value::from_bytes(b"deepx-offline-signing-check")],
            1_725_000_000_123,
        )
        .unwrap();

        assert_eq!(first, second);
        assert_eq!(
            hex::encode(first.signer()),
            "fcad0b19bb29d4674531d6f115237e16afce377c",
        );
        assert_eq!(first.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(
            hex::encode(first.bytes()),
            "f50184fcad0b19bb29d4674531d6f115237e16afce377ca524ebcf1d41cd1079a3bee1eb1b25d2ac0473e5546faa6e8fb828389f795ee933fece23b7435b4b58435d775b7d97e7430485a1ff25eca05b3ab6626f43cb9c01000b7b2203a291010000006c64656570782d6f66666c696e652d7369676e696e672d636865636b",
        );
        assert_eq!(
            hex::encode(first.extrinsic_hash()),
            "9695ee4aa7ac14ad58b7e5fd850bf6f0648df811a653c5486ba6146365f7de19",
        );
    }

    #[rstest]
    fn no_op_signing_matches_fixed_testnet_regression_vector() {
        let snapshot = snapshot();
        let payload = subxt_core::dynamic::tx("Subaccount", "no_op", Vec::<Value>::new());
        let params = DefaultExtrinsicParamsBuilder::<DeepXRuntimeConfig>::new()
            .nonce(1_725_000_000_124)
            .build();
        let signer_payload = tx::create_v4_signed(&payload, snapshot.client_state(), params)
            .unwrap()
            .signer_payload();
        assert_eq!(
            hex::encode(signer_payload),
            "131c000b7c2203a29101006e0100000100000086604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b",
        );

        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_no_op(&permit, &key(), 1_725_000_000_124).unwrap();
        assert_eq!(
            hex::encode(signed.signer()),
            "fcad0b19bb29d4674531d6f115237e16afce377c",
        );
        assert_eq!(
            hex::encode(signed.bytes()),
            "850184fcad0b19bb29d4674531d6f115237e16afce377c96c788579cf2cdda606129a7fe59b763031ddc0d19056af3c1585674b0a64d4928e9e1c787b283a9467c2207afa5c6338dd1d671df0b3aec5edb5128e79981e301000b7c2203a2910100131c",
        );
        assert_eq!(
            hex::encode(signed.extrinsic_hash()),
            "c2e2837a583ddf1e94fd0a5a177c4b6e471ac24b82a7e9fd32d600c0f674d28a",
        );
    }

    #[rstest]
    #[case::empty(Vec::new(), 0)]
    #[case::compact_boundary(vec![0x41; 32], 64)]
    #[case::label(b"Desk A".to_vec(), 63)]
    fn initialize_subaccount_exact_scale(#[case] name: Vec<u8>, #[case] nonce: u64) {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet
            .call_variant_by_name("initialize_subaccount")
            .unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 0);
        assert_eq!(call.fields.len(), 1);
        assert_eq!(call.fields[0].name.as_deref(), Some("name"));
        let params = DeepXInitializeSubaccountParams { name };
        let payload = subxt_core::dynamic::tx(
            "Subaccount",
            "initialize_subaccount",
            params.clone().into_dynamic_arguments(),
        );
        let mut expected = vec![19, 0];
        expected.extend(params.name.encode());
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
            expected,
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_initialize_subaccount(&permit, &key(), params.clone(), nonce).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(
            signed,
            sign_initialize_subaccount(&permit, &key(), params, nonce).unwrap(),
        );
    }

    #[rstest]
    fn initialize_subaccount_rejects_name_above_runtime_bound() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        assert!(matches!(
            sign_initialize_subaccount(
                &permit,
                &key(),
                DeepXInitializeSubaccountParams { name: vec![0; 33] },
                0,
            ),
            Err(SigningError::SubaccountNameTooLong { length: 33 })
        ));
    }

    #[rstest]
    fn initialize_subaccount_matches_pinned_sdk_spec371_vector() {
        let metadata: RpcResponse = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-86604388_metadata-4a620bad_spec-371_tx-1_finalized-64caaeda/metadata.json",
        )))
        .unwrap();
        let bytes = hex::decode(metadata.result.trim_start_matches("0x")).unwrap();
        let snapshot = RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            hex::decode_array("86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b")
                .unwrap(),
            371,
            1,
            &bytes,
        )
        .unwrap();
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_initialize_subaccount(
            &permit,
            &key(),
            DeepXInitializeSubaccountParams {
                name: b"Desk A".to_vec(),
            },
            1_725_000_000_137,
        )
        .unwrap();
        assert_eq!(
            hex::encode(signed.bytes()),
            "a10184fcad0b19bb29d4674531d6f115237e16afce377ce47df873811273afd909fca7b328f5057d80c69a35201af8af3a3443e725c3c62e96c9d472109872e5476e32ccd252a8cafa4cbc5d16be9b3c07ec398d95d79e00000b892203a29101001300184465736b2041",
        );
        assert!(signed.has_valid_hash());
    }

    #[rstest]
    #[case::spec366(
        366,
        "f10184fcad0b19bb29d4674531d6f115237e16afce377cb0ae4b143dafa3623ea2f265488d1bc58ce76ade03b04377e82fde6250830c611dbbfcaca46ceec3baa0588ed466c47813f9363ce2bcf3d8b2617c59ebb3962f00000b8e2203a291010013071111111111111111111111111111111111111111184465736b2042"
    )]
    #[case::spec369(
        369,
        "f10184fcad0b19bb29d4674531d6f115237e16afce377c4ea90a4f1720b8639b3796f1aa6da19e4ccdce11ca6601301504582774958db44b08d6eba7b1fcf6a283bd7354068d6c9ed7e2288c076cc1f118e919d2b51b9e01000b8e2203a291010013071111111111111111111111111111111111111111184465736b2042"
    )]
    #[case::spec371(
        371,
        "f10184fcad0b19bb29d4674531d6f115237e16afce377c364cede33287f7f1b06168470717edd10467d0f1a8396c01cbed9e9f3a43d3ad6b695285ba8e03272c1c865127ee69557e18069197b4dfcc3e408e49a43eb89c01000b8e2203a291010013071111111111111111111111111111111111111111184465736b2042"
    )]
    fn rename_subaccount_matches_metadata_and_pinned_sdk_vectors(
        #[case] spec_version: u32,
        #[case] expected_signed: &str,
    ) {
        let snapshot = match spec_version {
            366 => snapshot(),
            369 => snapshot_spec369(),
            371 => snapshot_spec371(),
            _ => unreachable!(),
        };
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet.call_variant_by_name("rename_subaccount").unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 7);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["subaccount", "new_name"]
        );
        let params = DeepXRenameSubaccountParams {
            subaccount: [0x11; 20],
            new_name: b"Desk B".to_vec(),
        };
        let payload = subxt_core::dynamic::tx(
            "Subaccount",
            "rename_subaccount",
            params.clone().into_dynamic_arguments(),
        );
        let mut expected = vec![19, 7];
        expected.extend_from_slice(&params.subaccount);
        expected.extend(params.new_name.encode());
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
            expected,
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let nonce = 1_725_000_000_142;
        let signed = sign_rename_subaccount(&permit, &key(), params.clone(), nonce).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert_eq!(hex::encode(signed.bytes()), expected_signed,);
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(
            signed,
            sign_rename_subaccount(&permit, &key(), params, nonce).unwrap()
        );
    }

    #[rstest]
    fn rename_subaccount_rejects_name_above_runtime_bound() {
        let service = DeepXRuntimeSnapshotService::new(snapshot_spec371());
        let permit = service.acquire().unwrap();
        assert!(
            sign_rename_subaccount(
                &permit,
                &key(),
                DeepXRenameSubaccountParams {
                    subaccount: [0x11; 20],
                    new_name: vec![0x41; 32],
                },
                0,
            )
            .is_ok()
        );
        assert!(matches!(
            sign_rename_subaccount(
                &permit,
                &key(),
                DeepXRenameSubaccountParams {
                    subaccount: [0x11; 20],
                    new_name: vec![0; 33],
                },
                0,
            ),
            Err(SigningError::SubaccountNameTooLong { length: 33 })
        ));
    }

    #[rstest]
    #[case::empty(Vec::new(), 0, 0)]
    #[case::compact_boundary(vec![0x41; 64], u64::MAX, 64)]
    #[case::label(b"Desk A".to_vec(), 1_900_000_000_000, 63)]
    fn set_delegate_account_exact_scale(
        #[case] name: Vec<u8>,
        #[case] valid_until: u64,
        #[case] nonce: u64,
    ) {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet.call_variant_by_name("set_delegate_account").unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 2);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| (field.name.as_deref(), field.type_name.as_deref()))
                .collect::<Vec<_>>(),
            [
                (Some("delegate"), Some("H160")),
                (Some("name"), Some("Vec<u8>")),
                (Some("valid_until"), Some("u64")),
            ]
        );
        let params = DeepXSetDelegateAccountParams {
            delegate: [0x22; 20],
            name,
            valid_until,
        };
        let payload = subxt_core::dynamic::tx(
            "Subaccount",
            "set_delegate_account",
            params.clone().into_dynamic_arguments(),
        );
        let mut expected = vec![19, 2];
        expected.extend_from_slice(&params.delegate);
        expected.extend(params.name.encode());
        expected.extend(params.valid_until.encode());
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
            expected,
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_set_delegate_account(&permit, &key(), params.clone(), nonce).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(
            signed,
            sign_set_delegate_account(&permit, &key(), params.clone(), nonce).unwrap()
        );
        let mut changed = params;
        changed.valid_until ^= 1;
        assert_ne!(
            signed.extrinsic_hash(),
            sign_set_delegate_account(&permit, &key(), changed, nonce)
                .unwrap()
                .extrinsic_hash()
        );
    }

    #[rstest]
    #[case(
        366,
        "110284fcad0b19bb29d4674531d6f115237e16afce377cd4f874caee0b3c5b2aa3d39107dec915307992a4193476e06f9cf9d35e2621f662f6e160a9b3399a7248115de472b3ac12d58cd53963337dc0a245c94ed1999101000b852203a291010013022222222222222222222222222222222222222222184465736b20410038d360ba010000"
    )]
    #[case(
        369,
        "110284fcad0b19bb29d4674531d6f115237e16afce377c998bf220f91e93247c558b925781324e9e920ace05a9549faba56bea5741faf5535a44e45cd5622695364324095e28484767745f02f26938ef592ee11939f7b701000b852203a291010013022222222222222222222222222222222222222222184465736b20410038d360ba010000"
    )]
    #[case(
        371,
        "110284fcad0b19bb29d4674531d6f115237e16afce377cd6ab2255195a425abd7c9effec82f193427a790a1bfa75a5b4e20c105b184c6025eba400662a5bef284aee1370959523ec70a97389ff641cdf6321fe32a3c9ae00000b852203a291010013022222222222222222222222222222222222222222184465736b20410038d360ba010000"
    )]
    fn set_delegate_account_matches_pinned_sdk_approved_runtimes(
        #[case] spec_version: u32,
        #[case] expected: &str,
    ) {
        let metadata: RpcResponse = serde_json::from_str(match spec_version {
            366 => include_str!(
                "../../test_data/runtime/testnet/genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json"
            ),
            369 => include_str!(
                "../../test_data/runtime/testnet/genesis-86604388_metadata-98136fdb_spec-369_tx-1_finalized-95febbff/metadata.json"
            ),
            371 => include_str!(
                "../../test_data/runtime/testnet/genesis-86604388_metadata-4a620bad_spec-371_tx-1_finalized-64caaeda/metadata.json"
            ),
            _ => unreachable!(),
        })
        .unwrap();
        let bytes = hex::decode(metadata.result.trim_start_matches("0x")).unwrap();
        let snapshot = RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            hex::decode_array("86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b")
                .unwrap(),
            spec_version,
            1,
            &bytes,
        )
        .unwrap();
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_set_delegate_account(
            &permit,
            &key(),
            DeepXSetDelegateAccountParams {
                delegate: [0x22; 20],
                name: b"Desk A".to_vec(),
                valid_until: 1_900_000_000_000,
            },
            1_725_000_000_133,
        )
        .unwrap();
        assert_eq!(hex::encode(signed.bytes()), expected);
        assert!(signed.has_valid_hash());
    }

    #[rstest]
    fn set_delegate_account_rejects_malformed_dynamic_arguments() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        for arguments in [
            vec![
                Value::from_bytes([0; 19]),
                Value::from_bytes(b"Desk A"),
                Value::u128(1),
            ],
            vec![
                Value::from_bytes([0; 20]),
                Value::bool(true),
                Value::u128(1),
            ],
            vec![
                Value::from_bytes([0; 20]),
                Value::from_bytes(b"Desk A"),
                Value::u128(u128::from(u64::MAX) + 1),
            ],
        ] {
            assert!(matches!(
                sign_dynamic_pallet_call(
                    &permit,
                    &key(),
                    "Subaccount",
                    "set_delegate_account",
                    arguments,
                    0,
                ),
                Err(SigningError::Encode(_))
            ));
        }
    }

    #[rstest]
    #[case::trading(DeepXWritableDelegateMode::PlaceOrCancelOrder, 0, 63)]
    #[case::disabled(DeepXWritableDelegateMode::Disable, 3, 64)]
    fn update_delegate_mode_exact_scale(
        #[case] new_mode: DeepXWritableDelegateMode,
        #[case] variant_index: u8,
        #[case] nonce: u64,
    ) {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet.call_variant_by_name("update_delegate_mode").unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 4);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| (field.name.as_deref(), field.type_name.as_deref()))
                .collect::<Vec<_>>(),
            [
                (Some("address"), Some("H160")),
                (Some("new_mode"), Some("DelegateMode")),
            ]
        );
        let params = DeepXUpdateDelegateModeParams {
            address: [0x22; 20],
            new_mode,
        };
        let payload = subxt_core::dynamic::tx(
            "Subaccount",
            "update_delegate_mode",
            params.into_dynamic_arguments(),
        );
        let mut expected = vec![19, 4];
        expected.extend_from_slice(&params.address);
        expected.push(variant_index);
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
            expected,
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_update_delegate_mode(&permit, &key(), params, nonce).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(
            signed,
            sign_update_delegate_mode(&permit, &key(), params, nonce).unwrap()
        );
    }

    #[rstest]
    #[case::trading(
        366,
        DeepXWritableDelegateMode::PlaceOrCancelOrder,
        1_725_000_000_138,
        "d90184fcad0b19bb29d4674531d6f115237e16afce377cf8c18a45c4f6f0d5e9c99db0bddace0ab6756e4e41a4f1b4f390057259e1a735074a61443d0a8aaea5b70c81e680d9e4a2f307aadde1f41947e149917dd7642d00000b8a2203a29101001304222222222222222222222222222222222222222200"
    )]
    #[case::disabled(
        366,
        DeepXWritableDelegateMode::Disable,
        1_725_000_000_139,
        "d90184fcad0b19bb29d4674531d6f115237e16afce377c93a0f83954052e2f63fd7da8396a670f8eb0ab9109916babd6fa3d4cc462f95d4813901d85615dc1689e667d2d8d0a9e4e02ce4ca3d8d33823b53acf4c0153af01000b8b2203a29101001304222222222222222222222222222222222222222203"
    )]
    #[case::trading_369(
        369,
        DeepXWritableDelegateMode::PlaceOrCancelOrder,
        1_725_000_000_138,
        "d90184fcad0b19bb29d4674531d6f115237e16afce377c5ca1e79b4048ec776a3cb07009ed397bf4dc9ea14184637380f27fadc5a4e8c838fe017673a159e2c696633357306974b3b45b5a92165a6eb56d02543db4143701000b8a2203a29101001304222222222222222222222222222222222222222200"
    )]
    #[case::disabled_369(
        369,
        DeepXWritableDelegateMode::Disable,
        1_725_000_000_139,
        "d90184fcad0b19bb29d4674531d6f115237e16afce377c90b5a60d71be9076cd6cfcfc0b7c7d0d3fca47380a9593a7da7e71ab2beb068b371df9d8c2da6c11245c1667e89e6ee9b0df5e2b1d4d9fcd2320997341a5d32b00000b8b2203a29101001304222222222222222222222222222222222222222203"
    )]
    #[case::trading_371(
        371,
        DeepXWritableDelegateMode::PlaceOrCancelOrder,
        1_725_000_000_138,
        "d90184fcad0b19bb29d4674531d6f115237e16afce377ce86633a8048fe092fd39fe893baf37f79d548d7712e63220fcccd12bace72fb11799012a7d3120073a837862dfb810c63f2de0598004d819ad2aa7a09322fe7001000b8a2203a29101001304222222222222222222222222222222222222222200"
    )]
    #[case::disabled_371(
        371,
        DeepXWritableDelegateMode::Disable,
        1_725_000_000_139,
        "d90184fcad0b19bb29d4674531d6f115237e16afce377cd812da37ae31ab9c9d7186c9351595e78eb8b96df05edfdccd9cf506cda7e29a2a6be8c94188f945bcc0d3927f160c491a44677ad423d33f3c6d10d02a632be900000b8b2203a29101001304222222222222222222222222222222222222222203"
    )]
    fn update_delegate_mode_matches_pinned_sdk_approved_runtimes(
        #[case] spec_version: u32,
        #[case] new_mode: DeepXWritableDelegateMode,
        #[case] nonce: u64,
        #[case] expected: &str,
    ) {
        let metadata: RpcResponse = serde_json::from_str(match spec_version {
            366 => include_str!(
                "../../test_data/runtime/testnet/genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json"
            ),
            369 => include_str!(
                "../../test_data/runtime/testnet/genesis-86604388_metadata-98136fdb_spec-369_tx-1_finalized-95febbff/metadata.json"
            ),
            371 => include_str!(
                "../../test_data/runtime/testnet/genesis-86604388_metadata-4a620bad_spec-371_tx-1_finalized-64caaeda/metadata.json"
            ),
            _ => unreachable!(),
        })
        .unwrap();
        let bytes = hex::decode(metadata.result.trim_start_matches("0x")).unwrap();
        let snapshot = RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            hex::decode_array("86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b")
                .unwrap(),
            spec_version,
            1,
            &bytes,
        )
        .unwrap();
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_update_delegate_mode(
            &permit,
            &key(),
            DeepXUpdateDelegateModeParams {
                address: [0x22; 20],
                new_mode,
            },
            nonce,
        )
        .unwrap();
        assert_eq!(hex::encode(signed.bytes()), expected);
        assert!(signed.has_valid_hash());
    }

    #[rstest]
    fn update_delegate_mode_rejects_unknown_dynamic_variant() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        assert!(matches!(
            sign_dynamic_pallet_call(
                &permit,
                &key(),
                "Subaccount",
                "update_delegate_mode",
                vec![
                    Value::from_bytes([0x22; 20]),
                    Value::unnamed_variant("Unknown", Vec::<Value>::new()),
                ],
                0,
            ),
            Err(SigningError::Encode(_))
        ));
    }

    #[rstest]
    #[case(
        366,
        "d50184fcad0b19bb29d4674531d6f115237e16afce377c8166060926b5b559089ecac98f9cb47b9a6b0b860651870eb4cb2933c757b2b22431044dd36b84f83781e86a2e55024087ec033aa4d01ab7a4ad7d03ae2b8cfc00000b842203a2910100131d2222222222222222222222222222222222222222"
    )]
    #[case(
        369,
        "d50184fcad0b19bb29d4674531d6f115237e16afce377c8c7922e7a0f874797e550871a94dc00133e6d84a9d080c23c737feb77efc7de5331dcf0adf778d0e7889a0c8956194cd3df5f570af82ee64877417a6aebd316f00000b842203a2910100131d2222222222222222222222222222222222222222"
    )]
    fn remove_delegate_matches_pinned_sdk_approved_runtimes(
        #[case] spec_version: u32,
        #[case] expected: &str,
    ) {
        let metadata: RpcResponse = serde_json::from_str(match spec_version {
            366 => include_str!(
                "../../test_data/runtime/testnet/genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json"
            ),
            369 => include_str!(
                "../../test_data/runtime/testnet/genesis-86604388_metadata-98136fdb_spec-369_tx-1_finalized-95febbff/metadata.json"
            ),
            _ => unreachable!(),
        })
        .unwrap();
        let bytes = hex::decode(metadata.result.trim_start_matches("0x")).unwrap();
        let runtime = RuntimeSnapshot::approved_testnet(
            &crate::common::DeepXEnvironment::Testnet,
            snapshot().identity().genesis_hash,
            spec_version,
            1,
            &bytes,
        )
        .unwrap();
        let permit = DeepXRuntimeSnapshotService::new(runtime).acquire().unwrap();
        let signed = sign_remove_delegate_account(
            &permit,
            &key(),
            DeepXRemoveDelegateAccountParams {
                delegate: [0x22; 20],
            },
            1_725_000_000_132,
        )
        .unwrap();
        assert_eq!(hex::encode(signed.bytes()), expected);
    }

    #[rstest]
    #[case([0; 20], 0)]
    #[case([0xff; 20], u64::MAX)]
    #[case([0x11; 20], 63)]
    #[case([0x22; 20], 64)]
    fn remove_delegate_exact_scale(#[case] delegate: [u8; 20], #[case] nonce: u64) {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet
            .call_variant_by_name("remove_delegate_account")
            .unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 29);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| (field.name.as_deref(), field.type_name.as_deref()))
                .collect::<Vec<_>>(),
            [(Some("delegate"), Some("H160"))]
        );
        let params = DeepXRemoveDelegateAccountParams { delegate };
        let payload = subxt_core::dynamic::tx(
            "Subaccount",
            "remove_delegate_account",
            params.into_dynamic_arguments(),
        );
        let mut expected = vec![19, 29];
        expected.extend_from_slice(&delegate);
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
            expected
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_remove_delegate_account(&permit, &key(), params, nonce).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(
            signed,
            sign_remove_delegate_account(&permit, &key(), params, nonce).unwrap()
        );
        let mut changed = params;
        changed.delegate[19] ^= 1;
        let other_key = DeepXPrivateKey::new(
            "0x1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        for altered in [
            sign_remove_delegate_account(&permit, &key(), changed, nonce).unwrap(),
            sign_remove_delegate_account(&permit, &key(), params, nonce ^ 1).unwrap(),
            sign_remove_delegate_account(&permit, &other_key, params, nonce).unwrap(),
        ] {
            assert_ne!(signed.bytes(), altered.bytes());
            assert_ne!(signed.extrinsic_hash(), altered.extrinsic_hash());
        }
    }

    #[rstest]
    #[case(Value::from_bytes([0; 19]))]
    #[case(Value::from_bytes([0; 21]))]
    #[case(Value::bool(true))]
    fn remove_delegate_malformed_arguments(#[case] value: Value) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        assert!(matches!(
            sign_dynamic_pallet_call(
                &permit,
                &key(),
                "Subaccount",
                "remove_delegate_account",
                vec![value],
                0,
            ),
            Err(SigningError::Encode(_))
        ));
    }

    #[rstest]
    fn remove_delegate_runtime_change_blocks_new_permit() {
        let snapshot = snapshot();
        let mut changed = snapshot.identity().clone();
        changed.transaction_version += 1;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        service.observe_runtime_identity(changed).unwrap();
        assert!(service.acquire().is_err());
        assert!(
            sign_remove_delegate_account(
                &permit,
                &key(),
                DeepXRemoveDelegateAccountParams {
                    delegate: [0x11; 20]
                },
                0,
            )
            .is_ok()
        );
    }

    #[rstest]
    fn perp_close_runtime_change_blocks_new_permit() {
        let snapshot = snapshot();
        let mut changed = snapshot.identity().clone();
        changed.spec_version += 1;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        service.observe_runtime_identity(changed).unwrap();
        assert!(service.acquire().is_err());
    }

    #[rstest]
    #[case(0, 0, 0)]
    #[case(u16::MAX, u128::MAX, u128::MAX)]
    #[case(7, 123, 456)]
    fn perp_points_exact_scale(
        #[case] market_id: u16,
        #[case] take_profit_point: u128,
        #[case] stop_loss_point: u128,
    ) {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("PerpMarket").unwrap();
        let call = pallet
            .call_variant_by_name("set_profit_and_loss_point")
            .unwrap();
        assert_eq!(pallet.index(), 22);
        assert_eq!(call.index, 13);
        assert!(pallet.call_variant_by_name("modify_order").is_none());
        assert_eq!(
            call.fields
                .iter()
                .map(|field| (field.name.as_deref(), field.type_name.as_deref()))
                .collect::<Vec<_>>(),
            [
                (Some("subaccount"), Some("H160")),
                (Some("market_id"), Some("u16")),
                (Some("take_profit_point"), Some("u128")),
                (Some("stop_loss_point"), Some("u128")),
            ]
        );
        let params = DeepXPerpProfitAndLossPointParams {
            subaccount: [0x11; 20],
            market_id,
            take_profit_point,
            stop_loss_point,
        };
        let mut expected = vec![22, 13];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&market_id.to_le_bytes());
        expected.extend_from_slice(&take_profit_point.to_le_bytes());
        expected.extend_from_slice(&stop_loss_point.to_le_bytes());
        let payload = subxt_core::dynamic::tx(
            "PerpMarket",
            "set_profit_and_loss_point",
            params.into_dynamic_arguments(),
        );
        assert_eq!(
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap(),
            expected
        );
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed =
            sign_perp_set_profit_and_loss_point(&permit, &key(), params, u64::MAX).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), u64::MAX);
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(
            signed,
            sign_perp_set_profit_and_loss_point(&permit, &key(), params, u64::MAX).unwrap()
        );
    }

    #[rstest]
    #[case::spec366(
        366,
        "5d0284fcad0b19bb29d4674531d6f115237e16afce377c1ff3f63618040c6efd4ad72fd01c1a94c59427accc446a7506a1ad9414542912480fabba84cd0b197befb291a298a38875bd6f84942c1eccf967bbb321fb1e3501000b822203a2910100160d111111111111111111111111111111111111111107007b000000000000000000000000000000c8010000000000000000000000000000"
    )]
    #[case::spec369(
        369,
        "5d0284fcad0b19bb29d4674531d6f115237e16afce377ca26d7048ee2f547a21f0f2ace0140d0af221042bc5de94a657a0e8b9607af25e67c7d396afa6d49eb5754fc77fc21e8083d0bc0f445c96ae1f73145b47f26bd401000b822203a2910100160d111111111111111111111111111111111111111107007b000000000000000000000000000000c8010000000000000000000000000000"
    )]
    #[case::spec371(
        371,
        "5d0284fcad0b19bb29d4674531d6f115237e16afce377ce67839366830606287ce288fd27c3ef4fa159d63fffd4c41ab8269279504653361da97fdf4ac2b895ec7191d8e0e239463dadf50c23ff85f082a6e05f565a4c300000b822203a2910100160d111111111111111111111111111111111111111107007b000000000000000000000000000000c8010000000000000000000000000000"
    )]
    fn perp_points_match_pinned_sdk_across_approved_runtimes(
        #[case] spec_version: u32,
        #[case] expected: &str,
    ) {
        let snapshot = match spec_version {
            366 => snapshot(),
            369 => snapshot_spec369(),
            371 => snapshot_spec371(),
            _ => unreachable!(),
        };
        let permit = DeepXRuntimeSnapshotService::new(snapshot)
            .acquire()
            .unwrap();
        let signed = sign_perp_set_profit_and_loss_point(
            &permit,
            &key(),
            DeepXPerpProfitAndLossPointParams {
                subaccount: [0x11; 20],
                market_id: 7,
                take_profit_point: 123,
                stop_loss_point: 456,
            },
            1_725_000_000_130,
        )
        .unwrap();

        assert_eq!(hex::encode(signed.bytes()), expected);
        assert!(signed.has_valid_hash());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    fn perp_points_signed_identity_sensitivity(#[case] mutation: u8) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut params = DeepXPerpProfitAndLossPointParams {
            subaccount: [0x11; 20],
            market_id: 7,
            take_profit_point: 123,
            stop_loss_point: 456,
        };
        let original = sign_perp_set_profit_and_loss_point(&permit, &key(), params, 125).unwrap();
        let mut nonce = 125;
        match mutation {
            0 => params.subaccount[19] ^= 1,
            1 => params.market_id += 1,
            2 => params.take_profit_point += 1,
            3 => params.stop_loss_point += 1,
            4 => nonce += 1,
            _ => unreachable!(),
        }
        let changed = sign_perp_set_profit_and_loss_point(&permit, &key(), params, nonce).unwrap();
        assert_ne!(original.bytes(), changed.bytes());
        assert_ne!(original.extrinsic_hash(), changed.extrinsic_hash());
    }

    #[rstest]
    #[case(0, Value::from_bytes([0x11; 19]))]
    #[case(1, Value::u128(u128::from(u16::MAX) + 1))]
    #[case(2, Value::bool(true))]
    #[case(3, Value::unnamed_variant("None", Vec::<Value>::new()))]
    fn perp_points_malformed_arguments(#[case] index: usize, #[case] value: Value) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut arguments = DeepXPerpProfitAndLossPointParams {
            subaccount: [0x11; 20],
            market_id: 7,
            take_profit_point: 123,
            stop_loss_point: 456,
        }
        .into_dynamic_arguments();
        arguments[index] = value;
        assert!(matches!(
            sign_dynamic_pallet_call(
                &permit,
                &key(),
                "PerpMarket",
                "set_profit_and_loss_point",
                arguments,
                125
            ),
            Err(SigningError::Encode(_))
        ));
    }

    #[rstest]
    fn perp_points_runtime_change_blocks_new_permit() {
        let snapshot = snapshot();
        let mut changed = snapshot.identity().clone();
        changed.spec_version += 1;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        service.observe_runtime_identity(changed).unwrap();
        assert!(service.acquire().is_err());
        let params = DeepXPerpProfitAndLossPointParams {
            subaccount: [0x11; 20],
            market_id: 7,
            take_profit_point: 123,
            stop_loss_point: 456,
        };
        assert!(sign_perp_set_profit_and_loss_point(&permit, &key(), params, 125).is_ok());
    }

    #[rstest]
    #[case(0, 0, None)]
    #[case(u16::MAX, u128::MAX, Some(u64::MAX))]
    #[case(7, 123, Some(0))]
    fn perp_close_exact_scale(
        #[case] market_id: u16,
        #[case] price: u128,
        #[case] slippage: Option<u64>,
    ) {
        let snapshot = snapshot();
        let metadata = snapshot.metadata();
        let pallet = metadata.pallet_by_name("PerpMarket").unwrap();
        let call = pallet.call_variant_by_name("close_position").unwrap();
        assert_eq!(call.index, 14);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| (field.name.as_deref(), field.type_name.as_deref()))
                .collect::<Vec<_>>(),
            [
                (Some("subaccount"), Some("H160")),
                (Some("market_id"), Some("u16")),
                (Some("price"), Some("u128")),
                (Some("slippage"), Some("Option<u64>"))
            ]
        );
        let params = DeepXPerpCloseParams {
            subaccount: [0x11; 20],
            market_id,
            price,
            slippage,
        };
        let mut expected = vec![pallet.index(), call.index];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&market_id.to_le_bytes());
        expected.extend_from_slice(&price.to_le_bytes());
        expected.push(u8::from(slippage.is_some()));
        if let Some(value) = slippage {
            expected.extend_from_slice(&value.to_le_bytes());
        }
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_perp_close(&permit, &key(), params, u64::MAX).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(signed.nonce(), u64::MAX);
        assert_eq!(
            signed,
            sign_perp_close(&permit, &key(), params, u64::MAX).unwrap()
        );
    }

    #[rstest]
    #[case::spec366_none(
        366,
        None,
        1_725_000_000_127,
        "210284fcad0b19bb29d4674531d6f115237e16afce377c147d6e346c65ccb9663f4c0091b47db0c17169c61a0c25eb777cfedf2875221c61adc8a02fc5509a6bc9b6ec020d53e07d1517ebf93ef452ccc5eafe7eb6b82a01000b7f2203a2910100160e111111111111111111111111111111111111111107007b00000000000000000000000000000000"
    )]
    #[case::spec366_some(
        366,
        Some(50),
        1_725_000_000_143,
        "410284fcad0b19bb29d4674531d6f115237e16afce377c4613ed73dcd73753a79c1bf448c161296392e003fe30bff3aab69ffb345177b757bb9001bdd8b6885d23c03c6dab6899da87e5b12d18fbae2c10ddf318eb06b301000b8f2203a2910100160e111111111111111111111111111111111111111107007b000000000000000000000000000000013200000000000000"
    )]
    #[case::spec369_none(
        369,
        None,
        1_725_000_000_127,
        "210284fcad0b19bb29d4674531d6f115237e16afce377c6ba42b051f330ea049da3f21601aba82ec47b9d78a6feaff8ec2f17a6baa9e880b7e02d1d2325e9ff2fa8349eebbea656478eb517b10f29ab8ab86ed8540272f00000b7f2203a2910100160e111111111111111111111111111111111111111107007b00000000000000000000000000000000"
    )]
    #[case::spec369_some(
        369,
        Some(50),
        1_725_000_000_143,
        "410284fcad0b19bb29d4674531d6f115237e16afce377c0a4eb4de6636540b97e6570468ae5fac180c310d0f1500157ad518915b2217a348ecd7c7f283d2d1c9e9c453a3f0337f0954c7f91852bee6e7c270c8f8431de100000b8f2203a2910100160e111111111111111111111111111111111111111107007b000000000000000000000000000000013200000000000000"
    )]
    #[case::spec371_none(
        371,
        None,
        1_725_000_000_127,
        "210284fcad0b19bb29d4674531d6f115237e16afce377c87a8b3c22a0abb14d596f04842477750059ada9eb78f0c8c39366e149abed37034d1dde1f881afda42f881fbad1bbe7dcc0ade6390114b80dd21445f5398e9e701000b7f2203a2910100160e111111111111111111111111111111111111111107007b00000000000000000000000000000000"
    )]
    #[case::spec371_some(
        371,
        Some(50),
        1_725_000_000_143,
        "410284fcad0b19bb29d4674531d6f115237e16afce377ce2801e02bf5514deec7d1aa4a430f3b27c74337ffce338ae21ebdd9b9e4c3daf3a8d8a4d824329d358d04b221b679383905025958b7c0ab24b8b9040cfec63cd01000b8f2203a2910100160e111111111111111111111111111111111111111107007b000000000000000000000000000000013200000000000000"
    )]
    fn perp_close_matches_pinned_sdk_across_approved_runtimes(
        #[case] spec_version: u32,
        #[case] slippage: Option<u64>,
        #[case] nonce: u64,
        #[case] expected: &str,
    ) {
        let snapshot = match spec_version {
            366 => snapshot(),
            369 => snapshot_spec369(),
            371 => snapshot_spec371(),
            _ => unreachable!(),
        };
        let permit = DeepXRuntimeSnapshotService::new(snapshot)
            .acquire()
            .unwrap();
        let signed = sign_perp_close(
            &permit,
            &key(),
            DeepXPerpCloseParams {
                subaccount: [0x11; 20],
                market_id: 7,
                price: 123,
                slippage,
            },
            nonce,
        )
        .unwrap();

        assert_eq!(hex::encode(signed.bytes()), expected);
        assert!(signed.has_valid_hash());
        assert_eq!(signed.runtime(), permit.snapshot().identity());
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    fn perp_close_signed_identity_sensitivity(#[case] mutation: u8) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut params = DeepXPerpCloseParams {
            subaccount: [0x11; 20],
            market_id: 7,
            price: 123,
            slippage: None,
        };
        let original = sign_perp_close(&permit, &key(), params, 125).unwrap();
        let mut nonce = 125;
        match mutation {
            0 => params.subaccount[19] ^= 1,
            1 => params.market_id += 1,
            2 => params.price += 1,
            3 => params.slippage = Some(0),
            4 => nonce += 1,
            _ => unreachable!(),
        }
        let changed = sign_perp_close(&permit, &key(), params, nonce).unwrap();
        assert_ne!(original.bytes(), changed.bytes());
        assert_ne!(original.extrinsic_hash(), changed.extrinsic_hash());
    }

    #[rstest]
    #[case(0, Value::from_bytes([0x11; 19]))]
    #[case(1, Value::u128(u128::from(u16::MAX) + 1))]
    #[case(2, Value::bool(true))]
    #[case(3, Value::unnamed_variant("Some", [Value::u128(u128::from(u64::MAX) + 1)]))]
    #[case(3, Value::unnamed_variant("Unknown", Vec::<Value>::new()))]
    fn perp_close_malformed_arguments(#[case] index: usize, #[case] value: Value) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut arguments = DeepXPerpCloseParams {
            subaccount: [0x11; 20],
            market_id: 7,
            price: 123,
            slippage: None,
        }
        .into_dynamic_arguments();
        arguments[index] = value;
        assert!(matches!(
            sign_dynamic_pallet_call(
                &permit,
                &key(),
                "PerpMarket",
                "close_position",
                arguments,
                125
            ),
            Err(SigningError::Encode(_))
        ));
    }

    #[rstest]
    #[case(0)]
    #[case(63)]
    #[case(64)]
    #[case(u64::MAX)]
    fn no_op_exact_metadata_contract(#[case] nonce: u64) {
        let snapshot = snapshot();
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let call = pallet.call_variant_by_name("no_op").unwrap();
        assert_eq!(pallet.index(), 19);
        assert_eq!(call.index, 28);
        assert!(call.fields.is_empty());
        let payload = subxt_core::dynamic::tx("Subaccount", "no_op", Vec::<Value>::new());
        let encoded =
            tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap();
        assert_eq!(encoded, [19, 28]);
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_no_op(&permit, &key(), nonce).unwrap();
        assert!(signed.bytes().ends_with(&encoded));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.nonce(), nonce);
        assert_eq!(signed.runtime(), permit.snapshot().identity());
        assert_eq!(signed, sign_no_op(&permit, &key(), nonce).unwrap());
        let changed = sign_no_op(&permit, &key(), nonce ^ 1).unwrap();
        assert_ne!(signed.bytes(), changed.bytes());
        assert_ne!(signed.extrinsic_hash(), changed.extrinsic_hash());
    }

    #[rstest]
    fn no_op_runtime_change_blocks_new_signing_permit() {
        let snapshot = snapshot();
        let mut changed = snapshot.identity().clone();
        changed.transaction_version += 1;
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        service.observe_runtime_identity(changed).unwrap();
        assert!(service.acquire().is_err());
        assert!(sign_no_op(&permit, &key(), 1).is_ok());
    }

    #[rstest]
    fn approved_testnet_catalog_contains_direct_market_calls() {
        let snapshot = snapshot();
        let interfaces = snapshot.interfaces();

        for (pallet, call) in [
            ("Subaccount", "no_op"),
            ("SpotMarket", "place_order"),
            ("SpotMarket", "cancel_order"),
            ("PerpMarket", "place_order"),
            ("PerpMarket", "cancel_order"),
        ] {
            interfaces.call(pallet, call).unwrap();
        }
    }

    #[rstest]
    fn perp_cancel_arguments_encode_against_approved_testnet_metadata() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let signed = sign_perp_cancel(
            &permit,
            &key(),
            DeepXPerpCancelParams {
                subaccount: [0x11; 20],
                order_id: 1_725_000_000_001,
                market_id: 7,
                fast_cancel: false,
            },
            1_725_000_000_125,
        )
        .unwrap();

        assert!(signed.has_valid_hash());
        assert_eq!(
            hex::encode(signed.bytes()),
            "050284fcad0b19bb29d4674531d6f115237e16afce377c91034ea2dd5af54cb435089b4ddd3cca24035b700655e0a59997f0a36d25a4175a3fdad77ba793855e1961a5a6d9c9c0f9a4ce3b54ece8b92c56d2cf6ed469d300000b7d2203a291010016031111111111111111111111111111111111111111012203a29101000007000000",
        );
        assert_eq!(
            hex::encode(signed.extrinsic_hash()),
            "4bf41e0b660b46dbf14be59a261b2ded7ea510b712f27063ace34a6d6d6e94c8",
        );
    }

    #[rstest]
    fn unknown_dynamic_call_is_rejected_without_panic() {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let result = sign_dynamic_pallet_call(
            &permit,
            &key(),
            "UnknownPallet",
            "unknown_call",
            Vec::new(),
            7,
        );

        assert!(matches!(
            result,
            Err(SigningError::RuntimeInterface(
                DeepXRuntimeInterfaceError::PalletUnavailable(pallet),
            )) if pallet == "UnknownPallet"
        ));
    }

    #[rstest]
    #[case::subaccount(0)]
    #[case::pair(1)]
    #[case::order(2)]
    #[case::side(3)]
    #[case::fast(4)]
    #[case::nonce(5)]
    fn spot_cancel_changes_signed_identity(#[case] mutation: u8) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut params = DeepXSpotCancelParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            order_id: 7,
            is_buy: false,
            fast_cancel: false,
        };
        let original = sign_spot_cancel(&permit, &key(), params, 125).unwrap();
        let mut nonce = 125;
        match mutation {
            0 => params.subaccount[19] ^= 1,
            1 => params.pair[31] ^= 1,
            2 => params.order_id += 1,
            3 => params.is_buy = true,
            4 => params.fast_cancel = true,
            5 => nonce += 1,
            _ => unreachable!(),
        }
        let changed = sign_spot_cancel(&permit, &key(), params, nonce).unwrap();
        assert_ne!(original.bytes(), changed.bytes());
        assert_ne!(original.extrinsic_hash(), changed.extrinsic_hash());
        assert_eq!(original.runtime(), changed.runtime());
        assert_eq!(original.signer(), changed.signer());
    }

    #[rstest]
    #[case::short_subaccount("subaccount", Value::from_bytes([0x11; 19]))]
    #[case::short_pair("pair", Value::from_bytes([0x22; 31]))]
    #[case::long_pair("pair", Value::from_bytes([0x22; 33]))]
    #[case::order_overflow("order_id", Value::u128(u128::from(u64::MAX) + 1))]
    #[case::wrong_side_type("is_buy", Value::u128(1))]
    #[case::unknown_reason("cancel_reason", Value::unnamed_variant("UnrecognizedCancelReason", Vec::<Value>::new()))]
    fn spot_cancel_rejects_malformed_dynamic_binding(#[case] field: &str, #[case] value: Value) {
        let service = DeepXRuntimeSnapshotService::new(snapshot());
        let permit = service.acquire().unwrap();
        let mut fields = vec![
            ("subaccount", Value::from_bytes([0x11; 20])),
            ("pair", Value::from_bytes([0x22; 32])),
            ("order_id", Value::u128(7)),
            ("is_buy", Value::bool(false)),
            (
                "cancel_reason",
                Value::unnamed_variant("UserCanceled", Vec::<Value>::new()),
            ),
            ("fast_cancel", Value::bool(false)),
        ];
        fields
            .iter_mut()
            .find(|(name, _)| *name == field)
            .unwrap()
            .1 = value;
        let result = sign_dynamic_pallet_call(
            &permit,
            &key(),
            "SpotMarket",
            "cancel_order",
            vec![Value::named_composite([(
                "params",
                Value::named_composite(fields),
            )])],
            125,
        );
        assert!(matches!(result, Err(SigningError::Encode(_))));
    }

    #[rstest]
    #[case(0, false, false)]
    #[case(u64::MAX, true, false)]
    #[case(1_725_000_000_001, false, true)]
    #[case(1, true, true)]
    fn spot_cancel_binds_spec366_schema(
        #[case] order_id: u64,
        #[case] is_buy: bool,
        #[case] fast_cancel: bool,
    ) {
        let snapshot = snapshot();
        let metadata = snapshot.metadata();
        let pallet = metadata.pallet_by_name("SpotMarket").unwrap();
        let call = pallet.call_variant_by_name("cancel_order").unwrap();
        assert_eq!(call.fields.len(), 1);
        assert_eq!(call.fields[0].name.as_deref(), Some("params"));
        let params_type = metadata.types().resolve(call.fields[0].ty.id).unwrap();
        let scale_info::TypeDef::Composite(composite) = &params_type.type_def else {
            panic!("Spot cancel params must be a composite");
        };
        assert_eq!(
            composite
                .fields
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            [
                "subaccount",
                "pair",
                "order_id",
                "is_buy",
                "cancel_reason",
                "fast_cancel"
            ],
        );
        let params = DeepXSpotCancelParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            order_id,
            is_buy,
            fast_cancel,
        };
        let mut expected = vec![pallet.index(), call.index];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&params.pair);
        expected.extend_from_slice(&order_id.to_le_bytes());
        expected.extend_from_slice(&[u8::from(is_buy), 0, u8::from(fast_cancel)]);
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire().unwrap();
        let signed = sign_spot_cancel(&permit, &key(), params, 1_725_000_000_125).unwrap();
        assert!(signed.bytes().ends_with(&expected));
        assert!(signed.has_valid_hash());
        assert_eq!(signed.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(signed.nonce(), 1_725_000_000_125);
        assert_eq!(signed.runtime(), permit.snapshot().identity());
    }
}
